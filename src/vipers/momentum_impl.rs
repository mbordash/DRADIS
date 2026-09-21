// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

/// Momentum Strategy
///
/// One-sided, non-hedged trades based on Binance price oracle signals.
/// Entry triggers when price velocity exceeds threshold and market conditions align.
/// Exits via take-profit, stop-loss, or reversal detection.
///
/// # Fees, and which leg can avoid them
///
/// Momentum enters with a FAK at the ask and pays the taker fee. Every exit
/// except two also crosses: stops, the reversal, decay, OBI-exhaustion and
/// near-expiry exits all sell at the bid with a FAK and pay the fee again. The
/// two that do not are settlement, which charges nothing, and the resting
/// take-profit (`momentum_resting_tp_enabled`): a post-only ask at the target,
/// emitted as [`StrategySignal::MakerRestingExit`] every tick a confirmed
/// position has no harder exit pending, and lifted only when the market runs
/// through the price the take-profit FAK would have sold at anyway. The patrol
/// owns that order — places it once, pulls it before any FAK needs the shares,
/// books the lift at the resting price net of the entry fee — exactly as it
/// does for FairValue's. Every taker exit rule here nets the fee it will pay
/// (`taker_exit_net_margin`) before calling a sale a profit.
///
/// There is deliberately no hold-to-settlement posture. The fee saved by
/// settling instead of selling is `rate × p × (1 − p)`, largest at $0.50 and
/// vanishing at $0.95 — largest exactly where settlement is a coin flip and
/// smallest where it is safe — while the downside of holding is the whole
/// price. Momentum has no model of `p` beyond the market's own bid, and against
/// that estimate holding is worth exactly the fee in expectation with a binary
/// variance attached. FairValue may hold because its model is an independent
/// reading; this strategy takes the certain sale.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::debug;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams, PositionKey};
use crate::vipers::{is_drawdown_limit_hit, resting_tp_price};
use crate::config;
use crate::helpers::price::ceil_to_tick_size;
use crate::venues::core::{MarketId, TimeInForce};

/// Stateful Momentum strategy implementation.
///
/// `prev_yes_obi` / `prev_no_obi` track the previous tick's computed OBI so that
/// the OBI-swing gate can detect sudden book-flip events between consecutive evaluations.
/// Uses `std::sync::Mutex` (non-async) because the values are read/written atomically
/// without any await points between lock acquisition and release.
///
/// `obi_exhaust_since` records, per held token, when the book *began* its current
/// unbroken run of reading exhausted. It is cleared the instant the book reads
/// normal again, so only a flip that persists for
/// `momentum_obi_exhaust_persist_secs` can arm the OBI exit — a single spike
/// cannot. Wall-clock rather than a tick count: the patrol loop runs at 75ms, so
/// any tick threshold small enough to look reasonable filters nothing. See the
/// exhaustion block in `evaluate_exit` for why this matters.
///
/// `reversal_since` is the same clock for the reversal exit: when the oracle
/// *began* its current unbroken run of reading reversed against a held token.
/// `velocity` is a 5s window, so one opposing tick reads as a reversal for up to
/// five seconds on its own; only a run longer than
/// `momentum_reversal_persist_secs` may fire the exit.
pub struct MomentumStrategyImpl {
    prev_yes_obi: Mutex<Decimal>,
    prev_no_obi:  Mutex<Decimal>,
    obi_exhaust_since: Mutex<HashMap<MarketId, chrono::DateTime<chrono::Utc>>>,
    reversal_since: Mutex<HashMap<MarketId, chrono::DateTime<chrono::Utc>>>,
    /// The same clock for the catastrophic floor: when the bid first read past it
    /// without interruption. One thin top-of-book reading must not sell.
    catastrophic_since: Mutex<HashMap<MarketId, chrono::DateTime<chrono::Utc>>>,
}

impl MomentumStrategyImpl {
    pub fn new() -> Self {
        Self {
            prev_yes_obi: Mutex::new(dec!(0)),
            prev_no_obi:  Mutex::new(dec!(0)),
            obi_exhaust_since: Mutex::new(HashMap::new()),
            reversal_since: Mutex::new(HashMap::new()),
            catastrophic_since: Mutex::new(HashMap::new()),
        }
    }
}

/// The take-profit target Momentum actually plans against at a given entry price.
///
/// Above $0.70 the configured percentage target is replaced by a flat 5%: the
/// contract cannot pay more than $1.00, so a 15% target on a $0.85 entry is a
/// target of $0.98 that the book will not reach. The entry-side fee gate and the
/// exit-side take-profit both go through here so they measure the same plan —
/// a gate that admitted a trade against one target while the exit chased another
/// would be checking the wrong arithmetic.
pub fn base_take_profit(entry_price: Decimal, configured_target: Decimal) -> Decimal {
    if entry_price >= MOMENTUM_FLAT_TP_ENTRY_FLOOR { MOMENTUM_FLAT_TP_TARGET } else { configured_target }
}

/// Entry price from which `base_take_profit` plans the flat target instead of
/// the configured one, and that flat target.
pub const MOMENTUM_FLAT_TP_ENTRY_FLOOR: Decimal = dec!(0.70);
pub const MOMENTUM_FLAT_TP_TARGET: Decimal = dec!(0.05);

/// Price tick the entry band is measured on.
const BAND_TICK: Decimal = dec!(0.01);

/// How often the card's band/crossing detail line is refreshed, and how often
/// a band that is narrower than the operator configured is written to the log.
const BAND_DETAIL_REFRESH_SECS: u64 = 60;
const BAND_WARN_INTERVAL_SECS: u64 = 3600;

/// True when the fee gate admits an entry at `ask` under this plan.
fn fee_admits(ask: Decimal, target: Decimal, max_fee_ratio: Decimal) -> bool {
    crate::vipers::fee_dominated_entry(ask, base_take_profit(ask, target), max_fee_ratio).is_none()
}

/// Why the strike-crossing entry branch cannot fire under this configuration,
/// or `None` when some price it admits would pass the fee gate.
///
/// The branch pays at most `cap`, at least `min_entry`, and only what the fee
/// gate admits. Its cap lived in a compile-time constant while the floor was a
/// Control Tower knob, and on the production build of 2026-09-21 the two had
/// drifted to 0.52 under 0.58: both sides dead, and the gate line folded that
/// into "ask above max entry" so nothing said the branch could never fire. The
/// conservative profile shipped it dead on its own (0.52 under a 0.72 floor).
/// The fee share cap kills it a second way: at a 15% target and a 40% cap no
/// price under ~$0.57 is admitted, whatever the floor says. Fee share falls as
/// the price rises within each target regime (`base_take_profit` switches to
/// the flat target at `MOMENTUM_FLAT_TP_ENTRY_FLOOR`), so the branch is live
/// iff the best price of either regime inside `[min_entry, cap]` is admitted.
pub fn crossing_branch_inert(cap: Decimal, min_entry: Decimal, target: Decimal, max_fee_ratio: Decimal) -> Option<String> {
    if cap <= Decimal::ZERO {
        return Some("off (cap 0)".to_string());
    }
    if cap < min_entry {
        return Some(format!("cap {:.2} below min entry {:.2}", cap, min_entry));
    }
    let best_low = cap.min(MOMENTUM_FLAT_TP_ENTRY_FLOOR - BAND_TICK);
    let low_live = best_low >= min_entry && fee_admits(best_low, target, max_fee_ratio);
    let high_live = cap >= MOMENTUM_FLAT_TP_ENTRY_FLOOR && fee_admits(cap, target, max_fee_ratio);
    if low_live || high_live {
        None
    } else {
        Some(format!(
            "fee-dominated at every price {:.2}–{:.2} (fee share cap {:.0}%)",
            min_entry, cap, max_fee_ratio * dec!(100),
        ))
    }
}

/// The entry prices the fee gate admits inside the configured band, as closed
/// intervals on the cent grid, lowest first.
///
/// The configured band says 0.58–0.78; the flat 5% target above $0.70 with a
/// 40% fee share cap refuses everything from $0.70 to $0.85, so the band the
/// operator can actually be filled in is 0.58–0.69. The Control Tower showed
/// the configured pair as if it were reachable. This is what the card and the
/// gate line report instead.
pub fn effective_entry_band(min_entry: Decimal, max_entry: Decimal, target: Decimal, max_fee_ratio: Decimal) -> Vec<(Decimal, Decimal)> {
    let mut out: Vec<(Decimal, Decimal)> = Vec::new();
    let lo = min_entry.max(BAND_TICK).round_dp(2);
    let hi = max_entry.min(Decimal::ONE - BAND_TICK).round_dp(2);
    let mut p = lo;
    let mut open: Option<Decimal> = None;
    while p <= hi {
        let admitted = fee_admits(p, target, max_fee_ratio);
        match (admitted, open) {
            (true, None) => open = Some(p),
            (false, Some(start)) => { out.push((start, p - BAND_TICK)); open = None; }
            _ => {}
        }
        p += BAND_TICK;
    }
    if let Some(start) = open { out.push((start, hi)); }
    out
}

/// "0.58–0.69" / "0.58–0.69, 0.86–0.90" / "none".
pub fn format_band(band: &[(Decimal, Decimal)]) -> String {
    if band.is_empty() { return "none".to_string(); }
    band.iter().map(|(a, b)| format!("{:.2}–{:.2}", a, b)).collect::<Vec<_>>().join(", ")
}

/// Advance or reset a token's "reversed since" clock and return how long, in
/// whole seconds, the oracle has read reversed against it without interruption.
///
/// `-1` when it does not read reversed right now (and the clock is dropped), `0`
/// on the first reversed reading. Pure over the map so the persistence rule can be
/// asserted on directly: a single reading must never satisfy a positive
/// persistence requirement.
pub fn reversal_persisted_secs(
    clocks: &mut HashMap<MarketId, chrono::DateTime<chrono::Utc>>,
    token: &MarketId,
    reversed_now: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> i64 {
    if reversed_now {
        let since = *clocks.entry(token.clone()).or_insert(now);
        (now - since).num_seconds()
    } else {
        clocks.remove(token);
        -1
    }
}

/// Net margin a TAKER exit at `bid` actually realizes, as a fraction of entry.
///
/// Every Momentum exit except a resting lift crosses to the bid with a FAK and
/// pays the venue's taker fee on the way out — `rate × bid × (1 − bid)` per
/// share on Polymarket International and Kalshi, nothing on Polymarket US. The
/// decay and near-expiry rules used to net out only `SELL_PRICE_OFFSET` (1¢)
/// and call anything above it a profit; at a $0.53 entry that is a "profit" of
/// one cent against an exit fee of 1.7¢, i.e. a MomentumDecay exit that books
/// a loss under a reason string that says profit. The toll netted here is the
/// larger of the offset and the fee, so on a zero-fee venue this is exactly
/// the old rule, and on a fee venue "net" means net. The entry fee is sunk by
/// the time any exit is decided and is not part of this figure.
pub fn taker_exit_net_margin(avg_entry: Decimal, bid: Decimal) -> Decimal {
    if avg_entry <= Decimal::ZERO { return Decimal::ZERO; }
    let toll = config::SELL_PRICE_OFFSET.max(crate::venues::taker_fee_per_share(bid));
    (bid - toll - avg_entry) / avg_entry
}

impl Default for MomentumStrategyImpl {
    fn default() -> Self { Self::new() }
}

/// True when a gate line for `key` on `asset` should be written now: it has not
/// been logged within `interval_secs`. The shared per-reason registry in
/// `crate::vipers::gate_log_permitted`, keyed under this strategy's name.
fn momentum_gate_log_permitted(asset: &str, key: &str, interval_secs: u64) -> bool {
    crate::vipers::gate_log_permitted("MomentumStrategy", asset, key, interval_secs)
}

/// Feed the "why no trades?" registry with `ui_reason` (unchanged from what the
/// Control Tower has always shown) and write a throttled info line.
///
/// Momentum used to report its gates only to that in-memory registry and to
/// `debug!`, so after a large BTC move on 2026-09-17 nothing on disk said whether
/// it saw the move and declined it or never triggered at all. `detail` is built
/// only when the line is actually written, so the quiet-book path costs a map
/// lookup and no formatting.
fn report_gate(
    asset: &str,
    ui_reason: &str,
    log_key: &str,
    interval_secs: u64,
    detail: impl FnOnce() -> String,
) {
    crate::helpers::viper_status::report_reason(asset, "MomentumStrategy", ui_reason);
    if momentum_gate_log_permitted(asset, log_key, interval_secs) {
        tracing::info!("🔒 Momentum gate: {}", detail());
    }
}

/// One side's gate readings at the moment its velocity trigger fired.
///
/// Mirrors the entry conditions in `evaluate_entry` so a near miss can be named
/// gate by gate. `strike` is `(oracle, strike, buffer)` when the market has a
/// resolved strike; without one the strike-relative and window checks do not
/// apply, exactly as in the no-strike entry branch.
pub struct SpikeGates {
    pub bull: bool,
    pub strike: Option<(Decimal, Decimal, Decimal)>,
    pub ask: Decimal,
    pub min_entry: Decimal,
    pub max_entry: Decimal,
    pub crossing_max: Decimal,
    /// The crossing branch cannot fire under this config (`crossing_branch_inert`).
    pub crossing_inert: bool,
    pub short_ok: bool,
    pub accel_ok: bool,
    pub window_blocks: bool,
    pub fee_blocks: bool,
    pub obi_adverse: bool,
    pub obi_exhausted: bool,
    pub obi_swing: bool,
    pub drift_blocks: bool,
}

/// The gates that held a triggered side, in entry-check order. Static names so
/// they can key the log throttle; the numbers go in the line itself.
///
/// With a strike, the side may enter by the primary branch (oracle beyond
/// strike + buffer, ask ≤ max entry) or the crossing branch (oracle beyond
/// strike, ask ≤ crossing cap); the price blocker is named only when neither
/// branch's price condition holds.
pub fn spike_blockers(g: &SpikeGates) -> Vec<&'static str> {
    let mut out = Vec::new();
    match g.strike {
        Some((oracle, strike, buffer)) => {
            let (beyond_strike, beyond_buffer) = if g.bull {
                (oracle > strike, oracle > strike + buffer)
            } else {
                (oracle < strike, oracle < strike - buffer)
            };
            let primary_ok = beyond_buffer && g.ask <= g.max_entry;
            let crossing_ok = beyond_strike && g.ask <= g.crossing_max;
            if !(primary_ok || crossing_ok) {
                if !beyond_strike {
                    out.push("oracle on wrong side of strike");
                } else if !beyond_buffer {
                    out.push(if g.crossing_inert {
                        "crossing branch inert (oracle inside strike buffer)"
                    } else {
                        "ask above crossing cap (oracle inside strike buffer)"
                    });
                } else {
                    out.push("ask above max entry");
                }
            }
        }
        None => {
            if g.ask > g.max_entry { out.push("ask above max entry"); }
        }
    }
    if g.ask < g.min_entry { out.push("ask below min entry"); }
    if !g.short_ok { out.push("1s velocity not confirming"); }
    if !g.accel_ok { out.push("decelerating"); }
    if g.strike.is_some() && g.window_blocks { out.push("daily window against"); }
    if g.fee_blocks { out.push("fee-dominated"); }
    if g.obi_adverse { out.push("OBI adverse"); }
    if g.obi_exhausted { out.push("OBI exhausted"); }
    if g.obi_swing { out.push("OBI swing"); }
    if g.drift_blocks { out.push("10m drift against"); }
    out
}

#[async_trait]
impl Strategy for MomentumStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        // "Why no trades?" registry feed (GET /api/vipers/status) plus a throttled
        // info line, so the blocking gate is recoverable from the log afterwards.
        let asset = ctx.crypto_filter.as_str();
        let idle = |r: &str| report_gate(asset, r, r, config::MOMENTUM_GATE_LOG_INTERVAL_SECS, || r.to_string());
        if !dc.enable_momentum {
            idle("disabled in config");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global Risk Check ────────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            idle("session drawdown limit hit");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Effective band and crossing-branch state ─────────────────────────
        // What the fee gate actually admits inside the configured band, and
        // whether the crossing branch can fire at all, on the card (detail line)
        // every minute and in the log hourly when either differs from what the
        // operator configured. Computed only when the throttle admits it.
        let crossing_max = dc.momentum_crossing_max_entry_price;
        let crossing_inert = crossing_branch_inert(
            crossing_max, dc.momentum_min_entry_price, dc.momentum_target_profit_pct, dc.momentum_max_fee_to_target_ratio);
        if momentum_gate_log_permitted(asset, "band detail", BAND_DETAIL_REFRESH_SECS) {
            let band = effective_entry_band(
                dc.momentum_min_entry_price, dc.momentum_max_entry_price,
                dc.momentum_target_profit_pct, dc.momentum_max_fee_to_target_ratio);
            let configured = format!("{:.2}–{:.2}", dc.momentum_min_entry_price, dc.momentum_max_entry_price);
            let reachable = format_band(&band);
            let crossing_txt = match &crossing_inert {
                Some(why) => format!("off: {why}"),
                None => format!("≤{:.2}", crossing_max),
            };
            crate::helpers::viper_status::report_detail(asset, "MomentumStrategy", Some(format!(
                "entry band {reachable} (configured {configured}) | crossing {crossing_txt}")));
            if (reachable != configured || crossing_inert.is_some())
                && momentum_gate_log_permitted(asset, "band warning", BAND_WARN_INTERVAL_SECS)
            {
                tracing::warn!(
                    "⚠️ Momentum entry band: configured {configured}, reachable {reachable} \
                     (target {:.0}%, flat {:.0}% from ${:.2}, fee share cap {:.0}%) | crossing branch {crossing_txt}",
                    dc.momentum_target_profit_pct * dec!(100), MOMENTUM_FLAT_TP_TARGET * dec!(100),
                    MOMENTUM_FLAT_TP_ENTRY_FLOOR, dc.momentum_max_fee_to_target_ratio * dec!(100),
                );
            }
        }

        let velocity = ctx.snapshot.velocity;
        let velocity_1s = ctx.snapshot.velocity_1s;
        let acceleration = ctx.snapshot.acceleration;
        let binance_price = ctx.snapshot.oracle_price;
        let strike_price = ctx.market.strike_price;

        // Oracle-relative thresholds — scale with asset price automatically
        let threshold    = config::oracle_threshold(dc.momentum_threshold_pct, binance_price);
        let strike_buffer = config::oracle_threshold(config::STRIKE_BUFFER_PCT, binance_price);

        let short_min = threshold * config::MOMENTUM_SHORT_WINDOW_FRACTION;
        let short_ok_bull = velocity_1s >= short_min;
        let short_ok_bear = velocity_1s <= -short_min;

        let accel_bypass = threshold * config::MOMENTUM_ACCELERATION_BYPASS_MULTIPLIER;
        let accel_ok_bull = acceleration >= dec!(0) || velocity >= accel_bypass;
        let accel_ok_bear = acceleration <= dec!(0) || velocity <= -accel_bypass;

        // ── Derivatives confirmation gate (Derivatives Raptor) ───────────────
        // A velocity spike with the perp book pushing the other way (aggressive
        // counter-taker flow) or unwinding hard (de-leveraging/squeeze) is a fade,
        // not a trend to chase. Block the contradicted direction. Disabled by
        // default; inert when OI/CVD report no data (zero = neutral). All-asset.
        if dc.momentum_deriv_gate_enabled {
            let cvd = ctx.snapshot.cvd_ratio;
            let oi_unwind = ctx.snapshot.oi_delta_pct <= dc.momentum_deriv_oi_unwind_block;
            if velocity > dec!(0) {
                let cvd_contradicts = cvd > dec!(0) && cvd <= dec!(1) - dc.momentum_deriv_cvd_confirm_margin;
                if cvd_contradicts || oi_unwind {
                    debug!(" Momentum deriv-gate blocked BULL: cvd={:.2} oi_unwind={}", cvd, oi_unwind);
                    idle("derivatives flow contradicts move");
                    return Ok(StrategySignal::NoSignal);
                }
            } else if velocity < dec!(0) {
                let cvd_contradicts = cvd > dec!(0) && cvd >= dec!(1) + dc.momentum_deriv_cvd_confirm_margin;
                if cvd_contradicts || oi_unwind {
                    debug!(" Momentum deriv-gate blocked BEAR: cvd={:.2} oi_unwind={}", cvd, oi_unwind);
                    idle("derivatives flow contradicts move");
                    return Ok(StrategySignal::NoSignal);
                }
            }
        }

        let trade_size = momentum_trade_size(
            dc.momentum_scaled_sizing_enabled,
            velocity, threshold,
            dc.momentum_min_trade_size_usdc,
            dc.momentum_max_trade_size_usdc,
        );

        // ── Strategy Exposure Check ──────────────────────────────────────────
        let current_exposure = {
            let pos_map = ctx.positions.lock().await;
            pos_map.iter()
                .filter(|(k, _)| k.strategy == "MomentumStrategy" && k.squadron == ctx.squadron_id)
                .filter(|(_, p)| p.counts_toward_exposure(chrono::Utc::now()))
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum::<Decimal>()
        };

        if current_exposure + trade_size > dc.momentum_max_exposure_usdc {
            idle("exposure cap reached");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Macro: build an entry OrderParams for a given token/fee ─────────
        macro_rules! entry_params {
            ($token:expr, $price:expr, $fee:expr) => {
                OrderParams {
                    token_id:    $token,
                    price:       $price,
                    shares:      trade_size / $price,
                    fee_bps:     $fee,
                    is_neg_risk: ctx.market.is_neg_risk,
                    market_name: ctx.market.market_name.clone(),
                    condition_id: ctx.market.condition_id.clone(),
                    order_type:  TimeInForce::Fak,
                    post_only:   false,
                    ghost_mode:  dc.ghost_mode,
                }
            };
        }

        // ── UNIVERSAL GATES (apply regardless of whether strike price is known) ──
        //
        // Previously, snapshot-age / spread / OBI checks only lived inside the
        // `if let Some(strike)` branch.  When strike resolution fails the bot falls
        // into the `else` branch which silently bypassed ALL these guards — observed
        // in 2026-05-13 session: trades with OBI_Y=-0.80 and OBI_Y=-0.53 entered
        // because the "without strike" path had no OBI veto.

        // ── Expiry guard ──────────────────────────────────────────────────────
        if let Some(close_time) = ctx.market.market_close_time {
            let secs_left = (close_time - chrono::Utc::now()).num_seconds();
            if secs_left < dc.momentum_min_secs_to_expiry_for_entry {
                debug!(" Momentum entry blocked: only {}s to expiry (min {}s)",
                    secs_left, dc.momentum_min_secs_to_expiry_for_entry);
                report_gate(asset, "too close to expiry", "too close to expiry", config::MOMENTUM_GATE_LOG_INTERVAL_SECS, || format!(
                    "too close to expiry | {}s left (min {}s)", secs_left, dc.momentum_min_secs_to_expiry_for_entry));
                return Ok(StrategySignal::NoSignal);
            }
        }

        // ── Market warmup gate ────────────────────────────────────────────────
        // After a market switch the WS orderbook subscription has only had a few
        // ticks to populate depth data.  The first evaluation fires within 1–2
        // seconds of the switch: OBI / velocity readings are unreliable, and
        // entries on that first tick reverse immediately (the book is still
        // repricing from the prior market context).
        //
        // Root cause of 2026-05-27 20:11 loss (−$0.7346):
        //   Switch at 20:11:33, entry at 20:11:39 ([0ms] first tick), SL −10%.
        //   Heartbeat showed YES OBI=−0.94 (strongly adverse) but evaluation used
        //   the very first WS depth tick on the new subscription — book hadn't
        //   settled to its equilibrium state yet.
        let secs_since_market_start = (chrono::Utc::now() - ctx.market_started_at).num_seconds();
        if secs_since_market_start < config::MOMENTUM_MARKET_WARMUP_SECS {
            debug!(" Momentum entry blocked: market warmup period ({}s < {}s min)",
                secs_since_market_start, config::MOMENTUM_MARKET_WARMUP_SECS);
            report_gate(asset, "market warmup", "market warmup", config::MOMENTUM_GATE_LOG_INTERVAL_SECS, || format!(
                "market warmup | {}s since switch (min {}s)", secs_since_market_start, config::MOMENTUM_MARKET_WARMUP_SECS));
            return Ok(StrategySignal::NoSignal);
        }

        // ── Snapshot staleness gate ───────────────────────────────────────────
        let snap_age = (chrono::Utc::now() - ctx.snapshot.timestamp).num_seconds();
        if snap_age > config::MOMENTUM_MAX_SNAPSHOT_AGE_SECS {
            debug!(" Momentum entry blocked: snapshot too stale ({}s > max {}s)",
                snap_age, config::MOMENTUM_MAX_SNAPSHOT_AGE_SECS);
            report_gate(asset, "snapshot stale", "snapshot stale", config::MOMENTUM_GATE_LOG_INTERVAL_SECS, || format!(
                "snapshot stale | {}s old (max {}s)", snap_age, config::MOMENTUM_MAX_SNAPSHOT_AGE_SECS));
            return Ok(StrategySignal::NoSignal);
        }

        // ── Spread gate: block wide-book entries ──────────────────────────────
        let ask_sum = ctx.snapshot.yes_ask + ctx.snapshot.no_ask;
        if ask_sum > dc.momentum_max_entry_ask_sum {
            debug!(" Momentum spread gate: ask_sum={:.3} > max {:.3} — book too wide",
                ask_sum, dc.momentum_max_entry_ask_sum);
            report_gate(asset, "book too wide", "book too wide", config::MOMENTUM_GATE_LOG_INTERVAL_SECS, || format!(
                "book too wide | ask_sum={:.3} > max {:.3}", ask_sum, dc.momentum_max_entry_ask_sum));
            return Ok(StrategySignal::NoSignal);
        }

        // ── Minimum price floor ───────────────────────────────────────────────
        // Block entries on near-zero priced tokens: buying YES at $0.09 creates
        // 100+ shares from a $9 budget; a 1¢ bid move = $1 swing.  Combined with
        // the 30s fill-confirm lock (no exits allowed) this caused a $2.13 loss on
        // 2026-05-13 (Trade #7: YES $0.09 × 106 shares, bid unchanged, -10% locked).
        // MOMENTUM_MIN_ENTRY_PRICE = 0.18 limits entries to 18%–82% probability range.
        let yes_ask = ctx.snapshot.yes_ask;
        let no_ask  = ctx.snapshot.no_ask;
        if yes_ask < dc.momentum_min_entry_price && no_ask < dc.momentum_min_entry_price {
            debug!(" Momentum min-price blocked: yes_ask={:.3} no_ask={:.3} both below floor {:.3}",
                yes_ask, no_ask, dc.momentum_min_entry_price);
            report_gate(asset, "asks below entry price floor", "asks below entry price floor", config::MOMENTUM_GATE_LOG_INTERVAL_SECS, || format!(
                "asks below entry price floor | yes_ask={:.3} no_ask={:.3} both < {:.3}",
                yes_ask, no_ask, dc.momentum_min_entry_price));
            return Ok(StrategySignal::NoSignal);
        }

        // ── Fee-dominated entry veto ──────────────────────────────────────────
        // The price floor above bounds share count and tick noise; it says nothing
        // about what the trade costs. Momentum pays a taker fee on both legs and
        // the round trip scales with entry price — 2 × rate × (1 − p) of notional,
        // 6.6% at $0.53 and 11.9% at a $0.15 floor — against a plan whose whole
        // edge is a percentage target. The take-profit floor in `evaluate_exit`
        // lifts the target so a trade at least clears its fee, but it cannot turn a
        // fee-dominated plan into a good bet; that decision belongs here, before
        // the money moves. Measured against the same target the exit will use.
        //
        // 2026-09-09 11:00 ET, first live Momentum trade on the aggressive profile:
        // YES at $0.53, 15% target. Round trip 6.58% — 44% of the plan — and the
        // position closed 62s later at −5.66% gross, so the fee was 116% of the
        // loss. With no fee the same trade nets −$0.27; it booked −$0.59.
        //
        // Per side, because the two asks differ. Binds on every venue: all three
        // charge the quadratic taker fee (Polymarket US at 0.06).
        let fee_reason_bull = crate::vipers::fee_dominated_entry(
            yes_ask, base_take_profit(yes_ask, dc.momentum_target_profit_pct), dc.momentum_max_fee_to_target_ratio);
        let fee_reason_bear = crate::vipers::fee_dominated_entry(
            no_ask, base_take_profit(no_ask, dc.momentum_target_profit_pct), dc.momentum_max_fee_to_target_ratio);
        let fee_blocks_bull = fee_reason_bull.is_some();
        let fee_blocks_bear = fee_reason_bear.is_some();
        if let Some(r) = &fee_reason_bull { debug!(" Momentum fee gate (BULL): {}", r); }
        if let Some(r) = &fee_reason_bear { debug!(" Momentum fee gate (BEAR): {}", r); }

        // ── OBI adverse-direction veto ────────────────────────────────────────
        // Default to -1.0 (maximally adverse) when depth data is missing.
        let whole_book = dc.obi_use_whole_book;
        let yes_obi = ctx.snapshot.yes_obi(whole_book);
        let no_obi  = ctx.snapshot.no_obi(whole_book);
        let obi_blocks_bull = yes_obi < dc.momentum_obi_adverse_block;
        let obi_blocks_bear = no_obi  < dc.momentum_obi_adverse_block;
        if obi_blocks_bull {
            debug!(" Momentum OBI veto (BULL): YES OBI={:.3} < block {:.3} — book fading the pump",
                yes_obi, dc.momentum_obi_adverse_block);
        }
        if obi_blocks_bear {
            debug!(" Momentum OBI veto (BEAR): NO OBI={:.3} < block {:.3} — book fading the dump",
                no_obi, dc.momentum_obi_adverse_block);
        }

        // ── OBI exhaustion veto ───────────────────────────────────────────────
        // When OBI > MOMENTUM_OBI_EXHAUSTION_BLOCK the book is dominated by bids
        // with no sellers — the momentum move is already spent and a reversal is
        // imminent.  Entering a BULL position into an all-bid book means we are
        // the last buyer before the flush.
        // 2026-05-24 8PM ghost trade: YES OBI=0.86 at entry → price dropped from
        // $0.67 to $0.61 in 30 s, -$0.72 loss.  Blocked at threshold 0.70.
        let obi_exhausted_bull = yes_obi > dc.momentum_obi_exhaustion_block;
        let obi_exhausted_bear = no_obi  > dc.momentum_obi_exhaustion_block;
        if obi_exhausted_bull {
            debug!(" Momentum OBI exhaustion (BULL): YES OBI={:.3} > threshold {:.3} — buyers exhausted",
                yes_obi, dc.momentum_obi_exhaustion_block);
        }
        if obi_exhausted_bear {
            debug!(" Momentum OBI exhaustion (BEAR): NO OBI={:.3} > threshold {:.3} — sellers exhausted",
                no_obi, dc.momentum_obi_exhaustion_block);
        }

        // ── OBI oscillation gate ─────────────────────────────────────────────────
        // When the YES OBI has been swinging wildly over the recent book ticks it
        // means informed traders are actively sweeping both sides — an extremely
        // unstable microstructure where momentum entries consistently reverse.
        //
        // Root cause of 2026-06-01 13:39 loss: 6 heartbeats before entry showed
        // OBI_Y cycling: −0.82 → −0.88 → +0.57 → +0.61 → −0.42 → +0.52.
        // The WS snapshot at entry had OBI_Y≈0.70+ (exhaustion) but the last
        // heartbeat recorded only 0.52 — the rapid oscillation masked the true state.
        //
        // Gate: block entry when the absolute difference between the current OBI
        // and the previous OBI snapshot exceeds MOMENTUM_OBI_SWING_BLOCK.  This
        // detects in-progress sweep events where the book is repricing too fast
        // for a safe directional entry.
        // Same source as the veto above, so the two never disagree about what the
        // book looks like. Toggling `obi_use_whole_book` mid-session makes the
        // first comparison after the flip span two different measures and can
        // read as one large swing; it self-corrects on the next tick.
        let cur_yes_obi = ctx.snapshot.yes_obi(whole_book);
        let cur_no_obi  = ctx.snapshot.no_obi(whole_book);

        // Read previous OBI and update atomically (non-async Mutex, no await held)
        let (prev_yes_obi_val, prev_no_obi_val) = {
            let mut py = self.prev_yes_obi.lock().unwrap();
            let mut pn = self.prev_no_obi.lock().unwrap();
            let old = (*py, *pn);
            *py = cur_yes_obi;
            *pn = cur_no_obi;
            old
        };

        let yes_obi_swing = (cur_yes_obi - prev_yes_obi_val).abs();
        let no_obi_swing  = (cur_no_obi  - prev_no_obi_val).abs();
        if yes_obi_swing > config::MOMENTUM_OBI_SWING_BLOCK {
            debug!(" Momentum OBI swing gate (BULL): swing={:.3} > block {:.3} — book unstable",
                yes_obi_swing, config::MOMENTUM_OBI_SWING_BLOCK);
        }
        if no_obi_swing > config::MOMENTUM_OBI_SWING_BLOCK {
            debug!(" Momentum OBI swing gate (BEAR): swing={:.3} > block {:.3} — book unstable",
                no_obi_swing, config::MOMENTUM_OBI_SWING_BLOCK);
        }
        let obi_swing_blocks_bull = yes_obi_swing > config::MOMENTUM_OBI_SWING_BLOCK;
        let obi_swing_blocks_bear = no_obi_swing  > config::MOMENTUM_OBI_SWING_BLOCK;

        // ── 10-minute oracle drift alignment gate ─────────────────────────────────
        // A 5-second velocity spike that contradicts the 10-minute oracle trend
        // is a dead-cat bounce / relief rally, not a new directional move.
        // Root cause: 2026-05-27 15:24 loss — BTC had been declining for 10m
        // before the 5s spike that triggered a YES entry at $0.64; the market
        // reversed $0.64→$0.61 in 30 seconds.
        // Oracle-relative 10m drift block — scales with asset price
        let drift_block_mag = config::oracle_threshold(config::MOMENTUM_DRIFT_10M_BLOCK_PCT, binance_price);
        let drift_bull_block = -drift_block_mag;
        let drift_bear_block =  drift_block_mag;
        let drift_10m = ctx.snapshot.oracle_drift_10m;
        // drift_bull_block is negative; block BULL entries when drift < this (BTC declining in last 10m)
        let drift_blocks_bull = drift_bull_block < dec!(0) && drift_10m < drift_bull_block;
        // drift_bear_block is positive; block BEAR entries when drift > this (BTC rising in last 10m)
        let drift_blocks_bear = drift_bear_block > dec!(0) && drift_10m > drift_bear_block;
        if drift_blocks_bull {
            debug!(" Momentum 10m-drift veto (BULL): drift_10m={:.2} < block {:.2} — BTC declining medium-term",
                drift_10m, drift_bull_block);
        }
        if drift_blocks_bear {
            debug!(" Momentum 10m-drift veto (BEAR): drift_10m={:.2} > block {:.2} — BTC rising medium-term",
                drift_10m, drift_bear_block);
        }

        // Viper Backtrace: shared stash helper — called once at whichever entry
        // branch actually fires, immediately before the Entry signal is returned.
        let stash_entry = |token: &crate::venues::core::MarketId, branch: &str, ask: rust_decimal::Decimal| {
            crate::helpers::metrics::stash_entry_signals_json(token.as_str(), serde_json::json!({
                "viper": "Momentum",
                "branch": branch,
                "velocity": velocity.to_string(),
                "threshold": threshold.to_string(),
                "binance_price": binance_price.to_string(),
                "strike": strike_price.map(|s| s.to_string()),
                "drift_10m": drift_10m.to_string(),
                "ask": ask.to_string(),
            }));
        };

        // ── Window/Daily trend filter ─────────────────────────────────────────
        // Read here rather than inside the strike branch so the near-miss log
        // below can report it; it is applied only when a strike is known, as
        // before.
        let window_yes_mid = match (&ctx.maker_market, &ctx.maker_snapshot) {
            (Some(_), Some(ws)) => Some(if ws.yes_bid > dec!(0) && ws.yes_ask < dec!(1) {
                (ws.yes_bid + ws.yes_ask) / dec!(2)
            } else {
                dec!(0.5)
            }),
            _ => None,
        };
        let window_blocks_bull = window_yes_mid.is_some_and(|m|
            config::MOMENTUM_WINDOW_BEARISH_BLOCK > dec!(0) && m < config::MOMENTUM_WINDOW_BEARISH_BLOCK);
        let window_blocks_bear = window_yes_mid.is_some_and(|m|
            config::MOMENTUM_WINDOW_BULLISH_BLOCK > dec!(0) && m > config::MOMENTUM_WINDOW_BULLISH_BLOCK);

        if let Some(strike) = strike_price {
            if let Some(m) = window_yes_mid {
                if window_blocks_bull || window_blocks_bear {
                    debug!(" Momentum window filter: YES_mid={:.3} blocks {}",
                        m, if window_blocks_bull { "BULL" } else { "BEAR" });
                }
            }

            // Primary entry
            if velocity > threshold && binance_price > (strike + strike_buffer)
                && yes_ask <= dc.momentum_max_entry_price
                && yes_ask >= dc.momentum_min_entry_price
                && short_ok_bull && accel_ok_bull && !window_blocks_bull && !fee_blocks_bull && !obi_blocks_bull && !obi_exhausted_bull && !obi_swing_blocks_bull && !drift_blocks_bull
            {
                stash_entry(&ctx.market.yes_token, "BULL_primary", yes_ask);
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.yes_token.clone(), yes_ask, ctx.market.yes_fee_bps as u16),
                    pair_params: None,
                });
            } else if velocity < -threshold && binance_price < (strike - strike_buffer)
                && no_ask <= dc.momentum_max_entry_price
                && no_ask >= dc.momentum_min_entry_price
                && short_ok_bear && accel_ok_bear && !window_blocks_bear && !fee_blocks_bear && !obi_blocks_bear && !obi_exhausted_bear && !obi_swing_blocks_bear && !drift_blocks_bear
            {
                stash_entry(&ctx.market.no_token, "BEAR_primary", no_ask);
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.no_token.clone(), no_ask, ctx.market.no_fee_bps as u16),
                    pair_params: None,
                });
            }

            // Secondary "strike-crossing" entry
            if velocity > threshold && binance_price > strike
                && yes_ask <= crossing_max
                && yes_ask >= dc.momentum_min_entry_price
                && short_ok_bull && accel_ok_bull && !window_blocks_bull && !fee_blocks_bull && !obi_blocks_bull && !obi_exhausted_bull && !obi_swing_blocks_bull && !drift_blocks_bull
            {
                stash_entry(&ctx.market.yes_token, "BULL_crossing", yes_ask);
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.yes_token.clone(), yes_ask, ctx.market.yes_fee_bps as u16),
                    pair_params: None,
                });
            } else if velocity < -threshold && binance_price < strike
                && no_ask <= crossing_max
                && no_ask >= dc.momentum_min_entry_price
                && short_ok_bear && accel_ok_bear && !window_blocks_bear && !fee_blocks_bear && !obi_blocks_bear && !obi_exhausted_bear && !obi_swing_blocks_bear && !drift_blocks_bear
            {
                stash_entry(&ctx.market.no_token, "BEAR_crossing", no_ask);
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.no_token.clone(), no_ask, ctx.market.no_fee_bps as u16),
                    pair_params: None,
                });
            }
        } else {
            // Without strike — universal gates already applied above; only
            // velocity + price bounds + drift alignment needed here.
            if velocity > threshold
                && yes_ask <= dc.momentum_max_entry_price
                && yes_ask >= dc.momentum_min_entry_price
                && short_ok_bull && accel_ok_bull && !fee_blocks_bull && !obi_blocks_bull && !obi_exhausted_bull && !obi_swing_blocks_bull && !drift_blocks_bull
            {
                stash_entry(&ctx.market.yes_token, "BULL_nostrike", yes_ask);
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.yes_token.clone(), yes_ask, ctx.market.yes_fee_bps as u16),
                    pair_params: None,
                });
            } else if velocity < -threshold
                && no_ask <= dc.momentum_max_entry_price
                && no_ask >= dc.momentum_min_entry_price
                && short_ok_bear && accel_ok_bear && !fee_blocks_bear && !obi_blocks_bear && !obi_exhausted_bear && !obi_swing_blocks_bear && !drift_blocks_bear
            {
                stash_entry(&ctx.market.no_token, "BEAR_nostrike", no_ask);
                return Ok(StrategySignal::Entry {
                    params: entry_params!(ctx.market.no_token.clone(), no_ask, ctx.market.no_fee_bps as u16),
                    pair_params: None,
                });
            }
        }

        // Fall-through: no velocity trigger fired (the common quiet-market case),
        // or a trigger fired but a directional gate (fee/OBI/drift/window/price) held.
        // The fee gate is named when it held the triggered side: it is the one
        // refusal here that is structural rather than a read of the book, and the
        // operator needs to see it as such — declining a fee-dominated trade is
        // the product working, and the Control Tower should say so.
        if velocity.abs() <= threshold {
            report_gate(asset, "velocity below trigger", "velocity below trigger",
                config::MOMENTUM_GATE_LOG_INTERVAL_SECS, || format!(
                "velocity below trigger | vel={:.2} trigger=±{:.2} (5s, oracle ${:.2})",
                velocity, threshold, binance_price));
            return Ok(StrategySignal::NoSignal);
        }

        // A spike fired and a gate held it. Name every gate that held the
        // triggered side, with the readings, so a sat-out move can be explained
        // from the log. The Control Tower reason is unchanged.
        let bull = velocity > threshold;
        let gates = if bull {
            SpikeGates {
                bull, strike: strike_price.map(|s| (binance_price, s, strike_buffer)),
                ask: yes_ask, min_entry: dc.momentum_min_entry_price, max_entry: dc.momentum_max_entry_price,
                crossing_max, crossing_inert: crossing_inert.is_some(),
                short_ok: short_ok_bull, accel_ok: accel_ok_bull, window_blocks: window_blocks_bull,
                fee_blocks: fee_blocks_bull, obi_adverse: obi_blocks_bull, obi_exhausted: obi_exhausted_bull,
                obi_swing: obi_swing_blocks_bull, drift_blocks: drift_blocks_bull,
            }
        } else {
            SpikeGates {
                bull, strike: strike_price.map(|s| (binance_price, s, strike_buffer)),
                ask: no_ask, min_entry: dc.momentum_min_entry_price, max_entry: dc.momentum_max_entry_price,
                crossing_max, crossing_inert: crossing_inert.is_some(),
                short_ok: short_ok_bear, accel_ok: accel_ok_bear, window_blocks: window_blocks_bear,
                fee_blocks: fee_blocks_bear, obi_adverse: obi_blocks_bear, obi_exhausted: obi_exhausted_bear,
                obi_swing: obi_swing_blocks_bear, drift_blocks: drift_blocks_bear,
            }
        };
        let blockers = spike_blockers(&gates);
        let side = if bull { "BULL" } else { "BEAR" };
        let key = format!("spike {} | {}", side, blockers.join(", "));
        let fee_reason = if bull { &fee_reason_bull } else { &fee_reason_bear };
        let ui_reason = if gates.fee_blocks {
            fee_reason.as_deref().unwrap_or("fee-dominated entry")
        } else {
            "spike blocked by entry gates (OBI/drift/price)"
        };
        report_gate(asset, ui_reason, &key, config::MOMENTUM_SPIKE_LOG_INTERVAL_SECS, || {
            let strike_txt = strike_price
                .map(|s| format!("${:.2} (buffer ${:.2})", s, strike_buffer))
                .unwrap_or_else(|| "unknown".into());
            let window_txt = window_yes_mid
                .map(|m| format!("{:.3}", m))
                .unwrap_or_else(|| "n/a".into());
            let (obi, swing) = if bull { (yes_obi, yes_obi_swing) } else { (no_obi, no_obi_swing) };
            format!(
                "{} spike held by [{}] | vel={:.2} trigger=±{:.2} 1s={:.2} (need {:.2}) accel={:.3} \
                 | oracle=${:.2} strike={} | {}_ask={:.3} (entry {:.2}–{:.2}, crossing {}) \
                 | OBI={:.2} (adverse <{:.2}, exhausted >{:.2}) swing={:.2} (max {:.2}) \
                 | drift10m={:.2} (block {:.2}) | daily YES mid={}{}",
                side,
                if blockers.is_empty() { "unclassified".to_string() } else { blockers.join(", ") },
                velocity, threshold, velocity_1s, short_min, acceleration,
                binance_price, strike_txt,
                if bull { "yes" } else { "no" }, gates.ask,
                dc.momentum_min_entry_price, dc.momentum_max_entry_price,
                match &crossing_inert { Some(why) => format!("off: {why}"), None => format!("≤{:.2}", crossing_max) },
                obi, dc.momentum_obi_adverse_block, dc.momentum_obi_exhaustion_block,
                swing, config::MOMENTUM_OBI_SWING_BLOCK,
                drift_10m, drift_block_mag, window_txt,
                fee_reason.as_ref().map(|r| format!(" | fee: {}", r)).unwrap_or_default(),
            )
        });
        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        let pos_map = ctx.positions.lock().await;

        // Resting take-profit asks, deferred until every position has had its
        // hard exits evaluated: a healthy position's ask must never preempt a
        // stop still pending on another, since one signal leaves per tick.
        let mut resting_tps: Vec<StrategySignal> = Vec::new();

        // Drop OBI-exhaustion clocks for tokens we no longer hold, so a closed
        // position cannot leave a primed timer behind for the next entry on the
        // same token. Cheap: the map only ever holds tokens Momentum is long.
        {
            let held = |tok: &MarketId| {
                pos_map.contains_key(&PositionKey::new(&ctx.squadron_id, "MomentumStrategy", tok.clone()))
            };
            let mut clocks = self.obi_exhaust_since.lock().unwrap();
            if !clocks.is_empty() {
                clocks.retain(|tok, _| held(tok));
            }
            let mut reversals = self.reversal_since.lock().unwrap();
            if !reversals.is_empty() {
                reversals.retain(|tok, _| held(tok));
            }
            let mut floors = self.catastrophic_since.lock().unwrap();
            if !floors.is_empty() {
                floors.retain(|tok, _| held(tok));
            }
        }

        for (key, position) in pos_map.iter() {
            if key.squadron != ctx.squadron_id { continue; }
            let (strategy_name, token_id) = (&key.strategy, &key.market);
            if strategy_name != "MomentumStrategy" { continue; }
            // Slice 2b: position keys are neutral MarketId throughout (no U256).
            let tok = token_id.clone();
            // Check hourly market first, then maker/daily market.
            // Bug fix (2026-06-12): positions reconciled from session restart can belong
            // to the maker market (daily "Up or Down") rather than the hourly market.
            // Skipping maker-market tokens caused reconciled positions to never hit
            // stop-loss evaluation, producing uncontrolled losses (e.g. -$5.38 on a
            // BasisStrategy position wrongly re-attributed to MomentumStrategy).
            // `ask` is carried alongside `bid` so exits that ask "has this position
            // actually moved against us?" can mark against mid rather than bid. A
            // position marked at bid the instant it fills is underwater by the full
            // spread with no price having moved at all.
            let (bid, ask, bid_depth, snap_ts) = if tok == ctx.market.yes_token {
                (ctx.snapshot.yes_bid, ctx.snapshot.yes_ask, ctx.snapshot.yes_bid_depth, ctx.snapshot.timestamp)
            } else if tok == ctx.market.no_token {
                (ctx.snapshot.no_bid, ctx.snapshot.no_ask, ctx.snapshot.no_bid_depth, ctx.snapshot.timestamp)
            } else if let (Some(mk), Some(mk_snap)) = (&ctx.maker_market, &ctx.maker_snapshot) {
                if tok == mk.yes_token {
                    (mk_snap.yes_bid, mk_snap.yes_ask, mk_snap.yes_bid_depth, mk_snap.timestamp)
                } else if tok == mk.no_token {
                    (mk_snap.no_bid, mk_snap.no_ask, mk_snap.no_bid_depth, mk_snap.timestamp)
                } else {
                    continue
                }
            } else {
                continue
            };

            // A zero bid is an empty book (or a position evaluated before its first
            // snapshot), not a price. Every exit below sells at the bid, so none of
            // them may act on it: a reconciled position could otherwise be stopped out
            // and written off at $0.
            if bid <= dec!(0) {
                continue;
            }

            let secs_held = (chrono::Utc::now() - position.opened_at).num_seconds();

            // A "catastrophic" stop that is TIGHTER than the normal stop is not an
            // emergency floor, it is a stricter stop that fires first — and it fires
            // during the fill-confirmation window, when the position is underwater by
            // the spread alone. Balanced shipped 0.06 catastrophic against a 0.11 stop,
            // so a 7%-spread entry cleared the "catastrophic" bar on tick one of every
            // trade. The templates now ladder it above the stop; this floor makes the
            // invariant hold for any value an operator can dial in from Control Tower.
            let catastrophic_sl_pct = dc.momentum_catastrophic_sl_pct.max(dc.momentum_stop_loss_pct);

            // Advance the catastrophic floor's persistence clock on every tick, before
            // any branch below can `continue` past it, so a mark that recovers above
            // the floor always resets the clock.
            let floor_breached = position.avg_entry > dec!(0)
                && (bid - position.avg_entry) / position.avg_entry <= -catastrophic_sl_pct;
            let floor_persisted_secs = reversal_persisted_secs(
                &mut self.catastrophic_since.lock().unwrap(), &tok, floor_breached, chrono::Utc::now());

            // Ghost fills count as confirmed; without this a simulated position
            // sat in the fill-confirmation branch for its entire life, where only
            // a catastrophic move may exit.
            if position.fill_effective_at(dc.ghost_mode).is_none() {
                let profit_margin_check = (bid - position.avg_entry) / position.avg_entry;
                if secs_held < config::MOMENTUM_FILL_CONFIRM_MIN_HOLD_SECS {
                    // During the fill-confirmation window, allow an immediate escape
                    // only if the loss is catastrophic (> MOMENTUM_CATASTROPHIC_SL_PCT).
                    // Prevents lock-in to large sudden adverse moves while waiting for
                    // the Polymarket indexer to register the balance.
                    // Root cause: 2026-05-13 Trade #3 lost -14% during a 30s lock with
                    // no exit allowed; a catastrophic SL at 8% would have exited at ~5s.
                    if profit_margin_check > -catastrophic_sl_pct {
                        continue; // Not catastrophic yet — wait for fill confirmation
                    }
                    // Fall through: the catastrophic floor below exits this tick
                } else {
                    // After 30s: normal stop-loss gate
                    if profit_margin_check > -dc.momentum_stop_loss_pct { continue; }
                }
            }

            let avg_entry = position.avg_entry;
            let velocity = ctx.snapshot.velocity;
            let threshold = config::oracle_threshold(dc.momentum_threshold_pct, ctx.snapshot.oracle_price);

            if avg_entry <= dec!(0) { continue; }
            let profit_margin = (bid - avg_entry) / avg_entry;

            // Resolve the market this token belongs to (hourly or daily/maker).
            // Reconciled positions can sit on the maker market even though MomentumStrategy
            // is normally venue=Hourly.  We need the right market context for exit params
            // and for the near-expiry check.
            let is_maker_token = ctx.maker_market.as_ref()
                .map(|mk| tok == mk.yes_token || tok == mk.no_token)
                .unwrap_or(false);
            let exit_market: &crate::state::MarketConfig = if is_maker_token {
                ctx.maker_market.as_ref().unwrap()
            } else {
                &ctx.market
            };
            let exit_close_time = exit_market.market_close_time;

            // Macro: build exit params for this token
            macro_rules! exit_params {
                () => {
                    OrderParams {
                        token_id: tok.clone(),
                        price: bid,
                        shares: position.shares,
                        fee_bps: if tok == exit_market.yes_token { exit_market.yes_fee_bps as u16 } else { exit_market.no_fee_bps as u16 },
                        is_neg_risk: exit_market.is_neg_risk,
                        market_name: exit_market.market_name.clone(),
                        condition_id: exit_market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    }
                };
            }

            // Catastrophic floor: the last-resort exit, at any hold time and in
            // either fill state.
            //
            // The stop-loss below waits MOMENTUM_MIN_HOLD_BEFORE_SL_SECS so a brief
            // wiggle after entry cannot stop a position out. Until 2026-09-15 the
            // catastrophic floor only lived in the fill-confirmation branch above,
            // and even there it fell through to that same held-time gate, so it
            // never fired at all. 2026-09-15 14:29 ET: Momentum bought YES at $0.63
            // on a $560 BTC spike that reversed within ten seconds; the bid was
            // $0.52 (−17%) 12 s in and $0.41 by 71 s, and the only exit that could
            // act was the stop at exactly 120 s, which sold at $0.31 (−51%, −$2.50).
            //
            // The floor reads the raw top of book, which publishes the best level
            // whatever its size, so it waits `momentum_catastrophic_persist_secs` of
            // continuous breach (the clock is advanced above) before it sells: one thin
            // level with the next bid ten cents down must not sell into a book that
            // reposts a second later. Zero bids never reach this point.
            if floor_breached && floor_persisted_secs >= dc.momentum_catastrophic_persist_secs {
                let spread = if ask > bid { ask - bid } else { dec!(0) };
                let age = (chrono::Utc::now() - snap_ts).num_seconds();
                let reason = format!(
                    "MomentumCatastrophicSL: bid=${:.4}, loss={:.2}%, held={}s, persisted={}s | ask=${:.4} spread=${:.2} bid_depth={:.0} age={}s",
                    bid, profit_margin * dec!(100), secs_held, floor_persisted_secs, ask, spread, bid_depth, age,
                );
                return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
            }

            // Near-expiry forced exit
            //
            // Inside the final MOMENTUM_EXPIRY_EXIT_SECS a position that is not a
            // clear winner is flattened rather than carried into a binary
            // settlement. "Clear winner" is measured net of what the flatten
            // actually costs — the taker fee at this bid, not a flat 1¢ — so a
            // +4% mark whose exit fee is 3.3% is sold for the certain +0.7%
            // rather than held on the strength of a "2.1% net" that was never
            // net. A position that does clear the bar is NOT held to settlement:
            // it stays under every rule below, and its resting ask keeps
            // working, so it leaves by lift, by decay, or by stop like any
            // other. See the module doc for why there is no settlement hold.
            if let Some(close_time) = exit_close_time {
                let secs_left = (close_time - chrono::Utc::now()).num_seconds();
                let net_profit_for_expiry = taker_exit_net_margin(avg_entry, bid);
                if secs_left <= config::MOMENTUM_EXPIRY_EXIT_SECS && net_profit_for_expiry < config::MOMENTUM_EXPIRY_MIN_PROFIT_TO_HOLD {
                    let reason = format!("NearExpiry: bid=${:.4}, net_profit={:.2}%", bid, net_profit_for_expiry * dec!(100));
                    return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
                }
            }

            // Take-profit target, floored so it actually clears the round trip.
            //
            // Venue fees are entry-price dependent: `2 × rate × (1 − entry)` of
            // notional (see `venues::round_trip_fee_pct`). A flat percentage target
            // is therefore below break-even over part of the permitted entry range —
            // with the balanced 10% target every entry under $0.286 books a "profit"
            // that is a net loss, and the conservative 6% target does so under $0.571,
            // i.e. across almost its whole $0.20–$0.50 entry band. Raising the target
            // to a margin above the fee makes MomentumTP mean what it says.
            //
            // This is the TAKER target: the bar for selling at the bid with a FAK,
            // which pays the second leg. The resting ask further down is floored
            // against the entry leg alone, because a lift pays nothing.
            let fee_floor = crate::venues::round_trip_fee_pct(avg_entry)
                * dc.momentum_tp_fee_margin_mult;
            let base_target = base_take_profit(avg_entry, dc.momentum_target_profit_pct);
            let target = base_target.max(fee_floor);
            if target > base_target {
                debug!("Momentum TP floor: target {:.2}% → {:.2}% (round-trip fee {:.2}% at entry ${:.4})",
                       base_target * dec!(100), target * dec!(100),
                       crate::venues::round_trip_fee_pct(avg_entry) * dec!(100), avg_entry);
            }
            let stop_loss = -dc.momentum_stop_loss_pct;
            let reversal_threshold = -(threshold * dc.momentum_reversal_ratio);

            if profit_margin >= target || bid >= dc.momentum_take_profit_ceiling {
                let reason = format!("MomentumTP: bid=${:.4}, profit={:.2}%", bid, profit_margin * dec!(100));
                return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
            }

            // Stop-loss: Requires MOMENTUM_MIN_HOLD_BEFORE_SL_SECS (120s) hold time before SL can trigger.
            // Root cause fix for 2026-06-05 instant stop-outs (30s exits at -$1.12, -$0.90).
            // Prevents timing-related stop-outs from brief adverse price swings immediately after entry.
            if secs_held >= config::MOMENTUM_MIN_HOLD_BEFORE_SL_SECS && profit_margin <= stop_loss {
                let reason = format!("MomentumSL: bid=${:.4}, loss={:.2}%", bid, profit_margin * dec!(100));
                return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
            }

            // Momentum Decay exit
            // Measured NET of what the taker sale costs. This first netted only
            // SELL_PRICE_OFFSET, after raw `profit_margin > 0` fired on a bid one
            // cent above entry and booked $0.0000 exits (2026-05-12 YES @ 0.71,
            // bid=$0.72). The offset alone has the same defect one step up on a
            // fee venue: at a $0.53 entry the exit fee is 1.7¢, so a 1¢ "profit"
            // was a MomentumDecay exit that lost money. The fee is netted now.
            //
            // This exit preempts the resting take-profit whenever it fires — it
            // is the strategy saying the move is spent and the target will not
            // be reached — so it has to at least clear the fee it chooses to pay
            // in place of the free lift it gives up.
            //
            // Two defects fixed 2026-09-16, both found by the entry replay
            // (~/dradis-research/momentum-replay-2026-09-15, registered run V0):
            // this exit took 30 of 42 exits, banked +5.4% of stake gross on
            // average and paid 92% of it to the two taker fees, +$0.02 net each.
            //
            //   1. Units. `decay_min` is a fraction of `threshold`, a 5 s velocity,
            //      but it was compared with the 1 s velocity. A 1 s move of 30% of
            //      the 5 s threshold is a pace 1.5x the entry trigger, so "spent"
            //      read true on nearly every second, and on every second the
            //      raptor's 1 s window read zero (about half of them at Binance's
            //      1 Hz ticker). The signal was vacuous and the exit was "sell on
            //      the first tick a fee above entry". The fade is now read on the
            //      same 5 s window the trigger uses, and the fraction is a knob.
            //   2. Bar. Net of the exit fee alone, "> 0" only means the trade
            //      loses no more than its entry fee, so the exit booked losses on
            //      positions that were ahead while forfeiting a lift that pays no
            //      fee. It must now also clear `momentum_decay_fee_margin_mult`
            //      times the entry-leg fee: at the default 1.0 the round trip is
            //      at least break-even before this exit may take it; 0 restores
            //      the old bar.
            let decay_min = threshold * dc.momentum_decay_exit_fraction;
            let is_yes = tok == ctx.market.yes_token;
            let net_profit_margin = taker_exit_net_margin(avg_entry, bid);
            let decay_floor = crate::venues::entry_only_fee_pct(avg_entry) * dc.momentum_decay_fee_margin_mult;
            let faded = if is_yes { velocity < decay_min } else { velocity > -decay_min };
            if faded && net_profit_margin > decay_floor {
                let reason = format!(
                    "MomentumDecay: bid=${:.4}, profit={:.2}%, net={:.2}% vs floor {:.2}%, v5={:.2} vs fade {:.2}",
                    bid, profit_margin * dec!(100), net_profit_margin * dec!(100), decay_floor * dec!(100),
                    velocity, if is_yes { decay_min } else { -decay_min },
                );
                return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
            }

            // Reversal exit: the oracle is now moving against the position at
            // `momentum_reversal_ratio` of the velocity that justified entering.
            //
            // This is a thesis-disproven exit, not a P&L exit, and deliberately
            // carries no fee floor. At the moment it fires the entry fee is sunk
            // and the exit fee is owed on every path out except settlement, so the
            // fee has nothing to say about hold-versus-exit; a floor that refused
            // to sell until the loss "covered" the fee would only ride a real
            // reversal down to the stop. What the fee does say is that a FALSE
            // reversal is expensive — it pays the second leg for nothing — so the
            // bar is on the signal, not the loss:
            //
            //   1. Min hold, as before but now a Control Tower knob.
            //   2. Persistence. `velocity` is a 5s window, so a single opposing
            //      tick reads as a reversal for up to five seconds by itself. The
            //      2026-09-09 11:01 ET exit fired on one such reading 62s after a
            //      $0.53 entry (well past the hold floor), sold at $0.50, and paid
            //      the second fee to turn a −5.7% mark into a −12.3% realized
            //      loss. The oracle must now read reversed continuously for
            //      `momentum_reversal_persist_secs`; any non-reversed reading
            //      resets the clock. Wall-clock, like the OBI clock: the patrol
            //      loop runs at 75ms, so a tick count filters nothing.
            //
            // The stop-loss and catastrophic stop above are untouched by either.
            let reversed_now = (is_yes && velocity < reversal_threshold)
                || (!is_yes && velocity > -reversal_threshold);
            let reversed_for_secs = reversal_persisted_secs(
                &mut self.reversal_since.lock().unwrap(), &tok, reversed_now, chrono::Utc::now());
            if reversed_now && secs_held >= dc.momentum_reversal_min_hold_secs {
                if reversed_for_secs >= dc.momentum_reversal_persist_secs {
                    let reason = format!(
                        "MomentumReversal: bid=${:.4}, profit={:.2}%, velocity={:.2} vs {:.2}, reversed={}s, held={}s",
                        bid, profit_margin * dec!(100), velocity, reversal_threshold, reversed_for_secs, secs_held,
                    );
                    return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
                }
                debug!(" Momentum reversal read but not yet persistent: {}s < {}s (velocity={:.2} vs {:.2})",
                    reversed_for_secs, dc.momentum_reversal_persist_secs, velocity, reversal_threshold);
            }

            // OBI exhaustion in-position exit
            //
            // When the book flips to exhaustion AFTER we enter (OBI > exhaustion_block
            // for a YES position, or for the NO book on a NO position) it can mean all
            // buyers have accumulated and a selling reversal is imminent. If we are at
            // or below breakeven, exit rather than wait for the full stop-loss.
            //
            // Root cause of 2026-06-01 13:39 loss: entry at $0.49 avg, 14 s later
            // OBI_Y=0.85 (above exhaustion=0.70) at bid=$0.48 (−4%), but no exit
            // path existed for this scenario.  The SL eventually fired at −8%
            // ($0.06 worse than the OBI-detected reversal signal).
            //
            // Three guards, all added after 2026-08-19 10:55 ET, where this branch
            // took its first-ever fire and produced the day's only loss: it bought YES
            // @ $0.43 and sold @ $0.40 **five seconds later**, calling −6.97% a loss
            // when the entire move was the bid/ask spread. BTC then ran $65,767 →
            // $66,632 and that token went to $0.95; FairValue bought the same token at
            // the same $0.43 seventy-one seconds later and booked +20.9%. The read was
            // right and the exit threw it away.
            //
            //   1. Min hold. Every other exit here has one — SL waits 120 s, Reversal
            //      45 s — but this branch fired on the first tick after fill
            //      confirmation. OBI exhaustion IS a reversal signal, so it now waits
            //      the same as Reversal.
            //   2. Mark at mid, not bid. `profit_margin <= 0` measured at bid is true
            //      by construction for a fresh taker fill: buy at ask, mark at bid, and
            //      the position is "down" by the spread before any price has moved.
            //      Marking at mid asks whether the *market* moved against us.
            //   3. Persistence. Single-sample OBI on this book is noise — the
            //      2026-08-19 10:00–11:15 ET heartbeats read +0.95, −0.80, +0.93,
            //      −0.93, −0.88 on consecutive minutes, crossing ±0.70 constantly. The
            //      book must now read exhausted continuously for
            //      `momentum_obi_exhaust_persist_secs`, and any normal reading resets
            //      the clock. Measured in wall-clock, not ticks: the patrol loop runs
            //      at 75ms, so a "3 tick" filter would be a quarter-second.
            // OBI-exhaustion exit — disabled in ghost mode before this.
            if position.fill_effective_at(dc.ghost_mode).is_some() {
                let yes_total_depth = ctx.snapshot.yes_bid_depth + ctx.snapshot.yes_ask_depth;
                let no_total_depth  = ctx.snapshot.no_bid_depth  + ctx.snapshot.no_ask_depth;
                let yes_obi = if yes_total_depth > dec!(0) {
                    (ctx.snapshot.yes_bid_depth - ctx.snapshot.yes_ask_depth) / yes_total_depth
                } else { dec!(-1.0) };
                let no_obi = if no_total_depth > dec!(0) {
                    (ctx.snapshot.no_bid_depth - ctx.snapshot.no_ask_depth) / no_total_depth
                } else { dec!(-1.0) };

                let obi_exhausted_now =
                    (is_yes  && yes_obi > dc.momentum_obi_exhaustion_block) ||
                        (!is_yes && no_obi  > dc.momentum_obi_exhaustion_block);

                // Guard 3: start or clear this token's exhaustion clock. Done on every
                // tick regardless of the other guards, so the elapsed time reflects the
                // book rather than how long we happened to be eligible to act on it.
                let now = chrono::Utc::now();
                let exhausted_for_secs = {
                    let mut m = self.obi_exhaust_since.lock().unwrap();
                    if obi_exhausted_now {
                        let since = *m.entry(tok.clone()).or_insert(now);
                        (now - since).num_seconds()
                    } else {
                        m.remove(&tok);
                        -1
                    }
                };

                // Guard 2: mark against mid so the spread is not read as an adverse move.
                // Falls back to bid if the book is crossed or the ask is missing.
                let mark = if ask > bid { (bid + ask) / dec!(2) } else { bid };
                let mid_margin = (mark - avg_entry) / avg_entry;

                // Guard 1: the same hold floor the Reversal exit uses.
                let held_long_enough = secs_held >= dc.momentum_obi_exhaust_min_hold_secs;

                if obi_exhausted_now
                    && held_long_enough
                    && exhausted_for_secs >= dc.momentum_obi_exhaust_persist_secs
                    && mid_margin <= dec!(0)
                {
                    // ── Max adverse move guard for OBI exhaustion exit ─────────────────────
                    // OBIExhaust is intended as an *early* reversal detector (exit before
                    // hitting the normal stop-loss). Without this guard, a late OBI spike
                    // after a large adverse move (e.g. -23% on 2026-06-03 Trade 4) will
                    // still trigger an exit — turning the mechanism into a "we're already
                    // wrecked" exit rather than a protective early one.
                    //
                    // Below this floor the position belongs to the stop-loss instead. The
                    // bound is now a Control Tower knob laddered above the stop loss; it
                    // shipped hardcoded at −8% against an 11% stop, which left the early
                    // exit live only in a band the spread alone could put a fresh fill into.
                    if mid_margin >= dc.momentum_obi_exhaust_max_adverse_pct {
                        let obi_val = if is_yes { yes_obi } else { no_obi };
                        let reason = format!(
                            "MomentumOBIExhaust: bid=${:.4}, obi={:.3}, exhausted={}s, held={}s, mid_profit={:.2}%",
                            bid, obi_val, exhausted_for_secs, secs_held, mid_margin * dec!(100)
                        );
                        return Ok(StrategySignal::Exit { params: exit_params!(), reason, exit_pair: false });
                    }
                    // If we reach here: OBI is exhausted but we're already deep underwater.
                    // Let the normal stop-loss (or catastrophic SL) handle it instead.
                }
            } else {
                // Not yet fill-confirmed — keep no stale clock for this token.
                self.obi_exhaust_since.lock().unwrap().remove(&tok);
                self.reversal_since.lock().unwrap().remove(&tok);
            }

            // ── Resting take-profit — the healthy position's way out ──────────
            // Every exit above declined this tick, so let the position leave by
            // being LIFTED at the target instead of crossing back to the bid for
            // a second taker fee. Only a confirmed fill owns shares that can back
            // a sell order; the patrol re-checks that, places the ask once,
            // no-ops on the re-emission, and pulls it before any FAK above needs
            // the shares — so no tick ever carries both a resting sell and a
            // market sell for the same position.
            //
            // Floored against the ENTRY fee alone. A lift pays no taker fee, so
            // the round-trip floor the FAK take-profit uses would park the ask a
            // full leg higher than this exit needs to clear. Where the two floors
            // differ the ask sits BELOW the FAK trigger, and a bid that reaches
            // the FAK trigger has lifted the ask on its way there; where the base
            // target dominates both, the two sit at the same price and the FAK
            // is the fallback for an ask that failed to rest.
            if dc.momentum_resting_tp_enabled
                && position.fill_effective_at(dc.ghost_mode).is_some()
                && position.shares >= config::MIN_ORDER_SHARES
            {
                let maker_floor = crate::venues::entry_only_fee_pct(avg_entry)
                    * dc.momentum_tp_fee_margin_mult;
                let maker_target = base_target.max(maker_floor);
                if let Some(price) = resting_tp_price(
                    avg_entry, maker_target, bid, dc.momentum_take_profit_ceiling,
                ) {
                    let uncapped = ceil_to_tick_size(avg_entry * (Decimal::ONE + maker_target));
                    resting_tps.push(StrategySignal::MakerRestingExit {
                        params: OrderParams {
                            token_id: tok.clone(),
                            price,
                            shares: position.shares,
                            fee_bps: if tok == exit_market.yes_token { exit_market.yes_fee_bps as u16 } else { exit_market.no_fee_bps as u16 },
                            is_neg_risk: exit_market.is_neg_risk,
                            market_name: exit_market.market_name.clone(),
                            condition_id: exit_market.condition_id.clone(),
                            order_type: TimeInForce::Gtc,
                            post_only: true,
                            ghost_mode: dc.ghost_mode,
                        },
                        reason: format!(
                            "MomentumRestingTP: ask=${:.4} entry=${:.4} target={:+.2}%{}",
                            price, avg_entry, (price - avg_entry) / avg_entry * dec!(100),
                            if price < uncapped { " (capped at the take-profit ceiling)" } else { "" },
                        ),
                    });
                }
            }
        }

        // One signal leaves per tick, so with several healthy positions
        // (reconciliation can put Momentum on both markets) each ask is
        // emitted in turn. The consumer no-ops on the ones it already has
        // resting, so rotation costs nothing and every position gets its ask
        // placed within a few ticks.
        if !resting_tps.is_empty() {
            static ROTATION: AtomicUsize = AtomicUsize::new(0);
            let i = ROTATION.fetch_add(1, Ordering::Relaxed) % resting_tps.len();
            return Ok(resting_tps.swap_remove(i));
        }
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "MomentumStrategy".to_string() }
    fn venue(&self) -> &'static str { "Hourly" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::MOMENTUM_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Gross one-sided" }
}

/// Momentum's trade size: flat at `min_size` unless the operator has turned on
/// Scaled Sizing (`momentum_scaled_sizing_enabled`), in which case it scales with
/// the strength of the triggering move. The switch is the runtime control;
/// `config::ENABLE_KELLY_SIZING` only seeds its default per profile.
pub fn momentum_trade_size(
    scaled:    bool,
    velocity:  rust_decimal::Decimal,
    threshold: rust_decimal::Decimal,
    min_size:  rust_decimal::Decimal,
    max_size:  rust_decimal::Decimal,
) -> rust_decimal::Decimal {
    if scaled { kelly_momentum_size(velocity, threshold, min_size, max_size) } else { min_size }
}

/// Kelly-fractional position sizing for Momentum.
/// Accepts min/max from DynamicConfig so the caller controls the range.
/// Structural params (KELLY_MAX_MULTIPLIER) remain compile-time constants.
pub fn kelly_momentum_size(
    velocity:  rust_decimal::Decimal,
    threshold: rust_decimal::Decimal,
    min_size:  rust_decimal::Decimal,
    max_size:  rust_decimal::Decimal,
) -> rust_decimal::Decimal {
    if threshold <= rust_decimal::Decimal::ZERO { return min_size; }
    let strength = (velocity.abs() / threshold)
        .max(rust_decimal::Decimal::ONE)
        .min(config::MOMENTUM_KELLY_MAX_MULTIPLIER);
    let fraction = (strength - rust_decimal::Decimal::ONE)
        / (config::MOMENTUM_KELLY_MAX_MULTIPLIER - rust_decimal::Decimal::ONE);
    min_size + fraction * (max_size - min_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::dynamic_config::DynamicConfig;

    fn clean_bull_spike() -> SpikeGates {
        SpikeGates {
            bull: true,
            strike: Some((dec!(78100), dec!(78000), dec!(8))),
            ask: dec!(0.65), min_entry: dec!(0.58), max_entry: dec!(0.78), crossing_max: dec!(0.62),
            crossing_inert: false, short_ok: true, accel_ok: true, window_blocks: false, fee_blocks: false,
            obi_adverse: false, obi_exhausted: false, obi_swing: false, drift_blocks: false,
        }
    }

    /// A spike that passes every gate has no blockers; the log names nothing.
    #[test]
    fn spike_blockers_empty_when_every_gate_passes() {
        assert!(spike_blockers(&clean_bull_spike()).is_empty());
    }

    /// The price blocker follows the two entry branches: inside the strike
    /// buffer only the crossing cap applies, beyond it the max entry applies,
    /// and on the wrong side of the strike neither branch can fire.
    #[test]
    fn spike_blockers_price_follows_primary_and_crossing_branches() {
        let mut g = clean_bull_spike();
        g.strike = Some((dec!(78004), dec!(78000), dec!(8)));   // inside buffer
        g.ask = dec!(0.60);
        assert!(spike_blockers(&g).is_empty(), "crossing branch admits $0.60");
        g.ask = dec!(0.65);
        assert_eq!(spike_blockers(&g), vec!["ask above crossing cap (oracle inside strike buffer)"]);

        g.strike = Some((dec!(78100), dec!(78000), dec!(8)));   // beyond buffer
        g.ask = dec!(0.80);
        assert_eq!(spike_blockers(&g), vec!["ask above max entry"]);

        g.strike = Some((dec!(77900), dec!(78000), dec!(8)));   // wrong side
        g.ask = dec!(0.60);
        assert_eq!(spike_blockers(&g), vec!["oracle on wrong side of strike"]);

        // BEAR mirrors it: oracle above strike is the wrong side.
        g.bull = false;
        assert!(spike_blockers(&g).is_empty());
        g.strike = Some((dec!(78100), dec!(78000), dec!(8)));
        assert_eq!(spike_blockers(&g), vec!["oracle on wrong side of strike"]);
    }

    /// Every gate is named, in entry-check order; the daily window only counts
    /// when a strike is known, as in the no-strike entry branch.
    #[test]
    fn spike_blockers_names_every_gate_in_order() {
        let mut g = clean_bull_spike();
        g.ask = dec!(0.50);
        g.short_ok = false; g.accel_ok = false; g.window_blocks = true; g.fee_blocks = true;
        g.obi_adverse = true; g.obi_exhausted = true; g.obi_swing = true; g.drift_blocks = true;
        assert_eq!(spike_blockers(&g), vec![
            "ask below min entry", "1s velocity not confirming", "decelerating", "daily window against",
            "fee-dominated", "OBI adverse", "OBI exhausted", "OBI swing", "10m drift against",
        ]);
        g.strike = None;
        assert!(!spike_blockers(&g).contains(&"daily window against"));
    }

    /// An inert crossing branch is named as such inside the buffer, instead of
    /// hiding behind a cap the operator could never satisfy.
    #[test]
    fn spike_blockers_names_an_inert_crossing_branch() {
        let mut g = clean_bull_spike();
        g.strike = Some((dec!(78004), dec!(78000), dec!(8)));   // inside buffer
        g.ask = dec!(0.60);
        g.crossing_max = dec!(0.52);
        g.crossing_inert = true;
        assert_eq!(spike_blockers(&g), vec!["crossing branch inert (oracle inside strike buffer)"]);
        // Beyond the buffer the primary branch owns the price test, unchanged.
        g.strike = Some((dec!(78100), dec!(78000), dec!(8)));
        assert!(spike_blockers(&g).is_empty());
    }

    /// The production configuration of 2026-09-21 — a 0.52 cap under a 0.58
    /// floor — is inert, and so is a cap the fee gate refuses at every price it
    /// admits; a cap at or above the floor with an admissible price is live.
    /// Fee rates differ by venue (0.06 on Polymarket US, 0.07 elsewhere), so
    /// the fee-dominated case uses a price both rates refuse.
    #[test]
    fn crossing_branch_inert_names_each_dead_configuration() {
        assert_eq!(crossing_branch_inert(dec!(0), dec!(0.58), dec!(0.15), dec!(0.40)).as_deref(), Some("off (cap 0)"));
        assert_eq!(
            crossing_branch_inert(dec!(0.52), dec!(0.58), dec!(0.15), dec!(0.40)).as_deref(),
            Some("cap 0.52 below min entry 0.58"),
        );
        let fee = crossing_branch_inert(dec!(0.45), dec!(0.43), dec!(0.15), dec!(0.40))
            .expect("6.6%+ round trip is over 40% of a 15% target at every price up to $0.45");
        assert!(fee.starts_with("fee-dominated at every price 0.43–0.45"), "{fee}");
        assert_eq!(crossing_branch_inert(dec!(0.62), dec!(0.58), dec!(0.15), dec!(0.40)), None, "balanced: 0.58–0.62 is live");
        assert_eq!(crossing_branch_inert(dec!(0.75), dec!(0.43), dec!(0.20), dec!(0.40)), None, "aggressive: live");
        // A cap above the flat-target floor is judged at the best price of BOTH
        // regimes: 0.69 at the configured target is admitted even though 0.75
        // at the flat 5% target is not.
        assert_eq!(crossing_branch_inert(dec!(0.75), dec!(0.58), dec!(0.15), dec!(0.40)), None);
    }

    /// The configured 0.58–0.78 band is reachable only up to 0.69: from $0.70
    /// the flat 5% target makes the round trip more than 40% of the plan until
    /// the mid-0.80s, which the 0.78 ceiling never reaches. Widen the ceiling
    /// and a second, high interval appears.
    #[test]
    fn effective_entry_band_stops_at_the_flat_target_floor() {
        let band = effective_entry_band(dec!(0.58), dec!(0.78), dec!(0.15), dec!(0.40));
        assert_eq!(band, vec![(dec!(0.58), dec!(0.69))]);
        assert_eq!(format_band(&band), "0.58–0.69");
        let wide = effective_entry_band(dec!(0.58), dec!(0.90), dec!(0.15), dec!(0.40));
        assert_eq!(wide.len(), 2, "{wide:?}");
        assert_eq!(wide[0], (dec!(0.58), dec!(0.69)));
        assert!(wide[1].0 >= dec!(0.83) && wide[1].0 <= dec!(0.86), "{wide:?}");
        assert_eq!(wide[1].1, dec!(0.90));
        assert_eq!(format_band(&[]), "none");
        // A band the fee gate admits in full reports exactly the configured pair.
        assert_eq!(format_band(&effective_entry_band(dec!(0.58), dec!(0.69), dec!(0.15), dec!(0.40))), "0.58–0.69");
    }

    /// Each reason logs once per interval, independently: two reasons flapping
    /// every tick log once each, not once per flip.
    #[test]
    fn gate_log_throttle_is_per_reason() {
        let a = "momtest_flap";
        assert!(momentum_gate_log_permitted(a, "velocity below trigger", 300));
        assert!(momentum_gate_log_permitted(a, "spike BULL | OBI adverse", 300));
        for _ in 0..10 {
            assert!(!momentum_gate_log_permitted(a, "velocity below trigger", 300));
            assert!(!momentum_gate_log_permitted(a, "spike BULL | OBI adverse", 300));
        }
        assert!(momentum_gate_log_permitted(a, "spike BULL | OBI swing", 300), "a new blocker set logs at once");
        assert!(momentum_gate_log_permitted("momtest_other_asset", "velocity below trigger", 300));
        assert!(momentum_gate_log_permitted(a, "velocity below trigger", 0), "interval elapsed");
    }

    /// The take-profit target must clear the round trip it has to pay for.
    ///
    /// Venue fees are entry-price dependent — `2 × rate × (1 − entry)` of notional —
    /// so a flat percentage target is below break-even on cheap entries. With the
    /// balanced 10% target every entry under $0.286 books a "profit" that is really a
    /// net loss; the conservative 6% target does so under $0.571, i.e. across almost
    /// the whole of its own $0.20–$0.50 permitted entry band. The floor in
    /// `evaluate_exit` exists to stop MomentumTP meaning "we lost money on plan".
    #[test]
    fn take_profit_target_clears_round_trip_fees_across_entry_range() {
        let dc = DynamicConfig::default();
        // Walk the permitted entry band in 1¢ steps.
        let mut price = dc.momentum_min_entry_price;
        while price <= dc.momentum_max_entry_price {
            let fee = crate::venues::round_trip_fee_pct(price);
            let base = base_take_profit(price, dc.momentum_target_profit_pct);
            let effective = base.max(fee * dc.momentum_tp_fee_margin_mult);
            assert!(
                effective > fee,
                "entry ${price}: effective TP {effective} does not clear round-trip fee {fee}",
            );
            price += dec!(0.01);
        }
    }

    /// A "catastrophic" stop tighter than the normal stop is not an emergency floor,
    /// it is a stricter stop that fires first — and it fires inside the
    /// fill-confirmation window, where a fresh taker fill is underwater by the spread
    /// alone. All three profiles shipped inverted (balanced: 0.06 catastrophic against
    /// a 0.11 stop), so a 7%-spread entry cleared the "catastrophic" bar on tick one.
    #[test]
    fn catastrophic_stop_is_never_tighter_than_the_normal_stop() {
        let dc = DynamicConfig::default();
        assert!(
            dc.momentum_catastrophic_sl_pct >= dc.momentum_stop_loss_pct,
            "catastrophic SL {} is tighter than stop loss {}",
            dc.momentum_catastrophic_sl_pct, dc.momentum_stop_loss_pct,
        );
    }

    /// The OBI-exhaustion exit is an *early* reversal detector: it only earns its keep
    /// if it can fire before the stop-loss would. Its max-adverse bound must therefore
    /// sit beyond the stop, or the branch is dead code. It shipped at −8% against an
    /// 11% stop, leaving it live only in a band the spread alone could put a fresh
    /// fill into — which is exactly how it fired 5s into the 2026-08-19 10:55 ET trade.
    #[test]
    fn obi_exhaust_window_extends_past_the_stop_loss() {
        let dc = DynamicConfig::default();
        assert!(
            dc.momentum_obi_exhaust_max_adverse_pct < -dc.momentum_stop_loss_pct,
            "OBI exhaust floor {} does not reach past stop loss {}",
            dc.momentum_obi_exhaust_max_adverse_pct, dc.momentum_stop_loss_pct,
        );
    }

    /// Every other Momentum exit waits before it may fire; this one did not, and
    /// closed a correct position five seconds after entry on a loss that was entirely
    /// bid/ask spread. OBI exhaustion is a reversal signal, so it must wait at least
    /// as long as the fill-confirmation window it was previously escaping.
    #[test]
    fn obi_exhaust_waits_for_fill_settle_before_firing() {
        let dc = DynamicConfig::default();
        assert!(
            dc.momentum_obi_exhaust_min_hold_secs >= config::MOMENTUM_FILL_CONFIRM_MIN_HOLD_SECS,
            "OBI exhaust min hold {}s is inside the {}s fill-confirmation window",
            dc.momentum_obi_exhaust_min_hold_secs, config::MOMENTUM_FILL_CONFIRM_MIN_HOLD_SECS,
        );
        assert!(
            dc.momentum_obi_exhaust_persist_secs > 0,
            "a single OBI sample must not arm the exit",
        );
        // Persistence has to fit inside the hold floor, or it can never be the
        // binding constraint and the noise filter is decorative.
        assert!(
            dc.momentum_obi_exhaust_persist_secs < dc.momentum_obi_exhaust_min_hold_secs,
            "persistence window {}s does not fit inside the {}s hold floor",
            dc.momentum_obi_exhaust_persist_secs, dc.momentum_obi_exhaust_min_hold_secs,
        );
    }

    /// The trade this work item came from, replayed through the entry gate.
    ///
    /// 2026-09-09 11:00 ET, aggressive profile: YES at $0.53 with a 15% target.
    /// The round trip was 6.58% of notional — 44% of the plan — and it fired the
    /// only loss of the day with the fee at 116% of the gross loss. Under the
    /// shipped 0.40 cap that entry is refused on the 0.07 venues (Polymarket
    /// International, Kalshi), and the refusal names the knob that owns it.
    ///
    /// The verdict follows the venue's rate, not the venue's name: on Polymarket
    /// US the schedule is 0.06, the same trade is 5.64% of notional — 38% of the
    /// plan — and it passes the 0.40 cap by two points. That is the arithmetic,
    /// pinned here so the gate is never mistaken for a venue switch. (Until the
    /// fix of 2026-09-09 that build carried a zero rate and this gate was inert.)
    #[test]
    fn the_2026_09_09_entry_is_refused_where_the_fee_dominates_the_plan() {
        let fee = crate::venues::round_trip_fee_pct(dec!(0.53));
        let share = fee / dec!(0.15);
        let verdict = crate::vipers::fee_dominated_entry(dec!(0.53), dec!(0.15), dec!(0.40));
        if share > dec!(0.40) {
            let reason = verdict.as_deref().unwrap_or_else(|| panic!("a {share:.3} fee share must be refused under a 0.40 cap"));
            assert!(reason.contains("fee-dominated"), "reason must carry the advisor's needle: {reason}");
            assert!(reason.contains("max 40%"), "reason must show the cap it broke: {reason}");
        } else {
            assert_eq!(verdict, None, "a {share:.3} fee share is under the cap and must pass");
        }
        // The venue's own rate decides which branch ran — and every shipped venue
        // charges one, so the trade is never free.
        assert!(fee > Decimal::ZERO, "every shipped venue charges a taker fee; got {fee}");
        let rate = crate::venues::taker_fee_rate();
        if rate >= dec!(0.07) {
            assert!(verdict.is_some(), "at {rate} the 2026-09-09 entry is refused");
        }
        if rate == dec!(0.06) {
            assert!(verdict.is_none(), "at 0.06 the same entry is 38% of the plan and passes");
        }
        // A cap that admits the fee lets the trade through on every venue.
        assert_eq!(crate::vipers::fee_dominated_entry(dec!(0.53), dec!(0.15), dec!(0.50)), None);
    }

    /// The gate is a share-of-plan test, so it follows the fee curve: the same
    /// 15% target that is fee-dominated at $0.53 is comfortably clear at $0.90,
    /// where the round trip is 1.4%. And a plan with no target at all has nothing
    /// for a fee to be a share of — refused wherever a fee is charged.
    #[test]
    fn fee_gate_follows_the_fee_curve_and_refuses_a_targetless_plan() {
        assert_eq!(crate::vipers::fee_dominated_entry(dec!(0.90), dec!(0.15), dec!(0.40)), None);
        let charged = crate::venues::round_trip_fee_pct(dec!(0.53)) > Decimal::ZERO;
        let no_plan = crate::vipers::fee_dominated_entry(dec!(0.53), Decimal::ZERO, dec!(0.40));
        assert_eq!(no_plan.is_some(), charged);
    }

    /// The entry gate and the exit's take-profit must measure the same plan.
    /// Above $0.70 the exit swaps the configured target for a flat 5%; if the gate
    /// kept using the configured 15% it would admit a $0.75 entry as "23% fee
    /// share" while the exit chased a target the fee is 70% of.
    #[test]
    fn entry_gate_and_exit_agree_on_the_take_profit_plan() {
        assert_eq!(base_take_profit(dec!(0.69), dec!(0.15)), dec!(0.15));
        assert_eq!(base_take_profit(dec!(0.70), dec!(0.15)), dec!(0.05));
        assert_eq!(base_take_profit(dec!(0.85), dec!(0.15)), dec!(0.05));
    }

    /// The shipped cap must keep the fee a minority of the plan. At 0.5 and
    /// above the gate would admit the trade this item came from (44%).
    #[test]
    fn shipped_fee_share_cap_keeps_the_fee_a_minority_of_the_target() {
        let dc = DynamicConfig::default();
        assert!(dc.momentum_max_fee_to_target_ratio > Decimal::ZERO, "a zero cap refuses every fee-charging entry");
        assert!(
            dc.momentum_max_fee_to_target_ratio < dec!(0.5),
            "cap {} lets the fee take half the plan or more",
            dc.momentum_max_fee_to_target_ratio,
        );
    }

    /// A single reversed reading must never satisfy the persistence rule, and a
    /// normal reading in the middle of a run must reset it.
    #[test]
    fn reversal_persistence_needs_an_unbroken_run() {
        let mut clocks = HashMap::new();
        let tok = MarketId::new("tok-yes");
        let t0 = chrono::Utc::now();
        let sec = |s: i64| t0 + chrono::Duration::seconds(s);

        assert_eq!(reversal_persisted_secs(&mut clocks, &tok, true, sec(0)), 0, "first reading starts the clock at zero");
        assert_eq!(reversal_persisted_secs(&mut clocks, &tok, true, sec(4)), 4);
        assert_eq!(reversal_persisted_secs(&mut clocks, &tok, true, sec(9)), 9);
        // One normal reading breaks the run and drops the clock.
        assert_eq!(reversal_persisted_secs(&mut clocks, &tok, false, sec(10)), -1);
        assert!(!clocks.contains_key(&tok), "a broken run must not leave a primed clock behind");
        // The next reversed reading starts over.
        assert_eq!(reversal_persisted_secs(&mut clocks, &tok, true, sec(11)), 0);
    }

    /// `velocity` is a 5s window, so one opposing tick reads as a reversal for
    /// up to five seconds by itself. A persistence requirement at or under the
    /// window confirms nothing — every profile must demand more than one window,
    /// or the 2026-09-09 single-reading exit fires exactly as before.
    #[test]
    fn reversal_persistence_outlives_a_single_velocity_window() {
        let dc = DynamicConfig::default();
        assert!(
            dc.momentum_reversal_persist_secs > config::MOMENTUM_WINDOW_SECS as i64,
            "persistence {}s does not outlast the {}s velocity window",
            dc.momentum_reversal_persist_secs, config::MOMENTUM_WINDOW_SECS,
        );
        assert!(dc.momentum_reversal_min_hold_secs > 0, "the reversal exit must not fire on tick one");
        assert!(dc.momentum_reversal_ratio > Decimal::ZERO, "a zero ratio calls every tick a reversal");
    }

    /// Round-trip fee is steeply price-dependent — the whole reason a flat
    /// take-profit target fails. Pins the shape so a venue-fee refactor cannot
    /// quietly flatten it.
    ///
    /// The assertion is conditional on the venue actually charging a taker fee.
    /// Every venue DRADIS ships does today (Polymarket US at 0.06 since the fix
    /// of 2026-09-09; it was carried as zero before that), so the zero branch is
    /// kept only so a genuinely free venue would pin the property it should hold.
    #[test]
    fn round_trip_fee_falls_as_entry_price_rises() {
        let cheap = crate::venues::round_trip_fee_pct(dec!(0.20));
        let mid   = crate::venues::round_trip_fee_pct(dec!(0.43));
        let rich  = crate::venues::round_trip_fee_pct(dec!(0.80));

        if mid > Decimal::ZERO {
            // Quadratic-fee venue (Polymarket intl, Kalshi): cheap entries cost more.
            assert!(cheap > mid && mid > rich, "fee curve is not decreasing: {cheap} / {mid} / {rich}");
        } else {
            // Zero-fee venue: flat at zero everywhere, never negative.
            assert!(
                cheap == Decimal::ZERO && rich == Decimal::ZERO,
                "zero-fee venue reported a non-zero fee: {cheap} / {mid} / {rich}",
            );
        }

        // Degenerate prices carry no fee rather than a negative one, on every venue.
        assert_eq!(crate::venues::round_trip_fee_pct(dec!(0)), Decimal::ZERO);
        assert_eq!(crate::venues::round_trip_fee_pct(dec!(1)), Decimal::ZERO);
    }

    // ── Resting take-profit ──────────────────────────────────────────────────

    /// The ask sits at the target rounded up to the tick, capped at the
    /// take-profit ceiling, or nowhere: never at or through the bid (a
    /// post-only sell there is rejected and the FAK rules own that case),
    /// never at $1.00, never without a target.
    #[test]
    fn the_resting_ask_sits_at_the_target_capped_at_the_ceiling_or_nowhere() {
        // The 2026-09-09 entry on the aggressive plan: $0.53 × 1.25 = $0.6625 → $0.67.
        assert_eq!(resting_tp_price(dec!(0.53), dec!(0.25), dec!(0.55), dec!(0.90)), Some(dec!(0.67)));
        // Bid at or through the ask: a post-only sell would cross, so nothing rests.
        assert_eq!(resting_tp_price(dec!(0.53), dec!(0.25), dec!(0.67), dec!(0.90)), None);
        assert_eq!(resting_tp_price(dec!(0.53), dec!(0.25), dec!(0.70), dec!(0.90)), None);
        // Above $0.70 the plan is a flat 5%: $0.88 × 1.05 = $0.924 → $0.93, capped at the ceiling.
        assert_eq!(resting_tp_price(dec!(0.88), dec!(0.05), dec!(0.89), dec!(0.90)), Some(dec!(0.90)));
        // Bid already at the ceiling: the ceiling FAK sells it, not a resting ask.
        assert_eq!(resting_tp_price(dec!(0.88), dec!(0.05), dec!(0.90), dec!(0.90)), None);
        // No ceiling configured: the target stands, but never $1.00 or beyond.
        assert_eq!(resting_tp_price(dec!(0.88), dec!(0.05), dec!(0.89), Decimal::ZERO), Some(dec!(0.93)));
        assert_eq!(resting_tp_price(dec!(0.98), dec!(0.05), dec!(0.97), Decimal::ZERO), None);
        // No entry or no target: nothing to rest.
        assert_eq!(resting_tp_price(Decimal::ZERO, dec!(0.25), dec!(0.55), dec!(0.90)), None);
        assert_eq!(resting_tp_price(dec!(0.53), Decimal::ZERO, dec!(0.55), dec!(0.90)), None);
    }

    /// A taker exit nets the fee it pays. On a fee venue a bid one tick above
    /// the entry is a loss once the exit fee comes off, which the 1¢ offset
    /// alone used to call a profit; on a zero-fee venue the rule is unchanged.
    #[test]
    fn a_taker_exit_is_net_of_the_fee_it_pays() {
        let entry = dec!(0.53);
        let fee_at_54 = crate::venues::taker_fee_per_share(dec!(0.54));
        let one_tick = taker_exit_net_margin(entry, dec!(0.54));
        if fee_at_54 > config::SELL_PRICE_OFFSET {
            assert!(one_tick < Decimal::ZERO, "a 1¢ move must not read as profit against a {fee_at_54} fee: {one_tick}");
            assert!(taker_exit_net_margin(entry, dec!(0.56)) > Decimal::ZERO, "three ticks clears the fee");
        } else {
            assert_eq!(one_tick, (dec!(0.54) - config::SELL_PRICE_OFFSET - entry) / entry);
        }
        assert_eq!(taker_exit_net_margin(Decimal::ZERO, dec!(0.54)), Decimal::ZERO);
    }

    // ── evaluate_exit, driven end to end ─────────────────────────────────────

    use crate::state::{MarketConfig, MarketSnapshot, Position, PositionMap};
    use std::sync::Arc;

    fn market(yes: &str, no: &str, name: &str, cid: &str) -> MarketConfig {
        MarketConfig {
            yes_token: MarketId::new(yes), no_token: MarketId::new(no),
            market_name: name.to_string(),
            market_close_time: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            strike_price: Some(dec!(65000)), is_neg_risk: false,
            condition_id: cid.to_string(), yes_fee_bps: 0, no_fee_bps: 0,
        }
    }

    /// A quiet hourly book with the given YES touch: no velocity, so no
    /// reversal reads and the decay exit's fade test holds (a mark past the
    /// decay fee floor would decay out), balanced depth so OBI is neutral.
    fn book(yes_bid: Decimal, yes_ask: Decimal) -> MarketSnapshot {
        MarketSnapshot {
            yes_bid, yes_bid_depth: dec!(100),
            yes_ask, yes_ask_depth: dec!(100),
            no_bid: dec!(1) - yes_ask, no_bid_depth: dec!(100),
            no_ask: dec!(1) - yes_bid, no_ask_depth: dec!(100),
            yes_bid_depth_total: dec!(100), yes_ask_depth_total: dec!(100),
            no_bid_depth_total: dec!(100), no_ask_depth_total: dec!(100),
            oracle_price: dec!(65000),
            velocity: dec!(0), velocity_1s: dec!(0), acceleration: dec!(0),
            funding_rate: dec!(0), oracle_drift_60m: dec!(0),
            oracle_drift_10m: dec!(0), hist_vol: dec!(0.003),
            institutional_pulse: dec!(0), tide_coherence: dec!(0),
            tradfi_velocity: dec!(0), macro_coherence: dec!(0),
            vix_proxy: dec!(0), vix_velocity: dec!(0),
            oi_delta_pct: dec!(0), cvd_ratio: dec!(1),
            secs_to_expiry: 3600, timestamp: chrono::Utc::now(),
        }
    }

    /// The balanced plan, pinned explicitly so the test does not depend on
    /// whichever profile the gitignored config.rs happens to hold.
    fn plan() -> DynamicConfig {
        let mut dc = DynamicConfig::default();
        dc.enable_momentum = true;
        dc.ghost_mode = false;
        dc.momentum_target_profit_pct = dec!(0.10);
        dc.momentum_stop_loss_pct = dec!(0.11);
        dc.momentum_catastrophic_sl_pct = dec!(0.17);
        dc.momentum_tp_fee_margin_mult = dec!(1.35);
        dc.momentum_take_profit_ceiling = dec!(0.90);
        dc.momentum_threshold_pct = dec!(0.001);
        dc.momentum_reversal_ratio = dec!(0.75);
        dc.momentum_reversal_min_hold_secs = 45;
        dc.momentum_reversal_persist_secs = 8;
        dc.momentum_resting_tp_enabled = true;
        dc.momentum_decay_exit_fraction = dec!(0.30);
        dc.momentum_decay_fee_margin_mult = dec!(1.0);
        dc
    }

    fn ctx(snapshot: MarketSnapshot, dc: DynamicConfig) -> StrategyContext {
        StrategyContext {
            squadron_id: "btc-open".to_string(),
            market: market("h-yes", "h-no", "Bitcoin Up or Down - September 9, 12PM ET", "cid-hourly"),
            snapshot,
            positions: Arc::new(tokio::sync::Mutex::new(PositionMap::new())),
            session_pnl: dec!(0), starting_collateral: dec!(100),
            available_collateral: dec!(100),
            crypto_filter: "btc".to_string(),
            market_started_at: chrono::Utc::now() - chrono::Duration::minutes(10),
            maker_snapshot: None,
            maker_market: None,
            dynamic_config: Arc::new(dc),
            arb_market_lockouts: None,
            sports: None,
        }
    }

    /// Hold `shares` YES at `entry`, opened `secs_ago`, confirmed or not.
    async fn hold_yes(c: &StrategyContext, entry: Decimal, shares: Decimal, secs_ago: i64, confirmed: bool) -> PositionKey {
        let opened = chrono::Utc::now() - chrono::Duration::seconds(secs_ago);
        let key = PositionKey::new(c.squadron_id.clone(), "MomentumStrategy", c.market.yes_token.clone());
        c.positions.lock().await.insert(key.clone(), Position {
            shares, avg_entry: entry, opened_at: opened,
            close_time: c.market.market_close_time,
            market_name: c.market.market_name.clone(),
            pair_token_id: c.market.yes_token.clone(),
            fill_confirmed_at: if confirmed { Some(opened) } else { None },
            paired_leg_token_id: None,
            entry_fee: dec!(0.1597),
        });
        key
    }

    /// A confirmed position with no harder exit pending rests its take-profit:
    /// a post-only GTC ask at entry × (1 + target) rounded up to the tick, for
    /// the whole position. Re-emitted every tick (the consumer is idempotent).
    /// The knob turns it off, and an unconfirmed fill owns no shares to sell.
    #[tokio::test]
    async fn a_healthy_confirmed_position_rests_its_take_profit() {
        // Marked flat at the bid: nothing to take, nothing to stop, nothing to decay.
        let c = ctx(book(dec!(0.53), dec!(0.55)), plan());
        let key = hold_yes(&c, dec!(0.53), dec!(10), 200, true).await;

        let strat = MomentumStrategyImpl::default();
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        let StrategySignal::MakerRestingExit { params, reason } = sig else {
            panic!("a healthy confirmed position must rest its take-profit, got {sig:?}");
        };
        assert_eq!(params.token_id, c.market.yes_token);
        // $0.53 × 1.10 = $0.583 → $0.59. The entry-leg floor (4.4% at $0.53 on
        // a fee venue, zero elsewhere) sits under the 10% plan on every venue.
        assert_eq!(params.price, dec!(0.59));
        assert_eq!(params.shares, dec!(10));
        assert!(params.post_only, "the ask must be post-only or it pays the taker fee it exists to avoid");
        assert_eq!(params.order_type, TimeInForce::Gtc);
        assert!(!params.ghost_mode);
        assert!(reason.starts_with("MomentumRestingTP: ask=$0.5900 entry=$0.5300 target=+11.32%"), "{reason}");
        assert!(!reason.contains("capped"), "{reason}");

        let again = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        assert!(matches!(again, StrategySignal::MakerRestingExit { .. }), "{again:?}");

        // Knob off: back to the taker take-profit, and at a flat mark it has nothing to do.
        let mut off = plan();
        off.momentum_resting_tp_enabled = false;
        let c_off = ctx(book(dec!(0.53), dec!(0.55)), off);
        hold_yes(&c_off, dec!(0.53), dec!(10), 200, true).await;
        let sig = strat.evaluate_exit(&c_off).await.expect("exit evaluation runs");
        assert!(matches!(sig, StrategySignal::NoSignal), "knob off must rest nothing, got {sig:?}");

        // Fill not yet confirmed: no shares to back a sell order.
        let mut pending = c.positions.lock().await.get(&key).cloned().expect("held");
        pending.fill_confirmed_at = None;
        c.positions.lock().await.insert(key.clone(), pending);
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        assert!(matches!(sig, StrategySignal::NoSignal), "an unconfirmed fill must rest nothing, got {sig:?}");
    }

    /// Above $0.70 the plan is a flat 5% and the ask can round past the
    /// take-profit ceiling; it is capped there, and the reason says so.
    #[tokio::test]
    async fn the_resting_ask_is_capped_at_the_take_profit_ceiling() {
        let c = ctx(book(dec!(0.88), dec!(0.89)), plan());
        hold_yes(&c, dec!(0.88), dec!(10), 200, true).await;
        let sig = MomentumStrategyImpl::default().evaluate_exit(&c).await.expect("exit evaluation runs");
        let StrategySignal::MakerRestingExit { params, reason } = sig else {
            panic!("expected a resting ask, got {sig:?}");
        };
        assert_eq!(params.price, dec!(0.90));
        assert!(reason.contains("capped at the take-profit ceiling"), "{reason}");
    }

    /// The hard exits always win the tick, and the patrol pulls the ask before
    /// their FAK needs the shares. A bid through the taker target takes with a
    /// FAK (the fallback for an ask that failed to rest); a mark past the stop
    /// after the min-hold stops out. Neither offers a resting signal.
    #[tokio::test]
    async fn a_fak_exit_wins_the_tick_over_the_resting_ask() {
        let strat = MomentumStrategyImpl::default();

        // +13.2% at the bid clears the 10% plan and its round-trip floor.
        let c = ctx(book(dec!(0.60), dec!(0.62)), plan());
        hold_yes(&c, dec!(0.53), dec!(10), 200, true).await;
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        let StrategySignal::Exit { params, reason, .. } = sig else {
            panic!("a bid through the target must take with a FAK, got {sig:?}");
        };
        assert_eq!(params.price, dec!(0.60));
        assert!(!params.post_only && params.order_type == TimeInForce::Fak, "a take-profit FAK crosses");
        assert!(reason.starts_with("MomentumTP: bid=$0.6000"), "{reason}");

        // −15% past the 120s min-hold: the stop, not an ask.
        let c = ctx(book(dec!(0.45), dec!(0.47)), plan());
        hold_yes(&c, dec!(0.53), dec!(10), 200, true).await;
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        let StrategySignal::Exit { params, reason, .. } = sig else {
            panic!("a −15% mark past the min-hold must stop out, got {sig:?}");
        };
        assert_eq!(params.price, dec!(0.45));
        assert!(!params.post_only && params.order_type == TimeInForce::Fak, "a stop crosses");
        assert!(reason.starts_with("MomentumSL: bid=$0.4500"), "{reason}");
    }

    /// Start a token's catastrophic-floor clock `secs_ago`, as a breach that has
    /// already persisted that long would have left it.
    fn floor_breached_since(strat: &MomentumStrategyImpl, token: &MarketId, secs_ago: i64) {
        strat.catastrophic_since.lock().unwrap()
            .insert(token.clone(), chrono::Utc::now() - chrono::Duration::seconds(secs_ago));
    }

    /// 2026-09-15 14:29 ET, trade 37: YES bought at $0.63 into a spike that
    /// reversed; 12 s in the bid was $0.52 (−17.46%), past the 17% catastrophic
    /// floor, and nothing exited until the stop's 120 s hold ran out at $0.31.
    /// The floor must act inside that hold, confirmed or not, once the breach has
    /// persisted; a single reading must not sell, a recovery resets the clock, an
    /// ordinary dip inside the hold still waits, and an empty bid never triggers it.
    #[tokio::test]
    async fn the_catastrophic_floor_exits_inside_the_stop_hold() {
        let mut dc = plan();
        dc.momentum_catastrophic_persist_secs = 2;

        // Confirmed fill, 12 s held, bid $0.52 against a $0.63 entry. The first
        // reading past the floor only starts the clock.
        let strat = MomentumStrategyImpl::default();
        let c = ctx(book(dec!(0.52), dec!(0.54)), dc.clone());
        hold_yes(&c, dec!(0.63), dec!(7.11), 12, true).await;
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        assert!(!matches!(sig, StrategySignal::Exit { .. }), "one reading past the floor must not sell, got {sig:?}");
        assert!(strat.catastrophic_since.lock().unwrap().contains_key(&c.market.yes_token), "the breach must start the clock");

        // Once the breach has persisted past the setting, it sells at the bid.
        floor_breached_since(&strat, &c.market.yes_token, 3);
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        let StrategySignal::Exit { params, reason, .. } = sig else {
            panic!("a −17.5% mark persisting past the floor must exit, got {sig:?}");
        };
        assert_eq!(params.price, dec!(0.52));
        assert_eq!(params.shares, dec!(7.11));
        assert!(!params.post_only && params.order_type == TimeInForce::Fak, "the floor crosses");
        assert!(reason.starts_with("MomentumCatastrophicSL: bid=$0.5200, loss=-17.46%, held="), "{reason}");
        assert!(reason.contains("persisted=3s | ask=$0.5400 spread=$0.02 bid_depth=100 age="), "{reason}");

        // A reading back above the floor resets the clock, so the next breach starts over.
        let strat = MomentumStrategyImpl::default();
        let c = ctx(book(dec!(0.55), dec!(0.57)), dc.clone());
        hold_yes(&c, dec!(0.63), dec!(7.11), 12, true).await;
        floor_breached_since(&strat, &c.market.yes_token, 3);
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        assert!(!matches!(sig, StrategySignal::Exit { .. }), "a −12.7% dip above the floor must wait out the stop hold, got {sig:?}");
        assert!(!strat.catastrophic_since.lock().unwrap().contains_key(&c.market.yes_token), "a recovery must reset the clock");

        // Unconfirmed fill, 5 s held, breach persisted: the escape the fill-confirmation branch promises.
        let strat = MomentumStrategyImpl::default();
        let c = ctx(book(dec!(0.52), dec!(0.54)), dc.clone());
        hold_yes(&c, dec!(0.63), dec!(7.11), 5, false).await;
        floor_breached_since(&strat, &c.market.yes_token, 3);
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        let StrategySignal::Exit { reason, .. } = sig else {
            panic!("an unconfirmed fill past the floor must exit too, got {sig:?}");
        };
        assert!(reason.starts_with("MomentumCatastrophicSL: bid=$0.5200"), "{reason}");

        // An empty bid reads as −100% but is not a price: no exit of any kind.
        let strat = MomentumStrategyImpl::default();
        let c = ctx(book(dec!(0), dec!(0.54)), dc.clone());
        hold_yes(&c, dec!(0.63), dec!(7.11), 200, true).await;
        floor_breached_since(&strat, &c.market.yes_token, 3);
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        assert!(!matches!(sig, StrategySignal::Exit { .. }), "a zero bid must not trigger any exit, got {sig:?}");

        // Past the stop's hold at −51% (the fill trade 37 took): a fresh breach is
        // the ordinary stop's; a persisted one is the floor's.
        let strat = MomentumStrategyImpl::default();
        let c = ctx(book(dec!(0.31), dec!(0.33)), dc.clone());
        hold_yes(&c, dec!(0.63), dec!(7.11), 123, true).await;
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        let StrategySignal::Exit { reason, .. } = sig else { panic!("a −51% mark past the hold must exit, got {sig:?}"); };
        assert!(reason.starts_with("MomentumSL: bid=$0.3100"), "{reason}");
        floor_breached_since(&strat, &c.market.yes_token, 3);
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        let StrategySignal::Exit { reason, .. } = sig else { panic!("a persisted −51% mark must exit, got {sig:?}"); };
        assert!(reason.starts_with("MomentumCatastrophicSL: bid=$0.3100"), "{reason}");

        // Persistence 0 fires on the first reading.
        let mut dc0 = plan();
        dc0.momentum_catastrophic_persist_secs = 0;
        let strat = MomentumStrategyImpl::default();
        let c = ctx(book(dec!(0.52), dec!(0.54)), dc0);
        hold_yes(&c, dec!(0.63), dec!(7.11), 12, true).await;
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        assert!(matches!(sig, StrategySignal::Exit { .. }), "persistence 0 must fire on the first reading, got {sig:?}");
    }

    /// The decay exit reads "the move is spent" on the 5 s window the trigger
    /// uses, and it may only preempt the resting ask with a gain that leaves the
    /// round trip at least break-even. 2026-09-15 BTC replay: it took 30 of 42
    /// exits on a 1 s velocity test that held almost always, banked +5.4% of
    /// stake gross and paid 92% of it in fees, and it booked losses of the entry
    /// fee on positions that were ahead. Margin 0 restores that old bar.
    #[tokio::test]
    async fn the_decay_exit_needs_a_faded_move_and_a_round_trip_that_clears_both_fees() {
        let strat = MomentumStrategyImpl::default();

        // Still running: 5 s velocity at twice the $65 threshold, bid well
        // above entry. The move is not spent, so the ask keeps working.
        let mut running = book(dec!(0.67), dec!(0.69));
        running.velocity = dec!(130);
        let c = ctx(running, plan());
        hold_yes(&c, dec!(0.63), dec!(7.94), 30, true).await;
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        assert!(matches!(sig, StrategySignal::MakerRestingExit { .. }), "a running move must not decay, got {sig:?}");

        // Faded (velocity 0) and +6.3% at the bid: net of the exit fee that
        // clears the entry-leg fee with room, so the gain is banked.
        let c = ctx(book(dec!(0.67), dec!(0.69)), plan());
        hold_yes(&c, dec!(0.63), dec!(7.94), 30, true).await;
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        let StrategySignal::Exit { params, reason, .. } = sig else {
            panic!("a faded move with the round trip covered must decay out, got {sig:?}");
        };
        assert_eq!(params.price, dec!(0.67));
        assert!(!params.post_only && params.order_type == TimeInForce::Fak, "decay crosses");
        assert!(reason.starts_with("MomentumDecay: bid=$0.6700, profit=6.34%, net="), "{reason}");
        assert!(reason.contains("v5=0.00 vs fade 19.50"), "{reason}");

        // Faded but only a hair above entry net of the exit fee (+3.2% at the
        // bid, the exit fee alone is about 2.5%): the old bar sold here and
        // booked a loss of the entry fee. Now the position keeps its free lift.
        let c = ctx(book(dec!(0.65), dec!(0.67)), plan());
        hold_yes(&c, dec!(0.63), dec!(7.94), 30, true).await;
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        assert!(matches!(sig, StrategySignal::MakerRestingExit { .. }), "a gain under the decay fee floor must rest, not decay, got {sig:?}");

        // Margin 0 is the old bar: any gain net of the exit fee alone sells.
        let mut dc0 = plan();
        dc0.momentum_decay_fee_margin_mult = dec!(0);
        let c = ctx(book(dec!(0.65), dec!(0.67)), dc0);
        hold_yes(&c, dec!(0.63), dec!(7.94), 30, true).await;
        let sig = strat.evaluate_exit(&c).await.expect("exit evaluation runs");
        let StrategySignal::Exit { reason, .. } = sig else { panic!("margin 0 must restore the old bar, got {sig:?}"); };
        assert!(reason.starts_with("MomentumDecay: bid=$0.6500"), "{reason}");
    }

    /// Scaled Sizing off: every trade is Min Size, however strong the move.
    /// On: Min Size at the threshold, Max Size at the Kelly cap and beyond.
    #[test]
    fn scaled_sizing_is_a_runtime_switch() {
        let (min, max) = (dec!(5), dec!(25));
        let threshold = dec!(77);
        assert_eq!(momentum_trade_size(false, dec!(273), threshold, min, max), min);
        assert_eq!(momentum_trade_size(false, threshold * dec!(100), threshold, min, max), min);
        assert_eq!(momentum_trade_size(true, threshold, threshold, min, max), min);
        assert_eq!(momentum_trade_size(true, threshold * config::MOMENTUM_KELLY_MAX_MULTIPLIER, threshold, min, max), max);
        assert_eq!(momentum_trade_size(true, threshold * dec!(100), threshold, min, max), max);
        let mid = momentum_trade_size(true, threshold * dec!(1.5), threshold, min, max);
        assert!(mid > min && mid < max, "a move between the threshold and the cap sizes between min and max, got {mid}");
    }
}
