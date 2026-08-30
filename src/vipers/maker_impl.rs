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

/// Maker Strategy - Two-Sided Market Making
///
/// Posts passive resting bids on BOTH YES and NO simultaneously, earning:
///   1. The spread when positions fill and converge to take-profit.
///   2. Daily USDC rebates from Polymarket's Maker Rebates program on every fill.
///
/// This version is strictly tied to the Window/Daily venue via a Fee Gate.
///
/// Trade Velocity / Taker-Flow Filter (Phase 9):
///   - evaluate_entry tracks bid-depth drain over a 1.5s window to suppress new
///     maker bids when takers are actively sweeping one side of the book.
///   - evaluate_exit reacts to an open position's book OBI falling below
///     MAKER_TOXIC_FLOW_EXIT_OBI, meaning the ask side has grown much larger
///     than the bid side — a book turn. The reaction is asymmetric by design:
///       * an UNFILLED resting quote is pulled immediately (cancelling is free);
///       * a CONFIRMED FILL additionally requires MAKER_TOXIC_MIN_HOLD_SECS,
///         a real adverse price move of MAKER_TOXIC_MIN_ADVERSE_PCT, and
///         MAKER_TOXIC_OBI_CONFIRM_TICKS consecutive breaches before exiting.
///     Without those confirmations the exit fires on the OBI dip caused by our
///     OWN quote being lifted and pays the spread to realize a loss the market
///     has not inflicted — historically the strategy's single largest cost.
///     Genuine collapses inside the confirmation window are still cut by the
///     ungated catastrophic floor (MAKER_CATASTROPHIC_SL_MULT).

use async_trait::async_trait;
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Instant;
use std::collections::HashMap;
use tokio::sync::Mutex;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::state::PositionKey;
use crate::vipers::is_drawdown_limit_hit;
use crate::config;
use crate::venues::core::TimeInForce;
use crate::helpers::price::{ceil_to_tick_size, floor_to_tick_size};

/// One price tick on both venues (see `helpers::price::floor_to_tick_size`).
const MAKER_TICK_SIZE: Decimal = dec!(0.01);
// Import OrderType

/// Tracks bid-depth at the previous evaluation tick for drain-rate computation.
struct DepthSample {
    yes_bid_depth: Decimal,
    no_bid_depth: Decimal,
    sampled_at: Instant,
}

/// Returns the first entry sub-gate a single side (YES or NO) fails, or `None`
/// if the side qualifies.  The first element is a STABLE category key (no live
/// numbers) used for log throttling; the second is a human-readable detail
/// string with the live values.  Keeping the throttle key stable prevents the
/// 50 ms tick loop from defeating the throttle via fluctuating price numbers.
fn side_reject_reason(
    has_ask: bool,
    book_ok: bool,
    taker_block: bool,
    toxic_cooldown: bool,
    spread: Decimal,
    bid_price: Decimal,
    ask: Decimal,
    complementary_bid: Decimal,
    velocity_block: bool,
    dc: &crate::helpers::dynamic_config::DynamicConfig,
) -> Option<(&'static str, String)> {
    // Checked before the spread, because without an ask there is no spread to
    // report — the venue fills an absent ask in at the $1.00 payout, which would
    // otherwise be subtracted from the bid and printed as a plausible number.
    if !has_ask {
        return Some(("no_ask", "no seller on this leg".to_string()));
    }
    if !book_ok {
        return Some(("book_imbalance", "book_imbalance".to_string()));
    }
    if taker_block {
        return Some(("taker_flow_drain", "taker_flow_drain".to_string()));
    }
    if toxic_cooldown {
        return Some(("toxic_cooldown", "toxic_cooldown (post-ToxicFill re-entry lockout)".to_string()));
    }
    if spread < dc.maker_min_spread {
        // Distinguish a knob that is merely tight from a market that cannot pay.
        //
        // `maker_min_spread` is configurable, so "spread 0.001 < min 0.010" reads
        // as an invitation to lower it — and usually that is right. But when the
        // spread is below the ROUND-TRIP FEE, no setting rescues it: quoting
        // would buy a guaranteed loss. Polymarket International's event markets
        // quote a tenth of a cent against fees two to seventeen times larger, and
        // an operator watching a squadron patrol without trading has no way to
        // tell "raise your appetite" from "this market is unquotable".
        //
        // `round_trip_fee_pct` is a fraction of notional; multiplying by the
        // price puts it in the same units as the spread. US Retail charges no
        // taker fee, so this branch is inert there rather than wrong.
        let fee_floor = crate::venues::round_trip_fee_pct(bid_price) * bid_price;
        if spread < fee_floor {
            return Some((
                "spread_below_fee",
                format!(
                    "spread {spread:.4} below fee floor {fee_floor:.4} — unquotable at any min_spread"
                ),
            ));
        }
        return Some(("spread", format!("spread {:.3} < min {:.3}", spread, dc.maker_min_spread)));
    }
    if bid_price < dc.maker_min_entry_price {
        return Some(("min_entry", format!("bid {:.3} < min_entry {:.3}", bid_price, dc.maker_min_entry_price)));
    }
    if bid_price > dc.maker_max_entry_price {
        return Some(("max_entry", format!("bid {:.3} > max_entry {:.3}", bid_price, dc.maker_max_entry_price)));
    }
    if bid_price > ask - dc.maker_cross_buffer {
        return Some(("cross_buffer", format!(
            "cross_buffer: bid {:.3} > ask {:.3} - {:.3}",
            bid_price, ask, dc.maker_cross_buffer
        )));
    }
    if complementary_bid > dc.maker_max_complementary_price {
        return Some(("complementary", format!(
            "complementary {:.3} > max {:.3}",
            complementary_bid, dc.maker_max_complementary_price
        )));
    }
    if velocity_block {
        return Some(("velocity_bias", "velocity_bias".to_string()));
    }
    None
}

pub struct MakerStrategyImpl {
    /// Per-strategy state: best-bid depths from the previous evaluation tick.
    /// Used to compute how fast bid depth is being consumed by takers within
    /// a MAKER_TAKER_FLOW_WINDOW_MS rolling window.
    /// Wrapped in Mutex because evaluate_entry and evaluate_exit run concurrently
    /// (tokio::join! in the executor).  evaluate_entry owns writes; evaluate_exit
    /// reads whatever sample is available (one-tick lag is acceptable for a gate).
    prev_depths: Mutex<Option<DepthSample>>,

    /// Gate-diagnostics throttle: (last reason logged, when).  We log a gate
    /// rejection whenever the reason changes OR MAKER_GATE_LOG_INTERVAL_SECS has
    /// elapsed, so the reason a maker is silent is visible without spamming every
    /// evaluation tick.
    last_gate_log: Mutex<Option<(String, Instant)>>,

    /// Throttle for the positive "quoting" log.  The eval loop runs on a ~50 ms
    /// tick, so an unthrottled quote log would emit ~20×/sec; this caps it to one
    /// line per MAKER_GATE_LOG_INTERVAL_SECS.
    last_quote_log: Mutex<Option<Instant>>,

    /// Dedicated throttle for the Horizon gate log.  The Horizon line fires
    /// BEFORE the main gate-summary log in the same tick, so routing it through
    /// `log_gate` made the two alternate keys ("horizon" → "no side qualifies")
    /// and the `prev_key != key` rule defeated the throttle for both — observed
    /// 2026-07-23: up to 370 identical Horizon lines/min during US-open veto
    /// windows.  A time-only throttle caps it at one line per interval.
    last_horizon_log: Mutex<Option<Instant>>,
}

/// Process-global market-maturation tracker: market identity → first time ANY
/// maker instance observed it.  Must be global (not per-strategy) because the
/// patrol loop rebuilds the strategy objects on every market rotation
/// (`create_all_strategies()`), which would otherwise wipe the baseline and
/// wrongly re-arm the 5-minute maturation blackout on a day-old daily maker
/// market each hour.  Keyed on `market_name` (stable for the daily maker venue
/// across hourly rotations; a genuinely new market gets a fresh entry, correctly
/// re-arming maturation).  Survives rotations within a process; re-arms once on a
/// full process restart (correct — a fresh process hasn't observed stability).
fn maker_market_first_seen() -> &'static std::sync::Mutex<HashMap<String, Instant>> {
    static REG: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Instant>>> =
        std::sync::OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Seconds since a maker market identity was first observed by any instance.
/// Returns 0 the first time an identity is seen (arming maturation) and grows
/// monotonically thereafter, surviving strategy re-instantiation on rotation.
fn market_age_secs(market_ident: &str) -> i64 {
    let mut reg = match maker_market_first_seen().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let first_seen = reg.entry(market_ident.to_string()).or_insert_with(Instant::now);
    first_seen.elapsed().as_secs() as i64
}

/// The maturation wait actually required for this market, in seconds.
///
/// `configured` is the absolute wait an operator set. It is a sensible number for
/// a market that lives for a day and a disqualifying one for a market that lives
/// for a quarter of an hour, so it is capped at `fraction` of the market's own
/// lifetime. Lifetime is `age + seconds_to_close`: the two move in opposite
/// directions at the same rate, so the estimate is stable across the market's
/// life rather than drifting as it ages.
///
/// A market with no close time cannot be scaled against anything and keeps the
/// absolute wait. So does one whose close time has already passed — the expiry
/// gate immediately below is what should reject that, not this one silently
/// admitting it with a zero wait.
fn effective_min_market_age(
    age_secs: i64,
    close_time: Option<DateTime<Utc>>,
    configured: i64,
    fraction: Decimal,
) -> i64 {
    let Some(close) = close_time else { return configured };
    maturation_wait(age_secs, (close - Utc::now()).num_seconds(), configured, fraction)
}

/// The arithmetic behind `effective_min_market_age`, with the clock read out.
///
/// Kept separate so it can be tested exactly: reading `Utc::now()` inside the
/// calculation makes the result depend on how long the test itself took, which
/// lands on a truncation boundary often enough to fail intermittently.
fn maturation_wait(
    age_secs: i64,
    secs_to_close: i64,
    configured: i64,
    fraction: Decimal,
) -> i64 {
    if secs_to_close <= 0 || fraction <= Decimal::ZERO {
        return configured;
    }
    let life = age_secs.saturating_add(secs_to_close);
    if life <= 0 {
        return configured;
    }
    let scaled: i64 = (Decimal::from(life) * fraction)
        .trunc()
        .try_into()
        .unwrap_or(configured);
    configured.min(scaled)
}

/// Process-global post-ToxicFill re-entry cooldown: token_id → the instant the
/// last ToxicFill exit fired for that token.  Global (not a per-strategy field)
/// for the same reason as `maker_market_first_seen`: patrol rebuilds the strategy
/// objects on every market rotation (`create_all_strategies()`), which would wipe
/// per-instance state and let the maker immediately re-quote into the very book it
/// was just picked off in.  Keyed on the specific YES/NO token so only the toxic
/// side is locked out; a genuinely new market gets a fresh token_id (no stale block).
fn maker_toxic_cooldowns() -> &'static std::sync::Mutex<HashMap<String, Instant>> {
    static REG: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Instant>>> =
        std::sync::OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Record that a ToxicFill exit just fired for `token_id`, arming the re-entry cooldown.
fn arm_maker_toxic_cooldown(token_id: &str) {
    let mut reg = match maker_toxic_cooldowns().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    reg.insert(token_id.to_string(), Instant::now());
}

/// Per-token throttle for the ToxicFill-exit log line. The exit signal legitimately
/// re-fires every 50ms tick while the patrol's EXIT_RETRY_COOLDOWN holds the actual
/// FAK attempt back (e.g. settlement-lag retries), which flooded 186 identical INFO
/// lines in 77s on 2026-08-05. Returns true at most once per 5s per token.
fn maker_toxic_log_permitted(token_id: &str) -> bool {
    static REG: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Instant>>> =
        std::sync::OnceLock::new();
    let reg = REG.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut reg = match reg.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    match reg.get(token_id) {
        Some(t) if t.elapsed().as_secs() < 5 => false,
        _ => {
            reg.insert(token_id.to_string(), Instant::now());
            true
        }
    }
}

/// Process-global consecutive-OBI-breach counter per token, backing the
/// `MAKER_TOXIC_OBI_CONFIRM_TICKS` gate.  Global for the same rotation-survival
/// reason as the trackers above.
fn maker_toxic_obi_streaks() -> &'static std::sync::Mutex<HashMap<String, u32>> {
    static REG: std::sync::OnceLock<std::sync::Mutex<HashMap<String, u32>>> =
        std::sync::OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Record this tick's OBI verdict for `token_id` and return the resulting run
/// length of CONSECUTIVE breaches.  A non-breaching tick resets the streak to 0,
/// so a book that flickers back to healthy has to start the count over — the
/// same "must qualify continuously" discipline the entry gates use
/// (`maker_gate_streak_secs`).
fn maker_toxic_obi_streak(token_id: &str, breached: bool) -> u32 {
    let mut reg = match maker_toxic_obi_streaks().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    if !breached {
        reg.remove(token_id);
        return 0;
    }
    let n = reg.entry(token_id.to_string()).or_insert(0);
    *n = n.saturating_add(1);
    *n
}

/// Drop any breach streak for `token_id` (position closed, or quote replaced).
fn clear_maker_toxic_obi_streak(token_id: &str) {
    let mut reg = match maker_toxic_obi_streaks().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    reg.remove(token_id);
}

/// Price the resting post-only ASK for a filled maker position, or `None` when
/// no valid ask can rest right now.
///
/// The price is the MORE PROFITABLE of two candidates:
///   * `ask − improvement` — undercut the book to take queue priority, and
///   * `entry × (1 + min_edge)` — the floor, so a spread that collapses after
///     the fill can never drag the exit down to a scratch.
///
/// Rounding is UP (`ceil_to_tick_size`), the sell-side mirror of the bid path's
/// floor-rounding: rounding a sell DOWN would both give away edge and risk
/// crossing. Returns `None` when the result would sit at or below the best bid
/// (a post-only sell there crosses the book and is rejected) or at/above $1.
fn resting_exit_price(
    bid: Decimal,
    ask: Decimal,
    avg_entry: Decimal,
    min_edge_pct: Decimal,
    ask_improvement_ticks: i64,
) -> Option<Decimal> {
    if avg_entry <= dec!(0) {
        return None;
    }
    let tick = dec!(0.01);
    let improvement = Decimal::from(ask_improvement_ticks.max(0)) * tick;
    let floor_price = avg_entry * (dec!(1) + min_edge_pct);
    let price = ceil_to_tick_size((ask - improvement).max(floor_price));

    if price <= bid || price >= dec!(1.00) {
        return None;
    }
    Some(price)
}

/// Returns true if `token_id` is still within its post-ToxicFill re-entry cooldown
/// (i.e. fewer than `cooldown_secs` have elapsed since the last toxic exit).  A
/// non-positive `cooldown_secs` disables the gate.  Expired entries are pruned on read.
fn maker_toxic_cooldown_active(token_id: &str, cooldown_secs: i64) -> bool {
    if cooldown_secs <= 0 {
        return false;
    }
    let mut reg = match maker_toxic_cooldowns().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(t) = reg.get(token_id) {
        if (t.elapsed().as_secs() as i64) < cooldown_secs {
            return true;
        }
        reg.remove(token_id);
    }
    false
}

/// Convert a raw oracle drift into an *adverse* drift for one side of the book.
///
/// A YES bid is hurt when the oracle falls; a NO bid is hurt when it rises. Shared by
/// the unfilled-quote pull and the filled-position exit so the two can never disagree
/// about which direction hurts — a sign error here would exit on favorable moves.
fn maker_adverse_drift(drift: Decimal, is_yes_side: bool) -> Decimal {
    if is_yes_side { -drift } else { drift }
}

/// Whether adverse oracle drift has reached an exit/pull threshold. A threshold of
/// zero disables the mechanism.
fn maker_drift_breached(adverse: Decimal, threshold: Decimal) -> bool {
    threshold > dec!(0) && adverse >= threshold
}

/// Process-global oracle baseline per quoted token: token_id → oracle price at the
/// moment the quote was (re)placed.  Global for the same rotation-survival reason as
/// the trackers above.  Used by the oracle-drift quote pull: the oracle is the
/// LEADING toxicity signal — informed takers act on Binance moves seconds before the
/// Polymarket book (OBI) reflects them, so an OBI-triggered pull loses the race
/// (2026-07-16: quote placed 12:56 @ BTC $64,420, BTC broke down ~12:58, OBI pull
/// fired 13:02:46, taker filled us anyway → −$0.365).  Drift-based pulls cancel the
/// stale quote minutes earlier.
fn maker_quote_oracle_baselines() -> &'static std::sync::Mutex<HashMap<String, Decimal>> {
    static REG: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Decimal>>> =
        std::sync::OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Record the oracle price behind a freshly placed/refreshed quote for `token_id`.
fn set_maker_quote_oracle_baseline(token_id: &str, oracle: Decimal) {
    if oracle <= dec!(0) { return; }
    let mut reg = match maker_quote_oracle_baselines().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    reg.insert(token_id.to_string(), oracle);
}

/// Clear the baseline once the quote is gone (pulled, filled, or exited).
fn clear_maker_quote_oracle_baseline(token_id: &str) {
    let mut reg = match maker_quote_oracle_baselines().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    reg.remove(token_id);
}

/// Process-global gate-qualification streaks: token_id → the instant that side
/// FIRST began continuously passing every entry gate.  Global for the usual
/// rotation-survival reason.  Backs the gate-dwell requirement: a single clean
/// tick amid book_imbalance/velocity_bias flicker during a directional move is
/// noise, not a regime change (2026-07-17: gates blocked NO at 12:47:51, one
/// clean tick at 12:47:55 posted a quote that was lifted within 5s → ToxicFill
/// −$0.20).  Requiring MAKER_GATE_DWELL_SECS of continuous qualification blocks
/// quoting into an active move; instant fills on fresh quotes are near-always
/// adverse selection.
fn maker_gate_streaks() -> &'static std::sync::Mutex<HashMap<String, Instant>> {
    static REG: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Instant>>> =
        std::sync::OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Update the qualification streak for `token_id` and return the seconds it has
/// been continuously qualifying (0 on the first qualifying tick).  A
/// non-qualifying tick resets the streak and returns None.
fn maker_gate_streak_secs(token_id: &str, qualifies: bool) -> Option<i64> {
    let mut reg = match maker_gate_streaks().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    if !qualifies {
        reg.remove(token_id);
        return None;
    }
    let started = reg.entry(token_id.to_string()).or_insert_with(Instant::now);
    Some(started.elapsed().as_secs() as i64)
}

/// Signed fractional oracle move since the quote baseline for `token_id`
/// (positive = oracle rose).  None when no baseline exists or oracle is invalid.
fn maker_quote_oracle_drift(token_id: &str, oracle_now: Decimal) -> Option<Decimal> {
    if oracle_now <= dec!(0) { return None; }
    let reg = match maker_quote_oracle_baselines().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    reg.get(token_id).map(|base| (oracle_now - base) / base)
}

impl MakerStrategyImpl {
    pub fn new() -> Self {
        Self {
            prev_depths: Mutex::new(None),
            last_gate_log: Mutex::new(None),
            last_quote_log: Mutex::new(None),
            last_horizon_log: Mutex::new(None),
        }
    }

    /// Build the resting post-only ASK for a healthy, filled maker position, or
    /// `None` when one should not rest right now.
    ///
    /// Price = the more profitable of (best_ask − improvement) and the floor
    /// (entry × (1 + min_edge)), so a collapsing spread can never drag the exit
    /// down to a scratch.  Returning a signal every tick is intentional and safe:
    /// the consumer is idempotent (place / reprice past the deadband / no-op).
    fn resting_exit_signal(
        &self,
        market: &crate::state::MarketConfig,
        snapshot: &crate::state::MarketSnapshot,
        position: &crate::state::Position,
        token_id: &crate::venues::core::MarketId,
        secs_to_expiry: i64,
        dc: &crate::helpers::dynamic_config::DynamicConfig,
    ) -> Option<StrategySignal> {
        if !dc.maker_resting_exit_enabled {
            return None;
        }
        // Only a CONFIRMED fill owns shares that can back a sell order — and a
        // ghost fill owns simulated shares just as a live one owns real ones, so
        // the resting exit must be exercised in simulation too.
        position.fill_effective_at(dc.ghost_mode)?;
        if position.shares < config::MIN_ORDER_SHARES || position.avg_entry <= dec!(0) {
            return None;
        }
        // Never leave shares committed to a resting ask as the market runs into
        // resolution — inside this window the near-expiry guard wants a certain,
        // immediate flatten, and an open sell order would lock the shares away
        // from it.
        if secs_to_expiry < dc.maker_min_secs_to_expiry {
            return None;
        }

        let is_yes = *token_id == market.yes_token;
        let (bid, ask) = if is_yes {
            (snapshot.yes_bid, snapshot.yes_ask)
        } else {
            (snapshot.no_bid, snapshot.no_ask)
        };

        let price = resting_exit_price(
            bid, ask, position.avg_entry,
            dc.maker_resting_exit_min_edge_pct,
            dc.maker_resting_exit_ask_improvement_ticks,
        )?;

        Some(StrategySignal::MakerRestingExit {
            params: OrderParams {
                token_id: token_id.clone(),
                price,
                shares: position.shares,
                fee_bps: if is_yes { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                is_neg_risk: market.is_neg_risk,
                market_name: market.market_name.clone(),
                condition_id: market.condition_id.clone(),
                order_type: TimeInForce::Gtc,
                post_only: true,
                ghost_mode: dc.ghost_mode,
            },
            reason: format!(
                "MakerRestingExit: ask=${:.4} entry=${:.4} edge={:.2}%",
                price, position.avg_entry,
                (price - position.avg_entry) / position.avg_entry * dec!(100)
            ),
        })
    }

    /// Throttled gate-rejection logger.  `key` is a STABLE category (no live
    /// numbers); `detail` carries the human-readable values.  Emits at INFO when
    /// `key` differs from the last logged key, or when MAKER_GATE_LOG_INTERVAL_SECS
    /// has passed since the last emit for the same key.  Throttling on the stable
    /// key (not the detail) keeps the 50 ms tick loop from flooding the log when
    /// live prices fluctuate.
    async fn log_gate(&self, asset: &str, key: &str, detail: &str) {
        // Unthrottled: feed the "why no trades?" registry with the human-readable
        // gate detail every time a gate rejects (GET /api/vipers/status).
        crate::helpers::viper_status::report_reason(asset, "MakerStrategy", detail);
        let mut guard = self.last_gate_log.lock().await;
        let should_log = match guard.as_ref() {
            Some((prev_key, at)) => {
                prev_key != key
                    || at.elapsed().as_secs() >= config::MAKER_GATE_LOG_INTERVAL_SECS
            }
            None => true,
        };
        if should_log {
            tracing::info!("🔒 Maker gate: {}", detail);
            *guard = Some((key.to_string(), Instant::now()));
        }
    }
}

impl Default for MakerStrategyImpl {
    fn default() -> Self { Self::new() }
}

#[async_trait]
impl Strategy for MakerStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        if !dc.enable_maker {
            crate::helpers::viper_status::report_reason(&ctx.crypto_filter, "MakerStrategy", "disabled in config");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global Risk Check ────────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            crate::helpers::viper_status::report_reason(&ctx.crypto_filter, "MakerStrategy", "session drawdown limit hit");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Select venue: prefer maker_market (window/daily) ──────────────────
        let market = ctx.maker_market.as_ref().unwrap_or(&ctx.market);
        let snapshot = ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot);


        // ── Market maturation gate ────────────────────────────────────────────
        // Age is measured from when THIS maker market was first observed (keyed on
        // market_name), not ctx.market_started_at — the latter resets on every
        // hourly rotation and would wrongly re-arm the maturation blackout on a
        // day-old daily maker market each hour.
        let secs_since_market_start = market_age_secs(&market.market_name);
        // The absolute wait is scaled down for short-lived markets. A flat 600s
        // is right for a daily market and unusable on a 15-minute one: measured
        // on Kalshi's KXBTC15M, the gate cleared at 21:12:00 on a market that
        // stopped accepting entries at 21:14:00 (60s RTB cutoff before a 21:15
        // close), leaving two minutes of a thirteen-minute market. The Maker
        // viper was, in practice, switched off there.
        //
        // Market life is measured as observed age + seconds to close, which is
        // stable as the market ages (age grows exactly as fast as the remainder
        // shrinks). Markets with no close time keep the absolute wait — nothing
        // to scale against.
        let min_age = effective_min_market_age(
            secs_since_market_start,
            market.market_close_time,
            dc.maker_min_market_age_secs,
            dc.maker_maturation_max_fraction,
        );
        if secs_since_market_start < min_age {
            maker_gate_streak_secs(market.yes_token.as_str(), false);
            maker_gate_streak_secs(market.no_token.as_str(), false);
            self.log_gate(&ctx.crypto_filter, "market_age", &format!(
                "market_age {}s < min {}s",
                secs_since_market_start, min_age
            )).await;
            return Ok(StrategySignal::NoSignal);
        }

        // ── Expiry gate ───────────────────────────────────────────────────────
        if let Some(close_time) = market.market_close_time {
            let secs_to_expiry = (close_time - Utc::now()).num_seconds();
            if secs_to_expiry < dc.maker_min_secs_to_expiry {
                maker_gate_streak_secs(market.yes_token.as_str(), false);
                maker_gate_streak_secs(market.no_token.as_str(), false);
                self.log_gate(&ctx.crypto_filter, "expiry", &format!(
                    "secs_to_expiry {}s < min {}s",
                    secs_to_expiry, dc.maker_min_secs_to_expiry
                )).await;
                return Ok(StrategySignal::NoSignal);
            }
        } else {
            maker_gate_streak_secs(market.yes_token.as_str(), false);
            maker_gate_streak_secs(market.no_token.as_str(), false);
            self.log_gate(&ctx.crypto_filter, "no_close_time", "no market_close_time").await;
            return Ok(StrategySignal::NoSignal);
        }

        let yes_bid = snapshot.yes_bid;
        let yes_ask = snapshot.yes_ask;
        let no_bid  = snapshot.no_bid;
        let no_ask  = snapshot.no_ask;

        // ── Orderbook imbalance gate ──────────────────────────────────────────
        // An ask/bid ratio rather than an imbalance, but built from the same
        // depths and just as sensitive to a one-contract touch, so it follows the
        // same source switch.
        let (yes_gate_bid, yes_gate_ask) = snapshot.yes_depths(dc.obi_use_whole_book);
        let (no_gate_bid,  no_gate_ask)  = snapshot.no_depths(dc.obi_use_whole_book);
        let yes_book_ok = yes_gate_bid > dec!(0)
            && (yes_gate_ask / yes_gate_bid) <= dc.maker_max_book_imbalance_ratio;
        let no_book_ok  = no_gate_bid > dec!(0)
            && (no_gate_ask  / no_gate_bid)  <= dc.maker_max_book_imbalance_ratio;

        if !yes_book_ok && !no_book_ok {
            maker_gate_streak_secs(market.yes_token.as_str(), false);
            maker_gate_streak_secs(market.no_token.as_str(), false);
            self.log_gate(&ctx.crypto_filter, "book_imbalance", &format!(
                "book_imbalance both sides (ratio>{:.1}): yes_bidD={:.0} yes_askD={:.0} | no_bidD={:.0} no_askD={:.0}",
                dc.maker_max_book_imbalance_ratio,
                snapshot.yes_bid_depth, snapshot.yes_ask_depth,
                snapshot.no_bid_depth, snapshot.no_ask_depth
            )).await;
            return Ok(StrategySignal::NoSignal);
        }

        // ── Taker-Flow / Bid-Depth Drain Gate ────────────────────────────────
        // Measure the fraction of best-bid depth consumed since the last tick.
        // A rapid drain (≥ MAKER_TAKER_FLOW_DRAIN_THRESHOLD within
        // MAKER_TAKER_FLOW_WINDOW_MS) indicates that takers are sweeping the book
        // one-sidedly — classic "toxic flow" that fills maker bids at adverse prices.
        // Suppress the affected side so we don't post into an active sweep.
        let (taker_flow_blocks_yes, taker_flow_blocks_no) = {
            let now_inst = Instant::now();
            let mut prev_guard = self.prev_depths.lock().await;

            let drain_flags = if let Some(ref p) = *prev_guard {
                let elapsed_ms = now_inst.duration_since(p.sampled_at).as_millis();
                // Only measure drain when the sample is both fresh (≤ WINDOW) and old enough
                // (≥ MIN_ELAPSED) to span multiple WS ticks.  Single-tick (49ms) comparisons
                // produce false positives from best-bid price-level rotation on thin books.
                if elapsed_ms >= config::MAKER_TAKER_FLOW_MIN_ELAPSED_MS as u128
                    && elapsed_ms <= config::MAKER_TAKER_FLOW_WINDOW_MS as u128
                {
                    // Positive value = depth decreased (bids were lifted by takers).
                    // Clamp at 0 so depth replenishment (depth increased) never triggers the gate.
                    let yes_drain = if p.yes_bid_depth > dec!(0) {
                        ((p.yes_bid_depth - snapshot.yes_bid_depth) / p.yes_bid_depth).max(dec!(0))
                    } else {
                        dec!(0)
                    };
                    let no_drain = if p.no_bid_depth > dec!(0) {
                        ((p.no_bid_depth - snapshot.no_bid_depth) / p.no_bid_depth).max(dec!(0))
                    } else {
                        dec!(0)
                    };

                    let block_yes = yes_drain >= config::MAKER_TAKER_FLOW_DRAIN_THRESHOLD;
                    let block_no  = no_drain  >= config::MAKER_TAKER_FLOW_DRAIN_THRESHOLD;

                    if block_yes {
                        tracing::info!(
                            "🚫 Maker YES entry suppressed: bid-depth drained {:.0}% in {}ms (taker sweep detected)",
                            yes_drain * dec!(100), elapsed_ms
                        );
                    }
                    if block_no {
                        tracing::info!(
                            "🚫 Maker NO entry suppressed: bid-depth drained {:.0}% in {}ms (taker sweep detected)",
                            no_drain * dec!(100), elapsed_ms
                        );
                    }

                    (block_yes, block_no)
                } else {
                    (false, false)
                }
            } else {
                (false, false)
            };

            // Store current depths for the next tick's comparison.
            // This write is owned by evaluate_entry; evaluate_exit only reads.
            *prev_guard = Some(DepthSample {
                yes_bid_depth: snapshot.yes_bid_depth,
                no_bid_depth:  snapshot.no_bid_depth,
                sampled_at:    now_inst,
            });

            drain_flags
        };

        // ── Inventory and Net Exposure Check ─────────────────────────────────
        let (yes_inv_value, no_inv_value) = {
            let pos_map = ctx.positions.lock().await;
            let yv = pos_map.get(&PositionKey::new(&ctx.squadron_id, "MakerStrategy", market.yes_token.clone()))
                .map(|p| p.shares * p.avg_entry).unwrap_or(dec!(0));
            let nv = pos_map.get(&PositionKey::new(&ctx.squadron_id, "MakerStrategy", market.no_token.clone()))
                .map(|p| p.shares * p.avg_entry).unwrap_or(dec!(0));
            (yv, nv)
        };

        // Skew calculation
        let imbalance = ((yes_inv_value - no_inv_value) / dc.maker_max_exposure_usdc)
            .clamp(dec!(-1), dec!(1));
        let skew = imbalance * config::MAKER_INVENTORY_SKEW_MAX;

        // Velocity bias from hourly oracle (always)
        let velocity = ctx.snapshot.velocity;
        let velocity_bias_strong_negative = velocity <= -config::MAKER_VELOCITY_BIAS_THRESHOLD;
        let velocity_bias_strong_positive = velocity >= config::MAKER_VELOCITY_BIAS_THRESHOLD;

        // ── Pricing Logic ─────────────────────────────────────────────────────
        // Use a wider buffer to avoid long-unfilled GTC orders in slower books
        let bid_buffer = if ctx.maker_market.is_some() { dc.maker_bid_buffer } else { dec!(0.015) };

        let raw_yes_price = (snapshot.yes_ask - bid_buffer - skew).max(dc.maker_min_entry_price);
        let raw_no_price  = (snapshot.no_ask - bid_buffer + skew).max(dc.maker_min_entry_price);

        // Clamp bid price to at most (ask - MAKER_CROSS_BUFFER) so that inventory-skew
        // rebalancing can never push the bid closer than 2 ticks from the ask.
        // Previously used a hardcoded dec!(0.01) which allowed 1-tick spreads when
        // the skew (±0.03) exceeded the bid_buffer (0.025), triggering the cap.
        // Now uses the configured MAKER_CROSS_BUFFER constant (0.02) for consistency.
        // Also cap at best_bid + one tick, so the maker IMPROVES the bid instead
        // of crossing the spread toward the ask.
        //
        // Both prices above are derived from the ASK. That is right on a tight
        // book — ask 0.52 / bid 0.50 quotes 0.50, at the bid — but on a wide one
        // it posts most of the way across. Kalshi, 2026-08-27: YES bid 0.35 /
        // ask 0.53, quoted 0.51, marked to the bid instantly and tripped the
        // catastrophic stop at -31.37%. The book never moved; the loss was
        // entirely the entry price, and it repeated on the NO side by the same
        // formula. `maker_min_spread` cannot catch it — that gate rejects books
        // too TIGHT, so a wider spread makes the maker keener, not warier.
        //
        // Guarded by a knob rather than hardcoded: crossing to the ask fills
        // faster, and an operator may want that on a venue where resting bids
        // rarely get lifted. Default is to improve the bid.
        let bid_cap = |best_bid: Decimal| best_bid + MAKER_TICK_SIZE;
        let yes_capped = if dc.maker_improve_bid_only {
            raw_yes_price.min(bid_cap(snapshot.yes_bid))
        } else {
            raw_yes_price
        };
        let no_capped = if dc.maker_improve_bid_only {
            raw_no_price.min(bid_cap(snapshot.no_bid))
        } else {
            raw_no_price
        };
        let yes_bid_price = floor_to_tick_size(yes_capped.min(snapshot.yes_ask - dc.maker_cross_buffer));
        let no_bid_price  = floor_to_tick_size(no_capped.min(snapshot.no_ask  - dc.maker_cross_buffer));

        let yes_spread = yes_ask - yes_bid;
        let no_spread  = no_ask - no_bid;

        // ── Qualification ─────────────────────────────────────────────────
        // Post-ToxicFill re-entry lockout: if this token was picked off by a
        // book-turn within the last `maker_toxic_reentry_cooldown_secs`, do not
        // re-quote that side — avoids catching the same falling knife twice.
        let cooldown_secs = dc.maker_toxic_reentry_cooldown_secs;
        let yes_toxic_cooldown = maker_toxic_cooldown_active(market.yes_token.as_str(), cooldown_secs);
        let no_toxic_cooldown  = maker_toxic_cooldown_active(market.no_token.as_str(), cooldown_secs);

        // ── Horizon Raptor gate (4th defense layer, observe-first) ───────────
        // TradFi front-runs BTC when macro_coherence is high, and a VIX-proxy
        // velocity spike is the earliest panic-onset signal — both LEAD the OBI
        // flip.  Risk-off flow ⇒ BTC likely down ⇒ suppress YES bids (they'd be
        // lifted by informed sellers); risk-on ⇒ suppress NO.  A VIX spike
        // suppresses BOTH sides regardless of coherence (panic is panic).
        // With MAKER_HORIZON_GATE_ENFORCE=false this only logs "would veto".
        let hz = &ctx.snapshot;
        let hz_vix_spike = hz.vix_velocity >= config::MAKER_HORIZON_VIX_VEL_MAX;
        let hz_coherent  = hz.macro_coherence >= config::MAKER_HORIZON_COHERENCE_MIN;
        let hz_risk_off  = hz_coherent && hz.tradfi_velocity <= -config::MAKER_HORIZON_TRADFI_VETO;
        let hz_risk_on   = hz_coherent && hz.tradfi_velocity >=  config::MAKER_HORIZON_TRADFI_VETO;
        let horizon_blocks_yes = hz_vix_spike || hz_risk_off;
        let horizon_blocks_no  = hz_vix_spike || hz_risk_on;
        if horizon_blocks_yes || horizon_blocks_no {
            // Time-only throttle (see `last_horizon_log`): this fires every tick
            // during a veto window, and sharing `log_gate`'s key-change rule with
            // the gate-summary line floods the log.
            let mut guard = self.last_horizon_log.lock().await;
            let due = guard
                .map(|at| at.elapsed().as_secs() >= config::MAKER_GATE_LOG_INTERVAL_SECS)
                .unwrap_or(true);
            if due {
                *guard = Some(Instant::now());
                drop(guard);
                tracing::info!(
                    "🔒 Maker gate: 🔭 Horizon gate{}: {}{}{} | tradfi_vel={:.3} coh={:.2} vix_vel={:.3} (blocks: YES={} NO={})",
                    if config::MAKER_HORIZON_GATE_ENFORCE { "" } else { " (observe — would veto)" },
                    if hz_vix_spike { "VIX spike " } else { "" },
                    if hz_risk_off { "risk-off " } else { "" },
                    if hz_risk_on { "risk-on " } else { "" },
                    hz.tradfi_velocity, hz.macro_coherence, hz.vix_velocity,
                    horizon_blocks_yes, horizon_blocks_no,
                );
            }
        }
        let horizon_vetoes_yes = config::MAKER_HORIZON_GATE_ENFORCE && horizon_blocks_yes;
        let horizon_vetoes_no  = config::MAKER_HORIZON_GATE_ENFORCE && horizon_blocks_no;

        let yes_gates_pass = snapshot.yes_has_ask()
            && yes_book_ok
            && !taker_flow_blocks_yes
            && !yes_toxic_cooldown
            && !horizon_vetoes_yes
            && yes_spread >= dc.maker_min_spread
            && yes_bid_price >= dc.maker_min_entry_price
            && yes_bid_price <= dc.maker_max_entry_price
            && yes_bid_price <= snapshot.yes_ask - dc.maker_cross_buffer
            && no_bid <= dc.maker_max_complementary_price
            && !velocity_bias_strong_negative;

        let no_gates_pass = snapshot.no_has_ask()
            && no_book_ok
            && !taker_flow_blocks_no
            && !no_toxic_cooldown
            && !horizon_vetoes_no
            && no_spread >= dc.maker_min_spread
            && no_bid_price >= dc.maker_min_entry_price
            && no_bid_price <= dc.maker_max_entry_price
            && no_bid_price <= snapshot.no_ask - dc.maker_cross_buffer
            && yes_bid <= dc.maker_max_complementary_price
            && !velocity_bias_strong_positive;

        // ── Gate-dwell requirement ────────────────────────────────────────────
        // Gates must pass CONTINUOUSLY for MAKER_GATE_DWELL_SECS before a side may
        // quote.  A single clean tick amid gate flicker during a directional move
        // is noise (2026-07-17 instant-fill ToxicFill); any gate failure resets
        // that side's streak.
        let yes_streak = maker_gate_streak_secs(market.yes_token.as_str(), yes_gates_pass);
        let no_streak  = maker_gate_streak_secs(market.no_token.as_str(), no_gates_pass);
        let yes_qualifies = yes_streak.is_some_and(|s| s >= config::MAKER_GATE_DWELL_SECS);
        let no_qualifies  = no_streak.is_some_and(|s| s >= config::MAKER_GATE_DWELL_SECS);

        if !yes_qualifies && !no_qualifies {
            let (yes_key, yes_detail) = match yes_streak {
                Some(s) => ("gate_dwell", format!("gate_dwell {}s/{}s", s, config::MAKER_GATE_DWELL_SECS)),
                None => side_reject_reason(
                    snapshot.yes_has_ask(), yes_book_ok, taker_flow_blocks_yes, yes_toxic_cooldown,
                    yes_spread, yes_bid_price,
                    snapshot.yes_ask, no_bid, velocity_bias_strong_negative, dc,
                ).unwrap_or(("unknown", "unknown".to_string())),
            };
            let (no_key, no_detail) = match no_streak {
                Some(s) => ("gate_dwell", format!("gate_dwell {}s/{}s", s, config::MAKER_GATE_DWELL_SECS)),
                None => side_reject_reason(
                    snapshot.no_has_ask(), no_book_ok, taker_flow_blocks_no, no_toxic_cooldown,
                    no_spread, no_bid_price,
                    snapshot.no_ask, yes_bid, velocity_bias_strong_positive, dc,
                ).unwrap_or(("unknown", "unknown".to_string())),
            };
            self.log_gate(&ctx.crypto_filter, 
                &format!("noqual:{}/{}", yes_key, no_key),
                &format!("no side qualifies | YES: {} | NO: {}", yes_detail, no_detail),
            ).await;
            return Ok(StrategySignal::NoSignal);
        }

        // ── Net Exposure Risk Check ──────────────────────────────────────────
        // Quote size is config-driven and clamped to the exposure cap so a single
        // quote from a flat book always fits under the limit (a quote larger than
        // the cap would self-gate the maker after one clip).
        let trade_size = dc.maker_quote_size_usdc.min(dc.maker_max_exposure_usdc);
        let projected_yes = yes_inv_value + (if yes_qualifies { trade_size } else { dec!(0.0) });
        let projected_no  = no_inv_value  + (if no_qualifies { trade_size } else { dec!(0.0) });
        let net_exposure  = (projected_yes - projected_no).abs();

        if net_exposure > dc.maker_max_exposure_usdc {
            self.log_gate(&ctx.crypto_filter, "net_exposure", &format!(
                "net_exposure ${:.2} > max ${:.2}",
                net_exposure, dc.maker_max_exposure_usdc
            )).await;
            return Ok(StrategySignal::NoSignal);
        }

        // ── Combined price guard ──────────────────────────────────────────────
        let (final_yes, final_no) = if yes_qualifies && no_qualifies {
            let combined = yes_bid_price + no_bid_price;
            if combined >= dc.maker_max_combined_bid {
                if yes_spread <= no_spread { (None, Some(no_bid_price)) } else { (Some(yes_bid_price), None) }
            } else {
                (Some(yes_bid_price), Some(no_bid_price))
            }
        } else if yes_qualifies {
            (Some(yes_bid_price), None)
        } else {
            (None, Some(no_bid_price))
        };

        if final_yes.is_none() && final_no.is_none() {
            self.log_gate(&ctx.crypto_filter, "combined_bid", "combined_bid guard suppressed both sides").await;
            return Ok(StrategySignal::NoSignal);
        }

        // Viper Backtrace: stash quote-time decision state per side.  The stash is
        // keyed by token and only drained when a fill is confirmed (record_entry_signal),
        // so re-quotes simply overwrite with the latest state — the row captures the
        // quote that actually filled.
        if let Some(p) = final_yes {
            crate::helpers::metrics::stash_entry_signals_json(market.yes_token.as_str(), serde_json::json!({
                "viper": "Maker",
                "side": "YES",
                "quote_bid": p.to_string(),
                "spread": yes_spread.to_string(),
                "trade_size": trade_size.to_string(),
                "net_exposure": net_exposure.to_string(),
                "both_sides_quoted": final_no.is_some(),
            }));
        }
        if let Some(p) = final_no {
            crate::helpers::metrics::stash_entry_signals_json(market.no_token.as_str(), serde_json::json!({
                "viper": "Maker",
                "side": "NO",
                "quote_bid": p.to_string(),
                "spread": no_spread.to_string(),
                "trade_size": trade_size.to_string(),
                "net_exposure": net_exposure.to_string(),
                "both_sides_quoted": final_yes.is_some(),
            }));
        }

        // ── Build detailed signals ───────────────────────────────────────────
        // Maker (post-only) orders are NEVER charged a taker fee by the CLOB —
        // the feeRateBps field is an EIP-712 struct attribute required by the API
        // but it is NOT deducted from maker fills.  Pass 0 so our P&L math is correct.
        let yes_params = final_yes.map(|p| OrderParams {
            token_id: market.yes_token.clone(),
            price: p,
            shares: trade_size / p,
            fee_bps: 0,
            is_neg_risk: market.is_neg_risk,
            market_name: market.market_name.clone(),
            condition_id: market.condition_id.clone(),
            order_type: TimeInForce::Gtc,
            post_only: true,
            ghost_mode: dc.ghost_mode,
        });

        let no_params = final_no.map(|p| OrderParams {
            token_id: market.no_token.clone(),
            price: p,
            shares: trade_size / p,
            fee_bps: 0,
            is_neg_risk: market.is_neg_risk,
            market_name: market.market_name.clone(),
            condition_id: market.condition_id.clone(),
            order_type: TimeInForce::Gtc,
            post_only: true,
            ghost_mode: dc.ghost_mode,
        });

        {
            let mut guard = self.last_quote_log.lock().await;
            let due = guard.map_or(true, |t| t.elapsed().as_secs() >= config::MAKER_GATE_LOG_INTERVAL_SECS);
            if due {
                tracing::info!(
                    "✅ Maker quoting: YES={} NO={}",
                    yes_params.as_ref().map(|p| format!("${:.3}", p.price)).unwrap_or_else(|| "—".to_string()),
                    no_params.as_ref().map(|p| format!("${:.3}", p.price)).unwrap_or_else(|| "—".to_string()),
                );
                *guard = Some(Instant::now());
            }
        }

        Ok(StrategySignal::MakerQuote {
            yes: yes_params,
            no: no_params,
        })
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        let market = ctx.maker_market.as_ref().unwrap_or(&ctx.market);
        let snapshot = ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot);

        let secs_to_expiry = market.market_close_time
            .map(|t| (t - Utc::now()).num_seconds())
            .unwrap_or(9999);

        // Near-expiry forced exit to avoid binary resolution risk.
        //
        // 2026-07-14: fires at maker_min_secs_to_expiry (1800s) instead of a hardcoded
        // 900s. Quoting already stops at 1800s, so the old 900s threshold left a
        // 15-minute dead zone holding pure directional risk with no new quotes — a
        // YES @ $0.48 rode through it to resolution at $0.004 (−$3.81, the single
        // largest maker loss). Flatten the moment the strategy stops quoting.
        let profit_threshold = dec!(0.02);
        if secs_to_expiry < dc.maker_min_secs_to_expiry {
            let pos_map = ctx.positions.lock().await;
            for token_id in [market.yes_token.clone(), market.no_token.clone()] {
                if let Some(position) = pos_map.get(&PositionKey::new(&ctx.squadron_id, "MakerStrategy", token_id.clone())) {
                    let bid = if token_id == market.yes_token { snapshot.yes_bid } else { snapshot.no_bid };
                    let profit_pct = (bid - position.avg_entry) / position.avg_entry;
                    if profit_pct < profit_threshold {
                        return Ok(StrategySignal::Exit {
                            params: OrderParams {
                                token_id: token_id.clone(),                                price: bid,
                                shares: position.shares,
                                fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                                is_neg_risk: market.is_neg_risk,
                                market_name: market.market_name.clone(),
                                condition_id: market.condition_id.clone(),
                                order_type: TimeInForce::Fak,
                                post_only: false,
                                ghost_mode: dc.ghost_mode,
                            },
                            reason: "NearExpiryProfitGuard".to_string(),
                            exit_pair: false,
                        });
                    }
                }
            }
        }

        let effective_stop_pct = if secs_to_expiry < config::MAKER_LATE_MARKET_STOP_TIGHTEN_SECS {
            config::MAKER_LATE_MARKET_STOP_LOSS_PERCENT
        } else {
            dc.maker_stop_loss_pct
        };

        // ── Taker-Flow Book-Turn Exit / Quote-Pull ────────────────────────────
        // A book that has turned adverse (OBI below the toxic threshold) is a
        // one-way sweep. For a FILLED position we exit at the bid (ToxicFill). For
        // an UNFILLED resting quote we cancel it (reactive quote-pull) so it isn't
        // left on the book to be picked off by the informed flow — the exact
        // adverse-selection mechanism behind the noon-ET maker losses.
        //
        // ORACLE-DRIFT pull (leading signal): OBI only turns AFTER informed takers
        // arrive, and the OBI pull loses the cancel race (2026-07-16: pull fired
        // 13:02:46, taker filled us anyway → −$0.365).  The oracle moves first, so
        // an unfilled quote whose oracle has drifted adversely beyond
        // MAKER_ORACLE_DRIFT_PULL_FRAC since placement is cancelled immediately.
        {
            let pos_map = ctx.positions.lock().await;
            let mut pull_tokens = Vec::new();
            for token_id in [market.yes_token.clone(), market.no_token.clone()] {
                let Some(position) = pos_map.get(&PositionKey::new(&ctx.squadron_id, "MakerStrategy", token_id.clone())) else {
                    // No quote/position on this token — drop any stale drift baseline.
                    clear_maker_quote_oracle_baseline(token_id.as_str());
                    clear_maker_toxic_obi_streak(token_id.as_str());
                    continue;
                };

                if position.fill_effective_at(dc.ghost_mode).is_none() {
                    // Unfilled resting quote: arm/check the oracle-drift baseline.
                    let oracle_now = snapshot.oracle_price;
                    match maker_quote_oracle_drift(token_id.as_str(), oracle_now) {
                        None => set_maker_quote_oracle_baseline(token_id.as_str(), oracle_now),
                        Some(drift) => {
                            // YES bid: oracle falling is adverse. NO bid: oracle rising.
                            let adverse = maker_adverse_drift(drift, token_id == market.yes_token);
                            if adverse >= dc.maker_oracle_drift_pull_frac {
                                arm_maker_toxic_cooldown(token_id.as_str());
                                clear_maker_quote_oracle_baseline(token_id.as_str());
                                tracing::info!(
                                    "⚡ Maker quote-pull (oracle drift): oracle moved {:.3}% adverse to resting {} quote (threshold {:.3}%) — cancelling before the book turns, re-entry locked {}s",
                                    adverse * dec!(100), if token_id == market.yes_token { "YES" } else { "NO" },
                                    dc.maker_oracle_drift_pull_frac * dec!(100), dc.maker_toxic_reentry_cooldown_secs
                                );
                                pull_tokens.push(token_id.clone());
                                continue;
                            }
                        }
                    }
                } else {
                    // Quote filled — KEEP the baseline. It anchors at quote placement, the
                    // price we actually committed to, so drift measured from it captures the
                    // whole adverse move including whatever happened at the instant of the
                    // fill. This used to clear it, which threw away the LEADING toxicity
                    // signal at the one moment it matters most: a fill in an adverse move IS
                    // the adverse-selection event, and from then on the position had only
                    // OBI — documented above as lagging by minutes — to save it.
                }

                let (bid_depth, ask_depth, bid) = if token_id == market.yes_token {
                    (snapshot.yes_bid_depth, snapshot.yes_ask_depth, snapshot.yes_bid)
                } else {
                    (snapshot.no_bid_depth, snapshot.no_ask_depth, snapshot.no_bid)
                };

                let total_depth = bid_depth + ask_depth;
                let obi = if total_depth > dec!(0) {
                    (bid_depth - ask_depth) / total_depth
                } else { dec!(0) };

                let breached = obi < dc.maker_toxic_flow_exit_obi;

                // ── UNFILLED resting quote: pull instantly on any breach ──────
                // Cancelling a quote that has not filled costs nothing and risks
                // nothing, so this half of the mechanism stays as eager as it has
                // always been. Only the FILLED path below is gated.
                if position.fill_effective_at(dc.ghost_mode).is_none() {
                    if breached {
                        arm_maker_toxic_cooldown(token_id.as_str());
                        clear_maker_quote_oracle_baseline(token_id.as_str());
                        clear_maker_toxic_obi_streak(token_id.as_str());
                        pull_tokens.push(token_id.clone());
                    }
                    continue;
                }

                // ── CONFIRMED FILL: require all three confirmations ───────────
                // See MAKER_TOXIC_* in config.rs. Track the streak on EVERY tick
                // (including non-breaching ones, which reset it) so the counter
                // reflects consecutive breaches rather than cumulative ones.
                let obi_streak = maker_toxic_obi_streak(token_id.as_str(), breached);

                // Oracle-drift trigger. The whole ToxicFill path used to be gated on
                // `breached` alone, so a position whose oracle had run away bled until
                // OBI — the lagging signal — caught up, or until the stop-loss fired.
                //
                // Ireland, 2026-08-27: a filled NO position went 13.51% adverse over 363s
                // at OBI -0.97 and exited for -$2.12, erasing two +6% wins. The 20% stop
                // never triggered and OBI confirmed only after the damage was done.
                //
                // A quote that survives to fill has, by construction, drifted less than
                // the (much tighter) pull threshold, so reaching the exit threshold means
                // the move happened at or after the fill.
                let drift_adverse = maker_quote_oracle_drift(token_id.as_str(), snapshot.oracle_price)
                    .map(|d| maker_adverse_drift(d, token_id == market.yes_token))
                    .unwrap_or(dec!(0));
                let drift_breached =
                    maker_drift_breached(drift_adverse, dc.maker_oracle_drift_exit_frac);

                if !(breached || drift_breached) {
                    continue;
                }

                let held_secs = position.fill_effective_at(dc.ghost_mode)
                    .map(|t| (Utc::now() - t).num_seconds())
                    .unwrap_or(0);
                let adverse_pct = if position.avg_entry > dec!(0) {
                    (position.avg_entry - bid) / position.avg_entry
                } else {
                    dec!(0)
                };
                let confirm_ticks = dc.maker_toxic_obi_confirm_ticks.max(1);

                let hold_ok   = held_secs >= dc.maker_toxic_min_hold_secs;
                let price_ok  = adverse_pct >= dc.maker_toxic_min_adverse_pct;
                // The OBI confirmation streak gates only an OBI-triggered exit. Demanding
                // it for a drift trigger would reinstate the lagging-signal wait this path
                // exists to avoid — hold_ok and price_ok still confirm the move is real.
                let streak_ok = drift_breached || obi_streak >= confirm_ticks;

                if !(hold_ok && price_ok && streak_ok) {
                    // Held deliberately. The book looks hostile but at least one
                    // confirmation is missing, so exiting here would pay the
                    // spread to realize a loss the price has not yet inflicted.
                    if maker_toxic_log_permitted(token_id.as_str()) {
                        let blocker = if !hold_ok {
                            format!("min_hold {}s/{}s", held_secs, dc.maker_toxic_min_hold_secs)
                        } else if !price_ok {
                            format!("adverse {:.2}% < {:.2}%",
                                    adverse_pct * dec!(100), dc.maker_toxic_min_adverse_pct * dec!(100))
                        } else {
                            format!("obi_confirm {}/{} ticks", obi_streak, confirm_ticks)
                        };
                        tracing::info!(
                            "🔒 Maker ToxicFill held: OBI={:.2} (threshold={:.2}) | bid=${:.4} entry=${:.4} | {}",
                            obi, dc.maker_toxic_flow_exit_obi, bid, position.avg_entry, blocker
                        );
                    }
                    continue;
                }

                arm_maker_toxic_cooldown(token_id.as_str());
                clear_maker_quote_oracle_baseline(token_id.as_str());
                clear_maker_toxic_obi_streak(token_id.as_str());
                if maker_toxic_log_permitted(token_id.as_str()) {
                    tracing::info!(
                        "⚡ Maker ToxicFill exit triggered ({}): OBI={:.2} (threshold={:.2}, confirmed {} ticks) | drift={:.3}% | bid=${:.4} adverse={:.2}% held={}s | re-entry locked {}s",
                        if drift_breached { "oracle drift" } else { "OBI" },
                        obi, dc.maker_toxic_flow_exit_obi, obi_streak, drift_adverse * dec!(100), bid,
                        adverse_pct * dec!(100), held_secs, dc.maker_toxic_reentry_cooldown_secs
                    );
                }
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token_id.clone(),
                        price: bid,
                        shares: position.shares,
                        fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!(
                        "ToxicFill: OBI={:.2} adverse={:.2}% held={}s (book turned adverse)",
                        obi, adverse_pct * dec!(100), held_secs
                    ),
                    exit_pair: false,
                });
            }
            if !pull_tokens.is_empty() {
                tracing::info!(
                    "⚡ Maker quote-pull: cancelling {} unfilled resting quote(s) — book turned toxic (OBI < {:.2}), re-entry locked {}s",
                    pull_tokens.len(), dc.maker_toxic_flow_exit_obi, dc.maker_toxic_reentry_cooldown_secs
                );
                return Ok(StrategySignal::MakerCancel { tokens: pull_tokens });
            }
        }

        let pos_map = ctx.positions.lock().await;

        // Deferred until after the loop so a hard exit on either leg wins.
        let mut resting_exit: Option<StrategySignal> = None;

        for token_id in [market.yes_token.clone(), market.no_token.clone()] {
            let Some(position) = pos_map.get(&PositionKey::new(&ctx.squadron_id, "MakerStrategy", token_id.clone())) else {
                continue;
            };

            let bid = if token_id == market.yes_token { snapshot.yes_bid } else { snapshot.no_bid };
            if position.avg_entry <= dec!(0) { continue; }

            let profit_pct = (bid - position.avg_entry) / position.avg_entry;
            // Ghost fills count as confirmed — see Position::fill_effective_at.
            let fill_at = position.fill_effective_at(dc.ghost_mode);
            let secs_since_fill = fill_at
                .map(|t| (Utc::now() - t).num_seconds())
                .unwrap_or(0);
            // "0s" was reported whenever there was no timestamp at all, which
            // read as an instant round trip and sent an investigation chasing a
            // pricing bug that did not exist. Say "unconfirmed" when that is
            // what it is.
            let held_label = fill_at
                .map(|t| format!("{}s", (Utc::now() - t).num_seconds()))
                .unwrap_or_else(|| "unconfirmed".to_string());

            // Catastrophic floor — ungated by the min-hold and by fill confirmation,
            // so a fast adverse move during the stop's blind window (or on an adopted
            // position) is still cut. See MAKER_CATASTROPHIC_SL_MULT.
            let catastrophic_pct = effective_stop_pct * config::MAKER_CATASTROPHIC_SL_MULT;
            if profit_pct <= -catastrophic_pct {
                // Loss exits arm the same re-entry lockout as ToxicFill: without it the
                // maker re-quotes the falling side within 1s of the stop (2026-08-02:
                // 24-share NO re-quote placed 1s after a −15% SL, saved only by the
                // oracle-drift pull 52s later).
                arm_maker_toxic_cooldown(token_id.as_str());
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token_id.clone(),
                        price: bid,
                        shares: position.shares,
                        fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!("Maker Catastrophic: loss={:.2}% ({held_label} held)", profit_pct * dec!(100)),
                    exit_pair: false,
                });
            }

            // Take-profit target, floored so it actually clears the exit it pays for.
            //
            // The Maker quote is post-only and is charged NO taker fee; only the
            // FAK that closes it is. So the cost to beat is the single-leg
            // `exit_only_fee_pct`, half the round trip Momentum floors against —
            // charging the full round trip here would roughly double the target
            // and stall the viper on exactly the cheap entries it is built for.
            //
            // Unfloored, the flat target is below break-even across the bottom of
            // the permitted entry band: the exit fee is `rate × (1 − p)` of
            // notional, so at rate 0.07 a $0.18 entry owes 5.7% before it profits
            // at all and the 7% target clears it by a hair, while a $0.15 entry
            // owes 6.0%. Observed 2026-08-30 on a $0.18 → $0.19 round trip:
            // "Maker TP: gain=5.55%" booked −$0.052, a loss logged as a win.
            //
            // The fee is charged at the EXIT, and on a quadratic schedule a
            // higher exit costs more on any contract below ~$0.50 — so pricing
            // the floor at the entry price understates it by
            // `rate · g · (1 − p(2 + g))` right across Maker's band. Two passes:
            // price the fee at the configured target, then re-price it at the
            // floor that produced, which is where the exit will actually land.
            // It converges immediately; a third pass moves nothing.
            let mut tp_target = dc.maker_target_profit_pct;
            for _ in 0..2 {
                let fee_floor = crate::venues::exit_fee_pct_at_gain(position.avg_entry, tp_target)
                    * dc.maker_tp_fee_margin_mult;
                tp_target = dc.maker_target_profit_pct.max(fee_floor);
            }
            if tp_target > dc.maker_target_profit_pct {
                tracing::debug!(
                    "Maker TP floor: target {:.2}% → {:.2}% (exit fee {:.2}% at entry ${:.4})",
                    dc.maker_target_profit_pct * dec!(100), tp_target * dec!(100),
                    crate::venues::exit_fee_pct_at_gain(position.avg_entry, tp_target) * dec!(100),
                    position.avg_entry,
                );
            }
            if fill_at.is_some() && profit_pct >= tp_target {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token_id.clone(),                        price: bid,
                        shares: position.shares,
                        fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!(
                        "Maker TP: gain={:.2}% (target {:.2}%, exit fee {:.2}%)",
                        profit_pct * dec!(100), tp_target * dec!(100),
                        crate::venues::exit_fee_pct_at_gain(position.avg_entry, profit_pct) * dec!(100),
                    ),
                    exit_pair: false,
                });
            }

            if fill_at.is_some()
                && secs_since_fill >= config::MAKER_MIN_HOLD_SECS_BEFORE_STOP
                && profit_pct <= -effective_stop_pct
            {
                // Same re-entry lockout as the catastrophic stop above.
                arm_maker_toxic_cooldown(token_id.as_str());
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token_id.clone(),                        price: bid,
                        shares: position.shares,
                        fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!("Maker SL: loss={:.2}% ({}s held)", profit_pct * dec!(100), secs_since_fill),
                    exit_pair: false,
                });
            }

            // ── Resting maker exit (spread capture) ───────────────────────────
            // This token survived every hard-exit check, so the position is
            // healthy and its natural way out is to be LIFTED at the ask rather
            // than to cross back to the bid. Only RECORD the candidate here —
            // returning it immediately would let a healthy YES leg preempt a
            // stop-loss still pending on the NO leg, since the hard exits above
            // are evaluated per token inside this same loop.
            if resting_exit.is_none() {
                resting_exit = self.resting_exit_signal(market, snapshot, position, &token_id, secs_to_expiry, dc);
            }
        }

        if let Some(signal) = resting_exit {
            return Ok(signal);
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }

    fn name(&self) -> String { "MakerStrategy".to_string() }
    fn venue(&self) -> &'static str { "Window/Daily" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::MAKER_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Net |YES-NO|" }
}

#[cfg(test)]
mod toxic_cooldown_tests {
    use super::{arm_maker_toxic_cooldown, maker_toxic_cooldown_active};

    #[test]
    fn armed_token_is_locked_out() {
        let tok = "test_tok_armed_lockout";
        assert!(!maker_toxic_cooldown_active(tok, 180));
        arm_maker_toxic_cooldown(tok);
        assert!(maker_toxic_cooldown_active(tok, 180));
    }

    #[test]
    fn unknown_token_is_not_locked() {
        assert!(!maker_toxic_cooldown_active("test_tok_never_armed", 180));
    }

    #[test]
    fn zero_or_negative_cooldown_disables_gate() {
        let tok = "test_tok_disabled_gate";
        arm_maker_toxic_cooldown(tok);
        assert!(!maker_toxic_cooldown_active(tok, 0));
        assert!(!maker_toxic_cooldown_active(tok, -5));
    }

    #[test]
    fn expired_cooldown_allows_reentry() {
        let tok = "test_tok_expired";
        arm_maker_toxic_cooldown(tok);
        // Immediately after arming, a sub-second window has already elapsed
        // (elapsed secs = 0 is NOT < 0), so the token reads as no-longer-locked
        // and is pruned on read. Use cooldown_secs so small it is already expired.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(!maker_toxic_cooldown_active(tok, 1));
    }
}

#[cfg(test)]
mod resting_exit_price_tests {
    use super::resting_exit_price;
    use rust_decimal_macros::dec;

    // Balanced-profile defaults.
    const EDGE: rust_decimal::Decimal = dec!(0.04);
    const IMPROVE: i64 = 1;

    #[test]
    fn undercuts_the_ask_by_one_tick_on_a_wide_book() {
        // The 2026-08-12 loss #1 book: filled NO @ 0.41 with the ask at 0.46.
        // Resting at 0.45 exits +9.8% by being LIFTED, where the old path
        // crossed to the 0.39 bid for −7.3%.
        let p = resting_exit_price(dec!(0.39), dec!(0.46), dec!(0.41), EDGE, IMPROVE);
        assert_eq!(p, Some(dec!(0.45)));
    }

    #[test]
    fn floor_wins_when_the_spread_collapses() {
        // Ask crushed to 0.42: undercutting would rest at 0.41 — a scratch on a
        // 0.41 entry. The floor holds the ask up at entry × 1.04 = 0.4264 → 0.43.
        let p = resting_exit_price(dec!(0.40), dec!(0.42), dec!(0.41), EDGE, IMPROVE);
        assert_eq!(p, Some(dec!(0.43)));
    }

    #[test]
    fn rounds_the_floor_up_never_down() {
        // entry 0.41 × 1.04 = 0.4264. Rounding DOWN to 0.42 would silently sell
        // below the configured minimum edge.
        let p = resting_exit_price(dec!(0.10), dec!(0.20), dec!(0.41), EDGE, IMPROVE);
        assert_eq!(p, Some(dec!(0.43)));
    }

    #[test]
    fn none_when_the_ask_would_cross_the_bid() {
        // A post-only sell at or below the bid is rejected by the exchange, so
        // there is nothing to place: bid 0.44 has already run past the 0.43 floor.
        let p = resting_exit_price(dec!(0.44), dec!(0.45), dec!(0.41), EDGE, IMPROVE);
        assert_eq!(p, None);
    }

    #[test]
    fn none_at_or_above_one_dollar() {
        let p = resting_exit_price(dec!(0.90), dec!(1.00), dec!(0.99), EDGE, IMPROVE);
        assert_eq!(p, None);
    }

    #[test]
    fn zero_improvement_joins_the_ask() {
        let p = resting_exit_price(dec!(0.39), dec!(0.46), dec!(0.41), EDGE, 0);
        assert_eq!(p, Some(dec!(0.46)));
    }

    #[test]
    fn negative_improvement_is_clamped_to_zero() {
        // A negative tick count must never push the ask UP past the book.
        let p = resting_exit_price(dec!(0.39), dec!(0.46), dec!(0.41), EDGE, -3);
        assert_eq!(p, Some(dec!(0.46)));
    }

    #[test]
    fn none_on_a_zero_entry_price() {
        let p = resting_exit_price(dec!(0.39), dec!(0.46), dec!(0), EDGE, IMPROVE);
        assert_eq!(p, None);
    }

    #[test]
    fn resting_price_always_beats_crossing_to_the_bid() {
        // The invariant that justifies the whole feature: whenever an ask can
        // rest, it exits strictly better than the FAK-at-bid path it replaces.
        for (bid, ask, entry) in [
            (dec!(0.39), dec!(0.46), dec!(0.41)),
            (dec!(0.40), dec!(0.42), dec!(0.41)),
            (dec!(0.30), dec!(0.38), dec!(0.32)),
            (dec!(0.55), dec!(0.62), dec!(0.57)),
        ] {
            if let Some(p) = resting_exit_price(bid, ask, entry, EDGE, IMPROVE) {
                assert!(p > bid, "resting ask {p} must beat the bid {bid}");
                assert!(p >= entry, "resting ask {p} must not book a loss on entry {entry}");
            }
        }
    }
}

#[cfg(test)]
mod toxic_obi_streak_tests {
    use super::{clear_maker_toxic_obi_streak, maker_toxic_obi_streak};

    #[test]
    fn consecutive_breaches_accumulate() {
        let tok = "test_tok_streak_accum";
        assert_eq!(maker_toxic_obi_streak(tok, true), 1);
        assert_eq!(maker_toxic_obi_streak(tok, true), 2);
        assert_eq!(maker_toxic_obi_streak(tok, true), 3);
        clear_maker_toxic_obi_streak(tok);
    }

    #[test]
    fn a_healthy_tick_resets_the_streak() {
        // This is the whole point of the gate: a book that flickers back to
        // healthy has to earn the confirmation count again from zero, so the
        // single-tick OBI dip caused by our own fill can never reach the
        // threshold on its own.
        let tok = "test_tok_streak_reset";
        assert_eq!(maker_toxic_obi_streak(tok, true), 1);
        assert_eq!(maker_toxic_obi_streak(tok, true), 2);
        assert_eq!(maker_toxic_obi_streak(tok, false), 0);
        assert_eq!(maker_toxic_obi_streak(tok, true), 1);
        clear_maker_toxic_obi_streak(tok);
    }

    #[test]
    fn clearing_drops_the_streak() {
        let tok = "test_tok_streak_clear";
        assert_eq!(maker_toxic_obi_streak(tok, true), 1);
        clear_maker_toxic_obi_streak(tok);
        assert_eq!(maker_toxic_obi_streak(tok, true), 1);
        clear_maker_toxic_obi_streak(tok);
    }

    #[test]
    fn unknown_token_starts_at_zero() {
        assert_eq!(maker_toxic_obi_streak("test_tok_streak_unknown", false), 0);
    }
}

#[cfg(test)]
mod tp_fee_floor_tests {
    use crate::helpers::dynamic_config::DynamicConfig;
    use rust_decimal_macros::dec;

    /// A Maker take-profit must clear the exit it pays for.
    ///
    /// The quote is post-only and is charged nothing; the closing FAK pays
    /// `rate × p × (1 − p)` per share, which against an entry notional of `p` is
    /// `rate × (1 − p)` — 5.7% at a $0.18 entry, 6.0% at $0.15. A flat 7% target
    /// clears those by a hair and fails outright once the exit prints below the
    /// entry-price approximation, which is exactly what happened on 2026-08-30:
    /// $0.18 → $0.19 booked "Maker TP: gain=5.55%" and −$0.052.
    #[test]
    fn take_profit_target_clears_the_exit_fee_across_the_entry_band() {
        let dc = DynamicConfig::default();
        let mut price = dc.maker_min_entry_price;
        while price <= dc.maker_max_entry_price {
            // Mirror the two-pass floor from `evaluate_exit`.
            let mut effective = dc.maker_target_profit_pct;
            for _ in 0..2 {
                let floor = crate::venues::exit_fee_pct_at_gain(price, effective)
                    * dc.maker_tp_fee_margin_mult;
                effective = dc.maker_target_profit_pct.max(floor);
            }
            // Price the fee where it is actually charged: at the exit this
            // target implies. Checking it at the ENTRY price is what let the
            // original bug through, and would let it through again here.
            let fee_at_exit = crate::venues::exit_fee_pct_at_gain(price, effective);
            assert!(
                effective > fee_at_exit,
                "at entry ${price:.2} the effective TP target {:.2}% does not clear \
                 the exit fee it will actually pay, {:.2}% — a take-profit there books a loss",
                effective * dec!(100),
                fee_at_exit * dec!(100),
            );
            price += dec!(0.01);
        }
    }

    /// The entry-price approximation is optimistic, and the test above must be
    /// able to see that. Pins the direction of the error Fable identified:
    /// on any contract below ~$0.50 the exit fee EXCEEDS the entry-price
    /// estimate, so a floor built on the estimate alone is too low.
    #[cfg(not(feature = "us_retail"))]
    #[test]
    fn pricing_the_fee_at_entry_understates_what_the_exit_pays() {
        let entry = dec!(0.18);
        let gain = dec!(0.07);
        let at_entry = crate::venues::exit_only_fee_pct(entry);
        let at_exit = crate::venues::exit_fee_pct_at_gain(entry, gain);
        assert!(
            at_exit > at_entry,
            "exit fee {at_exit} must exceed the entry-price estimate {at_entry} \
             on a cheap contract — this is why the floor is priced at the exit",
        );
        // And the approximation is exactly the zero-gain case.
        assert_eq!(crate::venues::exit_fee_pct_at_gain(entry, dec!(0)), at_entry);
    }

    /// The floor must charge ONE leg, not two. Maker pays no entry fee, so
    /// flooring against the round trip would roughly double the target and stall
    /// the viper on the cheap entries it is built for.
    #[test]
    fn maker_floors_against_one_leg_not_the_round_trip() {
        let entry = dec!(0.18);
        let single = crate::venues::exit_only_fee_pct(entry);
        let round_trip = crate::venues::round_trip_fee_pct(entry);
        assert_eq!(
            round_trip, single * dec!(2),
            "the round trip is exactly two legs; Maker must be charged one",
        );
        // US Retail charges no taker fee, so the floor is inert there rather than
        // wrong — assert it binds only on the venues that actually charge one.
        #[cfg(not(feature = "us_retail"))]
        assert!(
            single > dec!(0), "this venue charges a taker fee, so the floor must bind",
        );
    }

    /// The observed trade must no longer qualify as a take-profit.
    ///
    /// intl-only: it replays a Polymarket International round trip and prices it
    /// with that venue's own fee function, which is not compiled for the others.
    #[cfg(feature = "intl_clob")]
    #[test]
    fn the_losing_take_profit_of_2026_08_30_no_longer_fires() {
        let dc = DynamicConfig::default();
        let entry = dec!(0.18);
        let bid = dec!(0.19);
        let profit_pct = (bid - entry) / entry;

        let fee_floor = crate::venues::exit_only_fee_pct(entry) * dc.maker_tp_fee_margin_mult;
        let target = dc.maker_target_profit_pct.max(fee_floor);
        assert!(
            profit_pct < target,
            "gain {:.2}% must not clear target {:.2}% — it netted -$0.052 in production",
            profit_pct * dec!(100), target * dec!(100),
        );

        // And confirm the arithmetic that made it a loss, from the venue's own
        // fee function rather than a restatement of it.
        let shares = dec!(66.666666666666666666666666667);
        let gross = (bid - entry) * shares;
        let exit_fee = crate::venues::intl::taker_fee(crate::config::INTL_TAKER_FEE_RATE, bid, shares);
        assert!(
            gross - exit_fee < dec!(0),
            "gross ${gross:.4} minus exit fee ${exit_fee:.4} was a net loss",
        );
    }
}

#[cfg(test)]
mod maturation_gate_tests {
    use super::{effective_min_market_age, maturation_wait};
    use rust_decimal_macros::dec;

    /// The case that motivated the scaling. A Kalshi KXBTC15M market runs about
    /// thirteen minutes and stops accepting entries 60s before close, so a flat
    /// 600s wait left roughly two minutes of tradeable life.
    #[test]
    fn fifteen_minute_market_matures_well_inside_its_life() {
        let wait = maturation_wait(0, 780, 600, dec!(0.25));
        assert_eq!(wait, 195, "a 13-minute market should not wait 10 minutes");
        assert!(wait < 780 - 60, "maturation must clear before the RTB cutoff");
    }

    /// A daily market is long enough that the fraction never binds, so the
    /// operator's absolute wait is served in full — the scaling must not quietly
    /// shorten maturation where it was already correct.
    #[test]
    fn daily_market_keeps_the_full_absolute_wait() {
        assert_eq!(maturation_wait(0, 86_400, 600, dec!(0.25)), 600);
    }

    /// Lifetime is age + remaining, so it stays put as the market ages. Without
    /// this the required wait would shrink every tick and the gate would admit a
    /// market it had just rejected.
    #[test]
    fn required_wait_is_stable_as_the_market_ages() {
        let at_open  = maturation_wait(0,   780, 600, dec!(0.25));
        let mid_life = maturation_wait(300, 480, 600, dec!(0.25));
        let late     = maturation_wait(700,  80, 600, dec!(0.25));
        assert_eq!(at_open, mid_life);
        assert_eq!(at_open, late);
    }

    /// A market already past its close is the expiry gate's business, not this
    /// one's, so it keeps the absolute wait rather than collapsing to zero.
    #[test]
    fn expired_market_keeps_the_absolute_wait() {
        assert_eq!(maturation_wait(0, -60, 600, dec!(0.25)), 600);
        assert_eq!(maturation_wait(0, 0, 600, dec!(0.25)), 600);
    }

    /// A zero or negative fraction disables scaling rather than admitting every
    /// market instantly — an operator clearing the field must not silently turn
    /// the maturation gate off.
    #[test]
    fn zero_or_negative_fraction_disables_scaling() {
        assert_eq!(maturation_wait(0, 780, 600, dec!(0)), 600);
        assert_eq!(maturation_wait(0, 780, 600, dec!(-1)), 600);
    }

    /// Scaling never lengthens the wait: the fraction is a ceiling on the
    /// operator's number, never a floor.
    #[test]
    fn scaling_is_a_ceiling_never_a_floor() {
        assert_eq!(maturation_wait(0, 86_400, 60, dec!(0.25)), 60);
    }

    /// No close time means nothing to scale against.
    #[test]
    fn market_without_close_time_keeps_the_absolute_wait() {
        assert_eq!(effective_min_market_age(0, None, 600, dec!(0.25)), 600);
    }
}

#[cfg(test)]
mod spread_gate_wording_tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    /// The fee floor in price units, as the gate computes it.
    fn fee_floor(price: Decimal) -> Decimal {
        crate::venues::round_trip_fee_pct(price) * price
    }

    /// Below the fee, no `maker_min_spread` rescues the quote.
    ///
    /// The old message — "spread 0.001 < min 0.010" — reads as an invitation to
    /// lower the knob, and an operator chasing it would only quote into a
    /// guaranteed loss. Polymarket International's event books quote a tenth of
    /// a cent against fees several times larger, which is what a squadron that
    /// patrols and never trades is actually telling you.
    #[test]
    fn an_event_book_spread_is_below_the_fee_floor() {
        // A tenth of a cent, as observed on the intl politics and sports books.
        let spread = dec!(0.001);
        for price in [dec!(0.50), dec!(0.10), dec!(0.03)] {
            let floor = fee_floor(price);
            // Venues with no taker fee cannot be below it — see the US case below.
            if floor > Decimal::ZERO {
                assert!(
                    spread < floor,
                    "at price {price} a 0.1c spread must be under the {floor} fee floor",
                );
            }
        }
    }

    /// A workable spread must NOT be labelled unquotable — that would send an
    /// operator away from a knob that really would help.
    #[test]
    fn a_healthy_spread_is_above_the_fee_floor() {
        let floor = fee_floor(dec!(0.50));
        assert!(dec!(0.04) > floor || floor == Decimal::ZERO,
                "a 4c spread should clear the fee floor at mid");
    }

    /// US Retail takes no taker fee, so the fee branch is inert there rather
    /// than wrong — every spread is "above" a zero floor.
    #[test]
    fn a_zero_fee_venue_never_reports_below_fee() {
        let floor = fee_floor(dec!(0.50));
        #[cfg(feature = "us_retail")]
        assert_eq!(floor, Decimal::ZERO, "US Retail charges no taker fee");
        #[cfg(not(feature = "us_retail"))]
        assert!(floor > Decimal::ZERO, "fee-charging venues must have a real floor");
    }
}

#[cfg(test)]
mod ghost_exit_coverage_tests {
    /// No viper may gate an exit on the RAW `fill_confirmed_at` field.
    ///
    /// That field is stamped only by the venue's fill listener (and, on intl, by
    /// on-chain reconciliation). Ghost mode places no order and holds nothing
    /// on-chain, so it stays None for a simulated position's whole life. Every
    /// viper gated its non-catastrophic exits on it, which meant simulation cut
    /// every loser and let every winner run to rotation or expiry — a one-way
    /// ratchet that made ghost P&L worse than the strategy warranted. GBoost was
    /// worst: the gate wrapped its entire exit block, so a ghost position had no
    /// exits at all.
    ///
    /// `Position::fill_effective_at(ghost)` is the ghost-aware accessor. This
    /// asserts against the source because the failure is invisible at the call
    /// site — the code reads like an ordinary confirmation check.
    #[test]
    fn vipers_use_the_ghost_aware_fill_accessor() {
        const VIPERS: &[(&str, &str)] = &[
            ("maker",         include_str!("maker_impl.rs")),
            ("gboost",        include_str!("gboost_impl.rs")),
            ("momentum",      include_str!("momentum_impl.rs")),
            ("convergence",   include_str!("convergence_impl.rs")),
            ("trendreversal", include_str!("trendreversal_impl.rs")),
            ("arbitrage",     include_str!("arbitrage_impl.rs")),
        ];
        let mut offenders = Vec::new();
        for (name, src) in VIPERS {
            // Production code only. Test modules legitimately construct and
            // inspect the raw field — and this very test mentions the name, so
            // scanning itself would make it fail on its own text.
            let prod = src.split("#[cfg(test)]").next().unwrap_or(src);
            for (i, line) in prod.lines().enumerate() {
                let l = line.trim();
                if l.starts_with("//") || !l.contains("fill_confirmed_at") { continue; }
                // Constructing a Position (test fixtures, entry records) is fine;
                // only READING it to gate behaviour is the hazard.
                if l.contains("fill_confirmed_at:") { continue; }
                offenders.push(format!("{name}:{} — {l}", i + 1));
            }
        }
        assert!(
            offenders.is_empty(),
            "these read fill_confirmed_at directly and so are blind in ghost mode; \
             use position.fill_effective_at(dc.ghost_mode): {offenders:#?}",
        );
    }
}

#[cfg(test)]
mod bid_anchoring_tests {
    use super::*;

    /// Reproduces the Kalshi book of 2026-08-27 that cost -31.37% at entry.
    fn quote(ask: Decimal, bid: Decimal, buffer: Decimal, improve: bool) -> Decimal {
        let raw = ask - buffer;
        let capped = if improve { raw.min(bid + MAKER_TICK_SIZE) } else { raw };
        floor_to_tick_size(capped.min(ask - dec!(0.02)))
    }

    /// The bug: on a wide book the ask-anchored price crosses the spread.
    ///
    /// YES bid 0.35 / ask 0.53. The maker quoted 0.51 — sixteen cents above the
    /// best bid — the fill marked to the bid immediately, and the catastrophic
    /// stop booked -31.37%. The book never moved; the loss was the entry price.
    #[test]
    fn a_wide_book_no_longer_crosses_the_spread() {
        let (ask, bid) = (dec!(0.53), dec!(0.35));
        assert_eq!(quote(ask, bid, dec!(0.02), false), dec!(0.51), "the old behaviour");
        assert_eq!(quote(ask, bid, dec!(0.02), true), dec!(0.36), "improve the bid by one tick");
    }

    /// The NO side priced by the same formula and had the same exposure:
    /// NO bid 0.47 / ask 0.65 would have quoted 0.63.
    #[test]
    fn the_no_side_is_capped_too() {
        let (ask, bid) = (dec!(0.65), dec!(0.47));
        assert_eq!(quote(ask, bid, dec!(0.02), false), dec!(0.63), "the old behaviour");
        assert_eq!(quote(ask, bid, dec!(0.02), true), dec!(0.48));
    }

    /// On a tight book the cap must not bind — otherwise this would quietly
    /// reprice every normal quote and change fill rates everywhere.
    #[test]
    fn a_tight_book_is_unaffected() {
        let (ask, bid) = (dec!(0.52), dec!(0.50));
        assert_eq!(quote(ask, bid, dec!(0.02), false), dec!(0.50));
        assert_eq!(quote(ask, bid, dec!(0.02), true), dec!(0.50), "cap must not bind here");
    }

    /// Turning the knob off restores the previous behaviour exactly, so an
    /// operator who wants faster fills can still have them.
    #[test]
    fn the_knob_restores_the_old_behaviour() {
        for (ask, bid) in [(dec!(0.53), dec!(0.35)), (dec!(0.65), dec!(0.47)), (dec!(0.52), dec!(0.50))] {
            let old = ask - dec!(0.02);
            assert_eq!(quote(ask, bid, dec!(0.02), false), floor_to_tick_size(old.min(ask - dec!(0.02))));
        }
    }

    /// The cross-buffer clamp still wins when it is the tighter of the two, so
    /// inventory skew can never push the quote up against the ask.
    #[test]
    fn the_cross_buffer_still_binds() {
        // A bid one tick under the ask would otherwise allow 0.52 on a 0.53 ask.
        let q = quote(dec!(0.53), dec!(0.52), dec!(0.00), true);
        assert!(q <= dec!(0.51), "cross buffer must keep the quote off the ask, got {q}");
    }
}

#[cfg(test)]
mod maker_oracle_drift_exit_tests {
    use super::{maker_adverse_drift, maker_drift_breached};
    use rust_decimal_macros::dec;

    /// A YES bid is hurt when the oracle FALLS.
    #[test]
    fn a_falling_oracle_is_adverse_to_yes() {
        assert_eq!(maker_adverse_drift(dec!(-0.002), true), dec!(0.002));
        assert_eq!(maker_adverse_drift(dec!(0.002),  true), dec!(-0.002));
    }

    /// A NO bid is hurt when the oracle RISES — the mirror image.
    #[test]
    fn a_rising_oracle_is_adverse_to_no() {
        assert_eq!(maker_adverse_drift(dec!(0.002),  false), dec!(0.002));
        assert_eq!(maker_adverse_drift(dec!(-0.002), false), dec!(-0.002));
    }

    /// The Ireland shape: a NO position with the oracle running up against it.
    #[test]
    fn an_adverse_move_past_the_threshold_exits() {
        let adverse = maker_adverse_drift(dec!(0.0020), false);
        assert!(maker_drift_breached(adverse, dec!(0.0015)));
    }

    /// A favorable move must never trigger an exit — the sign error that would make
    /// this change actively harmful rather than merely useless.
    #[test]
    fn a_favorable_move_never_exits() {
        assert!(!maker_drift_breached(maker_adverse_drift(dec!(-0.010), false), dec!(0.0015)));
        assert!(!maker_drift_breached(maker_adverse_drift(dec!(0.010),  true),  dec!(0.0015)));
    }

    /// Ordinary noise below the threshold is held, so the maker still gets paid for
    /// sitting through normal book chop.
    #[test]
    fn noise_below_the_threshold_is_held() {
        assert!(!maker_drift_breached(maker_adverse_drift(dec!(0.0009), false), dec!(0.0015)));
    }

    /// Zero disables the mechanism, falling back to the OBI path alone.
    #[test]
    fn zero_disables_the_drift_exit() {
        assert!(!maker_drift_breached(dec!(0.05), dec!(0)));
    }
}
