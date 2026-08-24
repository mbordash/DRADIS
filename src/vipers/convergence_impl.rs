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

/// Convergence Strategy — Macro-Conviction Directional Viper
///
/// # Thesis
///
/// Where `MomentumStrategy` trades 5-second oracle velocity and `TrendCapture`
/// trades 10–60 minute drift, Convergence trades **institutional + derivatives
/// agreement** — a slower, higher-conviction regime signal the price-based Vipers
/// cannot see. It is the first Viper that *opens* a directional position off the
/// macro Raptor stack rather than merely gating on it.
///
/// # Entry — all conditions must agree on one direction
///   - Tide Raptor `institutional_pulse` beyond `CONVERGENCE_PULSE_THRESHOLD`
///     (sign = direction: >0 institutions bid → buy YES; <0 → buy NO), AND
///   - `tide_coherence ≥ CONVERGENCE_COHERENCE_MIN` (the three ETFs agree), AND
///   - Derivatives Raptor `cvd_ratio` confirms the same side
///     (bull: cvd ≥ 1+margin; bear: cvd ≤ 1−margin), AND
///   - `oi_delta_pct ≥ CONVERGENCE_OI_MIN_BUILD` (positioning not unwinding).
///
/// # Scope
///   BTC-only — `institutional_pulse` is BTC-only (no ETH/SOL ETF analog), so the
///   strategy no-ops for other assets. Naturally **US-cash-hours-only**: the pulse
///   is zero outside the session, so `|pulse| ≥ threshold` cannot be met. Entries
///   are marketable FAK takers at the touch so they fill while conviction is live.
///
/// # Risk
///   Fixed tiny size (`CONVERGENCE_POSITION_SIZE_USDC`) while it proves itself
///   live, capped by `CONVERGENCE_MAX_EXPOSURE_USDC`. One position per market.
///   Exits on take-profit, stop-loss, **signal decay/reversal** (the pulse flips
///   or coherence collapses), or near-expiry.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
use chrono::Utc;
use tracing::debug;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::vipers::is_drawdown_limit_hit;
use crate::config;
use crate::venues::core::{MarketId, TimeInForce};

const STRATEGY_NAME: &str = "ConvergenceStrategy";

/// Stateful Convergence strategy implementation.
pub struct ConvergenceStrategyImpl {
    /// Per-token cooldown after any exit. Key: token_id, Value: Instant of exit.
    post_exit_cooldown: Mutex<HashMap<MarketId, Instant>>,
    /// Per-market cooldown after ANY stop exit (SL or catastrophic). Key: condition_id.
    /// Blocks BOTH legs so the strategy cannot flip to the opposite side and get
    /// whipsawed again in the same chop (2026-07-15: NO stopped −11%, pulse flipped,
    /// YES bought 23 min later in the same hour → −19.5% catastrophic).
    stop_market_cooldown: Mutex<HashMap<String, Instant>>,
    /// Viper-level exit-signal cooldown to prevent FAK-miss re-fire storms.
    last_exit_signal_at: Mutex<Option<Instant>>,
    /// Best bid observed at entry-signal time, per token. The catastrophic stop
    /// measures adverse MARKET movement against this reference instead of the
    /// entry ask: marking a fresh fill against the bid always shows an instant
    /// paper loss equal to the spread, and with wide books that alone crossed
    /// the catastrophic floor and stopped positions the same second they opened
    /// (2026-07-14, −16.2% with zero price movement).
    entry_bid: Mutex<HashMap<MarketId, Decimal>>,
    /// Entry-signal persistence streak: (condition_id, want_bull, first_seen, last_seen).
    /// The full signal stack must hold continuously for
    /// CONVERGENCE_ENTRY_PERSISTENCE_SECS before an entry fires (anti-burst debounce —
    /// 2026-07-15: both losers were stopped <70s after entering on a transient
    /// mid-vol-burst pulse).
    signal_streak: Mutex<Option<(String, bool, Instant, Instant)>>,
}

impl ConvergenceStrategyImpl {
    pub fn new() -> Self {
        Self {
            post_exit_cooldown: Mutex::new(HashMap::new()),
            stop_market_cooldown: Mutex::new(HashMap::new()),
            last_exit_signal_at: Mutex::new(None),
            entry_bid: Mutex::new(HashMap::new()),
            signal_streak: Mutex::new(None),
        }
    }

    fn is_btc(ctx: &StrategyContext) -> bool {
        // Intl squadrons carry the underlying in crypto_filter ("BTC"); US
        // crypto-wing squadrons carry a venue key ("US-CRYPTO"), so fall back
        // to the market name to recognize a BTC market there.
        if ctx.crypto_filter.eq_ignore_ascii_case("btc") {
            return true;
        }
        let name = ctx.market.market_name.to_lowercase();
        name.contains("btc") || name.contains("bitcoin")
    }

    fn record_exit(&self, token_id: &MarketId) {
        if let Ok(mut map) = self.post_exit_cooldown.lock() {
            map.insert(token_id.clone(), Instant::now());
        }
        if let Ok(mut map) = self.entry_bid.lock() {
            map.remove(token_id);
        }
        if let Ok(mut last) = self.last_exit_signal_at.lock() {
            *last = Some(Instant::now());
        }
    }

    /// Arm the market-wide cooldown after a stop exit (SL or catastrophic) so neither
    /// leg of this market can be re-entered until CONVERGENCE_STOP_MARKET_COOLDOWN_SECS
    /// elapses.
    fn record_market_stop(&self, condition_id: &str) {
        if let Ok(mut map) = self.stop_market_cooldown.lock() {
            map.insert(condition_id.to_string(), Instant::now());
        }
    }
}

impl Default for ConvergenceStrategyImpl {
    fn default() -> Self { Self::new() }
}

/// Do the 10-minute and 60-minute oracle drift legs actively DISAGREE?
///
/// Both legs must clear `deadband` for the disagreement to count: a flat leg is
/// "no opinion", not a conflict, and treating it as one would veto most entries
/// during quiet tape. See `CONVERGENCE_DRIFT_COHERENCE_DEADBAND_PCT`.
fn drift_incoherent(drift_10m: Decimal, drift_60m: Decimal, deadband: Decimal) -> bool {
    drift_10m.abs() >= deadband
        && drift_60m.abs() >= deadband
        && (drift_10m.is_sign_positive() != drift_60m.is_sign_positive())
}

/// Is the 5-second oracle velocity actively running AGAINST the intended side?
///
/// A deadband test, not a confirmation test — `velocity == 0` (flat feed, or
/// insufficient history) must PASS. See `CONVERGENCE_VELOCITY_OPPOSITION_PCT`.
fn velocity_opposes_entry(velocity: Decimal, want_bull: bool, deadband: Decimal) -> bool {
    if want_bull { velocity <= -deadband } else { velocity >= deadband }
}

#[async_trait]
impl Strategy for ConvergenceStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        // "Why no trades?" registry feed (GET /api/vipers/status).
        let idle = |r: &str| crate::helpers::viper_status::report_reason(&ctx.crypto_filter, &self.name(), r);
        if !dc.enable_convergence {
            idle("disabled in config");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global risk + scope gates ─────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            idle("session drawdown limit hit");
            return Ok(StrategySignal::NoSignal);
        }
        // BTC-only: institutional_pulse has no ETH/SOL analog.
        if !Self::is_btc(ctx) {
            idle("BTC-only strategy (no ETF tide for this asset)");
            return Ok(StrategySignal::NoSignal);
        }
        // Market maturation — avoid the thin, noisy book at market open.
        let secs_since_start = (Utc::now() - ctx.market_started_at).num_seconds();
        if secs_since_start < config::CONVERGENCE_MARKET_WARMUP_SECS {
            idle("market warmup");
            return Ok(StrategySignal::NoSignal);
        }

        let snap   = &ctx.snapshot;
        let market = &ctx.market;

        // ── Macro conviction ─────────────────────────────────────────────────
        let pulse = snap.institutional_pulse;
        let coh   = snap.tide_coherence;
        let cvd   = snap.cvd_ratio;
        let oi    = snap.oi_delta_pct;

        // Direction from the institutional pulse sign (also gates US-hours, since
        // pulse is zero outside the cash session → neither branch fires).
        let want_bull = pulse >= dc.convergence_pulse_threshold;
        let want_bear = pulse <= -dc.convergence_pulse_threshold;
        if !want_bull && !want_bear {
            idle("no institutional pulse (or outside US hours)");
            return Ok(StrategySignal::NoSignal);
        }

        // The three ETFs must cohere.
        if coh < dc.convergence_coherence_min {
            idle("ETF tide not coherent");
            return Ok(StrategySignal::NoSignal);
        }

        // Open interest must not be unwinding (de-leveraging / squeeze).
        if oi < config::CONVERGENCE_OI_MIN_BUILD {
            idle("open interest unwinding");
            return Ok(StrategySignal::NoSignal);
        }

        // ── 60m drift exhaustion ceiling (2026-06-30) ─────────────────────────
        // Block when BTC has already moved hard in the entry direction over the
        // last hour — the move is priced in and prone to revert. Audit: losers
        // entered at avg |drift_60m| ≈ $116 vs winners ≈ $34.
        let exhaustion_thr = config::oracle_threshold(
            config::CONVERGENCE_EXHAUSTION_DRIFT_60M_PCT, snap.oracle_price);
        let drift_60m = snap.oracle_drift_60m;
        if (want_bull && drift_60m >= exhaustion_thr)
            || (want_bear && drift_60m <= -exhaustion_thr)
        {
            debug!(" Convergence blocked: 60m drift exhausted ({:.0} vs ±{:.0}) — move already priced in",
                drift_60m, exhaustion_thr);
            idle("60m drift exhausted (move priced in)");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Drift coherence: 10m and 60m must not disagree (2026-08-11) ───────
        // The exhaustion ceiling above only catches drift ALREADY RUN IN the entry
        // direction. It cannot see the opposite failure: a short-term bounce
        // against a strong hourly downtrend, which reads as fresh momentum to the
        // pulse but is a counter-trend entry. Trade id 347 was exactly that —
        // drift_10m +78.3 (bullish) against drift_60m −361.8 (strongly bearish),
        // bought YES at $0.66, stopped −13.6% sixty seconds later. The winner in
        // the same batch (id 344) had both legs bearish and agreeing.
        //
        // Only fires when BOTH legs clear the deadband: one flat leg is "no
        // opinion", not a conflict.
        let drift_10m = snap.oracle_drift_10m;
        let coherence_deadband = config::oracle_threshold(
            dc.convergence_drift_coherence_deadband_pct, snap.oracle_price);
        if drift_incoherent(drift_10m, drift_60m, coherence_deadband) {
            debug!(" Convergence blocked: drift incoherent (10m={:.0} vs 60m={:.0}, deadband=±{:.0}) — counter-trend bounce",
                drift_10m, drift_60m, coherence_deadband);
            idle("10m/60m drift disagree (counter-trend)");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Velocity opposition veto (2026-08-11) ─────────────────────────────
        // Never pay up for a side the price is actively moving away from. Trade
        // id 347 bought YES while the 5s oracle velocity was −4.01.
        //
        // A deadband, NOT a confirmation requirement: `velocity` is 0 when the
        // feed is flat or history is short, and the batch winner (id 344) entered
        // at velocity exactly 0.00 — demanding positive confirmation would have
        // blocked the only good trade of the day.
        let velocity = snap.velocity;
        let velocity_deadband = config::oracle_threshold(
            dc.convergence_velocity_opposition_pct, snap.oracle_price);
        if velocity_opposes_entry(velocity, want_bull, velocity_deadband) {
            debug!(" Convergence blocked: velocity {:.2} opposes {} entry (deadband=±{:.2})",
                velocity, if want_bull { "bull" } else { "bear" }, velocity_deadband);
            idle("oracle velocity opposes entry");
            return Ok(StrategySignal::NoSignal);
        }

        // Derivatives taker flow must CONFIRM the side. `cvd == 0` means no FAPI
        // data → no confirmation → stand down (conviction requires live confirmation).
        let cvd_confirms = if want_bull {
            cvd >= dec!(1) + dc.convergence_cvd_confirm_margin
        } else {
            cvd > dec!(0) && cvd <= dec!(1) - dc.convergence_cvd_confirm_margin
        };
        if !cvd_confirms {
            idle("taker flow (CVD) does not confirm");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Adverse order-book imbalance gate (2026-06-30) ────────────────────
        // Direction comes from the slow institutional pulse, but we must not enter
        // INTO a book stacked the other way. obi_yes = (yes_bid − yes_ask)/total.
        // Audit (15 trades): every NO entry with obi_yes > +0.5 lost (4/4, incl. a
        // −20.9% catastrophic); no winner on either side had adverse OBI ≥ 0.5.
        //   NO  (want_bear): adverse if YES has buy pressure  → obi_yes > +block
        //   YES (want_bull): adverse if YES has sell pressure → obi_yes < −block
        // Inputs follow `obi_use_whole_book`. The empty-book fallback stays 0
        // (neutral) rather than the shared accessor's -1: this gate is about an
        // adverse book in a known direction, and no data is not evidence of one.
        let (yes_bid_d, yes_ask_d) = snap.yes_depths(dc.obi_use_whole_book);
        let yes_depth = yes_bid_d + yes_ask_d;
        let obi_yes = if yes_depth > dec!(0) {
            (yes_bid_d - yes_ask_d) / yes_depth
        } else {
            dec!(0)
        };
        let obi_adverse = if want_bull {
            obi_yes < -dc.convergence_obi_adverse_block
        } else {
            obi_yes > dc.convergence_obi_adverse_block
        };
        if obi_adverse {
            debug!(" Convergence blocked: adverse OBI (obi_yes={:.2}, want_bull={}) — book stacked against entry",
                obi_yes, want_bull);
            idle("book stacked against entry (OBI)");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Entry-signal persistence debounce (anti-burst, 2026-07-15) ───────
        // All signal gates passed. Require the signal to hold continuously for
        // CONVERGENCE_ENTRY_PERSISTENCE_SECS before entering: a pulse that fires
        // mid-vol-burst decays within a tick or two (both of today's losers were
        // stopped <70s after entry), while genuine institutional flow persists.
        // A direction flip or a sighting gap > CONTINUITY_GAP resets the clock.
        if let Ok(mut streak) = self.signal_streak.lock() {
            let now = Instant::now();
            let held_long_enough = match streak.as_mut() {
                Some((cid, dir, first_seen, last_seen))
                    if *cid == market.condition_id
                        && *dir == want_bull
                        && last_seen.elapsed().as_secs()
                            <= config::CONVERGENCE_SIGNAL_CONTINUITY_GAP_SECS =>
                {
                    *last_seen = now;
                    first_seen.elapsed().as_secs() >= config::CONVERGENCE_ENTRY_PERSISTENCE_SECS
                }
                _ => {
                    // First sighting, direction flip, market rotation, or stale streak.
                    *streak = Some((market.condition_id.clone(), want_bull, now, now));
                    false
                }
            };
            if !held_long_enough {
                debug!(" Convergence debounce: signal must persist {}s before entry",
                    config::CONVERGENCE_ENTRY_PERSISTENCE_SECS);
                idle("signal persistence debounce");
                return Ok(StrategySignal::NoSignal);
            }
        }

        // ── Pick the token + touch price ──────────────────────────────────────
        let (token_id, ask, bid, fee_bps) = if want_bull {
            (market.yes_token.clone(), snap.yes_ask, snap.yes_bid, market.yes_fee_bps as u16)
        } else {
            (market.no_token.clone(), snap.no_ask, snap.no_bid, market.no_fee_bps as u16)
        };

        // ── Price / spread gates ──────────────────────────────────────────────
        if ask < dc.convergence_min_entry_price || ask > dc.convergence_max_entry_price {
            idle("ask outside entry price band");
            return Ok(StrategySignal::NoSignal);
        }
        // Coin-flip skip band: avoid the ~$0.50 zone (max binary uncertainty, most
        // gap-prone near resolution — the audit's worst price band).
        if dc.convergence_skip_band_low < dc.convergence_skip_band_high
            && ask >= dc.convergence_skip_band_low
            && ask <= dc.convergence_skip_band_high
        {
            debug!(" Convergence blocked: ask {:.3} in coin-flip skip band [{:.2}, {:.2}]",
                ask, dc.convergence_skip_band_low, dc.convergence_skip_band_high);
            idle("coin-flip price band");
            return Ok(StrategySignal::NoSignal);
        }
        let spread = if ask > dec!(0) { (ask - bid) / ask } else { Decimal::ONE };
        if spread > dc.convergence_max_token_spread_pct {
            debug!(" Convergence blocked: spread {:.1}% > max (ask={:.3} bid={:.3})",
                spread * dec!(100), ask, bid);
            idle("spread too wide");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Per-token cooldown ────────────────────────────────────────────────
        if let Ok(map) = self.post_exit_cooldown.lock() {
            if let Some(t) = map.get(&token_id) {
                if t.elapsed().as_secs() < config::CONVERGENCE_POST_EXIT_COOLDOWN_SECS {
                    idle("post-exit cooldown");
                    return Ok(StrategySignal::NoSignal);
                }
            }
        }

        // ── Market-wide stop cooldown ─────────────────────────────────────────
        // After ANY stop exit (SL or catastrophic) in this market, block BOTH legs
        // (not just the stopped token) so we don't flip to the opposite side and get
        // whipsawed again in the same chop (2026-07-08: YES @0.60 then NO @0.60, both
        // −21%; 2026-07-15: NO SL'd −11%, YES bought 23 min later → −19.5%).
        if let Ok(map) = self.stop_market_cooldown.lock() {
            if let Some(t) = map.get(&market.condition_id) {
                if t.elapsed().as_secs() < config::CONVERGENCE_STOP_MARKET_COOLDOWN_SECS {
                    debug!(" Convergence blocked: market in post-stop cooldown ({}s left)",
                        config::CONVERGENCE_STOP_MARKET_COOLDOWN_SECS.saturating_sub(t.elapsed().as_secs()));
                    idle("post-stop market cooldown");
                    return Ok(StrategySignal::NoSignal);
                }
            }
        }

        // ── Exposure + one-position-per-market checks ─────────────────────────
        let size = dc.convergence_position_size_usdc;
        {
            let pos_map = ctx.positions.lock().await;
            let mut exposure = Decimal::ZERO;
            for (key, pos) in pos_map.iter() {
                let (sname, tok) = (&key.strategy, &key.market);
                if sname != STRATEGY_NAME || key.squadron != ctx.squadron_id { continue; }
                exposure += pos.shares * pos.avg_entry;
                // Don't stack a second position on either leg of this market.
                if tok == &market.yes_token || tok == &market.no_token {
                    idle("position already open on this market");
                    return Ok(StrategySignal::NoSignal);
                }
            }
            if exposure + size > dc.convergence_max_exposure_usdc {
                idle("exposure cap reached");
                return Ok(StrategySignal::NoSignal);
            }
        }

        // ── Liquidity / near-resolution entry gate (2026-06-29) ───────────────
        // Block entries that would gap through the stop: too close to resolution,
        // or our position larger than the resting depth on our future-exit bid.
        let intended_shares = size / ask;
        let exit_bid_depth = if want_bull { snap.yes_bid_depth } else { snap.no_bid_depth };
        let secs_left = market.market_close_time.map(|ct| (ct - Utc::now()).num_seconds());
        if let Some(reason) = crate::vipers::entry_liquidity_gate(secs_left, intended_shares, exit_bid_depth) {
            debug!(" Convergence blocked: {}", reason);
            idle(&reason);
            return Ok(StrategySignal::NoSignal);
        }

        debug!(
            " Convergence {} entry: pulse={:.2} coh={:.2} cvd={:.2} oi={:.3} | {} ask={:.3} size=${:.2}",
            if want_bull { "BULL" } else { "BEAR" },
            pulse, coh, cvd, oi,
            if want_bull { "YES" } else { "NO" }, ask, size,
        );

        // Record the bid at entry time as the catastrophic-stop reference (see
        // `entry_bid` field doc). Overwritten on re-entry; removed on exit.
        if let Ok(mut map) = self.entry_bid.lock() {
            map.insert(token_id.clone(), bid);
        }
        // Consume the signal streak — the next entry must build fresh persistence.
        if let Ok(mut streak) = self.signal_streak.lock() {
            *streak = None;
        }

        // Viper Backtrace: persist the gate/decision state for this entry.
        crate::helpers::metrics::stash_entry_signals_json(token_id.as_str(), serde_json::json!({
            "viper": "Convergence",
            "branch": if want_bull { "BULL" } else { "BEAR" },
            "pulse": pulse.to_string(),
            "coherence": coh.to_string(),
            "cvd": cvd.to_string(),
            "oi": oi.to_string(),
            "ask": ask.to_string(),
            "bid": bid.to_string(),
            "trade_size": size.to_string(),
            "exit_bid_depth": exit_bid_depth.to_string(),
            "secs_left": secs_left,
        }));

        Ok(StrategySignal::Entry {
            params: OrderParams {
                token_id,
                price: ask,
                shares: size / ask,
                fee_bps,
                is_neg_risk: market.is_neg_risk,
                market_name: market.market_name.clone(),
                condition_id: market.condition_id.clone(),
                order_type: TimeInForce::Fak, // marketable taker — fill while conviction is live
                post_only: false,
                ghost_mode: dc.ghost_mode,
            },
            pair_params: None,
        })
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;

        // ── Soft-exit cooldown (take-profit / signal-decay only) ──────────────
        // After we emit a *discretionary* Exit signal, suppress further discretionary
        // re-fires for EXIT_SIGNAL_COOLDOWN_SECS so patrol has time to execute before
        // we re-fire (prevents FAK-miss re-fire storms).
        //
        // CRITICAL: safety-critical exits (stop-loss, catastrophic) are NEVER gated by
        // this cooldown. A prior soft-exit FAK miss must not freeze the stop-loss while
        // the position bleeds. (Jun 25 trade id 88: entry $0.23 — a discretionary FAK
        // miss froze ALL exit re-evaluation; the bid collapsed $0.23→$0.13 and the
        // position realized −43.5%, far past both the 10% stop and the 20% catastrophic
        // floor, because the blanket cooldown also gated the stop-loss.)
        let soft_exit_cooldown_active = {
            let last = self.last_exit_signal_at.lock().unwrap();
            match *last {
                Some(t) => t.elapsed().as_secs() < config::CONVERGENCE_EXIT_SIGNAL_COOLDOWN_SECS,
                None => false,
            }
        };

        let snap   = &ctx.snapshot;
        let market = &ctx.market;
        let pulse  = snap.institutional_pulse;
        let coh    = snap.tide_coherence;

        struct PendingExit {
            token_id:     MarketId,
            bid:          Decimal,
            shares:       Decimal,
            fee_bps:      u16,
            is_neg_risk:  bool,
            market_name:  String,
            condition_id: String,
            ghost_mode:   bool,
            reason:       String,
        }

        let pending: Option<PendingExit> = {
            let pos_map = ctx.positions.lock().await;
            let mut found: Option<PendingExit> = None;

            'outer: for (key, position) in pos_map.iter() {
                if key.squadron != ctx.squadron_id { continue; }
                let (sname, token_id) = (&key.strategy, &key.market);
                if sname != STRATEGY_NAME { continue; }

                let is_yes = token_id == &market.yes_token;
                let bid = if is_yes { snap.yes_bid }
                          else if token_id == &market.no_token { snap.no_bid }
                          else { continue };

                let avg_entry = position.avg_entry;
                if avg_entry <= dec!(0) { continue; }

                let fee_bps = if is_yes { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 };
                let secs_held = (Utc::now() - position.opened_at).num_seconds();
                let profit_margin = (bid - avg_entry) / avg_entry;

                let make_exit = |reason: String| PendingExit {
                    token_id: token_id.clone(),
                    bid,
                    shares: position.shares,
                    fee_bps,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                    ghost_mode: dc.ghost_mode,
                    reason,
                };

                // Catastrophic stop — ALWAYS active (pre- AND post-confirmation),
                // ungated by the soft-exit cooldown and the minimum hold. This is the
                // hard floor that must never be frozen by a prior FAK-miss cooldown.
                // Previously this only existed in the pre-confirmation branch, so a
                // CONFIRMED position had no catastrophic backstop at all (root cause of
                // the −43.5% overshoot on trade id 88).
                //
                // The threshold scales with the LIVE stop-loss (2×) rather than a fixed
                // -20%: with a tight 5% stop the old -20% floor let fast whipsaws (held <
                // MIN_HOLD, so the normal stop can't fire yet) cost 4× the intended risk.
                // Clamped so it can never be looser than CONVERGENCE_CATASTROPHIC_SL_PCT.
                //
                // 2026-07-14: measured against the bid AT ENTRY (when recorded), not the
                // entry ask. Mark-to-bid vs ask shows an instant paper loss equal to the
                // spread, which alone crossed this floor on wide books and stopped
                // positions the same second they opened. The catastrophic stop exists to
                // catch adverse MARKET moves; the regular SL (vs avg_entry, after
                // MIN_HOLD) still accounts for the spread in realized terms.
                let catastrophic_pct =
                    (-(dc.convergence_stop_loss_pct * config::CONVERGENCE_CATASTROPHIC_SL_MULT))
                        .max(config::CONVERGENCE_CATASTROPHIC_SL_PCT);
                // A TRUE entry bid (recorded at signal time) makes the move below
                // spread-neutral. Without one we fall back to avg_entry, which is an
                // ask — that comparison carries the spread and must not drive the
                // tighter dead-zone stop.
                let recorded_entry_bid = self.entry_bid.lock().ok()
                    .and_then(|m| m.get(token_id).copied())
                    .filter(|b| *b > dec!(0));
                let cat_ref = recorded_entry_bid.unwrap_or(avg_entry);
                let cat_move = (bid - cat_ref) / cat_ref;
                if cat_move <= catastrophic_pct {
                    found = Some(make_exit(format!(
                        "ConvergenceCatastrophic: bid=${:.4} move={:.2}% (ref=${:.4}) pnl={:.2}%",
                        bid, cat_move * dec!(100), cat_ref, profit_margin * dec!(100))));
                    break 'outer;
                }

                // ── Dead-zone stop (2026-08-11) ───────────────────────────────
                // For the first CONVERGENCE_MIN_HOLD_SECS the normal stop cannot
                // fire, so the catastrophic floor (1.5× the stop) is the ONLY
                // guard — a nominal 10% stop is really a 15% stop for 60 seconds.
                // Trade id 347 bled to −13.6% inside that window and stopped the
                // instant it became eligible, losing ~4pts to the gap.
                //
                // Safe to run pre-MIN_HOLD *only* because it is measured BID-TO-BID
                // against the entry bid. MIN_HOLD exists to avoid the instant paper
                // loss from marking a fresh fill's ask against the bid; a bid-to-bid
                // move has no spread component, so the artifact cannot occur.
                // Requires a recorded entry bid — otherwise the normal post-MIN_HOLD
                // stop remains the first line of defence, as before.
                if recorded_entry_bid.is_some()
                    && secs_held < config::CONVERGENCE_MIN_HOLD_SECS
                    && cat_move <= -dc.convergence_stop_loss_pct
                {
                    found = Some(make_exit(format!(
                        "ConvergenceDeadZoneSL: bid=${:.4} move={:.2}% bid-to-bid (ref=${:.4}, {}s held) pnl={:.2}%",
                        bid, cat_move * dec!(100), cat_ref, secs_held, profit_margin * dec!(100))));
                    break 'outer;
                }

                // Before fill-confirmation, only the catastrophic move above may exit.
                if position.fill_confirmed_at.is_none() {
                    continue;
                }

                // Stop-loss (after minimum hold) — safety-critical, NEVER gated by the
                // soft-exit cooldown so a prior discretionary FAK miss can't freeze it.
                if secs_held >= config::CONVERGENCE_MIN_HOLD_SECS
                    && profit_margin <= -dc.convergence_stop_loss_pct
                {
                    found = Some(make_exit(format!(
                        "ConvergenceSL: bid=${:.4} loss={:.2}%", bid, profit_margin * dec!(100))));
                    break 'outer;
                }

                // Take-profit (discretionary — suppressed during soft-exit cooldown).
                if !soft_exit_cooldown_active
                    && profit_margin >= dc.convergence_target_profit_pct
                {
                    found = Some(make_exit(format!(
                        "ConvergenceTP: bid=${:.4} profit={:.2}%", bid, profit_margin * dec!(100))));
                    break 'outer;
                }

                // Signal-decay / reversal exit (discretionary — suppressed during
                // soft-exit cooldown): the conviction that opened the position has
                // flipped against it, or coherence has collapsed.
                if !soft_exit_cooldown_active
                    && secs_held >= config::CONVERGENCE_MIN_HOLD_SECS {
                    let half_thr = dc.convergence_pulse_threshold / dec!(2);
                    let pulse_reversed = if is_yes { pulse <= -half_thr } else { pulse >= half_thr };
                    let coherence_collapsed = coh < dc.convergence_coherence_min / dec!(2);
                    if pulse_reversed || coherence_collapsed {
                        found = Some(make_exit(format!(
                            "ConvergenceDecay: bid=${:.4} pulse={:.2} coh={:.2} profit={:.2}%",
                            bid, pulse, coh, profit_margin * dec!(100))));
                        break 'outer;
                    }
                }
            }
            found
        };

        if let Some(p) = pending {
            self.record_exit(&p.token_id);
            // ANY stop exit (regular SL or catastrophic) cools down the WHOLE market
            // (both legs), not just the stopped token, to prevent an immediate
            // opposite-side whipsaw re-entry in the same chop.
            if p.reason.starts_with("ConvergenceCatastrophic")
                || p.reason.starts_with("ConvergenceSL")
            {
                self.record_market_stop(&p.condition_id);
            }
            return Ok(StrategySignal::Exit {
                params: OrderParams {
                    token_id:     p.token_id,
                    price:        p.bid,
                    shares:       p.shares,
                    fee_bps:      p.fee_bps,
                    is_neg_risk:  p.is_neg_risk,
                    market_name:  p.market_name,
                    condition_id: p.condition_id,
                    order_type:   TimeInForce::Fak, // exits are taker — sell at bid crosses
                    post_only:    false,
                    ghost_mode:   p.ghost_mode,
                },
                reason:    p.reason,
                exit_pair: false,
            });
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { STRATEGY_NAME.to_string() }
    fn venue(&self) -> &'static str { "Hourly" }
    fn max_exposure(&self) -> Decimal { config::CONVERGENCE_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Macro conviction (pulse+CVD+OI)" }

    /// Exit order was rejected at placement — clear the soft-exit cooldown so
    /// the rejected attempt does not suppress the next discretionary exit
    /// (roadmap bug #6). Safety exits were never gated by this cooldown.
    fn on_exit_order_failed(&self, _token_id: &crate::venues::core::MarketId) {
        if let Ok(mut last) = self.last_exit_signal_at.lock() {
            *last = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Live values from the 2026-08-11 BTC session (logs/btc-dradis.db, trades
    // 344 and 347). These two gates were added specifically to separate that
    // pair, so they are pinned here: a future retune that lets trade 347 back
    // through, or that starts rejecting trade 344, should fail loudly.
    //
    //   id 344 WIN  −$0.29 avoided: NO  @0.36, drift_10m −30.37, drift_60m −16.39, vel  0.00
    //   id 347 LOSS −$0.745        : YES @0.66, drift_10m +78.33, drift_60m −361.81, vel −4.01
    const ORACLE_344: Decimal = dec!(63949.61);
    const ORACLE_347: Decimal = dec!(63816.00);

    fn coherence_deadband(oracle: Decimal) -> Decimal {
        config::oracle_threshold(config::CONVERGENCE_DRIFT_COHERENCE_DEADBAND_PCT, oracle)
    }
    fn velocity_deadband(oracle: Decimal) -> Decimal {
        config::oracle_threshold(config::CONVERGENCE_VELOCITY_OPPOSITION_PCT, oracle)
    }

    #[test]
    fn winner_344_passes_both_gates() {
        // Bearish entry: both drift legs negative and agreeing.
        assert!(!drift_incoherent(dec!(-30.37), dec!(-16.39), coherence_deadband(ORACLE_344)),
            "trade 344 had coherent (both-bearish) drift and must not be blocked");
        // Velocity was exactly 0.00 — the reason this is a deadband and not a
        // "velocity must confirm" rule.
        assert!(!velocity_opposes_entry(dec!(0), /* want_bull */ false, velocity_deadband(ORACLE_344)),
            "zero velocity is 'no opinion' and must never veto");
    }

    #[test]
    fn loser_347_blocked_by_drift_incoherence() {
        // +78.33 over 10m against -361.81 over 60m: a bounce inside a downtrend.
        assert!(drift_incoherent(dec!(78.33), dec!(-361.81), coherence_deadband(ORACLE_347)),
            "trade 347's opposed drift legs must be blocked");
    }

    #[test]
    fn loser_347_blocked_by_velocity_opposition() {
        // Bought YES while the oracle was falling $4.01 per 5s window.
        assert!(velocity_opposes_entry(dec!(-4.01), /* want_bull */ true, velocity_deadband(ORACLE_347)),
            "trade 347 bought the bullish side into negative velocity and must be blocked");
    }

    #[test]
    fn a_single_flat_leg_is_not_a_conflict() {
        let db = coherence_deadband(ORACLE_347); // ~$31.9
        // 60m flat (below deadband) + 10m strongly bullish → no opinion, not conflict.
        assert!(!drift_incoherent(dec!(200), dec!(-5), db));
        assert!(!drift_incoherent(dec!(-5), dec!(200), db));
        // Both flat → also fine.
        assert!(!drift_incoherent(dec!(3), dec!(-3), db));
        // Both meaningful and opposed → blocked.
        assert!(drift_incoherent(dec!(200), dec!(-200), db));
    }

    #[test]
    fn velocity_gate_is_directional() {
        let db = velocity_deadband(ORACLE_347); // ~$2.55
        // Falling price vetoes a bull entry but confirms a bear one.
        assert!(velocity_opposes_entry(dec!(-10), true,  db));
        assert!(!velocity_opposes_entry(dec!(-10), false, db));
        // Rising price vetoes a bear entry but confirms a bull one.
        assert!(velocity_opposes_entry(dec!(10), false, db));
        assert!(!velocity_opposes_entry(dec!(10), true,  db));
        // Inside the deadband nothing is vetoed, either way.
        assert!(!velocity_opposes_entry(dec!(1), true,  db));
        assert!(!velocity_opposes_entry(dec!(-1), false, db));
    }
}
