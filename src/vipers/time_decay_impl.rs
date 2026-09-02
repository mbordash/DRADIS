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

/// Time Decay (Theta) Strategy
///
/// Exploits YES+NO price convergence toward $1.00 as hourly markets approach expiry.
///
/// ── Maker Entry (0% Fee) ────────────────────────────────────────────────────
/// Polymarket charges 0% on GTC maker fills.  This strategy posts resting GTC
/// bids for BOTH YES and NO tokens simultaneously during the theta window
/// (TIME_DECAY_MIN_SECS_TO_EXPIRY ↔ TIME_DECAY_MAX_SECS_TO_EXPIRY).
///
///   Entry cost  = YES_bid + NO_bid  (0% fee — maker fills)
///   Settlement  = $1.00             (0% fee — automatic at expiry)
///   Net profit  = 1.00 − YES_bid − NO_bid
///
/// Typical hourly market in final 30 min: combined_bid ≈ $0.97 → +$0.03/share.
/// At a $15 position per leg, that's ~$0.45 per round-trip, with zero fee drag.
///
/// Previously used FAK (taker) entries at ask prices, which were structurally
/// unprofitable: taker fee alone (1000 bps × $1.00) = $0.10, wiping all theta.
///
/// ── Exit Paths ──────────────────────────────────────────────────────────────
///   1. Settlement (preferred): hold both legs to market close; receive $1.00
///      automatically from Polymarket — no exit order needed, no exit fee.
///   2. Convergence exit: if combined_bid reaches TIME_DECAY_CONVERGENCE_EXIT_BID
///      ($0.998) before expiry, sell early via FAK to bank the profit sooner.
///      (FAK exit incurs taker fee, but profit is realized immediately.)
///   3. Stop-loss exit: if combined_bid diverges badly (IV spike), exit via FAK.
///   4. Expiry forced exit: sell before MARKET_EXPIRY_SAFETY_BUFFER_SECS to
///      avoid settlement edge cases.
///
/// ── Oracle Volatility Gate ───────────────────────────────────────────────────
///   Blocks entry when oracle signals active repricing or sustained trend:
///   - |velocity_5s| > TIME_DECAY_MAX_FAST_VELOCITY_* (active move in progress)
///   - |oracle_drift_60m| > TIME_DECAY_MAX_SLOW_DRIFT_* (sustained hourly trend)
///
///   For open positions, the stop-loss distance is halved when fast velocity is
///   elevated — exiting before a vol spike diverges the combined bid.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use chrono::{DateTime, Utc};

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams, PositionKey};
use crate::venues::core::MarketId;
use crate::vipers::is_drawdown_limit_hit;
use crate::config;
use crate::venues::core::TimeInForce;

const STRATEGY_NAME: &str = "TimeDecayStrategy";

/// Throttle for the gate-rejection log line: `asset → (last reason, logged at)`.
///
/// Process-global rather than a field on the strategy for the same reason as the
/// maker's trackers — patrol rebuilds every strategy object on each market
/// rotation, which would reset per-instance state every hour and turn a throttled
/// line into an hourly repeat.
fn time_decay_gate_log_state()
    -> &'static std::sync::Mutex<std::collections::HashMap<String, (String, std::time::Instant)>>
{
    static REG: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, (String, std::time::Instant)>>
    > = std::sync::OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// True when this gate rejection should be logged: the blocking reason CHANGED,
/// or the interval elapsed since the last emit for the same reason.
///
/// TimeDecay previously reported gate reasons ONLY to the in-memory
/// `viper_status` registry, which is a live snapshot with no history. That made
/// its idleness unexplainable after the fact: the strategy is the only
/// consistently profitable viper yet has entered 3 times ever, and nothing on
/// disk said which of its five conjunctive gates was doing the blocking. Logging
/// on change keeps the volume near zero on a steady book while still recording
/// every transition — which is what makes a widening experiment measurable.
fn time_decay_gate_log_permitted(asset: &str, reason: &str) -> bool {
    let reg = time_decay_gate_log_state();
    let mut reg = match reg.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    match reg.get(asset) {
        Some((prev, at))
            if prev == reason
                && at.elapsed().as_secs() < config::TIME_DECAY_GATE_LOG_INTERVAL_SECS =>
        {
            false
        }
        _ => {
            reg.insert(asset.to_string(), (reason.to_string(), std::time::Instant::now()));
            true
        }
    }
}

pub struct TimeDecayStrategyImpl;

#[async_trait]
impl Strategy for TimeDecayStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config; // hot-reloadable snapshot for this tick

        // "Why no trades?" registry feed (GET /api/vipers/status) plus a throttled
        // log line so the blocking gate is recoverable from history, not just from
        // a live snapshot — see `time_decay_gate_log_permitted`.
        let idle = |r: &str| {
            crate::helpers::viper_status::report_reason(&ctx.crypto_filter, &self.name(), r);
            if time_decay_gate_log_permitted(&ctx.crypto_filter, r) {
                tracing::info!("🔒 TimeDecay gate: {}", r);
            }
        };
        if !dc.enable_time_decay {
            idle("disabled in config");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global Risk Check ────────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            idle("session drawdown limit hit");
            return Ok(StrategySignal::NoSignal);
        }

        let (market, snap) = (&ctx.market, &ctx.snapshot);

        let seconds_to_expiry = match market.market_close_time {
            Some(close_time) => (close_time - Utc::now()).num_seconds(),
            None => { idle("market has no close time"); return Ok(StrategySignal::NoSignal) },
        };

        // ── Theta window gate (uses dynamic min/max secs) ────────────────────
        if seconds_to_expiry < dc.time_decay_min_secs_to_expiry
            || seconds_to_expiry > dc.time_decay_max_secs_to_expiry
        {
            idle("outside theta window (expiry timing)");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Oracle Volatility Gate ────────────────────────────────────────────
        let (max_fast_vel, max_slow_drift) = TimeDecayStrategy::iv_thresholds(ctx.snapshot.oracle_price, dc.time_decay_max_fast_velocity_pct, dc.time_decay_max_slow_drift_pct);
        if ctx.snapshot.velocity.abs() > max_fast_vel {
            idle("underlying moving too fast");
            return Ok(StrategySignal::NoSignal);
        }
        if ctx.snapshot.oracle_drift_60m.abs() > max_slow_drift {
            idle("60m drift too large");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Snapshot staleness gate ───────────────────────────────────────────
        // The snapshot is updated via WebSocket events.  Between events the snapshot
        // retains stale depth values — a book that appears neutral can actually be
        // adverse when the WebSocket hasn't fired recently.
        // 2026-05-07 T3: entered with entry_hb_age_sec=34, stale OBI slipped the gate.
        let snapshot_age_secs = (Utc::now() - snap.timestamp).num_seconds();
        if snapshot_age_secs > config::TIME_DECAY_MAX_SNAPSHOT_AGE_SECS {
            tracing::debug!(
                "🚫 TimeDecay entry blocked: snapshot too stale ({}s > max {}s)",
                snapshot_age_secs, config::TIME_DECAY_MAX_SNAPSHOT_AGE_SECS
            );
            idle("snapshot stale");
            return Ok(StrategySignal::NoSignal);
        }

        // ── OBI gate ─────────────────────────────────────────────────────────
        let yes_bid = snap.yes_bid;
        let no_bid  = snap.no_bid;
        // Source selected by `obi_use_whole_book`; -1 when there is no depth at
        // all, which blocks entry. "Ghost OBI" trades (zero depth at evaluation
        // but adverse heartbeat OBI) caused losses in the 2026-05-07 afternoon
        // session, where the tick snapshot had missing depth while the heartbeat
        // showed -0.76 to -0.96.
        let whole_book = dc.obi_use_whole_book;
        let yes_obi = snap.yes_obi(whole_book);
        let no_obi  = snap.no_obi(whole_book);
        // Use the stricter of the dynamic config value or the compile-time constant.
        // This prevents a stale DB value (written before the constant was tightened)
        // from silently bypassing the gate.  The config constant is the hard floor.
        let obi_block = dc.time_decay_obi_adverse_block.max(config::TIME_DECAY_OBI_ADVERSE_BLOCK);
        if yes_obi < obi_block || no_obi < obi_block {
            idle("adverse book imbalance (OBI)");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Price bounds gate ─────────────────────────────────────────────────
        // Use the stricter of the dynamic config value or the compile-time constant.
        // This prevents a stale DB value (e.g. 0.65 left from an earlier session)
        // from letting skewed entries (yes_bid=0.59, 0.63) through.
        // 2026-05-08 session: DB had max_entry_price=0.65; compile-time is 0.50.
        // TimeDecay only makes sense in the symmetric zone where BOTH legs are near 0.50.
        let max_entry = dc.time_decay_max_entry_price.min(config::TIME_DECAY_MAX_ENTRY_PRICE);
        if yes_bid > max_entry || yes_bid < dc.time_decay_min_entry_price {
            idle("market skewed (YES leg outside symmetric band)");
            return Ok(StrategySignal::NoSignal);
        }
        if no_bid > max_entry || no_bid < dc.time_decay_min_entry_price {
            idle("market skewed (NO leg outside symmetric band)");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Pre-entry convergence check ───────────────────────────────────────
        if yes_bid + no_bid >= dc.time_decay_convergence_exit_bid {
            idle("already converged (no theta left)");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Theta opportunity check (inline with dynamic thresholds) ──────────
        let combined_bid = yes_bid + no_bid;
        let net = dec!(1.0) - combined_bid;
        if net >= dc.min_time_decay_net_profit {
            let trade_size = dc.time_decay_position_size_usdc;

            // ── Strategy Exposure Check ──────────────────────────────────────
            let current_exposure = {
                let pos_map = ctx.positions.lock().await;
                pos_map.iter()
                    .filter(|(k, _)| (k.strategy == STRATEGY_NAME) && k.squadron == ctx.squadron_id)
                    .filter(|(_, p)| p.counts_toward_exposure(chrono::Utc::now()))
                    .map(|(_, p)| p.shares * p.avg_entry)
                    .sum::<Decimal>()
            };
            if current_exposure + trade_size > dc.time_decay_max_exposure_usdc {
                idle("exposure cap reached");
                return Ok(StrategySignal::NoSignal);
            }

            let pair_shares = trade_size / combined_bid;

            // Viper Backtrace: persist the gate/decision state for this entry
            // (keyed by the YES leg, the primary leg that record_entry_signal records).
            crate::helpers::metrics::stash_entry_signals_json(market.yes_token.as_str(), serde_json::json!({
                "viper": "TimeDecay",
                "yes_bid": yes_bid.to_string(),
                "no_bid": no_bid.to_string(),
                "combined_bid": combined_bid.to_string(),
                "net_theta": net.to_string(),
                "pair_shares": pair_shares.to_string(),
                "trade_size": trade_size.to_string(),
            }));

            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id:    market.yes_token.clone(),
                    price:       yes_bid,
                    shares:      pair_shares,
                    fee_bps:     0,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                    order_type: TimeInForce::Gtc,
                    post_only:  true,
                    ghost_mode: dc.ghost_mode,
                },
                pair_params: Some(OrderParams {
                    token_id:    market.no_token.clone(),
                    price:       no_bid,
                    shares:      pair_shares,
                    fee_bps:     0,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                    order_type: TimeInForce::Gtc,
                    post_only:  true,
                    ghost_mode: dc.ghost_mode,
                }),
            });
        }
        idle("theta below minimum net profit");
        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        let pos_map = ctx.positions.lock().await;

        let (market, snap) = (&ctx.market, &ctx.snapshot);

        let yes_key = PositionKey::new(&ctx.squadron_id, "TimeDecayStrategy", market.yes_token.clone());
        let no_key  = PositionKey::new(&ctx.squadron_id, "TimeDecayStrategy", market.no_token.clone());

        if let (Some(yp), Some(np)) = (pos_map.get(&yes_key), pos_map.get(&no_key)) {
            let yes_bid = snap.yes_bid;
            let no_bid  = snap.no_bid;

            // ── Convergence exit ──────────────────────────────────────────────
            if yes_bid + no_bid >= dc.time_decay_convergence_exit_bid {
                return Ok(StrategySignal::Exit {
                    params: OrderParams { token_id: market.yes_token.clone(), price: yes_bid, shares: yp.shares, fee_bps: market.yes_fee_bps as u16, is_neg_risk: market.is_neg_risk, market_name: market.market_name.clone(), condition_id: market.condition_id.clone(), order_type: TimeInForce::Fak, post_only: false, ghost_mode: dc.ghost_mode },
                    reason: "Time Decay convergence".to_string(),
                    exit_pair: true,
                });
            }

            // ── Dynamic stop: tighten when vol is elevated ────────────────────
            let (max_fast_vel, _) = TimeDecayStrategy::iv_thresholds(ctx.snapshot.oracle_price, dc.time_decay_max_fast_velocity_pct, dc.time_decay_max_slow_drift_pct);
            let iv_elevated = snap.velocity.abs() > max_fast_vel;
            let effective_stop_pct = if iv_elevated {
                let tight = dc.time_decay_stop_loss_pct * dc.time_decay_iv_stop_tighten_multiplier;
                tracing::debug!("⚡ TimeDecay IV elevated (|vel|={:.2}): stop tightened to {:.1}%", snap.velocity, tight * dec!(100));
                tight
            } else {
                dc.time_decay_stop_loss_pct
            };

            // ── Min-hold guard ────────────────────────────────────────────────
            let hold_secs = (Utc::now() - yp.opened_at).num_seconds();
            if hold_secs < dc.time_decay_min_hold_secs {
                tracing::debug!("⏳ TimeDecay SL suppressed: hold={}s < min={}s", hold_secs, dc.time_decay_min_hold_secs);
            } else {
                let combined_bid = yes_bid + no_bid;
                // ── Entry-relative stop-loss ──────────────────────────────────────
                // Previous formula used convergence_exit_bid (0.998) as the SL reference,
                // which caused the threshold (0.998 × 0.95 = 0.9481) to be ABOVE typical
                // entry combined bids (0.73–0.97) — either firing immediately or allowing
                // huge losses.  Now anchored to actual entry cost so 5% means 5% of
                // what we paid, regardless of how skewed the entry was.
                let entry_combined = yp.avg_entry + np.avg_entry;
                let sl_threshold = entry_combined * (dec!(1) - effective_stop_pct);
                if combined_bid < sl_threshold {
                    return Ok(StrategySignal::Exit {
                        params: OrderParams { token_id: market.yes_token.clone(), price: yes_bid, shares: yp.shares, fee_bps: market.yes_fee_bps as u16, is_neg_risk: market.is_neg_risk, market_name: market.market_name.clone(), condition_id: market.condition_id.clone(), order_type: TimeInForce::Fak, post_only: false, ghost_mode: dc.ghost_mode },
                        reason: format!("Time Decay SL{}", if iv_elevated { " (IV-tightened)" } else { "" }),
                        exit_pair: true,
                    });
                }
            }

            // ── Forced expiry exit ────────────────────────────────────────────
            if let Some(close_time) = market.market_close_time {
                if (close_time - Utc::now()).num_seconds() < config::MARKET_EXPIRY_SAFETY_BUFFER_SECS as i64 {
                    return Ok(StrategySignal::Exit {
                        params: OrderParams { token_id: market.yes_token.clone(), price: yes_bid, shares: yp.shares, fee_bps: market.yes_fee_bps as u16, is_neg_risk: market.is_neg_risk, market_name: market.market_name.clone(), condition_id: market.condition_id.clone(), order_type: TimeInForce::Fak, post_only: false, ghost_mode: dc.ghost_mode },
                        reason: "Time Decay Expiry".to_string(),
                        exit_pair: true,
                    });
                }
            }
        }
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "TimeDecayStrategy".to_string() }
    fn venue(&self) -> &'static str { "Hourly" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::TIME_DECAY_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Gross hedged (per leg)" }
}

pub struct TimeDecayStrategy;

impl TimeDecayStrategy {
    /// Return (max_fast_velocity, max_slow_drift) scaled to the current oracle price.
    pub fn iv_thresholds(oracle_price: Decimal, max_fast_velocity_pct: Decimal, max_slow_drift_pct: Decimal) -> (Decimal, Decimal) {
        (
            config::oracle_threshold(max_fast_velocity_pct, oracle_price),
            config::oracle_threshold(max_slow_drift_pct, oracle_price),
        )
    }

    /// Check whether the combined bid gap is wide enough to cover the
    /// MIN_TIME_DECAY_NET_PROFIT threshold.
    ///
    /// Now takes **bid prices** (not ask prices) and assumes **0% maker fee**:
    ///   net = 1.00 − yes_bid − no_bid
    ///
    /// The old signature took ask prices and deducted up to 10% taker fees,
    /// making it structurally impossible to fire.  Maker entry eliminates that.
    pub fn calculate_theta_opportunity(yes_bid: Decimal, no_bid: Decimal, secs: i64) -> Option<ThetaSignal> {
        if !TimeDecayStrategy::is_in_theta_window(secs) { return None; }
        let combined_bid = yes_bid + no_bid;
        let net = dec!(1.0) - combined_bid;    // 0% entry fee + 0% settlement exit
        if net >= config::MIN_TIME_DECAY_NET_PROFIT {
            return Some(ThetaSignal {
                mode: ThetaMode::Settlement,
                combined_ask: combined_bid,    // field reused for combined_bid in maker mode
                net_profit_per_share: net,
                total_fees: dec!(0),
            });
        }
        None
    }

    pub fn is_in_theta_window(secs: i64) -> bool {
        secs >= config::TIME_DECAY_MIN_SECS_TO_EXPIRY && secs <= config::TIME_DECAY_MAX_SECS_TO_EXPIRY
    }
    pub fn should_convergence_exit(yb: Decimal, nb: Decimal) -> bool {
        yb + nb >= config::TIME_DECAY_CONVERGENCE_EXIT_BID
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ThetaMode { Settlement, Convergence }

pub struct ThetaSignal { pub mode: ThetaMode, pub combined_ask: Decimal, pub net_profit_per_share: Decimal, pub total_fees: Decimal }

pub struct TimeDecayPosition { pub yes_token_id: MarketId, pub no_token_id: MarketId, pub entry_time: DateTime<Utc>, pub expiry_time: DateTime<Utc>, pub yes_entry_price: Decimal, pub no_entry_price: Decimal, pub position_size: Decimal, pub total_invested: Decimal, pub mode: ThetaMode }

impl TimeDecayPosition {
    pub fn time_to_expiry(&self) -> i64 { (self.expiry_time - Utc::now()).num_seconds() }
    pub fn is_expired(&self) -> bool { self.time_to_expiry() <= 0 }
}

#[cfg(test)]
mod gate_log_throttle_tests {
    use super::time_decay_gate_log_permitted;

    #[test]
    fn first_rejection_for_an_asset_logs() {
        assert!(time_decay_gate_log_permitted("tdtest_first", "outside theta window"));
    }

    #[test]
    fn same_reason_repeats_are_suppressed() {
        let a = "tdtest_repeat";
        assert!(time_decay_gate_log_permitted(a, "snapshot stale"));
        assert!(!time_decay_gate_log_permitted(a, "snapshot stale"));
        assert!(!time_decay_gate_log_permitted(a, "snapshot stale"));
    }

    #[test]
    fn a_changed_reason_always_logs_immediately() {
        // The transition is the whole signal — it is what says which gate took
        // over as the binding constraint, so it must never be throttled away.
        let a = "tdtest_change";
        assert!(time_decay_gate_log_permitted(a, "outside theta window"));
        assert!(!time_decay_gate_log_permitted(a, "outside theta window"));
        assert!(time_decay_gate_log_permitted(a, "underlying moving too fast"));
        assert!(!time_decay_gate_log_permitted(a, "underlying moving too fast"));
        assert!(time_decay_gate_log_permitted(a, "outside theta window"));
    }

    #[test]
    fn assets_throttle_independently() {
        assert!(time_decay_gate_log_permitted("tdtest_btc", "exposure cap reached"));
        assert!(time_decay_gate_log_permitted("tdtest_eth", "exposure cap reached"));
    }
}
