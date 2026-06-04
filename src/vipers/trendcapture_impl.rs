/// TrendCapture Strategy — Sustained Oracle Drift on Window/Daily Markets
///
/// # Thesis
///
/// Polymarket binary markets on window (4h) and daily horizons frequently **lag**
/// the Binance oracle when BTC makes a sustained multi-minute directional move.
/// While `MomentumStrategy` handles 5-second velocity spikes on hourly markets,
/// TrendCapture exploits drift events where:
///
///   - BTC has moved meaningfully over both the 10-minute AND 60-minute windows
///   - The corresponding YES/NO token is still in the tradable price range
///     (market hasn't fully priced in the move yet)
///   - BTC spot is already meaningfully away from the strike price
///
/// Example — today's BTC crash:
///   drift_10m = −$150 (BTC fell $150 in 10 min)
///   drift_60m = −$400 (BTC fell $400 in 60 min, confirmed downtrend)
///   binance_price = $66,900  vs  daily_strike = $68,000  → $1,100 below strike
///   NO on daily market still priced at $0.58  ← entry window
///   → TrendCapture buys NO at $0.58, targets $0.78 (+20 TP)
///
/// # Venue
///   Window or Daily market (uses `maker_market` / `maker_snapshot` when available;
///   falls back to the hourly market if no window/daily is configured).
///   A longer expiry gives the trade more time to develop before forced resolution.
///
/// # Key differences from MomentumStrategy
///   - Primary entry signal: `oracle_drift_10m` + `oracle_drift_60m` (multi-minute trend)
///     NOT short-term `velocity` / `velocity_1s` (spike-based)
///   - Position expected to be held minutes to hours, not seconds to minutes
///   - Higher TP target (20%), moderate SL (8%), longer min-hold before exits
///   - Trend-reversal exit fires when drift_10m crosses meaningfully counter-direction
///
/// # Cooldown & re-entry
///   Per-token post-exit cooldown prevents rapid re-entry after a loss.
///   Uses a `HashMap<U256, Instant>` protected by `std::sync::Mutex` (no async holds).

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use alloy::primitives::U256;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
use chrono::Utc;
use tracing::debug;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::vipers::is_drawdown_limit_hit;
use crate::config;
use polymarket_client_sdk_v2::clob::types::OrderType;

// ─── Stateful implementation ─────────────────────────────────────────────────

pub struct TrendCaptureStrategyImpl {
    /// Per-token cooldown after any exit.
    /// Key: token_id, Value: Instant of last exit.
    post_exit_cooldown: Mutex<HashMap<U256, Instant>>,
}

impl TrendCaptureStrategyImpl {
    pub fn new() -> Self {
        Self {
            post_exit_cooldown: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for TrendCaptureStrategyImpl {
    fn default() -> Self { Self::new() }
}

// ─── Entry evaluation ─────────────────────────────────────────────────────────

#[async_trait]
impl Strategy for TrendCaptureStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        if !dc.enable_trendcapture {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global drawdown guard ────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Venue selection: prefer Window/Daily ─────────────────────────────
        // TrendCapture needs a longer expiry for multi-minute drift trades.
        let (market, snap) = if let (Some(mk), Some(ms)) = (&ctx.maker_market, &ctx.maker_snapshot) {
            (mk, ms)
        } else {
            (&ctx.market, &ctx.snapshot)
        };

        // ── Snapshot staleness gate ───────────────────────────────────────────
        let snap_age = (Utc::now() - snap.timestamp).num_seconds();
        if snap_age > config::TRENDCAPTURE_MAX_SNAPSHOT_AGE_SECS {
            debug!("🦅 TrendCapture blocked: snapshot stale ({}s > {}s)",
                snap_age, config::TRENDCAPTURE_MAX_SNAPSHOT_AGE_SECS);
            return Ok(StrategySignal::NoSignal);
        }

        // ── Oracle staleness guard ────────────────────────────────────────────
        // If the 10m drift is still zero the oracle hasn't had 10 minutes of history
        // yet — we have no trend signal to trade on.
        let drift_10m = ctx.snapshot.oracle_drift_10m;
        let drift_60m = ctx.snapshot.oracle_drift_60m;
        if drift_10m == dec!(0) {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Expiry guard ──────────────────────────────────────────────────────
        if let Some(close_time) = market.market_close_time {
            let secs_left = (close_time - Utc::now()).num_seconds();
            if secs_left < config::TRENDCAPTURE_MIN_SECS_TO_EXPIRY {
                debug!("🦅 TrendCapture blocked: only {}s to expiry (min {}s)",
                    secs_left, config::TRENDCAPTURE_MIN_SECS_TO_EXPIRY);
                return Ok(StrategySignal::NoSignal);
            }
        }

        // ── Market warmup gate ────────────────────────────────────────────────
        let secs_since_market_start = (Utc::now() - ctx.market_started_at).num_seconds();
        if secs_since_market_start < config::TRENDCAPTURE_MARKET_WARMUP_SECS {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Spread gate ───────────────────────────────────────────────────────
        let ask_sum = snap.yes_ask + snap.no_ask;
        if ask_sum > config::TRENDCAPTURE_MAX_ENTRY_ASK_SUM {
            debug!("🦅 TrendCapture spread gate: ask_sum={:.3} > max {:.3} — book too wide",
                ask_sum, config::TRENDCAPTURE_MAX_ENTRY_ASK_SUM);
            return Ok(StrategySignal::NoSignal);
        }

        // ── Minimum price floor ───────────────────────────────────────────────
        let yes_ask = snap.yes_ask;
        let no_ask  = snap.no_ask;
        if yes_ask < config::TRENDCAPTURE_MIN_ENTRY_PRICE && no_ask < config::TRENDCAPTURE_MIN_ENTRY_PRICE {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Exposure guard ────────────────────────────────────────────────────
        let current_exposure = {
            let pos_map = ctx.positions.lock().await;
            pos_map.iter()
                .filter(|((s, _), _)| s == "TrendCaptureStrategy")
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum::<Decimal>()
        };
        if current_exposure >= dc.trendcapture_max_exposure_usdc {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Determine thresholds per asset ─────────────────────────────────
        let (bull_drift_10m_thr, bear_drift_10m_thr,
             bull_drift_60m_thr, bear_drift_60m_thr,
             bull_strike_gap,    bear_strike_gap,
             bull_reversal_thr,  bear_reversal_thr) =
        match ctx.crypto_filter.as_str() {
            "eth" => (
                config::TRENDCAPTURE_BULL_DRIFT_10M_ETH, config::TRENDCAPTURE_BEAR_DRIFT_10M_ETH,
                config::TRENDCAPTURE_BULL_DRIFT_60M_ETH, config::TRENDCAPTURE_BEAR_DRIFT_60M_ETH,
                config::TRENDCAPTURE_BULL_STRIKE_GAP_ETH, config::TRENDCAPTURE_BEAR_STRIKE_GAP_ETH,
                config::TRENDCAPTURE_REVERSAL_DRIFT_ETH,  config::TRENDCAPTURE_REVERSAL_DRIFT_ETH,
            ),
            "sol" => (
                config::TRENDCAPTURE_BULL_DRIFT_10M_SOL, config::TRENDCAPTURE_BEAR_DRIFT_10M_SOL,
                config::TRENDCAPTURE_BULL_DRIFT_60M_SOL, config::TRENDCAPTURE_BEAR_DRIFT_60M_SOL,
                config::TRENDCAPTURE_BULL_STRIKE_GAP_SOL, config::TRENDCAPTURE_BEAR_STRIKE_GAP_SOL,
                config::TRENDCAPTURE_REVERSAL_DRIFT_SOL,  config::TRENDCAPTURE_REVERSAL_DRIFT_SOL,
            ),
            _ => (
                config::TRENDCAPTURE_BULL_DRIFT_10M_BTC, config::TRENDCAPTURE_BEAR_DRIFT_10M_BTC,
                config::TRENDCAPTURE_BULL_DRIFT_60M_BTC, config::TRENDCAPTURE_BEAR_DRIFT_60M_BTC,
                config::TRENDCAPTURE_BULL_STRIKE_GAP_BTC, config::TRENDCAPTURE_BEAR_STRIKE_GAP_BTC,
                config::TRENDCAPTURE_REVERSAL_DRIFT_BTC,  config::TRENDCAPTURE_REVERSAL_DRIFT_BTC,
            ),
        };
        let _ = (bull_reversal_thr, bear_reversal_thr); // used only in exit

        // ── 60m drift alignment gate ──────────────────────────────────────────
        // REQUIREMENT: 60m drift MUST confirm the 10m direction.
        // We require at least 10 minutes of oracle history (drift_60m != 0) before entry.
        // A 10m spike that contradicts the 60m trend is a counter-trend bounce — skip.
        // Changed from optional to mandatory: no trades until 10+ minutes of history exists.
        let drift_60m_blocks_bull = drift_60m == dec!(0) || drift_60m < bull_drift_60m_thr;
        let drift_60m_blocks_bear = drift_60m == dec!(0) || drift_60m > bear_drift_60m_thr;

        // ── OBI adverse-direction veto ────────────────────────────────────────
        let yes_total_depth = snap.yes_bid_depth + snap.yes_ask_depth;
        let no_total_depth  = snap.no_bid_depth  + snap.no_ask_depth;
        let yes_obi = if yes_total_depth > dec!(0) {
            (snap.yes_bid_depth - snap.yes_ask_depth) / yes_total_depth
        } else { dec!(-1.0) };
        let no_obi = if no_total_depth > dec!(0) {
            (snap.no_bid_depth - snap.no_ask_depth) / no_total_depth
        } else { dec!(-1.0) };

        let obi_blocks_bull = yes_obi < config::TRENDCAPTURE_OBI_ADVERSE_BLOCK;
        let obi_blocks_bear = no_obi  < config::TRENDCAPTURE_OBI_ADVERSE_BLOCK;
        let obi_exhausted_bull = yes_obi > config::TRENDCAPTURE_OBI_EXHAUSTION_BLOCK;
        let obi_exhausted_bear = no_obi  > config::TRENDCAPTURE_OBI_EXHAUSTION_BLOCK;

        // ── Strike price distance requirement ─────────────────────────────────
        let binance_price = ctx.snapshot.oracle_price;
        let strike_price  = market.strike_price;

        // ── Per-token post-exit cooldown ──────────────────────────────────────
        // Checked inside each entry path below.
        let cooldowns = self.post_exit_cooldown.lock().unwrap();

        // ── Kelly-fractional position sizing ──────────────────────────────────
        let trade_size = |drift_abs: Decimal| -> Decimal {
            let thr = bull_drift_10m_thr.abs().max(Decimal::ONE);
            let strength = (drift_abs / thr)
                .max(Decimal::ONE)
                .min(config::TRENDCAPTURE_KELLY_MAX_MULTIPLIER);
            let fraction = (strength - Decimal::ONE)
                / (config::TRENDCAPTURE_KELLY_MAX_MULTIPLIER - Decimal::ONE);
            dc.trendcapture_min_trade_size_usdc
                + fraction * (dc.trendcapture_max_trade_size_usdc - dc.trendcapture_min_trade_size_usdc)
        };

        // ── Macro: entry OrderParams ───────────────────────────────────────────
        macro_rules! entry_params {
            ($token:expr, $price:expr, $fee:expr, $size:expr) => {
                OrderParams {
                    token_id:     $token,
                    price:        $price,
                    shares:       $size / $price,
                    fee_bps:      $fee,
                    is_neg_risk:  market.is_neg_risk,
                    market_name:  market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                    order_type:   OrderType::FAK,
                    post_only:    false,
                    ghost_mode:   dc.ghost_mode,
                }
            };
        }

        // ══ BULL entry: buy YES when trend is strongly upward ════════════════
        if drift_10m >= bull_drift_10m_thr
            && !drift_60m_blocks_bull
            && !obi_blocks_bull
            && !obi_exhausted_bull
        {
            // Strike gap check
            let passes_gap = match strike_price {
                Some(strike) => binance_price >= strike + bull_strike_gap,
                None => true, // no strike data → rely on drift signal alone
            };

            if passes_gap
                && yes_ask >= config::TRENDCAPTURE_MIN_ENTRY_PRICE
                && yes_ask <= dc.trendcapture_max_entry_price
            {
                // Cooldown check
                let token_id = market.yes_token;
                let in_cooldown = cooldowns.get(&token_id)
                    .map(|t| t.elapsed().as_secs() < config::TRENDCAPTURE_POST_EXIT_COOLDOWN_SECS as u64)
                    .unwrap_or(false);
                if !in_cooldown {
                    let size = trade_size(drift_10m.abs());
                    debug!("🦅 TrendCapture BULL entry: drift_10m={:.0} drift_60m={:.0} yes_ask={:.3} size={:.2}",
                        drift_10m, drift_60m, yes_ask, size);
                    drop(cooldowns);
                    return Ok(StrategySignal::Entry {
                        params: entry_params!(token_id, yes_ask, market.yes_fee_bps as u16, size),
                        pair_params: None,
                    });
                }
            }
        }

        // ══ BEAR entry: buy NO when trend is strongly downward ═══════════════
        if drift_10m <= bear_drift_10m_thr
            && !drift_60m_blocks_bear
            && !obi_blocks_bear
            && !obi_exhausted_bear
        {
            let passes_gap = match strike_price {
                Some(strike) => binance_price <= strike - bear_strike_gap,
                None => true,
            };

            if passes_gap
                && no_ask >= config::TRENDCAPTURE_MIN_ENTRY_PRICE
                && no_ask <= dc.trendcapture_max_entry_price
            {
                let token_id = market.no_token;
                let in_cooldown = cooldowns.get(&token_id)
                    .map(|t| t.elapsed().as_secs() < config::TRENDCAPTURE_POST_EXIT_COOLDOWN_SECS as u64)
                    .unwrap_or(false);
                if !in_cooldown {
                    let size = trade_size(drift_10m.abs());
                    debug!("🦅 TrendCapture BEAR entry: drift_10m={:.0} drift_60m={:.0} no_ask={:.3} size={:.2}",
                        drift_10m, drift_60m, no_ask, size);
                    drop(cooldowns);
                    return Ok(StrategySignal::Entry {
                        params: entry_params!(token_id, no_ask, market.no_fee_bps as u16, size),
                        pair_params: None,
                    });
                }
            }
        }

        drop(cooldowns);
        Ok(StrategySignal::NoSignal)
    }

    // ─── Exit evaluation ──────────────────────────────────────────────────────

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;

        // TrendCapture operates on the maker venue — resolve market/snap for exit checks
        let (market, snap) = if let (Some(mk), Some(ms)) = (&ctx.maker_market, &ctx.maker_snapshot) {
            (mk, ms)
        } else {
            (&ctx.market, &ctx.snapshot)
        };

        let drift_10m = ctx.snapshot.oracle_drift_10m;

        // Per-asset reversal threshold
        let reversal_thr = match ctx.crypto_filter.as_str() {
            "eth" => config::TRENDCAPTURE_REVERSAL_DRIFT_ETH,
            "sol" => config::TRENDCAPTURE_REVERSAL_DRIFT_SOL,
            _     => config::TRENDCAPTURE_REVERSAL_DRIFT_BTC,
        };

        // Collect exit decision inside the lock scope, then act outside it.
        struct PendingExit {
            token_id:     alloy::primitives::U256,
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

            'outer: for ((strategy_name, token_id), position) in pos_map.iter() {
                if strategy_name != "TrendCaptureStrategy" { continue; }

                let bid = if token_id == &market.yes_token { snap.yes_bid }
                          else if token_id == &market.no_token { snap.no_bid }
                          else { continue };

                let avg_entry = position.avg_entry;
                if avg_entry <= dec!(0) { continue; }

                let secs_held = (Utc::now() - position.opened_at).num_seconds();

                // Wait for fill confirmation before any non-catastrophic exit
                if position.fill_confirmed_at.is_none() {
                    let loss_pct = (avg_entry - bid) / avg_entry;
                    if loss_pct < config::TRENDCAPTURE_CATASTROPHIC_SL_PCT {
                        continue;
                    }
                }

                let profit_margin = (bid - avg_entry) / avg_entry;
                let fee_bps = if token_id == &market.yes_token { market.yes_fee_bps as u16 }
                              else { market.no_fee_bps as u16 };

                // Helper closure to build the pending exit
                let make_exit = |reason: String| PendingExit {
                    token_id:     *token_id,
                    bid,
                    shares:       position.shares,
                    fee_bps,
                    is_neg_risk:  market.is_neg_risk,
                    market_name:  market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                    ghost_mode:   dc.ghost_mode,
                    reason,
                };

                // Near-expiry forced exit
                if let Some(close_time) = market.market_close_time {
                    let secs_left = (close_time - Utc::now()).num_seconds();
                    let net_profit = (bid - config::SELL_PRICE_OFFSET - avg_entry) / avg_entry;
                    if secs_left <= config::TRENDCAPTURE_EXPIRY_EXIT_SECS
                        && net_profit < config::TRENDCAPTURE_EXPIRY_MIN_PROFIT_TO_HOLD
                    {
                        found = Some(make_exit(format!("TrendCaptureNearExpiry: bid=${:.4}, net={:.2}%", bid, net_profit * dec!(100))));
                        break 'outer;
                    }
                }

                // Take-profit
                if profit_margin >= dc.trendcapture_target_profit_pct
                    || bid >= config::TRENDCAPTURE_TAKE_PROFIT_CEILING
                {
                    found = Some(make_exit(format!("TrendCaptureTP: bid=${:.4}, profit={:.2}%", bid, profit_margin * dec!(100))));
                    break 'outer;
                }

                // Stop-loss (only after minimum hold)
                if secs_held >= config::TRENDCAPTURE_FILL_CONFIRM_MIN_HOLD_SECS
                    && profit_margin <= -dc.trendcapture_stop_loss_pct
                {
                    found = Some(make_exit(format!("TrendCaptureSL: bid=${:.4}, loss={:.2}%", bid, profit_margin * dec!(100))));
                    break 'outer;
                }

                // Trend-reversal exit
                if secs_held >= config::TRENDCAPTURE_MIN_HOLD_BEFORE_REVERSAL_SECS {
                    let is_yes = token_id == &market.yes_token;
                    let reversal = if is_yes { drift_10m <= -reversal_thr } else { drift_10m >= reversal_thr };
                    if reversal {
                        let net_profit = (bid - config::SELL_PRICE_OFFSET - avg_entry) / avg_entry;
                        if net_profit <= dec!(0.02) {
                            found = Some(make_exit(format!(
                                "TrendCaptureRev: bid=${:.4}, drift_10m={:.0}, profit={:.2}%",
                                bid, drift_10m, profit_margin * dec!(100)
                            )));
                            break 'outer;
                        }
                    }
                }
            }
            found
            // pos_map MutexGuard dropped here
        };

        if let Some(p) = pending {
            self.record_exit(&p.token_id);
            return Ok(StrategySignal::Exit {
                params: OrderParams {
                    token_id:     p.token_id,
                    price:        p.bid,
                    shares:       p.shares,
                    fee_bps:      p.fee_bps,
                    is_neg_risk:  p.is_neg_risk,
                    market_name:  p.market_name,
                    condition_id: p.condition_id,
                    order_type:   OrderType::FAK,
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
    fn name(&self) -> String { "TrendCaptureStrategy".to_string() }
    fn venue(&self) -> &'static str { "Window/Daily" }
    fn max_exposure(&self) -> Decimal { config::TRENDCAPTURE_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "One-sided drift" }
}

impl TrendCaptureStrategyImpl {
    /// Record exit time for post-exit cooldown tracking.
    fn record_exit(&self, token_id: &U256) {
        if let Ok(mut map) = self.post_exit_cooldown.lock() {
            map.insert(*token_id, Instant::now());
        }
    }
}

/// Kelly-fractional sizing helper for TrendCapture.
///
/// Scales linearly from `min_size` (at exactly 1× threshold) to `max_size`
/// (at `TRENDCAPTURE_KELLY_MAX_MULTIPLIER`×), capping above that.
pub fn kelly_trendcapture_size(
    drift_abs: Decimal,
    threshold: Decimal,
    min_size:  Decimal,
    max_size:  Decimal,
) -> Decimal {
    if threshold <= Decimal::ZERO { return min_size; }
    let strength = (drift_abs / threshold)
        .max(Decimal::ONE)
        .min(config::TRENDCAPTURE_KELLY_MAX_MULTIPLIER);
    let fraction = (strength - Decimal::ONE)
        / (config::TRENDCAPTURE_KELLY_MAX_MULTIPLIER - Decimal::ONE);
    min_size + fraction * (max_size - min_size)
}




