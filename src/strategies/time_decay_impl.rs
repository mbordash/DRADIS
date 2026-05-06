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
use alloy::primitives::U256;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::strategies::is_drawdown_limit_hit;
use crate::config;
use polymarket_client_sdk_v2::clob::types::OrderType;

const STRATEGY_NAME: &str = "TimeDecayStrategy";

pub struct TimeDecayStrategyImpl;

#[async_trait]
impl Strategy for TimeDecayStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        if !config::ENABLE_TIME_DECAY_TRADING {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global Risk Check ────────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            return Ok(StrategySignal::NoSignal);
        }

        // TimeDecay is Hourly — always use the primary market/snapshot.
        // No maker_market fallback: this strategy is intentionally scoped to
        // the hourly market's final 30-minute theta window.
        let (market, snap) = (&ctx.market, &ctx.snapshot);

        let seconds_to_expiry = match market.market_close_time {
            Some(close_time) => (close_time - Utc::now()).num_seconds(),
            None => return Ok(StrategySignal::NoSignal),
        };

        if !TimeDecayStrategy::is_in_theta_window(seconds_to_expiry) {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Oracle Volatility Gate ────────────────────────────────────────────
        let (max_fast_vel, max_slow_drift) = TimeDecayStrategy::iv_thresholds(&ctx.crypto_filter);
        if ctx.snapshot.velocity.abs() > max_fast_vel {
            return Ok(StrategySignal::NoSignal);
        }
        if ctx.snapshot.oracle_drift_60m.abs() > max_slow_drift {
            return Ok(StrategySignal::NoSignal);
        }

        // ── OBI gate: don't enter when the book has already decided a winner ──
        // An OBI of -0.89 on YES means almost no YES liquidity — the crowd has
        // moved to one side. Both legs must pass; we hold both at settlement.
        let yes_bid = snap.yes_bid;
        let no_bid  = snap.no_bid;
        let yes_total_depth = snap.yes_bid_depth + snap.yes_ask_depth;
        let yes_obi = if yes_total_depth > dec!(0) {
            (snap.yes_bid_depth - snap.yes_ask_depth) / yes_total_depth
        } else {
            dec!(0)
        };
        let no_total_depth = snap.no_bid_depth + snap.no_ask_depth;
        let no_obi = if no_total_depth > dec!(0) {
            (snap.no_bid_depth - snap.no_ask_depth) / no_total_depth
        } else {
            dec!(0)
        };
        if yes_obi < config::TIME_DECAY_OBI_ADVERSE_BLOCK || no_obi < config::TIME_DECAY_OBI_ADVERSE_BLOCK {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Price bounds gate: only enter genuine uncertainty zone ──────────
        // Extreme YES prices (e.g. 0.15, 0.92) mean the market has decided.
        // Share counts balloon at extreme prices — a 1¢ move = $1 P&L swing.
        if yes_bid > config::TIME_DECAY_MAX_ENTRY_PRICE || yes_bid < config::TIME_DECAY_MIN_ENTRY_PRICE {
            return Ok(StrategySignal::NoSignal);
        }
        if no_bid > config::TIME_DECAY_MAX_ENTRY_PRICE || no_bid < config::TIME_DECAY_MIN_ENTRY_PRICE {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Pre-entry convergence check: don't enter at the exit threshold ────
        // If bid_sum is already ≥ convergence exit level the exit fires instantly.
        if yes_bid + no_bid >= config::TIME_DECAY_CONVERGENCE_EXIT_BID {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Theta opportunity check (maker: uses bid prices, 0% fee) ─────────
        if TimeDecayStrategy::calculate_theta_opportunity(
            yes_bid, no_bid, seconds_to_expiry,
        ).is_some() {
            let trade_size = config::TIME_DECAY_POSITION_SIZE_USDC;

            // ── Strategy Exposure Check ──────────────────────────────────────
            let current_exposure = {
                let pos_map = ctx.positions.lock().await;
                pos_map.iter()
                    .filter(|((s, _), _)| s == STRATEGY_NAME)
                    .map(|(_, p)| p.shares * p.avg_entry)
                    .sum::<Decimal>()
            };
            if current_exposure + trade_size > config::TIME_DECAY_MAX_EXPOSURE_USDC {
                return Ok(StrategySignal::NoSignal);
            }

            // ── Equal-shares sizing (true hedge) ─────────────────────────────
            // Buying equal DOLLAR amounts gives unequal shares (e.g. 41.7 YES vs
            // 23.8 NO at 0.36/0.63), which is NOT a true hedge: if YES settles you
            // receive 41.7×$1.00 = $41.7 but paid $30 total, while if NO settles
            // you receive only 23.8×$1.00 = $23.8 — a -$6.2 loss on a $30 investment.
            //
            // Equal SHARES ensure settlement payout = pair_shares×$1.00 regardless
            // of which side wins:
            //   pair_shares = trade_size / (yes_bid + no_bid)
            //   total_cost  = pair_shares × (yes_bid + no_bid) = trade_size
            //   profit      = trade_size × (1.00 / (yes_bid + no_bid) − 1)
            //               = trade_size × net_per_dollar
            //
            // Example: yes_bid=0.36, no_bid=0.63, trade_size=$15
            //   pair_shares = 15 / 0.99 = 15.15   (same count for both legs)
            //   total_cost  = 15.15 × 0.99 = $15
            //   payout      = 15.15 × $1.00 = $15.15  (guaranteed either side)
            //   profit      = $0.15 (1% on $15)
            let pair_shares = trade_size / (yes_bid + no_bid);

            // Post GTC maker bids at current best bid — 0% fill fee.
            // Both legs must fill for the arb to be complete; the flash-exit
            // mechanism handles the one-leg-fills scenario if Leg B is rejected.
            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id:    market.yes_token,
                    price:       yes_bid,               // bid price → rests on book as maker
                    shares:      pair_shares,           // equal shares on both legs — true hedge
                    fee_bps:     0,                     // GTC maker fill = 0% fee
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                    order_type: OrderType::GTC,         // rest on book until filled or cancelled
                    post_only:  true,                   // reject if would cross (no accidental taker)
                    ghost_mode: config::GHOST_MODE,
                },
                pair_params: Some(OrderParams {
                    token_id:    market.no_token,
                    price:       no_bid,
                    shares:      pair_shares,           // same count as YES leg
                    fee_bps:     0,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                    order_type: OrderType::GTC,
                    post_only:  true,
                    ghost_mode: config::GHOST_MODE,
                }),
            });
        }
        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let pos_map = ctx.positions.lock().await;

        // TimeDecay is Hourly — always use the primary market/snapshot.
        let (market, snap) = (&ctx.market, &ctx.snapshot);

        let yes_key = ("TimeDecayStrategy".to_string(), market.yes_token);
        let no_key  = ("TimeDecayStrategy".to_string(), market.no_token);

        if let (Some(yp), Some(_)) = (pos_map.get(&yes_key), pos_map.get(&no_key)) {
            let yes_bid = snap.yes_bid;
            let no_bid  = snap.no_bid;

            // ── Convergence exit: combined bid approached $1.00 early ─────────
            // Take profit via FAK taker before settlement — fee applies here,
            // but the combined_bid already exceeds our maker entry cost by enough
            // to absorb it (entry ≈ $0.97, exit triggered at $0.998).
            if TimeDecayStrategy::should_convergence_exit(yes_bid, no_bid) {
                return Ok(StrategySignal::Exit {
                    params: OrderParams { token_id: market.yes_token, price: yes_bid, shares: yp.shares, fee_bps: market.yes_fee_bps as u16, is_neg_risk: market.is_neg_risk, market_name: market.market_name.clone(), condition_id: market.condition_id.clone(), order_type: OrderType::FAK, post_only: false, ghost_mode: config::GHOST_MODE },
                    reason: "Time Decay convergence".to_string(),
                    exit_pair: true,
                });
            }

            // ── Dynamic stop: tighten when vol is elevated ────────────────────
            let (max_fast_vel, _) = TimeDecayStrategy::iv_thresholds(&ctx.crypto_filter);
            let iv_elevated = snap.velocity.abs() > max_fast_vel;
            let effective_stop_pct = if iv_elevated {
                let tight = config::TIME_DECAY_STOP_LOSS_PERCENT * config::TIME_DECAY_IV_STOP_TIGHTEN_MULTIPLIER;
                tracing::debug!("⚡ TimeDecay IV elevated (|vel|={:.2}): stop tightened to {:.1}%", snap.velocity, tight * dec!(100));
                tight
            } else {
                config::TIME_DECAY_STOP_LOSS_PERCENT
            };

            // ── Min-hold guard: don't allow SL on noise immediately after entry ─
            let hold_secs = (Utc::now() - yp.opened_at).num_seconds();
            if hold_secs < config::TIME_DECAY_MIN_HOLD_SECS {
                tracing::debug!("⏳ TimeDecay SL suppressed: hold={}s < min={}s", hold_secs, config::TIME_DECAY_MIN_HOLD_SECS);
            } else {
                let combined_bid = yes_bid + no_bid;
                if combined_bid < config::TIME_DECAY_CONVERGENCE_EXIT_BID * (dec!(1) - effective_stop_pct) {
                    return Ok(StrategySignal::Exit {
                        params: OrderParams { token_id: market.yes_token, price: yes_bid, shares: yp.shares, fee_bps: market.yes_fee_bps as u16, is_neg_risk: market.is_neg_risk, market_name: market.market_name.clone(), condition_id: market.condition_id.clone(), order_type: OrderType::FAK, post_only: false, ghost_mode: config::GHOST_MODE },
                        reason: format!("Time Decay SL{}", if iv_elevated { " (IV-tightened)" } else { "" }),
                        exit_pair: true,
                    });
                }
            }

            // ── Forced expiry exit: sell before market closes ─────────────────
            // Preferred path is settlement ($0 fee), but if still holding at
            // MARKET_EXPIRY_SAFETY_BUFFER_SECS we exit via FAK as a safety net.
            if let Some(close_time) = market.market_close_time {
                if (close_time - Utc::now()).num_seconds() < config::MARKET_EXPIRY_SAFETY_BUFFER_SECS as i64 {
                    return Ok(StrategySignal::Exit {
                        params: OrderParams { token_id: market.yes_token, price: yes_bid, shares: yp.shares, fee_bps: market.yes_fee_bps as u16, is_neg_risk: market.is_neg_risk, market_name: market.market_name.clone(), condition_id: market.condition_id.clone(), order_type: OrderType::FAK, post_only: false, ghost_mode: config::GHOST_MODE },
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
    pub fn iv_thresholds(crypto_filter: &str) -> (Decimal, Decimal) {
        match crypto_filter {
            "eth" => (config::TIME_DECAY_MAX_FAST_VELOCITY_ETH, config::TIME_DECAY_MAX_SLOW_DRIFT_ETH),
            "sol" => (config::TIME_DECAY_MAX_FAST_VELOCITY_SOL, config::TIME_DECAY_MAX_SLOW_DRIFT_SOL),
            _     => (config::TIME_DECAY_MAX_FAST_VELOCITY_BTC, config::TIME_DECAY_MAX_SLOW_DRIFT_BTC),
        }
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

pub struct TimeDecayPosition { pub yes_token_id: U256, pub no_token_id: U256, pub entry_time: DateTime<Utc>, pub expiry_time: DateTime<Utc>, pub yes_entry_price: Decimal, pub no_entry_price: Decimal, pub position_size: Decimal, pub total_invested: Decimal, pub mode: ThetaMode }

impl TimeDecayPosition {
    pub fn time_to_expiry(&self) -> i64 { (self.expiry_time - Utc::now()).num_seconds() }
    pub fn is_expired(&self) -> bool { self.time_to_expiry() <= 0 }
}
