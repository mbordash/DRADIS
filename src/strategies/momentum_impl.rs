/// Momentum Strategy
///
/// One-sided, non-hedged trades based on Binance price oracle signals.
/// Entry triggers when price velocity exceeds threshold and market conditions align.
/// Exits via take-profit, stop-loss, or reversal detection.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal_macros::dec;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus};
use crate::config;

/// Implements Strategy trait for Momentum trading
pub struct MomentumStrategyImpl;

#[async_trait]
impl Strategy for MomentumStrategyImpl {
    /// Evaluate if a momentum entry signal should trigger
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        if !config::ENABLE_MOMENTUM_TRADING {
            return Ok(StrategySignal::NoSignal);
        }

        // Extract data from context
        let velocity = ctx.snapshot.velocity;
        let velocity_1s = ctx.snapshot.velocity_1s;
        let acceleration = ctx.snapshot.acceleration;
        let binance_price = ctx.snapshot.oracle_price;
        let strike_price = ctx.market.strike_price;
        let yes_token = ctx.market.yes_token;
        let no_token = ctx.market.no_token;
        let yes_ask = ctx.snapshot.yes_ask;
        let no_ask = ctx.snapshot.no_ask;
        let crypto_filter = &ctx.crypto_filter;

        // Get threshold for this crypto
        let threshold = match crypto_filter.as_str() {
            "eth" => config::ETH_MOMENTUM_THRESHOLD,
            "sol" => config::SOL_MOMENTUM_THRESHOLD,
            _ => config::BTC_MOMENTUM_THRESHOLD,
        };

        // Get strike buffer for this crypto
        let strike_buffer = match crypto_filter.as_str() {
            "eth" => config::ETH_STRIKE_BUFFER,
            "sol" => config::SOL_STRIKE_BUFFER,
            _ => config::BTC_STRIKE_BUFFER,
        };

        // ── Multi-timeframe confirmation gates ────────────────────────────────
        //
        // Gate 1: Short-window (1s) confirmation.
        //   The 5s velocity might be elevated from an impulse that already
        //   ended. The 1s velocity must still be at least 40% of threshold,
        //   proving the move is still happening right now.
        let short_min = threshold * config::MOMENTUM_SHORT_WINDOW_FRACTION;
        let short_ok_bull = velocity_1s >= short_min;
        let short_ok_bear = velocity_1s <= -short_min;

        // Gate 2: Acceleration check.
        //   Positive acceleration = building momentum → green light.
        //   Negative acceleration = decelerating. We still allow entry if the
        //   signal is very strong (≥ ACCELERATION_BYPASS_MULTIPLIER × threshold),
        //   because a powerful but slightly-fading move is still worth trading.
        let accel_bypass = threshold * config::MOMENTUM_ACCELERATION_BYPASS_MULTIPLIER;
        let accel_ok_bull = acceleration >= dec!(0) || velocity >= accel_bypass;
        let accel_ok_bear = acceleration <= dec!(0) || velocity <= -accel_bypass;

        // Call existing logic - if strike exists, use it
        if let Some(strike) = strike_price {
            // Primary entry: velocity strong AND price has clearly cleared strike ± buffer.
            if velocity > threshold && binance_price > (strike + strike_buffer) && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && short_ok_bull && accel_ok_bull
            {
                return Ok(StrategySignal::Entry { token_id: yes_token });
            } else if velocity < -threshold && binance_price < (strike - strike_buffer) && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && short_ok_bear && accel_ok_bear
            {
                return Ok(StrategySignal::Entry { token_id: no_token });
            }

            // Secondary "strike-crossing" entry:
            // Price has already crossed the strike in the momentum direction but hasn't yet
            // cleared the full buffer. Only valid when the token is still significantly discounted
            // (ask ≤ crossing cap), meaning the book hasn't yet repriced the in-the-money side.
            if velocity > threshold && binance_price > strike && yes_ask <= config::MAX_MOMENTUM_CROSSING_ENTRY_PRICE
                && short_ok_bull && accel_ok_bull
            {
                return Ok(StrategySignal::Entry { token_id: yes_token });
            } else if velocity < -threshold && binance_price < strike && no_ask <= config::MAX_MOMENTUM_CROSSING_ENTRY_PRICE
                && short_ok_bear && accel_ok_bear
            {
                return Ok(StrategySignal::Entry { token_id: no_token });
            }
        } else {
            // Without strike: simpler velocity-based evaluation
            if velocity > threshold && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && short_ok_bull && accel_ok_bull
            {
                return Ok(StrategySignal::Entry { token_id: yes_token });
            } else if velocity < -threshold && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE
                && short_ok_bear && accel_ok_bear
            {
                return Ok(StrategySignal::Entry { token_id: no_token });
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    /// Evaluate if we should exit a momentum position
    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        use crate::state::PositionMap;
        use tokio::sync::MutexGuard;

        // Lock and iterate — only inspect positions owned by MomentumStrategy
        let positions: MutexGuard<PositionMap> = ctx.positions.lock().await;

        for ((strategy_name, token_id), position) in positions.iter() {
            if strategy_name != "MomentumStrategy" { continue; }
            // Determine if this is a YES or NO position and get the bid price
            let position_bid = if token_id == &ctx.market.yes_token {
                ctx.snapshot.yes_bid
            } else if token_id == &ctx.market.no_token {
                ctx.snapshot.no_bid
            } else {
                continue; // Not a token in this market
            };

            let avg_entry = position.avg_entry;
            let velocity = ctx.snapshot.velocity;
            let velocity_1s = ctx.snapshot.velocity_1s;
            let crypto_filter = &ctx.crypto_filter;

            // Get threshold for this crypto
            let threshold = match crypto_filter.as_str() {
                "eth" => config::ETH_MOMENTUM_THRESHOLD,
                "sol" => config::SOL_MOMENTUM_THRESHOLD,
                _ => config::BTC_MOMENTUM_THRESHOLD,
            };

            // Check exit conditions
            if avg_entry <= dec!(0) {
                continue;
            }

            let profit_margin = (position_bid - avg_entry) / avg_entry;

            // ── Near-expiry forced exit ───────────────────────────────────────
            // If the market is within MOMENTUM_EXPIRY_EXIT_SECS of close AND the
            // position is not sufficiently profitable, exit immediately.
            // This prevents the "ride to zero at binary resolution" failure mode
            // where a position hovers just above the stop-loss then goes to $0.
            if let Some(close_time) = ctx.market.market_close_time {
                let secs_to_expiry = (close_time - chrono::Utc::now()).num_seconds();
                if secs_to_expiry <= config::MOMENTUM_EXPIRY_EXIT_SECS
                    && profit_margin < config::MOMENTUM_EXPIRY_MIN_PROFIT_TO_HOLD
                {
                    return Ok(StrategySignal::Exit {
                        token_id: *token_id,
                        reason: format!(
                            "NearExpiry: {}s to close, profit={:.2}% < hold threshold {:.2}% — exiting to avoid binary resolution risk",
                            secs_to_expiry,
                            profit_margin * dec!(100),
                            config::MOMENTUM_EXPIRY_MIN_PROFIT_TO_HOLD * dec!(100),
                        ),
                    });
                }
            }
            let target = if avg_entry >= dec!(0.70) {
                dec!(0.05)
            } else {
                config::MOMENTUM_TARGET_PROFIT_PERCENT
            };
            let stop_loss = -config::MOMENTUM_STOP_LOSS_PERCENT;
            let reversal_threshold = -(threshold * config::MOMENTUM_REVERSAL_RATIO);
            let now = chrono::Utc::now();
            let secs_held = (now - position.opened_at).num_seconds();

            // Check take profit
            if profit_margin >= target || position_bid >= config::MOMENTUM_TAKE_PROFIT_CEILING {
                return Ok(StrategySignal::Exit {
                    token_id: *token_id,
                    reason: format!(
                        "TakeProfit: bid=${:.4}, profit={:.2}%, target={:.2}%",
                        position_bid, profit_margin * dec!(100), target * dec!(100)
                    ),
                });
            }

            // Check stop loss
            if profit_margin <= stop_loss {
                return Ok(StrategySignal::Exit {
                    token_id: *token_id,
                    reason: format!("StopLoss: bid=${:.4}, loss={:.2}%", position_bid, profit_margin * dec!(100)),
                });
            }

            // ── Velocity-decay early take-profit ──────────────────────────────
            // If we're in profit AND the short-term velocity has collapsed below
            // DECAY_EXIT_FRACTION × threshold, the move has likely exhausted.
            // Exit now rather than wait for a full reversal to eat the gains.
            let decay_min = threshold * config::MOMENTUM_DECAY_EXIT_FRACTION;
            let is_yes_position = token_id == &ctx.market.yes_token;
            let velocity_decayed = if is_yes_position {
                velocity_1s < decay_min   // bull position: upward momentum has faded
            } else {
                velocity_1s > -decay_min  // bear position: downward momentum has faded
            };
            if profit_margin > dec!(0) && velocity_decayed {
                return Ok(StrategySignal::Exit {
                    token_id: *token_id,
                    reason: format!(
                        "MomentumDecay: bid=${:.4}, profit={:.2}%, velocity_1s={:.6}",
                        position_bid, profit_margin * dec!(100), velocity_1s
                    ),
                });
            }

            // Check reversal — direction-aware:
            // YES position (entered on positive velocity) reverses when velocity goes strongly negative.
            // NO position (entered on negative velocity) reverses when velocity goes strongly positive.
            let reversal_triggered = if is_yes_position {
                velocity < reversal_threshold  // YES: exit on strong negative velocity
            } else {
                velocity > -reversal_threshold // NO: exit on strong positive velocity
            };
            if secs_held >= config::MOMENTUM_MIN_HOLD_SECS_BEFORE_REVERSAL
                && reversal_triggered
            {
                return Ok(StrategySignal::Exit {
                    token_id: *token_id,
                    reason: format!(
                        "Reversal: velocity={:.6}, threshold={:.6}",
                        velocity, reversal_threshold
                    ),
                });
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    /// Get current status of the strategy
    fn status(&self) -> StrategyStatus {
        StrategyStatus::Active
    }

    /// Strategy name for logging
    fn name(&self) -> String {
        "MomentumStrategy".to_string()
    }
}

/// Compute a fractional Kelly trade size based on momentum signal strength.
///
/// Trade size scales linearly from `MOMENTUM_MIN_TRADE_SIZE_USDC` (at exactly 1× threshold)
/// to `MOMENTUM_MAX_TRADE_SIZE_USDC` (at `MOMENTUM_KELLY_MAX_MULTIPLIER`× threshold or above).
///
/// Formula:
///   signal_strength = |velocity| / threshold          (clamped to [1.0, MAX_MULTIPLIER])
///   kelly_fraction  = (signal_strength − 1) / (MAX_MULTIPLIER − 1)   // 0.0 → 1.0
///   trade_size      = MIN + kelly_fraction × (MAX − MIN)
///
/// Examples (BTC, threshold = $75/5s):
///   |velocity| =  $75  → 1× → $5.00  (marginal signal, minimum bet)
///   |velocity| = $150  → 2× → $11.67
///   |velocity| = $225  → 3× → $18.33
///   |velocity| = $300+ → 4× → $25.00 (high-conviction, maximum bet)
pub fn kelly_momentum_size(velocity: rust_decimal::Decimal, threshold: rust_decimal::Decimal) -> rust_decimal::Decimal {
    use tracing::info;

    if threshold <= rust_decimal::Decimal::ZERO {
        return config::MOMENTUM_MIN_TRADE_SIZE_USDC;
    }

    let signal_strength = (velocity.abs() / threshold)
        .max(rust_decimal::Decimal::ONE)
        .min(config::MOMENTUM_KELLY_MAX_MULTIPLIER);

    let max_mult = config::MOMENTUM_KELLY_MAX_MULTIPLIER;
    let min_size = config::MOMENTUM_MIN_TRADE_SIZE_USDC;
    let max_size = config::MOMENTUM_MAX_TRADE_SIZE_USDC;

    let kelly_fraction = (signal_strength - rust_decimal::Decimal::ONE) / (max_mult - rust_decimal::Decimal::ONE);
    let sized = min_size + kelly_fraction * (max_size - min_size);

    info!(
        "📐 Kelly sizing: |velocity|={:.2}, threshold={:.2}, strength={:.2}×, size=${:.2}",
        velocity.abs(), threshold, signal_strength, sized
    );

    sized
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use crate::state::MarketConfig;
    use chrono::Utc;
    use rust_decimal::Decimal;

    #[tokio::test]
    async fn test_momentum_entry_above_threshold() {
        let strategy = MomentumStrategyImpl;

        let ctx = create_test_context(
            dec!(5.0),   // velocity above ETH threshold of 3.0
            dec!(2000),  // oracle price
            Some(dec!(1950)), // strike price
            dec!(0.35),  // yes_ask (good price)
            dec!(0.70),  // no_ask
        );

        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        match signal {
            StrategySignal::Entry { token_id } => {
                assert_eq!(token_id, ctx.market.yes_token);
            }
            _ => panic!("Expected Entry signal, got {:?}", signal),
        }
    }

    #[tokio::test]
    async fn test_momentum_no_signal_below_threshold() {
        let strategy = MomentumStrategyImpl;

        let ctx = create_test_context(
            dec!(1.5),   // velocity below ETH threshold of 3.0
            dec!(2000),
            Some(dec!(1950)),
            dec!(0.35),
            dec!(0.70),
        );

        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        match signal {
            StrategySignal::NoSignal => {} // Expected
            _ => panic!("Expected NoSignal, got {:?}", signal),
        }
    }

    // Helper function to create test context
    fn create_test_context(
        velocity: Decimal,
        oracle_price: Decimal,
        strike_price: Option<Decimal>,
        yes_ask: Decimal,
        no_ask: Decimal,
    ) -> StrategyContext {
        use crate::state::{MarketSnapshot, PositionMap};
        use alloy::primitives::U256;

        let yes_token = U256::from(1u64);
        let no_token = U256::from(2u64);

        StrategyContext {
            market: MarketConfig {
                yes_token,
                no_token,
                market_name: "Test Market".to_string(),
                market_close_time: None,
                strike_price,
                is_neg_risk: false,
                yes_fee_bps: 50,
                no_fee_bps: 50,
            },
            snapshot: MarketSnapshot {
                yes_bid: dec!(0.30),
                yes_ask,
                yes_ask_depth: dec!(100),
                no_bid: dec!(0.65),
                no_ask,
                no_ask_depth: dec!(100),
                oracle_price,
                velocity,
                velocity_1s: velocity, // same as 5s for tests — both above threshold
                acceleration: dec!(0),
                funding_rate: dec!(0),
                timestamp: Utc::now(),
            },
            positions: Arc::new(Mutex::new(PositionMap::new())),
            crypto_filter: "eth".to_string(),
            market_started_at: Utc::now(),
            maker_market: None,
            maker_snapshot: None,
        }
    }
}


