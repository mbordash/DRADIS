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

        // Call existing logic - if strike exists, use it
        if let Some(strike) = strike_price {
            // Primary entry: velocity strong AND price has clearly cleared strike ± buffer.
            if velocity > threshold && binance_price > (strike + strike_buffer) && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE {
                return Ok(StrategySignal::Entry { token_id: yes_token });
            } else if velocity < -threshold && binance_price < (strike - strike_buffer) && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE {
                return Ok(StrategySignal::Entry { token_id: no_token });
            }

            // Secondary "strike-crossing" entry:
            // Price has already crossed the strike in the momentum direction but hasn't yet
            // cleared the full buffer. Only valid when the token is still significantly discounted
            // (ask ≤ crossing cap), meaning the book hasn't yet repriced the in-the-money side.
            if velocity > threshold && binance_price > strike && yes_ask <= config::MAX_MOMENTUM_CROSSING_ENTRY_PRICE {
                return Ok(StrategySignal::Entry { token_id: yes_token });
            } else if velocity < -threshold && binance_price < strike && no_ask <= config::MAX_MOMENTUM_CROSSING_ENTRY_PRICE {
                return Ok(StrategySignal::Entry { token_id: no_token });
            }
        } else {
            // Without strike: simpler velocity-based evaluation
            if velocity > threshold && yes_ask <= config::MAX_MOMENTUM_ENTRY_PRICE {
                return Ok(StrategySignal::Entry { token_id: yes_token });
            } else if velocity < -threshold && no_ask <= config::MAX_MOMENTUM_ENTRY_PRICE {
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

            // Check reversal
            if secs_held >= config::MOMENTUM_MIN_HOLD_SECS_BEFORE_REVERSAL
                && velocity < reversal_threshold
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
                timestamp: Utc::now(),
            },
            positions: Arc::new(Mutex::new(PositionMap::new())),
            crypto_filter: "eth".to_string(),
        }
    }
}


