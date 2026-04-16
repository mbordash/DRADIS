/// Arbitrage Strategy
///
/// Hedged, two-sided trades that exploit the YES+NO spread inefficiency.
/// Entry triggers when combined ask prices fall below a profitability threshold.
/// Exits when combined bid prices exceed target, or via manual rebalancing.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal_macros::dec;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus};
use crate::config;

/// Implements Strategy trait for Arbitrage trading
pub struct ArbitrageStrategyImpl;

#[async_trait]
impl Strategy for ArbitrageStrategyImpl {
    /// Evaluate if an arbitrage entry signal should trigger
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        // Extract data from context
        let yes_ask = ctx.snapshot.yes_ask;
        let no_ask = ctx.snapshot.no_ask;
        let yes_fee_bps = ctx.market.yes_fee_bps;
        let no_fee_bps = ctx.market.no_fee_bps;

        // Check if arbitrage opportunity is profitable
        if is_arbitrage_profitable(yes_ask, no_ask, yes_fee_bps, no_fee_bps) {
            // For arbitrage, we buy both YES and NO, so return a synthetic signal
            // In practice, the orchestrator will need to handle buying both sides
            // We return YES token ID as the entry signal (both sides are implied)
            return Ok(StrategySignal::Entry {
                token_id: ctx.market.yes_token,
            });
        }

        Ok(StrategySignal::NoSignal)
    }

    /// Evaluate if we should exit an arbitrage position
    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        use crate::state::PositionMap;
        use tokio::sync::MutexGuard;

        let positions: MutexGuard<PositionMap> = ctx.positions.lock().await;

        // Only look at ArbitrageStrategy-owned positions
        let yes_key = ("ArbitrageStrategy".to_string(), ctx.market.yes_token);
        let no_key  = ("ArbitrageStrategy".to_string(), ctx.market.no_token);

        let yes_combined_bid = if positions.contains_key(&yes_key) { ctx.snapshot.yes_bid } else { dec!(0) };
        let no_combined_bid  = if positions.contains_key(&no_key)  { ctx.snapshot.no_bid  } else { dec!(0) };

        if positions.contains_key(&yes_key) && positions.contains_key(&no_key) {
            if should_arbitrage_exit(yes_combined_bid, no_combined_bid) {
                return Ok(StrategySignal::Exit {
                    token_id: ctx.market.yes_token,
                    reason: format!(
                        "Arbitrage convergence: YES bid=${:.4}, NO bid=${:.4}, combined=${:.4}",
                        yes_combined_bid,
                        no_combined_bid,
                        yes_combined_bid + no_combined_bid
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
        "ArbitrageStrategy".to_string()
    }
}

/// Helper: Check if arbitrage opportunity is profitable
fn is_arbitrage_profitable(
    yes_ask: rust_decimal::Decimal,
    no_ask: rust_decimal::Decimal,
    yes_fee_bps: u32,
    no_fee_bps: u32,
) -> bool {
    let combined_ask = yes_ask + no_ask;
    let profit_margin_no_fees = dec!(1.0) - combined_ask;

    let yes_fee = yes_ask * (rust_decimal::Decimal::from(yes_fee_bps) / dec!(10_000));
    let no_fee = no_ask * (rust_decimal::Decimal::from(no_fee_bps) / dec!(10_000));
    let profit_margin_with_fees = profit_margin_no_fees - (yes_fee + no_fee);

    profit_margin_with_fees >= config::ARBITRAGE_PROFIT_THRESHOLD
}

/// Helper: Check if we should exit arbitrage position
fn should_arbitrage_exit(yes_bid: rust_decimal::Decimal, no_bid: rust_decimal::Decimal) -> bool {
    yes_bid + no_bid >= config::EARLY_EXIT_COMBINED_BID_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::state::MarketConfig;
    use chrono::Utc;
    use rust_decimal::Decimal;

    #[tokio::test]
    async fn test_arbitrage_entry_profitable() {
        let strategy = ArbitrageStrategyImpl;

        let ctx = create_test_context(
            dec!(0.40),  // yes_ask
            dec!(0.45),  // no_ask
            // Combined: $0.85, Profit before fees: $0.15
            // With 50 bps fees on each: ~$0.14 profit (above threshold of 0.05)
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
    async fn test_arbitrage_no_signal_unprofitable() {
        let strategy = ArbitrageStrategyImpl;

        let ctx = create_test_context(
            dec!(0.50),  // yes_ask
            dec!(0.50),  // no_ask
            // Combined: $1.00, Profit: $0.00 (below threshold)
        );

        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        match signal {
            StrategySignal::NoSignal => {} // Expected
            _ => panic!("Expected NoSignal, got {:?}", signal),
        }
    }

    #[tokio::test]
    async fn test_arbitrage_exit_convergence() {
        use crate::state::{Position, MarketSnapshot, PositionMap};
        use alloy::primitives::U256;

        let strategy = ArbitrageStrategyImpl;
        let yes_token = U256::from(1u64);
        let no_token = U256::from(2u64);

        // Create context with positions that should trigger exit
        let mut positions = PositionMap::new();
        positions.insert(
            ("ArbitrageStrategy".to_string(), yes_token),
            Position {
                shares: dec!(100),
                avg_entry: dec!(0.40),
                opened_at: Utc::now(),
                close_time: None,
                market_name: "Test".to_string(),
                pair_token_id: yes_token,
                fill_confirmed_at: None,
            },
        );
        positions.insert(
            ("ArbitrageStrategy".to_string(), no_token),
            Position {
                shares: dec!(100),
                avg_entry: dec!(0.45),
                opened_at: Utc::now(),
                close_time: None,
                market_name: "Test".to_string(),
                pair_token_id: no_token,
                fill_confirmed_at: None,
            },
        );

        let ctx = StrategyContext {
            market: MarketConfig {
                yes_token,
                no_token,
                market_name: "Test Market".to_string(),
                market_close_time: None,
                strike_price: None,
                is_neg_risk: false,
                yes_fee_bps: 50,
                no_fee_bps: 50,
            },
            snapshot: MarketSnapshot {
                yes_bid: dec!(0.60),  // Bid has improved
                yes_ask: dec!(0.61),
                yes_ask_depth: dec!(100),
                no_bid: dec!(0.40),   // NO bid has improved
                no_ask: dec!(0.41),
                no_ask_depth: dec!(100),
                oracle_price: dec!(0),
                velocity: dec!(0),
                timestamp: Utc::now(),
                // Combined bid: 0.60 + 0.40 = 1.00, triggers early exit at 0.995+
            },
            positions: Arc::new(tokio::sync::Mutex::new(positions)),
            crypto_filter: "btc".to_string(),
        };

        let signal = strategy.evaluate_exit(&ctx).await.unwrap();
        match signal {
            StrategySignal::Exit { token_id, reason } => {
                assert_eq!(token_id, yes_token);
                assert!(reason.contains("convergence"));
            }
            _ => panic!("Expected Exit signal, got {:?}", signal),
        }
    }

    // Helper function to create test context
    fn create_test_context(
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
                strike_price: None,
                is_neg_risk: false,
                yes_fee_bps: 50,
                no_fee_bps: 50,
            },
            snapshot: MarketSnapshot {
                yes_bid: dec!(0.35),
                yes_ask,
                yes_ask_depth: dec!(100),
                no_bid: dec!(0.40),
                no_ask,
                no_ask_depth: dec!(100),
                oracle_price: dec!(0),
                velocity: dec!(0),
                timestamp: Utc::now(),
            },
            positions: Arc::new(tokio::sync::Mutex::new(PositionMap::new())),
            crypto_filter: "btc".to_string(),
        }
    }
}


