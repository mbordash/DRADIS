/// Time Decay (Theta) Strategy
///
/// Exploits YES+NO price convergence toward $1.00 as hourly markets approach expiry.
///
/// Two modes:
/// - **Settlement**: combined_ask < $1.00 after fees → hold to settlement for guaranteed profit
/// - **Convergence**: combined_ask slightly above $1.00 (up to MAX) → exit when bids converge

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use chrono::{DateTime, Utc};
use alloy::primitives::U256;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus};
use crate::config;

/// Implements Strategy trait for Time Decay trading
pub struct TimeDecayStrategyImpl;

#[async_trait]
impl Strategy for TimeDecayStrategyImpl {
    /// Evaluate if a time decay entry signal should trigger
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        // Extract data from context
        let yes_ask = ctx.snapshot.yes_ask;
        let no_ask = ctx.snapshot.no_ask;
        let yes_fee_bps = ctx.market.yes_fee_bps;
        let no_fee_bps = ctx.market.no_fee_bps;

        // Calculate seconds to expiry
        let seconds_to_expiry = match ctx.market.market_close_time {
            Some(close_time) => (close_time - Utc::now()).num_seconds(),
            None => return Ok(StrategySignal::NoSignal), // No expiry info, can't evaluate
        };

        // Check if we're in the optimal time window
        if !TimeDecayStrategy::is_in_theta_window(seconds_to_expiry) {
            return Ok(StrategySignal::NoSignal);
        }

        // Check if theta opportunity exists
        if TimeDecayStrategy::calculate_theta_opportunity(
            yes_ask,
            no_ask,
            yes_fee_bps,
            no_fee_bps,
            seconds_to_expiry,
        ).is_some() {
            // For time decay, we buy both YES and NO, so return a synthetic signal
            // In practice, the orchestrator will need to handle buying both sides
            return Ok(StrategySignal::Entry {
                token_id: ctx.market.yes_token,
            });
        }

        Ok(StrategySignal::NoSignal)
    }

    /// Evaluate if we should exit a time decay position
    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        use crate::state::PositionMap;
        use tokio::sync::MutexGuard;

        let positions: MutexGuard<PositionMap> = ctx.positions.lock().await;

        // Only look at TimeDecayStrategy-owned positions
        let yes_key = ("TimeDecayStrategy".to_string(), ctx.market.yes_token);
        let no_key  = ("TimeDecayStrategy".to_string(), ctx.market.no_token);

        let has_yes_position = positions.contains_key(&yes_key);
        let has_no_position  = positions.contains_key(&no_key);

        // If we have both positions, check exit conditions
        if has_yes_position && has_no_position {
            let yes_bid = ctx.snapshot.yes_bid;
            let no_bid = ctx.snapshot.no_bid;

            // Check if convergence-mode should exit (bids have converged)
            if TimeDecayStrategy::should_convergence_exit(yes_bid, no_bid) {
                return Ok(StrategySignal::Exit {
                    token_id: ctx.market.yes_token,
                    reason: format!(
                        "Time Decay convergence: YES bid=${:.4}, NO bid=${:.4}, combined=${:.4} (threshold ${:.4})",
                        yes_bid,
                        no_bid,
                        yes_bid + no_bid,
                        config::TIME_DECAY_CONVERGENCE_EXIT_BID
                    ),
                });
            }

            // Check if spread has widened too much (stop loss)
            let combined_bid = yes_bid + no_bid;
            if combined_bid < config::TIME_DECAY_CONVERGENCE_EXIT_BID * (dec!(1) - config::TIME_DECAY_STOP_LOSS_PERCENT) {
                return Ok(StrategySignal::Exit {
                    token_id: ctx.market.yes_token,
                    reason: format!(
                        "Time Decay stop loss: spread widened, combined bid=${:.4}",
                        combined_bid
                    ),
                });
            }

            // Check market expiry (settlement mode exit)
            if let Some(close_time) = ctx.market.market_close_time {
                let seconds_to_expiry = (close_time - Utc::now()).num_seconds();
                // Exit shortly before expiry to avoid slippage at settlement
                if seconds_to_expiry < config::MARKET_EXPIRY_SAFETY_BUFFER_SECS as i64 {
                    return Ok(StrategySignal::Exit {
                        token_id: ctx.market.yes_token,
                        reason: format!(
                            "Time Decay market expiring soon: {}s left",
                            seconds_to_expiry
                        ),
                    });
                }
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
        "TimeDecayStrategy".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use crate::state::{MarketConfig, MarketSnapshot, PositionMap};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use alloy::primitives::U256;

    #[tokio::test]
    async fn test_time_decay_entry_settlement_mode() {
        let strategy = TimeDecayStrategyImpl;

        let now = Utc::now();
        let close_time = now + chrono::Duration::minutes(20); // 20 minutes to expiry

        let ctx = create_test_context(
            dec!(0.45),  // yes_ask
            dec!(0.48),  // no_ask
            // Combined: $0.93, Profit before fees: $0.07
            // With 50 bps fees on each: ~$0.065 profit (above min threshold of 0.002)
            Some(close_time),
            dec!(0),
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
    async fn test_time_decay_no_signal_outside_window() {
        let strategy = TimeDecayStrategyImpl;

        let now = Utc::now();
        let close_time = now + chrono::Duration::hours(2); // 2 hours to expiry (outside max window)

        let ctx = create_test_context(
            dec!(0.45),
            dec!(0.48),
            Some(close_time),
            dec!(0),
        );

        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        match signal {
            StrategySignal::NoSignal => {} // Expected
            _ => panic!("Expected NoSignal, got {:?}", signal),
        }
    }

    #[tokio::test]
    async fn test_time_decay_exit_convergence() {
        let strategy = TimeDecayStrategyImpl;

        let now = Utc::now();
        let close_time = now + chrono::Duration::minutes(5);

        let yes_token = U256::from(1u64);
        let no_token = U256::from(2u64);

        // Create context with positions that should trigger exit
        let mut positions = PositionMap::new();
        positions.insert(
            ("TimeDecayStrategy".to_string(), yes_token),
            crate::state::Position {
                shares: dec!(100),
                avg_entry: dec!(0.45),
                opened_at: Utc::now(),
                close_time: None,
                market_name: "Test".to_string(),
                pair_token_id: yes_token,
                fill_confirmed_at: None,
            },
        );
        positions.insert(
            ("TimeDecayStrategy".to_string(), no_token),
            crate::state::Position {
                shares: dec!(100),
                avg_entry: dec!(0.48),
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
                market_close_time: Some(close_time),
                strike_price: None,
                is_neg_risk: false,
                yes_fee_bps: 50,
                no_fee_bps: 50,
            },
            snapshot: MarketSnapshot {
                yes_bid: dec!(0.60),  // Bid has improved significantly
                yes_ask: dec!(0.61),
                yes_ask_depth: dec!(100),
                no_bid: dec!(0.40),   // NO bid has improved significantly
                no_ask: dec!(0.41),
                no_ask_depth: dec!(100),
                oracle_price: dec!(0),
                velocity: dec!(0),
                timestamp: Utc::now(),
                // Combined bid: 0.60 + 0.40 = 1.00, triggers exit at threshold
            },
            positions: Arc::new(Mutex::new(positions)),
            crypto_filter: "btc".to_string(),
            market_started_at: Utc::now(),
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
        close_time: Option<chrono::DateTime<Utc>>,
        velocity: Decimal,
    ) -> StrategyContext {
        let yes_token = U256::from(1u64);
        let no_token = U256::from(2u64);

        StrategyContext {
            market: MarketConfig {
                yes_token,
                no_token,
                market_name: "Test Market".to_string(),
                market_close_time: close_time,
                strike_price: None,
                is_neg_risk: false,
                yes_fee_bps: 50,
                no_fee_bps: 50,
            },
            snapshot: MarketSnapshot {
                yes_bid: dec!(0.40),
                yes_ask,
                yes_ask_depth: dec!(100),
                no_bid: dec!(0.43),
                no_ask,
                no_ask_depth: dec!(100),
                oracle_price: dec!(0),
                velocity,
                timestamp: Utc::now(),
            },
            positions: Arc::new(Mutex::new(PositionMap::new())),
            crypto_filter: "btc".to_string(),
            market_started_at: Utc::now(),
        }
    }
}



// ============================================================================
// Types (consolidated from time_decay.rs)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ThetaMode {
    /// Combined ask < $1.00 after fees — hold to settlement for guaranteed profit
    Settlement,
    /// Combined ask slightly > $1.00 — exit before settlement when bids converge
    Convergence,
}

#[derive(Debug, Clone)]
pub struct ThetaSignal {
    pub mode: ThetaMode,
    pub combined_ask: Decimal,
    pub net_profit_per_share: Decimal,
    pub total_fees: Decimal,
}

pub struct TimeDecayStrategy;

impl TimeDecayStrategy {
    pub fn calculate_theta_opportunity(
        yes_ask: Decimal,
        no_ask: Decimal,
        yes_fee_bps: u32,
        no_fee_bps: u32,
        seconds_to_expiry: i64,
    ) -> Option<ThetaSignal> {
        let combined_ask = yes_ask + no_ask;
        let yes_fee = yes_ask * Decimal::from(yes_fee_bps) / dec!(10_000);
        let no_fee = no_ask * Decimal::from(no_fee_bps) / dec!(10_000);
        let total_fees = yes_fee + no_fee;
        let net_profit = dec!(1.0) - combined_ask - total_fees;
        if net_profit >= config::MIN_TIME_DECAY_NET_PROFIT {
            return Some(ThetaSignal {
                mode: ThetaMode::Settlement,
                combined_ask,
                net_profit_per_share: net_profit,
                total_fees,
            });
        }
        if combined_ask <= config::MAX_TIME_DECAY_COMBINED_ASK
            && seconds_to_expiry < config::TIME_DECAY_CONVERGENCE_WINDOW_SECS
        {
            let convergence_target = config::TIME_DECAY_CONVERGENCE_EXIT_BID;
            let estimated_exit_profit = convergence_target - combined_ask - total_fees;
            if estimated_exit_profit > dec!(-0.005) {
                return Some(ThetaSignal {
                    mode: ThetaMode::Convergence,
                    combined_ask,
                    net_profit_per_share: estimated_exit_profit,
                    total_fees,
                });
            }
        }
        None
    }

    pub fn is_in_theta_window(seconds_to_expiry: i64) -> bool {
        seconds_to_expiry >= config::TIME_DECAY_MIN_SECS_TO_EXPIRY
            && seconds_to_expiry <= config::TIME_DECAY_MAX_SECS_TO_EXPIRY
    }

    pub fn calculate_current_pnl(
        current_yes_bid: Decimal,
        current_no_bid: Decimal,
        entry_combined_cost: Decimal,
        position_size: Decimal,
    ) -> (Decimal, Decimal) {
        let current_combined_bid = current_yes_bid + current_no_bid;
        let unrealized_pnl = (current_combined_bid - entry_combined_cost) * position_size;
        let pnl_pct = if entry_combined_cost > dec!(0) {
            (current_combined_bid - entry_combined_cost) / entry_combined_cost
        } else {
            dec!(0)
        };
        (unrealized_pnl, pnl_pct)
    }

    pub fn should_convergence_exit(
        current_yes_bid: Decimal,
        current_no_bid: Decimal,
    ) -> bool {
        current_yes_bid + current_no_bid >= config::TIME_DECAY_CONVERGENCE_EXIT_BID
    }
}

#[derive(Debug, Clone)]
pub enum EarlyExitReason {
    SpreadWidened {
        expected: Decimal,
        actual: Decimal,
    },
    MarketOutcomeObvious {
        dominant_side: String,
        confidence: Decimal,
    },
}

#[derive(Debug, Clone)]
pub struct TimeDecayPosition {
    pub yes_token_id: U256,
    pub no_token_id: U256,
    pub entry_time: DateTime<Utc>,
    pub expiry_time: DateTime<Utc>,
    pub yes_entry_price: Decimal,
    pub no_entry_price: Decimal,
    pub position_size: Decimal,
    pub total_invested: Decimal,
    pub mode: ThetaMode,
}

impl TimeDecayPosition {
    pub fn new(
        yes_id: U256,
        no_id: U256,
        entry_time: DateTime<Utc>,
        expiry_time: DateTime<Utc>,
        yes_price: Decimal,
        no_price: Decimal,
        size: Decimal,
        mode: ThetaMode,
    ) -> Self {
        let total_invested = (yes_price + no_price) * size;
        Self {
            yes_token_id: yes_id,
            no_token_id: no_id,
            entry_time,
            expiry_time,
            yes_entry_price: yes_price,
            no_entry_price: no_price,
            position_size: size,
            total_invested,
            mode,
        }
    }

    pub fn time_to_expiry(&self) -> i64 {
        (self.expiry_time - Utc::now()).num_seconds()
    }

    pub fn is_expired(&self) -> bool {
        self.time_to_expiry() <= 0
    }
}
