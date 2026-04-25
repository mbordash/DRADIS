/// Time Decay (Theta) Strategy
///
/// Exploits YES+NO price convergence toward $1.00 as hourly markets approach expiry.
///
/// This version prefers the **Window/Maker venue** to avoid high hourly fees.
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
        if !config::ENABLE_TIME_DECAY_TRADING {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Venue Selection: Prefer Window/Maker venue ─────────────────────
        let (market, snap) = if let (Some(mk_mkt), Some(mk_snap)) = (&ctx.maker_market, &ctx.maker_snapshot) {
            (mk_mkt, mk_snap)
        } else {
            (&ctx.market, &ctx.snapshot)
        };

        let yes_ask = snap.yes_ask;
        let no_ask = snap.no_ask;
        let yes_fee_bps = market.yes_fee_bps;
        let no_fee_bps = market.no_fee_bps;

        // Calculate seconds to expiry
        let seconds_to_expiry = match market.market_close_time {
            Some(close_time) => (close_time - Utc::now()).num_seconds(),
            None => return Ok(StrategySignal::NoSignal),
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
            // Orchestrator handles buying both sides when it sees this Entry
            return Ok(StrategySignal::Entry {
                token_id: market.yes_token,
            });
        }

        Ok(StrategySignal::NoSignal)
    }

    /// Evaluate if we should exit a time decay position
    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        use crate::state::PositionMap;
        use tokio::sync::MutexGuard;

        let positions: MutexGuard<PositionMap> = ctx.positions.lock().await;

        // TimeDecay often holds tokens from either Hourly or Window venues.
        // We need to find which venue matches the current held tokens.
        let (market, snap) = if let Some(mk) = &ctx.maker_market {
            // Check if our TimeDecay positions match the Maker venue tokens
            let yes_key = ("TimeDecayStrategy".to_string(), mk.yes_token);
            if positions.contains_key(&yes_key) {
                (mk, ctx.maker_snapshot.as_ref().unwrap())
            } else {
                (&ctx.market, &ctx.snapshot)
            }
        } else {
            (&ctx.market, &ctx.snapshot)
        };

        let yes_key = ("TimeDecayStrategy".to_string(), market.yes_token);
        let no_key  = ("TimeDecayStrategy".to_string(), market.no_token);

        if positions.contains_key(&yes_key) && positions.contains_key(&no_key) {
            let yes_bid = snap.yes_bid;
            let no_bid = snap.no_bid;

            if TimeDecayStrategy::should_convergence_exit(yes_bid, no_bid) {
                return Ok(StrategySignal::Exit {
                    token_id: market.yes_token,
                    reason: format!(
                        "Time Decay convergence: YES bid=${:.4}, NO bid=${:.4}, combined=${:.4}",
                        yes_bid, no_bid, yes_bid + no_bid
                    ),
                });
            }

            let combined_bid = yes_bid + no_bid;
            if combined_bid < config::TIME_DECAY_CONVERGENCE_EXIT_BID * (dec!(1) - config::TIME_DECAY_STOP_LOSS_PERCENT) {
                return Ok(StrategySignal::Exit {
                    token_id: market.yes_token,
                    reason: format!("Time Decay SL: combined bid=${:.4}", combined_bid),
                });
            }

            if let Some(close_time) = market.market_close_time {
                let seconds_to_expiry = (close_time - Utc::now()).num_seconds();
                if seconds_to_expiry < config::MARKET_EXPIRY_SAFETY_BUFFER_SECS as i64 {
                    return Ok(StrategySignal::Exit {
                        token_id: market.yes_token,
                        reason: format!("Time Decay Expiry: {}s left", seconds_to_expiry),
                    });
                }
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "TimeDecayStrategy".to_string() }
}

// ============================================================================
// Logic Helper
// ============================================================================

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

    pub fn should_convergence_exit(current_yes_bid: Decimal, current_no_bid: Decimal) -> bool {
        current_yes_bid + current_no_bid >= config::TIME_DECAY_CONVERGENCE_EXIT_BID
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ThetaMode { Settlement, Convergence }

#[derive(Debug, Clone)]
pub struct ThetaSignal {
    pub mode: ThetaMode,
    pub combined_ask: Decimal,
    pub net_profit_per_share: Decimal,
    pub total_fees: Decimal,
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
    pub fn time_to_expiry(&self) -> i64 {
        (self.expiry_time - Utc::now()).num_seconds()
    }

    pub fn is_expired(&self) -> bool {
        self.time_to_expiry() <= 0
    }
}
