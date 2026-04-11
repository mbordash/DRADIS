/// Time Decay (Theta) Strategy
///
/// Exploits YES+NO price convergence toward $1.00 as hourly markets approach expiry.
///
/// Two modes:
/// - **Settlement**: combined_ask < $1.00 after fees → hold to settlement for guaranteed profit
/// - **Convergence**: combined_ask slightly above $1.00 (up to MAX) → exit when bids converge

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use chrono::{DateTime, Utc};
use alloy::primitives::U256;

use crate::config;

// ============================================================================
// Types
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

// ============================================================================
// Strategy
// ============================================================================

pub struct TimeDecayStrategy;

impl TimeDecayStrategy {
    /// Evaluate whether a theta (time decay) opportunity exists.
    ///
    /// **Settlement mode**: `1.00 - combined_ask - fees > MIN_NET_PROFIT`
    ///   → guaranteed profit if held to expiry (one side always pays $1.00)
    ///
    /// **Convergence mode**: `combined_ask <= MAX_COMBINED_ASK` AND close to expiry
    ///   → spreads compress as expiry nears; exit when combined bid converges
    pub fn calculate_theta_opportunity(
        yes_ask: Decimal,
        no_ask: Decimal,
        yes_fee_bps: u32,
        no_fee_bps: u32,
        seconds_to_expiry: i64,
    ) -> Option<ThetaSignal> {
        let combined_ask = yes_ask + no_ask;

        // Fee calculation: fee = price * bps / 10_000 per side
        let yes_fee = yes_ask * Decimal::from(yes_fee_bps) / dec!(10_000);
        let no_fee = no_ask * Decimal::from(no_fee_bps) / dec!(10_000);
        let total_fees = yes_fee + no_fee;

        // Settlement mode: combined ask below $1.00 → guaranteed profit at expiry
        let net_profit = dec!(1.0) - combined_ask - total_fees;
        if net_profit >= config::MIN_TIME_DECAY_NET_PROFIT {
            return Some(ThetaSignal {
                mode: ThetaMode::Settlement,
                combined_ask,
                net_profit_per_share: net_profit,
                total_fees,
            });
        }

        // Convergence mode: combined ask slightly above $1.00 but within tolerance
        // Only valid in the convergence window (closer to expiry)
        if combined_ask <= config::MAX_TIME_DECAY_COMBINED_ASK
            && seconds_to_expiry < config::TIME_DECAY_CONVERGENCE_WINDOW_SECS
        {
            // Estimated profit from convergence (spread compression)
            // As expiry approaches, combined bid → ~$0.998+
            let convergence_target = config::TIME_DECAY_CONVERGENCE_EXIT_BID;
            let estimated_exit_profit = convergence_target - combined_ask - total_fees;

            if estimated_exit_profit > dec!(-0.005) {
                // Allow slightly negative estimated profit — convergence often overshoots
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

    /// Check if the market is in the theta-optimal entry window.
    /// For hourly crypto markets, the convergence acceleration zone is
    /// roughly the last 4–30 minutes before expiry.
    pub fn is_in_theta_window(seconds_to_expiry: i64) -> bool {
        seconds_to_expiry >= config::TIME_DECAY_MIN_SECS_TO_EXPIRY
            && seconds_to_expiry <= config::TIME_DECAY_MAX_SECS_TO_EXPIRY
    }

    /// Calculate current unrealized P&L for a time decay position
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

    /// Check if a convergence-mode position should exit because bids have converged
    pub fn should_convergence_exit(
        current_yes_bid: Decimal,
        current_no_bid: Decimal,
    ) -> bool {
        current_yes_bid + current_no_bid >= config::TIME_DECAY_CONVERGENCE_EXIT_BID
    }
}

// ============================================================================
// Early Exit Reasons (retained for compatibility)
// ============================================================================

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

// ============================================================================
// Position Tracking
// ============================================================================

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
