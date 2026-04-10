use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use chrono::{DateTime, Utc};


pub struct TimeDecayStrategy;

impl TimeDecayStrategy {
    /// Check if time decay opportunity exists
    /// Returns the theoretical profit if we buy both sides now
    pub fn calculate_decay_spread(yes_ask: Decimal, no_ask: Decimal) -> Option<Decimal> {
        let combined_ask = yes_ask + no_ask;

        // If combined ask is below 1.0, we have a decay opportunity
        if combined_ask < dec!(1.0) {
            let spread = dec!(1.0) - combined_ask;
            // Only trade if spread is worth it (account for fees)
            if spread > dec!(0.01) {
                return Some(spread);
            }
        }
        None
    }

    /// Check if market is young enough to enter time decay
    /// For hourly markets: ideal to enter within first 10-20 minutes
    pub fn is_market_young_enough(_seconds_to_expiry: i64) -> bool {
        // Market should have at least 20+ minutes left and less than 55 minutes
        // (assuming 1-hour markets)
        _seconds_to_expiry < 3300 && _seconds_to_expiry > 1200
    }

    /// Calculate position size based on time decay opportunity
    /// Larger spread = higher confidence, can size up a bit
    /// Closer to expiry = smaller size (less time for spread to decay)
    pub fn calculate_position_size(
        spread: Decimal,
        available_balance: Decimal,
        _seconds_to_expiry: i64,
    ) -> Decimal {
        // Base position size (standard $5-10 per side)
        let mut position_size = dec!(5);

        // Scale up if spread is very attractive (>2%)
        if spread > dec!(0.02) {
            position_size = dec!(7);
        }
        if spread > dec!(0.025) {
            position_size = dec!(10);
        }

        // Don't exceed available balance (need $2x for both sides)
        position_size.min(available_balance / dec!(2))
    }

    /// Determine if we should EXIT early
    /// Reasons to exit: spread widens, market outcome becomes obvious, etc.
    pub fn should_early_exit(
        current_combined_bid: Decimal,
        entry_combined_cost: Decimal,
        _seconds_elapsed: i64,
    ) -> Option<EarlyExitReason> {
        // If spread widens significantly, exit (trade went wrong)
        let combined_bid_expected = entry_combined_cost + dec!(0.005); // Small improvement expected
        if current_combined_bid < combined_bid_expected - dec!(0.01) {
            return Some(EarlyExitReason::SpreadWidened {
                expected: combined_bid_expected,
                actual: current_combined_bid,
            });
        }

        // If we've held for most of the market duration, hold to expiry
        // (seconds_elapsed / time_to_expiry ratio high = close to expiry)
        // This logic would be checked by caller with seconds_to_expiry

        None
    }

    /// Calculate expected vs actual return
    pub fn calculate_return(
        total_invested: Decimal,
        current_combined_value: Decimal,
    ) -> Decimal {
        if total_invested <= dec!(0) {
            return dec!(0);
        }
        (current_combined_value - total_invested) / total_invested
    }

    /// Check if we should EXIT to lock in profit
    /// Returns Some(exit_price) if we've hit our profit target
    pub fn should_take_profit(
        current_combined_bid: Decimal,
        entry_combined_cost: Decimal,
        target_profit_pct: Decimal,
    ) -> Option<Decimal> {
        let target_exit_price = entry_combined_cost * (dec!(1) + target_profit_pct);

        if current_combined_bid >= target_exit_price {
            return Some(current_combined_bid);
        }
        None
    }

    /// Calculate current unrealized P&L
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
    pub yes_token_id: u128,
    pub no_token_id: u128,
    pub entry_time: DateTime<Utc>,
    pub expiry_time: DateTime<Utc>,
    pub yes_entry_price: Decimal,
    pub no_entry_price: Decimal,
    pub position_size: Decimal,
    pub total_invested: Decimal,
}

impl TimeDecayPosition {
    pub fn new(
        yes_id: u128,
        no_id: u128,
        entry_time: DateTime<Utc>,
        expiry_time: DateTime<Utc>,
        yes_price: Decimal,
        no_price: Decimal,
        size: Decimal,
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
        }
    }

    pub fn time_to_expiry(&self) -> i64 {
        (self.expiry_time - Utc::now()).num_seconds()
    }

    pub fn is_expired(&self) -> bool {
        self.time_to_expiry() <= 0
    }
}


