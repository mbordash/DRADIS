/// Arbitrage Strategy
///
/// Hedged, two-sided trades that exploit the YES+NO spread inefficiency.
/// Entry triggers when combined ask prices fall below a profitability threshold.
/// Exits when combined bid prices exceed target, or via manual rebalancing.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use crate::config;

pub struct ArbitrageStrategy;

impl ArbitrageStrategy {
    /// Calculate profit margin for an arbitrage opportunity
    /// Returns (gross_margin, net_margin_after_fees)
    pub fn calculate_profit_margin(
        yes_ask: Decimal,
        no_ask: Decimal,
        yes_fee_bps: u32,
        no_fee_bps: u32,
    ) -> (Decimal, Decimal) {
        let combined_ask = yes_ask + no_ask;
        let profit_margin_no_fees = dec!(1.0) - combined_ask;

        let yes_fee = yes_ask * (Decimal::from(yes_fee_bps) / dec!(10_000));
        let no_fee = no_ask * (Decimal::from(no_fee_bps) / dec!(10_000));
        let profit_margin_with_fees = profit_margin_no_fees - (yes_fee + no_fee);

        (profit_margin_no_fees, profit_margin_with_fees)
    }

    /// Check if arbitrage opportunity is profitable
    pub fn is_profitable(yes_ask: Decimal, no_ask: Decimal, yes_fee_bps: u32, no_fee_bps: u32) -> bool {
        let (_, net_margin) = Self::calculate_profit_margin(yes_ask, no_ask, yes_fee_bps, no_fee_bps);
        net_margin >= config::ARBITRAGE_PROFIT_THRESHOLD
    }

    /// Check if combined bids reach early exit target
    pub fn should_early_exit(yes_bid: Decimal, no_bid: Decimal) -> bool {
        yes_bid + no_bid >= config::EARLY_EXIT_COMBINED_BID_THRESHOLD
    }
}

