use rust_decimal::Decimal;
use crate::config;

pub struct RiskEngine;

impl RiskEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn approve_buy(
        &self,
        fill_price: Decimal,
        current_exposure_usdc: Decimal,
        session_pnl: Decimal,
        trade_size_usdc: Decimal,
        starting_collateral: Decimal,
    ) -> bool {
        // Do not enter if the price of an individual share is too high.
        if fill_price > config::MAX_SHARE_PRICE_FOR_ENTRY {
            return false;
        }

        // Check total exposure. Note: for an arbitrage strategy, this is the combined cost.
        if current_exposure_usdc + trade_size_usdc > config::MAX_EXPOSURE_PER_TOKEN_USDC {
            return false;
        }

        // Enforce the session-level drawdown limit.
        let max_dd = config::max_session_drawdown(starting_collateral);
        if session_pnl <= -max_dd {
            return false;
        }

        true
    }
}