use rust_decimal::Decimal;
use crate::config;

pub struct RiskEngine;

impl RiskEngine {
    pub fn new() -> Self {
        Self
    }

    /// Approves a pair-based arbitrage trade
    pub fn approve_buy(
        &self,
        yes_ask: Decimal,
        no_ask: Decimal,
        current_exposure_usdc: Decimal,
        trade_size_usdc: Decimal,
        starting_collateral: Decimal,
        session_pnl: Decimal,           // ← passed in from main.rs
    ) -> bool {
        let sum_price = yes_ask + no_ask;

        if sum_price > config::MAX_SUM_PRICE_FOR_ENTRY {
            return false;
        }

        if current_exposure_usdc + trade_size_usdc > config::MAX_EXPOSURE_PER_TOKEN_USDC {
            return false;
        }

        let max_dd = config::max_session_drawdown(starting_collateral);
        if session_pnl <= -max_dd {
            return false;
        }

        true
    }
}