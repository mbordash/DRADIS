use rust_decimal::Decimal;
use crate::config;
use tracing::info;

pub struct RiskEngine;

impl RiskEngine {
    pub fn new() -> Self {
        Self
    }

    /// Return the configured max exposure for a given strategy name.
    pub fn strategy_max_exposure(strategy_name: &str) -> Decimal {
        match strategy_name {
            "MomentumStrategy"  => config::MOMENTUM_MAX_EXPOSURE_USDC,
            "MakerStrategy"     => config::MAKER_MAX_EXPOSURE_USDC,
            "ArbitrageStrategy" => config::ARBITRAGE_MAX_EXPOSURE_USDC,
            "TimeDecayStrategy" => config::TIME_DECAY_MAX_EXPOSURE_USDC,
            _                   => config::MAX_EXPOSURE_PER_TOKEN_USDC,
        }
    }

    /// Approve or reject a buy order.
    ///
    /// `max_exposure_usdc` should be the per-strategy budget from
    /// `RiskEngine::strategy_max_exposure(strategy_name)`.
    pub fn approve_buy(
        &self,
        yes_ask: Decimal,
        no_ask: Decimal,
        current_exposure_usdc: Decimal,
        trade_size_usdc: Decimal,
        starting_collateral: Decimal,
        session_pnl: Decimal,
        max_exposure_usdc: Decimal,
    ) -> bool {
        let sum_price = yes_ask + no_ask;

        if sum_price > config::MAX_SUM_PRICE_FOR_ENTRY {
            info!("🛡️ Risk Reject: Sum Price ${:.4} > Max ${:.4}", sum_price, config::MAX_SUM_PRICE_FOR_ENTRY);
            return false;
        }

        if current_exposure_usdc + trade_size_usdc > max_exposure_usdc {
            info!("🛡️ Risk Reject: Exposure ${:.2} would exceed Strategy Max ${:.2}", current_exposure_usdc + trade_size_usdc, max_exposure_usdc);
            return false;
        }

        let max_dd = config::max_session_drawdown(starting_collateral);
        if session_pnl <= -max_dd {
            info!("🛡️ Risk Reject: Session Drawdown ${:.2} >= Max ${:.2}", session_pnl.abs(), max_dd);
            return false;
        }

        true
    }
}