use rust_decimal::Decimal;
use crate::config;
use tracing::info;

pub struct RiskEngine;

impl RiskEngine {
    pub fn new() -> Self { Self }

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
    /// `is_one_sided` should be true for strategies that only buy one token (e.g. Momentum, Maker).
    /// When true, the sum-price check is skipped since only one side is being purchased.
    pub fn approve_buy(
        &self,
        yes_ask: Decimal,
        no_ask: Decimal,
        current_exposure_usdc: Decimal,
        trade_size_usdc: Decimal,
        starting_collateral: Decimal,
        session_pnl: Decimal,
        max_exposure_usdc: Decimal,
        is_one_sided: bool,
    ) -> bool {
        let sum_price = yes_ask + no_ask;

        // Sum-price check only applies to two-sided strategies (Arbitrage, TimeDecay)
        // that buy BOTH YES and NO — for one-sided strategies it's irrelevant.
        if !is_one_sided && sum_price > config::MAX_SUM_PRICE_FOR_ENTRY {
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

    /// Approve a two-sided maker quote based on NET directional exposure.
    ///
    /// Risk for market making is ABS(YES_value - NO_value), not YES+NO.
    /// A balanced book has near-zero directional risk regardless of gross size,
    /// allowing the strategy to quote in larger notional without increasing drawdown risk.
    ///
    /// `yes_current` / `no_current` = current MakerStrategy position values.
    /// `yes_new` / `no_new` = USDC value of the new orders (0 if not quoting that side).
    pub fn approve_maker_net_exposure(
        &self,
        yes_current: Decimal,
        no_current: Decimal,
        yes_new: Decimal,
        no_new: Decimal,
        session_pnl: Decimal,
        starting_collateral: Decimal,
    ) -> bool {
        let projected_yes = yes_current + yes_new;
        let projected_no  = no_current  + no_new;
        let net_exposure  = (projected_yes - projected_no).abs();

        if net_exposure > config::MAKER_MAX_EXPOSURE_USDC {
            info!("🛡️ Maker Net Exposure Reject: |YES ${:.2} - NO ${:.2}| = ${:.2} > Max ${:.2}",
                projected_yes, projected_no, net_exposure, config::MAKER_MAX_EXPOSURE_USDC);
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
