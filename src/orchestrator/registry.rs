use crate::orchestrator::Strategy;
use crate::vipers::momentum_impl::MomentumStrategyImpl;
use crate::vipers::arbitrage_impl::ArbitrageStrategyImpl;
use crate::vipers::time_decay_impl::TimeDecayStrategyImpl;
use crate::vipers::maker_impl::MakerStrategyImpl;
use crate::vipers::basis_impl::BasisStrategyImpl;
use crate::vipers::gboost_impl::GboostStrategyImpl;
use crate::vipers::trendcapture_impl::TrendCaptureStrategyImpl;

/// Registry for all available strategies
pub struct StrategyRegistry;

impl StrategyRegistry {
    /// Create a vector of ALL strategy instances.
    /// Every strategy is always instantiated so the DynamicConfig hot-patch can
    /// enable or disable any of them during a running session via the Control Tower UI.
    pub fn create_all_strategies() -> Vec<Box<dyn Strategy>> {
        vec![
            Box::new(MomentumStrategyImpl::new())          as Box<dyn Strategy>,
            Box::new(ArbitrageStrategyImpl)                as Box<dyn Strategy>,
            Box::new(TimeDecayStrategyImpl)                as Box<dyn Strategy>,
            Box::new(MakerStrategyImpl::new())             as Box<dyn Strategy>,
            Box::new(BasisStrategyImpl)                    as Box<dyn Strategy>,
            Box::new(GboostStrategyImpl::default())        as Box<dyn Strategy>,
            Box::new(TrendCaptureStrategyImpl::new())      as Box<dyn Strategy>,
        ]
    }

    /// Create only momentum strategy
    pub fn create_momentum() -> Box<dyn Strategy> {
        Box::new(MomentumStrategyImpl::new())
    }

    /// Create only arbitrage strategy
    pub fn create_arbitrage() -> Box<dyn Strategy> {
        Box::new(ArbitrageStrategyImpl)
    }

    /// Create only time decay strategy
    pub fn create_time_decay() -> Box<dyn Strategy> {
        Box::new(TimeDecayStrategyImpl)
    }

    /// Create only maker strategy
    pub fn create_maker() -> Box<dyn Strategy> {
        Box::new(MakerStrategyImpl::new())
    }

    /// Return the names of all strategies, in priority order for orphan adoption.
    /// All strategies are always registered — DynamicConfig controls whether they trade.
    pub fn strategy_names() -> Vec<String> {
        vec![
            "MomentumStrategy",
            "ArbitrageStrategy",
            "TimeDecayStrategy",
            "MakerStrategy",
            "BasisStrategy",
            "GboostStrategy",
            "TrendCaptureStrategy",
        ]
        .into_iter().map(|s| s.to_string()).collect()
    }

    /// Returns the priority of a strategy (lower number = higher priority).
    /// Returns None if the strategy name is not found.
    pub fn get_strategy_priority(strategy_name: &str) -> Option<usize> {
        Self::strategy_names().iter().position(|s| s == strategy_name)
    }
}
