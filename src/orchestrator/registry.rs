/// Strategy Registry - Manages instantiation and lifecycle of all trading strategies
///
/// Provides a central registry for creating and managing strategy instances.
/// This enables uniform handling of all strategies through the Strategy trait.
///
/// ── Hot-Enable Design ────────────────────────────────────────────────────────
/// ALL strategies are ALWAYS instantiated at startup, regardless of compile-time
/// ENABLE_* flags.  The DynamicConfig enable flags (enable_gboost, enable_momentum,
/// etc.) are the SOLE runtime gates — checked on every tick in each strategy's
/// evaluate_entry().  This means the Control Tower UI's PATCH /api/config toggle
/// takes effect immediately during a running session without a redeploy.
///
/// The compile-time ENABLE_* constants in config.rs / config-live.rs now serve
/// only as the DEFAULT value seeded into DynamicConfig on first startup.

use crate::orchestrator::Strategy;
use crate::strategies::momentum_impl::MomentumStrategyImpl;
use crate::strategies::arbitrage_impl::ArbitrageStrategyImpl;
use crate::strategies::time_decay_impl::TimeDecayStrategyImpl;
use crate::strategies::maker_impl::MakerStrategyImpl;
use crate::strategies::basis_impl::BasisStrategyImpl;
use crate::strategies::gboost_impl::GboostStrategyImpl;

/// Registry for all available strategies
pub struct StrategyRegistry;

impl StrategyRegistry {
    /// Create a vector of ALL strategy instances.
    /// Every strategy is always instantiated so the DynamicConfig hot-patch can
    /// enable or disable any of them during a running session via the Control Tower UI.
    pub fn create_all_strategies() -> Vec<Box<dyn Strategy>> {
        vec![
            Box::new(MomentumStrategyImpl)           as Box<dyn Strategy>,
            Box::new(ArbitrageStrategyImpl)          as Box<dyn Strategy>,
            Box::new(TimeDecayStrategyImpl)          as Box<dyn Strategy>,
            Box::new(MakerStrategyImpl::new())       as Box<dyn Strategy>,
            Box::new(BasisStrategyImpl)              as Box<dyn Strategy>,
            Box::new(GboostStrategyImpl::default())  as Box<dyn Strategy>,
        ]
    }

    /// Create only momentum strategy
    pub fn create_momentum() -> Box<dyn Strategy> {
        Box::new(MomentumStrategyImpl)
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
        ]
        .into_iter().map(|s| s.to_string()).collect()
    }
}
