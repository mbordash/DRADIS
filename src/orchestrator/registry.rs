/// Strategy Registry - Manages instantiation and lifecycle of all trading strategies
///
/// Provides a central registry for creating and managing strategy instances.
/// This enables uniform handling of all strategies through the Strategy trait.

use crate::orchestrator::Strategy;
use crate::strategies::momentum_impl::MomentumStrategyImpl;
use crate::strategies::arbitrage_impl::ArbitrageStrategyImpl;
use crate::strategies::time_decay_impl::TimeDecayStrategyImpl;
use crate::strategies::maker_impl::MakerStrategyImpl;
use crate::strategies::basis_impl::BasisStrategyImpl;
use crate::strategies::gboost_impl::GboostStrategyImpl;
use crate::config;

/// Registry for all available strategies
pub struct StrategyRegistry;

impl StrategyRegistry {
    /// Create a vector of all enabled strategies
    pub fn create_all_strategies() -> Vec<Box<dyn Strategy>> {
        let mut strategies: Vec<Box<dyn Strategy>> = Vec::new();
        if config::ENABLE_MOMENTUM_TRADING {
            strategies.push(Box::new(MomentumStrategyImpl));
        }
        if config::ENABLE_ARBITRAGE_TRADING {
            strategies.push(Box::new(ArbitrageStrategyImpl));
        }
        if config::ENABLE_TIME_DECAY_TRADING {
            strategies.push(Box::new(TimeDecayStrategyImpl));
        }
        if config::ENABLE_MAKER_TRADING {
            strategies.push(Box::new(MakerStrategyImpl::new()));
        }
        if config::ENABLE_BASIS_TRADING {
            strategies.push(Box::new(BasisStrategyImpl));
        }
        if config::ENABLE_GBOOST_TRADING {
            strategies.push(Box::new(GboostStrategyImpl::default()));
        }
        strategies
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

    /// Return the names of all enabled strategies, in priority order for orphan adoption.
    /// This is the single source of truth used by balance reconciliation so that
    /// developers adding a new strategy only need to register it here — no other
    /// file needs to be updated to ensure orphaned positions are adopted correctly.
    ///
    /// NOTE: Returns static names WITHOUT instantiating strategies.
    /// Calling create_all_strategies() here would construct a second GboostStrategyImpl
    /// (and trigger a second async model-load tokio::spawn) on every market switch —
    /// doubling model-load I/O and retrain CPU for no benefit.
    pub fn strategy_names() -> Vec<String> {
        let mut names: Vec<&str> = Vec::new();
        if config::ENABLE_MOMENTUM_TRADING   { names.push("MomentumStrategy"); }
        if config::ENABLE_ARBITRAGE_TRADING  { names.push("ArbitrageStrategy"); }
        if config::ENABLE_TIME_DECAY_TRADING { names.push("TimeDecayStrategy"); }
        if config::ENABLE_MAKER_TRADING      { names.push("MakerStrategy"); }
        if config::ENABLE_BASIS_TRADING      { names.push("BasisStrategy"); }
        if config::ENABLE_GBOOST_TRADING     { names.push("GboostStrategy"); }
        names.into_iter().map(|s| s.to_string()).collect()
    }
}

