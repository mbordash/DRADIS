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
use crate::config;

/// Registry for all available strategies
pub struct StrategyRegistry;

impl StrategyRegistry {
    /// Create a vector of all enabled strategies
    pub fn create_all_strategies() -> Vec<Box<dyn Strategy>> {
        let mut strategies: Vec<Box<dyn Strategy>> = vec![
            Box::new(MomentumStrategyImpl),
            Box::new(ArbitrageStrategyImpl),
            Box::new(TimeDecayStrategyImpl),
        ];
        if config::ENABLE_MAKER_TRADING {
            strategies.push(Box::new(MakerStrategyImpl));
        }
        if config::ENABLE_BASIS_TRADING {
            strategies.push(Box::new(BasisStrategyImpl));
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
        Box::new(MakerStrategyImpl)
    }

    /// Return the names of all enabled strategies, in priority order for orphan adoption.
    /// This is the single source of truth used by balance reconciliation so that
    /// developers adding a new strategy only need to register it here — no other
    /// file needs to be updated to ensure orphaned positions are adopted correctly.
    pub fn strategy_names() -> Vec<String> {
        Self::create_all_strategies()
            .iter()
            .map(|s| s.name())
            .collect()
    }
}

