/// Orchestrator module: manages strategy lifecycle, market data distribution, and coordination.
///
/// The orchestrator acts as the central hub for:
/// - Strategy registration and instantiation
/// - Market data broadcasting to all strategies
/// - Signal collection and execution
/// - Position/order coordination between strategies

pub mod market_data;
pub mod strategy;
pub mod registry;
pub mod executor;

pub use market_data::MarketDataBroadcaster;
pub use strategy::{Strategy, StrategyContext};
pub use registry::StrategyRegistry;
pub use executor::{
    evaluate_strategies,
    prioritize_signals,
    StrategyEvaluationResult,
    execute_strategies_concurrent,
    aggregate_and_resolve_signals,
    SignalConflictInfo,
};
