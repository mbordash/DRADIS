/// Strategy trait: defines the interface all trading strategies must implement.
///
/// This trait allows strategies to be treated uniformly by the orchestrator,
/// enabling independent strategy threading later while maintaining a common interface.

use anyhow::Result;
use chrono::{DateTime, Utc};
use crate::state::{MarketConfig, MarketSnapshot, StrategySignal, StrategyStatus, PositionMap};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Context passed to strategies containing all market data and shared state they need.
#[derive(Clone)]
pub struct StrategyContext {
    /// Current market configuration and metadata (hourly market — used by all strategies)
    pub market: MarketConfig,
    /// Current market snapshot (prices, oracle, velocity)
    pub snapshot: MarketSnapshot,
    /// All open positions (shared, read-only for strategies)
    pub positions: Arc<Mutex<PositionMap>>,
    /// Crypto identifier (e.g., "BTC", "ETH", "SOL") for threshold lookups
    pub crypto_filter: String,
    /// Timestamp when the bot started trading the current market.
    /// Used by strategies to enforce a minimum market maturation period before entry.
    pub market_started_at: DateTime<Utc>,
    /// Optional alternative market for the Maker strategy.
    /// When Some, MakerStrategy uses this window/daily market instead of `market`.
    /// All other strategies ignore this field.
    pub maker_market: Option<MarketConfig>,
    /// Live price snapshot for the maker_market venue.
    /// Populated only when maker_market is Some.
    pub maker_snapshot: Option<MarketSnapshot>,
}

/// Trait that all strategies must implement.
/// Enables uniform handling and future per-strategy threading.
#[async_trait::async_trait]
pub trait Strategy: Send + Sync {
    /// Evaluate if strategy should execute an entry.
    ///
    /// Returns:
    /// - `Ok(StrategySignal::Entry { token_id })` if entry conditions are met
    /// - `Ok(StrategySignal::NoSignal)` if no action should be taken
    /// - `Err(e)` on unrecoverable errors
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal>;

    /// Evaluate if strategy should execute an exit.
    ///
    /// Returns:
    /// - `Ok(StrategySignal::Exit { token_id, reason })` if exit conditions are met
    /// - `Ok(StrategySignal::NoSignal)` if position should be held
    /// - `Err(e)` on unrecoverable errors
    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal>;

    /// Get current status of the strategy (for monitoring/lifecycle).
    fn status(&self) -> StrategyStatus;

    /// Strategy name for logging and identification.
    fn name(&self) -> String;
}

