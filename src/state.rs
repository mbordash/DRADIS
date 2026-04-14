/// Shared state types for the orchestrator and strategies.
/// Defines clear ownership boundaries and data structures used across the system.

use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Represents a single position held in the trading system.
/// Shared across strategies and the main orchestrator.
#[derive(Debug, Clone)]
pub struct Position {
    /// Amount of shares held
    pub shares: Decimal,
    /// Average entry price
    pub avg_entry: Decimal,
    /// When the position was opened
    pub opened_at: DateTime<Utc>,
    /// When the position was closed (if applicable)
    pub close_time: Option<DateTime<Utc>>,
    /// Human-readable market name
    pub market_name: String,
    /// Token ID for this position
    pub pair_token_id: U256,
    /// When the position balance was confirmed on-chain
    pub fill_confirmed_at: Option<DateTime<Utc>>,
}

/// Shared positions state accessible by all strategies.
/// Typically wrapped in Arc<Mutex<>> for concurrent access.
pub type PositionMap = HashMap<U256, Position>;

/// Current market data snapshot.
/// Used for broadcasting to strategies.
#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    /// YES token best bid price
    pub yes_bid: Decimal,
    /// YES token best ask price
    pub yes_ask: Decimal,
    /// YES token ask-side depth
    pub yes_ask_depth: Decimal,
    /// NO token best bid price
    pub no_bid: Decimal,
    /// NO token best ask price
    pub no_ask: Decimal,
    /// NO token ask-side depth
    pub no_ask_depth: Decimal,
    /// Current oracle price from Binance
    pub oracle_price: Decimal,
    /// Price velocity (rate of change)
    pub velocity: Decimal,
    /// Timestamp of this snapshot
    pub timestamp: DateTime<Utc>,
}

/// Market identifiers and metadata.
#[derive(Debug, Clone)]
pub struct MarketConfig {
    /// YES token ID
    pub yes_token: U256,
    /// NO token ID
    pub no_token: U256,
    /// Human-readable market name
    pub market_name: String,
    /// Market close/expiry time
    pub market_close_time: Option<DateTime<Utc>>,
    /// Strike price (if applicable)
    pub strike_price: Option<Decimal>,
    /// Whether the market uses negative risk pricing
    pub is_neg_risk: bool,
    /// YES token fee rate in basis points
    pub yes_fee_bps: u32,
    /// NO token fee rate in basis points
    pub no_fee_bps: u32,
}

/// Strategy execution status for monitoring and lifecycle management.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyStatus {
    /// Strategy is active and evaluating
    Active,
    /// Strategy is disabled (e.g., no signal, cooldown)
    Disabled,
    /// Strategy encountered an error
    Error,
}

/// Signals returned by strategies for the orchestrator to act upon.
#[derive(Debug, Clone)]
pub enum StrategySignal {
    /// Entry signal: which token to buy
    Entry { token_id: U256 },
    /// Exit signal: which token to sell
    Exit { token_id: U256, reason: String },
    /// No action at this time
    NoSignal,
}

