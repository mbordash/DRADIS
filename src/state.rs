/// Shared state types for the orchestrator and strategies.
/// Defines clear ownership boundaries and data structures used across the system.

use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use polymarket_client_sdk_v2::clob::types::OrderType; // Import OrderType

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
    /// For paired strategies (Arbitrage, TimeDecay): token ID of the complementary leg.
    /// If Some, this position is part of a hedged pair. Used to detect orphaned positions
    /// when the paired leg fails to fill.
    pub paired_leg_token_id: Option<U256>,
}

/// Compound key for the shared position map: (strategy_name, token_id).
/// Each strategy has its own position slot per token, enabling fully independent
/// capital allocation and eliminating cross-strategy entry conflicts (Option A).
pub type PositionKey = (String, U256);

/// Shared positions state accessible by all strategies.
/// Keyed by (strategy_name, token_id) so that MomentumStrategy and MakerStrategy
/// can both hold YES simultaneously without colliding.
/// Typically wrapped in Arc<Mutex<>> for concurrent access.
pub type PositionMap = HashMap<PositionKey, Position>;

/// Current market data snapshot.
/// Used for broadcasting to strategies.
#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    /// YES token best bid price
    pub yes_bid: Decimal,
    /// YES token bid-side depth (shares available at best bid)
    pub yes_bid_depth: Decimal,
    /// YES token best ask price
    pub yes_ask: Decimal,
    /// YES token ask-side depth (shares available at best ask)
    pub yes_ask_depth: Decimal,
    /// NO token best bid price
    pub no_bid: Decimal,
    /// NO token bid-side depth (shares available at best bid)
    pub no_bid_depth: Decimal,
    /// NO token best ask price
    pub no_ask: Decimal,
    /// NO token ask-side depth (shares available at best ask)
    pub no_ask_depth: Decimal,
    /// Current oracle price from Binance
    pub oracle_price: Decimal,
    /// Price velocity over the primary window (MOMENTUM_WINDOW_SECS = 5s)
    pub velocity: Decimal,
    /// Price velocity over the short window (1s) — confirms move is still happening NOW
    pub velocity_1s: Decimal,
    /// Velocity rate-of-change: velocity_now - velocity_prev_tick
    /// Positive = momentum building, negative = momentum fading
    pub acceleration: Decimal,
    /// Binance perpetual futures funding rate (from /fapi/v1/premiumIndex).
    /// Negative = shorts paying longs (bearish bias from smart money).
    /// Positive = longs paying shorts (bullish bias from smart money).
    /// Updated every ~60 seconds; zero if unavailable.
    pub funding_rate: Decimal,
    /// 60-minute oracle price drift (current_price − price_60_minutes_ago).
    /// Positive = BTC trending UP over the last hour.
    /// Negative = BTC trending DOWN over the last hour.
    /// Zero when insufficient history is available (first hour of bot runtime).
    /// Used by MakerStrategy to suppress adverse-side bids during slow sustained trends.
    pub oracle_drift_60m: Decimal,
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
    /// Polymarket condition ID (bytes32 hex) — required for on-chain merge operations.
    /// Empty string when not available (non-maker markets).
    pub condition_id: String,
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

/// Parameters required to place an order on the CLOB.
#[derive(Debug, Clone)]
pub struct OrderParams {
    pub token_id: U256,
    pub price: Decimal,
    pub shares: Decimal,
    pub fee_bps: u16,
    pub is_neg_risk: bool,
    pub market_name: String,
    pub condition_id: String,
    pub order_type: OrderType, // Added this field
}

/// Signals returned by strategies for the orchestrator to act upon.
#[derive(Debug, Clone)]
pub enum StrategySignal {
    /// Entry signal with all metadata. For paired strategies, this is the primary leg.
    Entry {
        params: OrderParams,
        /// If Some, the strategy also wants to buy this second leg (Arbitrage/TimeDecay).
        pair_params: Option<OrderParams>,
    },
    /// Two-sided maker quote with metadata.
    MakerQuote {
        yes: Option<OrderParams>,
        no: Option<OrderParams>,
    },
    /// Exit signal with metadata.
    Exit {
        params: OrderParams,
        reason: String,
        /// If true, also exit the other leg of a paired position.
        exit_pair: bool,
    },
    /// No action at this time
    NoSignal,
}
