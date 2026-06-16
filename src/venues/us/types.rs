//! Wire models for the Polymarket US retail REST API (`api.polymarket.us`).
//!
//! These types are the **only** place JSON ↔ Rust conversion happens for the US
//! venue. They are deliberately self-contained: no `polymarket_client_sdk_v2`,
//! no `alloy`, no neutral-core leakage. The venue boundary (`mod.rs`) maps these
//! onto the venue-neutral `OrderIntent`/`Fill`/`Position` types.
//!
//! Spec: `docs/us_retail_api.md` §3.

use serde::{Deserialize, Serialize};

// ─── Enumerated protocol constants ───────────────────────────────────────────
// The US gateway uses fully-qualified protobuf-style string enums. Kept as
// constants (not Rust enums) so an unrecognised server value never panics a
// deserialise — we only ever *send* these, and parse statuses leniently.

pub mod order_action {
    pub const BUY: &str = "ORDER_ACTION_BUY";
    pub const SELL: &str = "ORDER_ACTION_SELL";
}

pub mod order_type {
    pub const LIMIT: &str = "ORDER_TYPE_LIMIT";
}

pub mod tif {
    pub const GTC: &str = "TIME_IN_FORCE_GOOD_TILL_CANCEL";
    pub const GTD: &str = "TIME_IN_FORCE_GOOD_TILL_DATE";
    /// Fill-and-kill (immediate, partial allowed) — a.k.a. immediate-or-cancel.
    pub const FAK: &str = "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL";
    /// Fill-or-kill (immediate, all-or-nothing).
    pub const FOK: &str = "TIME_IN_FORCE_FILL_OR_KILL";
}

/// The custodial outcome leg of a market instrument.
pub mod outcome {
    pub const LONG: &str = "LONG";
    pub const SHORT: &str = "SHORT";
}

// ─── Public market reference data (GET /v1/markets) ──────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct MarketsResponse {
    #[serde(default)]
    pub markets: Vec<UsMarket>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UsMarket {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub category: String,
    #[serde(default, rename = "startDate")]
    pub start_date: String,
    #[serde(default, rename = "endDate")]
    pub end_date: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default, rename = "marketType")]
    pub market_type: String,
    // Use Value to avoid nested parsing issues with malformed fields
    #[serde(default, rename = "marketSides")]
    pub market_sides: Vec<serde_json::Value>,
    // Legacy fields for compatibility - use Value since structure may vary
    #[serde(default)]
    pub instruments: Vec<serde_json::Value>,
    #[serde(default)]
    pub outcomes: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketSide {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub identifier: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub price: String,
    #[serde(default)]
    pub long: bool,
    #[serde(default, rename = "marketSideType")]
    pub market_side_type: String,
    // Use Value for nested team/player data to skip problematic deserialization
    #[serde(default)]
    pub team: Option<serde_json::Value>,
    #[serde(default)]
    pub player: Option<serde_json::Value>,
    // Catch-all for any other fields
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UsInstrument {
    pub symbol: String,
    /// `LONG` (YES) or `SHORT` (NO).
    #[serde(default)]
    pub outcome: String,
    /// Integer scale applied to raw price feeds (typically 1000 → $0.001 ticks).
    #[serde(default = "default_price_scale")]
    pub price_scale: u32,
}

fn default_price_scale() -> u32 {
    1000
}

// ─── Health (GET /v1/health) ─────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct HealthResponse {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub timestamp: String,
}

// ─── Order placement (POST /v1/trading/orders) ───────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct PlaceOrderRequest {
    /// Instrument token symbol (e.g. `tec-nfl-sbw-2026-02-08-kc-yes`). The live
    /// API accepts symbol-addressed orders via `outcomeSide` + `action`, so the
    /// older `market_slug` + `intent` pairing is no longer needed.
    pub symbol: String,
    /// `BUY` / `SELL`.
    pub action: String,
    /// `LONG` (YES) / `SHORT` (NO) — derived from the instrument symbol.
    #[serde(rename = "outcomeSide")]
    pub outcome_side: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub price: Money,
    pub quantity: u64,
    pub tif: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
    /// Reject (rather than cross) if the order would take liquidity.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub post_only: bool,
    /// Expiry (epoch seconds) for `GOOD_TILL_DATE`; omitted otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Money {
    pub value: String,
    pub currency: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlaceOrderResponse {
    pub order_id: String,
    #[serde(default)]
    pub client_order_id: Option<String>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub filled_quantity: u64,
    #[serde(default)]
    pub remaining_quantity: u64,
    #[serde(default)]
    pub created_at: String,
}

// ─── Batched orders (POST /v1/orders/batched) ────────────────────────────────
// Engine-atomic multi-leg placement: the gateway accepts a token array of orders
// and (with `atomic = true`) either places them all or none. Used for the two
// legs of an arbitrage pair so a single-sided orphan cannot occur.

#[derive(Debug, Clone, Serialize)]
pub struct BatchedOrderRequest {
    pub orders: Vec<PlaceOrderRequest>,
    /// All-or-nothing placement of the whole batch.
    pub atomic: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchedOrderResponse {
    /// Per-order acks, index-aligned with the submitted `orders`.
    #[serde(default)]
    pub orders: Vec<PlaceOrderResponse>,
}

// ─── Order cancel (DELETE /v1/trading/orders/{id}) ───────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct CancelOrderResponse {
    pub order_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub cancelled_at: Option<String>,
}

// ─── Portfolio positions (GET /v1/portfolio/positions) ───────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct PortfolioPositionsResponse {
    /// Map of symbol → position. Empty `{}` if no positions.
    #[serde(default)]
    pub positions: std::collections::HashMap<String, UsPosition>,
    #[serde(default)]
    pub next_cursor: String,
    #[serde(default)]
    pub eof: bool,
    #[serde(default, rename = "availablePositions")]
    pub available_positions: Vec<UsPosition>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UsPosition {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub quantity: i64,
    #[serde(default, rename = "avgEntryPrice")]
    pub avg_entry_price: String,
    #[serde(default, rename = "unrealizedPnl")]
    pub unrealized_pnl: Option<String>,
}

// ─── Account balances (GET /v1/account/balances) ─────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct AccountBalancesResponse {
    #[serde(default)]
    pub balances: Vec<UserBalance>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserBalance {
    #[serde(default, rename = "currentBalance")]
    pub current_balance: f64,
    #[serde(default)]
    pub currency: String,
    #[serde(default, rename = "lastUpdated")]
    pub last_updated: Option<String>,
    #[serde(default, rename = "buyingPower")]
    pub buying_power: f64,
    #[serde(default, rename = "assetNotional")]
    pub asset_notional: Option<f64>,
    #[serde(default, rename = "assetAvailable")]
    pub asset_available: Option<f64>,
    #[serde(default, rename = "pendingCredit")]
    pub pending_credit: Option<f64>,
    #[serde(default, rename = "openOrders")]
    pub open_orders: Option<f64>,
    #[serde(default, rename = "unsettledFunds")]
    pub unsettled_funds: Option<f64>,
    #[serde(default, rename = "marginRequirement")]
    pub margin_requirement: Option<f64>,
    #[serde(default, rename = "balanceReservation")]
    pub balance_reservation: Option<f64>,
}

// ─── Combined portfolio (internal helper) ────────────────────────────────────

/// Combined view of positions + balances for `collateral()` and `positions()`.
#[derive(Debug, Clone)]
pub struct PortfolioResponse {
    pub positions: Vec<UsPosition>,
    pub buying_power: f64,
}




