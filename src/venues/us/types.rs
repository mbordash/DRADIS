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
    pub market_slug: String,
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub instruments: Vec<UsInstrument>,
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

// ─── Portfolio (GET /v1/portfolio/positions) ─────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct PortfolioResponse {
    #[serde(default)]
    pub positions: Vec<UsPosition>,
    pub balances: Balances,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UsPosition {
    pub symbol: String,
    #[serde(default)]
    pub quantity: i64,
    #[serde(default)]
    pub avg_entry_price: String,
    #[serde(default)]
    pub unrealized_pnl: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Balances {
    #[serde(default)]
    pub available_margin_usd: String,
    #[serde(default)]
    pub locked_margin_usd: Option<String>,
}




