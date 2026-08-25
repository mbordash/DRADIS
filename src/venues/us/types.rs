// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

//! Wire models for the Polymarket US retail REST API (`api.polymarket.us`).
//!
//! These types are the **only** place JSON ↔ Rust conversion happens for the US
//! venue. They are deliberately self-contained: no `polymarket_client_sdk_v2`,
//! no `alloy`, no neutral-core leakage. The venue boundary (`mod.rs`) maps these
//! onto the venue-neutral `OrderIntent`/`Fill`/`Position` types.
//!
//! Spec: `docs/us_retail_api.md` §3.

pub use polymarket_us::types::*;

// ─── Lenient market-discovery types (shadow the SDK's strict versions) ────────
//
// The live API returns `"outcomes":"[...]"` as a JSON-encoded *string*, not a
// JSON array.  The SDK's `UsMarket.outcomes: Vec<serde_json::Value>` fails to
// deserialize that — `#[serde(default)]` only helps when the key is *absent*,
// not when the value has the wrong JSON type.
//
// Defining `MarketsResponse` and `UsMarket` here (after the glob import) causes
// Rust to shadow the SDK's types with our lenient local versions everywhere in
// this crate that writes `types::MarketsResponse` / `types::UsMarket`.

use serde::Deserialize;

// ─── Public market reference data (GET /v1/markets) ──────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct MarketsResponse {
    #[serde(default)]
    pub markets: Vec<UsMarket>,
}

/// Lenient market record — tolerates the API's non-standard field shapes:
/// * `outcomes` arrives as a JSON-encoded string `"[...]"` (not an array) —
///   captured as a raw `Value` so deserialization never fails.
/// * `marketSides` contains deeply nested team/player objects — kept as `Value`.
#[derive(Debug, Clone, Deserialize)]
pub struct UsMarket {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub question: String,
    /// Variant label within a templated event (e.g. `"$200,000"` for the
    /// question `"Will Bitcoin be above ___ in 2026?"`). Empty on classic
    /// standalone markets.
    #[serde(default)]
    pub title: String,
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
    /// When the underlying event starts (e.g. game tip-off / fight start).
    /// A market whose `gameStartTime` is in the past has already been played —
    /// the event is over and trading is or will soon be closed.
    #[serde(default, rename = "gameStartTime")]
    pub game_start_time: Option<String>,
    #[serde(default, rename = "marketType")]
    pub market_type: String,
    /// Cumulative trading volume in USD, numeric form.
    ///
    /// The gateway documents two spellings of the same quantity — `volumeNum`
    /// as a number and `volume` as a string — and does not send both on every
    /// endpoint. Reading only `volumeNum` meant a response carrying `volume`
    /// silently produced 0 for every market, which then failed any liquidity
    /// floor and emptied the Control Tower's market browser while the trader's
    /// own wings were happily trading those same markets. Read
    /// [`Self::volume`] rather than either field directly.
    #[serde(default, rename = "volumeNum")]
    pub volume_num: Option<f64>,
    /// Cumulative trading volume, string form. See [`Self::volume_num`].
    #[serde(default, rename = "volume")]
    pub volume_str: Option<String>,
    /// Primary instrument legs — contains `long: bool` + `identifier` fields.
    #[serde(default, rename = "marketSides")]
    pub market_sides: Vec<serde_json::Value>,
    /// Legacy instrument list (older API shape). Kept for compatibility.
    #[serde(default)]
    pub instruments: Vec<serde_json::Value>,
    /// API sends this as a JSON-encoded string `"[\"Yes\",\"No\"]"` OR a real
    /// array — using `Value` here accepts both without panicking.
    #[serde(default)]
    pub outcomes: serde_json::Value,
}

impl UsMarket {
    /// Cumulative trading volume in USD, from whichever spelling the gateway
    /// sent. Zero when it sent neither — which means "not reported on this
    /// endpoint", not "nobody has traded it".
    pub fn volume(&self) -> f64 {
        self.volume_num
            .or_else(|| self.volume_str.as_deref().and_then(|s| s.trim().parse::<f64>().ok()))
            .unwrap_or(0.0)
    }
}


#[cfg(any())]
mod legacy {

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

}

// ─── Event search (GET /v1/search) ───────────────────────────────────────────

/// Response envelope for `GET /v1/search` — events matching a text query.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SearchResponse {
    #[serde(default)]
    pub events: Vec<SearchEvent>,
}

/// Lenient event record from search — only the fields discovery needs.
///
/// Search embeds full market records (same shape as `/v1/markets`, including
/// `marketSides` with `identifier`/`long`) — discovery uses these directly
/// because the api host ignores the `eventSlug=` filter on `/v1/markets`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SearchEvent {
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub markets: Vec<UsMarket>,
}





#[cfg(test)]
mod volume_parsing_tests {
    use super::UsMarket;

    /// The gateway documents two spellings of the same quantity. Reading only
    /// `volumeNum` produced 0 for every market on any endpoint that sends the
    /// string form, which then failed the Control Tower's liquidity floor and
    /// emptied the market browser for all three classes.
    #[test]
    fn reads_the_numeric_spelling() {
        let m: UsMarket = serde_json::from_str(r#"{"volumeNum": 12345.5}"#).unwrap();
        assert_eq!(m.volume(), 12345.5);
    }

    #[test]
    fn reads_the_string_spelling() {
        let m: UsMarket = serde_json::from_str(r#"{"volume": "12345.5"}"#).unwrap();
        assert_eq!(m.volume(), 12345.5);
    }

    /// When both arrive the numeric form wins — no parsing step to get wrong.
    #[test]
    fn prefers_the_numeric_spelling_when_both_are_sent() {
        let m: UsMarket = serde_json::from_str(r#"{"volumeNum": 7.0, "volume": "9999"}"#).unwrap();
        assert_eq!(m.volume(), 7.0);
    }

    /// Neither field means "not reported on this endpoint", which must not be
    /// confused with a market nobody has traded — see the browser's filter.
    #[test]
    fn missing_volume_is_zero_not_an_error() {
        let m: UsMarket = serde_json::from_str(r#"{"slug": "x"}"#).unwrap();
        assert_eq!(m.volume(), 0.0);
    }

    /// A malformed string degrades to zero rather than dropping the market.
    #[test]
    fn an_unparseable_string_degrades_to_zero() {
        let m: UsMarket = serde_json::from_str(r#"{"volume": "n/a"}"#).unwrap();
        assert_eq!(m.volume(), 0.0);
    }
}
