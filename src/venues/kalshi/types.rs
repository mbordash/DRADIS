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

//! Wire models for the Kalshi Trade API v2 (`external-api.kalshi.com`).
//!
//! Lenient by design: every field is `#[serde(default)]` so market records
//! with missing/extra fields never fail the whole response. The V2 API uses
//! **fixed-point dollar strings** (`"0.5600"`) for prices and fractional
//! contract counts (`"10.00"`) — parsed into `Decimal` at the boundary.

use rust_decimal::Decimal;
use serde::Deserialize;

/// Parse a Kalshi fixed-point dollar/count string, tolerating missing values.
pub fn fp(s: &str) -> Option<Decimal> {
    if s.is_empty() {
        return None;
    }
    s.parse().ok()
}

// ─── Markets (GET /markets, /markets/{ticker}) ───────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MarketsResponse {
    #[serde(default)]
    pub markets: Vec<KalshiMarket>,
    #[serde(default)]
    pub cursor: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MarketResponse {
    #[serde(default)]
    pub market: KalshiMarket,
}

/// One binary market (a single strike within an event).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct KalshiMarket {
    #[serde(default)]
    pub ticker: String,
    #[serde(default)]
    pub event_ticker: String,
    #[serde(default)]
    pub title: String,
    /// `"Yes" outcome subtitle, e.g. "$73,800 or above"`.
    #[serde(default)]
    pub yes_sub_title: String,
    #[serde(default)]
    pub no_sub_title: String,
    /// `active`, `initialized`, `closed`, `settled`, …
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub market_type: String,
    /// `greater` | `less` | `between` (strike semantics).
    #[serde(default)]
    pub strike_type: String,
    #[serde(default)]
    pub floor_strike: Option<f64>,
    #[serde(default)]
    pub cap_strike: Option<f64>,
    #[serde(default)]
    pub open_time: String,
    #[serde(default)]
    pub close_time: String,
    #[serde(default)]
    pub expected_expiration_time: String,
    // Fixed-point dollar strings (V2). Empty when the book is blank.
    #[serde(default)]
    pub yes_bid_dollars: String,
    #[serde(default)]
    pub yes_ask_dollars: String,
    #[serde(default)]
    pub no_bid_dollars: String,
    #[serde(default)]
    pub no_ask_dollars: String,
    #[serde(default)]
    pub last_price_dollars: String,
    #[serde(default)]
    pub liquidity_dollars: String,
    #[serde(default)]
    pub notional_value_dollars: String,
    /// Fixed-point contract counts.
    #[serde(default)]
    pub volume_fp: String,
    #[serde(default)]
    pub open_interest_fp: String,
    #[serde(default)]
    pub rules_primary: String,
}

impl KalshiMarket {
    /// Market close time, parsed.
    pub fn close_time_utc(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.close_time
            .parse::<chrono::DateTime<chrono::Utc>>()
            .ok()
    }

    /// The strike price for above/below markets (floor for `greater*`,
    /// cap for `less*`); `between` markets return the floor (range low).
    pub fn strike(&self) -> Option<f64> {
        // Live values include `greater_or_equal` / `less_or_equal` variants.
        if self.strike_type.starts_with("greater") {
            self.floor_strike
        } else if self.strike_type.starts_with("less") {
            self.cap_strike
        } else {
            self.floor_strike.or(self.cap_strike)
        }
    }
}

// ─── Series (GET /series) ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SeriesListResponse {
    #[serde(default)]
    pub series: Vec<KalshiSeries>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct KalshiSeries {
    #[serde(default)]
    pub ticker: String,
    #[serde(default)]
    pub title: String,
    /// `hourly` | `fifteen_min` | `daily` | `weekly` | …
    #[serde(default)]
    pub frequency: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub fee_type: String,
    #[serde(default)]
    pub fee_multiplier: Option<f64>,
}

// ─── Orderbook (GET /markets/{ticker}/orderbook) ─────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
pub struct OrderbookResponse {
    #[serde(default)]
    pub orderbook_fp: OrderbookFp,
}

/// Bids-only book: a YES bid at P ≡ a NO ask at 1−P, so yes+no bids together
/// describe the full market. Levels are `[price_dollars, quantity_fp]` pairs.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct OrderbookFp {
    #[serde(default)]
    pub yes_dollars: Vec<[String; 2]>,
    #[serde(default)]
    pub no_dollars: Vec<[String; 2]>,
}

impl OrderbookFp {
    /// Best YES bid (highest yes level), as `(price, qty)`.
    pub fn best_yes_bid(&self) -> Option<(Decimal, Decimal)> {
        Self::best(&self.yes_dollars)
    }

    /// Best NO bid (highest no level), as `(price, qty)`.
    pub fn best_no_bid(&self) -> Option<(Decimal, Decimal)> {
        Self::best(&self.no_dollars)
    }

    /// Best YES ask, derived: 1 − best NO bid.
    pub fn best_yes_ask(&self) -> Option<(Decimal, Decimal)> {
        self.best_no_bid()
            .map(|(p, q)| (Decimal::ONE - p, q))
    }

    fn best(levels: &[[String; 2]]) -> Option<(Decimal, Decimal)> {
        levels
            .iter()
            .filter_map(|l| Some((fp(&l[0])?, fp(&l[1])?)))
            .max_by(|a, b| a.0.cmp(&b.0))
    }
}

// ─── Portfolio ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
pub struct BalanceResponse {
    /// Available balance in fixed-point dollars (string) — V2 shape; older
    /// deployments return integer cents in `balance`.
    #[serde(default)]
    pub balance_dollars: String,
    #[serde(default)]
    pub balance: Option<i64>,
}

impl BalanceResponse {
    pub fn dollars(&self) -> Decimal {
        if let Some(d) = fp(&self.balance_dollars) {
            return d;
        }
        // Legacy integer cents.
        self.balance
            .map(|c| Decimal::new(c, 2))
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PositionsResponse {
    #[serde(default)]
    pub market_positions: Vec<MarketPosition>,
    #[serde(default)]
    pub cursor: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MarketPosition {
    #[serde(default)]
    pub ticker: String,
    /// Signed fixed-point contracts: positive = long YES, negative = long NO.
    #[serde(default)]
    pub position_fp: String,
    /// Legacy integer shape.
    #[serde(default)]
    pub position: Option<i64>,
    #[serde(default)]
    pub market_exposure_dollars: String,
    #[serde(default)]
    pub total_traded_dollars: String,
}

impl MarketPosition {
    /// Signed position in contracts (positive YES / negative NO).
    pub fn signed_contracts(&self) -> Decimal {
        if let Some(d) = fp(&self.position_fp) {
            return d;
        }
        self.position.map(Decimal::from).unwrap_or_default()
    }
}

// ─── Orders (V2: POST /portfolio/events/orders) ──────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
pub struct OrderResponse {
    #[serde(default)]
    pub order: KalshiOrder,
}

/// Pull the order out of a create-order response.
///
/// `POST /portfolio/events/orders` returns the order's fields at the top level,
/// not wrapped in `{"order": …}`. Deserialising the wrapper against a flat
/// payload succeeds into an all-default `KalshiOrder` (every field is
/// `#[serde(default)]`), which is how an empty `order_id` and a fabricated fill
/// count reached the trader unnoticed. Accept either shape.
pub fn order_from_response(raw: &serde_json::Value) -> KalshiOrder {
    let body = raw.get("order").unwrap_or(raw);
    serde_json::from_value(body.clone()).unwrap_or_default()
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct OrdersResponse {
    #[serde(default)]
    pub orders: Vec<KalshiOrder>,
    #[serde(default)]
    pub cursor: String,
}

/// One Kalshi order, as returned by either the create-order response or the
/// resting-orders listing.
///
/// The two endpoints do **not** use the same key names: the listing returns the
/// `*_fp` fixed-point variants, while `POST /portfolio/events/orders` returns
/// bare `count` / `remaining_count` / `fill_count`. Both spellings are declared
/// separately rather than via `#[serde(alias)]` — a payload carrying *both* keys
/// would make an aliased field a duplicate-field error, silently defaulting the
/// whole struct. Read them through the accessors below, never directly.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct KalshiOrder {
    #[serde(default)]
    pub order_id: String,
    #[serde(default)]
    pub client_order_id: String,
    #[serde(default)]
    pub ticker: String,
    /// `bid` (buy YES) | `ask` (sell YES ≡ buy NO) — V2 single-book sides.
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub price_dollars: String,
    /// Original requested count (listing spelling).
    #[serde(default)]
    pub count_fp: String,
    /// Original requested count (create-order spelling).
    #[serde(default)]
    pub count: String,
    /// Count still resting (listing spelling).
    #[serde(default)]
    pub remaining_count_fp: String,
    /// Count still resting (create-order spelling).
    #[serde(default)]
    pub remaining_count: String,
    /// Contracts actually filled. Authoritative on the create-order response;
    /// absent from the resting-orders listing.
    #[serde(default)]
    pub fill_count: String,
    /// Volume-weighted fill price **in YES-book terms**, regardless of which leg
    /// the intent named — a NO buy at $0.33 reports `0.6700`.
    #[serde(default)]
    pub average_fill_price: String,
    /// Per-contract taker fee actually charged (Kalshi's quadratic schedule).
    #[serde(default)]
    pub average_fee_paid: String,
    #[serde(default)]
    pub time_in_force: String,
    #[serde(default)]
    pub created_time: String,
}

impl KalshiOrder {
    /// Contracts originally requested, from whichever spelling is present.
    pub fn requested_count(&self) -> Option<Decimal> {
        fp(&self.count_fp).or_else(|| fp(&self.count))
    }

    /// Contracts still resting, from whichever spelling is present.
    pub fn remaining(&self) -> Option<Decimal> {
        fp(&self.remaining_count_fp).or_else(|| fp(&self.remaining_count))
    }

    /// Contracts actually filled.
    ///
    /// Prefers the explicit `fill_count`; falls back to `requested − remaining`
    /// only when both of those are present. Returns `None` when the shape is
    /// unrecognised — callers must treat that as "unknown", never as "filled".
    pub fn filled_count(&self) -> Option<Decimal> {
        if let Some(f) = fp(&self.fill_count) {
            return Some(f.max(Decimal::ZERO));
        }
        match (self.requested_count(), self.remaining()) {
            (Some(total), Some(rem)) => Some((total - rem).max(Decimal::ZERO)),
            _ => None,
        }
    }

    /// Average fill price expressed on the requested leg.
    ///
    /// Kalshi quotes a single YES book, so `average_fill_price` is always a YES
    /// price; the NO leg is its complement.
    pub fn avg_fill_price_for_leg(&self, is_yes: bool) -> Option<Decimal> {
        let yes_price = fp(&self.average_fill_price)?;
        if yes_price <= Decimal::ZERO || yes_price >= Decimal::ONE {
            return None;
        }
        Some(if is_yes { yes_price } else { Decimal::ONE - yes_price })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn orderbook_best_levels() {
        let book = OrderbookFp {
            yes_dollars: vec![
                ["0.0200".into(), "100.00".into()],
                ["0.0230".into(), "43.00".into()],
            ],
            no_dollars: vec![
                ["0.9700".into(), "50.00".into()],
                ["0.9760".into(), "12.00".into()],
            ],
        };
        assert_eq!(book.best_yes_bid(), Some((dec!(0.0230), dec!(43.00))));
        assert_eq!(book.best_no_bid(), Some((dec!(0.9760), dec!(12.00))));
        // yes ask = 1 - best no bid
        assert_eq!(book.best_yes_ask(), Some((dec!(0.0240), dec!(12.00))));
    }

    #[test]
    fn balance_prefers_dollars_string() {
        let b = BalanceResponse {
            balance_dollars: "120.5000".into(),
            balance: Some(999),
        };
        assert_eq!(b.dollars(), dec!(120.5000));
        let legacy = BalanceResponse {
            balance_dollars: String::new(),
            balance: Some(12050),
        };
        assert_eq!(legacy.dollars(), dec!(120.50));
    }

    #[test]
    fn market_strike_semantics() {
        let above = KalshiMarket {
            strike_type: "greater".into(),
            floor_strike: Some(73799.99),
            ..Default::default()
        };
        assert_eq!(above.strike(), Some(73799.99));
        let below = KalshiMarket {
            strike_type: "less".into(),
            cap_strike: Some(55200.0),
            ..Default::default()
        };
        assert_eq!(below.strike(), Some(55200.0));
    }

    #[test]
    fn position_signed_contracts() {
        let long_yes = MarketPosition {
            position_fp: "500.00".into(),
            ..Default::default()
        };
        assert_eq!(long_yes.signed_contracts(), dec!(500.00));
        let long_no = MarketPosition {
            position_fp: "-25.00".into(),
            ..Default::default()
        };
        assert_eq!(long_no.signed_contracts(), dec!(-25.00));
    }

    /// Verbatim create-order response for a filled NO buy (2026-08-10 02:16 ET,
    /// KXBTCD-26AUG1003 NO @ $0.33 × 8.93).
    const FILLED_BUY: &str = r#"{
        "average_fee_paid":"0.0154","average_fill_price":"0.6700",
        "client_order_id":"b4c96f93-b432-4335-be43-edff6af2c638",
        "fill_count":"8.93","order_id":"d6feeda5-377e-4c86-92e1-4f67107f3520",
        "remaining_count":"0.00","ts_ms":1786342601398
    }"#;

    /// Verbatim create-order response for the stop-loss that did NOT fill
    /// (2026-08-10 02:18 ET). Booking this as filled abandoned a live position.
    const ZERO_FILL_SELL: &str = r#"{
        "client_order_id":"5e869d1b-43ed-4a77-a54c-8b4a55375f62",
        "fill_count":"0.00","order_id":"eaf13dcd-eac5-4ce6-8bfa-b30e296ff372",
        "remaining_count":"0.00","ts_ms":1786342726821
    }"#;

    fn parse(s: &str) -> KalshiOrder {
        order_from_response(&serde_json::from_str(s).unwrap())
    }

    #[test]
    fn flat_create_order_response_is_parsed() {
        let o = parse(FILLED_BUY);
        assert_eq!(o.order_id, "d6feeda5-377e-4c86-92e1-4f67107f3520");
        assert_eq!(o.filled_count(), Some(dec!(8.93)));
    }

    #[test]
    fn zero_fill_is_not_reported_as_filled() {
        let o = parse(ZERO_FILL_SELL);
        assert_eq!(o.order_id, "eaf13dcd-eac5-4ce6-8bfa-b30e296ff372");
        assert_eq!(o.filled_count(), Some(dec!(0.00)));
    }

    #[test]
    fn avg_fill_price_is_yes_denominated() {
        let o = parse(FILLED_BUY);
        // Reported 0.6700 on the YES book == $0.33 on the NO leg we bought.
        assert_eq!(o.avg_fill_price_for_leg(false), Some(dec!(0.3300)));
        assert_eq!(o.avg_fill_price_for_leg(true), Some(dec!(0.6700)));
        assert_eq!(fp(&o.average_fee_paid), Some(dec!(0.0154)));
    }

    #[test]
    fn nested_listing_shape_still_parses() {
        let o = parse(
            r#"{"order":{"order_id":"abc","ticker":"KXBTCD-1","side":"bid",
                "count_fp":"10.00","remaining_count_fp":"4.00","price_dollars":"0.4200"}}"#,
        );
        assert_eq!(o.order_id, "abc");
        assert_eq!(o.requested_count(), Some(dec!(10.00)));
        // No fill_count on the listing — fall back to requested − remaining.
        assert_eq!(o.filled_count(), Some(dec!(6.00)));
    }

    #[test]
    fn unrecognised_shape_reports_unknown_not_filled() {
        let o = parse(r#"{"something_else":"1.00"}"#);
        assert!(o.order_id.is_empty());
        assert_eq!(o.filled_count(), None, "unknown must never read as filled");
    }
}
