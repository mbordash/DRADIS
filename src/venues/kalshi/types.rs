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

#[derive(Debug, Clone, Default, Deserialize)]
pub struct OrdersResponse {
    #[serde(default)]
    pub orders: Vec<KalshiOrder>,
    #[serde(default)]
    pub cursor: String,
}

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
    /// Original requested count.
    #[serde(default)]
    pub count_fp: String,
    /// Count still resting.
    #[serde(default)]
    pub remaining_count_fp: String,
    #[serde(default)]
    pub time_in_force: String,
    #[serde(default)]
    pub created_time: String,
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
}
