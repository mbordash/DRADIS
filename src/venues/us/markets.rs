//! US retail market discovery (`GET /v1/markets`).
//!
//! Turns the gateway's market/instrument reference data into venue-neutral
//! *binary pairs* — each tradeable market that has both a `LONG` (YES) and a
//! `SHORT` (NO) instrument leg, keyed by the neutral [`MarketId`] (= symbol).
//! The arbitrage loop consumes these pairs directly; the pure
//! [`pair_markets`] reducer is unit-tested without any network.

use crate::venues::core::MarketId;

use super::types::{self, outcome as oc};

use chrono::{DateTime, Utc};

/// A tradeable binary market reduced to its two neutral leg ids.
#[derive(Debug, Clone)]
pub struct UsMarketPair {
    pub slug: String,
    pub question: String,
    /// `LONG` (YES) leg symbol.
    pub long: MarketId,
    /// `SHORT` (NO) leg symbol.
    pub short: MarketId,
    /// Market close/expiry time, parsed from the gateway's `endDate`.
    pub close_time: Option<DateTime<Utc>>,
    /// Cumulative USD trading volume — used to rank and rotate to the hottest market.
    pub volume: f64,
}

/// Parse the gateway's `endDate` string into a UTC instant.
///
/// Accepts RFC3339 (`2026-06-16T20:00:00Z`); returns `None` for empty or
/// unparseable values so a missing close time degrades to "always open" rather
/// than blocking the market.
fn parse_close_time(end_date: &str) -> Option<DateTime<Utc>> {
    if end_date.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(end_date)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

/// Reduce raw markets to binary `LONG`/`SHORT` pairs.
///
/// A market is included only when it is `ACTIVE` (or has no explicit status) and
/// exposes exactly one `LONG` and one `SHORT` instrument — the shape the
/// arbitrage strategy requires (`YES + NO = $1`). Non-binary or multi-outcome
/// markets are skipped.
pub fn pair_markets(markets: Vec<types::UsMarket>) -> Vec<UsMarketPair> {
    let mut out = Vec::new();
    for m in markets {
        // Skip markets the venue has explicitly closed (game played / trading halted).
        // `closed` is the only reliable signal — `gameStartTime` is the observation
        // window start for futures/climate markets and must NOT be used as a trade gate.
        if m.closed {
            continue;
        }
        if !m.status.is_empty() && !m.status.eq_ignore_ascii_case("ACTIVE") {
            // Also check the `active` boolean field
            if !m.active {
                continue;
            }
        }
        let mut long_sym = None;
        let mut short_sym = None;

        // Parse `marketSides` array (raw JSON values)
        for side_val in &m.market_sides {
            // Extract fields manually from the Value
            if let Some(side_type) = side_val.get("marketSideType").and_then(|v| v.as_str()) {
                if side_type != "MARKET_SIDE_TYPE_INSTRUMENT" {
                    continue;
                }
            }
            let identifier = side_val.get("identifier")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if identifier.is_empty() {
                continue;
            }
            let is_long = side_val.get("long")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            if is_long {
                long_sym.get_or_insert(identifier);
            } else {
                short_sym.get_or_insert(identifier);
            }
        }

        // Fallback: parse legacy `instruments`/`outcomes` arrays (spec structure).
        // `outcomes` may be a JSON-encoded string (e.g. `"[\"Yes\",\"No\"]"`) or a
        // real JSON array — treat it leniently so an unexpected shape doesn't block.
        if long_sym.is_none() || short_sym.is_none() {
            let outcomes_arr: Vec<serde_json::Value> = m.outcomes
                .as_array()
                .cloned()
                .unwrap_or_default();
            let legs: Vec<_> = m.instruments.iter().chain(outcomes_arr.iter()).collect();
            for inst_val in legs {
                let outcome = inst_val.get("outcome")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_ascii_uppercase();
                let symbol = inst_val.get("symbol")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if symbol.is_empty() {
                    continue;
                }
                match outcome.as_str() {
                    oc::LONG => long_sym.get_or_insert(symbol),
                    oc::SHORT => short_sym.get_or_insert(symbol),
                    _ => continue,
                };
            }
        }
        if let (Some(l), Some(s)) = (long_sym, short_sym) {
            out.push(UsMarketPair {
                slug: m.slug,
                question: m.question,
                long: MarketId::new(l),
                short: MarketId::new(s),
                close_time: parse_close_time(&m.end_date),
                volume: m.volume,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venues::us::types::UsMarket;
    use serde_json::json;

    /// Build a legacy-`instruments` leg as a raw JSON value (the shape
    /// `pair_markets` parses via `.get("symbol")` / `.get("outcome")`).
    fn inst(symbol: &str, outcome: &str) -> serde_json::Value {
        json!({ "symbol": symbol, "outcome": outcome, "priceScale": 1000 })
    }
    fn market(slug: &str, status: &str, instruments: Vec<serde_json::Value>) -> UsMarket {
        UsMarket {
            id: String::new(),
            slug: slug.to_string(),
            question: format!("Q {slug}?"),
            status: status.to_string(),
            category: String::new(),
            start_date: String::new(),
            end_date: String::new(),
            description: String::new(),
            active: status == "ACTIVE",
            closed: false,
            game_start_time: None,
            market_type: String::new(),
            volume: 10_000.0,
            market_sides: Vec::new(),
            instruments,
            outcomes: serde_json::Value::Array(Vec::new()),
        }
    }

    #[test]
    fn pairs_binary_active_markets() {
        let markets = vec![market(
            "chiefs-sb-lx",
            "ACTIVE",
            vec![inst("chiefs-sb-lx-yes", "LONG"), inst("chiefs-sb-lx-no", "SHORT")],
        )];
        let pairs = pair_markets(markets);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].long.as_str(), "chiefs-sb-lx-yes");
        assert_eq!(pairs[0].short.as_str(), "chiefs-sb-lx-no");
    }

    #[test]
    fn parses_close_time_from_end_date() {
        let mut m = market(
            "chiefs-sb-lx",
            "ACTIVE",
            vec![inst("chiefs-sb-lx-yes", "LONG"), inst("chiefs-sb-lx-no", "SHORT")],
        );
        m.end_date = "2026-06-16T20:00:00Z".to_string();
        let pairs = pair_markets(vec![m]);
        assert_eq!(pairs.len(), 1);
        assert_eq!(
            pairs[0].close_time,
            Some("2026-06-16T20:00:00Z".parse::<chrono::DateTime<chrono::Utc>>().unwrap())
        );

        // Empty / unparseable endDate → None (always-open market).
        let m2 = market("no-date", "ACTIVE", vec![inst("nd-yes", "LONG"), inst("nd-no", "SHORT")]);
        assert_eq!(pair_markets(vec![m2])[0].close_time, None);
    }

    #[test]
    fn skips_inactive_and_non_binary() {
        let markets = vec![
            market("closed", "RESOLVED", vec![inst("closed-yes", "LONG"), inst("closed-no", "SHORT")]),
            market("one-sided", "ACTIVE", vec![inst("one-sided-yes", "LONG")]),
            market("multi", "ACTIVE", vec![inst("a", "LONG"), inst("b", "SHORT"), inst("c", "LONG")]),
        ];
        let pairs = pair_markets(markets);
        // "multi" still pairs the first LONG + first SHORT; the others are dropped.
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].slug, "multi");
    }
}

