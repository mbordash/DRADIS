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

//! US retail market discovery (`GET /v1/markets`).
//!
//! Turns the gateway's market/instrument reference data into venue-neutral
//! *binary pairs* — each tradeable market that has both a `LONG` (YES) and a
//! `SHORT` (NO) instrument leg, keyed by the neutral [`MarketId`] (= symbol).
//! The arbitrage loop consumes these pairs directly; the pure
//! [`pair_markets`] reducer is unit-tested without any network.

use crate::venues::core::MarketId;

use super::types::{self, outcome as oc};

/// Suffixes distinguishing the two legs of a Polymarket US market.
///
/// The venue gives both sides one identifier and a `long: bool`. DRADIS keys
/// positions, books and orders by `MarketId`, so the leg has to live in the id.
/// `#` is used rather than `-` because a real symbol is dash-delimited and ends
/// in a team abbreviation (`-fcb`, `-lac`) — a dash suffix could not be told
/// apart from the venue's own naming.
pub const LEG_LONG: &str = "#long";
pub const LEG_SHORT: &str = "#short";

/// Strip the leg suffix, yielding the symbol the venue actually knows.
pub fn bare_symbol(symbol: &str) -> &str {
    symbol.split('#').next().unwrap_or(symbol)
}

/// Which leg a suffixed symbol refers to. `None` when unsuffixed.
pub fn leg_is_long(symbol: &str) -> Option<bool> {
    match symbol.split_once('#') {
        Some((_, "long")) => Some(true),
        Some((_, "short")) => Some(false),
        _ => None,
    }
}

use chrono::{DateTime, Utc};

/// A tradeable binary market reduced to its two neutral leg ids.
#[derive(Debug, Clone)]
pub struct UsMarketPair {
    pub slug: String,
    pub question: String,
    /// Venue category hint (e.g. `crypto`, `sports`) — feeds the shared
    /// market-class taxonomy so the trader can route pairs to the right wing.
    pub category: String,
    /// Venue long-form description — scanned as a strike-price fallback for
    /// crypto markets whose question omits the threshold.
    pub description: String,
    /// `LONG` (YES) leg symbol.
    pub long: MarketId,
    /// `SHORT` (NO) leg symbol.
    pub short: MarketId,
    /// Market close/expiry time, parsed from the gateway's `endDate`.
    pub close_time: Option<DateTime<Utc>>,
    /// Cumulative USD trading volume — used to rank and rotate to the hottest market.
    pub volume: f64,
}

/// Map one market's catalog entry to a leg's settlement answer.
///
/// Pure so the mapping is testable against captured gateway responses. The
/// discipline is the one on [`crate::venues::core::TokenResolution`]: only a
/// decisive venue answer books, "cannot have settled" is distinct from "no
/// answer yet", and everything else defers.
///
/// The gateway's market object is the venue's own settlement record: once a
/// market reaches `MARKET_STATUS_RESOLVED`, each `marketSides` entry's `price`
/// is pinned to that side's settlement value — exactly `"1"` for the paying
/// side and `"0"` for the other (verified live 2026-08-31 across resolved
/// NFL/NBA listings; open markets carry live mids like `"0.0800"` instead,
/// which is why the status gate comes first).
///
/// The leg's own side is read first; when its price is absent — the live API
/// has been seen sending one-element `outcomePrices` and side entries without
/// a price — the complement of the OTHER side's decisive value is used. A
/// resolved market with no decisive price on either side answers `Unknown`,
/// never a guess: the caller defers and this parser must not be the component
/// that fabricates a settlement.
///
/// The decisive gate also rejects any value above $1.00, so if the gateway
/// ever hands back something that is not a binary contract payout (an index
/// level, a notional), it becomes "no answer" rather than a booked win.
pub fn settlement_from_market(
    m: &types::UsMarket,
    is_long: bool,
) -> crate::venues::core::TokenResolution {
    use crate::venues::core::TokenResolution as R;
    use rust_decimal::Decimal;

    let status = m.status.to_ascii_uppercase();
    // Still trading: the position cannot have settled — settlement only exists
    // after resolution — so it left the account by a trade, which belongs on
    // the mark-priced reconciliation path.
    if status.ends_with("OPEN") || status.ends_with("ACTIVE") || (status.is_empty() && !m.closed) {
        return R::NotClosed;
    }
    if !status.ends_with("RESOLVED") {
        // Closed/suspended/unmodeled: resolution may be imminent. Defer.
        return R::Unknown;
    }

    // $0.00 or $1.00, within a cent — anything else is not a settlement value.
    let decisive = |px: Decimal| -> Option<Decimal> {
        if px >= Decimal::ZERO && px <= Decimal::new(1, 2) {
            Some(Decimal::ZERO)
        } else if px >= Decimal::new(99, 2) && px <= Decimal::ONE {
            Some(Decimal::ONE)
        } else {
            None
        }
    };
    let side_price = |want_long: bool| -> Option<Decimal> {
        m.market_sides.iter().find_map(|s| {
            if s.get("long").and_then(|v| v.as_bool())? != want_long {
                return None;
            }
            s.get("price")
                .and_then(|v| v.as_str())
                .and_then(|p| p.trim().parse::<Decimal>().ok())
        })
    };

    if let Some(v) = side_price(is_long).and_then(decisive) {
        return R::Resolved(v);
    }
    if let Some(v) = side_price(!is_long).and_then(decisive) {
        return R::Resolved(Decimal::ONE - v);
    }
    R::Unknown
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

            // Both sides carry the SAME identifier — Polymarket US addresses a
            // market with one symbol and puts the side in a boolean, where
            // Polymarket International has two ERC-1155 token ids. Without a
            // suffix the two legs collapse into one MarketId: the order builder
            // then cannot tell which side to trade (it infers the side from the
            // symbol) and the book feed subscribes twice to the same stream
            // while treating it as two independent sides.
            //
            // Suffixing mirrors Kalshi, which has the same one-ticker-two-sides
            // model. The bare identifier is recovered by `bare_symbol` wherever
            // the value goes on the wire.
            if is_long {
                long_sym.get_or_insert(format!("{identifier}{LEG_LONG}"));
            } else {
                short_sym.get_or_insert(format!("{identifier}{LEG_SHORT}"));
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
            // Read before the struct is picked apart below.
            let volume = m.volume();
            out.push(UsMarketPair {
                slug: m.slug,
                question: m.question,
                category: m.category,
                description: m.description,
                long: MarketId::new(l),
                short: MarketId::new(s),
                close_time: parse_close_time(&m.end_date),
                volume,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real `/v1/markets` response, captured from gateway.polymarket.us on
    /// 2026-08-23 and trimmed to the fields this parser reads.
    ///
    /// The hand-written fixtures below assert a `-yes` / `-no` symbol convention
    /// the venue does not use: live symbols carry `marketSides` with ONE shared
    /// identifier and a `long` boolean, and end in a team abbreviation. Those
    /// tests passed for months while ordering could not work at all, which is
    /// why the real shape is now pinned here.
    const REAL_MARKETS_JSON: &str = include_str!("testdata/markets_response.json");

    #[test]
    fn the_live_response_yields_two_distinguishable_legs() {
        #[derive(serde::Deserialize)]
        struct Resp { markets: Vec<UsMarket> }
        let resp: Resp = serde_json::from_str(REAL_MARKETS_JSON)
            .expect("captured gateway response must still parse");
        let pairs = pair_markets(resp.markets);
        let p = pairs.first().expect("the captured market must produce a pair");

        // The venue gives both sides the same identifier; DRADIS must not.
        assert_ne!(p.long, p.short, "legs collapsed into one MarketId");
        assert_eq!(bare_symbol(p.long.as_str()), bare_symbol(p.short.as_str()),
            "both legs must still address the same venue symbol");
        assert_eq!(leg_is_long(p.long.as_str()), Some(true));
        assert_eq!(leg_is_long(p.short.as_str()), Some(false));
    }

    /// The wire form must be the identifier the venue knows — suffixes are ours.
    #[test]
    fn the_bare_symbol_is_what_the_venue_published() {
        #[derive(serde::Deserialize)]
        struct Resp { markets: Vec<UsMarket> }
        let resp: Resp = serde_json::from_str(REAL_MARKETS_JSON).expect("parse");
        let published: Vec<String> = resp.markets.iter()
            .flat_map(|m| m.market_sides.iter())
            .filter_map(|s| s.get("identifier").and_then(|v| v.as_str()).map(str::to_string))
            .collect();
        let pairs = pair_markets(resp.markets);
        let p = pairs.first().expect("pair");
        assert!(published.iter().any(|id| id == bare_symbol(p.long.as_str())),
            "bare symbol is not one the venue published");
    }

    /// A leg suffix must never be mistaken for part of the symbol.
    #[test]
    fn stripping_is_idempotent_and_safe_on_unsuffixed_symbols() {
        assert_eq!(bare_symbol("aec-nfl-lac-ten-2025-11-02#long"), "aec-nfl-lac-ten-2025-11-02");
        assert_eq!(bare_symbol("aec-nfl-lac-ten-2025-11-02"), "aec-nfl-lac-ten-2025-11-02");
        assert_eq!(bare_symbol(bare_symbol("x#short")), "x");
        assert_eq!(leg_is_long("no-suffix-here"), None);
    }

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
            title: String::new(),
            status: status.to_string(),
            category: String::new(),
            start_date: String::new(),
            end_date: String::new(),
            description: String::new(),
            active: status == "ACTIVE",
            closed: false,
            game_start_time: None,
            market_type: String::new(),
            volume_num: Some(10_000.0),
            volume_str: None,
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

    /// Parse a raw market object the way `settlement_resolution`'s fetch does.
    fn market_from_json(raw: &str) -> UsMarket {
        serde_json::from_str(raw).expect("fixture must parse")
    }

    /// A resolved market prices each leg at its side's pinned settlement value.
    ///
    /// The fixture is the live shape captured from api.polymarket.us on
    /// 2026-08-31: `MARKET_STATUS_RESOLVED` with each `marketSides` entry's
    /// `price` at `"1"` or `"0"`, and — a trap — `active` STILL `true`, which
    /// is why the status string and not the `active` flag gates this path.
    #[test]
    fn a_resolved_market_prices_both_legs_from_their_own_sides() {
        use crate::venues::core::TokenResolution as R;
        let m = market_from_json(r#"{
            "slug": "aec-nfl-atl-ne-2025-11-02",
            "status": "MARKET_STATUS_RESOLVED",
            "active": true,
            "closed": true,
            "marketSides": [
                {"identifier": "aec-nfl-atl-ne-2025-11-02", "long": true,  "price": "0"},
                {"identifier": "aec-nfl-atl-ne-2025-11-02", "long": false, "price": "1"}
            ]
        }"#);
        assert_eq!(settlement_from_market(&m, true), R::Resolved(rust_decimal::Decimal::ZERO));
        assert_eq!(settlement_from_market(&m, false), R::Resolved(rust_decimal::Decimal::ONE));
    }

    /// An OPEN market's side prices are live mids, not settlement values — a
    /// position gone from the portfolio while its market trades left by a
    /// trade, and must go to mark-priced reconciliation, not to a $1/$0 book.
    #[test]
    fn an_open_market_is_not_closed_even_when_a_side_price_looks_decisive() {
        use crate::venues::core::TokenResolution as R;
        // Live shape 2026-08-31: a long-shot whose mid is $0.0100 — decisive-
        // looking, but the market is open. Without the status gate this would
        // book a total loss on a position that was merely sold.
        let m = market_from_json(r#"{
            "slug": "cpc-btc-150k-08-31-2026",
            "status": "MARKET_STATUS_OPEN",
            "active": true,
            "closed": false,
            "marketSides": [
                {"identifier": "cpc-btc-150k-08-31-2026", "long": true, "price": "0.0100"}
            ]
        }"#);
        assert_eq!(settlement_from_market(&m, true), R::NotClosed);
        assert_eq!(settlement_from_market(&m, false), R::NotClosed);
    }

    /// A missing side price is recovered from the other side's complement —
    /// the live API has been seen dropping a side (one-element `outcomePrices`,
    /// side entries without a `price`).
    #[test]
    fn a_missing_side_price_is_recovered_from_the_complement() {
        use crate::venues::core::TokenResolution as R;
        let m = market_from_json(r#"{
            "slug": "half",
            "status": "MARKET_STATUS_RESOLVED",
            "active": false,
            "closed": true,
            "marketSides": [
                {"identifier": "half", "long": true, "price": "1"}
            ]
        }"#);
        assert_eq!(settlement_from_market(&m, false), R::Resolved(rust_decimal::Decimal::ZERO));
    }

    /// A resolved market with no decisive price answers Unknown — deferred by
    /// the caller — never a guess. This also catches any non-payout value the
    /// gateway might hand back (an index level, a stale mid): anything outside
    /// a cent of $0/$1 must not book.
    #[test]
    fn a_resolved_market_without_a_decisive_price_defers() {
        use crate::venues::core::TokenResolution as R;
        let m = market_from_json(r#"{
            "slug": "mid",
            "status": "MARKET_STATUS_RESOLVED",
            "active": false,
            "closed": true,
            "marketSides": [
                {"identifier": "mid", "long": true,  "price": "0.55"},
                {"identifier": "mid", "long": false, "price": "78787.32"}
            ]
        }"#);
        assert_eq!(settlement_from_market(&m, true), R::Unknown);
        assert_eq!(settlement_from_market(&m, false), R::Unknown);
    }

    /// Statuses this parser does not model — closed-awaiting-resolution,
    /// suspended, or a future addition — defer rather than guessing either way.
    #[test]
    fn an_unmodeled_status_defers() {
        use crate::venues::core::TokenResolution as R;
        let m = market_from_json(r#"{
            "slug": "s",
            "status": "MARKET_STATUS_SUSPENDED",
            "active": false,
            "closed": true,
            "marketSides": [
                {"identifier": "s", "long": true, "price": "1"}
            ]
        }"#);
        assert_eq!(settlement_from_market(&m, true), R::Unknown);
    }
}

