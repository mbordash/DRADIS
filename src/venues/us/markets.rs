//! US retail market discovery (`GET /v1/markets`).
//!
//! Turns the gateway's market/instrument reference data into venue-neutral
//! *binary pairs* — each tradeable market that has both a `LONG` (YES) and a
//! `SHORT` (NO) instrument leg, keyed by the neutral [`MarketId`] (= symbol).
//! The arbitrage loop consumes these pairs directly; the pure
//! [`pair_markets`] reducer is unit-tested without any network.

use crate::venues::core::MarketId;

use super::types::{self, outcome as oc};

/// A tradeable binary market reduced to its two neutral leg ids.
#[derive(Debug, Clone)]
pub struct UsMarketPair {
    pub slug: String,
    pub question: String,
    /// `LONG` (YES) leg symbol.
    pub long: MarketId,
    /// `SHORT` (NO) leg symbol.
    pub short: MarketId,
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

        // Fallback: parse legacy `instruments`/`outcomes` arrays (spec structure)
        if long_sym.is_none() || short_sym.is_none() {
            let legs: Vec<_> = m.instruments.iter().chain(m.outcomes.iter()).collect();
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
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venues::us::types::{UsInstrument, UsMarket};

    fn inst(symbol: &str, outcome: &str) -> UsInstrument {
        UsInstrument { symbol: symbol.to_string(), outcome: outcome.to_string(), price_scale: 1000 }
    }
    fn market(slug: &str, status: &str, instruments: Vec<UsInstrument>) -> UsMarket {
        UsMarket {
            id: String::new(),
            slug: slug.to_string(),
            question: format!("Q {slug}?"),
            status: status.to_string(),
            category: String::new(),
            start_date: String::new(),
            end_date: String::new(),
            description: String::new(),
            instruments,
            outcomes: Vec::new(),
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

