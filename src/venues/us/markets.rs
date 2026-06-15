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
            continue;
        }
        let mut long_sym = None;
        let mut short_sym = None;
        for inst in &m.instruments {
            match inst.outcome.to_ascii_uppercase().as_str() {
                oc::LONG => long_sym.get_or_insert_with(|| inst.symbol.clone()),
                oc::SHORT => short_sym.get_or_insert_with(|| inst.symbol.clone()),
                _ => continue,
            };
        }
        if let (Some(l), Some(s)) = (long_sym, short_sym) {
            out.push(UsMarketPair {
                slug: m.market_slug,
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
            market_slug: slug.to_string(),
            question: format!("Q {slug}?"),
            status: status.to_string(),
            instruments,
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

