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

// Strategy modules for DRADIS
//
// Architecture (orchestrator-based):
//   - momentum_impl, arbitrage_impl, time_decay_impl, maker_impl, basis_impl (implement Strategy trait)

pub mod momentum_impl;
pub mod arbitrage_impl;
pub mod time_decay_impl;
pub mod maker_impl;
pub mod basis_impl;
pub mod gboost_impl;
pub mod trendreversal_impl;
pub mod convergence_impl;
pub mod fairvalue_impl;

use rust_decimal::Decimal;
use crate::config;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

static LAST_DRAWDOWN_REJECT_LOG: AtomicU64 = AtomicU64::new(0);

/// Shared risk utility for all strategies to check global drawdown.
pub fn is_drawdown_limit_hit(session_pnl: Decimal, starting_collateral: Decimal) -> bool {
    let max_dd = config::max_session_drawdown(starting_collateral);
    if session_pnl <= -max_dd {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        let last = LAST_DRAWDOWN_REJECT_LOG.load(Ordering::Relaxed);
        if now >= last + 60 { // Rate limit: 1 minute
            LAST_DRAWDOWN_REJECT_LOG.store(now, Ordering::Relaxed);
            info!("🛡️ Risk Reject: Global Session Drawdown ${:.2} >= Max ${:.2}", session_pnl.abs(), max_dd);
        }
        return true;
    }
    false
}

/// Shared liquidity/timing entry gate for directional vipers (2026-06-29).
///
/// Returns `Some(reason)` when a new entry should be BLOCKED because the book is
/// likely to gap straight through the stop, or `None` when entry is allowed.
///
/// Two checks:
///   1. **Near-resolution**: block when fewer than `ENTRY_MIN_SECS_TO_RESOLUTION`
///      seconds remain before the market resolves (books gap violently into close).
///   2. **Exit-side depth**: block when the resting depth on the side we would sell
///      into to exit (`exit_bid_depth`, in shares) is less than our intended
///      position size × `ENTRY_MIN_EXIT_BID_DEPTH_RATIO`. If we are larger than the
///      resting bid, a stop walks an empty book and gaps through.
///
/// `secs_left` is `None` for open-ended (no close time) markets, which skip check 1.
pub fn entry_liquidity_gate(
    secs_left: Option<i64>,
    intended_shares: Decimal,
    exit_bid_depth: Decimal,
) -> Option<String> {
    if let Some(s) = secs_left {
        if s < config::ENTRY_MIN_SECS_TO_RESOLUTION {
            return Some(format!(
                "near-resolution ({}s < {}s) — gap-through risk",
                s, config::ENTRY_MIN_SECS_TO_RESOLUTION
            ));
        }
    }
    let required = intended_shares * config::ENTRY_MIN_EXIT_BID_DEPTH_RATIO;
    if exit_bid_depth < required {
        return Some(format!(
            "thin exit book (bid_depth={:.1}sh < required {:.1}sh for {:.1}sh position) — stop would gap through",
            exit_bid_depth, required, intended_shares
        ));
    }
    None
}

// ── Venue resolution for an open position ────────────────────────────────────

/// Which of this tick's two markets actually quotes `token_id`?
///
/// Returns `None` when the token belongs to NEITHER, which is the case for any
/// position that has outlived a market rotation. Callers must skip such a
/// position, not price it against a venue that does not quote it.
///
/// Every exit path used to assume the answer instead of checking it. The shape
/// was: test the token against the maker market, and if it does not match, fall
/// through to the hourly market without testing it there either. After an hourly
/// rotation a surviving token matches nothing, so it silently inherited an
/// unrelated market's prices, close time and token identity.
///
/// That ran in production on 2026-08-30. A winning YES position on the 11PM ET
/// market was carried past the rotation onto the 12AM ET market, and FairValue
/// priced it there: `token_is_yes` compared the old token against the NEW
/// market's yes_token, came back false, so the position was read as NO and
/// valued at the new market's NO bid of $0.31. The strategy concluded the
/// position had reversed 62.65% and exited a position that was actually worth
/// $1.00 and about to redeem. The sell landed on the correct token so the money
/// was not lost, but it paid $0.06 of avoidable exit fees, and the trade was
/// written to the ledger against the wrong market, on the wrong side, with a
/// reason describing a loss that never happened.
///
/// Skipping is the honest answer. A rotated-away market is closing, so the
/// position redeems on-chain and the settlement path books it correctly. The
/// alternative, acting on prices from an unrelated market, is how the above
/// happened.
pub fn venue_for_token<'a>(
    ctx: &'a crate::orchestrator::strategy::StrategyContext,
    token_id: &crate::venues::core::MarketId,
) -> Option<(&'a crate::state::MarketConfig, &'a crate::state::MarketSnapshot)> {
    if let (Some(mk), Some(ms)) = (&ctx.maker_market, &ctx.maker_snapshot) {
        if token_id == &mk.yes_token || token_id == &mk.no_token {
            return Some((mk, ms));
        }
    }
    if token_id == &ctx.market.yes_token || token_id == &ctx.market.no_token {
        return Some((&ctx.market, &ctx.snapshot));
    }
    None
}

/// Report, at most once a minute per token, that a position has no live venue.
///
/// Skipping such a position is correct but silent, and silence is how the
/// original defect survived: the strategy appeared to be managing the position
/// while pricing it against an unrelated market. An operator watching a position
/// ride to settlement is entitled to know why nothing is acting on it.
pub fn note_position_without_venue(strategy: &str, token_id: &crate::venues::core::MarketId) {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    use std::time::Instant;

    static LAST: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
    let map = LAST.get_or_init(|| Mutex::new(HashMap::new()));
    let Ok(mut guard) = map.lock() else { return };

    let key = format!("{strategy}:{token_id}");
    let due = guard.get(&key).is_none_or(|t| t.elapsed().as_secs() >= 60);
    if !due {
        return;
    }
    guard.insert(key, Instant::now());
    // Prune so a long session cannot grow this without bound as markets rotate.
    guard.retain(|_, t| t.elapsed().as_secs() < 3600);

    info!(
        "⏸️  [{}] position on token {} has no live venue this tick — its market has rotated away. \
         Holding to settlement rather than pricing it against a different market.",
        strategy, token_id,
    );
}

#[cfg(test)]
mod venue_resolution_tests {
    use super::*;
    use crate::orchestrator::strategy::StrategyContext;
    use crate::state::{MarketConfig, MarketSnapshot, PositionMap};
    use crate::venues::core::MarketId;
    use crate::helpers::dynamic_config::DynamicConfig;
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn market(yes: &str, no: &str, name: &str) -> MarketConfig {
        MarketConfig {
            yes_token: MarketId::new(yes), no_token: MarketId::new(no),
            market_name: name.to_string(),
            market_close_time: Some(Utc::now() + chrono::Duration::hours(1)),
            strike_price: None, is_neg_risk: false,
            condition_id: "cid".to_string(), yes_fee_bps: 0, no_fee_bps: 0,
        }
    }

    fn snap(yes_bid: rust_decimal::Decimal) -> MarketSnapshot {
        MarketSnapshot {
            yes_bid, yes_bid_depth: dec!(200),
            yes_ask: dec!(0.52), yes_ask_depth: dec!(150),
            no_bid: dec!(0.48), no_bid_depth: dec!(180),
            no_ask: dec!(0.50), no_ask_depth: dec!(160),
            yes_bid_depth_total: dec!(1200), yes_ask_depth_total: dec!(900),
            no_bid_depth_total: dec!(1100), no_ask_depth_total: dec!(950),
            oracle_price: dec!(95000),
            velocity: dec!(0), velocity_1s: dec!(0), acceleration: dec!(0),
            funding_rate: dec!(0), oracle_drift_60m: dec!(0),
            oracle_drift_10m: dec!(0), hist_vol: dec!(0.003),
            institutional_pulse: dec!(0), tide_coherence: dec!(0),
            tradfi_velocity: dec!(0), macro_coherence: dec!(0),
            vix_proxy: dec!(0), vix_velocity: dec!(0),
            oi_delta_pct: dec!(0), cvd_ratio: dec!(1),
            secs_to_expiry: 3600, timestamp: Utc::now(),
        }
    }

    fn ctx(maker: Option<MarketConfig>) -> StrategyContext {
        StrategyContext {
            squadron_id: "btc-open".to_string(),
            market: market("h-yes", "h-no", "Hourly"),
            snapshot: snap(dec!(0.40)),
            positions: Arc::new(Mutex::new(PositionMap::new())),
            session_pnl: dec!(0), starting_collateral: dec!(100),
            available_collateral: dec!(100),
            crypto_filter: "btc".to_string(),
            market_started_at: Utc::now(),
            maker_snapshot: maker.as_ref().map(|_| snap(dec!(0.70))),
            maker_market: maker,
            dynamic_config: Arc::new(DynamicConfig::default()),
            arb_market_lockouts: None,
        }
    }

    /// The production incident, reduced. A token from a rotated-away market
    /// matches neither venue and must resolve to nothing — NOT silently to the
    /// hourly market, whose prices describe a different event entirely.
    #[test]
    fn a_token_from_a_rotated_away_market_resolves_to_no_venue() {
        let c = ctx(Some(market("m-yes", "m-no", "Window/Daily")));
        assert!(venue_for_token(&c, &MarketId::new("stale-token")).is_none());
    }

    /// And with no maker venue at all, which is the state during the gap between
    /// window markets — still must not adopt the hourly market by default.
    #[test]
    fn a_stale_token_resolves_to_nothing_when_there_is_no_maker_venue() {
        let c = ctx(None);
        assert!(venue_for_token(&c, &MarketId::new("stale-token")).is_none());
    }

    #[test]
    fn a_maker_token_resolves_to_the_maker_venue() {
        let c = ctx(Some(market("m-yes", "m-no", "Window/Daily")));
        for t in ["m-yes", "m-no"] {
            let (m, s) = venue_for_token(&c, &MarketId::new(t)).expect("maker token resolves");
            assert_eq!(m.market_name, "Window/Daily");
            assert_eq!(s.yes_bid, dec!(0.70), "must carry the MAKER snapshot, not the hourly one");
        }
    }

    #[test]
    fn an_hourly_token_resolves_to_the_hourly_venue() {
        let c = ctx(Some(market("m-yes", "m-no", "Window/Daily")));
        for t in ["h-yes", "h-no"] {
            let (m, s) = venue_for_token(&c, &MarketId::new(t)).expect("hourly token resolves");
            assert_eq!(m.market_name, "Hourly");
            assert_eq!(s.yes_bid, dec!(0.40));
        }
    }

    /// The mislabel that made a YES position book as NO: whichever venue is
    /// resolved must be the one that actually contains the token, so a
    /// `token == market.yes_token` test downstream answers truthfully.
    #[test]
    fn the_resolved_venue_always_contains_the_token() {
        let c = ctx(Some(market("m-yes", "m-no", "Window/Daily")));
        for t in ["m-yes", "m-no", "h-yes", "h-no"] {
            let id = MarketId::new(t);
            let (m, _) = venue_for_token(&c, &id).expect("resolves");
            assert!(
                id == m.yes_token || id == m.no_token,
                "{t} resolved to a venue that does not quote it",
            );
        }
    }
}
