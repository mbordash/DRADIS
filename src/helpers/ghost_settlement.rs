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

//! Simulated (ghost mode) settlements, venue-neutral.
//!
//! Ghost mode has no settlement path of its own: the two mechanisms that close
//! real positions are chain- or venue-account-based and exclude simulated rows
//! by design. These are the pieces that book a simulated position at
//! resolution and drop it from the map, shared by every venue's loop. What
//! differs per venue is only how the final price is learned: Polymarket
//! International asks Gamma (`tasks::cleanup::settle_ghost_event_positions`),
//! Kalshi reads the `result` the venue states in its market record and calls
//! [`settle_ghost_positions_at`] directly.
//!
//! Lived in `tasks::cleanup` until 1.4.2, which is compiled only for Polymarket
//! International; the Kalshi wiring needed the booking half on every venue.

use std::sync::Arc;

use rust_decimal::Decimal;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::venues::core::MarketId;

/// How a simulated settlement learned its price, which decides what the trade
/// row may claim. A resolution is a result; a tie is its own category; a mark
/// booked past the deferral bound is neither and must not read as either.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum PriceSource { Resolved, Tie, MarkFallback }


/// Book simulated positions on an event market whose venue has already
/// published the result, then drop them from the map.
///
/// The venue-neutral half of `tasks::cleanup::settle_ghost_event_positions`: that function
/// asks Gamma what Polymarket settled at, while a venue that states the result
/// in its market record (Kalshi: `result` is `yes` or `no` once determined)
/// hands the final YES price straight in. The NO leg is its complement. No
/// deferral bound applies because there is nothing to defer on: the caller
/// only has a price when the venue has decided.
///
/// Found 2026-10-19 in the 1.4.1 review and left open: a simulated position on
/// a Kalshi game had no settlement path at all, so the squadron would have
/// held it forever and the retirement added in 1.4.1 would never have fired
/// ("never over a position"). The seeder skips a class with a live squadron,
/// so one simulated game would have ended ghost trading for sports.
pub(crate) async fn settle_ghost_positions_at(
    positions: &Arc<Mutex<crate::state::PositionMap>>,
    pool: &sqlx::SqlitePool,
    asset: &str,
    market_class: Option<&str>,
    yes_market: &MarketId,
    no_market: &MarketId,
    yes_price: Decimal,
) -> usize {
    let candidates = {
        let map = positions.lock().await;
        settlement_candidates(&map, yes_market, no_market)
    };
    if candidates.is_empty() {
        return 0;
    }
    let resolved_px = resolution_prices(yes_market, no_market, yes_price);
    book_ghost_settlements(positions, pool, asset, market_class, yes_market, no_market, &resolved_px, candidates).await
}

/// Both legs' settlement prices from the final YES price: the NO leg pays the
/// complement. $0.50 is the tie category, as `price_for` reads it.
pub(crate) fn resolution_prices(
    yes_market: &MarketId,
    no_market: &MarketId,
    yes_price: Decimal,
) -> std::collections::HashMap<String, (Decimal, PriceSource)> {
    let source = if yes_price == Decimal::new(5, 1) { PriceSource::Tie } else { PriceSource::Resolved };
    let mut m = std::collections::HashMap::new();
    m.insert(yes_market.to_string(), (yes_price, source));
    m.insert(no_market.to_string(), (Decimal::ONE - yes_price, source));
    m
}

/// The simulated positions on this event's two tokens that are eligible to be
/// settled.
///
/// Only confirmed fills with size: a provisional position is one whose order has
/// not been acknowledged as filled, and booking a settlement for it would invent
/// a trade the operator never had. Split out from the lock so the filter itself
/// can be tested.
pub(crate) fn settlement_candidates(
    map: &crate::state::PositionMap,
    yes_market: &MarketId,
    no_market: &MarketId,
) -> Vec<(crate::state::PositionKey, crate::state::Position)> {
    map.iter()
        .filter(|(k, _)| k.market == *yes_market || k.market == *no_market)
        .filter(|(_, p)| p.fill_confirmed_at.is_some() && p.shares > Decimal::ZERO)
        .map(|(k, p)| (k.clone(), p.clone()))
        .collect()
}

/// What price a simulated settlement should book at, given what the venue said.
///
/// Pure, so the rule can be checked without a venue: the two HTTP calls that
/// produce `settled` and `mark` are the caller's, and every judgment about what
/// their answers mean is here.
///
/// * a settled price is a result — 0.5 is the tie or cancellation category,
///   which the pre-registration counts separately from wins and losses, and
///   anything else is a win or a loss;
/// * nothing settled and the deferral bound not yet reached defers, because a
///   game's stated close is kick-off and says nothing about the result;
/// * past the bound a mark is booked and labeled as a mark, which is the same
///   trade-off the live sweep takes: a mark is not a result, but an unbounded
///   defer is the pin this whole path exists to remove;
/// * past the bound with no mark still defers — there is nothing to book.
pub(crate) fn price_for(
    settled: Option<Decimal>,
    past_defer_bound: bool,
    mark: Option<Decimal>,
) -> Option<(Decimal, PriceSource)> {
    let half = Decimal::new(5, 1);
    match settled {
        Some(px) if px == half => Some((px, PriceSource::Tie)),
        Some(px) => Some((px, PriceSource::Resolved)),
        None if past_defer_bound => mark.map(|px| (px, PriceSource::MarkFallback)),
        None => None,
    }
}

/// The booking half of `tasks::cleanup::settle_ghost_event_positions`, with the venue's answer
/// already in hand.
///
/// Split out so it can be exercised without a venue: everything above this point
/// is two HTTP calls, and everything below is the part that moves an operator's
/// records — remove the position, book the row, close the ghost row. That is the
/// half worth a test, and until this split there was no way to run it at all.
///
/// Returns the number of settlements now on the books — rows written by this
/// pass plus rows an earlier pass already wrote — NOT the number of positions
/// looked at. A position whose write failed is put back and counted as nothing,
/// because nothing was recorded for it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn book_ghost_settlements(
    positions: &Arc<Mutex<crate::state::PositionMap>>,
    pool: &sqlx::SqlitePool,
    asset: &str,
    market_class: Option<&str>,
    yes_market: &MarketId,
    // The side is read as "YES or not": the candidates were already filtered
    // to this event's two tokens, so the NO leg needs no second comparison.
    _no_market: &MarketId,
    resolved_px: &std::collections::HashMap<String, (Decimal, PriceSource)>,
    candidates: Vec<(crate::state::PositionKey, crate::state::Position)>,
) -> usize {
    let mut settled = 0usize;
    for (key, pos) in candidates {
        let token = key.market.to_string();
        let Some(&(resolved, source)) = resolved_px.get(&token) else { continue };

        // A position in the map carries no ghost flag of its own; the only
        // gate above this point is the squadron's current ghost mode. A real
        // position held when ghost mode was switched on mid-session, or a live
        // resting order that filled after the switch, would otherwise be booked
        // as a simulated settlement and dropped from the map, freeing the
        // squadron to retire on top of a real holding. The database knows: a
        // real position has an `open_positions` row with `ghost_mode = 0`.
        // Such a token is left to the live settlement sweep. "Could not tell"
        // is not "simulated" either, so a failed read defers too.
        match crate::helpers::db::open_row_claims_token_in(pool, &token).await {
            Ok(false) => {}
            Ok(true) => {
                warn!(
                    "👻 Simulated settlement: a real open-position row claims {} ({}) — not booking it as simulated; the live sweep owns it",
                    token, key.strategy,
                );
                continue;
            }
            Err(e) => {
                warn!("👻 Simulated settlement: could not tell whether {} is real ({e}) — deferring", token);
                continue;
            }
        }

        // Remove before booking, and only if the position is still exactly as
        // it was snapshotted. A resolved market keeps a residual bid for hours,
        // so a simulated resting exit can fill against it during the probe
        // above and book its own exit; booking a settlement from the stale
        // snapshot would then record the same position twice, and the recorder
        // guards settlements against settlements, not against exits.
        {
            let mut map = positions.lock().await;
            match map.get(&key) {
                Some(cur) if cur.shares == pos.shares && cur.avg_entry == pos.avg_entry => {
                    map.remove(&key);
                }
                _ => continue,
            }
        }

        // Fee by entry type, mirroring the live settlement paths: a taker entry
        // paid the venue's quadratic schedule on the opening leg, a post-only
        // maker entry paid nothing, and the live maker path books
        // `Decimal::ZERO` with exactly that comment. Charging every strategy
        // would overstate a simulated Maker or Arbitrage settlement by a fee
        // the live books never show.
        let taker_entry = match key.strategy.as_str() {
            "FairValueStrategy" | "MomentumStrategy" | "GboostStrategy"
                | "ConvergenceStrategy" | "TrendReversalStrategy" => true,
            // Anchor buys the cheap side at the ask with a fill-and-kill order.
            crate::vipers::anchor_impl::STRATEGY_NAME => true,
            // Read from the constant rather than assumed: Basis enters as a
            // maker on every shipped profile, but the switch is compile-time,
            // and a profile that flips it should get the matching fee without
            // anyone remembering to edit this list.
            "BasisStrategy" => !crate::config::BASIS_ENTRY_AS_MAKER,
            // Helm's entry type will be the operator's choice per intent —
            // taker at the ask or a resting bid — and the intent model that
            // records it is a later increment. Until this reads the intent,
            // charge the taker fee: the conservative answer never flatters a
            // simulated Helm settlement, where booking it fee-free would.
            crate::vipers::helm_impl::STRATEGY_NAME => true,
            // Maker, Arbitrage and TimeDecay are maker-entry by construction.
            _ => false,
        };
        let entry_fee = if taker_entry {
            pos.avg_entry * pos.shares * crate::venues::entry_only_fee_pct(pos.avg_entry)
        } else {
            Decimal::ZERO
        };
        let pnl = (resolved - pos.avg_entry) * pos.shares - entry_fee;

        let side = if key.market == *yes_market { "YES" } else { "NO" };
        let outcome = match source {
            PriceSource::Tie => "tied or canceled".to_string(),
            // Not a result: say so in the row rather than dressing a mid-price
            // up as a win or a tie. A 0.93 mark booked after a day is a genuine
            // win the trial would otherwise drop from its tally, and a mark is
            // not the 0.5 category either.
            PriceSource::MarkFallback => "mark after 24h deferral".to_string(),
            PriceSource::Resolved => {
                if resolved > Decimal::new(5, 1) { "won".to_string() } else { "lost".to_string() }
            }
        };
        let reason = format!("Settlement ({outcome} — simulated)");

        // `ghost: true` so the row is marked simulated and cannot be read as
        // real P&L, and the class carried so a sports settlement is labeled as
        // one rather than left NULL.
        let mut scope = crate::state::TradeScope::new(
            asset, "", market_class.map(str::to_string), None,
        );
        scope.ghost = true;
        // Tri-state, not a bool: the recorder answers `false` both when the
        // fingerprint was already present and when the write failed, and those
        // two demand opposite handling here. A duplicate means an earlier pass
        // booked this settlement and the position is genuinely finished; a
        // failure means nothing is recorded, and retiring the position on the
        // strength of it loses the trade while reporting success.
        let written = crate::helpers::db::record_settlement_trade_checked(
            pool, &scope, &key.strategy, &pos.market_name, side,
            pos.avg_entry, resolved, pos.shares, pnl, entry_fee, &reason, None,
        ).await;
        match written {
            crate::helpers::db::SettlementWrite::Inserted => {
                info!(
                    "👻 Simulated settlement: {} {} {} | {} sh entry=${:.4} → resolved ${:.2} ({}) → pnl=${:.4}",
                    key.strategy, pos.market_name, side, pos.shares, pos.avg_entry, resolved, outcome, pnl,
                );
            }
            // Already on the books from an earlier pass. The removal above is
            // exactly the right outcome, so fall through and close the row.
            crate::helpers::db::SettlementWrite::Duplicate => {}
            crate::helpers::db::SettlementWrite::Failed => {
                error!(
                    "❌ Simulated settlement NOT recorded for {} {} — the position stays held \
                     and will be retried next pass rather than dropped with no trade row",
                    key.strategy, pos.market_name,
                );
                // Put it back, but only if the key is still free. The removal
                // above is what closes the exit/settlement double-book, and a
                // simulated resting exit could have filled and written its own
                // row while this insert was failing; re-inserting over that
                // would be the same double-book from the other side.
                let mut map = positions.lock().await;
                map.entry(key.clone()).or_insert(pos);
                continue;
            }
        }
        // If the process dies between the removal above and this close, no
        // later pass of this function recovers the row: ghost rows are never
        // rehydrated into the map (`main.rs`, the startup sweep's own comment),
        // so the token stops being a candidate. What recovers it is
        // `close_stale_ghost_positions` at startup, which drops every ghost row
        // from an earlier session — so the row survives only until the process
        // next starts, which after a crash is immediately. That sweep is load
        // bearing for this path and should not be removed on the belief that
        // the settlement pass covers it.
        crate::helpers::db::close_ghost_open_position(pool, &token).await;
        settled += 1;
    }
    settled
}

#[cfg(test)]
mod ghost_settlement_tests {
    use super::{book_ghost_settlements, price_for, resolution_prices, settle_ghost_positions_at, settlement_candidates, PriceSource};
    use crate::helpers::db;
    use crate::state::{Position, PositionKey, PositionMap};
    use crate::venues::core::MarketId;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    const YES: &str = "yes-token";
    const NO: &str = "no-token";

    fn yes_market() -> MarketId { MarketId::new(YES) }
    fn no_market() -> MarketId { MarketId::new(NO) }

    fn key(strategy: &str, token: &str) -> PositionKey {
        PositionKey {
            squadron: "sports-open".into(),
            strategy: strategy.into(),
            market: MarketId::new(token),
        }
    }

    fn position(shares: Decimal, entry: Decimal) -> Position {
        Position {
            shares,
            avg_entry: entry,
            opened_at: Utc::now(),
            close_time: None,
            market_name: "Toronto Blue Jays vs. Baltimore Orioles".into(),
            pair_token_id: no_market(),
            fill_confirmed_at: Some(Utc::now()),
            paired_leg_token_id: None,
            entry_fee: Decimal::ZERO,
        }
    }

    async fn pool() -> sqlx::SqlitePool {
        let p = SqlitePoolOptions::new().max_connections(1).connect("sqlite::memory:").await.unwrap();
        db::init_schema(&p).await.unwrap();
        // The trades table gains session_id, fees, ghost, venue, market_class
        // and underlying by migration, not by `init_schema`; without this the
        // settlement insert fails with "no column named session_id" and the
        // recorder returns false while the caller counts it settled.
        db::run_migrations(&p).await;
        p
    }

    /// A pool with `init_schema` and NOTHING else — the exact shape that made
    /// the settlement insert fail while the caller reported success.
    async fn bare_pool() -> sqlx::SqlitePool {
        let p = SqlitePoolOptions::new().max_connections(1).connect("sqlite::memory:").await.unwrap();
        db::init_schema(&p).await.unwrap();
        p
    }

    async fn open_ghost_row(p: &sqlx::SqlitePool, strategy: &str, token: &str) {
        sqlx::query(
            "INSERT INTO open_positions (ts, session_id, strategy, token_id, market, side, entry_price, shares, ghost_mode)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(Utc::now().to_rfc3339()).bind("test-session").bind(strategy).bind(token)
            .bind("Toronto Blue Jays vs. Baltimore Orioles").bind("YES").bind("0.60").bind("5")
            .execute(p).await.unwrap();
    }

    async fn ghost_rows(p: &sqlx::SqlitePool) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM open_positions WHERE ghost_mode = 1")
            .fetch_one(p).await.unwrap()
    }

    /// The whole point of the function: a simulated position on an event market
    /// is booked at the venue's resolution, dropped from the map, and its row
    /// closed — which is what finally lets the squadron retire.
    ///
    /// Before this existed, a ghost position on a sports or politics squadron
    /// stayed in the map forever and pinned the class, because an event squadron
    /// never rotates and retirement refuses while it holds anything.
    #[tokio::test]
    async fn a_resolved_position_is_booked_dropped_and_its_row_closed() {
        let p = pool().await;
        let k = key("FairValueStrategy", YES);
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(PositionMap::new()));
        map.lock().await.insert(k.clone(), position(dec!(5), dec!(0.60)));

        let mut resolved = HashMap::new();
        resolved.insert(YES.to_string(), (dec!(1), PriceSource::Resolved));

        let candidates = vec![(k.clone(), position(dec!(5), dec!(0.60)))];
        let n = book_ghost_settlements(
            &map, &p, "btc", Some("sports"), &yes_market(), &no_market(), &resolved, candidates,
        ).await;

        assert_eq!(n, 1, "one settlement booked");
        assert!(map.lock().await.get(&k).is_none(), "the position must leave the map, or the squadron stays pinned");

        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT strategy, side, reason FROM trades")
            .fetch_all(&p).await.unwrap();
        assert_eq!(rows.len(), 1, "exactly one trade row");
        assert_eq!(rows[0].0, "FairValueStrategy");
        assert_eq!(rows[0].1, "YES");
        assert_eq!(rows[0].2, "Settlement (won — simulated)");

        let ghost: i64 = sqlx::query_scalar("SELECT ghost FROM trades").fetch_one(&p).await.unwrap();
        assert_eq!(ghost, 1, "a simulated settlement must never read as real P&L");
    }

    /// A real position must never be booked as a simulated settlement. The map
    /// cannot tell the two apart, the database can: a real position has an
    /// `open_positions` row with `ghost_mode = 0`. Such a token is left to the
    /// live sweep, the position stays in the map, and nothing is written.
    #[tokio::test]
    async fn a_token_with_a_real_open_row_is_never_booked_as_simulated() {
        let p = pool().await;
        let k = key("FairValueStrategy", YES);
        sqlx::query(
            "INSERT INTO open_positions (ts, session_id, strategy, token_id, market, side, entry_price, shares, ghost_mode)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)")
            .bind(Utc::now().to_rfc3339()).bind("test-session").bind("FairValueStrategy").bind(YES)
            .bind("Toronto Blue Jays vs. Baltimore Orioles").bind("YES").bind("0.60").bind("5")
            .execute(&p).await.unwrap();
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(PositionMap::new()));
        map.lock().await.insert(k.clone(), position(dec!(5), dec!(0.60)));

        let n = settle_ghost_positions_at(&map, &p, "btc", Some("sports"), &yes_market(), &no_market(), dec!(1)).await;

        assert_eq!(n, 0, "a real position books no simulated settlement");
        assert!(map.lock().await.get(&k).is_some(), "the real position must stay in the map, so the squadron keeps holding");
        let trades: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trades").fetch_one(&p).await.unwrap();
        assert_eq!(trades, 0, "nothing written");
        let real_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM open_positions WHERE ghost_mode = 0").fetch_one(&p).await.unwrap();
        assert_eq!(real_rows, 1, "the real row is untouched");
    }

    /// A tie is its own declared category, not a loss. The pre-registration
    /// counts 0.5 settlements separately and excludes them from the win rate, so
    /// labeling one "lost" would corrupt the trial's tally.
    #[tokio::test]
    async fn a_tie_is_booked_as_its_own_outcome() {
        let p = pool().await;
        let k = key("MakerStrategy", NO);
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(PositionMap::new()));
        map.lock().await.insert(k.clone(), position(dec!(4), dec!(0.50)));

        let mut resolved = HashMap::new();
        resolved.insert(NO.to_string(), (dec!(0.5), PriceSource::Tie));

        let n = book_ghost_settlements(
            &map, &p, "btc", Some("sports"), &yes_market(), &no_market(), &resolved,
            vec![(k.clone(), position(dec!(4), dec!(0.50)))],
        ).await;

        assert_eq!(n, 1);
        let reason: String = sqlx::query_scalar("SELECT reason FROM trades").fetch_one(&p).await.unwrap();
        assert_eq!(reason, "Settlement (tied or canceled — simulated)");
        // A maker entry pays no opening fee, so a tie at the entry price is flat.
        let pnl: String = sqlx::query_scalar("SELECT pnl FROM trades").fetch_one(&p).await.unwrap();
        assert_eq!(pnl.parse::<f64>().unwrap(), 0.0, "a maker tie at entry is flat");
    }

    /// A position that changed under the probe is skipped rather than booked
    /// from a stale snapshot. A resolved market keeps a residual bid for hours,
    /// so a simulated resting exit can fill during the two HTTP calls above —
    /// and the recorder guards settlements against settlements, not against
    /// exits, so booking anyway would record the same position twice.
    #[tokio::test]
    async fn a_position_that_moved_under_the_probe_is_not_booked() {
        let p = pool().await;
        let k = key("FairValueStrategy", YES);
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(PositionMap::new()));
        // The map holds a DIFFERENT size from the snapshot handed in.
        map.lock().await.insert(k.clone(), position(dec!(2), dec!(0.60)));

        let mut resolved = HashMap::new();
        resolved.insert(YES.to_string(), (dec!(1), PriceSource::Resolved));

        let n = book_ghost_settlements(
            &map, &p, "btc", Some("sports"), &yes_market(), &no_market(), &resolved,
            vec![(k.clone(), position(dec!(5), dec!(0.60)))],
        ).await;

        assert_eq!(n, 0, "a changed position must be skipped");
        assert!(map.lock().await.get(&k).is_some(), "and left alone");
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trades").fetch_one(&p).await.unwrap();
        assert_eq!(rows, 0, "no row from a stale snapshot");
    }

    /// The bug the schema mishap exposed: when the settlement write fails, the
    /// position must stay held and its row stay open, because nothing was
    /// recorded. Before the tri-state result the caller removed the position,
    /// closed the row and counted it settled on a failed insert — losing the
    /// simulated trade the trial exists to score, and reporting success in the
    /// same breath.
    ///
    /// This is exactly the shape that failed for real: a pool without the
    /// migrations has no `session_id` column on `trades`, so the insert errors.
    #[tokio::test]
    async fn a_failed_settlement_write_keeps_the_position_and_its_row() {
        let p = bare_pool().await;
        let k = key("FairValueStrategy", YES);
        open_ghost_row(&p, "FairValueStrategy", YES).await;
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(PositionMap::new()));
        map.lock().await.insert(k.clone(), position(dec!(5), dec!(0.60)));

        let mut resolved = HashMap::new();
        resolved.insert(YES.to_string(), (dec!(1), PriceSource::Resolved));

        let n = book_ghost_settlements(
            &map, &p, "btc", Some("sports"), &yes_market(), &no_market(), &resolved,
            vec![(k.clone(), position(dec!(5), dec!(0.60)))],
        ).await;

        assert_eq!(n, 0, "a failed write must not be counted as a settlement");
        let held = map.lock().await;
        let back = held.get(&k).expect("the position must be put back, not dropped with no trade row");
        assert_eq!(back.shares, dec!(5));
        assert_eq!(back.avg_entry, dec!(0.60));
        drop(held);
        assert_eq!(ghost_rows(&p).await, 1, "and its open row must stay open for the next pass");
    }

    /// A settlement an earlier pass already booked is finished: the fingerprint
    /// insert affects no rows, and the position is dropped and counted anyway.
    /// Without this distinction "leave it on false" would pin the position
    /// forever on the very row that proves it is done.
    #[tokio::test]
    async fn an_already_booked_settlement_still_retires_the_position() {
        let p = pool().await;
        let k = key("FairValueStrategy", YES);
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(PositionMap::new()));
        let mut resolved = HashMap::new();
        resolved.insert(YES.to_string(), (dec!(1), PriceSource::Resolved));

        for pass in 0..2 {
            map.lock().await.insert(k.clone(), position(dec!(5), dec!(0.60)));
            let n = book_ghost_settlements(
                &map, &p, "btc", Some("sports"), &yes_market(), &no_market(), &resolved,
                vec![(k.clone(), position(dec!(5), dec!(0.60)))],
            ).await;
            assert_eq!(n, 1, "pass {pass} must report the settlement as handled");
            assert!(map.lock().await.get(&k).is_none(), "pass {pass} must drop the position");
        }
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trades").fetch_one(&p).await.unwrap();
        assert_eq!(rows, 1, "the fingerprint must keep the second pass from double-booking");
    }

    /// A taker entry paid the venue's opening fee, so its tie is NOT flat — the
    /// fee is the whole loss. The live rows prove this rule for FairValue, and
    /// only the maker case was covered.
    #[tokio::test]
    async fn a_taker_entry_tie_still_pays_the_opening_fee() {
        let p = pool().await;
        let k = key("FairValueStrategy", YES);
        let (shares, entry) = (dec!(4), dec!(0.60));
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(PositionMap::new()));
        map.lock().await.insert(k.clone(), position(shares, entry));

        let mut resolved = HashMap::new();
        resolved.insert(YES.to_string(), (dec!(0.5), PriceSource::Tie));

        let n = book_ghost_settlements(
            &map, &p, "btc", Some("sports"), &yes_market(), &no_market(), &resolved,
            vec![(k.clone(), position(shares, entry))],
        ).await;
        assert_eq!(n, 1);

        // Computed from the venue's own schedule rather than a literal: the
        // fee differs across the three venue features.
        let fee = entry * shares * crate::venues::entry_only_fee_pct(entry);
        let want = (dec!(0.5) - entry) * shares - fee;
        let (pnl, fees): (String, String) =
            sqlx::query_as("SELECT pnl, fees FROM trades").fetch_one(&p).await.unwrap();
        assert_eq!(pnl.parse::<f64>().unwrap(), want.to_string().parse::<f64>().unwrap());
        assert_eq!(fees.parse::<f64>().unwrap(), fee.to_string().parse::<f64>().unwrap());
        assert!(fee > Decimal::ZERO, "a taker entry must be charged something, or this test proves nothing");
    }

    /// The price rule, which is the whole judgment the two HTTP calls feed.
    #[test]
    fn price_for_covers_every_answer_the_venue_can_give() {
        // A settled price is a result.
        assert_eq!(price_for(Some(dec!(1)), false, None), Some((dec!(1), PriceSource::Resolved)));
        assert_eq!(price_for(Some(dec!(0)), false, None), Some((dec!(0), PriceSource::Resolved)));
        // 0.5 is its own category, and stays so past the bound.
        assert_eq!(price_for(Some(dec!(0.5)), false, None), Some((dec!(0.5), PriceSource::Tie)));
        assert_eq!(price_for(Some(dec!(0.5)), true, Some(dec!(0.93))),
                   Some((dec!(0.5), PriceSource::Tie)), "a settled tie beats any mark");
        // Undecided before the bound defers rather than guessing.
        assert_eq!(price_for(None, false, Some(dec!(0.93))), None,
                   "a mark must never be booked before the deferral bound");
        // Past the bound a mark is booked, and labeled as a mark.
        assert_eq!(price_for(None, true, Some(dec!(0.93))), Some((dec!(0.93), PriceSource::MarkFallback)));
        // Past the bound with nothing to book still defers.
        assert_eq!(price_for(None, true, None), None);
    }

    /// A venue that states the result hands in the YES price; the NO leg pays
    /// the complement, and a half is the tie category on both legs.
    #[test]
    fn a_stated_result_prices_both_legs() {
        let yes = MarketId::new("KXTEST#yes");
        let no = MarketId::new("KXTEST#no");
        let won = resolution_prices(&yes, &no, dec!(1));
        assert_eq!(won[&yes.to_string()], (dec!(1), PriceSource::Resolved));
        assert_eq!(won[&no.to_string()], (dec!(0), PriceSource::Resolved));
        let lost = resolution_prices(&yes, &no, dec!(0));
        assert_eq!(lost[&yes.to_string()], (dec!(0), PriceSource::Resolved));
        assert_eq!(lost[&no.to_string()], (dec!(1), PriceSource::Resolved));
        let tie = resolution_prices(&yes, &no, dec!(0.5));
        assert_eq!(tie[&yes.to_string()], (dec!(0.5), PriceSource::Tie));
        assert_eq!(tie[&no.to_string()], (dec!(0.5), PriceSource::Tie));
    }

    /// Only confirmed fills with size are settled: booking a provisional
    /// position would invent a trade the operator never had.
    #[test]
    fn only_confirmed_sized_positions_on_this_event_are_candidates() {
        let mut map = PositionMap::new();
        map.insert(key("FairValueStrategy", YES), position(dec!(5), dec!(0.60)));

        let mut provisional = position(dec!(5), dec!(0.60));
        provisional.fill_confirmed_at = None;
        map.insert(key("MakerStrategy", NO), provisional);

        map.insert(key("MomentumStrategy", NO), position(Decimal::ZERO, dec!(0.60)));
        map.insert(key("FairValueStrategy", "some-other-event-token"), position(dec!(5), dec!(0.60)));

        let got = settlement_candidates(&map, &yes_market(), &no_market());
        assert_eq!(got.len(), 1, "only the confirmed, sized position on this event");
        assert_eq!(got[0].0.strategy, "FairValueStrategy");
    }

    /// The fetch half, against the venue's real records.
    ///
    /// `#[ignore]` because it talks to Gamma; run it with
    /// `cargo test -- --ignored gamma_settlement`. It exists because the tie
    /// rule is the one place this path can be confidently wrong in a way no
    /// offline fixture would catch: an earlier version of the helper asked
    /// "closed, past endDate, both outcomes 0.50" and on a survey of closed
    /// sports markets that matched 472, of which 120 were never-resolved
    /// placeholders parked at 0.5/0.5 — every one of which it would have booked
    /// as a tie. The discriminator is `umaResolutionStatus`, and this test pins
    /// it to a real record rather than to anyone's reading of the API.
    ///
    /// Both markets are from Seahawks vs. Cardinals, 2026-09-20: the game
    /// moneyline (Seahawks won, so 1/0) and the first-half moneyline, which the
    /// half ended level and Polymarket resolved 0.5/0.5.
    #[tokio::test]
    #[ignore = "network: queries Gamma"]
    async fn gamma_settlement_prices_match_the_venues_own_records() {
        use crate::raptors::sports_ledger::settled_prices_for_market;
        let http = reqwest::Client::new();

        const DECISIVE: &str = "0xc1cd665abc12a80fe41a160f147739c03d148af9f3feafb4b4865a2a33d41c35";
        let (win, lose) = (
            "58980847861086903639275105617696204542710217219120585531915857239131341599728".to_string(),
            "44120015944269301314909412863299642492110451888750190919526243959851645377877".to_string(),
        );
        let px = settled_prices_for_market(&http, DECISIVE, &[win.clone(), lose.clone()]).await;
        assert_eq!(px.get(&win).copied(), Some(1.0), "the Seahawks leg won outright");
        assert_eq!(px.get(&lose).copied(), Some(0.0), "the Cardinals leg lost outright");
        assert_eq!(price_for(Decimal::from_f64_retain(px[&win]), false, None),
                   Some((dec!(1), PriceSource::Resolved)));

        const TIED: &str = "0xa3725b2d447032b727caa3d1f4a625bf3b108154d00f0093d0e07bbf6fb83293";
        let a = "29384481860595947055638006797469431299761828645000943811896087794813232257156".to_string();
        let px = settled_prices_for_market(&http, TIED, &[a.clone()]).await;
        assert_eq!(px.get(&a).copied(), Some(0.5), "a level first half resolves at 0.5, not as a loss");
        assert_eq!(price_for(Decimal::from_f64_retain(px[&a]), false, None),
                   Some((dec!(0.5), PriceSource::Tie)),
                   "and must book as the tie category the pre-registration counts separately");
    }

    /// A token the venue has not priced is left held, so the next pass asks
    /// again rather than guessing.
    #[tokio::test]
    async fn an_unresolved_token_is_left_alone() {
        let p = pool().await;
        let k = key("FairValueStrategy", YES);
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(PositionMap::new()));
        map.lock().await.insert(k.clone(), position(dec!(5), dec!(0.60)));

        let n = book_ghost_settlements(
            &map, &p, "btc", Some("sports"), &yes_market(), &no_market(), &HashMap::new(),
            vec![(k.clone(), position(dec!(5), dec!(0.60)))],
        ).await;

        assert_eq!(n, 0);
        assert!(map.lock().await.get(&k).is_some(), "still held, to be asked about again");
    }
}
