//! Local order book maintained from the venue's `book` snapshots and
//! `price_change` deltas — B36.
//!
//! Polymarket's market channel publishes the full book on subscription and
//! after every trade, and publishes order placements and cancellations as
//! `price_change` entries. Until 2026-09-05 DRADIS consumed only the full
//! snapshots, so between trades every intl strategy priced against the book as
//! it stood after the last trade swept it. A FairValue stop marked a bid of
//! $0.58 that had long since been refilled to $0.67, fired at −22.66%, and sold
//! a position that was inside its stop and would go on to win.
//!
//! This type is the venue-neutral core of the fix: it holds plain
//! `(price, size)` levels, so it has no SDK dependency and is unit-tested
//! against the exact shapes the venue sends. `spawn_ws_task` feeds it.
//!
//! ## `size` semantics — verified, not assumed
//!
//! The venue documentation does not say whether a `price_change` entry's
//! `size` is the new total at that price or a signed delta. It was settled
//! empirically on 2026-09-05 by subscribing raw to six live tokens for seven
//! minutes and polling REST `/book` every 15s: a book built with
//! `level[price] = size` matched REST on every level at every poll, while a
//! book built with `level[price] += size` diverged from the first poll. `size`
//! is the new **absolute** level size, and `0` removes the level. That is also
//! the interpretation that fails soft: applying an absolute level twice is
//! idempotent, whereas a delta applied twice double-counts.
//!
//! ## Fail-safe rules
//!
//! A wrong book is worse than a stale one — it could mark a stop against a
//! price that never existed — so this book trusts its own arithmetic only
//! while it can prove it. Every `book` snapshot is the authority and resets the
//! map. A `price_change` is applied only when it carries a size, the size is
//! non-negative, and its venue timestamp is not older than the snapshot in
//! force. After applying a batch, the derived best bid/ask is compared with the
//! `best_bid`/`best_ask` the venue stamps on the entry; a mismatch marks the
//! book inconsistent, and an inconsistent book publishes the last full snapshot
//! (stamped with the snapshot's own receipt time, so snapshot-age checks still
//! see it as old) until the next snapshot arrives and resets it.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::state::PriceState;

/// What an empty side reads as, both on the venue's `best_bid`/`best_ask`
/// stamps and in the `PriceState` this feed has always published.
const EMPTY_BID: Decimal = dec!(0);
const EMPTY_ASK: Decimal = dec!(1);

/// Which side of the book a level change touches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide { Bid, Ask }

/// One `price_change` entry for this token, already stripped of venue types.
#[derive(Debug, Clone)]
pub struct LevelChange {
    pub side:     BookSide,
    pub price:    Decimal,
    /// New absolute size at `price`; `None` when the venue omitted it.
    pub size:     Option<Decimal>,
    /// The venue's own best bid/ask after this change, when stamped.
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
}

/// What `apply` did with a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Levels updated and the venue's best bid/ask agreed (or was absent).
    Applied,
    /// The batch predates the snapshot in force; already reflected, ignored.
    DroppedOlder,
    /// No snapshot yet, nothing to apply against; ignored.
    NoSnapshot,
    /// The book cannot be trusted until the next snapshot; the reason says why.
    /// While inconsistent, `price_state` returns the last snapshot.
    Inconsistent(String),
}

#[derive(Debug, Clone)]
struct Snapshot {
    state:       PriceState,
    venue_ts:    i64,
}

/// Feed counters, reported when a snapshot resets the book.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BookStats {
    pub applied:       u64,
    pub dropped_older: u64,
    pub no_snapshot:   u64,
    pub inconsistent:  u64,
}

#[derive(Debug, Clone, Default)]
pub struct LocalBook {
    bids:         BTreeMap<Decimal, Decimal>,
    asks:         BTreeMap<Decimal, Decimal>,
    snapshot:     Option<Snapshot>,
    inconsistent: Option<String>,
    /// Receipt time of the most recent change folded into the derived book.
    last_change:  Option<DateTime<Utc>>,
    stats:        BookStats,
}

impl LocalBook {
    pub fn new() -> Self { Self::default() }

    /// Counters since construction.
    pub fn stats(&self) -> BookStats { self.stats }

    /// Counters, zeroed — for the per-snapshot log line.
    pub fn take_stats(&mut self) -> BookStats { std::mem::take(&mut self.stats) }

    pub fn is_consistent(&self) -> bool { self.inconsistent.is_none() }

    pub fn inconsistency(&self) -> Option<&str> { self.inconsistent.as_deref() }

    pub fn has_snapshot(&self) -> bool { self.snapshot.is_some() }

    /// Replace the book with a full snapshot. Always authoritative: clears any
    /// inconsistency and every level the deltas had built.
    pub fn reset(
        &mut self,
        venue_ts: i64,
        bids: impl IntoIterator<Item = (Decimal, Decimal)>,
        asks: impl IntoIterator<Item = (Decimal, Decimal)>,
        received_at: DateTime<Utc>,
    ) {
        self.bids = bids.into_iter().filter(|(_, s)| *s > Decimal::ZERO).collect();
        self.asks = asks.into_iter().filter(|(_, s)| *s > Decimal::ZERO).collect();
        self.inconsistent = None;
        self.last_change = Some(received_at);
        self.snapshot = Some(Snapshot {
            state:    Self::derive(&self.bids, &self.asks, received_at),
            venue_ts,
        });
    }

    /// Fold one `price_change` batch (the entries for this token, in venue
    /// order) into the book. See the module docs for the rules.
    pub fn apply(
        &mut self,
        venue_ts: i64,
        changes: &[LevelChange],
        received_at: DateTime<Utc>,
    ) -> ApplyOutcome {
        let Some(snapshot) = &self.snapshot else {
            self.stats.no_snapshot += 1;
            return ApplyOutcome::NoSnapshot;
        };
        if venue_ts < snapshot.venue_ts {
            self.stats.dropped_older += 1;
            return ApplyOutcome::DroppedOlder;
        }
        if let Some(reason) = &self.inconsistent {
            // Stay down until a snapshot proves the book again; a delta applied
            // to a book already known to be wrong does not make it right.
            return ApplyOutcome::Inconsistent(reason.clone());
        }

        for c in changes {
            let Some(size) = c.size else {
                return self.mark_inconsistent(format!(
                    "price_change at {} on {:?} carried no size", c.price, c.side
                ));
            };
            if size < Decimal::ZERO {
                return self.mark_inconsistent(format!(
                    "price_change at {} on {:?} carried negative size {}", c.price, c.side, size
                ));
            }
            let levels = match c.side { BookSide::Bid => &mut self.bids, BookSide::Ask => &mut self.asks };
            if size.is_zero() { levels.remove(&c.price); } else { levels.insert(c.price, size); }
        }

        // The venue stamps its own best bid/ask on each entry. If the book we
        // derived disagrees with the venue's after the whole batch, we missed
        // or misread something, and the derived book is not to be trusted.
        // An empty side is stamped `best_bid: "0"` / `best_ask: "1"` (observed
        // 2026-09-05 on a one-sided book), which is also what this feed has
        // always published for an empty side.
        if let Some(last) = changes.last() {
            if let (Some(vb), Some(va)) = (last.best_bid, last.best_ask) {
                let db = self.best_bid().unwrap_or(EMPTY_BID);
                let da = self.best_ask().unwrap_or(EMPTY_ASK);
                if db != vb || da != va {
                    return self.mark_inconsistent(format!(
                        "derived best {db}/{da} disagrees with venue best {vb}/{va}"
                    ));
                }
            }
        }

        self.stats.applied += 1;
        self.last_change = Some(received_at);
        ApplyOutcome::Applied
    }

    /// Stop trusting the derived book for a reason `apply` cannot see — an
    /// entry whose side the SDK could not classify, say. Publishes the last
    /// snapshot until the next one arrives, like any other inconsistency.
    pub fn distrust(&mut self, reason: String) -> ApplyOutcome {
        if let Some(existing) = &self.inconsistent {
            return ApplyOutcome::Inconsistent(existing.clone());
        }
        self.mark_inconsistent(reason)
    }

    fn mark_inconsistent(&mut self, reason: String) -> ApplyOutcome {
        self.stats.inconsistent += 1;
        self.inconsistent = Some(reason.clone());
        ApplyOutcome::Inconsistent(reason)
    }

    pub fn best_bid(&self) -> Option<Decimal> { self.bids.keys().next_back().copied() }
    pub fn best_ask(&self) -> Option<Decimal> { self.asks.keys().next().copied() }

    /// The book to publish: the derived book while it is consistent, the last
    /// full snapshot (with the snapshot's receipt time) while it is not.
    /// `None` before the first snapshot.
    pub fn price_state(&self) -> Option<PriceState> {
        let snapshot = self.snapshot.as_ref()?;
        if self.inconsistent.is_some() {
            return Some(snapshot.state);
        }
        let at = self.last_change.unwrap_or(snapshot.state.4);
        Some(Self::derive(&self.bids, &self.asks, at))
    }

    /// Same shape `spawn_ws_task` has always published: best bid and its size,
    /// best ask and its size, receipt time, then depth summed over every level.
    /// An empty side reads as it always has — bid 0, ask 1, size 0.
    fn derive(
        bids: &BTreeMap<Decimal, Decimal>,
        asks: &BTreeMap<Decimal, Decimal>,
        at: DateTime<Utc>,
    ) -> PriceState {
        let (bid, bid_depth) = bids.iter().next_back()
            .map(|(p, s)| (*p, *s)).unwrap_or((EMPTY_BID, dec!(0)));
        let (ask, ask_depth) = asks.iter().next()
            .map(|(p, s)| (*p, *s)).unwrap_or((EMPTY_ASK, dec!(0)));
        let bid_total: Decimal = bids.values().sum();
        let ask_total: Decimal = asks.values().sum();
        (bid, bid_depth, ask, ask_depth, at, bid_total, ask_total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed receipt time for the snapshot, so a test can prove the fallback
    /// republishes the snapshot's own time rather than the change's.
    fn t0() -> DateTime<Utc> { DateTime::from_timestamp(1_757_000_000, 0).unwrap() }

    fn bid(price: Decimal, size: Decimal, best: Option<(Decimal, Decimal)>) -> LevelChange {
        LevelChange { side: BookSide::Bid, price, size: Some(size),
                      best_bid: best.map(|b| b.0), best_ask: best.map(|b| b.1) }
    }
    fn ask(price: Decimal, size: Decimal, best: Option<(Decimal, Decimal)>) -> LevelChange {
        LevelChange { side: BookSide::Ask, price, size: Some(size),
                      best_bid: best.map(|b| b.0), best_ask: best.map(|b| b.1) }
    }

    /// The book as the venue left it on 2026-09-05 after a sweep: bids down
    /// to $0.58 with 304 at the touch, ask at $0.83.
    fn swept_book() -> LocalBook {
        let mut b = LocalBook::new();
        b.reset(1_000, [(dec!(0.58), dec!(304)), (dec!(0.50), dec!(50))],
                       [(dec!(0.83), dec!(20)), (dec!(0.90), dec!(100))], t0());
        b
    }

    /// The incident. Bids refill behind the sweep as `price_change` entries;
    /// the published best bid must follow them instead of staying at $0.58.
    #[test]
    fn refilled_bids_move_the_mark_off_the_swept_level() {
        let mut b = swept_book();
        assert_eq!(b.price_state().unwrap().0, dec!(0.58));
        let now = Utc::now();
        assert_eq!(b.apply(1_100, &[bid(dec!(0.65), dec!(120), Some((dec!(0.65), dec!(0.83))))], now),
                   ApplyOutcome::Applied);
        assert_eq!(b.apply(1_200, &[bid(dec!(0.67), dec!(40), Some((dec!(0.67), dec!(0.83))))], now),
                   ApplyOutcome::Applied);
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.1, s.2, s.3), (dec!(0.67), dec!(40), dec!(0.83), dec!(20)));
        assert_eq!(s.5, dec!(304) + dec!(50) + dec!(120) + dec!(40), "bid depth sums every level");
        assert_eq!(s.4, now, "a derived book is stamped with the change's receipt time");
    }

    /// `size` is the new absolute level size — the empirically verified
    /// semantics. A second entry for the same price replaces, not adds.
    #[test]
    fn size_replaces_the_level_rather_than_adding_to_it() {
        let mut b = swept_book();
        let now = Utc::now();
        b.apply(1_100, &[bid(dec!(0.58), dec!(500), None)], now);
        b.apply(1_101, &[bid(dec!(0.58), dec!(2000), None)], now);
        assert_eq!(b.price_state().unwrap().1, dec!(2000));
    }

    #[test]
    fn a_zero_size_removes_the_level() {
        let mut b = swept_book();
        let now = Utc::now();
        assert_eq!(b.apply(1_100, &[bid(dec!(0.58), dec!(0), Some((dec!(0.50), dec!(0.83))))], now),
                   ApplyOutcome::Applied);
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.1), (dec!(0.50), dec!(50)));
        // Emptying the whole side reads as the feed always has: bid 0, size 0.
        b.apply(1_200, &[bid(dec!(0.50), dec!(0), None)], now);
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.1, s.5), (dec!(0), dec!(0), dec!(0)));
    }

    #[test]
    fn asks_are_keyed_from_the_low_side() {
        let mut b = swept_book();
        let now = Utc::now();
        b.apply(1_100, &[ask(dec!(0.80), dec!(7), Some((dec!(0.58), dec!(0.80))))], now);
        let s = b.price_state().unwrap();
        assert_eq!((s.2, s.3, s.6), (dec!(0.80), dec!(7), dec!(127)));
    }

    /// A snapshot is the authority: it discards every derived level and clears
    /// an inconsistency, and the counters it reports cover the interval.
    #[test]
    fn a_snapshot_resets_everything() {
        let mut b = swept_book();
        let now = Utc::now();
        b.apply(1_100, &[bid(dec!(0.67), dec!(40), None)], now);
        b.apply(1_200, &[bid(dec!(0.70), dec!(1), Some((dec!(0.99), dec!(0.83))))], now); // mismatch
        assert!(!b.is_consistent());
        b.reset(2_000, [(dec!(0.60), dec!(10))], [(dec!(0.61), dec!(10))], now);
        assert!(b.is_consistent());
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.1, s.2, s.3, s.5, s.6),
                   (dec!(0.60), dec!(10), dec!(0.61), dec!(10), dec!(10), dec!(10)));
        assert_eq!(b.take_stats(), BookStats { applied: 1, dropped_older: 0, no_snapshot: 0, inconsistent: 1 });
        assert_eq!(b.stats(), BookStats::default());
    }

    #[test]
    fn a_change_older_than_the_snapshot_is_ignored() {
        let mut b = swept_book();
        assert_eq!(b.apply(999, &[bid(dec!(0.67), dec!(40), None)], Utc::now()), ApplyOutcome::DroppedOlder);
        assert_eq!(b.price_state().unwrap().0, dec!(0.58));
        // Same millisecond as the snapshot is applied: absolute sizes make a
        // replay of the snapshot's own change idempotent.
        assert_eq!(b.apply(1_000, &[bid(dec!(0.58), dec!(304), None)], Utc::now()), ApplyOutcome::Applied);
        assert_eq!(b.price_state().unwrap().1, dec!(304));
    }

    #[test]
    fn nothing_is_applied_before_the_first_snapshot() {
        let mut b = LocalBook::new();
        assert_eq!(b.apply(1, &[bid(dec!(0.67), dec!(40), None)], Utc::now()), ApplyOutcome::NoSnapshot);
        assert!(b.price_state().is_none());
    }

    /// The fail-safe: when the derived best disagrees with the venue's stamped
    /// best, the book stops trusting itself and publishes the last snapshot —
    /// with the snapshot's own receipt time, so it still reads as old.
    #[test]
    fn a_best_price_mismatch_falls_back_to_the_snapshot() {
        let mut b = swept_book();
        let now = Utc::now();
        b.apply(1_100, &[bid(dec!(0.67), dec!(40), Some((dec!(0.67), dec!(0.83))))], now);
        assert_eq!(b.price_state().unwrap().0, dec!(0.67));
        // Venue says the best bid is 0.70; we never saw a 0.70 level.
        let out = b.apply(1_200, &[bid(dec!(0.66), dec!(5), Some((dec!(0.70), dec!(0.83))))], now);
        assert!(matches!(out, ApplyOutcome::Inconsistent(ref r) if r.contains("0.67/0.83") && r.contains("0.70/0.83")), "{out:?}");
        assert_eq!(b.inconsistency(), Some("derived best 0.67/0.83 disagrees with venue best 0.70/0.83"));
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.1, s.4), (dec!(0.58), dec!(304), t0()), "snapshot values and snapshot time");
        // Stays down until a snapshot, even for a change that would agree.
        let out = b.apply(1_300, &[bid(dec!(0.70), dec!(5), Some((dec!(0.70), dec!(0.83))))], now);
        assert!(matches!(out, ApplyOutcome::Inconsistent(_)));
        assert_eq!(b.price_state().unwrap().0, dec!(0.58));
        assert_eq!(b.stats().inconsistent, 1, "one episode, not one per rejected change");
    }

    #[test]
    fn a_missing_or_negative_size_is_never_guessed_at() {
        let mut b = swept_book();
        let now = Utc::now();
        let mut c = bid(dec!(0.67), dec!(40), None);
        c.size = None;
        assert!(matches!(b.apply(1_100, &[c], now), ApplyOutcome::Inconsistent(ref r) if r.contains("no size")));
        assert_eq!(b.price_state().unwrap().0, dec!(0.58));

        let mut b = swept_book();
        assert!(matches!(b.apply(1_100, &[bid(dec!(0.67), dec!(-1), None)], now),
                         ApplyOutcome::Inconsistent(ref r) if r.contains("negative")));
        assert_eq!(b.price_state().unwrap().0, dec!(0.58));
    }

    /// Observed 2026-09-05 on a one-sided book: the venue stamps an empty bid
    /// side `best_bid: "0"` and an empty ask side `best_ask: "1"`. Those must
    /// read as agreement, or every one-sided book (common late in an hourly
    /// market) would trip the fail-safe and pin the feed to snapshots.
    #[test]
    fn an_empty_side_agrees_with_the_venues_zero_and_one_stamps() {
        let mut b = LocalBook::new();
        b.reset(1_000, [(dec!(0.955), dec!(168)), (dec!(0.001), dec!(5))], [], t0());
        let now = Utc::now();
        // Exact entries the venue sent: the 0.955 bid pulled, then re-placed.
        assert_eq!(b.apply(1_100, &[bid(dec!(0.955), dec!(0), Some((dec!(0.001), dec!(1))))], now),
                   ApplyOutcome::Applied);
        assert_eq!(b.apply(1_200, &[bid(dec!(0.955), dec!(168), Some((dec!(0.955), dec!(1))))], now),
                   ApplyOutcome::Applied);
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.1, s.2, s.3), (dec!(0.955), dec!(168), dec!(1), dec!(0)));
        // And the mirror: no bids at all.
        let mut b = LocalBook::new();
        b.reset(1_000, [], [(dec!(0.045), dec!(168))], t0());
        assert_eq!(b.apply(1_100, &[ask(dec!(0.045), dec!(0), Some((dec!(0), dec!(0.999))))], now),
                   ApplyOutcome::Inconsistent("derived best 0/1 disagrees with venue best 0/0.999".to_string()),
                   "a level we never held is a real disagreement, not an empty-side artifact");
    }

    #[test]
    fn distrust_falls_back_like_any_inconsistency_and_does_not_stack() {
        let mut b = swept_book();
        b.apply(1_100, &[bid(dec!(0.67), dec!(40), None)], Utc::now());
        assert!(matches!(b.distrust("unknown side".into()), ApplyOutcome::Inconsistent(ref r) if r == "unknown side"));
        assert_eq!(b.price_state().unwrap().0, dec!(0.58));
        assert!(matches!(b.distrust("another".into()), ApplyOutcome::Inconsistent(ref r) if r == "unknown side"));
        assert_eq!(b.stats().inconsistent, 1);
    }

    /// The venue's best is checked after the whole batch, not per entry: a
    /// batch that moves the touch in two steps is judged on where it ends.
    #[test]
    fn the_venue_best_is_compared_after_the_whole_batch() {
        let mut b = swept_book();
        let out = b.apply(1_100, &[
            bid(dec!(0.58), dec!(0),  Some((dec!(0.58), dec!(0.83)))), // stale intermediate stamp
            bid(dec!(0.60), dec!(9),  Some((dec!(0.60), dec!(0.83)))),
        ], Utc::now());
        assert_eq!(out, ApplyOutcome::Applied);
        assert_eq!(b.price_state().unwrap().0, dec!(0.60));
    }
}
