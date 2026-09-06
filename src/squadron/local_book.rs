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
//! idempotent, whereas a delta applied twice double-counts — and the venue does
//! send duplicate frames (observed 2026-09-05, the same cancellation twice in a
//! row).
//!
//! A later run of the same probe (2026-09-05 evening, a busy hourly token) did
//! not match REST level for level: sizes at deep levels such as 0.01/0.03/0.97
//! flickered by 1,000–2,000 shares against REST for up to a minute, on two
//! independent connections receiving byte-identical sequences, while the touch
//! agreed throughout. That is the venue's two views of the same book sampled at
//! different instants (a bot cycling deep orders every few seconds), possibly
//! with the odd cancellation the feed never announced; either way absolute
//! sizes self-heal a level the next time the venue touches it, and the next
//! trade's snapshot resets everything. No strategy decision reads deep levels —
//! order-book imbalance is a top-of-book ratio; the summed depths are
//! comparison data — so the touch, which the stamps prove, is what matters.
//!
//! ## What the venue's `best_bid`/`best_ask` stamps actually are
//!
//! Every entry carries the venue's own best bid and ask. A raw capture on
//! 2026-09-05 (16 hourly and daily tokens, one connection each, as production
//! subscribes) showed the stamps are the touch **as it will stand once the
//! venue has finished publishing the event**, not the touch after that one
//! entry. Two shapes follow from that, and both are routine:
//!
//! - **A marketable order.** The venue publishes the taker's placement as a
//!   `price_change` on the taker's side (`BUY 0.35 size 197.8`) while the
//!   resting ask at 0.35 it is about to consume has not been removed, and the
//!   stamps already show the post-match touch (`0.35/0.36`). Folding that entry
//!   in produces a **crossed** book — bid 0.35 against ask 0.35 — and the venue
//!   removes the consumed level not with a `price_change` but with the full
//!   `book` it sends after every trade, within a few milliseconds. This was
//!   every one of the eight "inconsistent" episodes reported from production
//!   on v1.1.3-rc1: derived `0.87/0.87` against venue `0.86/0.87`, always
//!   bid == ask, always with the complementary token at the same second.
//! - **A batched cancel-and-replace.** One maker pulling five bids and placing
//!   a sixth arrives as six frames sharing one venue `timestamp` and one
//!   `hash`, every frame stamped with the final touch. Only the last frame's
//!   derived book agrees with the stamps.
//!
//! Neither shape is the derived book being wrong; it is the derived book being
//! *behind* the stamps for a moment. So a disagreement puts the book on
//! **hold** rather than condemning it: while held, the last full snapshot is
//! published (exactly the pre-B36 feed), deltas keep being folded in, and the
//! hold lifts as soon as a stamped batch agrees with the derived touch again —
//! or when the next `book` resets everything. On the capture this cut the time
//! spent publishing a stale snapshot by roughly 95% against v1.1.3-rc1, whose
//! hold could only end at the next trade.
//!
//! ## Fail-safe rules
//!
//! A wrong book is worse than a stale one — it could mark a stop against a
//! price that never existed — so this book trusts its own arithmetic only
//! while it can prove it, and never publishes a derived book it cannot prove:
//!
//! - Every `book` snapshot is the authority and resets the map.
//! - A `price_change` is applied only when its venue timestamp is not older
//!   than the snapshot in force.
//! - A derived book whose best bid is at or above its best ask is **crossed**,
//!   which is never a valid book, and is held regardless of what the venue
//!   stamped — this invariant needs no stamps at all.
//! - A derived touch that disagrees with the venue's stamps is held.
//! - Those two are *soft* holds: deltas keep being applied, and a later
//!   stamped batch whose derived touch agrees releases the hold. A batch
//!   that agrees at the touch is the same proof v1.1.3-rc1 accepted for every
//!   batch; the only thing a release cannot vouch for is a level below the
//!   touch, which no stamp ever covered and the next trade's snapshot corrects.
//! - An entry that carries no size, a negative size, or a side the feed could
//!   not classify is data this book cannot fold in, so it is a *hard* hold:
//!   nothing further is applied and only the next snapshot lifts it.
//! - While held for either reason, `price_state` returns the last snapshot,
//!   stamped with the snapshot's own receipt time, so snapshot-age checks
//!   still see it as old.

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

/// Why the derived book is not being published. See the module docs for the
/// soft/hard distinction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    pub reason:  String,
    /// Receipt time of the batch that started the hold.
    pub since:   DateTime<Utc>,
    /// A hard hold is data this book could not fold in; only a snapshot
    /// lifts it. A soft hold is a touch the venue's stamps do not yet agree
    /// with; agreement lifts it.
    pub hard:    bool,
    /// Batches folded in while held (soft holds only).
    pub batches: u64,
}

impl Hold {
    pub fn held_for(&self, now: DateTime<Utc>) -> chrono::Duration { now - self.since }
}

/// What `apply` did with a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Levels updated; the book is not crossed and the venue's stamps agreed
    /// (or were absent). The derived book is published.
    Applied,
    /// This batch put the book on hold (or turned a soft hold hard). The last
    /// snapshot is published until the hold lifts.
    Held(Hold),
    /// Already on hold: the batch was folded in (soft) or ignored (hard) and
    /// the hold stands.
    StillHeld,
    /// The batch was folded in and the derived touch agrees with the venue's
    /// stamps again; the hold it carried is returned and the derived book is
    /// published from here on.
    Released(Hold),
    /// The batch predates the snapshot in force; already reflected, ignored.
    DroppedOlder,
    /// No snapshot yet, nothing to apply against; ignored.
    NoSnapshot,
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
    /// Hold episodes started.
    pub held:          u64,
    /// Hold episodes ended by a stamped batch agreeing, rather than a snapshot.
    pub released:      u64,
}

#[derive(Debug, Clone, Default)]
pub struct LocalBook {
    bids:         BTreeMap<Decimal, Decimal>,
    asks:         BTreeMap<Decimal, Decimal>,
    snapshot:     Option<Snapshot>,
    hold:         Option<Hold>,
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

    /// True when the derived book is what `price_state` publishes.
    pub fn is_consistent(&self) -> bool { self.hold.is_none() }

    pub fn hold(&self) -> Option<&Hold> { self.hold.as_ref() }

    pub fn has_snapshot(&self) -> bool { self.snapshot.is_some() }

    /// Replace the book with a full snapshot. Always authoritative: clears any
    /// hold and every level the deltas had built. Returns the hold it cleared,
    /// if there was one, so the caller can report how long it lasted.
    pub fn reset(
        &mut self,
        venue_ts: i64,
        bids: impl IntoIterator<Item = (Decimal, Decimal)>,
        asks: impl IntoIterator<Item = (Decimal, Decimal)>,
        received_at: DateTime<Utc>,
    ) -> Option<Hold> {
        self.bids = bids.into_iter().filter(|(_, s)| *s > Decimal::ZERO).collect();
        self.asks = asks.into_iter().filter(|(_, s)| *s > Decimal::ZERO).collect();
        self.last_change = Some(received_at);
        self.snapshot = Some(Snapshot {
            state:    Self::derive(&self.bids, &self.asks, received_at),
            venue_ts,
        });
        self.hold.take()
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
        if matches!(&self.hold, Some(h) if h.hard) {
            // Data has already been dropped; folding more in cannot repair
            // that, and only the next snapshot proves the book again.
            return ApplyOutcome::StillHeld;
        }

        for c in changes {
            let Some(size) = c.size else {
                return self.hold_hard(format!(
                    "price_change at {} on {:?} carried no size", c.price, c.side
                ), received_at);
            };
            if size < Decimal::ZERO {
                return self.hold_hard(format!(
                    "price_change at {} on {:?} carried negative size {}", c.price, c.side, size
                ), received_at);
            }
            let levels = match c.side { BookSide::Bid => &mut self.bids, BookSide::Ask => &mut self.asks };
            if size.is_zero() { levels.remove(&c.price); } else { levels.insert(c.price, size); }
        }
        self.stats.applied += 1;
        self.last_change = Some(received_at);
        if let Some(h) = &mut self.hold { h.batches += 1; }

        // Invariant 1: a book is never crossed. The venue publishes a taker's
        // placement before the `book` that removes what it consumed, so this
        // is the normal shape of a trade in flight — held, not trusted.
        let db = self.best_bid().unwrap_or(EMPTY_BID);
        let da = self.best_ask().unwrap_or(EMPTY_ASK);
        if self.best_bid().is_some() && self.best_ask().is_some() && db >= da {
            return self.hold_soft(format!(
                "derived book is crossed: bid {db} at or above ask {da}"
            ), received_at);
        }

        // Invariant 2: the derived touch agrees with the venue's stamps. The
        // stamps describe the touch after the whole event, which may still be
        // arriving — so disagreement is a hold, and agreement is the proof
        // that lifts it. An empty side is stamped `best_bid: "0"` /
        // `best_ask: "1"` (observed 2026-09-05 on a one-sided book), which is
        // also what this feed has always published for an empty side.
        let Some(last) = changes.last() else {
            return if self.hold.is_some() { ApplyOutcome::StillHeld } else { ApplyOutcome::Applied };
        };
        match (last.best_bid, last.best_ask) {
            (Some(vb), Some(va)) if db != vb || da != va => self.hold_soft(format!(
                "derived best {db}/{da} disagrees with venue best {vb}/{va}"
            ), received_at),
            (Some(_), Some(_)) => match self.hold.take() {
                Some(hold) => {
                    self.stats.released += 1;
                    ApplyOutcome::Released(hold)
                }
                None => ApplyOutcome::Applied,
            },
            // Unstamped: nothing to prove agreement with, so a hold stands.
            _ => if self.hold.is_some() { ApplyOutcome::StillHeld } else { ApplyOutcome::Applied },
        }
    }

    /// Stop trusting the derived book for a reason `apply` cannot see — an
    /// entry whose side the SDK could not classify, say. A hard hold: only the
    /// next snapshot lifts it.
    pub fn distrust(&mut self, reason: String, received_at: DateTime<Utc>) -> ApplyOutcome {
        if matches!(&self.hold, Some(h) if h.hard) {
            return ApplyOutcome::StillHeld;
        }
        self.hold_hard(reason, received_at)
    }

    /// Start a soft hold, or leave the existing one in force.
    fn hold_soft(&mut self, reason: String, received_at: DateTime<Utc>) -> ApplyOutcome {
        if self.hold.is_some() {
            return ApplyOutcome::StillHeld;
        }
        self.stats.held += 1;
        let hold = Hold { reason, since: received_at, hard: false, batches: 0 };
        self.hold = Some(hold.clone());
        ApplyOutcome::Held(hold)
    }

    /// Start a hard hold, or turn the existing soft hold hard. The hold keeps
    /// the earlier start time so the eventual report covers the whole episode.
    fn hold_hard(&mut self, reason: String, received_at: DateTime<Utc>) -> ApplyOutcome {
        let hold = match self.hold.take() {
            Some(soft) => Hold { reason, hard: true, ..soft },
            None => {
                self.stats.held += 1;
                Hold { reason, since: received_at, hard: true, batches: 0 }
            }
        };
        self.hold = Some(hold.clone());
        ApplyOutcome::Held(hold)
    }

    pub fn best_bid(&self) -> Option<Decimal> { self.bids.keys().next_back().copied() }
    pub fn best_ask(&self) -> Option<Decimal> { self.asks.keys().next().copied() }

    /// The book to publish: the derived book while it is proven, the last
    /// full snapshot (with the snapshot's receipt time) while it is held.
    /// `None` before the first snapshot.
    pub fn price_state(&self) -> Option<PriceState> {
        let snapshot = self.snapshot.as_ref()?;
        if self.hold.is_some() {
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

    /// A snapshot is the authority: it discards every derived level, clears a
    /// hold and hands it back, and the counters it reports cover the interval.
    #[test]
    fn a_snapshot_resets_everything() {
        let mut b = swept_book();
        let now = Utc::now();
        b.apply(1_100, &[bid(dec!(0.67), dec!(40), None)], now);
        b.apply(1_200, &[bid(dec!(0.70), dec!(1), Some((dec!(0.99), dec!(0.83))))], now); // mismatch
        assert!(!b.is_consistent());
        let cleared = b.reset(2_000, [(dec!(0.60), dec!(10))], [(dec!(0.61), dec!(10))], now);
        assert!(b.is_consistent());
        assert_eq!(cleared.map(|h| (h.since, h.hard)), Some((now, false)));
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.1, s.2, s.3, s.5, s.6),
                   (dec!(0.60), dec!(10), dec!(0.61), dec!(10), dec!(10), dec!(10)));
        assert_eq!(b.take_stats(), BookStats { applied: 2, dropped_older: 0, no_snapshot: 0, held: 1, released: 0 });
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

    /// The fail-safe: when the derived touch disagrees with the venue's
    /// stamps, the book is held and publishes the last snapshot — with the
    /// snapshot's own receipt time, so it still reads as old. The hold lifts
    /// when a stamped batch agrees again, and the derived book is back.
    #[test]
    fn a_best_price_mismatch_holds_the_snapshot_until_the_stamps_agree() {
        let mut b = swept_book();
        let now = Utc::now();
        b.apply(1_100, &[bid(dec!(0.67), dec!(40), Some((dec!(0.67), dec!(0.83))))], now);
        assert_eq!(b.price_state().unwrap().0, dec!(0.67));
        // Venue says the best bid is 0.70; we have not seen a 0.70 level yet.
        let out = b.apply(1_200, &[bid(dec!(0.66), dec!(5), Some((dec!(0.70), dec!(0.83))))], now);
        assert!(matches!(out, ApplyOutcome::Held(ref h) if h.reason == "derived best 0.67/0.83 disagrees with venue best 0.70/0.83" && !h.hard), "{out:?}");
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.1, s.4), (dec!(0.58), dec!(304), t0()), "snapshot values and snapshot time");
        // A change that still disagrees is folded in but the hold stands.
        assert_eq!(b.apply(1_250, &[bid(dec!(0.60), dec!(1), Some((dec!(0.70), dec!(0.83))))], now), ApplyOutcome::StillHeld);
        assert_eq!(b.price_state().unwrap().0, dec!(0.58));
        // The 0.70 level lands and the stamps agree: released, derived book
        // published, and the 0.66 and 0.60 folded in while held are there.
        let later = now + chrono::Duration::milliseconds(40);
        let out = b.apply(1_300, &[bid(dec!(0.70), dec!(5), Some((dec!(0.70), dec!(0.83))))], later);
        assert!(matches!(out, ApplyOutcome::Released(ref h) if h.batches == 2 && h.since == now), "{out:?}");
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.1, s.4), (dec!(0.70), dec!(5), later));
        assert_eq!(s.5, dec!(304) + dec!(50) + dec!(40) + dec!(5) + dec!(1) + dec!(5));
        assert_eq!((b.stats().held, b.stats().released), (1, 1), "one episode, not one per disagreeing change");
    }

    /// The production shape on v1.1.3-rc1, taken from the 2026-09-05 raw
    /// capture: a taker's `BUY 0.35 197.8` arrives with the resting ask at
    /// 0.35 still in the book and the stamps already showing the post-match
    /// touch `0.35/0.36`. The derived book is crossed and must not be
    /// published; the `book` the venue sends 1ms later resets it.
    #[test]
    fn a_marketable_order_crosses_the_book_until_the_snapshot_lands() {
        let mut b = LocalBook::new();
        b.reset(1_000, [(dec!(0.34), dec!(212.46)), (dec!(0.33), dec!(100))],
                       [(dec!(0.35), dec!(2.2)), (dec!(0.36), dec!(30))], t0());
        let now = Utc::now();
        let out = b.apply(1_100, &[bid(dec!(0.35), dec!(197.8), Some((dec!(0.35), dec!(0.36))))], now);
        assert!(matches!(out, ApplyOutcome::Held(ref h) if h.reason.contains("crossed") && h.reason.contains("0.35") && !h.hard), "{out:?}");
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.2, s.4), (dec!(0.34), dec!(0.35), t0()), "the snapshot, not a crossed book");
        let cleared = b.reset(1_101, [(dec!(0.35), dec!(195.6)), (dec!(0.34), dec!(212.46))],
                                     [(dec!(0.36), dec!(30))], now);
        assert!(cleared.is_some_and(|h| !h.hard));
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.1, s.2), (dec!(0.35), dec!(195.6), dec!(0.36)));
    }

    /// Also from the capture: one maker pulling five bids and placing a sixth
    /// arrives as six frames with one timestamp, each stamped with the final
    /// touch `0.46/0.52`. The first frame disagrees and holds; the last one
    /// agrees and releases, with the whole group folded in. The venue also
    /// sent one of the cancellations twice — harmless with absolute sizes.
    #[test]
    fn a_batched_cancel_stamped_with_its_final_touch_releases_when_it_lands() {
        let mut b = LocalBook::new();
        b.reset(1_000, [(dec!(0.51), dec!(106)), (dec!(0.50), dec!(61)), (dec!(0.49), dec!(6)),
                        (dec!(0.48), dec!(32)), (dec!(0.47), dec!(10))],
                       [(dec!(0.52), dec!(54.27)), (dec!(0.53), dec!(1246.95))], t0());
        let now = Utc::now();
        let stamped = Some((dec!(0.46), dec!(0.52)));
        let ts = 1_788_662_509_449;
        assert!(matches!(b.apply(ts, &[bid(dec!(0.46), dec!(61.91), stamped)], now), ApplyOutcome::Held(_)));
        for price in [dec!(0.51), dec!(0.50), dec!(0.49), dec!(0.48), dec!(0.48)] {
            assert_eq!(b.apply(ts, &[bid(price, dec!(0), stamped)], now), ApplyOutcome::StillHeld, "{price}");
            assert_eq!(b.price_state().unwrap().0, dec!(0.51), "snapshot published while held");
        }
        let out = b.apply(ts, &[bid(dec!(0.47), dec!(0), stamped)], now);
        assert!(matches!(out, ApplyOutcome::Released(ref h) if h.batches == 6), "{out:?}");
        let s = b.price_state().unwrap();
        assert_eq!((s.0, s.1, s.2, s.3, s.5), (dec!(0.46), dec!(61.91), dec!(0.52), dec!(54.27), dec!(61.91)));
    }

    /// The crossed-book invariant stands on its own: no stamps at all, and a
    /// crossed derived book is still held.
    #[test]
    fn a_crossed_book_is_held_even_without_venue_stamps() {
        let mut b = swept_book();
        let now = Utc::now();
        let out = b.apply(1_100, &[ask(dec!(0.58), dec!(9), None)], now);
        assert!(matches!(out, ApplyOutcome::Held(ref h) if h.reason == "derived book is crossed: bid 0.58 at or above ask 0.58"), "{out:?}");
        assert_eq!(b.price_state().unwrap().2, dec!(0.83));
        // Unstamped batches cannot prove anything, so the hold stands even
        // once the cross is gone...
        assert_eq!(b.apply(1_200, &[ask(dec!(0.58), dec!(0), None)], now), ApplyOutcome::StillHeld);
        // ...until a stamped batch agrees.
        assert!(matches!(b.apply(1_300, &[ask(dec!(0.83), dec!(20), Some((dec!(0.58), dec!(0.83))))], now), ApplyOutcome::Released(_)));
        assert_eq!(b.price_state().unwrap().2, dec!(0.83));
    }

    /// Data the book could not fold in is a hard hold: agreement at the touch
    /// does not lift it, only a snapshot does, and it stops applying.
    #[test]
    fn a_missing_or_negative_size_is_never_guessed_at() {
        let mut b = swept_book();
        let now = Utc::now();
        let mut c = bid(dec!(0.67), dec!(40), None);
        c.size = None;
        assert!(matches!(b.apply(1_100, &[c], now), ApplyOutcome::Held(ref h) if h.reason.contains("no size") && h.hard));
        assert_eq!(b.price_state().unwrap().0, dec!(0.58));
        assert_eq!(b.apply(1_200, &[bid(dec!(0.60), dec!(5), Some((dec!(0.58), dec!(0.83))))], now), ApplyOutcome::StillHeld,
                   "an agreeing batch does not lift a hard hold");
        assert_eq!(b.price_state().unwrap().0, dec!(0.58));
        assert!(b.reset(2_000, [(dec!(0.60), dec!(5))], [], now).is_some_and(|h| h.hard));
        assert!(b.is_consistent());

        let mut b = swept_book();
        assert!(matches!(b.apply(1_100, &[bid(dec!(0.67), dec!(-1), None)], now),
                         ApplyOutcome::Held(ref h) if h.reason.contains("negative") && h.hard));
        assert_eq!(b.price_state().unwrap().0, dec!(0.58));
    }

    /// A hard hold arriving during a soft one keeps the soft hold's start
    /// time, so the eventual report covers the whole episode.
    #[test]
    fn a_soft_hold_turning_hard_keeps_its_start_time() {
        let mut b = swept_book();
        let t1 = Utc::now();
        assert!(matches!(b.apply(1_100, &[bid(dec!(0.66), dec!(5), Some((dec!(0.70), dec!(0.83))))], t1), ApplyOutcome::Held(_)));
        let t2 = t1 + chrono::Duration::seconds(2);
        let out = b.distrust("unknown side".into(), t2);
        assert!(matches!(out, ApplyOutcome::Held(ref h) if h.hard && h.since == t1 && h.reason == "unknown side"), "{out:?}");
        assert_eq!(b.distrust("another".into(), t2), ApplyOutcome::StillHeld);
        assert_eq!(b.stats().held, 1, "one episode");
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
        let out = b.apply(1_100, &[ask(dec!(0.045), dec!(0), Some((dec!(0), dec!(0.999))))], now);
        assert!(matches!(out, ApplyOutcome::Held(ref h) if h.reason == "derived best 0/1 disagrees with venue best 0/0.999"),
                "a level we never held is a real disagreement, not an empty-side artifact: {out:?}");
    }

    #[test]
    fn distrust_holds_hard_and_does_not_stack() {
        let mut b = swept_book();
        let now = Utc::now();
        b.apply(1_100, &[bid(dec!(0.67), dec!(40), None)], now);
        assert!(matches!(b.distrust("unknown side".into(), now), ApplyOutcome::Held(ref h) if h.reason == "unknown side" && h.hard));
        assert_eq!(b.price_state().unwrap().0, dec!(0.58));
        assert_eq!(b.distrust("another".into(), now), ApplyOutcome::StillHeld);
        assert_eq!(b.hold().map(|h| h.reason.as_str()), Some("unknown side"));
        assert_eq!(b.stats().held, 1);
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
