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

//! Venue-neutral order lifecycle manager (Option C, Slice 2).
//!
//! One reconciliation engine both venues drive — fill-confirm, stale-cancel, and
//! naked-leg flatten — replacing per-venue bespoke lifecycles. It is built purely
//! on the [`Execution`] trait surface (`positions()`, `open_orders()`, `cancel()`,
//! `place_order()`, optional `subscribe_fills()`) plus the shared [`PositionMap`],
//! so it carries **no** venue-specific machinery (no signers, HMAC, `U256`, or
//! chain polling).
//!
//! This is the convergence target from `docs/VENUE_ABSTRACTION.md` §3e:
//!   * **US** drives it today (shipped Option A logic, lifted here unchanged in
//!     behavior but venue-neutral).
//!   * **intl** migrates onto it next (Slice 3), retiring `squadron/patrol_impl`'s
//!     on-chain bespoke lifecycle.
//!
//! Fill confirmation defaults to positions-poll granularity via [`reconcile`]; when
//! a venue exposes [`Execution::subscribe_fills`], [`spawn_fill_listener`] upgrades
//! confirmation to event-precise without changing the reconcile fallback.
//!
//! [`reconcile`]: OrderLifecycle::reconcile
//! [`spawn_fill_listener`]: OrderLifecycle::spawn_fill_listener

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::state::{PositionKey, PositionMap};
use crate::venues::core::{
    Execution, Fill, FillEvent, MarketId, OpenOrder, OrderId, OrderIntent, OrderState, Side, TimeInForce,
};

/// Does a venue acknowledgment with nothing filled describe an order that now
/// RESTS on the book? Only a buy placed good-till-cancelled (or -date) that
/// came back with an order id: an immediate order that matched nothing is
/// gone, a sell that matched nothing is retried by its exit arm, and an
/// acknowledgment with no id cannot be cancelled or tracked, so it is reported
/// the old way and left to the account sweep [B74].
pub fn acknowledged_resting(side: Side, tif: TimeInForce, filled: Decimal, order_id: &OrderId) -> bool {
    matches!(side, Side::Buy)
        && matches!(tif, TimeInForce::Gtc | TimeInForce::Gtd)
        && filled <= Decimal::ZERO
        && !order_id.0.is_empty()
}

/// Describes a naked-leg that was forcibly flattened during reconcile.
/// Returned to callers so they can write a `trades` DB record — lifecycle
/// itself has no DB access and stays venue-neutral.
#[derive(Debug, Clone)]
pub struct FlattenedLeg {
    pub strategy:    String,
    pub market_name: String,
    pub shares:      Decimal,
    pub avg_entry:   Decimal,
    pub exit_price:  Decimal,
    /// On-chain token id of the flattened leg, so the caller can resolve its
    /// YES/NO market outcome for the trade record (lifecycle itself is
    /// venue-neutral and has no YES/NO mapping).
    pub token_id:    MarketId,
}

/// Tunables for the lifecycle engine. Each venue/caller supplies its own window
/// sizes so intl (slow daily/window markets) and US (fast custodial fills) can
/// share one engine without sharing timing assumptions.
#[derive(Clone, Debug)]
pub struct LifecycleConfig {
    /// A resting order unfilled for at least this long is cancelled.
    pub stale_order_secs: u64,
    /// Limit price for the FAK flatten of a naked leg (cross down to guarantee exit).
    pub flatten_sell_limit: Decimal,
    /// `is_neg_risk` flag stamped on flatten orders.
    ///
    /// US has no neg-risk concept (always `false`). intl must thread the real
    /// per-market value when it adopts the engine (Slice 3); until then no intl
    /// caller exists, so the default is safe.
    pub flatten_is_neg_risk: bool,
    /// Breakeven cushion for the naked-leg re-hedge (taker fee + slippage).
    ///
    /// A naked leg is re-hedged (missing partner bought back) instead of
    /// flattened when `partner_ask + 1 tick ≤ 1.00 − filled_avg_entry − buffer`,
    /// mirroring the intl ARB ARBITER's economics. Set from the same
    /// `ARB_FAK_REHEDGE_BUFFER` constant intl uses.
    pub rehedge_buffer: Decimal,
}

impl LifecycleConfig {
    /// Defaults matching the shipped US reconciliation constants.
    pub fn us() -> Self {
        Self {
            stale_order_secs: 60,
            flatten_sell_limit: dec!(0.01),
            flatten_is_neg_risk: false,
            rehedge_buffer: crate::config::ARB_FAK_REHEDGE_BUFFER,
        }
    }

    /// Defaults for the intl CLOB venue (Slice 3 migration).
    ///
    /// GTC maker bids can rest for much longer than US custodial orders: the
    /// fill window for window/daily markets is up to 600 s. Use 30 min as the
    /// stale-cancel threshold so slowly-filling books are not disrupted.
    /// The existing `arb_pair_fill_monitor` handles the fast path (a 5 s grace
    /// for Repair-policy pairs), so this backstop only fires on genuinely
    /// abandoned resting orders. A HoldAndManage (TimeDecay) partner bid rests
    /// to the expiry buffer by design; the hourly theta window keeps that under
    /// this threshold, and if an operator widens the window past it the stale
    /// cancel only ends the bid early — the held leg stays the viper's.
    pub fn intl() -> Self {
        Self {
            stale_order_secs: 1800, // 30 min backstop for slow resting bids
            flatten_sell_limit: dec!(0.01),
            flatten_is_neg_risk: false, // ArbitrageStrategy only trades standard binary markets
            rehedge_buffer: crate::config::ARB_FAK_REHEDGE_BUFFER,
        }
    }
}

/// A resting order we placed and must reconcile. Only `Gtc`/`Gtd` buys are
/// tracked; immediate (`Fak`/`Fok`) orders settle within their ack.
#[derive(Clone, Debug)]
struct TrackedLeg {
    id: OrderId,
    market: MarketId,
    strategy: String,
    placed_at: Instant,
    /// Contracts the order was placed for. A fill event reports a cumulative
    /// count per order, so this is what says the order is done.
    quantity: Decimal,
    /// How long this order may rest unfilled before the reconciler cancels it.
    /// A paired arbitrage leg takes the venue's `stale_order_secs`; a lone
    /// quote that is meant to rest takes [`RESTING_QUOTE_STALE_SECS`].
    stale_after_secs: u64,
    /// A resting quote (tracked unfilled through `track_resting`), as opposed
    /// to a paired leg that filled at placement. A quote is never confirmed by
    /// the leg's holdings, which may be another viper's; only by its own order.
    is_quote: bool,
    /// First reconcile pass that found this quote missing from the venue's
    /// open orders with no answer about its fill, or the moment its cancel was
    /// confirmed with no answer about its fill. See
    /// [`VANISHED_QUOTE_GRACE_SECS`].
    missing_since: Option<Instant>,
    /// The venue confirmed this order cancelled, but could not yet say what
    /// traded before the cancel. The order is no longer working: it blocks
    /// neither a new quote nor an exit of what its guard holds, and the
    /// reconciler asks the venue again until the grace runs out.
    cancelled: bool,
    /// When this order was tracked. The guard it was opened for was in the map
    /// by then; a guard opened later on the same leg belongs to a later order
    /// (a re-quote after this one was sold or released), and what the venue
    /// says about THIS order must never size, confirm or release that guard.
    guard_opened_by: chrono::DateTime<Utc>,
    /// Partner leg's market for a paired (arbitrage) entry — lets the reconciler
    /// detect a naked leg when this one fills but the partner doesn't.
    #[allow(dead_code)]
    pair_market: Option<MarketId>,
}

/// Backstop for a resting quote the venue acknowledged unfilled: a post-only
/// Maker bid, a Time Decay theta bid, a FairValue or Basis maker entry. The
/// viper that placed it owns the pull (expiry, toxic book, oracle drift), so
/// this only ends a quote the viper forgot. Thirty minutes is the intl CLOB's
/// long-standing backstop for the same orders (`LifecycleConfig::intl`).
///
/// [B74] Before 2026-10-29 the Kalshi and Polymarket US dispatchers dropped a
/// zero-fill acknowledgment on the floor: the quote rested at the venue with
/// nothing tracking it, survived rotation, and its later fill surfaced as a
/// `ChainAdopted` holding under whichever squadron synced first. Meanwhile the
/// Maker, seeing its own bid as the new best bid, quoted one tick higher every
/// tick until the account ran out of collateral.
pub const RESTING_QUOTE_STALE_SECS: u64 = 1800;

/// A tracked quote no longer among the venue's open orders, whose fill the
/// venue cannot report by id: filled (the fill feed is late), expired (a
/// good-till-date quote), or pulled by the venue (its market closed). The
/// lifecycle waits this long for the fill feed, then releases the guard: a fill
/// that never reported is adopted by the account sweep as a venue holding,
/// which loses nothing, whereas confirming the guard from the leg's holdings
/// could claim another viper's contracts and sell them twice.
pub const VANISHED_QUOTE_GRACE_SECS: u64 = 60;

/// A resting quote the lifecycle released without a fill: cancelled at
/// rotation or stand-down, pulled by the viper, or cancelled as stale. The
/// caller closes its `pending` row; the lifecycle has no database access.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleasedQuote {
    pub strategy: String,
    pub market: MarketId,
}

/// A tracked order the venue's fill feed reported filled, with the venue's
/// cumulative count and price, so the caller can size the position's row to
/// what actually traded rather than what was asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfirmedFill {
    pub strategy: String,
    pub market: MarketId,
    pub filled: Decimal,
    pub price: Decimal,
}

/// What a confirmed cancel left behind, once the venue was asked what traded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CancelOutcome {
    /// Nothing traded: the guard is gone and the quote reported for its row.
    Released,
    /// Part or all traded before the cancel: the guard is a sized position.
    Traded,
    /// The venue could not say: the order stays tracked for the reconciler.
    Unknown,
}

/// Shared, venue-neutral order lifecycle engine.
pub struct OrderLifecycle {
    cfg: LifecycleConfig,
    /// Squadron whose positions this engine reconciles. See `new`.
    squadron_id: String,
    tracked: Mutex<Vec<TrackedLeg>>,
    /// Legs whose last flatten the venue reported killed. A killed report can be
    /// wrong (Kalshi answers zero when the acknowledgment carries no count it
    /// recognizes; an intl order accepted into delayed matching reports zero),
    /// so such a leg is sold again only while the venue still shows it held.
    killed_flattens: Mutex<HashSet<String>>,
    /// Quotes released without a fill since the caller last drained them.
    released: Mutex<Vec<ReleasedQuote>>,
    /// Tracked orders the fill feed confirmed since the caller last drained them.
    confirmed: Mutex<Vec<ConfirmedFill>>,
}

impl OrderLifecycle {
    /// `squadron_id` identifies the squadron this engine reconciles for. It is
    /// part of every `PositionKey` the engine touches, so that confirming or
    /// clearing a guard reaches the position belonging to THIS squadron rather
    /// than another squadron's position on the same token.
    pub fn new(cfg: LifecycleConfig, squadron_id: impl Into<String>) -> Self {
        Self {
            cfg,
            squadron_id: squadron_id.into(),
            tracked: Mutex::new(Vec::new()),
            killed_flattens: Mutex::new(HashSet::new()),
            released: Mutex::new(Vec::new()),
            confirmed: Mutex::new(Vec::new()),
        }
    }

    /// The squadron this engine reconciles for; the loops key their row
    /// writes by it.
    pub fn squadron_id(&self) -> &str {
        &self.squadron_id
    }

    /// Does this squadron hold anything, counting a guard that is still an
    /// unfilled quote with a tracked order as nothing? The rotation loops ask
    /// before leaving a market: a resting bid is cancelled by the rotation
    /// itself and must not pin the squadron to the market forever [B74]. A
    /// paired leg (an arbitrage or Time Decay entry) is a holding even before
    /// the reconciler confirms it, as it always was.
    pub async fn holds_anything(&self, positions: &Arc<Mutex<PositionMap>>) -> bool {
        let unconfirmed: Vec<(String, MarketId)> = {
            let map = positions.lock().await;
            let mut quotes = Vec::new();
            for (k, p) in map.iter().filter(|(k, _)| k.squadron == self.squadron_id) {
                if p.fill_confirmed_at.is_some() { return true; }
                quotes.push((k.strategy.clone(), k.market.clone()));
            }
            quotes
        };
        for (strategy, market) in unconfirmed {
            let is_quote = self.tracked.lock().await.iter()
                .any(|o| o.strategy == strategy && o.market == market && o.is_quote);
            if !is_quote { return true; }
        }
        false
    }

    /// Register a freshly placed order so the reconciler manages its lifecycle.
    /// No-op for immediate (`Fak`/`Fok`) orders, which fill or kill within their ack.
    pub async fn track(
        &self,
        fill: &Fill,
        strategy: &str,
        tif: TimeInForce,
        pair_market: Option<MarketId>,
    ) {
        self.track_with_stale(fill, strategy, tif, pair_market, self.cfg.stale_order_secs, false).await;
    }

    /// Register a resting quote the venue acknowledged UNFILLED (`filled` on
    /// `fill` is the size placed, `price` the limit), so the reconciler and the
    /// fill feed own it from here: confirmed by order id when it trades,
    /// cancelled at rotation and stand-down, pulled on the viper's signal, and
    /// cancelled as stale after [`RESTING_QUOTE_STALE_SECS`] [B74].
    pub async fn track_resting(
        &self,
        fill: &Fill,
        strategy: &str,
        tif: TimeInForce,
        pair_market: Option<MarketId>,
    ) {
        self.track_with_stale(fill, strategy, tif, pair_market, RESTING_QUOTE_STALE_SECS, true).await;
    }

    async fn track_with_stale(
        &self,
        fill: &Fill,
        strategy: &str,
        tif: TimeInForce,
        pair_market: Option<MarketId>,
        stale_after_secs: u64,
        is_quote: bool,
    ) {
        if !matches!(tif, TimeInForce::Gtc | TimeInForce::Gtd) {
            return;
        }
        self.tracked.lock().await.push(TrackedLeg {
            id: fill.order_id.clone(),
            market: fill.market.clone(),
            strategy: strategy.to_string(),
            placed_at: Instant::now(),
            quantity: fill.filled,
            stale_after_secs,
            is_quote,
            missing_since: None,
            cancelled: false,
            guard_opened_by: Utc::now(),
            pair_market,
        });
    }

    /// Ids of every tracked order, for a loop that has to leave a market with
    /// orders it could not settle and must say so.
    pub async fn tracked_ids(&self) -> Vec<String> {
        self.tracked.lock().await.iter().map(|o| format!("{} ({} on {})", o.id, o.strategy, o.market)).collect()
    }

    /// Mark a tracked order cancelled at the venue with its fill still unknown,
    /// and start its grace clock.
    async fn mark_cancelled(&self, id: &OrderId) {
        for o in self.tracked.lock().await.iter_mut().filter(|o| &o.id == id) {
            o.cancelled = true;
            o.missing_since.get_or_insert_with(Instant::now);
        }
    }

    /// Does this strategy have a tracked order still WORKING on `market`: one
    /// the venue has not confirmed cancelled? The dispatchers ask before
    /// placing another quote on the leg and before selling what the guard
    /// holds; a cancelled order whose fill is still being read blocks neither.
    pub async fn working_for(&self, strategy: &str, market: &MarketId) -> bool {
        self.tracked.lock().await.iter().any(|o| o.strategy == strategy && &o.market == market && !o.cancelled)
    }

    /// Does this engine track any order at all? Rotation waits for none: an
    /// order whose cancel or fill read failed must not be forgotten with the
    /// market's lifecycle.
    pub async fn tracks_anything(&self) -> bool {
        !self.tracked.lock().await.is_empty()
    }

    /// Which strategy's tracked order this id is, if this engine tracks it. The
    /// Maker's account-wide quote pull asks before cancelling an order it finds
    /// on the leg, so another viper's live quote is left alone.
    pub async fn tracked_owner(&self, id: &OrderId) -> Option<String> {
        self.tracked.lock().await.iter().find(|o| &o.id == id).map(|o| o.strategy.clone())
    }

    /// Size a guard to what the venue says traded on its order and mark it
    /// confirmed, and queue the figure for the caller's row. Always the venue's
    /// cumulative count, never above what was placed, whether the guard was
    /// unconfirmed (a quote carrying the size placed) or already confirmed (a
    /// quote lifted in part, then in more).
    async fn size_and_confirm(&self, positions: &Arc<Mutex<PositionMap>>, ord: &TrackedLeg, traded: Decimal, price: Decimal) -> bool {
        let mut filled = if traded > Decimal::ZERO { traded.min(ord.quantity) } else { ord.quantity };
        let found = {
            let mut map = positions.lock().await;
            match map.get_mut(&PositionKey::new(&self.squadron_id, &ord.strategy, ord.market.clone())).filter(|p| owns_guard(ord, p)) {
                Some(p) => {
                    if p.fill_confirmed_at.is_none() {
                        p.shares = filled;
                        if price > Decimal::ZERO { p.avg_entry = price; }
                        p.fill_confirmed_at = Some(Utc::now());
                        info!("✅ [{}] fill confirmed: {} — {filled} @ {:.4} (order {})", ord.strategy, ord.market, p.avg_entry, ord.id);
                    } else {
                        // Two sources size the same order (the fill feed and a
                        // reconcile reading taken a moment earlier); the larger
                        // count is the later truth, and a guard never shrinks
                        // below contracts the venue already reported.
                        filled = p.shares.max(filled).min(ord.quantity);
                        p.shares = filled;
                        if price > Decimal::ZERO { p.avg_entry = price; }
                    }
                    true
                }
                None => false,
            }
        };
        // A figure for a guard that is gone (the viper exited meanwhile) would
        // land on whatever row that strategy writes on the leg next.
        if found {
            self.confirmed.lock().await.push(ConfirmedFill {
                strategy: ord.strategy.clone(), market: ord.market.clone(), filled, price,
            });
        }
        found
    }

    /// Release a quote that never filled: its unconfirmed guard leaves the map
    /// and the caller is told to close its row. A confirmed guard is a position
    /// and is left alone.
    async fn release_quote(&self, positions: &Arc<Mutex<PositionMap>>, ord: &TrackedLeg, why: &str) -> bool {
        // Check and remove under one lock: a fill event confirming the guard
        // between the two would otherwise lose a position.
        let removed = {
            let mut map = positions.lock().await;
            let key = PositionKey::new(&self.squadron_id, &ord.strategy, ord.market.clone());
            if map.get(&key).is_some_and(|p| owns_guard(ord, p) && p.fill_confirmed_at.is_none()) { map.remove(&key).is_some() } else { false }
        };
        if !removed {
            return false;
        }
        info!("🧹 [{}] quote {} on {} released — {why}", ord.strategy, ord.id, ord.market);
        self.released.lock().await.push(ReleasedQuote { strategy: ord.strategy.clone(), market: ord.market.clone() });
        true
    }

    /// After the venue REFUSED a cancel (it filled, which is the usual reason,
    /// it is gone, or the call failed): ask the order. Traded contracts confirm
    /// and size the guard; nothing traded releases it (the order was already
    /// gone); no answer leaves it tracked and working for the next attempt.
    async fn settle_refused_cancel<V: Execution + ?Sized>(&self, venue: &V, positions: &Arc<Mutex<PositionMap>>, ord: &TrackedLeg, refusal: &str) -> Option<CancelOutcome> {
        match venue.order_state(&ord.id).await {
            // Still on the book: the cancel never landed (a timeout, a rate
            // limit, a routing miss). The order works on, tracked, and the
            // next attempt cancels it.
            Ok(Some(st)) if st.resting == Some(true) => {
                warn!("[{}] cancel for {} ({}) did not go through and the order still rests: {refusal} — stays tracked and working", ord.strategy, ord.id, ord.market);
                None
            }
            Ok(Some(st)) => {
                let gone = st.resting == Some(false);
                match st.filled {
                    Some(traded) if traded > Decimal::ZERO => {
                        info!("[{}] cancel of {} ({}) refused: {traded} of {} had traded — kept as a position", ord.strategy, ord.id, ord.market, ord.quantity);
                        self.size_and_confirm(positions, ord, traded, Decimal::ZERO).await;
                        // Sizing is always safe; dropping the order is not until
                        // the venue says it no longer works.
                        gone.then_some(CancelOutcome::Traded)
                    }
                    Some(_) if gone => Some(if self.release_quote(positions, ord, "cancel refused and nothing traded (already gone)").await { CancelOutcome::Released } else { CancelOutcome::Traded }),
                    _ => {
                        // Done at the venue but with no stated count: no longer
                        // working, so it must not block a quote or an exit; the
                        // reconciler keeps asking until the grace runs out.
                        if gone { self.mark_cancelled(&ord.id).await; }
                        warn!("[{}] cancel for {} ({}) did not go through and the venue's answer settles nothing: {refusal} — order stays tracked{}", ord.strategy, ord.id, ord.market, if gone { ", no longer working" } else { "" });
                        None
                    }
                }
            }
            _ => {
                warn!("[{}] cancel for {} ({}) did not go through: {refusal} — order stays tracked", ord.strategy, ord.id, ord.market);
                None
            }
        }
    }

    /// After the venue confirmed a cancel: a cancel of a partly traded order
    /// succeeds for the remainder, so ask what traded before calling the quote
    /// unfilled. Traded contracts confirm and size the guard; nothing traded
    /// (or a venue that does not report single orders) releases it; a failed
    /// read settles nothing, and the caller keeps the order tracked for the
    /// next reconcile pass, where an unconfirmed tracked guard is inert.
    async fn settle_cancel<V: Execution + ?Sized>(&self, venue: &V, positions: &Arc<Mutex<PositionMap>>, ord: &TrackedLeg, why: &str) -> CancelOutcome {
        match venue.order_state(&ord.id).await {
            Ok(Some(OrderState { filled: Some(traded), .. })) if traded > Decimal::ZERO => {
                info!("[{}] order {} on {} had traded {traded} of {} before the cancel — kept as a position", ord.strategy, ord.id, ord.market, ord.quantity);
                self.size_and_confirm(positions, ord, traded, Decimal::ZERO).await;
                CancelOutcome::Traded
            }
            // Nothing traded, or a venue that does not report single orders.
            Ok(Some(OrderState { filled: Some(_), .. })) | Ok(None) => {
                if self.release_quote(positions, ord, why).await { CancelOutcome::Released } else { CancelOutcome::Traded }
            }
            // Reported, but without a fill count: as good as no answer.
            Ok(Some(_)) => {
                warn!("[{}] order {} was read after its cancel but the venue stated no fill count — kept tracked for the next reconcile pass", ord.strategy, ord.id);
                CancelOutcome::Unknown
            }
            Err(e) => {
                warn!("[{}] could not read order {} after its cancel: {e} — kept tracked for the next reconcile pass", ord.strategy, ord.id);
                CancelOutcome::Unknown
            }
        }
    }

    /// Does this strategy have a tracked order still resting on `market`?
    /// The dispatchers ask before placing another quote on the same leg, and
    /// before selling a position whose guard is an unfilled quote.
    pub async fn resting_for(&self, strategy: &str, market: &MarketId) -> bool {
        self.tracked.lock().await.iter().any(|o| o.strategy == strategy && &o.market == market)
    }

    /// Cancel every tracked order this strategy has resting on `market` (the
    /// viper's quote-pull, or an exit signalled on a quote that never filled).
    /// Returns how many the venue confirmed cancelled. A cancelled quote that
    /// never traded is released (guard cleared, row reported through
    /// [`take_released`](Self::take_released)); one that traded in part before
    /// the cancel is confirmed at the traded size. A cancel the venue does not
    /// confirm (it filled, it is gone, or the call failed) leaves the order
    /// tracked and its guard in place for the fill feed or the reconciler.
    pub async fn cancel_resting_for<V: Execution + ?Sized>(
        &self,
        venue: &V,
        positions: &Arc<Mutex<PositionMap>>,
        strategy: &str,
        market: &MarketId,
    ) -> usize {
        // An order already cancelled at the venue, whose fill the reconciler is
        // still reading, is not cancelled again: that costs a routing lookup
        // and a refused call every tick while the book stays toxic.
        let mine: Vec<TrackedLeg> = self.tracked.lock().await
            .iter().filter(|o| o.strategy == strategy && &o.market == market && !o.cancelled).cloned().collect();
        let mut cancelled = 0;
        for ord in mine {
            match venue.cancel(ord.id.clone()).await {
                Ok(()) => {
                    // Tracked until what traded is known, so a fill event that
                    // lands meanwhile still finds its order and sizes the guard.
                    cancelled += 1;
                    if self.settle_cancel(venue, positions, &ord, "pulled by the viper").await == CancelOutcome::Unknown {
                        self.mark_cancelled(&ord.id).await;
                    } else {
                        self.tracked.lock().await.retain(|o| o.id != ord.id);
                    }
                }
                Err(e) => {
                    if self.settle_refused_cancel(venue, positions, &ord, &e.to_string()).await.is_some() {
                        self.tracked.lock().await.retain(|o| o.id != ord.id);
                    }
                }
            }
        }
        cancelled
    }

    /// Quotes released without a fill since the last call: the caller closes
    /// each one's `pending` row.
    pub async fn take_released(&self) -> Vec<ReleasedQuote> {
        std::mem::take(&mut *self.released.lock().await)
    }

    /// Tracked orders the fill feed confirmed since the last call: the caller
    /// sizes each one's row to the venue's count and marks it confirmed.
    pub async fn take_confirmed(&self) -> Vec<ConfirmedFill> {
        std::mem::take(&mut *self.confirmed.lock().await)
    }

    /// Reconcile resting orders against venue truth: confirm fills from the
    /// positions endpoint, cancel stale unfilled orders, then flatten any naked
    /// leg whose partner neither filled nor still rests.
    ///
    /// Returns a list of legs that were forcibly flattened so callers can write
    /// `trades` DB records (lifecycle itself has no DB access).
    ///
    /// Venue-neutral replacement for the US loop's `reconcile_orders` and intl's
    /// on-chain patrol lifecycle. Uses [`Execution::positions`] as the held-truth
    /// source and [`Execution::open_orders`] (when the venue reports it) to widen
    /// "still resting" beyond locally-tracked orders.
    pub async fn reconcile<V: Execution + ?Sized>(
        &self,
        venue: &V,
        positions: &Arc<Mutex<PositionMap>>,
    ) -> Vec<FlattenedLeg> {
        // Venue truth: market → shares currently held. A failed fetch ABANDONS
        // the pass — every decision below keys off `held`, and an error read as
        // "holds nothing" makes a genuinely-filled tracked order look unfilled:
        // once past `stale_order_secs` it would be "cancelled" (the venue
        // rejects that, the order already filled) and its guard cleared,
        // dropping a real position from the in-memory map over a network blip.
        // Reconcile runs on a periodic tick, so skipping costs one cycle of
        // latency and nothing else.
        let held: HashMap<String, Decimal> = match venue.positions().await {
            Ok(p) => p
                .into_iter()
                .map(|p| (p.market.as_str().to_string(), p.shares))
                .collect(),
            Err(e) => {
                warn!("Order lifecycle reconcile: positions query failed — skipping this pass: {e}");
                return Vec::new();
            }
        };

        // Venue-reported resting orders: the markets they rest on, for the
        // naked-leg pass, and each order's filled count by id, so a tracked
        // order the venue still lists as working is never mistaken for a fill
        // because some OTHER guard holds the same leg (`positions()` is per
        // leg, not per order). A failed read is not "no orders": the quotes
        // wait for the next pass, the way the positions read above is treated.
        let (venue_open, open_known): (Vec<OpenOrder>, bool) = match venue.open_orders().await {
            Ok(v) => (v, true),
            Err(e) => {
                warn!("Order lifecycle reconcile: open-orders query failed — resting quotes wait for the next pass: {e}");
                (Vec::new(), false)
            }
        };
        let venue_resting: HashSet<String> = venue_open
            .iter()
            .filter(|o| o.is_resting() && o.remaining_qty() > Decimal::ZERO)
            .map(|o| o.market.as_str().to_string())
            .collect();
        let venue_open_filled: HashMap<String, Decimal> = venue_open
            .iter()
            .map(|o| (o.order_id.0.clone(), o.filled_qty))
            .collect();

        // Pass 1 — fill-confirm or stale-cancel each tracked order. Orders that
        // settle leave tracking by id at the end; the list itself is not
        // replaced, so an order the fill listener settled meanwhile, or one the
        // loop tracked meanwhile, is neither resurrected nor lost.
        let snapshot: Vec<TrackedLeg> = self.tracked.lock().await.clone();
        let snapshot_ids: HashSet<String> = snapshot.iter().map(|o| o.id.0.clone()).collect();
        let mut keep: Vec<TrackedLeg> = Vec::with_capacity(snapshot.len());
        for mut ord in snapshot {
            let listed = venue_open_filled.get(&ord.id.0).copied();
            let stale = ord.placed_at.elapsed().as_secs() >= ord.stale_after_secs;
            if ord.is_quote {
                // A resting quote is confirmed only by its OWN order, never by
                // the leg's holdings [B74].
                //
                // Cancelled at the venue with its fill still unknown: ask
                // again. Every answer that is not a count runs the grace clock
                // down, a failed read included, because the order is not
                // working and nothing else will ever settle it. At the end an
                // unconfirmed guard is released for the account sweep; a
                // confirmed one is kept at its size and the order forgotten.
                if ord.cancelled {
                    match venue.order_state(&ord.id).await {
                        Ok(Some(OrderState { filled: Some(traded), .. })) if traded > Decimal::ZERO => { self.size_and_confirm(positions, &ord, traded, Decimal::ZERO).await; }
                        Ok(Some(OrderState { filled: Some(_), .. })) => { self.release_quote(positions, &ord, "cancelled and nothing traded").await; }
                        other => {
                            if let Err(e) = other { warn!("[{}] could not read cancelled order {} ({}): {e}", ord.strategy, ord.id, ord.market); }
                            let since = *ord.missing_since.get_or_insert_with(Instant::now);
                            if since.elapsed().as_secs() >= VANISHED_QUOTE_GRACE_SECS {
                                if !self.release_quote(positions, &ord, "cancelled; the venue never said what traded").await {
                                    warn!("[{}] cancelled order {} ({}) never answered about its fill — its confirmed guard is kept as sized and the order forgotten", ord.strategy, ord.id, ord.market);
                                }
                            } else {
                                keep.push(ord);
                            }
                        }
                    }
                    continue;
                }
                if !open_known { keep.push(ord); continue; }
                match listed {
                    // Working, part of it traded: a position for that part; the
                    // rest keeps working until it fills, is pulled, or goes stale.
                    Some(part) if part > Decimal::ZERO => {
                        // Seen working: an earlier pass that missed it is forgotten.
                        ord.missing_since = None;
                        self.size_and_confirm(positions, &ord, part, Decimal::ZERO).await;
                        if part >= ord.quantity { continue; }
                        if stale {
                            match venue.cancel(ord.id.clone()).await {
                                Ok(()) => {
                                    info!("[{}] cancelled the stale remainder of order {} ({}) — {part} of {} had traded at the listing", ord.strategy, ord.id, ord.market, ord.quantity);
                                    // More may have traded between the listing and the cancel.
                                    if self.settle_cancel(venue, positions, &ord, "stale remainder").await != CancelOutcome::Unknown { continue; }
                                    ord.cancelled = true;
                                    ord.missing_since.get_or_insert_with(Instant::now);
                                }
                                Err(e) => warn!("[{}] stale cancel of the remainder of {} ({}) did not go through: {e} — stays tracked", ord.strategy, ord.id, ord.market),
                            }
                        }
                        keep.push(ord);
                    }
                    // Working, nothing traded.
                    Some(_) => {
                        ord.missing_since = None;
                        if stale {
                            match venue.cancel(ord.id.clone()).await {
                                Ok(()) => {
                                    info!("[{}] cancelled stale resting order {} ({})", ord.strategy, ord.id, ord.market);
                                    if self.settle_cancel(venue, positions, &ord, "stale").await != CancelOutcome::Unknown { continue; }
                                    ord.cancelled = true;
                                    ord.missing_since.get_or_insert_with(Instant::now);
                                }
                                Err(e) => warn!("[{}] stale cancel for {} ({}) did not go through: {e} — stays tracked", ord.strategy, ord.id, ord.market),
                            }
                        }
                        keep.push(ord);
                    }
                    // No longer among the open orders: filled, expired, or pulled
                    // by the venue. Ask the order itself. A venue that does not
                    // report single orders gets the fill feed's grace, then the
                    // guard is released for the account sweep; a failed read
                    // settles nothing and the order waits. Never the leg's
                    // holdings, which may be another viper's.
                    None => match venue.order_state(&ord.id).await {
                        // Working after all, though not on the listing read:
                        // treated as listed, with what the venue says traded and
                        // the same stale backstop.
                        Ok(Some(st)) if st.resting == Some(true) => {
                            ord.missing_since = None;
                            if let Some(part) = st.filled.filter(|p| *p > Decimal::ZERO) {
                                self.size_and_confirm(positions, &ord, part, Decimal::ZERO).await;
                                if part >= ord.quantity { continue; }
                            }
                            if stale {
                                match venue.cancel(ord.id.clone()).await {
                                    Ok(()) => {
                                        info!("[{}] cancelled stale resting order {} ({}) the listing had missed", ord.strategy, ord.id, ord.market);
                                        if self.settle_cancel(venue, positions, &ord, "stale").await != CancelOutcome::Unknown { continue; }
                                        ord.cancelled = true;
                                        ord.missing_since.get_or_insert_with(Instant::now);
                                    }
                                    Err(e) => warn!("[{}] stale cancel for {} ({}) did not go through: {e} — stays tracked", ord.strategy, ord.id, ord.market),
                                }
                            }
                            keep.push(ord);
                        }
                        Ok(Some(OrderState { filled: Some(traded), .. })) if traded > Decimal::ZERO => {
                            self.size_and_confirm(positions, &ord, traded, Decimal::ZERO).await;
                        }
                        Ok(Some(OrderState { filled: Some(_), resting: Some(false) })) => {
                            self.release_quote(positions, &ord, "no longer open at the venue and nothing traded (expired or pulled)").await;
                        }
                        // No usable answer (the venue does not report single
                        // orders, stated nothing, or the read failed): the
                        // order is not on the book, so nothing but this clock
                        // will ever settle it. After the grace an unconfirmed
                        // guard is released for the account sweep and a
                        // confirmed one kept at its size, the order forgotten.
                        other => {
                            if let Err(e) = other { warn!("[{}] could not read order {} ({}): {e}", ord.strategy, ord.id, ord.market); }
                            let since = *ord.missing_since.get_or_insert_with(Instant::now);
                            if since.elapsed().as_secs() >= VANISHED_QUOTE_GRACE_SECS {
                                if !self.release_quote(positions, &ord, "vanished without a reported fill").await {
                                    warn!("[{}] order {} on {} is no longer open and never answered about its fill — its confirmed guard is kept as sized and the order forgotten", ord.strategy, ord.id, ord.market);
                                } else {
                                    warn!("[{}] order {} on {} was no longer open and the venue reported no fill for {}s — guard released; a fill will be adopted by the account sweep",
                                        ord.strategy, ord.id, ord.market, VANISHED_QUOTE_GRACE_SECS);
                                }
                            } else {
                                keep.push(ord);
                            }
                        }
                    },
                }
                continue;
            }
            // A paired leg that filled at placement: the venue's holdings
            // decide, as they always have. Not the listing: the intl CLOB lists
            // every resting order with a zero fill count it does not know, and a
            // partly filled leg still on the book is held all the same.
            let filled = held.get(ord.market.as_str()).copied().unwrap_or(Decimal::ZERO) > Decimal::ZERO;
            if filled {
                confirm_guard(positions, &self.squadron_id, &ord.strategy, &ord.market).await;
                continue;
            }
            if stale {
                match venue.cancel(ord.id.clone()).await {
                    Ok(_)  => info!(" [{}] cancelled stale resting order {} ({})", ord.strategy, ord.id, ord.market),
                    Err(e) => warn!("[{}] stale cancel failed for {} ({}): {e}", ord.strategy, ord.id, ord.market),
                }
                clear_guard(positions, &self.squadron_id, &ord.strategy, &ord.market).await;
                continue;
            }
            keep.push(ord);
        }
        // "Still resting" = locally tracked AND/OR venue-reported.
        let mut resting_tokens: HashSet<String> =
            keep.iter().map(|o| o.market.as_str().to_string()).collect();
        resting_tokens.extend(venue_resting);
        {
            let kept: HashMap<String, (Option<Instant>, bool)> = keep.iter().map(|o| (o.id.0.clone(), (o.missing_since, o.cancelled))).collect();
            let mut tracked = self.tracked.lock().await;
            // Settled = in the snapshot and not kept. Tracked since the
            // snapshot = not in the snapshot: left alone.
            tracked.retain(|o| !snapshot_ids.contains(&o.id.0) || kept.contains_key(&o.id.0));
            for o in tracked.iter_mut() {
                if let Some((ms, cancelled)) = kept.get(&o.id.0) {
                    o.missing_since = *ms;
                    o.cancelled |= *cancelled;
                }
            }
        }

        // Pass 2 — naked-leg detection. A confirmed paired leg whose partner is
        // neither held nor still resting is directionally exposed. Try to
        // re-hedge (buy the missing partner) when economical — completing the
        // pair locks the arb payout — otherwise flatten the exposed leg.
        let orphans: Vec<(String, MarketId, Decimal, String, Decimal, MarketId)> = {
            let map = positions.lock().await;
            map.iter()
                .filter_map(|(k, p)| {
                    if k.squadron != self.squadron_id { return None; }
                    let (s, t) = (&k.strategy, &k.market);
                    let partner = p.paired_leg_token_id.as_ref()?;
                    // A HoldAndManage pair (TimeDecay) owns its lone leg: the viper's
                    // exit manages it and the partner bid rests for its own window. The
                    // re-hedge/flatten below is the Arbitrage repair and must not reach
                    // it — a flatten here at the 0.01 limit would be the 2026-10-01
                    // taker loss again, by another route.
                    if crate::state::lone_leg_policy(s) == crate::state::LoneLegPolicy::HoldAndManage { return None; }
                    let i_held          = held.get(t.as_str()).copied().unwrap_or_default() > Decimal::ZERO;
                    let partner_held    = held.get(partner.as_str()).copied().unwrap_or_default() > Decimal::ZERO;
                    let partner_resting = resting_tokens.contains(partner.as_str());
                    if p.fill_confirmed_at.is_some() && i_held && !partner_held && !partner_resting {
                        Some((s.clone(), t.clone(), p.shares, p.market_name.clone(), p.avg_entry, partner.clone()))
                    } else {
                        None
                    }
                })
                .collect()
        };
        // A leg whose last flatten the venue reported killed, and which the venue
        // no longer shows: the first sale was real. It is not an orphan any more
        // (the list above needs the venue to show the leg), so it would never be
        // sold again, but its guard would sit in the map as a fill-confirmed entry
        // for shares that no longer exist, blocking re-entry and open to a later
        // map-driven settlement. The guard goes with nothing booked; the
        // account-level sweep books what the venue shows. The set is read before
        // the map is locked, so the two locks never nest.
        let sold_after_kill: Vec<(String, MarketId)> = {
            let killed = self.killed_flattens.lock().await.clone();
            if killed.is_empty() {
                Vec::new()
            } else {
                let map = positions.lock().await;
                map.iter()
                    .filter(|(k, _)| k.squadron == self.squadron_id && killed.contains(k.market.as_str()))
                    .filter(|(k, _)| held.get(k.market.as_str()).copied().unwrap_or_default() <= Decimal::ZERO)
                    .map(|(k, _)| (k.strategy.clone(), k.market.clone()))
                    .collect()
            }
        };
        for (strategy, token) in sold_after_kill {
            warn!("[{strategy}] {token}: the venue no longer shows the leg after a flatten it reported killed — the sale was real; clearing the guard without a second sell");
            self.killed_flattens.lock().await.remove(token.as_str());
            clear_guard(positions, &self.squadron_id, &strategy, &token).await;
        }
        let mut flattened: Vec<FlattenedLeg> = Vec::new();
        for (strategy, token, shares, market_name, avg_entry, partner) in orphans {
            warn!("️ [{strategy}] naked leg: {token} filled but partner neither filled nor resting — attempting re-hedge");

            // Economical re-hedge first (mirrors the intl ARB ARBITER): buy the
            // missing partner at ask + 1 tick when total pair cost still clears
            // the $1.00 settlement payout by at least `rehedge_buffer`.
            if let Ok(Some(ask)) = venue.best_ask(&partner).await {
                if let Some(limit) = rehedge_limit_if_viable(ask, avg_entry, self.cfg.rehedge_buffer) {
                    let intent = OrderIntent {
                        market: partner.clone(),
                        side: Side::Buy,
                        quantity: shares,
                        price: limit,
                        tif: TimeInForce::Fak,
                        post_only: false,
                        expiration_secs: 0,
                        is_neg_risk: self.cfg.flatten_is_neg_risk,
                        fee_bps: 0,
                    };
                    match venue.place_order(intent).await {
                        Ok(f) if f.filled > Decimal::ZERO => {
                            let entry = if f.price > Decimal::ZERO { f.price } else { limit };
                            info!(
                                "🩹 [{strategy}] re-hedged naked leg: bought {} {} @ {entry:.4} \
                                 (ask={ask:.4}, breakeven_ceil={:.4}, filled_entry={avg_entry:.4})",
                                f.filled, partner, dec!(1.00) - avg_entry - self.cfg.rehedge_buffer,
                            );
                            if f.filled < shares {
                                warn!("[{strategy}] re-hedge partial: {}/{shares} — residual exposure rides to next reconcile", f.filled);
                            }
                            // Guard the new leg so the pair is tracked and the
                            // strategy can't double-enter; already venue-held,
                            // so mark it fill-confirmed.
                            positions.lock().await.insert(
                                PositionKey::new(&self.squadron_id, strategy.clone(), partner.clone()),
                                crate::state::Position {
                                    shares: f.filled,
                                    avg_entry: entry,
                                    opened_at: Utc::now(),
                                    close_time: None,
                                    market_name: market_name.clone(),
                                    pair_token_id: partner.clone(),
                                    fill_confirmed_at: Some(Utc::now()),
                                    paired_leg_token_id: Some(token.clone()),
                                    entry_fee: f.fee,
                                },
                            );
                            continue; // pair completed — no flatten
                        }
                        Ok(_)  => warn!("[{strategy}] re-hedge FAK missed for {partner} — falling back to flatten"),
                        Err(e) => warn!("[{strategy}] re-hedge failed for {partner}: {e} — falling back to flatten"),
                    }
                } else {
                    info!(
                        "[{strategy}] re-hedge uneconomical for {partner}: ask={ask:.4} + tick > {:.4} ceiling — flattening",
                        dec!(1.00) - avg_entry - self.cfg.rehedge_buffer,
                    );
                }
            }

            warn!("️ [{strategy}] flattening naked leg {token} ({shares} shares)");
            let intent = OrderIntent {
                market: token.clone(),
                side: Side::Sell,
                quantity: shares,
                price: self.cfg.flatten_sell_limit,
                tif: TimeInForce::Fak,
                post_only: false,
                expiration_secs: 0,
                is_neg_risk: self.cfg.flatten_is_neg_risk,
                fee_bps: 0,
            };
            match venue.place_order(intent).await {
                Ok(f) => {
                    // Record the ACTUAL fill price/qty from the venue, not the
                    // flatten *limit*. Booking the 0.01 limit overstated losses:
                    // e.g. Jun 19 trade id 50 booked −$2.94 at exit 0.01 when the
                    // FAK sell actually crossed near the prevailing bid. Every
                    // venue reports an unfilled immediate order as zero filled
                    // ([B73]), so a killed sell books nothing: the leg is still
                    // held, stays in the map, and is tried again next reconcile.
                    let Some((exit_shares, exit_price)) = flatten_booking(f.filled, f.price, self.cfg.flatten_sell_limit) else {
                        warn!("[{strategy}] flatten of {token} killed unfilled (order {}) — {shares} shares still held, will retry next reconcile while the venue shows them", f.order_id);
                        self.killed_flattens.lock().await.insert(token.as_str().to_string());
                        continue;
                    };
                    self.killed_flattens.lock().await.remove(token.as_str());
                    info!("️ [{strategy}] flattened naked leg {token} (order {}) — {exit_shares} @ {exit_price:.4}", f.order_id);
                    flattened.push(FlattenedLeg {
                        strategy: strategy.clone(),
                        market_name,
                        shares: exit_shares,
                        avg_entry,
                        exit_price,
                        token_id: token.clone(),
                    });
                }
                Err(e) => warn!("[{strategy}] flatten of {token} failed: {e} — will retry next reconcile"),
            }
            // Clear the guard so we don't re-flatten before the sell settles.
            clear_guard(positions, &self.squadron_id, &strategy, &token).await;
        }
        flattened
    }

    /// Cancel every tracked resting order (squadron stand-down / market rotation),
    /// so no order is left working on a closing market.
    ///
    /// A quote the venue confirms cancelled while its guard is still unconfirmed
    /// never filled: the guard leaves the map here, and the quote is returned
    /// (and queued for [`take_released`](Self::take_released)) so the caller can
    /// close its `pending` row. A confirmed guard is a position and stays. A
    /// cancel the venue refuses means the order filled or is already gone; its
    /// guard stays too, for the fill feed and the account sweep to settle [B74].
    pub async fn cancel_all<V: Execution + ?Sized>(
        &self,
        venue: &V,
        positions: &Arc<Mutex<PositionMap>>,
    ) -> Vec<ReleasedQuote> {
        // Orders stay tracked while the venue is asked about them, so a fill
        // event landing meanwhile still finds its order; settled ones leave
        // tracking by id at the end.
        let orders: Vec<TrackedLeg> = self.tracked.lock().await.clone();
        let mut released = Vec::new();
        let mut settled: HashSet<String> = HashSet::new();
        for ord in orders {
            match venue.cancel(ord.id.clone()).await {
                Ok(()) => match self.settle_cancel(venue, positions, &ord, "cancelled at rotation or stand-down").await {
                    CancelOutcome::Released => {
                        released.push(ReleasedQuote { strategy: ord.strategy.clone(), market: ord.market.clone() });
                        settled.insert(ord.id.0.clone());
                    }
                    CancelOutcome::Traded => { settled.insert(ord.id.0.clone()); }
                    CancelOutcome::Unknown => { self.mark_cancelled(&ord.id).await; }
                },
                // Not confirmed cancelled: it filled (the usual reason a venue
                // refuses), it is gone, or the call failed. Ask the order; when
                // it cannot say, the order stays tracked and its guard stays,
                // inert, and the caller must not leave the market while any
                // order is tracked.
                Err(e) => match self.settle_refused_cancel(venue, positions, &ord, &e.to_string()).await {
                    Some(CancelOutcome::Released) => {
                        released.push(ReleasedQuote { strategy: ord.strategy.clone(), market: ord.market.clone() });
                        settled.insert(ord.id.0.clone());
                    }
                    Some(_) => { settled.insert(ord.id.0.clone()); }
                    None => {}
                },
            }
        }
        self.tracked.lock().await.retain(|o| !settled.contains(&o.id.0));
        released
    }

    /// If the venue exposes a fill-event feed, spawn a listener that confirms
    /// position guards **event-precisely** (no poll lag). Complements — does not
    /// replace — [`reconcile`](Self::reconcile), which remains the cancel/flatten
    /// path and the fallback for venues without a feed.
    ///
    /// Returns `None` when the venue has no feed (poll-only).
    pub fn spawn_fill_listener<V>(
        self: &Arc<Self>,
        venue: Arc<V>,
        positions: Arc<Mutex<PositionMap>>,
    ) -> Option<JoinHandle<()>>
    where
        V: Execution + Send + Sync + 'static,
    {
        let mut rx = venue.subscribe_fills()?;
        let lifecycle = Arc::clone(self);
        Some(tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        // Confirm the guard the order belongs to, or, for an
                        // order this engine did not track, whichever strategy
                        // holds this leg.
                        lifecycle.confirm_on_fill(&positions, &ev).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("OrderLifecycle fill listener lagged {n} events — reconcile will recover");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }))
    }

    /// Confirm the guard a fill event belongs to. Used by the event-driven fill
    /// listener.
    ///
    /// An event for a TRACKED order confirms that order's own guard and nothing
    /// else: two vipers can rest on the same leg, and the one whose quote was
    /// lifted is the one that now holds it. A guard that was an unfilled quote
    /// is sized to the venue's cumulative count (never above what was placed)
    /// at the venue's price, and the order leaves tracking once the count
    /// reaches what was placed. The caller reads the result from
    /// [`take_confirmed`](Self::take_confirmed) and writes it to the row.
    ///
    /// An event for an order this engine did not track (an immediate order,
    /// which is never tracked, or one placed before a restart) confirms every
    /// unconfirmed guard on the leg, as before.
    async fn confirm_on_fill(&self, positions: &Arc<Mutex<PositionMap>>, ev: &FillEvent) {
        let tracked = self.tracked.lock().await.iter().find(|o| o.id == ev.order_id).cloned();
        let Some(ord) = tracked else {
            // A sale confirms no buy guard. And a leg with a tracked quote on
            // it belongs to that quote's order: someone else's fill on the leg
            // (another viper's taker entry or exit, an operator's Close Now)
            // says nothing about whether the quote traded.
            if matches!(ev.side, Side::Sell) {
                return;
            }
            let quoted: HashSet<(String, String)> = self.tracked.lock().await.iter()
                .filter(|o| o.market == ev.market)
                .map(|o| (o.strategy.clone(), o.market.as_str().to_string()))
                .collect();
            let mut map = positions.lock().await;
            for (k, p) in map.iter_mut() {
                if k.squadron == self.squadron_id && k.market == ev.market && p.fill_confirmed_at.is_none()
                    && !quoted.contains(&(k.strategy.clone(), k.market.as_str().to_string()))
                {
                    p.fill_confirmed_at = Some(Utc::now());
                }
            }
            return;
        };
        self.size_and_confirm(positions, &ord, ev.filled, ev.price).await;
        let filled = if ev.filled > Decimal::ZERO { ev.filled.min(ord.quantity) } else { ord.quantity };
        if filled >= ord.quantity || ev.complete {
            self.tracked.lock().await.retain(|o| o.id != ord.id);
        }
    }
}

/// Mark a strategy's position guard fill-confirmed (idempotent).
async fn confirm_guard(positions: &Arc<Mutex<PositionMap>>, squadron: &str, strategy: &str, token: &MarketId) {
    if let Some(p) = positions.lock().await.get_mut(&PositionKey::new(squadron, strategy, token.clone())) {
        if p.fill_confirmed_at.is_none() {
            p.fill_confirmed_at = Some(Utc::now());
            info!("✅ [{strategy}] fill confirmed: {token}");
        }
    }
}

/// Is this guard the one the tracked order was opened for? A guard opened
/// after the order was tracked belongs to a later order on the same leg.
fn owns_guard(ord: &TrackedLeg, p: &crate::state::Position) -> bool {
    p.opened_at <= ord.guard_opened_by
}

/// Drop a strategy's position guard for a token (so the viper may re-enter).
async fn clear_guard(positions: &Arc<Mutex<PositionMap>>, squadron: &str, strategy: &str, token: &MarketId) {
    positions.lock().await.remove(&PositionKey::new(squadron, strategy, token.clone()));
}

/// What a flatten order's acknowledgment books: `(shares, price)`, or `None`
/// when the venue reports nothing filled.
///
/// Before B73 the intl and US venues echoed the requested size for an unfilled
/// immediate order, so this path could not tell a kill from a fill and booked
/// the intended shares either way; a killed flatten then closed the row while
/// the leg stayed at the venue. Every venue now reports the honest count, so
/// zero means the leg is still held and nothing is booked. A fill with no
/// reported price books at the limit sent, as before.
fn flatten_booking(filled: Decimal, price: Decimal, limit_sent: Decimal) -> Option<(Decimal, Decimal)> {
    if filled <= Decimal::ZERO {
        return None;
    }
    Some((filled, if price > Decimal::ZERO { price } else { limit_sent }))
}

/// Naked-leg re-hedge economics (mirrors intl's ARB ARBITER, `balance.rs`).
///
/// Buying the missing partner at `ask + 1 tick` completes the pair, which pays
/// $1.00 at settlement. Viable iff
/// `ask + 0.01 ≤ 1.00 − filled_avg_entry − buffer`; returns the FAK limit price
/// when viable, `None` when flattening is the better exit.
fn rehedge_limit_if_viable(ask: Decimal, filled_avg_entry: Decimal, buffer: Decimal) -> Option<Decimal> {
    if ask <= Decimal::ZERO {
        return None;
    }
    let limit = ask + dec!(0.01);
    (limit <= dec!(1.00) - filled_avg_entry - buffer).then_some(limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Explicit buffer literal — tests verify the helper's math, deliberately
    // independent of the tunable `config::ARB_FAK_REHEDGE_BUFFER` value.
    const BUF: Decimal = dec!(0.02);

    #[test]
    fn rehedge_viable_when_pair_cost_clears_payout() {
        // entry 0.40 → ceiling 1.00 − 0.40 − 0.02 = 0.58; ask 0.55 → limit 0.56 ≤ 0.58
        assert_eq!(rehedge_limit_if_viable(dec!(0.55), dec!(0.40), BUF), Some(dec!(0.56)));
        // exactly at the ceiling is still viable
        assert_eq!(rehedge_limit_if_viable(dec!(0.57), dec!(0.40), BUF), Some(dec!(0.58)));
    }

    #[test]
    fn rehedge_rejected_when_uneconomical() {
        // one tick over the ceiling
        assert_eq!(rehedge_limit_if_viable(dec!(0.58), dec!(0.40), BUF), None);
        // deep-entry leg: almost nothing clears
        assert_eq!(rehedge_limit_if_viable(dec!(0.10), dec!(0.95), BUF), None);
    }

    #[test]
    fn rehedge_rejected_on_degenerate_ask() {
        assert_eq!(rehedge_limit_if_viable(Decimal::ZERO, dec!(0.40), BUF), None);
        assert_eq!(rehedge_limit_if_viable(dec!(-0.05), dec!(0.40), BUF), None);
    }

    /// A flatten the venue killed unfilled books nothing; a fill books the
    /// venue's count and price, falling back to the limit sent only for the price.
    #[test]
    fn a_killed_flatten_books_nothing_and_a_fill_books_the_venue_figures() {
        assert_eq!(flatten_booking(Decimal::ZERO, Decimal::ZERO, dec!(0.01)), None);
        assert_eq!(flatten_booking(Decimal::ZERO, dec!(0.37), dec!(0.01)), None, "a price with no fill is not a sale");
        assert_eq!(flatten_booking(dec!(5), dec!(0.37), dec!(0.01)), Some((dec!(5), dec!(0.37))));
        assert_eq!(flatten_booking(dec!(3), Decimal::ZERO, dec!(0.01)), Some((dec!(3), dec!(0.01))), "no reported price books the limit sent");
    }

    /// [B74] Only a good-till-cancelled buy acknowledged with an order id and
    /// nothing filled is a resting quote. An immediate order that matched
    /// nothing is gone, a sell is the exit arm's to retry, an acknowledgment
    /// without an id cannot be tracked, and a fill is a fill.
    #[test]
    fn only_an_unfilled_gtc_buy_with_an_order_id_rests() {
        let id = OrderId("01a1".into());
        assert!(acknowledged_resting(Side::Buy, TimeInForce::Gtc, Decimal::ZERO, &id));
        assert!(acknowledged_resting(Side::Buy, TimeInForce::Gtd, Decimal::ZERO, &id));
        assert!(!acknowledged_resting(Side::Buy, TimeInForce::Fak, Decimal::ZERO, &id), "an immediate order that matched nothing is gone");
        assert!(!acknowledged_resting(Side::Buy, TimeInForce::Fok, Decimal::ZERO, &id));
        assert!(!acknowledged_resting(Side::Sell, TimeInForce::Gtc, Decimal::ZERO, &id), "a sell is the exit arm's to retry");
        assert!(!acknowledged_resting(Side::Buy, TimeInForce::Gtc, dec!(5), &id), "a fill is a fill");
        assert!(!acknowledged_resting(Side::Buy, TimeInForce::Gtc, Decimal::ZERO, &OrderId(String::new())), "no id, nothing to cancel or track");
    }
}

/// [B74] The lifecycle's handling of a resting quote: tracked by order id,
/// confirmed and sized by the fill feed, cancelled at rotation, pulled on the
/// viper's signal, never confirmed by another viper's holding on the same leg.
#[cfg(test)]
mod resting_quote_tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use crate::state::Position;
    use crate::venues::core::{MarketFacts, Position as Held};

    /// A venue that records the cancels it receives, refuses the ids it is
    /// told to, and reports whatever holdings and open orders the test set.
    struct QuoteVenue {
        cancelled: StdMutex<Vec<String>>,
        refuse: Vec<String>,
        held: Vec<Held>,
        open: Vec<OpenOrder>,
        /// `order_state` answers: an id's traded count (status unknown). An id
        /// in neither map answers `None` (not reported), as the default trait
        /// method does.
        fills: HashMap<String, Decimal>,
        /// `order_state` answers with a status word too.
        states: HashMap<String, OrderState>,
        /// `open_orders` fails.
        open_err: bool,
        /// `order_fill` fails.
        fill_err: bool,
    }

    impl QuoteVenue {
        fn new() -> Self {
            Self { cancelled: StdMutex::new(Vec::new()), refuse: Vec::new(), held: Vec::new(), open: Vec::new(), fills: HashMap::new(), states: HashMap::new(), open_err: false, fill_err: false }
        }
        fn cancelled(&self) -> Vec<String> { self.cancelled.lock().unwrap().clone() }
    }

    #[async_trait]
    impl Execution for QuoteVenue {
        async fn place_order(&self, _: OrderIntent) -> anyhow::Result<Fill> { unreachable!("these tests place nothing") }
        async fn place_atomic(&self, _: [OrderIntent; 2]) -> anyhow::Result<[Fill; 2]> { unreachable!("these tests place nothing") }
        async fn cancel(&self, id: OrderId) -> anyhow::Result<()> {
            if self.refuse.contains(&id.0) { anyhow::bail!("order {} is not resting", id.0); }
            self.cancelled.lock().unwrap().push(id.0);
            Ok(())
        }
        async fn collateral(&self) -> anyhow::Result<Decimal> { unreachable!("these tests read no collateral") }
        async fn positions(&self) -> anyhow::Result<Vec<Held>> { Ok(self.held.clone()) }
        async fn open_orders(&self) -> anyhow::Result<Vec<OpenOrder>> {
            if self.open_err { anyhow::bail!("HTTP 429 Too Many Requests"); }
            Ok(self.open.clone())
        }
        async fn order_state(&self, id: &OrderId) -> anyhow::Result<Option<OrderState>> {
            if self.fill_err { anyhow::bail!("HTTP 429 Too Many Requests"); }
            if let Some(st) = self.states.get(&id.0) { return Ok(Some(*st)); }
            Ok(self.fills.get(&id.0).map(|n| OrderState { filled: Some(*n), resting: None }))
        }
        async fn best_bid(&self, _: &MarketId) -> anyhow::Result<Option<Decimal>> { Ok(None) }
        async fn market_facts(&self, _: &MarketId) -> anyhow::Result<Option<MarketFacts>> { Ok(None) }
    }

    const SQ: &str = "btc-open";
    const LEG: &str = "KXBTCD-26OCT2917-T80499.99#yes";

    fn guard(map: &mut PositionMap, strategy: &str, leg: &str, shares: Decimal, price: Decimal, confirmed: bool) {
        map.insert(PositionKey::new(SQ, strategy, MarketId::new(leg)), Position {
            shares, avg_entry: price, opened_at: Utc::now(), close_time: None,
            market_name: "Bitcoin price on Oct 29, 2026?".into(), pair_token_id: MarketId::new(leg),
            fill_confirmed_at: confirmed.then(Utc::now), paired_leg_token_id: None, entry_fee: Decimal::ZERO,
        });
    }

    fn ack(id: &str, leg: &str, placed: Decimal, price: Decimal) -> Fill {
        Fill { order_id: OrderId(id.into()), market: MarketId::new(leg), filled: placed, price, fee: Decimal::ZERO }
    }

    fn open(id: &str, leg: &str, placed: Decimal, filled: Decimal) -> OpenOrder {
        OpenOrder { order_id: OrderId(id.into()), market: MarketId::new(leg), side: Side::Buy, price: dec!(0.30),
            original_qty: placed, filled_qty: filled, tif: TimeInForce::Gtc, pair_market: None }
    }

    fn event(id: &str, leg: &str, filled: Decimal, price: Decimal) -> FillEvent {
        FillEvent { order_id: OrderId(id.into()), market: MarketId::new(leg), side: Side::Buy, filled, price, complete: false }
    }

    async fn shares_of(map: &Arc<Mutex<PositionMap>>, strategy: &str) -> Option<(Decimal, bool)> {
        map.lock().await.get(&PositionKey::new(SQ, strategy, MarketId::new(LEG))).map(|p| (p.shares, p.fill_confirmed_at.is_some()))
    }

    /// Rotation cancels the resting quote, clears its unconfirmed guard and
    /// reports it for its pending row, while a confirmed guard (a position) stays
    /// in the map even though its order was cancelled too.
    #[tokio::test]
    async fn rotation_releases_an_unfilled_quote_and_keeps_a_filled_position() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let venue = QuoteVenue::new();
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        guard(&mut *map.lock().await, "FairValueStrategy", "other#no", dec!(5), dec!(0.40), true);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        lc.track(&ack("f1", "other#no", dec!(5), dec!(0.40)), "FairValueStrategy", TimeInForce::Gtc, None).await;

        let released = lc.cancel_all(&venue, &map).await;

        assert_eq!(venue.cancelled(), vec!["q1".to_string(), "f1".to_string()], "every tracked order is cancelled");
        assert_eq!(released, vec![ReleasedQuote { strategy: "MakerStrategy".into(), market: MarketId::new(LEG) }]);
        assert_eq!(shares_of(&map, "MakerStrategy").await, None, "the quote's guard leaves the map");
        assert!(map.lock().await.contains_key(&PositionKey::new(SQ, "FairValueStrategy", MarketId::new("other#no"))), "a position stays");
        assert_eq!(lc.take_released().await, released, "queued once for the caller's row cleanup");
        assert!(lc.take_released().await.is_empty());
        assert!(!lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await);
    }

    /// A fill event names an order. It confirms that order's own guard, sizes it
    /// to the venue's cumulative count at the venue's price, and leaves another
    /// viper's quote on the same leg untouched; the order stays tracked until
    /// the count reaches what was placed.
    #[tokio::test]
    async fn a_fill_event_confirms_and_sizes_only_the_order_it_names() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        guard(&mut *map.lock().await, "TimeDecayStrategy", LEG, dec!(10), dec!(0.28), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        lc.track_resting(&ack("q2", LEG, dec!(10), dec!(0.28)), "TimeDecayStrategy", TimeInForce::Gtc, None).await;

        lc.confirm_on_fill(&map, &event("q1", LEG, dec!(10), dec!(0.30))).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(10), true)), "sized to what traded, confirmed");
        assert_eq!(shares_of(&map, "TimeDecayStrategy").await, Some((dec!(10), false)), "the other viper's quote is untouched");
        assert_eq!(lc.take_confirmed().await, vec![ConfirmedFill { strategy: "MakerStrategy".into(), market: MarketId::new(LEG), filled: dec!(10), price: dec!(0.30) }]);
        assert!(lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await, "part of it still works");

        lc.confirm_on_fill(&map, &event("q1", LEG, dec!(26.67), dec!(0.30))).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), true)));
        assert!(!lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await, "fully traded: no longer resting");
        assert!(lc.resting_for("TimeDecayStrategy", &MarketId::new(LEG)).await);
    }

    /// An event for an order this engine did not track (an immediate fill, or a
    /// quote from before a restart) confirms every unconfirmed guard on the leg,
    /// as it always has.
    #[tokio::test]
    async fn an_untracked_fill_event_confirms_the_leg_as_before() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "AnchorStrategy", LEG, dec!(5), dec!(0.30), false);
        lc.confirm_on_fill(&map, &event("fak-1", LEG, dec!(5), dec!(0.30))).await;
        assert_eq!(shares_of(&map, "AnchorStrategy").await, Some((dec!(5), true)));
        assert!(lc.take_confirmed().await.is_empty(), "nothing tracked, nothing to resize");
    }

    /// Someone else's fill on the leg says nothing about a quote resting there:
    /// FairValue's take-profit sale, or Anchor's taker buy, must not confirm
    /// the Maker's unfilled bid or drop it from tracking (it would then be sold
    /// as if held, and left on the book at rotation).
    #[tokio::test]
    async fn another_vipers_fill_on_the_leg_leaves_a_resting_quote_alone() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        guard(&mut *map.lock().await, "AnchorStrategy", LEG, dec!(5), dec!(0.31), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;

        let mut sell = event("fv-tp", LEG, dec!(5), dec!(0.40));
        sell.side = Side::Sell;
        lc.confirm_on_fill(&map, &sell).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)), "a sale confirms no buy guard");
        assert_eq!(shares_of(&map, "AnchorStrategy").await, Some((dec!(5), false)));

        lc.confirm_on_fill(&map, &event("anchor-fak", LEG, dec!(5), dec!(0.31))).await;
        assert_eq!(shares_of(&map, "AnchorStrategy").await, Some((dec!(5), true)), "the untracked taker fill confirms its own guard");
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)), "the quote is still a quote");
        assert!(lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await, "and still tracked");
    }

    /// A quote the venue no longer lists is settled by its own order, never by
    /// the leg's holdings: traded contracts confirm it at the traded size, a
    /// cancelled or expired order releases it, and when the venue cannot say
    /// the guard waits out the fill-feed grace instead of being confirmed from
    /// someone else's holding. A failed open-orders read changes nothing.
    #[tokio::test]
    async fn a_vanished_quote_is_settled_by_its_own_order_not_the_legs_holdings() {
        let mk = || {
            let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
            let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
            (lc, map)
        };
        let mut venue = QuoteVenue::new();
        venue.held = vec![Held { market: MarketId::new(LEG), shares: dec!(5), avg_price: dec!(0.31) }]; // FairValue's

        // Traded in full before vanishing.
        let (lc, map) = mk();
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        venue.fills.insert("q1".into(), dec!(26.67));
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), true)));
        assert!(!lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await);
        assert_eq!(lc.take_confirmed().await.len(), 1);

        // Traded in part: sized to what traded.
        let (lc, map) = mk();
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        venue.fills.insert("q1".into(), dec!(10));
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(10), true)), "the guard carries what traded, not what was placed");

        // Nothing traded and the venue says the order is done (expired, or
        // pulled by the venue): released. A zero with no status word is not
        // enough: that waits out the grace below.
        let (lc, map) = mk();
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        venue.fills.remove("q1");
        venue.states.insert("q1".into(), OrderState { filled: Some(Decimal::ZERO), resting: Some(false) });
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, None);
        assert_eq!(lc.take_released().await, vec![ReleasedQuote { strategy: "MakerStrategy".into(), market: MarketId::new(LEG) }]);
        venue.states.remove("q1");

        // The venue cannot say: the guard waits, unconfirmed, inside the grace.
        let (lc, map) = mk();
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        venue.fills.remove("q1");
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)), "FairValue's five contracts never confirm the Maker's quote");
        assert!(lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await);

        // A failed open-orders read: nothing moves.
        venue.open_err = true;
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)));
        assert!(lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await);
        assert!(venue.cancelled().is_empty());
    }

    /// The reconciler confirming a working quote in part sizes the guard to that
    /// part, and a later fill event never grows it past what the venue reports.
    #[tokio::test]
    async fn a_reconcile_confirm_is_sized_and_a_later_event_keeps_the_venues_count() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.held = vec![Held { market: MarketId::new(LEG), shares: dec!(10), avg_price: dec!(0.30) }];
        venue.open = vec![open("q1", LEG, dec!(26.67), dec!(10))];
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;

        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(10), true)), "confirmed at the ten that traded, not the 26.67 placed");
        assert_eq!(lc.take_confirmed().await, vec![ConfirmedFill { strategy: "MakerStrategy".into(), market: MarketId::new(LEG), filled: dec!(10), price: Decimal::ZERO }]);

        lc.confirm_on_fill(&map, &event("q1", LEG, dec!(10), dec!(0.30))).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(10), true)));
        lc.confirm_on_fill(&map, &event("q1", LEG, dec!(12), dec!(0.30))).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(12), true)), "more traded: the guard follows the venue");
    }

    /// A cancel the venue does not confirm at stand-down leaves the quote
    /// tracked and its guard in place, so nothing downstream sells it as held
    /// and nothing forgets it on the book; a cancel that succeeds after part
    /// of the quote traded keeps that part as a position.
    #[tokio::test]
    async fn stand_down_keeps_a_quote_whose_cancel_failed_and_what_traded_before_a_cancel() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.refuse = vec!["q1".into()];
        venue.fills.insert("q2".into(), dec!(7));
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        guard(&mut *map.lock().await, "TimeDecayStrategy", LEG, dec!(10), dec!(0.28), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        lc.track_resting(&ack("q2", LEG, dec!(10), dec!(0.28)), "TimeDecayStrategy", TimeInForce::Gtc, None).await;

        let released = lc.cancel_all(&venue, &map).await;
        assert!(released.is_empty(), "nothing was cancelled unfilled: {released:?}");
        assert!(lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await, "the refused cancel keeps the quote tracked");
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)), "and its guard in place, still a quote");
        assert_eq!(shares_of(&map, "TimeDecayStrategy").await, Some((dec!(7), true)), "the seven that traded before the cancel are a position");
        assert!(!lc.resting_for("TimeDecayStrategy", &MarketId::new(LEG)).await);
        assert_eq!(lc.take_confirmed().await.len(), 1);
    }

    /// A paired leg (an arbitrage entry that filled at placement) is confirmed
    /// by the venue's holdings as it always was, even when the venue lists its
    /// order with a fill count it does not know: the intl CLOB lists every
    /// resting order with zero.
    #[tokio::test]
    async fn a_paired_leg_is_confirmed_by_holdings_whatever_the_listing_says() {
        let lc = OrderLifecycle::new(LifecycleConfig::intl(), SQ);
        let mut venue = QuoteVenue::new();
        venue.held = vec![Held { market: MarketId::new(LEG), shares: dec!(5), avg_price: dec!(0.40) }];
        venue.open = vec![open("arb-1", LEG, dec!(5), Decimal::ZERO)];
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "ArbitrageStrategy", LEG, dec!(5), dec!(0.40), false);
        lc.track(&ack("arb-1", LEG, dec!(5), dec!(0.40)), "ArbitrageStrategy", TimeInForce::Gtc, Some(MarketId::new("other#no"))).await;
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "ArbitrageStrategy").await, Some((dec!(5), true)));
        assert!(!lc.resting_for("ArbitrageStrategy", &MarketId::new(LEG)).await);
    }

    /// A venue that cannot answer about a cancelled or vanished order settles
    /// nothing: the order stays tracked, its guard stays an inert quote, and
    /// nothing is released or confirmed until the venue answers.
    #[tokio::test]
    async fn a_failed_order_read_keeps_the_quote_tracked_and_inert() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.fill_err = true;
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;

        assert_eq!(lc.cancel_resting_for(&venue, &map, "MakerStrategy", &MarketId::new(LEG)).await, 1, "cancelled at the venue, but not settled");
        assert_eq!(venue.cancelled(), vec!["q1".to_string()]);
        assert!(lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await, "still tracked, so nothing sells it as held");
        assert!(!lc.working_for("MakerStrategy", &MarketId::new(LEG)).await, "but no longer working: a new quote may be placed and an exit need not wait");
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)));
        assert!(lc.take_released().await.is_empty());

        // The read keeps failing inside the grace: waits.
        lc.reconcile(&venue, &map).await;
        assert!(lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await);
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)));
        assert!(!lc.holds_anything(&map).await, "an inert quote is not a holding");
        assert!(lc.tracks_anything().await, "but rotation waits for it to settle");

        // The grace runs out with the venue still not answering: the unfilled
        // guard is released for the account sweep, and the order forgotten.
        lc.tracked.lock().await.iter_mut().for_each(|o| o.missing_since = Some(Instant::now() - std::time::Duration::from_secs(VANISHED_QUOTE_GRACE_SECS + 1)));
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, None);
        assert!(!lc.tracks_anything().await);
        assert_eq!(lc.take_released().await, vec![ReleasedQuote { strategy: "MakerStrategy".into(), market: MarketId::new(LEG) }]);
    }

    /// A cancelled order the venue never answers about, whose guard a fill
    /// event already confirmed: after the grace the order is forgotten and the
    /// guard kept at its size, so the stop that was waiting on it can sell.
    #[tokio::test]
    async fn a_cancelled_order_never_answered_keeps_its_confirmed_guard_and_is_forgotten() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.fill_err = true;
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        lc.confirm_on_fill(&map, &event("q1", LEG, dec!(3), dec!(0.30))).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(3), true)));

        lc.cancel_resting_for(&venue, &map, "MakerStrategy", &MarketId::new(LEG)).await;
        assert!(!lc.working_for("MakerStrategy", &MarketId::new(LEG)).await, "cancelled: the exit arm may sell the three");
        assert!(lc.holds_anything(&map).await);

        lc.tracked.lock().await.iter_mut().for_each(|o| o.missing_since = Some(Instant::now() - std::time::Duration::from_secs(VANISHED_QUOTE_GRACE_SECS + 1)));
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(3), true)), "the position stays");
        assert!(!lc.tracks_anything().await, "the order is forgotten");
        assert!(lc.take_released().await.is_empty());
    }

    /// What the venue says about an order touches only the guard that order
    /// was opened for. A cancelled order whose fill the venue answers late must
    /// not size, confirm or release the guard of the quote that replaced it on
    /// the same leg.
    #[tokio::test]
    async fn a_late_answer_about_an_old_order_never_touches_the_next_quotes_guard() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.fill_err = true;
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(10), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(10), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        lc.confirm_on_fill(&map, &event("q1", LEG, dec!(3), dec!(0.30))).await;
        assert_eq!(lc.take_confirmed().await.len(), 1, "the event's own confirmation, drained by the loop");
        lc.cancel_resting_for(&venue, &map, "MakerStrategy", &MarketId::new(LEG)).await;
        assert!(!lc.working_for("MakerStrategy", &MarketId::new(LEG)).await, "cancelled, fill unknown");

        // The exit sells the three and drops the guard; the viper re-quotes.
        map.lock().await.remove(&PositionKey::new(SQ, "MakerStrategy", MarketId::new(LEG)));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.28), false);
        lc.track_resting(&ack("q2", LEG, dec!(26.67), dec!(0.28)), "MakerStrategy", TimeInForce::Gtc, None).await;

        // The venue finally answers about q1: three traded, long since sold.
        venue.fill_err = false;
        venue.states.insert("q1".into(), OrderState { filled: Some(dec!(3)), resting: Some(false) });
        venue.open = vec![open("q2", LEG, dec!(26.67), Decimal::ZERO)];
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)), "q2's guard is untouched by q1's answer");
        assert!(lc.take_confirmed().await.is_empty());
        assert!(lc.working_for("MakerStrategy", &MarketId::new(LEG)).await, "q2 works on");
        assert_eq!(lc.tracked_ids().await.len(), 1, "q1 is forgotten");

        // And an answer of nothing traded about an old cancelled order releases
        // nothing of the quote that replaced it.
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(10), dec!(0.30), false);
        lc.track_resting(&ack("q3", LEG, dec!(10), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        lc.mark_cancelled(&OrderId("q3".into())).await;
        map.lock().await.remove(&PositionKey::new(SQ, "MakerStrategy", MarketId::new(LEG)));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.28), false);
        lc.track_resting(&ack("q4", LEG, dec!(26.67), dec!(0.28)), "MakerStrategy", TimeInForce::Gtc, None).await;
        venue.states.insert("q3".into(), OrderState { filled: Some(Decimal::ZERO), resting: Some(false) });
        venue.open = vec![open("q4", LEG, dec!(26.67), Decimal::ZERO)];
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)), "q4's guard is untouched by q3's answer");
        assert!(lc.take_released().await.is_empty());
        assert_eq!(lc.tracked_ids().await.len(), 1, "q3 is forgotten, q4 works on");
    }

    /// A cancel the venue refused while the order still rests (the call never
    /// landed) leaves the order tracked and working: nothing is released, no
    /// second quote is placed, and the next attempt cancels it.
    #[tokio::test]
    async fn a_refused_cancel_on_an_order_still_resting_keeps_it_working() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.refuse = vec!["q1".into()];
        venue.states.insert("q1".into(), OrderState { filled: Some(Decimal::ZERO), resting: Some(true) });
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;

        assert_eq!(lc.cancel_resting_for(&venue, &map, "MakerStrategy", &MarketId::new(LEG)).await, 0);
        assert!(lc.working_for("MakerStrategy", &MarketId::new(LEG)).await);
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)));
        assert!(lc.cancel_all(&venue, &map).await.is_empty());
        assert!(lc.tracks_anything().await, "rotation waits for it");
        assert!(lc.take_released().await.is_empty());

        // Not on the page the listing returned, yet the venue says it rests:
        // nothing changes.
        lc.reconcile(&venue, &map).await;
        assert!(lc.working_for("MakerStrategy", &MarketId::new(LEG)).await);
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)));
    }

    /// A quote no longer listed whose read keeps failing is not frozen for
    /// good: after the grace an unconfirmed guard is released, and a confirmed
    /// one is kept while the order is forgotten, so an exit can proceed.
    #[tokio::test]
    async fn a_vanished_quote_with_a_failing_read_settles_after_the_grace() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.fill_err = true;
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        guard(&mut *map.lock().await, "TimeDecayStrategy", LEG, dec!(10), dec!(0.28), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        lc.track_resting(&ack("q2", LEG, dec!(10), dec!(0.28)), "TimeDecayStrategy", TimeInForce::Gtc, None).await;
        lc.confirm_on_fill(&map, &event("q2", LEG, dec!(4), dec!(0.28))).await;

        lc.reconcile(&venue, &map).await;
        assert!(lc.working_for("MakerStrategy", &MarketId::new(LEG)).await, "inside the grace it waits");
        lc.tracked.lock().await.iter_mut().for_each(|o| o.missing_since = Some(Instant::now() - std::time::Duration::from_secs(VANISHED_QUOTE_GRACE_SECS + 1)));
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, None, "the unfilled quote is released for the account sweep");
        assert_eq!(shares_of(&map, "TimeDecayStrategy").await, Some((dec!(4), true)), "the confirmed four stay");
        assert!(!lc.tracks_anything().await);
        assert_eq!(lc.take_released().await, vec![ReleasedQuote { strategy: "MakerStrategy".into(), market: MarketId::new(LEG) }]);
    }

    /// A quote one pass could not find and could not read starts the grace
    /// clock; a later pass that sees it working stops that clock, so a single
    /// later miss cannot release an order that still works.
    #[tokio::test]
    async fn seeing_a_quote_work_again_stops_the_grace_clock() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.fill_err = true;
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;

        lc.reconcile(&venue, &map).await; // missed and unreadable: the clock starts
        lc.tracked.lock().await.iter_mut().for_each(|o| o.missing_since = Some(Instant::now() - std::time::Duration::from_secs(VANISHED_QUOTE_GRACE_SECS - 1)));
        venue.open = vec![open("q1", LEG, dec!(26.67), Decimal::ZERO)];
        lc.reconcile(&venue, &map).await; // listed: the clock stops
        venue.open.clear();
        lc.reconcile(&venue, &map).await; // missed again, well inside a fresh grace
        assert!(lc.working_for("MakerStrategy", &MarketId::new(LEG)).await, "a fresh miss starts a fresh clock");
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)));

        // Reported resting though not listed: the same, and a partial it states is sized.
        venue.fill_err = false;
        venue.states.insert("q1".into(), OrderState { filled: Some(dec!(4)), resting: Some(true) });
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(4), true)));
        assert!(lc.working_for("MakerStrategy", &MarketId::new(LEG)).await);
    }

    /// A paired leg (an arbitrage entry) is a holding before the reconciler
    /// confirms it, so rotation waits for it as it always did; only a quote is
    /// inert.
    #[tokio::test]
    async fn a_paired_leg_is_a_holding_before_it_is_confirmed() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "TimeDecayStrategy", LEG, dec!(5), dec!(0.40), false);
        lc.track(&ack("td-1", LEG, dec!(5), dec!(0.40)), "TimeDecayStrategy", TimeInForce::Gtc, Some(MarketId::new("other#no"))).await;
        assert!(lc.holds_anything(&map).await);
    }

    /// A reconcile reading taken before a fill event never shrinks a guard the
    /// event already sized: the larger count is the later truth.
    #[tokio::test]
    async fn a_stale_listing_never_shrinks_an_event_sized_guard() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.open = vec![open("q1", LEG, dec!(26.67), dec!(10))];
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        lc.confirm_on_fill(&map, &event("q1", LEG, dec!(12), dec!(0.30))).await;
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(12), true)));
    }

    /// A refused stand-down cancel that the venue explains as a fill keeps the
    /// traded contracts as a position; a confirmed position stops rotation,
    /// a quote never does.
    #[tokio::test]
    async fn a_refused_cancel_explained_as_a_fill_becomes_a_position() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.refuse = vec!["q1".into()];
        venue.states.insert("q1".into(), OrderState { filled: Some(dec!(26.67)), resting: Some(false) });
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        assert!(!lc.holds_anything(&map).await, "a working quote does not pin the squadron to its market");

        assert!(lc.cancel_all(&venue, &map).await.is_empty());
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), true)));
        assert!(!lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await);
        assert!(lc.holds_anything(&map).await);
    }

    /// The venue lists the quote as working with nothing traded while another
    /// guard holds the same leg: the reconciler does not call that a fill. Once
    /// the quote's own window runs out it is cancelled, its guard cleared and
    /// the quote reported for its row.
    #[tokio::test]
    async fn a_working_quote_is_not_confirmed_by_anothers_holding_and_goes_stale_on_its_own_clock() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.held = vec![Held { market: MarketId::new(LEG), shares: dec!(5), avg_price: dec!(0.31) }];
        venue.open = vec![open("q1", LEG, dec!(26.67), Decimal::ZERO)];
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "FairValueStrategy", LEG, dec!(5), dec!(0.31), true);
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;

        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await, Some((dec!(26.67), false)), "FairValue's five contracts are not the Maker's fill");
        assert!(venue.cancelled().is_empty(), "inside its window the quote is left to work");

        // The same quote tracked with no window left: the reconciler cancels it.
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        lc.track_with_stale(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None, 0, true).await;
        lc.reconcile(&venue, &map).await;
        assert_eq!(venue.cancelled(), vec!["q1".to_string()]);
        assert_eq!(shares_of(&map, "MakerStrategy").await, None);
        assert_eq!(lc.take_released().await, vec![ReleasedQuote { strategy: "MakerStrategy".into(), market: MarketId::new(LEG) }]);
        assert!(map.lock().await.contains_key(&PositionKey::new(SQ, "FairValueStrategy", MarketId::new(LEG))), "the real position is untouched");
    }

    /// A tracked quote the venue lists as partly traded is confirmed and stays
    /// tracked for its remainder.
    #[tokio::test]
    async fn a_partly_traded_working_quote_is_confirmed_and_stays_tracked() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let mut venue = QuoteVenue::new();
        venue.held = vec![Held { market: MarketId::new(LEG), shares: dec!(10), avg_price: dec!(0.30) }];
        venue.open = vec![open("q1", LEG, dec!(26.67), dec!(10))];
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        lc.reconcile(&venue, &map).await;
        assert_eq!(shares_of(&map, "MakerStrategy").await.map(|(_, c)| c), Some(true));
        assert!(lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await, "the remainder still works");
    }

    /// The viper's pull cancels its own quote on the leg and nothing else; a
    /// cancel the venue refuses (the quote filled) leaves the order tracked for
    /// the reconciler and releases nothing.
    #[tokio::test]
    async fn a_quote_pull_cancels_one_vipers_quote_and_a_refused_cancel_keeps_it() {
        let lc = OrderLifecycle::new(LifecycleConfig::us(), SQ);
        let venue = QuoteVenue::new();
        let map: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        guard(&mut *map.lock().await, "MakerStrategy", LEG, dec!(26.67), dec!(0.30), false);
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        lc.track_resting(&ack("q2", LEG, dec!(10), dec!(0.28)), "TimeDecayStrategy", TimeInForce::Gtc, None).await;

        assert_eq!(lc.cancel_resting_for(&venue, &map, "MakerStrategy", &MarketId::new(LEG)).await, 1);
        assert_eq!(venue.cancelled(), vec!["q1".to_string()]);
        assert!(!lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await);
        assert!(lc.resting_for("TimeDecayStrategy", &MarketId::new(LEG)).await, "the other viper's quote keeps working");
        assert_eq!(lc.take_released().await, vec![ReleasedQuote { strategy: "MakerStrategy".into(), market: MarketId::new(LEG) }]);

        let mut refusing = QuoteVenue::new();
        refusing.refuse = vec!["q2".into()];
        assert_eq!(lc.cancel_resting_for(&refusing, &map, "TimeDecayStrategy", &MarketId::new(LEG)).await, 0);
        assert!(lc.resting_for("TimeDecayStrategy", &MarketId::new(LEG)).await, "a refused cancel means it filled: the reconciler settles it");
        assert!(lc.take_released().await.is_empty());
    }
}

