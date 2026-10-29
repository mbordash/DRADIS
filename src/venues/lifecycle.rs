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
    Execution, Fill, FillEvent, MarketId, OpenOrder, OrderId, OrderIntent, Side, TimeInForce,
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

    /// Register a freshly placed order so the reconciler manages its lifecycle.
    /// No-op for immediate (`Fak`/`Fok`) orders, which fill or kill within their ack.
    pub async fn track(
        &self,
        fill: &Fill,
        strategy: &str,
        tif: TimeInForce,
        pair_market: Option<MarketId>,
    ) {
        self.track_with_stale(fill, strategy, tif, pair_market, self.cfg.stale_order_secs).await;
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
        self.track_with_stale(fill, strategy, tif, pair_market, RESTING_QUOTE_STALE_SECS).await;
    }

    async fn track_with_stale(
        &self,
        fill: &Fill,
        strategy: &str,
        tif: TimeInForce,
        pair_market: Option<MarketId>,
        stale_after_secs: u64,
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
            pair_market,
        });
    }

    /// Does this strategy have a tracked order still resting on `market`?
    /// The dispatchers ask before placing another quote on the same leg, and
    /// before selling a position whose guard is an unfilled quote.
    pub async fn resting_for(&self, strategy: &str, market: &MarketId) -> bool {
        self.tracked.lock().await.iter().any(|o| o.strategy == strategy && &o.market == market)
    }

    /// Cancel every tracked order this strategy has resting on `market` (the
    /// viper's quote-pull, or an exit signalled on a quote that never filled).
    /// Returns how many the venue confirmed cancelled; those leave tracking and
    /// are reported through [`take_released`](Self::take_released). An order
    /// the venue refuses to cancel has filled or is already gone: it stays
    /// tracked for the reconciler to settle.
    pub async fn cancel_resting_for<V: Execution + ?Sized>(&self, venue: &V, strategy: &str, market: &MarketId) -> usize {
        let mine: Vec<TrackedLeg> = self.tracked.lock().await
            .iter().filter(|o| o.strategy == strategy && &o.market == market).cloned().collect();
        let mut cancelled = 0;
        for ord in mine {
            match venue.cancel(ord.id.clone()).await {
                Ok(()) => {
                    cancelled += 1;
                    self.tracked.lock().await.retain(|o| o.id != ord.id);
                    self.released.lock().await.push(ReleasedQuote { strategy: ord.strategy.clone(), market: ord.market.clone() });
                }
                Err(e) => warn!("[{strategy}] quote-pull cancel refused for {} ({}): {e} — it has filled or is gone; the reconciler settles it", ord.id, ord.market),
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

        // Venue-reported resting orders (empty for venues that stub open_orders()):
        // the markets they rest on, for the naked-leg pass, and each order's
        // filled count by id, so a tracked order the venue still lists as
        // working is never mistaken for a fill because some OTHER guard holds
        // the same leg (`positions()` is per leg, not per order).
        let venue_open: Vec<OpenOrder> = venue.open_orders().await.unwrap_or_default();
        let venue_resting: HashSet<String> = venue_open
            .iter()
            .filter(|o| o.is_resting() && o.remaining_qty() > Decimal::ZERO)
            .map(|o| o.market.as_str().to_string())
            .collect();
        let venue_open_filled: HashMap<String, Decimal> = venue_open
            .iter()
            .map(|o| (o.order_id.0.clone(), o.filled_qty))
            .collect();

        // Pass 1 — fill-confirm or stale-cancel each tracked order.
        let snapshot: Vec<TrackedLeg> = self.tracked.lock().await.clone();
        let mut keep: Vec<TrackedLeg> = Vec::with_capacity(snapshot.len());
        for ord in snapshot {
            let still_open = venue_open_filled.get(&ord.id.0).copied();
            let filled = match still_open {
                // Listed as working with part of it traded: the leg is held.
                Some(part) if part > Decimal::ZERO => true,
                // Listed as working, nothing traded: not a fill, whoever else
                // holds the leg.
                Some(_) => false,
                // Not listed (or the venue reports no orders): the venue's
                // holdings decide, as they always have.
                None => held.get(ord.market.as_str()).copied().unwrap_or(Decimal::ZERO) > Decimal::ZERO,
            };
            if filled {
                confirm_guard(positions, &self.squadron_id, &ord.strategy, &ord.market).await;
                // A partly traded order still works for the rest; it stays
                // tracked so the fill feed can finish sizing it and rotation
                // still cancels the remainder.
                if still_open.is_some_and(|part| part < ord.quantity) { keep.push(ord); }
                continue;
            }
            if ord.placed_at.elapsed().as_secs() >= ord.stale_after_secs {
                match venue.cancel(ord.id.clone()).await {
                    Ok(_)  => info!(" [{}] cancelled stale resting order {} ({})", ord.strategy, ord.id, ord.market),
                    Err(e) => warn!("[{}] stale cancel failed for {} ({}): {e}", ord.strategy, ord.id, ord.market),
                }
                clear_guard(positions, &self.squadron_id, &ord.strategy, &ord.market).await;
                self.released.lock().await.push(ReleasedQuote { strategy: ord.strategy.clone(), market: ord.market.clone() });
                continue;
            }
            keep.push(ord);
        }
        // "Still resting" = locally tracked AND/OR venue-reported.
        let mut resting_tokens: HashSet<String> =
            keep.iter().map(|o| o.market.as_str().to_string()).collect();
        resting_tokens.extend(venue_resting);
        *self.tracked.lock().await = keep;

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
        let orders: Vec<TrackedLeg> = std::mem::take(&mut *self.tracked.lock().await);
        let mut released = Vec::new();
        for ord in orders {
            match venue.cancel(ord.id.clone()).await {
                Ok(()) => {
                    let unfilled = positions.lock().await
                        .get(&PositionKey::new(&self.squadron_id, &ord.strategy, ord.market.clone()))
                        .is_some_and(|p| p.fill_confirmed_at.is_none());
                    if unfilled {
                        clear_guard(positions, &self.squadron_id, &ord.strategy, &ord.market).await;
                        info!("🧹 [{}] resting order {} on {} cancelled unfilled — quote released", ord.strategy, ord.id, ord.market);
                        released.push(ReleasedQuote { strategy: ord.strategy.clone(), market: ord.market.clone() });
                    }
                }
                Err(e) => warn!("stand-down cancel failed for {} ({}): {e}", ord.id, ord.market),
            }
        }
        self.released.lock().await.extend(released.iter().cloned());
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
            {
                let mut map = positions.lock().await;
                for (k, p) in map.iter_mut() {
                    if k.squadron == self.squadron_id && k.market == ev.market && p.fill_confirmed_at.is_none() {
                        p.fill_confirmed_at = Some(Utc::now());
                    }
                }
            }
            self.tracked.lock().await.retain(|o| o.market != ev.market);
            return;
        };
        let filled = if ev.filled > Decimal::ZERO { ev.filled.min(ord.quantity) } else { ord.quantity };
        {
            let mut map = positions.lock().await;
            if let Some(p) = map.get_mut(&PositionKey::new(&self.squadron_id, &ord.strategy, ord.market.clone())) {
                if p.fill_confirmed_at.is_none() {
                    // A resting quote's guard carried the size placed; the
                    // venue has now said what traded.
                    p.shares = filled;
                    if ev.price > Decimal::ZERO { p.avg_entry = ev.price; }
                    p.fill_confirmed_at = Some(Utc::now());
                    info!("✅ [{}] fill confirmed: {} — {filled} @ {:.4} (order {})", ord.strategy, ord.market, p.avg_entry, ord.id);
                } else if filled > p.shares {
                    // A later event on a partly traded quote: more of it traded.
                    p.shares = filled;
                }
            }
        }
        self.confirmed.lock().await.push(ConfirmedFill {
            strategy: ord.strategy.clone(), market: ord.market.clone(), filled, price: ev.price,
        });
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
    }

    impl QuoteVenue {
        fn new() -> Self {
            Self { cancelled: StdMutex::new(Vec::new()), refuse: Vec::new(), held: Vec::new(), open: Vec::new() }
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
        async fn open_orders(&self) -> anyhow::Result<Vec<OpenOrder>> { Ok(self.open.clone()) }
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
        lc.track_with_stale(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None, 0).await;
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
        lc.track_resting(&ack("q1", LEG, dec!(26.67), dec!(0.30)), "MakerStrategy", TimeInForce::Gtc, None).await;
        lc.track_resting(&ack("q2", LEG, dec!(10), dec!(0.28)), "TimeDecayStrategy", TimeInForce::Gtc, None).await;

        assert_eq!(lc.cancel_resting_for(&venue, "MakerStrategy", &MarketId::new(LEG)).await, 1);
        assert_eq!(venue.cancelled(), vec!["q1".to_string()]);
        assert!(!lc.resting_for("MakerStrategy", &MarketId::new(LEG)).await);
        assert!(lc.resting_for("TimeDecayStrategy", &MarketId::new(LEG)).await, "the other viper's quote keeps working");
        assert_eq!(lc.take_released().await, vec![ReleasedQuote { strategy: "MakerStrategy".into(), market: MarketId::new(LEG) }]);

        let mut refusing = QuoteVenue::new();
        refusing.refuse = vec!["q2".into()];
        assert_eq!(lc.cancel_resting_for(&refusing, "TimeDecayStrategy", &MarketId::new(LEG)).await, 0);
        assert!(lc.resting_for("TimeDecayStrategy", &MarketId::new(LEG)).await, "a refused cancel means it filled: the reconciler settles it");
        assert!(lc.take_released().await.is_empty());
    }
}

