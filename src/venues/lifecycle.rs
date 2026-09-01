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
    Execution, Fill, MarketId, OrderId, OrderIntent, Side, TimeInForce,
};

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
    /// The existing `arb_pair_fill_monitor` handles the fast-path (30 s grace),
    /// so this backstop only fires on genuinely abandoned resting orders.
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
    /// Partner leg's market for a paired (arbitrage) entry — lets the reconciler
    /// detect a naked leg when this one fills but the partner doesn't.
    #[allow(dead_code)]
    pair_market: Option<MarketId>,
}

/// Shared, venue-neutral order lifecycle engine.
pub struct OrderLifecycle {
    cfg: LifecycleConfig,
    /// Squadron whose positions this engine reconciles. See `new`.
    squadron_id: String,
    tracked: Mutex<Vec<TrackedLeg>>,
}

impl OrderLifecycle {
    /// `squadron_id` identifies the squadron this engine reconciles for. It is
    /// part of every `PositionKey` the engine touches, so that confirming or
    /// clearing a guard reaches the position belonging to THIS squadron rather
    /// than another squadron's position on the same token.
    pub fn new(cfg: LifecycleConfig, squadron_id: impl Into<String>) -> Self {
        Self { cfg, squadron_id: squadron_id.into(), tracked: Mutex::new(Vec::new()) }
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
        if !matches!(tif, TimeInForce::Gtc | TimeInForce::Gtd) {
            return;
        }
        self.tracked.lock().await.push(TrackedLeg {
            id: fill.order_id.clone(),
            market: fill.market.clone(),
            strategy: strategy.to_string(),
            placed_at: Instant::now(),
            pair_market,
        });
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

        // Venue-reported resting orders (empty for venues that stub open_orders()).
        let venue_resting: HashSet<String> = venue
            .open_orders()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|o| o.is_resting() && o.remaining_qty() > Decimal::ZERO)
            .map(|o| o.market.as_str().to_string())
            .collect();

        // Pass 1 — fill-confirm or stale-cancel each tracked order.
        let snapshot: Vec<TrackedLeg> = self.tracked.lock().await.clone();
        let mut keep: Vec<TrackedLeg> = Vec::with_capacity(snapshot.len());
        for ord in snapshot {
            let filled = held.get(ord.market.as_str()).copied().unwrap_or(Decimal::ZERO) > Decimal::ZERO;
            if filled {
                confirm_guard(positions, &self.squadron_id, &ord.strategy, &ord.market).await;
                continue; // resting done — drop from tracking
            }
            if ord.placed_at.elapsed().as_secs() >= self.cfg.stale_order_secs {
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
                Ok(f)  => {
                    // Record the ACTUAL fill price/qty from the venue, not the
                    // flatten *limit*. Booking the 0.01 limit overstated losses:
                    // e.g. Jun 19 trade id 50 booked −$2.94 at exit 0.01 when the
                    // FAK sell actually crossed near the prevailing bid. Fall back
                    // to the configured limit / intended shares only if the venue
                    // doesn't report them.
                    let exit_price  = if f.price  > dec!(0) { f.price }  else { self.cfg.flatten_sell_limit };
                    let exit_shares = if f.filled > dec!(0) { f.filled } else { shares };
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
    pub async fn cancel_all<V: Execution + ?Sized>(&self, venue: &V) {
        let orders: Vec<TrackedLeg> = std::mem::take(&mut *self.tracked.lock().await);
        for ord in orders {
            if let Err(e) = venue.cancel(ord.id.clone()).await {
                warn!("stand-down cancel failed for {} ({}): {e}", ord.id, ord.market);
            }
        }
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
                        // Confirm the guard for whichever strategy holds this leg.
                        lifecycle.confirm_on_fill(&positions, &ev.market).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("OrderLifecycle fill listener lagged {n} events — reconcile will recover");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }))
    }

    /// Confirm every strategy guard holding `market` and drop the order from
    /// tracking. Used by the event-driven fill listener.
    async fn confirm_on_fill(&self, positions: &Arc<Mutex<PositionMap>>, market: &MarketId) {
        {
            let mut map = positions.lock().await;
            for (k, p) in map.iter_mut() {
                if k.squadron == self.squadron_id && &k.market == market && p.fill_confirmed_at.is_none() {
                    p.fill_confirmed_at = Some(Utc::now());
                }
            }
        }
        self.tracked.lock().await.retain(|o| &o.market != market);
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
}

