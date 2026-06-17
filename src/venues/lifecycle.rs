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

use crate::state::PositionMap;
use crate::venues::core::{
    Execution, Fill, MarketId, OrderId, OrderIntent, Side, TimeInForce,
};

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
}

impl LifecycleConfig {
    /// Defaults matching the shipped US reconciliation constants.
    pub fn us() -> Self {
        Self {
            stale_order_secs: 60,
            flatten_sell_limit: dec!(0.01),
            flatten_is_neg_risk: false,
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
    tracked: Mutex<Vec<TrackedLeg>>,
}

impl OrderLifecycle {
    pub fn new(cfg: LifecycleConfig) -> Self {
        Self { cfg, tracked: Mutex::new(Vec::new()) }
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
    /// Venue-neutral replacement for the US loop's `reconcile_orders` and intl's
    /// on-chain patrol lifecycle. Uses [`Execution::positions`] as the held-truth
    /// source and [`Execution::open_orders`] (when the venue reports it) to widen
    /// "still resting" beyond locally-tracked orders.
    pub async fn reconcile<V: Execution + ?Sized>(
        &self,
        venue: &V,
        positions: &Arc<Mutex<PositionMap>>,
    ) {
        // Venue truth: market → shares currently held.
        let held: HashMap<String, Decimal> = venue
            .positions()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|p| (p.market.as_str().to_string(), p.shares))
            .collect();

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
                confirm_guard(positions, &ord.strategy, &ord.market).await;
                continue; // resting done — drop from tracking
            }
            if ord.placed_at.elapsed().as_secs() >= self.cfg.stale_order_secs {
                match venue.cancel(ord.id.clone()).await {
                    Ok(_)  => info!("🧹 [{}] cancelled stale resting order {} ({})", ord.strategy, ord.id, ord.market),
                    Err(e) => warn!("[{}] stale cancel failed for {} ({}): {e}", ord.strategy, ord.id, ord.market),
                }
                clear_guard(positions, &ord.strategy, &ord.market).await;
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
        // neither held nor still resting is directionally exposed → flatten it.
        let orphans: Vec<(String, MarketId, Decimal)> = {
            let map = positions.lock().await;
            map.iter()
                .filter_map(|((s, t), p)| {
                    let partner = p.paired_leg_token_id.as_ref()?;
                    let i_held          = held.get(t.as_str()).copied().unwrap_or_default() > Decimal::ZERO;
                    let partner_held    = held.get(partner.as_str()).copied().unwrap_or_default() > Decimal::ZERO;
                    let partner_resting = resting_tokens.contains(partner.as_str());
                    if p.fill_confirmed_at.is_some() && i_held && !partner_held && !partner_resting {
                        Some((s.clone(), t.clone(), p.shares))
                    } else {
                        None
                    }
                })
                .collect()
        };
        for (strategy, token, shares) in orphans {
            warn!("🛡️ [{strategy}] naked leg: {token} filled but partner neither filled nor resting — flattening {shares}");
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
                Ok(f)  => info!("🛡️ [{strategy}] flattened naked leg {token} (order {})", f.order_id),
                Err(e) => warn!("[{strategy}] flatten of {token} failed: {e} — will retry next reconcile"),
            }
            // Clear the guard so we don't re-flatten before the sell settles.
            clear_guard(positions, &strategy, &token).await;
        }
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
            for ((_, t), p) in map.iter_mut() {
                if t == market && p.fill_confirmed_at.is_none() {
                    p.fill_confirmed_at = Some(Utc::now());
                }
            }
        }
        self.tracked.lock().await.retain(|o| &o.market != market);
    }
}

/// Mark a strategy's position guard fill-confirmed (idempotent).
async fn confirm_guard(positions: &Arc<Mutex<PositionMap>>, strategy: &str, token: &MarketId) {
    if let Some(p) = positions.lock().await.get_mut(&(strategy.to_string(), token.clone())) {
        if p.fill_confirmed_at.is_none() {
            p.fill_confirmed_at = Some(Utc::now());
            info!("✅ [{strategy}] fill confirmed: {token}");
        }
    }
}

/// Drop a strategy's position guard for a token (so the viper may re-enter).
async fn clear_guard(positions: &Arc<Mutex<PositionMap>>, strategy: &str, token: &MarketId) {
    positions.lock().await.remove(&(strategy.to_string(), token.clone()));
}

