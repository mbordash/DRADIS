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

//! Venue-neutral execution contract.
//!
//! This module is the single call surface every venue implements. It contains
//! **no** venue-specific types — no signers, nonces, EIP-712 domains, `U256`
//! token IDs, or HMAC state. Each concrete venue (`intl`, `us`, future `kalshi`)
//! holds its own security/identity machinery privately and maps it onto these
//! neutral types at its boundary.
//!
//! See `docs/VENUE_ABSTRACTION.md` (decisions D3–D5) for the rationale.

use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// ─── Venue-neutral identifier (D5) ──────────────────────────────────────────

/// Venue-neutral market identifier.
///
/// Intl encodes an on-chain ERC-1155 token ID as its decimal-`U256` string;
/// US uses a custodial UUID/slug. A newtype string erases venue identity so the
/// rest of DRADIS never learns whether the underlying scheme is on-chain or web2.
/// `U256` knowledge stays private to `venues::intl`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketId(String);

impl MarketId {
    /// Wrap a venue-native identifier string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow the raw identifier string (decimal-`U256` for intl, UUID/slug for US).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for MarketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for MarketId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Venue-neutral order handle returned by a venue and used to cancel.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderId(pub String);

impl std::fmt::Display for OrderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ─── Neutral order primitives ───────────────────────────────────────────────

/// Order direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Side {
    Buy,
    Sell,
}

/// Time-in-force / resting semantics, venue-neutral.
///
/// `Gtc`/`Gtd` rest on the book (batchable); `Fak`/`Fok` are immediate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeInForce {
    /// Good-til-cancelled (resting maker).
    Gtc,
    /// Good-til-date (resting maker with expiry).
    Gtd,
    /// Fill-and-kill (immediate, partial allowed).
    Fak,
    /// Fill-or-kill (immediate, all-or-nothing).
    Fok,
}

/// A venue-neutral order request.
///
/// Carries only what every venue can act on. Venue-specific concerns
/// (verifying contract, signature type, neg-risk exchange routing) are derived
/// internally by each venue from `market`/`is_neg_risk` — never surfaced here.
#[derive(Clone, Debug)]
pub struct OrderIntent {
    pub market: MarketId,
    pub side: Side,
    pub quantity: Decimal,
    pub price: Decimal,
    pub tif: TimeInForce,
    /// Reject (rather than cross) if the order would take liquidity.
    pub post_only: bool,
    /// Expiry horizon in seconds for `Gtd`; `0` for non-expiring orders.
    pub expiration_secs: u64,
    /// Whether the market uses negative-risk pricing (intl exchange routing hint).
    pub is_neg_risk: bool,
    /// Fee rate in basis points (echoed for venues that require it).
    pub fee_bps: u16,
}

/// The outcome of a placed order.
#[derive(Clone, Debug)]
pub struct Fill {
    pub order_id: OrderId,
    pub market: MarketId,
    /// Quantity acknowledged by the venue.
    pub filled: Decimal,
    /// Price at which the order was placed/filled.
    pub price: Decimal,
    /// Total fee charged for this fill, in dollars (not per contract).
    ///
    /// `Decimal::ZERO` when the venue does not report one. Kalshi's quadratic
    /// taker fee is material — ~7% of notional per round trip on a mid-priced
    /// contract — so it has to reach recorded P&L rather than being absorbed
    /// silently into the collateral balance.
    pub fee: Decimal,
}

/// A venue-neutral open position snapshot.
#[derive(Clone, Debug)]
pub struct Position {
    pub market: MarketId,
    pub shares: Decimal,
    pub avg_price: Decimal,
}

/// What a venue says about a market whose position has left the account.
///
/// Every venue's settlement sweep answers the same question — "did this leg
/// settle, and at what final value?" — from its own authority: Polymarket
/// International asks Gamma for resolved outcome prices, Kalshi asks the
/// exchange's market `result`, Polymarket US asks the gateway's market status.
/// Three states, and collapsing any two of them misbooks real money:
///
///   * Resolved  + NotClosed collapsed → an off-strategy SALE booked at $1.00
///     or $0.00 instead of its actual sale price, and the row then deleted so
///     nothing can correct it.
///   * Resolved  + Unknown collapsed → a guess booked as a settlement.
///   * NotClosed + Unknown collapsed → an ordinary sale deferred forever, or a
///     transient outage treated as proof the market is still open.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenResolution {
    /// The market resolved and the price is FINAL: $1.00 or $0.00.
    Resolved(Decimal),
    /// The market is verifiably still open. A position cannot have settled,
    /// because settlement only exists after resolution — so it left the account
    /// by a trade, and belongs on the mark-priced reconciliation path, not the
    /// settlement one.
    NotClosed,
    /// No answer: unreachable, unparseable, or closed but not yet carrying a
    /// final price. Never assume anything from this — defer and ask again.
    Unknown,
}

// ─── Settlement probe backoff (Kalshi / Polymarket US) ──────────────────────

/// Base retry delay after a token's first [`TokenResolution::Unknown`] answer.
///
/// Equal to the dashboard sweep cadence on both venues that use this gate, so
/// the FIRST retry is not delayed at all relative to the old behavior — a
/// market that determines within a sweep or two of closing (the normal Kalshi
/// crypto-hourly case) is still booked within a minute of resolution.
#[cfg(any(feature = "kalshi", feature = "us_retail"))]
const PROBE_BASE_DELAY_SECS: u64 = 30;

/// Ceiling on the per-token retry delay, however many misses accumulate.
///
/// Reached after five consecutive `Unknown`s (~13 minutes of no answer), which
/// on a 15-minute Kalshi market means determination is genuinely delayed —
/// exchange review, a voided market — not merely in flight. Ten minutes keeps
/// a stuck row at ~144 venue calls/day instead of 2,880, while still booking
/// an eventually-resolved market within ten minutes of the answer appearing.
/// The 24h [`crate::helpers::db::SETTLEMENT_DEFER_MAX_SECS`] age bound is
/// stretched by at most this much, which it can absorb.
#[cfg(any(feature = "kalshi", feature = "us_retail"))]
const PROBE_MAX_DELAY_SECS: u64 = 600;

#[cfg(any(feature = "kalshi", feature = "us_retail"))]
struct ProbeState {
    /// Earliest instant at which the venue may be asked about this token again.
    next_probe: std::time::Instant,
    /// Consecutive `Unknown` answers so far; drives the exponential delay.
    misses: u32,
    /// Last time any sweep consulted this entry — the prune clock.
    last_touch: std::time::Instant,
}

/// Per-token rate limiter for settlement lookups that keep answering
/// [`TokenResolution::Unknown`].
///
/// Why this exists: `sync_dashboard` on Kalshi and Polymarket US runs every 30
/// seconds AND after every entry and every dispatched fill, and each run makes
/// one live venue API call per stale confirmed row. A row the venue cannot
/// answer for — a Kalshi market closed but not yet determined, a voided
/// market, a transient REST failure — therefore burned ~2,880 calls over its
/// 24h defer window, multiplied by the stale-row population (Kalshi runs seven
/// squadrons against one shard, each sweeping the same pool-wide row set) and
/// by the fill-driven extra sweeps. All of that sits in the trading loop, so a
/// slow venue turned settlement lookups into tick delay. The same per-row loop
/// was fine on the intl chain-sync path because that sweep runs every 300s in
/// a background task; the cost model does not carry to a 30s in-loop tick.
///
/// Mechanism: exponential per-token backoff on consecutive `Unknown` answers
/// (30s, 60s, 120s, … capped at [`PROBE_MAX_DELAY_SECS`]). A token inside its
/// backoff window is DEFERRED without a venue call — exactly the treatment a
/// fresh `Unknown` gets, so nothing is booked or deleted on a skipped probe. A
/// decisive answer (`Resolved`/`NotClosed`) clears the entry, and a token
/// never seen before probes immediately, so the first lookup after a position
/// vanishes — the one that books the normal settlement — is never delayed.
///
/// Deliberately in-memory and NOT the correctness bound: this state dies on
/// restart, which only means one immediate re-probe per deferred token and a
/// rebuilt backoff — more calls, never a missed settlement. The durable bound
/// remains the row-age check against `SETTLEMENT_DEFER_MAX_SECS` in the
/// sweeps, precisely because attempt counters that die on restart were
/// rejected as a bound when the age check was designed.
///
/// Process-wide by design: the seven Kalshi squadrons share one SQLite shard,
/// so each sweeps the same stale rows. A shared gate means the first squadron
/// to probe a token backs the other six off too.
#[cfg(any(feature = "kalshi", feature = "us_retail"))]
pub struct SettlementProbeGate {
    inner: std::sync::Mutex<std::collections::HashMap<String, ProbeState>>,
}

#[cfg(any(feature = "kalshi", feature = "us_retail"))]
impl Default for SettlementProbeGate {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(feature = "kalshi", feature = "us_retail"))]
impl SettlementProbeGate {
    pub fn new() -> Self {
        Self { inner: std::sync::Mutex::new(std::collections::HashMap::new()) }
    }

    /// May the venue be asked about `token` right now?
    ///
    /// `true` for a token with no recorded miss (first sight probes
    /// immediately) or whose backoff has expired. `false` means: treat the
    /// token exactly as a fresh `Unknown` — defer it, book nothing, delete
    /// nothing — and ask again on a later sweep.
    pub fn should_probe(&self, token: &str) -> bool {
        self.should_probe_at(token, std::time::Instant::now())
    }

    fn should_probe_at(&self, token: &str, now: std::time::Instant) -> bool {
        let mut map = self.inner.lock().unwrap();
        match map.get_mut(token) {
            Some(st) if now < st.next_probe => {
                st.last_touch = now;
                false
            }
            // Expired or absent — probe. The entry (and its miss count) is kept
            // so a chronically unanswerable token continues its long delays
            // instead of restarting from 30s on every successful skip cycle.
            _ => true,
        }
    }

    /// The venue answered `Unknown` for `token`: extend its backoff.
    pub fn record_unknown(&self, token: &str) {
        self.record_unknown_at(token, std::time::Instant::now());
    }

    fn record_unknown_at(&self, token: &str, now: std::time::Instant) {
        let mut map = self.inner.lock().unwrap();
        // Opportunistic prune, piggybacked here because misses are the only
        // path that grows the map. Entries can outlive their row — the row
        // ages out at SETTLEMENT_DEFER_MAX_SECS and is reconciled away, or the
        // token goes live again — so anything untouched for the full defer
        // bound is dead weight. The map is tiny (bounded by the deferred-row
        // population) so a linear scan costs nothing.
        let idle_bound =
            std::time::Duration::from_secs(crate::helpers::db::SETTLEMENT_DEFER_MAX_SECS as u64);
        map.retain(|_, st| now.duration_since(st.last_touch) < idle_bound);

        let st = map.entry(token.to_string()).or_insert(ProbeState {
            next_probe: now,
            misses: 0,
            last_touch: now,
        });
        st.misses = st.misses.saturating_add(1);
        // 30s · 2^(misses−1), capped. checked_shl saturates a huge shift to the
        // cap rather than wrapping to a tiny delay.
        let delay = PROBE_BASE_DELAY_SECS
            .checked_shl(st.misses - 1)
            .unwrap_or(u64::MAX)
            .min(PROBE_MAX_DELAY_SECS);
        st.next_probe = now + std::time::Duration::from_secs(delay);
        st.last_touch = now;
    }

    /// The venue gave a decisive answer (`Resolved`/`NotClosed`) for `token`,
    /// or the token is live at the venue again: forget its backoff, so a later
    /// stale spell starts from an immediate probe.
    pub fn record_decisive(&self, token: &str) {
        self.inner.lock().unwrap().remove(token);
    }
}

#[cfg(all(test, any(feature = "kalshi", feature = "us_retail")))]
mod probe_gate_tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn a_token_never_probed_before_is_probed_immediately() {
        let gate = SettlementProbeGate::new();
        assert!(gate.should_probe_at("tok", Instant::now()));
    }

    #[test]
    fn an_unknown_answer_blocks_reprobing_until_the_backoff_expires() {
        let gate = SettlementProbeGate::new();
        let t0 = Instant::now();
        gate.record_unknown_at("tok", t0);
        assert!(!gate.should_probe_at("tok", t0 + Duration::from_secs(1)));
        assert!(!gate.should_probe_at("tok", t0 + Duration::from_secs(PROBE_BASE_DELAY_SECS - 1)));
        assert!(gate.should_probe_at("tok", t0 + Duration::from_secs(PROBE_BASE_DELAY_SECS)));
    }

    #[test]
    fn consecutive_unknowns_double_the_backoff_up_to_the_cap() {
        let gate = SettlementProbeGate::new();
        let t0 = Instant::now();
        // Miss n leaves a delay of 30·2^(n−1)s, saturating at the cap.
        let mut now = t0;
        let mut expected = PROBE_BASE_DELAY_SECS;
        for _ in 0..8 {
            gate.record_unknown_at("tok", now);
            assert!(!gate.should_probe_at("tok", now + Duration::from_secs(expected - 1)));
            assert!(gate.should_probe_at("tok", now + Duration::from_secs(expected)));
            now += Duration::from_secs(expected);
            expected = (expected * 2).min(PROBE_MAX_DELAY_SECS);
        }
        // Well past saturation the delay must still be exactly the cap.
        gate.record_unknown_at("tok", now);
        assert!(!gate.should_probe_at("tok", now + Duration::from_secs(PROBE_MAX_DELAY_SECS - 1)));
        assert!(gate.should_probe_at("tok", now + Duration::from_secs(PROBE_MAX_DELAY_SECS)));
    }

    #[test]
    fn a_huge_miss_count_saturates_instead_of_wrapping_to_a_tiny_delay() {
        // 30 << 63 would overflow; the shift must saturate to the cap, because a
        // wrapped delay of ~0s would silently restore the very polling storm
        // this gate exists to prevent.
        let gate = SettlementProbeGate::new();
        let t0 = Instant::now();
        {
            let mut map = gate.inner.lock().unwrap();
            map.insert("tok".into(), ProbeState { next_probe: t0, misses: u32::MAX - 1, last_touch: t0 });
        }
        gate.record_unknown_at("tok", t0);
        assert!(!gate.should_probe_at("tok", t0 + Duration::from_secs(PROBE_MAX_DELAY_SECS - 1)));
        assert!(gate.should_probe_at("tok", t0 + Duration::from_secs(PROBE_MAX_DELAY_SECS)));
    }

    #[test]
    fn a_decisive_answer_clears_the_backoff_entirely() {
        let gate = SettlementProbeGate::new();
        let t0 = Instant::now();
        gate.record_unknown_at("tok", t0);
        gate.record_unknown_at("tok", t0);
        gate.record_decisive("tok");
        // Immediately probeable again, and the miss count restarted: one new
        // miss yields the BASE delay, not a continuation of the doubling.
        assert!(gate.should_probe_at("tok", t0));
        gate.record_unknown_at("tok", t0);
        assert!(gate.should_probe_at("tok", t0 + Duration::from_secs(PROBE_BASE_DELAY_SECS)));
    }

    #[test]
    fn entries_idle_past_the_defer_bound_are_pruned() {
        let gate = SettlementProbeGate::new();
        let t0 = Instant::now();
        gate.record_unknown_at("stale-row-token", t0);
        // A different token missing a day later triggers the piggybacked prune.
        let later = t0 + Duration::from_secs(
            crate::helpers::db::SETTLEMENT_DEFER_MAX_SECS as u64 + 1,
        );
        gate.record_unknown_at("other", later);
        assert!(!gate.inner.lock().unwrap().contains_key("stale-row-token"));
        assert!(gate.inner.lock().unwrap().contains_key("other"));
    }

    #[test]
    fn one_tokens_backoff_never_delays_a_different_token() {
        let gate = SettlementProbeGate::new();
        let t0 = Instant::now();
        gate.record_unknown_at("stuck", t0);
        assert!(gate.should_probe_at("fresh", t0));
    }
}

// ─── Order-lifecycle primitives (Option C foundation) ───────────────────────

/// A venue-neutral snapshot of a resting/working order, as reported by the venue.
///
/// This is the input the shared `OrderLifecycle` manager consumes (alongside
/// [`Position`]) to confirm fills, cancel stale legs, and re-hedge/flatten naked
/// legs — uniformly across venues. Intl sources it from the CLOB user feed /
/// open-orders query; US from `/v1/trading/orders`. The manager never learns
/// which scheme produced it.
#[derive(Clone, Debug)]
pub struct OpenOrder {
    pub order_id: OrderId,
    pub market: MarketId,
    pub side: Side,
    pub price: Decimal,
    /// Quantity originally requested when the order was placed.
    pub original_qty: Decimal,
    /// Quantity filled so far (cumulative). `original_qty - filled_qty` still rests.
    pub filled_qty: Decimal,
    pub tif: TimeInForce,
    /// The hedge-partner market for a paired arb leg, when the venue/tracker knows it.
    pub pair_market: Option<MarketId>,
}

impl OpenOrder {
    /// Quantity still resting on the book (never negative).
    pub fn remaining_qty(&self) -> Decimal {
        (self.original_qty - self.filled_qty).max(Decimal::ZERO)
    }

    /// Whether this order rests on the book (maker) rather than being immediate.
    pub fn is_resting(&self) -> bool {
        matches!(self.tif, TimeInForce::Gtc | TimeInForce::Gtd)
    }
}

/// A venue-neutral fill notification pushed by a venue's event feed.
///
/// Lets the shared lifecycle react to fills **event-precisely** instead of at
/// positions-poll granularity. Sourced from intl's chain/user WS or US's
/// `/v1/ws/private`. Venues without an event feed return `None` from
/// [`Execution::subscribe_fills`] and the lifecycle falls back to polling.
#[derive(Clone, Debug)]
pub struct FillEvent {
    pub order_id: OrderId,
    pub market: MarketId,
    pub side: Side,
    /// Cumulative quantity filled for this order at the time of the event.
    pub filled: Decimal,
    pub price: Decimal,
    /// `true` once the order is fully filled or otherwise closed by the venue.
    pub complete: bool,
}

/// Receiver half of a venue's fan-out fill-event feed.
///
/// A `broadcast` channel so multiple lifecycle consumers can observe the same
/// stream; the venue owns the `Sender` and pumps it from its internal WS task.
pub type FillStream = tokio::sync::broadcast::Receiver<FillEvent>;

// ─── The contract (D4: no signer/nonce/EIP-712 in any signature) ────────────

/// Compile-time execution contract every venue implements.
///
/// Selected at build time via `ActiveVenue` (static dispatch, no `dyn`), so the
/// unused venue's dependencies are stripped from the binary.
#[async_trait]
pub trait Execution: Send + Sync {
    /// Place a single order, returning its fill acknowledgement.
    async fn place_order(&self, intent: OrderIntent) -> Result<Fill>;

    /// Place two legs in a single round-trip (network-atomic, not engine-atomic).
    async fn place_atomic(&self, legs: [OrderIntent; 2]) -> Result<[Fill; 2]>;

    /// Cancel a resting order by id.
    async fn cancel(&self, id: OrderId) -> Result<()>;

    /// Available collateral (settlement currency) in venue units.
    async fn collateral(&self) -> Result<Decimal>;

    /// Currently held positions, as reported by the venue.
    async fn positions(&self) -> Result<Vec<Position>>;

    /// Currently resting/working orders, as reported by the venue.
    ///
    /// Foundation for the shared `OrderLifecycle` (Option C). The default is an
    /// ERROR, not an empty list, and the distinction is load-bearing: "this venue
    /// holds no resting orders" and "this venue cannot tell you" are different
    /// answers, and only one of them is safe to act on.
    ///
    /// The default used to be `Ok(vec![])`. Every reconciliation caller unwraps to
    /// default and so degrades identically either way — but the startup order
    /// sweep reads the result as fact, and on Polymarket US (which has never
    /// implemented this) it logged "no leftover orders from a previous session"
    /// having checked nothing at all. A sweep that cannot see is worse than no
    /// sweep, because it reports success.
    async fn open_orders(&self) -> Result<Vec<OpenOrder>> {
        anyhow::bail!("this venue does not implement open_orders()")
    }

    /// Subscribe to the venue's fill-event feed, if it has one.
    ///
    /// Returning `Some(stream)` lets the shared lifecycle confirm fills
    /// event-precisely; `None` (the default) signals no event feed, so the
    /// lifecycle falls back to polling [`open_orders`](Self::open_orders) /
    /// [`positions`](Self::positions).
    fn subscribe_fills(&self) -> Option<FillStream> {
        None
    }

    /// Best ask price currently resting on `market`'s book, if the venue can
    /// report it cheaply.
    ///
    /// Consulted by the shared lifecycle's naked-leg handler to decide whether
    /// an economical re-hedge (buy the missing partner leg) beats a forced
    /// flatten. The default `None` means "book unknown" — the lifecycle then
    /// falls straight through to the flatten path, so venues without a quote
    /// surface keep today's behavior.
    async fn best_ask(&self, _market: &MarketId) -> Result<Option<Decimal>> {
        Ok(None)
    }

    /// Best bid currently resting on `market`'s book, if the venue can report it
    /// cheaply. The sibling of [`Execution::best_ask`].
    ///
    /// Helm needs both ends to validate a posture before the operator commits:
    /// a stop is checked against the bid it would sell into, and an entry
    /// against the ask it would lift. `None` means "book unknown", which Helm
    /// treats as a refusal rather than a zero — a zero bid reads as a real price
    /// to anything pricing an exit.
    /// No default, deliberately: a venue that cannot answer this must say so in
    /// code rather than inherit a silent `None`. `market_facts` is the same.
    /// A source-scanning test used to guard this and was worthless — it passed on
    /// a comment, and it passed on Polymarket US while a Helm squadron could not
    /// be deployed there at all. The compiler is the right guard.
    ///
    /// `best_ask` keeps its default on purpose: see `quote_both`.
    async fn best_bid(&self, market: &MarketId) -> Result<Option<Decimal>>;

    /// Both ends of `market`'s book, for a validator rather than a trader.
    ///
    /// Distinct from `best_bid` + `best_ask` because `best_ask` is load-bearing
    /// elsewhere: `venues::lifecycle` gates a FAK BUY of a missing partner leg on
    /// it returning `Some`, so a venue that already re-hedges by another route
    /// must keep `best_ask`'s `None` default or arm a second one. Polymarket
    /// International is exactly that venue. A validator that only reads prices
    /// has no business being coupled to that decision, so it asks here.
    ///
    /// The default composes the two, which is correct for any venue where
    /// `best_ask` is safe to implement. Override it to fetch both in one round
    /// trip, or to supply an ask where the trait's `best_ask` deliberately does
    /// not.
    async fn quote_both(&self, market: &MarketId) -> Result<(Option<Decimal>, Option<Decimal>)> {
        let (bid, ask) = (self.best_bid(market).await?, self.best_ask(market).await?);
        Ok((bid, ask))
    }

    /// The YES and NO tokens of `market`, its question, and when it resolves.
    ///
    /// Venue-neutral because every venue identifies a market its own way — an
    /// on-chain condition id, a slug, a series ticker — and Helm must validate a
    /// posture against the close time without knowing which. `None` means the
    /// venue could not resolve it, which is a refusal: an intent whose market
    /// cannot be read is one whose posture cannot be checked.
    async fn market_facts(&self, market: &MarketId) -> Result<Option<MarketFacts>>;

    // ── Operator read surfaces ─────────────────────────────────────────────
    //
    // The Markets page shows a market the way the venue's own UI would. These
    // are read surfaces for a human and nothing in the trading lifecycle
    // consults them, so the `best_ask` warning above does not apply: a venue
    // that publishes none of this returns `Ok(None)`, which the page renders as
    // "not published by this venue", and a failed read is an `Err`, which is a
    // different fact (the venue publishes it and we could not get it).

    /// Resting depth for one leg, bids best-first and asks best-first.
    async fn order_book(&self, _leg: &MarketId) -> Result<Option<LegBook>> {
        Ok(None)
    }

    /// The last `limit` prints on the MARKET, both legs, newest first.
    async fn recent_prints(&self, _market: &MarketId, _limit: usize) -> Result<Option<Vec<TapePrint>>> {
        Ok(None)
    }

    /// One leg's price series since `since`, at the venue's finest public cadence.
    async fn price_history(
        &self,
        _leg: &MarketId,
        _since: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Vec<PricePoint>>> {
        Ok(None)
    }

    /// How one leg resolved, where the venue says. `Unknown` is the honest
    /// default and the page shows "awaiting resolution" for it.
    async fn resolution(&self, _leg: &MarketId) -> Result<TokenResolution> {
        Ok(TokenResolution::Unknown)
    }
}

/// One resting level of a leg's book.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BookLevel {
    pub price: Decimal,
    pub size: Decimal,
}

/// One leg's resting depth. Bids are sorted best (highest) first, asks best
/// (lowest) first, whatever order the venue sent them in.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct LegBook {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

impl LegBook {
    /// Build from raw `(price, size)` pairs, sorting each side best-first and
    /// dropping empty or non-positive levels.
    pub fn from_levels(bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) -> Self {
        let clean = |v: Vec<(Decimal, Decimal)>| -> Vec<BookLevel> {
            v.into_iter()
                .filter(|(p, q)| *p > Decimal::ZERO && *p < Decimal::ONE && *q > Decimal::ZERO)
                .map(|(price, size)| BookLevel { price, size })
                .collect()
        };
        let mut bids = clean(bids);
        let mut asks = clean(asks);
        bids.sort_by(|a, b| b.price.cmp(&a.price));
        asks.sort_by(|a, b| a.price.cmp(&b.price));
        Self { bids, asks }
    }

    /// The other leg's book seen through this one: a bid for YES at p is an
    /// ask for NO at 1 - p, and the reverse. Binary venues that publish one
    /// side's book (Polymarket US, Kalshi's yes/no levels) derive the other
    /// leg this way.
    pub fn mirrored(&self) -> Self {
        let flip = |v: &[BookLevel]| -> Vec<(Decimal, Decimal)> {
            v.iter().map(|l| (Decimal::ONE - l.price, l.size)).collect()
        };
        Self::from_levels(flip(&self.asks), flip(&self.bids))
    }
}

/// One print from the venue's public tape.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TapePrint {
    pub at: chrono::DateTime<chrono::Utc>,
    /// The taker's side: `Buy` lifted an ask, `Sell` hit a bid.
    pub taker_side: Side,
    /// Which leg printed; the price is that leg's.
    pub leg_is_yes: bool,
    pub price: Decimal,
    pub size: Decimal,
}

/// One point of a leg's public price series.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PricePoint {
    pub at: chrono::DateTime<chrono::Utc>,
    pub price: Decimal,
}

/// What a venue can say about one market, in terms no venue owns.
///
/// Deliberately the minimum Helm's posture validator needs rather than a general
/// market record: the two tokens it may hold, a name to show the operator, and
/// the close time a time limit has to sit inside.
#[derive(Clone, Debug)]
pub struct MarketFacts {
    /// The venue's own id for this market, echoed back.
    ///
    /// Carried so a caller can prove the facts describe the market it asked
    /// about. Squadron ids are deliberately timestamp-free and reused across
    /// redeploys (`squadron/mod.rs`), so anything that caches or passes these
    /// facts around must compare this against the market it meant, or a
    /// redeployed squadron silently validates against its predecessor's book.
    pub market_id: MarketId,
    /// The question as the venue words it, for the operator's own record.
    pub question: String,
    /// The venue's own resolution criteria, verbatim, or empty where it publishes
    /// none.
    ///
    /// A question is often not enough to form a conviction on: "Bitcoin Up or Down
    /// on October 3?" names no reference price, because the market compares two
    /// timestamps rather than quoting a strike. The criteria say which candle, on
    /// which exchange, at which two times — which is the difference between an
    /// informed conviction and a guess about what the question means.
    pub criteria: String,
    /// The two tokens, in the venue's own ordering.
    pub yes_token: MarketId,
    pub no_token: MarketId,
    /// What the venue calls each side, when "YES" and "NO" would mislead.
    ///
    /// On Polymarket International a binary market really is a yes/no question.
    /// On Polymarket US the two legs of one book are `#long` and `#short`, which
    /// for a game market are the two TEAMS — so an operator told they are buying
    /// "NO" has been told something false. `None` means yes/no is accurate.
    pub leg_labels: Option<(String, String)>,
    /// When the market resolves, taken as the EARLIEST credible close the venue
    /// publishes.
    ///
    /// `None` where the venue does not publish one it can be held to, which
    /// forbids a hold-to-settlement posture rather than guessing a date. That is
    /// deliberate: a `Some` that is too late makes "the time limit sits inside
    /// the close" pass when it should fail, and the position is then held past a
    /// market that has already resolved.
    pub close_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Whether the venue currently accepts orders on this market, where it says.
    ///
    /// `close_time` is the EARLIEST credible close, and on a sports market that
    /// is kick-off: Polymarket's `endDate` for "Will Arsenal FC win on
    /// 2026-10-10?" is the start of the match, and the book trades on through
    /// the game. A reader judging "live" from `close_time` alone therefore shows
    /// a match in play as closed. This is the venue's own answer
    /// (`acceptingOrders` and `closed` on Polymarket International, `status` on
    /// Kalshi, `active` and `closed` on Polymarket US); `None` where the venue
    /// publishes nothing, in which case `close_time` is all there is.
    pub accepting_orders: Option<bool>,
}


/// Read the session's starting collateral, retrying until the venue answers.
///
/// A wing that cannot read its balance cannot compute session P&L or its
/// drawdown limit, so treating a failed read as "starting from zero" is the one
/// answer that is certainly wrong. It was also the previous behavior
/// (`collateral().await.unwrap_or(Decimal::ZERO)`), and it poisons the whole
/// session rather than one tick: `session_pnl = total - starting` then reports
/// the entire balance as profit, for the lifetime of the process.
///
/// That is not hypothetical. Several engine restarts in quick succession trip
/// the Polymarket US rate limiter on this exact probe; every snapshot afterwards
/// recorded a $120 balance as $120 of session profit, which the portfolio chart
/// summed across three wings and the LLM advisor read as a winning session.
///
/// Retries with a fixed delay rather than failing: the venue being briefly
/// unreachable at startup is ordinary, and waiting is strictly better than
/// trading against a baseline known to be false. Returns `None` only if
/// cancelled, in which case the caller is shutting down anyway.
pub async fn starting_collateral<V: Execution + ?Sized>(
    venue: &V,
    cancel: &tokio_util::sync::CancellationToken,
    retry_secs: u64,
) -> Option<Decimal> {
    let mut attempt: u32 = 0;
    loop {
        if cancel.is_cancelled() {
            return None;
        }
        match venue.collateral().await {
            Ok(c) => {
                if attempt > 0 {
                    tracing::info!("💰 Starting collateral resolved after {attempt} retries: ${c:.2}");
                }
                return Some(c);
            }
            Err(e) => {
                attempt += 1;
                // Warn once, then stay quiet — a venue outage should not fill
                // the log with one line per retry for as long as it lasts.
                if attempt == 1 {
                    tracing::warn!(
                        "💰 Starting collateral unavailable ({e}) — retrying every {retry_secs}s. \
                         Trading is held until the baseline is known; a wrong baseline would \
                         misreport session P&L and the drawdown limit for the whole session."
                    );
                }
                tokio::select! {
                    _ = cancel.cancelled() => return None,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(retry_secs)) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod leg_book_tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// Venues send levels in their own order; the page relies on best-first.
    #[test]
    fn levels_are_sorted_best_first_and_junk_is_dropped() {
        let b = LegBook::from_levels(
            vec![(dec!(0.50), dec!(10)), (dec!(0.55), dec!(5)), (dec!(0), dec!(9)), (dec!(0.52), dec!(0))],
            vec![(dec!(0.60), dec!(1)), (dec!(0.57), dec!(2)), (dec!(1), dec!(3))],
        );
        assert_eq!(b.bids.iter().map(|l| l.price).collect::<Vec<_>>(), vec![dec!(0.55), dec!(0.50)]);
        assert_eq!(b.asks.iter().map(|l| l.price).collect::<Vec<_>>(), vec![dec!(0.57), dec!(0.60)]);
    }

    /// A YES bid at 0.55 is a NO ask at 0.45, and a YES ask at 0.57 a NO bid at 0.43.
    #[test]
    fn the_no_leg_book_is_the_yes_book_mirrored_through_one() {
        let yes = LegBook::from_levels(vec![(dec!(0.55), dec!(5)), (dec!(0.50), dec!(10))], vec![(dec!(0.57), dec!(2))]);
        let no = yes.mirrored();
        assert_eq!(no.bids, vec![BookLevel { price: dec!(0.43), size: dec!(2) }]);
        assert_eq!(no.asks, vec![BookLevel { price: dec!(0.45), size: dec!(5) }, BookLevel { price: dec!(0.50), size: dec!(10) }]);
        assert_eq!(no.mirrored(), yes, "mirroring twice is the identity");
    }
}
