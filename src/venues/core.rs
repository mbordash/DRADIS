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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    /// Foundation for the shared `OrderLifecycle` (Option C). Default returns an
    /// empty set so a venue that has not yet wired its open-orders query compiles
    /// and degrades to positions-poll reconciliation rather than failing.
    async fn open_orders(&self) -> Result<Vec<OpenOrder>> {
        Ok(Vec::new())
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
