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
}

/// A venue-neutral open position snapshot.
#[derive(Clone, Debug)]
pub struct Position {
    pub market: MarketId,
    pub shares: Decimal,
    pub avg_price: Decimal,
}

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
}

