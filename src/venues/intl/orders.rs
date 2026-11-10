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

//! Intl CLOB order placement helpers (EIP-712 signing over Polygon).
//!
//! Moved here from `helpers/orders.rs` as part of the venue-abstraction seam
//! (see `docs/VENUE_ABSTRACTION.md`, Step 1). The self-custody signing machinery
//! is venue-specific and now lives under `venues::intl`. `helpers::orders`
//! re-exports these symbols for backward compatibility with existing call sites.
//!
//! Provides generic order placement functions that work with any strategy,
//! token, or direction (buy/sell), including an atomic two-leg batch variant
//! that uses Polymarket's `/orders` endpoint to submit both legs simultaneously.

use anyhow::Result;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::timeout;

use super::trading_client::TradingClient;
use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::LocalSigner;
use chrono::Utc;
use polymarket_client_sdk_v2::clob::types::response::PostOrderResponse;
use polymarket_client_sdk_v2::clob::types::{
    Order, OrderPayload, OrderType, Side, SignableOrder, SignedOrder,
};
use rust_decimal::Decimal;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

use super::wallet::WalletConfig;
use crate::helpers::nonce::fetch_next_nonce;
use crate::helpers::price::{floor_to_tick_size, round_to_tick_size};
use crate::venues::core::MarketId;
use crate::venues::core::TimeInForce;
use crate::venues::intl::u256_from_market_id;
use rust_decimal::prelude::ToPrimitive;

/// Polymarket requires expiration timestamp to be >= now + 1 minute + 30 seconds.
/// We add 90 seconds (1.5 minutes) as a safety buffer.
const EXPIRATION_BUFFER_SECS: u64 = 90;

fn order_wallet(
    client: &TradingClient,
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    supplied_funder: Address,
    supplied_eoa: Address,
) -> Result<WalletConfig> {
    let wallet = client.wallet();
    anyhow::ensure!(
        wallet.signer == supplied_eoa,
        "supplied EOA differs from immutable client wallet"
    );
    anyhow::ensure!(
        wallet.funder == supplied_funder
            && wallet.signer == signer.address()
            && wallet.signer == client.address(),
        "Order wallet does not match the authenticated signer and configured funder"
    );
    Ok(wallet)
}

#[derive(Clone, Debug)]
pub struct OrderReceipt {
    pub order_id: String,
    pub signed_quantity: Decimal,
    pub making_amount: Decimal,
    pub taking_amount: Decimal,
}

#[derive(Debug)]
pub struct PartialBatchError {
    pub legs: [Option<OrderReceipt>; 2],
}
impl std::fmt::Display for PartialBatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CLOB batch partially accepted; accepted legs require reconciliation"
        )
    }
}
impl std::error::Error for PartialBatchError {}

pub fn signed_quantity(order: &SignedOrder) -> Result<Decimal> {
    use std::str::FromStr;
    let value = order.order();
    let amount = if value.side == Side::Buy as u8 {
        value.takerAmount
    } else {
        value.makerAmount
    };
    Ok(Decimal::from_str(&amount.to_string())? / Decimal::from(1_000_000u32))
}

fn validate_order_response(response: &PostOrderResponse) -> Result<()> {
    anyhow::ensure!(
        response.success
            && !matches!(
                response.status,
                polymarket_client_sdk_v2::clob::types::OrderStatusType::Unknown(_)
                    | polymarket_client_sdk_v2::clob::types::OrderStatusType::Delayed
                    | polymarket_client_sdk_v2::clob::types::OrderStatusType::Unmatched
            )
            && !response.order_id.trim().is_empty()
            && response
                .error_msg
                .as_deref()
                .is_none_or(|message| message.trim().is_empty()),
        "Order was not accepted: id={} status={} error={}",
        response.order_id,
        response.status,
        response
            .error_msg
            .as_deref()
            .unwrap_or("missing order acceptance")
    );
    Ok(())
}

fn validate_batch_response(responses: &[PostOrderResponse]) -> Result<()> {
    anyhow::ensure!(
        responses.len() == 2,
        "Atomic batch returned {} responses (expected 2)",
        responses.len()
    );
    for (index, response) in responses.iter().enumerate() {
        validate_order_response(response).map_err(|error| {
            anyhow::anyhow!(
                "Batch leg {} failed: {error}; returned order IDs: [{}, {}]",
                index + 1,
                responses[0].order_id,
                responses[1].order_id
            )
        })?;
    }
    Ok(())
}

/// How an order-placement failure should be charged by the caller's fault
/// accounting (the patrol loop's trade cooldown and circuit breaker).
///
/// The patrol used to treat every placement error as the strategy's fault:
/// arm `TRADE_COOLDOWN_SECS` and count one of `MAX_CONSECUTIVE_FAILURES`
/// toward the breaker (with a lone substring carve-out for "crosses book").
/// On 2026-09-01 Polymarket's order manager was down ~18:58:09–19:00:56 and
/// FairValue went 0-for-3 on the day — two of its three entry attempts were
/// the venue's own 425 "order manager not ready, please retry", each billed
/// as a strategy failure against a breaker threshold of 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementFault {
    /// The venue explicitly said it did NOT process the order — the 425
    /// "order manager not ready, please retry" condition. Not the strategy's
    /// fault: the right response is to re-evaluate next tick and enter
    /// naturally once the venue recovers, not to sit out a 60s cooldown or
    /// march toward the breaker.
    VenueUnavailable,
    /// The book moved between pricing and placement: a post-only bid now
    /// "crosses book", or a FAK found "no orders" resting at its limit.
    /// A definitive answer about liquidity, not a malfunction — the next
    /// evaluation reprices from a fresh snapshot.
    BookRace,
    /// The venue failed WITHOUT stating whether the order was processed —
    /// the execution-engine 500 "could not run the execution". Polymarket's
    /// error-codes reference documents not-processed semantics explicitly
    /// where they hold ("order timed out" 500: "rejected before reaching the
    /// order book and can be safely resubmitted"; 425: "Engine restarting —
    /// retry with backoff") and this message is absent from that table, so no
    /// such statement exists for it. Not the strategy's fault, so no cooldown
    /// and no breaker count — but it must NEVER be resent by a retry loop:
    /// every resend is a fresh salt and a distinct order id the venue cannot
    /// dedupe, and if the original request landed, the resend is a second
    /// live order. The next tick re-reads positions and balances, and
    /// chain-sync reconciliation adopts the order if it did land.
    Ambiguous,
    /// Everything else (bad params, balance/allowance, auth, unknown) —
    /// charged to the strategy exactly as before.
    Strategy,
}

/// Classify an intl CLOB placement error for fault accounting.
///
/// Still substring matching underneath — the SDK error is stringly typed once
/// it crosses the `anyhow` boundary — but the substrings live HERE, in the
/// module that wraps the SDK error, instead of scattered as `.contains()`
/// calls through the patrol loop. The SDK's `Status` Display is
/// `error(<code> <reason>) making <method> call to <path> with <body>`, so the
/// status-code form `error(425` is matched as well as the body text, in case
/// the venue ever rewords the message. Matching a bare "425" would be unsafe:
/// token ids and share amounts appear in these strings.
pub fn classify_placement_error(e: &anyhow::Error) -> PlacementFault {
    let es = e.to_string().to_lowercase();
    if es.contains("order manager not ready") || es.contains("error(425") {
        return PlacementFault::VenueUnavailable;
    }
    if es.contains("crosses book") || es.contains("no orders found") {
        return PlacementFault::BookRace;
    }
    if es.contains("could not run the execution") {
        return PlacementFault::Ambiguous;
    }
    PlacementFault::Strategy
}

/// Map the venue-neutral [`TimeInForce`] onto the intl SDK's `OrderType`.
/// Confines the SDK enum to this module — callers speak only `TimeInForce`.
fn to_clob(tif: TimeInForce) -> OrderType {
    match tif {
        TimeInForce::Gtc => OrderType::GTC,
        TimeInForce::Gtd => OrderType::GTD,
        TimeInForce::Fak => OrderType::FAK,
        TimeInForce::Fok => OrderType::FOK,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal: Build a SignedOrder without posting it
// ─────────────────────────────────────────────────────────────────────────────

/// Constructs and signs a single order struct without posting it.
///
/// Encapsulates all EIP-712 construction, amount alignment, and signing so that
/// both `place_limit_order` (single) and `place_limit_orders_atomic` (batch) can
/// reuse the same logic without duplication.
async fn build_signed_order(
    client: &Arc<TradingClient>,
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    wallet: &WalletConfig,
    verifying_contract: Address,
    token_id: U256,
    side: Side,
    quantity: Decimal,
    limit_price: Decimal,
    order_type: OrderType,
    post_only: bool,
    expiration_secs: u64,
) -> Result<SignedOrder> {
    client.verify_ctf_token(token_id).await?;
    anyhow::ensure!(quantity > Decimal::ZERO, "Order quantity must be positive");
    anyhow::ensure!(
        limit_price > Decimal::ZERO && limit_price < Decimal::ONE,
        "Order price must be between zero and one"
    );
    anyhow::ensure!(
        matches!(order_type, OrderType::GTD) == (expiration_secs > 0),
        "GTD orders require expiration; other order types require zero expiration"
    );
    anyhow::ensure!(
        !post_only || matches!(order_type, OrderType::GTC | OrderType::GTD),
        "postOnly is only supported for GTC and GTD orders"
    );
    anyhow::ensure!(
        signer.address() == wallet.signer && client.address() == wallet.signer,
        "Order signer does not match the authenticated wallet"
    );
    // Preserve the trusted market routing supplied by existing callers while
    // letting the SDK choose and sign its supported V2 domain.
    let neg_risk = if verifying_contract == super::exchange_verifying_contract(false) {
        false
    } else if verifying_contract == super::exchange_verifying_contract(true) {
        true
    } else {
        anyhow::bail!("Unsupported V2 exchange verifying contract");
    };
    client.set_neg_risk(token_id, neg_risk);

    // V2: expiration lives in OrderPayload (outside the signed struct).
    let expiration_v2 = if expiration_secs > 0 {
        let now_unix = Utc::now().timestamp() as u64;
        let buffer = expiration_secs.max(EXPIRATION_BUFFER_SECS);
        U256::from(
            now_unix
                .checked_add(buffer)
                .ok_or_else(|| anyhow::anyhow!("Order expiration overflow"))?,
        )
    } else {
        U256::ZERO
    };

    // timestamp_ms: milliseconds since UNIX epoch — required field in V2 order struct
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow::anyhow!("System clock is before the Unix epoch"))?;
    let timestamp_ms = timestamp_ms.as_millis();

    let mut order_struct = Order::default();
    static SALT_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let salt = RandomState::new()
        .hash_one((timestamp_ms, SALT_SEQUENCE.fetch_add(1, Ordering::Relaxed)))
        & ((1_u64 << 53) - 1);
    order_struct.salt = U256::from(salt);
    order_struct.maker = wallet.funder;
    order_struct.signer = wallet.order_signer();
    order_struct.tokenId = token_id;
    order_struct.timestamp = U256::from(timestamp_ms);
    order_struct.metadata = B256::ZERO;
    order_struct.builder = B256::ZERO;

    // Round price to minimum tick size (0.01) to comply with Polymarket validation.
    // For post-only BUY orders (maker bids), floor instead of round to prevent
    // rounding UP from crossing the book (e.g. $0.318 → $0.32 crossing a $0.19 ask).
    let rounded_price = if post_only && side == Side::Buy {
        floor_to_tick_size(limit_price)
    } else {
        round_to_tick_size(limit_price)
    };
    anyhow::ensure!(
        rounded_price > Decimal::ZERO && rounded_price < Decimal::ONE,
        "Rounded order price must be between zero and one"
    );

    // Convert price to integer cents (e.g. 0.63 → 63) for exact arithmetic.
    // Polymarket validates: makerAmount / takerAmount must be an exact multiple of 0.01.
    let price_cents = (rounded_price * Decimal::from(100u32))
        .round()
        .to_u128()
        .unwrap_or(0);

    match side {
        Side::Buy => {
            // makerAmount (USDC you pay)       = max 2dp → in 1e6, divisible by 10000
            // takerAmount (shares you receive) = max 4dp → in 1e6, divisible by 100
            let usdc_cents = quantity
                .checked_mul(rounded_price)
                .and_then(|notional| notional.checked_mul(Decimal::from(100)))
                .ok_or_else(|| anyhow::anyhow!("Order notional exceeds supported precision"))?
                .floor()
                .to_u128()
                .ok_or_else(|| anyhow::anyhow!("Order notional cannot be represented"))?;
            let usdc_cents_aligned = if price_cents > 0 {
                (usdc_cents / price_cents) * price_cents
            } else {
                usdc_cents
            };
            let maker_raw = usdc_cents_aligned
                .checked_mul(10000u128)
                .ok_or_else(|| anyhow::anyhow!("Order maker amount overflow"))?;
            let taker_raw = maker_raw
                .checked_mul(100)
                .ok_or_else(|| anyhow::anyhow!("Order taker amount overflow"))?
                / price_cents;
            order_struct.makerAmount = U256::from(maker_raw);
            order_struct.takerAmount = U256::from(taker_raw);
        }
        Side::Sell => {
            // makerAmount (shares you give)    = max 4dp → in 1e6, divisible by 100
            // takerAmount (USDC you receive)   = max 2dp → in 1e6, divisible by 10000
            let shares_2dp = quantity
                .checked_mul(Decimal::from(100))
                .ok_or_else(|| anyhow::anyhow!("Order quantity exceeds supported precision"))?
                .floor()
                .to_u128()
                .ok_or_else(|| anyhow::anyhow!("Order quantity cannot be represented"))?;
            let maker_raw = shares_2dp
                .checked_mul(10000u128)
                .ok_or_else(|| anyhow::anyhow!("Order maker amount overflow"))?;
            let taker_raw = shares_2dp
                .checked_mul(price_cents)
                .and_then(|amount| amount.checked_mul(100u128))
                .ok_or_else(|| anyhow::anyhow!("Order taker amount overflow"))?;
            order_struct.makerAmount = U256::from(maker_raw);
            order_struct.takerAmount = U256::from(taker_raw);
        }
        _ => return Err(anyhow::anyhow!("Unsupported order side")),
    }

    order_struct.side = side as u8;
    order_struct.signatureType = wallet.signature_type() as u8;
    anyhow::ensure!(
        order_struct.makerAmount > U256::ZERO && order_struct.takerAmount > U256::ZERO,
        "Order amount rounds to zero or exceeds supported precision"
    );

    let payload = OrderPayload::new(order_struct, expiration_v2);
    let signable = SignableOrder::builder()
        .payload(payload)
        .order_type(order_type)
        .post_only(post_only)
        .build();
    client
        .sign(signer, signable)
        .await
        .map_err(|e| anyhow::anyhow!("Signing failed: {e}"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Public: Single-leg order placement
// ─────────────────────────────────────────────────────────────────────────────

/// Generic order placement helper — works for any token, buy/sell, strategy.
///
/// Handles wallet validation, SDK signing, posting to CLOB, and bounded retry
/// only when the venue definitively refused to process the request.
///
/// Returns `(order_id, making_amount, taking_amount)` — the matched amounts come
/// straight from the CLOB `POST /order` response. For a marketable FAK/FOK order
/// these reflect the ACTUAL execution (so callers can compute the real average
/// fill price); for a resting GTC/GTD order they are typically zero until a match
/// occurs. Use [`place_limit_order`] if you only need the order id.
pub async fn place_limit_order_filled(
    client: &Arc<TradingClient>,
    nonce_manager: &Arc<AtomicU64>,
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    safe_address: Address,
    eoa_address: Address,
    verifying_contract: Address,
    token_id: &MarketId,
    side: Side,
    quantity: Decimal,
    limit_price: Decimal,
    _fee_rate_bps: u16,
    order_type: TimeInForce,
    post_only: bool,
    expiration_secs: u64,
    http: &reqwest::Client,
) -> Result<(String, Decimal, Decimal)> {
    let receipt = place_limit_order_receipt(
        client,
        nonce_manager,
        signer,
        safe_address,
        eoa_address,
        verifying_contract,
        token_id,
        side,
        quantity,
        limit_price,
        _fee_rate_bps,
        order_type,
        post_only,
        expiration_secs,
        http,
    )
    .await?;
    Ok((
        receipt.order_id,
        receipt.making_amount,
        receipt.taking_amount,
    ))
}

pub async fn place_limit_order_receipt(
    client: &Arc<TradingClient>,
    nonce_manager: &Arc<AtomicU64>,
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    safe_address: Address,
    eoa_address: Address,
    verifying_contract: Address,
    token_id: &MarketId,
    side: Side,
    quantity: Decimal,
    limit_price: Decimal,
    _fee_rate_bps: u16,
    order_type: TimeInForce,
    post_only: bool,
    expiration_secs: u64,
    http: &reqwest::Client,
) -> Result<OrderReceipt> {
    // Every Polymarket International order passes through here or the batch
    // below, so this is where a retired instance stops trading (E64).
    crate::helpers::migration::refuse_if_retired()?;
    let wallet = order_wallet(client, signer, safe_address, eoa_address)?;
    client.require_mutation_allowed()?;
    // Convert the neutral key to the on-chain id at the venue boundary (slice 2b).
    let token_id = u256_from_market_id(token_id)?;
    // Map the neutral TIF onto the SDK enum once, at the venue boundary.
    let order_type = to_clob(order_type);
    for attempt in 0..2 {
        // AtomicU64 load — kept for API compatibility; V2 orders have no nonce field.
        let _current_nonce = nonce_manager.load(Ordering::SeqCst);

        let signed_order = build_signed_order(
            client,
            signer,
            &wallet,
            verifying_contract,
            token_id,
            side,
            quantity,
            limit_price,
            order_type.clone(),
            post_only,
            expiration_secs,
        )
        .await?;

        let placed_quantity = signed_quantity(&signed_order)?;
        // Hard 12-second timeout: prevents a TCP stall from freezing the tokio::select! arm.
        let post_result = timeout(
            std::time::Duration::from_secs(12),
            client.post_order(signed_order),
        )
        .await;
        let post_result = match post_result {
            Err(_elapsed) => {
                warn!("⚠️ post_order timed out after 12s (attempt {}) — treating as transient failure", attempt + 1);
                return Err(anyhow::anyhow!("Order placement timed out after 12s"));
            }
            Ok(r) => r,
        };
        match post_result {
            Ok(resp) => {
                validate_order_response(&resp)?;
                return Ok(OrderReceipt {
                    order_id: resp.order_id,
                    signed_quantity: placed_quantity,
                    making_amount: resp.making_amount,
                    taking_amount: resp.taking_amount,
                });
            }
            Err(e) => {
                let err_msg = format!("{:?}", e).to_lowercase();
                // "invalid nonce" is a resend that STAYS: it is a
                // content-validation refusal — the venue evaluated the order's
                // fields and rejected them, and an order refused at validation
                // cannot have been accepted, so the resend cannot duplicate.
                // (Definitive by the nature of the error, unlike an engine
                // failure that occurs after acceptance.) In V2 the order
                // struct carries no nonce field, so this branch is expected to
                // be dead; it is kept as a harmless vestige for the V1 shape.
                if err_msg.contains("invalid nonce") && attempt == 0 {
                    warn!("⚠️ Nonce error (unexpected in V2). Re-syncing from API...");
                    if let Some(fresh_nonce) = fetch_next_nonce(http, safe_address).await {
                        nonce_manager.store(fresh_nonce, Ordering::SeqCst);
                        warn!("🔄 Nonce re-synced to {} — retrying order", fresh_nonce);
                    }
                    continue;
                }
                // The execution-engine 500 ("could not run the execution") is
                // deliberately NOT retried here any more. The old resend was
                // written on the belief the error was transient-and-unprocessed,
                // but no evidence supports the unprocessed half: the message
                // appears nowhere in Polymarket's error-codes reference, which
                // states not-processed semantics explicitly where they hold —
                // and a resend from this loop is a fresh salt and a distinct
                // order id (see `build_signed_order`), so if the original
                // request DID land, the resend is a second live order paid for
                // with real money. It now surfaces to the caller, classifies as
                // `PlacementFault::Ambiguous` (no cooldown, no breaker count),
                // and the strategy re-evaluates next tick against re-read
                // balances — with chain-sync reconciliation adopting the order
                // if it did land.
                // 425 "order manager not ready, please retry": the venue has
                // explicitly stated it did not process the order, so ONE resend
                // cannot double-fill. That statement is the entire safety case —
                // each attempt of this loop re-signs with a fresh salt (see
                // `build_signed_order` above), so the venue cannot dedupe a
                // resend by order identity; a timeout or generic 5xx, where the
                // order may have reached the book and the reply been lost, must
                // therefore never take this branch. Covers sub-second blips
                // only: the 2026-09-01 outage ran ~3 minutes, which is the
                // caller's fault-classification problem, not a retry loop's.
                if err_msg.contains("order manager not ready") && attempt == 0 {
                    warn!("⚠️ Order manager not ready (425) — venue did not process the order; retrying in 500ms (attempt {}/2)", attempt + 1);
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                return Err(anyhow::anyhow!("Order placement failed: {}", e));
            }
        }
    }
    Err(anyhow::anyhow!("Max retries reached"))
}

/// Thin wrapper over [`place_limit_order_filled`] for callers that only need the
/// order id (the historical signature). The matched amounts are discarded.
#[allow(clippy::too_many_arguments)]
pub async fn place_limit_order(
    client: &Arc<TradingClient>,
    nonce_manager: &Arc<AtomicU64>,
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    safe_address: Address,
    eoa_address: Address,
    verifying_contract: Address,
    token_id: &MarketId,
    side: Side,
    quantity: Decimal,
    limit_price: Decimal,
    fee_rate_bps: u16,
    order_type: TimeInForce,
    post_only: bool,
    expiration_secs: u64,
    http: &reqwest::Client,
) -> Result<String> {
    let (order_id, _making, _taking) = place_limit_order_filled(
        client,
        nonce_manager,
        signer,
        safe_address,
        eoa_address,
        verifying_contract,
        token_id,
        side,
        quantity,
        limit_price,
        fee_rate_bps,
        order_type,
        post_only,
        expiration_secs,
        http,
    )
    .await?;
    Ok(order_id)
}

// ─────────────────────────────────────────────────────────────────────────────
// Public: Atomic two-leg order placement
// ─────────────────────────────────────────────────────────────────────────────

/// Places two orders (YES + NO legs) in a single HTTP request to `POST /orders`.
///
/// Sending both orders in one round-trip minimises the latency gap between
/// Leg A and Leg B arriving at Polymarket's matching engine — typically reducing
/// the window from ~200-400 ms (two sequential calls) to < 5 ms (batch).
///
/// **Important — NOT server-side atomic:**
/// Polymarket's `/orders` batch endpoint processes each order independently.
/// If Leg A is accepted and Leg B is rejected, Leg A is live on the book.
/// The orphan-accumulation guard in main.rs + the cleanup cycle in cleanup.rs
/// detect and handle this case.  The "atomic" guarantee only holds at the
/// NETWORK level (one TCP round-trip) — not at the matching-engine level.
///
/// **GTC / GTD only:**
/// Only resting limit orders (GTC, GTD) may be submitted via this batch endpoint.
/// FAK/FOK (immediate-or-cancel / fill-or-kill) orders must use the single-order
/// `place_limit_order` → `POST /order` path.  This function enforces that
/// constraint at runtime and returns `Err` if either leg is FAK/FOK.
///
/// Returns `(leg_a_order_id, leg_b_order_id)` on success.
/// Returns `Err` if the HTTP call itself fails (network, auth, server 500).
/// A partial response (< 2 items) is also treated as an error.
#[allow(clippy::too_many_arguments)]
pub async fn place_limit_orders_atomic(
    client: &Arc<TradingClient>,
    nonce_manager: &Arc<AtomicU64>,
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    safe_address: Address,
    eoa_address: Address,
    // ── Leg A ──
    vc_a: Address,
    token_id_a: &MarketId,
    side_a: Side,
    quantity_a: Decimal,
    price_a: Decimal,
    order_type_a: TimeInForce,
    post_only_a: bool,
    expiration_a: u64,
    // ── Leg B ──
    vc_b: Address,
    token_id_b: &MarketId,
    side_b: Side,
    quantity_b: Decimal,
    price_b: Decimal,
    order_type_b: TimeInForce,
    post_only_b: bool,
    expiration_b: u64,
    http: &reqwest::Client,
) -> Result<(String, String)> {
    let [a, b] = place_limit_orders_atomic_filled(
        client,
        nonce_manager,
        signer,
        safe_address,
        eoa_address,
        vc_a,
        token_id_a,
        side_a,
        quantity_a,
        price_a,
        order_type_a,
        post_only_a,
        expiration_a,
        vc_b,
        token_id_b,
        side_b,
        quantity_b,
        price_b,
        order_type_b,
        post_only_b,
        expiration_b,
        http,
    )
    .await?;
    Ok((a.order_id, b.order_id))
}

pub async fn place_limit_orders_atomic_filled(
    client: &Arc<TradingClient>,
    nonce_manager: &Arc<AtomicU64>,
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    safe_address: Address,
    eoa_address: Address,
    // ── Leg A ──
    vc_a: Address,
    token_id_a: &MarketId,
    side_a: Side,
    quantity_a: Decimal,
    price_a: Decimal,
    order_type_a: TimeInForce,
    post_only_a: bool,
    expiration_a: u64,
    // ── Leg B ──
    vc_b: Address,
    token_id_b: &MarketId,
    side_b: Side,
    quantity_b: Decimal,
    price_b: Decimal,
    order_type_b: TimeInForce,
    post_only_b: bool,
    expiration_b: u64,
    http: &reqwest::Client,
) -> Result<[OrderReceipt; 2]> {
    // Convert the neutral keys to on-chain ids at the venue boundary (slice 2b).
    let token_id_a = u256_from_market_id(token_id_a)?;
    let token_id_b = u256_from_market_id(token_id_b)?;
    // ── GTC/GTD-only guard ─────────────────────────────────────────────────────
    // POST /orders (batch) only accepts resting limit orders.  FAK/FOK are
    // immediate orders that Polymarket routes through POST /order (single).
    // Sending them through the batch endpoint would either be silently ignored
    // or cause a server-side rejection that takes BOTH legs down.  Fail fast here
    // so callers are forced to use place_limit_order for taker fills.
    for (label, ot) in [("Leg A", &order_type_a), ("Leg B", &order_type_b)] {
        if matches!(ot, TimeInForce::Fak | TimeInForce::Fok) {
            return Err(anyhow::anyhow!(
                "place_limit_orders_atomic: {} uses {:?} — only GTC/GTD orders \
                 may be batched via POST /orders. Use place_limit_order instead.",
                label,
                ot
            ));
        }
    }
    crate::helpers::migration::refuse_if_retired()?;
    let wallet = order_wallet(client, signer, safe_address, eoa_address)?;
    client.require_mutation_allowed()?;
    // Map the neutral TIFs onto the SDK enum once, past the batch-eligibility guard.
    let order_type_a = to_clob(order_type_a);
    let order_type_b = to_clob(order_type_b);
    for attempt in 0..2 {
        let _current_nonce = nonce_manager.load(Ordering::SeqCst);

        let order_a = build_signed_order(
            client,
            signer,
            &wallet,
            vc_a,
            token_id_a,
            side_a,
            quantity_a,
            price_a,
            order_type_a.clone(),
            post_only_a,
            expiration_a,
        )
        .await?;

        let order_b = build_signed_order(
            client,
            signer,
            &wallet,
            vc_b,
            token_id_b,
            side_b,
            quantity_b,
            price_b,
            order_type_b.clone(),
            post_only_b,
            expiration_b,
        )
        .await?;

        let signed_quantities = [signed_quantity(&order_a)?, signed_quantity(&order_b)?];
        // Slightly longer timeout than single-order to account for batch validation.
        let post_result = timeout(
            std::time::Duration::from_secs(15),
            client.post_orders(vec![order_a, order_b]),
        )
        .await;

        let post_result = match post_result {
            Err(_elapsed) => {
                warn!(
                    "⚠️ post_orders (atomic) timed out after 15s (attempt {})",
                    attempt + 1
                );
                return Err(anyhow::anyhow!(
                    "Atomic order placement timed out after 15s"
                ));
            }
            Ok(r) => r,
        };

        match post_result {
            Ok(resps) => {
                // Each response is independent. Preserve accepted IDs in the
                // error so reconciliation can find an accepted orphan leg.
                if resps.len() != 2 {
                    anyhow::bail!("CLOB batch acceptance is uncertain");
                }
                let legs: [Option<OrderReceipt>; 2] = std::array::from_fn(|index| {
                    validate_order_response(&resps[index])
                        .ok()
                        .map(|_| OrderReceipt {
                            order_id: resps[index].order_id.clone(),
                            signed_quantity: signed_quantities[index],
                            making_amount: resps[index].making_amount,
                            taking_amount: resps[index].taking_amount,
                        })
                });
                if legs.iter().any(Option::is_none) {
                    return Err(PartialBatchError { legs }.into());
                }
                let [Some(a), Some(b)] = legs else {
                    unreachable!("validated both legs")
                };
                return Ok([a, b]);
            }
            Err(e) => {
                let err_msg = format!("{:?}", e).to_lowercase();
                // Validation refusal — safe to resend for the same reason as
                // the single-order path (the refusal is the venue's statement
                // that nothing was accepted). Dead in V2; kept as a vestige.
                if err_msg.contains("invalid nonce") && attempt == 0 {
                    warn!("⚠️ Nonce error in atomic batch — re-syncing from API...");
                    if let Some(fresh_nonce) = fetch_next_nonce(http, safe_address).await {
                        nonce_manager.store(fresh_nonce, Ordering::SeqCst);
                        warn!(
                            "🔄 Nonce re-synced to {} — retrying atomic batch",
                            fresh_nonce
                        );
                    }
                    continue;
                }
                // The execution-engine 500 is NOT retried — see the
                // single-order path for the evidence. The batch case is the
                // worse one: a resend here is a second PAIR of freshly-salted
                // legs, and the orphan machinery is built for one leg failing
                // to fill, not for two extra legs arriving. Surfaces to the
                // caller as `PlacementFault::Ambiguous`.
                return Err(anyhow::anyhow!("Atomic order placement failed: {}", e));
            }
        }
    }
    Err(anyhow::anyhow!("Max retries reached for atomic batch"))
}

#[cfg(test)]
mod signing_tests {
    use super::super::wallet::WalletType;
    use super::*;
    use alloy::dyn_abi::Eip712Domain;
    use alloy::primitives::{address, keccak256, Signature};
    use alloy::signers::Signer;
    use alloy::sol_types::{SolStruct, SolValue};
    use polymarket_client_sdk_v2::auth::Credentials;
    use polymarket_client_sdk_v2::clob::types::{OrderSignature, SignatureType};
    use polymarket_client_sdk_v2::clob::{Client as SdkClient, Config};
    use polymarket_client_sdk_v2::POLYGON;
    use rust_decimal_macros::dec;
    use std::borrow::Cow;
    use std::str::FromStr;

    type TestSigner = LocalSigner<alloy::signers::k256::ecdsa::SigningKey>;

    async fn offline_wallet(
        wallet_type: WalletType,
    ) -> Result<(Arc<TradingClient>, TestSigner, WalletConfig)> {
        // Public deterministic test key. Credentials are synthetic and are
        // supplied directly, so authentication performs no HTTP requests.
        let signer =
            LocalSigner::from_str(&format!("{:064x}", 1_u64))?.with_chain_id(Some(POLYGON));
        let wallet = WalletConfig {
            wallet_type,
            signer: signer.address(),
            funder: address!("0x1111111111111111111111111111111111111111"),
        };
        let client = SdkClient::new("http://127.0.0.1:9", Config::default())?
            .authentication_builder(&signer)
            .credentials(Credentials::new(
                Default::default(),
                "c3ludGhldGlj".into(),
                "offline".into(),
            ))
            .funder(wallet.funder)
            .signature_type(wallet.signature_type())
            .authenticate()
            .await?;
        let client = TradingClient::for_test(
            client,
            wallet,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .await?;
        Ok((Arc::new(client), signer, wallet))
    }

    fn domain(neg_risk: bool) -> Eip712Domain {
        Eip712Domain {
            name: Some(Cow::Borrowed("Polymarket CTF Exchange")),
            version: Some(Cow::Borrowed("2")),
            chain_id: Some(U256::from(POLYGON)),
            verifying_contract: Some(super::super::exchange_verifying_contract(neg_risk)),
            ..Eip712Domain::default()
        }
    }

    #[tokio::test]
    async fn safe_signatures_recover_the_eoa_for_both_exchange_domains() -> Result<()> {
        let (client, signer, wallet) = offline_wallet(WalletType::GnosisSafe).await?;
        for neg_risk in [false, true] {
            let signed = build_signed_order(
                &client,
                &signer,
                &wallet,
                super::super::exchange_verifying_contract(neg_risk),
                U256::from(1),
                Side::Buy,
                dec!(6.3796),
                dec!(0.63),
                OrderType::GTC,
                false,
                0,
            )
            .await?;
            assert_eq!(signed.order().maker, wallet.funder);
            assert_eq!(signed.order().signer, signer.address());
            assert_eq!(
                signed.order().signatureType,
                SignatureType::GnosisSafe as u8
            );
            assert_eq!(signed.order().makerAmount, U256::from(3_780_000));
            assert_eq!(signed.order().takerAmount, U256::from(6_000_000));
            assert_eq!(signed.v2().expiration, U256::ZERO);
            let OrderSignature::Ecdsa(signature) = &signed.signature else {
                anyhow::bail!("Safe order must carry an ECDSA signature");
            };
            let hash = signed.order().eip712_signing_hash(&domain(neg_risk));
            assert_eq!(
                signature.recover_address_from_prehash(&hash)?,
                signer.address()
            );
            let other_hash = signed.order().eip712_signing_hash(&domain(!neg_risk));
            assert_ne!(
                signature.recover_address_from_prehash(&other_hash)?,
                signer.address()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn poly1271_wraps_the_wallet_signature_for_both_exchange_domains() -> Result<()> {
        let (client, signer, wallet) = offline_wallet(WalletType::Poly1271).await?;
        for neg_risk in [false, true] {
            let signed = build_signed_order(
                &client,
                &signer,
                &wallet,
                super::super::exchange_verifying_contract(neg_risk),
                U256::from(1),
                Side::Sell,
                dec!(6.3796),
                dec!(0.63),
                OrderType::GTC,
                false,
                0,
            )
            .await?;
            assert_eq!(signed.order().maker, wallet.funder);
            assert_eq!(signed.order().signer, wallet.funder);
            assert_eq!(signed.order().signatureType, SignatureType::Poly1271 as u8);
            assert_eq!(signed.order().makerAmount, U256::from(6_370_000));
            assert_eq!(signed.order().takerAmount, U256::from(4_013_100));
            let OrderSignature::Wrapped(signature) = &signed.signature else {
                anyhow::bail!("Deposit wallet order must carry a wrapped signature");
            };
            let order_type = "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";
            assert_eq!(
                signature.len(),
                2 + 130 + 64 + 64 + order_type.len() * 2 + 4
            );
            let app_domain = domain(neg_risk).hash_struct();
            let contents_hash = signed.order().eip712_hash_struct();
            assert_eq!(
                format!("0x{}", &signature[132..196]),
                app_domain.to_string()
            );
            assert_eq!(
                format!("0x{}", &signature[196..260]),
                contents_hash.to_string()
            );

            // Independently recover the owner from the wrapped TypedDataSign
            // digest. The deposit wallet is the inner verifying contract.
            let typed_data_type = format!("TypedDataSign(Order contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt){order_type}");
            let typed_data_hash = keccak256(
                (
                    keccak256(typed_data_type.as_bytes()),
                    contents_hash,
                    keccak256(b"DepositWallet"),
                    keccak256(b"1"),
                    U256::from(POLYGON),
                    wallet.funder,
                    B256::ZERO,
                )
                    .abi_encode(),
            );
            let digest = keccak256(
                [
                    &[0x19_u8, 0x01][..],
                    app_domain.as_slice(),
                    typed_data_hash.as_slice(),
                ]
                .concat(),
            );
            let inner_signature = Signature::from_str(&signature[..132])?;
            assert_eq!(
                inner_signature.recover_address_from_prehash(&digest)?,
                signer.address()
            );
            let json = serde_json::to_value(&signed)?;
            assert_eq!(json["order"]["signatureType"], 3);
            assert_eq!(json["order"]["signature"], signature.as_str());
            assert!(json["order"].get("nonce").is_none());
            assert!(json["order"].get("feeRateBps").is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn gtd_and_post_only_preserve_expiration_and_bid_flooring() -> Result<()> {
        let (client, signer, wallet) = offline_wallet(WalletType::GnosisSafe).await?;
        let before = Utc::now().timestamp() as u64;
        let signed = build_signed_order(
            &client,
            &signer,
            &wallet,
            super::super::exchange_verifying_contract(false),
            U256::from(1),
            Side::Buy,
            dec!(5.9),
            dec!(0.318),
            OrderType::GTD,
            true,
            30,
        )
        .await?;
        let after = Utc::now().timestamp() as u64;
        assert!(signed.v2().expiration >= U256::from(before + 90));
        assert!(signed.v2().expiration <= U256::from(after + 90));
        assert_eq!(signed.order().makerAmount, U256::from(1_550_000));
        assert_eq!(signed.order().takerAmount, U256::from(5_000_000));
        assert_eq!(signed.post_only, Some(true));
        assert_eq!(signed.order_type, OrderType::GTD);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_amounts_time_in_force_and_domains_fail_before_signing() -> Result<()> {
        let (client, signer, wallet) = offline_wallet(WalletType::GnosisSafe).await?;
        let exchange = super::super::exchange_verifying_contract(false);
        for (quantity, price, tif, post_only, expiration) in [
            (dec!(0), dec!(0.5), OrderType::GTC, false, 0),
            (dec!(-1), dec!(0.5), OrderType::GTC, false, 0),
            (dec!(0.001), dec!(0.5), OrderType::GTC, false, 0),
            (dec!(1), dec!(0), OrderType::GTC, false, 0),
            (dec!(1), dec!(1), OrderType::GTC, false, 0),
            (dec!(1), dec!(0.001), OrderType::GTC, false, 0),
            (dec!(1), dec!(0.999), OrderType::GTC, false, 0),
            (dec!(1), dec!(0.5), OrderType::GTD, false, 0),
            (dec!(1), dec!(0.5), OrderType::GTC, false, 60),
            (dec!(1), dec!(0.5), OrderType::FAK, true, 0),
            (dec!(1), dec!(0.5), OrderType::FOK, true, 0),
            (Decimal::MAX, dec!(0.5), OrderType::GTC, false, 0),
        ] {
            assert!(build_signed_order(
                &client,
                &signer,
                &wallet,
                exchange,
                U256::from(1),
                Side::Buy,
                quantity,
                price,
                tif,
                post_only,
                expiration,
            )
            .await
            .is_err());
        }
        assert!(build_signed_order(
            &client,
            &signer,
            &wallet,
            Address::ZERO,
            U256::from(1),
            Side::Buy,
            dec!(5),
            dec!(0.5),
            OrderType::GTC,
            false,
            0,
        )
        .await
        .is_err());
        Ok(())
    }

    fn order_response(success: bool, id: &str, error: Option<&str>) -> PostOrderResponse {
        serde_json::from_value(serde_json::json!({
            "success": success, "orderID": id, "errorMsg": error,
            "status": "live", "makingAmount": "", "takingAmount": ""
        }))
        .expect("valid public response fixture")
    }

    #[test]
    fn order_acceptance_requires_success_id_and_no_error() {
        assert!(validate_order_response(&order_response(true, "accepted", None)).is_ok());
        assert!(validate_order_response(&order_response(true, "accepted", Some(""))).is_ok());
        assert!(
            validate_order_response(&order_response(false, "", Some("balance / allowance")))
                .is_err()
        );
        assert!(validate_order_response(&order_response(true, "", None)).is_err());
        assert!(
            validate_order_response(&order_response(true, "accepted", Some("rejected"))).is_err()
        );
    }

    #[test]
    fn batch_acceptance_checks_each_leg_and_retains_accepted_ids() {
        assert!(validate_batch_response(&[
            order_response(true, "first", None),
            order_response(true, "second", None)
        ])
        .is_ok());
        let error = validate_batch_response(&[
            order_response(true, "accepted-orphan", None),
            order_response(false, "", Some("rejected")),
        ])
        .unwrap_err()
        .to_string();
        assert!(error.contains("Batch leg 2"));
        assert!(error.contains("accepted-orphan"));
        assert!(validate_batch_response(&[]).is_err());
        assert!(validate_batch_response(&[order_response(true, "first", None)]).is_err());
    }
}

#[cfg(test)]
mod placement_fault_tests {
    use super::{classify_placement_error, PlacementFault};

    /// The 2026-09-01 incident. Polymarket's order manager was down roughly
    /// 18:58:09–19:00:56; FairValue's two entry attempts in that window (15s
    /// apart) both drew the 425 below, and each armed the 60s trade cooldown
    /// and counted one of MAX_CONSECUTIVE_FAILURES = 3 toward the breaker.
    /// With the earlier FAK no-match at 17:44:46 that made the strategy
    /// 0-for-3 on the day without ever doing anything wrong. A venue reply of
    /// "I did not process your order, please retry" must never be charged to
    /// the strategy.
    #[test]
    fn a_425_order_manager_not_ready_is_the_venues_fault_not_the_strategys() {
        let e = anyhow::Error::msg(
            "Order placement failed: Status: error(425 Too Early) making POST \
             call to /order with {\"error\":\"order manager not ready, please retry\"}",
        );
        assert_eq!(
            classify_placement_error(&e),
            PlacementFault::VenueUnavailable
        );
    }

    /// The status-code form must classify on its own: the body text is the
    /// venue's to reword, but the SDK's Status Display always carries
    /// "error(<code> <reason>)".
    #[test]
    fn a_425_classifies_by_status_code_even_if_the_venue_rewords_the_body() {
        let e = anyhow::anyhow!(
            "Order placement failed: Status: error(425 Too Early) making POST \
             call to /order with try later"
        );
        assert_eq!(
            classify_placement_error(&e),
            PlacementFault::VenueUnavailable
        );
    }

    /// The 17:44:46 failure from the same production day: a FAK that found no
    /// resting liquidity at its limit (400). A definitive answer about the
    /// book, not a malfunction — it must not count toward the breaker, and it
    /// gets the short book-race pause rather than the full 60s cooldown.
    #[test]
    fn a_fak_no_match_400_is_a_book_race_not_a_strategy_fault() {
        let e = anyhow::Error::msg(
            "Order placement failed: Status: error(400 Bad Request) making POST \
             call to /order with {\"error\":\"no orders found to match with FAK order. \
             FAK orders are partially filled or killed if no match is found.\"}",
        );
        assert_eq!(classify_placement_error(&e), PlacementFault::BookRace);
    }

    /// The execution-engine 500 that used to be blindly resent from inside
    /// `place_limit_order_filled` / `place_limit_orders_atomic`. Payload shape
    /// as reported against the live CLOB (py-clob-client issue #331, BTC5M,
    /// 2026-04-15): a 500 whose body never states whether the order was
    /// processed — and Polymarket's error-codes reference, which spells out
    /// not-processed semantics wherever they hold, omits this message
    /// entirely. Ambiguous means never resend: each resend is a fresh salt
    /// and a distinct order id, so a landed original plus a resend is two
    /// live orders. It also means no cooldown and no breaker count, because
    /// an engine failure is not the strategy's fault.
    #[test]
    fn an_execution_engine_500_is_ambiguous_never_resent_and_never_charged() {
        let e = anyhow::Error::msg(
            "Order placement failed: Status: error(500 Internal Server Error) making POST \
             call to /order with {\"error\":\"could not run the execution\"}",
        );
        assert_eq!(classify_placement_error(&e), PlacementFault::Ambiguous);
    }

    /// The historical carve-out keeps working through the classifier.
    #[test]
    fn a_post_only_crosses_book_rejection_is_a_book_race() {
        let e = anyhow::anyhow!("Order placement failed: invalid: order crosses book");
        assert_eq!(classify_placement_error(&e), PlacementFault::BookRace);
    }

    /// Everything unrecognized keeps today's accounting — cooldown plus a
    /// breaker count. The breaker exists to stop a genuinely broken loop
    /// (bad params, drained wallet) from machine-gunning the venue, and this
    /// change must not widen the exemption beyond the classes above. A
    /// GENERIC 500 stays charged: only the specific execution-engine message
    /// is known-ambiguous, and the client-side 12s timeout keeps its
    /// historical accounting too.
    #[test]
    fn an_unrecognized_rejection_is_still_charged_to_the_strategy() {
        for msg in [
            "Order placement failed: not enough balance / allowance",
            "Order placement timed out after 12s",
            "Order placement failed: Status: error(500 Internal Server Error) making POST call to /order with oops",
            "Signing failed: signature error",
        ] {
            let e = anyhow::anyhow!("{msg}");
            assert_eq!(classify_placement_error(&e), PlacementFault::Strategy, "{msg}");
        }
    }

    /// Token ids are long digit strings that can embed "425"; only the SDK's
    /// status form "error(425" may classify as venue-unavailable.
    #[test]
    fn a_425_inside_a_token_id_does_not_classify_as_venue_unavailable() {
        let e = anyhow::anyhow!(
            "Order placement failed: Status: error(400 Bad Request) making POST \
             call to /order with invalid token 76043073756653678226373981964254"
        );
        assert_eq!(classify_placement_error(&e), PlacementFault::Strategy);
    }
}
