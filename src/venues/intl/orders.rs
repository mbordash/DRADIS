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
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::timeout;

use polymarket_client_sdk_v2::clob::{Client as ClobClient};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::clob::types::{OrderType, Side, SignatureType, Order, SignedOrder, OrderPayload};
use polymarket_client_sdk_v2::{POLYGON};
use alloy::primitives::{U256, Address, B256};
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use alloy::dyn_abi::Eip712Domain;
use alloy::sol_types::SolStruct;
use chrono::Utc;
use rust_decimal::Decimal;
use tracing::warn;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_decimal::prelude::ToPrimitive;
use crate::helpers::price::{round_to_tick_size, floor_to_tick_size};
use crate::helpers::nonce::fetch_next_nonce;
use crate::venues::core::MarketId;
use crate::venues::core::TimeInForce;
use crate::venues::intl::u256_from_market_id;

const ORDER_NAME: &str = "Polymarket CTF Exchange";
/// EIP-712 domain version for the V2 CTF Exchange (pUSD collateral migration)
const VERSION: &str = "2";
/// Polymarket requires expiration timestamp to be >= now + 1 minute + 30 seconds.
/// We add 90 seconds (1.5 minutes) as a safety buffer.
const EXPIRATION_BUFFER_SECS: u64 = 90;

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
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    safe_address: Address,
    eoa_address: Address,
    verifying_contract: Address,
    token_id: U256,
    side: Side,
    quantity: Decimal,
    limit_price: Decimal,
    order_type: OrderType,
    post_only: bool,
    expiration_secs: u64,
) -> Result<SignedOrder> {
    // V2: expiration lives in OrderPayload (outside the signed struct).
    let expiration_v2 = if expiration_secs > 0 {
        let now_unix = Utc::now().timestamp() as u64;
        let buffer = expiration_secs.max(EXPIRATION_BUFFER_SECS);
        U256::from(now_unix + buffer)
    } else {
        U256::ZERO
    };

    // timestamp_ms: milliseconds since UNIX epoch — required field in V2 order struct
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time before epoch")
        .as_millis();

    let mut order_struct = Order::default();
    order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
    order_struct.maker = safe_address;
    order_struct.signer = eoa_address;
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
            let usdc_cents = (quantity * rounded_price * Decimal::from(100))
                .floor()
                .to_u128()
                .unwrap_or(0);
            let usdc_cents_aligned = if price_cents > 0 {
                (usdc_cents / price_cents) * price_cents
            } else {
                usdc_cents
            };
            let maker_raw = usdc_cents_aligned * 10000u128;
            let taker_raw = if price_cents > 0 { maker_raw * 100 / price_cents } else { 0 };
            order_struct.makerAmount = U256::from(maker_raw);
            order_struct.takerAmount = U256::from(taker_raw);
        }
        Side::Sell => {
            // makerAmount (shares you give)    = max 4dp → in 1e6, divisible by 100
            // takerAmount (USDC you receive)   = max 2dp → in 1e6, divisible by 10000
            let shares_2dp = (quantity * Decimal::from(100))
                .floor()
                .to_u128()
                .unwrap_or(0);
            let maker_raw = shares_2dp * 10000u128;
            let usdc_cents = if price_cents > 0 { shares_2dp * price_cents } else { 0 };
            let taker_raw = usdc_cents * 100u128;
            order_struct.makerAmount = U256::from(maker_raw);
            order_struct.takerAmount = U256::from(taker_raw);
        }
        _ => return Err(anyhow::anyhow!("Unsupported order side")),
    }

    order_struct.side = side as u8;
    order_struct.signatureType = SignatureType::GnosisSafe as u8;

    let domain = Eip712Domain {
        name: Some(Cow::Borrowed(ORDER_NAME)),
        version: Some(Cow::Borrowed(VERSION)),
        chain_id: Some(U256::from(POLYGON)),
        verifying_contract: Some(verifying_contract),
        ..Eip712Domain::default()
    };

    let hash = order_struct.eip712_signing_hash(&domain);
    let signature = signer.sign_hash(&hash).await
        .map_err(|e| anyhow::anyhow!("Signing failed: {}", e))?;

    let payload = OrderPayload::new(order_struct, expiration_v2);
    Ok(SignedOrder::builder()
        .payload(payload)
        .signature(signature)
        .order_type(order_type)
        .owner(client.credentials().key())
        .post_only(post_only)
        .build())
}

// ─────────────────────────────────────────────────────────────────────────────
// Public: Single-leg order placement
// ─────────────────────────────────────────────────────────────────────────────

/// Generic order placement helper — works for any token, buy/sell, strategy.
///
/// Handles: nonce management, EIP-712 signing, posting to CLOB, retry on
/// transient errors (nonce conflict, execution-engine 500).
///
/// Returns `(order_id, making_amount, taking_amount)` — the matched amounts come
/// straight from the CLOB `POST /order` response. For a marketable FAK/FOK order
/// these reflect the ACTUAL execution (so callers can compute the real average
/// fill price); for a resting GTC/GTD order they are typically zero until a match
/// occurs. Use [`place_limit_order`] if you only need the order id.
pub async fn place_limit_order_filled(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
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
    // Convert the neutral key to the on-chain id at the venue boundary (slice 2b).
    let token_id = u256_from_market_id(token_id)?;
    // Map the neutral TIF onto the SDK enum once, at the venue boundary.
    let order_type = to_clob(order_type);
    for attempt in 0..2 {
        // AtomicU64 load — kept for API compatibility; V2 orders have no nonce field.
        let _current_nonce = nonce_manager.load(Ordering::SeqCst);

        let signed_order = build_signed_order(
            client, signer, safe_address, eoa_address, verifying_contract,
            token_id, side, quantity, limit_price,
            order_type.clone(), post_only, expiration_secs,
        ).await?;

        // Hard 12-second timeout: prevents a TCP stall from freezing the tokio::select! arm.
        let post_result = timeout(
            std::time::Duration::from_secs(12),
            client.post_order(signed_order),
        ).await;
        let post_result = match post_result {
            Err(_elapsed) => {
                warn!("⚠️ post_order timed out after 12s (attempt {}) — treating as transient failure", attempt + 1);
                return Err(anyhow::anyhow!("Order placement timed out after 12s"));
            }
            Ok(r) => r,
        };
        match post_result {
            Ok(resp) => return Ok((resp.order_id, resp.making_amount, resp.taking_amount)),
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
    client: &Arc<ClobClient<Authenticated<Normal>>>,
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
        client, nonce_manager, signer, safe_address, eoa_address, verifying_contract,
        token_id, side, quantity, limit_price, fee_rate_bps, order_type, post_only,
        expiration_secs, http,
    ).await?;
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
    client: &Arc<ClobClient<Authenticated<Normal>>>,
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
                label, ot
            ));
        }
    }
    // Map the neutral TIFs onto the SDK enum once, past the batch-eligibility guard.
    let order_type_a = to_clob(order_type_a);
    let order_type_b = to_clob(order_type_b);
    for attempt in 0..2 {
        let _current_nonce = nonce_manager.load(Ordering::SeqCst);

        let order_a = build_signed_order(
            client, signer, safe_address, eoa_address, vc_a,
            token_id_a, side_a, quantity_a, price_a,
            order_type_a.clone(), post_only_a, expiration_a,
        ).await?;

        let order_b = build_signed_order(
            client, signer, safe_address, eoa_address, vc_b,
            token_id_b, side_b, quantity_b, price_b,
            order_type_b.clone(), post_only_b, expiration_b,
        ).await?;

        // Slightly longer timeout than single-order to account for batch validation.
        let post_result = timeout(
            std::time::Duration::from_secs(15),
            client.post_orders(vec![order_a, order_b]),
        ).await;

        let post_result = match post_result {
            Err(_elapsed) => {
                warn!("⚠️ post_orders (atomic) timed out after 15s (attempt {})", attempt + 1);
                return Err(anyhow::anyhow!("Atomic order placement timed out after 15s"));
            }
            Ok(r) => r,
        };

        match post_result {
            Ok(resps) => {
                if resps.len() < 2 {
                    return Err(anyhow::anyhow!(
                        "Atomic batch returned {} responses (expected 2)", resps.len()
                    ));
                }
                return Ok((resps[0].order_id.clone(), resps[1].order_id.clone()));
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
                        warn!("🔄 Nonce re-synced to {} — retrying atomic batch", fresh_nonce);
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
             call to /order with {\"error\":\"order manager not ready, please retry\"}"
        );
        assert_eq!(classify_placement_error(&e), PlacementFault::VenueUnavailable);
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
        assert_eq!(classify_placement_error(&e), PlacementFault::VenueUnavailable);
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
             FAK orders are partially filled or killed if no match is found.\"}"
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
             call to /order with {\"error\":\"could not run the execution\"}"
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
