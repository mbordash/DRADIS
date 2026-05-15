/// Order placement helpers for trading execution
///
/// Provides generic order placement functions that work with any strategy,
/// token, or direction (buy/sell), including an atomic two-leg batch variant
/// that uses Polymarket's `/orders` endpoint to submit both legs simultaneously.

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

const ORDER_NAME: &str = "Polymarket CTF Exchange";
/// EIP-712 domain version for the V2 CTF Exchange (pUSD collateral migration)
const VERSION: &str = "2";
/// Polymarket requires expiration timestamp to be >= now + 1 minute + 30 seconds.
/// We add 90 seconds (1.5 minutes) as a safety buffer.
const EXPIRATION_BUFFER_SECS: u64 = 90;

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
pub async fn place_limit_order(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    nonce_manager: &Arc<AtomicU64>,
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    safe_address: Address,
    eoa_address: Address,
    verifying_contract: Address,
    token_id: U256,
    side: Side,
    quantity: Decimal,
    limit_price: Decimal,
    _fee_rate_bps: u16,
    order_type: OrderType,
    post_only: bool,
    expiration_secs: u64,
    http: &reqwest::Client,
) -> Result<String> {
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
            Ok(resp) => return Ok(resp.order_id),
            Err(e) => {
                let err_msg = format!("{:?}", e).to_lowercase();
                if err_msg.contains("invalid nonce") && attempt == 0 {
                    warn!("⚠️ Nonce error (unexpected in V2). Re-syncing from API...");
                    if let Some(fresh_nonce) = fetch_next_nonce(http, safe_address).await {
                        nonce_manager.store(fresh_nonce, Ordering::SeqCst);
                        warn!("🔄 Nonce re-synced to {} — retrying order", fresh_nonce);
                    }
                    continue;
                }
                if err_msg.contains("could not run the execution") && attempt == 0 {
                    warn!("⚠️ Execution engine 500 — retrying in 500ms (attempt {}/2)", attempt + 1);
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                return Err(anyhow::anyhow!("Order placement failed: {}", e));
            }
        }
    }
    Err(anyhow::anyhow!("Max retries reached"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Public: Atomic two-leg order placement
// ─────────────────────────────────────────────────────────────────────────────

/// Atomically places two orders (YES + NO legs) in a single API call.
///
/// Uses Polymarket's `POST /orders` batch endpoint which validates and processes
/// all orders atomically — either **both** reach the book or **neither** does.
///
/// This eliminates the window between sequential Leg A and Leg B placements
/// where Leg A could be live on the book while Leg B fails, requiring a
/// cancel + flash-exit safety net.  With atomic placement, on any batch error
/// no orders are live, no cleanup is required, and we can retry cleanly.
///
/// Returns `(leg_a_order_id, leg_b_order_id)` on success.
#[allow(clippy::too_many_arguments)]
pub async fn place_limit_orders_atomic(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    nonce_manager: &Arc<AtomicU64>,
    signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    safe_address: Address,
    eoa_address: Address,
    // ── Leg A ──
    vc_a: Address,
    token_id_a: U256,
    side_a: Side,
    quantity_a: Decimal,
    price_a: Decimal,
    order_type_a: OrderType,
    post_only_a: bool,
    expiration_a: u64,
    // ── Leg B ──
    vc_b: Address,
    token_id_b: U256,
    side_b: Side,
    quantity_b: Decimal,
    price_b: Decimal,
    order_type_b: OrderType,
    post_only_b: bool,
    expiration_b: u64,
    http: &reqwest::Client,
) -> Result<(String, String)> {
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
                if err_msg.contains("invalid nonce") && attempt == 0 {
                    warn!("⚠️ Nonce error in atomic batch — re-syncing from API...");
                    if let Some(fresh_nonce) = fetch_next_nonce(http, safe_address).await {
                        nonce_manager.store(fresh_nonce, Ordering::SeqCst);
                        warn!("🔄 Nonce re-synced to {} — retrying atomic batch", fresh_nonce);
                    }
                    continue;
                }
                if err_msg.contains("could not run the execution") && attempt == 0 {
                    warn!("⚠️ Execution engine 500 on atomic batch — retrying in 500ms", );
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                return Err(anyhow::anyhow!("Atomic order placement failed: {}", e));
            }
        }
    }
    Err(anyhow::anyhow!("Max retries reached for atomic batch"))
}
