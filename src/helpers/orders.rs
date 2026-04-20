/// Order placement helpers for trading execution
///
/// Provides generic order placement function that works with any strategy,
/// token, or direction (buy/sell).

use anyhow::Result;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use polymarket_client_sdk::clob::{Client as ClobClient};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::{OrderType, Side, SignatureType, Order, SignedOrder};
use polymarket_client_sdk::{POLYGON};
use alloy::primitives::{U256, Address};
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use alloy::dyn_abi::Eip712Domain;
use alloy::sol_types::SolStruct;
use chrono::Utc;
use rust_decimal::Decimal;
use tracing::warn;

use rust_decimal::prelude::ToPrimitive;
use crate::helpers::price::{to_fixed_u128_with_precision, round_to_tick_size, floor_to_tick_size};
use crate::helpers::nonce::fetch_next_nonce;

const ORDER_NAME: &str = "Polymarket CTF Exchange";
const VERSION: &str = "1";
/// Polymarket requires expiration timestamp to be >= now + 1 minute + 30 seconds
/// We add 90 seconds (1.5 minutes) as a safety buffer
const EXPIRATION_BUFFER_SECS: u64 = 90;

/// Generic order placement helper - works for any token, buy/sell, strategy
///
/// This function handles the complete order placement flow:
/// - Nonce management with retry logic
/// - EIP712 signing
/// - Order posting to Polymarket CLOB
/// - Error handling for nonce conflicts
///
/// # Arguments
/// * `client` - The CLOB trading client
/// * `nonce_manager` - Mutex-wrapped nonce counter (incremented on success)
/// * `signer` - LocalSigner for message signing
/// * `safe_address` - The Safe wallet maker address
/// * `eoa_address` - The EOA signer address
/// * `verifying_contract` - The exchange contract address
/// * `token_id` - The token to trade (YES or NO)
/// * `side` - Buy or Sell
/// * `quantity` - Amount of shares
/// * `limit_price` - Limit price per share
/// * `fee_rate_bps` - Fee rate in basis points
/// * `order_type` - Order type (e.g. FAK, GTC, GTD)
/// * `post_only` - If true, ensures the order only adds liquidity (fails if it would take)
/// * `expiration_secs` - Time in seconds until order expires (0 for no expiration)
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
    fee_rate_bps: u16,
    order_type: OrderType,
    post_only: bool,
    expiration_secs: u64,
    http: &reqwest::Client,
) -> Result<()> {
    for attempt in 0..2 {
        // AtomicU64 load — no lock needed. Polymarket's GnosisSafe nonce acts as a
        // minimum-cancel-nonce for batch cancellation, not a strict replay counter,
        // so the same value can be used by concurrent orders without conflict.
        let current_nonce = nonce_manager.load(Ordering::SeqCst);

        let mut order_struct = Order::default();
        order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
        order_struct.maker = safe_address;
        order_struct.signer = eoa_address;
        order_struct.tokenId = token_id;

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
        // To guarantee this, we derive one amount from the other using integer price_cents.
        //
        // BUY:  takerAmount = trunc(shares, 4dp) * 1e6  → always divisible by 100
        //       makerAmount = takerAmount / 100 * price_cents  → exact integer, ratio = price_cents/100
        //
        // SELL: makerAmount = trunc(shares, 2dp) * 1e6  → always divisible by 10^4 (i.e. by 100)
        //       takerAmount = makerAmount / 100 * price_cents  → exact integer, ratio = price_cents/100
        //
        // This eliminates the rounding residual that previously caused
        // "Price (0.6305...) breaks minimum tick size rule: 0.01" rejections.
        let price_cents = (rounded_price * Decimal::from(100u32))
            .round()
            .to_u128()
            .unwrap_or(0);

        match side {
            Side::Buy => {
                // Polymarket BUY precision rules (from API error messages):
                //   makerAmount (USDC you pay)        = max 2 decimal places  → in 1e6 units, must be divisible by 10000
                //   takerAmount (shares you receive)  = max 4 decimal places  → in 1e6 units, must be divisible by 100
                //
                // Strategy: USDC-first alignment.
                // 1. Compute desired USDC in cents: usdc_cents = floor(qty * price * 100)
                // 2. Align to nearest multiple of price_cents (so exact share count is an integer):
                //    usdc_cents_aligned = floor(usdc_cents / price_cents) * price_cents
                // 3. maker_raw = usdc_cents_aligned * 10000  → divisible by 10000 ✓ (2dp USDC)
                // 4. taker_raw = maker_raw * 100 / price_cents → exact integer ✓  (4dp shares)
                //
                // Example: qty=57.69 shares @ $0.27 (price_cents=27)
                //   usdc_cents = floor(57.69*27) = floor(1557.63) = 1557
                //   aligned    = floor(1557/27)*27 = 57*27 = 1539  ($15.39)
                //   maker_raw  = 1539*10000 = 15390000  → $15.39 (2dp ✓)
                //   taker_raw  = 15390000*100/27 = 57000000 → 57.0000 shares (4dp ✓)
                let usdc_cents = (quantity * rounded_price * rust_decimal::Decimal::from(100))
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
                // Polymarket SELL precision rules:
                //   makerAmount (shares you give)     = max 4 decimal places  → in 1e6 divisible by 100
                //   takerAmount (USDC you receive)    = max 2 decimal places  → in 1e6 divisible by 10000
                //
                // Strategy: shares-first alignment (SELL gives shares, receives USDC).
                // 1. Align shares in cents of shares: shares_aligned = floor(qty*100)/100 (2dp)
                //    maker_raw = shares_2dp * 10^6 scaled to 1e6 → divisible by 10000 (≥ 4dp) ✓
                // 2. taker_raw (USDC) = (maker_raw / price_cents) * price_cents^2 / 100
                //    Simplified: taker_raw = (maker_raw / 100) * price_cents
                //    maker_raw divisible by 10000 → maker_raw/100 divisible by 100
                //    → taker_raw divisible by 100 ← need divisible by 10000 for 2dp USDC
                //
                // To guarantee 2dp USDC on sells, align shares to a multiple of price_cents:
                //   shares_cents = floor(qty*100), aligned = floor(shares_cents/price_cents)*price_cents
                //   maker_raw = aligned * 10000, taker_raw = maker_raw * price_cents / 10000  (= aligned*price_cents)
                //   taker_raw = aligned * price_cents  → exact integer divisible by price_cents, but need /100 for USDC...
                //
                // Simpler: truncate shares to 4dp (always safe for makerAmount),
                // derive USDC by truncating to 2dp (round down so we never over-receive).
                let shares_4dp_cents = (quantity * rust_decimal::Decimal::from(10000))
                    .floor()
                    .to_u128()
                    .unwrap_or(0);
                let maker_raw = shares_4dp_cents * 100u128;   // in 1e6, divisible by 100 ✓ (4dp shares)
                // USDC = shares * price; align to 2dp by flooring USDC cents then ×10000
                let usdc_cents = if price_cents > 0 { (maker_raw / 100) * price_cents / 10000 } else { 0 };
                let taker_raw = usdc_cents * 10000u128;         // divisible by 10000 ✓ (2dp USDC)
                order_struct.makerAmount = U256::from(maker_raw);
                order_struct.takerAmount = U256::from(taker_raw);
            }
            _ => return Err(anyhow::anyhow!("Unsupported order side")),
        }

        // Set expiration with safety buffer
        // Polymarket requires: now + 1 minute + 30 seconds minimum
        // We add EXPIRATION_BUFFER_SECS (90s) as safety margin
        order_struct.expiration = if expiration_secs > 0 {
            let now = Utc::now().timestamp() as u64;
            let buffer = expiration_secs.max(EXPIRATION_BUFFER_SECS);
            U256::from(now + buffer)
        } else {
            U256::ZERO
        };

        order_struct.nonce = U256::from(current_nonce);
        order_struct.feeRateBps = U256::from(fee_rate_bps);
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
        if let Ok(signature) = signer.sign_hash(&hash).await {
            // Using a single chain with maybe_post_only to avoid builder type-state mismatches.
            let signed_order = SignedOrder::builder()
                .order(order_struct)
                .signature(signature)
                .order_type(order_type.clone())
                .owner(client.credentials().key())
                .maybe_post_only(if post_only { Some(true) } else { None })
                .build();

            match client.post_order(signed_order).await {
                Ok(_) => {
                    // NOTE: Polymarket CLOB always returns next_nonce=0 for this wallet type
                    // (GnosisSafe signatureType).  The nonce field in Polymarket orders acts as
                    // a "minimum cancel nonce" for batch cancellation, not a strict per-order
                    // replay counter.  We leave the counter unchanged so it stays in
                    // sync with the API without the latency penalty.
                    return Ok(());
                }
                Err(e) => {
                    let err_msg = format!("{:?}", e).to_lowercase();
                    if err_msg.contains("invalid nonce") && attempt == 0 {
                        warn!("⚠️ Invalid nonce. Re-syncing from API...");
                        if let Some(fresh_nonce) = fetch_next_nonce(http, safe_address).await {
                            nonce_manager.store(fresh_nonce, Ordering::SeqCst);
                            warn!("🔄 Nonce re-synced to {} — retrying order", fresh_nonce);
                        }
                        continue;
                    }
                    // Polymarket's execution engine sometimes returns a transient 500
                    // "could not run the execution" — especially around market open.
                    // Retry once after a short delay.
                    if err_msg.contains("could not run the execution") && attempt == 0 {
                        warn!("⚠️ Execution engine 500 — retrying in 500ms (attempt {}/2)", attempt + 1);
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        continue;
                    }
                    return Err(anyhow::anyhow!("Order placement failed: {}", e));
                }
            }
        } else {
            return Err(anyhow::anyhow!("Signing failed"));
        }
    }
    Err(anyhow::anyhow!("Max retries reached"))
}
