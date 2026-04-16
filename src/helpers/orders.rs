/// Order placement helpers for trading execution
///
/// Provides generic order placement function that works with any strategy,
/// token, or direction (buy/sell).

use anyhow::Result;
use std::borrow::Cow;
use std::sync::Arc;

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
use tokio::sync::Mutex;
use tracing::warn;

use rust_decimal::prelude::ToPrimitive;
use crate::helpers::price::{to_fixed_u128_with_precision, round_to_tick_size};
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
    nonce_manager: &Arc<Mutex<u64>>,
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
        let mut guard = nonce_manager.lock().await;
        let current_nonce = *guard;

        let mut order_struct = Order::default();
        order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
        order_struct.maker = safe_address;
        order_struct.signer = eoa_address;
        order_struct.tokenId = token_id;

        // Round price to minimum tick size (0.01) to comply with Polymarket validation
        let rounded_price = round_to_tick_size(limit_price);

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
                // BUY order precision rules (Polymarket API):
                //   takerAmount (shares received) = max 2 decimal places
                //       → in 1e6 units, must be divisible by 10^4 = 10000
                //   makerAmount (USDC paid)       = max 4 decimal places
                //       → in 1e6 units, must be divisible by 10^2 = 100
                //
                // We truncate shares to 2dp first to guarantee taker_raw % 10000 == 0.
                // Then derive maker_raw = (taker_raw / 100) * price_cents.
                // Since taker_raw is divisible by 10000, taker_raw/100 is divisible by 100,
                // so maker_raw = (divisible_by_100) * price_cents = divisible by 100. ✓
                let taker_raw = to_fixed_u128_with_precision(quantity, 2);
                let maker_raw = (taker_raw / 100) * price_cents;
                order_struct.makerAmount = U256::from(maker_raw);
                order_struct.takerAmount = U256::from(taker_raw);
            }
            Side::Sell => {
                // makerAmount: shares truncated to 2dp, scaled to 1e6
                // = shares_2dp_int * 10^4  (always divisible by 100)
                let maker_raw = to_fixed_u128_with_precision(quantity, 2);
                // takerAmount = maker_raw / 100 * price_cents
                // = shares_2dp_int * 10^2 * price_cents  (exact integer, no rounding error)
                let taker_raw = (maker_raw / 100) * price_cents;
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
                    *guard += 1;
                    return Ok(());
                }
                Err(e) => {
                    drop(guard);
                    let err_msg = format!("{:?}", e).to_lowercase();
                    if err_msg.contains("invalid nonce") && attempt == 0 {
                        warn!("⚠️ Invalid nonce. Re-syncing from API...");
                        // Actually fetch the correct nonce from the API before retrying
                        if let Some(fresh_nonce) = fetch_next_nonce(http, safe_address).await {
                            let mut g = nonce_manager.lock().await;
                            *g = fresh_nonce;
                            warn!("🔄 Nonce re-synced to {} — retrying order", fresh_nonce);
                        }
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
