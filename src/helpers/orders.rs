/// Order placement helpers for trading execution
///
/// Provides generic order placement function that works with any strategy,
/// token, or direction (buy/sell).

use anyhow::Result;
use std::borrow::Cow;
use std::sync::Arc;

use polymarket_client_sdk::clob::{Client as ClobClient, Config};
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

use crate::helpers::price::to_fixed_u128;

const ORDER_NAME: &str = "Polymarket CTF Exchange";
const VERSION: &str = "1";

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
) -> Result<()> {
    for attempt in 0..2 {
        let mut guard = nonce_manager.lock().await;
        let current_nonce = *guard;

        let mut order_struct = Order::default();
        order_struct.salt = U256::from(Utc::now().timestamp_millis() & ((1 << 53) - 1));
        order_struct.maker = safe_address;
        order_struct.signer = eoa_address;
        order_struct.tokenId = token_id;
        order_struct.makerAmount = U256::from(to_fixed_u128(quantity * limit_price));
        order_struct.takerAmount = U256::from(to_fixed_u128(quantity));
        order_struct.expiration = U256::ZERO;
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
            let signed_order = SignedOrder::builder()
                .order(order_struct)
                .signature(signature)
                .order_type(OrderType::FAK)
                .owner(client.credentials().key())
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
                        warn!("⚠️ Invalid nonce. Re-syncing...");
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



