use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use regex::Regex;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::str::FromStr;
use alloy::primitives::U256;
use tokio::sync::Mutex;
use tokio::time::Duration;
use chrono::Utc;
use tracing::{info, warn};

use polymarket_client_sdk::clob::{Client as ClobClient};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk::clob::types::AssetType;

pub use crate::state::Position;

/// Parse balance from error message
/// Extracts numeric balance value from error strings like "balance: 1000000"
pub fn parse_balance_from_error(err_msg: &str) -> Option<Decimal> {
    let re = Regex::new(r"(?:balance|available):\s*(\d+)").unwrap();
    if let Some(cap) = re.captures(err_msg) {
        if let Ok(val) = cap[1].parse::<u128>() {
            return Some(Decimal::from(val) / dec!(1_000_000));
        }
    }
    None
}

/// Synchronize position balance from on-chain data
///
/// Fetches the actual balance from the exchange ledger and updates the local position.
/// Handles indexer lag gracefully by checking how long since the position was opened.
pub async fn sync_position_balance(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    positions: &Arc<Mutex<HashMap<U256, Position>>>,
    token_id: U256,
) -> Result<()> {
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let mut req = BalanceAllowanceRequest::default();
    req.asset_type = AssetType::Conditional;
    req.token_id = Some(token_id);

    if let Ok(resp) = client.balance_allowance(req).await {
        let actual_shares = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
        let mut pos_map = positions.lock().await;
        if let Some(pos) = pos_map.get_mut(&token_id) {
            if actual_shares > dec!(0) {
                info!("⚖️ Position Synced: Token {} quantity updated from {} to actual: {}", token_id, pos.shares, actual_shares);
                pos.shares = actual_shares;
                if pos.fill_confirmed_at.is_none() {
                    pos.fill_confirmed_at = Some(Utc::now());
                }
            } else {
                let time_since_open = Utc::now() - pos.opened_at;
                if time_since_open.num_seconds() > 15 {
                    warn!("⚠️ Position Sync FAILED: Token {} balance still 0 after {}s. Order likely never filled on-chain. Removing position.",
                          token_id, time_since_open.num_seconds());
                    pos_map.remove(&token_id);
                } else if pos.fill_confirmed_at.is_some() {
                    warn!("⚠️ Position Sync WARNING: Token {} balance disappeared (was confirmed at {:?}). Possible liquidation or indexer issue.",
                          token_id, pos.fill_confirmed_at);
                } else {
                    warn!("⚠️ Position Sync: Token {} balance is 0 ({}s since open). Might be indexer lag. Keeping local position.",
                          token_id, time_since_open.num_seconds());
                }
            }
        }
    }
    Ok(())
}
