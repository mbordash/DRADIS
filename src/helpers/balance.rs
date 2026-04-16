use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use regex::Regex;
use anyhow::Result;
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

pub use crate::state::{Position, PositionMap};

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

/// Synchronize position balance from on-chain data.
///
/// Fetches the actual balance from the exchange ledger and updates the local
/// position keyed by `(strategy_name, token_id)`.
pub async fn sync_position_balance(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    positions: &Arc<Mutex<PositionMap>>,
    strategy_name: &str,
    token_id: U256,
) -> Result<()> {
    let key = (strategy_name.to_string(), token_id);

    let max_wait_secs: i64 = 60;
    let check_interval_ms: u64 = 5000;

    tokio::time::sleep(Duration::from_millis(2000)).await;

    loop {
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Conditional;
        req.token_id = Some(token_id);

        if let Ok(resp) = client.balance_allowance(req).await {
            let actual_shares = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
            let mut pos_map = positions.lock().await;
            if let Some(pos) = pos_map.get_mut(&key) {
                if actual_shares > dec!(0) {
                    info!("⚖️ Position Synced [{}]: Token {} quantity updated from {} to actual: {}",
                          strategy_name, token_id, pos.shares, actual_shares);
                    pos.shares = actual_shares;
                    if pos.fill_confirmed_at.is_none() {
                        pos.fill_confirmed_at = Some(Utc::now());
                    }
                    return Ok(());
                } else {
                    let time_since_open = (Utc::now() - pos.opened_at).num_seconds();
                    if pos.fill_confirmed_at.is_some() {
                        warn!("⚠️ Position Sync WARNING [{}]: Token {} balance disappeared (was confirmed at {:?}). Possible liquidation or indexer issue.",
                              strategy_name, token_id, pos.fill_confirmed_at);
                        return Ok(());
                    } else if time_since_open >= max_wait_secs {
                        warn!("⚠️ Position Sync FAILED [{}]: Token {} balance still 0 after {}s. Order never filled on-chain. Removing phantom position.",
                              strategy_name, token_id, time_since_open);
                        pos_map.remove(&key);
                        return Ok(());
                    } else {
                        warn!("⚠️ Position Sync [{}]: Token {} balance is 0 ({}s since open, max {}s). Retrying in {}ms…",
                              strategy_name, token_id, time_since_open, max_wait_secs, check_interval_ms);
                        drop(pos_map);
                        tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                        continue;
                    }
                }
            } else {
                // Position was removed externally (e.g. phantom cleanup in exit handler)
                return Ok(());
            }
        } else {
            tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
            let pos_map = positions.lock().await;
            if !pos_map.contains_key(&key) {
                return Ok(());
            }
            let time_since_open = pos_map.get(&key)
                .map(|p| (Utc::now() - p.opened_at).num_seconds())
                .unwrap_or(0);
            drop(pos_map);
            if time_since_open >= max_wait_secs {
                warn!("⚠️ Position Sync TIMEOUT [{}]: Token {} — API errors for {}s. Removing phantom position.",
                      strategy_name, token_id, time_since_open);
                positions.lock().await.remove(&key);
                return Ok(());
            }
        }
    }
}
