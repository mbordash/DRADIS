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

/// Known strategy names for reconciliation adoption priority.
/// Maker is first because orphaned GTD fills are the most common cause.
const ADOPTION_STRATEGIES: &[&str] = &["MakerStrategy", "ArbitrageStrategy", "TimeDecayStrategy", "MomentumStrategy"];

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

/// Periodic reconciliation: checks on-chain balances for the given tokens and
/// re-adopts any shares the bot has forgotten about (e.g. orphaned GTD fills).
///
/// For each token, if a non-zero on-chain balance exists but NO strategy has a
/// local position tracking it, a new position is created under the first
/// available strategy slot (preferring MakerStrategy, since orphaned GTD fills
/// are the most common cause).
pub async fn reconcile_orphaned_positions(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    positions: &Arc<Mutex<PositionMap>>,
    tokens: &[(U256, &str)],  // (token_id, side_label)
    market_name: &str,
    market_close_time: Option<chrono::DateTime<Utc>>,
) {
    for &(token_id, side_label) in tokens {
        // Query on-chain balance for this token
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Conditional;
        req.token_id = Some(token_id);

        let actual_shares = match client.balance_allowance(req).await {
            Ok(resp) => {
                Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000)
            }
            Err(_) => continue,
        };

        if actual_shares <= dec!(0) {
            continue;
        }

        // Check if ANY strategy already tracks this token
        let pos_map = positions.lock().await;
        let already_tracked = pos_map.iter().any(|((_, tid), _)| *tid == token_id);
        drop(pos_map);

        if already_tracked {
            continue;
        }

        // Orphan detected — adopt under the first available strategy slot
        let adopted_strategy = {
            let pos_map = positions.lock().await;
            ADOPTION_STRATEGIES.iter().find(|&&s| {
                !pos_map.contains_key(&(s.to_string(), token_id))
            }).map(|s| s.to_string())
        };

        if let Some(strategy_name) = adopted_strategy {
            let mut pos_map = positions.lock().await;
            // Double-check after re-acquiring lock
            let key = (strategy_name.clone(), token_id);
            if pos_map.contains_key(&key) {
                continue;
            }
            pos_map.insert(key, Position {
                shares: actual_shares,
                avg_entry: dec!(0.50), // unknown entry price — use midpoint as conservative estimate
                opened_at: Utc::now(),
                close_time: market_close_time,
                market_name: market_name.to_string(),
                pair_token_id: token_id,
                fill_confirmed_at: Some(Utc::now()), // already confirmed on-chain
            });
            warn!(
                "🔁 RECONCILE: Adopted {} orphaned {} shares for token {} under [{}] on \"{}\"",
                actual_shares, side_label, token_id, strategy_name, market_name
            );
        }
    }
}

