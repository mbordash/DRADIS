use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use regex::Regex;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::str::FromStr;
use alloy::primitives::U256;
use alloy::primitives::B256;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use chrono::Utc;
use tracing::{debug, error, info, warn};

use polymarket_client_sdk::clob::{Client as ClobClient};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::request::{BalanceAllowanceRequest, OrdersRequest};
use polymarket_client_sdk::clob::types::AssetType;

pub use crate::state::{Position, PositionMap};

/// Shared map of (strategy:token_id) → Instant for phantom removal cooldowns.
pub type PhantomCooldowns = Arc<Mutex<HashMap<String, Instant>>>;

/// How long to block re-entry after a phantom removal (seconds).
pub const PHANTOM_COOLDOWN_SECS: u64 = 120;

/// Max seconds to wait for an on-chain balance to appear after an order.
pub const MAX_WAIT_SECS_HOURLY: i64 = 180;
pub const MAX_WAIT_SECS_WINDOW: i64 = 600;

const ADOPTION_STRATEGIES: &[&str] = &["MakerStrategy", "ArbitrageStrategy", "TimeDecayStrategy", "MomentumStrategy"];

pub fn parse_balance_from_error(err_msg: &str) -> Option<Decimal> {
    let re = Regex::new(r"(?:balance|available):\s*(\d+)").unwrap();
    if let Some(cap) = re.captures(err_msg) {
        if let Ok(val) = cap[1].parse::<u128>() {
            return Some(Decimal::from(val) / dec!(1_000_000));
        }
    }
    None
}

pub async fn sync_position_balance(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    positions: &Arc<Mutex<PositionMap>>,
    strategy_name: &str,
    token_id: U256,
    phantom_cooldowns: Option<&PhantomCooldowns>,
    baseline_shares: Decimal,
    max_wait_secs: i64,
) -> Result<()> {
    let key = (strategy_name.to_string(), token_id);
    let check_interval_ms: u64 = 3000;

    // Increased initial sleep: indexer takes time to reflect blockchain state
    tokio::time::sleep(Duration::from_secs(5)).await;

    loop {
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Conditional;
        req.token_id = Some(token_id);

        let raw_shares = match client.balance_allowance(req).await {
            Ok(resp) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
            Err(e) => {
                warn!("Position Sync API error [{}]: {}", strategy_name, e);
                tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                continue;
            }
        };

        let actual_shares = (raw_shares - baseline_shares).max(dec!(0));
        let mut pos_map = positions.lock().await;

        if let Some(pos) = pos_map.get_mut(&key) {
            let expected = pos.shares;
            let time_since_open = (Utc::now() - pos.opened_at).num_seconds();
            let fill_ratio = if expected > dec!(0) { actual_shares / expected } else { dec!(1) };

            if actual_shares >= crate::config::MIN_ORDER_SHARES {
                if fill_ratio < dec!(0.60) && time_since_open < 120 {
                    debug!("⏳ Position Sync PARTIAL [{}]: Token {} has {:.4} — still settling.", strategy_name, token_id, actual_shares);
                    drop(pos_map);
                    tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                    continue;
                }

                info!("⚖️ Position Synced [{}]: Token {} updated to actual: {}", strategy_name, token_id, actual_shares);
                pos.shares = actual_shares;
                if pos.fill_confirmed_at.is_none() { pos.fill_confirmed_at = Some(Utc::now()); }
                return Ok(());
            } else if actual_shares == dec!(0) {
                if time_since_open >= max_wait_secs {
                    drop(pos_map);
                    if check_for_resting_order(client, token_id).await {
                        tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                        continue;
                    }
                    error!("⚠️ Position Sync FAILED [{}] Token {} — phantom removed.", strategy_name, token_id);
                    positions.lock().await.remove(&key);
                    if let Some(cooldowns) = phantom_cooldowns {
                        cooldowns.lock().await.insert(format!("{}:{}", strategy_name, token_id), Instant::now());
                    }
                    return Ok(());
                } else {
                    // Suppress warning until at least 15s have passed (allow for indexer lag)
                    if time_since_open > 15 {
                        warn!("⚠️ Position Sync [{}]: Token {} balance is 0 ({}s since open). Retrying...", strategy_name, token_id, time_since_open);
                    }
                    drop(pos_map);
                    tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                    continue;
                }
            } else { return Ok(()); }
        } else {
            tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
            if !positions.lock().await.contains_key(&key) { return Ok(()); }
        }
    }
}

async fn check_for_resting_order(client: &Arc<ClobClient<Authenticated<Normal>>>, token_id: U256) -> bool {
    let req = OrdersRequest::builder().asset_id(token_id).build();
    match client.orders(&req, None).await {
        Ok(page) => !page.data.is_empty(),
        Err(_) => false
    }
}

pub async fn reconcile_orphaned_positions(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    positions: &Arc<Mutex<PositionMap>>,
    tokens: &[(U256, &str)],
    market_name: &str,
    market_close_time: Option<chrono::DateTime<Utc>>,
    token_bids: &[(U256, Decimal)],
) {
    for &(token_id, side_label) in tokens {
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Conditional;
        req.token_id = Some(token_id);
        let actual_shares = match client.balance_allowance(req).await {
            Ok(resp) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
            Err(_) => continue,
        };
        if actual_shares < crate::config::MIN_ORDER_SHARES { continue; }
        if positions.lock().await.iter().any(|((_, tid), _)| *tid == token_id) { continue; }

        if let Some(strategy_name) = ADOPTION_STRATEGIES.iter().find(|&&s| !positions.lock().await.contains_key(&(s.to_string(), token_id))) {
            let mut pos_map = positions.lock().await;
            let avg_entry = token_bids.iter().find(|(tid, _)| *tid == token_id).map(|(_, bid)| *bid).filter(|b| *b > dec!(0)).unwrap_or(dec!(0.50));
            pos_map.insert((strategy_name.to_string(), token_id), Position { shares: actual_shares, avg_entry, opened_at: Utc::now() - chrono::Duration::seconds(crate::config::MIN_HOLD_SECS_BEFORE_STOP_LOSS), close_time: market_close_time, pair_token_id: token_id, fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None });
            warn!("🔁 RECONCILE: Adopted {} {} shares for token {} under [{}]", actual_shares, side_label, token_id, strategy_name);
        }
    }
}

pub async fn quick_confirm_fill(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    strategy_name: &str,
    token_id: U256,
    positions: &Arc<Mutex<PositionMap>>,
    condition_id: &str,
) -> Result<bool> {
    let market_hash = match B256::from_str(condition_id) { Ok(h) => h, Err(_) => return Ok(false) };
    let req = polymarket_client_sdk::clob::types::request::CancelMarketOrderRequest::builder().market(market_hash).build();
    let _ = client.cancel_market_orders(&req).await;
    if !check_for_resting_order(client, token_id).await {
        let mut pos_map = positions.lock().await;
        if let Some(pos) = pos_map.get_mut(&(strategy_name.to_string(), token_id)) {
            pos.fill_confirmed_at = Some(Utc::now());
            info!("✅ QUICK CONFIRM FILL [{}]: Token {} filled instantly", strategy_name, token_id);
            return Ok(true);
        }
    }
    Ok(false)
}
