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
/// Now per-token instead of per-strategy so one side doesn't block the other.
pub type PhantomCooldowns = Arc<Mutex<HashMap<String, Instant>>>;

/// How long to block re-entry after a phantom removal (seconds).
/// Prevents the bot from immediately re-posting the same unfillable GTD order.
pub const PHANTOM_COOLDOWN_SECS: u64 = 120;

/// Max seconds to wait for an on-chain balance to appear after an order,
/// per venue. The window/4-hour CLOB is more aggressive about expiring GTD
/// post-only orders, so we give it a longer leash before declaring a phantom.
pub const MAX_WAIT_SECS_HOURLY: i64 = 180;
pub const MAX_WAIT_SECS_WINDOW: i64 = 600;

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
///
/// `baseline_shares` is the on-chain balance that existed *before* the order
/// was placed.  The true fill is `actual - baseline`, not the raw balance.
/// This prevents residual shares from previous trades inflating the position.
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
    let check_interval_ms: u64 = 3000;   // slightly faster polling

    tokio::time::sleep(Duration::from_millis(1500)).await;

    loop {
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Conditional;
        req.token_id = Some(token_id);

        let balance_resp = client.balance_allowance(req).await;
        let raw_shares = match balance_resp {
            Ok(resp) => Decimal::from_str(&resp.balance.to_string())
                .unwrap_or(dec!(0)) / dec!(1_000_000),
            Err(e) => {
                warn!("Position Sync API error [{}]: {}", strategy_name, e);
                // continue polling
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
                // Looser partial guard
                if fill_ratio < dec!(0.60) && time_since_open < 120 {
                    warn!("⚠️ Position Sync PARTIAL [{}]: Token {} has {:.4} of {:.4} expected ({:.0}%) — still settling. Retrying...",
                          strategy_name, token_id, actual_shares, expected, fill_ratio * dec!(100));
                    drop(pos_map);
                    tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                    continue;
                }

                info!("⚖️ Position Synced [{}]: Token {} updated from {} to actual: {} (raw: {}, baseline: {})",
                      strategy_name, token_id, pos.shares, actual_shares, raw_shares, baseline_shares);
                pos.shares = actual_shares;
                if pos.fill_confirmed_at.is_none() {
                    pos.fill_confirmed_at = Some(Utc::now());
                }
                return Ok(());
            } else if actual_shares == dec!(0) {
                if time_since_open >= max_wait_secs {
                    drop(pos_map);
                    // Check resting order EARLIER and MORE OFTEN
                    let has_resting = check_for_resting_order(client, token_id).await;
                    if has_resting {
                        debug!("⏳ Position Sync [{}]: Token {} still 0 after {}s but resting order found — keeping alive.",
                          strategy_name, token_id, time_since_open);
                        tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                        continue;
                    }

                    error!("⚠️ Position Sync FAILED [{}] Token {} — no open orders after {}s. Removing phantom.", strategy_name, token_id, max_wait_secs);
                    positions.lock().await.remove(&key);
                    if let Some(cooldowns) = phantom_cooldowns {
                        let cooldown_key = format!("{}:{}", strategy_name, token_id);
                        cooldowns.lock().await.insert(cooldown_key, Instant::now());
                    }
                    return Ok(());
                } else {
                    let time_since_open = (Utc::now() - pos.opened_at).num_seconds();
                    if pos.fill_confirmed_at.is_some() {
                        warn!("⚠️ Position Sync WARNING [{}]: Token {} balance disappeared (was confirmed at {:?}). Possible liquidation or indexer issue.",
                              strategy_name, token_id, pos.fill_confirmed_at);
                        return Ok(());
                    } else if time_since_open >= max_wait_secs {
                        // Before declaring phantom, check if a resting GTD order
                        // still exists in the CLOB for this token. If so, the order
                        // hasn't filled yet but is live — do not remove the position.
                        drop(pos_map);
                        let orders_req = OrdersRequest::builder()
                            .asset_id(token_id)
                            .build();
                        let has_resting_order = match client.orders(&orders_req, None).await {
                            Ok(page) => !page.data.is_empty(),
                            Err(_) => false, // if API fails, err on the side of caution and remove
                        };
                        if has_resting_order {
                            debug!("⏳ Position Sync [{}]: Token {} still 0 after {}s but resting order (GTC) found — keeping alive.",
                                  strategy_name, token_id, time_since_open);
                            tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                            continue;
                        }
                        warn!("⚠️ Position Sync FAILED [{}]: Token {} balance still 0 after {}s and no open orders in CLOB. \
                               GTD order expired without a fill, or was silently discarded (e.g. post-only rejected without an API error). Removing phantom position.",
                              strategy_name, token_id, time_since_open);
                        positions.lock().await.remove(&key);
                        if let Some(cooldowns) = phantom_cooldowns {
                            let cooldown_key = format!("{}:{}", strategy_name, token_id);
                            cooldowns.lock().await.insert(cooldown_key, Instant::now());
                        }
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
                if let Some(cooldowns) = phantom_cooldowns {
                    let cooldown_key = format!("{}:{}", strategy_name, token_id);
                    cooldowns.lock().await.insert(cooldown_key, Instant::now());
                }
                return Ok(());
            }
        }
    }
}

async fn check_for_resting_order(client: &Arc<ClobClient<Authenticated<Normal>>>, token_id: U256) -> bool {
    let req = OrdersRequest::builder().asset_id(token_id).build();
    match client.orders(&req, None).await {
        Ok(page) => !page.data.is_empty(),
        Err(e) => {
            warn!("CLOB orders API failed: {}", e);
            false // conservative: assume no order
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
    token_bids: &[(U256, Decimal)],  // (token_id, current_bid) for avg_entry
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

        if actual_shares <= dec!(0) || actual_shares < crate::config::MIN_ORDER_SHARES {
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
            // Use the current bid as avg_entry so stop-loss starts at breakeven.
            // Hardcoding $0.50 previously caused immediate stop-loss when bid != $0.50.
            let avg_entry = token_bids.iter()
                .find(|(tid, _)| *tid == token_id)
                .map(|(_, bid)| *bid)
                .filter(|b| *b > dec!(0))
                .unwrap_or(dec!(0.50)); // fallback only if bid unavailable
            // Pre-satisfy the stop-loss hold timer: orphaned shares are already
            // confirmed on-chain, so there's no reason to wait another 300s at a
            // bad price before allowing a stop-loss to fire.
            let orphan_opened_at = Utc::now()
                - chrono::Duration::seconds(crate::config::MIN_HOLD_SECS_BEFORE_STOP_LOSS);
            pos_map.insert(key, Position {
                shares: actual_shares,
                avg_entry,
                opened_at: orphan_opened_at,
                close_time: market_close_time,
                market_name: market_name.to_string(),
                pair_token_id: token_id,
                fill_confirmed_at: Some(Utc::now()), // already confirmed on-chain
                paired_leg_token_id: None, // reconciled orphans are single-leg (not part of a pair)
            });
            warn!(
                "🔁 RECONCILE: Adopted {} orphaned {} shares for token {} under [{}] on \"{}\"",
                actual_shares, side_label, token_id, strategy_name, market_name
            );
        }
    }
}

/// Fast fill confirmation using the "instant cancel" trick (Reddit-recommended).
/// Only affects the current market — safe for multi-strategy bots.
pub async fn quick_confirm_fill(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    strategy_name: &str,
    token_id: U256,
    positions: &Arc<Mutex<PositionMap>>,
    condition_id: &str,
) -> Result<bool> {
    // Convert string condition_id → B256
    let market_hash = match B256::from_str(condition_id) {
        Ok(hash) => hash,
        Err(e) => {
            warn!("⚠️ quick_confirm_fill: invalid condition_id '{}': {}", condition_id, e);
            return Ok(false);
        }
    };

    // Correct builder usage (non-exhaustive struct)
    let req = polymarket_client_sdk::clob::types::request::CancelMarketOrderRequest::builder()
        .market(market_hash)      // ← NO Some() here
        .build();

    let _ = client.cancel_market_orders(&req).await;

    // Check if our order is still resting
    let still_resting = check_for_resting_order(client, token_id).await;

    let mut pos_map = positions.lock().await;
    let key = (strategy_name.to_string(), token_id);

    if let Some(pos) = pos_map.get_mut(&key) {
        if !still_resting {
            pos.fill_confirmed_at = Some(Utc::now());
            info!("✅ QUICK CONFIRM FILL [{}]: Token {} filled on-chain instantly", strategy_name, token_id);
            return Ok(true);
        } else {
            debug!("⏳ Quick confirm [{}]: Token {} still resting (no fill yet)", strategy_name, token_id);
            return Ok(false);
        }
    }

    Ok(false)
}