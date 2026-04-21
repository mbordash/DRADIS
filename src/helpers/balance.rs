use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use regex::Regex;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::str::FromStr;
use alloy::primitives::U256;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use chrono::Utc;
use tracing::{info, warn};

use polymarket_client_sdk::clob::{Client as ClobClient};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::request::{BalanceAllowanceRequest, OrdersRequest};
use polymarket_client_sdk::clob::types::AssetType;

pub use crate::state::{Position, PositionMap};

/// Shared map of strategy → Instant for phantom removal cooldowns.
/// When sync_position_balance removes a phantom, it records the time here.
/// The entry gate in main.rs checks this to prevent immediate re-entry.
pub type PhantomCooldowns = Arc<Mutex<HashMap<String, Instant>>>;

/// How long to block re-entry after a phantom removal (seconds).
/// Prevents the bot from immediately re-posting the same unfillable GTD order.
pub const PHANTOM_COOLDOWN_SECS: u64 = 120;

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
            let raw_shares = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
            // Subtract pre-order residual so we only count shares from THIS fill.
            let actual_shares = (raw_shares - baseline_shares).max(dec!(0));
            let mut pos_map = positions.lock().await;
            if let Some(pos) = pos_map.get_mut(&key) {
                if actual_shares >= crate::config::MIN_ORDER_SHARES {
                    info!("⚖️ Position Synced [{}]: Token {} quantity updated from {} to actual: {} (raw: {}, baseline: {})",
                          strategy_name, token_id, pos.shares, actual_shares, raw_shares, baseline_shares);
                    pos.shares = actual_shares;
                    if pos.fill_confirmed_at.is_none() {
                        pos.fill_confirmed_at = Some(Utc::now());
                    }
                    return Ok(());
                } else if actual_shares > dec!(0) {
                    // Dust fill: net new shares are non-zero but below MIN_ORDER_SHARES.
                    // Treat it the same as a zero balance: remove the position and apply phantom cooldown.
                    warn!("⚠️ Position Sync DUST [{}]: Token {} has only {} net new shares (raw: {}, baseline: {}, < MIN_ORDER_SHARES {}). Treating as phantom and removing.",
                          strategy_name, token_id, actual_shares, raw_shares, baseline_shares, crate::config::MIN_ORDER_SHARES);
                    pos_map.remove(&key);
                    drop(pos_map);
                    if let Some(cooldowns) = phantom_cooldowns {
                        cooldowns.lock().await.insert(strategy_name.to_string(), Instant::now());
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
                            warn!("⏳ Position Sync [{}]: Token {} balance still 0 after {}s but resting GTD order found in CLOB — keeping position alive.",
                                  strategy_name, token_id, time_since_open);
                            tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                            continue;
                        }
                        warn!("⚠️ Position Sync FAILED [{}]: Token {} balance still 0 after {}s and no open orders in CLOB. \
                               Order was silently discarded (likely post-only crossed book at placement). Removing phantom position.",
                              strategy_name, token_id, time_since_open);
                        positions.lock().await.remove(&key);
                        if let Some(cooldowns) = phantom_cooldowns {
                            cooldowns.lock().await.insert(strategy_name.to_string(), Instant::now());
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
                    cooldowns.lock().await.insert(strategy_name.to_string(), Instant::now());
                }
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

