use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use regex::Regex;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::str::FromStr;
use alloy::primitives::U256;
use alloy::primitives::B256;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use chrono::Utc;
use tracing::{debug, error, info, warn};

use polymarket_client_sdk_v2::clob::{Client as ClobClient};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::clob::types::request::{BalanceAllowanceRequest, OrdersRequest};
use polymarket_client_sdk_v2::clob::types::{AssetType, OrderType}; // Import OrderType

use crate::helpers::metrics;

pub use crate::state::{Position, PositionMap};

/// Shared map of (strategy:token_id) → Instant for phantom removal cooldowns.
pub type PhantomCooldowns = Arc<Mutex<HashMap<String, Instant>>>;

/// Session-scoped set of token IDs that have been through orphan-detection and removal.
/// Never cleared on market switch — once a token is tombstoned it is never re-adopted
/// within the same session. This breaks the infinite reconcile→re-adopt→orphan-detect cycle
/// that occurs when an unhedged daily-market leg persists across many hourly rotations.
pub type OrphanTombstones = Arc<Mutex<HashSet<U256>>>;

/// How long to block re-entry after a phantom removal (seconds).
///
/// MUST be longer than the cleanup_ticker interval (300s) so that after a YES leg is
/// phantom-removed, the cleanup cycle has time to detect and untrack the orphaned NO leg
/// BEFORE the cooldown expires and a new arb entry can fire.
/// Previously 120s — this allowed re-entry at 120s even though cleanup hadn't run yet
/// (up to 300s), causing repeated NO-only accumulation cycles.
pub const PHANTOM_COOLDOWN_SECS: u64 = 600;

/// Max seconds to wait for an on-chain balance to appear after an order.
pub const MAX_WAIT_SECS_HOURLY: i64 = 180;
pub const MAX_WAIT_SECS_WINDOW: i64 = 600;



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
                    // Cancel any resting GTC order to prevent future unexpected fills.
                    // A GTC order that rested and was later matched may briefly show
                    // zero balance AND zero resting orders while Polygon settles the fill.
                    // After cancelling, wait one grace period and re-check the balance
                    // before declaring phantom — this catches the settlement-lag race.
                    let had_resting = cancel_resting_orders(client, token_id).await;
                    if had_resting {
                        // Order was live → may have been matching. Wait for on-chain settlement.
                        tokio::time::sleep(Duration::from_secs(20)).await;
                        // Re-check the balance one final time.
                        let mut req2 = BalanceAllowanceRequest::default();
                        req2.asset_type = AssetType::Conditional;
                        req2.token_id = Some(token_id);
                        if let Ok(resp) = client.balance_allowance(req2).await {
                            let latest = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
                            let latest_actual = (latest - baseline_shares).max(dec!(0));
                            if latest_actual >= crate::config::MIN_ORDER_SHARES {
                                // The order DID fill — update the position and continue normally.
                                let mut pos_map = positions.lock().await;
                                if let Some(pos) = pos_map.get_mut(&key) {
                                    warn!("✅ Position Sync RECOVERED [{}]: Token {} filled after cancel ({} shares) — keeping position",
                                          strategy_name, token_id, latest_actual);
                                    pos.shares = latest_actual;
                                    if pos.fill_confirmed_at.is_none() { pos.fill_confirmed_at = Some(Utc::now()); }
                                }
                                return Ok(());
                            }
                        }
                    }
                    error!("⚠️ Position Sync FAILED [{}] Token {} — phantom removed.", strategy_name, token_id);
                    positions.lock().await.remove(&key);
                    if let Some(cooldowns) = phantom_cooldowns {
                        cooldowns.lock().await.insert(format!("{}:{}", strategy_name, token_id), Instant::now());
                    }
                    return Ok(());
                } else {
                    if time_since_open > 15 {
                        // First warning fires at ~15s.  After that, throttle to once per
                        // 60 seconds so GTC orders resting on a slow daily market don't
                        // flood the log with hundreds of identical WARN lines.
                        if time_since_open <= 20 || time_since_open % 60 < 4 {
                            warn!("⚠️ Position Sync [{}]: Token {} balance is 0 ({}s since open). Retrying...", strategy_name, token_id, time_since_open);
                        } else {
                            debug!("⏳ Position Sync [{}]: Token {} balance is 0 ({}s since open). Retrying...", strategy_name, token_id, time_since_open);
                        }
                    }
                    drop(pos_map);
                    tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                    continue;
                }
            } else { return Ok(()); }
        } else {
            // Position key not yet in the map (race between order submission and on-chain
            // confirmation).  Must explicitly drop the guard before sleeping — holding a
            // tokio::sync::MutexGuard across an .await is legal but keeps the lock live,
            // which means the subsequent `positions.lock().await` below would self-deadlock
            // (tokio Mutex is non-reentrant: same task trying to re-lock → hangs forever).
            drop(pos_map);
            tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
            if !positions.lock().await.contains_key(&key) { return Ok(()); }
        }
    }
}

/// Fetch all open orders for a token and cancel them.
/// Returns true if any orders were found (and cancelled).
/// This prevents GTC orders that were "forgotten" by position-sync from
/// sitting on the book and filling unexpectedly later.
async fn cancel_resting_orders(client: &Arc<ClobClient<Authenticated<Normal>>>, token_id: U256) -> bool {
    let req = OrdersRequest::builder().asset_id(token_id).build();
    let order_ids: Vec<String> = match client.orders(&req, None).await {
        Ok(page) => page.data.into_iter().map(|o| o.id).collect(),
        Err(_) => return false,
    };
    if order_ids.is_empty() {
        return false;
    }
    let id_refs: Vec<&str> = order_ids.iter().map(|s| s.as_str()).collect();
    warn!("🛑 Cancelling {} resting GTC order(s) for token {} — preventing orphan fills",
          id_refs.len(), token_id);
    let _ = client.cancel_orders(&id_refs).await;
    true
}

/// Reconcile on-chain token balances against the in-memory position map.
/// `adoption_order` is the ordered list of strategy names to try when assigning an
/// orphaned position — derived from `StrategyRegistry::strategy_names()` so that
/// developers only need to register a strategy in the registry, not also edit this file.
///
/// `orphan_tombstones` (optional) is the session-scoped set of token IDs that have been
/// through the full orphan-detection cycle.  Tombstoned tokens are skipped permanently so
/// the reconcile → re-adopt → orphan-detect loop cannot repeat across market switches.
pub async fn reconcile_orphaned_positions(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    positions: &Arc<Mutex<PositionMap>>,
    tokens: &[(U256, &str)],
    market_name: &str,
    market_close_time: Option<chrono::DateTime<Utc>>,
    token_bids: &[(U256, Decimal)],
    adoption_order: &[String],
    orphan_tombstones: Option<&OrphanTombstones>,
) {
    for &(token_id, side_label) in tokens {
        // ── Tombstone check ───────────────────────────────────────────────────
        // If this token has been through orphan-detection this session, skip it
        // permanently.  Without this, cleanup removes the orphan, phantom_cooldowns
        // are cleared on the next hourly market switch, and this reconcile re-adopts it —
        // restarting the infinite detect→remove→re-adopt cycle.
        if let Some(tb) = orphan_tombstones {
            if tb.lock().await.contains(&token_id) {
                debug!("⏭️  RECONCILE: skipping tombstoned token {} — previously orphan-removed this session", token_id);
                continue;
            }
        }

        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Conditional;
        req.token_id = Some(token_id);
        let actual_shares = match client.balance_allowance(req).await {
            Ok(resp) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
            Err(e) => {
                warn!("⚠️ RECONCILE: balance query failed for token {} ({}): {}", token_id, side_label, e);
                continue;
            }
        };
        debug!("🔍 RECONCILE: token {} ({}) on-chain balance = {:.4}", token_id, side_label, actual_shares);
        if actual_shares < crate::config::MIN_ORDER_SHARES {
            debug!("⏭️  RECONCILE: skipping token {} — balance {:.4} below threshold", token_id, actual_shares);
            continue;
        }

        {
            let map = positions.lock().await;
            if map.iter().any(|((_, tid), _)| *tid == token_id) { continue; }
        }

        let current_bid = token_bids.iter().find(|(tid, _)| *tid == token_id)
            .map(|(_, bid)| *bid)
            .filter(|b| *b > dec!(0))
            .unwrap_or(dec!(0.50));

        // Try to recover real entry price AND originating strategy from the entry log
        // written at order time.  This is the authoritative source — the bot writes a
        // row to the entries DB immediately after each successful place_limit_order, so
        // if the bot crashed mid-session both the cost basis and the strategy name are
        // preserved.  Using the recorded strategy avoids misattributing ArbitrageStrategy
        // or GboostStrategy orphans to MomentumStrategy simply because Momentum happens
        // to be first in the registry's adoption_order list.
        //
        // If no log entry exists (e.g. entry predates this feature, or logs dir was wiped),
        // fall back to the first available strategy in adoption_order and a discounted bid.
        let db_entry = metrics::lookup_entry_from_csv(&token_id.to_string()).await;
        let (avg_entry, logged_strategy) = match db_entry {
            Some((real_entry, ref strat)) if !strat.is_empty() => {
                warn!("🔁 RECONCILE: Recovered entry_price={:.4} strategy={} for token {} from entry log",
                    real_entry, strat, token_id);
                (real_entry, Some(strat.clone()))
            }
            Some((real_entry, _)) => {
                warn!("🔁 RECONCILE: Recovered entry_price={:.4} for token {} (no strategy in log)", real_entry, token_id);
                (real_entry, None)
            }
            None => {
                // No log found — credit an artificial entry below current bid so profit_margin
                // is immediately above every strategy's take-profit threshold on the next tick.
                (current_bid * (dec!(1) - crate::config::RECONCILE_ADOPTED_ENTRY_DISCOUNT), None)
            }
        };

        // Determine which strategy should own this position.
        // Priority: (1) strategy recorded in the entry log, (2) first in adoption_order.
        let adopted_strategy = if let Some(ref logged) = logged_strategy {
            // Verify the strategy isn't already tracking this token before using it.
            let map = positions.lock().await;
            if !map.contains_key(&(logged.clone(), token_id)) {
                Some(logged.clone())
            } else {
                // Logged strategy already has this token — fall back to adoption_order.
                drop(map);
                let mut fallback = None;
                for s in adoption_order {
                    let map = positions.lock().await;
                    if !map.contains_key(&(s.clone(), token_id)) {
                        fallback = Some(s.clone());
                        break;
                    }
                }
                fallback
            }
        } else {
            let mut fallback = None;
            for s in adoption_order {
                let map = positions.lock().await;
                if !map.contains_key(&(s.clone(), token_id)) {
                    fallback = Some(s.clone());
                    break;
                }
            }
            fallback
        };

        if let Some(strategy_name) = adopted_strategy {
            let mut pos_map = positions.lock().await;

            pos_map.insert((strategy_name.to_string(), token_id), Position {
                shares: actual_shares,
                avg_entry,
                opened_at: Utc::now() - chrono::Duration::seconds(crate::config::MIN_HOLD_SECS_BEFORE_STOP_LOSS),
                close_time: market_close_time,
                market_name: market_name.to_string(),
                pair_token_id: token_id,
                fill_confirmed_at: Some(Utc::now()),
                paired_leg_token_id: None, // fixed up below
            });

            let source = if logged_strategy.is_some() { "DB" } else {
                &format!("discount@{:.0}%", crate::config::RECONCILE_ADOPTED_ENTRY_DISCOUNT * dec!(100))
            };
            warn!("🔁 RECONCILE: Adopted {} {} shares for token {} under [{}] — avg_entry={:.4} (source={}, current_bid={:.4})",
                actual_shares, side_label, token_id, strategy_name, avg_entry, source, current_bid);
        }
    }

    // ── Wire paired_leg_token_id so the cleanup orphan-detector works ─────────
    // Each position is inserted above with paired_leg_token_id: None, which means
    // the cleanup.rs orphan reconciler (which checks `if let Some(paired) = ...`)
    // would never fire for session-adopted positions.
    //
    // Post-pass: if exactly 2 tokens were provided (YES + NO pair), point each
    // adopted position at the other token.  If only one leg has an on-chain
    // balance (naked orphan), the cleanup cycle will detect the missing pair on
    // its next run and remove the position while preventing phantom re-entry.
    if tokens.len() == 2 {
        let (tok_a, _) = tokens[0];
        let (tok_b, _) = tokens[1];
        let mut pos_map = positions.lock().await;

        // Find which strategy adopted each token (may be None if balance was 0)
        let strat_a = pos_map.iter()
            .find(|((_, tid), _)| *tid == tok_a)
            .map(|((s, _), _)| s.clone());
        let strat_b = pos_map.iter()
            .find(|((_, tid), _)| *tid == tok_b)
            .map(|((s, _), _)| s.clone());

        if let Some(sa) = strat_a {
            if let Some(p) = pos_map.get_mut(&(sa, tok_a)) {
                p.paired_leg_token_id = Some(tok_b);
            }
        }
        if let Some(sb) = strat_b {
            if let Some(p) = pos_map.get_mut(&(sb, tok_b)) {
                p.paired_leg_token_id = Some(tok_a);
            }
        }
    }
}

pub async fn quick_confirm_fill(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    strategy_name: &str,
    token_id: U256,
    positions: &Arc<Mutex<PositionMap>>,
    condition_id: &str,
    order_type: OrderType, // Added order_type parameter
) -> Result<bool> {
    // Only quick-confirm FAK orders. GTC orders need to wait for on-chain sync.
    if order_type != OrderType::FAK {
        return Ok(false);
    }

    let market_hash = match B256::from_str(condition_id) { Ok(h) => h, Err(_) => return Ok(false) };
    let req = polymarket_client_sdk_v2::clob::types::request::CancelMarketOrderRequest::builder().market(market_hash).build();
    let _ = client.cancel_market_orders(&req).await;
    // After cancelling all market orders, check if there are any remaining resting orders
    // for this specific token (belt-and-suspenders: cancel_market_orders covers everything).
    let req2 = OrdersRequest::builder().asset_id(token_id).build();
    if !(match client.orders(&req2, None).await { Ok(p) => p.data.is_empty(), Err(_) => true }) {
        let mut pos_map = positions.lock().await;
        if let Some(pos) = pos_map.get_mut(&(strategy_name.to_string(), token_id)) {
            pos.fill_confirmed_at = Some(Utc::now());
            info!("✅ QUICK CONFIRM FILL [{}]: Token {} filled instantly", strategy_name, token_id);
            return Ok(true);
        }
    }
    Ok(false)
}
