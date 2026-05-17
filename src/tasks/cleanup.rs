/// Background task: position cleanup and orphan reconciliation.
///
/// Runs every 300 seconds (5 minutes) to:
/// 1. Remove positions for markets that have expired or are expiring within 60s.
/// 2. Detect and exit orphaned paired positions (ArbitrageStrategy / TimeDecayStrategy)
///    where the first leg filled but the second leg never did.
/// 3. Prune expired TimeDecay position metadata.
/// 4. Sync open_positions DB table against live on-chain holdings (purge stale rows).
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::Mutex;
use tokio::time::timeout as tokio_timeout;
use tracing::{info, warn};

use polymarket_client_sdk_v2::ctf::Client as CtfClient;
use polymarket_client_sdk_v2::ctf::types::{MergePositionsRequest, RedeemNegRiskRequest, RedeemPositionsRequest};
use polymarket_client_sdk_v2::clob::Client as ClobClient;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
use polymarket_client_sdk_v2::data::Client as DataClient;
use polymarket_client_sdk_v2::data::types::request::PositionsRequest;
use polymarket_client_sdk_v2::{POLYGON, contract_config};

use crate::helpers::balance::PhantomCooldowns;
use crate::helpers::{db, notifications::send_notification};
use crate::state::{Position, PositionMap};
use crate::strategies::time_decay_impl::TimeDecayPosition;

/// Remove all positions for a market that has expired or is expiring within 60s.
pub async fn cleanup_expired_positions(
    positions: Arc<Mutex<PositionMap>>,
    market_name: String,
    yes_token: U256,
    no_token: U256,
    close_time: Option<DateTime<Utc>>,
) {
    let mut pos_map = positions.lock().await;
    let now = Utc::now();

    if let Some(ct) = close_time {
        let is_expired = ct <= now;
        let is_expiring_soon = (ct - now).num_seconds() < 60;

        if is_expired || is_expiring_soon {
            let before = pos_map.len();
            pos_map.retain(|(_, token), _| token != &yes_token && token != &no_token);
            let removed = before - pos_map.len();

            if removed > 0 {
                warn!("🧹 Cleaned up {} position(s) for market \"{}\" (expires {})",
                    removed,
                    market_name,
                    if is_expired { "NOW" } else { "in <60s" }
                );
            }
        }
    }
}

/// Detect orphaned paired positions and remove them from tracking.
///
/// For ArbitrageStrategy and TimeDecayStrategy, positions must come in hedged
/// pairs (YES+NO). If Leg A fills but Leg B fails, a Flash-Exit task spawned in
/// main.rs will attempt an emergency sell within ~5-12 s of indexer confirmation.
/// This function is the SLOW backstop: it catches any orphans the flash-exit
/// missed (e.g., rare silent FAK failure with no explicit Err) by scanning for
/// paired positions that are still unpaired after 60 s, then logging and removing
/// them so they don't silently accumulate capital exposure.
///
/// IMPORTANT: Before removing an orphaned position from internal tracking we also
/// cancel any resting GTC order for that token on the Polymarket CLOB.  Without this,
/// a GTC order that was "forgotten" by position-sync can later fill and create an
/// untracked, unhedged position in the wallet.
pub async fn reconcile_orphaned_positions(
    positions: Arc<Mutex<PositionMap>>,
    clob_client: &Arc<ClobClient<Authenticated<Normal>>>,
    phantom_cooldowns: &PhantomCooldowns,
    tg_token: &str,
    tg_chat_id: &str,
) -> anyhow::Result<()> {
    let mut pos_map = positions.lock().await;
    let now = Utc::now();

    let mut orphans_to_exit: Vec<((String, U256), Position)> = Vec::new();

    for ((strategy_name, token_id), position) in pos_map.iter() {
        if strategy_name != "ArbitrageStrategy" && strategy_name != "TimeDecayStrategy" {
            continue;
        }
        let age_secs = (now - position.opened_at).num_seconds();
        if age_secs < 60 { continue; }

        if let Some(paired_token) = position.paired_leg_token_id {
            let pair_key = (strategy_name.clone(), paired_token);
            if pos_map.contains_key(&pair_key) { continue; }
            orphans_to_exit.push(((strategy_name.clone(), *token_id), position.clone()));
        }
    }

    for ((strategy_name, token_id), position) in orphans_to_exit {
        warn!("🚨 ORPHANED PAIR DETECTED [{}]: {} shares at ${:.4} ({}s old) — cancelling GTC + removing",
              strategy_name, position.shares, position.avg_entry,
              (now - position.opened_at).num_seconds());

        // Cancel any resting GTC order so it can't fill after we forget about it.
        // Hard 10s timeouts on both CLOB calls — same fix as the 2026-05-01 overnight freeze
        // (status_ticker arm). Without these, a TCP-level CLOB API stall inside the
        // cleanup_ticker select! arm blocks the ENTIRE event loop indefinitely.
        let req = OrdersRequest::builder().asset_id(token_id).build();
        match tokio_timeout(std::time::Duration::from_secs(10), clob_client.orders(&req, None)).await {
            Ok(Ok(page)) => {
                let ids: Vec<String> = page.data.into_iter().map(|o| o.id).collect();
                if !ids.is_empty() {
                    let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
                    warn!("🛑 Cancelling {} resting order(s) for orphaned token {}", id_refs.len(), token_id);
                    match tokio_timeout(std::time::Duration::from_secs(10), clob_client.cancel_orders(&id_refs)).await {
                        Ok(_) => {}
                        Err(_) => warn!("⚠️ Orphan cleanup: cancel_orders timed out (10s) for token {} — skipping cancel", token_id),
                    }
                }
            }
            Ok(Err(e)) => warn!("⚠️ Orphan cleanup: orders() error for token {}: {}", token_id, e),
            Err(_) => warn!("⚠️ Orphan cleanup: orders() timed out (10s) for token {} — skipping cancel", token_id),
        }

        pos_map.remove(&(strategy_name.clone(), token_id));

        // Block re-entry into this token for PHANTOM_COOLDOWN_SECS so the strategy
        // cannot immediately open a new position on top of untracked on-chain shares.
        phantom_cooldowns.lock().await.insert(
            format!("{}:{}", strategy_name, token_id),
            tokio::time::Instant::now(),
        );

        let _ = send_notification(tg_token, tg_chat_id,
            &format!("🚨 Orphaned pair exited [{}]: {} {} shares @ ${:.4}",
                     strategy_name,
                     if token_id == position.pair_token_id { "YES" } else { "NO" },
                     position.shares.trunc(),
                     position.avg_entry)).await;
    }

    Ok(())
}

/// Prune expired TimeDecay position metadata entries.
pub async fn cleanup_time_decay_positions(
    td_positions: Arc<Mutex<HashMap<U256, TimeDecayPosition>>>,
) {
    let mut td_map = td_positions.lock().await;
    td_map.retain(|_, pos| !pos.is_expired());
}

/// Sync the `open_positions` DB table against the wallet's actual live holdings on
/// Polymarket.  Runs at startup and every 300 s.
///
/// Two-way reconciliation:
///   PURGE  — DB rows whose token is no longer held on-chain (settled, sold, crashed
///             session that never called close_open_position, orphan that was flash-
///             exited) are deleted so stale rows don't inflate the portfolio value.
///
///   ADOPT  — On-chain positions that have no DB row (opened in a previous session
///             that crashed before writing the row, or positions entered manually
///             on the Polymarket UI) are re-inserted so the UI and portfolio value
///             reflect the full wallet.
///
/// IMPORTANT: token IDs are stored in the DB as *decimal* U256 strings
/// (from `U256::to_string()`).  The Data API also returns `asset` as U256, so
/// we must use `p.asset.to_string()` — NOT `format!("{:#x}", p.asset)` — when
/// building the live-ID set; the hex format would never match and would cause
/// every valid DB row to be wrongly purged on every tick.
pub async fn sync_open_positions_with_chain(safe_address: Address) {
    let pool = match db::pool() {
        Some(p) => p,
        None => { warn!("⚠️ Chain-sync: DB pool not available, skipping"); return; }
    };

    let data_client = DataClient::default();
    let req = PositionsRequest::builder().user(safe_address).build();

    let live_positions = match tokio::time::timeout(
        std::time::Duration::from_secs(15),
        data_client.positions(&req),
    ).await {
        Ok(Ok(p))  => p,
        Ok(Err(e)) => { warn!("⚠️ Chain-sync: Polymarket Data API error: {}", e); return; }
        Err(_)     => { warn!("⚠️ Chain-sync: Polymarket Data API timed out (15s)"); return; }
    };

    // Build map: decimal_token_id → &Position  (size > 0 only).
    // MUST use p.asset.to_string() (decimal) — the DB stores token_id as decimal U256.
    let live_map: std::collections::HashMap<String, &_> = live_positions
        .iter()
        .filter(|p| p.size > rust_decimal::Decimal::ZERO)
        .map(|p| (p.asset.to_string(), p))
        .collect();

    // ── Purge stale DB rows ───────────────────────────────────────────────────
    let live_ids: HashSet<String> = live_map.keys().cloned().collect();
    let purged = db::purge_stale_open_positions(pool, &live_ids).await;

    // ── Re-adopt on-chain positions missing from DB ───────────────────────────
    // Query current DB token_ids AFTER the purge so we don't re-adopt something
    // that was just correctly removed.
    let db_ids: HashSet<String> = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT token_id FROM open_positions"
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .into_iter()
    .collect();

    let mut adopted = 0usize;
    for (token_str, pos) in &live_map {
        if !db_ids.contains(token_str) {
            if db::adopt_chain_position(pool, token_str, &pos.title, pos.avg_price, pos.size).await {
                adopted += 1;
                info!("📥 Chain-sync: re-adopted on-chain position — token {} | {} shares @ ${:.4} | \"{}\"",
                    &token_str[..token_str.len().min(20)],
                    pos.size, pos.avg_price, pos.title);
            }
        }
    }

    if purged > 0 {
        info!("🧹 Chain-sync: purged {} stale open_positions row(s) (not found on-chain)", purged);
    }
    if purged == 0 && adopted == 0 {
        info!("✅ Chain-sync: open_positions DB is in sync with on-chain holdings ({} live)", live_map.len());
    }
}

fn shares_to_base_units(shares: Decimal) -> u128 {
    // CTF contract methods consume 6-decimal fixed-point token amounts.
    (shares.max(Decimal::ZERO).trunc_with_scale(6) * Decimal::from(1_000_000u32))
        .trunc()
        .to_u128()
        .unwrap_or(0)
}

/// Scan for resolved/closed markets and auto-settle positions so wallet + UI stay in sync.
///
/// - `mergeable`: merge matched YES/NO full sets back to collateral.
/// - `redeemable`: redeem resolved winning outcome tokens.
pub async fn auto_settle_closed_positions<P: Provider + Clone>(
    safe_address: Address,
    ctf_client: &Arc<CtfClient<P>>,
    ctf_neg_risk_client: &Arc<CtfClient<P>>,
) -> bool {
    let data_client = DataClient::default();
    let req = PositionsRequest::builder().user(safe_address).build();

    let positions = match tokio::time::timeout(
        std::time::Duration::from_secs(20),
        data_client.positions(&req),
    )
    .await
    {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            warn!("⚠️ Auto-settle: Data API positions() error: {}", e);
            return false;
        }
        Err(_) => {
            warn!("⚠️ Auto-settle: Data API positions() timed out (20s)");
            return false;
        }
    };

    let mut by_condition: HashMap<B256, Vec<_>> = HashMap::new();
    for p in positions.into_iter().filter(|p| p.size > Decimal::ZERO) {
        by_condition.entry(p.condition_id).or_default().push(p);
    }

    let min_merge_units = shares_to_base_units(crate::config::MIN_MERGE_SHARES);
    let mut settled_any = false;

    for (condition_id, legs) in by_condition {
        let has_mergeable = legs.iter().any(|p| p.mergeable);
        let has_redeemable = legs.iter().any(|p| p.redeemable);
        if !has_mergeable && !has_redeemable {
            continue;
        }

        let is_neg_risk = legs.iter().any(|p| p.negative_risk);
        let Some(cfg) = contract_config(POLYGON, is_neg_risk) else {
            warn!("⚠️ Auto-settle: missing contract config for chain={} neg_risk={}", POLYGON, is_neg_risk);
            continue;
        };

        let mut outcome_units: BTreeMap<i32, u128> = BTreeMap::new();
        for p in &legs {
            let units = shares_to_base_units(p.size);
            if units == 0 {
                continue;
            }
            *outcome_units.entry(p.outcome_index).or_insert(0) += units;
        }

        if has_mergeable && outcome_units.len() >= 2 {
            let merge_units = outcome_units.values().copied().min().unwrap_or(0);
            if merge_units >= min_merge_units {
                let merge_req = MergePositionsRequest::for_binary_market(
                    cfg.collateral,
                    condition_id,
                    U256::from(merge_units),
                );
                let merge_result = if is_neg_risk {
                    ctf_neg_risk_client.merge_positions(&merge_req).await
                } else {
                    ctf_client.merge_positions(&merge_req).await
                };

                match merge_result {
                    Ok(resp) => {
                        settled_any = true;
                        info!(
                            "🔄 Auto-settle: merged {} full-set shares for condition {} (tx {})",
                            Decimal::from(merge_units) / Decimal::from(1_000_000u32),
                            condition_id,
                            resp.transaction_hash
                        );
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        if err_str.contains("401") || err_str.contains("403") ||
                           err_str.contains("API key") || err_str.contains("tenant disabled") ||
                           err_str.contains("Unauthorized") {
                            warn!(
                                "🔑 ⚠️ Auto-settle: RPC authentication error (merge failed for {}): {}",
                                condition_id,
                                e
                            );
                            warn!("   → Check POLYGON_RPC_URL env var. API key may be disabled or expired.");
                            warn!("   → Fallback: Use public endpoint https://polygon-rpc.com");
                        } else {
                            warn!(
                                "⚠️ Auto-settle: merge failed for condition {} (neg_risk={}): {}",
                                condition_id,
                                is_neg_risk,
                                e
                            );
                        }
                    }
                }
            }
        }

        if has_redeemable {
            if is_neg_risk {
                let yes_units = *outcome_units.get(&0).unwrap_or(&0);
                let no_units = *outcome_units.get(&1).unwrap_or(&0);
                if yes_units == 0 && no_units == 0 {
                    continue;
                }

                let redeem_req = RedeemNegRiskRequest::builder()
                    .condition_id(condition_id)
                    .amounts(vec![U256::from(yes_units), U256::from(no_units)])
                    .build();

                match ctf_neg_risk_client.redeem_neg_risk(&redeem_req).await {
                    Ok(resp) => {
                        settled_any = true;
                        info!(
                            "🏁 Auto-settle: redeemed neg-risk condition {} (tx {})",
                            condition_id,
                            resp.transaction_hash
                        );
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        if err_str.contains("401") || err_str.contains("403") ||
                           err_str.contains("API key") || err_str.contains("tenant disabled") {
                            warn!(
                                "🔑 ⚠️ Auto-settle: RPC authentication error (neg-risk redeem failed for {}): {}",
                                condition_id,
                                e
                            );
                            warn!("   → Check POLYGON_RPC_URL env var. API key may be disabled or expired.");
                        } else {
                            warn!(
                                "⚠️ Auto-settle: neg-risk redeem failed for condition {}: {}",
                                condition_id,
                                e
                            );
                        }
                    }
                }
            } else {
                let redeem_req = RedeemPositionsRequest::for_binary_market(cfg.collateral, condition_id);
                match ctf_client.redeem_positions(&redeem_req).await {
                    Ok(resp) => {
                        settled_any = true;
                        info!(
                            "🏁 Auto-settle: redeemed condition {} (tx {})",
                            condition_id,
                            resp.transaction_hash
                        );
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        if err_str.contains("401") || err_str.contains("403") ||
                           err_str.contains("API key") || err_str.contains("tenant disabled") {
                            warn!(
                                "🔑 ⚠️ Auto-settle: RPC authentication error (redeem failed for {}): {}",
                                condition_id,
                                e
                            );
                            warn!("   → Check POLYGON_RPC_URL env var. API key may be disabled or expired.");
                        } else {
                            warn!(
                                "⚠️ Auto-settle: redeem failed for condition {}: {}",
                                condition_id,
                                e
                            );
                        }
                    }
                }
            }
        }
    }

    settled_any
}

