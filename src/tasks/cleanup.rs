/// Background task: position cleanup and orphan reconciliation.
///
/// Runs every 300 seconds (5 minutes) to:
/// 1. Remove positions for markets that have expired or are expiring within 60s.
/// 2. Detect and exit orphaned paired positions (ArbitrageStrategy / TimeDecayStrategy)
///    where the first leg filled but the second leg never did.
/// 3. Prune expired TimeDecay position metadata.
/// 4. Sync open_positions DB table against live on-chain holdings (purge stale rows).
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alloy::primitives::{Address, U256};
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tracing::{info, warn};

use polymarket_client_sdk_v2::clob::Client as ClobClient;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
use polymarket_client_sdk_v2::data::Client as DataClient;
use polymarket_client_sdk_v2::data::types::request::PositionsRequest;

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
        let req = OrdersRequest::builder().asset_id(token_id).build();
        if let Ok(page) = clob_client.orders(&req, None).await {
            let ids: Vec<String> = page.data.into_iter().map(|o| o.id).collect();
            if !ids.is_empty() {
                let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
                warn!("🛑 Cancelling {} resting order(s) for orphaned token {}", id_refs.len(), token_id);
                let _ = clob_client.cancel_orders(&id_refs).await;
            }
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

