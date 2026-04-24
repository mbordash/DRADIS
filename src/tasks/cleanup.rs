/// Background task: position cleanup and orphan reconciliation.
///
/// Runs every 300 seconds (5 minutes) to:
/// 1. Remove positions for markets that have expired or are expiring within 60s.
/// 2. Detect and exit orphaned paired positions (ArbitrageStrategy / TimeDecayStrategy)
///    where the first leg filled but the second leg never did.
/// 3. Prune expired TimeDecay position metadata.
use std::collections::HashMap;
use std::sync::Arc;

use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tracing::warn;

use crate::notifications::send_notification;
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
/// pairs (YES+NO). If the first leg fills but the second leg fails, we are left
/// with a one-sided position. This function detects such orphans (opened >60s
/// ago with no matching pair leg) and removes them, notifying via Telegram.
pub async fn reconcile_orphaned_positions(
    positions: Arc<Mutex<PositionMap>>,
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
        warn!("🚨 ORPHANED PAIR DETECTED [{}]: {} shares at ${:.4} ({}s old) — removing",
              strategy_name, position.shares, position.avg_entry,
              (now - position.opened_at).num_seconds());
        pos_map.remove(&(strategy_name.clone(), token_id));
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
