/// Background task: position merge scanner.
///
/// Runs every MERGE_SCAN_INTERVAL_SECS seconds.  For each tracked market,
/// if both YES and NO positions exist under the same strategy and the smaller
/// side is at least MIN_MERGE_RATIO of the larger side, the overlapping shares
/// are merged on-chain via the Polymarket Conditional Token contracts,
/// recovering USDC immediately without waiting for market resolution.
///
/// Contract addresses (Polygon):
///   NegRiskAdapter:    0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296
///   ConditionalTokens: 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
///
/// The merge call is executed by shelling out to the existing Node.js
/// poly_merger script so we reuse the proven Safe-wallet signing path.
/// A native alloy-based implementation can replace this in a future iteration.
use std::collections::HashMap;
use std::sync::Arc;

use alloy::primitives::U256;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::config;
use crate::notifications::send_notification;
use crate::state::PositionMap;

/// Describes one market that the merge scanner should watch.
#[derive(Clone, Debug)]
pub struct MergeMarket {
    pub yes_token: U256,
    pub no_token: U256,
    /// Polymarket condition ID (bytes32 hex string, e.g. "0xabc123...").
    /// Required by the on-chain merge contracts.
    pub condition_id: String,
    pub is_neg_risk: bool,
    pub market_name: String,
    /// Strategy name whose positions should be scanned (e.g. "MakerStrategy").
    pub strategy_name: String,
}

/// Tracks the last merge attempt time per market to avoid hammering the chain.
type LastMergeMap = HashMap<String, std::time::Instant>;

/// Entry point: spawned once from main.rs, runs for the lifetime of the bot.
pub async fn run_merge_scanner(
    positions: Arc<Mutex<PositionMap>>,
    markets: Arc<Mutex<Vec<MergeMarket>>>,
    tg_token: String,
    tg_chat_id: String,
) {
    let mut interval = tokio::time::interval(
        std::time::Duration::from_secs(config::MERGE_SCAN_INTERVAL_SECS)
    );
    let mut last_merge: LastMergeMap = HashMap::new();

    loop {
        interval.tick().await;

        let market_list = markets.lock().await.clone();
        for market in &market_list {
            // Rate-limit: skip if we attempted a merge for this market recently.
            if let Some(last) = last_merge.get(&market.condition_id) {
                if last.elapsed().as_secs() < config::MERGE_COOLDOWN_SECS {
                    continue;
                }
            }

            // Skip markets with no condition_id (fast-path hourly markets don't have one).
            if market.condition_id.is_empty() {
                continue;
            }

            let (yes_shares, no_shares) = {
                let pos_map = positions.lock().await;
                let yes = pos_map
                    .get(&(market.strategy_name.clone(), market.yes_token))
                    .map(|p| p.shares)
                    .unwrap_or(dec!(0));
                let no = pos_map
                    .get(&(market.strategy_name.clone(), market.no_token))
                    .map(|p| p.shares)
                    .unwrap_or(dec!(0));
                (yes, no)
            };

            let mergeable = yes_shares.min(no_shares);
            if mergeable < config::MIN_MERGE_SHARES {
                continue;
            }

            // Ratio gate: only merge if the smaller side is at least MIN_MERGE_RATIO
            // of the larger side.  Prevents burning gas to merge a tiny sliver.
            let larger = yes_shares.max(no_shares);
            let ratio = mergeable / larger;
            if ratio < config::MIN_MERGE_RATIO {
                continue;
            }

            info!(
                "🔀 MERGE [{}]: YES={:.4} NO={:.4} → merging {:.4} pairs (ratio {:.2}%)",
                market.market_name, yes_shares, no_shares, mergeable, ratio * dec!(100)
            );

            last_merge.insert(market.condition_id.clone(), std::time::Instant::now());

            match call_merge_script(mergeable, &market.condition_id, market.is_neg_risk).await {
                Ok(tx_hash) => {
                    info!("✅ MERGE complete [{}]: tx={}", market.market_name, tx_hash);

                    // Subtract merged shares from both positions.
                    {
                        let mut pos_map = positions.lock().await;
                        let yes_key = (market.strategy_name.clone(), market.yes_token);
                        let no_key  = (market.strategy_name.clone(), market.no_token);

                        if let Some(p) = pos_map.get_mut(&yes_key) {
                            p.shares -= mergeable;
                            if p.shares < config::MIN_ORDER_SHARES {
                                pos_map.remove(&yes_key);
                            }
                        }
                        if let Some(p) = pos_map.get_mut(&no_key) {
                            p.shares -= mergeable;
                            if p.shares < config::MIN_ORDER_SHARES {
                                pos_map.remove(&no_key);
                            }
                        }
                    }

                    let recovered_usdc = mergeable; // 1 pair = $1 USDC
                    let _ = send_notification(
                        &tg_token, &tg_chat_id,
                        &format!(
                            "🔀 Merge complete [{}]: {:.4} pairs → ${:.4} USDC recovered | tx: {}",
                            market.market_name, mergeable, recovered_usdc, &tx_hash[..12.min(tx_hash.len())]
                        ),
                    ).await;
                }
                Err(e) => {
                    warn!("⚠️ MERGE failed [{}]: {} — will retry next cycle", market.market_name, e);
                }
            }
        }
    }
}

/// Execute the merge by calling the poly_merger Node.js script.
///
/// Amount is converted to raw units (shares × 1_000_000) as required by the
/// Polymarket contracts which use 6-decimal USDC.
async fn call_merge_script(
    shares: Decimal,
    condition_id: &str,
    is_neg_risk: bool,
) -> anyhow::Result<String> {
    // Convert shares to raw token units (6 decimals)
    let raw_amount = (shares * dec!(1_000_000))
        .to_u64()
        .ok_or_else(|| anyhow::anyhow!("shares overflow u64: {}", shares))?;

    let script_path = std::env::var("MERGE_SCRIPT_PATH")
        .unwrap_or_else(|_| "poly-maker/poly_merger/merge.js".to_string());

    let output = tokio::process::Command::new("node")
        .arg(&script_path)
        .arg(raw_amount.to_string())
        .arg(condition_id)
        .arg(if is_neg_risk { "true" } else { "false" })
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("merge script failed: {}", stderr));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    // The script prints "merge positions <txHash>" on success
    let tx_hash = stdout
        .lines()
        .find(|l| l.contains("merge positions"))
        .and_then(|l| l.split_whitespace().last())
        .unwrap_or("unknown")
        .to_string();

    Ok(tx_hash)
}

// Extension trait to convert Decimal → u64 safely
trait ToU64 {
    fn to_u64(&self) -> Option<u64>;
}

impl ToU64 for Decimal {
    fn to_u64(&self) -> Option<u64> {
        use rust_decimal::prelude::ToPrimitive;
        ToPrimitive::to_u64(self)
    }
}
