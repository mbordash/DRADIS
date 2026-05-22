/// Background task: position cleanup and orphan reconciliation.
///
/// Runs every 300 seconds (5 minutes) to:
/// 1. Remove positions for markets that have expired or are expiring within 60s.
/// 2. Detect and exit orphaned paired positions (ArbitrageStrategy / TimeDecayStrategy)
///    where the first leg filled but the second leg never did.
/// 3. Prune expired TimeDecay position metadata.
/// 4. Sync open_positions DB table against live on-chain holdings (purge stale rows).
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::LazyLock;
use std::sync::Arc;

use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::Provider;
use alloy::sol;
use alloy::sol_types::SolCall;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::Mutex;
use tokio::time::timeout as tokio_timeout;
use tracing::{info, warn};

use polymarket_client_sdk_v2::clob::Client as ClobClient;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
use polymarket_client_sdk_v2::data::Client as DataClient;
use polymarket_client_sdk_v2::data::types::request::PositionsRequest;
use alloy::primitives::address as alloy_address;

use crate::helpers::{db, send_notification, PhantomCooldowns};
use crate::helpers::balance::OrphanTombstones;
use crate::state::{Position, PositionMap};
use crate::strategies::time_decay_impl::TimeDecayPosition;

// ── On-chain settlement contracts ────────────────────────────────────────────

/// pUSD (Polymarket USD) collateral token address on Polygon.
///
/// Polymarket v2 mints ALL outcome token positions with pUSD as the collateral.
/// The ERC1155 position ID is derived from (collateral, parentCollectionId, conditionId, indexSet),
/// so merge/redeem calls MUST pass pUSD here — NOT the raw USDC.e address returned by
/// contract_config().collateral — otherwise the CTF contract computes wrong position IDs,
/// finds balance = 0, and the tx succeeds as a silent no-op (no tokens burned, no payout).
///
/// Reference: https://docs.polymarket.com/developers/CTF/redeem-positions
const PUSD_COLLATERAL: Address = alloy_address!("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB");

/// Gnosis Conditional Token Framework contract on Polygon.
const CTF_ADDRESS: Address = alloy_address!("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045");

/// NegRisk adapter contract on Polygon (routes neg-risk market redemptions).
const NEG_RISK_ADAPTER_ADDRESS: Address = alloy_address!("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296");

/// Seconds to wait before re-submitting settlement for the same condition (indexer catch-up buffer).
///
/// Raised from 120s → 3600s (1 hour) on 2026-05-21.
///
/// Root cause of the duplicate-redemption bug observed in production:
/// After a successful CTF `redeemPositions` call, the Polymarket Data API
/// (Alchemy/subgraph indexer) continues returning the position as `redeemable: true`
/// for several minutes due to indexer lag.  With a 120s cooldown, every 5-minute
/// settlement_ticker cycle retried the redemption, submitted a live blockchain TX,
/// and "succeeded" (CTF burns 0 balance as a no-op) — wasting gas and advancing
/// the wallet nonce 8+ times in one session.  A 1-hour cooldown is safe because:
///   1. Settlement is fire-and-forget; a 1-hour retry window is more than enough
///      for the indexer to catch up.
///   2. If the first TX genuinely failed (dropped from mempool), the user will
///      see the position still live in their wallet and can manually settle.
const SETTLEMENT_CONDITION_COOLDOWN_SECS: i64 = 3600;

/// Seconds to skip a condition after a non-retryable settlement error (e.g. GS013 inner revert,
/// wrong parentCollectionId, zero Safe balance). 30 minutes — long enough to avoid endless spam
/// while still retrying in case the RPC or indexer was temporarily wrong.
const SETTLEMENT_CONDITION_ERROR_COOLDOWN_SECS: i64 = 1800;

/// Tracks when each condition_id was last successfully submitted for settlement.
static RECENT_SETTLEMENT_SUBMITS: LazyLock<Mutex<HashMap<B256, DateTime<Utc>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Conditions that were successfully redeemed this session.
///
/// After a successful `redeemPositions` TX the Polymarket Data API can keep
/// returning the position as `redeemable: true` for many minutes due to
/// indexer lag.  We track confirmed-redeemed conditions here and skip them
/// entirely in future auto_settle cycles — no cooldown expiry needed because
/// once a market is fully settled it never un-settles.  The set lives in a
/// LazyLock so it persists across hourly market rotations for the entire
/// process lifetime.
static PERMANENTLY_SETTLED_CONDITIONS: LazyLock<Mutex<HashSet<B256>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// Tracks conditions that produced a non-retryable error (GS013, inner revert, etc.).
/// These are skipped for SETTLEMENT_CONDITION_ERROR_COOLDOWN_SECS before being retried.
static FAILED_SETTLEMENT_CONDITIONS: LazyLock<Mutex<HashMap<B256, DateTime<Utc>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

sol! {
    /// Gnosis Safe 1.3 — we only need execTransaction.
    #[sol(rpc)]
    interface IGnosisSafe {
        function execTransaction(
            address to,
            uint256 value,
            bytes calldata data,
            uint8   operation,
            uint256 safeTxGas,
            uint256 baseGas,
            uint256 gasPrice,
            address gasToken,
            address refundReceiver,
            bytes   memory signatures
        ) external payable returns (bool success);
    }

    /// Calldata encoder for the CTF contract.
    interface ICtfDirect {
        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata indexSets
        ) external;

        function mergePositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata partition,
            uint256 amount
        ) external;
    }

    /// Calldata encoder for the NegRisk adapter contract.
    interface INegRiskDirect {
        function redeemPositions(
            bytes32 conditionId,
            uint256[] calldata amounts
        ) external;
    }
}

/// Execute a contract call through the Gnosis Safe.
///
/// The Safe holds all ERC1155 outcome tokens. CTF methods operate on `msg.sender`'s
/// balance, so we must call CTF *from within* the Safe using `execTransaction`.
///
/// ## Signature scheme
/// For a 1-of-1 Safe where the EOA is the direct owner AND msg.sender == owner,
/// Gnosis Safe accepts a pre-approved-owner signature without any hash computation:
///   - r = EOA address (right-aligned in 32 bytes)
///   - s = 0x00…00 (32 zero bytes)
///   - v = 0x01
/// Safe checks `msg.sender == address(r)` → passes for threshold = 1.
async fn execute_via_safe<P: Provider + Clone>(
    provider: P,
    safe_address: Address,
    eoa_address: Address,
    ctf_contract: Address,
    calldata: Vec<u8>,
) -> anyhow::Result<alloy::primitives::TxHash> {
    // Build the pre-approved owner signature (65 bytes).
    let mut sig = Vec::with_capacity(65);
    sig.extend_from_slice(&[0u8; 12]);           // left-pad address to 32 bytes
    sig.extend_from_slice(eoa_address.as_slice()); // 20-byte EOA address
    sig.extend_from_slice(&[0u8; 32]);           // s = 0
    sig.push(0x01u8);                            // v = 1 (owner pre-approve)

    let safe = IGnosisSafe::new(safe_address, provider);
    let pending = safe
        .execTransaction(
            ctf_contract,
            U256::ZERO,            // ETH value
            Bytes::from(calldata), // inner calldata
            0u8,                   // operation: CALL
            U256::ZERO,            // safeTxGas
            U256::ZERO,            // baseGas
            U256::ZERO,            // gasPrice
            Address::ZERO,         // gasToken
            Address::ZERO,         // refundReceiver
            Bytes::from(sig),      // packed signature
        )
        .send()
        .await?;

    let tx_hash = *pending.tx_hash();

    // Hard 30s cap on receipt wait — Polygon block time is ~2s; 30s is generous.
    // Without this the call can hang indefinitely if the RPC stalls post-submission,
    // which blocks the entire tokio select! event loop.
    tokio::time::timeout(std::time::Duration::from_secs(30), pending.get_receipt())
        .await
        .map_err(|_| anyhow::anyhow!("execute_via_safe: get_receipt timed out (30s)"))?
        .map_err(|e| anyhow::anyhow!("execute_via_safe: get_receipt error: {}", e))?;

    Ok(tx_hash)
}

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
    orphan_tombstones: &OrphanTombstones,
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

        // Tombstone this token so reconcile_orphaned_positions (balance.rs) never
        // re-adopts it within the same session.  Without this, phantom_cooldowns are
        // cleared on every hourly market switch and the reconcile loop immediately
        // re-adopts the unhedged on-chain leg, restarting the detect→remove→re-adopt cycle.
        orphan_tombstones.lock().await.insert(token_id);

        // Block re-entry into this token for PHANTOM_COOLDOWN_SECS so the strategy
        // cannot immediately open a new position on top of untracked on-chain shares.
        // NOTE: the cooldown is also set on the PAIRED token (if we know it) so that
        // an arb entry can't fire a new YES+NO pair on top of the just-untracked orphan.
        // The fill_confirmed_at check ensures we only set the extra cooldown for legs
        // that genuinely filled on-chain (not phantom half-fills that never confirmed).
        phantom_cooldowns.lock().await.insert(
            format!("{}:{}", strategy_name, token_id),
            tokio::time::Instant::now(),
        );
        if let Some(paired_token) = position.paired_leg_token_id {
            // Also set cooldown on the pair so new arb entries are blocked on both legs
            // until the orphan state fully clears (paired_leg cleanup + cooldown expiry).
            phantom_cooldowns.lock().await.insert(
                format!("{}:{}", strategy_name, paired_token),
                tokio::time::Instant::now(),
            );
        }

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

/// Scan for resolved/closed markets and auto-settle positions so wallet + UI stay in sync.
///
/// Only processes conditions where at least one leg is `redeemable: true` (market resolved).
///
/// **Merge is intentionally skipped.**  After resolution, `redeemPositions(indexSets=[1,2])`
/// redeems both outcome tokens in one call: the winning outcome pays out, the losing outcome
/// burns for $0.  There is no need to call `mergePositions` first.  Previous attempts to merge
/// active positions caused:
///   - GS013 reverts on NegRisk markets (wrong parentCollectionId = B256::ZERO)
///   - 45-second settlement cycles blocking the event loop for the full 60s timeout
///   - Watchdog triggering a market-loop restart after 3 min of strategy-ticker starvation
///
/// **IMPORTANT**: Requires POLYGON_RPC_URL set to a paid RPC (Alchemy / QuickNode / Infura).
pub async fn auto_settle_closed_positions<P: Provider + Clone>(
    wallet_provider: P,
    safe_address: Address,
    eoa_address: Address,
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
            warn!("Auto-settle: Data API positions() error: {}", e);
            return false;
        }
        Err(_) => {
            warn!("Auto-settle: Data API positions() timed out (20s)");
            return false;
        }
    };

    let mut by_condition: HashMap<B256, Vec<_>> = HashMap::new();
    for p in positions.into_iter().filter(|p| p.size > Decimal::ZERO) {
        by_condition.entry(p.condition_id).or_default().push(p);
    }

    let mut settled_any = false;

    for (condition_id, legs) in by_condition {
        // ── Permanently-settled guard: skip if we already redeemed this condition ──
        // After a successful redeemPositions TX the Data API can return the position
        // as redeemable: true for many minutes (indexer lag).  Without this guard
        // every settlement_ticker cycle would submit a new blockchain TX, burning
        // gas and advancing the wallet nonce unnecessarily (observed 2026-05-21:
        // 8 duplicate redeems of condition 0xd633214b within ~1 hour).
        {
            if PERMANENTLY_SETTLED_CONDITIONS.lock().await.contains(&condition_id) {
                continue;
            }
        }
        // ── Success cooldown: skip if we submitted successfully recently ────────
        {
            let recent = RECENT_SETTLEMENT_SUBMITS.lock().await;
            if let Some(last_submit_at) = recent.get(&condition_id) {
                if (Utc::now() - *last_submit_at).num_seconds() < SETTLEMENT_CONDITION_COOLDOWN_SECS {
                    continue;
                }
            }
        }
        // ── Error cooldown: skip conditions that produced non-retryable errors ──
        {
            let failed = FAILED_SETTLEMENT_CONDITIONS.lock().await;
            if let Some(failed_at) = failed.get(&condition_id) {
                if (Utc::now() - *failed_at).num_seconds() < SETTLEMENT_CONDITION_ERROR_COOLDOWN_SECS {
                    continue;
                }
            }
        }

        // Only settle conditions where at least one leg is flagged as redeemable.
        // Merge-only conditions (active markets with both YES and NO) are intentionally
        // skipped — trying to merge them causes GS013 reverts on NegRisk markets and
        // wastes gas/event-loop time on non-resolved positions.
        let has_redeemable = legs.iter().any(|p| p.redeemable);
        if !has_redeemable {
            continue;
        }

        let is_neg_risk = legs.iter().any(|p| p.negative_risk);

        let mut outcome_units: BTreeMap<i32, u128> = BTreeMap::new();
        for p in &legs {
            let units = shares_to_base_units(p.size);
            if units == 0 { continue; }
            *outcome_units.entry(p.outcome_index).or_insert(0) += units;
        }

        let yes_units = *outcome_units.get(&0).unwrap_or(&0);
        let no_units  = *outcome_units.get(&1).unwrap_or(&0);

        if yes_units == 0 && no_units == 0 {
            info!("Auto-settle: condition {} marked redeemable but zero units — skipping", condition_id);
            continue;
        }

        info!(
            "Auto-settle: attempting redeem for condition {} | YES: {} | NO: {} units | neg_risk={}",
            condition_id, yes_units, no_units, is_neg_risk
        );

        if is_neg_risk {
            // Neg-risk redemption via the NegRisk adapter.
            let calldata = INegRiskDirect::redeemPositionsCall {
                conditionId: condition_id,
                amounts: vec![U256::from(yes_units), U256::from(no_units)],
            }.abi_encode();

            match execute_via_safe(
                wallet_provider.clone(),
                safe_address,
                eoa_address,
                NEG_RISK_ADAPTER_ADDRESS,
                calldata,
            ).await {
                Ok(tx_hash) => {
                    settled_any = true;
                    RECENT_SETTLEMENT_SUBMITS.lock().await.insert(condition_id, Utc::now());
                    // Mark as permanently settled so we never re-attempt this condition
                    // in the same session, regardless of Data API indexer lag.
                    PERMANENTLY_SETTLED_CONDITIONS.lock().await.insert(condition_id);
                    info!("✅ Auto-settle: redeemed neg-risk condition {} (tx {})", condition_id, tx_hash);
                }
                Err(e) => {
                    warn!("Auto-settle: neg-risk redeem failed for condition {}: {}", condition_id, e);
                    FAILED_SETTLEMENT_CONDITIONS.lock().await.insert(condition_id, Utc::now());
                }
            }
        } else {
            // Standard binary market redemption via CTF.
            // indexSets=[1,2] redeems BOTH outcomes in one call:
            //   - winning outcome pays $1.00 per token
            //   - losing outcome pays $0.00 and is burned
            // collateralToken MUST be pUSD — positions were minted with pUSD.
            // We route through Safe.execTransaction so msg.sender == Safe (the token holder).
            let calldata = ICtfDirect::redeemPositionsCall {
                collateralToken: PUSD_COLLATERAL,
                parentCollectionId: B256::ZERO,
                conditionId: condition_id,
                indexSets: vec![U256::from(1u64), U256::from(2u64)],
            }.abi_encode();

            match execute_via_safe(
                wallet_provider.clone(),
                safe_address,
                eoa_address,
                CTF_ADDRESS,
                calldata,
            ).await {
                Ok(tx_hash) => {
                    settled_any = true;
                    RECENT_SETTLEMENT_SUBMITS.lock().await.insert(condition_id, Utc::now());
                    // Mark as permanently settled so we never re-attempt this condition
                    // in the same session, regardless of Data API indexer lag.
                    PERMANENTLY_SETTLED_CONDITIONS.lock().await.insert(condition_id);
                    info!(
                        "✅ Auto-settle: redeemed condition {} (tx {}) — YES:{} NO:{} units",
                        condition_id, tx_hash, yes_units, no_units
                    );
                }
                Err(e) => {
                    warn!("Auto-settle: redeem failed for condition {}: {}", condition_id, e);
                    FAILED_SETTLEMENT_CONDITIONS.lock().await.insert(condition_id, Utc::now());
                }
            }
        }
    }

    settled_any
}

fn shares_to_base_units(shares: Decimal) -> u128 {
    // CTF contract methods consume 6-decimal fixed-point token amounts.
    (shares.max(Decimal::ZERO).trunc_with_scale(6) * Decimal::from(1_000_000u32))
        .trunc()
        .to_u128()
        .unwrap_or(0)
}


