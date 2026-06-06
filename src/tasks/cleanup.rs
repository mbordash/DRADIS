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

use crate::helpers::{db, metrics, send_notification, PhantomCooldowns};
use crate::helpers::balance::OrphanTombstones;
use crate::state::{Position, PositionMap};
use crate::vipers::time_decay_impl::TimeDecayPosition;
use sqlx;
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
                warn!(" Cleaned up {} position(s) for market \"{}\" (expires {})",
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
/// A confirmed-fill orphan that the cleanup cycle removed from tracking.
///
/// Returned to the caller (main.rs cleanup_ticker arm) so it can attempt:
///   (1) Re-hedge — FAK buy the MISSING leg at its current ask.  Completes the
///       original arb and locks in profit without selling the position at all.
///   (2) Bid-based FAK sell — if re-hedge is uneconomical (market moved far).
///
/// Both paths use real market prices from the live price feeds available in
/// the cleanup_ticker arm, not a fixed $0.01 panic-sell floor.
pub struct OrphanExit {
    /// The on-chain token ID of the orphaned (filled) leg.
    pub token_id: U256,
    /// Confirmed on-chain share count.
    pub shares: Decimal,
    /// True if this token belongs to a NegRisk market.
    /// NB: ArbitrageStrategy only runs on standard binary markets so this is
    /// currently always false; kept for future neg-risk arb support.
    pub is_neg_risk: bool,
    /// Token ID of the LEG THAT NEVER FILLED — the counterpart we must buy
    /// to complete the hedge.  None if the position had no recorded pair
    /// (e.g. reconcile-adopted position with paired_leg_token_id = None).
    pub paired_token_id: Option<U256>,
    /// Price we paid to enter the orphaned leg (avg_entry from Position).
    /// Used to evaluate re-hedge profitability:
    ///   re_hedge_cost = paired_ask + original_entry
    /// If re_hedge_cost < RE_HEDGE_THRESHOLD the arb is still profitable.
    pub original_entry: Decimal,
}

pub async fn reconcile_orphaned_positions(
    positions: Arc<Mutex<PositionMap>>,
    clob_client: &Arc<ClobClient<Authenticated<Normal>>>,
    phantom_cooldowns: &PhantomCooldowns,
    orphan_tombstones: &OrphanTombstones,
    tg_token: &str,
    tg_chat_id: &str,
) -> anyhow::Result<Vec<OrphanExit>> {
    let mut pos_map = positions.lock().await;
    let now = Utc::now();

    let mut orphans_to_exit: Vec<((String, U256), Position)> = Vec::new();
    let mut exits_to_sell: Vec<OrphanExit> = Vec::new();

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
        warn!(" ORPHANED PAIR DETECTED [{}]: {} shares at ${:.4} ({}s old) — cancelling GTC + removing",
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
                    warn!(" Cancelling {} resting order(s) for orphaned token {}", id_refs.len(), token_id);
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
            &format!(" Orphaned pair exited [{}]: {} {} shares @ ${:.4}",
                     strategy_name,
                     if token_id == position.pair_token_id { "YES" } else { "NO" },
                     position.shares.trunc(),
                     position.avg_entry)).await;

        // If the orphan had a confirmed on-chain fill (not a phantom/never-filled leg),
        // If the orphan had a confirmed on-chain fill (not a phantom/never-filled leg),
        // schedule a re-hedge or bid-based exit so the unhedged exposure is closed rather
        // than silently riding to settlement.  Only confirmed fills have real shares on-chain.
        if position.fill_confirmed_at.is_some() && position.shares > Decimal::ZERO {
            exits_to_sell.push(OrphanExit {
                token_id,
                shares: position.shares,
                is_neg_risk: false, // ArbitrageStrategy only runs on standard binary markets
                paired_token_id: position.paired_leg_token_id,
                original_entry: position.avg_entry,
            });
        }
    }

    Ok(exits_to_sell)
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
    // Collect all per-asset pools.  The Data API returns wallet-wide positions
    // regardless of which asset opened them, so we must sync EVERY asset DB —
    // not just the primary — to keep secondary asset open_positions tables in sync.
    let all_assets = db::available_assets();
    if all_assets.is_empty() {
        warn!("⚠️ Chain-sync: no DB pools available, skipping");
        return;
    }

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

    // Build map: decimal_token_id → &Position  (size > 0, NOT redeemable only).
    // MUST use p.asset.to_string() (decimal) — the DB stores token_id as decimal U256.
    //
    // Redeemable positions are EXCLUDED intentionally:
    //   - A redeemable position means the market has resolved and the token is worth $0
    //     (losing side) or $1 (winning side, already pending redemption by auto_settle).
    //   - Including them in live_map would cause them to be re-adopted as active open
    //     positions on every restart, showing phantom losing positions in the UI.
    //   - By excluding them, purge_stale_open_positions removes any lingering DB rows,
    //     and auto_settle_closed_positions handles the on-chain redemption separately.
    //     This also covers the Data API indexer lag window after a manual "clear" on the
    //     Polymarket UI: the token may still show size > 0 at the API while the indexer
    //     catches up, but redeemable: true is set immediately at settlement.
    let live_map: std::collections::HashMap<String, &_> = live_positions
        .iter()
        .filter(|p| p.size > rust_decimal::Decimal::ZERO && !p.redeemable)
        .map(|p| (p.asset.to_string(), p))
        .collect();

    // Count redeemable positions so operators can see them in logs.
    let redeemable_count = live_positions.iter().filter(|p| p.redeemable).count();

    let live_ids: HashSet<String> = live_map.keys().cloned().collect();

    // ── Sync EVERY per-asset pool ─────────────────────────────────────────────
    // Each asset stores its own open_positions rows in its own SQLite file.
    // Fetch live_ids once (wallet-wide) and apply purge + adopt to every pool.
    let mut total_purged = 0usize;
    let mut total_adopted = 0usize;
    for asset in &all_assets {
        let pool = match db::pool_for(asset) {
            Some(p) => p,
            None    => continue,
        };

        // Purge stale rows in this asset's DB.
        total_purged += db::purge_stale_open_positions(&pool, &live_ids).await;

        // Re-adopt on-chain positions missing from this asset's DB.
        // Query AFTER the purge so we don't re-adopt something just removed.
        let db_ids: HashSet<String> = sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT token_id FROM open_positions"
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default()
        .into_iter()
        .collect();

        for (token_str, pos) in &live_map {
            if !db_ids.contains(token_str) {
                let side = pos.outcome.to_uppercase();
                if db::adopt_chain_position(&pool, token_str, &pos.title, &side, pos.avg_price, pos.size).await {
                    total_adopted += 1;
                    info!(" Chain-sync [{}]: re-adopted on-chain position — token {} | {} shares @ ${:.4} | \"{}\"",
                        asset.to_uppercase(),
                        &token_str[..token_str.len().min(20)],
                        pos.size, pos.avg_price, pos.title);
                }
            }
        }
    }

    if total_purged > 0 {
        info!(" Chain-sync: purged {} stale open_positions row(s) across {} asset DB(s)", total_purged, all_assets.len());
    }
    if redeemable_count > 0 {
        info!("⏳ Chain-sync: skipped {} redeemable (settled) position(s) — auto_settle will handle redemption", redeemable_count);
    }
    if total_purged == 0 && total_adopted == 0 {
        info!("✅ Chain-sync: open_positions DB(s) in sync with on-chain holdings ({} live, {} redeemable, {} asset DB(s))",
            live_map.len(), redeemable_count, all_assets.len());
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
                    PERMANENTLY_SETTLED_CONDITIONS.lock().await.insert(condition_id);
                    info!("✅ Auto-settle: redeemed neg-risk condition {} (tx {})", condition_id, tx_hash);
                    // Immediately purge open_positions DB rows for all legs in this condition
                    // so they don't linger during Data API indexer lag after redemption.
                    purge_settled_legs(&legs).await;
                    // Record the settled arb trade so it appears in the Recent Trades card.
                    record_settled_arb_trade(&legs).await;
                }
                Err(e) => {
                    warn!("Auto-settle: neg-risk redeem failed for condition {}: {}", condition_id, e);
                    FAILED_SETTLEMENT_CONDITIONS.lock().await.insert(condition_id, Utc::now());
                }
            }
        } else {
            // Standard binary market redemption via CTF.
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
                    PERMANENTLY_SETTLED_CONDITIONS.lock().await.insert(condition_id);
                    info!(
                        "✅ Auto-settle: redeemed condition {} (tx {}) — YES:{} NO:{} units",
                        condition_id, tx_hash, yes_units, no_units
                    );
                    // Immediately purge open_positions DB rows for all legs in this condition
                    // so they don't linger during Data API indexer lag after redemption.
                    purge_settled_legs(&legs).await;
                    // Record the settled arb trade so it appears in the Recent Trades card.
                    record_settled_arb_trade(&legs).await;
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

/// Record a combined settlement trade for an ArbitrageStrategy pair so it appears
/// in the Recent Trades card.
///
/// Arb always holds equal YES + NO shares and collects $1.00 per pair at settlement
/// regardless of which side wins.  We therefore record ONE combined row per condition:
///
///   entry_price = YES_avg_price + NO_avg_price  (total cost per YES+NO pair, in $)
///   exit_price  = $1.00                          (guaranteed CTF payout per pair)
///   shares      = min(YES_size, NO_size)          (number of complete pairs)
///   pnl         = (1.00 − entry_price) × shares
///   side        = "YES"                           (renders green in the UI)
///   reason      = "Settlement (YES+NO → $1.00)"
///
/// If the condition has only one leg (e.g. an orphaned single-leg position that was
/// adopted back in), we record it as a single-leg settlement with appropriate PNL.
async fn record_settled_arb_trade(
    legs: &[polymarket_client_sdk_v2::data::types::response::Position],
) {
    if legs.is_empty() {
        return;
    }

    // Separate YES (outcome_index = 0) and NO (outcome_index = 1) legs.
    let yes_leg = legs.iter().find(|p| p.outcome_index == 0);
    let no_leg  = legs.iter().find(|p| p.outcome_index == 1);

    // Use the primary asset pool for the trade record.
    let asset_str = db::available_assets()
        .into_iter()
        .next()
        .unwrap_or_else(|| "btc".to_string());

    match (yes_leg, no_leg) {
        (Some(yes_leg), Some(no_leg)) => {
            // Complete YES+NO pair settlement
            let yes_avg   = yes_leg.avg_price;
            let no_avg    = no_leg.avg_price;
            let yes_size  = yes_leg.size;
            let no_size   = no_leg.size;
            let pairs     = yes_size.min(no_size);

            if pairs <= Decimal::ZERO || (yes_avg <= Decimal::ZERO && no_avg <= Decimal::ZERO) {
                return;
            }

            let entry_per_pair = yes_avg + no_avg;
            let pnl = (Decimal::ONE - entry_per_pair) * pairs;
            let market_title = yes_leg.title.clone();

            info!(
                " ArbitrageStrategy settlement recorded: \"{}\" | {} pairs @ entry ${:.4}/pair → pnl ${:.4}",
                market_title, pairs, entry_per_pair, pnl
            );

            metrics::record_trade(
                &asset_str,
                "ArbitrageStrategy".to_string(),
                market_title,
                "YES".to_string(),
                entry_per_pair,
                Decimal::ONE,
                pairs,
                pnl,
                "Settlement (YES+NO → $1.00)".to_string(),
            ).await;
        }
        (Some(leg), None) | (None, Some(leg)) => {
            // Single-leg settlement (orphaned position)
            let side = if leg.outcome_index == 0 { "YES" } else { "NO" };
            let avg_price = leg.avg_price;
            let size = leg.size;

            if size <= Decimal::ZERO || avg_price <= Decimal::ZERO {
                return;
            }

            // Single-leg settled: if we held the winning side, we get full payout
            // For now, assume it settled (otherwise we wouldn't be here), so payout = $1/share
            let pnl = (Decimal::ONE - avg_price) * size;
            let market_title = leg.title.clone();

            info!(
                " ArbitrageStrategy single-leg settlement: \"{}\" | {} {} @ ${:.4} → pnl ${:.4}",
                market_title, size, side, avg_price, pnl
            );

            metrics::record_trade(
                &asset_str,
                "ArbitrageStrategy".to_string(),
                market_title,
                side.to_string(),
                avg_price,
                Decimal::ONE,
                size,
                pnl,
                format!("Settlement (single-leg {})", side),
            ).await;
        }
        (None, None) => {
            // No legs to record
            return;
        }
    }
}

/// Purge `open_positions` DB rows for all token_ids in a just-redeemed condition.
///
/// Called immediately after a successful `redeemPositions` on-chain transaction.
/// Without this, the rows persist until the Polymarket Data API's indexer catches up
/// (which can take minutes), and each chain-sync run during that window re-adopts
/// them as active open positions — showing stale settled positions in the UI.
///
/// Iterates ALL per-asset pools so secondary assets (ETH, SOL, …) are also purged —
/// a settled position originally entered via a non-primary asset loop would otherwise
/// survive in its own DB file until the next chain-sync window.
async fn purge_settled_legs(legs: &[polymarket_client_sdk_v2::data::types::response::Position]) {
    let all_assets = db::available_assets();
    if all_assets.is_empty() { return; }

    for leg in legs {
        let token_str = leg.asset.to_string();
        for asset in &all_assets {
            let pool = match db::pool_for(asset) {
                Some(p) => p,
                None    => continue,
            };
            match sqlx::query("DELETE FROM open_positions WHERE token_id = ?")
                .bind(&token_str)
                .execute(&pool)
                .await
            {
                Ok(r) if r.rows_affected() > 0 => {
                    info!("️  Auto-settle [{}]: purged open_positions row for redeemed token {} ({})",
                          asset.to_uppercase(),
                          &token_str[..token_str.len().min(20)], leg.title);
                }
                _ => {}
            }
        }
    }
}

fn shares_to_base_units(shares: Decimal) -> u128 {
    // CTF contract methods consume 6-decimal fixed-point token amounts.
    (shares.max(Decimal::ZERO).trunc_with_scale(6) * Decimal::from(1_000_000u32))
        .trunc()
        .to_u128()
        .unwrap_or(0)
}

/// Detect and record arbitrage settlements that occurred outside our settlement ticker.
///
/// **Why needed**: Polymarket may auto-settle positions before our ticker detects them.
/// This function scans for "orphaned" arbitrage entries (positions entered but never
/// exited in the trades table) and records them as settled trades if they:
/// 1. Are no longer present in the Polymarket positions API (auto-settled)
/// 2. Appear in both YES and NO legs (complete arbitrage pair)
///
/// Runs after `auto_settle_closed_positions` to catch any positions that were settled
/// by Polymarket's auto-redemption system rather than our explicit on-chain TX.
pub async fn detect_orphaned_arb_settlements(safe_address: Address) {
    use std::collections::HashMap;

    // Get all available asset pools
    let all_assets = db::available_assets();
    if all_assets.is_empty() {
        return;
    }

    // Query Polymarket for current positions to compare against entries
    let data_client = DataClient::default();
    let req = PositionsRequest::builder().user(safe_address).build();

    let current_positions = match tokio::time::timeout(
        std::time::Duration::from_secs(20),
        data_client.positions(&req),
    ).await {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            warn!("Orphan detection: Data API positions() error: {}", e);
            return;
        }
        Err(_) => {
            warn!("Orphan detection: Data API positions() timed out (20s)");
            return;
        }
    };

    // Build a set of token_ids that currently exist on-chain
    let mut active_tokens: HashSet<String> = HashSet::new();
    for p in &current_positions {
        if p.size > Decimal::ZERO {
            active_tokens.insert(p.asset.to_string());
        }
    }

    // For each asset pool, scan for orphaned arbitrage entries
    for asset in &all_assets {
        let pool = match db::pool_for(asset) {
            Some(p) => p,
            None => continue,
        };

        // Query all arbitrage entries from the last 14 days (markets typically resolve daily)
        let cutoff = Utc::now() - chrono::Duration::days(14);
        let cutoff_str = cutoff.to_rfc3339();

        #[derive(sqlx::FromRow)]
        struct EntryRow {
            token_id: String,
            market: String,
            side: String,
            entry_price: String,
            shares: String,
            ts: String,
        }

        let entries_result = sqlx::query_as::<_, EntryRow>(
            r#"
            SELECT token_id, market, side, entry_price, shares, ts
            FROM entries
            WHERE strategy = 'ArbitrageStrategy'
              AND ts > ?
            ORDER BY market, ts
            "#
        )
        .bind(&cutoff_str)
        .fetch_all(&pool)
        .await;

        let entries = match entries_result {
            Ok(e) => e,
            Err(e) => {
                warn!("Orphan detection [{}]: failed to query entries: {}", asset, e);
                continue;
            }
        };

        // Group entries by market to find complete YES+NO pairs
        let mut by_market: HashMap<String, Vec<_>> = HashMap::new();
        for entry in entries {
            by_market.entry(entry.market.clone()).or_default().push(entry);
        }

        // Check each market for orphaned pairs
        for (market_name, market_entries) in by_market {
            // Find YES and NO legs
            let yes_legs: Vec<_> = market_entries.iter()
                .filter(|e| e.side == "YES")
                .collect();
            let no_legs: Vec<_> = market_entries.iter()
                .filter(|e| e.side == "NO")
                .collect();

            if yes_legs.is_empty() || no_legs.is_empty() {
                // Single-leg entries (not a complete arbitrage pair) - skip for now
                continue;
            }

            // Match YES and NO legs for the same entry time (within 1 second)
            for yes_leg in &yes_legs {
                for no_leg in &no_legs {
                    // Parse timestamps
                    let yes_ts = match DateTime::parse_from_rfc3339(&yes_leg.ts) {
                        Ok(t) => t.with_timezone(&Utc),
                        Err(_) => continue,
                    };
                    let no_ts = match DateTime::parse_from_rfc3339(&no_leg.ts) {
                        Ok(t) => t.with_timezone(&Utc),
                        Err(_) => continue,
                    };

                    // Check if they're within 1 second of each other (same entry)
                    let time_diff = (yes_ts - no_ts).num_milliseconds().abs();
                    if time_diff > 1000 {
                        continue;
                    }

                    // Check if both tokens are no longer active (settled)
                    let yes_token_gone = !active_tokens.contains(&yes_leg.token_id);
                    let no_token_gone = !active_tokens.contains(&no_leg.token_id);

                    if !yes_token_gone || !no_token_gone {
                        // Position still exists, not settled yet
                        continue;
                    }

                    // Check if we already recorded this trade
                    #[derive(sqlx::FromRow)]
                    struct CountRow {
                        count: i64,
                    }

                    let trade_exists = sqlx::query_as::<_, CountRow>(
                        r#"
                        SELECT COUNT(*) as count
                        FROM trades
                        WHERE strategy = 'ArbitrageStrategy'
                          AND market = ?
                          AND ts >= ?
                          AND ts <= ?
                        "#
                    )
                    .bind(&market_name)
                    .bind(&yes_leg.ts)
                    .bind(&no_leg.ts)
                    .fetch_one(&pool)
                    .await
                    .ok()
                    .map(|r| r.count)
                    .unwrap_or(0) > 0;

                    if trade_exists {
                        // Already recorded, skip
                        continue;
                    }

                    // Parse entry prices and shares
                    let yes_price = match yes_leg.entry_price.parse::<Decimal>() {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let no_price = match no_leg.entry_price.parse::<Decimal>() {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let yes_shares = match yes_leg.shares.parse::<Decimal>() {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    let no_shares = match no_leg.shares.parse::<Decimal>() {
                        Ok(s) => s,
                        Err(_) => continue,
                    };

                    // Calculate settlement P&L
                    let pairs = yes_shares.min(no_shares);
                    if pairs <= Decimal::ZERO {
                        continue;
                    }

                    let entry_per_pair = yes_price + no_price;
                    let pnl = (Decimal::ONE - entry_per_pair) * pairs;

                    info!(
                        "🔍 Orphan detection [{}]: Found auto-settled arbitrage: \"{}\" | {} pairs @ entry ${:.4}/pair → pnl ${:.4}",
                        asset.to_uppercase(), market_name, pairs, entry_per_pair, pnl
                    );

                    // Record the settlement trade using the original entry timestamp
                    metrics::record_trade_with_timestamp(
                        asset,
                        "ArbitrageStrategy".to_string(),
                        market_name.clone(),
                        "YES".to_string(),
                        entry_per_pair,
                        Decimal::ONE,
                        pairs,
                        pnl,
                        "Settlement (auto-redeemed by Polymarket)".to_string(),
                        Some(yes_ts),
                    ).await;

                    // Also purge any lingering open_positions rows
                    let _ = sqlx::query("DELETE FROM open_positions WHERE token_id = ? OR token_id = ?")
                        .bind(&yes_leg.token_id)
                        .bind(&no_leg.token_id)
                        .execute(&pool)
                        .await;
                }
            }
        }
    }
}


