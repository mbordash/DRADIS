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
use tracing::{debug, info, warn};

use polymarket_client_sdk_v2::clob::Client as ClobClient;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
use polymarket_client_sdk_v2::data::Client as DataClient;
use polymarket_client_sdk_v2::data::types::request::PositionsRequest;
use polymarket_client_sdk_v2::data::types::request::ClosedPositionsRequest;
use polymarket_client_sdk_v2::data::types::ClosedPositionSortBy;

use alloy::primitives::address as alloy_address;

use crate::helpers::{db, send_notification, PhantomCooldowns};
use crate::helpers::balance::OrphanTombstones;
use crate::state::{Position, PositionMap};
use crate::venues::core::MarketId;
use crate::venues::intl::u256_from_market_id;
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
///
/// **2026-07-21 caveat**: "ALL" is only true for positions minted after the pUSD
/// migration.  Older positions (observed: June/early-July 2026 markets) were minted
/// with USDC.e collateral — redeeming those with pUSD computes the wrong position ID
/// and silently no-ops (6 stuck redeemables, each "redeemed" successfully at
/// 2026-07-20 23:19 with 0 tokens burned, then blacklisted forever by
/// PERMANENTLY_SETTLED_CONDITIONS).  Use `detect_condition_collateral` to pick the
/// right collateral per condition instead of assuming pUSD.
const PUSD_COLLATERAL: Address = alloy_address!("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB");

/// USDC.e (bridged USDC) on Polygon — collateral for pre-pUSD-migration positions.
const USDCE_COLLATERAL: Address = alloy_address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

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

/// Single-flight guard: only one auto-settle loop may run process-wide at a time.
/// Multiple squadron tasks can trigger settlement on the same cadence; this prevents
/// duplicate submissions for the same condition/nonces.
static AUTO_SETTLE_RUN_LOCK: LazyLock<Mutex<()>> =
    LazyLock::new(|| Mutex::new(()));

fn is_retryable_settlement_submit_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("replacement transaction underpriced")
        || msg.contains("in-flight transaction limit reached")
        || msg.contains("nonce too low")
        || msg.contains("already known")
}

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

    /// CTF view functions (called via RPC, not through the Safe).
    #[sol(rpc)]
    interface ICtfView {
        function getCollectionId(
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256 indexSet
        ) external view returns (bytes32);
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

/// Detect which collateral token a resolved condition's outcome tokens were minted with.
///
/// The ERC1155 position ID is `keccak256(collateralToken ++ collectionId)`, so a redeem
/// call with the wrong collateral computes a different position ID, finds balance = 0,
/// and the TX "succeeds" as a silent no-op (observed 2026-07-20: six pre-pUSD-migration
/// positions "redeemed" with pUSD collateral, 0 tokens burned, then blacklisted forever).
///
/// We fetch the collection ID for the leg's index set via the CTF view function (it uses
/// elliptic-curve math, so it can't be computed locally), then keccak-match the position's
/// actual token ID against candidate collaterals.  Falls back to pUSD (the current
/// default) if the RPC call fails or nothing matches.
async fn detect_condition_collateral<P: Provider + Clone>(
    provider: P,
    condition_id: B256,
    leg_token_id: U256,
    leg_outcome_index: i32,
) -> Address {
    use alloy::primitives::keccak256;

    let index_set = U256::from(1u64) << (leg_outcome_index as usize); // outcome 0 → 0b01, 1 → 0b10
    let ctf = ICtfView::new(CTF_ADDRESS, provider);
    let collection_id = match tokio_timeout(
        std::time::Duration::from_secs(10),
        ctf.getCollectionId(B256::ZERO, condition_id, index_set).call(),
    ).await {
        Ok(Ok(cid)) => cid,
        Ok(Err(e)) => {
            warn!("Auto-settle: getCollectionId failed for condition {} — assuming pUSD: {}", condition_id, e);
            return PUSD_COLLATERAL;
        }
        Err(_) => {
            warn!("Auto-settle: getCollectionId timed out for condition {} — assuming pUSD", condition_id);
            return PUSD_COLLATERAL;
        }
    };

    for collateral in [PUSD_COLLATERAL, USDCE_COLLATERAL] {
        let mut buf = [0u8; 52];
        buf[..20].copy_from_slice(collateral.as_slice());
        buf[20..].copy_from_slice(collection_id.as_slice());
        if U256::from_be_bytes(keccak256(buf).0) == leg_token_id {
            return collateral;
        }
    }
    warn!(
        "Auto-settle: token {} of condition {} matches neither pUSD nor USDC.e position ID — assuming pUSD",
        leg_token_id, condition_id
    );
    PUSD_COLLATERAL
}

/// Remove all positions for a market that has expired or is expiring within 60s.
pub async fn cleanup_expired_positions(
    positions: Arc<Mutex<PositionMap>>,
    market_name: String,
    yes_token: MarketId,
    no_token: MarketId,
    close_time: Option<DateTime<Utc>>,
) {
    crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::Cleanup);
    let mut pos_map = positions.lock().await;
    let now = Utc::now();

    // Slice 2b: positions are keyed by the neutral MarketId directly.
    let yes_market = yes_token;
    let no_market = no_token;

    if let Some(ct) = close_time {
        let is_expired = ct <= now;
        let is_expiring_soon = (ct - now).num_seconds() < 60;

        if is_expired || is_expiring_soon {
            let before = pos_map.len();
            pos_map.retain(|(_, token), _| token != &yes_market && token != &no_market);
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
    /// The on-chain token ID of the orphaned (filled) leg (neutral MarketId — slice 2a).
    pub token_id: MarketId,
    /// Confirmed on-chain share count.
    pub shares: Decimal,
    /// True if this token belongs to a NegRisk market.
    /// NB: ArbitrageStrategy only runs on standard binary markets so this is
    /// currently always false; kept for future neg-risk arb support.
    pub is_neg_risk: bool,
    /// Token ID of the LEG THAT NEVER FILLED — the counterpart we must buy
    /// to complete the hedge.  None if the position had no recorded pair
    /// (e.g. reconcile-adopted position with paired_leg_token_id = None).
    pub paired_token_id: Option<MarketId>,
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

    let mut exits_to_sell: Vec<OrphanExit> = Vec::new();

    // ── Naked-leg detection by per-market SHARE BALANCE ──────────────────────
    // A true hedge holds EQUAL shares on both legs of the SAME market. We assess
    // hedge integrity by share balance — not merely by presence of the opposite
    // leg — so we catch BOTH:
    //   (A) fully-naked single legs (pair never filled, or chain-adopted without
    //       pair linkage), and
    //   (B) imbalanced pairs where both legs exist but share counts differ — the
    //       excess shares on the heavier leg are unhedged directional risk.
    //
    // Detected naked exposure is routed to the existing re-hedge-first /
    // flatten-fallback economics downstream:
    //   * fully-naked legs  → OrphanExit with the known pair token, so a CHEAP
    //     re-hedge (total cost < $0.99) completes the arb at a profit; only if
    //     the market moved too far do we FAK-sell at bid.
    //   * imbalanced pairs  → flatten ONLY the excess shares (paired_token_id =
    //     None forces the bid-based sell), avoiding a duplicate-row hazard from
    //     re-hedging on top of an already-tracked opposite leg.
    // Fees are ~0 on the V2 CLOB, so the only real cost is the ~1-3¢ bid/ask
    // spread — making a fast cheap re-hedge strongly preferred over flatten.
    let mut legs_by_market: std::collections::HashMap<String, Vec<((String, MarketId), Position)>> =
        std::collections::HashMap::new();
    for ((strategy_name, token_id), position) in pos_map.iter() {
        if strategy_name != "ArbitrageStrategy" && strategy_name != "TimeDecayStrategy" {
            continue;
        }
        if (now - position.opened_at).num_seconds() < 60 { continue; }
        legs_by_market
            .entry(position.market_name.clone())
            .or_default()
            .push(((strategy_name.clone(), token_id.clone()), position.clone()));
    }

    // Fully-naked single legs → full orphan handling (re-hedge if cheap, else flatten).
    let mut orphans_to_exit: Vec<((String, MarketId), Position)> = Vec::new();
    // Imbalanced pairs → flatten ONLY the naked excess of the heavier leg.
    let mut imbalanced_to_trim: Vec<((String, MarketId), Position, Decimal)> = Vec::new();

    for (_market, legs) in legs_by_market {
        match legs.len() {
            1 => {
                let (key, pos) = legs.into_iter().next().unwrap();
                if pos.shares >= crate::config::MIN_ORDER_SHARES {
                    orphans_to_exit.push((key, pos));
                }
            }
            2 => {
                let mut it = legs.into_iter();
                let (k0, p0) = it.next().unwrap();
                let (k1, p1) = it.next().unwrap();
                // Only a genuine YES+NO pair (two DISTINCT tokens) can be a hedge.
                // Two keys on the SAME token (e.g. arb + time-decay) are the same
                // side — skip to avoid mis-flattening a non-hedge.
                if k0.1 == k1.1 { continue; }
                let excess = (p0.shares - p1.shares).abs();
                if excess >= crate::config::MIN_ORDER_SHARES {
                    let (heavy_key, heavy_pos) =
                        if p0.shares > p1.shares { (k0, p0) } else { (k1, p1) };
                    imbalanced_to_trim.push((heavy_key, heavy_pos, excess));
                }
            }
            _ => {
                // Unexpected >2 legs for one market — skip to avoid mis-flattening.
                continue;
            }
        }
    }

    for ((strategy_name, token_id), position) in orphans_to_exit {
        warn!(" ORPHANED PAIR DETECTED [{}]: {} shares at ${:.4} ({}s old) — cancelling GTC + removing",
              strategy_name, position.shares, position.avg_entry,
              (now - position.opened_at).num_seconds());

        // Slice 2a: convert the neutral key to the on-chain id for the SDK call.
        let token_u256 = match u256_from_market_id(&token_id) {
            Ok(t) => t,
            Err(e) => { warn!("⚠️ Orphan cleanup: bad MarketId {}: {} — skipping", token_id, e); continue; }
        };

        // Cancel any resting GTC order so it can't fill after we forget about it.
        // Hard 10s timeouts on both CLOB calls — same fix as the 2026-05-01 overnight freeze
        // (status_ticker arm). Without these, a TCP-level CLOB API stall inside the
        // cleanup_ticker select! arm blocks the ENTIRE event loop indefinitely.
        let req = OrdersRequest::builder().asset_id(token_u256).build();
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

        pos_map.remove(&(strategy_name.clone(), token_id.clone()));

        // Tombstone this token so reconcile_orphaned_positions (balance.rs) never
        // re-adopts it within the same session.  Without this, phantom_cooldowns are
        // cleared on every hourly market switch and the reconcile loop immediately
        // re-adopts the unhedged on-chain leg, restarting the detect→remove→re-adopt cycle.
        orphan_tombstones.lock().await.insert(token_id.clone());

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
        if let Some(paired_token) = position.paired_leg_token_id.as_ref() {
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

    // ── Imbalanced-pair trim: flatten ONLY the naked excess ──────────────────
    // Both legs are present but unequal — keep the hedged remainder, FAK-sell the
    // excess of the heavier leg at bid. We do NOT re-hedge here (paired_token_id =
    // None) because buying more of the opposite leg on top of an already-tracked
    // row risks duplicate bookkeeping; flattening the excess is the conservative
    // path and caps the loss at the spread (~1-3¢/share, fees ~0 on V2).
    for ((strategy_name, token_id), position, excess) in imbalanced_to_trim {
        // Re-processing guard: skip if we acted on this token within the cooldown
        // window (chain-sync needs time to reflect the sell on-chain).
        let cooldown_key = format!("{}:{}", strategy_name, token_id);
        let on_cooldown = phantom_cooldowns.lock().await
            .get(&cooldown_key)
            .map(|t| t.elapsed().as_secs() < crate::helpers::balance::PHANTOM_COOLDOWN_SECS)
            .unwrap_or(false);
        if on_cooldown { continue; }

        warn!("⚖️ IMBALANCED HEDGE [{}]: leg {} holds {:.4} shares but its pair is short — \
               {:.4} shares NAKED; flattening excess at bid",
              strategy_name, token_id, position.shares, excess);

        // Optimistically reduce the tracked shares to the hedged remainder so the
        // next cycle won't re-detect the same excess. Chain-sync self-heals this
        // if the FAK sell fails (it re-reads actual on-chain holdings).
        if let Some(p) = pos_map.get_mut(&(strategy_name.clone(), token_id.clone())) {
            p.shares = (p.shares - excess).max(Decimal::ZERO);
        }
        phantom_cooldowns.lock().await.insert(cooldown_key, tokio::time::Instant::now());

        let _ = send_notification(tg_token, tg_chat_id,
            &format!("⚖️ Imbalanced hedge trimmed [{}]: flattening {:.0} naked shares of token {} @ bid",
                     strategy_name, excess.trunc(), token_id)).await;

        if position.fill_confirmed_at.is_some() && excess > Decimal::ZERO {
            exits_to_sell.push(OrphanExit {
                token_id,
                shares: excess,
                is_neg_risk: false,
                paired_token_id: None, // force flatten-only (no re-hedge atop existing opposite leg)
                original_entry: position.avg_entry,
            });
        }
    }

    Ok(exits_to_sell)
}

/// Prune expired TimeDecay position metadata entries.
pub async fn cleanup_time_decay_positions(
    td_positions: Arc<Mutex<HashMap<MarketId, TimeDecayPosition>>>,
) {
    let mut td_map = td_positions.lock().await;
    td_map.retain(|_, pos| !pos.is_expired());
}

/// Infer the underlying asset from a Polymarket market title.
///
/// Examples:
/// - "Bitcoin Up or Down on June 7?" -> "btc"
/// - "Will ETH exceed ..." -> "eth"
fn infer_asset_from_title(title: &str) -> Option<&'static str> {
    let normalized: String = title
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { ' ' })
        .collect();
    let has_word = |needle: &str| normalized.split_whitespace().any(|w| w == needle);

    if has_word("btc") || has_word("bitcoin") {
        Some("btc")
    } else if has_word("eth") || has_word("ethereum") {
        Some("eth")
    } else if has_word("sol") || has_word("solana") {
        Some("sol")
    } else {
        None
    }
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
    crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::ChainSync);
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
    let filtered_live_positions: Vec<_> = live_positions
        .iter()
        .filter(|p| p.size > rust_decimal::Decimal::ZERO && !p.redeemable)
        .collect();

    // Build per-asset live maps so each SQLite pool only receives positions for
    // its own asset. Without this, wallet-wide chain adoption leaks BTC rows
    // into ETH/SOL DBs (and vice versa), causing cross-asset UI contamination.
    let mut live_by_asset: HashMap<String, HashMap<String, _>> = HashMap::new();
    let mut unmatched_titles = 0usize;
    for pos in &filtered_live_positions {
        if let Some(asset) = infer_asset_from_title(&pos.title) {
            live_by_asset
                .entry(asset.to_string())
                .or_default()
                .insert(pos.asset.to_string(), *pos);
        } else {
            unmatched_titles += 1;
        }
    }

    // Count redeemable positions so operators can see them in logs.
    let redeemable_count = live_positions.iter().filter(|p| p.redeemable).count();

    // Decimal token IDs of every resolved (redeemable) position, for explicit purge below.
    let redeemable_token_ids: HashSet<String> = live_positions
        .iter()
        .filter(|p| p.redeemable)
        .map(|p| p.asset.to_string())
        .collect();

    // Resolved marks for redeemable positions: token_id → (cur_price, on-chain size).
    // cur_price on a redeemable position reflects the resolved outcome (~$1 winner /
    // ~$0 loser). purge_stale_open_positions uses this to book BOTH legs of a settled
    // pair at resolution time ("pending redemption"), so net P&L never shows a phantom
    // loss while the winning leg awaits on-chain redemption.
    let redeemable_marks: HashMap<String, (Decimal, Decimal)> = live_positions
        .iter()
        .filter(|p| p.redeemable)
        .map(|p| (p.asset.to_string(), (p.cur_price, p.size)))
        .collect();

    // ── Sync EVERY per-asset pool ─────────────────────────────────────────────
    // Each asset stores its own open_positions rows in its own SQLite file.
    // Apply purge + adopt with asset-filtered live IDs for each pool.
    let mut total_purged = 0usize;
    let mut total_adopted = 0usize;
    for asset in &all_assets {
        let pool = match db::pool_for(asset) {
            Some(p) => p,
            None    => continue,
        };

        let live_map = live_by_asset.get(asset);
        let live_ids: HashSet<String> = live_map
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();

        // Purge stale rows in this asset's DB (books resolved legs at settlement value).
        total_purged += db::purge_stale_open_positions(&pool, &live_ids, &redeemable_marks).await;

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

        if let Some(asset_live_map) = live_map {
            let mut total_updated_shares = 0usize;
            for (token_str, pos) in asset_live_map {
                if !db_ids.contains(token_str) {
                    // Normalize binary market outcomes to YES/NO for consistency.
                    // Polymarket API sometimes returns "up"/"down" which must be mapped.
                    let raw_outcome = pos.outcome.to_uppercase();
                    let side = match raw_outcome.as_str() {
                        "UP"   => "YES",
                        "DOWN" => "NO",
                        _      => raw_outcome.as_str(),
                    };
                    if db::adopt_chain_position(&pool, token_str, &pos.title, side, pos.avg_price, pos.size, Some(pos.cur_price)).await {
                        total_adopted += 1;
                        info!(" Chain-sync [{}]: re-adopted on-chain position — token {} | {} shares @ ${:.4} cur=${:.4} | \"{}\"",
                            asset.to_uppercase(),
                            &token_str[..token_str.len().min(20)],
                            pos.size, pos.avg_price, pos.cur_price, pos.title);
                    }
                } else {
                    // Row exists — UPDATE shares and avg_price if the on-chain value differs.
                    // This corrects stale adoptions where the Data API returned a partial-fill
                    // size at adoption time (e.g. 8.0399 instead of the correct 10.0 shares).
                    let db_shares: Option<String> = sqlx::query_scalar(
                        "SELECT shares FROM open_positions WHERE token_id = ? LIMIT 1"
                    )
                    .bind(token_str)
                    .fetch_optional(&pool)
                    .await
                    .unwrap_or(None);

                    if let Some(db_shares_str) = db_shares {
                        let db_shares_val = db_shares_str.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                        // Update when on-chain differs by more than dust (0.001 shares tolerance)
                        if (pos.size - db_shares_val).abs() > Decimal::new(1, 3) {
                            db::update_position_from_chain(&pool, token_str, pos.size, pos.avg_price, Some(pos.cur_price)).await;
                            total_updated_shares += 1;
                            info!(" Chain-sync [{}]: corrected shares — token {} | {} → {} shares | \"{}\"",
                                asset.to_uppercase(),
                                &token_str[..token_str.len().min(20)],
                                db_shares_val, pos.size, pos.title);
                        } else {
                            // Shares unchanged — still refresh current_price so mark-to-market stays live.
                            db::update_position_current_price(&pool, token_str, pos.cur_price).await;
                        }
                    }
                }
            }

            // Write an accurate pnl_snapshot using Data API live current_value.
            // This overwrites the status-task snapshot (which uses entry_price×shares fallback)
            // so the Portfolio chart and banner reflect real mark-to-market.
            let live_positions_value: Decimal = asset_live_map
                .values()
                .filter(|p| p.size > Decimal::ZERO && !p.redeemable)
                .map(|p| p.current_value)
                .sum();
            // Get the most recent collateral and session_pnl from DB for the snapshot.
            if let Some(latest_snap) = db::get_pnl_history(&pool, 1).await.into_iter().next() {
                if let Ok(collateral) = latest_snap.collateral.parse::<Decimal>() {
                    if collateral > Decimal::ZERO {
                        let snap_session_pnl = latest_snap.session_pnl.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                        let snap_total = collateral + live_positions_value;
                        db::record_pnl_snapshot(&pool, snap_session_pnl, collateral, snap_total).await;
                        debug!(" Chain-sync [{}]: wrote accurate pnl_snapshot — collateral=${:.4} positions=${:.4} total=${:.4}",
                            asset.to_uppercase(), collateral, live_positions_value, snap_total);
                    }
                }
            }
            if total_updated_shares > 0 {
                info!("✅ Chain-sync [{}]: updated {} position(s) share count(s) from on-chain data",
                    asset.to_uppercase(), total_updated_shares);
            }
        }
    }

    // ── Purge resolved (redeemable) positions regardless of status ──────────────
    // A redeemable position has resolved on-chain; its payout is claimed into cash by
    // auto_settle OR by the operator clicking "Redeem" on the Polymarket UI. Once
    // resolved it must NEVER remain in open_positions, or it double-counts in the
    // portfolio value (the winnings are — or will be — reflected in collateral instead).
    //
    // purge_stale_open_positions intentionally skips status='pending' rows to avoid a
    // purge/re-adopt race on genuinely in-flight orders — but a redeemable position is
    // never an in-flight order, so those rows would otherwise live forever (observed:
    // chain-adopted 'pending' arb legs surviving long after the market settled and was
    // redeemed off-app). Delete them explicitly here, ignoring status, across every pool.
    let mut redeemable_purged = 0usize;
    if !redeemable_token_ids.is_empty() {
        for asset in &all_assets {
            let pool = match db::pool_for(asset) {
                Some(p) => p,
                None    => continue,
            };
            for token_str in &redeemable_token_ids {
                match sqlx::query("DELETE FROM open_positions WHERE token_id = ?")
                    .bind(token_str)
                    .execute(&pool)
                    .await
                {
                    Ok(r) if r.rows_affected() > 0 => {
                        redeemable_purged += r.rows_affected() as usize;
                        info!("🧹 Chain-sync [{}]: purged resolved (redeemable) open_positions row for token {} — payout settles to cash, not a held position",
                            asset.to_uppercase(), &token_str[..token_str.len().min(20)]);
                    }
                    _ => {}
                }
            }
        }
    }
    total_purged += redeemable_purged;

    if total_purged > 0 {
        info!(" Chain-sync: purged {} stale open_positions row(s) across {} asset DB(s)", total_purged, all_assets.len());
    }
    if redeemable_count > 0 {
        info!("⏳ Chain-sync: {} redeemable (settled) position(s) booked at resolution — auto_settle will claim the cash", redeemable_count);
    }
    if unmatched_titles > 0 {
        warn!("⚠️ Chain-sync: {} live position(s) had unknown market-asset title and were not mapped to an asset DB", unmatched_titles);
    }
    if total_purged == 0 && total_adopted == 0 {
        info!("✅ Chain-sync: open_positions DB(s) in sync with on-chain holdings ({} live, {} redeemable, {} asset DB(s))",
            filtered_live_positions.len(), redeemable_count, all_assets.len());
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
    crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::Settlement);
    // Avoid concurrent settlement scans/submits from multiple squadron tasks.
    let _settle_guard = match AUTO_SETTLE_RUN_LOCK.try_lock() {
        Ok(g) => g,
        Err(_) => {
            debug!("Auto-settle: another run is already in progress; skipping this cycle");
            return false;
        }
    };

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
                    if is_retryable_settlement_submit_error(&e) {
                        debug!(
                            "Auto-settle: transient submit error for condition {} (no error cooldown): {}",
                            condition_id,
                            e
                        );
                    } else {
                        FAILED_SETTLEMENT_CONDITIONS.lock().await.insert(condition_id, Utc::now());
                    }
                }
            }
        } else {
            // Standard binary market redemption via CTF.
            // Detect the collateral from a held leg's actual token ID: pre-pUSD-migration
            // positions were minted with USDC.e, and redeeming with the wrong collateral
            // silently no-ops (see PUSD_COLLATERAL doc).
            let collateral = match legs.iter().find(|p| shares_to_base_units(p.size) > 0) {
                Some(leg) => detect_condition_collateral(
                    wallet_provider.clone(),
                    condition_id,
                    leg.asset,
                    leg.outcome_index,
                ).await,
                None => PUSD_COLLATERAL, // unreachable: zero-units guard above
            };
            if collateral != PUSD_COLLATERAL {
                info!("Auto-settle: condition {} uses legacy USDC.e collateral", condition_id);
            }
            let calldata = ICtfDirect::redeemPositionsCall {
                collateralToken: collateral,
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
                    if is_retryable_settlement_submit_error(&e) {
                        debug!(
                            "Auto-settle: transient submit error for condition {} (no error cooldown): {}",
                            condition_id,
                            e
                        );
                    } else {
                        FAILED_SETTLEMENT_CONDITIONS.lock().await.insert(condition_id, Utc::now());
                    }
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

    // Route the settlement record to the asset DB that actually owns this market,
    // inferred from the market title (e.g. "Solana Up or Down…" → sol). Only crypto
    // markets DRADIS actually trades on the intl (Polymarket) side name BTC/ETH/SOL
    // in their title. The same wallet can also hold unrelated redeemable positions —
    // sports/politics markets ("Devils vs. Flames", etc.) bought manually or via
    // other venues. auto_settle still redeems those on-chain (the cash lands in
    // collateral and is reflected by chain-sync pnl_snapshots), but we must NOT
    // fabricate an "ArbitrageStrategy" trade row for a position the arb Viper never
    // opened. Previously such non-crypto settlements fell back to the primary (BTC)
    // pool and were booked as "₿ BTC / Arbitrage" trades — corrupting per-asset P&L
    // (the NBA/NHL "X vs. Y" single-leg NO rows in the BTC tradelog).
    let title_for_asset = yes_leg.or(no_leg).map(|l| l.title.as_str()).unwrap_or("");
    let asset_str = match infer_asset_from_title(title_for_asset) {
        Some(a) => a.to_string(),
        None => {
            info!(
                "Auto-settle: redeemed non-crypto position \"{}\" — cash claimed to \
                 collateral, trade row skipped (not a DRADIS-managed market)",
                title_for_asset
            );
            return;
        }
    };

    // Settlements are recorded idempotently: auto_settle can re-redeem an already-
    // settled condition after a restart (in-memory dedup is empty on a fresh start),
    // so a plain INSERT would double-count the same realized P&L once per session.
    let pool = match db::pool_for(&asset_str) {
        Some(p) => p,
        None    => return,
    };

    match (yes_leg, no_leg) {
        (Some(yes_leg), Some(no_leg)) => {
            // Complete YES+NO pair settlement
            let yes_size  = yes_leg.size;
            let no_size   = no_leg.size;
            let pairs     = yes_size.min(no_size);

            if pairs <= Decimal::ZERO {
                warn!("Auto-settle: pair settlement for \"{}\" skipped — zero pair size", yes_leg.title);
                return;
            }

            // Cost basis per leg: prefer Data API avg_price, fall back to the local
            // `entries` cost basis when the API returns 0 (churn-residual legs).
            let mut yes_avg = yes_leg.avg_price;
            if yes_avg <= Decimal::ZERO {
                yes_avg = db::lookup_entry_price_db(&pool, &yes_leg.asset.to_string()).await.unwrap_or(Decimal::ZERO);
            }
            let mut no_avg = no_leg.avg_price;
            if no_avg <= Decimal::ZERO {
                no_avg = db::lookup_entry_price_db(&pool, &no_leg.asset.to_string()).await.unwrap_or(Decimal::ZERO);
            }
            if yes_avg <= Decimal::ZERO && no_avg <= Decimal::ZERO {
                warn!(
                    "Auto-settle: pair settlement for \"{}\" NOT recorded — cost basis unknown for \
                     both legs (Data API avg=0, no local entries). Redemption cash is in collateral.",
                    yes_leg.title
                );
                return;
            }

            let entry_per_pair = yes_avg + no_avg;
            let pnl = (Decimal::ONE - entry_per_pair) * pairs;
            let market_title = yes_leg.title.clone();

            // Cross-path dedup: chain-sync may have already booked both legs at
            // resolution ("pending redemption" rows). This redemption is then a
            // cash-only event — booking the pair again would double-count the P&L.
            if db::market_has_pending_redemption_settlement(&pool, &market_title).await {
                debug!(
                    "Auto-settle: pair settlement for \"{}\" suppressed — already booked at \
                     resolution (pending-redemption rows exist)",
                    market_title
                );
                return;
            }

            let inserted = db::record_settlement_trade_idempotent(
                &pool,
                "ArbitrageStrategy",
                &market_title,
                "YES",
                entry_per_pair,
                Decimal::ONE,
                pairs,
                pnl,
                "Settlement (YES+NO → $1.00)",
                None,
            ).await;

            if inserted {
                info!(
                    " ArbitrageStrategy settlement recorded: \"{}\" | {} pairs @ entry ${:.4}/pair → pnl ${:.4}",
                    market_title, pairs, entry_per_pair, pnl
                );
            } else {
                debug!(
                    "Auto-settle: duplicate pair settlement for \"{}\" suppressed (already recorded)",
                    market_title
                );
            }
        }
        (Some(leg), None) | (None, Some(leg)) => {
            // Single-leg settlement (orphaned / unhedged position).
            let side = if leg.outcome_index == 0 { "YES" } else { "NO" };
            let size = leg.size;

            if size <= Decimal::ZERO {
                warn!("Auto-settle: single-leg {} settlement for \"{}\" skipped — zero size", side, leg.title);
                return;
            }

            // Cost basis AND originating strategy: prefer the local `entries` log (the
            // strategy that actually opened this leg). If the Data API avg_price is 0
            // (observed for churn-residual legs) fall back to the logged cost basis.
            // Previously this hardcoded strategy = "ArbitrageStrategy", which mislabeled
            // any non-arb single leg (e.g. a Convergence NO leg on an hourly market that
            // rode to settlement) AND, combined with the ledger-reconcile path booking
            // the same position under its true strategy, produced a DOUBLE-COUNT
            // (2026-07-13: one 13.72-share NO leg booked twice, ≈ −$4.94 each).
            let (mut avg_price, logged_strategy) = match db::lookup_entry_db(&pool, &leg.asset.to_string()).await {
                Some((p, s)) => (p, if s.is_empty() { None } else { Some(s) }),
                None => (leg.avg_price, None),
            };
            if avg_price <= Decimal::ZERO {
                avg_price = leg.avg_price;
            }
            if avg_price <= Decimal::ZERO {
                warn!(
                    "Auto-settle: single-leg {} settlement for \"{}\" NOT recorded — cost basis \
                     unknown (Data API avg=0, no local entry for token {}). Redemption cash is \
                     reflected in collateral; trade row omitted to avoid fabricating P&L.",
                    side, leg.title, leg.asset
                );
                return;
            }

            // A single (unhedged) leg pays the market's RESOLVED price: ~$1.00 if
            // this side won, ~$0.00 if it lost. `cur_price` on a redeemable position
            // already reflects the resolved outcome, so use it as the exit price.
            //
            // The previous code hardcoded exit_price = $1.00, so pnl was always
            // (1 − avg_price) × size > 0 — booking every LOSING settlement as a
            // phantom win. That masked real cash losses (the losing leg paid $0)
            // and is the root cause of the unexplained portfolio bleed.
            let exit_price = leg.cur_price;
            let pnl = (exit_price - avg_price) * size;
            let market_title = leg.title.clone();

            // Cross-path dedup: the ledger-reconcile path (purge_stale_open_positions)
            // may have already booked this same position (matched on market + shares)
            // under its true strategy/reason. Its fingerprint differs from ours, so the
            // idempotent-insert guard below would NOT catch it — check explicitly here.
            // Likewise, chain-sync may have booked this leg at resolution ("pending
            // redemption") — the redemption is then a cash-only event.
            if db::market_has_pending_redemption_settlement(&pool, &market_title).await {
                debug!(
                    "Auto-settle: single-leg {} settlement for \"{}\" suppressed — already \
                     booked at resolution (pending-redemption row exists)",
                    side, market_title
                );
                return;
            }
            if db::market_has_matching_trade(&pool, &market_title, size).await {
                debug!(
                    "Auto-settle: single-leg {} settlement for \"{}\" suppressed — already booked \
                     ({} sh) by another path (ledger reconcile or strategy close)",
                    side, market_title, size
                );
                return;
            }

            let settle_strategy = logged_strategy.as_deref().unwrap_or("ArbitrageStrategy");
            let inserted = db::record_settlement_trade_idempotent(
                &pool,
                settle_strategy,
                &market_title,
                side,
                avg_price,
                exit_price,
                size,
                pnl,
                &format!("Settlement (single-leg {})", side),
                None,
            ).await;

            if inserted {
                info!(
                    " {} single-leg settlement: \"{}\" | {} {} @ ${:.4} → resolved ${:.4} → pnl ${:.4}",
                    settle_strategy, market_title, size, side, avg_price, exit_price, pnl
                );
            } else {
                debug!(
                    "Auto-settle: duplicate single-leg {} settlement for \"{}\" suppressed (already recorded)",
                    side, market_title
                );
            }
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
pub async fn detect_orphaned_arb_settlements(safe_address: Address, squadron_asset: &str) {
    use std::collections::HashMap;

    // Query Polymarket for current positions to compare against entries
    let data_client = DataClient::default();
    let req = PositionsRequest::builder().user(safe_address).build();

    let current_positions = match tokio::time::timeout(
        std::time::Duration::from_secs(20),
        data_client.positions(&req),
    ).await {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            warn!("Orphan detection [{}]: Data API positions() error: {}", squadron_asset, e);
            return;
        }
        Err(_) => {
            warn!("Orphan detection [{}]: Data API positions() timed out (20s)", squadron_asset);
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

    // ── Resolution evidence: closed-positions ────────────────────────────────
    // CRITICAL: a token vanishing from `active_tokens` does NOT mean the market
    // settled — it also vanishes when the leg was SOLD/FLATTENED (orphan cleanup,
    // early exit) or never filled at all. Booking a $1.00 "auto-settled" trade on
    // mere absence fabricates phantom profit on a still-open market (root cause of
    // the 2026-06-21 trade booked at 04:18 on a noon-close market).
    //
    // The authoritative signal that a leg was REDEEMED (not sold) is the
    // `/closed-positions` record: a redemption closes at/after the market's
    // resolution (`end_date`), whereas an early sale closes strictly before it.
    // We require both legs to show genuine redemption before booking a settlement.
    let now = Utc::now();
    let closed_positions = match tokio::time::timeout(
        std::time::Duration::from_secs(20),
        data_client.closed_positions(
            &ClosedPositionsRequest::builder()
                .user(safe_address)
                .limit(50)
                .expect("closed-positions limit 50 is within the 0-50 bound")
                .sort_by(ClosedPositionSortBy::Timestamp)
                .build(),
        ),
    ).await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            warn!("Orphan detection [{}]: closed_positions() error: {} — skipping settlement booking this cycle", squadron_asset, e);
            return;
        }
        Err(_) => {
            warn!("Orphan detection [{}]: closed_positions() timed out (20s) — skipping settlement booking this cycle", squadron_asset);
            return;
        }
    };

    // Map token_id (decimal) → (market resolution date, close timestamp unix secs).
    let closed_by_token: HashMap<String, (DateTime<Utc>, i64)> = closed_positions
        .iter()
        .map(|cp| (cp.asset.to_string(), (cp.end_date, cp.timestamp)))
        .collect();

    // A leg was genuinely REDEEMED at settlement (not sold early / never filled)
    // iff it closed at/after the market's resolution time. 60s grace absorbs minor
    // indexer/clock skew between the redeem TX and the recorded end_date.
    let redeemed_at_resolution = |token: &str| -> bool {
        match closed_by_token.get(token) {
            Some((end_date, close_ts)) => {
                let market_resolved = *end_date <= now;
                let closed_after_resolution = *close_ts >= end_date.timestamp() - 60;
                market_resolved && closed_after_resolution
            }
            None => false,
        }
    };

    // Get database pool for this squadron's asset only
    let pool = match db::pool_for(squadron_asset) {
        Some(p) => p,
        None => {
            warn!("Orphan detection [{}]: No database pool available", squadron_asset);
            return;
        }
    };

    // Query all arbitrage entries from the last 14 days (markets typically resolve daily)
    let cutoff = Utc::now() - chrono::Duration::days(14);
    let cutoff_str = cutoff.to_rfc3339();

    #[derive(sqlx::FromRow)]
    struct EntryRow {
        token_id: String,
        market: String,
        side: String,
        ts: String,
        entry_price: String,
        shares: String,
    }

    let entries_result = sqlx::query_as::<_, EntryRow>(
        r#"
        SELECT token_id, market, side, ts, entry_price, shares
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
            warn!("Orphan detection [{}]: failed to query entries: {}", squadron_asset, e);
            return;
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

                // ── Resolution gate ──────────────────────────────────────────
                // Both legs are absent from live positions, but that alone is
                // ambiguous (sold / flattened / never-filled all look identical).
                // Only book a settlement when BOTH legs are confirmed redeemed at
                // the market's resolution. Otherwise the legs were closed for some
                // other reason — booking a $1.00 settlement here would fabricate
                // phantom profit on an open or early-exited market.
                if !redeemed_at_resolution(&yes_leg.token_id)
                    || !redeemed_at_resolution(&no_leg.token_id)
                {
                    debug!(
                        " Orphan detection [{}]: \"{}\" legs absent but not redeemed at resolution \
                         (sold/flattened/unfilled) — skipping phantom settlement booking",
                        squadron_asset.to_uppercase(), market_name
                    );
                    continue;
                }

                // ── Settlement recording (Polymarket auto-redeem backstop) ──
                // Both legs are confirmed REDEEMED at the market's resolution (the
                // `redeemed_at_resolution` gate above). This is the case Polymarket
                // auto-redeems on-chain OUTSIDE our settlement ticker: the tokens
                // leave the wallet, so `auto_settle_closed_positions` sees size=0,
                // filters them out (`size > 0`), and never books the trade. Without
                // this backstop the realized P&L is silently dropped (observed
                // 2026-07-04: a hedged YES@0.82 + NO@0.16 pair redeemed to $1.00 for
                // +$0.30 but session_pnl stayed 0 and the UI showed a phantom loss).
                //
                // Safe against the 2026-06-21 phantom-profit bug because booking is
                // gated on `redeemed_at_resolution` (confirmed on-chain redemption at
                // the market's close), not mere token absence (sold/flattened legs
                // close BEFORE resolution and are excluded).
                //
                // Idempotency: skip if ANY settlement row already exists for this
                // market — covers both a prior orphan cycle AND the auto_settle path
                // (which books "Settlement (YES+NO → $1.00)" when DRADIS itself
                // redeems), so we never double-count the same resolution.
                let already_booked: Option<(i64,)> = sqlx::query_as(
                    "SELECT 1 FROM trades \
                     WHERE strategy = 'ArbitrageStrategy' AND market = ? \
                       AND reason LIKE 'Settlement%' LIMIT 1"
                )
                .bind(&market_name)
                .fetch_optional(&pool)
                .await
                .unwrap_or(None);

                if already_booked.is_none() {
                    let yes_avg = yes_leg.entry_price.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                    let no_avg  = no_leg.entry_price.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                    let yes_sh  = yes_leg.shares.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                    let no_sh   = no_leg.shares.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                    let pairs   = yes_sh.min(no_sh);
                    let entry_per_pair = yes_avg + no_avg;

                    if pairs > Decimal::ZERO && entry_per_pair > Decimal::ZERO {
                        // A confirmed YES+NO pair resolves to exactly $1.00 (one leg
                        // pays $1, the other $0). Realized P&L = ($1.00 − cost)/pair.
                        let pnl = (Decimal::ONE - entry_per_pair) * pairs;
                        let inserted = db::record_settlement_trade_idempotent(
                            &pool,
                            "ArbitrageStrategy",
                            &market_name,
                            "YES",
                            entry_per_pair,
                            Decimal::ONE,
                            pairs,
                            pnl,
                            "Settlement (auto-redeemed by Polymarket)",
                            None,
                        ).await;
                        if inserted {
                            info!(
                                " Orphan detection [{}]: booked auto-redeemed settlement \"{}\" | {} pairs @ ${:.4}/pair → pnl ${:.4}",
                                squadron_asset.to_uppercase(), market_name, pairs, entry_per_pair, pnl
                            );
                        }
                    } else {
                        warn!(
                            " Orphan detection [{}]: \"{}\" redeemed at resolution but cost basis \
                             unknown (pairs={} entry/pair={}) — redemption cash is in collateral, \
                             trade row omitted to avoid fabricating P&L",
                            squadron_asset.to_uppercase(), market_name, pairs, entry_per_pair
                        );
                    }
                }

                // Purge any lingering `open_positions` rows for the confirmed-redeemed
                // pair so the portfolio value doesn't double-count.
                info!(
                    " Orphan detection [{}]: \"{}\" confirmed redeemed at resolution — \
                     purging stale open_positions rows",
                    squadron_asset.to_uppercase(), market_name
                );
                let _ = sqlx::query("DELETE FROM open_positions WHERE token_id = ? OR token_id = ?")
                    .bind(&yes_leg.token_id)
                    .bind(&no_leg.token_id)
                    .execute(&pool)
                    .await;
            }
        }
    }
}


