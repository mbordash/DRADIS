// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

/// Background task: position cleanup and orphan reconciliation.
///
/// Runs every 300 seconds (5 minutes) to:
/// 1. Remove positions for markets that have expired or are expiring within 60s.
/// 2. Detect and exit orphaned paired positions (Repair-policy pairs: ArbitrageStrategy;
///    TimeDecay holds and manages its own lone leg, see `state::lone_leg_policy`)
///    where the first leg filled but the second leg never did.
/// 3. Prune expired TimeDecay position metadata.
/// 4. Sync open_positions DB table against live on-chain holdings (purge stale rows).
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::sync::Arc;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::sol;
use alloy::sol_types::SolCall;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::Mutex;
use tokio::time::timeout as tokio_timeout;
use tracing::{debug, info, warn};

use crate::venues::intl::trading_client::TradingClient;
use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
// Data API v2 (v1 retired 2026-11-02; the SDK's data client still calls v1).
use crate::venues::intl::data_api;

use alloy::primitives::address as alloy_address;

use crate::helpers::{db, send_notification, PhantomCooldowns};
use crate::helpers::balance::OrphanTombstones;
use crate::state::{Position, PositionKey, PositionMap};
use crate::venues::core::MarketId;
use crate::venues::intl::u256_from_market_id;
use crate::venues::intl::wallet::WalletType;
use crate::venues::intl::wallet_executor::{ExecutionEffect, WalletExecutor};
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
/// PERMANENTLY_SETTLED_CONDITIONS). Settlement validates each held token ID
/// against its collateral before choosing the execution contract.
pub(crate) const PUSD_COLLATERAL: Address = alloy_address!("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB");

/// USDC.e (bridged USDC) on Polygon — collateral for pre-pUSD-migration positions.
pub(crate) const USDCE_COLLATERAL: Address = alloy_address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

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

    #[sol(rpc)]
    interface ISettlementTokens {
        function balanceOf(address account, uint256 id) external view returns (uint256);
        function isApprovedForAll(address account, address operator) external view returns (bool);
    }
    interface IProtocolRouter {
        function redeem(bytes31 conditionId, uint256 outcomeIndex, uint256 amount) external;
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


/// Remove all positions for a market that has expired or is expiring within 60s
/// from the map. Their rows stay: a real one for the chain reconciler, a
/// simulated one for the closed-hold sweep [B65].
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
            // A simulated position dropped here KEEPS its `open_positions` row:
            // the row is what the closed-hold sweep books from once Gamma states
            // the result (`ghost_settlement::settle_closed_ghost_holds`, run by
            // the lifecycle task) [B65]. Until 1.5.4 this deleted the row, so a
            // ghost hold through an hourly close vanished with no trade and no
            // P&L. A real position's row is reconciled against the chain and
            // swept by `purge_stale_open_positions`, as before.
            let before = pos_map.len();
            pos_map.retain(|k, _| k.market != yes_market && k.market != no_market);
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
    /// Strategy that held the orphan, for the trade row its sale writes.
    pub strategy_name: String,
    /// Market title, for the same row.
    pub market_name: String,
    /// The entry fee these shares carry, netted from the sale's P&L.
    pub entry_fee: Decimal,
}

pub async fn reconcile_orphaned_positions(
    positions: Arc<Mutex<PositionMap>>,
    clob_client: &Arc<TradingClient>,
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
    let mut legs_by_market: std::collections::HashMap<String, Vec<(PositionKey, Position)>> =
        std::collections::HashMap::new();
    for (key, position) in pos_map.iter() {
        let (strategy_name, token_id) = (&key.strategy, &key.market);
        if strategy_name != "ArbitrageStrategy" && strategy_name != "TimeDecayStrategy" {
            continue;
        }
        // Only a Repair-policy pair is this backstop's to repair. TimeDecay
        // (HoldAndManage) keeps its lone leg on purpose: the partner bid rests
        // for its window and the viper's exit holds the leg toward settlement.
        // Selling it here 60s after placement would reinstate the 2026-10-01
        // taker flatten by the back door.
        if crate::state::lone_leg_policy(strategy_name) == crate::state::LoneLegPolicy::HoldAndManage {
            continue;
        }
        if (now - position.opened_at).num_seconds() < 60 { continue; }
        legs_by_market
            .entry(position.market_name.clone())
            .or_default()
            .push((key.clone(), position.clone()));
    }

    // Fully-naked single legs → full orphan handling (re-hedge if cheap, else flatten).
    let mut orphans_to_exit: Vec<(PositionKey, Position)> = Vec::new();
    // Imbalanced pairs → flatten ONLY the naked excess of the heavier leg.
    let mut imbalanced_to_trim: Vec<(PositionKey, Position, Decimal)> = Vec::new();

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
                if k0.market == k1.market { continue; }
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

    for (key, position) in orphans_to_exit {
        let (strategy_name, token_id) = (key.strategy.clone(), key.market.clone());
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

        pos_map.remove(&key);

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
                strategy_name,
                market_name: position.market_name.clone(),
                entry_fee: position.entry_fee,
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
    for (key, position, excess) in imbalanced_to_trim {
        let (strategy_name, token_id) = (key.strategy.clone(), key.market.clone());
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
        // The entry fee follows the shares: the excess takes its share to the
        // sale that books it, and the hedged remainder keeps the rest.
        let excess_fee = if position.shares > Decimal::ZERO {
            position.entry_fee * (excess / position.shares)
        } else {
            Decimal::ZERO
        };
        if let Some(p) = pos_map.get_mut(&key) {
            p.shares = (p.shares - excess).max(Decimal::ZERO);
            p.entry_fee = (p.entry_fee - excess_fee).max(Decimal::ZERO);
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
                strategy_name,
                market_name: position.market_name.clone(),
                entry_fee: excess_fee,
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
// Shared with the Markets API, which infers the asset of a browsed market the
// same way; one definition so the two can never disagree on a title.
use crate::helpers::market_title::infer_asset_from_title;

/// HTTP client for the resolution lookups below.
///
/// Built once and reused: the sweep runs every 300s across every asset shard, and a
/// fresh client per pass would discard the connection pool each time. Timeouts are
/// short because a slow answer and no answer are handled identically here — the row
/// is deferred to the next sweep either way.
fn resolution_http() -> &'static reqwest::Client {
    static HTTP: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    HTTP.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(8))
            .build()
            .unwrap_or_default()
    })
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

    let live_positions = match tokio::time::timeout(
        std::time::Duration::from_secs(15),
        data_api::positions(safe_address),
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
    // Tokens the wallet demonstrably HOLDS but whose market title names no asset.
    //
    // `infer_asset_from_title` only recognizes bitcoin, ethereum and solana, so any
    // market outside those — a sports moneyline, say — infers nothing and its live
    // position never enters `live_by_asset`. Before 2026-09-17 that was the whole
    // story: the token was counted here and dropped, so it was missing from every
    // asset's `live_ids`, and `purge_stale_open_positions` read that absence as
    // "the wallet no longer holds this", deleted the row and booked a fabricated
    // exit at the last mark. The shares never moved. On 2026-09-17 that cycled four
    // times on one tennis market, booking +$4.67 of profit that did not exist while
    // $15.14 of real collateral sat in shares the engine had forgotten.
    //
    // Absence from the map means UNKNOWN ROUTING, never absent from the wallet.
    // These ids are therefore added to every asset's purge-protection set below,
    // and deliberately NOT to `live_by_asset`: adoption must not invent a BTC row
    // for a tennis position.
    let mut unmapped_live_ids: HashSet<String> = HashSet::new();
    for pos in &filtered_live_positions {
        let token = pos.asset.to_string();
        // Ownership first, title second.
        //
        // A shard that already holds a row for this token owns it, whatever the
        // market is called: that row was written by this engine, which recorded
        // what the position is at entry. Re-deriving the asset from the title
        // discards that and fails for every market outside btc/eth/sol. The title
        // guess is kept only for a token no shard has a row for, which is a
        // position bought outside DRADIS.
        let owner = match db::asset_owning_token(&token).await {
            Some(a) => Some(a),
            None => infer_asset_from_title(&pos.title).map(|a| a.to_string()),
        };
        if let Some(asset) = owner {
            live_by_asset
                .entry(asset)
                .or_default()
                .insert(token, *pos);
        } else {
            // No shard owns it and the title names no asset: a position this engine
            // never opened, in a market it cannot classify. It is still HELD, so it
            // is protected from the purge below, but nothing manages it.
            unmapped_live_ids.insert(token);
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
        // `live_map` drives ADOPTION into this asset's DB and must stay strictly
        // this asset's positions. `live_ids` answers a different question for the
        // purge — "does the wallet still hold this token?" — so the unmapped live
        // tokens belong in it. Without them the purge deletes rows for shares that
        // are demonstrably still held and books an invented exit price.
        let mut live_ids: HashSet<String> = live_map
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        live_ids.extend(unmapped_live_ids.iter().cloned());

        // ── Settled-and-already-redeemed positions ───────────────────────────
        //
        // A winner that redeems between two sync passes is never seen as
        // `redeemable`: it goes from live to gone, so it enters the purge in
        // neither set and hits the silent-delete arm — a real cash move with no
        // ledger row. That is what happened on 2026-08-31 to a FairValue position
        // (4.050628 shares at $0.79, settled at $1.00, +$0.80) whose row was
        // deleted and never booked.
        //
        // So ask the market what it resolved to, rather than inferring anything.
        // Gamma is authoritative and refuses to answer for a market that has not
        // closed. A token it prices is handed to `redeemable_marks`, where the
        // EXISTING resolution branch books it at exactly $1.00 or $0.00 with no
        // exit fee, idempotently and deduplicated — no new booking path.
        //
        // A size of zero is passed deliberately: the branch falls back to the row's
        // own quantity, which `settled_shares` preserves across the drift
        // corrector's zero write.
        //
        // Anything Gamma will not price — unresolved, or unreachable — is DEFERRED
        // rather than deleted. Retaining a row costs a stale dashboard entry for a
        // few minutes; deleting one costs the record permanently.
        let mut resolved_marks = redeemable_marks.clone();
        let mut deferred: HashSet<String> = HashSet::new();
        {
            // Deferral is bounded by AGE, not by attempts (see the shared
            // constant's doc). Past the bound the row falls through to the
            // pre-existing reconcile/purge path with a loud warning.
            let candidates: Vec<(String, String)> = db::confirmed_open_positions(&pool)
                .await
                .into_iter()
                .filter(|(t, _)| !live_ids.contains(t) && !redeemable_marks.contains_key(t))
                .collect();

            for (token, row_ts) in candidates {
                use crate::helpers::market::TokenResolution as R;
                let age_secs = chrono::DateTime::parse_from_rfc3339(&row_ts)
                    .map(|t| (chrono::Utc::now() - t.with_timezone(&chrono::Utc)).num_seconds())
                    .unwrap_or(i64::MAX);
                match crate::helpers::market::resolution_for_token(resolution_http(), &token).await {
                    R::Resolved(px) => {
                        info!("🧾 Settled position detected [{}]: market resolved at ${:.2} — booking",
                              &token[..token.len().min(12)], px);
                        resolved_marks.insert(token, (px, Decimal::ZERO));
                    }
                    // Verifiably still OPEN, so the position cannot have settled —
                    // redemption only exists after resolution. It left the wallet by
                    // a trade, which is the mark-priced ChainReconcile path's whole
                    // purpose. Adding it to neither set leaves that path reachable.
                    //
                    // Routing these to settlement instead would book an off-strategy
                    // sale at $1.00 or $0.00 rather than its actual sale price, and
                    // then delete the row so nothing could correct it.
                    //
                    // That path books at the row's mark, and a row this young may
                    // not have one: chain-sync only refreshes `current_price` for
                    // tokens the wallet holds, so a position opened and closed
                    // between two passes reaches the purge with NULL (2026-09-13:
                    // GBoost's row was 3.5 minutes old). Source the market's
                    // current price now, while the market is verifiably open, so
                    // the booking is at a real mark rather than labeled unknown.
                    R::NotClosed => {
                        if let Some(px) = crate::helpers::market::open_market_mark_for_token(resolution_http(), &token).await {
                            db::update_position_current_price(&pool, &token, px).await;
                            info!("🧾 Off-strategy exit detected [{}]: market still open, sourced mark ${:.4} for the booking",
                                  &token[..token.len().min(12)], px);
                        }
                    }
                    // No answer at all: never guess, retry next sweep — but not
                    // forever. A token the market API will never price again would
                    // otherwise pin its row open for the life of the process.
                    R::Unknown if age_secs < db::SETTLEMENT_DEFER_MAX_SECS => { deferred.insert(token); }
                    R::Unknown => {
                        warn!("⚠️ Resolution unavailable for {} after {}h — falling back to mark-priced reconciliation",
                              &token[..token.len().min(12)], age_secs / 3600);
                    }
                }
            }
        }

        // Purge stale rows in this asset's DB (books resolved legs at settlement value).
        total_purged += db::purge_stale_open_positions(&pool, &live_ids, &resolved_marks, &deferred).await;

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
                    // Never re-adopt a token the engine just closed. The Data API
                    // lags the book, so for a few seconds after a sale it still
                    // lists the pre-sale holding; adopting that creates a ghost
                    // the sweep then books as a second exit (2026-10-06, trade
                    // 156). The wallet's true balance is re-read next pass.
                    if !db::adoption_allowed(
                        db::recently_exited_at(token_str),
                        std::time::Instant::now(),
                        std::time::Duration::from_secs(db::CHAIN_ADOPT_EXIT_GRACE_SECS),
                    ) {
                        info!(" Chain-sync [{}]: NOT re-adopting token {} | {} shares — the engine closed it within {}s, \
                               so this is the Data API's pre-sale balance, not a position",
                            asset.to_uppercase(), &token_str[..token_str.len().min(20)], pos.size,
                            db::CHAIN_ADOPT_EXIT_GRACE_SECS);
                    } else if db::adopt_chain_position(&pool, token_str, &pos.title, side, pos.avg_price, pos.size, Some(pos.cur_price)).await {
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
                    // Read how the row was created, not just its size.
                    //
                    // `pos.size` and `pos.avg_price` are WALLET-level: the Data API
                    // reports the whole wallet's holding of this token and its average
                    // cost, with no notion of which strategy bought what. That is the
                    // right answer for a row this corrector adopted from chain, and the
                    // wrong answer for a row the engine wrote from its own fill — which
                    // is why this reads `engine_attributed` and not `chain_adopted`:
                    // `update_position_from_chain` sets `chain_adopted`, and it is
                    // reached from several ordinary engine paths, so that flag cannot
                    // tell an engine fill from an adoption.
                    //
                    // On 2026-10-03 a Helm position that had filled 190.476 shares at
                    // $0.0210 was rewritten to the wallet's 260.227 at a blended
                    // $0.0219, and settlement reads this row, so about $1.80 of a $6.09
                    // loss was booked against a conviction that never bought those
                    // shares.
                    let row: Option<(String, Option<String>, Option<i64>)> = sqlx::query_as(
                        "SELECT shares, entry_price, engine_attributed FROM open_positions WHERE token_id = ? LIMIT 1"
                    )
                    .bind(token_str)
                    .fetch_optional(&pool)
                    .await
                    .unwrap_or(None);

                    if let Some((db_shares_str, db_entry_price, engine_attributed)) = row {
                        let db_shares_val = db_shares_str.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                        let is_engine = engine_attributed.unwrap_or(0) == 1;
                        let decision = chain_correction_for(
                            is_engine, db_shares_val, pos.size, Decimal::new(1, 3),
                        );

                        // An engine-written row whose chain balance GREW holds shares
                        // this strategy did not buy. Crediting them changes its P&L, its
                        // calibration record, and — because an exit sells the row's size
                        // — what its next sale liquidates. Refuse, and say so: the
                        // silence is what made 2026-10-03 unexplainable afterwards.
                        if decision == ChainCorrection::RefuseUpward {
                            warn!(
                                "⚠️ Chain-sync [{}]: NOT correcting upward — token {} holds {} on chain \
                                 vs {} attributed for \"{}\". The extra {} share(s) did not come from this \
                                 strategy's fills, so crediting them would misattribute P&L and let an \
                                 exit sell them. Check the wallet for an unattributed purchase.",
                                asset.to_uppercase(),
                                &token_str[..token_str.len().min(20)],
                                pos.size, db_shares_val, pos.title, pos.size - db_shares_val,
                            );
                        } else if decision != ChainCorrection::Leave {
                            // Keep the entry price the fill established on an engine row;
                            // only a row adopted from chain has no better cost basis than
                            // the wallet average.
                            let price_to_write = if decision == ChainCorrection::SharesAndPrice {
                                pos.avg_price
                            } else {
                                db_entry_price
                                    .as_deref()
                                    .and_then(|p| p.parse::<Decimal>().ok())
                                    .filter(|p| *p > Decimal::ZERO)
                                    .unwrap_or(pos.avg_price)
                            };
                            db::update_position_from_chain(&pool, token_str, pos.size, price_to_write, Some(pos.cur_price)).await;
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
            if let Some(latest_snap) = db::get_pnl_history(&pool, 1, 24).await.into_iter().next() {
                if let Ok(collateral) = latest_snap.collateral.parse::<Decimal>() {
                    if collateral > Decimal::ZERO {
                        // Ask the ledger rather than copying the previous row's value
                        // forward. That copy made this writer a second, weaker authority
                        // over the figure: if the status task stalls, it would keep
                        // republishing the last known number indefinitely, which is the
                        // original freeze bug reintroduced through the back door.
                        let snap_session_pnl = db::session_realized_pnl(
                            &pool, crate::helpers::dynamic_config::ghosting_now()
                        ).await;
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
        warn!("⚠️ Chain-sync: {} live position(s) are held in the wallet but no shard currently tracks them \
               and their market title names no asset, so this engine cannot classify them (it may never have \
               opened them, or the row may have been removed earlier). They are protected from the stale purge and left in place, and they WILL still be \
               booked when the market resolves (the redeemable and resolution paths key on the token, not \
               the title), but no viper manages them meanwhile: no stop and no take-profit. Token(s): {}",
              unmatched_titles,
              unmapped_live_ids.iter().map(|t| t[..t.len().min(14)].to_string()).collect::<Vec<_>>().join(", "));
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
    executor: Arc<WalletExecutor<P>>,
    wallet_address: Address,
    eoa_address: Address,
) -> bool {
    if executor.wallet().funder != wallet_address || executor.wallet().signer != eoa_address {
        warn!("Auto-settle skipped: wallet identity mismatch");
        return false;
    }
    if crate::helpers::dynamic_config::ghosting_now()
        || crate::helpers::dynamic_config::read_only_mode()
    {
        return false;
    }
    crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::Settlement);
    let _guard = match AUTO_SETTLE_RUN_LOCK.try_lock() {
        Ok(g) => g,
        Err(_) => return false,
    };
    let positions = match tokio_timeout(
        std::time::Duration::from_secs(20),
        data_api::positions(wallet_address),
    )
    .await
    {
        Ok(Ok(positions)) => positions,
        _ => {
            warn!("Auto-settle: position read failed; no submission");
            return false;
        }
    };
    let mut groups: HashMap<(data_api::PositionSystem, B256), Vec<data_api::Position>> =
        HashMap::new();
    for position in positions.into_iter().filter(|p| p.size > Decimal::ZERO) {
        groups
            .entry((position.position_system, position.condition_id))
            .or_default()
            .push(position);
    }
    let mut settled_any = false;
    // A previous successful receipt can outlive the request or process that
    // submitted it. Never create a new nonce before verifying that intent.
    match executor.pending_operation().await {
        Ok(Some((target, calldata, hash))) => {
            let matched = pending_settlement_key(target, &calldata);
            if let Err(error) = executor.confirm_effect(hash).await {
                warn!(
                    "Auto-settle: pending token effect is not verified: {}",
                    error
                );
                return false;
            }
            if let Some(key) = matched {
                if let Some(legs) = groups.get(&key) {
                    if settlement_effect(executor.provider().clone(), wallet_address, legs).await {
                        finish_settlement(key.1, legs).await;
                        settled_any = true;
                    }
                }
            }
        }
        Ok(None) => {}
        Err(error) => {
            warn!("Auto-settle: pending operation unresolved: {}", error);
            return false;
        }
    }
    for ((system, condition_id), legs) in groups {
        if !legs.iter().any(|p| p.redeemable) {
            continue;
        }
        if PERMANENTLY_SETTLED_CONDITIONS
            .lock()
            .await
            .contains(&condition_id)
        {
            continue;
        }
        if let Some(last) = FAILED_SETTLEMENT_CONDITIONS.lock().await.get(&condition_id) {
            if (Utc::now() - *last).num_seconds() < SETTLEMENT_CONDITION_ERROR_COOLDOWN_SECS {
                continue;
            }
        }
        let calls = match settlement_calls(executor.as_ref(), system, condition_id, &legs).await {
            Ok(calls) if !calls.is_empty() => calls,
            Ok(_) => continue,
            Err(error) => {
                warn!("Auto-settle: route verification failed: {}", error);
                continue;
            }
        };
        let mut failed = false;
        for (target, calldata, token_ids) in calls {
            let effect = ExecutionEffect::TokenBalancesZero {
                contract: token_contract(system),
                token_ids: token_ids.clone(),
            };
            match executor.execute_with_effect(target, calldata, effect).await {
                Ok(hash) => {
                    let proof_legs: Vec<_> = legs
                        .iter()
                        .filter(|leg| token_ids.contains(&leg.asset))
                        .cloned()
                        .collect();
                    if !settlement_effect(executor.provider().clone(), wallet_address, &proof_legs)
                        .await
                    {
                        warn!("Auto-settle: tx {} confirmed but token redemption is unverified; intent retained", hash);
                        failed = true;
                        break;
                    }
                    if let Err(error) = executor.confirm_effect(hash).await {
                        warn!("Auto-settle: could not persist verified effect: {}", error);
                        failed = true;
                        break;
                    }
                }
                Err(error) => {
                    warn!("Auto-settle: execution unresolved or failed: {}", error);
                    FAILED_SETTLEMENT_CONDITIONS
                        .lock()
                        .await
                        .insert(condition_id, Utc::now());
                    failed = true;
                    break;
                }
            }
        }
        if !failed && settlement_effect(executor.provider().clone(), wallet_address, &legs).await {
            finish_settlement(condition_id, &legs).await;
            settled_any = true;
        }
    }
    settled_any
}

const CTF_COLLATERAL_ADAPTER: Address =
    alloy_address!("0xAdA100Db00Ca00073811820692005400218FcE1f");
const NEG_RISK_COLLATERAL_ADAPTER: Address =
    alloy_address!("0xadA2005600Dec949baf300f4C6120000bDB6eAab");
const POSITION_MANAGER: Address = alloy_address!("0x006F54F7f9A22e0000CC2AB60031000000ae9fEF");
const PROTOCOL_ROUTER: Address = alloy_address!("0x12121212006e4CD160D18e3f00711DA5c3372600");

type SettlementCall = (Address, Vec<u8>, Vec<U256>);

fn token_contract(system: data_api::PositionSystem) -> Address {
    match system {
        data_api::PositionSystem::Ctf => CTF_ADDRESS,
        data_api::PositionSystem::ProtocolV2 => POSITION_MANAGER,
    }
}

async fn held_units<P: Provider + Clone>(
    provider: P,
    system: data_api::PositionSystem,
    wallet: Address,
    token: U256,
) -> anyhow::Result<U256> {
    let contract = ISettlementTokens::new(token_contract(system), provider);
    tokio_timeout(
        std::time::Duration::from_secs(10),
        contract.balanceOf(wallet, token).call(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("settlement balance read timed out"))?
    .map_err(|_| anyhow::anyhow!("settlement balance read failed"))
}

async fn settlement_effect<P: Provider + Clone>(
    provider: P,
    wallet: Address,
    legs: &[data_api::Position],
) -> bool {
    if legs.is_empty() {
        return false;
    }
    for leg in legs {
        match held_units(provider.clone(), leg.position_system, wallet, leg.asset).await {
            Ok(balance) if balance.is_zero() => {}
            _ => return false,
        }
    }
    true
}

async fn finish_settlement(condition: B256, legs: &[data_api::Position]) {
    RECENT_SETTLEMENT_SUBMITS
        .lock()
        .await
        .insert(condition, Utc::now());
    PERMANENTLY_SETTLED_CONDITIONS
        .lock()
        .await
        .insert(condition);
    purge_settled_legs(legs).await;
    record_settled_arb_trade(legs).await;
    info!(
        "Auto-settle: condition {} redeemed; token effect verified",
        condition
    );
}

async fn settlement_calls<P: Provider + Clone>(
    executor: &WalletExecutor<P>,
    system: data_api::PositionSystem,
    condition: B256,
    legs: &[data_api::Position],
) -> anyhow::Result<Vec<SettlementCall>> {
    use alloy::primitives::keccak256;
    use anyhow::ensure;
    ensure!(!legs.is_empty(), "settlement has no legs");
    ensure!(
        legs.iter().all(|p| p.position_system == system
            && p.condition_id == condition
            && matches!(p.outcome_index, 0 | 1)),
        "settlement metadata conflicts"
    );
    let provider = executor.provider().clone();
    let wallet = executor.wallet();
    let neg_risk = legs[0].negative_risk;
    ensure!(
        legs.iter().all(|p| p.negative_risk == neg_risk),
        "conflicting neg-risk metadata"
    );
    let mut held = Vec::new();
    let mut collateral = None;
    for leg in legs {
        if system == data_api::PositionSystem::Ctf {
            let ctf = ICtfView::new(CTF_ADDRESS, provider.clone());
            let collection = tokio_timeout(
                std::time::Duration::from_secs(10),
                ctf.getCollectionId(B256::ZERO, condition, U256::from(1u64 << leg.outcome_index))
                    .call(),
            )
            .await
            .map_err(|_| anyhow::anyhow!("CTF collection read timed out"))?
            .map_err(|_| anyhow::anyhow!("CTF collection read failed"))?;
            let matched = [PUSD_COLLATERAL, USDCE_COLLATERAL]
                .into_iter()
                .find(|candidate| {
                    let mut bytes = candidate.as_slice().to_vec();
                    bytes.extend_from_slice(collection.as_slice());
                    U256::from_be_bytes(keccak256(bytes).0) == leg.asset
                })
                .ok_or_else(|| {
                    anyhow::anyhow!("token ID does not match a supported CTF collateral")
                })?;
            ensure!(
                collateral.is_none_or(|prior| prior == matched),
                "mixed collateral in settlement condition"
            );
            collateral = Some(matched);
        } else {
            let encoded = leg.asset.to_be_bytes::<32>();
            ensure!(
                encoded[..31] == condition.as_slice()[..31]
                    && encoded[31] == leg.outcome_index as u8,
                "Protocol V2 token metadata mismatch"
            );
        }
        let units = held_units(provider.clone(), system, wallet.funder, leg.asset).await?;
        // The supplied token must belong to one ledger only.
        let other = match system {
            data_api::PositionSystem::Ctf => data_api::PositionSystem::ProtocolV2,
            _ => data_api::PositionSystem::Ctf,
        };
        ensure!(
            held_units(provider.clone(), other, wallet.funder, leg.asset)
                .await?
                .is_zero(),
            "token held in multiple position systems"
        );
        if !units.is_zero() {
            held.push((leg.asset, leg.outcome_index, units));
        }
    }
    if held.is_empty() {
        return Ok(Vec::new());
    }
    let target = match system {
        data_api::PositionSystem::ProtocolV2 => PROTOCOL_ROUTER,
        data_api::PositionSystem::Ctf if wallet.wallet_type == WalletType::Poly1271 => {
            ensure!(
                collateral == Some(PUSD_COLLATERAL),
                "Deposit Wallet CTF settlement requires verified pUSD collateral"
            );
            if neg_risk {
                NEG_RISK_COLLATERAL_ADAPTER
            } else {
                CTF_COLLATERAL_ADAPTER
            }
        }
        data_api::PositionSystem::Ctf => {
            if neg_risk {
                NEG_RISK_ADAPTER_ADDRESS
            } else {
                CTF_ADDRESS
            }
        }
    };
    if target != CTF_ADDRESS
        && !(wallet.wallet_type == WalletType::GnosisSafe
            && system == data_api::PositionSystem::Ctf
            && neg_risk)
    {
        let tokens = ISettlementTokens::new(token_contract(system), provider.clone());
        let approved = tokio_timeout(
            std::time::Duration::from_secs(10),
            tokens.isApprovedForAll(wallet.funder, target).call(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("settlement approval read timed out"))?
        .map_err(|_| anyhow::anyhow!("settlement approval read failed"))?;
        ensure!(
            approved,
            "settlement contract lacks required ERC1155 operator approval"
        );
    }
    if system == data_api::PositionSystem::ProtocolV2 {
        return Ok(held
            .into_iter()
            .map(|(token, outcome, units)| {
                let mut condition31 = [0u8; 31];
                condition31.copy_from_slice(&condition.as_slice()[..31]);
                (
                    target,
                    IProtocolRouter::redeemCall {
                        conditionId: condition31.into(),
                        outcomeIndex: U256::from(outcome as u64),
                        amount: units,
                    }
                    .abi_encode(),
                    vec![token],
                )
            })
            .collect());
    }
    let data = if wallet.wallet_type == WalletType::GnosisSafe && neg_risk {
        let mut amounts = vec![U256::ZERO; 2];
        for (_, outcome, units) in &held {
            amounts[*outcome as usize] += *units;
        }
        INegRiskDirect::redeemPositionsCall {
            conditionId: condition,
            amounts,
        }
        .abi_encode()
    } else {
        ICtfDirect::redeemPositionsCall {
            collateralToken: collateral.unwrap(),
            parentCollectionId: B256::ZERO,
            conditionId: condition,
            indexSets: vec![U256::from(1), U256::from(2)],
        }
        .abi_encode()
    };
    Ok(vec![(
        target,
        data,
        held.into_iter().map(|(token, _, _)| token).collect(),
    )])
}

fn pending_settlement_key(
    target: Address,
    data: &[u8],
) -> Option<(data_api::PositionSystem, B256)> {
    if target == PROTOCOL_ROUTER {
        let call = IProtocolRouter::redeemCall::abi_decode(data).ok()?;
        let mut condition = [0u8; 32];
        condition[..31].copy_from_slice(call.conditionId.as_slice());
        Some((data_api::PositionSystem::ProtocolV2, B256::from(condition)))
    } else if matches!(
        target,
        CTF_ADDRESS | CTF_COLLATERAL_ADAPTER | NEG_RISK_COLLATERAL_ADAPTER
    ) {
        Some((
            data_api::PositionSystem::Ctf,
            ICtfDirect::redeemPositionsCall::abi_decode(data)
                .ok()?
                .conditionId,
        ))
    } else if target == NEG_RISK_ADAPTER_ADDRESS {
        Some((
            data_api::PositionSystem::Ctf,
            INegRiskDirect::redeemPositionsCall::abi_decode(data)
                .ok()?
                .conditionId,
        ))
    } else {
        None
    }
}

async fn record_settled_arb_trade(
    legs: &[data_api::Position],
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

            let scope = db::filing_scope_for_market(&pool, &market_title).await;
            let inserted = db::record_settlement_trade_idempotent(
                &pool,
                &scope,
                "ArbitrageStrategy",
                &market_title,
                "YES",
                entry_per_pair,
                Decimal::ONE,
                pairs,
                pnl,
                // Arb enters on resting maker bids; makers pay no venue fee.
                Decimal::ZERO,
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
            let scope = db::filing_scope_for_market(&pool, &market_title).await;
            let inserted = db::record_settlement_trade_idempotent(
                &pool,
                &scope,
                settle_strategy,
                &market_title,
                side,
                avg_price,
                exit_price,
                size,
                pnl,
                // Maker entry — no venue fee on the opening leg.
                Decimal::ZERO,
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
async fn purge_settled_legs(legs: &[data_api::Position]) {
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
    let current_positions = match tokio::time::timeout(
        std::time::Duration::from_secs(20),
        data_api::positions(safe_address),
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

    // ── Resolution evidence: on-chain REDEEM activity ────────────────────────
    // CRITICAL: a token vanishing from `active_tokens` does NOT mean the market
    // settled — it also vanishes when the leg was SOLD/FLATTENED (orphan cleanup,
    // early exit) or never filled at all. Booking a $1.00 "auto-settled" trade on
    // mere absence fabricates phantom profit on a still-open market (root cause of
    // the 2026-06-21 trade booked at 04:18 on a noon-close market).
    //
    // This used to infer redemption from `/closed-positions`: a redemption closes
    // at/after the market's `end_date`, an early sale strictly before it. That was
    // wrong twice over, and both faults were found together on 2026-08-15.
    //
    // 1. It never ran. `ClosedPosition::end_date` is typed `DateTime<Utc>` in
    //    polymarket_client_sdk_v2 0.7.0, but the API returns a date-only
    //    "YYYY-MM-DD" string, so chrono rejected every response with "premature
    //    end of input" — 72 failures in 74 cycles, i.e. 100% of calls that got
    //    this far. The whole settlement path had been dead, silently.
    //
    // 2. Fixing the parse would have been WORSE than leaving it broken. A
    //    date-only `end_date` resolves to midnight UTC, which for an hourly
    //    crypto market precedes the actual resolution by up to a full day — so
    //    "closed at/after end_date" is true for essentially every close,
    //    including ordinary early sales. Both of that night's sales (a FairValue
    //    stop-loss at 08:00Z and a TimeDecay orphan flatten at 05:35Z, against an
    //    endDate of 2026-08-15 00:00Z) would have been booked a second time as
    //    settlements — exactly the phantom-profit failure the paragraph above
    //    warns about.
    //
    // `/activity` filtered to REDEEM is direct evidence rather than an inference:
    // a REDEEM row exists if and only if the leg was actually redeemed on-chain.
    // It carries integer timestamps (no chrono landmine), the type filter is
    // applied server-side, and a leg sold early simply has no row.
    let redeem_cutoff = (Utc::now() - chrono::Duration::days(14)).timestamp().max(0) as u64;
    let redemptions = match tokio::time::timeout(
        std::time::Duration::from_secs(20),
        data_api::redemptions(safe_address, redeem_cutoff),
    ).await {
        Ok(Ok(a)) => a,
        Ok(Err(e)) => {
            warn!("Orphan detection [{}]: activity(REDEEM) error: {} — skipping settlement booking this cycle", squadron_asset, e);
            return;
        }
        Err(_) => {
            warn!("Orphan detection [{}]: activity(REDEEM) timed out (20s) — skipping settlement booking this cycle", squadron_asset);
            return;
        }
    };

    // Token ids (decimal strings) with an on-chain redemption in the window.
    // Defensive re-filter on activity_type: the server honors the filter today,
    // but booking a phantom settlement is expensive enough to not rely on that.
    let redeemed_tokens: HashSet<String> = redemptions
        .iter()
        .filter(|a| a.activity_type == "REDEEM")
        .filter_map(|a| a.asset.as_ref().map(|t| t.to_string()))
        .collect();

    debug!("Orphan detection [{}]: {} redeemed token(s) in the last 14d", squadron_asset, redeemed_tokens.len());

    // A leg was genuinely REDEEMED at settlement (not sold early / never filled).
    let redeemed_at_resolution = |token: &str| -> bool { redeemed_tokens.contains(token) };

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
                        let scope = db::filing_scope_for_market(&pool, &market_name).await;
                        let inserted = db::record_settlement_trade_idempotent(
                            &pool,
                            &scope,
                            "ArbitrageStrategy",
                            &market_name,
                            "YES",
                            entry_per_pair,
                            Decimal::ONE,
                            pairs,
                            pnl,
                            // Maker entry — no venue fee on the opening leg.
                            Decimal::ZERO,
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



/// What the chain-sync corrector should do with one `open_positions` row.
///
/// Split out from the corrector loop so the decision can be tested without a
/// Data API response, because getting it wrong costs money in a way the loop
/// makes hard to see.
#[derive(Debug, PartialEq, Eq)]
pub enum ChainCorrection {
    /// On-chain and booked agree within dust.
    Leave,
    /// Correct the share count and keep the entry price the fill established.
    SharesOnly,
    /// Chain is authoritative for both, because the engine never saw a fill here.
    SharesAndPrice,
    /// On-chain grew on a row the engine wrote: the extra shares are not this
    /// strategy's and must not be credited to it.
    RefuseUpward,
}

/// Decide, from how the row was created and which way the balance moved.
///
/// The Data API and `balance_allowance` both report a WALLET total, with no
/// notion of which strategy bought what. For a row this corrector adopted from
/// chain that is the best cost basis available. For a row the engine wrote from
/// its own fill it is not: on 2026-10-03 a Helm row of 190.476 shares at $0.0210
/// was rewritten to the wallet's 260.227 at a blended $0.0219, and settlement
/// reads the row, so about $1.80 of a $6.09 loss was booked against a conviction
/// that never bought those shares.
///
/// Keyed on `engine_attributed`, NOT on `chain_adopted`. `chain_adopted` cannot
/// answer this: `update_position_from_chain` sets it, and that is reached from
/// several ordinary engine paths (a partial FAK exit, a partial paired exit, a
/// maker quote pull, the 60-second drift corrector), so a row that began as an
/// engine fill looks adopted forever after its first correction — including after
/// a correction this very function authorized.
pub fn chain_correction_for(
    is_engine_attributed: bool,
    db_shares: Decimal,
    chain_shares: Decimal,
    tolerance: Decimal,
) -> ChainCorrection {
    if (chain_shares - db_shares).abs() <= tolerance {
        return ChainCorrection::Leave;
    }
    if !is_engine_attributed {
        // Nothing better to go on, and this is the case the corrector was built
        // for: an adoption that recorded a partial-fill size.
        return ChainCorrection::SharesAndPrice;
    }
    if chain_shares > db_shares {
        ChainCorrection::RefuseUpward
    } else {
        // Downward on an engine row is a sale or a settlement, which is real.
        ChainCorrection::SharesOnly
    }
}

#[cfg(test)]
mod settlement_evidence_tests {
    use super::*;

    /// A market title naming no asset must be recognized as UNROUTABLE, never as
    /// absent from the wallet.
    ///
    /// `infer_asset_from_title` matches only bitcoin, ethereum and solana, so a
    /// sports market returns None and its live position cannot be routed to an
    /// asset shard. Before 2026-09-17 such a position was dropped from
    /// `live_by_asset` and therefore from every shard's `live_ids`, which
    /// `purge_stale_open_positions` reads as "the wallet no longer holds this":
    /// it deleted the row and booked a fabricated exit at the last mark. The
    /// shares had not moved. On one tennis market that cycled four times, writing
    /// +$4.67 of profit that never happened while $15.14 of real collateral sat
    /// in shares the engine had forgotten, and each purge made the Maker viper
    /// believe it held nothing, so it bought again.
    ///
    /// This pins both halves: sports titles are unroutable, and crypto titles
    /// still route, so protecting the former cannot have broken the latter.
    #[test]
    fn a_wallet_position_whose_title_names_no_asset_is_unroutable_not_absent() {
        // The market that actually caused the incident, and its neighbors.
        for title in [
            "Valencia: Guiomar Maristany vs Marina Bassols Ribera",
            "Devils vs. Flames",
            "Will the Fed cut rates in December?",
        ] {
            assert_eq!(infer_asset_from_title(title), None,
                       "{title} names no asset, so it cannot be routed to a shard");
        }
        // Routing for the markets DRADIS does trade must be unchanged.
        assert_eq!(infer_asset_from_title("Bitcoin Up or Down - September 17, 4AM ET"), Some("btc"));
        assert_eq!(infer_asset_from_title("Bitcoin Up or Down on June 7?"), Some("btc"));
        assert_eq!(infer_asset_from_title("Will ETH exceed $3000?"), Some("eth"));
        assert_eq!(infer_asset_from_title("Solana Up or Down on September 17?"), Some("sol"));
    }

}

#[cfg(test)]
mod settlement_routing_tests {
    use super::*;
    use alloy::primitives::keccak256;
    use alloy::signers::local::LocalSigner;
    use axum::{extract::State, routing::post, Json, Router};

    #[derive(Clone)]
    struct Views {
        system: data_api::PositionSystem,
        approved: bool,
        wallet: Address,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    async fn respond(
        State(views): State<Views>,
        Json(request): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        assert_eq!(
            request["method"], "eth_call",
            "routing must never dispatch a transaction"
        );
        views
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let call = &request["params"][0];
        let target = call["to"].as_str().unwrap().parse::<Address>().unwrap();
        let input = call
            .get("input")
            .or_else(|| call.get("data"))
            .unwrap()
            .as_str()
            .unwrap();
        let input = alloy::primitives::hex::decode(input.trim_start_matches("0x")).unwrap();
        let result = if input.starts_with(&ICtfView::getCollectionIdCall::SELECTOR) {
            assert_eq!(target, CTF_ADDRESS);
            let call = ICtfView::getCollectionIdCall::abi_decode(&input).unwrap();
            U256::from_be_bytes(B256::repeat_byte(call.indexSet.to::<u8>()).0)
        } else if input.starts_with(&ISettlementTokens::balanceOfCall::SELECTOR) {
            let call = ISettlementTokens::balanceOfCall::abi_decode(&input).unwrap();
            assert_eq!(call.account, views.wallet);
            if target == token_contract(views.system) {
                U256::from(2_000_000)
            } else {
                U256::ZERO
            }
        } else {
            assert!(input.starts_with(&ISettlementTokens::isApprovedForAllCall::SELECTOR));
            let call = ISettlementTokens::isApprovedForAllCall::abi_decode(&input).unwrap();
            assert_eq!(call.account, views.wallet);
            U256::from(u8::from(views.approved))
        };
        Json(
            serde_json::json!({"jsonrpc":"2.0", "id":request["id"], "result":format!("0x{result:064x}")}),
        )
    }

    struct Fixture<P> {
        executor: WalletExecutor<P>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        server: tokio::task::JoinHandle<()>,
    }
    impl<P> Drop for Fixture<P> {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    async fn fixture(
        wallet_type: WalletType,
        system: data_api::PositionSystem,
        approved: bool,
    ) -> Fixture<impl Provider + Clone> {
        let signer = LocalSigner::from_slice(&[1u8; 32]).unwrap();
        let wallet = crate::venues::intl::wallet::WalletConfig {
            wallet_type,
            signer: signer.address(),
            funder: alloy_address!("2222222222222222222222222222222222222222"),
        };
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = Router::new().route("/", post(respond)).with_state(Views {
            system,
            approved,
            wallet: wallet.funder,
            calls: Arc::clone(&calls),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let provider = alloy::providers::ProviderBuilder::new().connect_http(host.parse().unwrap());
        Fixture {
            executor: WalletExecutor::for_read_tests(provider, signer, wallet),
            calls,
            server,
        }
    }

    fn position(
        system: data_api::PositionSystem,
        outcome: i32,
        neg_risk: bool,
    ) -> data_api::Position {
        let condition_id = if system == data_api::PositionSystem::ProtocolV2 {
            let mut bytes = [0x11; 32];
            bytes[31] = 0;
            B256::from(bytes)
        } else {
            B256::repeat_byte(0x11)
        };
        let asset = if system == data_api::PositionSystem::Ctf {
            let mut bytes = PUSD_COLLATERAL.as_slice().to_vec();
            bytes.extend_from_slice(B256::repeat_byte((outcome + 1) as u8).as_slice());
            U256::from_be_bytes(keccak256(bytes).0)
        } else {
            let mut bytes = condition_id.0;
            bytes[31] = outcome as u8;
            U256::from_be_bytes(bytes)
        };
        data_api::Position {
            asset,
            condition_id,
            position_system: system,
            size: Decimal::from(2),
            avg_price: Decimal::new(5, 1),
            current_value: Decimal::from(2),
            cur_price: Decimal::ONE,
            redeemable: true,
            negative_risk: neg_risk,
            title: "Synthetic settlement".into(),
            outcome: outcome.to_string(),
            outcome_index: outcome,
        }
    }

    #[tokio::test]
    async fn ctf_routes_preserve_safe_abi_and_select_deposit_collateral_adapters() {
        for wallet_type in [WalletType::GnosisSafe, WalletType::Poly1271] {
            for neg_risk in [false, true] {
                let fixture = fixture(wallet_type, data_api::PositionSystem::Ctf, true).await;
                let legs = vec![
                    position(data_api::PositionSystem::Ctf, 0, neg_risk),
                    position(data_api::PositionSystem::Ctf, 1, neg_risk),
                ];
                let calls = settlement_calls(
                    &fixture.executor,
                    data_api::PositionSystem::Ctf,
                    legs[0].condition_id,
                    &legs,
                )
                .await
                .unwrap();
                assert_eq!(calls.len(), 1);
                let (target, data, tokens) = &calls[0];
                assert_eq!(
                    tokens,
                    &legs.iter().map(|leg| leg.asset).collect::<Vec<_>>()
                );
                if wallet_type == WalletType::GnosisSafe && neg_risk {
                    assert_eq!(*target, NEG_RISK_ADAPTER_ADDRESS);
                    let call = INegRiskDirect::redeemPositionsCall::abi_decode(data).unwrap();
                    assert_eq!(call.conditionId, legs[0].condition_id);
                    assert_eq!(call.amounts, vec![U256::from(2_000_000); 2]);
                } else {
                    assert_eq!(
                        *target,
                        match (wallet_type, neg_risk) {
                            (WalletType::GnosisSafe, _) => CTF_ADDRESS,
                            (_, false) => CTF_COLLATERAL_ADAPTER,
                            (_, true) => NEG_RISK_COLLATERAL_ADAPTER,
                        }
                    );
                    let call = ICtfDirect::redeemPositionsCall::abi_decode(data).unwrap();
                    assert_eq!(call.collateralToken, PUSD_COLLATERAL);
                    assert_eq!(call.parentCollectionId, B256::ZERO);
                    assert_eq!(call.conditionId, legs[0].condition_id);
                    assert_eq!(call.indexSets, vec![U256::from(1), U256::from(2)]);
                }
                assert!(fixture.calls.load(std::sync::atomic::Ordering::Relaxed) > 0);
            }
        }
    }

    #[tokio::test]
    async fn protocol_v2_routes_each_outcome_through_the_router() {
        for wallet_type in [WalletType::GnosisSafe, WalletType::Poly1271] {
            let fixture = fixture(wallet_type, data_api::PositionSystem::ProtocolV2, true).await;
            let legs = vec![
                position(data_api::PositionSystem::ProtocolV2, 0, false),
                position(data_api::PositionSystem::ProtocolV2, 1, false),
            ];
            let calls = settlement_calls(
                &fixture.executor,
                data_api::PositionSystem::ProtocolV2,
                legs[0].condition_id,
                &legs,
            )
            .await
            .unwrap();
            assert_eq!(calls.len(), 2);
            for ((target, data, tokens), leg) in calls.iter().zip(&legs) {
                assert_eq!(*target, PROTOCOL_ROUTER);
                let call = IProtocolRouter::redeemCall::abi_decode(data).unwrap();
                assert_eq!(
                    call.conditionId.as_slice(),
                    &leg.condition_id.as_slice()[..31]
                );
                assert_eq!(call.outcomeIndex, U256::from(leg.outcome_index as u64));
                assert_eq!(call.amount, U256::from(2_000_000));
                assert_eq!(tokens, &vec![leg.asset]);
            }
        }
    }

    #[tokio::test]
    async fn unsupported_metadata_and_missing_approvals_never_produce_executable_calls() {
        for system in [
            data_api::PositionSystem::Ctf,
            data_api::PositionSystem::ProtocolV2,
        ] {
            let allowed = fixture(WalletType::Poly1271, system, true).await;
            let mut leg = position(system, 0, false);
            leg.asset = U256::ZERO;
            assert!(
                settlement_calls(&allowed.executor, system, leg.condition_id, &[leg])
                    .await
                    .is_err()
            );
            let mut leg = position(system, 0, false);
            leg.outcome_index = 2;
            assert!(
                settlement_calls(&allowed.executor, system, leg.condition_id, &[leg])
                    .await
                    .is_err()
            );
            let fixture = fixture(WalletType::Poly1271, system, false).await;
            let leg = position(system, 0, false);
            assert!(
                settlement_calls(&fixture.executor, system, leg.condition_id, &[leg])
                    .await
                    .is_err()
            );
        }
    }
}

// The simulated-settlement pieces are venue-neutral and live in
// `helpers::ghost_settlement` since 1.4.2; re-exported so this module's own
// Gamma-backed sweep and its callers read as before.
pub(crate) use crate::helpers::ghost_settlement::{
    book_ghost_settlements, price_for, settlement_candidates, PriceSource,
};

/// Book simulated positions on an event market as settlements once the venue
/// has stopped accepting orders, then drop them from the map.
///
/// **Polymarket International only.** Polymarket US runs its own deployment
/// loop (`UsDeploymentRunner::run_pinned`) which never enters
/// `Squadron::patrol`, so neither the venue-status retirement nor this booking
/// exists there, and a ghost event squadron on that venue pins in exactly the
/// way described below. Kalshi's pinned loop has had the venue-status
/// retirement since 1.4.1 and books its simulated positions through
/// `helpers::ghost_settlement::settle_ghost_positions_at` from the venue's own `result` field, which
/// needs no price lookup. Fixing Polymarket US means giving its loop the same
/// two things.
///
/// Ghost mode has no settlement path of its own: nothing anywhere books a
/// simulated position at resolution, because the two mechanisms that close
/// positions are chain-based and exclude ghost rows by design. On a rotating
/// crypto squadron that was invisible — the next rotation swept the row and the
/// map entry with it. An event squadron never rotates: it leaves by retirement,
/// and retirement refuses while the squadron holds anything. So a simulated
/// position on a sports or politics market stayed in the map forever, kept its
/// `open_positions` row open at its last mark, and pinned the squadron, which
/// blocked the class permanently — the seeder skips a class that already has a
/// live squadron. One simulated game would end ghost trading for that class.
///
/// Gated on the venue's own "no longer accepting orders" rather than the stated
/// close, for the reason retirement already trusts it: a sports market's close
/// time is kick-off itself on football and soccer but a week later on MLB, so
/// the stated close is not evidence that anything has settled.
///
/// `Unknown` defers rather than guessing, but only up to
/// `venue_closed_for_secs >= db::SETTLEMENT_DEFER_MAX_SECS`, the same bound the
/// live sweep uses. Without a bound the function reintroduced the pin it exists
/// to remove: `resolution_for_token` answers `Unknown` for any price between
/// $0.01 and $0.99, and Polymarket settles canceled and tied games at $0.50, so
/// a simulated position on a tie would have deferred forever. Past the bound it
/// books at Gamma's own price and names it in the reason.
///
/// The caller is expected to throttle this: it makes one Gamma request per
/// distinct token per pass and each can stall for the HTTP timeout, so running
/// it on every 75ms tick while a resolution is pending would be hundreds of
/// requests a minute against a venue that has already rate-limited this engine.
pub async fn settle_ghost_event_positions(
    positions: &Arc<Mutex<crate::state::PositionMap>>,
    http: &reqwest::Client,
    asset: &str,
    market_class: Option<&str>,
    condition_id: &str,
    yes_market: &MarketId,
    no_market: &MarketId,
    venue_closed_for_secs: i64,
) -> usize {
    use std::collections::HashMap as StdHashMap;

    // Snapshot first: the resolution probe is network-bound and must not be
    // awaited while the position map is locked — the patrol tick takes it.
    let candidates = {
        let map = positions.lock().await;
        settlement_candidates(&map, yes_market, no_market)
    };
    if candidates.is_empty() {
        return 0;
    }
    let Some(pool) = crate::helpers::db::pool_for(asset) else { return 0 };

    // One Gamma request for the market, which prices both sides, rather than a
    // probe per token. The rule is the sports ledger's own `resolved_price`:
    // 0 or 1 outright, and 0.5 only when Gamma also reports
    // `umaResolutionStatus: resolved`.
    //
    // This replaced a helper of mine that asked "closed, past endDate, both
    // outcomes 0.50". On a survey of closed sports markets that shape matched
    // 472 markets of which only 352 were resolved; the other 120 were
    // never-resolved placeholders sitting at 0.5/0.5, and every one would have
    // been booked as a tie. `endDate` is worthless as a discriminator here —
    // on a game market it is kick-off, so every finished game is past it.
    let past_defer_bound = venue_closed_for_secs >= crate::helpers::db::SETTLEMENT_DEFER_MAX_SECS;
    let mut resolved_px: StdHashMap<String, (Decimal, PriceSource)> = StdHashMap::new();
    let mut tokens: Vec<String> = candidates.iter().map(|(k, _)| k.market.to_string()).collect();
    tokens.sort();
    tokens.dedup();
    let settled_now = crate::raptors::sports_ledger::settled_prices_for_market(
        http, condition_id, &tokens,
    ).await;
    for token in &tokens {
        let settled = settled_now.get(token).and_then(|p| Decimal::from_f64_retain(*p));
        // The mark is a network call, so it is fetched only in the one case
        // that can use it: nothing settled and the deferral bound passed.
        let mark = if settled.is_none() && past_defer_bound {
            crate::helpers::market::open_market_mark_for_token(http, token).await
        } else {
            None
        };
        if let Some((px, source)) = price_for(settled, past_defer_bound, mark) {
            if source == PriceSource::MarkFallback {
                warn!(
                    "👻 Simulated settlement: {} still undecided {}h after the venue closed — \
                     falling back to the mark ${:.2}; this is not a resolution",
                    token, venue_closed_for_secs / 3600, px,
                );
            }
            resolved_px.insert(token.clone(), (px, source));
        }
    }
    if resolved_px.is_empty() {
        return 0;
    }

    book_ghost_settlements(positions, &pool, asset, market_class, yes_market, no_market, &resolved_px, candidates).await
}

#[cfg(test)]
mod chain_correction_tests {
    use super::{chain_correction_for as decide, ChainCorrection as C};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    const TOL: Decimal = dec!(0.001);

    /// The case this corrector was built for: an adoption that recorded a
    /// partial-fill size, where chain is the only cost basis there is.
    #[test]
    fn an_adopted_row_defers_to_chain_for_both() {
        assert_eq!(decide(false, dec!(8.0399), dec!(10), TOL), C::SharesAndPrice);
        // Adopted rows defer downward too.
        assert_eq!(decide(false, dec!(10), dec!(4), TOL), C::SharesAndPrice);
    }

    /// The 2026-10-03 live Helm trade, which is why this function exists.
    ///
    /// The row held the 190.476 shares the fill established; the wallet held
    /// 260.227 including 69.751 the conviction never bought. Correcting upward
    /// credited them to Helm, and settlement reads the row.
    #[test]
    fn an_engine_row_refuses_to_grow() {
        assert_eq!(
            decide(true, dec!(190.476), dec!(260.227), TOL),
            C::RefuseUpward,
            "shares the strategy did not buy must not be credited to it",
        );
    }

    /// Downward on an engine row is a real sale or settlement.
    ///
    /// This must keep working: refusing it would leave sold shares on the books
    /// and the position would never close.
    #[test]
    fn an_engine_row_still_follows_a_sale_down() {
        assert_eq!(decide(true, dec!(100), dec!(40), TOL), C::SharesOnly);
        // A settlement reads as zero, and the corrector's own guards downstream
        // treat that as a settlement rather than a correction to nothing.
        assert_eq!(decide(true, dec!(100), dec!(0), TOL), C::SharesOnly);
    }

    /// Dust either way is not a correction.
    #[test]
    fn agreement_within_dust_is_left_alone() {
        assert_eq!(decide(true, dec!(10), dec!(10.0005), TOL), C::Leave);
        assert_eq!(decide(false, dec!(10), dec!(9.9995), TOL), C::Leave);
        assert_eq!(decide(true, dec!(10), dec!(10), TOL), C::Leave);
    }

    /// An engine row that grows by only dust is left alone, not refused.
    ///
    /// Otherwise every rounding difference on a live position would log a
    /// money-looking warning, and the real ones would be lost in it.
    #[test]
    fn dust_growth_does_not_trip_the_refusal() {
        assert_eq!(decide(true, dec!(190.476), dec!(190.4765), TOL), C::Leave);
    }
}
