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

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use regex::Regex;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::str::FromStr;
use alloy::primitives::{U256, Address};
use alloy::signers::local::LocalSigner;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use chrono::Utc;
use tracing::{debug, error, info, warn};

use polymarket_client_sdk_v2::clob::{Client as ClobClient};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::clob::types::request::{BalanceAllowanceRequest, OrdersRequest, PriceRequest};
use polymarket_client_sdk_v2::clob::types::{AssetType, Side};

use crate::helpers::metrics;
use crate::helpers::orders::place_limit_order;
use crate::helpers::orders::place_limit_order_filled;
use crate::venues::core::MarketId;
use crate::venues::intl::{market_id_from_u256, u256_from_market_id};

pub use crate::state::{Position, PositionKey, PositionMap};

/// Shared map of (strategy:token_id) → Instant for phantom removal cooldowns.
/// Canonical definition lives in `crate::state` (venue-neutral); re-exported here
/// for the intl balance/orphan handlers that historically referenced
/// `helpers::balance::PhantomCooldowns`.
pub use crate::state::{PhantomCooldowns, OrphanTombstones};

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
    // Squadron that owns the positions being reconciled. Part of every
    // PositionKey built below, so two squadrons trading the same token each
    // reconcile their own position rather than one another's.
    squadron_id: &str,
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    positions: &Arc<Mutex<PositionMap>>,
    strategy_name: &str,
    token_id: &MarketId,
    phantom_cooldowns: Option<&PhantomCooldowns>,
    baseline_shares: Decimal,
    max_wait_secs: i64,
    token_ownership: &Arc<Mutex<HashMap<MarketId, String>>>,
    post_only: bool,
) -> Result<bool> {
    // Slice 2b: resolve the on-chain U256 once; the rest of the body is unchanged.
    let token_id = u256_from_market_id(token_id)?;
    let market = market_id_from_u256(token_id);
    let key = PositionKey::new(squadron_id, strategy_name, market.clone());
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
                // The entry fee was booked against the shares we *asked* for, but
                // the venue charges it on what actually filled — and a marketable
                // FAK routinely over-fills (2026-08-13 trade 353: 16.04 requested,
                // 16.94 filled). The fee is linear in shares, so it rescales by the
                // same ratio; left unscaled it understated every round trip by ~5%.
                if expected > dec!(0) && pos.entry_fee > dec!(0) {
                    pos.entry_fee = pos.entry_fee * (actual_shares / expected);
                }
                pos.shares = actual_shares;
                if pos.fill_confirmed_at.is_none() { pos.fill_confirmed_at = Some(Utc::now()); }
                return Ok(true);
            } else if actual_shares == dec!(0) {
                if time_since_open >= max_wait_secs {
                    drop(pos_map);
                    // Cancel any resting GTC order to prevent future unexpected fills.
                    // A GTC order that rested and was later matched may briefly show
                    // zero balance AND zero resting orders while Polygon settles the fill.
                    // After cancelling, wait one grace period and re-check the balance
                    // before declaring phantom — this catches the settlement-lag race.
                    let (had_resting, _) = cancel_resting_orders(client, token_id).await;
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
                                return Ok(true);
                            }
                        }
                        // Genuinely-resting post-only maker quote that simply wasn't hit
                        // within the window is NOT a phantom — it's a normal unfilled
                        // quote we just cancelled for refresh.  Clear it quietly with no
                        // ERROR and no phantom cooldown so the maker can re-quote.
                        if post_only {
                            info!("💤 Maker quote expired unfilled [{}]: Token {} rested {}s without a fill — cancelled for refresh (no penalty)",
                                  strategy_name, token_id, time_since_open);
                            positions.lock().await.remove(&key);
                            token_ownership.lock().await.remove(&market);
                            return Ok(false);
                        }
                    }
                    error!("⚠️ Position Sync FAILED [{}] Token {} — phantom removed.", strategy_name, token_id);
                    positions.lock().await.remove(&key);
                    // Release token claim — position was phantom/never filled.
                    token_ownership.lock().await.remove(&market);
                    if let Some(cooldowns) = phantom_cooldowns {
                        cooldowns.lock().await.insert(format!("{}:{}", strategy_name, token_id), Instant::now());
                    }
                    return Ok(false);
                } else {
                    if time_since_open > 15 {
                        // A resting post-only maker quote sitting at 0 balance is the
                        // EXPECTED state (waiting for a taker), so keep it at debug.
                        // First warning fires at ~15s.  After that, throttle to once per
                        // 60 seconds so GTC orders resting on a slow daily market don't
                        // flood the log with hundreds of identical WARN lines.
                        if !post_only && (time_since_open <= 20 || time_since_open % 60 < 4) {
                            warn!("⚠️ Position Sync [{}]: Token {} balance is 0 ({}s since open). Retrying...", strategy_name, token_id, time_since_open);
                        } else {
                            debug!("⏳ Position Sync [{}]: Token {} balance is 0 ({}s since open). Retrying...", strategy_name, token_id, time_since_open);
                        }
                    }
                    drop(pos_map);
                    tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
                    continue;
                }
            } else { return Ok(false); }
        } else {
            // Position key not yet in the map (race between order submission and on-chain
            // confirmation).  Must explicitly drop the guard before sleeping — holding a
            // tokio::sync::MutexGuard across an .await is legal but keeps the lock live,
            // which means the subsequent `positions.lock().await` below would self-deadlock
            // (tokio Mutex is non-reentrant: same task trying to re-lock → hangs forever).
            drop(pos_map);
            tokio::time::sleep(Duration::from_millis(check_interval_ms)).await;
            if !positions.lock().await.contains_key(&key) { return Ok(false); }
        }
    }
}

/// Fetch all open orders for a token and cancel them.
/// Returns `(had_orders, size_matched_total)` — whether any orders were found
/// (and cancelled), and the total shares already matched on those orders at
/// cancel time. A resting GTC quote can partially fill in the seconds before a
/// quote-pull (2026-07-23 trade 280: 5 of 9.09 shares filled in the 16s window);
/// callers MUST inspect `size_matched_total` and adopt any filled shares instead
/// of discarding the position.
async fn cancel_resting_orders(client: &Arc<ClobClient<Authenticated<Normal>>>, token_id: U256) -> (bool, Decimal) {
    let req = OrdersRequest::builder().asset_id(token_id).build();
    let orders = match client.orders(&req, None).await {
        Ok(page) => page.data,
        Err(_) => return (false, dec!(0)),
    };
    if orders.is_empty() {
        return (false, dec!(0));
    }
    let matched: Decimal = orders.iter().map(|o| o.size_matched).sum();
    let order_ids: Vec<String> = orders.into_iter().map(|o| o.id).collect();
    let id_refs: Vec<&str> = order_ids.iter().map(|s| s.as_str()).collect();
    warn!(" Cancelling {} resting GTC order(s) for token {} — preventing orphan fills{}",
          id_refs.len(), token_id,
          if matched > dec!(0) { format!(" ({matched:.4} shares already matched)") } else { String::new() });
    let _ = client.cancel_orders(&id_refs).await;
    (true, matched)
}

/// One-shot on-chain conditional-token balance lookup (shares, 6-dp scaled).
/// Used by the Maker quote-pull to detect a quote that FULLY filled before the
/// pull: a fully-matched order vanishes from the CLOB open-orders list, so
/// `cancel_resting_orders` reports zero matched even though shares are held
/// (2026-07-24 trade 288: an 8-share YES fill was dropped as "unfilled", then
/// mis-adopted under MomentumStrategy at market switch and SL'd for −$0.40).
/// Returns 0 on lookup failure — callers treat that as "no shares detected".
pub async fn onchain_balance_for_token(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    token_id: &MarketId,
) -> Decimal {
    let token_u = match u256_from_market_id(token_id) { Ok(u) => u, Err(_) => return dec!(0) };
    let mut req = BalanceAllowanceRequest::default();
    req.asset_type = AssetType::Conditional;
    req.token_id = Some(token_u);
    match tokio::time::timeout(Duration::from_secs(10), client.balance_allowance(req)).await {
        Ok(Ok(resp)) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
        _ => dec!(0),
    }
}

/// Public reactive quote-pull entry point: cancel any resting order on `token_id`.
/// Used by the Maker's book-turn quote-pull (patrol `MakerCancel` handling) to pull
/// an UNFILLED resting quote before informed flow picks it off. Resolves the
/// on-chain U256 from the venue-neutral `MarketId`, then delegates to
/// [`cancel_resting_orders`]. Returns `(order_found, matched)`:
/// - `order_found` is true when at least one resting order was still on the book
///   (it was cancelled). False means the order VANISHED — either it fully filled
///   (shares held but possibly not yet visible on the balance endpoint) or it was
///   never resting; callers must NOT treat this as "unfilled" without re-checking
///   the on-chain balance with settlement-lag retries.
/// - `matched` is the total shares already MATCHED on the cancelled order(s) —
///   non-zero means a partial fill landed before the pull and the caller must
///   adopt those shares as a live position instead of dropping them.
pub async fn cancel_resting_orders_for_token(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    token_id: &MarketId,
) -> (bool, Decimal) {
    match u256_from_market_id(token_id) {
        Ok(u) => cancel_resting_orders(client, u).await,
        Err(_) => (false, dec!(0)),
    }
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
    // Squadron that owns the positions being reconciled. Part of every
    // PositionKey built below, so two squadrons trading the same token each
    // reconcile their own position rather than one another's.
    squadron_id: &str,
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    positions: &Arc<Mutex<PositionMap>>,
    tokens: &[(MarketId, &str)],
    market_name: &str,
    market_close_time: Option<chrono::DateTime<Utc>>,
    token_bids: &[(MarketId, Decimal)],
    adoption_order: &[String],
    orphan_tombstones: Option<&OrphanTombstones>,
) {
    for (token_id, side_label) in tokens {
        // Slice 2b: keys are neutral MarketId; resolve the on-chain U256 at the SDK edge.
        let market = token_id.clone();
        let token_u = match u256_from_market_id(token_id) { Ok(t) => t, Err(_) => continue };
        // ── Tombstone check ───────────────────────────────────────────────────
        // If this token has been through orphan-detection this session, skip it
        // permanently.  Without this, cleanup removes the orphan, phantom_cooldowns
        // are cleared on the next hourly market switch, and this reconcile re-adopts it —
        // restarting the infinite detect→remove→re-adopt cycle.
        if let Some(tb) = orphan_tombstones {
            if tb.lock().await.contains(&market) {
                debug!("⏭️  RECONCILE: skipping tombstoned token {} — previously orphan-removed this session", token_id);
                continue;
            }
        }

        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Conditional;
        req.token_id = Some(token_u);
        // Hard 10s timeout — the outer 'market_loop calls this function BEFORE the inner
        // select! loop starts, so the watchdog ticker cannot fire to break a stall here.
        // Without this guard a CLOB API hang at market-switch time causes a permanent
        // silent halt (root cause of the recurring ghost-mode production halt).
        let actual_shares = match tokio::time::timeout(
            Duration::from_secs(10),
            client.balance_allowance(req),
        ).await {
            Ok(Ok(resp)) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
            Ok(Err(e)) => {
                warn!("⚠️ RECONCILE: balance query failed for token {} ({}): {}", token_id, side_label, e);
                continue;
            }
            Err(_) => {
                warn!("⚠️ RECONCILE: balance query timed out (10s) for token {} ({}) — skipping adopt", token_id, side_label);
                continue;
            }
        };
        debug!(" RECONCILE: token {} ({}) on-chain balance = {:.4}", token_id, side_label, actual_shares);
        if actual_shares < crate::config::MIN_ORDER_SHARES {
            debug!("⏭️  RECONCILE: skipping token {} — balance {:.4} below threshold", token_id, actual_shares);
            continue;
        }

        {
            let map = positions.lock().await;
            if map.iter().any(|(k, _)| k.market == market && k.squadron == squadron_id) { continue; }
        }

        let current_bid = token_bids.iter().find(|(tid, _)| tid == token_id)
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
                warn!(" RECONCILE: Recovered entry_price={:.4} strategy={} for token {} from entry log",
                    real_entry, strat, token_id);
                (real_entry, Some(strat.clone()))
            }
            Some((real_entry, _)) => {
                warn!(" RECONCILE: Recovered entry_price={:.4} for token {} (no strategy in log)", real_entry, token_id);
                (real_entry, None)
            }
            None => {
                // No log found — credit an artificial entry below current bid so profit_margin
                // is immediately above every strategy's take-profit threshold on the next tick.
                (current_bid * (dec!(1) - crate::config::RECONCILE_ADOPTED_ENTRY_DISCOUNT), None)
            }
        };

        // Determine which strategy should own this position.
        // Priority: (1) strategy recorded in the entry log, (2) MakerStrategy when the
        // token belongs to the maker market (side_label carries "(maker)") — adopting a
        // maker fill under whichever strategy happens to be first in adoption_order
        // applies the wrong TP/SL (2026-07-30 trade 304: a maker fill adopted under
        // MomentumStrategy was stop-lossed the same second at −37%), (3) first available
        // strategy in adoption_order.
        let adopted_strategy = {
            let mut candidates: Vec<String> = Vec::new();
            if let Some(ref logged) = logged_strategy {
                candidates.push(logged.clone());
            }
            if side_label.contains("maker") {
                candidates.push("MakerStrategy".to_string());
            }
            candidates.extend(adoption_order.iter().cloned());
            let map = positions.lock().await;
            candidates.into_iter().find(|s| !map.contains_key(&PositionKey::new(squadron_id, s.clone(), market.clone())))
        };

        if let Some(strategy_name) = adopted_strategy {
            let mut pos_map = positions.lock().await;

            pos_map.insert(PositionKey::new(squadron_id, strategy_name.clone(), market.clone()), Position {
                shares: actual_shares,
                avg_entry,
                opened_at: Utc::now() - chrono::Duration::seconds(crate::config::MIN_HOLD_SECS_BEFORE_STOP_LOSS),
                close_time: market_close_time,
                market_name: market_name.to_string(),
                pair_token_id: market.clone(),
                fill_confirmed_at: Some(Utc::now()),
                paired_leg_token_id: None, entry_fee: Decimal::ZERO, // fixed up below
            });

            let source = if logged_strategy.is_some() { "DB" } else {
                &format!("discount@{:.0}%", crate::config::RECONCILE_ADOPTED_ENTRY_DISCOUNT * dec!(100))
            };
            warn!(" RECONCILE: Adopted {} {} shares for token {} under [{}] — avg_entry={:.4} (source={}, current_bid={:.4})",
                actual_shares, side_label, token_id, strategy_name, avg_entry, source, current_bid);
        }
    }

    // ── Wire paired_leg_token_id so the cleanup orphan-detector works ─────────
    // Each position is inserted above with paired_leg_token_id: None, entry_fee: Decimal::ZERO, which means
    // the cleanup.rs orphan reconciler (which checks `if let Some(paired) = ...`)
    // would never fire for session-adopted positions.
    //
    // Post-pass: if exactly 2 tokens were provided (YES + NO pair), point each
    // adopted position at the other token.  If only one leg has an on-chain
    // balance (naked orphan), the cleanup cycle will detect the missing pair on
    // its next run and remove the position while preventing phantom re-entry.
    //
    // ⚠️ PAIRED-STRATEGY GUARD: only genuinely two-leg strategies may receive a
    // `paired_leg_token_id`. Single-leg directional strategies (TrendCapture,
    // Momentum, Gboost, Maker, Basis) hold ONE token by design and have no hedge
    // partner. Stamping a phantom partner on them makes the lifecycle naked-leg
    // detector (`venues/lifecycle.rs`) flatten a perfectly healthy position at the
    // $0.01 floor — exactly the Jun 19 trade id 50 (−$2.94) false flatten: a
    // TrendCapture YES leg was paired to the NO token it never held, then dumped.
    // Only ArbitrageStrategy / TimeDecayStrategy emit `pair_params: Some`.
    if tokens.len() == 2 {
        // Mirrors the strategies that enter with `pair_params: Some(..)`.
        let is_paired_strategy = |s: &str| {
            matches!(s, "ArbitrageStrategy" | "TimeDecayStrategy")
        };

        let market_a = tokens[0].0.clone();
        let market_b = tokens[1].0.clone();
        let mut pos_map = positions.lock().await;

        // Find which strategy adopted each token (may be None if balance was 0)
        let strat_a = pos_map.iter()
            .find(|(k, _)| k.market == market_a && k.squadron == squadron_id)
            .map(|(k, _)| k.strategy.clone());
        let strat_b = pos_map.iter()
            .find(|(k, _)| k.market == market_b && k.squadron == squadron_id)
            .map(|(k, _)| k.strategy.clone());

        if let Some(sa) = strat_a {
            if is_paired_strategy(&sa) {
                if let Some(p) = pos_map.get_mut(&PositionKey::new(squadron_id, sa.clone(), market_a.clone())) {
                    p.paired_leg_token_id = Some(market_b.clone());
                }
            } else {
                debug!(" RECONCILE: skipping phantom pairing for single-leg strategy [{}] on token {}", sa, market_a);
            }
        }
        if let Some(sb) = strat_b {
            if is_paired_strategy(&sb) {
                if let Some(p) = pos_map.get_mut(&PositionKey::new(squadron_id, sb.clone(), market_b.clone())) {
                    p.paired_leg_token_id = Some(market_a.clone());
                }
            } else {
                debug!(" RECONCILE: skipping phantom pairing for single-leg strategy [{}] on token {}", sb, market_b);
            }
        }
    }
}

/// Orphan-leg cleanup monitor spawned alongside every atomic two-leg arb entry.
///
/// Enforces the invariant:
///   If exactly one leg confirmed and the other has not filled, repair the pair:
///     1. POST /cancel — atomically cancels the remaining open GTC order on the missing leg.
///     2. FAK (Immediate-or-Cancel) taker buy on the missing leg at current ask + one tick,
///        capped at the dynamic breakeven ceiling, to close the delta exposure; or, if
///        re-hedge is uneconomical, FAK-flatten the filled leg at the bid.
///
/// ── Event-driven repair (first-leg-confirmation trigger) ─────────────────────
/// Rather than sleeping out the entire `max_wait_secs` window (up to 600s on window
/// markets) and checking once, the monitor POLLS the joint fill state every
/// `POLL_INTERVAL_SECS`. The moment exactly one leg is confirmed it starts a short
/// `FIRST_LEG_CONFIRM_GRACE_SECS` countdown — just enough time for the missing leg to
/// still fill naturally as a free maker — and repairs as soon as that grace elapses.
/// This bounds naked directional exposure to ~grace seconds after the first fill
/// instead of the full fill window (the dominant orphan-loss window). The original
/// `max_wait_secs + ARBITER_GRACE_SECS` is retained only as a hard deadline for the
/// neither-filled case, where the individual `sync_position_balance` tasks own cleanup.
pub async fn arb_pair_fill_monitor(
    // Squadron that owns the positions being reconciled. Part of every
    // PositionKey built below, so two squadrons trading the same token each
    // reconcile their own position rather than one another's.
    squadron_id: &str,
    client: Arc<ClobClient<Authenticated<Normal>>>,
    nonce_manager: Arc<AtomicU64>,
    signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    safe_address: Address,
    eoa_address: Address,
    vc_a: Address,
    vc_b: Address,
    positions: Arc<Mutex<PositionMap>>,
    phantom_cooldowns: PhantomCooldowns,
    token_ownership: Arc<Mutex<HashMap<MarketId, String>>>,
    strategy_name: String,
    leg_a_token: &MarketId,
    leg_b_token: &MarketId,
    leg_a_baseline: Decimal,
    leg_b_baseline: Decimal,
    leg_a_side_label: String,
    leg_b_side_label: String,
    max_wait_secs: i64,
    http: Arc<reqwest::Client>,
    asset: String,
    // Session P&L counter. Orphan flattens are real realised losses and must
    // land here as well as in the `trades` table, or the dashboard's session
    // figure drifts from the ledger.
    total_pnl: Arc<Mutex<Decimal>>,
) {
    /// Hard-deadline grace added on top of the fill window. Only governs the
    /// neither-leg-filled case now: once both legs have had their full chance to
    /// fill, the individual `sync_position_balance` tasks own phantom cleanup.
    const ARBITER_GRACE_SECS: u64 = 15;
    /// Once exactly one leg confirms, the missing leg gets only this short grace to
    /// fill naturally as a free (0-fee) maker before we step in with a taker
    /// re-hedge/flatten. Reduced from 30s → 5s: in crypto up/down markets the ask
    /// can move out of the rescue-profit ceiling within seconds of the first fill,
    /// turning a viable re-hedge into a forced flatten loss. Act quickly.
    const FIRST_LEG_CONFIRM_GRACE_SECS: u64 = 5;
    /// Cadence at which the joint fill state is polled while both legs are still
    /// in play — nothing is at risk yet, so a slow poll is free.
    const POLL_INTERVAL_SECS: u64 = 5;
    /// Cadence once exactly one leg has confirmed. From that instant a naked
    /// directional leg exists and every second of poll granularity is added
    /// straight onto the repair latency, so tighten right down: at the 5s
    /// cadence the 5s asymmetric grace could overshoot to 10s before the repair
    /// even started.
    const ARBITER_ASYMMETRIC_POLL_SECS: u64 = 1;
    /// After the filled leg is flattened, the missing leg's cancelled GTC can still be
    /// taker-matched in a late race (observed 2026-07-03: NO flattened at 11:10:33, YES
    /// GTC filled 11:10:38 → invisible naked YES). Keep watching the missing leg for this
    /// bounded window and flatten+book it too if it fills late, instead of leaving it to
    /// ride until the 30s lifecycle sweep (or, on a restart, unbooked entirely).
    const ARBITER_LATE_FILL_WATCH_SECS: u64 = 20;

    // Slice 2b: resolve on-chain U256 once; the rest of the body is unchanged.
    let leg_a_token = u256_from_market_id(leg_a_token).unwrap_or_default();
    let leg_b_token = u256_from_market_id(leg_b_token).unwrap_or_default();

    let key_a = PositionKey::new(squadron_id, strategy_name.clone(), market_id_from_u256(leg_a_token));
    let key_b = PositionKey::new(squadron_id, strategy_name.clone(), market_id_from_u256(leg_b_token));

    // Hard deadline for the neither-filled case; also a backstop upper bound for the
    // asymmetric case so we never wait past the original window.
    let deadline = Instant::now()
        + Duration::from_secs((max_wait_secs as u64).saturating_add(ARBITER_GRACE_SECS));
    // When we first observe exactly one confirmed leg, start the short repair countdown.
    let mut first_asymmetric_at: Option<Instant> = None;

    // Event-driven poll: exit with the asymmetric fill state to repair, or `return`
    // early on both-filled / deadline-reached-with-neither-filled.
    let (a_confirmed, a_shares, b_shares) = loop {
        // Adaptive: slow while both legs are still candidates, fast the moment
        // one has confirmed and the other has not.
        let poll = if first_asymmetric_at.is_some() {
            ARBITER_ASYMMETRIC_POLL_SECS
        } else {
            POLL_INTERVAL_SECS
        };
        tokio::time::sleep(Duration::from_secs(poll)).await;

        // Snapshot fill-confirmation state for both legs.
        let (a_conf, a_sh) = {
            let map = positions.lock().await;
            map.get(&key_a)
                .map(|p| (p.fill_confirmed_at.is_some(), p.shares))
                .unwrap_or((false, dec!(0)))
        };
        let (b_conf, b_sh) = {
            let map = positions.lock().await;
            map.get(&key_b)
                .map(|p| (p.fill_confirmed_at.is_some(), p.shares))
                .unwrap_or((false, dec!(0)))
        };

        match (a_conf, b_conf) {
            (true, true) => {
                // Symmetric fill — both legs confirmed. Nothing to do.
                debug!("✅ ARB ARBITER [{}]: Both legs confirmed ({} / {}) — no orphan", strategy_name, leg_a_token, leg_b_token);
                return;
            }
            (false, false) => {
                // Neither leg filled yet — reset any stale asymmetry timer and wait
                // until the hard deadline, then hand off to the sync tasks.
                first_asymmetric_at = None;
                if Instant::now() >= deadline {
                    debug!("⏭️  ARB ARBITER [{}]: Neither leg confirmed by deadline — sync tasks own cleanup", strategy_name);
                    return;
                }
            }
            _ => {
                // Asymmetric — exactly one leg filled. Give the missing leg a short
                // grace to fill as a free maker; repair the moment it elapses (or at
                // the hard deadline, whichever comes first).
                let since = *first_asymmetric_at.get_or_insert_with(Instant::now);
                if since.elapsed() >= Duration::from_secs(FIRST_LEG_CONFIRM_GRACE_SECS)
                    || Instant::now() >= deadline
                {
                    break (a_conf, a_sh, b_sh);
                }
            }
        }
    };

    // ── Asymmetric fill: one leg confirmed, the other is missing ──────────────
    let (filled_token, missing_token, missing_baseline, missing_vc, missing_side, filled_vc, filled_side) =
        if a_confirmed {
            warn!("⚡ ARB ARBITER [{}]: Leg A ({}) filled ({} shares) but Leg B ({}) is MISSING — initiating orphan-leg cleanup",
                  strategy_name, leg_a_token, a_shares, leg_b_token);
            (leg_a_token, leg_b_token, leg_b_baseline, vc_b, leg_b_side_label.as_str(), vc_a, leg_a_side_label.as_str())
        } else {
            warn!("⚡ ARB ARBITER [{}]: Leg B ({}) filled ({} shares) but Leg A ({}) is MISSING — initiating orphan-leg cleanup",
                  strategy_name, leg_b_token, b_shares, leg_a_token);
            (leg_b_token, leg_a_token, leg_a_baseline, vc_a, leg_a_side_label.as_str(), vc_b, leg_b_side_label.as_str())
        };

    // Slice 2a: neutral keys for map ops; raw U256 retained for SDK/chain calls.
    let filled_market  = market_id_from_u256(filled_token);
    let missing_market = market_id_from_u256(missing_token);

    // Step 1: Cancel any remaining GTC on the missing leg (idempotent if already cancelled
    //         by the individual sync task — the CLOB rejects cancels of non-existent orders
    //         gracefully).
    let _ = cancel_resting_orders(&client, missing_token).await;

    // Step 2: Short settlement grace — the cancel may have raced against a taker fill
    //         that was mid-settlement on-chain. Live knob (was a hardcoded 10s, the
    //         single largest discretionary slice of the 26s entry→flatten latency
    //         measured on 2026-08-15): every second here is a second of naked
    //         directional exposure, and the post-flatten late-fill watcher bounds
    //         the cost of cutting it too fine.
    let settle_grace = crate::helpers::dynamic_config::global_config_tx()
        .map(|tx| tx.borrow().arb_settle_grace_secs)
        .unwrap_or(crate::config::ARB_SETTLE_GRACE_SECS);
    tokio::time::sleep(Duration::from_secs(settle_grace)).await;

    // Step 3: Re-check the missing leg's on-chain balance (settle-lag race guard).
    let settled_shares = {
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Conditional;
        req.token_id = Some(missing_token);
        match client.balance_allowance(req).await {
            Ok(resp) => {
                let raw = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
                (raw - missing_baseline).max(dec!(0))
            }
            Err(e) => {
                warn!("⚠️ ARB ARBITER [{}]: Balance re-check error for missing leg {}: {}", strategy_name, missing_token, e);
                dec!(0)
            }
        }
    };

    if settled_shares >= crate::config::MIN_ORDER_SHARES {
        // The missing leg actually filled during the cancel settle-lag window — recover it.
        warn!("✅ ARB ARBITER [{}]: Missing leg {} RECOVERED post-cancel settle ({} shares)",
              strategy_name, missing_token, settled_shares);
        let key_m = PositionKey::new(squadron_id, strategy_name.clone(), missing_market.clone());
        let mut map = positions.lock().await;
        if let Some(pos) = map.get_mut(&key_m) {
            pos.shares = settled_shares;
            if pos.fill_confirmed_at.is_none() { pos.fill_confirmed_at = Some(Utc::now()); }
        } else {
            // Sync task already phantom-removed it; re-insert with data from the filled leg.
            let reference = map.get(&PositionKey::new(squadron_id, strategy_name.clone(), filled_market.clone())).cloned();
            if let Some(ref_pos) = reference {
                map.insert(key_m, Position {
                    shares: settled_shares,
                    avg_entry: ref_pos.avg_entry,
                    opened_at: Utc::now(),
                    close_time: ref_pos.close_time,
                    market_name: ref_pos.market_name.clone(),
                    pair_token_id: missing_market.clone(),
                    fill_confirmed_at: Some(Utc::now()),
                    paired_leg_token_id: Some(filled_market.clone()), entry_fee: Decimal::ZERO,
                });
            }
        }
        // Drop the positions guard BEFORE acquiring phantom_cooldowns to prevent an ABBA
        // deadlock: any task that holds phantom_cooldowns and then tries to acquire positions
        // would cross-deadlock if we held positions here across the .await.
        drop(map);
        phantom_cooldowns.lock().await
            .remove(&format!("{}:{}", strategy_name, missing_token));
        return;
    }

    // Step 4: Query the CLOB for the current best ASK on the missing leg (the price
    // we must pay to BUY it). NOTE: /price side semantics are "best resting order on
    // that side" — side=Sell → best ask, side=Buy → best bid (2026-07-24 fix: this
    // used Side::Buy and fetched the BID, understating the re-hedge cost).
    let ask_price = {
        let req = PriceRequest::builder()
            .token_id(missing_token)
            .side(Side::Sell)
            .build();
        match client.price(&req).await {
            Ok(resp) => {
                info!(" ARB ARBITER [{}]: Current ask for missing leg {} = {:.4}", strategy_name, missing_token, resp.price);
                resp.price
            }
            Err(e) => {
                warn!("⚠️ ARB ARBITER [{}]: Could not fetch ask for {}: {} — using $0.99 ceiling",
                      strategy_name, missing_token, e);
                dec!(0.99)
            }
        }
    };

    // Match the filled leg's confirmed share count AND entry price so we can compute a
    // breakeven ceiling — without this we'd cap at a fixed $0.99 and could lock in a loss.
    let (filled_shares_recorded, filled_avg_entry, filled_entry_fee) = positions.lock().await
        .get(&PositionKey::new(squadron_id, strategy_name.clone(), filled_market.clone()))
        .map(|p| (p.shares, p.avg_entry, p.entry_fee))
        .unwrap_or((dec!(0), dec!(0), dec!(0)));
    // PARTIAL-FILL GUARD (2026-07-26 trade 296): fill-confirm stamps a leg "filled"
    // on ANY matched size, so the recorded count can be the full order size while
    // only a sliver settled on-chain (16.48 recorded vs 1.47 held). Sizing the
    // flatten/re-hedge off the recorded count draws a "not enough balance" 400 and
    // the naked sliver rides to $0. Always trust the on-chain balance when it's
    // smaller than the recorded count.
    let filled_onchain = onchain_balance_for_token(&client, &filled_market).await;
    let filled_shares = if filled_onchain > dec!(0) && filled_onchain < filled_shares_recorded {
        warn!("⚖️ ARB ARBITER [{}]: filled leg {} partial fill — recorded {} but on-chain {} — sizing repair from on-chain",
              strategy_name, filled_token, filled_shares_recorded, filled_onchain);
        filled_onchain
    } else {
        filled_shares_recorded
    };
    let fak_qty = if filled_shares >= crate::config::MIN_ORDER_SHARES {
        filled_shares
    } else {
        crate::config::MIN_ORDER_SHARES
    };

    // Dynamic breakeven ceiling: the most we can pay for the missing leg without losing
    // money on the combined position.  For a binary outcome paying exactly $1.00:
    //   max_missing_price = $1.00 − filled_avg_entry − ARB_FAK_REHEDGE_BUFFER
    // The buffer covers the taker-exit fee plus a small adverse-price cushion.
    // If filled_avg_entry is unknown (0), fall back to the conservative hard $0.49 cap
    // so we never automatically pay more than half the payout.
    let dynamic_ceiling = if filled_avg_entry > dec!(0) {
        (dec!(1.00) - filled_avg_entry - crate::config::ARB_FAK_REHEDGE_BUFFER)
            .max(dec!(0.01)) // always allow at least a penny attempt
    } else {
        dec!(0.49) // conservative fallback when entry is unknown
    };

    // Re-hedge is only viable if we can cross the spread (ask + 1 tick) at or below
    // the breakeven ceiling. A FAK priced BELOW the ask cannot fill, so attempting it
    // would either no-op (and falsely record a phantom hedge) or fail — in both cases
    // the naked leg used to be abandoned and ride to a $0 settlement (the −$5 SOL
    // losses). Instead: only re-hedge when economical; otherwise flatten immediately.
    let rehedge_price = ask_price + dec!(0.01);
    let rehedge_viable = rehedge_price <= dynamic_ceiling;

    if rehedge_viable {
        warn!(" ARB ARBITER [{}]: Placing FAK taker buy re-hedge — token={} qty={} limit={:.4} (ask={:.4}, breakeven_ceil={:.4}, filled_entry={:.4})",
              strategy_name, missing_token, fak_qty, rehedge_price, ask_price, dynamic_ceiling, filled_avg_entry);

        match place_limit_order(
            &client, &nonce_manager, &signer,
            safe_address, eoa_address, missing_vc,
            &missing_market, Side::Buy, fak_qty, rehedge_price,
            0, crate::venues::core::TimeInForce::Fak, false, 0, &*http,
        ).await {
            Ok(order_id) => {
                warn!("✅ ARB ARBITER [{}]: FAK re-hedge placed (order_id={}) — delta exposure closed",
                      strategy_name, order_id);

                // Re-insert the missing leg into the positions map (the sync task already
                // phantom-removed it; we restore it with the FAK fill data).
                let reference = positions.lock().await
                    .get(&PositionKey::new(squadron_id, strategy_name.clone(), filled_market.clone()))
                    .cloned();
                if let Some(ref_pos) = reference {
                    let key_m = PositionKey::new(squadron_id, strategy_name.clone(), missing_market.clone());
                    let mut map = positions.lock().await;
                    if !map.contains_key(&key_m) {
                        map.insert(key_m.clone(), Position {
                            shares: fak_qty,
                            avg_entry: rehedge_price,
                            opened_at: Utc::now(),
                            close_time: ref_pos.close_time,
                            market_name: ref_pos.market_name.clone(),
                            pair_token_id: missing_market.clone(),
                            fill_confirmed_at: Some(Utc::now()),
                            paired_leg_token_id: Some(filled_market.clone()), entry_fee: Decimal::ZERO,
                        });
                    }
                    drop(map);

                    // Clear phantom cooldown so a future cycle can enter this market again.
                    phantom_cooldowns.lock().await
                        .remove(&format!("{}:{}", strategy_name, missing_token));

                    // Write the re-hedged fill to the DB.
                    if let Some(pool) = crate::helpers::db::pool_for(&asset) {
                        crate::helpers::db::record_open_position(&pool, squadron_id,
                            &strategy_name,
                            &missing_token.to_string(),
                            &ref_pos.market_name,
                            missing_side,
                            rehedge_price,
                            fak_qty,
                            false,
                        ).await;
                    }
                }
                // Re-hedge complete — the pair is whole again. Do NOT fall through to flatten.
                return;
            }
            Err(e) => {
                warn!("❌ ARB ARBITER [{}]: FAK re-hedge FAILED for missing leg {}: {} — falling back to guaranteed bid-flatten",
                      strategy_name, missing_token, e);
                // fall through to the bid-flatten safety net below
            }
        }
    } else {
        warn!(" ARB ARBITER [{}]: Re-hedge uneconomical (would need {:.4} > breakeven {:.4}) — flattening naked leg at bid to cap loss",
              strategy_name, rehedge_price, dynamic_ceiling);
    }

    // ── Purge the abandoned missing leg ─────────────────────────────────────────
    // Reaching here means we've committed to flattening the FILLED leg instead of
    // completing the pair (re-hedge was uneconomical or its FAK failed), so the
    // missing leg will never be held. It never filled on-chain (balance 0 — rechecked
    // above), which is exactly why the 5-min orphan backstop can't clean it: that
    // backstop keys off *on-chain* holdings, and a zero-balance leg is invisible to
    // it. Left alone, the missing leg's entry row lingers forever as a phantom
    // "pending" open_position (fake notional on the dashboard, a stale token-ownership
    // claim that blocks re-entry, polluted P&L). Drop it now from the DB, the
    // in-memory map, and the ownership set. Idempotent: the sync task may have already
    // phantom-removed the in-memory entry, and deleting an absent DB row is a no-op.
    // Read the missing leg's entry fee BEFORE the purge below deletes the row —
    // `close_open_position` DELETEs and the `entries` ledger has no fee column,
    // so this is the last moment it can be recovered. Only the late-fill branch
    // consumes it, and only if that leg fills after all; zero is the right
    // reading for the ordinary case of a maker GTC that never filled.
    let missing_entry_fee = match crate::helpers::db::pool_for(&asset) {
        Some(p) => crate::helpers::db::get_open_position_entry_fee(&p, &missing_token.to_string())
            .await
            .unwrap_or(dec!(0)),
        None => dec!(0),
    };

    positions.lock().await.remove(&PositionKey::new(squadron_id, strategy_name.clone(), missing_market.clone()));
    token_ownership.lock().await.remove(&missing_market);
    if let Some(pool) = crate::helpers::db::pool_for(&asset) {
        crate::helpers::db::close_open_position(&pool, &strategy_name, &missing_token.to_string()).await;
    }

    // ── Guaranteed bid-flatten: a naked leg must NEVER ride to settlement ────────
    // When re-hedge is impossible/uneconomical we immediately FAK-SELL the filled leg
    // at the live bid. Worst case is the spread (~1–3¢/share) instead of a full leg
    // resolving to $0. This is the hard guarantee: exposure is always closed now.
    let market_name = positions.lock().await
        .get(&PositionKey::new(squadron_id, strategy_name.clone(), filled_market.clone()))
        .map(|p| p.market_name.clone())
        .unwrap_or_default();

    // /price side semantics: side=Buy → best bid (what a seller can hit).
    let bid_price = {
        let req = PriceRequest::builder()
            .token_id(filled_token)
            .side(Side::Buy)
            .build();
        match client.price(&req).await {
            Ok(resp) => resp.price,
            Err(e) => {
                warn!("⚠️ ARB ARBITER [{}]: Could not fetch bid for filled leg {}: {} — using $0.01 floor",
                      strategy_name, filled_token, e);
                dec!(0.01)
            }
        }
    };
    // Cross by the LARGER of an absolute buffer and a percentage haircut so the FAK
    // limit reliably sweeps the real top-of-book even when the quoted bid is stale-high.
    // FAK ⇒ the executed price is still the best resting bid; the lower limit only
    // guarantees the fill. Floor at MIN_SELL_LIMIT_PRICE and snap to the tick grid.
    let cross = std::cmp::max(
        crate::config::ARB_FLATTEN_MIN_BID_BUFFER,
        bid_price * crate::config::ARB_FLATTEN_BID_HAIRCUT_PCT,
    );
    let sell_price = crate::helpers::price::floor_to_tick_size(
        (bid_price - cross).max(crate::config::MIN_SELL_LIMIT_PRICE)
    );

    warn!(" ARB ARBITER [{}]: Flattening naked leg {} — FAK SELL {} @ {:.4} (bid={:.4}, entry={:.4})",
          strategy_name, filled_token, filled_shares, sell_price, bid_price, filled_avg_entry);

    match place_limit_order_filled(
        &client, &nonce_manager, &signer,
        safe_address, eoa_address, filled_vc,
        &filled_market, Side::Sell, filled_shares, sell_price,
        0, crate::venues::core::TimeInForce::Fak, false, 0, &*http,
    ).await {
        Ok((order_id, making_amount, taking_amount)) => {
            // Book the ACTUAL average fill price from the CLOB match, not the
            // intended `sell_price` limit. For a SELL the matched proceeds/size
            // give the real price (ratio is unit-invariant); clamp to a valid
            // (0,1] binary price and fall back to the limit if the response
            // orientation is unexpected or nothing matched (partial/again later).
            let exit_price = if making_amount > dec!(0) && taking_amount > dec!(0) {
                let p = taking_amount / making_amount;
                if p > dec!(0) && p <= dec!(1) { p } else { sell_price }
            } else {
                sell_price
            };
            // Net of BOTH legs' fees, mirroring the normal exit path in
            // patrol_impl. This flatten is an FAK — always a taker fill, always
            // charged — while the entry fee is whatever the venue took to open
            // (zero for the resting GTC these strategies usually enter on).
            // Prorated when a partial fill made us size the repair off-chain, so
            // the fee follows the shares actually being closed.
            let gross = (exit_price - filled_avg_entry) * filled_shares;
            let entry_fee_share = if filled_shares_recorded > dec!(0) {
                filled_entry_fee * (filled_shares / filled_shares_recorded)
            } else {
                dec!(0)
            };
            let fees = entry_fee_share
                + crate::venues::intl::taker_fee(
                    crate::venues::intl::live_taker_fee_rate(), exit_price, filled_shares,
                );
            let realized = gross - fees;
            warn!("✅ ARB ARBITER [{}]: Naked leg flattened (order_id={}) — exposure closed @ {:.4} (limit {:.4}), realized ${:.4} (gross ${:.4} − fees ${:.4})",
                  strategy_name, order_id, exit_price, sell_price, realized, gross, fees);
            // Credit session P&L here too. This path recorded to `trades` but
            // never touched the running counter, so the dashboard's session
            // number silently diverged from the ledger by the full amount of
            // every orphan flatten (12 of them, −$2.25, since July).
            *total_pnl.lock().await += realized;
            // The leg is closed — drop it from tracking so it isn't counted as open.
            positions.lock().await.remove(&PositionKey::new(squadron_id, strategy_name.clone(), filled_market.clone()));
            // The filled leg has been flattened — release its token claim so other
            // strategies aren't blocked on a position we no longer hold.
            token_ownership.lock().await.remove(&filled_market);
            // Purge the filled leg's DB row too, so it can't linger as a phantom if the
            // on-chain-keyed backstop races or misses it (defense in depth).
            if let Some(pool) = crate::helpers::db::pool_for(&asset) {
                crate::helpers::db::close_open_position(&pool, &strategy_name, &filled_token.to_string()).await;
            }
            // Record the realized result so the dashboard reflects the true (small) loss.
            crate::helpers::metrics::record_trade(
                &crate::state::TradeScope::crypto(
                    &asset, crate::venues::intl::INTL_VENUE, &asset),
                fees,
                strategy_name.clone(),
                market_name.clone(),
                filled_side.to_string(),
                filled_avg_entry,
                exit_price,
                filled_shares,
                realized,
                "Orphan flatten (bid exit)".to_string(),
            ).await;
        }
        Err(e) => {
            warn!("❌ ARB ARBITER [{}]: Bid-flatten FAILED for naked leg {}: {} — the 5-min cleanup backstop will retry",
                  strategy_name, filled_token, e);
        }
    }

    // ── (A) Post-flatten late-fill guard ────────────────────────────────────────
    // Step 1 cancelled the missing leg's GTC, but a maker GTC can still be taker-matched
    // in a race AFTER we've already flattened the filled leg — leaving a naked directional
    // leg that either rides to a $0 settlement or (on a container restart mid-cleanup)
    // gets disposed with no trade record at all. This is the exact 2026-07-03 failure: the
    // NO leg was flattened at 11:10:33, then the YES GTC filled at 11:10:38, and the naked
    // YES was closed without ever being booked. Watch the missing leg for a bounded window;
    // if it fills late, flatten and BOOK it synchronously so exposure is closed now and the
    // dashboard reflects the true result.
    let late_watch_deadline = Instant::now() + Duration::from_secs(ARBITER_LATE_FILL_WATCH_SECS);
    loop {
        tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
        let late_shares = {
            let mut req = BalanceAllowanceRequest::default();
            req.asset_type = AssetType::Conditional;
            req.token_id = Some(missing_token);
            match client.balance_allowance(req).await {
                Ok(resp) => {
                    let raw = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
                    (raw - missing_baseline).max(dec!(0))
                }
                Err(_) => dec!(0),
            }
        };
        if late_shares >= crate::config::MIN_ORDER_SHARES {
            warn!("⚡ ARB ARBITER [{}]: Missing leg {} FILLED LATE post-flatten ({} shares) — flattening newly-naked leg to close exposure",
                  strategy_name, missing_token, late_shares);

            // Entry price/side from the entries ledger (record_open_position wrote it on fill).
            let late_entry = match crate::helpers::db::pool_for(&asset) {
                Some(p) => crate::helpers::db::lookup_entry_price_db(&p, &missing_token.to_string()).await.unwrap_or(dec!(0)),
                None => dec!(0),
            };
            let late_bid = {
                // side=Buy → best bid (see /price side semantics note above)
                let req = PriceRequest::builder().token_id(missing_token).side(Side::Buy).build();
                match client.price(&req).await { Ok(r) => r.price, Err(_) => dec!(0.01) }
            };
            let late_sell = (late_bid - dec!(0.01)).max(dec!(0.01));

            match place_limit_order_filled(
                &client, &nonce_manager, &signer,
                safe_address, eoa_address, missing_vc,
                &missing_market, Side::Sell, late_shares, late_sell,
                0, crate::venues::core::TimeInForce::Fak, false, 0, &*http,
            ).await {
                Ok((order_id, making_amount, taking_amount)) => {
                    let exit_price = if making_amount > dec!(0) && taking_amount > dec!(0) {
                        let p = taking_amount / making_amount;
                        if p > dec!(0) && p <= dec!(1) { p } else { late_sell }
                    } else {
                        late_sell
                    };
                    // Same round-trip netting as the primary flatten above; the
                    // entry fee was captured before this leg's row was purged.
                    let gross = (exit_price - late_entry) * late_shares;
                    let fees = missing_entry_fee
                        + crate::venues::intl::taker_fee(
                            crate::venues::intl::live_taker_fee_rate(), exit_price, late_shares,
                        );
                    let realized = gross - fees;
                    warn!("✅ ARB ARBITER [{}]: Late-fill naked leg flattened (order_id={}) — exposure closed @ {:.4} (limit {:.4}), realized ${:.4} (gross ${:.4} − fees ${:.4})",
                          strategy_name, order_id, exit_price, late_sell, realized, gross, fees);
                    *total_pnl.lock().await += realized;
                    positions.lock().await.remove(&PositionKey::new(squadron_id, strategy_name.clone(), missing_market.clone()));
                    token_ownership.lock().await.remove(&missing_market);
                    // (B) Book synchronously (awaited, not fire-and-forget) so a restart
                    // mid-cleanup cannot drop the record — the July-3 invisibility mode.
                    crate::helpers::metrics::record_trade(
                        &crate::state::TradeScope::crypto(
                            &asset, crate::venues::intl::INTL_VENUE, &asset),
                        fees,
                        strategy_name.clone(),
                        market_name.clone(),
                        missing_side.to_string(),
                        late_entry,
                        exit_price,
                        late_shares,
                        realized,
                        "Orphan flatten (late-fill race)".to_string(),
                    ).await;
                    let mut cd = phantom_cooldowns.lock().await;
                    cd.insert(format!("{}:{}", strategy_name, leg_a_token), tokio::time::Instant::now());
                    cd.insert(format!("{}:{}", strategy_name, leg_b_token), tokio::time::Instant::now());
                    return;
                }
                Err(e) => {
                    warn!("❌ ARB ARBITER [{}]: Late-fill flatten FAILED for {}: {} — leaving position for the 30s lifecycle sweep to retry",
                          strategy_name, missing_token, e);
                    // Leave the position/phantom in place so lifecycle reconcile catches it.
                    return;
                }
            }
        }
        if Instant::now() >= late_watch_deadline { break; }
    }

    // The missing leg never filled and its GTC was cancelled in Step 1 — proactively
    // drop its phantom position and release its token claim now instead of waiting for
    // `sync_position_balance` to age it out at `max_wait_secs`. This stops the redundant
    // Position-Sync retry loop on the next poll and frees the token immediately so
    // higher-priority strategies aren't rejected by TOKEN SOVEREIGNTY for ~minutes.
    positions.lock().await.remove(&PositionKey::new(squadron_id, strategy_name.clone(), missing_market.clone()));
    token_ownership.lock().await.remove(&missing_market);

    // Block re-entry on BOTH legs until the operator/cleanup confirms the state is clean.
    let mut cd = phantom_cooldowns.lock().await;
    cd.insert(format!("{}:{}", strategy_name, leg_a_token), tokio::time::Instant::now());
    cd.insert(format!("{}:{}", strategy_name, leg_b_token), tokio::time::Instant::now());
}

pub async fn quick_confirm_fill(
    // Squadron that owns the positions being reconciled. Part of every
    // PositionKey built below, so two squadrons trading the same token each
    // reconcile their own position rather than one another's.
    squadron_id: &str,
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    strategy_name: &str,
    token_id: &MarketId,
    positions: &Arc<Mutex<PositionMap>>,
    _condition_id: &str, // retained for API compatibility; no longer used (FAK orders can't be cancelled)
    order_type: crate::venues::core::TimeInForce,
) -> Result<bool> {
    // Slice 2b: resolve on-chain U256 once; the rest of the body is unchanged.
    let token_id = u256_from_market_id(token_id)?;
    // Only quick-confirm FAK orders. GTC orders need to wait for on-chain sync.
    if order_type != crate::venues::core::TimeInForce::Fak {
        return Ok(false);
    }

    // FAK orders are self-cancelling at the exchange (evaluate-and-discard).
    // There are no resting FAK orders to cancel, so skip straight to the fill check.
    let req2 = OrdersRequest::builder().asset_id(token_id).build();
    if !(match client.orders(&req2, None).await { Ok(p) => p.data.is_empty(), Err(_) => true }) {
        let mut pos_map = positions.lock().await;
        if let Some(pos) = pos_map.get_mut(&PositionKey::new(squadron_id, strategy_name, market_id_from_u256(token_id))) {
            pos.fill_confirmed_at = Some(Utc::now());
            info!("✅ QUICK CONFIRM FILL [{}]: Token {} filled instantly", strategy_name, token_id);
            return Ok(true);
        }
    }
    Ok(false)
}