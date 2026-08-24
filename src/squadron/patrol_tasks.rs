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

/// Peripheral tasks spawned by `Squadron::patrol()` — Phase 3f-4.
///
/// Each function spawns one independent Tokio task that runs at its own cadence
/// until the `peripheral_cancel` token fires.  Lifting these out of the main
/// `select!` loop means:
///
///   • A 45 s cleanup cycle can no longer delay strategy evaluation ticks.
///   • Status, settlement, and pulse tasks run on their own schedules without
///     being held back by a stalled `.await` elsewhere in the loop.
///   • The core `select!` in `patrol()` shrinks to three arms:
///     `cancel`, `market_rx.changed()`, and `ticker` (strategy evaluation).
///
/// **Lifecycle contract**: tasks stop when `peripheral_cancel` fires.
/// `patrol()` fires it before returning so no task outlives a market rotation.
///
/// **Watchdog contract**: `spawn_watchdog_task` accepts the patrol's own
/// `cancel` token.  When it detects loop silence it calls `cancel.cancel()`,
/// which triggers the `cancel.cancelled()` arm in `patrol()`'s `select!` and
/// causes the patrol to stand down and restart in the outer `'market_loop`.

use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use alloy::primitives::{U256, Address, address};
use alloy::providers::Provider;
use alloy::signers::local::LocalSigner;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::{watch, Mutex};
use tokio::time::{interval, Instant, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, error, debug};

use polymarket_client_sdk_v2::clob::Client as ClobClient;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::clob::types::{Side};
use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk_v2::clob::types::AssetType;

use crate::config;
use crate::state::{MarketConfig, PriceState};
use crate::helpers::{
    balance::*, orders::*,
    notifications::send_notification, db, metrics,
};

// V2 CTF Exchange contracts — duplicated here so patrol_tasks is self-contained.
const EXCHANGE_NORMAL:   Address = address!("0xE111180000d2663C0091e4f400237545B87B996B");
const EXCHANGE_NEG_RISK: Address = address!("0xe2222d279d744050d28e00520010520000310F59");

/// Inner-loop stall threshold: if `last_heartbeat_at` hasn't been updated for
/// this many seconds the watchdog fires.  Must be < 300 s (OS watchdog) and
/// > 120 s (ticker interval × one dropped tick tolerance).
const LOOP_WATCHDOG_SECS: u64 = 180;

// ─── Pulse task ──────────────────────────────────────────────────────────────

/// Periodically pings the CLOB API to verify the TCP connection is alive.
///
/// Logs network round-trip time.  A 10 s timeout prevents the task from
/// blocking the tokio runtime on a TCP-level stall.
pub fn spawn_pulse_task(
    trading_client: Arc<ClobClient<Authenticated<Normal>>>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(300));
        ticker.tick().await; // skip immediate first tick
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {
                    let start = Instant::now();
                    let mut req = BalanceAllowanceRequest::default();
                    req.asset_type = AssetType::Collateral;
                    match tokio::time::timeout(
                        Duration::from_secs(10),
                        trading_client.balance_allowance(req),
                    ).await {
                        Ok(_) => {}
                        Err(_) => warn!("⚠️ Network Pulse: balance_allowance timed out (10s) — CLOB API stall suspected"),
                    }
                    info!(" Network Pulse: {:?}", start.elapsed());
                }
            }
        }
    });
}

// ─── Settlement task ─────────────────────────────────────────────────────────

/// Periodically redeems fully settled Polymarket positions via the CTF contract.
///
/// Uses the Polygon RPC provider for on-chain calls.  Generic over `P` (the
/// alloy wallet provider) so it can be called from the generic `patrol<P>`.
pub fn spawn_settlement_task<P>(
    wallet_provider: P,
    safe_address: Address,
    eoa_address: Address,
    asset: String,
    cancel: CancellationToken,
) where
    P: Provider + Clone + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(config::MERGE_SCAN_INTERVAL_SECS));
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {
                    match tokio::time::timeout(Duration::from_secs(60), async {
                        let settled = crate::tasks::cleanup::auto_settle_closed_positions(
                            wallet_provider.clone(),
                            safe_address,
                            eoa_address,
                        ).await;
                        if settled {
                            // Keep Control Tower's open_positions mirror current right after settlement.
                            crate::tasks::cleanup::sync_open_positions_with_chain(safe_address).await;
                        }
                    }).await {
                        Ok(_) => {}
                        Err(_) => warn!("⚠️ settlement task timed out (60s) — skipping this cycle"),
                    }

                    // After processing explicit settlements, scan for positions that were
                    // auto-settled by Polymarket (outside our settlement ticker).
                    // Pass the squadron's asset so it only scans its own database pool.
                    match tokio::time::timeout(Duration::from_secs(30),
                        crate::tasks::cleanup::detect_orphaned_arb_settlements(safe_address, &asset)
                    ).await {
                        Ok(_) => {}
                        Err(_) => warn!("⚠️ orphan detection task timed out (30s) — skipping this cycle"),
                    }
                }
            }
        }
    });
}

// ─── Status task ─────────────────────────────────────────────────────────────

/// Calculate the mark-to-market value of all open positions.
/// Returns the sum of (shares × current_price) for each position.
///
/// IMPORTANT: each position is valued by its OWN token's live `current_price`
/// (refreshed per-token from the Polymarket Data API by the chain-sync task),
/// falling back to `entry_price` when no live price is available.
///
/// A single asset DB can hold positions across several DISTINCT markets
/// (e.g. the hourly venue plus the daily maker venue). The previous
/// implementation priced every position using the squadron's currently-attached
/// market YES/NO mids, which inflated and wildly oscillated the portfolio value
/// (positions in other/resolved markets were mis-priced). Valuing each token by
/// its own `current_price` keeps this snapshot consistent with `/api/portfolio`
/// and the chain-sync snapshot — a single source of truth.
/// On-chain share balance for one token, distinguishing "holds nothing" from
/// "could not ask".
///
/// `helpers::balance::onchain_balance_for_token` collapses both into `0`, which
/// is right for its callers (erring toward "still held" costs only a retry) but
/// wrong here: a transient API error would silently value a real position at
/// zero and make the portfolio drop by its full size for a minute.
async fn onchain_shares(
    client: &Arc<ClobClient<Authenticated<Normal>>>,
    token_id: &str,
) -> Option<Decimal> {
    let token_u = crate::venues::intl::u256_from_market_id(
        &crate::venues::core::MarketId::new(token_id)).ok()?;
    let mut req = BalanceAllowanceRequest::default();
    req.asset_type = AssetType::Conditional;
    req.token_id = Some(token_u);
    match tokio::time::timeout(Duration::from_secs(5), client.balance_allowance(req)).await {
        Ok(Ok(resp)) => Decimal::from_str(&resp.balance.to_string())
            .ok()
            .map(|b| b / dec!(1_000_000)),
        _ => None,
    }
}

/// May a chain reading of ZERO be written back over the stored share count?
///
/// The balance endpoint lags a fresh fill by up to ~15s and has read 0 for
/// positions that genuinely existed (trades 294, 300, 308, 310). Persisting that
/// zero would erase a real position from the row every consumer reads, so a
/// downward correction to nothing is only trusted once the row is older than the
/// settlement grace. Non-zero readings are always trusted: they cannot erase
/// anything, and they are how a partial fill or partial exit gets recorded.
fn persist_chain_correction(chain_shares: Decimal, row_ts: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
    if chain_shares > dec!(0) { return true; }
    match chrono::DateTime::parse_from_rfc3339(row_ts) {
        Ok(opened) => (now - opened.with_timezone(&chrono::Utc)).num_seconds()
            >= crate::config::FRESH_FILL_SETTLEMENT_GRACE_SECS,
        // Unparseable timestamp: refuse to zero the row rather than guess.
        Err(_) => false,
    }
}

/// How many shares should a portfolio valuation credit this row?
///
/// `None` means "do not value this row at all".
///
/// The chain is authoritative when it answers. Two production symptoms came
/// from trusting the DB row instead (2026-08-20 02:44-02:47):
///
///   * UNDER-count — a fill had settled and taken $7.92 of cash, but the row was
///     still `pending`, so the portfolio showed the cash gone and the shares
///     absent, dipping ~$8 for a minute.
///   * OVER-count — the shares were disposed of and the cash had come back, but
///     the row stayed open until the exit was booked two minutes later, so the
///     portfolio counted $8 of stock that no longer existed AND the cash it had
///     been sold for.
///
/// When the chain cannot be reached we fall back to the old DB-only rule rather
/// than guess, because a failed read must not erase a real position.
fn shares_to_value(
    chain_shares: Option<Decimal>,
    db_shares: Decimal,
    status: &str,
    chain_adopted: bool,
) -> Option<Decimal> {
    match chain_shares {
        Some(on_chain) => Some(on_chain.max(dec!(0))),
        // Unconfirmed phantom: an order we placed that the chain never
        // confirmed. Valuing it invents profit that does not exist.
        None if status == "pending" && !chain_adopted => None,
        None => Some(db_shares),
    }
}

/// Reconcile open positions against on-chain holdings, persist any correction,
/// and return the portfolio's mark-to-market position value.
///
/// The write-back is the point. Valuing correctly here would fix only this
/// snapshot, while `/api/portfolio` — which the dashboard polls continuously —
/// re-derives its own valuation straight from the same stale rows. Correcting
/// the row instead fixes every consumer at once, and keeps the claim those two
/// call sites make about being "one source of truth" actually true.
///
/// Only a SUCCESSFUL chain read is ever persisted: a failed request must not be
/// written back as zero shares.
async fn calculate_positions_value(
    pool: &sqlx::SqlitePool,
    client: &Arc<ClobClient<Authenticated<Normal>>>,
) -> Decimal {
    // Fetch all open positions
    let positions = db::get_open_positions(pool).await;
    if positions.is_empty() {
        return dec!(0);
    }


    // If the same token appears multiple times (e.g. one chain-adopted row plus one
    // strategy-owned row on the same outcome), value it ONCE to avoid portfolio
    // inflation — and pick the row that reflects on-chain reality.
    //
    // Prefer the CHAIN-ADOPTED row: chain-sync stamps it to the wallet's real on-chain
    // size (stale ones are purged), so it is authoritative. A non-adopted strategy row
    // may be a phantom that never settled on-chain. Among equal adoption status, prefer
    // larger shares. Mirrors the dedup rule in /api/portfolio so both snapshots and the
    // banner stay one source of truth.
    let mut deduped_by_token: std::collections::HashMap<String, db::OpenPositionRow> =
        std::collections::HashMap::new();
    for pos in positions {
        // Skip UNCONFIRMED phantoms: a row that is still `status='pending'` AND has
        // not been chain-adopted represents an order we placed but the chain never
        // confirmed (never filled, or rejected). Marking these to market for up to
        // the 60-min purge grace inflates the portfolio with profit that does not
        // exist on-chain (observed 2026-06-19: a never-filled TrendCapture June-20 NO
        // leg added a phantom +$2.83 / "open profitable trade"). A genuine fill flips
        // to chain_adopted=1 on the next chain-sync and starts counting then.
        match deduped_by_token.get(&pos.token_id) {
            None => {
                deduped_by_token.insert(pos.token_id.clone(), pos);
            }
            Some(existing) => {
                let existing_shares = existing.shares.parse::<Decimal>().unwrap_or(dec!(0));
                let candidate_shares = pos.shares.parse::<Decimal>().unwrap_or(dec!(0));
                let replace = (!existing.chain_adopted && pos.chain_adopted)
                    || (existing.chain_adopted == pos.chain_adopted && candidate_shares > existing_shares);
                if replace {
                    deduped_by_token.insert(pos.token_id.clone(), pos);
                }
            }
        }
    }

    let mut total_value = dec!(0);
    for (_, pos) in deduped_by_token {
        // Parse shares
        let db_shares = match pos.shares.parse::<Decimal>() {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Ask the chain what we actually hold. Typically 1-3 open positions, so
        // this is a handful of calls a minute against a task that already makes
        // one; each is individually timed out and a failure falls back to the
        // DB row rather than erasing the position.
        let chain = onchain_shares(client, &pos.token_id).await;
        if let Some(c) = chain {
            let c = c.max(dec!(0));
            // 5% tolerance absorbs rounding on fractional share sizes; anything
            // larger is real drift and gets written back so the API, the banner
            // and the next snapshot all agree.
            let drifted = (c - db_shares).abs() > (db_shares.abs() * dec!(0.05)).max(dec!(0.0001));
            if drifted && persist_chain_correction(c, &pos.ts, chrono::Utc::now()) {
                warn!("⚠️ Position drift [{}]: DB says {:.4} shares, chain says {:.4} — correcting the row",
                      pos.token_id, db_shares, c);
                let entry = pos.entry_price.parse::<Decimal>().unwrap_or(dec!(0));
                db::update_position_from_chain(pool, &pos.token_id, c, entry, None).await;
            } else if drifted {
                debug!("Position drift [{}] within settlement grace ({:.4} vs {:.4}) — not persisting yet",
                       pos.token_id, db_shares, c);
            }
        }
        let shares = match shares_to_value(chain, db_shares, &pos.status, pos.chain_adopted) {
            Some(s) => s,
            None => continue,
        };
        if shares <= dec!(0) { continue; }

        // Value this position by its OWN live current_price (per-token, set by
        // the chain-sync task from the Polymarket Data API). Fall back to the
        // entry price when no live price is available. Never use another
        // market's mids here — that inflates positions held in other markets.
        let price_to_use = pos
            .current_price
            .as_deref()
            .and_then(|p| p.parse::<Decimal>().ok())
            .filter(|p| *p > dec!(0))
            .or_else(|| pos.entry_price.parse::<Decimal>().ok())
            .unwrap_or(dec!(0));

        total_value += shares * price_to_use;
    }

    total_value
}

/// Periodic status heartbeat: logs prices/OBI, refreshes live collateral, and
/// records a PnL checkpoint to SQLite.
///
/// Also pulses the OS-thread process watchdog so a tokio stall can be
/// distinguished from a successful strategy tick.
pub fn spawn_status_task(
    live_collateral: Arc<Mutex<Decimal>>,
    total_pnl:       Arc<Mutex<Decimal>>,
    trading_client:  Arc<ClobClient<Authenticated<Normal>>>,
    yes_price_rx:    watch::Receiver<PriceState>,
    no_price_rx:     watch::Receiver<PriceState>,
    oracle_rx:       watch::Receiver<Decimal>,
    process_heartbeat_secs: Arc<AtomicU64>,
    asset:  String,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(60));
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {
                    // Pulse the OS-thread watchdog from the status task too — a stalled
                    // strategy ticker alone doesn't mean the runtime is dead.
                    process_heartbeat_secs.store(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        AtomicOrdering::Relaxed,
                    );

                    let (yb, ybd, ya, yad, _, ybd_all, yad_all) = *yes_price_rx.borrow();
                    let (nb, nbd, na, nad, _, nbd_all, nad_all) = *no_price_rx.borrow();
                    // Compute OBI for heartbeat visibility so thresholds can be tuned empirically.
                    let yes_obi = if ybd + yad > dec!(0) { (ybd - yad) / (ybd + yad) } else { dec!(0) };
                    let no_obi  = if nbd + nad > dec!(0) { (nbd - nad) / (nbd + nad) } else { dec!(0) };
                    // The same ratio over EVERY published level, logged beside the
                    // top-of-book figure the vipers actually gate on. Nothing reads
                    // these yet: they exist so the two series can be compared on live
                    // books before any threshold is re-derived. The top-of-book
                    // version is one resting order per side, which is why it swings
                    // between +0.9 and -0.3 on an unremarkable book.
                    let yes_obi_all = if ybd_all + yad_all > dec!(0) { (ybd_all - yad_all) / (ybd_all + yad_all) } else { dec!(0) };
                    let no_obi_all  = if nbd_all + nad_all > dec!(0) { (nbd_all - nad_all) / (nbd_all + nad_all) } else { dec!(0) };
                    info!(
                        " Heartbeat | Ask Sum ${:.4} (Y ask ${:.2} / N ask ${:.2}) | \
                         Bid Sum ${:.4} (Y bid ${:.2} / N bid ${:.2}) | \
                         Binance: ${:.2} | OBI Y={:.2} N={:.2} | OBIall Y={:.2} N={:.2} (depth Y {:.0}/{:.0} N {:.0}/{:.0})",
                        ya + na, ya, na, yb + nb, yb, nb, *oracle_rx.borrow(), yes_obi, no_obi,
                        yes_obi_all, no_obi_all, ybd_all, yad_all, nbd_all, nad_all,
                    );

                    // Refresh live pUSD balance so strategies can self-gate on insufficient funds.
                    // Hard 10 s timeout — a TCP-level CLOB API stall must not block this task.
                    let mut bal_req = BalanceAllowanceRequest::default();
                    bal_req.asset_type = AssetType::Collateral;
                    match tokio::time::timeout(
                        Duration::from_secs(10),
                        trading_client.balance_allowance(bal_req),
                    ).await {
                        Ok(Ok(resp)) => {
                            let bal = Decimal::from_str(&resp.balance.to_string())
                                .unwrap_or(dec!(0)) / dec!(1_000_000);
                            *live_collateral.lock().await = bal;
                            debug!(" Live pUSD balance: ${:.4}", bal);
                            if let Some(pool) = db::pool_for(&asset) {
                                let pnl_snap = *total_pnl.lock().await;

                                // Calculate total portfolio value: cash + mark-to-market positions
                                let positions_value = calculate_positions_value(&pool, &trading_client).await;
                           let total_value = bal + positions_value;

                                if tokio::time::timeout(
                                    Duration::from_secs(3),
                                    db::record_pnl_snapshot(&pool, pnl_snap, bal, total_value),
                                ).await.is_err() {
                                    warn!("⚠️ record_pnl_snapshot timed out (3s) — skipping this checkpoint");
                                }
                            }
                        }
                        Ok(Err(e)) => warn!("⚠️ balance_allowance error in status task: {}", e),
                        Err(_)    => warn!("⚠️ balance_allowance timed out (10s) in status task — skipping balance update this tick"),
                    }
                }
            }
        }
    });
}

// ─── Cleanup task ────────────────────────────────────────────────────────────

/// Periodic position maintenance: cleans expired positions, reconciles orphans,
/// attempts re-hedge or FAK-sell on confirmed naked legs, and syncs chain state.
///
/// Runs every 300 s.  Orphan re-hedge/exit order placement happens OUTSIDE the
/// 45 s cleanup timeout so order latency doesn't count against the cap.
#[allow(clippy::too_many_arguments)]
pub fn spawn_cleanup_task(
    // Squadron these positions belong to; recorded with each row so a restart
    // restores them to the squadron that opened them.
    squadron_id:          String,
    positions:            Arc<Mutex<crate::state::PositionMap>>,
    trading_client:       Arc<ClobClient<Authenticated<Normal>>>,
    nonce_manager:        Arc<AtomicU64>,
    signer:               LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    safe_address:         Address,
    eoa_address:          Address,
    shared_http:          Arc<reqwest::Client>,
    phantom_cooldowns:    PhantomCooldowns,
    orphan_tombstones:    OrphanTombstones,
    time_decay_positions: Arc<Mutex<std::collections::HashMap<crate::venues::core::MarketId, crate::vipers::time_decay_impl::TimeDecayPosition>>>,
    pending_orders:       Arc<Mutex<std::collections::HashMap<crate::state::PositionKey, Instant>>>,
    yes_price_rx:         watch::Receiver<PriceState>,
    no_price_rx:          watch::Receiver<PriceState>,
    maker_yes_price_rx:   Option<watch::Receiver<PriceState>>,
    maker_no_price_rx:    Option<watch::Receiver<PriceState>>,
    hourly_yes_token:     crate::venues::core::MarketId,
    hourly_no_token:      crate::venues::core::MarketId,
    hourly_market_name:   String,
    hourly_market_close_time: Option<chrono::DateTime<chrono::Utc>>,
    maker_market_config:  Option<MarketConfig>,
    tg_token:             String,
    tg_chat_id:           String,
    asset:                String,
    cancel:               CancellationToken,
) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(300));
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {
                    // Wrap the cleanup work in a 45 s outer timeout.
                    // Returns the list of confirmed-fill orphans so we can attempt FAK sells
                    // OUTSIDE the timeout — sell latency does not count against the 45 s cap.
                    let orphan_exits = match tokio::time::timeout(Duration::from_secs(45), async {
                        if hourly_yes_token != crate::venues::intl::market_id_from_u256(U256::ZERO) {
                            crate::tasks::cleanup::cleanup_expired_positions(
                                Arc::clone(&positions),
                                hourly_market_name.clone(),
                                hourly_yes_token.clone(), hourly_no_token.clone(),
                                hourly_market_close_time,
                            ).await;
                        }
                        if let Some(ref mk) = maker_market_config {
                            crate::tasks::cleanup::cleanup_expired_positions(
                                Arc::clone(&positions),
                                mk.market_name.clone(),
                                mk.yes_token.clone(), mk.no_token.clone(),
                                mk.market_close_time,
                            ).await;
                        }

                        let orphans = crate::tasks::cleanup::reconcile_orphaned_positions(
                            Arc::clone(&positions), &trading_client,
                            &phantom_cooldowns, &orphan_tombstones,
                            &tg_token, &tg_chat_id,
                        ).await.unwrap_or_else(|e| {
                            warn!("⚠️ Orphan reconciliation error: {}", e);
                            vec![]
                        });

                        crate::tasks::cleanup::cleanup_time_decay_positions(
                            Arc::clone(&time_decay_positions)
                        ).await;
                        crate::tasks::cleanup::sync_open_positions_with_chain(safe_address).await;

                        // ── Score settled GBoost vetoes ──────────────────────
                        // Attach real resolution outcomes to the shadow-log of
                        // gate-rejected signals. Until this runs the table can
                        // only say what the model BELIEVED, so there is no way to
                        // tell whether a gate blocked a winner or saved a loss —
                        // which is the entire question the entry stack turns on.
                        // Capped per sweep so a long unlabelled backlog cannot
                        // monopolise the 45s cleanup budget.
                        if let Some(pool) = crate::helpers::db::pool_for(&asset) {
                            let http = Arc::clone(&shared_http);
                            let scored = crate::helpers::db::score_pending_gboost_vetoes(
                                &pool,
                                config::GBOOST_VETO_SCORING_BATCH,
                                |token_id| {
                                    let http = Arc::clone(&http);
                                    let pool = pool.clone();
                                    async move {
                                        let cid = crate::helpers::db::condition_id_for_veto_token(&pool, &token_id).await?;
                                        let prices = crate::helpers::market::fetch_resolved_outcome_prices(&http, &cid).await?;
                                        prices.get(&token_id).copied()
                                    }
                                },
                            ).await;
                            if scored > 0 {
                                info!("🏷️ Scored {} settled GBoost veto(es) with real outcomes", scored);
                            }
                        }

                        // Periodically clean up expired pending order locks
                        {
                            let mut pending = pending_orders.lock().await;
                            pending.retain(|_, &mut instant| instant > Instant::now());
                        }
                        orphans
                    }).await {
                        Ok(v) => v,
                        Err(_) => {
                            warn!("⚠️ cleanup task timed out (45s) — CLOB/Data API stall suspected; task loop unblocked");
                            vec![]
                        }
                    };

                    // ── Re-hedge or exit each confirmed naked leg ──────────────────────────
                    //
                    // Priority 1 — RE-HEDGE: buy the MISSING leg at its current ask (FAK).
                    // Priority 2 — BID-BASED EXIT: sell the orphan at (current_bid − offset).
                    //
                    // Ghost guard: neither path places live orders when ghosting.
                    // Reads the live switch as well as the build constant — this is a
                    // background task with no tick snapshot to consult, and both of
                    // these branches place real orders.
                    if !crate::helpers::dynamic_config::ghosting_now() {
                        for orphan in orphan_exits {
                            // Slice 2b: OrphanExit and market tokens are all neutral MarketId.
                            let vc = if orphan.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                            let mut rehedged = false;

                            // PARTIAL-FILL GUARD (2026-07-26 trade 296): the recorded share
                            // count can be the full order size when only a sliver actually
                            // matched (fill-confirm doesn't sync shares). Sizing the FAK off
                            // the stale count draws "not enough balance" 400s and the real
                            // shares ride to $0 settlement. Verify on-chain and size from
                            // the smaller figure.
                            let orphan = {
                                let onchain = crate::helpers::balance::onchain_balance_for_token(
                                    &trading_client, &orphan.token_id).await;
                                if onchain > dec!(0) && onchain < orphan.shares {
                                    warn!("⚖️ ORPHAN EXIT: token {} recorded {} shares but on-chain {} — sizing from on-chain",
                                          orphan.token_id, orphan.shares, onchain);
                                    crate::tasks::cleanup::OrphanExit { shares: onchain, ..orphan }
                                } else {
                                    // onchain == 0 → lookup failure or nothing held; try the
                                    // recorded count (a 400 is harmless; next 5-min sweep retries).
                                    orphan
                                }
                            };

                            if let Some(paired_id) = orphan.paired_token_id {
                                let paired_ask = if paired_id == hourly_yes_token {
                                    yes_price_rx.borrow().2
                                } else if paired_id == hourly_no_token {
                                    no_price_rx.borrow().2
                                } else {
                                    maker_yes_price_rx.as_ref()
                                        .and_then(|rx| {
                                            let (_, _, ya, _, _, _, _) = *rx.borrow();
                                            if maker_market_config.as_ref().map_or(false, |mkc| mkc.yes_token == paired_id) {
                                                Some(ya)
                                            } else { None }
                                        })
                                        .or_else(|| maker_no_price_rx.as_ref().and_then(|rx| {
                                            let (_, _, na, _, _, _, _) = *rx.borrow();
                                            if maker_market_config.as_ref().map_or(false, |mkc| mkc.no_token == paired_id) {
                                                Some(na)
                                            } else { None }
                                        }))
                                        .unwrap_or(dec!(1))
                                };

                                let paired_ask_ticked = crate::helpers::price::round_to_tick_size(paired_ask);
                                let rehedge_cost = paired_ask_ticked + orphan.original_entry;

                                // Breakeven ceiling for a $1.00 binary payout, minus a buffer that
                                // covers the taker (FAK) fee — up to ~1.8% on Polymarket crypto/hourly
                                // markets — plus a small adverse-price cushion. Maker entries pay 0 fee;
                                // only this FAK re-hedge incurs the taker fee, and settlement redeem is
                                // free, so the buffer must absorb the re-hedge fee for the completed arb
                                // to stay profitable. Matches the atomic arb_pair_fill_monitor gate
                                // (balance.rs) so periodic and atomic re-hedges share one fee-aware threshold.
                                let rehedge_threshold = dec!(1.00) - config::ARB_FAK_REHEDGE_BUFFER;

                                if rehedge_cost < rehedge_threshold && paired_ask_ticked < dec!(0.99) {
                                    let buy_price = (paired_ask_ticked + config::BUY_PRICE_OFFSET)
                                        .min(config::MAX_BUY_LIMIT_PRICE);
                                    warn!(
                                        "♻️ ORPHAN RE-HEDGE [{}]: buying {} shares of missing leg {} @ ${:.4} ask \
                                         (orphan entry ${:.4} → total cost ${:.4} < threshold $0.99)",
                                        orphan.token_id, orphan.shares, paired_id,
                                        paired_ask_ticked, orphan.original_entry, rehedge_cost,
                                    );

                                    match place_limit_order(
                                        &trading_client, &nonce_manager, &signer,
                                        safe_address, eoa_address,
                                        vc, &paired_id, Side::Buy, orphan.shares,
                                        buy_price, 0, crate::venues::core::TimeInForce::Fak, false, 0, &shared_http,
                                    ).await {
                                                        Ok(order_id) => {
                                                            rehedged = true;
                                                            info!(
                                                                "✅ ORPHAN RE-HEDGE: FAK order placed {} — verifying fill...",
                                                                order_id,
                                                            );
                                                            let tok_o = tg_token.clone();
                                                            let cid_o = tg_chat_id.clone();
                                                            let sh_o = orphan.shares;
                                                            let cost_o = rehedge_cost;

                                                            // Get market and side info
                                                            let rh_market = if paired_id == hourly_yes_token || paired_id == hourly_no_token {
                                                                hourly_market_name.clone()
                                                            } else {
                                                                maker_market_config.as_ref()
                                                                    .map(|mkc| mkc.market_name.clone())
                                                                    .unwrap_or_else(|| hourly_market_name.clone())
                                                            };
                                                            let rh_side = if paired_id == hourly_yes_token
                                                                || maker_market_config.as_ref().map_or(false, |mkc| mkc.yes_token == paired_id)
                                                            { "YES" } else { "NO" };

                                                            let rh_squadron = squadron_id.clone();
                                                            let rh_tid = paired_id.to_string();
                                                            let rh_mkt = rh_market.clone();
                                                            let rh_sd  = rh_side.to_string();
                                                            let rh_ep  = paired_ask_ticked;
                                                            let rh_sh  = orphan.shares;
                                                            let rh_asset = asset.clone();
                                                            let rh_tc = Arc::clone(&trading_client);
                                                            // Needed so the async fill verifier can un-tombstone
                                                            // the still-naked orphan leg if the re-hedge buy
                                                            // doesn't actually fill on-chain (retry next cycle).
                                                            let rh_tombstones = orphan_tombstones.clone();
                                                            let rh_orphan_token = orphan.token_id.clone();

                                                            // Write pending position immediately (Viper Launch)
                                                            if let Some(pool) = db::pool_for(&rh_asset) {
                                                                db::record_open_position_with_status(
                                                                    &pool, &rh_squadron, "ArbitrageStrategy",
                                                                    &rh_tid, &rh_mkt, &rh_sd,
                                                                    rh_ep, rh_sh, false, "pending",
                                                                ).await;
                                                            }

                                                            tokio::spawn(async move {
                                                                // Wait 3s then verify fill on-chain
                                                                tokio::time::sleep(std::time::Duration::from_secs(3)).await;

                                                                let mut req = BalanceAllowanceRequest::default();
                                                                req.asset_type = AssetType::Conditional;
                                                                req.token_id = Some(crate::venues::intl::u256_from_market_id(&paired_id).unwrap_or_default());

                                                                let balance_ok = match tokio::time::timeout(
                                                                    std::time::Duration::from_secs(10),
                                                                    rh_tc.balance_allowance(req)
                                                                ).await {
                                                                    Ok(Ok(resp)) => {
                                                                        let balance = Decimal::from_str(&resp.balance.to_string())
                                                                            .unwrap_or(dec!(0)) / dec!(1_000_000);
                                                                        balance >= rh_sh * dec!(0.95) // Allow 5% tolerance
                                                                    }
                                                                    _ => false
                                                                };

                                                                if balance_ok {
                                                                    info!("✅ ORPHAN RE-HEDGE: confirmed on-chain — arb completed");
                                                                    let _ = send_notification(&tok_o, &cid_o, &format!(
                                                                        "♻️ Orphan re-hedged: bought {:.0} missing shares @ ${:.4} \
                                                                         (total arb cost ${:.4} → $1.00 payout at settle)",
                                                                        sh_o, paired_ask_ticked, cost_o,
                                                                    )).await;

                                                                    // Update to confirmed (Mission In-Flight) + record entry
                                                                    if let Some(pool) = db::pool_for(&rh_asset) {
                                                                        db::confirm_position_status(&pool, "ArbitrageStrategy", &rh_tid).await;
                                                                    }
                                                                    metrics::record_entry(
                                                                        &crate::state::TradeScope::crypto(
                                                                            &rh_asset, crate::venues::intl::INTL_VENUE, &rh_asset),
                                                                        "ArbitrageStrategy".to_string(),
                                                                        rh_tid.clone(), rh_mkt.clone(), rh_sd.clone(),
                                                                        rh_ep, rh_sh,
                                                                    ).await;
                                                                } else {
                                                                    warn!("⚠️ ORPHAN RE-HEDGE: FAK order accepted but fill not confirmed on-chain — removing pending position");
                                                                    if let Some(pool) = db::pool_for(&rh_asset) {
                                                                        db::close_open_position(&pool, "ArbitrageStrategy", &rh_tid).await;
                                                                    }
                                                                    // Re-hedge buy never filled → the original leg is
                                                                    // still naked. Un-tombstone it so the next cleanup
                                                                    // cycle re-adopts and retries (re-hedge or flatten)
                                                                    // instead of letting it ride to settlement.
                                                                    rh_tombstones.lock().await.remove(&rh_orphan_token);
                                                                }
                                                            });
                                                        }
                                        Err(e) => warn!(
                                            "⚠️ ORPHAN RE-HEDGE: FAK buy failed: {} — falling back to sell", e
                                        ),
                                    }
                                } else {
                                    warn!(
                                        "⚠️ ORPHAN RE-HEDGE skipped — rehedge cost ${:.4} ≥ threshold ${:.4} \
                                         (paired_ask=${:.4}); will sell orphan at current bid",
                                        rehedge_cost, rehedge_threshold, paired_ask_ticked,
                                    );
                                }
                            }

                            // ── Priority 2: Bid-based FAK sell (re-hedge failed/skipped) ──────
                            if !rehedged {
                                let orphan_bid = if orphan.token_id == hourly_yes_token {
                                    yes_price_rx.borrow().0
                                } else if orphan.token_id == hourly_no_token {
                                    no_price_rx.borrow().0
                                } else {
                                    maker_yes_price_rx.as_ref()
                                        .and_then(|rx| {
                                            let (yb, _, _, _, _, _, _) = *rx.borrow();
                                            if maker_market_config.as_ref().map_or(false, |mkc| mkc.yes_token == orphan.token_id) {
                                                Some(yb)
                                            } else { None }
                                        })
                                        .or_else(|| maker_no_price_rx.as_ref().and_then(|rx| {
                                            let (nb, _, _, _, _, _, _) = *rx.borrow();
                                            if maker_market_config.as_ref().map_or(false, |mkc| mkc.no_token == orphan.token_id) {
                                                Some(nb)
                                            } else { None }
                                        }))
                                        .unwrap_or(dec!(0))
                                };

                                let sell_price = if orphan_bid > dec!(0) {
                                    (orphan_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE)
                                } else {
                                    config::MIN_SELL_LIMIT_PRICE
                                };

                                warn!(
                                    " ORPHAN EXIT: selling {:.4} shares of token {} @ ${:.4} (current bid=${:.4})",
                                    orphan.shares, orphan.token_id, sell_price, orphan_bid,
                                );

                                match place_limit_order(
                                    &trading_client, &nonce_manager, &signer,
                                    safe_address, eoa_address,
                                    vc, &orphan.token_id, Side::Sell, orphan.shares,
                                    sell_price, 0, crate::venues::core::TimeInForce::Fak, false, 0, &shared_http,
                                ).await {
                                    Ok(order_id) => {
                                        info!("✅ ORPHAN EXIT: FAK sell submitted (order {})", order_id);
                                        let tok_o = tg_token.clone();
                                        let cid_o = tg_chat_id.clone();
                                        let sh_o = orphan.shares;
                                        tokio::spawn(async move {
                                            let _ = send_notification(&tok_o, &cid_o, &format!(
                                                " Orphan sold: {:.0} shares @ ${:.4} (bid-based FAK exit)",
                                                sh_o, sell_price,
                                            )).await;
                                        });
                                    }
                                    Err(e) => warn!(
                                        "⚠️ ORPHAN EXIT: FAK sell failed for token {}: {} \
                                         — position remains on-chain until settlement",
                                        orphan.token_id, e,
                                    ),
                                }
                            }
                        }
                    }
                }
            }
        }
    });
}

// ─── Watchdog task ───────────────────────────────────────────────────────────

/// Detects inner-loop stalls and triggers a patrol restart.
///
/// Checks `last_heartbeat_at` every 120 s.  If the strategy ticker has not
/// updated it within `LOOP_WATCHDOG_SECS` (180 s), the watchdog calls
/// `patrol_cancel.cancel()` which fires the `cancel.cancelled()` arm in the
/// patrol `select!` loop, causing `patrol()` to return and the outer
/// `'market_loop` to restart with a fresh context.
///
/// The watchdog stops cleanly when `peripheral_cancel` fires — i.e. when
/// `patrol()` exits normally (market rotation or CAG stand-down).
pub fn spawn_watchdog_task(
    last_heartbeat_at: Arc<Mutex<Instant>>,
    patrol_cancel:     CancellationToken,   // fires to trigger patrol restart
    peripheral_cancel: CancellationToken,   // fires when patrol exits normally
) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(120));
        ticker.tick().await; // consume immediate first tick
        loop {
            tokio::select! {
                biased;
                _ = peripheral_cancel.cancelled() => return,
                _ = ticker.tick() => {
                    let elapsed = last_heartbeat_at.lock().await.elapsed().as_secs();
                    if elapsed > LOOP_WATCHDOG_SECS {
                        error!(
                            " WATCHDOG: inner loop silent for {}s (limit={}s) — \
                             calling patrol cancel to trigger restart",
                            elapsed, LOOP_WATCHDOG_SECS,
                        );
                        patrol_cancel.cancel();
                        return;
                    }
                }
            }
        }
    });
}

// ─── Shared OrderLifecycle task (Slice 3 — intl migration) ───────────────────

/// Drive the shared venue-neutral [`OrderLifecycle`] for the intl CLOB venue.
///
/// Runs every `LIFECYCLE_SYNC_SECS` seconds. Confirms resting-order fills via
/// [`Execution::positions`] (on-chain ERC-1155 balance polling), cancels orders
/// that have been resting longer than `LifecycleConfig::intl().stale_order_secs`,
/// and flattens any naked leg whose hedge partner neither filled nor still rests.
///
/// This is additive alongside the existing `arb_pair_fill_monitor` /
/// `sync_position_balance` bespoke paths: both run in parallel until the
/// legacy paths are retired in a follow-on slice.
pub fn spawn_lifecycle_task(
    lifecycle: Arc<crate::venues::lifecycle::OrderLifecycle>,
    venue:     Arc<crate::venues::ActiveVenue>,
    positions: Arc<Mutex<crate::state::PositionMap>>,
    cancel:    CancellationToken,
    asset:     String,
) {
    const LIFECYCLE_SYNC_SECS: u64 = 30;
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(LIFECYCLE_SYNC_SECS));
        ticker.tick().await; // skip first tick — let the market settle
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {
                    let flattened = lifecycle.reconcile(venue.as_ref(), &positions).await;
                    for leg in flattened {
                        // Bug #9 hardening: the in-memory position map's avg_entry can be
                        // stale (e.g. adopted/topped-up fills). Cross-check against the DB
                        // open_positions row — written at entry and chain-stamped — and
                        // prefer it when the two diverge, logging the divergence so ghost
                        // validation can quantify how often the map drifts.
                        let tid_str = leg.token_id.to_string();
                        let avg_entry = match db::pool_for(&asset) {
                            Some(p) => match db::lookup_open_position_strategy(&p, &tid_str).await {
                                Some((db_entry, _)) if db_entry > dec!(0) => {
                                    if db_entry != leg.avg_entry {
                                        warn!(
                                            " [{strategy}] flatten avg_entry divergence: position map ${map:.4} vs DB ${db:.4} — using DB",
                                            strategy = leg.strategy, map = leg.avg_entry, db = db_entry,
                                        );
                                    }
                                    db_entry
                                }
                                _ => leg.avg_entry,
                            },
                            None => leg.avg_entry,
                        };
                        let pnl = (leg.exit_price - avg_entry) * leg.shares;
                        // Resolve the leg's real YES/NO outcome from the entries
                        // table so the trade isn't mislabelled "Sell" (the bare order
                        // direction). The venue-neutral lifecycle only knows token ids.
                        let side = match db::pool_for(&asset) {
                            Some(p) => db::lookup_entry_side_db(&p, &leg.token_id.to_string())
                                .await
                                .unwrap_or_else(|| "Sell".to_string()),
                            None => "Sell".to_string(),
                        };
                        warn!(
                            " [{strategy}] lifecycle flatten recorded: {market} entry={entry:.4} exit={exit:.4} shares={shares} pnl={pnl:.4}",
                            strategy = leg.strategy,
                            market   = leg.market_name,
                            entry    = avg_entry,
                            exit     = leg.exit_price,
                            shares   = leg.shares,
                        );
                        let asset_c    = asset.clone();
                        let strat      = leg.strategy.clone();
                        let market     = leg.market_name.clone();
                        let exit_price = leg.exit_price;
                        let shares     = leg.shares;
                        // Book synchronously (awaited, not a detached spawn): a container
                        // restart mid-cleanup used to kill the fire-and-forget task before
                        // the DB write flushed, leaving a flattened leg with no trade record
                        // (the invisible half of the 2026-07-03 arb orphan). Awaiting here
                        // guarantees the flatten is persisted before we move on.
                        metrics::record_trade(
                            &crate::state::TradeScope::crypto(
                                &asset_c, crate::venues::intl::INTL_VENUE, &asset_c),
                            Decimal::ZERO,
                            strat,
                            market,
                            side,
                            avg_entry,
                            exit_price,
                            shares,
                            pnl,
                            "LifecycleFlatten".to_string(),
                        ).await;
                        // Close the open_positions row now that the flatten is booked, so a
                        // stale row can't linger until the next chain-sync purge and tempt
                        // ChainReconcile into inventing a second exit for the same leg.
                        if let Some(p) = db::pool_for(&asset) {
                            db::close_open_position(&p, &leg.strategy, &tid_str).await;
                        }
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod portfolio_valuation_tests {
    use super::*;

    /// The OVER-count half of the 2026-08-20 02:45-02:47 divergence: the shares
    /// had been disposed of and the cash was already back in collateral, but the
    /// open_positions row survived until the exit was booked two minutes later.
    /// The portfolio then counted $8 of stock that no longer existed AND the
    /// cash it had been sold for, reporting 75.005 against a real 67.005.
    #[test]
    fn a_position_the_chain_no_longer_holds_is_worth_nothing() {
        assert_eq!(
            shares_to_value(Some(dec!(0)), dec!(18), "confirmed", true),
            Some(dec!(0)),
        );
    }

    /// The UNDER-count half: the fill had settled and taken $7.92 of cash, but
    /// the row was still `pending` and got skipped, so the portfolio showed the
    /// cash gone and the shares absent — 58.905 against a real 66.825.
    #[test]
    fn a_settled_fill_counts_even_while_the_row_says_pending() {
        assert_eq!(
            shares_to_value(Some(dec!(18)), dec!(18), "pending", false),
            Some(dec!(18)),
            "the chain says we hold them; the row's status is just lag",
        );
    }

    /// A failed chain read must never erase a real position — that would drop
    /// the portfolio by the position's full size for a minute.
    #[test]
    fn an_unreachable_chain_falls_back_to_the_database() {
        assert_eq!(
            shares_to_value(None, dec!(18), "confirmed", true),
            Some(dec!(18)),
        );
    }

    /// …but with no chain answer, the old phantom guard still applies: a row
    /// that is pending AND never chain-adopted is an order that may never have
    /// filled, and valuing it invents profit.
    #[test]
    fn an_unconfirmed_phantom_is_still_skipped_when_the_chain_is_unreachable() {
        assert_eq!(shares_to_value(None, dec!(18), "pending", false), None);
        // Chain-adopted rescues it: chain-sync stamped it to real holdings.
        assert_eq!(shares_to_value(None, dec!(18), "pending", true), Some(dec!(18)));
    }

    /// The chain is authoritative even when it disagrees with the row, in either
    /// direction — a partial fill or a partial exit both land here.
    #[test]
    fn the_chain_wins_over_a_stale_share_count() {
        assert_eq!(shares_to_value(Some(dec!(9)), dec!(18), "confirmed", true), Some(dec!(9)));
        assert_eq!(shares_to_value(Some(dec!(25)), dec!(18), "confirmed", true), Some(dec!(25)));
    }

    /// A negative balance is not physically meaningful; clamp rather than
    /// subtract from the portfolio.
    #[test]
    fn a_negative_chain_balance_clamps_to_zero() {
        assert_eq!(shares_to_value(Some(dec!(-5)), dec!(18), "confirmed", true), Some(dec!(0)));
    }

    // ── Write-back safety ──────────────────────────────────────────────────

    fn at(secs_ago: i64) -> (String, chrono::DateTime<chrono::Utc>) {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-20T02:47:26Z")
            .unwrap().with_timezone(&chrono::Utc);
        ((now - chrono::Duration::seconds(secs_ago)).to_rfc3339(), now)
    }

    /// The balance endpoint has read 0 for positions that genuinely existed
    /// (trades 294, 300, 308 and 310 all turned on exactly this). Writing that
    /// zero back would erase a real position from the row every consumer reads.
    #[test]
    fn a_zero_reading_on_a_fresh_fill_is_not_persisted() {
        let (ts, now) = at(10);
        assert!(!persist_chain_correction(dec!(0), &ts, now));
    }

    /// Past the settlement grace a zero is believable — this is the case that
    /// clears the phantom position which inflated total_value to 75.005.
    #[test]
    fn a_zero_reading_past_the_settlement_grace_is_persisted() {
        let (ts, now) = at(crate::config::FRESH_FILL_SETTLEMENT_GRACE_SECS + 30);
        assert!(persist_chain_correction(dec!(0), &ts, now));
    }

    /// A non-zero reading cannot erase anything, so it is always trusted — that
    /// is how a partial fill or partial exit reaches the row promptly.
    #[test]
    fn a_non_zero_reading_is_always_persisted() {
        let (fresh, now) = at(1);
        assert!(persist_chain_correction(dec!(9), &fresh, now));
    }

    /// An unparseable timestamp must not license zeroing the row.
    #[test]
    fn an_unparseable_timestamp_blocks_a_zeroing_write() {
        let now = chrono::Utc::now();
        assert!(!persist_chain_correction(dec!(0), "not-a-timestamp", now));
        assert!(persist_chain_correction(dec!(9), "not-a-timestamp", now));
    }
}
