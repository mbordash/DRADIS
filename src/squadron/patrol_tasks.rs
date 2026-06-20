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
async fn calculate_positions_value(pool: &sqlx::SqlitePool) -> Decimal {
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
        if pos.status == "pending" && !pos.chain_adopted {
            continue;
        }
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
        let shares = match pos.shares.parse::<Decimal>() {
            Ok(s) => s,
            Err(_) => continue,
        };

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

                    let (yb, ybd, ya, yad, _) = *yes_price_rx.borrow();
                    let (nb, nbd, na, nad, _) = *no_price_rx.borrow();
                    // Compute OBI for heartbeat visibility so thresholds can be tuned empirically.
                    let yes_obi = if ybd + yad > dec!(0) { (ybd - yad) / (ybd + yad) } else { dec!(0) };
                    let no_obi  = if nbd + nad > dec!(0) { (nbd - nad) / (nbd + nad) } else { dec!(0) };
                    info!(
                        " Heartbeat | Ask Sum ${:.4} (Y ask ${:.2} / N ask ${:.2}) | \
                         Bid Sum ${:.4} (Y bid ${:.2} / N bid ${:.2}) | \
                         Binance: ${:.2} | OBI Y={:.2} N={:.2}",
                        ya + na, ya, na, yb + nb, yb, nb, *oracle_rx.borrow(), yes_obi, no_obi,
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
                                let positions_value = calculate_positions_value(&pool).await;
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
    pending_orders:       Arc<Mutex<std::collections::HashMap<(String, crate::venues::core::MarketId), Instant>>>,
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
                    // GHOST_MODE guard: neither path places live orders in ghost mode.
                    if !config::GHOST_MODE {
                        for orphan in orphan_exits {
                            // Slice 2b: OrphanExit and market tokens are all neutral MarketId.
                            let vc = if orphan.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                            let mut rehedged = false;

                            if let Some(paired_id) = orphan.paired_token_id {
                                let paired_ask = if paired_id == hourly_yes_token {
                                    yes_price_rx.borrow().2
                                } else if paired_id == hourly_no_token {
                                    no_price_rx.borrow().2
                                } else {
                                    maker_yes_price_rx.as_ref()
                                        .and_then(|rx| {
                                            let (_, _, ya, _, _) = *rx.borrow();
                                            if maker_market_config.as_ref().map_or(false, |mkc| mkc.yes_token == paired_id) {
                                                Some(ya)
                                            } else { None }
                                        })
                                        .or_else(|| maker_no_price_rx.as_ref().and_then(|rx| {
                                            let (_, _, na, _, _) = *rx.borrow();
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
                                                                    &pool, "ArbitrageStrategy",
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
                                                                        &rh_asset,
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
                                            let (yb, _, _, _, _) = *rx.borrow();
                                            if maker_market_config.as_ref().map_or(false, |mkc| mkc.yes_token == orphan.token_id) {
                                                Some(yb)
                                            } else { None }
                                        })
                                        .or_else(|| maker_no_price_rx.as_ref().and_then(|rx| {
                                            let (nb, _, _, _, _) = *rx.borrow();
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
                        let pnl = (leg.exit_price - leg.avg_entry) * leg.shares;
                        warn!(
                            " [{strategy}] lifecycle flatten recorded: {market} entry={entry:.4} exit={exit:.4} shares={shares} pnl={pnl:.4}",
                            strategy = leg.strategy,
                            market   = leg.market_name,
                            entry    = leg.avg_entry,
                            exit     = leg.exit_price,
                            shares   = leg.shares,
                        );
                        let asset_c    = asset.clone();
                        let strat      = leg.strategy.clone();
                        let market     = leg.market_name.clone();
                        let avg_entry  = leg.avg_entry;
                        let exit_price = leg.exit_price;
                        let shares     = leg.shares;
                        tokio::spawn(async move {
                            metrics::record_trade(
                                &asset_c,
                                strat,
                                market,
                                "Sell".to_string(),
                                avg_entry,
                                exit_price,
                                shares,
                                pnl,
                                "LifecycleFlatten".to_string(),
                            ).await;
                        });
                    }
                }
            }
        }
    });
}
