/// `Squadron::patrol()` — the full inner tick loop.
///
/// Extracted from `main.rs`'s `'market_loop` in Phase 3f-3 so the CAG can
/// eventually spawn multiple concurrent squadron patrols (Phase 3f-5).
///
/// Phase 3f-4: peripheral tickers (pulse, settlement, cleanup, status, watchdog)
/// are lifted into independent Tokio tasks spawned via `patrol_tasks.rs`.
/// The core `select!` now has exactly three arms:
///   1. `cancel.cancelled()`       — CAG/watchdog stand-down
///   2. `ctx.market_rx.changed()`  — market rotation
///   3. `ticker.tick()`            — strategy evaluation

use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;

use alloy::primitives::{U256, Address, address};
use alloy::providers::Provider;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::time::{interval, Instant, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, error, debug};

use polymarket_client_sdk_v2::clob::types::{Side};
use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk_v2::clob::types::AssetType;

use crate::config;
use crate::state::{Position, StrategySignal, MarketConfig, MarketSnapshot};
use crate::venues::core::MarketId;
use crate::venues::intl::{market_id_from_u256, u256_from_market_id};
use crate::orchestrator::{StrategyRegistry, StrategyContext};
use crate::orchestrator::executor::{execute_strategies_concurrent, aggregate_and_resolve_signals};
use crate::helpers::{
    balance::*, orders::*,
    notifications::{send_notification, tweet_trade}, metrics, db,
};
use crate::squadron::Squadron;
use super::context::PatrolContext;
use super::patrol_tasks::{
    spawn_pulse_task, spawn_settlement_task, spawn_cleanup_task,
    spawn_status_task, spawn_watchdog_task, spawn_lifecycle_task,
};
use crate::venues::lifecycle::{LifecycleConfig, OrderLifecycle};

// V2 CTF Exchange contracts — same as main.rs constants
const EXCHANGE_NORMAL:   Address = address!("0xE111180000d2663C0091e4f400237545B87B996B");
const EXCHANGE_NEG_RISK: Address = address!("0xe2222d279d744050d28e00520010520000310F59");

const MAX_CANCEL_RETRIES: u32 = 5;
const BASE_CANCEL_RETRY_DELAY_MS: u64 = 200;

impl Squadron {
    /// Run the squadron's full patrol lifecycle.
    ///
    /// Drives the inner tick loop (strategy evaluation, order placement) until
    /// a market rotation is detected or the watchdog fires a restart.
    ///
    /// Peripheral tasks (pulse, settlement, cleanup, status, watchdog) run as
    /// independent Tokio tasks and are cancelled when `patrol()` returns.
    ///
    /// `ctx` is borrowed mutably so cooldown maps and per-market feeds persist
    /// across calls (PatrolContext is owned by `main.rs` outside `'market_loop`).
    ///
    /// The `cancel` token fires when the CAG signals a forced stand-down OR when
    /// the watchdog detects a stalled inner loop.
    pub async fn patrol<P>(
        &mut self,
        cancel: CancellationToken,
        ctx: &mut PatrolContext<P>,
    ) where
        P: Provider + Clone + Send + Sync + 'static,
    {
        // ── Preamble: pull ctx/self into local aliases ────────────────────────

        // Session-scoped Arc handles
        let positions             = ctx.session.positions.clone();
        let pending_orders        = ctx.session.pending_orders.clone();
        let total_pnl             = ctx.session.total_pnl.clone();
        let live_collateral       = ctx.session.live_collateral.clone();
        let starting_collateral_store = ctx.session.starting_collateral.clone();
        let phantom_cooldowns     = ctx.session.phantom_cooldowns.clone();
        let orphan_tombstones     = ctx.session.orphan_tombstones.clone();
        let time_decay_positions  = ctx.session.time_decay_positions.clone();
        let token_ownership       = ctx.session.token_ownership.clone();

        // Trading infrastructure
        let trading_client  = Arc::clone(&ctx.trading_client);
        let nonce_manager   = Arc::clone(&ctx.nonce_manager);
        let signer          = ctx.signer.clone();
        let safe_address    = ctx.safe_address;
        let eoa_address     = ctx.eoa_address;
        let shared_http     = Arc::clone(&ctx.shared_http);
        let wallet_provider = ctx.wallet_provider.clone();

        // Config / channels
        let dynamic_config = Arc::clone(&ctx.dynamic_config);
        let markets_tx = Arc::clone(&ctx.markets_tx);
        let crypto_filter = ctx.crypto_filter.clone();
        // Lowercase asset slug — used for per-asset DB pool lookups and metrics CSV naming.
        let asset_lc = crypto_filter.to_lowercase();

        // Notification credentials
        let tg_token             = ctx.tg_token.clone();
        let tg_chat_id           = ctx.tg_chat_id.clone();
        let tw_api_key           = ctx.tw_api_key.clone();
        let tw_api_secret        = ctx.tw_api_secret.clone();
        let tw_access_token      = ctx.tw_access_token.clone();
        let tw_access_token_secret = ctx.tw_access_token_secret.clone();

        // Watchdog heartbeat handles
        let process_heartbeat_secs = Arc::clone(&ctx.process_heartbeat_secs);
        let last_heartbeat_at      = Arc::clone(&ctx.last_heartbeat_at);

        // Price feeds (per-market, updated before each patrol() call)
        let yes_price_rx        = ctx.feeds.hourly_yes.clone();
        let no_price_rx         = ctx.feeds.hourly_no.clone();
        let maker_yes_price_rx  = ctx.feeds.maker_yes.clone();
        let maker_no_price_rx   = ctx.feeds.maker_no.clone();

        let maker_market_config = ctx.maker_market_config.clone();
        let market_started_at   = ctx.market_started_at;

        let cag = ctx.cag.clone();

        // Cooldown maps (survive market rotations — live in PatrolContext)
        let last_trade_time        = &mut ctx.last_trade_time;
        let last_stop_loss_time    = &mut ctx.last_stop_loss_time;
        let last_expiry_exit_time  = &mut ctx.last_expiry_exit_time;
        let last_exit_attempt_time = &mut ctx.last_exit_attempt_time;

        // Market rotation CIDs
        let current_hourly_cid = self.market.condition_id.clone();
        let current_maker_cid  = ctx.maker_market_config
            .as_ref()
            .map_or_else(String::new, |m| m.condition_id.clone());

        // Squadron's hourly market fields
        let hourly_yes_token         = self.market.yes_token.clone();
        let hourly_no_token          = self.market.no_token.clone();
        let hourly_market_name       = self.market.market_name.clone();
        let hourly_market_close_time = self.market.market_close_time;
        let hourly_strike_price      = self.market.strike_price;
        let hourly_is_neg_risk       = self.market.is_neg_risk;
        let hourly_yes_fee_rate      = self.market.yes_fee_bps;
        let hourly_no_fee_rate       = self.market.no_fee_bps;
        let hourly_condition_id      = self.market.condition_id.clone();

        // Raptor signal receivers
        let oracle_rx   = self.raptors.oracle.clone();
        let velocity_rx = self.raptors.velocity.clone();
        let drift_rx    = self.raptors.drift.clone();
        let funding_rx  = self.raptors.funding
            .as_ref()
            .expect("funding raptor always present")
            .clone();

        // ── Phase 3f-4: Peripheral token + spawned tasks ─────────────────────
        //
        // `peripheral_cancel` is fired when patrol() returns (market rotation,
        // CAG stand-down, or watchdog restart).  All spawned tasks watch it and
        // exit cleanly when it fires.
        //
        // The watchdog gets `cancel` (the patrol's own token) so it can trigger
        // the cancel.cancelled() arm in the select! below when it detects a stall.
        let peripheral_cancel = CancellationToken::new();

        // Reset heartbeat counters at the start of each patrol rotation.
        *last_heartbeat_at.lock().await = Instant::now();
        process_heartbeat_secs.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            AtomicOrdering::Relaxed,
        );

        spawn_pulse_task(Arc::clone(&trading_client), peripheral_cancel.clone());

        spawn_settlement_task(
            wallet_provider.clone(),
            safe_address,
            eoa_address,
            asset_lc.clone(),
            peripheral_cancel.clone(),
        );

        spawn_cleanup_task(
            Arc::clone(&positions),
            Arc::clone(&trading_client),
            Arc::clone(&nonce_manager),
            signer.clone(),
            safe_address,
            eoa_address,
            Arc::clone(&shared_http),
            phantom_cooldowns.clone(),
            orphan_tombstones.clone(),
            Arc::clone(&time_decay_positions),
            Arc::clone(&pending_orders),
            yes_price_rx.clone(),
            no_price_rx.clone(),
            maker_yes_price_rx.clone(),
            maker_no_price_rx.clone(),
            hourly_yes_token.clone(),
            hourly_no_token.clone(),
            hourly_market_name.clone(),
            hourly_market_close_time,
            maker_market_config.clone(),
            tg_token.clone(),
            tg_chat_id.clone(),
            asset_lc.clone(),
            peripheral_cancel.clone(),
        );

        spawn_status_task(
            Arc::clone(&live_collateral),
            Arc::clone(&total_pnl),
            Arc::clone(&trading_client),
            yes_price_rx.clone(),
            no_price_rx.clone(),
            oracle_rx.clone(),
            Arc::clone(&process_heartbeat_secs),
            asset_lc.clone(),
            peripheral_cancel.clone(),
        );

        // ── Slice 3: shared OrderLifecycle (intl migration) ──────────────────
        // One engine drives fill-confirm, stale-cancel, and naked-leg flatten
        // over the Execution trait surface. Runs alongside the existing bespoke
        // arb_pair_fill_monitor / sync_position_balance paths (additive for now).
        let lifecycle = std::sync::Arc::new(OrderLifecycle::new(LifecycleConfig::intl()));
        spawn_lifecycle_task(
            std::sync::Arc::clone(&lifecycle),
            std::sync::Arc::clone(&ctx.session.venue),
            Arc::clone(&positions),
            peripheral_cancel.clone(),
        );

        // Watchdog: fires cancel (the patrol token) on stall, stops on peripheral_cancel.
        spawn_watchdog_task(
            Arc::clone(&last_heartbeat_at),
            cancel.clone(),
            peripheral_cancel.clone(),
        );

        let strategies = StrategyRegistry::create_all_strategies();
        let adoption_order = StrategyRegistry::strategy_names();
        let live_collateral = Arc::clone(&live_collateral);

        // Allow CLOB API and WS orderbook snapshots to settle before reconciling.
        tokio::time::sleep(Duration::from_secs(5)).await;

        let hourly_token_bids: Vec<(MarketId, Decimal)> = if hourly_yes_token != market_id_from_u256(U256::ZERO) {
            vec![
                (hourly_yes_token.clone(), yes_price_rx.borrow().0),
                (hourly_no_token.clone(),  no_price_rx.borrow().0),
            ]
        } else {
            vec![]
        };

        let maker_token_bids: Vec<(MarketId, Decimal)> = match (&maker_yes_price_rx, &maker_no_price_rx, &maker_market_config) {
            (Some(yes_rx), Some(no_rx), Some(mk)) => vec![
                (mk.yes_token.clone(), yes_rx.borrow().0),
                (mk.no_token.clone(),  no_rx.borrow().0),
            ],
            _ => vec![],
        };

        if hourly_yes_token != market_id_from_u256(U256::ZERO) {
            reconcile_orphaned_positions(
                &trading_client, &positions,
                &[(hourly_yes_token.clone(), "YES"), (hourly_no_token.clone(), "NO")],
                &hourly_market_name, hourly_market_close_time, &hourly_token_bids, &adoption_order,
                Some(&orphan_tombstones),
            ).await;
        }
        if let Some(ref mk_config) = maker_market_config {
            reconcile_orphaned_positions(
                &trading_client, &positions,
                &[(mk_config.yes_token.clone(), "YES(maker)"), (mk_config.no_token.clone(), "NO(maker)")],
                &mk_config.market_name, mk_config.market_close_time, &maker_token_bids, &adoption_order,
                Some(&orphan_tombstones),
            ).await;
        }

        // ── Slice 3: register current market tokens with the venue ────────────
        // IntlClobVenue::positions() and open_orders() poll only the registered
        // set so OrderLifecycle::reconcile() has real data without scanning all
        // tokens ever traded. Clear first to drop tokens from the previous rotation.
        ctx.session.venue.clear_active_tokens().await;
        if hourly_yes_token != market_id_from_u256(U256::ZERO) {
            ctx.session.venue.register_tokens(&[hourly_yes_token.clone(), hourly_no_token.clone()]).await;
        }
        if let Some(ref mk) = maker_market_config {
            ctx.session.venue.register_tokens(&[mk.yes_token.clone(), mk.no_token.clone()]).await;
        }

        // ── Rebuild token ownership registry from the (now-reconciled) positions.
        //
        // This is the authoritative startup snapshot: any positions that were
        // re-adopted by `reconcile_orphaned_positions` above are immediately
        // reflected in the registry so the first strategy tick sees correct
        // ownership information and cannot double-enter a reconciled token.
        {
            let map = positions.lock().await;
            let mut ownership = token_ownership.lock().await;
            ownership.clear();
            for ((sn, tid), _) in map.iter() {
                let current_priority = StrategyRegistry::get_strategy_priority(sn).unwrap_or(usize::MAX);
                let entry = ownership.entry(tid.clone()).or_insert_with(|| sn.clone());
                let existing_priority = StrategyRegistry::get_strategy_priority(entry).unwrap_or(usize::MAX);

                if current_priority < existing_priority {
                    // Current strategy has higher priority, claim the token
                    *entry = sn.clone();
                }
            }
            if !ownership.is_empty() {
                info!(
                    "🗺️  Token ownership registry rebuilt from {} reconciled position(s):",
                    ownership.len()
                );
                for (tid, sn) in ownership.iter() {
                    info!("     {} → {}", &tid.to_string()[..16], sn);
                }
            }
        }

        let mut consecutive_failures: u32 = 0;
        let mut last_executor_summary = String::new();

        info!("🚀 Orchestrator ready: {} strategies loaded", strategies.len());
        info!("📋 Strategy venue attachments:");
        let mut strategy_markets_map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        for strategy in &strategies {
            let sn = strategy.name();
            let venue = strategy.venue();
            let market_name_attached = match venue {
                "Hourly" => hourly_market_name.clone(),
                "Window/Daily" => maker_market_config.as_ref().map_or_else(String::new, |m| m.market_name.clone()),
                _ => String::from("Unknown"),
            };
            let status_key = sn
                .strip_suffix("Strategy")
                .unwrap_or(&sn)
                .to_lowercase()
                .replace("timedecay", "time_decay");
            strategy_markets_map.insert(status_key, market_name_attached.clone());
            info!(
                "  - {} => venue={} | market=\"{}\" | budget=${} | risk={}",
                sn, venue, market_name_attached, strategy.max_exposure(), strategy.risk_model(),
            );
        }
        let _ = markets_tx.send(strategy_markets_map);

        // Extract venue Arc before the tick loop so it remains accessible inside
        // strategy signal arms where `ctx` is shadowed by a local StrategyContext.
        let patrol_venue = std::sync::Arc::clone(&ctx.session.venue);

        // ── Core tick loop: 3 arms ────────────────────────────────────────────
        let mut ticker = interval(config::main_ticker_interval());
        loop {
            tokio::select! {
                biased;
                // ── 1. CAG/watchdog stand-down ──────────────────────────────────
                // Fired by: CAG forced stand-down OR watchdog detecting stall.
                // In Phase 3f-3 / 3f-4 the CAG never fires this; the watchdog does.
                _ = cancel.cancelled() => {
                    info!("🛬  Squadron [{}] patrol cancelled — standing down", self.id);
                    self.cancel_ws();
                    break;
                }
                // ── 2. Market rotation ──────────────────────────────────────────
                _ = ctx.market_rx.changed() => {
                    let (
                        _new_hourly_yes_token,
                        _new_hourly_no_token,
                        _new_hourly_market_name,
                        _new_hourly_market_close_time,
                        _new_hourly_strike_price,
                        _new_hourly_desc,
                        new_maker_market_candidate,
                        new_hourly_condition_id,
                    ) = ctx.market_rx.borrow().clone();

                    let new_maker_cid = new_maker_market_candidate.as_ref().map_or_else(String::new, |m| m.condition_id.clone());

                    if new_hourly_condition_id == current_hourly_cid && new_maker_cid == current_maker_cid {
                        continue;
                    }
                    info!("🔄 Market switch detected — restarting trading loop with new market context");
                    let mut cancel_success = false;
                    for i in 0..MAX_CANCEL_RETRIES {
                        let delay = BASE_CANCEL_RETRY_DELAY_MS * (1 << i);
                        match tokio::time::timeout(
                            Duration::from_secs(8),
                            trading_client.as_ref().cancel_all_orders(),
                        ).await {
                            Ok(Ok(_)) => {
                                info!("✅ Successfully cancelled all orders after {} retries.", i);
                                cancel_success = true;
                                break;
                            },
                            Ok(Err(e)) => {
                                warn!("⚠️ Failed to cancel all orders (attempt {}/{}) with error: {}", i + 1, MAX_CANCEL_RETRIES, e);
                                if i < MAX_CANCEL_RETRIES - 1 {
                                    tokio::time::sleep(Duration::from_millis(delay)).await;
                                }
                            },
                            Err(_) => {
                                warn!("⚠️ cancel_all_orders timed out (8s) (attempt {}/{}) — retrying in {}ms", i + 1, MAX_CANCEL_RETRIES, delay);
                                if i < MAX_CANCEL_RETRIES - 1 {
                                    tokio::time::sleep(Duration::from_millis(delay)).await;
                                }
                            }
                        }
                    }
                    if !cancel_success {
                        error!("❌ Failed to cancel all orders after {} attempts. Proceeding with market switch, but orders may remain open.", MAX_CANCEL_RETRIES);
                    }

                    { phantom_cooldowns.lock().await.clear(); }
                    { pending_orders.lock().await.clear(); }
                    let _ = new_hourly_condition_id;
                    let _ = new_maker_cid;
                    self.stand_down();
                    info!("️  Squadron [{}] → state={}", self.id, self.state);
                    cag.update_state(&self.id, crate::squadron::SquadronState::StoodDown);
                    cag.remove(&self.id);
                    self.cancel_ws();
                    break;
                }
                // ── 3. Strategy evaluation tick ─────────────────────────────────
                _ = ticker.tick() => {
                    // Skip evaluation this tick if the market has changed — yield to arm 2.
                    if ctx.market_rx.has_changed().unwrap_or(false) { continue; }

                    // Pulse both heartbeat counters so the watchdog task sees recent activity.
                    *last_heartbeat_at.lock().await = Instant::now();
                    process_heartbeat_secs.store(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        AtomicOrdering::Relaxed,
                    );

                    // Get hourly market snapshot
                    let (hourly_yb, hourly_ybd, hourly_ya, hourly_yad, hourly_yes_ws_ts) = *yes_price_rx.borrow();
                    let (hourly_nb, hourly_nbd, hourly_na, hourly_nad, hourly_no_ws_ts) = *no_price_rx.borrow();
                    let hourly_snap_ts = hourly_yes_ws_ts.min(hourly_no_ws_ts);

                    // Get maker market snapshot if available
                    let (maker_yb, maker_ybd, maker_ya, maker_yad, maker_yes_ws_ts) = maker_yes_price_rx.as_ref().map_or((dec!(0), dec!(0), dec!(1), dec!(0), Utc::now()), |rx| *rx.borrow());
                    let (maker_nb, maker_nbd, maker_na, maker_nad, maker_no_ws_ts) = maker_no_price_rx.as_ref().map_or((dec!(0), dec!(0), dec!(1), dec!(0), Utc::now()), |rx| *rx.borrow());
                    let maker_snap_ts = maker_yes_ws_ts.min(maker_no_ws_ts);

                    // Only proceed if at least one market has valid prices
                    if (hourly_ya == dec!(1) && hourly_na == dec!(1)) && (maker_ya == dec!(1) && maker_na == dec!(1)) { continue; }

                    let hourly_market_config_for_ctx = MarketConfig {
                        yes_token: hourly_yes_token.clone(), no_token: hourly_no_token.clone(), market_name: hourly_market_name.clone(), market_close_time: hourly_market_close_time, strike_price: hourly_strike_price, is_neg_risk: hourly_is_neg_risk, condition_id: hourly_condition_id.clone(), yes_fee_bps: hourly_yes_fee_rate, no_fee_bps: hourly_no_fee_rate,
                    };

                    let maker_market_config_for_ctx = maker_market_config.clone();

                    let dyn_cfg = Arc::new(dynamic_config.read().unwrap().clone());

                    // Hoist mutex-await calls OUT of the struct literal so that
                    // borrow() Ref guards (oracle_rx, velocity_rx, etc.) in the
                    // snapshot fields are NOT alive at any .await point.
                    // Without this the future is non-Send and tokio::spawn rejects
                    // it (Phase 3f-6: concurrent multi-asset spawning).
                    let ctx_session_pnl          = *total_pnl.lock().await;
                    let ctx_starting_collateral  = *starting_collateral_store.lock().await;
                    let ctx_available_collateral = *live_collateral.lock().await;

                    let ctx = StrategyContext {
                        market: hourly_market_config_for_ctx.clone(),
                        snapshot: MarketSnapshot {
                            yes_bid: hourly_yb, yes_bid_depth: hourly_ybd, yes_ask: hourly_ya, yes_ask_depth: hourly_yad,
                            no_bid: hourly_nb, no_bid_depth: hourly_nbd, no_ask: hourly_na, no_ask_depth: hourly_nad,
                            oracle_price: *oracle_rx.borrow(),
                            velocity: velocity_rx.borrow().0,
                            velocity_1s: velocity_rx.borrow().1,
                            acceleration: velocity_rx.borrow().2,
                            funding_rate: *funding_rx.borrow(),
                            oracle_drift_60m: drift_rx.borrow().0,
                            oracle_drift_10m: drift_rx.borrow().1,
                            secs_to_expiry: hourly_market_close_time
                                .map(|t| (t - Utc::now()).num_seconds())
                                .unwrap_or(0),
                            timestamp: hourly_snap_ts,
                        },
                        positions: Arc::clone(&positions),
                        session_pnl:          ctx_session_pnl,
                        starting_collateral:  ctx_starting_collateral,
                        available_collateral: ctx_available_collateral,
                        crypto_filter: crypto_filter.clone(),
                        market_started_at,
                        maker_market: maker_market_config_for_ctx,
                        maker_snapshot: maker_market_config.as_ref().map(|mk| MarketSnapshot {
                            yes_bid: maker_yb, yes_bid_depth: maker_ybd, yes_ask: maker_ya, yes_ask_depth: maker_yad,
                            no_bid: maker_nb, no_bid_depth: maker_nbd, no_ask: maker_na, no_ask_depth: maker_nad,
                            oracle_price: *oracle_rx.borrow(), velocity: velocity_rx.borrow().0, velocity_1s: velocity_rx.borrow().1, acceleration: velocity_rx.borrow().2,
                            funding_rate: *funding_rx.borrow(), oracle_drift_60m: drift_rx.borrow().0, oracle_drift_10m: drift_rx.borrow().1,
                            secs_to_expiry: mk.market_close_time
                                .map(|t| (t - Utc::now()).num_seconds())
                                .unwrap_or(0),
                            timestamp: maker_snap_ts,
                        }),
                        dynamic_config: dyn_cfg,
                    };

                    let eval_result = match execute_strategies_concurrent(&strategies, &ctx, 500, &mut last_executor_summary).await {
                        Ok(r) => r,
                        Err(e) => { warn!("⚠️ Strategy evaluation error: {}", e); continue; }
                    };
                    let (resolved_signals, _) = aggregate_and_resolve_signals(&eval_result);
                    if resolved_signals.is_empty() { continue; }

                    // ── Signal-processing timeout guard (45 s) ───────────────────────
                    let signal_processing_result = tokio::time::timeout(Duration::from_secs(45), async {

                    for (strategy_name, signal) in resolved_signals {

                        let sn = strategy_name.clone();
                        let (target_yes_token, target_no_token, target_market_close_time, target_is_neg_risk, target_yes_fee_bps, target_no_fee_bps) = {
                            let strategy_venue = strategies.iter().find(|s| s.name() == sn).map(|s| s.venue()).unwrap_or("Hourly");
                            if strategy_venue == "Window/Daily" && maker_market_config.is_some() {
                                let mk = maker_market_config.as_ref().unwrap();
                                (mk.yes_token.clone(), mk.no_token.clone(), mk.market_close_time, mk.is_neg_risk, mk.yes_fee_bps, mk.no_fee_bps)
                            } else {
                                (hourly_yes_token.clone(), hourly_no_token.clone(), hourly_market_close_time, hourly_is_neg_risk, hourly_yes_fee_rate, hourly_no_fee_rate)
                            }
                        };

                        match signal {
                            // ════════════════════ EXIT ════════════════════
                            StrategySignal::Exit { params, reason, exit_pair } => {
                                if let Some(lt) = last_exit_attempt_time.get(&sn) {
                                    if lt.elapsed() < Duration::from_secs(config::EXIT_RETRY_COOLDOWN_SECS) {
                                        continue;
                                    }
                                }
                                last_exit_attempt_time.insert(sn.clone(), Instant::now());
                                let tid = params.token_id;
                                let tid_m = tid.clone(); // neutral key (slice 2a)
                                let pos_key = (sn.clone(), tid_m.clone());
                                let shares = { let map = positions.lock().await; match map.get(&pos_key) { Some(p) => p.shares, None => continue } };
                                if shares < config::MIN_ORDER_SHARES || params.price <= dec!(0) {
                                    let mut map = positions.lock().await; if let Some(p) = map.remove(&pos_key) { let aep = (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE); *total_pnl.lock().await += (aep - p.avg_entry) * p.shares; } continue;
                                }
                                info!("🔴 EXIT [{}]: {} | shares={:.2}, bid=${:.4} | {}", sn, params.market_name, shares, params.price, reason);
                                let vc = if target_is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };

                                if !config::GHOST_MODE {
                                    if let Err(e) = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, &tid, Side::Sell, shares, (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE), target_yes_fee_bps as u16, params.order_type, params.post_only, 0, &shared_http).await {
                                        let es = e.to_string();
                                        if es.contains("not enough balance") || es.contains("balance: 0") || es.contains("invalid price") {
                                            let mut map = positions.lock().await; if let Some(p) = map.remove(&pos_key) { if p.fill_confirmed_at.is_some() { let aep3 = (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE); *total_pnl.lock().await += (aep3 - p.avg_entry) * p.shares; } }
                                            last_trade_time.insert(sn.clone(), Instant::now()); continue;
                                        }
                                        if es.contains("no orders found") {
                                            warn!("⚠️ EXIT FAK miss [{}]: no buyers at ${:.4} — holding position, cooldown {}s", sn, params.price, config::STOP_LOSS_COOLDOWN_SECS);
                                            last_trade_time.insert(sn.clone(), Instant::now());
                                            if reason.to_lowercase().contains("sl") || reason.to_lowercase().contains("stop") || reason.to_lowercase().contains("toxic") {
                                                last_stop_loss_time.insert(sn.clone(), Instant::now());
                                            }
                                        } else {
                                            consecutive_failures += 1;
                                        }
                                        continue;
                                    }
                                }

                                    {
                                        let re_m;
                                        let rs_m;
                                        let rc_m;
                                        let pnl_m;

                                        {
                                            let mut map = positions.lock().await;
                                            if let Some(p) = map.remove(&pos_key) {
                                                let actual_exit_price = (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE);
                                                let pnl = (actual_exit_price - p.avg_entry) * p.shares;
                                                *total_pnl.lock().await += pnl;

                                                re_m = p.avg_entry;
                                                rs_m = p.shares;
                                                rc_m = p.close_time;
                                                pnl_m = pnl;

                                                {
                                                    let sn_task = sn.clone(); let m_name = params.market_name.clone(); let sid = if tid == target_yes_token { "YES".to_string() } else { "NO".to_string() }; let rp = actual_exit_price; let r_m = reason.clone(); let asset_t = asset_lc.clone();
                                                    tokio::spawn(async move { metrics::record_trade(&asset_t, sn_task, m_name, sid, re_m, rp, rs_m, pnl_m, r_m).await; });
                                                }
                                                {
                                                    let sn_close = sn.clone(); let tid_close = tid.to_string(); let asset_c = asset_lc.clone();
                                                    tokio::spawn(async move { if let Some(pool) = db::pool_for(&asset_c) { db::close_open_position(&pool, &sn_close, &tid_close).await; } });
                                                }
                                            } else { continue; }
                                        }
                                        // Release token claim — position is fully closed.
                                        token_ownership.lock().await.remove(&tid_m);

                                    if rs_m > dec!(0) && !config::GHOST_MODE {
                                        let ps = Arc::clone(&positions); let cl = Arc::clone(&trading_client); let tp = Arc::clone(&total_pnl); let m_name = params.market_name.clone();
                                        let sn_async = sn.clone();
                                        let tid_async = tid_m.clone(); // neutral key moved into the spawn
                                        tokio::spawn(async move {
                                            tokio::time::sleep(Duration::from_millis(2500)).await;
                                            let mut req = BalanceAllowanceRequest::default(); req.asset_type = AssetType::Conditional; req.token_id = Some(u256_from_market_id(&tid_async).unwrap_or_default());
                                            let rem = match cl.balance_allowance(req).await { Ok(r) => Decimal::from_str(&r.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000), Err(_) => return };

                                            let other_strats_shares = {
                                                let map = ps.lock().await;
                                                map.iter()
                                                    .filter(|((s, t), _)| *t == tid_async && s != &sn_async)
                                                    .map(|(_, p)| p.shares)
                                                    .fold(dec!(0), |a, b| a + b)
                                            };
                                            let our_rem = (rem - other_strats_shares).max(dec!(0));

                                            if our_rem >= config::MIN_ORDER_SHARES {
                                                let fill = (rs_m - our_rem).max(dec!(0)); let aep2 = (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE); let pnlc = -((aep2 - re_m) * our_rem.min(rs_m)); *tp.lock().await += pnlc;
                                                if fill < config::MIN_ORDER_SHARES {
                                                    warn!("⚠️ PARTIAL EXIT [{}]: FAK filled 0/{:.4} shares (our_rem={:.4}, other_strats={:.4}) — re-inserting for retry.", sn_async, rs_m, our_rem, other_strats_shares);
                                                    let mut map = ps.lock().await;
                                                    if !map.contains_key(&(sn_async.clone(), tid_async.clone())) {
                                                        map.insert((sn_async.clone(), tid_async.clone()), Position { shares: our_rem, avg_entry: re_m, opened_at: Utc::now(), close_time: rc_m, market_name: m_name, pair_token_id: tid_async.clone(), fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None });
                                                    }
                                                } else { warn!("⚠️ PARTIAL EXIT [{}]: sold {:.4}/{:.4} (our_rem={:.4}, other_strats={:.4}) — re-inserting.", sn_async, fill, rs_m, our_rem, other_strats_shares); let mut map = ps.lock().await; if !map.contains_key(&(sn_async.clone(), tid_async.clone())) { map.insert((sn_async.clone(), tid_async.clone()), Position { shares: our_rem, avg_entry: re_m, opened_at: Utc::now(), close_time: rc_m, market_name: m_name, pair_token_id: tid_async.clone(), fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None }); } }
                                            }
                                        });
                                    }
                                    let mut paired_pnl = dec!(0);
                                    if exit_pair {
                                        let other_tid = if tid == target_yes_token { target_no_token.clone() } else { target_yes_token.clone() };
                                        let other_tid_m = other_tid.clone(); // neutral key (slice 2a)
                                        let pk = (sn.clone(), other_tid_m.clone()); let ps = { let map = positions.lock().await; map.get(&pk).map(|p| p.shares) };
                                        if let Some(s) = ps {
                                            let exit_snap = if target_yes_token == ctx.market.yes_token {
                                                &ctx.snapshot
                                            } else {
                                                ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot)
                                            };
                                            let other_bid = if other_tid == target_yes_token { exit_snap.yes_bid } else { exit_snap.no_bid };
                                            let other_fee_bps = if other_tid == target_yes_token { target_yes_fee_bps as u16 } else { target_no_fee_bps as u16 };
                                            let other_vc = if target_is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                                if !config::GHOST_MODE { let _ = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, other_vc, &other_tid, Side::Sell, s, (other_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE), other_fee_bps, crate::venues::core::TimeInForce::Fak, false, 0, &shared_http).await; }
                                                    let mut map = positions.lock().await; if let Some(p) = map.remove(&pk) { let actual_other_exit = (other_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE); let pnl = (actual_other_exit - p.avg_entry) * p.shares; paired_pnl = pnl; *total_pnl.lock().await += pnl;
                                                // Release paired token claim.
                                                token_ownership.lock().await.remove(&other_tid_m);
                                                {
                                                    let sn_pm = sn.clone(); let m_name = params.market_name.clone(); let sid = if other_tid == target_yes_token { "YES".to_string() } else { "NO".to_string() }; let p_avg = p.avg_entry; let o_bid = actual_other_exit; let p_shares = p.shares; let pn = pnl; let asset_t = asset_lc.clone();
                                                    tokio::spawn(async move { metrics::record_trade(&asset_t, sn_pm, m_name, sid, p_avg, o_bid, p_shares, pn, "Convergence/PairedExit".to_string()).await; });
                                                }
                                                {
                                                    let sn_cp = sn.clone(); let tid_cp = other_tid.to_string(); let asset_c = asset_lc.clone();
                                                    tokio::spawn(async move { if let Some(pool) = db::pool_for(&asset_c) { db::close_open_position(&pool, &sn_cp, &tid_cp).await; } });
                                                }
                                            }
                                        }
                                    }
                                    info!("📊 Position closed [{}]: PnL ${:.4}", sn, pnl_m + paired_pnl);
                                    if reason.to_lowercase().contains("sl")
                                        || reason.to_lowercase().contains("stop")
                                        || reason.to_lowercase().contains("toxic")
                                        || reason.to_lowercase().contains("skewcollapse")
                                    {
                                        last_stop_loss_time.insert(sn.clone(), Instant::now());
                                    }
                                    if reason.to_lowercase().contains("expir") { last_expiry_exit_time.insert(sn.clone(), Instant::now()); }
                                    last_trade_time.insert(sn.clone(), Instant::now());
                                    { let tok = tg_token.clone(); let cid = tg_chat_id.clone(); let msg = format!("🔴 EXIT [{}] {} | bid=${:.4} | reason: {} | Session PnL: ${:.4}", sn, params.market_name, params.price, reason, *total_pnl.lock().await); tokio::spawn(async move { let _ = send_notification(&tok, &cid, &msg).await; }); }
                                    { let session_pnl = *total_pnl.lock().await; tweet_trade(tw_api_key.clone(), tw_api_secret.clone(), tw_access_token.clone(), tw_access_token_secret.clone(), sn.clone(), params.market_name.clone(), re_m, params.price, reason.clone(), pnl_m + paired_pnl, session_pnl); }
                                }
                            }

                            // ════════════════════ ENTRY ════════════════════
                            StrategySignal::Entry { params, pair_params } => {
                                let token_m = params.token_id.clone(); // neutral key (slice 2a)
                                if let Some(close_time) = target_market_close_time { if (close_time - Utc::now()).num_seconds() < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; } }
                                if let Some(lt) = last_trade_time.get(&sn) { if lt.elapsed() < Duration::from_secs(config::TRADE_COOLDOWN_SECS as u64) { continue; } }
                                if let Some(lt) = last_stop_loss_time.get(&sn) { if lt.elapsed() < Duration::from_secs(config::STOP_LOSS_COOLDOWN_SECS) { continue; } }
                                if let Some(lt) = last_expiry_exit_time.get(&sn) { if lt.elapsed() < Duration::from_secs(300) { continue; } }

                                {
                                    let cd = phantom_cooldowns.lock().await;
                                    let a_key = format!("{}:{}", sn, params.token_id);
                                    let a_on_cd = cd.get(&a_key)
                                        .map(|t| t.elapsed().as_secs() < crate::helpers::balance::PHANTOM_COOLDOWN_SECS)
                                        .unwrap_or(false);
                                    let pair_on_cd = pair_params.as_ref().map(|pp| {
                                        let p_key = format!("{}:{}", sn, pp.token_id);
                                        cd.get(&p_key)
                                            .map(|t| t.elapsed().as_secs() < crate::helpers::balance::PHANTOM_COOLDOWN_SECS)
                                            .unwrap_or(false)
                                    }).unwrap_or(false);
                                    if a_on_cd || pair_on_cd { debug!("⏳ ENTRY blocked by phantom cooldown [{}] — skipping tick", sn); continue; }
                                }

                                if pair_params.is_none() {
                                    let pm = positions.lock().await;
                                    let other_token = if params.token_id == target_yes_token { target_no_token.clone() } else { target_yes_token.clone() };
                                    if pm.contains_key(&(sn.clone(), other_token.clone())) { debug!("⏳ ENTRY blocked — already hold opposite leg in same market [{}] — must exit first", sn); continue; }
                                }

                                // ── Token sovereignty check ───────────────────────────────────────
                                // O(1) registry lookup first; secondary positions scan as a
                                // consistency guard in case the registry is momentarily behind.
                                // Upgraded to WARN so cross-strategy interference is always visible
                                // in production logs — previously this was a silent debug! drop.
                                {
                                    let mut ownership = token_ownership.lock().await;
                                    if let Some(existing_owner) = ownership.get(&token_m) {
                                        if existing_owner != &sn {
                                            let current_priority = StrategyRegistry::get_strategy_priority(&sn).unwrap_or(usize::MAX);
                                            let existing_priority = StrategyRegistry::get_strategy_priority(existing_owner).unwrap_or(usize::MAX);

                                            if current_priority < existing_priority {
                                                // Current strategy has higher priority, allow it to claim
                                                warn!(
                                                    "⚠️ TOKEN SOVEREIGNTY OVERRIDE [{}]: token {} previously claimed by {} (P={}) \
                                                     — now claimed by {} (P={})",
                                                    sn, &params.token_id.to_string()[..16], existing_owner, existing_priority, sn, current_priority,
                                                );
                                                ownership.insert(token_m.clone(), sn.clone());
                                            } else {
                                                // Current strategy has lower or equal priority, reject entry.
                                                // Apply a trade cooldown so the lower-priority strategy
                                                // backs off for TRADE_COOLDOWN_SECS instead of spinning
                                                // every tick (~7,000+ rejections per hour otherwise).
                                                warn!(
                                                    "🚫 TOKEN SOVEREIGNTY [{}]: token {} already claimed by {} (P={}) \
                                                     — entry rejected (registry hit) for {} (P={})",
                                                    sn, &params.token_id.to_string()[..16], existing_owner, existing_priority, sn, current_priority,
                                                );
                                                last_trade_time.insert(sn.clone(), Instant::now());
                                                continue;
                                            }
                                        }
                                    }
                                }
                                // Secondary scan: catches the rare case where the registry hasn't
                                // been updated yet (e.g. a position was just inserted by a paired
                                // entry in the same tick) and another strategy is scanning the same
                                // positions map concurrently (shouldn't happen in the single-threaded
                                // tick loop, but belt-and-suspenders).
                                {
                                    let pm = positions.lock().await;
                                    if pm.iter().any(|((other_sn, tid), _)| *tid == token_m && other_sn != &sn) {
                                        warn!(
                                            "🚫 TOKEN SOVEREIGNTY [{}]: token {} held by another strategy \
                                             (registry miss — positions scan fallback)",
                                            sn, params.token_id,
                                        );
                                        last_trade_time.insert(sn.clone(), Instant::now());
                                        continue;
                                    }
                                }

                                let pos_key = (sn.clone(), token_m.clone());

                                {
                                    let pending = pending_orders.lock().await;
                                    if let Some(expiry) = pending.get(&pos_key) { if expiry > &Instant::now() { continue; } }
                                }

                                    if config::GHOST_MODE {
                                    if positions.lock().await.contains_key(&pos_key) { continue; }
                                    let pos_close_time = target_market_close_time;
                                    let actual_entry_price = if params.post_only { params.price } else { (params.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE) };
                                    positions.lock().await.insert(pos_key.clone(), Position { shares: params.shares, avg_entry: actual_entry_price, opened_at: Utc::now(), close_time: pos_close_time, market_name: params.market_name.clone(), pair_token_id: token_m.clone(), fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: pair_params.as_ref().map(|p| p.token_id.clone()) });
                                    token_ownership.lock().await.insert(token_m.clone(), sn.clone());
                                    let side_g = if params.token_id == target_yes_token { "YES" } else { "NO" };
                                    info!("👻 GHOST_MODE ENTRY {} [{}]: {} | ${:.4} x {:.1} (simulated)", side_g, sn, params.market_name, params.price, params.shares);
                                    { let side_g = if params.token_id == target_yes_token { "YES" } else { "NO" }; let sn_g = sn.clone(); let tid_g = params.token_id.to_string(); let mn_g = params.market_name.clone(); let side_gs = side_g.to_string(); let ep_g = actual_entry_price; let sh_g = params.shares; let asset_g = asset_lc.clone(); tokio::spawn(async move { metrics::record_entry(&asset_g, sn_g, tid_g, mn_g, side_gs, ep_g, sh_g).await; }); }
                                    if let Some(pool) = db::pool_for(&asset_lc) { let side_g = if params.token_id == target_yes_token { "YES" } else { "NO" }; db::record_open_position(&pool, &sn, &params.token_id.to_string(), &params.market_name, side_g, actual_entry_price, params.shares, true).await; }
                                    if let Some(pp) = pair_params {
                                        let pp_close_time = target_market_close_time;
                                        let actual_paired_entry_price = if pp.post_only {
                                            pp.price
                                        } else {
                                            (pp.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE)
                                        };
                                        positions.lock().await.insert((sn.clone(), pp.token_id.clone()), Position { shares: pp.shares, avg_entry: actual_paired_entry_price, opened_at: Utc::now(), close_time: pp_close_time, market_name: pp.market_name.clone(), pair_token_id: pp.token_id.clone(), fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: Some(token_m.clone()) });
                                        token_ownership.lock().await.insert(pp.token_id.clone(), sn.clone());
                                        let side_gp = if pp.token_id == target_yes_token { "YES" } else { "NO" };
                                        info!("👻 GHOST_MODE ENTRY {} (paired) [{}]: {} | ${:.4} x {:.1} (simulated)", side_gp, sn, pp.market_name, pp.price, pp.shares);
                                        { let side_gp = if pp.token_id == target_yes_token { "YES" } else { "NO" }; let sn_gp = sn.clone(); let tid_gp = pp.token_id.to_string(); let mn_gp = pp.market_name.clone(); let side_gps = side_gp.to_string(); let ep_gp = actual_paired_entry_price; let sh_gp = pp.shares; let asset_gp = asset_lc.clone(); tokio::spawn(async move { metrics::record_entry(&asset_gp, sn_gp, tid_gp, mn_gp, side_gps, ep_gp, sh_gp).await; }); }
                                        if let Some(pool) = db::pool_for(&asset_lc) { let side_gp = if pp.token_id == target_yes_token { "YES" } else { "NO" }; db::record_open_position(&pool, &sn, &pp.token_id.to_string(), &pp.market_name, side_gp, actual_paired_entry_price, pp.shares, true).await; }
                                    }
                                    last_trade_time.insert(sn.clone(), Instant::now());
                                } else {
                                    let actual_entry_price = if params.post_only { params.price } else { (params.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE) };
                                    {
                                        let mut map = positions.lock().await; if map.contains_key(&pos_key) { continue; }
                                        let pos_close_time = target_market_close_time;
                                        map.insert(pos_key.clone(), Position { shares: params.shares, avg_entry: actual_entry_price, opened_at: Utc::now(), close_time: pos_close_time, market_name: params.market_name.clone(), pair_token_id: token_m.clone(), fill_confirmed_at: None, paired_leg_token_id: pair_params.as_ref().map(|p| p.token_id.clone()) });
                                    }
                                    // Claim token in ownership registry immediately — prevents any
                                    // concurrent strategy tick from racing into the same token
                                    // between this insert and the order placement below.
                                    token_ownership.lock().await.insert(token_m.clone(), sn.clone());
                                    { pending_orders.lock().await.insert(pos_key.clone(), Instant::now() + Duration::from_secs(3)); }
                                    info!("🟢 ENTRY [{}]: {} | ${:.4} x {:.1}", sn, params.market_name, params.price, params.shares);
                                    let primary_baseline = {
                                        let mut req = BalanceAllowanceRequest::default(); req.asset_type = AssetType::Conditional; req.token_id = Some(u256_from_market_id(&params.token_id).unwrap_or_default());
                                        match tokio::time::timeout(Duration::from_secs(10), trading_client.balance_allowance(req)).await {
                                            Ok(Ok(resp)) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
                                            Ok(Err(e)) => { warn!("⚠️ entry baseline balance_allowance error [{}]: {}", sn, e); dec!(0) }
                                            Err(_) => { warn!("⚠️ entry baseline balance_allowance timed out (10s) [{}]", sn); dec!(0) }
                                        }
                                    };
                                    let vc = if target_is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };

                                    if let Some(pp) = pair_params {
                                        let pp_token_m = pp.token_id.clone(); // neutral key (slice 2a)
                                        let actual_pair_entry_price = if pp.post_only { pp.price } else { (pp.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE) };
                                        let vc_p = if pp.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                        let pair_baseline = {
                                            let mut req = BalanceAllowanceRequest::default(); req.asset_type = AssetType::Conditional; req.token_id = Some(u256_from_market_id(&pp.token_id).unwrap_or_default());
                                            match tokio::time::timeout(Duration::from_secs(10), trading_client.balance_allowance(req)).await {
                                                Ok(Ok(resp)) => Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000),
                                                Ok(Err(e)) => { warn!("⚠️ pair baseline balance_allowance error [{}]: {}", sn, e); dec!(0) }
                                                Err(_) => { warn!("⚠️ pair baseline balance_allowance timed out (10s) [{}]", sn); dec!(0) }
                                            }
                                        };

                                        if primary_baseline >= config::MIN_ORDER_SHARES || pair_baseline >= config::MIN_ORDER_SHARES {
                                            warn!("🛡️ Paired entry BLOCKED [{}]: orphan accumulation guard — primary on-chain={:.4} pair on-chain={:.4} for \"{}\" (re-checking in {}s)", sn, primary_baseline, pair_baseline, params.market_name, crate::helpers::balance::PHANTOM_COOLDOWN_SECS);
                                            positions.lock().await.remove(&pos_key);
                                            pending_orders.lock().await.remove(&pos_key);
                                            // Release both token claims — entry was blocked by orphan guard.
                                            {
                                                let mut own = token_ownership.lock().await;
                                                own.remove(&token_m);
                                                own.remove(&pp_token_m);
                                            }
                                            { let mut cd = phantom_cooldowns.lock().await; cd.insert(format!("{}:{}", sn, params.token_id), tokio::time::Instant::now()); cd.insert(format!("{}:{}", sn, pp.token_id), tokio::time::Instant::now()); }
                                            last_trade_time.insert(sn.clone(), Instant::now());
                                            continue;
                                        }

                                        match place_limit_orders_atomic(
                                            &trading_client, &nonce_manager, &signer,
                                            safe_address, eoa_address,
                                            vc, &params.token_id, Side::Buy, params.shares, actual_entry_price, params.order_type.clone(), params.post_only, 0,
                                            vc_p, &pp.token_id, Side::Buy, pp.shares, actual_pair_entry_price, pp.order_type.clone(), pp.post_only, 0,
                                            &shared_http,
                                        ).await {
                                            Err(e) => {
                                                warn!("⚠️ Arb batch entry FAILED [{}]: {} — no orders placed", sn, e);
                                                positions.lock().await.remove(&pos_key);
                                                pending_orders.lock().await.remove(&pos_key);
                                                // Release both token claims — order was never sent.
                                                {
                                                    let mut own = token_ownership.lock().await;
                                                    own.remove(&token_m);
                                                    own.remove(&pp_token_m);
                                                }
                                                last_trade_time.insert(sn.clone(), Instant::now());
                                                consecutive_failures += 1; continue;
                                            }
                                            Ok((leg_a_id, leg_b_id)) => {
                                                let primary_wait_secs = if target_yes_token == hourly_yes_token { crate::helpers::balance::MAX_WAIT_SECS_HOURLY } else { crate::helpers::balance::MAX_WAIT_SECS_WINDOW };
                                                let cl_s = Arc::clone(&trading_client); let ps_s = Arc::clone(&positions); let pc_s = Arc::clone(&phantom_cooldowns); let to_s = Arc::clone(&token_ownership); let sn_s = sn.clone(); let tn_s = params.token_id.clone();
                                                let db_sn_a = sn.clone(); let db_tid_a = params.token_id.to_string(); let db_mn_a = params.market_name.clone();
                                                let db_side_a = if params.token_id == target_yes_token { "YES" } else { "NO" }; let db_ep_a = actual_entry_price; let db_sh_a = params.shares; let asset_a = asset_lc.clone();
                                                // Write pending position immediately (Viper Launch)
                                                if let Some(pool) = db::pool_for(&asset_a) {
                                                    db::record_open_position_with_status(&pool, &sn, &db_tid_a, &db_mn_a, db_side_a, db_ep_a, db_sh_a, false, "pending").await;
                                                }
                                                tokio::spawn(async move {
                                                    if sync_position_balance(&cl_s, &ps_s, &sn_s, &tn_s, Some(&pc_s), primary_baseline, primary_wait_secs, &to_s).await.is_ok() {
                                                        // Update to confirmed (Mission In-Flight) + record entry
                                                        if let Some(pool) = db::pool_for(&asset_a) {
                                                            db::confirm_position_status(&pool, &db_sn_a, &db_tid_a).await;
                                                        }
                                                        metrics::record_entry(&asset_a, db_sn_a, db_tid_a, db_mn_a, db_side_a.to_string(), db_ep_a, db_sh_a).await;
                                                    }
                                                });

                                        let pp_close_time = target_market_close_time;
                                        positions.lock().await.insert((sn.clone(), pp_token_m.clone()), Position { shares: pp.shares, avg_entry: actual_pair_entry_price, opened_at: Utc::now(), close_time: pp_close_time, market_name: pp.market_name.clone(), pair_token_id: pp_token_m.clone(), fill_confirmed_at: None, paired_leg_token_id: Some(token_m.clone()) });
                                        // Claim paired token in registry.
                                        token_ownership.lock().await.insert(pp_token_m.clone(), sn.clone());

                                                let pair_wait_secs = if pp.token_id == hourly_yes_token || pp.token_id == hourly_no_token { crate::helpers::balance::MAX_WAIT_SECS_HOURLY } else { crate::helpers::balance::MAX_WAIT_SECS_WINDOW };
                                                let sn_p = sn.clone(); let tn_p = pp.token_id.clone(); let ps_p = Arc::clone(&positions); let cl_p = Arc::clone(&trading_client); let pc_p = Arc::clone(&phantom_cooldowns); let to_p = Arc::clone(&token_ownership);
                                                let db_sn_b = sn.clone(); let db_tid_b = pp.token_id.to_string(); let db_mn_b = pp.market_name.clone();
                                                let db_side_b = if pp.token_id == target_yes_token { "YES" } else { "NO" }; let db_ep_b = actual_pair_entry_price; let db_sh_b = pp.shares; let asset_b = asset_lc.clone();
                                                // Write pending position immediately (Viper Launch)
                                                if let Some(pool) = db::pool_for(&asset_b) {
                                                    db::record_open_position_with_status(&pool, &sn, &db_tid_b, &db_mn_b, db_side_b, db_ep_b, db_sh_b, false, "pending").await;
                                                }
                                                tokio::spawn(async move {
                                                    if sync_position_balance(&cl_p, &ps_p, &sn_p, &tn_p, Some(&pc_p), pair_baseline, pair_wait_secs, &to_p).await.is_ok() {
                                                        // Update to confirmed (Mission In-Flight) + record entry
                                                        if let Some(pool) = db::pool_for(&asset_b) {
                                                            db::confirm_position_status(&pool, &db_sn_b, &db_tid_b).await;
                                                        }
                                                        metrics::record_entry(&asset_b, db_sn_b, db_tid_b, db_mn_b, db_side_b.to_string(), db_ep_b, db_sh_b).await;
                                                    }
                                                });

                                                {
                                                    let arb_cl = Arc::clone(&trading_client); let arb_nm = Arc::clone(&nonce_manager); let arb_sg = signer.clone(); let arb_ps = Arc::clone(&positions); let arb_pc = Arc::clone(&phantom_cooldowns); let arb_sn = sn.clone(); let arb_http = shared_http.clone();
                                                    let arb_tok_a = params.token_id.clone(); let arb_tok_b = pp.token_id.clone(); let arb_base_a = primary_baseline; let arb_base_b = pair_baseline;
                                                    let arb_side_a = if params.token_id == target_yes_token { "YES" } else { "NO" }.to_string();
                                                    let arb_side_b = if pp.token_id == target_yes_token { "YES" } else { "NO" }.to_string();
                                                    let arb_wait = if sn.contains("TimeDecay") {
                                                        // TimeDecay resting maker bids need the full theta window
                                                        // (up to TIME_DECAY_MAX_SECS_TO_EXPIRY = 1800s) to fill.
                                                        // Using MAX_WAIT_SECS_HOURLY (180s) caused the arbiter to
                                                        // declare orphan after 3 minutes while the GTC bid was still
                                                        // resting. Match the wait to the theta window so both legs
                                                        // get a fair chance before any orphan flatten fires.
                                                        crate::config::TIME_DECAY_MAX_SECS_TO_EXPIRY
                                                    } else {
                                                        primary_wait_secs.max(pair_wait_secs)
                                                    };
                                                    let arb_asset = asset_lc.clone();
                                                    tokio::spawn(async move {
                                                        crate::helpers::balance::arb_pair_fill_monitor(
                                                            arb_cl, arb_nm, arb_sg, safe_address, eoa_address, vc, vc_p,
                                                            arb_ps, arb_pc, arb_sn, &arb_tok_a, &arb_tok_b,
                                                            arb_base_a, arb_base_b, arb_side_a, arb_side_b, arb_wait, arb_http, arb_asset,
                                                        ).await;
                                                    });
                                                }

                                                // ── Slice 3: register legs with lifecycle engine ───────────────
                                                // Register the new arb pair in the venue's active-token set and
                                                // track both GTC orders with the shared OrderLifecycle so the
                                                // 30 s reconcile loop can confirm fills, cancel stale legs, and
                                                // flatten naked legs independent of arb_pair_fill_monitor.
                                                patrol_venue.register_tokens(
                                                    &[params.token_id.clone(), pp.token_id.clone()]
                                                ).await;
                                                lifecycle.track(
                                                    &crate::venues::core::Fill {
                                                        order_id: crate::venues::core::OrderId(leg_a_id),
                                                        market: params.token_id.clone(),
                                                        filled: params.shares,
                                                        price: actual_entry_price,
                                                    },
                                                    &sn,
                                                    crate::venues::core::TimeInForce::Gtc,
                                                    Some(pp.token_id.clone()),
                                                ).await;
                                                lifecycle.track(
                                                    &crate::venues::core::Fill {
                                                        order_id: crate::venues::core::OrderId(leg_b_id),
                                                        market: pp.token_id.clone(),
                                                        filled: pp.shares,
                                                        price: actual_pair_entry_price,
                                                    },
                                                    &sn,
                                                    crate::venues::core::TimeInForce::Gtc,
                                                    Some(params.token_id.clone()),
                                                ).await;
                                            }
                                        }
                                    } else {
                                        let leg_a_order_id = match place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, &params.token_id, Side::Buy, params.shares, actual_entry_price, target_yes_fee_bps as u16, params.order_type, params.post_only, 0, &shared_http).await {
                                            Err(e) => { warn!("⚠️ ENTRY order failed [{}]: {}", sn, e); positions.lock().await.remove(&pos_key); pending_orders.lock().await.remove(&pos_key); token_ownership.lock().await.remove(&token_m); last_trade_time.insert(sn.clone(), Instant::now()); consecutive_failures += 1; continue; }
                                            Ok(id) => id,
                                        };
                                        let _ = leg_a_order_id;
                                        let cl_s = Arc::clone(&trading_client); let ps_s = Arc::clone(&positions); let pc_s = Arc::clone(&phantom_cooldowns); let to_s = Arc::clone(&token_ownership); let sn_s = sn.clone(); let tn_s = params.token_id.clone();
                                        let primary_wait_secs = if target_yes_token == hourly_yes_token { crate::helpers::balance::MAX_WAIT_SECS_HOURLY } else { crate::helpers::balance::MAX_WAIT_SECS_WINDOW };
                                        let db_sn_s = sn.clone(); let db_tid_s = params.token_id.to_string(); let db_mn_s = params.market_name.clone();
                                        let db_side_s = if params.token_id == target_yes_token { "YES" } else { "NO" }; let db_ep_s = actual_entry_price; let db_sh_s = params.shares; let asset_s = asset_lc.clone();
                                        // Write pending position immediately (Viper Launch)
                                        if let Some(pool) = db::pool_for(&asset_s) {
                                            db::record_open_position_with_status(&pool, &sn, &db_tid_s, &db_mn_s, db_side_s, db_ep_s, db_sh_s, false, "pending").await;
                                        }
                                        tokio::spawn(async move {
                                            if sync_position_balance(&cl_s, &ps_s, &sn_s, &tn_s, Some(&pc_s), primary_baseline, primary_wait_secs, &to_s).await.is_ok() {
                                                // Update to confirmed (Mission In-Flight) + record entry
                                                if let Some(pool) = db::pool_for(&asset_s) {
                                                    db::confirm_position_status(&pool, &db_sn_s, &db_tid_s).await;
                                                }
                                                metrics::record_entry(&asset_s, db_sn_s, db_tid_s, db_mn_s, db_side_s.to_string(), db_ep_s, db_sh_s).await;
                                            }
                                        });
                                    }
                                    last_trade_time.insert(sn.clone(), Instant::now());
                                    { let tok = tg_token.clone(); let cid = tg_chat_id.clone(); let msg = format!("🟢 ENTRY [{}] {} | ${:.4} x {:.1}", sn, params.market_name, params.price, params.shares); tokio::spawn(async move { let _ = send_notification(&tok, &cid, &msg).await; }); }
                                }
                            }

                            // ════════════════════ MAKER QUOTE ════════════════════
                            StrategySignal::MakerQuote { yes, no } => {
                                let mut placed = false;
                                for p in [yes, no].into_iter().flatten() {
                                    let p_token_m = p.token_id.clone(); // neutral key (slice 2a)
                                    let pk = (sn.clone(), p_token_m.clone());
                                    { let pending = pending_orders.lock().await; if let Some(expiry) = pending.get(&pk) { if expiry > &Instant::now() { continue; } } }
                                    if config::GHOST_MODE {
                                        if positions.lock().await.contains_key(&pk) { continue; }
                                        positions.lock().await.insert(pk.clone(), Position { shares: p.shares, avg_entry: p.price, opened_at: Utc::now(), close_time: None, market_name: p.market_name.clone(), pair_token_id: p_token_m.clone(), fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: None });
                                        info!("👻 GHOST_MODE MakerQuote [{}]: {} | shares={:.2}, bid=${:.4} (simulated)", sn, p.market_name, p.shares, p.price);
                                        placed = true;
                                    } else {
                                        if !positions.lock().await.contains_key(&pk) {
                                            info!("📝 MakerQuote [{}]: {} | shares={:.2}, bid=${:.4}", sn, p.market_name, p.shares, p.price);
                                            positions.lock().await.insert(pk.clone(), Position { shares: p.shares, avg_entry: p.price, opened_at: Utc::now(), close_time: None, market_name: p.market_name.clone(), pair_token_id: p_token_m.clone(), fill_confirmed_at: None, paired_leg_token_id: None });
                                            token_ownership.lock().await.insert(p_token_m.clone(), sn.clone());
                                            { pending_orders.lock().await.insert(pk.clone(), Instant::now() + Duration::from_secs(3)); }
                                            let _ = tokio::time::timeout(Duration::from_secs(10), crate::helpers::balance::quick_confirm_fill(&trading_client, &sn, &p.token_id, &positions, &p.condition_id, p.order_type.clone())).await;
                                            let vc = if p.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                            if let Err(e) = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, &p.token_id, Side::Buy, p.shares, p.price, target_yes_fee_bps as u16, p.order_type, true, 0, &shared_http).await {
                                                positions.lock().await.remove(&pk);
                                                pending_orders.lock().await.remove(&pk);
                                                // Release token claim — order placement failed.
                                                token_ownership.lock().await.remove(&p_token_m);
                                                if !e.to_string().contains("crosses book") { consecutive_failures += 1; } continue;
                                            }
                                            let cl_m = Arc::clone(&trading_client); let ps_m = Arc::clone(&positions); let pc_m = Arc::clone(&phantom_cooldowns); let to_m = Arc::clone(&token_ownership); let sn_m = sn.clone();
                                            let tid_em = p.token_id.to_string(); let mn_em = p.market_name.clone();
                                            let side_em = if p.token_id == target_yes_token { "YES" } else { "NO" }.to_string();
                                            let ep_em = p.price; let sh_em = p.shares; let asset_em = asset_lc.clone();
                                            // Write pending position immediately (Viper Launch)
                                            if let Some(pool) = db::pool_for(&asset_em) {
                                                db::record_open_position_with_status(&pool, &sn, &tid_em, &mn_em, &side_em, ep_em, sh_em, false, "pending").await;
                                            }
                                            tokio::spawn(async move {
                                                if sync_position_balance(&cl_m, &ps_m, &sn_m, &p.token_id, Some(&pc_m), dec!(0), crate::helpers::balance::MAX_WAIT_SECS_WINDOW, &to_m).await.is_ok() {
                                                    // Update to confirmed (Mission In-Flight) + record entry
                                                    if let Some(pool) = db::pool_for(&asset_em) {
                                                        db::confirm_position_status(&pool, &sn_m, &tid_em).await;
                                                    }
                                                    metrics::record_entry(&asset_em, sn_m, tid_em, mn_em, side_em, ep_em, sh_em).await;
                                                }
                                            });
                                        }
                                        placed = true;
                                    }
                                }
                                if placed { last_trade_time.insert(sn.clone(), Instant::now()); }
                                if consecutive_failures >= config::MAX_CONSECUTIVE_FAILURES { error!("🚨 Circuit breaker hit!"); tokio::time::sleep(Duration::from_secs(60)).await; consecutive_failures = 0; }
                            }
                            StrategySignal::NoSignal => {}
                        }
                    }
                    Ok::<(), ()>(())
                    }).await;
                    if signal_processing_result.is_err() {
                        warn!("⚠️ Signal processing timed out (45s) — select! loop unblocked, watchdog/heartbeat resume");
                    }
                }
            }
        }

        // ── Tear-down: stop all peripheral tasks ─────────────────────────────
        // Clear the venue's active-token registry so the lifecycle task does not
        // query stale tokens during the brief window between peripheral_cancel
        // firing and the lifecycle task exiting.
        ctx.session.venue.clear_active_tokens().await;
        peripheral_cancel.cancel();
    }
}
