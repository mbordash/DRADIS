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
use crate::state::TradeScope;
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

/// How often a resting maker exit's on-chain balance is polled to detect that
/// the ask was lifted. The patrol tick is 50ms; polling the chain at that rate
/// would be absurd, and a few seconds of booking latency costs nothing because
/// the fill price is already known (it is the resting limit, which cannot slip).
const MAKER_RESTING_EXIT_POLL_SECS: u64 = 5;

/// Book an exit the exchange says already happened, from the money.
///
/// Returns `(exit_price, pnl)`, or `None` when the figure cannot be trusted —
/// in which case the caller books zero rather than inventing a number. Never
/// falls back to the order book: this path is only reachable from a stop or a
/// take-profit whose sell was rejected, and a book-derived price is a guess.
///
/// `baseline` is collateral as it read BEFORE the entry was placed, so
/// `now - baseline` is the round trip's net P&L directly — no division, and no
/// assumption about what the shares fetched. That is deliberate. The first
/// version of this reconstructed a per-share exit price from a supposedly
/// post-fill baseline, but `live_collateral` refreshes only once a minute, so
/// the "post-fill" reading was routinely taken before the buy had even settled
/// (production trade 378, 2026-08-20: baseline $67.0050 against an entry placed
/// 17s later). Dividing that by the share count produced a meaningless price.
///
/// Two guards, and between them they also disambiguate a baseline that landed on
/// the wrong side of the entry: a post-buy baseline makes `pnl` the gross
/// proceeds instead of the net, which pushes the derived price far outside a
/// binary's range and is rejected here.
///
///   * the derived price must be a possible binary price, and
///   * it must sit within `max_dev` of the bid observed at exit — the only
///     contemporaneous evidence of what the shares were worth. Measuring against
///     the ENTRY price instead was the other half of the trade-378 bug: FairValue
///     routinely takes 20%+ profits, so a correct reconciliation looked like a
///     wild outlier and got thrown away.
fn reconcile_unverified_exit(
    baseline: Option<Decimal>,
    now_collateral: Decimal,
    shares: Decimal,
    avg_entry: Decimal,
    observed_bid: Decimal,
    max_dev: Decimal,
) -> Option<(Decimal, Decimal)> {
    let base = baseline?;
    if shares <= dec!(0) { return None; }
    let pnl = now_collateral - base;
    let exit_price = avg_entry + pnl / shares;
    if exit_price <= dec!(0) || exit_price > dec!(1) { return None; }
    if (exit_price - observed_bid).abs() > max_dev { return None; }
    Some((exit_price, pnl))
}

/// Maker quote epochs: `(strategy, token) -> (epoch, last touched)`.
type QuoteEpochs = std::collections::HashMap<PositionKey, (u64, Instant)>;

/// Retire whatever quote currently owns `key` and return the new epoch.
///
/// Called both when a quote is PLACED (claiming an epoch for its own fill
/// watcher) and when one is PULLED (so the pulled quote's watcher can no longer
/// claim a later fill).
fn bump_quote_epoch(map: &mut QuoteEpochs, key: &PositionKey) -> u64 {
    let e = map.entry(key.clone()).or_insert((0, Instant::now()));
    e.0 += 1;
    e.1 = Instant::now();
    e.0
}

/// Close simulated position rows for the market a squadron is leaving.
///
/// The rotation half of the ghost-row leak. A ghost row is excluded from the chain
/// reconciler and from `purge_stale_open_positions` by design, which leaves market
/// expiry as the only moment it can be closed — and `cleanup_expired_positions`
/// only looks at the market the squadron is CURRENTLY running. Rotation breaks this
/// loop and redeploys against new tokens before that can happen, so the row is left
/// open forever. Observed on the v1.0.9 production instance, 2026-08-31: a paper
/// position opened on the 2AM ET market was still an open row nine hours and five
/// rotations later.
///
/// The in-memory entry is NOT dropped at rotation — it lingers in the session map
/// as a zombie. What makes the row safe to delete is not that the position is gone
/// from memory but that it is UNEXITABLE: once its market has rotated away,
/// `venue_for_token` can no longer resolve the token to a book, so no viper will
/// ever act on it again. Call this only for tokens whose market genuinely rotated.
///
/// Called where the tokens being left behind are still known, which is why it lives
/// here rather than in `Squadron::stand_down` — that has the squadron but not the
/// market it was trading.
async fn close_ghost_rows_for_market(
    asset: &str,
    tokens: &[&crate::venues::core::MarketId],
) {
    let Some(pool) = db::pool_for(asset) else { return };
    for token in tokens {
        db::close_ghost_open_position(&pool, &token.to_string()).await;
    }
}

/// Is `epoch` still the live quote for `key`?
///
/// A fill watcher spawned for a quote must ask this before recording an entry.
/// Answering `false` means the quote it was watching was pulled or replaced, and
/// the shares it can see belong to a later quote.
fn quote_epoch_is_current(map: &QuoteEpochs, key: &PositionKey, epoch: u64) -> bool {
    map.get(key).map(|(e, _)| *e) == Some(epoch)
}

/// One live post-only GTC ask resting against a filled maker position.
///
/// The position stays in the position map while the ask rests — it is still
/// owned, still marked, and still subject to every stop. This record only tracks
/// the ORDER so the patrol can reprice it, cancel it before a FAK exit needs the
/// shares, and book the exit at the exact resting price when it is lifted.
#[derive(Debug, Clone)]
struct MakerRestingExit {
    /// Limit price of the resting ask — also the exact exit price when lifted,
    /// since a resting limit order cannot slip.
    price: Decimal,
    /// Share count the position held when the ask was placed. A drop below this
    /// on-chain means the ask (partially) filled. Re-based to the remainder each
    /// time the sweep books a partial lift.
    shares: Decimal,
    /// Shares of THIS ask the sweep has already written to the ledger.
    ///
    /// The venue reports an open order's `size_matched` cumulatively since
    /// placement, so a path that reads it (reprice, stop, stand-down) must take
    /// the sweep's bookings off first or it re-books them. 2026-09-03 trade 5
    /// did exactly that: a 5.31-share partial the sweep had booked 21s earlier
    /// was counted again at reprice.
    booked: Decimal,
    /// Entry price captured at placement, for P&L on fill.
    avg_entry: Decimal,
    /// Entry fee still unbooked on the position at placement, in dollars.
    ///
    /// Zero for a maker-entered position (the Maker's quotes pay nothing), and
    /// the taker fee for a FairValue position that crossed the spread to get
    /// in. The lift is booked net of it — without this a taker-entered
    /// position's resting exit reported the gross move as profit, the same
    /// shape as B35. The live position is the authority while it exists;
    /// this copy serves only once the position is gone from the map.
    entry_fee: Decimal,
    market_name: String,
    /// Last time the on-chain balance was polled for this token.
    last_poll: Instant,
    /// Consecutive polls that read a share count below `shares`.
    ///
    /// `onchain_balance_for_token` returns 0 on ANY failure (timeout, RPC error)
    /// and the balance endpoint independently lags a real fill, so ONE short read
    /// is not evidence of a lift. Requiring agreement across consecutive polls
    /// gets that safety without blocking the 50ms patrol loop in a retry sleep.
    short_reads: u32,
    /// Largest balance seen across the current run of short reads — the exit is
    /// sized from this rather than the latest reading, so a transient 0 cannot
    /// inflate the booked quantity.
    max_short_read: Decimal,
}

/// Consecutive short balance reads required before a resting ask is booked as
/// lifted. Two polls, `MAKER_RESTING_EXIT_POLL_SECS` apart.
const MAKER_RESTING_EXIT_FILL_CONFIRMATIONS: u32 = 2;

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
        let arb_market_lockouts   = ctx.session.arb_market_lockouts.clone();
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
        // Filing dimensions for this squadron's rows. On the intl CLOB the shard
        // key and the underlying coincide — this venue is where the two concepts
        // were originally conflated — but they are recorded separately so the
        // tradelog reads the same across venues. See `state::TradeScope`.
        // Resolved once and reused: `scope` files every row under it, and the
        // viper set below is selected from it.
        let market_class = self.classify_and_link().await;
        let mut scope = TradeScope::new(
            asset_lc.clone(),
            crate::venues::intl::INTL_VENUE,
            Some(market_class.clone()),
            Some(asset_lc.clone()),
        );

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
        let consecutive_stop_losses = &mut ctx.consecutive_stop_losses;

        // Market rotation CIDs
        let current_hourly_cid = self.market.condition_id.clone();
        let current_maker_cid  = ctx.maker_market_config
            .as_ref()
            .map_or_else(String::new, |m| m.condition_id.clone());

        // One market, no split, no rotation — see `Squadron::single_market`. Read
        // once here because both consumers below sit inside the tick loop.
        let single_market = self.single_market;
        // Throttles the "waiting to go flat before retiring" line to once a
        // minute; the patrol tick is 50ms. Starts a full interval in the past so
        // the first one prints immediately.
        let mut retire_wait_logged_at = Instant::now()
            .checked_sub(Duration::from_secs(60))
            .unwrap_or_else(Instant::now);

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
        // Funding is optional BY CONSTRUCTION: `SquadronRaptors::empty()` and
        // `sports_only()` both leave it None, and between them those cover every
        // politics and sports squadron. This used to
        // `.expect("funding raptor always present")`, so the first tick of any
        // non-crypto squadron panicked the patrol task — the comment asserted an
        // invariant the type had already refused to make.
        //
        // A neutral 0% rate is the right stand-in: funding is a crypto-perp
        // signal with no meaning on an election or a tennis match, and the
        // strategies that read it treat zero as "no skew to fade". The dropped
        // sender matches how `empty()` builds its other idle feeds — nothing
        // waits on a change, `borrow()` just keeps returning the neutral value.
        let funding_rx = self.raptors.funding.clone().unwrap_or_else(|| {
            let (_neutral_tx, rx) = tokio::sync::watch::channel(Decimal::ZERO);
            rx
        });
        // Tide Raptor is optional (BTC-only); `None` for ETH/SOL squadrons and
        // momentum-only deployments. Read into a local so no borrow guard is held
        // across an .await when the snapshot is built below.
        let tide_rx = self.raptors.tide.clone();
        // Horizon Raptor is optional (shares the Alpaca WS with Tide; absent when
        // undeployed). Same borrow-guard discipline as `tide_rx`.
        let horizon_rx = self.raptors.horizon.clone();
        // Derivatives Raptor is optional (all-asset, but absent on price-only
        // deployments). Same borrow-guard discipline as `tide_rx`.
        let deriv_rx = self.raptors.derivatives.clone();

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
            self.id.clone(),
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
            // Filing dimensions for any row the cleanup task writes (orphan
            // re-hedge). Only venue/class/underlying are read from this clone;
            // its ghost flag is NOT — the task's writes carry their own.
            scope.clone(),
            peripheral_cancel.clone(),
        );

        spawn_status_task(
            Arc::clone(&live_collateral),
            Arc::clone(&total_pnl),
            Arc::clone(&trading_client),
            yes_price_rx.clone(),
            no_price_rx.clone(),
            maker_yes_price_rx.clone(),
            maker_no_price_rx.clone(),
            // Omitted for squadrons with no Price Raptor — see `has_price_feed`.
            self.raptors.has_price_feed.then(|| oracle_rx.clone()),
            Arc::clone(&process_heartbeat_secs),
            asset_lc.clone(),
            ctx.market_rx.clone(),
            peripheral_cancel.clone(),
        );

        // Squadron identity, cloned once for the whole patrol. Every PositionKey
        // built below carries it, so this squadron addresses its own positions
        // and not those of another squadron holding the same token.
        let squadron_id = self.id.clone();

        // ── Slice 3: shared OrderLifecycle (intl migration) ──────────────────
        // One engine drives fill-confirm, stale-cancel, and naked-leg flatten
        // over the Execution trait surface. Runs alongside the existing bespoke
        // arb_pair_fill_monitor / sync_position_balance paths (additive for now).
        let lifecycle = std::sync::Arc::new(OrderLifecycle::new(LifecycleConfig::intl(), self.id.clone()));
        spawn_lifecycle_task(
            std::sync::Arc::clone(&lifecycle),
            std::sync::Arc::clone(&ctx.session.venue),
            Arc::clone(&positions),
            peripheral_cancel.clone(),
            asset_lc.clone(),
        );

        // Watchdog: fires cancel (the patrol token) on stall, stops on peripheral_cancel.
        spawn_watchdog_task(
            Arc::clone(&last_heartbeat_at),
            cancel.clone(),
            peripheral_cancel.clone(),
        );

        // Run exactly the vipers this market's class calls for.
        //
        // The class was already resolved above for `scope`, and `vipers_for_class`
        // is the same taxonomy the Setup view edits and `/api/squadrons` reports.
        // Until now the intl CLOB alone ignored it and built all nine: the US and
        // Kalshi traders each filtered with a private copy of the same helper,
        // and this path had none. On the live Ireland box that put nine vipers on
        // a tennis match and a political market whose taxonomy allows two, and
        // the only reason it did no harm is that the seven crypto strategies
        // idled on missing oracle data rather than by design — TimeDecay, which
        // needs no oracle, was one theta window away from resting bids on
        // "Trump out as President by August 31?".
        let viper_kinds = match db::pool_for(&asset_lc) {
            Some(p) => crate::helpers::db::vipers_for_class(&p, &market_class).await,
            None => Vec::new(),
        };
        let strategies = StrategyRegistry::create_strategies_for_kinds(&viper_kinds);
        info!(
            "🎯 Squadron [{}] will run {} viper(s) for class '{}': {:?}",
            self.id,
            strategies.len(),
            market_class,
            strategies.iter().map(|s| s.name()).collect::<Vec<_>>(),
        );
        if strategies.is_empty() {
            warn!(
                "Squadron [{}]: no runnable vipers for class '{}' — dashboard only",
                self.id, market_class,
            );
        }
        // Adopt orphans only into strategies this squadron actually runs.
        //
        // `strategy_names()` is all nine in adoption-priority order; keeping that
        // order matters (balance.rs relies on it), so filter rather than rebuild.
        // Without this the class filter above would create the very bug the US
        // and Kalshi close-phase handlers were written to prevent: an on-chain
        // orphan on a politics market adopted under, say, MomentumStrategy, which
        // no longer runs there and so never evaluates an exit for it. The intl
        // path has no `flatten_before_stand_down`, so such a position would ride
        // to settlement with no stop and no take-profit — the shape of the
        // -$3.09 Kalshi loss on 2026-08-10.
        let running: std::collections::HashSet<String> =
            strategies.iter().map(|s| s.name().to_string()).collect();
        let adoption_order: Vec<String> = StrategyRegistry::strategy_names()
            .into_iter()
            .filter(|n| running.contains(n))
            .collect();
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
                &squadron_id, &trading_client, &positions,
                &[(hourly_yes_token.clone(), "YES"), (hourly_no_token.clone(), "NO")],
                &hourly_market_name, hourly_market_close_time, &hourly_token_bids, &adoption_order,
                Some(&orphan_tombstones),
            ).await;
        }
        if let Some(ref mk_config) = maker_market_config {
            reconcile_orphaned_positions(
                &squadron_id, &trading_client, &positions,
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
            for (k, _pos) in map.iter() {
                if k.squadron != squadron_id { continue; }
                let (sn, tid) = (&k.strategy, &k.market);
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
        // Live post-only asks resting against filled maker positions, keyed the
        // same way as the position map: (strategy, token).
        let mut maker_resting_exits: std::collections::HashMap<PositionKey, MakerRestingExit> =
            std::collections::HashMap::new();

        // Previous tick's ghost mode, so the registry is cleared on the LIVE edge
        // rather than on every live tick. Starts true so a loop entered in live
        // mode sweeps once, which also clears anything a previous deployment of
        // this squadron id left behind.
        let mut was_ghosting = true;

        // Ghost resting quotes now live in the venue-neutral registry
        // (`helpers::ghost_quotes`) rather than in this loop, because Kalshi and
        // Polymarket US reach the venue through their own traders and were still
        // fabricating instant fills while this loop alone was fixed.

        // Collateral as it read BEFORE a position was entered, keyed like the
        // position map.
        //
        // This is the only way to price an exit the exchange refuses to confirm.
        // When a sell is rejected with "position already gone", the shares were
        // disposed of by something we did not observe, and the observed bid is a
        // terrible proxy for what they fetched: that branch is only reachable
        // from a stop firing on an adverse book, so the bid is always BELOW
        // entry and the estimate can only ever manufacture a loss. Production
        // trade 377 (2026-08-20) booked -$0.18 that way while collateral rose
        // $0.18 — a $0.36 error, and all four such trades to date show the same
        // one-tick-down signature.
        //
        // Deliberately not persisted: after a restart the baseline is unknown,
        // and the exit is then booked as unreconciled rather than guessed.
        let mut pre_entry_collateral: std::collections::HashMap<PositionKey, Decimal> =
            std::collections::HashMap::new();

        // Monotonic epoch per (strategy, token), bumped every time a maker quote
        // is placed AND every time one is pulled.
        //
        // Each quote placement spawns its own fill-verification task, but pulling
        // the quote does not stop that task: it keeps waiting on the balance
        // endpoint, and when a LATER quote on the same token fills it sees the
        // shares appear and books an entry for its own — cancelled, never filled
        // — price and size. Production 2026-08-20 recorded rows 494 and 495 that
        // way, 30ms apart on one token: 18.18 @ $0.44 (the quote that really
        // filled) and 17.78 @ $0.45 (a quote pulled 47s earlier). The same
        // signature appears in 12+ groups across the trade history.
        //
        // A task captures the epoch at spawn and records only if it still
        // matches, so a pulled quote's task can no longer claim someone else's
        // fill. Shared because the tasks outlive the tick that spawned them.
        // Value is (epoch, last touched). The timestamp exists only so the map
        // can be pruned: DRADIS runs for weeks and tokens rotate hourly, so an
        // unbounded key-per-token map is a slow leak.
        let quote_epochs: Arc<tokio::sync::Mutex<QuoteEpochs>> =
            Arc::new(tokio::sync::Mutex::new(QuoteEpochs::new()));

        info!("🚀 Orchestrator ready: {} strategies loaded", strategies.len());
        info!("📋 Strategy venue attachments:");
        let mut strategy_markets_map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        for strategy in &strategies {
            let sn = strategy.name();
            let venue = strategy.venue();
            let market_name_attached = match venue {
                "Hourly" => hourly_market_name.clone(),
                // On a single-market squadron the window/daily vipers run on the
                // one market too (see `single_market_arb` in the tick), so name
                // it rather than reporting a blank attachment.
                "Window/Daily" => maker_market_config
                    .as_ref()
                    .map(|m| m.market_name.clone())
                    .unwrap_or_else(|| if single_market { hourly_market_name.clone() } else { String::new() }),
                _ => String::from("Unknown"),
            };
            let status_key = sn
                .strip_suffix("Strategy")
                .unwrap_or(&sn)
                .to_lowercase()
                .replace("timedecay", "time_decay")
                // The TrendReversal strategy (formerly TrendCapture) keeps the
                // "trendcapture" key everywhere else (viper-kind registry, UI
                // statusKey, enable_trendcapture) — publish under the same key so
                // the Control Tower card resolves its attached market.
                .replace("trendreversal", "trendcapture");
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
                    // Cancel resting orders first. The market-rotation arm below
                    // has always done this; this arm did not, so a stand-down
                    // left maker quotes and TimeDecay GTC bids live and fillable
                    // on a squadron the operator had just stopped — while the
                    // confirm dialog told them "resting orders are cancelled".
                    //
                    // Positions are left to settle rather than flattened: the
                    // intl path has no `flatten_before_stand_down`, and the
                    // dialog says "flattened or left to settle" for that reason.
                    crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::MarketRotate);
                    if !cancel_all_orders_unless_simulating(trading_client.as_ref()).await {
                        error!(
                            "❌ Squadron [{}] stand-down: failed to cancel all orders after {} attempts — resting orders may remain open.",
                            self.id, MAX_CANCEL_RETRIES,
                        );
                    }
                    self.cancel_ws();
                    // Real resting orders were just cancelled above; the simulated
                    // ones must go too. Only the rotation and event-retirement arms
                    // called `stand_down()`, so an operator or watchdog stand-down
                    // of a ghosting squadron left its quotes in the registry, where
                    // a later redeploy under the same id inherits them on a market
                    // that is still live.
                    crate::helpers::ghost_quotes::clear_squadron(&self.id);
                    // Simulated position ROWS are deliberately NOT swept here.
                    //
                    // An unfilled quote dies with the squadron, so clearing the
                    // registry above is right. A filled paper position does not: a
                    // stood-down squadron can be redeployed under the same id
                    // against the same market, and the position map is session
                    // scoped, so the position is still held and still exitable.
                    // Deleting its row would hide a live position rather than
                    // retire a dead one. Rotation handles the rotated-away case and
                    // the startup sweep is the backstop for a process that never
                    // comes back.
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
                    crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::MarketRotate);
                    let cancel_success = cancel_all_orders_unless_simulating(trading_client.as_ref()).await;
                    if !cancel_success {
                        error!("❌ Failed to cancel all orders after {} attempts. Proceeding with market switch, but orders may remain open.", MAX_CANCEL_RETRIES);
                    }

                    { phantom_cooldowns.lock().await.clear(); }
                    { pending_orders.lock().await.clear(); }
                    // Simulated rows for the market(s) actually being left behind.
                    //
                    // Gated per market, because the two rotate independently and
                    // this arm fires when EITHER changes. The daily maker market
                    // survives roughly 23 of every 24 hourly rotations, and the
                    // redeployed squadron keeps the same id and the same
                    // session-scoped position map — so its paper positions are
                    // still held, still found by `venue_for_token`, and still
                    // exited normally. Sweeping them here deleted the open row of
                    // a live position once an hour: Bug #19 inverted.
                    //
                    // A token whose market HAS rotated away is genuinely
                    // unexitable — no viper can resolve it to a book any more —
                    // which is the only case where dropping the row is right.
                    {
                        let mut toks: Vec<&crate::venues::core::MarketId> = Vec::new();
                        if new_hourly_condition_id != current_hourly_cid {
                            toks.push(&hourly_yes_token);
                            toks.push(&hourly_no_token);
                        }
                        if new_maker_cid != current_maker_cid {
                            if let Some(ref mk) = maker_market_config {
                                toks.push(&mk.yes_token);
                                toks.push(&mk.no_token);
                            }
                        }
                        if !toks.is_empty() {
                            close_ghost_rows_for_market(&asset_lc, &toks).await;
                        }
                        // The LIVE twin of the ghost sweep above. The venue has
                        // just confirmed every resting order cancelled, so an
                        // UNFILLED quote on the market being left behind is an
                        // order that no longer exists: its map entry is a zombie
                        // and its `pending` row would otherwise sit in the
                        // Control Tower as a "Launch" for an hour until the
                        // pending grace expires. Only when the cancel actually
                        // succeeded — if it did not, the order may still be
                        // working and the row is the record of that. Filled
                        // positions are untouched: the chain reconciles those.
                        if !toks.is_empty() && cancel_success && !crate::helpers::dynamic_config::ghosting_now() {
                            let dead: Vec<PositionKey> = {
                                let map = positions.lock().await;
                                map.iter()
                                    .filter(|(k, p)| toks.iter().any(|t| **t == k.market) && p.fill_effective_at(false).is_none())
                                    .map(|(k, _)| k.clone())
                                    .collect()
                            };
                            if !dead.is_empty() {
                                let mut map = positions.lock().await;
                                let mut own = token_ownership.lock().await;
                                for k in &dead {
                                    map.remove(k);
                                    if own.get(&k.market) == Some(&k.strategy) { own.remove(&k.market); }
                                }
                                info!("🧹 Rotation: dropped {} unfilled quote(s) whose orders were just cancelled", dead.len());
                            }
                            if let Some(pool) = db::pool_for(&asset_lc) {
                                for t in &toks { db::close_pending_open_position(&pool, &t.to_string()).await; }
                            }
                        }
                    }
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
                    // ── Event-market retirement ─────────────────────────────
                    // A single-market squadron has no rotation behind it, so it
                    // has to retire itself. Nothing else will: the auto-deploy
                    // seeder skips a class while a squadron for it is live, so
                    // one that lingers on a resolved market blocks every fresh
                    // market behind it. Observed on the v1.0.4 Marketplace AMI —
                    // a sports squadron held a resolved League of Legends market
                    // for hours with both its vipers gated out.
                    //
                    // Ahead of the snapshot build deliberately. A resolved market
                    // whose WS has since dropped reads as the all-defaults book,
                    // which the "at least one market has valid prices" guard
                    // below turns into a `continue` — the squadron would never
                    // reach a retirement check placed after it.
                    if single_market {
                        // Retire only when flat. The intl path has no
                        // `flatten_before_stand_down`, so standing down on top of
                        // a position strands it with no viper left to evaluate
                        // its exit. Keep patrolling instead: the exit paths below
                        // are exactly what closes it out, and the next tick
                        // re-checks. Cheap enough to compute every tick — the
                        // lock is held for one `any()` over an in-memory map.
                        let holding = {
                            let map = positions.lock().await;
                            map.keys().any(|k| k.squadron == squadron_id)
                        };
                        let now = Utc::now();
                        let grace = dynamic_config.read().unwrap().event_market_retire_grace_secs;
                        if event_market_retire_due(true, hourly_market_close_time, now, grace, holding) {
                            let overdue = hourly_market_close_time
                                .map_or(0, |c| (now - c).num_seconds());
                            info!(
                                "🏁 Squadron [{}] retiring: market \"{}\" closed {}s ago and the squadron is flat — the class is free for the next deploy",
                                self.id, hourly_market_name, overdue,
                            );
                            // Flat means no POSITION; resting quotes are still
                            // orders, and they hold collateral until the venue
                            // clears them at resolution. Same treatment the
                            // rotation arm gives them.
                            crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::MarketRotate);
                            if !cancel_all_orders_unless_simulating(trading_client.as_ref()).await {
                                error!("❌ Failed to cancel all orders after {} attempts. Standing down anyway; the venue clears the rest at resolution.", MAX_CANCEL_RETRIES);
                            }
                            self.stand_down();
                            cag.update_state(&self.id, crate::squadron::SquadronState::StoodDown);
                            cag.remove(&self.id);
                            self.cancel_ws();
                            break;
                        }
                        // Past its close but still holding: say so, throttled,
                        // because from the outside this is indistinguishable from
                        // the squadron simply being stuck.
                        if holding
                            && hourly_market_close_time.is_some_and(|c| (now - c).num_seconds() >= grace)
                            && retire_wait_logged_at.elapsed() >= Duration::from_secs(60)
                        {
                            info!(
                                "🕰️ Squadron [{}] market \"{}\" closed {}s ago — holding to exit its position before standing down",
                                self.id, hourly_market_name,
                                hourly_market_close_time.map_or(0, |c| (now - c).num_seconds()),
                            );
                            retire_wait_logged_at = Instant::now();
                        }
                    }

                    // Watchdog breadcrumb: we are building the market snapshot + evaluating
                    // strategies. The executor refines this to the specific viper; a stall
                    // before it reaches the executor still shows SIGNAL_EVAL.
                    crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::SignalEval);

                    crate::helpers::watchdog::set_detail(crate::helpers::watchdog::STEP_BOOK_SNAPSHOT);
                    // Get hourly market snapshot
                    let (hourly_yb, hourly_ybd, hourly_ya, hourly_yad, hourly_yes_ws_ts, hourly_ybd_all, hourly_yad_all) = *yes_price_rx.borrow();
                    let (hourly_nb, hourly_nbd, hourly_na, hourly_nad, hourly_no_ws_ts, hourly_nbd_all, hourly_nad_all) = *no_price_rx.borrow();
                    let hourly_snap_ts = hourly_yes_ws_ts.min(hourly_no_ws_ts);

                    // Get maker market snapshot if available
                    let (maker_yb, maker_ybd, maker_ya, maker_yad, maker_yes_ws_ts, maker_ybd_all, maker_yad_all) = maker_yes_price_rx.as_ref().map_or((dec!(0), dec!(0), dec!(1), dec!(0), Utc::now(), dec!(0), dec!(0)), |rx| *rx.borrow());
                    let (maker_nb, maker_nbd, maker_na, maker_nad, maker_no_ws_ts, maker_nbd_all, maker_nad_all) = maker_no_price_rx.as_ref().map_or((dec!(0), dec!(0), dec!(1), dec!(0), Utc::now(), dec!(0), dec!(0)), |rx| *rx.borrow());
                    let maker_snap_ts = maker_yes_ws_ts.min(maker_no_ws_ts);

                    // Only proceed if at least one market has valid prices
                    if (hourly_ya == dec!(1) && hourly_na == dec!(1)) && (maker_ya == dec!(1) && maker_na == dec!(1)) {
                        // A squadron with NO market at all is idle on purpose:
                        // the hourly expired and nothing cleared the volume
                        // floor, so the monitor released it and this loop waits
                        // (see `should_release_held_market`). Say so in the
                        // viper registry every tick, or the vipers simply stop
                        // being recorded — which is exactly what a wedged loop
                        // looks like from the outside. On a fresh Marketplace
                        // instance 2026-09-04 that read as "9 stale/error" over
                        // a healthy, correctly idle system.
                        //
                        // Deliberately NOT recorded when a market is held but
                        // its book is empty: that is a dark feed, which is a
                        // real anomaly and already has its own banner.
                        //
                        // Vipers the operator has switched off keep their own
                        // "disabled in config" row; only the ones that WOULD run
                        // are the ones waiting.
                        if squadron_has_no_market(&hourly_yes_token, maker_market_config.is_some()) {
                            let cfg = dynamic_config.read().unwrap();
                            for s in strategies.iter() {
                                // Every viper is stamped, not just the enabled
                                // ones. A viper switched off during this window
                                // never reaches its own `evaluate_entry` — the
                                // `continue` below sees to that — so skipping it
                                // here would freeze its row and age it into a
                                // fault, which is the very thing this path
                                // exists to prevent.
                                let reason = if cfg.strategy_enabled(&s.name()) {
                                    crate::helpers::viper_status::IDLE_NO_MARKET
                                } else {
                                    crate::helpers::viper_status::DISABLED_IN_CONFIG
                                };
                                crate::helpers::viper_status::record_idle(
                                    &crypto_filter, &s.name(), reason,
                                );
                            }
                        }
                        continue;
                    }

                    // The strike can arrive after deploy — see `live_hourly_strike`.
                    let hourly_strike_price =
                        live_hourly_strike(&ctx.market_rx, &hourly_condition_id, hourly_strike_price);
                    // Part of every PositionKey the vipers build this tick.
                    let hourly_market_config_for_ctx = MarketConfig {
                        yes_token: hourly_yes_token.clone(), no_token: hourly_no_token.clone(), market_name: hourly_market_name.clone(), market_close_time: hourly_market_close_time, strike_price: hourly_strike_price, is_neg_risk: hourly_is_neg_risk, condition_id: hourly_condition_id.clone(), yes_fee_bps: hourly_yes_fee_rate, no_fee_bps: hourly_no_fee_rate,
                    };

                    let maker_market_config_for_ctx = maker_market_config.clone();

                    crate::helpers::watchdog::set_detail(crate::helpers::watchdog::STEP_CONFIG_READ);
                    let dyn_cfg = Arc::new(dynamic_config.read().unwrap().clone());
                    // Snapshot the resting-exit knobs before `dyn_cfg` is moved
                    // into the StrategyContext below.
                    let resting_exit_reprice_threshold = dyn_cfg.maker_resting_exit_reprice_threshold;
                    let resting_exit_enabled = dyn_cfg.maker_resting_exit_enabled;
                    let fairvalue_resting_tp_enabled = dyn_cfg.fairvalue_resting_tp_enabled;
                    // Read through the floored accessor — see its doc for why the
                    // schema minimum alone is not enough.
                    let exit_retry_cooldown_secs = dyn_cfg.exit_retry_cooldown_secs_floored();
                    // ── Release positions the chain sweep has closed ──────────
                    // The sweep books and deletes the ROW; the map entry is this
                    // loop's to drop. Wallet-wide: a token the wallet no longer
                    // holds is gone for every squadron on this asset.
                    for tok in db::take_released_positions() {
                        let m = MarketId::new(tok.as_str());
                        let dead: Vec<PositionKey> = {
                            let map = positions.lock().await;
                            map.keys().filter(|k| k.market == m).cloned().collect()
                        };
                        if dead.is_empty() { continue; }
                        {
                            let mut map = positions.lock().await;
                            for k in &dead { map.remove(k); maker_resting_exits.remove(k); }
                        }
                        token_ownership.lock().await.remove(&m);
                        info!("🧾 Released {} in-memory position(s) on {} — the chain sweep booked and closed the row", dead.len(), tok);
                    }
                    let intl_taker_fee_rate = dyn_cfg.intl_taker_fee_rate;
                    // Hoisted like the two above: dyn_cfg is moved into `ctx` below.
                    let exit_reconcile_max_dev = dyn_cfg.exit_reconcile_max_deviation;

                    // Is this tick allowed to touch the exchange at all?
                    //
                    // `config::GHOST_MODE` is a BUILD-level switch; `ghost_mode` in
                    // DynamicConfig is the operator's runtime one, toggled from the
                    // Control Tower. Until 2026-08-20 the intl order paths read only the
                    // constant, so the runtime switch was inert on this venue — the UI
                    // said simulation while real orders went out. (Kalshi and Polymarket
                    // US always honored it; see `params.ghost_mode` in their traders.)
                    //
                    // Deliberately ONE value for the whole tick rather than a check per
                    // call site: eleven separate conditions is precisely the shape of
                    // change where one gets missed, and `params` is not in scope at
                    // several of them. The tick value is also fresher than the one
                    // captured on a signal when it was emitted.
                    let ghosting = config::GHOST_MODE || dyn_cfg.ghost_mode;
                    // Stamp the filing scope with the mode in force THIS tick, so
                    // every clone taken into a spawned exit task records the mode
                    // the trade actually happened under. See `TradeScope::ghost`.
                    scope.ghost = ghosting;

                    // ── Ghost maker fills: rest until the book crosses the quote ──
                    //
                    // A resting BUY at Q fills when somebody is willing to sell at
                    // Q, i.e. when the best ASK falls to Q or below. Anything
                    // sooner is not a fill, it is the simulator handing itself a
                    // trade. Evaluated before the vipers run so a quote that fills
                    // this tick is visible to its own exit logic on the same tick.
                    //
                    // Deliberately conservative: the ask is sampled once per tick,
                    // so a book that dips through the quote and recovers between
                    // samples is missed. Under-reporting simulated fills is the
                    // right direction to be wrong in — the alternative is the
                    // fabricated profit this replaced.
                    // Cleared on the way out of ghost mode: a simulated quote must
                    // never outlive the mode that created it. Without this, an
                    // operator toggling GHOST → LIVE → GHOST could have a stale
                    // simulated quote cross and overwrite a real position below.
                    // Edge-triggered: clearing on every live tick would take the
                    // registry's global lock and walk the whole map 20x/second per
                    // squadron to do nothing.
                    if was_ghosting && !ghosting {
                        crate::helpers::ghost_quotes::clear_squadron(&squadron_id);
                    }
                    was_ghosting = ghosting;
                    if ghosting {
                        // Resolve against BOTH books. The Maker viper falls back to
                        // the hourly market when there is no daily maker venue
                        // (`ctx.maker_market.unwrap_or(&ctx.market)`), so a quote can
                        // legitimately be keyed to hourly tokens. Matching only the
                        // maker book left those quotes permanently unfillable.
                        let ask_for = |m: &crate::venues::core::MarketId| -> Option<Decimal> {
                            if let Some(ref mk) = maker_market_config {
                                if m == &mk.yes_token { return Some(maker_ya); }
                                if m == &mk.no_token { return Some(maker_na); }
                            }
                            if m == &hourly_yes_token { return Some(hourly_ya); }
                            if m == &hourly_no_token { return Some(hourly_na); }
                            None
                        };
                        for (pk, resting) in crate::helpers::ghost_quotes::take_crossed(&squadron_id, ask_for) {
                            let pos = resting.position;
                            let rested = (Utc::now() - pos.opened_at).num_seconds();
                            let mut map = positions.lock().await;
                            // Refuse to overwrite an occupied slot. Chain-sync
                            // reconciliation and orphan adoption both write real
                            // positions into this map without consulting ghost
                            // mode, so a blind insert could replace real shares
                            // with a simulated record and leave the real ones
                            // untracked.
                            if map.contains_key(&pk) {
                                warn!(
                                    "👻 GHOST_MODE MakerFill [{}]: {} | dropping simulated fill @ ${:.4} — a position already occupies this slot",
                                    pk.strategy, pos.market_name, pos.avg_entry,
                                );
                                continue;
                            }
                            info!(
                                "👻 GHOST_MODE MakerFill [{}]: {} | shares={:.2} @ ${:.4} — ask crossed after {}s resting (simulated)",
                                pk.strategy, pos.market_name, pos.shares, pos.avg_entry, rested,
                            );
                            // Persist the fill, exactly as Kalshi and Polymarket US
                            // do through their own `record_entry`.
                            //
                            // Without this the simulated position lives only in this
                            // process's memory: the Control Tower positions panel
                            // never shows it, and a restart drops it silently so the
                            // round trip is never booked and the paper record loses
                            // the trade. That is the same class of hole as ghost
                            // exits writing nothing, fixed earlier — an operator
                            // reading the dashboard is entitled to see what the
                            // engine thinks it holds.
                            {
                                // Same rule as the `side_of` closure used further
                                // down the tick, which is not in scope this early.
                                let is_yes = pk.market == hourly_yes_token
                                    || maker_market_config.as_ref()
                                        .is_some_and(|mk| mk.yes_token == pk.market);
                                // AWAITED, not spawned. The exit for this fill can
                                // fire on the very next 50ms tick and calls
                                // `close_open_position`; a spawned INSERT landing
                                // after that DELETE leaves an open ghost row no map
                                // entry owns, and nothing else ever removes one.
                                if let Some(pool) = db::pool_for(&asset_lc) {
                                    db::record_open_position(
                                        &pool, &scope, &squadron_id, &pk.strategy,
                                        &pk.market.to_string(), &pos.market_name,
                                        if is_yes { "YES" } else { "NO" },
                                        pos.avg_entry, pos.shares, true,
                                    ).await;
                                }
                            }
                            map.insert(pk, pos);
                        }
                    }

                    // No rotation sweep here on purpose. A rotation breaks this loop
                    // and redeploys the squadron, and stand-down clears the registry
                    // for this squadron on the way out, so a quote cannot outlive the
                    // market it was placed in. An earlier sweep here could never drop
                    // a rotated quote (`maker_market_config` is bound once and never
                    // changes mid-loop) while its else-branch cleared every quote on
                    // every tick for any squadron with no daily maker venue.

                    // Hoist mutex-await calls OUT of the struct literal so that
                    // borrow() Ref guards (oracle_rx, velocity_rx, etc.) in the
                    // snapshot fields are NOT alive at any .await point.
                    // Without this the future is non-Send and tokio::spawn rejects
                    // it (Phase 3f-6: concurrent multi-asset spawning).
                    crate::helpers::watchdog::set_detail(crate::helpers::watchdog::STEP_PNL_LOCK);
                    let ctx_session_pnl          = *total_pnl.lock().await;
                    crate::helpers::watchdog::set_detail(crate::helpers::watchdog::STEP_COLLATERAL_LOCK);
                    let ctx_starting_collateral  = *starting_collateral_store.lock().await;
                    let ctx_available_collateral = *live_collateral.lock().await;

                    // Baseline for exit reconciliation: collateral as it stood
                    // BEFORE this position was entered.
                    //
                    // Recorded on first sight of the position — including while it
                    // is still an unconfirmed resting order — because a fill that
                    // settles inside the status task's 60s refresh window would
                    // otherwise be baselined against its own post-buy collateral.
                    // Positions that vanish are dropped so the map cannot grow.
                    {
                        crate::helpers::watchdog::set_detail(crate::helpers::watchdog::STEP_POSITIONS_LOCK);
                        let map = positions.lock().await;
                        for k in map.keys() {
                            pre_entry_collateral.entry(k.clone()).or_insert(ctx_available_collateral);
                        }
                        pre_entry_collateral.retain(|k, _| map.contains_key(k));
                    }
                    // Drop epochs no in-flight fill watcher could still consult.
                    // A watcher waits at most MAX_WAIT_SECS_WINDOW, so double that
                    // is comfortably past the last moment one can read this.
                    {
                        let cutoff = Duration::from_secs(
                            crate::helpers::balance::MAX_WAIT_SECS_WINDOW as u64 * 2);
                        crate::helpers::watchdog::set_detail(crate::helpers::watchdog::STEP_QUOTE_EPOCHS_LOCK);
                        let mut m = quote_epochs.lock().await;
                        m.retain(|_, (_, touched)| touched.elapsed() < cutoff);
                    }

                    // Read every raptor channel EXACTLY ONCE, before the
                    // StrategyContext literal below.
                    //
                    // Rust keeps a temporary alive until the end of the enclosing
                    // STATEMENT, so a `*rx.borrow()` written inside that literal
                    // holds its read guard until the whole `let ctx = ...;`
                    // completes — not just for its own field. The literal used to
                    // borrow the same channels repeatedly (oracle twice, velocity
                    // three times, drift three times, horizon four), which meant
                    // holding a read guard and then asking for another on the same
                    // lock.
                    //
                    // parking_lot's RwLock — which is what `tokio::sync::watch`
                    // uses — is writer-fair: once a writer queues, further SHARED
                    // acquisitions block behind it. So when the price raptor's
                    // `oracle_tx.send()` landed between two of those borrows, the
                    // second borrow blocked behind the writer, the writer waited
                    // for the first guard to drop, and that guard could only drop
                    // when the statement finished. A textbook recursive-read
                    // deadlock, with the patrol task as both holder and waiter.
                    //
                    // It froze the intl venue twice on 2026-08-25, each time
                    // reported by the OS watchdog as a silent SIGNAL_EVAL and
                    // killed at 300s. Reading once into a local also means one
                    // lock acquisition per channel per tick instead of three or
                    // four.
                    let oracle_now = *oracle_rx.borrow();
                    let (velocity_now, velocity_1s_now, acceleration_now) = *velocity_rx.borrow();
                    let (drift_60m_now, drift_10m_now, hist_vol_now) = *drift_rx.borrow();
                    let funding_now = *funding_rx.borrow();
                    let tide_now = tide_rx.as_ref().map(|r| *r.borrow());
                    let horizon_now = horizon_rx.as_ref().map(|r| *r.borrow());
                    let deriv_now = deriv_rx.as_ref().map(|r| *r.borrow());

                    // Hoisted out of the StrategyContext literal so the
                    // single-market arbitrage path below can hand Arbitrage the
                    // very same snapshot as its maker leg.
                    let hourly_snapshot = MarketSnapshot {
                            yes_bid: hourly_yb, yes_bid_depth: hourly_ybd, yes_ask: hourly_ya, yes_ask_depth: hourly_yad,
                            no_bid: hourly_nb, no_bid_depth: hourly_nbd, no_ask: hourly_na, no_ask_depth: hourly_nad,
                            yes_bid_depth_total: hourly_ybd_all, yes_ask_depth_total: hourly_yad_all,
                            no_bid_depth_total: hourly_nbd_all, no_ask_depth_total: hourly_nad_all,
                            oracle_price: oracle_now,
                            velocity: velocity_now,
                            velocity_1s: velocity_1s_now,
                            acceleration: acceleration_now,
                            funding_rate: funding_now,
                            institutional_pulse: tide_now.map(|s| s.institutional_pulse).unwrap_or(Decimal::ZERO),
                            tide_coherence: tide_now.map(|s| s.coherence).unwrap_or(Decimal::ZERO),
                            tradfi_velocity: horizon_now.map(|s| s.tradfi_velocity).unwrap_or(Decimal::ZERO),
                            macro_coherence: horizon_now.map(|s| s.macro_coherence).unwrap_or(Decimal::ZERO),
                            vix_proxy: horizon_now.map(|s| s.vix_proxy).unwrap_or(Decimal::ZERO),
                            vix_velocity: horizon_now.map(|s| s.vix_velocity).unwrap_or(Decimal::ZERO),
                            oi_delta_pct: deriv_now.map(|s| s.oi_delta_pct).unwrap_or(Decimal::ZERO),
                            cvd_ratio: deriv_now.map(|s| s.cvd_ratio).unwrap_or(Decimal::ZERO),
                            oracle_drift_60m: drift_60m_now,
                            oracle_drift_10m: drift_10m_now,
                            hist_vol: hist_vol_now,
                            secs_to_expiry: hourly_market_close_time
                                .map(|t| (t - Utc::now()).num_seconds())
                                .unwrap_or(0),
                            timestamp: hourly_snap_ts,
                    };

                    // ── Single-market arbitrage ─────────────────────────────
                    // Arbitrage reads the maker (window/daily) venue, and refuses
                    // to run without one: on a split venue the hourly leg is a
                    // different market, so a half-filled pair there is a naked
                    // directional bet the arbiter has to flatten at a loss (the
                    // 2026-06-19 episode in `arbitrage_impl`).
                    //
                    // A single-market squadron has no split to fall back FROM.
                    // Its one market is the venue, both legs quote on the same
                    // book, and a pair placed there is the ordinary hedged trade
                    // the guard was written to protect. Passing None left
                    // Arbitrage idling on "no daily/window venue available" for
                    // the life of every politics and sports squadron, while the
                    // class taxonomy went on advertising it as one of their two
                    // vipers. The US wing resolved the identical shape the same
                    // way on 2026-08-08 (`venues/us/trader.rs`).
                    //
                    // Scoped to `single_market` on purpose. A crypto squadron
                    // that transiently loses its daily market must keep idling:
                    // there the guard is load-bearing.
                    let single_market_arb =
                        single_market_arb_enabled(single_market, maker_market_config.is_some());
                    let maker_market_config_for_ctx = if single_market_arb {
                        Some(hourly_market_config_for_ctx.clone())
                    } else {
                        maker_market_config_for_ctx
                    };

                    let ctx = StrategyContext {
                        squadron_id: squadron_id.clone(),
                        market: hourly_market_config_for_ctx.clone(),
                        snapshot: hourly_snapshot.clone(),
                        positions: Arc::clone(&positions),
                        session_pnl:          ctx_session_pnl,
                        starting_collateral:  ctx_starting_collateral,
                        available_collateral: ctx_available_collateral,
                        crypto_filter: crypto_filter.clone(),
                        market_started_at,
                        maker_market: maker_market_config_for_ctx,
                        maker_snapshot: if single_market_arb {
                            Some(hourly_snapshot)
                        } else { maker_market_config.as_ref().map(|mk| MarketSnapshot {
                            yes_bid: maker_yb, yes_bid_depth: maker_ybd, yes_ask: maker_ya, yes_ask_depth: maker_yad,
                            no_bid: maker_nb, no_bid_depth: maker_nbd, no_ask: maker_na, no_ask_depth: maker_nad,
                            yes_bid_depth_total: maker_ybd_all, yes_ask_depth_total: maker_yad_all,
                            no_bid_depth_total: maker_nbd_all, no_ask_depth_total: maker_nad_all,
                            oracle_price: oracle_now, velocity: velocity_now, velocity_1s: velocity_1s_now, acceleration: acceleration_now,
                            funding_rate: funding_now, oracle_drift_60m: drift_60m_now, oracle_drift_10m: drift_10m_now,
                            hist_vol: hist_vol_now,
                            institutional_pulse: tide_now.map(|s| s.institutional_pulse).unwrap_or(Decimal::ZERO),
                            tide_coherence: tide_now.map(|s| s.coherence).unwrap_or(Decimal::ZERO),
                            tradfi_velocity: horizon_now.map(|s| s.tradfi_velocity).unwrap_or(Decimal::ZERO),
                            macro_coherence: horizon_now.map(|s| s.macro_coherence).unwrap_or(Decimal::ZERO),
                            vix_proxy: horizon_now.map(|s| s.vix_proxy).unwrap_or(Decimal::ZERO),
                            vix_velocity: horizon_now.map(|s| s.vix_velocity).unwrap_or(Decimal::ZERO),
                            oi_delta_pct: deriv_now.map(|s| s.oi_delta_pct).unwrap_or(Decimal::ZERO),
                            cvd_ratio: deriv_now.map(|s| s.cvd_ratio).unwrap_or(Decimal::ZERO),
                            secs_to_expiry: mk.market_close_time
                                .map(|t| (t - Utc::now()).num_seconds())
                                .unwrap_or(0),
                            timestamp: maker_snap_ts,
                        }) },
                        dynamic_config: dyn_cfg,
                        arb_market_lockouts: Some(arb_market_lockouts.clone()),
                    };

                    let eval_result = match execute_strategies_concurrent(&strategies, &ctx, 500, &mut last_executor_summary).await {
                        Ok(r) => r,
                        Err(e) => { warn!("⚠️ Strategy evaluation error: {}", e); continue; }
                    };
                    let (resolved_signals, _) = aggregate_and_resolve_signals(&eval_result);
                    if resolved_signals.is_empty() { continue; }

                    // ── Signal-processing timeout guard (45 s) ───────────────────────
                    crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::OrderPlace);
                    let signal_processing_result = tokio::time::timeout(Duration::from_secs(45), async {

                    for (strategy_name, signal) in resolved_signals {

                        let sn = strategy_name.clone();
                        let sq_key = squadron_id.clone();
                        let (target_yes_token, target_no_token, target_market_close_time, target_is_neg_risk, target_yes_fee_bps, target_no_fee_bps) = {
                            let strategy_venue = strategies.iter().find(|s| s.name() == sn).map(|s| s.venue()).unwrap_or("Hourly");
                            if strategy_venue == "Window/Daily" && maker_market_config.is_some() {
                                let mk = maker_market_config.as_ref().unwrap();
                                (mk.yes_token.clone(), mk.no_token.clone(), mk.market_close_time, mk.is_neg_risk, mk.yes_fee_bps, mk.no_fee_bps)
                            } else {
                                (hourly_yes_token.clone(), hourly_no_token.clone(), hourly_market_close_time, hourly_is_neg_risk, hourly_yes_fee_rate, hourly_no_fee_rate)
                            }
                        };

                        // ── YES/NO labeling ─────────────────────────────────
                        // Resolve a token's side from the market it ACTUALLY
                        // belongs to, never from `target_yes_token`.
                        //
                        // A strategy's declared venue is not necessarily the
                        // market it trades: FairValue declares "Window/Daily"
                        // but `fairvalue_prefer_hourly` makes it trade the
                        // hourly book, so `token == target_yes_token` could
                        // never hold and every derivation fell through to the
                        // `else` arm. Result: all six FairValue trades and four
                        // entries on 2026-08-13 were recorded NO while the
                        // viper logged (correctly) YES — the 14:46 heartbeat
                        // showed YES ask $0.75 against NO ask $0.27, and the
                        // position settled at $1.00.
                        //
                        // Checking both books makes the label independent of
                        // which venue a strategy declared.
                        let maker_yes_token = maker_market_config.as_ref().map(|mk| mk.yes_token.clone());
                        let side_of = |token: &MarketId| -> &'static str {
                            if *token == hourly_yes_token { return "YES"; }
                            if maker_yes_token.as_ref().is_some_and(|y| y == token) { return "YES"; }
                            "NO"
                        };

                        match signal {
                            // ════════════════════ EXIT ════════════════════
                            StrategySignal::Exit { params, reason, exit_pair } => {
                                if let Some(lt) = last_exit_attempt_time.get(&sn) {
                                    if lt.elapsed() < Duration::from_secs(exit_retry_cooldown_secs) {
                                        continue;
                                    }
                                }
                                last_exit_attempt_time.insert(sn.clone(), Instant::now());
                                let tid = params.token_id;
                                let tid_m = tid.clone(); // neutral key (slice 2a)
                                let pos_key = PositionKey::new(sq_key.clone(), sn.clone(), tid_m.clone());

                                // ── Free the shares before any FAK ────────────────
                                // A resting post-only ask COMMITS the shares at the
                                // exchange: leaving it on the book would make this sell
                                // fail "not enough balance" and silently defeat the stop
                                // it is trying to honor. Stops always outrank spread
                                // capture, so the ask is pulled first.
                                //
                                // Whatever the ask managed to trade before the pull is
                                // booked HERE, at the resting limit — a resting limit
                                // cannot slip, so that price is exact. Leaving it to the
                                // generic fallback would book it at the observed bid and
                                // record a spread-capturing win as a loss.
                                let resting_rec = if ghosting { None } else { maker_resting_exits.get(&pos_key).cloned() };
                                // Kept past the block below: if the venue later rejects the
                                // stop because the shares are already gone, this ask is the
                                // only order that could have taken them.
                                let resting_ask_price = resting_rec.as_ref().map(|r| r.price);
                                if let Some(rest) = resting_rec {
                                    // Leaving the position: pull everything we have resting
                                    // on the token, the ask AND any residual entry bid. Only
                                    // the ASK's matched size is an exit fill, though — the
                                    // bid's is the entry, already in the position.
                                    let cancel = cancel_resting_orders_for_token(
                                        &trading_client, &tid_m, RestingSide::Both).await;
                                    maker_resting_exits.remove(&pos_key);

                                    // How much do we still hold? A single balance read of
                                    // 0 proves nothing (the helper returns 0 on ANY
                                    // failure and the endpoint lags fills), so confirm
                                    // with settlement-lag retries, keeping the LARGEST
                                    // reading — erring toward "still held" can only cost
                                    // a retry, while erring the other way fabricates an
                                    // exit and orphans real shares.
                                    let mut held = onchain_balance_for_token(&trading_client, &tid_m).await;
                                    if held < config::MIN_ORDER_SHARES {
                                        for _ in 1..config::SETTLEMENT_LAG_RETRY_ATTEMPTS {
                                            tokio::time::sleep(
                                                Duration::from_secs(config::SETTLEMENT_LAG_RETRY_DELAY_SECS)
                                            ).await;
                                            let again = onchain_balance_for_token(&trading_client, &tid_m).await;
                                            if again > held { held = again; }
                                            if held >= config::MIN_ORDER_SHARES { break; }
                                        }
                                    }

                                    let prior_shares = {
                                        let map = positions.lock().await;
                                        match map.get(&pos_key) { Some(p) => p.shares, None => continue }
                                    };
                                    // Trust the balance over `matched`: a fully-lifted ask
                                    // has already left the book and reports no matched
                                    // size at all. What the venue does report is cumulative,
                                    // so the sweep's earlier bookings come off first.
                                    let matched = resting_exit::lift_since_booking(
                                        cancel.ask_found, cancel.ask_matched, rest.booked, prior_shares);
                                    let lifted = (prior_shares - held).max(matched).max(dec!(0));

                                    if lifted >= config::MIN_ORDER_SHARES {
                                        let (avg_entry, entry_fee) = {
                                            let map = positions.lock().await;
                                            map.get(&pos_key).map(|p| (p.avg_entry, p.entry_fee))
                                                .unwrap_or((rest.avg_entry, rest.entry_fee))
                                        };
                                        // Net of the entry fee this slice carries: zero for
                                        // the Maker, the taker fee for FairValue.
                                        let bk = resting_exit::book_lift(rest.price, avg_entry, entry_fee, lifted, prior_shares);
                                        let pnl = bk.pnl;
                                        let side_label = side_of(&tid_m).to_string();
                                        info!("✅ {} filled [{}]: {:.4} shares lifted @ ${:.4} (entry ${:.4}) pnl=${:.4}{} — preempted \"{}\"",
                                              resting_exit::label(&sn), sn, lifted, rest.price, avg_entry, pnl, bk.fee_note(), reason);
                                        *total_pnl.lock().await += pnl;
                                        metrics::record_trade(
                                            &scope, bk.entry_fee_booked, sn.clone(), params.market_name.clone(), side_label,
                                            avg_entry, rest.price, lifted, pnl,
                                            resting_exit::ledger_reason(&sn, rest.price, avg_entry),
                                        ).await;
                                        last_trade_time.insert(sn.clone(), Instant::now());

                                        if held < config::MIN_ORDER_SHARES {
                                            // Nothing left — the stop has nothing to do.
                                            positions.lock().await.remove(&pos_key);
                                            token_ownership.lock().await.remove(&tid_m);
                                            if let Some(pool) = db::pool_for(&asset_lc) {
                                                db::close_open_position(&pool, &sn, tid_m.as_str()).await;
                                            }
                                            continue;
                                        }
                                        // Partial lift: the stop still wants the rest out, and
                                        // the FAK that follows nets the entry fee left with it.
                                        if let Some(p) = positions.lock().await.get_mut(&pos_key) {
                                            p.shares = held;
                                            p.entry_fee = bk.entry_fee_left;
                                        }
                                    } else {
                                        info!("🚫 EXIT [{}]: pulled resting ask to free shares for \"{}\"", sn, reason);
                                        if held >= config::MIN_ORDER_SHARES {
                                            if let Some(p) = positions.lock().await.get_mut(&pos_key) {
                                                if held < p.shares { p.shares = held; }
                                            }
                                        }
                                    }
                                }

                                let shares = { let map = positions.lock().await; match map.get(&pos_key) { Some(p) => p.shares, None => continue } };
                                if shares < config::MIN_ORDER_SHARES || params.price <= dec!(0) {
                                    let mut map = positions.lock().await; if let Some(p) = map.remove(&pos_key) { let aep = (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE); *total_pnl.lock().await += (aep - p.avg_entry) * p.shares; } continue;
                                }
                                info!("🔴 EXIT [{}]: {} | shares={:.2}, bid=${:.4} | {}", sn, params.market_name, shares, params.price, reason);
                                let vc = if target_is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };

                                // Real average fill price for this exit, derived from the
                                // venue's matched amounts. `None` until the order returns.
                                //
                                // SELL_PRICE_OFFSET only lowers the FAK's LIMIT so it
                                // reliably sweeps the book — the executed price is the real
                                // best bid, never the limit. Booking the offset limit as the
                                // exit price therefore understated every single exit by a
                                // full tick (2026-08-11 audit: 6 of 6 Maker take-profits
                                // recorded exactly one tick below their trigger bid, e.g.
                                // trade 346 logged "gain=5.26%" but booked 2.63%).
                                // Same class of bug as intl trade 56 (2026-06-21), already
                                // fixed inside `IntlClobVenue::place_order`.
                                let mut exit_fill_price: Option<Decimal> = None;
                                // Shares the venue matched on THIS order, from its synchronous
                                // response. `None` only when placement returned Err.
                                let mut exit_filled_shares: Option<Decimal> = None;
                                if !ghosting {
                                    if let Err(e) = place_limit_order_filled(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, &tid, Side::Sell, shares, (params.price - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE), target_yes_fee_bps as u16, params.order_type, params.post_only, 0, &shared_http).await
                                        .map(|(_oid, making, taking)| {
                                            // SELL orientation: making = shares given, taking = USDC received.
                                            // Ratio is unit-invariant. Clamp to a valid binary price; anything
                                            // outside means an unexpected orientation → fall back below.
                                            exit_filled_shares = Some(making);
                                            if making > dec!(0) && taking > dec!(0) {
                                                let p = taking / making;
                                                if p > dec!(0) && p <= dec!(1) { exit_fill_price = Some(p); }
                                            }
                                            // Non-zero making/taking means this matched immediately,
                                            // i.e. we were the taker and owe the fee. A resting order
                                            // reports zeros and pays nothing.
                                        })
                                    {
                                        let es = e.to_string();
                                        if es.contains("not enough balance") || es.contains("balance: 0") || es.contains("invalid price") {
                                            // The exchange says we don't hold the shares. Two very different
                                            // causes produce this rejection:
                                            //  (a) the position IS gone on-chain (a prior FAK exit filled but a
                                            //      stale balance read resurrected it — observed 2026-07-16), or
                                            //  (b) the shares are TOO FRESH to sell — bought seconds ago and
                                            //      still settling (observed 2026-07-25 trade 294: a just-adopted
                                            //      partial fill was booked as "gone" while 1.25 shares floated
                                            //      unmanaged for 8 min until the next quote-pull re-adopted them).
                                            // Cross-check the on-chain balance to tell them apart.
                                            let held = crate::helpers::balance::onchain_balance_for_token(&trading_client, &tid_m).await;
                                            if held >= config::MIN_ORDER_SHARES {
                                                // (b) settlement lag — shares are still held. Keep the position
                                                // under management and retry the exit after the cooldown.
                                                warn!("⚠️ EXIT rejected but {:.4} shares still held on-chain [{}]: settlement lag — holding position, retrying exit in {}s",
                                                      held, sn, exit_retry_cooldown_secs);
                                                if let Some(p) = positions.lock().await.get_mut(&pos_key) { p.shares = held; }
                                                last_trade_time.insert(sn.clone(), Instant::now());
                                                // Bug #6: placement was rejected — clear the viper's exit-signal
                                                // cooldown so it can re-emit the exit after EXIT_RETRY_COOLDOWN.
                                                if let Some(strat) = strategies.iter().find(|s| s.name() == sn) { strat.on_exit_order_failed(&tid_m); }
                                                continue;
                                            }
                                            // (a?) on-chain also reads 0 — but the balance endpoint
                                            // ITSELF lags settlement, so a freshly-adopted fill can
                                            // read 0 both on the exchange AND here (2026-07-30/31
                                            // trades 308/310: ToxicFill fired seconds after quote-pull
                                            // fill adoption, both checks read 0, an est. exit was
                                            // booked — then the surviving shares were sold again later
                                            // and the same loss was recorded twice). If the fill was
                                            // confirmed within the settlement grace window, trust the
                                            // fill over the balance reads: hold and retry.
                                            let fill_is_fresh = {
                                                let map = positions.lock().await;
                                                map.get(&pos_key)
                                                    .and_then(|p| p.fill_confirmed_at)
                                                    .map(|fc| (Utc::now() - fc).num_seconds() < config::FRESH_FILL_SETTLEMENT_GRACE_SECS)
                                                    .unwrap_or(false)
                                            };
                                            if fill_is_fresh {
                                                warn!("⚠️ EXIT rejected [{}] but fill confirmed <{}s ago: settlement lag (balance endpoint not caught up) — holding position, retrying exit in {}s",
                                                      sn, config::FRESH_FILL_SETTLEMENT_GRACE_SECS, exit_retry_cooldown_secs);
                                                last_trade_time.insert(sn.clone(), Instant::now());
                                                // Bug #6: placement was rejected — clear the viper's exit-signal cooldown.
                                                if let Some(strat) = strategies.iter().find(|s| s.name() == sn) { strat.on_exit_order_failed(&tid_m); }
                                                continue;
                                            }
                                            // (a) confirmed gone on-chain — book the exit NOW at the estimated
                                            // price and close the DB row.
                                            // The old path credited session P&L silently with no trade record
                                            // and no open_positions cleanup, so the ledger diverged and
                                            // ChainReconcile later invented a second exit at the current mark.
                                            let removed = { let mut map = positions.lock().await; map.remove(&pos_key) };
                                            let fill_baseline = pre_entry_collateral.remove(&pos_key);
                                            if let Some(p) = removed {
                                                if p.fill_confirmed_at.is_some() {
                                                    // Price the exit from the MONEY, not from the book.
                                                    //
                                                    // The shares are gone, so whatever took them paid us
                                                    // something; collateral has already moved by exactly that
                                                    // amount. `params.price` — the current bid — is not that
                                                    // number and is structurally biased: this branch is only
                                                    // reachable from a stop firing on an adverse book, so the
                                                    // bid always sits below entry and the "estimate" can only
                                                    // ever book a loss. Trade 377 booked -$0.18 on a +$0.18
                                                    // move that way.
                                                    let now_collateral = *live_collateral.lock().await;
                                                    let implied = reconcile_unverified_exit(
                                                        fill_baseline, now_collateral, p.shares,
                                                        p.avg_entry, params.price, exit_reconcile_max_dev,
                                                    );
                                                    let sid3 = side_of(&tid).to_string();
                                                    let market_open = target_market_close_time.map_or(true, |c| c > Utc::now());
                                                    let outcome: Option<(Decimal, Decimal, String)> = match implied {
                                                        Some((px, pnl)) => {
                                                            warn!(
                                                                "⚠️ EXIT rejected by exchange [{}] (\"{}\"): position already gone — reconciled from collateral: pnl=${:.4} (implies exit @ ${:.4} against bid ${:.4})",
                                                                sn, es.chars().take(80).collect::<String>(), pnl, px, params.price
                                                            );
                                                            Some((px, pnl, format!("{} (ExitReconciled: pnl taken from collateral movement)", reason)))
                                                        }
                                                        // Collateral could not confirm it — but a resting ask on
                                                        // these exact shares CAN. A post-only limit cannot slip,
                                                        // so if the venue says the shares are gone and our own
                                                        // ask was the only order that could have taken them,
                                                        // the exit price is that ask, exactly. Trade 18,
                                                        // 2026-09-01: the 13.53-share remainder of a partial
                                                        // lift at $0.63 was booked "$0.61 → $0.61, pnl 0" here
                                                        // while its own reason string claimed +9.83%.
                                                        None => fak_exit::attribute_to_resting_ask(
                                                            resting_ask_price, market_open, p.avg_entry, p.entry_fee, p.shares,
                                                        ).map(|(px, pnl)| {
                                                            info!(
                                                                "✅ {} filled [{}]: {:.4} shares lifted @ ${:.4} (entry ${:.4}) pnl=${:.4} — attributed: the venue rejected \"{}\" because the ask had already filled",
                                                                resting_exit::label(&sn), sn, p.shares, px, p.avg_entry, pnl, reason
                                                            );
                                                            (px, pnl, format!(
                                                                "{} (attributed from the resting ask after the venue rejected the stop)",
                                                                resting_exit::ledger_reason(&sn, px, p.avg_entry)
                                                            ))
                                                        }),
                                                    };
                                                    match outcome {
                                                        Some((aep3, pnl3, tag)) => {
                                                            *total_pnl.lock().await += pnl3;
                                                            metrics::record_trade(
                                                                &scope, Decimal::ZERO, sn.clone(), params.market_name.clone(), sid3,
                                                                p.avg_entry, aep3, p.shares, pnl3, tag,
                                                            ).await;
                                                            if let Some(pool) = db::pool_for(&asset_lc) { db::close_open_position(&pool, &sn, &tid_m.to_string()).await; }
                                                        }
                                                        None => {
                                                            // Nothing verifiable: write NO trade row and leave the
                                                            // open_positions row OPEN. A row with a fabricated $0
                                                            // is worse than none — it is a financial record that
                                                            // claims a gain and books nothing — and deleting the
                                                            // open row is how a real settlement was lost before.
                                                            // The chain sweep owns it from here and books it at a
                                                            // SOURCED price: settlement value if the market
                                                            // resolved, else the stored mark, labelled as such.
                                                            warn!(
                                                                "⚠️ EXIT rejected by exchange [{}] (\"{}\"): position already gone and the exit price is NOT verifiable — no trade row written; open row left for the chain sweep. entry=${:.4} shares={:.4} bid_at_exit=${:.4} collateral_now=${:.4} baseline={}",
                                                                sn, es.chars().take(80).collect::<String>(),
                                                                p.avg_entry, p.shares, params.price, now_collateral,
                                                                fill_baseline.map(|b| format!("${:.4}", b)).unwrap_or_else(|| "unknown".into()),
                                                            );
                                                        }
                                                    }
                                                } else {
                                                    // Entry fill never confirmed and the shares read gone — nothing
                                                    // verifiable to record. Log loudly so this is never a silent
                                                    // trade-count mismatch (silent path found 2026-08-08).
                                                    warn!("⚠️ EXIT rejected [{}] (\"{}\"): position dropped without a confirmed entry fill — no trade recorded",
                                                          sn, es.chars().take(80).collect::<String>());
                                                }
                                                token_ownership.lock().await.remove(&tid_m);
                                            }
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
                                            // Silent path found 2026-08-08: four winning TP exits vanished
                                            // here with no log — always say WHY the placement failed.
                                            warn!("⚠️ EXIT order placement failed [{}] (\"{}\"): position held, will retry after cooldown",
                                                  sn, es.chars().take(120).collect::<String>());
                                            // Bug #6: genuine placement rejection (never reached the book) —
                                            // clear the viper's exit-signal cooldown so the rejected attempt
                                            // doesn't suppress the next legitimate exit. FAK misses above are
                                            // NOT cleared: those placed successfully and the cooldown
                                            // intentionally paces re-fires there.
                                            if let Some(strat) = strategies.iter().find(|s| s.name() == sn) { strat.on_exit_order_failed(&tid_m); }
                                        }
                                        continue;
                                    }
                                }

                                        {
                                            let re_m;
                                            let remainder_m: Decimal;
                                            let rs_m;
                                            let rc_m;
                                            let pnl_m;
                                            let fees_m;
                                            let entry_fee_m;

                                            {
                                                let mut map = positions.lock().await;
                                                if let Some(p) = map.remove(&pos_key) {
                                                    // Settle from the venue's OWN answer, synchronously.
                                                    //
                                                    // A FAK is filled-or-killed at the matching engine and
                                                    // the response carries the matched size (`making`, in
                                                    // shares for a SELL). The old path used it only for the
                                                    // fill PRICE and left the QUANTITY to a 2.5s/5s/10s
                                                    // balance poll — during which the position was out of
                                                    // the map, so the strategy could not re-fire. Trade 19,
                                                    // 2026-09-01: the venue matched 0 of 24 at 21:30:52, the
                                                    // engine polled for 18s while the bid fell $0.24 → $0.19,
                                                    // and the retry sold 32 shares at $0.19 for −$3.8647.
                                                    // The retry was fresh-priced; the blackout was the loss.
                                                    //
                                                    // In simulation there is no venue answer: the simulated
                                                    // exit IS the fill.
                                                    let matched = if ghosting { Some(p.shares) } else { exit_filled_shares };
                                                    let st = fak_exit::settle(
                                                        p.shares, matched, exit_fill_price, params.price,
                                                        p.avg_entry, p.entry_fee, intl_taker_fee_rate,
                                                    );
                                                    if st.filled >= config::MIN_ORDER_SHARES && !st.fees().is_zero() {
                                                        info!("🧾 [{}] round trip {}: gross ${:.4} − fees ${:.4} (entry ${:.4} + exit ${:.4}) = ${:.4}",
                                                              sn, tid_m, (st.exit_price - p.avg_entry) * st.filled, st.fees(), st.entry_fee_booked, st.exit_fee, st.pnl);
                                                    }
                                                    // Anything the venue did not sell stays under management
                                                    // as the SAME position — its hold clock, entry fee and
                                                    // pair link intact — so the strategy re-evaluates it at
                                                    // the fresh bid on the next eligible tick. Re-inserting a
                                                    // brand-new Position here restamped `fill_confirmed_at`
                                                    // (the "0s held" family of Bug #22) and zeroed the entry
                                                    // fee out of the eventual round trip.
                                                    if st.remainder >= config::MIN_ORDER_SHARES {
                                                        map.insert(pos_key.clone(), fak_exit::retain_remainder(&p, st.remainder, st.entry_fee_left));
                                                    }

                                                    re_m = p.avg_entry;
                                                    rs_m = st.filled;
                                                    rc_m = p.close_time;
                                                    pnl_m = st.pnl;
                                                    fees_m = st.fees();
                                                    entry_fee_m = st.entry_fee_booked;
                                                    remainder_m = st.remainder;
                                                } else { continue; }
                                            }
                                            let _ = (rc_m, entry_fee_m);
                                            // Release the token claim only when nothing is left to manage.
                                            if remainder_m < config::MIN_ORDER_SHARES {
                                                token_ownership.lock().await.remove(&tid_m);
                                            }

                                        if !ghosting {
                                            if rs_m >= config::MIN_ORDER_SHARES {
                                                // Book what the venue sold, now — P&L credit, trade row and
                                                // row close together, as before, but from the venue's
                                                // synchronous answer rather than a lagging balance read.
                                                let aep_exit = exit_fill_price.unwrap_or(params.price);
                                                if remainder_m >= config::MIN_ORDER_SHARES {
                                                    warn!("⚠️ PARTIAL EXIT [{}]: venue matched {:.4}, {:.4} retained @ ${:.4} pnl=${:.4} (net of ${:.4} fees) — remainder re-fires at the fresh bid", sn, rs_m, remainder_m, aep_exit, pnl_m, fees_m);
                                                } else {
                                                    info!("✅ FAK exit confirmed [{}]: {:.4} shares sold @ ${:.4} (marked ${:.4}) pnl=${:.4} (net of ${:.4} fees)", sn, rs_m, aep_exit, params.price, pnl_m, fees_m);
                                                }
                                                // A fill materially ABOVE the bid the strategy marked
                                                // against means the book it read was behind the venue.
                                                // 2026-09-05 (intl, real money): a FairValue stop marked
                                                // at $0.58 filled at $0.67; at $0.67 the position was
                                                // inside its stop and would not have been sold. One tick
                                                // of slack: a fill exactly at the mark is the norm and a
                                                // one-tick improvement is ordinary queue movement.
                                                if aep_exit > params.price + dec!(0.01) {
                                                    warn!("⚠️ Stale mark [{}]: venue filled ${:.4} against a marked bid of ${:.4} (+${:.4}) — the book this exit was priced from was behind the venue", sn, aep_exit, params.price, aep_exit - params.price);
                                                }
                                                *total_pnl.lock().await += pnl_m;
                                                metrics::record_trade(&scope, fees_m, sn.clone(), params.market_name.clone(), side_of(&tid).to_string(), re_m, aep_exit, rs_m, pnl_m, reason.clone()).await;
                                                if let Some(pool) = db::pool_for(&asset_lc) {
                                                    if remainder_m >= config::MIN_ORDER_SHARES {
                                                        db::update_position_from_chain(&pool, tid_m.as_str(), remainder_m, re_m, None).await;
                                                    } else {
                                                        db::close_open_position(&pool, &sn, &tid_m.to_string()).await;
                                                    }
                                                }
                                            } else {
                                                // The venue matched nothing: no P&L, no ledger row, the
                                                // position is exactly as it was. The strategy re-fires at
                                                // the fresh bid after `exit_retry_cooldown_secs` — the only
                                                // wait left on this path, and it paces retries rather than
                                                // blacking them out.
                                                warn!("⚠️ EXIT [{}]: FAK matched nothing at ${:.4} — {:.4} shares retained, re-firing at the fresh bid", sn, params.price, remainder_m);
                                            }

                                            // The fill-oracle assumption, watched rather than trusted: one
                                            // balance read after the endpoint has had time to catch up,
                                            // logged if it disagrees with what the venue told us. Telemetry
                                            // only — it moves no state, so it can neither resurrect a sold
                                            // position nor fabricate an exit.
                                            {
                                                let cl = Arc::clone(&trading_client);
                                                let tid_chk = tid_m.clone();
                                                let sn_chk = sn.clone();
                                                let expected = remainder_m;
                                                tokio::spawn(async move {
                                                    tokio::time::sleep(Duration::from_secs(config::SETTLEMENT_LAG_RETRY_DELAY_SECS)).await;
                                                    let mut req = BalanceAllowanceRequest::default(); req.asset_type = AssetType::Conditional; req.token_id = Some(u256_from_market_id(&tid_chk).unwrap_or_default());
                                                    if let Ok(r) = cl.balance_allowance(req).await {
                                                        let b = Decimal::from_str(&r.balance.to_string()).unwrap_or(dec!(0)) / dec!(1_000_000);
                                                        if (b - expected).abs() >= config::MIN_ORDER_SHARES {
                                                            warn!("⚠️ FAK fill-oracle check [{}]: venue response implied {:.4} shares retained, balance reads {:.4} — investigate before trusting either", sn_chk, expected, b);
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    let mut paired_pnl = dec!(0);
                                    if exit_pair {
                                        let other_tid = if tid == target_yes_token { target_no_token.clone() } else { target_yes_token.clone() };
                                        let other_tid_m = other_tid.clone(); // neutral key (slice 2a)
                                        let pk = PositionKey::new(sq_key.clone(), sn.clone(), other_tid_m.clone()); let ps = { let map = positions.lock().await; map.get(&pk).map(|p| p.shares) };
                                        if let Some(s) = ps {
                                            let exit_snap = if target_yes_token == ctx.market.yes_token {
                                                &ctx.snapshot
                                            } else {
                                                ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot)
                                            };
                                            let other_bid = if other_tid == target_yes_token { exit_snap.yes_bid } else { exit_snap.no_bid };
                                            let other_fee_bps = if other_tid == target_yes_token { target_yes_fee_bps as u16 } else { target_no_fee_bps as u16 };
                                            let other_vc = if target_is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                                if !ghosting { let _ = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, other_vc, &other_tid, Side::Sell, s, (other_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE), other_fee_bps, crate::venues::core::TimeInForce::Fak, false, 0, &shared_http).await; }
                                                    let mut map = positions.lock().await; if let Some(p) = map.remove(&pk) { let actual_other_exit = (other_bid - config::SELL_PRICE_OFFSET).max(config::MIN_SELL_LIMIT_PRICE); let pnl = (actual_other_exit - p.avg_entry) * p.shares; paired_pnl = pnl; *total_pnl.lock().await += pnl;
                                                // Release paired token claim.
                                                token_ownership.lock().await.remove(&other_tid_m);
                                                {
                                                    let sn_pm = sn.clone(); let m_name = params.market_name.clone(); let sid = side_of(&other_tid).to_string(); let p_avg = p.avg_entry; let o_bid = actual_other_exit; let p_shares = p.shares; let pn = pnl; let scope_pm = scope.clone();
                                                    tokio::spawn(async move { metrics::record_trade(&scope_pm, Decimal::ZERO, sn_pm, m_name, sid, p_avg, o_bid, p_shares, pn, "Convergence/PairedExit".to_string()).await; });
                                                }
                                                {
                                                    let sn_cp = sn.clone(); let tid_cp = other_tid.to_string(); let asset_c = asset_lc.clone();
                                                    tokio::spawn(async move { if let Some(pool) = db::pool_for(&asset_c) { db::close_open_position(&pool, &sn_cp, &tid_cp).await; } });
                                                }
                                            }
                                        }
                                    }
                                    // Ghost mode has no fill to wait for, so record here.
                                    //
                                    // The verification block above is gated on `!ghosting` and has
                                    // no ghost counterpart, so a simulated round trip removed the
                                    // position, logged its P&L, and wrote NOTHING: no trade row, no
                                    // closed position, no session-P&L credit. Everything an operator
                                    // reads — the dashboard, P&L history, win rate, and the trade
                                    // context the LLM advisor is given — comes from that table.
                                    //
                                    // This is the DEFAULT first-run experience, which is what makes
                                    // it worth more than tidiness: GHOST_MODE_DEFAULT is true for a
                                    // new install and intl_clob is the default build, so a customer's
                                    // first hour is an engine visibly trading in the log beside a
                                    // dashboard insisting nothing has happened. Kalshi records its
                                    // simulated exits; this venue did not.
                                    //
                                    // The simulated exit IS the fill — there is nothing to confirm —
                                    // so the same three effects the confirmed-fill branch applies are
                                    // applied here, immediately.
                                    if ghosting && rs_m > dec!(0) {
                                        *total_pnl.lock().await += pnl_m;
                                        let scope_g = scope.clone();
                                        let sn_g = sn.clone();
                                        let mn_g = params.market_name.clone();
                                        let sid_g = side_of(&tid_m).to_string();
                                        let asset_g = asset_lc.clone();
                                        let tid_g = tid_m.to_string();
                                        let (fees_g, re_g, aep_g, rs_g, pnl_g, r_g) =
                                            (fees_m, re_m, exit_fill_price.unwrap_or(params.price), rs_m, pnl_m, reason.clone());
                                        tokio::spawn(async move {
                                            metrics::record_trade(
                                                &scope_g, fees_g, sn_g.clone(), mn_g, sid_g,
                                                re_g, aep_g, rs_g, pnl_g, r_g,
                                            ).await;
                                            if let Some(pool) = db::pool_for(&asset_g) {
                                                db::close_open_position(&pool, &sn_g, &tid_g).await;
                                            }
                                        });
                                    }
                                    info!("📊 Exit order placed [{}]: est. PnL ${:.4} — awaiting fill verification", sn, pnl_m + paired_pnl);
                                    let reason_lc = reason.to_lowercase();
                                    if reason_lc.contains("sl")
                                        || reason_lc.contains("stop")
                                        || reason_lc.contains("toxic")
                                        || reason_lc.contains("skewcollapse")
                                    {
                                        last_stop_loss_time.insert(sn.clone(), Instant::now());
                                        // Loss streak → escalating re-entry cooldown (2026-08-08:
                                        // Basis revenge-traded the same falling market 20x).
                                        let c = consecutive_stop_losses.entry(sn.clone()).or_insert(0);
                                        *c += 1;
                                    } else {
                                        consecutive_stop_losses.insert(sn.clone(), 0);
                                    }
                                    if reason_lc.contains("expir") { last_expiry_exit_time.insert(sn.clone(), Instant::now()); }
                                    last_trade_time.insert(sn.clone(), Instant::now());
                                    { let tok = tg_token.clone(); let cid = tg_chat_id.clone(); let msg = format!("🔴 EXIT [{}] {} | bid=${:.4} | reason: {} | Session PnL: ${:.4}", sn, params.market_name, params.price, reason, *total_pnl.lock().await); tokio::spawn(async move { let _ = send_notification(&tok, &cid, &msg).await; }); }
                                    { let session_pnl = *total_pnl.lock().await; tweet_trade(tw_api_key.clone(), tw_api_secret.clone(), tw_access_token.clone(), tw_access_token_secret.clone(), sn.clone(), params.market_name.clone(), re_m, params.price, reason.clone(), pnl_m + paired_pnl, session_pnl); }
                                }
                            }

                            // ════════════════════ ENTRY ════════════════════
                            StrategySignal::Entry { params, pair_params } => {
                                let token_m = params.token_id.clone(); // neutral key (slice 2a)
                                // Venue-appropriate snapshot for entry-signal logging: Window/Daily
                                // strategies (e.g. TrendCapture) evaluate the maker snapshot; hourly
                                // strategies the primary snapshot. Captured here so both the ghost and
                                // live entry paths can persist the feature-vector behind the fill.
                                let entry_snap: crate::state::MarketSnapshot = {
                                    let strat_venue = strategies.iter().find(|s| s.name() == sn).map(|s| s.venue()).unwrap_or("Hourly");
                                    if strat_venue == "Window/Daily" {
                                        ctx.maker_snapshot.clone().unwrap_or_else(|| ctx.snapshot.clone())
                                    } else {
                                        ctx.snapshot.clone()
                                    }
                                };
                                if let Some(close_time) = target_market_close_time { if (close_time - Utc::now()).num_seconds() < config::MIN_SECONDS_TO_EXPIRY_FOR_ENTRY { continue; } }
                                if let Some(lt) = last_trade_time.get(&sn) { if lt.elapsed() < Duration::from_secs(config::TRADE_COOLDOWN_SECS as u64) { continue; } }
                                if let Some(lt) = last_stop_loss_time.get(&sn) {
                                    // Escalate with the loss streak: 180s → 360s → 720s → 1440s, capped 1h.
                                    let streak = consecutive_stop_losses.get(&sn).copied().unwrap_or(0);
                                    let mult = 1u64 << streak.saturating_sub(1).min(5);
                                    let cd = (config::STOP_LOSS_COOLDOWN_SECS * mult).min(3600);
                                    if lt.elapsed() < Duration::from_secs(cd) { continue; }
                                }
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
                                    if pm.contains_key(&PositionKey::new(sq_key.clone(), sn.clone(), other_token.clone())) { debug!("⏳ ENTRY blocked — already hold opposite leg in same market [{}] — must exit first", sn); continue; }
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
                                                // Sovereignty is strict per-token: on-chain the token is a
                                                // single fungible ERC-1155 balance, so two strategies holding
                                                // it cannot be reconciled independently (one's sync clobbers
                                                // the other). A residual dust holder still keeps the claim
                                                // until its position actually settles — letting a second
                                                // strategy pile onto the shared balance caused the 2026-06-27
                                                // Basis commingling loss.
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
                                    let blocked = pm.iter().any(|(k, _p)| {
                                        if k.squadron != sq_key { return false; }
                                        let (other_sn, tid) = (&k.strategy, &k.market);
                                        *tid == token_m && other_sn != &sn
                                    });
                                    if blocked {
                                        warn!(
                                            "🚫 TOKEN SOVEREIGNTY [{}]: token {} held by another strategy \
                                             (registry miss — positions scan fallback)",
                                            sn, params.token_id,
                                        );
                                        last_trade_time.insert(sn.clone(), Instant::now());
                                        continue;
                                    }
                                }

                                let pos_key = PositionKey::new(sq_key.clone(), sn.clone(), token_m.clone());

                                {
                                    let pending = pending_orders.lock().await;
                                    if let Some(expiry) = pending.get(&pos_key) { if expiry > &Instant::now() { continue; } }
                                }

                                    if ghosting {
                                    if positions.lock().await.contains_key(&pos_key) { continue; }
                                    let pos_close_time = target_market_close_time;
                                    // Simulate the fill at the TOUCH, not at the crossing limit.
                                    // BUY_PRICE_OFFSET only lifts the limit so a marketable FAK
                                    // sweeps the book; the executed price is the real best ask.
                                    // Booking the offset limit overstated every simulated entry by
                                    // a tick, so ghost P&L was pessimistic against live by ~1 tick
                                    // per entry and the two could not be compared.
                                    let actual_entry_price = params.price;
                                    positions.lock().await.insert(pos_key.clone(), Position { shares: params.shares, avg_entry: actual_entry_price, opened_at: Utc::now(), close_time: pos_close_time, market_name: params.market_name.clone(), pair_token_id: token_m.clone(), fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: pair_params.as_ref().map(|p| p.token_id.clone()), entry_fee: Decimal::ZERO });
                                    token_ownership.lock().await.insert(token_m.clone(), sn.clone());
                                    let side_g = side_of(&params.token_id);
                                    info!("👻 GHOST_MODE ENTRY {} [{}]: {} | ${:.4} x {:.1} (simulated)", side_g, sn, params.market_name, params.price, params.shares);
                                    { let side_g = side_of(&params.token_id); let sn_g = sn.clone(); let tid_g = params.token_id.to_string(); let mn_g = params.market_name.clone(); let side_gs = side_g.to_string(); let ep_g = actual_entry_price; let sh_g = params.shares; let asset_g = asset_lc.clone(); let scope_g = scope.clone(); tokio::spawn(async move { metrics::record_entry(&scope_g, sn_g, tid_g, mn_g, side_gs, ep_g, sh_g).await; }); }
                                    { let side_g = side_of(&params.token_id); let sn_g = sn.clone(); let tid_g = params.token_id.to_string(); let mn_g = params.market_name.clone(); let side_gs = side_g.to_string(); let ep_g = actual_entry_price; let sh_g = params.shares; let asset_g = asset_lc.clone(); let snap_g = entry_snap.clone(); tokio::spawn(async move { metrics::record_entry_signal(&asset_g, sn_g, tid_g, mn_g, side_gs, ep_g, sh_g, &snap_g).await; }); }
                                    if let Some(pool) = db::pool_for(&asset_lc) { let side_g = side_of(&params.token_id); db::record_open_position(&pool, &scope, &squadron_id, &sn, &params.token_id.to_string(), &params.market_name, side_g, actual_entry_price, params.shares, true).await; }
                                    if let Some(pp) = pair_params {
                                        let pp_close_time = target_market_close_time;
                                        // Same as the primary leg: simulate at the touch.
                                        let actual_paired_entry_price = pp.price;
                                        positions.lock().await.insert(PositionKey::new(sq_key.clone(), sn.clone(), pp.token_id.clone()), Position { shares: pp.shares, avg_entry: actual_paired_entry_price, opened_at: Utc::now(), close_time: pp_close_time, market_name: pp.market_name.clone(), pair_token_id: pp.token_id.clone(), fill_confirmed_at: Some(Utc::now()), paired_leg_token_id: Some(token_m.clone()), entry_fee: Decimal::ZERO });
                                        token_ownership.lock().await.insert(pp.token_id.clone(), sn.clone());
                                        let side_gp = side_of(&pp.token_id);
                                        info!("👻 GHOST_MODE ENTRY {} (paired) [{}]: {} | ${:.4} x {:.1} (simulated)", side_gp, sn, pp.market_name, pp.price, pp.shares);
                                        { let side_gp = side_of(&pp.token_id); let sn_gp = sn.clone(); let tid_gp = pp.token_id.to_string(); let mn_gp = pp.market_name.clone(); let side_gps = side_gp.to_string(); let ep_gp = actual_paired_entry_price; let sh_gp = pp.shares; let asset_gp = asset_lc.clone(); let scope_gp = scope.clone(); tokio::spawn(async move { metrics::record_entry(&scope_gp, sn_gp, tid_gp, mn_gp, side_gps, ep_gp, sh_gp).await; }); }
                                        if let Some(pool) = db::pool_for(&asset_lc) { let side_gp = side_of(&pp.token_id); db::record_open_position(&pool, &scope, &squadron_id, &sn, &pp.token_id.to_string(), &pp.market_name, side_gp, actual_paired_entry_price, pp.shares, true).await; }
                                    }
                                    last_trade_time.insert(sn.clone(), Instant::now());
                                } else {
                                    let actual_entry_price = if params.post_only { params.price } else { (params.price + config::BUY_PRICE_OFFSET).min(config::MAX_BUY_LIMIT_PRICE) };
                                    {
                                        let mut map = positions.lock().await; if map.contains_key(&pos_key) { continue; }
                                        let pos_close_time = target_market_close_time;
                                        map.insert(pos_key.clone(), Position { shares: params.shares, avg_entry: actual_entry_price, opened_at: Utc::now(), close_time: pos_close_time, market_name: params.market_name.clone(), pair_token_id: token_m.clone(), fill_confirmed_at: None, paired_leg_token_id: pair_params.as_ref().map(|p| p.token_id.clone()), entry_fee: Decimal::ZERO });
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
                                                // Only venue-side failures are exempted here — the 425
                                                // (venue said not processed) and the execution-engine
                                                // 500 (`Ambiguous`, which the placement helper no
                                                // longer resends: a batch resend would be a second
                                                // PAIR of freshly-salted legs). Narrower than the
                                                // single-leg entry arm's carve-out, deliberately: the
                                                // batch path had no crosses-book exemption before, and
                                                // this change should not loosen its fault accounting
                                                // beyond the venue-failure classes.
                                                if matches!(classify_placement_error(&e),
                                                            PlacementFault::VenueUnavailable | PlacementFault::Ambiguous) {
                                                    info!("⏸️ Arb batch [{}]: venue-side failure (not a strategy fault) — no cooldown, no breaker count; retrying after {}s", sn, config::CROSSES_BOOK_RETRY_PAUSE_SECS);
                                                    tokio::time::sleep(Duration::from_secs(config::CROSSES_BOOK_RETRY_PAUSE_SECS)).await;
                                                } else {
                                                    last_trade_time.insert(sn.clone(), Instant::now());
                                                    consecutive_failures += 1;
                                                }
                                                continue;
                                            }
                                            Ok((leg_a_id, leg_b_id)) => {
                                                let primary_wait_secs = if target_yes_token == hourly_yes_token { crate::helpers::balance::MAX_WAIT_SECS_HOURLY } else { crate::helpers::balance::MAX_WAIT_SECS_WINDOW };
                                                let cl_s = Arc::clone(&trading_client); let ps_s = Arc::clone(&positions); let pc_s = Arc::clone(&phantom_cooldowns); let to_s = Arc::clone(&token_ownership); let sn_s = sn.clone(); let tn_s = params.token_id.clone();
                                                let db_sn_a = sn.clone(); let db_tid_a = params.token_id.to_string(); let db_mn_a = params.market_name.clone();
                                                let db_side_a = side_of(&params.token_id); let db_ep_a = actual_entry_price; let db_sh_a = params.shares; let asset_a = asset_lc.clone(); let scope_a = scope.clone();
                                                // Write pending position immediately (Viper Launch)
                                                if let Some(pool) = db::pool_for(&asset_a) {
                                                    db::record_open_position_with_status(&pool, &scope, &squadron_id, &sn, &db_tid_a, &db_mn_a, db_side_a, db_ep_a, db_sh_a, false, "pending").await;
                                                }
                                                let sq_bal = squadron_id.clone();
                                                tokio::spawn(async move {
                                                    if let Ok(true) = sync_position_balance(&sq_bal, &cl_s, &ps_s, &sn_s, &tn_s, Some(&pc_s), primary_baseline, primary_wait_secs, &to_s, false).await {
                                                        // Update to confirmed (Mission In-Flight) + record entry
                                                        if let Some(pool) = db::pool_for(&asset_a) {
                                                            db::confirm_position_status(&pool, &db_sn_a, &db_tid_a).await;
                                                        }
                                                        metrics::record_entry(&scope_a, db_sn_a, db_tid_a, db_mn_a, db_side_a.to_string(), db_ep_a, db_sh_a).await;
                                                    }
                                                });

                                        let pp_close_time = target_market_close_time;
                                        positions.lock().await.insert(PositionKey::new(sq_key.clone(), sn.clone(), pp_token_m.clone()), Position { shares: pp.shares, avg_entry: actual_pair_entry_price, opened_at: Utc::now(), close_time: pp_close_time, market_name: pp.market_name.clone(), pair_token_id: pp_token_m.clone(), fill_confirmed_at: None, paired_leg_token_id: Some(token_m.clone()), entry_fee: Decimal::ZERO });
                                        // Claim paired token in registry.
                                        token_ownership.lock().await.insert(pp_token_m.clone(), sn.clone());

                                                let pair_wait_secs = if pp.token_id == hourly_yes_token || pp.token_id == hourly_no_token { crate::helpers::balance::MAX_WAIT_SECS_HOURLY } else { crate::helpers::balance::MAX_WAIT_SECS_WINDOW };
                                                let sn_p = sn.clone(); let tn_p = pp.token_id.clone(); let ps_p = Arc::clone(&positions); let cl_p = Arc::clone(&trading_client); let pc_p = Arc::clone(&phantom_cooldowns); let to_p = Arc::clone(&token_ownership);
                                                let db_sn_b = sn.clone(); let db_tid_b = pp.token_id.to_string(); let db_mn_b = pp.market_name.clone();
                                                let db_side_b = side_of(&pp.token_id); let db_ep_b = actual_pair_entry_price; let db_sh_b = pp.shares; let asset_b = asset_lc.clone(); let scope_b = scope.clone();
                                                // Write pending position immediately (Viper Launch)
                                                if let Some(pool) = db::pool_for(&asset_b) {
                                                    db::record_open_position_with_status(&pool, &scope, &squadron_id, &sn, &db_tid_b, &db_mn_b, db_side_b, db_ep_b, db_sh_b, false, "pending").await;
                                                }
                                                let sq_bal = squadron_id.clone();
                                                tokio::spawn(async move {
                                                    if let Ok(true) = sync_position_balance(&sq_bal, &cl_p, &ps_p, &sn_p, &tn_p, Some(&pc_p), pair_baseline, pair_wait_secs, &to_p, false).await {
                                                        // Update to confirmed (Mission In-Flight) + record entry
                                                        if let Some(pool) = db::pool_for(&asset_b) {
                                                            db::confirm_position_status(&pool, &db_sn_b, &db_tid_b).await;
                                                        }
                                                        metrics::record_entry(&scope_b, db_sn_b, db_tid_b, db_mn_b, db_side_b.to_string(), db_ep_b, db_sh_b).await;
                                                    }
                                                });

                                                {
                                                    let arb_cl = Arc::clone(&trading_client); let arb_nm = Arc::clone(&nonce_manager); let arb_sg = signer.clone(); let arb_ps = Arc::clone(&positions); let arb_pc = Arc::clone(&phantom_cooldowns); let arb_to = Arc::clone(&token_ownership); let arb_sn = sn.clone(); let arb_http = shared_http.clone();
                                                    let arb_tok_a = params.token_id.clone(); let arb_tok_b = pp.token_id.clone(); let arb_base_a = primary_baseline; let arb_base_b = pair_baseline;
                                                    let arb_side_a = side_of(&params.token_id).to_string();
                                                    let arb_side_b = side_of(&pp.token_id).to_string();
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
                                                    let arb_tp = Arc::clone(&total_pnl);
                                                    let arb_scope = scope.clone();
                                                    let sq_bal = squadron_id.clone();
                                                    tokio::spawn(async move {
                                                        crate::helpers::balance::arb_pair_fill_monitor(
                                                            &sq_bal,
                                                            arb_cl, arb_nm, arb_sg, safe_address, eoa_address, vc, vc_p,
                                                            arb_ps, arb_pc, arb_to, arb_sn, &arb_tok_a, &arb_tok_b,
                                                            arb_base_a, arb_base_b, arb_side_a, arb_side_b, arb_wait, arb_http, arb_asset,
                                                            arb_scope,
                                                            arb_tp,
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
                                                        price: actual_entry_price, fee: Decimal::ZERO
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
                                                        price: actual_pair_entry_price, fee: Decimal::ZERO
                                                    },
                                                    &sn,
                                                    crate::venues::core::TimeInForce::Gtc,
                                                    Some(params.token_id.clone()),
                                                ).await;
                                            }
                                        }
                                    } else {
                                        // Book the price the venue actually traded at, not the limit.
                                        //
                                        // BUY_PRICE_OFFSET only LIFTS the limit so a marketable FAK
                                        // reliably sweeps the book — the executed price is the real
                                        // best ask. Booking the offset limit inflated `avg_entry` by
                                        // a full tick on every taker entry, which then propagated
                                        // into every TP/SL comparison (profit understated, so TPs
                                        // fired late and SLs early) and into recorded P&L. It is
                                        // also why Convergence trade 347 was recorded at $0.66 when
                                        // its own $0.65 max-entry gate had passed: the gate saw the
                                        // ask, the ledger saw the limit.
                                        //
                                        // Mirror of the exit-side fix above, and of the fix already
                                        // living in `IntlClobVenue::place_order`.
                                        let (leg_a_order_id, entry_fill_price) = match place_limit_order_filled(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, &params.token_id, Side::Buy, params.shares, actual_entry_price, target_yes_fee_bps as u16, params.order_type, params.post_only, 0, &shared_http).await {
                                            Err(e) => {
                                                warn!("⚠️ ENTRY order failed [{}]: {}", sn, e);
                                                positions.lock().await.remove(&pos_key);
                                                pending_orders.lock().await.remove(&pos_key);
                                                token_ownership.lock().await.remove(&token_m);
                                                match classify_placement_error(&e) {
                                                    // Venue said it did not process the order (425
                                                    // "order manager not ready"). Charging the 60s
                                                    // cooldown plus a breaker count for a venue outage
                                                    // is what left FairValue 0-for-3 on 2026-09-01: two
                                                    // of its three entry attempts were 425s inside a
                                                    // ~3-minute order-manager outage, two counts against
                                                    // a breaker threshold of 3. Pause briefly so an
                                                    // extended outage is probed at ~1 attempt / 5s
                                                    // instead of every 50ms tick, then re-evaluate and
                                                    // enter naturally once the venue recovers.
                                                    // `Ambiguous` (execution-engine 500, no statement of
                                                    // whether the order was processed) rides with it: also
                                                    // not the strategy's fault, also never resent — by next
                                                    // tick balances are re-read and chain reconciliation
                                                    // adopts the order if it landed.
                                                    PlacementFault::VenueUnavailable | PlacementFault::Ambiguous => {
                                                        info!("⏸️ ENTRY [{}]: venue-side failure (not a strategy fault) — no cooldown, no breaker count; retrying after {}s", sn, config::CROSSES_BOOK_RETRY_PAUSE_SECS);
                                                        tokio::time::sleep(Duration::from_secs(config::CROSSES_BOOK_RETRY_PAUSE_SECS)).await;
                                                    }
                                                    // Book moved between pricing and placement — the
                                                    // historical "crosses book" carve-out, now joined by
                                                    // the FAK no-match 400 ("no orders found to match"),
                                                    // which is the same race in taker form: a definitive
                                                    // no-liquidity answer, not a malfunction. It used to
                                                    // eat the full 60s cooldown AND a breaker count; the
                                                    // next evaluation reprices from a fresh snapshot.
                                                    PlacementFault::BookRace => {
                                                        tokio::time::sleep(Duration::from_secs(config::CROSSES_BOOK_RETRY_PAUSE_SECS)).await;
                                                    }
                                                    PlacementFault::Strategy => {
                                                        last_trade_time.insert(sn.clone(), Instant::now());
                                                        consecutive_failures += 1;
                                                    }
                                                }
                                                continue;
                                            }
                                            Ok((id, making, taking)) => {
                                                // BUY orientation: making = USDC paid, taking = shares
                                                // received. Ratio is unit-invariant. A post_only order
                                                // rests and matches nothing now (making/taking = 0), so
                                                // it falls back to the touch — which for post_only IS
                                                // its limit, so that stays correct.
                                                let px = if making > dec!(0) && taking > dec!(0) {
                                                    let p = making / taking;
                                                    if p > dec!(0) && p <= dec!(1) { Some(p) } else { None }
                                                } else { None };
                                                (id, px.unwrap_or(params.price))
                                            }
                                        };
                                        let _ = leg_a_order_id;
                                        // The exchange charges its taker fee out of collateral and
                                        // reports it nowhere: making/taking come back as the matched
                                        // amounts BEFORE the fee, so booking that price alone leaves
                                        // the whole cost invisible to P&L. Carry it on the position
                                        // so the exit can net it out of the round trip.
                                        let entry_fee_paid = crate::venues::intl::taker_fee(
                                            intl_taker_fee_rate, entry_fill_price, params.shares,
                                        );
                                        // Correct the provisional booking made before placement.
                                        if let Some(p) = positions.lock().await.get_mut(&pos_key) {
                                            p.avg_entry = entry_fill_price;
                                            p.entry_fee = entry_fee_paid;
                                        }
                                        let cl_s = Arc::clone(&trading_client); let ps_s = Arc::clone(&positions); let pc_s = Arc::clone(&phantom_cooldowns); let to_s = Arc::clone(&token_ownership); let sn_s = sn.clone(); let tn_s = params.token_id.clone();
                                        let primary_wait_secs = if target_yes_token == hourly_yes_token { crate::helpers::balance::MAX_WAIT_SECS_HOURLY } else { crate::helpers::balance::MAX_WAIT_SECS_WINDOW };
                                        let db_sn_s = sn.clone(); let db_tid_s = params.token_id.to_string(); let db_mn_s = params.market_name.clone();
                                        let db_side_s = side_of(&params.token_id); let db_ep_s = entry_fill_price; let db_sh_s = params.shares; let asset_s = asset_lc.clone(); let scope_s = scope.clone();
                                        let feat_snap_s = entry_snap.clone();
                                        // Write pending position immediately (Viper Launch)
                                        if let Some(pool) = db::pool_for(&asset_s) {
                                            db::record_open_position_with_status(&pool, &scope, &squadron_id, &sn, &db_tid_s, &db_mn_s, db_side_s, db_ep_s, db_sh_s, false, "pending").await;
                                            // Stamp what opening this leg cost. If the position
                                            // later leaves via settlement or an off-strategy
                                            // close, that booking happens in db.rs with no view
                                            // of the in-memory Position — this row is its only
                                            // source for the entry fee.
                                            db::set_open_position_entry_fee(&pool, &db_tid_s, entry_fee_paid).await;
                                        }
                                        let sq_bal = squadron_id.clone();
                                        tokio::spawn(async move {
                                            if let Ok(true) = sync_position_balance(&sq_bal, &cl_s, &ps_s, &sn_s, &tn_s, Some(&pc_s), primary_baseline, primary_wait_secs, &to_s, false).await {
                                                // Update to confirmed (Mission In-Flight) + record entry
                                                if let Some(pool) = db::pool_for(&asset_s) {
                                                    db::confirm_position_status(&pool, &db_sn_s, &db_tid_s).await;
                                                }
                                                metrics::record_entry(&scope_s, db_sn_s.clone(), db_tid_s.clone(), db_mn_s.clone(), db_side_s.to_string(), db_ep_s, db_sh_s).await;
                                                metrics::record_entry_signal(&asset_s, db_sn_s, db_tid_s, db_mn_s, db_side_s.to_string(), db_ep_s, db_sh_s, &feat_snap_s).await;
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
                                    let pk = PositionKey::new(sq_key.clone(), sn.clone(), p_token_m.clone());
                                    { let pending = pending_orders.lock().await; if let Some(expiry) = pending.get(&pk) { if expiry > &Instant::now() { continue; } } }
                                    if ghosting {
                                        // Already holding a filled position on this leg.
                                        if positions.lock().await.contains_key(&pk) { continue; }
                                        // The quote RESTS. It becomes a position only when
                                        // the ask crosses it — see the fill simulation at
                                        // the top of the tick.
                                        // A resting quote's price is FROZEN until it
                                        // fills or is pulled, exactly as the live branch
                                        // below drops re-emissions while a quote exists.
                                        //
                                        // Repricing on every re-emission looked faithful
                                        // and was the opposite: the quote is priced at
                                        // `ask - maker_cross_buffer`, so following the ask
                                        // down keeps the same gap open forever and an ask
                                        // drifting toward the quote can never reach it.
                                        // Only a gap larger than the cross buffer inside a
                                        // single 50ms tick would fill, which is precisely
                                        // the adversely-selected fill. Simulated results
                                        // would then carry every pick-off and none of the
                                        // ordinary fills, understating Maker as badly as
                                        // the treadmill overstated it.
                                        if !crate::helpers::ghost_quotes::rest(pk.clone(), Position { shares: p.shares, avg_entry: p.price, opened_at: Utc::now(), close_time: None, market_name: p.market_name.clone(), pair_token_id: p_token_m.clone(), fill_confirmed_at: None, paired_leg_token_id: None, entry_fee: Decimal::ZERO }, p.clone()) { continue; }
                                        info!("👻 GHOST_MODE MakerQuote [{}]: {} | shares={:.2}, bid=${:.4} — resting until ask crosses (simulated)", sn, p.market_name, p.shares, p.price);
                                        placed = true;
                                    } else {
                                        if !positions.lock().await.contains_key(&pk) {
                                            info!("📝 MakerQuote [{}]: {} | shares={:.2}, bid=${:.4}", sn, p.market_name, p.shares, p.price);
                                            positions.lock().await.insert(pk.clone(), Position { shares: p.shares, avg_entry: p.price, opened_at: Utc::now(), close_time: None, market_name: p.market_name.clone(), pair_token_id: p_token_m.clone(), fill_confirmed_at: None, paired_leg_token_id: None, entry_fee: Decimal::ZERO });
                                            token_ownership.lock().await.insert(p_token_m.clone(), sn.clone());
                                            { pending_orders.lock().await.insert(pk.clone(), Instant::now() + Duration::from_secs(3)); }
                                            let _ = tokio::time::timeout(Duration::from_secs(10), crate::helpers::balance::quick_confirm_fill(&squadron_id, &trading_client, &sn, &p.token_id, &positions, &p.condition_id, p.order_type.clone())).await;
                                            let vc = if p.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                            if let Err(e) = place_limit_order(&trading_client, &nonce_manager, &signer, safe_address, eoa_address, vc, &p.token_id, Side::Buy, p.shares, p.price, target_yes_fee_bps as u16, p.order_type, true, 0, &shared_http).await {
                                                positions.lock().await.remove(&pk);
                                                // Release token claim — order placement failed.
                                                token_ownership.lock().await.remove(&p_token_m);
                                                match classify_placement_error(&e) {
                                                    PlacementFault::BookRace => {
                                                        // Post-only quote crossed the book: the viper re-signals
                                                        // every ~50ms tick while the book stays crossed, hammering
                                                        // the CLOB (2026-07-25: 84 rejected placements in ~2s).
                                                        // Park a pending-order expiry to suppress re-quoting until
                                                        // the book has had time to move.
                                                        pending_orders.lock().await.insert(pk.clone(), Instant::now() + Duration::from_secs(config::MAKER_CROSSES_BOOK_COOLDOWN_SECS));
                                                        info!("⏸️ Maker quote crossed book [{}]: {} — re-quote suppressed {}s", sn, p.market_name, config::MAKER_CROSSES_BOOK_COOLDOWN_SECS);
                                                    }
                                                    // Venue said it did not process the quote (425). Not the
                                                    // maker's fault, so no breaker count — but still pause
                                                    // before the next attempt so an extended outage is probed
                                                    // at ~1 quote / 5s rather than at tick rate.
                                                    PlacementFault::VenueUnavailable | PlacementFault::Ambiguous => {
                                                        pending_orders.lock().await.remove(&pk);
                                                        info!("⏸️ Maker quote [{}]: venue-side failure (not a strategy fault) — no breaker count; retrying after {}s", sn, config::CROSSES_BOOK_RETRY_PAUSE_SECS);
                                                        tokio::time::sleep(Duration::from_secs(config::CROSSES_BOOK_RETRY_PAUSE_SECS)).await;
                                                    }
                                                    PlacementFault::Strategy => {
                                                        pending_orders.lock().await.remove(&pk);
                                                        consecutive_failures += 1;
                                                    }
                                                }
                                                continue;
                                            }
                                            let cl_m = Arc::clone(&trading_client); let ps_m = Arc::clone(&positions); let pc_m = Arc::clone(&phantom_cooldowns); let to_m = Arc::clone(&token_ownership); let sn_m = sn.clone();
                                            let tid_em = p.token_id.to_string(); let mn_em = p.market_name.clone();
                                            let side_em = side_of(&p.token_id).to_string();
                                            let ep_em = p.price; let sh_em = p.shares; let asset_em = asset_lc.clone(); let scope_em = scope.clone();
                                            // Maker quotes evaluate the Window/Daily book — capture that
                                            // snapshot so the entry_signals feature-vector matches what
                                            // the strategy actually saw (falls back to primary snapshot).
                                            let feat_snap_m = ctx.maker_snapshot.clone().unwrap_or_else(|| ctx.snapshot.clone());
                                            // Write pending position immediately (Viper Launch)
                                            if let Some(pool) = db::pool_for(&asset_em) {
                                                db::record_open_position_with_status(&pool, &scope, &squadron_id, &sn, &tid_em, &mn_em, &side_em, ep_em, sh_em, false, "pending").await;
                                            }
                                            // Claim this quote's epoch. Anything that pulls or
                                            // replaces the quote bumps it, which is how the task
                                            // below learns its quote is no longer the live one.
                                            let epoch_map = Arc::clone(&quote_epochs);
                                            let my_epoch = bump_quote_epoch(&mut *epoch_map.lock().await, &pk);
                                            let epoch_key = pk.clone();
                                            let sq_bal = squadron_id.clone();
                                            tokio::spawn(async move {
                                                if let Ok(true) = sync_position_balance(&sq_bal, &cl_m, &ps_m, &sn_m, &p.token_id, Some(&pc_m), dec!(0), crate::helpers::balance::MAX_WAIT_SECS_WINDOW, &to_m, true).await {
                                                    // Shares exist — but are they THIS quote's? If the
                                                    // epoch moved, this quote was pulled or replaced and
                                                    // the fill belongs to whatever came after it.
                                                    let still_live = quote_epoch_is_current(
                                                        &*epoch_map.lock().await, &epoch_key, my_epoch);
                                                    if !still_live {
                                                        info!("↩️ Maker fill-watch [{}] {}: quote @ ${:.4} x{:.2} was pulled or replaced (epoch {} superseded) — not recording an entry for it",
                                                              sn_m, tid_em, ep_em, sh_em, my_epoch);
                                                        return;
                                                    }
                                                    // Update to confirmed (Mission In-Flight) + record entry
                                                    if let Some(pool) = db::pool_for(&asset_em) {
                                                        db::confirm_position_status(&pool, &sn_m, &tid_em).await;
                                                    }
                                                    metrics::record_entry(&scope_em, sn_m.clone(), tid_em.clone(), mn_em.clone(), side_em.clone(), ep_em, sh_em).await;
                                                    metrics::record_entry_signal(&asset_em, sn_m, tid_em, mn_em, side_em, ep_em, sh_em, &feat_snap_m).await;
                                                }
                                            });
                                        }
                                        placed = true;
                                    }
                                }
                                if placed { last_trade_time.insert(sn.clone(), Instant::now()); }
                                if consecutive_failures >= config::MAX_CONSECUTIVE_FAILURES { error!("🚨 Circuit breaker hit!"); tokio::time::sleep(Duration::from_secs(60)).await; consecutive_failures = 0; }
                            }
                            // ════════════════════ MAKER QUOTE-PULL ════════════════════
                            // Cancel resting UNFILLED maker quotes whose book turned toxic
                            // before they filled — pull them off the book so informed flow
                            // can't pick them off (the noon-ET adverse-selection losses).
                            StrategySignal::MakerCancel { tokens } => {
                                for tok in tokens {
                                    let pk = PositionKey::new(sq_key.clone(), sn.clone(), tok.clone());
                                    // In ghost mode an unfilled quote is not in the
                                    // position map at all — pulling it is just dropping
                                    // the simulated resting order.
                                    if ghosting {
                                        if let Some(pulled) = crate::helpers::ghost_quotes::pull(&pk) {
                                            info!("👻 GHOST_MODE MakerCancel [{}]: {} | pulling unfilled quote @ ${:.4} (simulated)", sn, pulled.position.market_name, pulled.position.avg_entry);
                                        }
                                        continue;
                                    }
                                    // Never touch a CONFIRMED fill — only pull unfilled resting quotes.
                                    let is_unfilled = {
                                        let pos = positions.lock().await;
                                        matches!(pos.get(&pk), Some(p) if p.fill_confirmed_at.is_none())
                                    };
                                    if !is_unfilled { continue; }
                                    // Retire this quote's epoch before doing anything else. Its
                                    // fill-verification task is still waiting on the balance
                                    // endpoint, and without this it would adopt the NEXT quote's
                                    // fill as its own and write a second entry row.
                                    bump_quote_epoch(&mut *quote_epochs.lock().await, &pk);
                                    // Defensive only: ghost pulls return above, where the
                                    // simulated resting quote actually lives. Kept so this
                                    // never reaches a venue call if that guard is ever moved.
                                    let matched = if ghosting {
                                        dec!(0)
                                    } else {
                                        // The quote is a BID; pull that side only. A resting
                                        // exit ask on this token belongs to the resting-exit
                                        // path, and its matched size is an exit, not a fill
                                        // to adopt.
                                        let cancel = crate::helpers::balance::cancel_resting_orders_for_token(
                                            &trading_client, &tok, crate::helpers::balance::RestingSide::Bids).await;
                                        let (order_found, m) = (cancel.bid_found, cancel.bid_matched);
                                        // ── Complete-fill guard ──────────────────────────
                                        // A quote that FULLY filled vanishes from the CLOB
                                        // open-orders list, so the cancel sweep reports zero
                                        // matched (2026-07-24 trade 288: an 8-share YES fill
                                        // was dropped as "unfilled", re-adopted under the
                                        // WRONG strategy at market switch, and SL'd −$0.40).
                                        // When the order is GONE, cross-check the on-chain
                                        // balance — WITH settlement-lag retries, because the
                                        // balance endpoint lags a fresh fill by up to ~15s
                                        // (2026-07-29 trade 300: fill landed <10s before the
                                        // pull, a single immediate balance check read 0, the
                                        // position was dropped and rode unmanaged to $0
                                        // settlement, −$3.84).
                                        if m >= config::MIN_ORDER_SHARES {
                                            m
                                        } else if !order_found {
                                            let mut held = crate::helpers::balance::onchain_balance_for_token(&trading_client, &tok).await;
                                            let mut attempt = 1u32;
                                            while held < config::MIN_ORDER_SHARES && attempt < config::SETTLEMENT_LAG_RETRY_ATTEMPTS {
                                                tokio::time::sleep(std::time::Duration::from_secs(config::SETTLEMENT_LAG_RETRY_DELAY_SECS)).await;
                                                held = crate::helpers::balance::onchain_balance_for_token(&trading_client, &tok).await;
                                                attempt += 1;
                                                if held >= config::MIN_ORDER_SHARES {
                                                    warn!("⚠️ Maker quote-pull [{}]: {} — balance appeared on retry {} ({:.4} shares): settlement lag confirmed", sn, tok, attempt, held);
                                                }
                                            }
                                            held
                                        } else {
                                            // Order was found on the book and cancelled with
                                            // sub-threshold matched size — genuinely unfilled.
                                            m
                                        }
                                    };
                                    // ── Fill-adoption guard ───────────────────────────────
                                    // A resting quote can partially or fully fill in the
                                    // seconds before the pull (trades 280 and 288). If any
                                    // matched size or on-chain balance is found, adopt those
                                    // shares as a live confirmed position so TP/SL/ToxicFill
                                    // manage it immediately.
                                    if matched >= config::MIN_ORDER_SHARES {
                                        let adopted = {
                                            let mut pos = positions.lock().await;
                                            pos.get_mut(&pk).map(|p| {
                                                p.shares = matched;
                                                p.fill_confirmed_at = Some(Utc::now());
                                                (p.market_name.clone(), p.avg_entry)
                                            })
                                        };
                                        if let Some((mn, ep)) = adopted {
                                            pending_orders.lock().await.remove(&pk);
                                            let side = side_of(&tok).to_string();
                                            if let Some(pool) = db::pool_for(&asset_lc) {
                                                db::update_position_from_chain(&pool, tok.as_str(), matched, ep, None).await;
                                                db::confirm_position_status(&pool, &sn, tok.as_str()).await;
                                            }
                                            metrics::record_entry(&scope, sn.clone(), tok.to_string(), mn.clone(), side.clone(), ep, matched).await;
                                            let feat_snap = ctx.maker_snapshot.clone().unwrap_or_else(|| ctx.snapshot.clone());
                                            metrics::record_entry_signal(&asset_lc, sn.clone(), tok.to_string(), mn, side, ep, matched, &feat_snap).await;
                                            // This position was born from a pull that had ALREADY
                                            // confirmed the toxic/drift condition — the cancel just
                                            // lost the race to the fill. Exempt it from the ToxicFill
                                            // min_hold, whose confirmation it has pre-served: the
                                            // `fill_confirmed_at = now` stamped above restarts
                                            // `held_secs` at zero (the true fill time is unknowable
                                            // here — cancel-sweep matched size and the balance
                                            // endpoint carry no timestamp), and on 2026-09-01 that
                                            // fresh 30s wait turned a $0.4100 exit into $0.3900,
                                            // ~$0.36 of a −$0.8864 realized loss.
                                            crate::vipers::maker_impl::mark_maker_fill_adopted_under_pull(tok.as_str());
                                            warn!("⚡ Maker quote-pull [{}]: {} — quote FILLED ({:.4} shares @ ${:.4}) before cancel; adopting as live position", sn, tok, matched, ep);
                                            continue;
                                        }
                                    }
                                    positions.lock().await.remove(&pk);
                                    pending_orders.lock().await.remove(&pk);
                                    token_ownership.lock().await.remove(&tok);
                                    if let Some(pool) = db::pool_for(&asset_lc) {
                                        db::close_open_position(&pool, &sn, tok.as_str()).await;
                                    }
                                    info!("🚫 Maker quote-pulled [{}]: {} — resting quote cancelled (toxic book / oracle drift)", sn, tok);
                                }
                            }
                            // ════════════════════ MAKER RESTING EXIT ════════════════════
                            // Post-only GTC ask against a FILLED maker position so it
                            // leaves by being lifted at the ask rather than crossing back
                            // to the bid. Idempotent by contract — the strategy re-emits
                            // this every tick, so the common path here is a no-op.
                            StrategySignal::MakerRestingExit { params, reason } => {
                                // Each strategy that rests an exit owns its own knob.
                                if !resting_exit::enabled_for(&sn, resting_exit_enabled, fairvalue_resting_tp_enabled) { continue; }
                                let tok = params.token_id.clone();
                                let pk = PositionKey::new(sq_key.clone(), sn.clone(), tok.clone());
                                let label = resting_exit::label(&sn);

                                // The position must still be live and confirmed —
                                // a stop may have closed it earlier in this very tick.
                                let (live_shares, avg_entry, entry_fee) = {
                                    let map = positions.lock().await;
                                    match map.get(&pk) {
                                        Some(p) if p.fill_confirmed_at.is_some() => (p.shares, p.avg_entry, p.entry_fee),
                                        _ => continue,
                                    }
                                };
                                if live_shares < config::MIN_ORDER_SHARES { continue; }

                                if ghosting || params.ghost_mode {
                                    if !maker_resting_exits.contains_key(&pk) {
                                        info!("👻 GHOST_MODE {} [{}]: {} | shares={:.2}, ask=${:.4} (simulated)",
                                              label, sn, params.market_name, live_shares, params.price);
                                        maker_resting_exits.insert(pk.clone(), MakerRestingExit {
                                            price: params.price, shares: live_shares, avg_entry, entry_fee,
                                            market_name: params.market_name.clone(), last_poll: Instant::now(),
                                            short_reads: 0, max_short_read: dec!(0), booked: dec!(0),
                                        });
                                    }
                                    continue;
                                }

                                // Already resting near this price → leave it alone.
                                // Cancel/replace surrenders queue priority, so chasing
                                // every 1-tick flicker would defeat the point of resting.
                                if let Some(existing) = maker_resting_exits.get(&pk) {
                                    if (existing.price - params.price).abs() < resting_exit_reprice_threshold
                                        && (existing.shares - live_shares).abs() < config::MIN_ORDER_SHARES
                                    {
                                        continue;
                                    }
                                    // Repricing: pull the stale ask so the shares are free
                                    // to back the new one.
                                    let stale_price = existing.price;
                                    let already_booked = existing.booked;
                                    let ask_baseline = existing.shares;
                                    // Pull OUR ASK only. The Maker's entry bid can still be
                                    // resting on this same token (a GTC quote keeps working
                                    // after a partial fill), and it belongs to the quote
                                    // lifecycle, not this one: it holds none of the shares,
                                    // and its matched size is the ENTRY fill — already in the
                                    // position, not something that left it.
                                    let cancel = cancel_resting_orders_for_token(
                                        &trading_client, &tok, RestingSide::Asks).await;
                                    // The record MUST go regardless of what the cancel
                                    // found: after this point no ask of ours rests, and
                                    // leaving the entry behind would make the next tick's
                                    // deadband check believe one still does — silently
                                    // stranding the position with no exit order at all.
                                    maker_resting_exits.remove(&pk);

                                    // What the venue says left THIS ask beyond what the
                                    // sweep has already booked. `ask_matched` is cumulative
                                    // since placement, so the sweep's partials come off
                                    // first — 2026-09-03 trade 5 re-booked a 5.31-share
                                    // partial the sweep had written 21s earlier, on top of
                                    // the entry bid's own 7.35 fill, and closed a 7.35-share
                                    // position as 17.98 shares sold.
                                    let venue_lift = resting_exit::lift_since_booking(
                                        cancel.ask_found, cancel.ask_matched, already_booked, live_shares);
                                    // Cross-check the chain, once and without sleeping in
                                    // the tick: a fully lifted ask has already left the
                                    // open-orders list, and the only reading trusted here is
                                    // a POSITIVE one below the baseline (the endpoint lags
                                    // high, never low, and reads 0 on any failure).
                                    let held = onchain_balance_for_token(&trading_client, &tok).await;
                                    let chain_lift = resting_exit::chain_drop(ask_baseline, held);
                                    let matched = venue_lift.max(chain_lift);

                                    if matched >= config::MIN_ORDER_SHARES {
                                        // Lifted while we were deciding. Book that slice
                                        // here at the stale limit (a resting limit cannot
                                        // slip) — the fill-detection sweep can no longer
                                        // see it now that the record is gone. Net of the
                                        // entry fee the slice carries.
                                        let bk = resting_exit::book_lift(stale_price, avg_entry, entry_fee, matched, live_shares);
                                        let pnl = bk.pnl;
                                        let side_label = side_of(&tok).to_string();
                                        info!("✅ {} filled [{}]: {:.4} shares lifted @ ${:.4} (entry ${:.4}) pnl=${:.4}{} — caught while repricing",
                                              label, sn, matched, stale_price, avg_entry, pnl, bk.fee_note());
                                        *total_pnl.lock().await += pnl;
                                        metrics::record_trade(
                                            &scope, bk.entry_fee_booked, sn.clone(), params.market_name.clone(), side_label,
                                            avg_entry, stale_price, matched, pnl,
                                            resting_exit::ledger_reason(&sn, stale_price, avg_entry),
                                        ).await;
                                        last_trade_time.insert(sn.clone(), Instant::now());

                                        let remaining = resting_exit::remaining_after(live_shares, matched, held);
                                        if remaining < config::MIN_ORDER_SHARES {
                                            positions.lock().await.remove(&pk);
                                            token_ownership.lock().await.remove(&tok);
                                            if let Some(pool) = db::pool_for(&asset_lc) {
                                                db::close_open_position(&pool, &sn, tok.as_str()).await;
                                            }
                                            continue;
                                        }
                                        // Re-post for the remainder on the next tick, once
                                        // the strategy has priced it against a fresh book.
                                        // The remainder keeps the entry fee not yet booked.
                                        if let Some(p) = positions.lock().await.get_mut(&pk) {
                                            p.shares = remaining;
                                            p.entry_fee = bk.entry_fee_left;
                                        }
                                        continue;
                                    } else if !cancel.ask_found {
                                        // Nothing verifiable sold: the ask is gone from the
                                        // book and the chain shows no drop. Either it was
                                        // pulled from outside or a full lift has not reached
                                        // the balance endpoint yet. Book nothing — the
                                        // re-post below fails on "not enough balance" if
                                        // the shares are gone, and the chain sweep then
                                        // closes the row from the wallet, not from a guess.
                                        info!("ℹ️ {} [{}]: ask @ ${:.4} was no longer on the book and the chain shows no lift — re-posting against a fresh book",
                                              label, sn, stale_price);
                                    }
                                }

                                let vc = if params.is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL };
                                match place_limit_order(
                                    &trading_client, &nonce_manager, &signer,
                                    safe_address, eoa_address, vc, &tok, Side::Sell,
                                    live_shares, params.price, params.fee_bps,
                                    params.order_type, params.post_only, 0, &shared_http,
                                ).await {
                                    Ok(_order_id) => {
                                        maker_resting_exits.insert(pk.clone(), MakerRestingExit {
                                            price: params.price, shares: live_shares, avg_entry, entry_fee,
                                            market_name: params.market_name.clone(), last_poll: Instant::now(),
                                            short_reads: 0, max_short_read: dec!(0), booked: dec!(0),
                                        });
                                        info!("📤 {} [{}]: {} | ASK {:.4} shares @ ${:.4} (entry ${:.4}) | {}",
                                              label, sn, params.market_name, live_shares, params.price, avg_entry, reason);
                                    }
                                    Err(e) => {
                                        let es = e.to_string();
                                        if es.contains("crosses book") {
                                            // The book moved under us between pricing and
                                            // placement. Harmless: the strategy re-emits
                                            // next tick against a fresh snapshot.
                                            debug!("⏸️ {} [{}]: ask ${:.4} crossed the book — retrying next tick", label, sn, params.price);
                                        } else if es.contains("not enough balance") || es.contains("balance: 0") {
                                            // Shares are still settling, or already gone.
                                            // Either way do NOT count it as a venue failure;
                                            // the next tick re-evaluates against real state.
                                            debug!("⏸️ {} [{}]: shares unavailable ({}) — retrying next tick",
                                                   label, sn, es.chars().take(60).collect::<String>());
                                        } else {
                                            warn!("⚠️ {} placement failed [{}] (\"{}\") — position held, retrying next tick",
                                                  label, sn, es.chars().take(120).collect::<String>());
                                        }
                                    }
                                }
                            }
                            StrategySignal::NoSignal => {}
                        }
                    }
                    Ok::<(), ()>(())
                    }).await;
                    if signal_processing_result.is_err() {
                        warn!("⚠️ Signal processing timed out (45s) — select! loop unblocked, watchdog/heartbeat resume");
                    }

                    // ── Resting maker exit: fill detection ───────────────────────
                    // A resting ask is lifted silently — there is no order response
                    // to read, so the fill shows up as a drop in the on-chain share
                    // count. The exit price is NOT estimated here: a resting limit
                    // order cannot slip, so `rec.price` IS the realized price. That
                    // is exactly what the FAK exits cannot promise, and why the
                    // ChainReconcile fallback (which books at "last mark") must
                    // never be the thing that closes one of these.
                    if !maker_resting_exits.is_empty() && !ghosting {
                        let due: Vec<PositionKey> = maker_resting_exits
                            .iter()
                            .filter(|(_, v)| v.last_poll.elapsed() >= Duration::from_secs(MAKER_RESTING_EXIT_POLL_SECS))
                            .map(|(k, _)| k.clone())
                            .collect();

                        for pk in due {
                            // Closed by a stop/FAK earlier — the ledger is already
                            // written, so just drop the stale order record.
                            if !positions.lock().await.contains_key(&pk) {
                                maker_resting_exits.remove(&pk);
                                continue;
                            }

                            // ── Confirm the drop before believing it ──────────────
                            // A single short read is NOT evidence of a lift: the
                            // helper returns 0 on ANY failure and the endpoint lags
                            // real fills. Trusting one would fabricate an exit, credit
                            // invented P&L, drop the position and leave real shares
                            // floating unmanaged to settlement — the −$3.84 failure
                            // mode already recorded twice in this file's history.
                            //
                            // So require CONSECUTIVE short polls to agree, and size the
                            // exit from the LARGEST balance seen across that run. This
                            // is deliberately not a retry-with-sleep: the sweep runs on
                            // every 50ms tick and must never block the loop that
                            // evaluates stops.
                            let held_now = onchain_balance_for_token(&trading_client, &pk.market).await;
                            let rec = match maker_resting_exits.get_mut(&pk) {
                                Some(r) => {
                                    r.last_poll = Instant::now();
                                    if r.shares - held_now >= config::MIN_ORDER_SHARES {
                                        r.short_reads += 1;
                                        if r.short_reads == 1 || held_now > r.max_short_read {
                                            r.max_short_read = held_now;
                                        }
                                    } else {
                                        r.short_reads = 0;
                                        r.max_short_read = dec!(0);
                                    }
                                    r.clone()
                                }
                                None => continue,
                            };

                            if rec.short_reads < MAKER_RESTING_EXIT_FILL_CONFIRMATIONS {
                                continue; // still resting, or awaiting confirmation
                            }

                            let held = rec.max_short_read;
                            let filled = rec.shares - held;
                            if filled < config::MIN_ORDER_SHARES {
                                continue;
                            }

                            // The live position is the authority on what this slice
                            // cost to open; the record only stands in if it is gone.
                            let (avg_entry, entry_fee) = positions.lock().await.get(&pk)
                                .map(|p| (p.avg_entry, p.entry_fee))
                                .unwrap_or((rec.avg_entry, rec.entry_fee));
                            let bk = resting_exit::book_lift(rec.price, avg_entry, entry_fee, filled, rec.shares);
                            let pnl = bk.pnl;
                            let side_label = if maker_market_config.as_ref().is_some_and(|m| m.yes_token == pk.market)
                                || pk.market == hourly_yes_token { "YES" } else { "NO" }.to_string();
                            let fully_closed = held < config::MIN_ORDER_SHARES;

                            info!(
                                "✅ {} FILLED [{}]: {} | {:.4} shares lifted @ ${:.4} (entry ${:.4}) pnl=${:.4}{}{}",
                                resting_exit::label(&pk.strategy), pk.strategy, rec.market_name, filled, rec.price, avg_entry, pnl,
                                bk.fee_note(),
                                if fully_closed { "" } else { " (partial — remainder still resting)" },
                            );

                            *total_pnl.lock().await += pnl;
                            metrics::record_trade(
                                &scope, bk.entry_fee_booked, pk.strategy.clone(), rec.market_name.clone(), side_label,
                                avg_entry, rec.price, filled, pnl,
                                resting_exit::ledger_reason(&pk.strategy, rec.price, avg_entry),
                            ).await;

                            if fully_closed {
                                positions.lock().await.remove(&pk);
                                token_ownership.lock().await.remove(&pk.market);
                                maker_resting_exits.remove(&pk);
                                if let Some(pool) = db::pool_for(&asset_lc) {
                                    db::close_open_position(&pool, &pk.strategy, pk.market.as_str()).await;
                                }
                                last_trade_time.insert(pk.strategy.clone(), Instant::now());
                            } else {
                                // Partial lift: the rest of the order is still on the
                                // book. Re-sync both the position and the record to the
                                // real on-chain size so the next poll measures against
                                // the right baseline.
                                if let Some(p) = positions.lock().await.get_mut(&pk) {
                                    p.shares = held;
                                    p.entry_fee = bk.entry_fee_left;
                                }
                                if let Some(r) = maker_resting_exits.get_mut(&pk) {
                                    r.shares = held; r.booked += filled; r.entry_fee = bk.entry_fee_left;
                                }
                            }
                        }
                    }

                    // Tick complete — back to waiting in the select!.
                    crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::Idle);
                }
            }
        }

        // ── Tear-down: pull any resting maker asks ───────────────────────────
        // These are the only orders this loop leaves on the book across a market
        // rotation or stand-down. The in-memory record dies with `patrol()`, so an
        // ask left resting would fill unmanaged and reach the ledger only via the
        // ChainReconcile fallback — booked at "last mark" instead of its real
        // resting price, which is precisely the ledger drift that path exists to
        // paper over. Cancel them while we still know they are ours.
        // Outside the tick, so there is no `ghosting` in scope — read the live
        // config directly rather than the build constant alone.
        if !maker_resting_exits.is_empty() && !crate::helpers::dynamic_config::ghosting_now() {
            for (pk, rec) in maker_resting_exits.iter() {
                let cancel = cancel_resting_orders_for_token(
                    &trading_client, &pk.market, RestingSide::Both).await;
                // Only the ask's fill is an exit, and only the part the sweep
                // has not already booked.
                let matched = resting_exit::lift_since_booking(
                    cancel.ask_found, cancel.ask_matched, rec.booked, rec.shares);
                if matched >= config::MIN_ORDER_SHARES {
                    // Lifted during tear-down: book it here at the resting price
                    // rather than leaving a silent cash move for the reconciler.
                    // Net of the entry fee, as everywhere else a lift is booked.
                    let (avg_entry, entry_fee) = positions.lock().await.get(pk)
                        .map(|p| (p.avg_entry, p.entry_fee))
                        .unwrap_or((rec.avg_entry, rec.entry_fee));
                    let bk = resting_exit::book_lift(rec.price, avg_entry, entry_fee, matched, rec.shares);
                    let pnl = bk.pnl;
                    *total_pnl.lock().await += pnl;
                    let side_label = if maker_market_config.as_ref().is_some_and(|m| m.yes_token == pk.market)
                        || pk.market == hourly_yes_token { "YES" } else { "NO" }.to_string();
                    info!("✅ {} filled during stand-down [{}]: {:.4} shares @ ${:.4} pnl=${:.4}{}",
                          resting_exit::label(&pk.strategy), pk.strategy, matched, rec.price, pnl, bk.fee_note());
                    metrics::record_trade(
                        &scope, bk.entry_fee_booked, pk.strategy.clone(), rec.market_name.clone(), side_label,
                        avg_entry, rec.price, matched, pnl,
                        format!("{}: lifted @ ${:.4} during stand-down", resting_exit::label(&pk.strategy), rec.price),
                    ).await;
                    positions.lock().await.remove(pk);
                    token_ownership.lock().await.remove(&pk.market);
                    if let Some(pool) = db::pool_for(&asset_lc) {
                        db::close_open_position(&pool, &pk.strategy, pk.market.as_str()).await;
                    }
                } else {
                    info!("🚫 {} cancelled on stand-down [{}]: ask ${:.4} pulled from the book", resting_exit::label(&pk.strategy), pk.strategy, rec.price);
                }
            }
            maker_resting_exits.clear();
        }

        // ── Tear-down: stop all peripheral tasks ─────────────────────────────
        // Clear the venue's active-token registry so the lifecycle task does not
        // query stale tokens during the brief window between peripheral_cancel
        // firing and the lifecycle task exiting.
        ctx.session.venue.clear_active_tokens().await;
        peripheral_cancel.cancel();
    }
}

/// Cancel every resting order, retrying with exponential backoff.
///
/// Shared by the two paths that end a squadron's tenure on a market: the market
/// rotation arm and event-market retirement. Returns false when every attempt
/// failed, which the caller reports but does not treat as fatal — the venue
/// cancels what is left when the market resolves.
/// Account-wide cancel of every resting order — unless the engine is simulating.
///
/// The gate every account-wide cancel path on this venue must pass through.
/// Five paths issue `DELETE /cancel-all` against the live wallet: the startup
/// sweep, the SIGTERM shutdown handler, and the patrol's stand-down, market
/// rotation and event-retirement arms. None of them checked ghost mode, so on
/// 2026-09-01 at 21:52:12 a production restart in ghost mode fired a real
/// account-wide cancel five times (answered 503 "cancels are disabled" — the
/// only reason nothing was touched) — the exact behavior the v1.0.9 release
/// notes promise no longer happens while simulating. In ghost mode the engine
/// has placed nothing, so the only orders this could ever cancel are a
/// previous live session's leftovers or the operator's own hand-placed orders.
///
/// While simulating, the resting orders are REPORTED instead (split by the
/// placing API key, so the operator can see which are the engine's own), and
/// `true` is returned: a deliberate skip is not a cancel failure, and the
/// callers' error arms describe failures.
pub async fn cancel_all_orders_unless_simulating(
    trading_client: &polymarket_client_sdk_v2::clob::Client<
        polymarket_client_sdk_v2::auth::state::Authenticated<
            polymarket_client_sdk_v2::auth::Normal,
        >,
    >,
) -> bool {
    if crate::helpers::dynamic_config::ghosting_now() {
        crate::helpers::balance::report_resting_orders_while_simulating(trading_client).await;
        return true;
    }
    cancel_all_orders_with_retries(trading_client).await
}

pub async fn cancel_all_orders_with_retries(
    trading_client: &polymarket_client_sdk_v2::clob::Client<
        polymarket_client_sdk_v2::auth::state::Authenticated<
            polymarket_client_sdk_v2::auth::Normal,
        >,
    >,
) -> bool {
    for i in 0..MAX_CANCEL_RETRIES {
        let delay = BASE_CANCEL_RETRY_DELAY_MS * (1 << i);
        match tokio::time::timeout(
            Duration::from_secs(8),
            trading_client.cancel_all_orders(),
        ).await {
            Ok(Ok(_)) => {
                info!("✅ Successfully cancelled all orders after {} retries.", i);
                return true;
            }
            Ok(Err(e)) => {
                warn!("⚠️ Failed to cancel all orders (attempt {}/{}) with error: {}", i + 1, MAX_CANCEL_RETRIES, e);
                if i < MAX_CANCEL_RETRIES - 1 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
            Err(_) => {
                warn!("⚠️ cancel_all_orders timed out (8s) (attempt {}/{}) — retrying in {}ms", i + 1, MAX_CANCEL_RETRIES, delay);
                if i < MAX_CANCEL_RETRIES - 1 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }
    false
}

/// Is a single-market squadron due to stand itself down?
///
/// Extracted from the patrol tick so the rule is testable: the tick that calls
/// it is a 2,000-line async loop, and the two facts this encodes — retire only
/// after the grace, and never on top of a position — are exactly the ones a
/// future edit could quietly invert.
///
/// `holding_position` is deliberately a caller-computed bool rather than the
/// position map: the caller already holds the lock and knows the squadron id.
pub(crate) fn event_market_retire_due(
    single_market: bool,
    close_time: Option<chrono::DateTime<Utc>>,
    now: chrono::DateTime<Utc>,
    grace_secs: i64,
    holding_position: bool,
) -> bool {
    if !single_market || holding_position {
        return false;
    }
    match close_time {
        // A market with no close time never retires on its own. Nothing to
        // measure against, and guessing would stand down a live squadron.
        None => false,
        Some(close) => (now - close).num_seconds() >= grace_secs,
    }
}

/// May this squadron's own market serve as its maker (window/daily) venue?
///
/// True only when the squadron was deployed onto one market to begin with. The
/// negative case is the safety-critical one: a crypto squadron whose daily
/// market is transiently unavailable must keep Arbitrage idle rather than let
/// it fall back to the hourly leg. See `Squadron::single_market`.
pub(crate) fn single_market_arb_enabled(single_market: bool, has_maker_venue: bool) -> bool {
    single_market && !has_maker_venue
}

/// Does this squadron hold nothing to evaluate against?
///
/// True only when the hourly slot carries the ZERO sentinel AND no maker
/// (window/daily) venue was resolved. Either one alone is a market the vipers
/// can run on; a held market whose book is merely empty is a different state
/// (a dark feed) and must not be reported as idle.
pub(crate) fn squadron_has_no_market(hourly_yes_token: &MarketId, has_maker_venue: bool) -> bool {
    *hourly_yes_token == market_id_from_u256(U256::ZERO) && !has_maker_venue
}

/// The hourly strike as the market channel holds it RIGHT NOW, for the market
/// this squadron deployed on.
///
/// An "Up or Down" market's strike is the opening print of its window, and the
/// squadron rotates onto the next hour's market before that window opens — so
/// the strike is legitimately unknown at deploy and arrives later, when the
/// market monitor re-broadcasts the same market with it filled in. That
/// re-broadcast carries the same condition id and is deliberately not a
/// rotation (rotation cancels every order and redeploys), so the value
/// captured at deploy would never see it. Read the channel each tick instead.
///
/// A different condition id on the channel is a rotation in flight; the
/// deployed value stands until the rotation arm handles it. One short borrow,
/// never held across an await — the same discipline as every other watch
/// channel read in the patrol tick.
fn live_hourly_strike(
    market_rx: &tokio::sync::watch::Receiver<crate::tasks::market_monitor::MarketState>,
    deployed_condition_id: &str,
    deployed_strike: Option<Decimal>,
) -> Option<Decimal> {
    let ms = market_rx.borrow();
    if !deployed_condition_id.is_empty() && ms.7 == deployed_condition_id {
        ms.4
    } else {
        deployed_strike
    }
}

#[cfg(test)]
mod event_market_retire_tests {
    use super::*;

    fn at(secs_from_close: i64) -> (chrono::DateTime<Utc>, chrono::DateTime<Utc>) {
        let close = Utc::now();
        (close, close + chrono::Duration::seconds(secs_from_close))
    }

    #[test]
    fn holds_until_the_grace_has_elapsed() {
        let (close, now) = at(299);
        assert!(!event_market_retire_due(true, Some(close), now, 300, false));
    }

    #[test]
    fn retires_once_the_grace_has_elapsed() {
        let (close, now) = at(300);
        assert!(event_market_retire_due(true, Some(close), now, 300, false));
    }

    /// The intl path has no `flatten_before_stand_down`, so retiring on top of a
    /// position strands it with no viper left to evaluate its exit.
    #[test]
    fn never_retires_while_holding_a_position() {
        let (close, now) = at(86_400);
        assert!(!event_market_retire_due(true, Some(close), now, 300, true));
    }

    /// A crypto squadron is rotated by the venue's own loop. If this fired on
    /// one it would stand down the wing with nothing to respawn it.
    #[test]
    fn never_retires_a_split_venue_squadron() {
        let (close, now) = at(86_400);
        assert!(!event_market_retire_due(false, Some(close), now, 300, false));
    }

    #[test]
    fn a_market_with_no_close_time_never_retires() {
        assert!(!event_market_retire_due(true, None, Utc::now(), 300, false));
    }

    /// A zero grace is a legal operator setting: retire at the close itself.
    #[test]
    fn zero_grace_retires_at_the_close() {
        let (close, now) = at(0);
        assert!(event_market_retire_due(true, Some(close), now, 0, false));
    }

    #[test]
    fn single_market_squadron_uses_its_own_market_as_the_maker_venue() {
        assert!(single_market_arb_enabled(true, false));
    }

    /// The load-bearing case: a split venue that has lost its daily market keeps
    /// Arbitrage idle rather than falling back to the hourly leg.
    #[test]
    fn split_venue_without_a_maker_market_does_not_fall_back() {
        assert!(!single_market_arb_enabled(false, false));
    }

    /// A single-market squadron that somehow does have a maker venue uses it.
    #[test]
    fn a_real_maker_venue_always_wins() {
        assert!(!single_market_arb_enabled(true, true));
        assert!(!single_market_arb_enabled(false, true));
    }
}

#[cfg(test)]
mod no_market_tests {
    use super::*;

    fn zero() -> MarketId { market_id_from_u256(U256::ZERO) }
    fn real() -> MarketId { market_id_from_u256(U256::from(42u64)) }

    /// The 2026-09-04 shape: hourly released, no daily venue. Idle, not stale.
    #[test]
    fn released_hourly_and_no_maker_venue_is_idle() {
        assert!(squadron_has_no_market(&zero(), false));
    }

    /// A maker venue alone is a market to evaluate; the vipers run on it and
    /// their own rows stay fresh, so this must not be labeled idle.
    #[test]
    fn a_maker_venue_alone_is_not_idle() {
        assert!(!squadron_has_no_market(&zero(), true));
    }

    /// A held hourly market with no book is a dark feed — a real anomaly with
    /// its own banner — and must never be dressed up as a deliberate wait.
    #[test]
    fn a_held_market_is_never_idle() {
        assert!(!squadron_has_no_market(&real(), false));
        assert!(!squadron_has_no_market(&real(), true));
    }
}

#[cfg(test)]
mod trade_accounting_tests {
    use super::*;

    /// Production trade 378 (2026-08-20, FairValue): the case that forced this
    /// redesign.
    ///
    /// 4.054053 shares entered at $0.74, take-profit at a $0.89 bid, sell
    /// rejected. Collateral moved 67.00502 -> 67.718402 — a real +$0.71 that the
    /// first implementation booked as $0.00, because it divided a pre-buy
    /// baseline by the share count and got a nonsense price of $0.176.
    #[test]
    fn books_the_real_gain_of_production_trade_378() {
        let (px, pnl) = reconcile_unverified_exit(
            Some(dec!(67.00502)), dec!(67.718402), dec!(4.054053),
            dec!(0.74), dec!(0.89), dec!(0.10),
        ).expect("a settled pre-entry baseline should reconcile");
        assert_eq!(pnl, dec!(0.713382));
        // Derived, not measured: entry + pnl/shares. The book showed an ask of
        // $0.91 twenty seconds later, so ~$0.916 is the right neighborhood.
        assert!(px > dec!(0.91) && px < dec!(0.92), "derived exit was {px}");
    }

    /// Production trade 377 (2026-08-20, Maker) must still reconcile: 18 shares
    /// at $0.44 against a $0.43 bid, collateral 58.90502 -> 67.00502 where
    /// 58.90502 is the PRE-ENTRY reading.
    #[test]
    fn still_books_the_maker_case_that_started_this() {
        let (px, pnl) = reconcile_unverified_exit(
            Some(dec!(66.82502)), dec!(67.00502), dec!(18),
            dec!(0.44), dec!(0.43), dec!(0.10),
        ).expect("clean baseline should reconcile");
        assert_eq!(pnl, dec!(0.18), "the gain that actually occurred");
        assert_eq!(px, dec!(0.45));
    }

    /// The band measures against the BID, not the entry. FairValue routinely
    /// takes 20%+ profits; measuring from entry made a correct reconciliation
    /// look like a wild outlier and threw it away.
    #[test]
    fn a_large_but_genuine_profit_is_not_mistaken_for_contamination() {
        // +$0.71 on a $0.74 entry is a 24% move — far outside a 0.10 band drawn
        // around the entry, but right on top of the observed bid.
        assert!(reconcile_unverified_exit(
            Some(dec!(67.00502)), dec!(67.718402), dec!(4.054053),
            dec!(0.74), dec!(0.89), dec!(0.10),
        ).is_some());
    }

    /// A baseline taken AFTER the buy makes `pnl` the gross proceeds rather than
    /// the net, which inflates the derived price past a binary's ceiling. That
    /// is how a mis-timed baseline is caught rather than silently booked.
    #[test]
    fn rejects_a_baseline_taken_on_the_wrong_side_of_the_entry() {
        // Post-buy baseline: 67.00502 - 3.00 = 64.00502.
        assert_eq!(
            reconcile_unverified_exit(
                Some(dec!(64.00502)), dec!(67.718402), dec!(4.054053),
                dec!(0.74), dec!(0.89), dec!(0.10),
            ),
            None,
        );
    }

    /// No baseline — the process restarted since the entry — is not an excuse to
    /// guess. The caller books zero.
    #[test]
    fn refuses_to_price_without_a_baseline() {
        assert_eq!(
            reconcile_unverified_exit(None, dec!(67.0), dec!(18), dec!(0.44), dec!(0.43), dec!(0.10)),
            None,
        );
    }

    /// Another position closing in between moves collateral by dollars, which
    /// throws the derived price nowhere near the bid.
    #[test]
    fn rejects_a_baseline_contaminated_by_another_position() {
        assert_eq!(
            reconcile_unverified_exit(
                Some(dec!(66.82502)), dec!(92.0), dec!(18), dec!(0.44), dec!(0.43), dec!(0.10)),
            None,
        );
    }

    /// A loss reconciles exactly like a gain — the sign is never assumed.
    #[test]
    fn a_genuine_loss_is_booked_as_a_loss() {
        let (px, pnl) = reconcile_unverified_exit(
            Some(dec!(66.82502)), dec!(66.64502), dec!(18),
            dec!(0.44), dec!(0.43), dec!(0.10),
        ).expect("a loss is still a reconcilable outcome");
        assert_eq!(pnl, dec!(-0.18));
        assert_eq!(px, dec!(0.43));
    }

    /// Zero shares must not panic on the division.
    #[test]
    fn rejects_a_zero_share_position() {
        assert_eq!(
            reconcile_unverified_exit(Some(dec!(58.0)), dec!(67.0), dec!(0), dec!(0.44), dec!(0.43), dec!(0.10)),
            None,
        );
    }

    // ── Maker quote epochs ─────────────────────────────────────────────────

    fn key() -> PositionKey {
        PositionKey::new(
            "btc-hourly",
            "MakerStrategy",
            MarketId::new("98160110645972743141257069093476801874133411306802274299633963514874310415390"),
        )
    }

    /// The production sequence behind duplicate entry rows 494 and 495
    /// (2026-08-20): a quote was placed, pulled 47s later, and replaced. The
    /// pulled quote's fill watcher was never stopped, so when the REPLACEMENT
    /// filled, both watchers saw the shares and both wrote an entry — one of
    /// them for a quote that never filled at all.
    #[test]
    fn a_pulled_quote_cannot_claim_the_replacement_quotes_fill() {
        let mut epochs = QuoteEpochs::new();
        let k = key();

        // 22:43:33 — quote YES 17.77 @ $0.45; its watcher captures this epoch.
        let watcher_a = bump_quote_epoch(&mut epochs, &k);
        // 22:44:13 — MakerCancel pulls it.
        bump_quote_epoch(&mut epochs, &k);
        // 22:44:13 — quote YES 18.18 @ $0.44 replaces it.
        let watcher_b = bump_quote_epoch(&mut epochs, &k);

        // 22:45:00 — the $0.44 quote fills. Both watchers wake and see 18 shares.
        assert!(!quote_epoch_is_current(&epochs, &k, watcher_a),
                "the pulled $0.45 quote must NOT record an entry");
        assert!(quote_epoch_is_current(&epochs, &k, watcher_b),
                "the $0.44 quote that actually filled must record exactly one");
    }

    /// The ordinary path must still record: a quote placed, left alone, filled.
    #[test]
    fn an_unpulled_quote_records_its_own_fill() {
        let mut epochs = QuoteEpochs::new();
        let k = key();
        let watcher = bump_quote_epoch(&mut epochs, &k);
        assert!(quote_epoch_is_current(&epochs, &k, watcher));
    }

    /// Epochs are per (strategy, token): the maker quotes YES and NO at once,
    /// and pulling one side must not silence the other side's watcher.
    #[test]
    fn epochs_do_not_leak_across_tokens() {
        let mut epochs = QuoteEpochs::new();
        let yes = key();
        let no = PositionKey::new("btc-hourly", "MakerStrategy", MarketId::new("85421064502935751366375640444050801090202872554536397578825264135513707159160"));

        let watch_yes = bump_quote_epoch(&mut epochs, &yes);
        let watch_no  = bump_quote_epoch(&mut epochs, &no);
        bump_quote_epoch(&mut epochs, &yes); // pull YES only

        assert!(!quote_epoch_is_current(&epochs, &yes, watch_yes));
        assert!(quote_epoch_is_current(&epochs, &no, watch_no), "NO side must be untouched");
    }

    /// An unknown key is not "current" — a watcher whose epoch was pruned must
    /// stand down rather than record against a map that no longer knows it.
    #[test]
    fn a_pruned_epoch_is_not_current() {
        let epochs = QuoteEpochs::new();
        assert!(!quote_epoch_is_current(&epochs, &key(), 1));
    }

    // ── Ghost mode ─────────────────────────────────────────────────────────

    /// No order path may consult the BUILD constant on its own.
    ///
    /// `config::GHOST_MODE` is compile-time; the operator's switch lives in
    /// DynamicConfig. Until 2026-08-20 eleven sites in this file read only the
    /// constant, so toggling Ghost Mode in the Control Tower did nothing on the
    /// intl venue — entries and exits went to the exchange for real while the UI
    /// reported simulation. Both are now folded once per tick into `ghosting`.
    ///
    /// This asserts the idiom rather than the behavior, because the failure mode
    /// is someone adding a NEW order path with the old habit. Behavior tests
    /// cannot catch a site that does not exist yet.
    #[test]
    fn no_order_path_reads_the_build_ghost_switch_directly() {
        let src = include_str!("patrol_impl.rs");
        // Assembled at compile time so this line does not match itself.
        let needle = concat!("config::", "GHOST_MODE");
        let offenders: Vec<(usize, &str)> = src
            .lines()
            .enumerate()
            .filter(|(_, l)| l.contains(needle))
            .filter(|(_, l)| {
                let t = l.trim_start();
                // The single hoist is the one legitimate read; comments describe it.
                !t.starts_with("//") && !t.starts_with("let ghosting =")
            })
            .map(|(i, l)| (i + 1, l.trim()))
            .collect();
        assert!(
            offenders.is_empty(),
            "these read the build-level ghost switch instead of the per-tick `ghosting`: {offenders:#?}",
        );
    }

    /// `ghosting` must be true if EITHER switch is set — a build compiled for
    /// simulation cannot be un-ghosted by the runtime knob, and a live build must
    /// still obey the operator.
    #[test]
    fn either_switch_is_enough_to_ghost() {
        let combine = |build: bool, runtime: bool| build || runtime;
        assert!(!combine(false, false), "live build, switch off — trades for real");
        assert!(combine(false, true),  "operator turned it on — must simulate");
        assert!(combine(true, false),  "ghost build — must simulate regardless");
        assert!(combine(true, true));
    }
}

#[cfg(test)]
mod context_borrow_tests {
    /// No `borrow()` may appear inside the `StrategyContext` literal.
    ///
    /// Rust keeps a temporary alive until the end of the enclosing STATEMENT, so
    /// a `rx.borrow()` written as a field value holds its read guard until the
    /// whole `let ctx = StrategyContext { .. };` completes. The literal once
    /// borrowed the same channels several times over — oracle twice, velocity
    /// three times, drift three times, horizon four — which is a thread taking a
    /// second read lock while still holding the first.
    ///
    /// `tokio::sync::watch` is built on a writer-fair parking_lot RwLock, so a
    /// writer that queues between two of those borrows blocks the second one,
    /// while itself waiting on the first guard that can only drop once the
    /// statement finishes. That deadlocked the intl venue twice on 2026-08-25
    /// (price raptor writing the oracle channel; patrol and the tide raptor
    /// queued behind it) and the OS watchdog killed the process both times.
    ///
    /// Nothing at the call site hints at this — the code reads like a plain
    /// field initializer — so the invariant is asserted against the source.
    #[test]
    fn the_strategy_context_literal_holds_no_borrow_guards() {
        let src = include_str!("patrol_impl.rs");
        let start = src.find("let ctx = StrategyContext {")
            .expect("the StrategyContext literal must exist");

        // Walk to the matching close brace so the scan covers exactly the statement.
        let bytes = src[start..].as_bytes();
        let (mut depth, mut end) = (0i32, None);
        for (i, b) in bytes.iter().enumerate() {
            match b {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 { end = Some(i); break; }
                }
                _ => {}
            }
        }
        let literal = &src[start..start + end.expect("literal must be balanced")];

        let offenders: Vec<&str> = literal
            .lines()
            .filter(|l| l.contains(".borrow()"))
            .map(str::trim)
            .collect();

        assert!(
            offenders.is_empty(),
            "read guards inside the StrategyContext literal stay alive for the whole \
             statement and deadlock against a queued writer — hoist these into locals \
             above it: {offenders:#?}",
        );
    }
}


/// Pure arithmetic for booking a resting maker ask that gets pulled — by a
/// reprice, by a stop, or at stand-down. Kept free of I/O so the shapes from
/// the 2026-09-03 incident are pinned by tests.
pub(crate) mod resting_exit {
    use rust_decimal::Decimal;
    use crate::config;

    /// Shares the venue says left THIS ask that the ledger has not seen yet.
    ///
    /// `ask_matched` is the open order's `size_matched`: cumulative since the
    /// ask was placed, not since anyone last looked. The balance sweep books
    /// partial lifts as they land and counts them in `booked`, so only the
    /// excess is new. Clamped to `live` because no more than the position can
    /// have been sold. An ask that is not on the book (`found` false) has no
    /// venue figure at all — a fully lifted order vanishes from the list, and
    /// so does an externally cancelled one — so this reports zero rather than
    /// guess; the caller's chain read decides.
    pub fn lift_since_booking(found: bool, ask_matched: Decimal, booked: Decimal, live: Decimal) -> Decimal {
        if !found { return Decimal::ZERO; }
        (ask_matched - booked).max(Decimal::ZERO).min(live.max(Decimal::ZERO))
    }

    /// Shares the chain says left the wallet since the ask was sized to
    /// `baseline`, from ONE balance reading.
    ///
    /// A single read is trusted only in the direction it cannot lie: the
    /// balance endpoint lags a fill and so reads HIGH, never low, and it reads
    /// 0 on any failure. A POSITIVE reading below the baseline is therefore
    /// evidence of a lift; a zero is ambiguous (full lift or a failed lookup)
    /// and reports nothing.
    pub fn chain_drop(baseline: Decimal, held: Decimal) -> Decimal {
        if held < config::MIN_ORDER_SHARES { return Decimal::ZERO; }
        (baseline - held).max(Decimal::ZERO)
    }

    /// What the position holds after booking `lifted`: the ledger's figure,
    /// lowered — never raised — to a positive chain reading.
    pub fn remaining_after(live: Decimal, lifted: Decimal, held: Decimal) -> Decimal {
        let ledger = (live - lifted).max(Decimal::ZERO);
        if held >= config::MIN_ORDER_SHARES && held < ledger { held } else { ledger }
    }

    /// Which knob gates a strategy's resting exit.
    ///
    /// The Maker and FairValue each own theirs: turning the Maker's
    /// spread-capture exit off must not silence FairValue's resting take-profit,
    /// and the reverse. Any other strategy that starts emitting the signal
    /// falls under the Maker's knob until it is given its own.
    pub fn enabled_for(strategy: &str, maker_enabled: bool, fairvalue_enabled: bool) -> bool {
        match strategy {
            "FairValueStrategy" => fairvalue_enabled,
            _ => maker_enabled,
        }
    }

    /// The name a strategy's resting exit goes by in the log and the ledger.
    /// The Maker's is unchanged from before FairValue shared the path.
    pub fn label(strategy: &str) -> String {
        match strategy {
            "MakerStrategy" => "Maker resting exit".to_string(),
            "FairValueStrategy" => "FairValue resting TP".to_string(),
            other => format!("{other} resting exit"),
        }
    }

    /// The ledger reason for a lift. Byte-for-byte the Maker's old text for
    /// the Maker; FairValue's names the take-profit rather than a spread.
    pub fn ledger_reason(strategy: &str, price: Decimal, entry: Decimal) -> String {
        let gain = if entry > Decimal::ZERO { (price - entry) / entry * Decimal::ONE_HUNDRED } else { Decimal::ZERO };
        match strategy {
            "MakerStrategy" => format!("Maker resting exit: lifted @ ${:.4} (spread captured, gain={:.2}%)", price, gain),
            _ => format!("{}: lifted @ ${:.4} (gain={:.2}%)", label(strategy), price, gain),
        }
    }

    /// One booked lift of a resting ask: the P&L on the slice, net of the
    /// entry fee it carries.
    #[derive(Debug, Clone, PartialEq)]
    pub struct LiftBooking {
        /// Net P&L on the lifted shares.
        pub pnl: Decimal,
        /// The entry fee attributed to the lifted shares (prorated).
        pub entry_fee_booked: Decimal,
        /// The entry fee that stays with whatever is still held.
        pub entry_fee_left: Decimal,
    }

    impl LiftBooking {
        /// Log suffix naming the fee netted, empty when there was none — so
        /// the Maker's lines read exactly as they did.
        pub fn fee_note(&self) -> String {
            if self.entry_fee_booked.is_zero() { String::new() }
            else { format!(" (net of entry fee ${:.4})", self.entry_fee_booked) }
        }
    }

    /// Book `lifted` shares sold at the resting `price` against a position
    /// entered at `entry` that still carries `entry_fee` unbooked across
    /// `held_before` shares.
    ///
    /// A resting ask pays no taker fee, so the only cost to net is the fee the
    /// position paid to OPEN — zero for a Maker fill, the taker fee for a
    /// FairValue entry that crossed the spread. Before this the lift was
    /// booked as `(price − entry) × lifted` with no fee term, which was exact
    /// for the Maker and would have overstated a FairValue round trip by the
    /// whole entry fee (39% of the gross on the first live TP). The fee is
    /// prorated by the share of the position sold, as `fak_exit::settle`
    /// does, and never goes negative or beyond what was carried.
    pub fn book_lift(
        price: Decimal,
        entry: Decimal,
        entry_fee: Decimal,
        lifted: Decimal,
        held_before: Decimal,
    ) -> LiftBooking {
        let lifted = lifted.max(Decimal::ZERO);
        let carried = entry_fee.max(Decimal::ZERO);
        let frac = if held_before > Decimal::ZERO {
            (lifted / held_before).min(Decimal::ONE)
        } else {
            Decimal::ONE
        };
        let entry_fee_booked = (carried * frac).min(carried);
        LiftBooking {
            pnl: (price - entry) * lifted - entry_fee_booked,
            entry_fee_booked,
            entry_fee_left: carried - entry_fee_booked,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use rust_decimal_macros::dec;

        /// 2026-09-03 trade 5, exactly. The ask (7.35 @ $0.35) had 5.314284
        /// matched, all of it booked by the sweep at 22:20:05; 2.035716 shares
        /// were still resting when the reprice pulled it 21s later. The right
        /// figure to book at that moment is nothing.
        #[test]
        fn a_partial_the_sweep_already_booked_is_not_booked_again() {
            assert_eq!(lift_since_booking(true, dec!(5.314284), dec!(5.314284), dec!(2.035716)), dec!(0));
        }

        /// Belt and braces for the same trade: even had the side-blind 12.664284
        /// reached here with nothing booked, no more than the position could
        /// have been sold.
        #[test]
        fn nothing_beyond_the_position_can_be_lifted() {
            assert_eq!(lift_since_booking(true, dec!(12.664284), dec!(0), dec!(2.035716)), dec!(2.035716));
        }

        /// A partial the sweep has NOT seen yet is exactly the case this path is for.
        #[test]
        fn an_unbooked_partial_is_booked_in_full() {
            assert_eq!(lift_since_booking(true, dec!(3), dec!(0), dec!(7.35)), dec!(3));
            assert_eq!(lift_since_booking(true, dec!(5.314284), dec!(3), dec!(7.35)), dec!(2.314284));
        }

        /// An ask that is no longer on the book carries no venue figure, whatever
        /// the caller thinks it remembers.
        #[test]
        fn a_vanished_ask_reports_nothing() {
            assert_eq!(lift_since_booking(false, dec!(5), dec!(0), dec!(7.35)), dec!(0));
        }

        #[test]
        fn a_stale_figure_below_what_was_booked_is_not_negative() {
            assert_eq!(lift_since_booking(true, dec!(1), dec!(2), dec!(7.35)), dec!(0));
        }

        /// The balance endpoint reads 0 on failure, so 0 proves nothing; a
        /// positive reading below the baseline is a lift; a reading at or above
        /// it (lag reads high) is not.
        #[test]
        fn a_chain_drop_needs_a_positive_reading_below_the_baseline() {
            assert_eq!(chain_drop(dec!(7.35), dec!(0)), dec!(0));
            assert_eq!(chain_drop(dec!(7.35), dec!(2.035716)), dec!(5.314284));
            assert_eq!(chain_drop(dec!(7.35), dec!(7.35)), dec!(0));
            assert_eq!(chain_drop(dec!(7.35), dec!(9)), dec!(0));
        }

        #[test]
        fn the_remainder_follows_the_ledger_unless_the_chain_reads_lower() {
            assert_eq!(remaining_after(dec!(7.35), dec!(5.314284), dec!(0)), dec!(2.035716));
            assert_eq!(remaining_after(dec!(7.35), dec!(5.314284), dec!(1.5)), dec!(1.5));
            assert_eq!(remaining_after(dec!(7.35), dec!(5.314284), dec!(9)), dec!(2.035716));
            assert_eq!(remaining_after(dec!(2), dec!(5), dec!(0)), dec!(0));
        }

        /// A Maker fill paid nothing to open, so its lift books exactly what
        /// it always did: the gross move, no fee term, no note.
        #[test]
        fn a_maker_lift_books_the_gross_move_as_before() {
            let bk = book_lift(dec!(0.35), dec!(0.33), dec!(0), dec!(7.35), dec!(7.35));
            assert_eq!(bk.pnl, dec!(0.02) * dec!(7.35));
            assert_eq!(bk.entry_fee_booked, dec!(0));
            assert_eq!(bk.entry_fee_left, dec!(0));
            assert_eq!(bk.fee_note(), "");
        }

        /// Live trade #3 (2026-09-06), had its take-profit rested: NO bought
        /// at $0.20, 28 shares, taker entry fee 0.07 × 0.20 × 0.80 × 28 =
        /// $0.3136, lifted in full at $0.24. The gross is $1.12; the round
        /// trip is $0.8064. Booking the gross would overstate it by 39%.
        #[test]
        fn a_taker_entered_lift_is_net_of_the_entry_fee() {
            let fee = crate::venues::intl::taker_fee(dec!(0.07), dec!(0.20), dec!(28));
            assert_eq!(fee, dec!(0.3136));
            let bk = book_lift(dec!(0.24), dec!(0.20), fee, dec!(28), dec!(28));
            assert_eq!(bk.pnl, dec!(1.12) - dec!(0.3136));
            assert_eq!(bk.entry_fee_booked, dec!(0.3136));
            assert_eq!(bk.entry_fee_left, dec!(0));
            assert_eq!(bk.fee_note(), " (net of entry fee $0.3136)");
        }

        /// Half the position lifted: half the fee goes with it, half stays
        /// for the remainder — which a later FAK or lift nets in turn, so the
        /// fee is booked exactly once across the whole position.
        #[test]
        fn a_partial_lift_prorates_the_entry_fee_and_leaves_the_rest() {
            let bk = book_lift(dec!(0.24), dec!(0.20), dec!(0.3136), dec!(14), dec!(28));
            assert_eq!(bk.entry_fee_booked, dec!(0.1568));
            assert_eq!(bk.entry_fee_left, dec!(0.1568));
            assert_eq!(bk.pnl, dec!(0.56) - dec!(0.1568));
            let rest = book_lift(dec!(0.24), dec!(0.20), bk.entry_fee_left, dec!(14), dec!(14));
            assert_eq!(bk.entry_fee_booked + rest.entry_fee_booked, dec!(0.3136));
            assert_eq!(rest.entry_fee_left, dec!(0));
        }

        /// The fee can be booked no more than once and never goes negative,
        /// whatever the caller's share arithmetic says.
        #[test]
        fn the_booked_fee_is_bounded_by_what_was_carried() {
            let bk = book_lift(dec!(0.24), dec!(0.20), dec!(0.3136), dec!(30), dec!(28));
            assert_eq!(bk.entry_fee_booked, dec!(0.3136));
            assert_eq!(bk.entry_fee_left, dec!(0));
            let bk = book_lift(dec!(0.24), dec!(0.20), dec!(-1), dec!(28), dec!(28));
            assert_eq!(bk.entry_fee_booked, dec!(0));
            let bk = book_lift(dec!(0.24), dec!(0.20), dec!(0.3136), dec!(28), dec!(0));
            assert_eq!(bk.entry_fee_booked, dec!(0.3136));
        }

        /// Each strategy's resting exit answers to its own knob.
        #[test]
        fn each_strategy_is_gated_by_its_own_knob() {
            assert!(enabled_for("MakerStrategy", true, false));
            assert!(!enabled_for("MakerStrategy", false, true));
            assert!(enabled_for("FairValueStrategy", false, true));
            assert!(!enabled_for("FairValueStrategy", true, false));
        }

        /// The Maker's ledger text is unchanged; FairValue's names the target.
        #[test]
        fn the_ledger_reason_keeps_the_makers_text_and_names_fairvalues_target() {
            assert_eq!(
                ledger_reason("MakerStrategy", dec!(0.35), dec!(0.33)),
                "Maker resting exit: lifted @ $0.3500 (spread captured, gain=6.06%)"
            );
            assert_eq!(
                ledger_reason("FairValueStrategy", dec!(0.24), dec!(0.20)),
                "FairValue resting TP: lifted @ $0.2400 (gain=20.00%)"
            );
            assert_eq!(label("MakerStrategy"), "Maker resting exit");
            assert_eq!(label("FairValueStrategy"), "FairValue resting TP");
        }
    }
}

/// Settlement of one exit FAK from the venue's synchronous response.
///
/// Pure, so the accounting on the most dangerous path in the engine — the
/// exit of a position that is already losing on a moving book — is pinned by
/// tests against the real fills rather than exercised only on real money.
pub mod fak_exit {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use crate::state::Position;

    /// What one FAK actually did.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Settlement {
        /// Shares the venue matched (clamped to what was requested).
        pub filled: Decimal,
        /// Shares still held afterwards.
        pub remainder: Decimal,
        /// Average price the matched shares sold at.
        pub exit_price: Decimal,
        /// Taker fee on the matched shares, at the exit price.
        pub exit_fee: Decimal,
        /// The entry fee attributable to the matched shares (prorated).
        pub entry_fee_booked: Decimal,
        /// The entry fee that stays with the remainder.
        pub entry_fee_left: Decimal,
        /// Net P&L on the matched shares.
        pub pnl: Decimal,
    }

    impl Settlement {
        pub fn fees(&self) -> Decimal { self.entry_fee_booked + self.exit_fee }
    }

    /// `matched` is the venue's matched size; `None` means the response carried
    /// no answer and is treated as nothing sold — never as everything sold,
    /// because the only safe error is to keep managing shares we may still hold.
    pub fn settle(
        requested: Decimal,
        matched: Option<Decimal>,
        venue_price: Option<Decimal>,
        fallback_price: Decimal,
        entry: Decimal,
        entry_fee: Decimal,
        fee_rate: Decimal,
    ) -> Settlement {
        let filled = matched.unwrap_or(Decimal::ZERO).max(Decimal::ZERO).min(requested);
        let remainder = requested - filled;
        let exit_price = venue_price.unwrap_or(fallback_price);
        let frac = if requested > Decimal::ZERO { filled / requested } else { Decimal::ZERO };
        let entry_fee_booked = entry_fee * frac;
        let exit_fee = crate::venues::intl::taker_fee(fee_rate, exit_price, filled);
        let pnl = (exit_price - entry) * filled - entry_fee_booked - exit_fee;
        Settlement {
            filled, remainder, exit_price, exit_fee, entry_fee_booked,
            entry_fee_left: entry_fee - entry_fee_booked, pnl,
        }
    }

    /// When a stop is rejected because the shares are already gone, can the
    /// exit be attributed to the resting ask that was on those shares?
    ///
    /// Only if such an ask existed AND the market was still open — past close
    /// the shares could have left by settlement, whose price is $1 or $0, not
    /// the ask. A post-only ask pays no taker fee, so only the entry fee is
    /// netted. `None` means nothing verifiable: the caller writes no row.
    pub fn attribute_to_resting_ask(
        resting_ask: Option<Decimal>,
        market_open: bool,
        entry: Decimal,
        entry_fee: Decimal,
        shares: Decimal,
    ) -> Option<(Decimal, Decimal)> {
        let px = resting_ask?;
        if !market_open || shares <= Decimal::ZERO || px <= Decimal::ZERO {
            return None;
        }
        Some((px, (px - entry) * shares - entry_fee))
    }

    /// The SAME position, holding only what the venue did not sell. Every
    /// timestamp and link survives; only size and the unbooked entry fee change.
    pub fn retain_remainder(p: &Position, remainder: Decimal, entry_fee_left: Decimal) -> Position {
        let mut kept = p.clone();
        kept.shares = remainder;
        kept.entry_fee = entry_fee_left.max(dec!(0));
        kept
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use chrono::{Duration, Utc};
        use crate::venues::core::MarketId;

        const RATE: Decimal = dec!(0.07);

        /// Trade 19, first attempt, 21:30:52: 24 shares requested at bid $0.24,
        /// the venue matched nothing. The right settlement is "nothing sold,
        /// everything retained, no P&L" — immediately, not after 18 seconds of
        /// balance polling while the bid fell to $0.19.
        #[test]
        fn a_fak_the_venue_matched_nothing_on_retains_everything_and_books_nothing() {
            let st = settle(dec!(24), Some(dec!(0)), None, dec!(0.24), dec!(0.30), dec!(0), RATE);
            assert_eq!(st.filled, dec!(0));
            assert_eq!(st.remainder, dec!(24));
            assert_eq!(st.pnl, dec!(0));
            assert_eq!(st.fees(), dec!(0));
        }

        /// Trade 19, second attempt, 21:31:10: 32 shares matched at $0.19.
        /// Booked −$3.8647 net of $0.3447 fees — the venue's own numbers,
        /// reproduced from its response rather than from a balance read.
        #[test]
        fn a_fully_matched_fak_books_the_venues_price_fee_and_pnl() {
            let taking = dec!(0.19) * dec!(32);
            let st = settle(dec!(32), Some(dec!(32)), Some(taking / dec!(32)), dec!(0.20), dec!(0.30), dec!(0), RATE);
            assert_eq!(st.filled, dec!(32));
            assert_eq!(st.remainder, dec!(0));
            assert!((st.exit_fee - dec!(0.3447)).abs() < dec!(0.0001), "fee {}", st.exit_fee);
            assert!((st.pnl - dec!(-3.8647)).abs() < dec!(0.0002), "pnl {}", st.pnl);
        }

        /// A partial match books only what sold, prorates the entry fee to it,
        /// and leaves the rest of the fee with the retained shares — so the
        /// eventual second exit still nets the whole round trip.
        #[test]
        fn a_partial_match_prorates_the_entry_fee_and_keeps_the_rest_with_the_remainder() {
            let st = settle(dec!(32), Some(dec!(8)), Some(dec!(0.24)), dec!(0.24), dec!(0.30), dec!(0.64), RATE);
            assert_eq!(st.filled, dec!(8));
            assert_eq!(st.remainder, dec!(24));
            assert_eq!(st.entry_fee_booked, dec!(0.16));
            assert_eq!(st.entry_fee_left, dec!(0.48));
        }

        /// A response with no answer must read as "nothing sold", never as
        /// "everything sold": the only safe error is to keep managing shares we
        /// may still hold. A retry sells at most the remainder, so this cannot
        /// sell a position twice.
        #[test]
        fn no_answer_from_the_venue_is_treated_as_nothing_sold() {
            let st = settle(dec!(32), None, None, dec!(0.24), dec!(0.30), dec!(0), RATE);
            assert_eq!(st.remainder, dec!(32));
            assert_eq!(st.pnl, dec!(0));
            // And an over-report cannot make us book more than we asked for.
            let over = settle(dec!(32), Some(dec!(40)), None, dec!(0.24), dec!(0.30), dec!(0), RATE);
            assert_eq!(over.filled, dec!(32));
        }

        /// Trade 18, 2026-09-01: 13.53 shares, entry $0.61, remainder of a
        /// partial lift with the ask still resting at $0.63. The venue rejected
        /// the take-profit FAK — the ask had already been lifted — and the
        /// engine wrote "$0.61 → $0.61, pnl 0" under a reason claiming +9.83%.
        /// The ask was the only order on those shares, and a resting limit
        /// cannot slip: the exit is $0.63, exactly, for +$0.2706 before the
        /// entry fee.
        #[test]
        fn a_rejected_stop_with_a_resting_ask_books_the_ask_price_not_zero() {
            let (px, pnl) = attribute_to_resting_ask(Some(dec!(0.63)), true, dec!(0.61), dec!(0), dec!(13.53))
                .expect("attributable");
            assert_eq!(px, dec!(0.63));
            assert_eq!(pnl, dec!(0.2706));
        }

        /// Without a resting ask there is no sourced price, and past close the
        /// shares may have settled at $1/$0 rather than sold — in both cases
        /// the answer is "nothing verifiable", never a guess.
        #[test]
        fn no_resting_ask_or_a_closed_market_is_not_attributable() {
            assert_eq!(attribute_to_resting_ask(None, true, dec!(0.61), dec!(0), dec!(13.53)), None);
            assert_eq!(attribute_to_resting_ask(Some(dec!(0.63)), false, dec!(0.61), dec!(0), dec!(13.53)), None);
        }

        /// The Bug #22 family in this path: the old re-insert built a brand-new
        /// Position with `fill_confirmed_at = now`, so a minutes-old position
        /// read "0s held" and every hold-gated protection saw a fresh fill.
        /// The retained remainder must be the same position.
        #[test]
        fn the_retained_remainder_keeps_its_hold_clock_entry_fee_and_pair_link() {
            let opened = Utc::now() - Duration::minutes(7);
            let confirmed = Utc::now() - Duration::minutes(6);
            let p = Position {
                shares: dec!(32), avg_entry: dec!(0.30), opened_at: opened,
                close_time: Some(Utc::now() + Duration::hours(1)),
                market_name: "Bitcoin Up or Down - September 1, 9PM ET".into(),
                pair_token_id: MarketId::new("tok"), fill_confirmed_at: Some(confirmed),
                paired_leg_token_id: Some(MarketId::new("pair")), entry_fee: dec!(0.64),
            };
            let kept = retain_remainder(&p, dec!(24), dec!(0.48));
            assert_eq!(kept.shares, dec!(24));
            assert_eq!(kept.entry_fee, dec!(0.48));
            assert_eq!(kept.fill_confirmed_at, Some(confirmed), "the hold clock must not restart");
            assert_eq!(kept.opened_at, opened);
            assert_eq!(kept.paired_leg_token_id, Some(MarketId::new("pair")));
        }
    }
}

#[cfg(test)]
mod live_strike_tests {
    use super::live_hourly_strike;
    use crate::tasks::market_monitor::MarketState;
    use crate::venues::core::MarketId;
    use rust_decimal_macros::dec;
    use tokio::sync::watch;

    fn state(cid: &str, strike: Option<rust_decimal::Decimal>) -> MarketState {
        (
            MarketId::new("y"), MarketId::new("n"), "Bitcoin Up or Down - September 3, 6AM ET".to_string(),
            Some(chrono::Utc::now() + chrono::Duration::hours(1)), strike, String::new(), None, cid.to_string(),
        )
    }

    /// Deployed pre-open without a strike; the monitor later re-broadcasts the
    /// same market with the window's opening print. The tick must pick it up
    /// without a rotation — the re-broadcast has the same condition id, and the
    /// rotation arm ignores it on purpose.
    #[test]
    fn a_strike_published_after_deploy_reaches_the_tick() {
        let (tx, rx) = watch::channel(state("cid-6am", None));
        assert_eq!(live_hourly_strike(&rx, "cid-6am", None), None, "pre-open: still no strike");

        tx.send(state("cid-6am", Some(dec!(77600.02)))).unwrap();
        assert_eq!(live_hourly_strike(&rx, "cid-6am", None), Some(dec!(77600.02)));
    }

    /// A different market on the channel is a rotation the rotation arm has
    /// not processed yet. Its strike belongs to the other market and must not
    /// be applied to this one.
    #[test]
    fn another_markets_strike_is_never_borrowed() {
        let (tx, rx) = watch::channel(state("cid-6am", Some(dec!(77600))));
        tx.send(state("cid-7am", Some(dec!(77900)))).unwrap();
        assert_eq!(live_hourly_strike(&rx, "cid-6am", Some(dec!(77600))), Some(dec!(77600)));
        // And a squadron with no hourly market (empty condition id) keeps its
        // None rather than matching the sentinel's empty id.
        let (_tx2, rx2) = watch::channel(state("", Some(dec!(1))));
        assert_eq!(live_hourly_strike(&rx2, "", None), None);
    }
}
