//! US retail trading loop — venue-neutral strategy execution over the
//! [`Execution`] trait.
//!
//! The loop is **data-driven**: it classifies the selected market, asks the
//! taxonomy which vipers are meaningful for that market class
//! (`db::vipers_for_class`), and runs exactly those strategy impls through the
//! shared orchestrator (`evaluate_strategies`). Whatever signals they emit are
//! dispatched onto the venue via [`Execution::place_atomic`] /
//! [`Execution::place_order`], honoring each signal's time-in-force.
//!
//! Flow:
//!   1. discover an active binary market (`GET /v1/markets`),
//!   2. classify it and resolve its eligible vipers,
//!   3. stream both legs' order books over the [`ws`] feed,
//!   4. each tick, build a venue-neutral [`StrategyContext`] and evaluate the
//!      resolved strategies, dispatching their signals to the venue.
//!
//! Order lifecycle (Option A — reconciliation-based): resting (`Gtc`/`Gtd`)
//! orders are tracked in an [`OpenOrders`] set and reconciled every
//! [`LIFECYCLE_SYNC_SECS`] against the venue's positions endpoint —
//! **confirming** fills (no fabricated fills), **cancelling** stale unfilled
//! orders ([`STALE_ORDER_SECS`]), and **flattening** any naked leg whose hedge
//! partner neither filled nor still rests. All tracked orders are cancelled on
//! stand-down / rotation. (Intl uses on-chain balance polling for the same job;
//! a shared `OrderLifecycle` over an extended `Execution` trait is the eventual
//! Option C convergence.)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::{watch, Mutex};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::api::server::AssetRaptorHealth;
use crate::cag::Cag;
use crate::helpers::db;
use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::metrics;
use crate::orchestrator::{
    aggregate_and_resolve_signals, evaluate_strategies, Strategy, StrategyContext,
    StrategyRegistry,
};
use crate::squadron::{CryptoAsset, Squadron, SquadronConfig, SquadronRaptors, SquadronState};
use crate::raptors::sports::SportsSnapshot;
use crate::state::{
    MarketConfig, MarketPhase, MarketSnapshot, OrderParams, Position, PositionMap, PriceState,
    StrategySignal,
};
use crate::venues::core::{Execution, MarketId, OrderIntent, Side};
use crate::venues::lifecycle::{LifecycleConfig, OrderLifecycle};

use super::{ws, UsRetailVenue};

/// Optional substring filter (matched against slug / question) to pick a market.
const ENV_MARKET_FILTER: &str = "POLYMARKET_US_MARKET_FILTER";

const TICK_MS: u64 = 500;
/// Pause after any order placement so the loop doesn't spam a fleeting book.
const ACTION_COOLDOWN_SECS: u64 = 30;
/// Retry cadence while waiting for a tradeable market to appear.
const DISCOVERY_RETRY_SECS: u64 = 300; // 5 min — avoid hammering when no markets are live
/// How often to refresh the dashboard + reload squadron config / collateral.
const DASHBOARD_SYNC_SECS: u64 = 30;
/// Skip selecting any market that closes within this many seconds — not worth
/// committing capital we can't work before resolution.
const MIN_TIME_TO_CLOSE_SECS: i64 = 300; // 5 minutes
/// Wind-down window: this many seconds before close, stop opening new positions
/// (squadron RTB) and let existing ones resolve, then rotate on close.
const MARKET_RTB_WINDOW_SECS: i64 = 120; // 2 minutes
/// How often the order-lifecycle reconciler runs (fill-confirm + stale-cancel +
/// naked-leg detection). Short enough to bound directional exposure on a resting
/// maker leg, long enough not to hammer the positions endpoint.
const LIFECYCLE_SYNC_SECS: u64 = 10;
// Stale-order and flatten thresholds now live in `LifecycleConfig::us()`
// (`crate::venues::lifecycle`), shared with the venue-neutral lifecycle engine.
/// How often to scan for a hotter market while already trading.
const MARKET_RESCAN_SECS: u64 = 300; // 5 minutes
/// Rotate to a new market only when it has at least this much more volume than
/// the current one. Prevents thrashing between near-equal markets.
const ROTATION_VOLUME_THRESHOLD: f64 = 10_000.0;
pub const US_ASSET: &str = "us";

/// Why a single-market trading session ended — drives the outer rotation loop.
enum MarketOutcome {
    /// The market reached its close time; rotate to the next one.
    Closed,
    /// A hotter market appeared and positions are flat — rotate now.
    BetterMarketFound,
    /// Global cancellation fired; exit the trader entirely.
    Cancelled,
}

/// Run the US retail trading loop until `cancel` fires.
///
/// Outer **rotation** loop: select a market, trade it until it closes, then
/// re-discover the next one. This mirrors the intl patrol's market rotation, but
/// the close trigger is each market's own `close_time` (a sports game resolves on
/// its own schedule) rather than the hourly-crypto cadence. The shared
/// [`MarketConfig::phase`] classifier and the squadron RTB/stand-down state
/// machine are reused so close semantics are identical across venues.
pub async fn run_us_trader(
    venue: Arc<UsRetailVenue>,
    cag: Cag,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
    markets_tx: Arc<watch::Sender<HashMap<String, String>>>,
    process_heartbeat_secs: Arc<AtomicU64>,
    sports_rx: watch::Receiver<SportsSnapshot>,
    cancel: CancellationToken,
) {
    let filter = std::env::var(ENV_MARKET_FILTER).ok().filter(|s| !s.is_empty());
    info!("🇺🇸 US trader starting — market filter={filter:?}");

    loop {
        if cancel.is_cancelled() {
            return;
        }

        // ── Select a tradeable market (retry until one matches or cancelled) ──
        let pair = match select_market(&venue, &filter, &cancel, &process_heartbeat_secs).await {
            Some(p) => p,
            None => return, // cancelled during discovery
        };

        // Per-market cancellation — a child of `cancel`, fired on rotation so this
        // market's WS feeds drain cleanly (mirrors intl's `ws_cancel`). It also
        // completes automatically if the global `cancel` fires.
        let market_cancel = cancel.child_token();

        let outcome = trade_one_market(
            &venue,
            &cag,
            &raptor_health_tx,
            &markets_tx,
            &process_heartbeat_secs,
            &sports_rx,
            &market_cancel,
            pair,
        ).await;

        // Tear down this market's feeds before re-discovering.
        market_cancel.cancel();

        match outcome {
            MarketOutcome::Cancelled => return,
            MarketOutcome::BetterMarketFound => {
                info!("🔀 US market rotation — hotter market found, switching");
                // No pause: the new market is already live and liquid.
            }
            MarketOutcome::Closed => {
                info!("🔁 US market closed — rotating to next market");
                // Brief pause so we don't hammer discovery the instant a market
                // resolves (its replacement may not be listed yet).
                if wait_or_cancel(&cancel, DISCOVERY_RETRY_SECS).await {
                    return;
                }
            }
        }
    }
}

/// Discover markets and pick one to trade, skipping any already closed or closing
/// within [`MIN_TIME_TO_CLOSE_SECS`]. Retries until a market matches or `cancel`
/// fires. Returns `None` only on cancellation.
async fn select_market(
    venue: &Arc<UsRetailVenue>,
    filter: &Option<String>,
    cancel: &CancellationToken,
    process_heartbeat_secs: &AtomicU64,
) -> Option<super::markets::UsMarketPair> {
    loop {
        if cancel.is_cancelled() {
            return None;
        }
        // Keep the OS watchdog satisfied while we poll for a tradeable market —
        // discovery can legitimately take many minutes (off-hours, thin slate).
        touch_heartbeat(process_heartbeat_secs);
        match venue.discover_binary_markets().await {
            Ok(markets) if !markets.is_empty() => {
                info!(
                    "📊 Discovered {} binary markets. First 5: {}",
                    markets.len(),
                    markets.iter()
                        .take(5)
                        .map(|m| format!("\"{}\"", m.question))
                        .collect::<Vec<_>>()
                        .join(", ")
                );

                // Drop markets already closed or too close to close to be worth
                // entering — no point committing capital we can't work first.
                let now = Utc::now();
                let total_pairs = markets.len();
                let tradeable: Vec<_> = markets.into_iter()
                    .filter(|m| match m.close_time {
                        Some(c) => (c - now).num_seconds() > MIN_TIME_TO_CLOSE_SECS,
                        None => true, // always-open market
                    })
                    .collect();

                if tradeable.is_empty() {
                    warn!(
                        "US trader: {total_pairs} pair(s) found but all closed or closing within {MIN_TIME_TO_CLOSE_SECS}s — retrying"
                    );
                } else {
                    let selected = match filter {
                        Some(f) => {
                            let fl = f.to_lowercase();
                            tradeable.into_iter().find(|m| {
                                m.slug.to_lowercase().contains(&fl)
                                    || m.question.to_lowercase().contains(&fl)
                            })
                        }
                        None => tradeable.into_iter().next(),
                    };
                    if let Some(m) = selected {
                        info!(
                            "🎯 US arb target: \"{}\" [YES={} / NO={}] close={:?}",
                            m.question, m.long, m.short, m.close_time
                        );
                        return Some(m);
                    }
                    warn!("US trader: no active market matched filter {filter:?} — retrying");
                }
            }
            Ok(_) => warn!("US trader: no active binary markets — retrying"),
            Err(e) => warn!("US trader: market discovery failed: {e} — retrying"),
        }
        if wait_or_cancel(cancel, DISCOVERY_RETRY_SECS).await {
            return None;
        }
    }
}

/// Trade a single market until it closes ([`MarketOutcome::Closed`]) or the
/// trader is cancelled ([`MarketOutcome::Cancelled`]). The caller rotates to the
/// next market on `Closed`.
#[allow(clippy::too_many_arguments)]
async fn trade_one_market(
    venue: &Arc<UsRetailVenue>,
    cag: &Cag,
    raptor_health_tx: &Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
    markets_tx: &Arc<watch::Sender<HashMap<String, String>>>,
    process_heartbeat_secs: &AtomicU64,
    sports_rx: &watch::Receiver<SportsSnapshot>,
    cancel: &CancellationToken,
    pair: super::markets::UsMarketPair,
) -> MarketOutcome {
    // ── Register a squadron with the CAG so the Control Tower lists it ────────
    // The US venue runs a standalone arb loop (no intl-style patrol), but the
    // dashboard reads squadrons from the CAG registry — so without this the UI
    // shows zero squadrons even though the venue is live.
    let squadron = register_us_squadron(cag, &pair, sports_rx.clone());
    let squadron_id = squadron.id.clone();

    // Seed the squadron's Viper config so the detail view's strategy cards render.
    seed_squadron_config(&squadron_id).await;

    // Classify the market's domain and link it to its eligible raptors/vipers via
    // the shared, venue-neutral taxonomy (same path intl uses).
    let market_class = squadron.classify_and_link().await;

    // Rename the squadron to describe what it hunts (its market class).
    cag.update_name(&squadron_id, us_squadron_name(&market_class));

    // Resolve the vipers meaningful for this market class and instantiate exactly
    // those strategy impls from the shared registry.
    let viper_kinds = match db::pool() {
        Some(p) => db::vipers_for_class(p, &market_class).await,
        None => Vec::new(),
    };
    let strategies = build_strategies(&viper_kinds);
    info!(
        "🎯 US loop will run {} viper(s) for class '{}': {:?}",
        strategies.len(),
        market_class,
        strategies.iter().map(|s| s.name()).collect::<Vec<_>>()
    );
    if strategies.is_empty() {
        warn!("US trader: no runnable vipers for class '{market_class}' — dashboard only");
    }

    // Venue-neutral market config (now carrying the real close time, so the
    // shared phase classifier can drive wind-down / rotation) + position map.
    let market_cfg = MarketConfig {
        yes_token: pair.long.clone(),
        no_token: pair.short.clone(),
        market_name: pair.question.clone(),
        market_close_time: pair.close_time,
        strike_price: None,
        is_neg_risk: false,
        condition_id: String::new(),
        yes_fee_bps: 0,
        no_fee_bps: 0,
    };
    let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
    // Shared, venue-neutral order lifecycle engine (Option C). Drives fill-confirm,
    // stale-cancel, and naked-leg flatten off the `Execution` trait surface.
    let lifecycle = Arc::new(OrderLifecycle::new(LifecycleConfig::us()));
    // Upgrade fill confirmation to event-precise if the venue exposes a feed
    // (no-op today: UsRetailVenue::subscribe_fills returns None → poll fallback).
    let _fill_listener = lifecycle.spawn_fill_listener(Arc::clone(&venue), Arc::clone(&positions));
    let market_started_at = Utc::now();

    // Publish Raptor telemetry + active market so the squadron detail panels
    // populate (both feed `/api/status`).
    publish_us_raptor_health(raptor_health_tx, true);
    publish_us_strategy_market(markets_tx, &viper_kinds, &pair.question);

    // ── Stream both legs' order books (tied to the per-market cancel token) ───
    let ws_url = venue.markets_ws_url();
    let ws_auth = venue.ws_auth();
    let default_feed: PriceState = (dec!(0), dec!(0), dec!(1), dec!(0), Utc::now());
    let (long_tx, long_rx) = watch::channel(default_feed);
    let (short_tx, short_rx) = watch::channel(default_feed);
    ws::spawn_market_feed(ws_url.clone(), pair.long.as_str().to_string(), ws_auth.clone(), long_tx, cancel.clone());
    ws::spawn_market_feed(ws_url, pair.short.as_str().to_string(), ws_auth, short_tx, cancel.clone());

    // ── Dashboard + strategy-context state ───────────────────────────────────
    let pool = db::pool_for(US_ASSET);
    let starting = venue.collateral().await.unwrap_or(Decimal::ZERO);
    let mut available_collateral = starting;
    let mut session_pnl = Decimal::ZERO;
    let mut dyn_cfg = DynamicConfig::load_for_squadron(&squadron_id).await;
    if let Some(p) = &pool {
        let (coll, total) = sync_dashboard(venue.as_ref(), p, starting).await;
        available_collateral = coll;
        session_pnl = total - starting;
    }

    // ── Tick loop ────────────────────────────────────────────────────────────
    let mut price_tick = tokio::time::interval(Duration::from_millis(TICK_MS));
    let mut dash_tick = tokio::time::interval(Duration::from_secs(DASHBOARD_SYNC_SECS));
    let mut lifecycle_tick = tokio::time::interval(Duration::from_secs(LIFECYCLE_SYNC_SECS));
    let mut rescan_tick = tokio::time::interval(Duration::from_secs(MARKET_RESCAN_SECS));
    let _ = rescan_tick.tick().await; // skip the immediate first fire
    let mut cooldown_until = Instant::now();
    let mut winding_down = false;

    // Squadron is now actively patrolling its market — reflect that in the UI.
    cag.update_state(&squadron_id, SquadronState::Patrolling);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("US trader: cancelled — standing down");
                lifecycle.cancel_all(venue.as_ref()).await;
                cag.update_state(&squadron_id, SquadronState::StoodDown);
                publish_us_raptor_health(raptor_health_tx, false);
                return MarketOutcome::Cancelled;
            }
            _ = dash_tick.tick() => {
                if let Some(p) = &pool {
                    let (coll, total) = sync_dashboard(venue.as_ref(), p, starting).await;
                    available_collateral = coll;
                    session_pnl = total - starting;
                }
                // Pick up any Control Tower config edits for this squadron.
                dyn_cfg = DynamicConfig::load_for_squadron(&squadron_id).await;
                continue;
            }
            _ = rescan_tick.tick() => {
                // Scan for a hotter market. Only rotate when flat (no open
                // positions) to avoid leaving a naked leg mid-arb.
                let has_positions = !positions.lock().await.is_empty();
                if has_positions {
                    continue;
                }
                match venue.discover_binary_markets().await {
                    Ok(mut candidates) if !candidates.is_empty() => {
                        // Best market by volume, excluding the one we're already on.
                        candidates.retain(|m| m.slug != pair.slug);
                        if let Some(best) = candidates.iter().max_by(|a, b| a.volume.partial_cmp(&b.volume).unwrap_or(std::cmp::Ordering::Equal)) {
                            if best.volume > pair.volume + ROTATION_VOLUME_THRESHOLD {
                                info!(
                                    "🔍 Hotter market found: \"{}\" vol={:.0} > current \"{}\" vol={:.0} + threshold {:.0} — rotating",
                                    best.question, best.volume, pair.question, pair.volume, ROTATION_VOLUME_THRESHOLD
                                );
                                lifecycle.cancel_all(venue.as_ref()).await;
                                cag.update_state(&squadron_id, SquadronState::StoodDown);
                                publish_us_raptor_health(raptor_health_tx, false);
                                return MarketOutcome::BetterMarketFound;
                            }
                        }
                    }
                    _ => {} // discovery failure during rescan is non-fatal
                }
                continue;
            }
            _ = lifecycle_tick.tick() => {
                // Confirm resting fills, cancel stale orders, flatten naked legs.
                let flattened = lifecycle.reconcile(venue.as_ref(), &positions).await;
                for leg in flattened {
                    let pnl = (leg.exit_price - leg.avg_entry) * leg.shares;
                    warn!(
                        "📋 [{strategy}] lifecycle flatten recorded: {market} entry={entry:.4} exit={exit:.4} shares={shares} pnl={pnl:.4}",
                        strategy = leg.strategy,
                        market   = leg.market_name,
                        entry    = leg.avg_entry,
                        exit     = leg.exit_price,
                        shares   = leg.shares,
                    );
                    let strat  = leg.strategy.clone();
                    let market = leg.market_name.clone();
                    let avg_entry  = leg.avg_entry;
                    let exit_price = leg.exit_price;
                    let shares     = leg.shares;
                    tokio::spawn(async move {
                        metrics::record_trade(
                            US_ASSET,
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
                continue;
            }
            _ = price_tick.tick() => {}
        }
        // Pulse the OS watchdog every tick so quiet markets (no actionable
        // signal for minutes) don't trip the 5-min silence kill-switch.
        touch_heartbeat(process_heartbeat_secs);

        // ── Close-phase gate (shared, venue-neutral MarketConfig::phase) ──────
        match market_cfg.phase(Utc::now(), MARKET_RTB_WINDOW_SECS) {
            MarketPhase::Closed => {
                info!("🏁 US market \"{}\" reached close — standing down to rotate", market_cfg.market_name);
                lifecycle.cancel_all(venue.as_ref()).await;
                cag.update_state(&squadron_id, SquadronState::StoodDown);
                publish_us_raptor_health(raptor_health_tx, false);
                return MarketOutcome::Closed;
            }
            MarketPhase::WindingDown => {
                if !winding_down {
                    winding_down = true;
                    info!(
                        "⏳ US market \"{}\" within {}s of close — RTB, no new entries",
                        market_cfg.market_name, MARKET_RTB_WINDOW_SECS
                    );
                    cag.update_state(&squadron_id, SquadronState::Rtb);
                }
                continue; // stop opening new positions; let existing ones resolve
            }
            MarketPhase::Open => {}
        }

        if strategies.is_empty() || Instant::now() < cooldown_until {
            continue;
        }

        // Build a venue-neutral snapshot from both legs' live books. The US feed
        // has no oracle/velocity/funding inputs, so those fields are zero — the
        // order-book-only vipers (arbitrage/maker) don't read them.
        let snapshot = build_snapshot(&long_rx, &short_rx);

        let ctx = StrategyContext {
            market: market_cfg.clone(),
            snapshot,
            positions: positions.clone(),
            session_pnl,
            starting_collateral: starting,
            crypto_filter: US_ASSET.to_uppercase(),
            market_started_at,
            maker_market: None,
            maker_snapshot: None,
            available_collateral,
            dynamic_config: dyn_cfg.clone(),
            arb_market_lockouts: None,
        };

        // Evaluate the resolved vipers and dispatch whatever they decide.
        let eval = match evaluate_strategies(&strategies, &ctx).await {
            Ok(e) => e,
            Err(e) => { warn!("US strategy evaluation error: {e}"); continue; }
        };
        let (signals, _) = aggregate_and_resolve_signals(&eval);
        if signals.is_empty() {
            continue;
        }

        let mut acted = false;
        for (strategy_name, signal) in signals {
            if dispatch_signal(venue.as_ref(), &pool, &positions, &lifecycle, &strategy_name, &signal, starting).await {
                acted = true;
            }
        }
        if acted {
            cooldown_until = Instant::now() + Duration::from_secs(ACTION_COOLDOWN_SECS);
        }
    }
}

// ─── Strategy plumbing ────────────────────────────────────────────────────────

/// Pulse the process-level OS watchdog heartbeat with the current wall-clock.
///
/// The watchdog (see `main.rs`) calls `process::exit(1)` if no loop has touched
/// the heartbeat in 5 minutes. The intl patrol pulses it every iteration; the US
/// loop must do the same or the watchdog will kill the backend after 300s of
/// (legitimate) quiet — e.g. waiting on a thin book with no actionable signal.
fn touch_heartbeat(hb: &AtomicU64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    hb.store(now, AtomicOrdering::Relaxed);
}

/// Instantiate the strategy impls whose viper kind is in `viper_kinds`.
/// The shared registry builds all strategies; we keep only the resolved ones.
fn build_strategies(viper_kinds: &[String]) -> Vec<Box<dyn Strategy>> {
    StrategyRegistry::create_all_strategies()
        .into_iter()
        .filter(|s| viper_kinds.iter().any(|k| k == strategy_name_to_kind(&s.name())))
        .collect()
}

/// Map a registry strategy name (`"ArbitrageStrategy"`) to its taxonomy viper
/// kind id (`"arbitrage"`) so resolved kinds can select strategy impls.
fn strategy_name_to_kind(name: &str) -> &'static str {
    match name {
        "ArbitrageStrategy"    => "arbitrage",
        "MakerStrategy"        => "maker",
        "MomentumStrategy"     => "momentum",
        "TimeDecayStrategy"    => "time_decay",
        "BasisStrategy"        => "basis",
        "GboostStrategy"       => "gboost",
        "TrendReversalStrategy" => "trendcapture",
        "TrendCaptureStrategy" => "trendcapture", // legacy alias (pre-rename positions)
        _ => "",
    }
}

/// Build a venue-neutral [`MarketSnapshot`] from the two US leg feeds.
/// `PriceState` layout: `(best_bid, bid_depth, best_ask, ask_depth, ts)`.
fn build_snapshot(
    long_rx: &watch::Receiver<PriceState>,
    short_rx: &watch::Receiver<PriceState>,
) -> MarketSnapshot {
    let (yb, ybd, ya, yad) = { let b = long_rx.borrow();  (b.0, b.1, b.2, b.3) };
    let (nb, nbd, na, nad) = { let b = short_rx.borrow(); (b.0, b.1, b.2, b.3) };
    MarketSnapshot {
        yes_bid: yb, yes_bid_depth: ybd, yes_ask: ya, yes_ask_depth: yad,
        no_bid:  nb, no_bid_depth:  nbd, no_ask:  na, no_ask_depth:  nad,
        oracle_price: dec!(0), velocity: dec!(0), velocity_1s: dec!(0), acceleration: dec!(0),
        funding_rate: dec!(0), oracle_drift_60m: dec!(0), oracle_drift_10m: dec!(0),
        institutional_pulse: dec!(0), tide_coherence: dec!(0),
        oi_delta_pct: dec!(0), cvd_ratio: dec!(0),
        secs_to_expiry: 0, timestamp: Utc::now(),
    }
}

/// Map a viper's venue-neutral [`OrderParams`] to a venue [`OrderIntent`],
/// preserving its time-in-force / post-only intent.
fn order_params_to_intent(p: &OrderParams, side: Side) -> OrderIntent {
    OrderIntent {
        market: p.token_id.clone(),
        side,
        quantity: p.shares,
        price: p.price,
        tif: p.order_type,
        post_only: p.post_only,
        expiration_secs: 0,
        is_neg_risk: p.is_neg_risk,
        fee_bps: p.fee_bps,
    }
}

/// Insert a per-strategy position guard so the viper won't re-enter the same
/// token next tick. `paired` links the hedge partner for paired strategies.
async fn record_guard(
    positions: &Arc<Mutex<PositionMap>>,
    strategy_name: &str,
    params: &OrderParams,
    paired: Option<&MarketId>,
) {
    let mut map = positions.lock().await;
    map.insert(
        (strategy_name.to_string(), params.token_id.clone()),
        Position {
            shares: params.shares,
            avg_entry: params.price,
            opened_at: Utc::now(),
            close_time: None,
            market_name: params.market_name.clone(),
            pair_token_id: params.token_id.clone(),
            fill_confirmed_at: None,
            paired_leg_token_id: paired.cloned(),
        },
    );
}

// ─── Order lifecycle ──────────────────────────────────────────────────────────
//
// Fill-confirm / stale-cancel / naked-leg flatten now live in the shared,
// venue-neutral `OrderLifecycle` (`crate::venues::lifecycle`), driven from the
// tick loop via `lifecycle.track(...)` / `lifecycle.reconcile(...)` /
// `lifecycle.cancel_all(...)`. The previous bespoke US implementation
// (`TrackedOrder` / `reconcile_orders` / `cancel_all_tracked`) was retired here
// as part of Option C convergence; intl migrates onto the same engine next.

/// Dispatch one resolved strategy signal onto the venue. Returns `true` if an
/// order placement (or ghost simulation) occurred, so the caller applies the
/// cooldown. Honors each signal's time-in-force; `ghost_mode` skips the venue.
///
/// Resting (`Gtc`/`Gtd`) placements are registered in `open` so the lifecycle
/// reconciler can confirm their fill or cancel them when stale, and re-hedge a
/// naked leg. `Exit { exit_pair }` sells the leg the signal carries and clears
/// the pair's guards.
async fn dispatch_signal(
    venue: &UsRetailVenue,
    pool: &Option<sqlx::SqlitePool>,
    positions: &Arc<Mutex<PositionMap>>,
    lifecycle: &OrderLifecycle,
    strategy_name: &str,
    signal: &StrategySignal,
    starting: Decimal,
) -> bool {
    match signal {
        StrategySignal::Entry { params, pair_params: Some(pp) } => {
            if params.ghost_mode {
                info!("👻 [{strategy_name}] ghost entry pair: {} + {}", params.token_id, pp.token_id);
                record_guard(positions, strategy_name, params, Some(&pp.token_id)).await;
                record_guard(positions, strategy_name, pp, Some(&params.token_id)).await;
                return true;
            }
            let legs = [
                order_params_to_intent(params, Side::Buy),
                order_params_to_intent(pp, Side::Buy),
            ];
            match venue.place_atomic(legs).await {
                Ok([a, b]) => {
                    info!("✅ [{strategy_name}] entry pair: {} @ {:.4} | {} @ {:.4}",
                        a.order_id, a.price, b.order_id, b.price);
                    record_guard(positions, strategy_name, params, Some(&pp.token_id)).await;
                    record_guard(positions, strategy_name, pp, Some(&params.token_id)).await;
                    // Track both resting legs so the reconciler manages their
                    // lifecycle (fill-confirm / stale-cancel / naked-leg hedge).
                    lifecycle.track(&a, strategy_name, params.order_type, Some(pp.token_id.clone())).await;
                    lifecycle.track(&b, strategy_name, pp.order_type, Some(params.token_id.clone())).await;
                    if let Some(p) = pool { sync_dashboard(venue, p, starting).await; }
                    true
                }
                Err(e) => { warn!("[{strategy_name}] atomic entry failed: {e}"); false }
            }
        }
        StrategySignal::Entry { params, pair_params: None } => {
            dispatch_single(venue, pool, positions, lifecycle, strategy_name, params, Side::Buy, starting).await
        }
        StrategySignal::MakerQuote { yes, no } => {
            let mut acted = false;
            for q in [yes.as_ref(), no.as_ref()].into_iter().flatten() {
                if dispatch_single(venue, pool, positions, lifecycle, strategy_name, q, Side::Buy, starting).await {
                    acted = true;
                }
            }
            acted
        }
        StrategySignal::Exit { params, reason, exit_pair } => {
            info!("🚪 [{strategy_name}] exit ({reason}): {} @ {:.4}", params.token_id, params.price);
            let acted = dispatch_single(venue, pool, positions, lifecycle, strategy_name, params, Side::Sell, starting).await;
            // Clear this strategy's guard for the leg (and the paired leg, if any)
            // so it can re-enter later.
            let mut map = positions.lock().await;
            map.remove(&(strategy_name.to_string(), params.token_id.clone()));
            if *exit_pair {
                let paired: Vec<_> = map.iter()
                    .filter(|((s, _), p)| s == strategy_name
                        && p.paired_leg_token_id.as_ref() == Some(&params.token_id))
                    .map(|((s, t), _)| (s.clone(), t.clone()))
                    .collect();
                for k in paired { map.remove(&k); }
            }
            acted
        }
        StrategySignal::NoSignal => false,
    }
}

/// Place a single venue order from viper params, recording a buy-side guard and
/// tracking the order for lifecycle reconciliation if it rests.
async fn dispatch_single(
    venue: &UsRetailVenue,
    pool: &Option<sqlx::SqlitePool>,
    positions: &Arc<Mutex<PositionMap>>,
    lifecycle: &OrderLifecycle,
    strategy_name: &str,
    params: &OrderParams,
    side: Side,
    starting: Decimal,
) -> bool {
    if params.ghost_mode {
        info!("👻 [{strategy_name}] ghost {side:?}: {} @ {:.4} × {:.2}",
            params.token_id, params.price, params.shares);
        if matches!(side, Side::Buy) {
            record_guard(positions, strategy_name, params, None).await;
        }
        return true;
    }
    match venue.place_order(order_params_to_intent(params, side)).await {
        Ok(f) => {
            info!("✅ [{strategy_name}] {side:?} {} @ {:.4} (order {})",
                params.token_id, f.price, f.order_id);
            if matches!(side, Side::Buy) {
                record_guard(positions, strategy_name, params, None).await;
                lifecycle.track(&f, strategy_name, params.order_type, None).await;
            }
            if let Some(p) = pool { sync_dashboard(venue, p, starting).await; }
            true
        }
        Err(e) => { warn!("[{strategy_name}] {side:?} order failed: {e}"); false }
    }
}

/// Reconcile the Control Tower's view of the US venue: upsert live open positions,
/// purge settled ones, and write a portfolio P&L snapshot. Returns
/// `(collateral, total_value)` so the tick loop can feed the strategy context.
async fn sync_dashboard(venue: &UsRetailVenue, pool: &sqlx::SqlitePool, starting: Decimal) -> (Decimal, Decimal) {
    let collateral = match venue.collateral().await {
        Ok(c) => c,
        // On a transient collateral read failure, return zero available collateral
        // (which safely gates strategies off) without writing a P&L snapshot.
        Err(e) => { warn!("US dashboard sync: collateral query failed: {e}"); return (Decimal::ZERO, starting); }
    };
    let positions = venue.positions().await.unwrap_or_default();

    let mut live_ids = std::collections::HashSet::new();
    let mut positions_value = Decimal::ZERO;
    for p in &positions {
        let sym = p.market.as_str();
        live_ids.insert(sym.to_string());
        db::record_open_position(
            pool, "ArbitrageStrategy", sym, sym, side_label(sym), p.avg_price, p.shares, false,
        ).await;
        positions_value += p.shares * p.avg_price;
    }
    // Drop rows for positions the venue no longer reports (settled to cash).
    let _ = db::purge_stale_open_positions(pool, &live_ids).await;

    let total = collateral + positions_value;
    db::record_pnl_snapshot(pool, total - starting, collateral, total).await;
    (collateral, total)
}

/// `YES`/`NO` display label inferred from an instrument symbol suffix.
fn side_label(symbol: &str) -> &'static str {
    match symbol.rsplit('-').next().unwrap_or("").to_ascii_lowercase().as_str() {
        "no" | "short" | "down" => "NO",
        _ => "YES",
    }
}

async fn wait_or_cancel(cancel: &CancellationToken, secs: u64) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(Duration::from_secs(secs)) => false,
    }
}


/// Assemble and register a single arb-wing squadron for the selected US market
/// so it appears in the Control Tower's CAG squadron list.
///
/// The US venue doesn't use the intl patrol/Raptor pipeline, so the signal
/// receivers are placeholder watch channels — they exist only to satisfy the
/// `SquadronRaptors` shape and are never read by the US arb loop. Returns the
/// registered [`Squadron`] so the caller can classify it and drive its
/// lifecycle state (`Patrolling` / `StoodDown`).
fn register_us_squadron(
    cag: &Cag,
    pair: &super::markets::UsMarketPair,
    sports_rx: watch::Receiver<SportsSnapshot>,
) -> Squadron {
    // Placeholder signal channels (US arb loop reads prices from the WS feed,
    // not from Raptors). Receivers stay valid after the senders drop.
    let (_, oracle_rx) = watch::channel(Decimal::ZERO);
    let (_, velocity_rx) = watch::channel((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
    let (_, drift_rx) = watch::channel((Decimal::ZERO, Decimal::ZERO));
    // The venue-neutral Sports Raptor IS a real feed on the US build — attach it
    // so its observe-only line-movement signal is available to US squadrons.
    let mut raptors = SquadronRaptors::price_only(oracle_rx, velocity_rx, drift_rx);
    raptors.sports = Some(sports_rx);

    let market = MarketConfig {
        yes_token: pair.long.clone(),
        no_token: pair.short.clone(),
        market_name: pair.question.clone(),
        market_close_time: None,
        strike_price: None,
        is_neg_risk: false,
        condition_id: String::new(),
        yes_fee_bps: 0,
        no_fee_bps: 0,
    };

    let squadron = Squadron::new(
        CryptoAsset::Custom(US_ASSET.to_uppercase()),
        SquadronConfig::arb_wing("US Retail Arb"),
        market,
        raptors,
    );
    cag.register(&squadron);
    squadron
}

/// Derive a squadron display name from its resolved market class, so the name
/// describes what the squadron hunts rather than a fixed "US Retail Arb".
/// Falls back to a venue-generic name for the `unknown` class.
fn us_squadron_name(class: &str) -> String {
    match class {
        "sports"   => "US Sports Squadron",
        "politics" => "US Politics Squadron",
        "crypto"   => "US Crypto Squadron",
        _           => "US Retail Squadron",
    }
    .to_string()
}

/// Ensure a `squadron_configs` row exists for this squadron so the Control
/// Tower's detail view can render the Viper strategy cards.
///
/// Only seeds when absent, so operator config edits made via
/// `PATCH /api/squadrons/{id}/config` survive a restart.
async fn seed_squadron_config(squadron_id: &str) {
    if let Some(pool) = db::pool() {
        if db::squadron_config_get(pool, squadron_id).await.is_none() {
            DynamicConfig::init_for_squadron(squadron_id).await;
        }
    }
}

/// Publish the US venue's Raptor telemetry into the `/api/status` health map.
///
/// Keyed by the `us` asset slug so the squadron detail panel finds it. The US
/// order-book WS feed is the price source; there is no separate funding raptor,
/// so both flags track the same `connected` state.
fn publish_us_raptor_health(
    tx: &watch::Sender<HashMap<String, AssetRaptorHealth>>,
    connected: bool,
) {
    tx.send_modify(|map| {
        let h = map.entry(US_ASSET.to_string()).or_default();
        h.price_connected = connected;
        h.funding_connected = connected;
    });
}

/// Publish the active market under **every** resolved viper kind into the
/// `/api/status` strategy→market map, so each viper card in the squadron detail
/// (Arbitrage, Maker, …) shows the market it's running on — not just Arbitrage.
fn publish_us_strategy_market(
    tx: &watch::Sender<HashMap<String, String>>,
    viper_kinds: &[String],
    market_name: &str,
) {
    tx.send_modify(|map| {
        for kind in viper_kinds {
            map.insert(kind.clone(), market_name.to_string());
        }
    });
}

