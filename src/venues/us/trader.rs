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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::{watch, Mutex};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::api::server::AssetRaptorHealth;
use crate::cag::Cag;
use crate::helpers::db;
use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::time::{extract_strike_price, fetch_historical_strike_price};
use crate::helpers::metrics;
use crate::orchestrator::{
    aggregate_and_resolve_signals, evaluate_strategies, Strategy, StrategyContext,
    StrategyRegistry,
};
use crate::squadron::{CryptoAsset, Squadron, SquadronConfig, SquadronRaptors, SquadronState};
use crate::raptors::derivatives::DerivativesSnapshot;
use crate::raptors::horizon::HorizonSnapshot;
use crate::raptors::sports::SportsSnapshot;
use crate::raptors::tennis::TennisSnapshot;
use crate::raptors::tide::TideSnapshot;
use crate::state::{
    TradeScope,
    MarketConfig, MarketPhase, MarketSnapshot, OrderParams, Position, PositionMap, PriceState,
    StrategySignal,
};
use crate::venues::core::{Execution, MarketId, OrderIntent, Side, Fill, OrderId};
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
/// SQLite shard key for the US general wing (storage identity, not an asset).
pub const US_ASSET: &str = "us";
/// Runtime venue identity persisted on every trade and entry row. Both US
/// wings share one venue; they differ by shard and market class.
pub const US_VENUE: &str = "polymarket-us";
/// Asset key for the US crypto wing — its own DB pool, squadron id, and
/// viper-status scope so the sports and crypto squadrons never collide.
pub const US_CRYPTO_ASSET: &str = "us-crypto";

/// Which market domain a US trading wing hunts. The venue runs one wing per
/// domain concurrently: the general wing keeps the original behaviour (sports /
/// politics / anything non-crypto → order-book vipers), while the crypto wing
/// targets crypto-class markets and feeds them the full Raptor intelligence
/// stack so all nine vipers can fly.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Wing {
    General,
    Crypto,
}

impl Wing {
    fn asset(self) -> &'static str {
        match self {
            Wing::General => US_ASSET,
            Wing::Crypto => US_CRYPTO_ASSET,
        }
    }
    fn label(self) -> &'static str {
        match self {
            Wing::General => "general",
            Wing::Crypto => "crypto",
        }
    }
    /// Wing-appropriate market discovery. The crypto wing goes through
    /// `/v1/search` → `/v1/markets?eventSlug=…` because the gateway ignores
    /// the `categories=` filter on `/v1/markets`, and the sports-dominated
    /// default query never surfaced crypto (3000 pairs, zero crypto,
    /// 2026-08-08). No volume floor — hourly crypto markets rotate and start
    /// near zero volume.
    async fn discover(
        self,
        venue: &UsRetailVenue,
    ) -> anyhow::Result<Vec<super::markets::UsMarketPair>> {
        match self {
            Wing::General => venue.discover_binary_markets().await,
            Wing::Crypto => venue.discover_crypto_markets_via_search().await,
        }
    }
}

/// Cloneable bundle of live Raptor signal receivers for one crypto underlying.
///
/// The raptors themselves (Binance WS/REST, Alpaca IEX) are venue-neutral and
/// process-lifetime — spawned lazily on the first crypto market for an
/// underlying and shared by every subsequent market on it (see
/// [`raptor_stack_for`]).
#[derive(Clone)]
struct CryptoRaptors {
    oracle: watch::Receiver<Decimal>,
    /// (5 s velocity, 1 s velocity, acceleration)
    velocity: watch::Receiver<(Decimal, Decimal, Decimal)>,
    /// (60-min drift, 10-min drift, normalized realized vol)
    drift: watch::Receiver<(Decimal, Decimal, Decimal)>,
    funding: watch::Receiver<Decimal>,
    derivatives: watch::Receiver<DerivativesSnapshot>,
    tide: Option<watch::Receiver<TideSnapshot>>,
    horizon: Option<watch::Receiver<HorizonSnapshot>>,
}

/// Process-lifetime registry of spawned raptor stacks, keyed by lowercase
/// underlying symbol (`"btc"`). Prevents duplicate Binance/Alpaca connections
/// across market rotations.
static CRYPTO_RAPTOR_STACKS: OnceLock<std::sync::Mutex<HashMap<String, CryptoRaptors>>> =
    OnceLock::new();

/// Supervised task spawn (respawn-on-exit/panic), mirroring `main.rs`'s intl
/// raptor supervision so a Binance disconnect or panic never silently kills a
/// signal feed.
fn spawn_supervised<F, Fut>(name: &'static str, factory: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            match tokio::spawn(factory()).await {
                Ok(()) => warn!("⚠️ Supervised feed '{name}' exited unexpectedly — respawning in 5s"),
                Err(e) if e.is_panic() => {
                    error!("💥 Supervised feed '{name}' PANICKED — respawning in 5s: {e:?}")
                }
                Err(e) => warn!("⚠️ Supervised feed '{name}' terminated ({e:?}) — respawning in 5s"),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

/// Get (or lazily spawn) the full Raptor stack for a crypto underlying.
///
/// Spawns Price (Binance WS), Funding (FAPI), and Derivatives (FAPI) raptors
/// for any underlying; BTC additionally gets the Tide + Horizon macro raptors
/// (Alpaca IEX — degrade to defaults without `ALPACA_API_KEY_ID`). Identical
/// wiring to the intl per-asset bootstrap in `main.rs`.
fn raptor_stack_for(
    underlying: &str,
    raptor_health_tx: &Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
) -> CryptoRaptors {
    let registry = CRYPTO_RAPTOR_STACKS.get_or_init(Default::default);
    let mut map = registry.lock().expect("raptor stack registry poisoned");
    if let Some(stack) = map.get(underlying) {
        return stack.clone();
    }

    info!("🦖 Spawning crypto Raptor stack for '{underlying}' (US crypto wing)");
    // Route any lookup keyed by the underlying to this wing's shard. Nothing in
    // the US trader writes under a bare underlying today — every path goes
    // through Wing::asset() — but pool_for() returns None on a miss rather than
    // falling back, so an alias turns a future mistake into a redirect instead
    // of a silent dropped write. Mirrors what the Kalshi trader does.
    crate::helpers::db::alias_pool(underlying, US_CRYPTO_ASSET);
    let http = Arc::new(reqwest::Client::new());

    let (oracle_tx, oracle_rx) = watch::channel(dec!(0));
    let (velocity_tx, velocity_rx) = watch::channel((dec!(0), dec!(0), dec!(0)));
    let (drift_tx, drift_rx) = watch::channel((dec!(0), dec!(0), dec!(0)));
    let (funding_tx, funding_rx) = watch::channel(dec!(0));
    let (deriv_tx, deriv_rx) = watch::channel(DerivativesSnapshot::default());

    {
        let asset = underlying.to_string();
        let health = Arc::clone(raptor_health_tx);
        spawn_supervised("us-price-raptor", move || {
            crate::raptors::price::run_price_raptor(
                asset.clone(), oracle_tx.clone(), velocity_tx.clone(), drift_tx.clone(),
                Arc::clone(&health),
            )
        });
    }
    {
        let asset = underlying.to_string();
        let http_c = Arc::clone(&http);
        let health = Arc::clone(raptor_health_tx);
        spawn_supervised("us-funding-raptor", move || {
            crate::raptors::funding::run_funding_raptor(
                Arc::clone(&http_c), asset.clone(), funding_tx.clone(), Arc::clone(&health),
            )
        });
    }
    {
        let asset = underlying.to_string();
        let http_c = Arc::clone(&http);
        let health = Arc::clone(raptor_health_tx);
        spawn_supervised("us-derivatives-raptor", move || {
            crate::raptors::derivatives::run_derivatives_raptor(
                Arc::clone(&http_c), asset.clone(), deriv_tx.clone(), Arc::clone(&health),
            )
        });
    }

    // Tide + Horizon are BTC-only macro raptors sharing one Alpaca connection.
    let (tide, horizon) = if underlying == "btc" {
        let shared_quotes = crate::raptors::tide::new_shared_quote_map();
        let (tide_tx, tide_rx) = watch::channel(TideSnapshot::default());
        {
            let oracle_rx_c = oracle_rx.clone();
            let health = Arc::clone(raptor_health_tx);
            let quotes = Arc::clone(&shared_quotes);
            spawn_supervised("us-tide-raptor", move || {
                crate::raptors::tide::run_tide_raptor(
                    oracle_rx_c.clone(), tide_tx.clone(), Arc::clone(&health), Arc::clone(&quotes),
                )
            });
        }
        let (horizon_tx, horizon_rx) = watch::channel(HorizonSnapshot::default());
        {
            let velocity_rx_c = velocity_rx.clone();
            let health = Arc::clone(raptor_health_tx);
            let quotes = Arc::clone(&shared_quotes);
            spawn_supervised("us-horizon-raptor", move || {
                crate::raptors::horizon::run_horizon_raptor(
                    Arc::clone(&quotes), velocity_rx_c.clone(), horizon_tx.clone(), Arc::clone(&health),
                )
            });
        }
        (Some(tide_rx), Some(horizon_rx))
    } else {
        (None, None)
    };

    let stack = CryptoRaptors {
        oracle: oracle_rx,
        velocity: velocity_rx,
        drift: drift_rx,
        funding: funding_rx,
        derivatives: deriv_rx,
        tide,
        horizon,
    };
    map.insert(underlying.to_string(), stack.clone());
    stack
}

/// Detect the crypto underlying of a market from token-delimited words in its
/// slug + question (`"bitcoin-up-or-down-…"` → `"btc"`). Token-based so `"eth"`
/// never matches inside `"whether"` nor `"sol"` inside `"resolution"`.
fn detect_underlying(pair: &super::markets::UsMarketPair) -> Option<&'static str> {
    let text = format!("{} {}", pair.slug, pair.question).to_ascii_lowercase();
    for token in text.split(|c: char| !c.is_ascii_alphanumeric()) {
        match token {
            "btc" | "bitcoin" => return Some("btc"),
            "eth" | "ethereum" => return Some("eth"),
            "sol" | "solana" => return Some("sol"),
            "xrp" | "ripple" => return Some("xrp"),
            _ => {}
        }
    }
    None
}

/// Whether a discovered US market belongs to the crypto domain.
///
/// Primary: the shared, data-driven market-class taxonomy (`classify_market` —
/// same rule table intl uses, matching venue category / leg-symbol tokens /
/// slug). Fallback: underlying detection from the slug + question tokens, so a
/// gateway with empty category metadata still routes correctly.
async fn pair_is_crypto(pair: &super::markets::UsMarketPair) -> bool {
    if let Some(pool) = db::pool() {
        let symbols = [pair.long.as_str(), pair.short.as_str()];
        let class = db::classify_market(pool, &pair.category, &symbols, &pair.slug).await;
        if class == "crypto" {
            return true;
        }
        if class != "unknown" {
            return false; // confidently classified as another domain
        }
    }
    detect_underlying(pair).is_some()
}

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
#[allow(clippy::too_many_arguments)]
pub async fn run_us_trader(
    venue: Arc<UsRetailVenue>,
    cag: Cag,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
    markets_tx: Arc<watch::Sender<HashMap<String, String>>>,
    process_heartbeat_secs: Arc<AtomicU64>,
    sports_rx: watch::Receiver<SportsSnapshot>,
    tennis_rx: watch::Receiver<TennisSnapshot>,
    cancel: CancellationToken,
) {
    let filter = std::env::var(ENV_MARKET_FILTER).ok().filter(|s| !s.is_empty());
    info!("🇺🇸 US trader starting — market filter={filter:?}");

    // Two concurrent wings over the same venue connection: the general wing
    // (sports/politics — original behaviour, asset "us") and the crypto wing
    // (crypto-class markets + full Raptor stack → all nine vipers, asset
    // "us-crypto"). Each has its own squadron, DB pool, and rotation loop.
    tokio::join!(
        run_wing(
            Wing::General, &venue, &cag, &raptor_health_tx, &markets_tx,
            &process_heartbeat_secs, &sports_rx, &tennis_rx, &filter, &cancel,
        ),
        run_wing(
            Wing::Crypto, &venue, &cag, &raptor_health_tx, &markets_tx,
            &process_heartbeat_secs, &sports_rx, &tennis_rx, &filter, &cancel,
        ),
    );
}

/// Run one wing's market rotation loop until `cancel` fires: select a market
/// in the wing's domain, trade it until it closes, re-discover the next one.
#[allow(clippy::too_many_arguments)]
async fn run_wing(
    wing: Wing,
    venue: &Arc<UsRetailVenue>,
    cag: &Cag,
    raptor_health_tx: &Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
    markets_tx: &Arc<watch::Sender<HashMap<String, String>>>,
    process_heartbeat_secs: &Arc<AtomicU64>,
    sports_rx: &watch::Receiver<SportsSnapshot>,
    tennis_rx: &watch::Receiver<TennisSnapshot>,
    filter: &Option<String>,
    cancel: &CancellationToken,
) {
    // ── Idle-time snapshot heartbeat ─────────────────────────────────────────
    // Market discovery can spin for hours off-hours (5-min retries), during
    // which trade_one_market — and its 30 s sync_dashboard — never runs. The
    // dashboard then serves the stale last-session snapshot (2026-08-08: a
    // June baseline showed "$120.00 ▲ $0.00" while the balance card sat empty).
    // This task writes a fresh snapshot every 60 s whenever the trade loop is
    // NOT active; the trade loop's own 30 s sync takes over when it is.
    let trading_active = Arc::new(AtomicBool::new(false));
    if let Some(snap_pool) = db::pool_for(wing.asset()) {
        let venue_bg = Arc::clone(venue);
        let active_bg = Arc::clone(&trading_active);
        let cancel_bg = cancel.clone();
        tokio::spawn(async move {
            let starting = venue_bg.collateral().await.unwrap_or(Decimal::ZERO);
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cancel_bg.cancelled() => return,
                    _ = tick.tick() => {}
                }
                if active_bg.load(AtomicOrdering::Relaxed) { continue; }
                sync_dashboard(venue_bg.as_ref(), &snap_pool, None, starting).await;
            }
        });
    }

    loop {
        if cancel.is_cancelled() {
            return;
        }

        // ── Select a tradeable market (retry until one matches or cancelled) ──
        let selection = match select_market(venue, filter, cancel, process_heartbeat_secs, wing).await {
            Some(p) => p,
            None => return, // cancelled during discovery
        };

        // Per-market cancellation — a child of `cancel`, fired on rotation so this
        // market's WS feeds drain cleanly (mirrors intl's `ws_cancel`). It also
        // completes automatically if the global `cancel` fires.
        let market_cancel = cancel.child_token();

        trading_active.store(true, AtomicOrdering::Relaxed);
        let outcome = trade_one_market(
            venue,
            cag,
            raptor_health_tx,
            markets_tx,
            process_heartbeat_secs,
            sports_rx,
            tennis_rx,
            &market_cancel,
            wing,
            selection,
        ).await;
        trading_active.store(false, AtomicOrdering::Relaxed);

        // Tear down this market's feeds before re-discovering.
        market_cancel.cancel();

        match outcome {
            MarketOutcome::Cancelled => return,
            MarketOutcome::BetterMarketFound => {
                info!("🔀 US {} wing rotation — hotter market found, switching", wing.label());
                // No pause: the new market is already live and liquid.
            }
            MarketOutcome::Closed => {
                info!("🔁 US {} wing market closed — rotating to next market", wing.label());
                // Brief pause so we don't hammer discovery the instant a market
                // resolves (its replacement may not be listed yet).
                if wait_or_cancel(cancel, DISCOVERY_RETRY_SECS).await {
                    return;
                }
            }
        }
    }
}

/// Discover markets and pick one to trade, skipping any already closed or closing
/// within [`MIN_TIME_TO_CLOSE_SECS`]. Retries until a market matches or `cancel`
/// fires. Returns `None` only on cancellation.
/// Longest time-to-close still treated as a short-cadence ("hourly") market.
/// Venue structure rather than a tuning parameter, which is why it is a constant
/// here and not a Control Tower knob — it describes how Polymarket US lists
/// crypto markets, not a preference about them. Mirrors the Kalshi trader's
/// MAX_CLOSE_SHORT_SECS so the two venues classify the same way.
const MAX_CLOSE_SHORT_SECS: i64 = 7_200; // 2h
/// Longest time-to-close considered for the secondary maker market.
const MAX_CLOSE_MAKER_SECS: i64 = 129_600; // 36h

/// The pair of markets a wing trades at once.
///
/// US previously selected a single market and then aliased `maker_market` to it,
/// so Maker and FairValue — both of which declare `venue() == "Window/Daily"` —
/// were quoting on an hourly market they were never designed for: an hour rather
/// than a day for a resting quote to fill, and every rotation eating the
/// inventory. Kalshi and the intl CLOB both split these; US now does too.
struct UsMarketSelection {
    primary: super::markets::UsMarketPair,
    /// Longer-dated market on the same underlying for passive quotes. None for
    /// the general wing, whose event markets have no hourly/daily pair, and for
    /// any underlying that happens to list only one cadence.
    maker: Option<super::markets::UsMarketPair>,
}

async fn select_market(
    venue: &Arc<UsRetailVenue>,
    filter: &Option<String>,
    cancel: &CancellationToken,
    process_heartbeat_secs: &AtomicU64,
    wing: Wing,
) -> Option<UsMarketSelection> {
    loop {
        if cancel.is_cancelled() {
            return None;
        }
        // Keep the OS watchdog satisfied while we poll for a tradeable market —
        // discovery can legitimately take many minutes (off-hours, thin slate).
        touch_heartbeat(process_heartbeat_secs);
        match wing.discover(venue).await {
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
                let mut tradeable: Vec<_> = markets.into_iter()
                    .filter(|m| match m.close_time {
                        Some(c) => (c - now).num_seconds() > MIN_TIME_TO_CLOSE_SECS,
                        None => true, // always-open market
                    })
                    .collect();

                // Partition by market domain so each wing hunts its own class:
                // the crypto wing takes crypto-class markets, the general wing
                // takes everything else (avoids both wings trading one market).
                let before_class = tradeable.len();
                let mut classed = Vec::with_capacity(tradeable.len());
                for m in tradeable.drain(..) {
                    if pair_is_crypto(&m).await == (wing == Wing::Crypto) {
                        classed.push(m);
                    }
                }
                let tradeable = classed;
                if tradeable.is_empty() && before_class > 0 {
                    info!(
                        "US {} wing: {before_class} tradeable pair(s), none in this wing's domain — retrying",
                        wing.label()
                    );
                }

                if tradeable.is_empty() {
                    warn!(
                        "US trader: {total_pairs} pair(s) found but all closed or closing within {MIN_TIME_TO_CLOSE_SECS}s — retrying"
                    );
                } else {
                    // Split by cadence the way Kalshi does: short-dated leads,
                    // longer-dated backs it for passive quotes.
                    let now = Utc::now();
                    let secs_to_close = |m: &super::markets::UsMarketPair| {
                        m.close_time.map(|c| (c - now).num_seconds())
                    };
                    let (short, long): (Vec<_>, Vec<_>) = tradeable.into_iter()
                        .partition(|m| secs_to_close(m).is_some_and(|s| s <= MAX_CLOSE_SHORT_SECS));

                    let apply_filter = |candidates: Vec<super::markets::UsMarketPair>| {
                        match filter {
                            Some(f) => {
                                let fl = f.to_lowercase();
                                candidates.into_iter().find(|m| {
                                    m.slug.to_lowercase().contains(&fl)
                                        || m.question.to_lowercase().contains(&fl)
                                })
                            }
                            None => candidates.into_iter().next(),
                        }
                    };

                    // Prefer a short-dated primary, but fall back to the longer
                    // set rather than idling — that is what this did before the
                    // split, and an always-open market has no close time at all.
                    let selected = apply_filter(short).or_else(|| apply_filter(long.clone()));

                    if let Some(m) = selected {
                        // Only the crypto wing has an hourly/daily pair to match
                        // on; general-wing event markets do not, and pairing two
                        // unrelated events would be worse than none.
                        let maker = if wing == Wing::Crypto {
                            let underlying = detect_underlying(&m);
                            long.into_iter().find(|c| {
                                c.slug != m.slug
                                    && detect_underlying(c) == underlying
                                    && secs_to_close(c).is_some_and(|s| s <= MAX_CLOSE_MAKER_SECS)
                                    && secs_to_close(c) > secs_to_close(&m)
                            })
                        } else {
                            None
                        };

                        info!(
                            "🎯 US primary: \"{}\" [YES={} / NO={}] close={:?}",
                            m.question, m.long, m.short, m.close_time
                        );
                        match &maker {
                            Some(mk) => info!(
                                "🎯 US maker:   \"{}\" close={:?}",
                                mk.question, mk.close_time,
                            ),
                            // Worth saying out loud: Maker and FairValue fall back
                            // to the primary, which is not what they are tuned for.
                            None if wing == Wing::Crypto => info!(
                                "🎯 US maker:   none found for this underlying — passive quotes will use the primary"
                            ),
                            None => {}
                        }
                        return Some(UsMarketSelection { primary: m, maker });
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
    tennis_rx: &watch::Receiver<TennisSnapshot>,
    cancel: &CancellationToken,
    wing: Wing,
    selection: UsMarketSelection,
) -> MarketOutcome {
    let asset = wing.asset();
    let pair = selection.primary;
    let maker_pair = selection.maker;

    // ── Crypto wing: Raptor intelligence + strike price ──────────────────────
    // Spawn (or reuse) the live Raptor stack for the market's underlying and
    // parse the strike from the question (fallback: historical price lookup at
    // the timestamp in the description — same chain intl uses). These feed the
    // oracle/velocity/funding/derivatives snapshot fields the intelligence-
    // driven vipers (Momentum, GBoost, Basis, FairValue, …) gate on.
    let mut detected_underlying: Option<String> = None;
    let (raptors, strike_price) = if wing == Wing::Crypto {
        let underlying = detect_underlying(&pair).unwrap_or("btc");
        detected_underlying = Some(underlying.to_string());
        let stack = raptor_stack_for(underlying, raptor_health_tx);
        let mut strike = extract_strike_price(&pair.question);
        if strike.is_none() {
            let http = reqwest::Client::new();
            let sym = underlying.to_uppercase();
            strike = fetch_historical_strike_price(&http, &sym, &pair.description).await;
            if strike.is_none() {
                strike = fetch_historical_strike_price(&http, &sym, &pair.question).await;
            }
        }
        info!(
            "🧠 US crypto wing: underlying={underlying} strike={strike:?} for \"{}\"",
            pair.question
        );
        (Some(stack), strike)
    } else {
        (None, None)
    };

    // ── Register a squadron with the CAG so the Control Tower lists it ────────
    // The US venue runs a standalone arb loop (no intl-style patrol), but the
    // dashboard reads squadrons from the CAG registry — so without this the UI
    // shows zero squadrons even though the venue is live.
    let squadron = register_us_squadron(cag, &pair, sports_rx.clone(), tennis_rx.clone(), wing, raptors.as_ref(), strike_price);
    let squadron_id = squadron.id.clone();

    // Seed the squadron's Viper config so the detail view's strategy cards render.
    seed_squadron_config(&squadron_id).await;

    // Classify the market's domain and link it to its eligible raptors/vipers via
    // the shared, venue-neutral taxonomy (same path intl uses).
    let market_class = squadron.classify_and_link().await;
    // Filing dimensions for this market's rows. The general wing hunts sports
    // and politics, which have no underlying instrument at all — `None` here is
    // the correct value, not a missing one.
    let scope = TradeScope::new(
        asset,
        US_VENUE,
        Some(market_class.clone()),
        detected_underlying.clone(),
    );

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
        strike_price,
        is_neg_risk: false,
        condition_id: String::new(),
        yes_fee_bps: 0,
        no_fee_bps: 0,
    };
    // Secondary market the Window/Daily vipers quote on. Its own close time
    // matters: it is typically hours later than the primary's, which is the
    // whole reason a resting quote has a chance of filling there.
    let maker_cfg = maker_pair.as_ref().map(|mk| MarketConfig {
        yes_token: mk.long.clone(),
        no_token: mk.short.clone(),
        market_name: mk.question.clone(),
        market_close_time: mk.close_time,
        strike_price,
        is_neg_risk: false,
        condition_id: String::new(),
        yes_fee_bps: 0,
        no_fee_bps: 0,
    });
    let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
    // Shared, venue-neutral order lifecycle engine (Option C). Drives fill-confirm,
    // stale-cancel, and naked-leg flatten off the `Execution` trait surface.
    let lifecycle = Arc::new(OrderLifecycle::new(LifecycleConfig::us()));
    // Upgrade fill confirmation to event-precise via the venue's private
    // account feed (`/v1/ws/private` → `subscribe_fills`); reconcile polling
    // remains the cancel/flatten path and the fallback backstop.
    let _fill_listener = lifecycle.spawn_fill_listener(Arc::clone(&venue), Arc::clone(&positions));
    let market_started_at = Utc::now();

    // Publish Raptor telemetry + active market so the squadron detail panels
    // populate (both feed `/api/status`).
    publish_us_raptor_health(raptor_health_tx, asset, true);
    publish_us_strategy_market(markets_tx, &viper_kinds, &pair.question);

    // ── Stream both legs' order books (tied to the per-market cancel token) ───
    let ws_url = venue.markets_ws_url();
    let ws_auth = venue.ws_auth();
    let default_feed: PriceState = (dec!(0), dec!(0), dec!(1), dec!(0), Utc::now(), dec!(0), dec!(0));
    let (long_tx, long_rx) = watch::channel(default_feed);
    let (short_tx, short_rx) = watch::channel(default_feed);
    ws::spawn_market_feed(ws_url.clone(), pair.long.as_str().to_string(), ws_auth.clone(), long_tx, cancel.clone());
    ws::spawn_market_feed(ws_url.clone(), pair.short.as_str().to_string(), ws_auth.clone(), short_tx, cancel.clone());

    // The secondary market needs its own book: Maker and FairValue quote on it,
    // and a snapshot built from the primary's feed would price the wrong market.
    let maker_feeds = maker_pair.as_ref().map(|mk| {
        let (mlong_tx, mlong_rx) = watch::channel(default_feed);
        let (mshort_tx, mshort_rx) = watch::channel(default_feed);
        ws::spawn_market_feed(ws_url.clone(), mk.long.as_str().to_string(), ws_auth.clone(), mlong_tx, cancel.clone());
        ws::spawn_market_feed(ws_url.clone(), mk.short.as_str().to_string(), ws_auth.clone(), mshort_tx, cancel.clone());
        (mlong_rx, mshort_rx)
    });

    // ── Dashboard + strategy-context state ───────────────────────────────────
    let pool = db::pool_for(asset);
    let starting = venue.collateral().await.unwrap_or(Decimal::ZERO);
    let mut available_collateral = starting;
    let mut session_pnl = Decimal::ZERO;
    let mut dyn_cfg = DynamicConfig::load_for_squadron(&squadron_id).await;
    if let Some(p) = &pool {
        let (coll, total) = sync_dashboard(venue.as_ref(), p, Some(&positions), starting).await;
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
                publish_us_raptor_health(raptor_health_tx, asset, false);
                return MarketOutcome::Cancelled;
            }
            _ = dash_tick.tick() => {
                if let Some(p) = &pool {
                    let (coll, total) = sync_dashboard(venue.as_ref(), p, Some(&positions), starting).await;
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
                match wing.discover(venue).await {
                    Ok(mut candidates) if !candidates.is_empty() => {
                        // Best market by volume, excluding the one we're already
                        // on and anything outside this wing's domain.
                        candidates.retain(|m| m.slug != pair.slug);
                        let mut domain_candidates = Vec::with_capacity(candidates.len());
                        for m in candidates.drain(..) {
                            if pair_is_crypto(&m).await == (wing == Wing::Crypto) {
                                domain_candidates.push(m);
                            }
                        }
                        let candidates = domain_candidates;
                        if let Some(best) = candidates.iter().max_by(|a, b| a.volume.partial_cmp(&b.volume).unwrap_or(std::cmp::Ordering::Equal)) {
                            if best.volume > pair.volume + ROTATION_VOLUME_THRESHOLD {
                                info!(
                                    "🔍 Hotter market found: \"{}\" vol={:.0} > current \"{}\" vol={:.0} + threshold {:.0} — rotating",
                                    best.question, best.volume, pair.question, pair.volume, ROTATION_VOLUME_THRESHOLD
                                );
                                lifecycle.cancel_all(venue.as_ref()).await;
                                cag.update_state(&squadron_id, SquadronState::StoodDown);
                                publish_us_raptor_health(raptor_health_tx, asset, false);
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
                    let scope_t = scope.clone();
                    tokio::spawn(async move {
                        metrics::record_trade(
                            &scope_t,
                            Decimal::ZERO,
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
                // cancel_all cancels RESTING ORDERS; it does not sell inventory.
                // Now that this wing trades a secondary market whose close is
                // hours later than the primary's, tearing down here would leave
                // any position on it unmanaged — no stop, no take-profit — until
                // it expired. That is the shape of the -$3.09 loss on Kalshi
                // (2026-08-10); giving US the same market pair would have given
                // it the same bug.
                lifecycle.cancel_all(venue.as_ref()).await;
                flatten_before_stand_down(
                    venue, &pool, &scope, &positions, &lifecycle,
                    &market_cfg, maker_cfg.as_ref(), maker_feeds.as_ref(),
                    raptors.as_ref(), starting, dyn_cfg.ghost_mode,
                ).await;
                cag.update_state(&squadron_id, SquadronState::StoodDown);
                publish_us_raptor_health(raptor_health_tx, asset, false);
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
                // Deliberately NOT `continue`. This skipped the whole tick, so
                // the RTB window stopped exits as well as entries: for its full
                // duration a position could not be stopped out or taken off, and
                // simply ran into the close. Fall through; entries are suppressed
                // at dispatch by `opens_exposure`.
            }
            MarketPhase::Open => {}
        }

        if strategies.is_empty() || Instant::now() < cooldown_until {
            continue;
        }

        // Build a venue-neutral snapshot from both legs' live books. The crypto
        // wing enriches it with live Raptor intelligence (oracle, velocity,
        // funding, derivatives, macro) so the full viper suite can gate; the
        // general wing's raptor fields stay zero — its order-book vipers
        // (arbitrage/maker) don't read them.
        let snapshot = build_snapshot(&long_rx, &short_rx, raptors.as_ref(), &market_cfg);

        // Maker snapshot from the secondary market's own feed. Falls back to the
        // primary when there is no secondary, or when its book has not arrived
        // yet — the same fallback Kalshi uses, so a viper always has something
        // priced rather than a zeroed book that reads as maximally adverse.
        let (mk_market, mk_snapshot) = match (&maker_cfg, &maker_feeds) {
            (Some(mcfg), Some((ml, ms))) => {
                let snap = build_snapshot(ml, ms, raptors.as_ref(), mcfg);
                if snap.yes_ask.is_zero() && snap.no_ask.is_zero() {
                    (Some(market_cfg.clone()), Some(snapshot.clone()))
                } else {
                    (Some(mcfg.clone()), Some(snap))
                }
            }
            _ => (Some(market_cfg.clone()), Some(snapshot.clone())),
        };

        let ctx = StrategyContext {
            market: market_cfg.clone(),
            snapshot: snapshot.clone(),
            positions: positions.clone(),
            session_pnl,
            starting_collateral: starting,
            crypto_filter: asset.to_uppercase(),
            market_started_at,
            // The US venue has no hourly/daily split — the single discovered
            // market IS the venue. Arbitrage (venue = "Window/Daily") refuses
            // to run without a maker market (intl orphan-loss guard), so feed
            // it the same market/snapshot rather than None, which left it
            // permanently idle ("no daily/window venue available", 2026-08-08).
            maker_market: mk_market,
            maker_snapshot: mk_snapshot,
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
            // Inside the RTB window we manage what we hold but take on nothing
            // new: cancels and exits still flow, entries and quotes do not.
            if winding_down && signal.opens_exposure() {
                continue;
            }
            if dispatch_signal(venue.as_ref(), &pool, &positions, &lifecycle, &scope, &strategy_name, &signal, starting).await {
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
        "ConvergenceStrategy"  => "convergence",
        "FairValueStrategy"    => "fairvalue",
        "TrendReversalStrategy" => "trendcapture",
        "TrendCaptureStrategy" => "trendcapture", // legacy alias (pre-rename positions)
        _ => "",
    }
}

#[cfg(test)]
mod market_pairing_tests {
    use super::{MAX_CLOSE_SHORT_SECS, MAX_CLOSE_MAKER_SECS};

    /// The two cadences must not overlap or the same market could be picked as
    /// both primary and maker, which is the self-reference this change exists to
    /// remove.
    #[test]
    fn the_cadence_bands_are_ordered_and_disjoint() {
        assert!(MAX_CLOSE_SHORT_SECS < MAX_CLOSE_MAKER_SECS);
    }

    /// A short-cadence market is roughly an hour out and a maker market roughly a
    /// day: the point of the split is that a resting quote gets meaningfully more
    /// time to fill on the second than the first.
    #[test]
    fn a_maker_market_lives_far_longer_than_a_primary() {
        assert!(MAX_CLOSE_SHORT_SECS >= 3_600, "an hourly market must fit inside the short band");
        assert!(
            MAX_CLOSE_MAKER_SECS >= MAX_CLOSE_SHORT_SECS * 12,
            "the maker band must be worth splitting out",
        );
    }

    /// Matches the Kalshi trader's bands, so the two venues classify the same
    /// market shape the same way — the consistency this change is for.
    #[test]
    fn the_bands_match_kalshi() {
        assert_eq!(MAX_CLOSE_SHORT_SECS, 7_200);
        assert_eq!(MAX_CLOSE_MAKER_SECS, 129_600);
    }
}

/// Sell any inventory still tradeable before the wing stands down.
///
/// The primary has closed and anything held on it can only settle, which is what
/// a binary is meant to do. Inventory on the SECONDARY market is the problem:
/// its close is typically hours later, this wing was its only manager, and
/// walking away leaves it with no stop and no take-profit until expiry.
///
/// Exits are synthesised at the live bid for the leg actually held — the same
/// FAK-at-the-bid path a stop takes. A leg whose book has emptied is left to
/// settle and logged, since selling into an empty book is worse than holding to
/// resolution.
#[allow(clippy::too_many_arguments)]
async fn flatten_before_stand_down(
    venue: &Arc<UsRetailVenue>,
    pool: &Option<sqlx::SqlitePool>,
    scope: &TradeScope,
    positions: &Arc<Mutex<PositionMap>>,
    lifecycle: &Arc<OrderLifecycle>,
    primary_cfg: &MarketConfig,
    maker_cfg: Option<&MarketConfig>,
    maker_feeds: Option<&(watch::Receiver<PriceState>, watch::Receiver<PriceState>)>,
    raptors: Option<&CryptoRaptors>,
    starting: Decimal,
    ghost: bool,
) {
    let held: Vec<(String, MarketId, Decimal)> = {
        let map = positions.lock().await;
        map.iter()
            .filter(|(_, p)| p.shares > dec!(0))
            .map(|((strategy, token), p)| (strategy.clone(), token.clone(), p.shares))
            .collect()
    };
    if held.is_empty() {
        return;
    }

    let (Some(mcfg), Some((mlong, mshort))) = (maker_cfg, maker_feeds) else {
        warn!(
            "🏁 Standing down holding {} position(s) with no open secondary market — leaving them to settle",
            held.len(),
        );
        return;
    };
    let snap = build_snapshot(mlong, mshort, raptors, mcfg);

    for (strategy, token, shares) in held {
        let bid = if token == mcfg.yes_token {
            snap.yes_bid
        } else if token == mcfg.no_token {
            snap.no_bid
        } else {
            info!(
                "🏁 [{strategy}] {shares} shares on \"{}\" are on the closed primary — leaving to settle",
                primary_cfg.market_name,
            );
            continue;
        };
        if bid <= dec!(0) {
            warn!(
                "🏁 [{strategy}] {shares} shares on \"{}\" have no bid — leaving to settle rather than dumping into an empty book",
                mcfg.market_name,
            );
            continue;
        }
        warn!(
            "🏁 [{strategy}] flattening {shares} shares on \"{}\" at ${bid:.2} before stand-down",
            mcfg.market_name,
        );
        let signal = StrategySignal::Exit {
            params: OrderParams {
                token_id: token.clone(),
                price: bid,
                shares,
                fee_bps: 0,
                is_neg_risk: false,
                market_name: mcfg.market_name.clone(),
                condition_id: String::new(),
                order_type: crate::venues::core::TimeInForce::Fak,
                post_only: false,
                ghost_mode: ghost,
            },
            reason: "SquadronStandDown".to_string(),
            exit_pair: false,
        };
        dispatch_signal(venue.as_ref(), pool, positions, lifecycle, scope, &strategy, &signal, starting).await;
    }
}

/// Build a venue-neutral [`MarketSnapshot`] from the two US leg feeds, enriched
/// with live Raptor intelligence when the wing has a stack attached.
/// `PriceState` layout: `(best_bid, bid_touch, best_ask, ask_touch, ts,
/// bid_depth_total, ask_depth_total)` — see `state::price_state`.
fn build_snapshot(
    long_rx: &watch::Receiver<PriceState>,
    short_rx: &watch::Receiver<PriceState>,
    raptors: Option<&CryptoRaptors>,
    market: &MarketConfig,
) -> MarketSnapshot {
    let yes_state = *long_rx.borrow();
    let no_state  = *short_rx.borrow();
    let (yb, ybd, ya, yad) = (yes_state.0, yes_state.1, yes_state.2, yes_state.3);
    let (nb, nbd, na, nad) = (no_state.0,  no_state.1,  no_state.2,  no_state.3);
    let now = Utc::now();
    let mut snap = MarketSnapshot {
        yes_bid: yb, yes_bid_depth: ybd, yes_ask: ya, yes_ask_depth: yad,
        no_bid:  nb, no_bid_depth:  nbd, no_ask:  na, no_ask_depth:  nad,
        yes_bid_depth_total: yes_state.5, yes_ask_depth_total: yes_state.6,
        no_bid_depth_total:  no_state.5,  no_ask_depth_total:  no_state.6,
        oracle_price: dec!(0), velocity: dec!(0), velocity_1s: dec!(0), acceleration: dec!(0),
        funding_rate: dec!(0), oracle_drift_60m: dec!(0), oracle_drift_10m: dec!(0),
        hist_vol: dec!(0),
        institutional_pulse: dec!(0), tide_coherence: dec!(0),
        tradfi_velocity: dec!(0), macro_coherence: dec!(0),
        vix_proxy: dec!(0), vix_velocity: dec!(0),
        oi_delta_pct: dec!(0), cvd_ratio: dec!(0),
        secs_to_expiry: market
            .market_close_time
            .map(|c| (c - now).num_seconds().max(0))
            .unwrap_or(0),
        timestamp: now,
    };
    // Same channel → field mapping the intl patrol uses (patrol_impl.rs).
    if let Some(r) = raptors {
        snap.oracle_price = *r.oracle.borrow();
        let (vel, vel_1s, accel) = *r.velocity.borrow();
        snap.velocity = vel;
        snap.velocity_1s = vel_1s;
        snap.acceleration = accel;
        let (drift_60m, drift_10m, hist_vol) = *r.drift.borrow();
        snap.oracle_drift_60m = drift_60m;
        snap.oracle_drift_10m = drift_10m;
        snap.hist_vol = hist_vol;
        snap.funding_rate = *r.funding.borrow();
        let deriv = r.derivatives.borrow().clone();
        snap.oi_delta_pct = deriv.oi_delta_pct;
        snap.cvd_ratio = deriv.cvd_ratio;
        if let Some(tide) = &r.tide {
            let t = tide.borrow().clone();
            snap.institutional_pulse = t.institutional_pulse;
            snap.tide_coherence = t.coherence;
        }
        if let Some(horizon) = &r.horizon {
            let h = horizon.borrow().clone();
            snap.tradfi_velocity = h.tradfi_velocity;
            snap.macro_coherence = h.macro_coherence;
            snap.vix_proxy = h.vix_proxy;
            snap.vix_velocity = h.vix_velocity;
        }
    }
    crate::state::price_state::log_heartbeat(
        &market.market_name, &yes_state, &no_state, snap.oracle_price,
    );
    snap
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
/// Record the position guard from what the venue actually filled.
///
/// Takes the `Fill` rather than the `OrderParams`: booking the REQUESTED size at
/// the LIMIT price left the bot believing it held more than it did, at a cost
/// basis it never paid, and every downstream exposure check and stop distance
/// then worked from that number. Exits were already clamped to the guard, which
/// is why the error stayed confined to the entry side and went unnoticed.
async fn record_guard(
    positions: &Arc<Mutex<PositionMap>>,
    strategy_name: &str,
    params: &OrderParams,
    paired: Option<&MarketId>,
    fill: &Fill,
) {
    let mut map = positions.lock().await;
    map.insert(
        (strategy_name.to_string(), params.token_id.clone()),
        Position {
            shares: fill.filled,
            avg_entry: fill.price,
            opened_at: Utc::now(),
            close_time: None,
            market_name: params.market_name.clone(),
            pair_token_id: params.token_id.clone(),
            fill_confirmed_at: None,
            paired_leg_token_id: paired.cloned(), entry_fee: fill.fee,
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
    scope: &TradeScope,
    strategy_name: &str,
    signal: &StrategySignal,
    starting: Decimal,
) -> bool {
    match signal {
        StrategySignal::Entry { params, pair_params: Some(pp) } => {
            if params.ghost_mode {
                info!("👻 [{strategy_name}] ghost entry pair: {} + {}", params.token_id, pp.token_id);
                let ghost_of = |p: &OrderParams| Fill {
                    order_id: OrderId(String::new()),
                    market: p.token_id.clone(),
                    filled: p.shares,
                    price: p.price,
                    fee: Decimal::ZERO,
                };
                let (ga, gb) = (ghost_of(params), ghost_of(pp));
                record_guard(positions, strategy_name, params, Some(&pp.token_id), &ga).await;
                record_guard(positions, strategy_name, pp, Some(&params.token_id), &gb).await;
                record_entry(pool, scope, strategy_name, params, &ga).await;
                record_entry(pool, scope, strategy_name, pp, &gb).await;
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
                    record_guard(positions, strategy_name, params, Some(&pp.token_id), &a).await;
                    record_guard(positions, strategy_name, pp, Some(&params.token_id), &b).await;
                    // Track both resting legs so the reconciler manages their
                    // lifecycle (fill-confirm / stale-cancel / naked-leg hedge).
                    lifecycle.track(&a, strategy_name, params.order_type, Some(pp.token_id.clone())).await;
                    lifecycle.track(&b, strategy_name, pp.order_type, Some(params.token_id.clone())).await;
                    record_entry(pool, scope, strategy_name, params, &a).await;
                    record_entry(pool, scope, strategy_name, pp, &b).await;
                    if let Some(p) = pool { sync_dashboard(venue, p, Some(positions), starting).await; }
                    true
                }
                Err(e) => { warn!("[{strategy_name}] atomic entry failed: {e}"); false }
            }
        }
        StrategySignal::Entry { params, pair_params: None } => {
            dispatch_single(venue, pool, positions, lifecycle, scope, strategy_name, params, Side::Buy, starting).await
        }
        StrategySignal::MakerQuote { yes, no } => {
            let mut acted = false;
            for q in [yes.as_ref(), no.as_ref()].into_iter().flatten() {
                if dispatch_single(venue, pool, positions, lifecycle, scope, strategy_name, q, Side::Buy, starting).await {
                    acted = true;
                }
            }
            acted
        }
        StrategySignal::MakerCancel { tokens } => {
            // Reactive quote-pull: cancel resting UNFILLED maker orders on these
            // tokens (book turned toxic before fill). Cancel via the venue's
            // open-orders surface, then drop the strategy's phantom guard.
            let mut acted = false;
            let open = venue.open_orders().await.unwrap_or_default();
            for tok in tokens {
                for ord in open.iter().filter(|o| &o.market == tok) {
                    if let Err(e) = venue.cancel(ord.order_id.clone()).await {
                        warn!("[{strategy_name}] maker quote-pull cancel failed for {} ({}): {e}", ord.order_id, tok);
                    } else {
                        info!("🚫 [{strategy_name}] maker quote-pulled: {} — resting order cancelled (toxic book)", tok);
                        acted = true;
                    }
                }
                positions.lock().await.remove(&(strategy_name.to_string(), tok.clone()));
            }
            acted
        }
        // The resting maker exit (post-only ask against a filled position) is
        // implemented only on the intl CLOB patrol loop, which owns the order
        // record, reprice, cancel-before-stop and fill-detection machinery it
        // requires. Ignoring it here is safe and lossless: the maker simply keeps
        // its historical bid-based take-profit, which is what this venue did
        // before the signal existed. `MakerStrategy` re-emits it every tick, so
        // there is nothing to queue or replay if this venue later implements it.
        StrategySignal::MakerRestingExit { .. } => false,
        StrategySignal::Exit { params, reason, exit_pair } => {
            info!("🚪 [{strategy_name}] exit ({reason}): {} @ {:.4}", params.token_id, params.price);
            // Snapshot the entry BEFORE the sell so the round-trip can be booked
            // to the tradelog — the guard is cleared below and there is no second
            // chance to read its cost basis.
            let entered = positions
                .lock()
                .await
                .get(&(strategy_name.to_string(), params.token_id.clone()))
                .map(|p| (p.avg_entry, p.shares));
            let acted = dispatch_single(venue, pool, positions, lifecycle, scope, strategy_name, params, Side::Sell, starting).await;
            if acted {
                record_round_trip(pool, scope, strategy_name, params, entered, reason).await;
            }
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
    scope: &TradeScope,
    strategy_name: &str,
    params: &OrderParams,
    side: Side,
    starting: Decimal,
) -> bool {
    if params.ghost_mode {
        info!("👻 [{strategy_name}] ghost {side:?}: {} @ {:.4} × {:.2}",
            params.token_id, params.price, params.shares);
        let ghost = Fill {
            order_id: OrderId(String::new()),
            market: params.token_id.clone(),
            filled: params.shares,
            price: params.price,
            fee: Decimal::ZERO,
        };
        if matches!(side, Side::Buy) {
            record_guard(positions, strategy_name, params, None, &ghost).await;
            record_entry(pool, scope, strategy_name, params, &ghost).await;
        }
        return true;
    }
    match venue.place_order(order_params_to_intent(params, side)).await {
        Ok(f) => {
            // A 2xx is not a fill. Kalshi grew this guard on 2026-08-10 after
            // booking `fill_count: 0` as done — phantom positions on entry,
            // abandoned live ones on exit — but US never got it.
            if f.filled <= Decimal::ZERO {
                warn!("⚠️ [{strategy_name}] {side:?} {} @ {:.4} filled 0 of {:.2} — no state change (order {})",
                    params.token_id, params.price, params.shares, f.order_id);
                return false;
            }
            if f.filled < params.shares.round_dp(2) {
                warn!("⚠️ [{strategy_name}] {side:?} {} partial fill {:.2} of {:.2} — booking what filled; \
                       lifecycle reconciliation owns the remainder",
                    params.token_id, f.filled, params.shares);
            }
            info!("✅ [{strategy_name}] {side:?} {} @ {:.4} × {:.2} (order {})",
                params.token_id, f.price, f.filled, f.order_id);
            if matches!(side, Side::Buy) {
                record_guard(positions, strategy_name, params, None, &f).await;
                lifecycle.track(&f, strategy_name, params.order_type, None).await;
                record_entry(pool, scope, strategy_name, params, &f).await;
            }
            if let Some(p) = pool { sync_dashboard(venue, p, Some(positions), starting).await; }
            true
        }
        Err(e) => { warn!("[{strategy_name}] {side:?} order failed: {e}"); false }
    }
}

/// Persist a new position to `entries` + `open_positions`.
///
/// Without this the Control Tower's positions panel had to fall back on
/// [`sync_dashboard`]'s venue sweep, which knows the instrument but not which
/// viper owns it — every live position was mislabelled as ArbitrageStrategy.
async fn record_entry(
    pool: &Option<sqlx::SqlitePool>,
    scope: &TradeScope,
    strategy_name: &str,
    params: &OrderParams,
    fill: &Fill,
) {
    let side = side_label(params.token_id.as_str());
    metrics::record_entry(
        scope,
        strategy_name.to_string(),
        params.token_id.to_string(),
        params.market_name.clone(),
        side.to_string(),
        fill.price,
        fill.filled,
    ).await;
    if let Some(p) = pool {
        // Live entries land as `pending` — the fill is not venue-confirmed yet, and
        // `purge_stale_open_positions` protects pending rows through its grace
        // window, so the row survives until `sync_dashboard` sees the holding and
        // promotes it. Ghost entries have no venue truth to wait for.
        let status = if params.ghost_mode { "confirmed" } else { "pending" };
        db::record_open_position_with_status(
            p, strategy_name, params.token_id.as_str(), &params.market_name,
            side, fill.price, fill.filled, params.ghost_mode, status,
        ).await;
    }
}

/// Book a completed round-trip to the `trades` ledger and clear its
/// `open_positions` row.
///
/// The US loop previously recorded a trade only on a lifecycle flatten, so every
/// ordinary TP/SL/bail exit moved real cash and left no ledger row — the same
/// gap found on the Kalshi loop on 2026-08-10, where a FairValue round-trip
/// realised −$1.24 against an empty `trades` table.
///
/// `entered` is the (avg_entry, shares) read before the exit order was placed;
/// `None` means no guard existed for this token, in which case there is no
/// verifiable cost basis and we log loudly rather than invent P&L.
async fn record_round_trip(
    pool: &Option<sqlx::SqlitePool>,
    scope: &TradeScope,
    strategy_name: &str,
    params: &OrderParams,
    entered: Option<(Decimal, Decimal)>,
    reason: &str,
) {
    let Some((avg_entry, shares)) = entered else {
        warn!("⚠️ [{strategy_name}] exit booked for {} with no tracked entry — no trade recorded",
            params.token_id);
        return;
    };
    // Exit sizing follows the guard, not the signal: a partially-filled entry
    // must not book P&L on shares we never owned.
    let shares = shares.min(params.shares).max(Decimal::ZERO);
    let pnl = (params.price - avg_entry) * shares;
    metrics::record_trade(
        scope,
        // Polymarket fees are taken from proceeds and not reported per fill, so no fee is booked on this venue.
        Decimal::ZERO,
        strategy_name.to_string(),
        params.market_name.clone(),
        side_label(params.token_id.as_str()).to_string(),
        avg_entry,
        params.price,
        shares,
        pnl,
        reason.to_string(),
    ).await;
    if let Some(p) = pool {
        db::close_open_position(p, strategy_name, params.token_id.as_str()).await;
    }
}

/// Reconcile the Control Tower's view of the US venue: upsert live open positions,
/// purge settled ones, and write a portfolio P&L snapshot. Returns
/// `(collateral, total_value)` so the tick loop can feed the strategy context.
/// `guards` is the trade loop's live position map when one is running; the idle
/// heartbeat passes `None` (no viper owns anything while the loop is parked).
async fn sync_dashboard(
    venue: &UsRetailVenue,
    pool: &sqlx::SqlitePool,
    guards: Option<&Arc<Mutex<PositionMap>>>,
    starting: Decimal,
) -> (Decimal, Decimal) {
    let collateral = match venue.collateral().await {
        Ok(c) => c,
        // On a transient collateral read failure, return zero available collateral
        // (which safely gates strategies off) without writing a P&L snapshot.
        Err(e) => { warn!("US dashboard sync: collateral query failed: {e}"); return (Decimal::ZERO, starting); }
    };
    let positions = venue.positions().await.unwrap_or_default();

    // token → (owning viper, market name) from the live guard map, so a venue
    // holding is attributed to the viper that opened it. Holdings with no guard
    // (prior session, manual trade) are adopted under a neutral label rather
    // than being blamed on a viper that never traded them.
    let owners: HashMap<String, (String, String)> = match guards {
        Some(g) => g.lock().await.iter()
            .map(|((s, t), p)| (t.as_str().to_string(), (s.clone(), p.market_name.clone())))
            .collect(),
        None => HashMap::new(),
    };

    let mut live_ids = std::collections::HashSet::new();
    let mut positions_value = Decimal::ZERO;
    for p in &positions {
        let sym = p.market.as_str();
        live_ids.insert(sym.to_string());
        match owners.get(sym) {
            // The viper's own entry row already exists — the venue confirming the
            // holding is what promotes it out of `pending`.
            Some((strategy, market_name)) => {
                db::record_open_position(
                    pool, strategy, sym, market_name, side_label(sym), p.avg_price, p.shares, false,
                ).await;
                db::confirm_position_status(pool, strategy, sym).await;
            }
            None => {
                db::record_open_position(
                    pool, "ChainAdopted", sym, sym, side_label(sym), p.avg_price, p.shares, false,
                ).await;
            }
        }
        positions_value += p.shares * p.avg_price;
    }
    // Drop rows for positions the venue no longer reports (settled to cash).
    let _ = db::purge_stale_open_positions(pool, &live_ids, &std::collections::HashMap::new()).await;

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


/// Assemble and register a single squadron for the selected US market so it
/// appears in the Control Tower's CAG squadron list.
///
/// The general wing gets placeholder signal channels (its arb loop reads prices
/// from the WS feed, not from Raptors); the crypto wing attaches the live
/// Raptor stack so the squadron detail view and downstream consumers see the
/// real intelligence feeds.
fn register_us_squadron(
    cag: &Cag,
    pair: &super::markets::UsMarketPair,
    sports_rx: watch::Receiver<SportsSnapshot>,
    tennis_rx: watch::Receiver<TennisSnapshot>,
    wing: Wing,
    crypto_raptors: Option<&CryptoRaptors>,
    strike_price: Option<Decimal>,
) -> Squadron {
    let raptors = match crypto_raptors {
        Some(r) => {
            let mut r2 = SquadronRaptors::full(
                r.oracle.clone(),
                r.velocity.clone(),
                r.drift.clone(),
                r.funding.clone(),
                r.derivatives.clone(),
                r.tide.clone(),
                r.horizon.clone(),
                Some(sports_rx),
            );
            // The venue-neutral Tennis Raptor rides along observe-only, same
            // post-construction attach as the general wing below.
            r2.tennis = Some(tennis_rx);
            r2
        }
        None => {
            // Placeholder signal channels (the general wing reads prices from
            // the WS feed). Receivers stay valid after the senders drop.
            let (_, oracle_rx) = watch::channel(Decimal::ZERO);
            let (_, velocity_rx) = watch::channel((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
            let (_, drift_rx) = watch::channel((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
            // The venue-neutral Sports Raptor IS a real feed on the US build —
            // attach it so its observe-only line-movement signal is available.
            let mut r = SquadronRaptors::price_only(oracle_rx, velocity_rx, drift_rx);
            r.sports = Some(sports_rx);
            r.tennis = Some(tennis_rx);
            r
        }
    };

    let market = MarketConfig {
        yes_token: pair.long.clone(),
        no_token: pair.short.clone(),
        market_name: pair.question.clone(),
        // Deliberately None: the squadron id derives from `{asset}-{cadence}`
        // and is the persistence key for operator config — a real close_time
        // would flip "us-open" → "us-hourly" per market and orphan saved
        // config. The trading loop's own market_cfg carries the real close.
        market_close_time: None,
        strike_price,
        is_neg_risk: false,
        condition_id: String::new(),
        yes_fee_bps: 0,
        no_fee_bps: 0,
    };

    let name = match wing {
        Wing::General => "US Retail Arb",
        Wing::Crypto => "US Crypto Squadron",
    };
    let squadron = Squadron::new(
        CryptoAsset::Custom(wing.asset().to_uppercase()),
        SquadronConfig::arb_wing(name),
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
/// Keyed by the wing's asset slug so the squadron detail panel finds it. The US
/// order-book WS feed is the price source for both wings (the crypto wing's
/// real raptors additionally publish under their own underlying key, e.g.
/// "btc"); there is no separate funding raptor for the general wing, so both
/// flags track the same `connected` state.
fn publish_us_raptor_health(
    tx: &watch::Sender<HashMap<String, AssetRaptorHealth>>,
    asset: &str,
    connected: bool,
) {
    tx.send_modify(|map| {
        let h = map.entry(asset.to_string()).or_default();
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

