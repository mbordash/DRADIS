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
use tracing::{debug, error, info, warn};

use crate::api::server::AssetRaptorHealth;
use crate::cag::Cag;
use crate::helpers::db;
use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::time::{extract_strike_price, fetch_historical_strike_price};
use crate::helpers::metrics;
use crate::orchestrator::{
    aggregate_and_resolve_signals, evaluate_strategies, StrategyContext,
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
    StrategySignal, PositionKey};
use crate::venues::core::{Execution, MarketId, OrderIntent, Side, Fill, OrderId};
use crate::venues::lifecycle::{LifecycleConfig, OrderLifecycle};

use super::{ws, UsRetailVenue};

/// How long to wait between attempts to read the session's starting collateral.
const COLLATERAL_RETRY_SECS: u64 = 15;

/// Optional substring filter (matched against slug / question) to pick a market.
const ENV_MARKET_FILTER: &str = "POLYMARKET_US_MARKET_FILTER";

const TICK_MS: u64 = 500;
/// Pause after any order placement so the loop doesn't spam a fleeting book.
const ACTION_COOLDOWN_SECS: u64 = 30;
/// Retry cadence while waiting for a tradeable market to appear.
const DISCOVERY_RETRY_SECS: u64 = 300; // 5 min — avoid hammering when no markets are live
/// How often to refresh the dashboard + reload squadron config / collateral.
const DASHBOARD_SYNC_SECS: u64 = 30;
/// Process-wide backoff on settlement lookups that keep answering `Unknown`.
///
/// `sync_dashboard` runs every [`DASHBOARD_SYNC_SECS`], after every entry and
/// dispatched fill, and from the idle-heartbeat task — and squadrons sharing
/// the shard sweep the same pool-wide stale-row set — so without this gate one
/// unanswerable row (a market between close and resolution, or a gateway
/// outage) cost ~2,880 live API calls over its 24h defer window, from inside
/// the trading loop. See [`crate::venues::core::SettlementProbeGate`] for the
/// policy and why in-memory state is safe here.
static SETTLEMENT_PROBES: std::sync::LazyLock<crate::venues::core::SettlementProbeGate> =
    std::sync::LazyLock::new(crate::venues::core::SettlementProbeGate::new);
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
/// Shard and squadron identity for the sports wing.
///
/// Renamed from a bare "us" so all three wings read consistently in the
/// Control Tower — the panel tags squadrons by asset, and "US" beside
/// "US-CRYPTO" and "US-POLITICS" looked like a different kind of thing rather
/// than a sibling. Done before launch deliberately: the squadron id derives
/// from this, and the id is the persistence key for operator config, so
/// changing it later would need a migration rather than a rename.
pub const US_ASSET: &str = "us-sports";
/// Runtime venue identity persisted on every trade and entry row. Both US
/// wings share one venue; they differ by shard and market class.
pub const US_VENUE: &str = "polymarket-us";
/// Asset key for the US crypto wing — its own DB pool, squadron id, and
/// viper-status scope so the sports and crypto squadrons never collide.
pub const US_CRYPTO_ASSET: &str = "us-crypto";

/// Shard for the politics wing.
pub const US_POLITICS_ASSET: &str = "us-politics";

/// Which market domain a US trading wing hunts. The venue runs one wing per
/// domain concurrently: the general wing keeps the original behavior (sports /
/// politics / anything non-crypto → order-book vipers), while the crypto wing
/// targets crypto-class markets and feeds them the full Raptor intelligence
/// stack so all nine vipers can fly.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Wing {
    /// Sports — the bulk of what Polymarket US lists (over a thousand NFL
    /// markets alone).
    Sports,
    /// Politics. Split out from the sports wing so the two run CONCURRENTLY:
    /// a single "everything non-crypto" wing trades one market at a time, so
    /// whichever ranked first won and the other category never got a look.
    /// Kalshi runs three concurrent squadrons; this brings the venues level.
    Politics,
    /// Crypto — the only class with a per-market price signal, and so the only
    /// one that can fly all nine vipers.
    Crypto,
}

impl Wing {
    fn asset(self) -> &'static str {
        match self {
            Wing::Sports => US_ASSET,
            Wing::Politics => US_POLITICS_ASSET,
            Wing::Crypto => US_CRYPTO_ASSET,
        }
    }
    /// Does this wing trade `pair`?
    ///
    /// Each wing takes only its own domain so the three run side by side without
    /// competing for the same market. The venue reports a `category` on every
    /// market, which is authoritative; `pair_is_crypto` remains the fallback for
    /// anything it does not label.
    async fn claims(self, pair: &super::markets::UsMarketPair) -> bool {
        let category = pair.category.to_ascii_lowercase();
        match self {
            Wing::Crypto => pair_is_crypto(pair).await,
            Wing::Politics => category == "politics",
            // Sports keeps everything else that is not crypto, so a market the
            // venue labels unusually still gets traded rather than dropped.
            Wing::Sports => category != "politics" && !pair_is_crypto(pair).await,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Wing::Sports => "sports",
            Wing::Politics => "politics",
            Wing::Crypto => "crypto",
        }
    }
    /// Wing-appropriate market discovery. The crypto wing goes through
    /// `/v1/search` → `/v1/markets?eventSlug=…` because the gateway ignores
    /// the `categories=` filter on `/v1/markets`, and the sports-dominated
    /// default query never surfaced crypto (3000 pairs, zero crypto,
    /// 2026-08-08). No volume floor — hourly crypto markets rotate and start
    /// near zero volume.
    pub(crate) async fn discover(
        self,
        venue: &UsRetailVenue,
    ) -> anyhow::Result<Vec<super::markets::UsMarketPair>> {
        match self {
            // Politics needs the search path for the same reason crypto does:
            // /v1/markets is sports-dominated and returned 60 pairs with not one
            // politics market, while /v1/search?query=politics returns 133 open
            // ones with real books.
            Wing::Politics => venue.discover_politics_markets_via_search().await,
            Wing::Sports => venue.discover_binary_markets().await,
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

    crate::venues::cancel_leftover_orders_at_startup(venue.as_ref()).await;

    // Three concurrent wings over the same venue connection — sports, politics
    // and crypto — each with its own squadron, DB shard and rotation loop.
    //
    // Sports and politics were previously one "everything non-crypto" wing that
    // traded a single market at a time, so whichever category ranked first won
    // and the other never got a look: Polymarket US lists over a thousand NFL
    // markets, which meant its 133 politics markets were effectively invisible.
    // Kalshi runs three concurrent squadrons; this brings the venues level.
    // Drain the deployment queue alongside the wings, exactly as Kalshi does.
    // Operator-deployed markets run concurrently with the venue's own selection
    // — a different market on a different cadence, not a replacement for it.
    tokio::spawn(crate::venues::deployment::run_deployment_processor(
        Arc::new(UsDeploymentRunner {
            venue: Arc::clone(&venue),
            cag: cag.clone(),
            raptor_health_tx: Arc::clone(&raptor_health_tx),
            markets_tx: Arc::clone(&markets_tx),
            process_heartbeat_secs: Arc::clone(&process_heartbeat_secs),
            sports_rx: sports_rx.clone(),
            tennis_rx: tennis_rx.clone(),
        }),
        cag.clone(),
        cancel.clone(),
    ));

    tokio::join!(
        run_wing(
            Wing::Sports, &venue, &cag, &raptor_health_tx, &markets_tx,
            &process_heartbeat_secs, &sports_rx, &tennis_rx, &filter, &cancel,
        ),
        run_wing(
            Wing::Politics, &venue, &cag, &raptor_health_tx, &markets_tx,
            &process_heartbeat_secs, &sports_rx, &tennis_rx, &filter, &cancel,
        ),
        run_wing(
            Wing::Crypto, &venue, &cag, &raptor_health_tx, &markets_tx,
            &process_heartbeat_secs, &sports_rx, &tennis_rx, &filter, &cancel,
        ),
    );
}


// ── Deployment runner ────────────────────────────────────────────────────────

/// Polymarket US's half of the shared deployment-queue consumer.
///
/// This venue previously had no consumer at all: the Control Tower offered a
/// Deploy button, `deploy_squadron` refused every request before it reached a
/// class, and the operator was told the venue "manages its own markets". It
/// does — via its wings — but that is not a reason to refuse a deployment, any
/// more than it is on Kalshi, whose rotation loop owns crypto and which still
/// accepts operator-deployed markets.
struct UsDeploymentRunner {
    venue: Arc<UsRetailVenue>,
    cag: Cag,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
    markets_tx: Arc<watch::Sender<HashMap<String, String>>>,
    process_heartbeat_secs: Arc<AtomicU64>,
    sports_rx: watch::Receiver<SportsSnapshot>,
    tennis_rx: watch::Receiver<TennisSnapshot>,
}

/// Discover the markets a class trades, using that class's own path.
///
/// Each class discovers differently — `/v1/markets` is sports-dominated, so
/// politics and crypto go through `/v1/search` instead. The Control Tower's
/// market browser used `/v1/markets` for everything, which is why browsing
/// politics or crypto returned nothing while both wings were trading. Sharing
/// this with the wings means the browser can only ever show markets the deploy
/// runner will also find.
pub async fn discover_for_class(
    venue: &UsRetailVenue,
    class: &str,
) -> anyhow::Result<Vec<super::markets::UsMarketPair>> {
    wing_for_class(class).discover(venue).await
}

/// Does `pair` belong to `class` on this venue?
///
/// The single definition of what a Polymarket US class contains, shared by the
/// trader's wings and by the Control Tower's market browser. Kept in one place
/// deliberately: the browser previously applied no class filter at all and
/// labeled every discovered market with whatever class was asked for, so
/// browsing "politics" listed Premier League football and tennis — and a deploy
/// from that list then failed at run time, because the politics wing quite
/// correctly did not recognize a tennis match as one of its markets.
pub async fn pair_matches_class(pair: &super::markets::UsMarketPair, class: &str) -> bool {
    wing_for_class(class).claims(pair).await
}

/// The wing a deployed market belongs to.
///
/// A deployment names a class, and the wings already encode what each class
/// trades — so a deployed market runs with the same raptors, vipers and DB
/// shard as one the wing discovered itself. Anything unrecognized goes to
/// Sports, matching `Wing::claims`, which keeps an oddly-labeled market traded
/// rather than dropped.
pub(crate) fn wing_for_class(class: &str) -> Wing {
    match class {
        "politics" => Wing::Politics,
        "crypto" => Wing::Crypto,
        _ => Wing::Sports,
    }
}

#[async_trait::async_trait]
impl crate::venues::deployment::DeploymentRunner for UsDeploymentRunner {
    fn venue_label(&self) -> &'static str { "Polymarket US" }

    async fn run_pinned(
        &self,
        dep: &crate::helpers::db::PendingDeployment,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let market_id = dep.market_id.as_str();
        let class = dep.market_type.as_str();
        let wing = wing_for_class(class);

        // Resolve the id against what this wing discovers, so a deployed market
        // is the same shape as a rotated one — the gateway has no single
        // "market by id" call that returns the paired legs.
        let pairs = wing.discover(&self.venue).await
            .map_err(|e| anyhow::anyhow!("{class} discovery failed: {e}"))?;
        let pair = pairs.into_iter()
            .find(|p| p.slug == market_id)
            .ok_or_else(|| anyhow::anyhow!(
                "market {market_id} is no longer listed under {class} — it may have closed"
            ))?;

        info!("📋 Deploying {class} squadron on \"{}\" [{}]", pair.question, pair.slug);
        let outcome = trade_one_market(
            &self.venue,
            &self.cag,
            &self.raptor_health_tx,
            &self.markets_tx,
            &self.process_heartbeat_secs,
            &self.sports_rx,
            &self.tennis_rx,
            &cancel,
            &dep.viper_budgets,
            wing,
            UsMarketSelection { primary: pair, maker: None },
        ).await;
        let _ = outcome;
        info!("📋 Deployed {class} squadron finished");
        Ok(())
    }

    async fn select_market(&self, class: &str, max_days_to_close: u32) -> Option<String> {
        let wing = wing_for_class(class);
        let pairs = match wing.discover(&self.venue).await {
            Ok(p) => p,
            Err(e) => {
                warn!("📋 Auto-deploy {class} discovery failed: {e}");
                return None;
            }
        };
        let now = Utc::now();
        let max_secs = max_days_to_close as i64 * 86_400;
        pairs.into_iter()
            .filter(|p| match p.close_time {
                // A market with no close time is "always open" by this venue's
                // convention and passes the horizon rather than being dropped.
                None => true,
                Some(ct) => {
                    let left = (ct - now).num_seconds();
                    left > 0 && left <= max_secs
                }
            })
            // Soonest close first, NOT highest volume: the Polymarket US
            // gateway reports no volume, so every pair is 0 and a max_by on it
            // returns whichever the iterator happened to reach first. Closing
            // soonest is at least a real ordering, and it favors a market that
            // will resolve inside a session rather than a 2026 future.
            .min_by_key(|p| p.close_time.unwrap_or(chrono::DateTime::<Utc>::MAX_UTC))
            .map(|p| p.slug)
    }
}

/// Run one wing's market rotation loop until `cancel` fires: select a market
/// in the wing's domain, trade it until it closes, re-discover the next one.

/// How long a switched-off wing waits before re-reading its switch.
const AUTO_DEPLOY_RECHECK_SECS: u64 = 30;

/// Is this wing allowed to go looking for a market?
///
/// Crypto is always allowed: it is the venue's own rotation, the equivalent of
/// Kalshi's crypto loop, and is not one of the classes the switches govern.
async fn wing_auto_deploy_enabled(wing: Wing) -> bool {
    let cfg = DynamicConfig::load_or_default().await;
    match wing {
        Wing::Politics => cfg.auto_deploy_politics,
        Wing::Sports => cfg.auto_deploy_sports,
        Wing::Crypto => true,
    }
}

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
        let sq_bg = wing.asset().to_string();
        let active_bg = Arc::clone(&trading_active);
        let cancel_bg = cancel.clone();
        tokio::spawn(async move {
            let Some(starting) = crate::venues::core::starting_collateral(
                venue_bg.as_ref(), &cancel_bg, COLLATERAL_RETRY_SECS).await else { return };
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cancel_bg.cancelled() => return,
                    _ = tick.tick() => {}
                }
                if active_bg.load(AtomicOrdering::Relaxed) { continue; }
                sync_dashboard(&sq_bg, venue_bg.as_ref(), &snap_pool, None, starting).await;
            }
        });
    }

    loop {
        if cancel.is_cancelled() {
            return;
        }

        // ── Auto-deploy switch ───────────────────────────────────────────────
        // The same two switches that decide whether Kalshi seeds a politics or
        // sports squadron decide whether this wing hunts for a market. Without
        // this the operator would have a control on one venue and not the other,
        // which is the asymmetry the switches exist to remove.
        //
        // Checked here rather than at spawn so a switch takes effect without a
        // restart: a wing turned off idles between its markets instead of dying,
        // and picks up again when it is turned back on. Positions already open
        // are unaffected — this gates the hunt for the NEXT market, and an
        // in-flight one runs to its close.
        if !wing_auto_deploy_enabled(wing).await {
            debug!("{} wing idle — auto-deploy switched off", wing.label());
            if wait_or_cancel(cancel, AUTO_DEPLOY_RECHECK_SECS).await {
                return;
            }
            continue;
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
            // The wing picked this market, so no operator budget applies.
            &Default::default(),
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
                    if wing.claims(&m).await {
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
    // Per-viper capital budgets the operator set in the deploy dialog. Empty for
    // a rotated market, which nobody chose the exposure for.
    viper_budgets: &std::collections::HashMap<String, f64>,
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
    let squadron = register_us_squadron(cag, &pair, sports_rx.clone(), tennis_rx.clone(), wing, raptors.as_ref(), strike_price, cancel);
    let squadron_id = squadron.id.clone();
    // Previous tick's ghost mode, so the registry is swept on the LIVE edge rather
    // than on every live tick: a level-triggered clear takes the registry's global
    // lock and walks the whole map on each pass to do nothing. Starts true so a
    // market entered live sweeps once.
    let mut was_ghosting = true;
    // Start this market clean of simulated resting quotes.
    //
    // Cleared on the way IN rather than on each way out, because there are many
    // return paths and only one entry. Kalshi and Polymarket US tear down through
    // `Cag::update_state`, which rewrites a summary string and does NOT call
    // `Squadron::stand_down`, so the stand-down hook that clears the registry on
    // intl never runs here. Without this a rotation orphaned that market's quotes
    // for the life of the process; worse, rotation keeps the same squadron id and
    // can return to a ticker traded earlier, so a quote frozen hours ago became
    // priceable again and crossed immediately into a fabricated entry. The stale
    // key also BLOCKED quoting, because `rest` refuses an occupied key.
    crate::helpers::ghost_quotes::clear_squadron(&squadron_id);

    // Seed the squadron's Viper config so the detail view's strategy cards render.
    seed_squadron_config(&squadron_id, viper_budgets).await;

    // Classify the market's domain and link it to its eligible raptors/vipers via
    // the shared, venue-neutral taxonomy (same path intl uses).
    let market_class = squadron.classify_and_link().await;
    // Filing dimensions for this market's rows. The general wing hunts sports
    // and politics, which have no underlying instrument at all — `None` here is
    // the correct value, not a missing one.
    let mut scope = TradeScope::new(
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
    let strategies = StrategyRegistry::create_strategies_for_kinds(&viper_kinds);
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
    let lifecycle = Arc::new(OrderLifecycle::new(LifecycleConfig::us(), squadron_id.clone()));
    // Upgrade fill confirmation to event-precise via the venue's private
    // account feed (`/v1/ws/private` → `subscribe_fills`); reconcile polling
    // remains the cancel/flatten path and the fallback backstop.
    let _fill_listener = lifecycle.spawn_fill_listener(Arc::clone(&venue), Arc::clone(&positions));
    let market_started_at = Utc::now();

    // Publish Raptor telemetry + active market so the squadron detail panels
    // populate (both feed `/api/status`).
    publish_us_raptor_health(raptor_health_tx, asset, true);
    publish_us_strategy_market(markets_tx, &squadron_id, &viper_kinds, &pair.question);

    // ── Stream both legs' order books (tied to the per-market cancel token) ───
    let ws_url = venue.markets_ws_url();
    let ws_auth = venue.ws_auth();
    let default_feed: PriceState = (dec!(0), dec!(0), dec!(1), dec!(0), Utc::now(), dec!(0), dec!(0));
    // ONE subscription per market, not one per leg.
    //
    // Both legs share the venue's identifier, so subscribing with each of them
    // opened two streams for the same book and then treated them as two
    // independent sides — the NO side mirrored the YES side instead of being its
    // complement. The book is quoted in LONG terms; SHORT is derived, exactly as
    // Kalshi derives its asks from the complementary bid.
    let (long_tx, long_rx) = watch::channel(default_feed);
    ws::spawn_market_feed(
        ws_url.clone(),
        super::markets::bare_symbol(pair.long.as_str()).to_string(),
        ws_auth.clone(),
        long_tx,
        cancel.clone(),
    );

    // The secondary market needs its own book: Maker and FairValue quote on it,
    // and a snapshot built from the primary's feed would price the wrong market.
    let maker_feeds = maker_pair.as_ref().map(|mk| {
        let (mlong_tx, mlong_rx) = watch::channel(default_feed);
        ws::spawn_market_feed(
            ws_url.clone(),
            super::markets::bare_symbol(mk.long.as_str()).to_string(),
            ws_auth.clone(),
            mlong_tx,
            cancel.clone(),
        );
        mlong_rx
    });

    // ── Dashboard + strategy-context state ───────────────────────────────────
    let pool = db::pool_for(asset);
    // Held until the venue answers. A zero baseline reports the whole balance
    // as session profit for the life of the process — see `starting_collateral`.
    let Some(starting) = crate::venues::core::starting_collateral(
        venue.as_ref(), cancel, COLLATERAL_RETRY_SECS).await else {
        return MarketOutcome::Cancelled;
    };
    let mut available_collateral = starting;
    let mut session_pnl = Decimal::ZERO;
    let mut dyn_cfg = DynamicConfig::load_for_squadron(&squadron_id).await;
    // See `TradeScope::ghost`: the ledger records whether a trade was real.
    scope.ghost = crate::config::GHOST_MODE || dyn_cfg.ghost_mode;
    if let Some(p) = &pool {
        let (coll, total) = sync_dashboard(&squadron_id, venue.as_ref(), p, Some(&positions), starting).await;
        available_collateral = coll;
        session_pnl = if dyn_cfg.ghost_mode { crate::helpers::metrics::realised_session_pnl() } else { total - starting };
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
                // Left in the registry so the operator keeps a record of the
                // stand-down; see the matching note in the Kalshi trader.
                cag.update_state(&squadron_id, SquadronState::StoodDown);
                publish_us_raptor_health(raptor_health_tx, asset, false);
                return MarketOutcome::Cancelled;
            }
            _ = dash_tick.tick() => {
                if let Some(p) = &pool {
                    let (coll, total) = sync_dashboard(&squadron_id, venue.as_ref(), p, Some(&positions), starting).await;
                    available_collateral = coll;
                    session_pnl = if dyn_cfg.ghost_mode { crate::helpers::metrics::realised_session_pnl() } else { total - starting };
                }
                // Pick up any Control Tower config edits for this squadron.
                dyn_cfg = DynamicConfig::load_for_squadron(&squadron_id).await;
                // Track a mid-session mode flip in the filing scope too.
                scope.ghost = crate::config::GHOST_MODE || dyn_cfg.ghost_mode;
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
                                // Rotating, not ending — the same squadron
                                // takes a new market and re-registers under this
                                // id. STOOD_DOWN parked it in the stood-down
                                // drawer for the gap, which reads as a death.
                                cag.update_state(&squadron_id, SquadronState::Rtb);
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
                    &squadron_id, venue, &pool, &scope, &positions, &lifecycle,
                    &market_cfg, maker_cfg.as_ref(), maker_feeds.as_ref(),
                    raptors.as_ref(), starting, dyn_cfg.ghost_mode,
                ).await;
                // Its market closed; the squadron continues onto the next.
                cag.update_state(&squadron_id, SquadronState::Rtb);
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
        let snapshot = build_snapshot(&long_rx, raptors.as_ref(), &market_cfg);

        // Maker snapshot from the secondary market's own feed. Falls back to the
        // primary when there is no secondary, or when its book has not arrived
        // yet — the same fallback Kalshi uses, so a viper always has something
        // priced rather than a zeroed book that reads as maximally adverse.
        let (mk_market, mk_snapshot) = match (&maker_cfg, &maker_feeds) {
            (Some(mcfg), Some(ml)) => {
                let snap = build_snapshot(ml, raptors.as_ref(), mcfg);
                if snap.yes_ask.is_zero() && snap.no_ask.is_zero() {
                    (Some(market_cfg.clone()), Some(snapshot.clone()))
                } else {
                    (Some(mcfg.clone()), Some(snap))
                }
            }
            _ => (Some(market_cfg.clone()), Some(snapshot.clone())),
        };

        // ── Ghost maker fills: rest until the book crosses the quote ──────
        //
        // See `helpers::ghost_quotes`. Before this, the ghost branch of
        // `dispatch_single` stamped a full fill at the quote price on placement,
        // and because the Maker viper quotes below the ask by construction the
        // position was born in profit and took its target on the next tick.
        // Leaving ghost mode drops this squadron's simulated quotes. Without it a
        // quote rested in ghost survives a GHOST → LIVE → GHOST round trip and can
        // cross hours later at a stale price, booking a fabricated entry on top of
        // whatever the key holds by then.
        if was_ghosting && !dyn_cfg.ghost_mode {
            crate::helpers::ghost_quotes::clear_squadron(&squadron_id);
        }
        was_ghosting = dyn_cfg.ghost_mode;
        if dyn_cfg.ghost_mode {
            let ask_for = |m: &crate::venues::core::MarketId| -> Option<Decimal> {
                if m == &market_cfg.yes_token { return Some(snapshot.yes_ask); }
                if m == &market_cfg.no_token  { return Some(snapshot.no_ask); }
                if let (Some(mcfg), Some(ms)) = (mk_market.as_ref(), mk_snapshot.as_ref()) {
                    if m == &mcfg.yes_token { return Some(ms.yes_ask); }
                    if m == &mcfg.no_token  { return Some(ms.no_ask); }
                }
                None
            };
            for (pk, resting) in crate::helpers::ghost_quotes::take_crossed(&squadron_id, ask_for) {
                let pos = resting.position;
                let rested = (Utc::now() - pos.opened_at).num_seconds();
                {
                    let mut map = positions.lock().await;
                    // Never overwrite an occupied slot: reconciliation writes real
                    // positions here without consulting ghost mode.
                    if map.contains_key(&pk) {
                        warn!("👻 GHOST_MODE MakerFill [{}]: {} | dropping simulated fill @ ${:.4} — slot already occupied",
                              pk.strategy, pos.market_name, pos.avg_entry);
                        continue;
                    }
                    info!("👻 GHOST_MODE MakerFill [{}]: {} | shares={:.2} @ ${:.4} — ask crossed after {}s resting (simulated)",
                          pk.strategy, pos.market_name, pos.shares, pos.avg_entry, rested);
                    map.insert(pk.clone(), pos.clone());
                }
                // Booked here rather than at placement, so the paper record only
                // carries fills a counterparty actually caused.
                let fill = Fill {
                    order_id: OrderId(String::new()),
                    market: resting.params.token_id.clone(),
                    filled: pos.shares,
                    price: pos.avg_entry,
                    fee: Decimal::ZERO,
                };
                record_entry(&squadron_id, &pool, &scope, &pk.strategy, &resting.params, &fill).await;
            }
        }

        let ctx = StrategyContext {
            market: market_cfg.clone(),
            snapshot: snapshot.clone(),
            positions: positions.clone(),
            session_pnl,
            starting_collateral: starting,
            squadron_id: squadron_id.clone(),
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
            if dispatch_signal(&squadron_id, venue.as_ref(), &pool, &positions, &lifecycle, &scope, &strategy_name, &signal, starting).await {
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

#[cfg(test)]
mod complement_tests {
    use super::complement;
    use rust_decimal_macros::dec;
    use chrono::Utc;

    fn state(bid: rust_decimal::Decimal, bid_sz: rust_decimal::Decimal,
             ask: rust_decimal::Decimal, ask_sz: rust_decimal::Decimal) -> crate::state::PriceState {
        (bid, bid_sz, ask, ask_sz, Utc::now(), bid_sz * dec!(3), ask_sz * dec!(3))
    }

    /// Polymarket US publishes ONE book per market, quoted in LONG terms, and
    /// puts the side in an order field. Subscribing with both legs opened two
    /// streams for the same book and treated them as independent sides, so the
    /// NO side mirrored the YES side instead of being its complement — YES+NO
    /// summed to 2× the long price rather than to a dollar.
    #[test]
    fn the_short_side_is_the_dollar_complement_of_the_long_side() {
        let long = state(dec!(0.40), dec!(10), dec!(0.45), dec!(7));
        let short = complement(long);

        assert_eq!(short.0, dec!(0.55), "short bid = 1 - long ask");
        assert_eq!(short.2, dec!(0.60), "short ask = 1 - long bid");
        // Sizes follow the level they came from.
        assert_eq!(short.1, dec!(7),  "short bid size is the long ask's size");
        assert_eq!(short.3, dec!(10), "short ask size is the long bid's size");

        // The pair prices a dollar, which is the whole point of a binary.
        assert_eq!(long.0 + short.2, dec!(1));
        assert_eq!(long.2 + short.0, dec!(1));
    }

    /// An untouched feed is bid 0 / ask 1 with no depth. Complementing that
    /// naively yields bid 0 / ask 1 again — but only because it is passed
    /// through: inventing a two-sided book at even money out of "no data" would
    /// read as a live market to every viper.
    #[test]
    fn an_empty_book_stays_empty() {
        let untouched = state(dec!(0), dec!(0), dec!(1), dec!(0));
        let short = complement(untouched);
        assert_eq!(short.1, dec!(0), "invented bid depth from an empty book");
        assert_eq!(short.3, dec!(0), "invented ask depth from an empty book");
    }

    /// Complementing twice returns the original, so the identity is sound.
    #[test]
    fn complementing_twice_is_the_identity() {
        let long = state(dec!(0.31), dec!(4), dec!(0.34), dec!(9));
        let back = complement(complement(long));
        assert_eq!((back.0, back.1, back.2, back.3), (long.0, long.1, long.2, long.3));
    }
}

#[cfg(test)]
mod wing_claim_tests {
    use super::*;

    fn pair(category: &str, slug: &str) -> super::super::markets::UsMarketPair {
        super::super::markets::UsMarketPair {
            slug: slug.to_string(),
            question: "q".into(),
            category: category.to_string(),
            description: String::new(),
            long: MarketId::new(format!("{slug}#long")),
            short: MarketId::new(format!("{slug}#short")),
            close_time: None,
            volume: 0.0,
        }
    }

    /// Sports and politics were one "everything non-crypto" wing that traded a
    /// single market at a time. Polymarket US lists over a thousand NFL markets
    /// against 133 politics ones, so politics never won the ranking and was
    /// effectively invisible. The wings must not both claim the same market, or
    /// they would compete for it instead of running side by side.
    #[tokio::test]
    async fn each_market_is_claimed_by_exactly_one_wing() {
        for (category, slug) in [
            ("politics", "vmc-ussep-mov-sc-rep-2026-08-25-gragte30"),
            ("sports",   "atc-lal-elc-fcb-2026-08-23-fcb"),
        ] {
            let p = pair(category, slug);
            let claims: Vec<&str> = [
                (Wing::Sports.claims(&p).await, "sports"),
                (Wing::Politics.claims(&p).await, "politics"),
            ].into_iter().filter_map(|(c, n)| c.then_some(n)).collect();
            assert_eq!(claims.len(), 1, "{category} claimed by {claims:?}");
            assert_eq!(claims[0], category);
        }
    }

    /// A market the venue labels unusually must still be traded rather than
    /// dropped — the sports wing keeps everything non-crypto that politics does
    /// not claim.
    #[tokio::test]
    async fn an_unlabeled_market_still_finds_a_wing() {
        let p = pair("", "aec-xyz-2026");
        assert!(Wing::Sports.claims(&p).await, "an unlabeled market fell through every wing");
        assert!(!Wing::Politics.claims(&p).await);
    }

    /// Each wing needs its own shard, or two squadrons share one database.
    #[test]
    fn the_wings_have_distinct_shards() {
        let assets = [Wing::Sports.asset(), Wing::Politics.asset(), Wing::Crypto.asset()];
        let mut uniq = assets.to_vec();
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(uniq.len(), 3, "wings share a shard: {assets:?}");
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
    // Squadron whose positions these keys address, so two squadrons
    // holding the same token stay independent.
    squadron_id: &str,
    venue: &Arc<UsRetailVenue>,
    pool: &Option<sqlx::SqlitePool>,
    scope: &TradeScope,
    positions: &Arc<Mutex<PositionMap>>,
    lifecycle: &Arc<OrderLifecycle>,
    primary_cfg: &MarketConfig,
    maker_cfg: Option<&MarketConfig>,
    maker_feeds: Option<&watch::Receiver<PriceState>>,
    raptors: Option<&CryptoRaptors>,
    starting: Decimal,
    ghost: bool,
) {
    let held: Vec<(String, MarketId, Decimal)> = {
        let map = positions.lock().await;
        map.iter()
            .filter(|(_, p)| p.shares > dec!(0))
            .filter(|(k, _)| k.squadron == squadron_id)
            .map(|(k, p)| (k.strategy.clone(), k.market.clone(), p.shares))
            .collect()
    };
    if held.is_empty() {
        return;
    }

    let (Some(mcfg), Some(mlong)) = (maker_cfg, maker_feeds) else {
        warn!(
            "🏁 Standing down holding {} position(s) with no open secondary market — leaving them to settle",
            held.len(),
        );
        return;
    };
    let snap = build_snapshot(mlong, raptors, mcfg);

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
        dispatch_signal(squadron_id, venue.as_ref(), pool, positions, lifecycle, scope, &strategy, &signal, starting).await;
    }
}

/// Build a venue-neutral [`MarketSnapshot`] from the two US leg feeds, enriched
/// with live Raptor intelligence when the wing has a stack attached.
/// `PriceState` layout: `(best_bid, bid_touch, best_ask, ask_touch, ts,
/// bid_depth_total, ask_depth_total)` — see `state::price_state`.
/// The SHORT side of a Polymarket US book, derived from the LONG side.
///
/// The venue publishes one book per market, quoted in LONG terms. A binary's
/// complement is `1 - price` with the sides swapped: the best SHORT bid is what
/// is left of a dollar after the best LONG ask, and its size is that ask's size.
/// Same identity Kalshi uses to derive asks from complementary bids.
///
/// An untouched feed (bid 0 / ask 1, no depth) complements to bid 0 / ask 1
/// again, so "no data" stays "no data" rather than becoming a fake two-sided
/// book at even money.
fn complement(p: PriceState) -> PriceState {
    let (bid, bid_sz, ask, ask_sz, ts, bid_tot, ask_tot) = p;
    let has_book = bid_sz > dec!(0) || ask_sz > dec!(0);
    if !has_book {
        return p;
    }
    (
        dec!(1) - ask,   // short bid  = 1 - long ask
        ask_sz,
        dec!(1) - bid,   // short ask  = 1 - long bid
        bid_sz,
        ts,
        ask_tot,
        bid_tot,
    )
}

fn build_snapshot(
    long_rx: &watch::Receiver<PriceState>,
    raptors: Option<&CryptoRaptors>,
    market: &MarketConfig,
) -> MarketSnapshot {
    let yes_state = *long_rx.borrow();
    let no_state  = complement(yes_state);
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
    // Book-feed health, recorded wherever the snapshot is built so a dark feed
    // is reported rather than merely declined by every gate.
    crate::state::price_state::book_feed::note(
        &market.market_name,
        crate::state::price_state::snapshot_has_book(&yes_state, &no_state),
    );
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
    // Squadron whose positions these keys address, so two squadrons
    // holding the same token stay independent.
    squadron_id: &str,
    positions: &Arc<Mutex<PositionMap>>,
    strategy_name: &str,
    params: &OrderParams,
    paired: Option<&MarketId>,
    fill: &Fill,
) {
    let mut map = positions.lock().await;
    map.insert(
        PositionKey::new(squadron_id, strategy_name, params.token_id.clone()),
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
    // Squadron whose positions these keys address, so two squadrons
    // holding the same token stay independent.
    squadron_id: &str,
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
                record_guard(squadron_id, positions, strategy_name, params, Some(&pp.token_id), &ga).await;
                record_guard(squadron_id, positions, strategy_name, pp, Some(&params.token_id), &gb).await;
                record_entry(squadron_id, pool, scope, strategy_name, params, &ga).await;
                record_entry(squadron_id, pool, scope, strategy_name, pp, &gb).await;
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
                    record_guard(squadron_id, positions, strategy_name, params, Some(&pp.token_id), &a).await;
                    record_guard(squadron_id, positions, strategy_name, pp, Some(&params.token_id), &b).await;
                    // Track both resting legs so the reconciler manages their
                    // lifecycle (fill-confirm / stale-cancel / naked-leg hedge).
                    lifecycle.track(&a, strategy_name, params.order_type, Some(pp.token_id.clone())).await;
                    lifecycle.track(&b, strategy_name, pp.order_type, Some(params.token_id.clone())).await;
                    record_entry(squadron_id, pool, scope, strategy_name, params, &a).await;
                    record_entry(squadron_id, pool, scope, strategy_name, pp, &b).await;
                    if let Some(p) = pool { sync_dashboard(squadron_id, venue, p, Some(positions), starting).await; }
                    true
                }
                Err(e) => { warn!("[{strategy_name}] atomic entry failed: {e}"); false }
            }
        }
        StrategySignal::Entry { params, pair_params: None } => {
            dispatch_single(squadron_id, venue, pool, positions, lifecycle, scope, strategy_name, params, Side::Buy, starting).await
        }
        StrategySignal::MakerQuote { yes, no } => {
            let mut acted = false;
            for q in [yes.as_ref(), no.as_ref()].into_iter().flatten() {
                // Ghost quotes REST; they do not fill. See `helpers::ghost_quotes`
                // and the crossing check in the tick loop.
                if q.ghost_mode {
                    let pk = PositionKey::new(squadron_id, strategy_name, q.token_id.clone());
                    if positions.lock().await.contains_key(&pk) { continue; }
                    let resting = Position {
                        shares: q.shares,
                        avg_entry: q.price,
                        opened_at: Utc::now(),
                        close_time: None,
                        market_name: q.market_name.clone(),
                        pair_token_id: q.token_id.clone(),
                        fill_confirmed_at: None,
                        paired_leg_token_id: None,
                        entry_fee: Decimal::ZERO,
                    };
                    if crate::helpers::ghost_quotes::rest(pk, resting, (*q).clone()) {
                        info!("👻 [{strategy_name}] ghost maker quote: {} @ {:.4} × {:.2} — resting until ask crosses",
                              q.token_id, q.price, q.shares);
                        acted = true;
                    }
                    continue;
                }
                if dispatch_single(squadron_id, venue, pool, positions, lifecycle, scope, strategy_name, q, Side::Buy, starting).await {
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
            // SIMULATING: pull the simulated quote and touch nothing else.
            //
            // This arm was unreachable in ghost mode until the maker viper learned
            // to see resting ghost quotes — and the moment it could emit
            // `MakerCancel` for one, the loop below started listing the ACCOUNT's
            // real open orders and cancelling any resting on that token. Ghost mode
            // never places a real order, so every match there is either the
            // operator's own manual order or a leftover from a previous live
            // session that the startup sweep just promised to leave alone.
            //
            // Inert here only for as long as this venue has no `open_orders()`
            // implementation; it becomes live the moment one lands. The intl patrol
            // has always returned before its real cancel path — this mirrors it.
            if scope.ghost {
                for tok in tokens {
                    let pk = PositionKey::new(squadron_id, strategy_name, tok.clone());
                    if let Some(pulled) = crate::helpers::ghost_quotes::pull(&pk) {
                        info!("👻 [{strategy_name}] ghost maker quote-pulled: {} @ {:.4} (simulated)",
                              tok, pulled.position.avg_entry);
                        acted = true;
                    }
                    positions.lock().await.remove(&pk);
                }
                return acted;
            }
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
                positions.lock().await.remove(&PositionKey::new(squadron_id, strategy_name, tok.clone()));
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
                .get(&PositionKey::new(squadron_id, strategy_name, params.token_id.clone()))
                .map(|p| (p.avg_entry, p.shares));
            let acted = dispatch_single(squadron_id, venue, pool, positions, lifecycle, scope, strategy_name, params, Side::Sell, starting).await;
            if acted {
                record_round_trip(pool, scope, strategy_name, params, entered, reason).await;
            }
            // Clear this strategy's guard for the leg (and the paired leg, if any)
            // so it can re-enter later.
            let mut map = positions.lock().await;
            map.remove(&PositionKey::new(squadron_id, strategy_name, params.token_id.clone()));
            if *exit_pair {
                let paired: Vec<_> = map.iter()
                    .filter(|(k, p)| k.strategy == strategy_name && k.squadron == squadron_id
                        && p.paired_leg_token_id.as_ref() == Some(&params.token_id))
                    .map(|(k, _)| (k.strategy.clone(), k.market.clone()))
                    .collect();
                for k in paired { map.remove(&PositionKey::new(squadron_id, k.0, k.1)); }
            }
            acted
        }
        StrategySignal::NoSignal => false,
    }
}

/// Place a single venue order from viper params, recording a buy-side guard and
/// tracking the order for lifecycle reconciliation if it rests.
async fn dispatch_single(
    // Squadron whose positions these guards belong to.
    squadron_id: &str,
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
            record_guard(squadron_id, positions, strategy_name, params, None, &ghost).await;
            record_entry(squadron_id, pool, scope, strategy_name, params, &ghost).await;
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
                record_guard(squadron_id, positions, strategy_name, params, None, &f).await;
                lifecycle.track(&f, strategy_name, params.order_type, None).await;
                record_entry(squadron_id, pool, scope, strategy_name, params, &f).await;
            }
            if let Some(p) = pool { sync_dashboard(squadron_id, venue, p, Some(positions), starting).await; }
            true
        }
        Err(e) => { warn!("[{strategy_name}] {side:?} order failed: {e}"); false }
    }
}

/// Persist a new position to `entries` + `open_positions`.
///
/// Without this the Control Tower's positions panel had to fall back on
/// [`sync_dashboard`]'s venue sweep, which knows the instrument but not which
/// viper owns it — every live position was mislabeled as ArbitrageStrategy.
async fn record_entry(
    // Squadron credited with the position in the database.
    squadron_id: &str,
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
            p, squadron_id, strategy_name, params.token_id.as_str(), &params.market_name,
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
    // Squadron whose guards are summarised for the dashboard.
    squadron_id: &str,
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
    // A failed positions fetch ABANDONS the sweep — it must never read as "the
    // account holds nothing". This used to be `unwrap_or_default()`, which
    // turned any transient error (timeout, 5xx, auth blip, rate limit) into an
    // empty `live_ids`: every confirmed row then read as stale, and the purge
    // below booked-and-deleted the venue's entire open-positions table — plus,
    // since the settlement sweep probes every non-live confirmed row, a burst
    // of resolution calls on the way. The intl chain-sync has always treated a
    // positions() error as "skip this pass" (`sync_open_positions_with_chain`
    // in tasks/cleanup.rs); same rule here and on the Kalshi loop.
    //
    // Collateral was already fetched successfully, so the caller gets the real
    // figure; `starting` as the total keeps its session P&L flat for the pass
    // rather than reporting a phantom drop equal to the unpriced positions, and
    // no P&L snapshot is written from a total we could not compute.
    let positions = match venue.positions().await {
        Ok(p) => p,
        Err(e) => {
            warn!("US dashboard sync: positions query failed — skipping reconcile/purge this pass: {e}");
            return (collateral, starting);
        }
    };

    // token → (owning viper, market name) from the live guard map, so a venue
    // holding is attributed to the viper that opened it. Holdings with no guard
    // (prior session, manual trade) are adopted under a neutral label rather
    // than being blamed on a viper that never traded them.
    let owners: HashMap<String, (String, String)> = match guards {
        Some(g) => g.lock().await.iter()
            .filter(|(k, _)| k.squadron == squadron_id)
            .map(|(k, p)| (k.market.as_str().to_string(), (k.strategy.clone(), p.market_name.clone())))
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
                    pool, squadron_id, strategy, sym, market_name, side_label(sym), p.avg_price, p.shares, false,
                ).await;
                db::confirm_position_status(pool, strategy, sym).await;
            }
            None => {
                db::record_open_position(
                    pool, squadron_id, "ChainAdopted", sym, sym, side_label(sym), p.avg_price, p.shares, false,
                ).await;
            }
        }
        positions_value += p.shares * p.avg_price;
    }
    // ── Settled positions the venue already cash-settled ────────────────────
    //
    // Polymarket US is custodial: when a market resolves, the gateway settles
    // positions to cash internally and drops them from
    // `/v1/portfolio/positions` — there is no redemption step and no event this
    // loop observes. A position that settles between two sweeps therefore
    // arrives at the purge as "not live", where it used to be booked at its
    // last mark or, with no usable mark (a loser marks near $0.00, failing the
    // mark's own `> 0` guard), deleted with nothing booked at all — real cash
    // moved and the ledger stayed empty. Same incident class as the 2026-08-31
    // Polymarket International loss of a winning $0.80 settlement.
    //
    // So ask the gateway what each stale market resolved to, rather than
    // inferring anything. A decisive answer feeds the purge's EXISTING
    // settlement branch (booked at exactly $1.00/$0.00, idempotent,
    // deduplicated); a verifiably-still-trading market is left to the
    // mark-priced reconcile path; anything unanswered is DEFERRED — not booked,
    // not deleted — and retried on [`SETTLEMENT_PROBES`]'s per-token backoff
    // (this sweep also runs on every fill, so retry-every-sweep was a polling
    // storm), bounded by age so an unanswerable row cannot pin the table
    // forever.
    //
    // Size ZERO is passed deliberately: the settlement branch falls back to the
    // row's own share count, which US rows retain (nothing zeroes them — the
    // intl chain-drift corrector does not run here).
    let mut resolved_marks: HashMap<String, (Decimal, Decimal)> = HashMap::new();
    let mut deferred: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (token, row_ts) in db::confirmed_open_positions(pool).await {
        if live_ids.contains(&token) {
            // Live again (a portfolio blip that briefly hid it, or re-adopted):
            // clear any backoff so its NEXT stale spell probes immediately.
            SETTLEMENT_PROBES.record_decisive(&token);
            continue;
        }
        // Inside its backoff window after earlier `Unknown` answers: treat it
        // exactly as a fresh `Unknown` — defer, book nothing, delete nothing —
        // WITHOUT spending a live API call. Only a token the gateway has not
        // answered for gets here, so a normal settlement (which answers
        // decisively on first sight) is never delayed by the gate.
        if !SETTLEMENT_PROBES.should_probe(&token) {
            deferred.insert(token);
            continue;
        }
        use crate::venues::core::TokenResolution as R;
        let age_secs = chrono::DateTime::parse_from_rfc3339(&row_ts)
            .map(|t| (Utc::now() - t.with_timezone(&Utc)).num_seconds())
            .unwrap_or(i64::MAX);
        match venue.settlement_resolution(&token).await {
            R::Resolved(px) => {
                SETTLEMENT_PROBES.record_decisive(&token);
                info!("🧾 Settled position detected [{}]: market resolution prices this leg at ${:.2} — booking", token, px);
                resolved_marks.insert(token, (px, Decimal::ZERO));
            }
            // Decisive too — the row leaves the table via the mark-priced
            // reconcile path this same sweep, so no backoff to keep.
            R::NotClosed => { SETTLEMENT_PROBES.record_decisive(&token); }
            R::Unknown if age_secs < db::SETTLEMENT_DEFER_MAX_SECS => {
                SETTLEMENT_PROBES.record_unknown(&token);
                deferred.insert(token);
            }
            R::Unknown => {
                SETTLEMENT_PROBES.record_unknown(&token);
                warn!("⚠️ Polymarket US settlement resolution unavailable for {} after {}h — falling back to mark-priced reconciliation",
                      token, age_secs / 3600);
            }
        }
    }
    let _ = db::purge_stale_open_positions(pool, &live_ids, &resolved_marks, &deferred).await;

    let total = collateral + positions_value;
    // Same reasoning as the trade loop's session P&L: portfolio value never
    // moves in ghost mode, so recording `total - starting` wrote 0.00 into every
    // snapshot beside a trades table that was steadily losing money. The chart,
    // the P&L history and the figure handed to the LLM advisor all read this
    // row, so the whole dashboard reported a flat session throughout.
    let session_pnl_recorded = if crate::helpers::dynamic_config::ghosting_now() {
        crate::helpers::metrics::realised_session_pnl()
    } else {
        total - starting
    };
    db::record_pnl_snapshot(pool, session_pnl_recorded, collateral, total).await;
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
    // The token this wing's trade loop selects on, so a stand-down stops it.
    cancel: &CancellationToken,
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
        Wing::Sports => "US Sports Arb",
        Wing::Politics => "US Politics Arb",
        Wing::Crypto => "US Crypto Squadron",
    };
    // Anything still addressing the sports wing by its former bare "us" — a
    // saved dashboard link, a stale client — resolves to the renamed shard
    // rather than 500ing on a missing pool.
    db::alias_pool("us", US_ASSET);

    let squadron = Squadron::new_with_category(
        CryptoAsset::Custom(wing.asset().to_uppercase()),
        SquadronConfig::arb_wing(name),
        market,
        raptors,
        // "sports", "crypto", … straight from the venue.
        Some(pair.category.clone()).filter(|c| !c.is_empty()),
    );
    cag.register_with_cancel(&squadron, cancel.clone());
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
/// Ensure the squadron has a config row, then apply the operator's deploy-time
/// per-viper budgets on top of it.
///
/// `budgets` is empty for a rotated market — the venue chose it, not an
/// operator — and non-empty only for a pinned deployment. Applying them after
/// the row exists rather than only when seeding it means a redeploy onto a
/// squadron id that already has config still honors the numbers just entered,
/// which is what the deploy dialog implies and what Polymarket International
/// has always done.
async fn seed_squadron_config(squadron_id: &str, budgets: &std::collections::HashMap<String, f64>) {
    if let Some(pool) = db::pool() {
        if db::squadron_config_get(pool, squadron_id).await.is_none() {
            DynamicConfig::init_for_squadron(squadron_id).await;
        }
    }
    if budgets.is_empty() {
        return;
    }
    let current = DynamicConfig::load_for_squadron(squadron_id).await;
    let mut cfg = (*current).clone();
    if crate::venues::deployment::apply_viper_budgets(&mut cfg, budgets) {
        cfg.save_for_squadron(squadron_id).await;
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
/// Publish "which market is each viper working" for the Control Tower.
///
/// Keyed by SQUADRON and viper kind, not by viper kind alone. Both US wings run
/// the same venue-agnostic vipers, so a bare kind meant the crypto wing
/// overwrote the general wing's entry and the squadron page showed one market in
/// its header while the viper strip named the other. Kalshi has the same shape
/// with three squadrons.
///
/// The squadron id is used rather than the asset because Kalshi's squadron asset
/// is "KALSHI" for every one of its squadrons, which would collide again.
fn publish_us_strategy_market(
    tx: &watch::Sender<HashMap<String, String>>,
    squadron_id: &str,
    viper_kinds: &[String],
    market_name: &str,
) {
    tx.send_modify(|map| {
        for kind in viper_kinds {
            map.insert(crate::state::strategy_market_key(squadron_id, kind), market_name.to_string());
        }
    });
}

