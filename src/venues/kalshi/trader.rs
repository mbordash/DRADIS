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

//! Kalshi trading loop — venue-neutral strategy execution over the
//! [`Execution`] trait, crypto-first.
//!
//! Mirrors the US retail crypto wing (`venues::us::trader`), adapted to
//! Kalshi's single-book market shape:
//!   1. discover open markets across the configured crypto series
//!      (`KXBTC15M`, `KXBTCD`, …) via `GET /markets?series_ticker=…`,
//!   2. classify + resolve eligible vipers via the shared taxonomy,
//!   3. stream the market's book over the [`ws`] orderbook feed (bids-only;
//!      asks derived as `1 − other-side bid`),
//!   4. each tick, build a venue-neutral [`StrategyContext`] and dispatch
//!      signals through the shared lifecycle engine.
//!
//! Fees: Kalshi charges quadratic taker fees, `ceil(7¢ · P·(1−P) · N)` —
//! worst case 1.75¢/contract at P=0.50. We surface a conservative 175 bps on
//! both legs so viper edge thresholds price the worst case in.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
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
use crate::helpers::metrics;
use crate::orchestrator::{
    aggregate_and_resolve_signals, evaluate_strategies, Strategy, StrategyContext,
    StrategyRegistry,
};
use crate::raptors::derivatives::DerivativesSnapshot;
use crate::raptors::horizon::HorizonSnapshot;
use crate::raptors::sports::SportsSnapshot;
use crate::raptors::tennis::TennisSnapshot;
use crate::raptors::tide::TideSnapshot;
use crate::squadron::{CryptoAsset, Squadron, SquadronConfig, SquadronRaptors, SquadronState};
use crate::state::{
    MarketConfig, MarketPhase, MarketSnapshot, OrderParams, Position, PositionMap, PriceState,
    StrategySignal, TradeScope, PositionKey};
use crate::venues::core::{Execution, Fill, MarketId, OrderId, OrderIntent, Side};
use crate::venues::lifecycle::{LifecycleConfig, OrderLifecycle};

use super::{leg_id, types::KalshiMarket, ws, KalshiVenue};

/// How long to wait between attempts to read the session's starting collateral.
const COLLATERAL_RETRY_SECS: u64 = 15;

/// Comma-separated series tickers to hunt (override with `KALSHI_SERIES`).
const ENV_SERIES: &str = "KALSHI_SERIES";
const DEFAULT_SERIES: &str = "KXBTC15M,KXBTCD,KXETH15M,KXETHD";
/// Optional substring filter (ticker / title) to pick a market.
const ENV_MARKET_FILTER: &str = "KALSHI_MARKET_FILTER";

const TICK_MS: u64 = 500;
const ACTION_COOLDOWN_SECS: u64 = 30;
const DISCOVERY_RETRY_SECS: u64 = 60; // 15-min markets rotate fast — rescan often
const DASHBOARD_SYNC_SECS: u64 = 30;
/// Skip markets closing within this window — not worth committing capital.
const MIN_TIME_TO_CLOSE_SECS: i64 = 180; // 3 min (15-min cadence needs headroom)
/// Maximum time-to-close for short-cadence markets (15M): 2 hours.
const MAX_CLOSE_SHORT_SECS: i64 = 7_200;
/// Maximum time-to-close for daily markets: 36 hours.
const MAX_CLOSE_DAILY_SECS: i64 = 129_600;
const MARKET_RTB_WINDOW_SECS: i64 = 60;
const LIFECYCLE_SYNC_SECS: u64 = 10;
const MARKET_RESCAN_SECS: u64 = 120;
/// Rotate only when a candidate has this much more volume (contracts).
const ROTATION_VOLUME_THRESHOLD: f64 = 5_000.0;
/// Conservative quadratic-fee ceiling (1.75¢/contract at P=0.5) in bps.
const KALSHI_FEE_BPS: u16 = 175;
/// SQLite shard key for this venue. A *storage* identity — every Kalshi
/// squadron writes here regardless of underlying, so it must not be shown as
/// the market's asset. Use `KALSHI_VENUE` / `TradeScope` for that.
pub const KALSHI_ASSET: &str = "kalshi";
/// Runtime venue identity persisted on every trade and entry row.
pub const KALSHI_VENUE: &str = "kalshi";

/// Market cadence derived from the series ticker suffix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cadence {
    /// 15-minute markets (`KXBTC15M`).
    FifteenMin,
    /// Daily markets (`KXBTCD`).
    Daily,
}

impl Cadence {
    /// Infer cadence from a market ticker string.
    fn from_ticker(ticker: &str) -> Option<Self> {
        let t = ticker.to_ascii_uppercase();
        if t.contains("15M") { return Some(Self::FifteenMin); }
        if t.contains("D-") || t.ends_with('D') { return Some(Self::Daily); }
        None
    }

    /// Maximum time-to-close for this cadence. Markets further out are
    /// filtered from discovery — they're either far-future listings (demo)
    /// or long-dated events where intraday strategies don't apply.
    fn max_close_secs(self) -> i64 {
        match self {
            Self::FifteenMin => MAX_CLOSE_SHORT_SECS,
            Self::Daily      => MAX_CLOSE_DAILY_SECS,
        }
    }
}

/// A tradeable Kalshi market expressed in DRADIS's two-leg shape.
#[derive(Clone, Debug)]
pub struct KalshiPair {
    pub ticker: String,
    pub question: String,
    /// YES leg (`{ticker}#yes`).
    pub long: MarketId,
    /// NO leg (`{ticker}#no`).
    pub short: MarketId,
    pub close_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Cumulative traded contracts — rotation ranking.
    pub volume: f64,
    pub underlying: &'static str,
    pub strike: Option<Decimal>,
    pub cadence: Option<Cadence>,
}

fn pair_from_market(m: &KalshiMarket) -> Option<KalshiPair> {
    let underlying = detect_underlying(&m.ticker)?;
    let question = if m.yes_sub_title.is_empty() {
        m.title.clone()
    } else {
        format!("{} — {}", m.title, m.yes_sub_title)
    };
    Some(KalshiPair {
        long: MarketId::new(leg_id(&m.ticker, true)),
        short: MarketId::new(leg_id(&m.ticker, false)),
        close_time: m.close_time_utc(),
        volume: super::types::fp(&m.volume_fp)
            .and_then(|d| f64::try_from(d).ok())
            .unwrap_or(0.0),
        strike: m.strike().and_then(|s| Decimal::try_from(s).ok()),
        ticker: m.ticker.clone(),
        cadence: Cadence::from_ticker(&m.ticker),
        question,
        underlying,
    })
}

/// Build a pair for a market with no crypto underlying — a politics or sports
/// market the operator deployed by hand.
///
/// `pair_from_market` returns None for these, because it derives the underlying
/// from the ticker prefix and there is none to find. That is correct for the
/// rotation loop, which only ever trades the configured crypto series, but it
/// also meant an operator-deployed market could not be represented at all.
fn pair_from_market_untethered(m: &KalshiMarket) -> KalshiPair {
    let question = if m.yes_sub_title.is_empty() {
        m.title.clone()
    } else {
        format!("{} — {}", m.title, m.yes_sub_title)
    };
    KalshiPair {
        long: MarketId::new(leg_id(&m.ticker, true)),
        short: MarketId::new(leg_id(&m.ticker, false)),
        close_time: m.close_time_utc(),
        volume: super::types::fp(&m.volume_fp)
            .and_then(|d| f64::try_from(d).ok())
            .unwrap_or(0.0),
        strike: m.strike().and_then(|s| Decimal::try_from(s).ok()),
        ticker: m.ticker.clone(),
        cadence: Cadence::from_ticker(&m.ticker),
        question,
        // Empty is the marker for "no crypto underlying". Every crypto-specific
        // path in the trade loop checks this rather than the market class, so a
        // future non-crypto class needs no further plumbing.
        underlying: "",
    }
}

impl KalshiPair {
    /// Does this market track a crypto underlying? False for politics, sports,
    /// and anything else deployed by hand.
    fn is_crypto(&self) -> bool { !self.underlying.is_empty() }
}

/// Why this market is being traded, and therefore how the loop treats it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum MarketMandate {
    /// Chosen by the venue's own volume rotation. May be rotated away from when
    /// a hotter market appears.
    Rotating,
    /// Deployed by the operator. Traded until it closes or is stood down —
    /// rotating away from a market someone deliberately chose would be wrong.
    Pinned,
}

/// Crypto underlying from a Kalshi series/market ticker prefix.
fn detect_underlying(ticker: &str) -> Option<&'static str> {
    let t = ticker.to_ascii_uppercase();
    for (needle, u) in [
        ("KXBTC", "btc"),
        ("KXETH", "eth"),
        ("KXSOL", "sol"),
        ("KXXRP", "xrp"),
        ("KXDOGE", "doge"),
    ] {
        if t.starts_with(needle) {
            return Some(u);
        }
    }
    None
}

fn configured_series() -> Vec<String> {
    std::env::var(ENV_SERIES)
        .unwrap_or_else(|_| DEFAULT_SERIES.to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

// ─── Raptor stack (per-underlying, process-lifetime) ─────────────────────────

/// Cloneable bundle of live Raptor signal receivers for one crypto underlying.
#[derive(Clone)]
struct CryptoRaptors {
    oracle: watch::Receiver<Decimal>,
    velocity: watch::Receiver<(Decimal, Decimal, Decimal)>,
    drift: watch::Receiver<(Decimal, Decimal, Decimal)>,
    funding: watch::Receiver<Decimal>,
    derivatives: watch::Receiver<DerivativesSnapshot>,
    tide: Option<watch::Receiver<TideSnapshot>>,
    horizon: Option<watch::Receiver<HorizonSnapshot>>,
}

impl CryptoRaptors {
    /// A stack that publishes nothing, for markets with no crypto underlying.
    ///
    /// Spawning the real stack for a politics market would open a Binance feed
    /// for a symbol that does not exist. The vipers that read these channels
    /// (Momentum, GBoost, Basis, FairValue) are not in the viper set for those
    /// classes anyway — the class-to-viper mapping in the database decides that
    /// — so a neutral snapshot is what the remaining vipers should see.
    ///
    /// The senders are dropped immediately; a `watch::Receiver` keeps serving
    /// its last value after the sender goes, which is exactly the behaviour
    /// wanted here.
    fn neutral() -> Self {
        let (_, oracle) = watch::channel(dec!(0));
        let (_, velocity) = watch::channel((dec!(0), dec!(0), dec!(0)));
        let (_, drift) = watch::channel((dec!(0), dec!(0), dec!(0)));
        let (_, funding) = watch::channel(dec!(0));
        let (_, derivatives) = watch::channel(DerivativesSnapshot::default());
        Self { oracle, velocity, drift, funding, derivatives, tide: None, horizon: None }
    }
}

static CRYPTO_RAPTOR_STACKS: OnceLock<std::sync::Mutex<HashMap<String, CryptoRaptors>>> =
    OnceLock::new();

/// Supervised task spawn (respawn-on-exit/panic) — same contract as intl/US.
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

/// Get (or lazily spawn) the full Raptor stack for a crypto underlying —
/// identical wiring to the US crypto wing / intl per-asset bootstrap.
fn raptor_stack_for(
    underlying: &str,
    raptor_health_tx: &Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
) -> CryptoRaptors {
    let registry = CRYPTO_RAPTOR_STACKS.get_or_init(Default::default);
    let mut map = registry.lock().expect("raptor stack registry poisoned");
    if let Some(stack) = map.get(underlying) {
        return stack.clone();
    }

    info!("🦖 Spawning crypto Raptor stack for '{underlying}' (Kalshi)");
    let http = Arc::new(reqwest::Client::new());

    let (oracle_tx, oracle_rx) = watch::channel(dec!(0));
    let (velocity_tx, velocity_rx) = watch::channel((dec!(0), dec!(0), dec!(0)));
    let (drift_tx, drift_rx) = watch::channel((dec!(0), dec!(0), dec!(0)));
    let (funding_tx, funding_rx) = watch::channel(dec!(0));
    let (deriv_tx, deriv_rx) = watch::channel(DerivativesSnapshot::default());

    {
        let asset = underlying.to_string();
        let health = Arc::clone(raptor_health_tx);
        spawn_supervised("kalshi-price-raptor", move || {
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
        spawn_supervised("kalshi-funding-raptor", move || {
            crate::raptors::funding::run_funding_raptor(
                Arc::clone(&http_c), asset.clone(), funding_tx.clone(), Arc::clone(&health),
            )
        });
    }
    {
        let asset = underlying.to_string();
        let http_c = Arc::clone(&http);
        let health = Arc::clone(raptor_health_tx);
        spawn_supervised("kalshi-derivatives-raptor", move || {
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
            spawn_supervised("kalshi-tide-raptor", move || {
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
            spawn_supervised("kalshi-horizon-raptor", move || {
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

// ─── Rotation loop ────────────────────────────────────────────────────────────

#[derive(Debug)]
enum MarketOutcome {
    Closed,
    BetterMarketFound,
    Cancelled,
}

/// Run the Kalshi trading loop until `cancel` fires: select the hottest
/// tradeable crypto market across the configured series, trade it until close,
/// rotate.
pub async fn run_kalshi_trader(
    venue: Arc<KalshiVenue>,
    cag: Cag,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
    markets_tx: Arc<watch::Sender<HashMap<String, String>>>,
    process_heartbeat_secs: Arc<AtomicU64>,
    // Venue-neutral observe-only feeds, spawned once in main.rs and shared with
    // whichever venue is running. Kalshi previously received neither, so its
    // sports squadron showed the Sports Raptor as linked — the taxonomy maps
    // `sports` to it — while nothing ever fed the channel.
    sports_rx: watch::Receiver<SportsSnapshot>,
    tennis_rx: watch::Receiver<TennisSnapshot>,
    cancel: CancellationToken,
) {
    let filter = std::env::var(ENV_MARKET_FILTER).ok().filter(|s| !s.is_empty());
    let series = configured_series();
    info!("🏛️ Kalshi trader starting — series={series:?} filter={filter:?}");

    // Venue-lifetime private fill feed (event-precise fill confirmation).
    venue.start_fill_feed(cancel.clone());

    // Drain the deployment queue alongside the rotation loop. Operator-deployed
    // markets run concurrently with the venue's own selection — they are a
    // different market class on a different cadence, not a replacement for it.
    tokio::spawn(crate::venues::deployment::run_deployment_processor(
        Arc::new(KalshiDeploymentRunner {
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

    // The squadron the rotation loop currently owns, so the next rotation can
    // retire it when it lands on a different underlying.
    let mut last_rotation_squadron: Option<String> = None;

    loop {
        if cancel.is_cancelled() {
            return;
        }

        let selection = match select_market(&venue, &series, &filter, &cancel, &process_heartbeat_secs).await {
            Some(s) => s,
            None => return,
        };

        let market_cancel = cancel.child_token();
        let outcome = trade_one_market(
            &venue,
            &cag,
            &raptor_health_tx,
            &markets_tx,
            &process_heartbeat_secs,
            &series,
            &market_cancel,
            selection,
            MarketMandate::Rotating,
            None,
            None,
            Some(&mut last_rotation_squadron),
            &sports_rx,
            &tennis_rx,
        ).await;
        market_cancel.cancel();

        match outcome {
            MarketOutcome::Cancelled => return,
            MarketOutcome::BetterMarketFound => {
                info!("🔀 Kalshi rotation — hotter market found, switching");
            }
            MarketOutcome::Closed => {
                info!("🔁 Kalshi market closed — rotating to next market");
                if wait_or_cancel(&cancel, 10).await {
                    return;
                }
            }
        }
    }
}

/// Discover open markets across all configured series and return tradeable
/// pairs sorted hottest-first.
async fn discover_pairs(venue: &KalshiVenue, series: &[String]) -> Vec<KalshiPair> {
    let mut pairs = Vec::new();
    for s in series {
        match venue.markets_for_series(s).await {
            Ok(markets) => {
                for m in &markets {
                    if m.status != "active" && !m.status.is_empty() {
                        continue;
                    }
                    if let Some(p) = pair_from_market(m) {
                        pairs.push(p);
                    }
                }
            }
            Err(e) => warn!("Kalshi discovery for series {s} failed: {e}"),
        }
    }
    let now = Utc::now();
    pairs.retain(|p| {
        let secs = match p.close_time {
            Some(c) => (c - now).num_seconds(),
            None => return false, // no close time → skip
        };
        if secs < MIN_TIME_TO_CLOSE_SECS {
            return false; // closing too soon
        }
        // Apply cadence-specific max-expiry window.
        let max = p.cadence.map(|c| c.max_close_secs()).unwrap_or(MAX_CLOSE_DAILY_SECS);
        secs <= max
    });
    pairs.sort_by(|a, b| b.volume.partial_cmp(&a.volume).unwrap_or(std::cmp::Ordering::Equal));
    pairs
}


// ── Deployment runner ────────────────────────────────────────────────────────

/// Kalshi's half of the shared deployment-queue consumer.
///
/// Everything about draining the queue — claiming rows, status transitions,
/// requeue-on-restart, the auto-deploy seeder — lives in
/// `crate::venues::deployment`. This supplies only what is genuinely
/// venue-shaped: resolving a Kalshi ticker into a tradeable pair, and choosing
/// one for a class.
struct KalshiDeploymentRunner {
    venue: Arc<KalshiVenue>,
    cag: Cag,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
    markets_tx: Arc<watch::Sender<HashMap<String, String>>>,
    process_heartbeat_secs: Arc<AtomicU64>,
    sports_rx: watch::Receiver<SportsSnapshot>,
    tennis_rx: watch::Receiver<TennisSnapshot>,
}

#[async_trait::async_trait]
impl crate::venues::deployment::DeploymentRunner for KalshiDeploymentRunner {
    fn venue_label(&self) -> &'static str { "Kalshi" }

    async fn run_pinned(
        &self,
        market_id: &str,
        class: &str,
        name: Option<&str>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let market = self.venue.market(market_id).await
            .map_err(|e| anyhow::anyhow!("could not load market {market_id}: {e}"))?;
        let pair = pair_from_market_untethered(&market);
        info!(
            "📋 Deploying {class} squadron on \"{}\" [{}] close={:?}",
            pair.question, pair.ticker, pair.close_time,
        );

        let outcome = trade_one_market(
            &self.venue,
            &self.cag,
            &self.raptor_health_tx,
            &self.markets_tx,
            &self.process_heartbeat_secs,
            // Rotation series are irrelevant to a pinned market; the rescan
            // branch returns early before ever reading them.
            &[],
            &cancel,
            MarketSelection { primary: pair, maker: None },
            MarketMandate::Pinned,
            Some(class),
            name,
            None,
            &self.sports_rx,
            &self.tennis_rx,
        ).await;
        info!("📋 Deployed {class} squadron finished: {outcome:?}");
        Ok(())
    }

    async fn select_market(&self, class: &str, max_days_to_close: u32) -> Option<String> {
        select_auto_deploy_market(&self.venue, class, max_days_to_close).await
    }
}

/// Highest-volume open market in `class`, within the operator's deploy horizon.
///
/// Mirrors the Control Tower's quick-deploy selection: same categories, same
/// sort, same horizon. Discovery reaches years out because that is how Kalshi
/// structures politics and sports, but Arbitrage locks collateral until the
/// market resolves, so a 2028 market is a poor use of it.
async fn select_auto_deploy_market(
    venue: &Arc<KalshiVenue>,
    class: &str,
    max_days_to_close: u32,
) -> Option<String> {
    let cats_raw = if class == "politics" {
        crate::config::KALSHI_POLITICS_CATEGORIES
    } else {
        crate::config::KALSHI_SPORTS_CATEGORIES
    };
    let cats: Vec<&str> = cats_raw.split(',').map(str::trim).filter(|c| !c.is_empty()).collect();
    let found = match venue.open_markets_for_categories(&cats).await {
        Ok(f) => f,
        Err(e) => {
            warn!("📋 Auto-deploy {class} discovery failed: {e:#}");
            return None;
        }
    };

    let now = Utc::now();
    let max_secs = max_days_to_close as i64 * 86_400;
    let mut best: Option<(f64, String)> = None;
    for (_cat, m) in found {
        if let Some(ct) = m.close_time_utc() {
            let secs_left = (ct - now).num_seconds();
            if secs_left < MIN_TIME_TO_CLOSE_SECS || secs_left > max_secs {
                continue;
            }
        } else {
            continue;
        }
        let volume = crate::venues::kalshi::types::fp(&m.volume_fp)
            .and_then(|d| f64::try_from(d).ok())
            .unwrap_or(0.0);
        if best.as_ref().is_none_or(|(v, _)| volume > *v) {
            best = Some((volume, m.ticker.clone()));
        }
    }
    best.map(|(_, ticker)| ticker)
}

struct MarketSelection {
    primary: KalshiPair,
    /// Daily market for passive maker/FairValue orders (longer-lived, lower fee).
    maker: Option<KalshiPair>,
}

async fn select_market(
    venue: &Arc<KalshiVenue>,
    series: &[String],
    filter: &Option<String>,
    cancel: &CancellationToken,
    process_heartbeat_secs: &AtomicU64,
) -> Option<MarketSelection> {
    loop {
        if cancel.is_cancelled() {
            return None;
        }
        touch_heartbeat(process_heartbeat_secs);
        let pairs = discover_pairs(venue, series).await;
        if pairs.is_empty() {
            warn!("Kalshi trader: no tradeable markets across {series:?} — retrying in {DISCOVERY_RETRY_SECS}s");
        } else {
            info!(
                "📊 Kalshi discovered {} tradeable market(s). Top 3: {}",
                pairs.len(),
                pairs.iter().take(3)
                    .map(|p| format!("\"{}\" ({:?}, vol {:.0})", p.question, p.cadence, p.volume))
                    .collect::<Vec<_>>().join(", ")
            );

            // Split by cadence: prefer short-cadence (15M) as primary, daily as maker.
            let (short, daily): (Vec<_>, Vec<_>) = pairs.into_iter()
                .partition(|p| p.cadence == Some(Cadence::FifteenMin));

            let apply_filter = |candidates: Vec<KalshiPair>| -> Option<KalshiPair> {
                match filter {
                    Some(f) => {
                        let fl = f.to_lowercase();
                        candidates.into_iter().find(|p| {
                            p.ticker.to_lowercase().contains(&fl)
                                || p.question.to_lowercase().contains(&fl)
                        })
                    }
                    None => candidates.into_iter().next(),
                }
            };

            // Primary: prefer 15M, fall back to daily.
            let primary = apply_filter(short.clone())
                .or_else(|| apply_filter(daily.clone()));

            if let Some(p) = primary {
                // Maker: pick the best daily market with the same underlying
                // (skip the primary if it's already daily to avoid self-reference).
                let maker = daily.into_iter()
                    .find(|d| d.underlying == p.underlying && d.ticker != p.ticker);

                info!(
                    "🎯 Kalshi primary: \"{}\" [{}] {:?} close={:?} strike={:?}",
                    p.question, p.ticker, p.cadence, p.close_time, p.strike
                );
                if let Some(ref m) = maker {
                    info!(
                        "🎯 Kalshi maker:   \"{}\" [{}] {:?} close={:?} strike={:?}",
                        m.question, m.ticker, m.cadence, m.close_time, m.strike
                    );
                }
                return Some(MarketSelection { primary: p, maker });
            }
            warn!("Kalshi trader: no market matched filter {filter:?} — retrying");
        }
        if wait_or_cancel(cancel, DISCOVERY_RETRY_SECS).await {
            return None;
        }
    }
}

// ─── Single-market session ───────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn trade_one_market(
    venue: &Arc<KalshiVenue>,
    cag: &Cag,
    raptor_health_tx: &Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
    markets_tx: &Arc<watch::Sender<HashMap<String, String>>>,
    process_heartbeat_secs: &AtomicU64,
    series: &[String],
    cancel: &CancellationToken,
    selection: MarketSelection,
    mandate: MarketMandate,
    // `deployed_as` is the class label for an operator-deployed market
    // ("politics", "sports"), used to name its squadron. None for the rotation
    // loop, which names the squadron after the crypto underlying.
    deployed_as: Option<&str>,
    // Operator-chosen name; folded into the squadron id so a second squadron of
    // this class gets its own config, budgets and positions.
    squadron_name: Option<&str>,
    // The squadron the rotation loop registered last time round, so a rotation
    // that lands on a DIFFERENT underlying can retire it. `None` for pinned
    // deployments, which do not rotate.
    last_rotation_squadron: Option<&mut Option<String>>,
    sports_rx: &watch::Receiver<SportsSnapshot>,
    tennis_rx: &watch::Receiver<TennisSnapshot>,
) -> MarketOutcome {
    let asset = KALSHI_ASSET;
    let pair = selection.primary;
    let maker_pair = selection.maker;

    // ── Raptor intelligence for the market's underlying ─────────────────────
    // Identity for logs and the viper-status registry: the crypto underlying
    // where there is one, the market class otherwise.
    let status_scope = if pair.is_crypto() { pair.underlying } else { deployed_as.unwrap_or("deployed") };

    let raptors = if pair.is_crypto() {
        info!(
            "🧠 Kalshi: underlying={} strike={:?} for \"{}\"",
            pair.underlying, pair.strike, pair.question
        );
        raptor_stack_for(pair.underlying, raptor_health_tx)
    } else {
        info!("🧠 Kalshi: no crypto underlying for \"{}\" — neutral signals", pair.question);
        CryptoRaptors::neutral()
    };

    // ── Register the squadron so the Control Tower lists it ─────────────────
    let squadron = register_kalshi_squadron(cag, &pair, &raptors, deployed_as, squadron_name, sports_rx, tennis_rx, cancel);
    let squadron_id = squadron.id.clone();
    // Retire the squadron this rotation replaced, but only when the id actually
    // changed. Rotating BTC→BTC re-registers the same id and must NOT be
    // removed — that flicker is what made a healthy squadron look like it was
    // dying every fifteen minutes. Rotating BTC→ETH is a different squadron,
    // and leaving the old one registered accumulated a permanent RTB row per
    // underlying the loop had ever visited: two crypto squadrons on screen, one
    // of them trading nothing and never able to again.
    if let Some(slot) = last_rotation_squadron {
        if let Some(prev) = slot.as_ref() {
            if prev != &squadron_id {
                info!("🔀 Kalshi rotation left {prev} — retiring it for {squadron_id}");
                cag.remove(prev);
            }
        }
        *slot = Some(squadron_id.clone());
    }
    // The squadron registers under the crypto underlying (so the taxonomy
    // classifies it as crypto), but this venue's DB scope is KALSHI_ASSET.
    // Alias the two so the Control Tower's `?asset=btc` queries reach this
    // venue's database instead of erroring out with an empty tradelog.
    // Route lookups by this squadron's public identity to the venue's shard.
    //
    // Every Kalshi squadron writes to one shard, but the Control Tower queries
    // positions by the SQUADRON's asset — "btc"/"eth" for the rotation loop,
    // "politics"/"sports" for a deployed one. Without an alias those resolve to
    // no pool and the endpoint 500s. The non-crypto case was missed originally,
    // and the crypto one only registers when a market is already selected, which
    // leaves a window across restarts and underlying rotations.
    db::alias_pool(status_scope, asset);
    seed_squadron_config(&squadron_id).await;
    let market_class = squadron.classify_and_link().await;
    // Filing dimensions for every row this market writes. `asset` above is
    // only the shard (one DB for all Kalshi squadrons); venue, class and
    // underlying are the attributes that actually describe the trade.
    let scope = TradeScope::new(
        asset,
        KALSHI_VENUE,
        Some(market_class.clone()),
        // A politics market has no underlying; recording "" would file every
        // such trade under a blank dimension rather than none.
        pair.is_crypto().then(|| pair.underlying.to_string()),
    );

    let viper_kinds = match db::pool() {
        Some(p) => db::vipers_for_class(p, &market_class).await,
        None => Vec::new(),
    };
    let strategies = build_strategies(&viper_kinds);
    info!(
        "🎯 Kalshi loop will run {} viper(s) for class '{}': {:?}",
        strategies.len(),
        market_class,
        strategies.iter().map(|s| s.name()).collect::<Vec<_>>()
    );
    if strategies.is_empty() {
        warn!("Kalshi trader: no runnable vipers for class '{market_class}' — dashboard only");
    }

    let market_cfg = MarketConfig {
        yes_token: pair.long.clone(),
        no_token: pair.short.clone(),
        market_name: pair.question.clone(),
        market_close_time: pair.close_time,
        strike_price: pair.strike,
        is_neg_risk: false,
        condition_id: String::new(),
        yes_fee_bps: KALSHI_FEE_BPS as u32,
        no_fee_bps: KALSHI_FEE_BPS as u32,
    };
    // Daily maker venue — gives passive orders more time to fill and provides
    // strike-based pricing for FairValue/Basis vipers.
    let maker_cfg = maker_pair.as_ref().map(|mp| MarketConfig {
        yes_token: MarketId::new(leg_id(&mp.ticker, true)),
        no_token: MarketId::new(leg_id(&mp.ticker, false)),
        market_name: mp.question.clone(),
        market_close_time: mp.close_time,
        strike_price: mp.strike,
        is_neg_risk: false,
        condition_id: String::new(),
        yes_fee_bps: KALSHI_FEE_BPS as u32,
        no_fee_bps: KALSHI_FEE_BPS as u32,
    });
    let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
    let lifecycle = Arc::new(OrderLifecycle::new(LifecycleConfig::us(), squadron_id.clone()));
    let _fill_listener = lifecycle.spawn_fill_listener(Arc::clone(venue), Arc::clone(&positions));
    let market_started_at = Utc::now();

    if pair.is_crypto() {
        publish_raptor_health(raptor_health_tx, pair.underlying, true);
    }
    // Map each viper to its preferred venue: "Hourly" vipers → primary (15M),
    // "Window/Daily" vipers → maker (daily), mirroring the intl/US patrol logic.
    let maker_name = maker_pair.as_ref().map(|mp| mp.question.as_str()).unwrap_or(&pair.question);
    markets_tx.send_modify(|map| {
        for s in &strategies {
            let key = s.name()
                .strip_suffix("Strategy").unwrap_or(&s.name()).to_lowercase()
                .replace("timedecay", "time_decay")
                .replace("trendreversal", "trendcapture");
            let market = if s.venue() == "Window/Daily" { maker_name } else { &pair.question };
            map.insert(crate::state::strategy_market_key(&squadron_id, &key), market.to_string());
        }
    });
    if let Some(ref mp) = maker_pair {
        cag.update_maker_market(&squadron_id, mp.question.clone());
    }

    // ── Stream the market's book (bids-only; asks derived) ──────────────────
    let hub = ws::new_book_hub();
    let mut book_tickers = vec![pair.ticker.clone()];
    if let Some(ref mp) = maker_pair {
        book_tickers.push(mp.ticker.clone());
    }
    ws::spawn_orderbook_feed(
        super::ws_url(),
        venue.auth.clone(),
        book_tickers,
        hub.clone(),
        cancel.clone(),
    );

    // ── Dashboard + strategy-context state ──────────────────────────────────
    let pool = db::pool_for(asset);
    // Held until the venue answers — see `starting_collateral`.
    let Some(starting) = crate::venues::core::starting_collateral(
        venue.as_ref(), cancel, COLLATERAL_RETRY_SECS).await else {
        return MarketOutcome::Cancelled;
    };
    let mut available_collateral = starting;
    let mut session_pnl = Decimal::ZERO;
    let mut dyn_cfg = DynamicConfig::load_for_squadron(&squadron_id).await;
    if let Some(p) = &pool {
        let (coll, total) = sync_dashboard(&squadron_id, venue.as_ref(), p, &positions, starting).await;
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

    cag.update_state(&squadron_id, SquadronState::Patrolling);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("Kalshi trader: cancelled — standing down");
                lifecycle.cancel_all(venue.as_ref()).await;
                // Marked STOOD_DOWN but deliberately LEFT in the registry, so
                // the operator keeps a record of what they stood down — the
                // Control Tower lists these under "stood-down / RTB". The other
                // exit paths remove the entry because the squadron is being
                // replaced by the next rotation; an operator stand-down is not
                // replaced by anything, and removing it made the squadron
                // vanish with no trace of the act.
                //
                // A redeploy under the same id revives this row rather than
                // adding one. That is only ambiguous for an UNNAMED redeploy:
                // a named squadron takes `{asset}-{cadence}-{slug}` and appears
                // as its own entry alongside this one.
                cag.update_state(&squadron_id, SquadronState::StoodDown);
                if pair.is_crypto() { publish_raptor_health(raptor_health_tx, pair.underlying, false); }
                return MarketOutcome::Cancelled;
            }
            _ = dash_tick.tick() => {
                if let Some(p) = &pool {
                    let (coll, total) = sync_dashboard(&squadron_id, venue.as_ref(), p, &positions, starting).await;
                    available_collateral = coll;
                    session_pnl = if dyn_cfg.ghost_mode { crate::helpers::metrics::realised_session_pnl() } else { total - starting };
                }
                dyn_cfg = DynamicConfig::load_for_squadron(&squadron_id).await;
                continue;
            }
            _ = rescan_tick.tick() => {
                // A market the operator deployed is never rotated away from —
                // they chose it, and swapping it for whatever is hottest in the
                // crypto series would be both surprising and off-class.
                if mandate == MarketMandate::Pinned {
                    continue;
                }
                // Only rotate when flat.
                if !positions.lock().await.is_empty() {
                    continue;
                }
                let mut candidates = discover_pairs(venue, series).await;
                candidates.retain(|p| p.ticker != pair.ticker);
                if let Some(best) = candidates.first() {
                    if best.volume > pair.volume + ROTATION_VOLUME_THRESHOLD {
                        info!(
                            "🔍 Hotter Kalshi market: \"{}\" vol={:.0} > \"{}\" vol={:.0} + {:.0} — rotating",
                            best.question, best.volume, pair.question, pair.volume, ROTATION_VOLUME_THRESHOLD
                        );
                        lifecycle.cancel_all(venue.as_ref()).await;
                        // A rotation is not an ending — the same squadron is
                        // moving to a new market and re-registers under this id
                        // moments later. Marking it STOOD_DOWN and removing it
                        // made the row vanish from the Control Tower for the
                        // ten-second gap and reappear afterwards, which reads as
                        // the squadron dying; it is alarming next to an
                        // unrelated stand-down the operator has just performed.
                        // RTB is what is actually true here.
                        cag.update_state(&squadron_id, SquadronState::Rtb);
                        if pair.is_crypto() { publish_raptor_health(raptor_health_tx, pair.underlying, false); }
                        return MarketOutcome::BetterMarketFound;
                    }
                }
                continue;
            }
            _ = lifecycle_tick.tick() => {
                let flattened = lifecycle.reconcile(venue.as_ref(), &positions).await;
                for leg in flattened {
                    let pnl = (leg.exit_price - leg.avg_entry) * leg.shares;
                    warn!(
                        "📋 [{}] lifecycle flatten recorded: {} entry={:.4} exit={:.4} shares={} pnl={pnl:.4}",
                        leg.strategy, leg.market_name, leg.avg_entry, leg.exit_price, leg.shares,
                    );
                    let strat  = leg.strategy.clone();
                    let market = leg.market_name.clone();
                    let (avg_entry, exit_price, shares) = (leg.avg_entry, leg.exit_price, leg.shares);
                    let scope_t = scope.clone();
                    tokio::spawn(async move {
                        metrics::record_trade(
                            // Lifecycle flatten legs don't surface a fee through
                            // OrderLifecycle yet — booked gross, flagged here.
                            &scope_t, Decimal::ZERO, strat, market, "Sell".to_string(),
                            avg_entry, exit_price, shares, pnl,
                            "LifecycleFlatten".to_string(),
                        ).await;
                    });
                }
                continue;
            }
            _ = price_tick.tick() => {}
        }
        touch_heartbeat(process_heartbeat_secs);

        match market_cfg.phase(Utc::now(), MARKET_RTB_WINDOW_SECS) {
            MarketPhase::Closed => {
                info!("🏁 Kalshi market \"{}\" reached close — standing down to rotate", market_cfg.market_name);
                // cancel_all cancels RESTING ORDERS; it does not sell inventory.
                // A squadron trades two markets at once — the hot primary that
                // rotates every 15-30 minutes, and a longer-lived daily maker
                // market that FairValue actively prefers — and its lifetime is
                // tied to the PRIMARY's close. Tearing down here therefore
                // abandoned any position sitting on the daily, which then ran
                // unmanaged (no stop, no take-profit) until it expired. That is
                // how -$3.09 was lost on 2026-08-10, recorded in neither
                // `trades` nor `entries`.
                lifecycle.cancel_all(venue.as_ref()).await;
                flatten_before_stand_down(
                    &squadron_id, venue, &hub, &pool, &scope, &positions, &lifecycle,
                    &market_cfg, maker_cfg.as_ref(), maker_pair.as_ref(), &raptors, starting,
                    dyn_cfg.ghost_mode,
                ).await;
                // Its market closed, but the squadron itself continues onto
                // the next one — see the rotation note above. Held in the
                // registry as RTB so the row stays put across the gap.
                cag.update_state(&squadron_id, SquadronState::Rtb);
                if pair.is_crypto() { publish_raptor_health(raptor_health_tx, pair.underlying, false); }
                return MarketOutcome::Closed;
            }
            MarketPhase::WindingDown => {
                if !winding_down {
                    winding_down = true;
                    info!(
                        "⏳ Kalshi market \"{}\" within {}s of close — RTB, no new entries",
                        market_cfg.market_name, MARKET_RTB_WINDOW_SECS
                    );
                    cag.update_state(&squadron_id, SquadronState::Rtb);
                }
                // Deliberately NOT `continue`. This arm used to skip the tick
                // entirely, which stopped exits as well as entries — so for the
                // whole run-up to a close, a position could not be stopped out
                // or taken off. Fall through and let evaluation run; entries are
                // suppressed at dispatch by `opens_exposure`.
            }
            MarketPhase::Open => {}
        }

        if strategies.is_empty() || Instant::now() < cooldown_until {
            continue;
        }

        // No book yet (feed still connecting) — nothing to evaluate this tick.
        let Some(snapshot) = build_snapshot(&hub, &pair.ticker, &raptors, &market_cfg).await else {
            continue;
        };

        // Build maker snapshot from the daily market (if available).
        let (mk_market, mk_snapshot) = match (&maker_cfg, &maker_pair) {
            (Some(mcfg), Some(mp)) => {
                match build_snapshot(&hub, &mp.ticker, &raptors, mcfg).await {
                    Some(ms) => (Some(mcfg.clone()), Some(ms)),
                    // Maker book not ready — fall back to primary.
                    None => (Some(market_cfg.clone()), Some(snapshot.clone())),
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
            squadron_id: squadron_id.clone(),
            // Doubles as the viper-status registry key, which is
            // (this string, strategy name). A market with no crypto underlying
            // left it empty, so every non-crypto squadron shared one set of
            // slots: deploying politics and sports produced two rows between
            // them instead of two each, with whichever evaluated last
            // overwriting the other, and the CAG rollup counting two squadrons
            // where there were three. Fall back to the market class, which the
            // deploy endpoint already enforces as one squadron apiece.
            crypto_filter: status_scope.to_uppercase(),
            market_started_at,
            maker_market: mk_market,
            maker_snapshot: mk_snapshot,
            available_collateral,
            dynamic_config: dyn_cfg.clone(),
            arb_market_lockouts: None,
        };

        let eval = match evaluate_strategies(&strategies, &ctx).await {
            Ok(e) => e,
            Err(e) => { warn!("Kalshi strategy evaluation error: {e}"); continue; }
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
            if dispatch_signal(&squadron_id, venue.as_ref(), &pool, &scope, &positions, &lifecycle, &strategy_name, &signal, starting).await {
                acted = true;
            }
        }
        if acted {
            cooldown_until = Instant::now() + Duration::from_secs(ACTION_COOLDOWN_SECS);
        }
    }
}

// ─── Strategy plumbing ────────────────────────────────────────────────────────

/// Sell any inventory that is still tradeable before the squadron stands down.
///
/// The primary market has closed, so anything held on it can only settle — that
/// is fine and expected, a binary pays out at resolution. What is not fine is
/// inventory on the SECONDARY daily market, which typically has up to an hour
/// left: the squadron was its only manager, and walking away leaves it with no
/// stop and no take-profit until it expires.
///
/// Exits are synthesised at the live bid for the leg actually held, so this is
/// the same FAK-at-the-bid path a stop would take, not a market order into
/// nothing. A leg whose book has gone empty is left to settle and logged, since
/// selling into an empty book is worse than holding to resolution.
#[allow(clippy::too_many_arguments)]
async fn flatten_before_stand_down(
    // Squadron whose positions these keys address, so two squadrons
    // holding the same token stay independent.
    squadron_id: &str,
    venue: &Arc<KalshiVenue>,
    hub: &ws::BookHub,
    pool: &Option<sqlx::SqlitePool>,
    scope: &TradeScope,
    positions: &Arc<Mutex<PositionMap>>,
    lifecycle: &Arc<OrderLifecycle>,
    primary_cfg: &MarketConfig,
    maker_cfg: Option<&MarketConfig>,
    maker_pair: Option<&KalshiPair>,
    raptors: &CryptoRaptors,
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

    // Only the maker market can still be traded; the primary is closed.
    let (Some(mcfg), Some(mpair)) = (maker_cfg, maker_pair) else {
        warn!(
            "🏁 Standing down holding {} position(s) with no open secondary market — leaving them to settle",
            held.len(),
        );
        return;
    };
    let Some(snap) = build_snapshot(hub, &mpair.ticker, raptors, mcfg).await else {
        warn!(
            "🏁 Standing down holding {} position(s) but the secondary book has not arrived — leaving them to settle",
            held.len(),
        );
        return;
    };

    for (strategy, token, shares) in held {
        let bid = if token == mcfg.yes_token {
            snap.yes_bid
        } else if token == mcfg.no_token {
            snap.no_bid
        } else {
            info!(
                "🏁 [{strategy}] {} shares on \"{}\" are on the closed primary — leaving to settle",
                shares, primary_cfg.market_name,
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
                fee_bps: KALSHI_FEE_BPS as u16,
                is_neg_risk: false,
                market_name: mcfg.market_name.clone(),
                condition_id: String::new(),
                order_type: crate::venues::core::TimeInForce::Fak,
                post_only: false,
                ghost_mode: ghost,
            },
            reason: "SquadronStandDown".to_string(),
            // Each leg is flattened on its own; pairing here would double-exit.
            exit_pair: false,
        };
        dispatch_signal(squadron_id, venue.as_ref(), pool, scope, positions, lifecycle, &strategy, &signal, starting).await;
    }
}

fn touch_heartbeat(hb: &AtomicU64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    hb.store(now, AtomicOrdering::Relaxed);
}

fn build_strategies(viper_kinds: &[String]) -> Vec<Box<dyn Strategy>> {
    StrategyRegistry::create_all_strategies()
        .into_iter()
        .filter(|s| viper_kinds.iter().any(|k| k == strategy_name_to_kind(&s.name())))
        .collect()
}

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
        "TrendCaptureStrategy" => "trendcapture",
        _ => "",
    }
}

/// Build a venue-neutral [`MarketSnapshot`] from the live Kalshi book +
/// Raptor intelligence.
///
/// Kalshi's book is bids-only per side; asks derive from the complement:
/// `yes_ask = 1 − best_no_bid`, `no_ask = 1 − best_yes_bid` (depth carries
/// over from the complementary bid level).
///
/// Returns `None` until the book is ready. Readiness used to be inferred by the
/// callers from "both asks are zero", which only worked because a missing ask
/// was itself defaulted to zero — the two bugs concealed each other. A ready
/// book with no sellers on either leg is real, tradeable data and must reach the
/// vipers; a book that has not arrived yet must not.
async fn build_snapshot(
    hub: &ws::BookHub,
    ticker: &str,
    raptors: &CryptoRaptors,
    market: &MarketConfig,
) -> Option<MarketSnapshot> {
    let (yes_state, no_state): (PriceState, PriceState) = {
        let books = hub.read().await;
        match books.get(ticker) {
            Some(b) if b.ready => {
                let now = Utc::now();
                // An absent level has to be filled in with the price that is
                // LEAST attractive to us, never the most. A missing bid is $0.00
                // (nobody is buying) and a missing ask is $1.00 (nobody is
                // selling below the full payout) — both are conservative, and
                // both make the leg unattractive rather than irresistible.
                //
                // Asks were previously defaulted to $0.00, which is the most
                // aggressive ask expressible. On a decided market with no NO
                // sellers that rendered as "Ask Sum $0.0100" — YES at a penny
                // plus NO at nothing, which reads as a guaranteed dollar for a
                // cent. Arbitrage did decline it, but only because the
                // locked/inverted-spread guard rejects `no_bid >= no_ask`
                // first; had it not, the safe-bid calculation would have
                // produced a bid of −$0.01. Polymarket US has always defaulted
                // asks to ONE (`venues/us/ws.rs`); this brings Kalshi in line.
                let (yb, ybd) = b.best_yes_bid().unwrap_or((dec!(0), dec!(0)));
                let (ya, yad) = b.best_yes_ask().unwrap_or((dec!(1), dec!(0)));
                let (nb, nbd) = b.best_no_bid().unwrap_or((dec!(0), dec!(0)));
                let (na, nad) = b.best_no_ask().unwrap_or((dec!(1), dec!(0)));
                // Cumulative depth alongside the touch. Nothing reads these yet;
                // they exist so order-book imbalance can be compared as a
                // whole-book ratio against the top-of-book one vipers gate on.
                let (ybd_all, yad_all) = (b.yes_bid_depth_total(), b.yes_ask_depth_total());
                let (nbd_all, nad_all) = (b.no_bid_depth_total(),  b.no_ask_depth_total());
                (
                    (yb, ybd, ya, yad, now, ybd_all, yad_all),
                    (nb, nbd, na, nad, now, nbd_all, nad_all),
                )
            }
            // Feed still connecting — no book to describe.
            _ => return None,
        }
    };

    let now = Utc::now();
    let mut snap = MarketSnapshot {
        yes_bid: yes_state.0, yes_bid_depth: yes_state.1, yes_ask: yes_state.2, yes_ask_depth: yes_state.3,
        no_bid:  no_state.0,  no_bid_depth:  no_state.1,  no_ask:  no_state.2,  no_ask_depth:  no_state.3,
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
    snap.oracle_price = *raptors.oracle.borrow();
    // Book-feed health, recorded wherever the snapshot is built so a dark feed
    // is reported rather than merely declined by every gate.
    crate::state::price_state::book_feed::note(
        ticker,
        crate::state::price_state::snapshot_has_book(&yes_state, &no_state),
    );
    crate::state::price_state::log_heartbeat(ticker, &yes_state, &no_state, snap.oracle_price);
    let (vel, vel_1s, accel) = *raptors.velocity.borrow();
    snap.velocity = vel;
    snap.velocity_1s = vel_1s;
    snap.acceleration = accel;
    let (drift_60m, drift_10m, hist_vol) = *raptors.drift.borrow();
    snap.oracle_drift_60m = drift_60m;
    snap.oracle_drift_10m = drift_10m;
    snap.hist_vol = hist_vol;
    snap.funding_rate = *raptors.funding.borrow();
    let deriv = raptors.derivatives.borrow().clone();
    snap.oi_delta_pct = deriv.oi_delta_pct;
    snap.cvd_ratio = deriv.cvd_ratio;
    if let Some(tide) = &raptors.tide {
        let t = tide.borrow().clone();
        snap.institutional_pulse = t.institutional_pulse;
        snap.tide_coherence = t.coherence;
    }
    if let Some(horizon) = &raptors.horizon {
        let h = horizon.borrow().clone();
        snap.tradfi_velocity = h.tradfi_velocity;
        snap.macro_coherence = h.macro_coherence;
        snap.vix_proxy = h.vix_proxy;
        snap.vix_velocity = h.vix_velocity;
    }
    Some(snap)
}

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

/// Record the position guard from what the venue actually filled.
///
/// Takes the `Fill` rather than the `OrderParams` on purpose. Booking the
/// REQUESTED size at the LIMIT price meant a partial fill left the bot believing
/// it held more than it did, at a cost basis it never paid — every downstream
/// exposure check, stop distance and P&L then worked from that number. Exits
/// were already protected (`record_round_trip` clamps to the guard, so it cannot
/// book shares we never owned), which is exactly why this stayed invisible: the
/// error was confined to the entry side, where nothing contradicted it.
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
            paired_leg_token_id: paired.cloned(),
            entry_fee: fill.fee,
        },
    );
}

async fn dispatch_signal(
    // Squadron whose positions these keys address, so two squadrons
    // holding the same token stay independent.
    squadron_id: &str,
    venue: &KalshiVenue,
    pool: &Option<sqlx::SqlitePool>,
    scope: &TradeScope,
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
                    lifecycle.track(&a, strategy_name, params.order_type, Some(pp.token_id.clone())).await;
                    lifecycle.track(&b, strategy_name, pp.order_type, Some(params.token_id.clone())).await;
                    record_entry(squadron_id, pool, scope, strategy_name, params, &a).await;
                    record_entry(squadron_id, pool, scope, strategy_name, pp, &b).await;
                    if let Some(p) = pool { sync_dashboard(squadron_id, venue, p, positions, starting).await; }
                    true
                }
                Err(e) => { warn!("[{strategy_name}] atomic entry failed: {e}"); false }
            }
        }
        StrategySignal::Entry { params, pair_params: None } => {
            dispatch_single(squadron_id, venue, pool, scope, positions, lifecycle, strategy_name, params, Side::Buy, starting)
                .await
                .is_some()
        }
        StrategySignal::MakerQuote { yes, no } => {
            let mut acted = false;
            for q in [yes.as_ref(), no.as_ref()].into_iter().flatten() {
                if dispatch_single(squadron_id, venue, pool, scope, positions, lifecycle, strategy_name, q, Side::Buy, starting)
                    .await
                    .is_some()
                {
                    acted = true;
                }
            }
            acted
        }
        StrategySignal::MakerCancel { tokens } => {
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
            // An exit that didn't fill will re-signal on the very next 50ms tick
            // (the SL condition is still true), so back off between attempts
            // rather than hammering the order endpoint 20×/sec.
            if exit_retry_backed_off(strategy_name, params.token_id.as_str()) {
                return false;
            }
            info!("🚪 [{strategy_name}] exit ({reason}): {} @ {:.4}", params.token_id, params.price);
            // Snapshot the entry BEFORE the sell so the round-trip can be booked
            // to the tradelog — the position is dropped from the map below and
            // there is no second chance to read its cost basis.
            let entered = positions
                .lock()
                .await
                .get(&PositionKey::new(squadron_id, strategy_name, params.token_id.clone()))
                .map(|p| (p.avg_entry, p.shares, p.entry_fee));
            let exit_fill = dispatch_single(squadron_id, venue, pool, scope, positions, lifecycle, strategy_name, params, Side::Sell, starting).await;
            let Some(exit_fill) = exit_fill else {
                // Nothing traded — we still own it. Dropping the position here is
                // what abandoned a live stop-loss to expire worthless on
                // 2026-08-10 (KXBTCD-26AUG1003): local state said flat, Kalshi
                // said long. Keep it and let the next attempt (or lifecycle
                // reconciliation) close it.
                warn!("↩️ [{strategy_name}] exit did NOT execute on {} — position retained, will retry",
                    params.token_id);
                arm_exit_retry_backoff(strategy_name, params.token_id.as_str());
                return false;
            };
            record_round_trip(pool, scope, strategy_name, params, entered, exit_fill.fee, reason).await;
            clear_exit_retry_backoff(strategy_name, params.token_id.as_str());
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
            true
        }
        StrategySignal::NoSignal => false,
    }
}

/// Minimum gap between retries of an exit that returned no fill.
const EXIT_RETRY_BACKOFF_SECS: u64 = 5;

fn exit_retry_backoff() -> &'static std::sync::Mutex<HashMap<String, std::time::Instant>> {
    static B: std::sync::OnceLock<std::sync::Mutex<HashMap<String, std::time::Instant>>> =
        std::sync::OnceLock::new();
    B.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn exit_retry_backed_off(strategy_name: &str, token_id: &str) -> bool {
    let map = exit_retry_backoff();
    let guard = match map.lock() { Ok(g) => g, Err(p) => p.into_inner() };
    guard
        .get(&format!("{strategy_name}:{token_id}"))
        .is_some_and(|t| t.elapsed().as_secs() < EXIT_RETRY_BACKOFF_SECS)
}

fn arm_exit_retry_backoff(strategy_name: &str, token_id: &str) {
    let map = exit_retry_backoff();
    let mut guard = match map.lock() { Ok(g) => g, Err(p) => p.into_inner() };
    guard.insert(format!("{strategy_name}:{token_id}"), std::time::Instant::now());
}

fn clear_exit_retry_backoff(strategy_name: &str, token_id: &str) {
    let map = exit_retry_backoff();
    let mut guard = match map.lock() { Ok(g) => g, Err(p) => p.into_inner() };
    guard.remove(&format!("{strategy_name}:{token_id}"));
}

async fn dispatch_single(
    // Squadron whose positions these guards belong to.
    squadron_id: &str,
    venue: &KalshiVenue,
    pool: &Option<sqlx::SqlitePool>,
    scope: &TradeScope,
    positions: &Arc<Mutex<PositionMap>>,
    lifecycle: &OrderLifecycle,
    strategy_name: &str,
    params: &OrderParams,
    side: Side,
    starting: Decimal,
) -> Option<Fill> {
    if params.ghost_mode {
        info!("👻 [{strategy_name}] ghost {side:?}: {} @ {:.4} × {:.2}",
            params.token_id, params.price, params.shares);
        // Ghost orders never touch the venue, so there is no fee to book — and
        // nothing partially fills, so the requested size IS the fill.
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
        return Some(ghost);
    }
    match venue.place_order(order_params_to_intent(params, side)).await {
        Ok(f) => {
            // A 2xx is not a fill. Kalshi answers 200 with `fill_count: 0` for an
            // order that crossed nothing, and the old code booked that as done —
            // phantom positions on entry, abandoned live ones on exit.
            if f.filled <= Decimal::ZERO {
                warn!("⚠️ [{strategy_name}] {side:?} {} @ {:.4} filled 0 of {:.2} — no state change (order {})",
                    params.token_id, params.price, params.shares, f.order_id);
                return None;
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
            if let Some(p) = pool { sync_dashboard(squadron_id, venue, p, positions, starting).await; }
            Some(f)
        }
        Err(e) => { warn!("[{strategy_name}] {side:?} order failed: {e}"); None }
    }
}

/// Persist a new position to `entries` + `open_positions`.
///
/// Without this the Control Tower's positions panel had to fall back on
/// [`sync_dashboard`]'s venue sweep, which knows the ticker but not which viper
/// owns it — every live position was mislabelled as ArbitrageStrategy.
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
/// The Kalshi loop previously recorded a trade only on a lifecycle flatten, so
/// every ordinary TP/SL/bail exit moved real cash and left no ledger row: the
/// tradelog stayed empty while collateral moved (observed 2026-08-10, a
/// FairValue round-trip that realised −$1.24 with zero trades in the DB).
///
/// `entered` is the (avg_entry, shares) read before the exit order was placed;
/// `None` means no guard existed for this token, in which case there is no
/// verifiable cost basis and we log loudly rather than invent P&L.
#[allow(clippy::too_many_arguments)]
async fn record_round_trip(
    pool: &Option<sqlx::SqlitePool>,
    scope: &TradeScope,
    strategy_name: &str,
    params: &OrderParams,
    entered: Option<(Decimal, Decimal, Decimal)>,
    exit_fee: Decimal,
    reason: &str,
) {
    let Some((avg_entry, shares, entry_fee)) = entered else {
        warn!("⚠️ [{strategy_name}] exit booked for {} with no tracked entry — no trade recorded",
            params.token_id);
        return;
    };
    // Exit sizing follows the guard, not the signal: a partially-filled entry
    // must not book P&L on shares we never owned.
    let shares = shares.min(params.shares).max(Decimal::ZERO);
    // Net of BOTH legs' fees. Kalshi's quadratic taker fee runs ~7% of notional
    // per round trip on a mid-priced contract, against a 20% TP / 10% SL — so a
    // gross figure systematically flatters every trade. Measured over the five
    // round trips on 2026-08-10: gross −$0.07, fees −$1.05, actual −$1.12.
    let fees = entry_fee + exit_fee;
    let gross = (params.price - avg_entry) * shares;
    let pnl = gross - fees;
    if !fees.is_zero() {
        info!("🧾 [{strategy_name}] round trip {}: gross ${:.4} − fees ${:.4} (entry ${:.4} + exit ${:.4}) = ${:.4}",
            params.token_id, gross, fees, entry_fee, exit_fee, pnl);
    }
    metrics::record_trade(
        scope,
        fees,
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

async fn sync_dashboard(
    // Squadron whose guards are summarised for the dashboard.
    squadron_id: &str,
    venue: &KalshiVenue,
    pool: &sqlx::SqlitePool,
    guards: &Arc<Mutex<PositionMap>>,
    starting: Decimal,
) -> (Decimal, Decimal) {
    let collateral = match venue.collateral().await {
        Ok(c) => c,
        Err(e) => { warn!("Kalshi dashboard sync: collateral query failed: {e}"); return (Decimal::ZERO, starting); }
    };
    let positions = venue.positions().await.unwrap_or_default();

    // token → (owning viper, market name) from the live guard map, so a venue
    // holding is attributed to the viper that opened it. Holdings with no guard
    // (prior session, manual trade) are adopted under a neutral label rather
    // than being blamed on a viper that never traded them.
    let owners: HashMap<String, (String, String)> = {
        let map = guards.lock().await;
        map.iter()
            .filter(|(k, _)| k.squadron == squadron_id)
            .map(|(k, p)| (k.market.as_str().to_string(), (k.strategy.clone(), p.market_name.clone())))
            .collect()
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
    let _ = db::purge_stale_open_positions(pool, &live_ids, &std::collections::HashMap::new()).await;

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

/// `YES`/`NO` display label from a `{ticker}#yes|#no` leg id.
fn side_label(symbol: &str) -> &'static str {
    if symbol.ends_with("#no") { "NO" } else { "YES" }
}

async fn wait_or_cancel(cancel: &CancellationToken, secs: u64) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(Duration::from_secs(secs)) => false,
    }
}

/// Assemble and register the Kalshi squadron with the full Raptor stack.
/// "politics" -> "Politics". Squadron names are shown in the UI.
fn title_case(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

fn register_kalshi_squadron(
    cag: &Cag,
    pair: &KalshiPair,
    r: &CryptoRaptors,
    deployed_as: Option<&str>,
    squadron_name: Option<&str>,
    sports_rx: &watch::Receiver<SportsSnapshot>,
    tennis_rx: &watch::Receiver<TennisSnapshot>,
    // The token the trade loop selects on, so a stand-down actually stops it.
    cancel: &CancellationToken,
) -> Squadron {
    let raptors = SquadronRaptors::full(
        r.oracle.clone(),
        r.velocity.clone(),
        r.drift.clone(),
        r.funding.clone(),
        r.derivatives.clone(),
        r.tide.clone(),
        r.horizon.clone(),
        // The sports feed reaches every Kalshi squadron now. It used to be None
        // with the note "no sports feed on the Kalshi crypto squadron" — true
        // when crypto was the only one, but the venue now runs a sports squadron
        // and the taxonomy links `sports` to this raptor, so the Control Tower
        // listed it while nothing fed the channel.
        Some(sports_rx.clone()),
    );
    let mut raptors = raptors;
    raptors.tennis = Some(tennis_rx.clone());

    let market = MarketConfig {
        yes_token: pair.long.clone(),
        no_token: pair.short.clone(),
        market_name: pair.question.clone(),
        // None: the squadron id derives from `{asset}-{cadence}` and is the
        // operator-config persistence key (same rationale as the US wings).
        market_close_time: None,
        strike_price: pair.strike,
        is_neg_risk: false,
        condition_id: String::new(),
        yes_fee_bps: KALSHI_FEE_BPS as u32,
        no_fee_bps: KALSHI_FEE_BPS as u32,
    };

    // Use the actual crypto underlying (Btc/Eth/Sol) so the squadron
    // self-classifies as "crypto" and links to the full raptor/viper set.
    //
    // An operator-deployed market has no underlying, so it takes a Custom asset
    // named for its class. classify_and_link() passes an empty category for
    // Custom, which is what makes the taxonomy fall through to its market-name
    // rules — so a politics market classifies as politics and picks up that
    // class's vipers rather than inheriting crypto's.
    let crypto_asset = match (deployed_as, pair.underlying) {
        (Some(class), _) => CryptoAsset::Custom(class.to_string()),
        (None, "eth") => CryptoAsset::Eth,
        (None, "sol") => CryptoAsset::Sol,
        (None, _)     => CryptoAsset::Btc,
    };
    // What the operator sees. Their own name wins when they gave one.
    let display_name = match (squadron_name, deployed_as) {
        (Some(n), _) if !n.trim().is_empty() => n.trim().to_string(),
        (_, Some(class)) => format!("Kalshi {} Squadron", title_case(class)),
        (_, None) => "Kalshi Crypto Squadron".to_string(),
    };
    let squadron = Squadron::new_named(
        crypto_asset,
        SquadronConfig::arb_wing(display_name),
        market,
        raptors,
        None,
        squadron_name,
    );
    cag.register_with_cancel(&squadron, cancel.clone());
    squadron
}

async fn seed_squadron_config(squadron_id: &str) {
    if let Some(pool) = db::pool() {
        if db::squadron_config_get(pool, squadron_id).await.is_none() {
            DynamicConfig::init_for_squadron(squadron_id).await;
        }
    }
}

fn publish_raptor_health(
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

fn publish_strategy_market(
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mkt(ticker: &str, title: &str, sub: &str, vol: &str) -> KalshiMarket {
        KalshiMarket {
            ticker: ticker.to_string(),
            title: title.to_string(),
            yes_sub_title: sub.to_string(),
            status: "active".to_string(),
            strike_type: "greater".to_string(),
            floor_strike: Some(65000.0),
            close_time: "2026-08-08T18:00:00Z".to_string(),
            volume_fp: vol.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn pair_from_market_maps_legs_and_underlying() {
        let p = pair_from_market(&mkt("KXBTC15M-26AUG081400-00", "BTC 2pm?", "$65,000 or above", "1234.00")).unwrap();
        assert_eq!(p.long.as_str(), "KXBTC15M-26AUG081400-00#yes");
        assert_eq!(p.short.as_str(), "KXBTC15M-26AUG081400-00#no");
        assert_eq!(p.underlying, "btc");
        assert_eq!(p.question, "BTC 2pm? — $65,000 or above");
        assert!((p.volume - 1234.0).abs() < 1e-9);
        assert_eq!(p.strike, Some(rust_decimal_macros::dec!(65000)));
        assert!(p.close_time.is_some());
        assert_eq!(p.cadence, Some(Cadence::FifteenMin));
    }

    #[test]
    fn cadence_detection() {
        assert_eq!(Cadence::from_ticker("KXBTC15M-26AUG081400-00"), Some(Cadence::FifteenMin));
        assert_eq!(Cadence::from_ticker("KXBTCD-28JAN1222-T99799.99"), Some(Cadence::Daily));
        assert_eq!(Cadence::from_ticker("KXETH15M-X"), Some(Cadence::FifteenMin));
        assert_eq!(Cadence::from_ticker("KXETHD-26AUG09"), Some(Cadence::Daily));
        assert_eq!(Cadence::from_ticker("NONSENSE"), None);
    }

    #[test]
    fn non_crypto_series_rejected() {
        assert!(pair_from_market(&mkt("KXNBA-FINALS", "NBA champ", "", "0")).is_none());
        assert_eq!(detect_underlying("KXETH15M-X"), Some("eth"));
        assert_eq!(detect_underlying("KXDOGE15M-X"), Some("doge"));
        assert_eq!(detect_underlying("FED-23DEC"), None);
    }

    #[test]
    fn series_env_parsing() {
        // Default when unset.
        std::env::remove_var(ENV_SERIES);
        let s = configured_series();
        assert!(s.contains(&"KXBTC15M".to_string()));
        assert!(s.contains(&"KXETHD".to_string()));
    }
}


#[cfg(test)]
mod fill_accounting_tests {
    use super::*;

    fn params(shares: Decimal, limit: Decimal) -> OrderParams {
        OrderParams {
            token_id: MarketId::new("KX-1#yes"),
            price: limit,
            shares,
            fee_bps: 0,
            is_neg_risk: false,
            market_name: "m".into(),
            condition_id: String::new(),
            order_type: crate::venues::core::TimeInForce::Fak,
            post_only: false,
            ghost_mode: false,
        }
    }

    fn fill(filled: Decimal, price: Decimal, fee: Decimal) -> Fill {
        Fill {
            order_id: OrderId("o1".into()),
            market: MarketId::new("KX-1#yes"),
            filled,
            price,
            fee,
        }
    }

    /// The bug: a partial fill booked the REQUESTED size at the LIMIT price, so
    /// the bot believed it held more than it did at a basis it never paid. Every
    /// downstream exposure check and stop distance then worked from that number.
    #[tokio::test]
    async fn a_partial_fill_books_only_what_filled() {
        let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        let p = params(dec!(100), dec!(0.55));
        // Asked for 100 at a 0.55 limit; got 30 at 0.52.
        record_guard("test-squadron", &positions, "MakerStrategy", &p, None, &fill(dec!(30), dec!(0.52), dec!(0.1))).await;

        let map = positions.lock().await;
        let pos = map.get(&PositionKey::new("test-squadron", "MakerStrategy", p.token_id.clone())).expect("guard recorded");
        assert_eq!(pos.shares, dec!(30), "booked the requested size, not the fill");
        assert_eq!(pos.avg_entry, dec!(0.52), "booked the limit price, not the fill price");
        assert_eq!(pos.entry_fee, dec!(0.1));
    }

    /// A full fill must be unchanged — this fix must not move the common case.
    #[tokio::test]
    async fn a_full_fill_is_booked_exactly() {
        let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        let p = params(dec!(50), dec!(0.40));
        record_guard("test-squadron", &positions, "ArbitrageStrategy", &p, None, &fill(dec!(50), dec!(0.40), dec!(0))).await;

        let map = positions.lock().await;
        let pos = map.get(&PositionKey::new("test-squadron", "ArbitrageStrategy", p.token_id.clone())).expect("guard recorded");
        assert_eq!(pos.shares, dec!(50));
        assert_eq!(pos.avg_entry, dec!(0.40));
    }

    /// The paired leg carries its own fill: legs of an atomic pair can fill
    /// differently, and booking both from one leg's fill would misstate the hedge.
    #[tokio::test]
    async fn each_leg_of_a_pair_books_its_own_fill() {
        let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
        let yes = params(dec!(20), dec!(0.45));
        let mut no = params(dec!(20), dec!(0.52));
        no.token_id = MarketId::new("KX-1#no");

        record_guard("test-squadron", &positions, "ArbitrageStrategy", &yes, Some(&no.token_id), &fill(dec!(20), dec!(0.45), dec!(0))).await;
        record_guard("test-squadron", &positions, "ArbitrageStrategy", &no, Some(&yes.token_id), &fill(dec!(12), dec!(0.50), dec!(0))).await;

        let map = positions.lock().await;
        let a = map.get(&PositionKey::new("test-squadron", "ArbitrageStrategy", yes.token_id.clone())).unwrap();
        let b = map.get(&PositionKey::new("test-squadron", "ArbitrageStrategy", no.token_id.clone())).unwrap();
        assert_eq!(a.shares, dec!(20));
        assert_eq!(b.shares, dec!(12), "the short leg's own partial fill was lost");
        assert_eq!(b.avg_entry, dec!(0.50));
    }
}

#[cfg(test)]
mod deployed_market_tests {
    use super::*;
    use crate::venues::kalshi::types::KalshiMarket;

    fn market(ticker: &str, title: &str) -> KalshiMarket {
        KalshiMarket { ticker: ticker.to_string(), title: title.to_string(), ..Default::default() }
    }

    /// pair_from_market derives the underlying from the ticker prefix and
    /// returns None when there is none. That is right for the rotation loop,
    /// which only trades the configured crypto series — but it also meant an
    /// operator-deployed politics or sports market could not be represented at
    /// all, which is why deploying one produced nothing.
    #[test]
    fn a_politics_market_has_no_pair_until_it_is_untethered() {
        let m = market("KXCITRINI-28JUL01", "Who will win the Citrini Prize?");
        assert!(pair_from_market(&m).is_none(), "test premise broken");

        let pair = pair_from_market_untethered(&m);
        assert!(!pair.is_crypto());
        assert_eq!(pair.ticker, "KXCITRINI-28JUL01");
        assert_eq!(pair.question, "Who will win the Citrini Prize?");
        assert_eq!(pair.long.as_str(), leg_id("KXCITRINI-28JUL01", true));
        assert_eq!(pair.short.as_str(), leg_id("KXCITRINI-28JUL01", false));
    }

    /// The crypto path must be untouched: a BTC market still reports its
    /// underlying, so it keeps its real Raptor stack and its crypto viper set.
    #[test]
    fn a_crypto_market_still_reports_its_underlying() {
        let m = market("KXBTCD-26AUG2218-T77099.99", "Bitcoin price on Aug 22");
        let pair = pair_from_market(&m).expect("crypto market must still pair");
        assert!(pair.is_crypto());
        assert_eq!(pair.underlying, "btc");

        // And the untethered constructor is only for markets that need it — it
        // deliberately drops the underlying even on a crypto ticker, so it must
        // never be reached from the rotation path.
        assert!(!pair_from_market_untethered(&m).is_crypto());
    }

    /// The question is built the same way on both paths, so a deployed squadron
    /// is labelled in the UI exactly as the rotation loop would label it.
    #[test]
    fn the_subtitle_is_appended_to_the_title() {
        let mut m = market("KXNBA-26", "Who wins the 2027 Final?");
        m.yes_sub_title = "Philadelphia".to_string();
        assert_eq!(
            pair_from_market_untethered(&m).question,
            "Who wins the 2027 Final? — Philadelphia",
        );
    }
}
