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
use rust_decimal::prelude::ToPrimitive;
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
use crate::squadron::game_over::{sports_game_over_due, sports_game_over_verdict, GameOver, SportsBook, SportsGameSignals};
use crate::orchestrator::{
    aggregate_and_resolve_signals, evaluate_strategies, StrategyContext,
    StrategyRegistry,
};
use crate::raptors::derivatives::DerivativesSnapshot;
use crate::raptors::horizon::HorizonSnapshot;
use crate::raptors::tennis::TennisSnapshot;
use crate::raptors::tide::TideSnapshot;
use crate::squadron::{CryptoAsset, Squadron, SquadronConfig, SquadronRaptors, SquadronState};
use crate::state::{
    MarketConfig, MarketPhase, MarketSnapshot, OrderParams, Position, PositionMap, PriceState,
    StrategySignal, TradeScope, PositionKey};
use crate::venues::core::{Execution, Fill, MarketId, OrderId, OrderIntent, Side, TimeInForce};
use crate::venues::lifecycle::{acknowledged_resting, LifecycleConfig, OrderLifecycle};

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
/// Process-wide backoff on settlement lookups that keep answering `Unknown`.
///
/// `sync_dashboard` runs every [`DASHBOARD_SYNC_SECS`] AND after every entry
/// and dispatched fill, and every Kalshi squadron on the shared shard sweeps
/// the same pool-wide stale-row set — so without this gate one unanswerable
/// row (closed-but-undetermined, voided, REST outage) cost ~2,880 live API
/// calls over its 24h defer window, times seven squadrons, from inside the
/// trading loop. See [`crate::venues::core::SettlementProbeGate`] for the
/// policy and why in-memory state is safe here.
static SETTLEMENT_PROBES: std::sync::LazyLock<crate::venues::core::SettlementProbeGate> =
    std::sync::LazyLock::new(crate::venues::core::SettlementProbeGate::new);
/// Skip markets closing within this window — not worth committing capital.
const MIN_TIME_TO_CLOSE_SECS: i64 = 180; // 3 min (15-min cadence needs headroom)
/// Maximum time-to-close for short-cadence markets (15M): 2 hours.
const MAX_CLOSE_SHORT_SECS: i64 = 7_200;
/// Maximum time-to-close for daily markets: 36 hours.
const MAX_CLOSE_DAILY_SECS: i64 = 129_600;
const MARKET_RTB_WINDOW_SECS: i64 = 60;
const LIFECYCLE_SYNC_SECS: u64 = 10;
const MARKET_RESCAN_SECS: u64 = 120;
/// How often a pinned market asks the venue whether it still trades. Kalshi
/// dates a game market's `close_time` at its LATEST expiration, two days after
/// the game, and finalizes it a few hours after the final; this probe is how
/// the loop hears about that. Same cadence as the intl patrol's venue probe.
const VENUE_STATUS_POLL_SECS: u64 = 60;
/// Bound on the probe's REST call. It is awaited inline in the tick body, so a
/// hung request would stall every exit and the operator-cancel arm for this
/// squadron; the shared client has no timeout of its own (the balance fetch
/// bounds itself the same way). A timeout is "no answer", nothing more.
const VENUE_STATUS_TIMEOUT_SECS: u64 = 5;
/// Rotate only when a candidate has this much more volume (contracts).
const ROTATION_VOLUME_THRESHOLD: f64 = 5_000.0;

/// How far into an hourly window the loop may still move to a hotter strike of
/// the same cadence. After this the strike is held to the close.
///
/// Kalshi lists a ladder of strikes per hour and volume moves across it as spot
/// moves, so following volume meant a hop every rescan or two: on 2026-10-20
/// production moved about a dozen times in seventy minutes. Every hop tears the
/// squadron down and rebuilds it, restarting the per-market warmups Momentum,
/// Convergence and Trend Reversal run on a new market, and the hourly recorder, which samples mids only while the squadron sits on a
/// market and writes at the close, kept a median of five minutes of mids per
/// hour, under what a training row needs. The first minutes are left free
/// because at the top of the hour every strike is empty and the first pick is a
/// guess (19:00:10 that day it was an ETH market with twenty contracts); five
/// minutes is GBoost's first decision minute by default, so a strike is normally
/// settled before the viper scores it (the pin lands at the first rescan after,
/// so by minute seven). The pin holds only against the market's own ladder: a
/// market of another underlying is still judged by volume, so that ETH pick
/// gives way once a BTC strike is clearly busier, whatever the minute.
const HOURLY_SETTLE_SECS: i64 = 300;
/// Conservative quadratic-fee ceiling (1.75¢/contract at P=0.5) in bps.
const KALSHI_FEE_BPS: u16 = 175;
/// SQLite shard key for this venue. A *storage* identity — every Kalshi
/// squadron writes here regardless of underlying, so it must not be shown as
/// the market's asset. Use `KALSHI_VENUE` / `TradeScope` for that.
pub const KALSHI_ASSET: &str = "kalshi";
/// Runtime venue identity persisted on every trade and entry row.
pub const KALSHI_VENUE: &str = "kalshi";

/// Market cadence: from the market's own open and close when the venue
/// publishes both (`from_window`), else from the series ticker (`from_ticker`).
///
/// The ticker alone misread Kalshi's `KXBTCD` series as daily. Kalshi lists it
/// as "Bitcoin price Above/below, hourly": one-hour markets on a strike ladder,
/// opened each hour, beside a 25-hour daily on the same series. The hour-shaped
/// Vipers (GBoost, Momentum, Time Decay, Convergence) are trained for exactly
/// that window, and until 2026-10-17 never saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Cadence {
    /// One-hour markets (`KXBTCD`'s hourly above/below listings).
    Hourly,
    /// 15-minute markets (`KXBTC15M`).
    FifteenMin,
    /// Daily markets (`KXBTCD`'s daily listing).
    Daily,
}

impl Cadence {
    /// The window length this cadence's markets trade over, in seconds.
    pub fn window_secs(self) -> i64 {
        match self { Self::Hourly => 3600, Self::FifteenMin => 900, Self::Daily => 86_400 }
    }

    /// Cadence from the published window length, with a little slack for a
    /// venue that opens a market a few seconds off the clock. Anything between
    /// the three shapes is `None`: better unclassified than misfiled.
    pub fn from_window(window_secs: i64) -> Option<Self> {
        match window_secs {
            840..=960 => Some(Self::FifteenMin),
            3_480..=3_720 => Some(Self::Hourly),
            s if s >= 72_000 => Some(Self::Daily),
            _ => None,
        }
    }

    /// Infer cadence from a market ticker string, for a market whose open time
    /// the venue did not publish.
    fn from_ticker(ticker: &str) -> Option<Self> {
        let t = ticker.to_ascii_uppercase();
        if t.contains("15M") { return Some(Self::FifteenMin); }
        if t.contains("D-") || t.ends_with('D') { return Some(Self::Daily); }
        None
    }

    /// Which cadence the hour-shaped Vipers want first: the one-hour market, then
    /// the 15-minute one, then whatever is left.
    fn primary_rank(this: Option<Self>) -> u8 {
        match this { Some(Self::Hourly) => 0, Some(Self::FifteenMin) => 1, Some(Self::Daily) => 2, None => 3 }
    }

    /// Maximum time-to-close for this cadence. Markets further out are
    /// filtered from discovery — they're either far-future listings (demo)
    /// or long-dated events where intraday strategies don't apply.
    fn max_close_secs(self) -> i64 {
        match self {
            Self::Hourly     => MAX_CLOSE_SHORT_SECS,
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
    /// Close minus open as the venue published them; `None` when it did not
    /// publish an open. This, not the cadence, is what `MarketConfig::window_secs`
    /// carries, so a model that scores one window length reads the real one.
    pub window_secs: Option<i64>,
}

/// Close minus open, when both are published and the order makes sense.
fn window_of(m: &KalshiMarket) -> Option<i64> {
    let (open, close) = (m.open_time_utc()?, m.close_time_utc()?);
    let secs = (close - open).num_seconds();
    (secs > 0).then_some(secs)
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
        cadence: window_of(m).and_then(Cadence::from_window).or_else(|| Cadence::from_ticker(&m.ticker)),
        window_secs: window_of(m),
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
        cadence: window_of(m).and_then(Cadence::from_window).or_else(|| Cadence::from_ticker(&m.ticker)),
        window_secs: window_of(m),
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
    /// a hotter market appears, except that an hourly market is held to its
    /// close once it is `HOURLY_SETTLE_SECS` into its window (`hourly_pinned`).
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
    /// its last value after the sender goes, which is exactly the behavior
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
            // Kalshi's crypto series settle on CF Benchmarks' real-time indices
            // (BRTI for bitcoin), quoted in US dollars, so the level the vipers
            // compare against a strike must be in dollars too. Binance quotes
            // in USDT; see `MarkDenomination::Usd` for the 2026-10-18 loss the
            // USDT level caused. `venues::settlement_denomination` says Usd here.
            crate::raptors::price::run_price_raptor(
                asset.clone(), crate::venues::settlement_denomination(),
                oracle_tx.clone(), velocity_tx.clone(), drift_tx.clone(),
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

/// What the venue's own market record says about trading on it.
///
/// `Some(true)`: trading. `Some(false)`: over — closed to trading, finalized or
/// settled, or carrying a result, which Kalshi records only once the market is
/// decided. `None`: no usable answer, in which case the stated close keeps its
/// authority exactly as before. A halt (`inactive`, `paused`: a review, an
/// injury hold, a data problem) is deliberately no answer and NOT "over": a
/// live game can be halted for longer than the retirement grace, and a flat
/// squadron that retired on it would lose the rest of the game, since the
/// seeder never redeploys onto a game that is off the board. Pre-open statuses
/// and anything this build does not know are no answer for the same reason.
/// Reads the same two fields as `interpret_settlement` (and the status list in
/// `orders.rs`) but asks a different question: not "who won" but "is there
/// anything left to trade".
pub(crate) fn venue_accepting_orders(status: &str, result: &str) -> Option<bool> {
    if !result.is_empty() {
        return Some(false);
    }
    match status.trim() {
        "active" | "open" => Some(true),
        "closed" | "finalized" | "settled" | "determined" => Some(false),
        "inactive" | "paused" | "initialized" | "unopened" => None,
        _ => None,
    }
}

/// The final YES price a Kalshi `result` states, or `None` when it states no
/// binary result yet. `yes` and `no` are the only values a $1/$0 settlement can
/// be booked from; anything else (empty, `void`, a scalar) is not a result for
/// this purpose and defers, as `interpret_settlement` does for live positions.
pub(crate) fn kalshi_result_yes_price(result: &str) -> Option<Decimal> {
    match result.trim() {
        "yes" => Some(Decimal::ONE),
        "no" => Some(Decimal::ZERO),
        _ => None,
    }
}

/// Should a pinned squadron stand down on the venue's word? Only once the venue
/// has said "not trading" continuously for the grace, and never while holding:
/// the exchange settles what we hold, and standing down on top of it would
/// strand the position with no viper to book its exit.
pub(crate) fn venue_closed_retire_due(closed_for_secs: Option<i64>, grace_secs: i64, holding: bool) -> bool {
    !holding && closed_for_secs.is_some_and(|s| s >= grace_secs)
}

/// The pinned market's book as the game-over rule reads it: whether any level
/// exists on either leg, and the best bid across both. `(false, 0)` while the
/// feed has no book yet, which the rule treats as absent. Kalshi books are bids
/// only and an ask is the mirrored opposite bid, so a level on either side shows
/// up as a bid here; this is `snapshot_has_book`'s rule in those terms.
async fn book_state(hub: &ws::BookHub, ticker: &str) -> (bool, Decimal) {
    let books = hub.read().await;
    match books.get(ticker) {
        Some(b) if b.ready => {
            let yb = b.best_yes_bid().map_or(Decimal::ZERO, |(p, _)| p);
            let nb = b.best_no_bid().map_or(Decimal::ZERO, |(p, _)| p);
            (yb > Decimal::ZERO || nb > Decimal::ZERO, yb.max(nb))
        }
        _ => (false, Decimal::ZERO),
    }
}

/// Clears the per-market registries when a market loop ends, however it ends.
///
/// `build_snapshot` records the book feed under each ticker it streams, and the
/// vipers record their verdicts under the squadron's status scope. Neither
/// registry knew when a pinned Kalshi market was over: the only `forget` on
/// this venue ran on crypto rotation and passed the asset, which never matched
/// a ticker. Observed 2026-10-20, the first night 1.4.1 retired a finished
/// game: the squadron was reaped at 23:38 ET and the Control Tower still showed
/// "market data has stopped" for its ticker four hours later, with four
/// "stale" sports vipers in the health strip for a squadron no longer on screen.
///
/// A `Drop` guard so every exit is covered — close, rotation, cancellation —
/// without a call at each `return`.
struct MarketRegistries {
    tickers: Vec<String>,
    /// `None` leaves the viper rows alone: a rotating crypto squadron keeps its
    /// scope across markets (the rotation code forgets it when the underlying
    /// changes), and Helm squadrons share one scope, so one finishing must not
    /// wipe the others' rows.
    status_scope: Option<String>,
    /// The `/api/status` `strategy_markets` map and the squadron id whose
    /// entries leave with the market. Same rule as `status_scope`: only a pinned
    /// squadron's own entries, since a rotating one overwrites its own keys on
    /// the next market. The third registry the 1.4.2 fix missed: seven hours
    /// after `sports-open` was reaped on 2026-10-23 the status page still listed
    /// its vipers on the finished game.
    markets: Option<(Arc<watch::Sender<HashMap<String, String>>>, String)>,
}

impl MarketRegistries {
    fn new(
        tickers: &[String],
        mandate: MarketMandate,
        status_scope: &str,
        markets: Option<(&Arc<watch::Sender<HashMap<String, String>>>, &str)>,
    ) -> Self {
        let own_scope = mandate == MarketMandate::Pinned
            && status_scope != crate::vipers::helm_impl::KIND;
        Self {
            tickers: tickers.to_vec(),
            status_scope: own_scope.then(|| status_scope.to_string()),
            markets: if own_scope { markets.map(|(tx, id)| (Arc::clone(tx), id.to_string())) } else { None },
        }
    }
}

impl Drop for MarketRegistries {
    fn drop(&mut self) {
        for t in &self.tickers {
            crate::state::price_state::book_feed::forget(t);
        }
        if let Some(scope) = &self.status_scope {
            crate::helpers::viper_status::forget(scope);
        }
        if let Some((tx, squadron_id)) = &self.markets {
            tx.send_modify(|map| crate::state::forget_squadron_markets(map, squadron_id));
        }
    }
}

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
    tennis_rx: watch::Receiver<TennisSnapshot>,
    cancel: CancellationToken,
) {
    let filter = std::env::var(ENV_MARKET_FILTER).ok().filter(|s| !s.is_empty());
    let series = configured_series();
    info!("🏛️ Kalshi trader starting — series={series:?} filter={filter:?}");

    // Register a session with the CAG so the venue-neutral API handlers work.
    //
    // `Cag::set_session` was called in exactly one place, inside main.rs's
    // `intl_clob` block, so on this venue `Cag::session()` was always `None` and
    // every Helm endpoint answered "no venue session is registered yet — the
    // engine is still starting; retry in a few seconds", permanently. The
    // `Execution` implementations Helm reads the book through were wired and
    // unit-tested here but unreachable.
    //
    // `Cag::session()` returns any registered session and Helm uses only its
    // `venue`, which is one object per process, so a single registration is
    // enough. The balance is a starting figure the API refreshes.
    {
        let startup_balance = venue.collateral().await.unwrap_or_default();
        cag.set_session(crate::cag::SessionState::new(
            startup_balance,
            KALSHI_ASSET,
            Arc::clone(&venue),
        ));
    }

    crate::venues::cancel_leftover_orders_at_startup(venue.as_ref(), crate::helpers::dynamic_config::ghosting_now()).await;

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

        // A retired instance selects no market: its replacement owns the
        // account (helpers::migration).
        if crate::helpers::migration::is_retired() {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
            }
            continue;
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
            // The venue picked this market, so no operator budget applies.
            &Default::default(),
            selection,
            MarketMandate::Rotating,
            None,
            None,
            Some(&mut last_rotation_squadron),
            &tennis_rx,
            None,
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
    tennis_rx: watch::Receiver<TennisSnapshot>,
}

#[async_trait::async_trait]
impl crate::venues::deployment::DeploymentRunner for KalshiDeploymentRunner {
    fn venue_label(&self) -> &'static str { "Kalshi" }

    async fn run_pinned(
        &self,
        dep: &crate::helpers::db::PendingDeployment,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let market_id = dep.market_id.as_str();
        let class = dep.market_type.as_str();
        let name = Some(dep.name.as_str()).filter(|n| !n.is_empty());
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
            &dep.viper_budgets,
            MarketSelection { primary: pair, maker: None },
            MarketMandate::Pinned,
            Some(class),
            name,
            None,
            &self.tennis_rx,
            Some(dep.id.as_str()),
        ).await;
        // The order-book feed for this market runs on this token and nothing
        // else cancels it: the rotation loop cancels its per-market token, but a
        // pinned deployment returned its outcome and left the WebSocket open and
        // reconnecting for the life of the process. Rare while a pinned market
        // only ended on its stated close; once per finished game now.
        cancel.cancel();
        // A Helm squadron's open intents go with it, whichever way the loop ended.
        //
        // The intl path does this inside `patrol_impl`'s retirement arm, which
        // this loop never reaches: it runs its own loop over the shared
        // `evaluate_strategies` rather than `squadron.patrol()`. Without this a
        // finished Helm squadron leaves its intents open forever, and they still
        // count against `helm_max_open_intents` — two of them block every future
        // intent on the instance.
        if class == crate::vipers::helm_impl::KIND {
            if let Some(sq) = db::deployment_squadron(&dep.id).await {
                if let Some(pool) = db::pool_for(crate::vipers::helm_impl::KIND) {
                    let n = crate::helpers::helm::close_open_for_squadron(
                        &pool, &sq, "the squadron's market loop ended",
                    ).await;
                    if n > 0 {
                        info!("🧭 Squadron [{sq}] closed {n} open Helm intent(s): market loop ended");
                    }
                } else {
                    warn!("🧭 Squadron [{sq}]: no `helm` pool — open Helm intents were NOT closed");
                }
            }
        }
        info!("📋 Deployed {class} squadron finished: {outcome:?}");
        Ok(())
    }

    async fn select_market(
        &self,
        class: &str,
        max_days_to_close: u32,
        min_liquidity_usd: f64,
    ) -> Option<String> {
        select_auto_deploy_market(&self.venue, class, max_days_to_close, min_liquidity_usd).await
    }
}

/// Does a Kalshi series ticker name a game series?
///
/// Kalshi's game-winner series all end in GAME, MATCH or FIGHT — `KXNFLGAME`,
/// `KXATPMATCH`, `KXUFCFIGHT` — and nothing else filed under Sports does
/// (checked against all 3,754 sports series on 2026-09-09). The suffix is the
/// only structural signal the venue gives: a market record carries no category
/// or shape of its own, and the event's "Sports" category is shared by season
/// futures, retirements, relocations and coaching changes.
pub(crate) fn is_game_series_ticker(ticker: &str) -> bool {
    let t = ticker.trim().to_ascii_uppercase();
    t.starts_with("KX") && (t.ends_with("GAME") || t.ends_with("MATCH") || t.ends_with("FIGHT"))
}

/// Split the operator's comma-separated series list into the game series it
/// names and the entries that are not game series, each normalized to the
/// venue's uppercase ticker form and deduplicated.
///
/// The rejected half is returned rather than dropped so the caller can say
/// which entries were ignored: a list edit that silently did nothing is the
/// failure mode this knob's description warns about.
pub(crate) fn parse_game_series_list(csv: &str) -> (Vec<String>, Vec<String>) {
    let mut accepted: Vec<String> = Vec::new();
    let mut rejected: Vec<String> = Vec::new();
    for raw in csv.split(',') {
        let t = raw.trim().to_ascii_uppercase();
        if t.is_empty() || accepted.contains(&t) || rejected.contains(&t) {
            continue;
        }
        if is_game_series_ticker(&t) { accepted.push(t) } else { rejected.push(t) }
    }
    (accepted, rejected)
}

/// The operator's game-series list, from the live global config.
///
/// Read from the broadcast rather than `load_or_default`, which writes a
/// config-history snapshot on every call and this runs on every idle seeder
/// tick; the DB read covers only the pre-registration window.
async fn sports_game_series_csv() -> String {
    match crate::helpers::dynamic_config::global_config_tx() {
        Some(tx) => tx.borrow().kalshi_sports_game_series.clone(),
        None => DynamicConfig::load_or_default().await.kalshi_sports_game_series.clone(),
    }
}

/// Every open game-winner market across the configured game series.
///
/// This is the sports class on Kalshi. It used to be "every open market under
/// the Sports category", swept from `/events?status=open` — but that sweep is
/// capped at 2,400 events and the venue lists its long-dated futures first, so
/// the games never appeared in it (2026-09-09: 279 Sports events in the sweep,
/// not one a game). The seeder's highest-volume pick from what did appear was
/// "Will Steve Kerr be out before Oct 25, 2026?", a coaching personnel market
/// nothing in the engine can price, which then held the sports slot. Querying
/// the game series directly is both cheaper — about one request per league —
/// and cannot return anything but games.
///
/// Shared by the seeder and the Control Tower's market browser, so the browser
/// can only show a sports market the seeder would also choose.
pub(crate) async fn open_sports_game_markets(venue: &KalshiVenue) -> Vec<KalshiMarket> {
    let csv = sports_game_series_csv().await;
    let (series, rejected) = parse_game_series_list(&csv);
    if !rejected.is_empty() {
        warn!(
            "📋 Kalshi sports: ignoring {} configured series that are not game series \
             (every game series ends in GAME, MATCH or FIGHT): {}",
            rejected.len(), rejected.join(", "),
        );
    }
    if series.is_empty() {
        warn!("📋 Kalshi sports: no game series configured — the sports class has nothing to discover");
        return Vec::new();
    }
    let mut out = Vec::new();
    for s in &series {
        match venue.markets_for_series(s).await {
            Ok(markets) => out.extend(
                markets.into_iter().filter(|m| m.status == "active" || m.status.is_empty()),
            ),
            Err(e) => warn!("📋 Kalshi sports discovery for series {s} failed: {e:#}"),
        }
    }
    out
}

/// Highest-volume open market in `class`, within the operator's deploy horizon.
///
/// Mirrors the Control Tower's quick-deploy selection: same discovery, same
/// sort, same horizon. Politics is Kalshi's own category taxonomy, which
/// reaches years out because that is how the venue structures it — but
/// Arbitrage locks collateral until the market resolves, so a 2028 market is a
/// poor use of it. Sports is the configured game series, and nothing else —
/// see [`open_sports_game_markets`].
async fn select_auto_deploy_market(
    venue: &Arc<KalshiVenue>,
    class: &str,
    max_days_to_close: u32,
    // Kalshi reports volume in contracts, each worth at most $1, so a dollar
    // floor applied to it is if anything slightly lenient — never stricter
    // than the operator asked for.
    min_liquidity_usd: f64,
) -> Option<String> {
    let found: Vec<KalshiMarket> = if class == "sports" {
        open_sports_game_markets(venue).await
    } else {
        let cats: Vec<&str> = crate::config::KALSHI_POLITICS_CATEGORIES
            .split(',').map(str::trim).filter(|c| !c.is_empty()).collect();
        match venue.open_markets_for_categories(&cats).await {
            Ok(f) => f.into_iter().map(|(_cat, m)| m).collect(),
            Err(e) => {
                warn!("📋 Auto-deploy {class} discovery failed: {e:#}");
                return None;
            }
        }
    };

    let now = Utc::now();
    let max_secs = max_days_to_close as i64 * 86_400;

    // A sports squadron must land on a game the bookmaker board can PRICE, not
    // merely the busiest game on the slate. The board is what every sports viper
    // reads, so a squadron on an uncovered game patrols, reports "no bookmaker
    // line" and holds the venue's one sports slot while doing nothing. Measured
    // 2026-09-26: this selector picked a college football game while the board held
    // only MLB, and Bookline idled on a working adapter for want of a line.
    //
    // Polymarket International has had this rule since its sports seeder was
    // written; `board_coverage` is that rule, lifted somewhere all three venues can
    // share it.
    if class == "sports" {
        let board = crate::raptors::sports_ledger::board();
        let candidates: Vec<(String, String, f64)> = found.iter().map(|m| (
            crate::venues::kalshi::leg_id(&m.ticker, true),
            crate::venues::kalshi::leg_id(&m.ticker, false),
            crate::venues::kalshi::types::fp(&m.volume_fp)
                .and_then(|d| f64::try_from(d).ok()).unwrap_or(0.0),
        )).collect();
        let cov = crate::raptors::sports_ledger::board_coverage(&candidates, &board, now);
        match cov.best {
            Some(i) => {
                info!(
                    "📋 Kalshi sports: {} of {} game(s) on the bookmaker board, {} pre-game — \
                     selecting {}",
                    cov.on_board, found.len(), cov.pre_game, found[i].ticker,
                );
                return Some(found[i].ticker.clone());
            }
            None => {
                // Throttled for the same reason as the Polymarket US twin: the
                // seeder reaches this arm on every pass while the slot is empty.
                if crate::raptors::sports_ledger::coverage_changed("Kalshi", &cov) {
                    info!(
                        "📋 Kalshi sports: none of {} open game(s) are on the bookmaker board \
                         pre-game ({} on board, {} pre-game) — leaving the sports slot idle rather \
                         than deploying onto a game nothing can price",
                        found.len(), cov.on_board, cov.pre_game,
                    );
                }
                return None;
            }
        }
    }

    let mut best: Option<(f64, String)> = None;
    for m in found {
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
        if volume < min_liquidity_usd {
            continue;
        }
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

            let primary = choose_primary(&pairs, filter.as_deref());

            if let Some(p) = primary {
                let maker = choose_maker(&pairs, &p);

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

/// The market the hour-shaped Vipers fly: the hottest market of the best
/// cadence on the board (one-hour first, then 15-minute, then daily), narrowed by
/// the operator's ticker/title filter when one is set.
///
/// `pairs` arrive hottest-first from `discover_pairs`, so within a cadence the
/// first match is the busiest strike of the ladder.
fn choose_primary(pairs: &[KalshiPair], filter: Option<&str>) -> Option<KalshiPair> {
    let matches = |p: &KalshiPair| match filter {
        Some(f) => {
            let fl = f.to_lowercase();
            p.ticker.to_lowercase().contains(&fl) || p.question.to_lowercase().contains(&fl)
        }
        None => true,
    };
    pairs.iter()
        .filter(|p| matches(p))
        .min_by_key(|p| Cadence::primary_rank(p.cadence))
        .and_then(|best| {
            // The hottest of the chosen cadence, not merely the first to rank.
            pairs.iter().find(|p| matches(p) && p.cadence == best.cadence)
        })
        .cloned()
}

/// The daily market the Window/Daily Vipers rest orders on: the hottest daily
/// with the primary's underlying that is not the primary itself.
fn choose_maker(pairs: &[KalshiPair], primary: &KalshiPair) -> Option<KalshiPair> {
    pairs.iter()
        .find(|d| d.cadence == Some(Cadence::Daily) && d.underlying == primary.underlying && d.ticker != primary.ticker)
        .cloned()
}

/// Is the hourly market `current` past its settle window, so the loop holds it
/// to the close whatever the rest of its ladder is doing? Only an hourly market
/// with a published close can be pinned: without the close the window's open is
/// a guess, and the volume rule is the honest fallback. A missing window length
/// (the venue always publishes one with an hourly close) reads as the nominal
/// hour.
fn hourly_pinned(current: &KalshiPair, now_s: i64) -> bool {
    match (current.cadence, current.close_time) {
        (Some(Cadence::Hourly), Some(close)) => {
            let open_s = close.timestamp() - current.window_secs.unwrap_or(Cadence::Hourly.window_secs());
            now_s >= open_s + HOURLY_SETTLE_SECS
        }
        _ => false,
    }
}

/// Should the loop leave `current` for `best`, the candidate `choose_primary`
/// picked from a fresh scan that excluded `current`, at `now_s`?
///
/// A better cadence always wins: a one-hour market appearing while the loop
/// flies a 15-minute one is the market the hour-shaped Vipers exist for. Within
/// the same cadence an hourly market that has settled (`hourly_pinned`) is held
/// to its close against the rest of its own ladder; a market of another
/// underlying is still judged by volume, so a quiet hour's first pick (an ETH
/// market with twenty contracts) gives way once a BTC strike is clearly busier.
/// Otherwise the old rule holds: only a clearly hotter market, by
/// `ROTATION_VOLUME_THRESHOLD` contracts, is worth the churn of moving.
fn should_rotate(current: &KalshiPair, best: &KalshiPair, now_s: i64) -> bool {
    let (cur, new) = (Cadence::primary_rank(current.cadence), Cadence::primary_rank(best.cadence));
    if new < cur { return true; }
    if new > cur { return false; }
    if best.underlying == current.underlying && hourly_pinned(current, now_s) { return false; }
    best.volume > current.volume + ROTATION_VOLUME_THRESHOLD
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
    // Per-viper capital budgets the operator set in the deploy dialog. Empty for
    // a rotated market, which nobody chose the exposure for.
    viper_budgets: &std::collections::HashMap<String, f64>,
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
    tennis_rx: &watch::Receiver<TennisSnapshot>,
    // The `deployment_queue` row this market was deployed from, so the row can
    // learn which squadron it produced. `None` for the rotation loop, which
    // nobody queued.
    deployment_id: Option<&str>,
) -> MarketOutcome {
    let asset = KALSHI_ASSET;
    let mut pair = selection.primary;
    let maker_pair = selection.maker;

    // ── Raptor intelligence for the market's underlying ─────────────────────
    // Identity for logs and the viper-status registry: the crypto underlying
    // where there is one, the market class otherwise.
    // A Helm squadron scopes to `helm` whatever it trades.
    //
    // The crypto branch would otherwise win on a crypto-underlying ticker and
    // scope it to `btc`, which sets `crypto_filter` to BTC and leaves
    // `pool_for("helm")` resolving to nothing: the intents would never be written
    // and the stand-down handler would silently fail to close them. The squadron
    // is a Helm squadron because the operator deployed it as one, not because of
    // what its market happens to be about.
    let status_scope = if deployed_as == Some(crate::vipers::helm_impl::KIND) {
        crate::vipers::helm_impl::KIND
    } else if pair.is_crypto() {
        pair.underlying
    } else {
        deployed_as.unwrap_or("deployed")
    };

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
    let squadron = register_kalshi_squadron(cag, &pair, &raptors, deployed_as, squadron_name, tennis_rx, cancel);
    let squadron_id = squadron.id.clone();
    if let Some(dep_id) = deployment_id {
        crate::helpers::db::set_deployment_squadron(dep_id, &squadron_id).await;
    }
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
                // Retire its REGISTRIES too, not just its CAG entry.
                //
                // The comment above describes a permanent RTB row per underlying;
                // the viper and book-feed registries had exactly the same leak and
                // were never given the same treatment. A rotated-away underlying
                // left nine viper rows behind that counted toward "across N
                // squadrons", then aged out and reported "N stale/error — check
                // squadron detail" for a squadron no longer on screen.
                //
                // The asset is the first segment of `{asset}-{cadence}[-{slug}]`,
                // which `Squadron::new_named` guarantees and a test pins.
                let prev_asset = prev.split('-').next().unwrap_or(prev);
                crate::helpers::viper_status::forget(prev_asset);
                crate::state::price_state::book_feed::forget(prev_asset);
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
    seed_squadron_config(&squadron_id, viper_budgets).await;
    let market_class = squadron.classify_and_link().await;
    // Carried into every tick's StrategyContext: the class is what a viper
    // asks to know what kind of market it is looking at.
    let market_class_for_ctx = market_class.clone();
    // Filing dimensions for every row this market writes. `asset` above is
    // only the shard (one DB for all Kalshi squadrons); venue, class and
    // underlying are the attributes that actually describe the trade.
    let mut scope = TradeScope::new(
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
    let strategies = StrategyRegistry::create_strategies_for_kinds(&viper_kinds);
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
        window_secs: pair.window_secs,
        is_neg_risk: false,
        // Kalshi's market id is the ticker. The CAG publishes it as the
        // squadron's `market_id`, which readers that join markets to squadrons
        // (collateral pools in use, the Markets page) rely on.
        condition_id: pair.ticker.clone(),
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
        window_secs: mp.window_secs,
        is_neg_risk: false,
        condition_id: String::new(),
        yes_fee_bps: KALSHI_FEE_BPS as u32,
        no_fee_bps: KALSHI_FEE_BPS as u32,
    });
    let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(HashMap::new()));
    let lifecycle = Arc::new(OrderLifecycle::new(LifecycleConfig::us(), squadron_id.clone()));
    let _fill_listener = lifecycle.spawn_fill_listener(Arc::clone(venue), Arc::clone(&positions));
    let market_started_at = Utc::now();

    // GBoost plan B's training store for this venue is written live: Kalshi
    // publishes no price history, so a one-hour BTC market is recorded while it
    // is flown (mids once a minute, the tape at the close) and settled by the
    // pipeline afterwards. See `vipers::gboost_planb_record`.
    let mut recorder = match (pair.cadence, pair.underlying, pair.close_time) {
        (Some(Cadence::Hourly), "btc", Some(close)) => Some(crate::vipers::gboost_planb_record::HourlyRecorder::new(
            &pair.ticker, pair.long.as_str(), pair.short.as_str(), close.timestamp(), pair.strike.and_then(|k| k.to_f64()),
        )),
        _ => None,
    };

    if pair.is_crypto() {
        publish_raptor_health(raptor_health_tx, pair.underlying, true);
    }
    // Map each viper to its preferred venue: "Hourly" vipers → primary (the
    // one-hour market when the board has one, else the 15-minute one),
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
    // Dropped when this function returns, whichever way — see the type.
    let _registries = MarketRegistries::new(&book_tickers, mandate, status_scope, Some((markets_tx, squadron_id.as_str())));
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
    // The squadron's live config handle, as the intl patrol registers one. Work
    // that runs beside the squadron reads it (`squadron_config_snapshot`): the
    // GBoost training pipeline takes its plan from here, and a Control Tower
    // PATCH lands in it at once. Until 2026-10-17 this loop never registered,
    // so on this venue the pipeline waited for a BTC squadron that was flying,
    // and the profile picker reported success over zero squadrons (`config_write_targets`).
    let live_cfg: Arc<std::sync::RwLock<DynamicConfig>> = Arc::new(std::sync::RwLock::new((*dyn_cfg).clone()));
    crate::helpers::dynamic_config::register_squadron_config_handle(&squadron_id, Arc::clone(&live_cfg));
    // See `TradeScope::ghost`: the ledger records whether a trade was real.
    scope.ghost = crate::config::GHOST_MODE || dyn_cfg.ghost_mode;
    if let Some(p) = &pool {
        let (coll, total) = sync_dashboard(&squadron_id, venue.as_ref(), p, &positions, Some(lifecycle.as_ref()), starting, None).await;
        available_collateral = coll;
        if let Some(total) = total {
            session_pnl = if dyn_cfg.ghost_mode { crate::helpers::metrics::realised_session_pnl() } else { total - starting };
        }
    }

    // ── Tick loop ────────────────────────────────────────────────────────────
    let mut price_tick = tokio::time::interval(Duration::from_millis(TICK_MS));
    let mut dash_tick = tokio::time::interval(Duration::from_secs(DASHBOARD_SYNC_SECS));
    let mut lifecycle_tick = tokio::time::interval(Duration::from_secs(LIFECYCLE_SYNC_SECS));
    let mut rescan_tick = tokio::time::interval(Duration::from_secs(MARKET_RESCAN_SECS));
    // The pin is logged once per market, the first time it holds against a
    // hotter strike; a line every rescan would be noise.
    let mut pin_logged = false;
    let _ = rescan_tick.tick().await; // skip the immediate first fire
    let mut cooldown_until = Instant::now();
    let mut winding_down = false;
    // Venue status and game-over probes for a pinned market; see the tick body.
    // Both clocks start due so the first tick asks at once.
    let poll = Duration::from_secs(VENUE_STATUS_POLL_SECS);
    let mut venue_status_probed_at = Instant::now().checked_sub(poll).unwrap_or_else(Instant::now);
    let mut sports_signals_probed_at = venue_status_probed_at;
    let mut venue_accepting: Option<bool> = None;
    let mut venue_closed_since: Option<Instant> = None;
    let mut venue_closed_desc = String::new();
    let mut venue_event_ticker: Option<String> = None;
    let mut venue_expected_expiration: Option<chrono::DateTime<Utc>> = None;
    let mut sports_kick_off: Option<chrono::DateTime<Utc>> = None;
    let mut sports_resolved = false;
    let mut book_absent_since: Option<Instant> = None;
    let mut game_over_since: Option<Instant> = None;

    cag.update_state(&squadron_id, SquadronState::Patrolling);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("Kalshi trader: cancelled — standing down");
                cancel_all_before_leaving(venue.as_ref(), &lifecycle, &positions, &pool, &pair.question).await;
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
                    let (coll, total) = sync_dashboard(
                        &squadron_id, venue.as_ref(), p, &positions, Some(lifecycle.as_ref()), starting, Some(available_collateral),
                    ).await;
                    available_collateral = coll;
                    // A pass the venue could not be read leaves session P&L alone:
                    // `total - starting` with no total is zero, and zero clears the
                    // drawdown guard for every viper for 30 s (review of [B69]).
                    if let Some(total) = total {
                        session_pnl = if dyn_cfg.ghost_mode { crate::helpers::metrics::realised_session_pnl() } else { total - starting };
                    }
                }
                release_swept_positions(&positions).await;
                release_operator_closed_positions(&positions).await;
                // Simulated holds carried through an earlier market's close, booked
                // from their rows once the venue states the result [B65].
                if let Some(p) = &pool {
                    let mut live = vec![pair.long.clone(), pair.short.clone()];
                    if let Some(mp) = &maker_pair {
                        live.push(mp.long.clone());
                        live.push(mp.short.clone());
                    }
                    crate::helpers::ghost_settlement::settle_closed_ghost_holds(
                        venue.as_ref(), &positions, p, &scope, asset, &squadron_id, &live,
                    ).await;
                }
                dyn_cfg = DynamicConfig::load_for_squadron(&squadron_id).await;
                if let Ok(mut live) = live_cfg.write() { *live = (*dyn_cfg).clone(); }
                // Track a mid-session mode flip in the filing scope too.
                scope.ghost = crate::config::GHOST_MODE || dyn_cfg.ghost_mode;
                continue;
            }
            _ = rescan_tick.tick() => {
                // A market the operator deployed is never rotated away from —
                // they chose it, and swapping it for whatever is hottest in the
                // crypto series would be both surprising and off-class.
                if mandate == MarketMandate::Pinned {
                    continue;
                }
                // Only rotate when flat. A guard that is still an unfilled quote
                // is not a holding: the rotation itself cancels it [B74].
                if lifecycle.holds_anything(&positions).await {
                    continue;
                }
                let mut candidates = discover_pairs(venue, series).await;
                // The current market's volume was frozen at selection, so it
                // always read colder than the ladder around it; refresh it from
                // the same scan the candidates come from.
                if let Some(cur) = candidates.iter().find(|p| p.ticker == pair.ticker) { pair.volume = cur.volume; }
                candidates.retain(|p| p.ticker != pair.ticker);
                let now_s = Utc::now().timestamp();
                let best = choose_primary(&candidates, None);
                if let Some(b) = best.as_ref() {
                    let hotter_same_ladder = b.cadence == pair.cadence && b.underlying == pair.underlying
                        && b.volume > pair.volume + ROTATION_VOLUME_THRESHOLD;
                    if !pin_logged && hotter_same_ladder && hourly_pinned(&pair, now_s) {
                        pin_logged = true;
                        info!(
                            "📌 Kalshi hourly held: staying on \"{}\" vol={:.0} to its close; \"{}\" vol={:.0} is hotter, but the strike settled {}s into the window",
                            pair.question, pair.volume, b.question, b.volume, HOURLY_SETTLE_SECS,
                        );
                    }
                }
                if let Some(best) = best.filter(|b| should_rotate(&pair, b, now_s)) {
                    {
                        info!(
                            "🔍 Better Kalshi market: \"{}\" {:?} vol={:.0} over \"{}\" {:?} vol={:.0} (threshold {:.0} within a cadence) — rotating",
                            best.question, best.cadence, best.volume, pair.question, pair.cadence, pair.volume, ROTATION_VOLUME_THRESHOLD
                        );
                        lifecycle.cancel_all(venue.as_ref(), &positions).await;
                        settle_lifecycle_reports(&pool, &lifecycle).await;
                        // The cancels may have found a quote that traded, or an
                        // order the venue could not settle. Either is this
                        // market's to manage: leaving now would drop the map
                        // and the lifecycle that own it [B74].
                        if lifecycle.holds_anything(&positions).await || lifecycle.tracks_anything().await {
                            warn!("🔀 Kalshi rotation held: a position landed or an order is unsettled while leaving \"{}\" — staying on to manage it", pair.question);
                            continue;
                        }
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
                settle_lifecycle_reports(&pool, &lifecycle).await;
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

        // ── Venue status and game-over probes (pinned markets only) ───────
        // The stated close is not enough for a market the operator or the
        // seeder pinned. Kalshi dates a game market's `close_time` at its
        // latest expiration, two days after the game, and the rescan arm above
        // returns early for pinned markets, so until 2026-10-19 a finished game
        // (KXNFLGAME-26OCT18DALGB-GB, finalized 03:47Z, stated close two days
        // on) held the sports slot and the "market data has stopped" banner
        // for the whole two days. Two further signals, both already proven on
        // the intl patrol: the venue's own market record once a minute, and
        // for sports the game-over rule (board, book, ledger). Never while
        // holding: the exchange settles what we hold and the settled-position
        // sweep books it. Crypto rotations are untouched; their close is exact.
        let mut venue_retire: Option<String> = None;
        if mandate == MarketMandate::Pinned {
            let holding = !positions.lock().await.is_empty();
            let grace = dyn_cfg.event_market_retire_grace_secs;
            if venue_status_probed_at.elapsed() >= Duration::from_secs(VENUE_STATUS_POLL_SECS) {
                venue_status_probed_at = Instant::now();
                let probe = tokio::time::timeout(Duration::from_secs(VENUE_STATUS_TIMEOUT_SECS), venue.market(&pair.ticker)).await;
                match probe.map_err(|_| anyhow::anyhow!("timed out after {VENUE_STATUS_TIMEOUT_SECS}s")).and_then(|r| r) {
                    Ok(m) => {
                        if venue_event_ticker.is_none() && !m.event_ticker.is_empty() {
                            venue_event_ticker = Some(m.event_ticker.clone());
                        }
                        if venue_expected_expiration.is_none() {
                            venue_expected_expiration = m.expected_expiration_time.parse::<chrono::DateTime<Utc>>().ok();
                        }
                        let accepting = venue_accepting_orders(&m.status, &m.result);
                        // Announced once so the log shows the probe is running;
                        // afterwards only a change of answer prints.
                        if venue_accepting.is_none() && accepting.is_some() {
                            info!(
                                "📡 Squadron [{}] venue reports \"{}\" status={:?} result={:?} — re-checking every {}s",
                                squadron_id, market_cfg.market_name, m.status, m.result, VENUE_STATUS_POLL_SECS,
                            );
                        }
                        venue_accepting = accepting;
                        match accepting {
                            Some(true) => {
                                if venue_closed_since.take().is_some() {
                                    info!(
                                        "🏁 Squadron [{}] venue is trading \"{}\" again — retirement clock reset",
                                        squadron_id, market_cfg.market_name,
                                    );
                                }
                            }
                            Some(false) => {
                                venue_closed_desc = format!("status={:?} result={:?}", m.status, m.result);
                                if venue_closed_since.is_none() {
                                    venue_closed_since = Some(Instant::now());
                                    info!(
                                        "🏁 Squadron [{}] venue reports \"{}\" no longer trading ({}) — retiring in {}s once flat",
                                        squadron_id, market_cfg.market_name, venue_closed_desc, grace,
                                    );
                                }
                                // Simulated positions have no settlement path of
                                // their own: the exchange settles what we really
                                // hold, and the settled-position sweep books it,
                                // but a ghost position is held by nobody. Left
                                // alone it pins the squadron ("never over a
                                // position") and the class with it. Kalshi
                                // states the result in the market record, so
                                // book straight from it; an empty or non-binary
                                // result books nothing and the clock keeps
                                // running. Once a minute, on the probe's cadence.
                                if holding && scope.ghost {
                                    if let (Some(yes_px), Some(p)) = (kalshi_result_yes_price(&m.result), pool.as_ref()) {
                                        let n = crate::helpers::ghost_settlement::settle_ghost_positions_at(
                                            &positions, p, asset, Some(market_class.as_str()),
                                            &pair.long, &pair.short, yes_px,
                                        ).await;
                                        if n > 0 {
                                            info!(
                                                "👻 Squadron [{}]: booked {} simulated settlement(s) on \"{}\" from the venue's result {:?} — releasing the squadron",
                                                squadron_id, n, market_cfg.market_name, m.result,
                                            );
                                        }
                                    }
                                }
                            }
                            None => {}
                        }
                    }
                    // Unanswered is neither "closed" nor "open": a clock already
                    // running keeps running, and the stated close keeps its
                    // authority. A transient REST failure must not retire anything.
                    Err(e) => {
                        debug!("📡 Squadron [{}] venue status probe failed: {e}", squadron_id);
                        venue_accepting = None;
                    }
                }
            }
            let closed_for = venue_closed_since.map(|t| t.elapsed().as_secs() as i64);
            if venue_closed_retire_due(closed_for, grace, holding) {
                venue_retire = Some(format!(
                    "the venue reports it no longer trading ({venue_closed_desc}) for {}s", closed_for.unwrap_or(0),
                ));
            }

            if venue_retire.is_none() && deployed_as == Some("sports") {
                let (has_book, top_bid) = book_state(&hub, &pair.ticker).await;
                if has_book {
                    book_absent_since = None;
                } else if book_absent_since.is_none() {
                    book_absent_since = Some(Instant::now());
                }
                if sports_signals_probed_at.elapsed() >= Duration::from_secs(VENUE_STATUS_POLL_SECS) {
                    sports_signals_probed_at = Instant::now();
                    // The ledger writes to the primary pool on every venue and keys
                    // a Kalshi game by its event ticker (`KalshiSportsCatalog`). A
                    // game the ledger never matched falls back to the kick-off the
                    // ticker or the expected expiration implies: the seeder only
                    // deploys games on the board, so this is for a Helm pick.
                    if let Some(p) = db::pool() {
                        if sports_kick_off.is_none() {
                            if let Some(ev) = venue_event_ticker.as_deref() {
                                sports_kick_off = db::sports_ledger_kick_off(p, ev).await
                                    .or_else(|| crate::venues::kalshi::sports::kickoff_for(ev, venue_expected_expiration));
                            }
                        }
                        if !sports_resolved {
                            sports_resolved = db::sports_resolved_price(p, market_cfg.yes_token.as_str()).await.is_some()
                                || db::sports_resolved_price(p, market_cfg.no_token.as_str()).await.is_some();
                        }
                    }
                }
                let signals = SportsGameSignals {
                    kick_off: sports_kick_off,
                    on_board: crate::raptors::sports_ledger::line_for(
                        market_cfg.yes_token.as_str(), market_cfg.no_token.as_str()).is_some(),
                    resolved_by_ledger: sports_resolved,
                    book: if has_book {
                        SportsBook::Live { top_bid }
                    } else {
                        SportsBook::Absent { for_secs: book_absent_since.map_or(0, |t| t.elapsed().as_secs() as i64) }
                    },
                };
                let verdict = sports_game_over_verdict(
                    Utc::now(), &signals, dyn_cfg.sports_game_over_after_secs, dyn_cfg.sports_game_over_decided_bid,
                );
                match verdict {
                    Some(v) => {
                        if game_over_since.is_none() {
                            game_over_since = Some(Instant::now());
                            info!(
                                "🏁 Squadron [{}] game \"{}\" reads as over: {} — retiring in {}s once flat ({})",
                                squadron_id, market_cfg.market_name, v.describe(),
                                if matches!(v, GameOver::Resolved) { 0 } else { grace },
                                match venue_accepting {
                                    Some(true) => "venue still reports it trading",
                                    Some(false) => "venue also reports it closed",
                                    None => "venue status unknown",
                                },
                            );
                        }
                    }
                    None => {
                        if game_over_since.take().is_some() {
                            info!(
                                "🏁 Squadron [{}] game \"{}\" no longer reads as over — retirement clock reset",
                                squadron_id, market_cfg.market_name,
                            );
                        }
                    }
                }
                let held = game_over_since.map_or(0, |t| t.elapsed().as_secs() as i64);
                if sports_game_over_due(verdict, held, grace, holding) {
                    venue_retire = Some(format!("game over: {}", verdict.map(|v| v.describe()).unwrap_or_default()));
                }
            }
        }

        let phase = if venue_retire.is_some() { MarketPhase::Closed } else { market_cfg.phase(Utc::now(), MARKET_RTB_WINDOW_SECS) };
        match phase {
            MarketPhase::Closed => {
                match &venue_retire {
                    Some(why) => info!("🏁 Kalshi market \"{}\" retired: {why} — standing down", market_cfg.market_name),
                    None => info!("🏁 Kalshi market \"{}\" reached close — standing down to rotate", market_cfg.market_name),
                }
                // The flat check behind `venue_retire` ran before the probe's
                // REST call, and a resting quote can fill in between. Returning
                // now would drop the fill listener on a position this squadron
                // owns, so pull what rests and look once more; a position means
                // staying on to manage it, and the probes ask again next tick.
                if venue_retire.is_some() {
                    lifecycle.cancel_all(venue.as_ref(), &positions).await;
                    settle_lifecycle_reports(&pool, &lifecycle).await;
                    if !positions.lock().await.is_empty() || lifecycle.tracks_anything().await {
                        warn!(
                            "🏁 Kalshi market \"{}\": a position landed or an order is unsettled while retiring — staying on to manage it",
                            market_cfg.market_name,
                        );
                        continue;
                    }
                }
                if let Some(r) = recorder.take() {
                    let (w, samples, ticker) = (r.window_start(), r.samples(), r.ticker().to_string());
                    // The whole hour's tape, paged; one page covered nine minutes.
                    let prints = match venue.trades_between(&pair.ticker, w, w + 3600).await {
                        Ok(p) => p,
                        Err(e) => { warn!("📼 Could not fetch the hour's prints for {ticker}: {e}; recording the mids alone"); Vec::new() }
                    };
                    let rec = r.finish(&prints);
                    let dir = crate::vipers::gboost_planb_train::DataDir::new("btc");
                    match crate::vipers::gboost_planb_record::write_record(&dir, w, &rec) {
                        Ok(path) => info!(
                            "📼 Recorded hourly market {ticker} for GBoost training: {samples} minute(s) of mids, {} print(s), outcome pending settlement → {}",
                            rec.tape.len(), path.display(),
                        ),
                        Err(e) => warn!("📼 Could not write the hourly market record for {ticker}: {e}"),
                    }
                }
                // cancel_all cancels RESTING ORDERS; it does not sell inventory.
                // A squadron trades two markets at once — the hot primary that
                // rotates every 15-30 minutes, and a longer-lived daily maker
                // market that FairValue actively prefers — and its lifetime is
                // tied to the PRIMARY's close. Tearing down here therefore
                // abandoned any position sitting on the daily, which then ran
                // unmanaged (no stop, no take-profit) until it expired. That is
                // how -$3.09 was lost on 2026-08-10, recorded in neither
                // `trades` nor `entries`.
                cancel_all_before_leaving(venue.as_ref(), &lifecycle, &positions, &pool, &market_cfg.market_name).await;
                flatten_before_stand_down(
                    &squadron_id, venue, &hub, &pool, &scope, &positions, &lifecycle,
                    &market_cfg, maker_cfg.as_ref(), maker_pair.as_ref(), &raptors, status_scope, starting,
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
        if let Some(r) = recorder.as_mut() {
            let mid = |bid: Decimal, ask: Decimal| (bid > Decimal::ZERO && ask > Decimal::ZERO).then(|| ((bid + ask) / dec!(2)).to_f64()).flatten();
            r.sample(Utc::now().timestamp(), mid(snapshot.yes_bid, snapshot.yes_ask), mid(snapshot.no_bid, snapshot.no_ask));
        }

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

        // ── Ghost maker fills: rest until the book crosses the quote ──────
        //
        // Simulated maker quotes are held in the venue-neutral registry and
        // become positions only when the best ASK reaches them. Before this,
        // `dispatch_single` stamped a full fill at the quote price the instant
        // it was placed, and since the Maker viper quotes below the ask by
        // construction the position was born in profit and took its target on
        // the next tick — the treadmill measured on intl, which lived here too.
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
                // The entry is booked HERE rather than at placement, so the paper
                // record carries fills that a counterparty actually caused.
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
            market_class: Some(market_class_for_ctx.clone()),
            // A crypto pair gets the real raptor stack (`raptor_stack_for`);
            // any other pair gets `CryptoRaptors::neutral()`, whose oracle has
            // always read 0.
            has_price_feed: pair.is_crypto(),
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
            sports: crate::raptors::sports_ledger::line_for(market_cfg.yes_token.as_str(), market_cfg.no_token.as_str()),
        };

        let eval = match evaluate_strategies(&strategies, &ctx).await {
            Ok(e) => e,
            Err(e) => { warn!("Kalshi strategy evaluation error: {e}"); continue; }
        };
        let (signals, _) = aggregate_and_resolve_signals(&eval);
        if signals.is_empty() {
            continue;
        }

        // [B74] What the fill feed and the reconciler learned about resting
        // quotes reaches the rows before any signal acts on them, so a figure
        // for a quote that has since been exited never lands on a newer row.
        settle_lifecycle_reports(&pool, &lifecycle).await;
        let mut acted = false;
        for (strategy_name, signal) in signals {
            // Inside the RTB window we manage what we hold but take on nothing
            // new: cancels and exits still flow, entries and quotes do not.
            if winding_down && signal.opens_exposure() {
                continue;
            }
            if dispatch_signal(&squadron_id, venue.as_ref(), &pool, &scope, &positions, &lifecycle, &strategy_name, status_scope, &signal, starting).await {
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
    status_scope: &str,
    starting: Decimal,
    ghost: bool,
) {
    let candidates: Vec<(String, MarketId, Decimal, bool)> = {
        let map = positions.lock().await;
        map.iter()
            .filter(|(_, p)| p.shares > dec!(0))
            .filter(|(k, _)| k.squadron == squadron_id)
            .map(|(k, p)| (k.strategy.clone(), k.market.clone(), p.shares, p.fill_confirmed_at.is_some()))
            .collect()
    };
    // [B74] A guard that is still an unfilled quote with its order tracked (the
    // stand-down cancel did not go through) holds nothing to sell.
    let mut held: Vec<(String, MarketId, Decimal)> = Vec::new();
    for (strategy, token, shares, confirmed) in candidates {
        if !confirmed && lifecycle.resting_for(&strategy, &token).await {
            warn!("🏁 [{strategy}] {token}: an unfilled quote whose cancel did not go through — nothing to sell; it stays tracked");
            continue;
        }
        held.push((strategy, token, shares));
    }
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
        dispatch_signal(squadron_id, venue.as_ref(), pool, scope, positions, lifecycle, &strategy, status_scope, &signal, starting).await;
    }
}

/// Cancel what the lifecycle tracks before leaving a market, trying again for
/// orders the venue could not settle at once (a refused cancel with no answer,
/// a failed read), and say which ones are left behind: they stay at the venue
/// with nothing tracking them, and any fill is adopted by the account sweep as
/// a venue holding. Bounded, so a venue that never answers cannot hold the
/// loop [B74].
async fn cancel_all_before_leaving<V: Execution + ?Sized>(
    venue: &V,
    lifecycle: &OrderLifecycle,
    positions: &Arc<Mutex<PositionMap>>,
    pool: &Option<sqlx::SqlitePool>,
    market_name: &str,
) {
    const ATTEMPTS: usize = 3;
    for attempt in 1..=ATTEMPTS {
        lifecycle.cancel_all(venue, positions).await;
        settle_lifecycle_reports(pool, lifecycle).await;
        if !lifecycle.tracks_anything().await {
            return;
        }
        if attempt < ATTEMPTS {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    let left = lifecycle.tracked_ids().await;
    warn!(
        "🏁 Leaving \"{market_name}\" with {} order(s) the venue could not settle: {left:?} — they stay at the venue untracked; a fill is adopted by the account sweep",
        left.len(),
    );
}

/// Write what the lifecycle learned about resting quotes to their rows: a quote
/// released without a fill (cancelled at rotation or stand-down, pulled, or
/// stale) loses its `pending` row; a tracked order the fill feed confirmed has
/// its row sized to the venue's count and marked confirmed. The lifecycle has
/// no database access, so the loop does this after every reconcile and every
/// cancel-all [B74].
async fn settle_lifecycle_reports(pool: &Option<sqlx::SqlitePool>, lifecycle: &OrderLifecycle) {
    let released = lifecycle.take_released().await;
    let confirmed = lifecycle.take_confirmed().await;
    let Some(p) = pool else { return };
    let squadron = lifecycle.squadron_id();
    for q in released {
        db::close_pending_open_position_for(p, squadron, &q.strategy, q.market.as_str()).await;
    }
    for c in confirmed {
        db::set_open_position_fill(p, squadron, &c.strategy, c.market.as_str(), c.filled, c.price).await;
        db::confirm_position_status_for(p, squadron, &c.strategy, c.market.as_str()).await;
    }
}

fn touch_heartbeat(hb: &AtomicU64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    hb.store(now, AtomicOrdering::Relaxed);
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
    // Viper-status registry key for this squadron (the crypto underlying or the
    // market class), so a venue-side refusal shows on the viper's card.
    status_scope: &str,
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
// A simulated taker pays what a real one pays. This was ZERO, so every
                    // ghost taker entry cost one leg instead of two and ghost P&L was
                    // optimistic against live by the entry fee. A post-only order is exempt:
                    // the CLOB charges the taker, so a resting quote pays nothing to open.
                    fee: if p.post_only { Decimal::ZERO } else { crate::venues::taker_fee_per_share(p.price) * p.shares },
                };
                let (ga, gb) = (ghost_of(params), ghost_of(pp));
                record_guard(squadron_id, positions, strategy_name, params, Some(&pp.token_id), &ga).await;
                record_guard(squadron_id, positions, strategy_name, pp, Some(&params.token_id), &gb).await;
                record_entry(squadron_id, pool, scope, strategy_name, params, &ga).await;
                record_entry(squadron_id, pool, scope, strategy_name, pp, &gb).await;
                return true;
            }
            // Both legs spend from the same market's pool.
            let cost = params.price * params.shares + pp.price * pp.shares;
            if let Err(refusal) = funding_preflight(venue, positions, squadron_id, &params.token_id, cost).await {
                refusal.report(strategy_name, status_scope, &params.token_id);
                return false;
            }
            let legs = [
                order_params_to_intent(params, Side::Buy),
                order_params_to_intent(pp, Side::Buy),
            ];
            match venue.place_atomic(legs).await {
                Ok([a, b]) => {
                    info!("✅ [{strategy_name}] entry pair: {} @ {:.4} | {} @ {:.4}",
                        a.order_id, a.price, b.order_id, b.price);
                    // [B74] A leg the venue acknowledged UNFILLED rests on the
                    // book: it is guarded and written at the size placed
                    // (unconfirmed) and tracked as a quote with the pair's stale
                    // window, so its fill sizes it by order id and a cancel
                    // releases it. Booking the zero the venue returned guarded
                    // nothing and tracked a quantity of zero: the first fill
                    // event then confirmed the leg at zero shares and the rest
                    // of the order worked on untracked. A leg that traded is
                    // guarded at its fill and tracked for the size placed.
                    let placed_a = Fill { filled: params.shares.round_dp(2), ..a.clone() };
                    let placed_b = Fill { filled: pp.shares.round_dp(2), ..b.clone() };
                    let a_rests = acknowledged_resting(Side::Buy, params.order_type, a.filled, &a.order_id);
                    let b_rests = acknowledged_resting(Side::Buy, pp.order_type, b.filled, &b.order_id);
                    record_guard(squadron_id, positions, strategy_name, params, Some(&pp.token_id), if a_rests { &placed_a } else { &a }).await;
                    record_guard(squadron_id, positions, strategy_name, pp, Some(&params.token_id), if b_rests { &placed_b } else { &b }).await;
                    if a_rests {
                        lifecycle.track_pair_leg(&placed_a, strategy_name, params.order_type, Some(pp.token_id.clone())).await;
                    } else {
                        lifecycle.track(&placed_a, strategy_name, params.order_type, Some(pp.token_id.clone())).await;
                    }
                    if b_rests {
                        lifecycle.track_pair_leg(&placed_b, strategy_name, pp.order_type, Some(params.token_id.clone())).await;
                    } else {
                        lifecycle.track(&placed_b, strategy_name, pp.order_type, Some(params.token_id.clone())).await;
                    }
                    record_entry(squadron_id, pool, scope, strategy_name, params, if a_rests { &placed_a } else { &a }).await;
                    record_entry(squadron_id, pool, scope, strategy_name, pp, if b_rests { &placed_b } else { &b }).await;
                    if let Some(p) = pool { sync_dashboard(squadron_id, venue, p, positions, Some(lifecycle), starting, None).await; }
                    true
                }
                Err(e) => {
                    if !note_unfunded_rejection(venue, &e, strategy_name, status_scope, &params.token_id).await {
                        warn!("[{strategy_name}] atomic entry failed: {e}");
                    }
                    false
                }
            }
        }
        StrategySignal::Entry { params, pair_params: None } => {
            dispatch_single(squadron_id, venue, pool, scope, positions, lifecycle, strategy_name, status_scope, params, Side::Buy, starting)
                .await
                .is_some()
        }
        StrategySignal::MakerQuote { yes, no } => {
            let mut acted = false;
            for q in [yes.as_ref(), no.as_ref()].into_iter().flatten() {
                // Ghost quotes REST; they do not fill. See `helpers::ghost_quotes`
                // and the crossing check in the tick loop. Routing them through
                // `dispatch_single` instead would stamp an instant full fill at
                // the quote price, which is the treadmill this replaced.
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
                // A quote already rests on this leg, or a fill from one is held:
                // the viper re-emits every tick, and the intl patrol has always
                // dropped the re-emission while a guard exists [B74].
                if positions.lock().await.contains_key(&PositionKey::new(squadron_id, strategy_name, q.token_id.clone())) {
                    continue;
                }
                if dispatch_single(squadron_id, venue, pool, scope, positions, lifecycle, strategy_name, status_scope, q, Side::Buy, starting)
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
            // The intl patrol has always returned before its real cancel path for
            // this reason; this mirrors it.
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
            // The quotes this engine tracks go first, by order id [B74]: a
            // cancelled quote that never traded is released, one that traded
            // before the cancel is kept as the position it is, and a cancel the
            // venue does not confirm keeps the quote tracked with its guard.
            for tok in tokens {
                let n = lifecycle.cancel_resting_for(venue, positions, strategy_name, tok).await;
                if n > 0 {
                    info!("🚫 [{strategy_name}] maker quote-pulled: {tok} — {n} tracked resting order(s) cancelled (toxic book)");
                    acted = true;
                }
            }
            settle_lifecycle_reports(pool, lifecycle).await;
            // The account scan catches a quote placed before a restart. An
            // order this engine tracks is a live quote (another viper's, or
            // this one's that the venue just refused to cancel) and is left alone.
            let open = venue.open_orders().await.unwrap_or_default();
            for tok in tokens {
                for ord in open.iter().filter(|o| &o.market == tok) {
                    if lifecycle.tracked_owner(&ord.order_id).await.is_some() { continue; }
                    if let Err(e) = venue.cancel(ord.order_id.clone()).await {
                        warn!("[{strategy_name}] maker quote-pull cancel failed for {} ({}): {e}", ord.order_id, tok);
                    } else {
                        info!("🚫 [{strategy_name}] maker quote-pulled: {} — resting order cancelled (toxic book)", tok);
                        acted = true;
                    }
                }
                // The guard goes only when no tracked order remains on the leg
                // and it is still a quote; a guard the fill feed or the cancel's
                // own reading confirmed is a position and stays.
                if !lifecycle.resting_for(strategy_name, tok).await {
                    let key = PositionKey::new(squadron_id, strategy_name, tok.clone());
                    let mut map = positions.lock().await;
                    if map.get(&key).is_some_and(|p| p.fill_confirmed_at.is_none()) { map.remove(&key); }
                }
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
        //
        // Helm emits its take-profit through this same signal, so on this venue
        // the Helm take-profit is a FAK at the bid once the bid reaches the
        // target, and that leg is charged. `venues::helm_take_profit_leg()` says
        // so to the fee verdict; implementing the resting exit here means
        // flipping that arm to `Resting` in the same change.
        StrategySignal::MakerRestingExit { .. } => false,
        StrategySignal::Exit { params, reason, exit_pair } => {
            // An exit that didn't fill will re-signal on the very next 50ms tick
            // (the SL condition is still true), so back off between attempts
            // rather than hammering the order endpoint 20×/sec.
            if exit_retry_backed_off(strategy_name, params.token_id.as_str()) {
                return false;
            }
            // One sale per token at a time: hold the claim for this exit so a
            // Close Now clicked while the order is in flight is refused, and
            // stand aside while the operator holds it. Taken before the release
            // check below so a Close Now cannot finish in between. Released when
            // this arm returns, by Drop [B73].
            let Some(_exit_claim) = db::ExitClaim::take(params.token_id.as_str()) else {
                return false;
            };
            // The operator closed this position by hand (the Trade Log's Close
            // Now) and this loop has not ticked since: the row is gone, so drop
            // the guard here instead of selling contracts no longer held. Only a
            // guard opened before the close counts; a re-entry since is a new
            // position and keeps its guard [B73].
            {
                let key = PositionKey::new(squadron_id, strategy_name, params.token_id.clone());
                let mut map = positions.lock().await;
                let opened_at = map.get(&key).map(|p| p.opened_at);
                if opened_at.is_some_and(|t| db::take_operator_release(strategy_name, params.token_id.as_str(), t)) {
                    map.remove(&key);
                    info!("🧾 [{strategy_name}] {} was closed by the operator — guard dropped, no exit sent", params.token_id);
                    return false;
                }
            }
            // [B74] An exit on a guard that is still an unfilled resting quote is
            // a cancel, not a sale: there is nothing to sell. A quote lifted in
            // part has a confirmed guard sized by the fill feed; its remainder
            // is cancelled here and what is held is sold below.
            if lifecycle.resting_for(strategy_name, &params.token_id).await {
                let key = PositionKey::new(squadron_id, strategy_name, params.token_id.clone());
                lifecycle.cancel_resting_for(venue, positions, strategy_name, &params.token_id).await;
                settle_lifecycle_reports(pool, lifecycle).await;
                // Still working after the cancel attempt (the venue did not
                // confirm the cancel): nothing is sold this tick whatever the
                // guard says. A cancelled order whose fill is still being read
                // no longer works; what its guard holds is the venue's own
                // figure and may be sold.
                // Standing down is the exception: the squadron is leaving, so
                // what the guard holds (the venue's own figure for this order)
                // is sold now; a remainder the venue would not cancel is logged
                // by the leaving path and any later fill of it is adopted by
                // the account sweep.
                if lifecycle.working_for(strategy_name, &params.token_id).await && reason != "SquadronStandDown" {
                    warn!("↩️ [{strategy_name}] exit ({reason}) on {} — its resting order could not be cancelled this tick, nothing sold; retrying", params.token_id);
                    arm_exit_retry_backoff(strategy_name, params.token_id.as_str());
                    return false;
                }
                // What the cancel left behind decides. No guard: the quote was
                // released, nothing to sell. A guard still unconfirmed: the
                // cancel did not go through and the quote may still be working,
                // so nothing is sold this tick. A confirmed guard: part or all
                // of the quote traded; it is sold at the size the venue reported,
                // which the viper re-reads from the map on its next tick if the
                // size it signalled was the one placed.
                let state = positions.lock().await.get(&key).map(|p| (p.fill_confirmed_at.is_some(), p.shares));
                match state {
                    None => {
                        info!("🚫 [{strategy_name}] exit ({reason}) on an unfilled quote {} — resting order cancelled, nothing to sell", params.token_id);
                        return true;
                    }
                    Some((false, _)) => {
                        warn!("↩️ [{strategy_name}] exit ({reason}) on quote {} — the cancel did not go through, nothing sold this tick; retrying", params.token_id);
                        arm_exit_retry_backoff(strategy_name, params.token_id.as_str());
                        return false;
                    }
                    Some((true, held)) if held != params.shares => {
                        info!("↩️ [{strategy_name}] exit ({reason}) on {}: the quote traded {held}, not the {} signalled — the viper re-emits at the traded size", params.token_id, params.shares);
                        return false;
                    }
                    Some((true, _)) => {}
                }
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
            let exit_fill = dispatch_single(squadron_id, venue, pool, scope, positions, lifecycle, strategy_name, status_scope, params, Side::Sell, starting).await;
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

#[cfg(test)]
mod exit_retry_backoff_tests {
    use super::*;

    /// A failed exit arms a per-(strategy, token) backoff that holds the next
    /// attempt off, a filled exit clears it, and neither touches another token or
    /// another strategy on the same token. Names are unique to this test because
    /// the map is process-wide.
    #[test]
    fn a_failed_exit_backs_off_only_its_own_strategy_and_token() {
        let (s, other_s) = ("KalshiBackoffTestStrategy", "KalshiBackoffTestOtherStrategy");
        let (tok, other_tok) = ("kalshi-backoff-test-token", "kalshi-backoff-test-other-token");
        assert!(!exit_retry_backed_off(s, tok), "nothing is armed before a failure");

        arm_exit_retry_backoff(s, tok);
        assert!(exit_retry_backed_off(s, tok), "a failed exit must back off the retry");
        assert!(!exit_retry_backed_off(s, other_tok), "another token is unaffected");
        assert!(!exit_retry_backed_off(other_s, tok), "another strategy on the same token is unaffected");

        clear_exit_retry_backoff(s, tok);
        assert!(!exit_retry_backed_off(s, tok), "a filled exit clears the backoff");
    }
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
    status_scope: &str,
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
// A simulated taker pays what a real one pays. This was ZERO, so every
            // ghost taker entry cost one leg instead of two and ghost P&L was
            // optimistic against live by the entry fee. A post-only order is exempt:
            // the CLOB charges the taker, so a resting quote pays nothing to open.
            fee: if params.post_only { Decimal::ZERO } else { crate::venues::taker_fee_per_share(params.price) * params.shares },
        };
        if matches!(side, Side::Buy) {
            record_guard(squadron_id, positions, strategy_name, params, None, &ghost).await;
            record_entry(squadron_id, pool, scope, strategy_name, params, &ghost).await;
        }
        return Some(ghost);
    }
    // Funding pre-flight: refuse a buy the market's exchange shard cannot cover,
    // on the viper's own card, instead of letting the venue reject it every
    // tick (3,239 identical rejections on 2026-10-18). Exits spend nothing.
    if matches!(side, Side::Buy) {
        let cost = params.price * params.shares;
        if let Err(refusal) = funding_preflight(venue, positions, squadron_id, &params.token_id, cost).await {
            refusal.report(strategy_name, status_scope, &params.token_id);
            return None;
        }
    }
    // [B74] One resting quote per viper per leg. A viper re-emits its quote
    // every tick while its gates hold; with the first one on the book and its
    // guard in the map, the next is not placed. Without this the Maker saw its
    // own bid as the new best bid and quoted a tick above it every tick
    // (2026-10-29: five bids in two seconds, then `insufficient_balance`).
    if matches!(side, Side::Buy)
        && matches!(params.order_type, TimeInForce::Gtc | TimeInForce::Gtd)
        && lifecycle.working_for(strategy_name, &params.token_id).await
    {
        debug!("[{strategy_name}] a quote already rests on {} — not placing another", params.token_id);
        return None;
    }
    match venue.place_order(order_params_to_intent(params, side)).await {
        Ok(f) => {
            // A 2xx is not a fill. Kalshi answers 200 with `fill_count: 0` for an
            // order that crossed nothing, and the old code booked that as done —
            // phantom positions on entry, abandoned live ones on exit.
            if f.filled <= Decimal::ZERO {
                // [B74] A good-till-cancelled buy that matched nothing is RESTING,
                // not gone. Until 2026-10-29 it was dropped here: nothing tracked
                // it, rotation never cancelled it, and its later fill surfaced as
                // a `ChainAdopted` holding under whichever squadron synced first.
                // It is now guarded at the size and limit placed (unconfirmed),
                // handed to the lifecycle under the venue's order id, and written
                // as a `pending` row, exactly as the intl CLOB has always carried
                // a resting quote. The fill feed confirms and sizes it by order
                // id; rotation, stand-down, the viper's pull and the stale
                // backstop cancel it and close the row.
                if acknowledged_resting(side, params.order_type, f.filled, &f.order_id) {
                    let resting = Fill {
                        order_id: f.order_id.clone(),
                        market: params.token_id.clone(),
                        filled: params.shares.round_dp(2),
                        price: params.price,
                        // Nothing has traded, so nothing has been charged yet.
                        // The fill feed carries no fee, so a maker fee the venue
                        // charges when the quote is lifted (Kalshi does on some
                        // series) is not on the row.
                        fee: Decimal::ZERO,
                    };
                    record_guard(squadron_id, positions, strategy_name, params, None, &resting).await;
                    lifecycle.track_resting(&resting, strategy_name, params.order_type, None).await;
                    record_entry(squadron_id, pool, scope, strategy_name, params, &resting).await;
                    info!("📝 [{strategy_name}] resting {} @ {:.4} × {:.2} — order {} is on the book, tracked until it fills, is pulled or is cancelled at rotation",
                        params.token_id, params.price, resting.filled, f.order_id);
                    return None;
                }
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
                // [B74] The venue said this much of a good-till-cancelled buy
                // traded at placement, so the guard is confirmed for it now; what
                // did not fill keeps working and is tracked for the size PLACED,
                // the fill feed sizing the guard as it trades. An order filled
                // in full has nothing left to track.
                if matches!(params.order_type, TimeInForce::Gtc | TimeInForce::Gtd) {
                    if let Some(p) = positions.lock().await.get_mut(&PositionKey::new(squadron_id, strategy_name, params.token_id.clone())) {
                        p.fill_confirmed_at = Some(Utc::now());
                    }
                    let placed = params.shares.round_dp(2);
                    if f.filled < placed && !f.order_id.0.is_empty() {
                        let working = Fill { filled: placed, ..f.clone() };
                        lifecycle.track_resting(&working, strategy_name, params.order_type, None).await;
                    }
                }
                record_entry(squadron_id, pool, scope, strategy_name, params, &f).await;
            }
            if let Some(p) = pool { sync_dashboard(squadron_id, venue, p, positions, Some(lifecycle), starting, None).await; }
            Some(f)
        }
        Err(e) => {
            if !note_unfunded_rejection(venue, &e, strategy_name, status_scope, &params.token_id).await {
                warn!("[{strategy_name}] {side:?} order failed: {e}");
            }
            None
        }
    }
}

/// Kalshi's rejection code for an order on an exchange shard the account has
/// not funded (`"Exchange user not found"` in its details).
const UNFUNDED_POOL_ERROR_CODE: &str = "insufficient_shard_balance";
/// How long buys on a pool pause after the venue rejects one as unfunded. The
/// pause clears early the moment a balance reading shows the pool funded.
const FUNDING_PAUSE_SECS: u64 = 300;
/// How often a refused buy repeats its warning and its card refusal while the
/// condition persists: one line per interval, not one per tick.
const FUNDING_REPORT_SECS: u64 = 30;

/// Why a buy was refused for funding, and whether this is the tick that says so.
struct FundingRefusal {
    reason: String,
    /// False while the last report for this pool is younger than
    /// [`FUNDING_REPORT_SECS`]; the card keeps the earlier reason.
    report: bool,
}

impl FundingRefusal {
    fn report(&self, strategy_name: &str, status_scope: &str, token: &MarketId) {
        if self.report {
            warn!("💰 [{strategy_name}] {token} — {}", self.reason);
            crate::helpers::viper_status::report_reason(status_scope, strategy_name, &self.reason);
        }
    }
}

#[derive(Default)]
struct FundingPause {
    /// Set after the venue itself rejected an order on this pool.
    until: Option<Instant>,
    last_report: Option<Instant>,
}

/// Per collateral pool (an exchange shard), never per market: one pool funds
/// every market on it, and one rejection says the same about all of them.
fn funding_pauses() -> &'static std::sync::Mutex<HashMap<String, FundingPause>> {
    static P: std::sync::OnceLock<std::sync::Mutex<HashMap<String, FundingPause>>> = std::sync::OnceLock::new();
    P.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn funding_pause_active(pool_id: &str) -> bool {
    let m = funding_pauses().lock().unwrap_or_else(|p| p.into_inner());
    m.get(pool_id).and_then(|p| p.until).is_some_and(|u| Instant::now() < u)
}

fn arm_funding_pause(pool_id: &str) {
    let mut m = funding_pauses().lock().unwrap_or_else(|p| p.into_inner());
    m.entry(pool_id.to_string()).or_default().until = Some(Instant::now() + Duration::from_secs(FUNDING_PAUSE_SECS));
}

fn clear_funding_pause(pool_id: &str) {
    let mut m = funding_pauses().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(p) = m.get_mut(pool_id) { p.until = None; }
}

/// True once per [`FUNDING_REPORT_SECS`] for a pool, and records the report.
fn funding_report_due(pool_id: &str) -> bool {
    let mut m = funding_pauses().lock().unwrap_or_else(|p| p.into_inner());
    let p = m.entry(pool_id.to_string()).or_default();
    let due = p.last_report.is_none_or(|t| t.elapsed() >= Duration::from_secs(FUNDING_REPORT_SECS));
    if due { p.last_report = Some(Instant::now()); }
    due
}

/// Does this squadron hold the other leg of `token`\'s market? Kalshi nets YES
/// against NO in one market, so buying the opposite leg of a holding reduces
/// exposure and needs no collateral; the pre-flight must not refuse it.
async fn holds_opposite_leg(positions: &Arc<Mutex<PositionMap>>, squadron_id: &str, token: &MarketId) -> bool {
    let (ticker, is_yes) = super::split_market_id(token.as_str());
    let opposite = leg_id(&ticker, !is_yes);
    let map = positions.lock().await;
    // A guard that is still an unfilled quote holds nothing the venue can net [B74].
    map.iter().any(|(k, p)| k.squadron == squadron_id && k.market.as_str() == opposite && p.shares > Decimal::ZERO && p.fill_confirmed_at.is_some())
}

/// May a buy costing `cost` on `token` go to the venue?
///
/// Fails open: an unknown shard, a missing breakdown or a failed balance read
/// all let the order through, as before shards existed. Refuses only when the
/// venue\'s own figure for the market\'s pool cannot cover the order, or the
/// venue rejected an order on that pool within [`FUNDING_PAUSE_SECS`] and the
/// pool still cannot cover this one. A refusal here never arms a pause: the
/// check is cheap (a cached reading) and one oversized order must not stop a
/// smaller one from another viper.
async fn funding_preflight(
    venue: &KalshiVenue,
    positions: &Arc<Mutex<PositionMap>>,
    squadron_id: &str,
    token: &MarketId,
    cost: Decimal,
) -> Result<(), FundingRefusal> {
    if holds_opposite_leg(positions, squadron_id, token).await {
        return Ok(());
    }
    let Ok(Some(pool)) = venue.collateral_pool_for(token).await else { return Ok(()) };
    if pool.balance >= cost {
        if funding_pause_active(&pool.id) {
            // The operator moved funds since the rejection: trade again now.
            clear_funding_pause(&pool.id);
            info!("💰 the {} pool is funded again (${:.2}); buys resume", pool.label, pool.balance);
        }
        return Ok(());
    }
    let reason = if funding_pause_active(&pool.id) {
        format!(
            "venue funding: the venue rejected an order on the {} pool (${:.2} there); buys on it pause up to {}s and resume as soon as a balance reading covers them",
            pool.label, pool.balance, FUNDING_PAUSE_SECS
        )
    } else {
        format!(
            "venue funding: the {} pool holds ${:.2} and this order needs ${:.2}; move funds to it (the venue does not move them for API orders)",
            pool.label, pool.balance, cost
        )
    };
    Err(FundingRefusal { reason, report: funding_report_due(&pool.id) })
}

/// If `err` is the venue\'s unfunded-shard rejection, pause buys on that pool,
/// say so once, and return true; false leaves the caller to log the error.
async fn note_unfunded_rejection(
    venue: &KalshiVenue,
    err: &anyhow::Error,
    strategy_name: &str,
    status_scope: &str,
    token: &MarketId,
) -> bool {
    if !err.to_string().contains(UNFUNDED_POOL_ERROR_CODE) {
        return false;
    }
    let (ticker, _) = super::split_market_id(token.as_str());
    let (pool_id, label) = match venue.collateral_pool_for(token).await {
        Ok(Some(p)) => (p.id, p.label),
        _ => (format!("market:{ticker}"), format!("pool of {ticker}")),
    };
    arm_funding_pause(&pool_id);
    let reason = format!(
        "venue funding: the venue rejected the order ({UNFUNDED_POOL_ERROR_CODE}) — the {label} holds no balance it can spend; move funds to it and buys resume within {FUNDING_PAUSE_SECS}s"
    );
    if funding_report_due(&pool_id) {
        warn!("💰 [{strategy_name}] {token} — {reason}");
        crate::helpers::viper_status::report_reason(status_scope, strategy_name, &reason);
    }
    true
}

/// Persist a new position to `entries` + `open_positions`.
///
/// Without this the Control Tower's positions panel had to fall back on
/// [`sync_dashboard`]'s venue sweep, which knows the ticker but not which viper
/// owns it — every live position was mislabeled as ArbitrageStrategy.
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
            p, scope, squadron_id, strategy_name, params.token_id.as_str(), &params.market_name,
            side, fill.price, fill.filled, params.ghost_mode, status,
        ).await;
        // [B67] The fee the venue charged for this fill goes on the row, not
        // only on the in-memory guard. The settlement sweep nets the row's
        // `entry_fee` out of the P&L it books, and with the column left NULL
        // every settlement on this venue was booked gross with `fees = 0`
        // (about seven cents a trade at five Kalshi contracts). A fee of zero
        // is left as NULL: the venue reported none, and the row says so.
        if !fill.fee.is_zero() {
            db::set_open_position_entry_fee_scoped(
                p, squadron_id, strategy_name, params.token_id.as_str(), fill.fee,
            ).await;
        }
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
    // The squadron's order lifecycle, which knows which unconfirmed guards are
    // quotes still working at the venue [B74].
    lifecycle: Option<&OrderLifecycle>,
    starting: Decimal,
    // The loop's current available collateral, returned unchanged when the
    // venue cannot be read; `None` on the loop's first sync and on the
    // post-fill syncs, whose result is not read. See
    // `core::collateral_after_failed_read`.
    last_collateral: Option<Decimal>,
    // Returns (available collateral, total account value). The total is `None`
    // when the venue could not be read this pass: the loop then leaves its
    // session P&L where it was, because a total it did not measure would read
    // as a session P&L of zero and clear the drawdown guard for the pass.
) -> (Decimal, Option<Decimal>) {
    let collateral = match venue.collateral().await {
        Ok(c) => c,
        Err(e) => {
            let kept = crate::venues::core::collateral_after_failed_read(last_collateral, starting);
            warn!(
                "Kalshi dashboard sync: collateral query failed: {e} — available collateral stays at the \
                 last reading (${kept:.2}); no P&L snapshot this pass"
            );
            return (kept, None);
        }
    };
    // A failed positions fetch ABANDONS the sweep — it must never read as "the
    // account holds nothing". This used to be `unwrap_or_default()`, which
    // turned any transient error (timeout, 5xx, auth blip, rate limit) into an
    // empty `live_ids`: every confirmed row then read as stale, and the purge
    // below booked-and-deleted the venue's entire open-positions table — plus,
    // since the settlement sweep probes every non-live confirmed row, a burst
    // of resolution calls on the way. The intl chain-sync has always treated a
    // positions() error as "skip this pass" (`sync_open_positions_with_chain`
    // in tasks/cleanup.rs); same rule here.
    //
    // Collateral was already fetched successfully, so the caller gets the real
    // figure; no total is returned, so the loop keeps the session P&L it had
    // rather than reporting a phantom drop equal to the unpriced positions, and
    // no P&L snapshot is written from a total we could not compute.
    let positions = match venue.positions().await {
        Ok(p) => p,
        Err(e) => {
            warn!("Kalshi dashboard sync: positions query failed — skipping reconcile/purge this pass: {e}");
            return (collateral, None);
        }
    };

    // token → (owning viper, market name) from the live guard map, so a venue
    // holding is attributed to the viper that opened it. Holdings with no guard
    // (prior session, manual trade) are adopted under a neutral label rather
    // than being blamed on a viper that never traded them.
    let candidates: Vec<(String, String, String, bool)> = {
        let map = guards.lock().await;
        map.iter()
            .filter(|(k, _)| k.squadron == squadron_id)
            .map(|(k, p)| (k.market.as_str().to_string(), k.strategy.clone(), p.market_name.clone(), p.fill_confirmed_at.is_none()))
            .collect()
    };
    // [B74] An unfilled quote with its order still working owns nothing the
    // venue holds: left out, so another viper's (or squadron's) contracts on
    // the leg never promote the quote's pending row to a confirmed position.
    let mut owners: HashMap<String, (String, String)> = HashMap::new();
    for (token, strategy, market_name, unconfirmed) in candidates {
        if unconfirmed {
            if let Some(lc) = lifecycle {
                if lc.resting_for(&strategy, &MarketId::new(token.clone())).await { continue; }
            }
        }
        owners.insert(token, (strategy, market_name));
    }

    let mut live_ids = std::collections::HashSet::new();
    let mut positions_value = Decimal::ZERO;
    // Reconciliation scope: this sweep knows the venue but not a holding's
    // market class or underlying, so those columns stay honestly NULL rather
    // than guessed — same rule as the off-strategy booking path in db.rs. It
    // only reaches rows adopted HERE: a row the owning viper already wrote
    // carries its full filing columns and the insert below is a no-op for it.
    let adopt_scope = TradeScope::new("", KALSHI_VENUE, None, None);
    for p in &positions {
        let sym = p.market.as_str();
        live_ids.insert(sym.to_string());
        // Both arms adopt a WALLET-sized holding, so both write
        // `engine_attributed = 0`. `record_open_position` stamps the flag as 1,
        // which claims the share count came from the engine's own fill; a row
        // written here with that stamp would be believed over the wallet by any
        // corrector that later runs on this venue, and the flag's one job is that
        // it cannot be forged by a wallet reading. Same defect, same fix, as the
        // Polymarket US sweep this loop mirrors.
        match owners.get(sym) {
            // The viper's own entry row already exists — the venue confirming the
            // holding is what promotes it out of `pending`. The insert is a no-op
            // for it; it only writes when the viper's row is missing, and then
            // the size is the wallet's, not the fill's.
            Some((strategy, market_name)) => {
                db::record_adopted_position(
                    pool, &adopt_scope, squadron_id, strategy, sym, market_name, side_label(sym), p.avg_price, p.shares,
                ).await;
                db::confirm_position_status_for(pool, squadron_id, strategy, sym).await;
            }
            // `positions()` is the whole account; this map is one squadron,
            // rebuilt on every rotation. A holding some row already claims
            // (another squadron's viper, or this squadron's own before the
            // rotation) is not an orphan: adopting it wrote duplicate
            // "ChainAdopted" rows for every fill once three squadrons shared
            // the account (2026-10-18), each later booked as a $0 off-strategy
            // exit, and each a second settlement booking had the position gone
            // to settlement. A holding no row claims waits out a short grace
            // before adoption, because the owner's row lands after its fill.
            // See `db::open_row_claims_token`.
            None => match db::open_row_claims_token(sym).await {
                Ok(true) => {
                    db::forget_unclaimed_holding(squadron_id, sym);
                    debug!("Kalshi dashboard sync [{squadron_id}]: {sym} is already an open-positions row — not adopting");
                }
                Ok(false) if db::unclaimed_holding_adoptable(squadron_id, sym) => {
                    db::forget_unclaimed_holding(squadron_id, sym);
                    db::record_adopted_position(
                        pool, &adopt_scope, squadron_id, "ChainAdopted", sym, sym, side_label(sym), p.avg_price, p.shares,
                    ).await;
                }
                Ok(false) => {
                    debug!("Kalshi dashboard sync [{squadron_id}]: {sym} unclaimed — adopting if still so after {}s", db::UNCLAIMED_ADOPT_GRACE_SECS);
                }
                // Could not tell: a duplicate row is worse than a thirty-second
                // delay, so adopt nothing this pass.
                Err(e) => {
                    warn!("Kalshi dashboard sync [{squadron_id}]: open-row lookup for {sym} failed — not adopting this pass: {e}");
                }
            },
        }
        positions_value += p.shares * p.avg_price;
    }

    // ── Settled positions the exchange already paid ─────────────────────────
    //
    // Kalshi settles cash-side: when a market is determined, winning contracts
    // pay $1.00 each straight into the balance and the position is dropped from
    // `/portfolio/positions` — there is no redemption step and no event this
    // loop observes. A position that settles between two sweeps therefore
    // arrives at the purge as "not live", where it used to be booked at its
    // last mark or, with no usable mark (a loser marks near $0.00, which fails
    // the mark's own `> 0` guard), deleted with nothing booked at all — real
    // cash moved and the ledger stayed empty. Same incident class as the
    // 2026-08-31 Polymarket International loss of a winning $0.80 settlement.
    //
    // So ask the exchange what each stale market's `result` was, rather than
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
    // row's own share count, which Kalshi rows retain (nothing zeroes them —
    // the intl chain-drift corrector does not run here).
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
        // WITHOUT spending a live API call. Only a token the exchange has not
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
                info!("🧾 Settled position detected [{}]: exchange result prices this leg at ${:.2} — booking", token, px);
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
                warn!("⚠️ Kalshi settlement resolution unavailable for {} after {}h — falling back to mark-priced reconciliation",
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
    (collateral, Some(total))
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
        window_secs: pair.window_secs,
        is_neg_risk: false,
        // Kalshi's market id is the ticker. The CAG publishes it as the
        // squadron's `market_id`, which the collateral-pools endpoint uses to
        // mark the shard each deployed squadron spends from.
        condition_id: pair.ticker.clone(),
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

    /// [B67] The fill's fee reaches the `open_positions` row, and the settlement
    /// sweep nets it out of the P&L it books.
    ///
    /// Before 1.5.2 the fee lived only on the in-memory guard, so every Kalshi
    /// and Polymarket US settlement was booked gross with `fees = 0`: Anchor's
    /// own ledger netted $0.07 a trade that the trade log did not (2026-10-23).
    #[tokio::test]
    async fn a_fills_fee_is_stamped_on_the_row_and_netted_at_settlement() {
        let pool = db::memory_pool_for_tests().await;
        db::run_migrations(&pool).await;
        let scope = TradeScope::shard_only("test");
        let mut p = params(dec!(5), dec!(0.28));
        p.market_name = "Bitcoin price above $81,299.99?".into();
        record_entry("btc-open", &Some(pool.clone()), &scope, "AnchorStrategy", &p,
                     &fill(dec!(5), dec!(0.28), dec!(0.0635))).await;

        let fee: Option<String> = sqlx::query_scalar(
            "SELECT entry_fee FROM open_positions WHERE token_id = 'KX-1#yes' AND strategy = 'AnchorStrategy'"
        ).fetch_one(&pool).await.unwrap();
        assert_eq!(fee.as_deref(), Some("0.0635"), "the fee is on the row");

        // The venue settles the contract at $1.
        let mut marks = std::collections::HashMap::new();
        marks.insert("KX-1#yes".to_string(), (Decimal::ONE, Decimal::ZERO));
        let swept = db::purge_stale_open_positions(
            &pool, &std::collections::HashSet::new(), &marks, &std::collections::HashSet::new(),
        ).await;
        assert_eq!(swept, 1);
        let (pnl, fees): (String, Option<String>) = sqlx::query_as(
            "SELECT pnl, fees FROM trades WHERE market = 'Bitcoin price above $81,299.99?'"
        ).fetch_one(&pool).await.unwrap();
        // (1.00 − 0.28) × 5 = 3.60 gross, 3.5365 net of the fee.
        assert_eq!(pnl.parse::<Decimal>().unwrap(), dec!(3.5365), "settlement P&L is net of the entry fee");
        assert_eq!(fees.as_deref(), Some("0.0635"));
    }

    /// [B67] A venue that reports no fee leaves the column unset rather than
    /// writing a zero that claims the fee is known.
    #[tokio::test]
    async fn a_zero_fee_is_not_stamped() {
        let pool = db::memory_pool_for_tests().await;
        db::run_migrations(&pool).await;
        let scope = TradeScope::shard_only("test");
        let p = params(dec!(5), dec!(0.28));
        record_entry("btc-open", &Some(pool.clone()), &scope, "AnchorStrategy", &p,
                     &fill(dec!(5), dec!(0.28), dec!(0))).await;
        let fee: Option<String> = sqlx::query_scalar(
            "SELECT entry_fee FROM open_positions WHERE token_id = 'KX-1#yes'"
        ).fetch_one(&pool).await.unwrap();
        assert_eq!(fee, None);
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
    /// is labeled in the UI exactly as the rotation loop would label it.
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

#[cfg(test)]
mod sports_game_series_tests {
    use super::{is_game_series_ticker, parse_game_series_list};

    /// The shipped default must consist entirely of game series, or the
    /// seeder would warn about its own configuration on every tick.
    #[test]
    fn the_default_series_list_is_all_game_series() {
        let (accepted, rejected) = parse_game_series_list(crate::config::KALSHI_SPORTS_GAME_SERIES);
        assert!(rejected.is_empty(), "default list carries non-game series: {rejected:?}");
        assert!(accepted.len() >= 5, "default list is suspiciously short: {accepted:?}");
        assert!(accepted.iter().any(|s| s == "KXNFLGAME"));
    }

    /// The market the seeder actually deployed on 2026-08-30 — a coaching
    /// personnel series under Kalshi's Sports category — must not be
    /// admittable through the list, however it is spelled.
    #[test]
    fn a_personnel_series_is_not_a_game_series() {
        assert!(!is_game_series_ticker("KXCOACHOUTNBADATE"));
        assert!(!is_game_series_ticker("KXNBAWINS"));
        assert!(!is_game_series_ticker("KXNFLRETIRE"));
        assert!(!is_game_series_ticker("KXBTC15M"));
        assert!(!is_game_series_ticker(""));
        let (accepted, rejected) = parse_game_series_list("KXNFLGAME, kxcoachoutnbadate ,KXATPMATCH");
        assert_eq!(accepted, vec!["KXNFLGAME".to_string(), "KXATPMATCH".to_string()]);
        assert_eq!(rejected, vec!["KXCOACHOUTNBADATE".to_string()]);
    }

    /// Every game-winner series Kalshi lists ends in one of three words
    /// (live catalog, 2026-09-09) — and the operator's casing and spacing
    /// must not matter, since the venue's tickers are uppercase.
    #[test]
    fn game_series_are_recognized_by_their_suffix_regardless_of_case() {
        for t in ["KXNFLGAME", "kxmlbgame", " KXWTAMATCH ", "KXUFCFIGHT", "KXEPLGAME"] {
            assert!(is_game_series_ticker(t), "{t:?} not recognized");
        }
        let (accepted, rejected) = parse_game_series_list("kxnflgame,KXNFLGAME,,KXMLBGAME");
        assert_eq!(accepted, vec!["KXNFLGAME".to_string(), "KXMLBGAME".to_string()], "duplicates and blanks must collapse");
        assert!(rejected.is_empty());
    }
}

/// Live checks against Kalshi. Ignored by default: they need the API key in
/// the environment and a network. Run with
/// `cargo test --no-default-features --features kalshi -- --ignored live_`.
#[cfg(test)]
mod live_discovery_checks {
    use super::*;

    /// The seeder's own selection for the sports class, end to end: every
    /// market discovered must belong to a game series, and the pick must be
    /// one of them. This is the path that chose "Will Steve Kerr be out
    /// before Oct 25, 2026?" on 2026-08-30.
    #[tokio::test]
    #[ignore = "live venue: needs credentials and network"]
    async fn live_sports_seeder_picks_a_game_market() {
        let venue = Arc::new(KalshiVenue::from_env().expect("Kalshi credentials"));
        let found = open_sports_game_markets(&venue).await;
        eprintln!("open game markets across configured series: {}", found.len());
        for m in found.iter().take(6) {
            eprintln!("  {} | {} | close={:?} | vol={}", m.ticker, m.title, m.close_time_utc(), m.volume_fp);
        }
        assert!(!found.is_empty(), "no open game markets in the configured series");
        for m in &found {
            let series = m.ticker.split('-').next().unwrap_or("");
            assert!(is_game_series_ticker(series), "{} is not from a game series", m.ticker);
        }
        let pick = select_auto_deploy_market(&venue, "sports", 7, 0.0).await
            .expect("a game market within 7 days");
        eprintln!("seeder pick: {pick}");
        assert!(is_game_series_ticker(pick.split('-').next().unwrap_or("")));
    }
}

/// Drop map entries the operator closed by hand (the Trade Log's Close Now)
/// since the last tick. Keyed by strategy and token, so another strategy's live
/// position on the same token is untouched, and consumed only by a loop that
/// holds the entry, so a sibling squadron's tick cannot take it. The exit arm
/// checks the same registry before selling, so the 30-second cadence here is
/// only how long the exposure caps keep counting a closed position [B73].
async fn release_operator_closed_positions(positions: &Arc<Mutex<PositionMap>>) {
    let mut map = positions.lock().await;
    let closed: Vec<PositionKey> = map.iter()
        .filter(|(k, p)| db::take_operator_release(&k.strategy, k.market.as_str(), p.opened_at))
        .map(|(k, _)| k.clone())
        .collect();
    for k in closed {
        map.remove(&k);
        info!("🧾 Released {}'s in-memory position on {} — the operator closed it from the Trade Log", k.strategy, k.market);
    }
}

/// Drop map entries for tokens whose `open_positions` row the settlement
/// sweep in `sync_dashboard` has just booked and deleted. The sweep is
/// DB-only; the map is this loop's to keep honest, or a settle-held leg keeps
/// counting against every exposure cap until the process restarts.
async fn release_swept_positions(positions: &Arc<Mutex<PositionMap>>) {
    for tok in db::take_released_positions() {
        let m = crate::venues::core::MarketId::new(tok.as_str());
        let mut map = positions.lock().await;
        let before = map.len();
        map.retain(|k, _| k.market != m);
        let dropped = before - map.len();
        if dropped > 0 {
            info!("🧾 Released {} in-memory position(s) on {} — the settlement sweep booked and closed the row", dropped, tok);
        }
    }
}

#[cfg(test)]
mod hourly_market_tests {
    use super::*;

    fn market(ticker: &str, open: &str, close: &str) -> KalshiMarket {
        KalshiMarket {
            ticker: ticker.into(), title: "Bitcoin price on Oct 17, 2026?".into(),
            status: "active".into(), strike_type: "greater".into(), floor_strike: Some(91_299.99),
            open_time: open.into(), close_time: close.into(),
            ..KalshiMarket::default()
        }
    }

    fn pair(ticker: &str, cadence: Option<Cadence>, volume: f64) -> KalshiPair {
        KalshiPair {
            ticker: ticker.into(), question: ticker.into(),
            long: MarketId::new(format!("{ticker}#yes")), short: MarketId::new(format!("{ticker}#no")),
            close_time: None, volume, underlying: "btc", strike: None, cadence,
            window_secs: cadence.map(Cadence::window_secs),
        }
    }

    /// Kalshi's `KXBTCD` series carries both a one-hour and a 25-hour listing.
    /// The published window tells them apart; the ticker alone called both daily.
    #[test]
    fn the_published_window_classifies_the_kxbtcd_series() {
        let hourly = pair_from_market(&market("KXBTCD-26OCT1719-T91299.99", "2026-10-17T22:00:00Z", "2026-10-17T23:00:00Z")).unwrap();
        assert_eq!(hourly.cadence, Some(Cadence::Hourly));
        assert_eq!(hourly.window_secs, Some(3600));
        let daily = pair_from_market(&market("KXBTCD-26OCT1817-T91249.99", "2026-10-17T20:00:00Z", "2026-10-18T21:00:00Z")).unwrap();
        assert_eq!(daily.cadence, Some(Cadence::Daily));
        assert_eq!(daily.window_secs, Some(90_000));
        let quarter = pair_from_market(&market("KXBTC15M-26OCT171830-30", "2026-10-17T22:15:00Z", "2026-10-17T22:30:00Z")).unwrap();
        assert_eq!(quarter.cadence, Some(Cadence::FifteenMin));
        // No published open: the ticker decides, and the window is unknown (B43).
        let blind = pair_from_market(&market("KXBTCD-28JAN1222-T99799.99", "", "2026-01-28T22:00:00Z")).unwrap();
        assert_eq!(blind.cadence, Some(Cadence::Daily));
        assert_eq!(blind.window_secs, None);
    }

    /// Odd windows stay unclassified rather than being forced into a shape.
    #[test]
    fn a_window_between_shapes_is_unclassified() {
        assert_eq!(Cadence::from_window(900), Some(Cadence::FifteenMin));
        assert_eq!(Cadence::from_window(3600), Some(Cadence::Hourly));
        assert_eq!(Cadence::from_window(86_400), Some(Cadence::Daily));
        assert_eq!(Cadence::from_window(7200), None);
    }

    /// The one-hour market is the primary when the board has one, however hot the
    /// 15-minute market is; without one the 15-minute market is, as before.
    #[test]
    fn the_hourly_market_is_primary_when_listed() {
        let board = vec![
            pair("KXBTC15M-A", Some(Cadence::FifteenMin), 50_000.0),
            pair("KXBTCD-DAILY", Some(Cadence::Daily), 20_000.0),
            pair("KXBTCD-HOUR-T81500", Some(Cadence::Hourly), 900.0),
            pair("KXBTCD-HOUR-T81000", Some(Cadence::Hourly), 300.0),
        ];
        let p = choose_primary(&board, None).unwrap();
        assert_eq!(p.ticker, "KXBTCD-HOUR-T81500", "the hottest strike of the hourly ladder");
        assert_eq!(choose_maker(&board, &p).unwrap().ticker, "KXBTCD-DAILY");
        let no_hourly: Vec<_> = board.iter().filter(|p| p.cadence != Some(Cadence::Hourly)).cloned().collect();
        assert_eq!(choose_primary(&no_hourly, None).unwrap().ticker, "KXBTC15M-A");
        // The operator's filter still narrows the field.
        assert_eq!(choose_primary(&board, Some("daily")).unwrap().ticker, "KXBTCD-DAILY");
    }

    /// A better cadence rotates at once; the same cadence rotates only for a
    /// clearly hotter market; a worse cadence never does.
    #[test]
    fn rotation_prefers_cadence_then_volume() {
        let quarter = pair("KXBTC15M-A", Some(Cadence::FifteenMin), 50_000.0);
        let hour = pair("KXBTCD-HOUR", Some(Cadence::Hourly), 100.0);
        assert!(should_rotate(&quarter, &hour, 0), "an hourly market appearing is the move the hour-shaped Vipers wait for");
        assert!(!should_rotate(&hour, &quarter, 0), "never back to the 15-minute market while an hourly is on the board");
        let hotter_hour = pair("KXBTCD-HOUR-2", Some(Cadence::Hourly), 100.0 + ROTATION_VOLUME_THRESHOLD + 1.0);
        let warmer_hour = pair("KXBTCD-HOUR-3", Some(Cadence::Hourly), 100.0 + ROTATION_VOLUME_THRESHOLD - 1.0);
        assert!(should_rotate(&hour, &hotter_hour, 0), "no published close: the volume rule, not a guessed pin");
        assert!(!should_rotate(&hour, &warmer_hour, 0));
    }

    /// Once an hourly market is five minutes into its window the loop holds it
    /// to the close: a hotter strike no longer moves it. Before that the volume
    /// rule still applies, so the first minutes after the top of the hour, when
    /// every strike is empty and the first pick is a guess, can correct
    /// themselves. A better cadence still wins at any minute.
    #[test]
    fn an_hourly_market_is_held_to_its_close_once_settled() {
        let close = chrono::DateTime::<Utc>::from_timestamp(1_792_540_800, 0).unwrap();
        let open = close.timestamp() - 3600;
        let mut hour = pair("KXBTCD-HOUR-A", Some(Cadence::Hourly), 100.0);
        hour.close_time = Some(close);
        let hotter = pair("KXBTCD-HOUR-B", Some(Cadence::Hourly), 100.0 + ROTATION_VOLUME_THRESHOLD + 1.0);
        assert!(hourly_pinned(&hour, open + HOURLY_SETTLE_SECS) && !hourly_pinned(&hour, open + HOURLY_SETTLE_SECS - 1));
        assert!(should_rotate(&hour, &hotter, open + 10), "ten seconds in: the pick may still correct itself");
        assert!(should_rotate(&hour, &hotter, open + HOURLY_SETTLE_SECS - 1));
        assert!(!should_rotate(&hour, &hotter, open + HOURLY_SETTLE_SECS), "settled: held to the close");
        assert!(!should_rotate(&hour, &hotter, close.timestamp() - 60));
        // The venue's own window length, not the cadence's nominal one, places the open.
        let mut odd = hour.clone();
        odd.window_secs = Some(3660);
        assert!(!should_rotate(&odd, &hotter, open - 60 + HOURLY_SETTLE_SECS) && should_rotate(&odd, &hotter, open - 60 + HOURLY_SETTLE_SECS - 1));
        // No published window length: the cadence's nominal hour places the open.
        let mut nominal = hour.clone();
        nominal.window_secs = None;
        assert!(!should_rotate(&nominal, &hotter, open + HOURLY_SETTLE_SECS) && should_rotate(&nominal, &hotter, open + HOURLY_SETTLE_SECS - 1));
        // Another underlying is never pinned against: a quiet hour's first pick
        // (ETH with twenty contracts) gives way once BTC is clearly busier, but
        // the ETH market holds against its own ladder.
        let mut eth = pair("KXETHD-HOUR-A", Some(Cadence::Hourly), 20.0);
        eth.underlying = "eth";
        eth.close_time = Some(close);
        let btc_busier = pair("KXBTCD-HOUR-E", Some(Cadence::Hourly), 20.0 + ROTATION_VOLUME_THRESHOLD + 1.0);
        assert!(should_rotate(&eth, &btc_busier, open + 1800), "a pinned ETH market still gives way to a clearly busier BTC one");
        let mut eth_busier = pair("KXETHD-HOUR-B", Some(Cadence::Hourly), 20.0 + ROTATION_VOLUME_THRESHOLD + 1.0);
        eth_busier.underlying = "eth";
        assert!(!should_rotate(&eth, &eth_busier, open + 1800), "but holds against its own ladder");
        let mut quarter = pair("KXBTC15M-A", Some(Cadence::FifteenMin), 50_000.0);
        quarter.close_time = Some(close);
        let any_hour = pair("KXBTCD-HOUR-C", Some(Cadence::Hourly), 1.0);
        assert!(should_rotate(&quarter, &any_hour, open + 3000), "a better cadence still wins; the pin is for strikes of the same hour");
        assert!(!should_rotate(&hour, &quarter, open + 10), "and a worse one never does, settled or not");
    }
}

#[cfg(test)]
mod venue_status_tests {
    use super::*;

    /// 2026-10-19 as production had it: KXNFLGAME-26OCT18DALGB-GB finalized at
    /// 03:47Z with result "yes" and a stated close two days on. The venue's own
    /// record is what says the market is over.
    #[test]
    fn a_finalized_game_with_a_result_is_no_longer_trading() {
        assert_eq!(venue_accepting_orders("finalized", "yes"), Some(false));
        assert_eq!(venue_accepting_orders("closed", ""), Some(false), "closed to trading but undetermined is still over");
        assert_eq!(venue_accepting_orders("settled", "no"), Some(false));
        assert_eq!(venue_accepting_orders("active", "yes"), Some(false), "a recorded result wins over a stale status");
    }

    /// Trading is trading, and a status this build does not know is no answer,
    /// so the stated close keeps its authority rather than retiring on a guess.
    #[test]
    fn an_active_market_is_trading_and_an_unknown_status_is_no_answer() {
        assert_eq!(venue_accepting_orders("active", ""), Some(true));
        assert_eq!(venue_accepting_orders("open", ""), Some(true));
        assert_eq!(venue_accepting_orders("initialized", ""), None);
        assert_eq!(venue_accepting_orders("unopened", ""), None);
        assert_eq!(venue_accepting_orders("", ""), None);
        assert_eq!(venue_accepting_orders("something-new", ""), None);
    }

    /// A halted live game is NOT over. Kalshi records a halt as `inactive`
    /// (`paused` in its query vocabulary); reading either as "closed" would
    /// retire a flat squadron five minutes into an injury hold or a review, and
    /// the seeder never redeploys onto a game that has left the board.
    #[test]
    fn a_halt_is_no_answer_not_closed() {
        assert_eq!(venue_accepting_orders("inactive", ""), None);
        assert_eq!(venue_accepting_orders("paused", ""), None);
        assert_eq!(venue_accepting_orders(" inactive ", ""), None, "trimmed like orders.rs");
    }

    /// The venue has to say "closed" for the whole grace, and a squadron that
    /// holds a position is never stood down on top of it.
    #[test]
    fn the_venue_must_say_closed_for_the_whole_grace_and_never_over_a_position() {
        assert!(!venue_closed_retire_due(None, 300, false));
        assert!(!venue_closed_retire_due(Some(299), 300, false));
        assert!(venue_closed_retire_due(Some(300), 300, false));
        assert!(!venue_closed_retire_due(Some(86_400), 300, true));
    }

    /// No book until the feed's first snapshot; a ready book with no levels is
    /// still no book; the top bid is read across both legs.
    #[tokio::test]
    async fn book_state_reads_absent_until_ready_and_the_top_bid_across_legs() {
        let hub = ws::new_book_hub();
        assert_eq!(book_state(&hub, "T").await, (false, Decimal::ZERO));
        {
            let mut books = hub.write().await;
            let mut b = ws::Book::default();
            b.ready = true;
            books.insert("T".into(), b);
        }
        assert_eq!(book_state(&hub, "T").await, (false, Decimal::ZERO), "ready with no levels is still no book");
        {
            let mut books = hub.write().await;
            let b = books.get_mut("T").unwrap();
            b.yes_bids.insert(dec!(0.02), dec!(10));
            b.no_bids.insert(dec!(0.97), dec!(10));
        }
        assert_eq!(book_state(&hub, "T").await, (true, dec!(0.97)));
    }

    /// Only a stated yes or no books a simulated settlement; everything else
    /// defers, the same way the live sweep reads the venue.
    #[test]
    fn only_a_binary_result_prices_a_simulated_settlement() {
        assert_eq!(kalshi_result_yes_price("yes"), Some(Decimal::ONE));
        assert_eq!(kalshi_result_yes_price("no"), Some(Decimal::ZERO));
        assert_eq!(kalshi_result_yes_price(""), None, "undetermined is not a result");
        assert_eq!(kalshi_result_yes_price("void"), None);
        assert_eq!(kalshi_result_yes_price("scalar"), None);
    }

    /// A pinned market's tickers leave the book-feed registry when its loop
    /// ends, so a finished game is not reported dark for the life of the
    /// process; its viper rows go with it so the health strip does not count
    /// a reaped squadron as stale.
    #[test]
    fn a_finished_pinned_market_leaves_both_registries() {
        use crate::helpers::viper_status;
        use crate::state::price_state::book_feed;
        let ticker = "KXTEST-REGISTRIES-GAME";
        let scope = "test-registries-sports";
        book_feed::note(ticker, false);
        viper_status::record_idle(scope, "MakerStrategy", "waiting");
        assert!(book_feed::is_tracked(ticker));
        assert!(!viper_status::snapshot(Some(scope)).is_empty());

        let (tx, rx) = watch::channel::<HashMap<String, String>>(HashMap::new());
        let tx = Arc::new(tx);
        tx.send_modify(|m| {
            m.insert(crate::state::strategy_market_key("test-registries-sports", "arbitrage"), "the game".into());
            m.insert(crate::state::strategy_market_key("test-registries-btc", "anchor"), "the hourly".into());
        });

        drop(MarketRegistries::new(&[ticker.to_string()], MarketMandate::Pinned, scope, Some((&tx, "test-registries-sports"))));

        let left = rx.borrow().clone();
        assert!(!left.contains_key("test-registries-sports:arbitrage"), "the squadron's status entries should be gone: {left:?}");
        assert!(left.contains_key("test-registries-btc:anchor"), "another squadron's entry stays");

        assert!(!book_feed::is_tracked(ticker), "the ticker should be forgotten");
        assert!(viper_status::snapshot(Some(scope)).is_empty(), "the scope's viper rows should be gone");
    }

    /// A rotating crypto market forgets its ticker but keeps the asset's viper
    /// rows (the next hourly market reuses them), and a Helm squadron keeps
    /// the shared Helm scope because other Helm squadrons may still be flying.
    #[test]
    fn rotation_and_helm_keep_their_shared_viper_rows() {
        use crate::helpers::viper_status;
        use crate::state::price_state::book_feed;
        let ticker = "KXTEST-REGISTRIES-HOURLY";
        book_feed::note(ticker, false);
        viper_status::record_idle("test-registries-btc", "MakerStrategy", "waiting");
        let (tx, rx) = watch::channel::<HashMap<String, String>>(HashMap::new());
        let tx = Arc::new(tx);
        tx.send_modify(|m| { m.insert(crate::state::strategy_market_key("test-registries-btc", "anchor"), "the hourly".into()); });
        drop(MarketRegistries::new(&[ticker.to_string()], MarketMandate::Rotating, "test-registries-btc", Some((&tx, "test-registries-btc"))));
        assert!(rx.borrow().contains_key("test-registries-btc:anchor"), "a rotating squadron keeps its status entries for the next market");
        assert!(!book_feed::is_tracked(ticker));
        assert!(!viper_status::snapshot(Some("test-registries-btc")).is_empty());
        viper_status::forget("test-registries-btc");

        let helm = crate::vipers::helm_impl::KIND;
        let guard = MarketRegistries::new(&[], MarketMandate::Pinned, helm, None);
        assert!(guard.markets.is_none(), "the Helm scope keeps its shared status entries too");
        assert!(guard.status_scope.is_none(), "the Helm scope is shared and never forgotten here");
    }
}
