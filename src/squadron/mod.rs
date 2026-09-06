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

/// Squadron — the core deployable unit of DRADIS.
///
/// A Squadron bundles one or more Raptors (signal scouts) with one or more
/// Vipers (trading strategies) and sends them to a specific Polymarket market
/// (the battle location).
///
/// ┌──────────────────────────────────────────────────────────────┐
/// │                        Squadron                              │
/// │                                                              │
/// │  Asset            ──►  CryptoAsset (BTC / ETH / SOL / …)   │
/// │  Battle Location  ──►  MarketConfig (yes/no tokens, expiry) │
/// │  Raptors          ──►  SquadronRaptors (watch signal feeds)  │
/// │  Vipers           ──►  Vec<Box<dyn Strategy>>                │
/// │  State            ──►  SquadronState lifecycle FSM           │
/// │  ws_cancel        ──►  CancellationToken for WS tasks        │
/// └──────────────────────────────────────────────────────────────┘
///
/// Phase 2:   types defined, wired into the CIC (main.rs).
/// Phase 3f-2: Squadron owns WS subscriptions.
///            `subscribe_markets()` spawns the 4 orderbook tasks and returns
///            `MarketPriceFeeds`.  `cancel_ws()` cleans them up on rotation.
/// Phase 3f-3 (current): `patrol()` drives the full inner tick loop.
///            main.rs creates a `PatrolContext` and calls
///            `squadron.patrol(cancel, &mut ctx).await` instead of running
///            the select! loop directly.

pub mod raptors;
pub mod config;
/// `PatrolContext` — all infrastructure `patrol()` needs.  Re-exported at
/// crate level so `main.rs` can construct it without reaching into sub-modules.
#[cfg(feature = "intl_clob")]
pub mod context;
#[cfg(feature = "intl_clob")]
pub use context::PatrolContext;

/// Inner tick-loop implementation for `Squadron::patrol()`.
/// Kept in a separate file to avoid bloating mod.rs.
#[cfg(feature = "intl_clob")]
mod patrol_impl;
/// Bulk order cancellation, shared by the three paths that end a squadron's
/// tenure on a market: hourly rotation, operator stand-down, and process
/// shutdown. Exported so `main` can register it as the shutdown hook — the
/// `Execution` trait has no cancel-all, so this is the venue SDK call.
///
/// intl-only, like the module it comes from: the Kalshi and Polymarket US
/// traders own their own order lifecycles and do not route through `patrol()`.
#[cfg(feature = "intl_clob")]
pub use patrol_impl::{cancel_all_orders_unless_simulating, cancel_all_orders_with_retries};

/// Local order book built from the venue's `book` snapshots and `price_change`
/// updates — B36. Venue-neutral core; `spawn_ws_task` feeds it.
#[cfg(feature = "intl_clob")]
mod local_book;
#[cfg(feature = "intl_clob")]
pub use local_book::{ApplyOutcome, BookSide, BookStats, Hold, LevelChange, LocalBook};

/// Peripheral tasks spawned by `patrol()` — Phase 3f-4.
/// Kept in a separate file for clarity; each function spawns one Tokio task.
#[cfg(feature = "intl_clob")]
mod patrol_tasks;
#[cfg(feature = "intl_clob")]
pub use patrol_tasks::{
    spawn_pulse_task, spawn_settlement_task, spawn_cleanup_task,
    spawn_status_task, spawn_watchdog_task,
};

pub use raptors::SquadronRaptors;
pub use config::{SquadronConfig, RaptorProfile, ViperProfile};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
#[cfg(feature = "intl_clob")]
use alloy::primitives::U256;
#[cfg(feature = "intl_clob")]
use futures::StreamExt as _;
#[cfg(feature = "intl_clob")]
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "intl_clob")]
use tracing::{info, warn};
#[cfg(not(feature = "intl_clob"))]
use tracing::info;

#[cfg(feature = "intl_clob")]
use polymarket_client_sdk_v2::clob::ws::{
    interest::InterestTracker, subscription::SubscriptionManager, PriceChangeBatchEntry, WsMessage,
};
#[cfg(feature = "intl_clob")]
use polymarket_client_sdk_v2::clob::types::Side;
#[cfg(feature = "intl_clob")]
use polymarket_client_sdk_v2::ws::{config::Config as WsConfig, ConnectionManager};
#[cfg(feature = "intl_clob")]
use std::sync::Arc;

/// Polymarket International market-data channel. The SDK's `Client` appends
/// `/ws/market` to its base endpoint; this is the assembled form because
/// `spawn_ws_task` builds the channel itself (see `open_market_stream`).
#[cfg(feature = "intl_clob")]
const INTL_MARKET_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

use crate::state::{MarketConfig, PriceState};

// ─── CryptoAsset ─────────────────────────────────────────────────────────────

/// The underlying crypto asset a squadron is watching.
///
/// Carried on every `Squadron` so the CAG (Phase 3) can:
///   • route price/funding Raptors to the right Binance WS symbol
///   • namespace DB paths, log files, and model artefacts per-asset
///   • expose per-asset squadron status in the Control Tower UI
///
/// The `Custom` variant lets future assets be added without a code change —
/// useful for a user deploying their own squadron config via the UI.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CryptoAsset {
    Btc,
    Eth,
    Sol,
    /// Any asset not yet enumerated.  `symbol` should be upper-case, e.g. "MATIC".
    Custom(String),
}

impl CryptoAsset {
    /// Upper-case trading symbol as used by Binance WS streams.
    /// e.g. `CryptoAsset::Btc.symbol()` → `"BTC"`
    pub fn symbol(&self) -> String {
        match self {
            Self::Btc          => "BTC".to_string(),
            Self::Eth          => "ETH".to_string(),
            Self::Sol          => "SOL".to_string(),
            Self::Custom(sym)  => sym.to_uppercase(),
        }
    }

    /// Lower-case slug used for file-system namespacing (`logs/btc/dradis.db`).
    pub fn slug(&self) -> String {
        self.symbol().to_lowercase()
    }
}

impl std::fmt::Display for CryptoAsset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.symbol())
    }
}

impl std::str::FromStr for CryptoAsset {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "btc" => Self::Btc,
            "eth" => Self::Eth,
            "sol" => Self::Sol,
            other => Self::Custom(other.to_uppercase()),
        })
    }
}

/// Unique identifier for a deployed squadron.
/// Format: "<asset>-<venue>-<market_close_time_iso>"
/// Example: "btc-hourly-2026-05-23T14:00:00Z"
pub type SquadronId = String;

/// Lifecycle state of a squadron.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SquadronState {
    /// Assembled and configured — waiting for a battle location assignment.
    Staged,

    /// Market acquired, WS orderbook subscriptions live, pre-flight checks running.
    Deployed,

    /// Active trading tick loop running — Raptors feeding, Vipers flying.
    Patrolling,

    /// Returning to base — market expiring or manual stand-down.
    /// No new entries; existing positions being wound down.
    Rtb,

    /// Market expired and all positions closed (or forcibly stood down by CAG).
    StoodDown,
}

impl std::fmt::Display for SquadronState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Staged      => write!(f, "STAGED"),
            Self::Deployed    => write!(f, "DEPLOYED"),
            Self::Patrolling  => write!(f, "PATROLLING"),
            Self::Rtb         => write!(f, "RTB"),
            Self::StoodDown   => write!(f, "STOOD_DOWN"),
        }
    }
}

// ─── MarketPriceFeeds ─────────────────────────────────────────────────────────

/// Live orderbook price receivers returned by `Squadron::subscribe_markets()`.
///
/// Holds one `watch::Receiver<PriceState>` per active token.  The squadron
/// keeps the corresponding senders internally; this struct is handed to the
/// caller (currently `main.rs`'s market loop, eventually `patrol()`) so it
/// can snapshot prices on every tick without any lock contention.
///
/// Maker receivers are `Option` because a maker venue may not be available on
/// every market rotation.
pub struct MarketPriceFeeds {
    /// Live bid/ask for the hourly market YES token.
    pub hourly_yes: watch::Receiver<PriceState>,
    /// Live bid/ask for the hourly market NO token.
    pub hourly_no:  watch::Receiver<PriceState>,
    /// Live bid/ask for the window/daily maker YES token (if present).
    pub maker_yes:  Option<watch::Receiver<PriceState>>,
    /// Live bid/ask for the window/daily maker NO token (if present).
    pub maker_no:   Option<watch::Receiver<PriceState>>,
}

// ─── SquadronId / SquadronState ───────────────────────────────────────────────

// ...existing code...

// ─── Squadron ────────────────────────────────────────────────────────────────

/// A fully-described squadron deployment.
///
/// In Phase 2 this is a descriptor/record type — owned by the CIC's market
/// loop at runtime.  In Phase 3f-3 it will grow a `patrol()` async method that
/// runs the tick loop so the CAG can spawn multiple concurrent squadrons.
pub struct Squadron {
    pub id:      SquadronId,
    /// The underlying crypto asset this squadron is watching.
    pub asset:   CryptoAsset,
    pub config:  SquadronConfig,
    pub market:  MarketConfig,
    pub raptors: SquadronRaptors,
    pub state:   SquadronState,
    pub deployed_at: DateTime<Utc>,

    /// Market class as the VENUE itself reports it, when it says.
    ///
    /// Classification otherwise derives its category from `asset`, which is a
    /// storage identity rather than a domain: Polymarket US wings are "us" and
    /// "us-crypto", neither of which matches a category rule. Its sports
    /// markets then fell through to the symbol-token rules, which cover
    /// nfl/nba/mlb and friends but not `atc-lal-…` for La Liga — so a live
    /// football market classified as `unknown`, lost the Sports Raptor, and
    /// displayed as "US Retail Squadron" instead of "US Sports Squadron".
    ///
    /// The venue's own `category` field says "sports" outright. Prefer it.
    pub venue_category: Option<String>,

    /// True when this squadron was deployed onto exactly ONE market, with no
    /// hourly/window-daily split and no rotation behind it.
    ///
    /// That is the shape every event-market deploy has: `adama` hands the patrol
    /// loop a dummy `market_rx` nothing ever sends on, and no maker venue. Two
    /// behaviors hang off it, and both were wrong while it did not exist:
    ///
    /// 1. Arbitrage refuses to run without a maker venue, because on a split
    ///    venue falling back to the hourly leg turns a half-filled pair into a
    ///    naked directional bet. With no split there is nothing to fall back
    ///    *from* — the single market is the venue — so the hourly pair is fed
    ///    through as the maker pair, exactly as the US wing does
    ///    (`venues/us/trader.rs`, 2026-08-08).
    /// 2. Nothing rotates the market, so the squadron must retire itself when
    ///    its market closes or it holds the class slot forever. Observed on the
    ///    v1.0.4 Marketplace AMI: a sports squadron sat on a resolved League of
    ///    Legends market for hours while `seed_auto_deployments` skipped the
    ///    class because a squadron for it was still live.
    ///
    /// Set post-construction rather than threaded through all three
    /// constructors, matching how `SquadronRaptors::tennis` is attached.
    pub single_market: bool,

    /// Cancellation token used to stop WS reconnect loops on market rotation.
    ///
    /// A fresh token is created on each `subscribe_markets()` call; the
    /// previous generation of tasks drains when they observe cancellation.
    /// `cancel_ws()` fires it; `patrol()` fires it on stand-down.
    ws_cancel: CancellationToken,
}

/// Reduce an operator-chosen name to something safe to embed in an id.
///
/// The id ends up in log lines, API paths and a database key, so it is kept to
/// lowercase alphanumerics and single dashes rather than trusting free text.
fn slugify(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut last_dash = true; // leading dashes are dropped
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    while out.ends_with('-') { out.pop(); }
    out.truncate(32);
    while out.ends_with('-') { out.pop(); }
    out
}

impl Squadron {
    /// Construct a new squadron descriptor at deployment time.
    pub fn new(
        asset:   CryptoAsset,
        config:  SquadronConfig,
        market:  MarketConfig,
        raptors: SquadronRaptors,
    ) -> Self {
        Self::new_with_category(asset, config, market, raptors, None)
    }

    /// Like [`Self::new`] but carrying the venue's own market category.
    pub fn new_with_category(
        asset:   CryptoAsset,
        config:  SquadronConfig,
        market:  MarketConfig,
        raptors: SquadronRaptors,
        venue_category: Option<String>,
    ) -> Self {
        Self::new_named(asset, config, market, raptors, venue_category, None)
    }

    /// Like [`Self::new_with_category`] but with an operator-chosen name that
    /// distinguishes this squadron from others of the same class.
    ///
    /// The name is folded into the id rather than replacing the asset, so
    /// classification, DB-pool aliasing and taxonomy lookups keep seeing the
    /// class they always did. What changes is the identity: the id is the
    /// persistence key for operator config and part of every `PositionKey`, so
    /// naming a squadron is what gives it its own config, budgets and positions.
    pub fn new_named(
        asset:   CryptoAsset,
        config:  SquadronConfig,
        market:  MarketConfig,
        raptors: SquadronRaptors,
        venue_category: Option<String>,
        name: Option<&str>,
    ) -> Self {
        let deployed_at = Utc::now();
        // Stable identity: `{asset}-{cadence}` — deliberately WITHOUT a timestamp.
        //
        // The squadron id is the persistence key for its operator config
        // (`squadron_configs` row, read/written by the Control Tower). Embedding
        // `deployed_at` here made the id change on every restart and every hourly
        // market rotation, orphaning the saved config and silently reverting
        // operator edits (e.g. a disabled viper re-enabling itself on restart).
        // `deployed_at` is retained as its own field for sorting/display, so we
        // lose no information by keeping the id stable across deployments.
        // Constant, not derived from the market.
        //
        // This used to read `if market.market_close_time.is_some() { "hourly" }
        // else { "open" }`, which made the squadron's IDENTITY depend on a
        // property of whichever market it happened to pick up. The intl rotation
        // loop passed a real close time and so produced `btc-hourly` on one
        // market and `btc-open` on the next — the same logical squadron under two
        // ids, and since the id is the persistence key for operator config and
        // part of every PositionKey, two config rows. On 2026-08-24 the advisor
        // proposed against the one that was not running and four operator
        // approvals were written to a config nothing read.
        //
        // Nothing anywhere reads `-hourly`, so every live id already ends in
        // `-open`. Keeping the suffix rather than dropping it holds existing ids
        // byte-identical, so no operator's saved config is orphaned; making it a
        // constant means a caller cannot reintroduce the flip by passing a close
        // time — which callers now do again, because the vipers read their close
        // time from this same MarketConfig and blanking it blinded them.
        let cadence = "open";
        // A named squadron takes `{asset}-{cadence}-{name}`, so a second one of
        // the same class no longer collides with the first. Unnamed squadrons
        // keep exactly the id they had, which matters: that id is the key their
        // persisted config is already stored under.
        let id = match name.map(slugify).filter(|s| !s.is_empty()) {
            Some(slug) => format!("{}-{}-{}", asset.slug(), cadence, slug),
            None => format!("{}-{}", asset.slug(), cadence),
        };
        Self {
            id,
            asset,
            config,
            market,
            raptors,
            state: SquadronState::Deployed,
            deployed_at,
            venue_category,
            // Split-venue by default: only the event-market deploy path sets it.
            single_market: false,
            ws_cancel: CancellationToken::new(),
        }
    }

    /// Transition to Patrolling once the tick loop starts.
    pub fn start_patrol(&mut self) {
        self.state = SquadronState::Patrolling;
    }

    /// Classify this squadron's market into a `market_class` and link it to the
    /// raptors/vipers that are meaningful for it, persisting the resolved class
    /// onto the squadron's `squadron_configs` row.
    ///
    /// This is **venue-neutral core**: the category hint is derived from the
    /// squadron's own asset (crypto assets self-identify as `crypto`; any other
    /// asset falls back to the symbol-token / slug rules), so both the intl and
    /// US registration paths get the same data-driven linkage. As future
    /// `sports`/`politics` raptors are built and their `raptor_kind.implemented`
    /// flag flipped, the matching squadrons light them up with no code change.
    ///
    /// Returns the resolved class. No-op-safe (`"unknown"`) if the DB pool is
    /// not yet initialized. Call after the squadron's config row is seeded.
    pub async fn classify_and_link(&self) -> String {
        let Some(pool) = crate::helpers::db::pool() else {
            return "unknown".to_string();
        };
        // Crypto assets self-identify. A Custom asset offers its own name as the
        // category: an operator who deploys a market as "politics" has declared
        // the class, and the category rule matches it exactly at the highest
        // priority. Names that match no category rule — "us", "us-crypto" —
        // simply fall through to the symbol-token and slug rules, because the
        // category test requires an exact match rather than merely a non-empty
        // string. So this strengthens a declared class without altering any
        // venue that does not declare one.
        // The venue's own category wins when it gives one — it is a domain
        // statement, where `asset` is a storage identity that happens to match
        // a rule for crypto and nothing else.
        let category = match self.venue_category.as_deref().filter(|c| !c.is_empty()) {
            Some(c) => c,
            None => match &self.asset {
                CryptoAsset::Btc | CryptoAsset::Eth | CryptoAsset::Sol => "crypto",
                CryptoAsset::Custom(name) => name.as_str(),
            },
        };
        let symbols = [self.market.yes_token.as_str(), self.market.no_token.as_str()];
        let class = crate::helpers::db::classify_market(
            pool, category, &symbols, &self.market.market_name,
        ).await;
        let raptors = crate::helpers::db::raptors_for_class(pool, &class).await;
        let vipers = crate::helpers::db::vipers_for_class(pool, &class).await;
        crate::helpers::db::set_squadron_market_class(pool, &self.id, &class).await;
        info!(
            "🧬 Squadron [{}] classified as '{class}' → raptors={raptors:?}, vipers={vipers:?}",
            self.id
        );
        class
    }

    /// Signal RTB — no new entries, existing positions winding down.
    pub fn rtb(&mut self) {
        self.state = SquadronState::Rtb;
    }

    /// Mark squadron stood-down (market expired or manual override).
    pub fn stand_down(&mut self) {
        self.state = SquadronState::StoodDown;
        // A simulated resting quote must not outlive the squadron that placed it.
        // The registry is process-global and keyed by squadron, so without this a
        // stood-down squadron's quote would sit there until the process ended and
        // could be crossed by a later squadron trading the same market.
        crate::helpers::ghost_quotes::clear_squadron(&self.id.to_string());
    }

    /// Returns true when the squadron should cease all trading activity.
    pub fn is_done(&self) -> bool {
        matches!(self.state, SquadronState::Rtb | SquadronState::StoodDown)
    }

    // ─── WS subscription ─────────────────────────────────────────────────────

    /// Subscribe to Polymarket WebSocket orderbook feeds for this squadron's
    /// battle location.
    ///
    /// Spawns one independent Tokio task per token (up to 4 total).  Each task
    /// maintains an auto-reconnecting WS stream and pushes
    /// `(bid, bid_depth, ask, ask_depth, timestamp)` updates into a
    /// `watch::Sender`.  Tasks stop when the WS cancel token fires.
    ///
    /// **Calling this a second time** (e.g. on market rotation) automatically
    /// cancels the previous generation of tasks before spawning new ones —
    /// no task leak, no stale price data.
    ///
    /// Returns `MarketPriceFeeds` — the caller holds these receivers for the
    /// duration of the patrol to drive strategy snapshots.
    ///
    /// Phase 3f-2: called by `main.rs` to replace the two inline WS blocks.
    /// Phase 3f-3: called internally by `patrol()`.
    #[cfg(feature = "intl_clob")]
    pub fn subscribe_markets(
        &mut self,
        hourly_yes_token: U256,
        hourly_no_token:  U256,
        maker:            Option<(U256, U256)>,  // (yes_token, no_token)
    ) -> MarketPriceFeeds {
        // Cancel any WS tasks from a previous call (e.g. prior market rotation).
        self.ws_cancel.cancel();

        // Fresh token for this generation of WS tasks.
        let cancel = CancellationToken::new();
        self.ws_cancel = cancel.clone();

        let default_feed: PriceState = (dec!(0), dec!(0), dec!(1), dec!(0), Utc::now(), dec!(0), dec!(0));

        // ── Hourly market feeds ───────────────────────────────────────────────
        let (yes_tx, yes_rx) = watch::channel(default_feed);
        let (no_tx,  no_rx)  = watch::channel(default_feed);

        if hourly_yes_token != U256::ZERO {
            spawn_ws_task(hourly_yes_token, yes_tx, cancel.clone(), "hourly");
            spawn_ws_task(hourly_no_token,  no_tx,  cancel.clone(), "hourly");
        }

        // ── Maker/window market feeds (optional) ─────────────────────────────
        let (maker_yes, maker_no) = if let Some((mk_yes, mk_no)) = maker {
            let (mk_yes_tx, mk_yes_rx) = watch::channel(default_feed);
            let (mk_no_tx,  mk_no_rx)  = watch::channel(default_feed);
            spawn_ws_task(mk_yes, mk_yes_tx, cancel.clone(), "maker");
            spawn_ws_task(mk_no,  mk_no_tx,  cancel.clone(), "maker");
            (Some(mk_yes_rx), Some(mk_no_rx))
        } else {
            (None, None)
        };

        info!(
            squadron = %self.id,
            hourly_has_market = (hourly_yes_token != U256::ZERO),
            has_maker = maker.is_some(),
            "📡  Squadron: WS subscriptions spawned",
        );

        MarketPriceFeeds {
            hourly_yes: yes_rx,
            hourly_no:  no_rx,
            maker_yes,
            maker_no,
        }
    }

    /// Signal all WS reconnect tasks for this squadron to stop.
    ///
    /// Called on market rotation (before the old squadron is stood down) to
    /// prevent task accumulation: without this, each rotation leaks 4 tasks
    /// that loop-reconnect forever, gradually exhausting heap.
    ///
    /// Safe to call multiple times — a cancelled token is a no-op on
    /// subsequent cancellations.
    pub fn cancel_ws(&self) {
        self.ws_cancel.cancel();
        info!(squadron = %self.id, "📡  Squadron: WS cancel signal sent");
    }

}
// patrol() is implemented in patrol_impl.rs (Phase 3f-3)

// ─── Private helpers ─────────────────────────────────────────────────────────

/// Open the market channel for one token and return the unfiltered, ordered
/// `WsMessage` stream.
///
/// Assembled the way the SDK's `Client` assembles it — one `ConnectionManager`
/// (heartbeats, backoff reconnect) and one `SubscriptionManager` (re-sends the
/// subscription after a reconnect, which is what makes the venue send a fresh
/// `book`) — but without `Client::subscribe_orderbook`'s filter, which drops
/// every message that is not a `book`. Consuming `price_change` is the point
/// of B36, and it has to come from the *same* stream as the snapshots: two
/// filtered streams over the same broadcast could hand back a `price_change`
/// before the `book` it belongs after, and the book would then be rebuilt
/// without it.
///
/// The `SubscriptionManager` is returned alongside the stream because the
/// connection lives only as long as a `ConnectionManager` clone does; the
/// caller keeps both for the life of the subscription.
#[cfg(feature = "intl_clob")]
fn open_market_stream(
    token: U256,
) -> polymarket_client_sdk_v2::Result<(
    Arc<SubscriptionManager>,
    impl futures::Stream<Item = polymarket_client_sdk_v2::Result<WsMessage>>,
)> {
    let interest = Arc::new(InterestTracker::new());
    let connection = ConnectionManager::new(
        INTL_MARKET_WS_ENDPOINT.to_string(), WsConfig::default(), Arc::clone(&interest),
    )?;
    let subscriptions = Arc::new(SubscriptionManager::new(connection, interest));
    subscriptions.start_reconnection_handler();
    let stream = subscriptions.subscribe_market(vec![token])?;
    Ok((subscriptions, stream))
}

/// One `price_change` entry as the local book understands it. `None` when the
/// SDK could not classify the side — the caller distrusts the book rather than
/// guess which side a level belongs to.
#[cfg(feature = "intl_clob")]
fn level_change(e: &PriceChangeBatchEntry) -> Option<LevelChange> {
    let side = match e.side {
        Side::Buy  => BookSide::Bid,
        Side::Sell => BookSide::Ask,
        _          => return None,
    };
    Some(LevelChange {
        side, price: e.price, size: e.size, best_bid: e.best_bid, best_ask: e.best_ask,
    })
}

#[cfg(feature = "intl_clob")]
fn publish_book(tx: &watch::Sender<PriceState>, book: &LocalBook) {
    if let Some(state) = book.price_state() {
        let _ = tx.send(state);
    }
}

/// A hold on the derived book that outlives this is worth a line in the log
/// at INFO; shorter ones are the venue publishing a trade or a batched
/// cancel across several frames (see `local_book`) and are routine, so they
/// are logged at DEBUG and counted in the per-snapshot stats instead.
#[cfg(feature = "intl_clob")]
const LONG_BOOK_HOLD_MS: i64 = 1_000;

/// Log the end of a hold: how long the derived book was withheld, why, and
/// what lifted it.
#[cfg(feature = "intl_clob")]
fn log_hold_lifted(venue: &str, token: U256, hold: &Hold, lifted_by: &str, now: chrono::DateTime<Utc>) {
    let held_ms = hold.held_for(now).num_milliseconds();
    if hold.hard || held_ms >= LONG_BOOK_HOLD_MS {
        info!(
            "📖 Book feed for {} token {}: derived book held for {}ms for `{}` ({} price_changes folded in meanwhile), lifted by {}",
            venue, token, held_ms, hold.reason, hold.batches, lifted_by
        );
    } else {
        tracing::debug!(
            "📖 Book feed for {} token {}: derived book held for {}ms for `{}`, lifted by {}",
            venue, token, held_ms, hold.reason, lifted_by
        );
    }
}

/// Spawn one auto-reconnecting WebSocket orderbook subscriber task.
///
/// Pushes `PriceState` updates into `tx`.  Stops cleanly when `cancel` fires.
/// The `venue` label is used only for log messages.
///
/// B36: the task keeps a [`LocalBook`]. Every `book` message resets it; every
/// `price_change` for this token is folded in between snapshots while the
/// `book_apply_price_changes` knob is on and the book can prove itself
/// against the venue's own best bid/ask. While it cannot — a trade or a
/// batched cancel still being published, see `local_book` — it publishes the
/// last snapshot, the feed as it was before B36, until the stamps agree again
/// or the next `book` arrives.
#[cfg(feature = "intl_clob")]
fn spawn_ws_task(
    token:  U256,
    tx:     watch::Sender<PriceState>,
    cancel: CancellationToken,
    venue:  &'static str,
) {
    tokio::spawn(async move {
        loop {
            if cancel.is_cancelled() { return; }

            let (_subscriptions, stream) = match open_market_stream(token) {
                Ok(s)  => s,
                Err(e) => {
                    warn!(
                        "⚠️ WS subscribe failed for {} token {}: {}. Retrying in 5s…",
                        venue, token, e
                    );
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                    }
                    continue;
                }
            };

            let mut stream = Box::pin(stream);
            info!("✅ WS orderbook subscribed for {} token {}", venue, token);

            // Fresh per connection: a reconnect always brings a new snapshot.
            let mut book = LocalBook::new();
            let mut announced = false;

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => { return; }
                    result = stream.next() => {
                        match result {
                            Some(Ok(WsMessage::Book(snapshot))) => {
                                let stats = book.take_stats();
                                // Stamp the WS update time at receipt, NOT at tick time.
                                let now = Utc::now();
                                let cleared = book.reset(
                                    snapshot.timestamp,
                                    snapshot.bids.iter().map(|l| (l.price, l.size)),
                                    snapshot.asks.iter().map(|l| (l.price, l.size)),
                                    now,
                                );
                                if let Some(hold) = cleared {
                                    log_hold_lifted(venue, token, &hold, "the next book snapshot", now);
                                }
                                if stats.applied > 0 || stats.dropped_older > 0 || stats.held > 0 {
                                    tracing::debug!(
                                        "📖 Book snapshot for {} token {}: reset after {} price_changes applied, {} dropped as older than the previous snapshot, {} holds ({} released by the venue's stamps agreeing)",
                                        venue, token, stats.applied, stats.dropped_older, stats.held, stats.released
                                    );
                                }
                                publish_book(&tx, &book);
                            }
                            Some(Ok(WsMessage::PriceChange(pc))) => {
                                // Kill switch. Off is the pre-B36 feed exactly: snapshots only.
                                if !crate::helpers::dynamic_config::book_price_changes_enabled() {
                                    continue;
                                }
                                let mine: Vec<&PriceChangeBatchEntry> = pc.price_changes.iter()
                                    .filter(|e| e.asset_id == token)
                                    .collect();
                                if mine.is_empty() { continue; }
                                let now = Utc::now();
                                let outcome = match mine.iter().map(|e| level_change(e)).collect::<Option<Vec<_>>>() {
                                    Some(changes) => book.apply(pc.timestamp, &changes, now),
                                    None => book.distrust(format!(
                                        "price_change carried a side the SDK could not classify: {:?}",
                                        mine.iter().map(|e| e.side).collect::<Vec<_>>()
                                    ), now),
                                };
                                match outcome {
                                    ApplyOutcome::Applied => {
                                        if !announced {
                                            info!(
                                                "📖 Book feed for {} token {}: folding price_change updates into the book between snapshots (B36; knob book_apply_price_changes)",
                                                venue, token
                                            );
                                            announced = true;
                                        }
                                        publish_book(&tx, &book);
                                    }
                                    ApplyOutcome::Released(hold) => {
                                        log_hold_lifted(venue, token, &hold, "the venue's stamps agreeing", now);
                                        publish_book(&tx, &book);
                                    }
                                    ApplyOutcome::Held(hold) => {
                                        // Transition only: the snapshot is republished once,
                                        // with its own receipt time, and stays until the
                                        // stamps agree or the next `book`. Consumers see
                                        // exactly the pre-B36 feed meanwhile.
                                        if hold.hard {
                                            warn!(
                                                "⚠️ Book feed for {} token {}: {} — publishing the last full snapshot until the venue sends the next book",
                                                venue, token, hold.reason
                                            );
                                        } else {
                                            tracing::debug!(
                                                "📖 Book feed for {} token {}: derived book held, {} — publishing the last full snapshot until the venue's stamps agree",
                                                venue, token, hold.reason
                                            );
                                        }
                                        publish_book(&tx, &book);
                                    }
                                    ApplyOutcome::StillHeld
                                    | ApplyOutcome::DroppedOlder
                                    | ApplyOutcome::NoSnapshot => {}
                                }
                            }
                            // `last_trade_price`, `tick_size_change`, and anything the
                            // venue adds later: no book content, nothing to publish.
                            Some(Ok(_)) => {}
                            Some(Err(_)) | None => {
                                warn!(
                                    "⚠️ WS stream error for {} token {}. Restarting…",
                                    venue, token
                                );
                                break;
                            }
                        }
                    }
                }
            }

            // Brief pause before reconnecting; respect cancel during the wait.
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
            }
        }
    });
}

#[cfg(test)]
mod squadron_identity_tests {
    use super::{CryptoAsset, Squadron, SquadronConfig, SquadronRaptors};
    use crate::state::MarketConfig;
    use crate::venues::core::MarketId;
    use chrono::Utc;

    fn market(close: Option<chrono::DateTime<Utc>>) -> MarketConfig {
        MarketConfig {
            yes_token: MarketId::new("y"), no_token: MarketId::new("n"),
            market_name: "Test market".to_string(),
            market_close_time: close,
            strike_price: None, is_neg_risk: false,
            condition_id: String::new(), yes_fee_bps: 0, no_fee_bps: 0,
        }
    }

    fn build(asset: CryptoAsset, close: Option<chrono::DateTime<Utc>>, name: Option<&str>) -> String {
        Squadron::new_named(
            asset, SquadronConfig::arb_wing("t".to_string()), market(close),
            SquadronRaptors::empty(), None, name,
        ).id
    }

    /// A close time must not change the squadron's identity.
    ///
    /// The cadence in `{asset}-{cadence}` was decided by whether the market
    /// carried a close time, so the intl rotation loop produced `btc-hourly` on
    /// one market and `btc-open` on the next. Squadron id is the persistence key
    /// for operator config and part of every PositionKey, so the same logical
    /// squadron accumulated two config rows — and on 2026-08-24 the advisor
    /// proposed against the one that was not running, with four operator
    /// approvals written to a config nothing read.
    #[test]
    fn identity_does_not_depend_on_the_market_close_time() {
        let with_close = build(CryptoAsset::Btc, Some(Utc::now()), None);
        let without    = build(CryptoAsset::Btc, None, None);
        assert_eq!(
            with_close, without,
            "a close time changed the squadron id: {with_close} vs {without}",
        );
    }

    /// A named squadron takes its own identity, so a second squadron of a class
    /// is distinguishable from the first.
    #[test]
    fn a_name_produces_a_distinct_identity() {
        let unnamed = build(CryptoAsset::Btc, None, None);
        let named   = build(CryptoAsset::Btc, None, Some("Scottie Scalper"));
        assert_ne!(unnamed, named);
        assert!(named.starts_with(&unnamed), "a named id should extend the unnamed one: {named}");
        assert!(named.ends_with("scottie-scalper"), "{named}");
    }

    /// Custom assets — the classes a deployed squadron uses — slug the same way
    /// whatever case the venue supplied, so `POLITICS` and `politics` cannot
    /// produce two identities for one class.
    #[test]
    fn custom_asset_identity_is_case_insensitive() {
        let upper = build(CryptoAsset::Custom("POLITICS".to_string()), None, None);
        let lower = build(CryptoAsset::Custom("politics".to_string()), None, None);
        assert_eq!(upper, lower, "{upper} vs {lower}");
    }
}

#[cfg(test)]
mod squadron_naming_tests {
    use super::slugify;

    /// An unnamed squadron must keep the id it already had. That id is the key
    /// its persisted operator config is stored under, so changing its shape
    /// would orphan every existing squadron's tuning on upgrade.
    #[test]
    fn an_empty_name_slugs_to_nothing() {
        assert_eq!(slugify(""), "");
        assert_eq!(slugify("   "), "");
        assert_eq!(slugify("!!!"), "");
    }

    /// The id reaches log lines, an API path and a database key, so free text
    /// is reduced to something safe rather than trusted.
    #[test]
    fn names_are_reduced_to_safe_slugs() {
        assert_eq!(slugify("Fast Scalper"), "fast-scalper");
        assert_eq!(slugify("15m  BTC"), "15m-btc");
        assert_eq!(slugify("  leading/trailing  "), "leading-trailing");
        assert_eq!(slugify("../../etc/passwd"), "etc-passwd");
    }

    /// Two different names must not collapse to one slug, or two squadrons
    /// would silently share a config row and a position namespace again.
    #[test]
    fn distinct_names_stay_distinct() {
        assert_ne!(slugify("scalper one"), slugify("scalper two"));
    }

    /// Long names are truncated, and truncation must not leave a trailing dash
    /// that makes the id look malformed.
    #[test]
    fn long_names_truncate_cleanly() {
        let s = slugify(&"a".repeat(80));
        assert_eq!(s.len(), 32);
        assert!(!s.ends_with('-'));
        let s2 = slugify(&format!("{} x", "b".repeat(31)));
        assert!(!s2.ends_with('-'), "truncation left a trailing dash: {s2:?}");
    }
}

/// Live check of the B36 feed against the venue's REST book — the test that
/// would catch a wrong reading of `price_change.size`.
///
/// Ignored by default: it needs the network and a live token. Run it as
///
/// ```text
/// DRADIS_PROBE_TOKEN=<token id> DRADIS_PROBE_SECS=180 \
///   cargo test live_book_matches_rest -- --ignored --nocapture
/// ```
///
/// It drives the production path exactly — `open_market_stream`,
/// `level_change`, `LocalBook` — and every 15s compares every level of the
/// derived book with `GET /book`. A level-for-level match across the run is
/// the evidence that `size` is the absolute level size; a delta reading
/// diverges on the first poll.
#[cfg(all(test, feature = "intl_clob"))]
mod live_book_probe {
    use super::*;
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use rust_decimal::Decimal;

    fn rest_levels(v: &serde_json::Value) -> BTreeMap<Decimal, Decimal> {
        v.as_array().into_iter().flatten().filter_map(|l| {
            let p = Decimal::from_str(l["price"].as_str()?).ok()?;
            let s = Decimal::from_str(l["size"].as_str()?).ok()?;
            (s > Decimal::ZERO).then_some((p, s))
        }).collect()
    }

    #[tokio::test]
    #[ignore = "needs the network and a live token; see the module docs"]
    async fn live_book_matches_rest() {
        let token = std::env::var("DRADIS_PROBE_TOKEN").expect("DRADIS_PROBE_TOKEN");
        let secs: u64 = std::env::var("DRADIS_PROBE_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(180);
        let token_u256 = U256::from_str(&token).expect("token id");
        // The binary installs this in `main`; a test process has to do it itself
        // or the TLS handshake panics inside the SDK's connection task.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let (_subs, stream) = open_market_stream(token_u256).expect("subscribe");
        let mut stream = Box::pin(stream);
        let http = reqwest::Client::builder().user_agent("dradis-live-book-probe").build().unwrap();
        let mut book = LocalBook::new();
        let mut levels: (BTreeMap<Decimal, Decimal>, BTreeMap<Decimal, Decimal>) = Default::default();
        let mut polls = 0u32;
        let mut polls_with_diffs = 0u32;
        // Hold accounting: how often the derived book was withheld, what
        // lifted it, and the longest it was withheld — the before/after
        // measure for the 2026-09-05 hold-and-release change.
        let mut holds = 0u32;
        let mut released_by_stamps = 0u32;
        let mut released_by_book = 0u32;
        let mut longest_hold_ms = 0i64;
        let mut held_over_1s = 0u32;
        let mut note_lift = |hold: &crate::squadron::Hold, now: DateTime<Utc>, by_book: bool| {
            let ms = hold.held_for(now).num_milliseconds();
            longest_hold_ms = longest_hold_ms.max(ms);
            if ms > 1_000 { held_over_1s += 1; }
            if by_book { released_by_book += 1; } else { released_by_stamps += 1; }
            if ms > 250 || hold.hard {
                println!("[hold] lifted after {ms}ms by {} (hard={}): {} ({} batches folded in while held)",
                         if by_book { "book" } else { "stamps" }, hold.hard, hold.reason, hold.batches);
            }
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
        let mut poll = tokio::time::interval(std::time::Duration::from_secs(15));
        poll.tick().await;

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                _ = poll.tick() => {
                    if !book.has_snapshot() { println!("[poll] no snapshot yet"); continue; }
                    let url = format!("https://clob.polymarket.com/book?token_id={token}");
                    let rest: serde_json::Value = http.get(&url).send().await.unwrap().json().await.unwrap();
                    let (rb, ra) = (rest_levels(&rest["bids"]), rest_levels(&rest["asks"]));
                    let diff = |mine: &BTreeMap<Decimal, Decimal>, theirs: &BTreeMap<Decimal, Decimal>| -> Vec<String> {
                        mine.keys().chain(theirs.keys()).collect::<std::collections::BTreeSet<_>>().into_iter()
                            .filter(|p| mine.get(p) != theirs.get(p))
                            .map(|p| format!("{p}: local={:?} rest={:?}", mine.get(p), theirs.get(p)))
                            .collect()
                    };
                    let (db, da) = (diff(&levels.0, &rb), diff(&levels.1, &ra));
                    polls += 1;
                    if !db.is_empty() || !da.is_empty() { polls_with_diffs += 1; }
                    let s = book.price_state().unwrap();
                    println!(
                        "[poll {polls}] consistent={} derived bid/ask={}/{} levels={}/{} | REST levels={}/{} | diffs bids={db:?} asks={da:?} | stats={:?}",
                        book.is_consistent(), s.0, s.2, levels.0.len(), levels.1.len(), rb.len(), ra.len(), book.stats()
                    );
                }
                msg = stream.next() => match msg {
                    Some(Ok(WsMessage::Book(b))) => {
                        levels.0 = b.bids.iter().filter(|l| l.size > Decimal::ZERO).map(|l| (l.price, l.size)).collect();
                        levels.1 = b.asks.iter().filter(|l| l.size > Decimal::ZERO).map(|l| (l.price, l.size)).collect();
                        let now = Utc::now();
                        if let Some(hold) = book.reset(b.timestamp, b.bids.iter().map(|l| (l.price, l.size)), b.asks.iter().map(|l| (l.price, l.size)), now) {
                            note_lift(&hold, now, true);
                        }
                        println!("[book] ts={} bids={} asks={}", b.timestamp, b.bids.len(), b.asks.len());
                    }
                    Some(Ok(WsMessage::PriceChange(pc))) => {
                        let mine: Vec<LevelChange> = pc.price_changes.iter()
                            .filter(|e| e.asset_id == token_u256).filter_map(level_change).collect();
                        if mine.is_empty() { continue; }
                        let now = Utc::now();
                        let out = book.apply(pc.timestamp, &mine, now);
                        // Mirror the level map so the poll can diff it level for level.
                        if matches!(out, ApplyOutcome::Applied | ApplyOutcome::Released(_) | ApplyOutcome::Held(_) | ApplyOutcome::StillHeld) {
                            for c in &mine {
                                let m = if c.side == BookSide::Bid { &mut levels.0 } else { &mut levels.1 };
                                match c.size { Some(s) if s > Decimal::ZERO => { m.insert(c.price, s); } _ => { m.remove(&c.price); } }
                            }
                        }
                        match &out {
                            ApplyOutcome::Held(_) => { holds += 1; }
                            ApplyOutcome::Released(hold) => note_lift(hold, now, false),
                            ApplyOutcome::DroppedOlder | ApplyOutcome::NoSnapshot => {
                                println!("[pc] ts={} outcome={out:?} entries={mine:?}", pc.timestamp);
                            }
                            ApplyOutcome::Applied | ApplyOutcome::StillHeld => {}
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => panic!("stream error: {e}"),
                    None => panic!("stream ended"),
                }
            }
        }
        drop(note_lift);
        println!(
            "FINAL polls={polls} polls_with_diffs={polls_with_diffs} stats={:?} consistent={} holds={holds} released_by_stamps={released_by_stamps} released_by_book={released_by_book} longest_hold_ms={longest_hold_ms} held_over_1s={held_over_1s}",
            book.stats(), book.is_consistent()
        );
        assert!(polls >= 2, "too few polls to conclude anything");
        assert!(polls_with_diffs <= 1, "derived book diverged from REST on {polls_with_diffs} of {polls} polls");
        assert!(book.is_consistent(), "book ended held: {:?}", book.hold());
    }
}
