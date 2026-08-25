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
use polymarket_client_sdk_v2::clob::ws::Client as WsClient;

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
        // Every call site now passes None, so every id already ends in `-open`
        // and nothing anywhere reads `-hourly`. Keeping the suffix rather than
        // dropping it holds existing ids byte-identical, so no operator's saved
        // config is orphaned; making it constant means a future caller cannot
        // reintroduce the flip by passing a close time.
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
    /// not yet initialised. Call after the squadron's config row is seeded.
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

/// Spawn one auto-reconnecting WebSocket orderbook subscriber task.
///
/// Pushes `PriceState` updates into `tx`.  Stops cleanly when `cancel` fires.
/// The `venue` label is used only for log messages.
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

            let client = WsClient::default();
            let stream = match client.subscribe_orderbook(vec![token]) {
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

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => { return; }
                    result = stream.next() => {
                        match result {
                            Some(Ok(book)) => {
                                let (bid, bid_depth) = book.bids.iter()
                                    .max_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                                    .map(|l| (l.price, l.size))
                                    .unwrap_or((dec!(0), dec!(0)));
                                let (ask, ask_depth) = book.asks.iter()
                                    .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
                                    .map(|l| (l.price, l.size))
                                    .unwrap_or((dec!(1), dec!(0)));
                                // Aggregate depth across EVERY published level, alongside
                                // the touch sizes above. The venue sends the whole book
                                // (`Vec<OrderBookLevel>`); until now everything but the
                                // best level was discarded, which is why order-book
                                // imbalance was really a top-of-book ratio. Published for
                                // comparison only — no consumer's behaviour changes yet.
                                let bid_depth_total: rust_decimal::Decimal = book.bids.iter().map(|l| l.size).sum();
                                let ask_depth_total: rust_decimal::Decimal = book.asks.iter().map(|l| l.size).sum();
                                // Stamp the WS update time at receipt, NOT at tick time.
                                let _ = tx.send((
                                    bid, bid_depth, ask, ask_depth, Utc::now(),
                                    bid_depth_total, ask_depth_total,
                                ));
                            }
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
