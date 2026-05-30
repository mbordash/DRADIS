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
/// └──────────────────────────────────────────────────────────────┘
///
/// Phase 2 (current): types are defined and wired into the CIC (main.rs).
///                    The existing `'market_loop` constructs a SquadronRaptors
///                    and Squadron descriptor for each deployment.
/// Phase 3 (next):    The CAG replaces the manual market-rotation loop with
///                    `Squadron::patrol()` — a proper async run method that
///                    owns the tick loop and can be independently cancelled.

pub mod raptors;
pub mod config;

pub use raptors::SquadronRaptors;
pub use config::{SquadronConfig, RaptorProfile, ViperProfile};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::state::MarketConfig;

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

/// A fully-described squadron deployment.
///
/// In Phase 2 this is a descriptor/record type — owned by the CIC's market
/// loop at runtime.  In Phase 3 it will grow a `patrol()` async method that
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
}

impl Squadron {
    /// Construct a new squadron descriptor at deployment time.
    pub fn new(
        asset:   CryptoAsset,
        config:  SquadronConfig,
        market:  MarketConfig,
        raptors: SquadronRaptors,
    ) -> Self {
        let deployed_at = Utc::now();
        let id = format!(
            "{}-{}-{}",
            asset.slug(),
            if market.market_close_time.is_some() { "hourly" } else { "open" },
            deployed_at.format("%Y-%m-%dT%H:%M:%SZ"),
        );
        Self {
            id,
            asset,
            config,
            market,
            raptors,
            state: SquadronState::Deployed,
            deployed_at,
        }
    }

    /// Transition to Patrolling once the tick loop starts.
    pub fn start_patrol(&mut self) {
        self.state = SquadronState::Patrolling;
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

    // ─── Phase 3 stub ────────────────────────────────────────────────────────

    /// Run the squadron's full patrol lifecycle as an independent async task.
    ///
    /// **Phase 3 entry point** — when fully implemented this method will:
    ///   1. Transition state to `Patrolling`.
    ///   2. Drive the tick loop (currently in `main.rs`'s `'market_loop`) until the
    ///      market expires or a cancellation token fires.
    ///   3. Wind down open positions (RTB phase).
    ///   4. Transition to `StoodDown` and return.
    ///
    /// The CAG calls `tokio::spawn(squadron.patrol(cancel_token))` for each
    /// squadron so they run concurrently without blocking one another.
    ///
    /// **Currently a stub** — wiring to the CIC tick-loop happens in Phase 3f.
    pub async fn patrol(mut self, cancel: tokio_util::sync::CancellationToken) -> Self {
        self.start_patrol();
        tracing::info!(
            squadron = %self.id,
            asset    = %self.asset,
            market   = %self.market.market_name,
            "⚔️  Squadron patrol() stub — tick-loop wiring pending Phase 3f",
        );
        // Await cancellation — real implementation replaces this with the tick loop.
        cancel.cancelled().await;
        self.stand_down();
        self
    }
}

