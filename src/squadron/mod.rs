/// Squadron — the core deployable unit of DRADIS.
///
/// A Squadron bundles one or more Raptors (signal scouts) with one or more
/// Vipers (trading strategies) and sends them to a specific Polymarket market
/// (the battle location).
///
/// ┌──────────────────────────────────────────────────────────────┐
/// │                        Squadron                              │
/// │                                                              │
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
use crate::state::MarketConfig;

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
    pub config:  SquadronConfig,
    pub market:  MarketConfig,
    pub raptors: SquadronRaptors,
    pub state:   SquadronState,
    pub deployed_at: DateTime<Utc>,
}

impl Squadron {
    /// Construct a new squadron descriptor at deployment time.
    pub fn new(
        config:  SquadronConfig,
        market:  MarketConfig,
        raptors: SquadronRaptors,
    ) -> Self {
        let deployed_at = Utc::now();
        let id = format!(
            "{}-{}-{}",
            market.market_name
                .to_lowercase()
                .split_whitespace()
                .next()
                .unwrap_or("unknown"),
            if market.market_close_time.is_some() { "hourly" } else { "open" },
            deployed_at.format("%Y-%m-%dT%H:%M:%SZ"),
        );
        Self {
            id,
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
}

