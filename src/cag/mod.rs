/// CAG — Carrier Air Group
///
/// The CAG is the top-level coordinator that manages a fleet of independently
/// running Squadrons.  Each Squadron patrols its own Polymarket market on a
/// separate async task; the CAG owns the registry and can spawn, query, and
/// stand down individual squadrons at runtime.
///
/// ┌────────────────────────────────────────────────────────────────┐
/// │                           CAG                                  │
/// │                                                                │
/// │  Squadron registry  ──►  DashMap<SquadronId, CagEntry>        │
/// │  sessions           ──►  HashMap<asset, SessionState> 3f-7    │
/// │  run_market_loop()  ──►  register() + squadron.patrol()       │
/// │  list_squadrons()   ──►  Vec<SquadronSummary> (for API/UI)     │
/// │  stand_down()       ──►  fire CancellationToken for one squad  │
/// └────────────────────────────────────────────────────────────────┘
///
/// Phase 3f-7 (current):
///   • `run_market_loop` in `run.rs` drives the full patrol lifecycle:
///     it calls `cag.register(&squadron)` then loops calling
///     `squadron.patrol(cancel, &mut ctx)` on each market rotation.
///   • The CAG holds one `SessionState` per asset (keyed by slug).
///   • All API data endpoints accept `?asset=` to query per-asset DBs.

pub mod session;
pub use session::SessionState;

pub mod run;
pub use run::{RunArgs, run_market_loop};

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::squadron::{Squadron, SquadronId, SquadronState, CryptoAsset};
use crate::squadron::config::SquadronConfig;

// ─── Types ────────────────────────────────────────────────────────────────────

/// Lightweight, serialisable summary of a squadron — sent to the Control Tower UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SquadronSummary {
    pub id:                SquadronId,
    pub asset:             String,           // CryptoAsset::symbol()
    pub name:              String,           // SquadronConfig::name
    pub state:             String,           // SquadronState::Display
    /// Primary (hourly) battle location.
    pub market_name:       String,
    /// Window/daily maker venue — `None` until the fee-rate fetch resolves it,
    /// a few seconds after the squadron is first registered.
    pub maker_market_name: Option<String>,
    pub deployed_at:       DateTime<Utc>,
}

impl SquadronSummary {
    /// Build a summary by borrowing an active Squadron.
    pub fn from_squadron(s: &Squadron) -> Self {
        Self {
            id:                s.id.clone(),
            asset:             s.asset.symbol(),
            name:              s.config.name.clone(),
            state:             s.state.to_string(),
            market_name:       s.market.market_name.clone(),
            maker_market_name: None,
            deployed_at:       s.deployed_at,
        }
    }
}

/// Internal CAG registry entry — bundles the live task handle with its cancel token.
struct CagEntry {
    summary:      SquadronSummary,
    cancel_token: CancellationToken,
    /// Reserved for a future phase where the CAG directly owns and spawns the
    /// patrol task.  Currently `None` — `run_market_loop` in `run.rs` drives
    /// `squadron.patrol()` directly and manages task lifetime itself.
    _handle:      Option<JoinHandle<()>>,
}

// ─── Cag ──────────────────────────────────────────────────────────────────────

/// The CAG manages all live squadrons for a single DRADIS instance.
///
/// Cheaply cloneable via `Arc` — hand a clone to every axum handler that
/// needs to query or mutate the squadron registry.
#[derive(Clone)]
pub struct Cag {
    inner: Arc<CagInner>,
}

struct CagInner {
    registry: DashMap<SquadronId, CagEntry>,
    /// Phase 3f-7: per-asset session map.  Key = lowercase asset symbol ("btc", "eth", …).
    /// The first asset registered is the "primary" for backward-compat callers.
    /// `RwLock` allows concurrent reads from API handlers without blocking.
    sessions: RwLock<HashMap<String, SessionState>>,
}

impl Cag {
    /// Create an empty CAG.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CagInner {
                registry: DashMap::new(),
                sessions: RwLock::new(HashMap::new()),
            }),
        }
    }

    // ── Session state ────────────────────────────────────────────────────────

    /// Store a session state bundle in the CAG, keyed by `session.asset`.
    ///
    /// Called from `main.rs` for each asset in the fleet after `SessionState`
    /// is constructed.  The CAG holds the canonical reference that API handlers
    /// can clone from via `session_for_asset()`.
    pub fn set_session(&self, session: SessionState) {
        let key = session.asset.clone();
        self.inner.sessions.write().expect("CAG sessions RwLock poisoned")
            .insert(key.clone(), session);
        info!("🗄️  CAG: session state registered for asset {} (Phase 3f-7)", key);
    }

    /// Return a clone of the session for the given asset, or `None` if not yet set.
    pub fn session_for_asset(&self, asset: &str) -> Option<SessionState> {
        self.inner.sessions.read().expect("CAG sessions RwLock poisoned")
            .get(&asset.to_lowercase()).cloned()
    }

    /// Return a clone of the **primary** (first registered) session, or `None`.
    ///
    /// Backward-compat accessor — callers that predate multi-asset support.
    pub fn session(&self) -> Option<SessionState> {
        self.inner.sessions.read().expect("CAG sessions RwLock poisoned")
            .values().next().cloned()
    }

    /// Return the lowercase asset names of all registered sessions, sorted.
    pub fn asset_names(&self) -> Vec<String> {
        let guard = self.inner.sessions.read().expect("CAG sessions RwLock poisoned");
        let mut v: Vec<String> = guard.keys().cloned().collect();
        v.sort();
        v
    }

    // ── Squadron management ──────────────────────────────────────────────────

    /// **Phase 3e** — Register a squadron that is still owned and driven by the
    /// existing `market_loop` in `main.rs`.  Borrows the squadron to build the
    /// summary; the caller retains ownership so the loop can continue using it.
    ///
    /// The returned `SquadronId` can be used with `update_state()` and `remove()`
    /// to keep the registry in sync as the market_loop transitions the squadron.
    pub fn register(&self, squadron: &Squadron) -> SquadronId {
        let id      = squadron.id.clone();
        let summary = SquadronSummary::from_squadron(squadron);
        let cancel_token = CancellationToken::new(); // unused in Phase 3e but kept for API symmetry

        self.inner.registry.insert(id.clone(), CagEntry {
            summary,
            cancel_token,
            _handle: None,
        });

        info!(squadron = %id, "✈️  CAG: squadron registered (Phase 3e stub)");
        id
    }

    /// **Reserved** — Take ownership of a Squadron and register it in the CAG.
    ///
    /// Currently behaves identically to `register()`: it adds the squadron to
    /// the summary registry but does NOT spawn a patrol task.  The patrol
    /// lifecycle is driven by `run_market_loop` in `run.rs`, which calls
    /// `cag.register(&squadron)` then loops calling `squadron.patrol(…)`.
    ///
    /// A future phase may move task ownership here so the CAG can independently
    /// restart or stand down individual squadrons via the cancellation token.
    pub fn spawn_squadron(&self, squadron: Squadron) -> SquadronId {
        let id      = squadron.id.clone();
        let summary = SquadronSummary::from_squadron(&squadron);
        let cancel_token = CancellationToken::new();

        self.inner.registry.insert(id.clone(), CagEntry {
            summary,
            cancel_token,
            _handle: None,
        });

        info!(squadron = %id, "✈️  CAG: squadron registered via spawn_squadron");
        id
    }

    /// Stand down a specific squadron by firing its cancellation token.
    ///
    /// Returns `true` if the squadron was found and signalled, `false` if unknown.
    pub fn stand_down(&self, id: &SquadronId) -> bool {
        if let Some(entry) = self.inner.registry.get(id) {
            entry.cancel_token.cancel();
            info!(squadron = %id, "🛬  CAG: stand-down signal sent");
            true
        } else {
            warn!(squadron = %id, "CAG: unknown squadron — stand-down ignored");
            false
        }
    }

    /// Stand down ALL active squadrons (e.g. on SIGTERM).
    pub fn stand_down_all(&self) {
        for entry in self.inner.registry.iter() {
            entry.cancel_token.cancel();
        }
        info!("🛬  CAG: stand-down signal broadcast to all squadrons");
    }

    /// Update the persisted state of a squadron in the registry summary.
    ///
    /// Called by the tick-loop (Phase 3f) when a squadron transitions states.
    pub fn update_state(&self, id: &SquadronId, state: SquadronState) {
        if let Some(mut entry) = self.inner.registry.get_mut(id) {
            entry.summary.state = state.to_string();
        }
    }

    /// Record the window/daily maker venue name for a registered squadron.
    ///
    /// Called from `run_market_loop` once the maker market fee-rate fetch
    /// completes — a few seconds after the squadron is first registered.
    pub fn update_maker_market(&self, id: &SquadronId, maker_market_name: String) {
        if let Some(mut entry) = self.inner.registry.get_mut(id) {
            entry.summary.maker_market_name = Some(maker_market_name);
        }
    }

    /// Remove a stood-down squadron from the registry (housekeeping).
    pub fn remove(&self, id: &SquadronId) {
        self.inner.registry.remove(id);
        info!(squadron = %id, "🗑️   CAG: squadron removed from registry");
    }

    // ── Queries ──────────────────────────────────────────────────────────────

    /// Return summaries of all registered squadrons, sorted by deployment time.
    pub fn list_squadrons(&self) -> Vec<SquadronSummary> {
        let mut list: Vec<_> = self.inner.registry
            .iter()
            .map(|e| e.summary.clone())
            .collect();
        list.sort_by_key(|s| s.deployed_at);
        list
    }

    /// Return the summary for one squadron, or `None` if not found.
    pub fn get_squadron(&self, id: &SquadronId) -> Option<SquadronSummary> {
        self.inner.registry.get(id).map(|e| e.summary.clone())
    }

    /// Number of currently registered (not yet removed) squadrons.
    pub fn squadron_count(&self) -> usize {
        self.inner.registry.len()
    }

    // ── Phase 3f stub ────────────────────────────────────────────────────────

    /// Drive the full CAG lifecycle: spawn all configured squadrons and wait
    /// until a global cancellation token fires, then stand down all squadrons.
    ///
    /// **Phase 3f** — this replaces `main.rs`'s `'market_loop`.
    /// Currently a stub that immediately returns.
    pub async fn run(&self, _cancel: CancellationToken) {
        // Phase 3f wiring pending — market_loop in main.rs still drives execution.
        tracing::info!("🗼  CAG::run() stub — Phase 3f wiring pending");
    }
}

impl Default for Cag {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Builder ─────────────────────────────────────────────────────────────────

/// Convenience builder for constructing a squadron config before handing it to
/// the CAG.  Intended for use by both main.rs (Phase 3e) and the Control Tower
/// API's `POST /api/squadrons` handler (Phase 3d).
pub struct SquadronBuilder {
    pub asset:  CryptoAsset,
    pub config: SquadronConfig,
}

impl SquadronBuilder {
    pub fn new(asset: CryptoAsset, config: SquadronConfig) -> Self {
        Self { asset, config }
    }
}
