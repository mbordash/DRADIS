/// CAG — Carrier Air Group
///
/// The CAG is the top-level coordinator that manages a fleet of independently
/// running Squadrons.  Each Squadron patrols its own Polymarket market on a
/// separate async task; the CAG owns the registry and can spawn, query, and
/// stand down individual squadrons at runtime.
///
/// ┌──────────────────────────────────────────────────────────────────────┐
/// │                              CAG                                     │
/// │                                                                      │
/// │  asset_tasks  ──►  DashMap<asset, AssetTask>                        │
/// │                     • AbortHandle     — force-terminate the loop    │
/// │                     • CancellationToken — graceful exit signal       │
/// │                                                                      │
/// │  registry     ──►  DashMap<SquadronId, CagEntry>                    │
/// │                     squadron summaries (new entry each rotation)     │
/// │                                                                      │
/// │  sessions     ──►  HashMap<asset, SessionState>                     │
/// │                     positions / P&L / collateral per asset           │
/// │                                                                      │
/// │  stand_down_asset()  ──►  cancel token + abort handle               │
/// │  stand_down_all()    ──►  cancels every asset loop                  │
/// └──────────────────────────────────────────────────────────────────────┘
///
/// ## Architecture — asset vs. squadron ownership
///
/// `run_market_loop` (`run.rs`) is the real top-level lifecycle driver.
/// `main.rs` is a thin bootstrapper that:
///   1. Assembles `RunArgs` (clients, raptors, session state, cancel token).
///   2. Calls `tokio::spawn(run_market_loop(args))` once per asset.
///   3. Calls `cag.register_loop_task(asset, handle.abort_handle(), cancel)`
///      so the CAG can gracefully or forcibly terminate any asset loop.
///
/// `run_market_loop` creates a fresh `Squadron` (new `SquadronId`) on every
/// market rotation, so one loop task outlives many squadron IDs.  The
/// `AbortHandle` therefore lives in `asset_tasks`, keyed by asset, not in
/// any individual `CagEntry`.
///
/// `CagEntry._handle` is reserved for the Admiral Adama extension, where the
/// CAG will directly spawn individual one-shot patrol tasks into user-chosen
/// markets.  It is always `None` in the current architecture.

pub mod session;
pub use session::SessionState;

#[cfg(feature = "intl_clob")]
pub mod run;
#[cfg(feature = "intl_clob")]
pub use run::{RunArgs, run_market_loop};

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::task::{JoinHandle, AbortHandle};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::squadron::{Squadron, SquadronId, SquadronState, CryptoAsset};
use crate::squadron::config::SquadronConfig;

// ─── Types ────────────────────────────────────────────────────────────────────

/// Per-asset loop task owned by the CAG.
///
/// One `AssetTask` is stored for every asset in the fleet (btc, eth, sol, …).
/// It gives the CAG the ability to signal a graceful exit (via `cancel`) and,
/// if the task has not exited, abort it outright (via `abort_handle.abort()`).
///
/// `AbortHandle` is used instead of `JoinHandle` because it is cheaply cloneable
/// and does not require ownership of the task future.  `main.rs` retains the
/// `JoinHandle` for awaiting; the CAG holds the `AbortHandle` for control.
struct AssetTask {
    abort_handle: AbortHandle,
    cancel:       CancellationToken,
}

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
    /// Per-asset loop tasks.  Key = lowercase asset symbol ("btc", "eth", …).
    /// Populated by `register_loop_task()` immediately after `main.rs` spawns
    /// each `run_market_loop` task.  Gives the CAG hard ownership of every
    /// long-running patrol task for graceful/forced stand-down.
    asset_tasks: DashMap<String, AssetTask>,
    /// Per-asset session map.  Key = lowercase asset symbol ("btc", "eth", …).
    /// The first asset registered is the "primary" for backward-compat callers.
    /// `RwLock` allows concurrent reads from API handlers without blocking.
    sessions: RwLock<HashMap<String, SessionState>>,
}

impl Cag {
    /// Create an empty CAG.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CagInner {
                registry:    DashMap::new(),
                asset_tasks: DashMap::new(),
                sessions:    RwLock::new(HashMap::new()),
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
        info!("🗄️  CAG: session state registered for asset {}", key.to_uppercase());
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

    // ── Asset loop task ownership ────────────────────────────────────────────

    /// Register the `AbortHandle` and `CancellationToken` for a per-asset
    /// `run_market_loop` task.
    ///
    /// Called from `main.rs` immediately after `tokio::spawn(run_market_loop(args))`.
    /// `main.rs` retains the `JoinHandle` for awaiting; the CAG holds the
    /// `AbortHandle` (cloneable, no ownership required) for control operations.
    pub fn register_loop_task(&self, asset: &str, abort_handle: AbortHandle, cancel: CancellationToken) {
        let key = asset.to_lowercase();
        self.inner.asset_tasks.insert(key.clone(), AssetTask { abort_handle, cancel });
        info!("✈️  CAG: loop task registered for asset {}", key.to_uppercase());
    }

    /// Stand down a single asset's market loop.
    ///
    /// Fires the `CancellationToken` first (gives `run_market_loop` a chance to
    /// exit cleanly at the next `'market_loop` iteration boundary), then calls
    /// `abort_handle.abort()` to guarantee termination even if the loop is
    /// blocked on an I/O await.
    ///
    /// Returns `true` if the asset was found, `false` if unknown.
    pub fn stand_down_asset(&self, asset: &str) -> bool {
        let key = asset.to_lowercase();
        if let Some(entry) = self.inner.asset_tasks.get(&key) {
            entry.cancel.cancel();
            entry.abort_handle.abort();
            info!("🛬  CAG: stand-down signal + abort sent for asset {}", key.to_uppercase());
            true
        } else {
            warn!("CAG: unknown asset '{}' — stand-down ignored", key);
            false
        }
    }

    /// Return the lowercase asset names of all registered loop tasks, sorted.
    pub fn loop_asset_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.inner.asset_tasks.iter()
            .map(|e| e.key().clone())
            .collect();
        v.sort();
        v
    }

    // ── Squadron management ──────────────────────────────────────────────────

    /// Register a squadron in the CAG summary registry.
    ///
    /// Borrows the squadron to build the summary; the caller (`run_market_loop`)
    /// retains ownership so the patrol loop can continue using it.  The returned
    /// `SquadronId` can be used with `update_state()`, `update_maker_market()`,
    /// and `remove()` to keep the registry in sync across market rotations.
    pub fn register(&self, squadron: &Squadron) -> SquadronId {
        let id      = squadron.id.clone();
        let summary = SquadronSummary::from_squadron(squadron);
        let cancel_token = CancellationToken::new(); // reserved for Admiral Adama extension

        self.inner.registry.insert(id.clone(), CagEntry {
            summary,
            cancel_token,
            _handle: None,
        });

        info!(squadron = %id, "✈️  CAG: squadron registered");
        id
    }

    /// Reserved for the Admiral Adama extension — will spawn an individual
    /// patrol task for a user-chosen market.  Currently behaves identically to
    /// `register()`: adds the squadron to the summary registry but does NOT
    /// spawn a patrol task.  The patrol lifecycle is driven by `run_market_loop`.
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

    /// Stand down ALL active squadrons and asset loops (e.g. on SIGTERM).
    ///
    /// Fires cancellation tokens on every registered squadron entry AND every
    /// per-asset `run_market_loop` task, then aborts each loop task handle to
    /// guarantee termination even if a loop is blocked on I/O.
    pub fn stand_down_all(&self) {
        // Signal squadron-level cancel tokens (patrol watchdog path).
        for entry in self.inner.registry.iter() {
            entry.cancel_token.cancel();
        }
        // Cancel + abort every asset loop task.
        for entry in self.inner.asset_tasks.iter() {
            entry.cancel.cancel();
            entry.abort_handle.abort();
        }
        info!("🛬  CAG: stand-down signal broadcast to all squadrons and asset loops");
    }

    /// Update the persisted state of a squadron in the registry summary.
    ///
    /// Called by `run_market_loop` when a squadron transitions states.
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

}

impl Default for Cag {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Builder ─────────────────────────────────────────────────────────────────

/// Convenience builder for constructing a squadron config before handing it to
/// the CAG.  Used by `main.rs` and reserved for the future
/// `POST /api/squadrons` handler (Admiral Adama extension).
pub struct SquadronBuilder {
    pub asset:  CryptoAsset,
    pub config: SquadronConfig,
}

impl SquadronBuilder {
    pub fn new(asset: CryptoAsset, config: SquadronConfig) -> Self {
        Self { asset, config }
    }
}
