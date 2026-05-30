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
/// │  spawn_squadron()   ──►  tokio::spawn(Squadron::patrol())      │
/// │  list_squadrons()   ──►  Vec<SquadronSummary> (for API/UI)     │
/// │  stand_down()       ──►  fire CancellationToken for one squad  │
/// │  run()              ──►  Phase 3f — replaces market_loop       │
/// └────────────────────────────────────────────────────────────────┘
///
/// Phase 3e (upcoming): main.rs instantiates a `Cag` and registers the single
///                      active squadron into it (market_loop still runs).
/// Phase 3f (cutover):  market_loop is removed; `Cag::run()` drives everything.

use std::sync::Arc;
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
    pub id:          SquadronId,
    pub asset:       String,          // CryptoAsset::symbol()
    pub name:        String,          // SquadronConfig::name
    pub state:       String,          // SquadronState::Display
    pub market_name: String,
    pub deployed_at: DateTime<Utc>,
}

impl SquadronSummary {
    /// Build a summary by borrowing an active Squadron.
    pub fn from_squadron(s: &Squadron) -> Self {
        Self {
            id:          s.id.clone(),
            asset:       s.asset.symbol(),
            name:        s.config.name.clone(),
            state:       s.state.to_string(),
            market_name: s.market.market_name.clone(),
            deployed_at: s.deployed_at,
        }
    }
}

/// Internal CAG registry entry — bundles the live task handle with its cancel token.
struct CagEntry {
    summary:      SquadronSummary,
    cancel_token: CancellationToken,
    /// The join handle for the `Squadron::patrol()` task.
    /// `None` in Phase 3e (stub path) when no task is actually running yet.
    _handle:      Option<JoinHandle<Squadron>>,
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
}

impl Cag {
    /// Create an empty CAG.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CagInner {
                registry: DashMap::new(),
            }),
        }
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

    /// **Phase 3f** — Take ownership of a Squadron and spawn its `patrol()` task
    /// as an independent Tokio task.  The CAG holds the `JoinHandle` and a
    /// `CancellationToken` to signal stand-down.
    ///
    /// Returns the `SquadronId` assigned to this squadron.
    pub fn spawn_squadron(&self, squadron: Squadron) -> SquadronId {
        let id      = squadron.id.clone();
        let summary = SquadronSummary::from_squadron(&squadron);
        let cancel_token = CancellationToken::new();

        let token = cancel_token.clone();
        let handle = tokio::spawn(async move { squadron.patrol(token).await });

        self.inner.registry.insert(id.clone(), CagEntry {
            summary,
            cancel_token,
            _handle: Some(handle),
        });

        info!(squadron = %id, "✈️  CAG: squadron spawned (Phase 3f)");
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



