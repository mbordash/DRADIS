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
/// ## Admiral Adama Extension
///
/// `CagEntry._handle` stores the JoinHandle for squadrons spawned directly
/// by Admiral Adama (via `register_adama_squadron`). The processor in
/// `adama.rs` polls the deployment_queue and spawns real trading squadrons
/// using `AdamaInfrastructure`, then registers them with the CAG.

pub mod session;
pub use session::SessionState;

#[cfg(feature = "intl_clob")]
pub mod run;
#[cfg(feature = "intl_clob")]
pub use run::{RunArgs, run_market_loop};

#[cfg(feature = "intl_clob")]
pub mod adama;

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

/// Lightweight, serializable summary of a squadron — sent to the Control Tower UI.
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

    /// Market taxonomy, resolved from the DB at request time by the API layer.
    /// Empty in the in-registry copy (classification runs *after* registration);
    /// `enrich_taxonomy()` populates these before the summary is serialized.
    #[serde(default)]
    pub market_class:      String,
    /// Crypto underlying that feeds this squadron's raptors (e.g. "btc").
    /// Distinct from `asset` when the venue identity differs from the
    /// underlying (Kalshi squadron asset is "KALSHI", underlying is "btc").
    /// The frontend uses this to look up raptor health in the shared map.
    #[serde(default)]
    pub underlying:        String,
    /// Implemented raptor kinds meaningful for this squadron's market class.
    #[serde(default)]
    pub raptors:           Vec<String>,
    /// Viper kinds meaningful for this squadron's market class.
    #[serde(default)]
    pub vipers:            Vec<String>,

    /// When the ENGINE retired this squadron (market closed, Helm complete,
    /// game over). An operator stand-down does not set it: the operator
    /// removed the squadron and it leaves the registry at once. A retired
    /// squadron lingers for `squadron_retire_linger_secs` so the operator can
    /// read why it ended, then `reap_retired` drops it.
    /// Skipped when absent rather than sent as `null`, so the Control Tower's
    /// optional fields are genuinely optional on the wire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stood_down_at:     Option<DateTime<Utc>>,
    /// Why the engine retired it, in the words of the retirement log line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stood_down_reason: Option<String>,

    /// The venue's id for the market this squadron flies (a condition id, a
    /// slug or a ticker), read from its deployment row by the API layer so the
    /// Markets page can open it. The registry itself never stored the id, so
    /// a summary straight from the registry carries `None` and omits the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_id:         Option<String>,
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
            market_class:      String::new(),
            underlying:        s.asset.slug(),
            raptors:           Vec::new(),
            vipers:            Vec::new(),
            stood_down_at:     None,
            stood_down_reason: None,
            // The condition id the squadron flies, so readers that join markets
            // to squadrons (the Markets page) find the hourly crypto squadrons
            // too: those never pass through `deployment_queue`, so the queue
            // lookup in `api/server.rs` has no row for them.
            market_id:         Some(s.market.condition_id.clone()).filter(|c| !c.is_empty()),
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
        self.register_with_cancel(squadron, CancellationToken::new())
    }

    /// Register a squadron along with the token that actually stops it.
    ///
    /// `register` fabricates a token instead, which nothing selects on — so
    /// `stand_down` cancelled it, logged that the signal was sent, returned
    /// true, and the squadron carried on trading. Any caller that owns a real
    /// cancellation path should use this one, so the registry's token and the
    /// trade loop's token are the same object.
    pub fn register_with_cancel(&self, squadron: &Squadron, cancel_token: CancellationToken) -> SquadronId {
        let id      = squadron.id.clone();
        let summary = SquadronSummary::from_squadron(squadron);

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

    /// Register an Admiral Adama squadron in the CAG.
    ///
    /// Called by the Adama processor after successfully spawning a squadron.
    /// Creates a `SquadronSummary` in the "PATROLLING" state visible in the
    /// Control Tower UI.
    ///
    /// The `handle` parameter is the JoinHandle from the spawned patrol task,
    /// allowing the CAG to track/cancel the squadron.
    ///
    /// `cancel_token` MUST be the token the patrol task actually selects on.
    /// This used to fabricate one with `CancellationToken::new()` — the same
    /// defect `register_with_cancel` above was written to fix — so `stand_down`
    /// cancelled a token nothing observed, logged that the signal was sent,
    /// returned true, and the squadron carried on trading. Operator-deployed
    /// intl squadrons were unkillable, and every surface reported otherwise.
    pub fn register_adama_squadron(
        &self,
        squadron_id: &str,
        market_id: &str,
        market_type: &str,
        market_question: &str,
        raptors: &[String],
        vipers: &[String],
        cancel_token: CancellationToken,
    ) -> SquadronId {

        let summary = SquadronSummary {
            id:                squadron_id.to_string(),
            asset:             market_type.to_uppercase(),
            name:              format!("{} Squadron", market_type.to_uppercase()),
            state:             "PATROLLING".to_string(),
            market_name:       market_question.to_string(),
            maker_market_name: None,
            deployed_at:       Utc::now(),
            market_class:      market_type.to_string(),
            // The raptor-health lookup key for the Control Tower, and only that.
            //
            // Not where DB rows get their underlying — that is the `TradeScope`
            // in `patrol_impl`, which is where the `underlying: 'helm'` defect
            // actually lived and is now fixed. Kept as the class so a squadron's
            // detail view keeps looking up the health it has always looked up;
            // deriving a crypto underlying here would point a politics or sports
            // squadron at a chain's raptor health, which `api/server.rs` refuses
            // on purpose.
            underlying:        market_type.to_lowercase(),
            raptors:           raptors.to_vec(),
            vipers:            vipers.to_vec(),
            stood_down_at:     None,
            stood_down_reason: None,
            market_id:         None,
        };

        self.inner.registry.insert(squadron_id.to_string(), CagEntry {
            summary,
            cancel_token,
            // The deployment processor awaits the patrol task itself, so it
            // keeps the JoinHandle; this field has never been read.
            _handle: None,
        });

        info!(
            squadron = %squadron_id,
            market_id = %market_id,
            market_type = %market_type,
            "✈️  CAG: Admiral Adama squadron registered and PATROLLING"
        );
        squadron_id.to_string()
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

    /// Stand down every squadron while LEAVING the per-asset loop tasks running.
    ///
    /// `main.rs` awaits those loop handles as the last statement of `run()`, so
    /// aborting them ends `run()`, returns from `block_on` and exits the process.
    /// That is fatal during a migration backup: `prepare` called `stand_down_all()`
    /// and the engine killed
    /// itself about 150 ms later, taking the freshly spawned backup task with the
    /// runtime. It happened on every attempt (2026-09-17, three in a row), leaving
    /// the instance retired with no archive and a blank progress state, because
    /// `PROGRESS` is in memory while `retired.json` is on disk. The operator saw a
    /// modal that never produced a download link.
    ///
    /// Cancelling the squadrons alone is enough for a quiet snapshot: retirement
    /// already refuses new orders (`refuse_if_retired` on every order path), and
    /// both the market loop (`cag::run`) and the deployment processor
    /// (`venues::deployment`) check `is_retired()` and keep squadrons down, so the
    /// surviving asset loops idle rather than re-deploying during the backup.
    pub fn stand_down_squadrons(&self) {
        for entry in self.inner.registry.iter() {
            entry.cancel_token.cancel();
        }
        info!("🛬  CAG: stand-down signal broadcast to all squadrons (asset loops left running)");
    }

    /// Stand down ALL active squadrons and asset loops.
    ///
    /// NOTE (2026-09-17): this has NO production caller. SIGTERM does not reach it
    /// — `shutdown::spawn_signal_handler` and the Setup restart handler both run
    /// the shutdown hook and call `process::exit(0)` directly, never touching the
    /// CAG. Its only remaining caller is a unit test. Aborting the asset loops ends
    /// `run()` and exits the process, so wiring this into any live path re-creates
    /// the migration self-kill described on `stand_down_squadrons`. Prefer that one.
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
            // A squadron that comes back to life sheds its retirement. Keeping
            // the stamps would show a live squadron a "retired — removed
            // shortly" banner, and would leave it reapable the moment a venue
            // loop marked it STOOD_DOWN again, defeating the rule that a
            // state-only stand-down is never reaped.
            if state != SquadronState::StoodDown {
                entry.summary.stood_down_at     = None;
                entry.summary.stood_down_reason = None;
            }
        }
    }

    /// Override the raptor-health lookup key for a squadron whose venue
    /// identity differs from its crypto underlying (e.g. Kalshi squadron
    /// asset is "KALSHI" but raptors write under "btc").
    pub fn set_underlying(&self, id: &SquadronId, underlying: &str) {
        if let Some(mut entry) = self.inner.registry.get_mut(id) {
            entry.summary.underlying = underlying.to_string();
        }
    }

    /// Rename a registered squadron's display name.
    ///
    /// Classification happens *after* registration (the squadron must exist
    /// before its market can be classified and linked), so the venue loops call
    /// this once the market class is resolved to give the squadron a name that
    /// describes what it hunts — e.g. "US Sports Squadron".
    pub fn update_name(&self, id: &SquadronId, name: String) {
        if let Some(mut entry) = self.inner.registry.get_mut(id) {
            entry.summary.name = name;
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

    /// The ENGINE ended this squadron — market closed, Helm intents complete,
    /// game over. It is marked stood down with the reason and the time, and
    /// stays listed for `squadron_retire_linger_secs` so the operator can see
    /// what happened; `reap_retired` drops it afterwards.
    ///
    /// Distinct from an operator stand-down on purpose: the operator removed
    /// that squadron, so the API removes it at once (`remove`). Routine
    /// rotation never comes through here either — RTB is a state, not an
    /// ending, and the rotation loop keeps its squadron id.
    /// The first reason wins, and so does the first clock. Two paths can retire
    /// the same squadron: the intl patrol retires itself with a precise reason
    /// (`market "<name>" <why>`, `patrol_impl.rs`), and the deployment tail then
    /// retires whatever its row produced with a generic one
    /// (`venues/deployment.rs`) because the Kalshi and US loops only mark state.
    /// Overwriting meant the operator always read the less informative reason
    /// and the linger clock restarted, so the squadron lingered twice as long as
    /// configured. A second retirement is therefore the no-op its caller's
    /// comment already claims it is.
    pub fn retire(&self, id: &SquadronId, reason: &str) {
        if let Some(mut entry) = self.inner.registry.get_mut(id) {
            if entry.summary.stood_down_at.is_some() {
                return;
            }
            entry.summary.state = SquadronState::StoodDown.to_string();
            entry.summary.stood_down_at = Some(Utc::now());
            entry.summary.stood_down_reason = Some(reason.to_string());
            info!(squadron = %id, "🏁  CAG: squadron retired — {reason}");
        }
    }

    /// Drop every retired squadron whose linger has elapsed. Only entries
    /// `retire` marked are candidates: a STOOD_DOWN entry with no
    /// `stood_down_at` (a venue path that marked state directly) is left
    /// alone, as is anything RTB or patrolling. Returns the ids dropped.
    pub fn reap_retired(&self, now: DateTime<Utc>, linger_secs: u64) -> Vec<SquadronId> {
        let candidates: Vec<SquadronId> = self.inner.registry
            .iter()
            .filter(|e| e.summary.state == SquadronState::StoodDown.to_string())
            .filter(|e| e.summary.stood_down_at
                .is_some_and(|at| (now - at).num_seconds() >= linger_secs as i64))
            .map(|e| e.key().clone())
            .collect();
        // The scan and the removal are separate steps, so re-check the condition
        // under the write lock. The venue rotation loops reuse squadron ids, and
        // `register*` inserts over whatever is there: a squadron re-registered
        // between the two steps is a live replacement, and removing it by id
        // would delete the running squadron rather than the retired one.
        let mut due = Vec::new();
        for id in candidates {
            let reaped = self.inner.registry
                .remove_if(&id, |_, e| {
                    e.summary.state == SquadronState::StoodDown.to_string()
                        && e.summary.stood_down_at
                            .is_some_and(|at| (now - at).num_seconds() >= linger_secs as i64)
                })
                .is_some();
            if reaped {
                info!(squadron = %id, "🗑️   CAG: retired squadron reaped after {linger_secs}s");
                due.push(id);
            }
        }
        due
    }

    // ── Queries ──────────────────────────────────────────────────────────────

    /// Return summaries of all registered squadrons, sorted by deployment time.
    pub fn list_squadrons(&self) -> Vec<SquadronSummary> {
        self.list_squadrons_at(
            Utc::now(),
            crate::helpers::dynamic_config::squadron_retire_linger_secs(),
        )
    }

    /// `list_squadrons` with the clock and the linger passed in, so a test can
    /// prove that listing is what does the reaping without waiting ten minutes
    /// or reaching into the global config channel.
    pub fn list_squadrons_at(&self, now: DateTime<Utc>, linger_secs: u64) -> Vec<SquadronSummary> {
        // Lazy reaping: a retired squadron whose linger has passed leaves the
        // registry the next time anyone lists it. Every reader of the list —
        // the API, the auto-deploy seeder — therefore sees the same, bounded
        // set, and nothing grows until restart.
        self.reap_retired(now, linger_secs);
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
    /// How many squadrons are still working.
    ///
    /// Retired entries are excluded. `api/migration.rs` waits on this reaching
    /// zero before snapshotting, bounded at 15s, so counting the retired
    /// squadrons that now linger for `squadron_retire_linger_secs` would burn
    /// that whole deadline for every migration started within ten minutes of
    /// any market close — a wait for squadrons that have already finished.
    pub fn squadron_count(&self) -> usize {
        self.inner.registry
            .iter()
            .filter(|e| e.summary.state != SquadronState::StoodDown.to_string())
            .count()
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

#[cfg(test)]
mod adama_registration_tests {
    use super::*;

    /// A migration backup must not kill the process that is building it.
    ///
    /// `main.rs` awaits the per-asset loop `JoinHandle`s as the last statement of
    /// `run()`. Aborting those handles therefore ends `run()`, returns from
    /// `block_on` and exits the process — fatal
    /// during `POST /api/migration/prepare`, which spawns the backup onto that same
    /// runtime. On 2026-09-17 prepare called `stand_down_all()` and the engine
    /// exited 0 about 150 ms later, three attempts in a row, each leaving the
    /// instance retired (the flag is on disk) with no archive and a blank progress
    /// state (the progress is in memory). The operator saw a modal that never
    /// produced a download link, and `build_backup` never ran at all.
    ///
    /// So the two must stay distinguishable: squadrons down, asset loops alive.
    #[tokio::test]
    async fn standing_down_squadrons_leaves_the_asset_loops_running() {
        let cag = Cag::new();
        let squadron_cancel = CancellationToken::new();
        cag.register_adama_squadron(
            "btc-open", "0xmarket", "crypto", "Bitcoin Up or Down?",
            &["price".to_string()], &["gboost".to_string()],
            squadron_cancel.clone(),
        );

        // An asset loop task that would outlive the squadrons, as main.rs spawns.
        let loop_cancel = CancellationToken::new();
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        cag.register_loop_task("btc", handle.abort_handle(), loop_cancel.clone());

        cag.stand_down_squadrons();
        assert!(squadron_cancel.is_cancelled(), "the squadron must stand down");
        assert!(!loop_cancel.is_cancelled(), "the asset loop's token must NOT be cancelled");
        assert!(!handle.is_finished(),
                "the asset loop task must still be running, or main's join returns and the process exits");

        // The SIGTERM path still takes everything down, which is what it is for.
        cag.stand_down_all();
        assert!(loop_cancel.is_cancelled(), "stand_down_all cancels the asset loop");
        let _ = handle.await;
    }

    /// The registry's token must BE the token the patrol task selects on.
    ///
    /// `register_adama_squadron` used to call `CancellationToken::new()` and
    /// store that instead, while `spawn_squadron` minted a second one for the
    /// patrol task and dropped its own handle to it. Nothing could reach the
    /// running squadron: `stand_down` cancelled an orphan, logged that the
    /// signal was sent, and returned `true`, so the Control Tower reported a
    /// successful stand-down while the squadron kept trading. This is the same
    /// defect `register_with_cancel` was written to fix on the `register` path,
    /// which is why the assertion is on the caller's token rather than on any
    /// return value.
    #[test]
    fn stand_down_cancels_the_token_the_patrol_task_holds() {
        let cag = Cag::new();
        let cancel = CancellationToken::new();

        cag.register_adama_squadron(
            "politics-open", "0xmarket", "politics", "Who wins?",
            &["price".to_string()], &["maker".to_string()],
            cancel.clone(),
        );

        assert!(!cancel.is_cancelled(), "registration must not pre-cancel the squadron");
        assert!(cag.stand_down(&"politics-open".to_string()), "squadron should be registered");
        assert!(
            cancel.is_cancelled(),
            "stand-down fired a token the patrol task does not hold — the squadron would keep trading",
        );
    }

    /// The other half of the test above, and the half that was missing.
    ///
    /// Firing the registered token only helps if the token the patrol loop
    /// selects on is DERIVED from it. On the intl venue it was not:
    /// `run_market_loop` minted a fresh `CancellationToken::new()` and moved it
    /// straight into `patrol()`, so `patrol()` awaited a token no other code
    /// held. The registered token fired into nothing, and the squadron kept
    /// entering, quoting and resting GTC bids with real money until the next
    /// hourly rotation — up to about 55 minutes — while the Control Tower showed
    /// STOOD_DOWN and promised resting orders had been cancelled.
    ///
    /// The test above passed throughout, because the CAG side was always
    /// correct. What was never asserted is this relationship.
    #[test]
    fn a_derived_patrol_token_is_cancelled_by_stand_down() {
        let cag = Cag::new();
        let cancel = CancellationToken::new();

        cag.register_adama_squadron(
            "politics-open", "0xmarket", "politics", "Who wins?",
            &["price".to_string()], &["maker".to_string()],
            cancel.clone(),
        );

        // Derived exactly as `run_market_loop` now derives it.
        let patrol_cancel = cancel.child_token();
        assert!(!patrol_cancel.is_cancelled(), "must not be pre-cancelled");

        assert!(cag.stand_down(&"politics-open".to_string()));
        assert!(
            patrol_cancel.is_cancelled(),
            "the token patrol() selects on was not reached by stand-down — \
             the squadron would keep trading real money",
        );
    }

    /// Regression guard naming the exact shape of the defect: a token minted
    /// independently of the registered one is NOT reached. If someone
    /// reintroduces `CancellationToken::new()` at the `patrol()` call site, this
    /// is what they have done.
    #[test]
    fn an_independent_patrol_token_is_never_reached_by_stand_down() {
        let cag = Cag::new();
        let cancel = CancellationToken::new();

        cag.register_adama_squadron(
            "politics-open", "0xmarket", "politics", "Who wins?",
            &["price".to_string()], &["maker".to_string()],
            cancel.clone(),
        );

        let independent = CancellationToken::new(); // what run.rs used to pass
        assert!(cag.stand_down(&"politics-open".to_string()));
        assert!(cancel.is_cancelled(), "the registered token fires");
        assert!(
            !independent.is_cancelled(),
            "an independent token is unreachable — this is why the derived token above matters",
        );
    }
}

#[cfg(test)]
mod retire_and_reap_tests {
    use super::*;

    fn register(cag: &Cag, id: &str) {
        cag.register_adama_squadron(
            id, "0xmarket", "crypto", "Bitcoin Up or Down?",
            &["price".to_string()], &["gboost".to_string()],
            CancellationToken::new(),
        );
    }

    fn ids(cag: &Cag) -> Vec<SquadronId> {
        let mut v: Vec<_> = cag.inner.registry.iter().map(|e| e.key().clone()).collect();
        v.sort();
        v
    }

    /// An engine retirement is readable for a while, then gone.
    ///
    /// Nothing used to drop a stood-down squadron at all: the registry grew
    /// until the process restarted, and a Helm squadron that retired the moment
    /// its intents went terminal sat in the panel's collapsed drawer
    /// indefinitely. A day of hourly crypto rotation leaves dozens. But dropping
    /// it the instant it retires is no better — the operator never sees why it
    /// ended. Hence the linger, and hence both halves asserted here: still
    /// listed one second before the window closes, gone one second after.
    #[test]
    fn a_retired_squadron_lingers_for_its_window_then_is_reaped() {
        let cag = Cag::new();
        register(&cag, "helm-open-accept1");
        cag.retire(&"helm-open-accept1".to_string(), "Helm intents complete");

        let retired_at = cag.inner.registry
            .get("helm-open-accept1").expect("still registered")
            .summary.stood_down_at.expect("retire records the time");

        let kept = cag.reap_retired(retired_at + chrono::Duration::seconds(599), 600);
        assert!(kept.is_empty(), "nothing is reaped before the linger elapses");
        assert_eq!(ids(&cag), vec!["helm-open-accept1".to_string()],
                   "the operator can still read why it ended");

        // Exactly at the window. The comparison is `>=`, and asserting only
        // 599/601 would let a change to `>` pass unnoticed.
        let at_the_boundary = cag.reap_retired(retired_at + chrono::Duration::seconds(600), 600);
        assert_eq!(at_the_boundary, vec!["helm-open-accept1".to_string()],
                   "the linger is inclusive: 600s after a 600s window is elapsed");
        assert!(ids(&cag).is_empty(), "the registry does not grow until restart");
    }

    /// A second retirement keeps the first reason and the first clock.
    ///
    /// Two paths retire the same squadron. The intl patrol retires itself with
    /// `market "<name>" <why>`; the deployment tail then retires whatever its
    /// row produced with "deployment finished: its market closed", because the
    /// Kalshi and US loops only mark state. Its comment says the second call is
    /// a no-op — it was not, so the operator always read the generic reason and
    /// the linger clock restarted, lingering twice as long as configured.
    #[test]
    fn retiring_twice_keeps_the_first_reason_and_the_first_clock() {
        let cag = Cag::new();
        register(&cag, "btc-open");

        cag.retire(&"btc-open".to_string(), "market \"Bitcoin Up or Down?\" closed on the venue");
        let first_at = cag.inner.registry.get("btc-open").unwrap().summary.stood_down_at.unwrap();

        cag.retire(&"btc-open".to_string(), "deployment finished: its market closed");
        let entry = cag.inner.registry.get("btc-open").unwrap();
        assert_eq!(entry.summary.stood_down_reason.as_deref(),
                   Some("market \"Bitcoin Up or Down?\" closed on the venue"),
                   "the precise reason must survive the generic one");
        assert_eq!(entry.summary.stood_down_at, Some(first_at),
                   "the clock must not restart, or the linger doubles");
    }

    /// A squadron marked STOOD_DOWN by a venue path is left alone forever.
    ///
    /// `update_state` sets the state and nothing else, and the venue rotation
    /// loops use it. Reaping on state alone would therefore delete squadrons
    /// whose retirement the engine never recorded a reason for — and, worse,
    /// would race the rotation loops that still own those ids. Only an entry
    /// `retire` stamped is a candidate, which is what the `stood_down_at`
    /// filter in `reap_retired` is for.
    #[test]
    fn a_state_only_stand_down_is_never_reaped() {
        let cag = Cag::new();
        register(&cag, "btc-open");
        cag.update_state(&"btc-open".to_string(), SquadronState::StoodDown);

        let dropped = cag.reap_retired(Utc::now() + chrono::Duration::days(30), 600);
        assert!(dropped.is_empty(), "no timestamp, no reaping — however old the clock says it is");
        assert_eq!(ids(&cag), vec!["btc-open".to_string()]);
    }

    /// Reaping never touches a squadron that is still working, and the state
    /// filter is what guarantees it.
    ///
    /// RTB is the dangerous one. It is an operating phase, not an ending — the
    /// squadron has stopped opening and is managing open positions to close, and
    /// it is entered 60s before every market close. Reaping one would drop a
    /// squadron that still holds money.
    ///
    /// The stamp is written straight into the registry here because no public
    /// path produces a stamped RTB entry any more — `update_state` clears the
    /// stamps on revival, which is the point of
    /// `a_revived_squadron_sheds_its_retirement`. Reaching in is deliberate: it
    /// is the only way to prove the `state == STOOD_DOWN` filter carries its own
    /// weight rather than being shadowed by the timestamp filter.
    #[test]
    fn reaping_leaves_every_working_squadron_where_it_is() {
        let cag = Cag::new();
        register(&cag, "btc-open");
        register(&cag, "sports-open");
        {
            let mut entry = cag.inner.registry.get_mut("btc-open").unwrap();
            entry.summary.state = SquadronState::Rtb.to_string();
            entry.summary.stood_down_at = Some(Utc::now() - chrono::Duration::days(30));
        }

        let dropped = cag.reap_retired(Utc::now(), 600);
        assert!(dropped.is_empty(),
                "RTB is winding down positions, not ended — however stale the stamp");
        assert_eq!(ids(&cag), vec!["btc-open".to_string(), "sports-open".to_string()]);
    }

    /// A squadron that comes back to life sheds its retirement.
    ///
    /// Keeping the stamps would show a live squadron the UI's "retired —
    /// removed shortly" banner, which is keyed on the reason alone. Worse, the
    /// stale timestamp would make it instantly reapable the moment a venue loop
    /// marked it STOOD_DOWN again, defeating the rule that a state-only
    /// stand-down is never reaped.
    #[test]
    fn a_revived_squadron_sheds_its_retirement() {
        let cag = Cag::new();
        register(&cag, "btc-open");
        cag.retire(&"btc-open".to_string(), "market closed");
        cag.update_state(&"btc-open".to_string(), SquadronState::Patrolling);

        {
            let entry = cag.inner.registry.get("btc-open").unwrap();
            assert!(entry.summary.stood_down_at.is_none(), "a live squadron is not retired");
            assert!(entry.summary.stood_down_reason.is_none(), "and shows no retirement banner");
        }

        // And is therefore safe from reaping once it next stands down.
        cag.update_state(&"btc-open".to_string(), SquadronState::StoodDown);
        let dropped = cag.reap_retired(Utc::now() + chrono::Duration::days(30), 600);
        assert!(dropped.is_empty(), "a state-only stand-down is never reaped, revival or not");
    }

    /// An operator stand-down removes the squadron; a retirement does not.
    ///
    /// The two paths are deliberately different, and conflating them is what
    /// produced the complaint that started this: the operator stood a squadron
    /// down and it stayed in the panel. `remove` is for "I removed it, it should
    /// go"; `retire` is for the engine ending something the operator did not ask
    /// to end, which is worth a reason and a moment to read it.
    ///
    /// This covers the registry primitives only. That `POST
    /// /api/squadrons/{id}/stand-down` calls `remove` rather than `retire` is
    /// not asserted here — deleting that call in `api/server.rs` would not fail
    /// this test, and proving it needs the handler's `AppState`.
    #[test]
    fn an_operator_removal_is_immediate_and_a_retirement_is_not() {
        let cag = Cag::new();
        register(&cag, "helm-open-operator");
        register(&cag, "helm-open-engine");

        cag.remove(&"helm-open-operator".to_string());
        cag.retire(&"helm-open-engine".to_string(), "market closed");

        assert_eq!(ids(&cag), vec!["helm-open-engine".to_string()],
                   "the operator's squadron leaves at once; the engine's lingers with its reason");
        let entry = cag.inner.registry.get("helm-open-engine").expect("still listed");
        assert_eq!(entry.summary.state, "STOOD_DOWN");
        assert_eq!(entry.summary.stood_down_reason.as_deref(), Some("market closed"),
                   "the engine's retirement carries the reason the operator will read");
        assert!(entry.summary.stood_down_at.is_some(), "and the clock the linger runs on");
    }

    /// A working squadron count must not wait on finished ones.
    ///
    /// `api/migration.rs` waits up to 15s for `squadron_count()` to reach zero
    /// before snapshotting. Retired squadrons now linger for ten minutes, so
    /// counting them would burn that whole deadline on squadrons that have
    /// already stopped trading — for any migration started within ten minutes
    /// of a market close.
    #[test]
    fn retired_squadrons_do_not_count_as_working() {
        let cag = Cag::new();
        register(&cag, "btc-open");
        register(&cag, "sports-open");
        assert_eq!(cag.squadron_count(), 2);

        cag.retire(&"btc-open".to_string(), "market closed");
        assert_eq!(cag.squadron_count(), 1, "a retired squadron is not still working");

        cag.update_state(&"sports-open".to_string(), SquadronState::Rtb);
        assert_eq!(cag.squadron_count(), 1,
                   "but RTB is: it still holds positions, and the backup must wait for it");
    }

    /// Listing is what does the reaping, so every reader sees the same bounded
    /// set and nothing needs a timer task.
    ///
    /// Both halves are asserted through `list_squadrons_at`, because a test that
    /// only proves the "keeps" half would pass just as well if `list_squadrons`
    /// never reaped at all — and one that leaned on the real
    /// `squadron_retire_linger_secs()` would be passing by accident of the
    /// default being 600 in every profile.
    #[test]
    fn listing_is_what_reaps_and_a_just_retired_squadron_survives_it() {
        let cag = Cag::new();
        register(&cag, "helm-open-fresh");
        register(&cag, "helm-open-stale");
        cag.retire(&"helm-open-fresh".to_string(), "Helm intents complete");
        cag.retire(&"helm-open-stale".to_string(), "market closed");

        let fresh_at = cag.inner.registry
            .get("helm-open-fresh").unwrap().summary.stood_down_at.unwrap();

        // Listing immediately: both are young, both survive with their reasons.
        let listed = cag.list_squadrons_at(fresh_at, 600);
        assert_eq!(listed.len(), 2, "a retirement must survive the listing that follows it");
        let fresh = listed.iter().find(|s| s.id == "helm-open-fresh").expect("listed");
        assert_eq!(fresh.state, "STOOD_DOWN");
        assert_eq!(fresh.stood_down_reason.as_deref(), Some("Helm intents complete"));

        // Listing after the window: the list itself is what removes them.
        let listed = cag.list_squadrons_at(fresh_at + chrono::Duration::seconds(601), 600);
        assert!(listed.is_empty(), "listing reaps — nothing else is going to");
        assert!(ids(&cag).is_empty(), "and the registry is bounded, not just the list");
    }
}
