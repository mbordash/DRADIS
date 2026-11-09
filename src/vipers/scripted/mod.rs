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

//! [E86] Scripted vipers: strategies the operator writes and saves without a
//! rebuild.
//!
//! A Marketplace instance ships binaries and cannot compile the engine, so an
//! operator's strategy is a Rhai script on the data volume, run behind one
//! generic adapter (`adapter::ScriptedStrategy`) that owns every safety check.
//!
//! * `manifest` — what a script declares: name, classes, venues, cap, params.
//! * `runtime`  — the sandboxed engine, helper functions, and the translation
//!   of a script's answer into an `Action`.
//! * `context`  — the read-only map a script sees.
//! * `store`    — the loaded scripts, saves and hot reload, per-squadron state.
//! * `paper`    — the ledger a viper trades in while its Live switch is off.
//! * `adapter`  — the `Strategy` implementation, and the dry run.
//! * `templates` — the starters the Build a Viper page offers.

pub mod adapter;
pub mod bundle;
pub mod context;
pub mod manifest;
pub mod paper;
pub mod registry;
pub mod runtime;
pub mod store;
pub mod templates;

use std::sync::atomic::{AtomicU64, Ordering};

use tracing::info;

use crate::orchestrator::strategy::Strategy;

/// One adapter per loaded script, for the registry to append.
pub fn adapters() -> Vec<Box<dyn Strategy>> {
    store::names().iter().map(|n| Box::new(adapter::ScriptedStrategy::new(n)) as Box<dyn Strategy>).collect()
}

/// Keeps a running squadron loop's script vipers current without waiting for
/// the loop to be rebuilt.
///
/// A loop builds its viper list once, when it starts on a market, and a Kalshi
/// crypto squadron starts a new one only on the hour; a sports or politics
/// squadron can keep one market for days. So a script saved in between would
/// wait that long to run. Each tick the loop calls [`Attacher::sync`]; when the
/// store has changed since the last look, every script for this squadron's
/// class that the loop does not run yet is appended. Nothing is ever removed:
/// a deleted script's adapter finds no script and holds, and a script whose
/// classes no longer include this squadron's keeps managing what it holds here
/// until the loop is rebuilt, as it would have without this.
///
/// It also records where each script runs, for the Build a Viper page, and
/// forgets that when the loop ends.
pub struct Attacher {
    loop_id: u64,
    squadron: String,
    status_asset: String,
    class: String,
    /// The venue whose scripts attach: this build's, except in tests.
    venue: &'static str,
    seen: Option<u64>,
}

impl Attacher {
    /// Create it before the loop reads its viper list, so a save between the
    /// two is still seen on the first tick.
    pub fn new(squadron: &str, status_asset: &str, class: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let loop_id = NEXT.fetch_add(1, Ordering::Relaxed);
        store::note_loop(loop_id, squadron, class);
        Self {
            loop_id,
            squadron: squadron.to_string(),
            status_asset: status_asset.to_lowercase(),
            class: class.to_string(),
            venue: crate::venues::venue_name(),
            seen: None,
        }
    }

    /// Append any script this loop should run and does not. Returns the
    /// strategy names it appended (empty almost every tick: one atomic load).
    pub fn sync(&mut self, strategies: &mut Vec<Box<dyn Strategy>>) -> Vec<String> {
        let g = store::generation();
        let first = self.seen.is_none();
        if self.seen == Some(g) { return Vec::new(); }
        let mut added = Vec::new();
        for n in store::names_for_class_on(&self.class, self.venue) {
            let sn = store::strategy_name(&n);
            if strategies.iter().any(|s| s.name() == sn) { continue; }
            strategies.push(Box::new(adapter::ScriptedStrategy::new(&n)));
            info!("📜 Script viper '{n}' attached to squadron {} (class '{}') without a rebuild", self.squadron, self.class);
            added.push(sn);
        }
        for s in strategies.iter() {
            let name = s.name();
            if let Some(script) = store::script_of(&name) {
                store::note_attached(self.loop_id, script, &self.squadron, &self.status_asset, &self.class,
                                     !first || added.contains(&name));
            }
        }
        self.seen = Some(g);
        added
    }
}

impl Drop for Attacher {
    fn drop(&mut self) { store::forget_loop(self.loop_id); }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(name: &str, class: &str) -> manifest::Manifest {
        serde_json::from_value(serde_json::json!({
            "name": name, "display_name": "Test", "classes": [class], "max_exposure_usdc": 5,
            "venues": [store::tests::other_venue()]
        })).unwrap()
    }
    const OK: &str = "fn entry(ctx) { () } fn exit(ctx, pos) { () }";

    fn attacher(squadron: &str, class: &str) -> Attacher {
        let mut a = Attacher::new(squadron, "TestAsset", class);
        a.venue = store::tests::other_venue();
        a
    }

    fn names(v: &[Box<dyn Strategy>]) -> Vec<String> { v.iter().map(|s| s.name()).collect() }

    #[test]
    fn a_script_saved_after_the_loop_started_joins_it_on_the_next_tick() {
        let d = std::env::temp_dir().join(format!("dradis-attach-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let mut a = attacher("att-sq-1", "politics");
        let mut strategies: Vec<Box<dyn Strategy>> = Vec::new();
        a.sync(&mut strategies);
        assert!(!names(&strategies).contains(&"Script:att_late".to_string()));

        store::save_in(&d, manifest("att_late", "politics"), OK.into()).unwrap();
        store::save_in(&d, manifest("att_other_class", "sports"), OK.into()).unwrap();
        let added = a.sync(&mut strategies);
        assert!(added.contains(&"Script:att_late".to_string()), "{added:?}");
        assert!(!names(&strategies).contains(&"Script:att_other_class".to_string()), "another class's script stays out");

        // Idempotent: a second save of the same script adds nothing twice.
        store::save_in(&d, manifest("att_late", "politics"), OK.into()).unwrap();
        a.sync(&mut strategies);
        assert_eq!(names(&strategies).iter().filter(|n| *n == "Script:att_late").count(), 1);

        // The page sees it on this squadron, attached after start.
        let on = store::attachments_of("att_late");
        assert!(on.iter().any(|x| x.squadron == "att-sq-1" && x.hot && x.status_asset == "testasset"), "{on:?}");
        assert!(store::running_loops().iter().any(|l| l.squadron == "att-sq-1" && l.class == "politics"));

        // A delete keeps the adapter (it holds), and nothing is removed.
        store::delete_in(&d, "att_late").unwrap();
        a.sync(&mut strategies);
        assert!(names(&strategies).contains(&"Script:att_late".to_string()));

        // The loop ends: its rows go with it.
        drop(a);
        assert!(store::attachments_of("att_late").iter().all(|x| x.squadron != "att-sq-1"));
        assert!(store::running_loops().iter().all(|l| l.squadron != "att-sq-1"));
        let _ = store::delete_in(&d, "att_other_class");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn a_helm_squadron_never_attaches_a_script() {
        let d = std::env::temp_dir().join(format!("dradis-attach-helm-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        store::save_in(&d, manifest("att_helm_probe", "crypto"), OK.into()).unwrap();
        let mut a = attacher("att-helm", crate::vipers::helm_impl::KIND);
        let mut strategies: Vec<Box<dyn Strategy>> = Vec::new();
        a.sync(&mut strategies);
        assert!(strategies.is_empty());
        let _ = store::delete_in(&d, "att_helm_probe");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// An older loop that ends after its successor started must not take the
    /// successor's rows with it.
    #[test]
    fn a_finished_loop_does_not_remove_its_successors_rows() {
        let d = std::env::temp_dir().join(format!("dradis-attach-succ-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        store::save_in(&d, manifest("att_succ", "unknown"), OK.into()).unwrap();
        let mut old = attacher("att-sq-2", "unknown");
        let mut s1: Vec<Box<dyn Strategy>> = Vec::new();
        old.sync(&mut s1);
        let mut new = attacher("att-sq-2", "unknown");
        let mut s2: Vec<Box<dyn Strategy>> = Vec::new();
        new.sync(&mut s2);
        drop(old);
        assert_eq!(store::attachments_of("att_succ").iter().filter(|x| x.squadron == "att-sq-2").count(), 1);
        drop(new);
        let _ = store::delete_in(&d, "att_succ");
        let _ = std::fs::remove_dir_all(&d);
    }
}
