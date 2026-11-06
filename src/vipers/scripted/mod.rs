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
pub mod context;
pub mod manifest;
pub mod paper;
pub mod runtime;
pub mod store;
pub mod templates;

use crate::orchestrator::strategy::Strategy;

/// One adapter per loaded script, for the registry to append.
pub fn adapters() -> Vec<Box<dyn Strategy>> {
    store::names().iter().map(|n| Box::new(adapter::ScriptedStrategy::new(n)) as Box<dyn Strategy>).collect()
}
