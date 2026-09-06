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

/// Background task modules.
///
/// Each module exposes a single `pub async fn run_*` entry point that is
/// `tokio::spawn`-ed from `main.rs`.  All long-running loops, shared-state
/// mutations, and side-effecting async work live here — keeping `main.rs`
/// as pure orchestration/wiring.
///
/// Note: Binance price and funding rate tasks have moved to `crate::raptors`
/// as part of the Raptor recon-layer separation of concerns.
#[cfg(feature = "intl_clob")]
pub mod market_monitor;
#[cfg(feature = "intl_clob")]
pub mod cleanup;
#[cfg(feature = "intl_clob")]
pub mod collateral_sweep;
