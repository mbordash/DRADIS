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

/// Orchestrator module: manages strategy lifecycle, market data distribution, and coordination.
///
/// The orchestrator acts as the central hub for:
/// - Strategy registration and instantiation
/// - Market data broadcasting to all strategies
/// - Signal collection and execution
/// - Position/order coordination between strategies

pub mod market_data;
pub mod strategy;
pub mod registry;
pub mod executor;

pub use market_data::MarketDataBroadcaster;
pub use strategy::{Strategy, StrategyContext};
pub use registry::StrategyRegistry;
pub use executor::{
    evaluate_strategies,
    prioritize_signals,
    StrategyEvaluationResult,
    execute_strategies_concurrent,
    aggregate_and_resolve_signals,
    SignalConflictInfo,
};
