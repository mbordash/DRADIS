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

//! Helm: the operator's own position, with the engine holding the exit.
//!
//! A Helm position is a single-member squadron. The operator deploys a squadron
//! onto one market with the class `helm`, and that class carries exactly one
//! viper — this one. Every other viper is absent rather than switched off, so a
//! Helm squadron cannot quote, arb or model the operator's market behind their
//! back. The squadron supplies what a position needs and nothing else does: a
//! book tick, a `squadron_id` for its `PositionKey`, persistence, chain
//! reconciliation, the lifecycle FSM and a Control Tower page.
//!
//! **This file is the foundation only.** The strategy registers, resolves, and
//! declines to act: both evaluations return `NoSignal` and say why. The intent
//! model (thesis, confidence, horizon, falsification condition), the entry it
//! produces, the structured exit posture the engine enforces, and the
//! squadron's retirement when the intent is complete arrive in later
//! increments. Until then there is no path from a Helm squadron to an order.

use crate::orchestrator::strategy::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus};
use anyhow::Result;

/// Registry name, as it appears in every `PositionKey`, `trades` row and log
/// line for a Helm position.
pub const STRATEGY_NAME: &str = "HelmStrategy";

/// Taxonomy id. It is at once the viper kind (`viper_kind.id`), the market
/// class a Helm squadron resolves to (`market_class.id`), and the asset a Helm
/// squadron is deployed under (`CryptoAsset::Custom("helm")`). One string for
/// all three is deliberate: the class exists only to carry this viper, and the
/// asset exists only to declare the class.
pub const KIND: &str = "helm";

/// The refusal the status registry shows while no intent exists. Stable text:
/// the Control Tower groups refusals by reason.
pub const AWAITING_INTENT: &str = "awaiting operator intent";

/// The operator's viper. Stateless until the intent model exists.
pub struct HelmStrategy;

impl HelmStrategy {
    pub fn new() -> Self { Self }
}

impl Default for HelmStrategy {
    fn default() -> Self { Self::new() }
}

#[async_trait::async_trait]
impl Strategy for HelmStrategy {
    /// No intent model yet, so no entry. The refusal is recorded so the
    /// squadron page reads "awaiting operator intent" rather than nothing.
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        crate::helpers::viper_status::report_reason(&ctx.crypto_filter, STRATEGY_NAME, AWAITING_INTENT);
        Ok(StrategySignal::NoSignal)
    }

    /// No position can exist yet, so there is nothing to exit. The posture
    /// that will manage one is a later increment.
    async fn evaluate_exit(&self, _ctx: &StrategyContext) -> Result<StrategySignal> {
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { STRATEGY_NAME.to_string() }
    fn venue(&self) -> &'static str { "Single market" }
    fn risk_model(&self) -> &'static str { "Operator intent; engine-enforced exit posture" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{MarketConfig, MarketSnapshot, PositionMap};
    use crate::venues::core::MarketId;
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn ctx() -> StrategyContext {
        StrategyContext {
            market_class: Some(KIND.to_string()),
            squadron_id: "helm-open-trial".to_string(),
            market: MarketConfig {
                yes_token: MarketId::new("helm-yes"), no_token: MarketId::new("helm-no"),
                market_name: "Bitcoin Up or Down - October 2, 3PM ET".to_string(),
                market_close_time: Some(Utc::now() + chrono::Duration::minutes(50)),
                strike_price: None, is_neg_risk: false,
                condition_id: "0xhelm".to_string(), yes_fee_bps: 0, no_fee_bps: 0,
            },
            snapshot: MarketSnapshot {
                yes_bid: dec!(0.58), yes_bid_depth: dec!(200),
                yes_ask: dec!(0.60), yes_ask_depth: dec!(150),
                no_bid: dec!(0.40), no_bid_depth: dec!(180),
                no_ask: dec!(0.42), no_ask_depth: dec!(160),
                yes_bid_depth_total: dec!(1200), yes_ask_depth_total: dec!(900),
                no_bid_depth_total: dec!(1100), no_ask_depth_total: dec!(950),
                oracle_price: dec!(0),
                velocity: dec!(0), velocity_1s: dec!(0), acceleration: dec!(0),
                funding_rate: dec!(0), oracle_drift_60m: dec!(0),
                oracle_drift_10m: dec!(0), hist_vol: dec!(0),
                institutional_pulse: dec!(0), tide_coherence: dec!(0),
                tradfi_velocity: dec!(0), macro_coherence: dec!(0),
                vix_proxy: dec!(0), vix_velocity: dec!(0),
                oi_delta_pct: dec!(0), cvd_ratio: dec!(1),
                secs_to_expiry: 3000, timestamp: Utc::now(),
            },
            positions: Arc::new(Mutex::new(PositionMap::new())),
            session_pnl: dec!(0), starting_collateral: dec!(100),
            available_collateral: dec!(100),
            crypto_filter: KIND.to_string(),
            market_started_at: Utc::now(),
            maker_market: None, maker_snapshot: None,
            dynamic_config: Arc::new(crate::helpers::dynamic_config::DynamicConfig::default()),
            arb_market_lockouts: None,
            sports: None,
        }
    }

    /// The foundation increment has no money path: a live book, collateral and
    /// an open window produce no signal from either evaluation.
    #[tokio::test]
    async fn neither_evaluation_emits_a_signal_without_an_intent() {
        let s = HelmStrategy::new();
        let c = ctx();
        assert!(matches!(s.evaluate_entry(&c).await.unwrap(), StrategySignal::NoSignal));
        assert!(matches!(s.evaluate_exit(&c).await.unwrap(), StrategySignal::NoSignal));
    }

    /// The registry name is what the taxonomy maps, so the two constants must
    /// agree with the mapping the registry publishes.
    #[test]
    fn name_and_kind_agree_with_the_registry() {
        assert_eq!(HelmStrategy::new().name(), STRATEGY_NAME);
        assert_eq!(crate::orchestrator::registry::strategy_name_to_kind(STRATEGY_NAME), KIND);
    }
}
