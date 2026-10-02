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
//! **What exists so far.** The strategy registers and resolves (increment 1),
//! and reads the squadron's intents (`helpers::helm`, increment 2) so the
//! status registry can say where the operator's conviction stands. **It does
//! not act on them.** Both evaluations return `NoSignal` whatever the intent
//! says; the entry an acknowledged intent produces and the exit posture the
//! engine enforces are the next increment. Until then there is no path from a
//! Helm squadron to an order.

use crate::helpers::helm::{self, IntentStatus, IntentTally};
use crate::orchestrator::strategy::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus};
use anyhow::Result;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Registry name, as it appears in every `PositionKey`, `trades` row and log
/// line for a Helm position.
pub const STRATEGY_NAME: &str = "HelmStrategy";

/// Taxonomy id. It is at once the viper kind (`viper_kind.id`), the market
/// class a Helm squadron resolves to (`market_class.id`), and the asset a Helm
/// squadron is deployed under (`CryptoAsset::Custom("helm")`). One string for
/// all three is deliberate: the class exists only to carry this viper, and the
/// asset exists only to declare the class.
pub const KIND: &str = "helm";

/// Refusal reasons the status registry shows. Stable text: the Control Tower
/// groups refusals by reason, so these carry the status and not the id.
pub const AWAITING_INTENT: &str = "awaiting operator intent";
pub const INTENT_PROPOSED: &str = "intent proposed, awaiting acknowledgement";
pub const INTENT_ACKNOWLEDGED: &str = "intent acknowledged, entry not yet implemented";
pub const INTENT_IN_FLIGHT: &str = "intent in flight, exit posture not yet implemented";
pub const INTENTS_COMPLETE: &str = "all intents terminal, squadron retiring";

/// How often the strategy re-reads its squadron's intents. The patrol ticks
/// every 50 ms; the operator edits intents on a human cadence.
const INTENT_REFRESH: Duration = Duration::from_secs(2);

/// What the strategy knows about its squadron's intents, refreshed on a timer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct IntentView {
    pub tally: IntentTally,
    /// The most advanced open status, if any intent is open.
    pub leading_open: Option<IntentStatus>,
}

impl IntentView {
    /// The reason to report for this view. Pure, so it is testable without a
    /// database; the mapping from status to text is the whole of it.
    pub(crate) fn reason(self) -> &'static str {
        match self.leading_open {
            None if self.tally.total == 0 => AWAITING_INTENT,
            None => INTENTS_COMPLETE,
            Some(IntentStatus::Proposed) => INTENT_PROPOSED,
            Some(IntentStatus::Acknowledged) => INTENT_ACKNOWLEDGED,
            Some(_) => INTENT_IN_FLIGHT,
        }
    }

    /// Rank open statuses so "most advanced" is well defined.
    fn rank(s: IntentStatus) -> u8 {
        match s {
            IntentStatus::Proposed => 1,
            IntentStatus::Acknowledged => 2,
            IntentStatus::Working => 3,
            IntentStatus::Missed => 4,
            IntentStatus::Partial => 5,
            IntentStatus::Filled => 6,
            IntentStatus::Closed | IntentStatus::Superseded => 0,
        }
    }

    pub(crate) fn from_intents(intents: &[helm::HelmIntent]) -> Self {
        let total = intents.len() as i64;
        let open: Vec<IntentStatus> = intents.iter().map(|i| i.status).filter(|s| !s.is_terminal()).collect();
        Self {
            tally: IntentTally { total, open: open.len() as i64 },
            leading_open: open.into_iter().max_by_key(|s| Self::rank(*s)),
        }
    }
}

/// The operator's viper. Holds nothing but a short cache of what the database
/// says about its squadron's intents.
pub struct HelmStrategy {
    view: Mutex<Option<(Instant, IntentView)>>,
}

impl HelmStrategy {
    pub fn new() -> Self {
        Self { view: Mutex::new(None) }
    }

    /// The squadron's intents, read from the pool its asset aliases to, no
    /// more often than `INTENT_REFRESH`. A squadron with no pool (tests, a
    /// venue path without one) reads as having no intents.
    async fn view(&self, ctx: &StrategyContext) -> IntentView {
        if let Ok(g) = self.view.lock() {
            if let Some((at, v)) = *g {
                if at.elapsed() < INTENT_REFRESH {
                    return v;
                }
            }
        }
        let fresh = match crate::helpers::db::pool_for(&ctx.crypto_filter) {
            Some(pool) => IntentView::from_intents(&helm::list_for_squadron(&pool, &ctx.squadron_id, true).await),
            None => IntentView::default(),
        };
        if let Ok(mut g) = self.view.lock() {
            *g = Some((Instant::now(), fresh));
        }
        fresh
    }
}

impl Default for HelmStrategy {
    fn default() -> Self { Self::new() }
}

#[async_trait::async_trait]
impl Strategy for HelmStrategy {
    /// Reads the intent, reports where it stands, emits nothing. The entry an
    /// acknowledged intent produces is the next increment.
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let view = self.view(ctx).await;
        crate::helpers::viper_status::report_reason(&ctx.crypto_filter, STRATEGY_NAME, view.reason());
        Ok(StrategySignal::NoSignal)
    }

    /// No position can exist yet, so there is nothing to exit. The posture
    /// that will manage one is the next increment.
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

    /// No money path: a live book, collateral and an open window produce no
    /// signal from either evaluation, with or without an intent.
    #[tokio::test]
    async fn neither_evaluation_emits_a_signal() {
        let s = HelmStrategy::new();
        let c = ctx();
        assert!(matches!(s.evaluate_entry(&c).await.unwrap(), StrategySignal::NoSignal));
        assert!(matches!(s.evaluate_exit(&c).await.unwrap(), StrategySignal::NoSignal));
    }

    /// The same, with an acknowledged intent in the cache: the strategy may
    /// know the operator is ready and still must not act. This is the test
    /// the entry increment will have to change deliberately.
    #[tokio::test]
    async fn an_acknowledged_intent_still_produces_no_signal() {
        let s = HelmStrategy::new();
        *s.view.lock().unwrap() = Some((Instant::now(), IntentView {
            tally: IntentTally { total: 1, open: 1 },
            leading_open: Some(IntentStatus::Acknowledged),
        }));
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

    /// Each view maps to one stable reason string, and the most advanced open
    /// intent is the one reported.
    #[test]
    fn the_reported_reason_follows_the_intents() {
        let v = |total, open, leading| IntentView { tally: IntentTally { total, open }, leading_open: leading };
        assert_eq!(v(0, 0, None).reason(), AWAITING_INTENT);
        assert_eq!(v(2, 0, None).reason(), INTENTS_COMPLETE);
        assert_eq!(v(1, 1, Some(IntentStatus::Proposed)).reason(), INTENT_PROPOSED);
        assert_eq!(v(1, 1, Some(IntentStatus::Acknowledged)).reason(), INTENT_ACKNOWLEDGED);
        assert_eq!(v(1, 1, Some(IntentStatus::Working)).reason(), INTENT_IN_FLIGHT);
        assert_eq!(v(1, 1, Some(IntentStatus::Filled)).reason(), INTENT_IN_FLIGHT);
    }

    #[test]
    fn the_view_picks_the_most_advanced_open_intent() {
        use crate::helpers::helm::{EntryKind, HelmIntent, Horizon, IntentContent};
        let mk = |id, status| HelmIntent {
            id, squadron_id: "s".into(), market_id: "".into(), market_name: "".into(), side: "YES".into(),
            first: IntentContent {
                thesis: "t".into(), confidence: 0.6, horizon: Horizon::Expiry, falsification: "f".into(),
                entry_kind: EntryKind::Taker, entry_limit_price: None, size_usdc: dec!(1),
                stop_price: None, take_profit_price: None, time_limit_at: None,
                hold_to_settlement: false, catastrophic_floor_pct: None,
            },
            current_version: 1, status, status_detail: None, critique: None, critique_at: None,
            critique_model: None, superseded_by: None, close_reason: None, ghost: true,
            venue: "".into(), session_id: "".into(), created_at: "".into(), acknowledged_at: None,
            updated_at: "".into(), closed_at: None,
        };
        let v = IntentView::from_intents(&[
            mk(1, IntentStatus::Closed),
            mk(2, IntentStatus::Proposed),
            mk(3, IntentStatus::Acknowledged),
        ]);
        assert_eq!(v.tally, IntentTally { total: 3, open: 2 });
        assert_eq!(v.leading_open, Some(IntentStatus::Acknowledged));
        assert_eq!(v.reason(), INTENT_ACKNOWLEDGED);

        let done = IntentView::from_intents(&[mk(1, IntentStatus::Closed), mk(2, IntentStatus::Superseded)]);
        assert!(done.tally.complete());
        assert_eq!(done.reason(), INTENTS_COMPLETE);
    }
}
