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

use crate::orchestrator::Strategy;
use crate::vipers::momentum_impl::MomentumStrategyImpl;
use crate::vipers::arbitrage_impl::ArbitrageStrategyImpl;
use crate::vipers::time_decay_impl::TimeDecayStrategyImpl;
use crate::vipers::maker_impl::MakerStrategyImpl;
use crate::vipers::basis_impl::BasisStrategyImpl;
use crate::vipers::gboost_impl::GboostStrategyImpl;
use crate::vipers::trendreversal_impl::TrendReversalStrategyImpl;
use crate::vipers::convergence_impl::ConvergenceStrategyImpl;
use crate::vipers::fairvalue_impl::FairValueStrategyImpl;
use crate::config;
use rust_decimal_macros::dec;
use tracing::info;

/// Registry for all available strategies
pub struct StrategyRegistry;

impl StrategyRegistry {
    /// Boot-time banner: print the active (compiled-in) per-viper thresholds so every
    /// session.log self-documents exactly which params the running binary holds —
    /// removing any ambiguity between "edited in source" and "live in prod".
    ///
    /// Reversal/threshold pct values that are oracle-relative (pct × oracle_price) are
    /// printed as the raw pct; their absolute dollar value is price-dependent at runtime.
    /// Gboost entry threshold is the STARTUP DEFAULT only — the live value may be
    /// overridden via DynamicConfig (PATCH /api/config) and persisted in SQLite.
    fn log_active_thresholds() {
        info!("🧭 Active viper thresholds (compiled-in defaults):");
        info!(
            "   Momentum    | threshold_pct={} (×oracle) | min_entry=${} | TP={}% SL={}% | max_ask_sum={}",
            config::MOMENTUM_THRESHOLD_PCT, config::MOMENTUM_MIN_ENTRY_PRICE,
            config::MOMENTUM_TARGET_PROFIT_PERCENT * dec!(100),
            config::MOMENTUM_STOP_LOSS_PERCENT * dec!(100),
            config::MOMENTUM_MAX_ENTRY_ASK_SUM,
        );
        info!(
            "   Arbitrage   | profit_thr={} | max_leg_price=${} | max_fill_gap=${} | max_leg_obi={}",
            config::ARBITRAGE_PROFIT_THRESHOLD, config::ARBITRAGE_MAX_LEG_PRICE,
            config::ARBITRAGE_MAX_FILL_GAP, config::ARBITRAGE_MAX_LEG_OBI,
        );
        info!(
            "   Maker       | min_spread=${} | entry=[${}..${}] | TP={}% SL={}%",
            config::MAKER_MIN_SPREAD, config::MAKER_MIN_ENTRY_PRICE, config::MAKER_MAX_ENTRY_PRICE,
            config::MAKER_TARGET_PROFIT_PERCENT * dec!(100),
            config::MAKER_STOP_LOSS_PERCENT * dec!(100),
        );
        info!(
            "   Basis       | skew_thr={} | max_entry=${} | TP={}% SL={}%",
            config::BASIS_ENTRY_SKEW_THRESHOLD, config::BASIS_MAX_ENTRY_PRICE,
            config::BASIS_TARGET_PROFIT_PERCENT * dec!(100),
            config::BASIS_STOP_LOSS_PERCENT * dec!(100),
        );
        info!(
            "   Gboost      | entry_thr={} (startup default) | min_edge={} | TP={}% SL={}%",
            config::GBOOST_ENTRY_THRESHOLD, config::GBOOST_MIN_EDGE_FROM_FAIR,
            config::GBOOST_TARGET_PROFIT_PERCENT * dec!(100),
            config::GBOOST_STOP_LOSS_PERCENT * dec!(100),
        );
        info!(
            "   {}| mode={} exhaust_mult={} | TP={}% SL={}% (fade) | min_entry=${} late=${} | max_ask_sum={}",
            if config::TRENDREVERSAL_MODE { "TrendReversal" } else { "TrendCapture " },
            if config::TRENDREVERSAL_MODE { "FADE drift" } else { "FOLLOW drift" },
            config::TRENDREVERSAL_EXHAUSTION_MULT,
            config::TRENDREVERSAL_TARGET_PROFIT_PCT * dec!(100),
            config::TRENDREVERSAL_STOP_LOSS_PCT * dec!(100),
            config::TRENDCAPTURE_MIN_ENTRY_PRICE,
            config::TRENDCAPTURE_LATE_MARKET_MIN_ENTRY_PRICE, config::TRENDCAPTURE_MAX_ENTRY_ASK_SUM,
        );
        info!(
            "   Convergence | pulse_thr={} coh_min={} cvd_margin={} | size=${} max_exp=${} | TP={}% SL={}% (BTC-only, live)",
            config::CONVERGENCE_PULSE_THRESHOLD, config::CONVERGENCE_COHERENCE_MIN,
            config::CONVERGENCE_CVD_CONFIRM_MARGIN, config::CONVERGENCE_POSITION_SIZE_USDC,
            config::CONVERGENCE_MAX_EXPOSURE_USDC,
            config::CONVERGENCE_TARGET_PROFIT_PERCENT * dec!(100),
            config::CONVERGENCE_STOP_LOSS_PERCENT * dec!(100),
        );
        info!(
            "   FairValue   | base_edge={} min_edge={} taper={}s | pin={}σ/{}s | size=${} max_exp=${} | TP={}% SL={}%",
            config::FAIRVALUE_BASE_EDGE, config::FAIRVALUE_MIN_EDGE, config::FAIRVALUE_EDGE_TAPER_SECS,
            config::FAIRVALUE_PIN_MIN_SIGMA, config::FAIRVALUE_PIN_GUARD_SECS,
            config::FAIRVALUE_TRADE_SIZE_USDC, config::FAIRVALUE_MAX_EXPOSURE_USDC,
            config::FAIRVALUE_TARGET_PROFIT_PERCENT * dec!(100),
            config::FAIRVALUE_STOP_LOSS_PERCENT * dec!(100),
        );
    }

    /// Create a vector of ALL strategy instances.
    /// Every strategy is always instantiated so the DynamicConfig hot-patch can
    /// enable or disable any of them during a running session via the Control Tower UI.
    pub fn create_all_strategies() -> Vec<Box<dyn Strategy>> {
        // Once per process, despite its name promising as much. Venues that
        // rediscover their primary market on a timer rebuild the squadron each
        // cycle — the Kalshi trader does so every two minutes — and this banner
        // is 24 lines. In an 8-minute QA capture it was 40% of the log, burying
        // the gate decisions an operator is actually reading for.
        static BANNER: std::sync::Once = std::sync::Once::new();
        BANNER.call_once(Self::log_active_thresholds);
        vec![
            Box::new(MomentumStrategyImpl::new())          as Box<dyn Strategy>,
            Box::new(ArbitrageStrategyImpl::default())                as Box<dyn Strategy>,
            Box::new(TimeDecayStrategyImpl)                as Box<dyn Strategy>,
            Box::new(MakerStrategyImpl::new())             as Box<dyn Strategy>,
            Box::new(BasisStrategyImpl::new())             as Box<dyn Strategy>,
            Box::new(GboostStrategyImpl::default())        as Box<dyn Strategy>,
            Box::new(TrendReversalStrategyImpl::new())      as Box<dyn Strategy>,
            Box::new(ConvergenceStrategyImpl::new())       as Box<dyn Strategy>,
            Box::new(FairValueStrategyImpl::new())         as Box<dyn Strategy>,
        ]
    }

    /// Instantiate exactly the strategies meaningful for a market class.
    ///
    /// `kinds` comes from `db::vipers_for_class`, the taxonomy that says a
    /// politics market gets Arbitrage and Maker while a crypto market gets all
    /// nine. Filtering here is what makes that taxonomy load-bearing rather than
    /// descriptive.
    ///
    /// An empty `kinds` yields no strategies. That is deliberate and matches the
    /// venue traders, which warn and run dashboard-only: a class with no vipers
    /// configured should sit out, not silently fall back to all nine.
    pub fn create_strategies_for_kinds(kinds: &[String]) -> Vec<Box<dyn Strategy>> {
        Self::create_all_strategies()
            .into_iter()
            .filter(|s| kinds.iter().any(|k| k == strategy_name_to_kind(&s.name())))
            .collect()
    }

    /// Create only momentum strategy
    pub fn create_momentum() -> Box<dyn Strategy> {
        Box::new(MomentumStrategyImpl::new())
    }

    /// Create only arbitrage strategy
    pub fn create_arbitrage() -> Box<dyn Strategy> {
        Box::new(ArbitrageStrategyImpl::default())
    }

    /// Create only time decay strategy
    pub fn create_time_decay() -> Box<dyn Strategy> {
        Box::new(TimeDecayStrategyImpl)
    }

    /// Create only maker strategy
    pub fn create_maker() -> Box<dyn Strategy> {
        Box::new(MakerStrategyImpl::new())
    }

    /// Return the names of all strategies, in priority order for orphan adoption.
    /// All strategies are always registered — DynamicConfig controls whether they trade.
    pub fn strategy_names() -> Vec<String> {
        vec![
            "MomentumStrategy",
            "ArbitrageStrategy",
            "TimeDecayStrategy",
            "MakerStrategy",
            "BasisStrategy",
            "GboostStrategy",
            "TrendReversalStrategy",
            "ConvergenceStrategy",
            "FairValueStrategy",
        ]
        .into_iter().map(|s| s.to_string()).collect()
    }

    /// Returns the priority of a strategy (lower number = higher priority).
    /// Returns None if the strategy name is not found.
    pub fn get_strategy_priority(strategy_name: &str) -> Option<usize> {
        Self::strategy_names().iter().position(|s| s == strategy_name)
    }
}

/// Map a registry strategy name (`"ArbitrageStrategy"`) to its taxonomy viper
/// kind id (`"arbitrage"`) so resolved kinds can select strategy impls.
///
/// Lives here rather than beside each venue loop because all three venues need
/// the same mapping, and two of them had grown byte-identical private copies
/// while the third had none — which is precisely why the intl CLOB ran all nine
/// vipers on markets its own taxonomy limited to two.
pub fn strategy_name_to_kind(name: &str) -> &'static str {
    match name {
        "ArbitrageStrategy"     => "arbitrage",
        "MakerStrategy"         => "maker",
        "MomentumStrategy"      => "momentum",
        "TimeDecayStrategy"     => "time_decay",
        "BasisStrategy"         => "basis",
        "GboostStrategy"        => "gboost",
        "ConvergenceStrategy"   => "convergence",
        "FairValueStrategy"     => "fairvalue",
        "TrendReversalStrategy" => "trendcapture",
        "TrendCaptureStrategy"  => "trendcapture", // legacy alias (pre-rename positions)
        _ => "",
    }
}

#[cfg(test)]
mod class_filter_tests {
    use super::*;

    // These build real strategy impls, and `GboostStrategyImpl`'s constructor
    // spawns its model-load task, so they need a runtime to exist.

    fn kinds(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// Every strategy the registry builds must map to a non-empty kind, or it
    /// would be silently unselectable for every market class.
    #[tokio::test]
    async fn every_registry_strategy_maps_to_a_kind() {
        for s in StrategyRegistry::create_all_strategies() {
            assert_ne!(
                strategy_name_to_kind(&s.name()), "",
                "{} has no taxonomy kind — it can never be selected", s.name(),
            );
        }
    }

    /// The case from production: a politics squadron is limited to two vipers,
    /// and must not instantiate the seven crypto ones.
    #[tokio::test]
    async fn politics_class_gets_only_its_two_vipers() {
        let got = StrategyRegistry::create_strategies_for_kinds(&kinds(&["arbitrage", "maker"]));
        let mut names: Vec<_> = got.iter().map(|s| s.name()).collect();
        names.sort();
        assert_eq!(names, vec!["ArbitrageStrategy", "MakerStrategy"]);
    }

    #[tokio::test]
    async fn crypto_class_gets_all_nine() {
        let all = StrategyRegistry::create_all_strategies();
        let kinds: Vec<String> = all
            .iter()
            .map(|s| strategy_name_to_kind(&s.name()).to_string())
            .collect();
        assert_eq!(StrategyRegistry::create_strategies_for_kinds(&kinds).len(), all.len());
    }

    #[tokio::test]
    async fn empty_class_runs_nothing_rather_than_everything() {
        assert!(StrategyRegistry::create_strategies_for_kinds(&[]).is_empty());
    }

    /// The pre-rename position label still selects the renamed impl.
    #[test]
    fn legacy_trendcapture_alias_still_resolves() {
        assert_eq!(strategy_name_to_kind("TrendCaptureStrategy"), "trendcapture");
        assert_eq!(strategy_name_to_kind("TrendReversalStrategy"), "trendcapture");
    }
}
