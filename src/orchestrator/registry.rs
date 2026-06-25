use crate::orchestrator::Strategy;
use crate::vipers::momentum_impl::MomentumStrategyImpl;
use crate::vipers::arbitrage_impl::ArbitrageStrategyImpl;
use crate::vipers::time_decay_impl::TimeDecayStrategyImpl;
use crate::vipers::maker_impl::MakerStrategyImpl;
use crate::vipers::basis_impl::BasisStrategyImpl;
use crate::vipers::gboost_impl::GboostStrategyImpl;
use crate::vipers::trendcapture_impl::TrendCaptureStrategyImpl;
use crate::vipers::convergence_impl::ConvergenceStrategyImpl;
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
            "   TrendCapture| reversal_pct={} (×oracle) | min_entry=${} late=${} | max_ask_sum={} | late_SL={}% | tp_ceiling=${}",
            config::TRENDCAPTURE_REVERSAL_DRIFT_PCT, config::TRENDCAPTURE_MIN_ENTRY_PRICE,
            config::TRENDCAPTURE_LATE_MARKET_MIN_ENTRY_PRICE, config::TRENDCAPTURE_MAX_ENTRY_ASK_SUM,
            config::TRENDCAPTURE_LATE_MARKET_STOP_LOSS_PERCENT * dec!(100),
            config::TRENDCAPTURE_TAKE_PROFIT_CEILING,
        );
        info!(
            "   Convergence | pulse_thr={} coh_min={} cvd_margin={} | size=${} max_exp=${} | TP={}% SL={}% (BTC-only, live)",
            config::CONVERGENCE_PULSE_THRESHOLD, config::CONVERGENCE_COHERENCE_MIN,
            config::CONVERGENCE_CVD_CONFIRM_MARGIN, config::CONVERGENCE_POSITION_SIZE_USDC,
            config::CONVERGENCE_MAX_EXPOSURE_USDC,
            config::CONVERGENCE_TARGET_PROFIT_PERCENT * dec!(100),
            config::CONVERGENCE_STOP_LOSS_PERCENT * dec!(100),
        );
    }

    /// Create a vector of ALL strategy instances.
    /// Every strategy is always instantiated so the DynamicConfig hot-patch can
    /// enable or disable any of them during a running session via the Control Tower UI.
    pub fn create_all_strategies() -> Vec<Box<dyn Strategy>> {
        Self::log_active_thresholds();
        vec![
            Box::new(MomentumStrategyImpl::new())          as Box<dyn Strategy>,
            Box::new(ArbitrageStrategyImpl)                as Box<dyn Strategy>,
            Box::new(TimeDecayStrategyImpl)                as Box<dyn Strategy>,
            Box::new(MakerStrategyImpl::new())             as Box<dyn Strategy>,
            Box::new(BasisStrategyImpl)                    as Box<dyn Strategy>,
            Box::new(GboostStrategyImpl::default())        as Box<dyn Strategy>,
            Box::new(TrendCaptureStrategyImpl::new())      as Box<dyn Strategy>,
            Box::new(ConvergenceStrategyImpl::new())       as Box<dyn Strategy>,
        ]
    }

    /// Create only momentum strategy
    pub fn create_momentum() -> Box<dyn Strategy> {
        Box::new(MomentumStrategyImpl::new())
    }

    /// Create only arbitrage strategy
    pub fn create_arbitrage() -> Box<dyn Strategy> {
        Box::new(ArbitrageStrategyImpl)
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
            "TrendCaptureStrategy",
            "ConvergenceStrategy",
        ]
        .into_iter().map(|s| s.to_string()).collect()
    }

    /// Returns the priority of a strategy (lower number = higher priority).
    /// Returns None if the strategy name is not found.
    pub fn get_strategy_priority(strategy_name: &str) -> Option<usize> {
        Self::strategy_names().iter().position(|s| s == strategy_name)
    }
}
