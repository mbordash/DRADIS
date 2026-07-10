//! Control Tower config schema registry.
//!
//! Single source of truth describing every editable `DynamicConfig` field, so the
//! Control Tower can render the Basic panels **and** a dynamic "Advanced" modal from
//! one place — with no hand-maintained frontend field list to drift out of sync.
//!
//! Roadmap: "Schema-driven Advanced modal (Option B)". Served at
//! `GET /api/config/schema`. The frontend groups by `group`, shows `advanced=false`
//! fields in the Basic panel (as today) and `advanced=true` fields in the modal,
//! using `value_type` + `min`/`max`/`step` for input rendering and clamping.
//!
//! NOTE: `key` MUST match the serde field name in `DynamicConfig` (snake_case) —
//! that is exactly what `PATCH /api/squadrons/{id}/config` merges. Keep this registry
//! in lock-step with `helpers::dynamic_config::DynamicConfig`; a future improvement is
//! to derive it via a proc-macro so it can never drift.

use serde::Serialize;

/// Metadata for one editable config field.
#[derive(Debug, Clone, Serialize)]
pub struct ConfigFieldSchema {
    /// Serde key in the `DynamicConfig` JSON (snake_case) — what PATCH expects.
    pub key: &'static str,
    /// Display group: a viper name or "Global".
    pub group: &'static str,
    /// The viper enable flag this field belongs to (`None` for global fields).
    pub enable_key: Option<&'static str>,
    /// Human label for the input.
    pub label: &'static str,
    /// Render/validation hint: `usd` | `price` | `pct` | `decimal` | `secs` | `bool`.
    #[serde(rename = "type")]
    pub value_type: &'static str,
    /// Optional unit suffix for display (e.g. "s", "USDC").
    pub unit: Option<&'static str>,
    /// Optional inclusive lower clamp for numeric inputs.
    pub min: Option<f64>,
    /// Optional inclusive upper clamp for numeric inputs.
    pub max: Option<f64>,
    /// Optional input step.
    pub step: Option<f64>,
    /// `false` → Basic panel (shown today); `true` → Advanced modal.
    pub advanced: bool,
    /// Short tooltip describing what the field does.
    pub description: &'static str,
}

impl ConfigFieldSchema {
    fn new(
        group: &'static str,
        enable_key: Option<&'static str>,
        key: &'static str,
        label: &'static str,
        value_type: &'static str,
        advanced: bool,
        description: &'static str,
    ) -> Self {
        Self {
            key, group, enable_key, label, value_type,
            unit: None, min: None, max: None, step: None,
            advanced, description,
        }
    }
    fn range(mut self, min: f64, max: f64) -> Self { self.min = Some(min); self.max = Some(max); self }
    fn min(mut self, min: f64) -> Self { self.min = Some(min); self }
    fn step(mut self, step: f64) -> Self { self.step = Some(step); self }
    fn unit(mut self, unit: &'static str) -> Self { self.unit = Some(unit); self }
}

/// Build the full editable-config schema.
///
/// Ordering is UI-friendly: Global first, then each viper with its Basic fields
/// before its Advanced fields.
pub fn config_schema() -> Vec<ConfigFieldSchema> {
    use ConfigFieldSchema as F;
    let mut v: Vec<ConfigFieldSchema> = Vec::new();

    // ── Global ────────────────────────────────────────────────────────────────
    v.push(F::new("Global", None, "ghost_mode", "Ghost Mode", "bool", false,
        "Simulate all orders — no real CLOB calls (validation framework)."));

    // ── Arbitrage ───────────────────────────────────────────────────────────────
    {
        let g = "Arbitrage"; let e = Some("enable_arbitrage");
        v.push(F::new(g, e, "enable_arbitrage", "Enabled", "bool", false,
            "Hedged maker bids on YES+NO — captures mispriced spread at 0% fee."));
        v.push(F::new(g, e, "arbitrage_position_size_usdc", "Position Size", "usd", false,
            "USDC deployed per arb pair (each leg).").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "arbitrage_max_exposure_usdc", "Max Exposure", "usd", false,
            "Hard cap on total arb capital at risk.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "arbitrage_profit_threshold", "Min Profit/Share", "price", false,
            "Minimum (1.00 − yes_bid − no_bid) edge required to enter.").range(0.0, 0.5).step(0.005));
        v.push(F::new(g, e, "arb_fak_rehedge_buffer", "Re-hedge Buffer", "price", false,
            "Breakeven cushion when FAK re-hedging a naked leg (taker fee + slippage).").range(0.0, 0.2).step(0.005));
        v.push(F::new(g, e, "arb_max_rescue_cost", "Max Rescue Cost", "price", false,
            "Block entry if a single-leg orphan can't be rescued below this cost.").range(1.0, 1.2).step(0.01));
        // Advanced
        v.push(F::new(g, e, "arbitrage_max_fill_gap", "Max Fill Gap", "price", true,
            "Skip if (ask − safe_bid) on either leg exceeds this — prevents one-sided fills.").range(0.0, 0.2).step(0.005));
        v.push(F::new(g, e, "arbitrage_max_leg_price", "Max Leg Price (legacy)", "price", true,
            "Legacy hard price cap per leg; used only when orderbook depth is unavailable.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "arbitrage_max_leg_obi", "Max Leg OBI", "decimal", true,
            "Max order-book imbalance on either leg before skipping (fill-asymmetry guard).").range(0.0, 1.0).step(0.05));
        v.push(F::new(g, e, "arbitrage_max_obi_asymmetry", "Max OBI Asymmetry", "decimal", true,
            "Max |YES_OBI − NO_OBI| before skipping — blocks lopsided books that orphan a leg.").range(0.0, 1.0).step(0.05));
        v.push(F::new(g, e, "arbitrage_min_leg_conviction", "Min Leg Conviction", "price", false,
            "Dominant leg bid must be ≥ this to enter — restricts arb to deep near-settlement markets and rejects ≈0.50 coin-flips (core orphan guard).").range(0.5, 1.0).step(0.01));
    }

    // ── Time Decay ────────────────────────────────────────────────────────────
    {
        let g = "Time Decay"; let e = Some("enable_time_decay");
        v.push(F::new(g, e, "enable_time_decay", "Enabled", "bool", false,
            "Targets gamma/theta as hourly markets approach expiry."));
        v.push(F::new(g, e, "time_decay_position_size_usdc", "Position Size", "usd", false,
            "USDC per time-decay position.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "time_decay_max_exposure_usdc", "Max Exposure", "usd", false,
            "Hard cap on total time-decay capital at risk.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "time_decay_stop_loss_pct", "Stop Loss", "pct", false,
            "Entry-relative stop loss (0.05 = 5%).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "time_decay_max_entry_price", "Max Entry", "price", false,
            "Highest price the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        // Advanced
        v.push(F::new(g, e, "time_decay_min_entry_price", "Min Entry", "price", true,
            "Lowest price the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "time_decay_obi_adverse_block", "OBI Adverse Block", "decimal", true,
            "Block entry when order-book imbalance is adverse beyond this.").range(0.0, 1.0).step(0.05));
        v.push(F::new(g, e, "time_decay_convergence_exit_bid", "Convergence Exit Bid", "price", true,
            "Exit when the bid converges to/above this level.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "time_decay_min_secs_to_expiry", "Min Secs to Expiry", "secs", true,
            "Don't enter with fewer than this many seconds left.").min(0.0).step(1.0).unit("s"));
        v.push(F::new(g, e, "time_decay_max_secs_to_expiry", "Max Secs to Expiry", "secs", true,
            "Don't enter with more than this many seconds left.").min(0.0).step(1.0).unit("s"));
        v.push(F::new(g, e, "min_time_decay_net_profit", "Min Net Profit", "price", true,
            "Minimum net edge (after fees) required to enter.").range(0.0, 1.0).step(0.005));
        v.push(F::new(g, e, "time_decay_max_fast_velocity_pct", "Max Fast Velocity", "decimal", true,
            "Block entry when short-window oracle velocity exceeds this fraction.").range(0.0, 0.01).step(0.00005));
        v.push(F::new(g, e, "time_decay_max_slow_drift_pct", "Max Slow Drift", "decimal", true,
            "Block entry when slow oracle drift exceeds this fraction.").range(0.0, 0.1).step(0.0005));
        v.push(F::new(g, e, "time_decay_iv_stop_tighten_multiplier", "IV Stop Tighten Mult", "decimal", true,
            "Multiplier that tightens the stop-loss as implied vol rises.").range(0.0, 1.0).step(0.05));
        v.push(F::new(g, e, "time_decay_min_hold_secs", "Min Hold Secs", "secs", true,
            "Minimum hold time before a stop-loss can trigger.").min(0.0).step(1.0).unit("s"));
    }

    // ── Momentum ────────────────────────────────────────────────────────────────
    {
        let g = "Momentum"; let e = Some("enable_momentum");
        v.push(F::new(g, e, "enable_momentum", "Enabled", "bool", false,
            "Rides Binance oracle velocity bursts."));
        v.push(F::new(g, e, "momentum_min_trade_size_usdc", "Min Size", "usd", false,
            "Lower bound on Kelly-sized trade.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "momentum_max_trade_size_usdc", "Max Size", "usd", false,
            "Upper bound on Kelly-sized trade.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "momentum_stop_loss_pct", "Stop Loss", "pct", false,
            "Entry-relative stop loss (0.05 = 5%).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "momentum_target_profit_pct", "Take Profit", "pct", false,
            "Entry-relative take profit (0.20 = 20%).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "momentum_max_exposure_usdc", "Max Exposure", "usd", false,
            "Hard cap on total momentum capital at risk.").min(0.0).step(0.5).unit("USDC"));
        // Advanced
        v.push(F::new(g, e, "momentum_max_entry_price", "Max Entry", "price", true,
            "Highest price the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "momentum_min_entry_price", "Min Entry", "price", true,
            "Lowest price the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "momentum_threshold_pct", "Velocity Threshold", "decimal", true,
            "Minimum oracle velocity (fractional) required to trigger entry.").range(0.0, 0.1).step(0.0005));
        v.push(F::new(g, e, "momentum_max_entry_ask_sum", "Max Entry Ask Sum", "decimal", true,
            "Skip entry when YES_ask + NO_ask exceeds this (fee/slippage guard).").range(1.0, 1.2).step(0.005));
        v.push(F::new(g, e, "momentum_obi_adverse_block", "OBI Adverse Block", "decimal", true,
            "Block entry when order-book imbalance is adverse beyond this (negative).").range(-1.0, 1.0).step(0.05));
        v.push(F::new(g, e, "momentum_obi_exhaustion_block", "OBI Exhaustion Block", "decimal", true,
            "Block entry when order-book imbalance signals exhaustion above this.").range(0.0, 1.0).step(0.05));
        v.push(F::new(g, e, "momentum_take_profit_ceiling", "Take-Profit Ceiling", "price", true,
            "Cap the take-profit target token price at this level.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "momentum_catastrophic_sl_pct", "Catastrophic Stop", "pct", true,
            "Hard emergency stop-loss overriding the min-hold window.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "momentum_min_secs_to_expiry_for_entry", "Min Secs to Expiry", "secs", true,
            "Don't enter with fewer than this many seconds left.").min(0.0).step(1.0).unit("s"));
    }

    // ── Maker ─────────────────────────────────────────────────────────────────
    {
        let g = "Maker"; let e = Some("enable_maker");
        v.push(F::new(g, e, "enable_maker", "Enabled", "bool", false,
            "Two-sided resting bids — captures spread + rebates."));
        v.push(F::new(g, e, "maker_max_entry_price", "Max Entry", "price", false,
            "Highest price a resting bid will sit at.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "maker_stop_loss_pct", "Stop Loss", "pct", false,
            "Entry-relative stop loss (0.05 = 5%).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "maker_target_profit_pct", "Take Profit", "pct", false,
            "Entry-relative take profit.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "maker_max_exposure_usdc", "Max Exposure", "usd", false,
            "Hard cap on total maker capital at risk.").min(0.0).step(0.5).unit("USDC"));
        // Advanced
        v.push(F::new(g, e, "maker_min_entry_price", "Min Entry", "price", true,
            "Lowest price a resting bid will sit at.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "maker_min_spread", "Min Spread", "price", true,
            "Minimum book spread required before quoting (in price units).").range(0.0, 0.5).step(0.005));
        v.push(F::new(g, e, "maker_bid_buffer", "Bid Buffer", "price", true,
            "Distance below best ask to place the resting bid.").range(0.0, 0.5).step(0.005));
        v.push(F::new(g, e, "maker_cross_buffer", "Cross Buffer", "price", true,
            "Anti-cross safety buffer to avoid taking the book.").range(0.0, 0.5).step(0.005));
        v.push(F::new(g, e, "maker_max_combined_bid", "Max Combined Bid", "price", true,
            "Skip when YES_bid + NO_bid exceeds this (overpriced pair guard).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "maker_max_complementary_price", "Max Complementary Price", "price", true,
            "Max allowed price on the complementary leg before skipping.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "maker_max_book_imbalance_ratio", "Max Book Imbalance", "decimal", true,
            "Skip when bid/ask depth ratio exceeds this (toxic imbalance).").range(1.0, 10.0).step(0.5));
        v.push(F::new(g, e, "maker_min_secs_to_expiry", "Min Secs to Expiry", "secs", true,
            "Don't quote with fewer than this many seconds left.").min(0.0).step(1.0).unit("s"));
        v.push(F::new(g, e, "maker_toxic_flow_exit_obi", "Toxic Flow Exit OBI", "decimal", true,
            "Exit a resting position when OBI turns adverse beyond this (negative).").range(-1.0, 0.0).step(0.05));
    }

    // ── Basis ─────────────────────────────────────────────────────────────────
    {
        let g = "Basis"; let e = Some("enable_basis");
        v.push(F::new(g, e, "enable_basis", "Enabled", "bool", false,
            "Fades retail-skewed YES/NO implied probabilities."));
        v.push(F::new(g, e, "basis_stop_loss_pct", "Stop Loss", "pct", false,
            "Entry-relative stop loss (0.05 = 5%).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "basis_target_profit_pct", "Take Profit", "pct", false,
            "Entry-relative take profit.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "basis_max_exposure_usdc", "Max Exposure", "usd", false,
            "Hard cap on total basis capital at risk.").min(0.0).step(0.5).unit("USDC"));
        // Advanced
        v.push(F::new(g, e, "basis_max_entry_price", "Max Entry", "price", true,
            "Highest token price the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "basis_min_trade_size_usdc", "Min Size", "usd", true,
            "Lower bound on Kelly-sized trade.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "basis_max_trade_size_usdc", "Max Size", "usd", true,
            "Upper bound on Kelly-sized trade.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "basis_entry_skew_threshold", "Entry Skew Threshold", "decimal", true,
            "Minimum YES/NO implied-prob skew required to fade for entry.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "basis_skew_collapse_threshold", "Skew Collapse Exit", "decimal", true,
            "Exit when the skew collapses to/below this level (thesis realised).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "basis_catastrophic_sl_pct", "Catastrophic Stop", "pct", true,
            "Hard emergency stop-loss overriding the min-hold window.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "basis_min_secs_to_expiry", "Min Secs to Expiry", "secs", true,
            "Don't enter with fewer than this many seconds left.").min(0.0).step(1.0).unit("s"));
    }

    // ── GBoost ────────────────────────────────────────────────────────────────
    {
        let g = "GBoost"; let e = Some("enable_gboost");
        v.push(F::new(g, e, "enable_gboost", "Enabled", "bool", false,
            "Online gradient-boosted orderbook classifier."));
        v.push(F::new(g, e, "gboost_entry_threshold", "Entry Threshold", "decimal", false,
            "Classifier probability required to enter (0.88 = 88%).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "gboost_stop_loss_pct", "Stop Loss", "pct", false,
            "Entry-relative stop loss (0.05 = 5%).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "gboost_target_profit_pct", "Take Profit", "pct", false,
            "Entry-relative take profit.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "gboost_max_exposure_usdc", "Max Exposure", "usd", false,
            "Hard cap on total GBoost capital at risk.").min(0.0).step(0.5).unit("USDC"));
        // Advanced
        v.push(F::new(g, e, "gboost_max_yes_entry_price", "Max YES Entry", "price", true,
            "Highest YES-token ask the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "gboost_max_no_entry_price", "Max NO Entry", "price", true,
            "Highest NO-token ask the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "gboost_min_entry_price", "Min Entry", "price", true,
            "Lowest token price the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "gboost_obi_adverse_block", "OBI Adverse Block", "decimal", true,
            "Block entry when order-book imbalance is adverse beyond this (negative).").range(-1.0, 1.0).step(0.05));
        v.push(F::new(g, e, "gboost_obi_exhaustion_block", "OBI Exhaustion Block", "decimal", true,
            "Block entry when order-book imbalance signals exhaustion above this.").range(0.0, 1.0).step(0.05));
        v.push(F::new(g, e, "gboost_min_edge_from_fair", "Min Edge from Fair", "price", true,
            "Minimum edge vs classifier fair value required to enter.").range(0.0, 0.5).step(0.005));
        v.push(F::new(g, e, "gboost_min_net_profit_usdc", "Min Net Profit", "usd", true,
            "Minimum net expected profit (after fees) required to enter.").min(0.0).step(0.05).unit("USDC"));
        v.push(F::new(g, e, "gboost_min_secs_to_expiry", "Min Secs to Expiry", "secs", true,
            "Don't enter with fewer than this many seconds left.").min(0.0).step(1.0).unit("s"));
        v.push(F::new(g, e, "gboost_signal_exit_threshold", "Signal Exit Threshold", "decimal", true,
            "Exit when classifier probability decays to/below this level.").range(0.0, 1.0).step(0.01));
    }

    // ── TrendReversal ─────────────────────────────────────────────────────────────
    {
        let g = "TrendReversal"; let e = Some("enable_trendcapture");
        v.push(F::new(g, e, "enable_trendcapture", "Enabled", "bool", false,
            "Fades priced-in oracle drift (TrendReversal): buys the opposite token to a strong, confirmed move and rides the mean-reversion."));
        v.push(F::new(g, e, "trendcapture_min_trade_size_usdc", "Min Size", "usd", false,
            "Lower bound on Kelly-sized trade.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "trendcapture_max_trade_size_usdc", "Max Size", "usd", false,
            "Upper bound on Kelly-sized trade.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "trendcapture_max_exposure_usdc", "Max Exposure", "usd", false,
            "Hard cap on total TrendCapture capital at risk.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "trendcapture_stop_loss_pct", "Stop Loss", "pct", false,
            "Entry-relative stop loss (0.12 = 12%).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "trendcapture_target_profit_pct", "Take Profit", "pct", false,
            "Entry-relative take profit (0.20 = 20%).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "trendcapture_max_entry_price", "Max Entry", "price", false,
            "Highest price the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        // Advanced
        v.push(F::new(g, e, "trendcapture_min_entry_price", "Min Entry", "price", true,
            "Lowest price the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "trendcapture_max_entry_ask_sum", "Max Entry Ask Sum", "decimal", true,
            "Skip entry when YES_ask + NO_ask exceeds this (fee/slippage guard).").range(1.0, 1.2).step(0.005));
        v.push(F::new(g, e, "trendcapture_obi_adverse_block", "OBI Adverse Block", "decimal", true,
            "Block entry when order-book imbalance is adverse beyond this (negative).").range(-1.0, 1.0).step(0.05));
        v.push(F::new(g, e, "trendcapture_obi_exhaustion_block", "OBI Exhaustion Block", "decimal", true,
            "Block entry when order-book imbalance signals exhaustion above this.").range(0.0, 1.0).step(0.05));
        v.push(F::new(g, e, "trendcapture_max_token_spread_pct", "Max Token Spread", "pct", true,
            "Skip entry when the token bid/ask spread exceeds this fraction.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "trendcapture_reversal_drift_pct", "Reversal Drift", "decimal", true,
            "Adverse drift fraction that signals the fade thesis is breaking (exit).").range(0.0, 0.1).step(0.0005));
        v.push(F::new(g, e, "trendcapture_strike_gap_pct", "Strike Gap", "decimal", true,
            "Minimum oracle-vs-strike gap (fraction) required to enter.").range(0.0, 0.1).step(0.0005));
        v.push(F::new(g, e, "trendcapture_take_profit_ceiling", "Take-Profit Ceiling", "price", true,
            "Cap the take-profit target token price at this level.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "trendcapture_catastrophic_sl_pct", "Catastrophic Stop", "pct", true,
            "Hard emergency stop-loss overriding the min-hold window.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "trendreversal_mode", "Follow Mode (invert fade)", "bool", true,
            "When on, follow the confirmed move instead of fading it (A/B toggle)."));
    }

    // ── Convergence ───────────────────────────────────────────────────────────
    {
        let g = "Convergence"; let e = Some("enable_convergence");
        v.push(F::new(g, e, "enable_convergence", "Enabled", "bool", false,
            "Macro-conviction directional Viper (BTC-only): enters on aligned institutional pulse + CVD/OI."));
        v.push(F::new(g, e, "convergence_position_size_usdc", "Size", "usd", false,
            "Fixed entry size per position.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "convergence_max_exposure_usdc", "Max Exposure", "usd", false,
            "Hard cap on total Convergence capital at risk.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "convergence_stop_loss_pct", "Stop Loss", "pct", false,
            "Entry-relative stop loss (0.10 = 10%).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "convergence_target_profit_pct", "Take Profit", "pct", false,
            "Entry-relative take profit (0.15 = 15%).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "convergence_max_entry_price", "Max Entry", "price", false,
            "Highest token ask the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        // Advanced
        v.push(F::new(g, e, "convergence_min_entry_price", "Min Entry", "price", true,
            "Lowest token price the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "convergence_pulse_threshold", "Pulse Threshold", "decimal", true,
            "Minimum institutional-pulse magnitude required to enter.").range(0.0, 5.0).step(0.1));
        v.push(F::new(g, e, "convergence_coherence_min", "Min Coherence", "decimal", true,
            "Minimum tide coherence required to trust the pulse signal.").range(0.0, 1.0).step(0.05));
        v.push(F::new(g, e, "convergence_cvd_confirm_margin", "CVD Confirm Margin", "decimal", true,
            "Minimum CVD confirmation margin required to align with the pulse.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "convergence_max_token_spread_pct", "Max Token Spread", "pct", true,
            "Skip entry when the token bid/ask spread exceeds this fraction.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "convergence_obi_adverse_block", "OBI Adverse Block", "decimal", true,
            "Block entry when order-book imbalance is adverse beyond this.").range(-1.0, 1.0).step(0.05));
        v.push(F::new(g, e, "convergence_skip_band_low", "Skip Band Low", "price", true,
            "Lower edge of the ~0.50 coin-flip band where entries are skipped.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "convergence_skip_band_high", "Skip Band High", "price", true,
            "Upper edge of the ~0.50 coin-flip band where entries are skipped.").range(0.0, 1.0).step(0.01));
    }

    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_serializes_and_keys_are_unique() {
        let schema = config_schema();
        assert!(!schema.is_empty(), "schema must not be empty");

        // Serializes cleanly and renames value_type → "type".
        let json = serde_json::to_string(&schema).expect("schema serializes");
        assert!(json.contains("\"type\":"), "value_type must serialize as `type`");

        // Keys are unique (a duplicate would make PATCH/render ambiguous).
        let mut keys: Vec<&str> = schema.iter().map(|f| f.key).collect();
        keys.sort_unstable();
        let before = keys.len();
        keys.dedup();
        assert_eq!(before, keys.len(), "duplicate field keys in schema");

        // Every numeric field with both bounds has min <= max.
        for f in &schema {
            if let (Some(min), Some(max)) = (f.min, f.max) {
                assert!(min <= max, "{}: min {} > max {}", f.key, min, max);
            }
        }
    }

    #[test]
    fn every_schema_key_exists_in_dynamic_config() {
        use crate::helpers::dynamic_config::DynamicConfig;
        // Serialize factory defaults; each schema `key` MUST be a real serde field
        // so PATCH/render can never target a phantom knob (drift guard).
        let json = serde_json::to_value(DynamicConfig::default())
            .expect("DynamicConfig serializes");
        let obj = json.as_object().expect("DynamicConfig is a JSON object");
        for f in config_schema() {
            assert!(
                obj.contains_key(f.key),
                "schema key `{}` (group {}) is not a DynamicConfig field",
                f.key, f.group,
            );
        }
    }
}
