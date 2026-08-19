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
    v.push(F::new("Global", None, "intl_taker_fee_rate", "Taker Fee Rate", "pct", true,
        "Polymarket's taker fee coefficient: fee = rate × price × (1 − price) × shares, charged on BOTH \
         legs of a round trip; makers pay nothing. Recorded P&L is net of it. 0.07 is the rate measured \
         from actual collateral movement — the venue's fee-rate endpoint advertises 1000 bps, but that is \
         the ceiling an order authorizes, not what is charged. Only change this if Polymarket's schedule \
         changes; setting it wrong silently skews every recorded trade.").range(0.0, 0.5).step(0.005));

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
        v.push(F::new(g, e, "arb_settle_grace_secs", "Orphan Settle Grace", "secs", true,
            "Seconds the orphan arbiter waits after cancelling the missing leg's GTC before re-reading its \
             on-chain balance and committing to a repair. Guards against flattening a leg whose fill was still \
             settling; every second is also a second of naked directional exposure. The post-flatten late-fill \
             watcher catches a leg that fills after we commit, so cutting this short costs at most two spreads. \
             Was hardcoded at 10s — the largest discretionary slice of the 26s entry→flatten latency measured \
             on 2026-08-15.").range(0.0, 30.0).step(1.0).unit("s"));
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
        v.push(F::new(g, e, "momentum_deriv_gate_enabled", "Deriv Gate", "bool", true,
            "Derivatives-Raptor confirmation gate: block entries the perp book contradicts (counter CVD flow or hard OI unwind). Inert when OI/CVD report no data."));
        v.push(F::new(g, e, "momentum_deriv_cvd_confirm_margin", "Deriv CVD Margin", "decimal", true,
            "Distance from neutral CVD ratio 1.0 that blocks the contradicted direction (0.15 ⇒ ≤0.85 blocks bulls, ≥1.15 blocks bears).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "momentum_deriv_oi_unwind_block", "Deriv OI Unwind Block", "decimal", true,
            "OI delta at/below which hard de-leveraging blocks BOTH directions (−0.05 = −5% per poll).").range(-1.0, 0.0).step(0.01));
        v.push(F::new(g, e, "momentum_obi_exhaust_min_hold_secs", "OBI Exhaust Min Hold", "secs", true,
            "Minimum hold before the in-position OBI-exhaustion exit may fire. A fresh fill is underwater by the bid/ask spread alone, so exiting on tick one books the spread as a loss — this is the floor that prevents it.").min(0.0).step(5.0).unit("s"));
        v.push(F::new(g, e, "momentum_obi_exhaust_persist_secs", "OBI Exhaust Persistence", "secs", true,
            "How long the book must read exhausted, continuously, before the OBI exit fires. Single-sample OBI on the hourly book swings across the threshold constantly, so a momentary spike must not arm the exit; any normal reading resets the clock. Seconds rather than ticks because the patrol loop runs at 75ms.").range(0.0, 120.0).step(1.0).unit("s"));
        v.push(F::new(g, e, "momentum_obi_exhaust_max_adverse_pct", "OBI Exhaust Max Adverse", "pct", true,
            "Deepest drawdown at which the OBI-exhaustion exit may still fire (negative). Past this the position is already wrecked and the stop-loss owns it. Keep it beyond the stop loss or the early exit can never fire.").range(-1.0, 0.0).step(0.01));
        v.push(F::new(g, e, "momentum_tp_fee_margin_mult", "TP Fee Margin", "decimal", true,
            "Multiple of the round-trip taker fee the take-profit must clear. Venue fees scale with entry price (2 × rate × (1 − entry) of notional), so a flat percentage target sits below break-even on cheap entries; the effective target is lifted to this multiple of the fee whenever it would not clear.").range(1.0, 3.0).step(0.05));
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
        v.push(F::new(g, e, "maker_quote_size_usdc", "Quote Size", "usd", false,
            "USDC notional per resting quote. Clamped to Max Exposure; keep at or below it (ideally ≤ half) so the maker can post without tripping the exposure cap.").min(0.0).step(0.5).unit("USDC"));
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
        v.push(F::new(g, e, "maker_toxic_reentry_cooldown_secs", "Toxic Re-entry Lockout", "secs", true,
            "After a ToxicFill exit, block re-quoting that same token for this long.").min(0.0).step(30.0).unit("s"));
        // ToxicFill confirmation gates. These apply only once a quote has FILLED —
        // an unfilled quote is still pulled the instant OBI breaches. Raising them
        // makes the maker sit through more book noise to let the spread convert;
        // lowering them returns toward the old exit-on-any-adverse-book behaviour.
        v.push(F::new(g, e, "maker_toxic_min_hold_secs", "Toxic Min Hold", "secs", true,
            "Minimum seconds a filled position is held before ToxicFill may fire. Covers the window where OBI is dominated by our own quote being lifted.").min(0.0).step(5.0).unit("s"));
        v.push(F::new(g, e, "maker_toxic_min_adverse_pct", "Toxic Min Adverse Move", "pct", true,
            "ToxicFill also requires the bid to be this far below entry (0.02 = 2%). A hostile-looking book that hasn't moved price is noise.").range(0.0, 0.5).step(0.005));
        v.push(F::new(g, e, "maker_toxic_obi_confirm_ticks", "Toxic OBI Confirm Ticks", "decimal", true,
            "Consecutive OBI breaches required before ToxicFill fires. A healthy tick resets the count.").range(1.0, 20.0).step(1.0));
        // Resting maker exit — the spread-capture exit path.
        v.push(F::new(g, e, "maker_resting_exit_enabled", "Resting Exit", "bool", true,
            "Exit filled positions with a resting post-only ask (captures the spread) instead of only crossing back to the bid. Stops and the near-expiry flatten still cross."));
        v.push(F::new(g, e, "maker_resting_exit_min_edge_pct", "Resting Exit Min Edge", "pct", true,
            "Price floor for the resting ask, over avg entry (0.04 = 4%). Stops a collapsing spread from dragging the exit down to a scratch.").range(0.0, 0.5).step(0.005));
        v.push(F::new(g, e, "maker_resting_exit_ask_improvement_ticks", "Resting Exit Ask Improvement", "decimal", true,
            "Ticks to undercut the best ask by. 1 = take queue priority; 0 = join the ask and earn the extra tick.").range(0.0, 5.0).step(1.0));
        v.push(F::new(g, e, "maker_resting_exit_reprice_threshold", "Resting Exit Reprice Deadband", "price", true,
            "Minimum price move before the resting ask is cancelled and re-posted. Repricing surrenders queue priority, so keep this wider than normal book flicker.").range(0.0, 0.2).step(0.005));
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
        v.push(F::new(g, e, "basis_max_spread_pct", "Max Entry Spread", "pct", true,
            "Skip entries when the entry-side book spread exceeds this fraction of mid — wide books birth positions straight into the catastrophic stop.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "basis_loss_lockout_count", "Loss Lockout Count", "int", true,
            "Stop-loss exits on one token before re-entry locks out (0 = disabled). Prevents grinding a trend day one stop at a time.").min(0.0).step(1.0));
        v.push(F::new(g, e, "basis_loss_lockout_secs", "Loss Lockout Duration", "secs", true,
            "How long a token stays locked out after hitting the loss-lockout count.").min(0.0).step(60.0).unit("s"));
        v.push(F::new(g, e, "basis_extreme_skew_bypass", "Extreme Skew Bypass", "bool", true,
            "Allow entries without funding confirmation at 2× skew threshold. Off by default — extreme skew on daily markets is usually the trend, not mispricing."));
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
        v.push(F::new(g, e, "gboost_concept_drift_threshold", "Drift Threshold", "decimal", true,
            "Chi-squared drift score above which a retrain counts toward suppression. Calibrated for drift window 400: normal BTC intraday vol scores 15–21, genuine regime collapse 22+.").range(1.0, 100.0).step(0.5));
        v.push(F::new(g, e, "gboost_drift_consecutive_required", "Drift Consecutive Required", "int", true,
            "Consecutive above-threshold retrains required before entries are suppressed.").range(1.0, 10.0).step(1.0));
        v.push(F::new(g, e, "gboost_drift_stable_clear_required", "Drift Stable Clear Required", "int", true,
            "Consecutive below-threshold retrains required before suppression is lifted.").range(1.0, 10.0).step(1.0));
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
        v.push(F::new(g, e, "trendcapture_deriv_gate_enabled", "Deriv Gate", "bool", true,
            "Derivatives-Raptor confirmation gate: block entries the perp book contradicts (counter CVD flow or hard OI unwind). Inert when OI/CVD report no data."));
        v.push(F::new(g, e, "trendcapture_deriv_cvd_confirm_margin", "Deriv CVD Margin", "decimal", true,
            "Distance from neutral CVD ratio 1.0 that blocks the contradicted direction (0.15 ⇒ ≤0.85 blocks bulls, ≥1.15 blocks bears).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "trendcapture_deriv_oi_unwind_block", "Deriv OI Unwind Block", "decimal", true,
            "OI delta at/below which hard de-leveraging blocks BOTH directions (−0.05 = −5% per poll).").range(-1.0, 0.0).step(0.01));
        v.push(F::new(g, e, "trendcapture_take_profit_ceiling", "Take-Profit Ceiling", "price", true,
            "Cap the take-profit target token price at this level.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "trendcapture_catastrophic_sl_pct", "Catastrophic Stop", "pct", true,
            "Hard emergency stop-loss overriding the min-hold window.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "trendreversal_mode", "Fade Mode", "bool", true,
            "ON = fade the 10m spike in a flat 60m macro (mean-reversion). OFF = legacy trend-following (buy with the confirmed drift)."));
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
        v.push(F::new(g, e, "convergence_drift_coherence_deadband_pct", "Drift Coherence Deadband", "pct", true,
            "Fraction of oracle price below which a 10m/60m drift leg counts as neutral. \
             Entry is vetoed when BOTH legs clear this and point opposite ways (counter-trend bounce). \
             Lower = stricter.").range(0.0, 0.01).step(0.0001));
        v.push(F::new(g, e, "convergence_velocity_opposition_pct", "Velocity Opposition Deadband", "pct", true,
            "Fraction of oracle price the 5s velocity must run AGAINST the intended side to veto entry. \
             Zero velocity never vetoes. Lower = stricter.").range(0.0, 0.005).step(0.00001));
    }

    // ── FairValue ─────────────────────────────────────────────────────────────
    {
        let g = "FairValue"; let e = Some("enable_fairvalue");
        v.push(F::new(g, e, "enable_fairvalue", "Enabled", "bool", false,
            "Analytic binary pricing Φ(ln(S/K)/σ√T): buys sides trading at a discount to model fair value; snipes settlements."));
        v.push(F::new(g, e, "fairvalue_trade_size_usdc", "Size", "usd", false,
            "Fixed entry size per position.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "fairvalue_max_exposure_usdc", "Max Exposure", "usd", false,
            "Hard cap on total FairValue capital at risk.").min(0.0).step(0.5).unit("USDC"));
        v.push(F::new(g, e, "fairvalue_stop_loss_pct", "Stop Loss", "pct", false,
            "Entry-relative stop loss (0.12 = 12%). Catastrophic bypass at 2× this.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "fairvalue_obi_adverse_block", "OBI Adverse Block", "decimal", true,
            "Reject entries when the book on the side being bought is this offer-heavy. OBI = (bid_depth − ask_depth)/total; −1.0 is an all-offer book with no bid support, which cannot be exited without giving back far more than the stop width.").range(-1.0, 0.0).step(0.05));
        v.push(F::new(g, e, "fairvalue_target_profit_pct", "Take Profit", "pct", false,
            "Entry-relative take profit (0.20 = 20%). Skipped in favor of fee-free settlement when the model is ≥0.90 near expiry.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "fairvalue_base_edge", "Base Edge", "price", false,
            "Required (fair − ask − fee) edge mid-session; tapers to Min Edge inside the final 30 min.").range(0.0, 0.5).step(0.005));
        // Advanced
        v.push(F::new(g, e, "fairvalue_model_reversal_decay_pct", "Reversal Decay", "pct", true,
            "Exit once the model's probability for the held side has lost this fraction of its value AT ENTRY \
             (0.35 = an entry at fair 0.30 exits below 0.195). Entry-relative on purpose: cheap-tail entries are \
             taken at a fair value of 0.30-0.34, so an absolute floor closed them on the 60s min-hold rather than \
             on any real change of thesis. Lower = cuts a decaying thesis sooner.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "fairvalue_sigma_floor_horizon_secs", "Vol Floor Horizon", "secs", true,
            "Forecast horizon at or below which the realized-vol floor stops binding, ramping to full \
             strength at twice this. The floor guards against trusting a 1-hour vol window for a settlement \
             many hours out. Set to 0 to restore an unconditional floor — recommended. RAISING this makes the \
             model assume LESS volatility, which pushes fair value toward 0/1 and manufactures edge against the \
             favorite. At 3600 the floor never bound on an hourly market at all (any secs_left ≤ 3600 collapses \
             it to the absolute backstop): prod on 2026-08-13/14 ran hourly σ at 1.9-2.6e-5/√s = 13% annualized \
             BTC vol, roughly half what the book implied, and FairValue lost $3.79 over 15 trades.")
            .min(0.0).step(300.0).unit("s"));
        v.push(F::new(g, e, "fairvalue_edge_noise_multiple", "Edge vs Noise", "decimal", true,
            "Multiple of the model's own recent fair-value noise the edge must clear, on top of Base Edge. \
             Noise is the std-dev of successive fair-value moves over the last 15 min, rescaled to a 2-minute \
             horizon. Guards the case where an 8¢ edge is read off a model that is itself swinging 18¢ per tick \
             — measured at 24% of hourly ticks on 2026-08-13/14. 0 disables the gate; higher = only trade when \
             the model has been steady.").range(0.0, 5.0).step(0.1));
        v.push(F::new(g, e, "fairvalue_stop_model_confirm_frac", "Stop Model Confirm", "decimal", true,
            "Multiple of the entry edge requirement the model must still show, at the live ask, for a losing \
             position to veto its own stop loss. The stop is a price rule inside a model-vs-price strategy: if \
             the model still sees entry-grade edge, the drawdown is the market coming toward us. Higher = \
             stricter = the stop fires more readily; 0 restores a price-only stop. Catastrophic stops (2× the \
             stop width) are never vetoed, so this can only extend a hold between one and two stop widths. \
             On 2026-08-15 a NO entered at $0.50 stopped out at $0.44 — 1.4× the model's own 120s noise, with \
             3,800s left — while the model read edge +0.145 vs req 0.140; it settled at $1.00.")
            .range(0.0, 5.0).step(0.1));
        v.push(F::new(g, e, "fairvalue_post_exit_cooldown_secs", "Post-Exit Cooldown", "secs", true,
            "Seconds a token is locked out after any FairValue exit. Second entries into a market the viper had \
             just left went 0-for-4 for −$1.88 gross on 2026-08-13/14 while first entries were flat, and the \
             300s setting let one of them back in 365s after the stop.").min(0.0).step(60.0).unit("s"));
        v.push(F::new(g, e, "fairvalue_max_stop_losses_per_market", "Max Stops / Market", "int", true,
            "Stop-outs allowed on one market before the breaker bars further entries there. Counts genuine \
             stop-outs, not exit retries. At 2 the breaker only fires after both losses are booked.")
            .min(1.0).step(1.0));
        v.push(F::new(g, e, "fairvalue_min_edge", "Min Edge", "price", true,
            "Edge floor at expiry — the settlement-snipe requirement.").range(0.0, 0.5).step(0.005));
        v.push(F::new(g, e, "fairvalue_max_entry_price", "Max Entry", "price", true,
            "Highest token ask the strategy will pay (0.985 admits settlement snipes).").range(0.0, 1.0).step(0.005));
        v.push(F::new(g, e, "fairvalue_min_entry_price", "Min Entry", "price", true,
            "Lowest token ask the strategy will pay to enter.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "fairvalue_prefer_hourly", "Prefer Hourly Market", "bool", true,
            "Trade the hourly market rather than the Window/Daily venue. The required edge scales with time to expiry and is capped, so on the daily venue it pins at the cap (25%) — an edge that does not occur on a liquid book. Off restores daily-first ordering."));
    }

    // ── Raptor polling ────────────────────────────────────────────────────────
    // Cadence for the two credentialed Raptors. The defaults are sized for each
    // provider's FREE tier; an operator on a paid plan raises the rate here
    // instead of recompiling.
    //
    // The minimums are deliberate and are about the provider, not about DRADIS:
    // these values set an outbound request rate against a third-party API, and
    // the LLM autonomy tiers can move config, so an unclamped field risks
    // getting the operator's key rate-limited or banned. Live Tennis allows
    // 30 req/min, hence a 5s floor with headroom; The Odds API is billed per
    // request on every tier, so its floor is higher.
    {
        let g = "Raptor Polling"; let e: Option<&'static str> = None;
        v.push(F::new(g, e, "sports_poll_secs", "Sports Poll Interval", "secs", false,
            "Seconds between Sports Raptor (The Odds API) polls. The 300s default suits the free tier's ~500 requests/month; lower it only on a paid plan.")
            .range(10.0, 86_400.0).step(10.0).unit("s"));
        v.push(F::new(g, e, "sports_low_budget_warn", "Sports Budget Warning", "secs", true,
            "Warn when The Odds API reports this many requests remaining. Raise it on a large plan so the warning still gives useful notice.")
            .min(0.0).step(10.0));
        v.push(F::new(g, e, "tennis_poll_secs", "Tennis Poll Interval", "secs", false,
            "Seconds between Tennis Raptor (Live Tennis API) polls. The 900s default keeps an all-day run inside the free tier's 100 requests/day; ~60s gives near point-level tracking but needs a paid plan.")
            .range(5.0, 86_400.0).step(5.0).unit("s"));
        v.push(F::new(g, e, "tennis_low_budget_warn", "Tennis Budget Warning", "secs", true,
            "Warn when the Live Tennis API reports this many requests remaining in the current window.")
            .min(0.0).step(5.0));

        // Free-text provider identifiers. These have no min/max because the
        // valid set belongs to the upstream API, not to DRADIS — the value is
        // passed through to the provider verbatim. A wrong one does not error
        // loudly: an unknown sport key yields empty polls, and an unknown tour
        // filter returns an empty match list that reads as a healthy quiet
        // period. The descriptions carry that warning, since it is the only
        // guard rail these fields have.
        v.push(F::new(g, e, "sports_odds_sport", "Sports Key", "string", true,
            "The Odds API sport key, passed to the provider verbatim. 'upcoming' = next games across all in-season sports; or a specific key such as 'americanfootball_nfl'. ⚠️ Not validated — a wrong key returns no events and the raptor simply reads as offline. Valid keys: the-odds-api.com/sports-odds-data/sports-apis.html"));
        v.push(F::new(g, e, "sports_odds_regions", "Sports Regions", "string", true,
            "Comma-separated bookmaker regions for the odds query: us, us2, uk, eu, au. ⚠️ Not validated — an unrecognised region returns no bookmakers."));
        v.push(F::new(g, e, "tennis_tour", "Tennis Tour", "string", true,
            "Live Tennis API tour filter: atp, wta, challenger, itf, juniors — or blank for all tours. ⚠️ Not validated, and this one fails SILENTLY: a misspelt tour returns an empty match list, which is indistinguishable from tennis being off-season or between sessions. Leave blank if unsure."));
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
