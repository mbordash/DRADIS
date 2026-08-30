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
use rust_decimal::prelude::ToPrimitive;

/// Which configuration row a field actually lives in.
///
/// DRADIS has two config scopes that look identical in the schema and behave
/// completely differently: the global `dynamic_config` row, and each deployed
/// squadron's `squadron_configs` row. Nothing recorded which one a field
/// belonged to, so a field could be RENDERED at one scope and READ at the other
/// and no part of the system would notice.
///
/// It went unnoticed three times. `arb_settle_grace_secs` rendered in the
/// Arbitrage card (per-squadron) and was read from the global row, so editing
/// "Orphan Settle Grace" wrote a row nobody read. `llm_max_output_tokens`
/// rendered in the Deployment panel and was never read at all — every provider
/// call used the compile-time constant, while its help text told operators to
/// raise it. `intl_taker_fee_rate` is read at BOTH scopes, so one round trip can
/// book at two different fee rates.
///
/// Declared per GROUP rather than per field because the group already decides
/// where the UI renders a field, and the two must agree by construction: the
/// Control Tower renders `Deployment` in Setup, and `Order Book` plus
/// `Exit Accounting` plus the viper-named groups on a squadron page.
#[derive(Serialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum ConfigScope {
    /// Lives in the global row. Read via `global_config_tx()` or `load_or_default`.
    Global,
    /// Lives in each squadron's row. Read from the per-tick config snapshot.
    Squadron,
}

/// The scope every group belongs to. Exhaustive on purpose: a new group with no
/// entry here fails `every_group_declares_a_scope` rather than defaulting to a
/// guess, because guessing is what produced the three defects above.
pub fn scope_for_group(group: &str) -> Option<ConfigScope> {
    Some(match group {
        // Instance-wide: rendered in Setup, read from the global row.
        "Global" | "Deployment" | "Raptor Polling" => ConfigScope::Global,
        // Per-squadron: rendered on a squadron page, read from its own row.
        "Order Book" | "Exit Accounting" => ConfigScope::Squadron,
        "Arbitrage" | "Basis" | "Convergence" | "FairValue" | "GBoost"
        | "Maker" | "Momentum" | "Time Decay" | "TrendReversal" => ConfigScope::Squadron,
        _ => return None,
    })
}

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
    /// Which config row this field lives in — see [`ConfigScope`]. Filled in by
    /// the post-pass at the end of `config_schema()` from the field's group, so
    /// a field cannot declare a scope that contradicts where it renders.
    pub scope: ConfigScope,
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
            // Placeholder; the post-pass at the end of `config_schema()` resolves
            // it from the group so the two can never disagree.
            scope: ConfigScope::Squadron,
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
        v.push(F::new(g, e, "maker_improve_bid_only", "Improve The Bid", "bool", false,
            "Cap the maker's bid at one tick above the best bid, instead of pricing it purely \
             back from the ask. On a tight book the two agree — ask $0.52, bid $0.50, quote \
             $0.50 — but on a wide one the ask-anchored price crosses most of the spread: with \
             a $0.35 bid against a $0.53 ask it quotes $0.51, sixteen cents above anything \
             anyone is bidding, and the position marks to the bid the moment it fills. That is \
             a loss taken at entry, not a market move. Leave this on unless resting bids on \
             your venue simply never get lifted and you would rather pay the spread for a fill.")
            );
        v.push(F::new(g, e, "maker_max_combined_bid", "Max Combined Bid", "price", true,
            "Skip when YES_bid + NO_bid exceeds this (overpriced pair guard).").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "maker_max_complementary_price", "Max Complementary Price", "price", true,
            "Max allowed price on the complementary leg before skipping.").range(0.0, 1.0).step(0.01));
        v.push(F::new(g, e, "maker_max_book_imbalance_ratio", "Max Book Imbalance", "decimal", true,
            "Skip when bid/ask depth ratio exceeds this (toxic imbalance).").range(1.0, 10.0).step(0.5));
        v.push(F::new(g, e, "maker_min_secs_to_expiry", "Min Secs to Expiry", "secs", true,
            "Don't quote with fewer than this many seconds left.").min(0.0).step(1.0).unit("s"));
        v.push(F::new(g, e, "maker_min_market_age_secs", "Market Maturation", "secs", true,
            "Observe a market for this long before quoting into it, so the book has settled. \
             Capped by Maturation Cap below, which keeps the wait sane on short-lived markets.")
            .min(0.0).step(30.0).unit("s"));
        v.push(F::new(g, e, "maker_maturation_max_fraction", "Maturation Cap", "decimal", true,
            "Ceiling on the maturation wait as a fraction of the market's own lifetime. \
             At 0.25 a 15-minute market matures in under 4 minutes while a daily market still \
             serves the full wait above. Set to 0 to always use the full wait — on short markets \
             that can consume the whole tradeable window.")
            .range(0.0, 1.0).step(0.05));
        v.push(F::new(g, e, "maker_toxic_flow_exit_obi", "Toxic Flow Exit OBI", "decimal", true,
            "Exit a resting position when OBI turns adverse beyond this (negative).").range(-1.0, 0.0).step(0.05));
        v.push(F::new(g, e, "maker_toxic_reentry_cooldown_secs", "Toxic Re-entry Lockout", "secs", true,
            "After a ToxicFill exit, block re-quoting that same token for this long.").min(0.0).step(30.0).unit("s"));
        // ToxicFill confirmation gates. These apply only once a quote has FILLED —
        // an unfilled quote is still pulled the instant OBI breaches. Raising them
        // makes the maker sit through more book noise to let the spread convert;
        // lowering them returns toward the old exit-on-any-adverse-book behavior.
        v.push(F::new(g, e, "maker_toxic_min_hold_secs", "Toxic Min Hold", "secs", true,
            "Minimum seconds a filled position is held before ToxicFill may fire. Covers the window where OBI is dominated by our own quote being lifted.").min(0.0).step(5.0).unit("s"));
        v.push(F::new(g, e, "maker_toxic_min_adverse_pct", "Toxic Min Adverse Move", "pct", true,
            "ToxicFill also requires the bid to be this far below entry (0.02 = 2%). A hostile-looking book that hasn't moved price is noise.").range(0.0, 0.5).step(0.005));
        v.push(F::new(g, e, "maker_toxic_obi_confirm_ticks", "Toxic OBI Confirm Ticks", "decimal", true,
            "Consecutive OBI breaches required before ToxicFill fires. A healthy tick resets the count.").range(1.0, 20.0).step(1.0));
        // Oracle-drift thresholds. The oracle (Binance) leads the Polymarket book by
        // minutes, so these fire before OBI can confirm. Two separate knobs because the
        // two actions have different costs: cancelling an unfilled quote is free, while
        // exiting a filled position pays the spread to realize a loss.
        v.push(F::new(g, e, "maker_oracle_drift_pull_frac", "Oracle Drift Quote Pull", "pct", true,
            "Cancel an UNFILLED resting quote once the oracle moves this far against it (0.0003 = 0.03%). Cancelling costs nothing, so keep this tight.").range(0.0, 0.05).step(0.0001));
        v.push(F::new(g, e, "maker_oracle_drift_exit_frac", "Oracle Drift Exit", "pct", true,
            "Exit a FILLED position once the oracle moves this far against it since the quote was placed (0.0015 = 0.15%). Fires ahead of the OBI path, which lags by minutes. Set 0 to disable and rely on OBI alone.").range(0.0, 0.05).step(0.0005));
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
        v.push(F::new(g, e, "gboost_budget", "Training Budget", "decimal", true,
            "How hard the model works on each retrain. The booster keeps adding trees until this \
             budget is spent, so a higher number fits the recorded outcomes more closely and a \
             lower one stops earlier. Raising it is not free in either direction: more trees on a \
             small pool of trades learns that pool's noise as if it were signal, and the model \
             then reports high conviction on setups it has never really seen. Too low is the \
             sharper failure: a model under 20 trees is discarded as a stump and the strategy \
             backs off instead of trading, so if the log reports degenerate retrains, raise this \
             before touching anything else. How many trees a given budget buys depends entirely \
             on the data — the same setting produced 5 trees on one pool and 390 on another — so \
             judge it by the tree count in the retrain log, never by the number here.")
            .range(0.1, 3.0).step(0.05));
        v.push(F::new(g, e, "gboost_iteration_limit", "Training Iteration Cap", "secs", true,
            "Hard ceiling on boosting rounds per retrain, whichever comes first with the budget. \
             It exists to bound wall-clock time: training runs on a blocking thread, and an \
             unbounded fit on a large pool holds that thread long enough to matter. Hitting the \
             cap is normal and harmless, it just means the budget had more to spend than the cap \
             allowed — so raise this and the budget together, or the extra budget cannot be used.")
            .range(50.0, 20_000.0).step(50.0).unit("iter"));
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
    // Exit accounting — venue-neutral, applies to every viper, so it is not
    // filed under a strategy card.
    {
        let g = "Exit Accounting"; let e: Option<&'static str> = None;
        v.push(F::new(g, e, "exit_reconcile_max_deviation", "Exit Reconcile Band", "decimal", true,
            "When the exchange rejects a sell because the shares are already gone, the profit or loss is taken from how much your collateral actually moved across the round trip. Trust it only if the exit price that implies lands within this distance of the bid showing at the time — a wider gap means something else moved collateral and the figure cannot be relied on, so the trade is booked with zero P&L rather than a guess. Raise it if genuine exits are being left unreconciled; lower it to be stricter.")
            .range(0.0, 0.50).step(0.01));
    }

    {
        let g = "Order Book"; let e: Option<&'static str> = None;
        v.push(F::new(g, e, "obi_use_whole_book", "Use Whole-Book Depth", "bool", false,
            "Measure order-book imbalance across every price level instead of only the best bid and ask. The best price alone is often one or two contracts, so a ratio built from it swings wildly on trades that move nothing real: measured on Kalshi, the two readings disagreed about which way the book leaned 41% of the time, and roughly three in four crypto entry vetoes fired on a top-of-book reading the rest of the book contradicted. Turning this on makes the gates steadier but noticeably less likely to veto, so treat it as loosening a safety check — try it on one squadron and compare before applying it everywhere. The GBoost model is not affected; it keeps using best-price depth, which is what it was trained on.")
            );
    }

    {
        let g = "Deployment"; let e: Option<&'static str> = None;
        v.push(F::new(g, e, "deploy_max_days_to_close", "Max Days To Resolution", "secs", false,
            "Furthest-out market a Quick deploy will choose, in days. Browsing is not affected — the market list still shows everything, and you can always deploy a longer-dated market by picking it by hand. This only bounds the automatic choice. It matters because Kalshi structures politics and sports as multi-year futures, and the strategies available to those classes do not suit that horizon: Arbitrage locks your collateral until the market resolves, so a 2028 market ties it up for years to earn a few percent, and Maker rests quotes expecting them to fill and mean-revert within a session. Raise it if you want Quick deploy to consider longer-dated markets.")
            .range(1.0, 3650.0).step(1.0).unit("d"));
        v.push(F::new(g, e, "auto_deploy_politics", "Auto-Deploy Politics", "bool", false,
            "Keep a politics squadron running without waiting for you to deploy one. DRADIS picks \
             the highest-volume politics market inside the resolution horizon above, and replaces \
             it with a fresh one when that market closes. The squadron behaves exactly like one you \
             deployed by hand — same one-per-class rule, same entry in the deployment list. Turn \
             this off to decide for yourself when capital goes to work on the class; a squadron \
             already trading is left alone and runs to its market's close.\n\n\
             There is deliberately no equivalent switch for crypto: this venue's own rotation \
             loop already keeps a crypto squadron running and replaces its market every hour, so \
             seeding one here would produce a second crypto squadron competing with the first for \
             the same capital. Politics and sports have no such loop, which is why they need one.")
            );
        v.push(F::new(g, e, "auto_deploy_sports", "Auto-Deploy Sports", "bool", false,
            "Keep a sports squadron running without waiting for you to deploy one — see Auto-Deploy \
             Politics for how the selection and replacement work. This does not need an Odds API \
             key: the class trades Arbitrage and Maker off the venue's own book, and the Sports \
             Raptor is an additive signal that idles harmlessly when no key is set.")
            );
        v.push(F::new(g, e, "event_market_retire_grace_secs", "Event Market Retire Delay", "secs", false,
            "How long a politics or sports squadron waits after its market closes before standing \
             itself down, in seconds. Standing down is what frees the class: DRADIS runs one \
             squadron per class and the auto-deploy seeder skips a class while a squadron for it \
             is still live, so a squadron that lingers on a resolved market blocks every fresh \
             market behind it. The delay exists because the close time is the venue's, not \
             yours, and a market can still print a late trade around it.\n\n\
             A squadron still holding a position ignores this entirely — it keeps patrolling so \
             its exits keep evaluating, and retires once it is flat. Crypto squadrons never reach \
             this path; this venue's own loop rotates them onto the next market instead.")
            .range(0.0, 86_400.0).step(30.0).unit("s"));
        v.push(F::new(g, e, "position_quote_ttl_secs", "Position Quote Freshness", "secs", true,
            "How long a live price quote for an open position is reused before the venue is asked \
             again. This is what the Trade Log shows you when you are deciding whether to close a \
             position by hand, so it is a trade between freshness and how often DRADIS calls the \
             venue: a second request for the same position inside the window is answered from \
             memory rather than re-asked. It is not request pooling, so two people watching at \
             once can still both trigger a fetch, and at the default the dashboard's own poll \
             usually outruns the window on purpose, because a fresh price is the point. Raise it \
             if you are running many positions and do not need second-by-second prices.")
            .range(1.0, 300.0).step(1.0).unit("s"));
        v.push(F::new(g, e, "llm_max_output_tokens", "AI Reply Budget", "secs", true,
            "Token ceiling for one AI Advisor reply. It covers the whole response — the written \
             analysis and the structured recommendations the engine parses — and the \
             recommendations come last, so a budget that is too small does not shorten them, it \
             removes them: you get readable analysis and nothing to approve. If the advisor keeps \
             reporting analysis with no recommendations, raise this.")
            .range(500.0, 8000.0).step(100.0).unit("tok"));
    }

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
            "Comma-separated bookmaker regions for the odds query: us, us2, uk, eu, au. ⚠️ Not validated — an unrecognized region returns no bookmakers."));
        v.push(F::new(g, e, "tennis_tour", "Tennis Tour", "string", true,
            "Live Tennis API tour filter: atp, wta, challenger, itf, juniors — or blank for all tours. ⚠️ Not validated, and this one fails SILENTLY: a misspelt tour returns an empty match list, which is indistinguishable from tennis being off-season or between sessions. Leave blank if unsure."));
    }

    // ── Build caps narrow the declared range ─────────────────────────────────
    //
    // Three fields are re-clamped to a compile-time constant on every config
    // read, so a value above that constant is written but never honored. The
    // hand-written ranges above are the STRATEGY's bounds and are wider: the
    // schema said `time_decay_max_entry_price` accepted 0.0 to 1.0 while the
    // build capped it at 0.46, which is how an operator came to save 0.5, be
    // told it applied, and watch it read back 0.46 forever.
    //
    // Applied here rather than at each `.range(...)` call so a new cap reaches
    // the UI automatically, and so the strategy's own bound and the build's stay
    // separately readable. Narrowing only — a cap never widens a range, and a
    // field with no declared max gains one.
    // Resolve each field's scope from its group. Unknown groups are left as the
    // constructor default and caught by `every_group_declares_a_scope`, which is
    // louder than silently filing a new field under the wrong config row.
    for f in &mut v {
        if let Some(scope) = scope_for_group(f.group) {
            f.scope = scope;
        }
    }

    for f in &mut v {
        if let Some(cap) = crate::helpers::dynamic_config::build_cap_for(f.key) {
            if let Some(cap) = cap.to_f64() {
                f.max = Some(f.max.map_or(cap, |m: f64| m.min(cap)));
            }
        }
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

    /// Knobs that exist only in Rust are invisible to the operator, which is the
    /// failure this registry exists to prevent: the engine reads them, the
    /// Control Tower cannot show or change them, and the only way to move one is
    /// a recompile. Pinning the newest additions here means adding a
    /// `DynamicConfig` field without registering it fails the build rather than
    /// shipping a knob nobody can reach.
    #[test]
    fn operator_facing_knobs_are_registered() {
        let keys: Vec<&str> = config_schema().iter().map(|f| f.key).collect();
        for key in [
            "maker_min_market_age_secs",
            "maker_maturation_max_fraction",
            "auto_deploy_politics",
            "auto_deploy_sports",
        ] {
            assert!(keys.contains(&key), "`{key}` is not exposed in the Control Tower");
        }
    }

    /// The two auto-deploy switches decide whether DRADIS commits capital to a
    /// market class on its own, so they belong on the Basic panel where an
    /// operator will see them, not behind the Advanced modal.
    #[test]
    fn auto_deploy_switches_are_not_hidden_behind_advanced() {
        for f in config_schema() {
            if f.key.starts_with("auto_deploy_") {
                assert_eq!(f.value_type, "bool", "{} should render as a switch", f.key);
                assert!(!f.advanced, "{} must not be hidden in the Advanced modal", f.key);
                assert_eq!(f.group, "Deployment", "{} must render in a group the UI shows", f.key);
            }
        }
    }

    /// A capped field's slider must stop at the cap, not at the strategy's own
    /// wider bound. `time_decay_max_entry_price` declared 0.0 to 1.0 while the
    /// build honored at most 0.46, so the UI invited operators to save a value
    /// that could never take effect — which one did, on the live Marketplace
    /// instance on 2026-08-29.
    /// Every group must declare a scope. A new group with no entry in
    /// `scope_for_group` silently inherits the constructor default, which is the
    /// "guess and hope" this whole mechanism exists to end.
    #[test]
    fn every_group_declares_a_scope() {
        let mut missing: Vec<&str> = config_schema()
            .iter()
            .filter(|f| scope_for_group(f.group).is_none())
            .map(|f| f.group)
            .collect();
        missing.sort_unstable();
        missing.dedup();
        assert!(
            missing.is_empty(),
            "these groups have no declared scope — add them to scope_for_group: {missing:?}",
        );
    }

    /// The load-bearing test: a field that lives in a SQUADRON row must never be
    /// read from the global one.
    ///
    /// `global_config_tx()` is the global read. Any squadron-scoped key reached
    /// through it is a field the UI writes per-squadron and the engine reads
    /// instance-wide, so operator edits land in a row nobody reads. That is
    /// exactly what `arb_settle_grace_secs` did, and this scan is what would have
    /// caught it — the same source-scanning approach `config_profiles_complete`
    /// already uses for constants.
    #[test]
    fn no_squadron_scoped_field_is_read_from_the_global_row() {
        let squadron_keys: Vec<&str> = config_schema()
            .iter()
            .filter(|f| f.scope == ConfigScope::Squadron)
            .map(|f| f.key)
            .collect();

        let mut offenders: Vec<String> = Vec::new();
        for entry in walk_rs_files("src") {
            let Ok(src) = std::fs::read_to_string(&entry) else { continue };
            // Skip this file: it NAMES every key, it does not read them.
            if entry.ends_with("config_schema.rs") {
                continue;
            }
            for (idx, _) in src.match_indices("global_config_tx()") {
                // The read normally reads `...borrow().some_field` within a short
                // window of the call. Widen only as far as the statement plausibly
                // runs, to avoid matching an unrelated field further down.
                let window = &src[idx..src.len().min(idx + 200)];
                for key in &squadron_keys {
                    if window.contains(&format!(".{key}")) {
                        offenders.push(format!("{}: {key}", entry));
                    }
                }
            }
        }
        offenders.sort();
        offenders.dedup();
        assert!(
            offenders.is_empty(),
            "squadron-scoped field(s) read from the global row — the UI writes these \
             per-squadron, so these edits go to a row nobody reads:\n  {}",
            offenders.join("\n  "),
        );
    }

    /// Recursively collect `.rs` files under `dir`, skipping build output.
    fn walk_rs_files(dir: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![std::path::PathBuf::from(dir)];
        while let Some(p) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&p) else { continue };
            for e in entries.flatten() {
                let path = e.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|x| x == "rs") {
                    out.push(path.to_string_lossy().into_owned());
                }
            }
        }
        out
    }

    #[test]
    fn build_caps_narrow_the_rendered_range() {
        use crate::helpers::dynamic_config::{build_cap_for, BUILD_CAPPED_KEYS};
        use rust_decimal::prelude::ToPrimitive;
        let schema = config_schema();
        for key in BUILD_CAPPED_KEYS {
            let cap = build_cap_for(key).expect("declared").to_f64().expect("finite");
            let f = schema.iter().find(|f| &f.key == key)
                .unwrap_or_else(|| panic!("{key} is capped but absent from the schema"));
            let max = f.max.unwrap_or_else(|| panic!("{key} renders with no upper bound"));
            assert!(max <= cap, "{key} renders a max of {max}, above its {cap} build cap");
        }
    }

    /// Narrowing only. A cap must never widen a range the strategy declared
    /// tighter than the build's own limit.
    #[test]
    fn a_cap_never_widens_a_declared_range() {
        use crate::helpers::dynamic_config::build_cap_for;
        for f in config_schema() {
            if let Some(cap) = build_cap_for(f.key) {
                use rust_decimal::prelude::ToPrimitive;
                if let (Some(max), Some(cap)) = (f.max, cap.to_f64()) {
                    assert!(max <= cap, "{}: max {max} exceeds cap {cap}", f.key);
                }
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
