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

//! Anchor: a market-anchored gamma viper for hourly crypto markets.
//!
//! # Thesis
//!
//! An hourly binary settles on an index the engine never sees (CF Benchmarks on
//! Kalshi, Chainlink on Polymarket), and late in the hour a $15 error in the
//! level is a quarter of the remaining sigma, so a model that prices the market
//! from an external feed plus a fixed basis is noise exactly where the gamma is.
//! Measured on 75 recorded Kalshi hours (337,549 prints), such a model called
//! prints cheap that then won 38% against a 47% price. The market's own price is
//! the better estimate of the level.
//!
//! What the market is slow at is the MOVE. So this viper takes the level from the
//! market and the move from the feed: it reads the index level the market's own
//! mid implies (`implied_basis`), keeps a rolling median of those readings from
//! one to ten minutes ago as the anchor, and prices the contract from the feed's
//! move since then through the contract's own gamma. Prints that model called
//! four cents cheap, bought and held to settlement, returned +2.8c per contract
//! against −3.4c for the prints it called expensive; the edge sat in minutes 30
//! to 45, within one sigma of the strike, on the 0.20 to 0.60 side. A resting
//! take-profit erased it, so the exit is settlement and nothing else.
//!
//! # Venue neutrality
//!
//! Every input is in `StrategyContext`: the oracle spot (already in the venue's
//! settlement denomination, `venues::settlement_denomination`), the market's
//! strike (`MarketConfig::strike_price`, filled by every venue's trader), the
//! book, the close time, and the taker fee through `venues::taker_fee_per_share`.
//! The anchor absorbs whatever offset sits between the feed and the venue's
//! index, which is what makes the same code honest on all three venues. Nothing
//! here names a venue; `vipers::venue_agnostic_guard` enforces that.
//!
//! # Order management
//!
//! The entry is a taker buy of the cheap side at the ask, sent to the squadron's
//! dispatcher as `StrategySignal::Entry` with a fill-and-kill order, exactly as
//! GBoost and FairValue send theirs. The dispatcher decides what that means: in
//! ghost mode it records a simulated fill, otherwise it places the venue order.
//! Nothing here knows the difference, and nothing here ever emits an exit: the
//! position rides to settlement, where the venue's own settlement sweep books
//! the result at $1.00 or $0.00 against this strategy.
//!
//! # The ledger
//!
//! Every fill is also written to the `anchor_shadow` ledger with the model's
//! reading at the moment of the signal (fair, anchor, sigma, z, seconds to
//! close, depth), which the trade list cannot hold and the go/no-go in
//! `docs/spikes/sideways-regime-spike.md` section 5 is computed from. Beside it
//! runs the control lane: the side the model calls EXPENSIVE by the entry
//! margin, recorded on every tick inside the gates and settled the same way,
//! never bought. If the control lane does not lose, the entry lane's gains are
//! the market drifting, not the model knowing.
//!
//! # Simulated settlements
//!
//! A simulated position held through an hourly close has no settlement path in
//! the loops: the Polymarket International patrol clears an expiring market's
//! simulated rows at rotation unscored, and the Kalshi and Polymarket US loops
//! leave them in place. So in ghost mode this viper books its own: the sweep
//! asks the venue's installed resolver (`venues::core::resolve_leg`) what each
//! filled entry settled at and books it through the shared simulated-settlement
//! path (`helpers::ghost_settlement`), or from the ledger when the loop has
//! already dropped the position. A real position is never touched by that path;
//! the venue sweep owns it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tracing::{debug, info, warn};

use crate::config;
use crate::helpers::db::{ANCHOR_LANE_CONTROL, ANCHOR_LANE_ENTRY};
use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::volatility::norm_cdf;
use crate::orchestrator::strategy::{Strategy, StrategyContext};
use crate::state::{OrderParams, PositionKey, StrategySignal, StrategyStatus};
use crate::venues::core::{MarketId, TimeInForce};

pub const STRATEGY_NAME: &str = "AnchorStrategy";
/// Taxonomy kind id (`viper_kind.id`), the Control Tower card's `statusKey`.
pub const KIND: &str = "anchor";

/// Oracle samples feeding the realized-vol estimate are taken this often. The
/// spike's backtest used five-second Binance returns over the trailing hour.
const SIGMA_SAMPLE_SECS: i64 = 5;
/// Length of the realized-vol window.
const SIGMA_WINDOW_SECS: i64 = 3600;
/// Fewest returns the vol estimate is read from; below this the viper is warming
/// up. 200 five-second returns is about seventeen minutes after process start.
const SIGMA_MIN_RETURNS: usize = 200;
/// Implied-basis samples are taken from the book this often.
const ANCHOR_SAMPLE_SECS: i64 = 5;
/// A basis sample is only taken from a mid in this band: outside it the inverse
/// normal is steep and a one-tick spread moves the implied level by hundreds of
/// dollars. The backtest sampled 0.15 to 0.85 and nothing else.
const ANCHOR_SAMPLE_MID_LO: f64 = 0.15;
const ANCHOR_SAMPLE_MID_HI: f64 = 0.85;
/// Widest YES spread a basis sample is read through. A torn book has no mid.
const ANCHOR_SAMPLE_MAX_SPREAD: f64 = 0.06;
/// A snapshot older than this is not a book.
const MAX_SNAPSHOT_AGE_SECS: i64 = 15;
/// The ledger sweep runs this often; the patrol ticks far faster than a
/// settlement arrives.
const SWEEP_SECS: u64 = 10;
/// How long an entry signal waits for its fill to show in the position map
/// before it is forgotten, and how long the same side is not asked for again.
/// A fill-and-kill order that did not fill leaves no position and arms no
/// cooldown in the loop, so without this gate the viper would resend on every
/// 50 ms tick while the snapshot showed depth.
const PENDING_FILL_SECS: u64 = 30;
/// A position still provisional this long after its order is treated as filled
/// for the ledger: a phantom is removed within seconds, and a fill whose venue
/// confirmation arrived before the position was recorded never confirms at all.
const PROVISIONAL_LEDGER_SECS: u64 = 60;
/// How often one closed market is asked for its resolution.
const SHADOW_PROBE_SECS: u64 = 60;
/// A row still unresolved this long after its close is written off unscored.
const SHADOW_GIVE_UP_SECS: i64 = 86_400;
/// Throttle on the gate lines this viper writes to the log.
const GATE_LOG_INTERVAL_SECS: u64 = 300;
/// How often the lane summary on the viper card is refreshed from the ledger.
const SUMMARY_REFRESH_SECS: u64 = 60;
/// √(seconds in a year): per-√second sigma times this is the annualized figure.
const ANNUALIZE: f64 = 5_615.692_3;

// ─── Model ───────────────────────────────────────────────────────────────────

/// Inverse of the standard normal CDF (Acklam's rational approximation,
/// relative error below 1.2e-9). Clamped away from 0 and 1, where the mid band
/// above keeps every caller anyway.
pub fn norm_ppf(p: f64) -> f64 {
    const A: [f64; 6] = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
        1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00];
    const B: [f64; 5] = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
        6.680131188771972e+01, -1.328068155288572e+01];
    const C: [f64; 6] = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
        -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00];
    const D: [f64; 4] = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
        3.754408661907416e+00];
    let p = p.clamp(1e-9, 1.0 - 1e-9);
    let p_low = 0.02425;
    if p < p_low {
        let q = (-2.0 * p.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if p > 1.0 - p_low {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    }
}

/// Realized vol per √second from an irregularly spaced oracle series:
/// √(Σ r² / Σ Δt), the per-second variance of log returns. `None` with fewer
/// than `min_returns` returns.
pub fn sigma_from_samples(samples: &VecDeque<(i64, f64)>, min_returns: usize) -> Option<f64> {
    if samples.len() < min_returns.max(2) + 1 { return None; }
    let (mut sum_sq, mut sum_dt) = (0.0_f64, 0.0_f64);
    for w in samples.iter().zip(samples.iter().skip(1)) {
        let ((t0, p0), (t1, p1)) = (*w.0, *w.1);
        if p0 > 0.0 && p1 > 0.0 && t1 > t0 {
            let r = (p1 / p0).ln();
            sum_sq += r * r;
            sum_dt += (t1 - t0) as f64;
        }
    }
    if sum_dt <= 0.0 { return None; }
    let sigma = (sum_sq / sum_dt).sqrt();
    (sigma > 0.0).then_some(sigma)
}

/// The feed level minus the level the market's YES mid implies under the normal
/// model: `S − K·exp(Φ⁻¹(mid)·σ·√T)`. Positive when the market prices the index
/// below the feed. One sample of the anchor.
pub fn implied_basis(spot: f64, strike: f64, mid_yes: f64, sigma: f64, secs_left: f64) -> Option<f64> {
    if spot <= 0.0 || strike <= 0.0 || sigma <= 0.0 || secs_left <= 0.0 { return None; }
    if !(ANCHOR_SAMPLE_MID_LO < mid_yes && mid_yes < ANCHOR_SAMPLE_MID_HI) { return None; }
    let implied = strike * (norm_ppf(mid_yes) * sigma * secs_left.sqrt()).exp();
    Some(spot - implied)
}

/// The anchor as read at `now_s`, and what it is made of.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnchorRead {
    /// Median implied basis over the eligible samples, in the oracle's units.
    pub basis: f64,
    pub samples: usize,
    /// Seconds between the oldest and newest eligible sample.
    pub span_secs: i64,
    /// Interquartile range of the eligible samples, in the oracle's units. A
    /// level reads tight; a feed that moved inside the window while the market
    /// lagged reads wide, because the early samples carry a different offset
    /// from the late ones. Recorded on every ledger row for review.
    pub spread: f64,
}

/// Median of the basis samples aged between `min_age` and `max_age` seconds,
/// when there are at least `min_samples` of them spanning at least
/// `min_span_secs`. A reading from fewer samples or a shorter span is not an
/// anchor, it is a guess, and the viper holds instead.
pub fn anchor_from(
    samples: &VecDeque<(i64, f64)>, now_s: i64,
    min_age_secs: i64, max_age_secs: i64, min_samples: usize, min_span_secs: i64,
) -> Option<AnchorRead> {
    let mut v: Vec<f64> = Vec::new();
    let (mut t_lo, mut t_hi) = (i64::MAX, i64::MIN);
    for &(t, b) in samples {
        let age = now_s - t;
        if age < min_age_secs || age > max_age_secs { continue; }
        v.push(b);
        t_lo = t_lo.min(t);
        t_hi = t_hi.max(t);
    }
    if v.len() < min_samples.max(1) { return None; }
    let span = t_hi - t_lo;
    if span < min_span_secs { return None; }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    let median = if n % 2 == 1 { v[n / 2] } else { 0.5 * (v[n / 2 - 1] + v[n / 2]) };
    // Quartiles want a few points per quarter; below that the full range is the
    // honest spread.
    let spread = if n >= 8 { v[(3 * n) / 4] - v[n / 4] } else { v[n - 1] - v[0] };
    Some(AnchorRead { basis: median, samples: n, span_secs: span, spread })
}

/// How far the oracle moved over the anchor's sample window: the range of the
/// spot samples aged between `min_age` and `max_age` seconds, in the oracle's
/// units. `None` with fewer than two samples in the window. Recorded beside
/// each ledger row so a large offset can be read against the move that made it.
pub fn spot_range_from(samples: &VecDeque<(i64, f64)>, now_s: i64, min_age_secs: i64, max_age_secs: i64) -> Option<f64> {
    let (mut lo, mut hi, mut n) = (f64::INFINITY, f64::NEG_INFINITY, 0usize);
    for &(t, p) in samples {
        let age = now_s - t;
        if age < min_age_secs || age > max_age_secs { continue; }
        lo = lo.min(p);
        hi = hi.max(p);
        n += 1;
    }
    (n >= 2).then_some(hi - lo)
}

/// The model's reading of one market: fair P(YES) from the anchored level, and
/// the distance of that level from the strike in sigmas.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Reading {
    pub fair_yes: f64,
    pub z: f64,
    /// The anchor's offset from the feed in sigmas of the anchor's own sample
    /// window, `|b| / (σ √W · S)` with W the window length. The level the
    /// market implies normally sits within tens of dollars of the feed, under
    /// half a window sigma; an offset of a window sigma or more means the feed
    /// moved inside the sample window and the market lagged it, so the anchor
    /// is the lag and not the level. Scaled by the window the samples span and
    /// not by the time to close, so the same offset reads the same at every
    /// point in the hour.
    pub basis_sigmas: f64,
}

/// `Φ(ln((S − b)/K) / (σ√T))`, `|ln((S − b)/K)| / (σ√T)` and `|b| / (σ√W·S)`,
/// `W` being the anchor's sample window in seconds.
pub fn reading(spot: f64, strike: f64, basis: f64, sigma: f64, secs_left: f64, window_secs: f64) -> Option<Reading> {
    let level = spot - basis;
    if level <= 0.0 || strike <= 0.0 || sigma <= 0.0 || secs_left <= 0.0 || window_secs <= 0.0 { return None; }
    let d = (level / strike).ln() / (sigma * secs_left.sqrt());
    if !d.is_finite() { return None; }
    Some(Reading { fair_yes: norm_cdf(d), z: d.abs(), basis_sigmas: basis.abs() / (sigma * window_secs.sqrt() * spot) })
}

// ─── Gates ───────────────────────────────────────────────────────────────────

/// One side of the book as the gate reads it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SideQuote {
    /// `None` when the venue publishes no ask on this side.
    pub ask: Option<f64>,
    pub ask_depth: f64,
    /// Order-book imbalance on this side, `[-1, 1]`; -1 is an all-offer book.
    pub obi: f64,
}

/// The entry rule's parameters, read from the knobs once per evaluation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Gates {
    pub min_edge: f64,
    pub max_z: f64,
    /// Nearest to the strike the model will trade; zero disables the gate.
    pub min_z: f64,
    /// Largest anchor offset, in sigmas of the anchor's sample window, the
    /// model will trade on; zero disables the gate.
    pub max_basis_sigmas: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub min_secs_to_close: i64,
    pub max_secs_to_close: i64,
    pub contracts: f64,
    pub max_exposure: f64,
    pub obi_adverse_block: f64,
}

impl Gates {
    pub fn from_config(dc: &DynamicConfig) -> Self {
        let f = |d: Decimal| d.to_f64().unwrap_or(0.0);
        Self {
            min_edge: f(dc.anchor_min_edge),
            max_z: f(dc.anchor_max_z),
            min_z: f(dc.anchor_min_z),
            max_basis_sigmas: f(dc.anchor_max_basis_sigmas),
            min_price: f(dc.anchor_min_entry_price),
            max_price: f(dc.anchor_max_entry_price),
            min_secs_to_close: dc.anchor_min_secs_to_close,
            max_secs_to_close: dc.anchor_max_secs_to_close,
            contracts: f(dc.anchor_contracts),
            max_exposure: f(dc.anchor_max_exposure_usdc),
            obi_adverse_block: f(dc.anchor_obi_adverse_block),
        }
    }
}

/// The side the rule would buy, and the reading it would buy it on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pick {
    pub is_yes: bool,
    pub ask: f64,
    pub fair: f64,
    /// `fair − ask − fee(ask)`.
    pub edge: f64,
    pub fee: f64,
}

/// Why no side qualified this tick. The label is what the Control Tower shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    OutsideWindow,
    TooFarFromStrike,
    TooCloseToStrike,
    AnchorAdrift,
    NoAsk,
    AskOutsideBand,
    EdgeBelowBar,
    AskTooThin,
    ObiAdverse,
    ExposureCap,
}

impl Refusal {
    pub fn label(self) -> &'static str {
        match self {
            Self::OutsideWindow    => "outside the entry window",
            Self::TooFarFromStrike => "level too far from the strike",
            Self::TooCloseToStrike => "level at the strike: the model has no opinion there",
            Self::AnchorAdrift     => "anchor adrift: the feed moved inside the sample window",
            Self::NoAsk            => "no ask on either side",
            Self::AskOutsideBand   => "ask outside the entry price band",
            Self::EdgeBelowBar     => "edge below the bar",
            Self::AskTooThin       => "ask too thin for the size",
            Self::ObiAdverse       => "OBI adverse: no bid support on the cheap side",
            Self::ExposureCap      => "exposure cap reached",
        }
    }
}

/// Apply the entry rule to one tick. Both sides are scored against the same
/// reading (fair NO is `1 − fair YES`); of the sides that clear every gate the
/// larger edge wins. Only the first gate a side fails is reported, from the side
/// that got furthest, so the operator reads the binding constraint and not the
/// first one in the list.
///
/// `fee` is the venue's taker fee per share at a price; `open_notional` is what
/// the lane already has at risk.
pub fn choose_side(
    r: Reading, secs_left: i64, yes: SideQuote, no: SideQuote,
    g: &Gates, open_notional: f64, fee: impl Fn(f64) -> f64,
) -> Result<Pick, Refusal> {
    if secs_left < g.min_secs_to_close || secs_left > g.max_secs_to_close {
        return Err(Refusal::OutsideWindow);
    }
    if r.z > g.max_z {
        return Err(Refusal::TooFarFromStrike);
    }
    // An offset of a window sigma or more is a move the market lagged, not a
    // level; trading on it fades the move (2026-10-23: $124 offset, $211 rally,
    // lost). Checked before the strike gate because a lagged level often lands
    // on the strike too, and this is the more specific cause.
    if anchor_adrift(r.basis_sigmas, g.max_basis_sigmas) {
        return Err(Refusal::AnchorAdrift);
    }
    // At the strike the model prices both sides at 0.50 whatever the anchor
    // reads, so a discount there is the market's directional view and not the
    // model's. The first three live entries under z 0.07 all lost.
    if r.z < g.min_z {
        return Err(Refusal::TooCloseToStrike);
    }
    let mut best: Option<Pick> = None;
    // The furthest any side got, by gate order.
    let mut furthest: Option<(u8, Refusal)> = None;
    let mut note = |rank: u8, why: Refusal| {
        if furthest.is_none_or(|(r, _)| rank > r) { furthest = Some((rank, why)); }
    };
    for (is_yes, q, fair) in [(true, yes, r.fair_yes), (false, no, 1.0 - r.fair_yes)] {
        let Some(ask) = q.ask else { note(0, Refusal::NoAsk); continue };
        if ask < g.min_price || ask >= g.max_price { note(1, Refusal::AskOutsideBand); continue; }
        let fee_at = fee(ask);
        let edge = fair - ask - fee_at;
        if edge < g.min_edge { note(2, Refusal::EdgeBelowBar); continue; }
        if q.ask_depth < g.contracts { note(3, Refusal::AskTooThin); continue; }
        if q.obi < g.obi_adverse_block { note(4, Refusal::ObiAdverse); continue; }
        if open_notional + ask * g.contracts > g.max_exposure { note(5, Refusal::ExposureCap); continue; }
        let pick = Pick { is_yes, ask, fair, edge, fee: fee_at };
        if best.is_none_or(|b| pick.edge > b.edge) { best = Some(pick); }
    }
    best.ok_or_else(|| furthest.map(|(_, why)| why).unwrap_or(Refusal::NoAsk))
}

/// Whether the anchor's offset exceeds the bound; a bound of zero disables the
/// gate, as a zero minimum strike distance does.
pub fn anchor_adrift(basis_sigmas: f64, max_basis_sigmas: f64) -> bool {
    max_basis_sigmas > 0.0 && basis_sigmas > max_basis_sigmas
}

/// The control lane's candidate: a side inside the same price band and window
/// that the model calls EXPENSIVE by the entry margin (`ask − fair ≥ min_edge`,
/// no fee term, the backtest's own control definition). Recorded and settled
/// like an entry, never booked. If the control lane does not lose, the entry
/// lane's gains are the market drifting, not the model knowing.
pub fn control_side(r: Reading, secs_left: i64, yes: SideQuote, no: SideQuote, g: &Gates) -> Option<Pick> {
    // The same structural gates as the entry rule, so the two lanes are drawn
    // from the same moments and stay comparable.
    if secs_left < g.min_secs_to_close || secs_left > g.max_secs_to_close { return None; }
    if r.z > g.max_z || r.z < g.min_z || anchor_adrift(r.basis_sigmas, g.max_basis_sigmas) { return None; }
    let mut best: Option<Pick> = None;
    for (is_yes, q, fair) in [(true, yes, r.fair_yes), (false, no, 1.0 - r.fair_yes)] {
        let Some(ask) = q.ask else { continue };
        if ask < g.min_price || ask >= g.max_price || q.ask_depth < g.contracts { continue; }
        let edge = fair - ask;
        if edge > -g.min_edge { continue; }
        let pick = Pick { is_yes, ask, fair, edge, fee: 0.0 };
        if best.is_none_or(|b| pick.edge < b.edge) { best = Some(pick); }
    }
    best
}

/// The model's reading at the moment an entry was sent, held until the fill
/// shows in the position map so the ledger row carries the price and size that
/// filled rather than the ask that was asked for.
#[derive(Debug, Clone)]
struct PendingEntry {
    squadron_id: String,
    market_key: String,
    condition_id: String,
    market_name: String,
    side: &'static str,
    close_s: i64,
    fair: f64,
    anchor_basis: f64,
    sigma: f64,
    z: f64,
    secs_left: i64,
    spot: f64,
    strike: f64,
    ask_depth: f64,
    /// Spread of the anchor's samples and the feed's range over the sample
    /// window at the moment of entry, for the ledger row.
    basis_spread: f64,
    spot_range: Option<f64>,
    /// The mode the order was sent in (`OrderParams::ghost_mode`), stamped on
    /// the ledger row so a switch flipped before the fill is ledgered cannot
    /// change who books the settlement.
    ghost: bool,
    sent_at: Instant,
}

/// This viper's notional in `positions` for `squadron` that the ledger does not
/// yet know: provisional fills and fills whose row is not written yet. Added to
/// the ledger's own open notional, this is everything the viper has bought and
/// not yet settled, which is what the exposure cap bounds. A position the
/// ledger already holds is counted there and skipped here.
pub fn unledgered_notional(positions: &crate::state::PositionMap, squadron: &str, ledgered: &HashSet<String>) -> f64 {
    positions.iter()
        .filter(|(k, _)| k.squadron == squadron && k.strategy == STRATEGY_NAME && !ledgered.contains(k.market.as_str()))
        .map(|(_, p)| (p.shares * p.avg_entry).to_f64().unwrap_or(0.0))
        .sum()
}

/// Who booked a filled entry's settlement. See `AnchorStrategy::book_settlement`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Booked {
    /// A real position: the venue's settlement sweep writes the trade.
    Venue,
    /// A simulated position still in the map, booked through the shared path.
    Simulated,
    /// A simulated position the loop had already dropped, booked from the ledger.
    Ledger,
    /// Could not tell yet; the row stays open for the next sweep.
    Deferred,
}

// ─── Per-asset state ─────────────────────────────────────────────────────────

struct AnchorState {
    samples: VecDeque<(i64, f64)>,
    last_sample_at: i64,
    touched_at: i64,
    /// The anchor as last computed, and when. Recomputed when a sample lands or
    /// every `ANCHOR_SAMPLE_SECS` as the samples age, not on every 50 ms tick.
    last_read: Option<AnchorRead>,
    last_read_at: i64,
    /// Logged once when the anchor first becomes readable on this market.
    announced: bool,
}

#[derive(Default)]
struct Globals {
    /// `(unix secs, oracle spot)`, one per `SIGMA_SAMPLE_SECS`, trailing hour.
    sigma: Mutex<VecDeque<(i64, f64)>>,
    /// Sigma as last computed from the series above, with the return count.
    sigma_cache: Mutex<(Option<f64>, usize)>,
    /// Keyed by the market (`gboost_planb::market_key`): a rotation starts a
    /// fresh anchor.
    anchors: Mutex<HashMap<String, AnchorState>>,
    /// `(lane, market key, side)` of every open ledger row, so the entry path
    /// asks memory and not the database on every tick. Filled on open and
    /// rebuilt from the ledger by each sweep.
    held: Mutex<HashSet<(String, String, String)>>,
    /// Tokens of the entry lane's open rows, rebuilt by each sweep: a fill in
    /// the ledger is counted once, from the ledger, by the exposure rule.
    ledgered: Mutex<HashSet<String>>,
    /// Σ price × size of the entry lane's open rows (unsettled fills, on open
    /// markets and closed ones alike), rebuilt by each sweep and bumped when a
    /// row is written. The venue-neutral exposure figure: the position map is
    /// per market on some venues and does not survive the close.
    open_notional: Mutex<f64>,
    /// Entries sent and not yet seen filled, by token: the reading that goes
    /// into the ledger row once the fill shows in the position map.
    pending: Mutex<HashMap<String, PendingEntry>>,
    /// `(market key, side)` → close time of every side filled this session, so
    /// a side is bought once per hour even after settlement has released the
    /// position from the map. Pruned a day after the close.
    attempted: Mutex<HashMap<(String, String), i64>>,
    shadow_resolved: Mutex<HashMap<String, f64>>,
    shadow_probed: Mutex<HashMap<String, Instant>>,
    last_sweep: Mutex<Option<Instant>>,
    summary: Mutex<(Option<Instant>, String)>,
}

fn registry() -> &'static Mutex<HashMap<String, &'static Globals>> {
    static R: OnceLock<Mutex<HashMap<String, &'static Globals>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashMap::new()))
}

fn globals(asset: &str) -> &'static Globals {
    let mut map = lock(registry());
    if let Some(g) = map.get(asset) { return g; }
    let g: &'static Globals = Box::leak(Box::default());
    map.insert(asset.to_string(), g);
    g
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// Feed the vol sampler and read sigma. Called before any structural gate, so
/// warmup runs while the market is between hours; the caller has already
/// checked the snapshot is fresh, so a frozen oracle adds no zero returns.
/// Sigma is recomputed only when a sample lands.
fn update_and_read_sigma(g: &Globals, now_s: i64, spot: f64) -> (Option<f64>, usize) {
    let mut s = lock(&g.sigma);
    if s.back().is_some_and(|(t, _)| now_s - t < SIGMA_SAMPLE_SECS) {
        return *lock(&g.sigma_cache);
    }
    s.push_back((now_s, spot));
    while s.front().is_some_and(|(t, _)| now_s - t > SIGMA_WINDOW_SECS) {
        s.pop_front();
    }
    let out = (sigma_from_samples(&s, SIGMA_MIN_RETURNS), s.len().saturating_sub(1));
    *lock(&g.sigma_cache) = out;
    out
}

/// Take a basis sample from the book if one is due, then read the anchor.
#[allow(clippy::too_many_arguments)]
fn sample_and_read_anchor(
    g: &Globals, market_key: &str, now_s: i64, dc: &DynamicConfig,
    spot: f64, strike: f64, mid_yes: Option<f64>, sigma: f64, secs_left: f64,
) -> (Option<AnchorRead>, usize) {
    let mut map = lock(&g.anchors);
    // A market the squadron left more than two hours ago has no anchor to keep.
    map.retain(|_, st| now_s - st.touched_at < 7200);
    let st = map.entry(market_key.to_string()).or_insert_with(|| AnchorState {
        samples: VecDeque::new(), last_sample_at: 0, touched_at: now_s,
        last_read: None, last_read_at: 0, announced: false,
    });
    st.touched_at = now_s;
    let mut landed = false;
    if now_s - st.last_sample_at >= ANCHOR_SAMPLE_SECS {
        if let Some(b) = mid_yes.and_then(|m| implied_basis(spot, strike, m, sigma, secs_left)) {
            st.samples.push_back((now_s, b));
            st.last_sample_at = now_s;
            landed = true;
        }
    }
    if landed || now_s - st.last_read_at >= ANCHOR_SAMPLE_SECS {
        let max_age = dc.anchor_max_age_secs;
        while st.samples.front().is_some_and(|(t, _)| now_s - t > max_age) {
            st.samples.pop_front();
        }
        st.last_read = anchor_from(
            &st.samples, now_s, dc.anchor_min_age_secs, max_age,
            dc.anchor_min_samples.max(1) as usize, dc.anchor_min_span_secs,
        );
        st.last_read_at = now_s;
        if let Some(a) = st.last_read {
            if !st.announced {
                st.announced = true;
                info!(
                    "⚓ Anchor ready on {}: market level sits {:+.0} from the feed ({} samples over {}s)",
                    market_key, -a.basis, a.samples, a.span_secs,
                );
            }
        }
    }
    (st.last_read, st.samples.len())
}

fn held_key(lane: &str, market_key: &str, side: &str) -> (String, String, String) {
    (lane.to_string(), market_key.to_string(), side.to_string())
}

// ─── The viper ───────────────────────────────────────────────────────────────

pub struct AnchorStrategy;

impl Default for AnchorStrategy {
    fn default() -> Self { Self::new() }
}

impl AnchorStrategy {
    pub fn new() -> Self { Self }

    /// Mid of the YES book when both sides are quoted and the spread is a price.
    fn yes_mid(snap: &crate::state::MarketSnapshot) -> Option<f64> {
        if !snap.yes_has_ask() || snap.yes_bid <= Decimal::ZERO { return None; }
        let (bid, ask) = (snap.yes_bid.to_f64()?, snap.yes_ask.to_f64()?);
        if ask <= bid || ask - bid > ANCHOR_SAMPLE_MAX_SPREAD { return None; }
        Some(0.5 * (bid + ask))
    }

    fn side_quote(snap: &crate::state::MarketSnapshot, is_yes: bool) -> SideQuote {
        if is_yes {
            SideQuote {
                ask: snap.yes_has_ask().then(|| snap.yes_ask.to_f64()).flatten(),
                ask_depth: snap.yes_ask_depth.to_f64().unwrap_or(0.0),
                obi: snap.yes_obi(false).to_f64().unwrap_or(-1.0),
            }
        } else {
            SideQuote {
                ask: snap.no_has_ask().then(|| snap.no_ask.to_f64()).flatten(),
                ask_depth: snap.no_ask_depth.to_f64().unwrap_or(0.0),
                obi: snap.no_obi(false).to_f64().unwrap_or(-1.0),
            }
        }
    }

    /// Write the ledger row for every sent entry whose fill has shown in the
    /// position map, at the price and size that filled, and forget signals whose
    /// order never filled. Runs on every exit tick, unthrottled, because a fill
    /// that settled before its row was written would be a trade with no reading.
    async fn ledger_fills(&self, ctx: &StrategyContext) {
        let asset = ctx.crypto_filter.to_lowercase();
        let g = globals(&asset);
        if lock(&g.pending).is_empty() { return; }
        let ghost = ctx.dynamic_config.ghost_mode;
        let filled: Vec<(String, PendingEntry, f64, f64)> = {
            let positions = ctx.positions.lock().await;
            let mut pending = lock(&g.pending);
            let mut out = Vec::new();
            pending.retain(|token, pe| {
                let key = PositionKey::new(&pe.squadron_id, STRATEGY_NAME, MarketId::new(token));
                match positions.get(&key) {
                    // A confirmed fill owns the shares the row claims. A position
                    // still provisional a minute on is one whose confirmation
                    // the venue will not echo (a phantom would have been removed
                    // by now), so it is ledgered rather than lost.
                    Some(p) if p.fill_effective_at(ghost).is_some()
                        || pe.sent_at.elapsed().as_secs() >= PROVISIONAL_LEDGER_SECS =>
                    {
                        out.push((token.clone(), pe.clone(), p.avg_entry.to_f64().unwrap_or(0.0), p.shares.to_f64().unwrap_or(0.0)));
                        false
                    }
                    Some(_) => true,
                    None if pe.sent_at.elapsed().as_secs() >= PENDING_FILL_SECS => {
                        debug!("⚓ Anchor {} entry on {} never filled; forgotten", pe.side, pe.market_name);
                        false
                    }
                    None => true,
                }
            });
            out
        };
        if filled.is_empty() { return; }
        let Some(pool) = crate::helpers::db::pool_for(&asset) else { return };
        for (token, pe, price, shares) in filled {
            // The side is spent the moment it fills, whatever the ledger says.
            lock(&g.attempted).insert((pe.market_key.clone(), pe.side.to_string()), pe.close_s);
            let key = held_key(ANCHOR_LANE_ENTRY, &pe.market_key, pe.side);
            if lock(&g.held).contains(&key) { continue; }
            // The model's fee at the fill price, the same schedule the edge was
            // measured against; the venue's actual fee is on the trade row.
            let fee = crate::venues::taker_fee_per_share(Decimal::try_from(price).unwrap_or(Decimal::ZERO)).to_f64().unwrap_or(0.0);
            let ok = crate::helpers::db::anchor_shadow_open(
                &pool, &asset, ANCHOR_LANE_ENTRY, &pe.squadron_id, &pe.market_key, &pe.condition_id, &token,
                &pe.market_name, pe.side, pe.close_s, price, shares, fee, pe.fair, pe.anchor_basis,
                pe.sigma, pe.z, pe.secs_left, pe.spot, pe.strike, pe.ask_depth, Some(pe.ghost),
                Some(pe.basis_spread), pe.spot_range,
            ).await;
            if ok {
                lock(&g.held).insert(key);
                lock(&g.ledgered).insert(token.clone());
                *lock(&g.open_notional) += price * shares;
                info!(
                    "⚓ Anchor {} filled on {}: {} contracts at ${:.3} (fair {:.3}, z {:.2}, T {}s){}; holding to settlement",
                    pe.side, pe.market_name, shares, price, pe.fair, pe.z, pe.secs_left,
                    if pe.ghost { ", simulated" } else { "" },
                );
            } else {
                warn!("⚓ Anchor ledger write failed for the {} fill on {}; the position is managed, the reading is lost", pe.side, pe.market_name);
            }
        }
    }

    /// Book the settlement of one filled entry, by the mode the fill was made in
    /// (`row.ghost`, stamped at fill time), never by the mode at the close: an
    /// operator who flips the ghost switch mid-hold must not get a position
    /// booked twice or not at all.
    ///
    /// * A real fill is the venue sweep's to book from the exchange's own
    ///   result; nothing here writes a trade for it. So is any token a real
    ///   `open_positions` row still claims, whatever the ledger says.
    /// * A simulated fill still in the position map (the Polymarket
    ///   International patrol keeps it as a zombie after rotation) is booked
    ///   through the shared simulated-settlement path, which writes the trade,
    ///   closes the position's row and releases it from the map.
    /// * A simulated fill the loop has already dropped (the Kalshi and Polymarket
    ///   US maps are per market and die at the close) is booked from the ledger,
    ///   the only record left, and its leaked simulated row is closed.
    ///
    /// Both simulated paths are idempotent: one buy per side per hour means
    /// (market, side) identifies the position, and a settlement already on the
    /// trade list for it is not written again. "Could not tell" defers: the
    /// ledger row stays open and the next sweep asks again.
    async fn book_settlement(
        positions: &std::sync::Arc<tokio::sync::Mutex<crate::state::PositionMap>>,
        pool: &sqlx::SqlitePool,
        asset: &str,
        market_class: Option<&str>,
        row: &crate::helpers::db::AnchorShadow,
        px: f64,
        pnl: f64,
    ) -> Booked {
        use crate::helpers::db;
        if row.ghost == Some(false) {
            return Booked::Venue;
        }
        match db::open_row_claims_token_in(pool, &row.token_id).await {
            Ok(true) => return Booked::Venue,
            Ok(false) => {}
            Err(e) => {
                warn!("⚓ Anchor could not tell whether {} is a real position ({e}); settlement deferred", row.token_id);
                return Booked::Deferred;
            }
        }
        match db::anchor_settlement_booked(pool, STRATEGY_NAME, &row.market, &row.side, true, &row.opened_at).await {
            // Written by an earlier pass whose ledger close did not land.
            Ok(true) => return Booked::Ledger,
            Ok(false) => {}
            Err(e) => {
                warn!("⚓ Anchor could not read the trade list for {} {} ({e}); settlement deferred", row.market, row.side);
                return Booked::Deferred;
            }
        }
        let d = |x: f64| Decimal::try_from(x).unwrap_or(Decimal::ZERO);
        let token = MarketId::new(&row.token_id);
        let key = PositionKey::new(&row.squadron_id, STRATEGY_NAME, token.clone());
        let candidate = positions.lock().await.get(&key).filter(|p| p.shares > Decimal::ZERO).map(|p| (key.clone(), p.clone()));
        if let Some(candidate) = candidate {
            let source = if px == 0.5 { crate::helpers::ghost_settlement::PriceSource::Tie } else { crate::helpers::ghost_settlement::PriceSource::Resolved };
            let prices = HashMap::from([(row.token_id.clone(), (d(px), source))]);
            // The shared path labels the side by comparing the token to the YES
            // leg and never reads the NO leg; the ledger knows only this token.
            let blank = MarketId::new("");
            let (yes_leg, no_leg) = if row.side == "YES" { (token.clone(), blank) } else { (blank, token.clone()) };
            let n = crate::helpers::ghost_settlement::book_ghost_settlements(
                positions, pool, asset, market_class, &yes_leg, &no_leg, &prices, vec![candidate],
            ).await;
            return if n > 0 { Booked::Simulated } else { Booked::Deferred };
        }
        let outcome = if px == 0.5 { "tied or canceled" } else if px > 0.5 { "won" } else { "lost" };
        let mut scope = crate::state::TradeScope::crypto(asset, "", asset);
        scope.ghost = true;
        let wrote = db::record_settlement_trade_for_token(
            pool, &scope, Some((&row.token_id, db::opened_at_or_dawn(&row.opened_at))), STRATEGY_NAME, &row.market, &row.side,
            d(row.entry_price), d(px), d(row.shares), d(pnl), d(row.entry_fee * row.shares),
            &format!("Settlement ({outcome} — simulated)"), None,
        ).await;
        if wrote == db::SettlementWrite::Failed {
            return Booked::Deferred;
        }
        // The simulated row the loop left behind, if any. No real row claims
        // this token (checked above), so only a simulated one can match.
        db::close_open_position(pool, STRATEGY_NAME, &row.token_id).await;
        Booked::Ledger
    }

    /// Settle the ledger's open rows through the venue's resolver, rebuild the
    /// in-memory holds from the ledger, and refresh the card's summary line.
    /// Runs whether or not the viper is enabled: a row opened while it was on
    /// is owed its settlement.
    async fn sweep_ledger(&self, ctx: &StrategyContext) {
        let asset = ctx.crypto_filter.to_lowercase();
        let g = globals(&asset);
        {
            let mut last = lock(&g.last_sweep);
            if last.is_some_and(|t| t.elapsed().as_secs() < SWEEP_SECS) { return; }
            *last = Some(Instant::now());
        }
        let Some(pool) = crate::helpers::db::pool_for(&asset) else { return };
        let now_s = Utc::now().timestamp();
        lock(&g.attempted).retain(|_, close_s| now_s < *close_s + SHADOW_GIVE_UP_SECS);
        // A failed read keeps the previous holds rather than clearing them,
        // which would let a fill be ledgered twice.
        if let Some(rows) = crate::helpers::db::anchor_shadow_open_rows(&pool, &asset).await {
            *lock(&g.held) = rows.iter().map(|r| held_key(&r.lane, &r.market_key, &r.side)).collect();
            let entries = rows.iter().filter(|r| r.lane == ANCHOR_LANE_ENTRY);
            *lock(&g.ledgered) = entries.clone().map(|r| r.token_id.clone()).collect();
            *lock(&g.open_notional) = entries.map(|r| r.entry_price * r.shares).sum();
            for row in rows {
                if now_s < row.close_s { continue; }
                let resolved = lock(&g.shadow_resolved).get(&row.token_id).copied();
                let Some(px) = resolved else {
                    // Through the venue's installed resolver, spawned: a venue
                    // call may take seconds and the patrol tick is waiting.
                    let due = {
                        let mut probed = lock(&g.shadow_probed);
                        probed.retain(|_, at| at.elapsed().as_secs() < 7200);
                        let due = probed.get(&row.token_id).is_none_or(|at| at.elapsed().as_secs() >= SHADOW_PROBE_SECS);
                        if due { probed.insert(row.token_id.clone(), Instant::now()); }
                        due
                    };
                    if due {
                        let (asset_c, cid, token) = (asset.clone(), row.condition_id.clone(), row.token_id.clone());
                        tokio::spawn(async move {
                            let got = crate::venues::core::resolve_leg(&cid, &MarketId::new(&token)).await;
                            if let Some(crate::venues::core::TokenResolution::Resolved(px)) = got {
                                if let Some(px) = px.to_f64() {
                                    let mut out = lock(&globals(&asset_c).shadow_resolved);
                                    out.insert(token, px);
                                    if out.len() > 512 { out.clear(); }
                                }
                            }
                        });
                    }
                    if now_s >= row.close_s + SHADOW_GIVE_UP_SECS
                        && crate::helpers::db::anchor_shadow_abandon(&pool, row.id, "unresolved").await
                    {
                        warn!(
                            "⚓ Anchor ledger row written off unscored [{}] {}: the venue never resolved it \
                             {}h after close, so it counts as no trade rather than as a guess",
                            row.market, row.side, (now_s - row.close_s) / 3600,
                        );
                    }
                    continue;
                };
                lock(&g.shadow_resolved).remove(&row.token_id);
                // Book and close in one spawned task, so the 500 ms strategy
                // timeout cannot drop the ledger row between the two writes.
                let (pool_c, asset_c, positions_c, class_c) = (pool.clone(), asset.clone(), ctx.positions.clone(), ctx.market_class.clone());
                tokio::spawn(async move {
                    // Settlement pays no exit fee; the entry fee was charged at the ask.
                    let per_contract = px - row.entry_price - row.entry_fee;
                    let pnl = per_contract * row.shares;
                    if row.lane != ANCHOR_LANE_ENTRY {
                        if crate::helpers::db::anchor_shadow_close(&pool_c, row.id, px, "settlement", pnl).await {
                            info!("⚓ Anchor control settled [{}] {} at ${:.3}, resolved ${:.2}: {:+.2}c per contract (never taken)",
                                  row.market, row.side, row.entry_price, px, per_contract * 100.0);
                        }
                        return;
                    }
                    let booked = Self::book_settlement(&positions_c, &pool_c, &asset_c, class_c.as_deref(), &row, px, pnl).await;
                    let reason = match booked {
                        Booked::Deferred => {
                            // Resolved but not booked: the next sweep asks the
                            // resolver again and lands here with the answer.
                            return;
                        }
                        Booked::Venue => "settlement (booked by the venue sweep)",
                        Booked::Simulated => "settlement (simulated)",
                        Booked::Ledger => "settlement (simulated, booked from the ledger)",
                    };
                    if !crate::helpers::db::anchor_shadow_close(&pool_c, row.id, px, reason, pnl).await {
                        return;
                    }
                    info!(
                        "⚓ Anchor settled [{}] {} bought at ${:.3}, resolved ${:.2}: {:+.2}c per contract, {:+.2} USD ({})",
                        row.market, row.side, row.entry_price, px, per_contract * 100.0, pnl, reason,
                    );
                });
            }
        }
        let refresh = lock(&g.summary).0.is_none_or(|t| t.elapsed().as_secs() >= SUMMARY_REFRESH_SECS);
        if refresh {
            let s = crate::helpers::db::anchor_shadow_summary(&pool, &asset, ANCHOR_LANE_ENTRY).await;
            let c = crate::helpers::db::anchor_shadow_summary(&pool, &asset, ANCHOR_LANE_CONTROL).await;
            let control = if c.closed == 0 { String::new() } else {
                format!(" | control: {} settled, {:+.1}c per contract", c.closed, 100.0 * c.net_per_contract)
            };
            let mode = if ctx.dynamic_config.ghost_mode { "simulated" } else { "live" };
            let line = if s.closed == 0 {
                format!("{mode}: {} open, none settled yet{control}", s.open)
            } else {
                format!(
                    "{mode}: {} settled, {:.0}% won, {:+.1}c per contract, {:+.2} USD, {} open{control}",
                    s.closed, 100.0 * s.wins as f64 / s.closed as f64,
                    100.0 * s.net_per_contract, s.net_usd, s.open,
                )
            };
            *lock(&g.summary) = (Some(Instant::now()), line);
        }
    }
}

#[async_trait]
impl Strategy for AnchorStrategy {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        let idle = |r: &str| crate::helpers::viper_status::report_reason(&ctx.crypto_filter, STRATEGY_NAME, r);
        if !dc.anchor_enabled {
            idle("disabled in config");
            return Ok(StrategySignal::NoSignal);
        }
        // A sports or politics squadron never instantiates this kind; the guard
        // is for a context built without a classified squadron.
        if ctx.sports.is_some() || ctx.market_class.as_deref().is_some_and(|c| c != "crypto") {
            idle("not a crypto market");
            return Ok(StrategySignal::NoSignal);
        }
        if !ctx.has_price_feed {
            idle("no price feed for this squadron");
            return Ok(StrategySignal::NoSignal);
        }
        let asset = ctx.crypto_filter.to_lowercase();
        let g = globals(&asset);
        let now = Utc::now();
        let now_s = now.timestamp();
        if (now - ctx.snapshot.timestamp).num_seconds() > MAX_SNAPSHOT_AGE_SECS {
            idle("snapshot stale");
            return Ok(StrategySignal::NoSignal);
        }

        // Vol sampler before the structural gates, so warmup runs between hours.
        let spot = match ctx.snapshot.oracle_price.to_f64() {
            Some(s) if s > 0.0 => s,
            _ => { idle("no oracle price"); return Ok(StrategySignal::NoSignal) },
        };
        let (sigma, returns) = update_and_read_sigma(g, now_s, spot);

        let market = &ctx.market;
        // An unknown window is unknown, not hourly (B43).
        if market.window_secs != Some(3600) {
            idle(if market.window_secs.is_none() { "market window length unknown" } else { "not an hourly market" });
            return Ok(StrategySignal::NoSignal);
        }
        let strike = match market.strike_price.and_then(|k| k.to_f64()) {
            Some(k) if k > 0.0 => k,
            _ => {
                let pre_open = market.market_close_time.is_some_and(|c| (c - now).num_seconds() > 3600);
                idle(if pre_open { "window not open yet, no strike until the open" } else { "market has no strike price" });
                return Ok(StrategySignal::NoSignal);
            }
        };
        let Some(secs_left) = market.secs_to_close(now) else {
            idle("market has no close time");
            return Ok(StrategySignal::NoSignal);
        };
        if secs_left <= 0 {
            idle("market closed");
            return Ok(StrategySignal::NoSignal);
        }
        let Some(sigma) = sigma else {
            idle(&format!("vol warmup {returns}/{SIGMA_MIN_RETURNS} returns"));
            return Ok(StrategySignal::NoSignal);
        };

        // The regime gate. A fast tape is repriced by the market faster than a
        // one-to-ten-minute anchor can follow, and the model inverts; neither
        // lane records anything there, so the record measures the regime it is
        // meant for. The anchor is still fed, so a tape that calms mid-hour
        // finds an anchor waiting.
        let vol_ann = sigma * ANNUALIZE;
        let vol_cap = dc.anchor_max_vol_annualized.to_f64().unwrap_or(f64::MAX);
        // The anchor is fed on every tick inside the hour so it is readable by
        // the time the entry window opens.
        let market_key = crate::vipers::gboost_planb::market_key(market);
        let (anchor, n_samples) = sample_and_read_anchor(
            g, &market_key, now_s, dc, spot, strike,
            Self::yes_mid(&ctx.snapshot), sigma, secs_left as f64,
        );
        let gates = Gates::from_config(dc);
        if secs_left > gates.max_secs_to_close || secs_left < gates.min_secs_to_close {
            idle("outside the entry window");
            return Ok(StrategySignal::NoSignal);
        }
        if vol_ann > vol_cap {
            idle(&format!("realized vol {:.0}% above the {:.0}% regime cap", vol_ann * 100.0, vol_cap * 100.0));
            return Ok(StrategySignal::NoSignal);
        }
        let Some(anchor) = anchor else {
            // Say why there is nothing to read when the book itself is the
            // reason: a market deep in or out of the money has no two-sided mid
            // inside the sampling band, so "warming" would never end.
            let mid = Self::yes_mid(&ctx.snapshot);
            idle(&match mid {
                None if n_samples == 0 => "no anchor: the YES book has no readable two-sided mid".to_string(),
                Some(m) if n_samples == 0 && !(ANCHOR_SAMPLE_MID_LO..=ANCHOR_SAMPLE_MID_HI).contains(&m) =>
                    format!("no anchor: YES mid {m:.2} is too far from the strike to read"),
                _ => format!("anchor warming: {n_samples} samples"),
            });
            return Ok(StrategySignal::NoSignal);
        };
        let Some(r) = reading(spot, strike, anchor.basis, sigma, secs_left as f64, dc.anchor_max_age_secs as f64) else {
            idle("fair value not computable");
            return Ok(StrategySignal::NoSignal);
        };
        // The feed's own range over the anchor's sample window, from the vol
        // sampler's spot series: the context a large offset is read against.
        let spot_range = spot_range_from(&lock(&g.sigma), now_s, dc.anchor_min_age_secs, dc.anchor_max_age_secs);
        let yes = Self::side_quote(&ctx.snapshot, true);
        let no = Self::side_quote(&ctx.snapshot, false);
        crate::helpers::viper_status::report_detail(&ctx.crypto_filter, STRATEGY_NAME, Some(format!(
            "anchor {:+.0} ({} samples, spread {:.0}, {:.2} window σ) | feed range {} | fair YES {:.3} vs ask {} | z {:.2} | vol {:.0}% | T {}s | {}",
            -anchor.basis, anchor.samples, anchor.spread, r.basis_sigmas,
            spot_range.map_or("n/a".to_string(), |x| format!("{x:.0}")),
            r.fair_yes,
            yes.ask.map_or("n/a".to_string(), |a| format!("{a:.3}")),
            r.z, vol_ann * 100.0, secs_left, lock(&g.summary).1,
        )));

        let Some(pool) = crate::helpers::db::pool_for(&asset) else {
            idle("no database for the ledger");
            return Ok(StrategySignal::NoSignal);
        };
        // The venue's taker fee at a price. Per-market fee bps
        // (`MarketConfig::{yes,no}_fee_bps`) and a venue's per-order rounding are
        // not applied; on the quadratic venues the two agree to within a tenth
        // of a cent per contract at ten contracts.
        let fee = |p: f64| crate::venues::taker_fee_per_share(Decimal::try_from(p).unwrap_or(Decimal::ZERO))
            .to_f64().unwrap_or(0.0);
        let close_s = now_s + secs_left;

        // What this viper already holds: one position per market, from the map
        // the dispatcher fills; and the cap over every unsettled fill, from the
        // ledger (which outlives the map) plus anything the ledger has not seen.
        let open_notional = {
            let positions = ctx.positions.lock().await;
            let mine = |tok: &MarketId| positions.get(&PositionKey::new(&ctx.squadron_id, STRATEGY_NAME, tok.clone()));
            let held = [(mine(&market.yes_token), "YES"), (mine(&market.no_token), "NO")];
            if held.iter().any(|(p, _)| p.is_some()) {
                // Only a confirmed fill spends the side: a provisional position
                // can still be removed as a phantom, and that entry should stay
                // retryable.
                let confirmed = held.iter().filter(|(p, _)| p.is_some_and(|p| p.fill_effective_at(dc.ghost_mode).is_some()));
                for (_, side) in confirmed {
                    lock(&g.attempted).insert((market_key.clone(), side.to_string()), close_s);
                }
                idle("position already open on this market");
                return Ok(StrategySignal::NoSignal);
            }
            *lock(&g.open_notional) + unledgered_notional(&positions, &ctx.squadron_id, &lock(&g.ledgered))
        };
        let pick = choose_side(r, secs_left, yes, no, &gates, open_notional, fee);

        // The control lane is sampled on EVERY tick inside the gates, whatever
        // the entry rule decided, so it is drawn from the same population of
        // moments as the entries it is meant to falsify.
        if let Some(c) = control_side(r, secs_left, yes, no, &gates) {
            let side = if c.is_yes { "YES" } else { "NO" };
            let key = held_key(ANCHOR_LANE_CONTROL, &market_key, side);
            if !lock(&g.held).contains(&key) {
                let token = if c.is_yes { &market.yes_token } else { &market.no_token };
                let depth = if c.is_yes { yes.ask_depth } else { no.ask_depth };
                let fee_at = fee(c.ask);
                if crate::helpers::db::anchor_shadow_open(
                    &pool, &asset, ANCHOR_LANE_CONTROL, &ctx.squadron_id, &market_key, &market.condition_id,
                    token.as_str(), &market.market_name, side, close_s, c.ask, gates.contracts, fee_at, c.fair,
                    anchor.basis, sigma, r.z, secs_left, spot, strike, depth, None, Some(anchor.spread), spot_range,
                ).await {
                    lock(&g.held).insert(key);
                    info!("⚓ Anchor control on {}: {} at ${:.3} is {:.3} over fair {:.3}; recorded, never taken",
                          market.market_name, side, c.ask, -c.edge, c.fair);
                }
            }
        }

        let pick = match pick {
            Ok(p) => p,
            Err(why) => {
                idle(why.label());
                if matches!(why, Refusal::AskTooThin | Refusal::ObiAdverse | Refusal::ExposureCap | Refusal::AnchorAdrift | Refusal::TooCloseToStrike)
                    && crate::vipers::gate_log_permitted(STRATEGY_NAME, &asset, why.label(), GATE_LOG_INTERVAL_SECS)
                {
                    info!("⚓ Anchor hold on {}: {} (fair YES {:.3}, z {:.2}, T {}s)",
                          market.market_name, why.label(), r.fair_yes, r.z, secs_left);
                }
                return Ok(StrategySignal::NoSignal);
            }
        };
        let side: &'static str = if pick.is_yes { "YES" } else { "NO" };
        // One buy per side per hour, whether the position is still in the map,
        // already settled out of it, or on the ledger from an earlier session.
        if lock(&g.attempted).contains_key(&(market_key.clone(), side.to_string()))
            || lock(&g.held).contains(&held_key(ANCHOR_LANE_ENTRY, &market_key, side))
        {
            idle("already bought this side this hour");
            return Ok(StrategySignal::NoSignal);
        }
        let token = if pick.is_yes { market.yes_token.clone() } else { market.no_token.clone() };
        // An order already out for this side waits for its fill or its forgetting
        // (`PENDING_FILL_SECS`) before the side is asked for again.
        if lock(&g.pending).get(token.as_str()).is_some_and(|pe| pe.sent_at.elapsed().as_secs() < PENDING_FILL_SECS) {
            idle("entry sent, awaiting the fill");
            return Ok(StrategySignal::NoSignal);
        }
        // The venue's own quote, not the f64 the rule priced from.
        let ask_dec = if pick.is_yes { ctx.snapshot.yes_ask } else { ctx.snapshot.no_ask };
        let fee_bps = (if pick.is_yes { market.yes_fee_bps } else { market.no_fee_bps }) as u16;
        let depth = if pick.is_yes { yes.ask_depth } else { no.ask_depth };
        lock(&g.pending).insert(token.as_str().to_string(), PendingEntry {
            squadron_id: ctx.squadron_id.clone(),
            market_key: market_key.clone(),
            condition_id: market.condition_id.clone(),
            market_name: market.market_name.clone(),
            side,
            close_s,
            fair: pick.fair,
            anchor_basis: anchor.basis,
            sigma,
            z: r.z,
            secs_left,
            spot,
            strike,
            ask_depth: depth,
            basis_spread: anchor.spread,
            spot_range,
            ghost: dc.ghost_mode,
            sent_at: Instant::now(),
        });
        crate::helpers::metrics::stash_entry_signals_json_for(STRATEGY_NAME, token.as_str(), serde_json::json!({
            "viper": "Anchor",
            "side": side,
            "ask": pick.ask,
            "fair": pick.fair,
            "edge_net_of_fee": pick.edge,
            "fee_per_share": pick.fee,
            "anchor_basis": anchor.basis,
            "anchor_samples": anchor.samples,
            "anchor_basis_spread": anchor.spread,
            "anchor_basis_sigmas": r.basis_sigmas,
            "spot_range_over_window": spot_range,
            "sigma_per_sqrt_sec": sigma,
            "vol_annualized": vol_ann,
            "z": r.z,
            "secs_to_close": secs_left,
            "spot": spot,
            "strike": strike,
            "ask_depth": depth,
            "exit": "settlement",
        }));
        info!(
            "⚓ Anchor {} ENTRY on {}: fair {:.3} vs ask ${:.3}, edge {:+.3} net of fee | anchor {:+.0} over {} samples \
             (spread {:.0}, {:.2} window σ; feed range {} over the window), sigma {:.2e}/√s ({:.0}% ann.), z {:.2}, T {}s, K ${:.2} | {} contracts{}",
            side, market.market_name, pick.fair, pick.ask, pick.edge, -anchor.basis, anchor.samples,
            anchor.spread, r.basis_sigmas, spot_range.map_or("n/a".to_string(), |x| format!("{x:.0}")),
            sigma, vol_ann * 100.0, r.z, secs_left, strike, dc.anchor_contracts,
            if dc.ghost_mode { " (simulated)" } else { "" },
        );
        Ok(StrategySignal::Entry {
            params: OrderParams {
                token_id: token,
                price: ask_dec,
                shares: dc.anchor_contracts,
                fee_bps,
                is_neg_risk: market.is_neg_risk,
                market_name: market.market_name.clone(),
                condition_id: market_key,
                order_type: TimeInForce::Fak,
                post_only: false,
                ghost_mode: dc.ghost_mode,
            },
            pair_params: None,
        })
    }

    /// Never an exit: the position rides to settlement, which the venue's own
    /// sweep books (or, for a simulated position, this viper's). What runs here
    /// is the bookkeeping: ledger rows for new fills, settlements for closed ones.
    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        if ctx.market_class.as_deref().is_some_and(|c| c != "crypto") {
            return Ok(StrategySignal::NoSignal);
        }
        self.ledger_fills(ctx).await;
        self.sweep_ledger(ctx).await;
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { STRATEGY_NAME.to_string() }
    fn venue(&self) -> &'static str { "Hourly" }
    fn max_exposure(&self) -> Decimal { config::ANCHOR_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Taker at the ask, hold to settlement" }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fee(p: f64) -> f64 { 0.07 * p * (1.0 - p) }

    /// The inverse must undo the CDF across the band the sampler reads.
    #[test]
    fn the_inverse_normal_undoes_the_cdf() {
        for p in [0.15, 0.2, 0.3, 0.5, 0.7, 0.8, 0.85] {
            let back = norm_cdf(norm_ppf(p));
            assert!((back - p).abs() < 1e-6, "p={p} round-tripped to {back}");
        }
        assert!((norm_ppf(0.5)).abs() < 1e-9);
        assert!(norm_ppf(0.975) > 1.959 && norm_ppf(0.975) < 1.961);
    }

    /// A mid that is exactly the model's fair at a known offset gives that
    /// offset back as the basis, and the reading from that basis gives the mid
    /// back as fair.
    #[test]
    fn the_implied_basis_inverts_the_model() {
        let (spot, strike, sigma, t): (f64, f64, f64, f64) = (83_200.0, 83_100.0, 4.0e-5, 1800.0);
        let basis = 93.0;
        let fair = norm_cdf(((spot - basis) / strike).ln() / (sigma * t.sqrt()));
        let b = implied_basis(spot, strike, fair, sigma, t).expect("in band");
        assert!((b - basis).abs() < 0.5, "basis {b} vs {basis}");
        let r = reading(spot, strike, b, sigma, t, 600.0).unwrap();
        assert!((r.fair_yes - fair).abs() < 1e-6);
        assert!(implied_basis(spot, strike, 0.95, sigma, t).is_none(), "outside the sample band");
        assert!(implied_basis(spot, strike, 0.5, 0.0, t).is_none(), "no sigma, no sample");
    }

    /// The anchor needs enough samples, old enough, spread far enough apart,
    /// and reads the median of exactly those.
    #[test]
    fn the_anchor_is_the_median_of_the_aged_samples_or_nothing() {
        let now = 10_000;
        let mut s = VecDeque::new();
        for i in 0..40 {
            // 40 samples 10 s apart, ages 60..450; the odd ones carry an outlier
            s.push_back((now - 60 - i * 10, if i % 2 == 1 { 500.0 } else { 90.0 + i as f64 }));
        }
        let a = anchor_from(&s, now, 60, 600, 20, 300).expect("ready");
        assert_eq!(a.samples, 40);
        assert_eq!(a.span_secs, 390);
        assert!(a.basis > 90.0 && a.basis < 500.0, "median {}", a.basis);
        // Half the samples sit near 100 and half at 500: the interquartile range spans the gap.
        assert!(a.spread > 350.0 && a.spread < 420.0, "spread {}", a.spread);
        let tight: VecDeque<_> = (0..40).map(|i| (now - 60 - i * 10, 90.0 + (i % 3) as f64)).collect();
        assert!(anchor_from(&tight, now, 60, 600, 20, 300).unwrap().spread <= 2.0, "a steady level reads tight");
        // With fewer than eight samples the spread is the full range, not a quartile guess.
        let few: VecDeque<_> = [10.0, 50.0, 20.0, 40.0, 30.0].iter().enumerate()
            .map(|(i, b)| (now - 60 - i as i64 * 100, *b)).collect();
        let a = anchor_from(&few, now, 60, 600, 5, 300).unwrap();
        assert!((a.spread - 40.0).abs() < 1e-9, "range of five samples, got {}", a.spread);
        assert!(anchor_from(&s, now, 60, 600, 41, 300).is_none(), "too few");
        assert!(anchor_from(&s, now, 60, 600, 20, 400).is_none(), "span too short");
        // Everything younger than the minimum age is excluded.
        let young: VecDeque<_> = (0..40).map(|i| (now - i, 1.0)).collect();
        assert!(anchor_from(&young, now, 60, 600, 20, 300).is_none());
    }

    /// Sigma from a series with a constant 5 s log step is that step per √5.
    #[test]
    fn sigma_is_the_per_second_variance_of_log_returns() {
        let mut s = VecDeque::new();
        let (mut p, step) = (80_000.0_f64, 1.0e-4_f64);
        for i in 0..=300 {
            s.push_back((i * 5, p));
            p *= if i % 2 == 0 { (step).exp() } else { (-step).exp() };
        }
        let sigma = sigma_from_samples(&s, 200).unwrap();
        assert!((sigma - step / 5.0_f64.sqrt()).abs() < 1e-9, "sigma {sigma}");
        assert!(sigma_from_samples(&s, 400).is_none(), "below the return floor");
    }

    fn gates() -> Gates {
        Gates {
            min_edge: 0.04, max_z: 1.0, min_z: 0.10, max_basis_sigmas: 1.0, min_price: 0.20, max_price: 0.60,
            min_secs_to_close: 300, max_secs_to_close: 1800, contracts: 10.0, max_exposure: 12.0,
            obi_adverse_block: -0.60,
        }
    }
    fn q(ask: f64, depth: f64) -> SideQuote { SideQuote { ask: Some(ask), ask_depth: depth, obi: 0.0 } }

    /// A fill the ledger already holds is counted from the ledger, not twice;
    /// a provisional or not-yet-ledgered position is counted here; another
    /// viper's or another squadron's position is not this viper's exposure.
    #[test]
    fn unledgered_notional_counts_only_this_vipers_unknown_fills() {
        use crate::state::{Position, PositionKey, PositionMap};
        let now = Utc::now();
        let pos = |shares: i64, entry: &str, confirmed: bool| Position {
            shares: Decimal::from(shares),
            avg_entry: entry.parse().unwrap(),
            opened_at: now,
            close_time: None,
            market_name: "Bitcoin price at 3PM".into(),
            pair_token_id: MarketId::new("tok"),
            fill_confirmed_at: confirmed.then_some(now),
            paired_leg_token_id: None,
            entry_fee: Decimal::ZERO,
        };
        let mut map = PositionMap::new();
        map.insert(PositionKey::new("btc-open", STRATEGY_NAME, MarketId::new("ledgered")), pos(10, "0.44", true));
        map.insert(PositionKey::new("btc-open", STRATEGY_NAME, MarketId::new("provisional")), pos(5, "0.30", false));
        map.insert(PositionKey::new("btc-open", STRATEGY_NAME, MarketId::new("fresh")), pos(2, "0.50", true));
        map.insert(PositionKey::new("btc-open", "FairValueStrategy", MarketId::new("fv")), pos(10, "0.60", true));
        map.insert(PositionKey::new("eth-open", STRATEGY_NAME, MarketId::new("eth")), pos(10, "0.60", true));
        let ledgered: HashSet<String> = ["ledgered".to_string()].into_iter().collect();
        let got = unledgered_notional(&map, "btc-open", &ledgered);
        assert!((got - (1.5 + 1.0)).abs() < 1e-9, "got {got}: the provisional and the fresh fill, not the ledgered one");
        assert_eq!(unledgered_notional(&map, "sol-open", &ledgered), 0.0);
    }

    /// The cheap side is bought: fair YES 0.52 against a 0.44 ask clears four
    /// cents net of fee, while NO at 0.58 against 0.48 fair does not.
    #[test]
    fn the_rule_buys_the_cheap_side_and_only_inside_its_gates() {
        let r = Reading { fair_yes: 0.52, z: 0.3, basis_sigmas: 0.1 };
        let p = choose_side(r, 1200, q(0.44, 50.0), q(0.58, 50.0), &gates(), 0.0, fee).unwrap();
        assert!(p.is_yes);
        assert!((p.edge - (0.52 - 0.44 - fee(0.44))).abs() < 1e-12);
        // The same book one minute from the close, or far from the strike, is refused before pricing.
        assert_eq!(choose_side(r, 60, q(0.44, 50.0), q(0.58, 50.0), &gates(), 0.0, fee), Err(Refusal::OutsideWindow));
        assert_eq!(choose_side(Reading { fair_yes: 0.52, z: 1.5, basis_sigmas: 0.1 }, 1200, q(0.44, 50.0), q(0.58, 50.0), &gates(), 0.0, fee),
                   Err(Refusal::TooFarFromStrike));
        // A thin ask, an all-offer book, and a full lane each stop the side that otherwise qualified.
        assert_eq!(choose_side(r, 1200, q(0.44, 4.0), q(0.58, 50.0), &gates(), 0.0, fee), Err(Refusal::AskTooThin));
        let toxic = SideQuote { ask: Some(0.44), ask_depth: 50.0, obi: -0.9 };
        assert_eq!(choose_side(r, 1200, toxic, q(0.58, 50.0), &gates(), 0.0, fee), Err(Refusal::ObiAdverse));
        assert_eq!(choose_side(r, 1200, q(0.44, 50.0), q(0.58, 50.0), &gates(), 9.0, fee), Err(Refusal::ExposureCap));
        // Outside the price band the edge is never computed.
        assert_eq!(choose_side(Reading { fair_yes: 0.80, z: 0.8, basis_sigmas: 0.1 }, 1200, q(0.70, 50.0), q(0.32, 50.0), &gates(), 0.0, fee),
                   Err(Refusal::EdgeBelowBar), "NO at 0.32 against fair 0.20 is dear; YES at 0.70 is outside the band");
        // No ask at all on either side.
        let none = SideQuote { ask: None, ask_depth: 0.0, obi: -1.0 };
        assert_eq!(choose_side(r, 1200, none, none, &gates(), 0.0, fee), Err(Refusal::NoAsk));
    }

    /// The reported refusal is the gate the best side reached, not the first in the list.
    #[test]
    fn the_refusal_names_the_binding_gate() {
        let r = Reading { fair_yes: 0.52, z: 0.3, basis_sigmas: 0.1 };
        // YES has the edge but no depth; NO fails earlier on price. The answer is depth.
        assert_eq!(choose_side(r, 1200, q(0.44, 1.0), q(0.70, 50.0), &gates(), 0.0, fee), Err(Refusal::AskTooThin));
    }

    /// At the strike the model has no opinion: a cheap side there is the
    /// market's view, and both lanes stand aside. Zero disables the gate.
    #[test]
    fn the_rule_stands_aside_at_the_strike_on_both_lanes() {
        let at_strike = Reading { fair_yes: 0.47, z: 0.05, basis_sigmas: 0.1 };
        assert_eq!(choose_side(at_strike, 1796, q(0.40, 400.0), q(0.62, 400.0), &gates(), 0.0, fee),
                   Err(Refusal::TooCloseToStrike), "YES at 0.40 against 0.47 is the 2026-10-22 7 PM loss");
        assert!(control_side(at_strike, 1796, q(0.40, 400.0), q(0.62, 400.0), &gates()).is_none(),
                "the control lane is drawn from the same moments");
        let mut open = gates();
        open.min_z = 0.0;
        assert!(choose_side(at_strike, 1796, q(0.40, 400.0), q(0.62, 400.0), &open, 0.0, fee).is_ok(), "zero disables the gate");
        // Just past the bound the rule prices as before.
        let near = Reading { fair_yes: 0.549, z: 0.12, basis_sigmas: 0.1 };
        assert!(choose_side(near, 1226, q(0.46, 60.0), q(0.56, 60.0), &gates(), 0.0, fee).unwrap().is_yes,
                "the 2026-10-22 6 AM win at z 0.12 still trades");
    }

    /// An anchor offset of a window sigma or more is a lagged move, not a level:
    /// the reading reports it in sigmas of the sample window and both lanes
    /// refuse it, whatever the time to close.
    #[test]
    fn an_anchor_adrift_by_a_window_sigma_is_refused_on_both_lanes() {
        // 2026-10-23 09:30 ET: spot 81,614.67, strike 81,499.99, offset $123.66,
        // sigma 4.06e-5/√s, 1,800 s left, 600 s sample window: σ√W·S is about $81.
        let r = reading(81_614.67, 81_499.99, 123.66, 4.06e-5, 1800.0, 600.0).unwrap();
        assert!(r.basis_sigmas > 1.45 && r.basis_sigmas < 1.6, "offset in window sigmas {}", r.basis_sigmas);
        assert!(r.z < 0.1, "the lagged level sat at the strike, z {}", r.z);
        // Both gates trip; the adrift one is reported, being the more specific cause.
        assert_eq!(choose_side(r, 1800, q(0.63, 100.0), q(0.37, 18.0), &gates(), 0.0, fee), Err(Refusal::AnchorAdrift),
                   "NO at 0.37 against the lagged 0.53 fair is the trade that lost");
        assert!(control_side(r, 1800, q(0.63, 100.0), q(0.37, 18.0), &gates()).is_none());
        // Zero disables the gate and the strike gate then answers.
        let mut g = gates();
        g.max_basis_sigmas = 0.0;
        assert_eq!(choose_side(r, 1800, q(0.63, 100.0), q(0.37, 18.0), &g, 0.0, fee), Err(Refusal::TooCloseToStrike));
        // The same offset reads the same with five minutes left as with thirty:
        // the scale is the sample window, not the time to close.
        let late = reading(81_614.67, 81_499.99, 123.66, 4.06e-5, 300.0, 600.0).unwrap();
        assert!((late.basis_sigmas - r.basis_sigmas).abs() < 1e-9);
        // The largest honest offset in the ledger, $24 on a $61 window sigma,
        // clears the offset gate early and late in the hour alike; the fair it
        // prices moves with T (and so does the edge), but the offset reading
        // does not.
        for t in [1464.0, 320.0] {
            let steady = reading(81_575.23, 81_499.99, 24.27, 3.054e-5, t, 600.0).unwrap();
            assert!(steady.basis_sigmas > 0.35 && steady.basis_sigmas < 0.45, "T {t}: {}", steady.basis_sigmas);
            assert!(!anchor_adrift(steady.basis_sigmas, gates().max_basis_sigmas), "T {t}: an honest offset is not adrift");
        }
        let steady = reading(81_575.23, 81_499.99, 24.27, 3.054e-5, 1464.0, 600.0).unwrap();
        assert!(choose_side(steady, 1464, q(0.77, 100.0), q(0.24, 400.0), &gates(), 0.0, fee).is_ok(),
                "the 2026-10-23 1 AM win still trades");
    }

    /// The feed's range over the sample window is read from the spot series
    /// the vol sampler already keeps, over the same ages as the anchor.
    #[test]
    fn the_spot_range_covers_only_the_anchor_window() {
        let now = 10_000;
        let mut s = VecDeque::new();
        for i in 0..200 {
            // Five-second spot samples: flat at 81,400, with a $211 rally only
            // inside the last twelve minutes (ages 0 to 720).
            let age = i * 5;
            let p = if age <= 720 { 81_400.0 + 211.0 * (1.0 - age as f64 / 720.0) } else { 81_400.0 };
            s.push_back((now - age, p));
        }
        let over_window = spot_range_from(&s, now, 60, 600).unwrap();
        assert!(over_window > 150.0 && over_window < 165.0, "{over_window}: the part of the rally inside ages 60 to 600");
        assert_eq!(spot_range_from(&s, now, 900, 1000), Some(0.0), "flat before the rally");
        assert_eq!(spot_range_from(&s, now, 5000, 6000), None, "no samples that old");
    }

    /// The ledger round trip: open, hold, settle, summarize.
    #[tokio::test]
    async fn the_shadow_ledger_opens_holds_and_settles() {
        use sqlx::sqlite::SqlitePoolOptions;
        use crate::helpers::db;
        let p = SqlitePoolOptions::new().max_connections(1).connect("sqlite::memory:").await.unwrap();
        db::init_schema(&p).await.unwrap();
        db::run_migrations(&p).await;
        let close_s = Utc::now().timestamp() + 900;
        assert!(db::anchor_shadow_open(
            &p, "btc", db::ANCHOR_LANE_ENTRY, "btc-open", "cid-1", "cid-1", "tok-yes", "Bitcoin price at 3PM", "YES", close_s,
            0.44, 10.0, fee(0.44), 0.52, 93.0, 4.0e-5, 0.3, 900, 83_200.0, 83_100.0, 50.0, Some(true), Some(12.0), Some(40.0),
        ).await);
        assert!(db::anchor_shadow_holds(&p, "btc", db::ANCHOR_LANE_ENTRY, "cid-1", "YES").await);
        assert!(!db::anchor_shadow_holds(&p, "btc", db::ANCHOR_LANE_ENTRY, "cid-1", "NO").await, "the other side is still free");
        assert!(!db::anchor_shadow_holds(&p, "btc", db::ANCHOR_LANE_CONTROL, "cid-1", "YES").await, "the control lane is a different ledger");
        assert!(!db::anchor_shadow_holds(&p, "btc", db::ANCHOR_LANE_ENTRY, "cid-2", "YES").await);
        assert!((db::anchor_shadow_open_notional(&p, "btc").await - 4.4).abs() < 1e-9);
        let (spread, range): (Option<f64>, Option<f64>) = sqlx::query_as(
            "SELECT basis_spread, spot_range FROM anchor_shadow WHERE token_id = 'tok-yes'"
        ).fetch_one(&p).await.unwrap();
        assert_eq!((spread, range), (Some(12.0), Some(40.0)), "the window readings are on the row");
        let open = db::anchor_shadow_open_rows(&p, "btc").await.expect("a readable ledger");
        assert_eq!(open.len(), 1);
        assert_eq!((open[0].lane.as_str(), open[0].market_key.as_str()), ("entry", "cid-1"));
        assert_eq!((open[0].squadron_id.as_str(), open[0].ghost), ("btc-open", Some(true)));
        assert_eq!(open[0].close_s, close_s);
        let pnl = (1.0 - 0.44 - fee(0.44)) * 10.0;
        assert!(db::anchor_shadow_close(&p, open[0].id, 1.0, "settlement", pnl).await);
        assert!(!db::anchor_shadow_close(&p, open[0].id, 1.0, "settlement", pnl).await, "closed once");
        assert!(db::anchor_shadow_open_rows(&p, "btc").await.unwrap().is_empty());
        let s = db::anchor_shadow_summary(&p, "btc", db::ANCHOR_LANE_ENTRY).await;
        assert_eq!((s.closed, s.wins, s.open), (1, 1, 0));
        assert!((s.net_usd - pnl).abs() < 1e-9);
        assert!((s.net_per_contract - pnl / 10.0).abs() < 1e-9);
    }

    /// The control lane takes the side the model calls dear by the entry margin,
    /// inside the same band and window, and nothing else.
    #[test]
    fn the_control_lane_records_the_expensive_side_only() {
        let r = Reading { fair_yes: 0.40, z: 0.3, basis_sigmas: 0.1 };
        // YES at 0.46 is six cents dear; NO at 0.56 against fair 0.60 is cheap, not a control.
        let c = control_side(r, 1200, q(0.46, 50.0), q(0.56, 50.0), &gates()).expect("a control");
        assert!(c.is_yes);
        assert!((c.edge - (0.40 - 0.46)).abs() < 1e-12);
        assert!(control_side(r, 1200, q(0.42, 50.0), q(0.56, 50.0), &gates()).is_none(), "two cents dear is inside the margin");
        assert!(control_side(r, 100, q(0.46, 50.0), q(0.56, 50.0), &gates()).is_none(), "outside the window");
        assert!(control_side(r, 1200, q(0.46, 2.0), q(0.56, 50.0), &gates()).is_none(), "a thin ask is not a print we could have hit");
        assert!(control_side(r, 1200, q(0.70, 50.0), q(0.32, 50.0), &gates()).is_none(), "outside the band on both sides");
    }

    /// An empty lane summarizes to zeros without an error: SUM over no rows is
    /// NULL in SQLite, and the sweep asks for both lanes every minute on every
    /// instance from upgrade onward, most of which will never hold a control row.
    #[tokio::test]
    async fn an_empty_lane_summarizes_to_zeros() {
        use sqlx::sqlite::SqlitePoolOptions;
        use crate::helpers::db;
        let p = SqlitePoolOptions::new().max_connections(1).connect("sqlite::memory:").await.unwrap();
        db::init_schema(&p).await.unwrap();
        db::run_migrations(&p).await;
        assert_eq!(db::anchor_shadow_summary(&p, "btc", db::ANCHOR_LANE_CONTROL).await, db::AnchorLaneSummary::default());
        assert_eq!(db::anchor_shadow_open_rows(&p, "btc").await, Some(Vec::new()));
    }

    /// A row the venue never resolves is closed without a return and leaves the
    /// summary untouched.
    #[tokio::test]
    async fn an_unresolved_row_is_abandoned_not_scored() {
        use sqlx::sqlite::SqlitePoolOptions;
        use crate::helpers::db;
        let p = SqlitePoolOptions::new().max_connections(1).connect("sqlite::memory:").await.unwrap();
        db::init_schema(&p).await.unwrap();
        db::run_migrations(&p).await;
        assert!(db::anchor_shadow_open(
            &p, "btc", db::ANCHOR_LANE_ENTRY, "btc-open", "cid-9", "cid-9", "tok-no", "m", "NO", 1, 0.30, 10.0, fee(0.30), 0.36, -20.0, 4.0e-5, 0.5, 600,
            1.0, 1.0, 20.0, None, None, None,
        ).await);
        let id = db::anchor_shadow_open_rows(&p, "btc").await.unwrap()[0].id;
        assert!(db::anchor_shadow_abandon(&p, id, "unresolved").await);
        let s = db::anchor_shadow_summary(&p, "btc", db::ANCHOR_LANE_ENTRY).await;
        assert_eq!((s.closed, s.open), (0, 0), "an abandoned row is neither settled nor open");
    }
}
