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
//! # Phase 0: the shadow lane
//!
//! This build places no order. Every would-be entry is written to the
//! `anchor_shadow` ledger with the model's reading at the time, settled through
//! the venue's installed resolver (`venues::core::resolve_leg`) once the market
//! closes, and booked into the trade list as a ghost row so the operator can see
//! it. The go/no-go for real money is computed from that ledger (see
//! `docs/spikes/sideways-regime-spike.md`, section 5) and nothing in this file
//! can spend until a later build adds the live path behind its own knob.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::config;
use crate::helpers::db::{ANCHOR_LANE_CONTROL, ANCHOR_LANE_ENTRY};
use crate::helpers::dynamic_config::DynamicConfig;
use crate::helpers::volatility::norm_cdf;
use crate::orchestrator::strategy::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus};
use crate::venues::core::MarketId;

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
    Some(AnchorRead { basis: median, samples: n, span_secs: span })
}

/// The model's reading of one market: fair P(YES) from the anchored level, and
/// the distance of that level from the strike in sigmas.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Reading {
    pub fair_yes: f64,
    pub z: f64,
}

/// `Φ(ln((S − b)/K) / (σ√T))` and `|ln((S − b)/K)| / (σ√T)`.
pub fn reading(spot: f64, strike: f64, basis: f64, sigma: f64, secs_left: f64) -> Option<Reading> {
    let level = spot - basis;
    if level <= 0.0 || strike <= 0.0 || sigma <= 0.0 || secs_left <= 0.0 { return None; }
    let d = (level / strike).ln() / (sigma * secs_left.sqrt());
    if !d.is_finite() { return None; }
    Some(Reading { fair_yes: norm_cdf(d), z: d.abs() })
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

/// The control lane's candidate: a side inside the same price band and window
/// that the model calls EXPENSIVE by the entry margin (`ask − fair ≥ min_edge`,
/// no fee term, the backtest's own control definition). Recorded and settled
/// like an entry, never booked. If the control lane does not lose, the entry
/// lane's gains are the market drifting, not the model knowing.
pub fn control_side(r: Reading, secs_left: i64, yes: SideQuote, no: SideQuote, g: &Gates) -> Option<Pick> {
    if secs_left < g.min_secs_to_close || secs_left > g.max_secs_to_close || r.z > g.max_z { return None; }
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
    /// Σ entry × shares of the entry lane's open rows, maintained the same way.
    open_notional: Mutex<f64>,
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

    /// Settle the lane's open rows through the venue's resolver, rebuild the
    /// in-memory holds from the ledger, and refresh the card's summary line.
    /// Runs whether or not the viper is enabled: a row opened while it was on
    /// is owed its settlement.
    async fn sweep_shadow(&self, ctx: &StrategyContext) {
        let asset = ctx.crypto_filter.to_lowercase();
        let g = globals(&asset);
        {
            let mut last = lock(&g.last_sweep);
            if last.is_some_and(|t| t.elapsed().as_secs() < SWEEP_SECS) { return; }
            *last = Some(Instant::now());
        }
        let Some(pool) = crate::helpers::db::pool_for(&asset) else { return };
        let now_s = Utc::now().timestamp();
        // A failed read keeps the previous holds and notional rather than
        // clearing them, which would let the entry path re-enter a held side.
        if let Some(rows) = crate::helpers::db::anchor_shadow_open_rows(&pool, &asset).await {
            *lock(&g.held) = rows.iter().map(|r| held_key(&r.lane, &r.market_key, &r.side)).collect();
            *lock(&g.open_notional) = rows.iter()
                .filter(|r| r.lane == ANCHOR_LANE_ENTRY)
                .map(|r| r.entry_price * r.shares).sum();
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
                            "⚓ Anchor shadow row written off unscored [{}] {}: the venue never resolved it \
                             {}h after close, so it counts as no trade rather than as a guess",
                            row.market, row.side, (now_s - row.close_s) / 3600,
                        );
                    }
                    continue;
                };
                lock(&g.shadow_resolved).remove(&row.token_id);
                // Close and book in one spawned task, so the 500 ms strategy
                // timeout cannot drop the ghost row between the two writes.
                let (pool_c, asset_c) = (pool.clone(), asset.clone());
                tokio::spawn(async move {
                    // Settlement pays no exit fee; the entry fee was charged at the ask.
                    let per_contract = px - row.entry_price - row.entry_fee;
                    let pnl = per_contract * row.shares;
                    if !crate::helpers::db::anchor_shadow_close(&pool_c, row.id, px, "settlement", pnl).await {
                        return;
                    }
                    if row.lane != ANCHOR_LANE_ENTRY {
                        info!("⚓ Anchor control settled [{}] {} at ${:.3}, resolved ${:.2}: {:+.2}c per contract (never taken)",
                              row.market, row.side, row.entry_price, px, per_contract * 100.0);
                        return;
                    }
                    let mut scope = crate::state::TradeScope::crypto(&asset_c, "", &asset_c);
                    scope.ghost = true;
                    let d = |x: f64| Decimal::try_from(x).unwrap_or(Decimal::ZERO);
                    crate::helpers::db::record_trade_db(
                        &pool_c, &scope, d(row.entry_fee * row.shares), STRATEGY_NAME, &row.market, &row.side,
                        d(row.entry_price), d(px), d(row.shares), d(pnl),
                        "Anchor settlement — simulated", None,
                    ).await;
                    info!(
                        "⚓ Anchor settled [{}] {} bought at ${:.3}, resolved ${:.2}: {:+.2}c per contract, {:+.2} USD (simulated)",
                        row.market, row.side, row.entry_price, px, per_contract * 100.0, pnl,
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
            let line = if s.closed == 0 {
                format!("shadow: {} open, none settled yet{control}", s.open)
            } else {
                format!(
                    "shadow: {} settled, {:.0}% won, {:+.1}c per contract, {:+.2} USD, {} open{control}",
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
            idle(&format!("anchor warming: {n_samples} samples"));
            return Ok(StrategySignal::NoSignal);
        };
        let Some(r) = reading(spot, strike, anchor.basis, sigma, secs_left as f64) else {
            idle("fair value not computable");
            return Ok(StrategySignal::NoSignal);
        };
        let yes = Self::side_quote(&ctx.snapshot, true);
        let no = Self::side_quote(&ctx.snapshot, false);
        crate::helpers::viper_status::report_detail(&ctx.crypto_filter, STRATEGY_NAME, Some(format!(
            "anchor {:+.0} ({} samples) | fair YES {:.3} vs ask {} | z {:.2} | vol {:.0}% | T {}s | {}",
            -anchor.basis, anchor.samples, r.fair_yes,
            yes.ask.map_or("n/a".to_string(), |a| format!("{a:.3}")),
            r.z, vol_ann * 100.0, secs_left, lock(&g.summary).1,
        )));

        let Some(pool) = crate::helpers::db::pool_for(&asset) else {
            idle("no database for the simulated ledger");
            return Ok(StrategySignal::NoSignal);
        };
        // The venue's taker fee at a price. Per-market fee bps
        // (`MarketConfig::{yes,no}_fee_bps`) and a venue's per-order rounding are
        // not applied; on the quadratic venues the two agree to within a tenth
        // of a cent per contract at ten contracts.
        let fee = |p: f64| crate::venues::taker_fee_per_share(Decimal::try_from(p).unwrap_or(Decimal::ZERO))
            .to_f64().unwrap_or(0.0);
        let close_s = now_s + secs_left;
        // From the sweep's cache: the entry path runs every tick and must not query.
        let open_notional = *lock(&g.open_notional);
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
                    anchor.basis, sigma, r.z, secs_left, spot, strike, depth,
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
                if matches!(why, Refusal::AskTooThin | Refusal::ObiAdverse | Refusal::ExposureCap)
                    && crate::vipers::gate_log_permitted(STRATEGY_NAME, &asset, why.label(), GATE_LOG_INTERVAL_SECS)
                {
                    info!("⚓ Anchor hold on {}: {} (fair YES {:.3}, z {:.2}, T {}s)",
                          market.market_name, why.label(), r.fair_yes, r.z, secs_left);
                }
                return Ok(StrategySignal::NoSignal);
            }
        };
        let side = if pick.is_yes { "YES" } else { "NO" };
        let key = held_key(ANCHOR_LANE_ENTRY, &market_key, side);
        if lock(&g.held).contains(&key) {
            idle("already holding this side this hour (simulated)");
            return Ok(StrategySignal::NoSignal);
        }
        let token = if pick.is_yes { &market.yes_token } else { &market.no_token };
        let depth = if pick.is_yes { yes.ask_depth } else { no.ask_depth };
        let opened = crate::helpers::db::anchor_shadow_open(
            &pool, &asset, ANCHOR_LANE_ENTRY, &ctx.squadron_id, &market_key, &market.condition_id, token.as_str(),
            &market.market_name, side, close_s, pick.ask, gates.contracts, pick.fee, pick.fair, anchor.basis,
            sigma, r.z, secs_left, spot, strike, depth,
        ).await;
        if !opened {
            idle("ledger write failed");
            return Ok(StrategySignal::NoSignal);
        }
        lock(&g.held).insert(key);
        *lock(&g.open_notional) += pick.ask * gates.contracts;
        info!(
            "⚓ Anchor {} entry on {}: fair {:.3} vs ask ${:.3}, edge {:+.3} net of fee | anchor {:+.0} over {} samples, \
             sigma {:.2e}/√s ({:.0}% ann.), z {:.2}, T {}s, K ${:.2} | {} contracts (simulated)",
            side, market.market_name, pick.fair, pick.ask, pick.edge, -anchor.basis, anchor.samples,
            sigma, vol_ann * 100.0, r.z, secs_left, strike, gates.contracts,
        );
        idle("simulated entry recorded this hour");
        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        if ctx.market_class.as_deref().is_some_and(|c| c != "crypto") {
            return Ok(StrategySignal::NoSignal);
        }
        self.sweep_shadow(ctx).await;
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { STRATEGY_NAME.to_string() }
    fn venue(&self) -> &'static str { "Hourly" }
    fn max_exposure(&self) -> Decimal { config::ANCHOR_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Taker at the ask, hold to settlement (simulated)" }
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
        let r = reading(spot, strike, b, sigma, t).unwrap();
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
            min_edge: 0.04, max_z: 1.0, min_price: 0.20, max_price: 0.60,
            min_secs_to_close: 300, max_secs_to_close: 1800, contracts: 10.0, max_exposure: 12.0,
            obi_adverse_block: -0.60,
        }
    }
    fn q(ask: f64, depth: f64) -> SideQuote { SideQuote { ask: Some(ask), ask_depth: depth, obi: 0.0 } }

    /// The cheap side is bought: fair YES 0.52 against a 0.44 ask clears four
    /// cents net of fee, while NO at 0.58 against 0.48 fair does not.
    #[test]
    fn the_rule_buys_the_cheap_side_and_only_inside_its_gates() {
        let r = Reading { fair_yes: 0.52, z: 0.3 };
        let p = choose_side(r, 1200, q(0.44, 50.0), q(0.58, 50.0), &gates(), 0.0, fee).unwrap();
        assert!(p.is_yes);
        assert!((p.edge - (0.52 - 0.44 - fee(0.44))).abs() < 1e-12);
        // The same book one minute from the close, or far from the strike, is refused before pricing.
        assert_eq!(choose_side(r, 60, q(0.44, 50.0), q(0.58, 50.0), &gates(), 0.0, fee), Err(Refusal::OutsideWindow));
        assert_eq!(choose_side(Reading { fair_yes: 0.52, z: 1.5 }, 1200, q(0.44, 50.0), q(0.58, 50.0), &gates(), 0.0, fee),
                   Err(Refusal::TooFarFromStrike));
        // A thin ask, an all-offer book, and a full lane each stop the side that otherwise qualified.
        assert_eq!(choose_side(r, 1200, q(0.44, 4.0), q(0.58, 50.0), &gates(), 0.0, fee), Err(Refusal::AskTooThin));
        let toxic = SideQuote { ask: Some(0.44), ask_depth: 50.0, obi: -0.9 };
        assert_eq!(choose_side(r, 1200, toxic, q(0.58, 50.0), &gates(), 0.0, fee), Err(Refusal::ObiAdverse));
        assert_eq!(choose_side(r, 1200, q(0.44, 50.0), q(0.58, 50.0), &gates(), 9.0, fee), Err(Refusal::ExposureCap));
        // Outside the price band the edge is never computed.
        assert_eq!(choose_side(Reading { fair_yes: 0.80, z: 0.8 }, 1200, q(0.70, 50.0), q(0.32, 50.0), &gates(), 0.0, fee),
                   Err(Refusal::EdgeBelowBar), "NO at 0.32 against fair 0.20 is dear; YES at 0.70 is outside the band");
        // No ask at all on either side.
        let none = SideQuote { ask: None, ask_depth: 0.0, obi: -1.0 };
        assert_eq!(choose_side(r, 1200, none, none, &gates(), 0.0, fee), Err(Refusal::NoAsk));
    }

    /// The reported refusal is the gate the best side reached, not the first in the list.
    #[test]
    fn the_refusal_names_the_binding_gate() {
        let r = Reading { fair_yes: 0.52, z: 0.3 };
        // YES has the edge but no depth; NO fails earlier on price. The answer is depth.
        assert_eq!(choose_side(r, 1200, q(0.44, 1.0), q(0.70, 50.0), &gates(), 0.0, fee), Err(Refusal::AskTooThin));
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
            0.44, 10.0, fee(0.44), 0.52, 93.0, 4.0e-5, 0.3, 900, 83_200.0, 83_100.0, 50.0,
        ).await);
        assert!(db::anchor_shadow_holds(&p, "btc", db::ANCHOR_LANE_ENTRY, "cid-1", "YES").await);
        assert!(!db::anchor_shadow_holds(&p, "btc", db::ANCHOR_LANE_ENTRY, "cid-1", "NO").await, "the other side is still free");
        assert!(!db::anchor_shadow_holds(&p, "btc", db::ANCHOR_LANE_CONTROL, "cid-1", "YES").await, "the control lane is a different ledger");
        assert!(!db::anchor_shadow_holds(&p, "btc", db::ANCHOR_LANE_ENTRY, "cid-2", "YES").await);
        assert!((db::anchor_shadow_open_notional(&p, "btc").await - 4.4).abs() < 1e-9);
        let open = db::anchor_shadow_open_rows(&p, "btc").await.expect("a readable ledger");
        assert_eq!(open.len(), 1);
        assert_eq!((open[0].lane.as_str(), open[0].market_key.as_str()), ("entry", "cid-1"));
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
        let r = Reading { fair_yes: 0.40, z: 0.3 };
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
            1.0, 1.0, 20.0,
        ).await);
        let id = db::anchor_shadow_open_rows(&p, "btc").await.unwrap()[0].id;
        assert!(db::anchor_shadow_abandon(&p, id, "unresolved").await);
        let s = db::anchor_shadow_summary(&p, "btc", db::ANCHOR_LANE_ENTRY).await;
        assert_eq!((s.closed, s.open), (0, 0), "an abandoned row is neither settled nor open");
    }
}
