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

//! GBoost plan B: an offline-trained, calibrated gradient-boosted model (2026-09-13).
//!
//! Replaces the self-retraining classifier in `gboost_impl` as the `GboostStrategy`
//! viper. That model learned "is BTC higher in three minutes" from order-book
//! snapshots and never showed a tradeable edge. This one is the model the research
//! harness validated (`~/dradis-research/gboost-holdout-2026-09-12`): perpetual at
//! budget 2.0 with Platt calibration, trained on June 1 to September 10 2026 BTC
//! hourly markets to predict whether a trade entered at the ask reaches a 20%
//! resting take-profit before an 11% stop. On unseen June and July markets that
//! configuration returned +3.4% per trade before this code existed; the live trial
//! is the real test.
//!
//! # Parity with the harness
//!
//! The model is only valid on inputs built exactly the way its training rows were
//! (`build_holdout.py`). So the features come from 1-minute Binance klines fetched
//! at each decision minute, not from the price raptor's ticker; realized volatility
//! uses the harness's own 4.2e-5 floor, not FairValue's knob; the funding rate is
//! the last SETTLED rate from Binance's history endpoint, not the funding raptor's
//! value (which can silently fall back to OKX); and the four futures features the
//! training run could not use are fed as zeros, as the evaluator fed them.
//! `build_features` is tested against rows the harness produced.
//!
//! # Trading rule
//!
//! At minutes 5 to 45 of each hourly window, once the minute's bar has closed, both
//! sides are scored. A side qualifies when its ask is inside the trained band and its
//! calibrated win probability clears the plan's break-even win rate plus the margin.
//! The first qualifying minute enters, once per market and side, as a taker, sized by
//! the trade-size knob and never below the venue's 5-share minimum. Exits: a resting
//! take-profit at +20% (capped at the ceiling), a taker stop when the bid falls 11%
//! below entry, a taker flatten just before the engine rotates to the next hour's
//! market (after which it can no longer see this book), and otherwise settlement.
//! GBoost does not consult any other viper.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use async_trait::async_trait;
use chrono::{Datelike, TimeZone, Utc};
use perpetual::booster::config::BoosterIO;
use perpetual::{Matrix, PerpetualBooster};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use rust_decimal_macros::dec;
use tracing::{info, warn};

use crate::config;
use crate::helpers::price::ceil_to_tick_size;
use crate::helpers::volatility::norm_cdf;
use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{OrderParams, PositionKey, StrategySignal, StrategyStatus};
use crate::venues::core::TimeInForce;
use crate::vipers::is_drawdown_limit_hit;

const STRATEGY_NAME: &str = "GboostStrategy";

/// Number of model inputs.
pub const N_FEATURES: usize = 29;

/// Model inputs, in the column order the model was trained on. The loader refuses a
/// model file whose stamped `feature_names` differ.
pub const FEATURE_NAMES: [&str; N_FEATURES] = [
    "side", "tau", "ask", "mid_s", "ask_sum", "mid_d1", "mid_d5", "sret1", "sret5", "sret15", "sret30", "sret60",
    "rv60", "rv15", "range30", "sdist", "z_side", "abs_z", "fair_side", "fv_edge", "mkt_vs_fair", "frac_above_side",
    "funding", "oi_d5", "oi_d30", "tlsr", "tlsr15", "hour_utc", "dow",
];

/// Realized-vol floor the training rows used for `fair_side` and `z_side`. A parity
/// constant: changing it would feed the model inputs it was never trained on.
const TRAINING_SIGMA_FLOOR: f64 = 4.2e-5;
/// Closed 1-minute bars needed before a decision: 60 returns plus the bar before.
const HISTORY_BARS: i64 = 61;
/// Bars requested per fetch (covers the history plus the window's strike bar).
const KLINES_LIMIT: u32 = 75;
/// Seconds after a minute boundary before its bar is treated as closed.
const BAR_SETTLE_SECS: i64 = 2;
/// A decision minute not scored within this many seconds is skipped, not scored late.
const DECISION_STALE_SECS: i64 = 45;
/// A recorded mid older than this is treated as missing (the harness's staleness rule).
const MID_STALE_SECS: i64 = 180;
/// How often the settled funding rate is re-read.
const FUNDING_REFRESH_SECS: u64 = 120;
/// How often the model file is checked for a newer version.
const MODEL_CHECK_SECS: u64 = 60;
const HTTP_TIMEOUT_SECS: u64 = 5;
const WARN_THROTTLE_SECS: u64 = 600;
/// Polymarket's `orderMinSize` on BTC hourly markets. The engine-wide `MIN_ORDER_SHARES`
/// is lower, but a resting ask below this is rejected, so plan B never buys less and
/// never tries to exit a remainder below it.
const VENUE_MIN_SHARES: Decimal = dec!(5);
/// How long before the engine's market rotation (`FINAL_EXPIRY_WINDOW_SECS` before close)
/// a held position is flattened. After the rotation the old book is gone, resting orders
/// are pulled and neither the stop nor the take-profit can act, so the position would
/// ride to settlement with no stop.
const ROTATION_FLATTEN_LEAD_SECS: i64 = 30;
/// A new entry needs at least this long before that flatten, or it would only pay two
/// taker fees.
const MIN_HOLD_BEFORE_FLATTEN_SECS: i64 = 120;

// ── Pure model inputs and rules ──────────────────────────────────────────────

/// One closed 1-minute Binance bar.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bar {
    pub open_s: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

/// Everything one decision minute needs.
pub struct DecisionInputs<'a> {
    /// Hourly window start (the strike is this minute's bar open).
    pub w: i64,
    /// Decision time: a minute boundary inside the window.
    pub t: i64,
    /// Closed bars, any order; must include the 61 ending with the one opening at `t - 60`.
    pub bars: &'a [Bar],
    /// Mid of each side (0 = YES/Up, 1 = NO/Down) at `t`, `t - 60` and `t - 300`.
    pub mid_now: [Option<f64>; 2],
    pub mid_m1: [Option<f64>; 2],
    pub mid_m5: [Option<f64>; 2],
    /// Ask each side would be bought at.
    pub ask: [f64; 2],
    /// Last settled funding rate at or before `t`.
    pub funding: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureGap {
    MissingBars,
    MissingStrikeBar,
    MissingMid,
    BadPrice,
}

fn pstd(x: &[f64]) -> f64 {
    let n = x.len() as f64;
    let mean = x.iter().sum::<f64>() / n;
    (x.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n).sqrt()
}

/// The 29 model inputs for both sides, exactly as `build_holdout.py` computes them.
pub fn build_features(inp: &DecisionInputs) -> std::result::Result<[[f64; N_FEATURES]; 2], FeatureGap> {
    let by_open: HashMap<i64, &Bar> = inp.bars.iter().map(|b| (b.open_s, b)).collect();
    let first_open = inp.t - HISTORY_BARS * 60;
    let mut hist: Vec<&Bar> = Vec::with_capacity(HISTORY_BARS as usize);
    for i in 0..HISTORY_BARS {
        match by_open.get(&(first_open + 60 * i)) {
            Some(b) => hist.push(b),
            None => return Err(FeatureGap::MissingBars),
        }
    }
    let strike = by_open.get(&inp.w).ok_or(FeatureGap::MissingStrikeBar)?.open;
    let (mid_up, mid_dn) = match (inp.mid_now[0], inp.mid_now[1]) {
        (Some(a), Some(b)) => (a, b),
        _ => return Err(FeatureGap::MissingMid),
    };
    if strike <= 0.0 || hist.iter().any(|b| b.close <= 0.0) {
        return Err(FeatureGap::BadPrice);
    }

    // hist[60] is the bar that closed at t (the harness's `ik`).
    let logc: Vec<f64> = hist.iter().map(|b| b.close.ln()).collect();
    let rets: Vec<f64> = (1..HISTORY_BARS as usize).map(|i| logc[i] - logc[i - 1]).collect();
    let sig_min = pstd(&rets);
    let rv15 = pstd(&rets[rets.len() - 15..]);
    let s_now = hist[60].close;
    let tau = (inp.w + 3600 - inp.t) as f64;
    let sig_sec_f = (sig_min / 60f64.sqrt()).max(TRAINING_SIGMA_FLOOR);
    let dist = (s_now / strike).ln();
    let z = dist / (sig_sec_f * tau.sqrt());
    let fair_up = norm_cdf(z);
    let ret = |n: usize| logc[60] - logc[60 - n];
    let hi30 = hist[31..].iter().map(|b| b.high).fold(f64::MIN, f64::max);
    let lo30 = hist[31..].iter().map(|b| b.low).fold(f64::MAX, f64::min);
    let range30 = (hi30 - lo30) / s_now;
    let ln_k = strike.ln();
    let path: Vec<f64> = hist.iter().filter(|b| b.open_s >= inp.w).map(|b| b.close.ln() - ln_k).collect();
    let frac_above = path.iter().filter(|x| **x > 0.0).count() as f64 / path.len() as f64;
    let hour_utc = (inp.t.div_euclid(3600) % 24) as f64;
    let dow = Utc
        .timestamp_opt(inp.t, 0)
        .single()
        .map_or(f64::NAN, |d| d.weekday().num_days_from_monday() as f64);

    let mut out = [[0.0f64; N_FEATURES]; 2];
    for side in 0..2 {
        let sgn = if side == 0 { 1.0 } else { -1.0 };
        let (mid_s, mid_o) = if side == 0 { (mid_up, mid_dn) } else { (mid_dn, mid_up) };
        let ask = inp.ask[side];
        let fair_side = if side == 0 { fair_up } else { 1.0 - fair_up };
        let drift = |then: Option<f64>| then.map_or(f64::NAN, |v| mid_s - v);
        let frac = if side == 0 { frac_above } else { 1.0 - frac_above };
        out[side] = [
            side as f64, tau, ask, mid_s, mid_s + mid_o - 1.0 + 0.02,
            drift(inp.mid_m1[side]), drift(inp.mid_m5[side]),
            sgn * ret(1), sgn * ret(5), sgn * ret(15), sgn * ret(30), sgn * ret(60),
            sig_min, rv15, range30,
            sgn * dist, sgn * z, z.abs(), fair_side, fair_side - ask, mid_s - fair_side, frac,
            inp.funding,
            // oi_d5, oi_d30, tlsr, tlsr15: absent from every training row, zeroed as the evaluator zeroed them.
            0.0, 0.0, 0.0, 0.0,
            hour_utc, dow,
        ];
    }
    Ok(out)
}

/// Break-even win rate of the plan bought at `ask`: entry fee, taker stop fee, the
/// target and the stop, as the harness's `break_even`.
pub fn break_even(ask: f64, tp: f64, sl: f64, fee_rate: f64) -> f64 {
    let fe = fee_rate * ask * (1.0 - ask) / ask;
    let stop = ask * (1.0 - sl);
    let fx = fee_rate * stop * (1.0 - stop) / ask;
    let gain = tp - fe;
    let loss = sl + fe + fx;
    loss / (gain + loss)
}

/// Platt calibration of a raw model probability, clipped as the evaluator clipped it.
pub fn calibrate(raw: f64, a: f64, b: f64) -> f64 {
    let r = raw.clamp(1e-4, 1.0 - 1e-4);
    let x = (r / (1.0 - r)).ln();
    (1.0 / (1.0 + (-(a * x + b)).exp())).clamp(1e-4, 1.0 - 1e-4)
}

/// Why one side did or did not qualify at a decision minute.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SideDecision {
    pub qualifies: bool,
    pub break_even: f64,
    pub required: f64,
    pub reason: &'static str,
}

pub fn decide_side(ask: f64, p: f64, fee_rate: f64, tp: f64, sl: f64, margin: f64, min_ask: f64, max_ask: f64) -> SideDecision {
    if !(ask > 0.0 && ask < 1.0) {
        return SideDecision { qualifies: false, break_even: f64::NAN, required: f64::NAN, reason: "no usable ask" };
    }
    let be = break_even(ask, tp, sl, fee_rate);
    let required = be + margin;
    if ask < min_ask || ask > max_ask {
        return SideDecision { qualifies: false, break_even: be, required, reason: "ask outside the trained band" };
    }
    if p >= required {
        SideDecision { qualifies: true, break_even: be, required, reason: "clears break-even plus margin" }
    } else {
        SideDecision { qualifies: false, break_even: be, required, reason: "below break-even plus margin" }
    }
}

/// The take-profit price for a position entered at `entry`.
pub fn take_profit_target(entry: Decimal, tp: Decimal, ceiling: Decimal) -> Decimal {
    let target = ceil_to_tick_size(entry * (Decimal::ONE + tp));
    if ceiling > Decimal::ZERO { target.min(ceiling) } else { target }
}

/// Shares to buy for a `trade_size` entry at `ask`: as many as the size buys with the taker
/// fee (`fee_rate × p × (1 − p)` per share) included, raised to the venue minimum when it
/// falls short, and refused when that many shares with the fee would not fit in the
/// exposure `room` left. The fee comes from the rate, not the market's `fee_bps`, which
/// the intl venue leaves at 0.
pub fn entry_shares(trade_size: Decimal, room: Decimal, ask: Decimal, fee_rate: Decimal) -> std::result::Result<Decimal, &'static str> {
    if ask <= Decimal::ZERO || ask >= Decimal::ONE {
        return Err("no usable ask");
    }
    let cost_per_share = ask * (Decimal::ONE + fee_rate * (Decimal::ONE - ask));
    let shares = (trade_size / cost_per_share)
        .round_dp_with_strategy(2, RoundingStrategy::ToZero)
        .max(VENUE_MIN_SHARES);
    let cost = shares * cost_per_share;
    if cost > room {
        return Err("Max Exposure has no room for the venue's 5-share minimum");
    }
    Ok(shares)
}

/// When a position on a market closing at `close_s` is flattened ahead of the rotation.
pub fn rotation_flatten_at(close_s: i64) -> i64 {
    close_s - config::FINAL_EXPIRY_WINDOW_SECS - ROTATION_FLATTEN_LEAD_SECS
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExitAction {
    Stop,
    TakeProfit,
    Rest(Decimal),
    Hold,
}

/// What a held position should do at this bid.
pub fn exit_action(entry: Decimal, bid: Decimal, tp: Decimal, sl: Decimal, ceiling: Decimal, resting: bool) -> ExitAction {
    if entry <= Decimal::ZERO {
        return ExitAction::Hold;
    }
    if bid > Decimal::ZERO && bid <= entry * (Decimal::ONE - sl) {
        return ExitAction::Stop;
    }
    let target = take_profit_target(entry, tp, ceiling);
    let tp_possible = target < Decimal::ONE && target > entry;
    if tp_possible && bid >= target {
        return ExitAction::TakeProfit;
    }
    if resting && tp_possible {
        return ExitAction::Rest(target);
    }
    ExitAction::Hold
}

/// The last mid recorded at or before `at`, if no older than the harness's staleness limit.
fn mid_at(series: Option<&BTreeMap<i64, [Option<f64>; 2]>>, side: usize, at: i64) -> Option<f64> {
    let (key, mids) = series?.range(..=at).next_back()?;
    if at - key > MID_STALE_SECS { return None; }
    mids[side]
}

fn book_mid(bid: Decimal, ask: Decimal) -> Option<f64> {
    if bid > Decimal::ZERO && ask > Decimal::ZERO && ask < Decimal::ONE {
        ((bid + ask) / dec!(2)).to_f64()
    } else {
        None
    }
}

// ── Model ────────────────────────────────────────────────────────────────────

pub struct PlanBModel {
    booster: PerpetualBooster,
    pub platt_a: f64,
    pub platt_b: f64,
    pub version: String,
    pub trees: usize,
}

/// Load and validate a model file: its layout, its input names, its calibration and
/// its size must all be what this code feeds it.
pub fn load_model(path: &std::path::Path) -> std::result::Result<PlanBModel, String> {
    let json = std::fs::read_to_string(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let booster = PerpetualBooster::from_json(&json).map_err(|e| format!("cannot parse {}: {e:?}", path.display()))?;
    let meta = |key: &str| booster.get_metadata(&key.to_string());
    if meta("feature_layout").as_deref() != Some("column_major") {
        return Err("model is not stamped feature_layout=column_major".to_string());
    }
    if meta("feature_names").as_deref() != Some(FEATURE_NAMES.join(",").as_str()) {
        return Err("model's feature_names do not match this build's inputs".to_string());
    }
    let num = |key: &str| meta(key).and_then(|s| s.parse::<f64>().ok()).filter(|v| v.is_finite());
    let platt_a = num("platt_a").ok_or("model has no usable platt_a")?;
    let platt_b = num("platt_b").ok_or("model has no usable platt_b")?;
    let trees = booster.get_prediction_trees().len();
    if trees < config::GBOOST_STRUCTURAL_MIN_TREES {
        return Err(format!("model has {trees} trees, below the structural minimum {}", config::GBOOST_STRUCTURAL_MIN_TREES));
    }
    let version = meta("model_version").unwrap_or_else(|| "unversioned".to_string());
    Ok(PlanBModel { booster, platt_a, platt_b, version, trees })
}

impl PlanBModel {
    /// `(raw, calibrated)` for each row.
    pub fn predict(&self, rows: &[[f64; N_FEATURES]]) -> Vec<(f64, f64)> {
        let n = rows.len();
        let mut data = vec![0.0f64; n * N_FEATURES];
        for (i, row) in rows.iter().enumerate() {
            for (j, v) in row.iter().enumerate() {
                data[j * n + i] = *v;
            }
        }
        let matrix = Matrix::new(&data, n, N_FEATURES);
        self.booster
            .predict_proba(&matrix, false, false)
            .into_iter()
            .map(|raw| (raw, calibrate(raw, self.platt_a, self.platt_b)))
            .collect()
    }
}

// ── Shared state (outlives strategy objects, which are rebuilt on rotation) ───

#[derive(Default)]
struct ModelSlot {
    model: Option<Arc<PlanBModel>>,
    mtime: Option<SystemTime>,
    loading: bool,
    last_check: Option<Instant>,
    last_warn: Option<Instant>,
}

#[derive(Default)]
struct MinuteData {
    bars_t: Option<i64>,
    bars: Vec<Bar>,
    fetching_t: Option<i64>,
    funding: Option<(f64, i64)>,
    funding_fetched: Option<Instant>,
    funding_fetching: bool,
    last_warn: Option<Instant>,
}

#[derive(Default)]
struct PlanBGlobals {
    model: StdMutex<ModelSlot>,
    data: StdMutex<MinuteData>,
    mids: StdMutex<HashMap<String, BTreeMap<i64, [Option<f64>; 2]>>>,
    decided: StdMutex<HashSet<(String, i64)>>,
    attempted: StdMutex<HashSet<(String, usize)>>,
    below_minimum_noted: StdMutex<HashSet<String>>,
}

fn lock<T>(m: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

fn globals(asset: &str) -> &'static PlanBGlobals {
    static G: OnceLock<StdMutex<HashMap<String, &'static PlanBGlobals>>> = OnceLock::new();
    let map = G.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut guard = lock(map);
    *guard
        .entry(asset.to_ascii_uppercase())
        .or_insert_with(|| Box::leak(Box::new(PlanBGlobals::default())))
}

fn model_path(asset: &str) -> PathBuf {
    PathBuf::from(format!("logs/{}-{}", asset.to_ascii_lowercase(), config::GBOOST_PLANB_MODEL_FILENAME))
}

fn warn_due(last: &mut Option<Instant>) -> bool {
    let due = last.is_none_or(|t| t.elapsed() >= Duration::from_secs(WARN_THROTTLE_SECS));
    if due { *last = Some(Instant::now()); }
    due
}

/// The loaded model, loading it (or a newer file) in the background when needed.
fn ensure_model(g: &'static PlanBGlobals, asset: &str) -> Option<Arc<PlanBModel>> {
    let path = model_path(asset);
    let mut slot = lock(&g.model);
    let due = slot.last_check.is_none_or(|t| t.elapsed() >= Duration::from_secs(MODEL_CHECK_SECS));
    if due && !slot.loading {
        slot.last_check = Some(Instant::now());
        let mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
        match mtime {
            None => {
                if warn_due(&mut slot.last_warn) {
                    warn!("GBoost plan-B: model file {} not found; the viper stays idle until it exists", path.display());
                }
            }
            Some(m) if Some(m) != slot.mtime => {
                slot.loading = true;
                tokio::task::spawn_blocking(move || {
                    let result = load_model(&path);
                    let mut slot = lock(&g.model);
                    slot.loading = false;
                    slot.mtime = Some(m); // a file that fails is not retried until it changes
                    match result {
                        Ok(model) => {
                            info!(
                                "GBoost plan-B: loaded model {} from {} ({} trees, Platt a={:.6} b={:.6})",
                                model.version, path.display(), model.trees, model.platt_a, model.platt_b,
                            );
                            slot.model = Some(Arc::new(model));
                        }
                        Err(e) => warn!("GBoost plan-B: rejected model file {}: {e}", path.display()),
                    }
                });
            }
            Some(_) => {}
        }
    }
    slot.model.clone()
}

fn http() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .build()
            .unwrap_or_default()
    })
}

/// Parse a Binance `/api/v3/klines` response.
pub fn parse_klines(v: &serde_json::Value) -> std::result::Result<Vec<Bar>, String> {
    let rows = v.as_array().ok_or("klines response is not an array")?;
    let num = |x: &serde_json::Value| x.as_str().and_then(|s| s.parse::<f64>().ok());
    rows.iter()
        .map(|r| {
            let r = r.as_array().ok_or("kline is not an array")?;
            if r.len() < 5 { return Err("kline has fewer than 5 fields".to_string()); }
            Ok(Bar {
                open_s: r[0].as_i64().ok_or("kline open time is not an integer")? / 1000,
                open: num(&r[1]).ok_or("bad open")?,
                high: num(&r[2]).ok_or("bad high")?,
                low: num(&r[3]).ok_or("bad low")?,
                close: num(&r[4]).ok_or("bad close")?,
            })
        })
        .collect()
}

/// Parse a Binance `/fapi/v1/fundingRate?limit=1` response into (rate, funding time in seconds).
pub fn parse_funding(v: &serde_json::Value) -> std::result::Result<(f64, i64), String> {
    let row = v.as_array().and_then(|a| a.last()).ok_or("funding response has no rows")?;
    let rate = row.get("fundingRate").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).ok_or("bad fundingRate")?;
    let time_ms = row.get("fundingTime").and_then(|x| x.as_i64()).ok_or("bad fundingTime")?;
    Ok((rate, time_ms / 1000))
}

async fn get_json(url: &str) -> std::result::Result<serde_json::Value, String> {
    let resp = http().get(url).send().await.map_err(|e| e.to_string())?;
    let status = resp.status();
    if !status.is_success() { return Err(format!("HTTP {status}")); }
    resp.json::<serde_json::Value>().await.map_err(|e| e.to_string())
}

async fn fetch_bars() -> std::result::Result<Vec<Bar>, String> {
    // The data mirror serves the same klines and answers where the main host is blocked.
    let mut last = String::new();
    for host in ["https://api.binance.com", "https://data-api.binance.vision"] {
        match get_json(&format!("{host}/api/v3/klines?symbol=BTCUSDT&interval=1m&limit={KLINES_LIMIT}")).await {
            Ok(v) => return parse_klines(&v),
            Err(e) => last = format!("{host}: {e}"),
        }
    }
    Err(last)
}

async fn fetch_funding() -> std::result::Result<(f64, i64), String> {
    parse_funding(&get_json("https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=1").await?)
}

/// Closed bars through `t` and the settled funding rate, fetching in the background
/// until both are in hand.
fn ensure_minute_data(g: &'static PlanBGlobals, t: i64) -> Option<(Vec<Bar>, f64)> {
    let mut d = lock(&g.data);
    let funding_due = !d.funding_fetching
        && d.funding_fetched.is_none_or(|x| x.elapsed() >= Duration::from_secs(FUNDING_REFRESH_SECS));
    if funding_due {
        d.funding_fetching = true;
        tokio::spawn(async move {
            let result = fetch_funding().await;
            let mut d = lock(&g.data);
            d.funding_fetching = false;
            match result {
                Ok(f) => {
                    d.funding = Some(f);
                    d.funding_fetched = Some(Instant::now());
                }
                Err(e) => {
                    // Retry in 30 s rather than waiting a full refresh period.
                    d.funding_fetched = Instant::now().checked_sub(Duration::from_secs(FUNDING_REFRESH_SECS - 30));
                    if warn_due(&mut d.last_warn) {
                        warn!("GBoost plan-B: settled funding rate unavailable ({e}); decisions wait for it");
                    }
                }
            }
        });
    }
    if d.bars_t != Some(t) && d.fetching_t != Some(t) {
        d.fetching_t = Some(t);
        tokio::spawn(async move {
            for _ in 0..3 {
                match fetch_bars().await {
                    Ok(mut bars) => {
                        bars.retain(|b| b.open_s <= t - 60);
                        if bars.last().map(|b| b.open_s) == Some(t - 60) {
                            let mut d = lock(&g.data);
                            d.bars = bars;
                            d.bars_t = Some(t);
                            d.fetching_t = None;
                            return;
                        }
                    }
                    Err(e) => {
                        let mut d = lock(&g.data);
                        if warn_due(&mut d.last_warn) {
                            warn!("GBoost plan-B: Binance klines unavailable ({e})");
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(1500)).await;
            }
            let mut d = lock(&g.data);
            if d.fetching_t == Some(t) { d.fetching_t = None; }
        });
    }
    let funding = d.funding.filter(|(_, settled)| *settled <= t).map(|(rate, _)| rate)?;
    (d.bars_t == Some(t)).then(|| (d.bars.clone(), funding))
}

fn fmt_features(v: &[f64; N_FEATURES]) -> String {
    let parts: Vec<String> = v.iter().map(|x| if x.is_nan() { "nan".to_string() } else { format!("{x}") }).collect();
    format!("[{}]", parts.join(","))
}

// ── Strategy ─────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct GboostPlanBStrategy;

impl GboostPlanBStrategy {
    pub fn new() -> Self { Self }
}

#[async_trait]
impl Strategy for GboostPlanBStrategy {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        let idle = |r: &str| crate::helpers::viper_status::report_reason(&ctx.crypto_filter, STRATEGY_NAME, r);
        if !dc.enable_gboost {
            idle("disabled in config");
            return Ok(StrategySignal::NoSignal);
        }
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            idle("session drawdown limit hit");
            return Ok(StrategySignal::NoSignal);
        }
        if !ctx.crypto_filter.eq_ignore_ascii_case("btc") {
            idle("the plan-B model is trained on BTC hourly markets only");
            return Ok(StrategySignal::NoSignal);
        }
        if dc.gboost_planb_first_minute > dc.gboost_planb_last_minute || dc.gboost_planb_min_ask > dc.gboost_planb_max_ask {
            idle("misconfigured: first decision minute after the last, or min ask above max ask");
            return Ok(StrategySignal::NoSignal);
        }
        let g = globals(&ctx.crypto_filter);
        // Checked on every tick, so a fresh start has the model loaded by its first decision minute.
        let model = ensure_model(g, &ctx.crypto_filter);
        let market = &ctx.market;
        let snap = &ctx.snapshot;
        let Some(close) = market.market_close_time else {
            idle("market has no close time");
            return Ok(StrategySignal::NoSignal);
        };
        let w_end = close.timestamp();
        let w = w_end - 3600;
        let now = Utc::now();
        let now_s = now.timestamp();
        if now_s < w || now_s >= w_end {
            idle("outside the hourly window");
            return Ok(StrategySignal::NoSignal);
        }
        let cid = market.condition_id.clone();
        let minute_start = w + (now_s - w) / 60 * 60;

        // Record each minute's mids from the first fresh tick after its boundary.
        if (now - snap.timestamp).num_seconds() <= config::GBOOST_MAX_SNAPSHOT_AGE_SECS {
            let mut mids = lock(&g.mids);
            let series = mids.entry(cid.clone()).or_default();
            series.entry(minute_start).or_insert([book_mid(snap.yes_bid, snap.yes_ask), book_mid(snap.no_bid, snap.no_ask)]);
            mids.retain(|_, s| s.last_key_value().is_some_and(|(k, _)| now_s - k < 7200));
        }

        let k = (now_s - w) / 60;
        let t = minute_start;
        if k < dc.gboost_planb_first_minute || k > dc.gboost_planb_last_minute {
            idle("outside the model's decision minutes");
            return Ok(StrategySignal::NoSignal);
        }
        if now_s + MIN_HOLD_BEFORE_FLATTEN_SECS > rotation_flatten_at(w_end) {
            idle("too close to the market rotation to manage a new position");
            return Ok(StrategySignal::NoSignal);
        }
        if now_s < t + BAR_SETTLE_SECS || lock(&g.decided).contains(&(cid.clone(), t)) {
            return Ok(StrategySignal::NoSignal);
        }
        if now_s > t + DECISION_STALE_SECS {
            lock(&g.decided).insert((cid.clone(), t));
            idle("decision minute passed before its data arrived");
            return Ok(StrategySignal::NoSignal);
        }
        let Some(model) = model else {
            idle("model not loaded");
            return Ok(StrategySignal::NoSignal);
        };
        let Some((bars, funding)) = ensure_minute_data(g, t) else {
            idle("waiting for Binance bars and the settled funding rate");
            return Ok(StrategySignal::NoSignal);
        };

        let (mid_now, mid_m1, mid_m5) = {
            let mids = lock(&g.mids);
            let s = mids.get(&cid);
            ([mid_at(s, 0, t), mid_at(s, 1, t)], [mid_at(s, 0, t - 60), mid_at(s, 1, t - 60)], [mid_at(s, 0, t - 300), mid_at(s, 1, t - 300)])
        };
        let ask = [snap.yes_ask.to_f64().unwrap_or(0.0), snap.no_ask.to_f64().unwrap_or(0.0)];
        {
            let mut decided = lock(&g.decided);
            decided.insert((cid.clone(), t));
            decided.retain(|(_, dt)| now_s - dt < 7200);
        }
        let inputs = DecisionInputs { w, t, bars: &bars, mid_now, mid_m1, mid_m5, ask, funding };
        let features = match build_features(&inputs) {
            Ok(f) => f,
            Err(gap) => {
                info!("GBoost plan-B [{}] minute {}: no decision, inputs incomplete ({:?})", market.market_name, k, gap);
                idle("inputs incomplete for this minute");
                return Ok(StrategySignal::NoSignal);
            }
        };
        let preds = model.predict(&features);
        let f = |d: Decimal| d.to_f64().unwrap_or(0.0);
        let decisions: Vec<SideDecision> = (0..2)
            .map(|side| decide_side(
                ask[side], preds[side].1, f(dc.intl_taker_fee_rate), f(dc.gboost_planb_take_profit_pct),
                f(dc.gboost_planb_stop_loss_pct), f(dc.gboost_planb_margin), f(dc.gboost_planb_min_ask), f(dc.gboost_planb_max_ask),
            ))
            .collect();
        let label = ["YES", "NO"];
        info!(
            "GBoost plan-B [{}] t={} k={} model={} | YES ask={:.3} raw={:.4} p={:.4} need={:.4} ({}) | NO ask={:.3} raw={:.4} p={:.4} need={:.4} ({}) | features YES={} NO={}",
            market.market_name, t, k, model.version,
            ask[0], preds[0].0, preds[0].1, decisions[0].required, decisions[0].reason,
            ask[1], preds[1].0, preds[1].1, decisions[1].required, decisions[1].reason,
            fmt_features(&features[0]), fmt_features(&features[1]),
        );

        let best = {
            let attempted = lock(&g.attempted);
            (0..2)
                .filter(|&s| decisions[s].qualifies && !attempted.contains(&(cid.clone(), s)))
                .max_by(|&a, &b| (preds[a].1 - decisions[a].required).total_cmp(&(preds[b].1 - decisions[b].required)))
        };
        let Some(side) = best else {
            idle("no side clears break-even plus margin");
            return Ok(StrategySignal::NoSignal);
        };

        let token_id = if side == 0 { market.yes_token.clone() } else { market.no_token.clone() };
        let ask_dec = if side == 0 { snap.yes_ask } else { snap.no_ask };
        let fee_bps = if side == 0 { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 };
        let room = {
            let positions = ctx.positions.lock().await;
            let mine = |tok: &crate::venues::core::MarketId| positions.get(&PositionKey::new(&ctx.squadron_id, STRATEGY_NAME, tok.clone()));
            let held = [mine(&market.yes_token), mine(&market.no_token)];
            if held.iter().any(Option::is_some) {
                // Only a confirmed fill spends the side: a provisional position can still be
                // removed as a phantom, and that entry should stay retryable.
                let mut attempted = lock(&g.attempted);
                for s in (0..2).filter(|&s| held[s].is_some_and(|p| p.fill_effective_at(dc.ghost_mode).is_some())) {
                    attempted.insert((cid.clone(), s));
                }
                idle("position already open on this market");
                return Ok(StrategySignal::NoSignal);
            }
            // A position whose market has closed waits on redemption, not risk.
            let exposure: Decimal = positions.iter()
                .filter(|(key, _)| key.strategy == STRATEGY_NAME && key.squadron == ctx.squadron_id)
                .filter(|(_, p)| p.counts_toward_exposure(now))
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum();
            dc.gboost_max_exposure_usdc - exposure
        };
        let shares = match entry_shares(dc.gboost_planb_trade_size_usdc, room, ask_dec, dc.intl_taker_fee_rate) {
            Ok(s) => s,
            Err(why) => {
                info!("GBoost plan-B [{}] {} qualifies but is not entered: {why} (room ${:.2})", market.market_name, label[side], room);
                idle(why);
                return Ok(StrategySignal::NoSignal);
            }
        };
        if ctx.available_collateral < shares * ask_dec {
            idle("insufficient collateral");
            return Ok(StrategySignal::NoSignal);
        }

        if dc.gboost_shadow_mode {
            lock(&g.attempted).insert((cid.clone(), side));
            info!(
                "GBoost plan-B [{}] shadow mode: would buy {} at ${:.3} x {:.2} (p={:.4} need={:.4})",
                market.market_name, label[side], ask[side], shares, preds[side].1, decisions[side].required,
            );
            idle("shadow mode: signal logged, not traded");
            return Ok(StrategySignal::NoSignal);
        }

        // A live side is marked attempted once its position is seen (above, or in the exit
        // pass), not here: an entry the patrol drops (a cooldown, a pending order) or the book
        // kills unfilled stays eligible at the next decision minute.
        info!(
            "GBoost plan-B [{}] ENTRY {} at ${:.3} x {:.2} (p={:.4} break-even={:.4} need={:.4}, model {})",
            market.market_name, label[side], ask[side], shares, preds[side].1, decisions[side].break_even, decisions[side].required, model.version,
        );
        crate::helpers::metrics::stash_entry_signals_json_for(STRATEGY_NAME, token_id.as_str(), serde_json::json!({
            "viper": "GBoostPlanB",
            "model_version": model.version,
            "side": label[side],
            "t": t,
            "minute": k,
            "ask": ask[side],
            "raw_p": preds[side].0,
            "p": preds[side].1,
            "break_even": decisions[side].break_even,
            "required": decisions[side].required,
            "feature_names": FEATURE_NAMES,
            "features": features[side].iter().map(|x| if x.is_nan() { serde_json::Value::Null } else { serde_json::json!(x) }).collect::<Vec<_>>(),
        }));
        Ok(StrategySignal::Entry {
            params: OrderParams {
                token_id,
                price: ask_dec,
                shares,
                fee_bps,
                is_neg_risk: market.is_neg_risk,
                market_name: market.market_name.clone(),
                condition_id: market.condition_id.clone(),
                order_type: TimeInForce::Fak,
                post_only: false,
                ghost_mode: dc.ghost_mode,
            },
            pair_params: None,
        })
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        let g = globals(&ctx.crypto_filter);
        let now_s = Utc::now().timestamp();
        let positions = ctx.positions.lock().await;
        let mut resting: Vec<StrategySignal> = Vec::new();

        for (key, position) in positions.iter() {
            if key.squadron != ctx.squadron_id || key.strategy != STRATEGY_NAME {
                continue;
            }
            let token_id = &key.market;
            let Some((market, snap)) = crate::vipers::venue_for_token(ctx, token_id) else {
                crate::vipers::note_position_without_venue(STRATEGY_NAME, token_id);
                continue;
            };
            // Only a confirmed fill owns shares that can back a sell.
            if position.fill_effective_at(dc.ghost_mode).is_none() {
                continue;
            }
            let token_is_yes = token_id == &market.yes_token;
            lock(&g.attempted).insert((market.condition_id.clone(), if token_is_yes { 0 } else { 1 }));
            if position.shares < VENUE_MIN_SHARES {
                // Nothing below the venue minimum can be rested, and a sell that the venue
                // rejects would only loop through fault cooldowns, so the remainder settles.
                if lock(&g.below_minimum_noted).insert(token_id.to_string()) {
                    info!(
                        "GBoost plan-B [{}] holding {:.2} shares to settlement: below the venue's {}-share minimum, no exit order can be placed",
                        market.market_name, position.shares, VENUE_MIN_SHARES,
                    );
                }
                continue;
            }
            let bid = if token_is_yes { snap.yes_bid } else { snap.no_bid };
            let entry = position.avg_entry;
            let params = |price: Decimal, order_type: TimeInForce, post_only: bool| OrderParams {
                token_id: token_id.clone(),
                price,
                shares: position.shares,
                fee_bps: if token_is_yes { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                is_neg_risk: market.is_neg_risk,
                market_name: market.market_name.clone(),
                condition_id: market.condition_id.clone(),
                order_type,
                post_only,
                ghost_mode: dc.ghost_mode,
            };
            if market.market_close_time.is_some_and(|c| now_s >= rotation_flatten_at(c.timestamp())) && bid > Decimal::ZERO {
                return Ok(StrategySignal::Exit {
                    params: params(bid, TimeInForce::Fak, false),
                    reason: format!("GBoostPlanBRotation: bid=${:.4} entry=${:.4}, flattened before the market rotation ends exit management", bid, entry),
                    exit_pair: false,
                });
            }
            match exit_action(entry, bid, dc.gboost_planb_take_profit_pct, dc.gboost_planb_stop_loss_pct, dc.gboost_planb_tp_ceiling, dc.gboost_resting_tp_enabled) {
                ExitAction::Stop => {
                    return Ok(StrategySignal::Exit {
                        params: params(bid, TimeInForce::Fak, false),
                        reason: format!("GBoostPlanBSL: bid=${:.4} stop=${:.4} entry=${:.4}", bid, entry * (Decimal::ONE - dc.gboost_planb_stop_loss_pct), entry),
                        exit_pair: false,
                    });
                }
                ExitAction::TakeProfit => {
                    return Ok(StrategySignal::Exit {
                        params: params(bid, TimeInForce::Fak, false),
                        reason: format!("GBoostPlanBTP: bid=${:.4} entry=${:.4}", bid, entry),
                        exit_pair: false,
                    });
                }
                ExitAction::Rest(target) if target > bid => {
                    resting.push(StrategySignal::MakerRestingExit {
                        params: params(target, TimeInForce::Gtc, true),
                        reason: format!(
                            "GBoostPlanBRestingTP: ask=${:.4} entry=${:.4} target={:+.2}%",
                            target, entry, (target - entry) / entry * dec!(100),
                        ),
                    });
                }
                _ => {}
            }
        }

        // One signal leaves per tick; rotate so every healthy position's ask is placed.
        if !resting.is_empty() {
            static ROTATION: AtomicUsize = AtomicUsize::new(0);
            let i = ROTATION.fetch_add(1, Ordering::Relaxed) % resting.len();
            return Ok(resting.swap_remove(i));
        }
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { STRATEGY_NAME.to_string() }
    fn venue(&self) -> &'static str { "Hourly" }
    fn max_exposure(&self) -> Decimal { config::GBOOST_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Plan B: resting take-profit, taker stop, settle" }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opt(v: &serde_json::Value) -> Option<f64> { v.as_f64() }

    /// Eight rows the research harness built from real Phase 1 markets
    /// (`gen_feature_fixtures.py`): the live builder, fed the same bars, mids, asks
    /// and funding, must reproduce every input the model was trained on.
    #[test]
    fn the_builder_reproduces_the_harness_rows() {
        let fx: serde_json::Value = serde_json::from_str(include_str!("testdata/gboost_planb_features.json")).unwrap();
        let names: Vec<&str> = fx["feature_names"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(names, FEATURE_NAMES.to_vec(), "fixture column order must match the model's");
        let fixtures = fx["fixtures"].as_array().unwrap();
        assert!(fixtures.len() >= 6, "expected the committed fixture set");
        for fixture in fixtures {
            let bars: Vec<Bar> = fixture["bars"].as_array().unwrap().iter().map(|b| {
                let b = b.as_array().unwrap();
                Bar { open_s: b[0].as_i64().unwrap(), open: b[1].as_f64().unwrap(), high: b[2].as_f64().unwrap(), low: b[3].as_f64().unwrap(), close: b[4].as_f64().unwrap() }
            }).collect();
            let pair = |k: &str| [opt(&fixture["mids"][k][0]), opt(&fixture["mids"][k][1])];
            let inputs = DecisionInputs {
                w: fixture["W"].as_i64().unwrap(),
                t: fixture["t"].as_i64().unwrap(),
                bars: &bars,
                mid_now: pair("now"),
                mid_m1: pair("m1"),
                mid_m5: pair("m5"),
                ask: [fixture["ask"][0].as_f64().unwrap(), fixture["ask"][1].as_f64().unwrap()],
                funding: fixture["funding"].as_f64().unwrap(),
            };
            let got = build_features(&inputs).expect("fixture inputs are complete");
            for side in 0..2 {
                let expected = fixture["expected"][side].as_array().unwrap();
                for (j, name) in FEATURE_NAMES.iter().enumerate() {
                    match expected[j].as_f64() {
                        None => assert!(got[side][j].is_nan(), "t={} side {side} {name}: expected missing, got {}", inputs.t, got[side][j]),
                        // norm_cdf is a 1.5e-7 approximation of the harness's erf; everything else is exact arithmetic.
                        Some(e) => assert!((got[side][j] - e).abs() < 1e-6, "t={} side {side} {name}: got {} expected {e}", inputs.t, got[side][j]),
                    }
                }
            }
        }
    }

    #[test]
    fn missing_history_or_mids_are_reported_not_guessed() {
        let bars: Vec<Bar> = (0..61).map(|i| Bar { open_s: 3600 + 60 * i, open: 1.0, high: 1.0, low: 1.0, close: 1.0 }).collect();
        let base = |bars: &[Bar]| build_features(&DecisionInputs {
            w: 7200, t: 3600 + 61 * 60, bars, mid_now: [Some(0.5), Some(0.5)], mid_m1: [None; 2], mid_m5: [None; 2], ask: [0.5, 0.5], funding: 0.0,
        });
        assert_eq!(base(&bars[1..]).unwrap_err(), FeatureGap::MissingBars);
        let mut with_strike = bars.clone();
        with_strike.retain(|b| b.open_s != 7200);
        assert_eq!(base(&with_strike).unwrap_err(), FeatureGap::MissingBars);
        let ok = build_features(&DecisionInputs {
            w: 3600 + 30 * 60, t: 3600 + 61 * 60, bars: &bars, mid_now: [Some(0.5), None], mid_m1: [None; 2], mid_m5: [None; 2], ask: [0.5, 0.5], funding: 0.0,
        });
        assert_eq!(ok.unwrap_err(), FeatureGap::MissingMid);
    }

    /// The harness's `break_even` at a $0.50 ask, 20% target, 11% stop, 7% fee rate.
    #[test]
    fn break_even_matches_the_harness_formula() {
        let be = break_even(0.50, 0.20, 0.11, 0.07);
        assert!((be - 0.521151).abs() < 1e-5, "break-even {be}");
        // The fee is rate × p × (1 − p) per share, a larger share of a cheaper contract, so the bottom
        // of the band needs the higher win rate: about 0.54 at $0.43 against about 0.45 at $0.75.
        assert!((break_even(0.43, 0.20, 0.11, 0.07) - 0.5406).abs() < 1e-3);
        assert!((break_even(0.75, 0.20, 0.11, 0.07) - 0.4481).abs() < 1e-3);
    }

    #[test]
    fn calibration_is_identity_at_unit_slope_and_clips() {
        assert!((calibrate(0.3, 1.0, 0.0) - 0.3).abs() < 1e-12);
        assert_eq!(calibrate(0.0, 1.0, 0.0), 1e-4);
        assert_eq!(calibrate(1.0, 1.0, 0.0), 1.0 - 1e-4);
    }

    #[test]
    fn a_side_qualifies_only_inside_the_band_and_above_break_even_plus_margin() {
        let d = |ask: f64, p: f64| decide_side(ask, p, 0.07, 0.20, 0.11, 0.10, 0.43, 0.75);
        let need = break_even(0.50, 0.20, 0.11, 0.07) + 0.10;
        assert!(d(0.50, need + 0.001).qualifies);
        assert!(!d(0.50, need - 0.001).qualifies);
        assert_eq!(d(0.42, 0.99).reason, "ask outside the trained band");
        assert_eq!(d(0.76, 0.99).reason, "ask outside the trained band");
        assert_eq!(d(0.0, 0.99).reason, "no usable ask");
    }

    #[test]
    fn exits_stop_take_profit_rest_or_hold() {
        let act = |entry, bid, resting| exit_action(entry, bid, dec!(0.20), dec!(0.11), dec!(0.90), resting);
        // $0.50 entry: stop at $0.445, target $0.60.
        assert_eq!(act(dec!(0.50), dec!(0.44), true), ExitAction::Stop);
        assert_eq!(act(dec!(0.50), dec!(0.445), true), ExitAction::Stop);
        assert_eq!(act(dec!(0.50), dec!(0.61), true), ExitAction::TakeProfit);
        assert_eq!(act(dec!(0.50), dec!(0.52), true), ExitAction::Rest(dec!(0.60)));
        assert_eq!(act(dec!(0.50), dec!(0.52), false), ExitAction::Hold);
        // No bid is not a stop.
        assert_eq!(act(dec!(0.50), dec!(0), false), ExitAction::Hold);
        // $0.75 entry: 20% would be $0.90, capped at the ceiling.
        assert_eq!(take_profit_target(dec!(0.75), dec!(0.20), dec!(0.90)), dec!(0.90));
        assert_eq!(take_profit_target(dec!(0.43), dec!(0.20), dec!(0.90)), dec!(0.52));
    }

    #[test]
    fn entry_size_is_raised_to_the_venue_minimum_and_refused_past_the_cap() {
        // The shipped defaults: $4 per trade inside a $4 cap buys at every ask in the trained band.
        // $0.75: $0.763 a share with the fee, 5.24 shares. $0.43: $0.447 a share, 8.94 shares.
        assert_eq!(entry_shares(dec!(4), dec!(4), dec!(0.75), dec!(0.07)), Ok(dec!(5.24)));
        assert_eq!(entry_shares(dec!(4), dec!(4), dec!(0.43), dec!(0.07)), Ok(dec!(8.94)));
        for cents in 43..=75 {
            assert!(entry_shares(dec!(4), dec!(4), Decimal::new(cents, 2), dec!(0.07)).is_ok(), "ask {cents}");
        }
        // $3 buys 3.93 shares at $0.75: raised to the 5-share minimum, about $3.82 with the fee.
        assert_eq!(entry_shares(dec!(3), dec!(4), dec!(0.75), dec!(0.07)), Ok(dec!(5)));
        assert!(entry_shares(dec!(3), dec!(3.5), dec!(0.75), dec!(0.07)).is_err());
        assert!(entry_shares(dec!(4), dec!(4), dec!(0), dec!(0.07)).is_err());
    }

    #[test]
    fn positions_are_flattened_before_the_engine_rotates_markets() {
        let close = 10_000_000;
        let at = rotation_flatten_at(close);
        assert!(at < close - config::FINAL_EXPIRY_WINDOW_SECS, "the flatten must come before the market switch");
        // The last trained decision minute still leaves room to hold before it.
        let w = close - 3600;
        assert!(w + 45 * 60 + BAR_SETTLE_SECS + MIN_HOLD_BEFORE_FLATTEN_SECS <= at);
    }

    #[test]
    fn a_mid_older_than_the_staleness_limit_is_missing() {
        let mut s = BTreeMap::new();
        s.insert(1000, [Some(0.40), Some(0.61)]);
        assert_eq!(mid_at(Some(&s), 0, 1000), Some(0.40));
        assert_eq!(mid_at(Some(&s), 1, 1000 + MID_STALE_SECS), Some(0.61));
        assert_eq!(mid_at(Some(&s), 0, 1000 + MID_STALE_SECS + 1), None);
        assert_eq!(mid_at(Some(&s), 0, 999), None);
        assert_eq!(mid_at(None, 0, 1000), None);
    }

    #[test]
    fn binance_responses_parse() {
        let k: serde_json::Value = serde_json::from_str(
            r#"[[1789041600000,"77268.09","77300.00","77200.01","77250.50","12.3",1789041659999,"1","2","3","4","0"]]"#,
        ).unwrap();
        assert_eq!(parse_klines(&k).unwrap(), vec![Bar { open_s: 1789041600, open: 77268.09, high: 77300.0, low: 77200.01, close: 77250.50 }]);
        let f: serde_json::Value = serde_json::from_str(
            r#"[{"symbol":"BTCUSDT","fundingTime":1789056000002,"fundingRate":"0.00007199","markPrice":"77218.5"}]"#,
        ).unwrap();
        assert_eq!(parse_funding(&f).unwrap(), (0.00007199, 1789056000));
    }

    /// Live smoke run of the path this viper trades on: the real kline fetch and its bar
    /// alignment, the model loaded through the hot-reload path, the current BTC hourly
    /// market's book, features and a decision. Needs network access, the model file at
    /// `GBOOST_PLANB_TEST_MODEL` and a settled funding rate in `GBOOST_PLANB_SMOKE_FUNDING`
    /// (Binance futures refuse US addresses). Run at minute 1 or later of the hour.
    #[tokio::test]
    #[ignore = "needs network, the model file and a settled funding rate"]
    async fn live_smoke_scores_the_current_market() {
        let asset = "smoketest";
        let link = model_path(asset);
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(std::env::var("GBOOST_PLANB_TEST_MODEL").unwrap(), &link).unwrap();
        let g = globals(asset);
        let now = Utc::now().timestamp();
        let t = now.div_euclid(60) * 60;
        let w = now.div_euclid(3600) * 3600;
        {
            let mut d = lock(&g.data);
            d.funding = Some((std::env::var("GBOOST_PLANB_SMOKE_FUNDING").unwrap().parse().unwrap(), t - 3600));
            d.funding_fetched = Some(Instant::now());
        }
        let deadline = Instant::now() + Duration::from_secs(90);
        let (model, (bars, funding)) = loop {
            if let (Some(m), Some(d)) = (ensure_model(g, asset), ensure_minute_data(g, t)) {
                break (m, d);
            }
            assert!(Instant::now() < deadline, "the model or the bars did not arrive");
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        let _ = std::fs::remove_file(&link);
        assert_eq!(bars.iter().map(|b| b.open_s).max(), Some(t - 60), "the newest bar kept is the one that closed at t");

        let events = get_json("https://gamma-api.polymarket.com/events?tag_slug=bitcoin&closed=false&limit=100&order=endDate&ascending=true").await.unwrap();
        let market = events.as_array().unwrap().iter()
            .flat_map(|e| e["markets"].as_array().cloned().unwrap_or_default())
            .find(|m| m["slug"].as_str().is_some_and(|s| s.starts_with("bitcoin-up-or-down-"))
                && m["endDate"].as_str().and_then(|d| chrono::DateTime::parse_from_rfc3339(d).ok()).is_some_and(|d| d.timestamp() == w + 3600))
            .expect("the current BTC hourly market");
        let tokens: Vec<String> = serde_json::from_str(market["clobTokenIds"].as_str().unwrap()).unwrap();
        let (mut bid, mut ask) = ([0.0f64; 2], [1.0f64; 2]);
        for (i, tok) in tokens.iter().enumerate().take(2) {
            let book = get_json(&format!("https://clob.polymarket.com/book?token_id={tok}")).await.unwrap();
            let px = |side: &str| book[side].as_array().unwrap().iter().filter_map(|l| l["price"].as_str()?.parse::<f64>().ok()).collect::<Vec<_>>();
            bid[i] = px("bids").into_iter().fold(0.0, f64::max);
            ask[i] = px("asks").into_iter().fold(1.0, f64::min);
        }
        let mid = |i: usize| (bid[i] > 0.0 && ask[i] < 1.0).then(|| (bid[i] + ask[i]) / 2.0);
        let inputs = DecisionInputs { w, t, bars: &bars, mid_now: [mid(0), mid(1)], mid_m1: [None; 2], mid_m5: [None; 2], ask, funding };
        let features = build_features(&inputs).expect("live inputs are complete");
        let preds = model.predict(&features);
        println!("{} minute {} model {} ({} trees)", market["slug"], (t - w) / 60, model.version, model.trees);
        for side in 0..2 {
            let d = decide_side(ask[side], preds[side].1, 0.07, 0.20, 0.11, 0.10, 0.43, 0.75);
            println!(
                "  {} bid {:.3} ask {:.3} raw {:.4} p {:.4} need {:.4} ({})",
                ["YES", "NO"][side], bid[side], ask[side], preds[side].0, preds[side].1, d.required, d.reason,
            );
            println!("  features {}", fmt_features(&features[side]));
            assert!(preds[side].0.is_finite() && preds[side].1 > 0.0 && preds[side].1 < 1.0);
        }
    }

    /// The engine must score the trained model exactly as the offline exporter does.
    /// Run with `GBOOST_PLANB_TEST_MODEL` (the stamped model file) and
    /// `GBOOST_PLANB_TEST_GOLDEN` (its golden_predictions.json) set.
    #[test]
    #[ignore = "needs the trained model file and its golden predictions"]
    fn the_engine_scores_the_trained_model_exactly_as_the_exporter() {
        let model = load_model(std::path::Path::new(&std::env::var("GBOOST_PLANB_TEST_MODEL").unwrap())).expect("model loads");
        let golden: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(std::env::var("GBOOST_PLANB_TEST_GOLDEN").unwrap()).unwrap()).unwrap();
        let rows: Vec<[f64; N_FEATURES]> = golden["rows"].as_array().unwrap().iter().map(|r| {
            let mut v = [0.0; N_FEATURES];
            for (j, x) in r["features"].as_array().unwrap().iter().enumerate() { v[j] = x.as_f64().unwrap_or(f64::NAN); }
            v
        }).collect();
        let preds = model.predict(&rows);
        for (i, r) in golden["rows"].as_array().unwrap().iter().enumerate() {
            assert_eq!(preds[i].0, r["raw"].as_f64().unwrap(), "row {i}: raw probability must match exactly");
            assert!((preds[i].1 - r["calibrated"].as_f64().unwrap()).abs() < 1e-12, "row {i}: calibrated probability");
        }
        assert!((model.platt_a - golden["platt_a"].as_f64().unwrap()).abs() < 1e-15);
        println!("{} rows matched; model {} with {} trees", rows.len(), model.version, model.trees);
    }
}
