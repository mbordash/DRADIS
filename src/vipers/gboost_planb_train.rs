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

//! GBoost plan B: the in-engine training pipeline (2026-09-13).
//!
//! Every DRADIS instance trains its own plan-B model from public data, so an operator
//! who starts from the Marketplace AMI with no files from anyone else ends up with a
//! model that was built, calibrated and validated on their own box, and kept current
//! there. Nothing in the pipeline is new science: it is the offline research harness
//! that produced the first production model (row building, walk-forward evaluation,
//! Platt calibration and export), ported so that the rows it trains on are built by the
//! same `build_features` the viper scores with.
//!
//! # What runs, and where
//!
//! One supervised tokio task per BTC asset (`run_pipeline`), spawned from `main.rs` on
//! the Polymarket International build only, because the markets plan B trades and the
//! public history it learns from are that venue's. The task loops every few minutes:
//!
//! 1. **Catch up the data store** (`logs/gboost_planb/{asset}/`). For every hourly
//!    window inside the training window that has resolved and has no record yet, it
//!    fetches the Gamma market (by slug, year included), the data-api trade prints and
//!    the CLOB one-minute price history of both tokens, and writes one JSON record in
//!    the same shape the research fetcher wrote, so the reference scripts can read what
//!    the engine fetched. Binance one-minute bars come from the official data mirror by
//!    UTC day, and the settled funding history from fapi. Only Binance's official hosts
//!    are used: where fapi answers HTTP 451 (US addresses) there is no funding source,
//!    the rows carry no funding, the fit zeroes the column and stamps it, and the viper
//!    serves that model without a live rate; that viper cannot reach fapi either, so
//!    training and serving agree by construction. A fresh instance backfills the whole
//!    window this way; a running one picks up each hour's market once it has resolved,
//!    which is how the instance's own trading days join the training set. Network work
//!    is async and paced at about one request a second, because production's address
//!    also carries the engine's own data-API calls.
//! 2. **Train when due** (first cycle after the backfill completes, then every
//!    `gboost_planb_retrain_hours`, or at once when the operator changes the plan the
//!    labels are built for). Rows are built and the booster fit on a dedicated OS
//!    thread, never on the tokio runtime or its blocking pool, with at most two rayon
//!    threads and one on a two-core box.
//!
//! # Validation and adoption
//!
//! The newest `gboost_planb_holdout_days` of markets are the test fold. The candidate
//! is fit on the older 80% of the markets before the fold, Platt-calibrated on the
//! newest 20%, with the harness's 3,600 s purge before both the calibration slice and
//! the fold, and then scored on the fold exactly as the harness scored its folds: the
//! trade rule takes the first minute per market and side whose calibrated probability
//! clears break-even plus the margin, and the fold's return per trade, win rate,
//! market bootstrap and log-loss skill are computed. The gate refuses a candidate with
//! too few trees, no log-loss skill over the base rate, too few trades, a mean return at
//! or below zero, or a win rate under the bar. The model already serving is scored on
//! the same fold with the same rule, and the candidate replaces it only when it does at
//! least as well there; a serving model whose stamped training range overlaps the fold
//! is scored anyway and the overlap is reported, which can only favor the incumbent.
//! The adopted artifact is the validated one, not a refit on all data, so what trades
//! is exactly what passed. It is written with its metadata beside the serving file and
//! renamed into place, so the viper's mtime hot-reload sees a whole file or nothing.
//! The replaced model is kept as `archive/incumbent.json`.
//!
//! Every cycle writes a report under `reports/`, the newest also to `status.json`, and
//! the GBoost card reads the pipeline's one-line status through the viper.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use perpetual::booster::config::BoosterIO;
use perpetual::objective::Objective;
use perpetual::{Matrix, PerpetualBooster};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::helpers::dynamic_config::DynamicConfig;
use crate::vipers::gboost_planb::{
    break_even, build_features, calibrate, load_model, model_path, Bar, DecisionInputs, PlanBModel, FEATURE_NAMES, N_FEATURES,
    TRAINED_VENUE,
};

// ── Mechanism constants (not risk appetite, so not profile constants) ────────

/// Purge between a training market's close and the calibration slice or the test fold,
/// at least the label horizon, as the harness used.
const PURGE_SECS: i64 = 3600;
/// Share of the pre-fold markets (newest first) that fit the Platt calibration.
const CAL_FRACTION: f64 = 0.20;
/// The harness's booster settings that are not tunable: bins, iteration cap.
const MAX_BIN: u16 = 63;
const ITERATION_LIMIT: usize = 1000;
/// A fit that has not converged in this long stops where it is rather than holding a
/// small box's CPU indefinitely.
const TRAIN_TIMEOUT_SECS: f32 = 1800.0;
/// Fewest rows each slice needs before a fit or a score is meaningful (harness values).
const MIN_FIT_ROWS: usize = 500;
const MIN_CAL_ROWS: usize = 100;
const MIN_TEST_ROWS: usize = 50;
/// Fewest trees a candidate may have; the viper's loader refuses less too.
const STRUCTURAL_MIN_TREES: usize = 5;
/// Market bootstrap resamples for the return interval.
const BOOTSTRAP_RESAMPLES: usize = 400;

/// Seconds a taker print counts as evidence of the entry price, before the
/// decision minute under [`EntryRule::LastBefore`] and after it under
/// [`EntryRule::FirstAfter`].
const ENTRY_PRINT_SECS: i64 = 20;
/// Latest an entry may be under [`EntryRule::FirstAfter`]: past this the viper
/// would have abandoned the minute (`DECISION_STALE_SECS`), so a row entered
/// later describes a trade it could not have taken.
const ENTRY_LATEST_SECS: i64 = 60;
/// Half the measured spread: the ask is never below mid plus this.
const HALF_SPREAD: f64 = 0.005;
/// Shares a print must carry to lift a resting take-profit (`orderMinSize`).
const MIN_PRINT_SHARES: f64 = 5.0;
/// A price-history point older than this is not a usable mid.
const MID_STALE_SECS: i64 = 180;
/// Price-history points each side needs before a market is used.
const MIN_HIST_POINTS: usize = 20;
/// Tape pages fetched per market (the harness cap).
const TAPE_PAGE: usize = 1000;
const TAPE_MAX_OFFSET: usize = 9000;
/// Pause after every public API call (about one request a second), and the first
/// backoff after a 429. Production's address also carries the engine's own data-API
/// position checks, which saw a 429 at startup on 2026-09-13.
const REQUEST_PAUSE_MS: u64 = 1000;
const BACKOFF_BASE_SECS: u64 = 2;
const HTTP_ATTEMPTS: u32 = 5;
const HTTP_TIMEOUT_SECS: u64 = 30;
/// A market is fetched only once this long has passed since its close, so Gamma has
/// had time to publish the resolution; one still unresolved after `UNRESOLVED_GIVE_UP`
/// is stored as it is and skipped by the builder.
const RESOLUTION_GRACE_SECS: i64 = 900;
const UNRESOLVED_GIVE_UP_SECS: i64 = 6 * 3600;
/// How often the loop looks for newly resolved markets.
const CATCHUP_INTERVAL_SECS: u64 = 300;
/// Records older than the window by this much are pruned.
const PRUNE_SLACK_SECS: i64 = 7 * 86400;
/// Days of bars kept before the first market (features need an hour, rounding needs a day).
const KLINE_LEAD_DAYS: i64 = 1;

const USER_AGENT: &str = "Mozilla/5.0 (compatible; dradis-engine/1.0)";
const GAMMA: &str = "https://gamma-api.polymarket.com";
const DATA_API: &str = "https://data-api.polymarket.com";
const CLOB: &str = "https://clob.polymarket.com";
/// Spot kline hosts, in order: Binance's official public data mirror answers
/// everywhere; the main host does not answer from the US (HTTP 451).
const KLINE_HOSTS: [&str; 2] = ["https://data-api.binance.vision", "https://api.binance.com"];
/// The only official source of settled funding rates. It answers 451 from the US, and
/// the public archive (`data.binance.vision`) publishes funding by closed month only,
/// which would give past months a rate and the current month none: a model trained on
/// that would need a live rate the same instance cannot fetch. So no archive, no
/// unofficial host: an instance fapi refuses trains and serves without funding.
const FUNDING_HOSTS: [&str; 1] = ["https://fapi.binance.com"];
/// Free memory a training run must find before it starts. The fit runs inside the
/// engine process, so an allocation the machine cannot serve gets the engine killed,
/// not just the fit. The reference fit (66,328 rows, budget 2.0, 2 threads) peaked at
/// 1.16 GB RSS; on a 2 vCPU instance the estimate is 1.5 to 2 GB, so the gate asks
/// for the upper figure. This is headroom, not machine size: a nominal 4 GB instance
/// reports 3.7 GB total after kernel reservations and, with the engine, Control Tower
/// and proxy running, about 3.0 GB available (production t3.medium, 2026-09-13),
/// which is room for a fit. A 2 GB instance has about 1.1 GB available and is not.
const TRAIN_MIN_FREE_MEMORY_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Below this much total memory the machine can never make the room above beside the
/// engine's own ~0.8 GB, whatever else it stops, so the refusal recommends a larger
/// instance. Above it the machine is big enough and something else is using the
/// memory, so the refusal says that instead of naming an instance type it would
/// itself refuse. A t3.medium reports 3.7 GB, a t3.small 1.9 GB.
const TRAIN_MIN_TOTAL_MEMORY_BYTES: u64 = 3 * 1024 * 1024 * 1024;

// ── Data store ───────────────────────────────────────────────────────────────

/// One market's raw inputs, in the shape the research fetcher wrote them so the
/// reference builder can read engine-fetched records.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Print {
    pub ts: i64,
    pub side: String,
    pub o: u8,
    pub p: f64,
    pub s: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct HistPoint {
    pub t: i64,
    pub p: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct MarketRecord {
    pub slug: String,
    #[serde(default)]
    pub missing: bool,
    #[serde(default)]
    pub window_start: String,
    #[serde(default, rename = "conditionId")]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub tokens: Vec<String>,
    #[serde(default, rename = "outcomePrices")]
    pub outcome_prices: Option<String>,
    #[serde(default)]
    pub closed: Option<bool>,
    #[serde(default, rename = "endDate")]
    pub end_date: Option<String>,
    #[serde(default)]
    pub tape: Vec<Print>,
    #[serde(default)]
    pub hist: HashMap<String, Vec<HistPoint>>,
}

impl MarketRecord {
    /// `Some(up_won)` when Gamma has published a 0/1 resolution.
    pub fn up_won(&self) -> Option<bool> {
        let raw = self.outcome_prices.as_deref()?;
        let prices: Vec<String> = serde_json::from_str(raw).ok()?;
        let set: HashSet<&str> = prices.iter().map(String::as_str).collect();
        if set.len() != 2 || !set.contains("0") || !set.contains("1") {
            return None;
        }
        Some(prices[0] == "1")
    }
}

/// Where one asset's pipeline keeps its data and artifacts.
#[derive(Clone, Debug)]
pub struct DataDir {
    pub root: PathBuf,
}

impl DataDir {
    pub fn new(asset: &str) -> Self {
        Self { root: PathBuf::from(format!("logs/gboost_planb/{}", asset.to_ascii_lowercase())) }
    }
    pub fn markets(&self) -> PathBuf { self.root.join("markets") }
    pub fn market(&self, w: i64) -> PathBuf { self.markets().join(format!("{w}.json")) }
    pub fn klines(&self) -> PathBuf { self.root.join("klines") }
    pub fn kline_day(&self, day: chrono::NaiveDate) -> PathBuf { self.klines().join(format!("{day}.json")) }
    pub fn funding(&self) -> PathBuf { self.root.join("funding.json") }
    pub fn candidate(&self) -> PathBuf { self.root.join("candidate.json") }
    pub fn archive(&self) -> PathBuf { self.root.join("archive").join("incumbent.json") }
    pub fn reports(&self) -> PathBuf { self.root.join("reports") }
    pub fn status(&self) -> PathBuf { self.root.join("status.json") }
}

/// Atomic write: the bytes go to `<path>.tmp` in the same directory, are synced, and
/// the temp file is renamed over the target, so a reader (the viper's hot-reload, a
/// restart mid-write) sees the old file or the whole new one and never a prefix.
pub fn write_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir)?;
        }
    }
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

fn write_json_atomically<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    write_atomically(path, &bytes)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", path.display()))
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
struct KlineDay {
    day: String,
    complete: bool,
    /// `[open_s, open, high, low, close]`
    bars: Vec<[f64; 5]>,
}

fn bars_from_day(d: &KlineDay) -> impl Iterator<Item = Bar> + '_ {
    d.bars.iter().map(|b| Bar { open_s: b[0] as i64, open: b[1], high: b[2], low: b[3], close: b[4] })
}

// ── Market slugs and windows ─────────────────────────────────────────────────

/// Gamma slug of the BTC hourly market opening at `w`, year included: without the year
/// Gamma answers with the 2025 market of the same name.
pub fn slug_for(w: i64) -> String {
    // One slug format for the whole engine: live discovery builds the same
    // string in `helpers::time`, so a format drift there would show up in the
    // test below rather than only in a training fetch.
    crate::helpers::time::hourly_market_slug("btc", Utc.timestamp_opt(w, 0).single().unwrap_or_default())
}

fn floor_hour(t: i64) -> i64 { t.div_euclid(3600) * 3600 }

fn utc_day(t: i64) -> chrono::NaiveDate {
    Utc.timestamp_opt(t, 0).single().unwrap_or_default().date_naive()
}

fn rfc3339(t: i64) -> String {
    Utc.timestamp_opt(t, 0).single().map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)).unwrap_or_default()
}

// ── Row builder: the harness's `build_holdout.py`, on the viper's feature code ──

/// Which price a labeled trade is bought at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryRule {
    // The stamp written on a model file and compared before serving lives in
    // `stamp()` below; a file without it predates [B47] and reads as `e1`.
    /// The reference harness's rule: the most recent print-implied ask in the 20 s
    /// BEFORE the minute, else the last price-history sample plus half a spread.
    ///
    /// Kept because the fixture rows in `testdata/gboost_planb_rows.json` come from
    /// that harness, and reproducing them is what proves the engine's builder and
    /// the research code agree. It is not what production trains on: with no print
    /// (64% of eligible rows) it buys at a mid a median 53 s old, from before the
    /// move the decision reacts to, and in the store that unobtainable price was the
    /// only profitable sub-population ([B47]).
    LastBefore,
    /// The first price the market actually showed at or after the minute: a print
    /// within 20 s, else the first price-history sample plus half a spread, and
    /// only while a live decision could still have acted. The exit simulation
    /// starts from that moment.
    ///
    /// The reference harness has not been ported to this rule, so the fixture test
    /// cross-checks `LastBefore` only; this rule's evidence is its unit test and
    /// the store censuses recorded in [B47].
    FirstAfter,
}

impl EntryRule {
    /// The token stamped on a model file and compared by `plan_mismatch`.
    pub fn stamp(&self) -> &'static str {
        match self { EntryRule::LastBefore => "e1", EntryRule::FirstAfter => "e2" }
    }
}

/// The plan the labels are built for, from the squadron's knobs.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    /// Defaults to the old rule when absent, so a report written before [B47]
    /// reads as what it was, and `plan_changed` sees the transition and retrains.
    #[serde(default = "entry_rule_before_b47")]
    pub entry: EntryRule,
    pub tp: f64,
    pub sl: f64,
    pub tp_ceiling: f64,
    pub lo: f64,
    pub hi: f64,
    pub fee: f64,
    pub margin: f64,
    pub first_minute: i64,
    pub last_minute: i64,
}

fn entry_rule_before_b47() -> EntryRule { EntryRule::LastBefore }

impl Plan {
    pub fn from_config(dc: &DynamicConfig, fee: f64) -> Self {
        let f = |d: rust_decimal::Decimal| d.to_f64().unwrap_or(0.0);
        Self {
            entry: EntryRule::FirstAfter,
            tp: f(dc.gboost_planb_take_profit_pct),
            sl: f(dc.gboost_planb_stop_loss_pct),
            tp_ceiling: f(dc.gboost_planb_tp_ceiling),
            lo: f(dc.gboost_planb_min_ask),
            hi: f(dc.gboost_planb_max_ask),
            fee,
            margin: f(dc.gboost_planb_margin),
            first_minute: dc.gboost_planb_first_minute,
            last_minute: dc.gboost_planb_last_minute,
        }
    }
    /// The plan as stamped on a model file, and as the viper compares it.
    ///
    /// Carries the label generation, so a model trained on the old entry price is
    /// refused rather than served: `e1` bought at a stale mid the book never
    /// showed, `e2` buys at the first observable price at or after the decision
    /// minute ([B47]).
    pub fn label(&self) -> String {
        format!("tp{:.4}_sl{:.4}_band{:.4}-{:.4}_{}", self.tp, self.sl, self.lo, self.hi, self.entry.stamp())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExitKind {
    Tp,
    Sl,
    Settle,
}

/// One (market, decision minute, side) training row.
#[derive(Clone, Debug)]
pub struct TrainingRow {
    pub w: i64,
    pub k: i64,
    pub side: usize,
    pub ask: f64,
    pub features: [f64; N_FEATURES],
    pub y: bool,
    pub ret: f64,
    pub exit: ExitKind,
    pub exit_t: i64,
    pub elig: bool,
}

fn fee_per_share(fee: f64, p: f64) -> f64 {
    if p > 0.0 && p < 1.0 { fee * p * (1.0 - p) } else { 0.0 }
}

fn ceil_tick(p: f64) -> f64 { (p / 0.01 - 1e-9).ceil() * 0.01 }

/// The last value at or before `t` in a series sorted by time.
fn last_at<T: Copy>(series: &[(i64, T)], t: i64) -> Option<(usize, T)> {
    let i = series.partition_point(|(ts, _)| *ts <= t);
    if i == 0 { None } else { Some((i - 1, series[i - 1].1)) }
}

/// A side's Polymarket mid at `t` from its CLOB price history (`prices-history`,
/// fidelity 1), sorted by time: the last point at or before `t`, or `None` when that
/// point is more than `MID_STALE_SECS` old.
///
/// The ONE definition of the model's market price, used by the training rows and
/// by the live viper alike ([B46]). Live used to read the order-book mid instead;
/// the history is a once-a-minute sample of that book taken at its own seconds,
/// so in a moving market the two differed by several cents at the same minute,
/// and at 27 live entries the price features sat 4.6 ¢ above what training would
/// have computed, while every Binance feature matched exactly.
pub fn history_mid_at(series: &[(i64, f64)], t: i64) -> Option<f64> {
    let (i, p) = last_at(series, t)?;
    if t - series[i].0 > MID_STALE_SECS { None } else { Some(p) }
}

/// Points of a `prices-history` response as `(t, p)`, sorted by time. Parsed the
/// same way as the training fetch, so both paths read identical series.
pub fn parse_price_history(v: &serde_json::Value) -> Vec<(i64, f64)> {
    let mut pts: Vec<(i64, f64)> = v["history"]
        .as_array()
        .map(|a| a.iter().filter_map(|p| Some((num(&p["t"])? as i64, num(&p["p"])?))).collect())
        .unwrap_or_default();
    pts.sort_by_key(|x| x.0);
    pts
}

/// Every row of one market, exactly as the reference builder produced them, with the
/// features from the viper's own `build_features`.
pub fn build_market_rows(w: i64, rec: &MarketRecord, bars: &[Bar], funding: &[(i64, f64)], plan: &Plan) -> Vec<TrainingRow> {
    let mut out = Vec::new();
    if rec.missing || rec.tape.is_empty() {
        return out;
    }
    let Some(up_won) = rec.up_won() else { return out };
    let w_end = w + 3600;
    let by_open: HashMap<i64, &Bar> = bars.iter().map(|b| (b.open_s, b)).collect();
    // The strike bar and the window's last closed bar must both exist.
    if !by_open.contains_key(&w) || !by_open.contains_key(&(w_end - 120)) {
        return out;
    }
    let mut tape: Vec<&Print> = rec.tape.iter().collect();
    tape.sort_by_key(|x| x.ts); // stable, as the harness's `sorted`
    let tape_ts: Vec<i64> = tape.iter().map(|x| x.ts).collect();
    let mut hist: [Vec<(i64, f64)>; 2] = [Vec::new(), Vec::new()];
    for side in 0..2 {
        let Some(h) = rec.hist.get(&side.to_string()) else { return out };
        let mut v: Vec<(i64, f64)> = h.iter().map(|p| (p.t, p.p)).collect();
        v.sort_by_key(|x| x.0);
        hist[side] = v;
    }
    if hist[0].len() < MIN_HIST_POINTS || hist[1].len() < MIN_HIST_POINTS {
        return out;
    }
    let mid_at = |side: usize, t: i64| history_mid_at(&hist[side], t);

    for k in plan.first_minute..=plan.last_minute {
        let t = w + 60 * k;
        let (Some(mid_up), Some(mid_dn)) = (mid_at(0, t), mid_at(1, t)) else { continue };
        let j_hi = tape_ts.partition_point(|ts| *ts <= t);
        // The entry each side is labeled on: the first price the market actually
        // showed at or after the decision minute, and when it showed it ([B47]).
        //
        // The old rule looked BACKWARD, and with no print in the 20 s before the
        // minute it bought at the last history sample plus half a spread:
        // a price a median 53 s old, from before the move the decision is reacting
        // to. That entry was the only profitable sub-population in the store
        // (58.1% win against 38.4% when repriced at the next real sample), so the
        // model's edge was measured at prices the book never offered.
        let mut entry: [Option<(i64, f64)>; 2] = [None, None];
        for side in 0..2 {
            let other = 1 - side;
            let mid_s = if side == 0 { mid_up } else { mid_dn };
            match plan.entry {
                EntryRule::LastBefore => {
                    let mut ask_ev: Option<f64> = None;
                    let mut jj = j_hi as i64 - 1;
                    while jj >= 0 && t - tape_ts[jj as usize] <= ENTRY_PRINT_SECS {
                        let x = tape[jj as usize];
                        if x.o as usize == side && x.side == "BUY" { ask_ev = Some(x.p); break; }
                        if x.o as usize == other && x.side == "SELL" { ask_ev = Some(1.0 - x.p); break; }
                        jj -= 1;
                    }
                    let a = match ask_ev {
                        Some(ev) => (mid_s + HALF_SPREAD).max(ev),
                        None => mid_s + HALF_SPREAD,
                    };
                    entry[side] = Some((t, a));
                }
                EntryRule::FirstAfter => {
                    // A taker print soon after the minute is an ask someone paid.
                    for x in &tape[j_hi..] {
                        if x.ts > t + ENTRY_PRINT_SECS || x.ts >= w_end { break; }
                        let px = if x.o as usize == side && x.side == "BUY" {
                            x.p
                        } else if x.o as usize == other && x.side == "SELL" {
                            1.0 - x.p
                        } else {
                            continue;
                        };
                        entry[side] = Some((x.ts, px));
                        break;
                    }
                    // Otherwise the first history sample after the minute, plus half a
                    // spread, but only while a live decision could still have acted:
                    // a feature vector at t paired with an entry minutes later (0.38%
                    // of side-minutes, history gaps) is not a trade anyone could take.
                    if let Some(&(ts, p)) = hist[side].iter().find(|(ts, _)| *ts > t) {
                        if ts < w_end && ts <= t + ENTRY_LATEST_SECS && entry[side].is_none_or(|(et, _)| ts < et) {
                            entry[side] = Some((ts, p + HALF_SPREAD));
                        }
                    }
                }
            }
        }
        // A side with no observable entry in time is not a trade anyone could have
        // taken; the minute then contributes no row for either side (2 minutes in
        // 123,451 in the production store).
        let (Some((entry_t_up, ask_up)), Some((entry_t_dn, ask_dn))) = (entry[0], entry[1]) else { continue };
        let entry_t = [entry_t_up, entry_t_dn];
        let ask = [ask_up.min(0.995), ask_dn.min(0.995)];
        let fr = last_at(funding, t).map(|(_, r)| r);
        let inputs = DecisionInputs {
            w,
            t,
            bars,
            mid_now: [Some(mid_up), Some(mid_dn)],
            mid_m1: [mid_at(0, t - 60), mid_at(1, t - 60)],
            mid_m5: [mid_at(0, t - 300), mid_at(1, t - 300)],
            ask,
            funding: fr,
        };
        let Ok(features) = build_features(&inputs) else { continue };

        for side in 0..2 {
            let other = 1 - side;
            let side_won = up_won == (side == 0);
            // Events after entry: prints and mids, chronological, bids and mids before asks
            // at the same second, as the harness ordered them.
            let mut ev: Vec<(i64, u8, f64, f64)> = Vec::new(); // (t, class: 0 bid/mid 1 askhit, px, size)
            let mut mids_tag: Vec<bool> = Vec::new();
            // The position exists from the moment it was bought, not from the
            // decision minute: an exit cannot fire on a price that came first.
            // Under `LastBefore` the two are the same instant, as the harness had it.
            let from = entry_t[side];
            for x in &tape[j_hi..] {
                if x.ts >= w_end { break; }
                if x.ts <= from { continue; }
                let (cls, px) = if x.o as usize == side && x.side == "BUY" {
                    (1u8, x.p)
                } else if x.o as usize == other && x.side == "SELL" {
                    (1u8, 1.0 - x.p)
                } else if x.o as usize == side && x.side == "SELL" {
                    (0u8, x.p)
                } else {
                    (0u8, 1.0 - x.p)
                };
                ev.push((x.ts, cls, px, x.s));
                mids_tag.push(false);
            }
            let hi_ = hist[side].partition_point(|(ts, _)| *ts <= from);
            for (tt, pp) in &hist[side][hi_..] {
                if *tt >= w_end { break; }
                ev.push((*tt, 0, *pp, 1e9));
                mids_tag.push(true);
            }
            let mut order: Vec<usize> = (0..ev.len()).collect();
            order.sort_by_key(|&i| (ev[i].0, ev[i].1));
            let a = ask[side];
            let fe = fee_per_share(plan.fee, a);
            let a_tp = ceil_tick(a * (1.0 + plan.tp)).min(plan.tp_ceiling);
            let tp_possible = a_tp < 1.0 && a_tp > a;
            let stop_px = a * (1.0 - plan.sl);
            let (mut exit_kind, mut exit_px, mut exit_t) = (ExitKind::Settle, if side_won { 1.0 } else { 0.0 }, w_end);
            for &i in &order {
                let (te, cls, px, sz) = ev[i];
                let is_mid = mids_tag[i];
                if !is_mid && cls == 0 && px <= stop_px {
                    exit_kind = ExitKind::Sl; exit_px = px; exit_t = te;
                    break;
                }
                if is_mid && px - HALF_SPREAD <= stop_px {
                    exit_kind = ExitKind::Sl; exit_px = px - HALF_SPREAD; exit_t = te;
                    break;
                }
                if tp_possible && !is_mid && cls == 1 && px >= a_tp && sz >= MIN_PRINT_SHARES {
                    exit_kind = ExitKind::Tp; exit_px = a_tp; exit_t = te;
                    break;
                }
                if tp_possible && is_mid && px - HALF_SPREAD >= a_tp {
                    exit_kind = ExitKind::Tp; exit_px = a_tp; exit_t = te;
                    break;
                }
            }
            let fx = if exit_kind == ExitKind::Sl { fee_per_share(plan.fee, exit_px) } else { 0.0 };
            let pnl = exit_px - a - fe - fx;
            out.push(TrainingRow {
                w,
                k,
                side,
                ask: a,
                features: features[side],
                y: pnl > 0.0,
                ret: pnl / a,
                exit: exit_kind,
                exit_t,
                elig: plan.lo <= a && a <= plan.hi,
            });
        }
    }
    out
}

// ── Statistics ───────────────────────────────────────────────────────────────

/// splitmix64: a small deterministic generator for the bootstrap, so a report is
/// reproducible and no dependency is added.
struct SplitMix(u64);
impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize { (self.next() % n as u64) as usize }
}

/// numpy's default (linear) percentile of a sorted sample.
fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() { return f64::NAN; }
    let pos = q / 100.0 * (sorted.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo as f64)
}

fn log_loss(y: &[bool], p: &[f64]) -> f64 {
    let n = y.len() as f64;
    y.iter().zip(p).map(|(&yy, &pp)| {
        let pp = pp.clamp(1e-15, 1.0 - 1e-15);
        if yy { -pp.ln() } else { -(1.0 - pp).ln() }
    }).sum::<f64>() / n
}

/// Rank AUC with tied scores averaged.
pub fn auc(y: &[bool], p: &[f64]) -> f64 {
    let n_pos = y.iter().filter(|&&v| v).count();
    let n_neg = y.len() - n_pos;
    if n_pos == 0 || n_neg == 0 { return f64::NAN; }
    let mut idx: Vec<usize> = (0..y.len()).collect();
    idx.sort_by(|&a, &b| p[a].total_cmp(&p[b]));
    let mut rank_sum_pos = 0.0;
    let mut i = 0;
    while i < idx.len() {
        let mut j = i;
        while j + 1 < idx.len() && p[idx[j + 1]] == p[idx[i]] { j += 1; }
        let avg_rank = (i + j + 2) as f64 / 2.0;
        for &r in &idx[i..=j] {
            if y[r] { rank_sum_pos += avg_rank; }
        }
        i = j + 1;
    }
    (rank_sum_pos - n_pos as f64 * (n_pos as f64 + 1.0) / 2.0) / (n_pos as f64 * n_neg as f64)
}

/// Platt scaling: a one-feature logistic regression of the label on logit(raw), fit
/// by Newton's method with the same negligible ridge sklearn's `C=1e6` applies to the
/// slope. Returns `(a, b)`.
pub fn fit_platt(raw: &[f64], y: &[bool]) -> Option<(f64, f64)> {
    if raw.len() != y.len() || raw.len() < 2 { return None; }
    if y.iter().all(|&v| v) || y.iter().all(|&v| !v) { return None; }
    let x: Vec<f64> = raw.iter().map(|r| { let r = r.clamp(1e-4, 1.0 - 1e-4); (r / (1.0 - r)).ln() }).collect();
    let ridge = 1e-6;
    let nll = |a: f64, b: f64| -> f64 {
        x.iter().zip(y).map(|(&xi, &yi)| {
            let z = a * xi + b;
            // log(1 + e^z) - y z, computed stably
            let lse = if z > 0.0 { z + (-z).exp().ln_1p() } else { z.exp().ln_1p() };
            lse - if yi { z } else { 0.0 }
        }).sum::<f64>() + 0.5 * ridge * a * a
    };
    let (mut a, mut b) = (1.0f64, 0.0f64);
    let mut f = nll(a, b);
    for _ in 0..200 {
        let (mut ga, mut gb, mut haa, mut hab, mut hbb) = (ridge * a, 0.0, ridge, 0.0, 0.0);
        for (&xi, &yi) in x.iter().zip(y) {
            let s = 1.0 / (1.0 + (-(a * xi + b)).exp());
            let r = s - if yi { 1.0 } else { 0.0 };
            ga += r * xi;
            gb += r;
            let wgt = s * (1.0 - s);
            haa += wgt * xi * xi;
            hab += wgt * xi;
            hbb += wgt;
        }
        let det = haa * hbb - hab * hab;
        if !(det.abs() > 1e-300) { break; }
        let da = (hbb * ga - hab * gb) / det;
        let db = (haa * gb - hab * ga) / det;
        // Damped step: halve until the objective does not rise.
        let mut step = 1.0;
        let mut accepted = false;
        for _ in 0..40 {
            let (na, nb) = (a - step * da, b - step * db);
            let nf = nll(na, nb);
            if nf <= f + 1e-12 {
                let moved = ((na - a).abs()).max((nb - b).abs());
                a = na; b = nb; f = nf; accepted = true;
                if moved < 1e-12 { return Some((a, b)); }
                break;
            }
            step *= 0.5;
        }
        if !accepted { break; }
    }
    if a.is_finite() && b.is_finite() { Some((a, b)) } else { None }
}

/// The trade rule's outcome on one scored fold.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct RuleStats {
    pub trades: usize,
    pub markets: usize,
    pub mean_ret: f64,
    pub median_ret: f64,
    pub win: f64,
    pub ci_lo: f64,
    pub ci_hi: f64,
    pub tp: usize,
    pub sl: usize,
    pub settle: usize,
}

/// Everything measured about a model on a fold.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct FoldStats {
    pub n_rows: usize,
    pub n_markets: usize,
    pub base_rate: f64,
    pub fold_rate: f64,
    pub logloss: f64,
    pub skill: f64,
    pub auc: f64,
    pub rule: RuleStats,
}

/// First qualifying minute per (market, side): calibrated `p` at or above break-even
/// plus the margin, as the harness's `trade_stats` on its rule mask.
pub fn rule_stats(rows: &[&TrainingRow], p: &[f64], plan: &Plan) -> RuleStats {
    let mut order: Vec<usize> = (0..rows.len()).filter(|&i| {
        let r = rows[i];
        p[i] >= break_even(r.ask, plan.tp, plan.sl, plan.fee) + plan.margin
    }).collect();
    order.sort_by_key(|&i| (rows[i].w + 60 * rows[i].k, rows[i].w, rows[i].side));
    let mut seen: HashSet<(i64, usize)> = HashSet::new();
    let mut trades: Vec<(i64, f64, ExitKind)> = Vec::new();
    for i in order {
        let r = rows[i];
        if seen.insert((r.w, r.side)) {
            trades.push((r.w, r.ret, r.exit));
        }
    }
    if trades.is_empty() {
        return RuleStats { mean_ret: f64::NAN, median_ret: f64::NAN, win: f64::NAN, ci_lo: f64::NAN, ci_hi: f64::NAN, ..Default::default() };
    }
    let rets: Vec<f64> = trades.iter().map(|t| t.1).collect();
    let mut sorted = rets.clone();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let mut by_market: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
    for (w, r, _) in &trades { by_market.entry(*w).or_default().push(*r); }
    let markets: Vec<&Vec<f64>> = by_market.values().collect();
    let mut rng = SplitMix(0);
    let mut boots: Vec<f64> = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
    for _ in 0..BOOTSTRAP_RESAMPLES {
        let (mut sum, mut n) = (0.0, 0usize);
        for _ in 0..markets.len() {
            let m = markets[rng.below(markets.len())];
            sum += m.iter().sum::<f64>();
            n += m.len();
        }
        boots.push(sum / n as f64);
    }
    boots.sort_by(|a, b| a.total_cmp(b));
    RuleStats {
        trades: trades.len(),
        markets: by_market.len(),
        mean_ret: rets.iter().sum::<f64>() / rets.len() as f64,
        median_ret: percentile(&sorted, 50.0),
        win: rets.iter().filter(|r| **r > 0.0).count() as f64 / rets.len() as f64,
        ci_lo: percentile(&boots, 5.0),
        ci_hi: percentile(&boots, 95.0),
        tp: trades.iter().filter(|t| t.2 == ExitKind::Tp).count(),
        sl: trades.iter().filter(|t| t.2 == ExitKind::Sl).count(),
        settle: trades.iter().filter(|t| t.2 == ExitKind::Settle).count(),
    }
}

pub fn fold_stats(rows: &[&TrainingRow], p: &[f64], base_rate: f64, plan: &Plan) -> FoldStats {
    let y: Vec<bool> = rows.iter().map(|r| r.y).collect();
    let ll = log_loss(&y, p);
    let ll0 = log_loss(&y, &vec![base_rate; y.len()]);
    let markets: HashSet<i64> = rows.iter().map(|r| r.w).collect();
    FoldStats {
        n_rows: rows.len(),
        n_markets: markets.len(),
        base_rate,
        fold_rate: y.iter().filter(|&&v| v).count() as f64 / y.len().max(1) as f64,
        logloss: ll,
        skill: 1.0 - ll / ll0,
        auc: auc(&y, p),
        rule: rule_stats(rows, p, plan),
    }
}

// ── Training ─────────────────────────────────────────────────────────────────

fn column_major(rows: &[[f64; N_FEATURES]], zeroed: &[usize]) -> Vec<f64> {
    let n = rows.len();
    let mut out = vec![0.0f64; n * N_FEATURES];
    for (i, r) in rows.iter().enumerate() {
        for j in 0..N_FEATURES {
            out[j * n + i] = if zeroed.contains(&j) { 0.0 } else { r[j] };
        }
    }
    out
}

/// Rayon threads for a fit: two as the harness used, one on a two-core box so the
/// trading loop keeps a core.
pub fn fit_threads() -> usize {
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2);
    if cores <= 2 { 1 } else { 2 }
}

/// Fit the harness's booster configuration.
pub fn fit_booster(x: &[[f64; N_FEATURES]], y: &[bool], zeroed: &[usize], budget: f32, threads: usize) -> Result<PerpetualBooster, String> {
    let data = column_major(x, zeroed);
    let m = Matrix::new(&data, x.len(), N_FEATURES);
    let yy: Vec<f64> = y.iter().map(|&v| if v { 1.0 } else { 0.0 }).collect();
    let mut b = PerpetualBooster::default()
        .set_objective(Objective::LogLoss)
        .set_budget(budget)
        .set_num_threads(Some(threads))
        .set_log_iterations(0)
        .set_max_bin(MAX_BIN)
        .set_iteration_limit(Some(ITERATION_LIMIT))
        .set_stopping_rounds(None)
        .set_save_node_stats(false)
        .set_timeout(Some(TRAIN_TIMEOUT_SECS));
    b.fit(&m, &yy, None, None).map_err(|e| format!("fit failed: {e:?}"))?;
    Ok(b)
}

fn predict_raw(b: &PerpetualBooster, x: &[[f64; N_FEATURES]], zeroed: &[usize]) -> Vec<f64> {
    if x.is_empty() { return Vec::new(); }
    let data = column_major(x, zeroed);
    let m = Matrix::new(&data, x.len(), N_FEATURES);
    b.predict_proba(&m, false, false)
}

/// Columns with no value anywhere in the fit rows carry nothing and are zeroed in
/// training and, through the stamp, at serve time.
pub fn dead_columns(x: &[[f64; N_FEATURES]]) -> Vec<usize> {
    (0..N_FEATURES).filter(|&j| x.iter().all(|r| r[j].is_nan())).collect()
}

/// What the gate decided and why.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GateResult {
    pub passed: bool,
    pub reasons: Vec<String>,
}

/// The absolute bar a candidate must clear on its holdout fold.
pub fn gate(trees: usize, stats: &FoldStats, min_trades: usize, min_win: f64) -> GateResult {
    let mut reasons = Vec::new();
    if trees < STRUCTURAL_MIN_TREES {
        reasons.push(format!("{trees} trees, below the structural minimum {STRUCTURAL_MIN_TREES}"));
    }
    if !(stats.skill > 0.0) {
        reasons.push(format!("log-loss skill {:+.4} is not above the base rate", stats.skill));
    }
    if stats.rule.trades < min_trades {
        reasons.push(format!("{} holdout trades, fewer than the {min_trades} required", stats.rule.trades));
    }
    if !(stats.rule.mean_ret > 0.0) {
        reasons.push(format!("holdout mean return {:+.2}% per trade is not positive", stats.rule.mean_ret * 100.0));
    }
    if !(stats.rule.win >= min_win) {
        reasons.push(format!("holdout win rate {:.3} is below the {min_win:.2} bar", stats.rule.win));
    }
    GateResult { passed: reasons.is_empty(), reasons }
}

/// Does the candidate do at least as well as the incumbent on the same fold? Compared
/// on return per trade when the incumbent trades enough there to be measured, on
/// log-loss skill otherwise.
pub fn beats_incumbent(candidate: &FoldStats, incumbent: &FoldStats, min_trades: usize) -> (bool, String) {
    if incumbent.rule.trades >= min_trades {
        let ok = candidate.rule.mean_ret >= incumbent.rule.mean_ret;
        (ok, format!(
            "return per trade {:+.2}% against the incumbent's {:+.2}% on the same fold",
            candidate.rule.mean_ret * 100.0, incumbent.rule.mean_ret * 100.0,
        ))
    } else {
        let ok = candidate.skill >= incumbent.skill;
        (ok, format!(
            "the incumbent took only {} holdout trades, so log-loss skill decides: {:+.4} against {:+.4}",
            incumbent.rule.trades, candidate.skill, incumbent.skill,
        ))
    }
}

/// The outcome of one training cycle, persisted as a report.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct CycleReport {
    pub started_at: String,
    pub finished_at: String,
    pub duration_secs: f64,
    pub plan: Option<Plan>,
    pub budget: f64,
    pub window_days: i64,
    pub holdout_days: i64,
    pub rows_total: usize,
    pub rows_eligible: usize,
    pub markets: usize,
    pub data_from: String,
    pub data_through: String,
    pub fold_from: String,
    pub fold_through: String,
    pub fit_rows: usize,
    pub fit_markets: usize,
    pub cal_rows: usize,
    pub cal_markets: usize,
    pub test_rows: usize,
    pub trees: usize,
    pub platt_a: f64,
    pub platt_b: f64,
    pub zeroed_features: Vec<String>,
    pub candidate: Option<FoldStats>,
    pub incumbent: Option<FoldStats>,
    pub incumbent_version: Option<String>,
    pub incumbent_overlaps_fold: bool,
    pub gate: GateResult,
    pub comparison: Option<String>,
    /// `adopted`, `rejected`, `incumbent_kept`, `auto_adopt_off`, `insufficient_data`,
    /// `refused` (too little memory; no fit was attempted), `error`
    pub decision: String,
    pub detail: String,
    pub model_version: Option<String>,
}

/// Everything a cycle needs, gathered on the async side so the blocking thread touches
/// only memory and the filesystem.
pub struct CycleInputs {
    pub asset: String,
    pub dir: DataDir,
    pub serving_path: PathBuf,
    pub plan: Plan,
    pub now: i64,
    pub window_days: i64,
    pub holdout_days: i64,
    pub min_trades: usize,
    pub min_win: f64,
    pub budget: f64,
    pub auto_adopt: bool,
    pub threads: usize,
}

/// Load every stored market in the window and its bars, and build the rows.
pub fn load_rows(inputs: &CycleInputs) -> Result<(Vec<TrainingRow>, usize, Option<(i64, i64)>), String> {
    let from = floor_hour(inputs.now) - inputs.window_days * 86400;
    let funding: Vec<(i64, f64)> = read_json::<Vec<(i64, f64)>>(&inputs.dir.funding()).unwrap_or_default();
    let mut bars: HashMap<i64, Bar> = HashMap::new();
    if let Ok(entries) = std::fs::read_dir(inputs.dir.klines()) {
        for e in entries.flatten() {
            if let Ok(day) = read_json::<KlineDay>(&e.path()) {
                for b in bars_from_day(&day) { bars.insert(b.open_s, b); }
            }
        }
    }
    let mut rows = Vec::new();
    let mut markets = 0usize;
    let (mut first_w, mut last_w) = (i64::MAX, i64::MIN);
    let mut ws: Vec<i64> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(inputs.dir.markets()) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(w) = name.strip_suffix(".json").and_then(|s| s.parse::<i64>().ok()) {
                if w >= from { ws.push(w); }
            }
        }
    }
    ws.sort_unstable();
    for w in ws {
        let Ok(rec) = read_json::<MarketRecord>(&inputs.dir.market(w)) else { continue };
        let slice: Vec<Bar> = ((w - 62 * 60)..(w + 3600)).step_by(60).filter_map(|o| bars.get(&o).copied()).collect();
        let mut r = build_market_rows(w, &rec, &slice, &funding, &inputs.plan);
        if !r.is_empty() {
            markets += 1;
            first_w = first_w.min(w);
            last_w = last_w.max(w);
        }
        rows.append(&mut r);
    }
    let span = (markets > 0).then_some((first_w, last_w));
    Ok((rows, markets, span))
}

/// One full cycle on the calling (blocking) thread: rows, split, fit, calibrate, score,
/// gate, compare, adopt. Never panics on bad data; every failure is a report.
pub fn run_cycle_blocking(inputs: &CycleInputs) -> CycleReport {
    let started = Instant::now();
    let mut rep = CycleReport {
        started_at: rfc3339(inputs.now),
        plan: Some(inputs.plan),
        budget: inputs.budget,
        window_days: inputs.window_days,
        holdout_days: inputs.holdout_days,
        ..Default::default()
    };
    let finish = |mut rep: CycleReport, decision: &str, detail: String| {
        rep.decision = decision.to_string();
        rep.detail = detail;
        rep.finished_at = rfc3339(Utc::now().timestamp());
        rep.duration_secs = started.elapsed().as_secs_f64();
        rep
    };

    let (rows, markets, span) = match load_rows(inputs) {
        Ok(v) => v,
        Err(e) => return finish(rep, "error", e),
    };
    rep.rows_total = rows.len();
    rep.markets = markets;
    if let Some((a, b)) = span {
        rep.data_from = rfc3339(a);
        rep.data_through = rfc3339(b + 3600);
    }
    let elig: Vec<&TrainingRow> = rows.iter().filter(|r| r.elig).collect();
    rep.rows_eligible = elig.len();

    let fold_start = floor_hour(inputs.now) - inputs.holdout_days * 86400;
    rep.fold_from = rfc3339(fold_start);
    rep.fold_through = rfc3339(floor_hour(inputs.now));
    let test: Vec<&TrainingRow> = elig.iter().copied().filter(|r| r.w >= fold_start).collect();
    let train: Vec<&TrainingRow> = elig.iter().copied().filter(|r| r.w + 3600 <= fold_start - PURGE_SECS).collect();
    let mut ws: Vec<i64> = train.iter().map(|r| r.w).collect::<HashSet<_>>().into_iter().collect();
    ws.sort_unstable();
    if ws.len() < 10 {
        return finish(rep, "insufficient_data", format!("{} training markets before the holdout fold; need more history", ws.len()));
    }
    let cut = ws[(ws.len() as f64 * (1.0 - CAL_FRACTION)) as usize];
    let fit: Vec<&TrainingRow> = train.iter().copied().filter(|r| r.w + 3600 <= cut - PURGE_SECS).collect();
    let cal: Vec<&TrainingRow> = train.iter().copied().filter(|r| r.w >= cut).collect();
    rep.fit_rows = fit.len();
    rep.fit_markets = fit.iter().map(|r| r.w).collect::<HashSet<_>>().len();
    rep.cal_rows = cal.len();
    rep.cal_markets = cal.iter().map(|r| r.w).collect::<HashSet<_>>().len();
    rep.test_rows = test.len();
    let both = |v: &[&TrainingRow]| v.iter().any(|r| r.y) && v.iter().any(|r| !r.y);
    if fit.len() < MIN_FIT_ROWS || cal.len() < MIN_CAL_ROWS || test.len() < MIN_TEST_ROWS || !both(&fit) || !both(&cal) {
        return finish(rep, "insufficient_data", format!(
            "fit {} rows, calibration {} rows, holdout {} rows (need {MIN_FIT_ROWS}/{MIN_CAL_ROWS}/{MIN_TEST_ROWS} with both labels)",
            fit.len(), cal.len(), test.len(),
        ));
    }

    let xf: Vec<[f64; N_FEATURES]> = fit.iter().map(|r| r.features).collect();
    let yf: Vec<bool> = fit.iter().map(|r| r.y).collect();
    let zeroed = dead_columns(&xf);
    rep.zeroed_features = zeroed.iter().map(|&j| FEATURE_NAMES[j].to_string()).collect();
    let booster = match fit_booster(&xf, &yf, &zeroed, inputs.budget as f32, inputs.threads) {
        Ok(b) => b,
        Err(e) => return finish(rep, "error", e),
    };
    rep.trees = booster.get_prediction_trees().len();
    let xc: Vec<[f64; N_FEATURES]> = cal.iter().map(|r| r.features).collect();
    let yc: Vec<bool> = cal.iter().map(|r| r.y).collect();
    let pc = predict_raw(&booster, &xc, &zeroed);
    let Some((a, b)) = fit_platt(&pc, &yc) else {
        return finish(rep, "error", "Platt calibration did not converge".to_string());
    };
    rep.platt_a = a;
    rep.platt_b = b;
    let xt: Vec<[f64; N_FEATURES]> = test.iter().map(|r| r.features).collect();
    let pt: Vec<f64> = predict_raw(&booster, &xt, &zeroed).into_iter().map(|raw| calibrate(raw, a, b)).collect();
    let base_rate = train.iter().filter(|r| r.y).count() as f64 / train.len() as f64;
    let cand = fold_stats(&test, &pt, base_rate, &inputs.plan);
    rep.gate = gate(rep.trees, &cand, inputs.min_trades, inputs.min_win);
    rep.candidate = Some(cand.clone());

    // The incumbent on the same fold.
    let incumbent: Option<PlanBModel> = match load_model(&inputs.serving_path) {
        Ok(m) => Some(m),
        Err(e) => {
            if inputs.serving_path.exists() {
                warn!("GBoost plan-B pipeline [{}]: serving model unreadable, treated as absent: {e}", inputs.asset);
            }
            None
        }
    };
    if let Some(inc) = &incumbent {
        let preds = inc.predict(&xt);
        let pi: Vec<f64> = preds.iter().map(|x| x.1).collect();
        rep.incumbent = Some(fold_stats(&test, &pi, base_rate, &inputs.plan));
        rep.incumbent_version = Some(inc.version.clone());
        rep.incumbent_overlaps_fold = inc.calibrated_through.as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.timestamp() > fold_start)
            .unwrap_or(true);
    }

    let version = format!("engine-{}-{}", inputs.asset.to_ascii_lowercase(), Utc.timestamp_opt(inputs.now, 0).single().map(|d| d.format("%Y%m%dT%H%MZ").to_string()).unwrap_or_default());
    rep.model_version = Some(version.clone());

    // Stamp and write the candidate whatever the decision, so it can be inspected.
    let mut booster = booster;
    let fit_from = fit.iter().map(|r| r.w).min().unwrap_or(0);
    let fit_through = fit.iter().map(|r| r.w).max().unwrap_or(0) + 3600;
    let cal_through = cal.iter().map(|r| r.w).max().unwrap_or(0) + 3600;
    let meta: Vec<(&str, String)> = vec![
        ("feature_layout", "column_major".into()),
        ("feature_names", FEATURE_NAMES.join(",")),
        ("model_version", version.clone()),
        ("trained_by", "dradis-engine".into()),
        ("engine_version", env!("CARGO_PKG_VERSION").into()),
        ("venue", TRAINED_VENUE.into()),
        ("asset", inputs.asset.to_ascii_lowercase()),
        ("label", inputs.plan.label()),
        ("entry_rule", inputs.plan.entry.stamp().to_string()),
        ("plan_tp", format!("{:?}", inputs.plan.tp)),
        ("plan_sl", format!("{:?}", inputs.plan.sl)),
        ("plan_min_ask", format!("{:?}", inputs.plan.lo)),
        ("plan_max_ask", format!("{:?}", inputs.plan.hi)),
        ("plan_tp_ceiling", format!("{:?}", inputs.plan.tp_ceiling)),
        ("plan_margin", format!("{:?}", inputs.plan.margin)),
        ("fee_rate", format!("{:?}", inputs.plan.fee)),
        ("budget", format!("{:?}", inputs.budget)),
        ("iteration_limit", ITERATION_LIMIT.to_string()),
        ("platt_a", format!("{a:?}")),
        ("platt_b", format!("{b:?}")),
        ("zeroed_features", rep.zeroed_features.join(",")),
        ("sigma_floor_per_sqrt_sec", "4.2e-5".into()),
        ("trained_from", rfc3339(fit_from)),
        ("fit_through", rfc3339(fit_through)),
        ("calibrated_through", rfc3339(cal_through)),
        ("holdout_from", rep.fold_from.clone()),
        ("holdout_through", rep.fold_through.clone()),
        ("fit_rows", fit.len().to_string()),
        ("cal_rows", cal.len().to_string()),
        ("holdout_rows", test.len().to_string()),
        ("holdout_trades", cand.rule.trades.to_string()),
        ("holdout_mean_ret", format!("{:?}", cand.rule.mean_ret)),
        ("holdout_win", format!("{:?}", cand.rule.win)),
        ("holdout_skill", format!("{:?}", cand.skill)),
        ("created_at", rfc3339(Utc::now().timestamp())),
    ];
    for (k, v) in &meta {
        booster.insert_metadata(k.to_string(), v.clone());
    }
    let bytes = match booster.json_dump() {
        Ok(s) => s.into_bytes(),
        Err(e) => return finish(rep, "error", format!("model serialization failed: {e:?}")),
    };
    drop(booster);
    if let Err(e) = write_atomically(&inputs.dir.candidate(), &bytes) {
        return finish(rep, "error", format!("cannot write the candidate file: {e}"));
    }

    if !rep.gate.passed {
        let why = rep.gate.reasons.join("; ");
        return finish(rep, "rejected", format!("candidate {version} failed the holdout gate: {why}"));
    }
    if let Some(inc_stats) = &rep.incumbent {
        let (ok, why) = beats_incumbent(&cand, inc_stats, inputs.min_trades);
        let overlap = if rep.incumbent_overlaps_fold { " (the incumbent's training range overlaps the fold, which favors it)" } else { "" };
        rep.comparison = Some(format!("{why}{overlap}"));
        if !ok {
            let detail = format!(
                "candidate {version} passed the gate but did not beat the serving model {}: {why}{overlap}",
                rep.incumbent_version.clone().unwrap_or_default(),
            );
            return finish(rep, "incumbent_kept", detail);
        }
    }
    if !inputs.auto_adopt {
        let detail = format!(
            "candidate {version} passed the gate{}; Auto Adopt is off, so it was written to {} and not put into service",
            rep.comparison.as_deref().map(|c| format!(" and beat the serving model ({c})")).unwrap_or_default(),
            inputs.dir.candidate().display(),
        );
        return finish(rep, "auto_adopt_off", detail);
    }
    // Adopt: archive the incumbent, then rename the stamped file into the serving path.
    if inputs.serving_path.exists() {
        if let Some(dir) = inputs.dir.archive().parent() { let _ = std::fs::create_dir_all(dir); }
        if let Err(e) = std::fs::rename(&inputs.serving_path, inputs.dir.archive()) {
            return finish(rep, "error", format!("cannot archive the serving model: {e}"));
        }
    }
    if let Err(e) = write_atomically(&inputs.serving_path, &bytes) {
        return finish(rep, "error", format!("cannot write the serving model: {e}"));
    }
    let summary = format!(
        "adopted {version}: {} trees, holdout {} trades at {:+.2}% per trade, win {:.3}, skill {:+.4}{}",
        rep.trees, cand.rule.trades, cand.rule.mean_ret * 100.0, cand.rule.win, cand.skill,
        rep.comparison.as_deref().map(|c| format!("; {c}")).unwrap_or_default(),
    );
    finish(rep, "adopted", summary)
}

// ── Fetching ─────────────────────────────────────────────────────────────────

fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .user_agent(USER_AGENT)
            .build()
            .unwrap_or_default()
    })
}

/// GET with pacing and exponential backoff on 429 and 5xx.
async fn get_json(url: &str) -> Result<serde_json::Value, String> {
    let mut last = String::new();
    for attempt in 0..HTTP_ATTEMPTS {
        match client().get(url).header("Accept", "application/json").send().await {
            Ok(r) if r.status().is_success() => {
                tokio::time::sleep(Duration::from_millis(REQUEST_PAUSE_MS)).await;
                return r.json::<serde_json::Value>().await.map_err(|e| e.to_string());
            }
            Ok(r) if r.status().as_u16() == 429 || r.status().is_server_error() => last = format!("HTTP {}", r.status()),
            Ok(r) => return Err(format!("HTTP {}", r.status())),
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(Duration::from_secs(BACKOFF_BASE_SECS << attempt)).await;
    }
    Err(format!("{url}: {last}"))
}

fn num(v: &serde_json::Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// What a market fetch found.
pub enum Fetched {
    Record(MarketRecord),
    /// Closed but Gamma has not published a 0/1 resolution yet; try again later.
    NotResolved,
}

async fn fetch_market(w: i64, now: i64) -> Result<Fetched, String> {
    let slug = slug_for(w);
    let ev = get_json(&format!("{GAMMA}/events?slug={slug}")).await?;
    let Some(m) = ev.as_array().and_then(|a| a.first()).and_then(|e| e["markets"].as_array()).and_then(|ms| ms.first()) else {
        return Ok(Fetched::Record(MarketRecord { slug, missing: true, window_start: rfc3339(w), ..Default::default() }));
    };
    let tokens: Vec<String> = m["clobTokenIds"].as_str().and_then(|s| serde_json::from_str(s).ok()).unwrap_or_default();
    let mut rec = MarketRecord {
        slug,
        missing: false,
        window_start: rfc3339(w),
        condition_id: m["conditionId"].as_str().map(str::to_string),
        tokens,
        outcome_prices: m["outcomePrices"].as_str().map(str::to_string),
        closed: m["closed"].as_bool(),
        end_date: m["endDate"].as_str().map(str::to_string),
        tape: Vec::new(),
        hist: HashMap::new(),
    };
    if rec.up_won().is_none() && now < w + 3600 + UNRESOLVED_GIVE_UP_SECS {
        return Ok(Fetched::NotResolved);
    }
    if rec.tokens.len() != 2 {
        return Err(format!("{}: market has {} tokens", rec.slug, rec.tokens.len()));
    }
    let cid = rec.condition_id.clone().ok_or("market has no conditionId")?;
    let mut off = 0usize;
    loop {
        let page = get_json(&format!("{DATA_API}/trades?market={cid}&limit={TAPE_PAGE}&offset={off}")).await?;
        let rows = page.as_array().cloned().unwrap_or_default();
        for x in &rows {
            let (Some(ts), Some(side), Some(o), Some(p), Some(s)) = (
                num(&x["timestamp"]).map(|v| v as i64), x["side"].as_str(), num(&x["outcomeIndex"]), num(&x["price"]), num(&x["size"]),
            ) else { continue };
            rec.tape.push(Print { ts, side: side.to_string(), o: o as u8, p, s });
        }
        if rows.len() < TAPE_PAGE || off >= TAPE_MAX_OFFSET { break; }
        off += TAPE_PAGE;
    }
    for (i, tok) in rec.tokens.iter().enumerate() {
        let h = get_json(&format!("{CLOB}/prices-history?market={tok}&startTs={}&endTs={}&fidelity=1", w - 3600, w + 3900)).await?;
        let pts: Vec<HistPoint> = h["history"].as_array().map(|a| a.iter().filter_map(|p| Some(HistPoint { t: num(&p["t"])? as i64, p: num(&p["p"])? })).collect()).unwrap_or_default();
        rec.hist.insert(i.to_string(), pts);
    }
    Ok(Fetched::Record(rec))
}

async fn fetch_klines_day(day: chrono::NaiveDate, now: i64) -> Result<KlineDay, String> {
    let start = day.and_hms_opt(0, 0, 0).unwrap_or_default().and_utc().timestamp();
    let end = start + 86400;
    let mut bars: Vec<[f64; 5]> = Vec::with_capacity(1440);
    let mut cursor = start;
    let mut last_err = String::new();
    'outer: while cursor < end && cursor < now {
        let mut got = None;
        for host in KLINE_HOSTS {
            let url = format!("{host}/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime={}&endTime={}&limit=1000", cursor * 1000, end * 1000 - 1);
            match get_json(&url).await.and_then(|v| crate::vipers::gboost_planb::parse_klines(&v)) {
                Ok(b) => { got = Some(b); break; }
                Err(e) => last_err = format!("{host}: {e}"),
            }
        }
        let Some(b) = got else { return Err(last_err) };
        if b.is_empty() { break 'outer; }
        let last_open = b.last().map(|x| x.open_s).unwrap_or(cursor);
        for x in &b {
            // Only closed bars: the bar opening in the current minute is still forming.
            if x.open_s + 60 <= now { bars.push([x.open_s as f64, x.open, x.high, x.low, x.close]); }
        }
        if last_open + 60 <= cursor { break; }
        cursor = last_open + 60;
    }
    bars.sort_by(|a, b| a[0].total_cmp(&b[0]));
    bars.dedup_by(|a, b| a[0] == b[0]);
    // A day is complete with all 1,440 bars, or once it is a full day old: a bar Binance
    // never published (an exchange pause) will not appear later, and refetching forever
    // would only spend requests. Rows across the gap are skipped by the builder.
    let settled = end <= now - 86400;
    Ok(KlineDay { day: day.to_string(), complete: bars.len() >= 1440 || settled, bars })
}

/// Settled funding history from `since` (exclusive), through whichever host answers.
async fn fetch_funding_since(since_s: i64) -> Result<(Vec<(i64, f64)>, &'static str), String> {
    let mut out: Vec<(i64, f64)> = Vec::new();
    let mut last_err = String::new();
    'hosts: for host in FUNDING_HOSTS {
        out.clear();
        let mut cursor_ms = since_s * 1000 + 1;
        loop {
            let url = format!("{host}/fapi/v1/fundingRate?symbol=BTCUSDT&startTime={cursor_ms}&limit=1000");
            let v = match get_json(&url).await {
                Ok(v) => v,
                Err(e) => { last_err = format!("{host}: {e}"); continue 'hosts; }
            };
            let rows = v.as_array().cloned().unwrap_or_default();
            for r in &rows {
                if let (Some(t), Some(rate)) = (r["fundingTime"].as_i64(), num(&r["fundingRate"])) {
                    out.push((t / 1000, rate));
                    cursor_ms = t + 1;
                }
            }
            if rows.len() < 1000 { return Ok((out, host)); }
        }
    }
    Err(last_err)
}

// ── Memory ───────────────────────────────────────────────────────────────────

/// What the machine and the container report about memory, each `None` where it
/// cannot be read. The gate wants headroom: what a fit can allocate right now beside
/// everything already running, which is neither the machine's size (a nominal 4 GB
/// instance reports 3.7 GB and was refused for it on 2026-09-13) nor the container's
/// limit alone (a limit is only room to the extent it is unused).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemoryReading {
    /// The machine: `MemTotal` and `MemAvailable` from `/proc/meminfo`.
    pub total: Option<u64>,
    pub available: Option<u64>,
    /// The container's cgroup limit and, when one applies, its usage net of page
    /// cache: the kernel reclaims cache before it kills, so, like `MemAvailable`,
    /// the room under a limit counts cache as free.
    pub limit: Option<u64>,
    pub used: Option<u64>,
}

impl MemoryReading {
    /// Memory a fit can take now: the machine's available memory, capped by what is
    /// left under the container's limit. Falls back to the machine's total where the
    /// kernel does not report `MemAvailable` (or on a developer's Mac).
    pub fn headroom(&self) -> Option<u64> {
        let machine = self.available.or(self.total);
        let container = self.limit.map(|l| l.saturating_sub(self.used.unwrap_or(0)));
        match (machine, container) {
            (Some(m), Some(c)) => Some(m.min(c)),
            (m, c) => m.or(c),
        }
    }

    /// The machine's total, or the container's limit when that is smaller: the number
    /// the status endpoint reports as `memory_bytes`.
    pub fn capacity(&self) -> Option<u64> {
        match (self.total, self.limit) {
            (Some(t), Some(l)) => Some(t.min(l)),
            (t, l) => t.or(l),
        }
    }

    /// True when the container's limit, not the machine, is what caps the headroom.
    fn container_bound(&self) -> bool {
        match (self.limit.map(|l| l.saturating_sub(self.used.unwrap_or(0))), self.available.or(self.total)) {
            (Some(c), Some(m)) => c < m,
            (Some(_), None) => true,
            _ => false,
        }
    }
}

/// `MemTotal` and `MemAvailable` from the text of `/proc/meminfo`, in bytes.
pub fn parse_meminfo(text: &str) -> (Option<u64>, Option<u64>) {
    let field = |name: &str| text.lines().find_map(|l| {
        let rest = l.strip_prefix(name)?.strip_prefix(':')?;
        rest.split_whitespace().next()?.parse::<u64>().ok().map(|kb| kb * 1024)
    });
    (field("MemTotal"), field("MemAvailable"))
}

/// One `key value` line of a cgroup `memory.stat`, in bytes.
pub fn parse_cgroup_stat(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|l| {
        let mut it = l.split_whitespace();
        if it.next()? != key { return None; }
        it.next()?.parse::<u64>().ok()
    })
}

/// Read the machine and the container. Any part that cannot be read is `None`.
pub fn read_memory() -> MemoryReading {
    let mut r = MemoryReading::default();
    // Linux: the machine.
    if let Ok(s) = std::fs::read_to_string("/proc/meminfo") {
        (r.total, r.available) = parse_meminfo(&s);
    }
    // Linux: the container (cgroup v2, then v1). v2 says "max" for no limit, v1 a
    // number near u64::MAX; neither parses as a limit. Usage is taken net of the
    // cgroup's page cache (`file` in v2, `total_cache` in v1).
    let read_u64 = |path: &str| std::fs::read_to_string(path).ok().and_then(|s| s.trim().parse::<u64>().ok());
    for (limit, used, stat, cache_key) in [
        ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.stat", "file"),
        ("/sys/fs/cgroup/memory/memory.limit_in_bytes", "/sys/fs/cgroup/memory/memory.usage_in_bytes", "/sys/fs/cgroup/memory/memory.stat", "total_cache"),
    ] {
        if let Some(l) = read_u64(limit).filter(|v| *v < u64::MAX / 2) {
            r.limit = Some(l);
            let cache = std::fs::read_to_string(stat).ok().and_then(|s| parse_cgroup_stat(&s, cache_key)).unwrap_or(0);
            r.used = read_u64(used).map(|u| u.saturating_sub(cache));
            break;
        }
    }
    // macOS (a developer's box): sysctl gives the total; there is no cheap available.
    if r.total.is_none() && cfg!(target_os = "macos") {
        if let Ok(out) = std::process::Command::new("sysctl").args(["-n", "hw.memsize"]).output() {
            r.total = String::from_utf8_lossy(&out.stdout).trim().parse::<u64>().ok();
        }
    }
    r
}

/// Why a training run may not start on this machine, or `None` when it may (or when
/// memory cannot be read at all: an unknown does not block training). The reason
/// names what was measured and what would fix it, and never recommends an instance
/// type the same gate would refuse.
pub fn memory_refusal(m: &MemoryReading) -> Option<String> {
    let gb = |b: u64| format!("{:.1} GB", b as f64 / (1024.0 * 1024.0 * 1024.0));
    let free = m.headroom()?;
    if free >= TRAIN_MIN_FREE_MEMORY_BYTES {
        return None;
    }
    let need = gb(TRAIN_MIN_FREE_MEMORY_BYTES);
    let why = match (m.container_bound(), m.limit, m.total) {
        (true, Some(limit), _) => format!(
            "the container's memory limit of {} leaves {}; raise the container's memory limit",
            gb(limit), gb(free),
        ),
        (_, _, Some(total)) if total < TRAIN_MIN_TOTAL_MEMORY_BYTES => format!(
            "this machine has {} free of {}; use a t3.medium (4 GB) or larger",
            gb(free), gb(total),
        ),
        (_, _, Some(total)) => format!(
            "this machine has {} free of {}, so something else is using the memory (a local LLM?); stop it or use a larger instance",
            gb(free), gb(total),
        ),
        (_, _, None) => format!("this machine has {} free; use a larger instance", gb(free)),
    };
    Some(format!("training refused: a fit needs {need} free beside the engine and {why}"))
}

// ── Status ───────────────────────────────────────────────────────────────────

/// The pipeline's state for one asset, as the API and the GBoost card see it.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct PipelineStatus {
    pub asset: String,
    /// `disabled` | `waiting_for_squadron` | `backfilling` | `catching_up` | `training` | `idle` | `not_this_venue`
    pub phase: String,
    pub message: String,
    pub backfill_done: usize,
    pub backfill_total: usize,
    pub backfill_missing: usize,
    pub backfill_unresolved: usize,
    pub window_from: Option<String>,
    pub window_through: Option<String>,
    pub funding_source: Option<String>,
    pub funding_through: Option<String>,
    /// The most recent standing error, or `None` when nothing is currently failing.
    /// Derived from `errors`, so a failure that has since healed does not linger: on
    /// 2026-09-13 production's backfill got one 429 on a May market, fetched it on the
    /// next pass as designed, and kept showing the 429 on the card for hours.
    pub last_error: Option<String>,
    /// Standing errors by source (`funding`, `bars`, `market:<window>`, `write`), each
    /// cleared when that source next succeeds or, for a market, once its record is on
    /// disk. A persistent failure stays here until it stops failing.
    #[serde(default)]
    pub errors: BTreeMap<String, String>,
    /// The machine's memory (or the container's limit when smaller), the headroom a
    /// fit could take at the last check, and why training is refused when that is too
    /// little (`phase` is then `memory_too_small`).
    pub memory_bytes: Option<u64>,
    #[serde(default)]
    pub memory_available_bytes: Option<u64>,
    pub memory_refusal: Option<String>,
    pub last_cycle: Option<CycleReport>,
    pub next_train_at: Option<String>,
    pub squadron_id: Option<String>,
    pub updated_at: String,
}

impl PipelineStatus {
    /// Record that `source` is failing now. `last_error` shows it until it heals.
    pub fn note_error(&mut self, source: &str, message: String) {
        self.errors.insert(source.to_string(), message.clone());
        self.last_error = Some(message);
    }

    /// `source` succeeded: its standing error, if any, is over.
    pub fn clear_error(&mut self, source: &str) {
        if self.errors.remove(source).is_some() {
            self.refresh_last_error();
        }
    }

    /// Drop every `market:<w>` error whose record `exists` on disk now, however it got
    /// there (a later pass, a retry). What remains is still genuinely missing.
    pub fn reconcile_market_errors(&mut self, exists: impl Fn(i64) -> bool) {
        let before = self.errors.len();
        self.errors.retain(|k, _| !k.strip_prefix("market:").and_then(|w| w.parse::<i64>().ok()).is_some_and(&exists));
        if self.errors.len() != before {
            self.refresh_last_error();
        }
    }

    fn refresh_last_error(&mut self) {
        // With no insertion order kept, any standing error is a fair "current trouble";
        // the map itself is on the status endpoint for the full picture.
        self.last_error = self.errors.values().next_back().cloned();
    }
}

static STATUS: OnceLock<Mutex<HashMap<String, PipelineStatus>>> = OnceLock::new();

fn status_map() -> &'static Mutex<HashMap<String, PipelineStatus>> {
    STATUS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn update_status(asset: &str, f: impl FnOnce(&mut PipelineStatus)) {
    let mut map = match status_map().lock() { Ok(m) => m, Err(p) => p.into_inner() };
    let st = map.entry(asset.to_ascii_lowercase()).or_insert_with(|| PipelineStatus { asset: asset.to_ascii_lowercase(), ..Default::default() });
    f(st);
    st.updated_at = rfc3339(Utc::now().timestamp());
}

/// The full pipeline state for `GET /api/gboost/planb/status`.
pub fn snapshot(asset: &str) -> Option<PipelineStatus> {
    let map = match status_map().lock() { Ok(m) => m, Err(p) => p.into_inner() };
    map.get(&asset.to_ascii_lowercase()).cloned()
}

/// One line for the GBoost card: what the pipeline is doing now, and what the last
/// cycle decided.
pub fn status_line(asset: &str) -> Option<String> {
    let st = snapshot(asset)?;
    let now_line = match st.phase.as_str() {
        "backfilling" => format!(
            "backfilling training data: {} of {} markets{}",
            st.backfill_done, st.backfill_total,
            if st.backfill_total > 0 { format!(" ({}%)", st.backfill_done * 100 / st.backfill_total) } else { String::new() },
        ),
        "training" => "training and validating a candidate model".to_string(),
        "catching_up" => "fetching newly resolved markets".to_string(),
        "waiting_for_squadron" => "training waits for a BTC squadron (the plan knobs come from it)".to_string(),
        "disabled" => "in-engine training is off".to_string(),
        "memory_too_small" => st.memory_refusal.clone().unwrap_or_else(|| "training refused: too little memory".to_string()),
        "idle" => match &st.next_train_at {
            Some(t) => format!("data current; next training at {}", fmt_et(t)),
            None => "data current".to_string(),
        },
        other => other.to_string(),
    };
    let last = st.last_cycle.as_ref().map(|c| match c.decision.as_str() {
        "adopted" => format!("last cycle {}: {}", fmt_et(&c.finished_at), c.detail),
        _ => format!("last cycle {} ({}): {}", fmt_et(&c.finished_at), c.decision.replace('_', " "), c.detail),
    });
    let err = st.last_error.as_ref().map(|e| format!("last error: {e}"));
    Some([Some(now_line), last, err].into_iter().flatten().collect::<Vec<_>>().join(" | "))
}

fn fmt_et(rfc: &str) -> String {
    DateTime::parse_from_rfc3339(rfc)
        .map(|d| d.with_timezone(&chrono_tz::America::New_York).format("%Y-%m-%d %H:%M ET").to_string())
        .unwrap_or_else(|_| rfc.to_string())
}

// ── Knobs ────────────────────────────────────────────────────────────────────

/// Instance-wide training knobs, from the global config row (their schema group is
/// Global-scoped and rendered in Setup).
#[derive(Clone, Copy, Debug)]
pub struct TrainingKnobs {
    pub enabled: bool,
    pub auto_adopt: bool,
    pub window_days: i64,
    pub holdout_days: i64,
    pub retrain_hours: i64,
    pub min_trades: usize,
    pub min_win: f64,
    pub budget: f64,
}

impl TrainingKnobs {
    pub fn from_config(dc: &DynamicConfig) -> Self {
        Self {
            enabled: dc.gboost_planb_training_enabled,
            auto_adopt: dc.gboost_planb_auto_adopt,
            window_days: dc.gboost_planb_train_window_days.max(1),
            holdout_days: dc.gboost_planb_holdout_days.max(1),
            retrain_hours: dc.gboost_planb_retrain_hours.max(1),
            min_trades: dc.gboost_planb_gate_min_trades.max(1) as usize,
            min_win: dc.gboost_planb_gate_min_win_rate.to_f64().unwrap_or(0.53),
            budget: dc.gboost_planb_budget.to_f64().unwrap_or(2.0),
        }
    }
}

fn training_knobs() -> TrainingKnobs {
    match crate::helpers::dynamic_config::global_config_tx() {
        Some(tx) => TrainingKnobs::from_config(&tx.borrow()),
        None => TrainingKnobs::from_config(&DynamicConfig::default()),
    }
}

/// The plan knobs of the asset's deployed squadron, and the fee rate. Squadron ids
/// begin with the asset slug; the first registered one is used and named in the status.
fn squadron_plan(asset: &str) -> Option<(String, Plan)> {
    let prefix = format!("{}-", asset.to_ascii_lowercase());
    let id = crate::helpers::dynamic_config::registered_squadron_ids()
        .into_iter()
        .find(|id| id == &asset.to_ascii_lowercase() || id.starts_with(&prefix))?;
    let dc = crate::helpers::dynamic_config::squadron_config_snapshot(&id)?;
    // The venue's rate from the global knob, the same figure the live gate and
    // the ledger use; the squadron row's copy is not kept in step with it.
    let fee = crate::venues::taker_fee_rate().to_f64().unwrap_or(0.07);
    Some((id, Plan::from_config(&dc, fee)))
}

// ── The loop ─────────────────────────────────────────────────────────────────

/// Catch the data store up to the last resolved market. Returns whether every window
/// in range has a record.
async fn catch_up(asset: &str, dir: &DataDir, knobs: &TrainingKnobs, now: i64) -> bool {
    let from = floor_hour(now) - knobs.window_days * 86400;
    let last_resolved = floor_hour(now - 3600 - RESOLUTION_GRACE_SECS);
    let _ = std::fs::create_dir_all(dir.markets());
    let _ = std::fs::create_dir_all(dir.klines());

    // Funding first: cheap, and every row wants it.
    let mut funding: Vec<(i64, f64)> = read_json(&dir.funding()).unwrap_or_default();
    let since = funding.last().map(|x| x.0).unwrap_or(from - 86400);
    if since < now - 8 * 3600 || funding.is_empty() {
        match fetch_funding_since(if funding.is_empty() { from - 86400 } else { since }).await {
            Ok((more, host)) => {
                funding.extend(more);
                funding.sort_by_key(|x| x.0);
                funding.dedup_by_key(|x| x.0);
                if let Err(e) = write_json_atomically(&dir.funding(), &funding) {
                    warn!("GBoost plan-B pipeline [{asset}]: cannot write funding.json: {e}");
                }
                let through = funding.last().map(|x| rfc3339(x.0));
                update_status(asset, |s| { s.funding_source = Some(host.to_string()); s.funding_through = through; s.clear_error("funding"); });
            }
            Err(e) => {
                warn!("GBoost plan-B pipeline [{asset}]: funding history unavailable ({e}); rows will carry no funding until it is");
                update_status(asset, |s| s.note_error("funding", format!("funding history unavailable: {e}")));
            }
        }
    }

    // Bars by UTC day.
    let mut day = utc_day(from - KLINE_LEAD_DAYS * 86400);
    let today = utc_day(now);
    while day <= today {
        let path = dir.kline_day(day);
        let have: Option<KlineDay> = read_json(&path).ok();
        let refetch = match &have {
            None => true,
            Some(d) => !d.complete && day < today || (day == today && d.bars.last().map(|b| b[0] as i64 + 120 <= now).unwrap_or(true)),
        };
        if refetch {
            match fetch_klines_day(day, now).await {
                Ok(d) => {
                    match write_json_atomically(&path, &d) {
                        Ok(()) => update_status(asset, |s| { s.clear_error("bars"); s.clear_error("write"); }),
                        Err(e) => {
                            warn!("GBoost plan-B pipeline [{asset}]: cannot write {}: {e}", path.display());
                            update_status(asset, |s| s.note_error("write", format!("cannot write {}: {e}", path.display())));
                        }
                    }
                }
                Err(e) => {
                    warn!("GBoost plan-B pipeline [{asset}]: Binance bars for {day} unavailable ({e})");
                    update_status(asset, |s| s.note_error("bars", format!("Binance bars for {day} unavailable: {e}")));
                    break;
                }
            }
        }
        let Some(next) = day.succ_opt() else { break };
        day = next;
    }

    // Markets: oldest first. Progress is reported as it goes.
    let windows: Vec<i64> = (floor_hour(from)..=last_resolved).step_by(3600).collect();
    let mut missing = 0usize;
    let mut done = 0usize;
    let mut todo: Vec<i64> = Vec::new();
    for &w in &windows {
        match read_json::<MarketRecord>(&dir.market(w)) {
            Ok(r) => { done += 1; if r.missing { missing += 1; } }
            Err(_) => todo.push(w),
        }
    }
    let total = windows.len();
    let phase = if todo.len() > 24 { "backfilling" } else { "catching_up" };
    update_status(asset, |s| {
        s.phase = phase.to_string();
        s.backfill_done = done; s.backfill_total = total; s.backfill_missing = missing; s.backfill_unresolved = 0;
        s.window_from = Some(rfc3339(from)); s.window_through = Some(rfc3339(last_resolved + 3600));
    });
    let mut unresolved = 0usize;
    let mut fetched_this_pass = 0usize;
    for w in todo {
        // The operator can stop a long backfill from the Control Tower.
        if !training_knobs().enabled { break; }
        match fetch_market(w, Utc::now().timestamp()).await {
            Ok(Fetched::Record(rec)) => {
                if rec.missing { missing += 1; }
                if let Err(e) = write_json_atomically(&dir.market(w), &rec) {
                    warn!("GBoost plan-B pipeline [{asset}]: cannot write market {w}: {e}");
                    update_status(asset, |s| s.note_error("write", format!("cannot write market {w}: {e}")));
                    break;
                }
                done += 1;
                fetched_this_pass += 1;
                update_status(asset, |s| { s.clear_error("write"); s.clear_error(&format!("market:{w}")); });
            }
            Ok(Fetched::NotResolved) => unresolved += 1,
            Err(e) => {
                warn!("GBoost plan-B pipeline [{asset}]: market {} ({w}) not fetched: {e}", slug_for(w));
                update_status(asset, |s| s.note_error(&format!("market:{w}"), format!("{}: {e}", slug_for(w))));
            }
        }
        update_status(asset, |s| { s.backfill_done = done; s.backfill_missing = missing; s.backfill_unresolved = unresolved; });
    }
    if fetched_this_pass > 0 {
        info!("GBoost plan-B pipeline [{asset}]: {fetched_this_pass} market(s) fetched; {done} of {total} windows on disk ({missing} without a market)");
    }
    // A market that failed on an earlier pass and is on disk now is no longer trouble.
    update_status(asset, |s| s.reconcile_market_errors(|w| dir.market(w).exists()));

    // Prune records that have slid out of the window.
    if let Ok(entries) = std::fs::read_dir(dir.markets()) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(w) = name.strip_suffix(".json").and_then(|s| s.parse::<i64>().ok()) {
                if w < from - PRUNE_SLACK_SECS { let _ = std::fs::remove_file(e.path()); }
            }
        }
    }
    done + unresolved >= total
}

/// The pipeline for one asset. Runs forever; supervised by `main.rs`.
pub async fn run_pipeline(asset: String) {
    let asset = asset.to_ascii_lowercase();
    let dir = DataDir::new(&asset);
    let serving_path = model_path(&asset);
    let _ = std::fs::create_dir_all(&dir.root);
    // A restart shows the last cycle's outcome rather than nothing, and does not retrain
    // at once: the schedule counts from the persisted cycle.
    let persisted: Option<CycleReport> = read_json(&dir.status()).ok();
    update_status(&asset, |s| { s.phase = "starting".into(); s.last_cycle = persisted.clone(); });
    let mut last_cycle: Option<CycleReport> = persisted;
    info!("GBoost plan-B pipeline [{asset}]: started; data under {}, serving model {}", dir.root.display(), serving_path.display());

    loop {
        // A retired instance stops writing training data and models, so the
        // migration backup copies a folder nothing is changing.
        if crate::helpers::migration::is_retired() {
            update_status(&asset, |s| { s.phase = "retired".into(); s.next_train_at = None; });
            tokio::time::sleep(Duration::from_secs(60)).await;
            continue;
        }
        let knobs = training_knobs();
        if !knobs.enabled {
            update_status(&asset, |s| { s.phase = "disabled".into(); s.next_train_at = None; });
            tokio::time::sleep(Duration::from_secs(60)).await;
            continue;
        }
        let Some((squadron_id, plan)) = squadron_plan(&asset) else {
            update_status(&asset, |s| { s.phase = "waiting_for_squadron".into(); s.squadron_id = None; });
            tokio::time::sleep(Duration::from_secs(60)).await;
            continue;
        };
        update_status(&asset, |s| s.squadron_id = Some(squadron_id.clone()));
        let now = Utc::now().timestamp();
        let complete = catch_up(&asset, &dir, &knobs, now).await;

        let plan_changed = last_cycle.as_ref().and_then(|c| c.plan).is_some_and(|p| p != plan);
        // A refusal is not a cycle: it does not push the schedule.
        let due_at = last_cycle.as_ref()
            .filter(|c| c.decision != "refused")
            .and_then(|c| DateTime::parse_from_rfc3339(&c.finished_at).ok())
            .map(|t| t.timestamp() + knobs.retrain_hours * 3600);
        let due = complete && (plan_changed || due_at.is_none_or(|t| now >= t));
        update_status(&asset, |s| {
            s.phase = if complete { "idle".into() } else { s.phase.clone() };
            s.next_train_at = if complete && !due { due_at.map(rfc3339) } else { None };
        });
        let memory = read_memory();
        let refusal = memory_refusal(&memory);
        update_status(&asset, |s| {
            s.memory_bytes = memory.capacity();
            s.memory_available_bytes = memory.headroom();
            s.memory_refusal = refusal.clone();
        });
        if let Some(why) = refusal.filter(|_| due) {
            // The data stays current and the serving model keeps trading; only the fit is
            // refused, and the card says why. Checked again next pass, so a resize takes.
            if !last_cycle.as_ref().is_some_and(|c| c.decision == "refused" && c.detail == why) {
                warn!("GBoost plan-B pipeline [{asset}]: {why}");
                let rep = CycleReport { decision: "refused".into(), detail: why.clone(), started_at: rfc3339(now), finished_at: rfc3339(now), plan: Some(plan), ..Default::default() };
                let _ = write_json_atomically(&dir.status(), &rep);
                last_cycle = Some(rep.clone());
                update_status(&asset, |s| s.last_cycle = Some(rep));
            }
            update_status(&asset, |s| { s.phase = "memory_too_small".into(); s.next_train_at = None; });
            tokio::time::sleep(Duration::from_secs(CATCHUP_INTERVAL_SECS)).await;
            continue;
        }
        if due {
            update_status(&asset, |s| { s.phase = "training".into(); s.next_train_at = None; });
            info!(
                "GBoost plan-B pipeline [{asset}]: training cycle starting ({}; plan {}, window {} d, holdout {} d, budget {})",
                if plan_changed { "the plan changed" } else { "scheduled" }, plan.label(), knobs.window_days, knobs.holdout_days, knobs.budget,
            );
            let inputs = CycleInputs {
                asset: asset.clone(),
                dir: dir.clone(),
                serving_path: serving_path.clone(),
                plan,
                now,
                window_days: knobs.window_days,
                holdout_days: knobs.holdout_days,
                min_trades: knobs.min_trades,
                min_win: knobs.min_win,
                budget: knobs.budget,
                auto_adopt: knobs.auto_adopt,
                threads: fit_threads(),
            };
            let (tx, rx) = tokio::sync::oneshot::channel();
            let spawned = std::thread::Builder::new()
                .name(format!("gboost-planb-trainer-{asset}"))
                .spawn(move || { let _ = tx.send(run_cycle_blocking(&inputs)); });
            let report = match spawned {
                Ok(_) => match rx.await {
                    Ok(r) => r,
                    Err(_) => CycleReport { decision: "error".into(), detail: "the training thread ended without a report".into(), finished_at: rfc3339(Utc::now().timestamp()), ..Default::default() },
                },
                Err(e) => CycleReport { decision: "error".into(), detail: format!("cannot start the training thread: {e}"), finished_at: rfc3339(Utc::now().timestamp()), ..Default::default() },
            };
            match report.decision.as_str() {
                "adopted" => info!("GBoost plan-B pipeline [{asset}]: {}", report.detail),
                "error" => warn!("GBoost plan-B pipeline [{asset}]: cycle failed: {}", report.detail),
                _ => info!("GBoost plan-B pipeline [{asset}]: {} ({})", report.detail, report.decision),
            }
            let stamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
            let _ = std::fs::create_dir_all(dir.reports());
            if let Err(e) = write_json_atomically(&dir.reports().join(format!("{stamp}.json")), &report) {
                warn!("GBoost plan-B pipeline [{asset}]: cannot write the cycle report: {e}");
            }
            if let Err(e) = write_json_atomically(&dir.status(), &report) {
                warn!("GBoost plan-B pipeline [{asset}]: cannot write status.json: {e}");
            }
            last_cycle = Some(report.clone());
            update_status(&asset, |s| { s.phase = "idle".into(); s.last_cycle = Some(report); s.next_train_at = Some(rfc3339(Utc::now().timestamp() + knobs.retrain_hours * 3600)); });
        }
        tokio::time::sleep(Duration::from_secs(CATCHUP_INTERVAL_SECS)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> serde_json::Value {
        serde_json::from_str(include_str!("testdata/gboost_planb_rows.json")).unwrap()
    }

    fn plan_from(fx: &serde_json::Value) -> Plan {
        Plan {
            tp: fx["plan"]["tp"].as_f64().unwrap(),
            sl: fx["plan"]["sl"].as_f64().unwrap(),
            tp_ceiling: 0.90,
            lo: fx["plan"]["lo"].as_f64().unwrap(),
            hi: fx["plan"]["hi"].as_f64().unwrap(),
            fee: fx["plan"]["fee"].as_f64().unwrap(),
            margin: 0.10,
            first_minute: 5,
            last_minute: 45,
            // The fixture rows come from the reference harness, which buys at the
            // price before the minute; production trains on `FirstAfter` ([B47]).
            entry: EntryRule::LastBefore,
        }
    }

    /// Three real June 2026 markets from the pre-registered backward period: the
    /// engine's builder, fed the stored record, bars and funding, must reproduce every
    /// row the reference builder produced: the executable ask, the 29 inputs, the
    /// plan-B label, return, exit kind and exit time, and eligibility.
    #[test]
    fn the_row_builder_reproduces_the_reference_rows() {
        let fx = fixture();
        let names: Vec<&str> = fx["feature_names"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(names, FEATURE_NAMES.to_vec());
        let plan = plan_from(&fx);
        let mut checked = 0;
        let mut exits: HashSet<String> = HashSet::new();
        for f in fx["fixtures"].as_array().unwrap() {
            let w = f["W"].as_i64().unwrap();
            let rec: MarketRecord = serde_json::from_value(f["record"].clone()).unwrap();
            let bars: Vec<Bar> = f["bars"].as_array().unwrap().iter().map(|b| {
                let b = b.as_array().unwrap();
                Bar { open_s: b[0].as_i64().unwrap(), open: b[1].as_f64().unwrap(), high: b[2].as_f64().unwrap(), low: b[3].as_f64().unwrap(), close: b[4].as_f64().unwrap() }
            }).collect();
            let funding: Vec<(i64, f64)> = f["funding"].as_array().unwrap().iter().map(|x| (x[0].as_i64().unwrap(), x[1].as_f64().unwrap())).collect();
            let got = build_market_rows(w, &rec, &bars, &funding, &plan);
            let expected = f["expected"].as_array().unwrap();
            assert_eq!(got.len(), expected.len(), "market {w}: row count");
            for (g, e) in got.iter().zip(expected) {
                let tag = format!("market {w} k={} side {}", g.k, g.side);
                assert_eq!((g.k, g.side), (e["k"].as_i64().unwrap(), e["side"].as_u64().unwrap() as usize), "{tag}: order");
                assert!((g.ask - e["ask"].as_f64().unwrap()).abs() < 1e-12, "{tag}: ask {} vs {}", g.ask, e["ask"]);
                for (j, name) in FEATURE_NAMES.iter().enumerate() {
                    match e["features"][j].as_f64() {
                        None => assert!(g.features[j].is_nan(), "{tag} {name}: expected missing, got {}", g.features[j]),
                        Some(x) => assert!((g.features[j] - x).abs() < 1e-6, "{tag} {name}: got {} expected {x}", g.features[j]),
                    }
                }
                assert_eq!(g.y, e["y"].as_i64().unwrap() == 1, "{tag}: label");
                assert!((g.ret - e["ret"].as_f64().unwrap()).abs() < 1e-12, "{tag}: return {} vs {}", g.ret, e["ret"]);
                let exit = match g.exit { ExitKind::Tp => "tp", ExitKind::Sl => "sl", ExitKind::Settle => "settle" };
                assert_eq!(exit, e["exit"].as_str().unwrap(), "{tag}: exit kind");
                assert_eq!(g.exit_t - (w + 60 * g.k), e["hold"].as_i64().unwrap(), "{tag}: exit time");
                assert_eq!(g.elig, e["elig"].as_i64().unwrap() == 1, "{tag}: eligibility");
                exits.insert(exit.to_string());
                checked += 1;
            }
        }
        assert!(checked >= 200, "checked {checked} rows");
        assert_eq!(exits.len(), 3, "the fixture must exercise take-profit, stop and settlement exits");
    }

    /// The futures columns are never available to the engine, so they are dead in every
    /// engine fit and stamped as zeroed; funding is dead only where no source answers.
    #[test]
    fn dead_columns_are_the_all_missing_ones() {
        let fx = fixture();
        let plan = plan_from(&fx);
        let f = &fx["fixtures"][0];
        let rec: MarketRecord = serde_json::from_value(f["record"].clone()).unwrap();
        let bars: Vec<Bar> = f["bars"].as_array().unwrap().iter().map(|b| {
            let b = b.as_array().unwrap();
            Bar { open_s: b[0].as_i64().unwrap(), open: b[1].as_f64().unwrap(), high: b[2].as_f64().unwrap(), low: b[3].as_f64().unwrap(), close: b[4].as_f64().unwrap() }
        }).collect();
        let with = build_market_rows(f["W"].as_i64().unwrap(), &rec, &bars, &[(0, 0.0001)], &plan);
        let xs: Vec<[f64; N_FEATURES]> = with.iter().map(|r| r.features).collect();
        let dead: Vec<&str> = dead_columns(&xs).into_iter().map(|j| FEATURE_NAMES[j]).collect();
        assert_eq!(dead, vec!["oi_d5", "oi_d30", "tlsr", "tlsr15"]);
        let without = build_market_rows(f["W"].as_i64().unwrap(), &rec, &bars, &[], &plan);
        let xs: Vec<[f64; N_FEATURES]> = without.iter().map(|r| r.features).collect();
        let dead: Vec<&str> = dead_columns(&xs).into_iter().map(|j| FEATURE_NAMES[j]).collect();
        assert_eq!(dead, vec!["funding", "oi_d5", "oi_d30", "tlsr", "tlsr15"]);
    }

    /// Platt scaling on 300 golden rows of the production model must land on the same
    /// slope and intercept sklearn's unregularized logistic regression found.
    #[test]
    fn platt_scaling_matches_sklearn() {
        let fx: serde_json::Value = serde_json::from_str(include_str!("testdata/gboost_planb_platt.json")).unwrap();
        let raw: Vec<f64> = fx["raw"].as_array().unwrap().iter().map(|v| v.as_f64().unwrap()).collect();
        let y: Vec<bool> = fx["y"].as_array().unwrap().iter().map(|v| v.as_i64().unwrap() == 1).collect();
        let (a, b) = fit_platt(&raw, &y).expect("converges");
        assert!((a - fx["platt_a"].as_f64().unwrap()).abs() < 1e-6, "a={a}");
        assert!((b - fx["platt_b"].as_f64().unwrap()).abs() < 1e-6, "b={b}");
        assert!(fit_platt(&raw, &vec![true; raw.len()]).is_none(), "one class cannot be calibrated");
    }

    #[test]
    fn auc_and_percentile_behave() {
        assert!((auc(&[true, false, true, false], &[0.9, 0.1, 0.8, 0.2]) - 1.0).abs() < 1e-12);
        assert!((auc(&[true, false, true, false], &[0.5, 0.5, 0.5, 0.5]) - 0.5).abs() < 1e-12);
        assert!(auc(&[true, true], &[0.1, 0.2]).is_nan());
        assert!((percentile(&[1.0, 2.0, 3.0, 4.0], 50.0) - 2.5).abs() < 1e-12);
        assert!((percentile(&[1.0, 2.0, 3.0, 4.0], 5.0) - 1.15).abs() < 1e-12);
    }

    fn synthetic_rows(n_markets: i64, seed: u64) -> Vec<TrainingRow> {
        let mut rng = SplitMix(seed);
        let mut rows = Vec::new();
        for m in 0..n_markets {
            let w = 1_780_000_000 + m * 3600;
            for k in 5..=45 {
                for side in 0..2 {
                    let mut f = [0.0f64; N_FEATURES];
                    for (j, v) in f.iter_mut().enumerate() { *v = (rng.next() % 1000) as f64 / 1000.0 + j as f64; }
                    f[0] = side as f64;
                    let ask = 0.43 + (rng.next() % 33) as f64 / 100.0;
                    f[2] = ask;
                    // A learnable signal: the label follows feature 19 (fv_edge) plus noise.
                    let signal = (f[19] - 19.0) > 0.5;
                    let noise = rng.next() % 4 == 0;
                    let y = signal ^ noise;
                    let ret = if y { 0.2 } else { -0.11 };
                    rows.push(TrainingRow { w, k, side, ask, features: f, y, ret, exit: if y { ExitKind::Tp } else { ExitKind::Sl }, exit_t: w + 60 * k + 600, elig: true });
                }
            }
        }
        rows
    }

    /// An engine-trained model, written through the atomic writer with its stamp, loads
    /// through the viper's own loader, carries the plan and provenance the viper checks,
    /// and scores finite calibrated probabilities.
    #[test]
    fn an_engine_trained_model_is_stamped_and_loads() {
        let tmp = std::env::temp_dir().join(format!("dradis-gboost-planb-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let dir = DataDir { root: tmp.join("pipeline") };
        let serving = tmp.join("btc-gboost_planb_v1.json");
        let rows = synthetic_rows(60, 7);
        let plan = Plan { entry: EntryRule::FirstAfter, tp: 0.20, sl: 0.11, tp_ceiling: 0.90, lo: 0.43, hi: 0.75, fee: 0.07, margin: 0.10, first_minute: 5, last_minute: 45 };
        let now = rows.last().unwrap().w + 3600 + 60;
        // Write the rows as a fake market store would not exercise the fetchers; call
        // the cycle on already-built rows through the same fit/stamp/write path.
        let inputs = CycleInputs {
            asset: "btc".into(), dir: dir.clone(), serving_path: serving.clone(), plan, now,
            window_days: 30, holdout_days: 1, min_trades: 5, min_win: 0.5, budget: 0.5, auto_adopt: true, threads: 1,
        };
        // Split as the cycle would, then train directly.
        let fold_start = floor_hour(now) - 86400;
        let train: Vec<&TrainingRow> = rows.iter().filter(|r| r.w + 3600 <= fold_start - PURGE_SECS).collect();
        let test: Vec<&TrainingRow> = rows.iter().filter(|r| r.w >= fold_start).collect();
        assert!(train.len() >= MIN_FIT_ROWS && test.len() >= MIN_TEST_ROWS);
        // Fit on the older 80% of the training markets, calibrate on the newest 20%, as the cycle does.
        let mut ws: Vec<i64> = train.iter().map(|r| r.w).collect::<HashSet<_>>().into_iter().collect();
        ws.sort_unstable();
        let cut = ws[(ws.len() as f64 * (1.0 - CAL_FRACTION)) as usize];
        let fit: Vec<&TrainingRow> = train.iter().copied().filter(|r| r.w + 3600 <= cut - PURGE_SECS).collect();
        let cal: Vec<&TrainingRow> = train.iter().copied().filter(|r| r.w >= cut).collect();
        let xf: Vec<[f64; N_FEATURES]> = fit.iter().map(|r| r.features).collect();
        let yf: Vec<bool> = fit.iter().map(|r| r.y).collect();
        let mut booster = fit_booster(&xf, &yf, &[], 0.5, 1).expect("fits");
        assert!(booster.get_prediction_trees().len() >= STRUCTURAL_MIN_TREES);
        let xc: Vec<[f64; N_FEATURES]> = cal.iter().map(|r| r.features).collect();
        let yc: Vec<bool> = cal.iter().map(|r| r.y).collect();
        let (a, b) = fit_platt(&predict_raw(&booster, &xc, &[]), &yc).expect("calibrates");
        for (k, v) in [
            ("feature_layout", "column_major".to_string()), ("feature_names", FEATURE_NAMES.join(",")),
            ("model_version", "engine-btc-test".into()), ("trained_by", "dradis-engine".into()), ("venue", TRAINED_VENUE.into()),
            ("platt_a", format!("{a:?}")), ("platt_b", format!("{b:?}")), ("zeroed_features", "oi_d5,oi_d30,tlsr,tlsr15".into()),
            ("plan_tp", "0.2".into()), ("plan_sl", "0.11".into()), ("plan_min_ask", "0.43".into()), ("plan_max_ask", "0.75".into()),
            ("calibrated_through", rfc3339(fold_start - PURGE_SECS)),
        ] {
            booster.insert_metadata(k.to_string(), v);
        }
        let bytes = booster.json_dump().unwrap().into_bytes();
        write_atomically(&serving, &bytes).unwrap();
        assert!(!serving.with_extension("json.tmp").exists(), "the temp file is renamed away");
        let model = load_model(&serving).expect("the viper's loader accepts the engine's stamp");
        assert_eq!(model.version, "engine-btc-test");
        assert_eq!(model.trained_by.as_deref(), Some("dradis-engine"));
        assert_eq!(model.zeroed, vec![23, 24, 25, 26]);
        assert_eq!(model.plan, Some([0.2, 0.11, 0.43, 0.75]));
        let xt: Vec<[f64; N_FEATURES]> = test.iter().map(|r| r.features).collect();
        let preds = model.predict(&xt);
        assert!(preds.iter().all(|(r, c)| r.is_finite() && c.is_finite() && *c > 0.0 && *c < 1.0));
        // It learned the synthetic signal, and calibrated it beats the base rate.
        let pc: Vec<f64> = preds.iter().map(|p| p.1).collect();
        let base = train.iter().filter(|r| r.y).count() as f64 / train.len() as f64;
        let stats = fold_stats(&test, &pc, base, &plan);
        assert!(stats.auc > 0.7, "auc {}", stats.auc);
        assert!(stats.skill > 0.0, "skill {} (a={a} b={b})", stats.skill);
        let _ = inputs;
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// The gate refuses a degenerate or losing candidate for a named reason and passes a
    /// sound one; a candidate beats an incumbent on return when the incumbent trades, on
    /// skill when it does not.
    #[test]
    fn the_gate_names_every_failure_and_the_comparison_is_by_return_then_skill() {
        let good = FoldStats { skill: 0.05, rule: RuleStats { trades: 40, mean_ret: 0.03, win: 0.60, ..Default::default() }, ..Default::default() };
        assert!(gate(300, &good, 30, 0.53).passed);
        let g = gate(3, &FoldStats { skill: -0.01, rule: RuleStats { trades: 10, mean_ret: -0.02, win: 0.40, ..Default::default() }, ..Default::default() }, 30, 0.53);
        assert!(!g.passed);
        assert_eq!(g.reasons.len(), 5, "{:?}", g.reasons);
        let nan = gate(300, &FoldStats { skill: f64::NAN, rule: RuleStats { trades: 0, mean_ret: f64::NAN, win: f64::NAN, ..Default::default() }, ..Default::default() }, 30, 0.53);
        assert!(!nan.passed, "a fold with no trades cannot pass");
        let worse = FoldStats { skill: 0.09, rule: RuleStats { trades: 40, mean_ret: 0.04, win: 0.6, ..Default::default() }, ..Default::default() };
        assert!(!beats_incumbent(&good, &worse, 30).0, "a lower return loses even with lower skill on the incumbent's side");
        assert!(beats_incumbent(&worse, &good, 30).0);
        let silent = FoldStats { skill: 0.09, rule: RuleStats { trades: 3, mean_ret: 0.5, ..Default::default() }, ..Default::default() };
        assert!(!beats_incumbent(&good, &silent, 30).0, "an incumbent that barely trades is compared on skill");
        assert!(beats_incumbent(&worse, &silent, 30).0);
    }

    /// First qualifying minute per market and side, and only eligible rows count.
    #[test]
    fn the_rule_takes_the_first_qualifying_minute_per_market_and_side() {
        let plan = Plan { entry: EntryRule::FirstAfter, tp: 0.20, sl: 0.11, tp_ceiling: 0.90, lo: 0.43, hi: 0.75, fee: 0.07, margin: 0.10, first_minute: 5, last_minute: 45 };
        let mk = |w, k, side, ret| TrainingRow { w, k, side, ask: 0.5, features: [0.0; N_FEATURES], y: ret > 0.0, ret, exit: ExitKind::Tp, exit_t: 0, elig: true };
        let rows = vec![mk(0, 10, 0, 0.1), mk(0, 5, 0, 0.3), mk(0, 7, 1, -0.1), mk(3600, 20, 0, 0.2)];
        let refs: Vec<&TrainingRow> = rows.iter().collect();
        let need = break_even(0.5, 0.2, 0.11, 0.07) + 0.10;
        let p = vec![need + 0.01, need + 0.01, need + 0.01, need - 0.01];
        let s = rule_stats(&refs, &p, &plan);
        assert_eq!(s.trades, 2);
        assert_eq!(s.markets, 1);
        assert!((s.mean_ret - 0.1).abs() < 1e-12, "minute 5's return and side 1's, not minute 10's: {}", s.mean_ret);
        assert!((s.win - 0.5).abs() < 1e-12);
        assert!(s.ci_lo <= s.mean_ret && s.mean_ret <= s.ci_hi);
    }

    /// A labeled trade is bought at the first price the market showed at or after the
    /// decision minute, and its exits start from that moment ([B47]).
    ///
    /// The old rule bought at a price from BEFORE the minute (a print in the previous
    /// 20 s, else a mid a median 53 s old), so the label could take a price the book
    /// no longer offered, and a print that arrived before the trade could end it.
    #[test]
    fn the_label_buys_at_the_first_price_shown_after_the_minute() {
        let w = 1_780_286_400;
        let t = w + 60 * 5; // the only decision minute in this plan
        let bars: Vec<Bar> = (0..125)
            .map(|i| { let o = w - 62 * 60 + 60 * i; Bar { open_s: o, open: 100.0, high: 100.0, low: 100.0, close: 100.0 } })
            .collect();
        let samples: Vec<HistPoint> = (0..50).map(|i| HistPoint { t: w + 60 * i + 9, p: 0.50 }).collect();
        let mut rec = MarketRecord {
            outcome_prices: Some("[\"1\", \"0\"]".into()), // Up won
            hist: HashMap::from([("0".to_string(), samples.clone()), ("1".to_string(), samples)]),
            // An early print: every market has a tape, and this one is old enough to be
            // nobody's entry and nobody's exit.
            tape: vec![Print { ts: w + 1, side: "BUY".into(), o: 1, p: 0.50, s: 1.0 }],
            ..Default::default()
        };
        let plan = Plan { entry: EntryRule::FirstAfter, tp: 0.20, sl: 0.11, tp_ceiling: 0.90, lo: 0.40, hi: 0.75, fee: 0.0, margin: 0.10, first_minute: 5, last_minute: 5 };
        let up_of = |rows: &[TrainingRow]| rows.iter().find(|r| r.side == 0).cloned().expect("a row for the Up side");

        // No print in the minute: the entry is the first sample AFTER it, plus half a spread.
        let rows = build_market_rows(w, &rec, &bars, &[], &plan);
        assert_eq!(rows.len(), 2, "one row per side");
        assert!((up_of(&rows).ask - (0.50 + HALF_SPREAD)).abs() < 1e-12, "ask {}", up_of(&rows).ask);

        // A print 5 s into the minute is what that trade paid, and it comes first.
        rec.tape.push(Print { ts: t + 5, side: "BUY".into(), o: 0, p: 0.62, s: 10.0 });
        let rows = build_market_rows(w, &rec, &bars, &[], &plan);
        assert!((up_of(&rows).ask - 0.62).abs() < 1e-12, "ask {}", up_of(&rows).ask);

        // A sale at 0.20 three seconds into the minute, before the Up side's entry
        // exists at t+9: it cannot stop a position that has not been bought.
        rec.tape = vec![Print { ts: w + 1, side: "BUY".into(), o: 1, p: 0.50, s: 1.0 },
                        Print { ts: t + 3, side: "SELL".into(), o: 0, p: 0.20, s: 100.0 }];
        let row = up_of(&build_market_rows(w, &rec, &bars, &[], &plan));
        assert!((row.ask - (0.50 + HALF_SPREAD)).abs() < 1e-12, "a sale is not an ask: {}", row.ask);
        assert_eq!(row.exit, ExitKind::Settle, "an exit cannot fire before the entry exists");
        assert!(row.y, "Up won this market");

        // The reference rule buys at the minute itself, so the same sale stops it: that
        // is what the harness fixture pins, and the stamp tells the two rules apart.
        let old = Plan { entry: EntryRule::LastBefore, ..plan };
        let row = up_of(&build_market_rows(w, &rec, &bars, &[], &old));
        assert_eq!(row.exit, ExitKind::Sl, "under LastBefore the sale is inside the position");
        assert!(!row.y);
        assert_ne!(plan.label(), old.label(), "the stamp must tell the two rules apart");
    }

    /// A whole training cycle on the research harness's own June to July 2026 market
    /// files, bars and funding: loads them from a temp data dir laid out as the pipeline
    /// lays it out, fits, calibrates, scores the newest fold, gates, and adopts into a
    /// temp serving path. Prints the report. Run with `GBOOST_PLANB_REFERENCE_DIR` set
    /// to a harness data dir (`markets/*.json`, `btc_1m.jsonl`, `futures_stats.json`).
    #[test]
    #[ignore = "needs the research harness's data directory"]
    fn a_full_cycle_runs_on_the_reference_data() {
        let src = PathBuf::from(std::env::var("GBOOST_PLANB_REFERENCE_DIR").unwrap());
        let tmp = std::env::temp_dir().join(format!("dradis-gboost-planb-cycle-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let dir = DataDir { root: tmp.join("pipeline") };
        std::fs::create_dir_all(dir.markets()).unwrap();
        std::fs::create_dir_all(dir.klines()).unwrap();
        // Markets: the harness files are already in the record shape.
        let mut n = 0;
        let mut last_w = 0;
        for e in std::fs::read_dir(src.join("markets")).unwrap().flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(w) = name.strip_suffix(".json").and_then(|s| s.parse::<i64>().ok()) {
                std::fs::copy(e.path(), dir.market(w)).unwrap();
                n += 1;
                last_w = last_w.max(w);
            }
        }
        // Bars: one file per UTC day.
        let mut days: BTreeMap<chrono::NaiveDate, KlineDay> = BTreeMap::new();
        for line in std::fs::read_to_string(src.join("btc_1m.jsonl")).unwrap().lines() {
            let r: Vec<serde_json::Value> = serde_json::from_str(line).unwrap();
            let open_s = r[0].as_i64().unwrap() / 1000;
            let f = |i: usize| r[i].as_str().unwrap().parse::<f64>().unwrap();
            let d = days.entry(utc_day(open_s)).or_insert_with(|| KlineDay { day: utc_day(open_s).to_string(), complete: true, bars: Vec::new() });
            d.bars.push([open_s as f64, f(1), f(2), f(3), f(4)]);
        }
        for (day, d) in &days { write_json_atomically(&dir.kline_day(*day), d).unwrap(); }
        let fs: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(src.join("futures_stats.json")).unwrap()).unwrap();
        let mut funding: Vec<(i64, f64)> = fs["funding"].as_array().unwrap().iter()
            .map(|x| (x["fundingTime"].as_i64().unwrap() / 1000, x["fundingRate"].as_str().unwrap().parse().unwrap())).collect();
        funding.sort_by_key(|x| x.0);
        write_json_atomically(&dir.funding(), &funding).unwrap();
        let serving = tmp.join("btc-gboost_planb_v1.json");
        let plan = Plan { entry: EntryRule::FirstAfter, tp: 0.20, sl: 0.11, tp_ceiling: 0.90, lo: 0.43, hi: 0.75, fee: 0.07, margin: 0.10, first_minute: 5, last_minute: 45 };
        let inputs = CycleInputs {
            asset: "btc".into(), dir: dir.clone(), serving_path: serving.clone(), plan, now: last_w + 3600 + 1800,
            window_days: 120, holdout_days: 14, min_trades: 30, min_win: 0.53, budget: 2.0, auto_adopt: true, threads: fit_threads(),
        };
        let t0 = Instant::now();
        let rep = run_cycle_blocking(&inputs);
        println!("markets copied: {n}; cycle in {:.1}s\n{}", t0.elapsed().as_secs_f64(), serde_json::to_string_pretty(&rep).unwrap());
        assert_ne!(rep.decision, "error", "{}", rep.detail);
        assert!(dir.candidate().exists());
        if rep.decision == "adopted" {
            let m = load_model(&serving).unwrap();
            assert_eq!(m.trained_by.as_deref(), Some("dradis-engine"));
            assert_eq!(m.plan, Some([0.20, 0.11, 0.43, 0.75]));
            // A second cycle now has an incumbent to beat.
            let rep2 = run_cycle_blocking(&CycleInputs { now: inputs.now + 3600, ..CycleInputs { asset: "btc".into(), dir: dir.clone(), serving_path: serving.clone(), plan, now: 0, window_days: 120, holdout_days: 14, min_trades: 30, min_win: 0.53, budget: 2.0, auto_adopt: true, threads: fit_threads() } });
            println!("second cycle: {} ({})", rep2.decision, rep2.detail);
            assert!(rep2.incumbent.is_some());
            assert_eq!(rep2.incumbent_version.as_deref(), rep.model_version.as_deref());
        }
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// The fetchers against the live public APIs from wherever this runs: one resolved
    /// market, its day of bars, and the funding history, then rows from them.
    #[tokio::test]
    #[ignore = "network"]
    async fn live_fetch_builds_rows_for_a_recent_market() {
        let now = Utc::now().timestamp();
        let w = floor_hour(now - 3 * 3600);
        let rec = match fetch_market(w, now).await.unwrap() {
            Fetched::Record(r) => r,
            Fetched::NotResolved => panic!("a market three hours old should have resolved"),
        };
        println!("{}: tape {} prints, hist {}/{} points, up_won {:?}", rec.slug, rec.tape.len(), rec.hist["0"].len(), rec.hist["1"].len(), rec.up_won());
        assert!(!rec.missing && rec.up_won().is_some());
        let day = fetch_klines_day(utc_day(w), now).await.unwrap();
        let prev = fetch_klines_day(utc_day(w - 86400), now).await.unwrap();
        println!("bars: {} today ({}), {} yesterday (complete {})", day.bars.len(), day.day, prev.bars.len(), prev.complete);
        // Funding comes from fapi alone. From a US address it answers 451, and the honest
        // outcome is rows with no funding, which the fit then zeroes and stamps.
        let funding = match fetch_funding_since(now - 3 * 86400).await {
            Ok((f, host)) => { println!("funding: {} rows from {host}, last {:?}", f.len(), f.last()); assert!(!f.is_empty()); f }
            Err(e) => { println!("funding unavailable here ({e}); rows will carry none"); Vec::new() }
        };
        let bars: Vec<Bar> = bars_from_day(&prev).chain(bars_from_day(&day)).collect();
        let plan = Plan { entry: EntryRule::FirstAfter, tp: 0.20, sl: 0.11, tp_ceiling: 0.90, lo: 0.43, hi: 0.75, fee: 0.07, margin: 0.10, first_minute: 5, last_minute: 45 };
        let rows = build_market_rows(w, &rec, &bars, &funding, &plan);
        println!("rows: {} ({} eligible); first: ask {:.3} y {} exit {:?}", rows.len(), rows.iter().filter(|r| r.elig).count(), rows[0].ask, rows[0].y, rows[0].exit);
        assert!(rows.len() >= 60);
        let xs: Vec<[f64; N_FEATURES]> = rows.iter().map(|r| r.features).collect();
        let dead: Vec<&str> = dead_columns(&xs).into_iter().map(|j| FEATURE_NAMES[j]).collect();
        println!("dead columns here: {dead:?}");
        assert_eq!(rows.iter().all(|r| !r.features[22].is_nan()), !funding.is_empty(), "funding is in every row exactly when a source answered");
        println!("memory: {:?}, refusal {:?}", read_memory(), memory_refusal(&read_memory()));
    }

    /// Production, 2026-09-13: one 429 on a May market during the backfill stayed on the
    /// card as `last_error` for hours after the next pass had fetched it. A standing
    /// error must clear when its source succeeds or its record appears on disk, and a
    /// failure that keeps failing must stay visible.
    #[test]
    fn last_error_reflects_current_trouble_only() {
        let mut st = PipelineStatus::default();
        assert!(st.last_error.is_none());
        st.note_error("market:1778950800", "bitcoin-up-or-down-may-16-2026-1pm-et: HTTP 429".into());
        assert!(st.last_error.as_deref().unwrap().contains("HTTP 429"));
        // The next pass fetched it: the record exists, so the error is over.
        st.reconcile_market_errors(|w| w == 1778950800);
        assert!(st.last_error.is_none(), "{:?}", st.last_error);
        assert!(st.errors.is_empty());
        // A retry that succeeds directly clears it too.
        st.note_error("market:1778950800", "HTTP 429".into());
        st.clear_error("market:1778950800");
        assert!(st.last_error.is_none());
        // Two markets failing, one heals: the other is still shown.
        st.note_error("market:100", "a: HTTP 429".into());
        st.note_error("market:200", "b: HTTP 503".into());
        st.reconcile_market_errors(|w| w == 200);
        assert_eq!(st.last_error.as_deref(), Some("a: HTTP 429"));
        assert_eq!(st.errors.len(), 1);
        // A persistent funding failure stays until funding succeeds, whatever the markets do.
        st.note_error("funding", "funding history unavailable: HTTP 451".into());
        st.reconcile_market_errors(|_| true);
        assert_eq!(st.last_error.as_deref(), Some("funding history unavailable: HTTP 451"));
        st.clear_error("bars"); // clearing a source that is not failing changes nothing
        assert_eq!(st.last_error.as_deref(), Some("funding history unavailable: HTTP 451"));
        st.clear_error("funding");
        assert!(st.last_error.is_none() && st.errors.is_empty());
        // The card line drops the error once it is gone.
        update_status("testerr", |s| { s.phase = "idle".into(); s.note_error("bars", "Binance bars for 2026-09-13 unavailable: timeout".into()); });
        assert!(status_line("testerr").unwrap().contains("last error: Binance bars"));
        update_status("testerr", |s| s.clear_error("bars"));
        assert_eq!(status_line("testerr").as_deref(), Some("data current"));
    }

    /// Production, 2026-09-13, engine v1.1.6: a t3.medium (the template's default and
    /// smallest type) was refused with "this machine has 3.7 GB of memory and a
    /// training run needs 4 GB; use a t3.medium or larger". The gate compared the
    /// kernel's MemTotal, which is always below the nominal size, against 4 GiB. It
    /// now measures headroom against the fit's peak, so the machine that has room
    /// trains and the reason, when there is one, is true of the machine it describes.
    #[test]
    fn a_t3_medium_trains_and_a_t3_small_is_refused() {
        // The production t3.medium's /proc/meminfo, verbatim, with the engine, Control
        // Tower and nginx running and no container limit (memory.max was "max").
        let meminfo = "MemTotal:        3924236 kB\nMemFree:         2410844 kB\nMemAvailable:    3176132 kB\nBuffers:            2144 kB\nCached:           880412 kB\n";
        let (total, available) = parse_meminfo(meminfo);
        assert_eq!(total, Some(4_018_417_664), "MemTotal matches `free -b`");
        assert_eq!(available, Some(3_252_359_168));
        let medium = MemoryReading { total, available, limit: None, used: None };
        assert_eq!(medium.headroom(), available);
        assert_eq!(medium.capacity(), total);
        assert_eq!(memory_refusal(&medium), None, "a t3.medium has 3.0 GB free: room for a 2 GB fit");

        // A t3.small: 1.9 GB reported, about 1.1 GB free beside the same stack.
        let gib = 1024u64 * 1024 * 1024;
        let small = MemoryReading { total: Some(2_002_000 * 1024), available: Some(1_150_000 * 1024), limit: None, used: None };
        let why = memory_refusal(&small).expect("a t3.small is refused");
        assert!(why.starts_with("training refused: a fit needs 2.0 GB free beside the engine"), "{why}");
        assert!(why.contains("1.1 GB free of 1.9 GB"), "{why}");
        assert!(why.contains("use a t3.medium (4 GB) or larger"), "{why}");
        // The type it recommends is one it would not refuse.
        assert!(memory_refusal(&medium).is_none());

        // A t3.medium that something else has filled (a local LLM, say) is refused
        // for the real reason and is not told to buy the instance it is already on.
        let busy = MemoryReading { total, available: Some(gib + gib / 2), limit: None, used: None };
        let why = memory_refusal(&busy).expect("1.5 GB free is refused");
        assert!(why.contains("1.5 GB free of 3.7 GB") && why.contains("something else is using the memory"), "{why}");
        assert!(!why.contains("t3.medium"), "{why}");

        // The boundary is the fit's peak, on headroom alone.
        let at = |free: u64| MemoryReading { total, available: Some(free), limit: None, used: None };
        assert!(memory_refusal(&at(2 * gib - 1)).is_some());
        assert!(memory_refusal(&at(2 * gib)).is_none());
        // No MemAvailable (an old kernel, a Mac): the total stands in for it.
        assert!(memory_refusal(&MemoryReading { total, ..Default::default() }).is_none());
        assert!(memory_refusal(&MemoryReading { total: Some(1900 * 1024 * 1024), ..Default::default() }).is_some());
        // Nothing readable at all does not block training.
        assert_eq!(memory_refusal(&MemoryReading::default()), None);
        // This box reports something sensible (printed so a run under a container
        // limit shows what the kernel and cgroup gave it).
        let here = read_memory();
        eprintln!("this box: {here:?}, headroom {:?}, refusal {:?}", here.headroom(), memory_refusal(&here));
        assert!(here.total.is_none_or(|b| b > 256 * 1024 * 1024), "{here:?}");
        assert!(here.headroom().is_none_or(|b| b > 0), "{here:?}");
        // The card shows the reason verbatim.
        update_status("testmem", |s| { s.phase = "memory_too_small".into(); s.memory_refusal = Some(why.clone()); });
        assert_eq!(status_line("testmem").as_deref(), Some(why.as_str()));
    }

    /// A container memory limit is respected: what counts is the room left under it,
    /// and the reason names the limit, not the machine.
    #[test]
    fn a_container_limit_caps_the_headroom() {
        let gib = 1024u64 * 1024 * 1024;
        let host = (Some(16 * gib), Some(14 * gib));
        // A 3 GB limit with 1.5 GB already used leaves 1.5 GB: refused, naming the limit.
        let tight = MemoryReading { total: host.0, available: host.1, limit: Some(3 * gib), used: Some(gib + gib / 2) };
        assert_eq!(tight.headroom(), Some(gib + gib / 2));
        assert_eq!(tight.capacity(), Some(3 * gib), "memory_bytes reports the limit when it is the smaller");
        let why = memory_refusal(&tight).expect("refused");
        assert!(why.contains("the container's memory limit of 3.0 GB leaves 1.5 GB"), "{why}");
        assert!(why.contains("raise the container's memory limit") && !why.contains("t3.medium"), "{why}");
        // The same limit with 0.8 GB used leaves 2.2 GB: trains.
        let roomy = MemoryReading { limit: Some(3 * gib), used: Some(800 * 1024 * 1024), ..tight };
        assert_eq!(memory_refusal(&roomy), None);
        // A limit whose usage cannot be read counts in full, and a limit below the
        // fit's peak refuses however empty the machine is.
        assert_eq!(memory_refusal(&MemoryReading { limit: Some(3 * gib), used: None, ..tight }), None);
        assert!(memory_refusal(&MemoryReading { limit: Some(gib), used: None, ..tight }).is_some());
        // A limit larger than the machine's room changes nothing: the machine binds.
        let medium = MemoryReading { total: Some(4_018_417_664), available: Some(3_252_359_168), limit: Some(8 * gib), used: Some(gib) };
        assert_eq!(medium.headroom(), Some(3_252_359_168));
        assert_eq!(medium.capacity(), Some(4_018_417_664));
        assert_eq!(memory_refusal(&medium), None);
        // The cgroup's page cache is reclaimable and must not count as used: a v2
        // memory.stat's `file` (and a v1 `total_cache`) is what read_memory subtracts.
        let v2 = "anon 734003200\nfile 2147483648\nkernel 12345\nfile_mapped 1024\n";
        assert_eq!(parse_cgroup_stat(v2, "file"), Some(2_147_483_648));
        assert_eq!(parse_cgroup_stat(v2, "file_mapped"), Some(1024), "exact key, not a prefix");
        assert_eq!(parse_cgroup_stat("cache 1\ntotal_cache 99\n", "total_cache"), Some(99));
        assert_eq!(parse_cgroup_stat(v2, "swap"), None);
    }

    #[test]
    fn slugs_carry_the_year_and_eastern_time() {
        // 2026-09-12 15:00 UTC is 11 AM EDT.
        assert_eq!(slug_for(1789225200), "bitcoin-up-or-down-september-12-2026-11am-et");
        // 2026-03-01 15:00 UTC is 10 AM EST.
        assert_eq!(slug_for(1772377200), "bitcoin-up-or-down-march-1-2026-10am-et");
        // Midnight and noon.
        assert_eq!(slug_for(1780286400), "bitcoin-up-or-down-june-1-2026-12am-et");
        assert_eq!(slug_for(1780329600), "bitcoin-up-or-down-june-1-2026-12pm-et");
    }

    #[test]
    fn a_record_resolves_only_on_a_zero_one_price_pair() {
        let mut r = MarketRecord { outcome_prices: Some(r#"["1", "0"]"#.into()), ..Default::default() };
        assert_eq!(r.up_won(), Some(true));
        r.outcome_prices = Some(r#"["0", "1"]"#.into());
        assert_eq!(r.up_won(), Some(false));
        r.outcome_prices = Some(r#"["0.5", "0.5"]"#.into());
        assert_eq!(r.up_won(), None);
        r.outcome_prices = None;
        assert_eq!(r.up_won(), None);
    }

    #[test]
    fn the_status_line_reports_progress_and_the_last_decision() {
        update_status("teststatus", |s| { s.phase = "backfilling".into(); s.backfill_done = 120; s.backfill_total = 480; });
        assert_eq!(status_line("teststatus").as_deref(), Some("backfilling training data: 120 of 480 markets (25%)"));
        update_status("teststatus", |s| {
            s.phase = "idle".into();
            s.last_cycle = Some(CycleReport { decision: "rejected".into(), detail: "candidate failed the holdout gate: 12 holdout trades, fewer than the 30 required".into(), finished_at: "2026-09-14T03:00:00Z".into(), ..Default::default() });
        });
        let line = status_line("teststatus").unwrap();
        assert!(line.starts_with("data current | last cycle 2026-09-13 23:00 ET (rejected): candidate failed"), "{line}");
    }
}
