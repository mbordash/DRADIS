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

//! GBoost plan B: a calibrated gradient-boosted model, scored here, trained by the
//! engine (2026-09-13).
//!
//! The `GboostStrategy` viper. It succeeded a self-retraining classifier that
//! learned "is BTC higher in three minutes" from order-book snapshots and never
//! showed a tradeable edge; that code is gone. This model is a perpetual booster
//! with Platt calibration that predicts whether a trade entered at the ask reaches a
//! 20% resting take-profit before an 11% stop. The first production model was fit
//! offline on June 1 to September 10 2026 BTC hourly markets and validated on a
//! walk-forward holdout of markets the fit never saw, where the configuration
//! returned +3.4% per trade. Since then the engine trains its own: the pipeline in
//! `gboost_planb_train` backfills the same public data on every instance, fits,
//! calibrates and validates a candidate on a held-out fold, and writes it into the
//! serving file only when it passes the gate and does no worse than the model
//! already there. This file only scores: it loads whatever model file is in place,
//! and the exporter parity test below pins the scoring to the offline exporter's own.
//!
//! # Parity with the harness
//!
//! The model is only valid on inputs built exactly the way its training rows were
//! (`build_holdout.py`, and now `gboost_planb_train::build_market_rows`, which calls
//! the `build_features` below). So the features come from 1-minute Binance klines
//! fetched at each decision minute, not from the price raptor's ticker; realized
//! volatility uses the harness's own 4.2e-5 floor, not FairValue's knob; the funding
//! rate is the last SETTLED rate from Binance's history endpoint, not the funding
//! raptor's value (which can silently fall back to OKX); and columns the model's
//! training rows never had a value for (`zeroed_features` in its stamp: always the
//! four futures features, and funding on an instance no funding source answers) are
//! fed as zeros, as the evaluator fed them. `build_features` is tested against rows
//! the harness produced.
//!
//! # What a model file must say about itself
//!
//! Beyond the layout, input names and calibration, the loader reads the venue the
//! model was trained for and refuses another venue's; the plan its labels were built
//! with (`plan_tp`, `plan_sl`, `plan_min_ask`, `plan_max_ask`, or the first model's
//! `label=B_aggr20`), which the viper compares with the configured plan and idles on
//! a mismatch until the pipeline retrains; and its provenance, shown on the card.
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

use std::collections::{HashMap, HashSet};
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

pub const STRATEGY_NAME: &str = "GboostStrategy";

/// The venue every plan-B model is trained for. Polymarket International's hourly
/// markets, prices, prints and fees are what the rows describe; a model stamped for
/// another venue is refused, and an unstamped one (the first production model) is
/// taken to be this venue's, which it was.
pub const TRAINED_VENUE: &str = "polymarket-intl";

/// Column index of `funding`, the one input whose availability differs by instance.
const FUNDING_COLUMN: usize = 22;
/// `oi_d5`, `oi_d30`, `tlsr`, `tlsr15` in `FEATURE_NAMES`.
pub const DERIV_COLUMNS: [usize; 4] = [23, 24, 25, 26];
/// The first production model's `label` stamp and the plan it was built with.
const REFERENCE_LABEL: &str = "B_aggr20";
const REFERENCE_PLAN: [f64; 4] = [0.20, 0.11, 0.43, 0.75];
/// How often the pipeline's status line is refreshed on the card.
const DETAIL_REFRESH_SECS: u64 = 2;
/// How often one holding reason is written to the log (the card refreshes live).
const GATE_LOG_INTERVAL_SECS: u64 = 300;

/// Fewest trees a loaded model file may have. A calibrated export of a real fit
/// has dozens; a file with fewer is a nothing-fit or a broken export, and is
/// refused at load rather than scored. Mechanism, not risk appetite, so it is
/// not a profile constant.
const STRUCTURAL_MIN_TREES: usize = 5;

/// Oldest order-book snapshot a decision may be scored, sized and priced on.
const MAX_SNAPSHOT_AGE_SECS: i64 = 10;

/// Seconds after a minute boundary before that minute is scored.
///
/// The history point a decision uses is the last sample at or before the minute,
/// and samples land a few seconds into a minute (mode :03 to :06, tail to :19), so
/// the point is around a minute old and publication lag (2 to 7 s) almost never
/// decides which point is chosen: 0.027% of samples fall in a minute's last 7 s
/// (measured over 360k samples in the production store). Waiting longer only
/// let the ask drift away from the price the model scored ([B48]), so the decision
/// is taken as soon as the closed bar is available, as it was before [B46].
const HISTORY_SETTLE_SECS: i64 = BAR_SETTLE_SECS;
/// History fetched per decision: enough before the minute for the 5-minute mid
/// and its staleness limit.
const HISTORY_LOOKBACK_SECS: i64 = 900;

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

/// What the shadow lane has recorded for the model currently in service.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ShadowStats {
    pub trades: usize,
    pub mean_ret: f64,
    /// Lower bound of the 90% market-bootstrap interval on mean return.
    pub lower_bound: f64,
    pub win: f64,
}

/// Which exit the plan takes for an open shadow trade right now, if any.
///
/// Pure, and deliberately the labeler's rule (`gboost_planb_train`, the
/// `ExitKind` loop) rather than a second opinion about how the plan behaves: the
/// shadow record is only evidence about the plan if it simulates the same plan
/// the holdout measured. Two differences from the labeler, both because live
/// prices are better than reconstructed ones:
///
/// * the sale price is the real best bid, where the labeler had to approximate a
///   sale from a mid by subtracting half a spread;
/// * the stop sells at that bid rather than at the mid that triggered it.
///
/// The stop is checked before the take-profit, as the labeler checks it, so a
/// tick that crosses both is scored the conservative way.
///
/// Three divergences remain, named here rather than discovered later:
///
/// * the lane samples the book every `SHADOW_SWEEP_SECS` where the labeler read
///   every print and every history mid, so a brief touch of either level is
///   missed — in both directions, so roughly symmetric;
/// * a squadron rotates ten minutes before its market closes, after which the
///   lane has no book for the old tokens and a position can only end at
///   settlement, where the labeler scored stops and take-profits through to the
///   close;
/// * the record therefore measures the PLAN the holdout measured, which is the
///   point, and not the live exit posture: at the default `ExitPosture::Gates`
///   the live viper flattens at the bid before rotation instead of holding to
///   settlement. A promotion licenses the plan on this instance's evidence; it
///   is not evidence about the posture.
pub fn shadow_exit(
    entry: f64, tp_price: f64, stop_price: f64, bid: Option<f64>, resolved: Option<f64>,
) -> Option<(f64, &'static str)> {
    // A bid of zero is not a price, it is the absence of one: an empty bid side
    // is published as $0.00 so that a dark feed reads as a market nobody is
    // bidding on rather than as an error (`state.rs`, `snapshot_has_book`).
    // Without this guard a websocket resync, or a thin losing side with no bids,
    // books a shadow trade at $0.00 — a total loss the live viper would never
    // take, because `exit_action` requires `bid > 0` before it will stop. One
    // such row drags the bootstrap lower bound down for the rest of a 40-trade
    // record, so it would corrupt the very evidence the promotion rests on.
    if let Some(b) = bid.filter(|b| *b > 0.0) {
        if b <= stop_price {
            return Some((b, "stop"));
        }
        // A take-profit above a dollar can never fill, and one at or below the
        // entry is not a profit; the labeler calls this `tp_possible`.
        if tp_price < 1.0 && tp_price > entry && b >= tp_price {
            return Some((tp_price, "take-profit"));
        }
    }
    resolved.map(|r| (r, "settlement"))
}

/// What one closed shadow trade returned, per unit staked.
///
/// The plan's arithmetic: the entry fee was paid on the way in, an exit fee is
/// paid only when the stop sells into the book, and a take-profit that rests is
/// a maker sale that pays none. A settlement pays no exit fee either, because
/// nothing is sold — the position redeems.
pub fn shadow_ret(entry: f64, entry_fee: f64, exit_price: f64, reason: &str, fee_rate: f64) -> f64 {
    let exit_fee = if reason == "stop" {
        crate::vipers::gboost_planb_train::fee_per_share(fee_rate, exit_price)
    } else {
        0.0
    };
    if entry <= 0.0 { return 0.0; }
    (exit_price - entry - entry_fee - exit_fee) / entry
}

/// Where the plan's take-profit and stop sit for an entry at `ask`.
pub fn shadow_levels(ask: f64, tp: f64, sl: f64, tp_ceiling: f64) -> (f64, f64) {
    (
        crate::vipers::gboost_planb_train::ceil_tick(ask * (1.0 + tp)).min(tp_ceiling),
        ask * (1.0 - sl),
    )
}

/// The shadow lane's record for one model, from its closed simulated trades.
///
/// `returns` is `(hourly window, net return per unit staked)`, one entry per
/// closed shadow trade. The interval is a MARKET bootstrap, resampling hourly
/// markets rather than rows, because the pre-registration's bar is stated that
/// way and for the reason it is stated that way: two entries on the same hourly
/// market are one market's worth of evidence. Resampling rows would report a
/// tighter interval than the evidence supports and promote the lane early,
/// which is the one error this gate exists to prevent.
///
/// The estimator is deliberately the same one `rule_stats` uses for the holdout
/// gate — same resample count, same deterministic generator, same percentile —
/// so "lower bound above zero" means the identical thing in the gate and in the
/// promotion, rather than two bars that merely share a name.
pub fn shadow_stats(returns: &[(i64, f64)]) -> ShadowStats {
    use crate::vipers::gboost_planb_train::{percentile, SplitMix, BOOTSTRAP_RESAMPLES};
    if returns.is_empty() {
        return ShadowStats::default();
    }
    let rets: Vec<f64> = returns.iter().map(|(_, r)| *r).collect();
    let mut by_market: std::collections::BTreeMap<i64, Vec<f64>> = std::collections::BTreeMap::new();
    for (w, r) in returns { by_market.entry(*w).or_default().push(*r); }
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
    ShadowStats {
        trades: rets.len(),
        mean_ret: rets.iter().sum::<f64>() / rets.len() as f64,
        lower_bound: percentile(&boots, 5.0),
        win: rets.iter().filter(|r| **r > 0.0).count() as f64 / rets.len() as f64,
    }
}

/// Why this instance is not yet spending real money on GBoost, or `None` when
/// it has earned the right to.
///
/// Both conditions must hold: the model cleared the holdout gate when it was
/// trained, and the shadow lane's own out-of-sample record on THIS instance
/// cleared the bar the pre-registration set for a live trial. The gate speaks
/// for the model, the record speaks for the instance, and neither alone is
/// evidence that this operator's box should be buying.
///
/// The bar is the pre-registration's, not a new one: at least
/// `min_trades` entries, mean return above zero, the 90% bootstrap lower bound
/// above zero, and a win rate at or above `min_win`. Returning the reason
/// rather than a bool is deliberate — the card and the log have to be able to
/// say which number is missing, or an operator cannot tell a viper that is
/// working from one that is stuck.
pub fn shadow_blocks_live(
    gate_passed: bool,
    rec: &ShadowStats,
    min_trades: usize,
    min_win: f64,
) -> Option<String> {
    if !gate_passed {
        return Some("the model has not cleared the holdout gate".to_string());
    }
    if rec.trades < min_trades {
        return Some(format!("shadow record {} of {} trades", rec.trades, min_trades));
    }
    if !(rec.mean_ret > 0.0) {
        return Some(format!("shadow mean return {:+.2}% is not above zero", rec.mean_ret * 100.0));
    }
    if !(rec.lower_bound > 0.0) {
        return Some(format!("shadow 90% lower bound {:+.2}% is not above zero", rec.lower_bound * 100.0));
    }
    if rec.win < min_win {
        return Some(format!("shadow win rate {:.3} is below the {min_win:.2} bar", rec.win));
    }
    None
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
    /// Last settled funding rate at or before `t`; `None` is a missing input.
    pub funding: Option<f64>,
    /// The four derivatives columns at `t`, from [`deriv_features_at`]; `None` is a
    /// missing input, as for funding.
    pub oi_d5: Option<f64>,
    pub oi_d30: Option<f64>,
    pub tlsr: Option<f64>,
    pub tlsr15: Option<f64>,
}

/// Binance futures-data series in the live API's stamp convention, oldest first:
/// `(timestamp_secs, value)`. One shape for open interest (`sumOpenInterest`) and
/// the taker buy/sell volume ratio (`buySellRatio`).
pub type DerivSeries = Vec<(i64, f64)>;

/// A `takerlongshortRatio` row stamped `T` aggregates the flow of `[T, T + 5m)`, so
/// the last bucket a decision at `t` may read is the one stamped at or before
/// `t - TLSR_BUCKET_SECS`: the last bucket COMPLETED by `t`.
///
/// This is the lookahead leak the research found on 2026-09-10 and corrected on
/// 09-12 (`build_holdout.py:142`): reading the bucket stamped at or before `t` let
/// the feature see taker aggression up to four minutes after the decision, and
/// Spearman(side-signed log tlsr, label) fell from +0.320 to +0.035 once the
/// completed bucket was used. The rule lives here, in the one function both the
/// trainer and the viper call, so neither can drift from it.
pub const TLSR_BUCKET_SECS: i64 = 300;

/// The archive's `create_time` for open interest is the live API's stamp minus this:
/// check 3 of the 2026-09-12 pre-registration matched `sum_open_interest` to the
/// API on 8,352 of 8,352 overlapping buckets with the archive stamp shifted +300 s.
/// The taker ratio's archive stamp already matches the API (offset 0).
pub const OI_ARCHIVE_STAMP_SHIFT_SECS: i64 = 300;

/// The four derivatives columns at decision time `t`, computed exactly as the
/// research harness computes them (`gboost-phase1-2026-09-10/gb/build_dataset.py`
/// lines 139-143, with the 09-12 `tlsr` correction from `build_holdout.py:142`):
///
/// * `oi_d5  = ln(oi_now / oi[io - 1])`, `oi_d30 = ln(oi_now / oi[io - 6])`: log
///   ratios one and six BUCKETS back from the newest open-interest sample at or
///   before `t` (index-based, as the harness; a gap in the series widens the
///   lookback rather than shifting it).
/// * `tlsr = buySellRatio` of the last bucket completed by `t` (stamp at or before
///   `t - TLSR_BUCKET_SECS`).
/// * `tlsr15 = mean of that bucket and the two before it` (`np.mean(tl_v[it-2:it+1])`),
///   NOT the value fifteen minutes ago (`DESIGN_NOTES.md:36`).
///
/// Every column is `None` when the series does not reach: a missing input, never a
/// zero, so `dead_columns` can see it.
pub fn deriv_features_at(oi: &[(i64, f64)], tlsr: &[(i64, f64)], t: i64) -> DerivFeatures {
    fn last_at(series: &[(i64, f64)], t: i64) -> Option<usize> {
        let i = series.partition_point(|(ts, _)| *ts <= t);
        (i > 0).then(|| i - 1)
    }
    let log_ratio = |now: f64, then: f64| (now > 0.0 && then > 0.0).then(|| (now / then).ln());
    let (mut oi_d5, mut oi_d30) = (None, None);
    if let Some(io) = last_at(oi, t) {
        let now = oi[io].1;
        if io >= 1 { oi_d5 = log_ratio(now, oi[io - 1].1); }
        if io >= 6 { oi_d30 = log_ratio(now, oi[io - 6].1); }
    }
    let (mut tl_now, mut tl_15) = (None, None);
    if let Some(it) = last_at(tlsr, t - TLSR_BUCKET_SECS) {
        tl_now = Some(tlsr[it].1);
        if it >= 2 {
            tl_15 = Some((tlsr[it - 2].1 + tlsr[it - 1].1 + tlsr[it].1) / 3.0);
        }
    }
    DerivFeatures { oi_d5, oi_d30, tlsr: tl_now, tlsr15: tl_15 }
}

/// The four derivatives columns, or the ones the series could not supply.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DerivFeatures {
    pub oi_d5: Option<f64>,
    pub oi_d30: Option<f64>,
    pub tlsr: Option<f64>,
    pub tlsr15: Option<f64>,
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
            inp.funding.unwrap_or(f64::NAN),
            // oi_d5, oi_d30, tlsr, tlsr15: from `deriv_features_at`, `NaN` when the
            // series did not reach. Until 2026-09-22 the engine could not compute
            // them and every model stamped them zeroed; a model now keeps them live
            // only when the training rows carried them end to end (`partial_columns`).
            inp.oi_d5.unwrap_or(f64::NAN), inp.oi_d30.unwrap_or(f64::NAN),
            inp.tlsr.unwrap_or(f64::NAN), inp.tlsr15.unwrap_or(f64::NAN),
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
///
/// With `resting` on, the take-profit IS the resting ask: the plan this model
/// was trained on exits as a maker at the target, and no taker take-profit
/// ever fires. A bid at or through the target does not change that. Either
/// the ask has already been lifted (the venue crossed it on the way up, and
/// the engine's fill sweep books the lift at the exact resting price), or it
/// never rested; in both cases a taker sell here would pull a filled ask and
/// send a sell for shares that are gone. 2026-09-13 08:37 ET, real money: the
/// $0.59 ask filled as the book gapped to $0.60, the taker exit fired on the
/// same tick, the venue rejected it, and the ledger lost the trade. So above
/// the target the ask simply re-posts one tick over the bid, where a post-only
/// order can rest; the engine leaves an existing ask alone within its reprice
/// deadband and pulls-and-books it otherwise.
pub fn exit_action(entry: Decimal, bid: Decimal, tp: Decimal, sl: Decimal, ceiling: Decimal, resting: bool) -> ExitAction {
    if entry <= Decimal::ZERO {
        return ExitAction::Hold;
    }
    if bid > Decimal::ZERO && bid <= entry * (Decimal::ONE - sl) {
        return ExitAction::Stop;
    }
    let target = take_profit_target(entry, tp, ceiling);
    let tp_possible = target < Decimal::ONE && target > entry;
    if !tp_possible {
        return ExitAction::Hold;
    }
    if resting {
        let ask = if bid >= target { bid + TICK } else { target };
        return if ask < Decimal::ONE { ExitAction::Rest(ask) } else { ExitAction::Hold };
    }
    if bid >= target {
        return ExitAction::TakeProfit;
    }
    ExitAction::Hold
}

/// One price tick on the venue; a post-only ask must sit at least this far
/// above the bid.
const TICK: Decimal = dec!(0.01);

/// How a held plan-B position is managed, chosen by the operator in the Control
/// Tower (`gboost_planb_exit_posture`).
///
/// The plan the model is trained on is `Gates`: a resting take-profit at +20% and
/// a taker stop at 11%. `Settlement` is the experimental posture the exit
/// comparison study (`~/dradis-research/gboost-exit-comparison-2026-09-15`) exists
/// to test: hold every entry to the market's resolution instead. On the study's
/// 608 spent entries a hold returned +6.11% per trade against the gates' +3.24%,
/// and did so with a maximum drawdown of $40.83 against $11.01 at $4 stakes, with
/// 36.3% of trades losing the whole stake against 0.8%. That is a different risk
/// profile, not a free improvement, which is why the operator selects it and why
/// the default is unchanged behavior.
///
/// `Split` runs both, assigning each position by `settlement_arm`. Both arms then
/// see identical market conditions, which is what makes the comparison worth
/// anything: the alternative is comparing periods, where the market moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitPosture {
    /// Resting take-profit, taker stop, flatten before the rotation. The default.
    Gates,
    /// No stop, no take-profit, and no rotation flatten: the position resolves
    /// on chain at $1.00 or $0.00.
    Settlement,
    /// Per-position, deterministically: half `Gates`, half `Settlement`.
    Split,
}

impl ExitPosture {
    /// Anything outside the three known values is `Gates`.
    ///
    /// The fallback is deliberate and one-directional. An unrecognized number can
    /// only ever mean the conservative posture, never the experimental one, so a
    /// bad write, a hand-edited config row or a future value from a newer build
    /// cannot silently put real money on the hold plan.
    pub fn from_i64(v: i64) -> Self {
        match v {
            1 => ExitPosture::Settlement,
            2 => ExitPosture::Split,
            _ => ExitPosture::Gates,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            ExitPosture::Gates => "gates",
            ExitPosture::Settlement => "settlement",
            ExitPosture::Split => "split",
        }
    }
}

/// Which arm a position takes under `ExitPosture::Split`: true = hold to settlement.
///
/// Derived from the token id alone, so it is stable for the life of the position.
/// `evaluate_exit` runs every tick and must reach the same answer every time: a
/// position that changed arms between ticks could take a stop and then be held, or
/// be held past the flatten and then stopped, which would be neither arm and would
/// corrupt the comparison it exists to serve. Deriving it from durable identity
/// rather than storing it also keeps the schema unchanged.
///
/// FNV-1a over the token id's bytes, low bit. The hash is only a coin: it needs an
/// even split across unrelated ids, not cryptographic strength.
pub fn settlement_arm(token_id: &str) -> bool {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in token_id.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h & 1 == 1
}

/// Whether this position is held to settlement under `posture`.
pub fn holds_to_settlement(posture: ExitPosture, token_id: &str) -> bool {
    match posture {
        ExitPosture::Gates => false,
        ExitPosture::Settlement => true,
        ExitPosture::Split => settlement_arm(token_id),
    }
}


// ── Model ────────────────────────────────────────────────────────────────────

pub struct PlanBModel {
    booster: PerpetualBooster,
    pub platt_a: f64,
    pub platt_b: f64,
    pub version: String,
    pub trees: usize,
    /// Columns the model's training rows never had a value for, fed as zeros.
    pub zeroed: Vec<usize>,
    /// `[tp, sl, min_ask, max_ask]` the labels were built with, when stamped.
    pub plan: Option<[f64; 4]>,
    /// Which price the labels bought at (`entry_rule` stamp). `None` on a model
    /// written before the stamp existed, which means the old rule ([B47]).
    pub entry_rule: Option<String>,
    pub trained_by: Option<String>,
    pub created_at: Option<String>,
    /// End of the calibration slice: the newest market the model has seen.
    pub calibrated_through: Option<String>,
    pub holdout_summary: Option<String>,
    /// Did this model clear the holdout gate when it was trained?
    ///
    /// A model is now written to the serving path whether or not it passed, so
    /// the viper always has something to score with and is never idle waiting
    /// for one. `false` means it trades the shadow lane only: simulated entries
    /// at the configured size, booked as simulated rows, until the instance's
    /// own record earns real money.
    ///
    /// A model written before the stamp existed has its verdict re-derived in
    /// `load_model` from the holdout numbers it did record. Treating the missing
    /// stamp as a failure would be permanent rather than safe: `shadow_blocks_live`
    /// refuses on a failed gate before it ever looks at the record, so such a
    /// model could never be promoted by its own evidence, however good. One that
    /// recorded no holdout either stays in the shadow lane.
    pub gate_passed: bool,
}

/// The gate's verdict on a model that was written before the verdict was stamped,
/// re-derived from the holdout numbers it did record.
///
/// Reading a missing stamp as "failed" would be permanent, not merely cautious:
/// `shadow_blocks_live` refuses on a failed gate BEFORE it looks at the record,
/// so an unstamped model could never be promoted by its own evidence however
/// good, and the only escape would be the pipeline training a passing
/// replacement — which on an instance whose candidates keep failing never comes.
///
/// Every input the gate weighs was already stamped by the trainer that wrote the
/// file, so this reads the model's own holdout result rather than assuming
/// anything about it. A file that recorded no holdout has produced no evidence
/// and does not pass: `gate` refuses a NaN skill, a zero trade count and a NaN
/// win rate on their own terms.
///
/// The bar is the compile-time one, because this runs in a loader with no
/// DynamicConfig in reach. That only ever applies to pre-stamp files; anything
/// this build trains carries the verdict the operator's own knobs produced.
pub fn gate_from_stamp(
    trees: usize, skill: Option<f64>, trades: Option<usize>, mean_ret: Option<f64>, win: Option<f64>,
) -> bool {
    use crate::vipers::gboost_planb_train::{gate, FoldStats, RuleStats};
    let stats = FoldStats {
        skill: skill.unwrap_or(f64::NAN),
        rule: RuleStats {
            trades: trades.unwrap_or(0),
            mean_ret: mean_ret.unwrap_or(f64::NAN),
            win: win.unwrap_or(f64::NAN),
            ..Default::default()
        },
        ..Default::default()
    };
    gate(
        trees, &stats,
        crate::config::GBOOST_PLANB_GATE_MIN_TRADES.max(0) as usize,
        crate::config::GBOOST_PLANB_GATE_MIN_WIN_RATE.to_f64().unwrap_or(0.53),
    ).passed
}

/// Load and validate a model file: its layout, its input names, its calibration, its
/// venue and its size must all be what this code feeds it.
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
    if let Some(v) = meta("venue") {
        if v != TRAINED_VENUE {
            return Err(format!("model was trained for venue '{v}', not {TRAINED_VENUE}; plan B trades only Polymarket International"));
        }
    }
    let num = |key: &str| meta(key).and_then(|s| s.parse::<f64>().ok()).filter(|v| v.is_finite());
    let platt_a = num("platt_a").ok_or("model has no usable platt_a")?;
    let platt_b = num("platt_b").ok_or("model has no usable platt_b")?;
    let trees = booster.get_prediction_trees().len();
    if trees < STRUCTURAL_MIN_TREES {
        return Err(format!("model has {trees} trees, below the structural minimum {}", STRUCTURAL_MIN_TREES));
    }
    let version = meta("model_version").unwrap_or_else(|| "unversioned".to_string());
    // The four futures columns are zeroed whatever the stamp says: no engine has ever
    // computed them. Any other zeroed column must be a known input.
    let mut zeroed: Vec<usize> = vec![23, 24, 25, 26];
    for name in meta("zeroed_features").unwrap_or_default().split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let j = FEATURE_NAMES.iter().position(|n| *n == name).ok_or_else(|| format!("model zeroes unknown input '{name}'"))?;
        if !zeroed.contains(&j) { zeroed.push(j); }
    }
    zeroed.sort_unstable();
    let plan = match (num("plan_tp"), num("plan_sl"), num("plan_min_ask"), num("plan_max_ask")) {
        (Some(tp), Some(sl), Some(lo), Some(hi)) => Some([tp, sl, lo, hi]),
        _ if meta("label").as_deref() == Some(REFERENCE_LABEL) => Some(REFERENCE_PLAN),
        _ => None,
    };
    let holdout_summary = match (meta("holdout_trades"), num("holdout_mean_ret"), num("holdout_win")) {
        (Some(n), Some(r), Some(w)) => Some(format!("holdout {n} trades at {:+.2}% per trade, win {w:.3}", r * 100.0)),
        _ => None,
    };
    // Which price the labels bought at. A model without the stamp predates [B47]
    // and was labeled on an entry the book never showed, so it is named as `e1`
    // rather than trusted: `plan_mismatch` refuses whatever does not match the
    // configured rule.
    let entry_rule = Some(meta("entry_rule").unwrap_or_else(|| "e1".to_string()));
    let trained_by = meta("trained_by");
    let created_at = meta("created_at");
    let calibrated_through = meta("calibrated_through");
    // Whether this model is licensed for real money.
    //
    // Normally the trainer's own verdict, stamped into the file. A model written
    // before this build carries no stamp, and reading that absence as "failed"
    // would strand it in the shadow lane permanently: `shadow_blocks_live`
    // refuses on a failed gate BEFORE it looks at the record, so no amount of
    // good evidence on this instance could ever release it. The only escape
    // would be the pipeline training a passing replacement, which on a box whose
    // candidates keep failing never comes.
    //
    // So an unstamped file is judged on the evidence it DID record. Every input
    // the gate weighs — trees, log-loss skill, holdout trades, mean return, win
    // rate — was already stamped by the trainer that wrote it, and re-deriving
    // the verdict from those numbers is reading the model's own holdout result,
    // not assuming anything about it. A file with no holdout stamp either (an
    // externally provided model) has recorded no evidence at all and stays in
    // the shadow lane, which is the right answer for a model nobody can check.
    //
    // The bar used here is the compile-time one rather than the operator's
    // current knob, because this runs in a loader that has no DynamicConfig. It
    // applies only to pre-stamp files; anything this build trains carries the
    // verdict the operator's own knobs produced.
    let gate_passed = match meta("gate_passed").as_deref() {
        Some(v) => v == "true",
        None => gate_from_stamp(
            trees,
            num("holdout_skill"),
            meta("holdout_trades").and_then(|s| s.parse::<usize>().ok()),
            num("holdout_mean_ret"),
            num("holdout_win"),
        ),
    };
    Ok(PlanBModel { booster, platt_a, platt_b, version, trees, zeroed, plan, entry_rule, trained_by, created_at, calibrated_through, holdout_summary, gate_passed })
}

impl PlanBModel {
    /// Whether a decision needs the settled funding rate, or the model zeroes it.
    pub fn needs_funding(&self) -> bool { !self.zeroed.contains(&FUNDING_COLUMN) }

    /// Whether a decision needs the derivatives series (open interest and the taker
    /// ratio), or the model zeroes all four of their columns.
    pub fn needs_derivs(&self) -> bool { DERIV_COLUMNS.iter().any(|c| !self.zeroed.contains(c)) }

    /// The plan the model was built for differs from the configured one.
    ///
    /// The entry rule is part of the plan: a model labeled on the old entry price
    /// answers a different question from the one the engine now trades, and serving
    /// it would keep the [B47] artifact alive. An unstamped model is `e1`.
    pub fn plan_mismatch(&self, tp: f64, sl: f64, lo: f64, hi: f64, entry_rule: &str) -> Option<String> {
        if self.entry_rule.as_deref().unwrap_or("e1") != entry_rule {
            return Some(format!(
                "model {} was labeled with entry rule {}, and the engine trades {}; a model for the configured rule is needed",
                self.version, self.entry_rule.as_deref().unwrap_or("e1"), entry_rule,
            ));
        }
        let p = self.plan?;
        let same = |a: f64, b: f64| (a - b).abs() < 1e-9;
        if same(p[0], tp) && same(p[1], sl) && same(p[2], lo) && same(p[3], hi) {
            None
        } else {
            Some(format!(
                "model {} was labeled for TP {:.0}%, SL {:.0}%, asks ${:.2} to ${:.2}; the configured plan is TP {:.0}%, SL {:.0}%, asks ${:.2} to ${:.2}",
                self.version, p[0] * 100.0, p[1] * 100.0, p[2], p[3], tp * 100.0, sl * 100.0, lo, hi,
            ))
        }
    }

    /// `(raw, calibrated)` for each row, with the model's zeroed columns applied.
    pub fn predict(&self, rows: &[[f64; N_FEATURES]]) -> Vec<(f64, f64)> {
        let n = rows.len();
        let mut data = vec![0.0f64; n * N_FEATURES];
        for (i, row) in rows.iter().enumerate() {
            for (j, v) in row.iter().enumerate() {
                data[j * n + i] = if self.zeroed.contains(&j) { 0.0 } else { *v };
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
    /// The newest open-interest and taker-ratio buckets, in the API's stamp
    /// convention, refreshed on the funding cadence from the same host.
    derivs: Option<(DerivSeries, DerivSeries)>,
    derivs_fetched: Option<Instant>,
    derivs_fetching: bool,
    last_warn: Option<Instant>,
}

/// The CLOB price history of both tokens for one (market, decision minute).
#[derive(Default)]
struct HistorySlot {
    /// `(condition id, decision minute)` the series were fetched for.
    for_key: Option<(String, i64)>,
    series: [Vec<(i64, f64)>; 2],
    fetching: Option<(String, i64)>,
}

#[derive(Default)]
struct PlanBGlobals {
    model: StdMutex<ModelSlot>,
    data: StdMutex<MinuteData>,
    history: StdMutex<HistorySlot>,
    decided: StdMutex<HashSet<(String, i64)>>,
    attempted: StdMutex<HashSet<(String, usize)>>,
    below_minimum_noted: StdMutex<HashSet<String>>,
    detail_refreshed: StdMutex<Option<Instant>>,
    /// Last time the shadow lane's open trades were swept for exits.
    shadow_swept: StdMutex<Option<Instant>>,
    /// Last resolution probe per condition, so a shadow position waiting on a
    /// settlement does not ask the venue on every sweep.
    shadow_probed: StdMutex<HashMap<String, Instant>>,
    /// Resolutions the spawned probes have brought back, by token, waiting to be
    /// applied by a later sweep.
    shadow_resolved: StdMutex<HashMap<String, f64>>,
    /// The promotion record for the serving model, and when it was read. Cached
    /// because it changes only when a shadow trade closes, and the entry path
    /// consults it every decision minute.
    shadow_record: StdMutex<Option<(String, ShadowStats, Instant)>>,
    /// Model versions whose lane state has been announced, so the promotion and
    /// demotion lines are logged once each rather than every tick.
    shadow_announced: StdMutex<HashSet<String>>,
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

/// The serving model file: what the viper loads and what the pipeline adopts into.
pub fn model_path(asset: &str) -> PathBuf {
    PathBuf::from(format!("logs/{}-{}", asset.to_ascii_lowercase(), config::GBOOST_PLANB_MODEL_FILENAME))
}

/// The card's detail line: which model is serving and where it came from, then what
/// the training pipeline is doing.
/// How the card names the lane the viper is in, and what is still missing.
///
/// An operator looking at a GBoost card has exactly one question — is this
/// spending my money, and if not, why not — so the line answers it with the
/// numbers rather than a status word. A viper that is working towards a
/// promotion and one that is stuck look identical without them.
pub fn lane_line(gate_passed: bool, rec: &ShadowStats, min_trades: usize, min_win: f64) -> String {
    let numbers = format!(
        "record {} trades, {:+.2}% per trade, 90% lower bound {:+.2}%, win {:.3}",
        rec.trades, rec.mean_ret * 100.0, rec.lower_bound * 100.0, rec.win,
    );
    match shadow_blocks_live(gate_passed, rec, min_trades, min_win) {
        Some(why) => format!("SHADOW — trading simulated, not real money: {why} ({numbers})"),
        None => format!("LIVE — the gate passed and this instance's own record earned it ({numbers})"),
    }
}

fn detail_line(model: Option<&PlanBModel>, asset: &str, lane: Option<String>) -> String {
    let serving = match model {
        Some(m) => {
            let by = match m.trained_by.as_deref() {
                Some("dradis-engine") => "trained by this engine",
                Some(other) => other,
                None => "provided model",
            };
            let when = m.created_at.as_deref().map(fmt_et).map(|s| format!(", {s}")).unwrap_or_default();
            let holdout = m.holdout_summary.as_deref().map(|h| format!("; {h}")).unwrap_or_default();
            format!("serving {} ({} trees, {by}{when}{holdout})", m.version, m.trees)
        }
        None => "no model in service".to_string(),
    };
    let serving = match lane {
        Some(l) => format!("{l} | {serving}"),
        None => serving,
    };
    match crate::vipers::gboost_planb_train::status_line(asset) {
        Some(p) => format!("{serving} | {p}"),
        None => serving,
    }
}

fn fmt_et(rfc: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(rfc)
        .map(|d| d.with_timezone(&chrono_tz::America::New_York).format("%Y-%m-%d %H:%M ET").to_string())
        .unwrap_or_else(|_| rfc.to_string())
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
                    warn!("GBoost plan-B: model file {} not found; the viper stays idle until the training pipeline adopts one", path.display());
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
                                "GBoost plan-B: loaded model {} from {} ({} trees, Platt a={:.6} b={:.6}, zeroed {:?}, trained by {}, plan {:?})",
                                model.version, path.display(), model.trees, model.platt_a, model.platt_b,
                                model.zeroed.iter().map(|&j| FEATURE_NAMES[j]).collect::<Vec<_>>(),
                                model.trained_by.as_deref().unwrap_or("unknown"), model.plan,
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
    // The data mirror is Binance's official public market-data host; it serves the same
    // klines and answers where the main host is blocked (HTTP 451 from the US).
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
    // fapi is the only official source of the settled rate and answers 451 from the US.
    // There is no fallback by design: where it does not answer, the training pipeline
    // sees no funding either, trains a model that zeroes the column, and this viper
    // serves that model without waiting for a rate (`PlanBModel::needs_funding`).
    parse_funding(&get_json("https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=1").await?)
}

/// One page of a Binance futures-data series, oldest first, in the API's stamps.
pub fn parse_deriv_series(v: &serde_json::Value, field: &str) -> DerivSeries {
    let mut out: DerivSeries = v.as_array().map(|rows| rows.iter().filter_map(|r| {
        let t = r["timestamp"].as_i64()?;
        let x = r[field].as_f64().or_else(|| r[field].as_str().and_then(|s| s.parse().ok()))?;
        Some((t / 1000, x))
    }).collect()).unwrap_or_default();
    out.sort_by_key(|x| x.0);
    out.dedup_by_key(|x| x.0);
    out
}

/// The newest buckets of open interest and the taker buy/sell volume ratio.
///
/// `deriv_features_at` looks back six open-interest buckets and three completed
/// taker buckets, so thirty 5-minute rows of each is ample. Same host as funding
/// and no mirror, because these endpoints exist only on `fapi`; an instance it
/// refuses (HTTP 451 from US addresses) gets `Err`, and the model it serves has
/// these columns zeroed anyway, because its training rows' newest day could only
/// have come from the same host (`partial_columns`).
async fn fetch_derivs() -> std::result::Result<(DerivSeries, DerivSeries), String> {
    let base = "https://fapi.binance.com/futures/data";
    let oi = parse_deriv_series(
        &get_json(&format!("{base}/openInterestHist?symbol=BTCUSDT&period=5m&limit=30")).await?, "sumOpenInterest");
    let tl = parse_deriv_series(
        &get_json(&format!("{base}/takerlongshortRatio?symbol=BTCUSDT&period=5m&limit=30")).await?, "buySellRatio");
    if oi.is_empty() || tl.is_empty() {
        return Err("empty open interest or taker ratio page".to_string());
    }
    Ok((oi, tl))
}

async fn fetch_price_history(tokens: &[String; 2], t: i64) -> std::result::Result<[Vec<(i64, f64)>; 2], String> {
    let url = |tok: &str| format!(
        "https://clob.polymarket.com/prices-history?market={tok}&startTs={}&endTs={}&fidelity=1",
        t - HISTORY_LOOKBACK_SECS, t + 60,
    );
    // Both tokens at once: a slow endpoint must not spend the decision window twice.
    let (up_url, down_url) = (url(&tokens[0]), url(&tokens[1]));
    let (up, down) = tokio::join!(get_json(&up_url), get_json(&down_url));
    let parse = crate::vipers::gboost_planb_train::parse_price_history;
    Ok([parse(&up?), parse(&down?)])
}

/// Both tokens' price history for decision minute `t` of market `cid`, fetched in
/// the background (a tick has 500 ms) and cached for that minute. `None` until it
/// arrives.
fn ensure_price_history(g: &'static PlanBGlobals, cid: &str, tokens: [String; 2], t: i64) -> Option<[Vec<(i64, f64)>; 2]> {
    let key = (cid.to_string(), t);
    let mut h = lock(&g.history);
    if h.for_key.as_ref() == Some(&key) {
        return Some(h.series.clone());
    }
    if h.fetching.as_ref() != Some(&key) {
        h.fetching = Some(key.clone());
        tokio::spawn(async move {
            // Two attempts: with both tokens fetched together and a 5 s HTTP timeout,
            // the worst case is about 12 s, inside the window between
            // HISTORY_SETTLE_SECS and DECISION_STALE_SECS.
            for _ in 0..2 {
                match fetch_price_history(&tokens, t).await {
                    Ok(series) => {
                        let mut h = lock(&g.history);
                        // Only the fetch still being waited for may fill the slot, so a
                        // late result for an earlier minute cannot replace a newer one.
                        if h.fetching.as_ref() == Some(&key) {
                            h.series = series;
                            h.for_key = Some(key.clone());
                            h.fetching = None;
                        }
                        return;
                    }
                    Err(e) => {
                        let mut d = lock(&g.data);
                        if warn_due(&mut d.last_warn) {
                            warn!("GBoost plan-B: Polymarket price history unavailable ({e})");
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(1500)).await;
            }
            let mut h = lock(&g.history);
            if h.fetching.as_ref() == Some(&key) { h.fetching = None; }
        });
    }
    None
}

/// Closed bars through `t` and the settled funding rate, fetching in the background
/// until both are in hand. A model that zeroes funding does not wait for it.
/// The bars, funding rate and derivatives columns a decision at `t` needs, or `None`
/// while any input the model depends on is still on its way. `need_funding` and
/// `need_derivs` are the model's own stamps: a model that zeroes a column never
/// waits for it.
fn ensure_minute_data(
    g: &'static PlanBGlobals,
    t: i64,
    need_funding: bool,
    need_derivs: bool,
) -> Option<(Vec<Bar>, Option<f64>, DerivFeatures)> {
    let mut d = lock(&g.data);
    let derivs_due = need_derivs
        && !d.derivs_fetching
        && d.derivs_fetched.is_none_or(|x| x.elapsed() >= Duration::from_secs(FUNDING_REFRESH_SECS));
    if derivs_due {
        d.derivs_fetching = true;
        tokio::spawn(async move {
            let result = fetch_derivs().await;
            let mut d = lock(&g.data);
            d.derivs_fetching = false;
            match result {
                Ok(series) => {
                    d.derivs = Some(series);
                    d.derivs_fetched = Some(Instant::now());
                }
                Err(e) => {
                    d.derivs_fetched = Instant::now().checked_sub(Duration::from_secs(FUNDING_REFRESH_SECS - 30));
                    if warn_due(&mut d.last_warn) {
                        warn!("GBoost plan-B: open interest and taker ratio unavailable ({e}); decisions wait for them");
                    }
                }
            }
        });
    }
    let funding_due = need_funding
        && !d.funding_fetching
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
    let funding = d.funding.filter(|(_, settled)| *settled <= t).map(|(rate, _)| rate);
    if need_funding && funding.is_none() {
        return None;
    }
    // Same function the trainer scores rows with, on the same shape of series.
    let derivs = d.derivs.as_ref()
        .map(|(oi, tl)| deriv_features_at(oi, tl, t))
        .unwrap_or_default();
    if need_derivs && (derivs.oi_d5.is_none() || derivs.oi_d30.is_none() || derivs.tlsr.is_none() || derivs.tlsr15.is_none()) {
        return None;
    }
    (d.bars_t == Some(t)).then(|| (d.bars.clone(), funding, derivs))
}

/// Why plan B does not trade on this build's venue, or `None` on the venue it was
/// validated for.
///
/// Plan B's rows, labels and fees describe Polymarket International's hourly markets,
/// and its training pipeline fetches that venue's history. No model has been validated
/// on Kalshi's or Polymarket US's markets, so on those builds the viper idles and says
/// so, whatever model file happens to be on disk.
pub fn venue_gate() -> Option<&'static str> {
    #[cfg(feature = "intl_clob")]
    { None }
    #[cfg(not(feature = "intl_clob"))]
    { Some("plan B is validated on Polymarket International BTC hourly markets only; it does not trade on this venue") }
}

fn fmt_features(v: &[f64; N_FEATURES]) -> String {
    let parts: Vec<String> = v.iter().map(|x| if x.is_nan() { "nan".to_string() } else { format!("{x}") }).collect();
    format!("[{}]", parts.join(","))
}

// ── Strategy ─────────────────────────────────────────────────────────────────

/// The venue's taker fee rate as the plan's f64 arithmetic wants it.
fn plan_fee_rate() -> f64 { crate::venues::taker_fee_rate().to_f64().unwrap_or(0.0) }

/// How often the shadow lane sweeps its open trades for exits.
///
/// Not every 50ms tick: the sweep reads the database, and a research lane whose
/// exits are scored to the second would still be scored against a book sampled
/// once a tick. Five seconds is finer than the labeler's own event granularity
/// and costs one small query.
const SHADOW_SWEEP_SECS: u64 = 5;
/// How often a shadow position past its market's close asks the venue whether it
/// has resolved. One request per condition per minute, the same restraint the
/// simulated event settlement uses.
const SHADOW_PROBE_SECS: u64 = 60;
/// How long the cached promotion record is trusted before it is re-read.
const SHADOW_RECORD_TTL_SECS: u64 = 30;
/// How long after its market closed a simulated position waits for a resolution
/// before it is written off unscored.
///
/// A market the venue never settles — voided, or one Gamma simply never flips —
/// would otherwise be probed once a minute forever and hold its market against
/// re-entry for the life of the instance. Written off rather than guessed: a
/// trade with no outcome is not a trade with a bad one, and the promotion
/// evidence must not contain a number nobody observed.
const SHADOW_GIVE_UP_SECS: i64 = 24 * 3600;

/// The promotion record for `version` on `asset`, cached briefly.
///
/// Version-scoped: a retrain starts a fresh record, so a new model can never
/// inherit the evidence an older one earned. That is the point of the scoping
/// and not an incidental detail — without it the first retrain after promotion
/// would hand its successor a real-money license it never tested.
async fn shadow_record_for(asset: &str, version: &str) -> ShadowStats {
    let g = globals(asset);
    {
        let cached = lock(&g.shadow_record);
        if let Some((v, rec, at)) = cached.as_ref() {
            if v == version && at.elapsed().as_secs() < SHADOW_RECORD_TTL_SECS {
                return *rec;
            }
        }
    }
    let Some(pool) = crate::helpers::db::pool_for(asset) else { return ShadowStats::default() };
    let rec = shadow_stats(&crate::helpers::db::gboost_shadow_returns(&pool, asset, version).await);
    *lock(&g.shadow_record) = Some((version.to_string(), rec, Instant::now()));
    rec
}

/// Drop the cached record, so the next read sees a trade that has just closed.
fn invalidate_shadow_record(asset: &str) {
    *lock(&globals(asset).shadow_record) = None;
}

/// Close out any simulated positions whose plan says they are done.
///
/// This is the shadow lane's exit engine. It exists inside the viper rather than
/// as a `ghost_mode` order because the intl patrol computes ONE `ghosting` value
/// per tick from the global switch and honors a per-order `ghost_mode` flag at
/// exactly one site, the resting-exit arm (`patrol_impl.rs`). An Entry signal
/// flagged simulated on a live instance would therefore place a real order —
/// which is the whole reason the lane books its own trades and emits no signal.
async fn sweep_shadow_exits(ctx: &StrategyContext, fee_rate: f64) {
    let asset = &ctx.crypto_filter;
    let g = globals(asset);
    {
        let mut swept = lock(&g.shadow_swept);
        if swept.is_some_and(|t| t.elapsed().as_secs() < SHADOW_SWEEP_SECS) { return; }
        *swept = Some(Instant::now());
    }
    let Some(pool) = crate::helpers::db::pool_for(asset) else { return };
    // Every open row for the asset, not just the serving model's. The exit rule
    // reads only the levels stored on the row, so a position a replaced model
    // opened can still be finished properly — and left unswept it would sit with
    // `closed_at` NULL forever, holding its market against re-entry and giving
    // that model a record with a hole in it.
    let open = crate::helpers::db::gboost_shadow_open_trades(&pool, asset).await;
    if open.is_empty() { return; }
    let now = Utc::now().timestamp();

    for t in open {
        let token = crate::venues::core::MarketId::new(&t.token_id);
        // The best bid is what a sale would actually get. A market that has
        // rotated out of the squadron has no book here, which is not an error:
        // the position then waits for its settlement below.
        let bid = crate::vipers::venue_for_token(ctx, &token).and_then(|(market, snap)| {
            let b = if token == market.yes_token { snap.yes_bid } else { snap.no_bid };
            if (Utc::now() - snap.timestamp).num_seconds() > MAX_SNAPSHOT_AGE_SECS { None } else { b.to_f64() }
        });

        // Past its market's close, ask the venue what it resolved to.
        //
        // Spawned rather than awaited here. `evaluate_exit` runs inside the
        // orchestrator's 500ms per-strategy timeout, and a Gamma call is allowed
        // five seconds; awaiting it inline would drop BOTH this viper's futures
        // for that tick, and if the tick were a decision minute the minute is
        // already marked decided, so it would be consumed with neither a live nor
        // a simulated entry. The rest of this viper fetches the same way
        // (`ensure_minute_data`, `ensure_price_history`) for exactly this reason.
        let resolved = lock(&g.shadow_resolved).get(&t.token_id).copied();
        if resolved.is_none() && now >= t.window_start + 3600 {
            let due = {
                let mut probed = lock(&g.shadow_probed);
                let due = probed.get(&t.condition_id).is_none_or(|at| at.elapsed().as_secs() >= SHADOW_PROBE_SECS);
                if due { probed.insert(t.condition_id.clone(), Instant::now()); }
                probed.retain(|_, at| at.elapsed().as_secs() < 7200);
                due
            };
            if due {
                let (asset_c, cid, token) = (asset.clone(), t.condition_id.clone(), t.token_id.clone());
                tokio::spawn(async move {
                    let got = crate::raptors::sports_ledger::settled_prices_for_market(
                        http(), &cid, std::slice::from_ref(&token),
                    ).await;
                    if let Some(px) = got.get(&token).copied() {
                        let mut out = lock(&globals(&asset_c).shadow_resolved);
                        out.insert(token, px);
                        // Bounded: one entry per token that has ever resolved.
                        if out.len() > 512 { out.clear(); }
                    }
                });
            }
            // Nothing to apply this pass; a later sweep picks the answer up.
            if now >= t.window_start + 3600 + SHADOW_GIVE_UP_SECS {
                if crate::helpers::db::gboost_shadow_abandon(&pool, t.id, "unresolved").await {
                    warn!(
                        "👻 GBoost shadow trade written off unscored [{}] {}: the venue never resolved it \
                         {}h after close, so it counts as no trade rather than as a guess",
                        t.market, t.side, (now - t.window_start - 3600) / 3600,
                    );
                }
                continue;
            }
        }

        let Some((exit_price, reason)) = shadow_exit(t.entry_price, t.tp_price, t.stop_price, bid, resolved) else {
            continue;
        };
        let ret = shadow_ret(t.entry_price, t.entry_fee, exit_price, reason, fee_rate);
        if !crate::helpers::db::gboost_shadow_close(&pool, t.id, exit_price, reason, ret).await {
            // Another sweep took it, or the write failed. Either way this sweep
            // must not also book a trade row for it.
            continue;
        }
        invalidate_shadow_record(asset);

        // The operator's trade list gets a ghost row, which is how a simulated
        // trade has always been shown. The row the promotion is computed from is
        // the shadow ledger's, not this one.
        let pnl = ret * t.entry_price * t.shares;
        // The fees the plan actually charged this trade, so the row's fee column
        // agrees with its P&L instead of reading zero against a net number.
        let exit_fee = if reason == "stop" {
            crate::vipers::gboost_planb_train::fee_per_share(fee_rate, exit_price)
        } else { 0.0 };
        let fees = (t.entry_fee + exit_fee) * t.shares;
        let mut scope = crate::state::TradeScope::new(asset, "", Some("crypto".to_string()), Some(asset.to_uppercase()));
        scope.ghost = true;
        crate::helpers::db::record_trade_db(
            &pool, &scope, Decimal::from_f64_retain(fees).unwrap_or_default(),
            STRATEGY_NAME, &t.market, &t.side,
            Decimal::from_f64_retain(t.entry_price).unwrap_or_default(),
            Decimal::from_f64_retain(exit_price).unwrap_or_default(),
            Decimal::from_f64_retain(t.shares).unwrap_or_default(),
            Decimal::from_f64_retain(pnl).unwrap_or_default(),
            &format!("Shadow {reason} — simulated ({})", t.model_version),
            None,
        ).await;
        info!(
            "👻 GBoost shadow {} [{}] {} | entry ${:.3} → ${:.3}, {:+.2}% (model {})",
            reason, t.market, t.side, t.entry_price, exit_price, ret * 100.0, t.model_version,
        );
    }
}

#[derive(Default)]
pub struct GboostPlanBStrategy;

impl GboostPlanBStrategy {
    pub fn new() -> Self { Self }
}

#[async_trait]
impl Strategy for GboostPlanBStrategy {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        // "Why no trades?" registry feed plus a throttled info line, so a hold
        // is recoverable from the log afterwards. Before 2026-09-21 this path
        // reported only to the registry: a disabled GBoost wrote nothing for
        // three hours across a +2.5% BTC move, and the startup banner's budget
        // line was the only trace of it, read as evidence it was live.
        let idle = |r: &str| {
            crate::helpers::viper_status::report_reason(&ctx.crypto_filter, STRATEGY_NAME, r);
            if crate::vipers::gate_log_permitted(STRATEGY_NAME, &ctx.crypto_filter, r, GATE_LOG_INTERVAL_SECS) {
                info!("🔒 GBoost gate: {}", r);
            }
        };
        if !dc.enable_gboost {
            idle("disabled in config");
            return Ok(StrategySignal::NoSignal);
        }
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            idle("session drawdown limit hit");
            return Ok(StrategySignal::NoSignal);
        }
        if let Some(why) = venue_gate() {
            idle(why);
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
        let f = |d: Decimal| d.to_f64().unwrap_or(0.0);
        {
            // The card's detail line: which lane, the serving model, and the
            // pipeline's state.
            let due = {
                let mut last = lock(&g.detail_refreshed);
                let due = last.is_none_or(|t| t.elapsed() >= Duration::from_secs(DETAIL_REFRESH_SECS));
                if due { *last = Some(Instant::now()); }
                due
            };
            if due {
                let lane = match model.as_deref() {
                    Some(m) => {
                        let rec = shadow_record_for(&ctx.crypto_filter, &m.version).await;
                        Some(lane_line(m.gate_passed, &rec, dc.gboost_planb_shadow_min_trades.max(0) as usize, f(dc.gboost_planb_shadow_min_win_rate)))
                    }
                    None => None,
                };
                crate::helpers::viper_status::report_detail(&ctx.crypto_filter, STRATEGY_NAME, Some(detail_line(model.as_deref(), &ctx.crypto_filter, lane)));
            }
        }
        if let Some(m) = &model {
            let plan = crate::vipers::gboost_planb_train::Plan::from_config(dc, f(crate::venues::taker_fee_rate()));
            if let Some(why) = m.plan_mismatch(f(dc.gboost_planb_take_profit_pct), f(dc.gboost_planb_stop_loss_pct), f(dc.gboost_planb_min_ask), f(dc.gboost_planb_max_ask), plan.entry.stamp()) {
                idle(&format!("{why}; waiting for the pipeline to train a model for the configured plan"));
                return Ok(StrategySignal::NoSignal);
            }
        }
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
        if now_s < t + BAR_SETTLE_SECS.max(HISTORY_SETTLE_SECS) || lock(&g.decided).contains(&(cid.clone(), t)) {
            return Ok(StrategySignal::NoSignal);
        }
        if now_s > t + DECISION_STALE_SECS {
            lock(&g.decided).insert((cid.clone(), t));
            idle("decision minute passed before its data arrived");
            return Ok(StrategySignal::NoSignal);
        }
        let Some(model) = model else {
            match crate::vipers::gboost_planb_train::status_line(&ctx.crypto_filter) {
                Some(line) => idle(&format!("no model in service; {line}")),
                None => idle("no model in service and the training pipeline is not running on this build"),
            }
            return Ok(StrategySignal::NoSignal);
        };
        // Both fetches are kicked off in the same tick: serialized, a slow Binance
        // host could spend the whole decision window before the history is asked for.
        let minute_data = ensure_minute_data(g, t, model.needs_funding(), model.needs_derivs());
        // The market's own price, from the same CLOB history and the same rule the
        // training rows use ([B46]), never from the order book.
        let tokens = [market.yes_token.as_str().to_string(), market.no_token.as_str().to_string()];
        let history = ensure_price_history(g, &cid, tokens, t);
        let Some((bars, funding, derivs)) = minute_data else {
            idle(match (model.needs_funding(), model.needs_derivs()) {
                (true, true) => "waiting for Binance bars, the settled funding rate, open interest and taker flow",
                (true, false) => "waiting for Binance bars and the settled funding rate",
                (false, true) => "waiting for Binance bars, open interest and taker flow",
                (false, false) => "waiting for Binance bars",
            });
            return Ok(StrategySignal::NoSignal);
        };
        let Some(hist) = history else {
            idle("waiting for the Polymarket price history");
            return Ok(StrategySignal::NoSignal);
        };
        let at = |side: usize, x: i64| crate::vipers::gboost_planb_train::history_mid_at(&hist[side], x);
        let (mid_now, mid_m1, mid_m5) = ([at(0, t), at(1, t)], [at(0, t - 60), at(1, t - 60)], [at(0, t - 300), at(1, t - 300)]);
        // A book old enough to be another market's is not this decision's price: the
        // removed per-minute recording was the only place the snapshot's age was
        // checked, and the ask below is scored, sized and paid on ([B48] review).
        if (now - snap.timestamp).num_seconds() > MAX_SNAPSHOT_AGE_SECS {
            idle("waiting for a fresh order book");
            return Ok(StrategySignal::NoSignal);
        }
        // ONE ask: what the model is asked about, what the gate judges, what is paid
        // ([B48]). Scoring an earlier ask than the order uses bought 3 to 5 ¢ above the
        // scored price on both 2026-09-19 trades, and the bias has a mechanism: the
        // model's strongest signal is a market lagging Binance, whose ask is rising.
        let ask = [snap.yes_ask.to_f64().unwrap_or(0.0), snap.no_ask.to_f64().unwrap_or(0.0)];
        {
            let mut decided = lock(&g.decided);
            decided.insert((cid.clone(), t));
            decided.retain(|(_, dt)| now_s - dt < 7200);
        }
        let inputs = DecisionInputs {
            w, t, bars: &bars, mid_now, mid_m1, mid_m5, ask, funding,
            oi_d5: derivs.oi_d5, oi_d30: derivs.oi_d30, tlsr: derivs.tlsr, tlsr15: derivs.tlsr15,
        };
        let features = match build_features(&inputs) {
            Ok(f) => f,
            Err(gap) => {
                info!("GBoost plan-B [{}] minute {}: no decision, inputs incomplete ({:?})", market.market_name, k, gap);
                idle("inputs incomplete for this minute");
                return Ok(StrategySignal::NoSignal);
            }
        };
        let preds = model.predict(&features);
        let decisions: Vec<SideDecision> = (0..2)
            .map(|side| decide_side(
                ask[side], preds[side].1, f(crate::venues::taker_fee_rate()), f(dc.gboost_planb_take_profit_pct),
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
            // A hold deliberately outlives its market, so it leaves the population above
            // the moment the market closes while still holding real shares on chain.
            // Without its own ceiling the next hour would see a full cap and open again,
            // hour after hour, with nothing bounding the total. This counts exactly the
            // population the cap above excludes.
            let held: Decimal = positions.iter()
                .filter(|(key, _)| key.strategy == STRATEGY_NAME && key.squadron == ctx.squadron_id)
                .filter(|(_, p)| !p.counts_toward_exposure(now))
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum();
            if held >= dc.gboost_planb_held_exposure_usdc {
                info!(
                    "GBoost plan-B [{}] not entering: ${:.2} already held awaiting settlement, cap ${:.2}",
                    market.market_name, held, dc.gboost_planb_held_exposure_usdc,
                );
                idle("held exposure cap reached");
                return Ok(StrategySignal::NoSignal);
            }
            dc.gboost_max_exposure_usdc - exposure
        };
        let shares = match entry_shares(dc.gboost_planb_trade_size_usdc, room, ask_dec, crate::venues::taker_fee_rate()) {
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

        // ── The shadow lane ──────────────────────────────────────────────────
        //
        // Real money needs two things, and this is the only place that decides
        // it: the model cleared the holdout gate when it was trained, and this
        // instance's own out-of-sample record cleared the pre-registration's bar.
        // The gate speaks for the model, the record speaks for the box, and
        // neither alone is evidence that this operator should be buying.
        //
        // Until both hold, the entry is taken simulated and NO signal is emitted.
        // It must be done this way rather than by flagging the order simulated:
        // the intl patrol derives one `ghosting` value per tick from the global
        // switch and consults a per-order flag at exactly one site, so an Entry
        // marked simulated on a live instance would place a real order.
        let record = shadow_record_for(&ctx.crypto_filter, &model.version).await;
        let min_trades = dc.gboost_planb_shadow_min_trades.max(0) as usize;
        let min_win = f(dc.gboost_planb_shadow_min_win_rate);
        if let Some(why) = shadow_blocks_live(model.gate_passed, &record, min_trades, min_win) {
            let Some(pool) = crate::helpers::db::pool_for(&ctx.crypto_filter) else {
                idle("the shadow lane has no database to record into");
                return Ok(StrategySignal::NoSignal);
            };
            // The live path asks the position map whether this market is already
            // held; the shadow lane is not in that map, so it has to ask its own
            // ledger or it would open a fresh simulated position every minute.
            if crate::helpers::db::gboost_shadow_holds(&pool, &ctx.crypto_filter, &cid).await {
                idle("a simulated position is already open on this market");
                return Ok(StrategySignal::NoSignal);
            }
            let (tp_price, stop_price) = shadow_levels(
                ask[side], f(dc.gboost_planb_take_profit_pct), f(dc.gboost_planb_stop_loss_pct),
                f(dc.gboost_planb_tp_ceiling),
            );
            let entry_fee = crate::vipers::gboost_planb_train::fee_per_share(
                f(crate::venues::taker_fee_rate()), ask[side],
            );
            let opened = crate::helpers::db::gboost_shadow_open(
                &pool, &ctx.crypto_filter, &model.version, &cid, token_id.as_str(),
                &market.market_name, label[side], w, ask[side], f(shares),
                tp_price, stop_price, entry_fee, preds[side].1, decisions[side].break_even,
            ).await;
            if opened {
                // Marked attempted exactly as a confirmed live fill is, so the
                // lane takes one entry per side per market and the record counts
                // the same population the live rule would have traded.
                lock(&g.attempted).insert((cid.clone(), side));
                invalidate_shadow_record(&ctx.crypto_filter);
                info!(
                    "👻 GBoost shadow ENTRY [{}] {} at ${:.3} x {:.2} (p={:.4} need={:.4}, model {}) | \
                     take-profit ${:.3}, stop ${:.3} | not spending real money: {}",
                    market.market_name, label[side], ask[side], shares, preds[side].1,
                    decisions[side].required, model.version, tp_price, stop_price, why,
                );
            }
            idle(&format!("trading simulated — {why}"));
            return Ok(StrategySignal::NoSignal);
        }
        // Promoted. Said once per model, because an instance crossing from
        // simulated to real money is the single most consequential thing this
        // viper does and it must be legible in the log afterwards.
        if lock(&g.shadow_announced).insert(model.version.clone()) {
            info!(
                "💰 GBoost is now trading REAL MONEY on model {}: it cleared the holdout gate, and this \
                 instance's shadow record is {} trades at {:+.2}% per trade, 90% lower bound {:+.2}%, win {:.3}",
                model.version, record.trades, record.mean_ret * 100.0, record.lower_bound * 100.0, record.win,
            );
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
            // The posture IN FORCE AT ENTRY, and this position's arm under it.
            //
            // `evaluate_exit` reads the posture fresh every tick, so an operator who
            // moves the knob changes how already-open positions are managed. Stamping
            // it here records what the position was opened under, which is both what
            // the exit study needs to score arms and the only way to detect that drift
            // after the fact (entry posture against the exit reason's posture).
            //
            // It is also the only durable record for a held position: a hold emits no
            // exit signal, so its trade row is written by the generic settlement path,
            // which knows nothing of GBoost or postures and is indistinguishable from
            // a below-minimum dust ride.
            "exit_posture": ExitPosture::from_i64(dc.gboost_planb_exit_posture).label(),
            "settlement_arm": holds_to_settlement(ExitPosture::from_i64(dc.gboost_planb_exit_posture), token_id.as_str()),
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
        // The shadow lane manages its own positions: they are not in the position
        // map, so nothing below this line would ever see them. Swept before the
        // map is locked, because the sweep awaits a database read and, for a
        // market that has closed, the venue — neither of which should be done
        // while the patrol tick is waiting on the lock.
        // Gated exactly as the entry path is. Without the venue and asset checks
        // `ensure_model` would look for a model on every ETH and SOL squadron and
        // on the other two venues, and warn every ten minutes that a file the
        // BTC-only pipeline was never going to write is missing.
        if dc.enable_gboost && venue_gate().is_none() && ctx.crypto_filter.eq_ignore_ascii_case("btc") {
            sweep_shadow_exits(ctx, plan_fee_rate()).await;
        }
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
            // The operator's posture for this position. A held position skips the
            // stop, both take-profits AND the rotation flatten: a flattened hold is
            // neither plan, and the study's hold arm is unflattened, so flattening
            // here would measure something nobody chose. What closes it instead is
            // the settlement path in `tasks::cleanup`, which asks the market what it
            // resolved to and books it at exactly $1.00 or $0.00 against this
            // strategy, idempotently. That path is already load-bearing: the
            // below-minimum branch above rides to settlement the same way.
            let posture = ExitPosture::from_i64(dc.gboost_planb_exit_posture);
            let held = holds_to_settlement(posture, token_id.as_str());
            if held {
                if lock(&g.below_minimum_noted).insert(format!("posture-hold:{token_id}")) {
                    info!(
                        "GBoost plan-B [{}] holding to settlement ({} posture{}): no stop, no take-profit, no rotation flatten",
                        market.market_name, posture.label(),
                        if posture == ExitPosture::Split { ", settlement arm" } else { "" },
                    );
                }
                continue;
            }
            if market.market_close_time.is_some_and(|c| now_s >= rotation_flatten_at(c.timestamp())) && bid > Decimal::ZERO {
                return Ok(StrategySignal::Exit {
                    params: params(bid, TimeInForce::Fak, false),
                    reason: format!("GBoostPlanBRotation: bid=${:.4} entry=${:.4}, flattened before the market rotation ends exit management | posture={}", bid, entry, posture.label()),
                    exit_pair: false,
                });
            }
            match exit_action(entry, bid, dc.gboost_planb_take_profit_pct, dc.gboost_planb_stop_loss_pct, dc.gboost_planb_tp_ceiling, dc.gboost_resting_tp_enabled) {
                ExitAction::Stop => {
                    return Ok(StrategySignal::Exit {
                        params: params(bid, TimeInForce::Fak, false),
                        reason: format!("GBoostPlanBSL: bid=${:.4} stop=${:.4} entry=${:.4} | posture={}", bid, entry * (Decimal::ONE - dc.gboost_planb_stop_loss_pct), entry, posture.label()),
                        exit_pair: false,
                    });
                }
                ExitAction::TakeProfit => {
                    return Ok(StrategySignal::Exit {
                        params: params(bid, TimeInForce::Fak, false),
                        reason: format!("GBoostPlanBTP: bid=${:.4} entry=${:.4} | posture={}", bid, entry, posture.label()),
                        exit_pair: false,
                    });
                }
                ExitAction::Rest(ask) => {
                    let target = take_profit_target(entry, dc.gboost_planb_take_profit_pct, dc.gboost_planb_tp_ceiling);
                    resting.push(StrategySignal::MakerRestingExit {
                        params: params(ask, TimeInForce::Gtc, true),
                        reason: format!(
                            "GBoostPlanBRestingTP: ask=${:.4} entry=${:.4} target={:+.2}%{}",
                            ask, entry, (ask - entry) / entry * dec!(100),
                            if ask > target { " (bid through the target: ask a tick over it)" } else { "" },
                        ),
                    });
                }
                ExitAction::Hold => {}
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

    /// An unrecognized posture is always the conservative one. The fallback is
    /// one-directional on purpose: a bad write, a hand-edited config row or a
    /// value from a newer build must never put real money on the hold plan by
    /// accident, and 36.3% of held trades lose the whole stake.
    #[test]
    fn the_exit_posture_falls_back_to_gates() {
        assert_eq!(ExitPosture::from_i64(0), ExitPosture::Gates);
        assert_eq!(ExitPosture::from_i64(1), ExitPosture::Settlement);
        assert_eq!(ExitPosture::from_i64(2), ExitPosture::Split);
        for unknown in [-9_i64, -1, 3, 7, 99, i64::MAX, i64::MIN] {
            assert_eq!(ExitPosture::from_i64(unknown), ExitPosture::Gates, "{unknown} must read as gates");
        }
    }

    /// `evaluate_exit` runs every tick, so a split position must reach the same
    /// arm every time. One that changed arms could take a stop and then be held,
    /// or be held past the flatten and then stopped, which is neither arm and
    /// would corrupt the comparison the posture exists to serve.
    #[test]
    fn a_split_position_keeps_its_arm() {
        let token = "99366542591070685238863007489473194324758699764861491299517243361573997343945";
        let first = settlement_arm(token);
        for _ in 0..100 {
            assert_eq!(settlement_arm(token), first, "the arm must not move between ticks");
        }
        // Different tokens are assigned independently, and the split is roughly even
        // across many ids: a coin that always answered the same way would put every
        // position in one arm and measure nothing.
        let arms: Vec<bool> = (0..400).map(|i| settlement_arm(&format!("{i}{}", token))).collect();
        let held = arms.iter().filter(|a| **a).count();
        assert!((120..=280).contains(&held), "expected a roughly even split, got {held} of 400");
    }

    /// The held-exposure cap counts exactly the population the main cap drops.
    ///
    /// `GBOOST_MAX_EXPOSURE_USDC` stops counting a position the moment its market
    /// closes, which was safe while nothing outlived its market. A hold posture
    /// makes that the normal case, so without this cap each hour's entry would see
    /// a full allowance while the previous hour's hold still held real shares, and
    /// holds would stack with nothing bounding the total.
    #[test]
    fn the_held_exposure_cap_counts_what_the_main_cap_drops() {
        let now = chrono::Utc::now();
        let open = |close: Option<chrono::DateTime<chrono::Utc>>| crate::state::Position {
            shares: dec!(5.06), avg_entry: dec!(0.79), opened_at: now,
            close_time: close, market_name: "Bitcoin Up or Down - September 16, 10AM ET".into(),
            pair_token_id: crate::venues::core::MarketId::new("t"), fill_confirmed_at: Some(now),
            paired_leg_token_id: None, entry_fee: Decimal::ZERO,
        };
        // A position whose market is still open is live risk: the main cap sees it,
        // the held cap does not.
        let live = open(Some(now + chrono::Duration::minutes(20)));
        assert!(live.counts_toward_exposure(now), "an open market is live risk");
        // Once the market closes the main cap drops it, which is exactly when the
        // held cap must pick it up. These two predicates must stay complementary:
        // if both ever excluded a position, its capital would be invisible to both.
        let held = open(Some(now - chrono::Duration::minutes(5)));
        assert!(!held.counts_toward_exposure(now), "a closed market leaves the main cap");
        assert!(live.counts_toward_exposure(now) != held.counts_toward_exposure(now),
                "every position must fall under exactly one of the two caps");
        // The gate refuses entry once the held total is at or over the cap, which is
        // what bounds accumulation. At the $4 trade size the $8 default admits two
        // concurrent holds and refuses the third, so a hold posture can trade while
        // the population stays bounded instead of growing hourly.
        let stake = held.shares * held.avg_entry;      // 5.06 x $0.79 = $3.9974
        let cap = dec!(8.0);
        assert!(stake < cap, "one hold must fit, or a hold posture could never trade");
        assert!(stake * dec!(2) < cap, "two holds must fit at the default");
        assert!(stake * dec!(3) >= cap, "three holds must be refused: {stake} each against {cap}");
    }

    /// The gates posture never holds, settlement always holds, and split defers
    /// to the token's own arm.
    #[test]
    fn holds_to_settlement_follows_the_posture() {
        let token = "12345678901234567890";
        assert!(!holds_to_settlement(ExitPosture::Gates, token), "gates never holds");
        assert!(holds_to_settlement(ExitPosture::Settlement, token), "settlement always holds");
        assert_eq!(holds_to_settlement(ExitPosture::Split, token), settlement_arm(token));
        assert_eq!(ExitPosture::Gates.label(), "gates");
        assert_eq!(ExitPosture::Settlement.label(), "settlement");
        assert_eq!(ExitPosture::Split.label(), "split");
    }

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
                funding: Some(fixture["funding"].as_f64().unwrap()),
                oi_d5: None, oi_d30: None, tlsr: None, tlsr15: None,
            };
            let got = build_features(&inputs).expect("fixture inputs are complete");
            for side in 0..2 {
                let expected = fixture["expected"][side].as_array().unwrap();
                for (j, name) in FEATURE_NAMES.iter().enumerate() {
                    // The builder leaves the four futures columns missing; the fixture carries
                    // them as the evaluator's zeros, which is what `predict` feeds the model.
                    if (23..=26).contains(&j) {
                        assert!(got[side][j].is_nan(), "t={} side {side} {name}: the builder never computes this", inputs.t);
                        assert_eq!(expected[j].as_f64(), Some(0.0));
                        continue;
                    }
                    match expected[j].as_f64() {
                        None => assert!(got[side][j].is_nan(), "t={} side {side} {name}: expected missing, got {}", inputs.t, got[side][j]),
                        // norm_cdf is a 1.5e-7 approximation of the harness's erf; everything else is exact arithmetic.
                        Some(e) => assert!((got[side][j] - e).abs() < 1e-6, "t={} side {side} {name}: got {} expected {e}", inputs.t, got[side][j]),
                    }
                }
            }
        }
    }

    /// A missing funding input is a missing value, not a zero, so a model that was
    /// trained with funding never sees a fabricated rate; a model that zeroes funding
    /// does not wait for one.
    #[test]
    fn missing_funding_is_missing_until_a_model_zeroes_it() {
        let bars: Vec<Bar> = (0..61).map(|i| Bar { open_s: 3600 + 60 * i, open: 1.0, high: 1.0, low: 1.0, close: 1.0 }).collect();
        let f = build_features(&DecisionInputs {
            w: 3600 + 30 * 60, t: 3600 + 61 * 60, bars: &bars, mid_now: [Some(0.5), Some(0.5)], mid_m1: [None; 2], mid_m5: [None; 2], ask: [0.5, 0.5], funding: None, oi_d5: None, oi_d30: None, tlsr: None, tlsr15: None,
        }).unwrap();
        assert!(f[0][22].is_nan() && f[1][22].is_nan());
        assert!((23..=26).all(|j| f[0][j].is_nan()));
    }

    /// The plan a model was labeled for is compared with the configured plan; the first
    /// production model carries it as `label=B_aggr20`.
    #[test]
    fn the_plan_stamp_is_compared_with_the_configured_plan() {
        let json = include_str!("testdata/gboost_planb_features.json"); // any file: the model is built below
        let _ = json;
        // Exercise the comparison on the struct directly; loading is covered by the
        // trainer's stamped-model test.
        let m_with = |plan: Option<[f64; 4]>, entry_rule: Option<String>| {
            // A booster is needed only to construct the struct; the smallest fit will do.
            let data = vec![0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0];
            let matrix = Matrix::new(&data, 4, 2);
            let mut b = PerpetualBooster::default().set_iteration_limit(Some(1)).set_num_threads(Some(1));
            b.fit(&matrix, &[0.0, 1.0, 0.0, 1.0], None, None).unwrap();
            PlanBModel { booster: b, platt_a: 1.0, platt_b: 0.0, version: "t".into(), trees: 1, zeroed: vec![23, 24, 25, 26], plan, entry_rule, trained_by: None, created_at: None, calibrated_through: None, holdout_summary: None, gate_passed: true }
        };
        let m = |plan: Option<[f64; 4]>| m_with(plan, Some("e2".to_string()));
        assert!(m(Some(REFERENCE_PLAN)).plan_mismatch(0.20, 0.11, 0.43, 0.75, "e2").is_none());
        assert!(m(Some(REFERENCE_PLAN)).plan_mismatch(0.15, 0.11, 0.43, 0.75, "e2").is_some());
        assert!(m(None).plan_mismatch(0.15, 0.11, 0.43, 0.75, "e2").is_none(), "an unstamped plan cannot be compared");
        // The entry rule is refused on its own, and a model with no stamp is the old
        // rule, whatever its numbers say ([B47]).
        let why = m(Some(REFERENCE_PLAN)).plan_mismatch(0.20, 0.11, 0.43, 0.75, "e1").expect("an e2 model is refused for an e1 plan");
        assert!(why.contains("entry rule"), "{why}");
        let unstamped = m_with(Some(REFERENCE_PLAN), None);
        assert!(unstamped.plan_mismatch(0.20, 0.11, 0.43, 0.75, "e2").is_some(), "a model from before the stamp is e1 and must be refused");
        assert!(unstamped.plan_mismatch(0.20, 0.11, 0.43, 0.75, "e1").is_none());
        assert!(m(Some(REFERENCE_PLAN)).needs_funding());
        let mut z = m(Some(REFERENCE_PLAN));
        z.zeroed.push(FUNDING_COLUMN);
        assert!(!z.needs_funding());
    }

    #[test]
    fn missing_history_or_mids_are_reported_not_guessed() {
        let bars: Vec<Bar> = (0..61).map(|i| Bar { open_s: 3600 + 60 * i, open: 1.0, high: 1.0, low: 1.0, close: 1.0 }).collect();
        let base = |bars: &[Bar]| build_features(&DecisionInputs {
            w: 7200, t: 3600 + 61 * 60, bars, mid_now: [Some(0.5), Some(0.5)], mid_m1: [None; 2], mid_m5: [None; 2], ask: [0.5, 0.5], funding: Some(0.0), oi_d5: None, oi_d30: None, tlsr: None, tlsr15: None,
        });
        assert_eq!(base(&bars[1..]).unwrap_err(), FeatureGap::MissingBars);
        let mut with_strike = bars.clone();
        with_strike.retain(|b| b.open_s != 7200);
        assert_eq!(base(&with_strike).unwrap_err(), FeatureGap::MissingBars);
        let ok = build_features(&DecisionInputs {
            w: 3600 + 30 * 60, t: 3600 + 61 * 60, bars: &bars, mid_now: [Some(0.5), None], mid_m1: [None; 2], mid_m5: [None; 2], ask: [0.5, 0.5], funding: Some(0.0), oi_d5: None, oi_d30: None, tlsr: None, tlsr15: None,
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
        assert_eq!(act(dec!(0.50), dec!(0.61), false), ExitAction::TakeProfit);
        assert_eq!(act(dec!(0.50), dec!(0.52), true), ExitAction::Rest(dec!(0.60)));
        assert_eq!(act(dec!(0.50), dec!(0.52), false), ExitAction::Hold);
        // No bid is not a stop.
        assert_eq!(act(dec!(0.50), dec!(0), false), ExitAction::Hold);
        // A rested position with the bid through the target is not a taker sale.
        assert_eq!(act(dec!(0.50), dec!(0.61), true), ExitAction::Rest(dec!(0.62)));
        assert_eq!(act(dec!(0.50), dec!(0.60), true), ExitAction::Rest(dec!(0.61)));
        // ...and a bid at $0.99 leaves nowhere for a post-only ask to sit.
        assert_eq!(act(dec!(0.80), dec!(0.99), true), ExitAction::Hold);
        // $0.75 entry: 20% would be $0.90, capped at the ceiling.
        assert_eq!(take_profit_target(dec!(0.75), dec!(0.20), dec!(0.90)), dec!(0.90));
        assert_eq!(take_profit_target(dec!(0.43), dec!(0.20), dec!(0.90)), dec!(0.52));
    }

    /// 2026-09-13 08:37:18 ET, real money. NO entered at $0.4899, the $0.59
    /// ask resting, and the book gapped to a $0.60 bid: the ask filled as a
    /// maker and the same tick fired a taker `TakeProfit`, which pulled the
    /// filled ask and sent a sell the venue rejected. With the ask resting the
    /// take-profit must never be a taker sale, at any bid.
    #[test]
    fn the_resting_take_profit_is_never_doubled_by_a_taker_sale() {
        let at = |bid| exit_action(dec!(0.4899), bid, dec!(0.20), dec!(0.11), dec!(0.90), true);
        assert_eq!(take_profit_target(dec!(0.4899), dec!(0.20), dec!(0.90)), dec!(0.59));
        assert_eq!(at(dec!(0.55)), ExitAction::Rest(dec!(0.59)));
        for bid in [dec!(0.59), dec!(0.60), dec!(0.67), dec!(0.90), dec!(0.98)] {
            match at(bid) {
                ExitAction::Rest(ask) => assert!(ask > bid && ask < Decimal::ONE, "bid {bid}: ask {ask}"),
                other => panic!("bid {bid}: {other:?} is a taker exit or a hold, not the resting plan"),
            }
        }
        // The stop still outranks the ask.
        assert_eq!(at(dec!(0.43)), ExitAction::Stop);
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
        assert!(w + 45 * 60 + BAR_SETTLE_SECS.max(HISTORY_SETTLE_SECS) + MIN_HOLD_BEFORE_FLATTEN_SECS <= at);
    }

    /// The live mid is the training rule applied to the CLOB history: the last
    /// point at or before the minute, missing when older than the staleness limit.
    #[test]
    fn a_mid_older_than_the_staleness_limit_is_missing() {
        use crate::vipers::gboost_planb_train::history_mid_at;
        let s = [(940, 0.38), (1000, 0.40)];
        assert_eq!(history_mid_at(&s, 1000), Some(0.40));
        assert_eq!(history_mid_at(&s, 1059), Some(0.40), "a point up to a minute old is that minute's mid");
        assert_eq!(history_mid_at(&s, 1000 + 180), Some(0.40));
        assert_eq!(history_mid_at(&s, 1000 + 181), None);
        assert_eq!(history_mid_at(&s, 939), None);
        assert_eq!(history_mid_at(&[], 1000), None);
    }

    /// Live and training parse a `prices-history` response into the same series.
    #[test]
    fn price_history_parses_sorted() {
        let v = serde_json::json!({"history": [{"t": 1060, "p": 0.305}, {"t": 1000, "p": 0.29}, {"t": "x", "p": 0.1}]});
        assert_eq!(crate::vipers::gboost_planb_train::parse_price_history(&v), vec![(1000, 0.29), (1060, 0.305)]);
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

#[cfg(test)]
mod deriv_feature_tests {
    use super::{deriv_features_at, DerivFeatures, TLSR_BUCKET_SECS};

    /// 5-minute buckets given as `(minutes before t, value)`, returned oldest first.
    fn series(t: i64, samples: &[(i64, f64)]) -> Vec<(i64, f64)> {
        let mut v: Vec<(i64, f64)> = samples.iter().map(|(m, x)| (t - m * 60, *x)).collect();
        v.sort_by_key(|x| x.0);
        v
    }

    /// The harness's arithmetic, longhand: `oi_d5 = ln(oi_now / oi[io-1])`,
    /// `oi_d30 = ln(oi_now / oi[io-6])`; `tlsr` is the last bucket completed by `t`
    /// (stamped `t - 300` here, not the one stamped `t`), and `tlsr15` is the mean
    /// of that bucket and the two before it.
    #[test]
    fn the_four_columns_match_build_dataset_py() {
        let t = 1_700_000_000;
        let oi = series(t, &[(30, 100.0), (25, 101.0), (20, 102.0), (15, 103.0), (10, 104.0), (5, 105.0), (0, 110.0)]);
        // Buckets stamped t-15, t-10, t-5 are complete by t; the one stamped t is not.
        let tl = series(t, &[(15, 0.90), (10, 1.00), (5, 1.10), (0, 5.00)]);
        let f = deriv_features_at(&oi, &tl, t);
        assert!((f.oi_d5.unwrap() - (110.0f64 / 105.0).ln()).abs() < 1e-12);
        assert!((f.oi_d30.unwrap() - (110.0f64 / 100.0).ln()).abs() < 1e-12);
        assert_eq!(f.tlsr, Some(1.10), "the bucket stamped at t is still open and must not be read");
        assert!((f.tlsr15.unwrap() - (0.90 + 1.00 + 1.10) / 3.0).abs() < 1e-12, "a three-bucket mean, not a difference");
    }

    /// The completed-bucket rule is the 2026-09-12 leak correction: a bucket stamped
    /// inside the last `TLSR_BUCKET_SECS` before `t` is not yet complete, and with
    /// only two completed buckets there is no three-bucket mean.
    #[test]
    fn a_bucket_still_open_at_t_is_not_read() {
        let t = 1_700_000_000;
        let tl = series(t, &[(15, 0.9), (10, 1.0), (4, 7.0), (1, 9.0)]);
        let f = deriv_features_at(&[], &tl, t);
        assert_eq!(f.tlsr, Some(1.0));
        assert_eq!(f.tlsr15, None);
        let tl = series(t, &[(15, 0.9), (10, 1.0), (5, 1.1)]);
        assert_eq!(deriv_features_at(&[], &tl, t + TLSR_BUCKET_SECS - 1).tlsr, Some(1.1));
        assert_eq!(deriv_features_at(&[], &tl, t - 1).tlsr, Some(1.0), "at t-1 the bucket stamped t-5 is not complete");
    }

    /// A series that does not reach yields a missing input, never a zero or the
    /// oldest sample; lookbacks are by bucket, so a gap widens them as in the harness;
    /// only samples at or before `t` count.
    #[test]
    fn missing_history_is_none_and_lookbacks_are_by_bucket() {
        let t = 1_700_000_000;
        assert_eq!(deriv_features_at(&[], &[], t), DerivFeatures::default());
        let oi = series(t, &[(5, 100.0), (0, 110.0)]);
        let f = deriv_features_at(&oi, &[], t);
        assert!(f.oi_d5.is_some() && f.oi_d30.is_none(), "one bucket back exists, six do not");
        let oi = series(t, &[(20, 100.0), (0, 110.0)]);
        assert!((deriv_features_at(&oi, &[], t).oi_d5.unwrap() - (110.0f64 / 100.0).ln()).abs() < 1e-12);
        let oi = series(t, &[(5, 100.0), (0, 110.0), (-5, 999.0)]);
        assert!((deriv_features_at(&oi, &[], t).oi_d5.unwrap() - (110.0f64 / 100.0).ln()).abs() < 1e-12);
    }
}

#[cfg(test)]
mod shadow_promotion_tests {
    use super::{lane_line, shadow_blocks_live, shadow_exit, shadow_levels, shadow_ret, shadow_stats, ShadowStats};

    fn passing() -> ShadowStats {
        ShadowStats { trades: 44, mean_ret: 0.026, lower_bound: 0.004, win: 0.57 }
    }

    /// Both halves must hold: the gate speaks for the model, the record speaks
    /// for this instance. Neither alone licenses spending an operator's money.
    #[test]
    fn a_passing_gate_and_a_passing_record_together_release_real_money() {
        assert!(shadow_blocks_live(true, &passing(), 40, 0.53).is_none());
        assert_eq!(shadow_blocks_live(false, &passing(), 40, 0.53).as_deref(),
            Some("the model has not cleared the holdout gate"));
    }

    /// Each unmet condition names itself, because a card that cannot say which
    /// number is missing leaves an operator unable to tell a viper that is
    /// working from one that is stuck.
    #[test]
    fn every_unmet_condition_names_itself() {
        let cases = [
            (ShadowStats { trades: 12, ..passing() }, "shadow record 12 of 40 trades"),
            (ShadowStats { mean_ret: -0.004, ..passing() }, "shadow mean return -0.40% is not above zero"),
            (ShadowStats { lower_bound: -0.021, ..passing() }, "shadow 90% lower bound -2.10% is not above zero"),
            (ShadowStats { win: 0.48, ..passing() }, "shadow win rate 0.480 is below the 0.53 bar"),
        ];
        for (rec, expected) in cases {
            assert_eq!(shadow_blocks_live(true, &rec, 40, 0.53).as_deref(), Some(expected));
        }
    }

    /// The bar is the pre-registration's, so a record that only just clears it
    /// clears it, and a zero lower bound does not: "above zero" is strict
    /// because an interval touching zero is the result that failed.
    #[test]
    fn the_bar_is_the_pre_registrations_and_zero_is_not_above_zero() {
        let edge = ShadowStats { trades: 40, mean_ret: 0.0001, lower_bound: 0.0001, win: 0.53 };
        assert!(shadow_blocks_live(true, &edge, 40, 0.53).is_none(), "exactly at the bar passes");
        let zero_lb = ShadowStats { lower_bound: 0.0, ..edge };
        assert!(shadow_blocks_live(true, &zero_lb, 40, 0.53).is_some(), "a zero lower bound is not above zero");
    }

    /// The plan's exit rule, which the shadow record is only evidence about if
    /// it is the same rule the holdout measured.
    #[test]
    fn the_shadow_exit_is_the_plans_own_rule() {
        let (entry, tp, stop) = (0.60, 0.72, 0.51);

        // Nothing while the bid sits between the levels and the market is open.
        assert_eq!(shadow_exit(entry, tp, stop, Some(0.65), None), None);

        // The stop sells into the book at the bid, not at the level.
        assert_eq!(shadow_exit(entry, tp, stop, Some(0.49), None), Some((0.49, "stop")));
        assert_eq!(shadow_exit(entry, tp, stop, Some(0.51), None), Some((0.51, "stop")),
                   "at the level is a stop, as the labeler has it");

        // A resting take-profit fills at its own price, not at the bid that
        // reached it: that is what resting means.
        assert_eq!(shadow_exit(entry, tp, stop, Some(0.80), None), Some((0.72, "take-profit")));

        // A tick that satisfies both is scored the conservative way. It takes a
        // degenerate plan to overlap the levels (the stop above the target), but
        // the knobs are the operator's and the precedence must be defined.
        assert_eq!(shadow_exit(entry, 0.61, 0.65, Some(0.63), None), Some((0.63, "stop")),
                   "the stop is checked first, as the labeler checks it");

        // A take-profit that cannot fill is not an exit.
        assert_eq!(shadow_exit(entry, 1.00, stop, Some(0.99), None), None,
                   "a dollar take-profit can never fill");
        assert_eq!(shadow_exit(entry, 0.55, stop, Some(0.99), None), None,
                   "a take-profit at or below the entry is not a profit");

        // An empty bid side is published as $0.00, which is the absence of a
        // price and not a price of zero. Booking it would record a total loss
        // the live viper would never take — `exit_action` refuses to stop on a
        // zero bid — and one such row would poison a 40-trade record.
        assert_eq!(shadow_exit(entry, tp, stop, Some(0.0), None), None,
                   "a websocket resync must not book a shadow trade as a total loss");
        assert_eq!(shadow_exit(entry, tp, stop, Some(0.0), Some(1.0)), Some((1.0, "settlement")),
                   "and it must not stop the real resolution from closing the trade");

        // With no book — the market rotated away — only a resolution closes it.
        assert_eq!(shadow_exit(entry, tp, stop, None, None), None);
        assert_eq!(shadow_exit(entry, tp, stop, None, Some(1.0)), Some((1.0, "settlement")));
        assert_eq!(shadow_exit(entry, tp, stop, None, Some(0.0)), Some((0.0, "settlement")));
    }

    /// Fees land where the plan puts them: on the way in always, on the way out
    /// only when the stop sells into the book.
    #[test]
    fn only_a_stop_pays_an_exit_fee() {
        let fee = 0.07;
        let entry_fee = crate::vipers::gboost_planb_train::fee_per_share(fee, 0.60);
        let tp = shadow_ret(0.60, entry_fee, 0.72, "take-profit", fee);
        let settle = shadow_ret(0.60, entry_fee, 1.0, "settlement", fee);
        let stop = shadow_ret(0.60, entry_fee, 0.51, "stop", fee);

        assert!((tp - (0.72 - 0.60 - entry_fee) / 0.60).abs() < 1e-12, "a resting sale pays no exit fee");
        assert!((settle - (1.0 - 0.60 - entry_fee) / 0.60).abs() < 1e-12, "a redemption sells nothing");
        let stop_fee = crate::vipers::gboost_planb_train::fee_per_share(fee, 0.51);
        assert!(stop_fee > 0.0);
        assert!((stop - (0.51 - 0.60 - entry_fee - stop_fee) / 0.60).abs() < 1e-12);
        assert!(stop < 0.0 && tp > 0.0);
    }

    /// The take-profit is ticked up and capped; the stop is entry-relative.
    #[test]
    fn the_levels_follow_the_plan_knobs() {
        let (tp, sl) = shadow_levels(0.60, 0.20, 0.15, 0.95);
        assert!((tp - 0.72).abs() < 1e-9);
        assert!((sl - 0.51).abs() < 1e-9);
        // The ceiling binds before a dollar.
        let (tp, _) = shadow_levels(0.90, 0.20, 0.15, 0.95);
        assert!((tp - 0.95).abs() < 1e-9, "the tp ceiling caps the target");
    }

    /// The record's interval resamples MARKETS, not rows: two entries on the
    /// same hourly market are one market's worth of evidence, and resampling
    /// rows would report a tighter bound than the evidence supports and promote
    /// the lane early.
    #[test]
    fn the_record_bootstraps_by_market() {
        assert_eq!(shadow_stats(&[]).trades, 0, "no record is not a passing record");

        let flat: Vec<(i64, f64)> = (0..40).map(|i| (i as i64 * 3600, 0.05)).collect();
        let r = shadow_stats(&flat);
        assert_eq!(r.trades, 40);
        assert!((r.mean_ret - 0.05).abs() < 1e-12);
        assert!((r.win - 1.0).abs() < 1e-12);
        assert!(r.lower_bound > 0.0, "40 identical winners have a lower bound above zero");

        // The same 40 returns from ONE market are one market of evidence.
        let same: Vec<(i64, f64)> = (0..40).map(|_| (0i64, 0.05)).collect();
        let one = shadow_stats(&same);
        assert_eq!(one.trades, 40);
        assert!((one.mean_ret - 0.05).abs() < 1e-12);

        let mixed: Vec<(i64, f64)> = (0..40)
            .map(|i| (i as i64 * 3600, if i % 2 == 0 { 0.30 } else { -0.28 }))
            .collect();
        let m = shadow_stats(&mixed);
        assert!((m.win - 0.5).abs() < 1e-12);
        assert!(m.lower_bound < m.mean_ret, "a noisy record's lower bound sits below its mean");
    }

    /// The card answers the operator's only question, with the numbers.
    #[test]
    fn the_card_names_the_lane_and_what_is_missing() {
        let thin = ShadowStats { trades: 12, mean_ret: 0.008, lower_bound: -0.021, win: 0.58 };
        let line = lane_line(true, &thin, 40, 0.53);
        assert!(line.starts_with("SHADOW"), "{line}");
        assert!(line.contains("shadow record 12 of 40 trades"), "it must name the missing number: {line}");

        let earned = ShadowStats { trades: 44, mean_ret: 0.026, lower_bound: 0.004, win: 0.57 };
        let line = lane_line(true, &earned, 40, 0.53);
        assert!(line.starts_with("LIVE"), "{line}");
        assert!(line.contains("+2.60%"), "{line}");

        // A model that never cleared the gate says so, however good the record.
        let line = lane_line(false, &earned, 40, 0.53);
        assert!(line.starts_with("SHADOW") && line.contains("holdout gate"), "{line}");
    }


    /// The shadow ledger end to end against a real database: open, refuse to
    /// double-open, close, and have the closed trade appear in the record that
    /// decides promotion.
    ///
    /// Written against a real pool rather than mocked because the last time a
    /// DRADIS ledger path was covered only by pure-function tests, the schema
    /// was wrong and the writer silently returned false.
    #[tokio::test]
    async fn the_shadow_ledger_opens_closes_and_feeds_the_record() {
        use crate::helpers::db;
        use sqlx::sqlite::SqlitePoolOptions;
        let p = SqlitePoolOptions::new().max_connections(1).connect("sqlite::memory:").await.unwrap();
        db::init_schema(&p).await.unwrap();
        db::run_migrations(&p).await;

        const V: &str = "engine-btc-20260923T1200Z";
        assert!(!db::gboost_shadow_holds(&p, "btc", "cond-1").await);

        let opened = db::gboost_shadow_open(
            &p, "btc", V, "cond-1", "tok-yes", "Bitcoin Up or Down - September 23, 12PM ET",
            "YES", 1_758_600_000, 0.60, 6.0, 0.72, 0.51, 0.0168, 0.71, 0.55,
        ).await;
        assert!(opened, "the shadow ledger must accept an entry, or the lane records nothing");
        assert!(db::gboost_shadow_holds(&p, "btc", "cond-1").await,
                "an open simulated position must be visible, or the lane re-enters every minute");

        let open = db::gboost_shadow_open_trades(&p, "btc").await;
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].side, "YES");
        assert!((open[0].tp_price - 0.72).abs() < 1e-9);
        assert!((open[0].entry_fee - 0.0168).abs() < 1e-9);

        assert_eq!(open[0].model_version, V, "the row carries the model that took it");
        // The RECORD is version-scoped, so a retrain cannot inherit the evidence
        // an older model earned — while the open position itself is still swept
        // and still blocks a second entry on the same market.
        assert!(db::gboost_shadow_returns(&p, "btc", "engine-btc-OTHER").await.is_empty());
        assert!(db::gboost_shadow_holds(&p, "btc", "cond-1").await,
                "a retrain mid-market must not let a new model stack a second position");

        // Nothing counts toward the record until it closes.
        assert!(db::gboost_shadow_returns(&p, "btc", V).await.is_empty());

        let ret = shadow_ret(0.60, 0.0168, 0.72, "take-profit", 0.07);
        assert!(db::gboost_shadow_close(&p, open[0].id, 0.72, "take-profit", ret).await);
        assert!(!db::gboost_shadow_close(&p, open[0].id, 0.72, "take-profit", ret).await,
                "a second sweep must not close the same trade twice");

        assert!(!db::gboost_shadow_holds(&p, "btc", "cond-1").await);
        assert!(db::gboost_shadow_open_trades(&p, "btc").await.is_empty());

        let rows = db::gboost_shadow_returns(&p, "btc", V).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 1_758_600_000, "the hourly window is carried for the market bootstrap");
        assert!((rows[0].1 - ret).abs() < 1e-12);

        let rec = shadow_stats(&rows);
        assert_eq!(rec.trades, 1);
        assert!(rec.mean_ret > 0.0);
        // One trade is nowhere near the bar, and the card must say which number.
        let why = shadow_blocks_live(true, &rec, 40, 0.53).expect("one trade cannot release real money");
        assert!(why.contains("1 of 40 trades"), "{why}");
    }


    /// A model written before the gate verdict was stamped is judged on the
    /// holdout it did record, not stranded in the shadow lane forever.
    ///
    /// This matters because `shadow_blocks_live` refuses on a failed gate BEFORE
    /// it looks at the record: a model read as failed can never be promoted by
    /// its own evidence, so "absent means failed" would be permanent rather than
    /// cautious, and every model in service anywhere when this shipped predates
    /// the stamp.
    #[test]
    fn an_unstamped_model_is_judged_on_the_holdout_it_recorded() {
        use super::gate_from_stamp;
        let trees = crate::vipers::gboost_planb::STRUCTURAL_MIN_TREES;
        let min_trades = crate::config::GBOOST_PLANB_GATE_MIN_TRADES as usize;

        // A model that recorded a passing holdout passes.
        assert!(gate_from_stamp(trees, Some(0.05), Some(min_trades), Some(0.03), Some(0.60)));

        // Each gate condition still bites on the recorded numbers.
        assert!(!gate_from_stamp(trees, Some(-0.003), Some(min_trades), Some(0.03), Some(0.60)),
                "skill at or below the base rate fails");
        assert!(!gate_from_stamp(trees, Some(0.05), Some(1), Some(0.03), Some(0.60)),
                "one holdout trade is not a holdout");
        assert!(!gate_from_stamp(trees, Some(0.05), Some(min_trades), Some(-0.39), Some(0.60)),
                "a negative mean return fails");
        assert!(!gate_from_stamp(trees, Some(0.05), Some(min_trades), Some(0.03), Some(0.10)),
                "a win rate below the bar fails");
        assert!(!gate_from_stamp(1, Some(0.05), Some(min_trades), Some(0.03), Some(0.60)),
                "the structural tree minimum still applies");

        // A file with no holdout stamp at all has recorded no evidence, so it
        // stays simulated — the right answer for a model nobody can check.
        assert!(!gate_from_stamp(trees, None, None, None, None),
                "no recorded evidence must never read as a passing gate");

        // The production shape at the time this shipped: skill -0.0034, one
        // holdout trade, -39.32% mean return. It must not be licensed.
        assert!(!gate_from_stamp(trees, Some(-0.0034), Some(1), Some(-0.3932), Some(0.0)));
    }

}
