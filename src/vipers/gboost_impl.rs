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

/// GBoost Strategy — Online gradient-boosted binary classification
///
/// Uses the `perpetual` crate's `PerpetualBooster` (LogLoss objective) to predict
/// near-term YES price direction from a rolling window of orderbook + oracle features.
///
/// ── Feature Vector (NUM_FEATURES = 30) ──────────────────────────────────────
///   [0]  yes_obi         — (yes_bid_depth − yes_ask_depth) / total depth
///   [1]  no_obi          — (no_bid_depth − no_ask_depth) / total depth
///   [2]  yes_ask         — best ask price for YES token
///   [3]  no_ask          — best ask price for NO token
///   [4]  yes_spread      — yes_ask − yes_bid
///   [5]  no_spread       — no_ask − no_bid
///   [6]  velocity        — 5-second Binance oracle velocity (÷ 1000 for scale)
///   [7]  velocity_1s     — 1-second oracle velocity (÷ 1000)
///   [8]  acceleration    — velocity derivative (÷ 1000)
///   [9]  funding_rate    — Binance perpetual funding rate
///  [10]  oracle_drift_60m — 60-minute oracle drift (÷ 10000)
///  [11]  oracle_price    — Binance oracle price (÷ 100_000 to reach O(1))
///  [12]  secs_to_expiry_norm — seconds until market expiry, clamped to
///                             [0, MAX_SECONDS_TO_EXPIRY_FOR_ENTRY] and normalized
///                             to [0.0, 1.0].  0 = expiry, 1 = 4h+ away.
///  [13]  yes_obi_change  — change in yes_obi from previous tick
///  [14]  yes_mid_change  — change in YES mid-price ((bid+ask)/2) from previous tick.
///                          Captures Polymarket venue momentum independent of oracle.
///  [15]  no_obi_change   — change in no_obi from previous tick.
///                          Detecting YES/NO OBI divergence is a stronger signal than
///                          either change in isolation.
///  [16]  relative_depth_ratio — yes_bid_depth / (yes_bid_depth + no_bid_depth).
///                          Cross-token depth balance: which side has more buyers?
///                          [0.0 = all buyers on NO, 0.5 = balanced, 1.0 = all on YES]
///  [17]  combined_ask_spread — (yes_ask + no_ask − 1.0).
///                          Book efficiency / round-trip cost signal.
///                          Near 0 = tight efficient book; large = expensive or illiquid.
///  [18]  oracle_drift_10m — 10-minute oracle drift (÷ 10000).
///                          Fills the 5s–60m temporal gap where profitable binary moves
///                          actually develop.  Zero until 10 min of oracle history exists.
///  [19]  spread_velocity  — rate of change of the YES bid-ask spread (clamped [-1, +1]).
///                          Positive = spread widening (uncertainty rising, bad for entry).
///                          Negative = spread tightening (liquidity improving, good for entry).
///                          Orthogonal to feature [4]: level vs. momentum of the spread.
///  [20]  hist_vol_regime  — rolling volatility of oracle log-returns over the last 60
///                          history snapshots, normalized to [0, 1] (0 = calm, 1 = chaotic).
///                          2% per-tick log-return std-dev maps to 1.0 (extreme regime).
///                          NOTE: "60 snapshots" is a proxy for ~1h; actual wall-clock
///                          duration depends on tick rate at runtime.
///  [21]  tick_momentum    — net directionality of the last 10 YES bid ticks, normalized
///                          to [-1, +1] over (N−1) comparisons.
///                          +1 = all 9 ticks up (strong up momentum).
///                          −1 = all 9 ticks down (strong down momentum).
///  [22]  institutional_pulse — Tide Raptor volume-weighted z-score of spot-BTC-ETF
///                          premium/discount vs synthetic iNAV. >0 = institutions paying
///                          a premium (bullish). BTC-only; 0 for ETH/SOL or outside US hours.
///  [23]  tide_coherence   — agreement across IBIT/FBTC/ARKB premiums in [0, 1].
///                          High coherence + large |pulse| = institutional conviction.
///  [24]  oi_delta_pct     — Derivatives Raptor perp open-interest delta since last poll.
///                          >0 = positioning building, <0 = de-leveraging/squeeze. All-asset.
///  [25]  cvd_ratio        — taker buy÷sell volume ratio. >1 = buy aggression, <1 = sell
///                          aggression, 0 = no data (treated neutral). All-asset.
///  [26]  tradfi_velocity  — Horizon Raptor volume-weighted 5s momentum of SPY+QQQ ($).
///                          >0 = risk-on front-running, <0 = risk-off. 0 outside US hours.
///  [27]  macro_coherence  — 10m rolling Pearson correlation of QQQ vs BTC velocity in
///                          [-1, 1]. High = BTC trading as high-beta tech (TradFi
///                          features informative); ~0 = decoupled regime.
///  [28]  vix_proxy        — UVXY last price ÷ 100 (fear/volatility level). The model
///                          can learn regime interactions (e.g. mean-reversion works
///                          in low-VIX chop, fails in high-VIX trends).
///  [29]  vix_velocity     — UVXY 5s rate of change ($). Sharp positive spike = panic
///                          onset. 0 outside US hours / raptor absent.
///
/// ── Label ────────────────────────────────────────────────────────────────────
///   1.0  if the oracle (Binance) price is higher GBOOST_LABEL_HORIZON_SECS later
///   0.0  otherwise (flat-oracle samples below GBOOST_LABEL_MIN_ORACLE_MOVE_FRAC are skipped)
///
/// ── Lifecycle ────────────────────────────────────────────────────────────────
///   1. Snapshots are pushed into a fixed-size ring buffer every tick.
///   2. Every GBOOST_RETRAIN_EVERY_N ticks (once MIN_TRAINING_SAMPLES exist),
///      training is offloaded to `tokio::task::spawn_blocking` so the rayon
///      threadpool never blocks the Tokio executor.
///   3. The trained model is swapped into `Arc<Mutex<Option<PerpetualBooster>>>`.
///   4. predict_proba() produces P(YES_UP) for each new tick.
///   5. Model is serialized to GBOOST_MODEL_PATH after each successful retrain
///      and reloaded from disk on strategy construction.
///   6. While the `gboost_shadow_mode` knob is on, a signal that clears every
///      entry gate is shadow-logged as a veto ("shadow mode: would enter ...",
///      scored as its own gate on /api/gboost/veto-scores) and no order is
///      placed. It ships on, so a model whose live behavior has never been
///      observed cannot start trading real money by default.
///
/// ── Matrix layout (B38) ──────────────────────────────────────────────────────
///   `perpetual::Matrix::new(data, rows, cols)` is column major. Every multi-row
///   matrix here is built by `column_major_matrix_data`; the single-row
///   prediction matrix is the same in both layouts. Until 2026-09-06 the fills
///   were row major, so every model ever trained learned scrambled columns.
///   Persisted models carry a `feature_layout` metadata tag; a file without it
///   predates the fix and is discarded at load rather than predicted from.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use std::collections::{VecDeque, HashMap}; // Added HashMap
use std::sync::{Arc, Mutex as StdMutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use chrono::Utc;
use perpetual::{Matrix, PerpetualBooster};
use perpetual::objective::Objective;
use perpetual::booster::config::BoosterIO;
use perpetual::drift::calculate_drift as perpetual_calculate_drift;
use crate::venues::core::MarketId; // venue-neutral key for pending_entries / cooldowns

use crate::config;
use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{MarketSnapshot, OrderParams, StrategySignal, StrategyStatus};
use crate::state::PositionKey;
use crate::vipers::is_drawdown_limit_hit;
use crate::helpers::price::floor_to_tick_size;
use crate::venues::core::TimeInForce;

/// Number of f64 features per snapshot row fed into the booster.
const NUM_FEATURES: usize = 30;

/// Represents a single training sample for the Gboost model.
/// Contains the features at the time of entry and whether the trade was profitable.
///
/// Serializable because the lookahead label pool is persisted to `logs/`
/// (see [`LabelPoolFile`]); the `[f64; 30]` array is within serde's 32-element
/// array support, so no custom impl is needed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TrainingSample {
    pub features: [f64; NUM_FEATURES],
    pub is_profitable: bool, // Label: true if profitable, false if loss
    pub entry_timestamp: chrono::DateTime<chrono::Utc>, // For context/debugging
}

// ── Feature extraction ────────────────────────────────────────────────────────

/// Normalization divisor for secs_to_expiry: same as MAX_SECONDS_TO_EXPIRY_FOR_ENTRY (4 h).
/// Values beyond this horizon all map to 1.0; at expiry the value is 0.0.
const SECS_TO_EXPIRY_NORM: f64 = 14_400.0;

/// Compute OBI in [-1, 1] for a (bid_depth, ask_depth) pair, returning -1.0 when total=0.
///
/// This mirrors `GboostStrategyImpl::side_obi` exactly so that `extract_features` and
/// the entry gate use the SAME value for zero-depth books.  Previously `extract_features`
/// defaulted to 0.0 (neutral) while the gate defaulted to -1.0 (maximally adverse),
/// causing the model to be trained and predict on a different OBI convention than the one
/// used to block entries — a silent but systematic feature–gate mismatch.
#[inline]
fn obi_from_depths(bid: rust_decimal::Decimal, ask: rust_decimal::Decimal) -> f64 {
    let total = bid + ask;
    if total > dec!(0) {
        ((bid - ask) / total).to_f64().unwrap_or(-1.0)
    } else {
        -1.0 // no depth data → same as side_obi → maximally adverse
    }
}

/// Compute tick-direction momentum from a slice of YES bid prices.
/// Returns (up_ticks − down_ticks) / (n−1), normalized to [−1, +1].
fn compute_tick_momentum(bids: &[rust_decimal::Decimal]) -> f64 {
    if bids.len() < 2 {
        return 0.0;
    }
    let mut up_ticks = 0i32;
    let mut down_ticks = 0i32;
    for i in 1..bids.len() {
        if bids[i] > bids[i - 1] {
            up_ticks += 1;
        } else if bids[i] < bids[i - 1] {
            down_ticks += 1;
        }
    }
    let comparisons = (bids.len() - 1) as f64;
    (up_ticks as f64 - down_ticks as f64) / comparisons
}

/// Read the canonical 60-min realized-vol captured on the snapshot at position `idx`.
///
/// Previously this recomputed volatility from a 60-sample window of `oracle_price`.
/// At the 50ms patrol cadence that window spans only ~3 seconds and is dominated by
/// duplicate oracle prints (the Binance watch channel updates far slower than the
/// tick), collapsing log-returns to ~0 → a literal 0.0000 that permanently vetoed the
/// flatness gate.  The Price raptor now stamps each snapshot with a proper 60-minute
/// `hist_vol` (computed off the always-live Binance feed), so we simply read it.
fn hist_vol_from_deque(h: &VecDeque<MarketSnapshot>, idx: usize) -> f64 {
    h.get(idx).map(|s| s.hist_vol.to_f64().unwrap_or(0.0)).unwrap_or(0.0)
}

/// Compute tick_momentum from a position in the history VecDeque (looks back up to 10 snapshots).
fn tick_momentum_from_deque(h: &VecDeque<MarketSnapshot>, idx: usize) -> f64 {
    let start = idx.saturating_sub(9);
    let bids: Vec<rust_decimal::Decimal> = (start..=idx)
        .filter_map(|k| h.get(k))
        .map(|s| s.yes_bid)
        .collect();
    compute_tick_momentum(&bids)
}

/// Read the canonical 60-min realized-vol captured on the snapshot at position `idx`
/// (slice variant, used in the concept-drift / retrain reconstruction path).
fn hist_vol_from_slice(snaps: &[MarketSnapshot], idx: usize) -> f64 {
    snaps.get(idx).map(|s| s.hist_vol.to_f64().unwrap_or(0.0)).unwrap_or(0.0)
}

/// Compute tick_momentum from a position in a `&[MarketSnapshot]` slice.
fn tick_momentum_from_slice(snaps: &[MarketSnapshot], idx: usize) -> f64 {
    let start = idx.saturating_sub(9);
    let bids: Vec<rust_decimal::Decimal> = snaps[start..=idx]
        .iter()
        .map(|s| s.yes_bid)
        .collect();
    compute_tick_momentum(&bids)
}

/// Convert a `MarketSnapshot` into a fixed-length `f64` feature array.
fn extract_features(s: &MarketSnapshot, prev_s: Option<&MarketSnapshot>, hist_vol: f64, tick_momentum: f64) -> [f64; NUM_FEATURES] {
    // obi_from_depths returns -1.0 on zero depth as a stable FEATURE convention (the
    // model needs some numeric value for a zero-depth book).  Note: the entry gates no
    // longer share this convention — they consume an EMA-smoothed OBI (smoothed_obi())
    // where zero depth means "unknown" and falls back to the last fresh EMA value.
    let yes_obi = obi_from_depths(s.yes_bid_depth, s.yes_ask_depth);
    let no_obi  = obi_from_depths(s.no_bid_depth,  s.no_ask_depth);

    // [13] yes_obi_change
    let yes_obi_change = if let Some(prev) = prev_s {
        let prev_yes_total = prev.yes_bid_depth + prev.yes_ask_depth;
        let prev_yes_obi = if prev_yes_total > dec!(0) {
            ((prev.yes_bid_depth - prev.yes_ask_depth) / prev_yes_total).to_f64().unwrap_or(0.0)
        } else { 0.0 };
        yes_obi - prev_yes_obi
    } else {
        0.0
    };

    // [14] yes_mid_change — Polymarket venue price momentum, independent of oracle.
    // When the YES mid-price ticks up, market makers are repricing YES higher.
    let yes_mid = (s.yes_bid.to_f64().unwrap_or(0.5) + s.yes_ask.to_f64().unwrap_or(0.5)) / 2.0;
    let yes_mid_change = if let Some(prev) = prev_s {
        let prev_mid = (prev.yes_bid.to_f64().unwrap_or(0.5) + prev.yes_ask.to_f64().unwrap_or(0.5)) / 2.0;
        yes_mid - prev_mid
    } else {
        0.0
    };

    // [15] no_obi_change — symmetric to yes_obi_change.
    // YES/NO OBI divergence (one rising, other falling) is a stronger signal than either alone.
    let no_obi_change = if let Some(prev) = prev_s {
        let prev_no_total = prev.no_bid_depth + prev.no_ask_depth;
        let prev_no_obi = if prev_no_total > dec!(0) {
            ((prev.no_bid_depth - prev.no_ask_depth) / prev_no_total).to_f64().unwrap_or(0.0)
        } else { 0.0 };
        no_obi - prev_no_obi
    } else {
        0.0
    };

    // [16] relative_depth_ratio — cross-token depth balance [0, 1].
    // 0.5 = balanced; > 0.5 = more buyers on YES side; < 0.5 = more buyers on NO side.
    let yes_bid_d = s.yes_bid_depth.to_f64().unwrap_or(0.0);
    let no_bid_d  = s.no_bid_depth.to_f64().unwrap_or(0.0);
    let total_bid_d = yes_bid_d + no_bid_d;
    let relative_depth_ratio = if total_bid_d > 0.0 { yes_bid_d / total_bid_d } else { 0.5 };

    // [17] combined_ask_spread — (yes_ask + no_ask - 1.0).
    // Near 0 = tight efficient book (cheap to enter); > 0 = expensive/illiquid.
    let combined_ask_spread = (s.yes_ask + s.no_ask - dec!(1.0)).to_f64().unwrap_or(0.0);

    // [18] oracle_drift_10m — medium-term oracle momentum (÷ 10000, same scale as drift_60m).
    // Fills the 5s–60m temporal gap where real binary directional moves develop.
    let oracle_drift_10m = s.oracle_drift_10m.to_f64().unwrap_or(0.0) / 10_000.0;

    // [19] spread_velocity — rate of change of YES bid-ask spread, clamped to [-1, +1].
    // Positive = spread widening (uncertainty rising, bad for entry).
    // Negative = spread tightening (liquidity improving, good for entry).
    // Orthogonal to feature [4]: level vs. momentum.
    let spread_now = (s.yes_ask - s.yes_bid).to_f64().unwrap_or(0.01);
    let spread_velocity = if let Some(prev) = prev_s {
        let spread_prev = (prev.yes_ask - prev.yes_bid).to_f64().unwrap_or(0.01);
        if spread_prev > 0.0 {
            let raw_vel = (spread_now - spread_prev) / spread_prev;
            raw_vel.max(-1.0).min(1.0)
        } else {
            0.0
        }
    } else {
        0.0
    };

    // Normalize secs_to_expiry to [0.0, 1.0]:
    //   0.0 = market has expired / about to expire
    //   1.0 = 4 hours or more until expiry (fully safe zone)
    let secs_to_expiry_norm = (s.secs_to_expiry.max(0) as f64)
        .min(SECS_TO_EXPIRY_NORM)
        / SECS_TO_EXPIRY_NORM;

    [
        yes_obi,                                                    // [0]
        no_obi,                                                     // [1]
        s.yes_ask.to_f64().unwrap_or(0.5),                         // [2]
        s.no_ask.to_f64().unwrap_or(0.5),                          // [3]
        (s.yes_ask - s.yes_bid).to_f64().unwrap_or(0.0),           // [4]
        (s.no_ask  - s.no_bid ).to_f64().unwrap_or(0.0),           // [5]
        s.velocity.to_f64().unwrap_or(0.0)          / 1_000.0,     // [6]
        s.velocity_1s.to_f64().unwrap_or(0.0)       / 1_000.0,     // [7]
        s.acceleration.to_f64().unwrap_or(0.0)      / 1_000.0,     // [8]
        s.funding_rate.to_f64().unwrap_or(0.0),                    // [9]
        s.oracle_drift_60m.to_f64().unwrap_or(0.0)  / 10_000.0,   // [10]
        s.oracle_price.to_f64().unwrap_or(70_000.0) / 100_000.0,  // [11]
        secs_to_expiry_norm,                                        // [12]
        yes_obi_change,                                             // [13]
        yes_mid_change,                                             // [14]
        no_obi_change,                                              // [15]
        relative_depth_ratio,                                       // [16]
        combined_ask_spread,                                        // [17]
        oracle_drift_10m,                                           // [18]
        spread_velocity,                                            // [19] NEW: spread momentum
        hist_vol,                                                   // [20] NEW: volatility regime
        tick_momentum,                                              // [21] NEW: tick direction momentum
        s.institutional_pulse.to_f64().unwrap_or(0.0),             // [22] NEW: institutional pulse (BTC ETF tide z-score)
        s.tide_coherence.to_f64().unwrap_or(0.0),                  // [23] NEW: tide coherence (ETF agreement, 0..1)
        s.oi_delta_pct.to_f64().unwrap_or(0.0),                    // [24] NEW: perp open-interest delta (positioning build/unwind)
        s.cvd_ratio.to_f64().unwrap_or(0.0),                       // [25] NEW: taker buy/sell ratio (aggression; 1.0-centred, 0=no data)
        s.tradfi_velocity.to_f64().unwrap_or(0.0),                 // [26] NEW: Horizon SPY+QQQ 5s momentum ($; 0 off-hours)
        s.macro_coherence.to_f64().unwrap_or(0.0),                 // [27] NEW: Horizon QQQ↔BTC 10m correlation [-1,1]
        s.vix_proxy.to_f64().unwrap_or(0.0)         / 100.0,       // [28] NEW: Horizon UVXY level ÷ 100 (vol regime)
        s.vix_velocity.to_f64().unwrap_or(0.0),                    // [29] NEW: Horizon UVXY 5s velocity ($; panic onset)
    ]
}

// ── Training helper (runs inside spawn_blocking) ──────────────────────────────

/// Build and train a fresh `PerpetualBooster` from a slice of `TrainingSample`s.
/// Called exclusively from `tokio::task::spawn_blocking` — never on an async thread.
///
/// Lay `rows` out the way `perpetual::Matrix::new` reads them.
///
/// `Matrix::new(data, rows, cols)` is column major: perpetual 3.0.0-rc.2's
/// `data.rs` sets `stride1: rows, stride2: 1`, so element (i, j) lives at
/// `data[j * rows + i]`. Every fill in this file used to append each sample's
/// features consecutively (row major), so the booster's "column 0" was the first
/// `n` numbers of the stream, samples 0..n/30 interleaved, and every column was
/// scrambled against its label (B38). `matrix_fill_matches_perpetual_column_major_layout`
/// pins this against perpetual's own accessor.
fn column_major_matrix_data(rows: &[[f64; NUM_FEATURES]]) -> Vec<f64> {
    let n = rows.len();
    let mut data = vec![0.0f64; n * NUM_FEATURES];
    for (i, row) in rows.iter().enumerate() {
        for (j, value) in row.iter().enumerate() {
            data[j * n + i] = *value;
        }
    }
    data
}

/// Metadata tag written into every model this build trains and required of every
/// model it loads. Files without it were fit before B38, on a row-major buffer
/// read as column major, and their predictions are noise; they are discarded at
/// startup rather than predicted from, until the first accepted retrain.
const MODEL_META_LAYOUT_KEY: &str = "feature_layout";
const MODEL_META_LAYOUT_VALUE: &str = "column_major";

/// True when `booster` carries the layout tag this build writes.
fn model_has_current_layout(booster: &PerpetualBooster) -> bool {
    booster.get_metadata(&MODEL_META_LAYOUT_KEY.to_string()).as_deref() == Some(MODEL_META_LAYOUT_VALUE)
}

/// When `booster` was accepted, in US/Eastern for the log, or a note that it
/// carries no acceptance record (a model written before the holdout test).
fn model_accepted_at_et(booster: &PerpetualBooster) -> String {
    booster.get_metadata(&MODEL_META_ACCEPTED_AT_KEY.to_string())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&chrono_tz::US::Eastern).format("%H:%M %Z").to_string())
        .unwrap_or_else(|| "before the holdout test existed".to_string())
}

/// `budget` and `iteration_limit` are passed in rather than read from `config`
/// so they can be tuned live from the Control Tower: this runs on a blocking
/// thread with no access to the dynamic config, so the caller reads them off its
/// own `dc` snapshot and hands them over.
fn train_model(
    samples: Vec<TrainingSample>,
    budget: f32,
    iteration_limit: u32,
) -> Result<PerpetualBooster> {
    let n = samples.len();

    if n < config::GBOOST_MIN_TRAINING_SAMPLES {
        return Err(anyhow::anyhow!(
            "GBoost: too few training samples ({}) for training (need at least {})", n, config::GBOOST_MIN_TRAINING_SAMPLES
        ));
    }
    train_model_unchecked(samples, budget, iteration_limit)
}

/// `train_model` without the sample-count floor. The production path never
/// calls this directly; the evidence harness uses it to fit decimated pools.
fn train_model_unchecked(
    samples: Vec<TrainingSample>,
    budget: f32,
    iteration_limit: u32,
) -> Result<PerpetualBooster> {
    let n = samples.len();
    if n == 0 {
        return Err(anyhow::anyhow!("GBoost: no training samples"));
    }

    let mut rows: Vec<[f64; NUM_FEATURES]> = Vec::with_capacity(n);
    let mut labels: Vec<f64>               = Vec::with_capacity(n);

    for sample in samples {
        rows.push(sample.features);
        // Label: 1.0 if profitable, 0.0 if not profitable
        labels.push(if sample.is_profitable { 1.0 } else { 0.0 });
    }

    // Column major, the layout Matrix::new reads (B38). Matrix<'a, T> borrows
    // the slice; both Vec and Matrix live in this closure scope.
    let feature_data = column_major_matrix_data(&rows);
    let matrix = Matrix::new(&feature_data, n, NUM_FEATURES);

    let mut booster = PerpetualBooster::default()
        .set_objective(Objective::LogLoss)
        .set_budget(budget)
        .set_num_threads(Some(config::GBOOST_NUM_THREADS as usize))
        // Suppresses perpetual's own stdout progress lines. It does NOT silence
        // the crate's `tracing` output — that is filtered at the subscriber in
        // main.rs, which is where its "iteration cap" WARN was reaching operator
        // logs from.
        .set_log_iterations(0)
        .set_max_bin(63)        // 63 bins is fast and sufficient for these features
        .set_iteration_limit(Some(iteration_limit as usize))
        .set_stopping_rounds(None)
        .set_save_node_stats(true); // Required for drift detection via perpetual::drift

    booster.fit(&matrix, &labels, None, None)
        .map_err(|e| anyhow::anyhow!("perpetual fit error: {:?}", e))?;
    // Provenance: fit on a correctly laid out matrix. The startup loader refuses
    // persisted models without this tag (see MODEL_META_LAYOUT_KEY).
    booster.insert_metadata(MODEL_META_LAYOUT_KEY.to_string(), MODEL_META_LAYOUT_VALUE.to_string());

    Ok(booster)
}

// ── Retrain acceptance (B37) ─────────────────────────────────────────────────
//
// A retrain used to be accepted on its tree count alone (`GBOOST_MIN_USABLE_TREES`,
// 20 to 30 depending on profile). Tree count is a byproduct of how much loss
// structure the data offers at the booster's learning rate, not a quality
// signal: over the production pool corr(trees, holdout AUC) was -0.33, and the
// floor bisected the 22..29 range most fits land in, so adoption was a coin
// flip on sampling noise. The floor also rejected every correctly laid out
// model (B38 fits ~26 trees on a full pool), which would have left GBoost idle.
//
// What replaces it: a time-ordered holdout. The newest HOLDOUT_FRACTION of the
// pool is set aside, a purge gap wider than the label horizon separates it from
// the training split, a validation model is fit on the older part and scored
// on the newest part against the best constant forecast. The retrain is
// adopted only when its logloss skill clears `gboost_holdout_min_skill`; the
// adopted model is then refit on the whole pool so it also knows the newest
// window. A small structural floor (`gboost_structural_min_trees`) still
// catches the fit that stops at a single stump, which is the case the old
// guard was written for.

/// Fraction of the pool, newest first, held out from the validation fit. On a
/// full conservative pool (8000 samples at ~1 s spacing) this is ~40 minutes,
/// several label horizons of independent outcomes. Mechanism, not a knob: the
/// thresholds applied to what it measures are the knobs.
const HOLDOUT_FRACTION: f64 = 0.10;
/// Below this many holdout samples the skill score is a handful of label runs
/// and says nothing; the retrain is deferred until the pool is longer.
const HOLDOUT_MIN_SAMPLES: usize = 100;
/// Probabilities are clamped away from 0 and 1 before the log so one
/// saturated wrong call costs ~13.8 nats rather than infinity. Overconfidence
/// is still punished hard, which is the point: the model gates entries on
/// P(UP) beyond 0.72, so confident wrong calls are the expensive kind.
const LOGLOSS_EPS: f64 = 1e-6;

/// Metadata keys written into an accepted model so a later process can see, in
/// the startup log, what the model cleared when it was adopted.
const MODEL_META_HOLDOUT_SKILL_KEY: &str = "holdout_skill";
const MODEL_META_HOLDOUT_N_KEY: &str = "holdout_samples";
const MODEL_META_ACCEPTED_AT_KEY: &str = "accepted_at";

/// Purge gap between the last training sample and the first holdout sample.
///
/// A training sample at `t` carries a label settled at `t + horizon` (more
/// precisely at the first snapshot at or after that instant). Any training
/// sample whose label window reaches past the first holdout timestamp shares
/// its outcome with the holdout, so the holdout would be scoring the model on
/// moves it was shown. One horizon closes that; the second covers the
/// snapshot-spacing slack in the labeler and leaves margin. Real trade
/// outcomes (`training_data`) are labeled at exit, which has no bound; they
/// are rare and the margin is documented rather than sized for them.
fn holdout_gap() -> chrono::Duration {
    chrono::Duration::seconds(2 * config::GBOOST_LABEL_HORIZON_SECS)
}

/// The pool cut for validation: everything older than the gap trains, the
/// newest window scores.
struct HoldoutSplit {
    train: Vec<TrainingSample>,
    holdout: Vec<TrainingSample>,
    /// Samples between the two, discarded because their label horizon reaches
    /// into the holdout.
    gap_dropped: usize,
}

/// Cut `sorted` (ascending `entry_timestamp`) so that `sorted[start..start+len]`
/// is the holdout and everything before `start` whose timestamp is at least
/// `gap` before the holdout's first timestamp is the training split. Samples
/// after the holdout are ignored, which lets a walk-forward evaluation reuse
/// this on interior windows.
fn split_for_holdout(sorted: &[TrainingSample], start: usize, len: usize, gap: chrono::Duration) -> HoldoutSplit {
    let end = (start + len).min(sorted.len());
    let holdout: Vec<TrainingSample> = sorted[start..end].to_vec();
    let cutoff = holdout.first().map(|s| s.entry_timestamp - gap);
    let mut train: Vec<TrainingSample> = Vec::with_capacity(start);
    let mut gap_dropped = 0usize;
    for s in &sorted[..start] {
        match cutoff {
            Some(c) if s.entry_timestamp > c => gap_dropped += 1,
            _ => train.push(s.clone()),
        }
    }
    HoldoutSplit { train, holdout, gap_dropped }
}

/// What the validation model did on the holdout. Everything the acceptance
/// decision and the log line need, in one place.
#[derive(Debug, Clone)]
struct HoldoutReport {
    train_n: usize,
    holdout_n: usize,
    gap_dropped: usize,
    gap_secs: i64,
    holdout_span_secs: i64,
    /// Trees in the validation fit (not the adopted refit).
    trees: usize,
    train_pos_rate: f64,
    holdout_pos_rate: f64,
    /// Logloss of the best constant forecast on the holdout: the holdout's own
    /// positive rate, which no real constant could know in advance. It is the
    /// strictest constant baseline, so "skill above zero" means the model beat
    /// even a hindsight-optimal coin.
    base_logloss: f64,
    model_logloss: f64,
    /// `1 - model_logloss / base_logloss`. Zero is the constant forecast,
    /// positive is better, negative is worse (usually overconfidence).
    skill: f64,
    auc: f64,
    /// Holdout calls where P(UP) was beyond `confident_threshold` either way,
    /// and how many were right: the calls the entry gate would act on.
    confident_n: usize,
    confident_hits: usize,
}

impl HoldoutReport {
    /// One-line summary shared by the accept and reject log lines.
    fn summary(&self) -> String {
        format!(
            "holdout skill {:+.1}% (logloss {:.3} vs {:.3} for the best constant; {} newest samples over {} min, \
             {:.0}% up vs {:.0}% in training; {} s gap, {} dropped in the gap; AUC {:.2}; {}/{} confident calls right; \
             {} trees on {} training samples)",
            self.skill * 100.0, self.model_logloss, self.base_logloss,
            self.holdout_n, self.holdout_span_secs / 60,
            self.holdout_pos_rate * 100.0, self.train_pos_rate * 100.0,
            self.gap_secs, self.gap_dropped,
            self.auc, self.confident_hits, self.confident_n, self.trees, self.train_n,
        )
    }
}

/// Binary logloss with clamped probabilities (see `LOGLOSS_EPS`).
fn logloss(probs: &[f64], labels: &[bool]) -> f64 {
    let n = probs.len().max(1) as f64;
    probs.iter().zip(labels).map(|(p, y)| {
        let p = p.clamp(LOGLOSS_EPS, 1.0 - LOGLOSS_EPS);
        if *y { -p.ln() } else { -(1.0 - p).ln() }
    }).sum::<f64>() / n
}

/// Rank-based AUC (Mann-Whitney); 0.5 when either class is absent.
fn rank_auc(scores: &[f64], labels: &[bool]) -> f64 {
    let npos = labels.iter().filter(|l| **l).count() as f64;
    let nneg = labels.len() as f64 - npos;
    if npos == 0.0 || nneg == 0.0 { return 0.5; }
    let mut idx: Vec<usize> = (0..scores.len()).collect();
    idx.sort_by(|a, b| scores[*a].partial_cmp(&scores[*b]).unwrap_or(std::cmp::Ordering::Equal));
    let rank_sum: f64 = idx.iter().enumerate()
        .filter(|(_, i)| labels[**i])
        .map(|(r, _)| (r + 1) as f64)
        .sum();
    (rank_sum - npos * (npos + 1.0) / 2.0) / (npos * nneg)
}

/// Score `model` on the holdout of `split`.
fn evaluate_holdout(
    model: &PerpetualBooster,
    split: &HoldoutSplit,
    gap: chrono::Duration,
    confident_threshold: f64,
) -> HoldoutReport {
    let rows: Vec<[f64; NUM_FEATURES]> = split.holdout.iter().map(|s| s.features).collect();
    let labels: Vec<bool> = split.holdout.iter().map(|s| s.is_profitable).collect();
    let data = column_major_matrix_data(&rows);
    let probs = model.predict_proba(&Matrix::new(&data, rows.len(), NUM_FEATURES), false, false);

    let holdout_n = labels.len();
    let holdout_pos = labels.iter().filter(|l| **l).count() as f64;
    let holdout_pos_rate = holdout_pos / holdout_n.max(1) as f64;
    let train_pos = split.train.iter().filter(|s| s.is_profitable).count() as f64;
    let train_pos_rate = train_pos / split.train.len().max(1) as f64;

    let base_probs = vec![holdout_pos_rate; holdout_n];
    let base_logloss = logloss(&base_probs, &labels);
    let model_logloss = logloss(&probs, &labels);
    let skill = if base_logloss > 0.0 { 1.0 - model_logloss / base_logloss } else { 0.0 };

    let margin = (confident_threshold - 0.5).abs();
    let (mut confident_n, mut confident_hits) = (0usize, 0usize);
    for (p, y) in probs.iter().zip(&labels) {
        if (p - 0.5).abs() >= margin {
            confident_n += 1;
            if (*p >= 0.5) == *y { confident_hits += 1; }
        }
    }
    let holdout_span_secs = match (split.holdout.first(), split.holdout.last()) {
        (Some(a), Some(b)) => (b.entry_timestamp - a.entry_timestamp).num_seconds(),
        _ => 0,
    };

    HoldoutReport {
        train_n: split.train.len(),
        holdout_n,
        gap_dropped: split.gap_dropped,
        gap_secs: gap.num_seconds(),
        holdout_span_secs,
        trees: model.trees.len(),
        train_pos_rate,
        holdout_pos_rate,
        base_logloss,
        model_logloss,
        skill,
        auc: rank_auc(&probs, &labels),
        confident_n,
        confident_hits,
    }
}

/// Why a retrain was not adopted. Each variant carries what its log line
/// needs, so the operator sees the specific reason rather than "degenerate".
#[derive(Debug)]
enum RetrainRejection {
    /// The pool cannot be cut into a training split of at least
    /// `GBOOST_MIN_TRAINING_SAMPLES` plus a `HOLDOUT_MIN_SAMPLES` holdout with
    /// the purge gap between them. Not a fault: labeling continues and the next
    /// cycle retries. Distinct from the other two because nothing was fit.
    PoolTooShort { total: usize, train_n: usize, holdout_n: usize, gap_dropped: usize, gap_secs: i64 },
    /// The fit stopped below the structural floor: nothing to learn in the
    /// labels (homogeneous window, frozen features).
    Structural { trees: usize, floor: usize, stage: &'static str },
    /// The validation model scored below the skill floor on the newest window.
    NoSkill { report: HoldoutReport, min_skill: f64 },
}

impl RetrainRejection {
    fn describe(&self) -> String {
        match self {
            Self::PoolTooShort { total, train_n, holdout_n, gap_dropped, gap_secs } => format!(
                "pool too short to validate: {} samples cut into {} for training, {} in the {} s gap and {} held out \
                 (needs {} to train and {} to hold out); labeling continues",
                total, train_n, gap_dropped, gap_secs, holdout_n,
                config::GBOOST_MIN_TRAINING_SAMPLES, HOLDOUT_MIN_SAMPLES,
            ),
            Self::Structural { trees, floor, stage } => format!(
                "{} fit stopped at {} tree{} (structural floor {}): the labels offered no structure to learn",
                stage, trees, if *trees == 1 { "" } else { "s" }, floor,
            ),
            Self::NoSkill { report, min_skill } => format!(
                "{} — below the {:+.1}% floor, the fit does not generalize to the newest window",
                report.summary(), min_skill * 100.0,
            ),
        }
    }
}

/// Outcome of one retrain attempt.
enum RetrainVerdict {
    Accepted {
        /// Refit on the whole pool after the validation fit passed.
        model: PerpetualBooster,
        report: HoldoutReport,
    },
    Rejected(RetrainRejection),
}

/// Fit, validate on the newest window, and refit on everything if it passes.
/// Runs on a blocking thread; every threshold is passed in from the caller's
/// `DynamicConfig` snapshot, and `gap` is `holdout_gap()` in production (a
/// parameter so a pool labeled at another horizon can be evaluated offline).
/// `samples` need not be sorted.
fn train_and_validate(
    mut samples: Vec<TrainingSample>,
    budget: f32,
    iteration_limit: u32,
    structural_min_trees: usize,
    min_skill: f64,
    confident_threshold: f64,
    gap: chrono::Duration,
) -> Result<RetrainVerdict> {
    samples.sort_by_key(|s| s.entry_timestamp);
    let total = samples.len();
    let holdout_len = ((total as f64 * HOLDOUT_FRACTION).round() as usize).max(HOLDOUT_MIN_SAMPLES).min(total);
    let start = total - holdout_len;
    let split = split_for_holdout(&samples, start, holdout_len, gap);

    if split.train.len() < config::GBOOST_MIN_TRAINING_SAMPLES || split.holdout.len() < HOLDOUT_MIN_SAMPLES {
        return Ok(RetrainVerdict::Rejected(RetrainRejection::PoolTooShort {
            total,
            train_n: split.train.len(),
            holdout_n: split.holdout.len(),
            gap_dropped: split.gap_dropped,
            gap_secs: gap.num_seconds(),
        }));
    }

    let validation = train_model(split.train.clone(), budget, iteration_limit)?;
    if validation.trees.len() < structural_min_trees {
        return Ok(RetrainVerdict::Rejected(RetrainRejection::Structural {
            trees: validation.trees.len(), floor: structural_min_trees, stage: "validation",
        }));
    }
    let report = evaluate_holdout(&validation, &split, gap, confident_threshold);
    if !(report.skill >= min_skill) {
        return Ok(RetrainVerdict::Rejected(RetrainRejection::NoSkill { report, min_skill }));
    }

    let mut model = train_model(samples, budget, iteration_limit)?;
    if model.trees.len() < structural_min_trees {
        return Ok(RetrainVerdict::Rejected(RetrainRejection::Structural {
            trees: model.trees.len(), floor: structural_min_trees, stage: "full-pool",
        }));
    }
    model.insert_metadata(MODEL_META_HOLDOUT_SKILL_KEY.to_string(), format!("{:.4}", report.skill));
    model.insert_metadata(MODEL_META_HOLDOUT_N_KEY.to_string(), report.holdout_n.to_string());
    model.insert_metadata(MODEL_META_ACCEPTED_AT_KEY.to_string(), Utc::now().to_rfc3339());
    Ok(RetrainVerdict::Accepted { model, report })
}

// ── Concept drift helper (runs inside spawn_blocking) ────────────────────────

/// Evaluate concept drift of a freshly-trained `booster` against a slice of recent
/// market snapshots.
///
/// Uses `perpetual::drift::calculate_drift(..., "concept")` which aggregates chi-squared
/// statistics at leaf-parent tree nodes — comparing the flow of live data through the
/// learned split points against the training-time distribution saved in each node.
///
/// Returns 0.0 if there are fewer than 10 snapshots (not enough data for a meaningful
/// chi-squared estimate).  Requires the booster to have been trained with
/// `save_node_stats = true`.
fn compute_concept_drift(booster: &PerpetualBooster, recent_history: &[MarketSnapshot]) -> f32 {
    let n = recent_history.len();
    if n < 10 {
        return 0.0;
    }
    let rows: Vec<[f64; NUM_FEATURES]> = recent_history.iter().enumerate().map(|(i, snap)| {
        let prev = if i > 0 { Some(&recent_history[i - 1]) } else { None };
        let hv = hist_vol_from_slice(recent_history, i);
        let tm = tick_momentum_from_slice(recent_history, i);
        extract_features(snap, prev, hv, tm)
    }).collect();
    // Column major (B38): the drift statistic reads the same layout the fit did.
    let feature_data = column_major_matrix_data(&rows);
    let matrix = Matrix::new(&feature_data, n, NUM_FEATURES);
    perpetual_calculate_drift(booster, &matrix, "concept", false)
}

// ── Strategy struct ───────────────────────────────────────────────────────────

pub struct GboostStrategyImpl {
    /// Trained booster. `std::sync::Mutex` (not tokio) because we never hold
    /// it across an `.await` — only for quick read/write of the model pointer.
    model: Arc<StdMutex<Option<PerpetualBooster>>>,
    /// Ring buffer of recent market snapshots for feature engineering and labeling.
    history: Arc<StdMutex<VecDeque<MarketSnapshot>>>,
    /// Ticks accumulated since the last retrain trigger.
    ticks_since_retrain: Arc<StdMutex<usize>>,
    /// Set to `true` while a background training task is running.
    is_training: Arc<AtomicBool>,
    /// Stores completed trade outcomes (features + profitability) for training.
    training_data: Arc<StdMutex<VecDeque<TrainingSample>>>,
    /// Stores entry snapshots, the previous snapshot (for accurate feature reconstruction),
    /// entry prices, and the hist_vol + tick_momentum values computed at entry time, for
    /// trades that are currently open (ghost mode).
    /// Storing prev_snap at entry time (not exit time) is critical: `record_training_outcome_on_exit`
    /// used to grab `h[len-2]` at exit time — a minutes-stale prev snapshot paired with the entry
    /// snapshot produces a corrupted feature vector and degraded training labels.
    /// hist_vol and tick_momentum are similarly captured at entry time so the training label
    /// features exactly match what the model saw when it made the prediction.
    pending_entries: Arc<StdMutex<HashMap<MarketId, (MarketSnapshot, Option<MarketSnapshot>, rust_decimal::Decimal, f64, f64)>>>,
    /// Per-token (start_time, cooldown_secs) of the last emitted exit signal.
    /// TP/SignalRev exits store GBOOST_POST_EXIT_COOLDOWN_SECS; SL exits store
    /// GBOOST_SL_POST_EXIT_COOLDOWN_SECS (longer, because an SL means the market
    /// moved adversely and re-entering quickly compounds the loss).
    post_exit_cooldowns: Arc<StdMutex<HashMap<MarketId, (chrono::DateTime<chrono::Utc>, i64)>>>,
    /// Count of consecutive rejected retrains (structural floor or holdout skill,
    /// see `RetrainRejection`). Used to apply exponential backoff so a 10-second
    /// retrain storm doesn't burn CPU for 110+ minutes as seen in the 2026-05-07
    /// evening session.
    consecutive_degenerate: Arc<StdMutex<usize>>,
    /// When set, `maybe_retrain` skips all retrain attempts until this instant passes.
    retrain_backoff_until: Arc<StdMutex<Option<Instant>>>,
    /// `Instant` of the last SUCCESSFUL retrain trigger.  Enforces a hard wall-clock
    /// floor (GBOOST_MIN_RETRAIN_INTERVAL_SECS) between CPU-bound retrains so a fast tick
    /// rate cannot storm booster.fit() every ~30s and starve the runtime (watchdog stalls,
    /// observed 2026-07-07).  Unlike retrain_backoff_until this applies to HEALTHY retrains
    /// too, not just degenerate ones.
    last_retrain_at: Arc<StdMutex<Option<Instant>>>,
    /// Throttle for the once-per-interval INFO status line explaining WHY retrain
    /// cycles are aborting (cold-start visibility).  Without this the abort paths
    /// are silent (debug-only) and a stalled bootstrap is invisible in prod logs.
    last_retrain_status_log: Arc<StdMutex<Option<Instant>>>,
    /// When set, records the `Instant` at which BTC spot first dropped below
    /// (daily_strike − BASIS_BTC_ORACLE_STRIKE_BUFFER).  Resets to None whenever spot
    /// recovers above the threshold.  Used to suppress YES entries on daily markets when
    /// BTC has been continuously below the strike buffer for ≥ GBOOST_BELOW_STRIKE_SUPPRESS_SECS.
    below_strike_since: Arc<StdMutex<Option<Instant>>>,
    /// Set to `true` after TWO CONSECUTIVE retrains where concept drift exceeded
    /// GBOOST_CONCEPT_DRIFT_THRESHOLD.  A single spike is not suppressed — it could
    /// be a transient liquidity shock.  Two in a row implies a genuine regime change.
    /// Cleared only after GBOOST_DRIFT_STABLE_CLEAR_REQUIRED consecutive below-threshold
    /// retrains confirm the regime has been genuinely recaptured.
    concept_drift_suppressed: Arc<AtomicBool>,
    /// Most recent concept drift score from `perpetual::drift::calculate_drift`.
    /// Logged at DEBUG level in the entry gate; exposed here for diagnostics.
    last_concept_drift_score: Arc<StdMutex<f32>>,
    /// Count of consecutive retrains where drift_score > GBOOST_CONCEPT_DRIFT_THRESHOLD.
    /// Suppression only activates when this reaches GBOOST_DRIFT_CONSECUTIVE_REQUIRED.
    /// Resets to 0 on any below-threshold retrain.
    consecutive_drift_above_threshold: Arc<StdMutex<usize>>,
    /// Count of consecutive retrains where drift_score ≤ GBOOST_CONCEPT_DRIFT_THRESHOLD.
    /// Suppression is only CLEARED when this reaches GBOOST_DRIFT_STABLE_CLEAR_REQUIRED.
    /// Prevents a single below-threshold "blink" from unlocking entries mid regime-change.
    consecutive_stable_retrains: Arc<StdMutex<usize>>,
    /// Market-level entry hold lock: keyed by condition_id, stores the Instant when the
    /// last entry (YES or NO) was placed on that market.  Prevents rapid signal-flip chop
    /// where the model enters YES, exits quickly, then immediately enters NO (or vice versa).
    /// Both YES and NO entries check this lock; it is set whenever either fires.
    market_hold_locks: Arc<StdMutex<std::collections::HashMap<String, Instant>>>,
    /// Throttle for the prediction-confidence diagnostic log (INFO).
    /// GBoost rarely trades, so we log the model's peak conviction periodically to
    /// reveal how close it gets to the entry threshold — calibration visibility when
    /// no entry fires. Rate-limited to GBOOST_PRED_LOG_INTERVAL_SECS.
    last_pred_log_at: Arc<StdMutex<Option<Instant>>>,
    /// Throttle for the eligible-but-vetoed diagnostic log (INFO). When a signal
    /// clears the entry threshold but a downstream quality gate rejects it, we log
    /// WHICH gate — rate-limited to GBOOST_PRED_LOG_INTERVAL_SECS so the per-tick
    /// eval loop never floods. This reveals why GBoost isn't trading despite the
    /// model being confident.
    last_veto_log_at: Arc<StdMutex<Option<Instant>>>,
    /// Entry-signal persistence streak (anti-whipsaw, 2026-08-05):
    /// (condition_id, is_yes_side, first_seen, last_seen). The model can flip
    /// P(UP) between 1.000 and 0.000 within minutes (observed 10:30→10:34→10:46
    /// on 2026-08-05); requiring the side-eligibility to hold continuously for
    /// GBOOST_ENTRY_PERSISTENCE_SECS filters momentum flicker from real signal.
    /// A side flip or a sighting gap > GBOOST_SIGNAL_CONTINUITY_GAP_SECS resets.
    entry_signal_streak: Arc<StdMutex<Option<(String, bool, Instant, Instant)>>>,
    /// EMA state for the OBI quality gates, one slot per (venue, side) — see
    /// OBI_SLOT_* constants. Each slot stores (ema_value, last_update_instant).
    /// Smoothing rationale: instantaneous OBI on thin books is quote-flicker noise
    /// (observed ±0.9 swings minute-to-minute on 2026-07-14, vetoing every eligible
    /// signal); the EMA captures the persistent book lean the gates are meant to read.
    obi_ema: Arc<StdMutex<[Option<(f64, Instant)>; 4]>>,
}

/// ── Process-global GBoost state (survives market rotations) ─────────────────
/// The patrol loop rebuilds all strategy objects on every hourly market rotation
/// (`create_all_strategies()`), which used to wipe the snapshot history, the real
/// trade-outcome buffer, and the trained model — GBoost restarted its bootstrap
/// from zero every hour.  In a flat regime the ~18-min history window can never
/// produce GBOOST_MIN_TRAINING_SAMPLES deadband-surviving labels on its own, so
/// the model NEVER trained (observed 2026-07-16: 17.5h of retrain triggers, zero
/// trains).  These globals follow the same pattern as maker_market_first_seen().
/// One crypto per process is already assumed (CRYPTO_FILTER namespaces the model
/// file), so a single global per state item is safe.
fn gboost_shared_model() -> Arc<StdMutex<Option<PerpetualBooster>>> {
    static REG: std::sync::OnceLock<Arc<StdMutex<Option<PerpetualBooster>>>> =
        std::sync::OnceLock::new();
    Arc::clone(REG.get_or_init(|| Arc::new(StdMutex::new(None))))
}

fn gboost_shared_history() -> Arc<StdMutex<VecDeque<MarketSnapshot>>> {
    static REG: std::sync::OnceLock<Arc<StdMutex<VecDeque<MarketSnapshot>>>> =
        std::sync::OnceLock::new();
    Arc::clone(REG.get_or_init(|| {
        Arc::new(StdMutex::new(VecDeque::with_capacity(config::GBOOST_HISTORY_BUFFER_SIZE + 16)))
    }))
}

fn gboost_shared_training_data() -> Arc<StdMutex<VecDeque<TrainingSample>>> {
    static REG: std::sync::OnceLock<Arc<StdMutex<VecDeque<TrainingSample>>>> =
        std::sync::OnceLock::new();
    Arc::clone(REG.get_or_init(|| {
        Arc::new(StdMutex::new(VecDeque::with_capacity(config::GBOOST_HISTORY_BUFFER_SIZE)))
    }))
}

/// Cumulative lookahead-label pool.  Each retrain cycle labels only NEW history
/// snapshots (timestamp > `last_harvest_ts`) and appends the deadband survivors
/// here, so informative samples ACCUMULATE across hours and rotations instead of
/// having to all come from one 18-min window.  Flat nights fill slowly, volatile
/// hours fill fast; FIFO-capped at GBOOST_LABEL_POOL_CAP so stale regimes age out.
///
/// Persisted (B33): until v1.1.3 this lived only in memory, so every restart,
/// redeploy or container recreate dropped it to 0 against
/// GBOOST_MIN_TRAINING_SAMPLES. It is now written to `logs/` (the bind-mounted
/// directory that survives container recreation) at most once per
/// `LABEL_POOL_SAVE_INTERVAL_SECS` after a harvest that added samples, and
/// reloaded once per process on the first `GboostStrategyImpl::new()`. Samples
/// older than the `gboost_label_max_age_hours` knob are pruned at every harvest,
/// whether they came from disk or from this process, so a reloaded pool can be
/// stale by at most that window.
#[derive(Debug, Default)]
struct LabelPool {
    samples: VecDeque<TrainingSample>,
    /// Timestamp of the last history snapshot already harvested.
    last_harvest_ts: Option<chrono::DateTime<chrono::Utc>>,
    /// Snapshots the harvester examined since boot, deadband survivors or not.
    /// A pool that is "filling slowly" and one whose labeling input is frozen
    /// look identical in the sample count; this separates them.
    candidates_total: u64,
    /// Deadband survivors appended since boot.
    kept_total: u64,
    /// Samples dropped by the wall-clock age cap since boot.
    pruned_total: u64,
    /// Samples restored from the pool file at boot (0 = cold start).
    restored: usize,
    /// Counter values at the last "retrain waiting" status line, so it can
    /// report deltas rather than totals.
    status_candidates: u64,
    status_kept: u64,
    status_pruned: u64,
    /// Samples were added since the last save was handed off.
    dirty: bool,
    last_save_at: Option<Instant>,
}

fn gboost_label_pool() -> &'static StdMutex<LabelPool> {
    static REG: std::sync::OnceLock<StdMutex<LabelPool>> = std::sync::OnceLock::new();
    REG.get_or_init(|| StdMutex::new(LabelPool::default()))
}

// ── Label pool persistence ───────────────────────────────────────────────────

/// File name suffix; prefixed with the DB shard key exactly as the SQLite shards
/// are (`logs/btc-dradis.db` → `logs/btc-gboost_label_pool.json`).
const LABEL_POOL_FILENAME: &str = "gboost_label_pool.json";
/// Minimum wall-clock gap between two pool writes. A full conservative pool is
/// ~5 MB of JSON; the harvester runs every 10–30 s while the pool is filling,
/// so writing on every harvest would burn several GB/day of EBS writes for no
/// benefit. Five minutes bounds the loss on a hard kill to a few hundred
/// samples against a pool that took hours to build. I/O plumbing, deliberately
/// not a tuning knob.
const LABEL_POOL_SAVE_INTERVAL_SECS: u64 = 300;
/// Bumped whenever [`LabelPoolFile`] changes shape.
const LABEL_POOL_FORMAT_VERSION: u32 = 1;

/// On-disk form of the label pool. The header fields exist so a file written
/// under a different feature set or label horizon is rejected rather than
/// silently mixed in: a "BTC higher after 300 s" label is a different training
/// target from a "BTC higher after 180 s" one, and a profile switch that changes
/// GBOOST_LABEL_HORIZON_SECS must start a fresh pool.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct LabelPoolFile {
    format_version: u32,
    num_features: usize,
    label_horizon_secs: i64,
    saved_at: chrono::DateTime<chrono::Utc>,
    last_harvest_ts: Option<chrono::DateTime<chrono::Utc>>,
    samples: Vec<TrainingSample>,
}

/// Set once the first `GboostStrategyImpl::new()` has kicked off the disk load;
/// every hourly rotation constructs a new strategy object and must not reload.
static LABEL_POOL_LOAD_STARTED: AtomicBool = AtomicBool::new(false);
/// A save is already on a blocking thread; the next harvest will not stack another.
static LABEL_POOL_SAVE_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

/// Where this process keeps its label pool, or `None` when persistence is off.
///
/// Keyed on the PRIMARY SQLite shard — the first `db::init_shard` call, which
/// every venue makes before it deploys a squadron — so an intl instance writes
/// `logs/btc-gboost_label_pool.json`, Kalshi `logs/kalshi-…` and Polymarket US
/// `logs/us-…`, and two venues sharing a `logs/` directory never read each
/// other's labels. The model file keys on `CRYPTO_FILTER` with a hard "btc"
/// fallback, which DOES collide across venues when the variable is unset; the
/// pool does not repeat that.
///
/// `GBOOST_LABEL_POOL_PATH` overrides the whole path (parity with
/// `GBOOST_MODEL_PATH`). With no primary shard — tests, or a DB init failure —
/// persistence is skipped rather than guessed at.
fn label_pool_path() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("GBOOST_LABEL_POOL_PATH") {
        if !p.trim().is_empty() {
            return Some(std::path::PathBuf::from(p));
        }
    }
    let db_file = crate::helpers::db::pool()?
        .connect_options()
        .get_filename()
        .to_path_buf();
    label_pool_path_for_shard_file(&db_file)
}

/// `logs/<shard>-dradis.db` → `logs/<shard>-gboost_label_pool.json`.
/// Split out from [`label_pool_path`] so the mapping is testable without a DB.
fn label_pool_path_for_shard_file(db_file: &std::path::Path) -> Option<std::path::PathBuf> {
    let stem = db_file.file_stem()?.to_str()?;
    let shard = stem.strip_suffix("-dradis").unwrap_or(stem);
    if shard.is_empty() {
        return None;
    }
    let dir = db_file.parent().map(|p| p.to_path_buf()).unwrap_or_default();
    Some(dir.join(format!("{}-{}", shard, LABEL_POOL_FILENAME)))
}

/// Atomic write: serialize to `<path>.tmp` in the same directory, then rename
/// over the target. A container killed mid-write leaves at worst a stray `.tmp`,
/// never a truncated pool file that fails to parse on every subsequent boot.
fn write_label_pool_file(path: &std::path::Path, file: &LabelPoolFile) -> std::io::Result<()> {
    use std::io::Write as _;
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir)?;
        }
    }
    let bytes = serde_json::to_vec(file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

/// Read and validate a pool file. Every failure is a `String` reason for the
/// log, never a panic: a corrupt or foreign pool file must not break startup.
fn read_label_pool_file(path: &std::path::Path) -> Result<LabelPoolFile, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read failed: {e}"))?;
    let file: LabelPoolFile = serde_json::from_slice(&bytes)
        .map_err(|e| format!("parse failed: {e}"))?;
    if file.format_version != LABEL_POOL_FORMAT_VERSION {
        return Err(format!(
            "format version {} (this build writes {})", file.format_version, LABEL_POOL_FORMAT_VERSION
        ));
    }
    if file.num_features != NUM_FEATURES {
        return Err(format!(
            "{} features per sample (this build uses {})", file.num_features, NUM_FEATURES
        ));
    }
    if file.label_horizon_secs != config::GBOOST_LABEL_HORIZON_SECS {
        return Err(format!(
            "labels use a {}s horizon (this build labels at {}s)",
            file.label_horizon_secs, config::GBOOST_LABEL_HORIZON_SECS
        ));
    }
    Ok(file)
}

/// Fold a loaded file into the live pool. Restored samples are older than
/// anything harvested since boot, so they go in FRONT; the FIFO cap then
/// evicts from the front as usual. The harvest watermark keeps whichever is
/// newer. Returns the number of samples actually restored after capping.
fn merge_restored_into_pool(pool: &mut LabelPool, file: LabelPoolFile) -> usize {
    let mut merged: VecDeque<TrainingSample> = file.samples.into_iter().collect();
    let restored_before_cap = merged.len();
    let in_memory = pool.samples.len();
    merged.extend(pool.samples.drain(..));
    while merged.len() > config::GBOOST_LABEL_POOL_CAP {
        merged.pop_front();
    }
    // Eviction is from the front, so it consumes restored samples first.
    let evicted = (restored_before_cap + in_memory).saturating_sub(merged.len());
    pool.samples = merged;
    pool.last_harvest_ts = match (pool.last_harvest_ts, file.last_harvest_ts) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    let restored = restored_before_cap.saturating_sub(evicted);
    pool.restored = restored;
    restored
}

/// Drop samples older than `max_age` from the pool. Applied at every harvest,
/// not only at reload, so the wall-clock cap means the same thing for samples
/// this process labeled and for ones read back from disk. Returns the count
/// dropped.
fn prune_stale_samples(
    pool: &mut LabelPool,
    now: chrono::DateTime<chrono::Utc>,
    max_age: chrono::Duration,
) -> usize {
    let before = pool.samples.len();
    pool.samples.retain(|s| now - s.entry_timestamp <= max_age);
    let dropped = before - pool.samples.len();
    pool.pruned_total += dropped as u64;
    dropped
}

/// Kick off the one-per-process reload of the persisted pool. Runs the file
/// read on a blocking thread and merges under the pool lock when done; the
/// first harvest cannot happen before GBOOST_LABEL_HORIZON_SECS of history
/// exists, so the merge lands well ahead of it in practice and is correct in
/// either order regardless.
fn spawn_label_pool_load() {
    if LABEL_POOL_LOAD_STARTED.swap(true, Ordering::SeqCst) {
        return;
    }
    tokio::spawn(async move {
        let Some(path) = label_pool_path() else {
            tracing::info!(
                " GboostStrategy: label pool persistence off — no primary DB shard to key it on \
                 (set GBOOST_LABEL_POOL_PATH to force a location)"
            );
            return;
        };
        load_label_pool_from(path).await;
    });
}

/// Body of the startup reload: read `path` on a blocking thread, validate it,
/// merge it into the live pool, and log what happened. Every outcome, including
/// a missing, corrupt or foreign file, ends in a log line and a usable pool.
/// Returns the number of samples restored (0 on any rejection).
async fn load_label_pool_from(path: std::path::PathBuf) -> usize {
    if !path.exists() {
        tracing::info!(
            " GboostStrategy: no persisted label pool at '{}' — starting from an empty pool",
            path.display()
        );
        return 0;
    }
    let read_path = path.clone();
    let loaded = tokio::task::spawn_blocking(move || read_label_pool_file(&read_path)).await;
    match loaded {
        Ok(Ok(file)) => {
            let saved_at = file.saved_at;
            let (oldest, newest) = (
                file.samples.first().map(|s| s.entry_timestamp),
                file.samples.last().map(|s| s.entry_timestamp),
            );
            let n_in_file = file.samples.len();
            let restored = {
                let mut pool = gboost_label_pool().lock().unwrap();
                merge_restored_into_pool(&mut pool, file)
            };
            let age_h = (Utc::now() - saved_at).num_minutes() as f64 / 60.0;
            tracing::info!(
                " GboostStrategy: restored {} of {} label samples from '{}' (saved {:.1}h ago, \
                 samples span {} → {}); older than {}h are pruned at the first harvest",
                restored, n_in_file, path.display(), age_h,
                oldest.map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)).unwrap_or_else(|| "-".into()),
                newest.map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)).unwrap_or_else(|| "-".into()),
                config::GBOOST_LABEL_MAX_AGE_HOURS,
            );
            restored
        }
        Ok(Err(reason)) => {
            tracing::warn!(
                " GboostStrategy: ignoring persisted label pool at '{}' ({}) — starting from an empty pool; \
                 the file is overwritten at the next save",
                path.display(), reason
            );
            0
        }
        Err(e) => {
            tracing::warn!(
                " GboostStrategy: label pool load task panicked ({}); starting from an empty pool", e
            );
            0
        }
    }
}

/// Hand a snapshot of the pool to a blocking thread for an atomic write.
/// `payload` is built under the pool lock by the caller; nothing here touches it.
fn spawn_label_pool_save(path: std::path::PathBuf, payload: LabelPoolFile) {
    if LABEL_POOL_SAVE_IN_FLIGHT.swap(true, Ordering::SeqCst) {
        // Leave `dirty` as the caller set it (false); the next harvest that
        // adds samples re-arms it and the interval gate retries.
        return;
    }
    tokio::spawn(async move {
        let n = payload.samples.len();
        let write_path = path.clone();
        let result = tokio::task::spawn_blocking(move || write_label_pool_file(&write_path, &payload)).await;
        match result {
            Ok(Ok(())) => {
                // First save per process at INFO so an operator on the default
                // log level sees, once, that persistence is working and where;
                // the every-five-minutes repeats stay at DEBUG.
                static FIRST_SAVE_LOGGED: AtomicBool = AtomicBool::new(false);
                if !FIRST_SAVE_LOGGED.swap(true, Ordering::SeqCst) {
                    tracing::info!(
                        " GboostStrategy: label pool persisted — {} samples → '{}' (rewritten at most every {}s)",
                        n, path.display(), LABEL_POOL_SAVE_INTERVAL_SECS
                    );
                } else {
                    tracing::debug!(
                        " GboostStrategy: label pool saved — {} samples → '{}'", n, path.display()
                    );
                }
            }
            Ok(Err(e)) => tracing::warn!(
                " GboostStrategy: label pool save failed [{}]: {}", path.display(), e
            ),
            Err(e) => tracing::warn!(" GboostStrategy: label pool save task panicked: {}", e),
        }
        LABEL_POOL_SAVE_IN_FLIGHT.store(false, Ordering::SeqCst);
    });
}

impl GboostStrategyImpl {
    pub fn new() -> Self {
        // Rotation-surviving model handle. Only hit the disk when no model is in
        // memory yet (process start) — reloading on every rotation could clobber a
        // freshly trained in-memory model with an older disk copy.
        let model_arc = gboost_shared_model();
        let need_disk_load = model_arc.lock().unwrap().is_none();

        // Warm-start: try to load a previously persisted model from disk.
        //
        // The model path is version-locked to the current feature set (NUM_FEATURES = 30).
        // NEVER load a model from a different version — the feature dimensions won't match.
        // History: v14f (14 features) → v19f (added yes_mid_change, no_obi_change,
        // relative_depth_ratio, combined_ask_spread, oracle_drift_10m in May 2026) →
        // v22f (added spread_velocity, hist_vol_regime, tick_momentum in May 2026) →
        // v24f (added institutional_pulse, tide_coherence in Jun 2026) →
        // v26f (added oi_delta_pct, cvd_ratio in Jun 2026) →
        // v30g (added Horizon tradfi_velocity, macro_coherence, vix_proxy, vix_velocity
        // in Jul 2026; zero off-US-hours so the model learns session-dependent splits).
        //
        // Override the path at runtime via the GBOOST_MODEL_PATH env var, e.g.:
        //   GBOOST_MODEL_PATH=/path/to/gboost_model_v19f.json cargo run
        // This is the recommended way to seed a local instance with a model trained on prod.
        // When not overridden the path is namespaced by CRYPTO_FILTER so that each
        // container in a shared-volume multi-instance deploy writes its own file:
        //   logs/btc-gboost_model_v30g.json, logs/eth-gboost_model_v30g.json, etc.
        let model_clone = Arc::clone(&model_arc);
        if need_disk_load { tokio::spawn(async move {
            // Env var takes precedence; fall back to CRYPTO_FILTER-namespaced default.
            let model_path = std::env::var("GBOOST_MODEL_PATH")
                .unwrap_or_else(|_| {
                    let crypto = std::env::var("CRYPTO_FILTER")
                        .unwrap_or_else(|_| "btc".to_string())
                        .to_lowercase();
                    format!("logs/{}-{}", crypto, config::GBOOST_MODEL_FILENAME)
                });

            match tokio::fs::read_to_string(&model_path).await {
                Ok(json) => match PerpetualBooster::from_json(&json) {
                    Ok(loaded) => {
                        let n = loaded.trees.len();
                        // Discard a stump startup model — a stale 1-tree model is worse
                        // than no model at all because it sticks as "previous" during retrain
                        // storms and prevents the engine from cold-starting cleanly.
                        // 2026-05-07 evening: startup model had 1 tree, kept as "previous"
                        // for 110 minutes while every retrain hit the same degenerate result.
                        // The compile-time floor is used here because this runs before any
                        // DynamicConfig snapshot exists; only accepted models reach the
                        // disk, so a file's quality was judged when it was written (B37).
                        if n < config::GBOOST_STRUCTURAL_MIN_TREES {
                            tracing::warn!(
                                " GboostStrategy: discarding persisted model from '{}' ({} trees, structural floor {}), cold-starting",
                                model_path, n, config::GBOOST_STRUCTURAL_MIN_TREES
                            );
                        } else if !model_has_current_layout(&loaded) {
                            // B38: fit on a row-major buffer read as column major,
                            // so its columns were scrambled and its predictions
                            // are noise. Idle until the first accepted retrain
                            // rather than predict from it.
                            tracing::warn!(
                                " GboostStrategy: discarding persisted model from '{}' ({} trees): it predates the \
                                 training-matrix layout fix (no '{}' metadata) and its predictions are noise; \
                                 cold-starting, the first accepted retrain replaces it",
                                model_path, n, MODEL_META_LAYOUT_KEY
                            );
                        } else {
                            let provenance = loaded.get_metadata(&MODEL_META_HOLDOUT_SKILL_KEY.to_string())
                                .zip(loaded.get_metadata(&MODEL_META_HOLDOUT_N_KEY.to_string()))
                                .map(|(skill, hn)| format!(
                                    "accepted {} with holdout skill {} on {} samples",
                                    model_accepted_at_et(&loaded), skill, hn
                                ))
                                .unwrap_or_else(|| "no acceptance record; predates the holdout test".to_string());
                            *model_clone.lock().unwrap() = Some(loaded);
                            tracing::info!(
                                " GboostStrategy: loaded persisted model from '{}' ({} trees; {})",
                                model_path, n, provenance
                            );
                        }
                    }
                    Err(e) => tracing::warn!(
                        " GboostStrategy: model parse failed for '{}' (will train from scratch): {:?}",
                        model_path, e
                    ),
                },
                Err(_) => tracing::info!(
                    " GboostStrategy: no persisted model at '{}' — collecting data to train \
                     (tip: copy prod model here, or set GBOOST_MODEL_PATH env var)",
                    model_path
                ),
            }
        }); }

        // B33: the lookahead label pool used to restart at 0 on every process
        // start. Reload it once per process (guarded inside), same lifecycle as
        // the model above.
        spawn_label_pool_load();

        Self {
            model: model_arc,
            history: gboost_shared_history(),
            ticks_since_retrain: Arc::new(StdMutex::new(0)),
            is_training: Arc::new(AtomicBool::new(false)),
            training_data: gboost_shared_training_data(),
            pending_entries: Arc::new(StdMutex::new(HashMap::new())),
            post_exit_cooldowns: Arc::new(StdMutex::new(HashMap::new())),
            consecutive_degenerate: Arc::new(StdMutex::new(0)),
            retrain_backoff_until: Arc::new(StdMutex::new(None)),
            last_retrain_at: Arc::new(StdMutex::new(None)),
            last_retrain_status_log: Arc::new(StdMutex::new(None)),
            below_strike_since: Arc::new(StdMutex::new(None)),
            concept_drift_suppressed: Arc::new(AtomicBool::new(false)),
            last_concept_drift_score: Arc::new(StdMutex::new(0.0_f32)),
            consecutive_drift_above_threshold: Arc::new(StdMutex::new(0)),
            consecutive_stable_retrains: Arc::new(StdMutex::new(0)),
            market_hold_locks: Arc::new(StdMutex::new(std::collections::HashMap::new())),
            last_pred_log_at: Arc::new(StdMutex::new(None)),
            last_veto_log_at: Arc::new(StdMutex::new(None)),
            entry_signal_streak: Arc::new(StdMutex::new(None)),
            obi_ema: Arc::new(StdMutex::new([None; 4])),
        }
    }

    /// Test-only constructor: replaces the process-global shared state (model,
    /// history, training_data) with fresh isolated instances so parallel tests
    /// don't observe each other's samples through the rotation-surviving globals.
    #[cfg(test)]
    fn new_isolated() -> Self {
        let mut s = Self::new();
        s.model = Arc::new(StdMutex::new(None));
        s.history = Arc::new(StdMutex::new(VecDeque::new()));
        s.training_data = Arc::new(StdMutex::new(VecDeque::new()));
        s
    }

    /// Push snapshot into the ring buffer, evicting the oldest entry when at capacity.
    /// Time-decimated: snapshots arriving less than GBOOST_HISTORY_MIN_SPACING_MS after
    /// the previous accepted snapshot are dropped, so the buffer spans wall-clock time
    /// (~18 min at 500ms spacing) instead of ~110s of near-duplicate 50ms ticks.
    fn push_snapshot(&self, snap: MarketSnapshot) {
        let mut h = self.history.lock().unwrap();
        if let Some(last) = h.back() {
            let spacing_ms = (snap.timestamp - last.timestamp).num_milliseconds();
            if spacing_ms < config::GBOOST_HISTORY_MIN_SPACING_MS {
                return;
            }
        }
        h.push_back(snap);
        if h.len() > config::GBOOST_HISTORY_BUFFER_SIZE {
            h.pop_front();
        }
    }

    /// Increment the retrain counter and, if the threshold is reached, kick off
    /// a background training job via `tokio::task::spawn_blocking`.
    ///
    /// Label sourcing priority:
    ///   1. Real trade outcomes stored in `training_data` (highest quality — actual P&L).
    ///   2. Lookahead labels from the `history` ring buffer when `training_data` is too
    ///      sparse.  Label: oracle price higher GBOOST_LABEL_HORIZON_SECS later → 1.0.
    ///      This breaks the chicken-and-egg deadlock that prevents the model from ever
    ///      reaching the minimum sample count required to produce its first predictions.
    fn maybe_retrain(&self, dc: &crate::helpers::dynamic_config::DynamicConfig) {
        if self.is_training.load(Ordering::Relaxed) { return; }

        // ── Degenerate-retrain backoff ─────────────────────────────────────────
        // When consecutive retrains produce degenerate models the engine must not
        // spin at 10-second intervals burning CPU.  Backoff grows exponentially:
        // 1st degen → 20s, 2nd → 40s, 3rd → 80s, … capped at 300s (5 min).
        {
            let guard = self.retrain_backoff_until.lock().unwrap();
            if let Some(until) = *guard {
                if Instant::now() < until {
                    return;
                }
            }
        }

        // ── Hard wall-clock retrain floor ──────────────────────────────────────
        // GBOOST_RETRAIN_EVERY_N is a tick counter, not a time interval; at a fast
        // main-loop tick rate it can trip every ~30s.  A full booster.fit() +
        // concept-drift pass every 30s pegs a 1-2 vCPU box, starves the tokio runtime
        // and trips the loop/OS watchdogs (observed 2026-07-07).  Enforce a minimum
        // wall-clock gap between SUCCESSFUL retrains regardless of tick rate.  This is
        // independent of the degenerate-backoff above (which only fires on bad models).
        {
            let guard = self.last_retrain_at.lock().unwrap();
            if let Some(last) = *guard {
                if last.elapsed().as_secs() < config::GBOOST_MIN_RETRAIN_INTERVAL_SECS {
                    return;
                }
            }
        }

        let triggered = {
            let mut t = self.ticks_since_retrain.lock().unwrap();
            *t += 1;
            *t >= config::GBOOST_RETRAIN_EVERY_N
        };
        if !triggered { return; }

        // Watchdog breadcrumb: the retrain trigger's SYNCHRONOUS section (sample
        // collection + drift-window capture) runs on the loop thread and acquires the
        // std::sync locks (training_data, history) that the eval path also takes — the
        // exact contention the OS watchdog comment names. Mark it distinctly so a stall
        // here reports GBOOST_RETRAIN rather than the generic SIGNAL_EVAL/gboost.
        crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::GboostRetrain);
        tracing::debug!(" GboostStrategy: retrain trigger fired — collecting samples (sync section)");

        // Throttled INFO status (once per 10 min): abort paths below are otherwise
        // silent/debug-only, which made a 17h cold-start stall invisible in prod logs.
        // Returns whether the line was emitted, so a caller reporting deltas can
        // reset its baseline only when the reader actually saw them.
        let status = |msg: String| -> bool {
            let mut guard = self.last_retrain_status_log.lock().unwrap();
            if guard.map_or(true, |t| t.elapsed().as_secs() >= 600) {
                tracing::info!(" GboostStrategy: retrain waiting — {}", msg);
                *guard = Some(Instant::now());
                true
            } else {
                false
            }
        };

        // ── Collect real trade outcomes (Source 1: highest quality labels) ──────────
        // These are actual entry/exit P&L labels — far more informative than lookahead
        // proxies.  Always collected first; they are prepended to the training batch so
        // the model encounters them before the denser lookahead fill.
        let real_samples: Vec<TrainingSample> = {
            let td = self.training_data.lock().unwrap();
            td.iter().cloned().collect()
        };

        // Captured before the move below. The retrain line reports it, and it is a
        // genuinely different quantity from the positive-label count they were
        // once conflated with — see the log call at the end of this function.
        let real_count = real_samples.len();

        // A pool save handed off by the harvest below, spawned once the pool and
        // history locks are released.
        let mut pool_save: Option<(std::path::PathBuf, LabelPoolFile)> = None;

        let training_samples: Vec<TrainingSample> = if real_samples.len() >= config::GBOOST_MIN_TRAINING_SAMPLES {
            // Enough real trade outcomes — use them exclusively for the cleanest signal.
            real_samples
        } else {
            // ── Source 2 (+ Source 1 blend): lookahead labels via the global pool ──────
            // When real outcomes exist but are too few, prepend them to the lookahead batch.
            // Label = 1.0 if the ORACLE price is higher GBOOST_LABEL_HORIZON_SECS later.
            // The market resolves on the oracle, so this is the actual prediction target —
            // the previous yes_bid-based label learned thin-book quote noise instead
            // (2026-07-14: model called DOWN 14/17 times while BTC rose +1.7%).
            //
            // Harvest is INCREMENTAL: each cycle labels only snapshots newer than the
            // pool's last-harvest watermark, and deadband survivors accumulate in the
            // process-global pool (see gboost_label_pool) across cycles and rotations.
            let h = self.history.lock().unwrap();
            let n = h.len();
            let horizon = chrono::Duration::seconds(config::GBOOST_LABEL_HORIZON_SECS);
            let mut pool_guard = gboost_label_pool().lock().unwrap();
            // Wall-clock age cap, applied before the harvest so it covers samples
            // restored from disk and ones this process labeled alike. Hot-tunable
            // (`gboost_label_max_age_hours`); clamped to [1h, 1y] so a typo can
            // neither empty the pool on every cycle nor overflow `Duration::hours`,
            // which panics rather than saturates.
            let max_age = chrono::Duration::hours(dc.gboost_label_max_age_hours.clamp(1, 24 * 365));
            let pruned_now = prune_stale_samples(&mut pool_guard, Utc::now(), max_age);
            if pruned_now > 0 {
                tracing::debug!(
                    " GboostStrategy: pruned {} label samples older than {}h ({} remain)",
                    pruned_now, max_age.num_hours(), pool_guard.samples.len()
                );
            }
            let kept_before = pool_guard.kept_total;
            let LabelPool { samples: pool, last_harvest_ts, candidates_total, kept_total, .. } = &mut *pool_guard;
            // `usable` = number of leading samples whose label horizon is fully inside
            // the buffer (a future snapshot ≥ horizon later exists for them).
            let usable = match (h.front(), h.back()) {
                (Some(first), Some(last)) if last.timestamp - first.timestamp >= horizon => {
                    h.iter().take_while(|s| last.timestamp - s.timestamp >= horizon).count()
                }
                _ => 0,
            };
            // Two-pointer scan: `j` tracks the first snapshot ≥ horizon after `i`.
            // Both indices only move forward, so the whole pass is O(n).
            let mut j = 0usize;
            for i in 0..usable {
                let snap = &h[i];
                // Already harvested by a previous cycle — never relabel a snapshot.
                if last_harvest_ts.map_or(false, |ts| snap.timestamp <= ts) {
                    continue;
                }
                while j < n && h[j].timestamp - snap.timestamp < horizon {
                    j += 1;
                }
                if j >= n {
                    break; // No future snapshot far enough ahead (shouldn't happen within `usable`)
                }
                let future = &h[j];
                // Mark processed regardless of whether the deadband keeps it below.
                *last_harvest_ts = Some(snap.timestamp);
                *candidates_total += 1;
                let cur = snap.oracle_price.to_f64().unwrap_or(0.0);
                let fut = future.oracle_price.to_f64().unwrap_or(0.0);
                // Skip directionally-uninformative samples: oracle essentially flat
                // (or missing) over the horizon. Force-labeling these 0 teaches the
                // model that "flat" equals "down".
                if cur <= 0.0 || fut <= 0.0
                    || ((fut - cur).abs() / cur) < config::GBOOST_LABEL_MIN_ORACLE_MOVE_FRAC
                {
                    continue;
                }
                let prev_snap = if i > 0 { Some(&h[i - 1]) } else { None };
                let hv = hist_vol_from_deque(&h, i);
                let tm = tick_momentum_from_deque(&h, i);
                pool.push_back(TrainingSample {
                    features: extract_features(snap, prev_snap, hv, tm),
                    is_profitable: fut > cur,
                    entry_timestamp: snap.timestamp,
                });
                *kept_total += 1;
                if pool.len() > config::GBOOST_LABEL_POOL_CAP {
                    pool.pop_front(); // FIFO: stale regimes age out
                }
            }
            let mut combined = real_samples; // Real outcomes first (highest quality)
            combined.extend(pool.iter().cloned());

            // ── Persist (B33) ─────────────────────────────────────────────────
            // Only after a harvest that added samples, and no more often than the
            // save interval. The payload is cloned here under the lock; the
            // serialize + atomic write happen on a blocking thread afterwards.
            if pool_guard.kept_total > kept_before {
                pool_guard.dirty = true;
            }
            let save_due = pool_guard.dirty
                && pool_guard.last_save_at
                    .map_or(true, |t| t.elapsed().as_secs() >= LABEL_POOL_SAVE_INTERVAL_SECS)
                && !LABEL_POOL_SAVE_IN_FLIGHT.load(Ordering::SeqCst);
            if save_due {
                if let Some(path) = label_pool_path() {
                    pool_save = Some((path, LabelPoolFile {
                        format_version: LABEL_POOL_FORMAT_VERSION,
                        num_features: NUM_FEATURES,
                        label_horizon_secs: config::GBOOST_LABEL_HORIZON_SECS,
                        saved_at: Utc::now(),
                        last_harvest_ts: pool_guard.last_harvest_ts,
                        samples: pool_guard.samples.iter().cloned().collect(),
                    }));
                    pool_guard.dirty = false;
                    pool_guard.last_save_at = Some(Instant::now());
                }
            }
            combined
        };

        if let Some((path, payload)) = pool_save.take() {
            spawn_label_pool_save(path, payload);
        }

        if training_samples.len() < config::GBOOST_MIN_TRAINING_SAMPLES {
            // Re-arm the tick counter so the trigger fires again after a full
            // GBOOST_RETRAIN_EVERY_N cycle instead of on every 50ms tick.  The pool
            // accumulates across cycles, so this state always progresses toward a train.
            //
            // The deltas answer the question the bare count cannot: a pool that is
            // filling slowly shows candidates scanned with few kept (flat oracle,
            // deadband rejects them); a pool whose labeling input has stopped shows
            // zero candidates. Both used to print as the same unchanging number.
            let real_outcomes = { let td = self.training_data.lock().unwrap(); td.len() };
            let (line, snapshot) = {
                let p = gboost_label_pool().lock().unwrap();
                let scanned = p.candidates_total - p.status_candidates;
                let kept    = p.kept_total - p.status_kept;
                let pruned  = p.pruned_total - p.status_pruned;
                (
                    format!(
                        "label pool filling ({} of {} — {} real outcomes; {} restored from disk; \
                         since last report: {} snapshots scanned, {} kept, {} aged out; \
                         flat regimes fill slowly)",
                        training_samples.len(), config::GBOOST_MIN_TRAINING_SAMPLES, real_outcomes,
                        p.restored, scanned, kept, pruned,
                    ),
                    (p.candidates_total, p.kept_total, p.pruned_total),
                )
            };
            if status(line) {
                let mut p = gboost_label_pool().lock().unwrap();
                (p.status_candidates, p.status_kept, p.status_pruned) = snapshot;
            }
            *self.ticks_since_retrain.lock().unwrap() = 0;
            return;
        }

        // ── Label-balance guard ───────────────────────────────────────────────
        // In a strongly trending market the lookahead window is nearly all-1 or
        // all-0.  Feeding a homogeneous batch to perpetual causes it to auto-stop
        // at 1 tree, which then replaces the current (good) model with a random
        // stump.  Detect and skip these cycles before spawning the expensive task.
        let pos_count = training_samples.iter().filter(|s| s.is_profitable).count();
        let pos_fraction = pos_count as f64 / training_samples.len() as f64;
        if pos_fraction > config::GBOOST_LOOKAHEAD_LABEL_BALANCE_MAX
            || pos_fraction < (1.0 - config::GBOOST_LOOKAHEAD_LABEL_BALANCE_MAX)
        {
            tracing::debug!(
                " GBoost: skipping retrain — labels imbalanced ({:.0}% positive > max {:.0}%), waiting for balanced data",
                pos_fraction * 100.0, config::GBOOST_LOOKAHEAD_LABEL_BALANCE_MAX * 100.0
            );
            status(format!(
                "labels imbalanced ({:.0}% positive, allowed {:.0}–{:.0}%) — one-sided drift over the horizon",
                pos_fraction * 100.0,
                (1.0 - config::GBOOST_LOOKAHEAD_LABEL_BALANCE_MAX) * 100.0,
                config::GBOOST_LOOKAHEAD_LABEL_BALANCE_MAX * 100.0
            ));
            *self.ticks_since_retrain.lock().unwrap() = 0;
            *self.last_retrain_at.lock().unwrap() = Some(Instant::now());
            return;
        }

        *self.ticks_since_retrain.lock().unwrap() = 0;
        *self.last_retrain_at.lock().unwrap() = Some(Instant::now());
        self.is_training.store(true, Ordering::Relaxed);
        // `real_count` and `pos_count` are different things and were once both
        // printed as "real outcomes": this line passed `pos_count` into a slot
        // labeled that way, so on 2026-08-29 a production box that had taken
        // ZERO trades reported "372 samples (231 real outcomes)". 231 was simply
        // 62% of 372. Meanwhile the "retrain waiting" line above uses the phrase
        // for `training_data.len()`, its true meaning, so two adjacent lines
        // disagreed about what a real outcome was.
        tracing::info!(
            " GboostStrategy: retraining model — {} samples ({} from real trade outcomes, {} labeled up = {:.0}%)",
            training_samples.len(), real_count, pos_count, pos_fraction * 100.0
        );

        let model_arc   = Arc::clone(&self.model);
        let is_training = Arc::clone(&self.is_training);
        let consecutive_degenerate    = Arc::clone(&self.consecutive_degenerate);
        let retrain_backoff_until     = Arc::clone(&self.retrain_backoff_until);
        let concept_drift_suppressed  = Arc::clone(&self.concept_drift_suppressed);
        let last_concept_drift_score  = Arc::clone(&self.last_concept_drift_score);
        let consecutive_drift_counter = Arc::clone(&self.consecutive_drift_above_threshold);
        let consecutive_stable_counter = Arc::clone(&self.consecutive_stable_retrains);

        // Hot-tunable drift knobs (bug #10): snapshot the current DynamicConfig
        // values so the async retrain closure uses the operator/profile-tuned
        // settings rather than compile-time consts.
        let drift_threshold: f32 = dc.gboost_concept_drift_threshold.to_f32().unwrap_or(22.0);
        let drift_consecutive_required  = dc.gboost_drift_consecutive_required.max(1) as usize;
        let drift_stable_clear_required = dc.gboost_drift_stable_clear_required.max(1) as usize;

        // Capture a window of recent snapshots for concept-drift evaluation.
        // Oldest-first order (same as extract_features expects for prev_s).
        let history_for_drift: Vec<MarketSnapshot> = {
            let h = self.history.lock().unwrap();
            let n = h.len().min(config::GBOOST_DRIFT_WINDOW);
            h.iter().skip(h.len().saturating_sub(n)).cloned().collect()
        };

        // Read off the config snapshot before the move — `dc` is borrowed and the
        // blocking closure outlives this scope.
        let train_budget = dc.gboost_budget.to_f32().unwrap_or(0.8);
        let train_iteration_limit = dc.gboost_iteration_limit.max(1);
        // Acceptance thresholds (B37), hot-tunable. The entry threshold is only
        // used to count "confident calls" in the holdout report.
        let structural_min_trees = dc.gboost_structural_min_trees.max(1) as usize;
        let min_skill = dc.gboost_holdout_min_skill.to_f64().unwrap_or(0.0);
        let confident_threshold = dc.gboost_entry_threshold.to_f64().unwrap_or(0.72);

        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                let verdict = train_and_validate(
                    training_samples, train_budget, train_iteration_limit,
                    structural_min_trees, min_skill, confident_threshold, holdout_gap(),
                )?;
                let drift = match &verdict {
                    RetrainVerdict::Accepted { model, .. } => compute_concept_drift(model, &history_for_drift),
                    RetrainVerdict::Rejected(_) => 0.0,
                };
                Ok::<(RetrainVerdict, f32), anyhow::Error>((verdict, drift))
            }).await;

            match result {
                Ok(Ok((RetrainVerdict::Rejected(why), _))) => {
                    // Rejected BEFORE persisting: keep the previous (validated) model
                    // rather than regressing, and say exactly why, with what is being
                    // kept. The old line said "degenerate model" for every rejection,
                    // which read as a frozen model when 16 of 65 retrains were in fact
                    // being adopted (B37).
                    //
                    // 2026-07-18/19: the save used to happen first ("crash safety"),
                    // which let a quiet-market 16-tree stump OVERWRITE the good model
                    // on disk; it was then rejected in memory, and the next deploy
                    // cold-started with nothing loadable — GBoost was blind for 21h.
                    // Only accepted models may touch the disk file.
                    let previous = {
                        let m = model_arc.lock().unwrap();
                        m.as_ref()
                            .map(|m| format!("{} trees, accepted {}", m.trees.len(), model_accepted_at_et(m)))
                            .unwrap_or_else(|| "none; GBoost stays idle".to_string())
                    };
                    match why {
                        RetrainRejection::PoolTooShort { .. } => {
                            // Nothing was fit and nothing is wrong; the wall-clock
                            // retrain floor already spaces the next attempt.
                            tracing::info!(
                                " GboostStrategy: retrain deferred — {}; keeping previous model ({})",
                                why.describe(), previous
                            );
                        }
                        _ => {
                            // Exponential backoff so a rejected fit does not storm
                            // the CPU every trigger.
                            let mut count = consecutive_degenerate.lock().unwrap();
                            *count += 1;
                            let backoff_secs = (20u64 * 2u64.pow((*count).saturating_sub(1).min(4) as u32)).min(300);
                            *retrain_backoff_until.lock().unwrap() =
                                Some(Instant::now() + std::time::Duration::from_secs(backoff_secs));
                            tracing::warn!(
                                " GboostStrategy: retrain rejected — {}; keeping previous model ({}) (backoff {}s, #{} consecutive)",
                                why.describe(), previous, backoff_secs, *count
                            );
                        }
                    }
                    is_training.store(false, Ordering::Relaxed);
                    return;
                }
                Ok(Ok((RetrainVerdict::Accepted { model: new_model, report }, drift_score))) => {
                    let n = new_model.trees.len();

                    // Accepted — reset the rejection backoff counters.
                    *consecutive_degenerate.lock().unwrap() = 0;
                    *retrain_backoff_until.lock().unwrap() = None;

                    // Persist the ACCEPTED model to disk so a crash/redeploy doesn't
                    // lose the trained weights.  Same path resolution as startup load
                    // (env var override first, then CRYPTO_FILTER-namespaced default
                    // so containers don't stomp each other).
                    if let Ok(json) = new_model.json_dump() {
                        let model_path = std::env::var("GBOOST_MODEL_PATH")
                            .unwrap_or_else(|_| {
                                let crypto = std::env::var("CRYPTO_FILTER")
                                    .unwrap_or_else(|_| "btc".to_string())
                                    .to_lowercase();
                                format!("logs/{}-{}", crypto, config::GBOOST_MODEL_FILENAME)
                            });
                        if let Err(e) = tokio::fs::write(&model_path, &json).await {
                            tracing::warn!(" GboostStrategy: model save failed [{}]: {}", model_path, e);
                        }
                    }

                    // ── Concept drift monitoring ──────────────────────────────
                    // Compare how live data flows through the new model's split points
                    // vs. the training distribution.  A high chi-squared score means the
                    // current market regime is outside what the model was trained on.
                    //
                    // Suppression: requires gboost_drift_consecutive_required consecutive
                    // above-threshold retrains to activate.
                    //
                    // Clearing: requires gboost_drift_stable_clear_required consecutive
                    // BELOW-threshold retrains before suppression is lifted.  A single
                    // below-threshold "blink" during a genuine regime change used to
                    // unlock entries prematurely; the stable-retrain requirement ensures
                    // the model has genuinely recaptured the distribution.
                    *last_concept_drift_score.lock().unwrap() = drift_score;
                    if drift_score > drift_threshold {
                        // Above threshold: increment drift counter, reset stable counter.
                        let mut count = consecutive_drift_counter.lock().unwrap();
                        let mut stable = consecutive_stable_counter.lock().unwrap();
                        *count += 1;
                        *stable = 0; // any above-threshold retrain resets the stable streak
                        if *count >= drift_consecutive_required {
                            tracing::warn!(
                                "⚠️ GBoost: concept drift confirmed ({} consecutive retrains, \
                                 latest score={:.2} > threshold {:.2}) — suppressing entries \
                                 until {} consecutive stable retrains recapture regime",
                                *count, drift_score, drift_threshold,
                                drift_stable_clear_required
                            );
                            concept_drift_suppressed.store(true, Ordering::Relaxed);
                        } else {
                            tracing::warn!(
                                "⚠️ GBoost: drift spike #{} (score={:.2} > threshold {:.2}) — \
                                 watching for {} consecutive trigger before suppressing",
                                *count, drift_score, drift_threshold,
                                drift_consecutive_required
                            );
                        }
                    } else {
                        // Below threshold: reset drift counter, increment stable counter.
                        // Only clear suppression after gboost_drift_stable_clear_required
                        // consecutive stable retrains — prevents premature unlock on a
                        // single below-threshold tick during a sustained regime change.
                        let mut drift_count = consecutive_drift_counter.lock().unwrap();
                        let mut stable = consecutive_stable_counter.lock().unwrap();
                        *drift_count = 0;
                        *stable += 1;
                        if concept_drift_suppressed.load(Ordering::Relaxed) {
                            if *stable >= drift_stable_clear_required {
                                tracing::info!(
                                    "✅ GBoost: concept drift cleared after {} consecutive stable \
                                     retrains (latest score={:.2} ≤ threshold {:.2}) — resuming entries",
                                    *stable, drift_score, drift_threshold
                                );
                                concept_drift_suppressed.store(false, Ordering::Relaxed);
                            } else {
                                tracing::info!(
                                    "⏳ GBoost: drift below threshold (score={:.2} ≤ {:.2}) but \
                                     suppression held — {}/{} stable retrains required",
                                    drift_score, drift_threshold,
                                    *stable, drift_stable_clear_required
                                );
                            }
                        } else {
                            tracing::debug!(
                                " GBoost: drift below threshold (score={:.2}), stable streak: {}/{}",
                                drift_score, *stable, drift_stable_clear_required
                            );
                        }
                    }

                    let old_tree_count = {
                        let old_model = model_arc.lock().unwrap();
                        old_model.as_ref().map(|m| m.trees.len()).unwrap_or(0)
                    };

                    // Always INFO: an accepted retrain that logged only at DEBUG unless
                    // the tree count moved by more than five made the accept/reject
                    // tally impossible to read from the production log (B37).
                    tracing::info!(
                        " GboostStrategy: retrain accepted — {} | refit on the full pool: {} trees (was {}) | drift={:.2}",
                        report.summary(), n, old_tree_count, drift_score
                    );

                    *model_arc.lock().unwrap() = Some(new_model);
                }
                Ok(Err(e)) => tracing::warn!(" GboostStrategy: training error: {}", e),
                Err(e)     => tracing::warn!(" GboostStrategy: spawn_blocking panic: {}", e),
            }

            is_training.store(false, Ordering::Relaxed);
        });
    }

    /// Return P(YES_UP) ∈ [0, 1] from the current model, or `None` if no model exists yet.
    fn predict(&self, snap: &MarketSnapshot) -> Option<f64> {
        let guard = self.model.lock().unwrap();
        let booster = guard.as_ref()?;
        // Refuse to predict from a stump — a single tree is random noise. Every
        // model that reaches this slot was already judged (the holdout test at
        // retrain, the structural floor at startup); this is the last-line guard.
        if booster.trees.len() < config::GBOOST_STRUCTURAL_MIN_TREES {
            return None;
        }
        let h = self.history.lock().unwrap();
        let n = h.len();
        let prev_snap = if n >= 2 { Some(&h[n - 2]) } else { None }; // Get previous snapshot
        let hist_vol = if n > 0 { hist_vol_from_deque(&h, n - 1) } else { 0.0 };
        let tick_momentum = if n > 0 { tick_momentum_from_deque(&h, n - 1) } else { 0.0 };
        let feats = extract_features(snap, prev_snap, hist_vol, tick_momentum); // Pass prev_snap
        // Stack-allocated array; Matrix borrows it for the duration of this call only.
        // One row: row-major and column-major coincide, so no re-layout is needed (B38).
        let matrix = Matrix::new(&feats, 1, NUM_FEATURES);
        booster.predict_proba(&matrix, false, false).first().copied()
    }

    /// Persist one supervised label only when an exit signal is emitted.
    /// This avoids training on transient mark-to-market states from non-exit ticks.
    fn record_training_outcome_on_exit(&self, token_id: MarketId, is_profitable: bool) {
        let mut pending_entries_guard = self.pending_entries.lock().unwrap();
        if let Some((entry_snap, entry_prev_snap, _entry_price, hist_vol, tick_momentum)) = pending_entries_guard.remove(&token_id) {
            // Use the prev_snap captured AT ENTRY TIME (stored in the tuple) rather than the
            // current history tail.  Using the current prev at exit time was a correctness bug:
            // pairing an exit-time prev with the entry snapshot produces a hybrid feature vector
            // that doesn't match what the model saw when it made the entry prediction.
            // hist_vol and tick_momentum are likewise the values computed at entry time.
            let training_sample = TrainingSample {
                features: extract_features(&entry_snap, entry_prev_snap.as_ref(), hist_vol, tick_momentum),
                is_profitable,
                entry_timestamp: entry_snap.timestamp,
            };
            let mut training_data_guard = self.training_data.lock().unwrap();
            training_data_guard.push_back(training_sample);
            if training_data_guard.len() > config::GBOOST_HISTORY_BUFFER_SIZE {
                training_data_guard.pop_front();
            }
        }
    }

    /// Mark token as cooling down with the standard cooldown after a TP or SignalRev exit.
    fn mark_post_exit_cooldown(&self, token_id: MarketId) {
        let mut guard = self.post_exit_cooldowns.lock().unwrap();
        guard.insert(token_id, (Utc::now(), config::GBOOST_POST_EXIT_COOLDOWN_SECS));
    }

    /// Mark token as cooling down with the **extended** cooldown after a stop-loss exit.
    ///
    /// An SL exit means the market moved adversely against the position — not just that the
    /// model changed direction.  Using a longer cooldown (GBOOST_SL_POST_EXIT_COOLDOWN_SECS)
    /// prevents re-entering the same adverse direction within 20 minutes of a loss.
    fn mark_post_exit_cooldown_sl(&self, token_id: MarketId) {
        let mut guard = self.post_exit_cooldowns.lock().unwrap();
        guard.insert(token_id, (Utc::now(), config::GBOOST_SL_POST_EXIT_COOLDOWN_SECS));
    }

    /// Returns remaining cooldown seconds for this token, if still cooling down.
    fn post_exit_cooldown_remaining_secs(&self, token_id: MarketId) -> Option<i64> {
        let now = Utc::now();
        let mut guard = self.post_exit_cooldowns.lock().unwrap();
        if let Some((ts, cooldown_secs)) = guard.get(&token_id).copied() {
            let elapsed = (now - ts).num_seconds();
            let remaining = cooldown_secs - elapsed;
            if remaining > 0 {
                return Some(remaining);
            }
            guard.remove(&token_id);
        }
        None
    }

    /// Compute side-specific orderbook imbalance (OBI) in [-1, 1].
    ///
    /// Returns `dec!(-1.0)` (maximally adverse) when depth data is missing (total = 0).
    /// This is intentional: a missing book means we cannot evaluate adverse selection,
    /// so we conservatively block the entry rather than silently allowing it.
    /// "Ghost OBI" entries (zero depth at evaluation but adverse at heartbeat time)
    /// were responsible for losing trades in the 2026-05-07 afternoon session.
    /// Raw one-sided OBI in [-1, 1] for a snapshot, or `None` when the book reports
    /// zero total depth (no data — NOT the same thing as an adverse book).
    fn side_obi(is_yes_side: bool, s: &MarketSnapshot) -> Option<rust_decimal::Decimal> {
        let (bid, ask) = if is_yes_side {
            (s.yes_bid_depth, s.yes_ask_depth)
        } else {
            (s.no_bid_depth, s.no_ask_depth)
        };
        let total = bid + ask;
        if total > dec!(0) {
            Some((bid - ask) / total)
        } else {
            None
        }
    }

    /// Update and read the EMA-smoothed OBI for one of the four gate slots
    /// (OBI_SLOT_*). Instantaneous OBI on these thin books whipsaws ±0.9 minute to
    /// minute, so the gates consume a GBOOST_OBI_EMA_TAU_SECS exponential moving
    /// average instead — cadence-independent via alpha = 1 − exp(−dt/τ).
    ///
    /// `raw == None` (zero-depth book): the previous EMA is returned as-is when it is
    /// fresher than GBOOST_OBI_EMA_STALE_SECS; otherwise `None` (caller vetoes).
    fn smoothed_obi(&self, slot: usize, raw: Option<rust_decimal::Decimal>) -> Option<rust_decimal::Decimal> {
        let now = Instant::now();
        let mut slots = self.obi_ema.lock().unwrap();
        let entry = &mut slots[slot];
        match raw {
            Some(r) => {
                let r = r.to_f64().unwrap_or(0.0);
                let ema = match *entry {
                    Some((prev, last_at)) => {
                        let dt = now.duration_since(last_at).as_secs_f64();
                        let alpha = 1.0 - (-dt / config::GBOOST_OBI_EMA_TAU_SECS).exp();
                        prev + alpha * (r - prev)
                    }
                    None => r,
                };
                *entry = Some((ema, now));
                rust_decimal::Decimal::from_f64_retain(ema).map(|d| d.round_dp(10))
            }
            None => match *entry {
                Some((prev, last_at))
                    if now.duration_since(last_at).as_secs_f64() <= config::GBOOST_OBI_EMA_STALE_SECS =>
                {
                    rust_decimal::Decimal::from_f64_retain(prev).map(|d| d.round_dp(10))
                }
                _ => None,
            },
        }
    }
}

/// Slot indices into `GboostStrategyImpl::obi_ema`.
const OBI_SLOT_TARGET_YES: usize = 0;
const OBI_SLOT_TARGET_NO:  usize = 1;
const OBI_SLOT_HOURLY_YES: usize = 2;
const OBI_SLOT_HOURLY_NO:  usize = 3;

// ── Position-sizing helpers ───────────────────────────────────────────────────

/// Scale GBoost trade size by model confidence and oracle volatility regime.
///
/// Confidence scaling (linear):
///   - `confidence == entry_thresh`  → GBOOST_MIN_EXPOSURE_USDC
///   - `confidence == 1.0`           → max_exposure (dc.gboost_max_exposure_usdc)
///   - In between                    → linear interpolation
///
/// Volatility scaling (multiplicative, applied on top of confidence scale):
///   - When hist_vol_regime > GBOOST_HIGH_VOL_REGIME_THRESHOLD:
///       apply GBOOST_HIGH_VOL_SIZE_SCALE (e.g. 0.50 × base)
///   - Rationale: elevated oracle volatility correlates with higher adverse selection
///     and fill-quality degradation; reducing size protects capital in these regimes.
///
/// Integer-basis-point arithmetic avoids a `FromPrimitive` trait dependency.
fn scale_trade_size(
    confidence: f64,       // model confidence for this direction (≥ entry_thresh)
    entry_thresh: f64,     // configured entry threshold (dc.gboost_entry_threshold)
    hist_vol: f64,         // current hist_vol_regime value in [0, 1]
    max_exposure: Decimal, // dc.gboost_max_exposure_usdc
) -> Decimal {
    let min_exposure = config::GBOOST_MIN_EXPOSURE_USDC;
    // Flat sizing fleet-wide: when Kelly/confidence upsizing is disabled, trade the
    // base (minimum) exposure regardless of model confidence.
    if !config::ENABLE_KELLY_SIZING { return min_exposure; }
    // Confidence scale: fraction of the [threshold, 1.0] range that `confidence` covers.
    let conf_range = (1.0_f64 - entry_thresh).max(1e-9);
    let conf_excess = (confidence - entry_thresh).max(0.0);
    let scale_f64 = (conf_excess / conf_range).min(1.0);
    // Convert to Decimal via integer basis-points (scale_bps / 10000).
    let scale_bps = (scale_f64 * 10_000.0_f64) as i64;
    let scale_dec = Decimal::new(scale_bps, 4);
    let base = min_exposure + (max_exposure - min_exposure) * scale_dec;
    // Apply volatility reduction when oracle is moving erratically.
    if hist_vol > config::GBOOST_HIGH_VOL_REGIME_THRESHOLD {
        let vol_scale_bps = (config::GBOOST_HIGH_VOL_SIZE_SCALE * 10_000.0_f64) as i64;
        let vol_scale_dec = Decimal::new(vol_scale_bps, 4);
        base * vol_scale_dec
    } else {
        base
    }
}

impl Default for GboostStrategyImpl {
    fn default() -> Self { Self::new() }
}

// ── Strategy trait ────────────────────────────────────────────────────────────

#[async_trait]
impl Strategy for GboostStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        // Maintain history and trigger background retrains.
        // This happens regardless of ENABLE_GBOOST_TRADING so the model can learn.
        //
        // Snapshot sourcing strategy:
        //   OBI / spread / price features → from the DAILY/maker market (what we trade)
        //   Oracle / momentum features    → from the HOURLY snapshot (always fresh Binance WS)
        //
        // The daily CLOB WS only fires when someone places/changes an order on the daily book.
        // Between daily order updates the maker_snapshot.oracle_price is frozen at the last
        // received value.  60 frozen oracle prices → hist_vol ≈ 0.00 → the model predicts
        // direction from OBI alone with no momentum context, producing unreliable high-confidence
        // calls (seen in 2026-06-17 T1: vol=0.00, P(UP)=0.046 → SL hit for -$0.49).
        //
        // The hourly snapshot's oracle fields (oracle_price, velocity, velocity_1s, acceleration,
        // funding_rate, oracle_drift_10m, oracle_drift_60m) are populated directly from the
        // Binance price raptor WS and are always current to the most recent WS tick.
        // Overriding the daily oracle fields with the hourly values ensures the training history
        // always reflects live Binance momentum, and hist_vol is computed from real BTC movement.
        let training_snapshot = if let Some(ref maker_snap) = ctx.maker_snapshot {
            let mut s = maker_snap.clone();
            // Override oracle/momentum fields with the always-fresh hourly snapshot values.
            s.oracle_price     = ctx.snapshot.oracle_price;
            s.velocity         = ctx.snapshot.velocity;
            s.velocity_1s      = ctx.snapshot.velocity_1s;
            s.acceleration     = ctx.snapshot.acceleration;
            s.funding_rate     = ctx.snapshot.funding_rate;
            s.oracle_drift_60m = ctx.snapshot.oracle_drift_60m;
            s.oracle_drift_10m = ctx.snapshot.oracle_drift_10m;
            s.hist_vol         = ctx.snapshot.hist_vol;
            s.institutional_pulse = ctx.snapshot.institutional_pulse;
            s.tide_coherence      = ctx.snapshot.tide_coherence;
            s.oi_delta_pct        = ctx.snapshot.oi_delta_pct;
            s.cvd_ratio           = ctx.snapshot.cvd_ratio;
            s
        } else {
            ctx.snapshot.clone()
        };
        self.push_snapshot(training_snapshot);
        self.maybe_retrain(&ctx.dynamic_config);

        let dc = &ctx.dynamic_config;
        // "Why no trades?" registry feed for gates above the veto! macro.
        let idle = |r: &str| crate::helpers::viper_status::report_reason(&ctx.crypto_filter, &self.name(), r);
        if !dc.enable_gboost {
            idle("disabled in config");
            return Ok(StrategySignal::NoSignal);
        }
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            idle("session drawdown limit hit");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate: market must be mature enough for orderbook features to be stable ──
        if (Utc::now() - ctx.market_started_at).num_seconds() < config::GBOOST_MIN_MARKET_AGE_SECS {
            idle("market too young");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate: hourly market health cross-check ────────────────────────────
        // When GBoost trades on a Window/Daily venue (maker_snapshot present), the
        // hourly market state is still a leading indicator for daily pricing.
        // A degenerate hourly book (ask_sum >> 1.0 or bid_sum << 0.7) means a
        // strong directional hourly move is under way — entering daily at this
        // moment means maximum adverse selection.
        if ctx.maker_snapshot.is_some() {
            let hourly_ask_sum = ctx.snapshot.yes_ask + ctx.snapshot.no_ask;
            let hourly_bid_sum = ctx.snapshot.yes_bid + ctx.snapshot.no_bid;
            if hourly_ask_sum > config::GBOOST_MAX_HOURLY_ASK_SUM
                || hourly_bid_sum < config::GBOOST_MIN_HOURLY_BID_SUM
            {
                tracing::debug!(
                    " GBoost entry blocked: hourly book degenerate (ask_sum={:.3} bid_sum={:.3})",
                    hourly_ask_sum, hourly_bid_sum
                );
                idle("hourly book degenerate");
                return Ok(StrategySignal::NoSignal);
            }

            // ── Gate: hourly market near-resolution guard ──────────────────────
            // When hourly YES bid < 0.05 or YES ask > 0.95, the hourly market has
            // effectively resolved in one direction.  Entering the DAILY YES in this
            // state means buying into a confirmed loser (bid < 0.05) or buying a coin
            // flip at maximum price (ask > 0.95) with no upside left.
            // OBI=0.0 on a dead market is the worst possible adverse context, not neutral.
            let hourly_yes_bid_f = ctx.snapshot.yes_bid.to_f64().unwrap_or(0.5);
            let hourly_yes_ask_f = ctx.snapshot.yes_ask.to_f64().unwrap_or(0.5);
            if hourly_yes_bid_f < config::GBOOST_MIN_HOURLY_YES_BID.to_f64().unwrap_or(0.05)
                || hourly_yes_ask_f > config::GBOOST_MAX_HOURLY_YES_ASK.to_f64().unwrap_or(0.95)
            {
                tracing::debug!(
                    " GBoost entry blocked: hourly market near-resolved \
                     (yes_bid={:.3} < {:.2} or yes_ask={:.3} > {:.2})",
                    hourly_yes_bid_f, config::GBOOST_MIN_HOURLY_YES_BID,
                    hourly_yes_ask_f, config::GBOOST_MAX_HOURLY_YES_ASK,
                );
                idle("hourly market near-resolved");
                return Ok(StrategySignal::NoSignal);
            }

            // ── Gate: hourly strong-trend block for daily entries ──────────────
            // When the current hourly market shows BTC strongly trending UP
            // (hourly YES bid > GBOOST_HOURLY_STRONG_TREND_BLOCK), entering DAILY NO
            // is systematically losing — the daily YES price follows the hourly.
            // Similarly, when hourly is strongly DOWN, entering DAILY YES is adverse.
            //
            // Evidence: 2026-06-17 T1 — hourly YES bid=$0.69 when GBoost entered DAILY NO
            // at $0.45.  DAILY NO fell to $0.39 as the hourly resolved YES (-$0.49 loss).
            // This gate at 0.65 would have blocked it (0.69 > 0.65).
            let trend_block = config::GBOOST_HOURLY_STRONG_TREND_BLOCK.to_f64().unwrap_or(0.65);
            let hourly_strong_up   = hourly_yes_bid_f > trend_block;
            let hourly_strong_down = hourly_yes_ask_f < (1.0 - trend_block);
            if hourly_strong_up || hourly_strong_down {
                tracing::debug!(
                    " GBoost entry blocked: hourly strong trend \
                     (yes_bid={:.3} strong_up={} yes_ask={:.3} strong_down={} threshold={:.2})",
                    hourly_yes_bid_f, hourly_strong_up,
                    hourly_yes_ask_f, hourly_strong_down,
                    trend_block
                );
                idle("hourly strong trend");
                return Ok(StrategySignal::NoSignal);
            }
        }

        // ── Gate: expiry guard ────────────────────────────────────────────────
        // GBoost should operate on the maker_market (Window/Daily) if available,
        // otherwise it falls back to the primary market (Hourly).
        let target_market = if let Some(ref mk) = ctx.maker_market {
            mk
        } else {
            &ctx.market
        };

        let target_snapshot = if ctx.maker_snapshot.is_some() {
            ctx.maker_snapshot.as_ref().unwrap()
        } else {
            &ctx.snapshot
        };

        // Build a prediction snapshot that mirrors the training snapshot convention:
        // daily OBI/price features from the maker snapshot, but oracle/momentum fields
        // patched from the hourly snapshot (always fresh from the Binance price raptor).
        // This keeps training features and prediction features consistent — the model was
        // trained with fresh oracle data (from push_snapshot above), so predictions must
        // also use fresh oracle data.
        let patched_maker_snapshot_storage: crate::state::MarketSnapshot;
        let predict_snapshot: &crate::state::MarketSnapshot = if ctx.maker_snapshot.is_some() {
            let mut s = ctx.maker_snapshot.as_ref().unwrap().clone();
            s.oracle_price     = ctx.snapshot.oracle_price;
            s.velocity         = ctx.snapshot.velocity;
            s.velocity_1s      = ctx.snapshot.velocity_1s;
            s.acceleration     = ctx.snapshot.acceleration;
            s.funding_rate     = ctx.snapshot.funding_rate;
            s.oracle_drift_60m = ctx.snapshot.oracle_drift_60m;
            s.oracle_drift_10m = ctx.snapshot.oracle_drift_10m;
            s.hist_vol         = ctx.snapshot.hist_vol;
            s.institutional_pulse = ctx.snapshot.institutional_pulse;
            s.tide_coherence      = ctx.snapshot.tide_coherence;
            s.oi_delta_pct        = ctx.snapshot.oi_delta_pct;
            s.cvd_ratio           = ctx.snapshot.cvd_ratio;
            patched_maker_snapshot_storage = s;
            &patched_maker_snapshot_storage
        } else {
            &ctx.snapshot
        };

        // ── Gate: target (daily/window) market spread gate ─────────────────
        // Guard against entering the DAILY book when it is too wide.
        // GBOOST_MAX_HOURLY_ASK_SUM checks the hourly book; this checks the
        // actual trading venue.  2026-05-07 afternoon: three entries with
        // target ask_sum = 1.07, 1.11, 1.48 — all hit SL, combined loss $1.17.
        // A healthy daily binary book sits at 1.01–1.04; anything wider means
        // the book is illiquid/broken and round-trip costs destroy any edge.
        let target_ask_sum = target_snapshot.yes_ask + target_snapshot.no_ask;
        if target_ask_sum > config::GBOOST_MAX_TARGET_ASK_SUM {
            tracing::debug!(
                " GBoost entry blocked: target book too wide (ask_sum={:.3} > max {:.3})",
                target_ask_sum, config::GBOOST_MAX_TARGET_ASK_SUM
            );
            idle("target book too wide");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Snapshot staleness gate ───────────────────────────────────────────
        // Stale snapshot depth values can let OBI gates silently pass when the actual
        // live book has moved adversely between WebSocket events.
        // 2026-05-07 T6 & T7: entry_hb_age_sec 16–35s; live snapshot OBI differed
        // from heartbeat OBI by > 0.50, causing adverse entries.
        let target_snap_age = (chrono::Utc::now() - target_snapshot.timestamp).num_seconds();
        if target_snap_age > config::GBOOST_MAX_SNAPSHOT_AGE_SECS {
            tracing::debug!(
                " GBoost entry blocked: target snapshot too stale ({}s > max {}s)",
                target_snap_age, config::GBOOST_MAX_SNAPSHOT_AGE_SECS
            );
            idle("target snapshot stale");
            return Ok(StrategySignal::NoSignal);
        }
        // Also gate on hourly snapshot staleness when trading the daily market.
        if ctx.maker_snapshot.is_some() {
            let hourly_snap_age = (chrono::Utc::now() - ctx.snapshot.timestamp).num_seconds();
            if hourly_snap_age > config::GBOOST_MAX_SNAPSHOT_AGE_SECS {
                tracing::debug!(
                    " GBoost entry blocked: hourly snapshot too stale ({}s > max {}s)",
                    hourly_snap_age, config::GBOOST_MAX_SNAPSHOT_AGE_SECS
                );
                idle("hourly snapshot stale");
                return Ok(StrategySignal::NoSignal);
            }
        }

        if let Some(close_time) = target_market.market_close_time {
            if (close_time - Utc::now()).num_seconds() < dc.gboost_min_secs_to_expiry {
                idle("too close to expiry");
                return Ok(StrategySignal::NoSignal);
            }
        }
        // ── Gate: sufficient collateral ───────────────────────────────────────
        if ctx.available_collateral < dc.gboost_max_exposure_usdc {
            idle("insufficient collateral");
            return Ok(StrategySignal::NoSignal);
        }

        // ── OBI EMA update (every evaluated tick) ────────────────────────────
        // Feed the smoothed-OBI slots on every tick that reaches evaluation so the
        // EMA tracks the book continuously — not just at the moment a gate fires.
        // The quality gates below consume these smoothed values.
        let smoothed_target_yes_obi = self.smoothed_obi(OBI_SLOT_TARGET_YES, Self::side_obi(true, target_snapshot));
        let smoothed_target_no_obi  = self.smoothed_obi(OBI_SLOT_TARGET_NO,  Self::side_obi(false, target_snapshot));
        let (smoothed_hourly_yes_obi, smoothed_hourly_no_obi) = if ctx.maker_snapshot.is_some() {
            (
                self.smoothed_obi(OBI_SLOT_HOURLY_YES, Self::side_obi(true, &ctx.snapshot)),
                self.smoothed_obi(OBI_SLOT_HOURLY_NO,  Self::side_obi(false, &ctx.snapshot)),
            )
        } else {
            (None, None)
        };

        let p_yes_up = match self.predict(predict_snapshot) {
            Some(p) => p,
            None    => { idle("model warming up"); return Ok(StrategySignal::NoSignal) },
        };

        // ── Diagnostic: periodic prediction-confidence visibility ────────────
        // GBoost trades rarely, so without this we are blind to how confident the
        // model actually gets. Log the conviction (distance from 0.50) and whether
        // it cleared the entry threshold, rate-limited so it never floods. This is
        // the data we use to calibrate gboost_entry_threshold.
        let entry_thresh = dc.gboost_entry_threshold.to_f64().unwrap_or(0.65);
        // A signal is "entry-eligible" once it clears the threshold on either side.
        // Used below to gate the eligible-but-vetoed diagnostic so we only log the
        // blocking gate for signals the model actually wanted to trade.
        let entry_eligible = p_yes_up >= entry_thresh || p_yes_up <= 1.0 - entry_thresh;
        {
            let conviction = (p_yes_up - 0.5).abs() * 2.0; // 0.0 at coin-flip, 1.0 at certainty
            let mut last = self.last_pred_log_at.lock().unwrap();
            let due = last.map_or(true, |t| t.elapsed().as_secs() >= config::GBOOST_PRED_LOG_INTERVAL_SECS);
            if due {
                *last = Some(Instant::now());
                tracing::info!(
                    " GboostStrategy: P(UP)={:.3} conviction={:.2} thr={:.2} {}",
                    p_yes_up, conviction, entry_thresh,
                    if entry_eligible { "→ ENTRY-ELIGIBLE (checking quality gates)" } else { "(below threshold)" },
                );
            }
        }

        // ── Entry-signal persistence debounce (anti-whipsaw, 2026-08-05) ─────
        // Observed the model flipping P(UP) 1.000 → 0.000 → 1.000 within 16 min:
        // it tracks instantaneous momentum, not settlement. Require the SAME side
        // to stay entry-eligible continuously for GBOOST_ENTRY_PERSISTENCE_SECS
        // before an entry is allowed. Brief dips below threshold are tolerated up
        // to GBOOST_SIGNAL_CONTINUITY_GAP_SECS; a side flip resets the clock.
        // Checked as the LAST gate in each entry block (mirrors Convergence's
        // debounce) so the veto shadow-log still records which hard gate binds.
        let signal_persisted = {
            let now = Instant::now();
            let mut streak = self.entry_signal_streak.lock().unwrap();
            if entry_eligible {
                let is_yes = p_yes_up >= entry_thresh;
                match streak.as_mut() {
                    Some((cid, dir, first_seen, last_seen))
                        if *cid == target_market.condition_id
                            && *dir == is_yes
                            && last_seen.elapsed().as_secs()
                                <= config::GBOOST_SIGNAL_CONTINUITY_GAP_SECS =>
                    {
                        *last_seen = now;
                        first_seen.elapsed().as_secs() >= config::GBOOST_ENTRY_PERSISTENCE_SECS
                    }
                    _ => {
                        // First sighting, side flip, market rotation, or stale streak.
                        *streak = Some((target_market.condition_id.clone(), is_yes, now, now));
                        false
                    }
                }
            } else {
                // Below threshold: leave the streak alone — the continuity gap
                // tolerates brief dips; a long lapse expires it naturally.
                false
            }
        };

        // Would-be entry parameters for the veto shadow-log (side the model wants).
        let (veto_side, veto_token, veto_ask) = if p_yes_up >= entry_thresh {
            ("YES", target_market.yes_token.clone(), target_snapshot.yes_ask)
        } else {
            ("NO", target_market.no_token.clone(), target_snapshot.no_ask)
        };
        let veto_secs_to_expiry = target_market.market_close_time
            .map(|ct| (ct - Utc::now()).num_seconds())
            .unwrap_or(-1);

        // Rate-limited INFO log naming which quality gate rejects an entry-eligible
        // signal. Only fires when the model cleared the threshold (entry_eligible),
        // so it isolates exactly why a confident GBoost signal did not become a trade.
        // Shares the GBOOST_PRED_LOG_INTERVAL_SECS cadence to avoid per-tick flooding.
        // Also shadow-logs the vetoed would-be entry to the gboost_vetoes table (same
        // throttle) so gate quality can be scored against settlement outcomes.
        macro_rules! veto {
            ($reason:expr) => {{
                // Unthrottled, cheap: feed the "why no trades?" registry every tick.
                crate::helpers::viper_status::report_reason(&ctx.crypto_filter, &self.name(), &$reason.to_string());
                if entry_eligible {
                    let mut last = self.last_veto_log_at.lock().unwrap();
                    let due = last.map_or(true, |t| t.elapsed().as_secs() >= config::GBOOST_PRED_LOG_INTERVAL_SECS);
                    if due {
                        *last = Some(Instant::now());
                        let reason = $reason.to_string();
                        tracing::info!(" GBoost eligible-but-VETOED [{}] | P(UP)={:.3}", reason, p_yes_up);
                        if let Some(pool) = crate::helpers::db::pool() {
                            let pool = pool.clone();
                            let market_name  = target_market.market_name.clone();
                            let condition_id = target_market.condition_id.clone();
                            let token_id     = veto_token.clone();
                            let ask          = veto_ask.to_string();
                            let oracle       = ctx.snapshot.oracle_price.to_string();
                            let drift        = ctx.snapshot.oracle_drift_60m.to_string();
                            // The regime the veto fired in. Same raptor value the
                            // flatness gate reads (the history's newest snapshot is
                            // the one pushed from ctx.snapshot this tick), read from
                            // ctx so the macro is usable before precomp_* exists.
                            let hist_vol     = ctx.snapshot.hist_vol.to_f64().unwrap_or(0.0);
                            tokio::spawn(async move {
                                crate::helpers::db::record_gboost_veto_db(
                                    &pool, &market_name, &condition_id, veto_side, token_id.as_str(),
                                    &ask, p_yes_up, &reason, &oracle, &drift, veto_secs_to_expiry,
                                    hist_vol,
                                ).await;
                            });
                        }
                    }
                }
                return Ok(StrategySignal::NoSignal);
            }};
        }

        // ── Gate: concept drift suppression ──────────────────────────────────
        // If the last retrain detected that live market data is flowing through
        // the model's split points very differently from the training distribution,
        // suppress entries until the next retrain recaptures the regime.
        if self.concept_drift_suppressed.load(Ordering::Relaxed) {
            veto!("concept drift");
        }

        // Pre-compute history-derived values once — shared by YES and NO entry paths.
        // Avoids acquiring the history lock twice and ensures prev_snap, hist_vol, and
        // tick_momentum are captured at the same tick for both sizing and pending_entries.
        let (precomp_prev_snap, precomp_hist_vol, precomp_tick_momentum) = {
            let h = self.history.lock().unwrap();
            let n = h.len();
            let ps = if n >= 2 { Some(h[n-2].clone()) } else { None };
            let hv = if n > 0 { hist_vol_from_deque(&h, n-1) } else { 0.0 };
            let tm = if n > 0 { tick_momentum_from_deque(&h, n-1) } else { 0.0 };
            (ps, hv, tm)
        };

        // ── Gate: minimum historical volatility ──────────────────────────────
        // When the oracle has been completely flat (hist_vol_regime == 0.0) the
        // model's velocity, acceleration, and volatility features carry no signal.
        // Any high-confidence prediction in a frozen oracle is unreliable and tends
        // to flip hard on the next retrain once the price finally moves.
        // Diagnosed from 2026-05-30 T1: vol=0.00 at entry → P(UP) flipped from
        // 0.087 → 0.779 in 12 min → SignalRev exit for -$0.1636.
        // Hot-tunable (`gboost_min_hist_vol`, defaults to the profile's
        // GBOOST_MIN_HIST_VOL). Score a candidate floor with
        // /api/gboost/veto-scores?max_hist_vol=… before lowering it.
        let min_hist_vol = dc.gboost_min_hist_vol.to_f64().unwrap_or(config::GBOOST_MIN_HIST_VOL);
        if precomp_hist_vol < min_hist_vol {
            veto!(format!("oracle too flat (hist_vol={:.4} < min={:.4})", precomp_hist_vol, min_hist_vol));
        }

        // ── Gate: trend-alignment ─────────────────────────────────────────────
        // If BTC has drifted strongly in one direction over the past 60 minutes,
        // entering counter-trend is systematically unprofitable.
        // Always uses the hourly snapshot for oracle data (drift is asset-level).
        //   drift >  +$200 → uptrend  → block NO entries
        //   drift < -$200  → downtrend → block YES entries
        // Mirrors MAKER_SLOW_TREND_THRESHOLD_BTC and TIME_DECAY_MAX_SLOW_DRIFT_BTC.
        let drift_60m = ctx.snapshot.oracle_drift_60m;
        let trend_block = config::oracle_threshold(config::GBOOST_TREND_DRIFT_BLOCK_PCT, ctx.snapshot.oracle_price);

        // ── Below-strike sustained suppressor ────────────────────────────────
        // If the daily market has a known strike price AND BTC spot has been
        // continuously at least BASIS_BTC_ORACLE_STRIKE_BUFFER below that strike
        // for ≥ GBOOST_BELOW_STRIKE_SUPPRESS_SECS, suppress YES entries.
        //
        // Rationale: a market priced below (strike − $150) for 60+ minutes is
        // pricing in NO predominance.  The hourly 60m drift gate ($200 threshold)
        // can miss this: BTC might only drift $112 in 1h but be $300 below strike
        // all session.  The strike-distance check catches this orthogonal condition.
        //
        // Reset: if BTC recovers above (strike − buffer), the suppressor is cleared.
        let below_strike_suppressed_for_yes = {
            let oracle_price = ctx.snapshot.oracle_price;
            let opt_strike = target_market.strike_price;
            if let Some(strike) = opt_strike {
                let buffer = config::oracle_threshold(config::BASIS_ORACLE_STRIKE_BUFFER_PCT, oracle_price);
                let threshold = strike - buffer;
                let mut bss = self.below_strike_since.lock().unwrap();
                if oracle_price < threshold {
                    // Spot is below the buffer — start or continue the timer
                    if bss.is_none() {
                        *bss = Some(Instant::now());
                        tracing::debug!(
                            " GBoost: BTC spot ${:.0} < strike(${:.0}) − buffer(${:.0}) = ${:.0} — starting below-strike timer",
                            oracle_price, strike, buffer, threshold
                        );
                    }
                    let elapsed_secs = bss.unwrap().elapsed().as_secs() as i64;
                    elapsed_secs >= config::GBOOST_BELOW_STRIKE_SUPPRESS_SECS
                } else {
                    // Spot recovered above the buffer — reset the timer
                    if bss.is_some() {
                        tracing::debug!(
                            "✅ GBoost: BTC spot ${:.0} >= threshold ${:.0} — below-strike timer reset",
                            oracle_price, threshold
                        );
                        *bss = None;
                    }
                    false
                }
            } else {
                false // no strike price for this market — don't suppress
            }
        };

        // Don't pyramid — check that no position is already open for this strategy.
        let (has_yes, has_no) = {
            let map = ctx.positions.lock().await;
            (
                map.contains_key(&PositionKey::new(&ctx.squadron_id, "GboostStrategy", target_market.yes_token.clone())),
                map.contains_key(&PositionKey::new(&ctx.squadron_id, "GboostStrategy", target_market.no_token.clone())),
            )
        };

        // ── YES entry: model predicts UP ──────────────────────────────────────
        if p_yes_up >= entry_thresh && !has_yes {
            // Trend-alignment: block YES entries in a downtrend
            if drift_60m < -trend_block {
                veto!(format!("counter-trend (drift_60m=${:.0} < -${:.0})", drift_60m, trend_block));
            }
            // Strike-distance: block YES entries when BTC has been below (strike−buffer) for 60+ min
            if below_strike_suppressed_for_yes {
                veto!("below-strike suppressed");
            }
            // ── Market-level holding lock ─────────────────────────────────────
            // After any position (YES or NO) was placed on this market, prevent
            // new entries on EITHER side for GBOOST_MIN_HOLDING_LOCK_SECS.
            // This stops rapid YES→NO flip chop where the model oscillates within
            // seconds of a quick SL exit.
            {
                let lock_map = self.market_hold_locks.lock().unwrap();
                if let Some(&lock_since) = lock_map.get(&target_market.condition_id) {
                    let elapsed = lock_since.elapsed().as_secs() as i64;
                    if elapsed < config::GBOOST_MIN_HOLDING_LOCK_SECS {
                        veto!("market hold lock active");
                    }
                }
            }
            // ── Entry latch: skip if an entry for this token is already in-flight ──
            // Between emitting an Entry signal and the position being confirmed in
            // pos_map (can be several seconds), evaluate_entry fires every tick.
            // Without this guard those ticks all re-emit Entry signals, flooding
            // the executor and potentially placing duplicate orders.
            if self.pending_entries.lock().unwrap().contains_key(&target_market.yes_token.clone()) {
                return Ok(StrategySignal::NoSignal);
            }
            if let Some(remaining_secs) = self.post_exit_cooldown_remaining_secs(target_market.yes_token.clone()) {
                veto!(format!("cooldown active ({remaining_secs}s left)"));
            }
            // Smoothed (EMA) OBI — see smoothed_obi(). None = zero-depth book with no
            // fresh EMA to fall back on: the book state is unknown, so don't trade it.
            if smoothed_target_yes_obi.is_none() {
                veto!("no OBI depth data");
            }
            let yes_obi = smoothed_target_yes_obi.unwrap();
            if yes_obi < dc.gboost_obi_adverse_block {
                veto!(format!("adverse OBI (yes_obi={:.2} < {:.2})", yes_obi, dc.gboost_obi_adverse_block));
            }
            // ── OBI exhaustion gate ───────────────────────────────────────────
            // When |obi| is very large the move is already mature — entering YES
            // into an already-resolved book is tail-chasing with maximum adverse
            // selection. 2026-05-08 T2: |obi_y|=0.61 on a ~93% YES market → SL -$0.457.
            if yes_obi.abs() > dc.gboost_obi_exhaustion_block {
                veto!(format!("OBI exhaustion (|yes_obi|={:.2} > {:.2})", yes_obi.abs(), dc.gboost_obi_exhaustion_block));
            }
            // ── Hourly OBI direction check for daily entries ──────────────────
            // When trading daily market, the hourly YES OBI foreshadows daily direction.
            // If hourly YES is being aggressively sold (OBI << 0), smart money is
            // fading a pump — entering daily YES contradicts the hourly signal.
            // 2026-05-07 afternoon: blocked entries where hourly OBI was -0.81 to -0.88.
            if ctx.maker_snapshot.is_some() {
                if smoothed_hourly_yes_obi.is_none() {
                    veto!("no hourly OBI depth data");
                }
                let hourly_yes_obi = smoothed_hourly_yes_obi.unwrap();
                if hourly_yes_obi < config::GBOOST_HOURLY_OBI_ADVERSE_BLOCK {
                    veto!(format!("hourly YES OBI adverse ({:.2} < {:.2})", hourly_yes_obi, config::GBOOST_HOURLY_OBI_ADVERSE_BLOCK));
                }
                // ── Hourly OBI exhaustion check ───────────────────────────────
                // When the hourly book is overwhelmingly bid-dominated (OBI > +threshold)
                // the momentum move on the hourly venue is exhausted — all buyers are in,
                // sellers haven't arrived yet.  The subsequent flush propagates to the daily
                // market, dragging daily YES down with it.
                // 2026-05-24 11:39: hourly YES OBI=0.85 at entry → price fell from $0.54
                // to $0.49 in 45 s (-$0.26 loss).  Blocked at OBI_EXHAUSTION_BLOCK=0.80.
                if hourly_yes_obi.abs() > dc.gboost_obi_exhaustion_block {
                    veto!(format!("hourly OBI exhausted (|{:.2}| > {:.2})", hourly_yes_obi, dc.gboost_obi_exhaustion_block));
                }
            }
            let price  = floor_to_tick_size(target_snapshot.yes_ask);
            if price >= dc.gboost_max_yes_entry_price
                || price < dc.gboost_min_entry_price
                || price <= dec!(0)
            {
                veto!(format!("YES price out of range (ask=${:.2}, allowed ${:.2}–${:.2})", price, dc.gboost_min_entry_price, dc.gboost_max_yes_entry_price));
            }
            // ── 50-cent coin-flip zone gate ───────────────────────────────────
            // Near 0.50 the market is directionally undecided. With 10% round-trip
            // taker fees, GBoost needs > 10% price move to break even — impossible
            // in a 50/50 coin flip. Require minimum edge distance from fair value.
            if (price - dec!(0.50)).abs() < dc.gboost_min_edge_from_fair {
                veto!(format!("price too close to 0.50 (ask=${:.2}, min edge {:.2})", price, dc.gboost_min_edge_from_fair));
            }
            // Confidence-proportional sizing: more capital for higher-conviction signals.
            // Volatility-scaled: reduce size when oracle hist_vol_regime is elevated.
            let trade_usdc = scale_trade_size(p_yes_up, entry_thresh, precomp_hist_vol, dc.gboost_max_exposure_usdc);
            // ── Minimum net profit gate ───────────────────────────────────────
            // Ensure expected TP gain after round-trip fees exceeds the minimum
            // viable profit threshold.  Prevents marginal bets where fee cost
            // consumes the majority of the expected gain (e.g. low-confidence
            // minimum-size entries near $0.45 with 3.5% round-trip fees).
            {
                let tp_pct = dc.gboost_target_profit_pct;
                // Dynamic Polymarket fee formula: fee = CRYPTO_FEE_RATE × p × (1-p)
                let price_f = price;
                let fee_rate_one_side = config::CRYPTO_FEE_RATE * price_f * (dec!(1) - price_f);
                let fee_roundtrip = fee_rate_one_side * dec!(2); // entry + exit
                let expected_gross = trade_usdc * tp_pct;
                let estimated_fee  = trade_usdc * fee_roundtrip;
                let net_expected   = expected_gross - estimated_fee;
                if net_expected < dc.gboost_min_net_profit_usdc {
                    veto!(format!("net profit too low (est ${:.2} < min ${:.2})", net_expected, dc.gboost_min_net_profit_usdc));
                }
            }
            // ── Persistence debounce ──────────────────────────────────────────
            if !signal_persisted {
                veto!(format!("persistence debounce (signal must hold {}s)", config::GBOOST_ENTRY_PERSISTENCE_SECS));
            }
            // ── Shadow mode (final gate) ──────────────────────────────────────
            // Every gate has passed: this is the order the model wants. While the
            // operator keeps `gboost_shadow_mode` on, it is recorded through the
            // veto shadow-log (gate "shadow mode" on /api/gboost/veto-scores, so
            // it is scored against settlement like any other gate) and no order
            // is placed. The model's live behavior has never been observed; this
            // is how it gets observed before it trades (B38).
            if dc.gboost_shadow_mode {
                veto!(format!("shadow mode: would enter YES @ ${:.2} for ${:.2} (P(UP)={:.3})", price, trade_usdc, p_yes_up));
            }
            let shares = trade_usdc / price;
            tracing::info!(
                " GBoost YES entry: P(UP)={:.3} | ask=${:.4} shares={:.2} usdc={:.2} vol={:.2}",
                p_yes_up, price, shares, trade_usdc, precomp_hist_vol
            );
            let (entry_prev_snap, entry_hist_vol, entry_tick_momentum) =
                (precomp_prev_snap.clone(), precomp_hist_vol, precomp_tick_momentum);
            self.pending_entries.lock().unwrap().insert(
                target_market.yes_token.clone(),
                (target_snapshot.clone(), entry_prev_snap, price, entry_hist_vol, entry_tick_momentum)
            );
            // Viper Backtrace: persist the model/decision state for this entry.
            crate::helpers::metrics::stash_entry_signals_json(target_market.yes_token.as_str(), serde_json::json!({
                "viper": "GBoost",
                "side": "YES",
                "p_yes_up": p_yes_up,
                "entry_thresh": entry_thresh,
                "entry_price": price.to_string(),
                "trade_usdc": trade_usdc.to_string(),
                "hist_vol": precomp_hist_vol,
                "tick_momentum": precomp_tick_momentum,
            }));
            // Record market-level hold lock to prevent rapid flip chop.
            self.market_hold_locks.lock().unwrap()
                .insert(target_market.condition_id.clone(), Instant::now());
            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: target_market.yes_token.clone(),
                    price, shares,
                    fee_bps:     target_market.yes_fee_bps as u16,
                    is_neg_risk: target_market.is_neg_risk,
                    market_name: target_market.market_name.clone(),
                    condition_id: target_market.condition_id.clone(),
                    order_type: TimeInForce::Fak,
                    post_only: false,
                    ghost_mode: dc.ghost_mode,
                },
                pair_params: None,
            });
        }

        // ── NO entry: model predicts DOWN (P(UP) is very low) ────────────────
        if p_yes_up <= (1.0 - entry_thresh) && !has_no {
            // Trend-alignment: block NO entries in an uptrend
            if drift_60m > trend_block {
                veto!(format!("counter-trend (drift_60m=+${:.0} > ${:.0})", drift_60m, trend_block));
            }
            // ── Market-level holding lock ─────────────────────────────────────
            {
                let lock_map = self.market_hold_locks.lock().unwrap();
                if let Some(&lock_since) = lock_map.get(&target_market.condition_id) {
                    let elapsed = lock_since.elapsed().as_secs() as i64;
                    if elapsed < config::GBOOST_MIN_HOLDING_LOCK_SECS {
                        veto!("market hold lock active");
                    }
                }
            }
            // ── Entry latch ──────────────────────────────────────────────────
            if self.pending_entries.lock().unwrap().contains_key(&target_market.no_token.clone()) {
                return Ok(StrategySignal::NoSignal);
            }
            if let Some(remaining_secs) = self.post_exit_cooldown_remaining_secs(target_market.no_token.clone()) {
                veto!(format!("cooldown active ({remaining_secs}s left)"));
            }
            // Smoothed (EMA) OBI — see smoothed_obi(). None = unknown book state.
            if smoothed_target_no_obi.is_none() {
                veto!("no OBI depth data");
            }
            let no_obi = smoothed_target_no_obi.unwrap();
            if no_obi < dc.gboost_obi_adverse_block {
                veto!(format!("adverse OBI (no_obi={:.2} < {:.2})", no_obi, dc.gboost_obi_adverse_block));
            }
            // ── OBI exhaustion gate ───────────────────────────────────────────
            // Blocks NO entries where the book is already heavily one-sided against
            // the NO leg — the move is in progress and the risk/reward is exhausted.
            if no_obi.abs() > dc.gboost_obi_exhaustion_block {
                veto!(format!("OBI exhaustion (|no_obi|={:.2} > {:.2})", no_obi.abs(), dc.gboost_obi_exhaustion_block));
            }
            // ── Hourly OBI direction check for daily entries ──────────────────
            // When trading daily market, the hourly NO OBI reveals whether smart money
            // is selling NO (= they think BTC went UP, so NO is losing = bad for NO entry).
            // If hourly NO is being aggressively sold (obi_n << 0), entering daily NO
            // directly contradicts the hourly directional signal.
            // 2026-05-07 trade 2: hourly NO OBI = -0.88 → blocked (saved $0.30).
            // 2026-05-07 trade 9: hourly NO OBI = -0.81 → blocked (saved $0.30).
            if ctx.maker_snapshot.is_some() {
                if smoothed_hourly_no_obi.is_none() {
                    veto!("no hourly OBI depth data");
                }
                let hourly_no_obi = smoothed_hourly_no_obi.unwrap();
                if hourly_no_obi < config::GBOOST_HOURLY_OBI_ADVERSE_BLOCK {
                    veto!(format!("hourly NO OBI adverse ({:.2} < {:.2})", hourly_no_obi, config::GBOOST_HOURLY_OBI_ADVERSE_BLOCK));
                }
                // ── Hourly OBI exhaustion check ───────────────────────────────
                // Mirrors the YES exhaustion check: when hourly NO book is overwhelmingly
                // bid-dominated (OBI > +threshold), the NO-side momentum is exhausted and
                // a reversal is imminent.  Entering daily NO at this point means buying
                // into the last ticks of a NO surge — adverse selection is at its peak.
                if hourly_no_obi.abs() > dc.gboost_obi_exhaustion_block {
                    veto!(format!("hourly OBI exhausted (|{:.2}| > {:.2})", hourly_no_obi, dc.gboost_obi_exhaustion_block));
                }
            }
            let price  = floor_to_tick_size(target_snapshot.no_ask);
            if price > dc.gboost_max_no_entry_price
                || price < dc.gboost_min_entry_price
                || price <= dec!(0)
            {
                veto!(format!("NO price out of range (ask=${:.2}, allowed ${:.2}–${:.2})", price, dc.gboost_min_entry_price, dc.gboost_max_no_entry_price));
            }
            // ── 50-cent coin-flip zone gate ───────────────────────────────────
            if (price - dec!(0.50)).abs() < dc.gboost_min_edge_from_fair {
                veto!(format!("price too close to 0.50 (ask=${:.2}, min edge {:.2})", price, dc.gboost_min_edge_from_fair));
            }
            // Confidence for NO direction is (1 - p_yes_up); scale size accordingly.
            let trade_usdc = scale_trade_size(1.0 - p_yes_up, entry_thresh, precomp_hist_vol, dc.gboost_max_exposure_usdc);
            // ── Minimum net profit gate ───────────────────────────────────────
            {
                let tp_pct = dc.gboost_target_profit_pct;
                let price_f = price;
                let fee_rate_one_side = config::CRYPTO_FEE_RATE * price_f * (dec!(1) - price_f);
                let fee_roundtrip = fee_rate_one_side * dec!(2);
                let expected_gross = trade_usdc * tp_pct;
                let estimated_fee  = trade_usdc * fee_roundtrip;
                let net_expected   = expected_gross - estimated_fee;
                if net_expected < dc.gboost_min_net_profit_usdc {
                    veto!(format!("net profit too low (est ${:.2} < min ${:.2})", net_expected, dc.gboost_min_net_profit_usdc));
                }
            }
            // ── Persistence debounce ──────────────────────────────────────────
            if !signal_persisted {
                veto!(format!("persistence debounce (signal must hold {}s)", config::GBOOST_ENTRY_PERSISTENCE_SECS));
            }
            // ── Shadow mode (final gate) ──────────────────────────────────────
            // Every gate has passed: this is the order the model wants. While the
            // operator keeps `gboost_shadow_mode` on, it is recorded through the
            // veto shadow-log (gate "shadow mode" on /api/gboost/veto-scores, so
            // it is scored against settlement like any other gate) and no order
            // is placed. The model's live behavior has never been observed; this
            // is how it gets observed before it trades (B38).
            if dc.gboost_shadow_mode {
                veto!(format!("shadow mode: would enter NO @ ${:.2} for ${:.2} (P(UP)={:.3})", price, trade_usdc, p_yes_up));
            }
            let shares = trade_usdc / price;
            tracing::info!(
                " GBoost NO entry: P(UP)={:.3} | ask=${:.4} shares={:.2} usdc={:.2} vol={:.2}",
                p_yes_up, price, shares, trade_usdc, precomp_hist_vol
            );
            let (entry_prev_snap, entry_hist_vol, entry_tick_momentum) =
                (precomp_prev_snap.clone(), precomp_hist_vol, precomp_tick_momentum);
            self.pending_entries.lock().unwrap().insert(
                target_market.no_token.clone(),
                (target_snapshot.clone(), entry_prev_snap, price, entry_hist_vol, entry_tick_momentum)
            );
            // Viper Backtrace: persist the model/decision state for this entry.
            crate::helpers::metrics::stash_entry_signals_json(target_market.no_token.as_str(), serde_json::json!({
                "viper": "GBoost",
                "side": "NO",
                "p_yes_up": p_yes_up,
                "entry_thresh": entry_thresh,
                "entry_price": price.to_string(),
                "trade_usdc": trade_usdc.to_string(),
                "hist_vol": precomp_hist_vol,
                "tick_momentum": precomp_tick_momentum,
            }));
            // Record market-level hold lock to prevent rapid flip chop.
            self.market_hold_locks.lock().unwrap()
                .insert(target_market.condition_id.clone(), Instant::now());
            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: target_market.no_token.clone(),
                    price, shares,
                    fee_bps:     target_market.no_fee_bps as u16,
                    is_neg_risk: target_market.is_neg_risk,
                    market_name: target_market.market_name.clone(),
                    condition_id: target_market.condition_id.clone(),
                    order_type: TimeInForce::Fak,
                    post_only: false,
                    ghost_mode: dc.ghost_mode,
                },
                pair_params: None,
            });
        }

        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        // GBoost should operate on the maker_market (Window/Daily) if available,
        // otherwise it falls back to the primary market (Hourly).
        let target_market = if let Some(ref mk) = ctx.maker_market {
            mk
        } else {
            &ctx.market
        };

        let target_snapshot = if ctx.maker_snapshot.is_some() {
            ctx.maker_snapshot.as_ref().unwrap()
        } else {
            &ctx.snapshot
        };

        let p_yes_up           = self.predict(target_snapshot);
        let signal_exit_thresh = dc.gboost_signal_exit_threshold.to_f64().unwrap_or(0.40);
        let tp                 = dc.gboost_target_profit_pct.to_f64().unwrap_or(0.15);
        let sl                 = dc.gboost_stop_loss_pct.to_f64().unwrap_or(0.10);

        let pos_map = ctx.positions.lock().await;

        // ── YES position ──────────────────────────────────────────────────────
        if let Some(pos) = pos_map.get(&PositionKey::new(&ctx.squadron_id, "GboostStrategy", target_market.yes_token.clone())) {
            // Ghost fills count as confirmed (Position::fill_effective_at).
            // This gate wraps the ENTIRE exit block for the leg, so in ghost mode
            // GBoost had no exits whatsoever — not take-profit, not stop-loss,
            // not the catastrophic floor. A simulated position could only leave
            // by squadron stand-down or market expiry.
            if let Some(fill_at) = pos.fill_effective_at(dc.ghost_mode) {
                let bid = target_snapshot.yes_bid;
                let profit_pct = if pos.avg_entry > dec!(0) {
                    ((bid - pos.avg_entry) / pos.avg_entry).to_f64().unwrap_or(0.0)
                } else { 0.0 };
                let secs_held = (Utc::now() - fill_at).num_seconds();

                let exit_params = || OrderParams {
                    token_id: target_market.yes_token.clone(),
                    price: bid, shares: pos.shares,
                    fee_bps: target_market.yes_fee_bps as u16,
                    is_neg_risk: target_market.is_neg_risk,
                    market_name: target_market.market_name.clone(),
                    condition_id: target_market.condition_id.clone(),
                    order_type: TimeInForce::Fak,
                    post_only: false,
                    ghost_mode: dc.ghost_mode,
                };

                if profit_pct >= tp {
                    self.record_training_outcome_on_exit(target_market.yes_token.clone(), profit_pct > 0.0);
                    self.mark_post_exit_cooldown(target_market.yes_token.clone());
                    return Ok(StrategySignal::Exit {
                        params: exit_params(),
                        reason: format!("GBoost TP YES: gain={:.2}%", profit_pct * 100.0),
                        exit_pair: false,
                    });
                }
                if secs_held >= config::GBOOST_SL_MIN_HOLD_SECS && profit_pct <= -sl {
                    self.record_training_outcome_on_exit(target_market.yes_token.clone(), profit_pct > 0.0);
                    self.mark_post_exit_cooldown_sl(target_market.yes_token.clone());
                    return Ok(StrategySignal::Exit {
                        params: exit_params(),
                        reason: format!("GBoost SL YES: loss={:.2}% ({}s)", profit_pct * 100.0, secs_held),
                        exit_pair: false,
                    });
                }
                // Signal reversal: model now strongly predicts DOWN while we are long YES.
                // Uses the longer GBOOST_MIN_HOLD_SECS to prevent whipsawing on neutral ticks.
                if let Some(p) = p_yes_up {
                    if secs_held >= config::GBOOST_MIN_HOLD_SECS && p <= signal_exit_thresh {
                        // Gate: only exit on signal reversal if the position has either
                        // (a) cleared round-trip spread costs (profit_pct ≥ SIGNAL_REV_MIN_PROFIT), OR
                        // (b) is already deep enough in the hole (loss ≥ half the SL) that
                        //     early protective exit is better than waiting for the full SL.
                        // This prevents exiting break-even positions that merely wasted spread.
                        let half_sl = sl / 2.0;
                        if profit_pct >= config::GBOOST_SIGNAL_REV_MIN_PROFIT || profit_pct <= -half_sl {
                            self.record_training_outcome_on_exit(target_market.yes_token.clone(), profit_pct > 0.0);
                            self.mark_post_exit_cooldown(target_market.yes_token.clone());
                            return Ok(StrategySignal::Exit {
                                params: exit_params(),
                                reason: format!("GBoost SignalRev YES: P(UP)={:.3}", p),
                                exit_pair: false,
                            });
                        } else {
                            tracing::debug!(
                                " GBoost SignalRev YES suppressed: profit={:.2}% not yet above min {:.0}% (not deep enough in red for protective exit)",
                                profit_pct * 100.0, config::GBOOST_SIGNAL_REV_MIN_PROFIT * 100.0
                            );
                        }
                    }
                }
            }
        }

        // ── NO position ───────────────────────────────────────────────────────
        if let Some(pos) = pos_map.get(&PositionKey::new(&ctx.squadron_id, "GboostStrategy", target_market.no_token.clone())) {
            // Ghost fills count as confirmed (Position::fill_effective_at).
            // This gate wraps the ENTIRE exit block for the leg, so in ghost mode
            // GBoost had no exits whatsoever — not take-profit, not stop-loss,
            // not the catastrophic floor. A simulated position could only leave
            // by squadron stand-down or market expiry.
            if let Some(fill_at) = pos.fill_effective_at(dc.ghost_mode) {
                let bid = target_snapshot.no_bid;
                let profit_pct = if pos.avg_entry > dec!(0) {
                    ((bid - pos.avg_entry) / pos.avg_entry).to_f64().unwrap_or(0.0)
                } else { 0.0 };
                let secs_held = (Utc::now() - fill_at).num_seconds();

                let exit_params = || OrderParams {
                    token_id: target_market.no_token.clone(),
                    price: bid, shares: pos.shares,
                    fee_bps: target_market.no_fee_bps as u16,
                    is_neg_risk: target_market.is_neg_risk,
                    market_name: target_market.market_name.clone(),
                    condition_id: target_market.condition_id.clone(),
                    order_type: TimeInForce::Fak,
                    post_only: false,
                    ghost_mode: dc.ghost_mode,
                };

                if profit_pct >= tp {
                    self.record_training_outcome_on_exit(target_market.no_token.clone(), profit_pct > 0.0);
                    self.mark_post_exit_cooldown(target_market.no_token.clone());
                    return Ok(StrategySignal::Exit {
                        params: exit_params(),
                        reason: format!("GBoost TP NO: gain={:.2}%", profit_pct * 100.0),
                        exit_pair: false,
                    });
                }
                if secs_held >= config::GBOOST_SL_MIN_HOLD_SECS && profit_pct <= -sl {
                    self.record_training_outcome_on_exit(target_market.no_token.clone(), profit_pct > 0.0);
                    self.mark_post_exit_cooldown_sl(target_market.no_token.clone());
                    return Ok(StrategySignal::Exit {
                        params: exit_params(),
                        reason: format!("GBoost SL NO: loss={:.2}% ({}s)", profit_pct * 100.0, secs_held),
                        exit_pair: false,
                    });
                }
                // Signal reversal for NO: model now strongly predicts UP.
                // Uses the longer GBOOST_MIN_HOLD_SECS to prevent whipsawing on neutral ticks.
                if let Some(p) = p_yes_up {
                    if secs_held >= config::GBOOST_MIN_HOLD_SECS && p >= (1.0 - signal_exit_thresh) {
                        // Same minimum-profit gate as YES reversal — don't exit if the position
                        // hasn't covered round-trip costs and isn't deeply in the red.
                        let half_sl = sl / 2.0;
                        if profit_pct >= config::GBOOST_SIGNAL_REV_MIN_PROFIT || profit_pct <= -half_sl {
                            self.record_training_outcome_on_exit(target_market.no_token.clone(), profit_pct > 0.0);
                            self.mark_post_exit_cooldown(target_market.no_token.clone());
                            return Ok(StrategySignal::Exit {
                                params: exit_params(),
                                reason: format!("GBoost SignalRev NO: P(UP)={:.3}", p),
                                exit_pair: false,
                            });
                        } else {
                            tracing::debug!(
                                " GBoost SignalRev NO suppressed: profit={:.2}% not yet above min {:.0}% (not deep enough in red for protective exit)",
                                profit_pct * 100.0, config::GBOOST_SIGNAL_REV_MIN_PROFIT * 100.0
                            );
                        }
                    }
                }
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    fn name(&self) -> String { "GboostStrategy".to_string() }
    fn venue(&self) -> &'static str { "Window/Daily" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::GBOOST_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Gross one-sided" }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use crate::state::{MarketConfig, Position, PositionMap};
    use crate::helpers::dynamic_config::DynamicConfig;
    // use alloy::primitives::U256; // Already imported by the main file

    fn make_snapshot() -> MarketSnapshot {
        MarketSnapshot {
            yes_bid: dec!(0.50), yes_bid_depth: dec!(200),
            yes_ask: dec!(0.52), yes_ask_depth: dec!(150),
            no_bid:  dec!(0.48), no_bid_depth:  dec!(180),
            no_ask:  dec!(0.50), no_ask_depth:  dec!(160),
            // Deeper than the touch, as a real book is — a fixture where the two
            // are equal would hide any bug that confuses them.
            yes_bid_depth_total: dec!(1200), yes_ask_depth_total: dec!(900),
            no_bid_depth_total:  dec!(1100), no_ask_depth_total:  dec!(950),
            oracle_price: dec!(95000),
            velocity: dec!(50), velocity_1s: dec!(10), acceleration: dec!(5),
            funding_rate: dec!(0.0001), oracle_drift_60m: dec!(100),
            oracle_drift_10m: dec!(30), // ~10min drift for test
            hist_vol: dec!(0.003), // normal live-BTC 60m realized-vol — clears GBOOST_MIN_HIST_VOL
            institutional_pulse: dec!(0.5), tide_coherence: dec!(0.7),
            tradfi_velocity: dec!(0), macro_coherence: dec!(0),
            vix_proxy: dec!(0), vix_velocity: dec!(0),
            oi_delta_pct: dec!(0.01), cvd_ratio: dec!(1.2),
            secs_to_expiry: 3600, // 1 hour — mid-range for tests
            timestamp: Utc::now(),
        }
    }

    fn make_ctx() -> StrategyContext {
        StrategyContext {
            squadron_id: "test-squadron".to_string(),
            market: MarketConfig {
                yes_token: MarketId::new("1"), no_token: MarketId::new("2"),
                market_name: "Test".to_string(),
                market_close_time: Some(Utc::now() + chrono::Duration::hours(1)),
                strike_price: None, is_neg_risk: false,
                condition_id: "abc".to_string(),
                yes_fee_bps: 0, no_fee_bps: 0,
            },
            snapshot: make_snapshot(),
            positions: Arc::new(Mutex::new(PositionMap::new())),
            session_pnl: dec!(0), starting_collateral: dec!(500),
            available_collateral: dec!(200),
            crypto_filter: "btc".to_string(),
            market_started_at: Utc::now() - chrono::Duration::seconds(300),
            maker_market: None, // Added missing field
            maker_snapshot: None, // Added missing field
            dynamic_config: Arc::new(DynamicConfig::default()),
            arb_market_lockouts: None,
        }
    }

    #[test]
    fn extract_features_ranges() {
        let snap = make_snapshot();
        let feats = extract_features(&snap, None, 0.0, 0.0); // Pass None for prev_s in test
        assert_eq!(feats.len(), NUM_FEATURES);
        assert!(feats[0].abs() <= 1.0, "yes_obi out of [-1,1]: {}", feats[0]);
        assert!(feats[1].abs() <= 1.0, "no_obi  out of [-1,1]: {}", feats[1]);
        // oracle normalized: 95000 / 100000 = 0.95
        assert!((feats[11] - 0.95).abs() < 0.01, "oracle_price feat: {}", feats[11]);
        // secs_to_expiry_norm: 3600 / 14400 = 0.25
        assert!((feats[12] - 0.25).abs() < 0.01, "secs_to_expiry_norm feat: {}", feats[12]);
        assert!(feats[12] >= 0.0 && feats[12] <= 1.0, "secs_to_expiry_norm out of [0,1]: {}", feats[12]);
        // new features [19-21] should be in their normalized ranges
        assert!(feats[19] >= -1.0 && feats[19] <= 1.0, "spread_velocity out of [-1,1]: {}", feats[19]);
        assert!(feats[20] >= 0.0 && feats[20] <= 1.0, "hist_vol out of [0,1]: {}", feats[20]);
        assert!(feats[21] >= -1.0 && feats[21] <= 1.0, "tick_momentum out of [-1,1]: {}", feats[21]);
    }

    #[test]
    fn train_model_returns_booster() {
        // This test needs to be updated to use TrainingSample
        let n = config::GBOOST_MIN_TRAINING_SAMPLES + 10; // No lookahead needed
        let mut samples: Vec<TrainingSample> = Vec::with_capacity(n);
        for i in 0..n {
            let snap = make_snapshot(); // Dummy snapshot
            samples.push(TrainingSample {
                features: extract_features(&snap, None, 0.0, 0.0), // Pass None for prev_s in test
                is_profitable: i % 2 == 0, // Alternate profitable/unprofitable
                entry_timestamp: Utc::now(),
            });
        }
        let booster = train_model(samples, config::GBOOST_BUDGET.to_f32().unwrap_or(0.8), config::GBOOST_ITERATION_LIMIT)
            .expect("train_model should succeed");
        assert!(!booster.trees.is_empty(), "booster should have trees after training");
    }

    /// Synthesize a learnable dataset: feature[0] separates the classes, the
    /// rest is noise. The existing `train_model_returns_booster` fixture uses
    /// identical snapshots with alternating labels, which is pure noise — the
    /// booster stops almost immediately on it, so it cannot show whether the
    /// budget knob is doing anything.
    fn learnable_samples(n: usize) -> Vec<TrainingSample> {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let profitable = i % 2 == 0;
            let mut features = [0.0f64; NUM_FEATURES];
            // Signal with overlap, so the fit has something to keep refining
            // rather than separating the classes in one split.
            features[0] = if profitable { 0.60 } else { 0.40 }
                + ((i % 17) as f64) * 0.01;
            for (k, f) in features.iter_mut().enumerate().skip(1) {
                *f = (((i * 31 + k * 7) % 100) as f64) / 100.0;
            }
            out.push(TrainingSample {
                features,
                is_profitable: profitable,
                entry_timestamp: Utc::now(),
            });
        }
        out
    }

    /// The budget knob has to reach the booster. It was a compile-time constant
    /// read inside `train_model`; it is now a `DynamicConfig` field passed in, so
    /// this proves the value is actually consumed rather than shadowed by the
    /// constant it used to read.
    #[test]
    fn training_budget_changes_how_many_trees_are_grown() {
        let n = config::GBOOST_MIN_TRAINING_SAMPLES + 10;
        let lean = train_model(learnable_samples(n), 0.15, config::GBOOST_ITERATION_LIMIT)
            .expect("low-budget fit succeeds");
        let rich = train_model(learnable_samples(n), 1.50, config::GBOOST_ITERATION_LIMIT)
            .expect("high-budget fit succeeds");
        assert!(
            rich.trees.len() > lean.trees.len(),
            "budget is not reaching the booster: 0.15 gave {} trees, 1.50 gave {}",
            lean.trees.len(),
            rich.trees.len(),
        );
    }

    /// The iteration cap has to bound the fit regardless of budget. This is the
    /// wall-clock guard: training runs on a blocking thread.
    #[test]
    fn iteration_limit_caps_a_generous_budget() {
        let n = config::GBOOST_MIN_TRAINING_SAMPLES + 10;
        let capped = train_model(learnable_samples(n), 1.50, 5)
            .expect("capped fit succeeds");
        assert!(
            capped.trees.len() <= 5,
            "iteration limit of 5 produced {} trees",
            capped.trees.len(),
        );
    }

    // ── Training-matrix layout (B38) ─────────────────────────────────────────

    /// Deterministic pseudo-random features in [0, 1) with a label that depends on
    /// two of them and on nothing periodic, so the only way a fit can separate the
    /// classes is by seeing each sample's own features in the columns perpetual
    /// reads. The older `learnable_samples` fixture alternates labels with the
    /// sample index, which a scrambled column can partly track by accident.
    fn structured_samples(n: usize, seed: u64) -> Vec<TrainingSample> {
        let mut state = seed;
        let mut next = move || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            ((state >> 11) as f64) / ((1u64 << 53) as f64)
        };
        (0..n).map(|_| {
            let mut features = [0.0f64; NUM_FEATURES];
            for f in features.iter_mut() { *f = next(); }
            let is_profitable = features[7] + 0.5 * features[19] > 0.75;
            TrainingSample { features, is_profitable, entry_timestamp: Utc::now() }
        }).collect()
    }

    /// Rank-based AUC (Mann-Whitney): 0.5 is chance, 1.0 is perfect ordering.
    fn auc(scores: &[f64], labels: &[bool]) -> f64 {
        let mut idx: Vec<usize> = (0..scores.len()).collect();
        idx.sort_by(|a, b| scores[*a].partial_cmp(&scores[*b]).unwrap());
        let npos = labels.iter().filter(|l| **l).count() as f64;
        let nneg = labels.len() as f64 - npos;
        let rank_sum: f64 = idx.iter().enumerate()
            .filter(|(_, i)| labels[**i])
            .map(|(r, _)| (r + 1) as f64)
            .sum();
        (rank_sum - npos * (npos + 1.0) / 2.0) / (npos * nneg)
    }

    /// The fill contract, pinned against perpetual's own accessor: element (i, j)
    /// of the matrix handed to the booster must be feature j of sample i.
    #[test]
    fn matrix_fill_matches_perpetual_column_major_layout() {
        let rows: Vec<[f64; NUM_FEATURES]> = (0..7).map(|i| {
            let mut r = [0.0f64; NUM_FEATURES];
            for (j, v) in r.iter_mut().enumerate() { *v = (i * 100 + j) as f64; }
            r
        }).collect();
        let data = column_major_matrix_data(&rows);
        let m = Matrix::new(&data, rows.len(), NUM_FEATURES);
        for (i, row) in rows.iter().enumerate() {
            for (j, expected) in row.iter().enumerate() {
                assert_eq!(m.get(i, j), expected, "element ({i}, {j})");
            }
        }
    }

    /// A booster fit on a learnable dataset must be able to order its own training
    /// data when asked about it the way production asks: one correctly laid out
    /// row at a time (a one-row matrix is the same in both layouts). Under the
    /// row-major fill the model learned scrambled columns and this AUC sits near
    /// 0.5 (the production pool gave 0.536 in-sample); it is the regression guard
    /// for B38. The batch matrix the drift check builds must agree with the
    /// single-row path, which pins the helper's layout a second way.
    #[test]
    fn fitted_model_separates_its_own_training_data() {
        let n = config::GBOOST_MIN_TRAINING_SAMPLES + 10;
        let samples = structured_samples(n, 0x9E37_79B9_7F4A_7C15);
        let labels: Vec<bool> = samples.iter().map(|s| s.is_profitable).collect();
        let rows: Vec<[f64; NUM_FEATURES]> = samples.iter().map(|s| s.features).collect();
        let booster = train_model(samples, 0.8, config::GBOOST_ITERATION_LIMIT).expect("fit succeeds");
        assert!(model_has_current_layout(&booster), "a freshly trained model must carry the layout tag");
        // The tag has to survive the disk round trip the startup loader checks;
        // a model without it (every file written before B38) is refused there.
        let reloaded = PerpetualBooster::from_json(&booster.json_dump().expect("dump")).expect("reload");
        assert!(model_has_current_layout(&reloaded), "layout tag must survive json_dump/from_json");
        let mut untagged = PerpetualBooster::default();
        untagged.metadata.clear();
        assert!(!model_has_current_layout(&untagged), "a model with no tag must be refused");

        // Production path: one row per prediction.
        let single: Vec<f64> = rows.iter()
            .map(|row| booster.predict_proba(&Matrix::new(row, 1, NUM_FEATURES), false, false)[0])
            .collect();
        let a = auc(&single, &labels);
        assert!(
            a > 0.9,
            "in-sample AUC {a:.3}: a booster that cannot order its own training data was fit on scrambled columns",
        );

        // Batch path (drift check): must be the same numbers.
        let data = column_major_matrix_data(&rows);
        let batch = booster.predict_proba(&Matrix::new(&data, n, NUM_FEATURES), false, false);
        for (i, (s, b)) in single.iter().zip(&batch).enumerate() {
            assert!((s - b).abs() < 1e-9, "row {i}: single-row {s} vs batch {b}");
        }
    }

    // ── Shadow mode (B38 landing) ────────────────────────────────────────────

    /// A model that is decisively bullish on `make_snapshot()`: the label follows
    /// the oracle_price feature ([11]) and the fixture's oracle is 95000 (0.95).
    /// Noise on the other features keeps the fit from collapsing to one stump, so
    /// it clears GBOOST_STRUCTURAL_MIN_TREES.
    fn model_bullish_on_fixture() -> PerpetualBooster {
        let n = config::GBOOST_MIN_TRAINING_SAMPLES + 10;
        let mut samples = structured_samples(n, 0xD1B5_4A32_D192_ED03);
        for s in samples.iter_mut() {
            s.is_profitable = s.features[11] + 0.3 * (s.features[19] - 0.5) > 0.5;
        }
        let booster = train_model(samples, 1.50, config::GBOOST_ITERATION_LIMIT).expect("fit succeeds");
        assert!(
            booster.trees.len() >= config::GBOOST_STRUCTURAL_MIN_TREES,
            "fixture model has {} trees, below the {} the predictor requires",
            booster.trees.len(), config::GBOOST_STRUCTURAL_MIN_TREES,
        );
        booster
    }

    /// A context that clears every deterministic entry gate for a YES entry, so
    /// the only things between the model and an order are the persistence
    /// debounce (satisfied by hand in `satisfy_persistence`) and shadow mode.
    fn entry_ready_ctx(asset: &str, shadow: bool) -> StrategyContext {
        let mut ctx = make_ctx();
        ctx.crypto_filter = asset.to_string();
        ctx.market_started_at = Utc::now() - chrono::Duration::seconds(config::GBOOST_MIN_MARKET_AGE_SECS + 60);
        ctx.snapshot.yes_ask = dec!(0.42); // inside the YES band, clear of the coin-flip zone
        ctx.snapshot.no_ask  = dec!(0.59);
        let mut dc = DynamicConfig::default();
        dc.enable_gboost              = true;
        dc.gboost_shadow_mode         = shadow;
        dc.gboost_entry_threshold     = dec!(0.60);
        dc.gboost_min_entry_price     = dec!(0.40);
        dc.gboost_max_yes_entry_price = dec!(0.55);
        dc.gboost_min_edge_from_fair  = dec!(0.04);
        dc.gboost_min_hist_vol        = dec!(0.0001);
        dc.gboost_min_net_profit_usdc = dec!(0);
        ctx.dynamic_config = Arc::new(dc);
        ctx
    }

    fn satisfy_persistence(strategy: &GboostStrategyImpl, ctx: &StrategyContext) {
        let held = std::time::Duration::from_secs(config::GBOOST_ENTRY_PERSISTENCE_SECS + 1);
        *strategy.entry_signal_streak.lock().unwrap() =
            Some((ctx.market.condition_id.clone(), true, Instant::now() - held, Instant::now()));
    }

    /// With shadow mode on, a signal that clears every gate is reported as a
    /// would-be entry and nothing else happens: no Entry signal, no pending entry,
    /// no market hold lock. With it off, the same tick emits the order. This is
    /// the mechanism that keeps the corrected model observe-only until the
    /// operator lifts it.
    #[tokio::test]
    async fn shadow_mode_records_the_would_be_entry_instead_of_placing_it() {
        let strategy = GboostStrategyImpl::new_isolated();
        *strategy.model.lock().unwrap() = Some(model_bullish_on_fixture());
        let last_reason = |asset: &str| crate::helpers::viper_status::snapshot(Some(asset))
            .into_iter()
            .find(|r| r.strategy == "GboostStrategy")
            .and_then(|r| r.last_reason)
            .unwrap_or_default();

        let ctx = entry_ready_ctx("btc-shadow-on", true);
        satisfy_persistence(&strategy, &ctx);
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal), "shadow mode must not emit an order, got {signal:?}");
        let reason = last_reason("btc-shadow-on");
        assert!(
            reason.starts_with("shadow mode: would enter YES @ $0.42"),
            "expected the would-be entry to be reported, got {reason:?}",
        );
        assert!(strategy.pending_entries.lock().unwrap().is_empty(), "shadow mode must not latch a pending entry");
        assert!(strategy.market_hold_locks.lock().unwrap().is_empty(), "shadow mode must not set a hold lock");

        let ctx = entry_ready_ctx("btc-shadow-off", false);
        satisfy_persistence(&strategy, &ctx);
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        match signal {
            StrategySignal::Entry { params, .. } => {
                assert_eq!(params.token_id, ctx.market.yes_token);
                assert_eq!(params.price, dec!(0.42));
            }
            other => panic!("with shadow mode off the same tick must emit the YES entry, got {other:?}"),
        }
        assert_eq!(strategy.pending_entries.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn no_signal_without_model() {
        let strategy = GboostStrategyImpl::new_isolated();
        let signal = strategy.evaluate_entry(&make_ctx()).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn evaluates_with_trained_model() {
        let strategy = GboostStrategyImpl::new_isolated();
        let n = config::GBOOST_MIN_TRAINING_SAMPLES + 10; // No lookahead needed
        let mut samples: Vec<TrainingSample> = Vec::with_capacity(n);
        for i in 0..n {
            let snap = make_snapshot(); // Dummy snapshot
            samples.push(TrainingSample {
                features: extract_features(&snap, None, 0.0, 0.0), // Pass None for prev_s in test
                is_profitable: i % 2 == 0, // Alternate profitable/unprofitable
                entry_timestamp: Utc::now(),
            });
        }
        *strategy.model.lock().unwrap() = Some(train_model(samples, config::GBOOST_BUDGET.to_f32().unwrap_or(0.8), config::GBOOST_ITERATION_LIMIT).unwrap());
        // Must not panic — signal depends on the dummy snapshot's feature values.
        let _ = strategy.evaluate_entry(&make_ctx()).await.unwrap();
        let _ = strategy.evaluate_exit(&make_ctx()).await.unwrap();
    }

    /// The flatness floor used to be `config::GBOOST_MIN_HIST_VOL` read inside the
    /// gate; it is now the `gboost_min_hist_vol` knob. Prove the knob is what the
    /// gate consumes: the same snapshot (hist_vol 0.003) is vetoed as "oracle too
    /// flat" under a 0.5 floor and passes the gate under a 0.0001 floor. Read back
    /// through the viper-status registry, which the veto! macro feeds on every
    /// tick regardless of the model's conviction.
    #[tokio::test]
    async fn min_hist_vol_knob_reaches_the_flatness_gate() {
        let strategy = GboostStrategyImpl::new_isolated();
        let n = config::GBOOST_MIN_TRAINING_SAMPLES + 10;
        let booster = train_model(learnable_samples(n), 1.50, config::GBOOST_ITERATION_LIMIT)
            .expect("fit succeeds");
        assert!(
            booster.trees.len() >= config::GBOOST_STRUCTURAL_MIN_TREES,
            "fixture model has {} trees, below the {} the predictor requires",
            booster.trees.len(), config::GBOOST_STRUCTURAL_MIN_TREES,
        );
        *strategy.model.lock().unwrap() = Some(booster);

        // The registry keeps the LAST reason a viper reported, and a tick whose
        // prediction is below the entry threshold falls through the gate stack
        // without reporting anything. So each floor gets its own asset key: a
        // fresh row whose reason is either the flatness veto or nothing at all.
        let last_reason = |asset: &str| crate::helpers::viper_status::snapshot(Some(asset))
            .into_iter()
            .find(|r| r.strategy == "GboostStrategy")
            .and_then(|r| r.last_reason)
            .unwrap_or_default();

        let strict_asset = "btc-histvol-knob-strict";
        let mut strict = DynamicConfig::default();
        strict.gboost_min_hist_vol = dec!(0.5);
        let mut ctx = make_ctx();
        ctx.crypto_filter = strict_asset.to_string();
        ctx.dynamic_config = Arc::new(strict);
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
        let reason = last_reason(strict_asset);
        assert!(
            reason.starts_with("oracle too flat") && reason.contains("min=0.5000"),
            "a 0.5 floor should veto hist_vol 0.003 as too flat, got {reason:?}",
        );

        let lax_asset = "btc-histvol-knob-lax";
        let mut lax = DynamicConfig::default();
        lax.gboost_min_hist_vol = dec!(0.0001);
        ctx.crypto_filter = lax_asset.to_string();
        ctx.dynamic_config = Arc::new(lax);
        let _ = strategy.evaluate_entry(&ctx).await.unwrap();
        let reason = last_reason(lax_asset);
        assert!(
            !reason.starts_with("oracle too flat"),
            "a 0.0001 floor must not veto hist_vol 0.003, got {reason:?}",
        );
    }

    #[tokio::test]
    async fn pending_entry_is_kept_when_no_exit_signal() {
        let strategy = GboostStrategyImpl::new_isolated();
        let ctx = make_ctx();

        strategy.pending_entries.lock().unwrap().insert(
            ctx.market.yes_token.clone(),
            (ctx.snapshot.clone(), None, dec!(0.50), 0.0, 0.0),
        );

        {
            let mut map = ctx.positions.lock().await;
            map.insert(
                PositionKey::new(&ctx.squadron_id, "GboostStrategy", ctx.market.yes_token.clone()),
                Position {
                    shares: dec!(10),
                    avg_entry: dec!(0.52),
                    opened_at: Utc::now(),
                    close_time: ctx.market.market_close_time,
                    market_name: ctx.market.market_name.clone(),
                    pair_token_id: ctx.market.yes_token.clone(),
                    fill_confirmed_at: Some(Utc::now()),
                    paired_leg_token_id: None, entry_fee: Decimal::ZERO,
                },
            );
        }

        let signal = strategy.evaluate_exit(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
        assert_eq!(strategy.pending_entries.lock().unwrap().len(), 1);
        assert_eq!(strategy.training_data.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn pending_entry_is_consumed_when_exit_signal_emitted() {
        let strategy = GboostStrategyImpl::new_isolated();
        let ctx = make_ctx();

        strategy.pending_entries.lock().unwrap().insert(
            ctx.market.yes_token.clone(),
            (ctx.snapshot.clone(), None, dec!(0.40), 0.0, 0.0),
        );

        {
            let mut map = ctx.positions.lock().await;
            map.insert(
                PositionKey::new(&ctx.squadron_id, "GboostStrategy", ctx.market.yes_token.clone()),
                Position {
                    shares: dec!(10),
                    avg_entry: dec!(0.40),
                    opened_at: Utc::now(),
                    close_time: ctx.market.market_close_time,
                    market_name: ctx.market.market_name.clone(),
                    pair_token_id: ctx.market.yes_token.clone(),
                    fill_confirmed_at: Some(Utc::now()),
                    paired_leg_token_id: None, entry_fee: Decimal::ZERO,
                },
            );
        }

        let signal = strategy.evaluate_exit(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::Exit { reason, .. } if reason.contains("GBoost TP YES")));
        assert_eq!(strategy.pending_entries.lock().unwrap().len(), 0);
        assert_eq!(strategy.training_data.lock().unwrap().len(), 1);
    }

    // ── Label pool persistence (B33) ─────────────────────────────────────────

    fn sample_at(ts: chrono::DateTime<Utc>, tag: f64) -> TrainingSample {
        let mut features = [0.0f64; NUM_FEATURES];
        features[0] = tag;
        features[NUM_FEATURES - 1] = -tag;
        TrainingSample { features, is_profitable: tag > 0.5, entry_timestamp: ts }
    }

    fn pool_file(samples: Vec<TrainingSample>) -> LabelPoolFile {
        LabelPoolFile {
            format_version: LABEL_POOL_FORMAT_VERSION,
            num_features: NUM_FEATURES,
            label_horizon_secs: config::GBOOST_LABEL_HORIZON_SECS,
            saved_at: Utc::now(),
            last_harvest_ts: samples.last().map(|s| s.entry_timestamp),
            samples,
        }
    }

    /// A scratch path under the OS temp dir, unique per test and process so
    /// parallel test threads never share a file.
    fn scratch_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("dradis-gboost-pool-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("logs").join("btc-gboost_label_pool.json")
    }

    /// The pool file follows the SQLite shard convention exactly, so the venue
    /// that owns `logs/kalshi-dradis.db` owns `logs/kalshi-gboost_label_pool.json`
    /// and never reads an intl instance's labels off a shared `logs/` mount.
    #[test]
    fn label_pool_path_follows_the_db_shard_name() {
        let p = |s: &str| label_pool_path_for_shard_file(std::path::Path::new(s))
            .map(|p| p.to_string_lossy().into_owned());
        assert_eq!(p("logs/btc-dradis.db").as_deref(),    Some("logs/btc-gboost_label_pool.json"));
        assert_eq!(p("logs/kalshi-dradis.db").as_deref(), Some("logs/kalshi-gboost_label_pool.json"));
        assert_eq!(p("logs/us-dradis.db").as_deref(),     Some("logs/us-gboost_label_pool.json"));
        assert_eq!(p("/app/logs/eth-dradis.db").as_deref(), Some("/app/logs/eth-gboost_label_pool.json"));
        // A shard file without the suffix still keys on its stem.
        assert_eq!(p("logs/custom.db").as_deref(), Some("logs/custom-gboost_label_pool.json"));
        assert!(p("").is_none());
    }

    #[test]
    fn label_pool_round_trips_through_an_atomic_write() {
        let path = scratch_path("roundtrip");
        let now = Utc::now();
        let samples: Vec<_> = (0..5).map(|i| sample_at(now - chrono::Duration::minutes(i), i as f64 / 10.0)).collect();
        let file = pool_file(samples.clone());

        write_label_pool_file(&path, &file).expect("write into a directory that did not exist yet");
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        assert!(!std::path::Path::new(&tmp).exists(), "temp file must be renamed away, not left beside the pool");

        let back = read_label_pool_file(&path).expect("what we wrote parses");
        assert_eq!(back.samples.len(), samples.len());
        assert_eq!(back.last_harvest_ts, file.last_harvest_ts);
        for (a, b) in back.samples.iter().zip(&samples) {
            assert_eq!(a.features, b.features);
            assert_eq!(a.is_profitable, b.is_profitable);
            assert_eq!(a.entry_timestamp, b.entry_timestamp);
        }

        // A second save replaces the first in place.
        write_label_pool_file(&path, &pool_file(samples[..2].to_vec())).unwrap();
        assert_eq!(read_label_pool_file(&path).unwrap().samples.len(), 2);
        let _ = std::fs::remove_dir_all(path.parent().unwrap().parent().unwrap());
    }

    /// A corrupt, truncated, missing or foreign pool file is a logged reason
    /// and an empty pool — never a panic on the startup path.
    #[test]
    fn corrupt_or_foreign_label_pool_files_are_rejected_without_panicking() {
        let path = scratch_path("reject");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        assert!(read_label_pool_file(&path).unwrap_err().starts_with("read failed"), "missing file");

        std::fs::write(&path, b"{\"format_version\":1,\"num_features\":30,\"sam").unwrap();
        assert!(read_label_pool_file(&path).unwrap_err().starts_with("parse failed"), "truncated file");

        std::fs::write(&path, b"\x00\xff not json at all").unwrap();
        assert!(read_label_pool_file(&path).unwrap_err().starts_with("parse failed"), "garbage");

        let good = pool_file(vec![sample_at(Utc::now(), 0.7)]);
        let mut wrong_horizon = pool_file(vec![sample_at(Utc::now(), 0.7)]);
        wrong_horizon.label_horizon_secs = config::GBOOST_LABEL_HORIZON_SECS + 60;
        write_label_pool_file(&path, &wrong_horizon).unwrap();
        assert!(read_label_pool_file(&path).unwrap_err().contains("horizon"), "different label target");

        let mut wrong_width = pool_file(vec![sample_at(Utc::now(), 0.7)]);
        wrong_width.num_features = NUM_FEATURES - 1;
        write_label_pool_file(&path, &wrong_width).unwrap();
        assert!(read_label_pool_file(&path).unwrap_err().contains("features"), "different feature set");

        let mut wrong_version = pool_file(vec![sample_at(Utc::now(), 0.7)]);
        wrong_version.format_version = LABEL_POOL_FORMAT_VERSION + 1;
        write_label_pool_file(&path, &wrong_version).unwrap();
        assert!(read_label_pool_file(&path).unwrap_err().contains("format version"));

        write_label_pool_file(&path, &good).unwrap();
        assert!(read_label_pool_file(&path).is_ok(), "the matching file still loads");
        let _ = std::fs::remove_dir_all(path.parent().unwrap().parent().unwrap());
    }

    #[test]
    fn stale_samples_are_pruned_by_wall_clock() {
        let now = Utc::now();
        let mut pool = LabelPool::default();
        pool.samples.push_back(sample_at(now - chrono::Duration::hours(49), 0.1));
        pool.samples.push_back(sample_at(now - chrono::Duration::hours(47), 0.2));
        pool.samples.push_back(sample_at(now - chrono::Duration::minutes(5), 0.3));

        let dropped = prune_stale_samples(&mut pool, now, chrono::Duration::hours(48));
        assert_eq!(dropped, 1);
        assert_eq!(pool.pruned_total, 1);
        let tags: Vec<f64> = pool.samples.iter().map(|s| s.features[0]).collect();
        assert_eq!(tags, vec![0.2, 0.3], "only the 49h-old sample goes; the 47h one is inside the window");

        assert_eq!(prune_stale_samples(&mut pool, now, chrono::Duration::hours(48)), 0, "idempotent");
    }

    /// Restored samples are older than anything harvested since boot, so they
    /// sit in front of the live ones; the FIFO cap evicts from that end first,
    /// and the watermark keeps the newer of the two.
    #[test]
    fn restored_samples_merge_ahead_of_live_ones_and_respect_the_cap() {
        let now = Utc::now();
        let cap = config::GBOOST_LABEL_POOL_CAP;

        let mut pool = LabelPool::default();
        pool.samples.push_back(sample_at(now - chrono::Duration::minutes(2), 0.91));
        pool.samples.push_back(sample_at(now - chrono::Duration::minutes(1), 0.92));
        pool.last_harvest_ts = Some(now - chrono::Duration::minutes(1));

        let restored: Vec<_> = (0..cap)
            .map(|i| sample_at(now - chrono::Duration::hours(3) + chrono::Duration::seconds(i as i64), 0.1))
            .collect();
        let mut file = pool_file(restored);
        file.last_harvest_ts = Some(now - chrono::Duration::hours(2));

        let n = merge_restored_into_pool(&mut pool, file);
        assert_eq!(pool.samples.len(), cap, "never exceeds the cap");
        assert_eq!(n, cap - 2, "two restored samples were evicted to make room for the live ones");
        assert_eq!(pool.restored, cap - 2);
        let tail: Vec<f64> = pool.samples.iter().rev().take(2).map(|s| s.features[0]).collect();
        assert_eq!(tail, vec![0.92, 0.91], "live samples stay at the back, in order");
        assert_eq!(pool.last_harvest_ts, Some(now - chrono::Duration::minutes(1)), "newer watermark wins");

        // An empty live pool takes the file's watermark.
        let mut fresh = LabelPool::default();
        let mut file = pool_file(vec![sample_at(now, 0.5)]);
        file.last_harvest_ts = Some(now);
        assert_eq!(merge_restored_into_pool(&mut fresh, file), 1);
        assert_eq!(fresh.last_harvest_ts, Some(now));
    }

    /// The whole B33 path, on the real code rather than its pieces: a history
    /// spanning the label horizon is harvested by `maybe_retrain`, the pool is
    /// handed to the atomic writer, the process-global pool is then wiped to
    /// simulate a restart, and the startup loader brings every sample back.
    ///
    /// Drives the process-global pool and the `GBOOST_LABEL_POOL_PATH` override,
    /// so it is the only test that may harvest; the other GBoost tests feed a
    /// single snapshot each and never reach the horizon.
    #[tokio::test]
    async fn harvest_saves_the_pool_and_a_restart_reloads_it() {
        let path = scratch_path("e2e");
        std::env::set_var("GBOOST_LABEL_POOL_PATH", &path);
        {
            let mut p = gboost_label_pool().lock().unwrap();
            *p = LabelPool::default();
        }

        let strategy = GboostStrategyImpl::new_isolated();
        // Well under GBOOST_MIN_TRAINING_SAMPLES on every profile, so the harvest
        // takes the "pool filling" branch and never spawns a training job.
        let candidates = 100i64;
        let span = config::GBOOST_LABEL_HORIZON_SECS + candidates;
        let start = Utc::now() - chrono::Duration::seconds(span + 5);
        for i in 0..=span {
            let mut snap = make_snapshot();
            snap.timestamp = start + chrono::Duration::seconds(i);
            // Monotone climb, far outside the deadband over any horizon.
            snap.oracle_price = dec!(95000) + Decimal::from(i * 10);
            strategy.push_snapshot(snap);
        }
        assert_eq!(strategy.history.lock().unwrap().len(), span as usize + 1);

        // Arm the tick trigger so this call harvests.
        *strategy.ticks_since_retrain.lock().unwrap() = config::GBOOST_RETRAIN_EVERY_N - 1;
        strategy.maybe_retrain(&DynamicConfig::default());

        let kept = {
            let p = gboost_label_pool().lock().unwrap();
            assert!(p.candidates_total >= candidates as u64 - 1, "scanned {} candidates", p.candidates_total);
            assert_eq!(p.kept_total as usize, p.samples.len(), "every survivor lands in the pool");
            assert!(!p.dirty, "the harvest handed its samples to the writer");
            assert!(p.last_save_at.is_some());
            p.samples.len()
        };
        assert!(kept > 0, "a climbing oracle must yield labels");

        // The save is a spawned task; let it run.
        let mut waited = 0;
        while !path.exists() && waited < 100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            waited += 1;
        }
        assert!(path.exists(), "pool file was written by the harvest");
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        assert!(!std::path::Path::new(&tmp).exists());
        let on_disk = read_label_pool_file(&path).expect("harvest output parses");
        assert_eq!(on_disk.samples.len(), kept);
        assert_eq!(on_disk.label_horizon_secs, config::GBOOST_LABEL_HORIZON_SECS);

        // "Restart": the process-global pool is gone; the loader brings it back.
        {
            let mut p = gboost_label_pool().lock().unwrap();
            *p = LabelPool::default();
        }
        let restored = load_label_pool_from(path.clone()).await;
        assert_eq!(restored, kept);
        {
            let p = gboost_label_pool().lock().unwrap();
            assert_eq!(p.samples.len(), kept);
            assert_eq!(p.restored, kept);
            assert_eq!(p.last_harvest_ts, on_disk.last_harvest_ts, "harvest watermark survives the restart");
        }

        std::env::remove_var("GBOOST_LABEL_POOL_PATH");
        let _ = std::fs::remove_dir_all(path.parent().unwrap().parent().unwrap());
    }

    // ── Retrain acceptance evidence harness (B37) ────────────────────────────

    /// Walk-forward evaluation of the acceptance test over a real label pool
    /// file. Ignored by default: it needs a pool file and takes a minute in
    /// release. Run with
    ///
    ///   GBOOST_POOL_EVAL_PATH=logs/btc-gboost_label_pool.json \
    ///   cargo test --release eval_pool_walk_forward -- --ignored --nocapture
    ///
    /// Prints, for a series of interior holdout windows, what the validation
    /// fit scores, what a label-shuffled null scores on the same window, the
    /// reverse (oldest-window) split, and the verdict the production path
    /// would return on the whole pool. This is how the skill floor was chosen
    /// and how "would the corrected model be accepted" is shown rather than
    /// asserted.
    #[test]
    #[ignore]
    fn eval_pool_walk_forward() {
        let Ok(path) = std::env::var("GBOOST_POOL_EVAL_PATH") else {
            eprintln!("GBOOST_POOL_EVAL_PATH not set; nothing to evaluate");
            return;
        };
        let bytes = std::fs::read(&path).expect("pool file readable");
        let file: LabelPoolFile = serde_json::from_slice(&bytes).expect("pool file parses");
        let mut samples = file.samples;
        samples.sort_by_key(|s| s.entry_timestamp);
        let n = samples.len();
        let gap = chrono::Duration::seconds(2 * file.label_horizon_secs);
        let budget = config::GBOOST_BUDGET.to_f32().unwrap_or(0.8);
        let iters = config::GBOOST_ITERATION_LIMIT;
        let threshold = config::GBOOST_ENTRY_THRESHOLD.to_f64().unwrap_or(0.72);
        let holdout_len = ((n as f64 * HOLDOUT_FRACTION).round() as usize).max(HOLDOUT_MIN_SAMPLES);
        eprintln!(
            "pool {}: {} samples, horizon {} s, gap {} s, holdout {} samples, budget {}, {} pos",
            path, n, file.label_horizon_secs, gap.num_seconds(), holdout_len, budget,
            samples.iter().filter(|s| s.is_profitable).count(),
        );

        // Null model: the same features with labels rotated by an offset far
        // larger than the gap, so autocorrelation survives but the feature to
        // label relation is destroyed. Its skill on the true holdout is what a
        // lucky model looks like.
        let shifted = |train: &[TrainingSample], by: usize| -> Vec<TrainingSample> {
            let m = train.len();
            (0..m).map(|i| TrainingSample {
                features: train[i].features,
                is_profitable: train[(i + by) % m].is_profitable,
                entry_timestamp: train[i].entry_timestamp,
            }).collect()
        };

        let mut real_skills = Vec::new();
        let mut null_skills = Vec::new();
        for frac in [0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90] {
            let start = ((n as f64) * frac) as usize;
            let split = split_for_holdout(&samples, start, holdout_len, gap);
            if split.train.len() < config::GBOOST_MIN_TRAINING_SAMPLES { continue; }
            let model = train_model(split.train.clone(), budget, iters).expect("fit");
            let r = evaluate_holdout(&model, &split, gap, threshold);
            eprintln!("split @{:.0}%  REAL  {}  train_pos={:.2} holdout_pos={:.2}",
                frac * 100.0, r.summary(), r.train_pos_rate, r.holdout_pos_rate);
            real_skills.push(r.skill);
            for by in [1500usize, 3000, 4500] {
                let by = by % split.train.len().max(1);
                let null_split = HoldoutSplit {
                    train: shifted(&split.train, by),
                    holdout: split.holdout.clone(),
                    gap_dropped: split.gap_dropped,
                };
                let null_model = train_model(null_split.train.clone(), budget, iters).expect("fit");
                let nr = evaluate_holdout(&null_model, &null_split, gap, threshold);
                eprintln!("split @{:.0}%  NULL+{:<5} skill {:+.1}% auc {:.2} conf {}/{} trees {}",
                    frac * 100.0, by, nr.skill * 100.0, nr.auc, nr.confident_hits, nr.confident_n, nr.trees);
                null_skills.push(nr.skill);
            }
        }

        // Reverse split: mirror time so the OLDEST window becomes the holdout.
        {
            let t0 = samples.first().unwrap().entry_timestamp;
            let t1 = samples.last().unwrap().entry_timestamp;
            let mut mirrored: Vec<TrainingSample> = samples.iter().map(|s| TrainingSample {
                features: s.features,
                is_profitable: s.is_profitable,
                entry_timestamp: t1 - (s.entry_timestamp - t0),
            }).collect();
            mirrored.sort_by_key(|s| s.entry_timestamp);
            let split = split_for_holdout(&mirrored, n - holdout_len, holdout_len, gap);
            let model = train_model(split.train.clone(), budget, iters).expect("fit");
            let r = evaluate_holdout(&model, &split, gap, threshold);
            eprintln!("REVERSE (oldest window held out)  {}", r.summary());
        }

        let mean = |v: &[f64]| v.iter().sum::<f64>() / v.len().max(1) as f64;
        let mut sorted_null = null_skills.clone();
        sorted_null.sort_by(|a, b| a.partial_cmp(b).unwrap());
        eprintln!(
            "REAL skills: min {:+.1}% mean {:+.1}% max {:+.1}% | NULL skills ({}): min {:+.1}% mean {:+.1}% max {:+.1}%",
            real_skills.iter().cloned().fold(f64::INFINITY, f64::min) * 100.0, mean(&real_skills) * 100.0,
            real_skills.iter().cloned().fold(f64::NEG_INFINITY, f64::max) * 100.0,
            null_skills.len(),
            sorted_null.first().copied().unwrap_or(0.0) * 100.0, mean(&null_skills) * 100.0,
            sorted_null.last().copied().unwrap_or(0.0) * 100.0,
        );

        // In-sample check on the production split: if the validation fit orders
        // and scores its own training data well but not the holdout, the failure
        // is generalization, not the evaluation plumbing.
        {
            let split = split_for_holdout(&samples, n - holdout_len, holdout_len, gap);
            let model = train_model(split.train.clone(), budget, iters).expect("fit");
            let in_sample = HoldoutSplit { train: split.train.clone(), holdout: split.train.clone(), gap_dropped: 0 };
            let r = evaluate_holdout(&model, &in_sample, gap, threshold);
            eprintln!("IN-SAMPLE (validation fit scored on its own training split): skill {:+.1}% logloss {:.3} auc {:.3} conf {}/{}",
                r.skill * 100.0, r.model_logloss, r.auc, r.confident_hits, r.confident_n);
            let probs = {
                let rows: Vec<[f64; NUM_FEATURES]> = split.holdout.iter().map(|s| s.features).collect();
                let data = column_major_matrix_data(&rows);
                model.predict_proba(&Matrix::new(&data, rows.len(), NUM_FEATURES), false, false)
            };
            let saturated = probs.iter().filter(|p| **p < 0.01 || **p > 0.99).count();
            let sat_wrong = probs.iter().zip(&split.holdout)
                .filter(|(p, s)| (**p < 0.01 || **p > 0.99) && ((**p >= 0.5) != s.is_profitable)).count();
            eprintln!("PRODUCTION SPLIT holdout: {} of {} predictions saturated beyond 0.01/0.99, {} of those wrong",
                saturated, probs.len(), sat_wrong);
        }

        // Variant matrix: is the failure the learning rate, the time-proxy
        // features (oracle_price [11], secs_to_expiry [12]), or the data?
        eprintln!("VARIANTS (split%, budget, masked features) -> skill / logloss / auc / confident / trees");
        for frac in [0.50, 0.70, 0.90] {
            let start = ((n as f64) * frac) as usize;
            for b in [0.3f32, 0.5, 0.8] {
                for mask in [&[][..], &[11usize, 12][..], &[2usize, 3, 11, 12][..]] {
                    let masked: Vec<TrainingSample> = samples.iter().map(|s| {
                        let mut f = s.features;
                        for &k in mask { f[k] = 0.0; }
                        TrainingSample { features: f, is_profitable: s.is_profitable, entry_timestamp: s.entry_timestamp }
                    }).collect();
                    let split = split_for_holdout(&masked, start, holdout_len, gap);
                    let model = train_model(split.train.clone(), b, iters).expect("fit");
                    let r = evaluate_holdout(&model, &split, gap, threshold);
                    eprintln!("  @{:.0}% budget {:.1} mask {:?}: skill {:+.1}% logloss {:.3} auc {:.2} conf {}/{} trees {}",
                        frac * 100.0, b, mask, r.skill * 100.0, r.model_logloss, r.auc,
                        r.confident_hits, r.confident_n, r.trees);
                }
            }
        }

        // Decimation: the pool holds ~300 near-duplicate rows per 300 s outcome
        // (median spacing under 1 s). Keep one sample per `every` seconds and
        // ask whether the features carry any signal once the duplicates are gone.
        eprintln!("DECIMATED (one sample per N s; holdout = newest 10% by count, gap unchanged)");
        for every in [30i64, 60, 120] {
            let mut thinned: Vec<TrainingSample> = Vec::new();
            for s in &samples {
                if thinned.last().map_or(true, |l: &TrainingSample| (s.entry_timestamp - l.entry_timestamp).num_seconds() >= every) {
                    thinned.push(s.clone());
                }
            }
            let m = thinned.len();
            let hl = ((m as f64) * HOLDOUT_FRACTION).round() as usize;
            for frac in [0.50, 0.70, 0.90] {
                let start = ((m as f64) * frac) as usize;
                for b in [0.3f32, 0.5, 0.8] {
                    let split = split_for_holdout(&thinned, start, hl, gap);
                    if split.train.len() < 50 { continue; }
                    let model = train_model_unchecked(split.train.clone(), b, iters).expect("fit");
                    let r = evaluate_holdout(&model, &split, gap, threshold);
                    eprintln!("  every {}s ({} samples) @{:.0}% budget {:.1}: skill {:+.1}% logloss {:.3} auc {:.2} conf {}/{} trees {} train {}",
                        every, m, frac * 100.0, b, r.skill * 100.0, r.model_logloss, r.auc,
                        r.confident_hits, r.confident_n, r.trees, r.train_n);
                }
            }
        }

        // The production path on the whole pool, with the shipped thresholds.
        let verdict = train_and_validate(
            samples.clone(), budget, iters,
            config::GBOOST_STRUCTURAL_MIN_TREES,
            config::GBOOST_HOLDOUT_MIN_SKILL.to_f64().unwrap_or(0.0),
            threshold, gap,
        ).expect("train_and_validate");
        match verdict {
            RetrainVerdict::Accepted { model, report } => eprintln!(
                "PRODUCTION PATH: ACCEPTED — {} | refit on all {} samples -> {} trees",
                report.summary(), n, model.trees.len()),
            RetrainVerdict::Rejected(why) => eprintln!("PRODUCTION PATH: REJECTED — {}", why.describe()),
        }
    }

    // ── Retrain acceptance (B37) ─────────────────────────────────────────────

    /// Give `samples` distinct timestamps one second apart from `start`, the
    /// spacing the live pool has (median 0.94 s on production).
    fn timed(mut samples: Vec<TrainingSample>, start: chrono::DateTime<Utc>) -> Vec<TrainingSample> {
        for (i, s) in samples.iter_mut().enumerate() {
            s.entry_timestamp = start + chrono::Duration::seconds(i as i64);
        }
        samples
    }

    /// Enough one-second samples that, after the purge gap and a 10% holdout,
    /// the training split still clears GBOOST_MIN_TRAINING_SAMPLES on every profile.
    fn acceptance_pool_len() -> usize {
        config::GBOOST_MIN_TRAINING_SAMPLES + holdout_gap().num_seconds() as usize + HOLDOUT_MIN_SAMPLES + 400
    }

    /// The production pathology: a clock feature and labels that come in runs,
    /// so a fit can memorize when each run happened but learns nothing that
    /// carries past the last training timestamp.
    fn clock_memorizable_samples(n: usize, start: chrono::DateTime<Utc>) -> Vec<TrainingSample> {
        timed(structured_samples(n, 5), start).into_iter().enumerate().map(|(i, mut s)| {
            s.features[11] = i as f64 / n as f64;
            s.is_profitable = (i / 150) % 2 == 0;
            s
        }).collect()
    }

    #[test]
    fn holdout_split_purges_a_gap_wider_than_the_label_horizon() {
        let n = 2000;
        let samples = timed(structured_samples(n, 7), Utc::now() - chrono::Duration::seconds(n as i64));
        let gap = holdout_gap();
        let horizon = chrono::Duration::seconds(config::GBOOST_LABEL_HORIZON_SECS);
        assert!(gap >= horizon * 2, "gap {} s must be at least twice the {} s label horizon", gap.num_seconds(), horizon.num_seconds());

        let split = split_for_holdout(&samples, n - 200, 200, gap);
        assert_eq!(split.holdout.len(), 200);
        let first_holdout = split.holdout[0].entry_timestamp;
        let last_train = split.train.last().unwrap().entry_timestamp;
        assert!(first_holdout - last_train >= gap, "training ends {} s before the holdout, gap is {} s",
            (first_holdout - last_train).num_seconds(), gap.num_seconds());
        assert_eq!(split.train.len() + split.gap_dropped + split.holdout.len(), n, "every sample is accounted for");
        assert!(split.gap_dropped as i64 >= gap.num_seconds() - 1 && split.gap_dropped as i64 <= gap.num_seconds());
        // The property the gap exists for: no training label is settled inside the holdout.
        assert!(split.train.iter().all(|s| s.entry_timestamp + horizon <= first_holdout));
        // Sorted input is preserved in both halves.
        assert!(split.train.windows(2).all(|w| w[0].entry_timestamp <= w[1].entry_timestamp));
        assert!(split.holdout.windows(2).all(|w| w[0].entry_timestamp <= w[1].entry_timestamp));
    }

    #[test]
    fn logloss_and_skill_score_what_they_claim() {
        let labels = [true, false, true, false];
        // A constant at the base rate scores exactly the base-rate entropy.
        let base = logloss(&[0.5; 4], &labels);
        assert!((base - std::f64::consts::LN_2).abs() < 1e-12);
        // A perfect forecaster scores ~0; a perfectly wrong one is capped by the clamp, not infinite.
        assert!(logloss(&[1.0, 0.0, 1.0, 0.0], &labels) < 1e-5);
        let wrong = logloss(&[0.0, 1.0, 0.0, 1.0], &labels);
        assert!(wrong.is_finite() && wrong > 13.0);
        assert!((rank_auc(&[0.9, 0.1, 0.8, 0.2], &labels) - 1.0).abs() < 1e-12);
        assert!((rank_auc(&[0.1, 0.9, 0.2, 0.8], &labels) - 0.0).abs() < 1e-12);
        assert_eq!(rank_auc(&[0.3, 0.4], &[true, true]), 0.5, "one class only is chance, not NaN");
    }

    /// A fit whose labels follow the features is adopted with skill well above
    /// the floor, and carries its acceptance record; the same machinery rejects
    /// a fit that only memorized a clock, naming the measured skill.
    #[test]
    fn acceptance_adopts_a_generalizing_fit_and_rejects_a_clock_memorizing_one() {
        let n = acceptance_pool_len();
        let start = Utc::now() - chrono::Duration::seconds(n as i64);
        let learnable = timed(structured_samples(n, 0xB37), start);
        match train_and_validate(learnable, 0.8, config::GBOOST_ITERATION_LIMIT, 3, 0.05, 0.72, holdout_gap()).unwrap() {
            RetrainVerdict::Accepted { model, report } => {
                assert!(report.skill > 0.05, "expected clear skill on learnable labels, got {}", report.summary());
                assert!(report.holdout_n >= HOLDOUT_MIN_SAMPLES);
                assert!(report.gap_secs >= 2 * config::GBOOST_LABEL_HORIZON_SECS);
                assert!(model.trees.len() >= 3);
                assert!(model_has_current_layout(&model), "the adopted refit carries the B38 layout tag");
                assert_eq!(
                    model.get_metadata(&MODEL_META_HOLDOUT_SKILL_KEY.to_string()).as_deref(),
                    Some(format!("{:.4}", report.skill).as_str()),
                );
                assert!(model.get_metadata(&MODEL_META_ACCEPTED_AT_KEY.to_string()).is_some());
                assert!(!model_accepted_at_et(&model).contains("before"), "accepted_at renders as a time");
            }
            RetrainVerdict::Rejected(why) => panic!("learnable labels were rejected: {}", why.describe()),
        }

        match train_and_validate(clock_memorizable_samples(n, start), 0.8, config::GBOOST_ITERATION_LIMIT, 3, 0.05, 0.72, holdout_gap()).unwrap() {
            RetrainVerdict::Rejected(RetrainRejection::NoSkill { report, min_skill }) => {
                assert!(report.skill < min_skill, "{}", report.summary());
                let msg = RetrainRejection::NoSkill { report, min_skill }.describe();
                assert!(msg.contains("holdout skill") && msg.contains("does not generalize"), "{msg}");
            }
            RetrainVerdict::Rejected(other) => panic!("expected a skill rejection, got: {}", other.describe()),
            RetrainVerdict::Accepted { report, .. } => panic!("a clock-memorizing fit must not be adopted: {}", report.summary()),
        }
    }

    /// The knob is what decides: the same clock-memorizing fit is adopted when
    /// the operator sets the floor below its measured skill (the documented
    /// "collect shadow data anyway" setting).
    #[test]
    fn skill_floor_knob_decides_acceptance() {
        let n = acceptance_pool_len();
        let start = Utc::now() - chrono::Duration::seconds(n as i64);
        let samples = clock_memorizable_samples(n, start);
        let measured = match train_and_validate(samples.clone(), 0.8, config::GBOOST_ITERATION_LIMIT, 3, 0.05, 0.72, holdout_gap()).unwrap() {
            RetrainVerdict::Rejected(RetrainRejection::NoSkill { report, .. }) => report.skill,
            other => panic!("expected a skill rejection first, got {}", match other {
                RetrainVerdict::Rejected(w) => w.describe(), RetrainVerdict::Accepted { report, .. } => report.summary() }),
        };
        match train_and_validate(samples, 0.8, config::GBOOST_ITERATION_LIMIT, 3, measured - 1.0, 0.72, holdout_gap()).unwrap() {
            RetrainVerdict::Accepted { .. } => {}
            RetrainVerdict::Rejected(why) => panic!("a floor below the measured skill must accept: {}", why.describe()),
        }
    }

    #[test]
    fn acceptance_defers_when_the_pool_is_too_short_to_validate() {
        // Enough to fit outright, not enough to leave a training split behind
        // the gap and a holdout in front of it.
        let n = config::GBOOST_MIN_TRAINING_SAMPLES + 50;
        let start = Utc::now() - chrono::Duration::seconds(n as i64);
        match train_and_validate(timed(structured_samples(n, 3), start), 0.8, config::GBOOST_ITERATION_LIMIT, 3, 0.05, 0.72, holdout_gap()).unwrap() {
            RetrainVerdict::Rejected(why @ RetrainRejection::PoolTooShort { .. }) => {
                let msg = why.describe();
                assert!(msg.contains("pool too short") && msg.contains("labeling continues"), "{msg}");
            }
            RetrainVerdict::Rejected(other) => panic!("expected deferral, got: {}", other.describe()),
            RetrainVerdict::Accepted { report, .. } => panic!("nothing should be fit on a pool this short: {}", report.summary()),
        }
    }

    /// Evidence probe for the structural floor: how many trees does perpetual
    /// stop at when the features carry nothing (identical rows, alternating
    /// labels), across pool sizes and budgets. Run with
    ///
    ///   cargo test --release probe_stump_tree_counts -- --ignored --nocapture
    #[test]
    #[ignore]
    fn probe_stump_tree_counts() {
        let snap = make_snapshot();
        for n in [320usize, 600, 679, 955, 1000, 2000, 2001, 4000, 8000] {
            for b in [0.5f32, 0.8, 1.0, 1.5] {
                let flat: Vec<TrainingSample> = (0..n).map(|i| TrainingSample {
                    features: extract_features(&snap, None, 0.0, 0.0),
                    is_profitable: i % 2 == 0,
                    entry_timestamp: Utc::now(),
                }).collect();
                let m = train_model_unchecked(flat, b, config::GBOOST_ITERATION_LIMIT).unwrap();
                eprintln!("flat n={n} budget={b}: {} trees", m.trees.len());
            }
        }
        // Homogeneous labels (all up) with real-looking features: the other
        // way a window gives the booster nothing.
        for n in [600usize, 2000] {
            let mut s = structured_samples(n, 11);
            for x in s.iter_mut() { x.is_profitable = true; }
            let m = train_model_unchecked(s, 0.8, config::GBOOST_ITERATION_LIMIT).unwrap();
            eprintln!("all-up n={n} budget=0.8: {} trees", m.trees.len());
        }
    }

    /// The case the original tree floor was written for: identical features
    /// with alternating labels give the booster nothing to split on, so it
    /// stops at a stump. The structural floor catches it before any holdout
    /// arithmetic, with its own reason.
    #[test]
    fn structural_floor_catches_a_stump() {
        let n = acceptance_pool_len();
        let start = Utc::now() - chrono::Duration::seconds(n as i64);
        let snap = make_snapshot();
        let flat: Vec<TrainingSample> = (0..n).map(|i| TrainingSample {
            features: extract_features(&snap, None, 0.0, 0.0),
            is_profitable: i % 2 == 0,
            entry_timestamp: start + chrono::Duration::seconds(i as i64),
        }).collect();
        let stump = train_model(flat.clone(), 0.8, config::GBOOST_ITERATION_LIMIT).unwrap();
        assert!(stump.trees.len() < config::GBOOST_STRUCTURAL_MIN_TREES,
            "a featureless fit produced {} trees; the structural floor of {} would not catch it",
            stump.trees.len(), config::GBOOST_STRUCTURAL_MIN_TREES);
        match train_and_validate(flat, 0.8, config::GBOOST_ITERATION_LIMIT, config::GBOOST_STRUCTURAL_MIN_TREES, 0.05, 0.72, holdout_gap()).unwrap() {
            RetrainVerdict::Rejected(why @ RetrainRejection::Structural { .. }) => {
                let msg = why.describe();
                assert!(msg.contains("structural floor") && msg.contains("no structure"), "{msg}");
            }
            RetrainVerdict::Rejected(other) => panic!("expected a structural rejection, got: {}", other.describe()),
            RetrainVerdict::Accepted { report, .. } => panic!("a stump must not be adopted: {}", report.summary()),
        }
    }

    /// The retrain path on the real code, not its pieces: `maybe_retrain` with
    /// enough real outcomes to train goes through spawn_blocking and
    /// `train_and_validate`; an accepted verdict installs the refit model, a
    /// rejected one leaves the installed model exactly as it was.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn maybe_retrain_installs_an_accepted_model_and_keeps_it_on_rejection() {
        let model_path = scratch_path("b37-model").with_file_name("btc-gboost_model_test.json");
        // The model writer assumes logs/ exists, as it does in every deployment.
        std::fs::create_dir_all(model_path.parent().unwrap()).unwrap();
        std::env::set_var("GBOOST_MODEL_PATH", &model_path);

        let strategy = GboostStrategyImpl::new_isolated();
        let n = acceptance_pool_len();
        let start = Utc::now() - chrono::Duration::seconds(n as i64);
        let training_flag = Arc::clone(&strategy.is_training);
        let wait_for_verdict = move || {
            let flag = Arc::clone(&training_flag);
            async move {
                let mut waited = 0;
                while flag.load(Ordering::Relaxed) && waited < 1200 {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    waited += 1;
                }
                assert!(!flag.load(Ordering::Relaxed), "retrain did not finish");
            }
        };
        let arm = |strategy: &GboostStrategyImpl, samples: Vec<TrainingSample>| {
            let mut td = strategy.training_data.lock().unwrap();
            td.clear();
            td.extend(samples);
            // Real outcomes at or above GBOOST_MIN_TRAINING_SAMPLES take the
            // real-samples branch, so the process-global label pool is untouched.
            assert!(td.len() >= config::GBOOST_MIN_TRAINING_SAMPLES);
            *strategy.ticks_since_retrain.lock().unwrap() = config::GBOOST_RETRAIN_EVERY_N - 1;
            *strategy.last_retrain_at.lock().unwrap() = None;
            *strategy.retrain_backoff_until.lock().unwrap() = None;
        };

        let dc = DynamicConfig::default();
        arm(&strategy, timed(structured_samples(n, 0xB37), start));
        strategy.maybe_retrain(&dc);
        assert!(strategy.is_training.load(Ordering::Relaxed), "the trigger must spawn a training job");
        wait_for_verdict().await;
        let accepted_at = {
            let m = strategy.model.lock().unwrap();
            let model = m.as_ref().expect("an accepted retrain installs the model");
            assert!(model.get_metadata(&MODEL_META_HOLDOUT_SKILL_KEY.to_string()).is_some());
            model.get_metadata(&MODEL_META_ACCEPTED_AT_KEY.to_string()).expect("acceptance record")
        };
        assert!(model_path.exists(), "only accepted models reach the disk, and this one was accepted");
        assert_eq!(*strategy.consecutive_degenerate.lock().unwrap(), 0);

        arm(&strategy, clock_memorizable_samples(n, start));
        strategy.maybe_retrain(&dc);
        assert!(strategy.is_training.load(Ordering::Relaxed));
        wait_for_verdict().await;
        {
            let m = strategy.model.lock().unwrap();
            let model = m.as_ref().expect("a rejected retrain keeps the previous model");
            assert_eq!(model.get_metadata(&MODEL_META_ACCEPTED_AT_KEY.to_string()).as_deref(), Some(accepted_at.as_str()),
                "the installed model is the one accepted before, untouched");
        }
        assert_eq!(*strategy.consecutive_degenerate.lock().unwrap(), 1, "a rejection counts toward backoff");
        assert!(strategy.retrain_backoff_until.lock().unwrap().is_some(), "a rejection arms the backoff");

        std::env::remove_var("GBOOST_MODEL_PATH");
        let _ = std::fs::remove_dir_all(model_path.parent().unwrap().parent().unwrap());
    }
}
