/// GBoost Strategy — Online gradient-boosted binary classification
///
/// Uses the `perpetual` crate's `PerpetualBooster` (LogLoss objective) to predict
/// near-term YES price direction from a rolling window of orderbook + oracle features.
///
/// ── Feature Vector (NUM_FEATURES = 22) ──────────────────────────────────────
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
///                             [0, MAX_SECONDS_TO_EXPIRY_FOR_ENTRY] and normalised
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
///                          history snapshots, normalised to [0, 1] (0 = calm, 1 = chaotic).
///                          2% per-tick log-return std-dev maps to 1.0 (extreme regime).
///                          NOTE: "60 snapshots" is a proxy for ~1h; actual wall-clock
///                          duration depends on tick rate at runtime.
///  [21]  tick_momentum    — net directionality of the last 10 YES bid ticks, normalised
///                          to [-1, +1] over (N−1) comparisons.
///                          +1 = all 9 ticks up (strong up momentum).
///                          −1 = all 9 ticks down (strong down momentum).
///
/// ── Label ────────────────────────────────────────────────────────────────────
///   1.0  if yes_bid rises in GBOOST_LOOKAHEAD_TICKS ticks
///   0.0  otherwise
///
/// ── Lifecycle ────────────────────────────────────────────────────────────────
///   1. Snapshots are pushed into a fixed-size ring buffer every tick.
///   2. Every GBOOST_RETRAIN_EVERY_N ticks (once MIN_TRAINING_SAMPLES exist),
///      training is offloaded to `tokio::task::spawn_blocking` so the rayon
///      threadpool never blocks the Tokio executor.
///   3. The trained model is swapped into `Arc<Mutex<Option<PerpetualBooster>>>`.
///   4. predict_proba() produces P(YES_UP) for each new tick.
///   5. Model is serialised to GBOOST_MODEL_PATH after each successful retrain
///      and reloaded from disk on strategy construction.

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
use alloy::primitives::U256; // For token_id in pending_entries

use crate::config;
use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{MarketSnapshot, OrderParams, StrategySignal, StrategyStatus};
use crate::vipers::is_drawdown_limit_hit;
use crate::helpers::price::floor_to_tick_size;
use crate::helpers::dynamic_config::DynamicConfig; // Corrected import
use polymarket_client_sdk_v2::clob::types::OrderType; // Import OrderType

/// Number of f64 features per snapshot row fed into the booster.
const NUM_FEATURES: usize = 22;

/// Represents a single training sample for the Gboost model.
/// Contains the features at the time of entry and whether the trade was profitable.
#[derive(Debug, Clone)]
pub struct TrainingSample {
    pub features: [f64; NUM_FEATURES],
    pub is_profitable: bool, // Label: true if profitable, false if loss
    pub entry_timestamp: chrono::DateTime<chrono::Utc>, // For context/debugging
}

// ── Feature extraction ────────────────────────────────────────────────────────

/// Normalisation divisor for secs_to_expiry: same as MAX_SECONDS_TO_EXPIRY_FOR_ENTRY (4 h).
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

/// Compute historical volatility regime from a slice of oracle prices (log-return std-dev).
/// Normalised to [0, 1] where 1.0 = 2% per-tick std-dev (extreme volatility).
fn compute_historical_volatility(prices: &[f64]) -> f64 {
    if prices.len() < 5 {
        return 0.0;
    }
    let mut log_returns: Vec<f64> = Vec::with_capacity(prices.len() - 1);
    for i in 1..prices.len() {
        if prices[i - 1] > 0.0 && prices[i] > 0.0 {
            log_returns.push((prices[i] / prices[i - 1]).ln());
        }
    }
    if log_returns.is_empty() {
        return 0.0;
    }
    let mean = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
    let variance = log_returns.iter()
        .map(|r| (r - mean).powi(2))
        .sum::<f64>() / log_returns.len() as f64;
    // Normalise: 0.020 (2% per-tick std-dev) → 1.0; cap at 1.0
    (variance.sqrt() / 0.020).min(1.0)
}

/// Compute tick-direction momentum from a slice of YES bid prices.
/// Returns (up_ticks − down_ticks) / (n−1), normalised to [−1, +1].
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

/// Compute hist_vol from a position in the history VecDeque (looks back up to 60 snapshots).
fn hist_vol_from_deque(h: &VecDeque<MarketSnapshot>, idx: usize) -> f64 {
    let start = idx.saturating_sub(59);
    let prices: Vec<f64> = (start..=idx)
        .filter_map(|k| h.get(k))
        .map(|s| s.oracle_price.to_f64().unwrap_or(1.0))
        .collect();
    compute_historical_volatility(&prices)
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

/// Compute hist_vol from a position in a `&[MarketSnapshot]` slice (used in concept-drift path).
fn hist_vol_from_slice(snaps: &[MarketSnapshot], idx: usize) -> f64 {
    let start = idx.saturating_sub(59);
    let prices: Vec<f64> = snaps[start..=idx]
        .iter()
        .map(|s| s.oracle_price.to_f64().unwrap_or(1.0))
        .collect();
    compute_historical_volatility(&prices)
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
    // Use obi_from_depths (which returns -1.0 on zero depth) to match the entry gate's
    // side_obi() convention.  The entry gate blocks zero-depth entries (OBI=-1.0 < adverse
    // threshold), so training records will also never have zero-depth — but prediction can
    // receive any snapshot. Aligning the default removes a hidden prediction/gate mismatch.
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

    // Normalise secs_to_expiry to [0.0, 1.0]:
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
    ]
}

// ── Training helper (runs inside spawn_blocking) ──────────────────────────────

/// Build and train a fresh `PerpetualBooster` from a slice of `TrainingSample`s.
/// Called exclusively from `tokio::task::spawn_blocking` — never on an async thread.
fn train_model(samples: Vec<TrainingSample>) -> Result<PerpetualBooster> {
    let n = samples.len();

    if n < config::GBOOST_MIN_TRAINING_SAMPLES {
        return Err(anyhow::anyhow!(
            "GBoost: too few training samples ({}) for training (need at least {})", n, config::GBOOST_MIN_TRAINING_SAMPLES
        ));
    }

    let mut feature_data: Vec<f64> = Vec::with_capacity(n * NUM_FEATURES);
    let mut labels: Vec<f64>       = Vec::with_capacity(n);

    for sample in samples {
        feature_data.extend_from_slice(&sample.features);
        // Label: 1.0 if profitable, 0.0 if not profitable
        labels.push(if sample.is_profitable { 1.0 } else { 0.0 });
    }

    // Matrix<'a, T> borrows the slice; both Vec and Matrix live in this closure scope.
    let matrix = Matrix::new(&feature_data, n, NUM_FEATURES);

    let mut booster = PerpetualBooster::default()
        .set_objective(Objective::LogLoss)
        .set_budget(config::GBOOST_BUDGET as f32)
        .set_num_threads(Some(config::GBOOST_NUM_THREADS as usize))
        .set_log_iterations(0)  // silent: suppress perpetual's stdout logging
        .set_max_bin(63)        // 63 bins is fast and sufficient for these features
        .set_iteration_limit(Some(1000))
        .set_stopping_rounds(None)
        .set_save_node_stats(true); // Required for drift detection via perpetual::drift

    booster.fit(&matrix, &labels, None, None)
        .map_err(|e| anyhow::anyhow!("perpetual fit error: {:?}", e))?;

    Ok(booster)
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
    let mut feature_data: Vec<f64> = Vec::with_capacity(n * NUM_FEATURES);
    for (i, snap) in recent_history.iter().enumerate() {
        let prev = if i > 0 { Some(&recent_history[i - 1]) } else { None };
        let hv = hist_vol_from_slice(recent_history, i);
        let tm = tick_momentum_from_slice(recent_history, i);
        let feats = extract_features(snap, prev, hv, tm);
        feature_data.extend_from_slice(&feats);
    }
    let matrix = Matrix::new(&feature_data, n, NUM_FEATURES);
    perpetual_calculate_drift(booster, &matrix, "concept", false)
}

// ── Strategy struct ───────────────────────────────────────────────────────────

pub struct GboostStrategyImpl {
    /// Trained booster. `std::sync::Mutex` (not tokio) because we never hold
    /// it across an `.await` — only for quick read/write of the model pointer.
    model: Arc<StdMutex<Option<PerpetualBooster>>>,
    /// Ring buffer of recent market snapshots for feature engineering and labelling.
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
    pending_entries: Arc<StdMutex<HashMap<U256, (MarketSnapshot, Option<MarketSnapshot>, rust_decimal::Decimal, f64, f64)>>>,
    /// Per-token (start_time, cooldown_secs) of the last emitted exit signal.
    /// TP/SignalRev exits store GBOOST_POST_EXIT_COOLDOWN_SECS; SL exits store
    /// GBOOST_SL_POST_EXIT_COOLDOWN_SECS (longer, because an SL means the market
    /// moved adversely and re-entering quickly compounds the loss).
    post_exit_cooldowns: Arc<StdMutex<HashMap<U256, (chrono::DateTime<chrono::Utc>, i64)>>>,
    /// Count of consecutive degenerate (< GBOOST_MIN_USABLE_TREES) retrain results.
    /// Used to apply exponential backoff so a 10-second retrain storm doesn't burn CPU
    /// for 110+ minutes as seen in the 2026-05-07 evening session.
    consecutive_degenerate: Arc<StdMutex<usize>>,
    /// When set, `maybe_retrain` skips all retrain attempts until this instant passes.
    retrain_backoff_until: Arc<StdMutex<Option<Instant>>>,
    /// When set, records the `Instant` at which BTC spot first dropped below
    /// (daily_strike − BASIS_BTC_ORACLE_STRIKE_BUFFER).  Resets to None whenever spot
    /// recovers above the threshold.  Used to suppress YES entries on daily markets when
    /// BTC has been continuously below the strike buffer for ≥ GBOOST_BELOW_STRIKE_SUPPRESS_SECS.
    below_strike_since: Arc<StdMutex<Option<Instant>>>,
    /// Set to `true` after TWO CONSECUTIVE retrains where concept drift exceeded
    /// GBOOST_CONCEPT_DRIFT_THRESHOLD.  A single spike is not suppressed — it could
    /// be a transient liquidity shock.  Two in a row implies a genuine regime change.
    /// Cleared when any retrain scores at or below the threshold.
    concept_drift_suppressed: Arc<AtomicBool>,
    /// Most recent concept drift score from `perpetual::drift::calculate_drift`.
    /// Logged at DEBUG level in the entry gate; exposed here for diagnostics.
    last_concept_drift_score: Arc<StdMutex<f32>>,
    /// Count of consecutive retrains where drift_score > GBOOST_CONCEPT_DRIFT_THRESHOLD.
    /// Suppression only activates when this reaches 2 — prevents single spikes from
    /// permanently blocking entries.  Resets to 0 on any below-threshold retrain.
    consecutive_drift_above_threshold: Arc<StdMutex<usize>>,
}

impl GboostStrategyImpl {
    pub fn new() -> Self {
        let model_arc = Arc::new(StdMutex::new(None::<PerpetualBooster>));

        // Warm-start: try to load a previously persisted model from disk.
        //
        // The model path is version-locked to the current feature set (NUM_FEATURES = 22).
        // NEVER load a model from a different version — the feature dimensions won't match.
        // History: v14f (14 features) → v19f (added yes_mid_change, no_obi_change,
        // relative_depth_ratio, combined_ask_spread, oracle_drift_10m in May 2026) →
        // v22f (added spread_velocity, hist_vol_regime, tick_momentum in May 2026).
        //
        // Override the path at runtime via the GBOOST_MODEL_PATH env var, e.g.:
        //   GBOOST_MODEL_PATH=/path/to/gboost_model_v19f.json cargo run
        // This is the recommended way to seed a local instance with a model trained on prod.
        let model_clone = Arc::clone(&model_arc);
        tokio::spawn(async move {
            // Env var takes precedence over the compiled-in path.
            let model_path = std::env::var("GBOOST_MODEL_PATH")
                .unwrap_or_else(|_| config::GBOOST_MODEL_PATH.to_string());

            match tokio::fs::read_to_string(&model_path).await {
                Ok(json) => match PerpetualBooster::from_json(&json) {
                    Ok(loaded) => {
                        let n = loaded.trees.len();
                        // Discard a degenerate startup model — a stale 1-tree model is worse
                        // than no model at all because it sticks as "previous" during retrain
                        // storms and prevents the engine from cold-starting cleanly.
                        // 2026-05-07 evening: startup model had 1 tree, kept as "previous"
                        // for 110 minutes while every retrain hit the same degenerate result.
                        if n < config::GBOOST_MIN_USABLE_TREES {
                            tracing::warn!(
                                "🤖 GboostStrategy: discarding persisted model from '{}' ({} trees < min {}), cold-starting",
                                model_path, n, config::GBOOST_MIN_USABLE_TREES
                            );
                        } else {
                            *model_clone.lock().unwrap() = Some(loaded);
                            tracing::info!(
                                "🤖 GboostStrategy: loaded persisted model from '{}' ({} trees)",
                                model_path, n
                            );
                        }
                    }
                    Err(e) => tracing::warn!(
                        "🤖 GboostStrategy: model parse failed for '{}' (will train from scratch): {:?}",
                        model_path, e
                    ),
                },
                Err(_) => tracing::info!(
                    "🤖 GboostStrategy: no persisted model at '{}' — collecting data to train \
                     (tip: copy prod model here, or set GBOOST_MODEL_PATH env var)",
                    model_path
                ),
            }
        });

        Self {
            model: model_arc,
            history: Arc::new(StdMutex::new(
                VecDeque::with_capacity(config::GBOOST_HISTORY_BUFFER_SIZE + 16)
            )),
            ticks_since_retrain: Arc::new(StdMutex::new(0)),
            is_training: Arc::new(AtomicBool::new(false)),
            training_data: Arc::new(StdMutex::new(
                VecDeque::with_capacity(config::GBOOST_HISTORY_BUFFER_SIZE)
            )),
            pending_entries: Arc::new(StdMutex::new(HashMap::new())),
            post_exit_cooldowns: Arc::new(StdMutex::new(HashMap::new())),
            consecutive_degenerate: Arc::new(StdMutex::new(0)),
            retrain_backoff_until: Arc::new(StdMutex::new(None)),
            below_strike_since: Arc::new(StdMutex::new(None)),
            concept_drift_suppressed: Arc::new(AtomicBool::new(false)),
            last_concept_drift_score: Arc::new(StdMutex::new(0.0_f32)),
            consecutive_drift_above_threshold: Arc::new(StdMutex::new(0)),
        }
    }

    /// Push snapshot into the ring buffer, evicting the oldest entry when at capacity.
    fn push_snapshot(&self, snap: MarketSnapshot) {
        let mut h = self.history.lock().unwrap();
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
    ///      sparse.  Label: yes_bid rises within GBOOST_LOOKAHEAD_TICKS ticks → 1.0.
    ///      This breaks the chicken-and-egg deadlock that prevents the model from ever
    ///      reaching the minimum sample count required to produce its first predictions.
    fn maybe_retrain(&self) {
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

        let triggered = {
            let mut t = self.ticks_since_retrain.lock().unwrap();
            *t += 1;
            *t >= config::GBOOST_RETRAIN_EVERY_N
        };
        if !triggered { return; }

        // ── Collect real trade outcomes (Source 1: highest quality labels) ──────────
        // These are actual entry/exit P&L labels — far more informative than lookahead
        // proxies.  Always collected first; they are prepended to the training batch so
        // the model encounters them before the denser lookahead fill.
        let real_samples: Vec<TrainingSample> = {
            let td = self.training_data.lock().unwrap();
            td.iter().cloned().collect()
        };

        let training_samples: Vec<TrainingSample> = if real_samples.len() >= config::GBOOST_MIN_TRAINING_SAMPLES {
            // Enough real trade outcomes — use them exclusively for the cleanest signal.
            real_samples
        } else {
            // ── Source 2 (+ Source 1 blend): lookahead labels from history ring buffer ──
            // When real outcomes exist but are too few, prepend them to the lookahead batch.
            // This breaks the chicken-and-egg bootstrap deadlock while continuously
            // improving model quality as real trade-outcome labels accumulate.
            // Label = 1.0 if the YES bid rises within GBOOST_LOOKAHEAD_TICKS ticks.
            let h = self.history.lock().unwrap();
            let n = h.len();
            let lookahead = config::GBOOST_LOOKAHEAD_TICKS;
            // After blending, we need at least (MIN - real_count) lookahead samples plus
            // the lookahead window itself.  Without real samples this collapses to the
            // original MIN_TRAINING_SAMPLES + lookahead check.
            let needed = config::GBOOST_MIN_TRAINING_SAMPLES.saturating_sub(real_samples.len());
            if n < needed + lookahead {
                return; // Not enough history yet, wait for more ticks
            }
            let usable = n - lookahead;
            let mut combined = real_samples; // Real outcomes first (highest quality)
            combined.extend((0..usable).map(|i| {
                let snap      = &h[i];
                let prev_snap = if i > 0 { Some(&h[i-1]) } else { None };
                let future    = &h[i + lookahead];
                let hv = hist_vol_from_deque(&h, i);
                let tm = tick_momentum_from_deque(&h, i);
                TrainingSample {
                    features: extract_features(snap, prev_snap, hv, tm),
                    is_profitable: future.yes_bid > snap.yes_bid,
                    entry_timestamp: snap.timestamp,
                }
            }));
            combined
        };

        if training_samples.len() < config::GBOOST_MIN_TRAINING_SAMPLES { return; }

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
                "🤖 GBoost: skipping retrain — labels imbalanced ({:.0}% positive > max {:.0}%), waiting for balanced data",
                pos_fraction * 100.0, config::GBOOST_LOOKAHEAD_LABEL_BALANCE_MAX * 100.0
            );
            *self.ticks_since_retrain.lock().unwrap() = 0;
            return;
        }

        *self.ticks_since_retrain.lock().unwrap() = 0;
        self.is_training.store(true, Ordering::Relaxed);

        let model_arc   = Arc::clone(&self.model);
        let is_training = Arc::clone(&self.is_training);
        let consecutive_degenerate    = Arc::clone(&self.consecutive_degenerate);
        let retrain_backoff_until     = Arc::clone(&self.retrain_backoff_until);
        let concept_drift_suppressed  = Arc::clone(&self.concept_drift_suppressed);
        let last_concept_drift_score  = Arc::clone(&self.last_concept_drift_score);
        let consecutive_drift_counter = Arc::clone(&self.consecutive_drift_above_threshold);

        // Capture a window of recent snapshots for concept-drift evaluation.
        // Oldest-first order (same as extract_features expects for prev_s).
        let history_for_drift: Vec<MarketSnapshot> = {
            let h = self.history.lock().unwrap();
            let n = h.len().min(config::GBOOST_DRIFT_WINDOW);
            h.iter().skip(h.len().saturating_sub(n)).cloned().collect()
        };

        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                let model = train_model(training_samples)?;
                let drift  = compute_concept_drift(&model, &history_for_drift);
                Ok::<(PerpetualBooster, f32), anyhow::Error>((model, drift))
            }).await;

            match result {
                Ok(Ok((new_model, drift_score))) => {
                    let n = new_model.trees.len();
                    // Persist to disk first so a crash doesn't lose the trained weights.
                    // Use the same path resolution as startup load (env var override first).
                    if let Ok(json) = new_model.json_dump() {
                        let model_path = std::env::var("GBOOST_MODEL_PATH")
                            .unwrap_or_else(|_| config::GBOOST_MODEL_PATH.to_string());
                        if let Err(e) = tokio::fs::write(&model_path, &json).await {
                            tracing::warn!("🤖 GboostStrategy: model save failed [{}]: {}", model_path, e);
                        }
                    }

                    // Reject degenerate models — a model with fewer than
                    // GBOOST_MIN_USABLE_TREES trees is essentially a random stump.
                    // Keep the previous (better) model rather than regressing.
                    // Apply exponential backoff so we don't storm every 10 seconds.
                    if n < config::GBOOST_MIN_USABLE_TREES {
                        let mut count = consecutive_degenerate.lock().unwrap();
                        *count += 1;
                        let backoff_secs = (20u64 * 2u64.pow((*count).saturating_sub(1).min(4) as u32)).min(300);
                        *retrain_backoff_until.lock().unwrap() =
                            Some(Instant::now() + std::time::Duration::from_secs(backoff_secs));
                        tracing::warn!(
                            "🤖 GboostStrategy: retrain produced degenerate model ({} trees < min {}), keeping previous (backoff {}s, #{} consecutive)",
                            n, config::GBOOST_MIN_USABLE_TREES, backoff_secs, *count
                        );
                        is_training.store(false, Ordering::Relaxed);
                        return;
                    }

                    // Good model — reset degenerate backoff counters.
                    *consecutive_degenerate.lock().unwrap() = 0;
                    *retrain_backoff_until.lock().unwrap() = None;

                    // ── Concept drift monitoring ──────────────────────────────
                    // Compare how live data flows through the new model's split points
                    // vs. the training distribution.  A high chi-squared score means the
                    // current market regime is outside what the model was trained on.
                    //
                    // Dampening: a SINGLE spike is insufficient for suppression — it could
                    // be a transient liquidity shock or the temporal mismatch between the
                    // training window (oldest ticks) and drift window (newest ticks).
                    // Only suppress after TWO CONSECUTIVE retrains above threshold.
                    *last_concept_drift_score.lock().unwrap() = drift_score;
                    if drift_score > config::GBOOST_CONCEPT_DRIFT_THRESHOLD {
                        let mut count = consecutive_drift_counter.lock().unwrap();
                        *count += 1;
                        if *count >= config::GBOOST_DRIFT_CONSECUTIVE_REQUIRED {
                            tracing::warn!(
                                "⚠️ GBoost: concept drift confirmed ({} consecutive retrains, \
                                 latest score={:.2} > threshold {:.2}) — suppressing entries \
                                 until next retrain recaptures regime",
                                *count, drift_score, config::GBOOST_CONCEPT_DRIFT_THRESHOLD
                            );
                            concept_drift_suppressed.store(true, Ordering::Relaxed);
                        } else {
                            tracing::warn!(
                                "⚠️ GBoost: drift spike #{} (score={:.2} > threshold {:.2}) — \
                                 watching for {} consecutive trigger before suppressing",
                                *count, drift_score, config::GBOOST_CONCEPT_DRIFT_THRESHOLD,
                                config::GBOOST_DRIFT_CONSECUTIVE_REQUIRED
                            );
                        }
                    } else {
                        // Below threshold → reset consecutive counter and clear suppression
                        let mut count = consecutive_drift_counter.lock().unwrap();
                        if *count > 0 || concept_drift_suppressed.load(Ordering::Relaxed) {
                            tracing::info!(
                                "✅ GBoost: concept drift cleared (score={:.2} ≤ threshold {:.2}, \
                                 consecutive counter was {}) — resuming entries",
                                drift_score, config::GBOOST_CONCEPT_DRIFT_THRESHOLD, *count
                            );
                        }
                        *count = 0;
                        concept_drift_suppressed.store(false, Ordering::Relaxed);
                    }

                    let old_tree_count = {
                        let old_model = model_arc.lock().unwrap();
                        old_model.as_ref().map(|m| m.trees.len()).unwrap_or(0)
                    };

                    if n > old_tree_count + 5 || (old_tree_count >= 5 && n + 5 < old_tree_count) || (old_tree_count == 0 && n > 0) {
                        tracing::info!(
                            "🤖 GboostStrategy: retrained — {} trees (was {}) | drift={:.2}",
                            n, old_tree_count, drift_score
                        );
                    } else {
                        tracing::debug!("🤖 GboostStrategy: retrained — {} trees | drift={:.2}", n, drift_score);
                    }

                    *model_arc.lock().unwrap() = Some(new_model);
                }
                Ok(Err(e)) => tracing::warn!("🤖 GboostStrategy: training error: {}", e),
                Err(e)     => tracing::warn!("🤖 GboostStrategy: spawn_blocking panic: {}", e),
            }

            is_training.store(false, Ordering::Relaxed);
        });
    }

    /// Return P(YES_UP) ∈ [0, 1] from the current model, or `None` if no model exists yet.
    fn predict(&self, snap: &MarketSnapshot) -> Option<f64> {
        let guard = self.model.lock().unwrap();
        let booster = guard.as_ref()?;
        // Refuse to predict from degenerate models — a single stump is random noise.
        if booster.trees.len() < config::GBOOST_MIN_USABLE_TREES {
            return None;
        }
        let h = self.history.lock().unwrap();
        let n = h.len();
        let prev_snap = if n >= 2 { Some(&h[n - 2]) } else { None }; // Get previous snapshot
        let hist_vol = if n > 0 { hist_vol_from_deque(&h, n - 1) } else { 0.0 };
        let tick_momentum = if n > 0 { tick_momentum_from_deque(&h, n - 1) } else { 0.0 };
        let feats = extract_features(snap, prev_snap, hist_vol, tick_momentum); // Pass prev_snap
        // Stack-allocated array; Matrix borrows it for the duration of this call only.
        let matrix = Matrix::new(&feats, 1, NUM_FEATURES);
        booster.predict_proba(&matrix, false, false).first().copied()
    }

    /// Persist one supervised label only when an exit signal is emitted.
    /// This avoids training on transient mark-to-market states from non-exit ticks.
    fn record_training_outcome_on_exit(&self, token_id: U256, is_profitable: bool) {
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
    fn mark_post_exit_cooldown(&self, token_id: U256) {
        let mut guard = self.post_exit_cooldowns.lock().unwrap();
        guard.insert(token_id, (Utc::now(), config::GBOOST_POST_EXIT_COOLDOWN_SECS));
    }

    /// Mark token as cooling down with the **extended** cooldown after a stop-loss exit.
    ///
    /// An SL exit means the market moved adversely against the position — not just that the
    /// model changed direction.  Using a longer cooldown (GBOOST_SL_POST_EXIT_COOLDOWN_SECS)
    /// prevents re-entering the same adverse direction within 20 minutes of a loss.
    fn mark_post_exit_cooldown_sl(&self, token_id: U256) {
        let mut guard = self.post_exit_cooldowns.lock().unwrap();
        guard.insert(token_id, (Utc::now(), config::GBOOST_SL_POST_EXIT_COOLDOWN_SECS));
    }

    /// Returns remaining cooldown seconds for this token, if still cooling down.
    fn post_exit_cooldown_remaining_secs(&self, token_id: U256) -> Option<i64> {
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
    fn side_obi(is_yes_side: bool, s: &MarketSnapshot) -> rust_decimal::Decimal {
        let (bid, ask) = if is_yes_side {
            (s.yes_bid_depth, s.yes_ask_depth)
        } else {
            (s.no_bid_depth, s.no_ask_depth)
        };
        let total = bid + ask;
        if total > dec!(0) {
            (bid - ask) / total
        } else {
            dec!(-1.0) // no depth data → treat as maximally adverse → block entry
        }
    }
}

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
        // Push the snapshot from the market GBoost actually TRADES on (daily/maker when
        // available, hourly otherwise).  Previously ctx.snapshot (hourly) was always pushed,
        // meaning the model was trained on hourly OBI features but predicted from daily OBI
        // features — a fundamental mismatch.  Near hourly expiry the hourly OBI freezes at
        // ±0.99, flooding the history buffer with homogeneous features and causing perpetual
        // to auto-stop at 1 tree.  Using the daily snapshot keeps features healthy.
        let training_snapshot = if ctx.maker_snapshot.is_some() {
            ctx.maker_snapshot.as_ref().unwrap().clone()
        } else {
            ctx.snapshot.clone()
        };
        self.push_snapshot(training_snapshot);
        self.maybe_retrain();

        let dc = &ctx.dynamic_config;
        if !dc.enable_gboost {
            return Ok(StrategySignal::NoSignal);
        }
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate: market must be mature enough for orderbook features to be stable ──
        if (Utc::now() - ctx.market_started_at).num_seconds() < config::GBOOST_MIN_MARKET_AGE_SECS {
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
                    "🚫 GBoost entry blocked: hourly book degenerate (ask_sum={:.3} bid_sum={:.3})",
                    hourly_ask_sum, hourly_bid_sum
                );
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
                    "🚫 GBoost entry blocked: hourly market near-resolved \
                     (yes_bid={:.3} < {:.2} or yes_ask={:.3} > {:.2})",
                    hourly_yes_bid_f, config::GBOOST_MIN_HOURLY_YES_BID,
                    hourly_yes_ask_f, config::GBOOST_MAX_HOURLY_YES_ASK,
                );
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
                "🚫 GBoost entry blocked: target book too wide (ask_sum={:.3} > max {:.3})",
                target_ask_sum, config::GBOOST_MAX_TARGET_ASK_SUM
            );
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
                "🚫 GBoost entry blocked: target snapshot too stale ({}s > max {}s)",
                target_snap_age, config::GBOOST_MAX_SNAPSHOT_AGE_SECS
            );
            return Ok(StrategySignal::NoSignal);
        }
        // Also gate on hourly snapshot staleness when trading the daily market.
        if ctx.maker_snapshot.is_some() {
            let hourly_snap_age = (chrono::Utc::now() - ctx.snapshot.timestamp).num_seconds();
            if hourly_snap_age > config::GBOOST_MAX_SNAPSHOT_AGE_SECS {
                tracing::debug!(
                    "🚫 GBoost entry blocked: hourly snapshot too stale ({}s > max {}s)",
                    hourly_snap_age, config::GBOOST_MAX_SNAPSHOT_AGE_SECS
                );
                return Ok(StrategySignal::NoSignal);
            }
        }

        if let Some(close_time) = target_market.market_close_time {
            if (close_time - Utc::now()).num_seconds() < config::GBOOST_MIN_SECS_TO_EXPIRY {
                return Ok(StrategySignal::NoSignal);
            }
        }
        // ── Gate: sufficient collateral ───────────────────────────────────────
        if ctx.available_collateral < dc.gboost_max_exposure_usdc {
            return Ok(StrategySignal::NoSignal);
        }

        let p_yes_up = match self.predict(target_snapshot) {
            Some(p) => p,
            None    => return Ok(StrategySignal::NoSignal),
        };

        // ── Gate: concept drift suppression ──────────────────────────────────
        // If the last retrain detected that live market data is flowing through
        // the model's split points very differently from the training distribution,
        // suppress entries until the next retrain recaptures the regime.
        if self.concept_drift_suppressed.load(Ordering::Relaxed) {
            tracing::debug!(
                "🚫 GBoost entry blocked: concept drift (score={:.2}) — awaiting next retrain",
                *self.last_concept_drift_score.lock().unwrap()
            );
            return Ok(StrategySignal::NoSignal);
        }

        let entry_thresh = dc.gboost_entry_threshold.to_f64().unwrap_or(0.65);

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

        // ── Gate: trend-alignment ─────────────────────────────────────────────
        // If BTC has drifted strongly in one direction over the past 60 minutes,
        // entering counter-trend is systematically unprofitable.
        // Always uses the hourly snapshot for oracle data (drift is asset-level).
        //   drift >  +$200 → uptrend  → block NO entries
        //   drift < -$200  → downtrend → block YES entries
        // Mirrors MAKER_SLOW_TREND_THRESHOLD_BTC and TIME_DECAY_MAX_SLOW_DRIFT_BTC.
        let drift_60m = ctx.snapshot.oracle_drift_60m;
        let trend_block = config::GBOOST_TREND_DRIFT_BLOCK_USD;

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
                let buffer = config::BASIS_BTC_ORACLE_STRIKE_BUFFER;
                let threshold = strike - buffer;
                let mut bss = self.below_strike_since.lock().unwrap();
                if oracle_price < threshold {
                    // Spot is below the buffer — start or continue the timer
                    if bss.is_none() {
                        *bss = Some(Instant::now());
                        tracing::debug!(
                            "🕐 GBoost: BTC spot ${:.0} < strike(${:.0}) − buffer(${:.0}) = ${:.0} — starting below-strike timer",
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
                map.contains_key(&("GboostStrategy".to_string(), target_market.yes_token)),
                map.contains_key(&("GboostStrategy".to_string(), target_market.no_token)),
            )
        };

        // ── YES entry: model predicts UP ──────────────────────────────────────
        if p_yes_up >= entry_thresh && !has_yes {
            // Trend-alignment: block YES entries in a downtrend
            if drift_60m < -trend_block {
                tracing::debug!(
                    "🚫 GBoost YES entry veto: counter-trend (drift_60m={:.0} < -{:.0})",
                    drift_60m, trend_block
                );
                return Ok(StrategySignal::NoSignal);
            }
            // Strike-distance: block YES entries when BTC has been below (strike−buffer) for 60+ min
            if below_strike_suppressed_for_yes {
                tracing::debug!(
                    "🚫 GBoost YES entry veto: below-strike suppressed \
                     (BTC spot below daily_strike − ${:.0} for ≥ {}min)",
                    config::BASIS_BTC_ORACLE_STRIKE_BUFFER,
                    config::GBOOST_BELOW_STRIKE_SUPPRESS_SECS / 60
                );
                return Ok(StrategySignal::NoSignal);
            }
            // ── Entry latch: skip if an entry for this token is already in-flight ──
            // Between emitting an Entry signal and the position being confirmed in
            // pos_map (can be several seconds), evaluate_entry fires every tick.
            // Without this guard those ticks all re-emit Entry signals, flooding
            // the executor and potentially placing duplicate orders.
            if self.pending_entries.lock().unwrap().contains_key(&target_market.yes_token) {
                return Ok(StrategySignal::NoSignal);
            }
            if let Some(remaining_secs) = self.post_exit_cooldown_remaining_secs(target_market.yes_token) {
                tracing::debug!(
                    "🚫 GBoost YES entry veto: cooldown active | market='{}' token={:?} remaining={}s",
                    target_market.market_name,
                    target_market.yes_token,
                    remaining_secs
                );
                return Ok(StrategySignal::NoSignal);
            }
            let yes_obi = Self::side_obi(true, target_snapshot);
            if yes_obi < config::GBOOST_OBI_ADVERSE_BLOCK {
                tracing::debug!(
                    "🚫 GBoost YES entry veto: adverse OBI | market='{}' token={:?} obi={:.3} block={:.3}",
                    target_market.market_name,
                    target_market.yes_token,
                    yes_obi,
                    config::GBOOST_OBI_ADVERSE_BLOCK
                );
                return Ok(StrategySignal::NoSignal);
            }
            // ── OBI exhaustion gate ───────────────────────────────────────────
            // When |obi| is very large the move is already mature — entering YES
            // into an already-resolved book is tail-chasing with maximum adverse
            // selection. 2026-05-08 T2: |obi_y|=0.61 on a ~93% YES market → SL -$0.457.
            if yes_obi.abs() > config::GBOOST_OBI_EXHAUSTION_BLOCK {
                tracing::debug!(
                    "🚫 GBoost YES entry veto: OBI exhaustion | market='{}' |obi|={:.3} > {:.3}",
                    target_market.market_name,
                    yes_obi.abs(),
                    config::GBOOST_OBI_EXHAUSTION_BLOCK
                );
                return Ok(StrategySignal::NoSignal);
            }
            // ── Hourly OBI direction check for daily entries ──────────────────
            // When trading daily market, the hourly YES OBI foreshadows daily direction.
            // If hourly YES is being aggressively sold (OBI << 0), smart money is
            // fading a pump — entering daily YES contradicts the hourly signal.
            // 2026-05-07 afternoon: blocked entries where hourly OBI was -0.81 to -0.88.
            if ctx.maker_snapshot.is_some() {
                let hourly_yes_obi = Self::side_obi(true, &ctx.snapshot);
                if hourly_yes_obi < config::GBOOST_HOURLY_OBI_ADVERSE_BLOCK {
                    tracing::debug!(
                        "🚫 GBoost YES entry veto: hourly YES OBI adverse | obi={:.3} block={:.3}",
                        hourly_yes_obi, config::GBOOST_HOURLY_OBI_ADVERSE_BLOCK
                    );
                    return Ok(StrategySignal::NoSignal);
                }
                // ── Hourly OBI exhaustion check ───────────────────────────────
                // When the hourly book is overwhelmingly bid-dominated (OBI > +threshold)
                // the momentum move on the hourly venue is exhausted — all buyers are in,
                // sellers haven't arrived yet.  The subsequent flush propagates to the daily
                // market, dragging daily YES down with it.
                // 2026-05-24 11:39: hourly YES OBI=0.85 at entry → price fell from $0.54
                // to $0.49 in 45 s (-$0.26 loss).  Blocked at OBI_EXHAUSTION_BLOCK=0.80.
                if hourly_yes_obi.abs() > config::GBOOST_OBI_EXHAUSTION_BLOCK {
                    tracing::debug!(
                        "🚫 GBoost YES entry veto: hourly OBI exhausted | hourly_obi={:.3} > {:.3}",
                        hourly_yes_obi, config::GBOOST_OBI_EXHAUSTION_BLOCK
                    );
                    return Ok(StrategySignal::NoSignal);
                }
            }
            let price  = floor_to_tick_size(target_snapshot.yes_ask);
            if price >= config::GBOOST_MAX_YES_ENTRY_PRICE
                || price < config::GBOOST_MIN_ENTRY_PRICE
                || price <= dec!(0)
            {
                return Ok(StrategySignal::NoSignal);
            }
            // ── 50-cent coin-flip zone gate ───────────────────────────────────
            // Near 0.50 the market is directionally undecided. With 10% round-trip
            // taker fees, GBoost needs > 10% price move to break even — impossible
            // in a 50/50 coin flip. Require minimum edge distance from fair value.
            if (price - dec!(0.50)).abs() < config::GBOOST_MIN_EDGE_FROM_FAIR {
                tracing::debug!(
                    "🚫 GBoost YES entry veto: price too close to 0.50 | ask={:.3} edge={:.3} < min {:.3}",
                    price,
                    (price - dec!(0.50)).abs(),
                    config::GBOOST_MIN_EDGE_FROM_FAIR
                );
                return Ok(StrategySignal::NoSignal);
            }
            // Confidence-proportional sizing: more capital for higher-conviction signals.
            // Volatility-scaled: reduce size when oracle hist_vol_regime is elevated.
            let trade_usdc = scale_trade_size(p_yes_up, entry_thresh, precomp_hist_vol, dc.gboost_max_exposure_usdc);
            let shares = trade_usdc / price;
            tracing::info!(
                "🔮 GBoost YES entry: P(UP)={:.3} | ask=${:.4} shares={:.2} usdc={:.2} vol={:.2}",
                p_yes_up, price, shares, trade_usdc, precomp_hist_vol
            );
            let (entry_prev_snap, entry_hist_vol, entry_tick_momentum) =
                (precomp_prev_snap.clone(), precomp_hist_vol, precomp_tick_momentum);
            self.pending_entries.lock().unwrap().insert(
                target_market.yes_token,
                (target_snapshot.clone(), entry_prev_snap, price, entry_hist_vol, entry_tick_momentum)
            );
            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: target_market.yes_token,
                    price, shares,
                    fee_bps:     target_market.yes_fee_bps as u16,
                    is_neg_risk: target_market.is_neg_risk,
                    market_name: target_market.market_name.clone(),
                    condition_id: target_market.condition_id.clone(),
                    order_type: OrderType::FAK,
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
                tracing::debug!(
                    "🚫 GBoost NO entry veto: counter-trend (drift_60m={:.0} > +{:.0})",
                    drift_60m, trend_block
                );
                return Ok(StrategySignal::NoSignal);
            }
            // ── Entry latch ──────────────────────────────────────────────────
            if self.pending_entries.lock().unwrap().contains_key(&target_market.no_token) {
                return Ok(StrategySignal::NoSignal);
            }
            if let Some(remaining_secs) = self.post_exit_cooldown_remaining_secs(target_market.no_token) {
                tracing::debug!(
                    "🚫 GBoost NO entry veto: cooldown active | market='{}' token={:?} remaining={}s",
                    target_market.market_name, target_market.no_token, remaining_secs
                );
                return Ok(StrategySignal::NoSignal);
            }
            let no_obi = Self::side_obi(false, target_snapshot);
            if no_obi < config::GBOOST_OBI_ADVERSE_BLOCK {
                tracing::debug!(
                    "🚫 GBoost NO entry veto: adverse OBI | market='{}' token={:?} obi={:.3} block={:.3}",
                    target_market.market_name, target_market.no_token, no_obi, config::GBOOST_OBI_ADVERSE_BLOCK
                );
                return Ok(StrategySignal::NoSignal);
            }
            // ── OBI exhaustion gate ───────────────────────────────────────────
            // Blocks NO entries where the book is already heavily one-sided against
            // the NO leg — the move is in progress and the risk/reward is exhausted.
            if no_obi.abs() > config::GBOOST_OBI_EXHAUSTION_BLOCK {
                tracing::debug!(
                    "🚫 GBoost NO entry veto: OBI exhaustion | market='{}' |obi|={:.3} > {:.3}",
                    target_market.market_name,
                    no_obi.abs(),
                    config::GBOOST_OBI_EXHAUSTION_BLOCK
                );
                return Ok(StrategySignal::NoSignal);
            }
            // ── Hourly OBI direction check for daily entries ──────────────────
            // When trading daily market, the hourly NO OBI reveals whether smart money
            // is selling NO (= they think BTC went UP, so NO is losing = bad for NO entry).
            // If hourly NO is being aggressively sold (obi_n << 0), entering daily NO
            // directly contradicts the hourly directional signal.
            // 2026-05-07 trade 2: hourly NO OBI = -0.88 → blocked (saved $0.30).
            // 2026-05-07 trade 9: hourly NO OBI = -0.81 → blocked (saved $0.30).
            if ctx.maker_snapshot.is_some() {
                let hourly_no_obi = Self::side_obi(false, &ctx.snapshot);
                if hourly_no_obi < config::GBOOST_HOURLY_OBI_ADVERSE_BLOCK {
                    tracing::debug!(
                        "🚫 GBoost NO entry veto: hourly NO OBI adverse | obi={:.3} block={:.3}",
                        hourly_no_obi, config::GBOOST_HOURLY_OBI_ADVERSE_BLOCK
                    );
                    return Ok(StrategySignal::NoSignal);
                }
                // ── Hourly OBI exhaustion check ───────────────────────────────
                // Mirrors the YES exhaustion check: when hourly NO book is overwhelmingly
                // bid-dominated (OBI > +threshold), the NO-side momentum is exhausted and
                // a reversal is imminent.  Entering daily NO at this point means buying
                // into the last ticks of a NO surge — adverse selection is at its peak.
                if hourly_no_obi.abs() > config::GBOOST_OBI_EXHAUSTION_BLOCK {
                    tracing::debug!(
                        "🚫 GBoost NO entry veto: hourly OBI exhausted | hourly_obi={:.3} > {:.3}",
                        hourly_no_obi, config::GBOOST_OBI_EXHAUSTION_BLOCK
                    );
                    return Ok(StrategySignal::NoSignal);
                }
            }
            let price  = floor_to_tick_size(target_snapshot.no_ask);
            if price > config::GBOOST_MAX_NO_ENTRY_PRICE
                || price < config::GBOOST_MIN_ENTRY_PRICE
                || price <= dec!(0)
            {
                return Ok(StrategySignal::NoSignal);
            }
            // ── 50-cent coin-flip zone gate ───────────────────────────────────
            if (price - dec!(0.50)).abs() < config::GBOOST_MIN_EDGE_FROM_FAIR {
                tracing::debug!(
                    "🚫 GBoost NO entry veto: price too close to 0.50 | ask={:.3} edge={:.3} < min {:.3}",
                    price,
                    (price - dec!(0.50)).abs(),
                    config::GBOOST_MIN_EDGE_FROM_FAIR
                );
                return Ok(StrategySignal::NoSignal);
            }
            // Confidence for NO direction is (1 - p_yes_up); scale size accordingly.
            let trade_usdc = scale_trade_size(1.0 - p_yes_up, entry_thresh, precomp_hist_vol, dc.gboost_max_exposure_usdc);
            let shares = trade_usdc / price;
            tracing::info!(
                "🔮 GBoost NO entry: P(UP)={:.3} | ask=${:.4} shares={:.2} usdc={:.2} vol={:.2}",
                p_yes_up, price, shares, trade_usdc, precomp_hist_vol
            );
            let (entry_prev_snap, entry_hist_vol, entry_tick_momentum) =
                (precomp_prev_snap.clone(), precomp_hist_vol, precomp_tick_momentum);
            self.pending_entries.lock().unwrap().insert(
                target_market.no_token,
                (target_snapshot.clone(), entry_prev_snap, price, entry_hist_vol, entry_tick_momentum)
            );
            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: target_market.no_token,
                    price, shares,
                    fee_bps:     target_market.no_fee_bps as u16,
                    is_neg_risk: target_market.is_neg_risk,
                    market_name: target_market.market_name.clone(),
                    condition_id: target_market.condition_id.clone(),
                    order_type: OrderType::FAK,
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
        let signal_exit_thresh = config::GBOOST_SIGNAL_EXIT_THRESHOLD.to_f64().unwrap_or(0.40);
        let tp                 = dc.gboost_target_profit_pct.to_f64().unwrap_or(0.15);
        let sl                 = dc.gboost_stop_loss_pct.to_f64().unwrap_or(0.10);

        let pos_map = ctx.positions.lock().await;

        // ── YES position ──────────────────────────────────────────────────────
        if let Some(pos) = pos_map.get(&("GboostStrategy".to_string(), target_market.yes_token)) {
            if pos.fill_confirmed_at.is_some() {
                let bid = target_snapshot.yes_bid;
                let profit_pct = if pos.avg_entry > dec!(0) {
                    ((bid - pos.avg_entry) / pos.avg_entry).to_f64().unwrap_or(0.0)
                } else { 0.0 };
                let secs_held = pos.fill_confirmed_at
                    .map(|t| (Utc::now() - t).num_seconds()).unwrap_or(0);

                let exit_params = || OrderParams {
                    token_id: target_market.yes_token,
                    price: bid, shares: pos.shares,
                    fee_bps: target_market.yes_fee_bps as u16,
                    is_neg_risk: target_market.is_neg_risk,
                    market_name: target_market.market_name.clone(),
                    condition_id: target_market.condition_id.clone(),
                    order_type: OrderType::FAK,
                    post_only: false,
                    ghost_mode: dc.ghost_mode,
                };

                if profit_pct >= tp {
                    self.record_training_outcome_on_exit(target_market.yes_token, profit_pct > 0.0);
                    self.mark_post_exit_cooldown(target_market.yes_token);
                    return Ok(StrategySignal::Exit {
                        params: exit_params(),
                        reason: format!("GBoost TP YES: gain={:.2}%", profit_pct * 100.0),
                        exit_pair: false,
                    });
                }
                if secs_held >= config::GBOOST_SL_MIN_HOLD_SECS && profit_pct <= -sl {
                    self.record_training_outcome_on_exit(target_market.yes_token, profit_pct > 0.0);
                    self.mark_post_exit_cooldown_sl(target_market.yes_token);
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
                            self.record_training_outcome_on_exit(target_market.yes_token, profit_pct > 0.0);
                            self.mark_post_exit_cooldown(target_market.yes_token);
                            return Ok(StrategySignal::Exit {
                                params: exit_params(),
                                reason: format!("GBoost SignalRev YES: P(UP)={:.3}", p),
                                exit_pair: false,
                            });
                        } else {
                            tracing::debug!(
                                "🚫 GBoost SignalRev YES suppressed: profit={:.2}% not yet above min {:.0}% (not deep enough in red for protective exit)",
                                profit_pct * 100.0, config::GBOOST_SIGNAL_REV_MIN_PROFIT * 100.0
                            );
                        }
                    }
                }
            }
        }

        // ── NO position ───────────────────────────────────────────────────────
        if let Some(pos) = pos_map.get(&("GboostStrategy".to_string(), target_market.no_token)) {
            if pos.fill_confirmed_at.is_some() {
                let bid = target_snapshot.no_bid;
                let profit_pct = if pos.avg_entry > dec!(0) {
                    ((bid - pos.avg_entry) / pos.avg_entry).to_f64().unwrap_or(0.0)
                } else { 0.0 };
                let secs_held = pos.fill_confirmed_at
                    .map(|t| (Utc::now() - t).num_seconds()).unwrap_or(0);

                let exit_params = || OrderParams {
                    token_id: target_market.no_token,
                    price: bid, shares: pos.shares,
                    fee_bps: target_market.no_fee_bps as u16,
                    is_neg_risk: target_market.is_neg_risk,
                    market_name: target_market.market_name.clone(),
                    condition_id: target_market.condition_id.clone(),
                    order_type: OrderType::FAK,
                    post_only: false,
                    ghost_mode: dc.ghost_mode,
                };

                if profit_pct >= tp {
                    self.record_training_outcome_on_exit(target_market.no_token, profit_pct > 0.0);
                    self.mark_post_exit_cooldown(target_market.no_token);
                    return Ok(StrategySignal::Exit {
                        params: exit_params(),
                        reason: format!("GBoost TP NO: gain={:.2}%", profit_pct * 100.0),
                        exit_pair: false,
                    });
                }
                if secs_held >= config::GBOOST_SL_MIN_HOLD_SECS && profit_pct <= -sl {
                    self.record_training_outcome_on_exit(target_market.no_token, profit_pct > 0.0);
                    self.mark_post_exit_cooldown_sl(target_market.no_token);
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
                            self.record_training_outcome_on_exit(target_market.no_token, profit_pct > 0.0);
                            self.mark_post_exit_cooldown(target_market.no_token);
                            return Ok(StrategySignal::Exit {
                                params: exit_params(),
                                reason: format!("GBoost SignalRev NO: P(UP)={:.3}", p),
                                exit_pair: false,
                            });
                        } else {
                            tracing::debug!(
                                "🚫 GBoost SignalRev NO suppressed: profit={:.2}% not yet above min {:.0}% (not deep enough in red for protective exit)",
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
    // use alloy::primitives::U256; // Already imported by the main file

    fn make_snapshot() -> MarketSnapshot {
        MarketSnapshot {
            yes_bid: dec!(0.50), yes_bid_depth: dec!(200),
            yes_ask: dec!(0.52), yes_ask_depth: dec!(150),
            no_bid:  dec!(0.48), no_bid_depth:  dec!(180),
            no_ask:  dec!(0.50), no_ask_depth:  dec!(160),
            oracle_price: dec!(95000),
            velocity: dec!(50), velocity_1s: dec!(10), acceleration: dec!(5),
            funding_rate: dec!(0.0001), oracle_drift_60m: dec!(100),
            oracle_drift_10m: dec!(30), // ~10min drift for test
            secs_to_expiry: 3600, // 1 hour — mid-range for tests
            timestamp: Utc::now(),
        }
    }

    fn make_ctx() -> StrategyContext {
        StrategyContext {
            market: MarketConfig {
                yes_token: U256::from(1u64), no_token: U256::from(2u64),
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
        }
    }

    #[test]
    fn extract_features_ranges() {
        let snap = make_snapshot();
        let feats = extract_features(&snap, None, 0.0, 0.0); // Pass None for prev_s in test
        assert_eq!(feats.len(), NUM_FEATURES);
        assert!(feats[0].abs() <= 1.0, "yes_obi out of [-1,1]: {}", feats[0]);
        assert!(feats[1].abs() <= 1.0, "no_obi  out of [-1,1]: {}", feats[1]);
        // oracle normalised: 95000 / 100000 = 0.95
        assert!((feats[11] - 0.95).abs() < 0.01, "oracle_price feat: {}", feats[11]);
        // secs_to_expiry_norm: 3600 / 14400 = 0.25
        assert!((feats[12] - 0.25).abs() < 0.01, "secs_to_expiry_norm feat: {}", feats[12]);
        assert!(feats[12] >= 0.0 && feats[12] <= 1.0, "secs_to_expiry_norm out of [0,1]: {}", feats[12]);
        // new features [19-21] should be in their normalised ranges
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
        let booster = train_model(samples).expect("train_model should succeed");
        assert!(!booster.trees.is_empty(), "booster should have trees after training");
    }

    #[tokio::test]
    async fn no_signal_without_model() {
        let strategy = GboostStrategyImpl::new();
        let signal = strategy.evaluate_entry(&make_ctx()).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn evaluates_with_trained_model() {
        let strategy = GboostStrategyImpl::new();
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
        *strategy.model.lock().unwrap() = Some(train_model(samples).unwrap());
        // Must not panic — signal depends on the dummy snapshot's feature values.
        let _ = strategy.evaluate_entry(&make_ctx()).await.unwrap();
        let _ = strategy.evaluate_exit(&make_ctx()).await.unwrap();
    }

    #[tokio::test]
    async fn pending_entry_is_kept_when_no_exit_signal() {
        let strategy = GboostStrategyImpl::new();
        let ctx = make_ctx();

        strategy.pending_entries.lock().unwrap().insert(
            ctx.market.yes_token,
            (ctx.snapshot.clone(), None, dec!(0.50), 0.0, 0.0),
        );

        {
            let mut map = ctx.positions.lock().await;
            map.insert(
                ("GboostStrategy".to_string(), ctx.market.yes_token),
                Position {
                    shares: dec!(10),
                    avg_entry: dec!(0.52),
                    opened_at: Utc::now(),
                    close_time: ctx.market.market_close_time,
                    market_name: ctx.market.market_name.clone(),
                    pair_token_id: ctx.market.yes_token,
                    fill_confirmed_at: Some(Utc::now()),
                    paired_leg_token_id: None,
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
        let strategy = GboostStrategyImpl::new();
        let ctx = make_ctx();

        strategy.pending_entries.lock().unwrap().insert(
            ctx.market.yes_token,
            (ctx.snapshot.clone(), None, dec!(0.40), 0.0, 0.0),
        );

        {
            let mut map = ctx.positions.lock().await;
            map.insert(
                ("GboostStrategy".to_string(), ctx.market.yes_token),
                Position {
                    shares: dec!(10),
                    avg_entry: dec!(0.40),
                    opened_at: Utc::now(),
                    close_time: ctx.market.market_close_time,
                    market_name: ctx.market.market_name.clone(),
                    pair_token_id: ctx.market.yes_token,
                    fill_confirmed_at: Some(Utc::now()),
                    paired_leg_token_id: None,
                },
            );
        }

        let signal = strategy.evaluate_exit(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::Exit { reason, .. } if reason.contains("GBoost TP YES")));
        assert_eq!(strategy.pending_entries.lock().unwrap().len(), 0);
        assert_eq!(strategy.training_data.lock().unwrap().len(), 1);
    }
}
