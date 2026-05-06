/// GBoost Strategy — Online gradient-boosted binary classification
///
/// Uses the `perpetual` crate's `PerpetualBooster` (LogLoss objective) to predict
/// near-term YES price direction from a rolling window of orderbook + oracle features.
///
/// ── Feature Vector (NUM_FEATURES = 13) ──────────────────────────────────────
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
///                             Teaches the model that binary market microstructure
///                             (gamma, adverse selection, spread) changes dramatically
///                             near expiry — entries and exits should be calibrated
///                             differently depending on time horizon.
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
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use std::collections::{VecDeque, HashMap}; // Added HashMap
use std::sync::{Arc, Mutex as StdMutex};
use std::sync::atomic::{AtomicBool, Ordering};
use chrono::Utc;
use perpetual::{Matrix, PerpetualBooster};
use perpetual::objective::Objective;
use perpetual::booster::config::BoosterIO;
use alloy::primitives::U256; // For token_id in pending_entries

use crate::config;
use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{MarketSnapshot, OrderParams, StrategySignal, StrategyStatus};
use crate::strategies::is_drawdown_limit_hit;
use crate::helpers::price::floor_to_tick_size;
use polymarket_client_sdk_v2::clob::types::OrderType; // Import OrderType

/// Number of f64 features per snapshot row fed into the booster.
const NUM_FEATURES: usize = 13;

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

/// Convert a `MarketSnapshot` into a fixed-length `f64` feature array.
fn extract_features(s: &MarketSnapshot) -> [f64; NUM_FEATURES] {
    let yes_total = s.yes_bid_depth + s.yes_ask_depth;
    let no_total  = s.no_bid_depth  + s.no_ask_depth;

    let yes_obi = if yes_total > dec!(0) {
        ((s.yes_bid_depth - s.yes_ask_depth) / yes_total).to_f64().unwrap_or(0.0)
    } else { 0.0 };

    let no_obi = if no_total > dec!(0) {
        ((s.no_bid_depth - s.no_ask_depth) / no_total).to_f64().unwrap_or(0.0)
    } else { 0.0 };

    // Normalise secs_to_expiry to [0.0, 1.0]:
    //   0.0 = market has expired / about to expire
    //   1.0 = 4 hours or more until expiry (fully safe zone)
    let secs_to_expiry_norm = (s.secs_to_expiry.max(0) as f64)
        .min(SECS_TO_EXPIRY_NORM)
        / SECS_TO_EXPIRY_NORM;

    [
        yes_obi,
        no_obi,
        s.yes_ask.to_f64().unwrap_or(0.5),
        s.no_ask.to_f64().unwrap_or(0.5),
        (s.yes_ask - s.yes_bid).to_f64().unwrap_or(0.0),
        (s.no_ask  - s.no_bid ).to_f64().unwrap_or(0.0),
        s.velocity.to_f64().unwrap_or(0.0)          / 1_000.0,
        s.velocity_1s.to_f64().unwrap_or(0.0)       / 1_000.0,
        s.acceleration.to_f64().unwrap_or(0.0)      / 1_000.0,
        s.funding_rate.to_f64().unwrap_or(0.0),
        s.oracle_drift_60m.to_f64().unwrap_or(0.0)  / 10_000.0,
        s.oracle_price.to_f64().unwrap_or(70_000.0) / 100_000.0,
        secs_to_expiry_norm,
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
        .set_max_bin(63);       // 63 bins is fast and sufficient for these features

    booster.fit(&matrix, &labels, None, None)
        .map_err(|e| anyhow::anyhow!("perpetual fit error: {:?}", e))?;

    Ok(booster)
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
    /// Stores entry snapshots and prices for trades that are currently open (ghost mode).
    pending_entries: Arc<StdMutex<HashMap<U256, (MarketSnapshot, rust_decimal::Decimal)>>>,
    /// Per-token timestamp of the last emitted exit signal to prevent rapid re-entry churn.
    post_exit_cooldowns: Arc<StdMutex<HashMap<U256, chrono::DateTime<chrono::Utc>>>>,
}

impl GboostStrategyImpl {
    pub fn new() -> Self {
        let model_arc = Arc::new(StdMutex::new(None::<PerpetualBooster>));

        // Warm-start: try to load a previously persisted model from disk.
        let model_clone = Arc::clone(&model_arc);
        tokio::spawn(async move {
            match tokio::fs::read_to_string(config::GBOOST_MODEL_PATH).await {
                Ok(json) => match PerpetualBooster::from_json(&json) {
                    Ok(loaded) => {
                        let n = loaded.trees.len();
                        *model_clone.lock().unwrap() = Some(loaded);
                        tracing::info!(
                            "🤖 GboostStrategy: loaded persisted model from {} ({} trees)",
                            config::GBOOST_MODEL_PATH, n
                        );
                    }
                    Err(e) => tracing::warn!(
                        "🤖 GboostStrategy: model parse failed (will train from scratch): {:?}", e
                    ),
                },
                Err(_) => tracing::info!(
                    "🤖 GboostStrategy: no persisted model at {} — collecting data to train",
                    config::GBOOST_MODEL_PATH
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
                VecDeque::with_capacity(config::GBOOST_HISTORY_BUFFER_SIZE) // Use similar capacity
            )),
            pending_entries: Arc::new(StdMutex::new(HashMap::new())),
            post_exit_cooldowns: Arc::new(StdMutex::new(HashMap::new())),
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

        let triggered = {
            let mut t = self.ticks_since_retrain.lock().unwrap();
            *t += 1;
            *t >= config::GBOOST_RETRAIN_EVERY_N
        };
        if !triggered { return; }

        // ── Source 1: real trade outcomes ────────────────────────────────────
        let trade_samples_count = {
            let td = self.training_data.lock().unwrap();
            td.len()
        };

        let training_samples: Vec<TrainingSample> = if trade_samples_count >= config::GBOOST_MIN_TRAINING_SAMPLES {
            let td = self.training_data.lock().unwrap();
            td.iter().cloned().collect()
        } else {
            // ── Source 2: lookahead labels from history ring buffer ───────────
            // Generates supervised labels without needing real trade outcomes.
            // Label = 1.0 if the YES bid rises GBOOST_LOOKAHEAD_TICKS ticks later.
            let h = self.history.lock().unwrap();
            let n = h.len();
            let lookahead = config::GBOOST_LOOKAHEAD_TICKS;
            if n < config::GBOOST_MIN_TRAINING_SAMPLES + lookahead {
                return; // Not enough history yet, wait for more ticks
            }
            let usable = n - lookahead;
            (0..usable).map(|i| {
                let snap   = &h[i];
                let future = &h[i + lookahead];
                TrainingSample {
                    features: extract_features(snap),
                    is_profitable: future.yes_bid > snap.yes_bid,
                    entry_timestamp: snap.timestamp,
                }
            }).collect()
        };

        if training_samples.len() < config::GBOOST_MIN_TRAINING_SAMPLES { return; }

        *self.ticks_since_retrain.lock().unwrap() = 0;
        self.is_training.store(true, Ordering::Relaxed);

        let model_arc   = Arc::clone(&self.model);
        let is_training = Arc::clone(&self.is_training);

        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || train_model(training_samples)).await;

            match result {
                Ok(Ok(new_model)) => {
                    let n = new_model.trees.len();
                    // Persist to disk first so a crash doesn't lose the trained weights.
                    if let Ok(json) = new_model.json_dump() {
                        if let Err(e) = tokio::fs::write(config::GBOOST_MODEL_PATH, &json).await {
                            tracing::warn!("🤖 GboostStrategy: model save failed: {}", e);
                        }
                    }

                    // Reject degenerate models — a model with fewer than
                    // GBOOST_MIN_USABLE_TREES trees is essentially a random stump.
                    // Keep the previous (better) model rather than regressing.
                    if n < config::GBOOST_MIN_USABLE_TREES {
                        tracing::warn!(
                            "🤖 GboostStrategy: retrain produced degenerate model ({} trees < min {}), keeping previous",
                            n, config::GBOOST_MIN_USABLE_TREES
                        );
                        is_training.store(false, Ordering::Relaxed);
                        return;
                    }

                    let old_tree_count = {
                        let old_model = model_arc.lock().unwrap();
                        old_model.as_ref().map(|m| m.trees.len()).unwrap_or(0)
                    };

                    if n > old_tree_count + 5 || (old_tree_count >= 5 && n + 5 < old_tree_count) || (old_tree_count == 0 && n > 0) {
                        tracing::info!("🤖 GboostStrategy: retrained — {} trees (was {})", n, old_tree_count);
                    } else {
                        tracing::debug!("🤖 GboostStrategy: retrained — {} trees", n);
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
        let feats = extract_features(snap);
        // Stack-allocated array; Matrix borrows it for the duration of this call only.
        let matrix = Matrix::new(&feats, 1, NUM_FEATURES);
        booster.predict_proba(&matrix, false, false).first().copied()
    }

    /// Persist one supervised label only when an exit signal is emitted.
    /// This avoids training on transient mark-to-market states from non-exit ticks.
    fn record_training_outcome_on_exit(&self, token_id: U256, is_profitable: bool) {
        let mut pending_entries_guard = self.pending_entries.lock().unwrap();
        if let Some((entry_snap, _entry_price)) = pending_entries_guard.remove(&token_id) {
            let training_sample = TrainingSample {
                features: extract_features(&entry_snap),
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

    /// Mark token as cooling down after an emitted exit signal.
    fn mark_post_exit_cooldown(&self, token_id: U256) {
        let mut guard = self.post_exit_cooldowns.lock().unwrap();
        guard.insert(token_id, Utc::now());
    }

    /// Returns remaining cooldown seconds for this token, if still cooling down.
    fn post_exit_cooldown_remaining_secs(&self, token_id: U256) -> Option<i64> {
        let now = Utc::now();
        let mut guard = self.post_exit_cooldowns.lock().unwrap();
        if let Some(ts) = guard.get(&token_id).copied() {
            let elapsed = (now - ts).num_seconds();
            let remaining = config::GBOOST_POST_EXIT_COOLDOWN_SECS - elapsed;
            if remaining > 0 {
                return Some(remaining);
            }
            guard.remove(&token_id);
        }
        None
    }

    /// Compute side-specific orderbook imbalance (OBI) in [-1, 1].
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
            dec!(0)
        }
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

        if let Some(close_time) = target_market.market_close_time {
            if (close_time - Utc::now()).num_seconds() < 90 {
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

        let entry_thresh = dc.gboost_entry_threshold.to_f64().unwrap_or(0.65);
        let trade_usdc   = dc.gboost_max_exposure_usdc;

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
            let price  = floor_to_tick_size(target_snapshot.yes_ask);
            if price >= config::GBOOST_MAX_ENTRY_PRICE
                || price < config::GBOOST_MIN_ENTRY_PRICE
                || price <= dec!(0)
            {
                return Ok(StrategySignal::NoSignal);
            }
            let shares = trade_usdc / price;
            tracing::info!(
                "🔮 GBoost YES entry: P(UP)={:.3} | ask=${:.4} shares={:.2}",
                p_yes_up, price, shares
            );
            // Store entry context for training feedback
            self.pending_entries.lock().unwrap().insert(
                target_market.yes_token,
                (target_snapshot.clone(), price)
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
            let price  = floor_to_tick_size(target_snapshot.no_ask);
            if price >= config::GBOOST_MAX_ENTRY_PRICE
                || price < config::GBOOST_MIN_ENTRY_PRICE
                || price <= dec!(0)
            {
                return Ok(StrategySignal::NoSignal);
            }
            let shares = trade_usdc / price;
            tracing::info!(
                "🔮 GBoost NO entry: P(UP)={:.3} | ask=${:.4} shares={:.2}",
                p_yes_up, price, shares
            );
            self.pending_entries.lock().unwrap().insert(
                target_market.no_token,
                (target_snapshot.clone(), price)
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
                    self.mark_post_exit_cooldown(target_market.yes_token);
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
                        self.record_training_outcome_on_exit(target_market.yes_token, profit_pct > 0.0);
                        self.mark_post_exit_cooldown(target_market.yes_token);
                        return Ok(StrategySignal::Exit {
                            params: exit_params(),
                            reason: format!("GBoost SignalRev YES: P(UP)={:.3}", p),
                            exit_pair: false,
                        });
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
                    self.mark_post_exit_cooldown(target_market.no_token);
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
                        self.record_training_outcome_on_exit(target_market.no_token, profit_pct > 0.0);
                        self.mark_post_exit_cooldown(target_market.no_token);
                        return Ok(StrategySignal::Exit {
                            params: exit_params(),
                            reason: format!("GBoost SignalRev NO: P(UP)={:.3}", p),
                            exit_pair: false,
                        });
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
            maker_market: None, maker_snapshot: None,
            dynamic_config: Arc::new(Default::default()),
        }
    }

    #[test]
    fn extract_features_ranges() {
        let snap = make_snapshot();
        let feats = extract_features(&snap);
        assert_eq!(feats.len(), NUM_FEATURES);
        assert!(feats[0].abs() <= 1.0, "yes_obi out of [-1,1]: {}", feats[0]);
        assert!(feats[1].abs() <= 1.0, "no_obi  out of [-1,1]: {}", feats[1]);
        // oracle normalised: 95000 / 100000 = 0.95
        assert!((feats[11] - 0.95).abs() < 0.01, "oracle_price feat: {}", feats[11]);
        // secs_to_expiry_norm: 3600 / 14400 = 0.25
        assert!((feats[12] - 0.25).abs() < 0.01, "secs_to_expiry_norm feat: {}", feats[12]);
        assert!(feats[12] >= 0.0 && feats[12] <= 1.0, "secs_to_expiry_norm out of [0,1]: {}", feats[12]);
    }

    #[test]
    fn train_model_returns_booster() {
        // This test needs to be updated to use TrainingSample
        let n = config::GBOOST_MIN_TRAINING_SAMPLES + 10; // No lookahead needed
        let mut samples: Vec<TrainingSample> = Vec::with_capacity(n);
        for i in 0..n {
            let snap = make_snapshot(); // Dummy snapshot
            samples.push(TrainingSample {
                features: extract_features(&snap),
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
                features: extract_features(&snap),
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
            (ctx.snapshot.clone(), dec!(0.50)),
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
            (ctx.snapshot.clone(), dec!(0.40)),
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
