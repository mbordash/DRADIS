/// GBoost Strategy — Online gradient-boosted binary classification
///
/// Uses the `perpetual` crate's `PerpetualBooster` (LogLoss objective) to predict
/// near-term YES price direction from a rolling window of orderbook + oracle features.
///
/// ── Feature Vector (NUM_FEATURES = 12) ──────────────────────────────────────
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
use std::collections::VecDeque;
use std::sync::{Arc, Mutex as StdMutex};
use std::sync::atomic::{AtomicBool, Ordering};
use chrono::Utc;
use perpetual::{Matrix, PerpetualBooster};
use perpetual::objective::Objective;
use perpetual::booster::config::BoosterIO;

use crate::config;
use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{MarketSnapshot, OrderParams, StrategySignal, StrategyStatus};
use crate::strategies::is_drawdown_limit_hit;
use crate::helpers::price::floor_to_tick_size;
use polymarket_client_sdk_v2::clob::types::OrderType; // Import OrderType

/// Number of f64 features per snapshot row fed into the booster.
const NUM_FEATURES: usize = 12;

// ── Feature extraction ────────────────────────────────────────────────────────

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
    ]
}

// ── Training helper (runs inside spawn_blocking) ──────────────────────────────

/// Build and train a fresh `PerpetualBooster` from a snapshot slice.
/// Called exclusively from `tokio::task::spawn_blocking` — never on an async thread.
fn train_model(snapshots: Vec<MarketSnapshot>) -> Result<PerpetualBooster> {
    let lookahead = config::GBOOST_LOOKAHEAD_TICKS;
    let n = snapshots.len();
    if n <= lookahead {
        return Err(anyhow::anyhow!(
            "GBoost: too few snapshots ({}) for labels (need > {})", n, lookahead
        ));
    }

    let labeled_n = n - lookahead;
    let mut feature_data: Vec<f64> = Vec::with_capacity(labeled_n * NUM_FEATURES);
    let mut labels: Vec<f64>       = Vec::with_capacity(labeled_n);

    for i in 0..labeled_n {
        feature_data.extend_from_slice(&extract_features(&snapshots[i]));
        // Binary label: 1 if YES bid rose over the lookahead window, else 0.
        let label = if snapshots[i + lookahead].yes_bid > snapshots[i].yes_bid { 1.0 } else { 0.0 };
        labels.push(label);
    }

    // Matrix<'a, T> borrows the slice; both Vec and Matrix live in this closure scope.
    let matrix = Matrix::new(&feature_data, labeled_n, NUM_FEATURES);

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
    fn maybe_retrain(&self) {
        if self.is_training.load(Ordering::Relaxed) { return; }

        let triggered = {
            let mut t = self.ticks_since_retrain.lock().unwrap();
            *t += 1;
            *t >= config::GBOOST_RETRAIN_EVERY_N
        };
        if !triggered { return; }

        let snapshots: Vec<MarketSnapshot> = {
            let h = self.history.lock().unwrap();
            if h.len() < config::GBOOST_MIN_TRAINING_SAMPLES { return; }
            h.iter().cloned().collect()
        };

        *self.ticks_since_retrain.lock().unwrap() = 0;
        self.is_training.store(true, Ordering::Relaxed);

        let model_arc   = Arc::clone(&self.model);
        let is_training = Arc::clone(&self.is_training);

        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || train_model(snapshots)).await;

            match result {
                Ok(Ok(new_model)) => {
                    let n = new_model.trees.len();
                    // Persist to disk first so a crash doesn't lose the trained weights.
                    if let Ok(json) = new_model.json_dump() {
                        if let Err(e) = tokio::fs::write(config::GBOOST_MODEL_PATH, &json).await {
                            tracing::warn!("🤖 GboostStrategy: model save failed: {}", e);
                        }
                    }

                    // Get old tree count before moving the lock
                    let old_tree_count = {
                        let old_model = model_arc.lock().unwrap();
                        old_model.as_ref().map(|m| m.trees.len()).unwrap_or(0)
                    };

                    // Log based on the comparison - fixed overflow issue
                    if n > old_tree_count + 5 || (old_tree_count >= 5 && n < old_tree_count - 5) || (old_tree_count == 0 && n > 0) {
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
        let feats = extract_features(snap);
        // Stack-allocated array; Matrix borrows it for the duration of this call only.
        let matrix = Matrix::new(&feats, 1, NUM_FEATURES);
        booster.predict_proba(&matrix, false, false).first().copied()
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
        self.push_snapshot(ctx.snapshot.clone());
        self.maybe_retrain();

        if !config::ENABLE_GBOOST_TRADING {
            return Ok(StrategySignal::NoSignal);
        }
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate: market must be mature enough for orderbook features to be stable ──
        if (Utc::now() - ctx.market_started_at).num_seconds() < config::GBOOST_MIN_MARKET_AGE_SECS {
            return Ok(StrategySignal::NoSignal);
        }
        // ── Gate: expiry guard ────────────────────────────────────────────────
        if let Some(close_time) = ctx.market.market_close_time {
            if (close_time - Utc::now()).num_seconds() < 90 {
                return Ok(StrategySignal::NoSignal);
            }
        }
        // ── Gate: sufficient collateral ───────────────────────────────────────
        if ctx.available_collateral < config::GBOOST_MAX_EXPOSURE_USDC {
            return Ok(StrategySignal::NoSignal);
        }

        let p_yes_up = match self.predict(&ctx.snapshot) {
            Some(p) => p,
            None    => return Ok(StrategySignal::NoSignal),
        };

        tracing::info!("🔮 GBoost prediction: P(UP)={:.3}", p_yes_up);

        let entry_thresh = config::GBOOST_ENTRY_THRESHOLD.to_f64().unwrap_or(0.65);
        let trade_usdc   = config::GBOOST_MAX_EXPOSURE_USDC;

        // Don't pyramid — check that no position is already open for this strategy.
        let (has_yes, has_no) = {
            let map = ctx.positions.lock().await;
            (
                map.contains_key(&("GboostStrategy".to_string(), ctx.market.yes_token)),
                map.contains_key(&("GboostStrategy".to_string(), ctx.market.no_token)),
            )
        };

        // ── YES entry: model predicts UP ──────────────────────────────────────
        if p_yes_up >= entry_thresh && !has_yes {
            let price  = floor_to_tick_size(ctx.snapshot.yes_ask);
            if price >= config::GBOOST_MAX_ENTRY_PRICE || price <= dec!(0) { return Ok(StrategySignal::NoSignal); }
            let shares = trade_usdc / price;
            tracing::info!(
                "🔮 GBoost YES entry: P(UP)={:.3} | ask=${:.4} shares={:.2}",
                p_yes_up, price, shares
            );
            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: ctx.market.yes_token,
                    price, shares,
                    fee_bps:     ctx.market.yes_fee_bps as u16,
                    is_neg_risk: ctx.market.is_neg_risk,
                    market_name: ctx.market.market_name.clone(),
                    condition_id: ctx.market.condition_id.clone(),
                    order_type: OrderType::FAK, // GBoost entries are typically FAK
                    post_only: false, // Not post-only
                },
                pair_params: None,
            });
        }

        // ── NO entry: model predicts DOWN (P(UP) is very low) ────────────────
        if p_yes_up <= (1.0 - entry_thresh) && !has_no {
            let price  = floor_to_tick_size(ctx.snapshot.no_ask);
            if price >= config::GBOOST_MAX_ENTRY_PRICE || price <= dec!(0) { return Ok(StrategySignal::NoSignal); }
            let shares = trade_usdc / price;
            tracing::info!(
                "🔮 GBoost NO entry: P(UP)={:.3} | ask=${:.4} shares={:.2}",
                p_yes_up, price, shares
            );
            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: ctx.market.no_token,
                    price, shares,
                    fee_bps:     ctx.market.no_fee_bps as u16,
                    is_neg_risk: ctx.market.is_neg_risk,
                    market_name: ctx.market.market_name.clone(),
                    condition_id: ctx.market.condition_id.clone(),
                    order_type: OrderType::FAK, // GBoost entries are typically FAK
                    post_only: false, // Not post-only
                },
                pair_params: None,
            });
        }

        Ok(StrategySignal::NoSignal)
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let p_yes_up           = self.predict(&ctx.snapshot);
        let signal_exit_thresh = config::GBOOST_SIGNAL_EXIT_THRESHOLD.to_f64().unwrap_or(0.40);
        let tp                 = config::GBOOST_TARGET_PROFIT_PERCENT.to_f64().unwrap_or(0.15);
        let sl                 = config::GBOOST_STOP_LOSS_PERCENT.to_f64().unwrap_or(0.10);

        let pos_map = ctx.positions.lock().await;

        // ── YES position ──────────────────────────────────────────────────────
        if let Some(pos) = pos_map.get(&("GboostStrategy".to_string(), ctx.market.yes_token)) {
            if pos.fill_confirmed_at.is_some() {
                let bid = ctx.snapshot.yes_bid;
                let profit_pct = if pos.avg_entry > dec!(0) {
                    ((bid - pos.avg_entry) / pos.avg_entry).to_f64().unwrap_or(0.0)
                } else { 0.0 };
                let secs_held = pos.fill_confirmed_at
                    .map(|t| (Utc::now() - t).num_seconds()).unwrap_or(0);

                let exit_params = || OrderParams {
                    token_id: ctx.market.yes_token,
                    price: bid, shares: pos.shares,
                    fee_bps: ctx.market.yes_fee_bps as u16,
                    is_neg_risk: ctx.market.is_neg_risk,
                    market_name: ctx.market.market_name.clone(),
                    condition_id: ctx.market.condition_id.clone(),
                    order_type: OrderType::FAK, // Exit orders are always FAK
                    post_only: false, // Exit orders are never post-only
                };

                if profit_pct >= tp {
                    return Ok(StrategySignal::Exit {
                        params: exit_params(),
                        reason: format!("GBoost TP YES: gain={:.2}%", profit_pct * 100.0),
                        exit_pair: false,
                    });
                }
                if secs_held >= config::GBOOST_MIN_HOLD_SECS && profit_pct <= -sl {
                    return Ok(StrategySignal::Exit {
                        params: exit_params(),
                        reason: format!("GBoost SL YES: loss={:.2}% ({}s)", profit_pct * 100.0, secs_held),
                        exit_pair: false,
                    });
                }
                // Signal reversal: model now strongly predicts DOWN while we are long YES.
                if let Some(p) = p_yes_up {
                    if p <= signal_exit_thresh {
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
        if let Some(pos) = pos_map.get(&("GboostStrategy".to_string(), ctx.market.no_token)) {
            if pos.fill_confirmed_at.is_some() {
                let bid = ctx.snapshot.no_bid;
                let profit_pct = if pos.avg_entry > dec!(0) {
                    ((bid - pos.avg_entry) / pos.avg_entry).to_f64().unwrap_or(0.0)
                } else { 0.0 };
                let secs_held = pos.fill_confirmed_at
                    .map(|t| (Utc::now() - t).num_seconds()).unwrap_or(0);

                let exit_params = || OrderParams {
                    token_id: ctx.market.no_token,
                    price: bid, shares: pos.shares,
                    fee_bps: ctx.market.no_fee_bps as u16,
                    is_neg_risk: ctx.market.is_neg_risk,
                    market_name: ctx.market.market_name.clone(),
                    condition_id: ctx.market.condition_id.clone(),
                    order_type: OrderType::FAK, // Exit orders are always FAK
                    post_only: false, // Exit orders are never post-only
                };

                if profit_pct >= tp {
                    return Ok(StrategySignal::Exit {
                        params: exit_params(),
                        reason: format!("GBoost TP NO: gain={:.2}%", profit_pct * 100.0),
                        exit_pair: false,
                    });
                }
                if secs_held >= config::GBOOST_MIN_HOLD_SECS && profit_pct <= -sl {
                    return Ok(StrategySignal::Exit {
                        params: exit_params(),
                        reason: format!("GBoost SL NO: loss={:.2}% ({}s)", profit_pct * 100.0, secs_held),
                        exit_pair: false,
                    });
                }
                // Signal reversal for NO: model now strongly predicts UP.
                if let Some(p) = p_yes_up {
                    if p >= (1.0 - signal_exit_thresh) {
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
    fn venue(&self) -> &'static str { "Hourly" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::GBOOST_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Gross one-sided" }

    fn status(&self) -> StrategyStatus {
        if config::ENABLE_GBOOST_TRADING { StrategyStatus::Active } else { StrategyStatus::Disabled }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use crate::state::{MarketConfig, PositionMap};
    use alloy::primitives::U256;

    fn make_snapshot() -> MarketSnapshot {
        MarketSnapshot {
            yes_bid: dec!(0.50), yes_bid_depth: dec!(200),
            yes_ask: dec!(0.52), yes_ask_depth: dec!(150),
            no_bid:  dec!(0.48), no_bid_depth:  dec!(180),
            no_ask:  dec!(0.50), no_ask_depth:  dec!(160),
            oracle_price: dec!(95000),
            velocity: dec!(50), velocity_1s: dec!(10), acceleration: dec!(5),
            funding_rate: dec!(0.0001), oracle_drift_60m: dec!(100),
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
    }

    #[test]
    fn train_model_returns_booster() {
        let n = config::GBOOST_MIN_TRAINING_SAMPLES + config::GBOOST_LOOKAHEAD_TICKS + 10;
        let mut snaps: Vec<MarketSnapshot> = Vec::with_capacity(n);
        let mut bid = dec!(0.50);
        for i in 0..n {
            let mut s = make_snapshot();
            bid += if i % 2 == 0 { dec!(0.01) } else { dec!(-0.01) };
            s.yes_bid = bid;
            snaps.push(s);
        }
        let booster = train_model(snaps).expect("train_model should succeed");
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
        let n = config::GBOOST_MIN_TRAINING_SAMPLES + config::GBOOST_LOOKAHEAD_TICKS + 10;
        let mut snaps: Vec<MarketSnapshot> = Vec::with_capacity(n);
        let mut bid = dec!(0.50);
        for i in 0..n {
            let mut s = make_snapshot();
            bid += if i % 2 == 0 { dec!(0.01) } else { dec!(-0.01) };
            s.yes_bid = bid;
            snaps.push(s);
        }
        *strategy.model.lock().unwrap() = Some(train_model(snaps).unwrap());
        // Must not panic — signal depends on the dummy snapshot's feature values.
        let _ = strategy.evaluate_entry(&make_ctx()).await.unwrap();
        let _ = strategy.evaluate_exit(&make_ctx()).await.unwrap();
    }
}
