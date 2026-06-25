/// DynamicConfig — runtime-tunable strategy parameters.
///
/// All values that operators commonly need to change between sessions
/// (position sizes, thresholds, enable flags, stop-loss %) live here.
/// On first startup the struct is seeded from the compile-time defaults in
/// config.rs and written to SQLite.  Subsequent startups load from SQLite.
///
/// ── Hot-Reload Flow ─────────────────────────────────────────────────────────
///   1. Control Tower UI sends  `PATCH /api/config  { "time_decay_stop_loss_pct": "0.03" }`
///   2. axum handler deserializes the patch, calls `config.apply_patch(&json)`
///   3. apply_patch merges, persists to SQLite, then sends the new Arc<DynamicConfig>
///      on the `watch::Sender<Arc<DynamicConfig>>` held by the API server
///   4. main.rs tick loop calls `config_rx.borrow().clone()` every 50ms — strategies
///      always read the freshest snapshot via `ctx.dynamic_config.*`
///
/// ── What stays in config.rs ─────────────────────────────────────────────────
///   Compile-time constants that are infrastructure, not tuning:
///   - API endpoints, exchange addresses
///   - Timing constants (cooldowns, retry intervals, watchdog)
///   - Order minimums (MIN_ORDER_SHARES, MIN_ORDER_USDC)
///   - Flash-exit timing, fee formulas
///
/// ── Config change audit log ──────────────────────────────────────────────────
///   Every call to `save()` or `apply_patch()` appends a row to `config_history`
///   in SQLite with:
///     - `session_id`  — which process start made the change
///     - `changed_by`  — "startup_default" | "operator" | "llm_advisor"
///     - `old_value`   — the previous JSON snapshot (NULL on first write)
///     - `new_value`   — the new JSON snapshot
///   This lets developers reconstruct the exact config active during any trade.

use serde::{Serialize, Deserialize};
use rust_decimal::Decimal;
use anyhow::Result;
use tracing::{info, warn};
use std::sync::Arc;

use crate::config;
use crate::helpers::db;

// ── serde default helpers ────────────────────────────────────────────────────
// Required when adding new fields to DynamicConfig: old DB rows that were
// serialized before the field existed will have it missing.  Without a default,
// serde returns a deserialization error and load_or_default resets to factory
// defaults — clobbering any operator customisation made in the previous session.
fn default_arb_max_leg_price()             -> Decimal { config::ARBITRAGE_MAX_LEG_PRICE             }
fn default_arb_max_leg_obi()               -> Decimal { config::ARBITRAGE_MAX_LEG_OBI               }
fn default_arb_max_obi_asymmetry()         -> Decimal { config::ARBITRAGE_MAX_OBI_ASYMMETRY         }
fn default_arb_fak_rehedge_buffer()        -> Decimal { config::ARB_FAK_REHEDGE_BUFFER              }
fn default_arb_max_rescue_cost()           -> Decimal { config::ARB_MAX_RESCUE_COST                 }
fn default_trendcapture_enable()           -> bool    { config::ENABLE_TRENDCAPTURE_TRADING          }
fn default_trendcapture_min_trade_size()   -> Decimal { config::TRENDCAPTURE_MIN_TRADE_SIZE_USDC     }
fn default_trendcapture_max_trade_size()   -> Decimal { config::TRENDCAPTURE_MAX_TRADE_SIZE_USDC     }
fn default_trendcapture_max_exposure()     -> Decimal { config::TRENDCAPTURE_MAX_EXPOSURE_USDC       }
fn default_trendcapture_stop_loss()        -> Decimal { config::TRENDCAPTURE_STOP_LOSS_PERCENT       }
fn default_trendcapture_target_profit()    -> Decimal { config::TRENDCAPTURE_TARGET_PROFIT_PERCENT   }
fn default_trendcapture_max_entry_price()  -> Decimal { config::TRENDCAPTURE_MAX_ENTRY_PRICE         }

fn default_convergence_enable()            -> bool    { config::ENABLE_CONVERGENCE_TRADING            }
fn default_convergence_position_size()     -> Decimal { config::CONVERGENCE_POSITION_SIZE_USDC        }
fn default_convergence_max_exposure()      -> Decimal { config::CONVERGENCE_MAX_EXPOSURE_USDC         }
fn default_convergence_stop_loss()         -> Decimal { config::CONVERGENCE_STOP_LOSS_PERCENT         }
fn default_convergence_target_profit()     -> Decimal { config::CONVERGENCE_TARGET_PROFIT_PERCENT     }
fn default_convergence_max_entry_price()   -> Decimal { config::CONVERGENCE_MAX_ENTRY_PRICE           }

// ─── Struct ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicConfig {
    // ── Global ────────────────────────────────────────────────────────────────
    /// When true all orders are simulated — no real CLOB calls.
    pub ghost_mode: bool,

    // ── Viper (strategy) enable flags ─────────────────────────────────────────
    pub enable_arbitrage:     bool,
    pub enable_time_decay:    bool,
    pub enable_momentum:      bool,
    pub enable_maker:         bool,
    pub enable_basis:         bool,
    pub enable_gboost:        bool,
    #[serde(default = "default_trendcapture_enable")]
    pub enable_trendcapture:  bool,

    // ── Arbitrage Viper ───────────────────────────────────────────────────────
    pub arbitrage_position_size_usdc: Decimal,
    pub arbitrage_max_exposure_usdc:  Decimal,
    pub arbitrage_profit_threshold:   Decimal,
    /// Max gap (ask − safe_bid) allowed on each leg before skipping entry.
    /// Prevents one-sided fills when the other side of the book is far away.
    pub arbitrage_max_fill_gap:       Decimal,
    /// LEGACY — hard price cap (0.60) used when order-book depth is unavailable.
    /// Superseded by `arbitrage_max_leg_obi` for live sessions.
    /// Kept in the struct for backward-compatible deserialization of old DB rows.
    #[serde(default = "default_arb_max_leg_price")]
    pub arbitrage_max_leg_price:      Decimal,
    /// Maximum order-book imbalance (OBI) on either leg before skipping entry.
    /// OBI = (bid_depth − ask_depth) / total_depth.  High positive OBI on a leg
    /// means few sellers exist → GTC bid unlikely to fill → one-sided orphan risk.
    /// Falls back to price-cap check when depth data is unavailable (depth = 0).
    /// Default 0.50 ≈ 3:1 bid/ask depth ratio ≈ >60% directional market.
    #[serde(default = "default_arb_max_leg_obi")]
    pub arbitrage_max_leg_obi:        Decimal,

    /// Max allowed |YES_OBI − NO_OBI| before skipping a paired arb entry.
    /// Blocks asymmetric books (one leg seller-heavy, the other buyer-heavy) that
    /// fill one leg alone and leave a naked orphan. Lower = stricter. Default 0.60.
    #[serde(default = "default_arb_max_obi_asymmetry")]
    pub arbitrage_max_obi_asymmetry:  Decimal,

    /// Breakeven buffer subtracted from the $1.00 payout when deciding whether to
    /// FAK re-hedge a naked arb leg. Per-squadron so thin alt books (ETH/SOL) can
    /// carry a larger taker-fee/adverse-price cushion than deep BTC books.
    #[serde(default = "default_arb_fak_rehedge_buffer")]
    pub arb_fak_rehedge_buffer:       Decimal,
    /// Upper bound on single-leg orphan RESCUE cost in the arb entry gate. Entry is
    /// blocked only when a single-leg fill would be materially unrecoverable
    /// (rescue ≥ this). Per-squadron so alts can demand a tighter bound than BTC.
    #[serde(default = "default_arb_max_rescue_cost")]
    pub arb_max_rescue_cost:          Decimal,

    // ── TimeDecay Viper ───────────────────────────────────────────────────────
    pub time_decay_position_size_usdc:  Decimal,
    pub time_decay_max_exposure_usdc:   Decimal,
    pub time_decay_stop_loss_pct:       Decimal,
    pub time_decay_max_entry_price:     Decimal,
    pub time_decay_min_entry_price:     Decimal,
    pub time_decay_obi_adverse_block:   Decimal,
    pub time_decay_convergence_exit_bid: Decimal,
    pub time_decay_min_secs_to_expiry:  i64,
    pub time_decay_max_secs_to_expiry:  i64,
    pub min_time_decay_net_profit:      Decimal,

    // ── Momentum Viper ────────────────────────────────────────────────────────
    pub momentum_min_trade_size_usdc:  Decimal,
    pub momentum_max_trade_size_usdc:  Decimal,
    pub momentum_stop_loss_pct:        Decimal,
    pub momentum_target_profit_pct:    Decimal,
    pub momentum_max_exposure_usdc:    Decimal,

    // ── Maker Viper ───────────────────────────────────────────────────────────
    pub maker_max_entry_price:    Decimal,
    pub maker_min_entry_price:    Decimal,
    pub maker_stop_loss_pct:      Decimal,
    pub maker_target_profit_pct:  Decimal,
    pub maker_max_exposure_usdc:  Decimal,

    // ── Basis Viper ───────────────────────────────────────────────────────────
    pub basis_max_exposure_usdc:  Decimal,
    pub basis_stop_loss_pct:      Decimal,
    pub basis_target_profit_pct:  Decimal,

    // ── GBoost Viper ──────────────────────────────────────────────────────────
    pub gboost_entry_threshold:   Decimal,
    pub gboost_stop_loss_pct:     Decimal,
    pub gboost_target_profit_pct: Decimal,
    pub gboost_max_exposure_usdc: Decimal,

    // ── TrendCapture Viper ────────────────────────────────────────────────────
    #[serde(default = "default_trendcapture_min_trade_size")]
    pub trendcapture_min_trade_size_usdc: Decimal,
    #[serde(default = "default_trendcapture_max_trade_size")]
    pub trendcapture_max_trade_size_usdc: Decimal,
    #[serde(default = "default_trendcapture_max_exposure")]
    pub trendcapture_max_exposure_usdc:   Decimal,
    #[serde(default = "default_trendcapture_stop_loss")]
    pub trendcapture_stop_loss_pct:       Decimal,
    #[serde(default = "default_trendcapture_target_profit")]
    pub trendcapture_target_profit_pct:   Decimal,
    #[serde(default = "default_trendcapture_max_entry_price")]
    pub trendcapture_max_entry_price:     Decimal,

    // ── Convergence Viper ─────────────────────────────────────────────────────
    #[serde(default = "default_convergence_enable")]
    pub enable_convergence:               bool,
    #[serde(default = "default_convergence_position_size")]
    pub convergence_position_size_usdc:   Decimal,
    #[serde(default = "default_convergence_max_exposure")]
    pub convergence_max_exposure_usdc:    Decimal,
    #[serde(default = "default_convergence_stop_loss")]
    pub convergence_stop_loss_pct:        Decimal,
    #[serde(default = "default_convergence_target_profit")]
    pub convergence_target_profit_pct:    Decimal,
    #[serde(default = "default_convergence_max_entry_price")]
    pub convergence_max_entry_price:      Decimal,
}

impl Default for DynamicConfig {
    /// Seeds all values from the compile-time defaults in config.rs.
    /// This is the definitive single source of truth for initial values —
    /// the SQLite row is only authoritative once the user has changed something.
    fn default() -> Self {
        Self {
            ghost_mode: config::GHOST_MODE,

            enable_arbitrage:     config::ENABLE_ARBITRAGE_TRADING,
            enable_time_decay:    config::ENABLE_TIME_DECAY_TRADING,
            enable_momentum:      config::ENABLE_MOMENTUM_TRADING,
            enable_maker:         config::ENABLE_MAKER_TRADING,
            enable_basis:         config::ENABLE_BASIS_TRADING,
            enable_gboost:        config::ENABLE_GBOOST_TRADING,
            enable_trendcapture:  config::ENABLE_TRENDCAPTURE_TRADING,

            arbitrage_position_size_usdc: config::ARBITRAGE_POSITION_SIZE_USDC,
            arbitrage_max_exposure_usdc:  config::ARBITRAGE_MAX_EXPOSURE_USDC,
            arbitrage_profit_threshold:   config::ARBITRAGE_PROFIT_THRESHOLD,
            arbitrage_max_fill_gap:       config::ARBITRAGE_MAX_FILL_GAP,
            arbitrage_max_leg_price:      config::ARBITRAGE_MAX_LEG_PRICE,
            arbitrage_max_leg_obi:        config::ARBITRAGE_MAX_LEG_OBI,
            arbitrage_max_obi_asymmetry:  config::ARBITRAGE_MAX_OBI_ASYMMETRY,
            arb_fak_rehedge_buffer:       config::ARB_FAK_REHEDGE_BUFFER,
            arb_max_rescue_cost:          config::ARB_MAX_RESCUE_COST,

            time_decay_position_size_usdc:  config::TIME_DECAY_POSITION_SIZE_USDC,
            time_decay_max_exposure_usdc:   config::TIME_DECAY_MAX_EXPOSURE_USDC,
            time_decay_stop_loss_pct:       config::TIME_DECAY_STOP_LOSS_PERCENT,
            time_decay_max_entry_price:     config::TIME_DECAY_MAX_ENTRY_PRICE,
            time_decay_min_entry_price:     config::TIME_DECAY_MIN_ENTRY_PRICE,
            time_decay_obi_adverse_block:   config::TIME_DECAY_OBI_ADVERSE_BLOCK,
            time_decay_convergence_exit_bid: config::TIME_DECAY_CONVERGENCE_EXIT_BID,
            time_decay_min_secs_to_expiry:  config::TIME_DECAY_MIN_SECS_TO_EXPIRY,
            time_decay_max_secs_to_expiry:  config::TIME_DECAY_MAX_SECS_TO_EXPIRY,
            min_time_decay_net_profit:      config::MIN_TIME_DECAY_NET_PROFIT,

            momentum_min_trade_size_usdc:  config::MOMENTUM_MIN_TRADE_SIZE_USDC,
            momentum_max_trade_size_usdc:  config::MOMENTUM_MAX_TRADE_SIZE_USDC,
            momentum_stop_loss_pct:        config::MOMENTUM_STOP_LOSS_PERCENT,
            momentum_target_profit_pct:    config::MOMENTUM_TARGET_PROFIT_PERCENT,
            momentum_max_exposure_usdc:    config::MOMENTUM_MAX_EXPOSURE_USDC,

            maker_max_entry_price:    config::MAKER_MAX_ENTRY_PRICE,
            maker_min_entry_price:    config::MAKER_MIN_ENTRY_PRICE,
            maker_stop_loss_pct:      config::MAKER_STOP_LOSS_PERCENT,
            maker_target_profit_pct:  config::MAKER_TARGET_PROFIT_PERCENT,
            maker_max_exposure_usdc:  config::MAKER_MAX_EXPOSURE_USDC,

            basis_max_exposure_usdc:  config::BASIS_MAX_EXPOSURE_USDC,
            basis_stop_loss_pct:      config::BASIS_STOP_LOSS_PERCENT,
            basis_target_profit_pct:  config::BASIS_TARGET_PROFIT_PERCENT,

            gboost_entry_threshold:   config::GBOOST_ENTRY_THRESHOLD,
            gboost_stop_loss_pct:     config::GBOOST_STOP_LOSS_PERCENT,
            gboost_target_profit_pct: config::GBOOST_TARGET_PROFIT_PERCENT,
            gboost_max_exposure_usdc: config::GBOOST_MAX_EXPOSURE_USDC,

            trendcapture_min_trade_size_usdc: config::TRENDCAPTURE_MIN_TRADE_SIZE_USDC,
            trendcapture_max_trade_size_usdc: config::TRENDCAPTURE_MAX_TRADE_SIZE_USDC,
            trendcapture_max_exposure_usdc:   config::TRENDCAPTURE_MAX_EXPOSURE_USDC,
            trendcapture_stop_loss_pct:       config::TRENDCAPTURE_STOP_LOSS_PERCENT,
            trendcapture_target_profit_pct:   config::TRENDCAPTURE_TARGET_PROFIT_PERCENT,
            trendcapture_max_entry_price:     config::TRENDCAPTURE_MAX_ENTRY_PRICE,

            enable_convergence:               config::ENABLE_CONVERGENCE_TRADING,
            convergence_position_size_usdc:   config::CONVERGENCE_POSITION_SIZE_USDC,
            convergence_max_exposure_usdc:    config::CONVERGENCE_MAX_EXPOSURE_USDC,
            convergence_stop_loss_pct:        config::CONVERGENCE_STOP_LOSS_PERCENT,
            convergence_target_profit_pct:    config::CONVERGENCE_TARGET_PROFIT_PERCENT,
            convergence_max_entry_price:      config::CONVERGENCE_MAX_ENTRY_PRICE,
        }
    }
}

// ─── SQLite key ──────────────────────────────────────────────────────────────

const DB_KEY: &str = "dynamic_config";

impl DynamicConfig {
    /// Load the most recent DynamicConfig from SQLite.
    /// If no record exists (first run), seeds defaults and writes them to DB.
    pub async fn load_or_default() -> Arc<Self> {
        if let Some(pool) = db::pool() {
            if let Some(json) = db::config_get(pool, DB_KEY).await {
                match serde_json::from_str::<DynamicConfig>(&json) {
                    Ok(mut cfg) => {
                        // ── Safety floor enforcement ─────────────────────────────────
                        // Compile-time constants are the hard limits.  A stale DB row can
                        // never override a tightened constant — code fixes take effect
                        // immediately on the next startup without a manual DB reset.
                        //
                        // Rule: "stricter wins" — for upper bounds use .min(), for lower
                        // bounds (OBI block is negative) the code already uses .max(db, config).
                        cfg.time_decay_max_entry_price = cfg.time_decay_max_entry_price
                            .min(config::TIME_DECAY_MAX_ENTRY_PRICE);
                        cfg.time_decay_stop_loss_pct = cfg.time_decay_stop_loss_pct
                            .min(config::TIME_DECAY_STOP_LOSS_PERCENT);

                        // Momentum SL safety floor: a stale DB row (e.g. from when
                        // MOMENTUM_STOP_LOSS_PERCENT was 8%) must never override a
                        // code-tightened constant.  Root cause of 2026-06-01 13:39 loss
                        // (-$0.6122): DB had 0.08 persisted while config.rs was 0.05 —
                        // no safety floor let the old value survive, causing exits at
                        // -8% instead of -5%.
                        cfg.momentum_stop_loss_pct = cfg.momentum_stop_loss_pct
                            .min(config::MOMENTUM_STOP_LOSS_PERCENT);

                        info!("⚙️  DynamicConfig loaded from SQLite (safety floors applied)");

                        // Record startup load in config_history so developers can see
                        // exactly what DynamicConfig was active at the start of every session.
                        // Tagged 'startup_dynamic' to distinguish from the compile-time
                        // 'startup_static' snapshot taken immediately before this.
                        if let Ok(new_json) = serde_json::to_string(&cfg) {
                            db::record_config_change(
                                pool,
                                "startup_dynamic",
                                "session_start_snapshot",
                                None,   // no "previous" — this is the session anchor
                                &new_json,
                            ).await;
                        }

                        return Arc::new(cfg);
                    }
                    Err(e) => {
                        warn!("⚠️  DynamicConfig parse error: {} — resetting to defaults", e);
                    }
                }
            } else {
                info!("⚙️  No DynamicConfig in DB — using compile-time defaults");
            }
        }
        let cfg = Arc::new(DynamicConfig::default());
        cfg.save_as("startup_dynamic").await;
        cfg
    }

    /// Persist current values as a JSON blob under DB_KEY.
    /// Also appends to config_history with the provided `changed_by` provenance tag.
    async fn save_as(&self, changed_by: &str) {
        if let Some(pool) = db::pool() {
            match serde_json::to_string(self) {
                Ok(new_json) => {
                    // Read old value before overwriting so the diff is recorded.
                    let old_json = db::config_get(pool, DB_KEY).await;
                    db::config_set(pool, DB_KEY, &new_json).await;
                    db::record_config_change(
                        pool,
                        changed_by,
                        "full_snapshot",
                        old_json.as_deref(),
                        &new_json,
                    ).await;
                }
                Err(e) => warn!("⚠️  DynamicConfig serialize error: {}", e),
            }
        }
    }

    /// Persist current values as a JSON blob under DB_KEY.
    /// Convenience alias with "operator" provenance for direct calls.
    pub async fn save(&self) {
        self.save_as("operator").await;
    }

    /// Apply a partial JSON patch (e.g. `{"time_decay_stop_loss_pct":"0.03"}`),
    /// persist the merged result, and return it wrapped in Arc.
    ///
    /// Called by the Control Tower API on `PATCH /api/config`.
    /// The watch::Sender should then broadcast the returned Arc so all in-flight
    /// tick contexts pick up the new values on the next 50ms interval.
    pub async fn apply_patch(current: &Arc<Self>, patch_json: &str) -> Result<Arc<Self>> {
        let mut value = serde_json::to_value(current.as_ref())?;
        let patch: serde_json::Value = serde_json::from_str(patch_json)?;

        // Merge: patch fields overwrite current fields; unknown keys are ignored.
        if let (Some(obj), Some(patch_obj)) = (value.as_object_mut(), patch.as_object()) {
            for (k, v) in patch_obj {
                obj.insert(k.clone(), v.clone());
            }
        }

        let updated: DynamicConfig = serde_json::from_value(value)?;
        updated.save_as("operator").await;
        info!("⚙️  DynamicConfig hot-patched and persisted");
        Ok(Arc::new(updated))
    }

    // ── Squadron-scoped config methods ─────────────────────────────────────────

    /// Load a squadron's config from the squadron_configs table.
    /// If none exists, returns a fresh copy of compile-time defaults (does NOT persist yet).
    /// Caller is responsible for persisting via save_for_squadron() if needed.
    pub async fn load_for_squadron(squadron_id: &str) -> Arc<Self> {
        if let Some(pool) = db::pool() {
            if let Some(json) = db::squadron_config_get(pool, squadron_id).await {
                match serde_json::from_str::<DynamicConfig>(&json) {
                    Ok(mut cfg) => {
                        // Apply safety floors (same as global config)
                        cfg.time_decay_max_entry_price = cfg.time_decay_max_entry_price
                            .min(config::TIME_DECAY_MAX_ENTRY_PRICE);
                        cfg.time_decay_stop_loss_pct = cfg.time_decay_stop_loss_pct
                            .min(config::TIME_DECAY_STOP_LOSS_PERCENT);
                        cfg.momentum_stop_loss_pct = cfg.momentum_stop_loss_pct
                            .min(config::MOMENTUM_STOP_LOSS_PERCENT);

                        info!("⚙️  Squadron config loaded from DB: {}", squadron_id);
                        return Arc::new(cfg);
                    }
                    Err(e) => {
                        warn!("⚠️  Squadron config parse error [{}]: {} — using defaults", squadron_id, e);
                    }
                }
            }
        }
        // No existing config → return defaults (caller decides whether to persist)
        Arc::new(DynamicConfig::default())
    }

    /// Initialize a squadron's config by copying compile-time defaults to its DB row.
    /// Call this when deploying a new squadron.
    pub async fn init_for_squadron(squadron_id: &str) -> Arc<Self> {
        let cfg = Arc::new(DynamicConfig::default());
        cfg.save_for_squadron(squadron_id).await;
        info!("⚙️  Squadron config initialized: {}", squadron_id);
        cfg
    }

    /// Load a squadron's persisted config, seeding compile-time defaults **only**
    /// if no row exists yet.
    ///
    /// Unlike [`init_for_squadron`], this never clobbers operator edits made via
    /// the Control Tower. Startup/rotation paths must use this so a disabled
    /// viper (or any tuned param) survives a process restart and hourly market
    /// rotation instead of silently reverting to defaults.
    pub async fn load_or_init_for_squadron(squadron_id: &str) -> Arc<Self> {
        if let Some(pool) = db::pool() {
            if db::squadron_config_get(pool, squadron_id).await.is_some() {
                return Self::load_for_squadron(squadron_id).await;
            }
        }
        Self::init_for_squadron(squadron_id).await
    }

    /// Persist this config for a specific squadron.
    pub async fn save_for_squadron(&self, squadron_id: &str) {
        if let Some(pool) = db::pool() {
            match serde_json::to_string(self) {
                Ok(json) => {
                    db::squadron_config_set(pool, squadron_id, &json).await;
                }
                Err(e) => warn!("⚠️  Squadron config serialize error [{}]: {}", squadron_id, e),
            }
        }
    }

    /// Apply a partial JSON patch to a squadron's config and persist.
    pub async fn apply_squadron_patch(squadron_id: &str, patch_json: &str) -> Result<Arc<Self>> {
        let current = Self::load_for_squadron(squadron_id).await;
        let mut value = serde_json::to_value(current.as_ref())?;
        let patch: serde_json::Value = serde_json::from_str(patch_json)?;

        if let (Some(obj), Some(patch_obj)) = (value.as_object_mut(), patch.as_object()) {
            for (k, v) in patch_obj {
                obj.insert(k.clone(), v.clone());
            }
        }

        let updated: DynamicConfig = serde_json::from_value(value)?;
        updated.save_for_squadron(squadron_id).await;
        info!("⚙️  Squadron config hot-patched: {}", squadron_id);
        Ok(Arc::new(updated))
    }
}

