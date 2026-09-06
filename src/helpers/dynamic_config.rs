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
use std::sync::{Arc, RwLock, Mutex, OnceLock};
use std::collections::HashMap;

use crate::config;
use crate::helpers::db;

/// Registry of the LIVE, in-memory config handle for each running squadron,
/// keyed by squadron id.  Each squadron's patrol loop reads its config every
/// tick from an `Arc<RwLock<DynamicConfig>>` seeded at deploy.  A squadron-scoped
/// PATCH persists to the DB, but the running loop never re-reads the DB except on
/// market rotation — so without this registry a live edit (Min Spread, viper
/// enable/disable, etc.) would not take effect until the next hourly rotation.
///
/// `register_squadron_config_handle` records the same `Arc` the patrol loop holds,
/// and `apply_squadron_patch` writes the merged config straight into it so edits
/// apply on the next tick.
static SQUADRON_CONFIG_REGISTRY: OnceLock<Mutex<HashMap<String, Arc<RwLock<DynamicConfig>>>>> =
    OnceLock::new();

fn squadron_config_registry() -> &'static Mutex<HashMap<String, Arc<RwLock<DynamicConfig>>>> {
    SQUADRON_CONFIG_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The GLOBAL config broadcast sender, registered once by `run_api_server`.
/// Lets routes outside the ApiState graph (e.g. the Setup profile picker)
/// hot-apply a global config change: read current via `.borrow()`, merge,
/// persist, then `.send()` so all strategy tick loops pick it up within 50 ms.
static GLOBAL_CONFIG_TX: OnceLock<Arc<tokio::sync::watch::Sender<Arc<DynamicConfig>>>> =
    OnceLock::new();

/// Register the global config broadcast sender (idempotent; first wins).
pub fn register_global_config_tx(tx: Arc<tokio::sync::watch::Sender<Arc<DynamicConfig>>>) {
    let _ = GLOBAL_CONFIG_TX.set(tx);
}

/// The registered global config sender.
///
/// Registered by `main` at the moment the channel is created, so this is
/// populated for the whole of startup — including venue code that decides
/// whether it is simulating before any server is listening.
pub fn global_config_tx() -> Option<&'static Arc<tokio::sync::watch::Sender<Arc<DynamicConfig>>>> {
    GLOBAL_CONFIG_TX.get()
}

/// Is the engine forbidden from touching the exchange right now?
///
/// Combines the BUILD-level `config::GHOST_MODE` with the operator's runtime
/// switch. For code inside the patrol tick, prefer the `ghosting` value hoisted
/// there — it reads the same tick's config snapshot. This exists for the paths
/// that run OUTSIDE a tick (background tasks, shutdown) and have no snapshot to
/// read from.
///
/// Fails safe: if the global handle is not up yet, only the compile-time switch
/// applies, which is the same answer the code gave before the runtime switch was
/// honored at all.
pub fn ghosting_now() -> bool {
    if crate::config::GHOST_MODE { return true; }
    global_config_tx().map(|tx| tx.borrow().ghost_mode).unwrap_or(false)
}

/// Should the intl order-book feed fold `price_change` updates into its local
/// book between full snapshots (B36)? Read by the per-token WebSocket tasks on
/// every `price_change`, so flipping the Control Tower knob takes effect on
/// the next message without a resubscribe. Before the global handle is up, the
/// build's own default applies.
pub fn book_price_changes_enabled() -> bool {
    global_config_tx()
        .map(|tx| tx.borrow().book_apply_price_changes)
        .unwrap_or(crate::config::BOOK_APPLY_PRICE_CHANGES)
}

/// Register (or replace) the live config handle a running squadron's patrol loop
/// reads each tick.  Called once per squadron deploy / market rotation.
pub fn register_squadron_config_handle(squadron_id: &str, handle: Arc<RwLock<DynamicConfig>>) {
    if let Ok(mut reg) = squadron_config_registry().lock() {
        reg.insert(squadron_id.to_string(), handle);
    }
}

/// Squadron IDs that currently hold a live config handle — i.e. the squadrons the
/// CAG actually has deployed right now.  This is the correct scope for any
/// fleet-wide config apply (see the Setup risk-profile picker).
///
/// Deliberately NOT derived from the `squadron_configs` table: that table also
/// retains a row per historical market rotation (916 dead `<asset>-hourly-<ts>`
/// rows as of 2026-08), and writing to those would revive long-dead config and
/// make the config-history diff unreadable.  The registry only ever contains
/// squadrons a patrol loop is reading from, so it is both the smallest and the
/// only meaningful target set.
pub fn registered_squadron_ids() -> Vec<String> {
    match squadron_config_registry().lock() {
        Ok(reg) => {
            let mut ids: Vec<String> = reg.keys().cloned().collect();
            ids.sort();
            ids
        }
        Err(_) => {
            warn!("⚠️  Squadron config registry poisoned — cannot enumerate deployed squadrons");
            Vec::new()
        }
    }
}

// ── serde default helpers ────────────────────────────────────────────────────
// Required when adding new fields to DynamicConfig: old DB rows that were
// serialized before the field existed will have it missing.  Without a default,
// serde returns a deserialization error and load_or_default resets to factory
// defaults — clobbering any operator customisation made in the previous session.
fn default_arb_max_leg_price()             -> Decimal { config::ARBITRAGE_MAX_LEG_PRICE             }
fn default_arb_max_leg_obi()               -> Decimal { config::ARBITRAGE_MAX_LEG_OBI               }
fn default_deriv_gate_enabled()            -> bool    { config::DERIV_GATE_ENABLED                  }
fn default_deriv_cvd_confirm_margin()      -> Decimal { config::DERIV_CVD_CONFIRM_MARGIN            }
fn default_deriv_oi_unwind_block()         -> Decimal { config::DERIV_OI_UNWIND_BLOCK               }fn default_arb_max_obi_asymmetry()         -> Decimal { config::ARBITRAGE_MAX_OBI_ASYMMETRY         }
fn default_arb_min_leg_conviction()        -> Decimal { config::ARBITRAGE_MIN_LEG_CONVICTION        }
fn default_arb_fak_rehedge_buffer()        -> Decimal { config::ARB_FAK_REHEDGE_BUFFER              }
fn default_arb_settle_grace_secs()         -> u64     { config::ARB_SETTLE_GRACE_SECS               }
fn default_arb_max_rescue_cost()           -> Decimal { config::ARB_MAX_RESCUE_COST                 }
fn default_trendcapture_enable()           -> bool    { config::ENABLE_TRENDCAPTURE_TRADING          }
fn default_trendcapture_min_trade_size()   -> Decimal { config::TRENDCAPTURE_MIN_TRADE_SIZE_USDC     }
fn default_trendcapture_max_trade_size()   -> Decimal { config::TRENDCAPTURE_MAX_TRADE_SIZE_USDC     }
fn default_trendcapture_max_exposure()     -> Decimal { config::TRENDCAPTURE_MAX_EXPOSURE_USDC       }
fn default_trendcapture_stop_loss()        -> Decimal { config::TRENDCAPTURE_STOP_LOSS_PERCENT       }
fn default_trendcapture_target_profit()    -> Decimal { config::TRENDCAPTURE_TARGET_PROFIT_PERCENT   }
fn default_trendcapture_max_entry_price()  -> Decimal { config::TRENDCAPTURE_MAX_ENTRY_PRICE         }

fn default_convergence_enable()            -> bool    { config::ENABLE_CONVERGENCE_TRADING            }
fn default_fairvalue_enable()              -> bool    { config::ENABLE_FAIRVALUE_TRADING              }
fn default_fairvalue_trade_size()          -> Decimal { config::FAIRVALUE_TRADE_SIZE_USDC             }
fn default_fairvalue_max_exposure()        -> Decimal { config::FAIRVALUE_MAX_EXPOSURE_USDC           }
fn default_fairvalue_base_edge()           -> Decimal { config::FAIRVALUE_BASE_EDGE                   }
fn default_fairvalue_prefer_hourly()       -> bool    { config::FAIRVALUE_PREFER_HOURLY               }
fn default_fairvalue_min_edge()            -> Decimal { config::FAIRVALUE_MIN_EDGE                    }
fn default_fairvalue_min_entry_price()     -> Decimal { config::FAIRVALUE_MIN_ENTRY_PRICE             }
fn default_fairvalue_max_entry_price()     -> Decimal { config::FAIRVALUE_MAX_ENTRY_PRICE             }
fn default_fairvalue_target_profit()       -> Decimal { config::FAIRVALUE_TARGET_PROFIT_PERCENT       }
fn default_fairvalue_stop_loss()           -> Decimal { config::FAIRVALUE_STOP_LOSS_PERCENT           }
fn default_fairvalue_reversal_decay()      -> Decimal { config::FAIRVALUE_MODEL_REVERSAL_DECAY_PCT    }
fn default_fairvalue_sigma_floor_horizon() -> i64     { config::FAIRVALUE_SIGMA_FLOOR_HORIZON_SECS    }
fn default_fairvalue_post_exit_cooldown()  -> i64     { config::FAIRVALUE_POST_EXIT_COOLDOWN_SECS     }
fn default_fairvalue_max_stop_losses()     -> u32     { config::FAIRVALUE_MAX_STOP_LOSSES_PER_MARKET  }
fn default_fairvalue_edge_noise_multiple() -> Decimal { config::FAIRVALUE_EDGE_NOISE_MULTIPLE         }
fn default_fairvalue_stop_model_confirm() -> Decimal { config::FAIRVALUE_STOP_MODEL_CONFIRM_FRAC      }
fn default_intl_taker_fee_rate()           -> Decimal { config::INTL_TAKER_FEE_RATE                   }
fn default_convergence_position_size()     -> Decimal { config::CONVERGENCE_POSITION_SIZE_USDC        }
fn default_convergence_max_exposure()      -> Decimal { config::CONVERGENCE_MAX_EXPOSURE_USDC         }
fn default_convergence_stop_loss()         -> Decimal { config::CONVERGENCE_STOP_LOSS_PERCENT         }
fn default_convergence_target_profit()     -> Decimal { config::CONVERGENCE_TARGET_PROFIT_PERCENT     }
fn default_convergence_max_entry_price()   -> Decimal { config::CONVERGENCE_MAX_ENTRY_PRICE           }

// ── Newly-exposed advanced knobs (previously compile-time only) ───────────────
fn default_basis_max_entry_price()          -> Decimal { config::BASIS_MAX_ENTRY_PRICE                 }
fn default_basis_min_trade_size_usdc()      -> Decimal { config::BASIS_MIN_TRADE_SIZE_USDC             }
fn default_basis_max_trade_size_usdc()      -> Decimal { config::BASIS_MAX_TRADE_SIZE_USDC             }
fn default_basis_entry_skew_threshold()     -> Decimal { config::BASIS_ENTRY_SKEW_THRESHOLD            }
fn default_basis_skew_collapse_threshold()  -> Decimal { config::BASIS_SKEW_COLLAPSE_THRESHOLD         }
fn default_basis_catastrophic_sl_pct()      -> Decimal { config::BASIS_CATASTROPHIC_SL_PCT             }
fn default_basis_min_secs_to_expiry()       -> i64     { config::BASIS_MIN_SECS_TO_EXPIRY              }
fn default_basis_max_spread_pct()           -> Decimal { config::BASIS_MAX_SPREAD_PCT                  }
fn default_basis_loss_lockout_count()       -> i64     { config::BASIS_LOSS_LOCKOUT_COUNT              }
fn default_basis_loss_lockout_secs()        -> i64     { config::BASIS_LOSS_LOCKOUT_SECS               }
fn default_basis_extreme_skew_bypass()      -> bool    { config::BASIS_EXTREME_SKEW_BYPASS             }

fn default_convergence_min_entry_price()    -> Decimal { config::CONVERGENCE_MIN_ENTRY_PRICE           }
fn default_convergence_pulse_threshold()    -> Decimal { config::CONVERGENCE_PULSE_THRESHOLD           }
fn default_convergence_coherence_min()      -> Decimal { config::CONVERGENCE_COHERENCE_MIN             }
fn default_convergence_cvd_confirm_margin() -> Decimal { config::CONVERGENCE_CVD_CONFIRM_MARGIN        }
fn default_convergence_max_token_spread_pct() -> Decimal { config::CONVERGENCE_MAX_TOKEN_SPREAD_PCT    }
fn default_convergence_obi_adverse_block()  -> Decimal { config::CONVERGENCE_OBI_ADVERSE_BLOCK         }
fn default_convergence_drift_coherence_deadband_pct() -> Decimal { config::CONVERGENCE_DRIFT_COHERENCE_DEADBAND_PCT }
fn default_convergence_velocity_opposition_pct()      -> Decimal { config::CONVERGENCE_VELOCITY_OPPOSITION_PCT      }
fn default_convergence_skip_band_low()      -> Decimal { config::CONVERGENCE_SKIP_BAND_LOW             }
fn default_convergence_skip_band_high()     -> Decimal { config::CONVERGENCE_SKIP_BAND_HIGH            }
fn default_fairvalue_obi_adverse_block()    -> Decimal { config::FAIRVALUE_OBI_ADVERSE_BLOCK           }
fn default_fairvalue_obi_clear_secs()       -> u64     { config::FAIRVALUE_OBI_CLEAR_SECS              }
fn default_sports_poll_secs()               -> u64     { config::SPORTS_POLL_SECS                      }
fn default_sports_low_budget_warn()         -> i64     { config::SPORTS_ODDS_LOW_BUDGET_WARN           }
fn default_tennis_poll_secs()               -> u64     { config::TENNIS_POLL_SECS                      }
fn default_tennis_low_budget_warn()         -> i64     { config::TENNIS_LOW_BUDGET_WARN                }
fn default_sports_odds_sport()              -> String  { config::SPORTS_ODDS_SPORT.to_string()         }
fn default_sports_odds_regions()            -> String  { config::SPORTS_ODDS_REGIONS.to_string()       }
fn default_tennis_tour()                    -> String  { config::TENNIS_TOUR.to_string()               }

fn default_deploy_max_days_to_close()       -> u32     { config::DEPLOY_MAX_DAYS_TO_CLOSE             }
fn default_llm_max_output_tokens()          -> u32     { config::LLM_MAX_OUTPUT_TOKENS                }
fn default_auto_deploy_politics()           -> bool    { config::AUTO_DEPLOY_POLITICS                 }
fn default_auto_deploy_sports()             -> bool    { config::AUTO_DEPLOY_SPORTS                   }
fn default_event_market_retire_grace_secs() -> i64     { config::EVENT_MARKET_RETIRE_GRACE_SECS       }
fn default_collateral_sweep_enabled()       -> bool    { config::COLLATERAL_SWEEP_ENABLED             }
fn default_collateral_sweep_min_usdc()      -> Decimal { config::COLLATERAL_SWEEP_MIN_USDC            }
fn default_gboost_budget()                  -> Decimal { config::GBOOST_BUDGET                       }
fn default_gboost_iteration_limit()         -> u32     { config::GBOOST_ITERATION_LIMIT               }
fn default_position_quote_ttl_secs()        -> u64     { config::POSITION_QUOTE_TTL_SECS              }
fn default_obi_use_whole_book()             -> bool    { config::OBI_USE_WHOLE_BOOK                   }
fn default_book_apply_price_changes()      -> bool    { config::BOOK_APPLY_PRICE_CHANGES             }
fn default_maker_min_spread()               -> Decimal { config::MAKER_MIN_SPREAD                      }
fn default_maker_bid_buffer()               -> Decimal { config::MAKER_BID_BUFFER                      }
fn default_maker_cross_buffer()             -> Decimal { config::MAKER_CROSS_BUFFER                    }
fn default_maker_improve_bid_only()          -> bool    { config::MAKER_IMPROVE_BID_ONLY }
fn default_maker_quote_size_usdc()          -> Decimal { config::MAKER_QUOTE_SIZE_USDC                 }
fn default_maker_max_combined_bid()         -> Decimal { config::MAKER_MAX_COMBINED_BID                }
fn default_maker_max_complementary_price()  -> Decimal { config::MAKER_MAX_COMPLEMENTARY_PRICE         }
fn default_maker_max_book_imbalance_ratio() -> Decimal { config::MAKER_MAX_BOOK_IMBALANCE_RATIO        }
fn default_maker_min_secs_to_expiry()       -> i64     { config::MAKER_MIN_SECS_TO_EXPIRY              }
fn default_maker_min_market_age_secs()      -> i64     { config::MAKER_MIN_MARKET_AGE_SECS             }
fn default_maker_maturation_max_fraction()  -> Decimal { config::MAKER_MATURATION_MAX_FRACTION         }
fn default_maker_toxic_flow_exit_obi()      -> Decimal { config::MAKER_TOXIC_FLOW_EXIT_OBI             }
fn default_maker_toxic_reentry_cooldown_secs() -> i64  { config::MAKER_TOXIC_REENTRY_COOLDOWN_SECS     }
fn default_maker_toxic_min_hold_secs()      -> i64     { config::MAKER_TOXIC_MIN_HOLD_SECS            }
fn default_maker_toxic_min_adverse_pct()    -> Decimal { config::MAKER_TOXIC_MIN_ADVERSE_PCT          }
fn default_maker_toxic_obi_confirm_ticks()  -> u32     { config::MAKER_TOXIC_OBI_CONFIRM_TICKS        }
fn default_maker_oracle_drift_pull_frac()   -> Decimal { config::MAKER_ORACLE_DRIFT_PULL_FRAC         }
fn default_maker_oracle_drift_exit_frac()   -> Decimal { config::MAKER_ORACLE_DRIFT_EXIT_FRAC         }
fn default_maker_resting_exit_enabled()     -> bool    { config::MAKER_RESTING_EXIT_ENABLED           }
fn default_exit_reconcile_max_deviation()   -> Decimal { config::EXIT_RECONCILE_MAX_DEVIATION        }
fn default_exit_retry_cooldown_secs()       -> u64     { config::EXIT_RETRY_COOLDOWN_SECS              }
fn default_ghost_mode()                     -> bool    { config::GHOST_MODE_DEFAULT                 }
fn default_maker_resting_exit_min_edge_pct() -> Decimal { config::MAKER_RESTING_EXIT_MIN_EDGE_PCT     }
fn default_maker_resting_exit_ask_improvement_ticks() -> i64 { config::MAKER_RESTING_EXIT_ASK_IMPROVEMENT_TICKS }
fn default_maker_resting_exit_reprice_threshold() -> Decimal { config::MAKER_RESTING_EXIT_REPRICE_THRESHOLD }

fn default_momentum_max_entry_price()       -> Decimal { config::MAX_MOMENTUM_ENTRY_PRICE              }
fn default_momentum_min_entry_price()       -> Decimal { config::MOMENTUM_MIN_ENTRY_PRICE              }
fn default_momentum_threshold_pct()         -> Decimal { config::MOMENTUM_THRESHOLD_PCT                }
fn default_momentum_max_entry_ask_sum()     -> Decimal { config::MOMENTUM_MAX_ENTRY_ASK_SUM            }
fn default_momentum_obi_adverse_block()     -> Decimal { config::MOMENTUM_OBI_ADVERSE_BLOCK            }
fn default_momentum_obi_exhaustion_block()  -> Decimal { config::MOMENTUM_OBI_EXHAUSTION_BLOCK         }
fn default_momentum_take_profit_ceiling()   -> Decimal { config::MOMENTUM_TAKE_PROFIT_CEILING          }
fn default_momentum_catastrophic_sl_pct()   -> Decimal { config::MOMENTUM_CATASTROPHIC_SL_PCT          }
fn default_momentum_min_secs_to_expiry_for_entry() -> i64 { config::MOMENTUM_MIN_SECS_TO_EXPIRY_FOR_ENTRY }
fn default_momentum_obi_exhaust_max_adverse_pct() -> Decimal { config::MOMENTUM_OBI_EXHAUST_MAX_ADVERSE_PCT }
fn default_momentum_obi_exhaust_min_hold_secs()   -> i64     { config::MOMENTUM_OBI_EXHAUST_MIN_HOLD_SECS   }
fn default_momentum_obi_exhaust_persist_secs()    -> i64     { config::MOMENTUM_OBI_EXHAUST_PERSIST_SECS    }
fn default_momentum_tp_fee_margin_mult()          -> Decimal { config::MOMENTUM_TP_FEE_MARGIN_MULT          }
fn default_maker_tp_fee_margin_mult()             -> Decimal { config::MAKER_TP_FEE_MARGIN_MULT             }
fn default_fairvalue_stop_veto_max_model_decay_pct() -> Decimal { config::FAIRVALUE_STOP_VETO_MAX_MODEL_DECAY_PCT }
fn default_fairvalue_settle_snipe_hold()  -> bool    { config::FAIRVALUE_SETTLE_SNIPE_HOLD             }
fn default_fairvalue_resting_tp_enabled() -> bool    { config::FAIRVALUE_RESTING_TP_ENABLED            }

fn default_time_decay_max_fast_velocity_pct()      -> Decimal { config::TIME_DECAY_MAX_FAST_VELOCITY_PCT      }
fn default_time_decay_max_slow_drift_pct()         -> Decimal { config::TIME_DECAY_MAX_SLOW_DRIFT_PCT         }
fn default_time_decay_iv_stop_tighten_multiplier() -> Decimal { config::TIME_DECAY_IV_STOP_TIGHTEN_MULTIPLIER }
fn default_time_decay_min_hold_secs()              -> i64     { config::TIME_DECAY_MIN_HOLD_SECS              }

fn default_gboost_max_yes_entry_price()     -> Decimal { config::GBOOST_MAX_YES_ENTRY_PRICE            }
fn default_gboost_max_no_entry_price()      -> Decimal { config::GBOOST_MAX_NO_ENTRY_PRICE             }
fn default_gboost_min_entry_price()         -> Decimal { config::GBOOST_MIN_ENTRY_PRICE                }
fn default_gboost_obi_adverse_block()       -> Decimal { config::GBOOST_OBI_ADVERSE_BLOCK              }
fn default_gboost_obi_exhaustion_block()    -> Decimal { config::GBOOST_OBI_EXHAUSTION_BLOCK           }
fn default_gboost_min_edge_from_fair()      -> Decimal { config::GBOOST_MIN_EDGE_FROM_FAIR             }
fn default_gboost_min_hist_vol()            -> Decimal { decimal_from_f64(config::GBOOST_MIN_HIST_VOL) }
fn default_gboost_min_net_profit_usdc()     -> Decimal { config::GBOOST_MIN_NET_PROFIT_USDC            }
fn default_gboost_min_secs_to_expiry()      -> i64     { config::GBOOST_MIN_SECS_TO_EXPIRY             }
fn default_gboost_signal_exit_threshold()   -> Decimal { config::GBOOST_SIGNAL_EXIT_THRESHOLD          }
fn default_gboost_concept_drift_threshold() -> Decimal { config::GBOOST_CONCEPT_DRIFT_THRESHOLD        }
fn default_gboost_drift_consecutive_required()  -> i64 { config::GBOOST_DRIFT_CONSECUTIVE_REQUIRED as i64 }
fn default_gboost_drift_stable_clear_required() -> i64 { config::GBOOST_DRIFT_STABLE_CLEAR_REQUIRED as i64 }
fn default_gboost_label_max_age_hours()     -> i64     { config::GBOOST_LABEL_MAX_AGE_HOURS             }
fn default_gboost_shadow_mode()             -> bool    { config::GBOOST_SHADOW_MODE                     }
fn default_gboost_structural_min_trees()    -> i64     { config::GBOOST_STRUCTURAL_MIN_TREES as i64     }
fn default_gboost_holdout_min_skill()       -> Decimal { config::GBOOST_HOLDOUT_MIN_SKILL               }

/// Bridge for knobs whose profile constant is an `f64` (`GBOOST_MIN_HIST_VOL`):
/// every DynamicConfig knob is a `Decimal`, because the Control Tower edits and
/// PATCHes them as strings and the LLM patch path treats a JSON number as an
/// integer field. Rounded so 0.0015f64 becomes 0.0015, not its binary expansion.
/// `tools/generate-profiles.py` sees through this wrapper when it maps the
/// Default impl back to the profile constants.
fn decimal_from_f64(v: f64) -> Decimal {
    Decimal::from_f64_retain(v)
        .map(|d| d.round_dp(6))
        .unwrap_or(Decimal::ZERO)
}

fn default_trendcapture_min_entry_price()      -> Decimal { config::TRENDCAPTURE_MIN_ENTRY_PRICE          }
fn default_trendcapture_max_entry_ask_sum()    -> Decimal { config::TRENDCAPTURE_MAX_ENTRY_ASK_SUM        }
fn default_trendcapture_obi_adverse_block()    -> Decimal { config::TRENDCAPTURE_OBI_ADVERSE_BLOCK        }
fn default_trendcapture_obi_exhaustion_block() -> Decimal { config::TRENDCAPTURE_OBI_EXHAUSTION_BLOCK     }
fn default_trendcapture_max_token_spread_pct() -> Decimal { config::TRENDCAPTURE_MAX_TOKEN_SPREAD_PCT     }
fn default_trendcapture_reversal_drift_pct()   -> Decimal { config::TRENDCAPTURE_REVERSAL_DRIFT_PCT       }
fn default_trendcapture_strike_gap_pct()       -> Decimal { config::TRENDCAPTURE_STRIKE_GAP_PCT           }
fn default_trendcapture_take_profit_ceiling()  -> Decimal { config::TRENDCAPTURE_TAKE_PROFIT_CEILING      }
fn default_trendcapture_catastrophic_sl_pct()  -> Decimal { config::TRENDCAPTURE_CATASTROPHIC_SL_PCT      }
fn default_trendreversal_mode()                -> bool    { config::TRENDREVERSAL_MODE                    }

// ─── Struct ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicConfig {
    // ── Global ────────────────────────────────────────────────────────────────
    /// When true all orders are simulated — no real CLOB calls.
    ///
    /// Defaulted like every other field (repo convention), and the default is
    /// deliberately the SAFE direction: a persisted config that somehow lacks
    /// this key comes back simulating rather than trading.
    #[serde(default = "default_ghost_mode")]
    pub ghost_mode: bool,
    /// Polymarket taker fee rate used to book the real cost of a round trip:
    /// `fee = rate · p · (1 − p) · shares`, charged on entry and exit alike.
    #[serde(default = "default_intl_taker_fee_rate")]
    pub intl_taker_fee_rate: Decimal,

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

    /// Minimum conviction to enter: the dominant leg's bid must be ≥ this.
    /// Restricts arb to DEEP near-settlement markets (one leg ≈0.90+) where both
    /// legs fill reliably, and rejects ≈0.50 coin-flips where a one-tick move
    /// orphans a leg. Core orphan-prevention gate (default 0.80). Higher = stricter.
    #[serde(default = "default_arb_min_leg_conviction")]
    pub arbitrage_min_leg_conviction: Decimal,

    /// Breakeven buffer subtracted from the $1.00 payout when deciding whether to
    /// FAK re-hedge a naked arb leg. Per-squadron so thin alt books (ETH/SOL) can
    /// carry a larger taker-fee/adverse-price cushion than deep BTC books.
    #[serde(default = "default_arb_fak_rehedge_buffer")]
    pub arb_fak_rehedge_buffer:       Decimal,
    /// Seconds the orphan arbiter waits after cancelling the missing leg's GTC
    /// before re-reading its balance and committing to a repair. Shorter = less
    /// naked directional exposure; the post-flatten late-fill watcher bounds the
    /// cost of cutting it too fine.
    #[serde(default = "default_arb_settle_grace_secs")]
    pub arb_settle_grace_secs:        u64,
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
    #[serde(default = "default_time_decay_max_fast_velocity_pct")]
    pub time_decay_max_fast_velocity_pct:      Decimal,
    #[serde(default = "default_time_decay_max_slow_drift_pct")]
    pub time_decay_max_slow_drift_pct:         Decimal,
    #[serde(default = "default_time_decay_iv_stop_tighten_multiplier")]
    pub time_decay_iv_stop_tighten_multiplier: Decimal,
    #[serde(default = "default_time_decay_min_hold_secs")]
    pub time_decay_min_hold_secs:              i64,

    // ── Momentum Viper ────────────────────────────────────────────────────────
    pub momentum_min_trade_size_usdc:  Decimal,
    pub momentum_max_trade_size_usdc:  Decimal,
    pub momentum_stop_loss_pct:        Decimal,
    pub momentum_target_profit_pct:    Decimal,
    pub momentum_max_exposure_usdc:    Decimal,
    #[serde(default = "default_momentum_max_entry_price")]
    pub momentum_max_entry_price:      Decimal,
    #[serde(default = "default_momentum_min_entry_price")]
    pub momentum_min_entry_price:      Decimal,
    #[serde(default = "default_momentum_threshold_pct")]
    pub momentum_threshold_pct:        Decimal,
    #[serde(default = "default_momentum_max_entry_ask_sum")]
    pub momentum_max_entry_ask_sum:    Decimal,
    #[serde(default = "default_momentum_obi_adverse_block")]
    pub momentum_obi_adverse_block:    Decimal,
    #[serde(default = "default_momentum_obi_exhaustion_block")]
    pub momentum_obi_exhaustion_block: Decimal,
    /// Derivatives Raptor confirmation gate — blocks entries the perp book
    /// actively contradicts (counter-taker CVD or hard OI unwind). Inert on
    /// no-data (zero = neutral). Off by default (observe-first).
    #[serde(default = "default_deriv_gate_enabled")]
    pub momentum_deriv_gate_enabled:   bool,
    /// Distance from neutral CVD ratio 1.0 that blocks the contradicted
    /// direction: 0.15 ⇒ cvd ≤ 0.85 blocks bulls, cvd ≥ 1.15 blocks bears.
    #[serde(default = "default_deriv_cvd_confirm_margin")]
    pub momentum_deriv_cvd_confirm_margin: Decimal,
    /// OI delta at/below which hard de-leveraging blocks BOTH directions.
    #[serde(default = "default_deriv_oi_unwind_block")]
    pub momentum_deriv_oi_unwind_block: Decimal,
    #[serde(default = "default_momentum_take_profit_ceiling")]
    pub momentum_take_profit_ceiling:  Decimal,
    #[serde(default = "default_momentum_catastrophic_sl_pct")]
    pub momentum_catastrophic_sl_pct:  Decimal,
    #[serde(default = "default_momentum_min_secs_to_expiry_for_entry")]
    pub momentum_min_secs_to_expiry_for_entry: i64,
    /// Deepest drawdown at which the in-position OBI-exhaustion exit may still fire.
    /// Beyond it the (catastrophic) stop-loss owns the position instead.
    #[serde(default = "default_momentum_obi_exhaust_max_adverse_pct")]
    pub momentum_obi_exhaust_max_adverse_pct: Decimal,
    /// Minimum hold before the OBI-exhaustion exit is allowed to fire. Guards against
    /// exiting on tick one, when the position is underwater by the spread alone.
    #[serde(default = "default_momentum_obi_exhaust_min_hold_secs")]
    pub momentum_obi_exhaust_min_hold_secs: i64,
    /// How long the book must read exhausted, continuously, before the OBI exit
    /// fires. Seconds rather than ticks: the patrol loop runs at 75ms.
    #[serde(default = "default_momentum_obi_exhaust_persist_secs")]
    pub momentum_obi_exhaust_persist_secs: i64,
    /// Multiple of the round-trip taker fee the take-profit target must clear.
    #[serde(default = "default_momentum_tp_fee_margin_mult")]
    pub momentum_tp_fee_margin_mult: Decimal,

    // ── Maker Viper ───────────────────────────────────────────────────────────
    pub maker_max_entry_price:    Decimal,
    pub maker_min_entry_price:    Decimal,
    pub maker_stop_loss_pct:      Decimal,
    pub maker_target_profit_pct:  Decimal,
    /// Multiple of the single-leg exit fee a Maker take-profit must clear.
    /// The quote is post-only and pays nothing; only the closing FAK is charged.
    #[serde(default = "default_maker_tp_fee_margin_mult")]
    pub maker_tp_fee_margin_mult: Decimal,
    pub maker_max_exposure_usdc:  Decimal,
    #[serde(default = "default_maker_quote_size_usdc")]
    pub maker_quote_size_usdc:    Decimal,
    /// Longest time-to-resolution, in days, that a Quick deploy may auto-select.
    /// Discovery still lists markets far beyond this — see the constant's note.
    #[serde(default = "default_deploy_max_days_to_close")]
    pub deploy_max_days_to_close:      u32,
    /// Keep a politics squadron running without waiting for an operator deploy.
    #[serde(default = "default_auto_deploy_politics")]
    pub auto_deploy_politics:          bool,
    /// Keep a sports squadron running without waiting for an operator deploy.
    #[serde(default = "default_auto_deploy_sports")]
    pub auto_deploy_sports:            bool,
    /// Seconds after an event market closes before its squadron stands down,
    /// freeing the class for the next auto-deploy. A squadron still holding a
    /// position keeps patrolling regardless and retires once flat.
    #[serde(default = "default_event_market_retire_grace_secs")]
    pub event_market_retire_grace_secs: i64,
    /// Wrap USDC.e settlement proceeds sitting in the Safe back into pUSD so
    /// they count as tradeable collateral again. Off by default: it moves funds
    /// on-chain. Polymarket International only.
    #[serde(default = "default_collateral_sweep_enabled")]
    pub collateral_sweep_enabled:      bool,
    /// Smallest stranded USDC.e balance worth a sweep transaction, in dollars.
    #[serde(default = "default_collateral_sweep_min_usdc")]
    pub collateral_sweep_min_usdc:     Decimal,
    /// How hard `perpetual` works on one GBoost retrain. Higher grows more trees
    /// and fits the label pool more closely; too high overfits a small pool.
    #[serde(default = "default_gboost_budget")]
    pub gboost_budget:                 Decimal,
    /// Hard iteration ceiling for one retrain, so a fit cannot hold a blocking
    /// thread indefinitely. Caps whatever the budget would otherwise spend.
    #[serde(default = "default_gboost_iteration_limit")]
    pub gboost_iteration_limit:        u32,
    /// Seconds a live position quote is reused before re-asking the venue.
    #[serde(default = "default_position_quote_ttl_secs")]
    pub position_quote_ttl_secs:       u64,
    /// Token ceiling for one LLM Advisor reply — prose plus proposal block.
    #[serde(default = "default_llm_max_output_tokens")]
    pub llm_max_output_tokens:         u32,
    /// Read whole-book depth rather than the touch in the OBI entry gates.
    /// Does not affect the GBoost feature vector — see the constant's note.
    #[serde(default = "default_obi_use_whole_book")]
    pub obi_use_whole_book:            bool,
    /// Fold the venue's `price_change` updates into the intl order book between
    /// full snapshots (B36). Off restores the snapshot-only feed, which is the
    /// book as the last trade left it. Instance-level: the WebSocket tasks read
    /// the global row, so a squadron cannot hold a different answer.
    #[serde(default = "default_book_apply_price_changes")]
    pub book_apply_price_changes:      bool,
    #[serde(default = "default_maker_min_spread")]
    pub maker_min_spread:              Decimal,
    #[serde(default = "default_maker_bid_buffer")]
    pub maker_bid_buffer:              Decimal,
    #[serde(default = "default_maker_cross_buffer")]
    pub maker_cross_buffer:            Decimal,

    /// Cap the maker's bid at `best_bid + one tick` as well as `ask - buffer`.
    ///
    /// Ask-anchored pricing crosses most of a wide spread — on a 0.35/0.53 book
    /// it quoted 0.51 and marked to the bid instantly at -31%. True (default)
    /// makes the maker improve the bid instead of crossing to the ask.
    #[serde(default = "default_maker_improve_bid_only")]
    pub maker_improve_bid_only:        bool,
    #[serde(default = "default_maker_max_combined_bid")]
    pub maker_max_combined_bid:        Decimal,
    #[serde(default = "default_maker_max_complementary_price")]
    pub maker_max_complementary_price: Decimal,
    #[serde(default = "default_maker_max_book_imbalance_ratio")]
    pub maker_max_book_imbalance_ratio: Decimal,
    #[serde(default = "default_maker_min_secs_to_expiry")]
    pub maker_min_secs_to_expiry:      i64,
    /// Seconds a maker market must be observed before quoting into it.
    #[serde(default = "default_maker_min_market_age_secs")]
    pub maker_min_market_age_secs:     i64,
    /// Ceiling on the maturation wait as a fraction of the market's own life.
    #[serde(default = "default_maker_maturation_max_fraction")]
    pub maker_maturation_max_fraction: Decimal,
    #[serde(default = "default_maker_toxic_flow_exit_obi")]
    pub maker_toxic_flow_exit_obi:     Decimal,
    #[serde(default = "default_maker_toxic_reentry_cooldown_secs")]
    pub maker_toxic_reentry_cooldown_secs: i64,
    /// Min seconds held (from fill confirmation) before ToxicFill may fire.
    #[serde(default = "default_maker_toxic_min_hold_secs")]
    pub maker_toxic_min_hold_secs:     i64,
    /// Bid must be at least this fraction below avg entry for ToxicFill to fire.
    #[serde(default = "default_maker_toxic_min_adverse_pct")]
    pub maker_toxic_min_adverse_pct:   Decimal,
    /// Consecutive OBI breaches required before ToxicFill fires.
    #[serde(default = "default_maker_toxic_obi_confirm_ticks")]
    pub maker_toxic_obi_confirm_ticks: u32,
    /// Adverse oracle drift that pulls an UNFILLED resting quote. Cancelling costs
    /// nothing, so this stays tight.
    #[serde(default = "default_maker_oracle_drift_pull_frac")]
    pub maker_oracle_drift_pull_frac:  Decimal,
    /// Adverse oracle drift that exits a FILLED position, measured from the oracle
    /// at quote placement. The oracle leads OBI by minutes, so this fires before
    /// the OBI path can confirm. Looser than the pull above because exiting pays
    /// the spread. Set 0 to disable and fall back to OBI alone.
    #[serde(default = "default_maker_oracle_drift_exit_frac")]
    pub maker_oracle_drift_exit_frac:  Decimal,
    /// Post a resting post-only ask against a filled maker position so it exits
    /// by being lifted (spread capture) instead of crossing back to the bid.
    #[serde(default = "default_maker_resting_exit_enabled")]
    pub maker_resting_exit_enabled:    bool,

    /// Contamination filter for pricing an exit the exchange refused to confirm.
    /// See `config::EXIT_RECONCILE_MAX_DEVIATION`.
    #[serde(default = "default_exit_reconcile_max_deviation")]
    pub exit_reconcile_max_deviation:  Decimal,
    /// Seconds between one strategy's exit attempts.
    ///
    /// The pace between a FAK that sold nothing (the venue's synchronous
    /// answer) and the next attempt at the fresh bid. It was a compile-time
    /// constant an operator could not reach, and it is the one wait left on
    /// the intl exit path after Bug #29 — on an 11-tick/18s collapse like
    /// trade 19 (2026-09-01) a 5s pace costs about 3 ticks. Read through
    /// [`DynamicConfig::exit_retry_cooldown_secs_floored`], never directly:
    /// the Control Tower PATCH path does not run the build caps, so the floor
    /// is enforced where the value is used.
    #[serde(default = "default_exit_retry_cooldown_secs")]
    pub exit_retry_cooldown_secs:      u64,
    /// Price floor for the resting ask, as a fraction over avg entry.
    #[serde(default = "default_maker_resting_exit_min_edge_pct")]
    pub maker_resting_exit_min_edge_pct: Decimal,
    /// Ticks to undercut the best ask by when posting the resting exit.
    #[serde(default = "default_maker_resting_exit_ask_improvement_ticks")]
    pub maker_resting_exit_ask_improvement_ticks: i64,
    /// Minimum price change before an existing resting ask is repriced.
    #[serde(default = "default_maker_resting_exit_reprice_threshold")]
    pub maker_resting_exit_reprice_threshold: Decimal,

    // ── Basis Viper ───────────────────────────────────────────────────────────
    pub basis_max_exposure_usdc:  Decimal,
    pub basis_stop_loss_pct:      Decimal,
    pub basis_target_profit_pct:  Decimal,
    #[serde(default = "default_basis_max_entry_price")]
    pub basis_max_entry_price:         Decimal,
    #[serde(default = "default_basis_min_trade_size_usdc")]
    pub basis_min_trade_size_usdc:     Decimal,
    #[serde(default = "default_basis_max_trade_size_usdc")]
    pub basis_max_trade_size_usdc:     Decimal,
    #[serde(default = "default_basis_entry_skew_threshold")]
    pub basis_entry_skew_threshold:    Decimal,
    #[serde(default = "default_basis_skew_collapse_threshold")]
    pub basis_skew_collapse_threshold: Decimal,
    #[serde(default = "default_basis_catastrophic_sl_pct")]
    pub basis_catastrophic_sl_pct:     Decimal,
    #[serde(default = "default_basis_min_secs_to_expiry")]
    pub basis_min_secs_to_expiry:      i64,
    #[serde(default = "default_basis_max_spread_pct")]
    pub basis_max_spread_pct:          Decimal,
    #[serde(default = "default_basis_loss_lockout_count")]
    pub basis_loss_lockout_count:      i64,
    #[serde(default = "default_basis_loss_lockout_secs")]
    pub basis_loss_lockout_secs:       i64,
    #[serde(default = "default_basis_extreme_skew_bypass")]
    pub basis_extreme_skew_bypass:     bool,

    // ── GBoost Viper ──────────────────────────────────────────────────────────
    pub gboost_entry_threshold:   Decimal,
    pub gboost_stop_loss_pct:     Decimal,
    pub gboost_target_profit_pct: Decimal,
    pub gboost_max_exposure_usdc: Decimal,
    #[serde(default = "default_gboost_max_yes_entry_price")]
    pub gboost_max_yes_entry_price:   Decimal,
    #[serde(default = "default_gboost_max_no_entry_price")]
    pub gboost_max_no_entry_price:    Decimal,
    #[serde(default = "default_gboost_min_entry_price")]
    pub gboost_min_entry_price:       Decimal,
    #[serde(default = "default_gboost_obi_adverse_block")]
    pub gboost_obi_adverse_block:     Decimal,
    #[serde(default = "default_gboost_obi_exhaustion_block")]
    pub gboost_obi_exhaustion_block:  Decimal,
    #[serde(default = "default_gboost_min_edge_from_fair")]
    pub gboost_min_edge_from_fair:    Decimal,
    /// Floor on the oracle's 60-minute realized volatility (the Price raptor's
    /// normalized `hist_vol`, 0.02 per-tick std-dev = 1.0) below which GBoost
    /// vetoes entries as "oracle too flat". Was the compile-time
    /// `GBOOST_MIN_HIST_VOL`, which the conservative profile bakes at 0.0015 —
    /// roughly the median of normal live BTC — so the only way to test whether
    /// the quiet-regime veto earns its keep was an AMI rebuild. Hot-tunable now;
    /// every veto is shadow-logged with its `hist_vol` so the scoreboard can
    /// score any candidate floor against settled outcomes before it is applied.
    #[serde(default = "default_gboost_min_hist_vol")]
    pub gboost_min_hist_vol:          Decimal,
    #[serde(default = "default_gboost_min_net_profit_usdc")]
    pub gboost_min_net_profit_usdc:   Decimal,
    #[serde(default = "default_gboost_min_secs_to_expiry")]
    pub gboost_min_secs_to_expiry:    i64,
    #[serde(default = "default_gboost_signal_exit_threshold")]
    pub gboost_signal_exit_threshold: Decimal,
    /// Chi-squared drift score above which a retrain counts toward suppression.
    /// Scale is calibrated against GBOOST_DRIFT_WINDOW=400 live sessions: normal
    /// BTC intraday vol scores 15–21, genuine regime collapse 22+ (bug #10).
    #[serde(default = "default_gboost_concept_drift_threshold")]
    pub gboost_concept_drift_threshold: Decimal,
    /// Consecutive above-threshold retrains required to activate suppression.
    #[serde(default = "default_gboost_drift_consecutive_required")]
    pub gboost_drift_consecutive_required: i64,
    /// Consecutive below-threshold retrains required to clear suppression.
    #[serde(default = "default_gboost_drift_stable_clear_required")]
    pub gboost_drift_stable_clear_required: i64,
    /// Wall-clock cap (hours) on lookahead label age. The label pool is persisted
    /// to `logs/` across restarts (B33); samples older than this are pruned at
    /// every harvest, restored or not, so a pool reloaded after a long stop cannot
    /// train the model on a regime that is days gone. Values below 1 are clamped.
    #[serde(default = "default_gboost_label_max_age_hours")]
    pub gboost_label_max_age_hours: i64,
    /// Observe-only. While on, a GBoost signal that clears every entry gate is
    /// shadow-logged ("shadow mode: would enter ..." on the veto scoreboard)
    /// instead of placed. Ships on (B38): every model before 2026-09-06 was fit
    /// on a mislaid training matrix, and the corrected model's live behavior has
    /// never been observed. Turn off only after its shadow entries have been
    /// scored against settlement on /api/gboost/veto-scores.
    #[serde(default = "default_gboost_shadow_mode")]
    pub gboost_shadow_mode: bool,
    /// Structural floor on a retrain's tree count (B37). Catches the fit that
    /// stops at a single stump because the window's labels offered nothing to
    /// learn (1 to 3 trees); it is not a quality bar, tree count does not track
    /// holdout quality. Values below 1 are treated as 1.
    #[serde(default = "default_gboost_structural_min_trees")]
    pub gboost_structural_min_trees: i64,
    /// Retrain acceptance bar (B37): the logloss skill a validation fit must
    /// show on the newest tenth of the pool, held out behind a purge gap of
    /// twice the label horizon, before the retrain is adopted. Skill is
    /// 1 - model_logloss / best_constant_logloss: 0 matches a coin weighted at
    /// the holdout's own base rate, negative is worse than that (usually an
    /// overconfident fit). A rejected retrain keeps the previous model and
    /// logs the measured skill, so a run of rejections in the log is the model
    /// failing to generalize to the latest window, not a fault.
    #[serde(default = "default_gboost_holdout_min_skill")]
    pub gboost_holdout_min_skill: Decimal,

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
    #[serde(default = "default_trendcapture_min_entry_price")]
    pub trendcapture_min_entry_price:      Decimal,
    #[serde(default = "default_trendcapture_max_entry_ask_sum")]
    pub trendcapture_max_entry_ask_sum:    Decimal,
    #[serde(default = "default_trendcapture_obi_adverse_block")]
    pub trendcapture_obi_adverse_block:    Decimal,
    /// Derivatives Raptor confirmation gate (see `momentum_deriv_gate_enabled`).
    #[serde(default = "default_deriv_gate_enabled")]
    pub trendcapture_deriv_gate_enabled:   bool,
    #[serde(default = "default_deriv_cvd_confirm_margin")]
    pub trendcapture_deriv_cvd_confirm_margin: Decimal,
    #[serde(default = "default_deriv_oi_unwind_block")]
    pub trendcapture_deriv_oi_unwind_block: Decimal,
    #[serde(default = "default_trendcapture_obi_exhaustion_block")]
    pub trendcapture_obi_exhaustion_block: Decimal,
    #[serde(default = "default_trendcapture_max_token_spread_pct")]
    pub trendcapture_max_token_spread_pct: Decimal,
    #[serde(default = "default_trendcapture_reversal_drift_pct")]
    pub trendcapture_reversal_drift_pct:   Decimal,
    #[serde(default = "default_trendcapture_strike_gap_pct")]
    pub trendcapture_strike_gap_pct:       Decimal,
    #[serde(default = "default_trendcapture_take_profit_ceiling")]
    pub trendcapture_take_profit_ceiling:  Decimal,
    #[serde(default = "default_trendcapture_catastrophic_sl_pct")]
    pub trendcapture_catastrophic_sl_pct:  Decimal,
    #[serde(default = "default_trendreversal_mode")]
    pub trendreversal_mode:                bool,

    // ── FairValue Viper (2026-08-05) ──────────────────────────────────────────
    #[serde(default = "default_fairvalue_enable")]
    pub enable_fairvalue:                 bool,
    #[serde(default = "default_fairvalue_trade_size")]
    pub fairvalue_trade_size_usdc:        Decimal,
    #[serde(default = "default_fairvalue_max_exposure")]
    pub fairvalue_max_exposure_usdc:      Decimal,
    #[serde(default = "default_fairvalue_base_edge")]
    pub fairvalue_base_edge:              Decimal,
    /// Prefer the hourly market over the Window/Daily venue for entries — the
    /// daily horizon pins the required edge at its cap, making entries impossible.
    #[serde(default = "default_fairvalue_prefer_hourly")]
    pub fairvalue_prefer_hourly:          bool,
    #[serde(default = "default_fairvalue_min_edge")]
    pub fairvalue_min_edge:               Decimal,
    #[serde(default = "default_fairvalue_min_entry_price")]
    pub fairvalue_min_entry_price:        Decimal,
    #[serde(default = "default_fairvalue_max_entry_price")]
    pub fairvalue_max_entry_price:        Decimal,
    #[serde(default = "default_fairvalue_target_profit")]
    pub fairvalue_target_profit_pct:      Decimal,
    #[serde(default = "default_fairvalue_stop_loss")]
    pub fairvalue_stop_loss_pct:          Decimal,
    /// Fraction of the entry fair value the model may lose before the
    /// model-reversal exit fires. Entry-relative, never an absolute floor.
    #[serde(default = "default_fairvalue_reversal_decay")]
    pub fairvalue_model_reversal_decay_pct: Decimal,
    /// How far fair value may retreat from its entry level before the stop-loss
    /// veto is withdrawn. The veto's arithmetic edge grows as the position
    /// loses, so without this it is strongest exactly when the stop is most
    /// needed. 0 disables the guard.
    #[serde(default = "default_fairvalue_stop_veto_max_model_decay_pct")]
    pub fairvalue_stop_veto_max_model_decay_pct: Decimal,
    /// Forecast horizon at or below which the σ floor stops binding, ramping to
    /// full strength at twice this. Trust an in-sample vol measurement; floor
    /// only what it cannot see.
    #[serde(default = "default_fairvalue_sigma_floor_horizon")]
    pub fairvalue_sigma_floor_horizon_secs: i64,
    /// Seconds a token is locked out after any FairValue exit. Re-entries into a
    /// market the viper has just left were 0-for-4 in prod (2026-08-13/14).
    #[serde(default = "default_fairvalue_post_exit_cooldown")]
    pub fairvalue_post_exit_cooldown_secs: i64,
    /// Stop-outs allowed on one market before the breaker bars further entries.
    #[serde(default = "default_fairvalue_max_stop_losses")]
    pub fairvalue_max_stop_losses_per_market: u32,
    /// Multiple of the model's own short-horizon noise the edge must clear.
    /// 0 disables the gate.
    #[serde(default = "default_fairvalue_edge_noise_multiple")]
    pub fairvalue_edge_noise_multiple:    Decimal,
    /// Multiple of the *entry* edge requirement the model must still show, at the
    /// live ask, for a losing position to veto its own (non-catastrophic) stop
    /// loss. Higher ⇒ stricter ⇒ the stop fires more readily. 0 disables the
    /// veto and restores a price-only stop.
    ///
    /// The stop was noise-blind while the entry gate was not: on 2026-08-15 a NO
    /// position entered at $0.50 with edge +0.178 (req 0.145) stopped out at
    /// $0.44 — six cents, or 1.4× the model's own 120s noise — with 3,800s left
    /// to run. At that moment the model still read edge +0.145 vs req 0.140, and
    /// the contract settled at $1.00. Realised −$0.48 against +$2.46 available.
    #[serde(default = "default_fairvalue_stop_model_confirm")]
    pub fairvalue_stop_model_confirm_frac: Decimal,
    /// Manage an entry whose take-profit is unreachable (entry × (1 + TP) ≥ $1)
    /// as a settlement snipe: no percentage stop, sell only when the fee-net
    /// bid is worth at least the model's settlement value; catastrophic floor
    /// and endgame bail-out kept. Off restores the percentage stop everywhere.
    #[serde(default = "default_fairvalue_settle_snipe_hold")]
    pub fairvalue_settle_snipe_hold:      bool,
    /// Take profit with a resting post-only ask at entry × (1 + TP) instead of
    /// a taker FAK at the bid. Lifted only when the market runs through the
    /// price the viper would have sold at anyway, so it carries none of the
    /// adverse selection of a resting bid; every stop still crosses and pulls
    /// the ask first. Off restores the taker take-profit.
    #[serde(default = "default_fairvalue_resting_tp_enabled")]
    pub fairvalue_resting_tp_enabled:     bool,

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
    #[serde(default = "default_convergence_min_entry_price")]
    pub convergence_min_entry_price:      Decimal,
    #[serde(default = "default_convergence_pulse_threshold")]
    pub convergence_pulse_threshold:      Decimal,
    #[serde(default = "default_convergence_coherence_min")]
    pub convergence_coherence_min:        Decimal,
    #[serde(default = "default_convergence_cvd_confirm_margin")]
    pub convergence_cvd_confirm_margin:   Decimal,
    #[serde(default = "default_convergence_max_token_spread_pct")]
    pub convergence_max_token_spread_pct: Decimal,
    #[serde(default = "default_convergence_obi_adverse_block")]
    pub convergence_obi_adverse_block:    Decimal,
    /// Deadband below which a drift leg counts as neutral in the 10m-vs-60m
    /// coherence check. Both legs must clear it before an opposition vetoes entry.
    #[serde(default = "default_convergence_drift_coherence_deadband_pct")]
    pub convergence_drift_coherence_deadband_pct: Decimal,
    /// Deadband beyond which 5s oracle velocity running against the intended side
    /// vetoes entry. Zero velocity never vetoes — this is opposition, not confirmation.
    #[serde(default = "default_convergence_velocity_opposition_pct")]
    pub convergence_velocity_opposition_pct: Decimal,
    #[serde(default = "default_convergence_skip_band_low")]
    pub convergence_skip_band_low:        Decimal,
    #[serde(default = "default_convergence_skip_band_high")]
    pub convergence_skip_band_high:       Decimal,

    // ── Raptor polling ────────────────────────────────────────────────────────
    // Cadence for the two credentialed, budget-metered Raptors. These are live
    // knobs rather than constants because the right value depends on the API
    // plan the operator bought, which the build cannot know: the compile-time
    // defaults are sized for each provider's FREE tier, and a paid plan wants a
    // much faster poll. Changing either takes effect on the next cycle — the
    // raptor loops select on this channel, so they do not sit out the remainder
    // of an old, long sleep before adopting a new value.
    //
    // The floors in `config_schema.rs` matter: these drive outbound request
    // rates against third-party rate limits, and the LLM autonomy tiers can move
    // config, so an unclamped value risks a provider ban rather than a bad fill.
    /// Entry veto on order-book imbalance for the side FairValue is buying.
    /// OBI = (bid_depth − ask_depth)/total on that token; below this, the book
    /// is too offer-heavy to exit without giving back far more than the stop.
    /// See FAIRVALUE_OBI_ADVERSE_BLOCK for the incident that motivated it.
    #[serde(default = "default_fairvalue_obi_adverse_block")]
    pub fairvalue_obi_adverse_block:      Decimal,
    /// Seconds the entry side's OBI must stay clear of the block before an
    /// entry is admitted. The block is a single 50ms sample; at the touch it
    /// is one or two orders wide and flickers. Zero restores the instant gate.
    #[serde(default = "default_fairvalue_obi_clear_secs")]
    pub fairvalue_obi_clear_secs:         u64,

    /// Seconds between Sports Raptor (The Odds API) polls.
    #[serde(default = "default_sports_poll_secs")]
    pub sports_poll_secs:                 u64,
    /// Warn when The Odds API reports this many requests left in the quota.
    /// Sized for the tier you are on — the free tier's ~500/month makes 50 a
    /// useful warning, while a paid plan would warn constantly at that value.
    #[serde(default = "default_sports_low_budget_warn")]
    pub sports_low_budget_warn:           i64,
    /// Seconds between Tennis Raptor (Live Tennis API) polls.
    #[serde(default = "default_tennis_poll_secs")]
    pub tennis_poll_secs:                 u64,
    /// Warn when the Live Tennis API reports this many requests left in the
    /// current window.
    #[serde(default = "default_tennis_low_budget_warn")]
    pub tennis_low_budget_warn:           i64,

    // ── Raptor feed selectors ─────────────────────────────────────────────────
    // Free-text provider identifiers. Unlike every numeric knob above these
    // cannot be range-clamped — the set of valid values is defined by the
    // upstream API, not by DRADIS — so a wrong value is accepted here and
    // rejected (or silently ignored) by the provider. The Setup UI warns about
    // that; getting the identifier right is the operator's responsibility.
    /// The Odds API sport key, e.g. `upcoming` (next games across all in-season
    /// sports) or a specific key like `americanfootball_nfl`.
    #[serde(default = "default_sports_odds_sport")]
    pub sports_odds_sport:                String,
    /// Comma-separated bookmaker regions for the odds query: `us`, `us2`, `uk`,
    /// `eu`, `au`.
    #[serde(default = "default_sports_odds_regions")]
    pub sports_odds_regions:              String,
    /// Live Tennis API tour filter: `atp`, `wta`, `challenger`, `itf`,
    /// `juniors`, or empty for all tours.
    #[serde(default = "default_tennis_tour")]
    pub tennis_tour:                      String,
}

impl Default for DynamicConfig {
    /// Seeds all values from the compile-time defaults in config.rs.
    /// This is the definitive single source of truth for initial values —
    /// the SQLite row is only authoritative once the user has changed something.
    fn default() -> Self {
        Self {
            // GHOST_MODE_DEFAULT, not GHOST_MODE: this seeds a fresh install only.
            ghost_mode: config::GHOST_MODE_DEFAULT,
            intl_taker_fee_rate: config::INTL_TAKER_FEE_RATE,

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
            arbitrage_min_leg_conviction: config::ARBITRAGE_MIN_LEG_CONVICTION,
            arb_fak_rehedge_buffer:       config::ARB_FAK_REHEDGE_BUFFER,
            arb_settle_grace_secs:        config::ARB_SETTLE_GRACE_SECS,
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
            time_decay_max_fast_velocity_pct:      config::TIME_DECAY_MAX_FAST_VELOCITY_PCT,
            time_decay_max_slow_drift_pct:         config::TIME_DECAY_MAX_SLOW_DRIFT_PCT,
            time_decay_iv_stop_tighten_multiplier: config::TIME_DECAY_IV_STOP_TIGHTEN_MULTIPLIER,
            time_decay_min_hold_secs:              config::TIME_DECAY_MIN_HOLD_SECS,

            momentum_min_trade_size_usdc:  config::MOMENTUM_MIN_TRADE_SIZE_USDC,
            momentum_max_trade_size_usdc:  config::MOMENTUM_MAX_TRADE_SIZE_USDC,
            momentum_stop_loss_pct:        config::MOMENTUM_STOP_LOSS_PERCENT,
            momentum_target_profit_pct:    config::MOMENTUM_TARGET_PROFIT_PERCENT,
            momentum_max_exposure_usdc:    config::MOMENTUM_MAX_EXPOSURE_USDC,
            momentum_max_entry_price:      config::MAX_MOMENTUM_ENTRY_PRICE,
            momentum_min_entry_price:      config::MOMENTUM_MIN_ENTRY_PRICE,
            momentum_threshold_pct:        config::MOMENTUM_THRESHOLD_PCT,
            momentum_max_entry_ask_sum:    config::MOMENTUM_MAX_ENTRY_ASK_SUM,
            momentum_obi_adverse_block:    config::MOMENTUM_OBI_ADVERSE_BLOCK,
            momentum_obi_exhaustion_block: config::MOMENTUM_OBI_EXHAUSTION_BLOCK,
            momentum_deriv_gate_enabled:       config::DERIV_GATE_ENABLED,
            momentum_deriv_cvd_confirm_margin: config::DERIV_CVD_CONFIRM_MARGIN,
            momentum_deriv_oi_unwind_block:    config::DERIV_OI_UNWIND_BLOCK,
            momentum_take_profit_ceiling:  config::MOMENTUM_TAKE_PROFIT_CEILING,
            momentum_catastrophic_sl_pct:  config::MOMENTUM_CATASTROPHIC_SL_PCT,
            momentum_min_secs_to_expiry_for_entry: config::MOMENTUM_MIN_SECS_TO_EXPIRY_FOR_ENTRY,
            momentum_obi_exhaust_max_adverse_pct: config::MOMENTUM_OBI_EXHAUST_MAX_ADVERSE_PCT,
            momentum_obi_exhaust_min_hold_secs:   config::MOMENTUM_OBI_EXHAUST_MIN_HOLD_SECS,
            momentum_obi_exhaust_persist_secs:    config::MOMENTUM_OBI_EXHAUST_PERSIST_SECS,
            momentum_tp_fee_margin_mult:          config::MOMENTUM_TP_FEE_MARGIN_MULT,

            maker_max_entry_price:    config::MAKER_MAX_ENTRY_PRICE,
            maker_min_entry_price:    config::MAKER_MIN_ENTRY_PRICE,
            maker_stop_loss_pct:      config::MAKER_STOP_LOSS_PERCENT,
            maker_target_profit_pct:  config::MAKER_TARGET_PROFIT_PERCENT,
            maker_tp_fee_margin_mult: config::MAKER_TP_FEE_MARGIN_MULT,
            maker_max_exposure_usdc:  config::MAKER_MAX_EXPOSURE_USDC,
            maker_quote_size_usdc:    config::MAKER_QUOTE_SIZE_USDC,
            deploy_max_days_to_close:      config::DEPLOY_MAX_DAYS_TO_CLOSE,
            auto_deploy_politics:          config::AUTO_DEPLOY_POLITICS,
            auto_deploy_sports:            config::AUTO_DEPLOY_SPORTS,
            event_market_retire_grace_secs: config::EVENT_MARKET_RETIRE_GRACE_SECS,
            collateral_sweep_enabled:      config::COLLATERAL_SWEEP_ENABLED,
            collateral_sweep_min_usdc:     config::COLLATERAL_SWEEP_MIN_USDC,
            gboost_budget:                 config::GBOOST_BUDGET,
            gboost_iteration_limit:        config::GBOOST_ITERATION_LIMIT,
            position_quote_ttl_secs:       config::POSITION_QUOTE_TTL_SECS,
            llm_max_output_tokens:         config::LLM_MAX_OUTPUT_TOKENS,
            obi_use_whole_book:            config::OBI_USE_WHOLE_BOOK,
            book_apply_price_changes:      config::BOOK_APPLY_PRICE_CHANGES,
            maker_min_spread:              config::MAKER_MIN_SPREAD,
            maker_bid_buffer:              config::MAKER_BID_BUFFER,
            maker_cross_buffer:            config::MAKER_CROSS_BUFFER,
            maker_improve_bid_only:        config::MAKER_IMPROVE_BID_ONLY,
            maker_max_combined_bid:        config::MAKER_MAX_COMBINED_BID,
            maker_max_complementary_price: config::MAKER_MAX_COMPLEMENTARY_PRICE,
            maker_max_book_imbalance_ratio: config::MAKER_MAX_BOOK_IMBALANCE_RATIO,
            maker_min_secs_to_expiry:      config::MAKER_MIN_SECS_TO_EXPIRY,
            maker_min_market_age_secs:     config::MAKER_MIN_MARKET_AGE_SECS,
            maker_maturation_max_fraction: config::MAKER_MATURATION_MAX_FRACTION,
            maker_toxic_flow_exit_obi:     config::MAKER_TOXIC_FLOW_EXIT_OBI,
            maker_toxic_reentry_cooldown_secs: config::MAKER_TOXIC_REENTRY_COOLDOWN_SECS,
            maker_toxic_min_hold_secs:     config::MAKER_TOXIC_MIN_HOLD_SECS,
            maker_toxic_min_adverse_pct:   config::MAKER_TOXIC_MIN_ADVERSE_PCT,
            maker_toxic_obi_confirm_ticks: config::MAKER_TOXIC_OBI_CONFIRM_TICKS,
            maker_oracle_drift_pull_frac:  config::MAKER_ORACLE_DRIFT_PULL_FRAC,
            maker_oracle_drift_exit_frac:  config::MAKER_ORACLE_DRIFT_EXIT_FRAC,
            maker_resting_exit_enabled:    config::MAKER_RESTING_EXIT_ENABLED,
            exit_reconcile_max_deviation:  config::EXIT_RECONCILE_MAX_DEVIATION,
            exit_retry_cooldown_secs:      config::EXIT_RETRY_COOLDOWN_SECS,
            maker_resting_exit_min_edge_pct: config::MAKER_RESTING_EXIT_MIN_EDGE_PCT,
            maker_resting_exit_ask_improvement_ticks: config::MAKER_RESTING_EXIT_ASK_IMPROVEMENT_TICKS,
            maker_resting_exit_reprice_threshold: config::MAKER_RESTING_EXIT_REPRICE_THRESHOLD,

            basis_max_exposure_usdc:  config::BASIS_MAX_EXPOSURE_USDC,
            basis_stop_loss_pct:      config::BASIS_STOP_LOSS_PERCENT,
            basis_target_profit_pct:  config::BASIS_TARGET_PROFIT_PERCENT,
            basis_max_entry_price:         config::BASIS_MAX_ENTRY_PRICE,
            basis_min_trade_size_usdc:     config::BASIS_MIN_TRADE_SIZE_USDC,
            basis_max_trade_size_usdc:     config::BASIS_MAX_TRADE_SIZE_USDC,
            basis_entry_skew_threshold:    config::BASIS_ENTRY_SKEW_THRESHOLD,
            basis_skew_collapse_threshold: config::BASIS_SKEW_COLLAPSE_THRESHOLD,
            basis_catastrophic_sl_pct:     config::BASIS_CATASTROPHIC_SL_PCT,
            basis_min_secs_to_expiry:      config::BASIS_MIN_SECS_TO_EXPIRY,
            basis_max_spread_pct:          config::BASIS_MAX_SPREAD_PCT,
            basis_loss_lockout_count:      config::BASIS_LOSS_LOCKOUT_COUNT,
            basis_loss_lockout_secs:       config::BASIS_LOSS_LOCKOUT_SECS,
            basis_extreme_skew_bypass:     config::BASIS_EXTREME_SKEW_BYPASS,

            gboost_entry_threshold:   config::GBOOST_ENTRY_THRESHOLD,
            gboost_stop_loss_pct:     config::GBOOST_STOP_LOSS_PERCENT,
            gboost_target_profit_pct: config::GBOOST_TARGET_PROFIT_PERCENT,
            gboost_max_exposure_usdc: config::GBOOST_MAX_EXPOSURE_USDC,
            gboost_max_yes_entry_price:   config::GBOOST_MAX_YES_ENTRY_PRICE,
            gboost_max_no_entry_price:    config::GBOOST_MAX_NO_ENTRY_PRICE,
            gboost_min_entry_price:       config::GBOOST_MIN_ENTRY_PRICE,
            gboost_obi_adverse_block:     config::GBOOST_OBI_ADVERSE_BLOCK,
            gboost_obi_exhaustion_block:  config::GBOOST_OBI_EXHAUSTION_BLOCK,
            gboost_min_edge_from_fair:    config::GBOOST_MIN_EDGE_FROM_FAIR,
            gboost_min_hist_vol:          decimal_from_f64(config::GBOOST_MIN_HIST_VOL),
            gboost_min_net_profit_usdc:   config::GBOOST_MIN_NET_PROFIT_USDC,
            gboost_min_secs_to_expiry:    config::GBOOST_MIN_SECS_TO_EXPIRY,
            gboost_signal_exit_threshold: config::GBOOST_SIGNAL_EXIT_THRESHOLD,
            gboost_concept_drift_threshold: config::GBOOST_CONCEPT_DRIFT_THRESHOLD,
            gboost_drift_consecutive_required: config::GBOOST_DRIFT_CONSECUTIVE_REQUIRED as i64,
            gboost_drift_stable_clear_required: config::GBOOST_DRIFT_STABLE_CLEAR_REQUIRED as i64,
            gboost_label_max_age_hours: config::GBOOST_LABEL_MAX_AGE_HOURS,
            gboost_shadow_mode:         config::GBOOST_SHADOW_MODE,
            gboost_structural_min_trees: config::GBOOST_STRUCTURAL_MIN_TREES as i64,
            gboost_holdout_min_skill:   config::GBOOST_HOLDOUT_MIN_SKILL,

            trendcapture_min_trade_size_usdc: config::TRENDCAPTURE_MIN_TRADE_SIZE_USDC,
            trendcapture_max_trade_size_usdc: config::TRENDCAPTURE_MAX_TRADE_SIZE_USDC,
            trendcapture_max_exposure_usdc:   config::TRENDCAPTURE_MAX_EXPOSURE_USDC,
            trendcapture_stop_loss_pct:       config::TRENDCAPTURE_STOP_LOSS_PERCENT,
            trendcapture_target_profit_pct:   config::TRENDCAPTURE_TARGET_PROFIT_PERCENT,
            trendcapture_max_entry_price:     config::TRENDCAPTURE_MAX_ENTRY_PRICE,
            trendcapture_min_entry_price:      config::TRENDCAPTURE_MIN_ENTRY_PRICE,
            trendcapture_max_entry_ask_sum:    config::TRENDCAPTURE_MAX_ENTRY_ASK_SUM,
            trendcapture_obi_adverse_block:    config::TRENDCAPTURE_OBI_ADVERSE_BLOCK,
            trendcapture_deriv_gate_enabled:       config::DERIV_GATE_ENABLED,
            trendcapture_deriv_cvd_confirm_margin: config::DERIV_CVD_CONFIRM_MARGIN,
            trendcapture_deriv_oi_unwind_block:    config::DERIV_OI_UNWIND_BLOCK,
            trendcapture_obi_exhaustion_block: config::TRENDCAPTURE_OBI_EXHAUSTION_BLOCK,
            trendcapture_max_token_spread_pct: config::TRENDCAPTURE_MAX_TOKEN_SPREAD_PCT,
            trendcapture_reversal_drift_pct:   config::TRENDCAPTURE_REVERSAL_DRIFT_PCT,
            trendcapture_strike_gap_pct:       config::TRENDCAPTURE_STRIKE_GAP_PCT,
            trendcapture_take_profit_ceiling:  config::TRENDCAPTURE_TAKE_PROFIT_CEILING,
            trendcapture_catastrophic_sl_pct:  config::TRENDCAPTURE_CATASTROPHIC_SL_PCT,
            trendreversal_mode:                config::TRENDREVERSAL_MODE,

            enable_fairvalue:                 config::ENABLE_FAIRVALUE_TRADING,
            fairvalue_trade_size_usdc:        config::FAIRVALUE_TRADE_SIZE_USDC,
            fairvalue_max_exposure_usdc:      config::FAIRVALUE_MAX_EXPOSURE_USDC,
            fairvalue_base_edge:              config::FAIRVALUE_BASE_EDGE,
            fairvalue_prefer_hourly:          config::FAIRVALUE_PREFER_HOURLY,
            fairvalue_min_edge:               config::FAIRVALUE_MIN_EDGE,
            fairvalue_min_entry_price:        config::FAIRVALUE_MIN_ENTRY_PRICE,
            fairvalue_max_entry_price:        config::FAIRVALUE_MAX_ENTRY_PRICE,
            fairvalue_target_profit_pct:      config::FAIRVALUE_TARGET_PROFIT_PERCENT,
            fairvalue_stop_loss_pct:          config::FAIRVALUE_STOP_LOSS_PERCENT,
            fairvalue_model_reversal_decay_pct: config::FAIRVALUE_MODEL_REVERSAL_DECAY_PCT,
            fairvalue_stop_veto_max_model_decay_pct: config::FAIRVALUE_STOP_VETO_MAX_MODEL_DECAY_PCT,
            fairvalue_sigma_floor_horizon_secs: config::FAIRVALUE_SIGMA_FLOOR_HORIZON_SECS,
            fairvalue_post_exit_cooldown_secs: config::FAIRVALUE_POST_EXIT_COOLDOWN_SECS,
            fairvalue_max_stop_losses_per_market: config::FAIRVALUE_MAX_STOP_LOSSES_PER_MARKET,
            fairvalue_edge_noise_multiple:    config::FAIRVALUE_EDGE_NOISE_MULTIPLE,
            fairvalue_stop_model_confirm_frac: config::FAIRVALUE_STOP_MODEL_CONFIRM_FRAC,
            fairvalue_settle_snipe_hold:      config::FAIRVALUE_SETTLE_SNIPE_HOLD,
            fairvalue_resting_tp_enabled:     config::FAIRVALUE_RESTING_TP_ENABLED,

            enable_convergence:               config::ENABLE_CONVERGENCE_TRADING,
            convergence_position_size_usdc:   config::CONVERGENCE_POSITION_SIZE_USDC,
            convergence_max_exposure_usdc:    config::CONVERGENCE_MAX_EXPOSURE_USDC,
            convergence_stop_loss_pct:        config::CONVERGENCE_STOP_LOSS_PERCENT,
            convergence_target_profit_pct:    config::CONVERGENCE_TARGET_PROFIT_PERCENT,
            convergence_max_entry_price:      config::CONVERGENCE_MAX_ENTRY_PRICE,
            convergence_min_entry_price:      config::CONVERGENCE_MIN_ENTRY_PRICE,
            convergence_pulse_threshold:      config::CONVERGENCE_PULSE_THRESHOLD,
            convergence_coherence_min:        config::CONVERGENCE_COHERENCE_MIN,
            convergence_cvd_confirm_margin:   config::CONVERGENCE_CVD_CONFIRM_MARGIN,
            convergence_max_token_spread_pct: config::CONVERGENCE_MAX_TOKEN_SPREAD_PCT,
            convergence_obi_adverse_block:    config::CONVERGENCE_OBI_ADVERSE_BLOCK,
            convergence_drift_coherence_deadband_pct: config::CONVERGENCE_DRIFT_COHERENCE_DEADBAND_PCT,
            convergence_velocity_opposition_pct: config::CONVERGENCE_VELOCITY_OPPOSITION_PCT,
            convergence_skip_band_low:        config::CONVERGENCE_SKIP_BAND_LOW,
            convergence_skip_band_high:       config::CONVERGENCE_SKIP_BAND_HIGH,

            fairvalue_obi_adverse_block:      config::FAIRVALUE_OBI_ADVERSE_BLOCK,
            fairvalue_obi_clear_secs:         config::FAIRVALUE_OBI_CLEAR_SECS,
            sports_poll_secs:                 config::SPORTS_POLL_SECS,
            sports_low_budget_warn:           config::SPORTS_ODDS_LOW_BUDGET_WARN,
            tennis_poll_secs:                 config::TENNIS_POLL_SECS,
            tennis_low_budget_warn:           config::TENNIS_LOW_BUDGET_WARN,
            sports_odds_sport:                config::SPORTS_ODDS_SPORT.to_string(),
            sports_odds_regions:              config::SPORTS_ODDS_REGIONS.to_string(),
            tennis_tour:                      config::TENNIS_TOUR.to_string(),
        }
    }
}

// ─── SQLite key ──────────────────────────────────────────────────────────────

const DB_KEY: &str = "dynamic_config";

/// Read-only / demo mode flag, mirroring the API server's `DRADIS_READ_ONLY` gate.
///
/// In demo mode the persisted DynamicConfig (global + squadron-scoped) is bypassed
/// entirely so the Control Tower always renders the compile-time defaults from
/// config.rs. The demo DB is never edited via the UI (all mutations are rejected),
/// so without this its stale config rows would shadow newer config.rs constants
/// (e.g. a lowered take-profit) indefinitely. Live deployments are unaffected.
pub fn read_only_mode() -> bool {
    std::env::var("DRADIS_READ_ONLY")
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

/// Fields that describe the INSTANCE rather than a squadron's risk appetite,
/// and must therefore read the same everywhere.
///
/// `ghost_mode` is the archetype: it says whether real money moves at all. A
/// squadron holding a different answer from the global row is not a
/// customization, it is a lie about what the engine is doing.
///
/// This registry exists because that lie was told. On 2026-08-29 the operator of
/// the production Marketplace instance pressed the Control Tower's GHOST/LIVE
/// button to go live. The global row flipped, every surface rendered LIVE, and
/// all three deployed squadrons kept `ghost_mode: true` and went on simulating
/// fills for a day. Worse, the divergence then survived a machine migration: the
/// config bundle exported global `false` alongside three squadron rows saying
/// `true`, and the import restored that state faithfully onto a new box.
///
/// Two places consult this, for the two halves of that failure:
/// [`reconcile_global_semantics`] is applied when a squadron's config is read,
/// so a divergent row can never be honored, and again on bundle import, so a
/// divergent row is not persisted in the first place. Adding a field means
/// adding it here and to the function; `reconciles_every_declared_key` fails if
/// you miss one.
pub const GLOBAL_SEMANTICS_KEYS: &[&str] = &["ghost_mode", "book_apply_price_changes"];

/// Force `cfg`'s instance-level fields to agree with `global`.
///
/// Returns the keys it had to correct, so callers can say so loudly rather than
/// fixing the problem in silence — silence is what made the original incident
/// cost a day.
pub fn reconcile_global_semantics(
    cfg: &mut DynamicConfig,
    global: &DynamicConfig,
) -> Vec<&'static str> {
    let mut corrected = Vec::new();
    if cfg.ghost_mode != global.ghost_mode {
        cfg.ghost_mode = global.ghost_mode;
        corrected.push("ghost_mode");
    }
    // The intl book feed is one WebSocket task per token reading the global
    // row; a squadron row saying otherwise would describe a feed that does not
    // exist.
    if cfg.book_apply_price_changes != global.book_apply_price_changes {
        cfg.book_apply_price_changes = global.book_apply_price_changes;
        corrected.push("book_apply_price_changes");
    }
    corrected
}

/// Fields whose runtime value is capped by a compile-time constant.
///
/// These three are re-clamped on EVERY read of a persisted config, in
/// `load_or_default` and `load_for_squadron`, under a "stricter wins" rule: a
/// stale database row must never loosen a limit the build has tightened. That
/// rule earned its place — on 2026-06-01 a DB row carrying an 8% momentum stop
/// survived a code change to 5% and exited a losing trade three points late.
///
/// The consequence is that raising one of these at runtime does nothing, and
/// for a long time it did nothing *silently*: on 2026-08-29 an operator
/// approved `time_decay_max_entry_price -> 0.5` on the live Marketplace
/// instance, the write succeeded, the audit ledger stamped it `applied`, and
/// every reader went on seeing 0.46 because the cap re-applied underneath. The
/// proposal had passed validation because the schema range says 0.0 to 1.0 and
/// nothing consulted the build cap.
///
/// So the caps live here, in one place, and both sides consult them: the read
/// path through [`apply_build_caps`] and the LLM proposal validator through
/// [`build_cap_for`]. Adding a fourth cap means adding it to `BUILD_CAPPED_KEYS`
/// and to both functions; `build_caps_cover_every_declared_key` fails if you
/// miss one.
pub const BUILD_CAPPED_KEYS: &[&str] = &[
    "time_decay_max_entry_price",
    "time_decay_stop_loss_pct",
    "momentum_stop_loss_pct",
];

/// The build's hard ceiling for `field`, or `None` when it has no cap.
///
/// A runtime value above this is not an error, it is simply unreachable: the
/// read path lowers it back on the next load.
pub fn build_cap_for(field: &str) -> Option<Decimal> {
    match field {
        "time_decay_max_entry_price" => Some(config::TIME_DECAY_MAX_ENTRY_PRICE),
        "time_decay_stop_loss_pct"   => Some(config::TIME_DECAY_STOP_LOSS_PERCENT),
        "momentum_stop_loss_pct"     => Some(config::MOMENTUM_STOP_LOSS_PERCENT),
        _ => None,
    }
}

/// Lower any capped field back to its build ceiling. "Stricter wins."
/// The shortest pace the exit path will run at, whatever the config says.
///
/// One second, because below it the pace stops being a pace: the patrol
/// ticks every 50ms and every exit attempt is a freshly signed FAK, so a zero
/// here would submit up to twenty orders a second per strategy against a book
/// the venue has just said holds no liquidity — the WebSocket snapshot the
/// price is taken from does not refresh faster than a few hundred
/// milliseconds, so those resubmissions would price against the SAME
/// snapshot and could only draw rate limiting. At 1s the cost on a trade-19
/// collapse (11 ticks in 18s) is at most ~0.6 tick, and attempts are bounded
/// at one per second. Enforced in code, not only in the schema: a PATCH from
/// the Control Tower or the LLM advisor does not pass through `apply_build_caps`.
pub const EXIT_RETRY_COOLDOWN_FLOOR_SECS: u64 = 1;

impl DynamicConfig {
    /// Is the named strategy switched on in this config?
    ///
    /// Each viper reads its own `enable_*` flag inside `evaluate_entry` and
    /// reports "disabled in config" when it is off. The patrol loop needs the
    /// same answer BEFORE evaluation when it records an idle tick for a
    /// squadron with no market: a viper the operator has switched off must keep
    /// its "disabled in config" row rather than be re-labeled as waiting, or
    /// the Control Tower ribbon's disabled tally drifts for the length of the
    /// gap. Unknown names count as enabled, matching the executor, which runs
    /// everything the registry builds.
    pub fn strategy_enabled(&self, strategy_name: &str) -> bool {
        match crate::orchestrator::registry::strategy_name_to_kind(strategy_name) {
            "arbitrage"    => self.enable_arbitrage,
            "maker"        => self.enable_maker,
            "momentum"     => self.enable_momentum,
            "time_decay"   => self.enable_time_decay,
            "basis"        => self.enable_basis,
            "gboost"       => self.enable_gboost,
            "convergence"  => self.enable_convergence,
            "fairvalue"    => self.enable_fairvalue,
            "trendcapture" => self.enable_trendcapture,
            _ => true,
        }
    }

    /// `exit_retry_cooldown_secs` with the floor applied. The only way the
    /// patrol reads the knob.
    pub fn exit_retry_cooldown_secs_floored(&self) -> u64 {
        self.exit_retry_cooldown_secs.max(EXIT_RETRY_COOLDOWN_FLOOR_SECS)
    }
}

pub fn apply_build_caps(cfg: &mut DynamicConfig) {
    cfg.time_decay_max_entry_price =
        cfg.time_decay_max_entry_price.min(config::TIME_DECAY_MAX_ENTRY_PRICE);
    cfg.time_decay_stop_loss_pct =
        cfg.time_decay_stop_loss_pct.min(config::TIME_DECAY_STOP_LOSS_PERCENT);
    cfg.momentum_stop_loss_pct =
        cfg.momentum_stop_loss_pct.min(config::MOMENTUM_STOP_LOSS_PERCENT);
}

impl DynamicConfig {
    /// Load the most recent DynamicConfig from SQLite.
    /// If no record exists (first run), seeds defaults and writes them to DB.
    pub async fn load_or_default() -> Arc<Self> {
        if read_only_mode() {
            info!("⚙️  READ-ONLY demo mode — bypassing persisted DynamicConfig, using compile-time defaults");
            return Arc::new(DynamicConfig::default());
        }
        if let Some(pool) = db::pool() {
            if let Some(json) = db::config_get(pool, DB_KEY).await {
                match serde_json::from_str::<DynamicConfig>(&json) {
                    Ok(mut cfg) => {
                        // ── Build-cap enforcement ────────────────────────────────────
                        // Compile-time constants are the hard limits. A stale DB row can
                        // never override a tightened constant — code fixes take effect
                        // immediately on the next startup without a manual DB reset.
                        // Rule: "stricter wins".
                        //
                        // Root cause of the 2026-06-01 13:39 loss (-$0.6122): the DB had
                        // an 8% momentum stop persisted while config.rs said 5%, and with
                        // no cap the old value survived, exiting at -8%.
                        //
                        // Shared with the LLM proposal validator so the two cannot
                        // disagree about what is reachable — see `apply_build_caps`.
                        apply_build_caps(&mut cfg);

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
        Self::apply_patch_as(current, patch_json, "operator").await
    }

    /// Like [`Self::apply_patch`] but with explicit attribution for the
    /// `config_history` trail (e.g. `"llm_advisor"` for autonomy-tier applies,
    /// `"llm_breaker"` for circuit-breaker reverts).
    pub async fn apply_patch_as(current: &Arc<Self>, patch_json: &str, actor: &str) -> Result<Arc<Self>> {
        let mut value = serde_json::to_value(current.as_ref())?;
        let patch: serde_json::Value = serde_json::from_str(patch_json)?;

        // Merge: patch fields overwrite current fields; unknown keys are ignored.
        if let (Some(obj), Some(patch_obj)) = (value.as_object_mut(), patch.as_object()) {
            for (k, v) in patch_obj {
                obj.insert(k.clone(), v.clone());
            }
        }

        let updated: DynamicConfig = serde_json::from_value(value)?;
        updated.save_as(actor).await;
        info!("⚙️  DynamicConfig hot-patched and persisted (by {actor})");
        Ok(Arc::new(updated))
    }

    // ── Squadron-scoped config methods ─────────────────────────────────────────

    /// Load a squadron's config from the squadron_configs table.
    /// If none exists, returns a fresh copy of compile-time defaults (does NOT persist yet).
    /// Caller is responsible for persisting via save_for_squadron() if needed.
    pub async fn load_for_squadron(squadron_id: &str) -> Arc<Self> {
        if read_only_mode() {
            info!("⚙️  READ-ONLY demo mode — squadron {} using compile-time defaults", squadron_id);
            return Arc::new(DynamicConfig::default());
        }
        if let Some(pool) = db::pool() {
            if let Some(json) = db::squadron_config_get(pool, squadron_id).await {
                match serde_json::from_str::<DynamicConfig>(&json) {
                    Ok(mut cfg) => {
                        // Same build caps as the global config — see `apply_build_caps`.
                        apply_build_caps(&mut cfg);

                        // Instance-level fields follow the global row, always.
                        // A squadron row that disagrees is never honored — see
                        // `GLOBAL_SEMANTICS_KEYS` for the incident that earned
                        // this. Loud on purpose: the original failure was silent.
                        // Read the global value from the live broadcast, NOT
                        // `load_or_default`: that function writes a full-config
                        // `session_start_snapshot` row to config_history on every
                        // call, and the Kalshi and US traders call
                        // `load_for_squadron` every 30s per squadron. Routing
                        // through it here would have written ~2,880 history rows
                        // per squadron per day, growing the DB without bound and
                        // burying the audit trail this table exists to provide.
                        // The sender is registered at startup and holds the same
                        // value the DB does, so this is both cheaper and fresher;
                        // the DB fallback covers the pre-registration window.
                        let global = match global_config_tx() {
                            Some(tx) => tx.borrow().clone(),
                            None => Self::load_or_default().await,
                        };
                        let corrected = reconcile_global_semantics(&mut cfg, &global);
                        if !corrected.is_empty() {
                            warn!(
                                "⚠️  Squadron [{}] config disagreed with the global row on {:?} — \
                                 overridden to match. The stored row is stale; re-save it from the \
                                 Control Tower to clear this.",
                                squadron_id, corrected,
                            );
                        }

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
        // Seed from the PERSISTED GLOBAL config, not compile-time defaults.
        //
        // `apply_profile` in api/setup.rs writes the operator's chosen profile to
        // the global row and fans it out to squadrons that are already deployed,
        // on the stated understanding that this function seeds future ones from
        // that same row. It did not — it used `DynamicConfig::default()`, which
        // is whichever profile the binary was COMPILED with. The AMI compiles
        // conservative, so on a fresh box the order is: choose a profile, restart
        // the engine, squadrons deploy, and every one of them is seeded
        // conservative regardless of the choice.
        //
        // The result was silent and total: the global row said aggressive while
        // every squadron actually trading said conservative, with Momentum,
        // Basis, Convergence and TrendReversal switched off. The first decision a
        // customer makes had no effect on the money.
        //
        // `load_or_default` falls back to compile-time defaults when no global
        // row exists, so a box where nobody has chosen a profile behaves exactly
        // as before.
        let cfg = Self::load_or_default().await;
        cfg.save_for_squadron(squadron_id).await;
        info!("⚙️  Squadron config initialized from global config: {}", squadron_id);
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
        if read_only_mode() {
            // Demo mode: never persist, always reflect compile-time defaults.
            return Self::load_for_squadron(squadron_id).await;
        }
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
        Self::apply_squadron_patch_as(squadron_id, patch_json, "operator").await
    }

    /// Like [`Self::apply_squadron_patch`] but with explicit attribution for the
    /// `config_history` trail (e.g. `"profile_conservative"` for a risk-profile
    /// apply).
    ///
    /// Squadron patches were previously unaudited: a squadron-scoped change —
    /// which is the ONLY kind that reaches a running patrol loop — left no trace,
    /// while the equivalent global change recorded a full snapshot.  That made a
    /// live config change effectively unrevertable.  Recording the pre-patch row
    /// here restores parity with `save_as`.
    pub async fn apply_squadron_patch_as(
        squadron_id: &str,
        patch_json: &str,
        actor: &str,
    ) -> Result<Arc<Self>> {
        // Serialized across all squadrons for the whole read-merge-write.
        //
        // This is a read-modify-write on a whole config document, and without a
        // lock two concurrent patches on one squadron lose an update: both read
        // the same starting config, both merge their own field, and the second
        // write carries the first's field at its ORIGINAL value. The change is
        // reported as applied by the API, recorded as applied in `llm_actions`,
        // and is simply not there.
        //
        // Observed 2026-08-24: an operator approved four LLM recommendations on
        // btc-hourly within two seconds; three landed and
        // `time_decay_max_entry_price -> 0.5` silently did not, while every
        // surface said it had. The advisor's own auto-apply path was never
        // exposed to this — it builds one combined patch and applies it once —
        // so only hand-approval could trigger it, which is the path an operator
        // uses to test recommendations.
        //
        // One global lock rather than one per squadron: patches are rare
        // (operator clicks, an hourly advisory) and cheap, so there is nothing
        // to gain from finer granularity and a map of locks to get wrong.
        static PATCH_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        let _guard = PATCH_LOCK.lock().await;

        let current = Self::load_for_squadron(squadron_id).await;
        let mut value = serde_json::to_value(current.as_ref())?;
        let patch: serde_json::Value = serde_json::from_str(patch_json)?;

        if let (Some(obj), Some(patch_obj)) = (value.as_object_mut(), patch.as_object()) {
            for (k, v) in patch_obj {
                obj.insert(k.clone(), v.clone());
            }
        }

        let mut updated: DynamicConfig = serde_json::from_value(value)?;

        // A squadron patch may not set an instance-level field.
        //
        // `current` above came through `load_for_squadron`, so it was already
        // reconciled — but the merge just layered the caller's patch on top and
        // could have reintroduced a divergence. Every writer of a squadron row
        // funnels through here, so this is the chokepoint: the AI Actions Approve
        // button, a hand-rolled `PATCH /api/squadrons/{id}/config`, and the
        // global fan-out all land on this line.
        //
        // The fan-out is unaffected: `patch_config` writes and persists the
        // global row BEFORE fanning out, so reconciling against global here
        // yields exactly the value the operator asked for. What it stops is a
        // squadron being set to a mode the instance is not in — one squadron
        // quietly trading real money while the header, the banner and
        // `GET /api/config` all report GHOST, with no API surface exposing the
        // split.
        let global: Arc<DynamicConfig> = match global_config_tx() {
            Some(tx) => tx.borrow().clone(),
            None => Self::load_or_default().await,
        };
        let overridden = reconcile_global_semantics(&mut updated, &global);
        if !overridden.is_empty() {
            warn!(
                "⚠️  Squadron [{}] patch by {} tried to set instance-level field(s) {:?} — \
                 ignored; these follow the global config. Change them globally instead.",
                squadron_id, actor, overridden,
            );
        }

        // Record the diff BEFORE overwriting so the change stays revertable.
        // Keyed per squadron so the history view can tell which squadron moved.
        if let Some(pool) = db::pool() {
            match serde_json::to_string(&updated) {
                Ok(new_json) => {
                    let old_json = db::squadron_config_get(pool, squadron_id).await;
                    db::record_config_change(
                        pool,
                        actor,
                        &format!("squadron:{squadron_id}"),
                        old_json.as_deref(),
                        &new_json,
                    ).await;
                }
                Err(e) => warn!("⚠️  Squadron config serialize error [{}]: {}", squadron_id, e),
            }
        }

        updated.save_for_squadron(squadron_id).await;

        // Push the merged config into the running squadron's live handle so the
        // patrol loop picks it up on the next tick (not just on market rotation).
        if let Ok(reg) = squadron_config_registry().lock() {
            if let Some(handle) = reg.get(squadron_id) {
                if let Ok(mut live) = handle.write() {
                    *live = updated.clone();
                    info!("⚙️  Squadron config applied live: {}", squadron_id);
                } else {
                    warn!("⚠️  Squadron config live handle poisoned [{}] — DB updated, live apply on next rotation", squadron_id);
                }
            } else {
                warn!("⚠️  Squadron config live handle not registered [{}] — DB updated, live apply on next rotation", squadron_id);
            }
        }

        info!("⚙️  Squadron config hot-patched: {} (by {})", squadron_id, actor);
        Ok(Arc::new(updated))
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    /// Config rows persist in SQLite across deploys, so every row written before
    /// a field existed must still deserialize — `apply_patch` and the loader at
    /// boot both go through `serde_json::from_*` against the full struct, and a
    /// missing `#[serde(default)]` turns an old row into a hard startup failure
    /// rather than a silent fallback.
    ///
    /// Built by serializing the current struct and *deleting* the new keys,
    /// which is exactly what an older row looks like on disk. Only fields added
    /// after the schema settled carry `#[serde(default)]` — the core ones are
    /// required — so an empty object is not a valid stand-in for a legacy row.
    #[test]
    fn a_config_row_predating_the_newest_knobs_still_loads() {
        let mut legacy = serde_json::to_value(DynamicConfig::default()).unwrap();
        let obj = legacy.as_object_mut().unwrap();
        for added in ["fairvalue_stop_model_confirm_frac", "arb_settle_grace_secs", "fairvalue_settle_snipe_hold", "fairvalue_resting_tp_enabled"] {
            assert!(obj.remove(added).is_some(), "{added} must be a serialized field");
        }
        let cfg: DynamicConfig =
            serde_json::from_value(legacy).expect("an old persisted row must still deserialize");

        assert_eq!(
            cfg.fairvalue_stop_model_confirm_frac,
            config::FAIRVALUE_STOP_MODEL_CONFIRM_FRAC
        );
        assert_eq!(cfg.arb_settle_grace_secs, config::ARB_SETTLE_GRACE_SECS);
        assert_eq!(cfg.fairvalue_settle_snipe_hold, config::FAIRVALUE_SETTLE_SNIPE_HOLD);
        assert_eq!(cfg.fairvalue_resting_tp_enabled, config::FAIRVALUE_RESTING_TP_ENABLED);
    }

    /// The orphan settle grace is a naked-exposure window, so it must stay well
    /// inside the post-flatten late-fill watch that backstops it — if the grace
    /// ever outgrew that watch, committing to a repair would no longer be
    /// covered by the mechanism that makes a short grace safe.
    #[test]
    fn the_settle_grace_stays_inside_its_backstop() {
        let grace = DynamicConfig::default().arb_settle_grace_secs;
        assert!(grace > 0, "a zero grace would flatten on an unsettled balance read");
        assert!(
            grace < 20,
            "grace {grace}s must stay under ARBITER_LATE_FILL_WATCH_SECS (20s), \
             the watcher that bounds the cost of committing early"
        );
    }
}

#[cfg(test)]
mod build_cap_tests {
    use super::*;

    /// The drift guard. `BUILD_CAPPED_KEYS` is the declared list, `build_cap_for`
    /// is what the LLM validator consults, and `apply_build_caps` is what the
    /// read path enforces. A cap added to only some of the three is precisely
    /// the shape of the 2026-08-29 defect, so this proves all three agree by
    /// pushing every declared key above its cap and watching it come back.
    #[test]
    fn build_caps_cover_every_declared_key() {
        for key in BUILD_CAPPED_KEYS {
            let cap = build_cap_for(key)
                .unwrap_or_else(|| panic!("{key} is declared capped but build_cap_for returns None"));

            let mut value = serde_json::to_value(DynamicConfig::default()).expect("serializes");
            let over = cap + Decimal::ONE;
            value[*key] = serde_json::Value::String(over.to_string());

            let mut cfg: DynamicConfig =
                serde_json::from_value(value).unwrap_or_else(|e| panic!("{key}: {e}"));
            apply_build_caps(&mut cfg);

            let after = serde_json::to_value(&cfg).expect("serializes");
            let got: Decimal = after[*key]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| panic!("{key} is not a Decimal-shaped field"));
            assert_eq!(got, cap, "apply_build_caps does not enforce the cap on {key}");
        }
    }

    /// A cap must never RAISE a value. "Stricter wins" is one-way.
    #[test]
    fn a_value_under_the_cap_is_left_alone() {
        for key in BUILD_CAPPED_KEYS {
            let cap = build_cap_for(key).expect("declared");
            let under = cap / Decimal::TWO;

            let mut value = serde_json::to_value(DynamicConfig::default()).expect("serializes");
            value[*key] = serde_json::Value::String(under.to_string());
            let mut cfg: DynamicConfig = serde_json::from_value(value).expect("deserializes");
            apply_build_caps(&mut cfg);

            let after = serde_json::to_value(&cfg).expect("serializes");
            let got: Decimal = after[*key].as_str().and_then(|s| s.parse().ok()).expect("decimal");
            assert_eq!(got, under, "apply_build_caps raised {key} toward its cap");
        }
    }

    #[test]
    fn an_uncapped_field_reports_no_cap() {
        assert!(build_cap_for("maker_max_entry_price").is_none());
        assert!(build_cap_for("not_a_field_at_all").is_none());
    }

    /// Applying the caps twice must equal applying them once.
    #[test]
    fn applying_the_caps_is_idempotent() {
        let mut once = DynamicConfig::default();
        once.time_decay_max_entry_price = Decimal::ONE;
        apply_build_caps(&mut once);
        let mut twice = once.clone();
        apply_build_caps(&mut twice);
        assert_eq!(
            serde_json::to_value(&once).unwrap(),
            serde_json::to_value(&twice).unwrap(),
        );
    }
}

#[cfg(test)]
mod global_semantics_tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// Drift guard, matching `build_caps_cover_every_declared_key`. Every key in
    /// `GLOBAL_SEMANTICS_KEYS` must actually be reconciled by the function; a key
    /// declared but not handled is a field that silently stays divergent, which
    /// is precisely the 2026-08-29 failure.
    #[test]
    fn reconciles_every_declared_key() {
        for key in GLOBAL_SEMANTICS_KEYS {
            let global = DynamicConfig::default();
            let mut value = serde_json::to_value(&global).expect("serializes");

            // Flip the declared key so it disagrees with global.
            match &value[*key] {
                serde_json::Value::Bool(b) => value[*key] = serde_json::Value::Bool(!b),
                other => panic!(
                    "{key} is {other:?}, which this test cannot flip — extend the test \
                     when adding a non-bool global-semantics field"
                ),
            }

            let mut cfg: DynamicConfig =
                serde_json::from_value(value).unwrap_or_else(|e| panic!("{key}: {e}"));
            let corrected = reconcile_global_semantics(&mut cfg, &global);

            assert!(corrected.contains(key), "{key} declared but not reconciled");
            let after = serde_json::to_value(&cfg).expect("serializes");
            let want = serde_json::to_value(&global).expect("serializes");
            assert_eq!(after[*key], want[*key], "{key} still disagrees after reconciliation");
        }
    }

    /// The exact production state: global says live, the squadron says ghost.
    /// The squadron must lose.
    #[test]
    fn a_ghosting_squadron_under_a_live_global_is_forced_live() {
        let mut global = DynamicConfig::default();
        global.ghost_mode = false;
        let mut squadron = DynamicConfig::default();
        squadron.ghost_mode = true;

        let corrected = reconcile_global_semantics(&mut squadron, &global);
        assert_eq!(corrected, vec!["ghost_mode"]);
        assert!(!squadron.ghost_mode, "squadron kept simulating under a live global");
    }

    /// And the safe direction too: a global set to ghost must pull a squadron
    /// back out of live trading, not only the other way around.
    #[test]
    fn a_live_squadron_under_a_ghost_global_is_forced_to_ghost() {
        let mut global = DynamicConfig::default();
        global.ghost_mode = true;
        let mut squadron = DynamicConfig::default();
        squadron.ghost_mode = false;

        let corrected = reconcile_global_semantics(&mut squadron, &global);
        assert_eq!(corrected, vec!["ghost_mode"]);
        assert!(squadron.ghost_mode, "squadron stayed live under a ghost global");
    }

    /// Agreement is silent: reconciliation must report nothing when there is
    /// nothing to correct, or every config read logs a warning.
    #[test]
    fn agreement_reports_no_corrections() {
        let global = DynamicConfig::default();
        let mut squadron = DynamicConfig::default();
        assert!(reconcile_global_semantics(&mut squadron, &global).is_empty());
    }

    /// The write-path chokepoint. Every squadron-row writer funnels through
    /// `apply_squadron_patch_as`, so reconciling AFTER the merge is what stops a
    /// caller reintroducing a divergence the read path would only mask later.
    /// This asserts the ordering property the fix depends on: merging a patch
    /// that sets an instance-level field, then reconciling, yields the global
    /// value rather than the patch's.
    #[test]
    fn a_patch_cannot_reintroduce_a_divergence_after_reconciliation() {
        let mut global = DynamicConfig::default();
        global.ghost_mode = false;

        // Start from an already-reconciled base, as `load_for_squadron` returns.
        let mut merged = DynamicConfig::default();
        merged.ghost_mode = false;
        assert!(reconcile_global_semantics(&mut merged, &global).is_empty());

        // A squadron-scoped patch layers ghost_mode back on.
        merged.ghost_mode = true;

        // Reconciling after the merge is what makes the write safe.
        let overridden = reconcile_global_semantics(&mut merged, &global);
        assert_eq!(overridden, vec!["ghost_mode"]);
        assert!(!merged.ghost_mode, "a squadron patch escaped the chokepoint");
    }

    /// Reconciliation is scoped: it must not touch a squadron's own risk
    /// settings, which are legitimately per-squadron.
    #[test]
    fn per_squadron_risk_settings_are_left_alone() {
        let global = DynamicConfig::default();
        let mut squadron = DynamicConfig::default();
        squadron.maker_max_entry_price = dec!(0.61);
        squadron.momentum_max_exposure_usdc = dec!(42);

        reconcile_global_semantics(&mut squadron, &global);
        assert_eq!(squadron.maker_max_entry_price, dec!(0.61));
        assert_eq!(squadron.momentum_max_exposure_usdc, dec!(42));
    }
}

#[cfg(test)]
mod exit_retry_cooldown_tests {
    use super::*;

    /// Promoting the constant to a knob must not move the default: a knob that
    /// silently ships a new value is two changes wearing one hat. The shipped
    /// pace stays exactly `config::EXIT_RETRY_COOLDOWN_SECS`.
    #[test]
    fn the_knob_defaults_to_the_compile_time_constant_and_changes_no_behavior() {
        let dc = DynamicConfig::default();
        assert_eq!(dc.exit_retry_cooldown_secs, config::EXIT_RETRY_COOLDOWN_SECS);
        assert_eq!(dc.exit_retry_cooldown_secs_floored(), config::EXIT_RETRY_COOLDOWN_SECS);
        // A persisted config written before the field existed — yesterday's
        // full record, minus this key — reads the same.
        let mut legacy = serde_json::to_value(DynamicConfig::default()).unwrap();
        legacy.as_object_mut().unwrap().remove("exit_retry_cooldown_secs");
        let legacy: DynamicConfig = serde_json::from_value(legacy).expect("pre-field config parses");
        assert_eq!(legacy.exit_retry_cooldown_secs_floored(), config::EXIT_RETRY_COOLDOWN_SECS);
    }

    /// The Control Tower PATCH path merges JSON and never runs `apply_build_caps`,
    /// so a value below the floor CAN be persisted. The read must clamp it: an
    /// operator who patches 0 gets one attempt per second, not twenty.
    #[test]
    fn a_patched_value_below_the_floor_is_clamped_where_it_is_read() {
        let mut v = serde_json::to_value(DynamicConfig::default()).unwrap();
        v["exit_retry_cooldown_secs"] = serde_json::json!(0);
        let patched: DynamicConfig = serde_json::from_value(v).expect("patched config parses");
        assert_eq!(patched.exit_retry_cooldown_secs, 0, "the raw field keeps what was patched");
        assert_eq!(patched.exit_retry_cooldown_secs_floored(), EXIT_RETRY_COOLDOWN_FLOOR_SECS);
    }

    /// Above the floor the operator's value is honored as-is.
    #[test]
    fn a_value_at_or_above_the_floor_is_used_unchanged() {
        let mut dc = DynamicConfig::default();
        dc.exit_retry_cooldown_secs = 2;
        assert_eq!(dc.exit_retry_cooldown_secs_floored(), 2);
        dc.exit_retry_cooldown_secs = EXIT_RETRY_COOLDOWN_FLOOR_SECS;
        assert_eq!(dc.exit_retry_cooldown_secs_floored(), EXIT_RETRY_COOLDOWN_FLOOR_SECS);
    }

    /// Every registry name must resolve to its own flag: a name that fell
    /// through to the `_ => true` arm would be recorded as waiting even when
    /// the operator had switched it off.
    #[test]
    fn strategy_enabled_follows_each_vipers_own_flag() {
        let mut dc = DynamicConfig::default();
        let setters: [(&str, fn(&mut DynamicConfig, bool)); 9] = [
            ("ArbitrageStrategy",     |d, v| d.enable_arbitrage = v),
            ("MakerStrategy",         |d, v| d.enable_maker = v),
            ("MomentumStrategy",      |d, v| d.enable_momentum = v),
            ("TimeDecayStrategy",     |d, v| d.enable_time_decay = v),
            ("BasisStrategy",         |d, v| d.enable_basis = v),
            ("GboostStrategy",        |d, v| d.enable_gboost = v),
            ("ConvergenceStrategy",   |d, v| d.enable_convergence = v),
            ("FairValueStrategy",     |d, v| d.enable_fairvalue = v),
            ("TrendReversalStrategy", |d, v| d.enable_trendcapture = v),
        ];
        for (name, set) in setters {
            set(&mut dc, true);
            assert!(dc.strategy_enabled(name), "{name} should read enabled");
            set(&mut dc, false);
            assert!(!dc.strategy_enabled(name), "{name} should read disabled");
        }
        assert!(dc.strategy_enabled("NoSuchStrategy"), "unknown names run, as in the executor");
    }
}
