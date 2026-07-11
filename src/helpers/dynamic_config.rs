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

/// Register (or replace) the live config handle a running squadron's patrol loop
/// reads each tick.  Called once per squadron deploy / market rotation.
pub fn register_squadron_config_handle(squadron_id: &str, handle: Arc<RwLock<DynamicConfig>>) {
    if let Ok(mut reg) = squadron_config_registry().lock() {
        reg.insert(squadron_id.to_string(), handle);
    }
}

// ── serde default helpers ────────────────────────────────────────────────────
// Required when adding new fields to DynamicConfig: old DB rows that were
// serialized before the field existed will have it missing.  Without a default,
// serde returns a deserialization error and load_or_default resets to factory
// defaults — clobbering any operator customisation made in the previous session.
fn default_arb_max_leg_price()             -> Decimal { config::ARBITRAGE_MAX_LEG_PRICE             }
fn default_arb_max_leg_obi()               -> Decimal { config::ARBITRAGE_MAX_LEG_OBI               }
fn default_arb_max_obi_asymmetry()         -> Decimal { config::ARBITRAGE_MAX_OBI_ASYMMETRY         }
fn default_arb_min_leg_conviction()        -> Decimal { config::ARBITRAGE_MIN_LEG_CONVICTION        }
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

// ── Newly-exposed advanced knobs (previously compile-time only) ───────────────
fn default_basis_max_entry_price()          -> Decimal { config::BASIS_MAX_ENTRY_PRICE                 }
fn default_basis_min_trade_size_usdc()      -> Decimal { config::BASIS_MIN_TRADE_SIZE_USDC             }
fn default_basis_max_trade_size_usdc()      -> Decimal { config::BASIS_MAX_TRADE_SIZE_USDC             }
fn default_basis_entry_skew_threshold()     -> Decimal { config::BASIS_ENTRY_SKEW_THRESHOLD            }
fn default_basis_skew_collapse_threshold()  -> Decimal { config::BASIS_SKEW_COLLAPSE_THRESHOLD         }
fn default_basis_catastrophic_sl_pct()      -> Decimal { config::BASIS_CATASTROPHIC_SL_PCT             }
fn default_basis_min_secs_to_expiry()       -> i64     { config::BASIS_MIN_SECS_TO_EXPIRY              }

fn default_convergence_min_entry_price()    -> Decimal { config::CONVERGENCE_MIN_ENTRY_PRICE           }
fn default_convergence_pulse_threshold()    -> Decimal { config::CONVERGENCE_PULSE_THRESHOLD           }
fn default_convergence_coherence_min()      -> Decimal { config::CONVERGENCE_COHERENCE_MIN             }
fn default_convergence_cvd_confirm_margin() -> Decimal { config::CONVERGENCE_CVD_CONFIRM_MARGIN        }
fn default_convergence_max_token_spread_pct() -> Decimal { config::CONVERGENCE_MAX_TOKEN_SPREAD_PCT    }
fn default_convergence_obi_adverse_block()  -> Decimal { config::CONVERGENCE_OBI_ADVERSE_BLOCK         }
fn default_convergence_skip_band_low()      -> Decimal { config::CONVERGENCE_SKIP_BAND_LOW             }
fn default_convergence_skip_band_high()     -> Decimal { config::CONVERGENCE_SKIP_BAND_HIGH            }

fn default_maker_min_spread()               -> Decimal { config::MAKER_MIN_SPREAD                      }
fn default_maker_bid_buffer()               -> Decimal { config::MAKER_BID_BUFFER                      }
fn default_maker_cross_buffer()             -> Decimal { config::MAKER_CROSS_BUFFER                    }
fn default_maker_quote_size_usdc()          -> Decimal { config::MAKER_QUOTE_SIZE_USDC                 }
fn default_maker_max_combined_bid()         -> Decimal { config::MAKER_MAX_COMBINED_BID                }
fn default_maker_max_complementary_price()  -> Decimal { config::MAKER_MAX_COMPLEMENTARY_PRICE         }
fn default_maker_max_book_imbalance_ratio() -> Decimal { config::MAKER_MAX_BOOK_IMBALANCE_RATIO        }
fn default_maker_min_secs_to_expiry()       -> i64     { config::MAKER_MIN_SECS_TO_EXPIRY              }
fn default_maker_toxic_flow_exit_obi()      -> Decimal { config::MAKER_TOXIC_FLOW_EXIT_OBI             }

fn default_momentum_max_entry_price()       -> Decimal { config::MAX_MOMENTUM_ENTRY_PRICE              }
fn default_momentum_min_entry_price()       -> Decimal { config::MOMENTUM_MIN_ENTRY_PRICE              }
fn default_momentum_threshold_pct()         -> Decimal { config::MOMENTUM_THRESHOLD_PCT                }
fn default_momentum_max_entry_ask_sum()     -> Decimal { config::MOMENTUM_MAX_ENTRY_ASK_SUM            }
fn default_momentum_obi_adverse_block()     -> Decimal { config::MOMENTUM_OBI_ADVERSE_BLOCK            }
fn default_momentum_obi_exhaustion_block()  -> Decimal { config::MOMENTUM_OBI_EXHAUSTION_BLOCK         }
fn default_momentum_take_profit_ceiling()   -> Decimal { config::MOMENTUM_TAKE_PROFIT_CEILING          }
fn default_momentum_catastrophic_sl_pct()   -> Decimal { config::MOMENTUM_CATASTROPHIC_SL_PCT          }
fn default_momentum_min_secs_to_expiry_for_entry() -> i64 { config::MOMENTUM_MIN_SECS_TO_EXPIRY_FOR_ENTRY }

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
fn default_gboost_min_net_profit_usdc()     -> Decimal { config::GBOOST_MIN_NET_PROFIT_USDC            }
fn default_gboost_min_secs_to_expiry()      -> i64     { config::GBOOST_MIN_SECS_TO_EXPIRY             }
fn default_gboost_signal_exit_threshold()   -> Decimal { config::GBOOST_SIGNAL_EXIT_THRESHOLD          }

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
    #[serde(default = "default_momentum_take_profit_ceiling")]
    pub momentum_take_profit_ceiling:  Decimal,
    #[serde(default = "default_momentum_catastrophic_sl_pct")]
    pub momentum_catastrophic_sl_pct:  Decimal,
    #[serde(default = "default_momentum_min_secs_to_expiry_for_entry")]
    pub momentum_min_secs_to_expiry_for_entry: i64,

    // ── Maker Viper ───────────────────────────────────────────────────────────
    pub maker_max_entry_price:    Decimal,
    pub maker_min_entry_price:    Decimal,
    pub maker_stop_loss_pct:      Decimal,
    pub maker_target_profit_pct:  Decimal,
    pub maker_max_exposure_usdc:  Decimal,
    #[serde(default = "default_maker_quote_size_usdc")]
    pub maker_quote_size_usdc:    Decimal,
    #[serde(default = "default_maker_min_spread")]
    pub maker_min_spread:              Decimal,
    #[serde(default = "default_maker_bid_buffer")]
    pub maker_bid_buffer:              Decimal,
    #[serde(default = "default_maker_cross_buffer")]
    pub maker_cross_buffer:            Decimal,
    #[serde(default = "default_maker_max_combined_bid")]
    pub maker_max_combined_bid:        Decimal,
    #[serde(default = "default_maker_max_complementary_price")]
    pub maker_max_complementary_price: Decimal,
    #[serde(default = "default_maker_max_book_imbalance_ratio")]
    pub maker_max_book_imbalance_ratio: Decimal,
    #[serde(default = "default_maker_min_secs_to_expiry")]
    pub maker_min_secs_to_expiry:      i64,
    #[serde(default = "default_maker_toxic_flow_exit_obi")]
    pub maker_toxic_flow_exit_obi:     Decimal,

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
    #[serde(default = "default_gboost_min_net_profit_usdc")]
    pub gboost_min_net_profit_usdc:   Decimal,
    #[serde(default = "default_gboost_min_secs_to_expiry")]
    pub gboost_min_secs_to_expiry:    i64,
    #[serde(default = "default_gboost_signal_exit_threshold")]
    pub gboost_signal_exit_threshold: Decimal,

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
    #[serde(default = "default_convergence_skip_band_low")]
    pub convergence_skip_band_low:        Decimal,
    #[serde(default = "default_convergence_skip_band_high")]
    pub convergence_skip_band_high:       Decimal,
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
            arbitrage_min_leg_conviction: config::ARBITRAGE_MIN_LEG_CONVICTION,
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
            momentum_take_profit_ceiling:  config::MOMENTUM_TAKE_PROFIT_CEILING,
            momentum_catastrophic_sl_pct:  config::MOMENTUM_CATASTROPHIC_SL_PCT,
            momentum_min_secs_to_expiry_for_entry: config::MOMENTUM_MIN_SECS_TO_EXPIRY_FOR_ENTRY,

            maker_max_entry_price:    config::MAKER_MAX_ENTRY_PRICE,
            maker_min_entry_price:    config::MAKER_MIN_ENTRY_PRICE,
            maker_stop_loss_pct:      config::MAKER_STOP_LOSS_PERCENT,
            maker_target_profit_pct:  config::MAKER_TARGET_PROFIT_PERCENT,
            maker_max_exposure_usdc:  config::MAKER_MAX_EXPOSURE_USDC,
            maker_quote_size_usdc:    config::MAKER_QUOTE_SIZE_USDC,
            maker_min_spread:              config::MAKER_MIN_SPREAD,
            maker_bid_buffer:              config::MAKER_BID_BUFFER,
            maker_cross_buffer:            config::MAKER_CROSS_BUFFER,
            maker_max_combined_bid:        config::MAKER_MAX_COMBINED_BID,
            maker_max_complementary_price: config::MAKER_MAX_COMPLEMENTARY_PRICE,
            maker_max_book_imbalance_ratio: config::MAKER_MAX_BOOK_IMBALANCE_RATIO,
            maker_min_secs_to_expiry:      config::MAKER_MIN_SECS_TO_EXPIRY,
            maker_toxic_flow_exit_obi:     config::MAKER_TOXIC_FLOW_EXIT_OBI,

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
            gboost_min_net_profit_usdc:   config::GBOOST_MIN_NET_PROFIT_USDC,
            gboost_min_secs_to_expiry:    config::GBOOST_MIN_SECS_TO_EXPIRY,
            gboost_signal_exit_threshold: config::GBOOST_SIGNAL_EXIT_THRESHOLD,

            trendcapture_min_trade_size_usdc: config::TRENDCAPTURE_MIN_TRADE_SIZE_USDC,
            trendcapture_max_trade_size_usdc: config::TRENDCAPTURE_MAX_TRADE_SIZE_USDC,
            trendcapture_max_exposure_usdc:   config::TRENDCAPTURE_MAX_EXPOSURE_USDC,
            trendcapture_stop_loss_pct:       config::TRENDCAPTURE_STOP_LOSS_PERCENT,
            trendcapture_target_profit_pct:   config::TRENDCAPTURE_TARGET_PROFIT_PERCENT,
            trendcapture_max_entry_price:     config::TRENDCAPTURE_MAX_ENTRY_PRICE,
            trendcapture_min_entry_price:      config::TRENDCAPTURE_MIN_ENTRY_PRICE,
            trendcapture_max_entry_ask_sum:    config::TRENDCAPTURE_MAX_ENTRY_ASK_SUM,
            trendcapture_obi_adverse_block:    config::TRENDCAPTURE_OBI_ADVERSE_BLOCK,
            trendcapture_obi_exhaustion_block: config::TRENDCAPTURE_OBI_EXHAUSTION_BLOCK,
            trendcapture_max_token_spread_pct: config::TRENDCAPTURE_MAX_TOKEN_SPREAD_PCT,
            trendcapture_reversal_drift_pct:   config::TRENDCAPTURE_REVERSAL_DRIFT_PCT,
            trendcapture_strike_gap_pct:       config::TRENDCAPTURE_STRIKE_GAP_PCT,
            trendcapture_take_profit_ceiling:  config::TRENDCAPTURE_TAKE_PROFIT_CEILING,
            trendcapture_catastrophic_sl_pct:  config::TRENDCAPTURE_CATASTROPHIC_SL_PCT,
            trendreversal_mode:                config::TRENDREVERSAL_MODE,

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
            convergence_skip_band_low:        config::CONVERGENCE_SKIP_BAND_LOW,
            convergence_skip_band_high:       config::CONVERGENCE_SKIP_BAND_HIGH,
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
fn read_only_mode() -> bool {
    std::env::var("DRADIS_READ_ONLY")
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
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
        if read_only_mode() {
            info!("⚙️  READ-ONLY demo mode — squadron {} using compile-time defaults", squadron_id);
            return Arc::new(DynamicConfig::default());
        }
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

        info!("⚙️  Squadron config hot-patched: {}", squadron_id);
        Ok(Arc::new(updated))
    }
}


