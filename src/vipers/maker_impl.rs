/// Maker Strategy - Two-Sided Market Making
///
/// Posts passive resting bids on BOTH YES and NO simultaneously, earning:
///   1. The spread when positions fill and converge to take-profit.
///   2. Daily USDC rebates from Polymarket's Maker Rebates program on every fill.
///
/// This version is strictly tied to the Window/Daily venue via a Fee Gate.
///
/// Trade Velocity / Taker-Flow Filter (Phase 9):
///   - evaluate_entry tracks bid-depth drain over a 1.5s window to suppress new
///     maker bids when takers are actively sweeping one side of the book.
///   - evaluate_exit fires an accelerated "ToxicFill" exit whenever an open
///     position's book OBI falls below MAKER_TOXIC_FLOW_EXIT_OBI, meaning
///     the ask side has grown 3× larger than the bid side — a book turn.

use async_trait::async_trait;
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Instant;
use std::collections::HashMap;
use tokio::sync::Mutex;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::vipers::is_drawdown_limit_hit;
use crate::config;
use crate::venues::core::TimeInForce;
use crate::helpers::price::floor_to_tick_size;
// Import OrderType

/// Tracks bid-depth at the previous evaluation tick for drain-rate computation.
struct DepthSample {
    yes_bid_depth: Decimal,
    no_bid_depth: Decimal,
    sampled_at: Instant,
}

/// Returns the first entry sub-gate a single side (YES or NO) fails, or `None`
/// if the side qualifies.  The first element is a STABLE category key (no live
/// numbers) used for log throttling; the second is a human-readable detail
/// string with the live values.  Keeping the throttle key stable prevents the
/// 50 ms tick loop from defeating the throttle via fluctuating price numbers.
fn side_reject_reason(
    book_ok: bool,
    taker_block: bool,
    toxic_cooldown: bool,
    spread: Decimal,
    bid_price: Decimal,
    ask: Decimal,
    complementary_bid: Decimal,
    velocity_block: bool,
    dc: &crate::helpers::dynamic_config::DynamicConfig,
) -> Option<(&'static str, String)> {
    if !book_ok {
        return Some(("book_imbalance", "book_imbalance".to_string()));
    }
    if taker_block {
        return Some(("taker_flow_drain", "taker_flow_drain".to_string()));
    }
    if toxic_cooldown {
        return Some(("toxic_cooldown", "toxic_cooldown (post-ToxicFill re-entry lockout)".to_string()));
    }
    if spread < dc.maker_min_spread {
        return Some(("spread", format!("spread {:.3} < min {:.3}", spread, dc.maker_min_spread)));
    }
    if bid_price < dc.maker_min_entry_price {
        return Some(("min_entry", format!("bid {:.3} < min_entry {:.3}", bid_price, dc.maker_min_entry_price)));
    }
    if bid_price > dc.maker_max_entry_price {
        return Some(("max_entry", format!("bid {:.3} > max_entry {:.3}", bid_price, dc.maker_max_entry_price)));
    }
    if bid_price > ask - dc.maker_cross_buffer {
        return Some(("cross_buffer", format!(
            "cross_buffer: bid {:.3} > ask {:.3} - {:.3}",
            bid_price, ask, dc.maker_cross_buffer
        )));
    }
    if complementary_bid > dc.maker_max_complementary_price {
        return Some(("complementary", format!(
            "complementary {:.3} > max {:.3}",
            complementary_bid, dc.maker_max_complementary_price
        )));
    }
    if velocity_block {
        return Some(("velocity_bias", "velocity_bias".to_string()));
    }
    None
}

pub struct MakerStrategyImpl {
    /// Per-strategy state: best-bid depths from the previous evaluation tick.
    /// Used to compute how fast bid depth is being consumed by takers within
    /// a MAKER_TAKER_FLOW_WINDOW_MS rolling window.
    /// Wrapped in Mutex because evaluate_entry and evaluate_exit run concurrently
    /// (tokio::join! in the executor).  evaluate_entry owns writes; evaluate_exit
    /// reads whatever sample is available (one-tick lag is acceptable for a gate).
    prev_depths: Mutex<Option<DepthSample>>,

    /// Gate-diagnostics throttle: (last reason logged, when).  We log a gate
    /// rejection whenever the reason changes OR MAKER_GATE_LOG_INTERVAL_SECS has
    /// elapsed, so the reason a maker is silent is visible without spamming every
    /// evaluation tick.
    last_gate_log: Mutex<Option<(String, Instant)>>,

    /// Throttle for the positive "quoting" log.  The eval loop runs on a ~50 ms
    /// tick, so an unthrottled quote log would emit ~20×/sec; this caps it to one
    /// line per MAKER_GATE_LOG_INTERVAL_SECS.
    last_quote_log: Mutex<Option<Instant>>,
}

/// Process-global market-maturation tracker: market identity → first time ANY
/// maker instance observed it.  Must be global (not per-strategy) because the
/// patrol loop rebuilds the strategy objects on every market rotation
/// (`create_all_strategies()`), which would otherwise wipe the baseline and
/// wrongly re-arm the 5-minute maturation blackout on a day-old daily maker
/// market each hour.  Keyed on `market_name` (stable for the daily maker venue
/// across hourly rotations; a genuinely new market gets a fresh entry, correctly
/// re-arming maturation).  Survives rotations within a process; re-arms once on a
/// full process restart (correct — a fresh process hasn't observed stability).
fn maker_market_first_seen() -> &'static std::sync::Mutex<HashMap<String, Instant>> {
    static REG: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Instant>>> =
        std::sync::OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Seconds since a maker market identity was first observed by any instance.
/// Returns 0 the first time an identity is seen (arming maturation) and grows
/// monotonically thereafter, surviving strategy re-instantiation on rotation.
fn market_age_secs(market_ident: &str) -> i64 {
    let mut reg = match maker_market_first_seen().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let first_seen = reg.entry(market_ident.to_string()).or_insert_with(Instant::now);
    first_seen.elapsed().as_secs() as i64
}

/// Process-global post-ToxicFill re-entry cooldown: token_id → the instant the
/// last ToxicFill exit fired for that token.  Global (not a per-strategy field)
/// for the same reason as `maker_market_first_seen`: patrol rebuilds the strategy
/// objects on every market rotation (`create_all_strategies()`), which would wipe
/// per-instance state and let the maker immediately re-quote into the very book it
/// was just picked off in.  Keyed on the specific YES/NO token so only the toxic
/// side is locked out; a genuinely new market gets a fresh token_id (no stale block).
fn maker_toxic_cooldowns() -> &'static std::sync::Mutex<HashMap<String, Instant>> {
    static REG: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Instant>>> =
        std::sync::OnceLock::new();
    REG.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Record that a ToxicFill exit just fired for `token_id`, arming the re-entry cooldown.
fn arm_maker_toxic_cooldown(token_id: &str) {
    let mut reg = match maker_toxic_cooldowns().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    reg.insert(token_id.to_string(), Instant::now());
}

/// Returns true if `token_id` is still within its post-ToxicFill re-entry cooldown
/// (i.e. fewer than `cooldown_secs` have elapsed since the last toxic exit).  A
/// non-positive `cooldown_secs` disables the gate.  Expired entries are pruned on read.
fn maker_toxic_cooldown_active(token_id: &str, cooldown_secs: i64) -> bool {
    if cooldown_secs <= 0 {
        return false;
    }
    let mut reg = match maker_toxic_cooldowns().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(t) = reg.get(token_id) {
        if (t.elapsed().as_secs() as i64) < cooldown_secs {
            return true;
        }
        reg.remove(token_id);
    }
    false
}

impl MakerStrategyImpl {
    pub fn new() -> Self {
        Self {
            prev_depths: Mutex::new(None),
            last_gate_log: Mutex::new(None),
            last_quote_log: Mutex::new(None),
        }
    }

    /// Throttled gate-rejection logger.  `key` is a STABLE category (no live
    /// numbers); `detail` carries the human-readable values.  Emits at INFO when
    /// `key` differs from the last logged key, or when MAKER_GATE_LOG_INTERVAL_SECS
    /// has passed since the last emit for the same key.  Throttling on the stable
    /// key (not the detail) keeps the 50 ms tick loop from flooding the log when
    /// live prices fluctuate.
    async fn log_gate(&self, key: &str, detail: &str) {
        let mut guard = self.last_gate_log.lock().await;
        let should_log = match guard.as_ref() {
            Some((prev_key, at)) => {
                prev_key != key
                    || at.elapsed().as_secs() >= config::MAKER_GATE_LOG_INTERVAL_SECS
            }
            None => true,
        };
        if should_log {
            tracing::info!("🔒 Maker gate: {}", detail);
            *guard = Some((key.to_string(), Instant::now()));
        }
    }
}

impl Default for MakerStrategyImpl {
    fn default() -> Self { Self::new() }
}

#[async_trait]
impl Strategy for MakerStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        if !dc.enable_maker {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global Risk Check ────────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Select venue: prefer maker_market (window/daily) ──────────────────
        let market = ctx.maker_market.as_ref().unwrap_or(&ctx.market);
        let snapshot = ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot);


        // ── Market maturation gate ────────────────────────────────────────────
        // Age is measured from when THIS maker market was first observed (keyed on
        // market_name), not ctx.market_started_at — the latter resets on every
        // hourly rotation and would wrongly re-arm the maturation blackout on a
        // day-old daily maker market each hour.
        let secs_since_market_start = market_age_secs(&market.market_name);
        if secs_since_market_start < config::MAKER_MIN_MARKET_AGE_SECS {
            self.log_gate("market_age", &format!(
                "market_age {}s < min {}s",
                secs_since_market_start, config::MAKER_MIN_MARKET_AGE_SECS
            )).await;
            return Ok(StrategySignal::NoSignal);
        }

        // ── Expiry gate ───────────────────────────────────────────────────────
        if let Some(close_time) = market.market_close_time {
            let secs_to_expiry = (close_time - Utc::now()).num_seconds();
            if secs_to_expiry < dc.maker_min_secs_to_expiry {
                self.log_gate("expiry", &format!(
                    "secs_to_expiry {}s < min {}s",
                    secs_to_expiry, dc.maker_min_secs_to_expiry
                )).await;
                return Ok(StrategySignal::NoSignal);
            }
        } else {
            self.log_gate("no_close_time", "no market_close_time").await;
            return Ok(StrategySignal::NoSignal);
        }

        let yes_bid = snapshot.yes_bid;
        let yes_ask = snapshot.yes_ask;
        let no_bid  = snapshot.no_bid;
        let no_ask  = snapshot.no_ask;

        // ── Orderbook imbalance gate ──────────────────────────────────────────
        let yes_book_ok = snapshot.yes_bid_depth > dec!(0)
            && (snapshot.yes_ask_depth / snapshot.yes_bid_depth) <= dc.maker_max_book_imbalance_ratio;
        let no_book_ok  = snapshot.no_bid_depth > dec!(0)
            && (snapshot.no_ask_depth  / snapshot.no_bid_depth)  <= dc.maker_max_book_imbalance_ratio;

        if !yes_book_ok && !no_book_ok {
            self.log_gate("book_imbalance", &format!(
                "book_imbalance both sides (ratio>{:.1}): yes_bidD={:.0} yes_askD={:.0} | no_bidD={:.0} no_askD={:.0}",
                dc.maker_max_book_imbalance_ratio,
                snapshot.yes_bid_depth, snapshot.yes_ask_depth,
                snapshot.no_bid_depth, snapshot.no_ask_depth
            )).await;
            return Ok(StrategySignal::NoSignal);
        }

        // ── Taker-Flow / Bid-Depth Drain Gate ────────────────────────────────
        // Measure the fraction of best-bid depth consumed since the last tick.
        // A rapid drain (≥ MAKER_TAKER_FLOW_DRAIN_THRESHOLD within
        // MAKER_TAKER_FLOW_WINDOW_MS) indicates that takers are sweeping the book
        // one-sidedly — classic "toxic flow" that fills maker bids at adverse prices.
        // Suppress the affected side so we don't post into an active sweep.
        let (taker_flow_blocks_yes, taker_flow_blocks_no) = {
            let now_inst = Instant::now();
            let mut prev_guard = self.prev_depths.lock().await;

            let drain_flags = if let Some(ref p) = *prev_guard {
                let elapsed_ms = now_inst.duration_since(p.sampled_at).as_millis();
                // Only measure drain when the sample is both fresh (≤ WINDOW) and old enough
                // (≥ MIN_ELAPSED) to span multiple WS ticks.  Single-tick (49ms) comparisons
                // produce false positives from best-bid price-level rotation on thin books.
                if elapsed_ms >= config::MAKER_TAKER_FLOW_MIN_ELAPSED_MS as u128
                    && elapsed_ms <= config::MAKER_TAKER_FLOW_WINDOW_MS as u128
                {
                    // Positive value = depth decreased (bids were lifted by takers).
                    // Clamp at 0 so depth replenishment (depth increased) never triggers the gate.
                    let yes_drain = if p.yes_bid_depth > dec!(0) {
                        ((p.yes_bid_depth - snapshot.yes_bid_depth) / p.yes_bid_depth).max(dec!(0))
                    } else {
                        dec!(0)
                    };
                    let no_drain = if p.no_bid_depth > dec!(0) {
                        ((p.no_bid_depth - snapshot.no_bid_depth) / p.no_bid_depth).max(dec!(0))
                    } else {
                        dec!(0)
                    };

                    let block_yes = yes_drain >= config::MAKER_TAKER_FLOW_DRAIN_THRESHOLD;
                    let block_no  = no_drain  >= config::MAKER_TAKER_FLOW_DRAIN_THRESHOLD;

                    if block_yes {
                        tracing::info!(
                            "🚫 Maker YES entry suppressed: bid-depth drained {:.0}% in {}ms (taker sweep detected)",
                            yes_drain * dec!(100), elapsed_ms
                        );
                    }
                    if block_no {
                        tracing::info!(
                            "🚫 Maker NO entry suppressed: bid-depth drained {:.0}% in {}ms (taker sweep detected)",
                            no_drain * dec!(100), elapsed_ms
                        );
                    }

                    (block_yes, block_no)
                } else {
                    (false, false)
                }
            } else {
                (false, false)
            };

            // Store current depths for the next tick's comparison.
            // This write is owned by evaluate_entry; evaluate_exit only reads.
            *prev_guard = Some(DepthSample {
                yes_bid_depth: snapshot.yes_bid_depth,
                no_bid_depth:  snapshot.no_bid_depth,
                sampled_at:    now_inst,
            });

            drain_flags
        };

        // ── Inventory and Net Exposure Check ─────────────────────────────────
        let (yes_inv_value, no_inv_value) = {
            let pos_map = ctx.positions.lock().await;
            let yv = pos_map.get(&("MakerStrategy".to_string(), market.yes_token.clone()))
                .map(|p| p.shares * p.avg_entry).unwrap_or(dec!(0));
            let nv = pos_map.get(&("MakerStrategy".to_string(), market.no_token.clone()))
                .map(|p| p.shares * p.avg_entry).unwrap_or(dec!(0));
            (yv, nv)
        };

        // Skew calculation
        let imbalance = ((yes_inv_value - no_inv_value) / dc.maker_max_exposure_usdc)
            .clamp(dec!(-1), dec!(1));
        let skew = imbalance * config::MAKER_INVENTORY_SKEW_MAX;

        // Velocity bias from hourly oracle (always)
        let velocity = ctx.snapshot.velocity;
        let velocity_bias_strong_negative = velocity <= -config::MAKER_VELOCITY_BIAS_THRESHOLD;
        let velocity_bias_strong_positive = velocity >= config::MAKER_VELOCITY_BIAS_THRESHOLD;

        // ── Pricing Logic ─────────────────────────────────────────────────────
        // Use a wider buffer to avoid long-unfilled GTC orders in slower books
        let bid_buffer = if ctx.maker_market.is_some() { dc.maker_bid_buffer } else { dec!(0.015) };

        let raw_yes_price = (snapshot.yes_ask - bid_buffer - skew).max(dc.maker_min_entry_price);
        let raw_no_price  = (snapshot.no_ask - bid_buffer + skew).max(dc.maker_min_entry_price);

        // Clamp bid price to at most (ask - MAKER_CROSS_BUFFER) so that inventory-skew
        // rebalancing can never push the bid closer than 2 ticks from the ask.
        // Previously used a hardcoded dec!(0.01) which allowed 1-tick spreads when
        // the skew (±0.03) exceeded the bid_buffer (0.025), triggering the cap.
        // Now uses the configured MAKER_CROSS_BUFFER constant (0.02) for consistency.
        let yes_bid_price = floor_to_tick_size(raw_yes_price.min(snapshot.yes_ask - dc.maker_cross_buffer));
        let no_bid_price  = floor_to_tick_size(raw_no_price.min(snapshot.no_ask  - dc.maker_cross_buffer));

        let yes_spread = yes_ask - yes_bid;
        let no_spread  = no_ask - no_bid;

        // ── Qualification ─────────────────────────────────────────────────
        // Post-ToxicFill re-entry lockout: if this token was picked off by a
        // book-turn within the last `maker_toxic_reentry_cooldown_secs`, do not
        // re-quote that side — avoids catching the same falling knife twice.
        let cooldown_secs = dc.maker_toxic_reentry_cooldown_secs;
        let yes_toxic_cooldown = maker_toxic_cooldown_active(market.yes_token.as_str(), cooldown_secs);
        let no_toxic_cooldown  = maker_toxic_cooldown_active(market.no_token.as_str(), cooldown_secs);

        let yes_qualifies = yes_book_ok
            && !taker_flow_blocks_yes
            && !yes_toxic_cooldown
            && yes_spread >= dc.maker_min_spread
            && yes_bid_price >= dc.maker_min_entry_price
            && yes_bid_price <= dc.maker_max_entry_price
            && yes_bid_price <= snapshot.yes_ask - dc.maker_cross_buffer
            && no_bid <= dc.maker_max_complementary_price
            && !velocity_bias_strong_negative;

        let no_qualifies = no_book_ok
            && !taker_flow_blocks_no
            && !no_toxic_cooldown
            && no_spread >= dc.maker_min_spread
            && no_bid_price >= dc.maker_min_entry_price
            && no_bid_price <= dc.maker_max_entry_price
            && no_bid_price <= snapshot.no_ask - dc.maker_cross_buffer
            && yes_bid <= dc.maker_max_complementary_price
            && !velocity_bias_strong_positive;

        if !yes_qualifies && !no_qualifies {
            let (yes_key, yes_detail) = side_reject_reason(
                yes_book_ok, taker_flow_blocks_yes, yes_toxic_cooldown, yes_spread, yes_bid_price,
                snapshot.yes_ask, no_bid, velocity_bias_strong_negative, dc,
            ).unwrap_or(("unknown", "unknown".to_string()));
            let (no_key, no_detail) = side_reject_reason(
                no_book_ok, taker_flow_blocks_no, no_toxic_cooldown, no_spread, no_bid_price,
                snapshot.no_ask, yes_bid, velocity_bias_strong_positive, dc,
            ).unwrap_or(("unknown", "unknown".to_string()));
            self.log_gate(
                &format!("noqual:{}/{}", yes_key, no_key),
                &format!("no side qualifies | YES: {} | NO: {}", yes_detail, no_detail),
            ).await;
            return Ok(StrategySignal::NoSignal);
        }

        // ── Net Exposure Risk Check ──────────────────────────────────────────
        // Quote size is config-driven and clamped to the exposure cap so a single
        // quote from a flat book always fits under the limit (a quote larger than
        // the cap would self-gate the maker after one clip).
        let trade_size = dc.maker_quote_size_usdc.min(dc.maker_max_exposure_usdc);
        let projected_yes = yes_inv_value + (if yes_qualifies { trade_size } else { dec!(0.0) });
        let projected_no  = no_inv_value  + (if no_qualifies { trade_size } else { dec!(0.0) });
        let net_exposure  = (projected_yes - projected_no).abs();

        if net_exposure > dc.maker_max_exposure_usdc {
            self.log_gate("net_exposure", &format!(
                "net_exposure ${:.2} > max ${:.2}",
                net_exposure, dc.maker_max_exposure_usdc
            )).await;
            return Ok(StrategySignal::NoSignal);
        }

        // ── Combined price guard ──────────────────────────────────────────────
        let (final_yes, final_no) = if yes_qualifies && no_qualifies {
            let combined = yes_bid_price + no_bid_price;
            if combined >= dc.maker_max_combined_bid {
                if yes_spread <= no_spread { (None, Some(no_bid_price)) } else { (Some(yes_bid_price), None) }
            } else {
                (Some(yes_bid_price), Some(no_bid_price))
            }
        } else if yes_qualifies {
            (Some(yes_bid_price), None)
        } else {
            (None, Some(no_bid_price))
        };

        if final_yes.is_none() && final_no.is_none() {
            self.log_gate("combined_bid", "combined_bid guard suppressed both sides").await;
            return Ok(StrategySignal::NoSignal);
        }

        // ── Build detailed signals ───────────────────────────────────────────
        // Maker (post-only) orders are NEVER charged a taker fee by the CLOB —
        // the feeRateBps field is an EIP-712 struct attribute required by the API
        // but it is NOT deducted from maker fills.  Pass 0 so our P&L math is correct.
        let yes_params = final_yes.map(|p| OrderParams {
            token_id: market.yes_token.clone(),
            price: p,
            shares: trade_size / p,
            fee_bps: 0,
            is_neg_risk: market.is_neg_risk,
            market_name: market.market_name.clone(),
            condition_id: market.condition_id.clone(),
            order_type: TimeInForce::Gtc,
            post_only: true,
            ghost_mode: dc.ghost_mode,
        });

        let no_params = final_no.map(|p| OrderParams {
            token_id: market.no_token.clone(),
            price: p,
            shares: trade_size / p,
            fee_bps: 0,
            is_neg_risk: market.is_neg_risk,
            market_name: market.market_name.clone(),
            condition_id: market.condition_id.clone(),
            order_type: TimeInForce::Gtc,
            post_only: true,
            ghost_mode: dc.ghost_mode,
        });

        {
            let mut guard = self.last_quote_log.lock().await;
            let due = guard.map_or(true, |t| t.elapsed().as_secs() >= config::MAKER_GATE_LOG_INTERVAL_SECS);
            if due {
                tracing::info!(
                    "✅ Maker quoting: YES={} NO={}",
                    yes_params.as_ref().map(|p| format!("${:.3}", p.price)).unwrap_or_else(|| "—".to_string()),
                    no_params.as_ref().map(|p| format!("${:.3}", p.price)).unwrap_or_else(|| "—".to_string()),
                );
                *guard = Some(Instant::now());
            }
        }

        Ok(StrategySignal::MakerQuote {
            yes: yes_params,
            no: no_params,
        })
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        let market = ctx.maker_market.as_ref().unwrap_or(&ctx.market);
        let snapshot = ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot);

        let secs_to_expiry = market.market_close_time
            .map(|t| (t - Utc::now()).num_seconds())
            .unwrap_or(9999);

        // Near-expiry forced exit to avoid binary resolution risk.
        //
        // 2026-07-14: fires at maker_min_secs_to_expiry (1800s) instead of a hardcoded
        // 900s. Quoting already stops at 1800s, so the old 900s threshold left a
        // 15-minute dead zone holding pure directional risk with no new quotes — a
        // YES @ $0.48 rode through it to resolution at $0.004 (−$3.81, the single
        // largest maker loss). Flatten the moment the strategy stops quoting.
        let profit_threshold = dec!(0.02);
        if secs_to_expiry < dc.maker_min_secs_to_expiry {
            let pos_map = ctx.positions.lock().await;
            for token_id in [market.yes_token.clone(), market.no_token.clone()] {
                if let Some(position) = pos_map.get(&("MakerStrategy".to_string(), token_id.clone())) {
                    let bid = if token_id == market.yes_token { snapshot.yes_bid } else { snapshot.no_bid };
                    let profit_pct = (bid - position.avg_entry) / position.avg_entry;
                    if profit_pct < profit_threshold {
                        return Ok(StrategySignal::Exit {
                            params: OrderParams {
                                token_id: token_id.clone(),                                price: bid,
                                shares: position.shares,
                                fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                                is_neg_risk: market.is_neg_risk,
                                market_name: market.market_name.clone(),
                                condition_id: market.condition_id.clone(),
                                order_type: TimeInForce::Fak,
                                post_only: false,
                                ghost_mode: dc.ghost_mode,
                            },
                            reason: "NearExpiryProfitGuard".to_string(),
                            exit_pair: false,
                        });
                    }
                }
            }
        }

        let effective_stop_pct = if secs_to_expiry < config::MAKER_LATE_MARKET_STOP_TIGHTEN_SECS {
            config::MAKER_LATE_MARKET_STOP_LOSS_PERCENT
        } else {
            dc.maker_stop_loss_pct
        };

        // ── Taker-Flow Book-Turn Exit / Quote-Pull ────────────────────────────
        // A book that has turned adverse (OBI below the toxic threshold) is a
        // one-way sweep. For a FILLED position we exit at the bid (ToxicFill). For
        // an UNFILLED resting quote we cancel it (reactive quote-pull) so it isn't
        // left on the book to be picked off by the informed flow — the exact
        // adverse-selection mechanism behind the noon-ET maker losses.
        {
            let pos_map = ctx.positions.lock().await;
            let mut pull_tokens = Vec::new();
            for token_id in [market.yes_token.clone(), market.no_token.clone()] {
                let Some(position) = pos_map.get(&("MakerStrategy".to_string(), token_id.clone())) else {
                    continue;
                };

                let (bid_depth, ask_depth, bid) = if token_id == market.yes_token {
                    (snapshot.yes_bid_depth, snapshot.yes_ask_depth, snapshot.yes_bid)
                } else {
                    (snapshot.no_bid_depth, snapshot.no_ask_depth, snapshot.no_bid)
                };

                let total_depth = bid_depth + ask_depth;
                let obi = if total_depth > dec!(0) {
                    (bid_depth - ask_depth) / total_depth
                } else { dec!(0) };

                if obi < dc.maker_toxic_flow_exit_obi {
                    arm_maker_toxic_cooldown(token_id.as_str());
                    if position.fill_confirmed_at.is_some() {
                        tracing::info!(
                            "⚡ Maker ToxicFill exit triggered: OBI={:.2} (threshold={:.2}) | bid=${:.4} | re-entry locked {}s",
                            obi, dc.maker_toxic_flow_exit_obi, bid, dc.maker_toxic_reentry_cooldown_secs
                        );
                        return Ok(StrategySignal::Exit {
                            params: OrderParams {
                                token_id: token_id.clone(),
                                price: bid,
                                shares: position.shares,
                                fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                                is_neg_risk: market.is_neg_risk,
                                market_name: market.market_name.clone(),
                                condition_id: market.condition_id.clone(),
                                order_type: TimeInForce::Fak,
                                post_only: false,
                                ghost_mode: dc.ghost_mode,
                            },
                            reason: format!("ToxicFill: OBI={:.2} (book turned adverse)", obi),
                            exit_pair: false,
                        });
                    } else {
                        // Unfilled resting quote sitting in a toxic book → pull it.
                        pull_tokens.push(token_id.clone());
                    }
                }
            }
            if !pull_tokens.is_empty() {
                tracing::info!(
                    "⚡ Maker quote-pull: cancelling {} unfilled resting quote(s) — book turned toxic (OBI < {:.2}), re-entry locked {}s",
                    pull_tokens.len(), dc.maker_toxic_flow_exit_obi, dc.maker_toxic_reentry_cooldown_secs
                );
                return Ok(StrategySignal::MakerCancel { tokens: pull_tokens });
            }
        }

        let pos_map = ctx.positions.lock().await;

        for token_id in [market.yes_token.clone(), market.no_token.clone()] {
            let Some(position) = pos_map.get(&("MakerStrategy".to_string(), token_id.clone())) else {
                continue;
            };

            let bid = if token_id == market.yes_token { snapshot.yes_bid } else { snapshot.no_bid };
            if position.avg_entry <= dec!(0) { continue; }

            let profit_pct = (bid - position.avg_entry) / position.avg_entry;
            let secs_since_fill = position.fill_confirmed_at
                .map(|t| (Utc::now() - t).num_seconds())
                .unwrap_or(0);

            // Catastrophic floor — ungated by the min-hold and by fill confirmation,
            // so a fast adverse move during the stop's blind window (or on an adopted
            // position) is still cut. See MAKER_CATASTROPHIC_SL_MULT.
            let catastrophic_pct = effective_stop_pct * config::MAKER_CATASTROPHIC_SL_MULT;
            if profit_pct <= -catastrophic_pct {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token_id.clone(),
                        price: bid,
                        shares: position.shares,
                        fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!("Maker Catastrophic: loss={:.2}% ({}s held)", profit_pct * dec!(100), secs_since_fill),
                    exit_pair: false,
                });
            }

            if position.fill_confirmed_at.is_some() && profit_pct >= dc.maker_target_profit_pct {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token_id.clone(),                        price: bid,
                        shares: position.shares,
                        fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!("Maker TP: gain={:.2}%", profit_pct * dec!(100)),
                    exit_pair: false,
                });
            }

            if position.fill_confirmed_at.is_some()
                && secs_since_fill >= config::MAKER_MIN_HOLD_SECS_BEFORE_STOP
                && profit_pct <= -effective_stop_pct
            {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token_id.clone(),                        price: bid,
                        shares: position.shares,
                        fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!("Maker SL: loss={:.2}% ({}s held)", profit_pct * dec!(100), secs_since_fill),
                    exit_pair: false,
                });
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }

    fn name(&self) -> String { "MakerStrategy".to_string() }
    fn venue(&self) -> &'static str { "Window/Daily" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::MAKER_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Net |YES-NO|" }
}

#[cfg(test)]
mod toxic_cooldown_tests {
    use super::{arm_maker_toxic_cooldown, maker_toxic_cooldown_active};

    #[test]
    fn armed_token_is_locked_out() {
        let tok = "test_tok_armed_lockout";
        assert!(!maker_toxic_cooldown_active(tok, 180));
        arm_maker_toxic_cooldown(tok);
        assert!(maker_toxic_cooldown_active(tok, 180));
    }

    #[test]
    fn unknown_token_is_not_locked() {
        assert!(!maker_toxic_cooldown_active("test_tok_never_armed", 180));
    }

    #[test]
    fn zero_or_negative_cooldown_disables_gate() {
        let tok = "test_tok_disabled_gate";
        arm_maker_toxic_cooldown(tok);
        assert!(!maker_toxic_cooldown_active(tok, 0));
        assert!(!maker_toxic_cooldown_active(tok, -5));
    }

    #[test]
    fn expired_cooldown_allows_reentry() {
        let tok = "test_tok_expired";
        arm_maker_toxic_cooldown(tok);
        // Immediately after arming, a sub-second window has already elapsed
        // (elapsed secs = 0 is NOT < 0), so the token reads as no-longer-locked
        // and is pruned on read. Use cooldown_secs so small it is already expired.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(!maker_toxic_cooldown_active(tok, 1));
    }
}
