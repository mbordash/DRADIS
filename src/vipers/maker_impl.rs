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

pub struct MakerStrategyImpl {
    /// Per-strategy state: best-bid depths from the previous evaluation tick.
    /// Used to compute how fast bid depth is being consumed by takers within
    /// a MAKER_TAKER_FLOW_WINDOW_MS rolling window.
    /// Wrapped in Mutex because evaluate_entry and evaluate_exit run concurrently
    /// (tokio::join! in the executor).  evaluate_entry owns writes; evaluate_exit
    /// reads whatever sample is available (one-tick lag is acceptable for a gate).
    prev_depths: Mutex<Option<DepthSample>>,
}

impl MakerStrategyImpl {
    pub fn new() -> Self {
        Self { prev_depths: Mutex::new(None) }
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
        let secs_since_market_start = (Utc::now() - ctx.market_started_at).num_seconds();
        if secs_since_market_start < config::MAKER_MIN_MARKET_AGE_SECS {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Expiry gate ───────────────────────────────────────────────────────
        if let Some(close_time) = market.market_close_time {
            if (close_time - Utc::now()).num_seconds() < dc.maker_min_secs_to_expiry {
                return Ok(StrategySignal::NoSignal);
            }
        } else {
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
        let yes_qualifies = yes_book_ok
            && !taker_flow_blocks_yes
            && yes_spread >= dc.maker_min_spread
            && yes_bid_price >= dc.maker_min_entry_price
            && yes_bid_price <= dc.maker_max_entry_price
            && yes_bid_price <= snapshot.yes_ask - dc.maker_cross_buffer
            && no_bid <= dc.maker_max_complementary_price
            && !velocity_bias_strong_negative;

        let no_qualifies = no_book_ok
            && !taker_flow_blocks_no
            && no_spread >= dc.maker_min_spread
            && no_bid_price >= dc.maker_min_entry_price
            && no_bid_price <= dc.maker_max_entry_price
            && no_bid_price <= snapshot.no_ask - dc.maker_cross_buffer
            && yes_bid <= dc.maker_max_complementary_price
            && !velocity_bias_strong_positive;

        if !yes_qualifies && !no_qualifies {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Net Exposure Risk Check ──────────────────────────────────────────
        let trade_size = dec!(10.0);
        let projected_yes = yes_inv_value + (if yes_qualifies { trade_size } else { dec!(0.0) });
        let projected_no  = no_inv_value  + (if no_qualifies { trade_size } else { dec!(0.0) });
        let net_exposure  = (projected_yes - projected_no).abs();

        if net_exposure > dc.maker_max_exposure_usdc {
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

        // Near-expiry forced exit to avoid binary resolution risk
        let profit_threshold = dec!(0.02);
        if secs_to_expiry < 900 {
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

        // ── Taker-Flow Book-Turn Exit ─────────────────────────────────────────
        {
            let pos_map = ctx.positions.lock().await;
            for token_id in [market.yes_token.clone(), market.no_token.clone()] {
                let Some(position) = pos_map.get(&("MakerStrategy".to_string(), token_id.clone())) else {
                    continue;
                };
                if position.fill_confirmed_at.is_none() { continue; }

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
                    tracing::info!(
                        "⚡ Maker ToxicFill exit triggered: OBI={:.2} (threshold={:.2}) | bid=${:.4}",
                        obi, dc.maker_toxic_flow_exit_obi, bid
                    );
                    return Ok(StrategySignal::Exit {
                        params: OrderParams {
                            token_id: token_id.clone(),                            price: bid,
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
                }
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
