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
use crate::strategies::is_drawdown_limit_hit;
use crate::helpers::price::floor_to_tick_size;
use crate::config;

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
        if !config::ENABLE_MAKER_TRADING {
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
            if (close_time - Utc::now()).num_seconds() < config::MAKER_MIN_SECS_TO_EXPIRY {
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
            && (snapshot.yes_ask_depth / snapshot.yes_bid_depth) <= config::MAKER_MAX_BOOK_IMBALANCE_RATIO;
        let no_book_ok  = snapshot.no_bid_depth > dec!(0)
            && (snapshot.no_ask_depth  / snapshot.no_bid_depth)  <= config::MAKER_MAX_BOOK_IMBALANCE_RATIO;

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
                if elapsed_ms > 0 && elapsed_ms <= config::MAKER_TAKER_FLOW_WINDOW_MS as u128 {
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
            let yv = pos_map.get(&("MakerStrategy".to_string(), market.yes_token))
                .map(|p| p.shares * p.avg_entry).unwrap_or(dec!(0));
            let nv = pos_map.get(&("MakerStrategy".to_string(), market.no_token))
                .map(|p| p.shares * p.avg_entry).unwrap_or(dec!(0));
            (yv, nv)
        };

        // Skew calculation
        let imbalance = ((yes_inv_value - no_inv_value) / config::MAKER_MAX_EXPOSURE_USDC)
            .clamp(dec!(-1), dec!(1));
        let skew = imbalance * config::MAKER_INVENTORY_SKEW_MAX;

        // Velocity bias from hourly oracle (always)
        let velocity = ctx.snapshot.velocity;
        let velocity_bias_strong_negative = velocity <= -config::MAKER_VELOCITY_BIAS_THRESHOLD;
        let velocity_bias_strong_positive = velocity >= config::MAKER_VELOCITY_BIAS_THRESHOLD;

        // ── Pricing Logic ─────────────────────────────────────────────────────
        // Use a wider buffer to avoid long-unfilled GTC orders in slower books
        let bid_buffer = if ctx.maker_market.is_some() { config::MAKER_BID_BUFFER } else { dec!(0.015) };

        let raw_yes_price = (snapshot.yes_ask - bid_buffer - skew).max(config::MAKER_MIN_ENTRY_PRICE);
        let raw_no_price  = (snapshot.no_ask - bid_buffer + skew).max(config::MAKER_MIN_ENTRY_PRICE);

        // Clamp bid price to at most (ask - MAKER_CROSS_BUFFER) so that inventory-skew
        // rebalancing can never push the bid closer than 2 ticks from the ask.
        // Previously used a hardcoded dec!(0.01) which allowed 1-tick spreads when
        // the skew (±0.03) exceeded the bid_buffer (0.025), triggering the cap.
        // Now uses the configured MAKER_CROSS_BUFFER constant (0.02) for consistency.
        let yes_bid_price = floor_to_tick_size(raw_yes_price.min(snapshot.yes_ask - config::MAKER_CROSS_BUFFER));
        let no_bid_price  = floor_to_tick_size(raw_no_price.min(snapshot.no_ask  - config::MAKER_CROSS_BUFFER));

        let yes_spread = yes_ask - yes_bid;
        let no_spread  = no_ask - no_bid;

        // ── Qualification ─────────────────────────────────────────────────────
        let yes_qualifies = yes_book_ok
            && !taker_flow_blocks_yes          // taker-flow drain gate
            && yes_spread >= config::MAKER_MIN_SPREAD
            && yes_bid_price >= config::MAKER_MIN_ENTRY_PRICE
            && yes_bid_price <= config::MAKER_MAX_ENTRY_PRICE
            && yes_bid_price <= snapshot.yes_ask - config::MAKER_CROSS_BUFFER  // enforce min spread to ask
            && no_bid <= config::MAKER_MAX_COMPLEMENTARY_PRICE
            && !velocity_bias_strong_negative;

        let no_qualifies = no_book_ok
            && !taker_flow_blocks_no           // taker-flow drain gate
            && no_spread >= config::MAKER_MIN_SPREAD
            && no_bid_price >= config::MAKER_MIN_ENTRY_PRICE
            && no_bid_price <= config::MAKER_MAX_ENTRY_PRICE
            && no_bid_price <= snapshot.no_ask - config::MAKER_CROSS_BUFFER    // enforce min spread to ask
            && yes_bid <= config::MAKER_MAX_COMPLEMENTARY_PRICE
            && !velocity_bias_strong_positive;

        if !yes_qualifies && !no_qualifies {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Net Exposure Risk Check ──────────────────────────────────────────
        let trade_size = dec!(10.0);
        let projected_yes = yes_inv_value + (if yes_qualifies { trade_size } else { dec!(0.0) });
        let projected_no  = no_inv_value  + (if no_qualifies { trade_size } else { dec!(0.0) });
        let net_exposure  = (projected_yes - projected_no).abs();

        if net_exposure > config::MAKER_MAX_EXPOSURE_USDC {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Combined price guard ──────────────────────────────────────────────
        let (final_yes, final_no) = if yes_qualifies && no_qualifies {
            let combined = yes_bid_price + no_bid_price;
            if combined >= config::MAKER_MAX_COMBINED_BID {
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
            token_id: market.yes_token,
            price: p,
            shares: trade_size / p,
            fee_bps: 0,
            is_neg_risk: market.is_neg_risk,
            market_name: market.market_name.clone(),
            condition_id: market.condition_id.clone(),
        });

        let no_params = final_no.map(|p| OrderParams {
            token_id: market.no_token,
            price: p,
            shares: trade_size / p,
            fee_bps: 0,
            is_neg_risk: market.is_neg_risk,
            market_name: market.market_name.clone(),
            condition_id: market.condition_id.clone(),
        });

        Ok(StrategySignal::MakerQuote {
            yes: yes_params,
            no: no_params,
        })
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let market = ctx.maker_market.as_ref().unwrap_or(&ctx.market);
        let snapshot = ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot);

        let secs_to_expiry = market.market_close_time
            .map(|t| (t - Utc::now()).num_seconds())
            .unwrap_or(9999);

        // Near-expiry forced exit to avoid binary resolution risk
        let profit_threshold = dec!(0.02); // 2% minimum profit to hold through the danger zone
        if secs_to_expiry < 900 { // 15 minutes before close
            let pos_map = ctx.positions.lock().await;
            for token_id in [market.yes_token, market.no_token] {
                if let Some(position) = pos_map.get(&("MakerStrategy".to_string(), token_id)) {
                    let bid = if token_id == market.yes_token { snapshot.yes_bid } else { snapshot.no_bid };
                    let profit_pct = (bid - position.avg_entry) / position.avg_entry;
                    if profit_pct < profit_threshold {
                        return Ok(StrategySignal::Exit {
                            params: OrderParams { token_id, price: bid, shares: position.shares, fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 }, is_neg_risk: market.is_neg_risk, market_name: market.market_name.clone(), condition_id: market.condition_id.clone() },
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
            config::MAKER_STOP_LOSS_PERCENT
        };

        // ── Taker-Flow Book-Turn Exit ─────────────────────────────────────────
        // If the OBI for the side we are long drops below MAKER_TOXIC_FLOW_EXIT_OBI,
        // the book has turned sharply adverse — a thick ask wall has materialised
        // while we hold the position.  This is the "pull existing orders" effect:
        // rather than waiting for the full stop-loss to hit, we exit early at the
        // current (still tradeable) bid before the spread widens further.
        // Only fires on fill-confirmed positions to avoid exiting phantom entries.
        {
            let pos_map = ctx.positions.lock().await;
            for token_id in [market.yes_token, market.no_token] {
                let Some(position) = pos_map.get(&("MakerStrategy".to_string(), token_id)) else {
                    continue;
                };
                if position.fill_confirmed_at.is_none() {
                    continue; // don't pull before the fill settles on-chain
                }

                let (bid_depth, ask_depth, bid) = if token_id == market.yes_token {
                    (snapshot.yes_bid_depth, snapshot.yes_ask_depth, snapshot.yes_bid)
                } else {
                    (snapshot.no_bid_depth, snapshot.no_ask_depth, snapshot.no_bid)
                };

                let total_depth = bid_depth + ask_depth;
                let obi = if total_depth > dec!(0) {
                    (bid_depth - ask_depth) / total_depth
                } else {
                    dec!(0)
                };

                if obi < config::MAKER_TOXIC_FLOW_EXIT_OBI {
                    tracing::info!(
                        "⚡ Maker ToxicFill exit triggered: OBI={:.2} (threshold={:.2}) | bid=${:.4}",
                        obi, config::MAKER_TOXIC_FLOW_EXIT_OBI, bid
                    );
                    return Ok(StrategySignal::Exit {
                        params: OrderParams {
                            token_id,
                            price: bid,
                            shares: position.shares,
                            fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                            is_neg_risk: market.is_neg_risk,
                            market_name: market.market_name.clone(),
                            condition_id: market.condition_id.clone(),
                        },
                        reason: format!("ToxicFill: OBI={:.2} (book turned adverse)", obi),
                        exit_pair: false,
                    });
                }
            }
        }

        let pos_map = ctx.positions.lock().await;

        for token_id in [market.yes_token, market.no_token] {
            let Some(position) = pos_map.get(&("MakerStrategy".to_string(), token_id)) else {
                continue;
            };

            let bid = if token_id == market.yes_token { snapshot.yes_bid } else { snapshot.no_bid };
            if position.avg_entry <= dec!(0) { continue; }

            let profit_pct = (bid - position.avg_entry) / position.avg_entry;
            let secs_since_fill = position.fill_confirmed_at
                .map(|t| (Utc::now() - t).num_seconds())
                .unwrap_or(0);

            if position.fill_confirmed_at.is_some() && profit_pct >= config::MAKER_TARGET_PROFIT_PERCENT {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id,
                        price: bid,
                        shares: position.shares,
                        fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
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
                        token_id,
                        price: bid,
                        shares: position.shares,
                        fee_bps: if token_id == market.yes_token { market.yes_fee_bps as u16 } else { market.no_fee_bps as u16 },
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                    },
                    reason: format!("Maker SL: loss={:.2}% ({}s held)", profit_pct * dec!(100), secs_since_fill),
                    exit_pair: false,
                });
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus {
        if config::ENABLE_MAKER_TRADING { StrategyStatus::Active } else { StrategyStatus::Disabled }
    }

    fn name(&self) -> String { "MakerStrategy".to_string() }
}
