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

/// Basis / Funding-Rate Mean-Reversion Strategy
///
/// # Thesis
///
/// Polymarket markets frequently exhibit **retail skew**:
/// bettors systematically over-bet one side, pushing its implied probability
/// above what Binance spot actually justifies.
///
/// This version is tied to the **Window/Maker venue** to take advantage of
/// significantly lower taker fees (0-200 bps vs 1000 bps on Hourly).
///
/// # Entry conditions
/// 1. Use maker_market (Window/Daily) if available, fallback to Hourly.
/// 2. YES mid-price > 0.50 + BASIS_ENTRY_SKEW_THRESHOLD (retail over-bet)
/// 3. Binance velocity.abs() < BASIS_MAX_VELOCITY (price isn't running)
/// 4. funding_rate aligns with fade OR extreme skew bypass
/// 5. taker fee <= BASIS_MAX_TAKER_FEE_BPS

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use chrono::Utc;

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::vipers::is_drawdown_limit_hit;
use crate::config;
use crate::venues::core::TimeInForce;

pub struct BasisStrategyImpl {
    /// Stop-loss tally per token: (consecutive SL count, time of last SL).
    /// Feeds the post-loss lockout gate — after N stops on one token, entries
    /// on that token are blocked for the lockout window. A take-profit or
    /// rejected exit order clears/decrements the tally.
    loss_book: Mutex<HashMap<String, (i64, Instant)>>,
}

impl BasisStrategyImpl {
    pub fn new() -> Self {
        Self { loss_book: Mutex::new(HashMap::new()) }
    }

    /// True if `token` is currently locked out from re-entry.
    fn locked_out(&self, token: &str, count: i64, secs: i64) -> bool {
        if count <= 0 { return false; }
        let book = self.loss_book.lock().unwrap();
        match book.get(token) {
            Some((n, at)) => *n >= count && (at.elapsed().as_secs() as i64) < secs,
            None => false,
        }
    }

    fn record_stop_loss(&self, token: &str) {
        let mut book = self.loss_book.lock().unwrap();
        let e = book.entry(token.to_string()).or_insert((0, Instant::now()));
        e.0 += 1;
        e.1 = Instant::now();
    }

    fn clear_losses(&self, token: &str) {
        self.loss_book.lock().unwrap().remove(token);
    }
}

impl Default for BasisStrategyImpl {
    fn default() -> Self { Self::new() }
}

#[async_trait]
impl Strategy for BasisStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        // "Why no trades?" registry feed (GET /api/vipers/status).
        let idle = |r: &str| crate::helpers::viper_status::report_reason(&ctx.crypto_filter, &self.name(), r);
        if !dc.enable_basis {
            idle("disabled in config");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global Risk Check ────────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            idle("session drawdown limit hit");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Venue Selection: Prefer Window/Maker venue for Basis ─────────────
        let (market, snap) = if let (Some(mk_mkt), Some(mk_snap)) = (&ctx.maker_market, &ctx.maker_snapshot) {
            (mk_mkt, mk_snap)
        } else {
            (&ctx.market, &ctx.snapshot)
        };

        // ── Expiry guard ─────────────────────────────────────────────────────
        if let Some(close_time) = market.market_close_time {
            let secs_left = (close_time - Utc::now()).num_seconds();
            if secs_left < dc.basis_min_secs_to_expiry {
                idle("too close to expiry");
                return Ok(StrategySignal::NoSignal);
            }
        }

        // ── Snapshot staleness gate ───────────────────────────────────────────
        // Stale snapshot depth/price values can let OBI and mid-price gates pass
        // silently when the actual live book has moved adversely.
        // GBoost and TimeDecay both gate on snapshot age; same protection here.
        let snap_age = (Utc::now() - snap.timestamp).num_seconds();
        if snap_age > config::BASIS_MAX_SNAPSHOT_AGE_SECS {
            idle("snapshot stale");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Require a known strike price ─────────────────────────────────────
        let strike = match market.strike_price {
            Some(s) => s,
            None => { idle("market has no strike price"); return Ok(StrategySignal::NoSignal) },
        };

        // ── Fee gate: skip high-fee markets ──────────────────────────────────
        let max_fee = market.yes_fee_bps.max(market.no_fee_bps);
        if max_fee > config::BASIS_MAX_TAKER_FEE_BPS {
            idle("market fees too high");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Oracle-relative thresholds ─────────────────────────────────────────
        let oracle_price  = ctx.snapshot.oracle_price;
        let max_velocity  = config::oracle_threshold(config::BASIS_MAX_VELOCITY_PCT, oracle_price);
        let oracle_buffer = config::oracle_threshold(config::BASIS_ORACLE_STRIKE_BUFFER_PCT, oracle_price);

        // ── Gate 1: Binance is flat ──────────────────────────────────────────
        if ctx.snapshot.velocity.abs() >= max_velocity {
            idle("underlying moving too fast");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate 2: Oracle near strike ───────────────────────────────────────
        if (ctx.snapshot.oracle_price - strike).abs() >= oracle_buffer {
            idle("oracle too far from strike");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Compute implied probability skew ─────────────────────────────────
        let yes_mid = if snap.yes_bid > dec!(0) && snap.yes_ask < dec!(1) {
            (snap.yes_bid + snap.yes_ask) / dec!(2)
        } else {
            idle("degenerate book (no usable mid)");
            return Ok(StrategySignal::NoSignal);
        };
        let skew = yes_mid - dec!(0.50);

        // ── Gate 3: Skew must exceed entry threshold ──────────────────────────
        if skew.abs() < dc.basis_entry_skew_threshold {
            idle("skew below entry threshold");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate 3b: Entry-side spread ────────────────────────────────────────
        // A taker entry fills near the ask while the exit marks to the bid; on a
        // wide book the position is born ~spread% underwater and trips the
        // catastrophic SL immediately (2026-08-09: NO @ 0.32, bid 0.20 → −37.5% @ 0s).
        let (entry_bid, entry_ask, entry_token) = if skew > dec!(0) {
            (snap.no_bid, snap.no_ask, market.no_token.as_str())
        } else {
            (snap.yes_bid, snap.yes_ask, market.yes_token.as_str())
        };
        let entry_mid = (entry_bid + entry_ask) / dec!(2);
        if entry_mid <= dec!(0) || (entry_ask - entry_bid) / entry_mid > dc.basis_max_spread_pct {
            idle("entry-side spread too wide");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate 3c: Post-loss lockout ────────────────────────────────────────
        // After N stop-losses on this token, the "mispricing" is a trend —
        // stand aside until the lockout expires or the market rotates.
        if self.locked_out(entry_token, dc.basis_loss_lockout_count, dc.basis_loss_lockout_secs) {
            idle("post-loss lockout (repeated stops on this market)");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate 4: Funding rate confirmation ────────────────────────────────
        let funding_confirms_no_trade = skew > dec!(0) // YES over-priced
            && ctx.snapshot.funding_rate < config::BASIS_NEGATIVE_FUNDING_THRESHOLD;
        let funding_confirms_yes_trade = skew < dec!(0) // NO over-priced
            && ctx.snapshot.funding_rate > config::BASIS_POSITIVE_FUNDING_THRESHOLD;
        let extreme_skew_bypass = dc.basis_extreme_skew_bypass
            && skew.abs() >= dc.basis_entry_skew_threshold * dec!(2);

        // ── Gate 4b: Institutional-tide contradiction veto ────────────────────
        // Basis fades retail skew; the Tide Raptor reports where institutions are
        // leaning. When institutions strongly disagree with the fade (and the three
        // ETFs cohere), don't fade into their flow. Absolute — not bypassed by
        // extreme skew. Inert for ETH/SOL / outside US hours (pulse/coherence = 0)
        // and disabled by default (observe-first).
        if config::BASIS_TIDE_GATE_ENABLED
            && ctx.snapshot.tide_coherence >= config::BASIS_TIDE_COHERENCE_THRESHOLD
            && (
                (skew > dec!(0) && ctx.snapshot.institutional_pulse >=  config::BASIS_TIDE_PULSE_THRESHOLD)
                || (skew < dec!(0) && ctx.snapshot.institutional_pulse <= -config::BASIS_TIDE_PULSE_THRESHOLD)
            )
        {
            idle("institutional tide contradicts fade");
            return Ok(StrategySignal::NoSignal);
        }

        // Kelly sizing — then back off by the taker fee so order_amount + fee never exceeds trade_size.
        // Without this, a $15 order at 1000 bps adds ~$0.67 in fees, pushing the required total
        // above the available pUSD balance and causing a 400 "not enough balance" rejection.
        let trade_size = crate::vipers::basis_impl::basis_trade_size(skew.abs(), dc.basis_min_trade_size_usdc, dc.basis_max_trade_size_usdc, dc.basis_entry_skew_threshold);
        let no_fee_headroom  = dec!(1) + Decimal::from(market.no_fee_bps)  / dec!(10000);
        let yes_fee_headroom = dec!(1) + Decimal::from(market.yes_fee_bps) / dec!(10000);

        // ── Balance Gate ─────────────────────────────────────────────────────
        // If the wallet can't cover even the minimum trade + fee, skip entirely.
        // This prevents 400 rejections from firing every 60s when the balance is depleted.
        let min_required = dc.basis_min_trade_size_usdc / no_fee_headroom.min(yes_fee_headroom);
        if ctx.available_collateral < min_required {
            idle("insufficient collateral");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Strategy Exposure Check ──────────────────────────────────────────
        let current_exposure = {
            let pos_map = ctx.positions.lock().await;
            pos_map.iter()
                .filter(|(k, _)| k.strategy == "BasisStrategy" && k.squadron == ctx.squadron_id)
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum::<Decimal>()
        };

        if current_exposure + trade_size > dc.basis_max_exposure_usdc {
            idle("exposure cap reached");
            return Ok(StrategySignal::NoSignal);
        }

        // ── Decide direction ─────────────────────────────────────────────────
        if skew > dec!(0) {
            // YES overpriced → fade by buying NO
            if !funding_confirms_no_trade && !extreme_skew_bypass {
                idle("funding rate does not confirm fade");
                return Ok(StrategySignal::NoSignal);
            }

            let target_price;
            let entry_fee_bps;
            let order_type;
            let post_only;
            let effective_fee_multiplier;

            if config::BASIS_ENTRY_AS_MAKER {
                // Aim to place a maker buy order for NO token
                let mut proposed_price = snap.no_bid + config::BASIS_MAKER_BUY_PRICE_ADJUSTMENT;
                // Ensure the proposed price does not cross the spread (i.e., is not >= current ask)
                // If it would cross, adjust it to be one tick below the ask, or at the bid if that's lower.
                if proposed_price >= snap.no_ask {
                    proposed_price = snap.no_ask - dec!(0.01);
                    if proposed_price <= snap.no_bid {
                        proposed_price = snap.no_bid;
                    }
                }
                target_price = proposed_price;
                entry_fee_bps = 0; // Maker orders have 0 fees
                order_type = TimeInForce::Gtc; // Good-Til-Cancelled for maker
                post_only = true; // Ensure it's a post-only order
                effective_fee_multiplier = dec!(1); // No fee to back off from trade_size
            } else {
                // Taker entry (current behavior)
                target_price = snap.no_ask;
                entry_fee_bps = market.no_fee_bps as u16;
                order_type = TimeInForce::Fak; // Fill-And-Kill for taker
                post_only = false; // Not post-only
                effective_fee_multiplier = no_fee_headroom;
            }

            if target_price > dc.basis_max_entry_price {
                idle("entry price above cap");
                return Ok(StrategySignal::NoSignal);
            }

            // Viper Backtrace: persist the gate/decision state for this entry.
            crate::helpers::metrics::stash_entry_signals_json(market.no_token.as_str(), serde_json::json!({
                "viper": "Basis",
                "branch": "fade_YES_buy_NO",
                "skew": skew.to_string(),
                "funding_confirms": funding_confirms_no_trade,
                "extreme_skew_bypass": extreme_skew_bypass,
                "target_price": target_price.to_string(),
                "trade_size": trade_size.to_string(),
                "maker_entry": config::BASIS_ENTRY_AS_MAKER,
            }));

            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: market.no_token.clone(),
                    price: target_price,
                    shares: (trade_size / effective_fee_multiplier) / target_price,
                    fee_bps: entry_fee_bps,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                    order_type,
                    post_only,
                    ghost_mode: dc.ghost_mode,
                },
                pair_params: None,
            });
        } else {
            // NO overpriced → fade by buying YES
            if !funding_confirms_yes_trade && !extreme_skew_bypass {
                idle("funding rate does not confirm fade");
                return Ok(StrategySignal::NoSignal);
            }

            let target_price;
            let entry_fee_bps;
            let order_type;
            let post_only;
            let effective_fee_multiplier;

            if config::BASIS_ENTRY_AS_MAKER {
                // Aim to place a maker buy order for YES token
                let mut proposed_price = snap.yes_bid + config::BASIS_MAKER_BUY_PRICE_ADJUSTMENT;
                // Ensure the proposed price does not cross the spread (i.e., is not >= current ask)
                if proposed_price >= snap.yes_ask {
                    proposed_price = snap.yes_ask - dec!(0.01);
                    if proposed_price <= snap.yes_bid {
                        proposed_price = snap.yes_bid;
                    }
                }
                target_price = proposed_price;
                entry_fee_bps = 0; // Maker orders have 0 fees
                order_type = TimeInForce::Gtc; // Good-Til-Cancelled for maker
                post_only = true; // Ensure it's a post-only order
                effective_fee_multiplier = dec!(1); // No fee to back off from trade_size
            } else {
                // Taker entry (current behavior)
                target_price = snap.yes_ask;
                entry_fee_bps = market.yes_fee_bps as u16;
                order_type = TimeInForce::Fak; // Fill-And-Kill for taker
                post_only = false; // Not post-only
                effective_fee_multiplier = yes_fee_headroom;
            }

            if target_price > dc.basis_max_entry_price {
                idle("entry price above cap");
                return Ok(StrategySignal::NoSignal);
            }

            // Viper Backtrace: persist the gate/decision state for this entry.
            crate::helpers::metrics::stash_entry_signals_json(market.yes_token.as_str(), serde_json::json!({
                "viper": "Basis",
                "branch": "fade_NO_buy_YES",
                "skew": skew.to_string(),
                "funding_confirms": funding_confirms_yes_trade,
                "extreme_skew_bypass": extreme_skew_bypass,
                "target_price": target_price.to_string(),
                "trade_size": trade_size.to_string(),
                "maker_entry": config::BASIS_ENTRY_AS_MAKER,
            }));

            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: market.yes_token.clone(),
                    price: target_price,
                    shares: (trade_size / effective_fee_multiplier) / target_price,
                    fee_bps: entry_fee_bps,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                    order_type,
                    post_only,
                    ghost_mode: dc.ghost_mode,
                },
                pair_params: None,
            });
        }
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        use crate::state::PositionMap;
        use tokio::sync::MutexGuard;

        let positions: MutexGuard<PositionMap> = ctx.positions.lock().await;

        for (key, position) in positions.iter() {
            let (strategy_name, token_id) = (&key.strategy, &key.market);
            if strategy_name != "BasisStrategy" || key.squadron != ctx.squadron_id { continue; }

            // The venue that actually quotes this token, or nothing. The old
            // shape fell through to the hourly market unchecked — see
            // `vipers::venue_for_token`.
            let Some((target_market, snap)) = crate::vipers::venue_for_token(ctx, token_id) else {
                crate::vipers::note_position_without_venue(strategy_name, token_id);
                continue;
            };

            let position_bid = if token_id == &target_market.yes_token {
                snap.yes_bid
            } else if token_id == &target_market.no_token {
                snap.no_bid
            } else {
                continue;
            };

            let avg_entry = position.avg_entry;
            if avg_entry <= dec!(0) { continue; }

            let profit_margin = (position_bid - avg_entry) / avg_entry;
            let now = Utc::now();
            let secs_held = (now - position.opened_at).num_seconds();

            // Recompute current YES mid to detect skew-collapse
            let yes_mid = if snap.yes_bid > dec!(0) && snap.yes_ask < dec!(1) {
                (snap.yes_bid + snap.yes_ask) / dec!(2)
            } else {
                dec!(0.5)
            };
            let current_skew = (yes_mid - dec!(0.50)).abs();

            if profit_margin >= dc.basis_target_profit_pct {
                self.clear_losses(token_id.as_str());
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token_id.clone(),
                        price: position_bid,
                        shares: position.shares,
                        fee_bps: if token_id == &target_market.yes_token { target_market.yes_fee_bps as u16 } else { target_market.no_fee_bps as u16 },
                        is_neg_risk: target_market.is_neg_risk,
                        market_name: target_market.market_name.clone(),
                        condition_id: target_market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!("BasisTP: bid=${:.4}, profit={:.2}%", position_bid, profit_margin * dec!(100)),
                    exit_pair: false,
                });
            }

            if profit_margin <= -dc.basis_stop_loss_pct {
                // The catastrophic (min-hold-bypass) trigger marks to the book MID,
                // not the bid: a fresh entry pays the spread and is born ~spread%
                // underwater at the bid, which fired the bypass 1-2s after entry
                // (2026-08-08, -6.25% @ 1s). The regular SL still marks to bid.
                let position_ask = if token_id == &target_market.yes_token { snap.yes_ask } else { snap.no_ask };
                let mid_margin = if position_ask > dec!(0) && position_ask < dec!(1) {
                    ((position_bid + position_ask) / dec!(2) - avg_entry) / avg_entry
                } else {
                    profit_margin
                };
                let is_catastrophic = mid_margin <= -dc.basis_catastrophic_sl_pct;
                if !is_catastrophic && secs_held < config::BASIS_MIN_HOLD_SECS_BEFORE_STOP_LOSS {
                    continue;
                }
                // EMERGENCY FIX: If the bid is too low, assume FAK will miss and defer exit.
                // This prevents repeated exit attempts at unfillable prices, which causes log floods.
                if position_bid < config::BASIS_MIN_STOP_LOSS_EXIT_BID {
                    tracing::warn!(
                        "⏭️  BasisSL skipped (bid {:.4} < floor {:.4}): assuming FAK miss, holding position.",
                        position_bid, config::BASIS_MIN_STOP_LOSS_EXIT_BID
                    );
                    return Ok(StrategySignal::NoSignal);
                }

                // Feed the post-loss lockout tally. Recorded at signal time
                // (same convention as TrendReversal's exit cooldown);
                // `on_exit_order_failed` rolls it back on placement rejection.
                self.record_stop_loss(token_id.as_str());

                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token_id.clone(),
                        price: position_bid,
                        shares: position.shares,
                        fee_bps: if token_id == &target_market.yes_token { target_market.yes_fee_bps as u16 } else { target_market.no_fee_bps as u16 },
                        is_neg_risk: target_market.is_neg_risk,
                        market_name: target_market.market_name.clone(),
                        condition_id: target_market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: if is_catastrophic {
                        format!("BasisCatastrophicSL: bid=${:.4}, loss={:.2}% (min-hold bypassed @ {}s)", position_bid, profit_margin * dec!(100), secs_held)
                    } else {
                        format!("BasisSL: bid=${:.4}, loss={:.2}%", position_bid, profit_margin * dec!(100))
                    },
                    exit_pair: false,
                });
            }

            if profit_margin > dec!(0) && current_skew < dc.basis_skew_collapse_threshold {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token_id.clone(),
                        price: position_bid,
                        shares: position.shares,
                        fee_bps: if token_id == &target_market.yes_token { target_market.yes_fee_bps as u16 } else { target_market.no_fee_bps as u16 },
                        is_neg_risk: target_market.is_neg_risk,
                        market_name: target_market.market_name.clone(),
                        condition_id: target_market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!("BasisSkewCollapse: yes_mid={:.4}, profit={:.2}%", yes_mid, profit_margin * dec!(100)),
                    exit_pair: false,
                });
            }

            if let Some(close_time) = position.close_time {
                let secs_left = (close_time - Utc::now()).num_seconds();
                if secs_left < dc.basis_min_secs_to_expiry / 2 {
                    // Skip BasisExpiry if the bid is too thin to get a FAK fill — near market
                    // close the order book dries up and FAK returns 0 fills while the position
                    // map is cleared optimistically, leaving orphaned on-chain shares.
                    // Better to let the position go to settlement than send an unfillable order.
                    if position_bid < config::BASIS_EXPIRY_MIN_EXIT_BID {
                        tracing::info!(
                            "⏭️  BasisExpiry skipped (bid {:.4} < floor {:.4}): {}s left — holding to settlement",
                            position_bid, config::BASIS_EXPIRY_MIN_EXIT_BID, secs_left
                        );
                    } else {
                        return Ok(StrategySignal::Exit {
                            params: OrderParams {
                                token_id: token_id.clone(),
                                price: position_bid,
                                shares: position.shares,
                                fee_bps: if token_id == &target_market.yes_token { target_market.yes_fee_bps as u16 } else { target_market.no_fee_bps as u16 },
                                is_neg_risk: target_market.is_neg_risk,
                                market_name: target_market.market_name.clone(),
                                condition_id: target_market.condition_id.clone(),
                                order_type: TimeInForce::Fak,
                                post_only: false,
                                ghost_mode: dc.ghost_mode,
                            },
                            reason: format!("BasisExpiry: {}s left", secs_left),
                            exit_pair: false,
                        });
                    }
                }
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "BasisStrategy".to_string() }

    /// Patrol reports a rejected exit placement — the stop never executed, so
    /// roll back the loss-lockout tally bump made at signal time.
    fn on_exit_order_failed(&self, token_id: &crate::venues::core::MarketId) {
        let mut book = self.loss_book.lock().unwrap();
        if let Some(e) = book.get_mut(token_id.as_str()) {
            e.0 -= 1;
            if e.0 <= 0 { book.remove(token_id.as_str()); }
        }
    }
    fn venue(&self) -> &'static str { "Window/Daily" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::BASIS_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Gross one-sided" }
}

pub fn basis_trade_size(skew_abs: Decimal, min_size: Decimal, max_size: Decimal, skew_threshold: Decimal) -> Decimal {
    if !config::ENABLE_KELLY_SIZING { return min_size; }
    let threshold = skew_threshold;
    if threshold <= Decimal::ZERO { return min_size; }
    let multiplier = (skew_abs / threshold).max(Decimal::ONE).min(config::BASIS_KELLY_MAX_MULTIPLIER);
    let fraction = (multiplier - Decimal::ONE) / (config::BASIS_KELLY_MAX_MULTIPLIER - Decimal::ONE);
    min_size + fraction * (max_size - min_size)
}