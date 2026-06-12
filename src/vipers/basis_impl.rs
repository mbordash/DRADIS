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

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::vipers::is_drawdown_limit_hit;
use crate::config;
use polymarket_client_sdk_v2::clob::types::OrderType; // Import OrderType

pub struct BasisStrategyImpl;

#[async_trait]
impl Strategy for BasisStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        if !dc.enable_basis {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global Risk Check ────────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
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
            if secs_left < config::BASIS_MIN_SECS_TO_EXPIRY {
                return Ok(StrategySignal::NoSignal);
            }
        }

        // ── Snapshot staleness gate ───────────────────────────────────────────
        // Stale snapshot depth/price values can let OBI and mid-price gates pass
        // silently when the actual live book has moved adversely.
        // GBoost and TimeDecay both gate on snapshot age; same protection here.
        let snap_age = (Utc::now() - snap.timestamp).num_seconds();
        if snap_age > config::BASIS_MAX_SNAPSHOT_AGE_SECS {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Require a known strike price ─────────────────────────────────────
        let strike = match market.strike_price {
            Some(s) => s,
            None => return Ok(StrategySignal::NoSignal),
        };

        // ── Fee gate: skip high-fee markets ──────────────────────────────────
        let max_fee = market.yes_fee_bps.max(market.no_fee_bps);
        if max_fee > config::BASIS_MAX_TAKER_FEE_BPS {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Oracle-relative thresholds ─────────────────────────────────────────
        let oracle_price  = ctx.snapshot.oracle_price;
        let max_velocity  = config::oracle_threshold(config::BASIS_MAX_VELOCITY_PCT, oracle_price);
        let oracle_buffer = config::oracle_threshold(config::BASIS_ORACLE_STRIKE_BUFFER_PCT, oracle_price);

        // ── Gate 1: Binance is flat ──────────────────────────────────────────
        if ctx.snapshot.velocity.abs() >= max_velocity {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate 2: Oracle near strike ───────────────────────────────────────
        if (ctx.snapshot.oracle_price - strike).abs() >= oracle_buffer {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Compute implied probability skew ─────────────────────────────────
        let yes_mid = if snap.yes_bid > dec!(0) && snap.yes_ask < dec!(1) {
            (snap.yes_bid + snap.yes_ask) / dec!(2)
        } else {
            return Ok(StrategySignal::NoSignal);
        };
        let skew = yes_mid - dec!(0.50);

        // ── Gate 3: Skew must exceed entry threshold ──────────────────────────
        if skew.abs() < config::BASIS_ENTRY_SKEW_THRESHOLD {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate 4: Funding rate confirmation ────────────────────────────────
        let funding_confirms_no_trade = skew > dec!(0) // YES over-priced
            && ctx.snapshot.funding_rate < config::BASIS_NEGATIVE_FUNDING_THRESHOLD;
        let funding_confirms_yes_trade = skew < dec!(0) // NO over-priced
            && ctx.snapshot.funding_rate > config::BASIS_POSITIVE_FUNDING_THRESHOLD;
        let extreme_skew_bypass = skew.abs() >= config::BASIS_ENTRY_SKEW_THRESHOLD * dec!(2);

        // Kelly sizing — then back off by the taker fee so order_amount + fee never exceeds trade_size.
        // Without this, a $15 order at 1000 bps adds ~$0.67 in fees, pushing the required total
        // above the available pUSD balance and causing a 400 "not enough balance" rejection.
        let trade_size = crate::vipers::basis_impl::basis_trade_size(skew.abs());
        let no_fee_headroom  = dec!(1) + Decimal::from(market.no_fee_bps)  / dec!(10000);
        let yes_fee_headroom = dec!(1) + Decimal::from(market.yes_fee_bps) / dec!(10000);

        // ── Balance Gate ─────────────────────────────────────────────────────
        // If the wallet can't cover even the minimum trade + fee, skip entirely.
        // This prevents 400 rejections from firing every 60s when the balance is depleted.
        let min_required = config::BASIS_MIN_TRADE_SIZE_USDC / no_fee_headroom.min(yes_fee_headroom);
        if ctx.available_collateral < min_required {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Strategy Exposure Check ──────────────────────────────────────────
        let current_exposure = {
            let pos_map = ctx.positions.lock().await;
            pos_map.iter()
                .filter(|((s, _), _)| s == "BasisStrategy")
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum::<Decimal>()
        };

        if current_exposure + trade_size > dc.basis_max_exposure_usdc {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Decide direction ─────────────────────────────────────────────────
        if skew > dec!(0) {
            // YES overpriced → fade by buying NO
            if !funding_confirms_no_trade && !extreme_skew_bypass {
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
                order_type = OrderType::GTC; // Good-Til-Cancelled for maker
                post_only = true; // Ensure it's a post-only order
                effective_fee_multiplier = dec!(1); // No fee to back off from trade_size
            } else {
                // Taker entry (current behavior)
                target_price = snap.no_ask;
                entry_fee_bps = market.no_fee_bps as u16;
                order_type = OrderType::FAK; // Fill-And-Kill for taker
                post_only = false; // Not post-only
                effective_fee_multiplier = no_fee_headroom;
            }

            if target_price > config::BASIS_MAX_ENTRY_PRICE {
                return Ok(StrategySignal::NoSignal);
            }

            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: market.no_token,
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
                order_type = OrderType::GTC; // Good-Til-Cancelled for maker
                post_only = true; // Ensure it's a post-only order
                effective_fee_multiplier = dec!(1); // No fee to back off from trade_size
            } else {
                // Taker entry (current behavior)
                target_price = snap.yes_ask;
                entry_fee_bps = market.yes_fee_bps as u16;
                order_type = OrderType::FAK; // Fill-And-Kill for taker
                post_only = false; // Not post-only
                effective_fee_multiplier = yes_fee_headroom;
            }

            if target_price > config::BASIS_MAX_ENTRY_PRICE {
                return Ok(StrategySignal::NoSignal);
            }

            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: market.yes_token,
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

        for ((strategy_name, token_id), position) in positions.iter() {
            if strategy_name != "BasisStrategy" { continue; }

            // Match current venue for the held token
            let (target_market, snap) = if let Some(mk) = &ctx.maker_market {
                if token_id == &mk.yes_token || token_id == &mk.no_token {
                    (mk, ctx.maker_snapshot.as_ref().unwrap())
                } else {
                    (&ctx.market, &ctx.snapshot)
                }
            } else {
                (&ctx.market, &ctx.snapshot)
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
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: *token_id,
                        price: position_bid,
                        shares: position.shares,
                        fee_bps: if token_id == &target_market.yes_token { target_market.yes_fee_bps as u16 } else { target_market.no_fee_bps as u16 },
                        is_neg_risk: target_market.is_neg_risk,
                        market_name: target_market.market_name.clone(),
                        condition_id: target_market.condition_id.clone(),
                        order_type: OrderType::FAK,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!("BasisTP: bid=${:.4}, profit={:.2}%", position_bid, profit_margin * dec!(100)),
                    exit_pair: false,
                });
            }

            if profit_margin <= -dc.basis_stop_loss_pct
                && secs_held >= config::BASIS_MIN_HOLD_SECS_BEFORE_STOP_LOSS
            {
                // EMERGENCY FIX: If the bid is too low, assume FAK will miss and defer exit.
                // This prevents repeated exit attempts at unfillable prices, which causes log floods.
                if position_bid < config::BASIS_MIN_STOP_LOSS_EXIT_BID {
                    tracing::warn!(
                        "⏭️  BasisSL skipped (bid {:.4} < floor {:.4}): assuming FAK miss, holding position.",
                        position_bid, config::BASIS_MIN_STOP_LOSS_EXIT_BID
                    );
                    return Ok(StrategySignal::NoSignal);
                }

                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: *token_id,
                        price: position_bid,
                        shares: position.shares,
                        fee_bps: if token_id == &target_market.yes_token { target_market.yes_fee_bps as u16 } else { target_market.no_fee_bps as u16 },
                        is_neg_risk: target_market.is_neg_risk,
                        market_name: target_market.market_name.clone(),
                        condition_id: target_market.condition_id.clone(),
                        order_type: OrderType::FAK,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!("BasisSL: bid=${:.4}, loss={:.2}%", position_bid, profit_margin * dec!(100)),
                    exit_pair: false,
                });
            }

            if profit_margin > dec!(0) && current_skew < config::BASIS_SKEW_COLLAPSE_THRESHOLD {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: *token_id,
                        price: position_bid,
                        shares: position.shares,
                        fee_bps: if token_id == &target_market.yes_token { target_market.yes_fee_bps as u16 } else { target_market.no_fee_bps as u16 },
                        is_neg_risk: target_market.is_neg_risk,
                        market_name: target_market.market_name.clone(),
                        condition_id: target_market.condition_id.clone(),
                        order_type: OrderType::FAK,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!("BasisSkewCollapse: yes_mid={:.4}, profit={:.2}%", yes_mid, profit_margin * dec!(100)),
                    exit_pair: false,
                });
            }

            if let Some(close_time) = position.close_time {
                let secs_left = (close_time - Utc::now()).num_seconds();
                if secs_left < config::BASIS_MIN_SECS_TO_EXPIRY / 2 {
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
                                token_id: *token_id,
                                price: position_bid,
                                shares: position.shares,
                                fee_bps: if token_id == &target_market.yes_token { target_market.yes_fee_bps as u16 } else { target_market.no_fee_bps as u16 },
                                is_neg_risk: target_market.is_neg_risk,
                                market_name: target_market.market_name.clone(),
                                condition_id: target_market.condition_id.clone(),
                                order_type: OrderType::FAK,
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
    fn venue(&self) -> &'static str { "Window/Daily" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::BASIS_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Gross one-sided" }
}

pub fn basis_trade_size(skew_abs: Decimal) -> Decimal {
    let threshold = config::BASIS_ENTRY_SKEW_THRESHOLD;
    if threshold <= Decimal::ZERO { return config::BASIS_MIN_TRADE_SIZE_USDC; }
    let multiplier = (skew_abs / threshold).max(Decimal::ONE).min(config::BASIS_KELLY_MAX_MULTIPLIER);
    let fraction = (multiplier - Decimal::ONE) / (config::BASIS_KELLY_MAX_MULTIPLIER - Decimal::ONE);
    config::BASIS_MIN_TRADE_SIZE_USDC + fraction * (config::BASIS_MAX_TRADE_SIZE_USDC - config::BASIS_MIN_TRADE_SIZE_USDC)
}