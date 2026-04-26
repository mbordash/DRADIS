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
use crate::strategies::is_drawdown_limit_hit;
use crate::config;

pub struct BasisStrategyImpl;

#[async_trait]
impl Strategy for BasisStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        if !config::ENABLE_BASIS_TRADING {
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

        // ── Select per-crypto constants ───────────────────────────────────────
        let (max_velocity, oracle_buffer) = match ctx.crypto_filter.as_str() {
            "eth" => (config::BASIS_ETH_MAX_VELOCITY, config::BASIS_ETH_ORACLE_STRIKE_BUFFER),
            "sol" => (config::BASIS_SOL_MAX_VELOCITY, config::BASIS_SOL_ORACLE_STRIKE_BUFFER),
            _     => (config::BASIS_BTC_MAX_VELOCITY, config::BASIS_BTC_ORACLE_STRIKE_BUFFER),
        };

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

        // Kelly sizing
        let trade_size = crate::strategies::basis_impl::basis_trade_size(skew.abs());

        // ── Strategy Exposure Check ──────────────────────────────────────────
        let current_exposure = {
            let pos_map = ctx.positions.lock().await;
            pos_map.iter()
                .filter(|((s, _), _)| s == "BasisStrategy")
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum::<Decimal>()
        };

        if current_exposure + trade_size > config::BASIS_MAX_EXPOSURE_USDC {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Decide direction ─────────────────────────────────────────────────
        if skew > dec!(0) {
            // YES overpriced → fade by buying NO
            if !funding_confirms_no_trade && !extreme_skew_bypass {
                return Ok(StrategySignal::NoSignal);
            }
            if snap.no_ask > config::BASIS_MAX_ENTRY_PRICE {
                return Ok(StrategySignal::NoSignal);
            }
            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: market.no_token,
                    price: snap.no_ask,
                    shares: trade_size / snap.no_ask,
                    fee_bps: market.no_fee_bps as u16,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                },
                pair_params: None,
            });
        } else {
            // NO overpriced → fade by buying YES
            if !funding_confirms_yes_trade && !extreme_skew_bypass {
                return Ok(StrategySignal::NoSignal);
            }
            if snap.yes_ask > config::BASIS_MAX_ENTRY_PRICE {
                return Ok(StrategySignal::NoSignal);
            }
            return Ok(StrategySignal::Entry {
                params: OrderParams {
                    token_id: market.yes_token,
                    price: snap.yes_ask,
                    shares: trade_size / snap.yes_ask,
                    fee_bps: market.yes_fee_bps as u16,
                    is_neg_risk: market.is_neg_risk,
                    market_name: market.market_name.clone(),
                    condition_id: market.condition_id.clone(),
                },
                pair_params: None,
            });
        }
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
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

            if profit_margin >= config::BASIS_TARGET_PROFIT_PERCENT {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: *token_id,
                        price: position_bid,
                        shares: position.shares,
                        fee_bps: if token_id == &target_market.yes_token { target_market.yes_fee_bps as u16 } else { target_market.no_fee_bps as u16 },
                        is_neg_risk: target_market.is_neg_risk,
                        market_name: target_market.market_name.clone(),
                        condition_id: target_market.condition_id.clone(),
                    },
                    reason: format!("BasisTP: bid=${:.4}, profit={:.2}%", position_bid, profit_margin * dec!(100)),
                    exit_pair: false,
                });
            }

            if profit_margin <= -config::BASIS_STOP_LOSS_PERCENT
                && secs_held >= config::BASIS_MIN_HOLD_SECS_BEFORE_STOP_LOSS
            {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: *token_id,
                        price: position_bid,
                        shares: position.shares,
                        fee_bps: if token_id == &target_market.yes_token { target_market.yes_fee_bps as u16 } else { target_market.no_fee_bps as u16 },
                        is_neg_risk: target_market.is_neg_risk,
                        market_name: target_market.market_name.clone(),
                        condition_id: target_market.condition_id.clone(),
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
                    },
                    reason: format!("BasisSkewCollapse: yes_mid={:.4}, profit={:.2}%", yes_mid, profit_margin * dec!(100)),
                    exit_pair: false,
                });
            }

            if let Some(close_time) = position.close_time {
                let secs_left = (close_time - Utc::now()).num_seconds();
                if secs_left < config::BASIS_MIN_SECS_TO_EXPIRY / 2 {
                    return Ok(StrategySignal::Exit {
                        params: OrderParams {
                            token_id: *token_id,
                            price: position_bid,
                            shares: position.shares,
                            fee_bps: if token_id == &target_market.yes_token { target_market.yes_fee_bps as u16 } else { target_market.no_fee_bps as u16 },
                            is_neg_risk: target_market.is_neg_risk,
                            market_name: target_market.market_name.clone(),
                            condition_id: target_market.condition_id.clone(),
                        },
                        reason: format!("BasisExpiry: {}s left", secs_left),
                        exit_pair: false,
                    });
                }
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "BasisStrategy".to_string() }
}

pub fn basis_trade_size(skew_abs: Decimal) -> Decimal {
    let threshold = config::BASIS_ENTRY_SKEW_THRESHOLD;
    if threshold <= Decimal::ZERO { return config::BASIS_MIN_TRADE_SIZE_USDC; }
    let multiplier = (skew_abs / threshold).max(Decimal::ONE).min(config::BASIS_KELLY_MAX_MULTIPLIER);
    let fraction = (multiplier - Decimal::ONE) / (config::BASIS_KELLY_MAX_MULTIPLIER - Decimal::ONE);
    config::BASIS_MIN_TRADE_SIZE_USDC + fraction * (config::BASIS_MAX_TRADE_SIZE_USDC - config::BASIS_MIN_TRADE_SIZE_USDC)
}
