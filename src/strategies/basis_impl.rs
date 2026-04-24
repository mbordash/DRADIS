/// Basis / Funding-Rate Mean-Reversion Strategy
///
/// # Thesis
///
/// Polymarket hourly "Up or Down" markets frequently exhibit **retail skew**:
/// amateur bettors systematically over-bet the bullish (YES) side, pushing its
/// implied probability above what Binance spot actually justifies.
///
/// Smart money's view is captured by the Binance **perpetual funding rate**:
/// - Negative funding → shorts paying longs → institutional bias is bearish
///   even while retail is bullish on Polymarket.
/// - Positive funding → longs paying shorts → institutional bias is bullish
///   even while retail is bearish on Polymarket.
///
/// When these two signals diverge — retail skew AND confirming funding signal —
/// the expected value of fading the skew is positive and we place a taker entry.
///
/// # Entry conditions (example: fade over-bullish YES)
/// 1. YES mid-price > 0.50 + BASIS_ENTRY_SKEW_THRESHOLD  (retail over-bet YES)
/// 2. Binance velocity.abs() < BASIS_MAX_VELOCITY  (price isn't actually flying up)
/// 3. oracle_price near strike (± BASIS_ORACLE_STRIKE_BUFFER)  (not already decided)
/// 4. funding_rate < BASIS_NEGATIVE_FUNDING_THRESHOLD  (smart money net-bearish)
///    OR skew is extreme enough to act on retail signal alone
/// 5. NO ask ≤ BASIS_MAX_ENTRY_PRICE  (avoid buying expensive tokens)
///
/// # Exit conditions
/// - Take-profit: position gains BASIS_TARGET_PROFIT_PERCENT
/// - Stop-loss:   position loses BASIS_STOP_LOSS_PERCENT
/// - Skew collapse: YES mid returns near 0.50 while in profit (thesis played out)
/// - Expiry guard: market too close to close (BASIS_MIN_SECS_TO_EXPIRY)

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use chrono::Utc;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus};
use crate::config;

pub struct BasisStrategyImpl;

#[async_trait]
impl Strategy for BasisStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        if !config::ENABLE_BASIS_TRADING {
            return Ok(StrategySignal::NoSignal);
        }

        let snap = &ctx.snapshot;
        let market = &ctx.market;

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
        // BasisStrategy is a taker strategy. At 1000bps (10%) per leg, the round-trip
        // cost is ~20% — far exceeding any realistic profit target.  Only enter when
        // the taker fee on BOTH tokens is within the configured threshold.
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

        // ── Gate 1: Binance is flat (no strong directional move) ─────────────
        if snap.velocity.abs() >= max_velocity {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate 2: Oracle near strike (not already decided) ─────────────────
        if (snap.oracle_price - strike).abs() >= oracle_buffer {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Compute implied probability skew ─────────────────────────────────
        // Use mid-price to avoid bid/ask directional bias in the skew signal.
        let yes_mid = if snap.yes_bid > dec!(0) && snap.yes_ask < dec!(1) {
            (snap.yes_bid + snap.yes_ask) / dec!(2)
        } else {
            return Ok(StrategySignal::NoSignal);
        };
        let skew = yes_mid - dec!(0.50); // positive = YES overpriced

        // ── Gate 3: Skew must exceed entry threshold ──────────────────────────
        if skew.abs() < config::BASIS_ENTRY_SKEW_THRESHOLD {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Gate 4: Funding rate confirmation (or extreme skew bypass) ────────
        // Confirming signal: funding aligns with our fade direction.
        // Bypass: skew is 2× the threshold — strong enough without funding signal.
        let funding_confirms_no_trade = skew > dec!(0) // YES over-priced
            && snap.funding_rate < config::BASIS_NEGATIVE_FUNDING_THRESHOLD; // smart money bearish
        let funding_confirms_yes_trade = skew < dec!(0) // NO over-priced
            && snap.funding_rate > config::BASIS_POSITIVE_FUNDING_THRESHOLD; // smart money bullish
        let extreme_skew_bypass = skew.abs() >= config::BASIS_ENTRY_SKEW_THRESHOLD * dec!(2);

        // ── Decide direction ─────────────────────────────────────────────────
        if skew > dec!(0) {
            // YES overpriced → fade by buying NO
            if !funding_confirms_no_trade && !extreme_skew_bypass {
                return Ok(StrategySignal::NoSignal);
            }
            if snap.no_ask > config::BASIS_MAX_ENTRY_PRICE {
                return Ok(StrategySignal::NoSignal);
            }
            return Ok(StrategySignal::Entry { token_id: market.no_token });
        } else {
            // NO overpriced → fade by buying YES
            if !funding_confirms_yes_trade && !extreme_skew_bypass {
                return Ok(StrategySignal::NoSignal);
            }
            if snap.yes_ask > config::BASIS_MAX_ENTRY_PRICE {
                return Ok(StrategySignal::NoSignal);
            }
            return Ok(StrategySignal::Entry { token_id: market.yes_token });
        }
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        use crate::state::PositionMap;
        use tokio::sync::MutexGuard;

        let positions: MutexGuard<PositionMap> = ctx.positions.lock().await;

        for ((strategy_name, token_id), position) in positions.iter() {
            if strategy_name != "BasisStrategy" { continue; }

            let position_bid = if token_id == &ctx.market.yes_token {
                ctx.snapshot.yes_bid
            } else if token_id == &ctx.market.no_token {
                ctx.snapshot.no_bid
            } else {
                continue;
            };

            let avg_entry = position.avg_entry;
            if avg_entry <= dec!(0) { continue; }

            let profit_margin = (position_bid - avg_entry) / avg_entry;
            let now = Utc::now();
            let secs_held = (now - position.opened_at).num_seconds();

            // Recompute current YES mid to detect skew-collapse
            let yes_mid = if ctx.snapshot.yes_bid > dec!(0) && ctx.snapshot.yes_ask < dec!(1) {
                (ctx.snapshot.yes_bid + ctx.snapshot.yes_ask) / dec!(2)
            } else {
                dec!(0.5)
            };
            let current_skew = (yes_mid - dec!(0.50)).abs();

            // Take profit
            if profit_margin >= config::BASIS_TARGET_PROFIT_PERCENT {
                return Ok(StrategySignal::Exit {
                    token_id: *token_id,
                    reason: format!(
                        "BasisTP: bid=${:.4}, profit={:.2}%",
                        position_bid, profit_margin * dec!(100)
                    ),
                });
            }

            // Stop loss — only after minimum hold time so FAK phantom positions
            // don't trigger a stop-loss before sync_position_balance can confirm
            // whether shares were actually received.
            if profit_margin <= -config::BASIS_STOP_LOSS_PERCENT
                && secs_held >= config::BASIS_MIN_HOLD_SECS_BEFORE_STOP_LOSS
            {
                return Ok(StrategySignal::Exit {
                    token_id: *token_id,
                    reason: format!(
                        "BasisSL: bid=${:.4}, loss={:.2}%",
                        position_bid, profit_margin * dec!(100)
                    ),
                });
            }

            // Skew collapse: thesis played out — exit in profit before full TP
            if profit_margin > dec!(0) && current_skew < config::BASIS_SKEW_COLLAPSE_THRESHOLD {
                return Ok(StrategySignal::Exit {
                    token_id: *token_id,
                    reason: format!(
                        "BasisSkewCollapse: yes_mid={:.4} near 0.50, profit={:.2}%",
                        yes_mid, profit_margin * dec!(100)
                    ),
                });
            }

            // Expiry guard: exit if too close to market close
            if let Some(close_time) = position.close_time {
                let secs_left = (close_time - Utc::now()).num_seconds();
                if secs_left < config::BASIS_MIN_SECS_TO_EXPIRY / 2 {
                    return Ok(StrategySignal::Exit {
                        token_id: *token_id,
                        reason: format!("BasisExpiry: {}s to close, profit={:.2}%", secs_left, profit_margin * dec!(100)),
                    });
                }
            }
        }

        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus {
        StrategyStatus::Active
    }

    fn name(&self) -> String {
        "BasisStrategy".to_string()
    }
}

/// Scale basis trade size by skew strength (fractional Kelly).
/// At 1× threshold → MIN; at BASIS_KELLY_MAX_MULTIPLIER× → MAX.
pub fn basis_trade_size(skew_abs: Decimal) -> Decimal {
    let threshold = config::BASIS_ENTRY_SKEW_THRESHOLD;
    if threshold <= Decimal::ZERO {
        return config::BASIS_MIN_TRADE_SIZE_USDC;
    }
    let multiplier = (skew_abs / threshold)
        .max(Decimal::ONE)
        .min(config::BASIS_KELLY_MAX_MULTIPLIER);
    let fraction = (multiplier - Decimal::ONE) / (config::BASIS_KELLY_MAX_MULTIPLIER - Decimal::ONE);
    config::BASIS_MIN_TRADE_SIZE_USDC
        + fraction * (config::BASIS_MAX_TRADE_SIZE_USDC - config::BASIS_MIN_TRADE_SIZE_USDC)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use crate::state::{MarketConfig, MarketSnapshot, PositionMap};
    use alloy::primitives::U256;

    fn make_ctx(
        yes_bid: Decimal, yes_ask: Decimal,
        no_bid: Decimal, no_ask: Decimal,
        oracle_price: Decimal, strike: Option<Decimal>,
        velocity: Decimal, funding_rate: Decimal,
    ) -> StrategyContext {
        use crate::orchestrator::StrategyContext;
        let yes_token = U256::from(1u64);
        let no_token  = U256::from(2u64);
        StrategyContext {
            market: MarketConfig {
                yes_token, no_token,
                market_name: "BTC Up or Down".to_string(),
                market_close_time: None,
                strike_price: strike,
                is_neg_risk: false,
                yes_fee_bps: 50,  // low-fee market so fee gate passes
                no_fee_bps: 50,
            },
            snapshot: MarketSnapshot {
                yes_bid, yes_bid_depth: dec!(100), yes_ask, yes_ask_depth: dec!(100),
                no_bid, no_bid_depth: dec!(100), no_ask, no_ask_depth: dec!(100),
                oracle_price, velocity, velocity_1s: velocity,
                acceleration: dec!(0), funding_rate,
                oracle_drift_60m: Default::default(),
                timestamp: Utc::now(),
            },
            positions: Arc::new(Mutex::new(PositionMap::new())),
            crypto_filter: "btc".to_string(),
            market_started_at: Utc::now(),
            maker_market: None,
            maker_snapshot: None,
        }
    }

    #[tokio::test]
    async fn test_fade_yes_overpriced_with_negative_funding() {
        // YES mid = 0.63 (skew = +0.13 > threshold 0.08)
        // Funding = -0.0002 (bearish smart money)
        // Velocity = 0 (flat Binance)
        // Oracle near strike
        let ctx = make_ctx(
            dec!(0.62), dec!(0.64),  // YES bid/ask, mid=0.63
            dec!(0.36), dec!(0.38),  // NO bid/ask
            dec!(84000), Some(dec!(84100)), // oracle near strike
            dec!(5),  // low velocity
            dec!(-0.0002), // negative funding
        );
        let signal = BasisStrategyImpl.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::Entry { token_id } if token_id == U256::from(2u64)),
            "Expected Entry(NO) to fade overpriced YES");
    }

    #[tokio::test]
    async fn test_no_signal_when_velocity_too_high() {
        // Skew = 0.13, but Binance is flying upward — skip
        let ctx = make_ctx(
            dec!(0.62), dec!(0.64),
            dec!(0.36), dec!(0.38),
            dec!(84000), Some(dec!(84100)),
            dec!(50),  // high velocity — NOT flat
            dec!(-0.0002),
        );
        let signal = BasisStrategyImpl.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn test_no_signal_when_oracle_far_from_strike() {
        // Oracle already $500 above strike — market is decided, don't fade
        let ctx = make_ctx(
            dec!(0.62), dec!(0.64),
            dec!(0.36), dec!(0.38),
            dec!(84600), Some(dec!(84100)), // $500 above strike > $200 buffer
            dec!(5),
            dec!(-0.0002),
        );
        let signal = BasisStrategyImpl.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn test_extreme_skew_bypasses_funding_gate() {
        // Skew = 0.20 (2× threshold = 0.16 < 0.20) → bypass funding gate even with neutral funding
        let ctx = make_ctx(
            dec!(0.69), dec!(0.71), // YES mid = 0.70, skew = 0.20
            dec!(0.29), dec!(0.31),
            dec!(84000), Some(dec!(84100)),
            dec!(5),
            dec!(0.0), // neutral funding — still fires due to extreme skew
        );
        let signal = BasisStrategyImpl.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::Entry { token_id } if token_id == U256::from(2u64)));
    }

    #[test]
    fn test_kelly_sizing() {
        let min = config::BASIS_MIN_TRADE_SIZE_USDC;
        let max = config::BASIS_MAX_TRADE_SIZE_USDC;
        let threshold = config::BASIS_ENTRY_SKEW_THRESHOLD;

        // At exactly threshold → min
        let size_min = basis_trade_size(threshold);
        assert!((size_min - min).abs() < dec!(0.01), "Expected min size, got {}", size_min);

        // At 3× threshold → max
        let size_max = basis_trade_size(threshold * dec!(3));
        assert!((size_max - max).abs() < dec!(0.01), "Expected max size, got {}", size_max);

        // At 2× threshold → midpoint
        let size_mid = basis_trade_size(threshold * dec!(2));
        let expected_mid = (min + max) / dec!(2);
        assert!((size_mid - expected_mid).abs() < dec!(0.10), "Expected mid size ~{}, got {}", expected_mid, size_mid);
    }
}

