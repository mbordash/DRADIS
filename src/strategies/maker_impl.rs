/// Maker Strategy - Two-Sided Market Making
///
/// Posts passive resting bids on BOTH YES and NO simultaneously, earning:
///   1. The spread when positions fill and converge to take-profit.
///   2. Daily USDC rebates from Polymarket's Maker Rebates program on every fill.
///
/// Inventory Skew: if we're heavy YES, the YES bid is lowered (less aggressive)
/// and the NO bid is raised (more aggressive) to rebalance inventory faster.
///
/// Combined Price Guard: YES_bid + NO_bid must be < MAKER_MAX_COMBINED_BID (0.90)
/// to prevent offering a free arb to takers who can sell both legs to us and
/// pocket the difference vs. $1.00 settlement.
///
/// Risk: uses NET exposure |YES_value - NO_value| instead of gross exposure,
/// so a balanced two-sided book can run at larger notional without extra risk.

use async_trait::async_trait;
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::debug;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus};
use crate::config;

/// Velocity threshold (oracle USD/s) above which we consider the market
/// strongly directional and suppress the adverse maker side.
const MAKER_VELOCITY_BIAS_THRESHOLD: Decimal = dec!(25.0);

pub struct MakerStrategyImpl;

#[async_trait]
impl Strategy for MakerStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        if !config::ENABLE_MAKER_TRADING {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Select venue: prefer maker_market (window/daily) over hourly ─────
        // When a window or daily market is available, Maker uses it exclusively.
        // This gives GTD orders a much better chance of filling before the market
        // reprices. Falls back to the hourly market when no alternative exists.
        let market = ctx.maker_market.as_ref().unwrap_or(&ctx.market);
        let snapshot = ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot);

        // ── Market maturation gate ────────────────────────────────────────────
        // When using a dedicated maker market the maturation clock resets to when
        // that specific market opened, not the hourly market. We approximate by
        // checking how long the bot has been running (conservative).
        let secs_since_market_start = (Utc::now() - ctx.market_started_at).num_seconds();
        if secs_since_market_start < config::MAKER_MIN_MARKET_AGE_SECS {
            debug!(" MakerStrategy blocked: market too young ({}s < {}s maturation gate)",
                secs_since_market_start, config::MAKER_MIN_MARKET_AGE_SECS);
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
        // If the ask side has significantly more depth than the bid side, the
        // book is skewed by sellers.  Bids posted into a one-sided book are
        // likely to be adversely selected by motivated sellers on a falling market.
        let yes_bid_depth = snapshot.yes_bid_depth;
        let yes_ask_depth = snapshot.yes_ask_depth;
        let no_bid_depth  = snapshot.no_bid_depth;
        let no_ask_depth  = snapshot.no_ask_depth;

        // Compute imbalance: ask_depth / bid_depth.  When bid_depth is zero we
        // treat it as maximally imbalanced to avoid division by zero.
        let yes_book_ok = yes_bid_depth > dec!(0)
            && (yes_ask_depth / yes_bid_depth) <= config::MAKER_MAX_BOOK_IMBALANCE_RATIO;
        let no_book_ok  = no_bid_depth > dec!(0)
            && (no_ask_depth  / no_bid_depth)  <= config::MAKER_MAX_BOOK_IMBALANCE_RATIO;

        if !yes_book_ok && !no_book_ok {
            debug!(" MakerStrategy blocked: both sides orderbook heavily skewed (YES ask/bid depth ratio: {:.1}, NO ask/bid: {:.1})",
                if yes_bid_depth > dec!(0) { yes_ask_depth / yes_bid_depth } else { dec!(99) },
                if no_bid_depth  > dec!(0) { no_ask_depth  / no_bid_depth  } else { dec!(99) });
            return Ok(StrategySignal::NoSignal);
        }

        // ── Inventory skew ────────────────────────────────────────────────────
        let (yes_inv_value, no_inv_value) = {
            let pos_map = ctx.positions.lock().await;
            let yv = pos_map.get(&("MakerStrategy".to_string(), market.yes_token))
                .map(|p| p.shares * p.avg_entry).unwrap_or(dec!(0));
            let nv = pos_map.get(&("MakerStrategy".to_string(), market.no_token))
                .map(|p| p.shares * p.avg_entry).unwrap_or(dec!(0));
            (yv, nv)
        };

        // imbalance: +1 = all YES, -1 = all NO, 0 = balanced
        let imbalance = ((yes_inv_value - no_inv_value) / config::MAKER_MAX_EXPOSURE_USDC)
            .clamp(dec!(-1), dec!(1));
        let skew = imbalance * config::MAKER_INVENTORY_SKEW_MAX;

        // ── Velocity bias uses the hourly oracle (always) ─────────────────────
        let velocity = ctx.snapshot.velocity;
        let velocity_bias_strong_negative = velocity <= -MAKER_VELOCITY_BIAS_THRESHOLD;
        let velocity_bias_strong_positive = velocity >= MAKER_VELOCITY_BIAS_THRESHOLD;

        // ── Compute spread-relative bid improvement for each side ─────────────
        let yes_spread = yes_ask - yes_bid;
        let no_spread  = no_ask  - no_bid;

        let yes_improvement = if yes_spread > dec!(0) {
            (yes_spread * config::MAKER_BID_IMPROVEMENT_RATIO)
                .max(config::MAKER_MIN_BID_IMPROVEMENT)
                .min(config::MAKER_MAX_BID_IMPROVEMENT)
        } else {
            config::MAKER_BID_IMPROVEMENT
        };
        let no_improvement = if no_spread > dec!(0) {
            (no_spread * config::MAKER_BID_IMPROVEMENT_RATIO)
                .max(config::MAKER_MIN_BID_IMPROVEMENT)
                .min(config::MAKER_MAX_BID_IMPROVEMENT)
        } else {
            config::MAKER_BID_IMPROVEMENT
        };

        // Apply skew: heavy YES → lower YES bid price, raise NO bid price.
        // skew > 0 when heavy YES, so YES bid decreases and NO bid increases.
        let yes_bid_price = (yes_bid + yes_improvement - skew).max(dec!(0.01));
        let no_bid_price  = (no_bid  + no_improvement  + skew).max(dec!(0.01));

        // ── Per-side qualification checks ─────────────────────────────────────
        let yes_qualifies = yes_book_ok                             // ask-side not overwhelming bid-side
            && yes_spread >= config::MAKER_MIN_SPREAD
            && yes_bid_price >= config::MAKER_MIN_ENTRY_PRICE
            && yes_bid_price <= config::MAKER_MAX_ENTRY_PRICE
            && yes_bid_price < yes_ask               // never cross the book (guards against stale WS + skew edge cases)
            && no_bid <= config::MAKER_MAX_COMPLEMENTARY_PRICE  // complementary: market not too directional
            && !velocity_bias_strong_negative;           // don't post YES into a falling oracle

        let no_qualifies = no_book_ok                              // ask-side not overwhelming bid-side
            && no_spread >= config::MAKER_MIN_SPREAD
            && no_bid_price >= config::MAKER_MIN_ENTRY_PRICE
            && no_bid_price <= config::MAKER_MAX_ENTRY_PRICE
            && no_bid_price < no_ask                // never cross the book (guards against stale WS + skew edge cases)
            && yes_bid <= config::MAKER_MAX_COMPLEMENTARY_PRICE  // complementary: market not too directional
            && !velocity_bias_strong_positive;           // don't post NO into a rising oracle

        if !yes_qualifies && !no_qualifies {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Combined price guard ──────────────────────────────────────────────
        // If YES_bid + NO_bid >= MAKER_MAX_COMBINED_BID, takers can sell both
        // legs to us and profit from the $1.00 settlement — a free arb at our expense.
        // When both sides qualify but combined price is too high, suppress the
        // more expensive side (narrower spread = less edge = higher priority to drop).
        let (final_yes, final_no) = if yes_qualifies && no_qualifies {
            let combined = yes_bid_price + no_bid_price;
            if combined >= config::MAKER_MAX_COMBINED_BID {
                debug!("⚠️ MakerQuote: combined bid ${:.4} >= threshold ${:.4} — suppressing expensive side",
                    combined, config::MAKER_MAX_COMBINED_BID);
                // Drop the side with the tighter spread (less edge)
                if yes_spread <= no_spread {
                    (None, Some(no_bid_price))
                } else {
                    (Some(yes_bid_price), None)
                }
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

        debug!(" MakerQuote: YES={:?} NO={:?} | imbalance={:.2} skew={:.4}",
            final_yes, final_no, imbalance, skew);

        Ok(StrategySignal::MakerQuote {
            yes_bid_price: final_yes,
            no_bid_price: final_no,
        })
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        // Use the same venue selection as evaluate_entry
        let market = ctx.maker_market.as_ref().unwrap_or(&ctx.market);
        let snapshot = ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot);

        // Seconds until this market resolves — used to tighten the stop near expiry.
        let secs_to_expiry = market.market_close_time
            .map(|t| (t - Utc::now()).num_seconds())
            .unwrap_or(9999);

        // Near-expiry: binary markets can gap 30-60% on tiny BTC moves as the
        // market converges toward 0 or 1. Halve the stop-loss tolerance when
        // within MAKER_LATE_MARKET_STOP_TIGHTEN_SECS of resolution.
        let effective_stop_pct = if secs_to_expiry < config::MAKER_LATE_MARKET_STOP_TIGHTEN_SECS {
            config::MAKER_LATE_MARKET_STOP_LOSS_PERCENT
        } else {
            config::MAKER_STOP_LOSS_PERCENT
        };

        let pos_map = ctx.positions.lock().await;

        for token_id in [market.yes_token, market.no_token] {
            let Some(position) = pos_map.get(&("MakerStrategy".to_string(), token_id)) else {
                continue;
            };

            let bid = if token_id == market.yes_token {
                snapshot.yes_bid
            } else {
                snapshot.no_bid
            };

            if position.avg_entry <= dec!(0) { continue; }

            let profit_pct = (bid - position.avg_entry) / position.avg_entry;
            let secs_since_open = (Utc::now() - position.opened_at).num_seconds();

            // Take-profit: only after fill confirmed on-chain to avoid phantom exits.
            if position.fill_confirmed_at.is_some() && profit_pct >= config::MAKER_TARGET_PROFIT_PERCENT {
                return Ok(StrategySignal::Exit {
                    token_id,
                    reason: format!(
                        "Maker take-profit: bid=${:.4}, entry=${:.4}, gain={:.2}%",
                        bid, position.avg_entry, profit_pct * dec!(100)
                    ),
                });
            }

            // Stop-loss: requires fill confirmation (prevents phantom stops on GTD orders that
            // never filled). The previous MIN_HOLD_SECS_BEFORE_STOP_LOSS gate is removed:
            // fill_confirmed_at.is_some() is already the correct phantom guard — adding a
            // 90s timer on top delayed stops and turned 8% losses into 39%+ disasters.
            // Near-expiry: effective_stop_pct is halved (see above) so the stop fires earlier
            // before binary resolution dynamics cause a catastrophic gap.
            if position.fill_confirmed_at.is_some()
                && profit_pct <= -effective_stop_pct
            {
                return Ok(StrategySignal::Exit {
                    token_id,
                    reason: format!(
                        "Maker stop-loss: bid=${:.4}, entry=${:.4}, loss={:.2}% ({}s since open, stop={:.1}%, {}s to expiry)",
                        bid, position.avg_entry, profit_pct * dec!(100), secs_since_open,
                        effective_stop_pct * dec!(100), secs_to_expiry,
                    ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use alloy::primitives::U256;
    use chrono::{Duration, Utc};
    use crate::state::{MarketConfig, MarketSnapshot, PositionMap};

    fn make_ctx(
        yes_bid: Decimal, yes_ask: Decimal,
        no_bid: Decimal, no_ask: Decimal,
        secs_to_expiry: i64,
        positions: PositionMap,
    ) -> StrategyContext {
        StrategyContext {
            market: MarketConfig {
                yes_token: U256::from(1u64),
                no_token: U256::from(2u64),
                market_name: "Test Market".to_string(),
                market_close_time: Some(Utc::now() + Duration::seconds(secs_to_expiry)),
                strike_price: None,
                is_neg_risk: false,
                yes_fee_bps: 100,
                no_fee_bps: 100,
            },
            snapshot: MarketSnapshot {
                yes_bid, yes_ask,
                yes_bid_depth: dec!(500),
                yes_ask_depth: dec!(500),
                no_bid, no_ask,
                no_bid_depth: dec!(500),
                no_ask_depth: dec!(500),
                oracle_price: dec!(74000),
                velocity: dec!(0),
                velocity_1s: dec!(0),
                acceleration: dec!(0),
                funding_rate: dec!(0),
                oracle_drift_60m: Default::default(),
                timestamp: Utc::now(),
            },
            positions: Arc::new(tokio::sync::Mutex::new(positions)),
            crypto_filter: "btc".to_string(),
            market_started_at: Utc::now() - Duration::seconds(1200),
            maker_market: None,
            maker_snapshot: None,
        }
    }

    #[tokio::test]
    async fn test_maker_no_entry_market_too_young() {
        let strategy = MakerStrategyImpl;
        let mut ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.55), dec!(0.65), 2400, PositionMap::new());
        ctx.market_started_at = Utc::now() - Duration::seconds(60);
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn test_maker_entry_wide_spread_both_sides() {
        let strategy = MakerStrategyImpl;
        // YES spread = 0.10, NO spread = 0.10, combined bid ~0.62 < 0.90 → both sides qualify
        let ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.35), dec!(0.45), 2400, PositionMap::new());
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::MakerQuote { yes_bid_price: Some(_), no_bid_price: Some(_) }));
    }

    #[tokio::test]
    async fn test_maker_no_entry_tight_spread() {
        let strategy = MakerStrategyImpl;
        // Both spreads = 0.01 < 0.05 min
        let ctx = make_ctx(dec!(0.50), dec!(0.51), dec!(0.49), dec!(0.50), 2400, PositionMap::new());
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn test_maker_no_entry_too_close_to_expiry() {
        let strategy = MakerStrategyImpl;
        let ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.35), dec!(0.45), 300, PositionMap::new());
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn test_maker_combined_price_guard() {
        let strategy = MakerStrategyImpl;
        // YES bid=0.44 + improvement≈0.03 = 0.47, NO bid=0.44 + improvement≈0.03 = 0.47
        // combined = 0.94 >= 0.90 → one side should be suppressed
        let ctx = make_ctx(dec!(0.44), dec!(0.54), dec!(0.44), dec!(0.54), 2400, PositionMap::new());
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        // Should get MakerQuote with at most one Some side when combined is too high
        match signal {
            StrategySignal::MakerQuote { yes_bid_price, no_bid_price } => {
                // At least one side must be None when combined price guard fires
                let both_some = yes_bid_price.is_some() && no_bid_price.is_some();
                if both_some {
                    let combined = yes_bid_price.unwrap() + no_bid_price.unwrap();
                    assert!(combined < config::MAKER_MAX_COMBINED_BID,
                        "Combined bid ${} should be < threshold ${}", combined, config::MAKER_MAX_COMBINED_BID);
                }
            }
            StrategySignal::NoSignal => {} // also acceptable if price gate blocks
            other => panic!("Unexpected signal: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_maker_inventory_skew() {
        use crate::state::Position;
        let strategy = MakerStrategyImpl;
        let yes_token = U256::from(1u64);
        let mut positions = PositionMap::new();
        // Heavy YES inventory: $14 in YES, nothing in NO
        positions.insert(("MakerStrategy".to_string(), yes_token), Position {
            shares: dec!(40),
            avg_entry: dec!(0.35),
            opened_at: Utc::now(),
            close_time: None,
            market_name: "Test".to_string(),
            pair_token_id: yes_token,
            fill_confirmed_at: Some(Utc::now()),
            paired_leg_token_id: None,
        });
        let ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.35), dec!(0.45), 2400, positions);
        let signal = strategy.evaluate_entry(&ctx).await.unwrap();
        if let StrategySignal::MakerQuote { yes_bid_price, no_bid_price } = signal {
            // With heavy YES, YES bid should be lower than NO bid relative to their baselines
            if let (Some(y), Some(n)) = (yes_bid_price, no_bid_price) {
                // YES base bid = 0.30+improvement, NO base bid = 0.35+improvement
                // After skew: YES is pushed down, NO is pushed up → y should be further below baseline
                let yes_vs_mid = dec!(0.35) - y;  // how far below yes mid
                let no_vs_mid  = dec!(0.40) - n;  // how far below no mid
                assert!(yes_vs_mid > no_vs_mid, "Heavy YES should result in lower YES bid (more room below mid)");
            }
        }
    }

    #[tokio::test]
    async fn test_maker_exit_take_profit() {
        use crate::state::Position;
        let strategy = MakerStrategyImpl;
        let yes_token = U256::from(1u64);
        let mut positions = PositionMap::new();
        positions.insert(("MakerStrategy".to_string(), yes_token), Position {
            shares: dec!(20),
            avg_entry: dec!(0.30),
            opened_at: Utc::now(),
            close_time: None,
            market_name: "Test Market".to_string(),
            pair_token_id: yes_token,
            fill_confirmed_at: Some(Utc::now()),
            paired_leg_token_id: None,
        });
        let ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.55), dec!(0.65), 2400, positions);
        let mut ctx2 = ctx.clone();
        ctx2.snapshot.yes_bid = dec!(0.33); // 10% gain > 8% TP
        let signal = strategy.evaluate_exit(&ctx2).await.unwrap();
        assert!(matches!(signal, StrategySignal::Exit { .. }));
    }

    #[tokio::test]
    async fn test_maker_exit_stop_loss() {
        use crate::state::Position;
        let strategy = MakerStrategyImpl;
        let yes_token = U256::from(1u64);
        let mut positions = PositionMap::new();
        positions.insert(("MakerStrategy".to_string(), yes_token), Position {
            shares: dec!(20),
            avg_entry: dec!(0.30),
            opened_at: Utc::now(),
            close_time: None,
            market_name: "Test Market".to_string(),
            pair_token_id: yes_token,
            // Fill confirmed: stop-loss is now eligible immediately without a hold timer.
            fill_confirmed_at: Some(Utc::now()),
            paired_leg_token_id: None,
        });
        let ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.55), dec!(0.65), 2400, positions);
        let mut ctx2 = ctx.clone();
        ctx2.snapshot.yes_bid = dec!(0.27); // -10% loss > 8% SL
        let signal = strategy.evaluate_exit(&ctx2).await.unwrap();
        assert!(matches!(signal, StrategySignal::Exit { .. }));
    }

    #[tokio::test]
    async fn test_maker_exit_stop_loss_no_fire_without_fill_confirmation() {
        use crate::state::Position;
        let strategy = MakerStrategyImpl;
        let yes_token = U256::from(1u64);
        let mut positions = PositionMap::new();
        positions.insert(("MakerStrategy".to_string(), yes_token), Position {
            shares: dec!(20),
            avg_entry: dec!(0.30),
            opened_at: Utc::now(),
            close_time: None,
            market_name: "Test Market".to_string(),
            pair_token_id: yes_token,
            fill_confirmed_at: None, // NOT yet confirmed — stop must be blocked
            paired_leg_token_id: None,
        });
        let ctx = make_ctx(dec!(0.30), dec!(0.40), dec!(0.55), dec!(0.65), 2400, positions);
        let mut ctx2 = ctx.clone();
        ctx2.snapshot.yes_bid = dec!(0.20); // -33% loss but no fill confirmation
        let signal = strategy.evaluate_exit(&ctx2).await.unwrap();
        // Stop-loss must NOT fire: fill_confirmed_at is None (phantom guard)
        assert!(matches!(signal, StrategySignal::NoSignal));
    }

    #[tokio::test]
    async fn test_maker_exit_near_expiry_tighter_stop() {
        use crate::state::Position;
        let strategy = MakerStrategyImpl;
        let yes_token = U256::from(1u64);
        let mut positions = PositionMap::new();
        positions.insert(("MakerStrategy".to_string(), yes_token), Position {
            shares: dec!(20),
            avg_entry: dec!(0.38),
            opened_at: Utc::now(),
            close_time: None,
            market_name: "Test Market".to_string(),
            pair_token_id: yes_token,
            fill_confirmed_at: Some(Utc::now()),
            paired_leg_token_id: None,
        });
        // secs_to_expiry = 1500 < MAKER_LATE_MARKET_STOP_TIGHTEN_SECS = 2700
        let ctx = make_ctx(dec!(0.38), dec!(0.48), dec!(0.55), dec!(0.65), 1500, positions);
        let mut ctx2 = ctx.clone();
        // -5.3% loss: above normal 8% stop but below the 4% tighter near-expiry stop
        ctx2.snapshot.yes_bid = dec!(0.36);
        let signal = strategy.evaluate_exit(&ctx2).await.unwrap();
        // Near-expiry: stop should fire at 4% threshold (0.36 / 0.38 - 1 = -5.3% < -4%)
        assert!(matches!(signal, StrategySignal::Exit { .. }));
    }
}
