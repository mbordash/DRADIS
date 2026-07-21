/// Arbitrage Strategy
///
/// Hedged, two-sided trades that exploit the YES+NO spread inefficiency.
///
/// ── Maker vs Taker ──────────────────────────────────────────────────────────
/// Polymarket charges 0% fees on maker (GTC/post-only) orders, but 1000 bps
/// on taker (FAK) fills.  At 10% round-trip cost, taker arb is structurally
/// impossible on a $1.00 binary market.
///
/// This strategy posts GTC maker bids on BOTH YES and NO tokens simultaneously
/// at their current best-bid prices.  If both legs fill:
///   cost  = YES_bid + NO_bid  (no fee — maker fill)
///   payout = $1.00  (settlement)
///   profit = 1.00 − YES_bid − NO_bid
///
/// Entry fires only when that profit > ARBITRAGE_PROFIT_THRESHOLD (1.5¢).
/// In practice combined bids of 0.97-0.98 are common on daily markets,
/// yielding 2-3¢ net per dollar — viable as maker, not as taker.
///
/// Exit: collect settlement at $1.00 or sell early when bid_sum converges
/// to EARLY_EXIT_COMBINED_BID_THRESHOLD (0.995).  Exit legs use FAK (taker)
/// since we need a guaranteed fill before market close.

use async_trait::async_trait;
use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::orchestrator::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus, OrderParams};
use crate::vipers::is_drawdown_limit_hit;
use crate::venues::core::TimeInForce;
use tracing::{debug, info};
use std::sync::Mutex;
use std::time::Instant;

const STRATEGY_NAME: &str = "ArbitrageStrategy";

#[derive(Default)]
pub struct ArbitrageStrategyImpl {
    /// Quote-persistence debounce state: (market condition_id, first Instant the
    /// profitable bid_sum was observed). Reset whenever the condition breaks or
    /// the market rotates. See ARB_QUOTE_PERSISTENCE_SECS.
    profitable_since: Mutex<Option<(String, Instant)>>,
}

#[async_trait]
impl Strategy for ArbitrageStrategyImpl {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;

        if !dc.enable_arbitrage {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Global Risk Check ────────────────────────────────────────────────
        if is_drawdown_limit_hit(ctx.session_pnl, ctx.starting_collateral) {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Maker venue required (orphan-loss guard) ─────────────────────────
        // The profitable arbitrage regime is the near-settlement daily/window
        // market, where both legs are deep (≈0.98+0.0x) and the spread converges
        // to settlement — directional/orphan risk is minimal.
        //
        // Previously, when the daily/window maker venue was unavailable (e.g. the
        // current day's market settled and the next day's is not yet listed) this
        // code silently fell back to `ctx.market` (the volatile HOURLY book via
        // `unwrap_or`). On the hourly venue mid-window a one-sided maker fill is a
        // real directional bet: a fast underlying move fills only one leg, leaving
        // a naked position that the arbiter must flatten at a loss (the 2026-06-19
        // 11:50 ET episode — NO leg filled, BTC rallied, YES ran away, forced
        // flatten −$1.44). Refuse to enter unless the dedicated maker venue (and
        // its snapshot) are actually present.
        let (Some(market), Some(snapshot)) =
            (ctx.maker_market.as_ref(), ctx.maker_snapshot.as_ref())
        else {
            debug!(" Arb skipped — no maker (daily/window) venue available; refusing hourly fallback");
            return Ok(StrategySignal::NoSignal);
        };

        let yes_bid = snapshot.yes_bid;
        let no_bid  = snapshot.no_bid;
        let yes_ask = snapshot.yes_ask;
        let no_ask  = snapshot.no_ask;

        // ── Locked / inverted-spread guard ───────────────────────────────────
        // If YES or NO bid ≥ ask, the WS snapshot is stale or the market is at
        // an inflection.  We must NOT place a post-only bid at or above the ask
        // (it would be rejected as "order crosses book").  Rather than silently
        // lowering the bid (which can make the arb unprofitable), bail out early.
        // Safe bids are computed below only for normal (bid < ask) books.
        if yes_bid >= yes_ask || no_bid >= no_ask {
            debug!(
                " Arb skipped — locked/inverted spread: YES {:.3}/{:.3}  NO {:.3}/{:.3}",
                yes_bid, yes_ask, no_bid, no_ask
            );
            return Ok(StrategySignal::NoSignal);
        }

        // ── Maker arb profitability gate (0% fee on GTC fills) ───────────────
        if !is_maker_arb_profitable(yes_bid, no_bid, dc.arbitrage_profit_threshold) {
            // Condition broke — reset the persistence clock.
            *self.profitable_since.lock().unwrap() = None;
            return Ok(StrategySignal::NoSignal);
        }

        // ── Quote-persistence debounce (anti-phantom-arb) ────────────────────
        // A single-snapshot profit check passes on transient book dislocations
        // during fast BTC moves ("phantom arbs" — 2026-07-15 09:21: YES filled
        // @ $0.90, the book reverted within 12s, NO never filled, orphan
        // flattened at a loss). Genuine resting-quote arbs sit on the book for
        // minutes, so require the profitable condition to persist continuously
        // for ARB_QUOTE_PERSISTENCE_SECS before entering.
        {
            let mut guard = self.profitable_since.lock().unwrap();
            match guard.as_ref() {
                Some((cid, first_seen)) if *cid == market.condition_id => {
                    let held = first_seen.elapsed().as_secs();
                    if held < crate::config::ARB_QUOTE_PERSISTENCE_SECS {
                        debug!(
                            " Arb debounce — profitable quote held {}s < {}s required",
                            held, crate::config::ARB_QUOTE_PERSISTENCE_SECS
                        );
                        return Ok(StrategySignal::NoSignal);
                    }
                    // Persisted long enough — fall through to the remaining gates.
                }
                _ => {
                    // First sighting (or market rotated) — start the clock.
                    *guard = Some((market.condition_id.clone(), Instant::now()));
                    debug!(" Arb debounce — profitable quote first seen, starting persistence clock");
                    return Ok(StrategySignal::NoSignal);
                }
            }
        }

        // ── Conviction (anti-coin-flip) gate ─────────────────────────────────
        // The core orphan-prevention check. The profitable maker-arb regime is the
        // DEEP near-settlement market (dominant leg ≈0.90+, complement ≈0.05): both
        // books are thick and the outcome near-decided, so resting GTC bids on each
        // side fill reliably. The danger regime is the ≈0.50/0.50 coin-flip — the
        // combined bid can still clear the profit threshold (so every other gate
        // passes), but a one-tick BTC move fills the leaning leg while the other
        // runs away, leaving a naked orphan we flatten at a loss. Lifetime, this
        // single failure mode lost −$6.66 across 15 orphan flattens (2026-06-26).
        // Require the dominant leg's bid ≥ conviction floor to admit only decided
        // markets and reject the coin-flip zone outright.
        let dominant_leg_bid = yes_bid.max(no_bid);
        if dominant_leg_bid < dc.arbitrage_min_leg_conviction {
            debug!(
                " Arb conviction gate — dominant leg bid {:.3} < min {:.3} — skipping \
                 (near-coin-flip market; leg-fill is directional → orphan risk)",
                dominant_leg_bid, dc.arbitrage_min_leg_conviction
            );
            return Ok(StrategySignal::NoSignal);
        }

        // ── Safe maker prices: cap bids one tick below ask ───────────────────
        // A GTC post-only order is rejected with "order crosses book" if
        // bid >= ask.  This can happen on tight markets where the WS snapshot
        // has bid == ask or a stale/inverted spread.  Cap each leg at
        // ask − 0.01 to guarantee the order rests on the book as a maker.
        // The locked-spread guard above ensures yes_bid < yes_ask at this point,
        // so the .min() only fires on the rare tight-but-not-locked case.
        let safe_yes_bid = yes_bid.min(yes_ask - dec!(0.01));
        let safe_no_bid  = no_bid.min(no_ask  - dec!(0.01));

        // Re-validate profitability at the capped prices — if we had to lower
        // the bid(s) the spread may no longer cover the threshold.
        if !is_maker_arb_profitable(safe_yes_bid, safe_no_bid, dc.arbitrage_profit_threshold) {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Liquidity gap gate ───────────────────────────────────────────────
        // A GTC maker bid resting far below the current ask will almost never
        // fill within MAX_WAIT_SECS — causing a one-sided fill and unhedged
        // directional exposure (the root cause of the May-13 orphan episode).
        // Skip entry if either leg's ask is more than arbitrage_max_fill_gap
        // above our safe bid — that means no real counterparty is near our price.
        let yes_fill_gap = yes_ask - safe_yes_bid;
        let no_fill_gap  = no_ask  - safe_no_bid;
        if yes_fill_gap > dc.arbitrage_max_fill_gap || no_fill_gap > dc.arbitrage_max_fill_gap {
            debug!(
                " Arb liquidity gap too wide — YES gap {:.3} NO gap {:.3} (max {:.3}) — skipping",
                yes_fill_gap, no_fill_gap, dc.arbitrage_max_fill_gap
            );
            return Ok(StrategySignal::NoSignal);
        }

        // ── Hard ask-price ceiling (always active) ───────────────────────────
        // When either leg's current ask exceeds the ceiling the market is
        // directional: sellers on that leg are priced above our acceptable
        // level and a GTC maker bid is very unlikely to fill within
        // MAX_WAIT_SECS, leaving a one-sided orphan.
        //
        // Checking the *ask* (not the bid) is critical.  The original legacy
        // price cap used safe_yes_bid (bid ≤ ask − 0.01) which passed silently
        // when YES bid = $0.60 but YES ask had marched to $0.63–$0.68 on a
        // strongly-directional underlying move (root cause of the May-28
        // orphan episode — 11 failed attempts, 8 YES shares left on-chain).
        //
        // This gate always fires regardless of whether orderbook depth data is
        // present, complementing the OBI gate that follows.
        // ── Complement-leg ask ceiling (orphan guard) ────────────────────────
        // Identify the COMPLEMENT (cheaper) leg by bid. The deep near-settlement
        // regime — the ONLY historically profitable one (7/7 wins in early June,
        // zero orphans) — intentionally has a high-priced DOMINANT leg (0.90+ ask),
        // so the legacy MAX-leg ask ceiling (0.60) rejected every profitable entry.
        // Stacked with the conviction gate (dominant bid ≥ 0.80) it made the
        // admissible set EMPTY → zero arb trades since 2026-06-26. Orphan risk lives
        // on the COMPLEMENT side: a decided market has a cheap complement (≈0.05);
        // a complement ask above the ceiling means the market is near coin-flip and
        // undecided → reject. The dominant leg being expensive is expected and safe.
        let yes_is_dominant = yes_bid >= no_bid;
        let complement_ask = if yes_is_dominant { no_ask } else { yes_ask };
        if complement_ask > dc.arbitrage_max_leg_price {
            debug!(
                " Arb ask ceiling — complement leg ask {:.3} > limit {:.3} — skipping \
                 (undecided/near-coin-flip market)",
                complement_ask, dc.arbitrage_max_leg_price
            );
            return Ok(StrategySignal::NoSignal);
        }

        // ── Order-book imbalance (OBI) fill-rate gate ────────────────────────
        // Complements the ask-price ceiling above with a dynamic, depth-based
        // measurement.  We place GTC BIDS on both legs; fill probability falls
        // when one side has few resting asks (sellers absent).
        //
        // OBI = (bid_depth − ask_depth) / (bid_depth + ask_depth) ∈ [−1, +1]
        //  +1.0 → all depth is bids, zero sellers → our bid will NOT fill
        //  −1.0 → all depth is asks, abundant sellers → fast fill
        //
        // Primary gate: if either leg OBI > arbitrage_max_leg_obi (default 0.50),
        //   skip — too directional, fill asymmetry likely.
        //
        // Fallback gate: if BOTH legs have zero depth (snapshot unavailable),
        //   fall back to a bid-price cap (arbitrage_max_leg_price = 0.60)
        //   as a secondary backstop (ask ceiling above already guards this case).
        let yes_total_depth = snapshot.yes_bid_depth + snapshot.yes_ask_depth;
        let no_total_depth  = snapshot.no_bid_depth  + snapshot.no_ask_depth;

        let depth_available = yes_total_depth > dec!(0) || no_total_depth > dec!(0);

        if depth_available {
            // Fill-rate check on the COMPLEMENT (cheaper) leg only. The DOMINANT
            // leg of a decided market is naturally bid-heavy (few sellers of a
            // near-certain winner → OBI ≈ +1); gating on it (or on cross-leg
            // asymmetry, which is inherently ≈2.0 in a hedged deep pair) rejected
            // the entire profitable deep-arb regime. What actually matters for a
            // clean hedge is that the COMPLEMENT leg can fill: if its book is so
            // bid-heavy that no sellers sit near our maker bid (OBI > limit), the
            // complement bid won't fill and we'd hold the dominant leg alone.
            // A dominant-only orphan is a near-certain winner (low risk), but we
            // still skip to avoid an unhedged entry. Orphan COST is independently
            // bounded by the rescue-profit gate and the per-market re-entry lockout
            // below, so the removed max-OBI / asymmetry gates are not load-bearing.
            let complement_total_depth = if yes_is_dominant { no_total_depth } else { yes_total_depth };
            let complement_obi = if complement_total_depth > dec!(0) {
                if yes_is_dominant {
                    (snapshot.no_bid_depth - snapshot.no_ask_depth) / no_total_depth
                } else {
                    (snapshot.yes_bid_depth - snapshot.yes_ask_depth) / yes_total_depth
                }
            } else {
                // Complement book empty — no sellers to fill our bid → unfillable.
                dec!(1)
            };
            if complement_obi > dc.arbitrage_max_leg_obi {
                debug!(
                    " Arb OBI gate — complement leg OBI {:.3} > limit {:.3} — skipping \
                     (complement bid unlikely to fill → unhedged entry)",
                    complement_obi, dc.arbitrage_max_leg_obi
                );
                return Ok(StrategySignal::NoSignal);
            }
        } else {
            // No orderbook depth data in snapshot — fall back to complement price cap.
            let complement_bid = if yes_is_dominant { safe_no_bid } else { safe_yes_bid };
            if complement_bid > dc.arbitrage_max_leg_price {
                debug!(
                    " Arb price-cap fallback (no depth data) — complement bid {:.3} > limit {:.3} — skipping",
                    complement_bid, dc.arbitrage_max_leg_price
                );
                return Ok(StrategySignal::NoSignal);
            }
        }

        // ── Rescue-profit gate ───────────────────────────────────────────────
        // Even when the maker bid sum looks profitable, the arbiter may need to
        // FAK-buy the MISSING leg at its current ask if only one leg fills.
        // That taker rescue costs: filled_leg_entry + missing_ask + 1tick + buffer.
        // If EITHER rescue path costs ≥ $1.00 settlement payout the trade is only
        // "profitable" if both legs fill simultaneously — which is never guaranteed.
        // Block entry unless BOTH single-leg failure cases are recoverable:
        //   YES fills first → rescue by buying NO at no_ask + 1 tick
        //   NO  fills first → rescue by buying YES at yes_ask + 1 tick
        let rehedge_buf = dc.arb_fak_rehedge_buffer;
        let yes_rescue_cost = safe_yes_bid + no_ask  + dec!(0.01) + rehedge_buf;
        let no_rescue_cost  = safe_no_bid  + yes_ask + dec!(0.01) + rehedge_buf;
        if yes_rescue_cost >= dc.arb_max_rescue_cost || no_rescue_cost >= dc.arb_max_rescue_cost {
            debug!(
                " Arb rescue-profit gate — YES rescue {:.4} or NO rescue {:.4} ≥ ${:.2} — skipping \
                 (single-leg orphan materially unrecoverable at live asks)",
                yes_rescue_cost, no_rescue_cost, dc.arb_max_rescue_cost
            );
            return Ok(StrategySignal::NoSignal);
        }

        // ── Strategy Exposure Check ──────────────────────────────────────────
        let trade_size = dc.arbitrage_position_size_usdc;

        // ── Available collateral gate ────────────────────────────────────────
        // Both legs are placed simultaneously and each costs ~trade_size/2 in
        // USDC from the collateral balance.  If available_collateral < trade_size
        // the NO leg will always fail with "not enough balance" from the CLOB.
        // A 5% buffer covers rounding and any open-order holds against the balance.
        if ctx.available_collateral < trade_size * dec!(1.05) {
            debug!(
                " Arb skipped — available collateral ${:.2} < required ${:.2}",
                ctx.available_collateral, trade_size * dec!(1.05)
            );
            return Ok(StrategySignal::NoSignal);
        }
        // ── Per-market re-entry lockout ──────────────────────────────────────
        // Once we've committed a hedged pair to THIS market this session, never
        // open a second pair on it — hold the single pair to settlement instead
        // of churning re-entries at the coin-flip midpoint (where one maker leg
        // always gets picked off first → orphan flatten). Root cause of the
        // 2026-06-21 overnight cascade: after a clean pair settled, the viper
        // re-armed on the same daily market every tick and bled out 5 orphans.
        if let Some(locks) = ctx.arb_market_lockouts.as_ref() {
            let locked = locks.lock().await;
            if locked.contains(&market.yes_token) || locked.contains(&market.no_token) {
                debug!(" Arb skipped — market already traded this session (re-entry lockout)");
                return Ok(StrategySignal::NoSignal);
            }
        }

        let current_exposure = {
            let pos_map = ctx.positions.lock().await;

            // ── Per-token existence guard ─────────────────────────────────────
            // If we already hold either leg of THIS specific market, skip entry.
            // Without this check the strategy returns an Entry signal on every
            // 50ms tick (because total exposure < max_exposure_usdc) even though
            // main.rs would silently block it at the positions-map check; the
            // repeated evaluate_entry calls waste CPU and flood the executor INFO
            // line with spurious  on every tick.
            // This guards against both re-adopted on-chain positions and live GTC
            // orders waiting for confirmation (fill_confirmed_at == None).
            if pos_map.contains_key(&(STRATEGY_NAME.to_string(), market.yes_token.clone()))
                || pos_map.contains_key(&(STRATEGY_NAME.to_string(), market.no_token.clone()))
            {
                debug!(" Arb skipped — already hold YES or NO leg for this market");
                return Ok(StrategySignal::NoSignal);
            }

            pos_map.iter()
                .filter(|((s, _), _)| s == STRATEGY_NAME)
                .map(|(_, p)| p.shares * p.avg_entry)
                .sum::<Decimal>()
        };
        if current_exposure + trade_size > dc.arbitrage_max_exposure_usdc {
            return Ok(StrategySignal::NoSignal);
        }

        // ── Post GTC maker bids on both legs ─────────────────────────────────
        // Equal shares on both legs are required for a true hedge.
        // Buying equal dollars gives unequal shares (e.g. 24 YES vs 17 NO at 0.41/0.58),
        // leaving the cheaper leg unhedged and creating a directional P&L on settlement.
        // Instead: spend the full budget on N balanced pairs where
        //   N = trade_size / (safe_yes_bid + safe_no_bid)
        // so YES_cost + NO_cost = trade_size and every YES share has one NO share.
        let pair_shares = trade_size / (safe_yes_bid + safe_no_bid);

        // ── Expensive-leg-first ordering ──────────────────────────────────────
        // Assign Leg A (params) = the higher-bid (more expensive) leg,
        // Leg B (pair_params) = the lower-bid (cheaper) leg.
        //
        // Rationale: The ARB ARBITER fires when "Leg A filled, Leg B missing"
        // and attempts a FAK taker-buy for the missing Leg B.  If Leg B is the
        // cheap leg (small ask), its current ask ≈ bid + 1 tick — very close to
        // our breakeven ceiling — giving the emergency FAK the best possible
        // chance of succeeding.
        //
        // The opposite ordering (cheap-first) caused the May-27 orphan loss:
        //   - YES (cheap, $0.18 bid) filled immediately as sellers unloaded
        //   - NO  (expensive, $0.81 bid) never filled as the directional move
        //     drove NO ask to $0.83
        //   - FAK re-hedge for NO needed limit ≥ $0.80 but ask was $0.83 → failed
        //   - Orphan exit: sold YES at $0.15 → −$0.29 realized loss
        //
        // With expensive-first, if NO (Leg A) fills and YES (Leg B) orphans, the
        // FAK gap for cheap YES is ≈ 1–2 ticks instead of 3–4, substantially
        // improving re-hedge success when markets have shifted slightly.
        let (leg_a_token, leg_a_price, leg_b_token, leg_b_price) =
            if safe_yes_bid >= safe_no_bid {
                // YES is the expensive leg — place it first
                (market.yes_token.clone(), safe_yes_bid, market.no_token.clone(), safe_no_bid)
            } else {
                // NO is the expensive leg — place it first
                (market.no_token.clone(), safe_no_bid, market.yes_token.clone(), safe_yes_bid)
            };

        // ── Commit: lock this market against any further arb entry this session ──
        // Insert BEFORE returning the signal so the very next tick (and every tick
        // after a future orphan flatten clears the position map) is refused. Both
        // tokens are locked because either leg appearing on a re-entry attempt
        // must block it.
        if let Some(locks) = ctx.arb_market_lockouts.as_ref() {
            let mut locked = locks.lock().await;
            locked.insert(market.yes_token.clone());
            locked.insert(market.no_token.clone());
        }

        // Viper Backtrace: persist the gate/decision state for this entry (keyed by
        // Leg A, the primary leg that record_entry_signal records).
        crate::helpers::metrics::stash_entry_signals_json(leg_a_token.as_str(), serde_json::json!({
            "viper": "Arbitrage",
            "leg_a_price": leg_a_price.to_string(),
            "leg_b_price": leg_b_price.to_string(),
            "pair_sum": (safe_yes_bid + safe_no_bid).to_string(),
            "pair_shares": pair_shares.to_string(),
            "trade_size": trade_size.to_string(),
        }));

        return Ok(StrategySignal::Entry {
            params: OrderParams {
                token_id: leg_a_token,
                price: leg_a_price,                    // expensive leg — capped one tick below ask
                shares: pair_shares,                   // balanced — same count as cheap leg
                fee_bps: 0,                            // maker = 0 fees
                is_neg_risk: market.is_neg_risk,
                market_name: market.market_name.clone(),
                condition_id: market.condition_id.clone(),
                order_type: TimeInForce::Gtc,            // rest on the book as maker
                post_only: true,                       // reject if it would cross (no accidental taker)
                ghost_mode: dc.ghost_mode,
            },
            pair_params: Some(OrderParams {
                token_id: leg_b_token,
                price: leg_b_price,                    // cheap leg — capped one tick below ask
                shares: pair_shares,                   // same count as expensive leg
                fee_bps: 0,
                is_neg_risk: market.is_neg_risk,
                market_name: market.market_name.clone(),
                condition_id: market.condition_id.clone(),
                order_type: TimeInForce::Gtc,
                post_only: true,
                ghost_mode: dc.ghost_mode,
            }),
        });
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        let market   = ctx.maker_market.as_ref().unwrap_or(&ctx.market);
        let snapshot = ctx.maker_snapshot.as_ref().unwrap_or(&ctx.snapshot);
        let pos_map = ctx.positions.lock().await;

        let yes_key = (STRATEGY_NAME.to_string(), market.yes_token.clone());
        let no_key  = (STRATEGY_NAME.to_string(), market.no_token.clone());

        if let (Some(yp), Some(_np)) = (pos_map.get(&yes_key), pos_map.get(&no_key)) {
            let yes_bid = snapshot.yes_bid;
            let no_bid  = snapshot.no_bid;

            // ── Fee-adjusted early-exit gate ──────────────────────────────────
            // Exiting via FAK (taker) incurs a fee on each leg.  The combined bid
            // must exceed $1.00 PLUS the total taker-fee cost for the FAK exit to
            // actually improve on holding to settlement ($1.00, 0% fee).
            //
            //   fee_yes = yes_fee_bps / 10_000 (e.g. 1000 bps → 0.10 / share)
            //   fee_no  = no_fee_bps  / 10_000
            //   threshold = 1.00 + fee_yes + fee_no
            //
            // At 1000 bps per side the threshold is 1.20 — structurally unreachable
            // on a binary market — so positions correctly settle at $1.00 (0% fee).
            // If Polymarket ever lowers taker fees, this will start firing again.
            let yes_fee_rate = Decimal::from(market.yes_fee_bps) / dec!(10000);
            let no_fee_rate  = Decimal::from(market.no_fee_bps)  / dec!(10000);
            let early_exit_threshold = dec!(1.0) + yes_fee_rate + no_fee_rate;

            // Exit early when combined bid has converged enough to cover fees
            if yes_bid + no_bid >= early_exit_threshold {
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: market.yes_token.clone(),
                        price: yes_bid,
                        shares: yp.shares,
                        fee_bps: market.yes_fee_bps as u16, // taker exit — fee applies
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                        order_type: TimeInForce::Fak,   // guaranteed exit before close
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: "Arbitrage convergence".to_string(),
                    exit_pair: true,
                });
            }
        }

        // ── Naked single-leg exit (restart-orphan guard, 2026-07-15) ─────────
        // The ARB ARBITER that rescues/flattens one-sided fills is an in-process
        // task: a redeploy kills it, and ChainReconcile re-adopts the naked leg
        // as a position that the pair-only exit above can never touch. On 07-12
        // two such legs rode to $0 settlement (−$7.70 combined). If we hold
        // exactly ONE leg of the pair — confirmed filled and older than the
        // arbiter's full rescue window (so we never fight a live arbiter) —
        // flatten it at bid via FAK. A naked arb leg is an unintended
        // directional bet; realizing the small flatten cost strictly dominates
        // settlement roulette.
        let single_leg = match (pos_map.get(&yes_key), pos_map.get(&no_key)) {
            (Some(p), None) => Some((p, market.yes_token.clone(), snapshot.yes_bid,
                                     market.yes_fee_bps as u16, "YES")),
            (None, Some(p)) => Some((p, market.no_token.clone(), snapshot.no_bid,
                                     market.no_fee_bps as u16, "NO")),
            _ => None,
        };
        if let Some((pos, token, bid, fee_bps, leg)) = single_leg {
            let age_secs = (chrono::Utc::now() - pos.opened_at).num_seconds();
            let arbiter_window =
                crate::helpers::balance::MAX_WAIT_SECS_WINDOW + 60; // grace margin
            if pos.fill_confirmed_at.is_some() && age_secs > arbiter_window && bid > Decimal::ZERO {
                info!(
                    " Arb naked-leg flatten — holding only {} leg ({} shares, age {}s) \
                     with no live arbiter; exiting at bid {:.3}",
                    leg, pos.shares, age_secs, bid
                );
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token,
                        price: bid,
                        shares: pos.shares,
                        fee_bps,
                        is_neg_risk: market.is_neg_risk,
                        market_name: market.market_name.clone(),
                        condition_id: market.condition_id.clone(),
                        order_type: TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: dc.ghost_mode,
                    },
                    reason: format!("Arb naked {} leg flatten (orphaned single-leg)", leg),
                    exit_pair: false,
                });
            }
        }
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { "ArbitrageStrategy".to_string() }
    fn venue(&self) -> &'static str { "Window/Daily" }
    fn max_exposure(&self) -> rust_decimal::Decimal { crate::config::ARBITRAGE_MAX_EXPOSURE_USDC }
    fn risk_model(&self) -> &'static str { "Gross hedged (per leg)" }
}

/// Returns true when posting GTC maker bids on both legs is profitable.
///
/// Maker fills incur 0% fee on Polymarket.  Combined cost = YES_bid + NO_bid.
/// Settlement always pays $1.00 per pair.
/// Profit = 1.00 − YES_bid − NO_bid ≥ profit_threshold.
fn is_maker_arb_profitable(yes_bid: Decimal, no_bid: Decimal, profit_threshold: Decimal) -> bool {
    (dec!(1.0) - yes_bid - no_bid) >= profit_threshold
}
