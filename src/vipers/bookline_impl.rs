// DRADIS — Bookline Viper (sports moneylines, maker-first)
//
// This file is part of DRADIS and is licensed under the terms in LICENSE.

//! # Bookline — a maker-first sports viper
//!
//! ## Why this exists rather than pointing FairValue at sports
//!
//! The 2026-09-09 sports spike (`docs/spikes/sports-viper-spike.md`) measured the
//! thing that decides the design. Polymarket's pre-game moneylines track the
//! de-vigged multi-book consensus to within the spread: across ten MLB games
//! priced by nine US books the mean absolute gap was 0.6 points and the maximum
//! 1.5, inside a 1c spread. There is no static mispricing to take.
//!
//! That kills a taker. On Polymarket International a taker at mid 0.50 needs the
//! true probability to beat mid by **+1.75 points** just to cover the fee, and
//! the gap is never that wide pre-game. It is why sports FairValue, running live
//! on demo for twenty hours across 44 matched games, reached its edge test 2,298
//! times and entered zero times. That was the correct outcome, not a bug.
//!
//! A maker changes the sign of the fee term. Resting at the bid and holding to
//! fee-free settlement, the same position needs the true probability to beat mid
//! by **−0.69 points**: it can be slightly wrong and still break even, because it
//! collects the half-spread and the venue's 15% maker rebate. That 2.4-point
//! swing is the entire thesis of this viper, and every rule below exists to
//! protect it. The viper never pays the TAKER fee and never pays the spread; if it
//! ever does either, the edge is gone and the trade was not worth taking.
//!
//! The resting leg is not free everywhere, though, and the difference is large
//! enough to change where this viper is worth running. Polymarket International
//! charges makers nothing and pays into a rebate pool; Polymarket US pays the maker
//! a rebate at trade, which is better still; Kalshi charges makers on every game
//! series it ships (`quadratic_with_maker_fees`, MLB at half rate), taking roughly
//! half the half-spread. Break-even at mid runs about -0.81 points on Polymarket
//! US, -0.69 on Polymarket International, and only -0.56 on a 2c Kalshi book —
//! close to a coin flip on a 1c one. `venues::sports_maker_fee_rate` carries this,
//! and the simulated return is signed by it, because a record that assumed a free
//! maker leg would be inflated on Kalshi exactly where the margin is thinnest.
//!
//! ## What this viper is NOT allowed to become
//!
//! Not a two-sided market maker. That is the Maker viper's job, and on a 1c
//! sports book it is fee-gated for good reasons even after the 2026-09-26 floor
//! correction. Bookline takes a view — the book's view — on ONE side of ONE
//! market at a time, and its profit path is settlement rather than the spread.
//!
//! ## The honest state of the evidence
//!
//! The spike gated this viper on two statistics from the line ledger. S1
//! (lead/lag) asks whether the books move before Polymarket does: target 60%
//! book-led, and below 40% "kills Bookline on the free feed". Computed on
//! 2026-09-26 over 1,901 consensus moves of at least a point, S1 read **6%
//! book-led**, with Polymarket already holding a median 101.5% of the move at the
//! same poll.
//!
//! That number does not settle the question, because the ledger polls a median of
//! 45 minutes apart while the odds feed itself is 0–2 seconds fresh. A lead that
//! resolves inside 45 minutes is invisible at that cadence, and both moves land
//! in the same bucket — which is exactly what a median of 1.015 looks like. So S1
//! is currently unmeasurable rather than failed.
//!
//! This viper therefore ships **ghost-only and disabled by default**. It exists to
//! accumulate simulated fills while the cadence question is settled, and its
//! ghost P&L is the Phase 1 statistic. DRADIS's ghost fill model is deliberately
//! pessimistic for a maker: a resting bid is only filled when the ask trades down
//! through it, so a real queue position would fill more often than this lane
//! shows. Read its record as a floor, not an estimate.
//!
//! Two further limits on what the simulated record can say, both worth knowing
//! before it is read as evidence:
//!
//! * **The resting take-profit never fills in simulation.** The patrol registers a
//!   ghost resting exit and stops there; both lift sweeps are live-only. So the
//!   record measures settlement and the settle-snipe, and the take-profit
//!   contributes nothing to it. It is not dead code — it is the live path — but no
//!   simulated P&L will ever be attributed to it.
//! * **At the current line cadence the pull rules cannot protect anything.** The
//!   ledger polls a median of 45 minutes apart, and a bid is filled or not long
//!   before the next reading. Every simulated fill is therefore an UNPROTECTED
//!   one, which makes the record a floor of a floor: the pull rules are the
//!   mechanism this design relies on, and they are effectively absent until the
//!   near-kick-off cadence is in minutes.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

/// The largest `|consensus - mid|` that can be an edge rather than a mistake.
///
/// Pre-game the two agree to within 1.5 points; a ten-point disagreement is not a
/// ten-point edge, it is the matching layer having paired this token with the
/// wrong outcome. The spike's own first probe did exactly that and produced two
/// six-point "gaps" that vanished when the date match was fixed.
const MAX_PLAUSIBLE_GAP: Decimal = rust_decimal_macros::dec!(0.10);

/// Why the line cannot price this market right now.
///
/// Every one of these is a refusal to quote, not a warning. A resting bid on a
/// market the model cannot currently price is a free option written to whoever
/// knows more than we do, which on a sports book is everyone with a faster feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineRefusal {
    /// The ledger has not matched this market to a game.
    NoLine,
    /// Fewer books than the floor. A one-to-three book "consensus" is one
    /// bookmaker's opinion; the spike found gaps of 1–3 points at low book counts
    /// that were a book-count artifact and not an edge.
    TooFewBooks,
    /// The books disagree by more than the ceiling. Wide dispersion is news in
    /// flight, and a resting bid is the wrong side of news.
    DispersionTooWide,
    /// The consensus is older than the ceiling.
    FeedStale,
    /// Too close to kick-off. The free feed goes stale the moment a game starts
    /// and in-play pricing is a different instrument entirely.
    TooCloseToStart,
    /// The consensus is too low to be trusted as a price.
    ///
    /// This is the refusal that decides whether the ghost record measures the
    /// maker thesis or an artifact of the de-vig. Proportional de-vigging
    /// systematically overstates longshots: on 1,432 pre-game rows with at least
    /// five books, mean `consensus - mid` ran +1.55 points under $0.10 and +1.05
    /// at $0.10-0.20, crossing to -1.10 at $0.70-0.80 and -1.69 above $0.90. A
    /// rule that buys wherever consensus exceeds the bid therefore fires almost
    /// only on longshots — 110 of 110 rows under $0.10 against 0 of 88 above
    /// $0.80 — and its P&L would be a measurement of the de-vig, not of the
    /// book's information. FairValue guards the same trap with
    /// `SPORTS_FAIRVALUE_MIN_CONSENSUS`.
    LongshotSide,
    /// The consensus and the market disagree by more than any real edge could
    /// explain, which in practice means the two sides were matched to each other
    /// wrongly. Cheap insurance against a side inversion in the matching layer.
    ImplausibleGap,
}

impl LineRefusal {
    pub fn label(self) -> &'static str {
        match self {
            LineRefusal::NoLine => "no bookmaker line for this market",
            LineRefusal::TooFewBooks => "too few books behind the consensus",
            LineRefusal::DispersionTooWide => "books disagree — news in flight",
            LineRefusal::FeedStale => "consensus feed is stale",
            LineRefusal::TooCloseToStart => "too close to kick-off for a pre-game line",
            LineRefusal::LongshotSide => "consensus below the favorite floor (de-vig overstates longshots)",
            LineRefusal::ImplausibleGap => "consensus and market disagree implausibly — check the side match",
        }
    }
}

/// Whether the line is fit to quote against.
///
/// `dispersion` and `max_book_age_secs` are `Option` on the raptor's line because
/// a single-book consensus has neither. A missing dispersion is treated as
/// unknown-and-therefore-refused rather than as zero: zero dispersion from one
/// book is not agreement.
#[allow(clippy::too_many_arguments)]
pub fn line_usable(
    consensus: Decimal,
    market_mid: Option<Decimal>,
    num_books: i64,
    dispersion: Option<Decimal>,
    feed_age_secs: i64,
    secs_to_start: i64,
    min_consensus: Decimal,
    min_books: i64,
    max_dispersion: Decimal,
    max_feed_age_secs: i64,
    pull_before_start_secs: i64,
) -> Result<(), LineRefusal> {
    if consensus < Decimal::ZERO || consensus > Decimal::ONE {
        return Err(LineRefusal::ImplausibleGap);
    }
    // Checked first: it is the cheapest and it is the one that decides what the
    // whole record means.
    if consensus < min_consensus {
        return Err(LineRefusal::LongshotSide);
    }
    if let Some(mid) = market_mid {
        if (consensus - mid).abs() > MAX_PLAUSIBLE_GAP {
            return Err(LineRefusal::ImplausibleGap);
        }
    }
    if num_books < min_books {
        return Err(LineRefusal::TooFewBooks);
    }
    match dispersion {
        Some(d) if d <= max_dispersion => {}
        Some(_) => return Err(LineRefusal::DispersionTooWide),
        None => return Err(LineRefusal::DispersionTooWide),
    }
    if feed_age_secs > max_feed_age_secs {
        return Err(LineRefusal::FeedStale);
    }
    if secs_to_start < pull_before_start_secs {
        return Err(LineRefusal::TooCloseToStart);
    }
    Ok(())
}

/// How far under the consensus a resting bid must sit before it is worth placing.
///
/// Two inputs, and both are about how far the line can travel against a bid
/// before it fills.
///
/// **Time to kick-off**, tapered as `base * sqrt(T / taper)` and floored at
/// `min_edge`. The further from the game, the more the line can move, so the more
/// edge is demanded; inside the taper the requirement relaxes toward the floor.
/// Capped at `base` so a game three days out is not asked for an absurd edge.
///
/// **Line velocity**, added as `drift.abs() * drift_mult`. This is the input
/// FairValue has no analogue for: the crypto model derives its uncertainty from a
/// realized-vol sampler, and here the equivalent is the raptor's own measurement
/// of how much this outcome's consensus just moved. A line that is travelling is
/// a line that will keep travelling, and a bid resting under a moving line is
/// picked off from the direction of travel.
pub fn required_edge(
    secs_to_start: i64,
    // Consensus change since the previous reading, with the seconds it spanned.
    // Both or neither: a change with no interval is not a velocity.
    drift: Option<(Decimal, i64)>,
    base: Decimal,
    min_edge: Decimal,
    taper_secs: i64,
    drift_mult: Decimal,
) -> Decimal {
    let t = secs_to_start.max(0);
    let taper = taper_secs.max(1);
    // sqrt(T / taper) in Decimal: the ratio is small and the sqrt is only a
    // taper shape, so the f64 hop is safe here in a way the tick grid was not.
    let ratio = (t as f64 / taper as f64).sqrt();
    let scaled = base * Decimal::try_from(ratio).unwrap_or(Decimal::ONE);
    let time_term = scaled.min(base).max(min_edge);
    // Per HOUR, not per poll. `SportsLine.drift` is a bare difference between two
    // board readings with no interval attached, so its meaning moves with the poll
    // cadence: at the shipped snapshot offsets it is a 110-minute change, and at
    // the production cadence a 45-to-60-minute one. Treating that as a velocity
    // would make `drift_mult` mean something different on every instance.
    let drift_term = match drift {
        Some((d, secs)) if secs > 0 => {
            let per_hour = d.abs() * Decimal::from(3600) / Decimal::from(secs);
            per_hour * drift_mult.max(Decimal::ZERO)
        }
        _ => Decimal::ZERO,
    };
    time_term + drift_term
}

/// Where a resting bid belongs, or why none does.
///
/// The rule from the spike: rest at `min(best_bid, consensus - required_edge)`.
/// In practice that resolves to two cases, and the interesting one is the refusal.
///
/// * `best_bid < target` — the queue is already below what we would pay, so join
///   it at the best bid and wait. We never improve the bid by a tick: improving
///   buys queue priority with edge, and edge is the only thing this design has.
///   The cost is a worse queue position and fewer fills, which is the trade the
///   spike chose deliberately.
/// * `best_bid >= target` — the market is already bidding at or above our limit.
///   Resting under the best bid would mean waiting for the market to come to us,
///   which only happens when the line moves against the position. That is not
///   patience, it is adverse selection with extra steps, so the viper idles.
///
/// Returns the price to rest at, in dollars, already on the tick grid.
pub fn quote_price(
    consensus: Decimal,
    best_bid: Decimal,
    required_edge: Decimal,
    tick: Decimal,
) -> Result<Decimal, &'static str> {
    if consensus < Decimal::ZERO || consensus > Decimal::ONE {
        return Err("consensus outside [0,1]");
    }
    let target = consensus - required_edge;
    if target <= Decimal::ZERO {
        return Err("no price under the consensus clears the required edge");
    }
    // A book with no bid at all is a book where nobody else wants this outcome.
    // Resting the only bid there is not making a market, it is naming a price to
    // a counterparty who has none — and the ghost fill model cannot simulate it
    // either, because there is no ask trading down through anything.
    if best_bid <= Decimal::ZERO {
        return Err("no bid on this leg to join");
    }
    if best_bid >= target {
        return Err("best bid is already at or above the consensus minus the required edge");
    }
    // Decimal, not f64. `(0.57f64 / 0.01).floor() * 0.01` is 0.56: the division
    // lands on 56.999... and the floor drops a whole tick, so the quote would sit
    // one tick UNDER the queue it meant to join. That happens on 0.29, 0.47, 0.57,
    // 0.58, 0.59 and 0.94 on the cent grid and on 126 of 999 prices on the
    // thousandth grid MLB moneylines use. `OrderParams.price` is a Decimal
    // anyway, so there was never a reason to leave the arithmetic in floats.
    let px = floor_to_tick(best_bid, tick);
    if px <= Decimal::ZERO {
        return Err("bid rounds to zero on the tick grid");
    }
    Ok(px)
}

/// Floor to the market's tick grid. Rounding DOWN is the safe direction for a
/// bid: rounding up could cross both the best bid and the target.
fn floor_to_tick(price: Decimal, tick: Decimal) -> Decimal {
    if tick <= Decimal::ZERO { return price; }
    (price / tick).floor() * tick
}

/// Ceil to the market's tick grid, the sell-side mirror: rounding UP keeps a
/// resting ask from landing below the intended target.
fn ceil_to_tick(price: Decimal, tick: Decimal) -> Decimal {
    if tick <= Decimal::ZERO { return price; }
    (price / tick).ceil() * tick
}

/// Why a bid that is already resting must come off the book.
///
/// The pull rules are the real work of this viper. A resting bid is a free option
/// written to the market, and the only thing that makes it a good trade is that
/// it is withdrawn the moment the reason for it stops being true. An unfilled
/// pull costs nothing — the venue charges no fee for a cancel — so the asymmetry
/// is enormous and the rules are deliberately trigger-happy.
///
/// `adverse_drift` is how far the consensus has moved AGAINST the resting bid
/// since it was placed, in probability points: positive means the line has fallen
/// away from us and the bid is now closer to fair than it was.
///
/// Venue status is deliberately NOT an input. `StrategyContext` carries no
/// venue-status field — the patrol holds it locally and cancels every order
/// itself when a market retires — so taking it as a parameter here would invent
/// a source the viper does not have.
pub fn pull_reason(
    line: Result<(), LineRefusal>,
    adverse_drift: Decimal,
    max_adverse_drift: Decimal,
) -> Option<&'static str> {
    if let Err(r) = line {
        return Some(r.label());
    }
    if adverse_drift > max_adverse_drift {
        return Some("consensus moved against the resting bid");
    }
    None
}

/// The take-profit ask for a filled Bookline position, in dollars.
///
/// Settlement is the plan; this is the bonus. A sports market resolves within
/// hours and settlement is fee-free, so there is no need to sell — but if the
/// market will pay us the consensus plus an edge before the game even starts,
/// that is the same money sooner and with the outcome risk removed. Returns
/// `None` when the target is not a real price, which is the common case on a
/// favorite already trading near a dollar.
pub fn resting_tp_price(
    consensus: Decimal,
    tp_edge: Decimal,
    entry: Decimal,
    bid: Decimal,
    tick: Decimal,
) -> Option<Decimal> {
    let px = ceil_to_tick(consensus + tp_edge, tick);
    // Above the bid as well as above the entry: a post-only ask at or under the
    // bid crosses the book, and the venue rejects it as a book race rather than
    // resting it. FairValue guards the same way.
    (px > entry && px > bid && px < Decimal::ONE).then_some(px)
}

/// Whether a filled position should be sold at the bid right now.
///
/// Deliberately an expected-value test and not a percentage stop. A binary that
/// resolves in three hours does not behave like a position that can be stopped
/// out: its price is a probability, and a 20% adverse move on a contract that
/// still settles at a dollar is not a loss, it is noise. FairValue's header makes
/// the same argument for the same reason.
///
/// So the only reason to sell is that the market is offering more, net of the
/// taker fee it would cost to take it, than the model now thinks the contract is
/// worth. `fee_pct` is a fraction of notional for one taker leg.
pub fn settle_snipe_sell(bid: Decimal, consensus: Decimal, fee_pct: Decimal) -> bool {
    if bid <= Decimal::ZERO || bid >= Decimal::ONE {
        return false;
    }
    let net = bid - (bid * fee_pct);
    net >= consensus
}

/// Whether room remains under the viper's own caps.
///
/// Sports positions are correlated only by sport and each resolves as a coin flip
/// at whatever probability was paid, so the cap that matters is the COUNT of open
/// markets rather than notional alone. A dozen $10 bets on a dozen games is a
/// different risk from one $120 bet, and the count cap is what keeps the first
/// from quietly becoming the second.
pub fn has_room(
    open_markets: usize,
    exposure: Decimal,
    size: Decimal,
    max_open_markets: usize,
    max_exposure: Decimal,
) -> Result<(), &'static str> {
    if open_markets >= max_open_markets {
        return Err("max open sports markets reached");
    }
    if exposure + size > max_exposure {
        return Err("max sports exposure reached");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn usable(consensus: Decimal, mid: Option<Decimal>, books: i64, disp: Option<Decimal>,
              age: i64, start: i64) -> Result<(), LineRefusal> {
        line_usable(consensus, mid, books, disp, age, start,
                    dec!(0.55), 3, dec!(0.06), 600, 900)
    }

    #[test]
    fn a_line_is_refused_for_each_reason_it_can_be_unfit() {
        assert_eq!(usable(dec!(0.65), Some(dec!(0.64)), 6, Some(dec!(0.02)), 60, 3600), Ok(()));

        assert_eq!(usable(dec!(0.65), None, 2, Some(dec!(0.02)), 60, 3600), Err(LineRefusal::TooFewBooks),
                   "a two-book consensus is one bookmaker's opinion");
        assert_eq!(usable(dec!(0.65), None, 6, Some(dec!(0.20)), 60, 3600), Err(LineRefusal::DispersionTooWide));
        // Unknown dispersion is refused, not treated as agreement: a single book
        // has no spread to measure and zero would be a lie.
        assert_eq!(usable(dec!(0.65), None, 6, None, 60, 3600), Err(LineRefusal::DispersionTooWide));
        assert_eq!(usable(dec!(0.65), None, 6, Some(dec!(0.02)), 900, 3600), Err(LineRefusal::FeedStale));
        assert_eq!(usable(dec!(0.65), None, 6, Some(dec!(0.02)), 60, 300), Err(LineRefusal::TooCloseToStart));
        // Already under way: the free feed is no longer pricing this game.
        assert_eq!(usable(dec!(0.65), None, 6, Some(dec!(0.02)), 60, -600), Err(LineRefusal::TooCloseToStart));
    }

    /// The refusal that decides what the ghost record actually measures.
    ///
    /// Proportional de-vig overstates longshots: on 1,432 pre-game rows with at
    /// least five books, mean `consensus - mid` was +1.55 points under $0.10 and
    /// -1.69 above $0.90. A rule that buys wherever consensus beats the bid fires
    /// on 110 of 110 rows under $0.10 and 0 of 88 above $0.80, so without this
    /// floor Bookline is a longshot buyer and its P&L measures the de-vig rather
    /// than the book's information.
    #[test]
    fn the_favorite_floor_keeps_the_de_vig_artifact_out_of_the_record() {
        assert_eq!(usable(dec!(0.05), None, 9, Some(dec!(0.01)), 60, 3600), Err(LineRefusal::LongshotSide));
        assert_eq!(usable(dec!(0.54), None, 9, Some(dec!(0.01)), 60, 3600), Err(LineRefusal::LongshotSide));
        assert_eq!(usable(dec!(0.55), None, 9, Some(dec!(0.01)), 60, 3600), Ok(()), "exactly at the floor passes");
        // It is checked before the cheaper faults, so the log names the real reason.
        assert_eq!(usable(dec!(0.05), None, 1, None, 9999, 10), Err(LineRefusal::LongshotSide));
    }

    /// A ten-point disagreement pre-game is a matching fault, not an edge. The
    /// spike's own first probe paired today's markets with tomorrow's events and
    /// produced two six-point "gaps" that vanished when the bug was fixed.
    #[test]
    fn an_implausible_gap_reads_as_a_bad_side_match() {
        assert_eq!(usable(dec!(0.90), Some(dec!(0.20)), 9, Some(dec!(0.01)), 60, 3600),
                   Err(LineRefusal::ImplausibleGap));
        assert_eq!(usable(dec!(0.70), Some(dec!(0.62)), 9, Some(dec!(0.01)), 60, 3600), Ok(()),
                   "eight points is a big edge but not an impossible one");
        // A consensus outside [0,1] is the same class of fault.
        assert_eq!(usable(dec!(1.5), None, 9, Some(dec!(0.01)), 60, 3600), Err(LineRefusal::ImplausibleGap));
        // With no market mid to compare against, the guard cannot fire.
        assert_eq!(usable(dec!(0.90), None, 9, Some(dec!(0.01)), 60, 3600), Ok(()));
    }

    #[test]
    fn the_required_edge_relaxes_toward_kick_off_and_widens_with_a_moving_line() {
        let e = |t, drift| required_edge(t, drift, dec!(0.02), dec!(0.005), 3600, dec!(0.5));

        assert_eq!(e(86_400, None), dec!(0.02), "capped at base a day out");
        assert_eq!(e(3600, None), dec!(0.02), "at the taper the full base is asked");
        assert!(e(900, None) < dec!(0.02) && e(900, None) > dec!(0.005));
        assert_eq!(e(0, None), dec!(0.005), "floored at min_edge at kick-off");
        assert_eq!(e(-600, None), dec!(0.005), "a negative clock is clamped, not negated");

        // Drift is per HOUR, so the same movement over a longer interval asks less.
        let fast = e(900, Some((dec!(0.04), 3600)));
        let slow = e(900, Some((dec!(0.04), 7200)));
        assert!(fast > slow, "the same move over twice the time is half the velocity");
        assert!(slow > e(900, None));
        // Direction of travel does not matter, only speed.
        assert_eq!(e(900, Some((dec!(0.04), 3600))), e(900, Some((dec!(-0.04), 3600))));
        // A change with no interval is not a velocity and contributes nothing.
        assert_eq!(e(900, Some((dec!(0.04), 0))), e(900, None));
    }

    #[test]
    fn a_bid_is_only_placed_where_it_keeps_the_whole_edge() {
        // The queue is below our limit: join it at the best bid.
        assert_eq!(quote_price(dec!(0.60), dec!(0.55), dec!(0.02), dec!(0.01)), Ok(dec!(0.55)));

        // The market is already bidding at or above the limit. Resting under it
        // would only fill when the line moves against us.
        assert_eq!(quote_price(dec!(0.60), dec!(0.58), dec!(0.02), dec!(0.01)),
                   Err("best bid is already at or above the consensus minus the required edge"));
        assert_eq!(quote_price(dec!(0.60), dec!(0.99), dec!(0.02), dec!(0.01)),
                   Err("best bid is already at or above the consensus minus the required edge"));

        // An empty bid side is not a market to join.
        assert_eq!(quote_price(dec!(0.60), dec!(0), dec!(0.02), dec!(0.01)), Err("no bid on this leg to join"));
        // A price where the edge eats the whole contract.
        assert_eq!(quote_price(dec!(0.015), dec!(0.01), dec!(0.02), dec!(0.01)),
                   Err("no price under the consensus clears the required edge"));
        assert!(quote_price(dec!(1.5), dec!(0.5), dec!(0.02), dec!(0.01)).is_err());
    }

    /// The float version of this dropped a whole tick on six prices of the cent
    /// grid, because `0.57f64 / 0.01` is 56.999... and the floor took 56. A quote
    /// one tick under the queue it meant to join is a quote that never fills.
    #[test]
    fn the_tick_grid_does_not_lose_a_tick_to_floating_point() {
        for bid in [dec!(0.29), dec!(0.47), dec!(0.57), dec!(0.58), dec!(0.59), dec!(0.94)] {
            // Consensus just far enough above the bid to clear the edge, and
            // still a real probability at $0.94.
            let got = quote_price(bid + dec!(0.03), bid, dec!(0.02), dec!(0.01)).unwrap();
            assert_eq!(got, bid, "a bid already on the grid must be joined exactly, not floored away");
        }
        // Off-grid bids floor DOWN, never up: rounding up could cross the queue.
        assert_eq!(quote_price(dec!(0.60), dec!(0.5555), dec!(0.02), dec!(0.01)), Ok(dec!(0.55)));
        // The thousandth grid MLB moneylines use.
        assert_eq!(quote_price(dec!(0.600), dec!(0.573), dec!(0.02), dec!(0.001)), Ok(dec!(0.573)));
    }

    #[test]
    fn a_resting_bid_comes_off_for_any_reason_at_all() {
        assert_eq!(pull_reason(Ok(()), dec!(0), dec!(0.01)), None);

        // Every line refusal is also a pull reason, reported as itself so the log
        // says which of them fired.
        for r in [LineRefusal::NoLine, LineRefusal::TooFewBooks, LineRefusal::DispersionTooWide,
                  LineRefusal::FeedStale, LineRefusal::TooCloseToStart, LineRefusal::LongshotSide,
                  LineRefusal::ImplausibleGap] {
            assert_eq!(pull_reason(Err(r), dec!(0), dec!(0.01)), Some(r.label()));
        }

        assert_eq!(pull_reason(Ok(()), dec!(0.02), dec!(0.01)), Some("consensus moved against the resting bid"));
        assert_eq!(pull_reason(Ok(()), dec!(0.01), dec!(0.01)), None, "at the threshold is not past it");
        // Drift in our FAVOR is not a reason to leave.
        assert_eq!(pull_reason(Ok(()), dec!(-0.05), dec!(0.01)), None);
    }

    #[test]
    fn the_take_profit_sits_above_the_entry_the_bid_and_below_a_dollar() {
        assert_eq!(resting_tp_price(dec!(0.60), dec!(0.03), dec!(0.55), dec!(0.56), dec!(0.01)), Some(dec!(0.63)));
        // Above the BID too: a post-only ask at or under the bid crosses and the
        // venue rejects it as a book race.
        assert_eq!(resting_tp_price(dec!(0.60), dec!(0.03), dec!(0.55), dec!(0.63), dec!(0.01)), None);
        assert_eq!(resting_tp_price(dec!(0.60), dec!(0.03), dec!(0.55), dec!(0.70), dec!(0.01)), None);
        // Nothing to place when the target is at or under what we paid.
        assert_eq!(resting_tp_price(dec!(0.50), dec!(0.01), dec!(0.55), dec!(0.40), dec!(0.01)), None);
        // Or when it is not a real price.
        assert_eq!(resting_tp_price(dec!(0.99), dec!(0.03), dec!(0.55), dec!(0.60), dec!(0.01)), None);
        // Ceil to the grid so the ask is never below the target.
        assert_eq!(resting_tp_price(dec!(0.601), dec!(0.03), dec!(0.55), dec!(0.56), dec!(0.01)), Some(dec!(0.64)));
    }

    #[test]
    fn the_exit_is_an_ev_test_and_not_a_percentage_stop() {
        assert!(settle_snipe_sell(dec!(0.80), dec!(0.70), dec!(0.02)));
        assert!(!settle_snipe_sell(dec!(0.40), dec!(0.70), dec!(0.02)),
                "a 30-point adverse move on a binary is noise, not a stop");
        assert!(settle_snipe_sell(dec!(0.70), dec!(0.70), dec!(0)));
        assert!(!settle_snipe_sell(dec!(0.70), dec!(0.70), dec!(0.05)), "the fee makes taking it a loss");
        assert!(!settle_snipe_sell(dec!(0), dec!(0.70), dec!(0.02)));
        assert!(!settle_snipe_sell(dec!(1), dec!(0.70), dec!(0.02)));
    }

    #[test]
    fn the_count_cap_binds_before_the_notional_one() {
        assert_eq!(has_room(0, dec!(0), dec!(10), 3, dec!(30)), Ok(()));
        assert_eq!(has_room(3, dec!(0), dec!(10), 3, dec!(30)), Err("max open sports markets reached"));
        assert_eq!(has_room(1, dec!(25), dec!(10), 3, dec!(30)), Err("max sports exposure reached"));
        assert_eq!(has_room(1, dec!(20), dec!(10), 3, dec!(30)), Ok(()));
    }
}

// ── The viper ────────────────────────────────────────────────────────────────

use crate::orchestrator::strategy::{Strategy, StrategyContext};
use crate::state::{StrategySignal, StrategyStatus};
use crate::venues::core::MarketId;
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use tracing::info;

/// What the RESTING leg cost, per share, at the price it rested at.
///
/// Zero on Polymarket International and negative on Polymarket US, where the maker
/// rebate is paid at trade — but positive on Kalshi, which charges makers on every
/// game series it ships. The quadratic is the same shape as the taker schedule.
fn maker_fee(price: Decimal) -> Decimal {
    if price <= Decimal::ZERO || price >= Decimal::ONE { return Decimal::ZERO; }
    // NOT `taker_leg_fee_pct_at`: that helper floors a non-positive rate at zero,
    // which is right for a taker (no venue pays one to cross) and wrong here —
    // Polymarket US pays the maker a rebate at trade, and flooring it would throw
    // away the very thing that makes that venue the friendliest for this design.
    crate::venues::sports_maker_fee_rate() * (Decimal::ONE - price) * price
}

pub const STRATEGY_NAME: &str = "BooklineStrategy";
/// How often a refusal repeats in the log.
const GATE_LOG_INTERVAL_SECS: u64 = 300;

/// Why there is no in-memory placement registry.
///
/// An earlier cut kept the consensus each bid was committed against in a static
/// map. Two things were wrong with that. The orchestrator runs entry and exit
/// concurrently against one context and the quote is only registered after both
/// have run, so a baseline written by entry was wiped by exit on the same tick and
/// the adverse-drift pull never fired at all. And a static map is empty after a
/// restart, which would silently disarm the same rule for the rest of a resting
/// quote's life.
///
/// The baseline now lives in the ledger row as `consensus_at_quote`, written once
/// when the quote is recorded. Durable, restart-correct, and there is no second
/// place for it to disagree with.

/// Book a closed simulated trade into the operator's trade list as a ghost row.
///
/// The lane's own ledger is what the Phase 1 statistics are computed from; this is
/// so the operator can SEE the trades. `ghost = 1` keeps them out of realized P&L
/// and out of the lifetime stat cards on a live instance, which is the whole reason
/// those were scoped by posture.
///
/// The fee column carries what the resting leg actually cost. That is zero on
/// Polymarket International, a credit on Polymarket US, and a real charge on
/// Kalshi — an earlier version recorded a flat zero and claimed "none were paid",
/// which was true only on the venue it was written against. Exit-side fees are
/// already netted into `ret` (a settle-snipe crosses and pays one; settlement and a
/// lifted ask do not), so the row's P&L agrees with the ledger either way.
async fn record_ghost_trade(
    pool: &sqlx::SqlitePool,
    ctx: &StrategyContext,
    row: &crate::helpers::db::BooklineShadow,
    exit_price: f64,
    reason: &str,
    ret: f64,
) {
    let entry = Decimal::try_from(row.quote_price).unwrap_or(Decimal::ZERO);
    let shares = Decimal::try_from(row.shares).unwrap_or(Decimal::ZERO);
    let pnl = Decimal::try_from(ret).unwrap_or(Decimal::ZERO) * entry * shares;
    let mut scope = crate::state::TradeScope::new(
        &ctx.crypto_filter, "", Some("sports".to_string()), None,
    );
    scope.ghost = true;
    crate::helpers::db::record_trade_db(
        pool, &scope, maker_fee(entry) * shares, STRATEGY_NAME, &row.market, &row.side,
        entry, Decimal::try_from(exit_price).unwrap_or(Decimal::ZERO), shares, pnl,
        &format!("Bookline {reason} — simulated"), None,
    ).await;
}

#[derive(Default)]
pub struct BooklineStrategy;

impl BooklineStrategy {
    pub fn new() -> Self { Self }
}

/// `(side index, token, bid, ask)` for both legs of the deployed market.
fn legs(ctx: &StrategyContext) -> [(usize, MarketId, Decimal, Decimal); 2] {
    [
        (0, ctx.market.yes_token.clone(), ctx.snapshot.yes_bid, ctx.snapshot.yes_ask),
        (1, ctx.market.no_token.clone(), ctx.snapshot.no_bid, ctx.snapshot.no_ask),
    ]
}


#[async_trait]
impl Strategy for BooklineStrategy {
    async fn evaluate_entry(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        let idle = |r: &str| {
            crate::helpers::viper_status::report_reason(&ctx.crypto_filter, STRATEGY_NAME, r);
            if crate::vipers::gate_log_permitted(STRATEGY_NAME, &ctx.crypto_filter, r, GATE_LOG_INTERVAL_SECS) {
                info!("🔒 Bookline gate: {}", r);
            }
        };

        if !dc.bookline_enabled {
            idle("disabled in config");
            return Ok(StrategySignal::NoSignal);
        }
        // No instance-wide simulation gate.
        //
        // This viper keeps its OWN books (`db::bookline_shadow_*`) and returns
        // `NoSignal` on every path, so it cannot reach a venue whatever mode the
        // instance is in. That is what lets it earn its record on the production
        // box, against the same books and the same latency the live vipers face —
        // which is the only place a record about these markets means anything.
        //
        // Gating on `ghost_mode` instead, as the first cut did, made the viper
        // permanently inert on the one instance whose evidence counts, and pushed
        // the experiment onto a quiet demo box where the result would not transfer.
        //
        // It must also not use the engine's ghost-quote registry: the patrol runs
        // the simulated fill sweep inside `if ghosting`, so on a live box a
        // registered quote would never cross, and the `MakerQuote` consumer reads
        // real-versus-simulated from the same tick-wide switch — an order flagged
        // simulated on a live instance becomes a real order.
        if ctx.market_class.as_deref() != Some("sports") {
            idle("Bookline trades sports moneylines only");
            return Ok(StrategySignal::NoSignal);
        }

        let Some(board) = ctx.sports.as_ref() else {
            idle(LineRefusal::NoLine.label());
            return Ok(StrategySignal::NoSignal);
        };
        let now = Utc::now();

        let Some(pool) = crate::helpers::db::pool_for(&ctx.crypto_filter) else {
            idle("no database for the simulated ledger");
            return Ok(StrategySignal::NoSignal);
        };

        // One side, one market, asked of the lane's own books.
        //
        // A resting bid on one outcome and another on the opposite outcome is
        // two-sided market making, which this viper exists not to do — and nothing
        // in the keying prevents it, because a key is per token.
        if crate::helpers::db::bookline_shadow_holds(&pool, &ctx.crypto_filter, &ctx.market.condition_id).await {
            idle("already committed on this market");
            return Ok(StrategySignal::NoSignal);
        }

        // Commitments, not fills. A resting bid is a commitment: counting only
        // filled rows would let Bookline rest a bid on every deployed sports market
        // at once and discover its own ceiling after the first one crossed.
        let size = dc.bookline_trade_size_usdc;
        let live = crate::helpers::db::bookline_shadow_open(&pool, &ctx.crypto_filter).await;
        let open_markets = live.iter().map(|r| r.condition_id.clone())
            .collect::<std::collections::HashSet<_>>().len();
        let exposure: Decimal = live.iter()
            .filter_map(|r| Decimal::try_from(r.quote_price * r.shares).ok())
            .sum();
        if let Err(why) = has_room(open_markets, exposure, size,
                                   dc.bookline_max_open_markets, dc.bookline_max_exposure_usdc) {
            idle(why);
            return Ok(StrategySignal::NoSignal);
        }

        // Score both sides and take the better one. Only one can pass the favorite
        // floor on a two-outcome game, so this is not really a choice in practice —
        // but a draw market has three outcomes and the floor does not guarantee it.
        let mut best: Option<(usize, MarketId, Decimal, Decimal, Decimal, Decimal)> = None;
        let mut last_refusal = String::new();
        for (side, token, bid, ask) in legs(ctx) {
            let Some(line) = board.side(side) else {
                last_refusal = LineRefusal::NoLine.label().to_string();
                continue;
            };
            let consensus = Decimal::try_from(line.consensus).unwrap_or(Decimal::ZERO);
            // An ABSENT ask is published as $1.00, not as zero — the feed fills a
            // missing level with the value least attractive to us. So `ask > 0` is
            // not a test for a seller, and a 0.60 bid with no ask yielded a mid of
            // 0.80: nonsense, and enough to trip the implausible-gap guard against a
            // perfectly good consensus. `has_ask` is the real test.
            let has_ask = if side == 0 { ctx.snapshot.yes_has_ask() } else { ctx.snapshot.no_has_ask() };
            let mid = (bid > Decimal::ZERO && has_ask)
                .then(|| (bid + ask) / Decimal::from(2));
            let dispersion = line.dispersion.and_then(|d| Decimal::try_from(d).ok());
            let secs_to_start = line.secs_to_start(now);
            if let Err(r) = line_usable(
                consensus, mid, line.num_books, dispersion, line.age_secs(now), secs_to_start,
                dc.bookline_min_consensus, dc.bookline_min_books, dc.bookline_max_dispersion,
                dc.bookline_max_feed_age_secs, dc.bookline_pull_before_start_secs,
            ) {
                last_refusal = r.label().to_string();
                continue;
            }
            let drift = match (line.drift, line.drift_secs) {
                (Some(d), Some(s)) => Decimal::try_from(d).ok().map(|d| (d, s)),
                _ => None,
            };
            let edge = required_edge(
                secs_to_start, drift, dc.bookline_base_edge, dc.bookline_min_edge,
                dc.bookline_edge_taper_secs, dc.bookline_drift_mult,
            );
            match quote_price(consensus, bid, edge, crate::venues::sports_tick_size()) {
                Ok(px) => {
                    let room = consensus - px;
                    if best.as_ref().is_none_or(|b| room > b.4) {
                        best = Some((side, token.clone(), px, consensus, room, edge));
                    }
                }
                Err(why) => last_refusal = why.to_string(),
            }
        }

        let Some((side, token, price, _consensus, room, edge)) = best else {
            idle(if last_refusal.is_empty() { "no side worth quoting" } else { &last_refusal });
            return Ok(StrategySignal::NoSignal);
        };

        let shares = size / price;
        if shares < crate::config::MIN_ORDER_SHARES {
            idle("trade size is below the venue's minimum at this price");
            return Ok(StrategySignal::NoSignal);
        }

        let line = board.side(side).expect("scored side has a line");
        info!(
            "📖 Bookline quote [{}] {} @ ${:.3} x {:.2} | consensus {:.3} ({} books, disp {:?}), \
             room {:.3}, {}m to kick-off — simulated",
            ctx.market.market_name,
            if side == 0 { "YES" } else { "NO" },
            price, shares, line.consensus, line.num_books, line.dispersion,
            room, line.secs_to_start(Utc::now()) / 60,
        );

        crate::helpers::db::bookline_shadow_quote(
            &pool, &ctx.crypto_filter, &ctx.market.condition_id, token.as_str(),
            &ctx.market.market_name, if side == 0 { "YES" } else { "NO" },
            Some(line.league.as_str()), Some(&line.commence.to_rfc3339()),
            price.to_f64().unwrap_or(0.0), shares.to_f64().unwrap_or(0.0),
            line.consensus, edge.to_f64().unwrap_or(0.0), line.num_books, line.dispersion,
        ).await;

        // No signal, ever. The lane's whole safety property is that nothing it
        // decides reaches a venue consumer, which is what lets it run on a live box.
        Ok(StrategySignal::NoSignal)
    }

    /// Sweep the lane's own books: fill resting quotes the book has crossed, pull
    /// the ones that should not be there, and close the filled ones.
    ///
    /// Emits nothing. Not gated on `bookline_enabled` either: turning the knob off
    /// must PULL what is resting, not abandon it, because this sweep is the only
    /// thing that will ever fill or close these rows. Disabled means "stop
    /// quoting", never "stop looking after what is already out there".
    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        if ctx.market_class.as_deref() != Some("sports") {
            return Ok(StrategySignal::NoSignal);
        }
        let Some(pool) = crate::helpers::db::pool_for(&ctx.crypto_filter) else {
            return Ok(StrategySignal::NoSignal);
        };
        let live = crate::helpers::db::bookline_shadow_open(&pool, &ctx.crypto_filter).await;
        if live.is_empty() {
            return Ok(StrategySignal::NoSignal);
        }
        let now = Utc::now();
        let board = crate::raptors::sports_ledger::board();

        for row in live {
            let token = MarketId::new(&row.token_id);
            // The book for this token, when this squadron still has one. A market
            // it has rotated away from has none, which is not an error: the row
            // then waits for the venue's resolution below.
            let book = crate::vipers::venue_for_token(ctx, &token).and_then(|(market, snap)| {
                let is_yes = token == market.yes_token;
                let (bid, ask) = if is_yes { (snap.yes_bid, snap.yes_ask) } else { (snap.no_bid, snap.no_ask) };
                let fee_bps = if is_yes { market.yes_fee_bps } else { market.no_fee_bps };
                // Absent levels are published as the values least attractive to
                // us: a missing bid at $0.00 and a missing ask at $1.00. Neither is
                // a price, so both have to be excluded — testing `ask > 0` would
                // accept a book with no seller at all.
                let has_ask = if is_yes { snap.yes_has_ask() } else { snap.no_has_ask() };
                has_ask.then_some((bid, ask, fee_bps))
            });

            // The line as it stands now, straight from the board rather than from
            // `ctx.sports`: this row may be on a market the squadron no longer holds.
            let line = board.get(&row.token_id).cloned();
            let quote_px = Decimal::try_from(row.quote_price).unwrap_or(Decimal::ZERO);
            let consensus_now = line.as_ref().and_then(|l| Decimal::try_from(l.consensus).ok());

            // ── Settlement: the plan for every position, and fee-free ──────────
            if row.filled_at.is_some() {
                if let Some(resolved) = crate::helpers::db::sports_resolved_price(&pool, &row.token_id).await {
                    let px = Decimal::try_from(resolved).unwrap_or(Decimal::ZERO);
                    // Settlement pays no exit fee: the contract redeems, nothing is
                    // sold. The ENTRY fee is not always zero, though — Kalshi
                    // charges makers on every game series it ships, so treating the
                    // resting leg as free would inflate a Kalshi record by roughly
                    // half its half-spread. `sports_maker_fee_rate` is negative on
                    // Polymarket US, where the rebate is paid at trade.
                    let ret = ((px - quote_px - maker_fee(quote_px)) / quote_px).to_f64().unwrap_or(0.0);
                    if crate::helpers::db::bookline_shadow_close(&pool, row.id, resolved, "settlement", ret).await {
                        record_ghost_trade(&pool, ctx, &row, resolved, "settlement", ret).await;
                        info!("📖 Bookline settled [{}] {} @ ${:.2} — {:+.2}% (simulated)",
                              row.market, row.side, resolved, ret * 100.0);
                    }
                    continue;
                }
            }

            let Some((bid, ask, fee_bps)) = book else { continue };

            if row.filled_at.is_none() {
                // ── Resting: pull, or let the book cross us ────────────────────
                let verdict = match line.as_ref() {
                    Some(l) => line_usable(
                        Decimal::try_from(l.consensus).unwrap_or(Decimal::ZERO), None, l.num_books,
                        l.dispersion.and_then(|d| Decimal::try_from(d).ok()),
                        l.age_secs(now), l.secs_to_start(now), dc.bookline_min_consensus,
                        dc.bookline_min_books, dc.bookline_max_dispersion,
                        dc.bookline_max_feed_age_secs, dc.bookline_pull_before_start_secs,
                    ),
                    None => Err(LineRefusal::NoLine),
                };
                // Measured from the consensus the bid was COMMITTED against, which
                // is durable in the row rather than in memory — so a restart cannot
                // silently disarm this rule for the rest of a quote's life.
                let adverse = match consensus_now {
                    Some(c) => Decimal::try_from(row.consensus_at_quote).unwrap_or(c) - c,
                    None => Decimal::ZERO,
                };
                let why = if !dc.bookline_enabled {
                    Some("Bookline disabled")
                } else {
                    pull_reason(verdict, adverse, dc.bookline_pull_on_adverse_drift)
                };
                if let Some(why) = why {
                    if crate::helpers::db::bookline_shadow_pull(&pool, row.id, why).await {
                        info!("📖 Bookline pull [{}] {}: {} (simulated)", row.market, row.side, why);
                    }
                    continue;
                }
                // The pessimistic fill rule, the same one the engine's own ghost
                // model uses: a resting BUY at Q is filled when somebody is willing
                // to sell at Q, i.e. when the best ASK falls to Q or below. Anything
                // sooner is the simulator handing itself a trade. A real queue
                // position would fill more often than this, so the record is a floor.
                if ask <= quote_px && crate::helpers::db::bookline_shadow_fill(&pool, row.id).await {
                    info!("📖 Bookline filled [{}] {} @ ${:.3} (ask ${:.3}) — simulated",
                          row.market, row.side, row.quote_price, ask);
                }
                continue;
            }

            // ── Filled and unresolved: the two early exits ─────────────────────
            let Some(consensus) = consensus_now else { continue };
            let fee = crate::venues::taker_leg_fee_pct_at(
                bid, crate::venues::fee_rate_from_ceiling_bps(fee_bps));

            if settle_snipe_sell(bid, consensus, fee) {
                let net = bid - bid * fee;
                let ret = ((net - quote_px - maker_fee(quote_px)) / quote_px).to_f64().unwrap_or(0.0);
                if crate::helpers::db::bookline_shadow_close(
                    &pool, row.id, bid.to_f64().unwrap_or(0.0), "settle-snipe", ret).await
                {
                    record_ghost_trade(&pool, ctx, &row, bid.to_f64().unwrap_or(0.0), "settle-snipe", ret).await;
                    info!("📖 Bookline settle-snipe [{}] {}: bid ${:.3} net beats consensus {:.3} — {:+.2}% (simulated)",
                          row.market, row.side, bid, consensus, ret * 100.0);
                }
                continue;
            }

            // The bonus path. A resting ask is lifted when the BID rises to it,
            // the mirror of the fill rule above, and pays no fee.
            if let Some(px) = resting_tp_price(consensus, dc.bookline_resting_tp_edge,
                                               quote_px, bid, crate::venues::sports_tick_size()) {
                if bid >= px {
                    // A lifted post-only ask pays no taker fee; the entry leg's
                    // maker fee still applies where the venue charges one.
                    let ret = ((px - quote_px - maker_fee(quote_px)) / quote_px).to_f64().unwrap_or(0.0);
                    if crate::helpers::db::bookline_shadow_close(
                        &pool, row.id, px.to_f64().unwrap_or(0.0), "take-profit", ret).await
                    {
                        record_ghost_trade(&pool, ctx, &row, px.to_f64().unwrap_or(0.0), "take-profit", ret).await;
                        info!("📖 Bookline take-profit [{}] {} @ ${:.3} — {:+.2}% (simulated)",
                              row.market, row.side, px, ret * 100.0);
                    }
                }
            }
        }
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { STRATEGY_NAME.to_string() }
    fn venue(&self) -> &'static str { "Sports" }
    fn risk_model(&self) -> &'static str {
        "Maker-first: resting bid under the bookmaker consensus, hold to fee-free settlement (simulated)"
    }
    fn max_exposure(&self) -> Decimal { crate::config::BOOKLINE_MAX_EXPOSURE_USDC }
}

#[cfg(test)]
mod venue_fee_tests {
    use super::maker_fee;
    use rust_decimal_macros::dec;
    use rust_decimal::Decimal;

    /// The resting leg is not free on every venue, and the sign matters.
    ///
    /// Bookline's thesis is a fee asymmetry, so the one number it must never get
    /// wrong is what the maker leg costs. Polymarket International charges makers
    /// nothing; Polymarket US pays a rebate AT TRADE, which an earlier version
    /// silently floored to zero by borrowing the taker helper; Kalshi charges
    /// makers on every game series it ships, which an earlier version claimed was
    /// free. All three are wrong in different directions and all three would bias
    /// the simulated record.
    #[test]
    fn the_maker_leg_is_priced_with_the_venues_own_sign() {
        let fee = maker_fee(dec!(0.60));
        let rate = crate::venues::sports_maker_fee_rate();

        // Whatever the venue, the quadratic shape holds and the extremes are free.
        assert_eq!(maker_fee(dec!(0)), Decimal::ZERO);
        assert_eq!(maker_fee(dec!(1)), Decimal::ZERO, "a resolved contract has no fee");

        if rate == Decimal::ZERO {
            assert_eq!(fee, Decimal::ZERO, "Polymarket International charges makers nothing");
        } else if rate < Decimal::ZERO {
            assert!(fee < Decimal::ZERO,
                    "a rebate must stay a CREDIT, not be floored to zero as a taker fee would be");
            assert_eq!(fee, rate * dec!(0.40) * dec!(0.60));
        } else {
            assert!(fee > Decimal::ZERO, "Kalshi charges makers on its game series");
            assert_eq!(fee, rate * dec!(0.40) * dec!(0.60));
        }
    }

    /// An absent ask is $1.00, so a mid computed without checking for a seller is
    /// not a mid.
    ///
    /// This shipped as `ask > 0`, which is never false: the feed fills a missing
    /// level with the value least attractive to us, so a leg with no seller reports
    /// an ask of exactly $1.00. A 0.60 bid with no ask therefore produced a "mid" of
    /// 0.80, and on production that was enough to trip the implausible-gap guard
    /// against a consensus that was fine — the guard fired, but for the wrong
    /// reason, which is the worst way for a guard to behave.
    #[test]
    fn a_book_with_no_seller_has_no_mid() {
        let mid = |bid: Decimal, ask: Decimal| {
            let has_ask = ask < Decimal::ONE;
            (bid > Decimal::ZERO && has_ask).then(|| (bid + ask) / Decimal::from(2))
        };
        assert_eq!(mid(dec!(0.60), dec!(0.62)), Some(dec!(0.61)), "a real two-sided book");
        assert_eq!(mid(dec!(0.60), dec!(1)), None, "no seller: $1.00 is the absence of an ask");
        assert_eq!(mid(dec!(0), dec!(0.62)), None, "no bidder: $0.00 is the absence of a bid");
        assert_eq!(mid(dec!(0), dec!(1)), None, "an empty book");
        // The old test would have accepted this and called it 0.80.
        let old_guard_would_accept = dec!(1) > Decimal::ZERO;
        assert!(old_guard_would_accept, "which is exactly why `ask > 0` was never a test");
    }

    /// The grid a post-only order must land on differs per venue: a thousandth on
    /// Polymarket International's sports books, a whole cent on Kalshi. An ask off
    /// the grid is rejected outright, so this cannot be a shared constant.
    #[test]
    fn the_sports_tick_is_a_venue_fact() {
        let tick = crate::venues::sports_tick_size();
        assert!(tick > Decimal::ZERO && tick <= dec!(0.01), "got {tick}");
    }
}
