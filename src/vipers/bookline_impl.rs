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
//! protect it. The viper never pays the fee and never pays the spread; if it ever
//! does either, the edge is gone and the trade was not worth taking.
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
use crate::state::{OrderParams, PositionKey, StrategySignal, StrategyStatus};
use crate::venues::core::MarketId;
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::{Mutex as StdMutex, OnceLock};
use tracing::info;

pub const STRATEGY_NAME: &str = "BooklineStrategy";
/// How often a refusal repeats in the log.
const GATE_LOG_INTERVAL_SECS: u64 = 300;

/// The consensus each resting bid is being judged against, by token.
///
/// `adverse_drift` is measured from where the line was when we committed, not
/// from the previous poll: a bid placed at 0.62 and still resting while the
/// consensus has walked to 0.58 is in danger whether it walked in one step or
/// four. FairValue keeps an entry-fair registry for the same reason.
///
/// Written on the first tick the quote is OBSERVED resting, not on the tick it is
/// emitted. The orchestrator runs `evaluate_entry` and `evaluate_exit`
/// concurrently against one context, and the patrol only registers the quote in
/// its consumer AFTER both have run — so a baseline written by entry was wiped by
/// exit on the very same tick, when exit saw neither a position nor a resting
/// quote and cleaned up what it took for a stale entry. The drift pull was
/// therefore dead: `adverse` read zero for the life of every quote and only the
/// line-refusal pulls ever fired.
///
/// Recording a tick later costs at most one board poll of drift and is
/// restart-correct for free: this registry is in-memory and the startup sweep
/// drops every simulated quote and position, so an empty registry is exactly the
/// right state after a restart.
fn placed_at() -> &'static StdMutex<HashMap<String, Decimal>> {
    static M: OnceLock<StdMutex<HashMap<String, Decimal>>> = OnceLock::new();
    M.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn lock<T>(m: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() { Ok(g) => g, Err(p) => p.into_inner() }
}

fn record_placed(token: &str, consensus: Decimal) {
    lock(placed_at()).insert(token.to_string(), consensus);
}
fn clear_placed(token: &str) {
    lock(placed_at()).remove(token);
}
fn consensus_at_placement(token: &str) -> Option<Decimal> {
    lock(placed_at()).get(token).copied()
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

/// This market's own taker coefficient, for the one leg an exit might cross on.
fn market_fee_rate(ctx: &StrategyContext, side: usize) -> Decimal {
    let bps = if side == 0 { ctx.market.yes_fee_bps } else { ctx.market.no_fee_bps };
    crate::venues::fee_rate_from_ceiling_bps(bps)
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
        // Ghost-only, and enforced here rather than trusted to a knob.
        //
        // Phase 2 of the spike's sequencing — real money — is gated on the
        // simulated record, and the simulated record is gated in turn on the line
        // cadence being in minutes rather than the ~45 it currently is. Until
        // both are settled this viper must be incapable of spending money, not
        // merely configured not to. An operator who turns `bookline_enabled` on
        // in a live session gets a simulated lane and a log line saying so.
        if !(crate::config::GHOST_MODE || dc.ghost_mode) {
            idle("Bookline is simulated-only in this build — turn on Simulation Mode to run it");
            return Ok(StrategySignal::NoSignal);
        }
        if ctx.market_class.as_deref() != Some("sports") {
            idle("Bookline trades sports moneylines only");
            return Ok(StrategySignal::NoSignal);
        }

        let Some(board) = ctx.sports.as_ref() else {
            idle(LineRefusal::NoLine.label());
            return Ok(StrategySignal::NoSignal);
        };
        let now = Utc::now();

        // One side, one market. The position key is per token, so nothing in the
        // keying stops a YES bid and a NO bid coexisting — and two resting bids on
        // opposite sides of the same game is the two-sided market making this
        // viper exists NOT to do. Any position or resting quote on either leg ends
        // the tick.
        for (_, token, _, _) in legs(ctx) {
            let key = PositionKey::new(&ctx.squadron_id, STRATEGY_NAME, token.clone());
            let held = ctx.positions.lock().await.contains_key(&key);
            if held || crate::helpers::ghost_quotes::is_resting(&key) {
                idle("already committed on this market");
                return Ok(StrategySignal::NoSignal);
            }
        }

        // Room, counted in MARKETS as well as notional.
        let size = dc.bookline_trade_size_usdc;
        // Positions AND resting quotes. A resting bid is a commitment: it is not a
        // position until it fills, so counting only the map would let Bookline rest
        // a bid on every deployed sports market at once and discover its own
        // ceiling only after the first one crossed.
        let (open_markets, exposure) = {
            let map = ctx.positions.lock().await;
            let mine: Vec<_> = map.iter()
                .filter(|(k, _)| k.strategy == STRATEGY_NAME)
                .collect();
            let mut markets: std::collections::HashSet<String> =
                mine.iter().map(|(_, p)| p.market_name.clone()).collect();
            let mut notional: Decimal = mine.iter().map(|(_, p)| p.shares * p.avg_entry).sum();
            let (quoted_markets, quoted_notional) =
                crate::helpers::ghost_quotes::resting_commitment(STRATEGY_NAME);
            markets.extend(quoted_markets);
            notional += quoted_notional;
            (markets.len(), notional)
        };
        if let Err(why) = has_room(open_markets, exposure, size,
                                   dc.bookline_max_open_markets, dc.bookline_max_exposure_usdc) {
            idle(why);
            return Ok(StrategySignal::NoSignal);
        }

        // Score both sides and take the better one. Only one can pass the favorite
        // floor on a two-outcome game, so this is not really a choice in practice —
        // but a draw market has three outcomes and the floor does not guarantee it.
        let mut best: Option<(usize, MarketId, Decimal, Decimal, Decimal)> = None;
        let mut last_refusal = String::new();
        for (side, token, bid, ask) in legs(ctx) {
            let Some(line) = board.side(side) else {
                last_refusal = LineRefusal::NoLine.label().to_string();
                continue;
            };
            let consensus = Decimal::try_from(line.consensus).unwrap_or(Decimal::ZERO);
            let mid = (bid > Decimal::ZERO && ask > Decimal::ZERO)
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
            match quote_price(consensus, bid, edge, crate::config::BOOKLINE_TICK_SIZE) {
                Ok(px) => {
                    let room = consensus - px;
                    if best.as_ref().is_none_or(|b| room > b.4) {
                        best = Some((side, token.clone(), px, consensus, room));
                    }
                }
                Err(why) => last_refusal = why.to_string(),
            }
        }

        let Some((side, token, price, consensus, room)) = best else {
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

        let params = OrderParams {
            token_id: token,
            price,
            shares,
            fee_bps: if side == 0 { ctx.market.yes_fee_bps as u16 } else { ctx.market.no_fee_bps as u16 },
            is_neg_risk: ctx.market.is_neg_risk,
            market_name: ctx.market.market_name.clone(),
            condition_id: ctx.market.condition_id.clone(),
            order_type: crate::venues::core::TimeInForce::Gtc,
            post_only: true,
            ghost_mode: true,
        };
        Ok(if side == 0 {
            StrategySignal::MakerQuote { yes: Some(params), no: None }
        } else {
            StrategySignal::MakerQuote { yes: None, no: Some(params) }
        })
    }

    async fn evaluate_exit(&self, ctx: &StrategyContext) -> Result<StrategySignal> {
        let dc = &ctx.dynamic_config;
        // Deliberately NOT gated on `bookline_enabled`. Turning the knob off while
        // a bid rests must PULL it, not abandon it: the fill simulation keeps
        // crossing a registered quote whether the viper is enabled or not, so
        // gating here would strand quotes that then fill against a stale line with
        // nothing left to manage them. Maker and FairValue leave their exits
        // ungated for the same reason. Disabled means "stop quoting", never "stop
        // looking after what is already out there".
        if ctx.market_class.as_deref() != Some("sports") {
            return Ok(StrategySignal::NoSignal);
        }
        let now = Utc::now();
        let board = ctx.sports.as_ref();

        for (side, token, bid, _ask) in legs(ctx) {
            let key = PositionKey::new(&ctx.squadron_id, STRATEGY_NAME, token.clone());
            let position = ctx.positions.lock().await.get(&key).cloned();
            // A simulated resting quote is NOT in the position map, so without this
            // the pull rules could never fire in ghost — and the pull rules are the
            // only thing protecting a resting bid from adverse selection. The Maker
            // reads the same registry for the same reason.
            let ghost_resting = position.is_none()
                && crate::helpers::ghost_quotes::is_resting(&key);

            if position.is_none() && !ghost_resting {
                clear_placed(token.as_str());
                continue;
            }

            // The line as it stands now, and whether it is still fit to hold against.
            let line_state = match board.and_then(|b| b.side(side)) {
                Some(line) => {
                    let consensus = Decimal::try_from(line.consensus).unwrap_or(Decimal::ZERO);
                    let dispersion = line.dispersion.and_then(|d| Decimal::try_from(d).ok());
                    let verdict = line_usable(
                        consensus, None, line.num_books, dispersion, line.age_secs(now),
                        line.secs_to_start(now), dc.bookline_min_consensus, dc.bookline_min_books,
                        dc.bookline_max_dispersion, dc.bookline_max_feed_age_secs,
                        dc.bookline_pull_before_start_secs,
                    );
                    Some((consensus, verdict))
                }
                None => None,
            };

            let unfilled = ghost_resting
                || position.as_ref().is_some_and(|p| p.fill_effective_at(dc.ghost_mode).is_none());

            if unfilled {
                // An unfilled pull is free, so the rules are deliberately eager.
                let verdict = line_state.map(|(_, v)| v).unwrap_or(Err(LineRefusal::NoLine));
                // First tick this quote is seen resting: this is where the drift
                // baseline is set. See `placed_at` for why it cannot be set at
                // quote time.
                if let Some((now_c, _)) = line_state {
                    if consensus_at_placement(token.as_str()).is_none() {
                        record_placed(token.as_str(), now_c);
                    }
                }
                let adverse = match (line_state, consensus_at_placement(token.as_str())) {
                    (Some((now_c, _)), Some(then_c)) => then_c - now_c,
                    _ => Decimal::ZERO,
                };
                // Disabled is itself a pull reason. Stopping quoting while leaving
                // a bid on the book is the one state that has all of this design's
                // risk and none of its upside.
                let why = if !dc.bookline_enabled {
                    Some("Bookline disabled — pulling the resting bid")
                } else {
                    pull_reason(verdict, adverse, dc.bookline_pull_on_adverse_drift)
                };
                if let Some(why) = why {
                    info!("📖 Bookline pull [{}] {}: {}", ctx.market.market_name,
                          if side == 0 { "YES" } else { "NO" }, why);
                    clear_placed(token.as_str());
                    return Ok(StrategySignal::MakerCancel { tokens: vec![token] });
                }
                continue;
            }

            // Filled. The plan is settlement, which is fee-free, so there is no
            // stop: a binary that resolves in hours does not behave like a
            // position that can be stopped out, and a 20-point adverse move on a
            // contract that still settles at a dollar is noise. The only reason to
            // sell is that the market is offering more than the model thinks it is
            // worth, net of the fee it would cost to take it.
            //
            // When the line is gone — the board drops a game six hours after
            // kick-off — that is hold-to-settlement, never "nothing to manage".
            let Some(position) = position else { continue };
            let Some((consensus, _)) = line_state else { continue };
            let fee = crate::venues::taker_leg_fee_pct_at(bid, market_fee_rate(ctx, side));

            if settle_snipe_sell(bid, consensus, fee) {
                info!(
                    "📖 Bookline settle-snipe [{}] {}: bid ${:.3} net of fee beats consensus {:.3}",
                    ctx.market.market_name, if side == 0 { "YES" } else { "NO" }, bid, consensus,
                );
                return Ok(StrategySignal::Exit {
                    params: OrderParams {
                        token_id: token,
                        price: bid,
                        shares: position.shares,
                        fee_bps: if side == 0 { ctx.market.yes_fee_bps as u16 } else { ctx.market.no_fee_bps as u16 },
                        is_neg_risk: ctx.market.is_neg_risk,
                        market_name: ctx.market.market_name.clone(),
                        condition_id: ctx.market.condition_id.clone(),
                        order_type: crate::venues::core::TimeInForce::Fak,
                        post_only: false,
                        ghost_mode: true,
                    },
                    reason: format!("BooklineSettleSnipe: bid=${bid:.3} beats consensus {consensus:.3} net of fee"),
                    exit_pair: false,
                });
            }

            // The bonus path: let the market lift us at consensus plus an edge.
            if let Some(px) = resting_tp_price(consensus, dc.bookline_resting_tp_edge,
                                               position.avg_entry, bid, crate::config::BOOKLINE_TICK_SIZE) {
                return Ok(StrategySignal::MakerRestingExit {
                    params: OrderParams {
                        token_id: token,
                        price: px,
                        shares: position.shares,
                        fee_bps: if side == 0 { ctx.market.yes_fee_bps as u16 } else { ctx.market.no_fee_bps as u16 },
                        is_neg_risk: ctx.market.is_neg_risk,
                        market_name: ctx.market.market_name.clone(),
                        condition_id: ctx.market.condition_id.clone(),
                        order_type: crate::venues::core::TimeInForce::Gtc,
                        post_only: true,
                        ghost_mode: true,
                    },
                    reason: format!("BooklineRestingTP: ask=${px:.3} consensus={consensus:.3}"),
                });
            }
        }
        Ok(StrategySignal::NoSignal)
    }

    fn status(&self) -> StrategyStatus { StrategyStatus::Active }
    fn name(&self) -> String { STRATEGY_NAME.to_string() }
    fn venue(&self) -> &'static str { "Sports" }
    fn max_exposure(&self) -> Decimal { crate::config::BOOKLINE_MAX_EXPOSURE_USDC }
}
