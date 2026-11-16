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

//! [B114] Strike ladders: which rung is at the money.
//!
//! Venue-neutral. Kalshi lists a ladder of hourly and daily strikes per
//! underlying; Polymarket lists "above $X" ladders beside its Up/Down markets.
//! The picker here reads any of them: a strike, a tight YES mid when there is
//! one, and volume as the last resort.

use std::cmp::Ordering;

// ─── Strike ladders: the at-the-money rung [B114] ─────────────────────────────

/// Widest YES spread at which a rung's mid still says where the money is. A
/// resting 0.01/0.99 bracket has a mid of exactly 0.50 and says nothing; a
/// 0.40/0.60 book is a real two-sided view of the strike.
pub const ATM_MID_MAX_SPREAD: f64 = 0.20;

/// One rung of a strike ladder as the picker sees it: the strike, the quoted
/// YES mid when the book is two-sided and tighter than `ATM_MID_MAX_SPREAD`
/// (`ladder_yes_mid`), and cumulative traded volume.
#[derive(Clone, Debug, PartialEq)]
pub struct LadderRung {
    pub strike: f64,
    pub yes_mid: Option<f64>,
    pub volume: f64,
}

/// What `pick_at_the_money` decided on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AtmBasis {
    /// A live price for the underlying: the rungs bracketing it.
    Reference,
    /// No price; the rung whose tight-spread YES mid is nearest 0.50.
    Mid,
    /// Neither price nor quotes; the hottest rung by volume (the rule before
    /// B114). The caller logs this: it is the state that put eth-open on a
    /// strike $850 out of the money for nine hours on 2026-11-14.
    Volume,
}

/// The YES mid a ladder rung is judged by, or None when either side is blank
/// or the spread is wider than `ATM_MID_MAX_SPREAD`.
pub fn ladder_yes_mid(yes_bid: Option<f64>, yes_ask: Option<f64>) -> Option<f64> {
    let (b, a) = (yes_bid?, yes_ask?);
    if !(0.0..=1.0).contains(&b) || !(0.0..=1.0).contains(&a) || a < b || a - b > ATM_MID_MAX_SPREAD {
        return None;
    }
    Some((a + b) / 2.0)
}

/// Which rung of `ladder` is at the money, and on what basis?
///
/// 1. `reference` > 0: of the two rungs bracketing it (nearest at or below and
///    nearest above) the hotter by volume; tie, the nearer. A reference beyond
///    the ladder's ends takes the end rung. Hotter-of-the-bracket rather than
///    strictly nearest keeps BTC's pick exactly where the volume rule had it
///    (the hot BTC strike is always one of the bracket) and damps flips between
///    adjacent rungs as spot drifts over the midpoint.
/// 2. No reference: the rung whose quoted (tight-spread) YES mid is nearest
///    0.50; ties go to the hotter.
/// 3. Neither: the hottest rung by volume.
///
/// Why the money and not the volume: Kalshi lists 190-300 strikes per hour on
/// ETH and SOL and the cumulative-volume leader is routinely a far rung with a
/// 1c book (a resting maker lifted at 1c over and over). Every hour-shaped
/// viper on such a strike idles. The same applies to any venue that lists a
/// strike ladder; the helper is venue-neutral on purpose.
pub fn pick_at_the_money(ladder: &[LadderRung], reference: Option<f64>) -> Option<(usize, AtmBasis)> {
    if ladder.is_empty() {
        return None;
    }
    let hotter = |a: usize, b: usize| -> Ordering {
        ladder[a].volume.partial_cmp(&ladder[b].volume).unwrap_or(Ordering::Equal)
    };
    if let Some(r) = reference.filter(|r| r.is_finite() && *r > 0.0) {
        let by_strike = |a: usize, b: usize| ladder[a].strike.partial_cmp(&ladder[b].strike).unwrap_or(Ordering::Equal);
        // Two rungs at one strike (two expiries of a cadence on the board at
        // once): the hotter stands for the strike on either side.
        let below = (0..ladder.len()).filter(|&i| ladder[i].strike <= r)
            .max_by(|&a, &b| by_strike(a, b).then_with(|| hotter(a, b)));
        let above = (0..ladder.len()).filter(|&i| ladder[i].strike > r)
            .min_by(|&a, &b| by_strike(a, b).then_with(|| hotter(b, a)));
        let pick = match (below, above) {
            (Some(b), Some(a)) => match hotter(b, a) {
                Ordering::Greater => Some(b),
                Ordering::Less => Some(a),
                Ordering::Equal => Some(if (r - ladder[b].strike) <= (ladder[a].strike - r) { b } else { a }),
            },
            (Some(b), None) => Some(b),
            (None, Some(a)) => Some(a),
            // Every strike non-finite: nothing brackets anything; the quote
            // and volume rules below still apply.
            (None, None) => None,
        };
        if let Some(pick) = pick {
            return Some((pick, AtmBasis::Reference));
        }
    }
    let by_mid = (0..ladder.len()).filter(|&i| ladder[i].yes_mid.is_some())
        .min_by(|&a, &b| {
            let da = (ladder[a].yes_mid.unwrap_or(0.5) - 0.5).abs();
            let db = (ladder[b].yes_mid.unwrap_or(0.5) - 0.5).abs();
            da.partial_cmp(&db).unwrap_or(Ordering::Equal).then_with(|| hotter(b, a))
        });
    if let Some(i) = by_mid {
        return Some((i, AtmBasis::Mid));
    }
    // `rev` so equal volumes resolve to the FIRST listed rung, as the volume
    // rule before B114 did on a hottest-first board.
    (0..ladder.len()).rev().max_by(|&a, &b| hotter(a, b)).map(|i| (i, AtmBasis::Volume))
}

/// The ladder's strikes, sorted and deduplicated: the index a rung sits at in
/// this list is what "one rung" means to the operator reading the board.
fn sorted_strikes(ladder: &[LadderRung]) -> Vec<f64> {
    let mut s: Vec<f64> = ladder.iter().map(|r| r.strike).filter(|s| s.is_finite()).collect();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    s.dedup();
    s
}

/// Index in the sorted ladder of the rung nearest `x` (lower on a tie).
fn nearest_index(sorted: &[f64], x: f64) -> Option<usize> {
    if sorted.is_empty() {
        return None;
    }
    let hi = sorted.partition_point(|s| *s < x).min(sorted.len() - 1);
    let lo = hi.saturating_sub(1);
    Some(if (x - sorted[lo]).abs() <= (sorted[hi] - x).abs() { lo } else { hi })
}

/// The strike of the rung nearest `reference` on this ladder.
pub fn nearest_rung(ladder: &[LadderRung], reference: f64) -> Option<f64> {
    let sorted = sorted_strikes(ladder);
    nearest_index(&sorted, reference).map(|i| sorted[i])
}

/// Distance in rungs between two strikes of one ladder: sort the ladder's
/// strikes, take the index difference. Exact on uneven steps (SOL mixes $0.25
/// and $0.50, ETH $5 and $10): no step statistic, and the count matches the
/// listing the operator sees. A strike not on the ladder is placed at its
/// nearest rung.
pub fn rungs_apart(ladder: &[LadderRung], a: f64, b: f64) -> usize {
    let sorted = sorted_strikes(ladder);
    match (nearest_index(&sorted, a), nearest_index(&sorted, b)) {
        (Some(i), Some(j)) => i.abs_diff(j),
        _ => 0,
    }
}

#[cfg(test)]
mod at_the_money_tests {
    use super::*;

    fn rung(strike: f64, volume: f64) -> LadderRung { LadderRung { strike, yes_mid: None, volume } }
    fn quoted(strike: f64, bid: f64, ask: f64) -> LadderRung {
        LadderRung { strike, yes_mid: ladder_yes_mid(Some(bid), Some(ask)), volume: 0.0 }
    }

    /// The 2026-11-14 ETH board in miniature: the far strike carries the
    /// volume, spot sits between two warm rungs. With a reference the hotter
    /// of the bracket wins; the far rung loses however hot it is.
    #[test]
    fn a_reference_picks_the_hotter_of_the_bracketing_rungs() {
        let ladder = vec![rung(2645.0, 40_000.0), rung(1780.0, 120.0), rung(1785.0, 300.0), rung(1790.0, 90.0), rung(1795.0, 10.0)];
        assert_eq!(pick_at_the_money(&ladder, Some(1789.4)), Some((2, AtmBasis::Reference)), "1,785 brackets from below and is hotter than 1,790");
        assert_eq!(pick_at_the_money(&ladder, Some(1791.0)), Some((3, AtmBasis::Reference)), "1,790/1,795 bracket: 1,790 is hotter");
        let mut tie = ladder.clone();
        tie[2].volume = 90.0;
        assert_eq!(pick_at_the_money(&tie, Some(1789.4)), Some((3, AtmBasis::Reference)), "equal volume: the nearer rung");
        assert_eq!(pick_at_the_money(&tie, Some(1787.5)), Some((2, AtmBasis::Reference)), "exactly midway, equal volume: the lower rung");
        assert_eq!(pick_at_the_money(&ladder, Some(1785.0)), Some((2, AtmBasis::Reference)), "a reference on a rung brackets from that rung");
    }

    /// Spot beyond either end of the ladder takes the end rung, not nothing.
    #[test]
    fn a_reference_beyond_the_ladder_takes_the_end_rung() {
        let ladder = vec![rung(100.0, 5.0), rung(90.0, 50.0), rung(95.0, 1.0)];
        assert_eq!(pick_at_the_money(&ladder, Some(120.0)), Some((0, AtmBasis::Reference)));
        assert_eq!(pick_at_the_money(&ladder, Some(10.0)), Some((1, AtmBasis::Reference)));
    }

    /// No price: the tight two-sided book nearest 0.50 says where the money
    /// is; a 0.01/0.99 bracket (mid exactly 0.50) and blank books say nothing.
    #[test]
    fn without_a_reference_the_tight_mid_nearest_a_half_decides() {
        let ladder = vec![
            quoted(2645.0, 0.01, 0.99),
            quoted(1780.0, 0.80, 0.86),
            quoted(1785.0, 0.44, 0.52),
            quoted(1790.0, 0.20, 0.30),
            LadderRung { strike: 1795.0, yes_mid: ladder_yes_mid(None, Some(0.05)), volume: 9_000.0 },
        ];
        assert_eq!(ladder[0].yes_mid, None, "a 0.01/0.99 bracket is not a quote");
        assert_eq!(ladder[4].yes_mid, None, "a one-sided book is not a quote");
        assert_eq!(pick_at_the_money(&ladder, None), Some((2, AtmBasis::Mid)));
        assert_eq!(pick_at_the_money(&ladder, Some(0.0)), Some((2, AtmBasis::Mid)), "a zero reference is no reference");
        assert_eq!(pick_at_the_money(&ladder, Some(-1.0)), Some((2, AtmBasis::Mid)));
    }

    /// No price and no quotes: the hottest rung, flagged as such; equal
    /// volumes resolve to the first listed, as the old rule did.
    #[test]
    fn without_price_or_quotes_volume_decides_and_says_so() {
        let ladder = vec![rung(1780.0, 120.0), rung(2645.0, 40_000.0), rung(1785.0, 300.0)];
        assert_eq!(pick_at_the_money(&ladder, None), Some((1, AtmBasis::Volume)));
        let flat = vec![rung(1780.0, 0.0), rung(1785.0, 0.0), rung(1790.0, 0.0)];
        assert_eq!(pick_at_the_money(&flat, None), Some((0, AtmBasis::Volume)));
        assert_eq!(pick_at_the_money(&[], Some(1789.0)), None);
        assert_eq!(pick_at_the_money(&[], None), None);
        // Nothing finite to bracket: the reference rule yields to the others
        // rather than panicking.
        let nan = vec![LadderRung { strike: f64::NAN, yes_mid: Some(0.48), volume: 5.0 }, LadderRung { strike: f64::NAN, yes_mid: None, volume: 9.0 }];
        assert_eq!(pick_at_the_money(&nan, Some(100.0)), Some((0, AtmBasis::Mid)));
    }

    /// Two expiries of one cadence on the board put two rungs at one strike:
    /// the hotter stands for the strike on either side of the reference.
    #[test]
    fn duplicate_strikes_are_represented_by_the_hotter_rung() {
        let ladder = vec![rung(1790.0, 500.0), rung(1785.0, 300.0), rung(1790.0, 20.0), rung(1785.0, 10.0)];
        assert_eq!(pick_at_the_money(&ladder, Some(1787.0)), Some((0, AtmBasis::Reference)), "above: the hotter 1,790");
        assert_eq!(pick_at_the_money(&ladder, Some(1786.0)), Some((0, AtmBasis::Reference)));
        let lower_hot = vec![rung(1790.0, 20.0), rung(1785.0, 300.0), rung(1790.0, 25.0), rung(1785.0, 10.0)];
        assert_eq!(pick_at_the_money(&lower_hot, Some(1787.0)), Some((1, AtmBasis::Reference)), "below: the hotter 1,785");
        assert_eq!(rungs_apart(&ladder, 1785.0, 1790.0), 1, "one strike, however many expiries list it");
    }

    /// Rung distance is the sorted-index difference, so a mixed-step ladder
    /// counts listed strikes, not dollars.
    #[test]
    fn rungs_apart_counts_listed_strikes_on_a_mixed_step_ladder() {
        let ladder = vec![rung(70.0, 0.0), rung(69.0, 0.0), rung(69.5, 0.0), rung(70.5, 0.0), rung(69.25, 0.0)];
        assert_eq!(rungs_apart(&ladder, 69.0, 69.25), 1);
        assert_eq!(rungs_apart(&ladder, 69.0, 70.5), 4);
        assert_eq!(rungs_apart(&ladder, 70.5, 69.0), 4);
        assert_eq!(rungs_apart(&ladder, 69.5, 69.5), 0);
        assert_eq!(rungs_apart(&ladder, 69.5, 70.4), 2, "a strike off the ladder sits at its nearest rung");
        assert_eq!(nearest_rung(&ladder, 69.6), Some(69.5));
        assert_eq!(nearest_rung(&ladder, 69.75), Some(69.5), "midway: the lower rung");
        assert_eq!(nearest_rung(&ladder, 80.0), Some(70.5));
        assert_eq!(nearest_rung(&[], 1.0), None);
    }
}
