//! When is a sports game OVER, as far as the engine can tell?
//!
//! Venue-neutral by construction: the inputs are the bookmaker board, the venue
//! book and the Sports Raptor's own records, none of which is venue-specific.
//! Wired into the Polymarket International single-market patrol
//! (`patrol_impl`), where the defect it closes was first observed, and since
//! 2026-10-19 into the Kalshi market loop (`venues::kalshi::trader`), which
//! showed the same symptom: Kalshi dates a game market's close two days after
//! the game, so a finished game held the sports slot until then. The Polymarket
//! US loop retires on its own close phase and has not shown the symptom; if it
//! does, this is the rule to wire, unchanged.

use chrono::Utc;
use rust_decimal::Decimal;

/// What a sports squadron's book says right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SportsBook {
    /// No level on either side, continuously for this long.
    Absent { for_secs: i64 },
    /// A book exists; this is the best bid on either side.
    Live { top_bid: Decimal },
}

/// The facts the game-over rule reads. The patrol gathers them once a minute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SportsGameSignals {
    /// From the ledger's rows; `None` when the ledger never matched the game.
    pub kick_off: Option<chrono::DateTime<Utc>>,
    /// Whether the bookmaker board still carries a line for either token. Both
    /// in-play and finished games leave the board; only a pre-game line stays.
    pub on_board: bool,
    /// Whether the ledger has recorded the venue's resolution for either token.
    pub resolved_by_ledger: bool,
    pub book: SportsBook,
}

/// Why a sports game reads as over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GameOver {
    /// The Sports Raptor recorded the venue's own resolution.
    Resolved,
    /// Well past kick-off, off the board, and no order book at all.
    BookDark { for_secs: i64 },
    /// Well past kick-off, off the board, and the book prices one side at or
    /// above the decided bid.
    BookDecided { top_bid: Decimal },
}

impl GameOver {
    pub fn describe(&self) -> String {
        match self {
            GameOver::Resolved => "the Sports Raptor recorded the venue's resolution".to_string(),
            GameOver::BookDark { for_secs } => format!("no order book for {for_secs}s"),
            GameOver::BookDecided { top_bid } => format!("book priced as decided (best bid {top_bid})"),
        }
    }
}

/// Is a sports squadron's game over, as far as the engine can tell RIGHT NOW?
///
/// This exists because the two signals `event_market_retire_reason` trusts both
/// fail on a finished Polymarket game: the venue reports the market open until
/// it formally resolves, hours or days after the final, and the stated close is
/// a week after the game. Meanwhile the sports slot is one squadron at a time
/// and the seeder skips a class while one is live, so a finished game holds the
/// slot through the next slate. Observed 2026-09-29: White Sox/Astros, first
/// pitch 3h40m earlier, book 0.999/0.001, board line gone, venue still "open".
///
/// Three facts have to agree, and each one alone is a trap:
///
/// * **Kick-off is well past** (`after_kick_off_secs`). Alone it would retire a
///   game running long; and a market with no kick-off on record (the ledger
///   never matched it) can never be judged over by this rule at all.
/// * **The board no longer holds a line.** Alone it would retire every in-play
///   game, because both in-play and finished games leave the board. And a line
///   still on the board means pre-game, which is never over, whatever the clock
///   or the book says.
/// * **The book says done**: no book at all, or one side bid at or above
///   `decided_bid`. A live game past the clock still shows a two-sided book
///   inside that bid and is left alone. A brand-new market with no book yet is
///   protected by the first fact, not this one: its kick-off is ahead.
///
/// The ledger's recorded resolution short-circuits the book: the venue itself
/// has said who won. The verdict is instantaneous; `sports_game_over_due` adds
/// the grace so a momentary dark or one-sided book does not retire anything.
pub fn sports_game_over_verdict(
    now: chrono::DateTime<Utc>,
    s: &SportsGameSignals,
    after_kick_off_secs: i64,
    decided_bid: Decimal,
) -> Option<GameOver> {
    if s.on_board {
        return None;
    }
    let kick_off = s.kick_off?;
    if (now - kick_off).num_seconds() < after_kick_off_secs.max(0) {
        return None;
    }
    if s.resolved_by_ledger {
        return Some(GameOver::Resolved);
    }
    match s.book {
        SportsBook::Absent { for_secs } => Some(GameOver::BookDark { for_secs }),
        SportsBook::Live { top_bid } if top_bid >= decided_bid => Some(GameOver::BookDecided { top_bid }),
        SportsBook::Live { .. } => None,
    }
}

/// Should the squadron stand down on a game-over verdict?
///
/// Never while holding, exactly as the other retirement reasons: standing down
/// on top of a position strands it with no viper to exit it. A recorded
/// resolution is acted on at once; a dark or decided book must have read that
/// way continuously for the grace, the same patience the venue's "no" gets.
pub fn sports_game_over_due(
    verdict: Option<GameOver>,
    held_for_secs: i64,
    grace_secs: i64,
    holding_position: bool,
) -> bool {
    if holding_position {
        return false;
    }
    match verdict {
        None => false,
        Some(GameOver::Resolved) => true,
        Some(GameOver::BookDark { .. }) | Some(GameOver::BookDecided { .. }) => held_for_secs >= grace_secs,
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    const AFTER: i64 = 10_800;
    const DECIDED: Decimal = dec!(0.99);
    const GRACE: i64 = 300;

    fn kicked_off(secs_ago: i64) -> Option<chrono::DateTime<Utc>> {
        Some(Utc::now() - chrono::Duration::seconds(secs_ago))
    }

    fn signals(kick_off: Option<chrono::DateTime<Utc>>, on_board: bool, resolved: bool, book: SportsBook) -> SportsGameSignals {
        SportsGameSignals { kick_off, on_board, resolved_by_ledger: resolved, book }
    }

    /// 2026-09-29, exactly as production had it: White Sox/Astros, first pitch
    /// 13,236s earlier, the board has dropped the game, the book is 0.999/0.001
    /// (the ledger's last row), and the venue still reports the market open.
    /// Over, and due once the book has read that way for the grace.
    #[test]
    fn tonights_case_a_finished_game_the_venue_still_calls_open_is_over() {
        let s = signals(kicked_off(13_236), false, false, SportsBook::Live { top_bid: dec!(0.999) });
        let v = sports_game_over_verdict(Utc::now(), &s, AFTER, DECIDED);
        assert_eq!(v, Some(GameOver::BookDecided { top_bid: dec!(0.999) }));
        assert!(!sports_game_over_due(v, GRACE - 1, GRACE, false), "a momentary one-sided book is not a retirement");
        assert!(sports_game_over_due(v, GRACE, GRACE, false));
        assert!(!sports_game_over_due(v, 86_400, GRACE, true), "never on top of a position");
    }

    /// The same evening as the engine saw it: no order book at all for minutes
    /// ("MARKET DATA DARK ... no book for 6m 10s"). Over for the same reason.
    #[test]
    fn tonights_case_as_a_dark_book_is_over_too() {
        let s = signals(kicked_off(13_236), false, false, SportsBook::Absent { for_secs: 370 });
        let v = sports_game_over_verdict(Utc::now(), &s, AFTER, DECIDED);
        assert_eq!(v, Some(GameOver::BookDark { for_secs: 370 }));
        assert!(sports_game_over_due(v, GRACE, GRACE, false));
    }

    /// Trap 2: a freshly rotated market whose book has not appeared yet. Its
    /// kick-off is ahead, so the rule says nothing and the MARKET DATA DARK
    /// warning keeps its job.
    #[test]
    fn a_fresh_market_with_no_book_yet_is_not_over() {
        let s = signals(kicked_off(-7_200), false, false, SportsBook::Absent { for_secs: 600 });
        assert_eq!(sports_game_over_verdict(Utc::now(), &s, AFTER, DECIDED), None);
        // Nor one whose line is still on the board, however the book looks.
        let s = signals(kicked_off(-7_200), true, false, SportsBook::Absent { for_secs: 600 });
        assert_eq!(sports_game_over_verdict(Utc::now(), &s, AFTER, DECIDED), None);
    }

    /// Trap 1: a game in progress. It has left the board, like a finished game,
    /// and it has a live two-sided book, unlike one. Not over: not an hour in,
    /// and not five hours in either (extra innings, overtime, a rain delay).
    #[test]
    fn a_live_in_play_game_is_not_over_however_long_it_runs() {
        for ago in [3_600, AFTER, AFTER + 3_600] {
            let s = signals(kicked_off(ago), false, false, SportsBook::Live { top_bid: dec!(0.70) });
            assert_eq!(sports_game_over_verdict(Utc::now(), &s, AFTER, DECIDED), None, "{ago}s in");
        }
        // Just under the decided bid is still a live book.
        let s = signals(kicked_off(AFTER), false, false, SportsBook::Live { top_bid: dec!(0.985) });
        assert_eq!(sports_game_over_verdict(Utc::now(), &s, AFTER, DECIDED), None);
    }

    /// Before the kick-off window, a decided-looking or dark book proves nothing:
    /// a blowout in the third quarter is not over, and neither is a feed hiccup.
    #[test]
    fn nothing_is_over_inside_the_kick_off_window() {
        let s = signals(kicked_off(AFTER - 1), false, false, SportsBook::Live { top_bid: dec!(0.999) });
        assert_eq!(sports_game_over_verdict(Utc::now(), &s, AFTER, DECIDED), None);
        let s = signals(kicked_off(AFTER - 1), false, false, SportsBook::Absent { for_secs: 3_000 });
        assert_eq!(sports_game_over_verdict(Utc::now(), &s, AFTER, DECIDED), None);
    }

    /// A line still on the board means pre-game, and pre-game is never over, even
    /// if the clock says otherwise (a postponed game keeps its old kick-off).
    #[test]
    fn a_line_on_the_board_is_never_over() {
        let s = signals(kicked_off(AFTER + 3_600), true, false, SportsBook::Live { top_bid: dec!(0.999) });
        assert_eq!(sports_game_over_verdict(Utc::now(), &s, AFTER, DECIDED), None);
    }

    /// No kick-off on record (the ledger never matched the game): the rule
    /// cannot judge, and says so rather than guessing.
    #[test]
    fn an_unmatched_game_cannot_be_judged_over() {
        let s = signals(None, false, false, SportsBook::Absent { for_secs: 86_400 });
        assert_eq!(sports_game_over_verdict(Utc::now(), &s, AFTER, DECIDED), None);
    }

    /// The ledger's recorded resolution is the venue's own word: over at once,
    /// whatever the book shows, with no grace to wait out.
    #[test]
    fn a_recorded_resolution_is_over_immediately() {
        let s = signals(kicked_off(AFTER), false, true, SportsBook::Live { top_bid: dec!(0.60) });
        let v = sports_game_over_verdict(Utc::now(), &s, AFTER, DECIDED);
        assert_eq!(v, Some(GameOver::Resolved));
        assert!(sports_game_over_due(v, 0, GRACE, false));
        assert!(!sports_game_over_due(v, 0, GRACE, true));
    }

    /// The kick-off lookup reads the ledger's rows, so a squadron on a matched
    /// game finds its start; one on a game the ledger never saw finds nothing.
    #[tokio::test]
    async fn kick_off_comes_from_the_ledgers_rows() {
        let pool = crate::helpers::db::memory_pool_for_tests().await;
        let row = crate::helpers::db::SportsLedgerRow {
            ts: "2026-09-29T19:00:00Z".into(), league: "mlb".into(), sport_key: "baseball_mlb".into(),
            odds_event_id: "ev".into(), pm_slug: "mlb-cws-hou-2026-09-29".into(),
            condition_id: "0xcws".into(), token_id: "cws-yes".into(), outcome_label: "Chicago White Sox".into(),
            odds_outcome: "Chicago White Sox".into(), commence: "2026-09-29T21:00:00Z".into(), secs_to_start: 7200,
            consensus: Some(0.41), num_books: 9, dispersion: Some(0.02), max_book_age_secs: Some(60),
            pm_bid: Some(0.40), pm_ask: Some(0.42), pm_bid_size: Some(100.0), pm_ask_size: Some(100.0),
            credits_remaining: Some(90_000), odds_at: Some("2026-09-29T19:00:00Z".into()),
            pm_at: Some("2026-09-29T19:00:00Z".into()), overround: Some(1.04), raw_consensus: Some(0.43),
        };
        assert_eq!(crate::helpers::db::record_sports_ledger_rows(&pool, &[row]).await, 1);
        let kick = crate::helpers::db::sports_ledger_kick_off(&pool, "0xcws").await.expect("matched game");
        assert_eq!(kick.to_rfc3339(), "2026-09-29T21:00:00+00:00");
        assert!(crate::helpers::db::sports_ledger_kick_off(&pool, "0xnever").await.is_none());
    }
}

