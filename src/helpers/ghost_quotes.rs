//! Venue-neutral simulation of resting maker quotes in ghost mode.
//!
//! # Why this exists
//!
//! A ghost maker quote used to be stamped filled the instant it was placed, on
//! every venue. That is not what a resting bid does, and the consequence was not
//! subtle: the Maker viper quotes at least `maker_cross_buffer` BELOW the ask by
//! construction, so a quote treated as instantly filled was born in profit
//! against the live bid and satisfied its own take-profit on the very next tick.
//! Quote, "fill", take profit, re-quote, roughly every five seconds.
//!
//! Measured on Polymarket International, 2026-08-30: 60 trades recorded in 45
//! minutes with 6 distinct outcomes among them, every one fabricated. Because
//! `GHOST_MODE_DEFAULT` is true for a new install, this was the DEFAULT first-run
//! experience, and it poisoned exactly the paper record ghost mode exists to
//! produce.
//!
//! The first fix lived inside the intl patrol loop and so repaired one venue of
//! three. Kalshi and Polymarket US reach the venue through their own traders and
//! kept fabricating fills. This module is that fix made venue-neutral: every
//! venue rests its simulated quotes here and asks the same question of its own
//! order book each tick.
//!
//! # The model
//!
//! A resting BUY at `Q` fills when somebody is willing to sell at `Q`, which is
//! when the best ASK falls to `Q` or below. Nothing sooner is a fill; it is the
//! simulator handing itself a trade.
//!
//! Two deliberate conservatisms, both erring toward reporting FEWER simulated
//! fills than reality would give:
//!
//!   * The ask is sampled once per tick, so a book that dips through a quote and
//!     recovers in between is missed.
//!   * Queue position is not modeled, but the fill requires the ask to reach the
//!     quote rather than merely touch the bid, which is the stricter test.
//!
//! # The price is frozen
//!
//! A resting quote keeps its price until it fills or is pulled. This mirrors the
//! live path, which drops a re-emitted quote while an unfilled one exists, and it
//! is load-bearing rather than cosmetic: the quote is priced at
//! `ask - maker_cross_buffer`, so a quote that followed the ask down would hold
//! that gap open forever and an ask drifting toward it could never arrive. Only a
//! gap larger than the cross buffer inside a single tick would fill, which is
//! precisely the adversely-selected fill. Simulated results would then carry
//! every pick-off and none of the ordinary fills, understating the Maker as badly
//! as the treadmill overstated it.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::state::{OrderParams, Position, PositionKey};
use crate::venues::core::MarketId;

/// Simulated quotes that have been PLACED but not yet FILLED.
///
/// Held outside the shared position map on purpose. `Position::fill_effective_at`
/// treats ANY ghost position as filled at `opened_at` — correct for taker entries,
/// where an immediate fill really is the right simulation, and wrong for a resting
/// quote. Keeping unfilled quotes out of that map leaves the taker behavior alone.
///
/// The originating `OrderParams` travel with the position because the venues
/// differ in what a fill obliges them to do: the intl patrol loop only needs the
/// position, while Kalshi and Polymarket US also record an entry through their
/// own `record_entry`, which is written against the order that caused it. Keeping
/// the request here means a venue can do its own bookkeeping at fill time instead
/// of reconstructing an order it no longer has.
#[derive(Debug, Clone)]
pub struct RestingQuote {
    /// The position this quote becomes if it fills.
    pub position: Position,
    /// The order as the viper asked for it.
    pub params: OrderParams,
}

fn registry() -> &'static Mutex<HashMap<PositionKey, RestingQuote>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PositionKey, RestingQuote>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock() -> std::sync::MutexGuard<'static, HashMap<PositionKey, RestingQuote>> {
    match registry().lock() {
        Ok(g) => g,
        // A panic in another holder must not take the trading loop with it; the
        // map is plain data and is safe to keep using.
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Would a resting BUY limit at `quote_price` have been filled by this book?
///
/// `ask == 1` is the "no book" sentinel the price channels default to, not an
/// offer to sell at a dollar, and `ask == 0` is an empty side. Neither fills.
pub fn is_crossed(ask: Decimal, quote_price: Decimal) -> bool {
    ask > dec!(0) && ask < dec!(1) && ask <= quote_price
}

/// Rest a simulated quote.
///
/// Returns true when this call actually placed one. A key that is already resting
/// is left untouched, price and all, so a viper re-emitting the same signal every
/// tick repositions nothing.
pub fn rest(key: PositionKey, position: Position, params: OrderParams) -> bool {
    let mut map = lock();
    if map.contains_key(&key) {
        return false;
    }
    map.insert(key, RestingQuote { position, params });
    true
}

/// Is a simulated quote resting on this key?
pub fn is_resting(key: &PositionKey) -> bool {
    lock().contains_key(key)
}

/// Remove a simulated quote: a quote-pull, or a leg being torn down.
pub fn pull(key: &PositionKey) -> Option<RestingQuote> {
    lock().remove(key)
}

/// Take `owner`'s resting quotes whose book has crossed them, leaving the rest.
///
/// `ask_for` returns the current best ask for a token, or `None` when this caller
/// has no book for it — a quote whose market this tick cannot price is left
/// resting rather than filled or discarded.
///
/// Returned positions are stamped `fill_confirmed_at`; the caller inserts them
/// into its own position map, which is also where it must decide what to do if
/// the slot is already occupied.
///
/// # Ownership is checked HERE, not by the caller
///
/// The registry is shared by every squadron in the process, and two squadrons of
/// the same class may trade the same market — that is what the squadron component
/// of `PositionKey` exists for. On intl they also share the persistent daily maker
/// market, so both price the same tokens.
///
/// An earlier version removed every crossed quote and left each caller to skip
/// the ones that were not its own. A skipped quote had already been removed, so
/// the first squadron to tick silently destroyed the other's fill: no position, no
/// log line, and the owner's viper then re-quoted at the current price, breaking
/// the frozen-price invariant at exactly the moment it matters. Filtering inside
/// the take is what makes that unrepresentable, rather than a rule the next venue
/// has to remember.
pub fn take_crossed(
    owner: &str,
    ask_for: impl Fn(&MarketId) -> Option<Decimal>,
) -> Vec<(PositionKey, RestingQuote)> {
    let mut map = lock();
    let crossed: Vec<PositionKey> = map
        .iter()
        .filter(|(k, q)| {
            k.squadron == owner
                && ask_for(&q.position.pair_token_id)
                    .is_some_and(|ask| is_crossed(ask, q.position.avg_entry))
        })
        .map(|(k, _)| k.clone())
        .collect();
    crossed
        .into_iter()
        .filter_map(|k| {
            map.remove(&k).map(|mut q| {
                q.position.fill_confirmed_at = Some(chrono::Utc::now());
                (k, q)
            })
        })
        .collect()
}

/// Drop every simulated quote in the process.
///
/// For a caller that owns a process-wide transition. Nothing owns one today —
/// ghost mode is toggled per squadron and each clears its own via
/// [`clear_squadron`] — so this is currently used only by tests. Kept because the
/// global ghost toggle fans out to every squadron, and a single clear is the
/// honest expression of that if it ever gets one owner.
#[cfg(test)]
pub fn clear() {
    lock().clear();
}

/// Drop every simulated quote belonging to one squadron, for stand-down and
/// market rotation.
pub fn clear_squadron(squadron_id: &str) {
    lock().retain(|k, _| k.squadron != squadron_id);
}

/// How many quotes are currently resting. Diagnostics and tests.
#[cfg(test)]
pub fn resting_count() -> usize {
    lock().len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    // The registry is process-global and tests run in parallel, so every test
    // uses its own squadron and token names, scopes its `ask_for` to its own
    // token, and tears down with `clear_squadron`. Nothing here may call the
    // global `clear()` or assert on `resting_count()`, both of which would see
    // and disturb other tests' quotes.

    fn params(token: &str, price: Decimal) -> OrderParams {
        OrderParams {
            token_id: MarketId::new(token),
            price,
            shares: dec!(10),
            fee_bps: 0,
            is_neg_risk: false,
            market_name: "test market".to_string(),
            condition_id: "test-condition".to_string(),
            order_type: crate::venues::core::TimeInForce::Gtc,
            post_only: true,
            ghost_mode: true,
        }
    }

    fn quote(token: &str, price: Decimal) -> Position {
        Position {
            shares: dec!(10),
            avg_entry: price,
            opened_at: Utc::now(),
            close_time: None,
            market_name: "test market".to_string(),
            pair_token_id: MarketId::new(token),
            fill_confirmed_at: None,
            paired_leg_token_id: None,
            entry_fee: Decimal::ZERO,
        }
    }

    fn key(squadron: &str, token: &str) -> PositionKey {
        PositionKey::new(squadron, "MakerStrategy", MarketId::new(token))
    }

    /// An ask quoted only for `token`; every other test's quote reads as unpriced.
    fn only(token: &'static str, ask: Decimal) -> impl Fn(&MarketId) -> Option<Decimal> {
        move |m: &MarketId| (m.as_str() == token).then_some(ask)
    }

    /// Reproduces the 2026-08-30 treadmill and asserts it cannot recur.
    ///
    /// The maker quoted a NO leg at $0.15 into a book whose ask was far above it.
    /// Filled on placement, the position was instantly in profit against the live
    /// $0.16 bid, took its target on the next tick, re-quoted, and repeated every
    /// five seconds: 60 trades in 45 minutes, 6 of them distinct.
    #[test]
    fn a_quote_under_the_ask_does_not_fill() {
        assert!(!is_crossed(dec!(0.21), dec!(0.15)), "ask $0.21 is nobody selling at $0.15");
        assert!(!is_crossed(dec!(0.16), dec!(0.15)), "not even one tick above");
        assert!(is_crossed(dec!(0.15), dec!(0.15)), "ask AT the quote fills");
        assert!(is_crossed(dec!(0.14), dec!(0.15)), "ask through the quote fills");
    }

    /// An absent book is not a counterparty.
    #[test]
    fn an_empty_book_never_fills() {
        assert!(!is_crossed(dec!(1), dec!(0.15)), "the no-book sentinel must not fill");
        assert!(!is_crossed(dec!(0), dec!(0.15)), "an empty ask side must not fill");
        assert!(!is_crossed(dec!(1), dec!(0.99)));
    }

    /// A resting quote's price is frozen: re-emission must not reprice it, or an
    /// ask drifting toward the quote could never reach it.
    #[test]
    fn re_resting_the_same_key_does_not_reprice() {
        const TOK: &str = "tok-freeze";
        let k = key("sq-freeze", TOK);
        assert!(rest(k.clone(), quote(TOK, dec!(0.15)), params(TOK, dec!(0.15))));
        assert!(!rest(k.clone(), quote(TOK, dec!(0.18)), params(TOK, dec!(0.18))), "second rest is a no-op");

        let filled = take_crossed("sq-freeze", only(TOK, dec!(0.15)));
        assert_eq!(filled.len(), 1);
        assert_eq!(filled[0].1.position.avg_entry, dec!(0.15), "the frozen price is what fills");
        clear_squadron("sq-freeze");
    }

    /// A crossed quote leaves the registry, is stamped filled, and is handed back
    /// exactly once.
    #[test]
    fn a_crossed_quote_is_taken_once_and_stamped() {
        const TOK: &str = "tok-take";
        let k = key("sq-take", TOK);
        rest(k.clone(), quote(TOK, dec!(0.40)), params(TOK, dec!(0.40)));

        assert!(take_crossed("sq-take", only(TOK, dec!(0.55))).is_empty(), "an ask above must not fill");
        assert!(is_resting(&k), "and must leave it resting");

        let filled = take_crossed("sq-take", only(TOK, dec!(0.39)));
        assert_eq!(filled.len(), 1);
        assert!(filled[0].1.position.fill_confirmed_at.is_some(), "the fill must be stamped");
        assert!(!is_resting(&k), "and the quote must be gone");
        assert!(take_crossed("sq-take", only(TOK, dec!(0.39))).is_empty(), "never twice");
        clear_squadron("sq-take");
    }

    /// A token this tick cannot price is left alone: not filled, not dropped.
    #[test]
    fn a_quote_with_no_book_is_left_resting() {
        const TOK: &str = "tok-nobook";
        let k = key("sq-nobook", TOK);
        rest(k.clone(), quote(TOK, dec!(0.30)), params(TOK, dec!(0.30)));
        assert!(take_crossed("sq-nobook", |_| None).is_empty(), "no book means no verdict");
        assert!(is_resting(&k), "and the quote survives to be judged later");
        clear_squadron("sq-nobook");
    }

    /// One squadron must not be able to take another's quote.
    ///
    /// Both squadrons quote the same token — the shape the squadron component of
    /// `PositionKey` exists to support, and on intl the daily maker market is
    /// shared by construction. An earlier version removed every crossed quote and
    /// filtered by owner afterwards, so whichever squadron ticked first destroyed
    /// the other's fill silently: no position, no log line, and the owner's viper
    /// then re-quoted at the current price.
    #[test]
    fn one_squadron_cannot_take_anothers_quote() {
        const TOK: &str = "tok-shared";
        let mine   = PositionKey::new("sq-owner",  "MakerStrategy", MarketId::new(TOK));
        let theirs = PositionKey::new("sq-bystander", "MakerStrategy", MarketId::new(TOK));
        rest(mine.clone(),   quote(TOK, dec!(0.30)), params(TOK, dec!(0.30)));
        rest(theirs.clone(), quote(TOK, dec!(0.30)), params(TOK, dec!(0.30)));

        // The bystander ticks first and prices the shared book.
        let taken = take_crossed("sq-bystander", only(TOK, dec!(0.29)));
        assert_eq!(taken.len(), 1, "it may take only its own");
        assert_eq!(taken[0].0.squadron, "sq-bystander");
        assert!(is_resting(&mine), "the other squadron's quote must survive intact");

        // And the owner still gets its fill when its own tick comes round.
        let taken = take_crossed("sq-owner", only(TOK, dec!(0.29)));
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].0.squadron, "sq-owner");

        clear_squadron("sq-owner");
        clear_squadron("sq-bystander");
    }

    /// Teardown is per squadron, because the registry is shared by all of them.
    #[test]
    fn clearing_one_squadron_leaves_the_others() {
        let a = key("sq-clear-a", "tok-clear-a");
        let b = key("sq-clear-b", "tok-clear-b");
        rest(a.clone(), quote("tok-clear-a", dec!(0.30)), params("tok-clear-a", dec!(0.30)));
        rest(b.clone(), quote("tok-clear-b", dec!(0.30)), params("tok-clear-b", dec!(0.30)));
        clear_squadron("sq-clear-a");
        assert!(!is_resting(&a), "the named squadron's quote is gone");
        assert!(is_resting(&b), "the other squadron is untouched");
        clear_squadron("sq-clear-b");
    }
}
