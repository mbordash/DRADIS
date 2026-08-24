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

/// Shared state types for the orchestrator and strategies.
/// Defines clear ownership boundaries and data structures used across the system.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use crate::venues::core::TimeInForce;

use crate::venues::core::MarketId;

// ─── Phantom-fill / orphan tracking aliases ───────────────────────────────────
// Venue-neutral shared maps used by the balance/orphan handlers and SessionState.
// Defined here (not in the intl-gated `helpers::balance`) so venue-neutral code
// can reference them under any active venue.
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Instant;

// ─── Trade filing dimensions ─────────────────────────────────────────────────

/// The dimensions a trade or entry row is filed under.
///
/// These were historically collapsed into a single `asset: &str` parameter that
/// meant different things on different venues — `btc`/`eth`/`sol` on the intl
/// CLOB, but `us`, `us-crypto`, `kalshi` elsewhere. The Control Tower rendered
/// that value under an "Asset" column, so a Kalshi BTC trade displayed as asset
/// "KALSHI", and BTC vs ETH trades on the same venue were indistinguishable
/// because they shared one shard. Splitting the concepts apart makes each field
/// answer exactly one question.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeScope {
    /// SQLite pool selector — a *storage location*, not a market attribute.
    /// One database file per shard; several assets may share one.
    pub shard: String,
    /// Which exchange executed this. Empty means "resolve from the shard
    /// registry" (see `db::venue_for_shard`).
    pub venue: String,
    /// Market taxonomy: `crypto` | `sports` | `politics` | `unknown`.
    /// `None` on reconciliation paths that recover a row without a live
    /// squadron to ask.
    pub market_class: Option<String>,
    /// Underlying symbol (`btc`, `eth`, `sol`).
    ///
    /// `None` is a first-class value, not a gap: a market like "will the Chiefs
    /// win" or "who takes the Senate" has no underlying instrument. Any design
    /// that forces a symbol here has to invent one.
    pub underlying: Option<String>,
}

impl TradeScope {
    pub fn new(
        shard: impl Into<String>,
        venue: impl Into<String>,
        market_class: Option<String>,
        underlying: Option<String>,
    ) -> Self {
        Self { shard: shard.into(), venue: venue.into(), market_class, underlying }
    }

    /// Scope for paths that know only which database to write to — chain-sync
    /// reconciliation, retrospective settlement booking, API backfill. The venue
    /// still resolves from the shard registry; class and underlying stay `NULL`
    /// rather than being guessed.
    pub fn shard_only(shard: impl Into<String>) -> Self {
        Self { shard: shard.into(), venue: String::new(), market_class: None, underlying: None }
    }

    /// Convenience for crypto markets, where the underlying and the taxonomy
    /// class are both known up front.
    pub fn crypto(
        shard: impl Into<String>,
        venue: impl Into<String>,
        underlying: impl Into<String>,
    ) -> Self {
        Self::new(shard, venue, Some("crypto".to_string()), Some(underlying.into()))
    }
}

/// Cooldown map keyed by an opaque fingerprint string → expiry `Instant`.
pub type PhantomCooldowns = Arc<Mutex<HashMap<String, Instant>>>;
/// Set of market ids that have been flattened/abandoned and must not be re-hedged.
pub type OrphanTombstones = Arc<Mutex<HashSet<MarketId>>>;
/// Set of token ids whose market the arbitrage viper has already committed a pair
/// to this session. Once locked, no second arb pair is opened on that market — the
/// viper holds the single pair to settlement instead of churning re-entries.
/// Session-scoped (never cleared on market rotation); each new daily/window market
/// has fresh tokens, so the next market trades normally.
pub type ArbMarketLockouts = Arc<Mutex<HashSet<MarketId>>>;

// ─── WebSocket price feed ─────────────────────────────────────────────────────

/// Live orderbook price snapshot from the Polymarket WebSocket.
///
/// Tuple layout: `(best_bid, bid_depth, best_ask, ask_depth, ws_update_timestamp)`
///
/// Previously a `type` alias private to `main.rs`.  Promoted here in Phase 3f-2
/// so `Squadron::subscribe_markets()` and the tick loop share a single definition.
/// One venue's top-of-book plus aggregate depth, as published on a watch channel.
///
/// `(best_bid, bid_size_at_touch, best_ask, ask_size_at_touch, ws_timestamp,
///   cumulative_bid_depth, cumulative_ask_depth)`
///
/// The first five are the historical layout and are read positionally in a dozen
/// places; the depths were appended rather than folded into a struct so those
/// indices stay valid. Prefer the `price_state` accessors below over indexing.
///
/// The distinction between the touch sizes and the cumulative depths matters.
/// Elements 1 and 3 are the size resting at the single best level; 5 and 6 sum
/// every level the venue publishes. Order-book imbalance has always been
/// computed from the former, which makes it a TOP-OF-BOOK ratio — one resting
/// order on each side deciding whether flow is toxic. On 2026-08-21 that read
/// 0.88, -0.33, -0.08, 0.90 and 0.11 across four minutes on an unremarkable
/// book. The cumulative figures exist so the same measure can be computed over
/// the whole book and the two compared on live data before any threshold moves.
pub type PriceState = (
    Decimal,        // 0 best bid
    Decimal,        // 1 size at best bid
    Decimal,        // 2 best ask
    Decimal,        // 3 size at best ask
    DateTime<Utc>,  // 4 websocket receipt time
    Decimal,        // 5 cumulative bid depth, all levels
    Decimal,        // 6 cumulative ask depth, all levels
);

/// Named accessors for [`PriceState`], so new code never indexes a 7-tuple.
#[cfg(test)]
mod strategy_market_key_tests {
    use super::strategy_market_key;

    /// The collision: both US wings and all three Kalshi squadrons run the same
    /// venue-agnostic viper kinds. Keyed by kind alone, whichever squadron
    /// published last won, so a squadron page named one market in its header and
    /// a different one on its viper cards.
    #[test]
    fn the_same_viper_on_two_squadrons_gets_two_keys() {
        assert_ne!(
            strategy_market_key("us-open", "maker"),
            strategy_market_key("us-crypto-open", "maker"),
        );
        assert_ne!(
            strategy_market_key("politics-open", "arbitrage"),
            strategy_market_key("sports-open", "arbitrage"),
        );
    }

    /// Squadron id rather than asset: every Kalshi squadron reports asset
    /// "KALSHI", so keying on that would collide exactly as before.
    #[test]
    fn different_vipers_on_one_squadron_stay_distinct() {
        assert_ne!(
            strategy_market_key("btc-open", "maker"),
            strategy_market_key("btc-open", "arbitrage"),
        );
    }

    /// The key must be reproducible from the same inputs, since the writer is
    /// the engine and the reader is the browser.
    #[test]
    fn the_key_is_stable_and_contains_both_parts() {
        let k = strategy_market_key("us-crypto-open", "fairvalue");
        assert_eq!(k, strategy_market_key("us-crypto-open", "fairvalue"));
        assert!(k.contains("us-crypto-open"));
        assert!(k.contains("fairvalue"));
    }
}

#[cfg(test)]
mod signal_exposure_tests {
    use super::{StrategySignal, OrderParams, MarketId};
    use crate::venues::core::TimeInForce;
    use rust_decimal_macros::dec;

    fn params() -> OrderParams {
        OrderParams {
            token_id: MarketId::new("t"), price: dec!(0.5), shares: dec!(1),
            fee_bps: 0, is_neg_risk: false, market_name: "m".into(),
            condition_id: String::new(), order_type: TimeInForce::Fak,
            post_only: false, ghost_mode: true,
        }
    }

    /// Inside the RTB window and after a primary market closes, the loop keeps
    /// managing what it holds but takes on nothing new. Getting this backwards
    /// in either direction is costly: block exits and a position runs to expiry
    /// with no stop — which is how -$3.09 was lost on 2026-08-10 — while
    /// allowing entries buys into a market that is about to stop trading.
    #[test]
    fn only_entries_and_quotes_open_exposure() {
        assert!(StrategySignal::Entry { params: params(), pair_params: None }.opens_exposure());
        assert!(StrategySignal::MakerQuote { yes: Some(params()), no: None }.opens_exposure());

        for reducing in [
            StrategySignal::Exit { params: params(), reason: "stop".into(), exit_pair: false },
            StrategySignal::MakerCancel { tokens: vec![MarketId::new("t")] },
            StrategySignal::MakerRestingExit { params: params(), reason: "tp".into() },
            StrategySignal::NoSignal,
        ] {
            assert!(
                !reducing.opens_exposure(),
                "{reducing:?} must still flow while winding down",
            );
        }
    }
}

#[cfg(test)]
mod snapshot_obi_tests {
    use super::MarketSnapshot;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    /// Only the YES depths vary; everything else is inert. The NO side mirrors
    /// the YES side so a test that reads the wrong one fails loudly.
    fn snap(yb: Decimal, ya: Decimal, ybt: Decimal, yat: Decimal) -> MarketSnapshot {
        MarketSnapshot {
            yes_bid: dec!(0.50), yes_bid_depth: yb, yes_ask: dec!(0.52), yes_ask_depth: ya,
            no_bid:  dec!(0.48), no_bid_depth:  ya, no_ask:  dec!(0.50), no_ask_depth:  yb,
            yes_bid_depth_total: ybt, yes_ask_depth_total: yat,
            no_bid_depth_total:  yat, no_ask_depth_total:  ybt,
            oracle_price: Decimal::ZERO, velocity: Decimal::ZERO,
            velocity_1s: Decimal::ZERO, acceleration: Decimal::ZERO,
            funding_rate: Decimal::ZERO, institutional_pulse: Decimal::ZERO,
            tide_coherence: Decimal::ZERO, tradfi_velocity: Decimal::ZERO,
            macro_coherence: Decimal::ZERO, vix_proxy: Decimal::ZERO,
            vix_velocity: Decimal::ZERO, oi_delta_pct: Decimal::ZERO,
            cvd_ratio: Decimal::ZERO, oracle_drift_60m: Decimal::ZERO,
            oracle_drift_10m: Decimal::ZERO, hist_vol: Decimal::ZERO,
            secs_to_expiry: 0, timestamp: chrono::Utc::now(),
        }
    }

    /// The measured failure: one contract at the touch against a balanced book.
    /// Top-of-book reads maximally bid-heavy; the book reads flat. A -0.60 veto
    /// keyed on the touch fires on nothing real.
    #[test]
    fn a_single_resting_contract_moves_the_touch_but_not_the_book() {
        let s = snap(dec!(1), dec!(71), dec!(683574), dec!(702306));
        let touch = s.yes_obi(false);
        let book  = s.yes_obi(true);
        assert!(touch < dec!(-0.9), "touch should read extreme, got {touch}");
        assert!(book.abs() < dec!(0.05), "book should read flat, got {book}");
    }

    /// Selecting a source must not change the formula: with an equal book the
    /// two agree exactly, so any divergence is the data and never the maths.
    #[test]
    fn the_two_sources_agree_when_the_book_is_the_touch() {
        let s = snap(dec!(30), dec!(70), dec!(30), dec!(70));
        assert_eq!(s.yes_obi(false), s.yes_obi(true));
        assert_eq!(s.yes_obi(false), dec!(-0.4));
    }

    /// No depth is treated as maximally adverse, in either source. This
    /// conflates "no data" with "toxic flow" deliberately — see yes_obi.
    #[test]
    fn an_empty_book_reads_maximally_adverse() {
        let s = snap(dec!(0), dec!(0), dec!(0), dec!(0));
        assert_eq!(s.yes_obi(false), dec!(-1));
        assert_eq!(s.yes_obi(true),  dec!(-1));
    }

    /// The result stays inside [-1, 1] whichever source is chosen.
    #[test]
    fn imbalance_stays_bounded() {
        for (b, a) in [(dec!(1), dec!(0)), (dec!(0), dec!(1)), (dec!(999999), dec!(1))] {
            let s = snap(b, a, b, a);
            for whole in [false, true] {
                let v = s.yes_obi(whole);
                assert!(v >= dec!(-1) && v <= dec!(1), "out of range: {v}");
            }
        }
    }
}

pub mod price_state {
    use super::{Decimal, PriceState};

    pub fn best_bid(p: &PriceState) -> Decimal { p.0 }
    pub fn best_ask(p: &PriceState) -> Decimal { p.2 }
    /// Size resting at the best bid only.
    pub fn bid_touch_size(p: &PriceState) -> Decimal { p.1 }
    /// Size resting at the best ask only.
    pub fn ask_touch_size(p: &PriceState) -> Decimal { p.3 }
    /// Size across every published bid level.
    pub fn bid_depth_total(p: &PriceState) -> Decimal { p.5 }
    /// Size across every published ask level.
    pub fn ask_depth_total(p: &PriceState) -> Decimal { p.6 }

    /// Imbalance in `[-1, 1]`; positive means more resting size on the bid.
    ///
    /// `total = false` reproduces the historical top-of-book ratio exactly.
    /// `total = true` uses the whole book. Both return `-1` on an empty book,
    /// which every caller treats as maximally adverse — note that this makes
    /// "no data" and "toxic flow" indistinguishable downstream.
    pub fn imbalance(p: &PriceState, total: bool) -> Decimal {
        let (b, a) = if total {
            (bid_depth_total(p), ask_depth_total(p))
        } else {
            (bid_touch_size(p), ask_touch_size(p))
        };
        let sum = b + a;
        if sum > Decimal::ZERO { (b - a) / sum } else { Decimal::NEGATIVE_ONE }
    }

    /// Emit the periodic book + signal line for one market, at most once per
    /// [`HEARTBEAT_SECS`] per `label`.
    ///
    /// The intl CLOB gets this from `spawn_status_task`, which is spawned only
    /// from its own patrol loop. Kalshi and US retail drive their own loops and
    /// never reached it, so those instances printed no heartbeat at all: no ask
    /// sum, no mark, and none of the whole-book depth the order-book-imbalance
    /// retune is meant to be gathering. Both already had every figure to hand.
    ///
    /// Throttled per label rather than globally because a venue runs several
    /// markets at once — Kalshi a primary and a maker market, US a general and a
    /// crypto wing — and one global gate would let whichever ticked first
    /// silence the others.
    pub fn log_heartbeat(label: &str, yes: &PriceState, no: &PriceState, oracle: Decimal) {
        use std::collections::HashMap;
        use std::sync::{Mutex, OnceLock};
        use std::time::{Duration, Instant};

        /// Interval between heartbeat lines for a given market.
        const HEARTBEAT_SECS: u64 = 30;

        static LAST: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
        let reg = LAST.get_or_init(|| Mutex::new(HashMap::new()));
        {
            let mut reg = match reg.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            let now = Instant::now();
            match reg.get(label) {
                Some(prev) if now.duration_since(*prev) < Duration::from_secs(HEARTBEAT_SECS) => return,
                _ => { reg.insert(label.to_string(), now); }
            }
        }

        // A market with no underlying has no mark, and printing "$0.00" for it
        // reads as a dead price feed rather than as "not applicable" — which is
        // exactly the wrong impression on a politics or sports squadron whose
        // Raptor stack is neutral by design.
        let mark = if oracle > Decimal::ZERO {
            format!("${oracle:.2}")
        } else {
            "n/a".to_string()
        };
        tracing::info!(
            " Heartbeat [{}] | Ask Sum ${:.4} (Y ${:.2} / N ${:.2}) | Bid Sum ${:.4} (Y ${:.2} / N ${:.2}) | \
             Mark: {} | OBI Y={:.2} N={:.2} | OBIall Y={:.2} N={:.2} (depth Y {:.0}/{:.0} N {:.0}/{:.0})",
            label,
            best_ask(yes) + best_ask(no), best_ask(yes), best_ask(no),
            best_bid(yes) + best_bid(no), best_bid(yes), best_bid(no),
            mark,
            imbalance(yes, false), imbalance(no, false),
            imbalance(yes, true),  imbalance(no, true),
            bid_depth_total(yes), ask_depth_total(yes),
            bid_depth_total(no),  ask_depth_total(no),
        );
    }
}

/// Represents a single position held in the trading system.
/// Shared across strategies and the main orchestrator.
#[derive(Debug, Clone)]
pub struct Position {
    /// Amount of shares held
    pub shares: Decimal,
    /// Average entry price
    pub avg_entry: Decimal,
    /// When the position was opened
    pub opened_at: DateTime<Utc>,
    /// When the position was closed (if applicable)
    pub close_time: Option<DateTime<Utc>>,
    /// Human-readable market name
    pub market_name: String,
    /// Token ID for this position (venue-neutral canonical key — slice 2a)
    pub pair_token_id: MarketId,
    /// When the position balance was confirmed on-chain
    pub fill_confirmed_at: Option<DateTime<Utc>>,
    /// For paired strategies (Arbitrage, TimeDecay): token ID of the complementary leg.
    /// If Some, this position is part of a hedged pair. Used to detect orphaned positions
    /// when the paired leg fails to fill.
    pub paired_leg_token_id: Option<MarketId>,
    /// Total fee paid to OPEN this position, in dollars.
    ///
    /// Carried on the position because round-trip P&L is booked at exit, long
    /// after the entry fill is gone. Deliberately kept out of `avg_entry` so
    /// strategy TP/SL percentages keep measuring price movement rather than
    /// silently shifting when fees change. `ZERO` on venues that report none.
    pub entry_fee: Decimal,
}

/// Compound key for the shared position map: (strategy_name, token_id).
/// Each strategy has its own position slot per token, enabling fully independent
/// capital allocation and eliminating cross-strategy entry conflicts (Option A).
///
/// Slice 2a: the token component is the venue-neutral [`MarketId`] (decimal-`U256`
/// string for intl) rather than a raw `U256`, so the canonical position key is
/// venue-agnostic.
/// Identity of a position: which squadron, which viper, which token.
///
/// The squadron component is what allows two squadrons of the same class to
/// trade the same market. Without it the key was `(strategy, token)`, so a
/// second crypto squadron running Maker on the market the first was already
/// quoting would address the *same* entry — each one reading the other's size
/// as its own, and whichever exited first taking the whole position with it.
/// That is why deploying one was refused outright rather than merely discouraged.
///
/// A struct rather than a tuple deliberately: the first two fields are both
/// `String`, and a transposed `(strategy, squadron)` would compile, run, and
/// simply never find a position — a viper that cannot see its own fill re-enters
/// instead of exiting.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PositionKey {
    /// Squadron that owns this position (`btc-open`, `politics-open`, …).
    pub squadron: String,
    /// Viper that opened it (`MakerStrategy`, `ArbitrageStrategy`, …).
    pub strategy: String,
    /// The YES or NO token held.
    pub market: MarketId,
}

impl PositionKey {
    pub fn new(squadron: impl Into<String>, strategy: impl Into<String>, market: MarketId) -> Self {
        Self { squadron: squadron.into(), strategy: strategy.into(), market }
    }
}

/// Shared positions state accessible by all strategies.
///
/// Keyed by (squadron, strategy, token) so that MomentumStrategy and
/// MakerStrategy can both hold YES simultaneously without colliding, and so can
/// two squadrons running the same viper on the same market.
/// Typically wrapped in Arc<Mutex<>> for concurrent access.
pub type PositionMap = HashMap<PositionKey, Position>;

impl MarketSnapshot {
    /// Is there a real seller on the YES leg?
    ///
    /// A binary outcome settles at $1.00, so nobody offers at or above that —
    /// an ask of $1.00 is the venue layer saying "no offer", not a price. Both
    /// Kalshi and Polymarket US fill an absent ask in this way deliberately: it
    /// is the least attractive value expressible, so a leg with no seller looks
    /// unappealing to every gate rather than irresistible.
    ///
    /// Strategies that reason about the ask — spread width, arbitrage cost,
    /// rehedge price — must ask this first. The alternative is computing a
    /// spread or a combined cost against a price that does not exist.
    pub fn yes_has_ask(&self) -> bool {
        self.yes_ask < Decimal::ONE
    }

    /// Is there a real seller on the NO leg? See [`Self::yes_has_ask`].
    pub fn no_has_ask(&self) -> bool {
        self.no_ask < Decimal::ONE
    }

    /// Depth pair to gate on for the YES side: `(bid, ask)`.
    ///
    /// `whole_book` selects the whole-book totals over the touch. See
    /// `DynamicConfig::obi_use_whole_book` for why this is a choice rather than
    /// a fixed answer.
    pub fn yes_depths(&self, whole_book: bool) -> (Decimal, Decimal) {
        if whole_book {
            (self.yes_bid_depth_total, self.yes_ask_depth_total)
        } else {
            (self.yes_bid_depth, self.yes_ask_depth)
        }
    }

    /// Depth pair to gate on for the NO side: `(bid, ask)`.
    pub fn no_depths(&self, whole_book: bool) -> (Decimal, Decimal) {
        if whole_book {
            (self.no_bid_depth_total, self.no_ask_depth_total)
        } else {
            (self.no_bid_depth, self.no_ask_depth)
        }
    }

    /// Order-book imbalance for the YES side, in `[-1, 1]`; positive means more
    /// size resting on the bid.
    ///
    /// Returns -1 when there is no depth at all, which every caller treats as
    /// maximally adverse and therefore blocks entry. That conflation of "no
    /// data" with "toxic flow" is deliberate and long-standing: ghost-OBI trades
    /// in the 2026-05-07 session took losses on ticks whose depth was missing
    /// while the heartbeat showed -0.76 to -0.96.
    pub fn yes_obi(&self, whole_book: bool) -> Decimal {
        let (bid, ask) = self.yes_depths(whole_book);
        Self::obi(bid, ask)
    }

    /// Order-book imbalance for the NO side. See [`yes_obi`](Self::yes_obi).
    pub fn no_obi(&self, whole_book: bool) -> Decimal {
        let (bid, ask) = self.no_depths(whole_book);
        Self::obi(bid, ask)
    }

    fn obi(bid: Decimal, ask: Decimal) -> Decimal {
        let total = bid + ask;
        if total > Decimal::ZERO { (bid - ask) / total } else { -Decimal::ONE }
    }
}

/// Key for the Control Tower's "which market is this viper working" map.
///
/// Scoped by squadron because every venue now runs more than one at a time —
/// two US wings, three Kalshi squadrons — and they share the venue-agnostic
/// viper kinds. Keyed by kind alone, the last squadron to publish overwrote the
/// rest, so a squadron page showed one market in its header and a different one
/// on its viper cards.
pub fn strategy_market_key(squadron_id: &str, viper_kind: &str) -> String {
    format!("{squadron_id}:{viper_kind}")
}

/// Current market data snapshot.
/// Used for broadcasting to strategies.
#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    /// YES token best bid price
    pub yes_bid: Decimal,
    /// YES token bid-side depth (shares available at best bid)
    pub yes_bid_depth: Decimal,
    /// YES token best ask price
    pub yes_ask: Decimal,
    /// YES token ask-side depth (shares available at best ask)
    pub yes_ask_depth: Decimal,
    /// NO token best bid price
    pub no_bid: Decimal,
    /// NO token bid-side depth (shares available at best bid)
    pub no_bid_depth: Decimal,
    /// NO token best ask price
    pub no_ask: Decimal,
    /// NO token ask-side depth (shares available at best ask)
    pub no_ask_depth: Decimal,

    // ── Whole-book depth ────────────────────────────────────────────────────
    //
    // The four fields above are the TOUCH — size resting at the best price only.
    // On a thin book that is one or two contracts, which makes any ratio built
    // from them extremely noisy: measured on Kalshi, top-of-book imbalance
    // disagreed in sign with the whole-book ratio 41% of the time, and the
    // Maker's taker-sweep detector read "99% of bid depth drained" when a single
    // contract was lifted.
    //
    // These carry the sum across every published level so a strategy can choose.
    // Nothing reads them yet — they are plumbed first, deliberately without
    // behavioural change, so the two series can be compared on live books before
    // any threshold moves.
    /// Total size across every published YES bid level.
    pub yes_bid_depth_total: Decimal,
    /// Total size across every published YES ask level.
    pub yes_ask_depth_total: Decimal,
    /// Total size across every published NO bid level.
    pub no_bid_depth_total: Decimal,
    /// Total size across every published NO ask level.
    pub no_ask_depth_total: Decimal,

    /// Current oracle price from Binance
    pub oracle_price: Decimal,
    /// Price velocity over the primary window (MOMENTUM_WINDOW_SECS = 5s)
    pub velocity: Decimal,
    /// Price velocity over the short window (1s) — confirms move is still happening NOW
    pub velocity_1s: Decimal,
    /// Velocity rate-of-change: velocity_now - velocity_prev_tick
    /// Positive = momentum building, negative = momentum fading
    pub acceleration: Decimal,
    /// Binance perpetual futures funding rate (from /fapi/v1/premiumIndex).
    /// Negative = shorts paying longs (bearish bias from smart money).
    /// Positive = longs paying shorts (bullish bias from smart money).
    /// Updated every ~60 seconds; zero if unavailable.
    pub funding_rate: Decimal,
    /// Institutional Pulse from the Tide Raptor — volume-weighted z-score of the
    /// spot-BTC-ETF (IBIT/FBTC/ARKB) premium/discount vs a synthetic iNAV.
    /// Positive = institutions paying a premium (bullish), negative = discount (bearish).
    /// BTC-only and US-market-hours-only: zero for ETH/SOL squadrons, outside US
    /// session, or when the Tide Raptor is not deployed.
    pub institutional_pulse: Decimal,
    /// Tide coherence in [0, 1] — agreement across the three ETF premiums.
    /// High coherence + large |pulse| = institutional conviction. Zero when the
    /// Tide Raptor is absent/dormant (same gating as `institutional_pulse`).
    pub tide_coherence: Decimal,
    /// TradFi velocity from the Horizon Raptor — volume-weighted 5-second momentum
    /// of SPY+QQQ in USD. Positive = risk-on front-running, negative = risk-off.
    /// Zero outside US market hours or when the Horizon Raptor is absent.
    pub tradfi_velocity: Decimal,
    /// Macro coherence from the Horizon Raptor — 10-minute rolling Pearson
    /// correlation of QQQ velocity vs BTC velocity in [-1, 1]. High = BTC trading
    /// as a high-beta tech asset (TradFi signals are informative); ~0 = decoupled
    /// regime (treat TradFi signals as noise). Zero when insufficient history.
    pub macro_coherence: Decimal,
    /// VIX proxy from the Horizon Raptor — UVXY last trade price. Higher = more
    /// fear/volatility. Zero outside US hours / when the raptor is absent.
    pub vix_proxy: Decimal,
    /// 5-second rate of change of the VIX proxy (UVXY velocity). A sharp positive
    /// spike signals panic onset — market-making vipers should stop quoting.
    /// Zero outside US hours / when the raptor is absent.
    pub vix_velocity: Decimal,
    /// Fractional change in Binance perp open interest since the previous poll
    /// (Derivatives Raptor). >0 = positioning building, <0 = unwinding/de-leveraging.
    /// Zero when the Derivatives Raptor is absent or on its first poll. All-asset.
    pub oi_delta_pct: Decimal,
    /// Taker buy÷sell volume ratio from the Derivatives Raptor. >1 = buyers lifting
    /// offers (bullish aggression), <1 = sellers hitting bids (bearish aggression),
    /// 0 = no data (FAPI unreachable). Treated as neutral when zero.
    pub cvd_ratio: Decimal,
    /// 60-minute oracle price drift (current_price − price_60_minutes_ago).
    /// Positive = BTC trending UP over the last hour.
    /// Negative = BTC trending DOWN over the last hour.
    /// Zero when insufficient history is available (first hour of bot runtime).
    /// Used by MakerStrategy to suppress adverse-side bids during slow sustained trends.
    pub oracle_drift_60m: Decimal,
    /// 10-minute oracle price drift (current_price − price_10_minutes_ago).
    /// Fills the temporal gap between the 5s velocity window and the 60m drift.
    /// Captures the medium-term directional move where profitable binary trades develop.
    /// Zero when fewer than 10 minutes of price history are available.
    pub oracle_drift_10m: Decimal,
    /// Canonical 60-minute realized volatility of the oracle (Binance) price, from the
    /// Price raptor's `normalized_hist_vol` over a proper time-spaced 60-min window.
    /// Normalized to [0, 1] where 1.0 = 2%-per-tick log-return std-dev. ~0.0025–0.0035
    /// for normal BTC; ~0.0 when the oracle is frozen. Consumed by GBoost's flatness
    /// gate (and its `hist_vol_regime` feature) instead of recomputing from the
    /// 50ms-cadence snapshot buffer, which oversamples duplicate prints and collapses
    /// to 0. Zero until the raptor has ≥5 samples.
    pub hist_vol: Decimal,
    /// Seconds remaining until this market's expiry at the time of snapshot creation.
    /// Negative if market has already expired.  Zero when close_time is unknown.
    /// Used by GBoost as a direct feature: binary market microstructure changes
    /// dramatically near expiry (gamma explosion, spread widening, adverse selection)
    /// and the model should learn these dynamics from data rather than via hard-coded gates.
    pub secs_to_expiry: i64,
    /// Timestamp of this snapshot
    pub timestamp: DateTime<Utc>,
}

/// Market identifiers and metadata.
#[derive(Debug, Clone)]
pub struct MarketConfig {
    /// YES token ID (venue-neutral)
    pub yes_token: MarketId,
    /// NO token ID (venue-neutral)
    pub no_token: MarketId,
    /// Human-readable market name
    pub market_name: String,
    /// Market close/expiry time
    pub market_close_time: Option<DateTime<Utc>>,
    /// Strike price (if applicable)
    pub strike_price: Option<Decimal>,
    /// Whether the market uses negative risk pricing
    pub is_neg_risk: bool,
    /// Polymarket condition ID (bytes32 hex) — required for on-chain merge operations.
    /// Empty string when not available (non-maker markets).
    pub condition_id: String,
    /// YES token fee rate in basis points
    pub yes_fee_bps: u32,
    /// NO token fee rate in basis points
    pub no_fee_bps: u32,
}

/// Lifecycle phase of a market derived from its close time.
///
/// **Venue-neutral core**: both the intl patrol and the US loop drive the same
/// close/wind-down/stand-down semantics off this single classifier, so neither
/// venue re-implements "is the market closing?" logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketPhase {
    /// Trading normally — opening new positions is allowed.
    Open,
    /// Inside the wind-down window — stop opening new positions and let existing
    /// ones resolve/exit (the squadron's RTB state).
    WindingDown,
    /// At or past close — stand down and rotate to the next market.
    Closed,
}

impl MarketConfig {
    /// Seconds until the market closes (negative if already past). `None` when
    /// the market has no close time (e.g. always-open markets that never rotate).
    pub fn secs_to_close(&self, now: DateTime<Utc>) -> Option<i64> {
        self.market_close_time.map(|c| (c - now).num_seconds())
    }

    /// Classify the market's lifecycle [`MarketPhase`]. `rtb_window_secs` is how
    /// long before close to stop opening new positions. Markets with no close
    /// time are always [`MarketPhase::Open`].
    pub fn phase(&self, now: DateTime<Utc>, rtb_window_secs: i64) -> MarketPhase {
        match self.secs_to_close(now) {
            None                            => MarketPhase::Open,
            Some(s) if s <= 0               => MarketPhase::Closed,
            Some(s) if s <= rtb_window_secs => MarketPhase::WindingDown,
            Some(_)                         => MarketPhase::Open,
        }
    }
}

/// Strategy execution status for monitoring and lifecycle management.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyStatus {
    /// Strategy is active and evaluating
    Active,
    /// Strategy is disabled (e.g., no signal, cooldown)
    Disabled,
    /// Strategy encountered an error
    Error,
}

/// Parameters required to place an order on the CLOB.
#[derive(Debug, Clone)]
pub struct OrderParams {
    pub token_id: MarketId,
    pub price: Decimal,
    pub shares: Decimal,
    pub fee_bps: u16,
    pub is_neg_risk: bool,
    pub market_name: String,
    pub condition_id: String,
    pub order_type: TimeInForce,
    pub post_only: bool,
    pub ghost_mode: bool, // Added this field
}

/// Signals returned by strategies for the orchestrator to act upon.
#[derive(Debug, Clone)]
pub enum StrategySignal {
    /// Entry signal with all metadata. For paired strategies, this is the primary leg.
    Entry {
        params: OrderParams,
        /// If Some, the strategy also wants to buy this second leg (Arbitrage/TimeDecay).
        pair_params: Option<OrderParams>,
    },
    /// Two-sided maker quote with metadata.
    MakerQuote {
        yes: Option<OrderParams>,
        no: Option<OrderParams>,
    },
    /// Reactive quote-pull: cancel resting UNFILLED maker quotes on these tokens
    /// when the book turns toxic before they fill (avoids adverse-selection fills).
    MakerCancel {
        tokens: Vec<MarketId>,
    },
    /// Resting maker exit: post (or reprice) a post-only GTC **sell** for an
    /// already-filled maker position, so the position leaves by being LIFTED at
    /// the ask instead of by crossing back to the bid.
    ///
    /// This is the difference between market-making and paying the spread twice.
    /// Every other exit path (`Exit`) sells at the bid with a FAK, which is
    /// correct for a stop but throws away the entire spread on a normal
    /// profit-taking exit. The consumer treats this as idempotent: it places the
    /// ask if none rests, reprices if the book has moved beyond the reprice
    /// threshold, and otherwise does nothing — so a strategy may safely re-emit
    /// it on every tick.
    MakerRestingExit {
        params: OrderParams,
        reason: String,
    },
    /// Exit signal with metadata.
    Exit {
        params: OrderParams,
        reason: String,
        /// If true, also exit the other leg of a paired position.
        exit_pair: bool,
    },
    /// No action at this time
    NoSignal,
}

impl StrategySignal {
    /// Would acting on this open or increase exposure?
    ///
    /// Lets a loop keep managing what it already holds while refusing new risk —
    /// during the run-up to a market close, and after a primary market has
    /// closed while inventory remains on a secondary that has not. Cancels and
    /// exits reduce exposure and must keep flowing in both cases; a position
    /// nobody is allowed to sell is a position nobody is managing.
    pub fn opens_exposure(&self) -> bool {
        matches!(self, Self::Entry { .. } | Self::MakerQuote { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    fn market_closing_in(secs: Option<i64>) -> MarketConfig {
        MarketConfig {
            yes_token: MarketId::new("yes"),
            no_token: MarketId::new("no"),
            market_name: "t".into(),
            market_close_time: secs.map(|s| Utc::now() + ChronoDuration::seconds(s)),
            strike_price: None,
            is_neg_risk: false,
            condition_id: String::new(),
            yes_fee_bps: 0,
            no_fee_bps: 0,
        }
    }

    #[test]
    fn phase_classifies_open_winddown_closed() {
        let now = Utc::now();
        // No close time → always Open.
        assert_eq!(market_closing_in(None).phase(now, 120), MarketPhase::Open);
        // Plenty of time → Open.
        assert_eq!(market_closing_in(Some(600)).phase(now, 120), MarketPhase::Open);
        // Inside the wind-down window → WindingDown.
        assert_eq!(market_closing_in(Some(60)).phase(now, 120), MarketPhase::WindingDown);
        // Past close → Closed.
        assert_eq!(market_closing_in(Some(-5)).phase(now, 120), MarketPhase::Closed);
    }
}


#[cfg(test)]
mod price_state_tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn ps(bid_touch: Decimal, ask_touch: Decimal, bid_all: Decimal, ask_all: Decimal) -> PriceState {
        (dec!(0.50), bid_touch, dec!(0.51), ask_touch, Utc::now(), bid_all, ask_all)
    }

    /// The historical top-of-book ratio must be reproduced exactly, or every
    /// existing threshold silently changes meaning.
    #[test]
    fn touch_imbalance_matches_the_historical_formula() {
        let p = ps(dec!(30), dec!(70), dec!(500), dec!(500));
        let expected = (dec!(30) - dec!(70)) / (dec!(30) + dec!(70));
        assert_eq!(price_state::imbalance(&p, false), expected);
    }

    /// The whole-book view can disagree sharply with the touch — that disagreement
    /// is the entire reason for collecting it.
    #[test]
    fn a_single_resting_order_can_invert_the_signal() {
        // One large offer at the touch reads as heavy selling; across the book the
        // two sides are balanced.
        let p = ps(dec!(10), dec!(200), dec!(1000), dec!(1000));
        assert!(price_state::imbalance(&p, false) < dec!(-0.9), "touch says toxic");
        assert_eq!(price_state::imbalance(&p, true), dec!(0), "book says balanced");
    }

    /// An empty book returns -1 on both measures. Callers read that as maximally
    /// adverse, which is the safe direction — but it means "no data" and "toxic
    /// flow" are indistinguishable, and that is worth knowing.
    #[test]
    fn an_empty_book_reads_as_maximally_adverse_on_both() {
        let p = ps(dec!(0), dec!(0), dec!(0), dec!(0));
        assert_eq!(price_state::imbalance(&p, false), dec!(-1));
        assert_eq!(price_state::imbalance(&p, true), dec!(-1));
    }

    /// Cumulative depth is never less than the touch it contains.
    #[test]
    fn cumulative_depth_contains_the_touch() {
        let p = ps(dec!(30), dec!(70), dec!(500), dec!(400));
        assert!(price_state::bid_depth_total(&p) >= price_state::bid_touch_size(&p));
        assert!(price_state::ask_depth_total(&p) >= price_state::ask_touch_size(&p));
    }

    /// Accessors must not drift from the tuple layout.
    #[test]
    fn accessors_map_to_the_documented_positions() {
        let p = ps(dec!(1), dec!(2), dec!(3), dec!(4));
        assert_eq!(price_state::best_bid(&p), dec!(0.50));
        assert_eq!(price_state::best_ask(&p), dec!(0.51));
        assert_eq!(price_state::bid_touch_size(&p), dec!(1));
        assert_eq!(price_state::ask_touch_size(&p), dec!(2));
        assert_eq!(price_state::bid_depth_total(&p), dec!(3));
        assert_eq!(price_state::ask_depth_total(&p), dec!(4));
    }
}

#[cfg(test)]
mod ask_presence_tests {
    use super::MarketSnapshot;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    /// A book with an ordinary two-sided market on both legs.
    fn snapshot(yes_ask: Decimal, no_ask: Decimal) -> MarketSnapshot {
        MarketSnapshot {
            yes_bid: dec!(0.40), yes_bid_depth: dec!(100),
            yes_ask, yes_ask_depth: dec!(100),
            no_bid: dec!(0.55), no_bid_depth: dec!(100),
            no_ask, no_ask_depth: dec!(100),
            yes_bid_depth_total: dec!(100), yes_ask_depth_total: dec!(100),
            no_bid_depth_total: dec!(100), no_ask_depth_total: dec!(100),
            oracle_price: dec!(0), velocity: dec!(0), velocity_1s: dec!(0),
            acceleration: dec!(0), funding_rate: dec!(0),
            oracle_drift_60m: dec!(0), oracle_drift_10m: dec!(0), hist_vol: dec!(0),
            institutional_pulse: dec!(0), tide_coherence: dec!(0),
            tradfi_velocity: dec!(0), macro_coherence: dec!(0),
            vix_proxy: dec!(0), vix_velocity: dec!(0),
            oi_delta_pct: dec!(0), cvd_ratio: dec!(0),
            secs_to_expiry: 3600,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn a_real_offer_counts_as_an_ask() {
        let s = snapshot(dec!(0.45), dec!(0.60));
        assert!(s.yes_has_ask());
        assert!(s.no_has_ask());
    }

    /// The venue layer fills an absent ask in at the settlement value, so $1.00
    /// means "nobody is selling", not "someone is selling at a dollar".
    #[test]
    fn the_payout_price_means_no_offer() {
        let s = snapshot(dec!(0.01), Decimal::ONE);
        assert!(s.yes_has_ask());
        assert!(!s.no_has_ask(), "$1.00 is the no-offer sentinel, not a price");
    }

    /// The exact shape seen on Kalshi's KXLAKECONF politics market: YES offered
    /// at a penny, no NO sellers at all. Read naively the two asks sum to $0.01
    /// for a $1.00 payout, which is why this must be recognised as an absent
    /// leg rather than priced.
    #[test]
    fn decided_market_with_one_empty_leg() {
        let s = snapshot(dec!(0.01), Decimal::ONE);
        assert!(!(s.yes_has_ask() && s.no_has_ask()),
            "a leg with no seller must not present as a tradeable pair");
    }
}

#[cfg(test)]
mod position_key_tests {
    use super::{PositionKey, PositionMap, Position};
    use crate::venues::core::MarketId;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn position(shares: rust_decimal::Decimal) -> Position {
        Position {
            shares,
            avg_entry: dec!(0.50),
            opened_at: Utc::now(),
            close_time: None,
            market_name: "BTC above $77,000".to_string(),
            pair_token_id: MarketId::new("pair"),
            fill_confirmed_at: None,
            paired_leg_token_id: None,
            entry_fee: dec!(0),
        }
    }

    /// The reason this change exists. Two crypto squadrons quoting the same
    /// market with the same viper each keep their own position; before the
    /// squadron was part of the key the second insert overwrote the first, so
    /// one squadron read the other's size as its own and whichever exited first
    /// took the whole thing.
    #[test]
    fn two_squadrons_hold_the_same_market_independently() {
        let market = MarketId::new("tok-yes");
        let mut map: PositionMap = PositionMap::new();
        map.insert(PositionKey::new("btc-open", "MakerStrategy", market.clone()), position(dec!(10)));
        map.insert(PositionKey::new("btc-15m", "MakerStrategy", market.clone()), position(dec!(25)));

        assert_eq!(map.len(), 2, "one squadron's position displaced the other's");
        assert_eq!(map[&PositionKey::new("btc-open", "MakerStrategy", market.clone())].shares, dec!(10));
        assert_eq!(map[&PositionKey::new("btc-15m", "MakerStrategy", market)].shares, dec!(25));
    }

    /// The property that already held and must survive: two vipers in ONE
    /// squadron can hold opposing sides of the same market on separate budgets.
    #[test]
    fn two_vipers_in_one_squadron_still_hold_the_same_market() {
        let market = MarketId::new("tok-yes");
        let mut map: PositionMap = PositionMap::new();
        map.insert(PositionKey::new("btc-open", "MakerStrategy", market.clone()), position(dec!(10)));
        map.insert(PositionKey::new("btc-open", "ArbitrageStrategy", market.clone()), position(dec!(7)));
        assert_eq!(map.len(), 2);
    }

    /// One squadron exiting must not remove another's position on that token —
    /// the failure that made a second squadron unsafe rather than merely untidy.
    #[test]
    fn exiting_one_squadron_leaves_the_other_holding() {
        let market = MarketId::new("tok-yes");
        let mut map: PositionMap = PositionMap::new();
        map.insert(PositionKey::new("btc-open", "MakerStrategy", market.clone()), position(dec!(10)));
        map.insert(PositionKey::new("btc-15m", "MakerStrategy", market.clone()), position(dec!(25)));

        map.remove(&PositionKey::new("btc-open", "MakerStrategy", market.clone()));

        assert_eq!(map.len(), 1);
        assert_eq!(map[&PositionKey::new("btc-15m", "MakerStrategy", market)].shares, dec!(25));
    }

    /// Squadron and strategy are both plain strings and adjacent in the
    /// constructor, so a transposed call would compile. It must not silently
    /// resolve to the same entry — a viper that cannot find its own position
    /// re-enters instead of exiting.
    #[test]
    fn transposing_squadron_and_strategy_is_a_different_key() {
        let market = MarketId::new("tok-yes");
        let right = PositionKey::new("btc-open", "MakerStrategy", market.clone());
        let wrong = PositionKey::new("MakerStrategy", "btc-open", market);
        assert_ne!(right, wrong);
    }

    /// Exposure is summed per squadron, which is what gives each its own budget
    /// rather than one shared pool across every squadron running that viper.
    #[test]
    fn exposure_sums_per_squadron() {
        let mut map: PositionMap = PositionMap::new();
        map.insert(PositionKey::new("btc-open", "MakerStrategy", MarketId::new("a")), position(dec!(10)));
        map.insert(PositionKey::new("btc-open", "MakerStrategy", MarketId::new("b")), position(dec!(20)));
        map.insert(PositionKey::new("btc-15m", "MakerStrategy", MarketId::new("c")), position(dec!(99)));

        let mine: rust_decimal::Decimal = map.iter()
            .filter(|(k, _)| k.strategy == "MakerStrategy" && k.squadron == "btc-open")
            .map(|(_, p)| p.shares)
            .sum();
        assert_eq!(mine, dec!(30), "another squadron's size leaked into this budget");
    }
}
