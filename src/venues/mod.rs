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

//! Venue abstraction — compile-time selection of exactly one trading venue.
//!
//! See `docs/VENUE_ABSTRACTION.md`. One venue per binary, chosen by Cargo
//! feature, dispatched statically (no `dyn`) via the [`ActiveVenue`] alias.

pub mod core;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;

/// Round-trip taker cost, expressed as a fraction of the entry notional.
///
/// Both quadratic-fee venues (Polymarket intl, Kalshi) charge `rate × p × (1 − p)`
/// per share on **each** leg of a round trip. Approximating the exit price by the
/// entry price — which is where it sits at the moment a profit target is being
/// set — the round trip costs `2 × rate × p × (1 − p)` per share against an entry
/// notional of `p` per share, i.e. **`2 × rate × (1 − p)`** of notional.
///
/// The price dependence is the point: at `p = $0.20` the round trip eats 11.2% of
/// notional, at `p = $0.80` only 2.8%. Any strategy holding a *flat* percentage
/// take-profit target is therefore below break-even across part of its permitted
/// entry range, and silently so — the trade closes "at target" and still loses
/// money. Callers should floor their target against this.
///
/// Polymarket US charges the same quadratic schedule at 0.06 (`us_taker_fee_rate`),
/// so the floor binds there too. It was carried as zero until 2026-09-09, which
/// made every floor and gate built on this function inert on that venue.
pub fn round_trip_fee_pct(entry_price: Decimal) -> Decimal {
    if entry_price <= Decimal::ZERO || entry_price >= Decimal::ONE { return Decimal::ZERO; }
    dec!(2) * taker_fee_rate() * (Decimal::ONE - entry_price)
}

/// Single-leg taker cost, as a fraction of the entry notional.
///
/// The sibling of [`round_trip_fee_pct`] for strategies that only pay a fee on
/// ONE leg. A post-only maker quote is never charged a taker fee by the CLOB —
/// only the FAK that closes it is — so charging Maker the round-trip figure
/// would overstate its cost by exactly 2× and push its take-profit target to
/// roughly double what the trade actually has to clear.
///
/// Same approximation as the round trip: the exit price is taken at the entry
/// price, which is where it sits when a profit target is being set, giving
/// `rate × (1 − p)` of notional.
pub fn exit_only_fee_pct(entry_price: Decimal) -> Decimal {
    if entry_price <= Decimal::ZERO || entry_price >= Decimal::ONE { return Decimal::ZERO; }
    taker_fee_rate() * (Decimal::ONE - entry_price)
}

/// The taker fee on ONE leg that crossed the spread to open a position, as a
/// fraction of the entry notional.
///
/// Numerically the same figure as [`exit_only_fee_pct`] — the schedule is
/// symmetric and both are evaluated at the entry price — but named for the leg
/// it describes. A taker entry that leaves by a resting post-only ask pays this
/// and nothing else: the lift is free, so the only toll a resting take-profit
/// has to clear is the one already paid to get in. Flooring that target against
/// the round trip would put the ask a full leg higher than the trade needs.
pub fn entry_only_fee_pct(entry_price: Decimal) -> Decimal {
    exit_only_fee_pct(entry_price)
}

/// The taker fee charged per share for one fill at `price`, in dollars.
///
/// `rate × p × (1 − p)` on the quadratic venues, zero where no taker fee is
/// charged, and zero outside the open interval — a contract at $0 or $1 has
/// resolved and no fee applies. For an exit that is the toll a FAK at the bid
/// actually pays, which is what an exit rule must net out before it can call
/// a sale a profit.
pub fn taker_fee_per_share(price: Decimal) -> Decimal {
    if price <= Decimal::ZERO || price >= Decimal::ONE { return Decimal::ZERO; }
    taker_fee_rate() * price * (Decimal::ONE - price)
}

/// Single-leg taker cost as a fraction of ENTRY notional, when the exit happens
/// at a given gain above the entry price.
///
/// [`exit_only_fee_pct`] approximates the exit price by the entry price, which is
/// where it sits when a target is first being set. That approximation is not
/// neutral: the fee is actually charged at the exit, and on a quadratic schedule
/// a higher exit price costs MORE on any contract below ~$0.50. The error is
/// `rate · g · (1 − p(2 + g))`, positive across the whole of Maker's $0.10–$0.48
/// entry band — so a target floored on the entry-price figure alone can still
/// book a small net loss at the bottom of the band.
///
/// Returns zero for a gain that would carry the exit to $1.00 or beyond, where
/// the contract has resolved and no taker fee is charged.
pub fn exit_fee_pct_at_gain(entry_price: Decimal, gain: Decimal) -> Decimal {
    if entry_price <= Decimal::ZERO || entry_price >= Decimal::ONE { return Decimal::ZERO; }
    let exit_price = entry_price * (Decimal::ONE + gain);
    if exit_price <= Decimal::ZERO || exit_price >= Decimal::ONE { return Decimal::ZERO; }
    // rate · p_exit · (1 − p_exit) per share, over an entry notional of p_entry.
    taker_fee_rate() * exit_price * (Decimal::ONE - exit_price) / entry_price
}

/// The most a taker can be charged per share under a quadratic schedule, in
/// basis points of $1.
///
/// `rate × p × (1 − p)` peaks at `p = 0.5`, so the ceiling is `rate / 4`. This is
/// the unit `MarketConfig::{yes,no}_fee_bps` carries wherever it is built from a
/// published schedule rather than the CLOB's legacy `/fee-rate` figure: Kalshi's
/// 0.07 is the 1.75¢ (175 bps) per-contract ceiling its trader has always used,
/// and Polymarket International's published sports rate of 0.05 becomes 125 bps.
///
/// A flat per-share bound is what the Arbitrage early-exit gate needs. It has to
/// know the MOST a FAK exit can cost before it can prefer that exit to settling
/// at $1.00 for free, and the ceiling is the only flat figure that is never an
/// underestimate. Zero for a zero or negative rate: a venue that charges nothing
/// has a ceiling of nothing, and a negative "fee" is not a schedule.
pub fn taker_fee_ceiling_bps(rate: Decimal) -> u32 {
    if rate <= Decimal::ZERO { return 0; }
    (rate / dec!(4) * dec!(10000)).round().to_u32().unwrap_or(0)
}

/// The per-share fee ceiling a market's `MarketConfig` carries, in basis points,
/// from the market's own published schedule or the venue-wide rate.
///
/// A published rate is used as it stands, including a published zero. Anything
/// else falls back to the venue-wide taker rate — the same knob every other
/// fee-aware gate on the squadron (Maker's floor, Momentum's and Convergence's
/// fee-dominance check) already reads — so the two fee channels on a squadron
/// agree rather than a third number appearing. That fallback overstates the fee
/// on a market the venue does not charge for, which only makes the Arbitrage
/// early exit less eager and settlement the preferred close; it never
/// understates it. Defaulting to zero was the defect this replaces, on both the
/// Polymarket International event-market path (Gamma's `feeSchedule`) and the
/// Polymarket US path (the gateway's `feeCoefficient`), and a zero the venue did
/// not actually publish must not come back through a renamed field or a
/// dropped one.
///
/// Returns the ceiling (see [`taker_fee_ceiling_bps`]) and `true` when the
/// market's own schedule supplied it.
pub fn published_or_venue_fee_bps(published: Option<Decimal>, venue_wide_rate: Decimal) -> (u32, bool) {
    match published {
        Some(rate) => (taker_fee_ceiling_bps(rate), true),
        None => (taker_fee_ceiling_bps(venue_wide_rate), false),
    }
}

/// The venue's quadratic taker-fee coefficient.
#[cfg(feature = "intl_clob")]
pub fn taker_fee_rate() -> Decimal { crate::venues::intl::live_taker_fee_rate() }

/// Kalshi quotes the same quadratic schedule as a per-contract ceiling of 1.75¢ at
/// P=0.5 (`KALSHI_FEE_BPS`), which is exactly `rate/4` — so the coefficient is 0.07.
#[cfg(feature = "kalshi")]
pub fn taker_fee_rate() -> Decimal { dec!(0.07) }

/// Polymarket US publishes the same quadratic schedule with Θ = 0.06 (taker) and
/// a −0.0125 maker rebate, per fill (docs.polymarket.us/fees). The gateway sends
/// the coefficient on every market record as `feeCoefficient`; this is the
/// venue-wide figure, read from the `us_taker_fee_rate` knob, and the trader
/// warns when a market publishes a different one.
///
/// Returned ZERO until 2026-09-09 on the belief that the venue charged no taker
/// fee, which switched off every fee-aware gate and floor on that build.
#[cfg(feature = "us_retail")]
pub fn taker_fee_rate() -> Decimal { crate::venues::us::live_taker_fee_rate() }

/// Cancel every resting order the VENUE reports, before trading begins.
///
/// A crashed or restarted session leaves its GTC orders working. Polymarket
/// International has swept them at startup since the beginning (`main.rs`), but
/// Kalshi and Polymarket US relied on `OrderLifecycle::cancel_all`, which drains
/// an in-memory tracked list — and that list is empty in a fresh process. So a
/// previous session's resting order survived the restart, could fill with nothing
/// watching it, and arrived later as a chain-adopted position with no entry of its
/// own: real money in a position no strategy had decided to hold.
///
/// Asks the venue what is actually open rather than trusting local state, which is
/// the whole point — local state is what was lost. A venue with no open-orders
/// surface returns an empty list and this is a no-op.
///
/// Failures are logged and never fatal. Refusing to start because a cancel failed
/// would leave the same orders working with no engine at all, which is strictly
/// worse than starting and reconciling.
/// `simulating` is passed by the caller (from `dynamic_config::ghosting_now()`)
/// rather than read from process globals in here, so the gate below — a shipped
/// v1.0.9 promise — is pinned by tests instead of depending on global state no
/// test can safely mutate.
pub async fn cancel_leftover_orders_at_startup<V: core::Execution + ?Sized>(
    venue: &V,
    simulating: bool,
) {
    // NEVER cancel while simulating.
    //
    // The sweep cannot tell its own leftovers from the account's other orders —
    // Kalshi lists the whole account with no filter by series, ticker or client
    // order id. A Kalshi or Polymarket US account is a RETAIL account that a
    // human also uses. (This comment used to add that the self-custody intl
    // wallet was fine "because nothing else trades it" — disproved in production
    // on 2026-09-01, when a ghost-mode restart ran the intl legacy sweep against
    // a wallet the operator also trades by hand. The intl paths now carry the
    // same gate: `squadron::cancel_all_orders_unless_simulating`.)
    //
    // So consider the AMI's default first-run posture: a customer connects their
    // personal account to evaluate DRADIS, ghost mode is on, the engine will never
    // place a real order — and the first thing it does is cancel every order they
    // placed by hand. Worse, it repeats on every watchdog restart. Simulating is a
    // promise not to touch the account, and cancelling is touching it.
    //
    // The cost of this gate is real and accepted: a leftover from a previous LIVE
    // session is not swept if the operator restarts into ghost. It is reported
    // instead, so the operator can act, and it is swept the moment they run live.
    if simulating {
        match venue.open_orders().await {
            Ok(open) if !open.is_empty() => tracing::warn!(
                "👻 Startup cancel skipped in ghost mode — {} resting order(s) on this account were LEFT ALONE. \
                 If any belong to a previous live DRADIS session, run live once to sweep them, or cancel them on the venue.",
                open.len(),
            ),
            _ => tracing::info!("👻 Startup cancel skipped — simulating, so the account is not touched"),
        }
        return;
    }

    let open = match venue.open_orders().await {
        Ok(o) => o,
        Err(e) => {
            // Says "unchecked", never "clean". A venue that cannot list its open
            // orders has not told us there are none.
            tracing::warn!("⚠️ Startup cancel SKIPPED — could not list open orders ({e}). \
                            Any order left working by a previous session is still live and \
                            unmanaged until its market is next traded.");
            return;
        }
    };
    if open.is_empty() {
        tracing::info!("✅ Startup cancel: no leftover orders from a previous session");
        return;
    }
    tracing::info!("🧹 Startup cancel: {} leftover order(s) from a previous session", open.len());
    let mut failed = 0usize;
    for ord in &open {
        if let Err(e) = venue.cancel(ord.order_id.clone()).await {
            failed += 1;
            tracing::warn!("⚠️ Startup cancel failed for {} ({}): {e}", ord.order_id, ord.market);
        }
    }
    if failed == 0 {
        tracing::info!("✅ Startup cancel complete ({} order(s))", open.len());
    } else {
        tracing::error!(
            "❌ Startup cancel: {}/{} order(s) could not be cancelled — they are still working on the venue",
            failed, open.len(),
        );
    }
}

/// Venue-neutral order lifecycle engine (Option C). Compiled for every venue;
/// US drives it today, intl migrates onto it next.
pub mod lifecycle;

/// Venue-neutral deployment-queue consumer, shared by every venue that accepts
/// operator-deployed squadrons.
pub mod deployment;

#[cfg(feature = "intl_clob")]
pub mod intl;

#[cfg(feature = "us_retail")]
pub mod us;

#[cfg(feature = "kalshi")]
pub mod kalshi;

// ── Compile-time venue selection (D3) ────────────────────────────────────────

/// The concrete venue this binary was compiled for. Resolves to a single type
/// so all call sites monomorphise to static dispatch with zero vtable cost.
#[cfg(feature = "intl_clob")]
pub type ActiveVenue = crate::venues::intl::IntlClobVenue;

#[cfg(feature = "us_retail")]
pub type ActiveVenue = crate::venues::us::UsRetailVenue;

#[cfg(feature = "kalshi")]
pub type ActiveVenue = crate::venues::kalshi::KalshiVenue;

#[cfg(any(
    all(feature = "intl_clob", feature = "us_retail"),
    all(feature = "intl_clob", feature = "kalshi"),
    all(feature = "us_retail", feature = "kalshi"),
))]
compile_error!("Pick exactly one venue: intl_clob OR us_retail OR kalshi");

#[cfg(not(any(feature = "intl_clob", feature = "us_retail", feature = "kalshi")))]
compile_error!("Pick a venue: --features intl_clob | us_retail | kalshi");


#[cfg(test)]
mod fee_ceiling_tests {
    use super::taker_fee_ceiling_bps;
    use rust_decimal_macros::dec;

    /// Kalshi's trader has always carried its 0.07 schedule as a 175 bps
    /// per-contract ceiling (`KALSHI_FEE_BPS`). The shared derivation must land
    /// on the same figure, or the two venues would describe the same schedule
    /// in two units.
    #[test]
    fn the_kalshi_ceiling_is_reproduced_from_its_rate() {
        assert_eq!(taker_fee_ceiling_bps(dec!(0.07)), 175);
    }

    /// Polymarket International's published sports rate (`feeSchedule.rate`
    /// 0.05 on every `sports_fees_v3` market, checked live 2026-09-09) becomes a
    /// 125 bps ceiling: 1.25¢ per share at the $0.50 peak.
    #[test]
    fn the_published_sports_rate_becomes_its_per_share_ceiling() {
        assert_eq!(taker_fee_ceiling_bps(dec!(0.05)), 125);
        assert_eq!(taker_fee_ceiling_bps(dec!(0.04)), 100);
    }

    /// No schedule, no ceiling — and a negative rate is not a schedule.
    #[test]
    fn a_free_market_has_a_zero_ceiling() {
        assert_eq!(taker_fee_ceiling_bps(dec!(0)), 0);
        assert_eq!(taker_fee_ceiling_bps(dec!(-0.05)), 0);
    }

    /// Polymarket US publishes 0.06 on every market (`feeCoefficient`, checked
    /// live 2026-09-09): a 150 bps ceiling, 1.5¢ a share at mid — the "$1.50
    /// per 100-lot at $0.50" on docs.polymarket.us/fees. A market that
    /// publishes nothing takes the venue-wide rate and says so; a published
    /// zero is a free market and stays zero rather than being "corrected".
    #[test]
    fn a_published_coefficient_wins_and_a_missing_one_falls_back_to_the_venue() {
        use super::published_or_venue_fee_bps;
        assert_eq!(published_or_venue_fee_bps(Some(dec!(0.06)), dec!(0.07)), (150, true));
        assert_eq!(published_or_venue_fee_bps(None, dec!(0.06)), (150, false));
        assert_eq!(published_or_venue_fee_bps(Some(dec!(0)), dec!(0.06)), (0, true));
    }
}

#[cfg(test)]
mod startup_sweep_gate_tests {
    use super::core::{Execution, Fill, OpenOrder, OrderId, OrderIntent, Position, Side, TimeInForce};
    use crate::venues::core::MarketId;
    use anyhow::Result;
    use async_trait::async_trait;
    use rust_decimal_macros::dec;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Venue double for the startup sweep: serves a fixed open-orders list and
    /// counts cancels. Everything the sweep must never touch panics, so a
    /// regression that starts placing or flattening from this path fails loudly
    /// rather than passing by accident.
    struct CountingVenue {
        open: Vec<OpenOrder>,
        cancels: AtomicUsize,
    }

    #[async_trait]
    impl Execution for CountingVenue {
        async fn place_order(&self, _: OrderIntent) -> Result<Fill> {
            unreachable!("the startup sweep must never place an order")
        }
        async fn place_atomic(&self, _: [OrderIntent; 2]) -> Result<[Fill; 2]> {
            unreachable!("the startup sweep must never place an order")
        }
        async fn cancel(&self, _: OrderId) -> Result<()> {
            self.cancels.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn collateral(&self) -> Result<rust_decimal::Decimal> {
            unreachable!("the startup sweep does not read collateral")
        }
        async fn positions(&self) -> Result<Vec<Position>> {
            unreachable!("the startup sweep does not read positions")
        }
        async fn open_orders(&self) -> Result<Vec<OpenOrder>> {
            Ok(self.open.clone())
        }
    }

    fn resting_order(id: &str) -> OpenOrder {
        OpenOrder {
            order_id: OrderId(id.to_string()),
            market: MarketId::new("mkt-1"),
            side: Side::Buy,
            price: dec!(0.42),
            original_qty: dec!(10),
            filled_qty: dec!(0),
            tif: TimeInForce::Gtc,
            pair_market: None,
        }
    }

    /// The v1.0.9 release-note promise, pinned: "Neither of those runs while
    /// simulating now." On 2026-09-01 at 21:52 a production ghost-mode restart
    /// issued a real account-wide cancel through the intl legacy path this
    /// sweep's gate never covered — the gate itself must stay exactly this
    /// strict: a simulating startup lists the account (reporting is allowed)
    /// but cancels NOTHING, however many orders are resting.
    #[tokio::test]
    async fn a_simulating_startup_reports_leftover_orders_but_cancels_none() {
        let venue = CountingVenue {
            open: vec![resting_order("ord-1"), resting_order("ord-2")],
            cancels: AtomicUsize::new(0),
        };
        super::cancel_leftover_orders_at_startup(&venue, true).await;
        assert_eq!(venue.cancels.load(Ordering::SeqCst), 0,
            "simulating is a promise not to touch the account");
    }

    /// The other half of the v1.0.9 decision must survive too: a LIVE startup
    /// still asks the venue what is open and sweeps every leftover, because a
    /// crashed session's GTC orders are exactly what local state cannot know.
    #[tokio::test]
    async fn a_live_startup_still_sweeps_every_leftover_order() {
        let venue = CountingVenue {
            open: vec![resting_order("ord-1"), resting_order("ord-2")],
            cancels: AtomicUsize::new(0),
        };
        super::cancel_leftover_orders_at_startup(&venue, false).await;
        assert_eq!(venue.cancels.load(Ordering::SeqCst), 2,
            "a live startup must sweep all leftovers the venue reports");
    }
}
