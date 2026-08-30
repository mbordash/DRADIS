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
/// US Retail charges no taker fee (`yes_fee_bps: 0` throughout), so this is zero
/// there and the floor becomes inert rather than wrong.
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

/// The venue's quadratic taker-fee coefficient.
#[cfg(feature = "intl_clob")]
fn taker_fee_rate() -> Decimal { crate::venues::intl::live_taker_fee_rate() }

/// Kalshi quotes the same quadratic schedule as a per-contract ceiling of 1.75¢ at
/// P=0.5 (`KALSHI_FEE_BPS`), which is exactly `rate/4` — so the coefficient is 0.07.
#[cfg(feature = "kalshi")]
fn taker_fee_rate() -> Decimal { dec!(0.07) }

/// US Retail takes no taker fee.
#[cfg(feature = "us_retail")]
fn taker_fee_rate() -> Decimal { Decimal::ZERO }

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

