//! Venue abstraction — compile-time selection of exactly one trading venue.
//!
//! See `docs/VENUE_ABSTRACTION.md`. One venue per binary, chosen by Cargo
//! feature, dispatched statically (no `dyn`) via the [`ActiveVenue`] alias.

pub mod core;

/// Venue-neutral order lifecycle engine (Option C). Compiled for every venue;
/// US drives it today, intl migrates onto it next.
pub mod lifecycle;

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

