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

// ── Compile-time venue selection (D3) ────────────────────────────────────────

/// The concrete venue this binary was compiled for. Resolves to a single type
/// so all call sites monomorphise to static dispatch with zero vtable cost.
#[cfg(feature = "intl_clob")]
pub type ActiveVenue = crate::venues::intl::IntlClobVenue;

#[cfg(feature = "us_retail")]
pub type ActiveVenue = crate::venues::us::UsRetailVenue;

#[cfg(all(feature = "intl_clob", feature = "us_retail"))]
compile_error!("Pick exactly one venue: intl_clob OR us_retail");

#[cfg(not(any(feature = "intl_clob", feature = "us_retail")))]
compile_error!("Pick a venue: --features intl_clob | us_retail");

