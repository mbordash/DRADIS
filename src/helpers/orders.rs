//! Order placement helpers — relocated to `venues::intl::orders`.
//!
//! The EIP-712 self-custody signing logic is venue-specific and now lives under
//! `venues::intl` (see `docs/VENUE_ABSTRACTION.md`, Step 1). This module re-exports
//! those symbols so existing call sites (`crate::helpers::orders::place_limit_order`,
//! `place_limit_orders_atomic`) continue to compile unchanged.
pub use crate::venues::intl::orders::{place_limit_order, place_limit_orders_atomic};
