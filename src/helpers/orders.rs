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

//! Order placement helpers — relocated to `venues::intl::orders`.
//!
//! The EIP-712 self-custody signing logic is venue-specific and now lives under
//! `venues::intl` (see `docs/VENUE_ABSTRACTION.md`, Step 1). This module re-exports
//! those symbols so existing call sites (`crate::helpers::orders::place_limit_order`,
//! `place_limit_orders_atomic`) continue to compile unchanged.
pub use crate::venues::intl::orders::{
    classify_placement_error, place_limit_order, place_limit_order_filled,
    place_limit_orders_atomic, PlacementFault,
};
