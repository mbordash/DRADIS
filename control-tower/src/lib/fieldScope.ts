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

import type { ConfigFieldSchema } from './types';

/**
 * Does `f` belong on a card for a squadron of `marketClass`?
 *
 * The schema scopes a few fields to one market class (FairValue's sports lane,
 * Maker's sports dispersion). A crypto squadron's card leaves those out. An
 * unknown class shows everything: hiding a field on a guess is worse than
 * showing one that does not apply.
 */
export function fieldAppliesTo(f: ConfigFieldSchema, marketClass: string | null | undefined): boolean {
  if (!f.market_classes || !marketClass) return true;
  return f.market_classes.includes(marketClass);
}
