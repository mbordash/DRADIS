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

/**
 * SeededConfigNotice — one line under the header while this session's config
 * was inherited from another database shard.
 *
 * A venue switch gives the engine a new primary shard. Until 1.4.0 that meant
 * the compiled template: ghost mode on, the sports ledger off, every knob the
 * operator had set gone, and nothing on screen said so (production, 2026-10-17).
 * The engine now seeds the new shard from the newest sibling shard and reports
 * it as `config_origin.kind === 'seeded'`; this is that report. The instance-level
 * keys it deliberately left at their defaults are named, because ghost mode
 * being on is the one the operator will go looking for. The engine clears the
 * origin on the first config save, so the notice disappears once Setup has
 * been reviewed.
 */

import type { ConfigOrigin } from '@/lib/setupApi';

const LABELS: Record<string, string> = {
  ghost_mode: 'ghost mode',
  book_apply_price_changes: 'book price-change feed',
  helm_enabled: 'Helm',
  helm_live_enabled: 'Helm live orders',
  helm_max_open_intents: 'Helm open-intent cap',
};

export default function SeededConfigNotice({
  origin, onReview,
}: { origin: ConfigOrigin | undefined; onReview: () => void }) {
  if (!origin || origin.kind !== 'seeded') return null;
  const source = origin.source.split('/').pop() ?? origin.source;
  const kept = origin.kept_default.map((k) => LABELS[k] ?? k);
  return (
    <div className="border-b border-amber-500/30 bg-amber-500/10 px-6 py-2 text-xs font-mono text-amber-200">
      <div className="max-w-7xl mx-auto flex flex-wrap items-center gap-x-3 gap-y-1">
        <span>
          Settings were inherited from <span className="text-amber-100">{source}</span> because this venue&apos;s
          database was new.
          {kept.length > 0
            ? <> Left at defaults: <span className="text-amber-100">{kept.join(', ')}</span>.</>
            : null}
          {' '}Review Setup before trading; saving any setting clears this notice.
        </span>
        <button
          type="button"
          onClick={onReview}
          className="rounded-lg border border-amber-500/40 px-2 py-0.5 text-amber-100 hover:bg-amber-500/20 transition-colors"
        >
          Open Setup
        </button>
      </div>
    </div>
  );
}
