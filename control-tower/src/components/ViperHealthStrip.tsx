'use client';

// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS Control Tower — operator dashboard for the DRADIS trading engine.
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

import useSWR from 'swr';
import { getVipersStatus } from '@/lib/api';
import { STALE_EVAL_SECS } from '@/components/ViperCard';

/**
 * One-line CAG-level rollup: "N/N vipers alive across all squadrons".
 *
 * Overview answers "is anything wrong anywhere?". The per-viper detail —
 * liveness badge, the named gate holding entry, and time since the last
 * signal — lives on each ViperCard in the squadron detail view, next to the
 * params that would clear the veto.
 */
export function ViperHealthStrip() {
  const { data } = useSWR('vipers-status-all', () => getVipersStatus(), {
    refreshInterval: 15_000,
    revalidateOnFocus: false,
  });

  if (!data || data.length === 0) return null;

  const active = data.filter((r) => r.last_reason !== 'disabled in config');
  const alive = active.filter(
    (r) => r.last_eval_secs_ago <= STALE_EVAL_SECS && r.last_outcome !== 'error' && r.last_outcome !== 'timeout',
  );
  const troubled = active.length - alive.length;
  // Distinct scopes, not distinct assets: every Kalshi squadron shares one DB
  // shard, so counting the shard name would report one squadron for all of them.
  // A row whose scope is empty predates the fix that gave non-crypto squadrons
  // their own key; count it rather than folding it into another squadron.
  const squadrons = new Set(data.map((r) => r.asset || '(unscoped)')).size;
  // Up to 3 distinct holding reasons for a quick "why quiet?" glance.
  const reasons = [...new Set(alive.map((r) => r.last_reason).filter(Boolean))].slice(0, 3);

  const ok = troubled === 0;
  return (
    <div
      className={`card px-4 py-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs font-mono ${ok ? 'text-gray-500' : 'text-red-400'}`}
      title="Per-viper detail: click a squadron below → Viper Layer"
    >
      <span>{ok ? '🟢' : '🔴'}</span>
      <span className={ok ? 'text-gray-400' : ''}>
        {alive.length}/{active.length} vipers alive
        {squadrons > 1 ? ` across ${squadrons} squadrons` : ''}
      </span>
      {!ok && <span>{troubled} stale/error — check squadron detail</span>}
      {ok && reasons.length > 0 && (
        <span className="text-gray-600 truncate">holding: {reasons.join(' · ')}</span>
      )}
    </div>
  );
}
