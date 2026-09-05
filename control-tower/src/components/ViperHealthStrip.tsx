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
import { isTroubled } from '@/components/ViperCard';

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
  const alive = active.filter((r) => !isTroubled(r));
  const troubled = active.length - alive.length;
  // Alive but with nothing to trade: the squadron holds no market and the
  // engine says so every tick. Counted as alive (the loop is ticking) and
  // stated separately, so the ribbon reads "waiting" rather than either
  // hiding the state or, as it did on a fresh Marketplace instance
  // 2026-09-04, reporting nine waiting vipers as "9 stale/error".
  const waiting = alive.filter((r) => r.last_outcome === 'idle').length;
  const disabled = data.length - active.length;
  // Distinct scopes, not distinct assets: every Kalshi squadron shares one DB
  // shard, so counting the shard name would report one squadron for all of them.
  // A row whose scope is empty predates the fix that gave non-crypto squadrons
  // their own key; count it rather than folding it into another squadron.
  //
  // Counted over `data`, deliberately: every deployed squadron is real even if
  // all of its vipers are disabled, and reporting only those with a live viper
  // would hide a squadron the operator can see on screen.
  //
  // The number that WAS missing is `disabled`. Without it the strip read
  // "9/9 vipers alive across 3 squadrons" on an instance running 13 vipers,
  // inviting the reader to conclude there were nine — the tally silently
  // dropped the four disabled ones instead of accounting for them.
  const squadrons = new Set(data.map((r) => r.asset || '(unscoped)')).size;
  // Up to 3 distinct holding reasons for a quick "why quiet?" glance. Waiting
  // rows are stated by count above; their one reason would only crowd these out.
  const reasons = [...new Set(
    alive.filter((r) => r.last_outcome !== 'idle').map((r) => r.last_reason).filter(Boolean),
  )].slice(0, 3);

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
        {/* Stated rather than silently subtracted, so the tally reconciles with
            the viper cards an operator can count on screen. */}
        {disabled > 0 ? ` · ${disabled} disabled` : ''}
        {waiting > 0 ? ` · ${waiting} waiting for a market` : ''}
      </span>
      {!ok && <span>{troubled} stale/error — check squadron detail</span>}
      {ok && reasons.length > 0 && (
        <span className="text-gray-600 truncate">holding: {reasons.join(' · ')}</span>
      )}
    </div>
  );
}
