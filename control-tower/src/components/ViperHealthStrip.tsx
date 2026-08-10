'use client';

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
  const squadrons = new Set(data.map((r) => r.asset)).size;
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
