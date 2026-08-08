'use client';

import useSWR from 'swr';
import { getVipersStatus, ViperStatusRow } from '@/lib/api';

/**
 * Viper Activity — answers "why isn't DRADIS trading right now?".
 *
 * Every strategy evaluation tick is recorded engine-side; instrumented vipers
 * additionally report the named gate that vetoed their most recent entry
 * attempt (e.g. "oracle too flat", "edge below required", "cooldown active").
 * A quiet market with all vipers ALIVE + named holding reasons means the
 * engine is healthy and correctly sitting out — not broken.
 */

function fmtAgo(secs: number | null): string {
  if (secs === null) return '—';
  if (secs < 5) return 'just now';
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

function outcomeBadge(row: ViperStatusRow) {
  // Stale = not evaluated in the last 2 minutes (ticks run sub-second).
  if (row.last_eval_secs_ago > 120) {
    return <span className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-red-500/10 text-red-400 border border-red-500/30">STALE</span>;
  }
  switch (row.last_outcome) {
    case 'signal':
      return <span className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">SIGNAL</span>;
    case 'error':
      return <span className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-red-500/10 text-red-400 border border-red-500/30">ERROR</span>;
    case 'timeout':
      return <span className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-amber-500/10 text-amber-300 border border-amber-500/30">TIMEOUT</span>;
    default:
      return <span className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-gray-500/10 text-gray-400 border border-gray-500/30">ALIVE</span>;
  }
}

export default function ViperActivityPanel({ asset }: { asset: string }) {
  const { data, isLoading } = useSWR(['vipers-status', asset], () => getVipersStatus(asset), {
    refreshInterval: 10_000,
    revalidateOnFocus: false,
  });

  // Hide entirely until the engine has evaluated at least one viper —
  // avoids an empty panel on idle/unconfigured instances.
  if (!isLoading && (!data || data.length === 0)) return null;

  return (
    <section>
      <div className="flex items-center justify-between mb-3">
        <p className="label-muted">Viper Activity</p>
        <span className="text-xs text-gray-600 font-mono">
          {asset.toUpperCase()} squadron · why isn&apos;t it trading?
        </span>
      </div>
      <div className="card p-0 overflow-x-auto">
        {isLoading || !data ? (
          <div className="p-4 text-sm text-gray-600">Loading viper activity…</div>
        ) : (
          <table className="w-full text-xs font-mono">
            <thead>
              <tr className="text-gray-600 border-b border-gray-800">
                <th className="text-left px-3 py-2 font-normal">Viper</th>
                <th className="text-left px-3 py-2 font-normal">Status</th>
                <th className="text-left px-3 py-2 font-normal">Holding because</th>
                <th className="text-right px-3 py-2 font-normal">Last signal</th>
              </tr>
            </thead>
            <tbody>
              {data.map((row) => {
                const disabled = row.last_reason === 'disabled in config';
                return (
                  <tr key={row.strategy} className={`border-b border-gray-800/50 last:border-0 ${disabled ? 'opacity-50' : ''}`}>
                    <td className="px-3 py-2 text-gray-300">
                      {row.strategy.replace(/Strategy$/, '')}
                    </td>
                    <td className="px-3 py-2">
                      {disabled
                        ? <span className="px-1.5 py-0.5 rounded text-[10px] bg-gray-500/10 text-gray-500 border border-gray-500/30">DISABLED</span>
                        : outcomeBadge(row)}
                      <span className="ml-2 text-gray-600" title={row.last_eval_at}>
                        eval {fmtAgo(row.last_eval_secs_ago)}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-gray-400">
                      {disabled ? (
                        <span className="text-gray-600">disabled in config</span>
                      ) : row.last_reason ? (
                        <>
                          {row.last_reason}
                          <span className="text-gray-600"> · {fmtAgo(row.last_reason_secs_ago)}</span>
                        </>
                      ) : (
                        <span className="text-gray-600" title="This viper is evaluating every tick but its entry gates don't report named holding reasons yet.">
                          evaluating (gates not instrumented)
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-right text-gray-500" title={row.last_signal_at ?? undefined}>
                      {row.last_signal_secs_ago !== null ? fmtAgo(row.last_signal_secs_ago) : 'none this session'}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}

/**
 * One-line CAG-level rollup: "N/N vipers alive across all squadrons".
 * Overview answers "is anything wrong anywhere?"; the per-squadron panel
 * (in the squadron detail view) carries the diagnostic detail.
 */
export function ViperHealthStrip() {
  const { data } = useSWR('vipers-status-all', () => getVipersStatus(), {
    refreshInterval: 15_000,
    revalidateOnFocus: false,
  });

  if (!data || data.length === 0) return null;

  const active = data.filter((r) => r.last_reason !== 'disabled in config');
  const alive = active.filter(
    (r) => r.last_eval_secs_ago <= 120 && r.last_outcome !== 'error' && r.last_outcome !== 'timeout',
  );
  const troubled = active.length - alive.length;
  const squadrons = new Set(data.map((r) => r.asset)).size;
  // Up to 3 distinct holding reasons for a quick "why quiet?" glance.
  const reasons = [...new Set(alive.map((r) => r.last_reason).filter(Boolean))].slice(0, 3);

  const ok = troubled === 0;
  return (
    <div
      className={`card px-4 py-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs font-mono ${ok ? 'text-gray-500' : 'text-red-400'}`}
      title="Per-squadron detail: click a squadron below → Viper Activity"
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
