'use client';

import type { OpenPositionRow } from '@/lib/types';

function fmtTime(iso: string) {
  return new Date(iso).toLocaleTimeString('en-US', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  });
}

function truncate(s: string, n: number) {
  return s.length > n ? s.slice(0, n) + '…' : s;
}

/** Inline tooltip cell: shows dotted underline and a styled popup on hover. */
function TipCell({ full, maxChars, className = '' }: { full: string; maxChars: number; className?: string }) {
  const isTruncated = full.length > maxChars;
  if (!isTruncated) return <span className={className}>{full}</span>;
  return (
    <span className="relative group inline-block">
      <span className={[
        'border-b border-dotted border-gray-600 cursor-help',
        className,
      ].filter(Boolean).join(' ')}>
        {truncate(full, maxChars)}
      </span>
      {/* Tooltip panel */}
      <span className="
        pointer-events-none absolute z-50 bottom-full left-0 mb-1.5
        w-max max-w-xs
        rounded-md px-2.5 py-1.5
        bg-[#1e1e35] border border-[#2e2e4e] text-gray-200 text-[11px] font-mono leading-snug
        shadow-lg shadow-black/60
        opacity-0 group-hover:opacity-100
        transition-opacity duration-100
        whitespace-pre-wrap break-words
      ">
        {full}
      </span>
    </span>
  );
}

/** Estimate unrealised P&L direction from side label colour only (no live price here). */
function strategyLabel(s: string) {
  return s.replace('Strategy', '');
}

interface Props {
  positions: OpenPositionRow[];
  isLoading: boolean;
}

export default function OpenPositionsCard({ positions, isLoading }: Props) {
  if (isLoading) {
    return (
      <div className="card p-6 flex items-center justify-center h-24 text-gray-600 text-sm">
        Loading open positions…
      </div>
    );
  }

  if (positions.length === 0) {
    return (
      <div className="card p-4 flex items-center gap-3 text-gray-600 text-sm">
        <span className="text-base opacity-50">📭</span>
        <span>No open positions — all strategies are flat.</span>
      </div>
    );
  }

  return (
    <div className="card overflow-hidden border border-amber-500/20">
      {/* Header */}
      <div className="px-4 pt-3 pb-2 flex items-center gap-2">
        <span className="text-amber-400 text-base">⏳</span>
        <p className="label-muted">Open Positions</p>
        <span className="ml-1 text-xs font-mono bg-amber-500/10 text-amber-300 border border-amber-500/20 rounded px-1.5 py-0.5">
          {positions.length} in-flight
        </span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs font-mono">
          <thead>
            <tr className="border-b border-[#1e1e32]">
              {['Entered', 'Strategy', 'Market', 'Side', 'Entry', 'Shares', 'Mode'].map(h => (
                <th key={h} className="px-3 py-2 text-left text-gray-500 font-normal whitespace-nowrap">
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {positions.map((p, i) => (
              <tr
                key={i}
                className="border-b border-[#1e1e32] hover:bg-[#1a1a2e] transition-colors"
              >
                <td className="px-3 py-2 text-gray-400 whitespace-nowrap">{fmtTime(p.ts)}</td>
                <td className="px-3 py-2 text-gray-300 whitespace-nowrap">{strategyLabel(p.strategy)}</td>
                <td className="px-3 py-2 text-gray-400 max-w-[160px]">
                  <TipCell full={p.market} maxChars={26} />
                </td>
                <td className={`px-3 py-2 font-semibold ${p.side === 'YES' ? 'text-green-400' : 'text-red-400'}`}>
                  {p.side}
                </td>
                <td className="px-3 py-2 text-gray-300">{parseFloat(p.entry_price).toFixed(4)}</td>
                <td className="px-3 py-2 text-gray-400">{parseFloat(p.shares).toFixed(2)}</td>
                <td className="px-3 py-2">
                  {p.ghost_mode
                    ? <span className="text-amber-400 opacity-70">👻 ghost</span>
                    : <span className="text-green-400 opacity-70">⚡ live</span>
                  }
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

