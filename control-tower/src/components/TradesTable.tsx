'use client';

import type { TradeRow } from '@/lib/types';

function pnlColor(pnl: string) {
  const n = parseFloat(pnl);
  if (n > 0)  return 'text-green-400';
  if (n < 0)  return 'text-red-400';
  return 'text-gray-400';
}

function fmtTime(iso: string) {
  return new Date(iso).toLocaleTimeString('en-US', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  });
}

function fmtPnl(pnl: string) {
  const n = parseFloat(pnl);
  if (isNaN(n)) return pnl;
  return `${n >= 0 ? '+' : ''}$${n.toFixed(4)}`;
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
      {/* Tooltip panel — select-none prevents hidden text from appearing in copy/paste */}
      <span
        aria-hidden="true"
        className="
          pointer-events-none select-none absolute z-50 bottom-full left-0 mb-1.5
          w-max max-w-xs
          rounded-md px-2.5 py-1.5
          bg-[#1e1e35] border border-[#2e2e4e] text-gray-200 text-[11px] font-mono leading-snug
          shadow-lg shadow-black/60
          opacity-0 group-hover:opacity-100
          transition-opacity duration-100
          whitespace-pre-wrap break-words
        "
      >
        {full}
      </span>
    </span>
  );
}

interface Props {
  trades: TradeRow[];
}

export default function TradesTable({ trades }: Props) {
  if (trades.length === 0) {
    return (
      <div className="card p-6 flex items-center justify-center h-32 text-gray-600 text-sm">
        No completed trades yet this session.
      </div>
    );
  }

  return (
    <div className="card overflow-hidden">
      <div className="px-4 pt-4 pb-2">
        <p className="label-muted">Recent Trades</p>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs font-mono">
          <thead>
            <tr className="border-b border-[#1e1e32]">
              {['Time', 'Strategy', 'Market', 'Side', 'Entry', 'Exit', 'Shares', 'P&L', 'Reason'].map(h => (
                <th key={h} className="px-3 py-2 text-left text-gray-500 font-normal whitespace-nowrap">
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {trades.map((t, i) => (
              <tr
                key={i}
                className="border-b border-[#1e1e32] hover:bg-[#1a1a2e] transition-colors"
              >
                <td className="px-3 py-2 text-gray-400 whitespace-nowrap">{fmtTime(t.ts)}</td>
                <td className="px-3 py-2 text-gray-300 whitespace-nowrap">{t.strategy}</td>
                <td className="px-3 py-2 text-gray-400 max-w-[160px]">
                  <TipCell full={t.market} maxChars={26} />
                </td>
                <td className={`px-3 py-2 font-semibold ${t.side === 'YES' ? 'text-green-400' : 'text-red-400'}`}>
                  {t.side}
                </td>
                <td className="px-3 py-2 text-gray-300">{parseFloat(t.entry_price).toFixed(4)}</td>
                <td className="px-3 py-2 text-gray-300">{parseFloat(t.exit_price).toFixed(4)}</td>
                <td className="px-3 py-2 text-gray-400">{parseFloat(t.shares).toFixed(2)}</td>
                <td className={`px-3 py-2 font-semibold ${pnlColor(t.pnl)}`}>{fmtPnl(t.pnl)}</td>
                <td className="px-3 py-2 text-gray-500 max-w-[200px]">
                  <TipCell full={t.reason} maxChars={32} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

