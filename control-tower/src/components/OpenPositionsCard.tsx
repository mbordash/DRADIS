'use client';

import { useState } from 'react';
import type { OpenPositionRow, TradeRow } from '@/lib/types';

function fmtTime(iso: string) {
  const d = new Date(iso);
  const date = d.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit' });
  const time = d.toLocaleTimeString('en-US', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  });
  return `${date} ${time}`;
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

/** Returns true for "long / bullish" outcomes: YES, UP, BUY, etc. */
function isLongSide(side: string): boolean {
  const s = side.toUpperCase();
  return s === 'YES' || s === 'UP' || s === 'BUY' || s === 'LONG';
}

/** Estimate unrealised P&L direction from side label colour only (no live price here). */
function strategyLabel(s: string) {
  return s.replace('Strategy', '');
}

function pnlColor(pnl: string) {
  const n = parseFloat(pnl);
  if (n > 0) return 'text-green-400';
  if (n < 0) return 'text-red-400';
  return 'text-gray-400';
}

function fmtPnl(pnl: string) {
  const n = parseFloat(pnl);
  if (isNaN(n)) return pnl;
  return `${n >= 0 ? '+' : ''}$${n.toFixed(4)}`;
}

interface Props {
  positions: OpenPositionRow[];
  trades: TradeRow[];
  isLoading: boolean;
  asset: string;
}

export default function OpenPositionsCard({ positions, trades, isLoading, asset }: Props) {
  const [activeTab, setActiveTab] = useState<'pending' | 'confirmed' | 'completed'>('pending');
  const [rtbModal, setRtbModal] = useState<{ show: boolean; position: OpenPositionRow | null }>({
    show: false,
    position: null,
  });
  const [rtbLoading, setRtbLoading] = useState(false);

  // Split positions by status
  const pending = positions.filter(p => p.status === 'pending');
  const confirmed = positions.filter(p => p.status === 'confirmed');

  const handleRtbClick = (position: OpenPositionRow) => {
    setRtbModal({ show: true, position });
  };

  const handleRtbConfirm = async () => {
    if (!rtbModal.position) return;

    setRtbLoading(true);
    try {
      const res = await fetch('/api/positions/manual-exit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          token_id: rtbModal.position.token_id,
          asset: asset,
          strategy: rtbModal.position.strategy,
          market: rtbModal.position.market,
          side: rtbModal.position.side,
          // TODO: Get actual current bid from live price feed
          current_bid: "0.5", // Placeholder - will be fetched by backend from CLOB
          // TODO: Get actual exchange address from config
          verifying_contract: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E", // Polymarket CTF Exchange
        }),
      });

      if (!res.ok) {
        const err = await res.text();
        alert(`RTB failed: ${err}`);
      } else {
        alert('Position closed successfully! Check Completed Missions tab.');
        // Refresh page to update UI
        window.location.reload();
      }
    } catch (err) {
      alert(`RTB error: ${err}`);
    } finally {
      setRtbLoading(false);
      setRtbModal({ show: false, position: null });
    }
  };

  if (isLoading) {
    return (
      <div className="card p-6 flex items-center justify-center h-24 text-gray-600 text-sm">
        Loading mission status…
      </div>
    );
  }

  return (
    <>
      <div className="card overflow-hidden border border-amber-500/20">
        {/* Header with Three Tabs */}
        <div className="px-4 pt-3 pb-2 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <span className="text-amber-400 text-base">⏳</span>
            <p className="label-muted">Mission Activity</p>
          </div>
          <div className="flex items-center gap-1 text-xs font-mono">
            <button
              onClick={() => setActiveTab('pending')}
              className={[
                'px-2.5 py-1 rounded border transition-colors',
                activeTab === 'pending'
                  ? 'bg-blue-500/10 text-blue-300 border-blue-500/30'
                  : 'bg-[#13131f] text-gray-500 border-[#1e1e32] hover:text-gray-300 hover:border-gray-600',
              ].join(' ')}
            >
              🚀 Viper Launches ({pending.length})
            </button>
            <button
              onClick={() => setActiveTab('confirmed')}
              className={[
                'px-2.5 py-1 rounded border transition-colors',
                activeTab === 'confirmed'
                  ? 'bg-amber-500/10 text-amber-300 border-amber-500/30'
                  : 'bg-[#13131f] text-gray-500 border-[#1e1e32] hover:text-gray-300 hover:border-gray-600',
              ].join(' ')}
            >
              ✈️ Missions In-Flight ({confirmed.length})
            </button>
            <button
              onClick={() => setActiveTab('completed')}
              className={[
                'px-2.5 py-1 rounded border transition-colors',
                activeTab === 'completed'
                  ? 'bg-green-500/10 text-green-300 border-green-500/30'
                  : 'bg-[#13131f] text-gray-500 border-[#1e1e32] hover:text-gray-300 hover:border-gray-600',
              ].join(' ')}
            >
              🎯 Completed Missions ({trades.length})
            </button>
          </div>
        </div>

        {/* Tab Content */}
        {activeTab === 'pending' && (
          pending.length === 0 ? (
            <div className="px-4 pb-4 flex items-center gap-3 text-gray-600 text-sm">
              <span className="text-base opacity-50">🚀</span>
              <span>No pending launches — all vipers are either confirmed or at rest.</span>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-xs font-mono">
                <thead>
                  <tr className="border-b border-[#1e1e32]">
                    {['Launched', 'Asset', 'Strategy', 'Market', 'Side', 'Entry', 'Shares', 'Mode'].map(h => (
                      <th key={h} className="px-3 py-2 text-left text-gray-500 font-normal whitespace-nowrap">
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pending.map((p, i) => (
                    <tr
                      key={i}
                      className="border-b border-[#1e1e32] hover:bg-[#1a1a2e] transition-colors"
                    >
                      <td className="px-3 py-2 text-gray-400 whitespace-nowrap">{fmtTime(p.ts)}</td>
                      <td className="px-3 py-2">
                        <span className="inline-block px-1.5 py-0.5 text-[10px] font-bold rounded bg-indigo-500/10 text-indigo-300 border border-indigo-500/20">
                          {asset.toUpperCase()}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-gray-300 whitespace-nowrap">{strategyLabel(p.strategy)}</td>
                      <td className="px-3 py-2 text-gray-400 max-w-[160px]">
                        <TipCell full={p.market} maxChars={26} />
                      </td>
                      <td className={`px-3 py-2 font-semibold ${isLongSide(p.side) ? 'text-green-400' : 'text-red-400'}`}>
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
          )
        )}

        {activeTab === 'confirmed' && (
          confirmed.length === 0 ? (
            <div className="px-4 pb-4 flex items-center gap-3 text-gray-600 text-sm">
              <span className="text-base opacity-50">✈️</span>
              <span>No active missions — all positions are either pending or closed.</span>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-xs font-mono">
                <thead>
                  <tr className="border-b border-[#1e1e32]">
                     {['Entered', 'Asset', 'Strategy', 'Market', 'Side', 'Entry', 'Cur Price', 'Shares', 'Mode', 'Actions'].map(h => (
                      <th key={h} className="px-3 py-2 text-left text-gray-500 font-normal whitespace-nowrap">
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {confirmed.map((p, i) => (
                    <tr
                      key={i}
                      className="border-b border-[#1e1e32] hover:bg-[#1a1a2e] transition-colors"
                    >
                      <td className="px-3 py-2 text-gray-400 whitespace-nowrap">
                        {p.chain_adopted
                          ? <span title="Re-adopted from on-chain wallet; original entry time unknown" className="text-amber-500/80 cursor-help">⛓ adopted</span>
                          : fmtTime(p.ts)
                        }
                      </td>
                      <td className="px-3 py-2">
                        <span className="inline-block px-1.5 py-0.5 text-[10px] font-bold rounded bg-indigo-500/10 text-indigo-300 border border-indigo-500/20">
                          {asset.toUpperCase()}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-gray-300 whitespace-nowrap">{strategyLabel(p.strategy)}</td>
                      <td className="px-3 py-2 text-gray-400 max-w-[160px]">
                        <TipCell full={p.market} maxChars={26} />
                      </td>
                      <td className={`px-3 py-2 font-semibold ${isLongSide(p.side) ? 'text-green-400' : 'text-red-400'}`}>
                        {p.side}
                      </td>
                      <td className="px-3 py-2 text-gray-300">{parseFloat(p.entry_price).toFixed(4)}</td>
                      <td className="px-3 py-2">
                        {p.current_price ? (() => {
                          const cur = parseFloat(p.current_price);
                          const entry = parseFloat(p.entry_price);
                          const color = cur > entry ? 'text-green-400' : cur < entry ? 'text-red-400' : 'text-gray-300';
                          return <span className={color}>{cur.toFixed(4)}</span>;
                        })() : <span className="text-gray-600">—</span>}
                      </td>
                      <td className="px-3 py-2 text-gray-400">{parseFloat(p.shares).toFixed(2)}</td>
                      <td className="px-3 py-2">
                        {p.ghost_mode
                          ? <span className="text-amber-400 opacity-70">👻 ghost</span>
                          : <span className="text-green-400 opacity-70">⚡ live</span>
                        }
                      </td>
                      <td className="px-3 py-2">
                        <button
                          onClick={() => handleRtbClick(p)}
                          className="px-2 py-0.5 text-[10px] rounded bg-orange-500/10 text-orange-300 border border-orange-500/30 hover:bg-orange-500/20 transition-colors"
                          title="Return to Base: Manually close this position"
                        >
                          🛬 RTB
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )
        )}

        {activeTab === 'completed' && (
          trades.length === 0 ? (
            <div className="px-4 pb-4 flex items-center gap-3 text-gray-600 text-sm">
              <span className="text-base opacity-50">🎯</span>
              <span>No completed missions yet this session.</span>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-xs font-mono">
                <thead>
                  <tr className="border-b border-[#1e1e32]">
                    {['Time', 'Strategy', 'Market', 'Side', 'Entry', 'Exit', 'Shares', 'P&L', 'Reason'].map((h) => (
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
                      <td className="px-3 py-2 text-gray-300 whitespace-nowrap">{strategyLabel(t.strategy)}</td>
                      <td className="px-3 py-2 text-gray-400 max-w-[160px]">
                        <TipCell full={t.market} maxChars={26} />
                      </td>
                      <td className={`px-3 py-2 font-semibold ${isLongSide(t.side) ? 'text-green-400' : 'text-red-400'}`}>
                        {t.side}
                      </td>
                      <td className="px-3 py-2 text-gray-300">{parseFloat(t.entry_price).toFixed(4)}</td>
                      <td className="px-3 py-2 text-gray-300">{parseFloat(t.exit_price).toFixed(4)}</td>
                      <td className="px-3 py-2 text-gray-400">{parseFloat(t.shares).toFixed(2)}</td>
                      <td className={`px-3 py-2 font-semibold ${pnlColor(t.pnl)}`}>{fmtPnl(t.pnl)}</td>
                      <td className="px-3 py-2 text-gray-500 max-w-[220px]">
                        <TipCell full={t.reason} maxChars={34} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )
        )}
      </div>

      {/* RTB Confirmation Modal */}
      {rtbModal.show && rtbModal.position && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm">
          <div className="bg-[#13131f] border border-amber-500/30 rounded-lg shadow-2xl p-6 max-w-md w-full mx-4">
            <h3 className="text-lg font-semibold text-amber-400 mb-3 flex items-center gap-2">
              <span>🛬</span> Return to Base Confirmation
            </h3>

            <div className="space-y-3 text-sm text-gray-300 mb-5">
              <p>
                <strong className="text-white">Position:</strong>{' '}
                {truncate(rtbModal.position.market, 50)}
              </p>
              <p>
                <strong className="text-white">Side:</strong>{' '}
                <span className={isLongSide(rtbModal.position.side) ? 'text-green-400' : 'text-red-400'}>
                  {rtbModal.position.side}
                </span>
              </p>
              <p>
                <strong className="text-white">Shares:</strong> {parseFloat(rtbModal.position.shares).toFixed(2)}
              </p>

              <div className="bg-orange-500/10 border border-orange-500/30 rounded p-3 mt-4">
                <p className="text-orange-300 font-semibold mb-1">⚠️ Warning:</p>
                <ul className="text-xs text-gray-400 space-y-1 list-disc list-inside">
                  <li>This will place a <strong>market order (FAK)</strong></li>
                  <li>Taker fees apply (~2% on Polymarket)</li>
                  <li>Alternative: Let position settle naturally (no fees)</li>
                </ul>
              </div>
            </div>

            <div className="flex gap-3">
              <button
                onClick={() => setRtbModal({ show: false, position: null })}
                disabled={rtbLoading}
                className="flex-1 px-4 py-2 rounded bg-gray-700 hover:bg-gray-600 text-white transition-colors disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                onClick={handleRtbConfirm}
                disabled={rtbLoading}
                className="flex-1 px-4 py-2 rounded bg-orange-500 hover:bg-orange-600 text-white font-semibold transition-colors disabled:opacity-50"
              >
                {rtbLoading ? 'Closing...' : '🛬 Confirm RTB'}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

