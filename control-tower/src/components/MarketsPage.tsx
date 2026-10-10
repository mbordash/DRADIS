'use client';

import { useEffect, useMemo, useState } from 'react';
import useSWR from 'swr';
import { getLiveMarkets } from '@/lib/api';
import type { LiveMarketRow, MarketType } from '@/lib/types';
import MarketDetailPanel from './markets/MarketDetailPanel';
import { fmtClosesIn, fmtMoney } from './markets/format';

interface Props {
  selectedId: string | null;
  onSelect: (id: string | null) => void;
  onOpenSquadron: (squadronId: string) => void;
  onTakeHelm: (m: { condition_id: string; question: string; market_class: MarketType; end_date: string; criteria?: string }) => void;
}

type Tab = 'crypto' | 'sports' | 'politics';

const TABS: { type: Tab; icon: string }[] = [
  { type: 'crypto', icon: '🪙' },
  { type: 'sports', icon: '🏈' },
  { type: 'politics', icon: '🗳️' },
];
const WINDOWS = ['1h', '4h', '24h', '7d', '30d'];
const STORE_KEY = 'dradis.markets.tab';

function isTab(v: string | null): v is Tab {
  return v === 'crypto' || v === 'sports' || v === 'politics';
}

export default function MarketsPage({ selectedId, onSelect, onOpenSquadron, onTakeHelm }: Props) {
  const [tab, setTab] = useState<Tab>('crypto');
  // null means "API default" (no window sent); crypto starts at 4h.
  const [windowSel, setWindowSel] = useState<string | null>('4h');

  useEffect(() => {
    try {
      const saved = localStorage.getItem(STORE_KEY);
      if (isTab(saved)) {
        setTab(saved);
        setWindowSel(saved === 'crypto' ? '4h' : null);
      }
    } catch { /* per-viewer convenience only */ }
  }, []);

  const pickTab = (t: Tab) => {
    setTab(t);
    setWindowSel(t === 'crypto' ? '4h' : null);
    try { localStorage.setItem(STORE_KEY, t); } catch { /* ignore */ }
  };

  const { data, error, isLoading } = useSWR(
    ['markets-live', tab, windowSel],
    () => getLiveMarkets(tab, windowSel ? { expiryWindow: windowSel } : undefined),
    { refreshInterval: 30_000, keepPreviousData: true },
  );

  const rows = useMemo<LiveMarketRow[]>(() => {
    const list = data?.markets ?? [];
    return [...list].sort((a, b) => Number(!!b.squadron_id) - Number(!!a.squadron_id));
  }, [data]);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-sm font-mono uppercase tracking-wide text-teal-300">📈 Markets</h2>
        <p className="text-[11px] font-mono text-gray-500 mt-1 max-w-3xl">
          Live markets on this venue, each with the venue&apos;s picture and the engine&apos;s own view. Pick one to open it; Take the Helm from here to act on it.
        </p>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        {TABS.map(b => (
          <button
            key={b.type}
            onClick={() => pickTab(b.type)}
            className={`rounded border px-3 py-2 text-xs font-mono transition-colors ${
              tab === b.type
                ? 'border-teal-500/50 bg-teal-500/10 text-teal-200'
                : 'border-[#1e1e32] text-gray-400 hover:text-gray-200'
            }`}
          >
            {b.icon} {b.type}
          </button>
        ))}
        <label className="ml-auto flex items-center gap-2 text-[11px] font-mono text-gray-500">
          Closes within
          <select
            value={windowSel ?? ''}
            onChange={e => setWindowSel(e.target.value || null)}
            className="rounded border border-[#1e1e32] bg-[#0d0d1a] px-2 py-1 text-xs font-mono text-gray-300"
          >
            {windowSel === null && <option value="">default</option>}
            {WINDOWS.map(w => <option key={w} value={w}>{w}</option>)}
          </select>
        </label>
      </div>

      <div className={selectedId ? 'grid lg:grid-cols-[1fr_1.2fr] gap-6 items-start' : ''}>
        <div className="min-w-0">
          {error && !data ? (
            <div className="card px-4 py-3 border border-red-500/30 bg-red-500/5 text-red-300 text-xs font-mono">
              {error instanceof Error ? error.message : 'Failed to load markets'}
            </div>
          ) : isLoading && !data ? (
            <p className="text-xs font-mono text-gray-500">Fetching live markets…</p>
          ) : rows.length === 0 ? (
            <p className="text-xs font-mono text-gray-500">No live {tab} markets inside this window on this venue.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left">
                <thead>
                  <tr className="text-[10px] font-mono uppercase tracking-wide text-gray-500">
                    <th className="py-2 pr-3 font-normal">Question</th>
                    <th className="py-2 pr-3 font-normal">Class</th>
                    <th className="py-2 pr-3 font-normal">Closes in</th>
                    <th className="py-2 pr-3 font-normal text-right">Liquidity</th>
                    <th className="py-2 font-normal">Squadron</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map(r => {
                    const sel = r.market_id === selectedId;
                    return (
                      <tr
                        key={r.market_id}
                        onClick={() => onSelect(r.market_id)}
                        className={`cursor-pointer border-t border-[#1e1e32] hover:bg-white/[0.02] ${sel ? 'bg-teal-500/5 ring-1 ring-teal-500/20' : ''}`}
                      >
                        <td className="py-2 pr-3 max-w-[22rem]">
                          <p className="text-xs font-mono text-gray-200 truncate" title={r.question}>{r.question}</p>
                          {sel && r.criteria && (
                            <p className="text-[10px] text-gray-600 font-mono">{r.criteria}</p>
                          )}
                          {r.note && <p className="text-[10px] font-mono text-amber-300">{r.note}</p>}
                        </td>
                        <td className="py-2 pr-3 text-[11px] font-mono text-gray-400">{r.market_class}</td>
                        <td className="py-2 pr-3 text-[11px] font-mono text-gray-300 whitespace-nowrap">
                          {fmtClosesIn(r.end_date, r.accepting_orders)}
                        </td>
                        <td className="py-2 pr-3 text-[11px] font-mono text-gray-300 text-right">{fmtMoney(r.liquidity)}</td>
                        <td className="py-2">
                          {r.squadron_id ? (
                            <button
                              onClick={e => { e.stopPropagation(); onOpenSquadron(r.squadron_id!); }}
                              className="rounded bg-teal-500/10 px-2 py-0.5 text-[10px] font-mono text-teal-300 hover:bg-teal-500/20"
                            >
                              {r.squadron_id}
                            </button>
                          ) : (
                            <span className="text-gray-600 text-[11px] font-mono">–</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {selectedId && (
          <div className="min-w-0">
            <MarketDetailPanel
              key={selectedId}
              id={selectedId}
              marketType={tab}
              onOpenSquadron={onOpenSquadron}
              onTakeHelm={onTakeHelm}
            />
          </div>
        )}
      </div>
    </div>
  );
}
