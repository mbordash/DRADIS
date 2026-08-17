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

import { useState, useMemo } from 'react';
import useSWR from 'swr';
import type { TradeRow, OpenPositionRow, TradeStats } from '@/lib/types';
import { getTrades, getTradeStats, getOpenPositions, downloadTradelogCsv } from '@/lib/api';
import { DEMO_MODE } from '@/lib/demo';

// ── Types ─────────────────────────────────────────────────────────────────────

type LogStatus = 'launch' | 'inflight' | 'completed';

interface LogEntry {
  key:       string;
  ts:        Date;
  /**
   * Which database the row came from. A *storage* location, not a market
   * attribute — it holds an underlying symbol on the intl CLOB but a venue name
   * on Kalshi and US. Displayed as "Book", never as the asset. The real
   * attributes are `venue` / `marketClass` / `underlying` below.
   */
  shard:     string;
  venue:       string | null;
  marketClass: string | null;
  /** Null for markets with no underlying instrument (sports, politics). */
  underlying:  string | null;
  /** Round-trip venue fees. `pnl` is already net of these. Null if uncaptured. */
  fees:        number | null;
  status:    LogStatus;
  strategy:  string;
  market:    string;
  side:      string;
  entry:     number;
  curOrExit: number | null; // current_price for open; exit_price for completed
  shares:    number;
  pnl:       number | null; // realized for completed; unrealized for open
  reason:    string;
  ghost:     boolean;
  chainAdopted: boolean;
  tokenId?:  string;        // for RTB on open positions
  rawPosition?: OpenPositionRow; // kept for RTB modal
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const ASSET_COLOR: Record<string, string> = {
  btc: 'bg-orange-500/10 text-orange-300 border-orange-500/20',
  eth: 'bg-blue-500/10 text-blue-300 border-blue-500/20',
  sol: 'bg-purple-500/10 text-purple-300 border-purple-500/20',
};

const ASSET_EMOJI: Record<string, string> = { btc: '₿', eth: 'Ξ', sol: '◎' };

const VENUE_LABEL: Record<string, string> = {
  'kalshi':          'Kalshi',
  'polymarket-us':   'Poly US',
  'polymarket-intl': 'Poly Intl',
};

const CLASS_EMOJI: Record<string, string> = {
  crypto: '₿', sports: '🏈', politics: '🏛', unknown: '◈',
};

/**
 * What to show for the market's subject. Crypto markets have an underlying
 * symbol; sports and politics genuinely do not, so they show their class
 * instead of an invented ticker.
 */
function subjectBadge(e: LogEntry): string {
  if (e.underlying)  return `${ASSET_EMOJI[e.underlying] ?? '◈'} ${e.underlying.toUpperCase()}`;
  if (e.marketClass) return `${CLASS_EMOJI[e.marketClass] ?? '◈'} ${e.marketClass.toUpperCase()}`;
  return '—';
}

const STATUS_META: Record<LogStatus, { icon: string; label: string; color: string }> = {
  launch:    { icon: '🚀', label: 'Launch',    color: 'text-blue-400' },
  inflight:  { icon: '✈️',  label: 'In-Flight', color: 'text-amber-400' },
  completed: { icon: '🎯', label: 'Completed', color: 'text-green-400' },
};

function shortStrategy(s: string) { return s.replace('Strategy', ''); }

function fmtTime(d: Date) {
  const date = d.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit' });
  const time = d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
  return `${date} ${time}`;
}

function fmtPnl(n: number | null, prefix = true) {
  if (n === null) return <span className="text-gray-600">—</span>;
  const sign = n >= 0 ? '+' : '';
  const cls  = n > 0 ? 'text-green-400' : n < 0 ? 'text-red-400' : 'text-gray-400';
  return <span className={cls}>{prefix ? `${sign}$${Math.abs(n).toFixed(4)}` : `${sign}$${n.toFixed(4)}`}</span>;
}

function fmtUnrealized(entry: number, cur: number | null, shares: number) {
  if (cur === null) return null;
  return (cur - entry) * shares;
}

function truncate(s: string, n: number) {
  return s.length > n ? s.slice(0, n) + '…' : s;
}

function TipCell({ full, maxChars, className = '' }: { full: string; maxChars: number; className?: string }) {
  if (full.length <= maxChars) return <span className={className}>{full}</span>;
  return (
    <span className="relative group inline-block">
      <span className={`border-b border-dotted border-gray-600 cursor-help ${className}`}>
        {truncate(full, maxChars)}
      </span>
      <span className="pointer-events-none select-none absolute z-50 bottom-full left-0 mb-1.5 w-max max-w-xs rounded-md px-2.5 py-1.5 bg-[#1e1e35] border border-[#2e2e4e] text-gray-200 text-[11px] font-mono leading-snug shadow-lg shadow-black/60 opacity-0 group-hover:opacity-100 transition-opacity duration-100 whitespace-pre-wrap break-words">
        {full}
      </span>
    </span>
  );
}

// Convert API data → LogEntry array for one asset
function assetToEntries(shard: string, trades: TradeRow[], positions: OpenPositionRow[]): LogEntry[] {
  const entries: LogEntry[] = [];

  for (const t of trades) {
    entries.push({
      key:        `${shard}-completed-${t.ts}-${t.market}`,
      ts:         new Date(t.ts),
      shard,
      venue:       t.venue ?? null,
      marketClass: t.market_class ?? null,
      underlying:  t.underlying ?? null,
      fees:        t.fees != null ? parseFloat(t.fees) : null,
      status:     'completed',
      strategy:   t.strategy,
      market:     t.market,
      side:       t.side,
      entry:      parseFloat(t.entry_price),
      curOrExit:  parseFloat(t.exit_price),
      shares:     parseFloat(t.shares),
      pnl:        parseFloat(t.pnl),
      reason:     t.reason,
      ghost:      false,
      chainAdopted: false,
    });
  }

  for (const p of positions) {
    const status: LogStatus = p.status === 'pending' ? 'launch' : 'inflight';
    const entry = parseFloat(p.entry_price);
    const cur   = p.current_price ? parseFloat(p.current_price) : null;
    const shares = parseFloat(p.shares);
    const unrealized = fmtUnrealized(entry, cur, shares);
    entries.push({
      key:         `${shard}-${status}-${p.ts}-${p.token_id}`,
      ts:          new Date(p.ts),
      shard,
      // open_positions carries no filing columns yet; the shard is the
      // underlying on the intl venue and a venue name elsewhere.
      venue:       null,
      marketClass: null,
      underlying:  ASSET_EMOJI[shard] ? shard : null,
      fees:        null,
      status,
      strategy:    p.strategy,
      market:      p.market,
      side:        p.side,
      entry,
      curOrExit:   cur,
      shares,
      pnl:         unrealized,
      reason:      '',
      ghost:       p.ghost_mode,
      chainAdopted: p.chain_adopted,
      tokenId:     p.token_id,
      rawPosition: p,
    });
  }

  return entries;
}

// ── Sub-components ────────────────────────────────────────────────────────────

function FilterPill({
  label, active, onClick,
}: { label: string; active: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={[
        'text-xs font-mono px-3 py-1.5 rounded-lg border transition-colors whitespace-nowrap',
        active
          ? 'bg-indigo-500/20 border-indigo-500/40 text-indigo-300'
          : 'bg-[#13131f] border-[#1e1e32] text-gray-500 hover:border-gray-600 hover:text-gray-300',
      ].join(' ')}
    >
      {label}
    </button>
  );
}

/**
 * Top-of-page totals.
 *
 * `entries` is a bounded recent window (200 rows per asset, and the API clamps
 * any limit to 500), so completed-mission counts and realized P&L come from
 * `stats` — lifetime aggregates computed server-side in SQL — rather than a
 * reduce over what happens to be loaded. Launches, in-flight, and unrealized
 * P&L still come from `entries`: those describe currently-open positions, which
 * are never truncated.
 */
function SummaryBar({ entries, stats }: { entries: LogEntry[]; stats: (TradeStats & { asset: string })[] }) {
  const launches   = entries.filter(e => e.status === 'launch').length;
  const inflight   = entries.filter(e => e.status === 'inflight').length;
  const completedCount = stats.reduce((s, t) => s + t.count, 0);
  const realizedPnl    = stats.reduce((s, t) => s + t.realized_pnl, 0);
  const unrealized = entries
    .filter(e => e.status !== 'completed' && e.pnl !== null)
    .reduce((s, e) => s + (e.pnl ?? 0), 0);

  const pnlTotal = realizedPnl + unrealized;
  const pnlColor = pnlTotal >= 0 ? 'text-green-400' : 'text-red-400';

  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
      <div className="card px-4 py-3 flex flex-col gap-1">
        <span className="label-muted">🚀 Viper Launches</span>
        <span className="stat-value text-blue-400">{launches}</span>
        <span className="text-xs text-gray-500">pending fills</span>
      </div>
      <div className="card px-4 py-3 flex flex-col gap-1">
        <span className="label-muted">✈️ Missions In-Flight</span>
        <span className="stat-value text-amber-400">{inflight}</span>
        <span className="text-xs text-gray-500">confirmed open</span>
      </div>
      <div className="card px-4 py-3 flex flex-col gap-1">
        <span className="label-muted">🎯 Completed Missions</span>
        <span className="stat-value text-gray-200">{completedCount}</span>
        <span className={`text-xs ${realizedPnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
          {realizedPnl >= 0 ? '+' : ''}${realizedPnl.toFixed(4)} realized
        </span>
      </div>
      <div className="card px-4 py-3 flex flex-col gap-1">
        <span className="label-muted">Net P&L</span>
        <span className={`stat-value ${pnlColor}`}>
          {pnlTotal >= 0 ? '+' : ''}${pnlTotal.toFixed(4)}
        </span>
        <span className="text-xs text-gray-500">realized + unrealized</span>
      </div>
    </div>
  );
}

// ── RTB Modal ────────────────────────────────────────────────────────────────

function RtbModal({
  entry,
  onClose,
  onConfirm,
  loading,
}: {
  entry: LogEntry;
  onClose: () => void;
  onConfirm: () => void;
  loading: boolean;
}) {
  function isLong(side: string) {
    const s = side.toUpperCase();
    return s === 'YES' || s === 'UP' || s === 'BUY';
  }
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm">
      <div className="bg-[#13131f] border border-amber-500/30 rounded-lg shadow-2xl p-6 max-w-md w-full mx-4">
        <h3 className="text-lg font-semibold text-amber-400 mb-3 flex items-center gap-2">
          🛬 Return to Base Confirmation
        </h3>
        <div className="space-y-2 text-sm text-gray-300 mb-5">
          <p><strong className="text-white">Venue:</strong> {entry.venue ? (VENUE_LABEL[entry.venue] ?? entry.venue) : entry.shard.toUpperCase()}</p>
          <p><strong className="text-white">Subject:</strong> {subjectBadge(entry)}</p>
          <p><strong className="text-white">Market:</strong> {truncate(entry.market, 60)}</p>
          <p>
            <strong className="text-white">Side:</strong>{' '}
            <span className={isLong(entry.side) ? 'text-green-400' : 'text-red-400'}>{entry.side}</span>
          </p>
          <p><strong className="text-white">Shares:</strong> {entry.shares.toFixed(2)}</p>
          <div className="bg-orange-500/10 border border-orange-500/30 rounded p-3 mt-3">
            <p className="text-orange-300 font-semibold mb-1">⚠️ Warning:</p>
            <ul className="text-xs text-gray-400 space-y-1 list-disc list-inside">
              <li>This places a <strong>market order (FAK)</strong></li>
              <li>Taker fees apply (~2% on Polymarket)</li>
              <li>Alternative: let position settle naturally (no fees)</li>
            </ul>
          </div>
        </div>
        <div className="flex gap-3">
          <button
            onClick={onClose}
            disabled={loading}
            className="flex-1 px-4 py-2 rounded bg-gray-700 hover:bg-gray-600 text-white transition-colors disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={loading}
            className="flex-1 px-4 py-2 rounded bg-orange-500 hover:bg-orange-600 text-white font-semibold transition-colors disabled:opacity-50"
          >
            {loading ? 'Closing…' : '🛬 Confirm RTB'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

interface Props {
  availableAssets: string[];
}

/** Download the complete trade history (all assets) as one CSV — tax
 *  reporting / offline review. A trade record, not a tax document. */
function ExportCsvButton({ assets }: { assets: string[] }) {
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(false);

  const run = async () => {
    setBusy(true);
    setErr(false);
    try { await downloadTradelogCsv(assets); }
    catch { setErr(true); }
    finally { setBusy(false); }
  };

  return (
    <button
      onClick={run}
      disabled={busy}
      title="Download the complete trade history (all assets, all filters ignored) as CSV. A trade record for tax prep or review — not a tax document; cost-basis treatment is your accountant's call."
      className="px-2.5 py-1 rounded-lg text-xs font-mono border border-[#1e1e32] text-gray-500 hover:text-gray-300 hover:border-indigo-500/40 transition-colors disabled:opacity-40"
    >
      {busy ? 'Exporting…' : err ? '⚠ Export failed — retry' : '⬇ Export CSV'}
    </button>
  );
}

export default function TradelogPage({ availableAssets }: Props) {
  // ── Filters ──────────────────────────────────────────────────────────────────
  const [assetFilter,    setAssetFilter]    = useState<string>('all');
  const [statusFilter,   setStatusFilter]   = useState<string>('all');
  const [strategyFilter, setStrategyFilter] = useState<string>('all');
  const [sideFilter,     setSideFilter]     = useState<string>('all');

  // ── RTB state ───────────────────────────────────────────────────────────────
  const [rtbEntry,   setRtbEntry]   = useState<LogEntry | null>(null);
  const [rtbLoading, setRtbLoading] = useState(false);

  // ── Fetch all assets in parallel ─────────────────────────────────────────────
  const assets = availableAssets.length > 0 ? availableAssets : ['btc', 'eth', 'sol'];

  const { data: allTrades = [], isLoading: tradesLoading } = useSWR(
    ['tradelog-trades', assets.join(',')],
    async () => {
      const results = await Promise.all(assets.map(a => getTrades(200, a).then(rows => rows.map(r => ({ asset: a, ...r })))));
      return results.flat();
    },
    { refreshInterval: 15_000 },
  );

  // Lifetime totals per asset for the summary cards. Kept separate from the
  // trade list above, which stays a bounded window for display.
  const { data: allStats = [] } = useSWR(
    ['tradelog-stats', assets.join(',')],
    async () => Promise.all(assets.map(a => getTradeStats(a).then(t => ({ asset: a, ...t })))),
    { refreshInterval: 15_000 },
  );

  const { data: allPositions = [], isLoading: positionsLoading } = useSWR(
    ['tradelog-positions', assets.join(',')],
    async () => {
      const results = await Promise.all(assets.map(a => getOpenPositions(a).then(rows => rows.map(r => ({ asset: a, ...r })))));
      return results.flat();
    },
    { refreshInterval: 15_000 },
  );

  const isLoading = tradesLoading || positionsLoading;

  // ── Build unified log ────────────────────────────────────────────────────────
  const allEntries = useMemo((): LogEntry[] => {
    const byAsset: Record<string, { trades: TradeRow[]; positions: OpenPositionRow[] }> = {};
    for (const a of assets) {
      byAsset[a] = { trades: [], positions: [] };
    }
    for (const t of allTrades as (TradeRow & { asset: string })[]) {
      if (byAsset[t.asset]) {
        const { asset: _a, ...row } = t as TradeRow & { asset: string };
        byAsset[t.asset].trades.push(row);
      }
    }
    for (const p of allPositions as (OpenPositionRow & { asset: string })[]) {
      if (byAsset[p.asset]) {
        const { asset: _a, ...row } = p as OpenPositionRow & { asset: string };
        byAsset[p.asset].positions.push(row);
      }
    }

    const entries: LogEntry[] = [];
    for (const a of assets) {
      entries.push(...assetToEntries(a, byAsset[a].trades, byAsset[a].positions));
    }
    entries.sort((a, b) => b.ts.getTime() - a.ts.getTime());
    return entries;
  }, [allTrades, allPositions, assets]);

  // ── Derived filter options ──────────────────────────────────────────────────
  const strategies = useMemo(() => {
    const set = new Set(allEntries.map(e => shortStrategy(e.strategy)));
    return ['all', ...Array.from(set).sort()];
  }, [allEntries]);

  // ── Apply filters ────────────────────────────────────────────────────────────
  const filtered = useMemo(() => {
    return allEntries.filter(e => {
      if (assetFilter    !== 'all' && e.shard                        !== assetFilter)    return false;
      if (statusFilter   !== 'all' && e.status                       !== statusFilter)   return false;
      if (strategyFilter !== 'all' && shortStrategy(e.strategy)      !== strategyFilter) return false;
      if (sideFilter     !== 'all' && e.side.toUpperCase()           !== sideFilter)     return false;
      return true;
    });
  }, [allEntries, assetFilter, statusFilter, strategyFilter, sideFilter]);

  // ── RTB handler ──────────────────────────────────────────────────────────────
  const handleRtbConfirm = async () => {
    if (DEMO_MODE) return;
    if (!rtbEntry?.rawPosition) return;
    setRtbLoading(true);
    try {
      const res = await fetch('/api/positions/manual-exit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          token_id:          rtbEntry.rawPosition.token_id,
          // The API's `asset` field is the shard/pool selector, not a market
          // attribute — keep sending the shard under its wire name.
          asset:             rtbEntry.shard,
          strategy:          rtbEntry.rawPosition.strategy,
          market:            rtbEntry.rawPosition.market,
          side:              rtbEntry.rawPosition.side,
          current_bid:       '0.5',
          verifying_contract: '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E',
        }),
      });
      if (!res.ok) {
        alert(`RTB failed: ${await res.text()}`);
      } else {
        alert('Position closed! Refreshing…');
        window.location.reload();
      }
    } catch (err) {
      alert(`RTB error: ${err}`);
    } finally {
      setRtbLoading(false);
      setRtbEntry(null);
    }
  };

  // ── Render ────────────────────────────────────────────────────────────────────
  return (
    <div className="space-y-5">

      {/* ── Summary stats ────────────────────────────────────────────────────── */}
      <SummaryBar
        entries={assetFilter === 'all' ? allEntries : allEntries.filter(e => e.shard === assetFilter)}
        stats={assetFilter === 'all' ? allStats : allStats.filter(s => s.asset === assetFilter)}
      />

      {/* ── Filters ──────────────────────────────────────────────────────────── */}
      <div className="card px-4 py-3 space-y-3">
        <div className="flex flex-wrap items-center gap-2">
          {/* Asset */}
          <span
            className="text-xs text-gray-500 font-mono mr-1 cursor-help"
            title="Which database the rows come from. On the intl venue this is the underlying asset; on Kalshi and US it is the venue."
          >Book:</span>
          <FilterPill label="All"         active={assetFilter === 'all'} onClick={() => setAssetFilter('all')} />
          {assets.map(a => (
            <FilterPill
              key={a}
              label={`${ASSET_EMOJI[a] ?? '◈'} ${a.toUpperCase()}`}
              active={assetFilter === a}
              onClick={() => setAssetFilter(a)}
            />
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {/* Status */}
          <span className="text-xs text-gray-500 font-mono mr-1">Status:</span>
          {[
            { v: 'all',       label: 'All' },
            { v: 'launch',    label: '🚀 Launches' },
            { v: 'inflight',  label: '✈️ In-Flight' },
            { v: 'completed', label: '🎯 Completed' },
          ].map(({ v, label }) => (
            <FilterPill key={v} label={label} active={statusFilter === v} onClick={() => setStatusFilter(v)} />
          ))}

          {/* Side */}
          <span className="text-xs text-gray-500 font-mono ml-4 mr-1">Side:</span>
          {['all', 'YES', 'NO'].map(s => (
            <FilterPill key={s} label={s === 'all' ? 'All' : s} active={sideFilter === s} onClick={() => setSideFilter(s)} />
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {/* Strategy */}
          <span className="text-xs text-gray-500 font-mono mr-1">Strategy:</span>
          {strategies.map(s => (
            <FilterPill
              key={s}
              label={s === 'all' ? 'All' : s}
              active={strategyFilter === s}
              onClick={() => setStrategyFilter(s)}
            />
          ))}
        </div>
      </div>

      {/* ── Table ────────────────────────────────────────────────────────────── */}
      <div className="card overflow-hidden">
        <div className="px-4 pt-3 pb-2 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-indigo-400 text-base">📋</span>
            <p className="label-muted">Mission Tradelog</p>
          </div>
          <div className="flex items-center gap-3">
            <ExportCsvButton assets={assets} />
            <span className="text-xs font-mono text-gray-600">
              {isLoading ? 'Loading…' : `${filtered.length} entries`}
              {filtered.length < allEntries.length && ` (filtered from ${allEntries.length})`}
            </span>
          </div>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center h-40 text-gray-600 text-sm">
            Loading tradelog across all assets…
          </div>
        ) : filtered.length === 0 ? (
          <div className="flex items-center justify-center h-40 text-gray-600 text-sm">
            No entries match the current filters.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono border-separate border-spacing-0 [&_td]:border-b [&_td]:border-[#1e1e32] [&_th]:border-b [&_th]:border-[#1e1e32]">
              <thead>
                <tr>
                  {/* Time is pinned left so a horizontally scrolled row stays
                      identifiable; Actions is pinned right because RTB closes a
                      live position and must never be scrolled out of reach. */}
                  <th className="sticky left-0 z-20 bg-[#13131f] px-3 py-2 text-left text-gray-500 font-normal whitespace-nowrap">
                    Time
                  </th>
                  {['Venue', 'Status', 'Strategy', 'Market', 'Size @ Entry → Exit', 'P&L', 'Reason / Mode'].map(h => (
                    <th key={h} className="px-3 py-2 text-left text-gray-500 font-normal whitespace-nowrap">
                      {h}
                    </th>
                  ))}
                  <th className="sticky right-0 z-20 bg-[#13131f] border-l border-[#1e1e32] px-3 py-2 text-left text-gray-500 font-normal whitespace-nowrap">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody>
                {filtered.map(e => {
                  const isLong   = ['YES', 'UP', 'BUY'].includes(e.side.toUpperCase());
                  const sm       = STATUS_META[e.status];
                  const assetCls = ASSET_COLOR[e.underlying ?? ''] ?? 'bg-gray-500/10 text-gray-300 border-gray-500/20';
                  const isOpen   = e.status !== 'completed';

                  return (
                    <tr
                      key={e.key}
                      className={[
                        'group hover:bg-[#1a1a2e] transition-colors',
                        e.status === 'launch'   ? 'opacity-70' : '',
                      ].join(' ')}
                    >
                      {/* Time — pinned left */}
                      <td className="sticky left-0 z-10 bg-[#13131f] group-hover:bg-[#1a1a2e] transition-colors px-3 py-2 text-gray-400 whitespace-nowrap">
                        {e.chainAdopted
                          ? <span className="text-amber-500/80 cursor-help" title="Re-adopted from on-chain wallet">⛓ adopted</span>
                          : fmtTime(e.ts)
                        }
                      </td>

                      {/* Venue */}
                      <td className="px-3 py-2 whitespace-nowrap">
                        <span className="inline-block px-1.5 py-0.5 text-[10px] font-bold rounded border bg-slate-500/10 text-slate-300 border-slate-500/20">
                          {e.venue ? (VENUE_LABEL[e.venue] ?? e.venue) : '—'}
                        </span>
                      </td>

                      {/* Status */}
                      <td className={`px-3 py-2 whitespace-nowrap ${sm.color}`}>
                        {sm.icon} {sm.label}
                      </td>

                      {/* Strategy */}
                      <td className="px-3 py-2 text-gray-300 whitespace-nowrap">
                        {shortStrategy(e.strategy)}
                      </td>

                      {/* Market, prefixed with the asset. The Subject column was
                          folded in here rather than dropped: it duplicates the
                          asset filter pills only while a filter is applied, and
                          on "All" it is the sole thing identifying the row. */}
                      <td className="px-3 py-2 text-gray-400 max-w-[190px]">
                        <span
                          className={`mr-1.5 inline-block px-1 py-0.5 text-[9px] font-bold rounded border align-middle ${assetCls}`}
                          title="Subject"
                        >
                          {subjectBadge(e)}
                        </span>
                        <TipCell full={e.market} maxChars={24} />
                      </td>

                      {/* Entry → Exit. Side rides along as a LABEL plus colour,
                          never colour alone, so it survives a mono display. */}
                      <td className="px-3 py-2 whitespace-nowrap">
                        <span className={`mr-1.5 text-[10px] font-bold ${isLong ? 'text-green-400' : 'text-red-400'}`}>
                          {e.side}
                        </span>
                        {/* Size rides with the price it was filled at — one
                            execution, one cell. Kept muted so the price journey
                            stays the thing the eye lands on. */}
                        <span className="text-gray-500" title={`${e.shares} shares`}>
                          {e.shares.toFixed(2)}
                        </span>
                        <span className="mx-1 text-gray-600">@</span>
                        <span className="text-gray-300">{e.entry.toFixed(4)}</span>
                        <span className="mx-1 text-gray-600">→</span>
                        {e.curOrExit !== null ? (() => {
                          const delta = e.curOrExit - e.entry;
                          const color = delta > 0 ? 'text-green-400' : delta < 0 ? 'text-red-400' : 'text-gray-300';
                          return (
                            <span className={color} title={isOpen ? 'Current mark price' : 'Exit price'}>
                              {e.curOrExit.toFixed(4)}
                              {isOpen && delta !== 0 && (
                                <span className="ml-1 opacity-60 text-[10px]">
                                  {delta > 0 ? '▲' : '▼'}
                                </span>
                              )}
                            </span>
                          );
                        })() : <span className="text-gray-600">—</span>}
                      </td>

                      {/* P&L, with fees folded in beneath it. The two belong
                          together: the headline figure is already NET of the
                          fee, so showing them apart invites double-counting. */}
                      <td className="px-3 py-2 font-semibold whitespace-nowrap">
                        <div>
                          {isOpen && e.curOrExit === null
                            ? <span className="text-gray-600">—</span>
                            : fmtPnl(e.pnl)
                          }
                          {isOpen && e.pnl !== null && (
                            <span className="ml-1 text-[10px] font-normal text-gray-600">(unrlzd)</span>
                          )}
                        </div>
                        {e.fees != null && e.fees > 0 && (
                          <div
                            className="text-[10px] font-normal text-gray-600 cursor-help"
                            title={`P&L is net of venue fees. Gross was ${(
                              (e.pnl ?? 0) + e.fees
                            ).toFixed(4)}.`}
                          >
                            net · ${e.fees.toFixed(4)} fees
                          </div>
                        )}
                      </td>

                      {/* Reason / Mode */}
                      <td className="px-3 py-2 text-gray-500 max-w-[180px]">
                        {e.status === 'completed' && e.reason ? (
                          <TipCell full={e.reason} maxChars={28} />
                        ) : e.ghost ? (
                          <span className="text-amber-400 opacity-70">👻 ghost</span>
                        ) : (
                          <span className="text-green-400 opacity-70">⚡ live</span>
                        )}
                      </td>

                      {/* Actions — pinned right */}
                      <td className="sticky right-0 z-10 bg-[#13131f] group-hover:bg-[#1a1a2e] transition-colors border-l border-[#1e1e32] px-3 py-2">
                        {e.status === 'inflight' && e.rawPosition && !DEMO_MODE && (
                          <button
                            onClick={() => setRtbEntry(e)}
                            className="px-2 py-0.5 text-[10px] rounded bg-orange-500/10 text-orange-300 border border-orange-500/30 hover:bg-orange-500/20 transition-colors"
                            title="Return to Base: manually close this position"
                          >
                            🛬 RTB
                          </button>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {/* Filtered P&L footer */}
        {!isLoading && filtered.length > 0 && (() => {
          const realized   = filtered.filter(e => e.status === 'completed').reduce((s, e) => s + (e.pnl ?? 0), 0);
          const unrealized = filtered.filter(e => e.status !== 'completed' && e.pnl !== null).reduce((s, e) => s + (e.pnl ?? 0), 0);
          const net = realized + unrealized;
          return (
            <div className="px-4 py-3 border-t border-[#1e1e32] flex flex-wrap gap-6 text-xs font-mono">
              <span className="text-gray-500">
                Realized: <span className={realized >= 0 ? 'text-green-400' : 'text-red-400'}>
                  {realized >= 0 ? '+' : ''}${realized.toFixed(4)}
                </span>
              </span>
              <span className="text-gray-500">
                Unrealized: <span className={unrealized >= 0 ? 'text-green-400' : 'text-red-400'}>
                  {unrealized >= 0 ? '+' : ''}${unrealized.toFixed(4)}
                </span>
              </span>
              <span className="text-gray-500">
                Net: <span className={`font-semibold ${net >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                  {net >= 0 ? '+' : ''}${net.toFixed(4)}
                </span>
              </span>
            </div>
          );
        })()}
      </div>

      {/* RTB Modal */}
      {rtbEntry && (
        <RtbModal
          entry={rtbEntry}
          onClose={() => setRtbEntry(null)}
          onConfirm={handleRtbConfirm}
          loading={rtbLoading}
        />
      )}
    </div>
  );
}

