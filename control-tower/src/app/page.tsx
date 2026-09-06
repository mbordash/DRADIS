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

import { useCallback, useEffect, useRef, useState } from 'react';
import useSWR, { useSWRConfig } from 'swr';
import dynamic from 'next/dynamic';
import ChunkBoundary from '@/components/ChunkBoundary';

import ViperCard       from '@/components/ViperCard';
import LlmAdvisorCard  from '@/components/LlmAdvisorCard';
import OpenPositionsCard from '@/components/OpenPositionsCard';
import SquadronsPanel  from '@/components/SquadronsPanel';
import SquadronDetailView from '@/components/SquadronDetailView';
import TradelogPage    from '@/components/TradelogPage';
import SetupPage       from '@/components/SetupPage';
import AiActionsPage   from '@/components/AiActionsPage';
import ConsolePage     from '@/components/ConsolePage';
import AlphaGate       from '@/components/AlphaGate';
import VenueGate       from '@/components/VenueGate';
import ErrorBoundary   from '@/components/ErrorBoundary';
import Footer          from '@/components/Footer';
import { ViperHealthStrip } from '@/components/ViperHealthStrip';
import { getAssets, getConfig, getPnlHistory, getTrades, getOpenPositions, getHealth, patchConfig, VIPER_DEFS, getStatus, getLlmRecommendations, getLlmActions, getPortfolioValue, getSquadrons } from '@/lib/api';
import { DEMO_MODE } from '@/lib/demo';
import { getSetupStatus } from '@/lib/setupApi';
import type { DynamicConfig, SquadronSummary } from '@/lib/types';

// Recharts must be loaded client-side only
// Loading states are explicit: without one these render nothing at all while
// their chunk is in flight, which on a slow connection is indistinguishable
// from a broken page.
const PnlChart = dynamic(() => import('@/components/PnlChart'), {
  ssr: false,
  loading: () => (
    <div className="card p-6 flex items-center justify-center h-64 text-gray-600 text-sm">
      Loading portfolio history…
    </div>
  ),
});
const TelemetryPage = dynamic(() => import('@/components/TelemetryPage'), {
  ssr: false,
  loading: () => (
    <div className="card p-6 flex items-center justify-center h-64 text-gray-600 text-sm">
      Loading telemetry…
    </div>
  ),
});

// ── Helpers ───────────────────────────────────────────────────────────────────

function fmt$(n: number) {
  return n.toLocaleString('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2 });
}

function fmtPct(n: number) {
  const sign = n >= 0 ? '+' : '';
  return `${sign}${(n * 100).toFixed(2)}%`;
}

// ── Session time helpers ──────────────────────────────────────────────────────

/** Format an ISO-8601 session start as a short "HH:MM" local-time string. */
function fmtSessionTime(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString(undefined, {
      hour: '2-digit',
      minute: '2-digit',
    });
  } catch {
    return '—';
  }
}

/** Return a human-readable "Xh Ym" uptime string from an ISO-8601 start. */
function fmtUptime(iso: string): string {
  try {
    const secs = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
    if (secs < 60) return `${secs}s`;
    const mins = Math.floor(secs / 60);
    if (mins < 60) return `${mins}m`;
    const h = Math.floor(mins / 60);
    const m = mins % 60;
    return m > 0 ? `${h}h ${m}m` : `${h}h`;
  } catch {
    return '—';
  }
}

// ── Engine status badge ───────────────────────────────────────────────────────

/// Reachability of the engine's API, which is a different question from whether
/// the engine is trading real money — that is the GHOST/LIVE control beside it.
/// Both badges used to render the word "LIVE" whenever the engine was reachable
/// and not simulating, so the header read "LIVE LIVE" and neither word explained
/// which axis it described.
///
/// Three states, not two: SWR's `data` is `undefined` until the first poll
/// resolves, and treating that as `health !== 'ok'` painted a red OFFLINE on
/// every first render — an alarming way to say "not asked yet".
function EngineStatus({ health }: { health?: string }) {
  const state =
    health === undefined ? 'pending' : health === 'ok' ? 'up' : 'down';

  const { label, dot, text, pulse, title } = {
    up:      { label: 'UP',      dot: 'bg-green-400',  text: 'text-green-400',  pulse: true,
               title: 'Engine API reachable' },
    down:    { label: 'DOWN',    dot: 'bg-red-500',    text: 'text-red-400',    pulse: false,
               title: 'Engine API unreachable' },
    pending: { label: 'PENDING', dot: 'bg-amber-400',  text: 'text-amber-400',  pulse: true,
               title: 'Contacting the engine API' },
  }[state];

  return (
    <div className="flex items-center gap-1.5 cursor-default" title={title}>
      <span className={`h-2 w-2 rounded-full ${dot} ${pulse ? 'animate-pulse' : ''}`} />
      <span className={`text-xs font-mono ${text}`}>{label}</span>
    </div>
  );
}

// ── Session badge ─────────────────────────────────────────────────────────────

function SessionBadge({ startedAt }: { startedAt?: string }) {
  // Re-render every minute so the uptime counter stays current.
  const [, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 60_000);
    return () => clearInterval(id);
  }, []);

  if (!startedAt) return null;
  const uptime = fmtUptime(startedAt);
  const startTime = fmtSessionTime(startedAt);
  return (
    <div
      className="hidden sm:flex items-center gap-1.5 text-xs font-mono text-gray-500 cursor-default"
      title={`Session started: ${startedAt}`}
    >
      <span className="text-gray-600">⏱</span>
      <span>
        <span className="text-gray-500">Session</span>
        <span className="text-gray-400 ml-1">{startTime}</span>
        <span className="text-gray-600 ml-1">({uptime})</span>
      </span>
    </div>
  );
}

// ── Stat card ─────────────────────────────────────────────────────────────────

function StatCard({ label, value, sub, valueClass = '' }: {
  label: string; value: string; sub?: string; valueClass?: string;
}) {
  return (
    <div className="card px-4 py-3 flex flex-col gap-1">
      <span className="label-muted">{label}</span>
      <span className={`stat-value ${valueClass}`}>{value}</span>
      {sub && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  );
}

// ── Ghost mode banner ─────────────────────────────────────────────────────────

/// Order-book feeds that have stopped arriving.
///
/// Nothing else on this dashboard reports it. When the venue's book stops, the
/// API still answers 200, the squadron still reads PATROLLING and the Maker
/// still logs that it is quoting — every gate correctly declines an empty book,
/// and declining quietly looks exactly like a quiet market. An operator whose
/// connection drops would otherwise see a healthy engine that has silently
/// stopped being able to trade.
function DarkFeedBanner({ feeds }: { feeds?: { market: string; market_name?: string; dark_for_secs: number }[] }) {
  if (!feeds || feeds.length === 0) return null;
  return (
    <div className="card px-4 py-3 border border-red-500/30 bg-red-500/10">
      <div className="flex items-start gap-3">
        <span className="text-lg leading-none">📡</span>
        <div className="min-w-0">
          <p className="text-sm font-mono text-red-300">
            Market data has stopped for {feeds.length} market{feeds.length === 1 ? '' : 's'}
          </p>
          <p className="text-[11px] text-gray-400 mt-1 leading-relaxed">
            The engine is still running and your positions are untouched, but with no order book
            it cannot evaluate entries or exits. If other markets are still trading, this market
            simply has no book — a freshly rotated or untraded market often does. If every market
            is listed here, check network access to the venue.
          </p>
          <ul className="mt-2 space-y-0.5">
            {feeds.map(f => (
              <li key={f.market} className="text-[11px] font-mono text-red-400/90 truncate">
                {/* Name the market, not just the asset: "btc" alone reads as a broken
                    connection even when every other btc market is trading fine. */}
                {f.market_name ? `${f.market} — "${f.market_name}"` : f.market}
                {' '}— no book for {Math.floor(f.dark_for_secs / 60)}m {f.dark_for_secs % 60}s
              </li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
}

function GhostBanner({ ghost }: { ghost: boolean }) {
  return ghost ? (
    <div className="bg-amber-500/10 border border-amber-500/30 rounded-lg px-4 py-2 text-amber-300 text-xs font-mono flex items-center gap-2">
      <span className="text-base"></span>
      <span><strong>GHOST MODE ACTIVE</strong> — orders are simulated, no real CLOB calls.</span>
    </div>
  ) : null;
}

// ── Asset selector tabs ───────────────────────────────────────────────────────

const ASSET_EMOJI: Record<string, string> = {
  btc: '₿',
  eth: 'Ξ',
  sol: '◎',
};

function AssetTabs({
  assets,
  selected,
  onChange,
}: {
  assets: string[];
  selected: string;
  onChange: (a: string) => void;
}) {
  if (assets.length <= 1) return null;
  return (
    <div className="flex items-center gap-1">
      {assets.map((a) => {
        const active = a === selected;
        return (
          <button
            key={a}
            onClick={() => onChange(a)}
            className={[
              'flex items-center gap-1.5 text-xs font-mono px-3 py-1.5 rounded-lg border transition-colors',
              active
                ? 'bg-indigo-500/20 border-indigo-500/40 text-indigo-300'
                : 'bg-[#13131f] border-[#1e1e32] text-gray-500 hover:border-gray-600 hover:text-gray-300',
            ].join(' ')}
          >
            <span>{ASSET_EMOJI[a] ?? '◈'}</span>
            <span>{a.toUpperCase()}</span>
          </button>
        );
      })}
    </div>
  );
}

// ── Portfolio value banner ────────────────────────────────────────────────────

function PortfolioValueBanner({
  totalValue, collateral, strandedCollateral, positionsValue, unrealizedPnl,
  positionCount, sessionPnl, ghostMode, pricesLive, isLoading,
}: {
  totalValue: number; collateral: number; strandedCollateral: number; positionsValue: number;
  unrealizedPnl: number; positionCount: number; sessionPnl: number;
  ghostMode?: boolean; pricesLive: boolean; isLoading: boolean;
}) {
  // The true session delta is realized P&L + unrealized P&L.
  // This is correct whether or not positions were carried in from a prior session,
  // because it does NOT assume the starting portfolio was just cash — it derives
  // the starting portfolio value as (totalValue - delta) rather than using the
  // raw collateral snapshot which omits the cost basis of any open positions.
  const delta                = sessionPnl + unrealizedPnl;
  const startingPortfolioVal = totalValue - delta;
  const deltaPct             = startingPortfolioVal > 0 ? delta / startingPortfolioVal : 0;
  const isPositive           = delta >= 0;

  return (
    <div className="card px-5 py-4 flex flex-col sm:flex-row sm:items-center gap-3 border border-indigo-500/20 bg-[#0d0d1a]">
      {/* Main figure */}
      <div className="flex flex-col flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-0.5">
          <span className="label-muted text-xs">Portfolio Value</span>
          {!pricesLive && (
            <span className="text-[10px] font-mono bg-yellow-500/10 text-yellow-400 border border-yellow-500/20 rounded px-1.5 py-0.5">
              ⚡ cached prices
            </span>
          )}
          {ghostMode && (
            <span className="text-[10px] font-mono bg-amber-500/10 text-amber-400 border border-amber-500/20 rounded px-1.5 py-0.5">
              virtual
            </span>
          )}
        </div>
        <span className={`text-3xl font-mono font-bold tracking-tight ${isLoading ? 'text-gray-600' : 'text-white'}`}>
          {isLoading ? '——' : fmt$(totalValue)}
        </span>
        {!isLoading && startingPortfolioVal > 0 && (
          <span className={`text-sm font-mono mt-0.5 ${isPositive ? 'text-green-400' : 'text-red-400'}`}>
            {isPositive ? '▲' : '▼'} {fmt$(Math.abs(delta))} ({fmtPct(Math.abs(deltaPct))}) vs session start
          </span>
        )}
      </div>

      {/* Breakdown */}
      <div className="flex gap-4 sm:gap-6 text-xs font-mono flex-wrap">
        <div className="flex flex-col gap-0.5">
          <span className="text-gray-500">Cash</span>
          <span className="text-gray-300">{isLoading ? '—' : fmt$(collateral)}</span>
          {/* Settlement proceeds paid as USDC.e sit in the Safe until wrapped into
              pUSD; the exchange cannot see them, so they are shown here rather than
              folded into Cash. Not counted in Portfolio Value. */}
          {!isLoading && strandedCollateral > 0 && (
            <span
              className="text-amber-400"
              title="USDC.e settlement proceeds in your Safe, not yet wrapped into pUSD. Real cash, not tradeable, not counted above. Enable Collateral Sweep in Setup to wrap it."
            >
              + {fmt$(strandedCollateral)} unwrapped
            </span>
          )}
        </div>
        <div className="flex flex-col gap-0.5">
          <span className="text-gray-500">Positions</span>
          <span className="text-gray-300">{isLoading ? '—' : fmt$(positionsValue)}</span>
          {positionCount > 0 && <span className="text-gray-600">{positionCount} open</span>}
        </div>
        <div className="flex flex-col gap-0.5">
          <span className="text-gray-500">Unrealized P&L</span>
          <span className={isLoading ? 'text-gray-600' : unrealizedPnl >= 0 ? 'text-green-400' : 'text-red-400'}>
            {isLoading ? '—' : (unrealizedPnl >= 0 ? '+' : '') + fmt$(unrealizedPnl)}
          </span>
        </div>
      </div>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

// ── Top-level nav ─────────────────────────────────────────────────────────────

type AppView = 'main' | 'telemetry' | 'tradelog' | 'ai' | 'console' | 'setup';

/**
 * The app's location, encoded in the URL hash.
 *
 * Navigation was state-only, so the browser had no record of it: Back from a
 * squadron detail left DRADIS entirely rather than returning to the CAG, which
 * is a good way to lose your place mid-investigation. The hash keeps view and
 * focused squadron, so Back and Forward walk the trail, a reload lands where you
 * were, and a squadron page can be linked to directly.
 *
 * Hash rather than real paths because the Control Tower is served as a static
 * export with no server-side routing.
 */
function encodeRoute(view: AppView, squadronId: string | null): string {
  return squadronId ? `#${view}/squadron/${encodeURIComponent(squadronId)}` : `#${view}`;
}

function decodeRoute(hash: string): { view: AppView; squadronId: string | null } {
  const [view, kind, id] = hash.replace(/^#/, '').split('/');
  // An unknown view means a hand-edited or stale URL; fall back rather than
  // rendering nothing.
  const known = VIEW_DEFS.some(v => v.id === view);
  return {
    view: known ? (view as AppView) : 'main',
    squadronId: kind === 'squadron' && id ? decodeURIComponent(id) : null,
  };
}

const VIEW_DEFS: { id: AppView; label: string; icon: string }[] = [
  { id: 'main',      label: 'Main',       icon: '🗺️' },
  { id: 'telemetry', label: 'Telemetry',  icon: '📡' },
  { id: 'tradelog',  label: 'Tradelog',   icon: '📋' },
  { id: 'ai',        label: 'AI Actions', icon: '🤖' },
  { id: 'console',   label: 'Console',    icon: '🖥️' },
  { id: 'setup',     label: 'Setup',      icon: '⚙️' },
];

/** Compact main-menu dropdown — replaces the row of nav tabs so the header
 *  stays narrow as views are added. */
function NavMenu({
  active,
  onChange,
}: {
  active: AppView;
  onChange: (v: AppView) => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // Close on outside click / Escape.
  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', onClick);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onClick);
      document.removeEventListener('keydown', onKey);
    };
  }, [open]);

  const current = VIEW_DEFS.find(v => v.id === active) ?? VIEW_DEFS[0];

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen(v => !v)}
        className="flex items-center gap-1.5 text-xs font-mono px-3 py-1.5 rounded-lg border bg-indigo-500/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30 transition-colors"
        aria-haspopup="menu"
        aria-expanded={open}
      >
        <span>{current.icon}</span>
        <span>{current.label}</span>
        <span className={`text-[9px] transition-transform ${open ? 'rotate-180' : ''}`}>▼</span>
      </button>
      {open && (
        <div
          role="menu"
          className="absolute left-0 top-full mt-1 min-w-[11rem] rounded-lg border border-[#1e1e32] bg-[#0d0d16] shadow-xl shadow-black/50 py-1 z-50"
        >
          {VIEW_DEFS.map(t => (
            <button
              key={t.id}
              role="menuitem"
              onClick={() => { onChange(t.id); setOpen(false); }}
              className={[
                'w-full flex items-center gap-2 text-left text-xs font-mono px-3 py-2 transition-colors',
                active === t.id
                  ? 'bg-indigo-500/15 text-indigo-300'
                  : 'text-gray-400 hover:bg-[#13131f] hover:text-gray-200',
              ].join(' ')}
            >
              <span>{t.icon}</span>
              <span>{t.label}</span>
              {active === t.id && <span className="ml-auto text-[9px]">✓</span>}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

export default function DashboardPage() {
  // ── Top-level view (Main vs Tradelog) ───────────────────────────────────────
  const [activeView, setActiveView] = useState<AppView>('main');

  // ── Squadron drill-down state ────────────────────────────────────────────────
  const [focusedSquadronId, setFocusedSquadronId] = useState<string | null>(null);

  /** Move to a view (optionally a squadron) and record it in browser history. */
  const navigate = useCallback((view: AppView, squadronId: string | null = null) => {
    setActiveView(view);
    setFocusedSquadronId(squadronId);
    const next = encodeRoute(view, squadronId);
    if (typeof window !== 'undefined' && window.location.hash !== next) {
      window.history.pushState({ view, squadronId }, '', next);
    }
  }, []);

  // Adopt the URL on first paint, and follow Back/Forward thereafter. Deliberately
  // does not push: this reacts to history rather than adding to it.
  useEffect(() => {
    const apply = () => {
      const { view, squadronId } = decodeRoute(window.location.hash);
      setActiveView(view);
      setFocusedSquadronId(squadronId);
    };
    apply();
    window.addEventListener('popstate', apply);
    return () => window.removeEventListener('popstate', apply);
  }, []);

  // ── Asset selector — populated from GET /api/assets on first load ───────────
  const { data: availableAssets = [] } = useSWR('assets', getAssets, {
    refreshInterval: 0,
    revalidateOnFocus: false,
    // Seed a sensible default while the request is in-flight
    fallbackData: [],
  });

  // Active asset: default to first available or 'btc'.
  const [selectedAsset, setSelectedAsset] = useState<string>('');
  // Resolve the effective asset for API calls (empty string → primary pool).
  const asset = selectedAsset || availableAssets[0] || '';

  const { data: config, mutate: refreshConfig, isLoading: configLoading } =
    useSWR('config', getConfig, { refreshInterval: 0, revalidateOnFocus: false });

  // CAG-level P&L history: fetch global aggregated history (all assets) for main dashboard
  const { data: pnl, isLoading: pnlLoading } =
    useSWR('pnl-global', () => getPnlHistory(1440), { refreshInterval: 60_000 });

  const { data: trades, isLoading: tradesLoading } =
    useSWR(['trades', asset], () => getTrades(60, asset), { refreshInterval: 15_000 });

  // Open positions polled every 15s — same cadence as trades so the activity log stays fresh.
  const { data: openPositions, isLoading: positionsLoading } =
    useSWR(['positions', asset], () => getOpenPositions(asset), { refreshInterval: 15_000 });

  // For chart markers: fetch ALL trades/positions across all assets (not filtered by selected asset).
  // The asset list is part of the SWR key: a static key would cache the initial []
  // result (assets not loaded yet) and markers wouldn't render until the 15s refresh.
  const { data: allTrades } =
    useSWR(availableAssets.length > 0 ? ['trades-all', ...availableAssets] : null, async () => {
      const results = await Promise.all(availableAssets.map(a => getTrades(60, a)));
      return results.flat();
    }, { refreshInterval: 15_000 });

  const { data: allOpenPositions } =
    useSWR(availableAssets.length > 0 ? ['positions-all', ...availableAssets] : null, async () => {
      const results = await Promise.all(availableAssets.map(a => getOpenPositions(a)));
      return results.flat();
    }, { refreshInterval: 15_000 });

  const { data: health } =
    useSWR('health', getHealth, { refreshInterval: 10_000 });

  const { data: status } =
    useSWR('status', getStatus, { refreshInterval: 30_000 });

  // Refresh EVERY server-backed card the moment the engine restarts.
  //
  // This used to revalidate only `pnl-global` — the chart — because that was the
  // card the complaint named. It was the wrong scope. The number an operator
  // actually stares at after entering their venue credentials is the wallet
  // balance on the `portfolio` key, which polls on its own unsynchronized 30s
  // interval, and `config` does not poll at all (`refreshInterval: 0`). So the
  // balance card kept showing the pre-restart value while the chart beside it had
  // already caught up, which reads as the key not having worked. On a fresh
  // Ireland box the log showed "Starting portfolio value: $59.49" within the same
  // second while the card was still empty.
  //
  // A restart invalidates all server state at once, so revalidate all of it at
  // once rather than curating a list that the next new card will fall off.
  //
  // `session_started_at` changes on every engine restart, so this refetches once
  // and then every key goes back to its own cadence. Note this is the BACKSTOP:
  // `status` itself polls at 30s, so a restart can go unnoticed here for that
  // long. The Setup view, which knows precisely when the engine went down and
  // polls at 3s for its return, does the same revalidation immediately.
  const { mutate: mutateKey } = useSWRConfig();
  const sessionStartedAt = status?.session_started_at;
  useEffect(() => {
    if (!sessionStartedAt) return;
    mutateKey(() => true);
  }, [sessionStartedAt, mutateKey]);

  // Poll every 5 minutes — recommendations only arrive every 30 min at most.
  // Global LLM Advisor reads ALL asset databases and writes to primary pool,
  // so we fetch without an asset filter (always reads from primary).
  const { data: llmRecs, isLoading: llmLoading } =
    useSWR('llmRecs', () => getLlmRecommendations(10), { refreshInterval: 300_000 });

  // AI config proposals awaiting approval — poll faster (30 s): they're
  // TTL-bound; the Main strip shows a count, the AI Actions view the detail.
  const { data: llmActions } =
    useSWR('llmActions', () => getLlmActions(100), { refreshInterval: 30_000 });
  const pendingLlmCount = (llmActions ?? []).filter(a => a.status === 'proposed').length;

  // Portfolio value: collateral + live mark-to-market on open positions.
  // Refresh every 30 s so the number stays fresh without hammering Polymarket CLOB.
  const { data: portfolio, isLoading: portfolioLoading } =
    useSWR('portfolio', getPortfolioValue, { refreshInterval: 30_000 });

  // CAG squadron registry — refresh every 10 s to catch state transitions quickly.
  const { data: squadrons, isLoading: squadronsLoading } =
    useSWR('squadrons', getSquadrons, { refreshInterval: 10_000 });

  // Setup state — drives the "engine idle, complete Setup" first-run banner.
  //
  // Polled at the same cadence as trades and positions rather than the 60s it
  // used to use. This value's whole job is to make a warning disappear the
  // moment setup is complete, so a stale minute of "ENGINE IDLE — venue
  // credentials not configured" on a correctly configured instance is the
  // worst-case cost of the slower interval, and it lands on a first-run or
  // post-migration operator with no way to tell whether their import worked.
  // Explicit revalidation on import and on restart is the fast path; this is
  // the ceiling on how wrong the banner can be if either is missed.
  const { data: setupStatus, mutate: mutateSetupStatus } =
    useSWR(!DEMO_MODE ? 'setupStatus' : null, getSetupStatus, { refreshInterval: 15_000 });

  // ── First-run overlays, in order ──────────────────────────────────────────
  //
  // Venue BEFORE jurisdiction, and the order is load-bearing. The risk gate
  // records an acknowledgment stamped with the running venue, and that record is
  // write-once. Shown first, it made a US buyer accept International terms — for
  // a venue the same screen tells them they may not use — and filed that
  // permanently before they ever reached the venue switcher.
  const multiVenue = (setupStatus?.venues_available?.length ?? 0) > 1;

  // Set when the operator backs out of the acknowledgment to pick again. The
  // venue file on disk stays as it is — reopening the chooser is a UI decision,
  // and the choice is only rewritten if they actually confirm a different one.
  const [reselectVenue, setReselectVenue] = useState(false);

  const needsVenue =
    !!setupStatus &&
    multiVenue &&
    (!setupStatus.venue_selected || reselectVenue);

  const venueGate = needsVenue ? (
    <VenueGate
      available={setupStatus!.venues_available!}
      onChosen={() => { setReselectVenue(false); mutateSetupStatus(); }}
    />
  ) : null;

  // Only once a venue is settled — either chosen here, or the sole one this
  // image carries — does the jurisdiction gate have a venue worth naming.
  const alphaGate = setupStatus && !needsVenue && !setupStatus.alpha_ack ? (
    <AlphaGate
      venue={setupStatus.venue}
      appVersion={setupStatus.app_version}
      edition={setupStatus.edition}
      onAcknowledged={() => mutateSetupStatus()}
      onBack={multiVenue ? () => setReselectVenue(true) : undefined}
    />
  ) : null;

  // ── Stats derived from P&L history ──────────────────────────────────────────
  const latestSnap  = pnl?.[0];
  const oldestSnap  = pnl?.[pnl.length - 1];
  const startingBal = oldestSnap  ? parseFloat(oldestSnap.collateral)  : 0;
  const sessionPnl  = latestSnap  ? parseFloat(latestSnap.session_pnl) : 0;
  const sessionPct  = startingBal > 0 ? sessionPnl / startingBal : 0;
  // In ghost mode, the on-chain pUSD balance never changes (no real orders are placed),
  // so we derive the virtual current balance as startingBal + accumulated session P&L.
  // In live mode, use the actual on-chain collateral from the latest snapshot.
  const currentBal  = config?.ghost_mode
    ? startingBal + sessionPnl
    : (latestSnap ? parseFloat(latestSnap.collateral) : 0);

  // ── Patch handler ────────────────────────────────────────────────────────────
  const handlePatch = useCallback(async (patch: Partial<DynamicConfig>) => {
    if (DEMO_MODE) return;
    await patchConfig(patch);
    await refreshConfig();
  }, [refreshConfig]);


  // ── Squadron navigation ────────────────────────────────────────────────────
  const handleSquadronClick = useCallback((sq: SquadronSummary) => {
    navigate('main', sq.id);
  }, [navigate]);

  const handleBackToCag = useCallback(() => {
    navigate('main', null);
  }, [navigate]);

  const focusedSquadron = squadrons?.find((s) => s.id === focusedSquadronId);

  // ── Render squadron detail view if one is selected ─────────────────────────
  if (focusedSquadron) {
    return (
      <div className="min-h-screen bg-[#0a0a12]">
        {venueGate}
        {alphaGate}
        <header className="sticky top-0 z-10 border-b border-[#1e1e32] bg-[#0a0a12]/90 backdrop-blur-sm px-6 py-3">
          <div className="max-w-7xl mx-auto relative flex items-center justify-between gap-4">
            {/* Logo + nav */}
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-1.5">
                <span className="font-mono font-bold text-lg tracking-wide text-indigo-400">DRADIS</span>
                <span className="text-gray-600 text-lg">|</span>
              </div>
              <NavMenu active={activeView} onChange={(v) => navigate(v)} />
            </div>

            {/* Center — BSG motto */}
            <div className="absolute left-1/2 -translate-x-1/2 hidden md:block pointer-events-none select-none">
              <span className="font-serif italic text-gray-300 text-base tracking-wide">Good Hunting</span>
            </div>

            {/* Right cluster */}
            <div className="flex items-center gap-3">
              <SessionBadge startedAt={status?.session_started_at} />
              <EngineStatus health={health} />
              {config && !DEMO_MODE && (
                <button
                  onClick={() => handlePatch({ ghost_mode: !config.ghost_mode })}
                  className={[
                    'flex items-center gap-2 text-xs font-mono px-3 py-1.5 rounded-lg border transition-colors',
                    config.ghost_mode
                      ? 'bg-amber-500/10 border-amber-500/30 text-amber-300 hover:bg-amber-500/20'
                      : 'bg-indigo-500/15 border-indigo-400/50 text-indigo-200 hover:bg-indigo-500/25 hover:border-indigo-300 shadow-[0_0_10px_-2px_rgba(99,102,241,0.5)]',
                  ].join(' ')}
                >
                  <span>{config.ghost_mode ? '' : '⚡'}</span>
                  <span>{config.ghost_mode ? 'GHOST' : 'LIVE'}</span>
                </button>
              )}
            </div>
          </div>
        </header>

        <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6">
          {config?.ghost_mode && <GhostBanner ghost />}
          <DarkFeedBanner feeds={status?.dark_market_feeds} />
          <SquadronDetailView squadron={focusedSquadron} onBack={handleBackToCag} />
          <div className="mt-12"><Footer /></div>
        </main>
      </div>
    );
  }

  // ── Render CAG overview (default) ──────────────────────────────────────────

  return (
    <div className="min-h-screen bg-[#0a0a12]">
      {venueGate}
        {alphaGate}
      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <header className="sticky top-0 z-10 border-b border-[#1e1e32] bg-[#0a0a12]/90 backdrop-blur-sm px-6 py-3">
        <div className="max-w-7xl mx-auto relative flex items-center justify-between gap-4">
          {/* Logo + nav tabs */}
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-1.5">
              <span className="font-mono font-bold text-lg tracking-wide text-indigo-400">DRADIS</span>
              <span className="text-gray-600 text-lg">|</span>
            </div>
            <NavMenu active={activeView} onChange={(v) => navigate(v)} />
          </div>

          {/* Center — BSG motto */}
          <div className="absolute left-1/2 -translate-x-1/2 hidden md:block pointer-events-none select-none">
            <span className="font-serif italic text-gray-300 text-base tracking-wide">Good Hunting</span>
          </div>

          {/* Right cluster */}
          <div className="flex items-center gap-3">
            <SessionBadge startedAt={status?.session_started_at} />
            <EngineStatus health={health} />
            {config && !DEMO_MODE && (
              <button
                onClick={() => handlePatch({ ghost_mode: !config.ghost_mode })}
                className={[
                  'flex items-center gap-2 text-xs font-mono px-3 py-1.5 rounded-lg border transition-colors',
                  config.ghost_mode
                    ? 'bg-amber-500/10 border-amber-500/30 text-amber-300 hover:bg-amber-500/20'
                    : 'bg-indigo-500/15 border-indigo-400/50 text-indigo-200 hover:bg-indigo-500/25 hover:border-indigo-300 shadow-[0_0_10px_-2px_rgba(99,102,241,0.5)]',
                ].join(' ')}
              >
                <span>{config.ghost_mode ? '' : '⚡'}</span>
                <span>{config.ghost_mode ? 'GHOST' : 'LIVE'}</span>
              </button>
            )}
          </div>
        </div>
      </header>

      {/* ── First-run: venue credentials missing — engine is idle ─────────── */}
      {setupStatus && !setupStatus.venue_configured && activeView !== 'setup' && (
        <div className="max-w-7xl mx-auto px-4 sm:px-6 pt-4">
          <button
            onClick={() => navigate('setup')}
            className="w-full text-left bg-amber-500/10 border border-amber-500/30 rounded-xl px-4 py-3 text-xs font-mono text-amber-300 hover:bg-amber-500/20 transition-colors"
          >
            ⚠️ ENGINE IDLE — venue credentials not configured. DRADIS is running but cannot
            trade. Click here to open Setup, enter your credentials, and restart the engine.
          </button>
        </div>
      )}

      {/* ── Tradelog view ──────────────────────────────────────────────────── */}
      {activeView === 'tradelog' && (
        <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">
          {config?.ghost_mode && <GhostBanner ghost />}
          <DarkFeedBanner feeds={status?.dark_market_feeds} />
          <TradelogPage availableAssets={availableAssets} />
          <Footer />
        </main>
      )}

      {/* ── Telemetry view ─────────────────────────────────────────────────── */}
      {activeView === 'telemetry' && (
        <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">
          {config?.ghost_mode && <GhostBanner ghost />}
          <DarkFeedBanner feeds={status?.dark_market_feeds} />
          <ErrorBoundary label="Telemetry">
            <ChunkBoundary name="Telemetry">
              <TelemetryPage availableAssets={availableAssets} venue={setupStatus?.venue} />
            </ChunkBoundary>
          </ErrorBoundary>
          <Footer />
        </main>
      )}

      {/* ── AI Actions view ────────────────────────────────────────────────── */}
      {activeView === 'ai' && (
        <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">
          {config?.ghost_mode && <GhostBanner ghost />}
          <DarkFeedBanner feeds={status?.dark_market_feeds} />
          <ErrorBoundary label="AI Actions">
            <AiActionsPage />
          </ErrorBoundary>
          <Footer />
        </main>
      )}

      {/* ── Console view ───────────────────────────────────────────────────── */}
      {activeView === 'console' && (
        <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">
          <ErrorBoundary label="Console">
            <ConsolePage />
          </ErrorBoundary>
          <Footer />
        </main>
      )}

      {/* ── Setup view ─────────────────────────────────────────────────────── */}
      {activeView === 'setup' && (
        <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">
          <ErrorBoundary label="Setup">
            <SetupPage />
          </ErrorBoundary>
          <Footer />
        </main>
      )}

      {/* ── Main view ──────────────────────────────────────────────────────── */}
      {activeView === 'main' && (
      <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">

        {/* Ghost mode banner */}
        {config?.ghost_mode && <GhostBanner ghost />}
          <DarkFeedBanner feeds={status?.dark_market_feeds} />

        {/* ── Portfolio Value Banner ─────────────────────────────────── */}
        <PortfolioValueBanner
          totalValue={portfolioLoading ? 0 : parseFloat(portfolio?.total_value ?? '0')}
          collateral={portfolioLoading ? 0 : parseFloat(portfolio?.collateral ?? '0')}
          strandedCollateral={portfolioLoading ? 0 : parseFloat(portfolio?.stranded_collateral ?? '0')}
          positionsValue={portfolioLoading ? 0 : parseFloat(portfolio?.positions_value ?? '0')}
          unrealizedPnl={portfolioLoading ? 0 : parseFloat(portfolio?.unrealized_pnl ?? '0')}
          positionCount={portfolio?.position_count ?? 0}
          sessionPnl={pnlLoading ? 0 : sessionPnl}
          ghostMode={config?.ghost_mode}
          pricesLive={portfolio?.prices_live ?? true}
          isLoading={portfolioLoading}
        />

        {/* ── Portfolio History Chart (CAG-level) ───────────────────────── */}
        {pnlLoading ? (
          <div className="card p-6 flex items-center justify-center h-64 text-gray-600 text-sm">
            Loading portfolio history…
          </div>
        ) : (
          <ChunkBoundary name="Portfolio history">
            <PnlChart
              data={pnl ?? []}
              startingBalance={startingBal}
              ghostMode={config?.ghost_mode}
              currentPortfolio={portfolio}
              trades={allTrades ?? []}
              openPositions={allOpenPositions ?? []}
            />
          </ChunkBoundary>
        )}

        {/* ── CAG-level stats ───────────────────────────────────────────── */}
        {/* Three cards, not four: an "Active Assets" card used to sit here
            showing `availableAssets.length`, which is the number of open SQLite
            shards. On Polymarket International those shards are BTC/ETH/SOL so
            it read as assets, but on Polymarket US and Kalshi they are market
            wings plus the venue's own default shard — so it reported 4 for a
            venue with three wings and no multi-asset ops at all. */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          <StatCard
            label="Active Squadrons"
            value={String(squadrons?.filter(s => s.state === 'PATROLLING' || s.state === 'DEPLOYED').length ?? 0)}
            sub="deployed + patrolling"
          />
          <StatCard
            label="Session P&L"
            value={fmt$(sessionPnl)}
            sub={fmtPct(sessionPct)}
            valueClass={sessionPnl >= 0 ? 'text-green-400' : 'text-red-400'}
          />
          <StatCard
            label="Total Squadrons"
            value={String(squadrons?.length ?? 0)}
            sub="all states"
          />
        </div>

        {/* ── LLM Advisor (summary strip — detail lives in AI Actions) ──── */}
        <LlmAdvisorCard
          recommendations={llmRecs ?? []}
          isLoading={llmLoading}
          advisorEnabled={true}
          pendingCount={pendingLlmCount}
          onGoToActions={() => navigate('ai')}
        />

        {/* ── CAG Squadron Registry ─────────────────────────────────────── */}
        <section>
          <div className="flex items-center justify-between mb-3">
            <p className="label-muted">Squadron Registry</p>
            <span className="text-xs text-gray-600 font-mono">
              Click a squadron to view details, raptors, vipers, and trades
            </span>
          </div>
          <SquadronsPanel
            squadrons={squadrons ?? []}
            isLoading={squadronsLoading}
            onSquadronClick={handleSquadronClick}
          />
        </section>

        {/* ── Viper health rollup (per-squadron detail lives in drill-down) ── */}
        <ViperHealthStrip />

        {/* Footer */}
        <Footer />
      </main>
      )}
    </div>
  );
}
