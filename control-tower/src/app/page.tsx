'use client';

import { useCallback } from 'react';
import useSWR from 'swr';
import dynamic from 'next/dynamic';

import ViperCard       from '@/components/ViperCard';
import TradesTable     from '@/components/TradesTable';
import LlmAdvisorCard  from '@/components/LlmAdvisorCard';
import OpenPositionsCard from '@/components/OpenPositionsCard';
import SquadronsPanel  from '@/components/SquadronsPanel';
import { getConfig, getPnlHistory, getTrades, getOpenPositions, getHealth, patchConfig, VIPER_DEFS, getStatus, getLlmRecommendations, getPortfolioValue, getSquadrons } from '@/lib/api';
import type { DynamicConfig } from '@/lib/types';

// Recharts must be loaded client-side only
const PnlChart = dynamic(() => import('@/components/PnlChart'), { ssr: false });

// ── Helpers ───────────────────────────────────────────────────────────────────

function fmt$(n: number) {
  return n.toLocaleString('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2 });
}

function fmtPct(n: number) {
  const sign = n >= 0 ? '+' : '';
  return `${sign}${(n * 100).toFixed(2)}%`;
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

function GhostBanner({ ghost }: { ghost: boolean }) {
  return ghost ? (
    <div className="bg-amber-500/10 border border-amber-500/30 rounded-lg px-4 py-2 text-amber-300 text-xs font-mono flex items-center gap-2">
      <span className="text-base"></span>
      <span><strong>GHOST MODE ACTIVE</strong> — orders are simulated, no real CLOB calls.</span>
    </div>
  ) : null;
}

// ── Portfolio value banner ────────────────────────────────────────────────────

function PortfolioValueBanner({
  totalValue, collateral, positionsValue, unrealizedPnl,
  positionCount, startingBal, ghostMode, pricesLive, isLoading,
}: {
  totalValue: number; collateral: number; positionsValue: number;
  unrealizedPnl: number; positionCount: number; startingBal: number;
  ghostMode?: boolean; pricesLive: boolean; isLoading: boolean;
}) {
  const delta      = totalValue - startingBal;
  const deltaPct   = startingBal > 0 ? delta / startingBal : 0;
  const isPositive = delta >= 0;

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
        {!isLoading && startingBal > 0 && (
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

export default function DashboardPage() {
  const { data: config, mutate: refreshConfig, isLoading: configLoading } =
    useSWR('config', getConfig, { refreshInterval: 0, revalidateOnFocus: false });

  const { data: pnl, isLoading: pnlLoading } =
    useSWR('pnl', () => getPnlHistory(200), { refreshInterval: 60_000 });

  const { data: trades, isLoading: tradesLoading } =
    useSWR('trades', () => getTrades(60), { refreshInterval: 15_000 });

  // Open positions polled every 15s — same cadence as trades so the activity log stays fresh.
  const { data: openPositions, isLoading: positionsLoading } =
    useSWR('positions', getOpenPositions, { refreshInterval: 15_000 });

  const { data: health } =
    useSWR('health', getHealth, { refreshInterval: 10_000 });

  const { data: status } =
    useSWR('status', getStatus, { refreshInterval: 30_000 });

  // Poll every 5 minutes — recommendations only arrive every 30 min at most.
  const { data: llmRecs, isLoading: llmLoading } =
    useSWR('llmRecs', () => getLlmRecommendations(10), { refreshInterval: 300_000 });

  // Portfolio value: collateral + live mark-to-market on open positions.
  // Refresh every 30 s so the number stays fresh without hammering Polymarket CLOB.
  const { data: portfolio, isLoading: portfolioLoading } =
    useSWR('portfolio', getPortfolioValue, { refreshInterval: 30_000 });

  // CAG squadron registry — refresh every 10 s to catch state transitions quickly.
  const { data: squadrons, isLoading: squadronsLoading } =
    useSWR('squadrons', getSquadrons, { refreshInterval: 10_000 });

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
    await patchConfig(patch);
    await refreshConfig();
  }, [refreshConfig]);

  const isConnected = health === 'ok';

  return (
    <div className="min-h-screen bg-[#0a0a12]">
      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <header className="sticky top-0 z-10 border-b border-[#1e1e32] bg-[#0a0a12]/90 backdrop-blur-sm px-6 py-3">
        <div className="max-w-7xl mx-auto relative flex items-center justify-between gap-4">
          {/* Logo */}
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-1.5">
              <span className="font-mono font-bold text-lg tracking-wide text-indigo-400">DRADIS</span>
              <span className="text-gray-600 text-lg">|</span>
              <span className="text-gray-400 text-sm font-medium">Control Tower</span>
            </div>
            <span className="hidden sm:inline text-xs bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 rounded px-2 py-0.5 font-mono">
              v0.2.0
            </span>
          </div>

          {/* Center — BSG motto */}
          <div className="absolute left-1/2 -translate-x-1/2 hidden md:block pointer-events-none select-none">
            <span className="font-serif italic text-gray-300 text-base tracking-wide">Good Hunting</span>
          </div>

          {/* Right cluster */}
          <div className="flex items-center gap-4">
            {/* API status */}
            <div className="flex items-center gap-1.5">
              <span className={`h-2 w-2 rounded-full ${isConnected ? 'bg-green-400 animate-pulse' : 'bg-red-500'}`} />
              <span className={`text-xs font-mono ${isConnected ? 'text-green-400' : 'text-red-400'}`}>
                {isConnected ? 'LIVE' : 'OFFLINE'}
              </span>
            </div>

            {/* Ghost mode toggle */}
            {config && (
              <button
                onClick={() => handlePatch({ ghost_mode: !config.ghost_mode })}
                className={[
                  'flex items-center gap-2 text-xs font-mono px-3 py-1.5 rounded-lg border transition-colors',
                  config.ghost_mode
                    ? 'bg-amber-500/10 border-amber-500/30 text-amber-300 hover:bg-amber-500/20'
                    : 'bg-[#13131f] border-[#1e1e32] text-gray-400 hover:border-gray-600',
                ].join(' ')}
              >
                <span>{config.ghost_mode ? '' : '⚡'}</span>
                <span>{config.ghost_mode ? 'GHOST' : 'LIVE'}</span>
              </button>
            )}
          </div>
        </div>
      </header>

      {/* ── Body ───────────────────────────────────────────────────────────── */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">

        {/* Ghost mode banner */}
        {config?.ghost_mode && <GhostBanner ghost />}

        {/* ── Portfolio Value Banner ─────────────────────────────────── */}
        <PortfolioValueBanner
          totalValue={portfolioLoading ? 0 : parseFloat(portfolio?.total_value ?? '0')}
          collateral={portfolioLoading ? 0 : parseFloat(portfolio?.collateral ?? '0')}
          positionsValue={portfolioLoading ? 0 : parseFloat(portfolio?.positions_value ?? '0')}
          unrealizedPnl={portfolioLoading ? 0 : parseFloat(portfolio?.unrealized_pnl ?? '0')}
          positionCount={portfolio?.position_count ?? 0}
          startingBal={startingBal}
          ghostMode={config?.ghost_mode}
          pricesLive={portfolio?.prices_live ?? true}
          isLoading={portfolioLoading}
        />

        {/* ── Stats row ─────────────────────────────────────────────────── */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <StatCard
            label="Session P&L"
            value={fmt$(sessionPnl)}
            sub={fmtPct(sessionPct)}
            valueClass={sessionPnl >= 0 ? 'text-green-400' : 'text-red-400'}
          />
          <StatCard
            label="Current Balance"
            value={pnlLoading ? '—' : fmt$(currentBal)}
            sub={config?.ghost_mode ? 'virtual pUSD' : 'pUSD'}
          />
          <StatCard
            label="Starting Balance"
            value={pnlLoading ? '—' : fmt$(startingBal)}
            sub="session start"
          />
          <StatCard
            label="Trades This Session"
            value={tradesLoading ? '—' : String(trades?.length ?? 0)}
            sub="completed round-trips"
          />
        </div>

        {/* ── P&L Chart ─────────────────────────────────────────────────── */}
        {pnlLoading ? (
          <div className="card p-6 flex items-center justify-center h-48 text-gray-600 text-sm">
            Loading balance history…
          </div>
        ) : (
        <PnlChart data={pnl ?? []} startingBalance={startingBal} ghostMode={config?.ghost_mode} />
        )}

        {/* ── LLM Advisor ───────────────────────────────────────────────── */}
        <LlmAdvisorCard
          recommendations={llmRecs ?? []}
          isLoading={llmLoading}
          advisorEnabled={true}
        />

        {/* ── Viper Strategies ──────────────────────────────────────────── */}
        <section>
          <div className="flex items-center justify-between mb-3">
            <p className="label-muted">Viper Strategies</p>
            {configLoading && (
              <span className="text-xs text-gray-600 font-mono animate-pulse">loading…</span>
            )}
          </div>

          {/* Active markets banner */}
          {status && (
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mb-3">
              {[
                { label: '⏰ Hourly', key: 'time_decay' },
                { label: ' Window / Daily', key: 'maker' },
              ].map(({ label, key }) => {
                const mkt = status.strategy_markets[key];
                return (
                  <div
                    key={key}
                    className="flex items-start gap-2 bg-[#0d0d1a] border border-[#1e1e32] rounded-lg px-3 py-2"
                  >
                    <span className="text-xs font-mono text-gray-500 whitespace-nowrap mt-0.5">{label}</span>
                    <span
                      className="text-xs font-mono text-gray-300 truncate"
                      title={mkt || undefined}
                    >
                      {mkt || <span className="text-gray-700 italic">none</span>}
                    </span>
                  </div>
                );
              })}
            </div>
          )}

          {config ? (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {VIPER_DEFS.map(v => (
                <ViperCard
                  key={v.name}
                  viper={v}
                  config={config}
                  onPatch={handlePatch}
                  market={status?.strategy_markets[v.statusKey]}
                />
              ))}
            </div>
          ) : (
            <div className="card p-6 flex items-center justify-center h-32 text-gray-600 text-sm">
              {isConnected ? 'Loading config…' : 'API offline — start DRADIS first.'}
            </div>
          )}
        </section>

        {/* ── CAG Squadron Registry ─────────────────────────────────────── */}
        <section>
          <p className="label-muted mb-3">Squadron Registry</p>
          <SquadronsPanel
            squadrons={squadrons ?? []}
            isLoading={squadronsLoading}
          />
        </section>

        {/* ── Open Positions ────────────────────────────────────────────── */}
        <section>
          <p className="label-muted mb-3">Open Positions</p>
          <OpenPositionsCard
            positions={openPositions ?? []}
            isLoading={positionsLoading}
          />
        </section>

        {/* ── Recent Trades ─────────────────────────────────────────────── */}
        <section>
          <p className="label-muted mb-3">Recent Trades</p>
          {tradesLoading ? (
            <div className="card p-6 flex items-center justify-center h-32 text-gray-600 text-sm">
              Loading trades…
            </div>
          ) : (
            <TradesTable trades={trades ?? []} />
          )}
        </section>


        {/* Footer */}
        <footer className="text-center text-xs text-gray-700 pb-4 font-mono">
          DRADIS Control Tower · Polymarket CLOB Orchestrator ·{' '}
          <span className="text-gray-600">So say we all.</span>
        </footer>
      </main>
    </div>
  );
}

