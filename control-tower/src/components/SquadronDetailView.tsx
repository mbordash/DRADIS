'use client';

import { useCallback } from 'react';
import useSWR from 'swr';
import dynamic from 'next/dynamic';
import type { SquadronSummary, DynamicConfig } from '@/lib/types';
import {
  getConfig,
  getPnlHistory,
  getTrades,
  getOpenPositions,
  getStatus,
  patchConfig,
  getSquadronConfig,
  patchSquadronConfig,
  VIPER_DEFS,
} from '@/lib/api';
import ViperCard from '@/components/ViperCard';
import TradesTable from '@/components/TradesTable';
import OpenPositionsCard from '@/components/OpenPositionsCard';

const PnlChart = dynamic(() => import('@/components/PnlChart'), { ssr: false });

// ── Helpers ───────────────────────────────────────────────────────────────────

function fmt$(n: number) {
  return n.toLocaleString('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2 });
}

function fmtPct(n: number) {
  const sign = n >= 0 ? '+' : '';
  return `${sign}${(n * 100).toFixed(2)}%`;
}

// ── Raptor health panel ───────────────────────────────────────────────────────

function RaptorHealthPanel({
  raptors,
  asset,
}: {
  raptors?: Record<string, { price_connected: boolean; funding_connected: boolean }>;
  asset: string;
}) {
  const h = raptors?.[asset];
  const priceOk = h?.price_connected ?? false;
  const fundingOk = h?.funding_connected ?? false;

  return (
    <div className="card p-4">
      <p className="label-muted mb-3">Raptor Telemetry</p>
      {!h ? (
        <div className="text-xs font-mono text-gray-600">No health snapshot yet for {asset.toUpperCase()}.</div>
      ) : (
        <div className="space-y-2">
          <div className="flex items-center justify-between px-3 py-2 rounded-lg border border-[#1e1e32] bg-[#0d0d1a]">
            <div className="flex items-center gap-2">
              <span className={`h-2 w-2 rounded-full ${priceOk ? 'bg-cyan-400 animate-pulse' : 'bg-red-500'}`} />
              <span className="text-xs font-mono text-gray-300">Price Raptor</span>
            </div>
            <span className={`text-xs font-mono ${priceOk ? 'text-cyan-300' : 'text-red-400'}`}>
              {priceOk ? 'Connected' : 'Reconnecting'}
            </span>
          </div>
          <div className="flex items-center justify-between px-3 py-2 rounded-lg border border-[#1e1e32] bg-[#0d0d1a]">
            <div className="flex items-center gap-2">
              <span className={`h-2 w-2 rounded-full ${fundingOk ? 'bg-teal-400 animate-pulse' : 'bg-red-500'}`} />
              <span className="text-xs font-mono text-gray-300">Funding Raptor</span>
            </div>
            <span className={`text-xs font-mono ${fundingOk ? 'text-teal-300' : 'text-red-400'}`}>
              {fundingOk ? 'Connected' : 'Reconnecting'}
            </span>
          </div>
          <div className="text-[10px] font-mono text-gray-600 pt-1">
            Source: Binance Spot WS + Funding API
          </div>
        </div>
      )}
    </div>
  );
}

// ── Squadron info card ────────────────────────────────────────────────────────

const STATE_COLORS: Record<string, string> = {
  PATROLLING: 'text-green-400',
  DEPLOYED: 'text-blue-400',
  RTB: 'text-amber-400',
  STOOD_DOWN: 'text-red-400',
  STAGED: 'text-gray-500',
};

function SquadronInfoCard({ squadron }: { squadron: SquadronSummary }) {
  const stateColor = STATE_COLORS[squadron.state] ?? 'text-gray-400';
  return (
    <div className="card p-4">
      <p className="label-muted mb-3">Squadron Info</p>
      <div className="space-y-2 text-xs font-mono">
        <div className="flex justify-between">
          <span className="text-gray-500">Name</span>
          <span className="text-gray-200">{squadron.name}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-gray-500">Asset</span>
          <span className="text-gray-200">{squadron.asset}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-gray-500">State</span>
          <span className={stateColor}>{squadron.state}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-gray-500">Deployed</span>
          <span className="text-gray-400">{new Date(squadron.deployed_at).toLocaleString()}</span>
        </div>
        <div className="flex flex-col gap-1 pt-2 border-t border-[#1e1e32]">
          <span className="text-gray-500">Primary Market (Hourly)</span>
          <span className="text-gray-300 text-[11px] break-words">{squadron.market_name}</span>
        </div>
        {squadron.maker_market_name && (
          <div className="flex flex-col gap-1 pt-2 border-t border-[#1e1e32]">
            <span className="text-gray-500">Maker Market (Window/Daily)</span>
            <span className="text-gray-300 text-[11px] break-words">{squadron.maker_market_name}</span>
          </div>
        )}
        <div className="pt-2 border-t border-[#1e1e32]">
          <span className="text-gray-700 text-[10px]">ID: {squadron.id}</span>
        </div>
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

interface Props {
  squadron: SquadronSummary;
  onBack: () => void;
}

export default function SquadronDetailView({ squadron, onBack }: Props) {
  const asset = squadron.asset.toLowerCase();

  // ── Data fetching ──────────────────────────────────────────────────────────
  // Load squadron-specific config instead of global config
  const { data: config, mutate: refreshConfig } = useSWR(
    ['squadron-config', squadron.id],
    () => getSquadronConfig(squadron.id),
    { refreshInterval: 0, revalidateOnFocus: false }
  );

  const { data: pnl, isLoading: pnlLoading } = useSWR(
    ['pnl', asset],
    () => getPnlHistory(1440, asset),
    { refreshInterval: 60_000 }
  );

  const { data: trades, isLoading: tradesLoading } = useSWR(
    ['trades', asset],
    () => getTrades(60, asset),
    { refreshInterval: 15_000 }
  );

  const { data: openPositions, isLoading: positionsLoading } = useSWR(
    ['positions', asset],
    () => getOpenPositions(asset),
    { refreshInterval: 15_000 }
  );

  const { data: status } = useSWR('status', getStatus, { refreshInterval: 30_000 });

  // ── Derived stats ──────────────────────────────────────────────────────────
  const latestSnap = pnl?.[0];
  const oldestSnap = pnl?.[pnl.length - 1];
  const startingBal = oldestSnap ? parseFloat(oldestSnap.collateral) : 0;
  const sessionPnl = latestSnap ? parseFloat(latestSnap.session_pnl) : 0;
  const sessionPct = startingBal > 0 ? sessionPnl / startingBal : 0;
  const currentBal = config?.ghost_mode
    ? startingBal + sessionPnl
    : latestSnap
    ? parseFloat(latestSnap.collateral)
    : 0;

  // ── Handlers ───────────────────────────────────────────────────────────────
  const handlePatch = useCallback(
    async (patch: Partial<DynamicConfig>) => {
      await patchSquadronConfig(squadron.id, patch);
      await refreshConfig();
    },
    [squadron.id, refreshConfig]
  );

  return (
    <div className="space-y-6">
      {/* ── Back navigation ───────────────────────────────────────────────── */}
      <button
        onClick={onBack}
        className="flex items-center gap-2 text-sm font-mono text-indigo-400 hover:text-indigo-300 transition-colors"
      >
        <span>←</span>
        <span>Back to CAG Overview</span>
      </button>

      {/* ── Header banner ─────────────────────────────────────────────────── */}
      <div className="card px-5 py-4 border border-indigo-500/20 bg-[#0d0d1a]">
        <div className="flex items-center gap-3">
          <span className="text-2xl">✈️</span>
          <div>
            <h1 className="text-xl font-mono font-bold text-white">{squadron.name}</h1>
            <p className="text-xs font-mono text-gray-500 mt-0.5">
              {squadron.asset} Squadron · {squadron.state}
            </p>
          </div>
        </div>
      </div>

      {/* ── Squadron + Raptor info ────────────────────────────────────────── */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <SquadronInfoCard squadron={squadron} />
        <RaptorHealthPanel raptors={status?.raptors} asset={asset} />
      </div>

      {/* ── P&L stats for this asset ──────────────────────────────────────── */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div className="card px-4 py-3">
          <span className="label-muted">Session P&L</span>
          <span className={`stat-value ${sessionPnl >= 0 ? 'text-green-400' : 'text-red-400'}`}>
            {fmt$(sessionPnl)}
          </span>
          <span className="text-xs text-gray-500">{fmtPct(sessionPct)}</span>
        </div>
        <div className="card px-4 py-3">
          <span className="label-muted">Current Balance</span>
          <span className="stat-value">{pnlLoading ? '—' : fmt$(currentBal)}</span>
          <span className="text-xs text-gray-500">{config?.ghost_mode ? 'virtual' : 'pUSD'}</span>
        </div>
        <div className="card px-4 py-3">
          <span className="label-muted">Completed Trades</span>
          <span className="stat-value">{tradesLoading ? '—' : String(trades?.length ?? 0)}</span>
          <span className="text-xs text-gray-500">this session</span>
        </div>
        <div className="card px-4 py-3">
          <span className="label-muted">Open Positions</span>
          <span className="stat-value">{positionsLoading ? '—' : String(openPositions?.length ?? 0)}</span>
          <span className="text-xs text-gray-500">active</span>
        </div>
      </div>

      {/* ── P&L Chart ─────────────────────────────────────────────────────── */}
      {pnlLoading ? (
        <div className="card p-6 flex items-center justify-center h-48 text-gray-600 text-sm">
          Loading balance history…
        </div>
      ) : (
        <PnlChart data={pnl ?? []} startingBalance={startingBal} ghostMode={config?.ghost_mode} />
      )}

      {/* ── Viper Strategies ──────────────────────────────────────────────── */}
      <section>
        <div className="flex items-center justify-between mb-3">
          <p className="label-muted">Viper Layer (Active Strategies)</p>
          <div className="flex items-center gap-2">
            <span className="text-[10px] font-mono bg-indigo-500/10 text-indigo-300 border border-indigo-500/20 rounded px-2 py-0.5">
              🎯 Squadron-Scoped Config
            </span>
            <span className="text-xs text-gray-600 font-mono">
              {asset.toUpperCase()} execution configs
            </span>
          </div>
        </div>

        {/* Info banner explaining squadron configs */}
        <div className="mb-3 px-4 py-2 bg-indigo-500/5 border border-indigo-500/20 rounded-lg text-xs font-mono text-indigo-300">
          <span className="font-semibold">Squadron Config:</span> Changes here only affect this squadron.
          Each squadron has independent viper parameters.
        </div>

        {/* Active markets banner */}
        {status && (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mb-3">
            {[
              { label: '⏰ Hourly', key: 'time_decay' },
              { label: '🗓 Window / Daily', key: 'maker' },
            ].map(({ label, key }) => {
              const mkt = status.strategy_markets[key];
              return (
                <div
                  key={key}
                  className="flex items-start gap-2 bg-[#0d0d1a] border border-[#1e1e32] rounded-lg px-3 py-2"
                >
                  <span className="text-xs font-mono text-gray-500 whitespace-nowrap mt-0.5">{label}</span>
                  <span className="text-xs font-mono text-gray-300 truncate" title={mkt || undefined}>
                    {mkt || <span className="text-gray-700 italic">none</span>}
                  </span>
                </div>
              );
            })}
          </div>
        )}

        {config ? (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {VIPER_DEFS.map((v) => (
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
            Loading config…
          </div>
        )}
      </section>

      {/* ── Open Positions ────────────────────────────────────────────────── */}
      <section>
        <p className="label-muted mb-3">Open Positions ({asset.toUpperCase()})</p>
        <OpenPositionsCard positions={openPositions ?? []} isLoading={positionsLoading} />
      </section>

      {/* ── Recent Trades ─────────────────────────────────────────────────── */}
      <section>
        <p className="label-muted mb-3">Recent Trades ({asset.toUpperCase()})</p>
        {tradesLoading ? (
          <div className="card p-6 flex items-center justify-center h-32 text-gray-600 text-sm">
            Loading trades…
          </div>
        ) : (
          <TradesTable trades={trades ?? []} />
        )}
      </section>
    </div>
  );
}

