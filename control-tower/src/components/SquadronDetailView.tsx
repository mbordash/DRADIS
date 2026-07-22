'use client';

import { useCallback } from 'react';
import useSWR from 'swr';
import type { SquadronSummary, DynamicConfig, AssetRaptorHealth } from '@/lib/types';
import {
  getTrades,
  getOpenPositions,
  getStatus,
  getSquadronConfig,
  patchSquadronConfig,
  VIPER_DEFS,
} from '@/lib/api';
import ViperCard from '@/components/ViperCard';
import OpenPositionsCard from '@/components/OpenPositionsCard';
import { DEMO_MODE } from '@/lib/demo';

// ── Raptor health panel ───────────────────────────────────────────────────────

/** Display metadata per raptor kind. `flag` ties the kind to its health field
 *  in the /api/status raptor map; kinds without a flag (future sports/politics)
 *  render as "Pending" until their feed publishes health. */
const RAPTOR_META: Record<
  string,
  {
    label: string;
    flag?: 'price_connected' | 'funding_connected' | 'deriv_connected' | 'tide_connected' | 'sports_connected' | 'horizon_connected';
    dot: string; text: string; source: string;
    /** Health-map key to read this raptor's flag from, when it differs from the
     *  squadron's asset (e.g. the venue-neutral Sports Raptor publishes under "sports"). */
    healthKey?: string;
    /** When the feed is expected to be intermittently offline (e.g. off-hours),
     *  render the disconnected state as a neutral idle badge rather than a red error. */
    offlineText?: string; offlineDot?: string; offlineClass?: string;
  }
> = {
  price:   { label: 'Price Raptor',   flag: 'price_connected',   dot: 'bg-cyan-400', text: 'text-cyan-300', source: 'Binance Spot WS' },
  funding: { label: 'Funding Raptor', flag: 'funding_connected', dot: 'bg-teal-400', text: 'text-teal-300', source: 'Binance Funding API' },
  derivatives: { label: 'Derivatives Raptor', flag: 'deriv_connected', dot: 'bg-amber-400', text: 'text-amber-300', source: 'Binance FAPI (OI + CVD)' },
  tide:    {
    label: 'Tide Raptor', flag: 'tide_connected', dot: 'bg-sky-400', text: 'text-sky-300',
    source: 'Alpaca IEX (ETF iNAV)',
    offlineText: 'Idle (off-hours)', offlineDot: 'bg-gray-600', offlineClass: 'text-gray-500',
  },
  horizon: {
    label: 'Horizon Raptor', flag: 'horizon_connected', dot: 'bg-orange-400', text: 'text-orange-300',
    source: 'Alpaca IEX (SPY/QQQ/UVXY)',
    // Macro raptor — publishes health under the "btc" key regardless of squadron asset.
    healthKey: 'btc',
    offlineText: 'Idle (off-hours)', offlineDot: 'bg-gray-600', offlineClass: 'text-gray-500',
  },
  sports:  {
    label: 'Sports Raptor', flag: 'sports_connected', dot: 'bg-fuchsia-400', text: 'text-fuchsia-300',
    source: 'The Odds API (line movement)', healthKey: 'sports',
    offlineText: 'Idle (no key)', offlineDot: 'bg-gray-600', offlineClass: 'text-gray-500',
  },
};

function RaptorHealthPanel({
  raptorKinds,
  raptors,
  asset,
  marketClass,
}: {
  raptorKinds: string[];
  raptors?: Record<string, AssetRaptorHealth>;
  asset: string;
  marketClass: string;
}) {
  const h = raptors?.[asset];

  return (
    <div className="card p-4">
      <p className="label-muted mb-3">Raptor Telemetry</p>
      {raptorKinds.length === 0 ? (
        <div className="text-xs font-mono text-gray-600">
          No raptors linked to the{' '}
          <span className="text-gray-300">{marketClass || 'unknown'}</span> market class yet.
        </div>
      ) : (
        <div className="space-y-2">
          {raptorKinds.map((kind) => {
            const meta = RAPTOR_META[kind];
            const label = meta?.label ?? `${kind.charAt(0).toUpperCase()}${kind.slice(1)} Raptor`;
            // Implemented raptors with a health flag report live connection;
            // any without (roadmapped kinds) show as pending.
            const hasFlag = !!meta?.flag;
            const src = meta?.healthKey ? raptors?.[meta.healthKey] : h;
            const connected = hasFlag ? (src?.[meta!.flag!] ?? false) : false;
            // A feed with an `offlineText` (e.g. Tide off-hours) shows a neutral
            // idle badge when down rather than a red "Reconnecting" error.
            const idleStyle = !connected && meta?.offlineText;
            const dot = !hasFlag
              ? 'bg-gray-600'
              : connected
                ? `${meta!.dot} animate-pulse`
                : idleStyle ? (meta!.offlineDot ?? 'bg-gray-600') : 'bg-red-500';
            const statusText = !hasFlag
              ? 'Pending'
              : connected
                ? 'Connected'
                : idleStyle ? meta!.offlineText! : 'Reconnecting';
            const statusClass = !hasFlag
              ? 'text-gray-500'
              : connected
                ? meta!.text
                : idleStyle ? (meta!.offlineClass ?? 'text-gray-500') : 'text-red-400';
            return (
              <div
                key={kind}
                className="flex items-center justify-between px-3 py-2 rounded-lg border border-[#1e1e32] bg-[#0d0d1a]"
              >
                <div className="flex items-center gap-2">
                  <span className={`h-2 w-2 rounded-full ${dot}`} />
                  <span className="text-xs font-mono text-gray-300">{label}</span>
                </div>
                <span className={`text-xs font-mono ${statusClass}`}>{statusText}</span>
              </div>
            );
          })}
          {(() => {
            const sources = raptorKinds.map((k) => RAPTOR_META[k]?.source).filter(Boolean);
            return sources.length > 0 ? (
              <div className="text-[10px] font-mono text-gray-600 pt-1">
                Source: {sources.join(' + ')}
              </div>
            ) : null;
          })()}
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
        {squadron.market_class && (
          <div className="flex justify-between">
            <span className="text-gray-500">Market Class</span>
            <span className="text-indigo-300 capitalize">{squadron.market_class}</span>
          </div>
        )}
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

  // Market taxonomy resolved by the backend (data-driven; falls back to the
  // full set if an older backend didn't supply it).
  const raptorKinds = squadron.raptors ?? [];
  const marketClass = squadron.market_class ?? 'unknown';
  const activeVipers =
    squadron.vipers && squadron.vipers.length > 0
      ? VIPER_DEFS.filter((v) => squadron.vipers!.includes(v.statusKey))
      : VIPER_DEFS;

  // ── Data fetching ──────────────────────────────────────────────────────────
  // Load squadron-specific config instead of global config
  const { data: config, mutate: refreshConfig } = useSWR(
    ['squadron-config', squadron.id],
    () => getSquadronConfig(squadron.id),
    { refreshInterval: 0, revalidateOnFocus: false }
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


  // ── Handlers ───────────────────────────────────────────────────────────────
  const handlePatch = useCallback(
    async (patch: Partial<DynamicConfig>) => {
      if (DEMO_MODE) return;
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
        <RaptorHealthPanel
          raptorKinds={raptorKinds}
          raptors={status?.raptors}
          asset={asset}
          marketClass={marketClass}
        />
      </div>

      {/* ── Performance stats for this squadron/asset ─────────────────────── */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div className="card px-4 py-3 flex flex-col gap-1">
          <span className="label-muted">Completed Trades</span>
          <span className="stat-value">{tradesLoading ? '—' : String(trades?.length ?? 0)}</span>
          <span className="text-xs text-gray-500">this session</span>
        </div>
        <div className="card px-4 py-3 flex flex-col gap-1">
          <span className="label-muted">Open Positions</span>
          <span className="stat-value">{positionsLoading ? '—' : String(openPositions?.length ?? 0)}</span>
          <span className="text-xs text-gray-500">active now</span>
        </div>
        <div className="card px-4 py-3 flex flex-col gap-1">
          <span className="label-muted">Win Rate</span>
          <span className="stat-value text-gray-600">—</span>
          <span className="text-xs text-gray-500">coming soon</span>
        </div>
        <div className="card px-4 py-3 flex flex-col gap-1">
          <span className="label-muted">Avg Trade P&L</span>
          <span className="stat-value text-gray-600">—</span>
          <span className="text-xs text-gray-500">coming soon</span>
        </div>
      </div>

      {/* ── Viper Strategies ──────────────────────────────────────────────── */}
      <section>
        <div className="flex items-center justify-between mb-3">
          <p className="label-muted">Viper Layer (Active Strategies)</p>
          <div className="flex items-center gap-2">
            <span className="text-[10px] font-mono bg-indigo-500/10 text-indigo-300 border border-indigo-500/20 rounded px-2 py-0.5">
               Squadron-Scoped Config
            </span>
            <span className="text-xs text-gray-600 font-mono">
              {asset.toUpperCase()} execution configs
            </span>
          </div>
        </div>

        {/* Info banner explaining squadron configs */}
        <div className="mb-3 px-4 py-2 bg-indigo-500/5 border border-indigo-500/20 rounded-lg text-xs font-mono text-indigo-300">
          <span className="font-semibold">Squadron Config:</span> Changes here only affect this squadron.
          Vipers shown are those linked to the{' '}
          <span className="capitalize text-indigo-200">{marketClass}</span> market class.
        </div>

        {config ? (
          activeVipers.length > 0 ? (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {activeVipers.map((v) => (
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
              No vipers linked to the {marketClass} market class.
            </div>
          )
        ) : (
          <div className="card p-6 flex items-center justify-center h-32 text-gray-600 text-sm">
            Loading config…
          </div>
        )}
      </section>

      {/* ── Open Positions & Trades ───────────────────────────────────────── */}
      <section>
        <p className="label-muted mb-3">Mission Activity ({asset.toUpperCase()})</p>
        <OpenPositionsCard
          positions={openPositions ?? []}
          trades={trades ?? []}
          isLoading={positionsLoading || tradesLoading}
          asset={asset}
        />
      </section>
    </div>
  );
}

