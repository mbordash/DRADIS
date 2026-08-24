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

import { useCallback, useState } from 'react';
import useSWR from 'swr';
import type { SquadronSummary, DynamicConfig, AssetRaptorHealth } from '@/lib/types';
import {
  getTrades,
  getTradeStats,
  getOpenPositions,
  getStatus,
  getSquadronConfig,
  getVipersStatus,
  patchSquadronConfig,
  getConfigSchema,
  standDownSquadron,
  VIPER_DEFS,
} from '@/lib/api';
import ViperCard, { fmtAgo, STALE_EVAL_SECS } from '@/components/ViperCard';
import { AdvancedRow } from '@/components/AdvancedConfigModal';
import OpenPositionsCard from '@/components/OpenPositionsCard';
import { useConfirm } from '@/components/ConfirmDialog';
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
          <span className="text-gray-500">
            {squadron.asset.toLowerCase().startsWith('us') ? 'Active Market' : 'Primary Market (Hourly)'}
          </span>
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

/**
 * Config groups that belong to the squadron rather than to any single viper.
 *
 * The schema is rendered by matching `group` against a viper's name, so a group
 * named for something other than a viper had no home and simply never appeared —
 * "Order Book" and "Exit Accounting" were registered in the Rust schema and
 * unreachable in the UI. Listed explicitly rather than inferred as "not a viper
 * name", because a squadron only carries the vipers of its market class: a
 * politics squadron has no Momentum card, and inferring would then treat
 * Momentum's own fields as squadron-wide.
 */
const SQUADRON_GROUPS = ['Order Book', 'Exit Accounting'];

function SquadronSettingsCard({
  config,
  onPatch,
}: {
  config: DynamicConfig;
  onPatch: (patch: Partial<DynamicConfig>) => Promise<void>;
}) {
  const { data: schema } = useSWR('configSchema', getConfigSchema);
  const fields = (schema ?? []).filter(f => SQUADRON_GROUPS.includes(f.group));
  if (fields.length === 0) return null;

  return (
    <div className="card p-4 space-y-3">
      <div>
        <h3 className="text-sm font-mono text-gray-200">Squadron settings</h3>
        <p className="text-[11px] text-gray-500 mt-1 leading-relaxed">
          Apply to every viper in this squadron. Changing them here affects only this
          squadron, so a setting can be tried on one market class and compared against
          the others.
        </p>
      </div>
      {SQUADRON_GROUPS.map(group => {
        const inGroup = fields.filter(f => f.group === group);
        if (inGroup.length === 0) return null;
        return (
          <div key={group} className="space-y-2">
            <p className="text-[10px] font-mono uppercase tracking-wide text-gray-600">{group}</p>
            {inGroup.map(f => (
              <AdvancedRow
                key={f.key}
                field={f}
                config={config}
                onPatch={onPatch}
                disabled={DEMO_MODE}
              />
            ))}
          </div>
        );
      })}
    </div>
  );
}

export default function SquadronDetailView({ squadron, onBack }: Props) {
  const asset = squadron.asset.toLowerCase();
  // Raptor health is keyed by crypto underlying (btc/eth/sol), which may
  // differ from the squadron's venue asset (e.g. "kalshi"). Fall back to
  // asset for older backends that don't send `underlying`.
  const raptorAsset = (squadron.underlying || asset).toLowerCase();

  // Market taxonomy resolved by the backend (data-driven; falls back to the
  // full set if an older backend didn't supply it).
  const raptorKinds = squadron.raptors ?? [];
  const marketClass = squadron.market_class ?? 'unknown';
  const activeVipers =
    squadron.vipers && squadron.vipers.length > 0
      ? VIPER_DEFS.filter((v) => squadron.vipers!.includes(v.statusKey))
      : VIPER_DEFS;

  // ── Stand down ─────────────────────────────────────────────────────────────
  // Stops this squadron without stopping the engine. Confirmed first, because it
  // can flatten open positions and there is no undo — the operator redeploys.
  const [confirm, confirmDialog] = useConfirm();
  const [standingDown, setStandingDown] = useState(false);
  const [standDownError, setStandDownError] = useState<string | null>(null);

  const handleStandDown = useCallback(async () => {
    const autoDeployed = marketClass === 'politics' || marketClass === 'sports';
    const ok = await confirm({
      title: `Stand down ${squadron.name}?`,
      body: (
        <div className="space-y-2">
          <p>
            This squadron stops trading {squadron.market_name || 'its market'}. Resting
            orders are cancelled and any open position is flattened or left to settle.
          </p>
          {autoDeployed && (
            <p className="text-amber-300">
              Auto-deploy for {marketClass} will be switched off, so DRADIS does not
              immediately start a replacement. Turn it back on in Setup → Deployment.
            </p>
          )}
          <p className="text-gray-400">The engine and other squadrons keep running.</p>
        </div>
      ),
      confirmLabel: 'Stand down',
      tone: 'danger',
    });
    if (!ok) return;

    setStandingDown(true);
    setStandDownError(null);
    try {
      await standDownSquadron(squadron.id);
      onBack();
    } catch (err) {
      setStandDownError(err instanceof Error ? err.message : 'Stand-down failed');
    } finally {
      setStandingDown(false);
    }
  }, [confirm, squadron.id, squadron.name, squadron.market_name, marketClass, onBack]);

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

  // Summary cards read lifetime aggregates, NOT a reduce over `trades` above:
  // that call returns only the newest 60 rows (and the API clamps any limit to
  // 500), so every "total" it fed was silently truncated once the shard passed
  // 60 trades. `trades` still backs the list/table, which wants a recent window.
  const { data: tradeStats, isLoading: statsLoading } = useSWR(
    ['trade-stats', asset],
    () => getTradeStats(asset),
    { refreshInterval: 15_000 }
  );

  const { data: openPositions, isLoading: positionsLoading } = useSWR(
    ['positions', asset],
    () => getOpenPositions(asset),
    { refreshInterval: 15_000 }
  );

  const { data: status } = useSWR('status', getStatus, { refreshInterval: 30_000 });

  // Per-viper liveness + veto reasons, rendered on each ViperCard.
  const { data: viperStatus } = useSWR(
    ['vipers-status', asset],
    () => getVipersStatus(asset),
    { refreshInterval: 10_000, revalidateOnFocus: false }
  );

  // Registry rows keyed by `Strategy::name()` — the same key VIPER_DEFS carries.
  const statusByStrategy = new Map((viperStatus ?? []).map((r) => [r.strategy, r]));

  // The registry holds every viper the engine has evaluated, which is not
  // necessarily the set that renders as a card (market-class filtering, or a
  // viper with no VIPER_DEFS entry yet). Surface the remainder rather than
  // dropping it — an unlisted viper erroring silently is exactly what this
  // panel exists to catch.
  const rendered = new Set(activeVipers.map((v) => v.strategyName));
  const unmapped = (viperStatus ?? []).filter((r) => !rendered.has(r.strategy));


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
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <span className="text-2xl">✈️</span>
            <div>
              <h1 className="text-xl font-mono font-bold text-white">{squadron.name}</h1>
              <p className="text-xs font-mono text-gray-500 mt-0.5">
                {squadron.asset} Squadron · {squadron.state}
              </p>
            </div>
          </div>
          {squadron.state !== 'STOOD_DOWN' && (
            <button
              onClick={handleStandDown}
              disabled={standingDown}
              className="shrink-0 text-[11px] font-mono border rounded px-3 py-1.5 transition-colors
                         border-red-500/30 text-red-300 bg-red-500/10 hover:bg-red-500/20
                         disabled:opacity-50 disabled:cursor-not-allowed"
              title="Stop this squadron"
            >
              {standingDown ? 'Standing down…' : '🛬 Stand Down'}
            </button>
          )}
        </div>
        {standDownError && (
          <p className="text-[11px] font-mono text-red-400 mt-2">{standDownError}</p>
        )}
      </div>
      {confirmDialog}

      {/* ── Squadron + Raptor info ────────────────────────────────────────── */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <SquadronInfoCard squadron={squadron} />
        <RaptorHealthPanel
          raptorKinds={raptorKinds}
          raptors={status?.raptors}
          asset={raptorAsset}
          marketClass={marketClass}
        />
      </div>

      {/* ── Performance stats for this squadron/asset ─────────────────────── */}
      {(() => {
        const total   = tradeStats?.count ?? 0;
        const wins    = tradeStats?.wins ?? 0;
        // Win rate is measured over decided trades only. Exactly-flat trades are
        // neither wins nor losses, and counting them as losses (which dividing by
        // `count` would do) understates the rate.
        const decided = wins + (tradeStats?.losses ?? 0);
        const winRate = decided > 0 ? (wins / decided) * 100 : null;
        const avgPnl  = total > 0 ? (tradeStats?.realized_pnl ?? 0) / total : null;
        const since   = tradeStats?.first_ts
          ? new Date(tradeStats.first_ts).toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
          : null;
        return (
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div className="card px-4 py-3 flex flex-col gap-1">
          <span className="label-muted">Completed Trades</span>
          <span className="stat-value">{statsLoading ? '—' : String(total)}</span>
          <span className="text-xs text-gray-500">{since ? `all time, since ${since}` : 'all time'}</span>
        </div>
        <div className="card px-4 py-3 flex flex-col gap-1">
          <span className="label-muted">Open Positions</span>
          <span className="stat-value">{positionsLoading ? '—' : String(openPositions?.length ?? 0)}</span>
          <span className="text-xs text-gray-500">active now</span>
        </div>
        <div className="card px-4 py-3 flex flex-col gap-1">
          <span className="label-muted">Win Rate</span>
          <span className={`stat-value ${winRate === null ? 'text-gray-600' : winRate >= 50 ? 'text-emerald-300' : 'text-amber-300'}`}>
            {statsLoading || winRate === null ? '—' : `${winRate.toFixed(0)}%`}
          </span>
          <span className="text-xs text-gray-500">
            {winRate === null ? 'no closed trades' : `${wins}/${decided} profitable`}
          </span>
        </div>
        <div className="card px-4 py-3 flex flex-col gap-1">
          <span className="label-muted">Avg Trade P&L</span>
          <span className={`stat-value ${avgPnl === null ? 'text-gray-600' : avgPnl >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
            {statsLoading || avgPnl === null ? '—' : `${avgPnl >= 0 ? '+' : '−'}$${Math.abs(avgPnl).toFixed(2)}`}
          </span>
          <span className="text-xs text-gray-500">
            {avgPnl === null ? 'no closed trades' : 'per closed trade, net of fees'}
          </span>
        </div>
      </div>
        );
      })()}

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
                  // Scoped by squadron: both US wings and all three Kalshi
                  // squadrons share the venue-agnostic viper kinds, so a bare
                  // kind returned whichever squadron published last — the header
                  // named one market and the viper cards another.
                  market={status?.strategy_markets[`${squadron.id}:${v.statusKey}`]}
                  status={statusByStrategy.get(v.strategyName)}
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

        {/* Settings that apply across the squadron rather than to one viper.
            Rendered here because these read the SQUADRON's config — the same
            values the vipers above read — so a change lands where it is seen. */}
        {config && (
          <div className="mt-3">
            <SquadronSettingsCard config={config} onPatch={handlePatch} />
          </div>
        )}

        {unmapped.length > 0 && (
          <div className="mt-3 px-4 py-2 rounded-lg border border-amber-500/20 bg-amber-500/5 text-[11px] font-mono text-amber-300/80">
            <span className="font-semibold">Reporting without a card:</span>{' '}
            {unmapped.map((r, i) => {
              const bad = r.last_eval_secs_ago > STALE_EVAL_SECS
                || r.last_outcome === 'error'
                || r.last_outcome === 'timeout';
              return (
                <span key={r.strategy}>
                  {i > 0 && ' · '}
                  <span className={bad ? 'text-red-400' : ''}>
                    {r.strategy.replace(/Strategy$/, '')}
                  </span>
                  <span className="text-gray-500"> (eval {fmtAgo(r.last_eval_secs_ago)})</span>
                </span>
              );
            })}
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

