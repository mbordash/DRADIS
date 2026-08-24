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

import { useEffect, useState } from 'react';
import useSWR from 'swr';
import type { SquadronSummary, SquadronState, DeploymentStatus } from '@/lib/types';
import { getOpenPositions, getVipersForClass, getDeployments } from '@/lib/api';
import DeploySquadronModal from './DeploySquadronModal';

// ── State badge ───────────────────────────────────────────────────────────────

const STATE_STYLES: Record<SquadronState, { bg: string; text: string; dot: string; pulse: boolean }> = {
  STAGED:      { bg: 'bg-gray-500/10',   text: 'text-gray-400',   dot: 'bg-gray-500',   pulse: false },
  DEPLOYED:    { bg: 'bg-blue-500/10',   text: 'text-blue-300',   dot: 'bg-blue-400',   pulse: false },
  PATROLLING:  { bg: 'bg-green-500/10',  text: 'text-green-300',  dot: 'bg-green-400',  pulse: true  },
  RTB:         { bg: 'bg-amber-500/10',  text: 'text-amber-300',  dot: 'bg-amber-400',  pulse: false },
  STOOD_DOWN:  { bg: 'bg-red-500/10',    text: 'text-red-400',    dot: 'bg-red-500',    pulse: false },
};

function StateBadge({ state }: { state: SquadronState }) {
  const s = STATE_STYLES[state] ?? STATE_STYLES['STAGED'];
  return (
    <span className={`inline-flex items-center gap-1.5 text-[10px] font-mono px-2 py-0.5 rounded-full border border-white/5 ${s.bg} ${s.text}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${s.dot} ${s.pulse ? 'animate-pulse' : ''}`} />
      {state}
    </span>
  );
}

// ── Asset chip ────────────────────────────────────────────────────────────────

const ASSET_COLORS: Record<string, string> = {
  BTC: 'bg-orange-500/10 text-orange-300 border-orange-500/20',
  ETH: 'bg-indigo-500/10 text-indigo-300 border-indigo-500/20',
  SOL: 'bg-purple-500/10 text-purple-300 border-purple-500/20',
};

function AssetChip({ asset }: { asset: string }) {
  const cls = ASSET_COLORS[asset] ?? 'bg-gray-500/10 text-gray-300 border-gray-500/20';
  return (
    <span className={`inline-flex items-center text-[10px] font-mono font-bold px-2 py-0.5 rounded border ${cls}`}>
      {asset}
    </span>
  );
}

// ── Time-ago helper ───────────────────────────────────────────────────────────

function timeAgo(iso: string): string {
  const diffMs = Date.now() - new Date(iso).getTime();
  const mins   = Math.floor(diffMs / 60_000);
  if (mins < 1)   return 'just now';
  if (mins < 60)  return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs  < 24)  return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

// ── Squadron row ──────────────────────────────────────────────────────────────

/**
 * Viper coverage for a squadron's market class.
 *
 * Only crypto markets get the full nine strategies; sports, politics and the
 * `unknown` fallback are mapped to arbitrage + maker only (see the
 * market_class_viper seeding in helpers/db.rs). That is a market-class limit
 * rather than a venue one — it applies identically on Polymarket and Kalshi —
 * and it is invisible today: a customer on a sports market sees seven strategies
 * sit idle with no explanation. This surfaces it on the card.
 *
 * The denominator is read from the crypto class rather than hardcoded to 9, so
 * adding a tenth viper updates this automatically.
 */
function ViperCoverage({ marketClass }: { marketClass: string }) {
  const { data: mine } = useSWR(
    ['vipers-for-class', marketClass],
    () => getVipersForClass(marketClass as never),
    { revalidateOnFocus: false },
  );
  const { data: full } = useSWR(
    ['vipers-for-class', 'crypto'],
    () => getVipersForClass('crypto' as never),
    { revalidateOnFocus: false },
  );
  if (!mine || !full || full.length === 0) return null;

  const partial = mine.length < full.length;
  return (
    <span
      className={`text-[10px] font-mono rounded px-2 py-0.5 border ${
        partial
          ? 'bg-amber-500/10 text-amber-300 border-amber-500/25'
          : 'bg-emerald-500/10 text-emerald-300 border-emerald-500/20'
      }`}
      title={
        partial
          ? `${mine.length} of ${full.length} strategies apply to ${marketClass} markets: ` +
            `${mine.map(v => v.display).join(', ')}. The rest need a per-market price ` +
            `signal that only crypto markets provide.`
          : `All ${full.length} strategies apply to ${marketClass} markets.`
      }
    >
      {mine.length}/{full.length} apply
    </span>
  );
}

function SquadronRow({
  sq,
  onClick,
  missionCount
}: {
  sq: SquadronSummary;
  onClick?: (sq: SquadronSummary) => void;
  missionCount?: number;
}) {
  return (
    <button
      onClick={() => onClick?.(sq)}
      className="w-full flex flex-col sm:flex-row sm:items-center gap-2 sm:gap-4 px-4 py-3 border-b border-[#1e1e32] last:border-0 hover:bg-white/[0.02] transition-colors text-left cursor-pointer"
    >
      {/* Left — asset + name */}
      <div className="flex items-center gap-2 min-w-0 flex-1">
        <AssetChip asset={sq.asset} />
        <div className="min-w-0">
          <p className="text-xs font-mono text-gray-200 truncate" title={sq.name}>{sq.name}</p>
          <p className="text-[10px] font-mono text-gray-500 truncate mt-0.5" title={sq.market_name}>
            ⚔️ {sq.market_name}
          </p>
          {sq.maker_market_name && (
            <p className="text-[10px] font-mono text-gray-600 truncate mt-0.5" title={sq.maker_market_name}>
              🗓 {sq.maker_market_name}
            </p>
          )}
        </div>
      </div>

      {/* Right — tags on one line, metadata beneath.
          Stacked rather than run together on a single line: rows carry a
          varying number of tags (a squadron may have no mission count, or an
          unknown market class), so on one line the deployed-time and id slid
          horizontally from row to row and no column lined up. Splitting them
          gives the tags a row of their own, right-aligned, so they read as a
          column down the panel. */}
      <div className="flex flex-col items-end gap-1 shrink-0">
        <div className="flex items-center gap-2">
          {sq.market_class && sq.market_class !== 'unknown' && (
            <span
              className="text-[10px] font-mono uppercase tracking-wide bg-violet-500/10 text-violet-300 border border-violet-500/20 rounded px-2 py-0.5"
              title={`Market class: ${sq.market_class}`}
            >
              {sq.market_class}
            </span>
          )}
          {sq.market_class && <ViperCoverage marketClass={sq.market_class} />}
          {missionCount !== undefined && missionCount > 0 && (
            <span className="text-[10px] font-mono bg-cyan-500/10 text-cyan-300 border border-cyan-500/20 rounded px-2 py-0.5" title={`${missionCount} active mission${missionCount === 1 ? '' : 's'}`}>
              ✈️ {missionCount}
            </span>
          )}
          <StateBadge state={sq.state} />
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[10px] font-mono text-gray-600" title={sq.deployed_at}>
            {timeAgo(sq.deployed_at)}
          </span>
          <span
            className="hidden lg:inline text-[9px] font-mono text-gray-700 truncate max-w-[180px]"
            title={sq.id}
          >
            {sq.id}
          </span>
        </div>
      </div>
    </button>
  );
}

// ── In-flight deployment row ──────────────────────────────────────────────────
//
// A deploy is queued, not executed. `POST /api/squadrons/deploy` returns as soon
// as the row is written; the engine picks it up on its own poll and only then
// does a squadron exist to list. In between, the modal has already closed and
// the squadron list looks exactly as it did before — which reads as "nothing
// happened" rather than "starting". Worse, a deployment that FAILS leaves that
// state permanently: the error is recorded in the queue and was never surfaced,
// so a failure and a slow success were indistinguishable.
//
// These rows come straight from the queue, so they say what is actually true
// rather than assuming the deploy will succeed.

const DEPLOY_STATUS_STYLES: Record<string, { label: string; bg: string; text: string; dot: string; pulse: boolean }> = {
  pending:    { label: 'QUEUED',   bg: 'bg-amber-500/10', text: 'text-amber-300', dot: 'bg-amber-400', pulse: true  },
  processing: { label: 'STARTING', bg: 'bg-blue-500/10',  text: 'text-blue-300',  dot: 'bg-blue-400',  pulse: true  },
  failed:     { label: 'FAILED',   bg: 'bg-red-500/10',   text: 'text-red-300',   dot: 'bg-red-500',   pulse: false },
};

function PendingDeploymentRow({ dep }: { dep: DeploymentStatus }) {
  const style = DEPLOY_STATUS_STYLES[dep.status] ?? DEPLOY_STATUS_STYLES.pending;
  return (
    <div className="flex items-center justify-between px-4 py-3 border-b border-[#1e1e32] last:border-b-0">
      <div className="flex items-center gap-3 min-w-0">
        <span className={`inline-flex items-center gap-1.5 text-[10px] font-mono ${style.bg} ${style.text} rounded-full px-2 py-0.5 shrink-0`}>
          <span className={`w-1.5 h-1.5 rounded-full ${style.dot} ${style.pulse ? 'animate-pulse' : ''}`} />
          {style.label}
        </span>
        <div className="min-w-0">
          <div className="text-xs font-mono text-gray-300 truncate">
            {dep.market_type} squadron
          </div>
          {dep.status === 'failed' && dep.error ? (
            <div className="text-[10px] font-mono text-red-400/80 truncate" title={dep.error}>
              {dep.error}
            </div>
          ) : (
            <div className="text-[10px] font-mono text-gray-600 truncate" title={dep.market_id}>
              {dep.market_id}
            </div>
          )}
        </div>
      </div>
      {dep.status !== 'failed' && (
        <span className="text-[10px] font-mono text-gray-600 shrink-0">
          waiting for the engine
        </span>
      )}
    </div>
  );
}

// ── Empty state ───────────────────────────────────────────────────────────────

function EmptyState({ isLoading }: { isLoading: boolean }) {
  return (
    <div className="flex flex-col items-center justify-center py-10 gap-2 text-gray-600">
      {isLoading ? (
        <>
          <span className="text-2xl animate-pulse">✈️</span>
          <span className="text-xs font-mono">Loading squadrons…</span>
        </>
      ) : (
        <>
          <span className="text-2xl opacity-30">🛬</span>
          <span className="text-xs font-mono">No squadrons deployed</span>
          <span className="text-[10px] text-gray-700">Start DRADIS to deploy a squadron</span>
        </>
      )}
    </div>
  );
}

// ── Main panel ────────────────────────────────────────────────────────────────

interface Props {
  squadrons: SquadronSummary[];
  isLoading: boolean;
  onSquadronClick?: (sq: SquadronSummary) => void;
  onDeploySuccess?: () => void;
}

export default function SquadronsPanel({ squadrons, isLoading, onSquadronClick, onDeploySuccess }: Props) {
  const [deployModalOpen, setDeployModalOpen] = useState(false);
  // A deploy this panel just made, held until the engine's own queue confirms
  // it. Optimistic on purpose: the API answered success, so the row is true the
  // moment it is shown, and it is retired by real state rather than by a timer.
  const [justDeployed, setJustDeployed] = useState<{ id: string; marketType: string } | null>(null);
  
  // STAGED is active (pending deployment), not inactive
  // RTB belongs with the ACTIVE squadrons. "Return to base" is an operating
  // phase, not an ending: the squadron is alive and managing its open positions
  // to close, it has only stopped opening new ones. It is entered 60s before
  // every market close, so grouping it with STOOD_DOWN made a healthy crypto
  // squadron drop into a collapsed drawer labelled "stood-down" every fifteen
  // minutes and reappear afterwards — routine rotation shown as a death.
  const active   = squadrons.filter(s =>
    s.state === 'PATROLLING' || s.state === 'DEPLOYED' || s.state === 'STAGED' || s.state === 'RTB');
  const inactive = squadrons.filter(s => s.state === 'STOOD_DOWN');

  // Get unique assets from squadrons
  const assets = [...new Set(squadrons.map(s => s.asset.toLowerCase()))];

  // Fetch positions for all assets concurrently
  const { data: allPositions } = useSWR(
    assets.length > 0 ? ['squadron-missions', ...assets] : null,
    async () => await Promise.all(assets.map(asset => getOpenPositions(asset))),
    { refreshInterval: 15_000 }
  );

  // In-flight and failed deployments. Polled faster than the squadron list
  // because this is exactly the window the operator is staring at: the engine's
  // own queue poll is 5s, so anything slower would make the UI look stuck for
  // reasons that have nothing to do with the engine.
  const { data: deployments } = useSWR<DeploymentStatus[]>(
    'deployment-queue',
    getDeployments,
    { refreshInterval: 3_000 }
  );

  // A queued row is worth showing until its squadron exists — after that the
  // real row carries the state and showing both would double-count.
  //
  // Keyed on the deployment's OWN status, not on whether the class is live.
  // Suppressing by class was correct while only one squadron per class could
  // exist: a live class meant the deployment had landed. Now that a class can
  // hold several squadrons, it suppressed the feedback exactly when it was most
  // wanted — deploying a second sports squadron beside a running one showed
  // nothing at all. A row's status goes pending → processing → active, and
  // 'active' is precisely the moment the squadron appears in the list.
  const inFlight = (deployments ?? []).filter(d =>
    d.status === 'pending' || d.status === 'processing');

  // Failures have no later status to retire them, so they are bounded by age
  // instead — long enough that one cannot slip by between two poll ticks,
  // short enough that last week's failures do not accumulate on screen.
  const FAILURE_VISIBLE_MS = 10 * 60 * 1000;
  const failed = (deployments ?? []).filter(d => {
    if (d.status !== 'failed') return false;
    const at = Date.parse(d.created_at);
    return Number.isNaN(at) || Date.now() - at < FAILURE_VISIBLE_MS;
  });
  // Retire the optimistic row once the engine's queue accounts for it — either
  // it is now one of the in-flight rows (which carry the real market id), or it
  // has left pending/processing entirely, meaning the squadron exists.
  const optimisticSettled = justDeployed !== null && (
    inFlight.some(d => d.id === justDeployed.id) ||
    (deployments ?? []).some(d => d.id === justDeployed.id && d.status !== 'pending' && d.status !== 'processing')
  );
  useEffect(() => {
    if (optimisticSettled) setJustDeployed(null);
  }, [optimisticSettled]);

  const optimisticRow: DeploymentStatus[] = justDeployed && !optimisticSettled
    ? [{
        id: justDeployed.id,
        market_id: 'selecting market…',
        market_type: justDeployed.marketType as DeploymentStatus['market_type'],
        raptors: [], vipers: [],
        status: 'pending',
        created_at: new Date().toISOString(),
      }]
    : [];

  const queueRows = [...optimisticRow, ...inFlight.filter(d => d.id !== justDeployed?.id), ...failed];

  // Build mission count map: asset -> count
  const missionCounts: Record<string, number> = {};
  if (allPositions) {
    assets.forEach((asset, i) => {
      missionCounts[asset] = allPositions[i]?.length ?? 0;
    });
  }

  return (
    <>
      <div className="rounded-xl border border-[#1e1e32] bg-[#0d0d1a] overflow-hidden">
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-[#1e1e32]">
          <div className="flex items-center gap-2">
            <span className="text-sm font-mono font-semibold text-gray-200">✈️ CAG Registry</span>
            {!isLoading && (squadrons.length > 0 || queueRows.length > 0) && (
              <span className="text-[10px] font-mono bg-green-500/10 text-green-400 border border-green-500/20 rounded-full px-2 py-0.5">
                {active.length} active
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            {/* Deploy Squadron button */}
            <button
              onClick={() => setDeployModalOpen(true)}
              className="flex items-center gap-1.5 text-[10px] font-mono bg-green-500/10 text-green-400 border border-green-500/20 rounded px-2 py-1 hover:bg-green-500/20 transition-colors"
              title="Deploy a new squadron"
            >
              <span>+</span>
              <span>Deploy</span>
            </button>
          </div>
        </div>

        {/* Body */}
        {isLoading || (squadrons.length === 0 && queueRows.length === 0) ? (
          <EmptyState isLoading={isLoading} />
        ) : (
        <>
          {/* Deployments the engine has not picked up yet, and ones that failed */}
          {queueRows.length > 0 && (
            <div>
              {queueRows.map(dep => (
                <PendingDeploymentRow key={dep.id} dep={dep} />
              ))}
            </div>
          )}

          {/* Active squadrons */}
          {active.length > 0 && (
            <div>
              {active.map(sq => (
                <SquadronRow
                  key={sq.id}
                  sq={sq}
                  onClick={onSquadronClick}
                  missionCount={missionCounts[sq.asset.toLowerCase()]}
                />
              ))}
            </div>
          )}

          {/* Inactive / historical — collapsed by default if active ones are present */}
          {inactive.length > 0 && (
            <details className="group">
              <summary className="flex items-center gap-2 px-4 py-2 text-[10px] font-mono text-gray-600 cursor-pointer hover:text-gray-400 transition-colors border-t border-[#1e1e32] list-none">
                <span className="group-open:rotate-90 transition-transform inline-block">▶</span>
                {inactive.length} stood-down
              </summary>
              {inactive.map(sq => (
                <SquadronRow
                  key={sq.id}
                  sq={sq}
                  onClick={onSquadronClick}
                  missionCount={missionCounts[sq.asset.toLowerCase()]}
                />
              ))}
            </details>
          )}
        </>
      )}
    </div>

    {/* Deploy Squadron Modal */}
    <DeploySquadronModal
      isOpen={deployModalOpen}
      onClose={() => setDeployModalOpen(false)}
      onDeployed={(deploymentId, marketType) => {
        // Show the row immediately rather than waiting to discover it by poll.
        // The queued → active window is only a few seconds and both this panel
        // and the engine poll on their own schedules, so a purely poll-driven
        // row can be missed entirely — which leaves the operator watching an
        // unchanged table, the exact complaint this was meant to answer.
        setJustDeployed({ id: deploymentId, marketType });
        setDeployModalOpen(false);
        onDeploySuccess?.();
      }}
    />
  </>
  );
}

