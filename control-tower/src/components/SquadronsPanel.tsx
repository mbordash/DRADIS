'use client';

import type { SquadronSummary, SquadronState } from '@/lib/types';

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

function SquadronRow({ sq, onClick }: { sq: SquadronSummary; onClick?: (sq: SquadronSummary) => void }) {
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

      {/* Right — state + deployed time + id */}
      <div className="flex items-center gap-3 shrink-0">
        <StateBadge state={sq.state} />
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
    </button>
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
}

export default function SquadronsPanel({ squadrons, isLoading, onSquadronClick }: Props) {
  const active   = squadrons.filter(s => s.state === 'PATROLLING' || s.state === 'DEPLOYED');
  const inactive = squadrons.filter(s => s.state !== 'PATROLLING' && s.state !== 'DEPLOYED');

  return (
    <div className="rounded-xl border border-[#1e1e32] bg-[#0d0d1a] overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-[#1e1e32]">
        <div className="flex items-center gap-2">
          <span className="text-sm font-mono font-semibold text-gray-200">✈️ CAG Registry</span>
          {!isLoading && squadrons.length > 0 && (
            <span className="text-[10px] font-mono bg-green-500/10 text-green-400 border border-green-500/20 rounded-full px-2 py-0.5">
              {active.length} active
            </span>
          )}
        </div>
        {/* Phase badge */}
        <span className="text-[9px] font-mono text-gray-700 border border-[#1e1e32] rounded px-1.5 py-0.5">
          Phase 3f
        </span>
      </div>

      {/* Body */}
      {isLoading || squadrons.length === 0 ? (
        <EmptyState isLoading={isLoading} />
      ) : (
        <>
          {/* Active squadrons */}
          {active.length > 0 && (
            <div>
              {active.map(sq => <SquadronRow key={sq.id} sq={sq} onClick={onSquadronClick} />)}
            </div>
          )}

          {/* Inactive / historical — collapsed by default if active ones are present */}
          {inactive.length > 0 && (
            <details className="group">
              <summary className="flex items-center gap-2 px-4 py-2 text-[10px] font-mono text-gray-600 cursor-pointer hover:text-gray-400 transition-colors border-t border-[#1e1e32] list-none">
                <span className="group-open:rotate-90 transition-transform inline-block">▶</span>
                {inactive.length} stood-down / RTB
              </summary>
              {inactive.map(sq => <SquadronRow key={sq.id} sq={sq} onClick={onSquadronClick} />)}
            </details>
          )}
        </>
      )}
    </div>
  );
}

