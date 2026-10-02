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

// Helm intents: where each stands, what the critique said, what the fee verdict
// was, and — once an intent has resolved — whether the critique named the
// failure mode that happened.
//
// Intents outlive their squadrons. A Helm squadron retires itself the moment
// its intents are all terminal, and the CAG reaps it soon after, so a panel
// that lived only on the squadron page would vanish exactly when the
// thesis-versus-outcome record becomes readable and scorable. With no
// `squadronId` this panel lists every intent on the instance, from the
// `/api/helm/intents` listing, which does not depend on any squadron existing.
// With one, it is that squadron's own view.
//
// It shows no calibration figure. The summary endpoint says how many intents
// have resolved against how many are needed; below that line everything is
// recorded and nothing is displayed.

import { useCallback, useState } from 'react';
import useSWR from 'swr';
import type { HelmIntent } from '@/lib/types';
import {
  acknowledgeHelmIntent, cancelHelmIntent, getHelmSummary, HelmApiError, listHelmIntents, scoreHelmCritique,
} from '@/lib/api';

const STATUS_COLOR: Record<string, string> = {
  proposed: 'text-gray-300 border-gray-600',
  acknowledged: 'text-teal-200 border-teal-500/40',
  working: 'text-amber-200 border-amber-500/40',
  filled: 'text-green-200 border-green-500/40',
  partial: 'text-green-200 border-green-500/40',
  missed: 'text-red-200 border-red-500/40',
  closed: 'text-gray-400 border-gray-700',
  superseded: 'text-gray-500 border-gray-700',
};

const IN_FLIGHT = new Set(['working', 'filled', 'partial']);

/** Nothing more will happen to it. Matches `IntentStatus::is_terminal` in Rust. */
const isTerminal = (status: string) => status === 'closed' || status === 'superseded';

function IntentRow({ intent, showSquadron, onChanged }: { intent: HelmIntent; showSquadron: boolean; onChanged: () => void }) {
  const [read, setRead] = useState(false);
  const [busy, setBusy] = useState(false);
  const [note, setNote] = useState<string | null>(null);
  const critiqueUnavailable = intent.critique?.startsWith('unavailable:') ?? false;
  const inFlight = IN_FLIGHT.has(intent.status);
  const terminal = isTerminal(intent.status);

  const run = useCallback(async (f: () => Promise<unknown>) => {
    setBusy(true); setNote(null);
    try { await f(); onChanged(); }
    catch (e) {
      if (e instanceof HelmApiError) {
        setNote(e.violations.length > 0 ? e.violations.join(' · ') : e.message);
      } else {
        setNote(e instanceof Error ? e.message : String(e));
      }
    } finally { setBusy(false); }
  }, [onChanged]);

  return (
    <div className="rounded border border-[#1e1e32] bg-[#0a0a14] px-3 py-2.5 space-y-2">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <span className="text-xs font-mono text-gray-200">#{intent.id}</span>
          <span className={`text-[10px] font-mono border rounded px-1.5 py-0.5 ${STATUS_COLOR[intent.status] ?? 'text-gray-300 border-gray-600'}`}>{intent.status}</span>
          <span className="text-xs font-mono text-gray-300">{intent.side}</span>
          <span className="text-xs font-mono text-gray-500">conf {(intent.first.confidence * 100).toFixed(0)}% · {intent.first.horizon} · v{intent.current_version}</span>
          {intent.ghost && <span className="text-[10px] font-mono text-gray-500">ghost</span>}
        </div>
        <span className="text-[10px] font-mono text-gray-500 shrink-0">{new Date(intent.created_at).toLocaleString()}</span>
      </div>
      {showSquadron && (
        <p className="text-[11px] font-mono text-teal-300/80 truncate" title={intent.market_name}>
          {intent.squadron_id} · {intent.market_name}
        </p>
      )}
      <p className="text-xs font-mono text-gray-300 whitespace-pre-wrap">{intent.first.thesis}</p>
      <p className="text-[11px] font-mono text-gray-500">Wrong if: {intent.first.falsification}</p>
      {intent.status_detail && <p className="text-[11px] font-mono text-gray-500">{intent.status_detail}</p>}
      {intent.fee_verdict && <p className="text-[11px] font-mono text-amber-200">Fee: {intent.fee_verdict}</p>}
      <div className="rounded border border-indigo-500/20 bg-indigo-500/5 px-2 py-1.5">
        <p className="text-[10px] font-mono uppercase tracking-wide text-indigo-300">Critique</p>
        {intent.critique == null
          ? <p className="text-[11px] font-mono text-gray-500">{intent.critique_requested_at ? 'pending…' : 'not requested'}</p>
          : <p className={`text-[11px] font-mono whitespace-pre-wrap ${critiqueUnavailable ? 'text-gray-500' : 'text-indigo-100'}`}>{intent.critique}</p>}
      </div>
      {note && <p className="text-[11px] font-mono text-amber-300">{note}</p>}
      <div className="flex flex-wrap items-center gap-2">
        {intent.status === 'proposed' && (
          <>
            <label className="flex items-center gap-1.5 text-[11px] font-mono text-gray-300">
              <input type="checkbox" checked={read} onChange={(e) => setRead(e.target.checked)} />
              read the critique
            </label>
            <button
              disabled={busy || !read}
              onClick={() => run(() => acknowledgeHelmIntent(intent.id))}
              className="text-[11px] font-mono border border-teal-500/40 bg-teal-500/10 text-teal-200 rounded px-2 py-1 hover:bg-teal-500/20 disabled:opacity-40"
            >
              Acknowledge and enter
            </button>
          </>
        )}
        {!terminal && !inFlight && (
          <button
            disabled={busy}
            onClick={() => run(() => cancelHelmIntent(intent.id, 'cancelled from the Control Tower'))}
            className="text-[11px] font-mono border border-red-500/30 bg-red-500/10 text-red-200 rounded px-2 py-1 hover:bg-red-500/20 disabled:opacity-40"
          >
            Cancel
          </button>
        )}
        {inFlight && (
          <span className="text-[10px] font-mono text-gray-500">
            held: the position leaves by its posture, by RTB, or by settlement; the intent closes when it does
          </span>
        )}
        {terminal && !intent.critique_outcome && intent.critique && !critiqueUnavailable && (
          <>
            <span className="text-[10px] font-mono text-gray-500">Did the critique name what happened?</span>
            <button disabled={busy} onClick={() => run(() => scoreHelmCritique(intent.id, 'named_it'))} className="text-[11px] font-mono border border-[#1e1e32] rounded px-2 py-1 text-gray-300 hover:bg-white/[0.03] disabled:opacity-40">named it</button>
            <button disabled={busy} onClick={() => run(() => scoreHelmCritique(intent.id, 'missed_it'))} className="text-[11px] font-mono border border-[#1e1e32] rounded px-2 py-1 text-gray-300 hover:bg-white/[0.03] disabled:opacity-40">missed it</button>
          </>
        )}
        {terminal && !intent.critique_outcome && (!intent.critique || critiqueUnavailable) && (
          <button disabled={busy} onClick={() => run(() => scoreHelmCritique(intent.id, 'no_critique'))} className="text-[11px] font-mono border border-[#1e1e32] rounded px-2 py-1 text-gray-500 hover:bg-white/[0.03] disabled:opacity-40">record: no critique</button>
        )}
        {intent.critique_outcome && <span className="text-[10px] font-mono text-gray-500">critique scored: {intent.critique_outcome}</span>}
        {intent.close_reason && <span className="text-[10px] font-mono text-gray-500">closed: {intent.close_reason}</span>}
      </div>
    </div>
  );
}

/**
 * One line on the operating page, and only when money is committed.
 *
 * The full intent list used to sit on the main page, which put a column of
 * mostly-resolved history in the middle of the view used to operate. A live
 * intent is operational — a position held under a posture that can still be
 * revised — so that count belongs here; the record belongs in the Helm view.
 * Renders nothing while flat.
 */
export function HelmLiveLine() {
  const { data: intents } = useSWR(
    ['helm-intents', '*'],
    () => listHelmIntents(undefined, true),
    { refreshInterval: 10_000 },
  );
  const live = intents?.filter((i) => !isTerminal(i.status)) ?? [];
  if (live.length === 0) return null;

  const held = live.filter((i) => i.status === 'filled' || i.status === 'partial').length;

  return (
    <a
      href="#helm"
      className="flex items-center gap-2 card px-3 py-2 border border-teal-500/30 bg-teal-500/[0.04] hover:bg-teal-500/[0.08] transition-colors"
    >
      <span className="text-xs">🧭</span>
      <span className="text-[11px] font-mono text-teal-200">
        {live.length} Helm intent{live.length === 1 ? '' : 's'} live
        {held > 0 && <span className="text-teal-400"> · {held} holding a position</span>}
      </span>
      <span className="text-[10px] font-mono text-gray-500 ml-auto">view the record →</span>
    </a>
  );
}

interface Props {
  /** One squadron's intents; omit for every intent on the instance. */
  squadronId?: string;
  /** With no squadron, hide the whole card while there is nothing to show. */
  hideWhenEmpty?: boolean;
  /**
   * Which half of the lifecycle to show. A live intent is an operation — money
   * is committed on a conviction under a posture that may need revising. A
   * resolved one is a record, read deliberately against what happened. They
   * belong on different screens, so the Helm view asks for each separately.
   * Omit for both, as a squadron's own page wants.
   */
  show?: 'live' | 'resolved';
  /** Render the rows bare, for a page that supplies its own heading. */
  bare?: boolean;
  /** Shown in place of the stock empty line. */
  emptyText?: string;
}

export default function HelmIntentsPanel({
  squadronId, hideWhenEmpty = false, show, bare = false, emptyText,
}: Props) {
  const [expanded, setExpanded] = useState(false);

  // A bare list has no ribbon to expand; everything else that is not scoped to
  // one squadron opens collapsed, being a record rather than something to
  // operate from. A squadron's own intents are the point of its page.
  const collapsible = !squadronId && !bare;
  const open = !collapsible || expanded;

  // SWR defaults: the poll pauses while the tab is hidden (`refreshWhenHidden`
  // is false) and revalidates once on focus, so the list is refreshed within a
  // fetch of the operator coming back rather than left stale.
  //
  // Collapsed, the only thing on screen is the ribbon's count, so it polls at a
  // tenth of the rate rather than pulling every intent on the instance every
  // three seconds for a panel nobody has opened.
  const { data: intents, mutate } = useSWR(
    ['helm-intents', squadronId ?? '*'],
    () => listHelmIntents(squadronId, true),
    { refreshInterval: open ? 3_000 : 30_000 },
  );
  const { data: summary, mutate: mutateSummary } = useSWR('helm-summary', getHelmSummary, { refreshInterval: open ? 10_000 : 60_000 });
  const refresh = useCallback(() => { mutate(); mutateSummary(); }, [mutate, mutateSummary]);

  // Same definition of terminal as `IntentRow` uses.
  const shown = !intents ? undefined : intents.filter((i) =>
    show === 'live' ? !isTerminal(i.status)
    : show === 'resolved' ? isTerminal(i.status)
    : true);

  if (hideWhenEmpty && !squadronId && shown && shown.length === 0) return null;

  const title = squadronId ? '🧭 Helm intents' : '🧭 Helm — all intents';

  // Live ones called out in the ribbon: that is the number worth seeing closed.
  const live = intents?.filter((i) => !isTerminal(i.status)).length ?? 0;

  if (bare) {
    return (
      <div className="space-y-3">
        {!shown && <p className="text-xs font-mono text-gray-500">Loading…</p>}
        {shown && shown.length === 0 && (
          <p className="text-xs font-mono text-gray-500">{emptyText ?? 'Nothing here yet.'}</p>
        )}
        {shown?.map((i) => <IntentRow key={i.id} intent={i} showSquadron={!squadronId} onChanged={refresh} />)}
      </div>
    );
  }

  return (
    <div className="card p-4 border border-teal-500/20 bg-[#0d0d1a] space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div className={collapsible ? 'min-w-0 flex-1' : ''}>
          {collapsible ? (
            <button
              type="button"
              onClick={() => setExpanded((e) => !e)}
              aria-expanded={expanded}
              className="flex items-center gap-2 text-left w-full group"
            >
              <span className="text-[10px] font-mono text-teal-500/70 w-3 shrink-0">{expanded ? '▾' : '▸'}</span>
              <h3 className="text-xs font-mono uppercase tracking-wide text-teal-300 group-hover:text-teal-200">{title}</h3>
              <span className="text-[10px] font-mono text-gray-500">
                {intents ? `${intents.length} total` : '…'}
                {live > 0 && <span className="text-teal-300"> · {live} live</span>}
              </span>
            </button>
          ) : (
            <h3 className="text-xs font-mono uppercase tracking-wide text-teal-300">{title}</h3>
          )}
          {!squadronId && expanded && (
            <p className="text-[10px] font-mono text-gray-500 mt-1 ml-5">
              Every intent on this instance, whether or not its squadron still exists. Intents outlive
              their squadrons: the record and the critique scoring live here.
            </p>
          )}
        </div>
        {summary && open && (
          <span className="text-[10px] font-mono text-gray-500 shrink-0" title="Everything is recorded; a calibration figure is shown only once enough intents have resolved.">
            {summary.resolved} resolved of {summary.calibration_min_resolved} needed before any calibration is shown
          </span>
        )}
      </div>
      {open && !shown && <p className="text-xs font-mono text-gray-500">Loading…</p>}
      {open && shown && shown.length === 0 && (
        <p className="text-xs font-mono text-gray-500">
          {emptyText ?? (squadronId
            ? 'No intent yet. The viper reports "awaiting operator intent" until one is acknowledged.'
            : 'No Helm intents yet. "Take the Helm" above creates one.')}
        </p>
      )}
      {open && shown?.map((i) => <IntentRow key={i.id} intent={i} showSquadron={!squadronId} onChanged={refresh} />)}
    </div>
  );
}
