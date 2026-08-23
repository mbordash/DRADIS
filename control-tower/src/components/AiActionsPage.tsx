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

/**
 * AI Actions view — the full llm_actions audit trail (Epic S6).
 *
 * Every config change the LLM Advisor has proposed, with its lifecycle:
 * proposed → applied / rejected / expired / reverted / failed. This is the
 * observability + retraining surface: outcomes recorded here feed the
 * few-shot corpus injected back into the advisor prompt (S7).
 */

import { useState } from 'react';
import useSWR from 'swr';
import type { LlmActionRow } from '@/lib/types';
import { getLlmActions, approveLlmAction, rejectLlmAction } from '@/lib/api';
import { getSetupStatus } from '@/lib/setupApi';

const STATUS_STYLE: Record<string, string> = {
  proposed: 'bg-amber-500/10 text-amber-400 border-amber-500/20',
  applied:  'bg-green-500/10 text-green-400 border-green-500/20',
  approved: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20',
  rejected: 'bg-red-500/10 text-red-400 border-red-500/20',
  expired:  'bg-gray-500/10 text-gray-500 border-gray-500/20',
  reverted: 'bg-orange-500/10 text-orange-400 border-orange-500/20',
  failed:   'bg-rose-500/10 text-rose-400 border-rose-500/20',
};

const TIER_LABEL: Record<number, string> = { 1: 'T1 recommend', 2: 'T2 limited', 3: 'T3 autonomous' };

function fmtTs(iso: string): string {
  try {
    return new Date(iso).toLocaleString('en-US', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false,
    });
  } catch { return iso; }
}

const unquote = (s: string) => s.replaceAll('"', '');

/** Provider ids are config values; these are what an operator calls them. */
const LLM_LABEL: Record<string, string> = {
  anthropic: 'Claude',
  openai:    'OpenAI',
  ollama:    'Ollama',
};

export default function AiActionsPage() {
  // Public endpoint — no admin session needed just to say whether an advisor exists.
  const { data: setup } = useSWR('setupStatus', getSetupStatus, { refreshInterval: 60_000 });
  const { data: actions, isLoading, mutate } =
    useSWR('llmActionsFull', () => getLlmActions(250), { refreshInterval: 30_000 });
  const [filter, setFilter] = useState<string>('all');
  const [busyIds, setBusyIds] = useState<Set<number>>(new Set());
  const [error, setError] = useState<string | null>(null);

  const all = actions ?? [];
  const counts = all.reduce<Record<string, number>>((m, a) => {
    m[a.status] = (m[a.status] ?? 0) + 1;
    return m;
  }, {});
  const rows = filter === 'all' ? all : all.filter(a => a.status === filter);

  const run = async (id: number, fn: (id: number) => Promise<LlmActionRow>) => {
    setError(null);
    setBusyIds(prev => new Set(prev).add(id));
    try {
      await fn(id);
      await mutate();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusyIds(prev => { const n = new Set(prev); n.delete(id); return n; });
    }
  };

  return (
    <section>
      <div className="flex flex-wrap items-center justify-between gap-2 mb-3">
        <div className="flex items-center gap-2">
          <p className="label-muted">AI Actions</p>
          <span className="text-xs font-mono text-gray-600">🤖 config-change audit trail</span>
        </div>
        {/* Status filter chips */}
        <div className="flex flex-wrap items-center gap-1">
          {['all', 'proposed', 'applied', 'rejected', 'expired', 'reverted', 'failed'].map(s => (
            <button
              key={s}
              onClick={() => setFilter(s)}
              className={[
                'text-[10px] font-mono px-2 py-0.5 rounded border transition-colors',
                filter === s
                  ? 'bg-indigo-500/20 border-indigo-500/40 text-indigo-300'
                  : 'bg-[#13131f] border-[#1e1e32] text-gray-500 hover:text-gray-300',
              ].join(' ')}
            >
              {s}{s !== 'all' && counts[s] ? ` ${counts[s]}` : s === 'all' ? ` ${all.length}` : ''}
            </button>
          ))}
        </div>
      </div>

      {error && (
        <div className="mb-3 text-xs font-mono rounded-lg px-3 py-2 bg-rose-500/10 border border-rose-500/30 text-rose-300">
          ✗ {error}
        </div>
      )}

      <div className="card p-0 overflow-x-auto">
        {isLoading ? (
          <div className="flex items-center justify-center h-24 text-gray-600 text-sm">Loading AI actions…</div>
        ) : rows.length === 0 ? (
          <div className="flex flex-col items-center justify-center gap-2 py-10 text-center">
            <span className="text-3xl">🤖</span>
            {filter !== 'all' ? (
              <p className="text-sm text-gray-500">No &apos;{filter}&apos; actions.</p>
            ) : setup && !setup.llm_configured ? (
              // An empty table looks the same whether the advisor is running and
              // has proposed nothing, or was never set up. Say which.
              <>
                <p className="text-sm text-gray-400">No LLM Advisor is configured.</p>
                <p className="text-xs text-gray-600 max-w-md leading-relaxed">
                  Nothing will ever appear here until one is. The advisor is optional —
                  it reviews live trading and proposes config changes for your approval.
                  Choose a provider under{' '}
                  <span className="text-gray-400 font-mono">Setup → LLM Advisor</span>.
                </p>
              </>
            ) : (
              <>
                <p className="text-sm text-gray-500">No AI actions yet.</p>
                <p className="text-xs text-gray-600">
                  {setup?.llm_provider
                    ? `The advisor (${LLM_LABEL[setup.llm_provider.toLowerCase()] ?? setup.llm_provider}) records every config proposal here.`
                    : 'The LLM Advisor records every config proposal here.'}
                </p>
              </>
            )}
          </div>
        ) : (
          <table className="w-full text-xs font-mono">
            <thead>
              <tr className="text-left text-gray-600 border-b border-[#1e1e32]">
                <th className="px-3 py-2 font-normal">When</th>
                <th className="px-3 py-2 font-normal">Squadron</th>
                <th className="px-3 py-2 font-normal">Field</th>
                <th className="px-3 py-2 font-normal">Change</th>
                <th className="px-3 py-2 font-normal">Δ</th>
                <th className="px-3 py-2 font-normal">Status</th>
                <th className="px-3 py-2 font-normal">Tier</th>
                <th className="px-3 py-2 font-normal">Reason / detail</th>
                <th className="px-3 py-2 font-normal">Outcome</th>
                <th className="px-3 py-2 font-normal"></th>
              </tr>
            </thead>
            <tbody>
              {rows.map(a => {
                const busy = busyIds.has(a.id);
                return (
                  <tr key={a.id} className="border-b border-[#1e1e32]/50 align-top hover:bg-[#13131f]/50">
                    <td className="px-3 py-2 text-gray-500 whitespace-nowrap">
                      {fmtTs(a.ts)}
                      {a.ghost_mode && (
                        <span className="ml-1 text-[9px] bg-gray-800 text-gray-500 border border-gray-700 rounded px-1">GHOST</span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-gray-400 whitespace-nowrap">
                      {a.squadron_id ?? (
                        // Written before the advisor was squadron-scoped: applied
                        // to a config no strategy reads, so it never moved
                        // anything live and cannot be approved now.
                        <span className="text-gray-600" title="Pre-dates squadron-scoped advice — targets a config no strategy reads">
                          —
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-gray-300">{a.field}</td>
                    <td className="px-3 py-2 whitespace-nowrap">
                      <span className="text-gray-600">{unquote(a.from_value)}</span>
                      <span className="text-gray-600 mx-1">→</span>
                      <span className="text-violet-300">{unquote(a.to_value)}</span>
                      {a.clamped && (
                        <span className="ml-1 text-[9px] bg-amber-500/10 text-amber-400 border border-amber-500/20 rounded px-1">clamped</span>
                      )}
                    </td>
                    <td className={`px-3 py-2 whitespace-nowrap ${
                      a.delta_pct == null ? 'text-gray-700' : a.delta_pct >= 0 ? 'text-green-500' : 'text-red-500'
                    }`}>
                      {a.delta_pct == null ? '—' : `${a.delta_pct >= 0 ? '+' : ''}${(a.delta_pct * 100).toFixed(1)}%`}
                    </td>
                    <td className="px-3 py-2">
                      <span className={`text-[10px] border rounded px-1.5 py-0.5 ${STATUS_STYLE[a.status] ?? STATUS_STYLE.expired}`}>
                        {a.status}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-gray-500 whitespace-nowrap">{TIER_LABEL[a.tier] ?? `T${a.tier}`}</td>
                    <td className="px-3 py-2 text-gray-500 max-w-[24rem]">
                      <span title={a.reason}>{a.reason}</span>
                      {a.status_detail && (
                        <p className="text-[10px] text-gray-600 mt-0.5" title={a.status_detail}>{a.status_detail}</p>
                      )}
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap">
                      {a.outcome_score == null ? (
                        <span className="text-gray-700">—</span>
                      ) : (
                        <span className={a.outcome_score >= 0 ? 'text-green-400' : 'text-red-400'} title={a.outcome_detail ?? ''}>
                          {a.outcome_score >= 0 ? '+' : ''}{a.outcome_score.toFixed(2)}
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap">
                      {a.status === 'proposed' && (
                        <span className="flex gap-1">
                          <button
                            onClick={() => run(a.id, approveLlmAction)}
                            disabled={busy}
                            className="px-2 py-0.5 rounded bg-green-500/10 text-green-400 border border-green-500/20 hover:bg-green-500/20 disabled:opacity-30 transition-colors"
                          >
                            {busy ? '…' : 'apply'}
                          </button>
                          <button
                            onClick={() => run(a.id, rejectLlmAction)}
                            disabled={busy}
                            className="px-2 py-0.5 rounded bg-red-500/10 text-red-400 border border-red-500/20 hover:bg-red-500/20 disabled:opacity-30 transition-colors"
                          >
                            reject
                          </button>
                        </span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      <p className="mt-2 text-[10px] font-mono text-gray-600">
        Rejected and reverted actions become negative few-shot examples in future advisor prompts;
        applied actions are outcome-scored against post-apply P&L.
      </p>
    </section>
  );
}
