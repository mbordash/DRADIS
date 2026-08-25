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
 * LLM Advisor — compact summary strip for the Main view.
 *
 * One row: latest analysis metadata (time, model, P&L at analysis) plus a
 * "N proposals pending" badge that jumps to the AI Actions tab (the detail
 * surface for the approval queue and audit trail). The full prose analysis
 * expands inline on demand; older analyzes are browsable while expanded.
 */

import { useState } from 'react';
import type { LlmRecommendationRow } from '@/lib/types';

interface Props {
  recommendations: LlmRecommendationRow[];
  isLoading: boolean;
  advisorEnabled: boolean;
  /** Count of AI config proposals awaiting approval (status 'proposed'). */
  pendingCount?: number;
  /** Navigate to the AI Actions view (approval queue + audit trail). */
  onGoToActions?: () => void;
}

/** Format an ISO timestamp to a short local string, e.g. "May 11, 14:32" */
function fmtTs(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString('en-US', {
      month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit', hour12: false,
    });
  } catch {
    return iso;
  }
}

export default function LlmAdvisorCard({
  recommendations, isLoading, advisorEnabled,
  pendingCount = 0, onGoToActions,
}: Props) {
  const [expanded, setExpanded] = useState(false);
  const [idx, setIdx] = useState(0);

  const total = recommendations.length;
  const safeIdx = total > 0 ? Math.min(idx, total - 1) : 0;
  const rec = total > 0 ? recommendations[safeIdx] : null;

  return (
    <section>
      <div className="card px-4 py-3">
        {/* Summary strip */}
        <div className="flex flex-wrap items-center gap-3">
          <span className="label-muted">LLM Advisor</span>
          <span className="text-xs font-mono text-gray-600">🤖</span>

          {!advisorEnabled && (
            <span className="text-[10px] font-mono bg-gray-800 text-gray-500 border border-gray-700 rounded px-1.5 py-0.5">
              DISABLED
            </span>
          )}

          {isLoading ? (
            <span className="text-xs font-mono text-gray-600">Loading…</span>
          ) : rec ? (
            <>
              <span className="text-xs font-mono text-gray-400">{fmtTs(rec.ts)}</span>
              <span className="text-[10px] font-mono bg-violet-500/10 text-violet-400 border border-violet-500/20 rounded px-1.5 py-0.5">
                {rec.model}
              </span>
              <span className="text-[10px] font-mono text-gray-600 hidden sm:inline">
                {rec.trade_count} trade{rec.trade_count !== 1 ? 's' : ''}
              </span>
              {!rec.is_current_session && (
                <span className="text-[10px] font-mono bg-gray-800 text-gray-500 border border-gray-700 rounded px-1.5 py-0.5">
                  PRIOR SESSION
                </span>
              )}
              <span className={`text-[10px] font-mono ${
                parseFloat(rec.session_pnl) >= 0 ? 'text-green-500' : 'text-red-500'
              }`}>
                P&L {parseFloat(rec.session_pnl) >= 0 ? '+' : ''}${parseFloat(rec.session_pnl).toFixed(2)}
              </span>
            </>
          ) : (
            <span className="text-xs font-mono text-gray-600">
              {advisorEnabled ? 'awaiting first analysis' : 'enable in config.rs + rebuild'}
            </span>
          )}

          {/* Right cluster: pending badge + expand toggle */}
          <span className="ml-auto flex items-center gap-2">
            {pendingCount > 0 && (
              <button
                onClick={onGoToActions}
                className="text-[10px] font-mono px-2 py-0.5 rounded border bg-amber-500/10 text-amber-400 border-amber-500/20 hover:bg-amber-500/20 transition-colors"
                title="Review in the AI Actions view"
              >
                ⏳ {pendingCount} proposal{pendingCount !== 1 ? 's' : ''} pending →
              </button>
            )}
            {onGoToActions && pendingCount === 0 && (
              <button
                onClick={onGoToActions}
                className="text-[10px] font-mono text-gray-600 hover:text-gray-400 underline underline-offset-2 transition-colors"
                title="Open the AI Actions audit trail"
              >
                AI actions
              </button>
            )}
            {rec && (
              <button
                onClick={() => setExpanded(v => !v)}
                className="text-xs px-2 py-0.5 rounded bg-[#13131f] border border-[#1e1e32] text-gray-400 hover:text-gray-200 transition-colors"
                title={expanded ? 'Collapse analysis' : 'Read the full analysis'}
              >
                {expanded ? 'collapse' : 'read'}
              </button>
            )}
          </span>
        </div>

        {/* Expanded prose */}
        {expanded && rec && (
          <div className="mt-3 pt-3 border-t border-[#1e1e32]">
            {total > 1 && (
              <div className="flex items-center gap-2 mb-2">
                <span className="text-xs text-gray-600 font-mono">{safeIdx + 1} / {total}</span>
                <button
                  onClick={() => setIdx(i => Math.min(i + 1, total - 1))}
                  disabled={safeIdx >= total - 1}
                  className="text-xs px-2 py-0.5 rounded bg-[#13131f] border border-[#1e1e32] text-gray-400 hover:text-gray-200 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
                  title="Older"
                >
                  ←
                </button>
                <button
                  onClick={() => setIdx(i => Math.max(i - 1, 0))}
                  disabled={safeIdx === 0}
                  className="text-xs px-2 py-0.5 rounded bg-[#13131f] border border-[#1e1e32] text-gray-400 hover:text-gray-200 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
                  title="Newer"
                >
                  →
                </button>
              </div>
            )}
            <pre className="text-xs text-gray-300 font-mono whitespace-pre-wrap leading-relaxed overflow-y-auto max-h-96 scrollbar-thin">
              {rec.analysis}
            </pre>
          </div>
        )}
      </div>
    </section>
  );
}
