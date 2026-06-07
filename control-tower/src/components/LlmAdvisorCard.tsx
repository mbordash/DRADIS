'use client';

import { useState } from 'react';
import type { LlmRecommendationRow } from '@/lib/types';

interface Props {
  recommendations: LlmRecommendationRow[];
  isLoading: boolean;
  advisorEnabled: boolean;
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

export default function LlmAdvisorCard({ recommendations, isLoading, advisorEnabled }: Props) {
  const [idx, setIdx] = useState(0);
  const [dismissed, setDismissed] = useState<Set<number>>(new Set());

  // Filter out dismissed recommendations
  const visible = recommendations.filter(r => !dismissed.has(r.id));
  const total   = visible.length;
  const rec     = total > 0 ? visible[idx] : null;

  // Reset index when list shrinks (e.g. after dismiss)
  const safeIdx = total > 0 ? Math.min(idx, total - 1) : 0;
  if (safeIdx !== idx) setIdx(safeIdx);

  const dismiss = (id: number) => {
    setDismissed(prev => new Set(prev).add(id));
    // If we dismissed the last item, step back
    setIdx(i => (total > 1 ? Math.min(i, total - 2) : 0));
  };

  return (
    <section>
      {/* Section header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <p className="label-muted">LLM Advisor</p>
          <span className="text-xs font-mono text-gray-600">🤖</span>
          {!advisorEnabled && (
            <span className="text-[10px] font-mono bg-gray-800 text-gray-500 border border-gray-700 rounded px-1.5 py-0.5">
              DISABLED
            </span>
          )}
          {dismissed.size > 0 && (
            <button
              onClick={() => { setDismissed(new Set()); setIdx(0); }}
              className="text-[10px] font-mono text-gray-600 hover:text-gray-400 underline underline-offset-2 transition-colors"
              title="Restore all dismissed recommendations"
            >
              restore {dismissed.size} dismissed
            </button>
          )}
        </div>

        {/* Pagination + dismiss — only shown when there are visible entries */}
        {total > 0 && (
          <div className="flex items-center gap-2">
            {total > 1 && (
              <>
                <span className="text-xs text-gray-600 font-mono">
                  {safeIdx + 1} / {total}
                </span>
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
              </>
            )}
            {rec && (
              <button
                onClick={() => dismiss(rec.id)}
                className="text-xs px-2 py-0.5 rounded bg-[#13131f] border border-[#1e1e32] text-gray-500 hover:text-red-400 hover:border-red-900/50 disabled:opacity-30 transition-colors"
                title="Dismiss this recommendation"
              >
                ✕
              </button>
            )}
          </div>
        )}
      </div>

      {/* Card body */}
      <div className="card p-4">
        {isLoading ? (
          <div className="flex items-center justify-center h-24 text-gray-600 text-sm">
            Loading recommendations…
          </div>
        ) : !rec ? (
          /* Empty state */
          <div className="flex flex-col items-center justify-center gap-2 py-8 text-center">
            <span className="text-3xl">🤖</span>
            <p className="text-sm text-gray-500">
              {dismissed.size > 0
                ? <>All recommendations dismissed. <button onClick={() => { setDismissed(new Set()); setIdx(0); }} className="text-gray-400 underline underline-offset-2 hover:text-gray-200 transition-colors">Restore</button>.</>
                : advisorEnabled
                  ? <>
                      Awaiting first analysis (if enabled in config).
                      Analyses run regardless of trade count — even zero trades triggers a
                      settings-stringency review.
                      Check logs for{' '}
                      <code className="text-xs bg-[#13131f] px-1 rounded">🤖 LLM Advisor</code>{' '}
                      lines to trace progress.
                    </>
                  : <>LLM Advisor is disabled. Set <code className="text-xs bg-[#13131f] px-1 rounded">ENABLE_LLM_ADVISOR&nbsp;=&nbsp;true</code> in <code className="text-xs bg-[#13131f] px-1 rounded">config.rs</code> and rebuild.</>
              }
            </p>
          </div>
        ) : (
          <>
            {/* Metadata row */}
            <div className="flex flex-wrap items-center gap-3 mb-3 pb-3 border-b border-[#1e1e32]">
              <span className="text-xs font-mono text-gray-400">{fmtTs(rec.ts)}</span>
              <span className="text-[10px] font-mono bg-violet-500/10 text-violet-400 border border-violet-500/20 rounded px-1.5 py-0.5">
                {rec.model}
              </span>
              <span className="text-[10px] font-mono text-gray-600">
                {rec.trade_count} trade{rec.trade_count !== 1 ? 's' : ''} analysed
              </span>
              {!rec.is_current_session && (
                <span className="text-[10px] font-mono bg-gray-800 text-gray-500 border border-gray-700 rounded px-1.5 py-0.5">
                  PRIOR SESSION
                </span>
              )}
              <span className={`text-[10px] font-mono ml-auto ${
                parseFloat(rec.session_pnl) >= 0 ? 'text-green-500' : 'text-red-500'
              }`}>
                P&L {parseFloat(rec.session_pnl) >= 0 ? '+' : ''}${parseFloat(rec.session_pnl).toFixed(2)}
              </span>
            </div>

            {/* Analysis text — preserve the LLM's formatting */}
            <pre className="text-xs text-gray-300 font-mono whitespace-pre-wrap leading-relaxed overflow-y-auto max-h-96 scrollbar-thin">
              {rec.analysis}
            </pre>
          </>
        )}
      </div>
    </section>
  );
}

