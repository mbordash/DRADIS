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

  const total = recommendations.length;
  const rec   = total > 0 ? recommendations[idx] : null;

  // Reset index when new data arrives and current index is out of bounds
  if (idx >= total && total > 0) setIdx(0);

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
        </div>

        {/* Pagination — only shown when there are multiple entries */}
        {total > 1 && (
          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-600 font-mono">
              {idx + 1} / {total}
            </span>
            <button
              onClick={() => setIdx(i => Math.min(i + 1, total - 1))}
              disabled={idx >= total - 1}
              className="text-xs px-2 py-0.5 rounded bg-[#13131f] border border-[#1e1e32] text-gray-400 hover:text-gray-200 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
              title="Older"
            >
              ←
            </button>
            <button
              onClick={() => setIdx(i => Math.max(i - 1, 0))}
              disabled={idx === 0}
              className="text-xs px-2 py-0.5 rounded bg-[#13131f] border border-[#1e1e32] text-gray-400 hover:text-gray-200 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
              title="Newer"
            >
              →
            </button>
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
              {advisorEnabled
                ? <>
                    Awaiting first analysis — fires every 30 min once at least{' '}
                    <span className="text-gray-400 font-mono">5</span> session trades have completed,
                    or supplemented by prior-session history.
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

