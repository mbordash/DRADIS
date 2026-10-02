/**
 * The Helm view — DRADIS's record of why a trade was entered.
 *
 * Every other viper's rationale lives in its source and its config. A Helm
 * intent instead carries what the operator believed, how strongly, what would
 * have proved them wrong, and what the LLM said about that reasoning before the
 * money went in. None of that fits a trade row, which is why this is its own
 * screen rather than a section of the Tradelog: the log answers what executed,
 * this answers why it was entered.
 *
 * The page is ordered by what it is for. Live intents first, because those are
 * operations with money committed. Then the resolved record, read deliberately
 * against what actually happened. Calibration last, and only once enough
 * intents have resolved to mean anything.
 */
'use client';

import useSWR from 'swr';
import HelmIntentsPanel from './HelmIntentsPanel';
import { getHelmSummary } from '@/lib/api';

export default function HelmPage() {
  const { data: summary } = useSWR('helm-summary', getHelmSummary, { refreshInterval: 10_000 });

  const resolved = summary?.resolved ?? 0;
  const needed = summary?.calibration_min_resolved ?? 30;
  const pct = needed > 0 ? Math.min(100, Math.round((resolved / needed) * 100)) : 0;

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-sm font-mono uppercase tracking-wide text-teal-300">🧭 Helm</h2>
        <p className="text-[11px] font-mono text-gray-500 mt-1 max-w-3xl">
          Convictions you took the helm for, with the thesis as first submitted. Intents outlive their
          squadrons, so the record stays here after a squadron retires.
        </p>
      </div>

      <section className="space-y-2">
        <h3 className="text-xs font-mono uppercase tracking-wide text-gray-400">Live</h3>
        <p className="text-[10px] font-mono text-gray-600">
          Money committed now, under a posture you can still revise.
        </p>
        <HelmIntentsPanel
          bare
          show="live"
          emptyText="Nothing live. Take the Helm from the CAG overview to open an intent."
        />
      </section>

      <section className="space-y-2">
        <h3 className="text-xs font-mono uppercase tracking-wide text-gray-400">Resolved</h3>
        <p className="text-[10px] font-mono text-gray-600">
          How each conviction ended, and whether the critique named the failure that actually happened.
        </p>
        <HelmIntentsPanel
          bare
          show="resolved"
          emptyText="No resolved intents yet."
        />
      </section>

      <section className="space-y-2">
        <h3 className="text-xs font-mono uppercase tracking-wide text-gray-400">Calibration</h3>
        <div className="card p-4 border border-[#1e1e32] bg-[#0d0d1a] space-y-2">
          {/*
            Deliberately shows no figure until the sample supports one. Ten
            resolved intents with stated probabilities would invite a
            calibration claim the sample cannot carry, and Helm produces a
            handful a month, so the temptation would arrive long before the
            evidence. Everything is recorded from the first intent; only the
            display waits.
          */}
          <p className="text-xs font-mono text-gray-300">
            {resolved} of {needed} resolved intents
          </p>
          <div className="h-1.5 bg-[#1e1e32] rounded overflow-hidden">
            <div className="h-full bg-teal-500/60" style={{ width: `${pct}%` }} />
          </div>
          {summary?.calibration_visible && summary.calibration ? (
            <pre className="text-[11px] font-mono text-gray-300 whitespace-pre-wrap">
              {JSON.stringify(summary.calibration, null, 2)}
            </pre>
          ) : (
            <p className="text-[10px] font-mono text-gray-500">
              Stated confidence against realized outcome, and the critique&apos;s hit rate, appear once{' '}
              {needed} intents have resolved. Recorded from the first one; a figure drawn from fewer
              would read as evidence without being any.
            </p>
          )}
        </div>
      </section>
    </div>
  );
}
