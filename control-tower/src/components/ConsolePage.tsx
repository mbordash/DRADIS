'use client';

/**
 * ConsolePage — live view of the engine's recent log output.
 *
 * Reads GET /api/logs (in-memory ring buffer inside the engine — no Docker
 * socket, no file access) on a short poll. Built for AMI operators without
 * SSH: confirm the engine is alive, watch activity, and copy a snippet to
 * paste into a GitHub Issue.
 */

import { useEffect, useMemo, useRef, useState } from 'react';
import useSWR from 'swr';
import { getLogs } from '@/lib/api';

type Level = 'all' | 'info' | 'warn' | 'error';

const LEVEL_TESTS: Record<Exclude<Level, 'all'>, (l: string) => boolean> = {
  info:  l => l.includes(' INFO '),
  warn:  l => l.includes(' WARN '),
  error: l => l.includes(' ERROR '),
};

function lineColor(l: string): string {
  if (l.includes(' ERROR ')) return 'text-red-400';
  if (l.includes(' WARN '))  return 'text-amber-300';
  if (l.includes(' DEBUG ')) return 'text-gray-600';
  return 'text-gray-400';
}

export default function ConsolePage() {
  const [tail, setTail] = useState(500);
  const [level, setLevel] = useState<Level>('all');
  const [follow, setFollow] = useState(true);
  const [copied, setCopied] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  const { data, error, isLoading } = useSWR(['logs', tail], () => getLogs(tail), {
    refreshInterval: 3_000,
  });

  const lines = useMemo(() => {
    const all = data?.lines ?? [];
    return level === 'all' ? all : all.filter(LEVEL_TESTS[level]);
  }, [data, level]);

  // Follow mode: keep the viewport pinned to the newest lines.
  useEffect(() => {
    if (follow && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [lines, follow]);

  const copyVisible = async () => {
    try {
      await navigator.clipboard.writeText(lines.join('\n'));
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      /* clipboard unavailable (http origin) — ignore */
    }
  };

  const pill = (active: boolean) =>
    `px-2.5 py-1 rounded-lg text-xs font-mono border transition-colors cursor-pointer ${
      active
        ? 'bg-indigo-500/20 border-indigo-500/40 text-indigo-300'
        : 'bg-transparent border-[#1e1e32] text-gray-500 hover:text-gray-300'
    }`;

  return (
    <div className="space-y-4">
      {/* ── Toolbar ─────────────────────────────────────────────────────────── */}
      <div className="card px-4 py-3 flex flex-wrap items-center gap-2">
        <span className="text-xs text-gray-500 font-mono mr-1">Level:</span>
        {(['all', 'info', 'warn', 'error'] as Level[]).map(v => (
          <button key={v} className={pill(level === v)} onClick={() => setLevel(v)}>
            {v.toUpperCase()}
          </button>
        ))}
        <span className="text-xs text-gray-500 font-mono ml-4 mr-1">Tail:</span>
        {[200, 500, 2000].map(n => (
          <button key={n} className={pill(tail === n)} onClick={() => setTail(n)}>
            {n}
          </button>
        ))}
        <label className="flex items-center gap-1.5 ml-4 text-xs font-mono text-gray-500 cursor-pointer">
          <input
            type="checkbox"
            className="h-3.5 w-3.5 accent-indigo-500"
            checked={follow}
            onChange={e => setFollow(e.target.checked)}
          />
          Follow
        </label>
        <div className="flex-1" />
        <button className={pill(false)} onClick={copyVisible}>
          {copied ? '✓ Copied' : '⧉ Copy visible'}
        </button>
      </div>

      {/* ── Log pane ────────────────────────────────────────────────────────── */}
      <div className="card overflow-hidden">
        <div className="px-4 pt-3 pb-2 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-indigo-400 text-base">🖥️</span>
            <p className="label-muted">Engine Console</p>
          </div>
          <span className="text-xs font-mono text-gray-600">
            {error ? 'engine unreachable' : isLoading ? 'Loading…' : `${lines.length} lines · refreshes every 3s`}
          </span>
        </div>
        <div
          ref={scrollRef}
          onWheel={() => setFollow(false)}
          className="h-[65vh] overflow-y-auto bg-[#0a0a12] border-t border-[#1e1e32] px-4 py-3 font-mono text-[11px] leading-relaxed"
        >
          {lines.length === 0 && !isLoading ? (
            <p className="text-gray-600">No log lines yet — the buffer fills as the engine runs.</p>
          ) : (
            lines.map((l, i) => (
              <div key={i} className={`whitespace-pre-wrap break-all ${lineColor(l)}`}>
                {l}
              </div>
            ))
          )}
        </div>
      </div>

      <p className="text-[11px] text-gray-600 font-mono">
        Shows the engine&apos;s most recent in-memory log lines (up to 2,000). Sharing a snippet in a
        GitHub Issue? Use &ldquo;Copy visible&rdquo; with the ERROR filter — and skim it for market
        names or figures you&apos;d rather not post publicly.
      </p>
    </div>
  );
}
