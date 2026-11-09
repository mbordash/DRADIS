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
 * [E96] Importing a viper from a bundle (a file, or a version from the
 * community registry): the review the operator reads before anything is
 * written. It shows who wrote it (verified when the registry signed it),
 * whether the bundle is intact, the readme (as plain text: it is someone else's writing and
 * is never rendered as HTML), every parameter, the full source, and, when it
 * would replace a viper of the same name, a diff against that one.
 *
 * Nothing here fetches a file from anywhere: the operator picks a file (or
 * the engine fetched a registry version from its compiled-in registry), and
 * the engine receives the bundle as a request body and checks it again.
 */

import { useEffect, useMemo, useState } from 'react';
import { checkImport, importBundle } from '@/lib/api';
import type { ImportCheck, ScriptManifest, ScriptParam, ViperBundle } from '@/lib/types';
import { toast } from './Toast';

const inputCls = 'w-full bg-[#0a0a14] border border-[#1e1e32] rounded px-2 py-1.5 text-xs font-mono text-gray-200 focus:outline-none focus:border-teal-500/50 disabled:opacity-50';
const labelCls = 'block text-[10px] font-mono uppercase tracking-wide text-gray-500 mb-1';
const btn = 'px-3 py-1.5 rounded border text-xs font-mono transition-colors disabled:opacity-40 disabled:cursor-not-allowed';
const btnPrimary = `${btn} border-teal-500/50 bg-teal-500/10 text-teal-200 hover:bg-teal-500/20`;
const btnPlain = `${btn} border-[#1e1e32] text-gray-300 hover:bg-white/[0.03]`;

/** Largest file the page will read; the engine refuses bigger bundles anyway. */
const MAX_FILE_BYTES = 128 * 1024;

const val = (v: unknown) => (v === null || v === undefined || v === '' ? '—' : String(v));

// ── Diff ─────────────────────────────────────────────────────────────────────

type DiffLine = { kind: ' ' | '+' | '-'; text: string };

/** A line diff by longest common subsequence. Scripts are capped at 64 KB, so
 *  the table stays small enough to build in the browser. */
export function lineDiff(a: string, b: string): DiffLine[] {
  const x = a.replace(/\r\n?/g, '\n').split('\n');
  const y = b.replace(/\r\n?/g, '\n').split('\n');
  const n = x.length, m = y.length;
  const t: Uint32Array[] = Array.from({ length: n + 1 }, () => new Uint32Array(m + 1));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      t[i][j] = x[i] === y[j] ? t[i + 1][j + 1] + 1 : Math.max(t[i + 1][j], t[i][j + 1]);
    }
  }
  const out: DiffLine[] = [];
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (x[i] === y[j]) { out.push({ kind: ' ', text: x[i] }); i++; j++; }
    else if (t[i + 1][j] >= t[i][j + 1]) { out.push({ kind: '-', text: x[i++] }); }
    else { out.push({ kind: '+', text: y[j++] }); }
  }
  while (i < n) out.push({ kind: '-', text: x[i++] });
  while (j < m) out.push({ kind: '+', text: y[j++] });
  return out;
}

/** Unchanged runs longer than this are folded, keeping context around changes. */
const CONTEXT = 3;

function DiffView({ before, after }: { before: string; after: string }) {
  const lines = useMemo(() => lineDiff(before, after), [before, after]);
  const changed = lines.some((l) => l.kind !== ' ');
  if (!changed) return <p className="text-[11px] font-mono text-gray-500">The source is identical.</p>;
  const keep = lines.map((_, i) => lines.slice(Math.max(0, i - CONTEXT), i + CONTEXT + 1).some((l) => l.kind !== ' '));
  const rows: React.ReactNode[] = [];
  for (let i = 0; i < lines.length; i++) {
    if (!keep[i]) {
      let k = i;
      while (k < lines.length && !keep[k]) k++;
      rows.push(<div key={`f${i}`} className="text-gray-600">⋯ {k - i} unchanged line{k - i === 1 ? '' : 's'}</div>);
      i = k - 1;
      continue;
    }
    const l = lines[i];
    const cls = l.kind === '+' ? 'bg-teal-500/10 text-teal-200' : l.kind === '-' ? 'bg-red-500/10 text-red-300' : 'text-gray-400';
    rows.push(<div key={i} className={`whitespace-pre ${cls}`}>{l.kind} {l.text}</div>);
  }
  return <div className="rounded border border-[#1e1e32] bg-[#0a0a14] p-2 text-[11px] font-mono overflow-x-auto max-h-96 overflow-y-auto">{rows}</div>;
}

function paramText(p?: ScriptParam) {
  if (!p) return '';
  return `${p.type}, default ${val(p.default)}, ${val(p.min)}–${val(p.max)}`;
}

function ParamDiff({ before, after }: { before: ScriptManifest; after: ScriptManifest }) {
  const names = Array.from(new Set([...Object.keys(before.params ?? {}), ...Object.keys(after.params ?? {})])).sort();
  const rows = names.map((k) => {
    const a = paramText(before.params?.[k]);
    const b = paramText(after.params?.[k]);
    return { k, a, b, kind: !a ? 'added' : !b ? 'removed' : a !== b ? 'changed' : '' };
  }).filter((r) => r.kind);
  const top: { label: string; a: string; b: string }[] = [
    { label: 'classes', a: before.classes.join(', '), b: after.classes.join(', ') },
    { label: 'venues', a: before.venues.join(', ') || 'all', b: after.venues.join(', ') || 'all' },
    { label: 'market', a: before.market ?? 'hourly', b: after.market ?? 'hourly' },
    { label: 'cap', a: `$${val(before.max_exposure_usdc)}`, b: `$${val(after.max_exposure_usdc)}` },
  ].filter((r) => r.a !== r.b);
  if (!rows.length && !top.length) return <p className="text-[11px] font-mono text-gray-500">No manifest or parameter changes.</p>;
  return (
    <div className="text-[11px] font-mono space-y-1">
      {top.map((r) => (
        <div key={r.label}><span className="text-gray-500">{r.label}:</span> <span className="text-red-300 line-through">{r.a}</span> → <span className="text-teal-200">{r.b}</span></div>
      ))}
      {rows.map((r) => (
        <div key={r.k}>
          <span className="text-gray-300">{r.k}</span> <span className="text-gray-600">({r.kind})</span>{' '}
          {r.a && <span className="text-red-300 line-through">{r.a}</span>}{r.a && r.b && ' → '}{r.b && <span className="text-teal-200">{r.b}</span>}
        </div>
      ))}
    </div>
  );
}

// ── Import ───────────────────────────────────────────────────────────────────

/** Read a picked file as a bundle, or say why it is not one. */
export async function readBundleFile(f: File): Promise<ViperBundle> {
  if (f.size > MAX_FILE_BYTES) throw new Error(`${f.name} is ${Math.round(f.size / 1024)} KB; a viper bundle is at most ${MAX_FILE_BYTES / 1024} KB.`);
  let j: unknown;
  try { j = JSON.parse(await f.text()); } catch { throw new Error(`${f.name} is not JSON.`); }
  const b = j as Partial<ViperBundle>;
  if (!b || typeof b !== 'object' || typeof b.source !== 'string' || !b.manifest || typeof b.sha256 !== 'string') {
    throw new Error(`${f.name} is not a DRADIS viper bundle (no manifest, source or sha256).`);
  }
  return b as ViperBundle;
}

export function ImportPanel({ bundle, fileName, initialName, onDone, onCancel }: {
  bundle: ViperBundle;
  /** Where it came from, for the header: a file name or the registry. */
  fileName: string;
  /** The local name to start from (an update names the installed copy). */
  initialName?: string;
  /** Called with the local name after a successful import. */
  onDone: (name: string) => void;
  onCancel: () => void;
}) {
  const [name, setName] = useState(initialName ?? bundle.manifest.name);
  const [replace, setReplace] = useState(false);
  const [check, setCheck] = useState<ImportCheck | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [view, setView] = useState<'source' | 'diff'>('source');

  // Re-check whenever the local name changes: it decides collisions and the
  // name rules. The bundle itself never changes in this panel.
  useEffect(() => {
    let live = true;
    const t = window.setTimeout(async () => {
      try {
        const c = await checkImport(bundle, name);
        if (!live) return;
        setCheck(c); setError(null);
        if (!c.exists) setReplace(false);
      } catch (e) {
        if (live) { setCheck(null); setError(e instanceof Error ? e.message : String(e)); }
      }
    }, 250);
    return () => { live = false; window.clearTimeout(t); };
  }, [bundle, name]);

  const blocked = !check || !check.ok || (check.exists && !replace);

  const run = async () => {
    if (!check) return;
    setBusy(true);
    try {
      const r = await importBundle(bundle, name, replace);
      toast.success(replace ? `Replaced ${r.name}: back on paper, memory cleared.` : `Imported ${r.name}: off and on paper. Switch it on in Settings when ready.`);
      onDone(r.name);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      toast.error('Import failed. Nothing was changed.');
    } finally {
      setBusy(false);
    }
  };

  const m = check?.manifest ?? bundle.manifest;
  return (
    <div className="card p-4 border border-[#1e1e32] space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <div className="text-sm font-mono text-gray-100">Import {m.display_name || m.name}</div>
          <div className="text-[10px] font-mono text-gray-500">
            from {fileName}
            {bundle.publisher ? ` · by ${bundle.publisher}` : ''}
            {bundle.version ? ` · version ${bundle.version}` : ''}
            {` · license ${bundle.license ?? 'MIT'}`}
          </div>
        </div>
        {check?.signed ? (
          <span className="text-[9px] font-mono uppercase border rounded px-1 text-teal-300 border-teal-500/40"
                title={`Signed by the dradis.live registry (key ${check.signed.key_id}) on ${check.signed.published_at}. The registry reviewed it before publishing; it did not test it for profitability.`}>
            signed · dradis.live
          </span>
        ) : (
          <span className="text-[9px] font-mono uppercase border rounded px-1 text-amber-300 border-amber-500/40"
                title="Not signed by the registry. The digest proves the file is intact, not who wrote it.">
            unsigned file
          </span>
        )}
      </div>

      <p className="text-[11px] font-mono text-gray-400">
        {check?.signed
          ? <>Published by <span className="text-gray-200">{check.signed.publisher}</span> (verified by the registry&apos;s signature). Community content, MIT licensed: not investment advice, and not reviewed for profitability. </>
          : bundle.publisher
            ? <>The publisher name comes from the file and is not verified. </>
            : null}
        An imported viper arrives <span className="text-gray-200">switched off on every squadron and on paper</span>. Read the
        script before you switch it on: it trades under the same caps and checks as your own vipers.
      </p>

      {check && (
        <div className="text-[11px] font-mono text-gray-500">
          digest <span className="text-gray-300">{check.digest.slice(0, 16)}…</span>
          {check.problems.some((p) => p.includes('does not match'))
            ? <span className="text-red-300"> · does not match the file&apos;s sha256</span>
            : <span className="text-teal-300"> · matches the file: unchanged since export</span>}
        </div>
      )}

      {check && !check.runs_here && (
        <div className="rounded border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-[11px] font-mono text-amber-300">
          This viper is declared for {m.venues.join(', ')}, not this engine&apos;s venue ({check.venue}). It can be imported and
          read, but it will not attach to any squadron here unless you add {check.venue} to its venues.
        </div>
      )}

      {(error || (check && check.problems.length > 0)) && (
        <div className="rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs font-mono text-red-300 space-y-1">
          {error && <div className="whitespace-pre-wrap">{error}</div>}
          {check?.problems.map((p, i) => <div key={i} className="whitespace-pre-wrap">{p}</div>)}
        </div>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <div>
          <label className={labelCls} htmlFor="import-name">Name here</label>
          <input id="import-name" className={inputCls} value={name} disabled={busy}
                 onChange={(e) => setName(e.target.value.toLowerCase())} />
          {check?.exists && (
            <div className="mt-2 space-y-1 text-[11px] font-mono">
              <p className="text-amber-300">You already have a viper named {name}.</p>
              <label className="flex items-center gap-2 text-gray-300">
                <input type="radio" checked={!replace} onChange={() => { setReplace(false); setName(check.free_name); }} disabled={busy} />
                Import as {check.free_name}
              </label>
              <label className="flex items-center gap-2 text-gray-300">
                <input type="radio" checked={replace} onChange={() => setReplace(true)} disabled={busy} />
                Replace {name} (keeps its on/off switches, goes back to paper, clears its memory)
              </label>
            </div>
          )}
        </div>
        <div className="text-[11px] font-mono text-gray-400 space-y-0.5">
          <div><span className="text-gray-500">classes:</span> {m.classes.join(', ')}</div>
          <div><span className="text-gray-500">venues:</span> {m.venues.length ? m.venues.join(', ') : 'all'}</div>
          <div><span className="text-gray-500">market:</span> {m.market ?? 'hourly'}</div>
          <div><span className="text-gray-500">cap:</span> ${val(m.max_exposure_usdc)}</div>
          <div><span className="text-gray-500">script API level:</span> {m.api_version ?? 1}</div>
        </div>
      </div>

      {bundle.readme && (
        <div>
          <p className={labelCls}>Readme (plain text)</p>
          <pre className="rounded border border-[#1e1e32] bg-[#0a0a14] p-2 text-[11px] font-mono text-gray-300 whitespace-pre-wrap max-h-64 overflow-y-auto">{bundle.readme}</pre>
        </div>
      )}

      <div>
        <p className={labelCls}>Parameters</p>
        {Object.keys(m.params ?? {}).length === 0
          ? <p className="text-[11px] font-mono text-gray-500">None.</p>
          : (
            <div className="text-[11px] font-mono space-y-0.5">
              {Object.entries(m.params).map(([k, p]) => (
                <div key={k}><span className="text-gray-200">{k}</span> <span className="text-gray-500">{paramText(p)}</span>{p.description ? <span className="text-gray-600"> · {p.description}</span> : null}</div>
              ))}
            </div>
          )}
      </div>

      <div className="space-y-2">
        <div className="flex gap-1">
          <button className={`${btn} ${view === 'source' ? 'border-teal-500/50 bg-teal-500/10 text-teal-200' : 'border-[#1e1e32] text-gray-400'}`} onClick={() => setView('source')}>Source</button>
          {check?.current && (
            <button className={`${btn} ${view === 'diff' ? 'border-teal-500/50 bg-teal-500/10 text-teal-200' : 'border-[#1e1e32] text-gray-400'}`} onClick={() => setView('diff')}>
              Changes from your {name}
            </button>
          )}
        </div>
        {view === 'source' || !check?.current ? (
          <pre className="rounded border border-[#1e1e32] bg-[#0a0a14] p-2 text-[11px] font-mono text-gray-300 whitespace-pre overflow-x-auto max-h-96 overflow-y-auto">{check?.source ?? bundle.source}</pre>
        ) : (
          <div className="space-y-2">
            <ParamDiff before={check.current.manifest} after={check.manifest} />
            <DiffView before={check.current.source} after={check.source} />
          </div>
        )}
      </div>

      <div className="flex flex-wrap gap-2">
        <button className={btnPrimary} disabled={busy || blocked} onClick={run}>
          {busy ? 'Importing…' : replace ? `Replace ${name}` : 'Import (off, on paper)'}
        </button>
        <button className={btnPlain} disabled={busy} onClick={onCancel}>Cancel</button>
      </div>
    </div>
  );
}
