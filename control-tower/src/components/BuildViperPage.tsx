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
 * [E86] Build a Viper: the operator writes a strategy as a script, saves it,
 * and it flies, without a rebuild.
 *
 * A new viper paper-trades. It reaches the order path only when its own Live
 * switch is on, and it trades real money only while the instance is live as
 * well. The engine's adapter, not the script, enforces the cap, the daily
 * drawdown limit and every price and size check, so this page can afford to be
 * an editor rather than a gatekeeper.
 */

import { useEffect, useMemo, useRef, useState } from 'react';
import useSWR from 'swr';
import {
  deleteScript, dryRunScript, getConfig, getScript, getScriptTemplates, listScripts,
  patchConfig, resetScriptState, saveScript, validateScript,
} from '@/lib/api';
import type {
  CustomViperConfig, ScriptDryRun, ScriptList, ScriptListItem, ScriptManifest, ScriptParam, ScriptParamType, ScriptTemplate,
} from '@/lib/types';
import { useConfirm } from './ConfirmDialog';

const inputCls = 'w-full bg-[#0a0a14] border border-[#1e1e32] rounded px-2 py-1.5 text-xs font-mono text-gray-200 focus:outline-none focus:border-teal-500/50 disabled:opacity-50';
const labelCls = 'block text-[10px] font-mono uppercase tracking-wide text-gray-500 mb-1';
const btn = 'px-3 py-1.5 rounded border text-xs font-mono transition-colors disabled:opacity-40 disabled:cursor-not-allowed';
const btnPrimary = `${btn} border-teal-500/50 bg-teal-500/10 text-teal-200 hover:bg-teal-500/20`;
const btnPlain = `${btn} border-[#1e1e32] text-gray-300 hover:bg-white/[0.03]`;
const btnDanger = `${btn} border-red-500/40 text-red-300 hover:bg-red-500/10`;

const CLASSES = ['crypto', 'sports', 'politics', 'unknown'];
const EMPTY_CFG: CustomViperConfig = {};
const VENUES: { id: string; label: string }[] = [
  { id: 'kalshi', label: 'Kalshi' },
  { id: 'intl_clob', label: 'Polymarket International' },
  { id: 'us_retail', label: 'Polymarket US' },
];

type Tab = 'script' | 'settings' | 'test' | 'paper' | 'reference';

/** One parameter row. The id is stable, so renaming a parameter edits the row
 *  in place instead of remounting it (and losing focus) on every keystroke. */
interface ParamEntry { id: number; name: string; p: ScriptParam }

interface Draft {
  manifest: ScriptManifest;
  /** The editable parameters; `manifest.params` is rebuilt from these. */
  params: ParamEntry[];
  source: string;
  /** Not saved yet: the name may still change. */
  isNew: boolean;
}

let nextParamId = 1;
function toEntries(m: ScriptManifest): ParamEntry[] {
  return Object.entries(m.params ?? {}).map(([name, p]) => ({ id: nextParamId++, name, p }));
}
function draftOf(manifest: ScriptManifest, source: string, isNew: boolean): Draft {
  return { manifest, params: toEntries(manifest), source, isNew };
}

function num(v: unknown): string {
  if (v === null || v === undefined) return '';
  return String(v);
}

function money(v: string | number | null | undefined): string {
  const n = Number(v ?? 0);
  return `${n >= 0 ? '+' : '−'}$${Math.abs(n).toFixed(2)}`;
}

function problemsOf(e: unknown): string[] {
  const p = (e as { problems?: string[] })?.problems;
  if (Array.isArray(p) && p.length) return p;
  return [e instanceof Error ? e.message : String(e)];
}

// ── Code editor: a textarea with a line-number gutter ─────────────────────────

function CodeEditor({ value, onChange, disabled }: { value: string; onChange: (v: string) => void; disabled?: boolean }) {
  const gutter = useRef<HTMLDivElement>(null);
  const lines = value.split('\n').length;
  return (
    <div className="flex rounded border border-[#1e1e32] bg-[#07070f] overflow-hidden h-[460px]">
      <div
        ref={gutter}
        aria-hidden
        className="select-none text-right pr-2 pl-2 py-2 text-[11px] leading-[18px] font-mono text-gray-600 bg-[#0a0a14] border-r border-[#1e1e32] overflow-hidden"
      >
        {Array.from({ length: lines }, (_, i) => <div key={i}>{i + 1}</div>)}
      </div>
      <textarea
        value={value}
        disabled={disabled}
        spellCheck={false}
        aria-label="Script source"
        onChange={(e) => onChange(e.target.value)}
        onScroll={(e) => { if (gutter.current) gutter.current.scrollTop = e.currentTarget.scrollTop; }}
        onKeyDown={(e) => {
          // Tab indents instead of leaving the editor.
          if (e.key === 'Tab' && !e.shiftKey) {
            e.preventDefault();
            const t = e.currentTarget;
            const { selectionStart: a, selectionEnd: b } = t;
            const next = value.slice(0, a) + '    ' + value.slice(b);
            onChange(next);
            requestAnimationFrame(() => { t.selectionStart = t.selectionEnd = a + 4; });
          }
        }}
        className="flex-1 resize-none bg-transparent px-3 py-2 text-[12px] leading-[18px] font-mono text-gray-200 focus:outline-none whitespace-pre overflow-auto"
      />
    </div>
  );
}

// ── Manifest form ─────────────────────────────────────────────────────────────

function ParamRow({ name, p, onName, onChange, onRemove, disabled }: {
  name: string; p: ScriptParam; onName: (n: string) => void; onChange: (p: ScriptParam) => void;
  onRemove: () => void; disabled?: boolean;
}) {
  return (
    <div className="grid grid-cols-12 gap-2 items-start">
      <input className={`${inputCls} col-span-2`} value={name} disabled={disabled} placeholder="name"
             onChange={(e) => onName(e.target.value)} aria-label="Parameter name" />
      <select className={`${inputCls} col-span-2`} value={p.type} disabled={disabled} aria-label="Parameter type"
              onChange={(e) => {
                const type = e.target.value as ScriptParamType;
                onChange({ ...p, type, default: type === 'bool' ? false : 0 });
              }}>
        <option value="decimal">decimal</option>
        <option value="int">int</option>
        <option value="bool">bool</option>
      </select>
      {p.type === 'bool' ? (
        <select className={`${inputCls} col-span-2`} value={String(p.default)} disabled={disabled} aria-label="Default"
                onChange={(e) => onChange({ ...p, default: e.target.value === 'true' })}>
          <option value="true">true</option>
          <option value="false">false</option>
        </select>
      ) : (
        <input className={`${inputCls} col-span-2`} value={num(p.default)} disabled={disabled} placeholder="default"
               aria-label="Default" onChange={(e) => onChange({ ...p, default: e.target.value })} />
      )}
      <input className={`${inputCls} col-span-1`} value={num(p.min)} disabled={disabled || p.type === 'bool'} placeholder="min"
             aria-label="Minimum" onChange={(e) => onChange({ ...p, min: e.target.value === '' ? null : e.target.value })} />
      <input className={`${inputCls} col-span-1`} value={num(p.max)} disabled={disabled || p.type === 'bool'} placeholder="max"
             aria-label="Maximum" onChange={(e) => onChange({ ...p, max: e.target.value === '' ? null : e.target.value })} />
      <input className={`${inputCls} col-span-3`} value={p.description ?? ''} disabled={disabled} placeholder="what it does"
             aria-label="Description" onChange={(e) => onChange({ ...p, description: e.target.value })} />
      <button type="button" className={`${btnPlain} col-span-1`} disabled={disabled} onClick={onRemove} aria-label={`Remove ${name}`}>✕</button>
    </div>
  );
}

function ManifestForm({ draft, setDraft, disabled }: { draft: Draft; setDraft: (d: Draft) => void; disabled?: boolean }) {
  const m = draft.manifest;
  const set = (patch: Partial<ScriptManifest>) => setDraft({ ...draft, manifest: { ...m, ...patch } });
  const toggle = (list: string[], v: string) => (list.includes(v) ? list.filter((x) => x !== v) : [...list, v]);
  const setParams = (params: ParamEntry[]) => setDraft({ ...draft, params });
  const update = (id: number, patch: Partial<ParamEntry>) =>
    setParams(draft.params.map((e) => (e.id === id ? { ...e, ...patch } : e)));

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        <div>
          <label className={labelCls} htmlFor="viper-name">Name {draft.isNew ? '' : '(fixed once saved)'}</label>
          <input id="viper-name" className={inputCls} value={m.name} disabled={disabled || !draft.isNew}
                 onChange={(e) => set({ name: e.target.value.toLowerCase() })} placeholder="fade_spikes" />
        </div>
        <div>
          <label className={labelCls} htmlFor="viper-display">Display name</label>
          <input id="viper-display" className={inputCls} value={m.display_name} disabled={disabled}
                 onChange={(e) => set({ display_name: e.target.value })} />
        </div>
        <div>
          <label className={labelCls} htmlFor="viper-cap">Max exposure (USD)</label>
          <input id="viper-cap" className={inputCls} value={num(m.max_exposure_usdc)} disabled={disabled}
                 onChange={(e) => set({ max_exposure_usdc: e.target.value })} />
        </div>
      </div>
      <div>
        <label className={labelCls} htmlFor="viper-desc">Description</label>
        <input id="viper-desc" className={inputCls} value={m.description ?? ''} disabled={disabled}
               onChange={(e) => set({ description: e.target.value })} />
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <div>
          <label className={labelCls}>Market classes it flies</label>
          <div className="flex flex-wrap gap-3">
            {CLASSES.map((c) => (
              <label key={c} className="flex items-center gap-1.5 text-xs font-mono text-gray-300">
                <input type="checkbox" checked={m.classes.includes(c)} disabled={disabled}
                       onChange={() => set({ classes: toggle(m.classes, c) })} />
                {c}
              </label>
            ))}
          </div>
        </div>
        <div>
          <label className={labelCls}>Venues (none ticked: every venue)</label>
          <div className="flex flex-wrap gap-3">
            {VENUES.map((v) => (
              <label key={v.id} className="flex items-center gap-1.5 text-xs font-mono text-gray-300">
                <input type="checkbox" checked={m.venues.includes(v.id)} disabled={disabled}
                       onChange={() => set({ venues: toggle(m.venues, v.id) })} />
                {v.label}
              </label>
            ))}
          </div>
        </div>
      </div>
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <label className={labelCls}>Parameters (read as ctx.params.&lt;name&gt;; edited on the Settings tab)</label>
          <button type="button" className={btnPlain} disabled={disabled}
                  onClick={() => {
                    let i = draft.params.length + 1;
                    while (draft.params.some((e) => e.name === `param_${i}`)) i++;
                    setParams([...draft.params, { id: nextParamId++, name: `param_${i}`, p: { type: 'decimal', default: 0, description: '' } }]);
                  }}>
            + Parameter
          </button>
        </div>
        {draft.params.length === 0 && <p className="text-[10px] font-mono text-gray-600">No parameters.</p>}
        {draft.params.map((e) => (
          <ParamRow key={e.id} name={e.name} p={e.p} disabled={disabled}
                    onName={(n) => update(e.id, { name: n.toLowerCase() })}
                    onChange={(np) => update(e.id, { p: np })}
                    onRemove={() => setParams(draft.params.filter((x) => x.id !== e.id))} />
        ))}
      </div>
    </div>
  );
}

const finite = (v: unknown) => v !== '' && v !== null && v !== undefined && Number.isFinite(Number(v));

/** The manifest as the engine expects it (numbers as numbers), or the
 *  problems that would otherwise be sent silently: an unparsable bound would
 *  serialize as null and drop, a duplicate name would drop a parameter. */
function normalized(d: Draft): { manifest: ScriptManifest; problems: string[] } {
  const problems: string[] = [];
  const params: Record<string, ScriptParam> = {};
  for (const { name, p } of d.params) {
    if (!name.trim()) { problems.push('a parameter has no name'); continue; }
    if (params[name]) problems.push(`two parameters are named '${name}'`);
    for (const [label, v] of [['min', p.min], ['max', p.max]] as const) {
      if (v !== '' && v !== null && v !== undefined && !finite(v)) problems.push(`parameter '${name}': ${label} '${v}' is not a number`);
    }
    if (p.type !== 'bool') {
      if (!finite(p.default)) problems.push(`parameter '${name}': default '${num(p.default)}' is not a number`);
      else if (p.type === 'int' && !Number.isInteger(Number(p.default))) problems.push(`parameter '${name}': default ${num(p.default)} is not a whole number`);
    }
    params[name] = {
      ...p,
      default: p.type === 'bool' ? p.default === true || p.default === 'true' : Number(p.default),
      min: finite(p.min) ? Number(p.min) : null,
      max: finite(p.max) ? Number(p.max) : null,
    };
  }
  if (!finite(d.manifest.max_exposure_usdc) || Number(d.manifest.max_exposure_usdc) <= 0) {
    problems.push(`max exposure '${num(d.manifest.max_exposure_usdc)}' must be a number above 0`);
  }
  return { manifest: { ...d.manifest, max_exposure_usdc: Number(d.manifest.max_exposure_usdc), params }, problems };
}

// ── Settings: switches, cap and parameter values ──────────────────────────────

function SettingsPanel({ name, manifest, cfg, instanceGhost, paperCount, paperPnl, onSaved }: {
  name: string; manifest: ScriptManifest; cfg: CustomViperConfig; instanceGhost: boolean;
  paperCount: number; paperPnl: string; onSaved: () => void;
}) {
  const [confirm, confirmDialog] = useConfirm();
  const [cap, setCap] = useState(num(cfg.max_exposure_usdc));
  const [values, setValues] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const enabled = cfg.enabled ?? true;
  const live = cfg.live ?? false;

  // Keyed on content, not object identity: the list and config refresh in the
  // background, and a new object with the same values must not wipe what the
  // operator is typing.
  const cfgKey = JSON.stringify(cfg);
  const paramsKey = JSON.stringify(manifest.params ?? {});
  useEffect(() => {
    setCap(num(cfg.max_exposure_usdc));
    const v: Record<string, string> = {};
    for (const [k, p] of Object.entries(manifest.params ?? {})) v[k] = num(cfg.params?.[k] ?? p.default);
    setValues(v);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [name, cfgKey, paramsKey]);
  // The engine enforces the lower of the manifest's cap and the operator's.
  const manifestCap = Number(manifest.max_exposure_usdc);
  const effectiveCap = cfg.max_exposure_usdc != null && cfg.max_exposure_usdc !== ''
    ? Math.min(Number(cfg.max_exposure_usdc), manifestCap) : manifestCap;

  const patch = async (entry: CustomViperConfig) => {
    setBusy(true); setError(null);
    try {
      await patchConfig({ custom_vipers: { [name]: entry } });
      onSaved();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const setLive = async (on: boolean) => {
    if (on) {
      const ok = await confirm({
        title: `Put ${manifest.display_name} on live?`,
        tone: 'danger',
        confirmLabel: 'Go live',
        body: (
          <div className="space-y-2 text-xs font-mono text-gray-300">
            <p>
              {instanceGhost
                ? 'The instance is simulated, so its orders stay simulated until the instance itself goes live. Then they are real.'
                : 'The instance is live: this viper will place real orders with real money.'}
            </p>
            <p>
              It may hold up to <span className="text-gray-100">${effectiveCap}</span> at
              a time. The engine enforces that cap, the daily drawdown limit, and every price and size check, whatever the script returns.
            </p>
            <p>
              Its paper record so far: {paperCount} scored trade{paperCount === 1 ? '' : 's'}, {money(paperPnl)} net.
              {paperCount < 20 && ' That is too few trades to tell skill from luck.'}
            </p>
            <p className="text-gray-500">Taking it off live later stops new entries; what it holds keeps its stop and take-profit until it closes.</p>
          </div>
        ),
      });
      if (!ok) return;
    }
    await patch({ live: on });
  };

  const saveValues = async () => {
    // Checked here because the engine quietly falls back to the default for a
    // value it cannot use, and the operator would never know.
    const bad: string[] = [];
    const params: Record<string, number | boolean> = {};
    for (const [k, p] of Object.entries(manifest.params ?? {})) {
      const v = values[k];
      if (v === undefined || v === '') continue;
      if (p.type === 'bool') { params[k] = v === 'true'; continue; }
      const n = Number(v);
      if (!Number.isFinite(n)) { bad.push(`${k}: '${v}' is not a number`); continue; }
      if (p.type === 'int' && !Number.isInteger(n)) { bad.push(`${k}: ${v} is not a whole number`); continue; }
      if (p.min != null && p.min !== '' && n < Number(p.min)) { bad.push(`${k}: ${v} is below the minimum ${p.min}`); continue; }
      if (p.max != null && p.max !== '' && n > Number(p.max)) { bad.push(`${k}: ${v} is above the maximum ${p.max}`); continue; }
      params[k] = n;
    }
    if (cap !== '' && (!Number.isFinite(Number(cap)) || Number(cap) <= 0)) bad.push(`your cap '${cap}' must be a number above 0, or blank`);
    if (bad.length) { setError(bad.join('\n')); return; }
    await patch({ params, max_exposure_usdc: cap === '' ? null : cap });
  };

  return (
    <div className="space-y-4">
      {confirmDialog}
      {error && <div className="rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs font-mono text-red-300 whitespace-pre-wrap">{error}</div>}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <div className="card p-3 border border-[#1e1e32] space-y-2">
          <div className="flex items-center justify-between">
            <span className="text-xs font-mono text-gray-200">Enabled</span>
            <button className={enabled ? btnPrimary : btnPlain} disabled={busy} aria-pressed={enabled}
                    onClick={() => patch({ enabled: !enabled })}>{enabled ? 'On' : 'Off'}</button>
          </div>
          <p className="text-[10px] font-mono text-gray-500">
            Off stops new entries. Positions it holds keep their exits until they close or settle. This sets every
            squadron, including any you switched off on a squadron&apos;s own card.
          </p>
        </div>
        <div className={`card p-3 border space-y-2 ${live ? 'border-red-500/40' : 'border-[#1e1e32]'}`}>
          <div className="flex items-center justify-between">
            <span className="text-xs font-mono text-gray-200">Live</span>
            <button className={live ? btnDanger : btnPlain} disabled={busy} aria-pressed={live}
                    onClick={() => setLive(!live)}>{live ? 'Live' : 'Paper'}</button>
          </div>
          <p className="text-[10px] font-mono text-gray-500">
            {live
              ? instanceGhost
                ? 'Live, on a simulated instance: orders are simulated until the instance goes live.'
                : 'Live: real orders with real money.'
              : 'Paper: trades in its own ledger. Nothing reaches the venue.'}
          </p>
        </div>
      </div>

      <div className="card p-3 border border-[#1e1e32] space-y-3">
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          <div>
            <label className={labelCls}>Your cap (USD; blank: the manifest&apos;s ${num(manifest.max_exposure_usdc)})</label>
            <input className={inputCls} value={cap} onChange={(e) => setCap(e.target.value)} placeholder={num(manifest.max_exposure_usdc)} />
            <p className="text-[10px] font-mono text-gray-600 mt-1">The lower of the two applies.</p>
          </div>
        </div>
        {Object.keys(manifest.params ?? {}).length > 0 && (
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            {Object.entries(manifest.params).map(([k, p]) => (
              <div key={k}>
                <label className={labelCls}>
                  {k} <span className="normal-case text-gray-600">({p.type}{p.min != null || p.max != null ? `, ${num(p.min)}–${num(p.max)}` : ''})</span>
                </label>
                {p.type === 'bool' ? (
                  <select className={inputCls} value={values[k] ?? ''} onChange={(e) => setValues({ ...values, [k]: e.target.value })}>
                    <option value="true">true</option>
                    <option value="false">false</option>
                  </select>
                ) : (
                  <input className={inputCls} value={values[k] ?? ''} onChange={(e) => setValues({ ...values, [k]: e.target.value })} />
                )}
                {p.description && <p className="text-[10px] font-mono text-gray-600 mt-1">{p.description}</p>}
              </div>
            ))}
          </div>
        )}
        <div className="flex gap-2">
          <button className={btnPrimary} disabled={busy} onClick={saveValues}>Save values</button>
          <button className={btnPlain} disabled={busy}
                  onClick={async () => {
                    if (!(await confirm({ title: 'Reset the viper’s memory?', body: 'Clears the `this` state the script keeps between ticks, on every squadron. Positions are untouched.' }))) return;
                    try {
                      await resetScriptState(name);
                      setError(null);
                    } catch (e) {
                      setError(e instanceof Error ? e.message : String(e));
                    }
                  }}>
            Reset memory
          </button>
        </div>
        <p className="text-[10px] font-mono text-gray-600">
          A value outside the manifest&apos;s range, or of the wrong type, is ignored by the engine and the default applies.
        </p>
      </div>
    </div>
  );
}

// ── Where it runs ─────────────────────────────────────────────────────────────

const ago = (s: number | null | undefined) =>
  s == null ? '' : s < 60 ? `${s}s ago` : s < 3600 ? `${Math.floor(s / 60)}m ago` : `${Math.floor(s / 3600)}h ago`;

/** A status row this old means the squadron stopped ticking the viper. */
const STALE_SECS = 120;

/** Each running squadron of the viper's classes, and what the viper is doing
 *  there: joining, waiting for a market, ticking (with its last decision), or
 *  failing. The engine attaches a saved script on the squadron's next tick. */
function WhereItRuns({ item, squadrons, enabled, venue }: {
  item: ScriptListItem; squadrons: ScriptList['squadrons']; enabled: boolean; venue: string;
}) {
  const classes = item.manifest.classes ?? [];
  // An empty venue list means every venue.
  const venueOk = !item.manifest.venues?.length || item.manifest.venues.includes(venue);
  const venueLabel = VENUES.find((v) => v.id === venue)?.label ?? venue;
  const matching = venueOk ? squadrons.filter((s) => classes.includes(s.class)) : [];
  // Attached where its classes or venues no longer reach (the manifest was
  // narrowed): it stays on that squadron until the squadron moves on, and the
  // engine refuses it new entries there.
  const leftover = item.running_on.filter((a) => !matching.some((m) => m.squadron === a.squadron))
    .filter((a, i, all) => all.findIndex((x) => x.squadron === a.squadron) === i);

  const row = (squadron: string, cls: string, note?: string) => {
    // The newest, should a re-deployed squadron briefly have two loops.
    const a = item.running_on.filter((x) => x.squadron === squadron)
      .sort((x, y) => y.attached_at.localeCompare(x.attached_at))[0];
    const st = a?.status ?? null;
    let badge: { label: string; cls: string };
    let line: string | null = null;
    if (!a) {
      badge = { label: 'joining', cls: 'text-amber-300 border-amber-500/40' };
      line = 'Attaches on this squadron’s next tick.';
    } else if (!st) {
      badge = { label: 'attached', cls: 'text-teal-300 border-teal-500/40' };
      line = 'Waiting for its first tick here.';
    } else if (st.last_eval_secs_ago > STALE_SECS) {
      badge = { label: 'stalled', cls: 'text-red-300 border-red-500/40' };
      line = `No tick for ${ago(st.last_eval_secs_ago).replace(' ago', '')}. The squadron detail shows why.`;
    } else if (st.last_outcome === 'error' || st.last_outcome === 'timeout') {
      badge = { label: st.last_outcome, cls: 'text-red-300 border-red-500/40' };
      line = st.last_reason;
    } else if (st.last_reason === 'disabled in config') {
      badge = { label: 'off', cls: 'text-gray-500 border-gray-600/40' };
      line = 'Switched off on this squadron: no new entries; what it holds keeps its exits.';
    } else if (st.last_outcome === 'idle') {
      badge = { label: 'waiting', cls: 'text-gray-400 border-gray-600/40' };
      line = st.last_reason ?? 'waiting for a tradeable market';
    } else {
      badge = { label: st.last_outcome === 'signal' ? 'signal' : 'running', cls: 'text-teal-300 border-teal-500/40' };
      line = st.last_reason ? `${st.last_outcome === 'signal' ? 'entry' : 'hold'}: ${st.last_reason}` : null;
    }
    return (
      <div key={squadron} className="rounded border border-[#1e1e32] px-2 py-1.5 space-y-0.5">
        <div className="flex items-center justify-between gap-2">
          <span className="text-xs font-mono text-gray-200">
            {squadron} <span className="text-gray-600">· {cls}</span>
          </span>
          <span className="flex items-center gap-2">
            {st && <span className="text-[10px] font-mono text-gray-500">ticked {ago(st.last_eval_secs_ago)}</span>}
            <span className={`text-[9px] font-mono uppercase border rounded px-1 ${badge.cls}`}>{badge.label}</span>
          </span>
        </div>
        {line && <div className="text-[10px] font-mono text-gray-400 break-words">{line}</div>}
        {st && st.refusals.length > 0 && (
          <div className="text-[10px] font-mono text-gray-500">
            Refused: {st.refusals.slice(0, 3).map((r) => `${r.reason} ×${r.count}`).join(' · ')}
          </div>
        )}
        {note && <div className="text-[10px] font-mono text-amber-300/80">{note}</div>}
      </div>
    );
  };

  return (
    <div className="card p-3 border border-[#1e1e32] space-y-2">
      <p className={labelCls}>Where it runs</p>
      {!enabled && (
        <p className="text-[10px] font-mono text-amber-300">
          Switched off in Settings: it stays attached and manages what it holds, but takes no new entries.
        </p>
      )}
      {!venueOk && (
        <p className="text-[11px] font-mono text-amber-300">
          Its venues leave out {venueLabel}, which this instance trades, so it joins no squadron here. Add {venueLabel} under
          Venues, or leave Venues empty for all of them.
        </p>
      )}
      {venueOk && matching.length === 0 && leftover.length === 0 && (
        <p className="text-[11px] font-mono text-gray-500">
          No {classes.join(' or ')} squadron is running on this instance. It joins one on that squadron&apos;s first tick
          once you deploy it.
        </p>
      )}
      {matching.map((s) => row(s.squadron, s.class))}
      {leftover.map((a) => row(a.squadron, a.class,
        `Its ${venueOk ? `classes no longer include ${a.class}` : `venues no longer include ${venueLabel}`}. It stays on this squadron, with no new entries, until the squadron moves to a new market.`))}
    </div>
  );
}

// ── Test run ──────────────────────────────────────────────────────────────────

function TestRunPanel({ name, squadrons, unsavedSource, isNew, manifest, manifestProblems }: {
  name: string; squadrons: { squadron: string; context_at: string }[]; unsavedSource: string | null; isNew: boolean;
  manifest: ScriptManifest; manifestProblems: string[];
}) {
  const [squadron, setSquadron] = useState(squadrons[0]?.squadron ?? '');
  const [useDraft, setUseDraft] = useState(true);
  const [result, setResult] = useState<ScriptDryRun | null>(null);
  const [error, setError] = useState<string[] | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => { if (!squadron && squadrons[0]) setSquadron(squadrons[0].squadron); }, [squadrons, squadron]);

  const run = async () => {
    if (manifestProblems.length) { setError(manifestProblems); setResult(null); return; }
    setBusy(true); setError(null); setResult(null);
    try {
      const draft = useDraft && unsavedSource !== null;
      setResult(await dryRunScript(name, squadron, draft ? unsavedSource : undefined, draft || isNew ? manifest : undefined));
    } catch (e) {
      setError(problemsOf(e));
    } finally {
      setBusy(false);
    }
  };

  if (squadrons.length === 0) {
    return (
      <p className="text-xs font-mono text-gray-500">
        No squadron has run a script yet, so there is no market to test against.
        {isNew
          ? ' Save this viper (it paper-trades until you put it on live): it joins every running squadron of its classes on that squadron’s next tick, and the test run works from then.'
          : ' It joins a running squadron of its classes on that squadron’s next tick; Where it runs, on the Script tab, shows when.'}
      </p>
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-end gap-3">
        <div className="min-w-[200px]">
          <label className={labelCls}>Squadron</label>
          <select className={inputCls} value={squadron} onChange={(e) => setSquadron(e.target.value)}>
            {squadrons.map((s) => <option key={s.squadron} value={s.squadron}>{s.squadron}</option>)}
          </select>
        </div>
        {unsavedSource !== null && (
          <label className="flex items-center gap-1.5 text-xs font-mono text-gray-300 pb-1.5">
            <input type="checkbox" checked={useDraft} onChange={(e) => setUseDraft(e.target.checked)} />
            Use the editor&apos;s unsaved code
          </label>
        )}
        <button className={btnPrimary} disabled={busy || !squadron || (isNew && unsavedSource === null)} onClick={run}>
          {busy ? 'Running…' : 'Run'}
        </button>
      </div>
      <p className="text-[10px] font-mono text-gray-600">
        Evaluates entry(), and exit() for each paper position on that market, against the squadron&apos;s last market data,
        with this viper&apos;s parameters and a copy of its memory. Nothing is sent and nothing is recorded. ctx.exposure
        here counts paper positions only.
      </p>
      {error && <div className="rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs font-mono text-red-300 whitespace-pre-wrap">{error.join('\n')}</div>}
      {result && (
        <div className="space-y-2">
          <div className="text-[11px] font-mono text-gray-400">
            Market data from {new Date(result.context_at).toLocaleTimeString()} · {result.micros} µs
          </div>
          <div className={`rounded border px-3 py-2 text-xs font-mono whitespace-pre-wrap ${result.entry.error ? 'border-red-500/30 bg-red-500/10 text-red-300' : 'border-teal-500/30 bg-teal-500/5 text-teal-200'}`}>
            entry(): {result.entry.error ?? result.entry.ok}
          </div>
          {result.exits.map((x) => (
            <div key={x.paper_row} className={`rounded border px-3 py-2 text-xs font-mono ${x.exit.error ? 'border-red-500/30 text-red-300' : 'border-[#1e1e32] text-gray-300'}`}>
              exit() on paper {x.side} #{x.paper_row}: {x.exit.error ?? x.exit.ok}
            </div>
          ))}
          <p className="text-[10px] font-mono text-gray-600">{result.note}</p>
          <details>
            <summary className="text-[11px] font-mono text-gray-400 cursor-pointer">The ctx the script saw</summary>
            <pre className="mt-2 text-[11px] font-mono text-gray-400 whitespace-pre-wrap max-h-80 overflow-auto">
              {JSON.stringify(result.context, null, 2)}
            </pre>
          </details>
        </div>
      )}
    </div>
  );
}

// ── Paper ledger ──────────────────────────────────────────────────────────────

function PaperPanel({ name }: { name: string }) {
  const { data } = useSWR(['script', name], () => getScript(name), { refreshInterval: 15_000 });
  const rows = data?.paper_rows ?? [];
  const s = data?.paper;
  return (
    <div className="space-y-3">
      {s && (
        <div className="flex flex-wrap gap-4 text-xs font-mono text-gray-300">
          <span>{s.open} open</span>
          <span>{s.closed} closed</span>
          <span>{s.wins} of {s.scored} won</span>
          <span className={Number(s.pnl) >= 0 ? 'text-emerald-300' : 'text-red-300'}>{money(s.pnl)} net</span>
          <span className="text-gray-500">fees ${Number(s.fees).toFixed(2)}</span>
        </div>
      )}
      {rows.length === 0 ? (
        <p className="text-xs font-mono text-gray-500">No paper trades yet.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-[11px] font-mono">
            <thead className="text-gray-500 text-left">
              <tr>
                <th className="py-1 pr-3">Opened</th><th className="pr-3">Market</th><th className="pr-3">Side</th>
                <th className="pr-3">Entry</th><th className="pr-3">Shares</th><th className="pr-3">Exit</th>
                <th className="pr-3">P&amp;L</th><th className="pr-3">Why</th><th className="pr-3">Version</th>
              </tr>
            </thead>
            <tbody className="text-gray-300">
              {rows.map((r) => (
                <tr key={r.id} className="border-t border-[#1e1e32] align-top">
                  <td className="py-1 pr-3 whitespace-nowrap">{new Date(r.opened_at).toLocaleString()}</td>
                  <td className="pr-3">{r.market}</td>
                  <td className="pr-3">{r.side}</td>
                  <td className="pr-3">{Number(r.entry_price).toFixed(2)}</td>
                  <td className="pr-3">{Number(r.shares)}</td>
                  <td className="pr-3">{r.exit_price === null ? (r.closed_at ? '—' : 'open') : Number(r.exit_price).toFixed(2)}</td>
                  <td className={`pr-3 ${r.pnl === null ? 'text-gray-500' : Number(r.pnl) >= 0 ? 'text-emerald-300' : 'text-red-300'}`}>
                    {r.pnl === null ? (r.closed_at ? 'not scored' : '') : money(r.pnl)}
                  </td>
                  <td className="pr-3 text-gray-400">{r.exit_reason ?? r.entry_reason}</td>
                  <td className="pr-3 text-gray-500">{r.script_hash}{r.close_hash && r.close_hash !== r.script_hash ? `→${r.close_hash}` : ''}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Reference ─────────────────────────────────────────────────────────────────

function ReferencePanel() {
  const row = (k: string, v: string) => (
    <tr key={k} className="border-t border-[#1e1e32] align-top">
      <td className="py-1 pr-4 text-teal-300 whitespace-nowrap">{k}</td><td className="text-gray-400">{v}</td>
    </tr>
  );
  return (
    <div className="space-y-4 text-xs font-mono">
      <p className="text-gray-400">
        Numbers with a decimal point are exact decimals, the type the engine trades in. Whole numbers are integers and mix with
        decimals in arithmetic. A signal whose feed is not running on the squadron reads <span className="text-gray-200">()</span>, not 0.
      </p>
      <section>
        <h4 className="text-gray-300 mb-1">What a script returns</h4>
        <table className="w-full"><tbody>
          {row('entry(ctx) → ()', 'or #{ hold: "why" }: sit out; the reason shows on the viper card')}
          {row('entry(ctx) → #{ buy: … }', '#{ buy: "yes"|"no", price, shares, take_profit?, reason? }: a taker buy, limited to 2¢ over the ask')}
          {row('exit(ctx, pos) → ()', 'hold the position')}
          {row('exit(ctx, pos) → #{ sell: … }', '#{ sell: price, reason? }: a taker sale at or under the bid, never more than 2¢ under it')}
          {row('exit(ctx, pos) → #{ take_profit: p }', 'set or move the take-profit; the engine sells as a taker when the bid reaches it')}
          {row('this', 'the viper’s own memory, kept between ticks per squadron (e.g. this.last_entry = ctx.now_secs)')}
        </tbody></table>
      </section>
      <section>
        <h4 className="text-gray-300 mb-1">ctx</h4>
        <table className="w-full"><tbody>
          {row('market_name, venue', 'strings')}
          {row('market_class', '"crypto", "sports", "politics" or "unknown"; () on a squadron with no class')}
          {row('has_price_feed', 'true when this squadron has a price feed behind oracle and the other signals')}
          {row('strike', 'decimal, or () when the market has none')}
          {row('secs_to_close, market_age_secs, window_secs', 'integers (secs_to_close and window_secs may be ())')}
          {row('now_secs, book_age_secs', 'integers: the tick’s clock (the only one a script has) and the book’s age')}
          {row('yes, no', 'each #{ bid, ask, bid_depth, ask_depth, bid_depth_total, ask_depth_total, obi }')}
          {row('oracle', 'the price feed’s level in dollars; () without a feed')}
          {row('velocity, velocity_1s', 'the oracle’s move over 5 s and 1 s, in dollars (divide by ctx.oracle for a fraction)')}
          {row('acceleration', 'change in velocity since the previous tick, in dollars')}
          {row('drift_10m, drift_60m', 'the oracle’s move over 10 and 60 minutes, in dollars')}
          {row('hist_vol', 'a 0-1 flatness index (log-return spread over the hour ÷ 2%, ~0.003 for ordinary BTC), not a move')}
          {row('funding_rate, institutional_pulse, cvd_ratio, oi_delta_pct', 'derivatives and ETF-flow signals')}
          {row('tide_coherence, tradfi_velocity, vix_proxy', 'macro signals (BTC, US hours)')}
          {row('sports', '#{ yes, no } each #{ consensus, num_books, dispersion, odds_age_secs, secs_to_start, league }; () until the sports ledger matches the market')}
          {row('collateral, day_pnl', 'the account')}
          {row('ghost', 'true when this viper’s orders are not real right now')}
          {row('exposure, max_exposure', 'this viper’s open cost basis across every squadron, and its cap')}
          {row('min_shares', 'the venue’s smallest order')}
          {row('params', 'your parameters, by name')}
        </tbody></table>
      </section>
      <section>
        <h4 className="text-gray-300 mb-1">pos (in exit)</h4>
        <table className="w-full"><tbody>
          {row('side', '"yes" or "no"')}
          {row('shares, avg_entry, entry_fee', 'decimals')}
          {row('held_secs', 'integer')}
          {row('take_profit', 'decimal, or ()')}
          {row('market_name', 'string')}
        </tbody></table>
      </section>
      <section>
        <h4 className="text-gray-300 mb-1">Helpers</h4>
        <table className="w-full"><tbody>
          {row('taker_fee(price)', 'the venue’s taker fee per contract at that price')}
          {row('round_tick(price)', 'round to the one-cent tick')}
          {row('norm_cdf(z)', 'standard normal CDF, for option-style fair values')}
          {row('clamp(x, lo, hi), min(a, b), max(a, b)', 'on any numbers')}
          {row('to_decimal(n)', 'an integer as a decimal')}
          {row('x.sqrt(), x.exp(), x.ln(), x.abs(), x.round(dp)', 'decimal maths; sqrt, exp and ln can return up to 28 places, and round(dp) takes halves to the even digit (2.25 → 2.2)')}
        </tbody></table>
      </section>
      <section>
        <h4 className="text-gray-300 mb-1">What the engine enforces, whatever the script returns</h4>
        <ul className="list-disc pl-5 text-gray-400 space-y-1">
          <li>Paper unless the viper is on live; real money only while the instance is live too.</li>
          <li>The cap, across every squadron, fees included. An order that does not fit is refused, not resized.</li>
          <li>The daily drawdown limit, for every entry made while the viper is on live (simulated or real); paper entries are exempt.</li>
          <li>Prices inside (0, 1) on the tick; at least the venue’s minimum size (whole contracts on Kalshi and Polymarket US); one position per market.</li>
          <li>No file, network or clock access; each call is stopped after about half a millisecond of work.</li>
        </ul>
      </section>
    </div>
  );
}

// ── The page ──────────────────────────────────────────────────────────────────

export default function BuildViperPage() {
  // Every 5 s: the Where it runs panel is the operator's view of a save taking
  // effect, and a 15 s lag read as "nothing happened".
  const { data: list, mutate: refreshList, error: listError } = useSWR('scripts', listScripts, { refreshInterval: 5_000 });
  const { data: config, mutate: refreshConfig, error: configError } = useSWR('config', getConfig);
  const { data: templates } = useSWR('script-templates', getScriptTemplates);
  const [confirm, confirmDialog] = useConfirm();

  const [selected, setSelected] = useState<string | null>(null);
  const [draft, setDraft] = useState<Draft | null>(null);
  const [saved, setSaved] = useState<{ manifest: string; source: string } | null>(null);
  const [tab, setTab] = useState<Tab>('script');
  const [problems, setProblems] = useState<string[] | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  // Compared as the engine would receive it, so reordering nothing or typing
  // and deleting a character does not leave the draft marked unsaved.
  const draftKey = draft ? JSON.stringify(normalized(draft).manifest) : '';
  const dirty = !!draft && (!saved || draftKey !== saved.manifest || draft.source !== saved.source);

  // Leaving the page (reload, close, another site) with unsaved work asks first.
  useEffect(() => {
    if (!dirty) return;
    const warn = (e: BeforeUnloadEvent) => { e.preventDefault(); e.returnValue = ''; };
    window.addEventListener('beforeunload', warn);
    return () => window.removeEventListener('beforeunload', warn);
  }, [dirty]);

  // Only the latest open() may land: a slow response for one viper must not
  // fill the editor after the operator has moved to another.
  const openSeq = useRef(0);
  const open = async (name: string) => {
    if (dirty && !(await confirm({ title: 'Discard unsaved changes?', tone: 'danger', confirmLabel: 'Discard' }))) return;
    const seq = ++openSeq.current;
    setNotice(null);
    let d;
    try {
      d = await getScript(name);
    } catch (e) {
      if (seq === openSeq.current) setProblems([`Could not open ${name}: ${problemsOf(e).join('; ')}`]);
      return;
    }
    if (seq !== openSeq.current) return;
    setTab('script');
    setSelected(name);
    if (d.manifest && typeof d.source === 'string') {
      const next = draftOf(d.manifest, d.source, false);
      setDraft(next);
      setSaved({ manifest: JSON.stringify(normalized(next).manifest), source: d.source });
      setProblems(null);
    } else {
      // A script on disk that did not load: show what is there so it can be fixed.
      let manifest: ScriptManifest = { name, display_name: name, classes: ['crypto'], venues: [], max_exposure_usdc: 5, params: {} };
      try { if (d.manifest_raw) manifest = { ...manifest, ...JSON.parse(d.manifest_raw) }; } catch { /* keep the stub */ }
      setDraft(draftOf(manifest, d.source ?? '', false));
      setSaved(null);
      setProblems(d.broken ? [`Did not load: ${d.broken}`] : null);
    }
  };

  const startFrom = async (t: ScriptTemplate) => {
    if (dirty && !(await confirm({ title: 'Discard unsaved changes?', tone: 'danger', confirmLabel: 'Discard' }))) return;
    ++openSeq.current;
    setSelected(null); setProblems(null); setNotice(null); setTab('script');
    setDraft(draftOf(structuredClone(t.manifest), t.source, true));
    setSaved(null);
  };

  const check = async () => {
    if (!draft) return;
    const { manifest, problems: local } = normalized(draft);
    if (local.length) { setProblems(local); setNotice(null); return; }
    setBusy(true); setNotice(null);
    try {
      const p = await validateScript(manifest, draft.source);
      setProblems(p.length ? p : []);
      if (!p.length) setNotice('No problems found.');
    } catch (e) {
      setProblems(problemsOf(e));
    } finally {
      setBusy(false);
    }
  };

  const save = async () => {
    if (!draft) return;
    const { manifest: m, problems: local } = normalized(draft);
    if (local.length) { setProblems(local); setNotice(null); return; }
    // A new viper may not take an existing viper's name: that would replace
    // its code, and it might be live. The engine refuses it too.
    if (draft.isNew && (list?.scripts.some((x) => x.name === m.name) || list?.broken.some((x) => x.name === m.name))) {
      setProblems([`A viper named '${m.name}' already exists. Pick another name, or open it to edit it.`]);
      return;
    }
    setBusy(true); setNotice(null);
    try {
      const r = await saveScript(m, draft.source, draft.isNew);
      setProblems(null);
      setNotice(draft.isNew
        ? `Saved as version ${r.version}, on paper. It joins its squadrons on their next tick: see Where it runs below.`
        : `Saved as version ${r.version}. It runs from each squadron’s next tick.`);
      ++openSeq.current;
      setSelected(m.name);
      setDraft({ ...draft, manifest: m, isNew: false });
      setSaved({ manifest: JSON.stringify(m), source: draft.source });
      await refreshList();
    } catch (e) {
      setProblems(problemsOf(e));
    } finally {
      setBusy(false);
    }
  };

  const remove = async () => {
    if (!selected) return;
    const ok = await confirm({
      title: `Delete ${selected}?`, tone: 'danger', confirmLabel: 'Delete',
      body: 'The script and its manifest are removed. Its open paper positions are closed unscored. A viper that holds a real position cannot be deleted: switch it off and wait until those positions close.',
    });
    if (!ok) return;
    setBusy(true);
    try {
      await deleteScript(selected);
      setSelected(null); setDraft(null); setSaved(null); setProblems(null);
      setNotice(`Deleted ${selected}.`);
      await refreshList();
    } catch (e) {
      setProblems(problemsOf(e));
    } finally {
      setBusy(false);
    }
  };

  const current = useMemo(() => list?.scripts.find((s) => s.name === selected), [list, selected]);
  const cfg: CustomViperConfig = useMemo(
    () => (selected && config?.custom_vipers?.[selected]) || EMPTY_CFG,
    [selected, config],
  );
  // Until the config has loaded the page does not know whether the instance is
  // live, so it assumes it is: the Live confirm must never say "simulated" by
  // default on a box that trades real money. Settings waits for the config.
  const configLoaded = config !== undefined;
  const instanceGhost = config?.ghost_mode ?? false;

  const status = (name: string) => {
    if (!configLoaded) return { label: '…', cls: 'text-gray-500 border-gray-600/40' };
    const c = config?.custom_vipers?.[name] ?? {};
    if (c.enabled === false) return { label: 'off', cls: 'text-gray-500 border-gray-600/40' };
    if (c.live) return instanceGhost
      ? { label: 'live · sim', cls: 'text-amber-300 border-amber-500/40' }
      : { label: 'live', cls: 'text-red-300 border-red-500/40' };
    return { label: 'paper', cls: 'text-teal-300 border-teal-500/40' };
  };

  const tabs: { id: Tab; label: string; needsSaved?: boolean }[] = [
    { id: 'script', label: 'Script' },
    { id: 'settings', label: configLoaded ? 'Settings' : 'Settings (loading)', needsSaved: true },
    { id: 'test', label: 'Test run' },
    { id: 'paper', label: 'Paper ledger', needsSaved: true },
    { id: 'reference', label: 'Reference' },
  ];

  return (
    <div className="space-y-6">
      {confirmDialog}
      <div>
        <h2 className="text-sm font-mono uppercase tracking-wide text-teal-300">🛠️ Build a Viper</h2>
        <p className="text-[11px] font-mono text-gray-500 mt-1 max-w-3xl">
          Write your own strategy as a script and save it: it runs from the next tick, with no rebuild. A new viper
          paper-trades in its own ledger; it reaches the venue only when you put it on live, and trades real money only while
          the instance is live too. The engine enforces its cap, the daily drawdown limit and every price and size check,
          whatever the script returns.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-4">
        {/* ── Viper list ── */}
        <aside className="space-y-3">
          <div className="card p-3 border border-[#1e1e32] space-y-2">
            <p className={labelCls}>Your vipers</p>
            {listError && <p className="text-[11px] font-mono text-red-300">Could not load: {String(listError.message ?? listError)}</p>}
            {configError && (
              <p className="text-[11px] font-mono text-red-300">
                Could not load the config, so the live/paper state is unknown and Settings is unavailable: {String(configError.message ?? configError)}
              </p>
            )}
            {list && list.scripts.length === 0 && list.broken.length === 0 && (
              <p className="text-[11px] font-mono text-gray-500">None yet. Start from a template below.</p>
            )}
            {list?.scripts.map((s) => {
              const st = status(s.name);
              return (
                <button key={s.name} onClick={() => open(s.name)}
                        className={`w-full text-left rounded border px-2 py-1.5 transition-colors ${selected === s.name ? 'border-teal-500/50 bg-teal-500/10' : 'border-[#1e1e32] hover:bg-white/[0.02]'}`}>
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-xs font-mono text-gray-200 truncate">{s.manifest.display_name}</span>
                    <span className={`text-[9px] font-mono uppercase border rounded px-1 ${st.cls}`}>{st.label}</span>
                  </div>
                  <div className="text-[10px] font-mono text-gray-500">
                    {s.name} · paper {s.paper.closed} closed, {money(s.paper.pnl)}
                  </div>
                </button>
              );
            })}
            {list?.broken.map((b) => (
              <button key={b.name} onClick={() => open(b.name)}
                      className="w-full text-left rounded border border-red-500/30 bg-red-500/5 px-2 py-1.5">
                <div className="text-xs font-mono text-red-300">{b.name} · did not load</div>
                <div className="text-[10px] font-mono text-red-300/70 truncate" title={b.error}>{b.error}</div>
              </button>
            ))}
          </div>
          <div className="card p-3 border border-[#1e1e32] space-y-2">
            <p className={labelCls}>Templates</p>
            <p className="text-[10px] font-mono text-gray-500">Pick one to start a new viper.</p>
            {templates?.map((t) => (
              <button key={t.id} onClick={() => startFrom(t)} className="w-full text-left rounded border border-[#1e1e32] px-2 py-1.5 hover:bg-white/[0.02]">
                <div className="text-xs font-mono text-gray-200">{t.title}</div>
                <div className="text-[10px] font-mono text-gray-500">{t.summary}</div>
              </button>
            ))}
            <p className="text-[10px] font-mono text-gray-600">
              The built-in vipers are compiled into the engine and cannot be opened as scripts.
            </p>
          </div>
        </aside>

        {/* ── Editor ── */}
        <section className="lg:col-span-3 space-y-3">
          {!draft ? (
            <div className="card p-6 border border-[#1e1e32] text-xs font-mono text-gray-500 space-y-3">
              {notice && <div className="rounded border border-teal-500/30 bg-teal-500/5 px-3 py-2 text-teal-200">{notice}</div>}
              {problems && problems.length > 0 && (
                <div className="rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-red-300 whitespace-pre-wrap">{problems.join('\n')}</div>
              )}
              <div>Pick a viper, or start one from a template.</div>
            </div>
          ) : (
            <div className="card p-4 border border-[#1e1e32] space-y-4">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <div className="text-sm font-mono text-gray-100">
                    {draft.manifest.display_name || draft.manifest.name}
                    {dirty && <span className="ml-2 text-amber-300 text-[10px]">● unsaved</span>}
                  </div>
                  <div className="text-[10px] font-mono text-gray-500">
                    {draft.isNew ? 'new, not saved' : `Script:${draft.manifest.name}${current ? ` · version ${current.version}` : ''}`}
                  </div>
                </div>
                <div className="flex gap-1" role="tablist">
                  {tabs.map((t) => (
                    <button key={t.id} role="tab" aria-selected={tab === t.id}
                            disabled={(t.needsSaved && draft.isNew) || (t.id === 'settings' && !configLoaded)}
                            onClick={() => setTab(t.id)}
                            className={`${btn} ${tab === t.id ? 'border-teal-500/50 bg-teal-500/10 text-teal-200' : 'border-[#1e1e32] text-gray-400 hover:bg-white/[0.03]'}`}>
                      {t.label}
                    </button>
                  ))}
                </div>
              </div>

              {notice && <div className="rounded border border-teal-500/30 bg-teal-500/5 px-3 py-2 text-xs font-mono text-teal-200">{notice}</div>}
              {problems && problems.length > 0 && (
                <div className="rounded border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs font-mono text-red-300 space-y-1">
                  {problems.map((p, i) => <div key={i} className="whitespace-pre-wrap">{p}</div>)}
                </div>
              )}

              {tab === 'script' && (
                <div className="space-y-4">
                  <ManifestForm draft={draft} setDraft={setDraft} disabled={busy} />
                  <CodeEditor value={draft.source} onChange={(source) => setDraft({ ...draft, source })} disabled={busy} />
                  <div className="flex flex-wrap gap-2">
                    <button className={btnPlain} disabled={busy} onClick={check}>Check</button>
                    <button className={btnPrimary} disabled={busy || !dirty} onClick={save}>{draft.isNew ? 'Save (starts on paper)' : 'Save'}</button>
                    {saved && (
                      <button className={btnPlain} disabled={busy || !dirty}
                              onClick={() => setDraft(draftOf(JSON.parse(saved.manifest), saved.source, false))}>
                        Revert
                      </button>
                    )}
                    {!draft.isNew && <button className={btnDanger} disabled={busy} onClick={remove}>Delete</button>}
                  </div>
                  <p className="text-[10px] font-mono text-gray-600">
                    Saving checks the manifest and compiles the script first; if anything is wrong nothing is written and the running
                    version keeps running. A saved change applies from the next tick, including to positions the viper already holds.
                  </p>
                  {!draft.isNew && current && list && (
                    <WhereItRuns item={current} squadrons={list.squadrons ?? []} enabled={cfg.enabled ?? true} venue={list.venue} />
                  )}
                </div>
              )}

              {tab === 'settings' && selected && current && configLoaded && (
                <SettingsPanel name={selected} manifest={current.manifest} cfg={cfg} instanceGhost={instanceGhost}
                               paperCount={current.paper.scored} paperPnl={current.paper.pnl}
                               onSaved={() => { refreshConfig(); }} />
              )}

              {tab === 'test' && (
                <TestRunPanel name={draft.manifest.name} squadrons={list?.dry_run_squadrons ?? []}
                              unsavedSource={dirty || draft.isNew ? draft.source : null} isNew={draft.isNew}
                              manifest={normalized(draft).manifest} manifestProblems={normalized(draft).problems} />
              )}

              {tab === 'paper' && selected && <PaperPanel name={selected} />}

              {tab === 'reference' && <ReferencePanel />}
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
