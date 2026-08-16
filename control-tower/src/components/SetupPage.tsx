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
 * Setup page — venue credentials, integrations, and admin password management.
 *
 * Flow:
 *  - GET /api/setup/status → admin_set?
 *      - false → first-boot wizard: banner + open forms + "create admin password"
 *      - true  → login gate (password → Bearer token in localStorage)
 *  - Credential fields are write-only: server returns set/…last4 hints, never values.
 *  - Test buttons validate candidate creds without persisting.
 *  - Save persists to the engine's data/secrets.env; Restart applies them.
 */

import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  SetupStatus, CredentialInfo, TestResult, AutonomyStatus,
  RaptorSource, RaptorTier, getRaptorSources,
  getSetupStatus, getCredentials, putCredentials, testConnection,
  login, setAdminPassword, restartEngine,
  getAutonomy, putAutonomy,
  exportBundle, importBundle,
  getProfiles, applyProfile, ConfigProfile,
  getAdminToken, clearAdminToken, SetupApiError,
} from '@/lib/setupApi';
import { useConfirm } from '@/components/ConfirmDialog';
import useSWR from 'swr';
import { getConfig, patchConfig, getConfigSchema } from '@/lib/api';
import type { DynamicConfig, ConfigFieldSchema } from '@/lib/types';

// Which /api/setup/test kind exercises a given credential scope/group.
const TEST_KINDS: Record<string, { kind: string; label: string; keys: string[] }> = {
  intl_wallet: { kind: 'intl_wallet', label: 'Test wallet + CLOB auth', keys: ['POLYMARKET_PRIVATE_KEY'] },
  polygon_rpc: { kind: 'polygon_rpc', label: 'Test RPC', keys: ['POLYGON_RPC_URL'] },
  us_keys:     { kind: 'us_keys',     label: 'Test API keys', keys: ['POLYMARKET_US_KEY_ID', 'POLYMARKET_US_SECRET_KEY'] },
  alpaca:      { kind: 'alpaca',      label: 'Test Alpaca', keys: ['ALPACA_API_KEY_ID', 'ALPACA_API_SECRET_KEY'] },
  odds:        { kind: 'odds',        label: 'Test key',    keys: ['ODDS_API_KEY'] },
  telegram:    { kind: 'telegram',    label: 'Test Telegram', keys: ['TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID'] },
  llm:         { kind: 'llm',         label: 'Test LLM', keys: ['LLM_PROVIDER', 'OLLAMA_URL', 'OLLAMA_MODEL', 'LLM_API_BASE', 'LLM_API_KEY', 'LLM_MODEL'] },
};

// Group layout: section title → credential keys + test kind.
function groupsForVenue(venue: 'intl' | 'us') {
  const groups: { title: string; blurb: string; keys: string[]; test?: keyof typeof TEST_KINDS }[] = [];
  if (venue === 'intl') {
    groups.push(
      { title: 'Polymarket Wallet', blurb: 'Self-custody EOA key — Safe address and CLOB auth are derived from it.', keys: ['POLYMARKET_PRIVATE_KEY'], test: 'intl_wallet' },
      { title: 'Polygon RPC', blurb: 'JSON-RPC endpoint for on-chain settlement + balance checks.', keys: ['POLYGON_RPC_URL'], test: 'polygon_rpc' },
    );
  } else {
    groups.push(
      { title: 'Polymarket US API Keys', blurb: 'Custodial venue key ID + secret from your Polymarket US account.', keys: ['POLYMARKET_US_KEY_ID', 'POLYMARKET_US_SECRET_KEY'], test: 'us_keys' },
    );
  }
  // Raptor signal keys (Alpaca, The Odds API, …) deliberately do NOT appear
  // here — they live in the Raptor Signal Sources panel below, which is driven
  // by GET /api/setup/raptors so a new Raptor needs no change to this file.
  groups.push(
    { title: 'Telegram Alerts', blurb: 'Bot token + chat ID for trade notifications (optional).', keys: ['TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID'], test: 'telegram' },
    { title: 'LLM Advisor', blurb: 'Provider: ollama (local/remote, no key — set Ollama URL + model) or a hosted API: openai-compatible / anthropic (set API base, key, model). Applies on restart.', keys: ['LLM_PROVIDER', 'OLLAMA_URL', 'OLLAMA_MODEL', 'LLM_API_BASE', 'LLM_API_KEY', 'LLM_MODEL'], test: 'llm' },
  );
  return groups;
}

const inputCls =
  'w-full bg-[#0d0d16] border border-[#1e1e32] rounded-lg px-3 py-2 text-sm font-mono text-gray-200 ' +
  'placeholder-gray-600 focus:outline-none focus:border-indigo-500/60';

const btnCls = (variant: 'primary' | 'ghost' | 'danger' = 'ghost') =>
  [
    'text-xs font-mono px-3 py-1.5 rounded-lg border transition-colors disabled:opacity-40 disabled:cursor-not-allowed',
    variant === 'primary' && 'bg-indigo-500/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30',
    variant === 'danger'  && 'bg-rose-500/10 border-rose-500/30 text-rose-300 hover:bg-rose-500/20',
    variant === 'ghost'   && 'bg-[#13131f] border-[#1e1e32] text-gray-400 hover:border-gray-600 hover:text-gray-200',
  ].filter(Boolean).join(' ');

// ── Login / first-boot password card ─────────────────────────────────────────

function PasswordCard({
  mode, onDone,
}: {
  mode: 'login' | 'create';
  onDone: () => void;
}) {
  const [password, setPassword] = useState('');
  const [confirm, setConfirm] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    if (mode === 'create') {
      if (password.length < 8) { setError('Password must be at least 8 characters.'); return; }
      if (password !== confirm) { setError('Passwords do not match.'); return; }
    }
    setBusy(true);
    try {
      if (mode === 'create') await setAdminPassword(password);
      else await login(password);
      onDone();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Request failed');
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="max-w-md mx-auto bg-[#13131f] border border-[#1e1e32] rounded-xl p-6 space-y-4">
      <div>
        <h2 className="text-sm font-mono text-gray-200">
          {mode === 'create' ? '🛡️ Create admin password' : '🔐 Admin login'}
        </h2>
        <p className="text-xs text-gray-500 mt-1">
          {mode === 'create'
            ? 'First-boot setup: this password protects credential management on this DRADIS instance.'
            : 'Enter the admin password to manage credentials.'}
        </p>
      </div>
      <form onSubmit={submit} className="space-y-3">
        <input
          type="password"
          className={inputCls}
          placeholder="Password"
          value={password}
          onChange={e => setPassword(e.target.value)}
          autoFocus
        />
        {mode === 'create' && (
          <input
            type="password"
            className={inputCls}
            placeholder="Confirm password"
            value={confirm}
            onChange={e => setConfirm(e.target.value)}
          />
        )}
        {error && <div className="text-xs text-rose-400 font-mono">{error}</div>}
        <button type="submit" disabled={busy || !password} className={btnCls('primary') + ' w-full py-2'}>
          {busy ? '…' : mode === 'create' ? 'Set password & continue' : 'Log in'}
        </button>
      </form>
    </div>
  );
}

// ── Credential group card ─────────────────────────────────────────────────────

function CredentialGroup({
  title, blurb, creds, drafts, onDraft, testKind, onTested,
}: {
  title: string;
  blurb: string;
  creds: CredentialInfo[];
  drafts: Record<string, string>;
  onDraft: (key: string, value: string) => void;
  testKind?: keyof typeof TEST_KINDS;
  onTested?: (ok: boolean) => void;
}) {
  const [testing, setTesting] = useState(false);
  const [result, setResult] = useState<TestResult | null>(null);

  const runTest = async () => {
    if (!testKind) return;
    setTesting(true);
    setResult(null);
    try {
      // Send drafts for this group's keys so unsaved values are validated.
      const candidate: Record<string, string> = {};
      for (const k of TEST_KINDS[testKind].keys) {
        if (drafts[k]) candidate[k] = drafts[k];
      }
      const r = await testConnection(TEST_KINDS[testKind].kind, candidate);
      setResult(r);
      onTested?.(r.ok);
    } catch (err) {
      setResult({ ok: false, ms: 0, error: err instanceof Error ? err.message : 'test failed' });
    } finally {
      setTesting(false);
    }
  };

  return (
    <div className="bg-[#13131f] border border-[#1e1e32] rounded-xl p-5 space-y-3">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-mono text-gray-200">{title}</h3>
          <p className="text-xs text-gray-500 mt-0.5">{blurb}</p>
        </div>
        {testKind && (
          <button onClick={runTest} disabled={testing} className={btnCls('ghost') + ' shrink-0'}>
            {testing ? 'Testing…' : TEST_KINDS[testKind].label}
          </button>
        )}
      </div>

      <div className="space-y-2">
        {creds.map(c => (
          <div key={c.key}>
            <div className="flex items-center justify-between mb-1">
              <label className="text-xs font-mono text-gray-400">{c.label}</label>
              <span className={`text-[10px] font-mono ${c.set ? 'text-emerald-400' : 'text-gray-600'}`}>
                {c.set ? `set ${c.hint} · ${c.source}` : 'not set'}
              </span>
            </div>
            <input
              type={/URL|CHAT_ID|PROVIDER|MODEL|BASE/.test(c.key) ? 'text' : 'password'}
              className={inputCls}
              placeholder={c.set ? '•••••••• (leave blank to keep current)' : 'Enter value'}
              value={drafts[c.key] ?? ''}
              onChange={e => onDraft(c.key, e.target.value)}
              autoComplete="off"
              spellCheck={false}
            />
          </div>
        ))}
      </div>

      {result && (
        <div className={`text-xs font-mono rounded-lg px-3 py-2 border ${
          result.ok
            ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300'
            : 'bg-rose-500/10 border-rose-500/30 text-rose-300'
        }`}>
          {result.ok
            ? `✓ Connection OK (${result.ms}ms)${result.details ? ' — ' + Object.entries(result.details).map(([k, v]) => `${k}: ${v}`).join(', ') : ''}`
            : `✗ ${result.error}`}
        </div>
      )}
    </div>
  );
}

// ── Raptor signal sources panel ───────────────────────────────────────────────

// Tier badges describe how much the SIGNAL matters, not whether it is currently
// configured — a "required" Raptor on a public feed needs no key at all.
const TIER_BADGE: Record<RaptorTier, string> = {
  required:    'bg-rose-500/10 border-rose-500/30 text-rose-300',
  recommended: 'bg-amber-500/10 border-amber-500/30 text-amber-300',
  optional:    'bg-sky-500/10 border-sky-500/30 text-sky-300',
};

const PERIOD_SECS: Record<'day' | 'month', number> = { day: 86_400, month: 2_592_000 };

/**
 * Poll cadence control. Separate from the credential inputs because it saves to
 * a different place: keys go to the secrets file and need a restart, whereas the
 * cadence is a live DynamicConfig knob that the raptor loops pick up on their
 * next cycle.
 *
 * The projected request count is the point of the control. An operator raising
 * the rate is spending a third-party allowance, and the consequence should be
 * visible before saving rather than discovered as a 429 hours later.
 */
function PollCadence({ raptor, schema }: { raptor: RaptorSource; schema: ConfigFieldSchema[] }) {
  const field = raptor.poll_field!;
  const { data: config, mutate } = useSWR('dynamic-config', getConfig, { revalidateOnFocus: false });
  const spec = schema.find(f => f.key === field);
  const [draft, setDraft] = useState<string>('');
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const saved = config ? Number((config as unknown as Record<string, unknown>)[field] ?? 0) : null;
  const shown = draft !== '' ? Number(draft) : saved;
  const min = spec?.min ?? 1;
  const max = spec?.max ?? 86_400;

  // Projected spend at the *displayed* value, so the warning tracks what you
  // are about to save rather than what is already saved.
  const projection = (() => {
    if (!raptor.free_quota || !shown || shown <= 0) return null;
    const { requests, period } = raptor.free_quota;
    const used = Math.round(PERIOD_SECS[period] / shown);
    return { used, requests, period, over: used > requests };
  })();

  const save = async () => {
    const n = Number(draft);
    if (!Number.isFinite(n)) return;
    const clamped = Math.min(max, Math.max(min, n));
    setSaving(true);
    setErr(null);
    try {
      await patchConfig({ [field]: clamped } as unknown as Partial<DynamicConfig>);
      await mutate();
      setDraft('');
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'save failed');
    } finally {
      setSaving(false);
    }
  };

  const dirty = draft !== '' && Number(draft) !== saved;

  return (
    <div className="border-t border-[#1e1e32] pt-3 space-y-1.5">
      <div className="flex items-center justify-between gap-2">
        <label className="text-xs font-mono text-gray-400">Poll interval</label>
        <div className="flex items-center gap-1.5">
          <input
            type="number"
            min={min}
            max={max}
            step={spec?.step ?? 1}
            className={inputCls + ' w-24 text-right py-1'}
            value={draft !== '' ? draft : (saved ?? '')}
            onChange={e => setDraft(e.target.value)}
            disabled={!config}
          />
          <span className="text-[11px] font-mono text-gray-600">s</span>
          <button onClick={save} disabled={!dirty || saving} className={btnCls('primary')}>
            {saving ? '…' : 'Save'}
          </button>
        </div>
      </div>

      {projection && (
        <p className={`text-[10px] font-mono ${projection.over ? 'text-amber-400' : 'text-gray-600'}`}>
          ≈ {projection.used.toLocaleString()} requests/{projection.period}
          {projection.over
            ? ` — over the free tier's ${projection.requests.toLocaleString()}/${projection.period}; needs a paid plan`
            : ` — within the free tier's ${projection.requests.toLocaleString()}/${projection.period}`}
        </p>
      )}
      <p className="text-[10px] font-mono text-gray-700">
        Applies on the raptor&apos;s next cycle — no restart, unlike the key above.
      </p>
      {err && <p className="text-[10px] font-mono text-rose-400">{err}</p>}
    </div>
  );
}


/**
 * Free-text feed selector (sport key, region list, tour filter). Kept visually
 * distinct from the credential inputs because it is neither secret nor
 * validated: the value is handed to the provider verbatim, and DRADIS has no
 * way to tell a valid identifier from a typo. The schema description carries
 * the specific warning, which is the only guard rail these fields have.
 */
function FeedSelector({ fieldKey, schema }: { fieldKey: string; schema: ConfigFieldSchema[] }) {
  const { data: config, mutate } = useSWR('dynamic-config', getConfig, { revalidateOnFocus: false });
  const spec = schema.find(f => f.key === fieldKey);
  const [draft, setDraft] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const saved = config ? String((config as unknown as Record<string, unknown>)[fieldKey] ?? '') : null;
  // null draft = untouched; '' is a MEANINGFUL value here (blank tour = all tours).
  const shown = draft ?? saved ?? '';
  const dirty = draft !== null && draft !== saved;

  const save = async () => {
    if (draft === null) return;
    setSaving(true);
    setErr(null);
    try {
      await patchConfig({ [fieldKey]: draft.trim() } as unknown as Partial<DynamicConfig>);
      await mutate();
      setDraft(null);
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'save failed');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between gap-2">
        <label className="text-xs font-mono text-gray-400">{spec?.label ?? fieldKey}</label>
        <div className="flex items-center gap-1.5">
          <input
            type="text"
            className={inputCls + ' w-44 py-1'}
            value={shown}
            placeholder="(blank = no filter)"
            onChange={e => setDraft(e.target.value)}
            disabled={!config}
            autoComplete="off"
            spellCheck={false}
          />
          <button onClick={save} disabled={!dirty || saving} className={btnCls('primary')}>
            {saving ? '…' : 'Save'}
          </button>
        </div>
      </div>
      {spec?.description && (
        <p className="text-[10px] font-mono text-gray-600 leading-snug">{spec.description}</p>
      )}
      {err && <p className="text-[10px] font-mono text-rose-400">{err}</p>}
    </div>
  );
}

function RaptorCard({
  raptor, creds, drafts, onDraft, schema,
}: {
  raptor: RaptorSource;
  creds: CredentialInfo[];
  drafts: Record<string, string>;
  onDraft: (key: string, value: string) => void;
  schema: ConfigFieldSchema[];
}) {
  const [testing, setTesting] = useState(false);
  const [result, setResult] = useState<TestResult | null>(null);

  const fields = raptor.keys
    .map(k => creds.find(c => c.key === k))
    .filter((c): c is CredentialInfo => !!c);
  // A Raptor is live when every key it needs is set. Keyless Raptors are always
  // live, which is exactly why they render without inputs.
  const configured = fields.length === 0 || fields.every(f => f.set);

  const runTest = async () => {
    if (!raptor.test_kind) return;
    setTesting(true);
    setResult(null);
    try {
      // Send unsaved drafts so a key can be validated before it is persisted.
      const candidate: Record<string, string> = {};
      for (const k of raptor.keys) {
        if (drafts[k]) candidate[k] = drafts[k];
      }
      setResult(await testConnection(raptor.test_kind, candidate));
    } catch (err) {
      setResult({ ok: false, ms: 0, error: err instanceof Error ? err.message : 'test failed' });
    } finally {
      setTesting(false);
    }
  };

  return (
    <div className="bg-[#13131f] border border-[#1e1e32] rounded-xl p-5 space-y-3">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <h3 className="text-sm font-mono text-gray-200">{raptor.name}</h3>
            <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded border ${TIER_BADGE[raptor.tier]}`}>
              {raptor.tier}
            </span>
            <span className={`text-[10px] font-mono ${configured ? 'text-emerald-400' : 'text-gray-600'}`}>
              {configured ? '● live' : '○ idle'}
            </span>
          </div>
          <p className="text-[11px] font-mono text-gray-600 mt-0.5">{raptor.source}</p>
          <p className="text-xs text-gray-500 mt-1">{raptor.blurb}</p>
        </div>
        {raptor.test_kind && (
          <button onClick={runTest} disabled={testing} className={btnCls('ghost') + ' shrink-0'}>
            {testing ? 'Testing…' : 'Test key'}
          </button>
        )}
      </div>

      {fields.length === 0 ? (
        <p className="text-[11px] font-mono text-gray-600 border-t border-[#1e1e32] pt-3">
          No credentials required — public endpoint, always on.
        </p>
      ) : (
        <div className="space-y-2">
          {/* Where to generate the key. Shown above the inputs because an
              operator who lacks a key needs the link before the field. */}
          {raptor.signup_url && (
            <a
              href={raptor.signup_url}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 text-[11px] font-mono text-indigo-400 hover:text-indigo-300 hover:underline"
            >
              Get a key at {raptor.signup_url.replace(/^https:\/\//, '')} ↗
            </a>
          )}
          {fields.map(c => (
            <div key={c.key}>
              <div className="flex items-center justify-between mb-1">
                <label className="text-xs font-mono text-gray-400">{c.label}</label>
                <span className={`text-[10px] font-mono ${c.set ? 'text-emerald-400' : 'text-gray-600'}`}>
                  {c.set ? `set ${c.hint} · ${c.source}` : 'not set'}
                </span>
              </div>
              <input
                type="password"
                className={inputCls}
                placeholder={c.set ? '•••••••• (leave blank to keep current)' : 'Enter value'}
                value={drafts[c.key] ?? ''}
                onChange={e => onDraft(c.key, e.target.value)}
                autoComplete="off"
                spellCheck={false}
              />
            </div>
          ))}
        </div>
      )}

      {raptor.selector_fields.length > 0 && (
        <div className="border-t border-[#1e1e32] pt-3 space-y-3">
          {raptor.selector_fields.map(f => (
            <FeedSelector key={f} fieldKey={f} schema={schema} />
          ))}
        </div>
      )}

      {raptor.poll_field && <PollCadence raptor={raptor} schema={schema} />}

      {result && (
        <div className={`text-xs font-mono rounded-lg px-3 py-2 border ${
          result.ok
            ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300'
            : 'bg-rose-500/10 border-rose-500/30 text-rose-300'
        }`}>
          {result.ok
            ? `✓ Connection OK (${result.ms}ms)${result.details ? ' — ' + Object.entries(result.details).map(([k, v]) => `${k}: ${v}`).join(', ') : ''}`
            : `✗ ${result.error}`}
        </div>
      )}
    </div>
  );
}

/**
 * Raptor signal sources — the recon layer's credentials, kept separate from the
 * venue credentials above because they fail differently: a missing venue key
 * means DRADIS cannot trade, whereas a missing Raptor key only means that one
 * Raptor idles and publishes a neutral snapshot.
 *
 * The card list comes from GET /api/setup/raptors, so contributors adding a
 * Raptor register it in `RAPTOR_SOURCES` (src/api/setup.rs) and it appears here
 * with no change to this file.
 */
function RaptorPanel({
  creds, drafts, onDraft, onAuthError,
}: {
  creds: CredentialInfo[];
  drafts: Record<string, string>;
  onDraft: (key: string, value: string) => void;
  onAuthError: () => void;
}) {
  const [raptors, setRaptors] = useState<RaptorSource[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Shared SWR key, so N cards dedupe to one schema request.
  const { data: schema = [] } = useSWR('config-schema', getConfigSchema, { revalidateOnFocus: false });

  useEffect(() => {
    getRaptorSources()
      .then(r => setRaptors(r.raptors))
      .catch(err => {
        if (err instanceof SetupApiError && err.status === 401) onAuthError();
        else setError(err instanceof Error ? err.message : 'Failed to load Raptor sources');
      });
  }, [onAuthError]);

  return (
    <div className="space-y-3">
      <div>
        <h3 className="text-sm font-mono text-gray-200">📡 Raptor Signal Sources</h3>
        <p className="text-xs text-gray-500 mt-0.5">
          Optional recon feeds. A Raptor without its key idles and publishes a neutral
          snapshot — it never blocks trading. Badges rate the <span className="text-gray-400">signal</span>,
          not whether it is configured. Saved keys apply on engine restart.
        </p>
      </div>

      {error && (
        <div className="text-xs font-mono rounded-xl px-4 py-3 border bg-rose-500/10 border-rose-500/30 text-rose-300">
          {error}
        </div>
      )}

      {raptors ? (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {raptors.map(r => (
            <RaptorCard key={r.id} raptor={r} creds={creds} drafts={drafts} onDraft={onDraft} schema={schema} />
          ))}
        </div>
      ) : !error && (
        <div className="text-center text-gray-500 font-mono text-sm py-6">Loading Raptor sources…</div>
      )}
    </div>
  );
}

// ── AI autonomy panel ─────────────────────────────────────────────────────────

const TIER_DEFS: { tier: 1 | 2 | 3; name: string; blurb: string }[] = [
  { tier: 1, name: 'Recommend', blurb: 'AI proposes config changes; nothing applies until you press apply. Proposals expire after 30 min.' },
  { tier: 2, name: 'Limited', blurb: 'Safe changes auto-apply: schema-clamped, delta-capped, rate-limited, never money fields. The rest queue for approval.' },
  { tier: 3, name: 'Autonomous', blurb: 'AI applies its changes directly (still schema-clamped; mode flips excluded). Circuit breaker reverts + demotes on a P&L drawdown.' },
];

// A few headline numbers per profile so the picker communicates real differences.
const PROFILE_HIGHLIGHTS: { key: string; label: string; fmt?: (v: unknown) => string }[] = [
  { key: 'time_decay_stop_loss_pct', label: 'TD stop', fmt: v => `${(parseFloat(String(v)) * 100).toFixed(1)}%` },
  { key: 'momentum_stop_loss_pct',   label: 'Momentum stop', fmt: v => `${(parseFloat(String(v)) * 100).toFixed(1)}%` },
  { key: 'maker_min_spread',         label: 'Maker min spread', fmt: v => `${(parseFloat(String(v)) * 100).toFixed(0)}¢` },
  { key: 'arbitrage_max_exposure_usdc', label: 'Arb exposure', fmt: v => `$${v}` },
];

const PROFILE_ACCENTS: Record<string, string> = {
  conservative: 'border-emerald-800/60 hover:border-emerald-500/60',
  balanced:     'border-sky-800/60 hover:border-sky-500/60',
  aggressive:   'border-amber-800/60 hover:border-amber-500/60',
};

function ProfilesPanel({ onAuthError }: { onAuthError: () => void }) {
  const [profiles, setProfiles] = useState<Record<string, ConfigProfile> | null>(null);
  const [deployed, setDeployed] = useState<string[]>([]);
  const [busy, setBusy] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ kind: 'ok' | 'err'; text: string } | null>(null);
  const [confirm, confirmDialog] = useConfirm();

  useEffect(() => {
    getProfiles()
      .then(r => {
        setProfiles(r.profiles);
        setDeployed(r.deployed_squadrons ?? []);
      })
      .catch(err => {
        if (err instanceof SetupApiError && err.status === 401) onAuthError();
        else setNotice({ kind: 'err', text: err instanceof Error ? err.message : 'Failed to load profiles' });
      });
  }, [onAuthError]);

  const apply = async (name: string) => {
    const p = profiles?.[name];
    const fieldCount = p ? Object.keys(p.values).length : 0;
    // Name the blast radius: squadron rows are allowed to diverge per market, and
    // a full profile apply discards that divergence. Show it, don't describe it.
    const ok = await confirm({
      title: `Apply the ${p?.label ?? name} profile?`,
      tone: 'danger',
      confirmLabel: `Apply ${name}`,
      body: (
        <>
          <p>
            Replaces all <span className="text-gray-200 font-mono">{fieldCount}</span> runtime-tunable
            settings on the global config
            {deployed.length > 0 && <> and the {deployed.length} deployed squadron(s) below</>}.
          </p>
          {deployed.length > 0 ? (
            <>
              <ul className="font-mono text-[11px] text-gray-300 bg-[#0e0e18] border border-[#1e1e32] rounded-lg px-3 py-2 space-y-0.5">
                {deployed.map(s => <li key={s}>{s}</li>)}
              </ul>
              <p className="text-amber-400">
                Any per-squadron tuning on these will be replaced.
              </p>
            </>
          ) : (
            <p className="text-gray-500">
              No squadrons are currently deployed, so this seeds the next one deployed
              but changes nothing that is trading right now.
            </p>
          )}
          <p className="text-gray-500">
            Applies live (no restart) and is recorded in config history.
          </p>
        </>
      ),
    });
    if (!ok) return;
    setBusy(name);
    setNotice(null);
    try {
      const r = await applyProfile(name);
      const where = r.squadrons_applied.length
        ? ` across global + ${r.squadrons_applied.join(', ')}`
        : ' on the global config (no squadrons deployed)';
      if (r.squadron_errors.length) {
        setNotice({
          kind: 'err',
          text: `Applied '${r.profile}'${where}, but ${r.squadron_errors.length} squadron(s) failed and are STILL TRADING their old values: ${
            r.squadron_errors.map(e => `${e.squadron} (${e.error})`).join('; ')}`,
        });
      } else {
        setNotice({
          kind: 'ok',
          text: `Applied '${r.profile}' — ${r.fields_applied} settings live now${where} (no restart needed).`,
        });
        setDeployed(r.squadrons_applied.length ? r.squadrons_applied : deployed);
      }
    } catch (err) {
      if (err instanceof SetupApiError && err.status === 401) onAuthError();
      else setNotice({ kind: 'err', text: err instanceof Error ? err.message : 'Apply failed' });
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="bg-[#13131f] border border-[#1e1e32] rounded-xl p-5 space-y-4">
      <div>
        <h3 className="text-sm font-mono text-gray-200">🎛️ Risk Profile</h3>
        <p className="text-xs text-gray-500 mt-0.5">
          Replace the strategy config from a curated preset — the global config and
          every deployed squadron, so it reaches the running patrol loops. Applies
          live and is recorded in config history; individual settings can still be
          tuned afterwards in the Config view.
        </p>
        <p className="text-[11px] font-mono text-gray-600 mt-1.5">
          {deployed.length
            ? <>Will overwrite <span className="text-gray-400">{deployed.length}</span> deployed squadron(s): <span className="text-gray-400">{deployed.join(', ')}</span></>
            : 'No squadrons currently deployed — will seed the global config only.'}
        </p>
      </div>
      {notice && (
        <div className={`text-xs font-mono rounded-lg px-3 py-2 ${notice.kind === 'ok' ? 'bg-emerald-950/50 text-emerald-300' : 'bg-red-950/50 text-red-300'}`}>
          {notice.text}
        </div>
      )}
      {profiles ? (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {Object.entries(profiles).map(([name, p]) => (
            <div key={name} className={`border rounded-lg p-4 flex flex-col gap-2 bg-[#0e0e18] transition-colors ${PROFILE_ACCENTS[name] ?? 'border-[#1e1e32]'}`}>
              <div className="text-sm font-mono text-gray-100">
                {p.label}
                {name === 'conservative' && <span className="ml-2 text-[10px] text-emerald-400">RECOMMENDED START</span>}
              </div>
              <p className="text-xs text-gray-500 flex-1">{p.description}</p>
              <ul className="text-[11px] font-mono text-gray-400 space-y-0.5">
                {PROFILE_HIGHLIGHTS.map(h => (
                  h.key in p.values ? (
                    <li key={h.key} className="flex justify-between">
                      <span className="text-gray-600">{h.label}</span>
                      <span>{h.fmt ? h.fmt(p.values[h.key]) : String(p.values[h.key])}</span>
                    </li>
                  ) : null
                ))}
              </ul>
              <button
                className={btnCls('ghost') + ' mt-1'}
                disabled={busy !== null}
                onClick={() => apply(name)}
              >
                {busy === name ? 'Applying…' : `Apply ${p.label}`}
              </button>
            </div>
          ))}
        </div>
      ) : (
        <div className="text-xs text-gray-600 font-mono">Loading profiles…</div>
      )}
      {confirmDialog}
    </div>
  );
}

function AutonomyPanel({ onAuthError }: { onAuthError: () => void }) {
  const [state, setState] = useState<AutonomyStatus | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try { setState(await getAutonomy()); }
    catch (err) {
      if (err instanceof SetupApiError && err.status === 401) onAuthError();
      else setError(err instanceof Error ? err.message : 'Failed to load autonomy state');
    }
  }, [onAuthError]);

  useEffect(() => { load(); }, [load]);

  const update = async (body: { tier?: number; kill_switch?: boolean; reset_breaker?: boolean }) => {
    setBusy(true);
    setError(null);
    try { setState(await putAutonomy(body)); }
    catch (err) {
      if (err instanceof SetupApiError && err.status === 401) onAuthError();
      else setError(err instanceof Error ? err.message : 'Update failed');
    }
    finally { setBusy(false); }
  };

  return (
    <div className="bg-[#13131f] border border-[#1e1e32] rounded-xl p-5 space-y-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-mono text-gray-200">🤖 AI Autonomy</h3>
          <p className="text-xs text-gray-500 mt-0.5">
            How much control the LLM Advisor has over live config. Changes apply immediately — no restart.
            Every AI action is logged and TTL-bound; schema bounds are enforced at every tier.
          </p>
        </div>
        {state && (
          <button
            onClick={() => update({ kill_switch: !state.kill_switch })}
            disabled={busy}
            className={btnCls(state.kill_switch ? 'primary' : 'danger') + ' shrink-0'}
            title="Hard stop: no auto-applies at any tier; proposals still queue"
          >
            {state.kill_switch ? '▶ Resume autonomy' : '⛔ Kill switch'}
          </button>
        )}
      </div>

      {!state ? (
        <p className="text-xs text-gray-600 font-mono">Loading…</p>
      ) : (
        <>
          {state.kill_switch && (
            <div className="text-xs font-mono rounded-lg px-3 py-2 bg-rose-500/10 border border-rose-500/30 text-rose-300">
              ⛔ Kill switch engaged — all AI changes queue for human approval regardless of tier.
            </div>
          )}
          {state.breaker_demoted && (
            <div className="flex items-center justify-between gap-2 text-xs font-mono rounded-lg px-3 py-2 bg-amber-500/10 border border-amber-500/30 text-amber-300">
              <span>🧯 Circuit breaker tripped — autonomy demoted to Recommend after a P&L drawdown. Review reverted changes before resetting.</span>
              <button onClick={() => update({ reset_breaker: true })} disabled={busy} className={btnCls('ghost') + ' shrink-0'}>
                Reset breaker
              </button>
            </div>
          )}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
            {TIER_DEFS.map(t => {
              const active = state.tier === t.tier;
              return (
                <button
                  key={t.tier}
                  onClick={() => update({ tier: t.tier })}
                  disabled={busy || active}
                  className={[
                    'text-left rounded-lg border px-3 py-2.5 transition-colors disabled:cursor-default',
                    active
                      ? 'bg-indigo-500/15 border-indigo-500/50'
                      : 'bg-[#0d0d16] border-[#1e1e32] hover:border-gray-600',
                  ].join(' ')}
                >
                  <p className={`text-xs font-mono ${active ? 'text-indigo-300' : 'text-gray-300'}`}>
                    {t.tier} · {t.name}{active ? ' ✓' : ''}
                  </p>
                  <p className="text-[11px] text-gray-500 mt-1 leading-snug">{t.blurb}</p>
                </button>
              );
            })}
          </div>
          <p className="text-[11px] text-gray-600 font-mono">
            Guardrails: max {state.max_patches_per_hour} patch batch/h · ±{Math.round(state.max_delta_pct * 100)}% per field (tier 2)
            {' '}· breaker: ${state.breaker_drawdown_usdc.toFixed(0)} drawdown / {Math.round(state.breaker_window_secs / 3600)}h window.
            Applies in both LIVE and GHOST modes.
          </p>
        </>
      )}

      {error && (
        <div className="text-xs font-mono rounded-lg px-3 py-2 bg-rose-500/10 border border-rose-500/30 text-rose-300">
          ✗ {error}
        </div>
      )}
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function SetupPage() {
  const [status, setStatus] = useState<SetupStatus | null>(null);
  const [creds, setCreds] = useState<CredentialInfo[] | null>(null);
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [authed, setAuthed] = useState(false);
  const [saving, setSaving] = useState(false);
  const [restarting, setRestarting] = useState(false);
  const [notice, setNotice] = useState<{ kind: 'ok' | 'err' | 'info'; text: string } | null>(null);
  const [showChangePw, setShowChangePw] = useState(false);
  const [confirm, confirmDialog] = useConfirm();

  const loadStatus = useCallback(async () => {
    try {
      const s = await getSetupStatus();
      setStatus(s);
      // Operator disabled the setup gate (DRADIS_SETUP_AUTH=off) → no login.
      // Drop any stale token so it isn't sent along needlessly.
      // No admin password yet → first-boot wizard, routes are open.
      if (s.auth_disabled) { clearAdminToken(); setAuthed(true); }
      else if (!s.admin_set) setAuthed(true);
      else if (getAdminToken()) setAuthed(true);
    } catch {
      setNotice({ kind: 'err', text: 'Cannot reach the DRADIS engine.' });
    }
  }, []);

  const loadCreds = useCallback(async () => {
    try {
      const r = await getCredentials();
      setCreds(r.credentials);
    } catch (err) {
      if (err instanceof SetupApiError && err.status === 401) {
        clearAdminToken();
        setAuthed(false);
      } else {
        setNotice({ kind: 'err', text: err instanceof Error ? err.message : 'Failed to load credentials' });
      }
    }
  }, []);

  useEffect(() => { loadStatus(); }, [loadStatus]);
  useEffect(() => { if (authed) loadCreds(); }, [authed, loadCreds]);

  const dirty = useMemo(() => Object.values(drafts).some(v => v.trim() !== ''), [drafts]);

  const save = async () => {
    const payload: Record<string, string> = {};
    for (const [k, v] of Object.entries(drafts)) {
      if (v.trim() !== '') payload[k] = v.trim();
    }
    if (Object.keys(payload).length === 0) return;
    setSaving(true);
    setNotice(null);
    try {
      const r = await putCredentials(payload);
      setDrafts({});
      await loadCreds();
      await loadStatus();
      setNotice({
        kind: 'ok',
        text: `Saved ${r.changed.length} credential(s).` + (r.restart_required ? ' Restart the engine to apply.' : ''),
      });
    } catch (err) {
      if (err instanceof SetupApiError && err.status === 401) {
        clearAdminToken();
        setAuthed(false);
      } else {
        setNotice({ kind: 'err', text: err instanceof Error ? err.message : 'Save failed' });
      }
    } finally {
      setSaving(false);
    }
  };

  const restart = async () => {
    const ok = await confirm({
      title: 'Restart the DRADIS engine?',
      tone: 'danger',
      confirmLabel: 'Restart',
      body: <p>Open positions keep managing after the ~30-60s respawn.</p>,
    });
    if (!ok) return;
    setRestarting(true);
    setNotice({ kind: 'info', text: 'Engine restarting — back in ~30-60s…' });
    try {
      await restartEngine();
    } catch {
      // Expected if the process exits before the response flushes.
    }
    // Poll status until the engine is back.
    const started = Date.now();
    const poll = setInterval(async () => {
      try {
        await getSetupStatus();
        clearInterval(poll);
        setRestarting(false);
        setNotice({ kind: 'ok', text: 'Engine is back online.' });
        loadCreds();
      } catch {
        if (Date.now() - started > 120_000) {
          clearInterval(poll);
          setRestarting(false);
          setNotice({ kind: 'err', text: 'Engine did not come back within 2 minutes — check the container.' });
        }
      }
    }, 3000);
  };

  if (!status) {
    return <div className="text-center text-gray-500 font-mono text-sm py-16">Loading setup…</div>;
  }

  // First-boot: wizard forces password creation AFTER credentials are entered?
  // Simpler + safer: force password creation FIRST, then the credential forms.
  if (!status.admin_set && !status.auth_disabled) {
    return (
      <div className="space-y-6">
        <div className="bg-amber-500/10 border border-amber-500/30 rounded-xl px-4 py-3 text-xs font-mono text-amber-300">
          ⚠️ First-boot setup — no admin password configured yet. Create one to secure this instance.
        </div>
        <PasswordCard mode="create" onDone={() => { setAuthed(true); loadStatus(); }} />
      </div>
    );
  }

  if (!authed) {
    return <PasswordCard mode="login" onDone={() => { setAuthed(true); }} />;
  }

  const groups = groupsForVenue(status.venue);

  return (
    <div className="space-y-6">
      {!status.venue_configured && (
        <div className="bg-amber-500/10 border border-amber-500/30 rounded-xl px-4 py-3 text-xs font-mono text-amber-300">
          ⚠️ Venue credentials not configured — DRADIS cannot trade until the{' '}
          {status.venue === 'intl' ? 'Polymarket wallet' : 'Polymarket US API keys'} are set.
        </div>
      )}

      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-sm font-mono text-gray-200">⚙️ Setup — Credentials</h2>
          <p className="text-xs text-gray-500 mt-0.5">
            Venue: <span className="text-gray-300">{status.venue === 'intl' ? 'Polymarket CLOB (intl, self-custody)' : 'Polymarket US (custodial)'}</span>
            {' '}· stored in <span className="text-gray-300">data/secrets.env</span> · values are write-only
          </p>
        </div>
        <div className="flex items-center gap-2">
          {status.auth_disabled ? (
            <span className="text-[10px] font-mono text-gray-600 border border-[#1e1e32] rounded-lg px-2 py-1.5">
              admin gate off (DRADIS_SETUP_AUTH=off)
            </span>
          ) : (
            <>
              <button onClick={() => setShowChangePw(v => !v)} className={btnCls('ghost')}>
                {showChangePw ? 'Cancel' : 'Change password'}
              </button>
              <button
                onClick={() => { clearAdminToken(); setAuthed(false); }}
                className={btnCls('ghost')}
              >
                Log out
              </button>
            </>
          )}
        </div>
      </div>

      {showChangePw && (
        <PasswordCard mode="create" onDone={() => { setShowChangePw(false); setNotice({ kind: 'ok', text: 'Admin password updated.' }); }} />
      )}

      {notice && (
        <div className={`text-xs font-mono rounded-xl px-4 py-3 border ${
          notice.kind === 'ok'   ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300'
          : notice.kind === 'err' ? 'bg-rose-500/10 border-rose-500/30 text-rose-300'
          :                         'bg-indigo-500/10 border-indigo-500/30 text-indigo-300'
        }`}>
          {notice.text}
        </div>
      )}

      {creds ? (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {groups.map(g => (
            <CredentialGroup
              key={g.title}
              title={g.title}
              blurb={g.blurb}
              creds={g.keys
                .map(k => creds.find(c => c.key === k))
                .filter((c): c is CredentialInfo => !!c)}
              drafts={drafts}
              onDraft={(k, v) => setDrafts(d => ({ ...d, [k]: v }))}
              testKind={g.test}
            />
          ))}
        </div>
      ) : (
        <div className="text-center text-gray-500 font-mono text-sm py-8">Loading credentials…</div>
      )}

      {creds && (
        <RaptorPanel
          creds={creds}
          drafts={drafts}
          onDraft={(k, v) => setDrafts(d => ({ ...d, [k]: v }))}
          onAuthError={() => { clearAdminToken(); setAuthed(false); }}
        />
      )}

      <ProfilesPanel onAuthError={() => { clearAdminToken(); setAuthed(false); }} />

      <AutonomyPanel onAuthError={() => { clearAdminToken(); setAuthed(false); }} />

      {/* ── Config bundle export / import (instance migration) ────────────── */}
      <div className="bg-[#13131f] border border-[#1e1e32] rounded-xl p-4 space-y-3">
        <div>
          <h3 className="text-sm font-mono text-gray-200">📦 Instance Migration</h3>
          <p className="text-xs text-gray-500 mt-0.5">
            Export this instance&apos;s full configuration — credentials, admin password,
            global + squadron configs — as a single bundle, then import it on a new
            instance (e.g. a newer AMI) and restart. The bundle contains secrets; store it safely.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            className={btnCls('ghost')}
            onClick={async () => {
              try {
                const blob = await exportBundle();
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'dradis-config-bundle.json';
                a.click();
                URL.revokeObjectURL(url);
                setNotice({ kind: 'ok', text: 'Bundle downloaded — treat it as a secret.' });
              } catch (e) {
                setNotice({ kind: 'err', text: e instanceof Error ? e.message : 'Export failed' });
              }
            }}
          >
            ⬇ Export bundle
          </button>
          <label className={btnCls('ghost') + ' cursor-pointer'}>
            ⬆ Import bundle…
            <input
              type="file"
              accept="application/json,.json"
              className="hidden"
              onChange={async (e) => {
                const file = e.target.files?.[0];
                e.target.value = '';
                if (!file) return;
                const ok = await confirm({
                  title: 'Import this bundle?',
                  tone: 'danger',
                  confirmLabel: 'Import',
                  body: (
                    <>
                      <p>Existing credentials and configs will be <span className="text-amber-400">overwritten</span>.</p>
                      <p className="text-gray-500">The engine needs a restart afterwards for the changes to take effect.</p>
                    </>
                  ),
                });
                if (!ok) return;
                try {
                  const text = await file.text();
                  const r = await importBundle(text);
                  setNotice({
                    kind: 'ok',
                    text: `Imported ${r.secrets_imported} secret(s), global config: ${r.dynamic_config_restored ? 'yes' : 'no'}, ${r.squadron_configs_restored} squadron config(s). Restart the engine to apply.`,
                  });
                  loadCreds();
                } catch (err) {
                  setNotice({ kind: 'err', text: err instanceof Error ? err.message : 'Import failed' });
                }
              }}
            />
          </label>
        </div>
      </div>

      {/* ── Support policy ─────────────────────────────────────────────────── */}
      <div className="bg-[#13131f] border border-[#1e1e32] rounded-xl p-4 space-y-2">
        <h3 className="text-sm font-mono text-gray-200">🧪 Support</h3>
        <p className="text-xs text-gray-500">
          DRADIS is community-supported — <span className="text-gray-400">individual support is not
          included</span>. For setup help, ask an AI assistant (ChatGPT, Gemini, Claude): paste the README
          and your question — they are very good at this.
        </p>
        <div className="flex flex-wrap items-center gap-2 pt-1">
          <a
            href="https://github.com/mbordash/DRADIS/issues"
            target="_blank" rel="noreferrer"
            className={btnCls('ghost')}
          >
            🐛 Report a bug (GitHub Issues)
          </a>
          <a
            href="https://github.com/mbordash/DRADIS/discussions"
            target="_blank" rel="noreferrer"
            className={btnCls('ghost')}
          >
            💡 Request a feature (Discussions)
          </a>
        </div>
      </div>

      <div className="flex items-center justify-end gap-2 border-t border-[#1e1e32] pt-4">
        <button onClick={save} disabled={!dirty || saving} className={btnCls('primary')}>
          {saving ? 'Saving…' : 'Save changes'}
        </button>
        <button onClick={restart} disabled={restarting} className={btnCls('danger')}>
          {restarting ? 'Restarting…' : '🔄 Restart engine to apply'}
        </button>
      </div>

      <p className="text-[11px] text-gray-600 font-mono">
        Saved credentials persist on the data volume and override container env on boot.
        Changes take effect after an engine restart (Docker respawns the container automatically).
      </p>
      {confirmDialog}
    </div>
  );
}
