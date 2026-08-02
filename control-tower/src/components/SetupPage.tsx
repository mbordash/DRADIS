'use client';

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
  getSetupStatus, getCredentials, putCredentials, testConnection,
  login, setAdminPassword, restartEngine,
  getAutonomy, putAutonomy,
  getAdminToken, clearAdminToken, SetupApiError,
} from '@/lib/setupApi';

// Which /api/setup/test kind exercises a given credential scope/group.
const TEST_KINDS: Record<string, { kind: string; label: string; keys: string[] }> = {
  intl_wallet: { kind: 'intl_wallet', label: 'Test wallet + CLOB auth', keys: ['POLYMARKET_PRIVATE_KEY'] },
  polygon_rpc: { kind: 'polygon_rpc', label: 'Test RPC', keys: ['POLYGON_RPC_URL'] },
  us_keys:     { kind: 'us_keys',     label: 'Test API keys', keys: ['POLYMARKET_US_KEY_ID', 'POLYMARKET_US_SECRET_KEY'] },
  alpaca:      { kind: 'alpaca',      label: 'Test Alpaca', keys: ['ALPACA_API_KEY_ID', 'ALPACA_API_SECRET_KEY'] },
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
  groups.push(
    { title: 'Alpaca Market Data', blurb: 'Used by the Tide raptor for US equities session data (optional).', keys: ['ALPACA_API_KEY_ID', 'ALPACA_API_SECRET_KEY'], test: 'alpaca' },
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

// ── AI autonomy panel ─────────────────────────────────────────────────────────

const TIER_DEFS: { tier: 1 | 2 | 3; name: string; blurb: string }[] = [
  { tier: 1, name: 'Recommend', blurb: 'AI proposes config changes; nothing applies until you press apply. Proposals expire after 30 min.' },
  { tier: 2, name: 'Limited', blurb: 'Safe changes auto-apply: schema-clamped, delta-capped, rate-limited, never money fields. The rest queue for approval.' },
  { tier: 3, name: 'Autonomous', blurb: 'AI applies its changes directly (still schema-clamped; mode flips excluded). Circuit breaker reverts + demotes on a P&L drawdown.' },
];

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
    if (!window.confirm('Restart the DRADIS engine now? Open positions keep managing after the ~30-60s respawn.')) return;
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

      <AutonomyPanel onAuthError={() => { clearAdminToken(); setAuthed(false); }} />

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
    </div>
  );
}
