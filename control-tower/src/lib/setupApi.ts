/**
 * Setup & credentials API client — talks to /api/setup/* and /api/auth/*.
 *
 * The admin session Bearer token lives in localStorage (dradis_admin_token)
 * and is attached to every setup request; the Next.js proxy forwards the
 * Authorization header to the engine.
 */

const BASE = process.env.NEXT_PUBLIC_API_URL ?? '';
const TOKEN_KEY = 'dradis_admin_token';

// ── Types ────────────────────────────────────────────────────────────────────

export interface SetupStatus {
  venue: 'intl' | 'us';
  admin_set: boolean;
  auth_disabled: boolean;
  venue_configured: boolean;
  alpha_ack: boolean;
  app_version: string;
}

export interface CredentialInfo {
  key: string;
  label: string;
  scope: 'intl' | 'us' | 'shared';
  set: boolean;
  hint: string;            // "…last4" when set
  source: 'managed' | 'env' | 'unset';
}

export interface TestResult {
  ok: boolean;
  ms: number;
  details?: Record<string, unknown>;
  error?: string;
}

// ── Token management ─────────────────────────────────────────────────────────

export function getAdminToken(): string | null {
  if (typeof window === 'undefined') return null;
  return window.localStorage.getItem(TOKEN_KEY);
}

export function setAdminToken(token: string) {
  window.localStorage.setItem(TOKEN_KEY, token);
}

export function clearAdminToken() {
  window.localStorage.removeItem(TOKEN_KEY);
}

function authHeaders(): HeadersInit {
  const h: Record<string, string> = { 'Content-Type': 'application/json' };
  const token = getAdminToken();
  // Sent as X-Admin-Token (NOT Authorization): the Authorization header is
  // owned by CT Basic Auth — overriding it forces the browser login loop.
  // The Next.js proxy translates this to Authorization: Bearer for the engine.
  if (token) h['X-Admin-Token'] = token;
  return h;
}

/** Error carrying the HTTP status so callers can detect 401 → show login. */
export class SetupApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: { ...authHeaders(), ...(init?.headers ?? {}) },
    cache: 'no-store',
  });
  if (!res.ok) {
    let msg = `${init?.method ?? 'GET'} ${path} → ${res.status}`;
    try {
      const body = await res.json();
      if (body?.error) msg = body.error;
    } catch { /* non-JSON error body */ }
    throw new SetupApiError(res.status, msg);
  }
  return res.json();
}

// ── Endpoints ────────────────────────────────────────────────────────────────

export function getSetupStatus(): Promise<SetupStatus> {
  return request('/api/setup/status');
}

/** Record the one-time alpha risk + jurisdiction acknowledgment (public route). */
export function acknowledgeAlpha(): Promise<{ ok: boolean; already_acknowledged: boolean }> {
  return request('/api/setup/acknowledge', { method: 'POST' });
}

export async function login(password: string): Promise<void> {
  const r = await request<{ token: string }>('/api/auth/login', {
    method: 'POST',
    body: JSON.stringify({ password }),
  });
  setAdminToken(r.token);
}

/** Set or change the admin password. Returns a fresh session token. */
export async function setAdminPassword(password: string): Promise<void> {
  const r = await request<{ ok: boolean; token: string }>('/api/setup/admin', {
    method: 'POST',
    body: JSON.stringify({ password }),
  });
  setAdminToken(r.token);
}

export function getCredentials(): Promise<{ credentials: CredentialInfo[] }> {
  return request('/api/setup/credentials');
}

export function putCredentials(
  credentials: Record<string, string>,
): Promise<{ ok: boolean; changed: string[]; restart_required: boolean }> {
  return request('/api/setup/credentials', {
    method: 'PUT',
    body: JSON.stringify({ credentials }),
  });
}

export async function testConnection(
  kind: string,
  credentials: Record<string, string> = {},
): Promise<TestResult> {
  // Failed tests come back 502 with {ok:false, error} — surface as a result,
  // not an exception (401/403 still throw so the login gate can react).
  const res = await fetch(`${BASE}/api/setup/test`, {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify({ kind, credentials }),
    cache: 'no-store',
  });
  if (res.status === 401 || res.status === 403) {
    throw new SetupApiError(res.status, 'admin session required');
  }
  try {
    return await res.json();
  } catch {
    return { ok: false, ms: 0, error: `HTTP ${res.status}` };
  }
}

/** AI autonomy state: tier, kill switch, breaker, and effective policy knobs. */
export interface AutonomyStatus {
  tier: 1 | 2 | 3;
  kill_switch: boolean;
  breaker_demoted: boolean;
  max_patches_per_hour: number;
  max_delta_pct: number;
  breaker_drawdown_usdc: number;
  breaker_window_secs: number;
}

export function getAutonomy(): Promise<AutonomyStatus> {
  return request('/api/setup/autonomy');
}

/** Set tier / kill switch (applies live, persisted) or clear a breaker demotion. */
export function putAutonomy(body: {
  tier?: number;
  kill_switch?: boolean;
  reset_breaker?: boolean;
}): Promise<AutonomyStatus> {
  return request('/api/setup/autonomy', { method: 'PUT', body: JSON.stringify(body) });
}

export function restartEngine(): Promise<{ ok: boolean; message: string }> {
  return request('/api/setup/restart', { method: 'POST' });
}

// ── Config bundle export / import (AMI upgrade path) ─────────────────────────

export interface ImportResult {
  ok: boolean;
  secrets_imported: number;
  dynamic_config_restored: boolean;
  squadron_configs_restored: number;
  restart_required: boolean;
}

/** Download the portable config bundle (secrets + configs). Sensitive file. */
export async function exportBundle(): Promise<Blob> {
  const res = await fetch(`${BASE}/api/setup/export`, { headers: authHeaders() });
  if (!res.ok) throw new Error(`export failed: HTTP ${res.status}`);
  return res.blob();
}

/** Restore a bundle produced by exportBundle. Follow with restartEngine(). */
export function importBundle(bundleJson: string): Promise<ImportResult> {
  return request('/api/setup/import', { method: 'POST', body: bundleJson });
}

// ── Risk-posture config profiles ──────────────────────────────────────────────

export interface ConfigProfile {
  label: string;
  description: string;
  values: Record<string, unknown>;
}

/** How far a profile apply reaches. See `ProfileScope` in src/api/setup.rs. */
export type ProfileScope = 'global_only' | 'global_and_deployed';

export interface ApplyProfileResult {
  ok: boolean;
  profile: string;
  scope: ProfileScope;
  fields_applied: number;
  squadrons_applied: string[];
  squadron_errors: { squadron: string; error: string }[];
}

export function getProfiles(): Promise<{
  schema_version: number;
  profiles: Record<string, ConfigProfile>;
  /** Squadrons a `global_and_deployed` apply would touch, right now. */
  deployed_squadrons: string[];
}> {
  return request('/api/setup/profiles');
}

/**
 * Apply a named profile (audited, no restart).
 *
 * Defaults to `global_and_deployed`: a deployed squadron reads its own
 * `squadron_configs` row, so a global-only apply would leave every running
 * patrol loop on its old values.
 */
export function applyProfile(
  name: string,
  scope: ProfileScope = 'global_and_deployed',
): Promise<ApplyProfileResult> {
  return request('/api/setup/profiles/apply', {
    method: 'POST',
    body: JSON.stringify({ name, scope }),
  });
}
