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
 * Setup & credentials API client — talks to /api/setup/* and /api/auth/*.
 *
 * The admin session Bearer token lives in localStorage (dradis_admin_token)
 * and is attached to every setup request; the Next.js proxy forwards the
 * Authorization header to the engine.
 */

const BASE = process.env.NEXT_PUBLIC_API_URL ?? '';
const TOKEN_KEY = 'dradis_admin_token';

// ── Types ────────────────────────────────────────────────────────────────────

/** Venue compiled into the running engine binary — mirrors `build_venue()`. */
export type VenueId = 'intl' | 'us' | 'kalshi';

/**
 * How this instance was distributed. Drives whether the risk gate shows
 * commercial support contacts or the community "no individual support" wording.
 */
export type Edition = 'community' | 'marketplace';

export interface SetupStatus {
  venue: VenueId;
  /**
   * Venues baked into this image. Length < 2 means there is nothing to switch
   * between (dev build, or the single-venue production image) and the venue
   * selector is hidden.
   */
  venues_available?: VenueId[];
  /**
   * Has the operator actually chosen a venue? False on a fresh multi-venue
   * instance, where the engine is running on its fallback and no choice has
   * been made. The risk gate must not be shown until this is true — its
   * acknowledgment is write-once and stamped with the running venue.
   */
  venue_selected?: boolean;
  /** 'marketplace' on the paid AMI; absent or 'community' everywhere else. */
  edition?: Edition;
  admin_set: boolean;
  auth_disabled: boolean;
  venue_configured: boolean;
  alpha_ack: boolean;
  /** Provider the operator explicitly chose, or "" if none. Never a secret. */
  llm_provider?: string;
  /** True when the LLM Advisor has everything it needs AND is switched on. */
  llm_configured?: boolean;
  /** Provider + credentials are valid, regardless of whether the advisor runs. */
  llm_provider_ready?: boolean;
  /** ENABLE_LLM_ADVISOR — the advisor loop is gated on this. */
  llm_enabled?: boolean;
  app_version: string;
}

export interface CredentialInfo {
  key: string;
  label: string;
  scope: VenueId | 'shared';
  /** Which Setup panel owns this key: core venue, Raptor signal, or integration. */
  panel: 'venue' | 'raptor' | 'integration';
  set: boolean;
  hint: string;            // "…last4" when set
  source: 'managed' | 'env' | 'unset';
  /** True for PEM-style values, which need a textarea: a single-line input
   *  silently strips the newlines a PEM cannot do without. */
  multiline?: boolean;
}

/** How much a Raptor's signal matters — drives the badge on its card. */
export type RaptorTier = 'required' | 'recommended' | 'optional';

/**
 * A Raptor signal source, as advertised by `GET /api/setup/raptors`. The card
 * layout is server-driven so adding a Raptor needs no front-end change: it is
 * enough to add it to `RAPTOR_SOURCES` in `src/api/setup.rs`.
 */
export interface RaptorSource {
  id: string;
  name: string;
  source: string;
  blurb: string;
  tier: RaptorTier;
  /** Env keys this Raptor reads; empty ⇒ nothing to configure. */
  keys: string[];
  /** `POST /api/setup/test` kind validating `keys`, when one exists. */
  test_kind: string | null;
  /** Where to generate the key, when it comes from a third party. Null for
   *  Raptors on public endpoints, which need no account. */
  signup_url: string | null;
  /** Where this feed is unavailable and what happens instead. Null when the
   *  source is reachable everywhere. */
  region_note: string | null;
  /** DynamicConfig field holding this Raptor's poll cadence, when tunable.
   *  Server-driven so a contributed Raptor gets a cadence control for free. */
  poll_field: string | null;
  /** Free-tier allowance, used to show whether a chosen cadence fits the free
   *  plan or needs a paid one. */
  free_quota: { requests: number; period: 'day' | 'month' } | null;
  /** Free-text config keys selecting what this Raptor watches (sport, region,
   *  tour). Rendered as text inputs; the schema description carries the
   *  "not validated" warning. */
  selector_fields: string[];
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

export function getRaptorSources(): Promise<{ raptors: RaptorSource[] }> {
  return request('/api/setup/raptors');
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

/**
 * Select which venue binary the engine runs next.
 *
 * Only meaningful on the multi-venue AMI (see `SetupStatus.venues_available`).
 * The change is written but NOT applied — follow with `restartEngine()`.
 */
export function putVenue(venue: VenueId): Promise<{
  ok: boolean;
  venue: VenueId;
  previous?: VenueId;
  restart_required: boolean;
}> {
  return request('/api/setup/venue', { method: 'PUT', body: JSON.stringify({ venue }) });
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
