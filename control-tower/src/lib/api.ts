import type { DynamicConfig, ConfigFieldSchema, PnlSnapshotRow, TradeRow, OpenPositionRow, LlmRecommendationRow, LlmActionRow, ViperDef, StatusResponse, PortfolioValue, SquadronSummary, TelemetrySnapshot, TelemetrySample } from './types';

// In development, NEXT_PUBLIC_API_URL=http://localhost:9000 (set in .env.local)
// hits the DRADIS API directly.
//
// In Docker, NEXT_PUBLIC_API_URL is NOT set → BASE = '' → fetch('/api/config')
// → browser calls same-origin /api/* → Next.js rewrite proxy forwards to
//   DRADIS_API_URL (http://dradis-btc:9000) inside the Docker network.
const BASE = process.env.NEXT_PUBLIC_API_URL ?? '';

// API key for authenticated requests (server-side only)
const API_KEY = process.env.DRADIS_API_KEY;

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Append ?asset=<a> to a URL if `asset` is non-empty. */
function withAsset(url: string, asset?: string): string {
  if (!asset) return url;
  const sep = url.includes('?') ? '&' : '?';
  return `${url}${sep}asset=${encodeURIComponent(asset.toLowerCase())}`;
}

/** Build headers with optional API key (when server-side). */
function buildHeaders(): HeadersInit {
  const headers: HeadersInit = {};
  if (API_KEY) {
    headers['X-API-Key'] = API_KEY;
  }
  return headers;
}

// ── Fetchers (used as SWR keys + fetch functions) ────────────────────────────

export async function getAssets(): Promise<string[]> {
  const res = await fetch(`${BASE}/api/assets`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/assets → ${res.status}`);
  return res.json();
}

export async function getConfig(): Promise<DynamicConfig> {
  const res = await fetch(`${BASE}/api/config`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/config → ${res.status}`);
  return res.json();
}

/** Editable-config field schema — drives the dynamic Advanced modal. */
export async function getConfigSchema(): Promise<ConfigFieldSchema[]> {
  const res = await fetch(`${BASE}/api/config/schema`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/config/schema → ${res.status}`);
  return res.json();
}

export async function patchConfig(patch: Partial<DynamicConfig>): Promise<DynamicConfig> {
  const res = await fetch(`${BASE}/api/config`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(patch),
    cache: 'no-store',
  });
  if (!res.ok) throw new Error(`PATCH /api/config → ${res.status}: ${await res.text()}`);
  return res.json();
}

export async function getPnlHistory(limit = 200, asset?: string): Promise<PnlSnapshotRow[]> {
  const url = withAsset(`${BASE}/api/pnl/history?limit=${limit}`, asset);
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/pnl/history → ${res.status}`);
  return res.json();
}

export async function getTrades(limit = 60, asset?: string): Promise<TradeRow[]> {
  const url = withAsset(`${BASE}/api/trades?limit=${limit}`, asset);
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/trades → ${res.status}`);
  return res.json();
}

/** Recent engine log lines (oldest first) from the in-memory ring buffer. */
export async function getLogs(tail = 500): Promise<{ count: number; lines: string[] }> {
  const res = await fetch(`${BASE}/api/logs?tail=${tail}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/logs → ${res.status}`);
  return res.json();
}

export interface LatencySnapshot {
  venue: string;
  ok: boolean;
  probed: boolean;
  last_ms: number | null;
  p50_ms: number | null;
  samples: number;
}

/** Rolling engine→venue round-trip latency (footer meter). */
export async function getLatency(): Promise<LatencySnapshot> {
  const res = await fetch(`${BASE}/api/latency`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/latency → ${res.status}`);
  return res.json();
}

export interface ViperStatusRow {
  strategy: string;
  last_eval_at: string;
  last_eval_secs_ago: number;
  last_outcome: 'signal' | 'no_signal' | 'error' | 'timeout';
  last_reason: string | null;
  last_reason_secs_ago: number | null;
  last_signal_at: string | null;
  last_signal_secs_ago: number | null;
}

/** Per-viper "why aren't we trading?" status registry. */
export async function getVipersStatus(): Promise<ViperStatusRow[]> {
  const res = await fetch(`${BASE}/api/vipers/status`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/vipers/status → ${res.status}`);
  return res.json();
}

/**
 * Download the full tradelog as one CSV. Multi-asset deployments keep one
 * DB per asset, so fetch each asset's export and merge them client-side
 * with a leading `asset` column (prepending to each line is quote-safe).
 */
export async function downloadTradelogCsv(assets: string[]): Promise<void> {
  const list = assets.length > 0 ? assets : ['btc'];
  const merged: string[] = [];
  for (const asset of list) {
    const res = await fetch(withAsset(`${BASE}/api/trades/export`, asset), { cache: 'no-store' });
    if (!res.ok) throw new Error(`GET /api/trades/export (${asset}) → ${res.status}`);
    const lines = (await res.text()).split('\n').filter(l => l.length > 0);
    if (merged.length === 0 && lines.length > 0) merged.push(`asset,${lines[0]}`);
    for (const line of lines.slice(1)) merged.push(`${asset},${line}`);
  }
  const blob = new Blob([merged.join('\n') + '\n'], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `dradis-tradelog-${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

export async function getOpenPositions(asset?: string): Promise<OpenPositionRow[]> {
  const url = withAsset(`${BASE}/api/positions`, asset);
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/positions → ${res.status}`);
  return res.json();
}

export async function getPendingPositions(asset?: string): Promise<OpenPositionRow[]> {
  const url = withAsset(`${BASE}/api/positions/pending`, asset);
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/positions/pending → ${res.status}`);
  return res.json();
}

export async function getConfirmedPositions(asset?: string): Promise<OpenPositionRow[]> {
  const url = withAsset(`${BASE}/api/positions/confirmed`, asset);
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/positions/confirmed → ${res.status}`);
  return res.json();
}

export async function getHealth(): Promise<string> {
  const res = await fetch(`${BASE}/api/health`, { cache: 'no-store' });
  return res.ok ? 'ok' : 'error';
}

export async function getStatus(): Promise<StatusResponse> {
  const res = await fetch(`${BASE}/api/status`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/status → ${res.status}`);
  return res.json();
}

/** Live Raptor signal snapshot (oracle/velocity/drift/funding) keyed by asset. */
export async function getTelemetry(): Promise<TelemetrySnapshot> {
  const res = await fetch(`${BASE}/api/telemetry`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/telemetry → ${res.status}`);
  return res.json();
}

/** Durable Raptor signal history from the server ring buffer (oldest→newest). */
export async function getTelemetryHistory(asset: string, limit = 1800): Promise<TelemetrySample[]> {
  const url = withAsset(`${BASE}/api/telemetry/history?limit=${limit}`, asset);
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/telemetry/history → ${res.status}`);
  return res.json();
}

export async function getLlmRecommendations(limit = 10, asset?: string): Promise<LlmRecommendationRow[]> {
  const url = withAsset(`${BASE}/api/llm/recommendations?limit=${limit}`, asset);
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/llm/recommendations → ${res.status}`);
  return res.json();
}

/** AI action audit trail, newest first (proposed/applied/rejected/…). */
export async function getLlmActions(limit = 100): Promise<LlmActionRow[]> {
  const res = await fetch(`${BASE}/api/llm/actions?limit=${limit}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/llm/actions → ${res.status}`);
  return res.json();
}

/** Approve a proposed AI config change — revalidated server-side, then applied. */
export async function approveLlmAction(id: number): Promise<LlmActionRow> {
  const res = await fetch(`${BASE}/api/llm/actions/${id}/approve`, { method: 'POST', cache: 'no-store' });
  if (!res.ok) throw new Error(`POST /api/llm/actions/${id}/approve → ${res.status}: ${await res.text()}`);
  return res.json();
}

/** Reject a proposed AI config change. */
export async function rejectLlmAction(id: number): Promise<LlmActionRow> {
  const res = await fetch(`${BASE}/api/llm/actions/${id}/reject`, { method: 'POST', cache: 'no-store' });
  if (!res.ok) throw new Error(`POST /api/llm/actions/${id}/reject → ${res.status}: ${await res.text()}`);
  return res.json();
}

export async function getPortfolioValue(): Promise<PortfolioValue> {
  const res = await fetch(`${BASE}/api/portfolio`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/portfolio → ${res.status}`);
  return res.json();
}

export async function getSquadrons(): Promise<SquadronSummary[]> {
  const res = await fetch(`${BASE}/api/squadrons`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/squadrons → ${res.status}`);
  return res.json();
}

export async function getSquadronConfig(squadronId: string): Promise<DynamicConfig> {
  const res = await fetch(`${BASE}/api/squadrons/${encodeURIComponent(squadronId)}/config`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/squadrons/${squadronId}/config → ${res.status}`);
  return res.json();
}

export async function patchSquadronConfig(squadronId: string, patch: Partial<DynamicConfig>): Promise<DynamicConfig> {
  const res = await fetch(`${BASE}/api/squadrons/${encodeURIComponent(squadronId)}/config`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(patch),
    cache: 'no-store',
  });
  if (!res.ok) throw new Error(`PATCH /api/squadrons/${squadronId}/config → ${res.status}: ${await res.text()}`);
  return res.json();
}

// ── Viper metadata ────────────────────────────────────────────────────────────
//
// Presentation-only metadata for each viper card. The editable parameter list is
// NOT defined here anymore — ViperCard derives its Basic params (and the Advanced
// modal its extra params) from the Rust schema registry served at
// GET /api/config/schema. This list only supplies accent color, blurb and the
// /api/status strategy key, none of which the schema models.

// ── Deployment API ────────────────────────────────────────────────────────────

import type { DeploymentRegionInfo, AvailableMarketsResponse, MarketType, DeploySquadronRequest, DeploySquadronResponse, RaptorKind, ViperKindInfo } from './types';

/** Get deployment region and available market types. */
export async function getDeploymentRegion(): Promise<DeploymentRegionInfo> {
  const res = await fetch(`${BASE}/api/deployment/region`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/deployment/region → ${res.status}`);
  return res.json();
}

/** Get available markets for deployment, filtered by type. */
export async function getAvailableMarkets(
  marketType: MarketType,
  options?: { expiryWindow?: string; minLiquidity?: number }
): Promise<AvailableMarketsResponse> {
  const params = new URLSearchParams({ market_type: marketType });
  if (options?.expiryWindow) params.set('expiry_window', options.expiryWindow);
  if (options?.minLiquidity) params.set('min_liquidity', String(options.minLiquidity));
  
  const res = await fetch(`${BASE}/api/markets/available?${params}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/markets/available → ${res.status}`);
  return res.json();
}

/** Get raptors available for a market class. */
export async function getRaptorsForClass(marketClass: MarketType): Promise<RaptorKind[]> {
  const res = await fetch(`${BASE}/api/taxonomy/raptors?market_class=${marketClass}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/taxonomy/raptors → ${res.status}`);
  return res.json();
}

/** Get vipers available for a market class. */
export async function getVipersForClass(marketClass: MarketType): Promise<ViperKindInfo[]> {
  const res = await fetch(`${BASE}/api/taxonomy/vipers?market_class=${marketClass}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/taxonomy/vipers → ${res.status}`);
  return res.json();
}

/** Deploy a new squadron. */
export async function deploySquadron(request: DeploySquadronRequest): Promise<DeploySquadronResponse> {
  const res = await fetch(`${BASE}/api/squadrons/deploy`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
    cache: 'no-store',
  });
  if (!res.ok) {
    const errorText = await res.text();
    return { success: false, error: errorText };
  }
  return res.json();
}

import type { DeploymentStatus } from './types';

/** Get all deployment requests with their status. */
export async function getDeployments(): Promise<DeploymentStatus[]> {
  const res = await fetch(`${BASE}/api/deployments`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/deployments → ${res.status}`);
  return res.json();
}

// ── Viper metadata ────────────────────────────────────────────────────────────

export const VIPER_DEFS: ViperDef[] = [
  {
    name: 'Arbitrage',
    enableKey: 'enable_arbitrage',
    accentColor: 'teal',
    statusKey: 'arbitrage',
    description: 'Hedged maker bids on YES+NO — captures mispriced spread at 0% fee',
  },
  {
    name: 'Time Decay',
    enableKey: 'enable_time_decay',
    accentColor: 'indigo',
    statusKey: 'time_decay',
    description: 'Targets gamma as hourly markets approach expiry',
  },
  {
    name: 'Momentum',
    enableKey: 'enable_momentum',
    accentColor: 'blue',
    statusKey: 'momentum',
    description: 'Rides Binance oracle velocity bursts',
  },
  {
    name: 'Maker',
    enableKey: 'enable_maker',
    accentColor: 'emerald',
    statusKey: 'maker',
    description: 'Two-sided resting bids — captures spread + rebates',
  },
  {
    name: 'Basis',
    enableKey: 'enable_basis',
    accentColor: 'orange',
    statusKey: 'basis',
    description: 'Fades retail-skewed YES/NO implied probabilities',
  },
  {
    name: 'GBoost',
    enableKey: 'enable_gboost',
    accentColor: 'purple',
    statusKey: 'gboost',
    description: 'Online gradient-boosted orderbook classifier',
  },
  {
    name: 'TrendReversal',
    enableKey: 'enable_trendcapture',
    accentColor: 'rose',
    statusKey: 'trendcapture',
    description: 'Fades priced-in multi-minute oracle drift on Window/Daily markets (mean-reversion)',
  },
  {
    name: 'Convergence',
    enableKey: 'enable_convergence',
    accentColor: 'cyan',
    statusKey: 'convergence',
    description: 'Macro-conviction directional (BTC-only): enters on aligned Institutional Pulse + CVD/OI during US hours',
  },
  {
    name: 'FairValue',
    enableKey: 'enable_fairvalue',
    accentColor: 'amber',
    statusKey: 'fairvalue',
    description: 'Analytic binary pricing Φ(ln(S/K)/σ√T) — buys sides trading at a discount to model fair value; snipes settlements',
  },
];

