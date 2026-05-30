import type { DynamicConfig, PnlSnapshotRow, TradeRow, OpenPositionRow, LlmRecommendationRow, ViperDef, StatusResponse, PortfolioValue, SquadronSummary } from './types';

// In development, NEXT_PUBLIC_API_URL=http://localhost:9000 (set in .env.local)
// hits the DRADIS API directly.
//
// In Docker, NEXT_PUBLIC_API_URL is NOT set → BASE = '' → fetch('/api/config')
// → browser calls same-origin /api/* → Next.js rewrite proxy forwards to
//   DRADIS_API_URL (http://dradis-btc:9000) inside the Docker network.
const BASE = process.env.NEXT_PUBLIC_API_URL ?? '';

// ── Fetchers (used as SWR keys + fetch functions) ────────────────────────────

export async function getConfig(): Promise<DynamicConfig> {
  const res = await fetch(`${BASE}/api/config`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/config → ${res.status}`);
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

export async function getPnlHistory(limit = 200): Promise<PnlSnapshotRow[]> {
  const res = await fetch(`${BASE}/api/pnl/history?limit=${limit}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/pnl/history → ${res.status}`);
  return res.json();
}

export async function getTrades(limit = 60): Promise<TradeRow[]> {
  const res = await fetch(`${BASE}/api/trades?limit=${limit}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/trades → ${res.status}`);
  return res.json();
}

export async function getOpenPositions(): Promise<OpenPositionRow[]> {
  const res = await fetch(`${BASE}/api/positions`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/positions → ${res.status}`);
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

export async function getLlmRecommendations(limit = 10): Promise<LlmRecommendationRow[]> {
  const res = await fetch(`${BASE}/api/llm/recommendations?limit=${limit}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`GET /api/llm/recommendations → ${res.status}`);
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

// ── Viper metadata ────────────────────────────────────────────────────────────

export const VIPER_DEFS: ViperDef[] = [
  {
    name: 'Arbitrage',
    enableKey: 'enable_arbitrage',
    accentColor: 'teal',
    statusKey: 'arbitrage',
    description: 'Hedged maker bids on YES+NO — captures mispriced spread at 0% fee',
    fields: [
      { key: 'arbitrage_position_size_usdc', label: 'Position Size',     type: 'usd'     },
      { key: 'arbitrage_max_exposure_usdc',  label: 'Max Exposure',      type: 'usd'     },
      { key: 'arbitrage_profit_threshold',   label: 'Min Profit/Share',  type: 'price'   },
    ],
  },
  {
    name: 'Time Decay',
    enableKey: 'enable_time_decay',
    accentColor: 'indigo',
    statusKey: 'time_decay',
    description: 'Targets gamma as hourly markets approach expiry',
    fields: [
      { key: 'time_decay_position_size_usdc', label: 'Position Size',  type: 'usd'   },
      { key: 'time_decay_max_exposure_usdc',  label: 'Max Exposure',   type: 'usd'   },
      { key: 'time_decay_stop_loss_pct',      label: 'Stop Loss',      type: 'pct'   },
      { key: 'time_decay_max_entry_price',    label: 'Max Entry',      type: 'price' },
    ],
  },
  {
    name: 'Momentum',
    enableKey: 'enable_momentum',
    accentColor: 'blue',
    statusKey: 'momentum',
    description: 'Rides Binance oracle velocity bursts',
    fields: [
      { key: 'momentum_min_trade_size_usdc', label: 'Min Size',    type: 'usd' },
      { key: 'momentum_max_trade_size_usdc', label: 'Max Size',    type: 'usd' },
      { key: 'momentum_stop_loss_pct',       label: 'Stop Loss',   type: 'pct' },
      { key: 'momentum_target_profit_pct',   label: 'Take Profit', type: 'pct' },
      { key: 'momentum_max_exposure_usdc',   label: 'Max Exposure',type: 'usd' },
    ],
  },
  {
    name: 'Maker',
    enableKey: 'enable_maker',
    accentColor: 'emerald',
    statusKey: 'maker',
    description: 'Two-sided resting bids — captures spread + rebates',
    fields: [
      { key: 'maker_max_entry_price',   label: 'Max Entry',   type: 'price' },
      { key: 'maker_stop_loss_pct',     label: 'Stop Loss',   type: 'pct'   },
      { key: 'maker_target_profit_pct', label: 'Take Profit', type: 'pct'   },
      { key: 'maker_max_exposure_usdc', label: 'Max Exposure',type: 'usd'   },
    ],
  },
  {
    name: 'Basis',
    enableKey: 'enable_basis',
    accentColor: 'orange',
    statusKey: 'basis',
    description: 'Fades retail-skewed YES/NO implied probabilities',
    fields: [
      { key: 'basis_stop_loss_pct',     label: 'Stop Loss',   type: 'pct' },
      { key: 'basis_target_profit_pct', label: 'Take Profit', type: 'pct' },
      { key: 'basis_max_exposure_usdc', label: 'Max Exposure',type: 'usd' },
    ],
  },
  {
    name: 'GBoost',
    enableKey: 'enable_gboost',
    accentColor: 'purple',
    statusKey: 'gboost',
    description: 'Online gradient-boosted orderbook classifier',
    fields: [
      { key: 'gboost_entry_threshold',   label: 'Entry Threshold', type: 'decimal' },
      { key: 'gboost_stop_loss_pct',     label: 'Stop Loss',       type: 'pct'     },
      { key: 'gboost_target_profit_pct', label: 'Take Profit',     type: 'pct'     },
      { key: 'gboost_max_exposure_usdc', label: 'Max Exposure',    type: 'usd'     },
    ],
  },
];

