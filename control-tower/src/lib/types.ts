// ── API response shapes ───────────────────────────────────────────────────────

/** Rust Decimal values are serialized as strings over the wire. */
export interface DynamicConfig {
  // Global
  ghost_mode: boolean;

  // Viper enable flags
  enable_arbitrage:    boolean;
  enable_time_decay:   boolean;
  enable_momentum:     boolean;
  enable_maker:        boolean;
  enable_basis:        boolean;
  enable_gboost:       boolean;
  enable_trendcapture: boolean;

  // Arbitrage Viper
  arbitrage_position_size_usdc: string;
  arbitrage_max_exposure_usdc:  string;
  arbitrage_profit_threshold:   string;

  // TimeDecay Viper
  time_decay_position_size_usdc:   string;
  time_decay_max_exposure_usdc:    string;
  time_decay_stop_loss_pct:        string;
  time_decay_max_entry_price:      string;
  time_decay_min_entry_price:      string;
  time_decay_obi_adverse_block:    string;
  time_decay_convergence_exit_bid: string;
  time_decay_min_secs_to_expiry:   number;
  time_decay_max_secs_to_expiry:   number;
  min_time_decay_net_profit:       string;

  // Momentum Viper
  momentum_min_trade_size_usdc: string;
  momentum_max_trade_size_usdc: string;
  momentum_stop_loss_pct:       string;
  momentum_target_profit_pct:   string;
  momentum_max_exposure_usdc:   string;

  // Maker Viper
  maker_max_entry_price:   string;
  maker_min_entry_price:   string;
  maker_stop_loss_pct:     string;
  maker_target_profit_pct: string;
  maker_max_exposure_usdc: string;

  // Basis Viper
  basis_max_exposure_usdc: string;
  basis_stop_loss_pct:     string;
  basis_target_profit_pct: string;

  // GBoost Viper
  gboost_entry_threshold:   string;
  gboost_stop_loss_pct:     string;
  gboost_target_profit_pct: string;
  gboost_max_exposure_usdc: string;

  // TrendCapture Viper
  trendcapture_min_trade_size_usdc: string;
  trendcapture_max_trade_size_usdc: string;
  trendcapture_max_exposure_usdc:   string;
  trendcapture_stop_loss_pct:       string;
  trendcapture_target_profit_pct:   string;
  trendcapture_max_entry_price:     string;
}

export interface PnlSnapshotRow {
  ts:          string; // ISO 8601
  session_pnl: string; // Decimal string
  collateral:  string; // Decimal string
}

export interface TradeRow {
  ts:          string;
  strategy:    string;
  market:      string;
  side:        string;
  entry_price: string;
  exit_price:  string;
  shares:      string;
  pnl:         string;
  reason:      string;
}

/** A position that has been entered but not yet exited (all strategies, ghost+live). */
export interface OpenPositionRow {
  ts:             string;  // entry timestamp (ISO 8601) — or adoption timestamp if chain_adopted
  strategy:       string;
  token_id:       string;
  market:         string;
  side:           string;  // "YES" | "NO" | "UP" | "DOWN" (varies by market type)
  entry_price:    string;  // Decimal string
  shares:         string;  // Decimal string
  ghost_mode:     boolean;
  chain_adopted:  boolean; // true when re-adopted from on-chain (ts = adoption time, not original entry)
}

export interface LlmRecommendationRow {
  id:                 number;
  ts:                 string;   // ISO 8601
  session_id:         string;   // session that produced this recommendation
  model:              string;   // ollama model name
  trade_count:        number;   // trades analysed
  session_pnl:        string;   // Decimal string
  analysis:           string;   // full LLM output text
  is_current_session: boolean;  // true when generated in the currently-running session
}

/** Connection health for one asset's pair of Binance Raptors. */
export interface AssetRaptorHealth {
  price_connected:   boolean;  // Price Raptor (Binance Spot WS) is live
  funding_connected: boolean;  // Funding Raptor (Binance FAPI REST) last polled OK
}

/** Response from GET /api/status — maps strategy key to active market name. */
export interface StatusResponse {
  strategy_markets: Record<string, string>;
  /** RFC-3339 timestamp of the current bot session start (= process startup). */
  session_started_at?: string;
  /** Per-asset Binance Raptor connection health. Key = asset symbol (e.g. "btc"). */
  raptors?: Record<string, AssetRaptorHealth>;
}

/** Portfolio value response from /api/portfolio — cash + open positions at live prices. */
export interface PortfolioValue {
  collateral:      string; // pUSD cash on deposit
  positions_value: string; // Σ(shares × current mid-price)
  total_value:     string; // collateral + positions_value
  unrealized_pnl:  string; // Σ(shares × (current_mid − entry_price))
  position_count:  number;
  prices_live:     boolean; // false when Polymarket CLOB was unreachable
}

// ── Squadron / CAG types (Phase 3d) ──────────────────────────────────────────

/** Lifecycle state string returned by the CAG registry. */
export type SquadronState = 'STAGED' | 'DEPLOYED' | 'PATROLLING' | 'RTB' | 'STOOD_DOWN';

/** Summary of one active squadron — returned by GET /api/squadrons and GET /api/squadrons/{id}. */
export interface SquadronSummary {
  id:                string;        // e.g. "btc-hourly-2026-05-29T14:00:00Z"
  asset:             string;        // "BTC" | "ETH" | "SOL" | …
  name:              string;        // SquadronConfig::name
  state:             SquadronState;
  market_name:       string;        // primary (hourly) Polymarket market name
  maker_market_name?: string;       // window/daily maker venue name (present once fee-rate fetch completes)
  deployed_at:       string;        // ISO 8601
}

// ── Field descriptor for ViperCard ───────────────────────────────────────────

export type FieldType = 'usd' | 'pct' | 'price' | 'decimal';

export interface FieldDef {
  key:   keyof DynamicConfig;
  label: string;
  type:  FieldType;
}

export interface ViperDef {
  name:       string;
  enableKey:  keyof DynamicConfig;
  accentColor: string; // Tailwind color class prefix, e.g. 'indigo'
  description: string;
  /** Lower-snake key used in /api/status strategy_markets map */
  statusKey:  string;
  fields:     FieldDef[];
}

// ── Conversion helpers ───────────────────────────────────────────────────────

/** Convert raw API value → display string for an input field. */
export function toDisplay(type: FieldType, raw: string | number): string {
  const n = parseFloat(String(raw));
  if (isNaN(n)) return String(raw);
  switch (type) {
    case 'pct':   return (n * 100).toFixed(2);    // 0.08 → "8.00"
    case 'usd':   return n.toFixed(2);             // 15    → "15.00"
    case 'price': return n.toFixed(4);             // 0.48  → "0.4800"
    default:      return String(raw);
  }
}

/** Convert display string → API patch value. */
export function fromDisplay(type: FieldType, display: string): string {
  const n = parseFloat(display);
  if (isNaN(n)) return display;
  switch (type) {
    case 'pct': return (n / 100).toFixed(6); // "8.00" → "0.080000"
    default:    return n.toString();
  }
}

export function fieldUnit(type: FieldType): string {
  switch (type) {
    case 'usd':   return 'USDC';
    case 'pct':   return '%';
    case 'price': return 'cts';
    default:      return '';
  }
}
