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
  enable_convergence:  boolean;

  // Arbitrage Viper
  arbitrage_position_size_usdc: string;
  arbitrage_max_exposure_usdc:  string;
  arbitrage_profit_threshold:   string;
  arb_fak_rehedge_buffer:       string;
  arb_max_rescue_cost:          string;

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
  time_decay_max_fast_velocity_pct:      string;
  time_decay_max_slow_drift_pct:         string;
  time_decay_iv_stop_tighten_multiplier: string;
  time_decay_min_hold_secs:              number;

  // Momentum Viper
  momentum_min_trade_size_usdc: string;
  momentum_max_trade_size_usdc: string;
  momentum_stop_loss_pct:       string;
  momentum_target_profit_pct:   string;
  momentum_max_exposure_usdc:   string;
  momentum_max_entry_price:      string;
  momentum_min_entry_price:      string;
  momentum_threshold_pct:        string;
  momentum_max_entry_ask_sum:    string;
  momentum_obi_adverse_block:    string;
  momentum_obi_exhaustion_block: string;
  momentum_take_profit_ceiling:  string;
  momentum_catastrophic_sl_pct:  string;
  momentum_min_secs_to_expiry_for_entry: number;

  // Maker Viper
  maker_max_entry_price:   string;
  maker_min_entry_price:   string;
  maker_stop_loss_pct:     string;
  maker_target_profit_pct: string;
  maker_max_exposure_usdc: string;
  maker_quote_size_usdc: string;
  maker_min_spread:              string;
  maker_bid_buffer:              string;
  maker_cross_buffer:            string;
  maker_max_combined_bid:        string;
  maker_max_complementary_price: string;
  maker_max_book_imbalance_ratio: string;
  maker_min_secs_to_expiry:      number;
  maker_toxic_flow_exit_obi:     string;

  // Basis Viper
  basis_max_exposure_usdc: string;
  basis_stop_loss_pct:     string;
  basis_target_profit_pct: string;
  basis_max_entry_price:         string;
  basis_min_trade_size_usdc:     string;
  basis_max_trade_size_usdc:     string;
  basis_entry_skew_threshold:    string;
  basis_skew_collapse_threshold: string;
  basis_catastrophic_sl_pct:     string;
  basis_min_secs_to_expiry:      number;

  // GBoost Viper
  gboost_entry_threshold:   string;
  gboost_stop_loss_pct:     string;
  gboost_target_profit_pct: string;
  gboost_max_exposure_usdc: string;
  gboost_max_yes_entry_price:   string;
  gboost_max_no_entry_price:    string;
  gboost_min_entry_price:       string;
  gboost_obi_adverse_block:     string;
  gboost_obi_exhaustion_block:  string;
  gboost_min_edge_from_fair:    string;
  gboost_min_net_profit_usdc:   string;
  gboost_min_secs_to_expiry:    number;
  gboost_signal_exit_threshold: string;

  // TrendCapture Viper
  trendcapture_min_trade_size_usdc: string;
  trendcapture_max_trade_size_usdc: string;
  trendcapture_max_exposure_usdc:   string;
  trendcapture_stop_loss_pct:       string;
  trendcapture_target_profit_pct:   string;
  trendcapture_max_entry_price:     string;
  trendcapture_min_entry_price:      string;
  trendcapture_max_entry_ask_sum:    string;
  trendcapture_obi_adverse_block:    string;
  trendcapture_obi_exhaustion_block: string;
  trendcapture_max_token_spread_pct: string;
  trendcapture_reversal_drift_pct:   string;
  trendcapture_strike_gap_pct:       string;
  trendcapture_take_profit_ceiling:  string;
  trendcapture_catastrophic_sl_pct:  string;
  trendreversal_mode:                boolean;

  // Convergence Viper
  convergence_position_size_usdc: string;
  convergence_max_exposure_usdc:  string;
  convergence_stop_loss_pct:      string;
  convergence_target_profit_pct:  string;
  convergence_max_entry_price:    string;
  convergence_min_entry_price:      string;
  convergence_pulse_threshold:      string;
  convergence_coherence_min:        string;
  convergence_cvd_confirm_margin:   string;
  convergence_max_token_spread_pct: string;
  convergence_obi_adverse_block:    string;
  convergence_skip_band_low:        string;
  convergence_skip_band_high:       string;
}

/** One editable config field, from GET /api/config/schema (Rust source of truth). */
export interface ConfigFieldSchema {
  key:         string;          // serde key in DynamicConfig (PATCH target)
  group:       string;          // viper name or "Global"
  enable_key:  string | null;   // owning viper enable flag (null for global)
  label:       string;
  type:        'usd' | 'price' | 'pct' | 'decimal' | 'secs' | 'bool';
  unit:        string | null;
  min:         number | null;
  max:         number | null;
  step:        number | null;
  advanced:    boolean;         // false → Basic panel, true → Advanced modal
  description: string;
}

export interface PnlSnapshotRow {
  ts:          string; // ISO 8601
  session_pnl: string; // Decimal string
  collateral:  string; // Decimal string
  total_value?: string; // Decimal string (Phase 3f-7: cash + positions)
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
  status:         string;  // "pending" (Viper Launch) | "confirmed" (Mission In-Flight)
  current_price?: string;  // Live mark-to-market price from Polymarket Data API (null until first chain-sync)
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

/** Connection health + live signal snapshot for one asset's Binance Raptors. */
export interface AssetRaptorHealth {
  price_connected:   boolean;  // Price Raptor (Binance Spot WS) is live
  funding_connected: boolean;  // Funding Raptor (Binance FAPI REST) last polled OK
  deriv_connected?:  boolean;  // Derivatives Raptor (Binance FAPI REST) last polled OK

  // Live signal values (Decimal → number over the wire). Present from /api/status
  // and /api/telemetry; default 0 until the first Raptor tick arrives.
  oracle_price?: number;  // current spot price (oracle)
  velocity_5s?:  number;  // Δprice over trailing 5s
  velocity_1s?:  number;  // Δprice over trailing 1s
  acceleration?: number;  // rate of change of 5s velocity
  drift_60m?:    number;  // Δprice over trailing 60m
  drift_10m?:    number;  // Δprice over trailing 10m
  funding_rate?: number;  // perpetual funding rate (×100 for percent)
  open_interest?: number; // perp open interest (base contracts)
  oi_delta_pct?:  number;  // Δ open interest vs previous poll (×100 for percent)
  cvd_ratio?:     number;  // taker buy÷sell volume ratio (>1 buy aggression, 0 = no data)

  // ── Tide Raptor — "Institutional Pulse" (spot-BTC-ETF premium) ──
  tide_connected?:      boolean; // ≥1 fresh in-session ETF premium this tick
  tide_market_open?:    boolean; // true during US cash session (09:30–16:00 ET)
  institutional_pulse?: number;  // volume-weighted, vol-normalized aggregate z-score (signed)
  tide_coherence?:      number;  // 0..1 agreement of the Big Three premium signs
  ibit_premium_bps?:    number;  // per-ETF premium vs synthetic iNAV (bps)
  fbtc_premium_bps?:    number;
  arkb_premium_bps?:    number;

  // ── Sports Raptor — line movement (The Odds API, observe-only) ──
  sports_connected?:      boolean; // fresh cross-book consensus this poll
  sports_consensus_prob?: number;  // vig-free consensus implied prob (0..1)
  sports_line_drift?:     number;  // Δ consensus vs previous poll (signed)
  sports_book_dispersion?: number; // spread of per-book implied probs (0..1)
  sports_num_books?:      number;  // bookmakers in the sample (0 = no data)

  // ── Horizon Raptor — TradFi velocity / VIX proxy (Alpaca IEX, observe-only) ──
  horizon_connected?:  boolean; // ≥1 fresh SPY/QQQ/UVXY print this tick
  tradfi_velocity?:    number;  // SPY+QQQ 5s momentum, volume-weighted
  macro_coherence?:    number;  // 10-min Pearson(BTC_vel, QQQ_vel)
  vix_proxy?:          number;  // UVXY price
  vix_velocity?:       number;  // UVXY 5s rate-of-change
}

/** Live Raptor signal snapshot keyed by asset symbol — GET /api/telemetry. */
export type TelemetrySnapshot = Record<string, AssetRaptorHealth>;

/** One timestamped Raptor signal sample from the server ring buffer —
 *  GET /api/telemetry/history. Decimal values arrive as numbers over the wire. */
export interface TelemetrySample {
  t:                 number;  // epoch milliseconds (UTC)
  oracle_price:      number;
  velocity_5s:       number;
  velocity_1s:       number;
  acceleration:      number;
  drift_60m:         number;
  drift_10m:         number;
  funding_rate:      number;  // fraction; ×100 for percent
  open_interest:     number;  // perp open interest (base contracts)
  oi_delta_pct:      number;  // Δ open interest vs previous poll (fraction; ×100 for percent)
  cvd_ratio:         number;  // taker buy÷sell volume ratio (>1 buy aggression, 0 = no data)
  price_connected:   boolean;
  funding_connected: boolean;
  deriv_connected:   boolean;

  // ── Tide Raptor — "Institutional Pulse" (spot-BTC-ETF premium) ──
  tide_connected:      boolean;
  tide_market_open:    boolean;
  institutional_pulse: number;  // signed volume-weighted z-score
  tide_coherence:      number;  // 0..1 agreement
  ibit_premium_bps:    number;
  fbtc_premium_bps:    number;
  arkb_premium_bps:    number;

  // ── Sports Raptor — line movement (The Odds API, observe-only) ──
  sports_connected:      boolean;
  sports_consensus_prob: number;  // vig-free consensus implied prob (0..1)
  sports_line_drift:     number;  // Δ consensus vs previous poll (signed)
  sports_book_dispersion: number; // spread of per-book implied probs (0..1)
  sports_num_books:      number;  // bookmakers in the sample (0 = no data)
  sports_event?:         string;  // tracked event label ("A vs B")
  sports_reference?:     string;  // outcome the consensus/drift refer to
  sports_sport?:         string;  // sport title ("MLB", "NFL", …)
  sports_commence?:      string;  // ISO-8601 UTC kickoff of the tracked event
  sports_books?:         string;  // comma-separated bookmaker titles

  // ── Horizon Raptor — TradFi velocity / VIX proxy (Alpaca IEX, observe-only) ──
  horizon_connected:  boolean;
  horizon_market_open: boolean;
  tradfi_velocity:    number;  // SPY+QQQ 5s momentum, volume-weighted
  macro_coherence:    number;  // 10-min Pearson(BTC_vel, QQQ_vel)
  vix_proxy:          number;  // UVXY price
  vix_velocity:       number;  // UVXY 5s rate-of-change
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

  // ── Market taxonomy (data-driven; resolved from the DB join tables) ─────────
  /** Resolved market domain, e.g. "crypto" | "sports" | "politics" | "unknown". */
  market_class?:     string;
  /** Implemented raptor kinds meaningful for this market class, e.g. ["price","funding"]. */
  raptors?:          string[];
  /** Viper kinds meaningful for this market class, e.g. ["arbitrage","maker"]. */
  vipers?:           string[];
}

// ── Field descriptor for ViperCard ───────────────────────────────────────────

export type FieldType = 'usd' | 'pct' | 'price' | 'decimal' | 'secs';

export interface FieldDef {
  key:   keyof DynamicConfig;
  label: string;
  type:  FieldType;
}

/**
 * Display-only metadata for a viper card. The editable field list is NO LONGER
 * hand-maintained here — ViperCard derives its Basic params from the Rust schema
 * registry (GET /api/config/schema, `advanced:false` entries). This struct only
 * carries presentation bits the schema doesn't model (accent, blurb, status key).
 */
export interface ViperDef {
  name:       string;
  enableKey:  keyof DynamicConfig;
  accentColor: string; // Tailwind color class prefix, e.g. 'indigo'
  description: string;
  /** Lower-snake key used in /api/status strategy_markets map */
  statusKey:  string;
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
    case 'secs':  return String(Math.round(n));    // 1800  → "1800"
    default:      return String(raw);
  }
}

/** Convert display string → API patch value. */
export function fromDisplay(type: FieldType, display: string): string {
  const n = parseFloat(display);
  if (isNaN(n)) return display;
  switch (type) {
    case 'pct':  return (n / 100).toFixed(6); // "8.00" → "0.080000"
    case 'secs': return String(Math.round(n));
    default:     return n.toString();
  }
}

export function fieldUnit(type: FieldType): string {
  switch (type) {
    case 'usd':   return 'USDC';
    case 'pct':   return '%';
    case 'price': return 'cts';
    case 'secs':  return 's';
    default:      return '';
  }
}
