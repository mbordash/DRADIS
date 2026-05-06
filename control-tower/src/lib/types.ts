// ── API response shapes ───────────────────────────────────────────────────────

/** Rust Decimal values are serialized as strings over the wire. */
export interface DynamicConfig {
  // Global
  ghost_mode: boolean;

  // Viper enable flags
  enable_time_decay: boolean;
  enable_momentum:   boolean;
  enable_maker:      boolean;
  enable_basis:      boolean;
  enable_gboost:     boolean;

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

