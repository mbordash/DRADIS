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

// ── API response shapes ───────────────────────────────────────────────────────

/** Rust Decimal values are serialized as strings over the wire. */
export interface DynamicConfig {
  // Global
  ghost_mode: boolean;
  intl_taker_fee_rate: string;

  // Viper enable flags
  enable_arbitrage:    boolean;
  enable_time_decay:   boolean;
  enable_momentum:     boolean;
  enable_maker:        boolean;
  enable_basis:        boolean;
  enable_gboost:       boolean;
  enable_trendcapture: boolean;
  enable_convergence:  boolean;
  enable_fairvalue:    boolean;

  // FairValue Viper
  fairvalue_trade_size_usdc:   string;
  fairvalue_max_exposure_usdc: string;
  fairvalue_base_edge:         string;
  fairvalue_min_edge:          string;
  fairvalue_min_entry_price:   string;
  fairvalue_prefer_hourly:     boolean;
  fairvalue_max_entry_price:   string;
  fairvalue_target_profit_pct: string;
  fairvalue_stop_loss_pct:     string;
  fairvalue_model_reversal_decay_pct: string;
  fairvalue_sigma_floor_horizon_secs: number;
  fairvalue_post_exit_cooldown_secs:  number;
  fairvalue_max_stop_losses_per_market: number;
  fairvalue_edge_noise_multiple:      string;
  fairvalue_stop_model_confirm_frac:  string;

  // Arbitrage Viper
  arbitrage_position_size_usdc: string;
  arbitrage_max_exposure_usdc:  string;
  arbitrage_profit_threshold:   string;
  arb_fak_rehedge_buffer:       string;
  arb_settle_grace_secs:        number;
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
  momentum_obi_exhaust_max_adverse_pct:  string;
  momentum_obi_exhaust_min_hold_secs:    number;
  momentum_obi_exhaust_persist_secs:     number;
  momentum_tp_fee_margin_mult:           string;

  // Maker Viper
  maker_max_entry_price:   string;
  maker_min_entry_price:   string;
  maker_stop_loss_pct:     string;
  maker_target_profit_pct: string;
  maker_max_exposure_usdc: string;
  maker_quote_size_usdc: string;
  deploy_max_days_to_close:      number;
  auto_deploy_politics:          boolean;
  auto_deploy_sports:            boolean;
  event_market_retire_grace_secs: number;
  gboost_budget:                 string;
  gboost_iteration_limit:        number;
  position_quote_ttl_secs:       number;
  llm_max_output_tokens:         number;
  obi_use_whole_book:            boolean;
  maker_min_spread:              string;
  maker_bid_buffer:              string;
  maker_cross_buffer:            string;
  maker_max_combined_bid:        string;
  maker_max_complementary_price: string;
  maker_max_book_imbalance_ratio: string;
  maker_min_secs_to_expiry:      number;
  maker_min_market_age_secs:     number;
  maker_maturation_max_fraction: string;
  maker_toxic_flow_exit_obi:     string;
  maker_toxic_reentry_cooldown_secs: number;
  maker_toxic_min_hold_secs:     number;
  maker_toxic_min_adverse_pct:   string;
  maker_toxic_obi_confirm_ticks: number;
  maker_oracle_drift_pull_frac:  string;
  maker_oracle_drift_exit_frac:  string;
  maker_resting_exit_enabled:    boolean;
  maker_resting_exit_min_edge_pct: string;
  maker_resting_exit_ask_improvement_ticks: number;
  maker_resting_exit_reprice_threshold: string;

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
  gboost_concept_drift_threshold: string;
  gboost_drift_consecutive_required: number;
  gboost_drift_stable_clear_required: number;

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

  // Raptor polling — live cadence + budget thresholds for the two credentialed,
  // budget-metered Raptors. Numbers, not Decimal strings.
  sports_poll_secs:                 number;
  sports_low_budget_warn:           number;
  tennis_poll_secs:                 number;
  tennis_low_budget_warn:           number;

  // Raptor feed selectors — free-text provider identifiers, passed through to
  // the upstream API verbatim and NOT validated by DRADIS.
  sports_odds_sport:                string;
  sports_odds_regions:              string;
  tennis_tour:                      string;
}

/** One editable config field, from GET /api/config/schema (Rust source of truth). */
export interface ConfigFieldSchema {
  key:         string;          // serde key in DynamicConfig (PATCH target)
  group:       string;          // viper name or "Global"
  enable_key:  string | null;   // owning viper enable flag (null for global)
  label:       string;
  type:        'usd' | 'price' | 'pct' | 'decimal' | 'secs' | 'int' | 'bool' | 'string';
  unit:        string | null;
  min:         number | null;
  max:         number | null;
  step:        number | null;
  advanced:    boolean;         // false → Basic panel, true → Advanced modal
  /**
   * Which config row this field lives in. 'global' fields are written by
   * PATCH /api/config and read instance-wide; 'squadron' fields are written by
   * patchSquadronConfig and read from that squadron's own row.
   *
   * Rendering a field at the wrong scope means operator edits land in a row
   * nothing reads — it happened three times before the engine started declaring
   * this. Prefer it over inferring scope from the group name.
   */
  scope:       'global' | 'squadron';
  description: string;
}

/** Live venue quote for one open position — see GET /api/positions/quotes. */
export interface PositionQuote {
  token_id: string;
  /** What a sale would execute against right now. Null means no bid exists. */
  bid:      string | null;
  ask:      string | null;
  mid:      string | null;
  /** Seconds since this quote was fetched from the venue. */
  age_secs: number;
}

export interface PnlSnapshotRow {
  ts:          string; // ISO 8601
  session_pnl: string; // Decimal string
  collateral:  string; // Decimal string
  total_value?: string; // Decimal string (Phase 3f-7: cash + positions)
}

/**
 * Lifetime aggregates over a shard's entire trade history, from
 * `GET /api/trades/stats`.
 *
 * Summary cards must use this rather than reducing over `getTrades(n)`: that
 * call is a bounded recent window (and the API clamps any limit to 500), so a
 * client-side total silently truncates once history outgrows the window.
 *
 * `wins + losses` need not equal `count` — exactly-zero P&L trades are neither.
 */
export interface TradeStats {
  count:        number;
  wins:         number;
  losses:       number;
  realized_pnl: number;
  fees:         number;
  first_ts:     string | null;
  last_ts:      string | null;
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
  /** Exchange that executed the trade. Null on rows predating the column. */
  venue?:        string | null;
  /** 'crypto' | 'sports' | 'politics' | 'unknown'. Null on legacy rows. */
  market_class?: string | null;
  /**
   * Underlying symbol ('btc', 'eth', 'sol'). Null is meaningful, not missing:
   * sports and politics markets have no underlying instrument.
   */
  underlying?:   string | null;
  /**
   * Total venue fees for the round trip. `pnl` is already NET of this, so
   * `pnl + fees` recovers the gross figure. Null on rows predating fee capture.
   */
  fees?:         string | null;
  /** Was this a simulated fill? False on rows written before the column existed. */
  ghost?:        boolean;
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
  /**
   * When current_price was last refreshed (RFC3339), or null if never.
   *
   * The price is refreshed by a 300s chain-sync sweep off an indexer-backed
   * API, so it can be minutes old. An operator timing a manual RTB needs to
   * see that, or they act on a number the book left behind.
   */
  price_updated_at?: string | null;
  /**
   * Same filing dimensions as TradeRow, so an in-flight row files like the
   * completed trade it becomes. Null on rows written before the columns
   * existed and on reconciliation writes that only know the book — the
   * tradelog falls back to the shard for those.
   */
  venue?:        string | null;
  /** 'crypto' | 'sports' | 'politics' | 'unknown'. Null on legacy rows. */
  market_class?: string | null;
  /** Underlying symbol. Null is meaningful — sports/politics have none. */
  underlying?:   string | null;
}

export interface LlmRecommendationRow {
  id:                 number;
  ts:                 string;   // ISO 8601
  session_id:         string;   // session that produced this recommendation
  model:              string;   // ollama model name
  trade_count:        number;   // trades analyzed
  session_pnl:        string;   // Decimal string
  analysis:           string;   // full LLM output text
  is_current_session: boolean;  // true when generated in the currently-running session
}

/** One row of the llm_actions audit trail — a proposed AI config change. */
export interface LlmActionRow {
  /** Squadron this proposal targets. Null for rows written before the advisor
   *  became squadron-scoped — those were applied to a config no strategy reads. */
  squadron_id?: string | null;
  id:             number;
  batch_id:       string;
  session_id:     string;
  ts:             string;   // ISO 8601
  expires_at:     string;   // ISO 8601 — proposal TTL
  model:          string;
  tier:           number;   // 1 recommend / 2 limited / 3 autonomous
  ghost_mode:     boolean;
  field:          string;   // serde config key
  from_value:     string;   // JSON-encoded current value at proposal time
  to_value:       string;   // JSON-encoded proposed value
  clamped:        boolean;
  delta_pct:      number | null;
  reason:         string;
  status:         'proposed' | 'approved' | 'applied' | 'rejected' | 'expired' | 'reverted' | 'failed';
  status_detail:  string | null;
  status_ts:      string | null;
  inverse_patch:  string | null;
  pnl_at_apply:   number | null;
  outcome_score:  number | null;
  outcome_detail: string | null;
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

  // ── Tennis Raptor — live event state (Live Tennis API, observe-only) ──
  // `tennis_connected` is false on a failed poll OR a stale score, so a stale
  // feed reads exactly like a missing one.
  tennis_connected:     boolean;
  tennis_num_live:      number;  // live matches in the sample (0 = nothing on court)
  tennis_sets_p1:       number;  // sets won, tracked match
  tennis_sets_p2:       number;
  tennis_games_p1:      number;  // games won in the CURRENT set
  tennis_games_p2:      number;
  tennis_server:        number;  // serving side (1/2; 0 = unknown)
  tennis_break_point:   boolean; // receiver holds break point (never in a tiebreak)
  tennis_is_tiebreak:   boolean;
  tennis_feed_age_secs: number;  // age of the score timestamp (-1 = unknown)
  tennis_match?:        string;  // tracked match label ("C. Alcaraz vs J. Sinner")
  tennis_tournament?:   string;  // tournament name
  tennis_tour?:         string;  // "atp" / "wta" / …
  tennis_points?:       string;  // in-game points ("30–40", "AD–40")
  tennis_score_at?:     string;  // ISO-8601 UTC of the last score change

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
  /** Markets whose order-book feed has gone dark. Empty is the healthy case. */
  dark_market_feeds?: { market: string; dark_for_secs: number }[];
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

/**
 * What to print where a market name goes when the squadron holds no market.
 *
 * `market_name` is the empty string while a crypto squadron waits out the gap
 * between hourly markets (the hourly expired, and nothing has cleared the volume
 * floor yet). That is a deliberate, healthy state — the engine refuses to quote
 * into an empty book — and a blank line asked the operator to guess whether
 * something had broken. Say what is happening instead.
 */
export const NO_MARKET_LABEL = 'waiting for a tradeable market';

/** A squadron's market name for display, never blank. */
export function marketLabel(name: string | undefined | null): string {
  return name && name.length > 0 ? name : NO_MARKET_LABEL;
}

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
  /** Crypto underlying that feeds this squadron's raptors (e.g. "btc").
   *  Distinct from `asset` when the venue identity differs from the underlying
   *  (e.g. Kalshi squadron asset is "KALSHI", underlying is "btc"). */
  underlying?:       string;
  /** Implemented raptor kinds meaningful for this market class, e.g. ["price","funding"]. */
  raptors?:          string[];
  /** Viper kinds meaningful for this market class, e.g. ["arbitrage","maker"]. */
  vipers?:           string[];
}

// ── Field descriptor for ViperCard ───────────────────────────────────────────

// 'string' covers free-text provider identifiers (sport keys, tour filters).
// toDisplay/fromDisplay already fall through for non-numeric input, so these
// pass across the wire unchanged.
export type FieldType = 'usd' | 'pct' | 'price' | 'decimal' | 'secs' | 'string';

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
  /** Exact `Strategy::name()` value used to key /api/vipers/status rows.
   *  Not derivable from `statusKey` — e.g. `trendcapture` ↔ `TrendReversalStrategy`. */
  strategyName: string;
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

// ── Squadron Deployment types ────────────────────────────────────────────────

/** Market types available for squadron deployment. */
export type MarketType = 'crypto' | 'sports' | 'politics';

/** Deployment region determines available market types. */
export type DeploymentRegion = 'us' | 'intl' | 'kalshi';

/** Response from GET /api/deployment/region. */
export interface DeploymentRegionInfo {
  region: DeploymentRegion;
  available_types: MarketType[];
}

/** A market available for squadron deployment. */
export interface AvailableMarket {
  condition_id: string;
  question: string;
  market_class: MarketType;
  end_date: string;        // ISO 8601
  liquidity: number;
  tokens: {
    yes_id: string;
    no_id: string;
  };
}

/** Response from GET /api/markets/available. */
export interface AvailableMarketsResponse {
  markets: AvailableMarket[];
}

/** Raptor kind with implementation status. */
export interface RaptorKind {
  id: string;
  display: string;
  implemented: boolean;
}

/** Viper kind with venue compatibility. */
export interface ViperKindInfo {
  id: string;
  display: string;
  venue_agnostic: boolean;
}

/** Request body for POST /api/squadrons/deploy. */
export interface DeploySquadronRequest {
  mode: 'quick' | 'manual';
  market_type: MarketType;
  // Quick mode: DRADIS auto-selects
  auto_config?: boolean;
  // Manual mode: user specifies
  market_id?: string;
  raptors?: string[];
  vipers?: string[];
  /** Per-viper capital budgets: viper kind id → max-exposure USDC. */
  viper_budgets?: Record<string, number>;
  /** Operator-chosen name; distinguishes two squadrons of the same class. */
  name?: string;
}

/** Response from POST /api/squadrons/deploy. */
export interface DeploySquadronResponse {
  success: boolean;
  squadron_id?: string;
  error?: string;
}

/** Response from POST /api/squadrons/{id}/stand-down. */
export interface StandDownResult {
  success: boolean;
  squadron_id: string;
  /** Set when the class's auto-deploy switch was turned off as part of this. */
  auto_deploy_disabled?: string;
  message: string;
}

/** Deployment status from GET /api/deployments. */
export interface DeploymentStatus {
  id: string;
  market_id: string;
  market_type: MarketType;
  raptors: string[];
  vipers: string[];
  /// Written by the engine: queued → claimed → trading → finished, or failed.
  /// 'deployed' was never one of them — the engine writes 'active' when the
  /// squadron starts and 'completed' when its market closes.
  status: 'pending' | 'processing' | 'active' | 'completed' | 'failed' | 'dismissed';
  squadron_id?: string;
  error?: string;
  created_at: string;
}
