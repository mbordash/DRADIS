use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Duration as StdDuration;

// ============================================================================
// NOTIFICATION SETTINGS (Telegram)
// ============================================================================

pub const ENABLE_TELEGRAM: bool = true;

// ============================================================================
// ARBITRAGE STRATEGY PARAMETERS
// ============================================================================

/// If true, the bot will log trades but NOT actually execute them on-chain.
/// ⚠️  Set to `false` only after verifying signals in a dry run.
pub const GHOST_MODE: bool = false;

/// Margin threshold to trigger entry.
/// IMPORTANT: This should be higher than (2 * BUY_PRICE_OFFSET) to be profitable.
pub const ARBITRAGE_PROFIT_THRESHOLD: Decimal = dec!(0.05);

pub const MAX_SUM_PRICE_FOR_ENTRY: Decimal = dec!(0.98);

/// Minimum number of shares for a single order (exchange requirement)
/// Minimum shares to treat a fill as real (not dust).
/// At MAKER_MIN_ENTRY_PRICE=$0.10, 1 share = $0.10 minimum value — still economical.
/// Lowered from 5.0: a 3.87-share partial fill at $0.61 is a real $2.36 position.
pub const MIN_ORDER_SHARES: Decimal = dec!(1.0);

/// Minimum USDC value for a single order (exchange requirement)
pub const MIN_ORDER_USDC: Decimal = dec!(1.05);

/// Minimum ratio of target shares that MUST be available at the top of the book
/// before firing an order. Prevents massive partial fills on thin books.
pub const MIN_LIQUIDITY_FILL_RATIO: Decimal = dec!(0.80);

/// Price offset added to the ask price when placing buy orders to ensure aggressive fills.
pub const BUY_PRICE_OFFSET: Decimal = dec!(0.01);

/// Extra price offset for momentum buy orders — compensates for MM repricing during fast moves.
/// Applied ON TOP of BUY_PRICE_OFFSET. Total momentum offset = BUY_PRICE_OFFSET + MOMENTUM_BUY_PRICE_OFFSET.
pub const MOMENTUM_BUY_PRICE_OFFSET: Decimal = dec!(0.03);

/// Number of price-bump retries when a momentum FAK entry is rejected for no liquidity.
/// Each retry bumps the price by MOMENTUM_BUY_PRICE_OFFSET again, up to MAX_BUY_LIMIT_PRICE.
pub const MOMENTUM_ENTRY_FAK_RETRIES: u32 = 2;

/// Maximum price for a single share when placing a buy order.
pub const MAX_BUY_LIMIT_PRICE: Decimal = dec!(0.99);

/// Price offset subtracted from the bid price when placing sell orders to ensure aggressive fills.
pub const SELL_PRICE_OFFSET: Decimal = dec!(0.01);
/// Minimum price for a single share when placing a sell order.
pub const MIN_SELL_LIMIT_PRICE: Decimal = dec!(0.01);

/// Combined bid threshold (YES_bid + NO_bid) to trigger an early exit from a hedged position.
pub const EARLY_EXIT_COMBINED_BID_THRESHOLD: Decimal = dec!(0.995);


// ============================================================================
// MOMENTUM & ORACLE SETTINGS (Predictive Arbitrage)
// ============================================================================

/// Allow one-sided momentum trades (riskier, non-hedged entries based on Binance oracle)
pub const ENABLE_MOMENTUM_TRADING: bool = true;

// ── Fractional Kelly Position Sizing (Momentum only) ─────────────────────────
//
// Trade size scales linearly between MIN and MAX based on how many multiples of
// the velocity threshold the current signal is.  At exactly 1× threshold you get
// the minimum size; at KELLY_MAX_MULTIPLIER× (or above) you get the maximum.
// This rewards high-conviction signals without over-betting on marginal ones.
//
// Example (BTC, threshold = $75/5s):
//   velocity = $75  →  1× → $5  USDC
//   velocity = $150 →  2× → $11.67 USDC
//   velocity = $300 →  4× → $25 USDC (max)

/// Signal strength at which trade size saturates at MOMENTUM_MAX_TRADE_SIZE_USDC.
/// Signals above this multiple are capped — no benefit to overbetting extreme moves.
pub const MOMENTUM_KELLY_MAX_MULTIPLIER: Decimal = dec!(4.0);

/// Minimum trade size for a momentum entry (used at exactly 1× threshold).
/// Keep above MIN_ORDER_USDC ($1.05).
pub const MOMENTUM_MIN_TRADE_SIZE_USDC: Decimal = dec!(5.0);

/// Maximum trade size for a momentum entry (used at MOMENTUM_KELLY_MAX_MULTIPLIER× threshold).
/// Should not exceed MOMENTUM_MAX_EXPOSURE_USDC ($25).
pub const MOMENTUM_MAX_TRADE_SIZE_USDC: Decimal = dec!(25.0);

/// Number of consecutive signal ticks required before firing a trade.
/// Set to 2 to filter out single-tick outliers and "fakeouts".
pub const MOMENTUM_CONFIRMATION_TICKS: u32 = 2;

/// Dynamic Take Profit: Exit if we gain this % over our entry price.
/// Lowered to 3% to capture quick moves in volatile markets.
pub const MOMENTUM_TARGET_PROFIT_PERCENT: Decimal = dec!(0.05);

/// Stop Loss: Exit if we lose this % from our entry price.
pub const MOMENTUM_STOP_LOSS_PERCENT: Decimal = dec!(0.10);

/// Reversal Exit: Exit if momentum velocity reverses past this % of the entry threshold.
/// NOTE: Reversal is directional — flat velocity (near zero) does NOT trigger reversal exit.
/// Only a strong move in the OPPOSITE direction triggers this.
/// Raised from 0.20 → 0.75: requires $56.25/5s BTC reversal (vs $15 before).
/// This prevents being shaken out by brief bounces within a continuing trend.
pub const MOMENTUM_REVERSAL_RATIO: Decimal = dec!(0.75);

/// Minimum seconds to hold a momentum position before allowing a reversal exit.
/// Prevents panic-exiting immediately after entry when velocity naturally decays
/// as the 5-second measurement window slides past the initial price spike.
/// Raised from 12 → 25: gives the position more time to develop before reversal check activates.
pub const MOMENTUM_MIN_HOLD_SECS_BEFORE_REVERSAL: i64 = 25;

/// Static Take Profit Ceiling: Exit if bid hits this price regardless of entry.
pub const MOMENTUM_TAKE_PROFIT_CEILING: Decimal = dec!(0.90);

/// Minimum distance from strike price (in USD) to trigger a momentum trade.
pub const BTC_STRIKE_BUFFER: Decimal = dec!(50.0);
pub const ETH_STRIKE_BUFFER: Decimal = dec!(5.0);
pub const SOL_STRIKE_BUFFER: Decimal = dec!(0.2);

/// Maximum token price allowed for a momentum entry.
/// Raised from 0.85 to 0.88 to allow entries slightly after market repricing.
/// Still blocks extreme prices (0.90+) where risk/reward is poor.
pub const MAX_MOMENTUM_ENTRY_PRICE: Decimal = dec!(0.88);
pub const MAX_MOMENTUM_CROSSING_ENTRY_PRICE: Decimal = dec!(0.75);

/// The time window (in seconds) used to calculate price velocity from Binance.
/// Shortened from 10s to 5s for faster signal detection before Polymarket reprices.
pub const MOMENTUM_WINDOW_SECS: u64 = 5;

/// Price change threshold (absolute USD) within the window to trigger a signal.
/// Lowered to catch momentum earlier — before market makers fully reprice the book.
/// Thresholds scaled for 5s window (roughly half of previous 10s values).
pub const BTC_MOMENTUM_THRESHOLD: Decimal = dec!(75.0);
pub const ETH_MOMENTUM_THRESHOLD: Decimal = dec!(3.0);
pub const SOL_MOMENTUM_THRESHOLD: Decimal = dec!(0.3);


// ============================================================================
// MAKER (PASSIVE LIMIT ORDER) STRATEGY PARAMETERS
// ============================================================================

/// Allow passive maker orders (post bids below the ask; makers pay 0 fees on Polymarket —
/// only taker exits incur the market fee rate).
/// Conservative by default — only fires when spread is wide, expiry is far, and market is mature.
pub const ENABLE_MAKER_TRADING: bool = true;

/// Minimum bid-ask spread (YES or NO side) required to post a maker order.
/// Wide spread = more room to profit before adverse selection erodes the edge.
/// Conservative default: 5 cents. Lower only after validating fill quality.
pub const MAKER_MIN_SPREAD: Decimal = dec!(0.05);

/// How much to improve over the current best bid when posting the maker order.
/// One tick (0.01) gives queue priority without giving away too much edge.
/// This is the FALLBACK value used when spread is zero or not computable.
pub const MAKER_BID_IMPROVEMENT: Decimal = dec!(0.01);

/// Fraction of the current bid-ask spread to use as the bid improvement.
/// e.g. 0.30 = post 30% of the way through the spread above the best bid.
/// This keeps the order below the ask in tight-spread markets, preventing
/// "invalid post-only order: order crosses book" rejections.
pub const MAKER_BID_IMPROVEMENT_RATIO: Decimal = dec!(0.30);

/// Floor for the computed spread-relative bid improvement (one tick minimum).
pub const MAKER_MIN_BID_IMPROVEMENT: Decimal = dec!(0.01);

/// Ceiling for the computed spread-relative bid improvement (avoid overpaying).
pub const MAKER_MAX_BID_IMPROVEMENT: Decimal = dec!(0.03);

/// Short cooldown (seconds) applied after a "crosses book" post-only rejection.
/// These are market-microstructure events, NOT system failures — they must NOT
/// count toward the circuit breaker's consecutive_failures counter.
pub const CROSSES_BOOK_COOLDOWN_SECS: i64 = 30;

/// Do not post maker orders if market closes within this many seconds.
/// Raised from 600s (10 min) → 1800s (30 min): blocks late-session entries where
/// adverse selection risk is highest and there is no time for price recovery.
pub const MAKER_MIN_SECS_TO_EXPIRY: i64 = 1800;

/// Maximum token price allowed for a maker entry bid.
/// Lowered from $0.65 → $0.55: entries at $0.55+ mean the market already believes
/// the outcome is likely decided — adverse selection risk is maximum at high YES prices.
/// The 7PM winner entered at $0.50 (allowed). The 8PM -$2.38 disaster entered at $0.64 (blocked).
/// At $0.55 we only post bids when the market is genuinely uncertain (near 50/50).
pub const MAKER_MAX_ENTRY_PRICE: Decimal = dec!(0.55);

/// Minimum bid price for a maker entry.
/// Blocks entries on tokens priced near zero (market already resolved against this side).
/// A WS snapshot lag can cause the complementary-token check to pass on a crashed market;
/// this floor catches it regardless of snapshot staleness.
pub const MAKER_MIN_ENTRY_PRICE: Decimal = dec!(0.10);

/// Take-profit: exit when position gains this % over avg entry.
/// Raised from 4% → 8%: improves risk/reward to 1.6:1 (need only 38% wins to break even).
pub const MAKER_TARGET_PROFIT_PERCENT: Decimal = dec!(0.08);

/// Stop-loss: exit when position loses this % from avg entry.
/// Raised from 3% → 5%: at $0.62 entry, 3% stop = $0.019 move — too tight for market noise.
/// 5% gives $0.031 breathing room while still cutting adverse selections quickly.
pub const MAKER_STOP_LOSS_PERCENT: Decimal = dec!(0.05);
pub const MIN_HOLD_SECS_BEFORE_STOP_LOSS: i64 = 300;

/// Cooldown (seconds) before MakerStrategy may re-enter after a stop-loss exit.
/// Prevents immediately re-posting into the same adverse directional move.
/// 10 minutes gives the market time to mean-revert (or confirm the trend).
pub const MAKER_STOP_LOSS_COOLDOWN_SECS: i64 = 600;

/// Maximum combined bid for simultaneous YES + NO maker quotes.
/// If YES_bid_price + NO_bid_price >= this threshold we would be offering
/// a near-riskless arb to takers who sell both legs to us (they collect
/// our combined bid and receive $1.00 at settlement).
/// 0.90 leaves a minimum 10¢ margin on every two-sided quote.
pub const MAKER_MAX_COMBINED_BID: Decimal = dec!(0.90);

/// Maximum per-side bid price offset applied for inventory skew.
/// When inventory is 100% imbalanced (all YES, no NO), the YES bid
/// is lowered by this amount (less aggressive) and the NO bid is
/// raised by this amount (more aggressive to rebalance faster).
/// 3¢ at full imbalance is meaningful without overshooting the spread.
pub const MAKER_INVENTORY_SKEW_MAX: Decimal = dec!(0.03);


/// Minimum seconds the bot must have been trading on the CURRENT market before
/// MakerStrategy is allowed to enter.  The first few minutes of a new hourly market
/// often have wild, unstable pricing (large swings) — entering during this phase
/// leads to buying local peaks that immediately revert.
pub const MAKER_MIN_MARKET_AGE_SECS: i64 = 600; // 10 minutes


// ============================================================================
// TIME DECAY (THETA) STRATEGY PARAMETERS
// ============================================================================

/// Allow time decay trading (exploits YES+NO convergence to $1.00)
pub const ENABLE_TIME_DECAY_TRADING: bool = true;

/// Minimum net profit per share (after fees) for settlement-mode entry.
/// At 0 bps fees: requires combined_ask < $0.998.  At 100 bps: < ~$0.988.
pub const MIN_TIME_DECAY_NET_PROFIT: Decimal = dec!(0.002);

/// Maximum combined ask price for convergence-mode entry.
/// Allows entries slightly above $1.00 — profit comes from bid convergence near expiry.
pub const MAX_TIME_DECAY_COMBINED_ASK: Decimal = dec!(1.008);

/// Convergence mode only activates when market is within this many seconds of expiry.
/// Tighter window = higher convergence confidence.
pub const TIME_DECAY_CONVERGENCE_WINDOW_SECS: i64 = 1200;  // 20 minutes

/// Exit convergence-mode positions when combined bid reaches this level.
pub const TIME_DECAY_CONVERGENCE_EXIT_BID: Decimal = dec!(0.998);

/// Minimum seconds to expiry for time decay entry (must exceed MARKET_EXPIRY_SAFETY_BUFFER_SECS)
pub const TIME_DECAY_MIN_SECS_TO_EXPIRY: i64 = 240;  // 4 minutes

/// Maximum seconds to expiry for time decay entry
pub const TIME_DECAY_MAX_SECS_TO_EXPIRY: i64 = 1800;  // 30 minutes

/// Base position size for time decay (per side: YES and NO)
pub const TIME_DECAY_POSITION_SIZE_USDC: Decimal = dec!(10);

/// Auto-exit when profit reaches this percentage
pub const TIME_DECAY_TARGET_PROFIT_PERCENT: Decimal = dec!(0.015);  // 1.5% profit target

/// Maximum combined exposure in time decay positions
pub const TIME_DECAY_MAX_TOTAL_EXPOSURE_USDC: Decimal = dec!(50);

/// Maximum number of simultaneous time decay positions
pub const TIME_DECAY_MAX_POSITIONS: usize = 5;

/// Exit if spread widens more than this (lose this much)
pub const TIME_DECAY_STOP_LOSS_PERCENT: Decimal = dec!(0.01);  // Stop at 1% loss


// ============================================================================
// EMERGENCY CIRCUIT BREAKERS
// ============================================================================

/// Maximum number of consecutive failed trade attempts before the bot kills itself.
pub const MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// Cooldown period after a failed trade attempt (seconds).
pub const FAILURE_COOLDOWN_SECS: i64 = 60;


// ============================================================================
// TRADING PARAMETERS - Price Thresholds and Position Sizing
// ============================================================================

/// Price scale conversion factor for on-chain operations (shares to base units)
pub const SHARE_SCALE: Decimal = dec!(1_000_000);


// ============================================================================
// POSITION DURATION AND TIMING (Seconds)
// ============================================================================

/// Cooldown period after a successful trade before entering a new one.
pub const TRADE_COOLDOWN_SECS: i64 = 8;

/// Cooldown period after a partial fill before attempting a new trade.
pub const PARTIAL_FILL_COOLDOWN_SECS: i64 = 30;

/// Minimum time until market expiry to still allow new position entry (seconds)
/// Increased from 60 to 300 to ensure markets don't expire while trading executes
pub const MIN_SECONDS_TO_EXPIRY_FOR_ENTRY: i64 = 300;

/// Maximum time until market expiry to consider a market (seconds, 4 hours)
pub const MAX_SECONDS_TO_EXPIRY_FOR_ENTRY: i64 = 14400;

/// Final expiry window: stops all trading this close to market close (seconds, 10 minutes)
pub const FINAL_EXPIRY_WINDOW_SECS: i64 = 600;

/// Safety buffer to re-check market expiry before each trade (seconds)
/// Prevents selecting markets that expire during order execution
pub const MARKET_EXPIRY_SAFETY_BUFFER_SECS: i64 = 180;


// ============================================================================
// INTERVAL TIMINGS (Duration for Market Scanning and Monitoring)
// ============================================================================

/// Market switch evaluation interval (checks for better markets to trade)
pub fn market_switch_interval() -> StdDuration {
    StdDuration::from_secs(90)
}

/// Periodic on-chain balance sync interval (syncs positions with blockchain)
pub fn periodic_sync_interval() -> StdDuration {
    StdDuration::from_secs(300)
}

/// Main ticker interval for trade execution checks (milliseconds)
/// Reduced from 100ms to 50ms for faster momentum signal response.
/// At 50ms, worst-case polling jitter is halved, and 2-tick confirmation
/// resolves in ~100ms instead of ~200ms.
pub fn main_ticker_interval() -> StdDuration {
    StdDuration::from_millis(50)
}


// ============================================================================
// MONITORING AND LOGGING INTERVALS
// ============================================================================

/// Status log interval (shows open positions, P&L summary)
pub fn status_log_interval() -> StdDuration {
    StdDuration::from_secs(60)
}


// ============================================================================
// WEBSOCKET AND CONNECTION TIMEOUTS
// ============================================================================

/// HTTP request timeout for API calls
pub fn http_timeout() -> StdDuration {
    StdDuration::from_secs(20)
}

/// TCP keepalive duration for connection persistence
pub fn tcp_keepalive() -> StdDuration {
    StdDuration::from_secs(30)
}

// ============================================================================
// RISK MANAGEMENT THRESHOLDS
// ============================================================================

/// Minimum volume (USDC) required to trade a market.
/// Lowered to 0 to catch new hourly sessions as they launch.
pub const MIN_MARKET_VOLUME: f64 = 0.0;

/// Maximum exposure allowed per individual token (in USDC) — global fallback
pub const MAX_EXPOSURE_PER_TOKEN_USDC: Decimal = dec!(25);

// ── Per-strategy capital budgets ─────────────────────────────────────────────
// Each strategy has its own independent exposure ceiling.  The risk engine uses
// these instead of the global cap so each strategy can run its full book without
// being blocked by another strategy's open positions.

/// Maximum on-risk USDC for MomentumStrategy positions
pub const MOMENTUM_MAX_EXPOSURE_USDC: Decimal = dec!(25);

/// Maximum on-risk USDC for MakerStrategy positions
pub const MAKER_MAX_EXPOSURE_USDC: Decimal = dec!(15);

/// Maximum on-risk USDC for ArbitrageStrategy positions (per leg; total = 2×)
pub const ARBITRAGE_MAX_EXPOSURE_USDC: Decimal = dec!(50);

/// Maximum on-risk USDC for TimeDecayStrategy positions (per leg; total = 2×)
pub const TIME_DECAY_MAX_EXPOSURE_USDC: Decimal = dec!(50);

/// Session drawdown limit: 1% of collateral with $5 minimum
pub fn max_session_drawdown(collateral: Decimal) -> Decimal {
    (collateral * dec!(0.01)).max(dec!(5.00))
}


// ============================================================================
// API ENDPOINTS AND URLS
// ============================================================================

pub const CLOB_API_BASE: &str = "https://clob.polymarket.com";


// ============================================================================
// MARKET FILTERING CRITERIA
// ============================================================================

/// Number of pages to scan for markets in the Gamma API.
/// 30 pages = 3000 markets (sorted by 24hr volume descending).
/// Crypto hourly markets have enough volume to appear within the first 3000.
pub const GAMMA_API_MARKET_SCAN_PAGES: usize = 30;

/// Blocked market name keywords (politics, long-term events, etc.)
pub fn is_bad_market(name: &str) -> bool {
    let n = name.to_lowercase();
    n.contains("presidential") || n.contains("nomination") || n.contains("election") ||
        n.contains("democratic") || n.contains("republican") ||
        n.contains("masters") || n.contains("tournament") || n.contains("spieth") || n.contains("jordan") ||
        n.contains("5-minute") || n.contains("5 minute") || n.contains("5m")
}

/// Long-term 2026 markets (typically too illiquid for short-term trading)
pub fn is_long_term_2026(name: &str) -> bool {
    let n = name.to_lowercase();
    n.contains("2026") && (n.contains("win the") || n.contains("finals") || n.contains("cup") || n.contains("stanley"))
}

/// Detect price-range / price-band markets (e.g. "Will BTC be between $72,000 and $74,000 on April 12?")
/// These are NegRisk markets that are often already decided and unsuitable for directional strategies.
pub fn is_range_market(name: &str) -> bool {
    let n = name.to_lowercase();
    // "between $X and $Y" pattern
    (n.contains("between") && n.contains("and $")) ||
    // "price of X be between" pattern
    (n.contains("price of") && n.contains("between")) ||
    // "will ... be above/below $X" single-sided range
    (n.contains("will") && (n.contains("above $") || n.contains("below $")) && !n.contains("up or down"))
}

/// Hourly crypto markets (high-priority short-term session)
pub fn is_hourly_crypto_market(name: &str) -> bool {
    let n = name.to_lowercase();
    is_crypto_market(&n) && (
        n.contains("up or down") ||
            n.contains("hour") ||
            n.contains("et") ||
            n.contains("pm et") ||
            n.contains("am et")
    )
}

/// High-priority market text patterns (very short time windows)
pub fn is_high_priority_text(s: &str) -> bool {
    let n = s.to_lowercase();
    n.contains("up or down") ||
        n.contains("5 minutes") ||
        n.contains("5m") ||
        n.contains("updown") ||
        n.contains("next 5") ||
        n.contains("next hour")
}

/// Crypto market detection by coin name
pub fn is_crypto_market(name: &str) -> bool {
    let n = name.to_lowercase();
    n.contains("btc") || n.contains("bitcoin") ||
        n.contains("eth") || n.contains("ethereum") ||
        n.contains("sol") || n.contains("solana")
}

/// Ultra-short time window market detection (15-minute or specific time ranges)
pub fn is_ultra_short_window_market(name: &str) -> bool {
    let n = name.to_lowercase();

    if n.contains("15 minutes") || n.contains("15 minute") {
        return true;
    }

    let has_et = n.contains(" et");
    let has_range = n.contains('-');
    let has_time_marker = n.contains("am") || n.contains("pm");
    let has_minutes = n.contains(':');

    has_et && has_range && has_time_marker && has_minutes
}


// ============================================================================
// SLEEP AND RETRY DURATIONS
// ============================================================================

/// Sleep duration before retrying failed API calls
pub fn retry_sleep_duration() -> StdDuration {
    StdDuration::from_secs(5)
}

/// Initial sleep on application startup (allows connectors to initialize)
pub fn startup_delay() -> StdDuration {
    StdDuration::from_secs(10)
}

/// Connector initialization delay (allows connection to establish)
pub fn connector_init_delay() -> StdDuration {
    StdDuration::from_secs(10)
}

/// Order execution delay (prevents rate limiting)
pub fn order_execution_delay() -> StdDuration {
    StdDuration::from_millis(300)
}