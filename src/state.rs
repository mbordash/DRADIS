/// Shared state types for the orchestrator and strategies.
/// Defines clear ownership boundaries and data structures used across the system.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use crate::venues::core::TimeInForce;

use crate::venues::core::MarketId;

// ─── Phantom-fill / orphan tracking aliases ───────────────────────────────────
// Venue-neutral shared maps used by the balance/orphan handlers and SessionState.
// Defined here (not in the intl-gated `helpers::balance`) so venue-neutral code
// can reference them under any active venue.
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// Cooldown map keyed by an opaque fingerprint string → expiry `Instant`.
pub type PhantomCooldowns = Arc<Mutex<HashMap<String, Instant>>>;
/// Set of market ids that have been flattened/abandoned and must not be re-hedged.
pub type OrphanTombstones = Arc<Mutex<HashSet<MarketId>>>;
/// Set of token ids whose market the arbitrage viper has already committed a pair
/// to this session. Once locked, no second arb pair is opened on that market — the
/// viper holds the single pair to settlement instead of churning re-entries.
/// Session-scoped (never cleared on market rotation); each new daily/window market
/// has fresh tokens, so the next market trades normally.
pub type ArbMarketLockouts = Arc<Mutex<HashSet<MarketId>>>;

// ─── WebSocket price feed ─────────────────────────────────────────────────────

/// Live orderbook price snapshot from the Polymarket WebSocket.
///
/// Tuple layout: `(best_bid, bid_depth, best_ask, ask_depth, ws_update_timestamp)`
///
/// Previously a `type` alias private to `main.rs`.  Promoted here in Phase 3f-2
/// so `Squadron::subscribe_markets()` and the tick loop share a single definition.
pub type PriceState = (Decimal, Decimal, Decimal, Decimal, DateTime<Utc>);

/// Represents a single position held in the trading system.
/// Shared across strategies and the main orchestrator.
#[derive(Debug, Clone)]
pub struct Position {
    /// Amount of shares held
    pub shares: Decimal,
    /// Average entry price
    pub avg_entry: Decimal,
    /// When the position was opened
    pub opened_at: DateTime<Utc>,
    /// When the position was closed (if applicable)
    pub close_time: Option<DateTime<Utc>>,
    /// Human-readable market name
    pub market_name: String,
    /// Token ID for this position (venue-neutral canonical key — slice 2a)
    pub pair_token_id: MarketId,
    /// When the position balance was confirmed on-chain
    pub fill_confirmed_at: Option<DateTime<Utc>>,
    /// For paired strategies (Arbitrage, TimeDecay): token ID of the complementary leg.
    /// If Some, this position is part of a hedged pair. Used to detect orphaned positions
    /// when the paired leg fails to fill.
    pub paired_leg_token_id: Option<MarketId>,
}

/// Compound key for the shared position map: (strategy_name, token_id).
/// Each strategy has its own position slot per token, enabling fully independent
/// capital allocation and eliminating cross-strategy entry conflicts (Option A).
///
/// Slice 2a: the token component is the venue-neutral [`MarketId`] (decimal-`U256`
/// string for intl) rather than a raw `U256`, so the canonical position key is
/// venue-agnostic.
pub type PositionKey = (String, MarketId);

/// Shared positions state accessible by all strategies.
/// Keyed by (strategy_name, token_id) so that MomentumStrategy and MakerStrategy
/// can both hold YES simultaneously without colliding.
/// Typically wrapped in Arc<Mutex<>> for concurrent access.
pub type PositionMap = HashMap<PositionKey, Position>;

/// Current market data snapshot.
/// Used for broadcasting to strategies.
#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    /// YES token best bid price
    pub yes_bid: Decimal,
    /// YES token bid-side depth (shares available at best bid)
    pub yes_bid_depth: Decimal,
    /// YES token best ask price
    pub yes_ask: Decimal,
    /// YES token ask-side depth (shares available at best ask)
    pub yes_ask_depth: Decimal,
    /// NO token best bid price
    pub no_bid: Decimal,
    /// NO token bid-side depth (shares available at best bid)
    pub no_bid_depth: Decimal,
    /// NO token best ask price
    pub no_ask: Decimal,
    /// NO token ask-side depth (shares available at best ask)
    pub no_ask_depth: Decimal,
    /// Current oracle price from Binance
    pub oracle_price: Decimal,
    /// Price velocity over the primary window (MOMENTUM_WINDOW_SECS = 5s)
    pub velocity: Decimal,
    /// Price velocity over the short window (1s) — confirms move is still happening NOW
    pub velocity_1s: Decimal,
    /// Velocity rate-of-change: velocity_now - velocity_prev_tick
    /// Positive = momentum building, negative = momentum fading
    pub acceleration: Decimal,
    /// Binance perpetual futures funding rate (from /fapi/v1/premiumIndex).
    /// Negative = shorts paying longs (bearish bias from smart money).
    /// Positive = longs paying shorts (bullish bias from smart money).
    /// Updated every ~60 seconds; zero if unavailable.
    pub funding_rate: Decimal,
    /// Institutional Pulse from the Tide Raptor — volume-weighted z-score of the
    /// spot-BTC-ETF (IBIT/FBTC/ARKB) premium/discount vs a synthetic iNAV.
    /// Positive = institutions paying a premium (bullish), negative = discount (bearish).
    /// BTC-only and US-market-hours-only: zero for ETH/SOL squadrons, outside US
    /// session, or when the Tide Raptor is not deployed.
    pub institutional_pulse: Decimal,
    /// Tide coherence in [0, 1] — agreement across the three ETF premiums.
    /// High coherence + large |pulse| = institutional conviction. Zero when the
    /// Tide Raptor is absent/dormant (same gating as `institutional_pulse`).
    pub tide_coherence: Decimal,
    /// Fractional change in Binance perp open interest since the previous poll
    /// (Derivatives Raptor). >0 = positioning building, <0 = unwinding/de-leveraging.
    /// Zero when the Derivatives Raptor is absent or on its first poll. All-asset.
    pub oi_delta_pct: Decimal,
    /// Taker buy÷sell volume ratio from the Derivatives Raptor. >1 = buyers lifting
    /// offers (bullish aggression), <1 = sellers hitting bids (bearish aggression),
    /// 0 = no data (FAPI unreachable). Treated as neutral when zero.
    pub cvd_ratio: Decimal,
    /// 60-minute oracle price drift (current_price − price_60_minutes_ago).
    /// Positive = BTC trending UP over the last hour.
    /// Negative = BTC trending DOWN over the last hour.
    /// Zero when insufficient history is available (first hour of bot runtime).
    /// Used by MakerStrategy to suppress adverse-side bids during slow sustained trends.
    pub oracle_drift_60m: Decimal,
    /// 10-minute oracle price drift (current_price − price_10_minutes_ago).
    /// Fills the temporal gap between the 5s velocity window and the 60m drift.
    /// Captures the medium-term directional move where profitable binary trades develop.
    /// Zero when fewer than 10 minutes of price history are available.
    pub oracle_drift_10m: Decimal,
    /// Canonical 60-minute realized volatility of the oracle (Binance) price, from the
    /// Price raptor's `normalized_hist_vol` over a proper time-spaced 60-min window.
    /// Normalized to [0, 1] where 1.0 = 2%-per-tick log-return std-dev. ~0.0025–0.0035
    /// for normal BTC; ~0.0 when the oracle is frozen. Consumed by GBoost's flatness
    /// gate (and its `hist_vol_regime` feature) instead of recomputing from the
    /// 50ms-cadence snapshot buffer, which oversamples duplicate prints and collapses
    /// to 0. Zero until the raptor has ≥5 samples.
    pub hist_vol: Decimal,
    /// Seconds remaining until this market's expiry at the time of snapshot creation.
    /// Negative if market has already expired.  Zero when close_time is unknown.
    /// Used by GBoost as a direct feature: binary market microstructure changes
    /// dramatically near expiry (gamma explosion, spread widening, adverse selection)
    /// and the model should learn these dynamics from data rather than via hard-coded gates.
    pub secs_to_expiry: i64,
    /// Timestamp of this snapshot
    pub timestamp: DateTime<Utc>,
}

/// Market identifiers and metadata.
#[derive(Debug, Clone)]
pub struct MarketConfig {
    /// YES token ID (venue-neutral)
    pub yes_token: MarketId,
    /// NO token ID (venue-neutral)
    pub no_token: MarketId,
    /// Human-readable market name
    pub market_name: String,
    /// Market close/expiry time
    pub market_close_time: Option<DateTime<Utc>>,
    /// Strike price (if applicable)
    pub strike_price: Option<Decimal>,
    /// Whether the market uses negative risk pricing
    pub is_neg_risk: bool,
    /// Polymarket condition ID (bytes32 hex) — required for on-chain merge operations.
    /// Empty string when not available (non-maker markets).
    pub condition_id: String,
    /// YES token fee rate in basis points
    pub yes_fee_bps: u32,
    /// NO token fee rate in basis points
    pub no_fee_bps: u32,
}

/// Lifecycle phase of a market derived from its close time.
///
/// **Venue-neutral core**: both the intl patrol and the US loop drive the same
/// close/wind-down/stand-down semantics off this single classifier, so neither
/// venue re-implements "is the market closing?" logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketPhase {
    /// Trading normally — opening new positions is allowed.
    Open,
    /// Inside the wind-down window — stop opening new positions and let existing
    /// ones resolve/exit (the squadron's RTB state).
    WindingDown,
    /// At or past close — stand down and rotate to the next market.
    Closed,
}

impl MarketConfig {
    /// Seconds until the market closes (negative if already past). `None` when
    /// the market has no close time (e.g. always-open markets that never rotate).
    pub fn secs_to_close(&self, now: DateTime<Utc>) -> Option<i64> {
        self.market_close_time.map(|c| (c - now).num_seconds())
    }

    /// Classify the market's lifecycle [`MarketPhase`]. `rtb_window_secs` is how
    /// long before close to stop opening new positions. Markets with no close
    /// time are always [`MarketPhase::Open`].
    pub fn phase(&self, now: DateTime<Utc>, rtb_window_secs: i64) -> MarketPhase {
        match self.secs_to_close(now) {
            None                            => MarketPhase::Open,
            Some(s) if s <= 0               => MarketPhase::Closed,
            Some(s) if s <= rtb_window_secs => MarketPhase::WindingDown,
            Some(_)                         => MarketPhase::Open,
        }
    }
}

/// Strategy execution status for monitoring and lifecycle management.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyStatus {
    /// Strategy is active and evaluating
    Active,
    /// Strategy is disabled (e.g., no signal, cooldown)
    Disabled,
    /// Strategy encountered an error
    Error,
}

/// Parameters required to place an order on the CLOB.
#[derive(Debug, Clone)]
pub struct OrderParams {
    pub token_id: MarketId,
    pub price: Decimal,
    pub shares: Decimal,
    pub fee_bps: u16,
    pub is_neg_risk: bool,
    pub market_name: String,
    pub condition_id: String,
    pub order_type: TimeInForce,
    pub post_only: bool,
    pub ghost_mode: bool, // Added this field
}

/// Signals returned by strategies for the orchestrator to act upon.
#[derive(Debug, Clone)]
pub enum StrategySignal {
    /// Entry signal with all metadata. For paired strategies, this is the primary leg.
    Entry {
        params: OrderParams,
        /// If Some, the strategy also wants to buy this second leg (Arbitrage/TimeDecay).
        pair_params: Option<OrderParams>,
    },
    /// Two-sided maker quote with metadata.
    MakerQuote {
        yes: Option<OrderParams>,
        no: Option<OrderParams>,
    },
    /// Cancel a resting (unfilled) maker quote before adverse fill.
    /// Used when the book turns toxic (OBI) while the GTC bid is still open.
    /// Removes the phantom position and cancels the live order; arms re-entry cooldown.
    MakerCancel {
        token_id: MarketId,
        reason: String,
    },
    /// Exit signal with metadata.
    Exit {
        params: OrderParams,
        reason: String,
        /// If true, also exit the other leg of a paired position.
        exit_pair: bool,
    },
    /// No action at this time
    NoSignal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    fn market_closing_in(secs: Option<i64>) -> MarketConfig {
        MarketConfig {
            yes_token: MarketId::new("yes"),
            no_token: MarketId::new("no"),
            market_name: "t".into(),
            market_close_time: secs.map(|s| Utc::now() + ChronoDuration::seconds(s)),
            strike_price: None,
            is_neg_risk: false,
            condition_id: String::new(),
            yes_fee_bps: 0,
            no_fee_bps: 0,
        }
    }

    #[test]
    fn phase_classifies_open_winddown_closed() {
        let now = Utc::now();
        // No close time → always Open.
        assert_eq!(market_closing_in(None).phase(now, 120), MarketPhase::Open);
        // Plenty of time → Open.
        assert_eq!(market_closing_in(Some(600)).phase(now, 120), MarketPhase::Open);
        // Inside the wind-down window → WindingDown.
        assert_eq!(market_closing_in(Some(60)).phase(now, 120), MarketPhase::WindingDown);
        // Past close → Closed.
        assert_eq!(market_closing_in(Some(-5)).phase(now, 120), MarketPhase::Closed);
    }
}
