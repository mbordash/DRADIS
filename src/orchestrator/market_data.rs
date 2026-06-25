/// Market data broadcaster: distributes real-time market data to all strategies.
///
/// Uses tokio watch channels to efficiently broadcast price updates to multiple
/// subscribers. Each strategy can subscribe to market data and will receive updates
/// as they occur.

use crate::state::{MarketSnapshot, MarketConfig};
use rust_decimal::Decimal;
use tokio::sync::watch;
use chrono::Utc;

/// Broadcaster that sends market snapshots to all subscribed strategies.
pub struct MarketDataBroadcaster {
    /// Watch channel for market data
    tx: watch::Sender<Option<MarketSnapshot>>,
}

impl MarketDataBroadcaster {
    /// Create a new market data broadcaster.
    pub fn new() -> Self {
        let (tx, _) = watch::channel(None);
        Self { tx }
    }

    /// Subscribe to market data updates.
    /// Returns a receiver that will get all future market snapshots.
    pub fn subscribe(&self) -> watch::Receiver<Option<MarketSnapshot>> {
        self.tx.subscribe()
    }

    /// Broadcast an updated market snapshot to all subscribers.
    pub fn broadcast(&self, snapshot: MarketSnapshot) {
        let _ = self.tx.send(Some(snapshot));
    }

    /// Build a market snapshot from raw price and oracle data.
    /// Helper method to simplify snapshot creation.
    pub fn create_snapshot(
        _market: &MarketConfig,
        yes_bid: Decimal,
        yes_bid_depth: Decimal,
        yes_ask: Decimal,
        yes_ask_depth: Decimal,
        no_bid: Decimal,
        no_bid_depth: Decimal,
        no_ask: Decimal,
        no_ask_depth: Decimal,
        oracle_price: Decimal,
        velocity: Decimal,
        velocity_1s: Decimal,
        acceleration: Decimal,
        funding_rate: Decimal,
        oracle_drift_60m: Decimal,
        oracle_drift_10m: Decimal,
        secs_to_expiry: i64,
    ) -> MarketSnapshot {
        MarketSnapshot {
            yes_bid,
            yes_bid_depth,
            yes_ask,
            yes_ask_depth,
            no_bid,
            no_bid_depth,
            no_ask,
            no_ask_depth,
            oracle_price,
            velocity,
            velocity_1s,
            acceleration,
            funding_rate,
            oracle_drift_60m,
            oracle_drift_10m,
            institutional_pulse: Decimal::ZERO,
            tide_coherence: Decimal::ZERO,
            oi_delta_pct: Decimal::ZERO,
            cvd_ratio: Decimal::ZERO,
            secs_to_expiry,
            timestamp: Utc::now(),
        }
    }
}

impl Default for MarketDataBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for MarketDataBroadcaster {
    /// Clone creates a new reference to the same underlying broadcaster.
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}


