/// Background task: Binance oracle WebSocket price feed.
///
/// Connects to the Binance ticker stream for the configured crypto pair and
/// broadcasts oracle price, velocity (5s + 1s), acceleration, and 60-minute
/// drift via watch channels.  Reconnects automatically on disconnect.
use std::str::FromStr;

use futures::StreamExt as _;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, warn};
use std::collections::VecDeque;

use crate::config;

pub async fn run_oracle(
    crypto_filter: String,
    oracle_tx: watch::Sender<Decimal>,
    velocity_tx: watch::Sender<(Decimal, Decimal, Decimal)>,
    drift_60m_tx: watch::Sender<Decimal>,
) {
    let binance_pair = match crypto_filter.as_str() {
        "eth" => "ethusdt",
        "sol" => "solusdt",
        _     => "btcusdt",
    };
    let url_str = format!("wss://stream.binance.com:9443/ws/{}@ticker", binance_pair);
    let mut price_history: VecDeque<(Instant, Decimal)> = VecDeque::new();
    let mut price_history_60m: VecDeque<(Instant, Decimal)> = VecDeque::new();
    let mut prev_velocity = dec!(0);

    loop {
        if let Ok((mut ws_stream, _)) = connect_async(&url_str).await {
            info!("📡 Connected to Binance Oracle for {}", binance_pair.to_uppercase());
            while let Some(Ok(msg)) = ws_stream.next().await {
                if let Message::Text(text) = msg {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                        if let Some(price_str) = v.get("c").and_then(|p| p.as_str()) {
                            if let Ok(price) = Decimal::from_str(price_str) {
                                let now = Instant::now();
                                let _ = oracle_tx.send(price);
                                price_history.push_back((now, price));

                                // Trim entries older than the primary window (5s)
                                while let Some((t, _)) = price_history.front() {
                                    if now.duration_since(*t).as_secs() >= config::MOMENTUM_WINDOW_SECS {
                                        price_history.pop_front();
                                    } else { break; }
                                }

                                // Primary velocity (5s window)
                                let velocity_5s = if let Some((_, start_price)) = price_history.front() {
                                    price - start_price
                                } else { dec!(0) };

                                // Short velocity (1s window)
                                let velocity_1s = {
                                    let cutoff = config::MOMENTUM_SHORT_WINDOW_SECS;
                                    let start_1s = price_history.iter()
                                        .find(|(t, _)| now.duration_since(*t).as_secs() < cutoff);
                                    match start_1s {
                                        Some((_, p)) => price - p,
                                        None => velocity_5s,
                                    }
                                };

                                // Acceleration: rate of change of velocity
                                let acceleration = velocity_5s - prev_velocity;
                                prev_velocity = velocity_5s;
                                let _ = velocity_tx.send((velocity_5s, velocity_1s, acceleration));

                                // 60-minute drift
                                price_history_60m.push_back((now, price));
                                while let Some((t, _)) = price_history_60m.front() {
                                    if now.duration_since(*t).as_secs() > 3600 {
                                        price_history_60m.pop_front();
                                    } else { break; }
                                }
                                let drift_60m = if price_history_60m.len() > 1 {
                                    if let Some((oldest_t, oldest_p)) = price_history_60m.front() {
                                        if now.duration_since(*oldest_t).as_secs() >= 3600 {
                                            price - oldest_p
                                        } else { dec!(0) }
                                    } else { dec!(0) }
                                } else { dec!(0) };
                                let _ = drift_60m_tx.send(drift_60m);
                            }
                        }
                    }
                }
            }
        }
        warn!("⚠️ Binance Oracle disconnected. Reconnecting in 5s...");
        prev_velocity = dec!(0);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
