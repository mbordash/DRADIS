/// Price Raptor — Binance Spot WebSocket price feed.
///
/// Connects to the Binance `<symbol>@ticker` stream for the configured crypto pair
/// and broadcasts the following signals via `watch` channels:
///
/// │ Channel        │ Type                           │ Description                        │
/// │────────────────│────────────────────────────────│────────────────────────────────────│
/// │ oracle_tx      │ Decimal                        │ Current spot price                 │
/// │ velocity_tx    │ (Decimal, Decimal, Decimal)    │ (5s velocity, 1s velocity, accel)  │
/// │ drift_tx       │ (Decimal, Decimal)             │ (60-min drift, 10-min drift)       │
///
/// Reconnects automatically on:
///   • Disconnect or WS error
///   • 30s with no message at all (dead TCP / half-open socket)
///   • 60s with no *price tick* — catches "zombie" connections where Binance
///     keepalive pings reset the 30s timer but ticker text has silently stopped
///
/// Consumers should treat a `dec!(0)` oracle price as "not yet connected".
use std::str::FromStr;
use std::collections::HashMap;

use futures::StreamExt as _;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tokio::time::{Duration, Instant, timeout as tokio_timeout};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, warn};
use std::collections::VecDeque;
use std::sync::Arc;

use crate::config;
use crate::api::server::AssetRaptorHealth;

pub async fn run_price_raptor(
    crypto_filter: String,
    oracle_tx: watch::Sender<Decimal>,
    velocity_tx: watch::Sender<(Decimal, Decimal, Decimal)>,
    // Sends (drift_60m, drift_10m) — both raw USD Decimal values.
    // drift_10m fills the 5s–60m temporal gap for GBoost feature [18].
    drift_tx: watch::Sender<(Decimal, Decimal)>,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
) {
    let binance_pair = match crypto_filter.as_str() {
        "eth" => "ethusdt",
        "sol" => "solusdt",
        _     => "btcusdt",
    };
    let url_str = format!("wss://stream.binance.com:9443/ws/{}@ticker", binance_pair);
    let mut price_history: VecDeque<(Instant, Decimal)> = VecDeque::new();
    let mut price_history_60m: VecDeque<(Instant, Decimal)> = VecDeque::new();
    let mut price_history_10m: VecDeque<(Instant, Decimal)> = VecDeque::new();
    let mut prev_velocity = dec!(0);

    loop {
        if let Ok((mut ws_stream, _)) = connect_async(&url_str).await {
            info!(" Price Raptor connected to Binance for {}", binance_pair.to_uppercase());
            // Mark price raptor as healthy for this asset.
            raptor_health_tx.send_modify(|map| {
                map.entry(crypto_filter.clone()).or_default().price_connected = true;
            });
            // last_price_tick tracks when we last received an actual ticker text
            // message with a valid price.  Binance sends periodic WS ping frames
            // that reset the 30s tokio_timeout below but carry no price data.  A
            // "zombie" connection — alive at the TCP level but delivering no ticker
            // updates — would otherwise be invisible forever.  If 60s elapse with
            // no real price tick we force a reconnect regardless of ping activity.
            let mut last_price_tick = Instant::now();

            'ws: loop {
                // Staleness guard: independent of WS keepalive pings.
                if last_price_tick.elapsed() >= Duration::from_secs(60) {
                    warn!("⚠️ Price Raptor: no price tick in 60s (zombie WS — pings alive but ticker silent) — reconnecting");
                    break 'ws;
                }

                match tokio_timeout(Duration::from_secs(30), ws_stream.next()).await {
                    Ok(Some(Ok(msg))) => {
                        if let Message::Text(text) = msg {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                if let Some(price_str) = v.get("c").and_then(|p| p.as_str()) {
                                    if let Ok(price) = Decimal::from_str(price_str) {
                                        let now = Instant::now();
                                        last_price_tick = now; // reset staleness clock
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

                                        // 10-minute drift — fills the 5s–60m gap for GBoost feature [18].
                                        // Captures the medium-term trend where profitable binary moves develop.
                                        //
                                        // Previously returned dec!(0) unless exactly 10 minutes of history
                                        // were available.  Fixed: if at least 60 seconds of data exists,
                                        // return the drift over whatever window IS available.  This ensures
                                        // the momentum 10m-drift gate is active from the second minute
                                        // rather than silent for the entire first 10 minutes of a session.
                                        price_history_10m.push_back((now, price));
                                        while let Some((t, _)) = price_history_10m.front() {
                                            if now.duration_since(*t).as_secs() > 600 {
                                                price_history_10m.pop_front();
                                            } else { break; }
                                        }
                                        let drift_10m = if let Some((oldest_t, oldest_p)) = price_history_10m.front() {
                                            let window_secs = now.duration_since(*oldest_t).as_secs();
                                            // Require at least 60s of history before trusting the drift.
                                            // Below that, the window is too short to distinguish noise from trend.
                                            if window_secs >= 60 {
                                                price - oldest_p
                                            } else { dec!(0) }
                                        } else { dec!(0) };

                                        let _ = drift_tx.send((drift_60m, drift_10m));
                                    }
                                }
                            }
                        }
                    }
                    // Stream closed cleanly or returned an error — reconnect.
                    Ok(None) | Ok(Some(Err(_))) => break 'ws,
                    // 30s elapsed with no message — silent stall; force reconnect.
                    Err(_) => {
                        warn!("⚠️ Price Raptor: no tick in 30s — reconnecting");
                        break 'ws;
                    }
                }
            }
        }
        warn!("⚠️ Price Raptor disconnected. Reconnecting in 5s...");
        // Mark price raptor as offline while reconnecting.
        raptor_health_tx.send_modify(|map| {
            map.entry(crypto_filter.clone()).or_default().price_connected = false;
        });
        prev_velocity = dec!(0);
        price_history_10m.clear();
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
