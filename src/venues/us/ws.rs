//! US retail market-data WebSocket feed (`/v1/ws/markets`).
//!
//! The US gateway forbids polling for execution logic (spec §4), so live order
//! books arrive over a streaming socket. This module spawns one auto-reconnecting
//! subscriber per instrument symbol and pushes venue-neutral [`PriceState`]
//! `(best_bid, bid_depth, best_ask, ask_depth, ts)` snapshots into a
//! `watch::Sender` — the exact shape the intl venue's `spawn_ws_task` produces, so
//! the (future) US patrol loop reads prices identically regardless of venue.
//!
//! Guardrails (spec §5):
//!   * **Sequence tracking** — `sequence_number` must advance by exactly 1; a gap
//!     means dropped frames, so the book is resynced (reconnect).
//!   * **Timestamp rejection** — frames older than [`STALE_FRAME_MS`] are dropped
//!     so a stalled socket can't feed strategies a stale book.

use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;

use chrono::Utc;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use tungstenite::http::Uri;
use tungstenite::ClientRequestBuilder;

use crate::state::PriceState;
use crate::venues::us::auth::UsAuth;

/// Drop order-book frames whose exchange timestamp lags wall-clock by more than
/// this, shielding strategies from a stalled stream (spec §5 "Timestamp Rejection").
pub const STALE_FRAME_MS: i64 = 200;

/// Market-data WS path appended to the venue's WS base URL.
const MARKETS_WS_PATH: &str = "/v1/ws/markets";
/// Reconnect backoff after a socket error / sequence gap.
const RECONNECT_DELAY_SECS: u64 = 5;

#[derive(Serialize)]
struct SubscribeFrame<'a> {
    action: &'a str,
    channels: Vec<&'a str>,
    symbols: Vec<&'a str>,
}

/// One `order_book` event (spec §4.1). `bids`/`asks` are `[price, size]` string
/// pairs; `timestamp` is epoch-millis.
#[derive(Debug, Clone, Deserialize)]
struct OrderBookEvent {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    sequence_number: u64,
    #[serde(default)]
    bids: Vec<[String; 2]>,
    #[serde(default)]
    asks: Vec<[String; 2]>,
    #[serde(default)]
    timestamp: i64,
}

/// Derive `wss://…` market-data URL from the venue's `https://…` REST base.
pub fn ws_url_from_base(base_url: &str) -> String {
    let host = base_url
        .strip_prefix("https://")
        .or_else(|| base_url.strip_prefix("http://"))
        .unwrap_or(base_url);
    let scheme = if base_url.starts_with("http://") { "ws://" } else { "wss://" };
    format!("{scheme}{host}{MARKETS_WS_PATH}")
}

/// Best bid/ask reducer: highest bid price, lowest ask price, with their depths.
fn book_to_price(ev: &OrderBookEvent) -> Option<PriceState> {
    let parse = |lvl: &[String; 2]| -> Option<(Decimal, Decimal)> {
        Some((Decimal::from_str(&lvl[0]).ok()?, Decimal::from_str(&lvl[1]).ok()?))
    };

    let best_bid = ev
        .bids
        .iter()
        .filter_map(parse)
        .max_by(|a, b| a.0.cmp(&b.0));
    let best_ask = ev
        .asks
        .iter()
        .filter_map(parse)
        .min_by(|a, b| a.0.cmp(&b.0));

    // A usable book needs at least one side; missing sides default to the
    // "no liquidity" sentinels the intl feed uses (bid 0 / ask 1).
    let (bid, bid_depth) = best_bid.unwrap_or((Decimal::ZERO, Decimal::ZERO));
    let (ask, ask_depth) = best_ask.unwrap_or((Decimal::ONE, Decimal::ZERO));
    if best_bid.is_none() && best_ask.is_none() {
        return None;
    }
    Some((bid, bid_depth, ask, ask_depth, Utc::now()))
}

/// True if an epoch-millis frame timestamp lags wall-clock beyond the staleness
/// budget. A non-positive timestamp (absent) is treated as fresh — some frames
/// omit it and we don't want to discard an otherwise-valid book.
fn is_stale(frame_ts_ms: i64, now_ms: i64) -> bool {
    frame_ts_ms > 0 && now_ms.saturating_sub(frame_ts_ms) > STALE_FRAME_MS
}

/// Spawn one auto-reconnecting `order_book` subscriber for `symbol`.
///
/// Pushes `PriceState` updates into `tx`; stops cleanly when `cancel` fires.
/// `ws_url` is the full `wss://…/v1/ws/markets` endpoint (see [`ws_url_from_base`]).
///
/// The US gateway rejects an unauthenticated WS upgrade with `401`, so `auth`
/// signs the handshake with the same `X-PM-*` headers used for REST. Headers are
/// re-signed on every (re)connect so the timestamp stays inside the replay window.
pub fn spawn_market_feed(
    ws_url: String,
    symbol: String,
    auth: Arc<UsAuth>,
    tx: watch::Sender<PriceState>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            if cancel.is_cancelled() {
                return;
            }

            let request = match authed_request(&ws_url, &auth) {
                Ok(r) => r,
                Err(e) => {
                    warn!("⚠️ US WS request build failed for {symbol}: {e}. Retrying in {RECONNECT_DELAY_SECS}s…");
                    if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await {
                        return;
                    }
                    continue;
                }
            };

            let stream = match tokio_tungstenite::connect_async(request).await {
                Ok((s, _)) => s,
                Err(e) => {
                    warn!("⚠️ US WS connect failed for {symbol}: {e}. Retrying in {RECONNECT_DELAY_SECS}s…");
                    if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await {
                        return;
                    }
                    continue;
                }
            };
            let (mut write, mut read) = stream.split();

            // Subscribe to this symbol's order book.
            let frame = SubscribeFrame {
                action: "subscribe",
                channels: vec!["order_book"],
                symbols: vec![&symbol],
            };
            let sub = match serde_json::to_string(&frame) {
                Ok(s) => s,
                Err(e) => {
                    warn!("⚠️ US WS subscribe encode failed for {symbol}: {e}");
                    return;
                }
            };
            if let Err(e) = write.send(Message::Text(sub.into())).await {
                warn!("⚠️ US WS subscribe send failed for {symbol}: {e}. Reconnecting…");
                if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await {
                    return;
                }
                continue;
            }
            info!("✅ US WS order_book subscribed for {symbol}");

            let mut last_seq: Option<u64> = None;
            let mut resync = false;

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    msg = read.next() => {
                        let msg = match msg {
                            Some(Ok(m))  => m,
                            Some(Err(e)) => { warn!("⚠️ US WS stream error for {symbol}: {e}. Restarting…"); break; }
                            None         => { warn!("⚠️ US WS closed for {symbol}. Restarting…"); break; }
                        };
                        let text = match msg {
                            Message::Text(t)   => t.to_string(),
                            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
                            Message::Close(_)  => { warn!("⚠️ US WS close frame for {symbol}. Restarting…"); break; }
                        };

                        let ev: OrderBookEvent = match serde_json::from_str(&text) {
                            Ok(e)  => e,
                            Err(_) => continue, // non-orderbook control/ack frame
                        };
                        if ev.channel != "order_book" || ev.symbol != symbol {
                            continue;
                        }

                        // Sequence-gap guard: a skipped number means we lost frames;
                        // the local book is unreliable, so resync via reconnect.
                        if let Some(prev) = last_seq {
                            if ev.sequence_number != prev + 1 {
                                warn!("⚠️ US WS sequence gap for {symbol}: {prev} → {} — resyncing", ev.sequence_number);
                                resync = true;
                                break;
                            }
                        }
                        last_seq = Some(ev.sequence_number);

                        // Staleness guard.
                        if is_stale(ev.timestamp, Utc::now().timestamp_millis()) {
                            debug!("US WS dropped stale frame for {symbol} (ts={})", ev.timestamp);
                            continue;
                        }

                        if let Some(price) = book_to_price(&ev) {
                            let _ = tx.send(price);
                        }
                    }
                }
            }

            // Brief pause before reconnect (immediate on a deliberate resync).
            if !resync && wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await {
                return;
            }
        }
    });
}

/// Sleep `secs` unless cancelled. Returns `true` if cancelled (caller should stop).
async fn wait_or_cancel(cancel: &CancellationToken, secs: u64) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(std::time::Duration::from_secs(secs)) => false,
    }
}

/// Build a WS handshake request carrying freshly-signed `X-PM-*` auth headers.
///
/// The signature covers `GET` + the WS path (`/v1/ws/markets`), matching the
/// REST signing scheme so the gateway accepts the upgrade. Re-signing per call
/// keeps the timestamp inside the gateway's replay window across reconnects.
fn authed_request(ws_url: &str, auth: &UsAuth) -> anyhow::Result<ClientRequestBuilder> {
    let uri: Uri = ws_url.parse()?;
    let mut builder = ClientRequestBuilder::new(uri);
    for (name, value) in auth.signed_headers("GET", MARKETS_WS_PATH) {
        builder = builder.with_header(name, value);
    }
    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(bids: &[[&str; 2]], asks: &[[&str; 2]], seq: u64, ts: i64) -> OrderBookEvent {
        OrderBookEvent {
            channel: "order_book".to_string(),
            symbol: "sym".to_string(),
            sequence_number: seq,
            bids: bids.iter().map(|b| [b[0].to_string(), b[1].to_string()]).collect(),
            asks: asks.iter().map(|a| [a[0].to_string(), a[1].to_string()]).collect(),
            timestamp: ts,
        }
    }

    #[test]
    fn ws_url_derives_wss_from_https() {
        assert_eq!(
            ws_url_from_base("https://api.prod.polymarketexchange.com"),
            "wss://api.prod.polymarketexchange.com/v1/ws/markets"
        );
        assert_eq!(
            ws_url_from_base("http://localhost:8080"),
            "ws://localhost:8080/v1/ws/markets"
        );
    }

    #[test]
    fn book_reduces_to_best_bid_ask() {
        let e = ev(&[["0.54", "12000"], ["0.53", "45000"]], &[["0.57", "19500"], ["0.56", "8000"]], 1, 0);
        let (bid, bid_d, ask, ask_d, _) = book_to_price(&e).unwrap();
        assert_eq!(bid.to_string(), "0.54");
        assert_eq!(bid_d.to_string(), "12000");
        assert_eq!(ask.to_string(), "0.56");
        assert_eq!(ask_d.to_string(), "8000");
    }

    #[test]
    fn empty_book_yields_none() {
        assert!(book_to_price(&ev(&[], &[], 1, 0)).is_none());
    }

    #[test]
    fn one_sided_book_uses_sentinel_for_missing_side() {
        let bid_only = book_to_price(&ev(&[["0.40", "10"]], &[], 1, 0)).unwrap();
        assert_eq!(bid_only.2.to_string(), "1"); // ask sentinel
        let ask_only = book_to_price(&ev(&[], &[["0.60", "10"]], 1, 0)).unwrap();
        assert_eq!(ask_only.0.to_string(), "0"); // bid sentinel
    }

    #[test]
    fn staleness_threshold() {
        let now = 1_000_000;
        assert!(!is_stale(now, now)); // fresh
        assert!(!is_stale(now - STALE_FRAME_MS, now)); // exactly at budget
        assert!(is_stale(now - STALE_FRAME_MS - 1, now)); // just over
        assert!(!is_stale(0, now)); // absent timestamp treated as fresh
    }
}

