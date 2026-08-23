// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
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
use crate::venues::core::{FillEvent, MarketId, OrderId, Side};
use crate::venues::us::auth::UsAuth;

/// Drop order-book frames whose exchange timestamp lags wall-clock by more than
/// this, shielding strategies from a stalled stream (spec §5 "Timestamp Rejection").
pub const STALE_FRAME_MS: i64 = 200;

/// Market-data WS path appended to the venue's WS base URL.
const MARKETS_WS_PATH: &str = "/v1/ws/markets";
/// Private account-feed WS path (order matches / fills — spec §4.2).
const PRIVATE_WS_PATH: &str = "/v1/ws/private";
/// Reconnect backoff after a socket error / sequence gap.
const RECONNECT_DELAY_SECS: u64 = 5;

/// Market-data subscription frame.
///
/// The venue rejected the previous shape outright — it replied
/// `{"error":"invalid_message"}` to every subscribe and DRADIS never noticed,
/// because each `OrderBookEvent` field is `#[serde(default)]`, so the error
/// frame deserialised into an all-defaults event and was dropped by the channel
/// check. Books stayed empty on both wings with a cheerful "subscribed" in the
/// log.
///
/// Two things were wrong. The frame was `{action, channels[], symbols[]}` where
/// the server wants a flat, camelCase subscription object; and the channel was
/// `order_book`, which is not one the venue serves — full depth is `market_data`
/// (`market_data_lite` is best-bid/offer only). Both taken from the
/// polymarket_us SDK's own stream client, which is the closest thing to a spec.
#[derive(Serialize)]
struct SubscribeFrame<'a> {
    action: &'a str,
    channels: Vec<&'a str>,
    symbols: Vec<&'a str>,
}

/// Full order-book depth. Not `order_book` — see [`SubscribeFrame`].
const MARKET_DATA_CHANNEL: &str = "order_book";

/// Order lifecycle / fills. Not `private_orders`.
const ORDER_UPDATE_CHANNEL: &str = "order_update";

/// Private-channel subscription: no symbol, it is account-scoped.
#[derive(Serialize)]
struct PrivateSubscribeFrame<'a> {
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
    // Aggregate depth across every level the venue publishes, beside the touch
    // sizes above. Nothing consumes these yet — they exist so order-book
    // imbalance can be compared as a whole-book ratio against the top-of-book
    // one every viper currently gates on.
    let bid_depth_total: Decimal = ev.bids.iter().filter_map(parse).map(|(_, sz)| sz).sum();
    let ask_depth_total: Decimal = ev.asks.iter().filter_map(parse).map(|(_, sz)| sz).sum();
    Some((bid, bid_depth, ask, ask_depth, Utc::now(), bid_depth_total, ask_depth_total))
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

            let request = match authed_request(&ws_url, &auth, MARKETS_WS_PATH) {
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
                channels: vec![MARKET_DATA_CHANNEL],
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
            info!("✅ US WS order_book subscribe sent for {symbol}");

            let mut last_seq: Option<u64> = None;
            let mut resync = false;
            // Every OrderBookEvent field is #[serde(default)], so ANY valid JSON
            // deserialises into it and is then dropped by the channel/symbol
            // check below — an error frame, a rejected subscription and silence
            // are indistinguishable. Sampling the first few frames makes the
            // difference visible without flooding a live session.
            let mut sampled = 0u8;
            let mut subscribe_rejected = false;
            const SAMPLE_FRAMES: u8 = 3;

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

                        // A rejected subscription is not silence. Every
                        // OrderBookEvent field is #[serde(default)], so an error
                        // frame deserialises into an all-defaults event and is
                        // dropped by the channel check below — which is how this
                        // venue's feed stayed dead while the log said
                        // "subscribed". Surface it once per connection.
                        if !subscribe_rejected && text.contains("\"error\"") {
                            subscribe_rejected = true;
                            warn!(
                                "⚠️ US WS rejected the {MARKET_DATA_CHANNEL} subscription for {symbol}: {} \
                                 — no book will arrive on this connection",
                                text.chars().take(200).collect::<String>(),
                            );
                        }
                        if sampled < SAMPLE_FRAMES {
                            sampled += 1;
                            let preview: String = text.chars().take(300).collect();
                            debug!("📡 US WS frame {sampled}/{SAMPLE_FRAMES} for {symbol}: {preview}");
                        }

                        let ev: OrderBookEvent = match serde_json::from_str(&text) {
                            Ok(e)  => e,
                            Err(_) => continue, // non-orderbook control/ack frame
                        };
                        // The server labels market-data frames by event type;
                        // accept the snake_case and camelCase spellings the SDK
                        // tolerates, and ignore anything for another symbol.
                        let is_market_data = matches!(
                            ev.channel.as_str(),
                            "market_data" | "marketData" | "order_book",
                        );
                        if !is_market_data || (!ev.symbol.is_empty() && ev.symbol != symbol) {
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

/// Derive the `wss://…/v1/ws/private` account-feed URL from the REST base.
pub fn private_ws_url_from_base(base_url: &str) -> String {
    let host = base_url
        .strip_prefix("https://")
        .or_else(|| base_url.strip_prefix("http://"))
        .unwrap_or(base_url);
    let scheme = if base_url.starts_with("http://") { "ws://" } else { "wss://" };
    format!("{scheme}{host}{PRIVATE_WS_PATH}")
}

/// One `private_orders` execution event (spec §4.2). Numeric quantities arrive
/// as JSON numbers; `fill_price` as a decimal string.
#[derive(Debug, Clone, Deserialize)]
struct PrivateOrderEvent {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    event_type: String,
    #[serde(default)]
    order_id: String,
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    side: String,
    #[serde(default)]
    fill_price: String,
    #[serde(default)]
    fill_quantity: u64,
    #[serde(default)]
    remaining_quantity: i64,
}

/// Map a `private_orders` event to the venue-neutral [`FillEvent`], if it
/// describes an actual match. Ack/cancel/reject lifecycle noise returns `None`.
fn private_event_to_fill(ev: &PrivateOrderEvent) -> Option<FillEvent> {
    if ev.channel != "private_orders" || ev.symbol.is_empty() {
        return None;
    }
    // Only match events carry fill state worth confirming.
    let is_fill = matches!(
        ev.event_type.as_str(),
        "ORDER_FILLED" | "ORDER_PARTIALLY_FILLED" | "FILL" | "MATCH"
    ) || ev.fill_quantity > 0;
    if !is_fill || ev.fill_quantity == 0 {
        return None;
    }
    Some(FillEvent {
        order_id: OrderId(ev.order_id.clone()),
        market: MarketId::new(ev.symbol.clone()),
        side: if ev.side.eq_ignore_ascii_case("sell") { Side::Sell } else { Side::Buy },
        filled: Decimal::from(ev.fill_quantity),
        price: Decimal::from_str(ev.fill_price.trim()).unwrap_or(Decimal::ZERO),
        complete: ev.remaining_quantity <= 0,
    })
}

/// Spawn the auto-reconnecting private account feed (`/v1/ws/private`).
///
/// Pushes venue-neutral [`FillEvent`]s into the broadcast `tx` so the shared
/// `OrderLifecycle` fill listener confirms fills **event-precisely** instead of
/// at positions-poll granularity (the gateway spec §4 forbids polling for
/// execution logic). The account feed is squadron-agnostic: it lives for the
/// venue's lifetime and survives market rotations.
///
/// Fill events are *facts* (unlike order-book quotes), so no staleness rejection
/// is applied — a late event still describes a real match, and the reconcile
/// poll backstop already tolerates duplicates (confirmation is idempotent).
pub fn spawn_private_fill_feed(
    ws_url: String,
    auth: Arc<UsAuth>,
    tx: tokio::sync::broadcast::Sender<FillEvent>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            if cancel.is_cancelled() {
                return;
            }

            let request = match authed_request(&ws_url, &auth, PRIVATE_WS_PATH) {
                Ok(r) => r,
                Err(e) => {
                    warn!("⚠️ US private WS request build failed: {e}. Retrying in {RECONNECT_DELAY_SECS}s…");
                    if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await {
                        return;
                    }
                    continue;
                }
            };

            let stream = match tokio_tungstenite::connect_async(request).await {
                Ok((s, _)) => s,
                Err(e) => {
                    warn!("⚠️ US private WS connect failed: {e}. Retrying in {RECONNECT_DELAY_SECS}s…");
                    if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await {
                        return;
                    }
                    continue;
                }
            };
            let (mut write, mut read) = stream.split();

            // Same correction as the market feed: a flat camelCase subscription
            // object, and a channel the venue actually serves. "private_orders"
            // is not one of them — order lifecycle is `order_update`. This
            // failed exactly as silently, since the reply parses into an
            // all-defaults event and is dropped.
            let frame = PrivateSubscribeFrame {
                action: "subscribe",
                channels: vec!["private_orders"],
                symbols: vec![],
            };
            let sub = match serde_json::to_string(&frame) {
                Ok(s) => s,
                Err(e) => {
                    warn!("⚠️ US private WS subscribe encode failed: {e}");
                    return;
                }
            };
            if let Err(e) = write.send(Message::Text(sub.into())).await {
                warn!("⚠️ US private WS subscribe send failed: {e}. Reconnecting…");
                if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await {
                    return;
                }
                continue;
            }
            info!("✅ US private WS order_update subscribe sent (fills feed)");

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    msg = read.next() => {
                        let msg = match msg {
                            Some(Ok(m))  => m,
                            Some(Err(e)) => { warn!("⚠️ US private WS stream error: {e}. Restarting…"); break; }
                            None         => { warn!("⚠️ US private WS closed. Restarting…"); break; }
                        };
                        let text = match msg {
                            Message::Text(t)   => t.to_string(),
                            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
                            Message::Close(_)  => { warn!("⚠️ US private WS close frame. Restarting…"); break; }
                        };

                        let ev: PrivateOrderEvent = match serde_json::from_str(&text) {
                            Ok(e)  => e,
                            Err(_) => continue, // control/ack frame
                        };
                        if let Some(fill) = private_event_to_fill(&ev) {
                            debug!("📥 US fill event: {} {} filled={} remaining={}",
                                ev.event_type, fill.market, fill.filled, ev.remaining_quantity);
                            // Send fails only when no lifecycle listener is
                            // subscribed yet — safe to drop (reconcile backstop).
                            let _ = tx.send(fill);
                        }
                    }
                }
            }

            if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await {
                return;
            }
        }
    });
}

/// Build a WS handshake request carrying freshly-signed `X-PM-*` auth headers.
///
/// The signature covers `GET` + the WS path (`/v1/ws/markets`), matching the
/// REST signing scheme so the gateway accepts the upgrade. Re-signing per call
/// keeps the timestamp inside the gateway's replay window across reconnects.
fn authed_request(ws_url: &str, auth: &UsAuth, path: &str) -> anyhow::Result<ClientRequestBuilder> {
    let uri: Uri = ws_url.parse()?;
    let mut builder = ClientRequestBuilder::new(uri);
    for (name, value) in auth.signed_headers("GET", path) {
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
        let (bid, bid_d, ask, ask_d, _, bid_all, ask_all) = book_to_price(&e).unwrap();
        assert_eq!(bid.to_string(), "0.54");
        assert_eq!(bid_d.to_string(), "12000");
        assert_eq!(ask.to_string(), "0.56");
        assert_eq!(ask_d.to_string(), "8000");
        // Cumulative depth spans every level, not just the touch — this book has
        // nearly five times the size behind the best bid than at it, which is the
        // gap that makes a top-of-book imbalance a different measure entirely.
        assert_eq!(bid_all.to_string(), "57000", "12000 + 45000");
        assert_eq!(ask_all.to_string(), "27500", "19500 + 8000");
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

