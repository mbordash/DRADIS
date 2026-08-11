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

//! Kalshi WebSocket layer — orderbook snapshot/delta engine + fill feed.
//!
//! Protocol (`wss://…/trade-api/ws/v2`, RSA-PSS-signed handshake):
//! - Subscribe: `{"id":N,"cmd":"subscribe","params":{"channels":[…],"market_tickers":[…]}}`
//! - `orderbook_snapshot` arrives first per market, then `orderbook_delta`
//!   increments; both carry `seq` — a gap means we lost a frame and must
//!   rebuild (we reconnect, which re-delivers snapshots).
//! - The book is BIDS-ONLY (yes bids + no bids); the yes ask is derived as
//!   `1 − best_no_bid`.
//! - Kalshi pings every 10s; tungstenite auto-pongs on read, and we also
//!   answer explicitly since the stream is split.
//! - `fill` channel pushes our matches → venue-neutral [`FillEvent`]s.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::venues::core::{FillEvent, MarketId, OrderId, Side};

use super::auth::KalshiAuth;
use super::types::fp;

const RECONNECT_DELAY_SECS: u64 = 5;
const WS_SIGN_PATH: &str = "/trade-api/ws/v2";

// ─── Local orderbook ─────────────────────────────────────────────────────────

/// One market's book: price → resting quantity, bids only, per Kalshi side.
#[derive(Debug, Default, Clone)]
pub struct Book {
    pub yes_bids: BTreeMap<Decimal, Decimal>,
    pub no_bids: BTreeMap<Decimal, Decimal>,
    /// Set once the first snapshot has arrived.
    pub ready: bool,
}

impl Book {
    pub fn best_yes_bid(&self) -> Option<(Decimal, Decimal)> {
        self.yes_bids.iter().next_back().map(|(p, q)| (*p, *q))
    }

    pub fn best_no_bid(&self) -> Option<(Decimal, Decimal)> {
        self.no_bids.iter().next_back().map(|(p, q)| (*p, *q))
    }

    /// Best price to buy YES right now = 1 − best NO bid.
    pub fn best_yes_ask(&self) -> Option<(Decimal, Decimal)> {
        self.best_no_bid().map(|(p, q)| (Decimal::ONE - p, q))
    }

    /// Best price to buy NO right now = 1 − best YES bid.
    pub fn best_no_ask(&self) -> Option<(Decimal, Decimal)> {
        self.best_yes_bid().map(|(p, q)| (Decimal::ONE - p, q))
    }

    fn apply_delta(&mut self, side: &str, price: Decimal, delta: Decimal) {
        let levels = if side == "yes" { &mut self.yes_bids } else { &mut self.no_bids };
        let q = levels.entry(price).or_insert(Decimal::ZERO);
        *q += delta;
        if *q <= Decimal::ZERO {
            levels.remove(&price);
        }
    }
}

/// Shared live books, keyed by market ticker.
pub type BookHub = Arc<RwLock<HashMap<String, Book>>>;

/// Fresh, empty hub.
pub fn new_book_hub() -> BookHub {
    Arc::new(RwLock::new(HashMap::new()))
}

// ─── Handshake ───────────────────────────────────────────────────────────────

fn authed_request(
    ws_url: &str,
    auth: &KalshiAuth,
) -> anyhow::Result<tokio_tungstenite::tungstenite::handshake::client::Request> {
    let mut request = ws_url.into_client_request()?;
    for (k, v) in auth.signed_headers("GET", WS_SIGN_PATH) {
        request.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::HeaderName::from_bytes(k.as_bytes())?,
            v.parse()?,
        );
    }
    Ok(request)
}

async fn wait_or_cancel(cancel: &CancellationToken, secs: u64) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(std::time::Duration::from_secs(secs)) => false,
    }
}

// ─── Market data feed (orderbook_delta) ──────────────────────────────────────

/// Spawn the auto-reconnecting orderbook feed for `tickers`, maintaining `hub`.
///
/// A `seq` gap forces a reconnect: Kalshi re-sends full snapshots on
/// re-subscribe, which is the simplest correct rebuild path.
pub fn spawn_orderbook_feed(
    ws_url: String,
    auth: Arc<KalshiAuth>,
    tickers: Vec<String>,
    hub: BookHub,
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
                    warn!("⚠️ Kalshi WS request build failed: {e}. Retrying in {RECONNECT_DELAY_SECS}s…");
                    if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await { return; }
                    continue;
                }
            };
            let stream = match tokio_tungstenite::connect_async(request).await {
                Ok((s, _)) => s,
                Err(e) => {
                    warn!("⚠️ Kalshi WS connect failed: {e}. Retrying in {RECONNECT_DELAY_SECS}s…");
                    if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await { return; }
                    continue;
                }
            };
            let (mut write, mut read) = stream.split();

            // Books must be rebuilt from the fresh snapshots on every (re)connect.
            // Global seq counter — Kalshi sequences span all subscribed markets
            // on a single WS connection, so gap detection must be connection-wide.
            let mut last_seq: u64 = 0;
            {
                let mut books = hub.write().await;
                for t in &tickers {
                    books.insert(t.clone(), Book::default());
                }
            }

            let sub = serde_json::json!({
                "id": 1,
                "cmd": "subscribe",
                "params": { "channels": ["orderbook_delta"], "market_tickers": tickers }
            });
            if let Err(e) = write.send(Message::Text(sub.to_string().into())).await {
                warn!("⚠️ Kalshi WS subscribe send failed: {e}. Reconnecting…");
                if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await { return; }
                continue;
            }
            info!("✅ Kalshi WS orderbook feed subscribed ({} market(s))", tickers.len());

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    msg = read.next() => {
                        let msg = match msg {
                            Some(Ok(m))  => m,
                            Some(Err(e)) => { warn!("⚠️ Kalshi WS stream error: {e}. Restarting…"); break; }
                            None         => { warn!("⚠️ Kalshi WS closed. Restarting…"); break; }
                        };
                        let text = match msg {
                            Message::Text(t)   => t.to_string(),
                            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                            Message::Ping(p)   => { let _ = write.send(Message::Pong(p)).await; continue; }
                            Message::Pong(_) | Message::Frame(_) => continue,
                            Message::Close(_)  => { warn!("⚠️ Kalshi WS close frame. Restarting…"); break; }
                        };
                        let v: serde_json::Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if !apply_ws_message(&hub, &v, &mut last_seq).await {
                            warn!("⚠️ Kalshi WS seq gap detected. Reconnecting for fresh snapshots…");
                            break;
                        }
                    }
                }
            }
            if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await { return; }
        }
    });
}

/// Apply one parsed WS frame to the hub. Returns `false` on a seq gap
/// (caller must resync by reconnecting).
///
/// `last_seq` is the **connection-global** sequence counter. Kalshi sequences
/// span all subscribed markets on one WS connection, so gap detection must
/// be connection-wide — not per-book.
async fn apply_ws_message(hub: &BookHub, v: &serde_json::Value, last_seq: &mut u64) -> bool {
    let msg_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");

    // Global seq check — applies to both snapshots and deltas.
    let seq = v.get("seq").and_then(|s| s.as_u64()).unwrap_or(0);
    if seq != 0 && *last_seq != 0 && seq != *last_seq + 1 {
        return false; // gap
    }
    if seq != 0 {
        *last_seq = seq;
    }

    match msg_type {
        "orderbook_snapshot" => {
            let Some(msg) = v.get("msg") else { return true };
            let Some(ticker) = msg.get("market_ticker").and_then(|t| t.as_str()) else { return true };
            let mut book = Book { ready: true, ..Default::default() };
            for (field, side) in [("yes_dollars_fp", "yes"), ("no_dollars_fp", "no")] {
                if let Some(levels) = msg.get(field).and_then(|l| l.as_array()) {
                    for lvl in levels {
                        if let (Some(p), Some(q)) = (
                            lvl.get(0).and_then(|x| x.as_str()).and_then(fp),
                            lvl.get(1).and_then(|x| x.as_str()).and_then(fp),
                        ) {
                            book.apply_delta(side, p, q);
                        }
                    }
                }
            }
            debug!("📸 Kalshi snapshot {ticker}: {} yes / {} no levels (seq {seq})",
                book.yes_bids.len(), book.no_bids.len());
            hub.write().await.insert(ticker.to_string(), book);
            true
        }
        "orderbook_delta" => {
            let Some(msg) = v.get("msg") else { return true };
            let Some(ticker) = msg.get("market_ticker").and_then(|t| t.as_str()) else { return true };
            let side = msg.get("side").and_then(|s| s.as_str()).unwrap_or("yes");
            let (Some(price), Some(delta)) = (
                msg.get("price_dollars").and_then(|p| p.as_str()).and_then(fp),
                msg.get("delta_fp").and_then(|d| d.as_str()).and_then(fp),
            ) else { return true };

            let mut books = hub.write().await;
            let Some(book) = books.get_mut(ticker) else { return true };
            if !book.ready {
                return true; // delta before snapshot — ignore, snapshot will come
            }
            book.apply_delta(side, price, delta);
            true
        }
        "error" => {
            warn!("⚠️ Kalshi WS error frame: {v}");
            true
        }
        _ => true,
    }
}

// ─── Fill feed ───────────────────────────────────────────────────────────────

/// Spawn the auto-reconnecting private `fill` feed.
///
/// Kalshi fills are per-trade counts, not cumulative — we accumulate per
/// order id locally so [`FillEvent::filled`] is cumulative as the shared
/// lifecycle expects. `complete` is left `false`; the reconcile poll backstop
/// closes orders (idempotent confirmation, same contract as the US venue).
pub fn spawn_fill_feed(
    ws_url: String,
    auth: Arc<KalshiAuth>,
    tx: tokio::sync::broadcast::Sender<FillEvent>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let mut cum: HashMap<String, Decimal> = HashMap::new();
        loop {
            if cancel.is_cancelled() {
                return;
            }

            let request = match authed_request(&ws_url, &auth) {
                Ok(r) => r,
                Err(e) => {
                    warn!("⚠️ Kalshi fill WS request build failed: {e}. Retrying…");
                    if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await { return; }
                    continue;
                }
            };
            let stream = match tokio_tungstenite::connect_async(request).await {
                Ok((s, _)) => s,
                Err(e) => {
                    warn!("⚠️ Kalshi fill WS connect failed: {e}. Retrying…");
                    if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await { return; }
                    continue;
                }
            };
            let (mut write, mut read) = stream.split();

            let sub = serde_json::json!({
                "id": 1, "cmd": "subscribe", "params": { "channels": ["fill"] }
            });
            if let Err(e) = write.send(Message::Text(sub.to_string().into())).await {
                warn!("⚠️ Kalshi fill WS subscribe failed: {e}. Reconnecting…");
                if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await { return; }
                continue;
            }
            info!("✅ Kalshi WS fill feed subscribed");

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    msg = read.next() => {
                        let msg = match msg {
                            Some(Ok(m))  => m,
                            Some(Err(e)) => { warn!("⚠️ Kalshi fill WS error: {e}. Restarting…"); break; }
                            None         => { warn!("⚠️ Kalshi fill WS closed. Restarting…"); break; }
                        };
                        let text = match msg {
                            Message::Text(t)   => t.to_string(),
                            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                            Message::Ping(p)   => { let _ = write.send(Message::Pong(p)).await; continue; }
                            Message::Pong(_) | Message::Frame(_) => continue,
                            Message::Close(_)  => { warn!("⚠️ Kalshi fill WS close frame. Restarting…"); break; }
                        };
                        let v: serde_json::Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if let Some(fill) = fill_msg_to_event(&v, &mut cum) {
                            debug!("📥 Kalshi fill: {} filled={} @ {}", fill.market, fill.filled, fill.price);
                            let _ = tx.send(fill);
                        }
                    }
                }
            }
            if wait_or_cancel(&cancel, RECONNECT_DELAY_SECS).await { return; }
        }
    });
}

/// Map a `fill` frame to a cumulative venue-neutral [`FillEvent`].
fn fill_msg_to_event(
    v: &serde_json::Value,
    cum: &mut HashMap<String, Decimal>,
) -> Option<FillEvent> {
    if v.get("type").and_then(|t| t.as_str()) != Some("fill") {
        return None;
    }
    let msg = v.get("msg")?;
    let order_id = msg.get("order_id")?.as_str()?.to_string();
    let ticker = msg.get("market_ticker")?.as_str()?.to_string();
    let count = msg.get("count_fp").and_then(|c| c.as_str()).and_then(fp)?;
    if count <= Decimal::ZERO {
        return None;
    }
    // The leg we ended up holding: purchased_side when present, else side.
    let leg = msg
        .get("purchased_side")
        .or_else(|| msg.get("side"))
        .and_then(|s| s.as_str())
        .unwrap_or("yes");
    let yes_price = msg
        .get("yes_price_dollars")
        .and_then(|p| p.as_str())
        .and_then(fp)
        .unwrap_or_default();
    let price = if leg == "yes" { yes_price } else { Decimal::ONE - yes_price };
    let action = msg.get("action").and_then(|a| a.as_str()).unwrap_or("buy");

    let total = cum.entry(order_id.clone()).or_insert(Decimal::ZERO);
    *total += count;

    Some(FillEvent {
        order_id: OrderId(order_id),
        market: MarketId::new(super::leg_id(&ticker, leg == "yes")),
        side: if action == "sell" { Side::Sell } else { Side::Buy },
        filled: *total,
        price,
        complete: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn snapshot_frame() -> serde_json::Value {
        serde_json::json!({
            "type": "orderbook_snapshot", "sid": 2, "seq": 2,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [["0.0800", "300.00"], ["0.2200", "333.00"]],
                "no_dollars_fp":  [["0.5400", "20.00"],  ["0.5600", "146.00"]]
            }
        })
    }

    #[tokio::test]
    async fn snapshot_then_delta_maintains_book() {
        let hub = new_book_hub();
        let mut seq = 0u64;
        assert!(apply_ws_message(&hub, &snapshot_frame(), &mut seq).await);
        assert_eq!(seq, 2);
        {
            let books = hub.read().await;
            let b = books.get("KXBTC15M-TEST").unwrap();
            assert_eq!(b.best_yes_bid(), Some((dec!(0.22), dec!(333))));
            assert_eq!(b.best_no_bid(), Some((dec!(0.56), dec!(146))));
            // yes ask = 1 − best no bid
            assert_eq!(b.best_yes_ask(), Some((dec!(0.44), dec!(146))));
        }

        // Delta: eat 300 off the 0.22 yes level → 33 left, still best.
        let delta = serde_json::json!({
            "type": "orderbook_delta", "sid": 2, "seq": 3,
            "msg": { "market_ticker": "KXBTC15M-TEST", "price_dollars": "0.2200",
                     "delta_fp": "-300.00", "side": "yes" }
        });
        assert!(apply_ws_message(&hub, &delta, &mut seq).await);
        // Remove the rest → 0.08 becomes best yes bid.
        let delta2 = serde_json::json!({
            "type": "orderbook_delta", "sid": 2, "seq": 4,
            "msg": { "market_ticker": "KXBTC15M-TEST", "price_dollars": "0.2200",
                     "delta_fp": "-33.00", "side": "yes" }
        });
        assert!(apply_ws_message(&hub, &delta2, &mut seq).await);
        let books = hub.read().await;
        let b = books.get("KXBTC15M-TEST").unwrap();
        assert_eq!(b.best_yes_bid(), Some((dec!(0.08), dec!(300))));
        assert_eq!(seq, 4);
    }

    #[tokio::test]
    async fn seq_gap_detected() {
        let hub = new_book_hub();
        let mut seq = 0u64;
        assert!(apply_ws_message(&hub, &snapshot_frame(), &mut seq).await);
        let gap = serde_json::json!({
            "type": "orderbook_delta", "sid": 2, "seq": 9, // gap: expected 3
            "msg": { "market_ticker": "KXBTC15M-TEST", "price_dollars": "0.0800",
                     "delta_fp": "-1.00", "side": "yes" }
        });
        assert!(!apply_ws_message(&hub, &gap, &mut seq).await);
    }

    #[tokio::test]
    async fn delta_for_unknown_market_ignored() {
        let hub = new_book_hub();
        let mut seq = 0u64;
        let delta = serde_json::json!({
            "type": "orderbook_delta", "sid": 2, "seq": 5,
            "msg": { "market_ticker": "NOPE", "price_dollars": "0.5000",
                     "delta_fp": "10.00", "side": "no" }
        });
        assert!(apply_ws_message(&hub, &delta, &mut seq).await); // no gap signal, just ignored
    }

    #[test]
    fn fill_events_accumulate_per_order() {
        let mut cum = HashMap::new();
        let frame = |count: &str| {
            serde_json::json!({
                "type": "fill", "sid": 13,
                "msg": {
                    "trade_id": "t-1", "order_id": "o-1",
                    "market_ticker": "KXBTC15M-TEST",
                    "side": "yes", "purchased_side": "yes", "action": "buy",
                    "yes_price_dollars": "0.7500", "count_fp": count,
                    "post_position_fp": "500.00"
                }
            })
        };
        let f1 = fill_msg_to_event(&frame("100.00"), &mut cum).unwrap();
        assert_eq!(f1.filled, dec!(100));
        assert_eq!(f1.price, dec!(0.75));
        assert_eq!(f1.market.as_str(), "KXBTC15M-TEST#yes");
        assert_eq!(f1.side, Side::Buy);
        let f2 = fill_msg_to_event(&frame("50.00"), &mut cum).unwrap();
        assert_eq!(f2.filled, dec!(150)); // cumulative

        // NO-leg fill: price converts to the NO side.
        let no_frame = serde_json::json!({
            "type": "fill", "sid": 13,
            "msg": { "order_id": "o-2", "market_ticker": "KXBTC15M-TEST",
                     "purchased_side": "no", "action": "buy",
                     "yes_price_dollars": "0.7500", "count_fp": "10.00" }
        });
        let f3 = fill_msg_to_event(&no_frame, &mut cum).unwrap();
        assert_eq!(f3.price, dec!(0.25));
        assert_eq!(f3.market.as_str(), "KXBTC15M-TEST#no");
    }

    #[test]
    fn non_fill_frames_ignored() {
        let mut cum = HashMap::new();
        assert!(fill_msg_to_event(&serde_json::json!({"type":"subscribed"}), &mut cum).is_none());
        assert!(fill_msg_to_event(&serde_json::json!({"type":"fill","msg":{"order_id":"o","market_ticker":"T","count_fp":"0.00"}}), &mut cum).is_none());
    }
}

// Live WS smoke test vs demo.kalshi.co — run with:
// cargo test --features kalshi kalshi_demo_ws_smoke -- --ignored --nocapture
#[cfg(test)]
#[tokio::test]
#[ignore]
async fn kalshi_demo_ws_smoke() {
    dotenv::dotenv().ok();
    let auth = Arc::new(KalshiAuth::from_env().expect("auth"));
    let venue = super::KalshiVenue::from_env().expect("venue");
    let mkts = venue.markets_for_series("KXBTC15M").await.expect("markets");
    let tickers: Vec<String> = mkts.iter().map(|m| m.ticker.clone()).collect();
    println!("subscribing to {tickers:?}");
    let hub = new_book_hub();
    let cancel = CancellationToken::new();
    spawn_orderbook_feed(super::ws_url(), auth, tickers.clone(), hub.clone(), cancel.clone());
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    let books = hub.read().await;
    let mut ready = 0;
    for t in &tickers {
        if let Some(b) = books.get(t) {
            println!("{t}: ready={} yes_bid={:?} yes_ask={:?}",
                b.ready, b.best_yes_bid(), b.best_yes_ask());
            if b.ready { ready += 1; }
        }
    }
    cancel.cancel();
    assert!(ready > 0, "no snapshots arrived");
    let _ = venue; // silence unused when no markets
}
