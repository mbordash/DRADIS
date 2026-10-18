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

/// Price Raptor — Binance Spot WebSocket price feed.
///
/// Connects to the Binance `<symbol>@ticker` stream for the configured crypto pair
/// (documented update speed 1000ms, so about one sample per second) and
/// broadcasts the following signals via `watch` channels:
///
/// │ Channel        │ Type                           │ Description                        │
/// │────────────────│────────────────────────────────│────────────────────────────────────│
/// │ oracle_tx      │ Decimal                        │ Spot level, venue currency (0=unknown) │
/// │ velocity_tx    │ (Decimal, Decimal, Decimal)    │ (5s velocity, 1s velocity, accel)  │
/// │ drift_tx       │ (Decimal, Decimal, Decimal)    │ (60m drift, 10m drift, hist_vol)   │
///
/// Reconnects automatically on:
///   • Disconnect or WS error
///   • 30s with no message at all (dead TCP / half-open socket)
///   • 60s with no *price tick* — catches "zombie" connections where Binance
///     keepalive pings reset the 30s timer but ticker text has silently stopped
///
/// Consumers should treat a `dec!(0)` oracle price as "not yet connected".
///
/// The oracle LEVEL is published in the currency the venue settles in
/// ([`MarkDenomination`]); the rates (velocity, acceleration, drift, realized
/// vol) are always differences over the raw Binance series, where a stablecoin
/// basis cancels and its tick noise would otherwise be added to a one-second
/// velocity.
use std::str::FromStr;
use std::collections::HashMap;

use futures::StreamExt as _;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tokio::time::{Duration, Instant, timeout as tokio_timeout};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, warn};
use std::collections::VecDeque;
use std::sync::Arc;

use crate::config;
use crate::api::server::AssetRaptorHealth;
use crate::helpers::volatility::{normalized_hist_vol, range_pct};

/// The prior sample nearest `window_ms` ago, as `(age_ms, price)`: chosen among
/// the samples older than the tick just pushed (the newest entry), ties to the
/// older sample. `None` when that tick is the only sample.
///
/// Binance's `<symbol>@ticker` stream publishes once per 1000ms (its documented
/// update speed; production telemetry shows ~3,585 samples per hour), so a
/// window of `MOMENTUM_SHORT_WINDOW_SECS` = 1s holds, on most ticks, only the
/// tick that was just pushed. The rule this replaces — the oldest sample
/// younger than the window, compared in whole seconds — therefore selected the
/// current tick whenever the previous one was 1000ms or older and reported a 1s
/// velocity of exactly zero, and selected the previous tick only when WebSocket
/// jitter delivered it at 999ms or less. On 2026-09-21 Momentum's "1s velocity
/// not confirming" gate read 0.00 beside a 5s velocity of $233 and an
/// acceleration of $211 (05:26:23 ET), then $134 one second later: a lottery
/// on delivery jitter, not a measurement.
///
/// Nearest-to-window is stream-rate agnostic: with one tick per second it is
/// the previous tick, with a real-time trade stream it is the print nearest one
/// window ago. Compared in milliseconds so a 1001ms sample is one millisecond
/// off, not a second. Whether the anchor is close enough to stand for the
/// window is [`short_window_start`]'s decision.
pub fn short_window_anchor(
    history: &VecDeque<(Instant, Decimal)>,
    now: Instant,
    window_ms: u128,
) -> Option<(u128, Decimal)> {
    let n = history.len();
    if n < 2 {
        return None;
    }
    let mut best: Option<(u128, u128, Decimal)> = None;
    for (t, p) in history.iter().take(n - 1) {
        let age = now.saturating_duration_since(*t).as_millis();
        let off = age.abs_diff(window_ms);
        match best {
            Some((best_off, _, _)) if best_off <= off => {}
            _ => best = Some((off, age, *p)),
        }
    }
    best.map(|(_, age, p)| (age, p))
}

/// How far the anchor's age may sit from the window, either side, and still
/// stand for it: half the window (500ms for the 1s window).
///
/// Without a bound the nearest anchor is whatever the 5s history holds. In a
/// feed gap — samples at 4,900ms and now — that is a 4.9-second delta reported
/// as a one-second velocity, which confirms a move that may already be over,
/// and it does so precisely when the feed is degraded: the old rule vetoed
/// there (it read zero), so an unbounded anchor inverted the gate's behavior
/// in gaps. Half a window admits ordinary delivery jitter on the 1000ms
/// ticker and refuses a missed tick (2,000ms) outright; a real-time stream
/// always has a print inside it. Symmetric because a much younger anchor is
/// not a one-second measurement either — it understates, which only makes the
/// gate stricter, but "unconfirmed" is the honest reading for both.
pub const SHORT_WINDOW_ANCHOR_TOLERANCE_MS: u128 = 500;

/// The price the short-window velocity is measured from, or `None` when no
/// prior sample sits within [`SHORT_WINDOW_ANCHOR_TOLERANCE_MS`] of the window.
///
/// `None` means "no one-second measurement exists on this tick", and the
/// raptor then publishes a 1s velocity of zero: the value Momentum's
/// confirmation gate already treats as not confirming (`velocity_1s >=
/// short_min` with a positive `short_min`). DRADIS prefers honest idleness to
/// degraded activity; a gap in the feed is not evidence that the last second
/// carried the move. The raptor also says so in the log, throttled, so a gap
/// is visible rather than indistinguishable from a flat second.
pub fn short_window_start(
    history: &VecDeque<(Instant, Decimal)>,
    now: Instant,
    window_ms: u128,
    tolerance_ms: u128,
) -> Option<Decimal> {
    short_window_anchor(history, now, window_ms)
        .filter(|(age, _)| age.abs_diff(window_ms) <= tolerance_ms)
        .map(|(_, p)| p)
}

/// How often an unusable short-window anchor is written to the log.
const SHORT_WINDOW_GAP_LOG_SECS: u64 = 60;

/// The currency of the published oracle level.
///
/// Binance quotes BTC in USDT, and USDT is not a dollar: on 2026-10-18 it traded
/// at $0.9987, so BTC/USDT read about $100 above the dollar price of bitcoin.
/// A market that settles on a DOLLAR index (Kalshi's crypto series resolve on
/// CF Benchmarks' real-time indices, quoted in USD) was then compared against a
/// mark in the wrong currency: FairValue saw BTC/USDT $83 above an $81,000
/// strike with eleven minutes left, priced YES at 0.79 against a $0.42 ask, and
/// bought. Coinbase BTC-USD never crossed the strike; the market settled NO and
/// the "edge" was the USDT discount. The venue that spawns the raptor says
/// which currency its markets settle in; the raptor publishes the level in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkDenomination {
    /// The Binance quote asset as traded (BTC/USDT). Right for a market whose
    /// rules settle on that very pair: Polymarket's "Up or Down" markets resolve
    /// on the Binance BTC/USDT one-minute candle.
    QuoteAsset,
    /// US dollars: BTC/USDT divided by USDC/USDT from the same Binance feed.
    /// USDC is taken as the dollar; its own basis is a few basis points and the
    /// alternative is a second vendor for the spot level.
    Usd,
}

/// Longest a USDC/USDT quote may stand in for the current one. USDT's dollar
/// price moves in basis points per hour, so a quote this old is still the
/// right factor to within a dollar or two of BTC; past it the USD level is
/// unknown and the oracle says so (`dec!(0)`) rather than publish a stale
/// conversion as a fresh mark.
pub const BASIS_MAX_AGE_SECS: u64 = 900;

/// Sanity bounds on a USDC/USDT print. A quote outside this band is a bad tick
/// or a broken stablecoin, and either way not a conversion factor. This band
/// alone still admits a single print that would move the BTC level by a
/// thousand dollars; [`BASIS_MAX_JUMP`] is what stops that.
const USDC_USDT_SANE: (Decimal, Decimal) = (dec!(0.90), dec!(1.10));

/// Largest move a single print may make against the factor in force, as a
/// fraction (30 basis points, about $240 of BTC at $81,000). USDT's dollar
/// price moves in basis points per hour, so one print further away than this
/// is a bad tick and is refused. A real break in the peg is a run of such
/// prints: once they have been refused for [`BASIS_REGIME_CONFIRM_SECS`]
/// without a single print landing back inside the band, the move is taken as
/// real and the next print that agrees with the run (within this same
/// fraction of the run's first print) is accepted as the new regime. Until
/// then the factor in force stays published. The run survives a reconnect of
/// the feed, so the lag on a peg move is normally two minutes; a feed that is
/// down for the whole move is bounded by [`BASIS_MAX_AGE_SECS`], after which
/// the level is withheld and the first in-band print is accepted.
pub const BASIS_MAX_JUMP: Decimal = dec!(0.003);

/// How long prints must keep disagreeing with the factor in force before the
/// disagreement is believed (see [`BASIS_MAX_JUMP`]).
pub const BASIS_REGIME_CONFIRM_SECS: u64 = 120;

/// The current run of jump-refused in-band prints: when it began and the
/// dollars-per-USDT factor of its first print, which later prints must agree
/// with to confirm it.
pub type RefusedRun = Option<(Instant, Decimal)>;

/// Decide whether a USDC/USDT print replaces the factor in force. `Some(f)` is
/// the new dollars-per-USDT factor; `None` keeps what is there. `run` is the
/// current run of refused prints ([`RefusedRun`], `None` when the last print
/// was accepted).
pub fn accept_basis_print(
    usdc_usdt_last: Decimal,
    in_force: Option<(Instant, Decimal)>,
    run: RefusedRun,
    now: Instant,
    max_age: Duration,
) -> Option<Decimal> {
    let factor = usd_per_quote(usdc_usdt_last)?;
    match in_force {
        Some((at, prev)) if now.saturating_duration_since(at) <= max_age && prev > Decimal::ZERO => {
            let jump = ((factor - prev) / prev).abs();
            if jump <= BASIS_MAX_JUMP {
                return Some(factor);
            }
            let confirmed = run.is_some_and(|(since, first)| {
                now.saturating_duration_since(since) >= Duration::from_secs(BASIS_REGIME_CONFIRM_SECS)
                    && first > Decimal::ZERO
                    && ((factor - first) / first).abs() <= BASIS_MAX_JUMP
            });
            if confirmed { Some(factor) } else { None }
        }
        _ => Some(factor),
    }
}

/// The level to publish for one tick: the raw price for a quote-asset venue,
/// the dollar conversion ([`usd_mark`]) for a dollar venue. `None` means the
/// level is unknown this tick and EVERYTHING the raptor publishes for it must
/// read as "not connected" (level 0, rates 0): a zero level beside a live
/// velocity turns every `oracle_threshold(pct, oracle)` into zero, and
/// Momentum's gates then pass on any downward tick.
pub fn publish_level(
    denomination: MarkDenomination,
    price: Decimal,
    basis: Option<(Instant, Decimal)>,
    now: Instant,
    max_age: Duration,
) -> Option<Decimal> {
    match denomination {
        MarkDenomination::QuoteAsset => Some(price),
        MarkDenomination::Usd => usd_mark(price, basis, now, max_age),
    }
}

/// How often a withheld USD level is written to the log.
const BASIS_GAP_LOG_SECS: u64 = 60;

/// Dollars per one unit of the quote asset, from the last USDC/USDT print.
/// `None` for a print outside [`USDC_USDT_SANE`] or at zero.
pub fn usd_per_quote(usdc_usdt_last: Decimal) -> Option<Decimal> {
    let (lo, hi) = USDC_USDT_SANE;
    if usdc_usdt_last <= Decimal::ZERO || usdc_usdt_last < lo || usdc_usdt_last > hi {
        return None;
    }
    Some(dec!(1) / usdc_usdt_last)
}

/// The oracle level in dollars: the quote-asset price times the most recent
/// dollars-per-quote factor, provided that factor is no older than `max_age`.
/// `None` when no factor has arrived yet or the last one has gone stale; the
/// caller publishes "not connected" for that tick rather than a guess.
pub fn usd_mark(
    quote_price: Decimal,
    basis: Option<(Instant, Decimal)>,
    now: Instant,
    max_age: Duration,
) -> Option<Decimal> {
    let (at, factor) = basis?;
    if now.saturating_duration_since(at) > max_age {
        return None;
    }
    Some((quote_price * factor).round_dp(2))
}

/// USDC/USDT ticker feed: publishes `(when, dollars per USDT)` on every print
/// that passes [`usd_per_quote`]. Same host rotation and stall handling as the
/// price stream. A disconnect leaves the last factor in place with its own
/// timestamp; [`usd_mark`] ages it out, so nothing here ever clears it to a
/// value that could be mistaken for a fresh quote.
async fn run_stablecoin_basis_feed(basis_tx: watch::Sender<Option<(Instant, Decimal)>>) {
    // Returns when the raptor that owns the receiver is gone, so a restarted
    // raptor does not leave the old feed's socket open beside its new one.
    let ws_urls = [
        "wss://stream.binance.com:9443/ws/usdcusdt@ticker".to_string(),
        "wss://data-stream.binance.vision/ws/usdcusdt@ticker".to_string(),
    ];
    let mut ws_url_idx = 0usize;
    let mut announced = false;
    // The run of prints refused for jumping away from the factor in force
    // (`accept_basis_print`); kept across reconnects so a stall in the middle
    // of a peg move does not restart the confirmation clock. Cleared by any
    // accepted print.
    let mut refused_run: RefusedRun = None;
    loop {
        if basis_tx.is_closed() {
            info!(" Price Raptor: USDC/USDT basis feed closing — its raptor is gone");
            return;
        }
        let url_str = &ws_urls[ws_url_idx];
        let conn = tokio_timeout(Duration::from_secs(20), connect_async(url_str)).await;
        let mut connected = false;
        if let Ok(Ok((mut ws_stream, _))) = conn {
            connected = true;
            info!(" Price Raptor: USDC/USDT basis feed connected via {url_str}");
            let mut last_tick = Instant::now();
            'ws: loop {
                if basis_tx.is_closed() {
                    break 'ws;
                }
                if last_tick.elapsed() >= Duration::from_secs(60) {
                    warn!("⚠️ Price Raptor: no USDC/USDT tick in 60s — reconnecting the basis feed");
                    break 'ws;
                }
                match tokio_timeout(Duration::from_secs(30), ws_stream.next()).await {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else { continue };
                        let Some(last) = v.get("c").and_then(|p| p.as_str()).and_then(|s| Decimal::from_str(s).ok()) else { continue };
                        last_tick = Instant::now();
                        let in_force = *basis_tx.borrow();
                        match accept_basis_print(last, in_force, refused_run, last_tick, Duration::from_secs(BASIS_MAX_AGE_SECS)) {
                            Some(factor) => {
                                if !announced {
                                    announced = true;
                                    info!(" Price Raptor: USD mark active — 1 USDT = ${factor:.4} (USDC/USDT {last})");
                                } else if let Some((since, _)) = refused_run {
                                    info!(" Price Raptor: USDC/USDT regime change accepted after {}s of disagreeing prints — 1 USDT = ${factor:.4} (USDC/USDT {last})",
                                          last_tick.saturating_duration_since(since).as_secs());
                                }
                                refused_run = None;
                                let _ = basis_tx.send(Some((last_tick, factor)));
                            }
                            None => {
                                if let (Some(f), None) = (usd_per_quote(last), refused_run) {
                                    refused_run = Some((last_tick, f));
                                }
                                warn!(
                                    "⚠️ Price Raptor: refused USDC/USDT print {last} as a conversion factor (outside sane bounds, or more than {}% from the factor in force); keeping the previous one",
                                    BASIS_MAX_JUMP * dec!(100),
                                );
                            }
                        }
                    }
                    Ok(Some(Ok(_))) => {}
                    Ok(None) | Ok(Some(Err(_))) => break 'ws,
                    Err(_) => {
                        warn!("⚠️ Price Raptor: no USDC/USDT message in 30s — reconnecting the basis feed");
                        break 'ws;
                    }
                }
            }
        }
        warn!("⚠️ Price Raptor: USDC/USDT basis feed disconnected. Reconnecting in 5s...");
        if !connected {
            ws_url_idx = (ws_url_idx + 1) % ws_urls.len();
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

pub async fn run_price_raptor(
    crypto_filter: String,
    // Currency of the published oracle level; see [`MarkDenomination`].
    denomination: MarkDenomination,
    oracle_tx: watch::Sender<Decimal>,
    velocity_tx: watch::Sender<(Decimal, Decimal, Decimal)>,
    // Sends (drift_60m, drift_10m, hist_vol) — drift values are raw quote-asset (USDT) Decimal;
    // hist_vol is the normalized [0,1] 60-min realized-vol (canonical flatness measure).
    // drift_10m fills the 5s–60m temporal gap for GBoost feature [18].
    drift_tx: watch::Sender<(Decimal, Decimal, Decimal)>,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
) {
    let binance_pair = match crypto_filter.as_str() {
        "eth" => "ethusdt",
        "sol" => "solusdt",
        _     => "btcusdt",
    };
    // Host rotation: binance.com is geo-blocked (HTTP 451) from US IPs; the
    // data-only mirror data-stream.binance.vision serves the same ticker
    // stream without restriction. Rotate hosts on each failed connect.
    let ws_urls = [
        format!("wss://stream.binance.com:9443/ws/{}@ticker", binance_pair),
        format!("wss://data-stream.binance.vision/ws/{}@ticker", binance_pair),
    ];
    let mut ws_url_idx = 0usize;
    let mut price_history: VecDeque<(Instant, Decimal)> = VecDeque::new();
    let mut price_history_60m: VecDeque<(Instant, Decimal)> = VecDeque::new();
    let mut price_history_10m: VecDeque<(Instant, Decimal)> = VecDeque::new();
    let mut prev_velocity = dec!(0);
    // Throttle for the periodic realized-volatility telemetry log.
    // Seeded in the past so the first eligible tick logs immediately.
    let mut last_vol_log = Instant::now()
        .checked_sub(Duration::from_secs(3600))
        .unwrap_or_else(Instant::now);
    // Throttle for the short-window gap warning; seeded in the past likewise.
    let mut last_gap_log = last_vol_log;
    // Throttle for the withheld-USD-level warning; seeded in the past likewise.
    let mut last_basis_gap_log = last_vol_log;

    // Dollars per USDT, from its own stream, only when the venue settles in
    // dollars. `None` until the first good print.
    let (basis_tx, basis_rx) = watch::channel::<Option<(Instant, Decimal)>>(None);
    if denomination == MarkDenomination::Usd {
        // Supervised like the raptor itself: a panic restarts the feed, a
        // clean return (the raptor is gone) ends it.
        tokio::spawn(async move {
            loop {
                match tokio::spawn(run_stablecoin_basis_feed(basis_tx.clone())).await {
                    Ok(()) => return,
                    Err(e) => {
                        warn!("⚠️ Price Raptor: USDC/USDT basis feed task died ({e}); restarting in 5s");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });
    } else {
        drop(basis_tx);
    }

    loop {
        // Bounded connect: an unbounded `connect_async().await` can hang forever on a
        // half-open TCP path or geo-block, silently wedging the task with no reconnect
        // and no log (observed 2026-07-07 — oracle price frozen for ~10h). Cap it.
        let url_str = &ws_urls[ws_url_idx];
        let conn = tokio_timeout(Duration::from_secs(20), connect_async(url_str)).await;
        let mut connected = false;
        if let Ok(Ok((mut ws_stream, _))) = conn {
            connected = true;
            info!(" Price Raptor connected to Binance for {} via {}", binance_pair.to_uppercase(), url_str);
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
                                        // The LEVEL goes out in the venue's settlement currency;
                                        // every rate below stays on the raw series (see the
                                        // module doc). `dec!(0)` is the documented "not
                                        // connected" value: a dollar level with no fresh
                                        // conversion factor is unknown, not the USDT price.
                                        let level = publish_level(
                                            denomination, price, *basis_rx.borrow(), now, Duration::from_secs(BASIS_MAX_AGE_SECS),
                                        );
                                        if level.is_none()
                                            && now.duration_since(last_basis_gap_log).as_secs() >= BASIS_GAP_LOG_SECS
                                        {
                                            last_basis_gap_log = now;
                                            let age = basis_rx.borrow().map(|(at, _)| now.saturating_duration_since(at).as_secs());
                                            match age {
                                                Some(a) => warn!(
                                                    "⚠️ Price Raptor [{}]: USD level withheld — last USDC/USDT quote is {a}s old (limit {}s); oracle and rates read 0 (not connected) until the basis feed returns",
                                                    crypto_filter.to_uppercase(), BASIS_MAX_AGE_SECS,
                                                ),
                                                None => warn!(
                                                    "⚠️ Price Raptor [{}]: USD level withheld — no USDC/USDT quote yet; oracle and rates read 0 (not connected) until the first print",
                                                    crypto_filter.to_uppercase(),
                                                ),
                                            }
                                        }
                                        let _ = oracle_tx.send(level.unwrap_or(Decimal::ZERO));
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

                                        // Short velocity (1s window) — measured from the prior
                                        // sample nearest one window ago, when one sits close
                                        // enough to stand for it; zero (unconfirmed) otherwise.
                                        // See `short_window_anchor` and `short_window_start`.
                                        let window_ms = u128::from(config::MOMENTUM_SHORT_WINDOW_SECS) * 1000;
                                        let anchor = short_window_anchor(&price_history, now, window_ms);
                                        let anchor_age_ms = anchor.map_or(0, |(age, _)| age);
                                        let velocity_1s = match short_window_start(
                                            &price_history, now, window_ms, SHORT_WINDOW_ANCHOR_TOLERANCE_MS,
                                        ) {
                                            Some(p) => price - p,
                                            None => {
                                                // A gap (or a fresh connection): say so, throttled, so
                                                // the zero below is not mistaken for a flat second.
                                                if now.duration_since(last_gap_log).as_secs() >= SHORT_WINDOW_GAP_LOG_SECS {
                                                    last_gap_log = now;
                                                    warn!(
                                                        "⚠️ Price Raptor [{}]: no {}s-window anchor — nearest prior sample is {}ms old \
                                                         (accepted {}–{}ms); 1s velocity reads 0 (unconfirmed) until the feed catches up",
                                                        crypto_filter.to_uppercase(), config::MOMENTUM_SHORT_WINDOW_SECS, anchor_age_ms,
                                                        window_ms.saturating_sub(SHORT_WINDOW_ANCHOR_TOLERANCE_MS),
                                                        window_ms + SHORT_WINDOW_ANCHOR_TOLERANCE_MS,
                                                    );
                                                }
                                                dec!(0)
                                            }
                                        };

                                        // Acceleration: rate of change of velocity
                                        let acceleration = velocity_5s - prev_velocity;
                                        prev_velocity = velocity_5s;
                                        // Rates go out only beside a level (see `publish_level`);
                                        // the histories keep filling so they resume at once.
                                        let (velocity_5s, velocity_1s, acceleration) = if level.is_some() {
                                            (velocity_5s, velocity_1s, acceleration)
                                        } else {
                                            (dec!(0), dec!(0), dec!(0))
                                        };
                                        let _ = velocity_tx.send((velocity_5s, velocity_1s, acceleration));

                                        // 60-minute drift
                                        price_history_60m.push_back((now, price));
                                        while let Some((t, _)) = price_history_60m.front() {
                                            if now.duration_since(*t).as_secs() > 3600 {
                                                price_history_60m.pop_front();
                                            } else { break; }
                                        }
                                        // Graceful degradation (mirrors drift_10m below): once at least
                                        // DRIFT_60M_MIN_WINDOW_SECS of history exists, report the drift over
                                        // whatever window IS available rather than staying 0 until a full
                                        // hour accrues.  The prior all-or-nothing `>= 3600s` check left the
                                        // Convergence 60m-exhaustion gate blind for a full hour after every
                                        // restart.  A shorter window yields a smaller drift → conservative.
                                        let drift_60m = if let Some((oldest_t, oldest_p)) = price_history_60m.front() {
                                            let window_secs = now.duration_since(*oldest_t).as_secs();
                                            if window_secs >= config::DRIFT_60M_MIN_WINDOW_SECS {
                                                price - oldest_p
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

                                        // Canonical 60-min realized volatility (normalized [0,1]),
                                        // computed over the proper time-spaced 60-min price window —
                                        // the same value logged in the periodic realized-vol telemetry.
                                        // Sent every tick so GBoost's flatness gate reads a real
                                        // measure instead of recomputing from its 50ms-cadence buffer.
                                        let hist_vol_norm = {
                                            let prices: Vec<f64> = price_history_60m
                                                .iter()
                                                .map(|(_, p)| p.to_f64().unwrap_or(0.0))
                                                .collect();
                                            Decimal::from_f64_retain(normalized_hist_vol(&prices)).map(|d| d.round_dp(10)).unwrap_or(dec!(0))
                                        };

                                        let (drift_60m, drift_10m, hist_vol_norm) = if level.is_some() {
                                            (drift_60m, drift_10m, hist_vol_norm)
                                        } else {
                                            (dec!(0), dec!(0), dec!(0))
                                        };
                                        let _ = drift_tx.send((drift_60m, drift_10m, hist_vol_norm));

                                        // Mirror the latest signal snapshot into the shared
                                        // raptor-health map so GET /api/telemetry can graph it.
                                        raptor_health_tx.send_modify(|map| {
                                            let h = map.entry(crypto_filter.clone()).or_default();
                                            h.oracle_price = level.unwrap_or(Decimal::ZERO);
                                            // A live socket with no publishable level is not a
                                            // connected price feed from the consumer's side, and
                                            // nothing below is being published either.
                                            h.price_connected = level.is_some();
                                            h.velocity_5s  = velocity_5s;
                                            h.velocity_1s  = velocity_1s;
                                            h.velocity_1s_anchor_ms = anchor_age_ms;
                                            h.acceleration = acceleration;
                                            h.drift_60m    = drift_60m;
                                            h.drift_10m    = drift_10m;
                                        });

                                        // Periodic realized-volatility telemetry (~every 120s).
                                        // Shared oracle-vol math so any viper can calibrate its
                                        // own choppiness gates against a common 60m measure.
                                        if now.duration_since(last_vol_log).as_secs()
                                            >= config::DIAGNOSTIC_LOG_INTERVAL_SECS
                                        {
                                            last_vol_log = now;
                                            let prices: Vec<f64> = price_history_60m
                                                .iter()
                                                .map(|(_, p)| p.to_f64().unwrap_or(0.0))
                                                .collect();
                                            if prices.len() >= 5 {
                                                let basis_note = match denomination {
                                                    MarkDenomination::QuoteAsset => String::new(),
                                                    MarkDenomination::Usd => match *basis_rx.borrow() {
                                                        Some((at, f)) => format!(" | usd/usdt={f:.4} ({}s old)", now.saturating_duration_since(at).as_secs()),
                                                        None => " | usd/usdt=unknown".to_string(),
                                                    },
                                                };
                                                info!(
                                                    " [{}] 60m realized-vol: hist_vol={:.4} (norm 0-1) | range={:.3}% | samples={}{}",
                                                    crypto_filter.to_uppercase(),
                                                    normalized_hist_vol(&prices),
                                                    range_pct(&prices),
                                                    prices.len(),
                                                    basis_note,
                                                );
                                            }
                                        }
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
        // Rotate hosts only when the *connect* itself failed (e.g. HTTP 451
        // geo-block from US IPs — data-stream.binance.vision is the open
        // mirror). A mid-stream drop on a working host stays on that host.
        if !connected {
            ws_url_idx = (ws_url_idx + 1) % ws_urls.len();
        }
        // Mark price raptor as offline while reconnecting.
        raptor_health_tx.send_modify(|map| {
            map.entry(crypto_filter.clone()).or_default().price_connected = false;
        });
        prev_velocity = dec!(0);
        price_history_10m.clear();
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[cfg(test)]
mod short_window_tests {
    use super::*;

    const TOL: u128 = SHORT_WINDOW_ANCHOR_TOLERANCE_MS;

    fn hist(now: Instant, ages_ms: &[u64]) -> VecDeque<(Instant, Decimal)> {
        // Oldest first, newest (the current tick, age 0) last; price = age so
        // the chosen sample can be read back from the returned price.
        let mut v: VecDeque<(Instant, Decimal)> = VecDeque::new();
        let mut sorted = ages_ms.to_vec();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        for a in sorted {
            v.push_back((now - Duration::from_millis(a), Decimal::from(a)));
        }
        v
    }

    /// One tick per second: the previous tick is the start whether jitter
    /// delivered it at 999ms or 1001ms. This is the 2026-09-21 defect: the old
    /// rule chose the current tick at 1001ms and reported 0.00.
    #[test]
    fn one_tick_per_second_uses_the_previous_tick_regardless_of_jitter() {
        let now = Instant::now();
        assert_eq!(short_window_start(&hist(now, &[2002, 1001, 0]), now, 1000, TOL), Some(Decimal::from(1001)));
        assert_eq!(short_window_start(&hist(now, &[1998, 999, 0]), now, 1000, TOL), Some(Decimal::from(999)));
        assert_eq!(short_window_start(&hist(now, &[3000, 2000, 1000, 0]), now, 1000, TOL), Some(Decimal::from(1000)));
    }

    /// The tick just pushed is never its own anchor: alone it yields `None`.
    #[test]
    fn the_current_tick_is_never_the_anchor() {
        let now = Instant::now();
        assert_eq!(short_window_anchor(&hist(now, &[0]), now, 1000), None);
        assert!(short_window_anchor(&VecDeque::new(), now, 1000).is_none());
        assert_eq!(short_window_start(&hist(now, &[0]), now, 1000, TOL), None);
    }

    /// A feed gap yields no start: samples at 4,900ms and now would otherwise
    /// report a 4.9-second delta as a one-second velocity and pass the
    /// confirmation gate exactly when the old rule vetoed it. A missed tick
    /// (2,000ms) is refused the same way. The anchor itself is still reported
    /// so the raptor can name the gap in its log line.
    #[test]
    fn a_gap_yields_no_start_but_names_the_nearest_sample() {
        let now = Instant::now();
        assert_eq!(short_window_start(&hist(now, &[4900, 0]), now, 1000, TOL), None);
        assert_eq!(short_window_anchor(&hist(now, &[4900, 0]), now, 1000), Some((4900, Decimal::from(4900))));
        assert_eq!(short_window_start(&hist(now, &[3000, 2000, 0]), now, 1000, TOL), None);
    }

    /// The bound is inclusive and symmetric: 1,500ms and 500ms stand for the
    /// window, 1,501ms and 499ms do not.
    #[test]
    fn the_anchor_bound_is_inclusive_and_symmetric() {
        let now = Instant::now();
        assert_eq!(short_window_start(&hist(now, &[1500, 0]), now, 1000, TOL), Some(Decimal::from(1500)));
        assert_eq!(short_window_start(&hist(now, &[1501, 0]), now, 1000, TOL), None);
        assert_eq!(short_window_start(&hist(now, &[500, 0]), now, 1000, TOL), Some(Decimal::from(500)));
        assert_eq!(short_window_start(&hist(now, &[499, 0]), now, 1000, TOL), None);
    }

    /// A real-time stream picks the print nearest one window ago; ties go to
    /// the older sample.
    #[test]
    fn real_time_stream_picks_the_nearest_print_and_ties_go_older() {
        let now = Instant::now();
        assert_eq!(
            short_window_start(&hist(now, &[1500, 1200, 1050, 900, 400, 100, 0]), now, 1000, TOL),
            Some(Decimal::from(1050)),
        );
        assert_eq!(short_window_start(&hist(now, &[1100, 900, 0]), now, 1000, TOL), Some(Decimal::from(1100)));
    }
}

/// Runs the raptor against Binance itself. Ignored by default (network); run
/// with `cargo test live_binance -- --ignored --nocapture`.
///
/// This is the check the 2026-09-21 defect needed and no unit test can give:
/// on the real 1000ms `@ticker` cadence, whenever the last price differs from
/// the previous tick's, the 1s velocity must be that tick-to-tick delta, never
/// a zero produced by the window missing the previous sample. Over ~20 ticks
/// the old rule read zero on roughly half of them.
#[cfg(test)]
mod live_binance_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "connects to Binance"]
    async fn live_binance_short_window_tracks_the_previous_tick() {
        let (oracle_tx, oracle_rx) = watch::channel(dec!(0));
        let (velocity_tx, mut velocity_rx) = watch::channel((dec!(0), dec!(0), dec!(0)));
        let (drift_tx, _drift_rx) = watch::channel((dec!(0), dec!(0), dec!(0)));
        let health = Arc::new(watch::channel(HashMap::new()).0);
        let raptor = tokio::spawn(run_price_raptor("btc".into(), MarkDenomination::QuoteAsset, oracle_tx, velocity_tx, drift_tx, health));

        let mut prev_price: Option<Decimal> = None;
        let mut prev_at: Option<Instant> = None;
        let mut checked = 0usize;
        let mut moved = 0usize;
        let mut zero_on_move = 0usize;
        let deadline = Instant::now() + Duration::from_secs(40);
        while Instant::now() < deadline && checked < 25 {
            if tokio::time::timeout(Duration::from_secs(20), velocity_rx.changed()).await.is_err() {
                break;
            }
            let (v5, v1, _) = *velocity_rx.borrow();
            let price = *oracle_rx.borrow();
            let at = Instant::now();
            if let (Some(prev), Some(prev_t)) = (prev_price, prev_at) {
                checked += 1;
                let gap_ms = at.duration_since(prev_t).as_millis();
                // A tick that arrived outside the anchor bound is a gap by
                // design (the raptor reports 0, unconfirmed); only a zero on an
                // in-bound tick is the defect.
                let in_bound = gap_ms.abs_diff(1000) <= SHORT_WINDOW_ANCHOR_TOLERANCE_MS;
                if price != prev && in_bound {
                    moved += 1;
                    if v1 == dec!(0) { zero_on_move += 1; }
                    println!("tick: price={price} prev={prev} v5={v5} v1={v1} tick_delta={} gap={gap_ms}ms", price - prev);
                }
            }
            prev_price = Some(price);
            prev_at = Some(at);
        }
        raptor.abort();
        assert!(checked >= 10, "only {checked} ticks received; Binance unreachable from here?");
        assert!(moved >= 3, "price moved on only {moved} of {checked} ticks; rerun in a livelier tape");
        assert_eq!(zero_on_move, 0, "1s velocity read 0.00 on {zero_on_move} of {moved} ticks where the price moved");
    }
}

#[cfg(test)]
mod usd_mark_tests {
    use super::*;

    /// 2026-10-18, 12:48 ET: Binance BTC/USDT 81,083.53 while USDC/USDT printed
    /// 1.0013. The dollar level is below the $80,999.99 strike that the USDT
    /// level was $83 above.
    #[test]
    fn the_dollar_level_is_the_quote_price_divided_by_usdc_usdt() {
        let factor = usd_per_quote(dec!(1.0013)).expect("sane print");
        let now = Instant::now();
        let level = usd_mark(dec!(81083.53), Some((now, factor)), now, Duration::from_secs(900)).unwrap();
        assert!(level < dec!(80999.99), "{level} should sit below the strike");
        assert!(level > dec!(80950), "{level} is about $105 under the USDT quote, not more");
        // At par the level is the quote price.
        assert_eq!(usd_mark(dec!(81083.53), Some((now, usd_per_quote(dec!(1)).unwrap())), now, Duration::from_secs(900)), Some(dec!(81083.53)));
    }

    /// A zero or absurd USDC/USDT print is not a conversion factor.
    #[test]
    fn insane_or_zero_basis_prints_are_refused() {
        assert_eq!(usd_per_quote(Decimal::ZERO), None);
        assert_eq!(usd_per_quote(dec!(-1)), None);
        assert_eq!(usd_per_quote(dec!(0.5)), None);
        assert_eq!(usd_per_quote(dec!(1.2)), None);
        assert!(usd_per_quote(dec!(0.95)).is_some());
        assert!(usd_per_quote(dec!(1.05)).is_some());
    }

    /// No basis yet, or a basis past its age limit, withholds the level: the
    /// caller publishes 0 (not connected) rather than a stale conversion.
    #[test]
    fn a_missing_or_stale_basis_withholds_the_level() {
        let now = Instant::now();
        let max = Duration::from_secs(BASIS_MAX_AGE_SECS);
        assert_eq!(usd_mark(dec!(81000), None, now, max), None);
        let fresh = now - Duration::from_secs(BASIS_MAX_AGE_SECS - 1);
        assert!(usd_mark(dec!(81000), Some((fresh, dec!(0.9987))), now, max).is_some());
        let stale = now - Duration::from_secs(BASIS_MAX_AGE_SECS + 1);
        assert_eq!(usd_mark(dec!(81000), Some((stale, dec!(0.9987))), now, max), None);
    }

    /// A quote-asset venue publishes the raw price whatever the basis says; a
    /// dollar venue publishes the conversion or nothing.
    #[test]
    fn the_quote_asset_venue_publishes_the_raw_price() {
        let now = Instant::now();
        let max = Duration::from_secs(BASIS_MAX_AGE_SECS);
        assert_eq!(publish_level(MarkDenomination::QuoteAsset, dec!(81083.53), None, now, max), Some(dec!(81083.53)));
        assert_eq!(publish_level(MarkDenomination::QuoteAsset, dec!(81083.53), Some((now, dec!(0.9987))), now, max), Some(dec!(81083.53)));
        assert_eq!(publish_level(MarkDenomination::Usd, dec!(81083.53), None, now, max), None);
        assert_eq!(publish_level(MarkDenomination::Usd, dec!(81083.53), Some((now, dec!(0.9987))), now, max), Some(dec!(80978.12)));
    }

    /// The sane band is inclusive at both edges.
    #[test]
    fn the_sane_band_edges_are_accepted() {
        assert_eq!(usd_per_quote(dec!(0.90)), Some(dec!(1) / dec!(0.90)));
        assert_eq!(usd_per_quote(dec!(1.10)), Some(dec!(1) / dec!(1.10)));
        assert_eq!(usd_per_quote(dec!(0.8999)), None);
        assert_eq!(usd_per_quote(dec!(1.1001)), None);
    }

    /// One print may not move the factor more than 30 bp while the factor in
    /// force is fresh: a 1.02 print beside a 1.0013 regime (about $1,500 of
    /// BTC) is refused, a 1.0014 print is taken. With no factor, or a stale
    /// one, the first in-band print is accepted as the new regime.
    #[test]
    fn a_print_far_from_the_factor_in_force_is_refused_until_it_ages_out() {
        let now = Instant::now();
        let max = Duration::from_secs(BASIS_MAX_AGE_SECS);
        let in_force = Some((now, usd_per_quote(dec!(1.0013)).unwrap()));
        assert_eq!(accept_basis_print(dec!(1.02), in_force, None, now, max), None);
        assert_eq!(accept_basis_print(dec!(0.98), in_force, None, now, max), None);
        assert_eq!(accept_basis_print(dec!(1.0014), in_force, None, now, max), usd_per_quote(dec!(1.0014)));
        assert_eq!(accept_basis_print(dec!(1.0040), in_force, None, now, max), usd_per_quote(dec!(1.0040)));
        assert_eq!(accept_basis_print(dec!(1.02), None, None, now, max), usd_per_quote(dec!(1.02)));
        let stale = Some((now - Duration::from_secs(BASIS_MAX_AGE_SECS + 1), usd_per_quote(dec!(1.0013)).unwrap()));
        assert_eq!(accept_basis_print(dec!(1.02), stale, None, now, max), usd_per_quote(dec!(1.02)));
        // Out of band is refused regardless.
        assert_eq!(accept_basis_print(dec!(1.2), None, None, now, max), None);
    }

    /// A real peg move is a run of far prints. While the run is younger than
    /// the confirmation window the factor in force stands; once prints have
    /// disagreed for the whole window the next one is accepted, so a wrong
    /// rate is never held for the full age limit.
    #[test]
    fn a_sustained_run_of_far_prints_is_accepted_as_a_new_regime() {
        let now = Instant::now();
        let max = Duration::from_secs(BASIS_MAX_AGE_SECS);
        let in_force = Some((now, usd_per_quote(dec!(1.0013)).unwrap()));
        let first = usd_per_quote(dec!(1.02)).unwrap();
        let young = Some((now - Duration::from_secs(BASIS_REGIME_CONFIRM_SECS - 1), first));
        assert_eq!(accept_basis_print(dec!(1.02), in_force, young, now, max), None);
        let confirmed = Some((now - Duration::from_secs(BASIS_REGIME_CONFIRM_SECS), first));
        assert_eq!(accept_basis_print(dec!(1.02), in_force, confirmed, now, max), usd_per_quote(dec!(1.02)));
        assert_eq!(accept_basis_print(dec!(1.0215), in_force, confirmed, now, max), usd_per_quote(dec!(1.0215)), "agrees with the run within 30 bp");
        // The confirming print must agree with the run: a stray print at the
        // confirmation moment is not the new regime.
        assert_eq!(accept_basis_print(dec!(1.05), in_force, confirmed, now, max), None);
        assert_eq!(accept_basis_print(dec!(0.98), in_force, confirmed, now, max), None);
        // An out-of-band print is still never a factor, run or no run.
        assert_eq!(accept_basis_print(dec!(1.2), in_force, confirmed, now, max), None);
    }
}
