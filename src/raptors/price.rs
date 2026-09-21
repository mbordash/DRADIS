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
/// │ oracle_tx      │ Decimal                        │ Current spot price                 │
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

pub async fn run_price_raptor(
    crypto_filter: String,
    oracle_tx: watch::Sender<Decimal>,
    velocity_tx: watch::Sender<(Decimal, Decimal, Decimal)>,
    // Sends (drift_60m, drift_10m, hist_vol) — drift values are raw USD Decimal;
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

                                        let _ = drift_tx.send((drift_60m, drift_10m, hist_vol_norm));

                                        // Mirror the latest signal snapshot into the shared
                                        // raptor-health map so GET /api/telemetry can graph it.
                                        raptor_health_tx.send_modify(|map| {
                                            let h = map.entry(crypto_filter.clone()).or_default();
                                            h.oracle_price = price;
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
                                                info!(
                                                    " [{}] 60m realized-vol: hist_vol={:.4} (norm 0-1) | range={:.3}% | samples={}",
                                                    crypto_filter.to_uppercase(),
                                                    normalized_hist_vol(&prices),
                                                    range_pct(&prices),
                                                    prices.len(),
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
        let raptor = tokio::spawn(run_price_raptor("btc".into(), oracle_tx, velocity_tx, drift_tx, health));

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
