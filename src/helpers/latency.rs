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

//! Venue latency probe.
//!
//! Periodically times a lightweight unauthenticated GET against the trading
//! venue (Polymarket CLOB `/time` on intl builds, Polymarket US `/v1/health`
//! on US builds) and keeps a small rolling window of round-trip samples.
//!
//! The Control Tower footer surfaces the result so an operator can instantly
//! see whether their server is deployed too far from the venue — the first
//! thing to check when fills look slow. The probe measures latency **from the
//! engine host**, not from the operator's browser, which is what actually
//! matters for order execution.
//!
//! `run_latency_probe` is spawned once from `run_api_server`; `snapshot()` is
//! read by the `GET /api/latency` handler.

use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::Serialize;
use tracing::debug;

/// Rolling window size — at one probe per [`PROBE_INTERVAL`] this covers the
/// last ~5 minutes.
const SAMPLE_CAP: usize = 20;
/// Seconds between probes. Cheap enough to be invisible in venue rate limits.
const PROBE_INTERVAL: Duration = Duration::from_secs(15);
/// Per-request timeout; anything slower is recorded as a failed probe.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

struct ProbeState {
    /// Round-trip times of recent successful probes, oldest → newest, in ms.
    samples: VecDeque<u64>,
    /// Whether the most recent probe succeeded.
    last_ok: bool,
    /// Whether at least one probe has completed (success or failure).
    probed: bool,
}

static STATE: OnceLock<Mutex<ProbeState>> = OnceLock::new();

fn state() -> &'static Mutex<ProbeState> {
    STATE.get_or_init(|| Mutex::new(ProbeState {
        samples: VecDeque::with_capacity(SAMPLE_CAP),
        last_ok: false,
        probed: false,
    }))
}

/// Short venue label + probe URL for the active build.
#[cfg(feature = "intl_clob")]
fn probe_target() -> (&'static str, String) {
    ("CLOB", format!("{}/time", crate::config::CLOB_API_BASE))
}

/// Kalshi build probes the Kalshi REST exchange status (public, no auth).
#[cfg(feature = "kalshi")]
fn probe_target() -> (&'static str, String) {
    let base = crate::venues::kalshi::base_url();
    ("Kalshi API", format!("{}/exchange/status", base))
}

/// US retail build probes the venue REST health endpoint (public, no auth).
#[cfg(not(any(feature = "intl_clob", feature = "kalshi")))]
fn probe_target() -> (&'static str, String) {
    let base = std::env::var("POLYMARKET_US_BASE_URL")
        .unwrap_or_else(|_| "https://api.polymarket.us".to_string());
    ("US API", format!("{}/v1/health", base.trim_end_matches('/')))
}

/// Snapshot returned by `GET /api/latency`.
#[derive(Serialize)]
pub struct LatencySnapshot {
    /// Short label for the probed venue ("CLOB" or "US API").
    pub venue: &'static str,
    /// Whether the most recent probe succeeded.
    pub ok: bool,
    /// Whether at least one probe has completed since startup.
    pub probed: bool,
    /// Most recent successful round-trip, ms.
    pub last_ms: Option<u64>,
    /// Median of the rolling window, ms.
    pub p50_ms: Option<u64>,
    /// Number of successful samples currently in the window.
    pub samples: usize,
}

/// Current probe state for the API handler.
pub fn snapshot() -> LatencySnapshot {
    let (venue, _) = probe_target();
    let st = match state().lock() {
        Ok(s) => s,
        Err(poisoned) => poisoned.into_inner(),
    };
    let last_ms = st.samples.back().copied();
    let p50_ms = if st.samples.is_empty() {
        None
    } else {
        let mut sorted: Vec<u64> = st.samples.iter().copied().collect();
        sorted.sort_unstable();
        Some(sorted[sorted.len() / 2])
    };
    LatencySnapshot { venue, ok: st.last_ok, probed: st.probed, last_ms, p50_ms, samples: st.samples.len() }
}

fn record(sample_ms: Option<u64>) {
    let mut st = match state().lock() {
        Ok(s) => s,
        Err(poisoned) => poisoned.into_inner(),
    };
    st.probed = true;
    match sample_ms {
        Some(ms) => {
            st.last_ok = true;
            if st.samples.len() == SAMPLE_CAP {
                st.samples.pop_front();
            }
            st.samples.push_back(ms);
        }
        None => st.last_ok = false,
    }
}

/// Background loop: probe the venue every [`PROBE_INTERVAL`] forever.
pub async fn run_latency_probe() {
    let (venue, url) = probe_target();
    let client = match reqwest::Client::builder().timeout(PROBE_TIMEOUT).build() {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("Latency probe disabled — HTTP client build failed: {e}");
            return;
        }
    };
    tracing::info!("📶 Venue latency probe started ({venue} → {url})");
    loop {
        let started = Instant::now();
        let result = client.get(&url).send().await;
        match result {
            Ok(resp) => {
                // Drain the (tiny) body so we time the full round trip.
                let status = resp.status();
                let _ = resp.bytes().await;
                let ms = started.elapsed().as_millis() as u64;
                if status.is_success() {
                    debug!("Latency probe {venue}: {ms}ms");
                    record(Some(ms));
                } else {
                    debug!("Latency probe {venue}: HTTP {status}");
                    record(None);
                }
            }
            Err(e) => {
                debug!("Latency probe {venue} failed: {e}");
                record(None);
            }
        }
        tokio::time::sleep(PROBE_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the tests in this module against each other.
    ///
    /// `record` and `snapshot` operate on one process-wide `STATE`, and cargo
    /// runs tests in parallel threads inside a single process — so without this
    /// the two tests below interleave on the same static. That is exactly how
    /// CI failed: `snapshot_reports_window_and_median` recorded a failed probe
    /// and asserted `!snap.ok`, while `window_is_capped` recorded a success in
    /// between and flipped `ok` back to true. It passed locally and failed in
    /// CI purely on thread timing.
    static TEST_GUARD: Mutex<()> = Mutex::new(());

    /// Return the shared state to its initial values so each test starts from a
    /// known point and can assert exact numbers rather than `>=` bounds.
    fn reset() {
        let mut s = state().lock().unwrap();
        s.samples.clear();
        s.last_ok = false;
        s.probed = false;
    }

    #[test]
    fn snapshot_reports_window_and_median() {
        let _guard = TEST_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        reset();

        record(Some(100));
        record(Some(300));
        record(Some(200));

        let snap = snapshot();
        assert!(snap.probed);
        assert!(snap.ok);
        assert_eq!(snap.samples, 3);
        assert_eq!(snap.last_ms, Some(200));
        assert_eq!(snap.p50_ms, Some(200), "median of 100/200/300");

        // A failed probe keeps the window but flips `ok`.
        record(None);
        let snap = snapshot();
        assert!(!snap.ok);
        assert_eq!(snap.samples, 3, "a failure must not discard the window");
    }

    #[test]
    fn window_is_capped() {
        let _guard = TEST_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        reset();

        for i in 0..(SAMPLE_CAP as u64 + 10) {
            record(Some(i));
        }

        let snap = snapshot();
        assert_eq!(snap.samples, SAMPLE_CAP, "the window must hold exactly the cap");
    }
}
