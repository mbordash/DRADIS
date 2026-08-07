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

/// US retail build probes the venue REST health endpoint (public, no auth).
#[cfg(not(feature = "intl_clob"))]
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

    #[test]
    fn snapshot_reports_window_and_median() {
        // Empty window → no data, not yet probed... but other tests share the
        // static, so only assert invariants that hold regardless of ordering.
        record(Some(100));
        record(Some(300));
        record(Some(200));
        let snap = snapshot();
        assert!(snap.probed);
        assert!(snap.ok);
        assert!(snap.samples >= 3);
        assert!(snap.last_ms.is_some());
        assert!(snap.p50_ms.is_some());

        record(None); // failed probe keeps samples but flips ok
        let snap = snapshot();
        assert!(!snap.ok);
        assert!(snap.samples >= 3);
    }

    #[test]
    fn window_is_capped() {
        for i in 0..(SAMPLE_CAP as u64 + 10) {
            record(Some(i));
        }
        let snap = snapshot();
        assert!(snap.samples <= SAMPLE_CAP);
    }
}
