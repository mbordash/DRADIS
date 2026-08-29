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

//! In-memory log ring buffer — powers the Control Tower "Console" view.
//!
//! A `MakeWriter` tee for `tracing_subscriber::fmt`: every formatted log line
//! still goes to stdout (Docker log driver, journald, …) and is ALSO pushed
//! into a bounded in-memory ring so `GET /api/logs` can serve recent history
//! without touching the Docker socket or the filesystem. AMI operators use
//! this to confirm the engine is alive and to copy snippets into GitHub
//! Issues without needing SSH or CLI access.
//!
//! Capacity is line-based (`LOG_RING_CAPACITY`, default 2000) — at DRADIS's
//! normal log volume that is roughly the last half hour of activity, a few
//! hundred KB of memory at most. ANSI escape sequences are stripped on
//! insertion so the API output is clean text.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::{Mutex, OnceLock};

const LOG_RING_CAPACITY: usize = 2000;

static RING: OnceLock<Mutex<VecDeque<String>>> = OnceLock::new();

fn ring() -> &'static Mutex<VecDeque<String>> {
    RING.get_or_init(|| Mutex::new(VecDeque::with_capacity(LOG_RING_CAPACITY)))
}

/// Last `n` log lines, oldest first. `n` is clamped to the ring capacity.
pub fn tail(n: usize) -> Vec<String> {
    let ring = ring().lock().unwrap_or_else(|p| p.into_inner());
    ring.iter().rev().take(n).rev().cloned().collect()
}

fn push_line(line: &str) {
    let line = strip_ansi(line);
    if line.trim().is_empty() {
        return;
    }
    let mut ring = ring().lock().unwrap_or_else(|p| p.into_inner());
    if ring.len() == LOG_RING_CAPACITY {
        ring.pop_front();
    }
    ring.push_back(line);
}

/// Remove ANSI CSI escape sequences (`ESC [ … <final byte>`).
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // Skip "[" plus parameter/intermediate bytes up to the final byte (@–~).
            if chars.next() == Some('[') {
                for c2 in chars.by_ref() {
                    if ('\u{40}'..='\u{7e}').contains(&c2) {
                        break;
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Per-event writer handed out by [`TeeMakeWriter`]. Buffers the formatted
/// event, forwards it verbatim to stdout, and pushes complete lines into the
/// ring on drop (fmt may issue several small writes per event).
pub struct TeeWriter {
    buf: Vec<u8>,
}

impl Write for TeeWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for TeeWriter {
    fn drop(&mut self) {
        let _ = io::stdout().write_all(&self.buf);
        for line in String::from_utf8_lossy(&self.buf).lines() {
            push_line(line);
        }
    }
}

/// `MakeWriter` for `tracing_subscriber::fmt().with_writer(...)`.
#[derive(Clone, Copy, Default)]
pub struct TeeMakeWriter;

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TeeMakeWriter {
    type Writer = TeeWriter;

    fn make_writer(&'a self) -> Self::Writer {
        TeeWriter { buf: Vec::with_capacity(256) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ansi_stripping() {
        assert_eq!(strip_ansi("\u{1b}[32m INFO\u{1b}[0m hello"), " INFO hello");
        assert_eq!(strip_ansi("plain"), "plain");
    }

    #[test]
    fn ring_tail_order_and_bound() {
        for i in 0..(LOG_RING_CAPACITY + 10) {
            push_line(&format!("line-{i}"));
        }
        let t = tail(3);
        assert_eq!(t.len(), 3);
        assert_eq!(t[2], format!("line-{}", LOG_RING_CAPACITY + 9));
        assert!(tail(usize::MAX).len() <= LOG_RING_CAPACITY);
    }
}

// ── Subscriber filter ─────────────────────────────────────────────────────────

/// Build the `EnvFilter` the engine's subscriber runs with.
///
/// Lives here rather than inline in `main.rs` so it can be tested: it is the
/// only thing standing between a dependency's internal chatter and an operator's
/// log, and "we added a directive" is not the same claim as "the directive
/// rejects the line we meant it to".
///
/// Starts from `RUST_LOG` and then silences the `perpetual` gradient booster
/// below ERROR. That crate logs a WARN whenever a fit spends its whole iteration
/// budget: "Reached the configured iteration cap before auto stopping. Try to
/// decrease the budget or increase the iteration limit." For GBoost that is
/// routine — the retrain succeeds a second later — but it names knobs by their
/// crate-internal names, so to an operator it reads as a fault in a library they
/// have never heard of, with advice they cannot act on. It reached a customer's
/// log on the v1.0.5 Marketplace AMI on 2026-08-29.
///
/// `set_log_iterations(0)` in `gboost_impl` silences that crate's stdout
/// progress lines but NOT this, which comes through `tracing` — a distinction
/// the comment there used to get wrong.
///
/// The suppression yields to an explicit request: if `RUST_LOG` mentions
/// `perpetual` at all, whatever it says stands, so the booster stays debuggable.
pub fn build_env_filter(rust_log: Option<&str>) -> tracing_subscriber::EnvFilter {
    let mut filter = match rust_log {
        Some(spec) => tracing_subscriber::EnvFilter::new(spec),
        None => tracing_subscriber::EnvFilter::from_default_env(),
    };
    if !rust_log.unwrap_or_default().contains("perpetual") {
        filter = filter.add_directive(
            "perpetual=error".parse().expect("static directive parses"),
        );
    }
    filter
}

#[cfg(test)]
mod env_filter_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Capture writer: collects everything the subscriber formats.
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Capture;
        fn make_writer(&'a self) -> Self::Writer { self.clone() }
    }

    /// Run `body` under a subscriber wired with the filter under test, and
    /// return everything it emitted. Exercises the real path — filter, layer and
    /// formatter — rather than asking the filter a question in isolation.
    fn emitted(rust_log: &str, body: impl FnOnce()) -> String {
        let cap = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(build_env_filter(Some(rust_log)))
            .with_writer(cap.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, body);
        let bytes = cap.0.lock().unwrap().clone();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    /// The exact line that reached a customer's log on the v1.0.5 AMI.
    #[test]
    fn a_perpetual_warn_is_suppressed() {
        let out = emitted("info,dradis=info", || {
            tracing::warn!(
                target: "perpetual::booster::core",
                "Reached the configured iteration cap before auto stopping."
            );
        });
        assert!(out.is_empty(), "perpetual WARN reached the log: {out}");
    }

    /// Suppression is not a blackout — a real fault in the booster still shows.
    #[test]
    fn a_perpetual_error_still_passes() {
        let out = emitted("info,dradis=info", || {
            tracing::error!(target: "perpetual::booster::core", "fit failed");
        });
        assert!(out.contains("fit failed"), "perpetual ERROR was swallowed: {out:?}");
    }

    /// The escape hatch: asking for it explicitly wins, so the booster stays
    /// debuggable for anyone who needs it.
    #[test]
    fn an_explicit_rust_log_directive_wins() {
        let out = emitted("info,perpetual=warn", || {
            tracing::warn!(target: "perpetual::booster::core", "iteration cap");
        });
        assert!(out.contains("iteration cap"), "explicit RUST_LOG was overridden: {out:?}");
    }

    /// Nothing else is affected — DRADIS's own output must be untouched.
    #[test]
    fn dradis_output_is_untouched() {
        let out = emitted("info,dradis=info", || {
            tracing::warn!(target: "dradis::vipers::gboost_impl", "degenerate retrain");
            tracing::info!(target: "dradis::squadron::patrol_impl", "squadron deployed");
        });
        assert!(out.contains("degenerate retrain"), "{out:?}");
        assert!(out.contains("squadron deployed"), "{out:?}");
    }
}
