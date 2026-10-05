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

/// Where the durable copy of the log goes, once resolved.
///
/// `None` until the first write, then either an open file or a decision not to
/// keep one. Resolved once rather than per event, because this runs on every
/// log line.
static LOG_FILE: OnceLock<Option<Mutex<std::fs::File>>> = OnceLock::new();

/// The largest a single log file may grow before it is replaced.
///
/// A new file starts when this is exceeded, and exactly one previous generation
/// is kept alongside it (`dradis.log.1`), so the footprint is bounded at roughly
/// twice this figure. A t3.medium accumulates a few megabytes a day at
/// `RUST_LOG=info`, so this holds weeks rather than hours.
const LOG_FILE_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Directory for the durable log, matching the bind mount in the AMI's compose
/// file (`./logs:/app/logs`). Overridable for a local run or a test.
fn log_dir() -> std::path::PathBuf {
    std::env::var("DRADIS_LOG_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/app/logs"))
}

/// Open the durable log, rotating first if the current one is already full.
///
/// Returns `None` when the directory does not exist, which is the normal case
/// for a local `cargo run` outside the container: the engine then behaves
/// exactly as it did before this existed.
fn open_log_file() -> Option<Mutex<std::fs::File>> {
    let dir = log_dir();
    if !dir.is_dir() {
        return None;
    }
    let path = dir.join("dradis.log");
    if std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0) >= LOG_FILE_MAX_BYTES {
        // One generation back. A longer history is not worth the disk on an
        // instance whose whole record of what it traded is the SQLite file
        // beside it.
        let _ = std::fs::rename(&path, dir.join("dradis.log.1"));
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .ok()
        .map(Mutex::new)
}

impl Drop for TeeWriter {
    fn drop(&mut self) {
        let _ = io::stdout().write_all(&self.buf);

        // A durable copy, because stdout is not one.
        //
        // The engine's log lived only in the container's stdout, so every
        // `docker compose up` destroyed it. That is not a theoretical loss: on
        // 2026-10-03 a deploy erased the entry trace for a live Helm position
        // whose share count was wrong, and the mechanism had to be found by
        // reading code instead; and the Momentum break-even gate recorded
        // verdicts from 2026-09-30 onward that no longer exist. The SQLite
        // files in this same directory already survive a redeploy — the log is
        // the one piece of evidence that did not.
        //
        // ANSI escapes are stripped on the way in: the ring strips them for the
        // Console, and a file full of colour codes is worse than useless to
        // `grep`.
        if let Some(file) = LOG_FILE.get_or_init(open_log_file).as_ref() {
            if let Ok(mut f) = file.lock() {
                let _ = f.write_all(strip_ansi(&String::from_utf8_lossy(&self.buf)).as_bytes());
            }
        }

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

    /// The durable log is opened only where a directory exists for it.
    ///
    /// A local `cargo run` has no `/app/logs`, and the engine must behave
    /// exactly as it did before the file sink existed rather than failing or
    /// creating stray directories.
    #[test]
    fn a_missing_log_directory_yields_no_file() {
        let missing = std::env::temp_dir().join("dradis-logbuf-does-not-exist-xyz");
        let _ = std::fs::remove_dir_all(&missing);
        // SAFETY: single-threaded test, and the var is read only by `log_dir`.
        unsafe { std::env::set_var("DRADIS_LOG_DIR", &missing); }
        assert!(super::open_log_file().is_none(), "no directory means no file, not a panic");
        assert!(!missing.exists(), "and nothing is created behind the operator's back");
        unsafe { std::env::remove_var("DRADIS_LOG_DIR"); }
    }

    /// A full log is rotated, keeping exactly one generation.
    ///
    /// Unbounded growth is how a long-lived instance fills its disk; discarding
    /// everything is how the 2026-10-03 deploy destroyed the evidence this sink
    /// exists to keep. One generation back bounds the footprint at roughly twice
    /// the limit while holding weeks of history at `info`.
    #[test]
    fn a_full_log_rotates_and_keeps_one_generation() {
        let dir = std::env::temp_dir().join(format!("dradis-logbuf-rot-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let live = dir.join("dradis.log");
        let prev = dir.join("dradis.log.1");

        // A log already past the limit, and an older generation to be replaced.
        std::fs::write(&prev, b"older generation").unwrap();
        std::fs::write(&live, vec![b'x'; (super::LOG_FILE_MAX_BYTES + 1) as usize]).unwrap();

        unsafe { std::env::set_var("DRADIS_LOG_DIR", &dir); }
        let opened = super::open_log_file();
        assert!(opened.is_some(), "a writable directory must yield a file");
        assert_eq!(
            std::fs::metadata(&live).unwrap().len(), 0,
            "the live log restarts empty after rotation",
        );
        assert_eq!(
            std::fs::metadata(&prev).unwrap().len(), super::LOG_FILE_MAX_BYTES + 1,
            "the full log becomes the kept generation, replacing the older one",
        );
        unsafe { std::env::remove_var("DRADIS_LOG_DIR"); }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A log below the limit is appended to, not rotated.
    ///
    /// Rotating on every open would discard the log on each restart, which is
    /// the failure this sink is meant to end.
    #[test]
    fn a_short_log_survives_a_restart() {
        let dir = std::env::temp_dir().join(format!("dradis-logbuf-app-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("dradis.log"), b"from the previous process\n").unwrap();

        unsafe { std::env::set_var("DRADIS_LOG_DIR", &dir); }
        assert!(super::open_log_file().is_some());
        let kept = std::fs::read_to_string(dir.join("dradis.log")).unwrap();
        assert!(kept.contains("previous process"), "a restart must not truncate the log");
        assert!(!dir.join("dradis.log.1").exists(), "and must not rotate a short log");
        unsafe { std::env::remove_var("DRADIS_LOG_DIR"); }
        let _ = std::fs::remove_dir_all(&dir);
    }
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
/// The booster's `set_log_iterations(0)` silences its stdout progress lines
/// but NOT this, which comes through `tracing`. DRADIS no longer trains a
/// booster in-process (the GBoost model is exported offline and only loaded
/// here), so the line should not recur; the filter stays so a future fit
/// cannot bring it back.
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
            tracing::warn!(target: "dradis::vipers::gboost_planb", "degenerate retrain");
            tracing::info!(target: "dradis::squadron::patrol_impl", "squadron deployed");
        });
        assert!(out.contains("degenerate retrain"), "{out:?}");
        assert!(out.contains("squadron deployed"), "{out:?}");
    }
}
