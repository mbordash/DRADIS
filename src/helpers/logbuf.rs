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
