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

//! [E111] Machine load sampler.
//!
//! Every [`SAMPLE_INTERVAL`] the sampler reads the kernel's own counters for
//! the machine the engine runs on (`/proc/stat`, `/proc/loadavg`,
//! `/proc/meminfo`) and for the engine process (`/proc/self/stat`,
//! `/proc/self/status`), and keeps one snapshot for `GET /api/load`. The
//! Control Tower footer shows it beside the venue latency, so an operator who
//! has just armed every viper on every squadron can see what that costs the
//! box without opening a shell.
//!
//! The figures are the machine's, not the container's: `/proc` inside Docker
//! shows the host kernel's counters, which is what "is my machine overwhelmed"
//! asks about. A platform without `/proc` (a macOS dev box) reports
//! `probed: false` and the footer shows nothing.
//!
//! `run_load_sampler` is spawned once from `run_api_server`; `snapshot()` is
//! read by the handler.

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::Serialize;

/// Seconds between samples. Each sample is five small file reads.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// Clock ticks per second in `/proc` time fields (`USER_HZ`). Fixed at 100 by
/// the kernel's `/proc` ABI on every architecture, whatever the scheduler tick.
const USER_HZ: f64 = 100.0;

/// One reading of the counters that need a previous reading to mean anything.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Counters {
    /// Machine-wide busy jiffies, summed over every core.
    busy: u64,
    /// Machine-wide jiffies of any kind, summed over every core.
    total: u64,
    /// This process's user + system jiffies.
    engine: u64,
    at: Instant,
}

/// Snapshot returned by `GET /api/load`.
#[derive(Serialize, Clone, Debug, Default, PartialEq)]
pub struct LoadSnapshot {
    /// Whether two samples have been taken, so the rates below exist.
    pub probed: bool,
    /// Logical cores the engine can see.
    pub cores: usize,
    /// Machine-wide CPU in use over the last interval, percent of all cores
    /// together (100 = every core busy).
    pub machine_cpu_pct: Option<f64>,
    /// This engine's CPU over the same interval, percent of all cores together.
    pub engine_cpu_pct: Option<f64>,
    /// The kernel's one-minute load average.
    pub load_1m: Option<f64>,
    /// This engine's resident memory, MB.
    pub engine_rss_mb: Option<u64>,
    /// The machine's memory, MB.
    pub mem_total_mb: Option<u64>,
    /// Memory the machine can still hand out without swapping, MB.
    pub mem_available_mb: Option<u64>,
    /// Seconds between samples, for the hover text.
    pub interval_secs: u64,
}

struct State {
    last: Option<Counters>,
    snapshot: LoadSnapshot,
}

static STATE: OnceLock<Mutex<State>> = OnceLock::new();

fn state() -> &'static Mutex<State> {
    STATE.get_or_init(|| Mutex::new(State { last: None, snapshot: LoadSnapshot { interval_secs: SAMPLE_INTERVAL.as_secs(), ..Default::default() } }))
}

/// Current snapshot for the API handler.
pub fn snapshot() -> LoadSnapshot {
    state().lock().unwrap_or_else(|p| p.into_inner()).snapshot.clone()
}

/// The `cpu` line of `/proc/stat`: user nice system idle iowait irq softirq
/// steal [guest guest_nice]. Idle time is idle + iowait; guest time is already
/// counted inside user, so the sum stops at steal.
fn parse_proc_stat(text: &str) -> Option<(u64, u64)> {
    let line = text.lines().find(|l| l.starts_with("cpu "))?;
    let v: Vec<u64> = line.split_whitespace().skip(1).filter_map(|x| x.parse().ok()).collect();
    if v.len() < 4 { return None; }
    let idle = v[3] + v.get(4).copied().unwrap_or(0);
    let total: u64 = v.iter().take(8).sum();
    Some((total.saturating_sub(idle), total))
}

/// utime + stime from `/proc/self/stat`. The second field is the command name
/// in parentheses and may hold spaces, so the split starts after the last `)`.
fn parse_self_stat(text: &str) -> Option<u64> {
    let rest = &text[text.rfind(')')? + 1..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    // `rest` starts at field 3 (state); utime is field 14, stime field 15.
    let utime: u64 = f.get(11)?.parse().ok()?;
    let stime: u64 = f.get(12)?.parse().ok()?;
    Some(utime + stime)
}

/// A `Key:   12345 kB` line's number, from `/proc/meminfo` or `/proc/self/status`.
fn kb_field(text: &str, key: &str) -> Option<u64> {
    text.lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|x| x.parse().ok())
}

fn parse_loadavg(text: &str) -> Option<f64> {
    text.split_whitespace().next()?.parse().ok()
}

/// The two rates between consecutive readings, percent of all cores together,
/// rounded to one decimal. `None` when the machine counters did not advance.
fn rates(prev: &Counters, cur: &Counters, cores: usize) -> (Option<f64>, Option<f64>) {
    let total = cur.total.saturating_sub(prev.total);
    if total == 0 { return (None, None); }
    let busy = cur.busy.saturating_sub(prev.busy);
    let machine = (busy as f64 / total as f64 * 100.0).clamp(0.0, 100.0);
    let secs = cur.at.saturating_duration_since(prev.at).as_secs_f64();
    let engine = if secs > 0.0 {
        let share = cur.engine.saturating_sub(prev.engine) as f64 / USER_HZ / secs / cores.max(1) as f64 * 100.0;
        Some((share.clamp(0.0, 100.0) * 10.0).round() / 10.0)
    } else {
        None
    };
    (Some((machine * 10.0).round() / 10.0), engine)
}

fn read(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

/// Take one sample and fold it into the snapshot.
fn sample() {
    let cores = cores();
    let machine = read("/proc/stat").and_then(|t| parse_proc_stat(&t));
    let engine = read("/proc/self/stat").and_then(|t| parse_self_stat(&t));
    let cur = match (machine, engine) {
        (Some((busy, total)), Some(engine)) => Some(Counters { busy, total, engine, at: Instant::now() }),
        _ => None,
    };
    let load_1m = read("/proc/loadavg").and_then(|t| parse_loadavg(&t));
    let meminfo = read("/proc/meminfo");
    let mem_total_mb = meminfo.as_deref().and_then(|t| kb_field(t, "MemTotal:")).map(|kb| kb / 1024);
    let mem_available_mb = meminfo.as_deref().and_then(|t| kb_field(t, "MemAvailable:")).map(|kb| kb / 1024);
    let engine_rss_mb = read("/proc/self/status").and_then(|t| kb_field(&t, "VmRSS:")).map(|kb| kb / 1024);

    let mut st = state().lock().unwrap_or_else(|p| p.into_inner());
    let (machine_cpu_pct, engine_cpu_pct) = match (&st.last, &cur) {
        (Some(prev), Some(cur)) => rates(prev, cur, cores),
        _ => (None, None),
    };
    st.snapshot = LoadSnapshot {
        probed: machine_cpu_pct.is_some(),
        cores,
        machine_cpu_pct,
        engine_cpu_pct,
        load_1m,
        engine_rss_mb,
        mem_total_mb,
        mem_available_mb,
        interval_secs: SAMPLE_INTERVAL.as_secs(),
    };
    if cur.is_some() {
        st.last = cur;
    }
}

/// Background loop: sample every [`SAMPLE_INTERVAL`] forever. Returns at once
/// on a platform without `/proc`, leaving `probed: false`.
pub async fn run_load_sampler() {
    if read("/proc/stat").is_none() {
        tracing::info!("🖥️ Machine load sampler not started: no /proc on this platform");
        return;
    }
    tracing::info!("🖥️ Machine load sampler started (every {}s)", SAMPLE_INTERVAL.as_secs());
    loop {
        sample();
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cpu_line_splits_into_busy_and_total() {
        // user nice system idle iowait irq softirq steal guest guest_nice
        let text = "cpu  100 10 50 800 20 5 5 10 3 0\ncpu0 50 5 25 400 10 2 2 5 1 0\n";
        let (busy, total) = parse_proc_stat(text).unwrap();
        assert_eq!(total, 1000, "user through steal; guest is already inside user");
        assert_eq!(busy, 180, "everything but idle and iowait");
        assert_eq!(parse_proc_stat("intr 1 2 3\n"), None);
    }

    #[test]
    fn the_process_line_survives_a_space_in_the_command_name() {
        // pid (comm) state ppid pgrp session tty tpgid flags minflt cminflt majflt cmajflt utime stime ...
        let text = "4242 (dradis engine) S 1 4242 4242 0 -1 4194560 100 0 0 0 1234 567 0 0 20 0 9 0 100 1000 2000";
        assert_eq!(parse_self_stat(text), Some(1234 + 567));
        assert_eq!(parse_self_stat("garbage"), None);
    }

    #[test]
    fn kilobyte_fields_and_the_load_average_parse() {
        let meminfo = "MemTotal:        3924532 kB\nMemFree:         1508592 kB\nMemAvailable:    2937000 kB\n";
        assert_eq!(kb_field(meminfo, "MemTotal:"), Some(3_924_532));
        assert_eq!(kb_field(meminfo, "MemAvailable:"), Some(2_937_000));
        assert_eq!(kb_field(meminfo, "SwapTotal:"), None);
        assert_eq!(kb_field("VmRSS:\t   90332 kB\n", "VmRSS:"), Some(90_332));
        assert_eq!(parse_loadavg("0.11 0.15 0.13 1/412 3196307\n"), Some(0.11));
    }

    /// Five seconds on two cores: the machine counters advance 1000 jiffies
    /// in all, 120 of them busy; the engine used 60 jiffies, which is 0.6 s
    /// of one core, 6% of the two together.
    #[test]
    fn rates_are_a_share_of_every_core_together() {
        let at = Instant::now();
        let prev = Counters { busy: 1_000, total: 10_000, engine: 500, at };
        let cur = Counters { busy: 1_120, total: 11_000, engine: 560, at: at + Duration::from_secs(5) };
        assert_eq!(rates(&prev, &cur, 2), (Some(12.0), Some(6.0)));
        // Nothing advanced: no rate rather than a division by zero.
        assert_eq!(rates(&prev, &prev, 2), (None, None));
        // A counter that went backwards (a reset) reads as zero, not as a panic.
        let back = Counters { busy: 0, total: 10_500, engine: 0, at: at + Duration::from_secs(5) };
        assert_eq!(rates(&prev, &back, 2), (Some(0.0), Some(0.0)));
    }
}
