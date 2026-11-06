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

//! The paper ledger: where a scripted viper trades while its Live switch is off.
//!
//! Nothing here reaches the order path, the position map or the `trades`
//! table, so paper results can never be mixed into real ones, on any venue,
//! whether the instance is live or simulated. That is why it exists: the
//! dispatcher has no notion of one viper simulating while the instance trades
//! real money.
//!
//! The fills are a taker's: an entry fills at the ask, up to the size shown
//! there, and pays the venue's taker fee; an exit fills at the bid and pays it
//! again; a take-profit is watched and sold as a taker once the bid reaches
//! it. A position still open after its market closes is scored at the venue's
//! own resolution (no fee), through the resolver every venue installs.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use chrono::{DateTime, Utc};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::{Row, SqlitePool};

/// First resolution probe this long after the stated close.
pub const SETTLE_GRACE_SECS: i64 = 60;
/// One probe per token this often while unresolved.
pub const PROBE_SECS: u64 = 60;
/// Past this, an unresolved row is closed unscored.
pub const GIVE_UP_SECS: i64 = 24 * 3600;

#[derive(Debug, Clone, Serialize)]
pub struct PaperRow {
    pub id: i64,
    pub script: String,
    pub squadron_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub market: String,
    pub side: String,
    pub opened_at: DateTime<Utc>,
    pub close_time: Option<DateTime<Utc>>,
    pub entry_price: Decimal,
    pub shares: Decimal,
    pub entry_fee: Decimal,
    pub take_profit: Option<Decimal>,
    pub entry_reason: String,
    pub script_hash: String,
    pub closed_at: Option<DateTime<Utc>>,
    pub exit_price: Option<Decimal>,
    pub exit_fee: Option<Decimal>,
    pub exit_reason: Option<String>,
    pub close_hash: Option<String>,
    pub pnl: Option<Decimal>,
}

impl PaperRow {
    pub fn cost(&self) -> Decimal { self.entry_price * self.shares + self.entry_fee }
}

fn f(d: Decimal) -> f64 { d.to_f64().unwrap_or(0.0) }
fn dec(x: f64) -> Decimal { Decimal::from_f64(x).unwrap_or_default().round_dp(6) }
fn ts(s: Option<String>) -> Option<DateTime<Utc>> {
    s.and_then(|s| DateTime::parse_from_rfc3339(&s).ok()).map(|t| t.with_timezone(&Utc))
}

fn row(r: &sqlx::sqlite::SqliteRow) -> PaperRow {
    PaperRow {
        id: r.get("id"),
        script: r.get("script"),
        squadron_id: r.get("squadron_id"),
        condition_id: r.get("condition_id"),
        token_id: r.get("token_id"),
        market: r.get("market"),
        side: r.get("side"),
        opened_at: ts(r.get("opened_at")).unwrap_or_else(Utc::now),
        close_time: ts(r.get("close_time")),
        entry_price: dec(r.get("entry_price")),
        shares: dec(r.get("shares")),
        entry_fee: dec(r.get("entry_fee")),
        take_profit: r.get::<Option<f64>, _>("take_profit").map(dec),
        entry_reason: r.get("entry_reason"),
        script_hash: r.get("script_hash"),
        closed_at: ts(r.get("closed_at")),
        exit_price: r.get::<Option<f64>, _>("exit_price").map(dec),
        exit_fee: r.get::<Option<f64>, _>("exit_fee").map(dec),
        exit_reason: r.get("exit_reason"),
        close_hash: r.get("close_hash"),
        pnl: r.get::<Option<f64>, _>("pnl").map(dec),
    }
}

pub async fn insert(pool: &SqlitePool, asset: &str, r: &PaperRow) -> Option<i64> {
    sqlx::query(
        "INSERT INTO script_paper (script, asset, squadron_id, condition_id, token_id, market, side,
            opened_at, close_time, entry_price, shares, entry_fee, take_profit, entry_reason, script_hash)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(&r.script).bind(asset.to_lowercase()).bind(&r.squadron_id).bind(&r.condition_id)
    .bind(&r.token_id).bind(&r.market).bind(&r.side).bind(r.opened_at.to_rfc3339())
    .bind(r.close_time.map(|t| t.to_rfc3339())).bind(f(r.entry_price)).bind(f(r.shares))
    .bind(f(r.entry_fee)).bind(r.take_profit.map(f)).bind(&r.entry_reason).bind(&r.script_hash)
    .execute(pool).await.ok().map(|x| x.last_insert_rowid())
}

pub async fn close(pool: &SqlitePool, id: i64, exit_price: Decimal, exit_fee: Decimal,
                   reason: &str, close_hash: &str, pnl: Option<Decimal>) -> bool {
    sqlx::query(
        "UPDATE script_paper SET closed_at = ?, exit_price = ?, exit_fee = ?, exit_reason = ?,
            close_hash = ?, pnl = ? WHERE id = ? AND closed_at IS NULL"
    )
    .bind(Utc::now().to_rfc3339()).bind(f(exit_price)).bind(f(exit_fee)).bind(reason)
    .bind(close_hash).bind(pnl.map(f)).bind(id)
    .execute(pool).await.is_ok_and(|x| x.rows_affected() == 1)
}

pub async fn set_take_profit(pool: &SqlitePool, id: i64, tp: Decimal) -> bool {
    sqlx::query("UPDATE script_paper SET take_profit = ? WHERE id = ? AND closed_at IS NULL")
        .bind(f(tp)).bind(id).execute(pool).await.is_ok()
}

pub async fn open_rows(pool: &SqlitePool, squadron: &str, script: &str) -> Vec<PaperRow> {
    sqlx::query("SELECT * FROM script_paper WHERE closed_at IS NULL AND squadron_id = ? AND script = ? ORDER BY id")
        .bind(squadron).bind(script)
        .fetch_all(pool).await.unwrap_or_default().iter().map(row).collect()
}

/// The most recent rows of one script, newest first, for the page.
pub async fn recent(pool: &SqlitePool, script: &str, limit: i64) -> Vec<PaperRow> {
    sqlx::query("SELECT * FROM script_paper WHERE script = ? ORDER BY id DESC LIMIT ?")
        .bind(script).bind(limit.clamp(1, 1000))
        .fetch_all(pool).await.unwrap_or_default().iter().map(row).collect()
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Summary {
    pub open: i64,
    pub closed: i64,
    pub scored: i64,
    pub wins: i64,
    pub pnl: Decimal,
    pub fees: Decimal,
}

pub async fn summary(pool: &SqlitePool, script: &str) -> Summary {
    let r = sqlx::query(
        "SELECT SUM(closed_at IS NULL), SUM(closed_at IS NOT NULL), SUM(pnl IS NOT NULL),
                SUM(pnl > 0), COALESCE(SUM(pnl), 0.0), COALESCE(SUM(CASE WHEN pnl IS NOT NULL THEN entry_fee + COALESCE(exit_fee, 0.0) END), 0.0)
           FROM script_paper WHERE script = ?"
    ).bind(script).fetch_one(pool).await;
    match r {
        Ok(r) => Summary {
            open: r.get::<Option<i64>, _>(0).unwrap_or(0),
            closed: r.get::<Option<i64>, _>(1).unwrap_or(0),
            scored: r.get::<Option<i64>, _>(2).unwrap_or(0),
            wins: r.get::<Option<i64>, _>(3).unwrap_or(0),
            pnl: dec(r.get::<f64, _>(4)),
            fees: dec(r.get::<f64, _>(5)),
        },
        Err(_) => Summary::default(),
    }
}

/// P&L of a paper exit at `price` paying `exit_fee`.
pub fn pnl_at(r: &PaperRow, price: Decimal, exit_fee: Decimal) -> Decimal {
    (price - r.entry_price) * r.shares - r.entry_fee - exit_fee
}

// ── In-memory view, so a tick costs no database read ─────────────────────────

type Key = (String, String); // (squadron, script)

#[derive(Default)]
struct Book {
    loaded: bool,
    rows: Vec<PaperRow>,
}

fn books() -> &'static Mutex<HashMap<Key, Book>> {
    static B: OnceLock<Mutex<HashMap<Key, Book>>> = OnceLock::new();
    B.get_or_init(Default::default)
}

fn lock() -> std::sync::MutexGuard<'static, HashMap<Key, Book>> {
    books().lock().unwrap_or_else(|p| p.into_inner())
}

/// The open paper rows of this script on this squadron, read from the database
/// the first time and kept in memory after that.
pub async fn cached_open(pool: &SqlitePool, squadron: &str, script: &str) -> Vec<PaperRow> {
    let key = (squadron.to_string(), script.to_string());
    if let Some(b) = lock().get(&key).filter(|b| b.loaded) {
        return b.rows.clone();
    }
    let rows = open_rows(pool, squadron, script).await;
    let mut m = lock();
    let b = m.entry(key).or_default();
    if !b.loaded {
        b.rows = rows;
        b.loaded = true;
    }
    b.rows.clone()
}

/// Every open row of this script the process has loaded, on any squadron.
/// The exposure cap is the viper's, not one squadron's.
pub fn cached_all(script: &str) -> Vec<PaperRow> {
    lock().iter().filter(|((_, s), _)| s == script).flat_map(|(_, b)| b.rows.clone()).collect()
}

/// The open rows of one squadron as last loaded, without touching the database
/// (for the dry run, which is synchronous).
pub fn cached_for(squadron: &str, script: &str) -> Vec<PaperRow> {
    lock().get(&(squadron.to_string(), script.to_string())).map(|b| b.rows.clone()).unwrap_or_default()
}

/// Close every open row of a script without scoring it: the script is being
/// deleted, and a row no viper follows would sit open forever. Paper rows are
/// not money; the record says why they stopped.
pub async fn close_all_unscored(pool: &SqlitePool, script: &str, reason: &str) -> u64 {
    let n = sqlx::query(
        "UPDATE script_paper SET closed_at = ?, exit_reason = ?, pnl = NULL WHERE script = ? AND closed_at IS NULL"
    ).bind(Utc::now().to_rfc3339()).bind(reason).bind(script)
        .execute(pool).await.map(|x| x.rows_affected()).unwrap_or(0);
    lock().retain(|(_, s), _| s != script);
    n
}

pub fn cache_add(squadron: &str, script: &str, r: PaperRow) {
    lock().entry((squadron.to_string(), script.to_string())).or_default().rows.push(r);
}

pub fn cache_remove(squadron: &str, script: &str, id: i64) {
    if let Some(b) = lock().get_mut(&(squadron.to_string(), script.to_string())) {
        b.rows.retain(|r| r.id != id);
    }
}

pub fn cache_set_tp(squadron: &str, script: &str, id: i64, tp: Decimal) {
    if let Some(b) = lock().get_mut(&(squadron.to_string(), script.to_string())) {
        if let Some(r) = b.rows.iter_mut().find(|r| r.id == id) { r.take_profit = Some(tp); }
    }
}

// ── Settlement probes ────────────────────────────────────────────────────────

fn probes() -> &'static Mutex<(HashMap<String, Instant>, HashMap<String, Decimal>)> {
    static P: OnceLock<Mutex<(HashMap<String, Instant>, HashMap<String, Decimal>)>> = OnceLock::new();
    P.get_or_init(Default::default)
}

/// The venue's final price for `token` if a probe has brought one back;
/// otherwise start a probe (at most one per token per `PROBE_SECS`) and return
/// `None`. Spawned, because a venue call can outlast the per-strategy timeout.
pub fn resolution(condition_id: &str, token: &str) -> Option<Decimal> {
    let mut p = probes().lock().unwrap_or_else(|x| x.into_inner());
    if let Some(px) = p.1.get(token) { return Some(*px); }
    let due = p.0.get(token).is_none_or(|t| t.elapsed().as_secs() >= PROBE_SECS);
    if due {
        p.0.insert(token.to_string(), Instant::now());
        p.0.retain(|_, t| t.elapsed().as_secs() < 2 * GIVE_UP_SECS as u64);
        let (cid, tok) = (condition_id.to_string(), token.to_string());
        tokio::spawn(async move {
            let got = crate::venues::core::resolve_leg(&cid, &crate::venues::core::MarketId::new(tok.as_str())).await;
            if let Some(crate::venues::core::TokenResolution::Resolved(px)) = got {
                let mut p = probes().lock().unwrap_or_else(|x| x.into_inner());
                if p.1.len() > 512 { p.1.clear(); }
                p.1.insert(tok, px);
            }
        });
    }
    None
}

/// Forget a token's resolution once its rows are booked.
pub fn forget_resolution(token: &str) {
    probes().lock().unwrap_or_else(|x| x.into_inner()).1.remove(token);
}

#[cfg(test)]
pub fn set_resolution_for_test(token: &str, px: Decimal) {
    probes().lock().unwrap_or_else(|x| x.into_inner()).1.insert(token.to_string(), px);
}
