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

//! Helm intents: the operator's stated conviction, written down before anything
//! acts on it.
//!
//! An intent is the record a Helm position is built from and judged against. It
//! carries the thesis, a probability, a horizon, the condition that would prove
//! the thesis wrong, and the structured exit posture the engine will enforce.
//! The pre-registration discipline DRADIS applies to its research spikes is
//! applied here to the operator: the claim is stored first, and what is stored
//! is what was believed.
//!
//! **Two rules this module enforces by shape rather than by convention.**
//!
//! 1. *The first submission is immutable.* The `helm_intents` row holds the
//!    content exactly as first submitted and is never updated in place. Every
//!    later edit — after reading the critique, after a tightening — is a row in
//!    `helm_intent_revisions`, numbered from 2, and `current_version` says which
//!    one the engine reads. The retrospective can always see what the operator
//!    first wrote, and whether reading the critique changed it.
//! 2. *Status moves only along the machine.* `IntentStatus::can_transition_to`
//!    is the single definition, `transition` is the single writer, and every
//!    move is appended to `helm_intent_events`. A terminal status has no exit.
//!
//! This module stores and reads. It places no order and emits no signal; the
//! strategy that will act on an intent (`vipers::helm_impl`) is inert while the
//! entry and exit increments are unbuilt.

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

// ── Status machine ───────────────────────────────────────────────────────────

/// Where an intent is in its life.
///
/// `proposed → acknowledged → working → filled | partial | missed → closed | superseded`
///
/// `closed` and `superseded` are terminal. `partial → filled` is the one lateral
/// move: a partially filled entry that completes is the same intent, not a new
/// one. A `missed` entry does not re-arm in place; the operator supersedes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentStatus {
    /// Submitted. The critique (a later increment) runs against this version.
    Proposed,
    /// The operator has read the critique and confirmed. Eligible for entry.
    Acknowledged,
    /// The entry order is on the book or in flight.
    Working,
    /// The entry filled in full; the exit posture is managing the position.
    Filled,
    /// The entry filled in part; the posture manages what filled.
    Partial,
    /// The entry did not fill in its window.
    Missed,
    /// Done: exited, settled, cancelled or expired. `close_reason` says which.
    Closed,
    /// Replaced by a new intent (`superseded_by`), typically a loosening.
    Superseded,
}

impl IntentStatus {
    pub const ALL: [IntentStatus; 8] = [
        Self::Proposed, Self::Acknowledged, Self::Working, Self::Filled,
        Self::Partial, Self::Missed, Self::Closed, Self::Superseded,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Proposed => "proposed",
            Self::Acknowledged => "acknowledged",
            Self::Working => "working",
            Self::Filled => "filled",
            Self::Partial => "partial",
            Self::Missed => "missed",
            Self::Closed => "closed",
            Self::Superseded => "superseded",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|st| st.as_str() == s.trim().to_ascii_lowercase())
    }

    /// No transitions leave a terminal status.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Superseded)
    }

    /// The machine, in one place.
    pub fn can_transition_to(self, to: Self) -> bool {
        use IntentStatus::*;
        match (self, to) {
            (Proposed, Acknowledged) => true,
            (Acknowledged, Working) => true,
            (Working, Filled | Partial | Missed) => true,
            (Partial, Filled) => true,
            (from, Closed | Superseded) => !from.is_terminal(),
            _ => false,
        }
    }
}

/// Ride to expiry, or expect to exit sooner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Horizon { Expiry, Sooner }

impl Horizon {
    pub fn as_str(self) -> &'static str {
        match self { Self::Expiry => "expiry", Self::Sooner => "sooner" }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "expiry" => Some(Self::Expiry),
            "sooner" => Some(Self::Sooner),
            _ => None,
        }
    }
}

/// Take the ask now, or rest a bid and wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind { Taker, Resting }

impl EntryKind {
    pub fn as_str(self) -> &'static str {
        match self { Self::Taker => "taker", Self::Resting => "resting" }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "taker" => Some(Self::Taker),
            "resting" => Some(Self::Resting),
            _ => None,
        }
    }
}

// ── Content ──────────────────────────────────────────────────────────────────

/// Everything the operator states. Stored verbatim as version 1 on the intent
/// row; every later edit is a revision carrying a complete copy.
///
/// The posture fields are structured on purpose. "What would make me wrong" is
/// prose, and prose is kept; but the stop the engine enforces is a number the
/// operator typed, not a number a model inferred from the prose.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntentContent {
    /// What you believe and why.
    pub thesis: String,
    /// Probability the thesis resolves in your favor, strictly inside (0, 1).
    pub confidence: f64,
    pub horizon: Horizon,
    /// The falsification condition, in the operator's words.
    pub falsification: String,
    pub entry_kind: EntryKind,
    /// Resting bid price; required for `Resting`, absent for `Taker`.
    #[serde(default)]
    pub entry_limit_price: Option<Decimal>,
    /// Notional to commit, in USDC.
    pub size_usdc: Decimal,
    /// Stop price on the held token, or none.
    #[serde(default)]
    pub stop_price: Option<Decimal>,
    /// Resting take-profit price on the held token, or none.
    #[serde(default)]
    pub take_profit_price: Option<Decimal>,
    /// Exit at this time if still held, or none.
    #[serde(default)]
    pub time_limit_at: Option<DateTime<Utc>>,
    /// Hold to the market's resolution rather than stopping on price.
    #[serde(default)]
    pub hold_to_settlement: bool,
    /// Insurance floor as a fraction of entry (e.g. 0.30), armed under a hold.
    #[serde(default)]
    pub catastrophic_floor_pct: Option<Decimal>,
}

/// What the write surface needs to open an intent.
#[derive(Clone, Debug)]
pub struct NewIntent {
    pub squadron_id: String,
    /// The venue's condition id, when known; the registry does not keep it,
    /// `deployment_queue` does (`market_id_for_squadron`).
    pub market_id: String,
    pub market_name: String,
    /// `YES` or `NO`.
    pub side: String,
    pub content: IntentContent,
    /// Simulated squadron at the time of submission.
    pub ghost: bool,
    pub venue: String,
}

/// One stored intent, as the API returns it.
#[derive(Clone, Debug, Serialize)]
pub struct HelmIntent {
    pub id: i64,
    pub squadron_id: String,
    pub market_id: String,
    pub market_name: String,
    pub side: String,
    /// Version 1, exactly as first submitted. Never changes.
    pub first: IntentContent,
    /// Which version the engine reads; 1 means `first`.
    pub current_version: i64,
    pub status: IntentStatus,
    pub status_detail: Option<String>,
    pub critique: Option<String>,
    pub critique_at: Option<String>,
    pub critique_model: Option<String>,
    pub superseded_by: Option<i64>,
    pub close_reason: Option<String>,
    pub ghost: bool,
    pub venue: String,
    pub session_id: String,
    pub created_at: String,
    pub acknowledged_at: Option<String>,
    pub updated_at: String,
    pub closed_at: Option<String>,
}

/// A later version of an intent's content.
#[derive(Clone, Debug, Serialize)]
pub struct IntentRevision {
    pub id: i64,
    pub intent_id: i64,
    pub version: i64,
    /// Why the operator revised: shown beside the diff in the retrospective.
    pub reason: String,
    pub content: IntentContent,
    pub created_at: String,
}

/// One status move.
#[derive(Clone, Debug, Serialize)]
pub struct IntentEvent {
    pub id: i64,
    pub intent_id: i64,
    pub at: String,
    pub from_status: Option<IntentStatus>,
    pub to_status: IntentStatus,
    pub detail: Option<String>,
}

/// How many intents a squadron has, and how many are still open.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct IntentTally {
    pub total: i64,
    /// Not terminal.
    pub open: i64,
}

impl IntentTally {
    /// Every intent the squadron was given has reached a terminal status.
    /// False for a squadron that was never given one: it has nothing to be
    /// complete about and retires on the market's close like any other.
    pub fn complete(self) -> bool {
        self.total > 0 && self.open == 0
    }
}

// ── Shape check ──────────────────────────────────────────────────────────────

/// Is the content well formed? Structure only: a probability inside (0, 1), a
/// side that exists, a resting entry with a price, a size above zero, prices
/// inside the market's range. This is not the deterministic validator — that
/// also reads the book (a stop on the held side of the bid, a time limit inside
/// the close) and arrives with the form. Nothing here depends on market state,
/// so nothing here can be gamed except by filling the form honestly.
pub fn shape_check(side: &str, c: &IntentContent) -> Result<(), Vec<String>> {
    let mut errs = Vec::new();
    if !matches!(side.trim().to_ascii_uppercase().as_str(), "YES" | "NO") {
        errs.push(format!("side must be YES or NO, got {side:?}"));
    }
    if c.thesis.trim().is_empty() {
        errs.push("thesis is empty".into());
    }
    if c.falsification.trim().is_empty() {
        errs.push("falsification condition is empty".into());
    }
    if !(c.confidence.is_finite() && c.confidence > 0.0 && c.confidence < 1.0) {
        errs.push(format!("confidence must be strictly inside (0, 1), got {}", c.confidence));
    }
    if c.size_usdc <= Decimal::ZERO {
        errs.push(format!("size_usdc must be above zero, got {}", c.size_usdc));
    }
    let in_range = |p: Decimal| p > Decimal::ZERO && p < Decimal::ONE;
    match (c.entry_kind, c.entry_limit_price) {
        (EntryKind::Resting, None) => errs.push("a resting entry needs entry_limit_price".into()),
        (EntryKind::Resting, Some(p)) if !in_range(p) => errs.push(format!("entry_limit_price must be inside (0, 1), got {p}")),
        (EntryKind::Taker, Some(_)) => errs.push("a taker entry takes no entry_limit_price".into()),
        _ => {}
    }
    for (name, v) in [
        ("stop_price", c.stop_price),
        ("take_profit_price", c.take_profit_price),
        ("catastrophic_floor_pct", c.catastrophic_floor_pct),
    ] {
        if let Some(p) = v {
            if !in_range(p) {
                errs.push(format!("{name} must be inside (0, 1), got {p}"));
            }
        }
    }
    if errs.is_empty() { Ok(()) } else { Err(errs) }
}

// ── Schema ───────────────────────────────────────────────────────────────────

/// Create the Helm tables and add the join columns. Idempotent; called from
/// `db::init_schema` on every pool.
pub(crate) async fn init_schema(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS helm_intents (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            squadron_id            TEXT    NOT NULL,
            market_id              TEXT    NOT NULL DEFAULT '',
            market_name            TEXT    NOT NULL DEFAULT '',
            side                   TEXT    NOT NULL,
            -- version 1, immutable
            thesis                 TEXT    NOT NULL,
            confidence             REAL    NOT NULL,
            horizon                TEXT    NOT NULL,
            falsification          TEXT    NOT NULL,
            entry_kind             TEXT    NOT NULL,
            entry_limit_price      TEXT,
            size_usdc              TEXT    NOT NULL,
            stop_price             TEXT,
            take_profit_price      TEXT,
            time_limit_at          TEXT,
            hold_to_settlement     INTEGER NOT NULL DEFAULT 0,
            catastrophic_floor_pct TEXT,
            -- lifecycle
            current_version        INTEGER NOT NULL DEFAULT 1,
            status                 TEXT    NOT NULL DEFAULT 'proposed',
            status_detail          TEXT,
            critique               TEXT,
            critique_at            TEXT,
            critique_model         TEXT,
            superseded_by          INTEGER,
            close_reason           TEXT,
            ghost                  INTEGER NOT NULL DEFAULT 0,
            venue                  TEXT    NOT NULL DEFAULT '',
            session_id             TEXT    NOT NULL DEFAULT '',
            created_at             TEXT    NOT NULL,
            acknowledged_at        TEXT,
            updated_at             TEXT    NOT NULL,
            closed_at              TEXT
        )"
    ).execute(pool).await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_helm_intents_squadron ON helm_intents (squadron_id, status)"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS helm_intent_revisions (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            intent_id              INTEGER NOT NULL REFERENCES helm_intents(id),
            version                INTEGER NOT NULL,
            reason                 TEXT    NOT NULL,
            thesis                 TEXT    NOT NULL,
            confidence             REAL    NOT NULL,
            horizon                TEXT    NOT NULL,
            falsification          TEXT    NOT NULL,
            entry_kind             TEXT    NOT NULL,
            entry_limit_price      TEXT,
            size_usdc              TEXT    NOT NULL,
            stop_price             TEXT,
            take_profit_price      TEXT,
            time_limit_at          TEXT,
            hold_to_settlement     INTEGER NOT NULL DEFAULT 0,
            catastrophic_floor_pct TEXT,
            created_at             TEXT    NOT NULL,
            UNIQUE (intent_id, version)
        )"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS helm_intent_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            intent_id   INTEGER NOT NULL REFERENCES helm_intents(id),
            at          TEXT    NOT NULL,
            from_status TEXT,
            to_status   TEXT    NOT NULL,
            detail      TEXT
        )"
    ).execute(pool).await?;

    // The join the retrospective needs, and the label a purged-and-readopted
    // position keeps. Nullable: every row that is not Helm's stays NULL.
    // `ADD COLUMN` has no IF NOT EXISTS in SQLite; the duplicate-column error
    // on a second start is the no-op, as for every other migration here.
    let _ = sqlx::query("ALTER TABLE open_positions ADD COLUMN intent_id INTEGER").execute(pool).await;
    let _ = sqlx::query("ALTER TABLE trades ADD COLUMN intent_id INTEGER").execute(pool).await;
    Ok(())
}

// ── Row mapping ──────────────────────────────────────────────────────────────

fn dec_opt(row: &sqlx::sqlite::SqliteRow, col: &str) -> Option<Decimal> {
    row.try_get::<Option<String>, _>(col).ok().flatten().and_then(|s| Decimal::from_str(&s).ok())
}

fn dec(row: &sqlx::sqlite::SqliteRow, col: &str) -> Decimal {
    dec_opt(row, col).unwrap_or(Decimal::ZERO)
}

fn ts_opt(row: &sqlx::sqlite::SqliteRow, col: &str) -> Option<DateTime<Utc>> {
    row.try_get::<Option<String>, _>(col).ok().flatten()
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|d| d.with_timezone(&Utc))
}

fn content_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<IntentContent> {
    let horizon_s: String = row.try_get("horizon")?;
    let entry_s: String = row.try_get("entry_kind")?;
    Ok(IntentContent {
        thesis: row.try_get("thesis")?,
        confidence: row.try_get("confidence")?,
        horizon: Horizon::parse(&horizon_s).ok_or_else(|| anyhow!("bad horizon {horizon_s:?}"))?,
        falsification: row.try_get("falsification")?,
        entry_kind: EntryKind::parse(&entry_s).ok_or_else(|| anyhow!("bad entry_kind {entry_s:?}"))?,
        entry_limit_price: dec_opt(row, "entry_limit_price"),
        size_usdc: dec(row, "size_usdc"),
        stop_price: dec_opt(row, "stop_price"),
        take_profit_price: dec_opt(row, "take_profit_price"),
        time_limit_at: ts_opt(row, "time_limit_at"),
        hold_to_settlement: row.try_get::<i64, _>("hold_to_settlement").unwrap_or(0) != 0,
        catastrophic_floor_pct: dec_opt(row, "catastrophic_floor_pct"),
    })
}

fn intent_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<HelmIntent> {
    let status_s: String = row.try_get("status")?;
    Ok(HelmIntent {
        id: row.try_get("id")?,
        squadron_id: row.try_get("squadron_id")?,
        market_id: row.try_get("market_id")?,
        market_name: row.try_get("market_name")?,
        side: row.try_get("side")?,
        first: content_from_row(row)?,
        current_version: row.try_get("current_version")?,
        status: IntentStatus::parse(&status_s).ok_or_else(|| anyhow!("bad status {status_s:?}"))?,
        status_detail: row.try_get("status_detail")?,
        critique: row.try_get("critique")?,
        critique_at: row.try_get("critique_at")?,
        critique_model: row.try_get("critique_model")?,
        superseded_by: row.try_get("superseded_by")?,
        close_reason: row.try_get("close_reason")?,
        ghost: row.try_get::<i64, _>("ghost")? != 0,
        venue: row.try_get("venue")?,
        session_id: row.try_get("session_id")?,
        created_at: row.try_get("created_at")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        updated_at: row.try_get("updated_at")?,
        closed_at: row.try_get("closed_at")?,
    })
}

const SELECT_INTENT: &str = "SELECT * FROM helm_intents";

// ── Writes ───────────────────────────────────────────────────────────────────

/// Open an intent as `proposed`. The content is version 1 and never changes.
pub async fn create(pool: &SqlitePool, new: &NewIntent) -> Result<i64> {
    if let Err(errs) = shape_check(&new.side, &new.content) {
        bail!("intent is malformed: {}", errs.join("; "));
    }
    let now = Utc::now().to_rfc3339();
    let c = &new.content;
    let id = sqlx::query(
        "INSERT INTO helm_intents (
            squadron_id, market_id, market_name, side,
            thesis, confidence, horizon, falsification, entry_kind, entry_limit_price,
            size_usdc, stop_price, take_profit_price, time_limit_at, hold_to_settlement,
            catastrophic_floor_pct, status, ghost, venue, session_id, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'proposed', ?, ?, ?, ?, ?)"
    )
    .bind(&new.squadron_id).bind(&new.market_id).bind(&new.market_name)
    .bind(new.side.trim().to_ascii_uppercase())
    .bind(&c.thesis).bind(c.confidence).bind(c.horizon.as_str()).bind(&c.falsification)
    .bind(c.entry_kind.as_str()).bind(c.entry_limit_price.map(|d| d.to_string()))
    .bind(c.size_usdc.to_string()).bind(c.stop_price.map(|d| d.to_string()))
    .bind(c.take_profit_price.map(|d| d.to_string()))
    .bind(c.time_limit_at.map(|t| t.to_rfc3339()))
    .bind(c.hold_to_settlement as i64)
    .bind(c.catastrophic_floor_pct.map(|d| d.to_string()))
    .bind(new.ghost as i64).bind(&new.venue)
    .bind(crate::helpers::db::current_session_id())
    .bind(&now).bind(&now)
    .execute(pool).await?
    .last_insert_rowid();
    record_event(pool, id, None, IntentStatus::Proposed, None).await?;
    Ok(id)
}

async fn record_event(
    pool: &SqlitePool, intent_id: i64, from: Option<IntentStatus>, to: IntentStatus, detail: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO helm_intent_events (intent_id, at, from_status, to_status, detail) VALUES (?, ?, ?, ?, ?)"
    )
    .bind(intent_id).bind(Utc::now().to_rfc3339())
    .bind(from.map(IntentStatus::as_str)).bind(to.as_str()).bind(detail)
    .execute(pool).await?;
    Ok(())
}

/// Move an intent along the machine. Refuses a move the machine does not
/// allow, and refuses to touch a terminal intent at all. `detail` is kept on
/// the row as `status_detail` and, for `Closed`, as `close_reason`.
pub async fn transition(
    pool: &SqlitePool, id: i64, to: IntentStatus, detail: Option<&str>,
) -> Result<HelmIntent> {
    let cur = get(pool, id).await?.ok_or_else(|| anyhow!("intent {id} not found"))?;
    if !cur.status.can_transition_to(to) {
        bail!(
            "intent {id} cannot move {} → {}{}",
            cur.status.as_str(), to.as_str(),
            if cur.status.is_terminal() { " (terminal)" } else { "" },
        );
    }
    let now = Utc::now().to_rfc3339();
    let mut q = String::from("UPDATE helm_intents SET status = ?, status_detail = ?, updated_at = ?");
    if to == IntentStatus::Acknowledged { q.push_str(", acknowledged_at = ?"); }
    if to.is_terminal() { q.push_str(", closed_at = ?"); }
    if to == IntentStatus::Closed { q.push_str(", close_reason = ?"); }
    q.push_str(" WHERE id = ?");
    let mut stmt = sqlx::query(&q).bind(to.as_str()).bind(detail).bind(&now);
    if to == IntentStatus::Acknowledged { stmt = stmt.bind(&now); }
    if to.is_terminal() { stmt = stmt.bind(&now); }
    if to == IntentStatus::Closed { stmt = stmt.bind(detail.unwrap_or("closed")); }
    stmt.bind(id).execute(pool).await?;
    record_event(pool, id, Some(cur.status), to, detail).await?;
    get(pool, id).await?.ok_or_else(|| anyhow!("intent {id} vanished"))
}

/// Add a revision. The first submission is untouched; the new content becomes
/// `current_version`. Refused on a terminal intent, which can no longer be
/// about anything.
pub async fn revise(pool: &SqlitePool, id: i64, content: &IntentContent, reason: &str) -> Result<i64> {
    let cur = get(pool, id).await?.ok_or_else(|| anyhow!("intent {id} not found"))?;
    if cur.status.is_terminal() {
        bail!("intent {id} is {} and cannot be revised", cur.status.as_str());
    }
    if let Err(errs) = shape_check(&cur.side, content) {
        bail!("revision is malformed: {}", errs.join("; "));
    }
    if reason.trim().is_empty() {
        bail!("a revision needs a reason");
    }
    let version = cur.current_version + 1;
    let now = Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO helm_intent_revisions (
            intent_id, version, reason, thesis, confidence, horizon, falsification, entry_kind,
            entry_limit_price, size_usdc, stop_price, take_profit_price, time_limit_at,
            hold_to_settlement, catastrophic_floor_pct, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(id).bind(version).bind(reason.trim())
    .bind(&content.thesis).bind(content.confidence).bind(content.horizon.as_str()).bind(&content.falsification)
    .bind(content.entry_kind.as_str()).bind(content.entry_limit_price.map(|d| d.to_string()))
    .bind(content.size_usdc.to_string()).bind(content.stop_price.map(|d| d.to_string()))
    .bind(content.take_profit_price.map(|d| d.to_string()))
    .bind(content.time_limit_at.map(|t| t.to_rfc3339()))
    .bind(content.hold_to_settlement as i64)
    .bind(content.catastrophic_floor_pct.map(|d| d.to_string()))
    .bind(&now)
    .execute(&mut *tx).await?;
    sqlx::query("UPDATE helm_intents SET current_version = ?, updated_at = ? WHERE id = ?")
        .bind(version).bind(&now).bind(id)
        .execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(version)
}

/// Replace an intent with a new one. The old one becomes `superseded` and
/// points at its successor; the new one opens as `proposed`, so the critique
/// (when it exists) runs on the new thesis too. This is how a loosening is
/// expressed: not as an edit, but as a new stated reason.
pub async fn supersede(pool: &SqlitePool, old_id: i64, new: &NewIntent, reason: &str) -> Result<i64> {
    let old = get(pool, old_id).await?.ok_or_else(|| anyhow!("intent {old_id} not found"))?;
    if old.status.is_terminal() {
        bail!("intent {old_id} is {} and cannot be superseded", old.status.as_str());
    }
    if reason.trim().is_empty() {
        bail!("superseding needs a reason");
    }
    let new_id = create(pool, new).await?;
    sqlx::query("UPDATE helm_intents SET superseded_by = ? WHERE id = ?")
        .bind(new_id).bind(old_id).execute(pool).await?;
    transition(pool, old_id, IntentStatus::Superseded, Some(reason.trim())).await?;
    Ok(new_id)
}

/// Record the critique against an intent. Observational: changes no status.
pub async fn set_critique(pool: &SqlitePool, id: i64, critique: &str, model: &str) -> Result<()> {
    sqlx::query(
        "UPDATE helm_intents SET critique = ?, critique_at = ?, critique_model = ?, updated_at = ? WHERE id = ?"
    )
    .bind(critique).bind(Utc::now().to_rfc3339()).bind(model).bind(Utc::now().to_rfc3339()).bind(id)
    .execute(pool).await?;
    Ok(())
}

// ── Join plumbing for positions and trades ───────────────────────────────────

/// Label an open position with the intent that produced it. Keyed the way the
/// position table is keyed — token and strategy — so a chain-readopted row can
/// be relabeled by the same call.
pub async fn set_position_intent(pool: &SqlitePool, token_id: &str, strategy: &str, intent_id: i64) -> Result<u64> {
    let r = sqlx::query("UPDATE open_positions SET intent_id = ? WHERE token_id = ? AND strategy = ?")
        .bind(intent_id).bind(token_id).bind(strategy).execute(pool).await?;
    Ok(r.rows_affected())
}

pub async fn position_intent(pool: &SqlitePool, token_id: &str, strategy: &str) -> Option<i64> {
    sqlx::query_scalar::<_, Option<i64>>(
        "SELECT intent_id FROM open_positions WHERE token_id = ? AND strategy = ? ORDER BY id DESC LIMIT 1"
    )
    .bind(token_id).bind(strategy).fetch_optional(pool).await.ok().flatten().flatten()
}

/// Label a booked trade with its intent.
pub async fn set_trade_intent(pool: &SqlitePool, trade_id: i64, intent_id: i64) -> Result<u64> {
    let r = sqlx::query("UPDATE trades SET intent_id = ? WHERE id = ?")
        .bind(intent_id).bind(trade_id).execute(pool).await?;
    Ok(r.rows_affected())
}

// ── Reads ────────────────────────────────────────────────────────────────────

pub async fn get(pool: &SqlitePool, id: i64) -> Result<Option<HelmIntent>> {
    let row = sqlx::query(&format!("{SELECT_INTENT} WHERE id = ?"))
        .bind(id).fetch_optional(pool).await?;
    row.as_ref().map(intent_from_row).transpose()
}

/// A squadron's intents, newest first. `include_terminal` false returns only
/// the ones still open.
pub async fn list_for_squadron(pool: &SqlitePool, squadron_id: &str, include_terminal: bool) -> Vec<HelmIntent> {
    let q = if include_terminal {
        format!("{SELECT_INTENT} WHERE squadron_id = ? ORDER BY id DESC")
    } else {
        format!("{SELECT_INTENT} WHERE squadron_id = ? AND status NOT IN ('closed', 'superseded') ORDER BY id DESC")
    };
    sqlx::query(&q).bind(squadron_id).fetch_all(pool).await
        .map(|rows| rows.iter().filter_map(|r| intent_from_row(r).ok()).collect())
        .unwrap_or_default()
}

/// Every intent, newest first, capped.
pub async fn list_all(pool: &SqlitePool, limit: i64) -> Vec<HelmIntent> {
    sqlx::query(&format!("{SELECT_INTENT} ORDER BY id DESC LIMIT ?"))
        .bind(limit).fetch_all(pool).await
        .map(|rows| rows.iter().filter_map(|r| intent_from_row(r).ok()).collect())
        .unwrap_or_default()
}

pub async fn revisions(pool: &SqlitePool, intent_id: i64) -> Vec<IntentRevision> {
    sqlx::query("SELECT * FROM helm_intent_revisions WHERE intent_id = ? ORDER BY version ASC")
        .bind(intent_id).fetch_all(pool).await
        .map(|rows| rows.iter().filter_map(|r| Some(IntentRevision {
            id: r.try_get("id").ok()?,
            intent_id: r.try_get("intent_id").ok()?,
            version: r.try_get("version").ok()?,
            reason: r.try_get("reason").ok()?,
            content: content_from_row(r).ok()?,
            created_at: r.try_get("created_at").ok()?,
        })).collect())
        .unwrap_or_default()
}

pub async fn events(pool: &SqlitePool, intent_id: i64) -> Vec<IntentEvent> {
    sqlx::query("SELECT * FROM helm_intent_events WHERE intent_id = ? ORDER BY id ASC")
        .bind(intent_id).fetch_all(pool).await
        .map(|rows| rows.iter().filter_map(|r| {
            let to: String = r.try_get("to_status").ok()?;
            let from: Option<String> = r.try_get("from_status").ok()?;
            Some(IntentEvent {
                id: r.try_get("id").ok()?,
                intent_id: r.try_get("intent_id").ok()?,
                at: r.try_get("at").ok()?,
                from_status: from.and_then(|s| IntentStatus::parse(&s)),
                to_status: IntentStatus::parse(&to)?,
                detail: r.try_get("detail").ok()?,
            })
        }).collect())
        .unwrap_or_default()
}

/// The content the engine reads: version 1 unless a revision superseded it.
pub async fn current_content(pool: &SqlitePool, intent: &HelmIntent) -> IntentContent {
    if intent.current_version <= 1 {
        return intent.first.clone();
    }
    revisions(pool, intent.id).await.into_iter()
        .find(|r| r.version == intent.current_version)
        .map(|r| r.content)
        .unwrap_or_else(|| intent.first.clone())
}

/// How many intents the squadron has and how many are still open. The
/// retirement check reads this; see `IntentTally::complete`.
pub async fn squadron_tally(pool: &SqlitePool, squadron_id: &str) -> IntentTally {
    sqlx::query(
        "SELECT COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN status NOT IN ('closed', 'superseded') THEN 1 ELSE 0 END), 0) AS open
         FROM helm_intents WHERE squadron_id = ?"
    )
    .bind(squadron_id).fetch_one(pool).await
    .map(|r| IntentTally {
        total: r.try_get::<i64, _>("total").unwrap_or(0),
        open: r.try_get::<i64, _>("open").unwrap_or(0),
    })
    .unwrap_or_default()
}

/// The condition id a deployed squadron was given, from the deployment row
/// that produced it. The CAG registry does not keep it.
pub async fn market_id_for_squadron(pool: &SqlitePool, squadron_id: &str) -> Option<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT market_id FROM deployment_queue WHERE squadron_id = ? ORDER BY created_at DESC LIMIT 1"
    )
    .bind(squadron_id).fetch_optional(pool).await.ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn pool() -> SqlitePool {
        let p = SqlitePoolOptions::new().max_connections(1).connect("sqlite::memory:").await.unwrap();
        crate::helpers::db::init_schema(&p).await.expect("schema");
        p
    }

    fn content() -> IntentContent {
        IntentContent {
            thesis: "BTC is pinned under the strike into the close".into(),
            confidence: 0.7,
            horizon: Horizon::Expiry,
            falsification: "a print above $0.55 on the YES token".into(),
            entry_kind: EntryKind::Taker,
            entry_limit_price: None,
            size_usdc: dec!(4),
            stop_price: Some(dec!(0.55)),
            take_profit_price: None,
            time_limit_at: None,
            hold_to_settlement: false,
            catastrophic_floor_pct: None,
        }
    }

    fn new_intent(squadron: &str) -> NewIntent {
        NewIntent {
            squadron_id: squadron.into(),
            market_id: "0xcond".into(),
            market_name: "Bitcoin Up or Down - October 2, 3PM ET".into(),
            side: "no".into(),
            content: content(),
            ghost: true,
            venue: "intl_clob".into(),
        }
    }

    // ── The machine ─────────────────────────────────────────────────────

    /// Every allowed edge, and nothing else. Written out rather than derived
    /// so a change to the machine has to be made in two places on purpose.
    #[test]
    fn the_status_machine_is_exactly_as_specified() {
        use IntentStatus::*;
        let allowed: &[(IntentStatus, IntentStatus)] = &[
            (Proposed, Acknowledged), (Proposed, Closed), (Proposed, Superseded),
            (Acknowledged, Working), (Acknowledged, Closed), (Acknowledged, Superseded),
            (Working, Filled), (Working, Partial), (Working, Missed), (Working, Closed), (Working, Superseded),
            (Filled, Closed), (Filled, Superseded),
            (Partial, Filled), (Partial, Closed), (Partial, Superseded),
            (Missed, Closed), (Missed, Superseded),
        ];
        for from in IntentStatus::ALL {
            for to in IntentStatus::ALL {
                let expected = allowed.contains(&(from, to));
                assert_eq!(from.can_transition_to(to), expected, "{} → {}", from.as_str(), to.as_str());
            }
        }
    }

    #[test]
    fn terminal_statuses_have_no_exit() {
        for from in [IntentStatus::Closed, IntentStatus::Superseded] {
            assert!(from.is_terminal());
            for to in IntentStatus::ALL {
                assert!(!from.can_transition_to(to), "{} must not leave", from.as_str());
            }
        }
    }

    #[test]
    fn status_round_trips_through_its_string() {
        for s in IntentStatus::ALL {
            assert_eq!(IntentStatus::parse(s.as_str()), Some(s));
        }
        assert_eq!(IntentStatus::parse(" Filled "), Some(IntentStatus::Filled));
        assert_eq!(IntentStatus::parse("done"), None);
    }

    // ── Shape ───────────────────────────────────────────────────────────

    #[test]
    fn a_well_formed_intent_passes_the_shape_check() {
        assert_eq!(shape_check("YES", &content()), Ok(()));
    }

    #[test]
    fn the_shape_check_names_every_defect_at_once() {
        let mut c = content();
        c.confidence = 1.0;
        c.thesis = "  ".into();
        c.size_usdc = dec!(0);
        c.entry_kind = EntryKind::Resting; // and no limit price
        c.stop_price = Some(dec!(1.2));
        let errs = shape_check("maybe", &c).unwrap_err();
        let joined = errs.join("\n");
        for needle in ["side must be", "thesis is empty", "confidence must be", "size_usdc", "resting entry needs", "stop_price must be"] {
            assert!(joined.contains(needle), "missing {needle:?} in {joined}");
        }
        assert_eq!(errs.len(), 6);
    }

    #[test]
    fn a_taker_entry_rejects_a_limit_price() {
        let mut c = content();
        c.entry_limit_price = Some(dec!(0.5));
        assert!(shape_check("YES", &c).is_err());
    }

    // ── Storage ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_opens_as_proposed_and_records_the_event() {
        let p = pool().await;
        let id = create(&p, &new_intent("helm-open-trial")).await.unwrap();
        let it = get(&p, id).await.unwrap().unwrap();
        assert_eq!(it.status, IntentStatus::Proposed);
        assert_eq!(it.side, "NO", "side is normalized");
        assert_eq!(it.current_version, 1);
        assert_eq!(it.first, content());
        assert!(it.ghost);
        let ev = events(&p, id).await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].from_status, None);
        assert_eq!(ev[0].to_status, IntentStatus::Proposed);
    }

    #[tokio::test]
    async fn a_malformed_intent_is_refused_at_the_write() {
        let p = pool().await;
        let mut n = new_intent("s");
        n.content.confidence = 0.0;
        assert!(create(&p, &n).await.is_err());
        assert!(list_all(&p, 10).await.is_empty());
    }

    #[tokio::test]
    async fn transitions_follow_the_machine_and_stamp_the_row() {
        let p = pool().await;
        let id = create(&p, &new_intent("s")).await.unwrap();
        // Skipping acknowledgement is refused.
        assert!(transition(&p, id, IntentStatus::Working, None).await.is_err());
        let it = transition(&p, id, IntentStatus::Acknowledged, Some("read the critique")).await.unwrap();
        assert!(it.acknowledged_at.is_some());
        assert_eq!(it.status_detail.as_deref(), Some("read the critique"));
        let it = transition(&p, id, IntentStatus::Closed, Some("cancelled by operator")).await.unwrap();
        assert_eq!(it.status, IntentStatus::Closed);
        assert_eq!(it.close_reason.as_deref(), Some("cancelled by operator"));
        assert!(it.closed_at.is_some());
        // Terminal: nothing moves it, including a second close.
        assert!(transition(&p, id, IntentStatus::Closed, None).await.is_err());
        assert!(transition(&p, id, IntentStatus::Acknowledged, None).await.is_err());
        assert_eq!(events(&p, id).await.len(), 3);
    }

    /// The rule the module exists for: the first submission is what the
    /// retrospective sees, however many times the operator edits afterwards.
    #[tokio::test]
    async fn revising_never_touches_the_first_submission() {
        let p = pool().await;
        let id = create(&p, &new_intent("s")).await.unwrap();
        let mut edited = content();
        edited.thesis = "actually, pinned under the strike AND funding is negative".into();
        edited.confidence = 0.6;
        edited.stop_price = Some(dec!(0.52));
        let v = revise(&p, id, &edited, "tightened after reading the critique").await.unwrap();
        assert_eq!(v, 2);

        let it = get(&p, id).await.unwrap().unwrap();
        assert_eq!(it.first, content(), "version 1 is byte-identical");
        assert_eq!(it.current_version, 2);
        assert_eq!(current_content(&p, &it).await, edited, "the engine reads version 2");

        let revs = revisions(&p, id).await;
        assert_eq!(revs.len(), 1);
        assert_eq!(revs[0].version, 2);
        assert_eq!(revs[0].reason, "tightened after reading the critique");

        // A revision without a reason, or on a closed intent, is refused.
        assert!(revise(&p, id, &edited, "   ").await.is_err());
        transition(&p, id, IntentStatus::Closed, Some("cancelled")).await.unwrap();
        assert!(revise(&p, id, &edited, "too late").await.is_err());
    }

    #[tokio::test]
    async fn superseding_closes_the_old_and_opens_the_new_as_proposed() {
        let p = pool().await;
        let old = create(&p, &new_intent("s")).await.unwrap();
        transition(&p, old, IntentStatus::Acknowledged, None).await.unwrap();
        let mut n = new_intent("s");
        n.content.stop_price = None;
        n.content.hold_to_settlement = true;
        let new = supersede(&p, old, &n, "loosening: the thesis is now settlement-only").await.unwrap();
        let o = get(&p, old).await.unwrap().unwrap();
        let nw = get(&p, new).await.unwrap().unwrap();
        assert_eq!(o.status, IntentStatus::Superseded);
        assert_eq!(o.superseded_by, Some(new));
        assert_eq!(nw.status, IntentStatus::Proposed, "the critique runs again on the new thesis");
        assert!(nw.first.hold_to_settlement);
        // A superseded intent cannot be superseded again.
        assert!(supersede(&p, old, &n, "again").await.is_err());
    }

    /// The retirement signal. A squadron with no intents is not complete;
    /// one with any open intent is not complete; one whose every intent is
    /// terminal is.
    #[tokio::test]
    async fn the_tally_says_when_a_squadron_is_complete() {
        let p = pool().await;
        assert_eq!(squadron_tally(&p, "s").await, IntentTally { total: 0, open: 0 });
        assert!(!squadron_tally(&p, "s").await.complete(), "no intents is not complete");

        let a = create(&p, &new_intent("s")).await.unwrap();
        let b = create(&p, &new_intent("s")).await.unwrap();
        create(&p, &new_intent("other-squadron")).await.unwrap();
        assert_eq!(squadron_tally(&p, "s").await, IntentTally { total: 2, open: 2 });

        transition(&p, a, IntentStatus::Closed, Some("cancelled")).await.unwrap();
        assert_eq!(squadron_tally(&p, "s").await, IntentTally { total: 2, open: 1 });
        assert!(!squadron_tally(&p, "s").await.complete());

        transition(&p, b, IntentStatus::Superseded, Some("x")).await.unwrap();
        let t = squadron_tally(&p, "s").await;
        assert_eq!(t, IntentTally { total: 2, open: 0 });
        assert!(t.complete());
        assert_eq!(list_for_squadron(&p, "s", false).await.len(), 0);
        assert_eq!(list_for_squadron(&p, "s", true).await.len(), 2);
    }

    /// The join columns exist on both ledgers and round-trip an id. Nothing
    /// writes them yet; the entry increment will.
    #[tokio::test]
    async fn positions_and_trades_carry_an_intent_id() {
        let p = pool().await;
        let id = create(&p, &new_intent("s")).await.unwrap();
        sqlx::query(
            "INSERT INTO open_positions (ts, session_id, strategy, token_id, market, side, entry_price, shares)
             VALUES ('t', 's', 'HelmStrategy', 'tok', 'm', 'NO', '0.45', '8')"
        ).execute(&p).await.unwrap();
        assert_eq!(position_intent(&p, "tok", "HelmStrategy").await, None);
        assert_eq!(set_position_intent(&p, "tok", "HelmStrategy", id).await.unwrap(), 1);
        assert_eq!(position_intent(&p, "tok", "HelmStrategy").await, Some(id));

        let trade_id = sqlx::query(
            "INSERT INTO trades (ts, strategy, market, side, entry_price, exit_price, shares, pnl, reason)
             VALUES ('t', 'HelmStrategy', 'm', 'NO', '0.45', '0.50', '8', '0.4', 'test')"
        ).execute(&p).await.unwrap().last_insert_rowid();
        assert_eq!(set_trade_intent(&p, trade_id, id).await.unwrap(), 1);
        let got: Option<i64> = sqlx::query_scalar("SELECT intent_id FROM trades WHERE id = ?")
            .bind(trade_id).fetch_one(&p).await.unwrap();
        assert_eq!(got, Some(id));
    }

    /// Schema init runs on every start; a second run must be a no-op.
    #[tokio::test]
    async fn init_schema_is_idempotent() {
        let p = pool().await;
        init_schema(&p).await.unwrap();
        init_schema(&p).await.unwrap();
        let id = create(&p, &new_intent("s")).await.unwrap();
        assert!(get(&p, id).await.unwrap().is_some());
    }
}
