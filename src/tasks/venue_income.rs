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

//! Venue income ledger ([E57]): credits Polymarket pays the wallet outside any
//! trade — maker rebates, taker rebates and rewards.
//!
//! Live session P&L is `total − starting`, so it absorbed these silently while
//! the summed-trades figure excluded them, and the two diverged by exactly the
//! rebate with nothing saying why. Recording them from the venue's own typed
//! activity rows, never from an unexplained collateral delta, lets the ledger
//! account for that gap without guessing.
//!
//! Read with a plain HTTP call rather than `polymarket_client_sdk_v2`'s
//! `activity()`. In SDK 0.7.0 `ActivityType::MakerRebate` serializes as
//! `MAKERREBATE`, which the API rejects ("invalid activity filter type
//! MAKERREBATE", 2026-09-18), and a rebate row carries empty strings in
//! `conditionId`, `asset` and `side`, fields the SDK types as `Option<B256>`,
//! `Option<U256>` and `Option<Side>`. Through the SDK this ledger would have
//! recorded nothing, with no error.

use std::str::FromStr;
use std::sync::{Mutex, OnceLock};

use alloy::primitives::Address;
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::helpers::db::{self, VenueCredit};

/// Activity types recorded as venue income, spelled as the API spells them.
pub const VENUE_INCOME_KINDS: [&str; 3] = ["MAKER_REBATE", "TAKER_REBATE", "REWARD"];

/// Data API v2 (v1 retired 2026-11-02).
fn activity_url() -> String { format!("{}/activity", crate::helpers::clob_public::DATA_API_V2) }
/// Rebates are paid once a day per epoch, so an hourly read is timely enough.
const POLL_SECS: u64 = 3600;
const PAGE_SIZE: usize = 500;
/// Bound on one poll's pagination. Seven credits existed after five months of
/// trading, so this is room for decades, not a practical limit.
const MAX_PAGES: usize = 20;

fn last_poll_cell() -> &'static Mutex<Option<DateTime<Utc>>> {
    static CELL: OnceLock<Mutex<Option<DateTime<Utc>>>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new(None))
}

/// When the ledger last completed a read of the venue, or `None` before the
/// first one. The Control Tower renders "not read yet" rather than $0.00 until
/// this is set, since an empty ledger and an unread one are different facts.
pub fn last_polled_at() -> Option<DateTime<Utc>> {
    *last_poll_cell().lock().unwrap_or_else(|p| p.into_inner())
}

/// Venue-income credits from one page of the `/v2/activity` response. Rows of
/// any other type, or missing an amount, timestamp or transaction hash, are
/// skipped.
pub fn parse_credits(page: &serde_json::Value) -> Vec<VenueCredit> {
    crate::helpers::clob_public::data_page(page).0.iter()
        .filter_map(|r| {
            let kind = r.get("type")?.as_str()?;
            if !VENUE_INCOME_KINDS.contains(&kind) {
                return None;
            }
            // The number's own text ("1.2592"), not an f64 round trip.
            let amount = Decimal::from_str(&r.get("usdc_size")?.as_number()?.to_string()).ok()?;
            let credited_at = Utc.timestamp_opt(r.get("timestamp")?.as_i64()?, 0).single()?;
            let tx_hash = r.get("transaction_hash")?.as_str()?.to_string();
            if tx_hash.is_empty() {
                return None;
            }
            Some(VenueCredit { kind: kind.to_string(), amount, credited_at, tx_hash })
        })
        .collect()
}

/// Read every venue-income credit for `safe` and record the new ones.
/// Returns `(seen, newly_recorded)`.
///
/// The feed is newest-first with cursor paging; every poll rescans the whole
/// history (`start=1`: v2 otherwise floors to three years back), so a credit
/// missed on one pass is recorded on the next.
pub async fn poll_once(http: &reqwest::Client, safe: Address) -> anyhow::Result<(usize, usize)> {
    let pool = db::pool().ok_or_else(|| anyhow::anyhow!("no database pool yet"))?;
    let kinds = VENUE_INCOME_KINDS.join(",");
    let (mut seen, mut new) = (0usize, 0usize);
    let mut cursor: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let mut q: Vec<(&str, String)> = vec![
            ("user", safe.to_string()), ("type", kinds.clone()), ("start", "1".into()),
        ];
        match &cursor {
            Some(c) => q.push(("cursor", c.clone())),
            None => q.push(("limit", PAGE_SIZE.to_string())),
        }
        let resp = http.get(activity_url()).query(&q).send().await?.error_for_status()?;
        let page: serde_json::Value = resp.json().await?;
        for credit in parse_credits(&page) {
            seen += 1;
            if db::record_venue_credit(pool, crate::venues::intl::INTL_VENUE, &credit).await? {
                new += 1;
                info!(
                    "💸 Venue income recorded: {} ${} credited {} (wallet-level, not attributed to any viper)",
                    credit.kind, credit.amount, credit.credited_at.format("%Y-%m-%d %H:%M UTC"),
                );
            }
        }
        match crate::helpers::clob_public::data_page(&page).1 {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    Ok((seen, new))
}

/// Keep the venue-income ledger current: an immediate read (which backfills
/// the wallet's history on first run), then one per hour.
pub async fn run_venue_income_ledger(safe: Address) {
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .unwrap_or_default();
    loop {
        // A retired instance stops writing, so a migration backup copies a
        // database nothing is changing.
        if !crate::helpers::migration::is_retired() {
            match poll_once(&http, safe).await {
                Ok((seen, new)) => {
                    *last_poll_cell().lock().unwrap_or_else(|p| p.into_inner()) = Some(Utc::now());
                    if new > 0 {
                        info!("💸 Venue income ledger: {new} new credit(s), {seen} on record at the venue");
                    }
                }
                Err(e) => warn!("⚠️ Venue income ledger: read failed, retrying next hour: {e}"),
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(POLL_SECS)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// A real rebate row from the production wallet (2026-09-03), in the v2
    /// field names, with the empty `condition_id`, `token_id` and `side`
    /// strings a rebate carries, next to a trade row that must be ignored.
    #[test]
    fn parses_the_venue_rows_and_skips_trades() {
        let page = serde_json::json!({"data": [
            {"proxy_wallet":"0x3a4d0004ad7a5ff5805f6c54ee730e4b23836f30","timestamp":1788396306,
             "condition_id":"","type":"MAKER_REBATE","size":1.2592,"usdc_size":1.2592,
             "transaction_hash":"0xe69c56e6019aafae1e5148c060b087044c88e05beeb89a6b9ce0f53a9396a424",
             "price":0,"token_id":"","side":"","outcome_index":999},
            {"timestamp":1788396000,"type":"TRADE","usdc_size":3.05,"transaction_hash":"0xabc"},
            {"timestamp":1783633412,"type":"TAKER_REBATE","usdc_size":10,"transaction_hash":"0xdef"}
        ], "pagination": {"limit": 500, "offset": 0, "has_more": false, "next_cursor": null}});
        let credits = parse_credits(&page);
        assert_eq!(credits.len(), 2);
        assert_eq!(credits[0].kind, "MAKER_REBATE");
        assert_eq!(credits[0].amount, dec!(1.2592));
        assert_eq!(credits[0].credited_at.to_rfc3339(), "2026-09-03T00:45:06+00:00");
        assert_eq!(credits[1].kind, "TAKER_REBATE");
        assert_eq!(credits[1].amount, dec!(10));
    }

    #[test]
    fn rows_missing_what_the_ledger_keys_on_are_skipped() {
        let page = serde_json::json!({"data": [
            {"timestamp":1788396306,"type":"MAKER_REBATE","usdc_size":1.0,"transaction_hash":""},
            {"timestamp":1788396306,"type":"MAKER_REBATE","transaction_hash":"0x1"},
            {"type":"MAKER_REBATE","usdc_size":1.0,"transaction_hash":"0x2"}
        ], "pagination": {"limit": 500, "offset": 0, "has_more": false, "next_cursor": null}});
        assert!(parse_credits(&page).is_empty());
        assert!(parse_credits(&serde_json::json!({"error":"bad"})).is_empty());
    }

    /// A credit seen on every hourly poll is recorded once.
    #[tokio::test]
    async fn recording_is_idempotent_per_transaction() {
        let pool = db::memory_pool_for_tests().await;
        let c = VenueCredit {
            kind: "MAKER_REBATE".into(), amount: dec!(1.2592),
            credited_at: Utc.timestamp_opt(1788396306, 0).single().unwrap(), tx_hash: "0xe69c".into(),
        };
        assert!(db::record_venue_credit(&pool, "polymarket-intl", &c).await.unwrap());
        assert!(!db::record_venue_credit(&pool, "polymarket-intl", &c).await.unwrap());
        let rows = db::venue_income_rows(&pool).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].amount, "1.2592");
    }
}
