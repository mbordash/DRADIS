//! Polymarket Data API v2: the wallet's positions and redemptions.
//!
//! Polymarket retired Data API v1 on 2026-11-02; every v1 route answers 410.
//! polymarket_client_sdk_v2 (0.7.0 and 0.9.0 alike) still calls v1, so the
//! three readers that used its data client (chain-sync, auto-settle and orphan
//! detection) read v2 here instead.
//!
//! v2 differs from v1 in ways that matter to those readers:
//! - every response is `{"data": [...], "pagination": {...}}`, paged by an
//!   opaque cursor only (an `offset` parameter is a 400);
//! - fields are snake_case, and a position's `asset`/`size`/`curPrice` are now
//!   `token_id`/`current_size`/`current_price`;
//! - the default positions floor is 0.1 shares (v1: 1 share) and v1 returned
//!   at most 100 positions with no paging, so v2 sees small holdings and large
//!   wallets that v1 left out.
//!
//! A reader that misses a holding can mistake it for a sold one and purge its
//! row (see `sync_open_positions_with_chain`), so a response this module cannot
//! fully read is an error, never a shorter list.

use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use anyhow::{anyhow, bail, Context, Result};
use rust_decimal::Decimal;
use serde_json::Value;

use crate::helpers::clob_public::{data_page, DATA_API_V2};

/// Rows per page; the API's maximum is 1000.
const PAGE_SIZE: usize = 500;
/// Pages read before giving up. 10,000 positions is far past any wallet this
/// engine runs; hitting the bound is an error rather than a truncated list.
const MAX_PAGES: usize = 20;
const USER_AGENT: &str = "Mozilla/5.0 (compatible; dradis-engine/1.0)";

/// Position systems have different condition widths and settlement contracts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PositionSystem { Ctf, ProtocolV2 }

/// One open (or redeemable) position, with the fields the readers use. Names
/// follow the SDK's v1 `Position` so the readers did not change shape.
#[derive(Clone, Debug, PartialEq)]
pub struct Position {
    /// The outcome token (v2 `token_id`).
    pub asset: U256,
    /// CTF uses all 32 bytes. Protocol V2 stores its 31 bytes followed by zero;
    /// `position_system` must be checked before choosing a settlement ABI.
    pub condition_id: B256,
    pub position_system: PositionSystem,
    /// Shares held now (v2 `current_size`).
    pub size: Decimal,
    pub avg_price: Decimal,
    pub current_value: Decimal,
    /// Last price (v2 `current_price`).
    pub cur_price: Decimal,
    pub redeemable: bool,
    pub negative_risk: bool,
    pub title: String,
    pub outcome: String,
    pub outcome_index: i32,
}

/// One activity row: its type as the API spells it ("REDEEM", ...) and the
/// token it moved, when it names one.
#[derive(Clone, Debug, PartialEq)]
pub struct Activity {
    pub activity_type: String,
    pub asset: Option<U256>,
}

fn http() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_default()
    })
}

/// A JSON number (or numeric string) as the decimal its text spells, not an
/// f64 round trip.
fn dec(v: &Value) -> Option<Decimal> {
    match v {
        Value::Number(n) => Decimal::from_str(&n.to_string())
            .or_else(|_| Decimal::from_scientific(&n.to_string()))
            .ok(),
        Value::String(s) => Decimal::from_str(s.trim()).ok(),
        _ => None,
    }
}

fn token(v: &Value) -> Option<U256> {
    let s = v.as_str()?.trim();
    if s.is_empty() { return None; }
    U256::from_str_radix(s, 10).ok()
}

/// A position row, or why it cannot be read.
pub fn parse_position(r: &Value) -> Result<Position> {
    let field = |k: &str| r.get(k).ok_or_else(|| anyhow!("position row has no `{k}`"));
    let num = |k: &str| -> Result<Decimal> {
        dec(field(k)?).ok_or_else(|| anyhow!("position `{k}` is not a number: {}", r[k]))
    };
    let text = |k: &str| -> Result<String> {
        Ok(field(k)?.as_str().ok_or_else(|| anyhow!("position `{k}` is not a string"))?.to_string())
    };
    let flag = |k: &str| -> Result<bool> {
        field(k)?.as_bool().ok_or_else(|| anyhow!("position `{k}` is not a bool"))
    };
    let asset = token(field("token_id")?).ok_or_else(|| anyhow!("position token_id unreadable"))?;
    let raw_condition = text("condition_id")?;
    let condition_hex = raw_condition.strip_prefix("0x").unwrap_or(&raw_condition);
    let declared_system = match r.get("version") {
        None | Some(Value::Null) => None,
        Some(Value::String(version)) if version == "v1" => Some(PositionSystem::Ctf),
        Some(Value::String(version)) if version == "v2" => Some(PositionSystem::ProtocolV2),
        _ => anyhow::bail!("position declares an unsupported protocol version"),
    };
    let (condition_id, position_system) = match condition_hex.len() {
        64 => {
            anyhow::ensure!(declared_system != Some(PositionSystem::ProtocolV2),
                "Protocol V2 condition must contain exactly 31 bytes");
            (B256::from_str(&raw_condition).context("position condition_id")?, PositionSystem::Ctf)
        }
        62 => {
            anyhow::ensure!(declared_system != Some(PositionSystem::Ctf),
                "CTF condition must contain exactly 32 bytes");
            let normalized = B256::from_str(&format!("0x{condition_hex}00")).context("position V2 condition_id")?;
            let bytes = asset.to_be_bytes::<32>();
            anyhow::ensure!(&bytes[..31] == &normalized.as_slice()[..31] && bytes[31] <= 1,
                "Protocol V2 token does not match its condition/outcome");
            (normalized, PositionSystem::ProtocolV2)
        }
        _ => anyhow::bail!("position condition has an unsupported width"),
    };
    let outcome_index = field("outcome_index")?.as_i64().ok_or_else(|| anyhow!("position outcome_index unreadable"))?;
    anyhow::ensure!((0..=1).contains(&outcome_index), "unsupported binary outcome index");
    if position_system == PositionSystem::ProtocolV2 {
        anyhow::ensure!(asset.to_be_bytes::<32>()[31] == outcome_index as u8,
            "Protocol V2 outcome index does not match its token");
    }
    Ok(Position {
        asset,
        condition_id,
        position_system,
        size: num("current_size")?,
        avg_price: num("avg_price")?,
        current_value: num("current_value")?,
        cur_price: num("current_price")?,
        redeemable: flag("redeemable")?,
        negative_risk: flag("negative_risk")?,
        title: text("title")?,
        outcome: text("outcome")?,
        outcome_index: outcome_index as i32,
    })
}

/// An activity row. Only the type is required: a row without a token (a
/// deposit, say) has `asset: None`.
pub fn parse_activity(r: &Value) -> Result<Activity> {
    let activity_type = r.get("type").and_then(Value::as_str)
        .ok_or_else(|| anyhow!("activity row has no `type`"))?
        .to_string();
    Ok(Activity { activity_type, asset: r.get("token_id").and_then(token) })
}

/// Every page of a v2 list route, each row read by `parse`. `params` are sent
/// on every page: the cursor carries position and size, not the filters.
async fn read_all<T>(route: &str, params: &[(&str, String)], parse: fn(&Value) -> Result<T>) -> Result<Vec<T>> {
    let base = format!("{DATA_API_V2}/{route}");
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let mut q: Vec<(&str, String)> = params.to_vec();
        match &cursor {
            Some(c) => q.push(("cursor", c.clone())),
            None => q.push(("limit", PAGE_SIZE.to_string())),
        }
        let page: Value = http().get(&base).query(&q).send().await?
            .error_for_status()?
            .json().await?;
        if page.get("data").is_some_and(|d| !d.is_null() && !d.is_array()) {
            bail!("{route}: `data` is not a list");
        }
        let (rows, next) = data_page(&page);
        for r in rows {
            out.push(parse(r).with_context(|| format!("{route} row"))?);
        }
        match next {
            Some(c) => cursor = Some(c),
            None => return Ok(out),
        }
    }
    bail!("{route}: more than {MAX_PAGES} pages of {PAGE_SIZE}")
}

/// The wallet's open positions, redeemable ones included (v2's default
/// `OPEN` status is the superset).
pub async fn positions(user: Address) -> Result<Vec<Position>> {
    read_all("positions", &[("user", user.to_string())], parse_position).await
}

/// The wallet's REDEEM activity since `start` (epoch seconds).
pub async fn redemptions(user: Address, start: u64) -> Result<Vec<Activity>> {
    read_all(
        "activity",
        &[("user", user.to_string()), ("type", "REDEEM".to_string()), ("start", start.max(1).to_string())],
        parse_activity,
    ).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    /// A row as `/v2/positions` returned it on 2026-11-02.
    fn row() -> Value {
        json!({"proxy_wallet":"0x0000000000000000000000000000000000000001",
            "token_id":"29677413448356631423250052573262008356913370083986817030510370064664558041792",
            "condition_id":"0x87822b3d4ccba1835d698354b01c050b55f20c5f34b14908bff4ea5a6d58af4c",
            "current_size":10.0,"avg_price":0.5203,"entry_cost_usdc":0.0,"entry_fees_usdc":0.0,
            "total_cost_usdc":0.0,"current_price":1.0,"current_value":10.0,"total_size":0.0,
            "realized_pnl":0.0,"unrealized_pnl":10.0,"total_pnl":10.0,"percent_pnl":0.0,
            "percent_realized_pnl":0.0,"status":"REDEEMABLE","redeemable":true,"mergeable":false,
            "negative_risk":true,"archived":false,"title":"Will Mark Carney be the next Canadian Prime Minister?",
            "slug":"s","icon":"","event_id":"16125","event_slug":"e","outcome":"Yes","outcome_index":0,
            "opposite_outcome":"No","opposite_token_id":"1","end_date":"","last_event_at":0,
            "first_entry_at":0,"name":"","profile_image":"","verified":false})
    }

    #[test]
    fn a_v2_position_row_reads_into_the_fields_the_readers_use() {
        let p = parse_position(&row()).unwrap();
        assert_eq!(p.asset.to_string(), "29677413448356631423250052573262008356913370083986817030510370064664558041792");
        assert_eq!(format!("{:#x}", p.condition_id), "0x87822b3d4ccba1835d698354b01c050b55f20c5f34b14908bff4ea5a6d58af4c");
        assert_eq!(p.size, dec!(10));
        assert_eq!(p.avg_price, dec!(0.5203));
        assert_eq!(p.cur_price, dec!(1));
        assert!(p.redeemable && p.negative_risk);
        assert_eq!((p.outcome.as_str(), p.outcome_index), ("Yes", 0));
    }

    #[test]
    fn protocol_v2_preserves_condition_width_and_validates_token_outcome() {
        let mut value = row();
        let condition = "00".repeat(30) + "ab";
        let normalized: B256 = format!("0x{condition}00").parse().unwrap();
        let token = U256::from_be_bytes(normalized.0);
        value["condition_id"] = json!(format!("0x{condition}"));
        value["version"] = json!("v2");
        value["token_id"] = json!(token.to_string());
        value["outcome_index"] = json!(0);
        let parsed = parse_position(&value).unwrap();
        assert_eq!(parsed.position_system, PositionSystem::ProtocolV2);
        assert_eq!(parsed.condition_id, normalized);
        value["outcome_index"] = json!(1);
        assert!(parse_position(&value).is_err());
        value["token_id"] = json!((token + U256::from(1)).to_string());
        assert!(parse_position(&value).is_ok());
        value["token_id"] = json!((token + U256::from(2)).to_string());
        assert!(parse_position(&value).is_err());
        value["condition_id"] = json!(format!("0x{}", "11".repeat(32)));
        assert!(parse_position(&value).is_err());
    }

    #[test]
    fn unsupported_or_conflicting_protocol_metadata_is_an_error() {
        let mut value = row();
        value["version"] = json!("v1");
        assert_eq!(parse_position(&value).unwrap().position_system, PositionSystem::Ctf);
        for version in [json!("v3"), json!("unknown"), json!(2)] {
            value["version"] = version;
            assert!(parse_position(&value).is_err());
        }
        let condition = "00".repeat(30) + "ab";
        let token = U256::from_be_bytes(format!("0x{condition}00").parse::<B256>().unwrap().0);
        value["condition_id"] = json!(format!("0x{condition}"));
        value["token_id"] = json!(token.to_string());
        value["version"] = json!("v1");
        assert!(parse_position(&value).is_err());
        value.as_object_mut().unwrap().remove("version");
        assert_eq!(parse_position(&value).unwrap().position_system, PositionSystem::ProtocolV2);
    }

    /// A row missing a field is an error, not a row skipped: a skipped holding
    /// reads as a sold one to chain-sync, which would purge it.
    #[test]
    fn a_v1_shaped_or_broken_row_is_an_error_not_a_skip() {
        let v1 = json!({"asset":"1","conditionId":"0x00","size":1.0,"curPrice":0.5});
        assert!(parse_position(&v1).is_err());
        let mut r = row();
        r["current_size"] = json!("lots");
        assert!(parse_position(&r).is_err());
    }

    #[test]
    fn activity_rows_keep_their_type_and_token() {
        let a = parse_activity(&json!({"type":"REDEEM","token_id":"42"})).unwrap();
        assert_eq!(a, Activity { activity_type: "REDEEM".into(), asset: Some(U256::from(42u8)) });
        assert_eq!(parse_activity(&json!({"type":"DEPOSIT","token_id":""})).unwrap().asset, None);
        assert!(parse_activity(&json!({"token_id":"1"})).is_err());
    }

    /// Live probe of the routes this module reads.
    /// `cargo test --release data_api_v2_live -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "hits the live Polymarket data API"]
    async fn data_api_v2_live() {
        let user: Address = std::env::var("DRADIS_PROBE_SAFE_ADDRESS")
            .unwrap_or_else(|_| "0x0000000000000000000000000000000000000001".into())
            .parse().unwrap();
        let p = positions(user).await.expect("v2 positions must read");
        println!("{} position(s)", p.len());
        let a = redemptions(user, 1).await.expect("v2 activity must read");
        assert!(a.iter().all(|a| a.activity_type == "REDEEM"));
        println!("{} redemption(s)", a.len());
    }
}
