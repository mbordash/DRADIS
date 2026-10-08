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

use anyhow::Result;
use reqwest;
use rust_decimal::Decimal;
use serde::Serialize;
use tracing::{error, info};
use crate::config;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use hmac::{Hmac, Mac};
use sha1::Sha1;

type HmacSha1 = Hmac<Sha1>;

// ── Telegram ────────────────────────────────────────────────────────────────

// ── Trade alerts ─────────────────────────────────────────────────────────────
//
// One line per fill, written for an operator reading it on a phone. The exit
// line used to carry the bid, the reason and the session total and nothing
// else: not which side was held, not what the trade itself made. A customer
// cannot tell a $0.60 win from a $1.10 loss until they open the Control Tower,
// which is the opposite of what an alert is for.

/// Everything the exit line says, gathered where the patrol settles the fill.
pub struct ExitAlert<'a> {
    pub strategy: &'a str,
    pub market: &'a str,
    /// "YES" or "NO".
    pub side: &'a str,
    /// Shares the venue actually sold. Zero is a killed FAK, not an exit.
    pub filled: Decimal,
    /// Shares the exit asked for (filled + whatever stays under management).
    pub requested: Decimal,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    /// Realized P&L of this trade after both fees.
    pub trade_pnl: Decimal,
    pub fees: Decimal,
    pub reason: &'a str,
    pub session_pnl: Decimal,
    /// Simulated fill: say so on the line, or a ghost run reads like real money.
    pub ghost: bool,
    /// A paired exit (TimeDecay, Arbitrage) sells the other leg in the same
    /// breath and books its P&L to the session too. `(pnl, fees)` of that leg,
    /// when it filled, so the line reports the pair and not one side of it.
    pub paired_leg: Option<(Decimal, Decimal)>,
}

/// What the ledger books is not always what the venue matched. A fill under
/// the venue minimum is not booked (no row, no session credit) and a remainder
/// under it is dropped rather than retained, so the alert reports booked
/// shares: `(filled, requested)` as the ledger sees them, with a sub-minimum
/// fill reading as no fill at all.
pub fn booked_exit_sizes(matched: Decimal, remainder: Decimal, min_shares: Decimal) -> (Decimal, Decimal) {
    if matched < min_shares {
        return (Decimal::ZERO, matched + remainder);
    }
    let kept = if remainder >= min_shares { remainder } else { Decimal::ZERO };
    (matched, matched + kept)
}

fn signed_dollars(x: Decimal) -> String {
    if x.is_sign_negative() { format!("-${:.4}", -x) } else { format!("+${:.4}", x) }
}

fn ghost_tag(ghost: bool) -> &'static str {
    if ghost { " | GHOST" } else { "" }
}

/// `🟢 ENTRY [Maker] <market> | YES 7.6 sh @ $0.6500`
pub fn entry_alert(strategy: &str, market: &str, side: &str, price: Decimal, shares: Decimal, ghost: bool) -> String {
    format!("🟢 ENTRY [{}] {} | {} {:.1} sh @ ${:.4}{}", strategy, market, side, shares, price, ghost_tag(ghost))
}

/// A filled exit names the side, the size, the round trip and the trade's own
/// P&L before the session total. A killed exit says so instead of reading as a
/// sale: `⚠️ EXIT NOT FILLED` with the bid it was sent at and the position kept.
pub fn exit_alert(a: &ExitAlert) -> String {
    if a.filled.is_zero() {
        return format!(
            "⚠️ EXIT NOT FILLED [{}] {} | {} {:.1} sh unsold at ${:.4} bid | position retained | {}{}",
            a.strategy, a.market, a.side, a.requested, a.exit_price, a.reason, ghost_tag(a.ghost),
        );
    }
    let size = if a.filled < a.requested {
        format!("{:.1} of {:.1} sh", a.filled, a.requested)
    } else {
        format!("{:.1} sh", a.filled)
    };
    let (trade, legs) = match a.paired_leg {
        Some((pair_pnl, pair_fees)) => (
            format!("trade {} both legs (fees ${:.4})", signed_dollars(a.trade_pnl + pair_pnl), a.fees + pair_fees),
            " + paired leg",
        ),
        None => (format!("trade {} (fees ${:.4})", signed_dollars(a.trade_pnl), a.fees), ""),
    };
    format!(
        "🔴 EXIT [{}] {} | {} {} ${:.4} → ${:.4}{} | {} | {} | session {}{}",
        a.strategy, a.market, a.side, size, a.entry_price, a.exit_price, legs,
        trade, a.reason, signed_dollars(a.session_pnl), ghost_tag(a.ghost),
    )
}

#[cfg(test)]
mod trade_alert_tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn exit() -> ExitAlert<'static> {
        ExitAlert {
            strategy: "GboostStrategy", market: "Bitcoin Up or Down - October 8, 7AM ET", side: "NO",
            filled: dec!(5.24), requested: dec!(5.24), entry_price: dec!(0.76), exit_price: dec!(0.90),
            trade_pnl: dec!(0.6578), fees: dec!(0.0812), reason: "take-profit", session_pnl: dec!(12.3456), ghost: false,
            paired_leg: None,
        }
    }

    /// TimeDecay trade #5 shape: the alerted leg lost $0.60 on its own while the
    /// pair netted something else. One leg's figure labeled "trade" misleads;
    /// the line reports the pair's total and says so.
    #[test]
    fn a_paired_exit_reports_both_legs_together() {
        let mut a = exit(); a.trade_pnl = dec!(-0.5951); a.fees = dec!(0.3751); a.paired_leg = Some((dec!(1.10), dec!(0.20)));
        let line = exit_alert(&a);
        assert!(line.contains("$0.7600 → $0.9000 + paired leg | trade +$0.5049 both legs (fees $0.5751) |"), "{line}");
    }

    /// The ledger books nothing under the venue minimum, so neither does the line.
    #[test]
    fn sub_minimum_fills_and_remainders_follow_the_ledger() {
        let min = dec!(1);
        assert_eq!(booked_exit_sizes(dec!(0.5), dec!(4.5), min), (dec!(0), dec!(5.0)), "a dust fill is no fill");
        assert_eq!(booked_exit_sizes(dec!(4.5), dec!(0.5), min), (dec!(4.5), dec!(4.5)), "a dust remainder is dropped, not retained");
        assert_eq!(booked_exit_sizes(dec!(3), dec!(4.6), min), (dec!(3), dec!(7.6)));
        assert_eq!(booked_exit_sizes(dec!(5), dec!(0), min), (dec!(5), dec!(5)));
    }

    /// The side, the round trip and the trade's own P&L are on the line, before
    /// the session total that used to be the only number.
    #[test]
    fn a_filled_exit_names_side_round_trip_and_trade_pnl() {
        let line = exit_alert(&exit());
        assert_eq!(
            line,
            "🔴 EXIT [GboostStrategy] Bitcoin Up or Down - October 8, 7AM ET | NO 5.2 sh $0.7600 → $0.9000 \
             | trade +$0.6578 (fees $0.0812) | take-profit | session +$12.3456"
        );
    }

    #[test]
    fn a_loss_carries_its_sign_on_both_figures() {
        let mut a = exit(); a.trade_pnl = dec!(-1.1892); a.session_pnl = dec!(-0.4); a.exit_price = dec!(0.56); a.reason = "stop";
        let line = exit_alert(&a);
        assert!(line.contains("$0.7600 → $0.5600 | trade -$1.1892 (fees $0.0812) | stop | session -$0.4000"), "{line}");
    }

    /// A FAK the venue killed sold nothing. Calling that an EXIT told the
    /// operator a position was closed that is still open.
    #[test]
    fn a_killed_exit_says_so_and_that_the_position_is_kept() {
        let mut a = exit(); a.filled = Decimal::ZERO; a.requested = dec!(7.6); a.exit_price = dec!(0.57);
        let line = exit_alert(&a);
        assert!(line.starts_with("⚠️ EXIT NOT FILLED [GboostStrategy]"), "{line}");
        assert!(line.contains("NO 7.6 sh unsold at $0.5700 bid | position retained | take-profit"), "{line}");
        assert!(!line.contains("trade "), "a killed exit has no trade P&L to report: {line}");
    }

    #[test]
    fn a_partial_fill_shows_sold_of_requested() {
        let mut a = exit(); a.filled = dec!(3); a.requested = dec!(7.6);
        assert!(exit_alert(&a).contains("| NO 3.0 of 7.6 sh $0.7600"), "{}", exit_alert(&a));
    }

    /// Simulated fills are tagged, on entry and exit alike.
    #[test]
    fn ghost_fills_are_tagged() {
        let mut a = exit(); a.ghost = true;
        assert!(exit_alert(&a).ends_with(" | GHOST"));
        assert!(!exit_alert(&exit()).contains("GHOST"));
        assert_eq!(
            entry_alert("MomentumStrategy", "M", "YES", dec!(0.65), dec!(7.6), true),
            "🟢 ENTRY [MomentumStrategy] M | YES 7.6 sh @ $0.6500 | GHOST"
        );
    }
}

#[derive(Serialize)]
struct TelegramMessage {
    chat_id: String,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_mode: Option<String>,
}

pub async fn send_notification(token: &str, chat_id: &str, message: &str) -> Result<()> {
    if !config::ENABLE_TELEGRAM || token.is_empty() || chat_id.is_empty() {
        return Ok(());
    }

    let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap_or_default();

    let payload = TelegramMessage {
        chat_id: chat_id.to_string(),
        text: message.to_string(),
        parse_mode: None,
    };

    let resp = client.post(&url)
        .json(&payload)
        .send()
        .await?;

    let status = resp.status();
    if status.is_success() {
        info!("📱 Telegram notification sent successfully");
        Ok(())
    } else {
        let err_body = resp.text().await.unwrap_or_default();
        error!("❌ Failed to send Telegram notification: HTTP {} - {}", status, err_body);
        Err(anyhow::anyhow!("Failed to send notification, status: {}", status))
    }
}

// ── Twitter / X ─────────────────────────────────────────────────────────────

/// Percent-encode a string per RFC 3986 (required by OAuth 1.0a).
fn pct(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// Build an `Authorization: OAuth …` header using HMAC-SHA1 (OAuth 1.0a).
/// The Twitter v2 REST endpoint uses JSON bodies, so only oauth params are
/// included in the signature base string (no body params).
fn oauth1_header(
    method: &str,
    url: &str,
    api_key: &str,
    api_secret: &str,
    access_token: &str,
    access_token_secret: &str,
) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Nonce: nanos is distinct enough for low-frequency tweet volume.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let nonce = format!("{}{:09}", ts, nanos);
    let ts_str = ts.to_string();

    let params: Vec<(&str, String)> = vec![
        ("oauth_consumer_key",     api_key.to_string()),
        ("oauth_nonce",            nonce.clone()),
        ("oauth_signature_method","HMAC-SHA1".to_string()),
        ("oauth_timestamp",        ts_str.clone()),
        ("oauth_token",            access_token.to_string()),
        ("oauth_version",          "1.0".to_string()),
    ];

    let mut sorted = params.clone();
    sorted.sort_by(|a, b| a.0.cmp(b.0));

    let param_str = sorted.iter()
        .map(|(k, v)| format!("{}={}", pct(k), pct(v)))
        .collect::<Vec<_>>()
        .join("&");

    let base = format!("{}&{}&{}", pct(method), pct(url), pct(&param_str));
    let signing_key = format!("{}&{}", pct(api_secret), pct(access_token_secret));

    let mut mac = HmacSha1::new_from_slice(signing_key.as_bytes())
        .expect("HMAC can take any key length");
    mac.update(base.as_bytes());
    let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());

    // Build the full Authorization header including the signature.
    let mut header_params = params;
    header_params.push(("oauth_signature", signature.clone()));
    header_params.sort_by(|a, b| a.0.cmp(b.0));

    let header_value = header_params.iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, pct(v)))
        .collect::<Vec<_>>()
        .join(", ");

    format!("OAuth {}", header_value)
}

/// Truncate a string to `max` chars (Unicode-aware), appending "…" if cut.
fn truncate(s: &str, max: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        s.to_string()
    } else {
        let cut: String = chars[..max - 1].iter().collect();
        format!("{}…", cut)
    }
}

/// Post a tweet to Twitter/X using the v2 API with OAuth 1.0a User Context.
///
/// Credentials come from env vars at startup:
///   `X_API_KEY`, `X_API_SECRET`,
///   `X_ACCESS_TOKEN`, `X_ACCESS_TOKEN_SECRET`
pub async fn post_tweet(
    api_key: &str,
    api_secret: &str,
    access_token: &str,
    access_token_secret: &str,
    text: &str,
) -> Result<()> {
    if !config::ENABLE_X
        || api_key.is_empty()
        || api_secret.is_empty()
        || access_token.is_empty()
        || access_token_secret.is_empty()
    {
        return Ok(());
    }

    // Twitter caps tweets at 280 chars; hard-truncate to be safe.
    let tweet_text = truncate(text, 280);

    let endpoint = "https://api.twitter.com/2/tweets";
    let auth = oauth1_header("POST", endpoint, api_key, api_secret, access_token, access_token_secret);

    let body = serde_json::json!({ "text": tweet_text });

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    let resp = client
        .post(endpoint)
        .header("Authorization", auth)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    let status = resp.status();
    if status.is_success() {
        info!("🐦 Tweet posted successfully");
        Ok(())
    } else {
        let err_body = resp.text().await.unwrap_or_default();
        error!("❌ Failed to post tweet: HTTP {} - {}", status, err_body);
        Err(anyhow::anyhow!("Tweet failed, status: {}", status))
    }
}

/// Post a single combined trade recap tweet on close (detached — never blocks the trading loop).
/// Includes entry price, exit price, reason, trade P&L, and running session P&L.
pub fn tweet_trade(
    tw_key: String, tw_secret: String, tw_token: String, tw_token_secret: String,
    strategy: String, market_name: String,
    entry_price: rust_decimal::Decimal, exit_price: rust_decimal::Decimal,
    reason: String, trade_pnl: rust_decimal::Decimal, session_pnl: rust_decimal::Decimal,
) {
    if !config::ENABLE_X { return; }
    tokio::spawn(async move {
        let name      = truncate(&market_name, 50);
        let strat     = truncate(&strategy, 20);
        let pnl_sign  = if trade_pnl   >= rust_decimal::Decimal::ZERO { "+" } else { "" };
        let sess_sign = if session_pnl >= rust_decimal::Decimal::ZERO { "+" } else { "" };
        let now = chrono::Utc::now().format("%m/%d %H:%M UTC");
        let text = format!(
            "📊 {strat} | {name}\nEntry ${entry_price:.2} → Exit ${exit_price:.2} | {reason}\nP&L: {pnl_sign}{trade_pnl:.2} | Session: {sess_sign}{session_pnl:.2}\n{now} | #polymarket #DRADIStrading",
        );
        let _ = post_tweet(&tw_key, &tw_secret, &tw_token, &tw_token_secret, &text).await;
    });
}
