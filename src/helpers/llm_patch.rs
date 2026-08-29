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

//! LLM-authored config patch proposals — contract, parsing, validation (Epic S1).
//!
//! The LLM Advisor's report ends with a fenced ```json block:
//!
//! ```json
//! {"proposals":[{"field":"momentum_stop_loss_pct","to":0.08,"reason":"..."}]}
//! ```
//!
//! This module turns that raw text into validated, apply-ready patch changes:
//!
//!   1. [`extract_json_block`] — tolerant extraction (fenced block preferred,
//!      bare `{"proposals": …}` object as fallback; prose around it is fine).
//!   2. [`parse_proposals`] — serde parse into [`RawProposal`]s.
//!   3. [`validate_proposals`] — checks every proposal against the Control Tower
//!      schema registry (`api::config_schema`) and the live `DynamicConfig`:
//!      unknown keys are rejected, values are type-coerced to the field's JSON
//!      representation (Decimal fields are JSON strings), and numeric targets
//!      are clamped to schema `min`/`max` bounds (physical sanity limits that
//!      hold at every autonomy tier).
//!
//! A malformed or missing block is never an error for the advisor — the prose
//! report still flows to Telegram/DB; the proposal batch is simply empty, with
//! rejects recorded for the audit trail (Epic S2).
//!
//! Autonomy policy (tier gating, rate limits, delta caps, money-field guard)
//! deliberately does NOT live here — see the policy engine (Epic S3). This
//! module answers only: "is this a well-formed, in-bounds change to a real key?"

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::config_schema::{config_schema, ConfigFieldSchema};
use crate::helpers::dynamic_config::{build_cap_for, DynamicConfig};
use rust_decimal::prelude::ToPrimitive;

/// Hard cap on changes per advisory cycle — anything beyond is rejected.
/// Mirrors the "up to 4 recommendations" instruction in the system prompt.
pub const MAX_PROPOSALS_PER_CYCLE: usize = 4;

// ── Contract types ────────────────────────────────────────────────────────────

/// Envelope the LLM must emit inside the fenced json block.
#[derive(Deserialize)]
struct ProposalEnvelope {
    proposals: Vec<RawProposal>,
}

/// One proposed change, as authored by the LLM (untrusted).
#[derive(Debug, Clone, Deserialize)]
pub struct RawProposal {
    /// Exact `DynamicConfig` serde key (snake_case).
    pub field: String,
    /// Target value: JSON number, bool, or numeric string — coerced during validation.
    pub to: Value,
    /// Short rationale (surfaced in the approval UI / audit log).
    #[serde(default)]
    pub reason: String,
}

/// A proposal that survived validation and is ready for the policy engine.
#[derive(Debug, Clone, Serialize)]
pub struct ValidatedChange {
    /// `DynamicConfig` serde key — what a PATCH body uses.
    pub key: String,
    /// Current value, in the config's own JSON representation.
    pub from: Value,
    /// Target value, encoded to match the field's JSON representation
    /// (string for Decimal fields, bool for flags, number for integer fields).
    pub to: Value,
    /// True when the requested value fell outside schema bounds and was clamped.
    pub clamped: bool,
    /// Signed relative change for numeric fields, e.g. `0.25` = +25%.
    /// `None` for bools or when the current value is zero/non-numeric.
    pub delta_pct: Option<f64>,
    /// LLM's rationale.
    pub reason: String,
}

/// A proposal rejected during validation (kept for the audit trail).
#[derive(Debug, Clone, Serialize)]
pub struct RejectedProposal {
    pub field: String,
    pub to: Value,
    pub why: String,
}

/// Outcome of one advisory cycle's proposal block.
#[derive(Debug, Default, Serialize)]
pub struct ProposalBatch {
    pub accepted: Vec<ValidatedChange>,
    pub rejected: Vec<RejectedProposal>,
}

impl ProposalBatch {
    pub fn is_empty(&self) -> bool {
        self.accepted.is_empty() && self.rejected.is_empty()
    }

    /// JSON merge-patch body applying all accepted changes — the exact shape
    /// `DynamicConfig::apply_patch` / `PATCH /api/config` expects.
    pub fn patch_json(&self) -> Value {
        let mut map = serde_json::Map::new();
        for c in &self.accepted {
            map.insert(c.key.clone(), c.to.clone());
        }
        Value::Object(map)
    }

    /// Inverse merge-patch restoring the pre-apply values — stored alongside
    /// every applied action so it can be reverted with one call.
    pub fn inverse_patch_json(&self) -> Value {
        let mut map = serde_json::Map::new();
        for c in &self.accepted {
            map.insert(c.key.clone(), c.from.clone());
        }
        Value::Object(map)
    }
}

// ── Extraction ────────────────────────────────────────────────────────────────

/// Pull the proposals JSON out of the LLM's full text response.
///
/// Preference order:
///   1. A fenced ```json … ``` block (what the prompt demands).
///   2. Any fenced ``` … ``` block containing `"proposals"`.
///   3. A bare `{ … "proposals" … }` object — first `{` at/before the
///      `"proposals"` key, brace-balanced with string/escape awareness.
pub fn extract_json_block(text: &str) -> Option<String> {
    // 1 & 2: fenced blocks.
    let mut rest = text;
    while let Some(open) = rest.find("```") {
        let after = &rest[open + 3..];
        // Skip the info string (e.g. "json") up to the first newline.
        let body_start = after.find('\n').map(|i| i + 1).unwrap_or(0);
        let info = after[..body_start].trim().to_ascii_lowercase();
        let body = &after[body_start..];
        if let Some(close) = body.find("```") {
            let candidate = body[..close].trim();
            if (info == "json" || candidate.contains("\"proposals\"")) && !candidate.is_empty() {
                return Some(candidate.to_string());
            }
            rest = &body[close + 3..];
        } else {
            break;
        }
    }

    // 3: bare object.
    let key_pos = text.find("\"proposals\"")?;
    let start = text[..key_pos].rfind('{')?;
    balanced_object(&text[start..]).map(|s| s.to_string())
}

/// Return the brace-balanced JSON object at the start of `s`, if closed.
fn balanced_object(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    let mut depth = 0usize;
    let mut in_str = false;
    let mut escaped = false;
    for (i, &b) in bytes.iter().enumerate() {
        if in_str {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_str = false;
            }
            continue;
        }
        match b {
            b'"' => in_str = true,
            b'{' => depth += 1,
            b'}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(&s[..=i]);
                }
            }
            _ => {}
        }
    }
    None
}

/// Remove the proposals block (fenced or bare) from the prose report so the
/// Telegram message / dashboard card stays human-readable. No-op when absent.
pub fn strip_proposal_block(text: &str) -> String {
    let Some(block) = extract_json_block(text) else {
        return text.to_string();
    };
    // Remove the fenced wrapper too, if the block came from one.
    let stripped = if let Some(pos) = text.find(&block) {
        let before = &text[..pos];
        let after = &text[pos + block.len()..];
        // Trim a trailing ```json/``` fence pair around the block.
        let before = before
            .trim_end_matches(|c: char| c.is_whitespace())
            .trim_end_matches("```json")
            .trim_end_matches("```");
        let after = after.trim_start_matches(|c: char| c.is_whitespace() || c == '`');
        format!("{}\n{}", before.trim_end(), after.trim_start())
    } else {
        text.to_string()
    };
    stripped.trim().to_string()
}

// ── Parse + validate ──────────────────────────────────────────────────────────

/// Parse the extracted JSON into raw proposals. `Err` carries a short reason
/// suitable for the audit log.
pub fn parse_proposals(json_block: &str) -> Result<Vec<RawProposal>, String> {
    serde_json::from_str::<ProposalEnvelope>(json_block)
        .map(|e| e.proposals)
        .map_err(|e| format!("proposal JSON malformed: {e}"))
}

/// Validate raw proposals against the schema registry + live config.
///
/// Guarantees for every `accepted` entry:
///   * key exists in the Control Tower schema (so it's a real, editable field)
///   * `to` is encoded in the field's JSON representation and differs from `from`
///   * numeric values are finite and clamped to schema `min`/`max`
pub fn validate_proposals(raw: Vec<RawProposal>, current: &DynamicConfig) -> ProposalBatch {
    let schema = config_schema();
    let current_json = serde_json::to_value(current).unwrap_or(Value::Null);
    let mut batch = ProposalBatch::default();

    for (i, p) in raw.into_iter().enumerate() {
        if i >= MAX_PROPOSALS_PER_CYCLE {
            batch.rejected.push(RejectedProposal {
                field: p.field,
                to: p.to,
                why: format!("over the {MAX_PROPOSALS_PER_CYCLE}-change per-cycle limit"),
            });
            continue;
        }

        let Some(field) = schema.iter().find(|f| f.key == p.field) else {
            // Suggest the closest real key so the few-shot loop can teach the
            // model the correct name instead of letting it repeat the mistake.
            let why = match closest_key(&p.field, &schema) {
                Some(k) if schema.iter().any(|f| f.key == k && f.advanced) => format!(
                    "unknown field (not in config schema); closest is \"{k}\", which is advanced and not tunable by the advisor"
                ),
                Some(k) => format!("unknown field (not in config schema); did you mean \"{k}\"?"),
                None => "unknown field (not in config schema)".into(),
            };
            batch.rejected.push(RejectedProposal {
                field: p.field,
                to: p.to,
                why,
            });
            continue;
        };

        // Advanced fields are excluded from the Machine-Editable Keys list the
        // advisor is shown; enforce that policy here too so a correctly-spelled
        // advanced key can't slip through the tuner.
        if field.advanced {
            batch.rejected.push(RejectedProposal {
                field: p.field,
                to: p.to,
                why: "advanced field — not tunable by the advisor (human-only via the UI)".into(),
            });
            continue;
        }

        let Some(from) = current_json.get(field.key).cloned() else {
            batch.rejected.push(RejectedProposal {
                field: p.field,
                to: p.to,
                why: "field missing from live config (schema drift)".into(),
            });
            continue;
        };

        // Reject anything the build will not let take effect.
        //
        // Three fields are re-clamped to a compile-time constant on every config
        // read ("stricter wins", so a stale DB row cannot loosen a limit the
        // build has tightened). On 2026-08-29 an operator approved
        // `time_decay_max_entry_price -> 0.5` against a 0.46 cap on the live
        // Marketplace instance: it validated, it was written, the audit ledger
        // stamped it `applied`, and every reader went on seeing 0.46.
        //
        // Deliberately BEFORE `encode_target`, and against the raw proposed
        // value. The schema max now carries the cap too, so encoding would
        // quietly clamp 0.5 to 0.46 and the proposal would come out the far side
        // as "no-op (target equals current value)" — rejected, but for a reason
        // that teaches the advisor nothing. It re-proposes 0.5 next cycle.
        //
        // Rejecting rather than clamping silently: rejections are fed back to
        // the advisor, and an operator who approves a change is entitled to have
        // it mean something.
        if let Some(cap) = build_cap_for(field.key) {
            let over = cap
                .to_f64()
                .is_some_and(|c| coerce_number(&p.to).is_some_and(|n| n > c));
            if over {
                batch.rejected.push(RejectedProposal {
                    field: p.field,
                    to: p.to,
                    why: format!(
                        "capped at {cap} by a build safety limit — a higher value is \
                         written but never read, so this change cannot take effect"
                    ),
                });
                continue;
            }
        }

        match encode_target(field, &from, &p.to) {
            Ok((to, clamped)) => {
                if to == from {
                    batch.rejected.push(RejectedProposal {
                        field: p.field,
                        to: p.to,
                        why: "no-op (target equals current value)".into(),
                    });
                    continue;
                }
                let delta_pct = numeric_delta_pct(&from, &to);
                batch.accepted.push(ValidatedChange {
                    key: field.key.to_string(),
                    from,
                    to,
                    clamped,
                    delta_pct,
                    reason: p.reason,
                });
            }
            Err(why) => batch.rejected.push(RejectedProposal { field: p.field, to: p.to, why }),
        }
    }

    batch
}

/// One-call convenience for the advisor loop: extract → parse → validate.
/// The `Option` is `None` when the response simply contains no proposal block.
pub fn proposals_from_response(
    response_text: &str,
    current: &DynamicConfig,
) -> Option<Result<ProposalBatch, String>> {
    let block = extract_json_block(response_text)?;
    Some(parse_proposals(&block).map(|raw| validate_proposals(raw, current)))
}

// ── Coercion helpers ──────────────────────────────────────────────────────────

/// Best-effort nearest schema key for an unknown proposed field, used to make
/// rejection messages self-correcting via the few-shot loop. Prefers
/// prefix/containment matches (e.g. `maker_min_entry` → `maker_min_entry_price`),
/// then falls back to small edit distance.
fn closest_key(proposed: &str, schema: &[ConfigFieldSchema]) -> Option<&'static str> {
    let p = proposed.to_ascii_lowercase();
    if p.is_empty() {
        return None;
    }
    // Containment either way (truncated or over-specified names).
    if let Some(f) = schema
        .iter()
        .filter(|f| f.key.starts_with(&p) || p.starts_with(f.key) || f.key.contains(&p))
        .min_by_key(|f| f.key.len().abs_diff(p.len()))
    {
        return Some(f.key);
    }
    // Fall back to edit distance for typos (cap keeps suggestions sane).
    schema
        .iter()
        .map(|f| (edit_distance(&p, f.key), f.key))
        .filter(|(d, _)| *d <= 4)
        .min_by_key(|(d, _)| *d)
        .map(|(_, k)| k)
}

fn edit_distance(a: &str, b: &str) -> usize {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut cur = vec![i + 1];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            cur.push((prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1));
        }
        prev = cur;
    }
    prev[b.len()]
}

/// Encode the LLM's target value into the field's JSON representation,
/// clamping numerics to schema bounds. Returns `(encoded, was_clamped)`.
fn encode_target(field: &ConfigFieldSchema, from: &Value, to: &Value) -> Result<(Value, bool), String> {
    match from {
        Value::Bool(_) => match coerce_bool(to) {
            Some(b) => Ok((Value::Bool(b), false)),
            None => Err(format!("expected true/false for bool field, got {to}")),
        },
        Value::String(_) => {
            // Decimal fields serialize as JSON strings.
            let (n, clamped) = coerce_clamped_number(field, to)?;
            Ok((Value::String(format_decimal(n)), clamped))
        }
        Value::Number(_) => {
            // Integer fields (secs and similar) serialize as JSON numbers.
            let (n, clamped) = coerce_clamped_number(field, to)?;
            let rounded = n.round();
            let int = serde_json::Number::from(rounded as i64);
            Ok((Value::Number(int), clamped || (rounded - n).abs() > f64::EPSILON))
        }
        other => Err(format!("unsupported field representation: {other}")),
    }
}

fn coerce_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn coerce_number(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().trim_end_matches('%').parse::<f64>().ok(),
        _ => None,
    }
}

fn coerce_clamped_number(field: &ConfigFieldSchema, to: &Value) -> Result<(f64, bool), String> {
    let n = coerce_number(to).ok_or_else(|| format!("expected a number, got {to}"))?;
    if !n.is_finite() {
        return Err("value must be finite".into());
    }
    let mut clamped = n;
    if let Some(min) = field.min {
        clamped = clamped.max(min);
    }
    if let Some(max) = field.max {
        clamped = clamped.min(max);
    }
    Ok((clamped, (clamped - n).abs() > f64::EPSILON))
}

/// Format a float for a Decimal-typed JSON string without float artifacts
/// (e.g. 0.1+0.2 style tails) — round-trips through `rust_decimal`.
fn format_decimal(n: f64) -> String {
    use std::str::FromStr;
    // Shortest-repr f64 formatting is already artifact-free for values the
    // schema ranges allow; Decimal::from_str normalizes exponent forms.
    let s = format!("{n}");
    match rust_decimal::Decimal::from_str(&s) {
        Ok(d) => d.normalize().to_string(),
        Err(_) => s,
    }
}

/// Signed relative delta between two encoded values, when both are numeric
/// and the base is non-zero.
fn numeric_delta_pct(from: &Value, to: &Value) -> Option<f64> {
    let f = coerce_number(from)?;
    let t = coerce_number(to)?;
    if f == 0.0 {
        return None;
    }
    Some((t - f) / f.abs())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> DynamicConfig {
        DynamicConfig::default()
    }

    fn propose(field: &str, to: serde_json::Value) -> ProposalBatch {
        validate_proposals(
            vec![RawProposal { field: field.into(), to, reason: "t".into() }],
            &cfg(),
        )
    }

    /// The live regression. On 2026-08-29 the advisor proposed
    /// `time_decay_max_entry_price: 0.46 -> 0.5` on the Marketplace instance.
    /// The schema range is 0.0 to 1.0 so it validated, the write succeeded, the
    /// audit ledger said `applied`, and the build cap of 0.46 silently put it
    /// back on the next read. It must be rejected before any of that.
    #[test]
    fn rejects_a_value_above_its_build_cap() {
        let b = propose("time_decay_max_entry_price", serde_json::json!(0.5));
        assert!(b.accepted.is_empty(), "must not accept an unreachable value");
        let why = &b.rejected.first().expect("one rejection").why;
        assert!(why.contains("0.46"), "the message must name the cap: {why}");
        assert!(why.contains("cannot take effect"), "and say why: {why}");
    }

    /// Every capped field, not just the one that was caught in production.
    #[test]
    fn rejects_above_cap_for_every_capped_field() {
        use crate::helpers::dynamic_config::{build_cap_for, BUILD_CAPPED_KEYS};
        use rust_decimal::prelude::ToPrimitive;
        for key in BUILD_CAPPED_KEYS {
            let cap = build_cap_for(key).expect("declared");
            let over = cap.to_f64().expect("finite") + 0.01;
            let b = propose(key, serde_json::json!(over));
            assert!(b.accepted.is_empty(), "{key}: accepted {over}, above its {cap} cap");
        }
    }

    /// The cap is one-way. TIGHTENING a capped field is exactly the kind of
    /// proposal that should still get through.
    #[test]
    fn accepts_a_value_below_its_build_cap() {
        let b = propose("time_decay_max_entry_price", serde_json::json!(0.40));
        assert_eq!(b.accepted.len(), 1, "tightening must still be allowed: {:?}", b.rejected);
        assert!(b.rejected.is_empty());
    }

    /// An uncapped field keeps its old behavior, bounded only by the schema.
    #[test]
    fn an_uncapped_field_is_unaffected_by_the_cap_check() {
        let b = propose("maker_max_entry_price", serde_json::json!(0.55));
        assert_eq!(b.accepted.len(), 1, "{:?}", b.rejected);
    }

    #[test]
    fn extracts_fenced_json_block() {
        let text = "📊 REPORT\nsome prose\n```json\n{\"proposals\":[{\"field\":\"momentum_stop_loss_pct\",\"to\":0.08,\"reason\":\"r\"}]}\n```\n";
        let block = extract_json_block(text).expect("block");
        let raw = parse_proposals(&block).expect("parse");
        assert_eq!(raw.len(), 1);
        assert_eq!(raw[0].field, "momentum_stop_loss_pct");
    }

    #[test]
    fn extracts_bare_object_without_fence() {
        let text = "prose before {\"proposals\":[{\"field\":\"ghost_mode\",\"to\":true}]} prose after";
        let block = extract_json_block(text).expect("block");
        assert!(parse_proposals(&block).is_ok());
    }

    #[test]
    fn no_block_is_none_and_malformed_is_err() {
        assert!(extract_json_block("just prose, no json").is_none());
        assert!(parse_proposals("{\"proposals\": [{\"field\": }]}").is_err());
    }

    #[test]
    fn unknown_field_rejected() {
        let raw = vec![RawProposal {
            field: "definitely_not_a_field".into(),
            to: serde_json::json!(1.0),
            reason: String::new(),
        }];
        let batch = validate_proposals(raw, &cfg());
        assert!(batch.accepted.is_empty());
        assert_eq!(batch.rejected.len(), 1);
        assert!(batch.rejected[0].why.contains("unknown field"));
    }

    #[test]
    fn unknown_field_suggests_closest_key() {
        // The historical repeat offender: truncated `maker_min_entry`.
        let raw = vec![RawProposal {
            field: "maker_min_entry".into(),
            to: serde_json::json!(0.01),
            reason: String::new(),
        }];
        let batch = validate_proposals(raw, &cfg());
        assert_eq!(batch.rejected.len(), 1);
        assert!(
            batch.rejected[0]
                .why
                .contains("closest is \"maker_min_entry_price\", which is advanced"),
            "got: {}",
            batch.rejected[0].why
        );
    }

    #[test]
    fn advanced_field_rejected_even_when_spelled_correctly() {
        // Advanced fields are excluded from the advisor's Machine-Editable
        // Keys list; the validator must enforce the same policy.
        let raw = vec![RawProposal {
            field: "maker_min_entry_price".into(),
            to: serde_json::json!(0.01),
            reason: String::new(),
        }];
        let batch = validate_proposals(raw, &cfg());
        assert_eq!(batch.accepted.len(), 0);
        assert_eq!(batch.rejected.len(), 1);
        assert!(
            batch.rejected[0].why.contains("advanced field"),
            "got: {}",
            batch.rejected[0].why
        );
    }

    #[test]
    fn decimal_field_encodes_as_string_and_clamps() {
        // arbitrage_profit_threshold has schema range [0.0, 0.5]
        let raw = vec![RawProposal {
            field: "arbitrage_profit_threshold".into(),
            to: serde_json::json!(0.9), // above max → clamp to 0.5
            reason: "test".into(),
        }];
        let batch = validate_proposals(raw, &cfg());
        assert_eq!(batch.accepted.len(), 1, "rejected: {:?}", batch.rejected);
        let c = &batch.accepted[0];
        assert!(c.clamped);
        assert_eq!(c.to, serde_json::json!("0.5"));
        assert!(matches!(c.from, Value::String(_)));
    }

    #[test]
    fn bool_field_accepts_bool_and_string() {
        let d = cfg();
        let flip = !d.ghost_mode;
        for to in [serde_json::json!(flip), serde_json::json!(flip.to_string())] {
            let raw = vec![RawProposal { field: "ghost_mode".into(), to, reason: String::new() }];
            let batch = validate_proposals(raw, &d);
            assert_eq!(batch.accepted.len(), 1);
            assert_eq!(batch.accepted[0].to, serde_json::json!(flip));
        }
    }

    #[test]
    fn noop_change_rejected() {
        let d = cfg();
        let raw = vec![RawProposal {
            field: "ghost_mode".into(),
            to: serde_json::json!(d.ghost_mode),
            reason: String::new(),
        }];
        let batch = validate_proposals(raw, &d);
        assert!(batch.accepted.is_empty());
        assert!(batch.rejected[0].why.contains("no-op"));
    }

    #[test]
    fn over_limit_proposals_rejected() {
        let raw: Vec<RawProposal> = (0..MAX_PROPOSALS_PER_CYCLE + 2)
            .map(|_| RawProposal {
                field: "arbitrage_profit_threshold".into(),
                to: serde_json::json!(0.02),
                reason: String::new(),
            })
            .collect();
        let batch = validate_proposals(raw, &cfg());
        assert_eq!(batch.accepted.len() + batch.rejected.len(), MAX_PROPOSALS_PER_CYCLE + 2);
        assert!(batch.rejected.iter().any(|r| r.why.contains("per-cycle limit")));
    }

    #[test]
    fn patch_and_inverse_round_trip() {
        let d = cfg();
        let raw = vec![RawProposal {
            field: "arbitrage_profit_threshold".into(),
            to: serde_json::json!(0.02),
            reason: String::new(),
        }];
        let batch = validate_proposals(raw, &d);
        let patch = batch.patch_json();
        let inverse = batch.inverse_patch_json();
        assert_eq!(patch["arbitrage_profit_threshold"], serde_json::json!("0.02"));
        let cur = serde_json::to_value(&d).unwrap();
        assert_eq!(inverse["arbitrage_profit_threshold"], cur["arbitrage_profit_threshold"]);
    }

    #[test]
    fn strip_removes_block_keeps_prose() {
        let text = "REPORT LINE\n```json\n{\"proposals\":[]}\n```";
        let stripped = strip_proposal_block(text);
        assert!(stripped.contains("REPORT LINE"));
        assert!(!stripped.contains("proposals"));
    }

    #[test]
    fn percent_string_coerced() {
        // "8%"-style output must not sneak through as 8.0 for a pct field:
        // trim_end_matches('%') parses it as 8.0, then schema clamp applies.
        // momentum_stop_loss_pct range check — verify it clamps to the max.
        let schema = config_schema();
        let f = schema.iter().find(|f| f.key == "momentum_stop_loss_pct");
        if let Some(f) = f {
            if let Some(max) = f.max {
                let raw = vec![RawProposal {
                    field: "momentum_stop_loss_pct".into(),
                    to: serde_json::json!("8%"),
                    reason: String::new(),
                }];
                let batch = validate_proposals(raw, &cfg());
                if let Some(c) = batch.accepted.first() {
                    let got: f64 = match &c.to {
                        Value::String(s) => s.parse().unwrap(),
                        _ => panic!("expected string encoding"),
                    };
                    assert!(got <= max);
                }
            }
        }
    }
}
