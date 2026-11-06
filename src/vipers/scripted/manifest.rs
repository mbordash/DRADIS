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

//! The manifest every scripted viper carries beside its script
//! (`<name>.json` next to `<name>.rhai`).
//!
//! It is what the engine knows about a script without running it: the
//! operator's name for it, which market classes and venues it may fly, the
//! dollar cap the adapter enforces whatever the script asks for, and the
//! parameters the operator edits from the viper card. JSON rather than TOML so
//! it needs no new parser and the Control Tower writes it as-is.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Market classes a script may declare. `helm` is deliberately absent: a Helm
/// squadron carries the operator's one position and nothing else.
pub const CLASSES: &[&str] = &["crypto", "sports", "politics", "unknown"];
/// Venue builds a script may declare (`venues::venue_name`).
pub const VENUES: &[&str] = &["intl_clob", "us_retail", "kalshi"];
/// Hard ceiling on a manifest's exposure cap, whatever the operator types.
pub const MAX_EXPOSURE_CEILING: Decimal = rust_decimal_macros::dec!(1000);
/// At most this many declared parameters per script.
pub const MAX_PARAMS: usize = 32;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// `[a-z0-9_]{3,32}`; the file stem and the key everything else uses.
    pub name: String,
    /// What the operator sees on cards and in the trade log.
    pub display_name: String,
    #[serde(default)]
    pub description: String,
    /// Market classes whose squadrons attach this viper.
    pub classes: Vec<String>,
    /// Venue builds it may run on; empty means every venue.
    #[serde(default)]
    pub venues: Vec<String>,
    /// The adapter's hard cap on this viper's open cost basis, in dollars.
    pub max_exposure_usdc: Decimal,
    /// Operator parameters, by name, readable from the script as `ctx.params`.
    #[serde(default)]
    pub params: BTreeMap<String, Param>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Param {
    /// `decimal`, `int` or `bool`.
    #[serde(rename = "type")]
    pub kind: String,
    pub default: serde_json::Value,
    #[serde(default)]
    pub min: Option<Decimal>,
    #[serde(default)]
    pub max: Option<Decimal>,
    #[serde(default)]
    pub description: String,
}

/// A parameter's value as the script sees it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ParamValue {
    Bool(bool),
    Int(i64),
    Decimal(Decimal),
}

/// Rhai's keywords and reserved words. A parameter with one of these names
/// could not be read as `ctx.params.<name>`, so the manifest refuses it.
const RESERVED: &[&str] = &[
    "true", "false", "let", "const", "if", "else", "switch", "do", "while", "until", "loop",
    "for", "in", "continue", "break", "return", "throw", "try", "catch", "import", "export",
    "as", "global", "private", "fn", "this", "is", "var", "static", "shared", "with", "go",
    "spawn", "thread", "sync", "async", "await", "yield", "default", "void", "null", "nil",
    "case", "public", "protected", "super", "new", "use", "module", "package", "match", "goto",
    "exit", "type_of", "print", "debug", "eval", "call", "curry", "is_def_var", "is_def_fn",
    "is_shared", "object", "func", "macro", "include", "def",
];

pub fn valid_name(name: &str) -> bool {
    (3..=32).contains(&name.len())
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && name.as_bytes()[0].is_ascii_lowercase()
}

impl Manifest {
    /// Every problem with the manifest, so the page can show them all at once.
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        if !valid_name(&self.name) {
            out.push(format!("name '{}' must be 3-32 characters of a-z, 0-9 and _, starting with a letter", self.name));
        }
        if self.display_name.trim().is_empty() || self.display_name.len() > 64 {
            out.push("display_name must be 1-64 characters".into());
        }
        if self.classes.is_empty() {
            out.push(format!("classes must name at least one of {}", CLASSES.join(", ")));
        }
        for c in &self.classes {
            if !CLASSES.contains(&c.as_str()) {
                out.push(format!("class '{c}' is not one of {}", CLASSES.join(", ")));
            }
        }
        for v in &self.venues {
            if !VENUES.contains(&v.as_str()) {
                out.push(format!("venue '{v}' is not one of {}", VENUES.join(", ")));
            }
        }
        if self.max_exposure_usdc <= Decimal::ZERO || self.max_exposure_usdc > MAX_EXPOSURE_CEILING {
            out.push(format!("max_exposure_usdc must be above 0 and at most {MAX_EXPOSURE_CEILING}"));
        }
        if self.params.len() > MAX_PARAMS {
            out.push(format!("at most {MAX_PARAMS} params"));
        }
        for (k, p) in &self.params {
            if !valid_name(k) {
                out.push(format!("param name '{k}' must be 3-32 characters of a-z, 0-9 and _, starting with a letter"));
            } else if RESERVED.contains(&k.as_str()) {
                out.push(format!("param name '{k}' is a reserved word in the script language"));
            }
            match p.coerce(&p.default) {
                Ok(_) => {}
                Err(e) => out.push(format!("param '{k}' default: {e}")),
            }
            if let (Some(lo), Some(hi)) = (p.min, p.max) {
                if lo > hi { out.push(format!("param '{k}': min is above max")); }
            }
        }
        out
    }

    /// Whether this script may run on the given venue build.
    pub fn runs_on(&self, venue: &str) -> bool {
        self.venues.is_empty() || self.venues.iter().any(|v| v == venue)
    }

    /// The parameter values in force: the operator's overrides where they are
    /// valid, the manifest default otherwise. An override that no longer fits
    /// the manifest (a type change, a narrowed range) falls back to the
    /// default rather than reaching the script out of range.
    pub fn resolve_params(&self, overrides: &BTreeMap<String, serde_json::Value>) -> BTreeMap<String, ParamValue> {
        self.params.iter().filter_map(|(k, p)| {
            let v = overrides.get(k).and_then(|o| p.coerce(o).ok())
                .or_else(|| p.coerce(&p.default).ok())?;
            Some((k.clone(), v))
        }).collect()
    }
}

impl Param {
    /// The value as this parameter's type, inside its range.
    pub fn coerce(&self, v: &serde_json::Value) -> Result<ParamValue, String> {
        let in_range = |d: Decimal| -> Result<(), String> {
            if self.min.is_some_and(|lo| d < lo) { return Err(format!("{d} is below the minimum {}", self.min.unwrap())); }
            if self.max.is_some_and(|hi| d > hi) { return Err(format!("{d} is above the maximum {}", self.max.unwrap())); }
            Ok(())
        };
        let as_decimal = |v: &serde_json::Value| -> Option<Decimal> {
            match v {
                serde_json::Value::Number(n) => n.to_string().parse().ok(),
                serde_json::Value::String(s) => s.trim().parse().ok(),
                _ => None,
            }
        };
        match self.kind.as_str() {
            "bool" => v.as_bool().map(ParamValue::Bool).ok_or_else(|| "expected true or false".into()),
            "int" => {
                let d = as_decimal(v).filter(|d| d.fract().is_zero()).ok_or("expected a whole number")?;
                in_range(d)?;
                i64::try_from(d).map(ParamValue::Int).map_err(|_| "out of range".into())
            }
            "decimal" => {
                let d = as_decimal(v).ok_or("expected a number")?;
                in_range(d)?;
                Ok(ParamValue::Decimal(d))
            }
            other => Err(format!("type '{other}' is not one of decimal, int, bool")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn manifest() -> Manifest {
        serde_json::from_value(serde_json::json!({
            "name": "fade_spikes",
            "display_name": "Fade Spikes",
            "classes": ["crypto"],
            "max_exposure_usdc": 10,
            "params": {
                "min_edge": { "type": "decimal", "default": 0.03, "min": 0, "max": 0.5 },
                "contracts": { "type": "int", "default": 5, "min": 1, "max": 50 },
                "use_tp": { "type": "bool", "default": true }
            }
        })).unwrap()
    }

    #[test]
    fn a_good_manifest_has_no_problems_and_resolves_its_defaults() {
        let m = manifest();
        assert!(m.problems().is_empty(), "{:?}", m.problems());
        let p = m.resolve_params(&BTreeMap::new());
        assert_eq!(p["min_edge"], ParamValue::Decimal(dec!(0.03)), "a decimal default stays exact");
        assert_eq!(p["contracts"], ParamValue::Int(5));
        assert_eq!(p["use_tp"], ParamValue::Bool(true));
        assert!(m.runs_on("kalshi"), "no venues listed means every venue");
    }

    #[test]
    fn an_override_out_of_range_or_of_the_wrong_type_falls_back_to_the_default() {
        let m = manifest();
        let o: BTreeMap<String, serde_json::Value> = serde_json::from_value(serde_json::json!({
            "min_edge": 0.9, "contracts": "7", "use_tp": 3
        })).unwrap();
        let p = m.resolve_params(&o);
        assert_eq!(p["min_edge"], ParamValue::Decimal(dec!(0.03)), "0.9 is above the 0.5 maximum");
        assert_eq!(p["contracts"], ParamValue::Int(7), "a numeric string is accepted");
        assert_eq!(p["use_tp"], ParamValue::Bool(true), "3 is not a bool");
    }

    #[test]
    fn every_problem_is_reported() {
        let mut m = manifest();
        m.name = "X".into();
        m.classes = vec!["helm".into()];
        m.venues = vec!["nyse".into()];
        m.max_exposure_usdc = dec!(0);
        m.params.insert("default".into(), m.params["min_edge"].clone());
        let p = m.problems();
        assert_eq!(p.len(), 5, "{p:?}");
        assert!(p.iter().any(|x| x.contains("'default' is a reserved word")), "{p:?}");
        assert!(p.iter().any(|x| x.contains("class 'helm'")), "a script cannot fly a Helm squadron");
    }
}
