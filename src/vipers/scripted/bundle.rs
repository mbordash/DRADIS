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

//! [E96] Viper bundles: one JSON document carrying a scripted viper (manifest,
//! source, readme), so it can be exported from one DRADIS and imported into
//! another, and later published to the community registry.
//!
//! The content digest is the identity every later piece depends on (the
//! "Published / Edited locally" label, updates, results reports), so it is
//! defined here once and pinned by golden-vector tests:
//!
//! - the source, line endings normalized to `\n`;
//! - the manifest as parsed, **without** `name`, `display_name` and `origin`
//!   (an import may rename on collision, and `origin` carries the import time;
//!   including them would make every import look edited), with defaulted
//!   fields materialized and every number in it rendered as a normalized
//!   decimal string, so `0.5`, `"0.5"` and `0.50` hash the same;
//! - `{"manifest": ..., "source": ...}` serialized with sorted keys and no
//!   whitespace (RFC 8785 for these value shapes: strings, booleans, integers,
//!   arrays and objects), then SHA-256, lowercase hex.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::manifest::{Manifest, Origin, Param};
use super::store;

/// The script API level this build provides. Bump it when the script API
/// gains something a script can depend on (a new `ctx` field, helper, action
/// or manifest option), so a bundle that needs it is refused cleanly by an
/// older engine instead of failing every tick.
pub const API_VERSION: u32 = 1;
/// The bundle format this build reads and writes.
pub const FORMAT: u32 = 1;
/// Largest readme accepted.
pub const MAX_README_BYTES: usize = 32 * 1024;
/// Largest bundle body accepted, whole.
pub const MAX_BUNDLE_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bundle {
    pub format: u32,
    pub manifest: Manifest,
    pub source: String,
    #[serde(default)]
    pub readme: String,
    #[serde(default = "default_license")]
    pub license: String,
    #[serde(default)]
    pub publisher: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default = "default_api_version")]
    pub api_version: u32,
    /// The content digest; an import recomputes it and refuses a mismatch.
    pub sha256: String,
    /// The registry's signature over the signed envelope (phase 2). Carried
    /// through untouched; a file import is labeled unsigned whatever it holds,
    /// since this build has no registry keys to check it against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry_signature: Option<Value>,
}

fn default_license() -> String { "MIT".into() }
fn default_api_version() -> u32 { 1 }

/// Line endings normalized to `\n`.
pub fn normalize_source(source: &str) -> String {
    source.replace("\r\n", "\n").replace('\r', "\n")
}

fn dec_str(d: Decimal) -> String { d.normalize().to_string() }

/// A parameter value (default, min or max) in its canonical rendering: a bool
/// stays a bool, a number becomes a normalized decimal string. A value that is
/// neither (which `Manifest::problems` would refuse) is kept as written.
fn canonical_value(p: &Param, v: &Value) -> Value {
    match v {
        Value::Bool(b) if p.kind == "bool" => Value::Bool(*b),
        Value::Number(n) => n.to_string().parse::<Decimal>().map_or(v.clone(), |d| Value::String(dec_str(d))),
        Value::String(s) => s.trim().parse::<Decimal>().map_or(v.clone(), |d| Value::String(dec_str(d))),
        other => other.clone(),
    }
}

/// The manifest as the digest sees it.
fn canonical_manifest(m: &Manifest) -> Value {
    let params: serde_json::Map<String, Value> = m.params.iter().map(|(k, p)| {
        let mut o = serde_json::Map::new();
        o.insert("type".into(), Value::String(p.kind.clone()));
        o.insert("default".into(), canonical_value(p, &p.default));
        o.insert("min".into(), p.min.map_or(Value::Null, |d| Value::String(dec_str(d))));
        o.insert("max".into(), p.max.map_or(Value::Null, |d| Value::String(dec_str(d))));
        o.insert("description".into(), Value::String(p.description.clone()));
        (k.clone(), Value::Object(o))
    }).collect();
    serde_json::json!({
        "description": m.description,
        "classes": m.classes,
        "venues": m.venues,
        "max_exposure_usdc": dec_str(m.max_exposure_usdc),
        "params": params,
        "market": m.market,
        "api_version": m.api_version.unwrap_or(1),
    })
}

/// Sorted keys, no whitespace. `serde_json::Map` is a `BTreeMap` in this
/// build (no `preserve_order`), so `to_string` already sorts; this sorts
/// explicitly anyway, so turning that feature on elsewhere cannot change a
/// digest.
fn canonical_json(v: &Value) -> String {
    fn sort(v: &Value) -> Value {
        match v {
            Value::Object(m) => {
                let mut keys: Vec<&String> = m.keys().collect();
                keys.sort();
                let mut out = serde_json::Map::new();
                for k in keys { out.insert(k.clone(), sort(&m[k])); }
                Value::Object(out)
            }
            Value::Array(a) => Value::Array(a.iter().map(sort).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string(&sort(v)).unwrap_or_default()
}

/// The content digest of a viper: 64 lowercase hex characters.
pub fn content_digest(manifest: &Manifest, source: &str) -> String {
    use sha2::{Digest, Sha256};
    let doc = serde_json::json!({ "manifest": canonical_manifest(manifest), "source": normalize_source(source) });
    Sha256::digest(canonical_json(&doc).as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

/// How a local viper relates to where it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    /// Written here; no origin.
    Local,
    /// Imported and unchanged since.
    Imported,
    /// Imported, then edited here.
    Edited,
}

pub fn provenance(manifest: &Manifest, source: &str) -> Provenance {
    match &manifest.origin {
        None => Provenance::Local,
        Some(o) if o.sha256 == content_digest(manifest, source) => Provenance::Imported,
        Some(_) => Provenance::Edited,
    }
}

/// A saved viper as a bundle.
pub fn export(script: &store::Script) -> Bundle {
    let mut manifest = script.manifest.clone();
    let origin = manifest.origin.take();
    // The level the script was written against; absent means 1, never this
    // build's level, or an old script exported from a newer engine would be
    // refused by older ones that run it fine.
    let api_version = manifest.api_version.unwrap_or(1);
    let sha256 = content_digest(&manifest, &script.source);
    // An unchanged import keeps saying whose it is and which version.
    let unchanged = origin.filter(|o| o.sha256 == sha256);
    Bundle {
        format: FORMAT,
        source: script.source.clone(),
        readme: String::new(),
        license: default_license(),
        publisher: unchanged.as_ref().and_then(|o| o.publisher.clone()),
        version: unchanged.as_ref().and_then(|o| o.version.clone()),
        api_version,
        sha256,
        manifest,
        registry_signature: None,
    }
}

/// What an import would do, or why it cannot.
#[derive(Debug, Clone, Serialize)]
pub struct Checked {
    /// The manifest as it would be saved: renamed, with `origin` set.
    pub manifest: Manifest,
    pub source: String,
    pub digest: String,
    /// Every problem; the import may go ahead only when this is empty.
    pub problems: Vec<String>,
}

/// Check a bundle for import under `local_name` (the published name when
/// `None`). Pure: touches neither the disk nor the running set.
pub fn check_import(bundle: &Bundle, local_name: Option<&str>, now: chrono::DateTime<chrono::Utc>) -> Checked {
    let mut problems = Vec::new();
    if bundle.format != FORMAT {
        problems.push(format!("bundle format {} is not one this build reads ({FORMAT})", bundle.format));
    }
    let needs = bundle.api_version.max(bundle.manifest.api_version.unwrap_or(1));
    if needs > API_VERSION {
        problems.push(format!(
            "this viper needs script API level {needs}; this DRADIS provides level {API_VERSION}. Update DRADIS to import it"
        ));
    }
    for (what, v, max) in [("publisher", bundle.publisher.as_deref(), 64), ("version", bundle.version.as_deref(), 64), ("license", Some(bundle.license.as_str()), 32)] {
        if v.is_some_and(|v| v.len() > max) { problems.push(format!("the {what} is over {max} characters")); }
    }
    if bundle.readme.len() > MAX_README_BYTES {
        problems.push(format!("the readme is over {} KB", MAX_README_BYTES / 1024));
    }
    if bundle.source.len() > store::MAX_SOURCE_BYTES {
        problems.push(format!("the script is over {} KB", store::MAX_SOURCE_BYTES / 1024));
    }
    let digest = content_digest(&bundle.manifest, &bundle.source);
    if !bundle.sha256.eq_ignore_ascii_case(&digest) {
        problems.push("the bundle's sha256 does not match its contents; it was changed after export".into());
    }
    let published_name = bundle.manifest.name.clone();
    let mut manifest = bundle.manifest.clone();
    if let Some(n) = local_name { manifest.name = n.to_string(); }
    // `api_version` stays as the bundle's manifest wrote it: the digest was
    // computed over that, and changing it here would make a fresh import read
    // as edited. The envelope's level is enforced above.
    manifest.origin = Some(Origin {
        source: "file".into(),
        publisher: bundle.publisher.clone(),
        name: published_name,
        version: bundle.version.clone(),
        sha256: digest.clone(),
        imported_at: now,
    });
    let source = normalize_source(&bundle.source);
    if let Err(p) = store::check_for_save(&manifest, &source) { problems.extend(p); }
    Checked { manifest, source, digest, problems }
}

/// A free local name for an import of `name`: the name itself, or the first
/// `name_2`, `name_3`, ... not taken, kept within the 32-character limit.
pub fn free_name(name: &str, taken: impl Fn(&str) -> bool) -> String {
    if !taken(name) { return name.to_string(); }
    for i in 2..1000 {
        let suffix = format!("_{i}");
        let stem: String = name.chars().take(32 - suffix.len()).collect();
        let n = format!("{stem}{suffix}");
        if !taken(&n) { return n; }
    }
    name.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Manifest {
        serde_json::from_value(serde_json::json!({
            "name": "late_favorite",
            "display_name": "Late Favorite",
            "description": "Fair value from distance to strike.",
            "classes": ["crypto"],
            "max_exposure_usdc": "5",
            "params": {
                "min_fair": { "type": "decimal", "default": 0.75, "min": "0.5", "max": "0.99", "description": "floor" },
                "contracts": { "type": "int", "default": 5, "min": 1, "max": 100 },
                "use_cap": { "type": "bool", "default": true }
            }
        })).unwrap()
    }

    const SRC: &str = "fn entry(ctx) { #{ hold: \"x\" } }\nfn exit(ctx, pos) { () }\n";

    /// Pins the digest definition. If this changes, every published version
    /// changes identity: bump `FORMAT` and keep the old definition readable.
    /// The value was checked independently with Python's `json.dumps(...,
    /// sort_keys=True, separators=(",", ":"), ensure_ascii=False)` and
    /// SHA-256, the way the registry (TypeScript) will compute it.
    #[test]
    fn the_digest_is_pinned() {
        assert_eq!(content_digest(&manifest(), SRC), "c249b7004bc22c7ee37364c05bcaf01ad2b2c01e700ff165554d57dfe5a79b2e");
    }

    #[test]
    fn the_digest_ignores_names_origin_line_endings_and_number_spelling() {
        let base = content_digest(&manifest(), SRC);
        let mut m = manifest();
        m.name = "late_favorite_2".into();
        m.display_name = "Mine".into();
        m.origin = Some(Origin { source: "file".into(), publisher: None, name: "x".into(), version: None, sha256: "y".into(), imported_at: chrono::Utc::now() });
        assert_eq!(content_digest(&m, SRC), base, "a renamed import is not an edit");
        assert_eq!(content_digest(&manifest(), &SRC.replace('\n', "\r\n")), base, "CRLF is not an edit");
        let mut m = manifest();
        m.params.get_mut("min_fair").unwrap().default = serde_json::json!("0.750");
        m.max_exposure_usdc = rust_decimal_macros::dec!(5.00);
        assert_eq!(content_digest(&m, SRC), base, "0.75, \"0.750\" and 5.00 vs 5 are one value");
        let mut m = manifest();
        m.api_version = Some(1);
        assert_eq!(content_digest(&m, SRC), base, "an absent api_version means level 1");
    }

    #[test]
    fn the_digest_sees_every_real_change() {
        let base = content_digest(&manifest(), SRC);
        let mut m = manifest();
        m.params.get_mut("min_fair").unwrap().default = serde_json::json!(0.6);
        assert_ne!(content_digest(&m, SRC), base, "a changed default is a new version");
        let mut m = manifest();
        m.params.get_mut("min_fair").unwrap().max = Some(rust_decimal_macros::dec!(0.95));
        assert_ne!(content_digest(&m, SRC), base, "a changed range is a new version");
        let mut m = manifest();
        m.venues = vec!["kalshi".into()];
        assert_ne!(content_digest(&m, SRC), base);
        assert_ne!(content_digest(&manifest(), &SRC.replace("\"x\"", "\"y\"")), base);
    }

    fn bundle() -> Bundle {
        let m = manifest();
        Bundle {
            format: FORMAT, sha256: content_digest(&m, SRC), manifest: m, source: SRC.into(),
            readme: "# Late Favorite".into(), license: "MIT".into(), publisher: Some("mbordash".into()),
            version: Some("1.0.0".into()), api_version: 1, registry_signature: None,
        }
    }

    #[test]
    fn a_good_bundle_imports_under_a_new_name_with_its_origin() {
        let now = chrono::Utc::now();
        let c = check_import(&bundle(), Some("my_late_favorite"), now);
        assert!(c.problems.is_empty(), "{:?}", c.problems);
        assert_eq!(c.manifest.name, "my_late_favorite");
        let o = c.manifest.origin.as_ref().unwrap();
        assert_eq!((o.name.as_str(), o.publisher.as_deref(), o.version.as_deref()), ("late_favorite", Some("mbordash"), Some("1.0.0")));
        assert_eq!(provenance(&c.manifest, &c.source), Provenance::Imported, "fresh import reads as imported, not edited");
        let mut edited = c.manifest.clone();
        edited.params.get_mut("contracts").unwrap().default = serde_json::json!(7);
        assert_eq!(provenance(&edited, &c.source), Provenance::Edited);
        assert_eq!(provenance(&manifest(), SRC), Provenance::Local);
    }

    #[test]
    fn a_tampered_newer_or_oversized_bundle_is_refused() {
        let now = chrono::Utc::now();
        let mut b = bundle();
        b.source = b.source.replace("\"x\"", "\"tampered\"");
        assert!(check_import(&b, None, now).problems.iter().any(|p| p.contains("does not match")));
        let mut b = bundle();
        b.api_version = API_VERSION + 1;
        assert!(check_import(&b, None, now).problems.iter().any(|p| p.contains("script API level")));
        let mut b = bundle();
        b.readme = "x".repeat(MAX_README_BYTES + 1);
        assert!(check_import(&b, None, now).problems.iter().any(|p| p.contains("readme")));
        let mut b = bundle();
        b.format = 2;
        assert!(check_import(&b, None, now).problems.iter().any(|p| p.contains("format")));
        assert!(!check_import(&bundle(), Some("../evil"), now).problems.is_empty(), "the local name goes through the name rules");
    }

    #[test]
    fn an_envelope_level_never_changes_what_is_saved_or_its_digest() {
        let mut b = bundle();
        b.api_version = 1;
        b.manifest.api_version = None;
        let c = check_import(&b, Some("lf"), chrono::Utc::now());
        assert_eq!(c.manifest.api_version, None, "the manifest is saved as the bundle wrote it");
        assert_eq!(provenance(&c.manifest, &c.source), Provenance::Imported);
        let mut b = bundle();
        b.publisher = Some("x".repeat(65));
        assert!(check_import(&b, None, chrono::Utc::now()).problems.iter().any(|p| p.contains("publisher")));
    }

    #[test]
    fn a_free_name_stays_within_32_characters() {
        let taken = |n: &str| n == "late_favorite" || n == "late_favorite_2";
        assert_eq!(free_name("late_favorite", taken), "late_favorite_3");
        assert_eq!(free_name("other", taken), "other");
        let long = "a".repeat(32);
        let n = free_name(&long, |n| n.len() == 32 && n.chars().all(|c| c == 'a'));
        assert_eq!(n.len(), 32);
        assert!(n.ends_with("_2"));
    }

    #[test]
    fn a_manifest_with_the_new_fields_round_trips_and_an_old_one_writes_none() {
        let old = serde_json::to_value(manifest()).unwrap();
        assert!(old.get("origin").is_none() && old.get("api_version").is_none(), "an old manifest file is unchanged by this build");
        let c = check_import(&bundle(), None, chrono::Utc::now());
        let back: Manifest = serde_json::from_slice(&serde_json::to_vec(&c.manifest).unwrap()).unwrap();
        assert_eq!(back, c.manifest);
    }
}
