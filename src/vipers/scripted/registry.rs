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

//! [E96] phase 2: the community viper registry on dradis.live.
//!
//! The registry is a set of static, signed JSON documents:
//!
//! - `index.json`: every published viper and version, with yanks, as
//!   `{"signed": {...}, "signature": {"key_id", "sig"}}`. The signature covers
//!   the canonical JSON of `signed`, so a storage compromise cannot add a
//!   version or edit a yank. It could still serve an *older* signed index
//!   (rollback, to un-yank a version): each engine remembers the newest
//!   `generated_at` it has seen and refuses an older index, which protects
//!   every engine that has browsed since the yank, not one that never has.
//! - `<publisher>/<name>/<version>.json`: an immutable bundle whose
//!   `registry_signature` covers an **envelope** naming the publisher, name,
//!   version, content digest, script API level and publish time, so a valid
//!   signature cannot be replayed under another name or version.
//!
//! The engine only ever fetches from the compiled-in [`ORIGIN`], on an
//! operator's action, with paths built from validated identifiers, no
//! redirects and capped bodies. No URL from a bundle, manifest or request is
//! fetched. Signatures are ed25519 under the keys in [`TRUSTED_KEYS`]; the
//! signing key stays offline with the publisher (`viper-registry` tool).
//!
//! Verification does not depend on the transport: a signed bundle imported as
//! a file is checked against the same keys and labeled signed.

use ed25519_dalek::{Signature, SigningKey, Signer, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::bundle::{self, Bundle};
use super::manifest::valid_ident;

/// The registry this build reads. Compiled in; never taken from a request.
pub const ORIGIN: &str = "https://dradis.live/api/vipers";
/// The registry's display name, stored as an import's `origin.source`.
pub const SOURCE: &str = "dradis.live";
/// Largest index accepted.
pub const MAX_INDEX_BYTES: usize = 1024 * 1024;
/// The index document format this build reads.
pub const INDEX_FORMAT: u32 = 1;

/// Registry public keys this build trusts, by key id. A new key ships in a
/// release before it signs anything; a revoked one is removed the same way.
pub const TRUSTED_KEYS: &[(&str, &str)] = &[
    ("reg-2026-11", "e0f474d48f2d5733fb9c7893e4876101e8bd86eb46085e2167994ab2ae66b0fe"),
];

/// The registry origin. Debug builds may point it at a loopback dev server
/// (`DRADIS_REGISTRY_ORIGIN=http://localhost:3000/api/vipers`); release
/// builds always use [`ORIGIN`].
pub fn origin() -> String {
    #[cfg(debug_assertions)]
    if let Ok(o) = std::env::var("DRADIS_REGISTRY_ORIGIN") {
        if let Ok(u) = url::Url::parse(&o) {
            let loopback = matches!(u.host_str(), Some("localhost" | "127.0.0.1"));
            if u.scheme() == "http" && loopback && u.username().is_empty() && u.password().is_none() {
                return o.trim_end_matches('/').to_string();
            }
        }
    }
    ORIGIN.to_string()
}

// ── Identifiers ──────────────────────────────────────────────────────────────

/// A publisher id: a GitHub login, lowercased (1-39 of a-z, 0-9 and `-`, not
/// starting or ending with `-`).
pub fn valid_publisher(p: &str) -> bool {
    (1..=39).contains(&p.len())
        && p.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !p.starts_with('-') && !p.ends_with('-')
}

/// `MAJOR.MINOR.PATCH`, each 1-4 digits.
pub fn parse_version(v: &str) -> Option<(u32, u32, u32)> {
    let parts: Vec<&str> = v.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|p| p.is_empty() || p.len() > 4 || !p.bytes().all(|b| b.is_ascii_digit())) {
        return None;
    }
    Some((parts[0].parse().ok()?, parts[1].parse().ok()?, parts[2].parse().ok()?))
}

// ── Signatures ───────────────────────────────────────────────────────────────

fn hex(bytes: &[u8]) -> String { bytes.iter().map(|b| format!("{b:02x}")).collect() }

fn unhex<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != N * 2 { return None; }
    let mut out = [0u8; N];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn trusted_key(key_id: &str) -> Option<VerifyingKey> {
    TRUSTED_KEYS.iter().find(|(id, _)| *id == key_id)
        .and_then(|(_, k)| unhex::<32>(k))
        .and_then(|b| VerifyingKey::from_bytes(&b).ok())
}

fn verify_with(key: &VerifyingKey, msg: &[u8], sig_hex: &str) -> Result<(), String> {
    let sig = unhex::<64>(sig_hex).ok_or("the signature is not 64 bytes of hex")?;
    key.verify_strict(msg, &Signature::from_bytes(&sig)).map_err(|_| "the signature does not verify".to_string())
}

fn verify(key_id: &str, msg: &[u8], sig_hex: &str) -> Result<(), String> {
    let key = trusted_key(key_id).ok_or_else(|| format!("signed with key '{key_id}', which this DRADIS does not trust; update DRADIS"))?;
    verify_with(&key, msg, sig_hex)
}

/// A signing key from its 32-byte seed in hex (the tool's key file).
pub fn signing_key(seed_hex: &str) -> Option<SigningKey> {
    unhex::<32>(seed_hex.trim()).map(|b| SigningKey::from_bytes(&b))
}

/// The public key of a signing key, in hex, for [`TRUSTED_KEYS`].
pub fn public_hex(key: &SigningKey) -> String { hex(key.verifying_key().as_bytes()) }

/// The envelope layout this build signs. A verifier rebuilds the envelope
/// from the bundle, so the layout is named in the signature: a later build
/// can add fields and still verify bundles signed under an older layout.
pub const ENVELOPE_VERSION: u32 = 1;

/// What the registry signs for one bundle version: everything the operator is
/// shown as the publisher's (identity, code, readme, display name, license).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub envelope: u32,
    pub publisher: String,
    pub name: String,
    pub display_name: String,
    pub license: String,
    pub version: String,
    pub sha256: String,
    /// SHA-256 of the readme, so a signed bundle's readme cannot be swapped.
    /// Kept out of the content digest: a readme fix is not a code change.
    pub readme_sha256: String,
    pub api_version: u32,
    /// RFC 3339, signed as written.
    pub published_at: String,
}

impl Envelope {
    pub fn message(&self) -> Vec<u8> {
        bundle::canonical_json(&serde_json::to_value(self).expect("an envelope always serializes")).into_bytes()
    }
}

/// A bundle's `registry_signature`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleSignature {
    /// The envelope layout ([`ENVELOPE_VERSION`]).
    pub envelope: u32,
    pub key_id: String,
    pub sig: String,
    pub published_at: String,
}

/// Who the registry says published a bundle, once its signature checks out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Signed {
    pub publisher: String,
    pub version: String,
    pub key_id: String,
    pub published_at: String,
}

/// Sign a bundle in place (the tool). The bundle must already carry its
/// publisher, version and correct digest.
pub fn sign_bundle(b: &mut Bundle, key: &SigningKey, key_id: &str, published_at: &str) -> Result<(), String> {
    let env = envelope_of(b, ENVELOPE_VERSION, published_at)?;
    let sig = hex(&key.sign(&env.message()).to_bytes());
    b.registry_signature = Some(serde_json::to_value(BundleSignature {
        envelope: ENVELOPE_VERSION, key_id: key_id.into(), sig, published_at: published_at.into(),
    }).map_err(|e| e.to_string())?);
    Ok(())
}

fn envelope_of(b: &Bundle, layout: u32, published_at: &str) -> Result<Envelope, String> {
    if layout != ENVELOPE_VERSION {
        return Err(format!("signed with envelope layout {layout}, which this DRADIS does not read; update DRADIS"));
    }
    Ok(Envelope {
        envelope: layout,
        publisher: b.publisher.clone().ok_or("the bundle names no publisher")?,
        name: b.manifest.name.clone(),
        display_name: b.manifest.display_name.clone(),
        license: b.license.clone(),
        version: b.version.clone().ok_or("the bundle has no version")?,
        // The digest of what the bundle actually holds, never its own claim.
        sha256: bundle::content_digest(&b.manifest, &b.source),
        readme_sha256: {
            use sha2::{Digest, Sha256};
            hex(&Sha256::digest(b.readme.as_bytes()))
        },
        api_version: b.api_version,
        published_at: published_at.into(),
    })
}

/// Check a bundle's registry signature. `Ok(None)` when it carries none;
/// `Err` when it carries one that does not check out (a tampered or forged
/// bundle, which an import refuses).
pub fn verify_bundle(b: &Bundle) -> Result<Option<Signed>, String> {
    let Some(raw) = &b.registry_signature else { return Ok(None) };
    let s: BundleSignature = serde_json::from_value(raw.clone())
        .map_err(|_| "the bundle's registry signature is malformed".to_string())?;
    let env = envelope_of(b, s.envelope, &s.published_at)?;
    verify(&s.key_id, &env.message(), &s.sig)
        .map_err(|e| format!("registry signature: {e}; the bundle was changed after it was published"))?;
    Ok(Some(Signed { publisher: env.publisher, version: env.version, key_id: s.key_id, published_at: s.published_at }))
}

// ── Index ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub format: u32,
    pub generated_at: String,
    pub vipers: Vec<Entry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub publisher: String,
    pub name: String,
    pub display_name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub classes: Vec<String>,
    #[serde(default)]
    pub venues: Vec<String>,
    #[serde(default)]
    pub market: Value,
    /// Oldest first.
    pub versions: Vec<Version>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Version {
    pub version: String,
    pub sha256: String,
    pub api_version: u32,
    pub published_at: String,
    /// The reason, when this version was yanked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub yanked: Option<String>,
}

impl Entry {
    /// The newest version this build can run: not yanked, at or below its
    /// script API level.
    pub fn available(&self) -> Option<&Version> {
        self.versions.iter()
            .filter(|v| v.yanked.is_none() && v.api_version <= bundle::API_VERSION)
            .max_by_key(|v| parse_version(&v.version))
    }
    pub fn version(&self, v: &str) -> Option<&Version> { self.versions.iter().find(|x| x.version == v) }
}

impl Index {
    pub fn entry(&self, publisher: &str, name: &str) -> Option<&Entry> {
        self.vipers.iter().find(|e| e.publisher == publisher && e.name == name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSignature {
    pub key_id: String,
    pub sig: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedIndex {
    pub signed: Value,
    pub signature: IndexSignature,
}

pub fn sign_index(index: &Index, key: &SigningKey, key_id: &str) -> Result<SignedIndex, String> {
    let signed = serde_json::to_value(index).map_err(|e| e.to_string())?;
    let sig = hex(&key.sign(bundle::canonical_json(&signed).as_bytes()).to_bytes());
    Ok(SignedIndex { signed, signature: IndexSignature { key_id: key_id.into(), sig } })
}

pub fn verify_index(doc: &SignedIndex) -> Result<Index, String> {
    let key = trusted_key(&doc.signature.key_id).ok_or_else(|| format!(
        "registry index: signed with key '{}', which this DRADIS does not trust; update DRADIS", doc.signature.key_id))?;
    verify_index_with(doc, &key)
}

/// [`verify_index`] against a given key (the tool, checking its own index).
pub fn verify_index_with(doc: &SignedIndex, key: &VerifyingKey) -> Result<Index, String> {
    verify_with(key, bundle::canonical_json(&doc.signed).as_bytes(), &doc.signature.sig)
        .map_err(|e| format!("registry index: {e}"))?;
    let index: Index = serde_json::from_value(doc.signed.clone()).map_err(|e| format!("registry index: {e}"))?;
    if index.format != INDEX_FORMAT {
        return Err(format!("registry index format {} is not one this build reads ({INDEX_FORMAT}); update DRADIS", index.format));
    }
    Ok(index)
}

// ── Fetching ─────────────────────────────────────────────────────────────────

/// Why a registry call failed, so the API can answer with a fitting status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    /// The request named something invalid (400).
    Invalid(String),
    /// The registry does not list it (404).
    NotFound(String),
    /// Withdrawn by the registry (410).
    Yanked(String),
    /// The registry was unreachable or answered something unusable (502).
    Upstream(String),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self { Self::Invalid(m) | Self::NotFound(m) | Self::Yanked(m) | Self::Upstream(m) => f.write_str(m) }
    }
}

async fn get_capped(path: &str, cap: usize) -> Result<Vec<u8>, String> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(15))
        .user_agent(concat!("DRADIS/", env!("CARGO_PKG_VERSION"), " (viper registry)"))
        .build()
        .map_err(|e| format!("registry client: {e}"))?;
    let url = format!("{}/{path}", origin());
    let mut resp = client.get(&url).send().await.map_err(|e| format!("could not reach the viper registry: {e}"))?;
    let status = resp.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(format!("the registry has no {path}"));
    }
    if !status.is_success() {
        return Err(format!("the viper registry answered {status}"));
    }
    if resp.content_length().is_some_and(|n| n as usize > cap) {
        return Err(format!("the registry's answer is over {} KB", cap / 1024));
    }
    let mut buf = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(|e| format!("reading from the viper registry: {e}"))? {
        buf.extend_from_slice(&chunk);
        if buf.len() > cap { return Err(format!("the registry's answer is over {} KB", cap / 1024)); }
    }
    Ok(buf)
}

/// The newest index time this engine has accepted, kept beside the scripts
/// (no `.json` extension, so the script loader skips it).
fn seen_path() -> std::path::PathBuf { super::store::dir().join(".registry-index-seen") }

/// Refuse an index older than the newest one seen (a rollback that would
/// un-yank a version); remember a newer one. RFC 3339 UTC strings from the
/// tool (`...Z`, whole seconds) order correctly as strings.
fn check_fresh(generated_at: &str, seen: Option<&str>) -> Result<(), String> {
    match seen {
        Some(s) if generated_at < s => Err(format!(
            "the registry served an index from {generated_at}, older than the one this DRADIS already saw ({s}); refusing a rolled-back index"
        )),
        _ => Ok(()),
    }
}

/// The registry index, signature and freshness checked.
pub async fn fetch_index() -> Result<Index, String> {
    let body = get_capped("index.json", MAX_INDEX_BYTES).await?;
    let doc: SignedIndex = serde_json::from_slice(&body).map_err(|e| format!("registry index: {e}"))?;
    let index = verify_index(&doc)?;
    let seen = std::fs::read_to_string(seen_path()).ok().map(|s| s.trim().to_string());
    check_fresh(&index.generated_at, seen.as_deref())?;
    if seen.as_deref() != Some(index.generated_at.as_str()) {
        if let Err(e) = std::fs::write(seen_path(), &index.generated_at) {
            tracing::warn!("📜 Could not record the registry index time: {e}");
        }
    }
    Ok(index)
}

/// One published version, checked against both its own signature and the
/// signed index: it must be listed there with the same digest and not yanked.
pub async fn fetch_bundle(publisher: &str, name: &str, version: &str) -> Result<(Bundle, Signed), FetchError> {
    use FetchError::*;
    if !valid_publisher(publisher) { return Err(Invalid("not a valid publisher id".into())); }
    if !valid_ident(name) { return Err(Invalid("not a valid viper name".into())); }
    if parse_version(version).is_none() { return Err(Invalid("not a valid version (MAJOR.MINOR.PATCH)".into())); }
    let index = fetch_index().await.map_err(Upstream)?;
    let listed = index.entry(publisher, name).and_then(|e| e.version(version))
        .ok_or_else(|| NotFound(format!("the registry does not list {publisher}/{name} {version}")))?.clone();
    if let Some(reason) = &listed.yanked {
        return Err(Yanked(format!("{publisher}/{name} {version} was withdrawn by the registry: {reason}")));
    }
    let body = get_capped(&format!("{publisher}/{name}/{version}.json"), bundle::MAX_BUNDLE_BYTES).await.map_err(Upstream)?;
    let b: Bundle = serde_json::from_slice(&body).map_err(|e| Upstream(format!("registry bundle: {e}")))?;
    let signed = check_fetched(&b, publisher, name, version, &listed).map_err(Upstream)?;
    Ok((b, signed))
}

/// The checks on a fetched bundle that need no network (tested directly).
fn check_fetched(b: &Bundle, publisher: &str, name: &str, version: &str, listed: &Version) -> Result<Signed, String> {
    let signed = verify_bundle(b)?.ok_or("the registry served an unsigned bundle")?;
    if signed.publisher != publisher || b.manifest.name != name || signed.version != version {
        return Err("the registry served a bundle for a different viper or version".into());
    }
    if bundle::content_digest(&b.manifest, &b.source) != listed.sha256
        || signed.published_at != listed.published_at || b.api_version != listed.api_version {
        return Err("the bundle does not match its entry in the registry index".into());
    }
    Ok(signed)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A fixed test key: its public half is not in `TRUSTED_KEYS`, so tests
    /// verify with it directly.
    fn key() -> SigningKey { SigningKey::from_bytes(&[7u8; 32]) }

    fn bundle() -> Bundle {
        let manifest: super::super::manifest::Manifest = serde_json::from_value(serde_json::json!({
            "name": "late_favorite", "display_name": "Late Favorite", "classes": ["crypto"], "max_exposure_usdc": "5",
            "params": { "contracts": { "type": "int", "default": 5, "min": 1, "max": 100 } }
        })).unwrap();
        let source = "fn entry(ctx) { () }\nfn exit(ctx, pos) { () }\n".to_string();
        Bundle {
            format: bundle::FORMAT, sha256: bundle::content_digest(&manifest, &source), manifest, source,
            readme: String::new(), license: "MIT".into(), publisher: Some("mbordash".into()),
            version: Some("1.0.0".into()), api_version: 1, registry_signature: None,
        }
    }

    fn check_sig(b: &Bundle) -> Result<(), String> {
        let s: BundleSignature = serde_json::from_value(b.registry_signature.clone().unwrap()).unwrap();
        verify_with(&key().verifying_key(), &envelope_of(b, s.envelope, &s.published_at)?.message(), &s.sig)
    }

    #[test]
    fn a_signed_bundle_verifies_and_any_change_breaks_it() {
        let mut b = bundle();
        sign_bundle(&mut b, &key(), "test", "2026-11-09T20:00:00Z").unwrap();
        check_sig(&b).unwrap();
        // Survives a JSON round trip (what a file or the registry carries).
        let back: Bundle = serde_json::from_str(&serde_json::to_string_pretty(&b).unwrap()).unwrap();
        check_sig(&back).unwrap();
        let tampers: [fn(&mut Bundle); 9] = [
            |b: &mut Bundle| b.source.push_str("// x\n"),
            |b: &mut Bundle| b.version = Some("1.0.1".into()),
            |b: &mut Bundle| b.publisher = Some("someone".into()),
            |b: &mut Bundle| b.manifest.name = "late_favorite_2".into(),
            |b: &mut Bundle| b.api_version = 2,
            |b: &mut Bundle| b.readme.push_str("<script>"),
            |b: &mut Bundle| b.manifest.display_name = "Official Sniper (verified)".into(),
            |b: &mut Bundle| b.license = "Proprietary".into(),
            |b: &mut Bundle| { b.manifest.params.get_mut("contracts").unwrap().default = serde_json::json!(50); },
        ];
        for tamper in tampers {
            let mut t = b.clone();
            tamper(&mut t);
            assert!(check_sig(&t).is_err(), "a changed bundle must not verify");
        }
    }

    #[test]
    fn an_untrusted_key_or_garbage_signature_is_refused_and_none_is_unsigned() {
        assert_eq!(verify_bundle(&bundle()), Ok(None));
        let mut b = bundle();
        sign_bundle(&mut b, &key(), "test", "2026-11-09T20:00:00Z").unwrap();
        assert!(verify_bundle(&b).unwrap_err().contains("does not trust"));
        b.registry_signature = Some(serde_json::json!({ "nope": 1 }));
        assert!(verify_bundle(&b).unwrap_err().contains("malformed"));
    }

    #[test]
    fn an_index_signature_covers_yanks() {
        let index = Index {
            format: INDEX_FORMAT, generated_at: "2026-11-09T20:00:00Z".into(),
            vipers: vec![Entry {
                publisher: "mbordash".into(), name: "late_favorite".into(), display_name: "Late Favorite".into(),
                description: String::new(), classes: vec!["crypto".into()], venues: vec![], market: Value::String("hourly".into()),
                versions: vec![
                    Version { version: "1.0.0".into(), sha256: "a".into(), api_version: 1, published_at: "t".into(), yanked: None },
                    Version { version: "1.2.0".into(), sha256: "b".into(), api_version: 1, published_at: "t".into(), yanked: Some("bug".into()) },
                    Version { version: "1.10.0".into(), sha256: "c".into(), api_version: 99, published_at: "t".into(), yanked: None },
                ],
            }],
        };
        let doc = sign_index(&index, &key(), "test").unwrap();
        let msg = bundle::canonical_json(&doc.signed);
        verify_with(&key().verifying_key(), msg.as_bytes(), &doc.signature.sig).unwrap();
        let mut hidden = doc.clone();
        hidden.signed["vipers"][0]["versions"][1]["yanked"] = Value::Null;
        assert!(verify_with(&key().verifying_key(), bundle::canonical_json(&hidden.signed).as_bytes(), &doc.signature.sig).is_err(),
            "hiding a yank breaks the index signature");
        assert_eq!(index.vipers[0].available().unwrap().version, "1.0.0", "skips the yanked and the too-new versions");
    }

    #[test]
    fn a_fetched_bundle_must_match_the_request_and_the_index() {
        // `check_fetched` needs a trusted key; exercise the parts after it by
        // checking what a mismatch reports with an unsigned bundle.
        let b = bundle();
        let listed = Version { version: "1.0.0".into(), sha256: b.sha256.clone(), api_version: 1, published_at: "t".into(), yanked: None };
        assert!(check_fetched(&b, "mbordash", "late_favorite", "1.0.0", &listed).unwrap_err().contains("unsigned"));
    }

    #[test]
    fn an_older_index_than_one_seen_is_refused() {
        assert!(check_fresh("2026-11-09T20:00:00Z", None).is_ok());
        assert!(check_fresh("2026-11-09T20:00:00Z", Some("2026-11-09T20:00:00Z")).is_ok());
        assert!(check_fresh("2026-11-10T08:00:00Z", Some("2026-11-09T20:00:00Z")).is_ok());
        assert!(check_fresh("2026-11-09T19:59:59Z", Some("2026-11-09T20:00:00Z")).unwrap_err().contains("rolled-back"));
    }

    #[test]
    fn identifiers_are_strict() {
        assert!(valid_publisher("mbordash") && valid_publisher("a-b1"));
        for bad in ["", "-a", "a-", "A", "a/b", "a.b", "../x", &"a".repeat(40)] { assert!(!valid_publisher(bad), "{bad}"); }
        assert_eq!(parse_version("1.10.0"), Some((1, 10, 0)));
        for bad in ["1.0", "1.0.0.0", "1.0.x", "../1", "1..0", "01234.0.0"] { assert!(parse_version(bad).is_none(), "{bad}"); }
    }

    /// The first published bundle, verified with the trusted key compiled
    /// into this build: a key rotation that drops it, or a change to the
    /// envelope or digest, fails here before it fails for operators.
    #[test]
    fn the_published_seed_bundle_verifies_with_the_trusted_key() {
        let b: Bundle = serde_json::from_str(include_str!("testdata/late_favorite-1.0.0.json")).unwrap();
        let s = verify_bundle(&b).unwrap().expect("signed");
        assert_eq!((s.publisher.as_str(), s.version.as_str(), s.key_id.as_str()), ("mbordash", "1.0.0", "reg-2026-11"));
        let mut t = b.clone();
        t.readme.push('x');
        assert!(verify_bundle(&t).is_err(), "the readme is signed too");
        let mut t = b.clone();
        t.source.push_str("// x\n");
        assert!(verify_bundle(&t).is_err());
    }

    #[test]
    fn the_trusted_keys_parse() {
        for (id, _) in TRUSTED_KEYS { assert!(trusted_key(id).is_some(), "trusted key {id} does not parse"); }
    }
}

/// Manual end-to-end check against a local copy of the registry:
/// `python3 -m http.server 8765 -d ~/dradis-web/public/api/vipers` then
/// `DRADIS_REGISTRY_ORIGIN=http://127.0.0.1:8765 cargo test --lib registry_live -- --ignored`.
#[cfg(test)]
mod live {
    #[tokio::test]
    #[ignore]
    async fn registry_live() {
        let index = super::fetch_index().await.expect("index");
        let e = &index.vipers[0];
        let v = e.available().expect("available").version.clone();
        let (b, signed) = super::fetch_bundle(&e.publisher, &e.name, &v).await.expect("bundle");
        assert_eq!(signed.publisher, e.publisher);
        let c = super::super::bundle::check_import(&b, None, chrono::Utc::now());
        assert!(c.problems.is_empty(), "{:?}", c.problems);
        assert!(matches!(super::fetch_bundle(&e.publisher, &e.name, "9.9.9").await, Err(super::FetchError::NotFound(_))));
        assert!(matches!(super::fetch_bundle("..", &e.name, &v).await, Err(super::FetchError::Invalid(_))));
        println!("OK {}/{} {v} signed by {}", e.publisher, e.name, signed.key_id);
    }
}
