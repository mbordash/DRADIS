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

//! [E96] `viper-registry`: the publisher's offline tool for the community
//! viper registry. It writes the static, signed documents the engine reads
//! (see `vipers::scripted::registry`) into a directory that dradis-web serves
//! as `https://dradis.live/api/vipers/`.
//!
//! ```text
//! cargo run --features registry-tool --bin viper-registry -- keygen  <key file>
//! cargo run --features registry-tool --bin viper-registry -- pubkey  <key file>
//! cargo run --features registry-tool --bin viper-registry -- publish <key file> <key id> <out dir> <publisher> <version> <source dir>
//! cargo run --features registry-tool --bin viper-registry -- yank    <key file> <key id> <out dir> <publisher> <name> <version> <reason>
//! cargo run --features registry-tool --bin viper-registry -- verify  <out dir>
//! ```
//!
//! A source dir holds `manifest.json`, `script.rhai` and `README.md`. Published
//! versions are immutable: `publish` refuses to overwrite one, and a bad one is
//! yanked, never rewritten.

use std::path::{Path, PathBuf};

use dradis::vipers::scripted::bundle::{self, Bundle};
use dradis::vipers::scripted::manifest::{valid_name, Manifest};
use dradis::vipers::scripted::registry::{self, Entry, Index, SignedIndex, Version};
use dradis::vipers::scripted::store;

type R<T> = Result<T, String>;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let a: Vec<&str> = args.iter().map(String::as_str).collect();
    let out = match a.as_slice() {
        ["keygen", key] => keygen(Path::new(key)),
        ["pubkey", key] => load_key(Path::new(key)).map(|k| println!("{}", registry::public_hex(&k))),
        ["publish", key, key_id, out, publisher, version, src] => publish(Path::new(key), key_id, Path::new(out), publisher, version, Path::new(src)),
        ["yank", key, key_id, out, publisher, name, version, reason] => yank(Path::new(key), key_id, Path::new(out), publisher, name, version, reason),
        ["verify", out] => verify(Path::new(out)),
        _ => Err("usage: see the header of src/bin/viper_registry.rs".into()),
    };
    if let Err(e) = out {
        eprintln!("viper-registry: {e}");
        std::process::exit(1);
    }
}

fn now() -> String { chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true) }

fn keygen(path: &Path) -> R<()> {
    if path.exists() { return Err(format!("{} exists; refusing to overwrite a signing key", path.display())); }
    let mut seed = [0u8; 32];
    use std::io::Read;
    std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut seed)).map_err(|e| e.to_string())?;
    let hex: String = seed.iter().map(|b| format!("{b:02x}")).collect();
    if let Some(dir) = path.parent() { std::fs::create_dir_all(dir).map_err(|e| e.to_string())?; }
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path).map_err(|e| e.to_string())?;
        writeln!(f, "{hex}").map_err(|e| e.to_string())?;
    }
    let key = registry::signing_key(&hex).ok_or("bad seed")?;
    println!("Wrote {} (mode 600). Keep it offline and backed up.", path.display());
    println!("Public key (for registry::TRUSTED_KEYS): {}", registry::public_hex(&key));
    Ok(())
}

fn load_key(path: &Path) -> R<ed25519_dalek::SigningKey> {
    let s = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    registry::signing_key(&s).ok_or_else(|| format!("{} does not hold a 32-byte hex seed", path.display()))
}

fn index_path(out: &Path) -> PathBuf { out.join("index.json") }

/// The current index, checked against the publishing key; empty when there
/// is none yet.
fn load_index(out: &Path, key: &ed25519_dalek::SigningKey) -> R<Index> {
    let p = index_path(out);
    if !p.exists() {
        return Ok(Index { format: registry::INDEX_FORMAT, generated_at: now(), vipers: vec![] });
    }
    let doc: SignedIndex = serde_json::from_slice(&std::fs::read(&p).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
    // A key this build trusts (so a new key can take over an index signed by
    // the old one), or the publishing key itself (before its public half is
    // compiled into a release).
    registry::verify_index(&doc).or_else(|_| registry::verify_index_with(&doc, &key.verifying_key()))
}

fn write_index(out: &Path, mut index: Index, key: &ed25519_dalek::SigningKey, key_id: &str) -> R<()> {
    index.generated_at = now();
    index.vipers.sort_by(|a, b| (&a.name, &a.publisher).cmp(&(&b.name, &b.publisher)));
    let doc = registry::sign_index(&index, key, key_id)?;
    write_json(&index_path(out), &doc, true)
}

fn write_json<T: serde::Serialize>(path: &Path, v: &T, overwrite: bool) -> R<()> {
    if !overwrite && path.exists() { return Err(format!("{} exists; published versions are immutable", path.display())); }
    if let Some(dir) = path.parent() { std::fs::create_dir_all(dir).map_err(|e| e.to_string())?; }
    let mut body = serde_json::to_string_pretty(v).map_err(|e| e.to_string())?;
    body.push('\n');
    std::fs::write(path, body).map_err(|e| format!("{}: {e}", path.display()))
}

fn publish(key_path: &Path, key_id: &str, out: &Path, publisher: &str, version: &str, src: &Path) -> R<()> {
    let key = load_key(key_path)?;
    if !registry::valid_publisher(publisher) { return Err(format!("'{publisher}' is not a valid publisher id")); }
    let parsed = registry::parse_version(version).ok_or("version must be MAJOR.MINOR.PATCH")?;
    let read = |f: &str| std::fs::read_to_string(src.join(f)).map_err(|e| format!("{}: {e}", src.join(f).display()));
    let mut manifest: Manifest = serde_json::from_str(&read("manifest.json")?).map_err(|e| format!("manifest.json: {e}"))?;
    manifest.origin = None;
    let source = bundle::normalize_source(&read("script.rhai")?);
    let readme = read("README.md")?;
    let name = manifest.name.clone();
    if !valid_name(&name) { return Err(format!("'{name}' is not a valid viper name")); }
    if readme.len() > bundle::MAX_README_BYTES { return Err("README.md is over 32 KB".into()); }
    store::check_for_save(&manifest, &source).map_err(|p| format!("the script does not pass the save checks:\n  {}", p.join("\n  ")))?;

    let mut index = load_index(out, &key)?;
    if let Some(e) = index.vipers.iter().find(|e| e.name == name && e.publisher != publisher) {
        return Err(format!("'{name}' is already published by {}; viper names are unique across the registry", e.publisher));
    }
    if let Some(e) = index.entry(publisher, &name) {
        if let Some(newest) = e.versions.iter().filter_map(|v| registry::parse_version(&v.version)).max() {
            if parsed <= newest { return Err(format!("{version} is not newer than the published {}.{}.{}", newest.0, newest.1, newest.2)); }
        }
    }

    let api_version = manifest.api_version.unwrap_or(1);
    let published_at = now();
    let mut b = Bundle {
        format: bundle::FORMAT,
        sha256: bundle::content_digest(&manifest, &source),
        manifest: manifest.clone(),
        source,
        readme,
        license: "MIT".into(),
        publisher: Some(publisher.into()),
        version: Some(version.into()),
        api_version,
        registry_signature: None,
    };
    registry::sign_bundle(&mut b, &key, key_id, &published_at)?;
    let bundle_path = out.join(publisher).join(&name).join(format!("{version}.json"));
    write_json(&bundle_path, &b, false)?;

    let v = Version { version: version.into(), sha256: b.sha256.clone(), api_version, published_at, yanked: None };
    let market = serde_json::to_value(&manifest.market).unwrap_or_default();
    match index.vipers.iter_mut().find(|e| e.publisher == publisher && e.name == name) {
        Some(e) => {
            // The listing describes the newest version. If that version is
            // later yanked, the listing still describes it; republish a fixed
            // version rather than leaving a yanked one newest.
            e.display_name = manifest.display_name.clone();
            e.description = manifest.description.clone();
            e.classes = manifest.classes.clone();
            e.venues = manifest.venues.clone();
            e.market = market;
            e.versions.push(v);
        }
        None => index.vipers.push(Entry {
            publisher: publisher.into(), name: name.clone(), display_name: manifest.display_name.clone(),
            description: manifest.description.clone(), classes: manifest.classes.clone(), venues: manifest.venues.clone(),
            market, versions: vec![v],
        }),
    }
    write_index(out, index, &key, key_id)?;
    println!("Published {publisher}/{name} {version} (digest {}…) to {}", &b.sha256[..12], bundle_path.display());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn yank(key_path: &Path, key_id: &str, out: &Path, publisher: &str, name: &str, version: &str, reason: &str) -> R<()> {
    let key = load_key(key_path)?;
    if reason.trim().is_empty() { return Err("a yank needs a reason".into()); }
    let mut index = load_index(out, &key)?;
    let v = index.vipers.iter_mut().find(|e| e.publisher == publisher && e.name == name)
        .and_then(|e| e.versions.iter_mut().find(|v| v.version == version))
        .ok_or("no such published version")?;
    v.yanked = Some(reason.trim().into());
    write_index(out, index, &key, key_id)?;
    println!("Yanked {publisher}/{name} {version}: {reason}");
    Ok(())
}

/// Check everything under `out` the way an engine of this build would: the
/// index and every listed bundle against `registry::TRUSTED_KEYS`.
fn verify(out: &Path) -> R<()> {
    let doc: SignedIndex = serde_json::from_slice(&std::fs::read(index_path(out)).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
    let index = registry::verify_index(&doc)?;
    let mut n = 0;
    for e in &index.vipers {
        for v in &e.versions {
            let p = out.join(&e.publisher).join(&e.name).join(format!("{}.json", v.version));
            let b: Bundle = serde_json::from_slice(&std::fs::read(&p).map_err(|x| format!("{}: {x}", p.display()))?).map_err(|x| format!("{}: {x}", p.display()))?;
            let s = registry::verify_bundle(&b)?.ok_or_else(|| format!("{} is unsigned", p.display()))?;
            if s.publisher != e.publisher || b.manifest.name != e.name || s.version != v.version || b.sha256 != v.sha256 {
                return Err(format!("{} does not match its index entry", p.display()));
            }
            n += 1;
        }
    }
    println!("OK: index ({} vipers) and {n} bundle(s) verify against this build's trusted keys", index.vipers.len());
    Ok(())
}
