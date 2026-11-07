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

//! The process-wide set of operator scripts.
//!
//! Scripts live on the data volume (`$DRADIS_DATA_DIR/vipers/<name>.rhai` and
//! `<name>.json`), so they survive an AMI upgrade and travel in a backup. They
//! are compiled once at startup and again on every save; the adapters look the
//! current version up by name each tick, so a save applies at the next tick
//! without a restart and a squadron rebuild picks up nothing stale.
//!
//! A save is all-or-nothing: the manifest and the script are checked and
//! compiled before anything touches disk, so a bad edit is reported to the
//! page and the running version keeps running.
//!
//! Also kept here, because the adapters are rebuilt with their squadrons (when
//! a squadron moves to a new market) and must not lose it: each viper's `this` state per
//! squadron, and the last context each squadron handed a script, which is what
//! the dry run evaluates against.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use chrono::{DateTime, Utc};
use rhai::{Dynamic, Map, AST};
use serde::Serialize;
use tracing::{info, warn};

use super::manifest::Manifest;
use super::runtime;

/// The strategy-name prefix that keeps operator names apart from the built-in
/// vipers (`MomentumStrategy`), the adoption priority list and the advisor's
/// key prefixes.
pub const NAME_PREFIX: &str = "Script:";
/// The registry kind prefix (`script:<name>`).
pub const KIND_PREFIX: &str = "script:";
/// Largest script source accepted.
pub const MAX_SOURCE_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub struct Script {
    pub manifest: Manifest,
    pub source: String,
    pub ast: Arc<AST>,
    pub hash: String,
    pub saved_at: DateTime<Utc>,
}

/// One script on disk that did not load, kept so the page can show why.
#[derive(Debug, Clone, Serialize)]
pub struct Broken {
    pub name: String,
    pub error: String,
}

#[derive(Default)]
struct Store {
    scripts: BTreeMap<String, Arc<Script>>,
    broken: BTreeMap<String, Broken>,
    /// Test scripts the adapter can find by name but no class list ever
    /// includes, so a test's script never joins another test's squadron.
    #[cfg(test)]
    hidden: BTreeMap<String, Arc<Script>>,
}

fn store() -> &'static RwLock<Store> {
    static S: OnceLock<RwLock<Store>> = OnceLock::new();
    S.get_or_init(|| RwLock::new(Store::default()))
}

fn read() -> std::sync::RwLockReadGuard<'static, Store> {
    store().read().unwrap_or_else(|p| p.into_inner())
}

fn write() -> std::sync::RwLockWriteGuard<'static, Store> {
    store().write().unwrap_or_else(|p| p.into_inner())
}

pub fn dir() -> PathBuf {
    PathBuf::from(std::env::var("DRADIS_DATA_DIR").unwrap_or_else(|_| "./data".to_string())).join("vipers")
}

pub fn strategy_name(name: &str) -> String { format!("{NAME_PREFIX}{name}") }

/// Bumped after every change to the running set (save, delete, load), so a
/// running squadron can tell cheaply, once a tick, that it should look for a
/// script to attach. Bumped while the write lock is still held and after the
/// change, so a reader that sees the new number and then takes the read lock
/// sees the change too.
static GENERATION: AtomicU64 = AtomicU64::new(0);

pub fn generation() -> u64 { GENERATION.load(Ordering::Acquire) }

fn bump() { GENERATION.fetch_add(1, Ordering::AcqRel); }

/// The script name inside a strategy name, if it is one.
pub fn script_of(strategy_name: &str) -> Option<&str> { strategy_name.strip_prefix(NAME_PREFIX) }

/// `script:<name>` as a `'static` string, interned once per name for the life
/// of the process. `strategy_name_to_kind` hands out `&'static str`, and a
/// handful of names an operator ever creates is a bounded leak.
pub fn kind_of(name: &str) -> &'static str {
    static KINDS: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let mut m = KINDS.get_or_init(Default::default).lock().unwrap_or_else(|p| p.into_inner());
    if let Some(k) = m.get(name) { return k; }
    let k: &'static str = Box::leak(format!("{KIND_PREFIX}{name}").into_boxed_str());
    m.insert(name.to_string(), k);
    k
}

pub fn get(name: &str) -> Option<Arc<Script>> {
    let s = read();
    #[cfg(test)]
    if let Some(h) = s.hidden.get(name) { return Some(Arc::clone(h)); }
    s.scripts.get(name).cloned()
}

/// Register a script for a test without writing it to disk or adding it to any
/// class list.
#[cfg(test)]
pub fn insert_hidden_for_test(manifest: Manifest, source: &str) -> Arc<Script> {
    let ast = check(&manifest, source).unwrap_or_else(|p| panic!("{p:?}"));
    let s = Arc::new(Script { hash: runtime::source_hash(source), manifest, source: source.to_string(), ast: Arc::new(ast), saved_at: Utc::now() });
    write().hidden.insert(s.manifest.name.clone(), Arc::clone(&s));
    s
}

pub fn names() -> Vec<String> { read().scripts.keys().cloned().collect() }

pub fn all() -> Vec<Arc<Script>> { read().scripts.values().cloned().collect() }

pub fn broken() -> Vec<Broken> { read().broken.values().cloned().collect() }

/// Scripts that attach to squadrons of `class` on this venue build. Never for
/// a Helm squadron, whatever a manifest says.
pub fn names_for_class(class: &str) -> Vec<String> {
    names_for_class_on(class, crate::venues::venue_name())
}

pub fn names_for_class_on(class: &str, venue: &str) -> Vec<String> {
    if class == crate::vipers::helm_impl::KIND { return Vec::new(); }
    read().scripts.values()
        .filter(|s| s.manifest.classes.iter().any(|c| c == class) && s.manifest.runs_on(venue))
        .map(|s| s.manifest.name.clone())
        .collect()
}

/// Check a manifest and source together; every problem, or the compiled AST.
pub fn check(manifest: &Manifest, source: &str) -> Result<AST, Vec<String>> {
    let mut problems = manifest.problems();
    if source.len() > MAX_SOURCE_BYTES {
        problems.push(format!("the script is over {} KB", MAX_SOURCE_BYTES / 1024));
    }
    let ast = match runtime::compile(source) {
        Ok(a) => Some(a),
        Err(e) => { problems.push(e); None }
    };
    match ast {
        Some(a) if problems.is_empty() => Ok(a),
        _ => Err(problems),
    }
}

/// [`check`] plus the rules a script must pass to be saved but that a script
/// already on disk is never refused for: refusing one at startup would stop it
/// managing positions it holds.
///
/// [B92] A read of a parameter the viper does not define is `()` at run time,
/// and the first arithmetic on it fails every tick with a message that names an
/// operator, not the parameter. Check and Save refuse it by name instead.
pub fn check_for_save(manifest: &Manifest, source: &str) -> Result<AST, Vec<String>> {
    let undefined: Vec<String> = params_read(source).into_iter()
        .filter(|name| !manifest.params.contains_key(name))
        .map(|name| format!(
            "the script reads ctx.params.{name}, which is not one of this viper's parameters; add it or fix the name"
        ))
        .collect();
    match check(manifest, source) {
        Ok(ast) if undefined.is_empty() => Ok(ast),
        Ok(_) => Err(undefined),
        Err(mut p) => { p.extend(undefined); Err(p) }
    }
}

/// Every `ctx.params.<name>` the source reads, once each, in order of first
/// use. Only code counts: comments (nested block comments included), string
/// and character literals are skipped, while the code inside a template's
/// `${...}` is a real read and counts. A map method (`ctx.params.len()`,
/// `.contains("x")`, `.keys()`) is not a parameter read.
fn params_read(source: &str) -> Vec<String> {
    static READ: OnceLock<regex::Regex> = OnceLock::new();
    let read = READ.get_or_init(|| {
        regex::Regex::new(r"\bctx\s*\.\s*params\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)(\s*\()?").expect("static regex")
    });
    let code = code_only(source);
    let mut out: Vec<String> = Vec::new();
    for c in read.captures_iter(&code) {
        if c.get(2).is_some() { continue; }
        let name = c[1].to_string();
        if !out.contains(&name) { out.push(name); }
    }
    out
}

/// The source with everything that is not code blanked to spaces: `//` and
/// nested `/* */` comments, `"..."` strings, `'c'` characters, `#"..."#` raw
/// strings, and the text of `` `...` `` templates, whose `${...}` stays.
fn code_only(source: &str) -> String {
    let c: Vec<char> = source.chars().collect();
    let mut out = String::with_capacity(source.len());
    let mut i = 0;
    // Brace depth of each open `${`, innermost last: a `}` at that depth
    // returns to the template's text.
    let mut interp: Vec<usize> = Vec::new();
    let mut depth = 0usize;
    let mut in_template = false;
    let blank = |out: &mut String, ch: char| out.push(if ch == '\n' { '\n' } else { ' ' });
    while i < c.len() {
        let ch = c[i];
        let next = c.get(i + 1).copied();
        if in_template {
            if ch == '`' { in_template = false; blank(&mut out, ch); i += 1; continue; }
            if ch == '$' && next == Some('{') {
                in_template = false;
                depth += 1;
                interp.push(depth);
                out.push_str("  ");
                i += 2;
                continue;
            }
            blank(&mut out, ch);
            i += 1;
            continue;
        }
        match (ch, next) {
            ('/', Some('/')) => {
                while i < c.len() && c[i] != '\n' { blank(&mut out, c[i]); i += 1; }
            }
            ('/', Some('*')) => {
                let mut level = 0usize;
                while i < c.len() {
                    if c[i] == '/' && c.get(i + 1) == Some(&'*') { level += 1; out.push_str("  "); i += 2; continue; }
                    if c[i] == '*' && c.get(i + 1) == Some(&'/') {
                        level -= 1; out.push_str("  "); i += 2;
                        if level == 0 { break; }
                        continue;
                    }
                    blank(&mut out, c[i]);
                    i += 1;
                }
            }
            ('"', _) | ('\'', _) => {
                let q = ch;
                blank(&mut out, ch);
                i += 1;
                while i < c.len() && c[i] != q {
                    if c[i] == '\\' && i + 1 < c.len() { blank(&mut out, c[i]); i += 1; }
                    blank(&mut out, c[i]);
                    i += 1;
                }
                if i < c.len() { blank(&mut out, c[i]); i += 1; }
            }
            ('#', Some('"')) | ('#', Some('#')) => {
                // A raw string: n hashes, a quote, and the text up to a quote
                // followed by the same n hashes. Anything else is not one.
                let mut n = 0;
                while c.get(i + n) == Some(&'#') { n += 1; }
                if c.get(i + n) != Some(&'"') { out.push(ch); i += 1; continue; }
                let start = i;
                i += n + 1;
                let mut closed = false;
                while i < c.len() {
                    if c[i] == '"' && (0..n).all(|k| c.get(i + 1 + k) == Some(&'#')) { i += n + 1; closed = true; break; }
                    i += 1;
                }
                if !closed { i = c.len(); }
                for &x in &c[start..i] { blank(&mut out, x); }
            }
            ('`', _) => { in_template = true; blank(&mut out, ch); i += 1; }
            ('{', _) => { depth += 1; out.push(ch); i += 1; }
            ('}', _) => {
                if interp.last() == Some(&depth) { interp.pop(); in_template = true; blank(&mut out, ch); }
                else { out.push(ch); }
                depth = depth.saturating_sub(1);
                i += 1;
            }
            _ => { out.push(ch); i += 1; }
        }
    }
    out
}

fn write_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

/// Check, write and swap in a script. The running version is untouched unless
/// every step succeeds.
pub fn save(manifest: Manifest, source: String) -> Result<Arc<Script>, Vec<String>> {
    save_in(&dir(), manifest, source)
}

pub fn save_in(dir: &Path, manifest: Manifest, source: String) -> Result<Arc<Script>, Vec<String>> {
    // The name becomes two file names in `dir`. `check` refuses a bad one
    // too; this guard sits beside the path it protects, as in `delete_in`, so
    // no later change to `check` can turn a request body into a path outside
    // the folder.
    if !super::manifest::valid_name(&manifest.name) {
        return Err(vec![format!("name '{}' must be 3-32 characters of a-z, 0-9 and _, starting with a letter", manifest.name)]);
    }
    let ast = check_for_save(&manifest, &source)?;
    let name = manifest.name.clone();
    std::fs::create_dir_all(dir).map_err(|e| vec![format!("cannot create {}: {e}", dir.display())])?;
    let json = serde_json::to_vec_pretty(&manifest).map_err(|e| vec![e.to_string()])?;
    // Script first, manifest second: the loader keys on the manifest, so a
    // crash between the two leaves the previous manifest naming a script that
    // is at worst the new one, already checked.
    write_atomically(&dir.join(format!("{name}.rhai")), source.as_bytes())
        .and_then(|_| write_atomically(&dir.join(format!("{name}.json")), &json))
        .map_err(|e| vec![format!("cannot write the script: {e}")])?;
    let script = Arc::new(Script {
        hash: runtime::source_hash(&source),
        manifest, source, ast: Arc::new(ast), saved_at: Utc::now(),
    });
    let mut s = write();
    s.broken.remove(&name);
    let replaced = s.scripts.insert(name.clone(), Arc::clone(&script)).is_some();
    bump();
    info!("📜 Script viper '{}' {} (version {})", name, if replaced { "updated" } else { "added" }, script.hash);
    Ok(script)
}

/// Remove a script from disk and from the running set.
pub fn delete(name: &str) -> std::io::Result<bool> { delete_in(&dir(), name) }

pub fn delete_in(dir: &Path, name: &str) -> std::io::Result<bool> {
    if !super::manifest::valid_name(name) { return Ok(false); }
    for ext in ["json", "rhai"] {
        let p = dir.join(format!("{name}.{ext}"));
        if p.exists() { std::fs::remove_file(p)?; }
    }
    let mut s = write();
    let had = s.scripts.remove(name).is_some() | s.broken.remove(name).is_some();
    bump();
    if had { info!("📜 Script viper '{name}' deleted"); }
    Ok(had)
}

/// Load every script in the folder. Called once at startup; a script that does
/// not load is listed with its error and does not run.
pub fn load_all() -> usize { load_all_in(&dir()) }

pub fn load_all_in(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    let mut loaded = BTreeMap::new();
    let mut broken = BTreeMap::new();
    for e in entries.flatten() {
        let path = e.path();
        if path.extension().and_then(|x| x.to_str()) != Some("json") { continue; }
        let Some(stem) = path.file_stem().and_then(|x| x.to_str()).map(str::to_string) else { continue };
        let result = (|| -> Result<Arc<Script>, String> {
            let manifest: Manifest = serde_json::from_slice(&std::fs::read(&path).map_err(|e| e.to_string())?)
                .map_err(|e| format!("manifest: {e}"))?;
            if manifest.name != stem {
                return Err(format!("manifest name '{}' does not match its file name '{stem}'", manifest.name));
            }
            let source = std::fs::read_to_string(dir.join(format!("{stem}.rhai"))).map_err(|e| format!("script: {e}"))?;
            let ast = check(&manifest, &source).map_err(|p| p.join("; "))?;
            Ok(Arc::new(Script { hash: runtime::source_hash(&source), manifest, source, ast: Arc::new(ast), saved_at: Utc::now() }))
        })();
        match result {
            Ok(s) => { loaded.insert(stem, s); }
            Err(error) => {
                warn!("📜 Script viper '{stem}' did not load: {error}");
                broken.insert(stem.clone(), Broken { name: stem, error });
            }
        }
    }
    let n = loaded.len();
    if n > 0 {
        info!("📜 Loaded {n} script viper(s) from {}: {}", dir.display(), loaded.keys().cloned().collect::<Vec<_>>().join(", "));
    }
    // Added to the running set rather than replacing it: this runs once at
    // startup on an empty set, and tests that load folders in parallel must
    // not wipe each other's scripts.
    let mut s = write();
    for k in loaded.keys() { s.broken.remove(k); }
    s.scripts.extend(loaded);
    s.broken.extend(broken);
    bump();
    n
}

// ── Per-squadron state the adapters must not lose on a rebuild ──────────────

type StateKey = (String, String); // (squadron, script)

fn states() -> &'static Mutex<HashMap<StateKey, Dynamic>> {
    static S: OnceLock<Mutex<HashMap<StateKey, Dynamic>>> = OnceLock::new();
    S.get_or_init(Default::default)
}

/// Run `f` with this script's `this` state on this squadron.
pub fn with_state<R>(squadron: &str, script: &str, f: impl FnOnce(&mut Dynamic) -> R) -> R {
    let mut m = states().lock().unwrap_or_else(|p| p.into_inner());
    let st = m.entry((squadron.to_string(), script.to_string())).or_insert_with(|| Map::new().into());
    f(st)
}

/// A copy of the state, for the dry run, which must not change the real one.
pub fn state_copy(squadron: &str, script: &str) -> Dynamic {
    states().lock().unwrap_or_else(|p| p.into_inner())
        .get(&(squadron.to_string(), script.to_string())).cloned()
        .unwrap_or_else(|| Map::new().into())
}

/// Clear a script's state everywhere (the page's "reset state").
pub fn reset_state(script: &str) {
    states().lock().unwrap_or_else(|p| p.into_inner()).retain(|(_, s), _| s != script);
}

/// The last context and positions a squadron handed a script, for the dry run.
#[derive(Clone)]
pub struct LastContext {
    pub at: DateTime<Utc>,
    pub ctx: Map,
}

fn contexts() -> &'static Mutex<HashMap<String, LastContext>> {
    static C: OnceLock<Mutex<HashMap<String, LastContext>>> = OnceLock::new();
    C.get_or_init(Default::default)
}

pub fn note_context(squadron: &str, ctx: &Map) {
    contexts().lock().unwrap_or_else(|p| p.into_inner())
        .insert(squadron.to_string(), LastContext { at: Utc::now(), ctx: ctx.clone() });
}

pub fn last_context(squadron: &str) -> Option<LastContext> {
    contexts().lock().unwrap_or_else(|p| p.into_inner()).get(squadron).cloned()
}

pub fn context_squadrons() -> Vec<(String, DateTime<Utc>)> {
    contexts().lock().unwrap_or_else(|p| p.into_inner()).iter().map(|(k, v)| (k.clone(), v.at)).collect()
}

// ── Where each script runs ────────────────────────────────────────────────────

/// One squadron loop running a script, as the Build a Viper page shows it.
#[derive(Debug, Clone, Serialize)]
pub struct Attachment {
    pub script: String,
    pub squadron: String,
    /// The key the viper status registry files this squadron's rows under.
    pub status_asset: String,
    pub class: String,
    pub attached_at: DateTime<Utc>,
    /// Attached by a running loop after a save, not when the loop started.
    pub hot: bool,
    #[serde(skip)]
    loop_id: u64,
}

/// One running squadron loop, whether or not it runs any script, so the page
/// can tell "waiting for a squadron" from "no squadron of that class".
#[derive(Debug, Clone, Serialize)]
pub struct RunningLoop {
    pub squadron: String,
    pub class: String,
    #[serde(skip)]
    loop_id: u64,
}

fn loops() -> &'static Mutex<Vec<RunningLoop>> {
    static L: OnceLock<Mutex<Vec<RunningLoop>>> = OnceLock::new();
    L.get_or_init(Default::default)
}

pub(crate) fn note_loop(loop_id: u64, squadron: &str, class: &str) {
    loops().lock().unwrap_or_else(|p| p.into_inner())
        .push(RunningLoop { squadron: squadron.to_string(), class: class.to_string(), loop_id });
}

/// The squadron loops running now, oldest first.
pub fn running_loops() -> Vec<RunningLoop> {
    loops().lock().unwrap_or_else(|p| p.into_inner()).clone()
}

fn attachments() -> &'static Mutex<Vec<Attachment>> {
    static A: OnceLock<Mutex<Vec<Attachment>>> = OnceLock::new();
    A.get_or_init(Default::default)
}

pub(crate) fn note_attached(loop_id: u64, script: &str, squadron: &str, status_asset: &str, class: &str, hot: bool) {
    let mut a = attachments().lock().unwrap_or_else(|p| p.into_inner());
    if a.iter().any(|x| x.loop_id == loop_id && x.script == script) { return; }
    a.push(Attachment {
        script: script.to_string(), squadron: squadron.to_string(), status_asset: status_asset.to_string(),
        class: class.to_string(), attached_at: Utc::now(), hot, loop_id,
    });
}

/// A squadron loop ended (rotation, stand-down): its attachments go with it.
/// Keyed by loop, so a loop that ends after its successor started cannot
/// remove the successor's rows.
pub(crate) fn forget_loop(loop_id: u64) {
    loops().lock().unwrap_or_else(|p| p.into_inner()).retain(|x| x.loop_id != loop_id);
    attachments().lock().unwrap_or_else(|p| p.into_inner()).retain(|x| x.loop_id != loop_id);
}

/// Every running attachment of a script.
pub fn attachments_of(script: &str) -> Vec<Attachment> {
    attachments().lock().unwrap_or_else(|p| p.into_inner())
        .iter().filter(|x| x.script == script).cloned().collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Test scripts are declared for a venue other than the one being built,
    /// so they never join a real class's viper list in another test.
    pub(crate) fn other_venue() -> &'static str {
        let here = crate::venues::venue_name();
        super::super::manifest::VENUES.iter().copied().find(|v| *v != here).unwrap()
    }

    fn manifest(name: &str) -> Manifest {
        serde_json::from_value(serde_json::json!({
            "name": name, "display_name": "Test", "classes": ["crypto"], "max_exposure_usdc": 5,
            "venues": [other_venue()]
        })).unwrap()
    }
    const OK: &str = "fn entry(ctx) { () } fn exit(ctx, pos) { () }";

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("dradis-scripts-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn a_name_that_is_not_a_plain_identifier_never_reaches_the_filesystem() {
        let d = tmp("pathname");
        for bad in ["../escape", "a/b", "..", "x\\y", "/etc/passwd"] {
            let mut m = manifest("store_path_ok");
            m.name = bad.to_string();
            assert!(save_in(&d, m, OK.into()).is_err(), "{bad} must be refused");
        }
        assert!(!d.exists() || std::fs::read_dir(&d).unwrap().next().is_none(), "nothing written");
        assert!(!std::env::temp_dir().join("escape.rhai").exists());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn a_read_of_an_undefined_parameter_is_refused_by_name() {
        let mut m = manifest("store_params");
        m.params = serde_json::from_value(serde_json::json!({
            "hourly_vol": { "type": "decimal", "default": 0.003, "description": "" }
        })).unwrap();
        // The B92 case: code saved before its parameter was added.
        let src = "fn entry(ctx) { let x = ctx.oracle - ctx.params.basis; let v = ctx.params.hourly_vol; () }\n\
                   fn exit(ctx, pos) { () }";
        let err = check_for_save(&m, src).unwrap_err();
        assert_eq!(err.len(), 1, "{err:?}");
        assert!(err[0].contains("ctx.params.basis"), "{err:?}");
        // Comments do not count; a string template does.
        let ok = "// ctx.params.basis was here\n/* and ctx.params.gone */\n\
                  fn entry(ctx) { #{ hold: `vol ${ctx.params.hourly_vol}` } } fn exit(ctx, pos) { () }";
        assert!(check_for_save(&m, ok).is_ok());
        let templ = "fn entry(ctx) { #{ hold: `x ${ctx.params.missing}` } } fn exit(ctx, pos) { () }";
        assert!(check_for_save(&m, templ).unwrap_err()[0].contains("ctx.params.missing"));
        // A script already on disk is never refused for it: it may hold positions.
        assert!(check(&m, src).is_ok());
        assert_eq!(params_read("ctx . params . a + ctx.params.b + ctx.params.a"), vec!["a", "b"]);
        // [review] Map methods are not reads; strings, nested comments and raw
        // strings hide nothing and trip nothing; a `//` inside a string does not
        // hide a read after it on the same line.
        assert!(params_read("let n = ctx.params.len(); let k = ctx.params.contains (\"x\");").is_empty());
        assert!(params_read(r#"#{ hold: "set ctx.params.basis first" }"#).is_empty());
        assert!(params_read("/* a /* b */ ctx.params.old */ let x = 1;").is_empty());
        assert!(params_read(r##"let r = #"ctx.params.raw"#;"##).is_empty());
        assert_eq!(params_read(r#"let u = "http://x"; let v = ctx.params.zzz;"#), vec!["zzz"]);
        assert_eq!(params_read("let s = `a ${ctx.params.p + #{x: 1}.x} b ${ctx.params.q}`; ctx.params.r"), vec!["p", "q", "r"]);
        assert_eq!(params_read("let c = '\"'; let v = ctx.params.after_char;"), vec!["after_char"]);
    }

    #[test]
    fn a_bad_save_keeps_the_running_version_and_reports_every_problem() {
        let d = tmp("save");
        let first = save_in(&d, manifest("store_keep"), OK.into()).unwrap();
        let mut bad = manifest("store_keep");
        bad.max_exposure_usdc = rust_decimal::Decimal::ZERO;
        let err = save_in(&d, bad, "fn entry(ctx) { let = ; }".into()).unwrap_err();
        assert_eq!(err.len(), 2, "manifest and syntax problems together: {err:?}");
        assert_eq!(get("store_keep").unwrap().hash, first.hash, "the running version is untouched");
        assert_eq!(std::fs::read_to_string(d.join("store_keep.rhai")).unwrap(), OK, "nothing written");
        assert!(delete_in(&d, "store_keep").unwrap());
        assert!(get("store_keep").is_none());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn the_loader_runs_good_scripts_and_lists_broken_ones() {
        let d = tmp("load");
        save_in(&d, manifest("store_good"), OK.into()).unwrap();
        std::fs::write(d.join("store_bad.json"), serde_json::to_vec(&manifest("store_bad")).unwrap()).unwrap();
        std::fs::write(d.join("store_bad.rhai"), "fn entry(ctx) { () }").unwrap();
        std::fs::write(d.join("store_misnamed.json"), serde_json::to_vec(&manifest("other_name")).unwrap()).unwrap();
        assert_eq!(load_all_in(&d), 1);
        assert!(get("store_good").is_some());
        let b = broken();
        assert!(b.iter().any(|x| x.name == "store_bad" && x.error.contains("fn exit")), "{b:?}");
        assert!(b.iter().any(|x| x.name == "store_misnamed" && x.error.contains("does not match")), "{b:?}");
        assert!(names_for_class_on("crypto", other_venue()).contains(&"store_good".to_string()));
        assert!(!names_for_class("crypto").contains(&"store_good".to_string()), "not on a venue it does not declare");
        assert!(names_for_class_on(crate::vipers::helm_impl::KIND, other_venue()).is_empty(), "never on a Helm squadron");
        for n in ["store_good", "store_bad", "store_misnamed"] { let _ = delete_in(&d, n); }
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn kinds_are_interned_and_names_round_trip() {
        assert!(std::ptr::eq(kind_of("abc"), kind_of("abc")));
        assert_eq!(kind_of("abc"), "script:abc");
        assert_eq!(script_of(&strategy_name("abc")), Some("abc"));
        assert_eq!(script_of("MomentumStrategy"), None);
    }
}
