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

//! Guard: every `config::CONST` the source tree references must be declared by
//! ALL THREE profile templates.
//!
//! `src/config.rs` is gitignored — each checkout carries a different profile — so
//! it never reaches CI. The GitHub build copies `config.balanced.rs.example` over
//! it and compiles that (`.github/workflows/rust.yml`). A constant added to a
//! developer's local `src/config.rs` but not to the templates therefore builds
//! clean locally and fails CI with `cannot find value ... in module config`
//! (observed 2026-08-12 for `TIME_DECAY_GATE_LOG_INTERVAL_SECS` and
//! `GBOOST_VETO_SCORING_BATCH`).
//!
//! `tools/generate-profiles.py` does not catch this: it only maps constants that
//! back a `DynamicConfig` field, so a purely compile-time constant passes its
//! field-count check while still breaking the build.
//!
//! This test reproduces the CI failure at `cargo test` time, against every
//! profile at once rather than only the one CI happens to use.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const PROFILES: [&str; 3] = [
    "src/config.conservative.rs.example",
    "src/config.balanced.rs.example",
    "src/config.aggressive.rs.example",
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Strip `//` line comments so a constant merely *named* in prose is not mistaken
/// for a reference. Deliberately naive — it does not track string literals or
/// block comments, which only risks over-reporting (a spurious requirement that a
/// template declare a constant), never under-reporting a real break.
fn strip_line_comments(src: &str) -> String {
    src.lines()
        .map(|l| l.split_once("//").map_or(l, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n")
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// All-caps identifiers referenced as `config::NAME` anywhere under `src/`.
/// Lowercase paths (`config::main_ticker_interval()`) and CamelCase types are
/// excluded by the all-caps requirement, leaving only constants.
fn referenced_constants() -> BTreeSet<String> {
    let mut files = Vec::new();
    rust_files(&repo_root().join("src"), &mut files);

    let mut out = BTreeSet::new();
    for file in files {
        // The local config.rs and the templates DECLARE constants; they are not
        // call sites, and including them would make the test self-satisfying.
        let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name == "config.rs" || name.starts_with("config.") {
            continue;
        }
        let Ok(src) = std::fs::read_to_string(&file) else { continue };
        let src = strip_line_comments(&src);
        for (idx, _) in src.match_indices("config::") {
            // Require a word boundary before `config`, or every reference to a
            // constant in ANOTHER module whose name ends in `config` is read as
            // one of this module's. `dynamic_config::GLOBAL_SEMANTICS_KEYS` was
            // reported as a missing `config::GLOBAL_SEMANTICS_KEYS` in all three
            // profile templates — a constant that does not belong in config.rs
            // and never will.
            let boundary_ok = idx == 0
                || !matches!(src.as_bytes()[idx - 1], b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_');
            if !boundary_ok {
                continue;
            }
            let rest = &src[idx + "config::".len()..];
            let ident: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !ident.is_empty()
                && ident.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
                && ident.chars().any(|c| c.is_ascii_uppercase())
            {
                out.insert(ident);
            }
        }
    }
    out
}

/// Names declared as `pub const NAME` at the start of a line in `path`.
fn declared_constants(path: &Path) -> BTreeSet<String> {
    let src = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    src.lines()
        .filter_map(|l| l.strip_prefix("pub const "))
        .map(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect::<String>()
        })
        .filter(|s| !s.is_empty())
        .collect()
}

#[test]
fn every_referenced_constant_exists_in_all_profile_templates() {
    let referenced = referenced_constants();
    assert!(
        referenced.len() > 100,
        "only {} config constants found — the scan is broken, not the templates",
        referenced.len()
    );

    let mut failures = Vec::new();
    for profile in PROFILES {
        let path = repo_root().join(profile);
        let declared = declared_constants(&path);
        let missing: Vec<&String> = referenced.difference(&declared).collect();
        if !missing.is_empty() {
            failures.push(format!(
                "{profile} is missing {} constant(s): {}",
                missing.len(),
                missing.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "profile templates are incomplete — CI copies config.balanced.rs.example \
         over the gitignored src/config.rs, so these WILL fail the GitHub build:\n  {}",
        failures.join("\n  ")
    );
}

/// The three templates must declare the SAME set of constants. A constant added
/// to one but not the others compiles under whichever profile the author happens
/// to run locally and breaks the moment a different profile is selected — a
/// failure the reference scan above cannot see, because an unreferenced constant
/// is still part of the profile contract.
#[test]
fn profile_templates_declare_identical_constant_sets() {
    let sets: Vec<(&str, BTreeSet<String>)> = PROFILES
        .iter()
        .map(|p| (*p, declared_constants(&repo_root().join(p))))
        .collect();

    let (base_name, base) = &sets[0];
    for (name, other) in &sets[1..] {
        let only_base: Vec<&str> = base.difference(other).map(|s| s.as_str()).collect();
        let only_other: Vec<&str> = other.difference(base).map(|s| s.as_str()).collect();
        assert!(
            only_base.is_empty() && only_other.is_empty(),
            "profile templates disagree:\n  only in {base_name}: {}\n  only in {name}: {}",
            only_base.join(", "),
            only_other.join(", "),
        );
    }
}
