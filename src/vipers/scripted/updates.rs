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

//! [E97] Community update notices.
//!
//! Until now the engine read the registry only when the operator opened the
//! Community card, so an operator who never looked never heard that a viper
//! they imported had a new version, or that the registry had withdrawn the
//! version they run. This checks the signed index every six hours (on unless
//! `COMMUNITY_UPDATE_CHECK` is set to false in Setup) and keeps what it found
//! for the Control Tower to show.
//!
//! It only tells. It never updates a viper, and it never switches one off,
//! even when the registry withdraws the version an operator is running: the
//! registry is a remote document, and what an operator trades stays their
//! decision. The Community card already shows the diff and the Update button.
//!
//! The request is the one the Community card makes: an anonymous GET of the
//! static, signed `index.json`, with the engine's version in the User-Agent
//! and nothing that identifies the instance. The index's signature and
//! freshness are checked as for every other read (`registry::fetch_index`).
//!
//! Notices are worked out when they are read, from the last index and the
//! scripts on disk at that moment, so an update or a delete shows at once
//! rather than at the next check.

use std::sync::RwLock;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;

use super::bundle::{self, Provenance};
use super::registry::{self, parse_version, Index};
use super::store;

/// How often the background check runs.
pub const CHECK_EVERY: Duration = Duration::from_secs(6 * 3600);
/// The first check waits this long after a start, so a restart loop does not
/// become a stream of requests and the start itself is not slowed.
pub const FIRST_CHECK_AFTER: Duration = Duration::from_secs(120);
/// The Setup switch. Unset means on.
pub const SETTING: &str = "COMMUNITY_UPDATE_CHECK";

/// Whether the background check is on: anything but an explicit off.
pub fn enabled() -> bool {
    enabled_from(std::env::var(SETTING).ok().as_deref())
}

fn enabled_from(v: Option<&str>) -> bool {
    !matches!(v.map(|s| s.trim().to_ascii_lowercase()).as_deref(), Some("false" | "0" | "off" | "no"))
}

/// One imported viper the operator should hear about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Notice {
    /// The local name (an import may have been renamed).
    pub name: String,
    pub publisher: String,
    /// The name it is published under.
    pub registry_name: String,
    pub installed: String,
    /// A newer version this build can run, if there is one.
    pub available: Option<String>,
    /// The registry's reason, when it withdrew the installed version.
    pub yanked: Option<String>,
    /// Edited here since the import: updating replaces the edits.
    pub edited: bool,
}

/// A signed import as the comparison needs it.
#[derive(Debug, Clone)]
pub struct Installed {
    pub name: String,
    pub publisher: String,
    pub registry_name: String,
    pub version: String,
    pub edited: bool,
}

/// Signed registry imports on disk. A file import carries no publisher the
/// registry vouched for, so it is never matched to a registry entry.
pub fn installed() -> Vec<Installed> {
    store::all().iter().filter_map(|s| {
        let o = s.manifest.origin.as_ref()?;
        if o.source != registry::SOURCE { return None; }
        Some(Installed {
            name: s.manifest.name.clone(),
            publisher: o.publisher.clone()?,
            registry_name: o.name.clone(),
            version: o.version.clone()?,
            edited: bundle::provenance(&s.manifest, &s.source) == Provenance::Edited,
        })
    }).collect()
}

/// What to tell the operator: each import with a newer runnable version, or
/// whose installed version the registry withdrew. Sorted by local name.
pub fn notices(index: &Index, installed: &[Installed]) -> Vec<Notice> {
    let mut out: Vec<Notice> = installed.iter().filter_map(|i| {
        let entry = index.entry(&i.publisher, &i.registry_name)?;
        // The withdrawal first: a version string that does not parse can still
        // be the one the registry withdrew, and that notice matters most.
        let yanked = entry.version(&i.version).and_then(|v| v.yanked.clone());
        let available = parse_version(&i.version).and_then(|have| entry.available()
            .filter(|v| parse_version(&v.version).is_some_and(|n| n > have))
            .map(|v| v.version.clone()));
        if available.is_none() && yanked.is_none() { return None; }
        Some(Notice {
            name: i.name.clone(),
            publisher: i.publisher.clone(),
            registry_name: i.registry_name.clone(),
            installed: i.version.clone(),
            available,
            yanked,
            edited: i.edited,
        })
    }).collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

#[derive(Default)]
struct State {
    index: Option<Index>,
    checked_at: Option<DateTime<Utc>>,
    error: Option<String>,
    /// Notices already logged, so each is logged once per process.
    logged: std::collections::HashSet<String>,
}

static STATE: RwLock<Option<State>> = RwLock::new(None);

fn with_state<R>(f: impl FnOnce(&mut State) -> R) -> R {
    let mut g = STATE.write().unwrap_or_else(|e| e.into_inner());
    f(g.get_or_insert_with(State::default))
}

/// Keep an index read elsewhere (the Community card), so the notices follow
/// what the operator just saw.
pub fn remember(index: &Index) {
    with_state(|s| {
        s.index = Some(index.clone());
        s.checked_at = Some(Utc::now());
        s.error = None;
    });
}

/// What the Control Tower reads: no network, worked out from the last index.
#[derive(Debug, Serialize)]
pub struct Snapshot {
    pub enabled: bool,
    pub checked_at: Option<DateTime<Utc>>,
    /// The last check's failure, when it failed (the last good index stays).
    pub error: Option<String>,
    pub notices: Vec<Notice>,
}

pub fn snapshot() -> Snapshot {
    let installed = installed();
    with_state(|s| Snapshot {
        enabled: enabled(),
        checked_at: s.checked_at,
        error: s.error.clone(),
        notices: s.index.as_ref().map(|i| notices(i, &installed)).unwrap_or_default(),
    })
}

/// One check: fetch, keep, and log anything new. An engine with no community
/// imports has nothing to hear about, so it does not contact the registry.
pub async fn check_now() {
    if installed().is_empty() { return; }
    match registry::fetch_index().await {
        Ok(index) => {
            let found = notices(&index, &installed());
            remember(&index);
            with_state(|s| {
                for n in &found {
                    let key = format!("{}|{}|{:?}|{:?}", n.name, n.installed, n.available, n.yanked);
                    if !s.logged.insert(key) { continue; }
                    match (&n.yanked, &n.available) {
                        (Some(reason), _) => tracing::warn!(
                            "📜 The registry withdrew {}/{} {} (installed as '{}'): {reason}. It keeps running as configured; review it in Build a Viper → Community.",
                            n.publisher, n.registry_name, n.installed, n.name),
                        (None, Some(v)) => tracing::info!(
                            "📜 Community update: {}/{} {v} is available (installed as '{}' at {}).",
                            n.publisher, n.registry_name, n.name, n.installed),
                        (None, None) => {}
                    }
                }
            });
        }
        Err(e) => {
            // Once per distinct failure: a check that keeps failing (an
            // untrusted new registry key, blocked egress) would otherwise hide
            // a withdrawal for good.
            with_state(|s| {
                if s.logged.insert(format!("error|{e}")) {
                    tracing::warn!("📜 Community update check failed: {e}");
                }
                s.error = Some(e);
            });
        }
    }
}

/// The background loop: a first check shortly after the start, then every
/// six hours. The switch is read at each turn, so turning it off in Setup
/// takes effect at the next check without a restart.
pub async fn run_update_watch() {
    tokio::time::sleep(FIRST_CHECK_AFTER).await;
    loop {
        if enabled() {
            check_now().await;
        }
        tokio::time::sleep(CHECK_EVERY).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::registry::{Entry, Version};

    fn v(version: &str, api: u32, yanked: Option<&str>) -> Version {
        Version {
            version: version.into(), sha256: "00".into(), api_version: api,
            published_at: "2026-11-10T00:00:00Z".into(), yanked: yanked.map(Into::into),
        }
    }

    fn index(versions: Vec<Version>) -> Index {
        Index {
            format: 1,
            generated_at: "2026-11-10T00:00:00Z".into(),
            vipers: vec![Entry {
                publisher: "mbordash".into(), name: "late_favorite".into(),
                display_name: "Late Favorite".into(), description: String::new(),
                classes: vec![], venues: vec![], market: serde_json::Value::Null, versions,
            }],
        }
    }

    fn have(version: &str) -> Installed {
        Installed {
            name: "late_favorite".into(), publisher: "mbordash".into(),
            registry_name: "late_favorite".into(), version: version.into(), edited: false,
        }
    }

    #[test]
    fn a_newer_version_is_a_notice() {
        let i = index(vec![v("1.0.0", 1, None), v("1.0.1", 1, None)]);
        let n = notices(&i, &[have("1.0.0")]);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].available.as_deref(), Some("1.0.1"));
        assert_eq!(n[0].yanked, None);
    }

    #[test]
    fn up_to_date_is_quiet() {
        let i = index(vec![v("1.0.0", 1, None), v("1.0.1", 1, None)]);
        assert!(notices(&i, &[have("1.0.1")]).is_empty());
    }

    #[test]
    fn versions_compare_as_numbers_not_text() {
        let i = index(vec![v("1.0.9", 1, None), v("1.0.10", 1, None)]);
        assert_eq!(notices(&i, &[have("1.0.9")])[0].available.as_deref(), Some("1.0.10"));
    }

    #[test]
    fn a_version_this_build_cannot_run_is_not_offered() {
        let i = index(vec![v("1.0.0", 1, None), v("2.0.0", bundle::API_VERSION + 1, None)]);
        assert!(notices(&i, &[have("1.0.0")]).is_empty());
    }

    #[test]
    fn a_yanked_newer_version_is_not_offered() {
        let i = index(vec![v("1.0.0", 1, None), v("1.0.1", 1, Some("bad stop"))]);
        assert!(notices(&i, &[have("1.0.0")]).is_empty());
    }

    #[test]
    fn a_withdrawn_installed_version_is_a_notice_with_its_reason() {
        let i = index(vec![v("1.0.0", 1, Some("enters on stale prices")), v("1.0.1", 1, None)]);
        let n = notices(&i, &[have("1.0.0")]);
        assert_eq!(n[0].yanked.as_deref(), Some("enters on stale prices"));
        assert_eq!(n[0].available.as_deref(), Some("1.0.1"));
    }

    #[test]
    fn a_withdrawn_version_with_nothing_newer_is_still_a_notice() {
        let i = index(vec![v("1.0.0", 1, Some("withdrawn"))]);
        let n = notices(&i, &[have("1.0.0")]);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].available, None);
    }

    #[test]
    fn an_import_the_registry_does_not_list_is_ignored() {
        let i = index(vec![v("1.0.0", 1, None)]);
        let mut other = have("1.0.0");
        other.publisher = "someone".into();
        assert!(notices(&i, &[other]).is_empty());
    }

    #[test]
    fn a_renamed_and_edited_copy_keeps_its_local_name() {
        let i = index(vec![v("1.0.0", 1, None), v("1.1.0", 1, None)]);
        let mut c = have("1.0.0");
        c.name = "my_late_favorite".into();
        c.edited = true;
        let n = notices(&i, &[c]);
        assert_eq!(n[0].name, "my_late_favorite");
        assert!(n[0].edited);
    }

    #[test]
    fn a_withdrawn_version_that_does_not_parse_is_still_a_notice() {
        let i = index(vec![v("1.0", 1, Some("withdrawn")), v("1.0.1", 1, None)]);
        let n = notices(&i, &[have("1.0")]);
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].yanked.as_deref(), Some("withdrawn"));
        assert_eq!(n[0].available, None);
    }

    #[test]
    fn the_setting_is_on_unless_turned_off() {
        assert!(enabled_from(None));
        assert!(enabled_from(Some("true")));
        assert!(enabled_from(Some("")));
        assert!(!enabled_from(Some("false")));
        assert!(!enabled_from(Some(" Off ")));
        assert!(!enabled_from(Some("0")));
    }
}
