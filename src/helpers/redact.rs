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

//! Redaction helpers for values that reach the log.
//!
//! Logs leave the machine. The documented way to report a problem is to run
//! `docker logs dradis` and send the output to support, so anything printed here
//! should be assumed to end up in a support inbox, a bug report, or a pasted
//! terminal buffer. A credential must not be in it.

/// Strip the secret-bearing part of an endpoint URL, keeping only scheme and host.
///
/// Every mainstream Polygon RPC provider puts the API key in the URL itself, and
/// each one puts it somewhere different:
///
/// ```text
/// https://polygon-mainnet.g.alchemy.com/v2/<KEY>       key in the path
/// https://polygon-mainnet.infura.io/v3/<KEY>           key in the path
/// https://<name>.matic.quiknode.pro/<TOKEN>/           token in the path
/// https://mainnet.helius-rpc.com/?api-key=<KEY>        key in the query
/// ```
///
/// Rather than pattern-match each provider (and miss the next one), keep the
/// parts that are never secret — scheme and host — and drop everything after
/// them. That still answers the question the log line exists to answer, which is
/// which provider and which network the engine is talking to.
///
/// A string that does not look like a URL is redacted wholesale rather than
/// passed through, so a malformed or unexpected value cannot leak by falling off
/// the end of the parser.
pub fn redact_endpoint(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return "<redacted>".to_string();
    };
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if host.is_empty() {
        return "<redacted>".to_string();
    }
    // Credentials can also ride in the userinfo section (`user:pass@host`).
    let host = host.rsplit_once('@').map_or(host, |(_, h)| h);
    format!("{scheme}://{host}/…")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_key_in_path() {
        assert_eq!(
            redact_endpoint("https://polygon-mainnet.g.alchemy.com/v2/SECRETKEY123"),
            "https://polygon-mainnet.g.alchemy.com/…",
        );
        assert_eq!(
            redact_endpoint("https://polygon-mainnet.infura.io/v3/abc123"),
            "https://polygon-mainnet.infura.io/…",
        );
    }

    #[test]
    fn redacts_key_in_query() {
        assert_eq!(
            redact_endpoint("https://mainnet.helius-rpc.com/?api-key=SECRETKEY123"),
            "https://mainnet.helius-rpc.com/…",
        );
    }

    #[test]
    fn redacts_userinfo() {
        assert_eq!(
            redact_endpoint("https://user:hunter2@rpc.example.com/v2/key"),
            "https://rpc.example.com/…",
        );
    }

    #[test]
    fn keeps_host_when_there_is_no_secret() {
        assert_eq!(
            redact_endpoint("https://polygon-rpc.com"),
            "https://polygon-rpc.com/…",
        );
    }

    #[test]
    fn non_url_is_redacted_whole() {
        assert_eq!(redact_endpoint("not-a-url"), "<redacted>");
        assert_eq!(redact_endpoint(""), "<redacted>");
        assert_eq!(redact_endpoint("https://"), "<redacted>");
    }

    /// The regression this module exists for: no substring of the original
    /// secret survives into the redacted form.
    #[test]
    fn secret_never_survives() {
        let secret = "PPMQIewap9ta1XJfgnfnj";
        let out = redact_endpoint(&format!(
            "https://polygon-mainnet.g.alchemy.com/v2/{secret}"
        ));
        assert!(!out.contains(secret), "leaked secret in {out}");
    }
}
