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

//! What a market's title says about it, on every venue. `helpers::market` is
//! the Polymarket International discovery module and is compiled only on that
//! build; the title rules the reconciliation sweep and the Markets page share
//! have to live where every venue can reach them.

/// Infer the underlying asset from a market title, by whole word.
///
/// "Bitcoin Up or Down on June 7?" is btc, "Will ETH exceed ..." is eth. Only
/// bitcoin, ethereum and solana are recognized; anything else is `None`, which
/// callers treat as "not a crypto market" rather than guessing. Whole-word
/// matching keeps "Netherlands" from reading as eth.
pub fn infer_asset_from_title(title: &str) -> Option<&'static str> {
    let normalized: String = title
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { ' ' })
        .collect();
    let has_word = |needle: &str| normalized.split_whitespace().any(|w| w == needle);

    if has_word("btc") || has_word("bitcoin") {
        Some("btc")
    } else if has_word("eth") || has_word("ethereum") {
        Some("eth")
    } else if has_word("sol") || has_word("solana") {
        Some("sol")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::infer_asset_from_title;

    #[test]
    fn titles_name_their_underlying_by_whole_word() {
        assert_eq!(infer_asset_from_title("Bitcoin Up or Down - September 17, 4AM ET"), Some("btc"));
        assert_eq!(infer_asset_from_title("Will ETH exceed $5,000 on Friday?"), Some("eth"));
        assert_eq!(infer_asset_from_title("Solana Up or Down on June 7?"), Some("sol"));
        assert_eq!(infer_asset_from_title("Will the Netherlands win?"), None, "a substring is not a word");
        assert_eq!(infer_asset_from_title("Lakers vs Celtics"), None);
    }
}
