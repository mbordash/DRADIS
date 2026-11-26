//! [E110] The crypto underlyings DRADIS can fly, in one table.
//!
//! Every venue and feed spells a coin its own way: Kalshi by a ticker prefix
//! (`KXXRP`) and a title word ("Ripple price at ..."), Polymarket by a slug
//! (`xrp-updown-15m-...`, `bitcoin-up-or-down-...`) and a title word ("XRP Up
//! or Down"), Binance and OKX by a pair. Before this table each spelling lived
//! in its own three-arm `match` with a Bitcoin fallback, so a coin one site did
//! not know read Bitcoin's spot in silence, and the operator's list was checked
//! against a constant that named three coins. A coin is now one row here, and
//! every site asks the table.

/// One supported underlying.
#[derive(Debug)]
pub struct Coin {
    /// Lower-case slug: the operator's spelling in `TRADE_COINS`, the squadron
    /// id prefix (`xrp-open`), the database file name (`xrp-dradis.db`), and
    /// Polymarket's stem for its 15-minute markets (`xrp-updown-15m-...`) and,
    /// where the coin has hourly markets there, its 4-hour windows.
    pub slug: &'static str,
    /// Upper-case ticker, as a market title prints it.
    pub symbol: &'static str,
    /// The coin's name as a market title spells it ("Dogecoin").
    pub name: &'static str,
    /// Other words a market title or slug uses for this coin, lower-case
    /// ("ripple" on Kalshi). The slug, symbol and name are always words of it.
    pub aliases: &'static [&'static str],
    /// Polymarket's stem for its hourly and daily "Up or Down" markets
    /// (`bitcoin-up-or-down-november-26-2026-7pm-et`), or `None` where
    /// Polymarket lists no hourly or daily market on the coin (Dogecoin today:
    /// 15-minute markets only). A `None` coin is left out of the hourly probes.
    pub polymarket_hourly_slug: Option<&'static str>,
    /// Binance spot pair, upper-case.
    pub binance_pair: &'static str,
    /// OKX perpetual swap instrument.
    pub okx_swap: &'static str,
    /// Kalshi ticker prefix shared by every series on the coin.
    pub kalshi_prefix: &'static str,
    /// Kalshi's hourly strike series.
    pub kalshi_hourly: &'static str,
    /// Does the bare ticker name the coin in a market title? True for most:
    /// "BTC/USDT above 60k" is Bitcoin. False for DOGE, which is also the
    /// Department of Government Efficiency ("Will DOGE cut $1T?" is a politics
    /// market), so only the word "Dogecoin" names that coin in a title. The
    /// operator's own spelling in `TRADE_COINS` is unaffected (see [`find`]).
    pub ticker_names_it: bool,
}

/// The table. Order matters where a title names several coins: the first row
/// whose word appears wins in the single-pass detectors, and the slug list the
/// operator sees prints in this order.
pub const COINS: &[Coin] = &[
    Coin {
        slug: "btc", symbol: "BTC", name: "Bitcoin", aliases: &[],
        polymarket_hourly_slug: Some("bitcoin"), binance_pair: "BTCUSDT", okx_swap: "BTC-USDT-SWAP",
        kalshi_prefix: "KXBTC", kalshi_hourly: "KXBTCD", ticker_names_it: true,
    },
    Coin {
        slug: "eth", symbol: "ETH", name: "Ethereum", aliases: &[],
        polymarket_hourly_slug: Some("ethereum"), binance_pair: "ETHUSDT", okx_swap: "ETH-USDT-SWAP",
        kalshi_prefix: "KXETH", kalshi_hourly: "KXETHD", ticker_names_it: true,
    },
    Coin {
        slug: "sol", symbol: "SOL", name: "Solana", aliases: &[],
        polymarket_hourly_slug: Some("solana"), binance_pair: "SOLUSDT", okx_swap: "SOL-USDT-SWAP",
        kalshi_prefix: "KXSOL", kalshi_hourly: "KXSOLD", ticker_names_it: true,
    },
    Coin {
        slug: "xrp", symbol: "XRP", name: "XRP", aliases: &["ripple"],
        polymarket_hourly_slug: Some("xrp"), binance_pair: "XRPUSDT", okx_swap: "XRP-USDT-SWAP",
        kalshi_prefix: "KXXRP", kalshi_hourly: "KXXRPD", ticker_names_it: true,
    },
    Coin {
        slug: "doge", symbol: "DOGE", name: "Dogecoin", aliases: &[],
        polymarket_hourly_slug: None, binance_pair: "DOGEUSDT", okx_swap: "DOGE-USDT-SWAP",
        kalshi_prefix: "KXDOGE", kalshi_hourly: "KXDOGED", ticker_names_it: false,
    },
];

impl Coin {
    /// Every lower-case word that names this coin to the operator: slug,
    /// symbol, name, aliases. For reading a market title use [`Coin::title_words`].
    pub fn words(&self) -> impl Iterator<Item = String> + '_ {
        [self.slug, self.symbol, self.name]
            .into_iter()
            .chain(self.aliases.iter().copied())
            .map(|w| w.to_ascii_lowercase())
    }

    /// The lower-case words that name this coin in a market title or slug:
    /// the name and aliases always, the slug and ticker only where the ticker
    /// is not also something else ([`Coin::ticker_names_it`]).
    pub fn title_words(&self) -> impl Iterator<Item = String> + '_ {
        let ticker: [Option<&'static str>; 2] = if self.ticker_names_it { [Some(self.slug), Some(self.symbol)] } else { [None, None] };
        std::iter::once(self.name)
            .chain(self.aliases.iter().copied())
            .chain(ticker.into_iter().flatten())
            .map(|w| w.to_ascii_lowercase())
    }
}

/// The coin spelled `s` in any of its forms (slug, symbol, name or alias),
/// case-insensitive: `"xrp"`, `"XRP"`, `"Ripple"` all find XRP.
pub fn find(s: &str) -> Option<&'static Coin> {
    let want = s.trim().to_ascii_lowercase();
    COINS.iter().find(|c| c.words().any(|w| w == want))
}

/// The coin with this slug, case-insensitive (`"btc"`, `"BTC"`).
pub fn by_slug(slug: &str) -> Option<&'static Coin> {
    COINS.iter().find(|c| c.slug.eq_ignore_ascii_case(slug.trim()))
}

/// Is `s` a supported underlying's slug?
pub fn is_slug(s: &str) -> bool {
    by_slug(s).is_some()
}

/// The slugs, in table order.
pub fn slugs() -> Vec<&'static str> {
    COINS.iter().map(|c| c.slug).collect()
}

/// Does `text` contain any coin's title word as a substring? This keeps the
/// pre-E110 semantics of the name-based crypto checks, which matched "eth"
/// inside "whether" (except for a ticker that is also something else: "doge"
/// alone does not count, "dogecoin" does): they classify a market the operator
/// deployed, where a false positive is corrected by the class rules. Use
/// [`detect_in_title`] where a whole-word answer matters.
pub fn mentioned_in(text: &str) -> bool {
    let n = text.to_ascii_lowercase();
    COINS.iter().any(|c| c.title_words().any(|w| n.contains(&w)))
}

/// The coin whose title words include `word` (one token of a title or slug,
/// any case): "ripple" and "xrp" are XRP, "dogecoin" is Dogecoin, a bare
/// "doge" is nothing.
pub fn by_title_word(word: &str) -> Option<&'static Coin> {
    let want = word.to_ascii_lowercase();
    COINS.iter().find(|c| c.title_words().any(|w| w == want))
}

/// The coin a market title names, by whole word.
///
/// Names and aliases are checked case-insensitively across every coin before
/// any ticker, so "Bitcoin beat Solana?" names two and is `None`, and a title
/// naming one coin by its full name resolves to it even if another coin's
/// ticker appears as a stray word. Tickers count only in upper case ("SOL" is
/// a ticker, "sol" in prose usually is not) and only for coins whose ticker
/// names them ([`Coin::ticker_names_it`]). `None` is a first-class answer:
/// "Will the Chiefs win" has no underlying.
pub fn detect_in_title(title: &str) -> Option<&'static str> {
    let words: Vec<&str> = title
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .collect();
    let has_word = |want: &str| words.iter().any(|w| w.eq_ignore_ascii_case(want));
    let has_ticker = |want: &str| words.iter().any(|w| *w == want);
    let named: Vec<&'static str> = COINS
        .iter()
        .filter(|c| has_word(c.name) || c.aliases.iter().any(|a| has_word(a)))
        .map(|c| c.slug)
        .collect();
    match named.len() {
        1 => return Some(named[0]),
        n if n > 1 => return None,
        _ => {}
    }
    let tickers: Vec<&'static str> = COINS
        .iter()
        .filter(|c| c.ticker_names_it && has_ticker(c.symbol))
        .map(|c| c.slug)
        .collect();
    (tickers.len() == 1).then(|| tickers[0])
}

/// The coin whose Kalshi ticker prefix starts `ticker` (`KXXRP15M-...` → xrp).
pub fn by_kalshi_ticker(ticker: &str) -> Option<&'static Coin> {
    let t = ticker.to_ascii_uppercase();
    COINS.iter().find(|c| t.starts_with(c.kalshi_prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_spelling_finds_the_coin() {
        assert_eq!(find("xrp").map(|c| c.slug), Some("xrp"));
        assert_eq!(find("XRP").map(|c| c.slug), Some("xrp"));
        assert_eq!(find("Ripple").map(|c| c.slug), Some("xrp"));
        assert_eq!(find("dogecoin").map(|c| c.slug), Some("doge"));
        assert_eq!(find(" BTC ").map(|c| c.slug), Some("btc"));
        assert!(find("ada").is_none());
        assert!(find("doge").is_some(), "the operator's spelling always finds the coin");
        assert!(by_title_word("doge").is_none(), "a bare DOGE in a title is not the coin");
        assert_eq!(by_title_word("Dogecoin").map(|c| c.slug), Some("doge"));
        assert_eq!(by_title_word("ripple").map(|c| c.slug), Some("xrp"));
        assert_eq!(by_title_word("BTC").map(|c| c.slug), Some("btc"));
        assert!(by_slug("DOGE").is_some());
        assert!(by_slug("dogecoin").is_none(), "by_slug takes the slug only");
        assert_eq!(slugs(), vec!["btc", "eth", "sol", "xrp", "doge"]);
    }

    #[test]
    fn rows_are_consistent() {
        for c in COINS {
            assert_eq!(c.slug, c.slug.to_ascii_lowercase());
            assert_eq!(c.symbol, c.symbol.to_ascii_uppercase());
            assert!(c.binance_pair.ends_with("USDT"), "{}", c.slug);
            assert!(c.okx_swap.ends_with("-USDT-SWAP"), "{}", c.slug);
            assert!(c.kalshi_prefix.starts_with("KX"), "{}", c.slug);
            assert_eq!(c.kalshi_hourly, format!("{}D", c.kalshi_prefix), "{}", c.slug);
            assert!(c.aliases.iter().all(|a| *a == a.to_ascii_lowercase()), "{}", c.slug);
        }
        // The strings the feeds send, frozen.
        let pairs: Vec<(&str, &str, &str)> = COINS.iter().map(|c| (c.slug, c.binance_pair, c.okx_swap)).collect();
        assert_eq!(pairs, vec![
            ("btc", "BTCUSDT", "BTC-USDT-SWAP"),
            ("eth", "ETHUSDT", "ETH-USDT-SWAP"),
            ("sol", "SOLUSDT", "SOL-USDT-SWAP"),
            ("xrp", "XRPUSDT", "XRP-USDT-SWAP"),
            ("doge", "DOGEUSDT", "DOGE-USDT-SWAP"),
        ]);
        assert_eq!(by_slug("doge").unwrap().polymarket_hourly_slug, None, "Polymarket lists no hourly Dogecoin market");
        let n = COINS.len();
        let distinct: std::collections::HashSet<_> = COINS.iter().map(|c| c.slug).collect();
        assert_eq!(distinct.len(), n, "slugs are unique");
    }

    #[test]
    fn titles_resolve_by_whole_word() {
        assert_eq!(detect_in_title("Ripple price at Nov 26, 2026 at 7pm EST?"), Some("xrp"));
        assert_eq!(detect_in_title("XRP Up or Down - November 26, 6:00PM-6:15PM ET"), Some("xrp"));
        assert_eq!(detect_in_title("Dogecoin price on Nov 26, 2026?"), Some("doge"));
        assert_eq!(detect_in_title("Bitcoin price on Nov 26, 2026?"), Some("btc"));
        assert_eq!(detect_in_title("Will Bitcoin beat Solana this year?"), None);
        assert_eq!(detect_in_title("Will the Netherlands win? Resolved by console"), None);
        assert_eq!(detect_in_title("Will the Chiefs win"), None);
        assert_eq!(detect_in_title("BTC/USDT above 60k"), Some("btc"));
        assert_eq!(detect_in_title("Will ETH exceed $5,000?"), Some("eth"));
        assert_eq!(detect_in_title("the sol farm resolved"), None, "a lower-case ticker in prose is not a coin");
        assert_eq!(detect_in_title("Will DOGE cut $1T in 2026?"), None, "DOGE alone is the department, not the coin");
        assert_eq!(detect_in_title("Dogecoin Up or Down - November 26, 6:00PM-6:15PM ET"), Some("doge"));
        assert_eq!(detect_in_title("Ethereum or Bitcoin first to $100k?"), None);
    }

    #[test]
    fn kalshi_prefixes_and_mentions() {
        assert_eq!(by_kalshi_ticker("KXDOGE15M-26NOV261745-45").map(|c| c.slug), Some("doge"));
        assert_eq!(by_kalshi_ticker("kxxrpd-26NOV2619-T1.6399").map(|c| c.slug), Some("xrp"));
        assert!(by_kalshi_ticker("KXNFLGAME-X").is_none());
        assert!(mentioned_in("Ripple price at Nov 26"));
        assert!(mentioned_in("Dogecoin 15 min"));
        assert!(!mentioned_in("Will DOGE cut spending?"));
        assert!(!mentioned_in("Denver wins"));
    }
}
