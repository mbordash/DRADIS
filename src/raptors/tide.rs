/// Tide Raptor — "Institutional Pulse" spot-BTC-ETF premium/discount signal.
///
/// A *macro* Raptor that flies above even the Derivatives Raptor. Where the
/// Funding/Derivatives Raptors read perp-market positioning, the Tide Raptor
/// reads the **US cash-market institutional tide** — the premium or discount at
/// which the Big Three spot Bitcoin ETFs (IBIT / FBTC / ARKB) trade versus their
/// fair value. Sustained premium ⇒ Authorized Participants face net subscription
/// demand and buy spot BTC to create shares (creation pressure leads spot flow);
/// sustained discount ⇒ redemption pressure.
///
/// ── How the signal is built ─────────────────────────────────────────────────
/// For each ETF i, fair value is its **synthetic iNAV**, computed millisecond-
/// fresh from the live Binance oracle the Price Raptor already streams:
///
/// ```text
/// synthetic_inav_i = btc_per_share_i × binance_oracle
/// premium_i_bps     = (equity_last_i / synthetic_inav_i − 1) × 10_000
/// ```
///
/// Each premium is z-scored against its own rolling mean/σ (so a structurally
/// wider quote doesn't dominate), then volume-weighted by traded dollar-volume
/// into two emitted fields:
///
/// ```text
/// institutional_pulse = Σ(z_i · $vol_i) / Σ($vol_i)          // signed magnitude
/// coherence           = |Σ(sign(z_i) · $vol_i)| / Σ($vol_i)  // 0..1 agreement
/// ```
///
/// Because every synthetic iNAV multiplies the **same** oracle, oracle error
/// (latency, the USDT≠USD basis) is *common-mode* across all three ETFs and
/// cancels in `coherence` and cross-ETF dispersion — only a small constant bias
/// survives in the absolute `institutional_pulse` level.
///
/// ── Consumption status (2026-07-21) ─────────────────────────────────────────
/// Published into `MarketSnapshot` (institutional_pulse / tide_coherence) and
/// consumed by Convergence (core signal), GBoost (model features [22][23]) and
/// the optional Basis tide gate. The real-time equity leg
/// (Alpaca free-tier IEX WS) is implemented — set `ALPACA_API_KEY_ID` /
/// `ALPACA_API_SECRET_KEY` to stream live IBIT/FBTC/ARKB prints. Without keys the
/// Raptor still gates on US market hours and publishes synthetic iNAV, but emits
/// a zero pulse with `tide_connected = false`. `btc_per_share` multipliers use
/// sane hardcoded fallbacks pending the daily fund-disclosure scrape (deferred).
///
/// Like the other macro Raptors it degrades silently to its `Default`
/// (all-zero, `market_open = false`) when data is unavailable; consumers treat a
/// zero pulse as neutral.
use std::collections::HashMap;
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{Datelike, Timelike};
use chrono_tz::US::Eastern;
use futures::{SinkExt, StreamExt as _};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal_macros::dec;
use tokio::sync::{watch, Mutex};
use tokio::time::{timeout as tokio_timeout, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, warn};

use crate::config;
use crate::api::server::AssetRaptorHealth;

/// Normalised institutional-tide snapshot broadcast to every consuming Squadron.
///
/// `Copy` so the `watch` channel hands out cheap value clones, and `Default`
/// (all-zero, `market_open = false`) so the channel can be seeded before the
/// first compute tick and so off-hours reads are unambiguous.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TideSnapshot {
    /// Volume-weighted, vol-normalized aggregate premium z-score across the
    /// Big Three. `> 0` net premium (creation/demand pressure), `< 0` net
    /// discount (redemption pressure), `0` neutral / no data.
    pub institutional_pulse: Decimal,
    /// Agreement of the three ETFs' premium signs, dollar-volume weighted.
    /// `1.0` = all active ETFs lean the same way (high conviction); `~0` = split
    /// (treat the pulse as microstructure noise). `0` when no ETF is active.
    pub coherence: Decimal,
    /// Per-ETF premium in basis points (equity price vs synthetic iNAV).
    pub ibit_bps: Decimal,
    pub fbtc_bps: Decimal,
    pub arkb_bps: Decimal,
    /// True only during the US cash session (09:30–16:00 ET, Mon–Fri). When
    /// false the premium fields are stale/last-close and the pulse is held at 0.
    pub market_open: bool,
}

/// One Big-Three ETF and its fallback BTC-per-share multiplier.
///
/// `btc_per_share = total_BTC_in_custody / shares_outstanding`, published daily
/// on each issuer's fund-disclosure page. These compile-time values are sane
/// fallbacks only — they drift down slowly as sponsors sell BTC to accrue the
/// expense ratio, so the (deferred) daily scrape will refresh them each morning.
struct EtfSpec {
    ticker: &'static str,
    fallback_btc_per_share: Decimal,
}

const BIG_THREE: [EtfSpec; 3] = [
    EtfSpec { ticker: "IBIT", fallback_btc_per_share: dec!(0.00057) },
    EtfSpec { ticker: "FBTC", fallback_btc_per_share: dec!(0.00090) },
    EtfSpec { ticker: "ARKB", fallback_btc_per_share: dec!(0.00143) },
];

/// Horizon Raptor symbols — TradFi velocity and VIX proxy.
/// Subscribed alongside the Big Three on the same Alpaca connection.
const HORIZON_SYMBOLS: [&str; 3] = ["SPY", "QQQ", "UVXY"];

/// All symbols subscribed on the shared Alpaca IEX connection.
fn all_tickers() -> Vec<&'static str> {
    BIG_THREE.iter().map(|s| s.ticker)
        .chain(HORIZON_SYMBOLS.iter().copied())
        .collect()
}

/// Latest real-time equity print for one symbol, written by the equity-feed task
/// and read by the compute loops. Shared behind a `Mutex` so the deferred Alpaca
/// IEX WS task can update it independently of the recompute cadence.
///
/// Public so the Horizon Raptor can consume quotes from the same shared map.
#[derive(Clone, Copy, Debug, Default)]
pub struct EquityQuote {
    /// Last trade price (USD). `0` ⇒ no print received yet.
    pub last_price: Decimal,
    /// Wall-clock instant of the last print, for the staleness guard.
    pub last_at: Option<Instant>,
    /// Traded dollar-volume summed over the trailing `TIDE_VOLUME_WINDOW_SECS`.
    pub dollar_vol: Decimal,
}

/// Shared quote map for all Alpaca-subscribed symbols (BTC ETFs + Horizon TradFi).
/// Both the Tide and Horizon Raptors read from this map.
pub type SharedQuoteMap = Arc<Mutex<HashMap<&'static str, EquityQuote>>>;

/// Create a new shared quote map. Called once in main.rs before spawning Tide
/// and Horizon raptors.
pub fn new_shared_quote_map() -> SharedQuoteMap {
    Arc::new(Mutex::new(HashMap::new()))
}

// Internal alias for backward compat
type QuoteMap = SharedQuoteMap;

/// Per-ETF rolling premium buffer for z-scoring, owned by the compute loop.
struct PremiumBuffer {
    btc_per_share: Decimal,
    history: VecDeque<Decimal>, // recent premium_bps samples
}

impl PremiumBuffer {
    fn new(btc_per_share: Decimal) -> Self {
        Self { btc_per_share, history: VecDeque::with_capacity(config::TIDE_ZSCORE_WINDOW) }
    }

    /// Push a new premium sample, evicting the oldest beyond the window.
    fn push(&mut self, premium_bps: Decimal) {
        self.history.push_back(premium_bps);
        while self.history.len() > config::TIDE_ZSCORE_WINDOW {
            self.history.pop_front();
        }
    }

    /// Z-score the latest premium against the buffer's mean/σ. Returns `0` until
    /// there is enough history (≥10 samples) and non-zero dispersion.
    fn zscore(&self, premium_bps: Decimal) -> Decimal {
        let n = self.history.len();
        if n < 10 { return dec!(0); }
        let nn = Decimal::from(n as u64);
        let mean: Decimal = self.history.iter().copied().sum::<Decimal>() / nn;
        let var: Decimal = self.history.iter()
            .map(|x| { let d = *x - mean; d * d })
            .sum::<Decimal>() / nn;
        let std = decimal_sqrt(var);
        if std <= dec!(0) { return dec!(0); }
        (premium_bps - mean) / std
    }
}

/// Newton's-method square root for `Decimal` (no `sqrt` in rust_decimal core).
/// Falls back through `f64` for the seed; precision is ample for a z-score σ.
fn decimal_sqrt(v: Decimal) -> Decimal {
    if v <= dec!(0) { return dec!(0); }
    let seed = v.to_f64().map(|f| f.sqrt()).unwrap_or(0.0);
    let mut x = Decimal::try_from(seed).unwrap_or(dec!(1));
    if x <= dec!(0) { x = dec!(1); }
    // A couple of Newton iterations refine the f64 seed at Decimal precision.
    for _ in 0..3 {
        if x <= dec!(0) { break; }
        x = (x + v / x) / dec!(2);
    }
    x
}

/// True during the US cash session: 09:30–16:00 America/New_York, Mon–Fri.
/// (Market holidays are not yet handled — on a holiday this returns true but the
/// equity feed simply yields no prints, so the pulse stays at 0 regardless.)
pub fn is_us_market_open(now: chrono::DateTime<chrono::Utc>) -> bool {
    let et = now.with_timezone(&Eastern);
    let wd = et.weekday().number_from_monday(); // 1=Mon .. 7=Sun
    if wd >= 6 { return false; }
    let mins = et.hour() * 60 + et.minute();
    (9 * 60 + 30..=16 * 60).contains(&mins)
}

/// Spawn the Tide Raptor. Returns the shared quote map that the Horizon Raptor
/// should also consume (both read from the same Alpaca IEX feed).
pub async fn run_tide_raptor(
    oracle_rx: watch::Receiver<Decimal>,
    tide_tx: watch::Sender<TideSnapshot>,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
    shared_quotes: SharedQuoteMap,
) {
    // Spawn the real-time equity leg. In observe-only mode (no Alpaca creds) it
    // logs once and idles, leaving the quote map empty so the pulse stays 0.
    // Subscribes to ALL symbols (BTC ETFs + Horizon TradFi) on one connection.
    tokio::spawn(run_equity_feed(Arc::clone(&shared_quotes)));

    // Per-ETF rolling premium buffers, seeded with fallback multipliers.
    let mut buffers: HashMap<&'static str, PremiumBuffer> = BIG_THREE
        .iter()
        .map(|s| (s.ticker, PremiumBuffer::new(s.fallback_btc_per_share)))
        .collect();

    info!("🌊 Tide Raptor online — tracking IBIT/FBTC/ARKB premium vs synthetic iNAV (consumed by Convergence/GBoost/Basis)");

    loop {
        tokio::time::sleep(Duration::from_secs(config::TIDE_RECOMPUTE_SECS)).await;

        let market_open = is_us_market_open(chrono::Utc::now());
        let oracle = *oracle_rx.borrow();

        // Per-ETF premium bps for the snapshot, plus weighted aggregation accums.
        let mut per_etf_bps: HashMap<&'static str, Decimal> = HashMap::new();
        let mut weighted_z_sum = dec!(0);
        let mut weighted_sign_sum = dec!(0);
        let mut weight_total = dec!(0);
        let mut any_connected = false;

        // Snapshot the quote map for this compute tick.
        let snap = { shared_quotes.lock().await.clone() };

        for spec in BIG_THREE.iter() {
            let buf = buffers.get_mut(spec.ticker).expect("buffer seeded for every ETF");
            let synthetic_inav = buf.btc_per_share * oracle;

            let q = snap.get(spec.ticker).copied().unwrap_or_default();
            let fresh = q.last_at
                .map(|t| t.elapsed() < Duration::from_secs(config::TIDE_QUOTE_STALENESS_SECS))
                .unwrap_or(false);

            // Compute premium only with a live oracle, a real print, and freshness.
            if !market_open || !fresh || synthetic_inav <= dec!(0) || q.last_price <= dec!(0) {
                continue;
            }
            any_connected = true;

            let premium_bps = (q.last_price / synthetic_inav - dec!(1)) * dec!(10000);
            buf.push(premium_bps);
            per_etf_bps.insert(spec.ticker, premium_bps);

            let z = buf.zscore(premium_bps);
            let w = q.dollar_vol.max(dec!(0));
            if w > dec!(0) {
                weighted_z_sum += z * w;
                weighted_sign_sum += sign(z) * w;
                weight_total += w;
            }
        }

        let (pulse, coherence) = if market_open && weight_total > dec!(0) {
            (weighted_z_sum / weight_total, (weighted_sign_sum / weight_total).abs())
        } else {
            (dec!(0), dec!(0))
        };

        let snapshot = TideSnapshot {
            institutional_pulse: pulse,
            coherence,
            ibit_bps: per_etf_bps.get("IBIT").copied().unwrap_or(dec!(0)),
            fbtc_bps: per_etf_bps.get("FBTC").copied().unwrap_or(dec!(0)),
            arkb_bps: per_etf_bps.get("ARKB").copied().unwrap_or(dec!(0)),
            market_open,
        };
        let _ = tide_tx.send(snapshot);

        // Mirror into the BTC raptor-health entry for GET /api/telemetry.
        raptor_health_tx.send_modify(|map| {
            let h = map.entry("btc".to_string()).or_default();
            h.tide_connected       = any_connected;
            h.tide_market_open      = market_open;
            h.institutional_pulse   = pulse;
            h.tide_coherence        = coherence;
            h.ibit_premium_bps      = snapshot.ibit_bps;
            h.fbtc_premium_bps      = snapshot.fbtc_bps;
            h.arkb_premium_bps      = snapshot.arkb_bps;
        });
    }
}

/// Sign of a Decimal as `-1 / 0 / +1`.
fn sign(v: Decimal) -> Decimal {
    if v > dec!(0) { dec!(1) } else if v < dec!(0) { dec!(-1) } else { dec!(0) }
}

/// Real-time equity leg — Alpaca free-tier IEX WebSocket.
///
/// Connects to Alpaca's IEX market-data stream, authenticates with the operator's
/// API keys, subscribes to IBIT/FBTC/ARKB trades, and writes each `EtfQuote`
/// (last price + rolling `TIDE_VOLUME_WINDOW_SECS` dollar-volume + print instant)
/// into the shared map the compute loop reads. Single-venue (IEX) and real-time
/// (NOT 15-min delayed) — adequate for directional premium bps on liquid names.
///
/// When `ALPACA_API_KEY_ID` / `ALPACA_API_SECRET_KEY` are absent it logs once and
/// idles, keeping the Raptor in valid observe-only mode (empty quotes ⇒ zero
/// pulse, `tide_connected = false`). Reconnects with a 5s backoff on any error.
///
/// Off-hours there are simply no trades, so the quote map goes stale and the
/// compute loop's staleness guard drops every ETF — the connection stays open
/// idle until the next session resumes prints.
async fn run_equity_feed(quotes: QuoteMap) {
    let key = match std::env::var(config::TIDE_ALPACA_KEY_ENV) {
        Ok(k) if !k.is_empty() => k,
        _ => return idle_observe_only(),
    };
    let secret = match std::env::var(config::TIDE_ALPACA_SECRET_ENV) {
        Ok(s) if !s.is_empty() => s,
        _ => return idle_observe_only(),
    };

    // Per-symbol rolling dollar-volume windows, owned by this task.
    // Covers both BTC ETFs (Tide) and TradFi symbols (Horizon).
    let mut vol_windows: HashMap<&'static str, VecDeque<(Instant, Decimal)>> =
        all_tickers().into_iter().map(|t| (t, VecDeque::new())).collect();

    // Consecutive 406 ("connection limit") counter. A single 406 is almost always a
    // self-reconnect race (Alpaca hasn't reaped our prior slot yet) and clears on a
    // short backoff; repeated 406s mean a genuine second instance shares the key, so
    // the backoff escalates from MIN toward MAX.
    let mut conn_limit_strikes: u32 = 0;

    loop {
        match stream_alpaca_iex(&key, &secret, &quotes, &mut vol_windows).await {
            Ok(())  => {
                conn_limit_strikes = 0;
                warn!(
                    "🌊 Alpaca equity feed: stream ended cleanly — reconnecting in {}s",
                    config::TIDE_RECONNECT_DELAY_SECS,
                );
                tokio::time::sleep(Duration::from_secs(config::TIDE_RECONNECT_DELAY_SECS)).await;
            }
            // Alpaca free tier allows only ONE concurrent market-data connection per
            // account (error 406). With a correctly-separated key this is almost always
            // a self-reconnect race against our own just-dropped slot, which clears on a
            // short backoff. Only persistent, repeated 406s indicate a real second
            // instance sharing the key — so escalate the backoff toward the max and
            // surface the operator action only once it's clearly not a transient race.
            Err(e) if e.contains("406") || e.to_lowercase().contains("connection limit") => {
                conn_limit_strikes = conn_limit_strikes.saturating_add(1);
                let backoff = (config::TIDE_CONN_LIMIT_BACKOFF_MIN_SECS * conn_limit_strikes as u64)
                    .min(config::TIDE_CONN_LIMIT_BACKOFF_MAX_SECS);
                if conn_limit_strikes <= 1 {
                    warn!(
                        "🌊 Alpaca equity feed: {e}. Likely a self-reconnect race against our own \
                         just-dropped Alpaca slot — backing off {backoff}s and retrying.",
                    );
                } else {
                    warn!(
                        "🌊 Alpaca equity feed: {e}. Persistent connection-limit (strike #{conn_limit_strikes}) — \
                         another instance is likely using this key. Alpaca's free tier permits ONE \
                         concurrent market-data connection per account. Backing off {backoff}s.",
                    );
                }
                tokio::time::sleep(Duration::from_secs(backoff)).await;
            }
            Err(e)  => {
                conn_limit_strikes = 0;
                warn!(
                    "🌊 Alpaca equity feed: {e} — reconnecting in {}s",
                    config::TIDE_RECONNECT_DELAY_SECS,
                );
                tokio::time::sleep(Duration::from_secs(config::TIDE_RECONNECT_DELAY_SECS)).await;
            }
        }
    }
}

fn idle_observe_only() {
    warn!(
        "🌊 Alpaca equity feed idle — set {} / {} to enable live \
         Tide + Horizon Raptors (observe-only until then)",
        config::TIDE_ALPACA_KEY_ENV, config::TIDE_ALPACA_SECRET_ENV,
    );
}

const ALPACA_IEX_WS: &str = "wss://stream.data.alpaca.markets/v2/iex";

/// One connect→auth→subscribe→consume cycle. Returns `Ok` on clean stream close,
/// `Err(msg)` on any protocol/transport failure (caller reconnects).
async fn stream_alpaca_iex(
    key: &str,
    secret: &str,
    quotes: &QuoteMap,
    vol_windows: &mut HashMap<&'static str, VecDeque<(Instant, Decimal)>>,
) -> Result<(), String> {
    let (mut ws, _) = connect_async(ALPACA_IEX_WS).await.map_err(|e| format!("connect failed: {e}"))?;

    // Authenticate, then subscribe once the server confirms auth. Alpaca replies
    // to each control message with a JSON array of status objects.
    let auth = format!(r#"{{"action":"auth","key":"{key}","secret":"{secret}"}}"#);
    ws.send(Message::Text(auth.into())).await.map_err(|e| format!("auth send failed: {e}"))?;

    let mut subscribed = false;
    // Subscribe to all symbols: BTC ETFs (Tide) + TradFi (Horizon)
    let tickers: Vec<&str> = all_tickers();

    loop {
        // Generous read timeout: off-hours the feed is legitimately silent, so a
        // timeout just re-polls (keeping pings flowing) rather than reconnecting.
        match tokio_timeout(Duration::from_secs(120), ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                handle_alpaca_payload(
                    &text, quotes, vol_windows, &mut ws, &mut subscribed, &tickers,
                ).await?;
            }
            // Alpaca's default is JSON text; tolerate binary by attempting UTF-8.
            Ok(Some(Ok(Message::Binary(bin)))) => {
                if let Ok(text) = String::from_utf8(bin.to_vec()) {
                    handle_alpaca_payload(
                        &text, quotes, vol_windows, &mut ws, &mut subscribed, &tickers,
                    ).await?;
                }
            }
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {} // keepalive
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => return Ok(()),
            Ok(Some(Ok(_))) => {} // frame types we don't care about
            Ok(Some(Err(e))) => return Err(format!("ws error: {e}")),
            Err(_) => {} // 120s idle — normal off-hours; keep the socket open
        }
    }
}

/// Parse one Alpaca JSON payload (an array of message objects) and act on it:
/// send the trade subscription once authenticated, ingest trade prints, and
/// surface auth/connection errors as a reconnect trigger.
async fn handle_alpaca_payload(
    text: &str,
    quotes: &QuoteMap,
    vol_windows: &mut HashMap<&'static str, VecDeque<(Instant, Decimal)>>,
    ws: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    subscribed: &mut bool,
    tickers: &[&str],
) -> Result<(), String> {
    let msgs: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    let Some(arr) = msgs.as_array() else { return Ok(()) };

    for m in arr {
        match m.get("T").and_then(|t| t.as_str()) {
            // Connection / auth / subscription status frames.
            Some("success") => {
                if m.get("msg").and_then(|x| x.as_str()) == Some("authenticated") && !*subscribed {
                    let list = tickers.iter()
                        .map(|t| format!("\"{t}\""))
                        .collect::<Vec<_>>()
                        .join(",");
                    let sub = format!(r#"{{"action":"subscribe","trades":[{list}]}}"#);
                    ws.send(Message::Text(sub.into())).await
                        .map_err(|e| format!("subscribe send failed: {e}"))?;
                    *subscribed = true;
                    info!("🌊 Alpaca equity feed: authenticated, subscribed {:?} (IEX)", tickers);
                }
            }
            Some("subscription") => {} // subscription confirmation — no action
            Some("error") => {
                let code = m.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
                let msg = m.get("msg").and_then(|x| x.as_str()).unwrap_or("unknown");
                return Err(format!("Alpaca error {code}: {msg}"));
            }
            // Trade print: {"T":"t","S":"IBIT","p":<price>,"s":<size>,...}
            Some("t") => ingest_trade(m, quotes, vol_windows).await,
            _ => {}
        }
    }
    Ok(())
}

/// Ingest a single trade print into the rolling dollar-volume window and the
/// shared quote map.
async fn ingest_trade(
    m: &serde_json::Value,
    quotes: &QuoteMap,
    vol_windows: &mut HashMap<&'static str, VecDeque<(Instant, Decimal)>>,
) {
    let Some(sym) = m.get("S").and_then(|s| s.as_str()).and_then(resolve_ticker) else { return };
    let Some(price) = m.get("p").and_then(json_decimal) else { return };
    let size = m.get("s").and_then(json_decimal).unwrap_or(dec!(0));
    if price <= dec!(0) { return; }

    let now = Instant::now();
    let notional = price * size;

    let dq = vol_windows.entry(sym).or_default();
    dq.push_back((now, notional));
    while let Some((t, _)) = dq.front() {
        if now.duration_since(*t) > Duration::from_secs(config::TIDE_VOLUME_WINDOW_SECS) {
            dq.pop_front();
        } else {
            break;
        }
    }
    let dollar_vol: Decimal = dq.iter().map(|(_, n)| *n).sum();

    let mut map = quotes.lock().await;
    map.insert(sym, EquityQuote { last_price: price, last_at: Some(now), dollar_vol });
}

/// Map an Alpaca symbol string to one of our known tickers (BTC ETFs + TradFi).
fn resolve_ticker(s: &str) -> Option<&'static str> {
    // Check BTC ETFs first
    if let Some(spec) = BIG_THREE.iter().find(|spec| spec.ticker == s) {
        return Some(spec.ticker);
    }
    // Check Horizon TradFi symbols
    HORIZON_SYMBOLS.iter().copied().find(|&t| t == s)
}

/// Coerce a JSON number (or numeric string) to `Decimal`.
fn json_decimal(v: &serde_json::Value) -> Option<Decimal> {
    if let Some(f) = v.as_f64() {
        return Decimal::from_f64(f);
    }
    v.as_str().and_then(|s| Decimal::from_str(s).ok())
}
