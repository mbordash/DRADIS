/// Horizon Raptor — TradFi equity velocity and VIX proxy signal.
///
/// A *macro* Raptor that bridges the latency gap between the fast, local crypto
/// feeds (Price Raptor's 5s velocity) and the lagging institutional ETF flow
/// indicators (Tide Raptor). The Horizon Raptor tracks:
///
/// │ Symbol │ Purpose                                                      │
/// │────────│──────────────────────────────────────────────────────────────│
/// │ SPY    │ S&P 500 proxy — broad risk-on/risk-off gauge                 │
/// │ QQQ    │ Nasdaq-100 proxy — tech/growth sentiment, BTC correlation    │
/// │ UVXY   │ VIX futures ETF — volatility/panic proxy                     │
///
/// ── Emitted Signals ──────────────────────────────────────────────────────────
///
/// • **TradFi Velocity**: 5-second momentum of SPY+QQQ, volume-weighted.
///   Positive = risk-on front-running, negative = risk-off front-running.
///   Vipers use this to detect fakeout breakouts in crypto when TradFi diverges.
///
/// • **Macro Coherence** ($C_m$): 10-minute rolling Pearson correlation between
///   QQQ velocity and BTC velocity. High ⇒ BTC trading as high-beta tech asset;
///   low ⇒ BTC decoupled (idiosyncratic or flight-to-quality regime).
///
/// • **VIX Proxy**: UVXY price level and rate-of-change. Spikes signal global
///   panic; market-making Vipers should widen spreads accordingly.
///
/// ── Data Source ──────────────────────────────────────────────────────────────
///
/// **Shares the Alpaca IEX WebSocket connection with the Tide Raptor.** The Tide
/// Raptor subscribes to all symbols (BTC ETFs + SPY/QQQ/UVXY) and publishes
/// quotes to a shared `SharedQuoteMap`. The Horizon Raptor reads from that map
/// to compute its signals. This avoids needing two Alpaca accounts (free tier
/// allows only one concurrent connection per account).
///
/// ── Consumption status (2026-07-21) ─────────────────────────────────────────
///
/// Published into `MarketSnapshot` (tradfi_velocity / macro_coherence /
/// vix_proxy / vix_velocity) and consumed by the Maker Horizon gate and the
/// TrendReversal fade veto (both observe-first: *_ENFORCE consts arm them).
/// Zero signals degrade gracefully (neutral).
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tokio::time::{Duration, Instant};
use tracing::info;

use crate::api::server::AssetRaptorHealth;
use crate::config;
use crate::raptors::tide::{SharedQuoteMap, is_us_market_open};

// ─── Public Snapshot ──────────────────────────────────────────────────────────

/// Normalised TradFi/VIX snapshot broadcast to every consuming Squadron.
///
/// `Copy` so the `watch` channel hands out cheap value clones, and `Default`
/// (all-zero) so the channel can be seeded before the first compute tick.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct HorizonSnapshot {
    /// Volume-weighted 5-second velocity of SPY+QQQ (USD Δprice).
    /// Positive = TradFi rallying (risk-on), negative = selling (risk-off).
    pub tradfi_velocity: Decimal,

    /// 10-minute rolling Pearson correlation of QQQ velocity vs BTC velocity.
    /// `1.0` = perfect positive correlation (BTC = high-beta tech),
    /// `0.0` = no correlation (decoupled regime),
    /// `-1.0` = inverse (rare, flight-to-safety?).
    /// Zero when insufficient history.
    pub macro_coherence: Decimal,

    /// UVXY last trade price (VIX futures ETF proxy).
    /// Higher = more fear/volatility in the market.
    pub vix_proxy: Decimal,

    /// 5-second rate of change of VIX proxy (UVXY velocity).
    /// Sharp positive spike = panic onset.
    pub vix_velocity: Decimal,

    /// True during the US cash session (09:30–16:00 ET, Mon–Fri).
    /// Pre/post-market IEX volume is thin — signals are lower-confidence.
    pub market_open: bool,

    /// True when at least one fresh trade print arrived this tick.
    pub connected: bool,
}

// ─── Velocity Buffer ──────────────────────────────────────────────────────────

/// Rolling price buffer for velocity calculation (5s window).
struct VelocityBuffer {
    prices: VecDeque<(Instant, Decimal)>,
}

impl VelocityBuffer {
    fn new() -> Self {
        Self { prices: VecDeque::with_capacity(128) }
    }

    fn push(&mut self, price: Decimal) {
        let now = Instant::now();
        self.prices.push_back((now, price));
        // Trim to 10s max (keep some headroom beyond 5s window)
        while let Some((t, _)) = self.prices.front() {
            if now.duration_since(*t) > Duration::from_secs(10) {
                self.prices.pop_front();
            } else {
                break;
            }
        }
    }

    /// Compute velocity over the 5s window (latest price − oldest in window).
    fn velocity_5s(&self) -> Decimal {
        if self.prices.len() < 2 { return dec!(0); }
        let now = Instant::now();
        let cutoff = now.checked_sub(Duration::from_secs(5)).unwrap_or(now);

        // Find oldest price within 5s
        let oldest = self.prices.iter()
            .find(|(t, _)| *t >= cutoff)
            .map(|(_, p)| *p);
        let latest = self.prices.back().map(|(_, p)| *p);

        match (oldest, latest) {
            (Some(o), Some(l)) => l - o,
            _ => dec!(0),
        }
    }
}

// ─── Correlation Buffer ───────────────────────────────────────────────────────

/// Rolling buffer for 10-minute Pearson correlation between two velocity series.
struct CorrelationBuffer {
    /// (timestamp, btc_velocity, qqq_velocity) samples
    samples: VecDeque<(Instant, Decimal, Decimal)>,
    window_secs: u64,
}

impl CorrelationBuffer {
    fn new(window_secs: u64) -> Self {
        Self {
            samples: VecDeque::with_capacity(256),
            window_secs,
        }
    }

    fn push(&mut self, btc_vel: Decimal, qqq_vel: Decimal) {
        let now = Instant::now();
        self.samples.push_back((now, btc_vel, qqq_vel));
        // Trim to window
        let cutoff = now.checked_sub(Duration::from_secs(self.window_secs)).unwrap_or(now);
        while let Some((t, _, _)) = self.samples.front() {
            if *t < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Pearson correlation coefficient. Returns 0 if insufficient data (< 10 samples).
    fn correlation(&self) -> Decimal {
        let n = self.samples.len();
        if n < 10 { return dec!(0); }

        let nd = Decimal::from(n as u64);
        let sum_x: Decimal = self.samples.iter().map(|(_, x, _)| *x).sum();
        let sum_y: Decimal = self.samples.iter().map(|(_, _, y)| *y).sum();
        let sum_xy: Decimal = self.samples.iter().map(|(_, x, y)| *x * *y).sum();
        let sum_x2: Decimal = self.samples.iter().map(|(_, x, _)| *x * *x).sum();
        let sum_y2: Decimal = self.samples.iter().map(|(_, _, y)| *y * *y).sum();

        let numerator = nd * sum_xy - sum_x * sum_y;
        let denom_x = nd * sum_x2 - sum_x * sum_x;
        let denom_y = nd * sum_y2 - sum_y * sum_y;

        if denom_x <= dec!(0) || denom_y <= dec!(0) { return dec!(0); }

        let denom = decimal_sqrt(denom_x) * decimal_sqrt(denom_y);
        if denom <= dec!(0) { return dec!(0); }

        (numerator / denom).min(dec!(1)).max(dec!(-1))
    }
}

/// Newton's-method square root for `Decimal`.
fn decimal_sqrt(v: Decimal) -> Decimal {
    if v <= dec!(0) { return dec!(0); }
    let seed = v.to_f64().map(|f| f.sqrt()).unwrap_or(0.0);
    let mut x = Decimal::try_from(seed).unwrap_or(dec!(1));
    if x <= dec!(0) { x = dec!(1); }
    for _ in 0..3 {
        if x <= dec!(0) { break; }
        x = (x + v / x) / dec!(2);
    }
    x
}

// ─── Main Raptor Entry Point ──────────────────────────────────────────────────

/// Spawn the Horizon Raptor. Consumes the BTC velocity feed (from Price Raptor)
/// for macro coherence calculation.
///
/// **Note:** Does NOT spawn its own Alpaca connection. Instead, it reads from
/// the shared `SharedQuoteMap` populated by the Tide Raptor's equity feed.
pub async fn run_horizon_raptor(
    shared_quotes: SharedQuoteMap,
    btc_velocity_rx: watch::Receiver<(Decimal, Decimal, Decimal)>,
    horizon_tx: watch::Sender<HorizonSnapshot>,
    raptor_health_tx: Arc<watch::Sender<HashMap<String, AssetRaptorHealth>>>,
) {
    // Per-symbol velocity buffers
    let mut spy_vel = VelocityBuffer::new();
    let mut qqq_vel = VelocityBuffer::new();
    let mut uvxy_vel = VelocityBuffer::new();

    // 10-minute correlation buffer (BTC vel vs QQQ vel)
    let mut corr_buf = CorrelationBuffer::new(config::HORIZON_CORRELATION_WINDOW_SECS);

    let mut tick_interval = tokio::time::interval(Duration::from_millis(config::HORIZON_TICK_MS));
    tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!("🌅 Horizon Raptor online — reading SPY/QQQ/UVXY from shared Alpaca feed (consumed by Maker/TrendReversal Horizon gates)");

    loop {
        tick_interval.tick().await;

        let now_utc = chrono::Utc::now();
        let market_open = is_us_market_open(now_utc);
        let now_inst = Instant::now();
        let staleness_limit = Duration::from_secs(config::HORIZON_QUOTE_STALENESS_SECS);

        // Read current quotes from shared map
        let map = shared_quotes.lock().await;
        let spy_q = map.get("SPY").copied().unwrap_or_default();
        let qqq_q = map.get("QQQ").copied().unwrap_or_default();
        let uvxy_q = map.get("UVXY").copied().unwrap_or_default();
        drop(map);

        // Check freshness
        let spy_fresh = spy_q.last_at.map(|t| now_inst.duration_since(t) < staleness_limit).unwrap_or(false);
        let qqq_fresh = qqq_q.last_at.map(|t| now_inst.duration_since(t) < staleness_limit).unwrap_or(false);
        let uvxy_fresh = uvxy_q.last_at.map(|t| now_inst.duration_since(t) < staleness_limit).unwrap_or(false);
        let connected = spy_fresh || qqq_fresh || uvxy_fresh;

        // Update velocity buffers
        if spy_fresh && spy_q.last_price > dec!(0) {
            spy_vel.push(spy_q.last_price);
        }
        if qqq_fresh && qqq_q.last_price > dec!(0) {
            qqq_vel.push(qqq_q.last_price);
        }
        if uvxy_fresh && uvxy_q.last_price > dec!(0) {
            uvxy_vel.push(uvxy_q.last_price);
        }

        // TradFi velocity: volume-weighted average of SPY and QQQ velocities
        let spy_v = spy_vel.velocity_5s();
        let qqq_v = qqq_vel.velocity_5s();
        let total_vol = spy_q.dollar_vol + qqq_q.dollar_vol;
        let tradfi_velocity = if total_vol > dec!(0) {
            (spy_v * spy_q.dollar_vol + qqq_v * qqq_q.dollar_vol) / total_vol
        } else {
            (spy_v + qqq_v) / dec!(2)
        };

        // VIX proxy
        let vix_proxy = uvxy_q.last_price;
        let vix_velocity = uvxy_vel.velocity_5s();

        // Macro coherence: correlate QQQ velocity with BTC velocity
        let btc_vel = btc_velocity_rx.borrow().0; // 5s velocity
        corr_buf.push(btc_vel, qqq_v);
        let macro_coherence = corr_buf.correlation();

        let snapshot = HorizonSnapshot {
            tradfi_velocity,
            macro_coherence,
            vix_proxy,
            vix_velocity,
            market_open,
            connected,
        };

        let _ = horizon_tx.send(snapshot);

        // Update raptor health telemetry
        raptor_health_tx.send_modify(|map| {
            // Use "btc" key since Horizon is a macro raptor (asset-neutral)
            let h = map.entry("btc".to_string()).or_default();
            h.horizon_connected = connected;
            h.tradfi_velocity = tradfi_velocity;
            h.macro_coherence = macro_coherence;
            h.vix_proxy = vix_proxy;
            h.vix_velocity = vix_velocity;
        });
    }
}
