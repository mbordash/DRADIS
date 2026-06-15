//! US retail MVP trading loop — venue-neutral arbitrage over the [`Execution`]
//! trait.
//!
//! Arbitrage is the one strategy that is fully venue-agnostic: on any binary
//! market the `YES` and `SHORT` legs settle to exactly `$1` combined, so buying
//! both for less than `$1` locks a risk-free edge. This loop:
//!   1. discovers an active binary market (`GET /v1/markets`),
//!   2. streams both legs' order books over the [`ws`] feed,
//!   3. enters via [`Execution::place_atomic`] when the combined ask is cheap
//!      enough, and
//!   4. logs live collateral after each entry.
//!
//! It is intentionally small and self-contained so the venue can be funded and
//! trialled end-to-end, and so other developers can read it as a reference for
//! the `Execution` contract. Richer risk controls / persistence converge with
//! the intl patrol loop in a later step.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::helpers::db;
use crate::state::PriceState;
use crate::venues::core::{Execution, OrderIntent, Side, TimeInForce};

use super::{ws, UsRetailVenue};

/// Number of contracts per leg.
const ENV_TRADE_SIZE: &str = "POLYMARKET_US_TRADE_SIZE";
/// Minimum risk-free edge per pair (dollars), e.g. `0.02` = 2¢.
const ENV_ARB_EDGE: &str = "POLYMARKET_US_ARB_EDGE";
/// Optional substring filter (matched against slug / question) to pick a market.
const ENV_MARKET_FILTER: &str = "POLYMARKET_US_MARKET_FILTER";

const DEFAULT_TRADE_SIZE: u64 = 10;
const DEFAULT_ARB_EDGE: Decimal = dec!(0.02);
const TICK_MS: u64 = 500;
/// Pause after each entry/attempt so the loop doesn't spam a fleeting book.
const ARB_COOLDOWN_SECS: u64 = 30;
/// Retry cadence while waiting for a tradeable market to appear.
const DISCOVERY_RETRY_SECS: u64 = 30;
/// How often to refresh the dashboard (open positions + portfolio snapshot).
const DASHBOARD_SYNC_SECS: u64 = 30;
/// Asset slug the US venue writes its DB rows under (`logs/us-dradis.db`,
/// surfaced in the Control Tower asset selector).
pub const US_ASSET: &str = "us";

/// Run the US retail arbitrage loop until `cancel` fires.
pub async fn run_us_trader(venue: Arc<UsRetailVenue>, cancel: CancellationToken) {
    let trade_size = env_u64(ENV_TRADE_SIZE, DEFAULT_TRADE_SIZE).max(1);
    let edge = env_decimal(ENV_ARB_EDGE, DEFAULT_ARB_EDGE);
    let filter = std::env::var(ENV_MARKET_FILTER).ok().filter(|s| !s.is_empty());
    let size_dec = Decimal::from(trade_size);

    info!(
        "🇺🇸 US trader starting — size={trade_size} contracts, min edge=${edge}, filter={:?}",
        filter
    );

    // ── Select a market (retry until one matches or we're cancelled) ─────────
    let pair = loop {
        if cancel.is_cancelled() {
            return;
        }
        match venue.discover_binary_markets().await {
            Ok(markets) if !markets.is_empty() => {
                let selected = match &filter {
                    Some(f) => {
                        let fl = f.to_lowercase();
                        markets.into_iter().find(|m| {
                            m.slug.to_lowercase().contains(&fl)
                                || m.question.to_lowercase().contains(&fl)
                        })
                    }
                    None => markets.into_iter().next(),
                };
                if let Some(m) = selected {
                    break m;
                }
                warn!("US trader: no active market matched filter {filter:?} — retrying");
            }
            Ok(_) => warn!("US trader: no active binary markets — retrying"),
            Err(e) => warn!("US trader: market discovery failed: {e} — retrying"),
        }
        if wait_or_cancel(&cancel, DISCOVERY_RETRY_SECS).await {
            return;
        }
    };

    info!(
        "🎯 US arb target: \"{}\" [YES={} / NO={}]",
        pair.question, pair.long, pair.short
    );

    // ── Stream both legs' order books ────────────────────────────────────────
    let ws_url = venue.markets_ws_url();
    let default_feed: PriceState = (dec!(0), dec!(0), dec!(1), dec!(0), Utc::now());
    let (long_tx, long_rx) = watch::channel(default_feed);
    let (short_tx, short_rx) = watch::channel(default_feed);
    ws::spawn_market_feed(ws_url.clone(), pair.long.as_str().to_string(), long_tx, cancel.clone());
    ws::spawn_market_feed(ws_url, pair.short.as_str().to_string(), short_tx, cancel.clone());

    // ── Dashboard wiring ─────────────────────────────────────────────────────
    // Snapshot starting collateral so portfolio P&L is session-relative, then push
    // an initial sync so the Control Tower shows the venue immediately.
    let pool = db::pool_for(US_ASSET);
    let starting = venue.collateral().await.unwrap_or(Decimal::ZERO);
    if let Some(p) = &pool {
        sync_dashboard(venue.as_ref(), p, starting).await;
    }

    // ── Tick loop ────────────────────────────────────────────────────────────
    let mut price_tick = tokio::time::interval(Duration::from_millis(TICK_MS));
    let mut dash_tick = tokio::time::interval(Duration::from_secs(DASHBOARD_SYNC_SECS));
    let mut cooldown_until = Instant::now();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => { info!("US trader: cancelled — standing down"); return; }
            _ = dash_tick.tick() => {
                if let Some(p) = &pool { sync_dashboard(venue.as_ref(), p, starting).await; }
                continue;
            }
            _ = price_tick.tick() => {}
        }
        if Instant::now() < cooldown_until {
            continue;
        }

        // Snapshot both legs' best asks + depths without holding the borrow.
        let (long_ask, long_depth) = { let b = long_rx.borrow(); (b.2, b.3) };
        let (short_ask, short_depth) = { let b = short_rx.borrow(); (b.2, b.3) };

        // Require enough resting depth on both legs to fill the whole size.
        if long_depth < size_dec || short_depth < size_dec {
            continue;
        }

        let cost = long_ask + short_ask;
        let profit = dec!(1) - cost;
        if profit < edge {
            continue;
        }

        info!(
            "⚡ US arb: cost {:.4} (YES {:.4} + NO {:.4}) → edge {:.4}/pair × {} = ${:.2}",
            cost, long_ask, short_ask, profit, trade_size, profit * size_dec
        );

        // FOK both legs (all-or-nothing, immediate). `place_atomic` now uses the
        // gateway's engine-atomic `/v1/orders/batched` endpoint, so the pair places
        // together or not at all — no single-leg orphan.
        let legs = [
            leg(&pair.long, long_ask, size_dec),
            leg(&pair.short, short_ask, size_dec),
        ];
        match venue.place_atomic(legs).await {
            Ok([a, b]) => {
                info!(
                    "✅ US arb filled: YES {} @ {:.4} | NO {} @ {:.4}",
                    a.order_id, a.price, b.order_id, b.price
                );
                if let Some(p) = &pool {
                    db::record_open_position(p, "ArbitrageStrategy", pair.long.as_str(), &pair.question, "YES", a.price, a.filled, false).await;
                    db::record_open_position(p, "ArbitrageStrategy", pair.short.as_str(), &pair.question, "NO", b.price, b.filled, false).await;
                    sync_dashboard(venue.as_ref(), p, starting).await;
                }
            }
            Err(e) => warn!("US arb order failed: {e}"),
        }
        cooldown_until = Instant::now() + Duration::from_secs(ARB_COOLDOWN_SECS);
    }
}

/// Reconcile the Control Tower's view of the US venue: upsert live open positions,
/// purge settled ones, and write a portfolio P&L snapshot.
async fn sync_dashboard(venue: &UsRetailVenue, pool: &sqlx::SqlitePool, starting: Decimal) {
    let collateral = match venue.collateral().await {
        Ok(c) => c,
        Err(e) => { warn!("US dashboard sync: collateral query failed: {e}"); return; }
    };
    let positions = venue.positions().await.unwrap_or_default();

    let mut live_ids = std::collections::HashSet::new();
    let mut positions_value = Decimal::ZERO;
    for p in &positions {
        let sym = p.market.as_str();
        live_ids.insert(sym.to_string());
        db::record_open_position(
            pool, "ArbitrageStrategy", sym, sym, side_label(sym), p.avg_price, p.shares, false,
        ).await;
        positions_value += p.shares * p.avg_price;
    }
    // Drop rows for positions the venue no longer reports (settled to cash).
    let _ = db::purge_stale_open_positions(pool, &live_ids).await;

    let total = collateral + positions_value;
    db::record_pnl_snapshot(pool, total - starting, collateral, total).await;
}

/// `YES`/`NO` display label inferred from an instrument symbol suffix.
fn side_label(symbol: &str) -> &'static str {
    match symbol.rsplit('-').next().unwrap_or("").to_ascii_lowercase().as_str() {
        "no" | "short" | "down" => "NO",
        _ => "YES",
    }
}

fn leg(market: &crate::venues::core::MarketId, price: Decimal, qty: Decimal) -> OrderIntent {
    OrderIntent {
        market: market.clone(),
        side: Side::Buy,
        quantity: qty,
        price,
        tif: TimeInForce::Fok,
        post_only: false,
        expiration_secs: 0,
        is_neg_risk: false,
        fee_bps: 0,
    }
}

async fn wait_or_cancel(cancel: &CancellationToken, secs: u64) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(Duration::from_secs(secs)) => false,
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_decimal(key: &str, default: Decimal) -> Decimal {
    std::env::var(key)
        .ok()
        .and_then(|s| Decimal::from_str_exact(s.trim()).ok())
        .unwrap_or(default)
}

