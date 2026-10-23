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

//! Kalshi [`Execution`] implementation — V2 order endpoints.
//!
//! Order model mapping (V2 single-book sides):
//!
//! | Neutral intent          | Kalshi V2 order                     |
//! |-------------------------|-------------------------------------|
//! | Buy  `{t}#yes` @ P      | `side=bid`, `price=P`               |
//! | Sell `{t}#yes` @ P      | `side=ask`, `price=P`               |
//! | Buy  `{t}#no`  @ P      | `side=ask`, `price=1−P` (sell YES)  |
//! | Sell `{t}#no`  @ P      | `side=bid`, `price=1−P` (buy YES)   |
//!
//! Prices are fixed-point dollar strings; counts are fractional contracts.
//! Endpoint: `POST /portfolio/events/orders` (the legacy `/portfolio/orders`
//! is deprecated).

use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::venues::core::{
    Execution, Fill, MarketFacts, MarketId, OpenOrder, OrderId, OrderIntent, Position, Side,
    TimeInForce,
};

use super::{split_market_id, types, KalshiVenue};

/// Largest gap tolerated between a reported average fill price and the order's
/// limit price before we distrust the reported value. Taker fills sit at or
/// inside the limit, so anything wider means the field was misread.
const MAX_FILL_PRICE_DIVERGENCE: Decimal = rust_decimal_macros::dec!(0.10);

/// Map a neutral intent onto Kalshi's single YES book.
/// Returns `(ticker, book_side, price_on_yes_book)`.
fn map_intent(intent: &OrderIntent) -> (String, &'static str, Decimal) {
    let (ticker, is_yes) = split_market_id(intent.market.as_str());
    let (side, price) = match (is_yes, intent.side) {
        (true, Side::Buy) => ("bid", intent.price),
        (true, Side::Sell) => ("ask", intent.price),
        (false, Side::Buy) => ("ask", Decimal::ONE - intent.price),
        (false, Side::Sell) => ("bid", Decimal::ONE - intent.price),
    };
    (ticker, side, price)
}

fn tif_str(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::Gtc | TimeInForce::Gtd => "good_till_canceled",
        TimeInForce::Fak => "immediate_or_cancel",
        TimeInForce::Fok => "fill_or_kill",
    }
}

/// Pages of resting orders searched when a cancel's ticker is unknown.
const LOOKUP_MAX_PAGES: usize = 10;

/// Path of the V2 cancel. With a ticker Kalshi auto-routes to the shard the
/// market lives on; without one the request goes to shard 0.
fn cancel_path(order_id: &str, ticker: Option<&str>) -> String {
    match ticker {
        Some(t) => format!("/portfolio/events/orders/{order_id}?market_ticker={t}"),
        None => format!("/portfolio/events/orders/{order_id}"),
    }
}

impl KalshiVenue {
    /// Find the ticker of a resting order this process never placed or listed
    /// (one adopted from a previous run). The resting-orders list spans every
    /// shard, so a hit is remembered for the cancel that follows. `Ok(None)`
    /// means the order rests nowhere; `Err` means Kalshi could not be asked,
    /// which is not the same thing and must not route a cancel blind.
    async fn lookup_order_ticker(&self, order_id: &str) -> Result<Option<String>> {
        let mut cursor = String::new();
        for _page in 0..LOOKUP_MAX_PAGES {
            let path = if cursor.is_empty() {
                "/portfolio/orders?status=resting&limit=200".to_string()
            } else {
                format!("/portfolio/orders?status=resting&limit=200&cursor={cursor}")
            };
            let resp: types::OrdersResponse = self.get_json(&path).await?;
            if let Some(o) = resp.orders.iter().find(|o| o.order_id == order_id) {
                self.order_tickers.insert(order_id.to_string(), o.ticker.clone());
                return Ok(Some(o.ticker.clone()));
            }
            if resp.cursor.is_empty() || resp.orders.is_empty() {
                break;
            }
            cursor = resp.cursor;
        }
        Ok(None)
    }

    async fn place_one(&self, intent: &OrderIntent) -> Result<Fill> {
        let (ticker, side, price) = map_intent(intent);
        // Kalshi counts are submitted at 2dp, so this — not the raw intent
        // quantity — is what we actually asked for. Comparing fills against the
        // unrounded value made every complete fill look partial (8.19 filled vs
        // 8.19000819… requested), which cried wolf on every single order.
        let submitted = intent.quantity.round_dp(2);
        let mut body = serde_json::json!({
            "ticker": ticker,
            "client_order_id": uuid_v4(),
            "side": side,
            "count": format!("{:.2}", submitted),
            "price": format!("{:.4}", price),
            "time_in_force": tif_str(intent.tif),
            "self_trade_prevention_type": "taker_at_cross",
            "post_only": intent.post_only,
        });
        if matches!(intent.tif, TimeInForce::Gtd) && intent.expiration_secs > 0 {
            body["expiration_time"] = serde_json::json!(
                chrono::Utc::now().timestamp() + intent.expiration_secs as i64
            );
        }
        // The create-order response is flat, not `{"order": …}` — see
        // `types::order_from_response`. Deserialize in two steps so an
        // unexpected shape can still be shown in full.
        let raw: serde_json::Value = self
            .post_json("/portfolio/events/orders", &body)
            .await?;
        let o = types::order_from_response(&raw);
        if !o.order_id.is_empty() {
            self.order_tickers.insert(o.order_id.clone(), ticker.clone());
        }
        if o.order_id.is_empty() {
            tracing::warn!(
                "⚠️ Kalshi order response carried no order_id — cancel/lifecycle tracking \
                 will not work for this order. Raw response: {}",
                super::truncate(&raw.to_string(), 400)
            );
        }
        let requested = submitted;
        // Never infer a fill. The previous version derived `filled` from
        // `count − remaining` with `count` defaulting to the requested size, so
        // a response carrying neither key read as 100% filled — which is how a
        // stop-loss that returned `"fill_count":"0.00"` was booked as a closed
        // position while the real one sat on Kalshi until it expired worthless
        // (observed 2026-08-10, KXBTCD-26AUG1003). Unknown means zero.
        let filled = match o.filled_count() {
            Some(f) => f,
            None => {
                tracing::error!(
                    "🛑 Kalshi order response had no recognizable fill count — treating as \
                     UNFILLED. If the order did fill, lifecycle reconciliation must re-adopt \
                     it. Raw response: {}",
                    super::truncate(&raw.to_string(), 400)
                );
                Decimal::ZERO
            }
        };
        if filled < requested {
            tracing::warn!(
                "⚠️ Kalshi partial/zero fill: {} {:?} {} — requested {:.2}, filled {:.2}",
                intent.market.as_str(), intent.side, ticker, requested, filled,
            );
        } else if !o.order_id.is_empty() {
            // Nothing rests, so nothing will be cancelled: keep the map to resting orders.
            self.order_tickers.remove(&o.order_id);
        }
        // Money moved; the next balance reading must come from the venue.
        self.invalidate_balance().await;
        // Book the price Kalshi actually traded at, not the limit we asked for.
        // `average_fill_price` is quoted on the YES book for both legs, so the
        // NO leg needs the complement. Guard against the convention flipping
        // under us: a large divergence from the limit means we've misread the
        // field, and a corrupted entry price would poison every downstream
        // TP/SL calculation — keep the limit price and say so instead.
        let (_, is_yes) = split_market_id(intent.market.as_str());
        let price = match o.avg_fill_price_for_leg(is_yes) {
            Some(p) if (p - intent.price).abs() <= MAX_FILL_PRICE_DIVERGENCE => p,
            Some(p) => {
                tracing::warn!(
                    "⚠️ Kalshi avg fill ${:.4} diverges >{:.2} from limit ${:.4} on {} — \
                     keeping limit price for P&L. Raw response: {}",
                    p, MAX_FILL_PRICE_DIVERGENCE, intent.price, intent.market.as_str(),
                    super::truncate(&raw.to_string(), 400)
                );
                intent.price
            }
            None => intent.price,
        };
        // Kalshi reports the fee per contract; P&L needs the total.
        let fee_per_contract = types::fp(&o.average_fee_paid).unwrap_or_default();
        let fee = (fee_per_contract * filled).max(Decimal::ZERO);
        if !fee.is_zero() {
            tracing::info!(
                "💸 Kalshi fee: ${:.4}/contract × {:.2} = ${:.4} on {}",
                fee_per_contract, filled, fee, intent.market.as_str(),
            );
        }
        Ok(Fill {
            order_id: OrderId(o.order_id),
            market: intent.market.clone(),
            filled,
            price,
            fee,
        })
    }
}

#[async_trait]
impl Execution for KalshiVenue {
    async fn place_order(&self, intent: OrderIntent) -> Result<Fill> {
        // A retired instance places nothing: its replacement owns the account (E64).
        crate::helpers::migration::refuse_if_retired()?;
        self.place_one(&intent).await
    }

    async fn place_atomic(&self, legs: [OrderIntent; 2]) -> Result<[Fill; 2]> {
        crate::helpers::migration::refuse_if_retired()?;
        // Kalshi has no atomic two-leg endpoint; place sequentially
        // (network-atomic best effort, same contract as the US venue).
        let a = self.place_one(&legs[0]).await?;
        let b = match self.place_one(&legs[1]).await {
            Ok(f) => f,
            Err(e) => {
                // Second leg failed — try to cancel the first so we're not naked.
                let _ = self.cancel(a.order_id.clone()).await;
                return Err(e);
            }
        };
        Ok([a, b])
    }

    async fn cancel(&self, id: OrderId) -> Result<()> {
        // Kalshi routes a cancel to an exchange shard by market ticker; the
        // order id alone goes to shard 0 and misses any order resting on the
        // crypto or sports shard. Use the ticker remembered at placement, else
        // look the order up across all shards before giving up on routing.
        let ticker = match self.order_tickers.get(&id.0).map(|t| t.clone()) {
            Some(t) => Some(t),
            None => self.lookup_order_ticker(&id.0).await.with_context(|| format!(
                "Kalshi cancel {}: ticker unknown and the resting-orders lookup failed, \
                 so the cancel cannot be routed to its exchange shard",
                id.0
            ))?,
        };
        if ticker.is_none() {
            // Not resting on any shard: filled or already gone. The bare cancel
            // goes to shard 0 and reports whatever Kalshi knows about the id.
            tracing::warn!(
                "⚠️ Kalshi cancel {}: order rests on no exchange shard; sending the cancel unrouted",
                id.0
            );
        }
        let _: serde_json::Value = self
            .delete_json(&cancel_path(&id.0, ticker.as_deref()))
            .await?;
        self.order_tickers.remove(&id.0);
        self.invalidate_balance().await;
        Ok(())
    }

    async fn collateral(&self) -> Result<Decimal> {
        // Shared across callers for a few seconds, never longer: this figure is
        // booked into P&L snapshots right after fills, and a fill empties the
        // cache first, so the post-fill read still reaches the venue. See
        // `COLLATERAL_SHARED_SECS` for the rate limit that forced the sharing.
        let resp = self.balance_shared().await?;
        Ok(resp.dollars())
    }

    async fn collateral_pools(&self) -> Result<Vec<crate::venues::core::CollateralPool>> {
        Ok(self.balance().await?.pools())
    }

    async fn move_collateral(&self, from: &str, to: &str, amount: Decimal, idempotency_key: &str) -> Result<String> {
        let shard = |id: &str| id.parse::<i64>().with_context(|| format!("pool id {id:?} is not a Kalshi exchange shard index"));
        let (from_shard, to_shard) = (shard(from)?, shard(to)?);
        if from_shard == to_shard {
            anyhow::bail!("source and destination are the same shard");
        }
        if amount <= Decimal::ZERO {
            anyhow::bail!("amount must be positive");
        }
        let body = types::transfer_body(from_shard, to_shard, amount, idempotency_key);
        let resp: types::TransferResponse = self
            .post_json("/portfolio/intra_exchange_instance_transfer", &body)
            .await?;
        // Kalshi processes the transfer asynchronously; the next reading must
        // come from the venue rather than the cache.
        self.invalidate_balance().await;
        Ok(resp.transfer_id)
    }

    async fn collateral_allocation(&self) -> Result<Option<Vec<crate::venues::core::AllocationTarget>>> {
        let resp: types::AllocationResponse = self.get_json("/portfolio/target_balance_allocation").await?;
        Ok(Some(
            resp.allocations
                .into_iter()
                .map(|a| crate::venues::core::AllocationTarget {
                    pool_id: a.exchange_index.to_string(),
                    percent: a.percent.clamp(0, 100) as u8,
                })
                .collect(),
        ))
    }

    async fn set_collateral_allocation(&self, targets: &[crate::venues::core::AllocationTarget]) -> Result<()> {
        let mut shards = Vec::with_capacity(targets.len());
        for t in targets {
            let idx = t.pool_id.parse::<i64>().with_context(|| format!("pool id {:?} is not a Kalshi exchange shard index", t.pool_id))?;
            shards.push((idx, t.percent));
        }
        let _: serde_json::Value = self
            .post_json("/portfolio/target_balance_allocation", &types::allocation_body(&shards))
            .await?;
        self.invalidate_balance().await;
        Ok(())
    }

    /// The shard pool `market` spends from. `None` when Kalshi gave no
    /// breakdown or never said which shard the market trades on; the caller
    /// then falls back to the whole wallet, as before shards existed.
    async fn collateral_pool_for(&self, market: &MarketId) -> Result<Option<crate::venues::core::CollateralPool>> {
        let (ticker, _) = split_market_id(market.as_str());
        let Some(idx) = self.shard_of(&ticker).await else { return Ok(None) };
        let balance = self.balance().await?;
        Ok(balance.shard_dollars(idx).map(|b| crate::venues::core::CollateralPool {
            id: idx.to_string(),
            label: types::shard_label(idx),
            balance: b,
        }))
    }

    async fn positions(&self) -> Result<Vec<Position>> {
        let resp: types::PositionsResponse = self
            .get_json("/portfolio/positions?count_filter=position&limit=200")
            .await?;
        let mut out = Vec::new();
        for p in resp.market_positions {
            let signed = p.signed_contracts();
            if signed.is_zero() {
                continue;
            }
            // Positive = long YES leg, negative = long NO leg.
            let yes = signed > Decimal::ZERO;
            let shares = signed.abs();
            // avg price ≈ traded dollars / contracts when available.
            let avg_price = types::fp(&p.market_exposure_dollars)
                .and_then(|expo| if shares.is_zero() { None } else { Some(expo / shares) })
                .unwrap_or_default();
            out.push(Position {
                market: MarketId::new(super::leg_id(&p.ticker, yes)),
                shares,
                avg_price,
            });
        }
        Ok(out)
    }

    async fn open_orders(&self) -> Result<Vec<OpenOrder>> {
        let resp: types::OrdersResponse = self
            .get_json("/portfolio/orders?status=resting&limit=200")
            .await?;
        let mut out = Vec::new();
        for o in resp.orders {
            self.order_tickers.insert(o.order_id.clone(), o.ticker.clone());
            let total = o.requested_count().unwrap_or_default();
            let filled = o.filled_count().unwrap_or_default();
            let price = types::fp(&o.price_dollars).unwrap_or_default();
            // V2 `bid` = buy on the YES book. Express every resting order as a
            // YES-leg order so lifecycle reconciliation has one convention.
            let side = if o.side == "bid" { Side::Buy } else { Side::Sell };
            out.push(OpenOrder {
                order_id: OrderId(o.order_id),
                market: MarketId::new(super::leg_id(&o.ticker, true)),
                side,
                price,
                original_qty: total,
                filled_qty: filled,
                tif: match o.time_in_force.as_str() {
                    "fill_or_kill" => TimeInForce::Fok,
                    "immediate_or_cancel" => TimeInForce::Fak,
                    _ => TimeInForce::Gtc,
                },
                pair_market: None,
            });
        }
        Ok(out)
    }

    fn subscribe_fills(&self) -> Option<crate::venues::core::FillStream> {
        Some(self.fills_tx.subscribe())
    }

    async fn best_ask(&self, market: &MarketId) -> Result<Option<Decimal>> {
        let (ticker, is_yes) = split_market_id(market.as_str());
        let book = self.orderbook(&ticker).await?;
        // Ask for the requested leg = 1 − best bid of the *other* leg.
        let ask = if is_yes {
            book.best_yes_ask().map(|(p, _)| p)
        } else {
            book.best_yes_bid().map(|(p, _)| Decimal::ONE - p)
        };
        Ok(ask)
    }

    /// Best bid on the requested leg — the mirror of `best_ask` above.
    ///
    /// Kalshi publishes one book per market, quoted on the YES side, so the NO
    /// leg is the crossed complement: its bid is one minus the YES ASK, exactly
    /// as its ask is one minus the YES bid. Taking `1 − yes_bid` for both would
    /// collapse the spread to a single price and make a stop look reachable when
    /// it is not.
    async fn best_bid(&self, market: &MarketId) -> Result<Option<Decimal>> {
        let (ticker, is_yes) = split_market_id(market.as_str());
        let book = self.orderbook(&ticker).await?;
        let bid = if is_yes {
            book.best_yes_bid().map(|(p, _)| p)
        } else {
            book.best_yes_ask().map(|(p, _)| Decimal::ONE - p)
        };
        Ok(bid)
    }

    /// The market's two legs, its title and when it closes.
    ///
    /// `market` may arrive with or without a leg suffix; either way the ticker
    /// identifies the market and `leg_id` recomposes both legs.
    ///
    /// A Kalshi market publishes two times: `close_time`, when trading stops,
    /// and `expected_expiration_time`, when it expects to settle.
    ///
    /// `close_time` alone is used, deliberately, even though a market can close
    /// early. It is the figure the tick enforces against
    /// (`pair_from_market_untethered` reads `close_time_utc()`), and a validator
    /// that judges a posture on a different number than the engine will act on is
    /// worse than one that is slightly generous: taking the earlier of the two
    /// made the API refuse a legitimate in-play entry as "too close to market
    /// close" while the tick saw plenty of time, and refused nothing the tick
    /// would have caught. One figure, in both places.
    ///
    /// `leg_labels` stays `None`: a Kalshi market really is a yes/no question, so
    /// labelling the legs would add nothing an operator does not already know.
    async fn market_facts(&self, market: &MarketId) -> Result<Option<MarketFacts>> {
        let (ticker, _) = split_market_id(market.as_str());
        let m = self.market(&ticker).await?;
        let parse = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s.trim())
                .ok()
                .map(|d| d.with_timezone(&chrono::Utc))
        };
        // No fallback to `expected_expiration_time`: when `close_time` fails to
        // parse the tick gets `None`, so falling back here would once again judge
        // a posture on a figure the engine will not act on.
        let close_time = parse(&m.close_time);
        // Kalshi takes orders under `active` (its record vocabulary) or `open`
        // (its query vocabulary, which `interpret_settlement` also accepts);
        // `initialized` and `unopened` are before the open, `paused` and
        // `inactive` a halt, `closed`, `determined`, `finalized` and `settled`
        // after it. A status this list does not know is left to the clock
        // rather than read as either answer.
        let accepting_orders = match m.status.trim() {
            "active" | "open" => Some(true),
            "initialized" | "unopened" | "inactive" | "paused" | "closed" | "determined" | "finalized" | "settled" => Some(false),
            _ => None,
        };
        Ok(Some(MarketFacts {
            market_id: market.clone(),
            question: m.title,
            // Kalshi's record carries no free-text criteria field; the title is the
            // whole statement of the question on this venue.
            criteria: String::new(),
            yes_token: MarketId::new(crate::venues::kalshi::leg_id(&ticker, true)),
            no_token: MarketId::new(crate::venues::kalshi::leg_id(&ticker, false)),
            leg_labels: None,
            close_time,
            accepting_orders,
        }))
    }

    async fn order_book(&self, leg: &MarketId) -> Result<Option<crate::venues::core::LegBook>> {
        let (ticker, is_yes) = split_market_id(leg.as_str());
        let book = self.orderbook(&ticker).await?;
        Ok(Some(crate::venues::kalshi::types::leg_book_from_fp(&book, is_yes)))
    }

    async fn recent_prints(&self, market: &MarketId, limit: usize) -> Result<Option<Vec<crate::venues::core::TapePrint>>> {
        let (ticker, _) = split_market_id(market.as_str());
        let resp: crate::venues::kalshi::types::TradesResponse = self
            .get_json(&format!("/markets/trades?ticker={ticker}&limit={}", limit.clamp(1, 1000)))
            .await?;
        let mut prints: Vec<_> = resp.trades.iter().filter_map(crate::venues::kalshi::types::print_from_trade).collect();
        prints.sort_by(|a, b| b.at.cmp(&a.at));
        Ok(Some(prints))
    }

    async fn resolution(&self, leg: &MarketId) -> Result<crate::venues::core::TokenResolution> {
        Ok(self.settlement_resolution(leg.as_str()).await)
    }
}

/// Pages of the trades endpoint the recorder will walk for one hour. A busy
/// hourly market prints well over the single page `recent_prints` returns
/// (1,000 prints covered the last nine minutes of the first recorded hour).
const TRADES_MAX_PAGES: usize = 40;

impl KalshiVenue {
    /// Every print of `ticker` between `min_ts` and `max_ts` (unix seconds), walking
    /// the endpoint's cursor, oldest first. For the hourly market recorder: the
    /// Execution trait's `recent_prints` is one page, which is not an hour.
    pub async fn trades_between(&self, ticker: &str, min_ts: i64, max_ts: i64) -> Result<Vec<crate::venues::core::TapePrint>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..TRADES_MAX_PAGES {
            let mut url = format!("/markets/trades?ticker={ticker}&limit=1000&min_ts={min_ts}&max_ts={max_ts}");
            if let Some(c) = cursor.as_deref().filter(|c| !c.is_empty()) { url.push_str(&format!("&cursor={c}")); }
            let resp: crate::venues::kalshi::types::TradesResponse = self.get_json(&url).await?;
            out.extend(resp.trades.iter().filter_map(crate::venues::kalshi::types::print_from_trade));
            match resp.cursor.filter(|c| !c.is_empty()) {
                Some(c) if !resp.trades.is_empty() => cursor = Some(c),
                _ => break,
            }
        }
        out.sort_by(|a, b| a.at.cmp(&b.at));
        Ok(out)
    }
}

/// Random v4-style UUID for client_order_id idempotency (no new dep — derived
/// from two `getrandom`-backed u64s via the `rsa` crate's rand_core).
fn uuid_v4() -> String {
    use rsa::rand_core::RngCore;
    let mut rng = rsa::rand_core::OsRng;
    let (a, b) = (rng.next_u64(), rng.next_u64());
    let bytes = [a.to_be_bytes(), b.to_be_bytes()].concat();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-4{:01x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6] & 0x0f, bytes[7],
        (bytes[8] & 0x3f) | 0x80, bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn intent(market: &str, side: Side, price: Decimal) -> OrderIntent {
        OrderIntent {
            market: MarketId::new(market),
            side,
            quantity: dec!(10),
            price,
            tif: TimeInForce::Gtc,
            post_only: false,
            expiration_secs: 0,
            is_neg_risk: false,
            fee_bps: 0,
        }
    }

    #[test]
    fn maps_yes_and_no_legs_onto_single_book() {
        // Buy YES @ 0.30 → bid 0.30
        let (t, s, p) = map_intent(&intent("KX-1#yes", Side::Buy, dec!(0.30)));
        assert_eq!((t.as_str(), s, p), ("KX-1", "bid", dec!(0.30)));
        // Sell YES @ 0.30 → ask 0.30
        let (_, s, p) = map_intent(&intent("KX-1#yes", Side::Sell, dec!(0.30)));
        assert_eq!((s, p), ("ask", dec!(0.30)));
        // Buy NO @ 0.70 → ask (sell YES) at 1−0.70 = 0.30
        let (_, s, p) = map_intent(&intent("KX-1#no", Side::Buy, dec!(0.70)));
        assert_eq!((s, p), ("ask", dec!(0.30)));
        // Sell NO @ 0.70 → bid (buy YES) at 0.30
        let (_, s, p) = map_intent(&intent("KX-1#no", Side::Sell, dec!(0.70)));
        assert_eq!((s, p), ("bid", dec!(0.30)));
    }

    /// A cancel must carry the market ticker: Kalshi routes requests to an
    /// exchange shard by ticker, and an order id alone lands on shard 0 where a
    /// crypto or sports order never rested.
    #[test]
    fn a_cancel_carries_the_market_ticker_so_kalshi_routes_it_to_the_right_shard() {
        assert_eq!(
            cancel_path("abc-1", Some("KXBTCD-26OCT1810-T81999.99")),
            "/portfolio/events/orders/abc-1?market_ticker=KXBTCD-26OCT1810-T81999.99"
        );
    }

    /// Without a ticker the path is the bare V2 cancel, which Kalshi sends to
    /// shard 0; the caller logs that the routing is a guess.
    #[test]
    fn a_cancel_without_a_known_ticker_falls_back_to_the_bare_path() {
        assert_eq!(cancel_path("abc-1", None), "/portfolio/events/orders/abc-1");
    }

    #[test]
    fn uuid_shape() {
        let u = uuid_v4();
        assert_eq!(u.len(), 36);
        assert_eq!(u.chars().filter(|c| *c == '-').count(), 4);
        assert_eq!(&u[14..15], "4"); // version nibble
    }
}
