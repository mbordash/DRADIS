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

use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::venues::core::{
    Execution, Fill, OpenOrder, OrderId, OrderIntent, MarketId, Position, Side, TimeInForce,
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

impl KalshiVenue {
    async fn place_one(&self, intent: &OrderIntent) -> Result<Fill> {
        let (ticker, side, price) = map_intent(intent);
        let mut body = serde_json::json!({
            "ticker": ticker,
            "client_order_id": uuid_v4(),
            "side": side,
            "count": format!("{:.2}", intent.quantity),
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
        if o.order_id.is_empty() {
            tracing::warn!(
                "⚠️ Kalshi order response carried no order_id — cancel/lifecycle tracking \
                 will not work for this order. Raw response: {}",
                super::truncate(&raw.to_string(), 400)
            );
        }
        let requested = intent.quantity;
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
                    "🛑 Kalshi order response had no recognisable fill count — treating as \
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
        }
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
        if let Some(fee) = types::fp(&o.average_fee_paid) {
            if !fee.is_zero() && !filled.is_zero() {
                tracing::info!(
                    "💸 Kalshi fee: ${:.4}/contract × {:.2} = ${:.4} on {} (not yet in recorded P&L)",
                    fee, filled, fee * filled, intent.market.as_str(),
                );
            }
        }
        Ok(Fill {
            order_id: OrderId(o.order_id),
            market: intent.market.clone(),
            filled,
            price,
        })
    }
}

#[async_trait]
impl Execution for KalshiVenue {
    async fn place_order(&self, intent: OrderIntent) -> Result<Fill> {
        self.place_one(&intent).await
    }

    async fn place_atomic(&self, legs: [OrderIntent; 2]) -> Result<[Fill; 2]> {
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
        let _: serde_json::Value = self
            .delete_json(&format!("/portfolio/events/orders/{}", id.0))
            .await?;
        Ok(())
    }

    async fn collateral(&self) -> Result<Decimal> {
        let resp: types::BalanceResponse = self.get_json("/portfolio/balance").await?;
        Ok(resp.dollars())
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

    #[test]
    fn uuid_shape() {
        let u = uuid_v4();
        assert_eq!(u.len(), 36);
        assert_eq!(u.chars().filter(|c| *c == '-').count(), 4);
        assert_eq!(&u[14..15], "4"); // version nibble
    }
}
