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

//! Intl CLOB venue — Polymarket's international, self-custody platform.
//!
//! Holds all venue-specific security/identity state (authenticated CLOB client,
//! EOA signer, nonce manager, Safe/EOA addresses) privately, and maps the
//! venue-neutral [`Execution`] contract onto EIP-712 signed orders over Polygon.
//!
//! `U256` token-id knowledge is confined to this module: the neutral
//! [`MarketId`] carries a decimal-`U256` string that we parse only at the
//! trait boundary (see `docs/VENUE_ABSTRACTION.md`, decision D5).

pub mod orders;

/// Runtime venue identity persisted on every trade and entry row.
///
/// On this venue the SQLite shard key genuinely *is* the underlying asset
/// (`btc`, `eth`, `sol`), which is why the two were conflated in the first
/// place. They are still distinct concepts — see `state::TradeScope`.
pub const INTL_VENUE: &str = "polymarket-intl";

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::str::FromStr;
use tracing::info;

use alloy::primitives::{address, Address, U256};
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;

use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config};
use polymarket_client_sdk_v2::clob::types::{Side as ClobSide, SignatureType};
use polymarket_client_sdk_v2::clob::types::request::{BalanceAllowanceRequest, OrdersRequest};
use polymarket_client_sdk_v2::clob::types::AssetType;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::{POLYGON, PRIVATE_KEY_VAR, derive_safe_wallet};

use tokio::sync::Mutex;

use crate::config;
use crate::helpers::nonce::fetch_next_nonce;
use crate::venues::core::{
    Execution, Fill, MarketId, OpenOrder, OrderId, OrderIntent, Position, Side, TimeInForce,
};

// ── V2 CTF Exchange verifying contracts (per neg-risk routing) ───────────────
// Mirrors the constants in `squadron/patrol_tasks.rs`; kept private to the venue.
const EXCHANGE_NORMAL: Address = address!("0xE111180000d2663C0091e4f400237545B87B996B");
const EXCHANGE_NEG_RISK: Address = address!("0xe2222d279d744050d28e00520010520000310F59");

/// Public resolver for the EIP-712 verifying contract (CTF Exchange) that must be
/// used when signing an order for a market of the given neg-risk status. Callers
/// outside the venue (e.g. the API's manual-exit/RTB path) MUST derive the address
/// this way rather than trusting a client-supplied value — a mismatched verifying
/// contract produces the wrong EIP-712 domain and an "invalid POLY_GNOSIS_SAFE
/// signature" rejection from the CLOB.
pub fn exchange_verifying_contract(is_neg_risk: bool) -> Address {
    if is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL }
}

// ── MarketId ↔ U256 boundary (decision D5/D6) ────────────────────────────────
// The neutral `MarketId` carries the decimal-`U256` string for the intl venue.
// These are the ONLY sanctioned conversion points outside the order-signing path:
// chain-edge helpers (balance queries, redeem math, gamma parsing) call them so
// `U256` never becomes a domain key elsewhere in the codebase.

/// Wrap an on-chain ERC-1155 token id as a venue-neutral [`MarketId`]
/// (decimal-string form, identical to `U256::to_string()`).
pub fn market_id_from_u256(token: U256) -> MarketId {
    MarketId::new(token.to_string())
}

/// Parse a [`MarketId`] back into its on-chain `U256` token id.
///
/// Errors if the id is not a decimal `U256` string (e.g. a US UUID/slug),
/// which would indicate a venue mismatch.
pub fn u256_from_market_id(market: &MarketId) -> Result<U256> {
    U256::from_str_radix(market.as_str(), 10)
        .with_context(|| format!("intl: invalid MarketId (not decimal U256): {market}"))
}

/// Polymarket's taker fee for a matched order, in USDC.
///
///   fee = rate · p · (1 − p) · shares
///
/// Quadratic in price, so it peaks at 50¢ and collapses toward either tail —
/// the same shape Kalshi uses (see `venues::kalshi::trader`), with the same 7¢
/// coefficient. **Makers pay nothing**: call this only for an order that
/// matched immediately, which the callers detect by a non-zero making/taking
/// pair coming back from the exchange.
///
/// The venue's `fee-rate-bps` endpoint reports 1000 bps on these markets, but
/// that is the *ceiling* an order authorizes, not the charged rate — signing
/// against it and then booking 10% of notional would overstate the cost by
/// nearly 2×. The rate here is the one actually charged, recovered from
/// collateral movement and validated against every leg of the 2026-08-12 BTC
/// session to within $0.0006 (see `fee_matches_observed_intl_fills`).
///
/// Prices outside `[0, 1]` cannot occur on a binary book; they clamp to zero
/// fee rather than producing a negative charge.
pub fn taker_fee(rate: Decimal, price: Decimal, shares: Decimal) -> Decimal {
    if price <= Decimal::ZERO || price >= Decimal::ONE || shares <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    rate * price * (Decimal::ONE - price) * shares
}

/// The live taker-fee rate, falling back to the compile-time default before the
/// config channel is published (venue bootstrap) or if it has gone away.
///
/// The patrol loop snapshots the same knob once per tick rather than calling
/// this, so a rate edited in Control Tower reaches both paths.
pub fn live_taker_fee_rate() -> Decimal {
    crate::helpers::dynamic_config::global_config_tx()
        .map(|tx| tx.borrow().intl_taker_fee_rate)
        .unwrap_or(config::INTL_TAKER_FEE_RATE)
}

/// The international (self-custody) Polymarket CLOB venue.
pub struct IntlClobVenue {
    /// Authenticated CLOB REST client used for all order/balance operations.
    clob: Arc<ClobClient<Authenticated<Normal>>>,
    /// EOA signing key for EIP-712 order signatures.
    signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    /// Session-scoped nonce manager (AtomicU64, retained for API compatibility).
    nonce: Arc<AtomicU64>,
    /// Shared HTTP client for nonce re-sync and order placement.
    http: Arc<reqwest::Client>,
    /// Derived Gnosis Safe (maker) address.
    safe_address: Address,
    /// EOA (signer) address.
    eoa_address: Address,
    /// Active token IDs the lifecycle engine should query for positions / open-orders.
    ///
    /// The patrol loop registers the current market's YES+NO tokens here so that
    /// `positions()` and `open_orders()` can poll the CLOB for just those tokens —
    /// avoiding a full scan of every token ever traded. Cleared on market rotation.
    active_tokens: Arc<Mutex<HashSet<MarketId>>>,
}

impl IntlClobVenue {
    /// Bootstrap the intl venue: load the private key, authenticate the CLOB
    /// client, derive the Safe/EOA addresses, and initialize the nonce from the
    /// Polymarket API.
    ///
    /// Encapsulates the signer/nonce bootstrap that previously lived inline in
    /// `main.rs`. The Polygon settlement `Provider` is intentionally *not* owned
    /// here — it is a separate on-chain subsystem, generic over the patrol loop.
    pub async fn connect(http: Arc<reqwest::Client>) -> Result<Self> {
        let private_key = std::env::var(PRIVATE_KEY_VAR)
            .context("POLYMARKET_PRIVATE_KEY not set")?;

        let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
        let eoa_address = signer.address();
        info!("Trading wallet (EOA) address: {}", eoa_address);

        let clob = Arc::new(
            ClobClient::new(config::CLOB_API_BASE, Config::default())?
                .authentication_builder(&signer)
                .signature_type(SignatureType::GnosisSafe)
                .authenticate()
                .await?,
        );

        let safe_address = derive_safe_wallet(eoa_address, POLYGON)
            .context("Safe derivation failed")?;
        info!("Authenticated on Polymarket CLOB. Safe (Maker) address: {}", safe_address);

        let initial_nonce = fetch_next_nonce(&http, safe_address).await.unwrap_or(0);
        info!(" Initialized Nonce from API (Maker/Safe): {}", initial_nonce);
        let nonce = Arc::new(AtomicU64::new(initial_nonce));

        Ok(Self { clob, signer, nonce, http, safe_address, eoa_address,
                   active_tokens: Arc::new(Mutex::new(HashSet::new())) })
    }

    // ── Accessors (raw infra for call sites not yet on the Execution trait) ──

    /// Authenticated CLOB client (fee-rate / neg-risk / balance / cancel queries).
    pub fn trading_client(&self) -> &Arc<ClobClient<Authenticated<Normal>>> {
        &self.clob
    }

    /// EOA signing key.
    pub fn signer(&self) -> &LocalSigner<alloy::signers::k256::ecdsa::SigningKey> {
        &self.signer
    }

    /// Session-scoped nonce manager.
    pub fn nonce_manager(&self) -> &Arc<AtomicU64> {
        &self.nonce
    }

    /// Shared HTTP client.
    pub fn shared_http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Derived Gnosis Safe (maker) address.
    pub fn safe_address(&self) -> Address {
        self.safe_address
    }

    /// EOA (signer) address.
    pub fn eoa_address(&self) -> Address {
        self.eoa_address
    }

    // ── Token registry (shared OrderLifecycle support) ────────────────────────

    /// Register tokens the lifecycle engine should watch (YES + NO legs of the
    /// current market). Called by the patrol loop when entering a new market or
    /// after placing an arb order.
    pub async fn register_tokens(&self, tokens: &[MarketId]) {
        let mut set = self.active_tokens.lock().await;
        for t in tokens { set.insert(t.clone()); }
    }

    /// Remove tokens from the active set (e.g. after confirmed settlement).
    pub async fn unregister_tokens(&self, tokens: &[MarketId]) {
        let mut set = self.active_tokens.lock().await;
        for t in tokens { set.remove(t); }
    }

    /// Clear all active tokens on market rotation so stale tokens are not
    /// queried in `positions()` / `open_orders()` for the next market cycle.
    pub async fn clear_active_tokens(&self) {
        self.active_tokens.lock().await.clear();
    }

    // ── Private boundary helpers (U256 stays inside venues::intl) ────────────

    /// Pick the EIP-712 verifying contract for a market's neg-risk flag.
    fn verifying_contract(is_neg_risk: bool) -> Address {
        if is_neg_risk { EXCHANGE_NEG_RISK } else { EXCHANGE_NORMAL }
    }

    fn map_side(side: Side) -> ClobSide {
        match side {
            Side::Buy => ClobSide::Buy,
            Side::Sell => ClobSide::Sell,
        }
    }

}

#[async_trait]
impl Execution for IntlClobVenue {
    async fn place_order(&self, intent: OrderIntent) -> Result<Fill> {
        let vc = Self::verifying_contract(intent.is_neg_risk);

        let (order_id, making_amount, taking_amount) = orders::place_limit_order_filled(
            &self.clob,
            &self.nonce,
            &self.signer,
            self.safe_address,
            self.eoa_address,
            vc,
            &intent.market,
            Self::map_side(intent.side),
            intent.quantity,
            intent.price,
            intent.fee_bps,
            intent.tif,
            intent.post_only,
            intent.expiration_secs,
            &self.http,
        )
        .await?;

        // Derive the REAL average fill price from the matched amounts rather than
        // echoing the limit. A marketable FAK/FOK (e.g. a naked-leg flatten with a
        // $0.01 limit) often fills far better than its limit; booking the limit
        // produced phantom losses (2026-06-21 trade 56: a leg that sold at $0.4426
        // was booked at $0.01 → −$5.30 instead of −$0.97).
        //
        // making/taking come back in the order's maker/taker orientation:
        //   SELL → making = shares given, taking = USDC received → price = taking/making
        //   BUY  → making = USDC paid,    taking = shares recv   → price = making/taking
        // The ratio is unit-invariant (any shared 1e6 scaling cancels). We clamp to
        // a valid binary price (0,1]; anything outside means the response orientation
        // was unexpected, so we fall back to the limit. Resting GTC/GTD orders match
        // nothing immediately (making/taking = 0) and also fall back to the limit.
        let fill_price = if making_amount > dec!(0) && taking_amount > dec!(0) {
            let p = match intent.side {
                Side::Sell => taking_amount / making_amount,
                Side::Buy  => making_amount / taking_amount,
            };
            if p > dec!(0) && p <= dec!(1) { p } else { intent.price }
        } else {
            intent.price
        };

        // Fee on the same terms the price was derived: a non-zero making/taking
        // pair means the order matched immediately, so we were the taker. A
        // resting order reports zeros, pays nothing now, and is re-priced by the
        // lifecycle when it does fill. The exchange charges this out of
        // collateral and reports it nowhere, so leaving it at zero here made
        // every lifecycle-adopted position carry a free entry (see `taker_fee`).
        let matched_shares = if making_amount > dec!(0) && taking_amount > dec!(0) {
            match intent.side {
                Side::Sell => making_amount,
                Side::Buy  => taking_amount,
            }
        } else {
            Decimal::ZERO
        };
        let fee = taker_fee(live_taker_fee_rate(), fill_price, matched_shares);

        Ok(Fill {
            order_id: OrderId(order_id),
            market: intent.market,
            filled: intent.quantity,
            price: fill_price, fee
        })
    }

    async fn place_atomic(&self, legs: [OrderIntent; 2]) -> Result<[Fill; 2]> {
        let [a, b] = legs;

        let (id_a, id_b) = orders::place_limit_orders_atomic(
            &self.clob,
            &self.nonce,
            &self.signer,
            self.safe_address,
            self.eoa_address,
            Self::verifying_contract(a.is_neg_risk),
            &a.market,
            Self::map_side(a.side),
            a.quantity,
            a.price,
            a.tif,
            a.post_only,
            a.expiration_secs,
            Self::verifying_contract(b.is_neg_risk),
            &b.market,
            Self::map_side(b.side),
            b.quantity,
            b.price,
            b.tif,
            b.post_only,
            b.expiration_secs,
            &self.http,
        )
        .await?;

        Ok([
            Fill { order_id: OrderId(id_a), market: a.market, filled: a.quantity, price: a.price, fee: Decimal::ZERO},
            Fill { order_id: OrderId(id_b), market: b.market, filled: b.quantity, price: b.price, fee: Decimal::ZERO},
        ])
    }

    async fn cancel(&self, id: OrderId) -> Result<()> {
        let id_str = id.0.clone();
        self.clob
            .cancel_orders(&[id_str.as_str()])
            .await
            .map_err(|e| anyhow::anyhow!("intl cancel failed for {}: {e}", id.0))?;
        Ok(())
    }

    async fn collateral(&self) -> Result<Decimal> {
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Collateral;
        let resp = self.clob.balance_allowance(req).await
            .map_err(|e| anyhow::anyhow!("balance_allowance failed: {e}"))?;
        let raw = Decimal::from_str(&resp.balance.to_string()).unwrap_or(Decimal::ZERO);
        Ok(raw / Decimal::from(1_000_000u32))
    }

    async fn positions(&self) -> Result<Vec<Position>> {
        let tokens: Vec<MarketId> = self.active_tokens.lock().await.iter().cloned().collect();
        let mut result = Vec::with_capacity(tokens.len());
        for token in tokens {
            let token_u256 = match u256_from_market_id(&token) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let mut req = BalanceAllowanceRequest::default();
            req.asset_type = AssetType::Conditional;
            req.token_id = Some(token_u256);
            match self.clob.balance_allowance(req).await {
                Ok(resp) => {
                    let bal = Decimal::from_str(&resp.balance.to_string())
                        .unwrap_or(Decimal::ZERO)
                        / dec!(1_000_000);
                    if bal >= config::MIN_ORDER_SHARES {
                        result.push(Position {
                            market: token,
                            shares: bal,
                            avg_price: Decimal::ZERO, // cost basis tracked in PositionMap, not queried here
                        });
                    }
                }
                Err(_) => continue,
            }
        }
        Ok(result)
    }

    async fn open_orders(&self) -> Result<Vec<OpenOrder>> {
        // The CLOB `orders()` endpoint returns only resting/working orders for the
        // given token. Every item returned is by definition still resting, so we
        // map it with remaining_qty = 1 (> 0) to satisfy `is_resting() && remaining > 0`.
        // The shared lifecycle uses this to extend the `resting_tokens` set — it does
        // not need accurate qty/price from the venue response, only market identity.
        let tokens: Vec<MarketId> = self.active_tokens.lock().await.iter().cloned().collect();
        let mut result = Vec::new();
        for token in tokens {
            let token_u256 = match u256_from_market_id(&token) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let req = OrdersRequest::builder().asset_id(token_u256).build();
            match self.clob.orders(&req, None).await {
                Ok(page) => {
                    for o in page.data {
                        result.push(OpenOrder {
                            order_id: OrderId(o.id),
                            market: token.clone(),
                            side: Side::Buy,           // intl lifecycle only tracks GTC buy bids
                            price: Decimal::ZERO,      // not consumed by lifecycle reconcile
                            original_qty: Decimal::ONE, // CLOB only lists resting orders → qty > 0
                            filled_qty: Decimal::ZERO,
                            tif: TimeInForce::Gtc,
                            pair_market: None,          // pair linkage kept in TrackedLeg
                        });
                    }
                }
                Err(_) => continue,
            }
        }
        Ok(result)
    }
}
#[cfg(test)]
mod tests {
    use super::taker_fee;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::str::FromStr;

    /// Ground truth for the fee model: every leg of the 2026-08-12 BTC session,
    /// with the cash movement each one actually produced. The observed figures
    /// are collateral deltas straight out of `pnl_snapshots` in
    /// `logs/btc-dradis.db` — that session ran a single squadron (btc-hourly),
    /// so no other order could contaminate them.
    ///
    /// This is what pins the rate at 7% against the 1000 bps the venue's
    /// `fee-rate-bps` endpoint advertises. Booking the advertised ceiling would
    /// overstate every fee by ~1.8×.
    #[test]
    fn fee_matches_observed_intl_fills() {
        let rate = crate::config::INTL_TAKER_FEE_RATE;
        // (shares, fill price, is_buy, observed collateral delta)
        let legs = [
            (dec!(19.20), dec!(0.15), true,  Decimal::from_str("3.051360").unwrap()),
            (dec!(19.20), dec!(0.06), false, Decimal::from_str("1.076200").unwrap()),
            (Decimal::from_str("15.833332").unwrap(), dec!(0.18), true,
                Decimal::from_str("3.013579").unwrap()),
            (Decimal::from_str("15.833332").unwrap(), dec!(0.18), false,
                Decimal::from_str("2.685850").unwrap()),
        ];
        // A cent of tolerance: the venue rounds, and the recorded quote is the
        // touch rather than the exact matched price on a swept book.
        let tol = dec!(0.001);
        for (shares, price, is_buy, observed) in legs {
            let notional = shares * price;
            let fee = taker_fee(rate, price, shares);
            let cash = if is_buy { notional + fee } else { notional - fee };
            let diff = (cash - observed).abs();
            assert!(
                diff < tol,
                "leg {shares} @ {price} (buy={is_buy}): modeled cash {cash} vs observed {observed} (diff {diff})"
            );
        }
    }

    /// The winning round trip of that session, end to end: entry and exit fees
    /// together must reproduce the +$0.7465 the wallet actually moved, not the
    /// +$1.0920 that was booked when fees were ignored.
    #[test]
    fn round_trip_reproduces_observed_collateral_move() {
        let rate = crate::config::INTL_TAKER_FEE_RATE;
        let (shares, entry_px, exit_px) = (dec!(13.65), dec!(0.20), dec!(0.28));
        let paid = shares * entry_px + taker_fee(rate, entry_px, shares);
        let recv = shares * exit_px - taker_fee(rate, exit_px, shares);
        let net = recv - paid;
        let observed = Decimal::from_str("0.746500").unwrap();
        assert!((net - observed).abs() < dec!(0.001), "net {net} vs observed {observed}");
        // And it must be materially worse than the fee-free figure that was booked.
        let gross = (exit_px - entry_px) * shares;
        assert!(gross - net > dec!(0.34), "fees must account for the booking gap");
    }

    /// Makers pay nothing, and a degenerate price can never produce a charge.
    #[test]
    fn fee_is_zero_outside_a_live_binary_price() {
        let rate = dec!(0.07);
        assert_eq!(taker_fee(rate, dec!(0), dec!(10)), Decimal::ZERO);
        assert_eq!(taker_fee(rate, dec!(1), dec!(10)), Decimal::ZERO);
        assert_eq!(taker_fee(rate, dec!(0.5), dec!(0)), Decimal::ZERO);
        // Peaks at the coin flip, collapses toward either tail.
        assert!(taker_fee(rate, dec!(0.50), dec!(10)) > taker_fee(rate, dec!(0.10), dec!(10)));
        assert!(taker_fee(rate, dec!(0.50), dec!(10)) > taker_fee(rate, dec!(0.90), dec!(10)));
    }
}
