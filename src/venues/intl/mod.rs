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

pub mod data_api;
pub mod orders;
pub mod balance;
pub mod wallet;
pub mod trading_client;
pub mod order_journal;
pub mod relayer;
pub mod wallet_executor;
pub mod protocol;

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
use tracing::{debug, info};

use alloy::primitives::{address, Address, U256};
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;

use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config};
use polymarket_client_sdk_v2::clob::types::Side as ClobSide;
use polymarket_client_sdk_v2::clob::types::request::{BalanceAllowanceRequest, OrdersRequest};
use polymarket_client_sdk_v2::clob::types::AssetType;
use polymarket_client_sdk_v2::{POLYGON, PRIVATE_KEY_VAR};
use wallet::{WalletConfig, WalletType};
use trading_client::TradingClient;
use std::sync::RwLock;

use tokio::sync::Mutex;

use crate::config;
use crate::helpers::nonce::fetch_next_nonce;
use crate::venues::core::{
    Execution, Fill, MarketFacts, MarketId, OpenOrder, OrderId, OrderIntent, Position, Side,
    TimeInForce,
};
use polymarket_client_sdk_v2::clob::types::request::PriceRequest;

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

/// Shares the exchange reports as matched immediately, in the order's own
/// orientation: a SELL gives up `making` shares, a BUY receives `taking`. Zero
/// when nothing matched (a resting order, or a FAK that found no liquidity).
fn matched_shares(side: Side, making: Decimal, taking: Decimal) -> Decimal {
    if making > Decimal::ZERO && taking > Decimal::ZERO {
        match side {
            Side::Sell => making,
            Side::Buy  => taking,
        }
    } else {
        Decimal::ZERO
    }
}

/// The size to report as filled: what the exchange matched, when it matched.
///
/// It used to echo the requested quantity, which is not what trades. A BUY is
/// signed for whole shares, so a request of 6.3796 buys 6; and a marketable BUY
/// spends a fixed amount of USDC, so a better fill price buys MORE than asked
/// (2026-10-06: 5.0 requested, 5.0684 received). Callers book this figure, so
/// the lifecycle's naked-leg flatten booked the requested size of a sale.
///
/// A resting order (GTC, GTD) that matched nothing keeps the requested size,
/// unchanged from before: it has not filled yet and reports what it was placed
/// for, and callers that care treat that case through their own fallbacks.
///
/// An immediate order (FAK, FOK) that matched nothing is gone, so it reports
/// zero. Echoing the requested size there told the Trade Log's Close Now that
/// an unfilled fill-or-kill had sold the whole position, and the row was closed
/// while the shares were still held ([B73], 2026-10-26). The shared lifecycle's
/// flatten books nothing on a zero and keeps the leg for the next reconcile
/// (`flatten_booking`); its re-hedge falls through to the flatten instead of
/// guarding a leg that was never bought.
fn reported_filled(matched: Decimal, _requested: Decimal, _tif: crate::venues::core::TimeInForce) -> Decimal {
    matched
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

fn map_order_state(order: &polymarket_client_sdk_v2::clob::types::response::OpenOrderResponse)
    -> crate::venues::core::OrderState {
    use polymarket_client_sdk_v2::clob::types::OrderStatusType;
    let resting = match &order.status {
        OrderStatusType::Live => Some(true),
        OrderStatusType::Matched | OrderStatusType::Canceled => Some(false),
        _ => None,
    };
    crate::venues::core::OrderState { filled: Some(order.size_matched), resting }
}

fn map_open_order(order: polymarket_client_sdk_v2::clob::types::response::OpenOrderResponse) -> Result<OpenOrder> {
    use polymarket_client_sdk_v2::clob::types::OrderType;
    anyhow::ensure!(order.original_size > Decimal::ZERO && order.size_matched >= Decimal::ZERO
        && order.size_matched <= order.original_size, "CLOB returned inconsistent order quantities");
    let side = match order.side { ClobSide::Buy => Side::Buy, ClobSide::Sell => Side::Sell,
        _ => anyhow::bail!("CLOB returned an unknown order side") };
    let tif = match order.order_type {
        OrderType::GTC => TimeInForce::Gtc, OrderType::GTD => TimeInForce::Gtd,
        OrderType::FAK => TimeInForce::Fak, OrderType::FOK => TimeInForce::Fok,
        _ => anyhow::bail!("CLOB returned an unknown order type"),
    };
    Ok(OpenOrder { order_id: OrderId(order.id), market: market_id_from_u256(order.asset_id),
        side, price: order.price, original_qty: order.original_size, filled_qty: order.size_matched,
        tif, pair_market: None })
}

type ReadinessProbe = dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<String>> + Send>> + Send + Sync;

/// The international (self-custody) Polymarket CLOB venue.
pub struct IntlClobVenue {
    /// Authenticated CLOB REST client used for all order/balance operations.
    clob: Arc<TradingClient>,
    /// EOA signing key for EIP-712 order signatures.
    signer: LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    /// Session-scoped nonce manager (AtomicU64, retained for API compatibility).
    nonce: Arc<AtomicU64>,
    /// Shared HTTP client for nonce re-sync and order placement.
    http: Arc<reqwest::Client>,
    /// Immutable signer/funder/executor identity selected at connection time.
    wallet: WalletConfig,
    live_blockers: RwLock<Vec<String>>,
    readiness_probe: RwLock<Option<Arc<ReadinessProbe>>>,
    readiness_lock: Mutex<()>,
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
    /// Resolve the signer/funder identity and authenticate the CLOB client.
    /// Poly1271 derives existing credentials for balance-only operation.
    ///
    /// Encapsulates the signer/nonce bootstrap that previously lived inline in
    /// `main.rs`. The Polygon settlement `Provider` is intentionally *not* owned
    /// here — it is a separate on-chain subsystem, generic over the patrol loop.
    pub async fn connect(http: Arc<reqwest::Client>) -> Result<Self> {
        Self::connect_existing(http, false).await
    }

    /// Connect a diagnostic reader using only existing CLOB credentials.
    /// This never attempts API-key creation, including for a legacy Safe.
    pub async fn connect_read_only(http: Arc<reqwest::Client>) -> Result<Self> {
        Self::connect_existing(http, true).await
    }

    async fn connect_existing(http: Arc<reqwest::Client>, existing_only: bool) -> Result<Self> {
        let private_key = std::env::var(PRIVATE_KEY_VAR)
            .context("POLYMARKET_PRIVATE_KEY not set")?;

        let signer = LocalSigner::from_str(private_key.trim())
            .map_err(|_| anyhow::anyhow!("wallet.signer: invalid POLYMARKET_PRIVATE_KEY; check managed secrets override"))?
            .with_chain_id(Some(POLYGON));
        let eoa_address = signer.address();
        let wallet = WalletConfig::from_env(eoa_address).context("wallet.configuration")?;
        info!(wallet_type = ?wallet.wallet_type, signer = %eoa_address, funder = %wallet.funder,
            "CLOB wallet identity resolved");

        let client = ClobClient::new(config::CLOB_API_BASE, Config::default())
            .map_err(|e| balance::sdk_error("clob.client", e))?;
        // Existing Deposit Wallet accounts derive their credentials without
        // creating new API keys during connection.
        let credentials = if existing_only || wallet.wallet_type == WalletType::Poly1271 {
            Some(tokio::time::timeout(std::time::Duration::from_secs(15),
                client.derive_api_key(&signer, None)).await
                .context("clob.derive_api_key: timeout (15s)")?
                .map_err(|e| balance::sdk_error("clob.derive_api_key", e))?)
        } else {
            None
        };
        let mut auth = client.authentication_builder(&signer)
            .funder(wallet.funder).signature_type(wallet.signature_type());
        if let Some(credentials) = credentials {
            auth = auth.credentials(credentials);
        }
        let raw_client = tokio::time::timeout(std::time::Duration::from_secs(15), auth.authenticate())
            .await.context("clob.authenticate: timeout (15s)")?
            .map_err(|e| balance::sdk_error("clob.authenticate", e))?;
        let state_dir = std::path::PathBuf::from(std::env::var("DRADIS_DATA_DIR").unwrap_or_else(|_| "data".into()));
        let clob = Arc::new(TradingClient::from_authenticated(raw_client, wallet, state_dir).await?);
        info!(wallet_type = ?wallet.wallet_type, funder = %wallet.funder, "CLOB authenticated");

        // V2 has no order nonce. Retain legacy bookkeeping only for Safe callers.
        let initial_nonce = if wallet.wallet_type == WalletType::GnosisSafe {
            fetch_next_nonce(&http, wallet.funder).await.unwrap_or(0)
        } else { 0 };
        let nonce = Arc::new(AtomicU64::new(initial_nonce));

        Ok(Self { clob, signer, nonce, http, wallet, live_blockers: RwLock::new(vec!["Execution readiness checks have not completed".into()]), readiness_probe: RwLock::new(None), readiness_lock: Mutex::new(()), eoa_address,
                   active_tokens: Arc::new(Mutex::new(HashSet::new())) })
    }

    #[cfg(test)]
    pub(crate) async fn test_connection(host: &str) -> Self {
        use polymarket_client_sdk_v2::auth::Credentials;
        let signer = LocalSigner::from_slice(&[1u8; 32]).unwrap().with_chain_id(Some(POLYGON));
        let eoa_address = signer.address();
        let wallet = WalletConfig::resolve(eoa_address, Some("poly1271"),
            Some("0x1111111111111111111111111111111111111111")).unwrap();
        let clob = ClobClient::new(host, Config::default()).unwrap()
            .authentication_builder(&signer)
            .credentials(Credentials::new(Default::default(), "c3ludGhldGlj".into(), "offline".into()))
            .funder(wallet.funder).signature_type(wallet.signature_type()).authenticate().await.unwrap();
        let clob = TradingClient::for_test(clob, wallet,
            Arc::new(std::sync::atomic::AtomicBool::new(false))).await.unwrap();
        Self { clob: Arc::new(clob), signer, nonce: Arc::new(AtomicU64::new(0)),
            http: Arc::new(reqwest::Client::new()), wallet, live_blockers: RwLock::new(vec!["Synthetic fixture blocks execution".into()]), readiness_probe: RwLock::new(None), readiness_lock: Mutex::new(()), eoa_address,
            active_tokens: Arc::new(Mutex::new(HashSet::new())) }
    }

    // ── Accessors (raw infra for call sites not yet on the Execution trait) ──

    /// Authenticated CLOB client (fee-rate / neg-risk / balance / cancel queries).
    pub fn trading_client(&self) -> &Arc<TradingClient> {
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

    pub fn wallet(&self) -> WalletConfig {
        self.wallet
    }

    pub fn trading_address(&self) -> Address {
        self.wallet.funder
    }

    pub fn is_balance_only(&self) -> bool {
        self.wallet.wallet_type == WalletType::Poly1271 && !self.live_blockers().is_empty()
    }

    pub fn live_blockers(&self) -> Vec<String> {
        self.live_blockers.read().expect("wallet readiness lock poisoned").clone()
    }

    pub fn set_readiness_probe(&self, probe: Arc<ReadinessProbe>) {
        *self.readiness_probe.write().expect("wallet readiness probe lock poisoned") = Some(probe);
    }

    pub async fn refresh_live_readiness(&self) -> Vec<String> {
        let _guard = self.readiness_lock.lock().await;
        let probe = self.readiness_probe.read().expect("wallet readiness probe lock poisoned").clone();
        if let Some(probe) = probe {
            let blockers = tokio::time::timeout(std::time::Duration::from_secs(8), probe()).await
                .unwrap_or_else(|_| vec!["Wallet readiness verification timed out".to_owned()]);
            self.set_live_blockers(blockers);
        }
        self.live_blockers()
    }

    pub fn set_live_blockers(&self, blockers: Vec<String>) {
        self.clob.set_execution_ready(blockers.is_empty());
        *self.live_blockers.write().expect("wallet readiness lock poisoned") = blockers;
    }

    /// Compatibility accessor: the trading funder; use wallet().on_chain_safe()
    /// when selecting an on-chain executor.
    pub fn safe_address(&self) -> Address {
        self.trading_address()
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
        self.clob.require_mutation_allowed()?;
        let vc = Self::verifying_contract(intent.is_neg_risk);

        let (order_id, making_amount, taking_amount) = orders::place_limit_order_filled(
            &self.clob,
            &self.nonce,
            &self.signer,
            self.trading_address(),
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
        let matched_shares = matched_shares(intent.side, making_amount, taking_amount);
        let fee = taker_fee(live_taker_fee_rate(), fill_price, matched_shares);

        Ok(Fill {
            order_id: OrderId(order_id),
            market: intent.market,
            filled: reported_filled(matched_shares, intent.quantity, intent.tif),
            price: fill_price, fee
        })
    }

    async fn place_atomic(&self, legs: [OrderIntent; 2]) -> Result<[Fill; 2]> {
        self.clob.require_mutation_allowed()?;
        let [a, b] = legs;

        let receipts = orders::place_limit_orders_atomic_filled(
            &self.clob,
            &self.nonce,
            &self.signer,
            self.trading_address(),
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

        let fill = |intent: OrderIntent, receipt: orders::OrderReceipt| {
            let matched = matched_shares(intent.side, receipt.making_amount, receipt.taking_amount);
            let price = if receipt.making_amount > Decimal::ZERO && receipt.taking_amount > Decimal::ZERO {
                let price = match intent.side {
                    Side::Sell => receipt.taking_amount / receipt.making_amount,
                    Side::Buy => receipt.making_amount / receipt.taking_amount,
                };
                if price > Decimal::ZERO && price <= Decimal::ONE { price } else { intent.price }
            } else { intent.price };
            Fill { order_id: OrderId(receipt.order_id), market: intent.market, filled: matched,
                price, fee: taker_fee(live_taker_fee_rate(), price, matched) }
        };
        let [receipt_a, receipt_b] = receipts;
        Ok([fill(a, receipt_a), fill(b, receipt_b)])
    }

    async fn acknowledge_order_adoption(&self, id: &OrderId) -> Result<()> {
        self.clob.acknowledge_placement(&id.0).await
    }

    async fn cancel(&self, id: OrderId) -> Result<()> {
        let response = self.clob.cancel_orders(&[id.0.as_str()]).await?;
        anyhow::ensure!(response.canceled.iter().any(|canceled| canceled == &id.0)
            && !response.not_canceled.contains_key(&id.0), "CLOB did not confirm requested cancellation");
        Ok(())
    }

    async fn collateral(&self) -> Result<Decimal> {
        self.clob.collateral().await
    }

    async fn positions(&self) -> Result<Vec<Position>> {
        let tokens: Vec<MarketId> = self.active_tokens.lock().await.iter().cloned().collect();
        let mut result = Vec::with_capacity(tokens.len());
        for token in tokens {
            let mut request = BalanceAllowanceRequest::default();
            request.asset_type = AssetType::Conditional;
            request.token_id = Some(u256_from_market_id(&token)?);
            request.signature_type = Some(self.wallet.signature_type());
            let response = tokio::time::timeout(std::time::Duration::from_secs(10),
                self.clob.balance_allowance(request)).await.context("CLOB positions timeout")?
                .map_err(|error| balance::sdk_error("positions", error))?;
            let shares = response.balance / dec!(1_000_000);
            if shares > Decimal::ZERO {
                result.push(Position { market: token, shares, avg_price: Decimal::ZERO });
            }
        }
        Ok(result)
    }

    async fn open_orders(&self) -> Result<Vec<OpenOrder>> {
        let request = OrdersRequest::default();
        let mut cursor = None;
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for _ in 0..100 {
            let page = tokio::time::timeout(std::time::Duration::from_secs(10),
                self.clob.orders(&request, cursor.clone())).await.context("CLOB open orders timeout")?
                .map_err(|error| balance::sdk_error("open_orders", error))?;
            for order in page.data {
                anyhow::ensure!(order.maker_address == self.wallet.funder,
                    "CLOB returned an order belonging to a different trading funder");
                result.push(map_open_order(order)?);
            }
            if page.next_cursor.is_empty() || page.next_cursor == "LTE=" { return Ok(result); }
            anyhow::ensure!(seen.insert(page.next_cursor.clone()), "CLOB order pagination repeated a cursor");
            cursor = Some(page.next_cursor);
        }
        anyhow::bail!("CLOB order pagination exceeded the bounded page limit")
    }

    async fn order_state(&self, id: &OrderId) -> Result<Option<crate::venues::core::OrderState>> {
        let order = tokio::time::timeout(std::time::Duration::from_secs(10), self.clob.order(&id.0))
            .await.context("CLOB order state timeout")?
            .map_err(|error| balance::sdk_error("order_state", error))?;
        anyhow::ensure!(order.maker_address == self.wallet.funder && order.id == id.0,
            "CLOB order identity does not match the requested wallet/order");
        Ok(Some(map_order_state(&order)))
    }

    /// Best bid on `market`'s book.
    ///
    /// Lifted from the Trade Log's old intl-only quote helper
    /// (`api::server::fetch_side_price`, removed in 1.5.1), which took a whole
    /// `SessionState` only to reach `session.venue.trading_client()`. Helm's
    /// posture validator needs both ends of the book and had been reaching into
    /// `venues::intl` directly to get them, which is what kept the Helm API
    /// compiled for one venue.
    ///
    /// A zero is not a bid. The venue returns zero rather than an error when a
    /// side is empty, and passing it through would let a stop validate against a
    /// price nobody is offering.
    async fn best_bid(&self, market: &MarketId) -> Result<Option<Decimal>> {
        Ok(self.quote(market, ClobSide::Buy).await)
    }

    /// Both ends of the book in one call, for a validator rather than a trader.
    ///
    /// Separate from `best_ask` on purpose: see the note above. This is also one
    /// round trip's worth of latency cheaper than two sequential calls, which
    /// matters because an operator is waiting on it.
    async fn quote_both(&self, market: &MarketId) -> Result<(Option<Decimal>, Option<Decimal>)> {
        let (bid, ask) = tokio::join!(
            self.quote(market, ClobSide::Buy),
            self.quote(market, ClobSide::Sell),
        );
        Ok((bid, ask))
    }

    // `best_ask` is deliberately NOT implemented here, so this venue keeps the
    // trait's `None` default.
    //
    // It is not a missing feature. `venues::lifecycle.rs` gates a FAK BUY of a
    // missing partner leg on `best_ask` returning `Some`, and intl already
    // re-hedges naked legs in its own ARB ARBITER sweep
    // (`squadron/patrol_tasks.rs`, every 300s). Implementing it here would arm a
    // SECOND re-hedge path on the live wallet, so two tasks could buy the same
    // partner leg. The shared lifecycle's version exists for the venues that have
    // no arbiter of their own.
    //
    // Helm does not need it: its posture validator wants the bid it would sell
    // into and the ask it would lift, and reads both through `quote_both` below,
    // which no order-placing path consults.

    /// The market's tokens, question and close, from Gamma.
    ///
    /// A binary market here really is a yes/no question, so `leg_labels` is
    /// `None`. `market` is the condition id, which is how this venue names a
    /// market everywhere outside `src/venues/`.
    async fn market_facts(&self, market: &MarketId) -> Result<Option<MarketFacts>> {
        let Some(info) =
            crate::cag::adama::fetch_market_info(self.shared_http(), market.as_str()).await
        else {
            return Ok(None);
        };
        Ok(Some(MarketFacts {
            market_id: market.clone(),
            question: info.question,
            criteria: info.description,
            yes_token: MarketId::new(info.yes_token),
            no_token: MarketId::new(info.no_token),
            leg_labels: None,
            close_time: info.close_time,
            accepting_orders: info.accepting_orders,
        }))
    }

    async fn order_book(&self, leg: &MarketId) -> Result<Option<crate::venues::core::LegBook>> {
        let url = format!("https://clob.polymarket.com/book?token_id={}", leg.as_str());
        let v: serde_json::Value = self.shared_http().get(&url)
            .timeout(std::time::Duration::from_secs(5)).send().await?.error_for_status()?.json().await?;
        let (bids, asks) = crate::helpers::clob_public::parse_book(&v);
        Ok(Some(crate::venues::core::LegBook::from_levels(bids, asks)))
    }

    async fn recent_prints(&self, market: &MarketId, limit: usize) -> Result<Option<Vec<crate::venues::core::TapePrint>>> {
        let url = format!("{}/trades", crate::helpers::clob_public::DATA_API_V2);
        let v: serde_json::Value = self.shared_http().get(&url)
            .query(&[("condition", market.as_str().to_string()), ("limit", limit.clamp(1, 500).to_string())])
            .timeout(std::time::Duration::from_secs(5)).send().await?.error_for_status()?.json().await?;
        let mut prints: Vec<crate::venues::core::TapePrint> = crate::helpers::clob_public::parse_trades(&v)
            .into_iter()
            .filter_map(|p| Some(crate::venues::core::TapePrint {
                at: chrono::DateTime::from_timestamp(p.ts, 0)?,
                taker_side: if p.side.eq_ignore_ascii_case("BUY") { crate::venues::core::Side::Buy } else { crate::venues::core::Side::Sell },
                leg_is_yes: p.outcome_index == 0,
                price: crate::helpers::clob_public::dec_from_f64(p.price)?,
                size: crate::helpers::clob_public::dec_from_f64(p.size)?,
            }))
            .collect();
        prints.sort_by(|a, b| b.at.cmp(&a.at));
        Ok(Some(prints))
    }

    async fn price_history(
        &self,
        leg: &MarketId,
        since: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Vec<crate::venues::core::PricePoint>>> {
        let url = format!(
            "https://clob.polymarket.com/prices-history?market={}&startTs={}&endTs={}&fidelity=1",
            leg.as_str(), since.timestamp(), chrono::Utc::now().timestamp(),
        );
        let v: serde_json::Value = self.shared_http().get(&url)
            .timeout(std::time::Duration::from_secs(5)).send().await?.error_for_status()?.json().await?;
        let pts = crate::helpers::clob_public::parse_price_history(&v)
            .into_iter()
            .filter_map(|(t, p)| Some(crate::venues::core::PricePoint {
                at: chrono::DateTime::from_timestamp(t, 0)?,
                price: crate::helpers::clob_public::dec_from_f64(p)?,
            }))
            .collect();
        Ok(Some(pts))
    }

    /// The CLOB's neg-risk flag for the token, which selects the exchange an
    /// order is signed for: the same lookup every automated order path does.
    async fn neg_risk(&self, leg: &MarketId) -> Result<bool> {
        let token = u256_from_market_id(leg)?;
        let r = self.trading_client().neg_risk(token).await
            .map_err(|e| anyhow::anyhow!("neg_risk lookup for {}: {e}", leg.as_str()))?;
        Ok(r.neg_risk)
    }

    /// Gamma's final price for the leg, through the same reader the settlement
    /// sweep trusts: `Unknown` while UMA is pending or the price is indecisive.
    async fn resolution(&self, leg: &MarketId) -> Result<crate::venues::core::TokenResolution> {
        Ok(crate::helpers::market::resolution_for_token(self.shared_http(), leg.as_str()).await)
    }
}

impl IntlClobVenue {
    /// One side of a token's book, or `None` when the venue cannot price it.
    ///
    /// Bounded at five seconds: a quote that has not arrived by then is no use
    /// to a validator the operator is waiting on, and the caller treats absence
    /// as a refusal rather than a zero.
    async fn quote(&self, market: &MarketId, side: ClobSide) -> Option<Decimal> {
        let token = u256_from_market_id(market).ok()?;
        let req = PriceRequest::builder().token_id(token).side(side).build();
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.trading_client().price(&req),
        )
        .await
        {
            Ok(Ok(r)) if r.price > Decimal::ZERO => Some(r.price),
            Ok(Ok(_)) => {
                debug!("quote: venue returned a non-positive price for {market}");
                None
            }
            Ok(Err(e)) => {
                debug!("quote: price fetch failed for {market}: {e}");
                None
            }
            Err(_) => {
                debug!("quote: price fetch timed out for {market}");
                None
            }
        }
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

#[cfg(test)]
mod reported_fill_tests {
    use super::{matched_shares, reported_filled};
    use crate::venues::core::{Side, TimeInForce};
    use rust_decimal_macros::dec;

    /// 2026-10-06: a BUY requested at 5.0 shares received 5.0684, because a
    /// marketable buy spends fixed USDC and the price improved. Report the
    /// shares received, not the shares asked for.
    #[test]
    fn a_price_improved_buy_reports_the_shares_received() {
        // BUY orientation: making = USDC paid, taking = shares received.
        let m = matched_shares(Side::Buy, dec!(3.65), dec!(5.0684));
        assert_eq!(m, dec!(5.0684));
        assert_eq!(reported_filled(m, dec!(5.0), TimeInForce::Fak), dec!(5.0684));
    }

    /// A request of 6.3796 is signed for whole shares, so 6 trade.
    #[test]
    fn a_whole_share_buy_reports_six_not_the_fraction_requested() {
        let m = matched_shares(Side::Buy, dec!(3.42), dec!(6));
        assert_eq!(reported_filled(m, dec!(6.3796), TimeInForce::Fak), dec!(6));
    }

    /// SELL orientation: making = shares given up. A stop that sold 5.06 of a
    /// 5.0684 holding (sells floor to two decimals) reports 5.06.
    #[test]
    fn a_sell_reports_the_shares_given_up() {
        let m = matched_shares(Side::Sell, dec!(5.06), dec!(3.0866));
        assert_eq!(reported_filled(m, dec!(5.0684), TimeInForce::Fak), dec!(5.06));
    }

    /// An acknowledged resting order is not a fill. Its working size is tracked
    /// separately until cumulative venue fills arrive.
    #[test]
    fn an_unmatched_resting_order_reports_zero_until_reconciled() {
        let m = matched_shares(Side::Buy, dec!(0), dec!(0));
        assert_eq!(m, dec!(0));
        assert_eq!(reported_filled(m, dec!(5.0), TimeInForce::Gtc), dec!(0));
        assert_eq!(reported_filled(m, dec!(5.0), TimeInForce::Gtd), dec!(0));
        assert_eq!(matched_shares(Side::Sell, dec!(5), dec!(0)), dec!(0), "half a pair is not a match");
    }

    /// Nothing matched on an IMMEDIATE order: it is gone, so it reports zero.
    /// Echoing the requested size here booked an unfilled fill-or-kill as a
    /// full close of a position still held ([B73]).
    #[test]
    fn an_unmatched_immediate_order_reports_zero() {
        let m = matched_shares(Side::Sell, dec!(0), dec!(0));
        assert_eq!(reported_filled(m, dec!(5.0), TimeInForce::Fak), dec!(0));
        assert_eq!(reported_filled(m, dec!(5.0), TimeInForce::Fok), dec!(0));
    }
}

#[cfg(test)]
mod order_read_tests {
    use super::*;
    use serde_json::{json, Value};
    use polymarket_client_sdk_v2::clob::types::{OrderStatusType, response::OpenOrderResponse};

    fn order_value(id: &str) -> Value {
        json!({"id":id,"status":"LIVE","owner":"00000000-0000-0000-0000-000000000000",
            "maker_address":"0x1111111111111111111111111111111111111111",
            "market":format!("0x{}", "11".repeat(32)),"asset_id":"42","side":"SELL",
            "original_size":"10","size_matched":"3.25","price":"0.60","associate_trades":[],
            "outcome":"Yes","created_at":0,"expiration":"1000000000","order_type":"GTD"})
    }

    #[test]
    fn order_mapping_keeps_actual_side_size_fills_price_and_time_in_force() {
        let order: OpenOrderResponse = serde_json::from_value(order_value("a")).unwrap();
        let state = map_order_state(&order);
        assert_eq!(state.filled, Some(dec!(3.25)));
        assert_eq!(state.resting, Some(true));
        let mapped = map_open_order(order).unwrap();
        assert_eq!(mapped.side, Side::Sell);
        assert_eq!(mapped.original_qty, dec!(10));
        assert_eq!(mapped.filled_qty, dec!(3.25));
        assert_eq!(mapped.remaining_qty(), dec!(6.75));
        assert_eq!(mapped.price, dec!(0.60));
        assert_eq!(mapped.tif, TimeInForce::Gtd);
    }

    #[test]
    fn uncertain_order_status_is_not_a_terminal_order() {
        let mut order: OpenOrderResponse = serde_json::from_value(order_value("a")).unwrap();
        order.status = OrderStatusType::Unknown("not-understood".into());
        assert_eq!(map_order_state(&order).resting, None);
        order.status = OrderStatusType::Canceled;
        assert_eq!(map_order_state(&order).resting, Some(false));
        order.size_matched = dec!(11);
        assert!(map_open_order(order).is_err());
    }

    #[tokio::test]
    async fn open_orders_follow_pages_and_single_order_lookup_preserves_cumulative_fill() {
        use axum::{routing::get, Router, Json, http::Uri};
        use crate::venues::core::Execution;
        let app = Router::new()
            .route("/data/orders", get(|uri: Uri| async move {
                let second = uri.query().unwrap_or_default().contains("cursor-two");
                Json(json!({"data":[order_value(if second {"b"} else {"a"})],
                    "next_cursor":if second {"LTE="} else {"cursor-two"}, "limit":1,"count":1}))
            }))
            .route("/data/order/a", get(|| async { Json(order_value("a")) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        let venue = IntlClobVenue::test_connection(&host).await;
        let orders = venue.open_orders().await.unwrap();
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[1].order_id.0, "b");
        let state = venue.order_state(&OrderId("a".into())).await.unwrap().unwrap();
        assert_eq!(state.filled, Some(dec!(3.25)));
        assert_eq!(state.resting, Some(true));
        server.abort();
    }
}
