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
}/// The international (self-custody) Polymarket CLOB venue.
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
    /// client, derive the Safe/EOA addresses, and initialise the nonce from the
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

        Ok(Fill {
            order_id: OrderId(order_id),
            market: intent.market,
            filled: intent.quantity,
            price: fill_price,
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
            Fill { order_id: OrderId(id_a), market: a.market, filled: a.quantity, price: a.price },
            Fill { order_id: OrderId(id_b), market: b.market, filled: b.quantity, price: b.price },
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