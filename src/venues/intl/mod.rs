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

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::str::FromStr;
use tracing::info;

use alloy::primitives::{address, Address, U256};
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;

use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config};
use polymarket_client_sdk_v2::clob::types::{OrderType as ClobOrderType, Side as ClobSide, SignatureType};
use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk_v2::clob::types::AssetType;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::Normal;
use polymarket_client_sdk_v2::{POLYGON, PRIVATE_KEY_VAR, derive_safe_wallet};

use crate::config;
use crate::helpers::nonce::fetch_next_nonce;
use crate::venues::core::{
    Execution, Fill, MarketId, OrderId, OrderIntent, Position, Side, TimeInForce,
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

        Ok(Self { clob, signer, nonce, http, safe_address, eoa_address })
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

    fn map_tif(tif: TimeInForce) -> ClobOrderType {
        match tif {
            TimeInForce::Gtc => ClobOrderType::GTC,
            TimeInForce::Gtd => ClobOrderType::GTD,
            TimeInForce::Fak => ClobOrderType::FAK,
            TimeInForce::Fok => ClobOrderType::FOK,
        }
    }
}

#[async_trait]
impl Execution for IntlClobVenue {
    async fn place_order(&self, intent: OrderIntent) -> Result<Fill> {
        let vc = Self::verifying_contract(intent.is_neg_risk);

        let order_id = orders::place_limit_order(
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
            Self::map_tif(intent.tif),
            intent.post_only,
            intent.expiration_secs,
            &self.http,
        )
        .await?;

        Ok(Fill {
            order_id: OrderId(order_id),
            market: intent.market,
            filled: intent.quantity,
            price: intent.price,
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
            Self::map_tif(a.tif),
            a.post_only,
            a.expiration_secs,
            Self::verifying_contract(b.is_neg_risk),
            &b.market,
            Self::map_side(b.side),
            b.quantity,
            b.price,
            Self::map_tif(b.tif),
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

    async fn cancel(&self, _id: OrderId) -> Result<()> {
        // Single-order cancel is wired in a later venue-abstraction step; existing
        // call paths use the client's `cancel_all_orders` directly for now.
        anyhow::bail!("IntlClobVenue::cancel: single-order cancel not yet wired")
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
        // Position reconciliation flows through the existing chain-sync path;
        // a direct venue query is wired in a later venue-abstraction step.
        Ok(Vec::new())
    }
}

