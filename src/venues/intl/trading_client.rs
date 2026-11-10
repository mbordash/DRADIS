//! Session identity and the sole CLOB mutation boundary.
use super::{
    order_journal::{JournalOrder, OrderJournal, TrackedOrder},
    wallet::{WalletConfig, WalletType},
};
use alloy::{
    primitives::{Address, U256},
    signers::Signer,
    sol_types::SolStruct,
};
use anyhow::{Context, Result};
use polymarket_client_sdk_v2 as sdk;
use rust_decimal::Decimal;
use sdk::{
    auth::{state::Authenticated, Credentials, Normal},
    clob::{
        types::{request::*, response::*, *},
        Client,
    },
};
use std::{
    borrow::Cow,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

pub struct RecoveredOrder {
    pub record: JournalOrder,
    pub order: OpenOrderResponse,
}

pub struct TradingClient {
    client: Client<Authenticated<Normal>>,
    wallet: WalletConfig,
    execution_ready: AtomicBool,
    journal: OrderJournal,
    token_registry: super::protocol::CtfTokenRegistry,
    #[cfg(test)]
    test_gate: Option<Arc<AtomicBool>>,
}

impl TradingClient {
    pub async fn from_authenticated(
        client: Client<Authenticated<Normal>>,
        wallet: WalletConfig,
        state_dir: PathBuf,
    ) -> Result<Self> {
        anyhow::ensure!(
            client.address() == wallet.signer,
            "CLOB authenticated signer does not match wallet"
        );
        Ok(Self {
            client,
            wallet,
            execution_ready: AtomicBool::new(wallet.wallet_type == WalletType::GnosisSafe),
            journal: OrderJournal::open(&state_dir).await?,
            token_registry: Default::default(),
            #[cfg(test)]
            test_gate: None,
        })
    }
    #[cfg(test)]
    pub(crate) async fn for_test(
        client: Client<Authenticated<Normal>>,
        wallet: WalletConfig,
        gate: Arc<AtomicBool>,
    ) -> Result<Self> {
        anyhow::ensure!(
            client.address() == wallet.signer,
            "CLOB authenticated signer does not match wallet"
        );
        Ok(Self {
            client,
            wallet,
            execution_ready: AtomicBool::new(true),
            journal: OrderJournal::memory().await?,
            token_registry: super::protocol::CtfTokenRegistry::for_tests(),
            test_gate: Some(gate),
        })
    }
    #[cfg(test)]
    pub(crate) async fn seed_accepted_for_test(&self, record: &JournalOrder) -> Result<()> {
        self.journal.prepare(&[record.clone()]).await?;
        self.journal.outcome(record, "accepted").await
    }
    pub fn wallet(&self) -> WalletConfig {
        self.wallet
    }
    pub fn set_execution_ready(&self, ready: bool) {
        self.execution_ready.store(ready, Ordering::Release);
    }
    pub fn execution_ready(&self) -> bool {
        self.execution_ready.load(Ordering::Acquire)
    }
    pub fn require_mutation_allowed(&self) -> Result<()> {
        crate::helpers::migration::refuse_if_retired()?;
        anyhow::ensure!(
            self.execution_ready(),
            "CLOB execution is not ready for this wallet"
        );
        self.require_cancel_allowed()
    }
    fn require_cancel_allowed(&self) -> Result<()> {
        anyhow::ensure!(
            !crate::helpers::dynamic_config::read_only_mode(),
            "CLOB mutations disabled by read-only runtime policy"
        );
        #[cfg(test)]
        if let Some(gate) = &self.test_gate {
            anyhow::ensure!(
                gate.load(Ordering::Acquire),
                "CLOB mutations disabled while simulating"
            );
            return Ok(());
        }
        anyhow::ensure!(
            !crate::config::GHOST_MODE,
            "CLOB mutations disabled by build-time simulation policy"
        );
        let channel = crate::helpers::dynamic_config::global_config_tx()
            .context("CLOB mutations disabled before runtime configuration is initialized")?;
        anyhow::ensure!(
            !channel.borrow().ghost_mode,
            "CLOB mutations disabled while simulating"
        );
        Ok(())
    }
    pub async fn verify_ctf_token(&self, token: U256) -> Result<()> {
        self.token_registry.verify(token).await
    }
    pub fn address(&self) -> Address {
        self.client.address()
    }
    pub fn credentials(&self) -> &Credentials {
        self.client.credentials()
    }
    pub fn set_neg_risk(&self, token: U256, value: bool) {
        self.client.set_neg_risk(token, value)
    }
    pub fn set_tick_size(&self, token: U256, value: TickSize) {
        self.client.set_tick_size(token, value)
    }
    pub async fn sign<S: Signer>(
        &self,
        signer: &S,
        order: SignableOrder,
    ) -> sdk::Result<SignedOrder> {
        self.client.sign(signer, order).await
    }
    pub async fn collateral(&self) -> Result<Decimal> {
        super::balance::read_collateral(&self.client, self.wallet.signature_type()).await
    }
    pub async fn balance_allowance(
        &self,
        r: BalanceAllowanceRequest,
    ) -> sdk::Result<BalanceAllowanceResponse> {
        self.client.balance_allowance(r).await
    }
    pub async fn update_balance_allowance(
        &self,
        r: UpdateBalanceAllowanceRequest,
    ) -> sdk::Result<()> {
        self.client.update_balance_allowance(r).await
    }
    pub async fn orders(
        &self,
        r: &OrdersRequest,
        c: Option<String>,
    ) -> sdk::Result<Page<OpenOrderResponse>> {
        self.client.orders(r, c).await
    }
    pub async fn order(&self, id: &str) -> sdk::Result<OpenOrderResponse> {
        self.client.order(id).await
    }
    pub async fn trades(
        &self,
        r: &TradesRequest,
        c: Option<String>,
    ) -> sdk::Result<Page<TradeResponse>> {
        self.client.trades(r, c).await
    }
    pub async fn neg_risk(&self, t: U256) -> sdk::Result<NegRiskResponse> {
        self.client.neg_risk(t).await
    }
    pub async fn tick_size(&self, t: U256) -> sdk::Result<TickSizeResponse> {
        self.client.tick_size(t).await
    }
    pub async fn fee_rate_bps(&self, t: U256) -> sdk::Result<FeeRateResponse> {
        self.client.fee_rate_bps(t).await
    }
    pub async fn price(&self, r: &PriceRequest) -> sdk::Result<PriceResponse> {
        self.client.price(r).await
    }
    pub async fn prices(&self, r: &[PriceRequest]) -> sdk::Result<PricesResponse> {
        self.client.prices(r).await
    }
    pub async fn order_book(
        &self,
        r: &OrderBookSummaryRequest,
    ) -> sdk::Result<OrderBookSummaryResponse> {
        self.client.order_book(r).await
    }
    pub async fn version(&self) -> sdk::Result<u32> {
        self.client.version().await
    }

    async fn identity(&self, order: &SignedOrder) -> Result<JournalOrder> {
        let payload = match &order.payload {
            OrderPayload::V2(payload) => payload,
            _ => anyhow::bail!("only V2 orders are supported"),
        };
        let value = &payload.order;
        self.verify_ctf_token(value.tokenId).await?;
        anyhow::ensure!(
            value.maker == self.wallet.funder
                && value.signer == self.wallet.order_signer()
                && value.signatureType == self.wallet.signature_type() as u8,
            "signed order identity differs from the authenticated wallet"
        );
        let neg_risk = self.client.neg_risk(value.tokenId).await?.neg_risk;
        let domain = alloy::dyn_abi::Eip712Domain {
            name: Some(Cow::Borrowed("Polymarket CTF Exchange")),
            version: Some(Cow::Borrowed("2")),
            chain_id: Some(U256::from(sdk::POLYGON)),
            verifying_contract: Some(super::exchange_verifying_contract(neg_risk)),
            ..Default::default()
        };
        let quantity = if value.side == Side::Buy as u8 {
            value.takerAmount
        } else {
            value.makerAmount
        };
        use std::str::FromStr;
        Ok(JournalOrder {
            order_hash: value.eip712_signing_hash(&domain).to_string(),
            order_id: None,
            funder: self.wallet.funder.to_string(),
            token_id: value.tokenId.to_string(),
            side: value.side,
            signed_quantity: Decimal::from_str(&quantity.to_string())?
                / Decimal::from(1_000_000u32),
            maker_amount: value.makerAmount.to_string(),
            taker_amount: value.takerAmount.to_string(),
        })
    }
    fn definite_rejection(error: &sdk::error::Error) -> bool {
        error
            .downcast_ref::<sdk::error::Status>()
            .is_some_and(|status| {
                matches!(
                    status.status_code.as_u16(),
                    400 | 401 | 403 | 404 | 422 | 425 | 429
                )
            })
    }
    async fn record_response(
        &self,
        record: &mut JournalOrder,
        response: &PostOrderResponse,
        immediate: bool,
    ) -> Result<bool> {
        if !response.order_id.trim().is_empty() {
            record.order_id = Some(response.order_id.clone());
        }
        let accepted = response.success
            && record.order_id.is_some()
            && response
                .error_msg
                .as_deref()
                .is_none_or(|value| value.trim().is_empty());
        let zero =
            response.making_amount == Decimal::ZERO && response.taking_amount == Decimal::ZERO;
        let mut closed_zero = zero && matches!(response.status, OrderStatusType::Canceled);
        if accepted && zero && immediate && !closed_zero {
            if let Ok(order) = self.client.order(&response.order_id).await {
                closed_zero = order.id == response.order_id
                    && order.maker_address == self.wallet.funder
                    && order.asset_id.to_string() == record.token_id
                    && order.size_matched == Decimal::ZERO
                    && matches!(order.status, OrderStatusType::Canceled);
            }
        }
        let uncertain = accepted
            && ((zero && immediate && !closed_zero)
                || matches!(
                    response.status,
                    OrderStatusType::Unknown(_)
                        | OrderStatusType::Delayed
                        | OrderStatusType::Unmatched
                ));
        let state = if accepted && closed_zero {
            "resolved"
        } else if uncertain {
            "uncertain"
        } else if accepted {
            "accepted"
        } else if !response.success && record.order_id.is_none() {
            "rejected"
        } else {
            "uncertain"
        };
        self.journal.outcome(record, state).await?;
        Ok(state == "uncertain")
    }
    pub async fn post_order(&self, order: SignedOrder) -> Result<PostOrderResponse> {
        self.require_mutation_allowed()?;
        let immediate = matches!(order.order_type, OrderType::FAK | OrderType::FOK);
        let mut record = self.identity(&order).await?;
        self.journal.prepare(&[record.clone()]).await?;
        if let Err(error) = self.require_mutation_allowed() {
            self.journal.outcome(&record, "rejected").await?;
            return Err(error);
        }
        match self.client.post_order(order).await {
            Ok(response) => {
                if self
                    .record_response(&mut record, &response, immediate)
                    .await?
                {
                    anyhow::bail!("CLOB acceptance or matched quantity is uncertain; reconcile the durable order identity");
                }
                Ok(response)
            }
            Err(error) => {
                self.journal
                    .outcome(
                        &record,
                        if Self::definite_rejection(&error) {
                            "rejected"
                        } else {
                            "uncertain"
                        },
                    )
                    .await?;
                Err(error.into())
            }
        }
    }
    pub async fn post_orders(&self, orders: Vec<SignedOrder>) -> Result<Vec<PostOrderResponse>> {
        self.require_mutation_allowed()?;
        let immediate: Vec<bool> = orders
            .iter()
            .map(|order| matches!(order.order_type, OrderType::FAK | OrderType::FOK))
            .collect();
        let mut records = Vec::with_capacity(orders.len());
        for order in &orders {
            records.push(self.identity(order).await?);
        }
        self.journal.prepare(&records).await?;
        if let Err(error) = self.require_mutation_allowed() {
            for record in &records {
                self.journal.outcome(record, "rejected").await?;
            }
            return Err(error);
        }
        match self.client.post_orders(orders).await {
            Ok(responses) => {
                if responses.len() != records.len() {
                    for record in &records {
                        self.journal.outcome(record, "uncertain").await?;
                    }
                    anyhow::bail!("CLOB batch acceptance is uncertain: response count differs from submitted orders");
                }
                for ((record, response), immediate) in
                    records.iter_mut().zip(&responses).zip(immediate)
                {
                    self.record_response(record, response, immediate).await?;
                }
                Ok(responses)
            }
            Err(error) => {
                for record in &records {
                    self.journal
                        .outcome(
                            record,
                            if Self::definite_rejection(&error) {
                                "rejected"
                            } else {
                                "uncertain"
                            },
                        )
                        .await?;
                }
                Err(error.into())
            }
        }
    }
    pub async fn cancel_orders(&self, ids: &[&str]) -> Result<CancelOrdersResponse> {
        self.require_cancel_allowed()?;
        for id in ids {
            let order = self.client.order(id).await?;
            anyhow::ensure!(
                order.id == *id && order.maker_address == self.wallet.funder,
                "CLOB cancel order does not belong to the configured funder"
            );
        }
        self.require_cancel_allowed()?;
        let response = self.client.cancel_orders(ids).await?;
        anyhow::ensure!(
            ids.iter()
                .all(|id| response.canceled.iter().any(|c| c == id)
                    && !response.not_canceled.contains_key(*id)),
            "CLOB did not confirm cancellation of every requested order"
        );
        Ok(response)
    }
    pub async fn cancel_all_orders(&self) -> Result<CancelOrdersResponse> {
        self.require_cancel_allowed()?;
        let mut ids = Vec::new();
        let mut cursor = None;
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = self
                .client
                .orders(&OrdersRequest::default(), cursor)
                .await?;
            ids.extend(
                page.data
                    .into_iter()
                    .filter(|order| order.maker_address == self.wallet.funder)
                    .map(|order| order.id),
            );
            if page.next_cursor.is_empty() || page.next_cursor == "LTE=" {
                break;
            }
            anyhow::ensure!(
                seen.insert(page.next_cursor.clone()),
                "CLOB orders pagination repeated a cursor"
            );
            cursor = Some(page.next_cursor);
        }
        let mut combined = CancelOrdersResponse::default();
        for chunk in ids.chunks(100) {
            let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
            let response = self.cancel_orders(&refs).await?;
            combined.canceled.extend(response.canceled);
        }
        Ok(combined)
    }
    pub async fn track_order(&self, owner: &TrackedOrder) -> Result<()> {
        self.journal
            .track(&self.wallet.funder.to_string(), owner)
            .await
    }
    pub async fn tracked_orders(&self, squadron: &str) -> Result<Vec<TrackedOrder>> {
        self.journal
            .tracked(&self.wallet.funder.to_string(), squadron)
            .await
    }
    pub async fn finish_tracked_order(&self, id: &str) -> Result<()> {
        self.journal
            .finish_tracked(&self.wallet.funder.to_string(), id)
            .await
    }

    pub async fn unresolved_tokens(&self) -> Result<Vec<String>> {
        let mut tokens: Vec<_> = self
            .journal
            .unresolved(&self.wallet.funder.to_string())
            .await?
            .into_iter()
            .map(|record| record.token_id)
            .collect();
        tokens.sort();
        tokens.dedup();
        Ok(tokens)
    }
    pub async fn pending_exposures(&self) -> Result<usize> {
        Ok(self.unresolved_tokens().await?.len())
    }
    pub async fn unresolved_orders(&self) -> Result<Vec<JournalOrder>> {
        self.journal
            .unresolved(&self.wallet.funder.to_string())
            .await
    }
    pub async fn recover_uncertain(&self) -> Result<Vec<RecoveredOrder>> {
        let mut recovered = Vec::new();
        for record in self
            .journal
            .unresolved(&self.wallet.funder.to_string())
            .await?
        {
            let id = record.order_id.as_deref().unwrap_or(&record.order_hash);
            // Absence, a transient failure or an unknown status cannot prove
            // rejection. Only an order that identifies this wallet is usable.
            let Ok(order) = self.client.order(id).await else {
                continue;
            };
            anyhow::ensure!(
                order.id == id
                    && order.maker_address == self.wallet.funder
                    && order.asset_id.to_string() == record.token_id,
                "recovered CLOB order identity does not match the durable request"
            );
            if !matches!(
                order.status,
                OrderStatusType::Live
                    | OrderStatusType::Matched
                    | OrderStatusType::Canceled
                    | OrderStatusType::Unmatched
            ) {
                continue;
            }
            self.journal.outcome(&record, "recovering").await?;
            recovered.push(RecoveredOrder { record, order });
        }
        Ok(recovered)
    }
    pub async fn acknowledge_placement(&self, id: &str) -> Result<()> {
        self.journal
            .acknowledge_placement(id, &self.wallet.funder.to_string())
            .await
    }
    pub async fn acknowledge_recovered(&self, hash: &str) -> Result<()> {
        self.journal
            .acknowledge_recovered(hash, &self.wallet.funder.to_string())
            .await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use alloy::{primitives::address, signers::local::LocalSigner};
    use sdk::clob::Config;
    use std::str::FromStr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    pub(crate) async fn client(
        host: &str,
        gate: Arc<AtomicBool>,
    ) -> Result<(
        TradingClient,
        LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
    )> {
        let signer =
            LocalSigner::from_str(&format!("{:064x}", 1u64))?.with_chain_id(Some(sdk::POLYGON));
        let wallet = WalletConfig {
            wallet_type: WalletType::GnosisSafe,
            signer: signer.address(),
            funder: address!("1111111111111111111111111111111111111111"),
        };
        let raw = Client::new(host, Config::default())?
            .authentication_builder(&signer)
            .credentials(Credentials::new(
                Default::default(),
                "c3ludGhldGlj".into(),
                "offline".into(),
            ))
            .funder(wallet.funder)
            .signature_type(wallet.signature_type())
            .authenticate()
            .await?;
        Ok((TradingClient::for_test(raw, wallet, gate).await?, signer))
    }
    pub(crate) async fn signed(
        client: &TradingClient,
        signer: &LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
        salt: u64,
    ) -> Result<SignedOrder> {
        let mut order = Order::default();
        order.salt = U256::from(salt);
        order.maker = client.wallet.funder;
        order.signer = client.wallet.signer;
        order.signatureType = 2;
        order.tokenId = U256::from(7u64);
        order.makerAmount = U256::from(500_000u64);
        order.takerAmount = U256::from(1_000_000u64);
        client.set_neg_risk(order.tokenId, false);
        Ok(client
            .sign(
                signer,
                SignableOrder::builder()
                    .payload(OrderPayload::new(order, U256::ZERO))
                    .order_type(OrderType::GTC)
                    .build(),
            )
            .await?)
    }
    pub(crate) async fn server(
        body: serde_json::Value,
        status: u16,
    ) -> Result<(
        String,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    )> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let host = format!("http://{}", listener.local_addr()?);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = calls.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let Ok(size) = socket.read(&mut buffer).await else {
                        break;
                    };
                    if size == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..size]);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&request[..end]);
                        let length = header
                            .lines()
                            .find_map(|line| {
                                line.to_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|value| value.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                seen.fetch_add(1, Ordering::SeqCst);
                let body = body.to_string();
                let reply=format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                let _ = socket.write_all(reply.as_bytes()).await;
            }
        });
        Ok((host, calls, task))
    }
    #[tokio::test]
    async fn ghost_blocks_every_mutation_before_http_even_when_ready() -> Result<()> {
        let (host, calls, task) = server(serde_json::json!({}), 200).await?;
        let gate = Arc::new(AtomicBool::new(false));
        let (client, signer) = client(&host, gate.clone()).await?;
        assert!(client
            .post_order(signed(&client, &signer, 1).await?)
            .await
            .is_err());
        assert!(client
            .post_orders(vec![signed(&client, &signer, 2).await?])
            .await
            .is_err());
        assert!(client.cancel_orders(&["known-order"]).await.is_err());
        assert!(client.cancel_all_orders().await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(client.unresolved_tokens().await?.is_empty());
        gate.store(true, Ordering::SeqCst);
        client.set_execution_ready(false);
        assert!(client
            .post_order(signed(&client, &signer, 3).await?)
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        task.abort();
        Ok(())
    }
    #[tokio::test]
    async fn ambiguous_http_response_blocks_a_fresh_salt_for_the_same_token() -> Result<()> {
        let (host, calls, task) = server(
            serde_json::json!({"error":"execution outcome unknown"}),
            500,
        )
        .await?;
        let (client, signer) = client(&host, Arc::new(AtomicBool::new(true))).await?;
        assert!(client
            .post_order(signed(&client, &signer, 1).await?)
            .await
            .is_err());
        assert_eq!(client.unresolved_tokens().await?, vec!["7"]);
        assert!(client
            .post_order(signed(&client, &signer, 2).await?)
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        task.abort();
        Ok(())
    }
    #[tokio::test]
    async fn accepted_order_stays_claimed_until_explicit_durable_adoption() -> Result<()> {
        let body = serde_json::json!({"success":true,"orderID":"accepted-order","errorMsg":"","status":"live","makingAmount":"","takingAmount":""});
        let (host, calls, task) = server(body, 200).await?;
        let (client, signer) = client(&host, Arc::new(AtomicBool::new(true))).await?;
        client
            .post_order(signed(&client, &signer, 1).await?)
            .await?;
        assert!(client
            .post_order(signed(&client, &signer, 2).await?)
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        client.acknowledge_placement("accepted-order").await?;
        assert!(client.unresolved_tokens().await?.is_empty());
        task.abort();
        Ok(())
    }
    #[tokio::test]
    async fn canceled_zero_immediate_order_finalizes_but_delayed_does_not() -> Result<()> {
        for (status, closed) in [("canceled", true), ("delayed", false), ("unmatched", false)] {
            let body = serde_json::json!({"success":true,"orderID":"zero-order","errorMsg":"","status":status,"makingAmount":"","takingAmount":""});
            let (host, _calls, task) = server(body, 200).await?;
            let (client, signer) = client(&host, Arc::new(AtomicBool::new(true))).await?;
            let mut order = signed(&client, &signer, 1).await?;
            order.order_type = OrderType::FAK;
            let result = client.post_order(order).await;
            assert_eq!(result.is_ok(), closed, "{status}");
            assert_eq!(
                client.unresolved_tokens().await?.is_empty(),
                closed,
                "{status}"
            );
            task.abort();
        }
        Ok(())
    }
}
