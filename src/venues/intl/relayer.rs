//! Deposit Wallet batches using the official WALLET relayer protocol.

use std::{str::FromStr, sync::Arc, time::Duration};

use alloy::primitives::{address, Address, Bytes, B256, U256};
use alloy::signers::{k256::ecdsa::SigningKey, local::LocalSigner, Signer};
use alloy::sol;
use alloy::sol_types::{Eip712Domain, SolStruct};
use anyhow::{bail, ensure, Context, Result};
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Url,
};
use serde::{Deserialize, Serialize};

pub const DEPOSIT_FACTORY: Address = address!("00000000000Fb5C9ADea0298D729A0CB3823Cc07");
pub const DEPOSIT_IMPLEMENTATION: Address = address!("58CA52ebe0DadfdF531Cde7062e76746de4Db1eB");
pub const DEPOSIT_BEACON: Address = address!("7A18EDfe055488A3128f01F563e5B479D92ffc3a");
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

sol! {
    struct Call { address target; uint256 value; bytes data; }
    struct Batch { address wallet; uint256 nonce; uint256 deadline; Call[] calls; }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalletCall {
    pub target: Address,
    pub value: String,
    pub data: Bytes,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletParams {
    pub deposit_wallet: Address,
    pub deadline: String,
    pub calls: Vec<WalletCall>,
}

// Deliberately no Debug: the signed request authorizes a wallet operation.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletRequest {
    #[serde(rename = "type")]
    pub transaction_type: &'static str,
    pub from: Address,
    pub to: Address,
    pub nonce: String,
    pub signature: String,
    pub deposit_wallet_params: WalletParams,
}

pub fn batch_digest(
    wallet: Address,
    nonce: U256,
    deadline: U256,
    calls: &[WalletCall],
) -> Result<B256> {
    let calls = calls
        .iter()
        .map(|call| {
            Ok(Call {
                target: call.target,
                value: U256::from_str(&call.value).context("invalid WALLET call value")?,
                data: call.data.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let batch = Batch {
        wallet,
        nonce,
        deadline,
        calls,
    };
    Ok(batch.eip712_signing_hash(&Eip712Domain {
        name: Some("DepositWallet".into()),
        version: Some("1".into()),
        chain_id: Some(U256::from(137)),
        verifying_contract: Some(wallet),
        ..Default::default()
    }))
}

pub async fn sign_batch(
    signer: &LocalSigner<SigningKey>,
    wallet: Address,
    nonce: U256,
    deadline: u64,
    calls: Vec<WalletCall>,
) -> Result<WalletRequest> {
    ensure!(!calls.is_empty(), "WALLET batch must contain calls");
    let digest = batch_digest(wallet, nonce, U256::from(deadline), &calls)?;
    let signature = signer
        .sign_hash(&digest)
        .await
        .map_err(|_| anyhow::anyhow!("WALLET batch signing failed"))?;
    Ok(WalletRequest {
        transaction_type: "WALLET",
        from: signer.address(),
        to: DEPOSIT_FACTORY,
        nonce: nonce.to_string(),
        signature: signature.to_string(),
        deposit_wallet_params: WalletParams {
            deposit_wallet: wallet,
            deadline: deadline.to_string(),
            calls,
        },
    })
}

#[derive(Clone, Debug, Deserialize)]
pub struct RelayerTransaction {
    #[serde(rename = "transactionID", alias = "transaction_id")]
    pub id: String,
    pub state: String,
    #[serde(rename = "transactionHash", alias = "transaction_hash", default)]
    pub transaction_hash: Option<String>,
}

impl RelayerTransaction {
    pub fn confirmed_hash(&self) -> Result<B256> {
        ensure!(
            self.state == "STATE_CONFIRMED",
            "relayer transaction is not confirmed"
        );
        let hash = self
            .transaction_hash
            .as_deref()
            .context("confirmed relayer transaction has no hash")?;
        let hash = B256::from_str(hash)
            .map_err(|_| anyhow::anyhow!("invalid relayer transaction hash"))?;
        ensure!(!hash.is_zero(), "relayer returned a zero transaction hash");
        Ok(hash)
    }
}

pub struct Relayer {
    http: Arc<reqwest::Client>,
    url: Url,
    headers: HeaderMap,
}

impl Relayer {
    pub fn from_env(_http: Arc<reqwest::Client>, signer: Address) -> Result<Option<Self>> {
        let key = std::env::var("POLYMARKET_RELAYER_API_KEY")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let address = std::env::var("POLYMARKET_RELAYER_API_KEY_ADDRESS")
            .ok()
            .filter(|s| !s.trim().is_empty());
        if key.is_none() && address.is_none() {
            return Ok(None);
        }
        let key = key.context("POLYMARKET_RELAYER_API_KEY is required")?;
        let address = address
            .context("POLYMARKET_RELAYER_API_KEY_ADDRESS is required")?
            .parse::<Address>()
            .map_err(|_| anyhow::anyhow!("invalid relayer API key address"))?;
        ensure!(
            address == signer,
            "relayer API key address must match wallet owner signer"
        );
        let url = std::env::var("POLYMARKET_RELAYER_URL")
            .unwrap_or_else(|_| "https://relayer-v2.polymarket.com".into());
        Self::new(&url, &key, address).map(Some)
    }

    fn new(url: &str, key: &str, address: Address) -> Result<Self> {
        let mut url = Url::parse(url).map_err(|_| anyhow::anyhow!("invalid relayer URL"))?;
        let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        ensure!(
            url.scheme() == "https" || (cfg!(test) && loopback && url.scheme() == "http"),
            "relayer URL must use HTTPS"
        );
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "relayer URL must not contain credentials, query, or fragment"
        );
        ensure!(
            url.path() == "/" || url.path().is_empty(),
            "relayer URL must not contain a path"
        );
        url.set_path("/");
        let mut headers = HeaderMap::new();
        let mut key = HeaderValue::from_str(key.trim())
            .map_err(|_| anyhow::anyhow!("invalid relayer API key header"))?;
        key.set_sensitive(true);
        headers.insert("RELAYER_API_KEY", key);
        headers.insert(
            "RELAYER_API_KEY_ADDRESS",
            HeaderValue::from_str(&address.to_checksum(None))?,
        );
        // Never forward relayer credentials through redirects.
        let http = Arc::new(
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(REQUEST_TIMEOUT)
                .build()?,
        );
        Ok(Self { http, url, headers })
    }

    async fn get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        let request = self
            .http
            .get(self.url.join(path)?)
            .headers(self.headers.clone())
            .query(query);
        let response = tokio::time::timeout(REQUEST_TIMEOUT, request.send())
            .await
            .map_err(|_| anyhow::anyhow!("relayer read timed out"))?
            .map_err(|_| anyhow::anyhow!("relayer read transport failed"))?;
        checked_json(response).await
    }

    pub async fn nonce(&self, signer: Address) -> Result<U256> {
        #[derive(Deserialize)]
        struct Nonce {
            address: Address,
            nonce: String,
        }
        let response: Nonce = self
            .get(
                "v1/account/transactions/params",
                &[("address", signer.to_string()), ("type", "WALLET".into())],
            )
            .await?;
        ensure!(
            response.address == signer,
            "relayer params address differs from signer"
        );
        U256::from_str(&response.nonce).map_err(|_| anyhow::anyhow!("relayer nonce is invalid"))
    }

    pub async fn deployed(&self, wallet: Address) -> Result<bool> {
        #[derive(Deserialize)]
        struct Deployed {
            deployed: bool,
        }
        let response: Deployed = self
            .get(
                "deployed",
                &[("address", wallet.to_string()), ("type", "WALLET".into())],
            )
            .await?;
        Ok(response.deployed)
    }

    pub async fn submit(&self, request: &WalletRequest) -> Result<RelayerTransaction> {
        let response = tokio::time::timeout(
            REQUEST_TIMEOUT,
            self.http
                .post(self.url.join("submit")?)
                .headers(self.headers.clone())
                .json(request)
                .send(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("relayer submit outcome unknown: timeout"))?
        .map_err(|_| anyhow::anyhow!("relayer submit outcome unknown: transport"))?;
        let transaction: RelayerTransaction = checked_json(response).await?;
        ensure!(
            !transaction.id.trim().is_empty(),
            "relayer submission has no transaction ID; outcome unknown"
        );
        Ok(transaction)
    }

    pub async fn transaction(&self, id: &str) -> Result<RelayerTransaction> {
        ensure!(
            !id.is_empty()
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid relayer transaction ID"
        );
        let row: RelayerTransaction = self
            .get(&format!("v1/account/transactions/{id}"), &[])
            .await?;
        ensure!(row.id == id, "relayer transaction ID mismatch");
        Ok(row)
    }
}

async fn checked_json<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let status = response.status();
    if !status.is_success() {
        bail!("relayer HTTP failure status={}", status.as_u16());
    }
    tokio::time::timeout(REQUEST_TIMEOUT, response.json())
        .await
        .map_err(|_| anyhow::anyhow!("relayer response timed out"))?
        .map_err(|_| anyhow::anyhow!("relayer response is malformed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{OriginalUri, State},
        http::{HeaderMap, StatusCode},
        routing::get,
        Json, Router,
    };
    use tokio::sync::mpsc;

    #[test]
    fn batch_digest_matches_independent_eip712_vector() {
        // Independently encoded with Python eth_abi + eth_utils.keccak using the
        // official Batch/Call types, chain 137 and DepositWallet/1 domain.
        let digest = batch_digest(
            address!("2222222222222222222222222222222222222222"),
            U256::from(7),
            U256::from(1_900_000_600u64),
            &[WalletCall {
                target: address!("3333333333333333333333333333333333333333"),
                value: "0".into(),
                data: Bytes::from_static(&[0x12, 0x34, 0x56, 0x78]),
            }],
        )
        .unwrap();
        assert_eq!(
            digest,
            B256::from_str("0x6df8fca9d68078f42d296d150a29464bc77a06a5d7f6979d620808a94071e536")
                .unwrap()
        );
        let digest = batch_digest(
            address!("2222222222222222222222222222222222222222"),
            U256::from(7),
            U256::from(1_900_000_600u64),
            &[
                WalletCall {
                    target: address!("3333333333333333333333333333333333333333"),
                    value: "0".into(),
                    data: Bytes::from_static(&[0x12, 0x34, 0x56, 0x78]),
                },
                WalletCall {
                    target: address!("4444444444444444444444444444444444444444"),
                    value: "17".into(),
                    data: Bytes::from_static(&[0xab, 0xcd, 0xef]),
                },
            ],
        )
        .unwrap();
        assert_eq!(
            digest,
            B256::from_str("0x50ee43bebdf97db7b9351e717525595d7cde5fbce37369cc3de761aace5f8761")
                .unwrap()
        );
    }

    struct Fixture {
        relayer: Relayer,
        requests: mpsc::UnboundedReceiver<(String, HeaderMap, serde_json::Value)>,
        server: tokio::task::JoinHandle<()>,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    #[derive(Clone)]
    struct FixtureState {
        status: StatusCode,
        response: serde_json::Value,
        lost: bool,
        requests: mpsc::UnboundedSender<(String, HeaderMap, serde_json::Value)>,
    }
    async fn read(
        State(state): State<FixtureState>,
        uri: OriginalUri,
        headers: HeaderMap,
    ) -> (StatusCode, Json<serde_json::Value>) {
        state
            .requests
            .send((uri.0.to_string(), headers, serde_json::Value::Null))
            .unwrap();
        (state.status, Json(state.response))
    }
    async fn submit(
        State(state): State<FixtureState>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        state
            .requests
            .send(("/submit".into(), headers, body))
            .unwrap();
        if state.lost {
            return (
                StatusCode::OK,
                Json(serde_json::json!({"missing":"transaction_id"})),
            );
        }
        (state.status, Json(state.response))
    }
    impl Fixture {
        async fn new(status: StatusCode, response: serde_json::Value, lost: bool) -> Self {
            let (sender, requests) = mpsc::unbounded_channel();
            let state = FixtureState {
                status,
                response,
                lost,
                requests: sender,
            };
            let app = Router::new()
                .route("/v1/account/transactions/params", get(read))
                .route("/v1/account/transactions/{id}", get(read))
                .route("/submit", axum::routing::post(submit))
                .with_state(state);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let relayer = Relayer::new(
                &format!("http://{address}"),
                "synthetic-relayer-key",
                address!("1111111111111111111111111111111111111111"),
            )
            .unwrap();
            Self {
                relayer,
                requests,
                server,
            }
        }
    }

    fn synthetic_signer() -> LocalSigner<SigningKey> {
        LocalSigner::from_slice(&[1u8; 32])
            .unwrap()
            .with_chain_id(Some(137))
    }
    fn calls() -> Vec<WalletCall> {
        vec![WalletCall {
            target: address!("3333333333333333333333333333333333333333"),
            value: "0".into(),
            data: Bytes::from_static(&[0x12, 0x34, 0x56, 0x78]),
        }]
    }

    #[tokio::test]
    async fn current_authenticated_params_require_matching_owner_and_numeric_nonce() {
        let owner = address!("1111111111111111111111111111111111111111");
        let mut fixture = Fixture::new(
            StatusCode::OK,
            serde_json::json!({"address":owner,"nonce":"9"}),
            false,
        )
        .await;
        assert_eq!(fixture.relayer.nonce(owner).await.unwrap(), U256::from(9));
        let (uri, headers, _) = fixture.requests.recv().await.unwrap();
        assert!(uri.starts_with("/v1/account/transactions/params?"));
        assert!(uri.contains("type=WALLET"));
        assert_eq!(headers["RELAYER_API_KEY"], "synthetic-relayer-key");
        assert_eq!(headers["RELAYER_API_KEY_ADDRESS"], owner.to_checksum(None));
        let mismatch = Fixture::new(
            StatusCode::OK,
            serde_json::json!({"address":Address::ZERO,"nonce":"9"}),
            false,
        )
        .await;
        assert!(mismatch.relayer.nonce(owner).await.is_err());
        let malformed = Fixture::new(
            StatusCode::OK,
            serde_json::json!({"address":owner,"nonce":"not-numeric"}),
            false,
        )
        .await;
        assert!(malformed.relayer.nonce(owner).await.is_err());
    }

    #[tokio::test]
    async fn wallet_submission_uses_official_payload_and_owner_signature() {
        let mut fixture = Fixture::new(
            StatusCode::OK,
            serde_json::json!({"transactionID":"synthetic-42","state":"STATE_NEW"}),
            false,
        )
        .await;
        let signer = synthetic_signer();
        let wallet = address!("2222222222222222222222222222222222222222");
        let request = sign_batch(&signer, wallet, U256::from(7), 1_900_000_600, calls())
            .await
            .unwrap();
        let signature = alloy::primitives::Signature::from_str(&request.signature).unwrap();
        let digest = batch_digest(
            wallet,
            U256::from(7),
            U256::from(1_900_000_600u64),
            &calls(),
        )
        .unwrap();
        assert_eq!(
            signature.recover_address_from_prehash(&digest).unwrap(),
            signer.address()
        );
        assert_eq!(
            fixture.relayer.submit(&request).await.unwrap().id,
            "synthetic-42"
        );
        let (_, headers, body) = fixture.requests.recv().await.unwrap();
        assert_eq!(headers["RELAYER_API_KEY"], "synthetic-relayer-key");
        assert_eq!(body["type"], "WALLET");
        assert_eq!(body["from"], serde_json::json!(signer.address()));
        assert_eq!(body["to"], serde_json::json!(DEPOSIT_FACTORY));
        assert_eq!(body["nonce"], "7");
        assert_eq!(
            body["depositWalletParams"]["depositWallet"],
            serde_json::json!(wallet)
        );
        assert_eq!(body["depositWalletParams"]["deadline"], "1900000600");
        assert_eq!(body["depositWalletParams"]["calls"][0]["value"], "0");
    }

    #[tokio::test]
    async fn rejection_and_lost_response_never_become_confirmed_submissions() {
        let signer = synthetic_signer();
        let request = sign_batch(
            &signer,
            address!("2222222222222222222222222222222222222222"),
            U256::from(1),
            1_900_000_600,
            calls(),
        )
        .await
        .unwrap();
        for status in [StatusCode::UNAUTHORIZED, StatusCode::INTERNAL_SERVER_ERROR] {
            let fixture = Fixture::new(
                status,
                serde_json::json!({"error":"sensitive-response-sentinel"}),
                false,
            )
            .await;
            let error = fixture.relayer.submit(&request).await.unwrap_err();
            assert!(!format!("{error:?}").contains("sensitive-response-sentinel"));
            assert!(!format!("{error:?}").contains("synthetic-relayer-key"));
        }
        let lost = Fixture::new(StatusCode::OK, serde_json::Value::Null, true).await;
        assert!(lost.relayer.submit(&request).await.is_err());
    }

    #[tokio::test]
    async fn polling_uses_authenticated_current_transaction_endpoint() {
        let hash = B256::repeat_byte(4);
        let mut fixture = Fixture::new(StatusCode::OK, serde_json::json!({"transaction_id":"synthetic-42","transaction_hash":hash,"state":"STATE_CONFIRMED"}), false).await;
        assert_eq!(
            fixture
                .relayer
                .transaction("synthetic-42")
                .await
                .unwrap()
                .confirmed_hash()
                .unwrap(),
            hash
        );
        let (uri, headers, _) = fixture.requests.recv().await.unwrap();
        assert_eq!(uri, "/v1/account/transactions/synthetic-42");
        assert!(headers.contains_key("RELAYER_API_KEY"));
        for state in [
            "STATE_NEW",
            "STATE_MINED",
            "STATE_FAILED",
            "STATE_INVALID",
            "unknown",
        ] {
            assert!(RelayerTransaction {
                id: "id".into(),
                state: state.into(),
                transaction_hash: Some(hash.to_string())
            }
            .confirmed_hash()
            .is_err());
        }
    }

    #[test]
    fn relayer_transport_rejects_unsafe_urls_and_query_credentials() {
        for url in [
            "http://example.com",
            "https://example.com/?key=secret",
            "https://user:pass@example.com",
            "https://example.com/path",
        ] {
            assert!(Relayer::new(url, "synthetic", Address::ZERO).is_err());
        }
    }
}
