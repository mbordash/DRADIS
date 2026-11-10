// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded collateral reads with explicit wallet identity and sanitized failures.

use std::time::Duration;

use anyhow::Result;
use polymarket_client_sdk_v2::auth::{state::Authenticated, Normal};
use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk_v2::clob::types::{AssetType, SignatureType};
use polymarket_client_sdk_v2::clob::Client as ClobClient;
use polymarket_client_sdk_v2::error::{Error, Status};
use rust_decimal::Decimal;

/// Preserve the failure category and HTTP status without retaining an SDK error
/// source, whose display/debug output can contain response bodies or URLs.
pub fn sdk_error(stage: &str, error: Error) -> anyhow::Error {
    let status = error
        .downcast_ref::<Status>()
        .map(|status| status.status_code.as_u16())
        .or_else(|| {
            error
                .downcast_ref::<reqwest::Error>()
                .and_then(|error| error.status().map(|status| status.as_u16()))
        });
    anyhow::anyhow!(
        "CLOB {} failed (category={:?}, status={})",
        stage,
        error.kind(),
        status.map_or_else(|| "none".to_string(), |code| code.to_string()),
    )
}

/// Read collateral in dollars. The SDK already decodes the raw balance as a
/// Decimal; malformed responses remain errors, and a genuine zero stays zero.
pub async fn read_collateral(
    client: &ClobClient<Authenticated<Normal>>,
    signature_type: SignatureType,
) -> Result<Decimal> {
    read_collateral_with_timeout(client, signature_type, Duration::from_secs(10)).await
}

async fn read_collateral_with_timeout(
    client: &ClobClient<Authenticated<Normal>>,
    signature_type: SignatureType,
    deadline: Duration,
) -> Result<Decimal> {
    let mut request = BalanceAllowanceRequest::default();
    request.asset_type = AssetType::Collateral;
    request.signature_type = Some(signature_type);
    let response = tokio::time::timeout(deadline, client.balance_allowance(request))
        .await
        .map_err(|_| anyhow::anyhow!("CLOB collateral failed (category=timeout, status=none)"))?
        .map_err(|error| sdk_error("collateral", error))?;
    Ok(response.balance / Decimal::from(1_000_000u32))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use alloy::primitives::{address, Address};
    use alloy::signers::{local::LocalSigner, Signer};
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode, Uri};
    use axum::{routing::get, Router};
    use base64::Engine as _;
    use polymarket_client_sdk_v2::auth::{Credentials, Uuid};
    use polymarket_client_sdk_v2::clob::Config;
    use polymarket_client_sdk_v2::POLYGON;
    use rust_decimal_macros::dec;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct Reply {
        status: StatusCode,
        body: String,
        delay: Duration,
        requests: mpsc::UnboundedSender<(Uri, HeaderMap)>,
    }

    async fn respond(
        State(reply): State<Reply>,
        uri: Uri,
        headers: HeaderMap,
    ) -> (StatusCode, String) {
        let _ = reply.requests.send((uri, headers));
        tokio::time::sleep(reply.delay).await;
        (reply.status, reply.body)
    }

    struct Fixture {
        client: ClobClient<Authenticated<Normal>>,
        eoa: Address,
        requests: mpsc::UnboundedReceiver<(Uri, HeaderMap)>,
        server: tokio::task::JoinHandle<()>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    impl Fixture {
        async fn new(
            signature_type: SignatureType,
            status: StatusCode,
            body: &str,
            delay: Duration,
        ) -> Self {
            let (requests, receiver) = mpsc::unbounded_channel();
            let app = Router::new()
                .route("/balance-allowance", get(respond))
                .with_state(Reply {
                    status,
                    body: body.to_string(),
                    delay,
                    requests,
                });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let host = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            // Synthetic signing material is used only against the local fixture.
            let signer = LocalSigner::from_slice(&[1u8; 32])
                .unwrap()
                .with_chain_id(Some(POLYGON));
            let eoa = signer.address();
            let credentials = Credentials::new(
                Uuid::nil(),
                base64::engine::general_purpose::URL_SAFE.encode([0u8; 32]),
                "synthetic-passphrase".into(),
            );
            let client = ClobClient::new(&host, Config::default())
                .unwrap()
                .authentication_builder(&signer)
                .funder(address!("0x1111111111111111111111111111111111111111"))
                .signature_type(signature_type)
                .credentials(credentials)
                .authenticate()
                .await
                .unwrap();
            Self {
                client,
                eoa,
                requests: receiver,
                server,
            }
        }
    }

    #[tokio::test]
    async fn collateral_uses_explicit_wallet_type_and_eoa_l2_headers() {
        for signature_type in [SignatureType::GnosisSafe, SignatureType::Poly1271] {
            // Authenticate as Safe, then explicitly override the request for Poly1271.
            let mut fixture = Fixture::new(
                SignatureType::GnosisSafe,
                StatusCode::OK,
                r#"{"balance":"123456789","allowances":{}}"#,
                Duration::ZERO,
            )
            .await;
            assert_eq!(
                read_collateral(&fixture.client, signature_type)
                    .await
                    .unwrap(),
                dec!(123.456789)
            );
            let (uri, headers) = fixture.requests.recv().await.unwrap();
            let query: HashMap<_, _> =
                url::form_urlencoded::parse(uri.query().unwrap().as_bytes()).collect();
            assert_eq!(query["asset_type"], "COLLATERAL");
            assert_eq!(query["signature_type"], (signature_type as u8).to_string());
            assert_eq!(headers["poly_address"], fixture.eoa.to_checksum(None));
            assert_eq!(headers["poly_api_key"], Uuid::nil().to_string());
            assert_eq!(headers["poly_passphrase"], "synthetic-passphrase");
            assert!(!headers["poly_signature"].is_empty());
            assert!(!headers["poly_timestamp"].is_empty());
        }
    }

    #[tokio::test]
    async fn a_zero_collateral_balance_remains_zero() {
        let fixture = Fixture::new(
            SignatureType::Poly1271,
            StatusCode::OK,
            r#"{"balance":"0","allowances":{}}"#,
            Duration::ZERO,
        )
        .await;
        assert_eq!(
            read_collateral(&fixture.client, SignatureType::Poly1271)
                .await
                .unwrap(),
            Decimal::ZERO
        );
    }

    #[tokio::test]
    async fn rejected_collateral_reads_report_status_without_response_content() {
        let fixture = Fixture::new(
            SignatureType::Poly1271,
            StatusCode::UNAUTHORIZED,
            "sensitive-response-sentinel",
            Duration::ZERO,
        )
        .await;
        let error = read_collateral(&fixture.client, SignatureType::Poly1271)
            .await
            .unwrap_err();
        let rendered = format!("{error:?}");
        assert!(rendered.contains("collateral"));
        assert!(rendered.contains("category=Status"));
        assert!(rendered.contains("status=401"));
        assert!(!rendered.contains("sensitive-response-sentinel"));
        assert!(!rendered.contains("synthetic-passphrase"));
        assert!(error.source().is_none());
    }

    #[tokio::test]
    async fn malformed_collateral_is_an_error_instead_of_a_zero_balance() {
        let fixture = Fixture::new(
            SignatureType::Poly1271,
            StatusCode::OK,
            r#"{"balance":"sensitive-invalid-number","allowances":{}}"#,
            Duration::ZERO,
        )
        .await;
        let error = read_collateral(&fixture.client, SignatureType::Poly1271)
            .await
            .unwrap_err();
        assert!(!format!("{error:?}").contains("sensitive-invalid-number"));
        assert!(error.source().is_none());
    }

    #[tokio::test]
    async fn collateral_reads_stop_at_the_deadline() {
        let fixture = Fixture::new(
            SignatureType::Poly1271,
            StatusCode::OK,
            r#"{"balance":"123456789","allowances":{}}"#,
            Duration::from_secs(30),
        )
        .await;
        let error = read_collateral_with_timeout(
            &fixture.client,
            SignatureType::Poly1271,
            Duration::from_millis(25),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("category=timeout"));
    }
}
