/// Background task: USDC collateral balance poller.
///
/// Polls the CLOB API every 5 seconds and broadcasts the latest balance
/// via a `watch` channel so all strategies can read it without blocking.
use std::str::FromStr;
use std::sync::Arc;

use polymarket_client_sdk::clob::Client as ClobClient;
use polymarket_client_sdk::clob::types::AssetType;
use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tracing::warn;

pub async fn run_balance_poller(
    trading_client: Arc<ClobClient<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>>>,
    balance_tx: watch::Sender<Decimal>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        interval.tick().await;
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Collateral;
        match trading_client.balance_allowance(req).await {
            Ok(resp) => {
                let usdc = Decimal::from_str(&resp.balance.to_string()).unwrap_or(dec!(0))
                    / dec!(1_000_000);
                let _ = balance_tx.send(usdc);
            }
            Err(e) => warn!("⚠️ Balance poll failed: {}", e),
        }
    }
}
