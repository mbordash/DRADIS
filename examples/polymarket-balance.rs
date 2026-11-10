// SPDX-License-Identifier: AGPL-3.0-only
//! Read CLOB collateral without starting the engine or a trading session.

use dradis::venues::{core::Execution, intl::IntlClobVenue};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    dradis::api::setup::load_secrets_file();
    let http = Arc::new(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?,
    );
    let venue = IntlClobVenue::connect_read_only(http).await?;
    let balance = venue.collateral().await?;
    println!(
        "{}",
        serde_json::json!({
            "wallet_type": match venue.wallet().wallet_type {
                dradis::venues::intl::wallet::WalletType::GnosisSafe => "gnosis_safe",
                dradis::venues::intl::wallet::WalletType::Poly1271 => "poly1271",
            },
            "signature_type": venue.wallet().signature_type() as u8,
            "collateral": balance.to_string(),
            "collateral_source": "live",
        })
    );
    Ok(())
}
