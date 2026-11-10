//! Distinguish the EOA signer, CLOB funder, and supported on-chain executor.

use alloy::primitives::Address;
use anyhow::{bail, Context, Result};
use polymarket_client_sdk_v2::{clob::types::SignatureType, derive_safe_wallet, POLYGON};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalletType {
    GnosisSafe,
    Poly1271,
}

impl std::str::FromStr for WalletType {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim() {
            "gnosis_safe" => Ok(Self::GnosisSafe),
            "poly1271" => Ok(Self::Poly1271),
            _ => bail!("POLYMARKET_WALLET_TYPE must be gnosis_safe or poly1271"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletConfig {
    pub wallet_type: WalletType,
    pub signer: Address,
    pub funder: Address,
}

impl WalletConfig {
    pub fn from_env(signer: Address) -> Result<Self> {
        let mode = std::env::var("POLYMARKET_WALLET_TYPE").ok();
        let funder = std::env::var("POLYMARKET_FUNDER_ADDRESS").ok();
        Self::resolve(signer, mode.as_deref(), funder.as_deref())
    }

    pub fn resolve(signer: Address, mode: Option<&str>, funder: Option<&str>) -> Result<Self> {
        if signer.is_zero() {
            bail!("wallet configuration: EOA signer must be nonzero");
        }
        let wallet_type: WalletType = mode.unwrap_or("gnosis_safe").parse()?;
        let explicit = funder
            .map(|value| {
                value.trim().parse::<Address>().map_err(|_| {
                    anyhow::anyhow!("POLYMARKET_FUNDER_ADDRESS must be a valid address")
                })
            })
            .transpose()?;
        let funder = match (wallet_type, explicit) {
            (WalletType::Poly1271, None) => bail!("poly1271 requires POLYMARKET_FUNDER_ADDRESS"),
            (_, Some(address)) if address.is_zero() => {
                bail!("POLYMARKET_FUNDER_ADDRESS must be nonzero")
            }
            (_, Some(address)) => address,
            (WalletType::GnosisSafe, None) => derive_safe_wallet(signer, POLYGON)
                .context("Gnosis Safe address derivation failed")?,
        };
        if funder == signer {
            bail!("smart-wallet funder must differ from EOA signer");
        }
        Ok(Self {
            wallet_type,
            signer,
            funder,
        })
    }

    pub fn signature_type(self) -> SignatureType {
        match self.wallet_type {
            WalletType::GnosisSafe => SignatureType::GnosisSafe,
            WalletType::Poly1271 => SignatureType::Poly1271,
        }
    }

    pub fn order_signer(self) -> Address {
        match self.wallet_type {
            WalletType::GnosisSafe => self.signer,
            WalletType::Poly1271 => self.funder,
        }
    }

    pub fn on_chain_safe(self) -> Option<Address> {
        match self.wallet_type {
            WalletType::GnosisSafe => Some(self.funder),
            WalletType::Poly1271 => None,
        }
    }

    pub fn require_safe_execution(self, address: Address) -> Result<()> {
        if self.on_chain_safe() != Some(address) {
            bail!("Safe execution unavailable for configured wallet type or address");
        }
        Ok(())
    }
}

/// CTF trading still requires the legacy negative-risk adapter approvals in
/// addition to both exchange approvals, including for Deposit Wallets.
pub fn ctf_trading_operators() -> [(&'static str, Address); 3] {
    [
        (
            "standard exchange",
            super::exchange_verifying_contract(false),
        ),
        (
            "negative-risk exchange",
            super::exchange_verifying_contract(true),
        ),
        (
            "negative-risk adapter",
            alloy::primitives::address!("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"),
        ),
    ]
}

pub fn collateral_approval_blockers(
    allowances: &std::collections::HashMap<Address, String>,
) -> Vec<String> {
    ctf_trading_operators()
        .into_iter()
        .filter_map(|(label, operator)| {
            let approved = allowances
                .get(&operator)
                .and_then(|raw| raw.parse::<alloy::primitives::U256>().ok())
                .is_some_and(|amount| amount > alloy::primitives::U256::ZERO);
            (!approved).then(|| format!("Collateral spending approval is missing for the {label}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    const EOA: Address = address!("1111111111111111111111111111111111111111");
    const FUNDER: &str = "0x2222222222222222222222222222222222222222";

    #[test]
    fn exchange_approvals_do_not_substitute_for_negative_risk_adapter() {
        let operators = ctf_trading_operators();
        let mut allowances = std::collections::HashMap::from([
            (operators[0].1, "1".to_owned()),
            (operators[1].1, "1".to_owned()),
        ]);
        let blockers = collateral_approval_blockers(&allowances);
        assert_eq!(
            blockers,
            vec!["Collateral spending approval is missing for the negative-risk adapter"]
        );
        for invalid in ["0", "-1", "invalid"] {
            allowances.insert(operators[2].1, invalid.to_owned());
            assert!(!collateral_approval_blockers(&allowances).is_empty());
        }
        allowances.insert(operators[2].1, "1".to_owned());
        assert!(collateral_approval_blockers(&allowances).is_empty());
    }

    #[test]
    fn default_preserves_derived_safe() {
        let wallet = WalletConfig::resolve(EOA, None, None).unwrap();
        assert_eq!(wallet.funder, derive_safe_wallet(EOA, POLYGON).unwrap());
        assert_eq!(wallet.signature_type(), SignatureType::GnosisSafe);
        assert_eq!(wallet.order_signer(), EOA);
        assert!(wallet.require_safe_execution(wallet.funder).is_ok());
        assert!(wallet.require_safe_execution(EOA).is_err());
    }

    #[test]
    fn explicit_safe_funder_is_preserved() {
        let wallet = WalletConfig::resolve(EOA, Some("gnosis_safe"), Some(FUNDER)).unwrap();
        assert_eq!(wallet.funder.to_string(), FUNDER);
        assert_eq!(wallet.on_chain_safe(), Some(wallet.funder));
    }

    #[test]
    fn deposit_identity_never_becomes_a_safe_executor() {
        let wallet = WalletConfig::resolve(EOA, Some("poly1271"), Some(FUNDER)).unwrap();
        assert_eq!(wallet.order_signer(), wallet.funder);
        assert_eq!(wallet.signer, EOA);
        assert_eq!(wallet.signature_type(), SignatureType::Poly1271);
        assert_eq!(wallet.on_chain_safe(), None);
        assert!(wallet.require_safe_execution(wallet.funder).is_err());
        assert!(wallet
            .require_safe_execution(derive_safe_wallet(EOA, POLYGON).unwrap())
            .is_err());
    }

    #[test]
    fn invalid_configuration_fails_closed() {
        for (mode, funder) in [
            (Some("poly1271"), None),
            (Some("poly1271"), Some("")),
            (Some("poly1271"), Some("bad-address")),
            (
                Some("poly1271"),
                Some("0x0000000000000000000000000000000000000000"),
            ),
            (
                Some("poly1271"),
                Some("0x1111111111111111111111111111111111111111"),
            ),
            (Some("unsupported"), Some(FUNDER)),
            (Some(""), Some(FUNDER)),
        ] {
            assert!(WalletConfig::resolve(EOA, mode, funder).is_err());
        }
        assert!(WalletConfig::resolve(Address::ZERO, None, None).is_err());
    }
}
