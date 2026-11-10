//! Immutable wallet transport with durable, fail-closed execution reconciliation.

use super::{
    relayer::{self, Relayer, WalletCall},
    wallet::{WalletConfig, WalletType},
};
use alloy::primitives::{keccak256, Address, Bytes, B256, U256};
use alloy::providers::Provider;
use alloy::signers::{k256::ecdsa::SigningKey, local::LocalSigner};
use alloy::{
    sol,
    sol_types::{SolCall, SolValue},
};
use anyhow::{bail, ensure, Context, Result};
use fs4::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const EXECUTION_TIMEOUT: Duration = Duration::from_secs(45);

sol! {
    #[sol(rpc)]
    interface IEffectViews {
        function balanceOf(address account, uint256 id) external view returns (uint256);
    }
    #[sol(rpc)]
    interface IERC20EffectViews {
        function balanceOf(address account) external view returns (uint256);
        function allowance(address account, address spender) external view returns (uint256);
    }
    interface IEffectCalls {
        function approve(address spender, uint256 amount) external;
        function wrap(address asset, address to, uint256 amount) external;
        function redeem(bytes31 conditionId, uint256 outcomeIndex, uint256 amount) external;
    }
    #[sol(rpc)]
    interface IWalletIdentity {
        function nonce() external view returns (uint256);
        function getOwners() external view returns (address[]);
        function getThreshold() external view returns (uint256);
        function BEACON() external view returns (address);
        function execTransaction(address to,uint256 value,bytes data,uint8 operation,uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,address refundReceiver,bytes signatures) external payable returns (bool success);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum IntentState {
    Prepared,
    Submitted,
    ReceiptConfirmed,
    EffectConfirmed,
    Failed,
}

/// Observable effect required before an intent may leave reconciliation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExecutionEffect {
    TokenBalancesZero {
        contract: Address,
        token_ids: Vec<U256>,
    },
    Allowance {
        token: Address,
        spender: Address,
        minimum: U256,
    },
    Wrap {
        token: Address,
        before: U256,
        amount: U256,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Intent {
    wallet: Address,
    signer: Address,
    call_hash: B256,
    effect: ExecutionEffect,
    target: Address,
    calldata: Bytes,
    nonce: Option<String>,
    deadline: Option<u64>,
    relay_id: Option<String>,
    transaction_hash: Option<B256>,
    state: IntentState,
}

impl Intent {
    fn unresolved(&self) -> bool {
        !matches!(
            self.state,
            IntentState::EffectConfirmed | IntentState::Failed
        )
    }
}

struct Journal {
    dir: PathBuf,
}

impl Journal {
    fn new(data_dir: PathBuf, wallet: Address) -> Self {
        Self {
            dir: data_dir.join("wallet-execution").join(wallet.to_string()),
        }
    }
    fn lock(&self) -> Result<File> {
        fs::create_dir_all(&self.dir).context("create wallet execution directory")?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.dir.join("execution.lock"))?;
        FileExt::try_lock(&file)
            .context("wallet execution is already running in another process")?;
        Ok(file)
    }
    fn read(&self) -> Result<Option<Intent>> {
        match fs::read(self.dir.join("intent.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .context("wallet intent is unreadable; execution blocked"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => bail!("wallet intent cannot be read; execution blocked"),
        }
    }
    fn write(&self, intent: &Intent) -> Result<()> {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let temp = self
            .dir
            .join(format!("intent-{}-{suffix}.tmp", std::process::id()));
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(&serde_json::to_vec(intent)?)?;
        file.sync_all()?;
        fs::rename(&temp, self.dir.join("intent.json"))?;
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
}

pub struct WalletExecutor<P> {
    provider: P,
    signer: LocalSigner<SigningKey>,
    wallet: WalletConfig,
    relayer: Option<Relayer>,
    relayer_config_blocker: Option<String>,
    journal: Journal,
    gate: Mutex<()>,
}

impl<P: Provider + Clone> WalletExecutor<P> {
    pub async fn new(
        provider: P,
        signer: LocalSigner<SigningKey>,
        wallet: WalletConfig,
        http: Arc<reqwest::Client>,
        data_dir: PathBuf,
    ) -> Result<Self> {
        ensure!(
            wallet.signer == signer.address(),
            "executor signer differs from immutable wallet configuration"
        );
        let (relayer, relayer_config_blocker) = match wallet.wallet_type {
            WalletType::GnosisSafe => (None, None),
            WalletType::Poly1271 => match Relayer::from_env(http, wallet.signer) {
                Ok(Some(relayer)) => (Some(relayer), None),
                Ok(None) => (None, Some("Deposit Wallet relayer API credentials are not configured".into())),
                Err(_) => (None, Some("Deposit Wallet relayer configuration is invalid; check API key, owner address, and HTTPS URL".into())),
            },
        };
        Ok(Self {
            provider,
            signer,
            wallet,
            relayer,
            relayer_config_blocker,
            journal: Journal::new(data_dir, wallet.funder),
            gate: Mutex::new(()),
        })
    }
    pub fn provider(&self) -> &P {
        &self.provider
    }
    pub fn wallet(&self) -> WalletConfig {
        self.wallet
    }

    #[cfg(test)]
    pub(crate) fn for_read_tests(
        provider: P,
        signer: LocalSigner<SigningKey>,
        wallet: WalletConfig,
    ) -> Self {
        assert_eq!(wallet.signer, signer.address());
        Self {
            provider,
            signer,
            wallet,
            relayer: None,
            relayer_config_blocker: None,
            journal: Journal::new(
                std::env::temp_dir().join("dradis-read-only-fixture"),
                wallet.funder,
            ),
            gate: Mutex::new(()),
        }
    }

    pub fn live_blockers(&self) -> Vec<String> {
        let mut blockers = Vec::new();
        if let Some(blocker) = &self.relayer_config_blocker {
            blockers.push(blocker.clone());
        }
        match self.journal.read() {
            Ok(Some(intent)) if intent.unresolved() => blockers.push(
                "wallet execution intent awaits reconciliation or token effect verification".into(),
            ),
            Err(_) => blockers.push("wallet execution journal is unreadable".into()),
            _ => {}
        }
        blockers
    }

    pub async fn verify_ready(&self) -> Result<()> {
        self.verify_identity().await?;
        self.reconcile_pending_effect().await?;
        let blockers = self.live_blockers();
        ensure!(blockers.is_empty(), "{}", blockers.join("; "));
        Ok(())
    }

    /// This recovery is read-only on chain and is allowed while GHOST is set.
    pub async fn reconcile_pending_effect(&self) -> Result<()> {
        let _gate = self.gate.lock().await;
        let _file_lock = self.journal.lock()?;
        let Some(mut intent) = self.journal.read()? else {
            return Ok(());
        };
        if !intent.unresolved() {
            return Ok(());
        }
        ensure!(
            intent.wallet == self.wallet.funder && intent.signer == self.wallet.signer,
            "wallet intent identity mismatch"
        );
        self.reconcile(&mut intent).await?;
        self.verify_effect(&intent.effect).await?;
        intent.state = IntentState::EffectConfirmed;
        self.journal.write(&intent)
    }

    async fn verify_effect(&self, effect: &ExecutionEffect) -> Result<()> {
        match effect {
            ExecutionEffect::TokenBalancesZero {
                contract,
                token_ids,
            } => {
                ensure!(!token_ids.is_empty(), "redemption effect has no token IDs");
                let tokens = IEffectViews::new(*contract, self.provider.clone());
                for id in token_ids {
                    let balance = tokio::time::timeout(
                        RPC_TIMEOUT,
                        tokens.balanceOf(self.wallet.funder, *id).call(),
                    )
                    .await
                    .context("redemption effect read timed out")?
                    .map_err(|_| anyhow::anyhow!("redemption effect read failed"))?;
                    ensure!(
                        balance.is_zero(),
                        "redemption token burn is not verified; intent retained"
                    );
                }
            }
            ExecutionEffect::Allowance {
                token,
                spender,
                minimum,
            } => {
                let token = IERC20EffectViews::new(*token, self.provider.clone());
                let allowance = tokio::time::timeout(
                    RPC_TIMEOUT,
                    token.allowance(self.wallet.funder, *spender).call(),
                )
                .await
                .context("approval effect read timed out")?
                .map_err(|_| anyhow::anyhow!("approval effect read failed"))?;
                ensure!(
                    allowance >= *minimum,
                    "approval effect is not verified; intent retained"
                );
            }
            ExecutionEffect::Wrap {
                token,
                before,
                amount,
            } => {
                let token = IERC20EffectViews::new(*token, self.provider.clone());
                let balance =
                    tokio::time::timeout(RPC_TIMEOUT, token.balanceOf(self.wallet.funder).call())
                        .await
                        .context("wrap effect read timed out")?
                        .map_err(|_| anyhow::anyhow!("wrap effect read failed"))?;
                ensure!(
                    *before >= *amount && balance <= *before - *amount,
                    "wrap balance effect is not verified; intent retained"
                );
            }
        }
        Ok(())
    }

    async fn verify_identity(&self) -> Result<()> {
        let chain = tokio::time::timeout(RPC_TIMEOUT, self.provider.get_chain_id())
            .await
            .context("wallet chain read timed out")?
            .map_err(|_| anyhow::anyhow!("wallet chain read failed"))?;
        ensure!(chain == 137, "wallet execution requires Polygon chain 137");
        let code = tokio::time::timeout(RPC_TIMEOUT, self.provider.get_code_at(self.wallet.funder))
            .await
            .context("wallet deployment read timed out")?
            .map_err(|_| anyhow::anyhow!("wallet deployment read failed"))?;
        ensure!(!code.is_empty(), "configured wallet is not deployed");
        let contract = IWalletIdentity::new(self.wallet.funder, self.provider.clone());
        match self.wallet.wallet_type {
            WalletType::GnosisSafe => {
                let owners = tokio::time::timeout(RPC_TIMEOUT, contract.getOwners().call())
                    .await
                    .context("Safe owners read timed out")?
                    .map_err(|_| anyhow::anyhow!("Safe owners read failed"))?;
                let threshold = tokio::time::timeout(RPC_TIMEOUT, contract.getThreshold().call())
                    .await
                    .context("Safe threshold read timed out")?
                    .map_err(|_| anyhow::anyhow!("Safe threshold read failed"))?;
                ensure!(
                    owners.contains(&self.wallet.signer) && threshold == U256::from(1),
                    "Safe requires one direct owner signature"
                );
            }
            WalletType::Poly1271 => {
                let (uups, beacon) = deposit_addresses(self.wallet.signer)?;
                ensure!(self.wallet.funder == uups || self.wallet.funder == beacon, "Deposit Wallet is not a canonical wallet for this owner; session keys and unknown variants are unsupported");
                let relayer = self
                    .relayer
                    .as_ref()
                    .context("Deposit Wallet relayer API credentials are not configured")?;
                let relayer_nonce = relayer.nonce(self.wallet.signer).await?;
                let chain_nonce = tokio::time::timeout(RPC_TIMEOUT, contract.nonce().call())
                    .await
                    .context("Deposit Wallet nonce read timed out")?
                    .map_err(|_| anyhow::anyhow!("Deposit Wallet nonce read failed"))?;
                ensure!(
                    relayer_nonce == chain_nonce,
                    "relayer nonce differs from Deposit Wallet on-chain nonce"
                );
            }
        }
        Ok(())
    }

    pub fn pending_matches(&self, target: Address, calldata: &[u8]) -> Result<bool> {
        Ok(self.journal.read()?.is_some_and(|intent| {
            intent.unresolved() && intent.call_hash == call_hash(target, calldata)
        }))
    }

    /// Resume only the recorded request; this method never submits a new one.
    pub async fn pending_operation(&self) -> Result<Option<(Address, Bytes, B256)>> {
        let _gate = self.gate.lock().await;
        let _file_lock = self.journal.lock()?;
        let Some(mut intent) = self.journal.read()? else {
            return Ok(None);
        };
        if !intent.unresolved() {
            return Ok(None);
        }
        ensure!(
            intent.wallet == self.wallet.funder && intent.signer == self.wallet.signer,
            "wallet intent identity mismatch"
        );
        let hash = self.reconcile(&mut intent).await?;
        Ok(Some((intent.target, intent.calldata, hash)))
    }

    /// Receipt confirmation leaves the intent blocked until the caller proves
    /// the intended token/allowance effect and calls `confirm_effect`.
    pub async fn execute(&self, target: Address, calldata: Vec<u8>) -> Result<B256> {
        use alloy::primitives::address;
        let effect = if target == address!("12121212006e4CD160D18e3f00711DA5c3372600") {
            let call = IEffectCalls::redeemCall::abi_decode(&calldata)
                .context("unsupported Router calldata")?;
            ensure!(
                call.outcomeIndex <= U256::from(1),
                "unsupported outcome index"
            );
            let mut token = [0u8; 32];
            token[..31].copy_from_slice(call.conditionId.as_slice());
            token[31] = call.outcomeIndex.to::<u8>();
            ExecutionEffect::TokenBalancesZero {
                contract: address!("006F54F7f9A22e0000CC2AB60031000000ae9fEF"),
                token_ids: vec![U256::from_be_bytes(token)],
            }
        } else if let Ok(call) = IEffectCalls::approveCall::abi_decode(&calldata) {
            ensure!(
                target == address!("2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
                    && call.spender == address!("93070a847efEf7F70739046A929D47a521F5B8ee"),
                "unsupported wallet approval"
            );
            ExecutionEffect::Allowance {
                token: target,
                spender: call.spender,
                minimum: call.amount,
            }
        } else {
            let call = IEffectCalls::wrapCall::abi_decode(&calldata)
                .context("wallet execution requires a supported effect contract")?;
            ensure!(
                target == address!("93070a847efEf7F70739046A929D47a521F5B8ee")
                    && call.to == self.wallet.funder,
                "unsupported wrap target"
            );
            let token = IERC20EffectViews::new(call.asset, self.provider.clone());
            let before =
                tokio::time::timeout(RPC_TIMEOUT, token.balanceOf(self.wallet.funder).call())
                    .await
                    .context("wrap baseline read timed out")?
                    .map_err(|_| anyhow::anyhow!("wrap baseline read failed"))?;
            ExecutionEffect::Wrap {
                token: call.asset,
                before,
                amount: call.amount,
            }
        };
        self.execute_with_effect(target, calldata, effect).await
    }

    pub async fn execute_with_effect(
        &self,
        target: Address,
        calldata: Vec<u8>,
        effect: ExecutionEffect,
    ) -> Result<B256> {
        ensure_mutations_allowed()?;
        ensure!(
            !target.is_zero() && calldata.len() >= 4,
            "invalid wallet execution target or calldata"
        );
        let _gate = self.gate.lock().await;
        let _file_lock = self.journal.lock()?;
        let digest = call_hash(target, &calldata);
        if let Some(mut intent) = self.journal.read()? {
            ensure!(
                intent.wallet == self.wallet.funder && intent.signer == self.wallet.signer,
                "wallet execution journal identity mismatch"
            );
            if intent.unresolved() {
                ensure!(
                    intent.call_hash == digest,
                    "another wallet execution has an unresolved outcome"
                );
                return self.reconcile(&mut intent).await;
            }
        }
        self.verify_identity().await?;
        let (nonce, deadline) = match self.wallet.wallet_type {
            WalletType::GnosisSafe => (None, None),
            WalletType::Poly1271 => {
                let nonce = self
                    .relayer
                    .as_ref()
                    .context("relayer credentials missing")?
                    .nonce(self.wallet.signer)
                    .await?;
                (
                    Some(nonce.to_string()),
                    Some(chrono::Utc::now().timestamp() as u64 + 600),
                )
            }
        };
        let mut intent = Intent {
            wallet: self.wallet.funder,
            signer: self.wallet.signer,
            call_hash: digest,
            effect,
            target,
            calldata: calldata.clone().into(),
            nonce,
            deadline,
            relay_id: None,
            transaction_hash: None,
            state: IntentState::Prepared,
        };
        self.journal.write(&intent)?;
        self.pre_dispatch_result(&mut intent, ensure_mutations_allowed())?;
        match self.wallet.wallet_type {
            WalletType::Poly1271 => {
                let signed_request = async {
                    relayer::sign_batch(
                        &self.signer,
                        self.wallet.funder,
                        U256::from_str(intent.nonce.as_deref().context("intent nonce missing")?)?,
                        intent.deadline.context("intent deadline missing")?,
                        vec![WalletCall {
                            target,
                            value: "0".into(),
                            data: Bytes::from(calldata),
                        }],
                    )
                    .await
                }
                .await;
                let request = self.pre_dispatch_result(&mut intent, signed_request)?;
                self.pre_dispatch_result(&mut intent, ensure_mutations_allowed())?;
                let response = self
                    .relayer
                    .as_ref()
                    .context("relayer credentials missing")?
                    .submit(&request)
                    .await?;
                intent.relay_id = Some(response.id);
                intent.state = IntentState::Submitted;
                self.journal.write(&intent)?;
            }
            WalletType::GnosisSafe => {
                let mut signature = vec![0u8; 12];
                signature.extend_from_slice(self.wallet.signer.as_slice());
                signature.extend_from_slice(&[0u8; 32]);
                signature.push(1);
                let safe = IWalletIdentity::new(self.wallet.funder, self.provider.clone());
                self.pre_dispatch_result(&mut intent, ensure_mutations_allowed())?;
                let pending = tokio::time::timeout(
                    RPC_TIMEOUT,
                    safe.execTransaction(
                        target,
                        U256::ZERO,
                        calldata.into(),
                        0,
                        U256::ZERO,
                        U256::ZERO,
                        U256::ZERO,
                        Address::ZERO,
                        Address::ZERO,
                        signature.into(),
                    )
                    .send(),
                )
                .await
                .context("Safe submission outcome unknown: timeout")?
                .map_err(|_| anyhow::anyhow!("Safe submission outcome unknown: transport"))?;
                intent.transaction_hash = Some(*pending.tx_hash());
                intent.state = IntentState::Submitted;
                self.journal.write(&intent)?;
            }
        }
        self.reconcile(&mut intent).await
    }

    /// Only for failures in the current attempt before its first network submit.
    /// A Prepared intent loaded after restart may already have been submitted
    /// and must continue through reconciliation without this transition.
    fn pre_dispatch_result<T>(&self, intent: &mut Intent, result: Result<T>) -> Result<T> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                ensure!(
                    intent.state == IntentState::Prepared
                        && intent.relay_id.is_none()
                        && intent.transaction_hash.is_none(),
                    "only an unsubmitted current attempt may be released"
                );
                intent.state = IntentState::Failed;
                self.journal.write(intent)?;
                Err(error)
            }
        }
    }

    async fn reconcile(&self, intent: &mut Intent) -> Result<B256> {
        if intent.state == IntentState::ReceiptConfirmed {
            return intent
                .transaction_hash
                .context("confirmed intent has no hash");
        }
        ensure!(
            intent.state != IntentState::Prepared,
            "wallet submission outcome unknown; inspect the durable intent before any retry"
        );
        let result = tokio::time::timeout(EXECUTION_TIMEOUT, async {
            loop {
                if self.wallet.wallet_type == WalletType::Poly1271 {
                    let id = intent
                        .relay_id
                        .as_deref()
                        .context("relayer transaction ID missing; outcome unknown")?;
                    let transaction = self
                        .relayer
                        .as_ref()
                        .context("relayer credentials missing")?
                        .transaction(id)
                        .await?;
                    if matches!(transaction.state.as_str(), "STATE_FAILED" | "STATE_INVALID") {
                        // Preserve the unresolved intent. A failed upstream state
                        // alone is not permission to sign a new operation.
                        bail!("relayer execution failed; intent retained for review");
                    }
                    if transaction.state != "STATE_CONFIRMED" {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    intent.transaction_hash = Some(transaction.confirmed_hash()?);
                    self.journal.write(intent)?;
                }
                let hash = intent
                    .transaction_hash
                    .context("submitted wallet intent has no transaction hash")?;
                let receipt =
                    tokio::time::timeout(RPC_TIMEOUT, self.provider.get_transaction_receipt(hash))
                        .await
                        .context("wallet receipt read timed out")?
                        .map_err(|_| anyhow::anyhow!("wallet receipt read failed"))?;
                if let Some(receipt) = receipt {
                    ensure!(
                        receipt.transaction_hash == hash,
                        "RPC returned another transaction receipt"
                    );
                    let expected_to = match self.wallet.wallet_type {
                        WalletType::GnosisSafe => self.wallet.funder,
                        WalletType::Poly1271 => relayer::DEPOSIT_FACTORY,
                    };
                    ensure!(
                        receipt.to == Some(expected_to),
                        "wallet receipt destination mismatch"
                    );
                    ensure!(
                        receipt.status(),
                        "wallet transaction reverted; intent retained for review"
                    );
                    if self.wallet.wallet_type == WalletType::GnosisSafe {
                        let success = keccak256("ExecutionSuccess(bytes32,uint256)");
                        let failure = keccak256("ExecutionFailure(bytes32,uint256)");
                        let successful = receipt.logs().iter().any(|log| {
                            log.address() == self.wallet.funder && log.topic0() == Some(&success)
                        });
                        let failed = receipt.logs().iter().any(|log| {
                            log.address() == self.wallet.funder && log.topic0() == Some(&failure)
                        });
                        require_safe_success_event(successful, failed)?;
                    }
                    intent.state = IntentState::ReceiptConfirmed;
                    self.journal.write(intent)?;
                    return Ok(hash);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
        .await;
        result.context("wallet execution confirmation timed out; intent retained")?
    }

    pub async fn confirm_effect(&self, hash: B256) -> Result<()> {
        let _gate = self.gate.lock().await;
        let _file_lock = self.journal.lock()?;
        let mut intent = self.journal.read()?.context("wallet intent missing")?;
        ensure!(
            intent.transaction_hash == Some(hash) && intent.state == IntentState::ReceiptConfirmed,
            "wallet effect does not match a confirmed receipt"
        );
        self.verify_effect(&intent.effect).await?;
        intent.state = IntentState::EffectConfirmed;
        self.journal.write(&intent)
    }
}

fn require_safe_success_event(successful: bool, failed: bool) -> Result<()> {
    ensure!(!failed, "Safe inner execution failed");
    ensure!(successful, "Safe success event missing; outcome unresolved");
    Ok(())
}

fn ensure_mutations_allowed() -> Result<()> {
    mutation_mode_allowed(
        crate::helpers::dynamic_config::global_config_tx().is_some(),
        crate::helpers::dynamic_config::ghosting_now(),
        crate::helpers::dynamic_config::read_only_mode(),
    )?;
    crate::helpers::migration::refuse_if_retired()
}

fn mutation_mode_allowed(registered: bool, ghost: bool, read_only: bool) -> Result<()> {
    ensure!(
        registered,
        "wallet mutation blocked before runtime configuration is registered"
    );
    ensure!(
        !ghost,
        "wallet mutation blocked while GHOST mode is enabled"
    );
    ensure!(
        !read_only,
        "wallet mutation blocked while read-only mode is enabled"
    );
    Ok(())
}

fn call_hash(target: Address, data: &[u8]) -> B256 {
    let mut input = target.as_slice().to_vec();
    input.extend_from_slice(data);
    keccak256(input)
}

/// Official canonical owner-bound UUPS and beacon derivations:
/// https://github.com/Polymarket/py-sdk/blob/main/src/polymarket/_internal/wallet.py
pub fn deposit_addresses(owner: Address) -> Result<(Address, Address)> {
    let mut wallet_id = [0u8; 32];
    wallet_id[12..].copy_from_slice(owner.as_slice());
    let args = (relayer::DEPOSIT_FACTORY, B256::from(wallet_id)).abi_encode();
    let salt = keccak256(&args);
    let derive = |beacon: bool| -> Result<Address> {
        let (base, implementation, pieces) = if beacon {
            (
                0x6100523d8160233d3973u128,
                relayer::DEPOSIT_BEACON,
                vec![
                    "60195155f3363d3d373d3d363d602036600436635c60da",
                    "1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6c",
                    "b3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3",
                ],
            )
        } else {
            (
                0x61003d3d8160233d3973u128,
                relayer::DEPOSIT_IMPLEMENTATION,
                vec![
                    "6009",
                    "5155f3363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076",
                    "cc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3",
                ],
            )
        };
        let prefix = base + ((args.len() as u128) << 56);
        let mut bytecode = prefix.to_be_bytes()[6..].to_vec();
        bytecode.extend_from_slice(implementation.as_slice());
        for piece in pieces {
            bytecode.extend_from_slice(&alloy::primitives::hex::decode(piece)?);
        }
        bytecode.extend_from_slice(&args);
        let mut create2 = vec![0xff];
        create2.extend_from_slice(relayer::DEPOSIT_FACTORY.as_slice());
        create2.extend_from_slice(salt.as_slice());
        create2.extend_from_slice(keccak256(bytecode).as_slice());
        Ok(Address::from_slice(&keccak256(create2).as_slice()[12..]))
    };
    Ok((derive(false)?, derive(true)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    #[test]
    fn a_successful_outer_receipt_never_hides_a_failed_safe_inner_call() {
        assert!(require_safe_success_event(true, false).is_ok());
        assert!(require_safe_success_event(false, false).is_err());
        assert!(require_safe_success_event(false, true).is_err());
        assert!(require_safe_success_event(true, true).is_err());
    }

    #[test]
    fn mutations_require_registered_live_writable_mode() {
        assert!(mutation_mode_allowed(true, false, false).is_ok());
        assert!(mutation_mode_allowed(false, false, false).is_err());
        assert!(mutation_mode_allowed(true, true, false).is_err());
        assert!(mutation_mode_allowed(true, false, true).is_err());
    }

    async fn proof_fixture(balance: U256) -> (impl Provider + Clone, tokio::task::JoinHandle<()>) {
        use axum::{routing::post, Json, Router};
        let app = Router::new().route("/", post(move |Json(body): Json<serde_json::Value>| async move {
            assert_eq!(body["method"], "eth_call", "effect recovery must never send a transaction");
            Json(serde_json::json!({"jsonrpc":"2.0","id":body["id"],"result":format!("0x{balance:064x}")}))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let provider = alloy::providers::ProviderBuilder::new().connect_http(url.parse().unwrap());
        (provider, server)
    }

    #[tokio::test]
    async fn known_predispatch_failures_release_the_current_intent_durably() {
        for result in [
            mutation_mode_allowed(true, true, false),
            Err(anyhow::anyhow!("synthetic signing failure")),
        ] {
            let (provider, server) = proof_fixture(U256::ZERO).await;
            let signer = LocalSigner::from_slice(&[1u8; 32]).unwrap();
            let wallet = WalletConfig::resolve(
                signer.address(),
                Some("gnosis_safe"),
                Some("0x2222222222222222222222222222222222222222"),
            )
            .unwrap();
            let path = std::env::temp_dir().join(format!(
                "dradis-predispatch-{}-{}",
                std::process::id(),
                chrono::Utc::now().timestamp_nanos_opt().unwrap()
            ));
            let executor = WalletExecutor {
                provider,
                signer,
                wallet,
                relayer: None,
                relayer_config_blocker: None,
                journal: Journal::new(path.clone(), wallet.funder),
                gate: Mutex::new(()),
            };
            let mut intent = Intent {
                wallet: wallet.funder,
                signer: wallet.signer,
                call_hash: B256::repeat_byte(7),
                effect: ExecutionEffect::TokenBalancesZero {
                    contract: address!("4444444444444444444444444444444444444444"),
                    token_ids: vec![U256::from(17)],
                },
                target: address!("3333333333333333333333333333333333333333"),
                calldata: Bytes::from_static(&[1, 2, 3, 4]),
                nonce: None,
                deadline: None,
                relay_id: None,
                transaction_hash: None,
                state: IntentState::Prepared,
            };
            let file_lock = executor.journal.lock().unwrap();
            executor.journal.write(&intent).unwrap();
            assert!(intent.unresolved());
            assert!(executor.pre_dispatch_result(&mut intent, result).is_err());
            drop(file_lock);
            let recovered = Journal::new(path.clone(), wallet.funder)
                .read()
                .unwrap()
                .unwrap();
            assert_eq!(recovered.state, IntentState::Failed);
            assert!(!recovered.unresolved());
            assert!(executor.live_blockers().is_empty());
            server.abort();
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[tokio::test]
    async fn confirmed_token_burn_recovers_after_restart_without_data_api_rows() {
        for (balance, succeeds) in [(U256::ZERO, true), (U256::from(3), false)] {
            let (provider, server) = proof_fixture(balance).await;
            let signer = LocalSigner::from_slice(&[1u8; 32]).unwrap();
            let wallet = WalletConfig::resolve(
                signer.address(),
                Some("gnosis_safe"),
                Some("0x2222222222222222222222222222222222222222"),
            )
            .unwrap();
            let path = std::env::temp_dir().join(format!(
                "dradis-effect-recovery-{}-{}",
                std::process::id(),
                chrono::Utc::now().timestamp_nanos_opt().unwrap()
            ));
            let journal = Journal::new(path.clone(), wallet.funder);
            let file_lock = journal.lock().unwrap();
            let hash = B256::repeat_byte(5);
            journal
                .write(&Intent {
                    wallet: wallet.funder,
                    signer: wallet.signer,
                    call_hash: B256::repeat_byte(7),
                    effect: ExecutionEffect::TokenBalancesZero {
                        contract: address!("4444444444444444444444444444444444444444"),
                        token_ids: vec![U256::from(17)],
                    },
                    target: address!("3333333333333333333333333333333333333333"),
                    calldata: Bytes::from_static(&[1, 2, 3, 4]),
                    nonce: None,
                    deadline: None,
                    relay_id: None,
                    transaction_hash: Some(hash),
                    state: IntentState::ReceiptConfirmed,
                })
                .unwrap();
            drop(file_lock);
            let executor = WalletExecutor {
                provider,
                signer,
                wallet,
                relayer: None,
                relayer_config_blocker: None,
                journal: Journal::new(path.clone(), wallet.funder),
                gate: Mutex::new(()),
            };
            assert!(!executor.live_blockers().is_empty());
            assert_eq!(executor.reconcile_pending_effect().await.is_ok(), succeeds);
            assert_eq!(
                executor.journal.read().unwrap().unwrap().unresolved(),
                !succeeds
            );
            server.abort();
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[tokio::test]
    async fn lost_submit_response_is_durably_blocked_and_never_resubmitted() {
        let (provider, server) = proof_fixture(U256::ZERO).await;
        let signer = LocalSigner::from_slice(&[1u8; 32]).unwrap();
        let wallet = WalletConfig::resolve(
            signer.address(),
            Some("gnosis_safe"),
            Some("0x2222222222222222222222222222222222222222"),
        )
        .unwrap();
        let path = std::env::temp_dir().join(format!(
            "dradis-lost-submit-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        let journal = Journal::new(path.clone(), wallet.funder);
        let file_lock = journal.lock().unwrap();
        journal
            .write(&Intent {
                wallet: wallet.funder,
                signer: wallet.signer,
                call_hash: B256::repeat_byte(7),
                effect: ExecutionEffect::TokenBalancesZero {
                    contract: address!("4444444444444444444444444444444444444444"),
                    token_ids: vec![U256::from(17)],
                },
                target: address!("3333333333333333333333333333333333333333"),
                calldata: Bytes::from_static(&[1, 2, 3, 4]),
                nonce: None,
                deadline: None,
                relay_id: None,
                transaction_hash: None,
                state: IntentState::Prepared,
            })
            .unwrap();
        drop(file_lock);
        let executor = WalletExecutor {
            provider,
            signer,
            wallet,
            relayer: None,
            relayer_config_blocker: None,
            journal,
            gate: Mutex::new(()),
        };
        assert!(executor.reconcile_pending_effect().await.is_err());
        assert_eq!(
            executor.journal.read().unwrap().unwrap().state,
            IntentState::Prepared
        );
        assert!(!executor.live_blockers().is_empty());
        server.abort();
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn canonical_derivation_matches_official_independent_vectors() {
        let (uups, beacon) =
            deposit_addresses(address!("0000000000000000000000000000000000000001")).unwrap();
        assert_eq!(uups, address!("57ffbc34de23124faeb8387fcd689d314e57accd"));
        assert_eq!(beacon, address!("94bf330955a0b957662feaf878de77bf25f76cd9"));
    }

    #[test]
    fn journal_survives_restart_and_excludes_concurrent_execution() {
        let path = std::env::temp_dir().join(format!(
            "dradis-wallet-journal-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        let journal = Journal::new(path.clone(), Address::ZERO);
        let lock = journal.lock().unwrap();
        assert!(Journal::new(path.clone(), Address::ZERO).lock().is_err());
        let intent = Intent {
            wallet: Address::ZERO,
            signer: Address::ZERO,
            call_hash: B256::ZERO,
            effect: ExecutionEffect::TokenBalancesZero {
                contract: Address::ZERO,
                token_ids: vec![U256::from(1)],
            },
            target: Address::ZERO,
            calldata: Bytes::new(),
            nonce: Some("7".into()),
            deadline: Some(123),
            relay_id: None,
            transaction_hash: None,
            state: IntentState::Prepared,
        };
        journal.write(&intent).unwrap();
        drop(lock);
        assert!(Journal::new(path.clone(), Address::ZERO)
            .read()
            .unwrap()
            .unwrap()
            .unresolved());
        fs::remove_dir_all(path).unwrap();
    }
}
