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

//! Sweep stranded settlement proceeds back into tradeable collateral.
//!
//! # The defect this closes
//!
//! Polymarket's CLOB trades pUSD, and the collateral figure DRADIS reports (the
//! status task's `balance_allowance(Collateral)`, every `pnl_snapshots` row, the
//! Control Tower's "Cash") is the Safe's on-chain pUSD balance — verified to the
//! micro-dollar against `pUSD.balanceOf(safe)` on 2026-09-05. Redeeming a
//! resolved position pays out in whatever token the condition was minted with,
//! and the venue's crypto hourlies are still minted with USDC.e. So when
//! `auto_settle_closed_positions` correctly detected a USDC.e condition and
//! redeemed it through the Safe (tx `0x54d72b89…`, block 93294170), the CTF paid
//! 5.053757 USDC.e into the Safe, where it sat beside 49.379608 pUSD. The trade
//! row booked +$0.33, the CLOB balance never moved, the Control Tower showed
//! the cash gone, and nothing in DRADIS could trade it. Every such settlement
//! shrinks the tradeable balance by the entire payout until someone wraps it.
//!
//! # What the sweep does
//!
//! Each settlement cycle it reads the Safe's USDC.e balance (the "stranded"
//! figure, published for the log and `/api/portfolio` whether or not the sweep
//! is enabled). When the operator has enabled the sweep and the balance clears
//! the minimum, it approves Polymarket's permissionless `CollateralOnramp` for
//! exactly that amount and calls `wrap(USDC.e, safe, amount)`, both as Safe
//! `execTransaction`s signed by the owning EOA, then re-reads the balance and
//! reports success only if the USDC.e actually left.
//!
//! # Why it cannot loop or double-spend
//!
//! Every cycle starts from chain state, never from memory: a sweep that was
//! cut off after its approve is picked up by the allowance check next cycle,
//! one cut off after its wrap finds nothing left to sweep. The approve is for
//! the exact amount, never unlimited. Any failure — RPC, revert, or a wrap that
//! "succeeded" without moving the balance — puts the sweep on a 30-minute
//! cooldown so a broken on-ramp costs at most two transactions of gas per half
//! hour. A global lock keeps the per-squadron settlement tasks from running two
//! sweeps at once, and the on-ramp is asked for its collateral token before
//! every sweep so a wrong address cannot be handed an approval.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};

use alloy::primitives::{Address, U256, address as alloy_address};
use alloy::providers::Provider;
use alloy::sol;
use alloy::sol_types::SolCall;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::config;
use crate::tasks::cleanup::{execute_via_safe_confirmed, PUSD_COLLATERAL, USDCE_COLLATERAL};

/// Polymarket's permissionless USDC/USDC.e → pUSD on-ramp on Polygon.
///
/// Documented at <https://docs.polymarket.com/concepts/pusd> and verified
/// on-chain on 2026-09-05: its `COLLATERAL_TOKEN()` returns the pUSD address
/// and its bytecode carries `wrap(address,address,uint256)`. The sweep asks it
/// for `COLLATERAL_TOKEN()` again before every run rather than trusting this
/// constant.
pub const COLLATERAL_ONRAMP: Address = alloy_address!("0x93070a847efEf7F70739046A929D47a521F5B8ee");

/// How long the sweep stands down after any failure. A bare constant rather
/// than a knob: it is a safety backoff, not something to tune.
const FAILURE_COOLDOWN_SECS: i64 = 30 * 60;

/// Per-call cap on the view calls. The Safe transactions carry their own
/// 30-second receipt cap inside `execute_via_safe_confirmed`.
const VIEW_TIMEOUT_SECS: u64 = 10;

sol! {
    /// The two ERC-20 views and the one call the sweep needs.
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
    }

    /// Polymarket `CollateralOnramp` (ctf-exchange-v2 `src/collateral/CollateralOnramp.sol`).
    ///
    /// `wrap` pulls `amount` of `asset` from `msg.sender` (the Safe, via
    /// `execTransaction`) into the pUSD contract and mints pUSD to `to`. It
    /// reverts while `asset` is paused.
    #[sol(rpc)]
    interface ICollateralOnramp {
        function COLLATERAL_TOKEN() external view returns (address);
        function paused(address asset) external view returns (bool);
        function wrap(address asset, address to, uint256 amount) external;
    }
}

/// One sweep at a time across every squadron's settlement task.
static SWEEP_RUN_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// When the last sweep attempt failed, for the cooldown.
static LAST_FAILURE: std::sync::Mutex<Option<DateTime<Utc>>> = std::sync::Mutex::new(None);

/// Last observed stranded USDC.e in the Safe, for `/api/portfolio`.
static STRANDED_USDCE: std::sync::Mutex<Option<Decimal>> = std::sync::Mutex::new(None);

/// Set after a confirmed sweep so the status task asks the CLOB to refresh its
/// cached balance before its next read.
static CLOB_REFRESH_NEEDED: AtomicBool = AtomicBool::new(false);

/// The Safe's USDC.e balance as of the last settlement cycle, or `None` before
/// the first read (and after a failed one, the previous reading stands).
pub fn stranded_usdce() -> Option<Decimal> {
    *STRANDED_USDCE.lock().unwrap_or_else(|e| e.into_inner())
}

/// Consume the "a sweep just landed" flag. True at most once per sweep.
pub fn take_clob_refresh_needed() -> bool {
    CLOB_REFRESH_NEEDED.swap(false, Ordering::AcqRel)
}

/// What the sweep should do with an observed stranded balance.
#[derive(Debug, PartialEq, Eq)]
pub enum SweepDecision {
    /// Nothing is stranded.
    Nothing,
    /// Money is stranded and the operator has not enabled the sweep.
    Disabled,
    /// Stranded, enabled, but under the minimum worth a transaction.
    BelowMinimum,
    /// A recent attempt failed; wait it out.
    CoolingDown,
    /// Sweep the full balance.
    Sweep,
}

/// The pure decision, separated so it can be tested without a chain.
pub fn sweep_decision(
    enabled: bool,
    stranded: Decimal,
    min_usdc: Decimal,
    last_failure: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> SweepDecision {
    if stranded <= Decimal::ZERO {
        return SweepDecision::Nothing;
    }
    if !enabled {
        return SweepDecision::Disabled;
    }
    // A zero or negative minimum would sweep dust for gas; one cent is the floor.
    let floor = min_usdc.max(Decimal::new(1, 2));
    if stranded < floor {
        return SweepDecision::BelowMinimum;
    }
    if let Some(failed_at) = last_failure {
        if (now - failed_at).num_seconds() < FAILURE_COOLDOWN_SECS {
            return SweepDecision::CoolingDown;
        }
    }
    SweepDecision::Sweep
}

/// Six-decimal token base units to dollars.
pub fn base_units_to_decimal(raw: U256) -> Decimal {
    raw.to_string()
        .parse::<Decimal>()
        .map(|d| d / Decimal::from(1_000_000u32))
        .unwrap_or(Decimal::ZERO)
}

/// The operator's live settings, falling back to the compile-time defaults
/// before the global config channel is published.
fn knobs() -> (bool, Decimal) {
    crate::helpers::dynamic_config::global_config_tx()
        .map(|tx| {
            let c = tx.borrow();
            (c.collateral_sweep_enabled, c.collateral_sweep_min_usdc)
        })
        .unwrap_or((config::COLLATERAL_SWEEP_ENABLED, config::COLLATERAL_SWEEP_MIN_USDC))
}

fn record_failure() {
    *LAST_FAILURE.lock().unwrap_or_else(|e| e.into_inner()) = Some(Utc::now());
}

fn publish_stranded(v: Decimal) {
    *STRANDED_USDCE.lock().unwrap_or_else(|e| e.into_inner()) = Some(v);
}

/// Observe the Safe's stranded USDC.e and, when enabled, wrap it into pUSD.
///
/// Runs from each squadron's settlement task right after `auto_settle_closed_positions`,
/// so a redemption's payout is seen in the same cycle that produced it. Safe
/// to call concurrently: a second caller finds the lock held and returns.
pub async fn sweep_stranded_collateral<P: Provider + Clone>(
    provider: P,
    safe_address: Address,
    eoa_address: Address,
) {
    crate::helpers::watchdog::enter(crate::helpers::watchdog::Phase::Settlement);
    let _guard = match SWEEP_RUN_LOCK.try_lock() {
        Ok(g) => g,
        Err(_) => {
            debug!("Collateral sweep: another run is in progress; skipping this cycle");
            return;
        }
    };

    let usdce = IERC20::new(USDCE_COLLATERAL, provider.clone());
    let raw = match timeout(
        std::time::Duration::from_secs(VIEW_TIMEOUT_SECS),
        usdce.balanceOf(safe_address).call(),
    ).await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => { warn!("Collateral sweep: USDC.e balanceOf failed: {}", e); return; }
        Err(_)    => { warn!("Collateral sweep: USDC.e balanceOf timed out ({}s)", VIEW_TIMEOUT_SECS); return; }
    };
    let stranded = base_units_to_decimal(raw);
    publish_stranded(stranded);

    let (enabled, min_usdc) = knobs();
    let last_failure = *LAST_FAILURE.lock().unwrap_or_else(|e| e.into_inner());
    match sweep_decision(enabled, stranded, min_usdc, last_failure, Utc::now()) {
        SweepDecision::Nothing => return,
        SweepDecision::Disabled => {
            info!(
                "💤 Collateral sweep: ${:.4} USDC.e is sitting in the Safe outside tradeable pUSD \
                 (settlement proceeds). Sweep is off — enable Collateral Sweep in Setup to wrap it.",
                stranded
            );
            return;
        }
        SweepDecision::BelowMinimum => {
            debug!("Collateral sweep: ${:.4} USDC.e stranded is under the ${:.2} minimum", stranded, min_usdc);
            return;
        }
        SweepDecision::CoolingDown => {
            debug!("Collateral sweep: ${:.4} USDC.e stranded; cooling down after a failed attempt", stranded);
            return;
        }
        SweepDecision::Sweep => {}
    }

    info!(
        "🧹 Collateral sweep: wrapping ${:.4} USDC.e in the Safe into pUSD via CollateralOnramp {}",
        stranded, COLLATERAL_ONRAMP
    );

    // The on-ramp must vouch for itself before it is handed an approval.
    let onramp = ICollateralOnramp::new(COLLATERAL_ONRAMP, provider.clone());
    match timeout(std::time::Duration::from_secs(VIEW_TIMEOUT_SECS), onramp.COLLATERAL_TOKEN().call()).await {
        Ok(Ok(token)) if token == PUSD_COLLATERAL => {}
        Ok(Ok(token)) => {
            warn!(
                "Collateral sweep: on-ramp {} reports collateral token {} (expected pUSD {}) — refusing to sweep",
                COLLATERAL_ONRAMP, token, PUSD_COLLATERAL
            );
            record_failure();
            return;
        }
        Ok(Err(e)) => { warn!("Collateral sweep: COLLATERAL_TOKEN() failed: {}", e); record_failure(); return; }
        Err(_)    => { warn!("Collateral sweep: COLLATERAL_TOKEN() timed out"); record_failure(); return; }
    }
    // A paused asset would make the wrap revert; ask first and save the gas.
    // An error here is not fatal — the wrap carries the same check on-chain.
    if let Ok(Ok(true)) = timeout(
        std::time::Duration::from_secs(VIEW_TIMEOUT_SECS),
        onramp.paused(USDCE_COLLATERAL).call(),
    ).await {
        warn!("Collateral sweep: the on-ramp has USDC.e wrapping paused — will retry after cooldown");
        record_failure();
        return;
    }

    // Approve exactly the stranded amount, only when the standing allowance
    // does not already cover it (a sweep cut off after its approve resumes here).
    let allowance = match timeout(
        std::time::Duration::from_secs(VIEW_TIMEOUT_SECS),
        usdce.allowance(safe_address, COLLATERAL_ONRAMP).call(),
    ).await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => { warn!("Collateral sweep: allowance() failed: {}", e); record_failure(); return; }
        Err(_)    => { warn!("Collateral sweep: allowance() timed out"); record_failure(); return; }
    };
    if allowance < raw {
        let calldata = IERC20::approveCall { spender: COLLATERAL_ONRAMP, amount: raw }.abi_encode();
        match execute_via_safe_confirmed(provider.clone(), safe_address, eoa_address, USDCE_COLLATERAL, calldata).await {
            Ok(tx) => info!("Collateral sweep: approved on-ramp for ${:.4} USDC.e (tx {})", stranded, tx),
            Err(e) => {
                warn!("Collateral sweep: approve failed: {}", e);
                record_failure();
                return;
            }
        }
    } else {
        debug!("Collateral sweep: existing allowance covers the sweep; skipping approve");
    }

    let calldata = ICollateralOnramp::wrapCall { asset: USDCE_COLLATERAL, to: safe_address, amount: raw }.abi_encode();
    let wrap_tx = match execute_via_safe_confirmed(provider.clone(), safe_address, eoa_address, COLLATERAL_ONRAMP, calldata).await {
        Ok(tx) => tx,
        Err(e) => {
            warn!("Collateral sweep: wrap failed: {}", e);
            record_failure();
            return;
        }
    };

    // Success is the balance moving, not the transaction landing.
    let after = match timeout(
        std::time::Duration::from_secs(VIEW_TIMEOUT_SECS),
        usdce.balanceOf(safe_address).call(),
    ).await {
        Ok(Ok(v)) => v,
        _ => {
            // The wrap is confirmed on-chain; only the read-back failed. Leave
            // the stranded figure as it was — the next cycle re-reads it — and
            // do not count this as a failed sweep.
            warn!("Collateral sweep: wrap tx {} confirmed but the USDC.e read-back failed; next cycle will verify", wrap_tx);
            CLOB_REFRESH_NEEDED.store(true, Ordering::Release);
            return;
        }
    };
    if after >= raw {
        warn!(
            "Collateral sweep: wrap tx {} confirmed but the Safe still holds ${:.4} USDC.e — treating as a failed sweep",
            wrap_tx, base_units_to_decimal(after)
        );
        record_failure();
        return;
    }
    let after_dec = base_units_to_decimal(after);
    publish_stranded(after_dec);
    let pusd_now = match timeout(
        std::time::Duration::from_secs(VIEW_TIMEOUT_SECS),
        IERC20::new(PUSD_COLLATERAL, provider.clone()).balanceOf(safe_address).call(),
    ).await {
        Ok(Ok(v)) => format!("${:.4}", base_units_to_decimal(v)),
        _ => "unread".to_string(),
    };
    *LAST_FAILURE.lock().unwrap_or_else(|e| e.into_inner()) = None;
    CLOB_REFRESH_NEEDED.store(true, Ordering::Release);
    info!(
        "✅ Collateral sweep: wrapped ${:.4} USDC.e into pUSD (tx {}) | Safe USDC.e ${:.4} → ${:.4} | Safe pUSD now {}",
        stranded - after_dec, wrap_tx, stranded, after_dec, pusd_now
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn at(secs_ago: i64, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        Some(now - chrono::Duration::seconds(secs_ago))
    }

    #[test]
    fn nothing_stranded_is_nothing_to_do_even_when_enabled() {
        let now = Utc::now();
        assert_eq!(sweep_decision(true, dec!(0), dec!(1), None, now), SweepDecision::Nothing);
    }

    #[test]
    fn disabled_reports_but_never_sweeps() {
        let now = Utc::now();
        assert_eq!(sweep_decision(false, dec!(5.053757), dec!(1), None, now), SweepDecision::Disabled);
    }

    #[test]
    fn the_observed_payout_is_swept_when_enabled() {
        let now = Utc::now();
        assert_eq!(sweep_decision(true, dec!(5.053757), dec!(1), None, now), SweepDecision::Sweep);
    }

    #[test]
    fn dust_under_the_minimum_is_left_alone() {
        let now = Utc::now();
        assert_eq!(sweep_decision(true, dec!(0.40), dec!(1), None, now), SweepDecision::BelowMinimum);
        // The minimum cannot be configured below one cent.
        assert_eq!(sweep_decision(true, dec!(0.005), dec!(0), None, now), SweepDecision::BelowMinimum);
    }

    #[test]
    fn a_recent_failure_holds_the_sweep_and_an_old_one_does_not() {
        let now = Utc::now();
        assert_eq!(
            sweep_decision(true, dec!(5), dec!(1), at(FAILURE_COOLDOWN_SECS - 1, now), now),
            SweepDecision::CoolingDown
        );
        assert_eq!(
            sweep_decision(true, dec!(5), dec!(1), at(FAILURE_COOLDOWN_SECS + 1, now), now),
            SweepDecision::Sweep
        );
    }

    #[test]
    fn base_units_convert_to_six_decimal_dollars() {
        // The exact payout observed in the Safe on 2026-09-05.
        assert_eq!(base_units_to_decimal(U256::from(5_053_757u64)), dec!(5.053757));
        assert_eq!(base_units_to_decimal(U256::ZERO), dec!(0));
    }

    /// The selectors were read out of the deployed on-ramp's bytecode on
    /// 2026-09-05; a typo in the interface would silently call nothing.
    #[test]
    fn interface_selectors_match_the_deployed_onramp() {
        assert_eq!(ICollateralOnramp::wrapCall::SELECTOR, [0x62, 0x35, 0x56, 0x38]);
        assert_eq!(ICollateralOnramp::COLLATERAL_TOKENCall::SELECTOR, [0xf5, 0xf1, 0xf1, 0xa7]);
        assert_eq!(ICollateralOnramp::pausedCall::SELECTOR, [0x2e, 0x48, 0x15, 0x2c]);
        assert_eq!(IERC20::approveCall::SELECTOR, [0x09, 0x5e, 0xa7, 0xb3]);
        assert_eq!(IERC20::allowanceCall::SELECTOR, [0xdd, 0x62, 0xed, 0x3e]);
    }

    #[test]
    fn wrap_calldata_targets_usdce_and_pays_the_safe() {
        let safe = alloy_address!("0x3A4d0004Ad7a5ff5805F6C54eE730E4B23836f30");
        let data = ICollateralOnramp::wrapCall { asset: USDCE_COLLATERAL, to: safe, amount: U256::from(5_053_757u64) }.abi_encode();
        let decoded = ICollateralOnramp::wrapCall::abi_decode(&data).expect("round-trips");
        assert_eq!(decoded.asset, USDCE_COLLATERAL);
        assert_eq!(decoded.to, safe);
        assert_eq!(decoded.amount, U256::from(5_053_757u64));
    }
}
