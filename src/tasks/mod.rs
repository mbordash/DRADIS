/// Background task modules.
///
/// Each module exposes a single `pub async fn run_*` entry point that is
/// `tokio::spawn`-ed from `main.rs`.  All long-running loops, shared-state
/// mutations, and side-effecting async work live here — keeping `main.rs`
/// as pure orchestration/wiring.
///
/// Note: Binance price and funding rate tasks have moved to `crate::raptors`
/// as part of the Raptor recon-layer separation of concerns.
pub mod market_monitor;
pub mod cleanup;
