/// Background task modules.
///
/// Each module exposes a single `pub async fn run_*` entry point that is
/// `tokio::spawn`-ed from `main.rs`.  All long-running loops, shared-state
/// mutations, and side-effecting async work live here — keeping `main.rs`
/// as pure orchestration/wiring.
pub mod oracle;
pub mod funding;
pub mod market_monitor;
pub mod cleanup;
pub mod merge;