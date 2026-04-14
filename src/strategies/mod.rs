// Strategy modules for RustPolyBot
//
// Architecture:
// - OLD MODULES (helpers only, not used for trading):
//   - momentum, arbitrage, time_decay (contain pure calculation helpers)
// - NEW MODULES (orchestrator-based):
//   - momentum_impl, arbitrage_impl, time_decay_impl (implement Strategy trait)
//
// During transition: old modules are kept as helper libraries
// After migration: will be refactored into utils or inlined into _impl files

pub mod momentum;
pub mod momentum_impl;
pub mod arbitrage;
pub mod arbitrage_impl;
pub mod time_decay;
pub mod time_decay_impl;

