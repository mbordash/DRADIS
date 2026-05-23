/// Raptor recon layer — lightweight signal scouts that fly ahead of the Vipers
/// and report external intelligence back to the CIC.
///
/// Each Raptor polls a specific external data source on its own schedule and
/// publishes a normalised signal the Viper strategies consume via `watch` channels.
/// Raptors are intentionally dumb: they fetch, normalise, and broadcast — no
/// trading logic, no position awareness, no side effects.
///
/// Current Raptors
/// ───────────────
/// │ Raptor          │ Source               │ Signal                                   │
/// │─────────────────│──────────────────────│──────────────────────────────────────────│
/// │ Price Raptor    │ Binance Spot WS      │ spot price, 5s/1s velocity, accel, drift │
/// │ Funding Raptor  │ Binance FAPI REST    │ perpetual funding rate (smart-money)     │
///
/// Future Raptors (not yet implemented)
/// ─────────────────────────────────────
/// │ Sports Raptor   │ Line-movement APIs   │ betting-line drift, public money %       │
/// │ Politics Raptor │ Polling aggregators  │ approval drift, event probability shifts │
///
/// When multiple Raptors are active the GBoost and Basis strategies fuse their
/// signals as features — no single Raptor has veto power alone.
pub mod price;
pub mod funding;
