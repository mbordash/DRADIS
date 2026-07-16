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
/// │ Tide Raptor     │ Binance oracle + IEX │ ETF "Institutional Pulse" + coherence    │
///
/// │ Sports Raptor   │ The Odds API (h2h)   │ line drift, consensus prob, book spread  │
///
/// Future Raptors (not yet implemented)
/// ─────────────────────────────────────
/// │ Politics Raptor │ Polling aggregators  │ approval drift, event probability shifts │
/// │ Horizon Raptor  │ Alpaca IEX WS        │ TradFi velocity (SPY/QQQ), VIX proxy     │
///
/// The Sports and Horizon Raptors are venue-neutral (shared by all pipelines) and,
/// like the Tide Raptor, run observe-only: they publish telemetry but no Viper
/// consumes them for sizing yet.
///
/// When multiple Raptors are active the GBoost and Basis strategies fuse their
/// signals as features — no single Raptor has veto power alone.
pub mod price;
pub mod funding;
pub mod derivatives;
pub mod tide;
pub mod sports;
pub mod horizon;
