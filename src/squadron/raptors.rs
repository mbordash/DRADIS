/// SquadronRaptors — the eyes and ears of a deployed squadron.
///
/// Bundles all Raptor signal receivers assigned to a squadron.  Constructed
/// by the CIC (main.rs) from the global Raptor watch channels and handed to
/// each squadron at deployment time.
///
/// Raptors are spawned once per asset (Price, Funding) and their watch
/// channels are cloned cheaply into each squadron that needs them — a Raptor
/// is never duplicated just because two squadrons target the same asset.
///
/// Optional Raptors (funding, and future sports/politics) are `Option<_>` so
/// that a squadron can be assembled with only the signals it actually needs.
use rust_decimal::Decimal;
use tokio::sync::watch;

use crate::raptors::derivatives::DerivativesSnapshot;
use crate::raptors::tide::TideSnapshot;
use crate::raptors::sports::SportsSnapshot;

/// All Raptor signal receivers available to a squadron.
pub struct SquadronRaptors {
    /// Current spot price from the Price Raptor (Binance WS).
    pub oracle: watch::Receiver<Decimal>,

    /// (5s velocity, 1s velocity, acceleration) from the Price Raptor.
    pub velocity: watch::Receiver<(Decimal, Decimal, Decimal)>,

    /// (60-min drift, 10-min drift) from the Price Raptor.
    pub drift: watch::Receiver<(Decimal, Decimal)>,

    /// Perpetual funding rate from the Funding Raptor (Binance FAPI).
    /// `None` when the Funding Raptor is not deployed for this squadron
    /// (e.g. a pure momentum run that doesn't need smart-money sentiment).
    pub funding: Option<watch::Receiver<Decimal>>,

    /// Open-interest / taker-flow snapshot from the Derivatives Raptor
    /// (Binance FAPI). A *macro* signal — the Viper fuses it with the fast
    /// price/velocity micro signals to read 10-minute regime shifts. `None`
    /// when the Derivatives Raptor is not deployed for this squadron.
    pub derivatives: Option<watch::Receiver<DerivativesSnapshot>>,

    /// Institutional-tide snapshot from the Tide Raptor (synthetic iNAV vs IEX
    /// ETF prints). A *macro / observe-only* signal — BTC-only, so `None` for
    /// ETH/SOL squadrons and for momentum-only deployments. Not yet consumed by
    /// Viper sizing (telemetry observation phase).
    pub tide: Option<watch::Receiver<TideSnapshot>>,

    /// Line-movement / consensus-probability snapshot from the Sports Raptor
    /// (The Odds API). A *macro / observe-only* signal shared by the US and intl
    /// pipelines — not yet consumed by Viper sizing (telemetry observation phase).
    /// `None` when the Sports Raptor is not deployed for this squadron.
    pub sports: Option<watch::Receiver<SportsSnapshot>>,
    // ── Future Raptors ────────────────────────────────────────────────────────
    // pub politics: Option<watch::Receiver<PoliticsSignal>>,
}

impl SquadronRaptors {
    /// Compose a full squadron raptor bundle from all available signal channels.
    /// `tide` and `sports` are optional — `tide` is BTC-only; `sports` is present
    /// only when the venue-neutral Sports Raptor is deployed.
    pub fn full(
        oracle:      watch::Receiver<Decimal>,
        velocity:    watch::Receiver<(Decimal, Decimal, Decimal)>,
        drift:       watch::Receiver<(Decimal, Decimal)>,
        funding:     watch::Receiver<Decimal>,
        derivatives: watch::Receiver<DerivativesSnapshot>,
        tide:        Option<watch::Receiver<TideSnapshot>>,
        sports:      Option<watch::Receiver<SportsSnapshot>>,
    ) -> Self {
        Self {
            oracle,
            velocity,
            drift,
            funding: Some(funding),
            derivatives: Some(derivatives),
            tide,
            sports,
        }
    }

    /// Compose a price-only bundle (no Funding / Derivatives / Tide / Sports Raptor).
    /// Suitable for momentum-only deployments where macro signals are not consumed.
    pub fn price_only(
        oracle:   watch::Receiver<Decimal>,
        velocity: watch::Receiver<(Decimal, Decimal, Decimal)>,
        drift:    watch::Receiver<(Decimal, Decimal)>,
    ) -> Self {
        Self { oracle, velocity, drift, funding: None, derivatives: None, tide: None, sports: None }
    }
}

