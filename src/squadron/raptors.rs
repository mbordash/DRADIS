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
    // ── Future Raptors ────────────────────────────────────────────────────────
    // pub sports:   Option<watch::Receiver<SportsSignal>>,
    // pub politics: Option<watch::Receiver<PoliticsSignal>>,
}

impl SquadronRaptors {
    /// Compose a full squadron raptor bundle from all available signal channels.
    pub fn full(
        oracle:   watch::Receiver<Decimal>,
        velocity: watch::Receiver<(Decimal, Decimal, Decimal)>,
        drift:    watch::Receiver<(Decimal, Decimal)>,
        funding:  watch::Receiver<Decimal>,
    ) -> Self {
        Self { oracle, velocity, drift, funding: Some(funding) }
    }

    /// Compose a price-only bundle (no Funding Raptor).
    /// Suitable for momentum-only deployments where funding rate is not consumed.
    pub fn price_only(
        oracle:   watch::Receiver<Decimal>,
        velocity: watch::Receiver<(Decimal, Decimal, Decimal)>,
        drift:    watch::Receiver<(Decimal, Decimal)>,
    ) -> Self {
        Self { oracle, velocity, drift, funding: None }
    }
}

