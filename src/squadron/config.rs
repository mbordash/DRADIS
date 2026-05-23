/// Squadron composition configuration.
///
/// Describes which Raptors and Vipers are assembled for a given squadron
/// deployment.  The CAG (Phase 3) will use this to assemble the right
/// set of signal sources and strategies for each battle location.

/// Which Raptor signal sources are active for this squadron.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaptorProfile {
    /// Price Raptor only (spot price, velocity, drift).
    /// Suitable for momentum-only deployments.
    PriceOnly,

    /// Price Raptor + Funding Raptor (adds smart-money sentiment).
    /// Required for Basis and GBoost Vipers.
    PriceAndFunding,

    /// All available Raptors (current + any future scouts).
    Full,
}

/// Which Vipers are deployed in this squadron.
#[derive(Debug, Clone)]
pub enum ViperProfile {
    /// Every registered Viper flies (current default behaviour).
    Full,

    /// Only the named Vipers fly.  Useful for targeted single-strategy runs
    /// or A/B testing a new Viper without the full wing.
    Custom(Vec<String>),
}

/// Full composition spec for a squadron.
#[derive(Debug, Clone)]
pub struct SquadronConfig {
    /// Human-readable name, e.g. "BTC Full Wing" or "BTC Momentum Only".
    pub name: String,

    /// Which Raptors provide signals.
    pub raptor_profile: RaptorProfile,

    /// Which Vipers execute trades.
    pub viper_profile: ViperProfile,
}

impl SquadronConfig {
    /// Standard full-wing config: all Raptors + all Vipers.
    /// This mirrors the current hardcoded DRADIS behaviour.
    pub fn full_wing(name: impl Into<String>) -> Self {
        Self {
            name:           name.into(),
            raptor_profile: RaptorProfile::Full,
            viper_profile:  ViperProfile::Full,
        }
    }

    /// Momentum-only config: Price Raptor + Momentum + GBoost Vipers.
    pub fn momentum_only(name: impl Into<String>) -> Self {
        Self {
            name:           name.into(),
            raptor_profile: RaptorProfile::PriceOnly,
            viper_profile:  ViperProfile::Custom(vec![
                "MomentumStrategy".into(),
                "GboostStrategy".into(),
            ]),
        }
    }

    /// Arbitrage config: Price + Funding Raptors + Arb + Basis Vipers.
    pub fn arb_wing(name: impl Into<String>) -> Self {
        Self {
            name:           name.into(),
            raptor_profile: RaptorProfile::PriceAndFunding,
            viper_profile:  ViperProfile::Custom(vec![
                "ArbitrageStrategy".into(),
                "BasisStrategy".into(),
            ]),
        }
    }
}

