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
use crate::raptors::tennis::TennisSnapshot;
use crate::raptors::horizon::HorizonSnapshot;

/// All Raptor signal receivers available to a squadron.
pub struct SquadronRaptors {
    /// Current spot price from the Price Raptor (Binance WS).
    pub oracle: watch::Receiver<Decimal>,

    /// (5s velocity, 1s velocity, acceleration) from the Price Raptor.
    pub velocity: watch::Receiver<(Decimal, Decimal, Decimal)>,

    /// (60-min drift, 10-min drift, 60-min normalized realized-vol) from the Price Raptor.
    pub drift: watch::Receiver<(Decimal, Decimal, Decimal)>,

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

    /// TradFi velocity / VIX proxy snapshot from the Horizon Raptor (SPY/QQQ/UVXY
    /// via Alpaca IEX). A *macro / observe-only* signal shared by all squadrons.
    /// `None` when the Horizon Raptor is not deployed.
    pub horizon: Option<watch::Receiver<HorizonSnapshot>>,

    /// Line-movement / consensus-probability snapshot from the Sports Raptor
    /// (The Odds API). A *macro / observe-only* signal shared by the US and intl
    /// pipelines — not yet consumed by Viper sizing (telemetry observation phase).
    /// `None` when the Sports Raptor is not deployed for this squadron.
    pub sports: Option<watch::Receiver<SportsSnapshot>>,

    /// Live tennis event-state snapshot from the Tennis Raptor (Live Tennis
    /// API). A *macro / observe-only* signal shared by all pipelines — not yet
    /// consumed by Viper sizing (telemetry observation phase). `None` when the
    /// Tennis Raptor is not deployed for this squadron. Attached after
    /// construction (`raptors.tennis = Some(rx)`), the same way the US general
    /// wing attaches its sports feed, so the constructor signatures stay stable.
    pub tennis: Option<watch::Receiver<TennisSnapshot>>,

    /// Whether `oracle`, `velocity` and `drift` are fed by a real Price Raptor.
    ///
    /// Those three are not `Option` like the rest, because every consumer reads
    /// them unconditionally. A squadron with no Price Raptor is handed
    /// placeholder channels whose senders are dropped at construction, so they
    /// read a permanent zero. That is fine for the vipers, which gate on the
    /// signals derived from them, but not for anything that REPORTS the value:
    /// the 60s heartbeat printed `Binance: $0.00` on every politics and sports
    /// squadron, which is indistinguishable from a Price Raptor that has died.
    ///
    /// A bool rather than inferring from a zero reading, because a genuinely
    /// broken Binance feed also reads zero and must keep looking broken.
    pub has_price_feed: bool,
    // ── Future Raptors ────────────────────────────────────────────────────────
    // pub politics: Option<watch::Receiver<PoliticsSignal>>,
}

impl SquadronRaptors {
    /// Compose a full squadron raptor bundle from all available signal channels.
    /// `tide`, `horizon`, and `sports` are optional — `tide` is BTC-only; `horizon`
    /// and `sports` require their respective Alpaca/Odds API keys.
    pub fn full(
        oracle:      watch::Receiver<Decimal>,
        velocity:    watch::Receiver<(Decimal, Decimal, Decimal)>,
        drift:       watch::Receiver<(Decimal, Decimal, Decimal)>,
        funding:     watch::Receiver<Decimal>,
        derivatives: watch::Receiver<DerivativesSnapshot>,
        tide:        Option<watch::Receiver<TideSnapshot>>,
        horizon:     Option<watch::Receiver<HorizonSnapshot>>,
        sports:      Option<watch::Receiver<SportsSnapshot>>,
    ) -> Self {
        Self {
            oracle,
            velocity,
            drift,
            funding: Some(funding),
            derivatives: Some(derivatives),
            tide,
            horizon,
            sports,
            tennis: None,
            has_price_feed: true,
        }
    }

    /// Compose a price-only bundle (no Funding / Derivatives / Tide / Horizon / Sports Raptor).
    /// Suitable for momentum-only deployments where macro signals are not consumed.
    pub fn price_only(
        oracle:   watch::Receiver<Decimal>,
        velocity: watch::Receiver<(Decimal, Decimal, Decimal)>,
        drift:    watch::Receiver<(Decimal, Decimal, Decimal)>,
    ) -> Self {
        Self { oracle, velocity, drift, funding: None, derivatives: None, tide: None, horizon: None, sports: None, tennis: None, has_price_feed: true }
    }

    /// Compose a sports-only bundle for Admiral Adama sports market squadrons.
    /// Uses placeholder channels for price signals (sports markets don't use crypto oracles).
    pub fn sports_only(sports: SportsRaptorHandle) -> Self {
        let (_, oracle_rx) = watch::channel(Decimal::ZERO);
        let (_, velocity_rx) = watch::channel((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
        let (_, drift_rx) = watch::channel((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
        Self {
            oracle: oracle_rx,
            velocity: velocity_rx,
            drift: drift_rx,
            funding: None,
            derivatives: None,
            tide: None,
            horizon: None,
            sports: Some(sports),
            tennis: None,
            // Placeholder channels above; there is no Price Raptor here.
            has_price_feed: false,
        }
    }

    /// Create an empty raptor bundle with placeholder channels.
    /// Used for market types that don't have implemented raptors yet (e.g. politics).
    pub fn empty() -> Self {
        let (_, oracle_rx) = watch::channel(Decimal::ZERO);
        let (_, velocity_rx) = watch::channel((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
        let (_, drift_rx) = watch::channel((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
        Self {
            oracle: oracle_rx,
            velocity: velocity_rx,
            drift: drift_rx,
            funding: None,
            derivatives: None,
            tide: None,
            horizon: None,
            sports: None,
            tennis: None,
            // Placeholder channels above; there is no Price Raptor here.
            has_price_feed: false,
        }
    }
}

/// Handle to a Sports Raptor signal channel — cloneable for sharing across squadrons.
pub type SportsRaptorHandle = watch::Receiver<SportsSnapshot>;


#[cfg(test)]
mod price_feed_tests {
    use super::*;

    /// `has_price_feed` must describe the channels the constructor actually
    /// installed. It is a hand-maintained claim about `oracle`/`velocity`/
    /// `drift`, so a new constructor that forgets it, or one that starts handing
    /// out placeholders, silently makes the heartbeat lie again.
    #[test]
    fn placeholder_bundles_report_no_price_feed() {
        assert!(!SquadronRaptors::empty().has_price_feed);

        let (_tx, sports_rx) = watch::channel(SportsSnapshot::default());
        assert!(!SquadronRaptors::sports_only(sports_rx).has_price_feed);
    }

    #[test]
    fn price_only_reports_a_price_feed() {
        let (_o, oracle) = watch::channel(Decimal::ZERO);
        let (_v, velocity) = watch::channel((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
        let (_d, drift) = watch::channel((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
        assert!(SquadronRaptors::price_only(oracle, velocity, drift).has_price_feed);
    }

    /// A placeholder bundle reads a permanent zero, which is exactly the value a
    /// dead Price Raptor reports. The flag is what tells the two apart, so it
    /// must not be inferred from the reading.
    #[test]
    fn a_placeholder_oracle_reads_zero_like_a_dead_feed_would() {
        let r = SquadronRaptors::empty();
        assert_eq!(*r.oracle.borrow(), Decimal::ZERO);
        assert!(!r.has_price_feed);
    }
}
