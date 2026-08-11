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

//! Shared realized-volatility helpers.
//!
//! Centralizes the oracle-volatility math so every consumer (the GBoost
//! flatness gate, the Price raptor's periodic telemetry, and any future viper
//! that wants to self-gate on choppiness) computes it identically.

/// Normalized historical volatility of a price series, on the same scale as
/// GBoost's `GBOOST_MIN_HIST_VOL` gate: the standard deviation of consecutive
/// log-returns divided by 0.020 (a 2%-per-tick std-dev maps to 1.0), capped at
/// 1.0. Returns 0.0 for fewer than 5 samples or when no valid returns exist.
pub fn normalized_hist_vol(prices: &[f64]) -> f64 {
    if prices.len() < 5 {
        return 0.0;
    }
    let mut log_returns: Vec<f64> = Vec::with_capacity(prices.len() - 1);
    for i in 1..prices.len() {
        if prices[i - 1] > 0.0 && prices[i] > 0.0 {
            log_returns.push((prices[i] / prices[i - 1]).ln());
        }
    }
    if log_returns.is_empty() {
        return 0.0;
    }
    let mean = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
    let variance = log_returns
        .iter()
        .map(|r| (r - mean).powi(2))
        .sum::<f64>()
        / log_returns.len() as f64;
    // Normalise: 0.020 (2% per-tick std-dev) → 1.0; cap at 1.0
    (variance.sqrt() / 0.020).min(1.0)
}

/// Peak-to-trough range of a price series as a percentage of the minimum price.
/// An interpretable "how far did it actually travel" companion to the
/// normalized volatility. Returns 0.0 for an empty series.
pub fn range_pct(prices: &[f64]) -> f64 {
    let mut lo = f64::MAX;
    let mut hi = f64::MIN;
    for &p in prices {
        if p > 0.0 {
            lo = lo.min(p);
            hi = hi.max(p);
        }
    }
    if lo == f64::MAX || lo <= 0.0 {
        return 0.0;
    }
    (hi - lo) / lo * 100.0
}

/// Standard normal CDF Φ(x) via the Abramowitz & Stegun 7.1.26 erf
/// approximation (|error| < 1.5e-7 — far below any tradeable edge).
pub fn norm_cdf(x: f64) -> f64 {
    // Φ(x) = 0.5 · (1 + erf(x/√2))
    let z = x / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + 0.3275911 * z.abs());
    let poly = t * (0.254829592
        + t * (-0.284496736
        + t * (1.421413741
        + t * (-1.453152027
        + t * 1.061405429))));
    let erf_abs = 1.0 - poly * (-z * z).exp();
    let erf = if z >= 0.0 { erf_abs } else { -erf_abs };
    0.5 * (1.0 + erf)
}

/// Theoretical probability that `spot` finishes above `strike` after
/// `secs_left` seconds, given `sigma_per_sqrt_sec` (std-dev of log-returns per
/// √second, drift assumed 0 over intraday horizons):
///
///   P(S_T > K) = Φ( ln(S/K) / (σ·√T) )
///
/// The binary-option fair value that the FairValue viper compares against the
/// market's YES ask. Returns None when inputs are degenerate (non-positive
/// prices, zero vol, or no time remaining — the outcome is then effectively
/// decided by sign of S−K, which the caller should handle explicitly).
pub fn fair_yes_probability(
    spot: f64,
    strike: f64,
    sigma_per_sqrt_sec: f64,
    secs_left: f64,
) -> Option<f64> {
    if spot <= 0.0 || strike <= 0.0 || sigma_per_sqrt_sec <= 0.0 || secs_left <= 0.0 {
        return None;
    }
    let d = (spot / strike).ln() / (sigma_per_sqrt_sec * secs_left.sqrt());
    Some(norm_cdf(d))
}

/// Std-dev of log-returns between consecutive samples of a price series,
/// normalised to per-√second units given the average sample spacing.
/// Returns None with fewer than `min_samples` prices or degenerate spacing.
pub fn sigma_per_sqrt_sec(prices: &[f64], total_span_secs: f64, min_samples: usize) -> Option<f64> {
    if prices.len() < min_samples.max(3) || total_span_secs <= 0.0 {
        return None;
    }
    let mut rets: Vec<f64> = Vec::with_capacity(prices.len() - 1);
    for w in prices.windows(2) {
        if w[0] > 0.0 && w[1] > 0.0 {
            rets.push((w[1] / w[0]).ln());
        }
    }
    if rets.len() < 2 {
        return None;
    }
    let mean = rets.iter().sum::<f64>() / rets.len() as f64;
    let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / rets.len() as f64;
    let avg_spacing = total_span_secs / rets.len() as f64;
    if avg_spacing <= 0.0 {
        return None;
    }
    Some(var.sqrt() / avg_spacing.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_series_is_zero_vol() {
        let prices = vec![100.0; 10];
        assert_eq!(normalized_hist_vol(&prices), 0.0);
        assert_eq!(range_pct(&prices), 0.0);
    }

    #[test]
    fn too_few_samples_is_zero() {
        assert_eq!(normalized_hist_vol(&[100.0, 101.0]), 0.0);
    }

    #[test]
    fn moving_series_is_positive() {
        let prices = vec![100.0, 101.0, 100.5, 102.0, 101.0, 103.0];
        assert!(normalized_hist_vol(&prices) > 0.0);
        assert!((range_pct(&prices) - 3.0).abs() < 1e-9);
    }

    #[test]
    fn norm_cdf_reference_values() {
        assert!((norm_cdf(0.0) - 0.5).abs() < 1e-6);
        assert!((norm_cdf(1.0) - 0.841345).abs() < 1e-5);
        assert!((norm_cdf(-1.0) - 0.158655).abs() < 1e-5);
        assert!((norm_cdf(2.0) - 0.977250).abs() < 1e-5);
        assert!(norm_cdf(6.0) > 0.999999);
        assert!(norm_cdf(-6.0) < 0.000001);
    }

    #[test]
    fn fair_probability_behaviour() {
        // At the strike → exactly 50/50.
        let p = fair_yes_probability(100_000.0, 100_000.0, 1e-4, 600.0).unwrap();
        assert!((p - 0.5).abs() < 1e-9);
        // Above strike → > 0.5; further above → closer to 1.
        let p1 = fair_yes_probability(100_200.0, 100_000.0, 1e-4, 600.0).unwrap();
        let p2 = fair_yes_probability(100_600.0, 100_000.0, 1e-4, 600.0).unwrap();
        assert!(p1 > 0.5 && p2 > p1);
        // Same distance, less time → more certain.
        let p_late = fair_yes_probability(100_200.0, 100_000.0, 1e-4, 60.0).unwrap();
        assert!(p_late > p1);
        // Degenerate inputs → None.
        assert!(fair_yes_probability(100.0, 100.0, 0.0, 60.0).is_none());
        assert!(fair_yes_probability(100.0, 100.0, 1e-4, 0.0).is_none());
    }

    #[test]
    fn sigma_per_sqrt_sec_scaling() {
        // Alternating ±0.1% returns, 10 samples over 135s (15s spacing).
        let mut prices = vec![100_000.0];
        for i in 1..10 {
            let f = if i % 2 == 0 { 1.001 } else { 0.999 };
            let last = *prices.last().unwrap();
            prices.push(last * f);
        }
        let sig = sigma_per_sqrt_sec(&prices, 135.0, 5).unwrap();
        assert!(sig > 0.0);
        // per-sample σ ≈ 0.001 → per-√sec ≈ 0.001/√15 ≈ 2.58e-4
        assert!((sig - 0.001 / 15.0f64.sqrt()).abs() / sig < 0.15);
        // Too few samples → None.
        assert!(sigma_per_sqrt_sec(&prices[..3], 30.0, 5).is_none());
    }
}
