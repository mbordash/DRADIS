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
}
