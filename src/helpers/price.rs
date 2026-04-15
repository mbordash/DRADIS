use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

const USDC_DECIMALS: u32 = 6;

/// Convert Decimal to fixed-point u128 representation (10^6 scaling)
/// with a specific decimal precision for the input value.
pub fn to_fixed_u128_with_precision(d: Decimal, precision: u32) -> u128 {
    // Truncate to the allowed precision first (e.g., 2 for USDC, 4 for shares)
    let truncated = d.trunc_with_scale(precision);

    // Scale to 6 decimals as required by the CTF contract
    let scaled = truncated * Decimal::from(10u32.pow(USDC_DECIMALS));

    scaled.to_u128().unwrap_or(0)
}

/// Convert Decimal to fixed-point u128 representation for USDC (6 decimals)
pub fn to_fixed_u128(d: Decimal) -> u128 {
    to_fixed_u128_with_precision(d, USDC_DECIMALS)
}

/// Extract f64 value from serde_json::Value
/// Handles string, f64, and numeric types
pub fn value_to_f64(v: &serde_json::Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        Some(n)
    } else if let Some(s) = v.as_str() {
        s.trim().parse::<f64>().ok()
    } else {
        None
    }
}
