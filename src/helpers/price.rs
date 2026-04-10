use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

const USDC_DECIMALS: u32 = 6;

/// Convert Decimal to fixed-point u128 representation for USDC (6 decimals)
pub fn to_fixed_u128(d: Decimal) -> u128 {
    d.normalize()
        .trunc_with_scale(USDC_DECIMALS)
        .mantissa()
        .to_u128()
        .unwrap_or(0)
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

