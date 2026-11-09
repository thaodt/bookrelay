//! Exact decimal conversion at the output schema's six-decimal scale.

use anyhow::{Context, ensure};

pub const SCALE: i64 = 1_000_000;

pub fn parse(input: &str) -> anyhow::Result<i64> {
    let (negative, digits) = match input.as_bytes().first() {
        Some(b'-') => (true, &input[1..]),
        Some(b'+') => (false, &input[1..]),
        _ => (false, input),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    ensure!(!whole.is_empty(), "decimal needs an integer part");
    ensure!(
        whole.bytes().all(|c| c.is_ascii_digit()),
        "invalid decimal integer"
    );
    ensure!(
        fraction.bytes().all(|c| c.is_ascii_digit()),
        "invalid decimal fraction"
    );
    ensure!(
        !digits.ends_with('.'),
        "decimal needs a fractional part after '.'"
    );
    ensure!(
        fraction.bytes().skip(6).all(|c| c == b'0'),
        "decimal exceeds six-digit precision"
    );
    let whole: i128 = whole.parse().context("decimal integer overflow")?;
    let mut fractional = 0i128;
    for i in 0..6 {
        fractional =
            fractional * 10 + i128::from(fraction.as_bytes().get(i).map_or(0, |c| c - b'0'));
    }
    let magnitude = whole
        .checked_mul(i128::from(SCALE))
        .and_then(|value| value.checked_add(fractional))
        .context("fixed-point overflow")?;
    i64::try_from(if negative { -magnitude } else { magnitude })
        .context("fixed-point outside signed 64-bit range")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_conversion_is_exact_including_fractional_contracts() {
        for (input, expected) in [
            ("0", 0),
            ("0.4200", 420_000),
            ("13.25", 13_250_000),
            ("-54.00", -54_000_000),
            ("+0.000001", 1),
            ("0.42000000", 420_000),
            ("9223372036854.775807", i64::MAX),
            ("-9223372036854.775808", i64::MIN),
        ] {
            assert_eq!(parse(input).unwrap(), expected, "{input}");
        }
    }

    #[test]
    fn rejects_rounding_overflow_and_non_decimal_syntax() {
        for input in [
            "",
            "-",
            ".5",
            "1.",
            "1.2.3",
            "1e-6",
            " 1",
            "NaN",
            "0.0000001",
            "9223372036854.775808",
            "-9223372036854.775809",
            "9999999999999999999999999999999999999999999999999",
        ] {
            assert!(parse(input).is_err(), "unexpected acceptance: {input}");
        }
    }
}
