//! Exact decimal-string to fixed-point conversion. 1_000_000 = 1 unit.

use anyhow::{Context, bail, ensure};

pub const SCALE: i64 = 1_000_000;
const DECIMALS: usize = 6;

/// Parses a decimal string like `"0.9600"` or `"-54.00"` into fixed point,
/// rejecting anything that can't be represented exactly.
pub fn parse(s: &str) -> anyhow::Result<i64> {
    let (negative, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    ensure!(!int.is_empty(), "missing integer part in {s:?}");
    ensure!(frac.len() <= DECIMALS, "too many decimals in {s:?}");
    if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        bail!("invalid decimal {s:?}");
    }

    let int: i64 = int
        .parse()
        .with_context(|| format!("integer part of {s:?}"))?;
    let frac: i64 = if frac.is_empty() {
        0
    } else {
        frac.parse::<i64>()? * 10i64.pow((DECIMALS - frac.len()) as u32)
    };
    let value = int
        .checked_mul(SCALE)
        .and_then(|v| v.checked_add(frac))
        .with_context(|| format!("{s:?} overflows fixed point"))?;
    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kalshi_formats() {
        assert_eq!(parse("0.9600").unwrap(), 960_000);
        assert_eq!(parse("0.0001").unwrap(), 100);
        assert_eq!(parse("1").unwrap(), SCALE);
        assert_eq!(parse("54.00").unwrap(), 54_000_000);
        assert_eq!(parse("-54.25").unwrap(), -54_250_000);
        assert_eq!(parse("0.123456").unwrap(), 123_456);
    }

    #[test]
    fn rejects_inexact_or_malformed() {
        for bad in [
            "",
            "-",
            ".5",
            "1.2345678",
            "1e3",
            "abc",
            "1.-2",
            "--1",
            "+1",
            "1.2.3",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} should fail");
        }
        assert!(parse("99999999999999").is_err());
    }
}
