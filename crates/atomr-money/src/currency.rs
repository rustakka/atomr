//! ISO 4217 currency descriptor.

use serde::{Deserialize, Serialize};

use crate::error::MoneyError;

/// A currency identified by its ISO 4217 alphabetic code plus the number of
/// minor units (decimal places) it conventionally uses.
///
/// `Copy` and tiny — currencies are cheap to pass around and compare. The
/// `code` is always 3 uppercase ASCII letters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Currency {
    code: [u8; 3],
    minor_units: u8,
}

impl Currency {
    /// Construct from a raw code + minor-unit count. Prefer the associated
    /// constants ([`Currency::USD`] etc.) or [`Currency::from_code`].
    pub const fn new(code: [u8; 3], minor_units: u8) -> Self {
        Self { code, minor_units }
    }

    /// US dollar (2 minor units).
    pub const USD: Currency = Currency::new(*b"USD", 2);
    /// Euro (2 minor units).
    pub const EUR: Currency = Currency::new(*b"EUR", 2);
    /// Pound sterling (2 minor units).
    pub const GBP: Currency = Currency::new(*b"GBP", 2);
    /// Japanese yen (0 minor units).
    pub const JPY: Currency = Currency::new(*b"JPY", 0);
    /// Swiss franc (2 minor units).
    pub const CHF: Currency = Currency::new(*b"CHF", 2);

    /// Look up a currency by 3-letter code. Known codes carry their correct
    /// minor-unit count; unknown (but well-formed) codes default to 2.
    pub fn from_code(code: &str) -> Result<Currency, MoneyError> {
        let bytes = code.as_bytes();
        if bytes.len() != 3 || !bytes.iter().all(|b| b.is_ascii_alphabetic()) {
            return Err(MoneyError::BadCurrencyCode(code.to_string()));
        }
        let upper =
            [bytes[0].to_ascii_uppercase(), bytes[1].to_ascii_uppercase(), bytes[2].to_ascii_uppercase()];
        Ok(match &upper {
            b"USD" => Currency::USD,
            b"EUR" => Currency::EUR,
            b"GBP" => Currency::GBP,
            b"JPY" => Currency::JPY,
            b"CHF" => Currency::CHF,
            other => Currency::new(*other, 2),
        })
    }

    /// The 3-letter code as a string slice.
    pub fn code(&self) -> &str {
        // Safe: `code` is only ever set from ASCII-alphabetic bytes.
        std::str::from_utf8(&self.code).unwrap_or("???")
    }

    /// Number of minor units (decimal places).
    pub fn minor_units(&self) -> u8 {
        self.minor_units
    }
}

impl std::fmt::Display for Currency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_code_known_and_unknown() {
        assert_eq!(Currency::from_code("usd").unwrap(), Currency::USD);
        assert_eq!(Currency::from_code("JPY").unwrap().minor_units(), 0);
        assert_eq!(Currency::from_code("xyz").unwrap().minor_units(), 2);
        assert!(Currency::from_code("US").is_err());
        assert!(Currency::from_code("US1").is_err());
    }

    #[test]
    fn code_round_trips() {
        assert_eq!(Currency::USD.code(), "USD");
        assert_eq!(Currency::USD.to_string(), "USD");
    }
}
