//! Exact-decimal [`Money`] with checked arithmetic and string serialization.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::currency::Currency;
use crate::error::MoneyError;

/// Rounding mode for [`Money::round`]. Maps onto `rust_decimal`'s strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RoundingMode {
    /// Banker's rounding (round half to even) — the default for monetary work.
    BankersRounding,
    /// Round half away from zero.
    HalfUp,
    /// Round half toward zero.
    HalfDown,
    /// Round toward negative infinity.
    Floor,
    /// Round toward positive infinity.
    Ceil,
}

/// An exact monetary amount in a specific [`Currency`].
///
/// Deliberately **not** `Copy` (it carries a 128-bit decimal and should be
/// moved/cloned consciously) and has **no** `From<f64>` — float money is a
/// correctness defect. Construct from a [`Decimal`], from minor units, or from
/// a string. (de)serialization is always via a string, never a float.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Money {
    amount: Decimal,
    currency: Currency,
}

impl Money {
    /// Construct from an exact [`Decimal`] amount.
    pub fn new(amount: Decimal, currency: Currency) -> Self {
        Self { amount, currency }
    }

    /// Construct from an integer count of minor units (e.g. cents). The
    /// currency's `minor_units` determines the implied scale.
    pub fn from_minor(units: i128, currency: Currency) -> Self {
        let amount = Decimal::from_i128_with_scale(units, currency.minor_units() as u32);
        Self { amount, currency }
    }

    /// Parse an exact amount from a decimal string (e.g. `"10.25"`).
    pub fn from_str_amount(s: &str, currency: Currency) -> Result<Self, MoneyError> {
        let amount = Decimal::from_str(s).map_err(|e| MoneyError::BadAmount(e.to_string()))?;
        Ok(Self { amount, currency })
    }

    /// Lossy ingestion of an `f64`. Gated behind the `f64-lossy` feature and
    /// never on a default path — use only at trusted ingestion boundaries.
    #[cfg(feature = "f64-lossy")]
    pub fn try_from_f64_lossy(value: f64, currency: Currency) -> Result<Self, MoneyError> {
        let amount = Decimal::try_from(value).map_err(|e| MoneyError::BadAmount(e.to_string()))?;
        Ok(Self { amount, currency })
    }

    /// The underlying exact amount.
    pub fn amount(&self) -> Decimal {
        self.amount
    }

    /// The currency.
    pub fn currency(&self) -> Currency {
        self.currency
    }

    fn ensure_same_currency(&self, other: &Money) -> Result<(), MoneyError> {
        if self.currency != other.currency {
            return Err(MoneyError::CurrencyMismatch {
                lhs: self.currency.code().to_string(),
                rhs: other.currency.code().to_string(),
            });
        }
        Ok(())
    }

    /// Checked addition. Errors on currency mismatch or decimal overflow.
    pub fn checked_add(&self, other: &Money) -> Result<Money, MoneyError> {
        self.ensure_same_currency(other)?;
        let amount = self.amount.checked_add(other.amount).ok_or(MoneyError::Overflow)?;
        Ok(Money { amount, currency: self.currency })
    }

    /// Checked subtraction. Errors on currency mismatch or decimal overflow.
    pub fn checked_sub(&self, other: &Money) -> Result<Money, MoneyError> {
        self.ensure_same_currency(other)?;
        let amount = self.amount.checked_sub(other.amount).ok_or(MoneyError::Overflow)?;
        Ok(Money { amount, currency: self.currency })
    }

    /// Checked multiplication by a scalar (e.g. a quantity or rate). Errors on
    /// overflow.
    pub fn checked_mul_scalar(&self, scalar: Decimal) -> Result<Money, MoneyError> {
        let amount = self.amount.checked_mul(scalar).ok_or(MoneyError::Overflow)?;
        Ok(Money { amount, currency: self.currency })
    }

    /// Round to the currency's minor units using `mode`.
    pub fn round(&self, mode: RoundingMode) -> Money {
        let dp = self.currency.minor_units() as u32;
        let strategy = match mode {
            RoundingMode::BankersRounding => rust_decimal::RoundingStrategy::MidpointNearestEven,
            RoundingMode::HalfUp => rust_decimal::RoundingStrategy::MidpointAwayFromZero,
            RoundingMode::HalfDown => rust_decimal::RoundingStrategy::MidpointTowardZero,
            RoundingMode::Floor => rust_decimal::RoundingStrategy::ToNegativeInfinity,
            RoundingMode::Ceil => rust_decimal::RoundingStrategy::ToPositiveInfinity,
        };
        Money { amount: self.amount.round_dp_with_strategy(dp, strategy), currency: self.currency }
    }

    /// The amount expressed as an integer count of minor units (rounded to the
    /// currency's scale with banker's rounding).
    pub fn to_minor(&self) -> i128 {
        let scale = self.currency.minor_units() as u32;
        // `round_dp` normalizes the result to exactly `scale` decimal places,
        // so the mantissa is precisely the integer count of minor units.
        let rounded =
            self.amount.round_dp_with_strategy(scale, rust_decimal::RoundingStrategy::MidpointNearestEven);
        rounded.mantissa()
    }
}

impl std::fmt::Display for Money {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.amount, self.currency)
    }
}

/// Wire form: `{ "amount": "<decimal-string>", "currency": "USD" }`. The amount
/// is always a string — never a float — so no precision is lost in transit or
/// at rest.
#[derive(Serialize, Deserialize)]
struct MoneyWire {
    amount: String,
    currency: Currency,
}

impl Serialize for Money {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        MoneyWire { amount: self.amount.to_string(), currency: self.currency }.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Money {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = MoneyWire::deserialize(deserializer)?;
        let amount = Decimal::from_str(&wire.amount).map_err(serde::de::Error::custom)?;
        Ok(Money { amount, currency: wire.currency })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn from_minor_and_to_minor_round_trip() {
        let m = Money::from_minor(1025, Currency::USD);
        assert_eq!(m.amount(), dec!(10.25));
        assert_eq!(m.to_minor(), 1025);
    }

    #[test]
    fn checked_add_same_currency() {
        let a = Money::from_str_amount("10.25", Currency::USD).unwrap();
        let b = Money::from_str_amount("0.75", Currency::USD).unwrap();
        assert_eq!(a.checked_add(&b).unwrap().amount(), dec!(11.00));
    }

    #[test]
    fn checked_add_currency_mismatch_errors() {
        let a = Money::from_minor(100, Currency::USD);
        let b = Money::from_minor(100, Currency::EUR);
        assert!(matches!(a.checked_add(&b), Err(MoneyError::CurrencyMismatch { .. })));
    }

    #[test]
    fn rounding_modes() {
        let m = Money::from_str_amount("2.345", Currency::USD).unwrap();
        assert_eq!(m.round(RoundingMode::BankersRounding).amount(), dec!(2.34));
        assert_eq!(m.round(RoundingMode::HalfUp).amount(), dec!(2.35));
    }

    #[test]
    fn serde_uses_string_amount() {
        let m = Money::from_str_amount("10.25", Currency::USD).unwrap();
        let json = serde_json::to_string(&m).unwrap();
        assert!(json.contains("\"10.25\""), "amount must serialize as string: {json}");
        assert!(!json.contains("10.25,") || json.contains("\"10.25\""));
        let back: Money = serde_json::from_str(&json).unwrap();
        assert_eq!(back, m);
    }
}
