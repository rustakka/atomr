//! Tick/lot-aware [`Price`] and [`Qty`] helpers.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// An instrument price. Serializes as a decimal string (never a float).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Price(#[serde(with = "rust_decimal::serde::str")] Decimal);

impl Price {
    /// Construct from an exact decimal.
    pub fn new(value: Decimal) -> Self {
        Price(value)
    }

    /// The underlying decimal.
    pub fn value(self) -> Decimal {
        self.0
    }

    /// Round to the nearest multiple of `tick` (rounding half to even). A
    /// non-positive `tick` is returned unchanged.
    pub fn round_to_tick(self, tick: Decimal) -> Self {
        if tick <= Decimal::ZERO {
            return self;
        }
        let steps =
            (self.0 / tick).round_dp_with_strategy(0, rust_decimal::RoundingStrategy::MidpointNearestEven);
        Price(steps * tick)
    }
}

/// An order/position quantity. Serializes as a decimal string (never a float).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Qty(#[serde(with = "rust_decimal::serde::str")] Decimal);

impl Qty {
    /// Construct from an exact decimal.
    pub fn new(value: Decimal) -> Self {
        Qty(value)
    }

    /// The underlying decimal.
    pub fn value(self) -> Decimal {
        self.0
    }

    /// Round *down* to the nearest multiple of `lot` (you cannot trade a
    /// partial lot). A non-positive `lot` is returned unchanged.
    pub fn round_to_lot(self, lot: Decimal) -> Self {
        if lot <= Decimal::ZERO {
            return self;
        }
        let steps = (self.0 / lot).floor();
        Qty(steps * lot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn price_rounds_to_tick() {
        let p = Price::new(dec!(100.123));
        assert_eq!(p.round_to_tick(dec!(0.05)).value(), dec!(100.10));
        assert_eq!(p.round_to_tick(dec!(0.25)).value(), dec!(100.00));
    }

    #[test]
    fn qty_floors_to_lot() {
        let q = Qty::new(dec!(157));
        assert_eq!(q.round_to_lot(dec!(100)).value(), dec!(100));
        assert_eq!(q.round_to_lot(dec!(10)).value(), dec!(150));
    }

    #[test]
    fn serde_is_string() {
        let json = serde_json::to_string(&Price::new(dec!(100.10))).unwrap();
        assert_eq!(json, "\"100.10\"");
    }
}
