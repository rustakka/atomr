//! # atomr-money
//!
//! Exact-decimal monetary primitives for the atomr substrate.
//!
//! Financial consumers (NAV/GL, OMS fills, fees, PnL/Greeks) need exact money
//! arithmetic — `f64` money is a regulatory and P&L-correctness defect. This
//! crate provides a [`Money`] type over [`rust_decimal::Decimal`] (128-bit)
//! behind a domain-safe API:
//!
//! * **No defaulted `f64` constructor.** Lossy `f64` ingestion exists only
//!   behind the off-by-default `f64-lossy` feature.
//! * **String (de)serialization, never float.** [`Money`]/[`Price`]/[`Qty`]
//!   serialize their amount as a decimal string so nothing is lost in transit
//!   or at rest.
//! * **Checked arithmetic.** Currency mismatch and overflow return
//!   [`MoneyError`] — never a panic or silent truncation.
//!
//! ```
//! use atomr_money::{Money, Currency};
//!
//! let a = Money::from_str_amount("10.25", Currency::USD).unwrap();
//! let b = Money::from_minor(75, Currency::USD); // 0.75
//! assert_eq!(a.checked_add(&b).unwrap().to_minor(), 1100);
//! ```

#![forbid(unsafe_code)]

mod currency;
mod error;
mod money;
mod price_qty;

pub use currency::Currency;
pub use error::MoneyError;
pub use money::{Money, RoundingMode};
pub use price_qty::{Price, Qty};

/// Re-export of the backing decimal type so consumers can construct exact
/// amounts without taking a direct `rust_decimal` dependency.
pub use rust_decimal::Decimal;
