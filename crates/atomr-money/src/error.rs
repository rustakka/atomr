//! Errors raised by money arithmetic and construction.

use thiserror::Error;

/// Failures from [`Money`](crate::Money) operations. Arithmetic is always
/// checked: currency mismatch and overflow are errors, never panics or silent
/// truncation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum MoneyError {
    /// Two operands carried different currencies.
    #[error("currency mismatch: {lhs} vs {rhs}")]
    CurrencyMismatch { lhs: String, rhs: String },

    /// The result did not fit in the backing decimal.
    #[error("decimal overflow")]
    Overflow,

    /// An ISO 4217 code was not a 3-letter ASCII alphabetic string.
    #[error("invalid currency code: {0:?}")]
    BadCurrencyCode(String),

    /// A string amount could not be parsed as a decimal.
    #[error("invalid decimal amount: {0}")]
    BadAmount(String),
}
