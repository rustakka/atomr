//! Exact-decimal monetary primitives (`atomr-money`) exposed to Python.
//!
//! **String-only interop.** `Money`/`Price`/`Qty` are constructed from
//! decimal *strings*, never Python floats — float money is a correctness
//! defect, so any `float` argument is rejected with `TypeError`. Minor
//! units (`from_minor` / `to_minor`) round-trip through Python `int`.
//!
//! Currency mismatch and decimal overflow surface as `ValueError`
//! (via `atomr_money::MoneyError`), matching the Rust API's checked
//! arithmetic — never a panic or a silent truncation.

use std::str::FromStr;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyFloat, PyString};

use atomr_money::{Currency, Decimal, Money, Price, Qty, RoundingMode};

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

/// Reject Python floats outright and parse everything else as a decimal
/// string. Accepts `str` (and `int`, which Python stringifies losslessly)
/// but never `float`. This is the single choke point that enforces the
/// "no float coercion anywhere" contract.
fn parse_decimal(value: &Bound<'_, PyAny>) -> PyResult<Decimal> {
    if value.is_instance_of::<PyFloat>() {
        return Err(PyTypeError::new_err(
            "money values must be passed as a decimal string, never a float \
             (float money loses precision); pass e.g. \"10.25\" instead",
        ));
    }
    // `bool` is a subclass of int in Python — reject it as nonsensical here.
    let s: String = if let Ok(py_str) = value.downcast::<PyString>() {
        py_str.to_str()?.to_string()
    } else {
        // Allow Python int (lossless) by stringifying; reject anything else.
        let cls = value.get_type();
        let name = cls.name()?;
        if name == "int" {
            value.str()?.to_str()?.to_string()
        } else {
            return Err(PyTypeError::new_err(format!(
                "expected a decimal string for a money amount, got `{name}`"
            )));
        }
    };
    Decimal::from_str(s.trim())
        .map_err(|e| PyValueError::new_err(format!("invalid decimal string `{s}`: {e}")))
}

fn parse_rounding_mode(mode: &str) -> PyResult<RoundingMode> {
    Ok(match mode {
        "bankers" | "bankers_rounding" | "half_even" => RoundingMode::BankersRounding,
        "half_up" => RoundingMode::HalfUp,
        "half_down" => RoundingMode::HalfDown,
        "floor" => RoundingMode::Floor,
        "ceil" | "ceiling" => RoundingMode::Ceil,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown rounding mode `{other}`; expected one of \
                 bankers | half_up | half_down | floor | ceil"
            )))
        }
    })
}

// ---------------------------------------------------------------------
// Currency
// ---------------------------------------------------------------------

/// ISO 4217 currency descriptor. Frozen value type.
#[pyclass(name = "Currency", module = "atomr._native.money", frozen)]
#[derive(Clone, Copy)]
pub struct PyCurrency {
    pub(crate) inner: Currency,
}

#[pymethods]
impl PyCurrency {
    /// Look up a currency by its 3-letter code (case-insensitive). Known
    /// codes carry their correct minor-unit count; unknown well-formed
    /// codes default to 2 minor units.
    #[staticmethod]
    fn from_code(code: &str) -> PyResult<Self> {
        let inner = Currency::from_code(code).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner })
    }

    #[getter]
    fn code(&self) -> String {
        self.inner.code().to_string()
    }

    #[getter]
    fn minor_units(&self) -> u8 {
        self.inner.minor_units()
    }

    fn __str__(&self) -> String {
        self.inner.code().to_string()
    }

    fn __repr__(&self) -> String {
        format!("Currency({})", self.inner.code())
    }

    fn __eq__(&self, other: &PyCurrency) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> u64 {
        // Stable hash over the code bytes.
        let bytes = self.inner.code().as_bytes();
        let mut h: u64 = 1469598103934665603; // FNV-1a offset basis
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(1099511628211);
        }
        h
    }
}

// ---------------------------------------------------------------------
// Money
// ---------------------------------------------------------------------

/// Exact monetary amount in a specific currency. Constructed only from
/// decimal strings or integer minor units — never a float.
#[pyclass(name = "Money", module = "atomr._native.money")]
#[derive(Clone)]
pub struct PyMoney {
    pub(crate) inner: Money,
}

#[pymethods]
impl PyMoney {
    /// Parse an exact amount from a decimal string (e.g. `"10.25"`). A
    /// `float` `amount` raises `TypeError`.
    #[staticmethod]
    fn from_str(amount: &Bound<'_, PyAny>, currency: &str) -> PyResult<Self> {
        let dec = parse_decimal(amount)?;
        let cur = Currency::from_code(currency).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: Money::new(dec, cur) })
    }

    /// Construct from an integer count of minor units (e.g. cents). The
    /// currency's `minor_units` determines the implied scale.
    #[staticmethod]
    fn from_minor(units: i128, currency: &str) -> PyResult<Self> {
        let cur = Currency::from_code(currency).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: Money::from_minor(units, cur) })
    }

    #[getter]
    fn currency(&self) -> PyCurrency {
        PyCurrency { inner: self.inner.currency() }
    }

    /// The exact amount as a decimal string (never a float).
    #[getter]
    fn amount(&self) -> String {
        self.inner.amount().to_string()
    }

    /// Checked addition. Raises `ValueError` on currency mismatch or overflow.
    fn checked_add(&self, other: &PyMoney) -> PyResult<Self> {
        let inner = self.inner.checked_add(&other.inner).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Checked subtraction. Raises `ValueError` on currency mismatch or overflow.
    fn checked_sub(&self, other: &PyMoney) -> PyResult<Self> {
        let inner = self.inner.checked_sub(&other.inner).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Checked multiply by a scalar passed as a decimal string. Raises on overflow.
    fn checked_mul(&self, scalar: &Bound<'_, PyAny>) -> PyResult<Self> {
        let dec = parse_decimal(scalar)?;
        let inner = self.inner.checked_mul_scalar(dec).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Round to the currency's minor units using `mode` (`bankers`,
    /// `half_up`, `half_down`, `floor`, `ceil`).
    #[pyo3(signature = (mode="bankers".to_string()))]
    fn round(&self, mode: String) -> PyResult<Self> {
        let m = parse_rounding_mode(&mode)?;
        Ok(Self { inner: self.inner.round(m) })
    }

    /// The amount as an integer count of minor units (banker's rounding).
    fn to_minor(&self) -> i128 {
        self.inner.to_minor()
    }

    fn __eq__(&self, other: &PyMoney) -> bool {
        self.inner == other.inner
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("Money(\"{}\", {})", self.inner.amount(), self.inner.currency().code())
    }
}

// ---------------------------------------------------------------------
// Price / Qty
// ---------------------------------------------------------------------

/// Instrument price. Tick-aware; string-only interop.
#[pyclass(name = "Price", module = "atomr._native.money")]
#[derive(Clone, Copy)]
pub struct PyPrice {
    pub(crate) inner: Price,
}

#[pymethods]
impl PyPrice {
    /// Construct from a decimal string. A `float` raises `TypeError`.
    #[new]
    fn new(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self { inner: Price::new(parse_decimal(value)?) })
    }

    #[staticmethod]
    fn from_str(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self { inner: Price::new(parse_decimal(value)?) })
    }

    /// The underlying value as a decimal string.
    #[getter]
    fn value(&self) -> String {
        self.inner.value().to_string()
    }

    /// Round to the nearest multiple of `tick` (half-to-even). `tick` is a
    /// decimal string; a non-positive tick returns the price unchanged.
    fn round_to_tick(&self, tick: &Bound<'_, PyAny>) -> PyResult<Self> {
        let t = parse_decimal(tick)?;
        Ok(Self { inner: self.inner.round_to_tick(t) })
    }

    fn __eq__(&self, other: &PyPrice) -> bool {
        self.inner == other.inner
    }

    fn __str__(&self) -> String {
        self.inner.value().to_string()
    }

    fn __repr__(&self) -> String {
        format!("Price(\"{}\")", self.inner.value())
    }
}

/// Order/position quantity. Lot-aware; string-only interop.
#[pyclass(name = "Qty", module = "atomr._native.money")]
#[derive(Clone, Copy)]
pub struct PyQty {
    pub(crate) inner: Qty,
}

#[pymethods]
impl PyQty {
    /// Construct from a decimal string. A `float` raises `TypeError`.
    #[new]
    fn new(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self { inner: Qty::new(parse_decimal(value)?) })
    }

    #[staticmethod]
    fn from_str(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self { inner: Qty::new(parse_decimal(value)?) })
    }

    /// The underlying value as a decimal string.
    #[getter]
    fn value(&self) -> String {
        self.inner.value().to_string()
    }

    /// Round *down* to the nearest multiple of `lot` (you cannot trade a
    /// partial lot). `lot` is a decimal string; a non-positive lot returns
    /// the quantity unchanged.
    fn round_to_lot(&self, lot: &Bound<'_, PyAny>) -> PyResult<Self> {
        let l = parse_decimal(lot)?;
        Ok(Self { inner: self.inner.round_to_lot(l) })
    }

    fn __eq__(&self, other: &PyQty) -> bool {
        self.inner == other.inner
    }

    fn __str__(&self) -> String {
        self.inner.value().to_string()
    }

    fn __repr__(&self) -> String {
        format!("Qty(\"{}\")", self.inner.value())
    }
}

// ---------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "money")?;
    sub.add_class::<PyCurrency>()?;
    sub.add_class::<PyMoney>()?;
    sub.add_class::<PyPrice>()?;
    sub.add_class::<PyQty>()?;
    m.add_submodule(&sub)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn money_from_str_and_to_minor() {
        let m = Money::from_str_amount("10.25", Currency::USD).unwrap();
        assert_eq!(m.to_minor(), 1025);
    }

    #[test]
    fn rounding_mode_parse() {
        assert_eq!(parse_rounding_mode("bankers").unwrap(), RoundingMode::BankersRounding);
        assert_eq!(parse_rounding_mode("half_up").unwrap(), RoundingMode::HalfUp);
        assert!(parse_rounding_mode("nope").is_err());
    }
}
