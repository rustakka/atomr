//! Property tests: no penny drift and additive associativity over many ops.

use atomr_money::{Currency, Decimal, Money};
use proptest::prelude::*;

/// Generate a Money in minor units within a bounded range so sums stay in
/// range and overflow is not the thing under test.
fn money_strategy() -> impl Strategy<Value = Money> {
    (-1_000_000_000i128..1_000_000_000i128).prop_map(|m| Money::from_minor(m, Currency::USD))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// (a + b) + c == a + (b + c) exactly, no rounding drift.
    #[test]
    fn addition_is_associative(a in money_strategy(), b in money_strategy(), c in money_strategy()) {
        let left = a.checked_add(&b).unwrap().checked_add(&c).unwrap();
        let right = b.checked_add(&c).unwrap();
        let right = a.checked_add(&right).unwrap();
        prop_assert_eq!(left.amount(), right.amount());
    }

    /// Summing minor-unit values and converting once equals summing Money and
    /// converting — i.e. no penny is created or lost.
    #[test]
    fn no_penny_drift(units in proptest::collection::vec(-1_000_000i128..1_000_000i128, 1..1000)) {
        let expected: i128 = units.iter().sum();
        let mut acc = Money::from_minor(0, Currency::USD);
        for u in &units {
            acc = acc.checked_add(&Money::from_minor(*u, Currency::USD)).unwrap();
        }
        prop_assert_eq!(acc.to_minor(), expected);
    }

    /// minor -> Money -> minor round-trips exactly.
    #[test]
    fn minor_round_trip(m in -1_000_000_000_000i128..1_000_000_000_000i128) {
        let money = Money::from_minor(m, Currency::USD);
        prop_assert_eq!(money.to_minor(), m);
    }

    /// Scalar multiplication distributes over the scalar exactly for integers.
    #[test]
    fn scalar_mul_matches_repeated_add(units in -1_000_000i128..1_000_000i128, n in 0u32..50) {
        let m = Money::from_minor(units, Currency::USD);
        let scaled = m.checked_mul_scalar(Decimal::from(n)).unwrap();
        let mut acc = Money::from_minor(0, Currency::USD);
        for _ in 0..n {
            acc = acc.checked_add(&m).unwrap();
        }
        prop_assert_eq!(scaled.amount(), acc.amount());
    }
}
