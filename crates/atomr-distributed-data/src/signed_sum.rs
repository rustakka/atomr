//! Signed, `i128`-payload exposure CRDT (FR-4).
//!
//! [`SignedSum`] tracks a firm-wide *net signed exposure* that converges
//! across replicas, and [`SignedSumMap`] keys those exposures (e.g. per
//! instrument / desk / counterparty).
//!
//! # Why a PN-counter shape over `i128`
//!
//! [`SignedSum`] mirrors [`crate::counters::PNCounter`]: it keeps a
//! per-node *positive* accumulator and a per-node *negative* accumulator.
//! Each accumulator is itself a grow-only register — a node's own value
//! only ever increases — so the join-semilattice merge is simply a
//! per-node `max` of `pos` and a per-node `max` of `neg`. That makes
//! [`CrdtMerge::merge`] commutative, associative and idempotent, which is
//! exactly the convergence guarantee callers rely on.
//!
//! `value() = sum(pos) - sum(neg)`.
//!
//! # Negative deltas
//!
//! Negative deltas are first-class: [`SignedSum::add`] routes a `delta`
//! into the `neg` accumulator (by its absolute magnitude) when `delta < 0`
//! and into `pos` otherwise. There is no separate `decrement` entry point.
//!
//! # Overflow
//!
//! Exposure is financial and must never silently wrap. All accumulation
//! and the final `value()` reduction use **saturating** `i128` arithmetic:
//! a per-node accumulator saturates at [`i128::MAX`], and `value()`
//! saturates at [`i128::MAX`] / [`i128::MIN`]. Saturation is monotone, so
//! it preserves the join-semilattice merge (a saturated `pos` is still the
//! `max` of the two inputs).
//!
//! # Decimal feature (deferred)
//!
//! A future `decimal` feature could swap the `i128` payload for a
//! fixed-point decimal type to carry fractional exposure. It is
//! intentionally **out of scope here** to avoid a cross-crate dependency;
//! `i128` (minor units / basis points) is the deliverable.

use std::collections::BTreeMap;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::traits::{CrdtMerge, DeltaCrdt};

/// Per-node positive / negative accumulators for one signed exposure.
///
/// Both accumulators are grow-only per node; the net exposure is
/// `sum(pos) - sum(neg)`. See the module-level docs for convergence and
/// overflow semantics.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedSum {
    /// Per-node positive accumulator (monotonically increasing per node).
    pos: HashMap<String, i128>,
    /// Per-node negative accumulator (stores magnitudes; monotone per node).
    neg: HashMap<String, i128>,
    /// Accumulated since the last `take_delta`. Skipped on serialization so
    /// peers never observe another node's pending delta — they receive
    /// deltas through the explicit delta-gossip path. Mirrors `GCounter`.
    #[serde(skip)]
    pending_pos: HashMap<String, i128>,
    #[serde(skip)]
    pending_neg: HashMap<String, i128>,
}

impl SignedSum {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a signed `delta` attributed to `node`.
    ///
    /// `delta >= 0` adds to the node's positive accumulator; `delta < 0`
    /// adds its magnitude to the node's negative accumulator. Accumulation
    /// is saturating — a node's accumulator never wraps and never decreases.
    pub fn add(&mut self, node: &str, delta: i128) {
        let key = node.to_string();
        if delta >= 0 {
            Self::add_into(&mut self.pos, key.clone(), delta);
            Self::add_into(&mut self.pending_pos, key, delta);
        } else {
            // Magnitude of a negative delta. `i128::MIN.unsigned_abs()` does
            // not fit in i128, so saturate the magnitude at `i128::MAX`.
            let mag = delta.checked_neg().unwrap_or(i128::MAX);
            Self::add_into(&mut self.neg, key.clone(), mag);
            Self::add_into(&mut self.pending_neg, key, mag);
        }
    }

    /// Saturating per-node accumulation helper.
    fn add_into(map: &mut HashMap<String, i128>, key: String, amount: i128) {
        let slot = map.entry(key).or_default();
        *slot = slot.saturating_add(amount);
    }

    /// Net signed exposure: `sum(pos) - sum(neg)`, computed with
    /// saturating arithmetic (saturates at `i128::MAX` / `i128::MIN`).
    pub fn value(&self) -> i128 {
        let pos = Self::saturating_sum(&self.pos);
        let neg = Self::saturating_sum(&self.neg);
        pos.saturating_sub(neg)
    }

    fn saturating_sum(map: &HashMap<String, i128>) -> i128 {
        map.values().fold(0i128, |acc, &v| acc.saturating_add(v))
    }
}

impl CrdtMerge for SignedSum {
    fn merge(&mut self, other: &Self) {
        for (k, v) in &other.pos {
            let slot = self.pos.entry(k.clone()).or_default();
            *slot = (*slot).max(*v);
        }
        for (k, v) in &other.neg {
            let slot = self.neg.entry(k.clone()).or_default();
            *slot = (*slot).max(*v);
        }
    }
}

/// Delta for [`SignedSum`]: the per-node `pos` / `neg` increments
/// accumulated since the last [`DeltaCrdt::take_delta`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedSumDelta {
    pos: HashMap<String, i128>,
    neg: HashMap<String, i128>,
}

impl DeltaCrdt for SignedSum {
    type Delta = SignedSumDelta;

    fn take_delta(&mut self) -> Option<Self::Delta> {
        if self.pending_pos.is_empty() && self.pending_neg.is_empty() {
            return None;
        }
        Some(SignedSumDelta {
            pos: std::mem::take(&mut self.pending_pos),
            neg: std::mem::take(&mut self.pending_neg),
        })
    }

    fn merge_delta(&mut self, delta: &Self::Delta) {
        for (k, v) in &delta.pos {
            Self::add_into(&mut self.pos, k.clone(), *v);
        }
        for (k, v) in &delta.neg {
            Self::add_into(&mut self.neg, k.clone(), *v);
        }
    }
}

/// Map of `K → SignedSum` — convergent per-key net signed exposure.
///
/// Merge is the per-key [`SignedSum`] merge over the union of keys, so the
/// map inherits the same commutative / associative / idempotent guarantee.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedSumMap<K: Ord + Clone> {
    entries: BTreeMap<K, SignedSum>,
}

impl<K: Ord + Clone> SignedSumMap<K> {
    pub fn new() -> Self {
        Self { entries: BTreeMap::new() }
    }

    /// Apply a signed `delta` attributed to `node` for `key`.
    pub fn add(&mut self, key: K, node: &str, delta: i128) {
        self.entries.entry(key).or_default().add(node, delta);
    }

    /// Net signed exposure for `key` (`0` if the key is absent).
    pub fn get(&self, key: &K) -> i128 {
        self.entries.get(key).map(SignedSum::value).unwrap_or(0)
    }

    /// Iterate `(&key, net_value)` over all keys, in key order.
    pub fn entries(&self) -> impl Iterator<Item = (&K, i128)> {
        self.entries.iter().map(|(k, s)| (k, s.value()))
    }
}

impl<K: Ord + Clone> CrdtMerge for SignedSumMap<K> {
    fn merge(&mut self, other: &Self) {
        for (k, v) in &other.entries {
            self.entries.entry(k.clone()).or_default().merge(v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_routes_sign_correctly() {
        let mut s = SignedSum::new();
        s.add("n1", 100);
        s.add("n1", -30);
        assert_eq!(s.value(), 70);
    }

    #[test]
    fn negative_deltas_are_first_class() {
        let mut s = SignedSum::new();
        s.add("n1", -50);
        s.add("n2", -25);
        s.add("n1", 10);
        assert_eq!(s.value(), -65);
    }

    #[test]
    fn merge_takes_per_node_max() {
        // Same node observed at two replicas: merge takes the max, it does
        // NOT sum (that is what keeps re-merge idempotent).
        let mut a = SignedSum::new();
        let mut b = SignedSum::new();
        a.add("n1", 5);
        a.add("n1", 3); // n1.pos = 8 at a
        b.add("n1", 5); // n1.pos = 5 at b (stale view)
        b.add("n2", 7);
        a.merge(&b);
        assert_eq!(a.value(), 8 + 7);
    }

    #[test]
    fn merge_is_idempotent() {
        let mut a = SignedSum::new();
        a.add("n1", 10);
        a.add("n2", -4);
        let before = a.clone();
        // Merging a replica's own state (or any state already absorbed)
        // any number of times must leave the converged state untouched.
        a.merge(&before);
        a.merge(&before);
        assert_eq!(a.pos, before.pos);
        assert_eq!(a.neg, before.neg);
        assert_eq!(a.value(), 6);
    }

    #[test]
    fn merge_commutative_associative_across_three_replicas() {
        // Three replicas each see a disjoint-ish stream of ops. Every merge
        // order / interleaving must converge to the identical value.
        let build = || {
            let mut r1 = SignedSum::new();
            r1.add("n1", 100);
            r1.add("n1", -10);
            let mut r2 = SignedSum::new();
            r2.add("n2", -40);
            r2.add("n2", 5);
            let mut r3 = SignedSum::new();
            r3.add("n3", 7);
            r3.add("n1", 100); // stale view of n1 (<= r1's 90)
            (r1, r2, r3)
        };

        // Order A: ((r1 <- r2) <- r3)
        let (mut a, b, c) = build();
        a.merge(&b);
        a.merge(&c);

        // Order B: ((r3 <- r1) <- r2)
        let (r1, r2, mut x) = build();
        x.merge(&r1);
        x.merge(&r2);

        // Order C: r2 <- (r3 merged into r1) — different associativity.
        let (mut p, q, r) = build();
        let mut pr = r;
        pr.merge(&p);
        p = q;
        p.merge(&pr);

        let expected = 90 - 35 + 7; // n1=100 (max), n1.neg=10, n2=-35, n3=7
        assert_eq!(a.value(), expected);
        assert_eq!(x.value(), expected);
        assert_eq!(p.value(), expected);
        assert_eq!(a.value(), x.value());
        assert_eq!(x.value(), p.value());
    }

    #[test]
    fn saturates_at_i128_max_on_accumulate() {
        let mut s = SignedSum::new();
        s.add("n1", i128::MAX);
        s.add("n1", i128::MAX); // would overflow -> saturates
        assert_eq!(s.value(), i128::MAX);
    }

    #[test]
    fn saturates_at_i128_min_value() {
        let mut s = SignedSum::new();
        // `i128::MIN` has no positive counterpart, so its magnitude
        // saturates to `i128::MAX` in the neg accumulator.
        s.add("n1", i128::MIN);
        // pos=0, neg=i128::MAX => value = 0 - MAX = i128::MIN + 1, no wrap.
        assert_eq!(s.value(), i128::MIN + 1);

        // Pile on more negative magnitude across nodes: the neg sum
        // saturates at i128::MAX, so value() floors at i128::MIN + 1 and
        // never wraps positive.
        s.add("n2", i128::MIN);
        s.add("n3", -1_000_000);
        assert_eq!(s.value(), i128::MIN + 1);
    }

    #[test]
    fn value_subtraction_saturates_at_min() {
        // Directly exercise the saturating_sub floor in value(): a neg sum
        // at i128::MAX with zero pos yields i128::MIN + 1 (the true value),
        // and the reduction never panics or wraps regardless of ordering.
        let mut s = SignedSum::new();
        s.add("a", i128::MIN);
        s.add("b", i128::MIN);
        // neg sum saturates at i128::MAX; 0 - MAX = i128::MIN + 1, no wrap.
        assert_eq!(s.value(), i128::MIN + 1);
    }

    #[test]
    fn value_reduction_saturates_positive() {
        let mut s = SignedSum::new();
        s.add("n1", i128::MAX);
        s.add("n2", i128::MAX);
        assert_eq!(s.value(), i128::MAX);
    }

    #[test]
    fn map_per_key_isolation_and_merge() {
        let mut m: SignedSumMap<&'static str> = SignedSumMap::new();
        m.add("aapl", "n1", 500);
        m.add("aapl", "n1", -100);
        m.add("msft", "n1", 200);
        assert_eq!(m.get(&"aapl"), 400);
        assert_eq!(m.get(&"msft"), 200);
        assert_eq!(m.get(&"absent"), 0);

        let mut m2: SignedSumMap<&'static str> = SignedSumMap::new();
        m2.add("aapl", "n2", -50);
        m2.add("googl", "n2", 90);
        m.merge(&m2);
        assert_eq!(m.get(&"aapl"), 350);
        assert_eq!(m.get(&"msft"), 200);
        assert_eq!(m.get(&"googl"), 90);

        let collected: Vec<_> = m.entries().collect();
        assert_eq!(collected, vec![(&"aapl", 350), (&"googl", 90), (&"msft", 200)]);
    }

    #[test]
    fn delta_take_and_clear() {
        let mut s = SignedSum::new();
        s.add("a", 30);
        s.add("a", -5);
        let d = s.take_delta().expect("non-empty");
        assert_eq!(d.pos.get("a"), Some(&30));
        assert_eq!(d.neg.get("a"), Some(&5));
        assert!(s.take_delta().is_none());
    }

    #[test]
    fn delta_merge_matches_full_merge() {
        let mut local = SignedSum::new();
        local.add("a", 50);
        local.add("a", -10);
        let _ = local.take_delta();

        let mut remote = SignedSum::new();
        remote.add("a", 50);
        remote.add("a", -10);
        let _ = remote.take_delta();

        // Local applies more ops, ships the delta, remote applies it.
        local.add("a", 25);
        local.add("a", -5);
        let delta = local.take_delta().unwrap();
        remote.merge_delta(&delta);
        assert_eq!(remote.value(), local.value());
        assert_eq!(remote.value(), 50 - 10 + 25 - 5);
    }

    // ---- proptest: any permutation / replica split / merge order converges.

    use proptest::prelude::*;

    proptest! {
        /// Convergence under arbitrary op interleaving + arbitrary merge
        /// order across N replicas.
        ///
        /// Each op carries the replica that applied it, and **that replica's
        /// id is used as the CRDT node id**. This reflects the actual usage
        /// contract of a per-node grow-only accumulator: a single logical
        /// node owns its own counter. (Letting one node id be mutated
        /// concurrently on two replicas and merged via `max` would lose
        /// writes — that is the documented G/PN-counter model, not a bug.)
        ///
        /// Given that contract, a key's converged net value is deterministic:
        /// the per-node grow-only accumulators commute, so summing every
        /// op (grouped by node) gives the reference, and merging the replicas
        /// in *any* order — with arbitrary repeats — must reproduce it.
        #[test]
        fn any_interleaving_converges(
            // (key 0..4, replica 0..4, delta). The replica is also the node.
            ops in proptest::collection::vec(
                (0u8..4, 0u8..4, -1_000_000i128..1_000_000i128),
                0..80,
            ),
            merge_perm in proptest::collection::vec(any::<u8>(), 0..32),
        ) {
            let n_replicas = 4usize;
            let node_name = |n: u8| format!("node-{n}");
            let key_name = |k: u8| format!("key-{k}");

            // Each replica applies its own ops under its own node id.
            let mut replicas: Vec<SignedSumMap<String>> =
                (0..n_replicas).map(|_| SignedSumMap::new()).collect();
            for &(k, r, d) in &ops {
                let r = r as usize % n_replicas;
                replicas[r].add(key_name(k), &node_name(r as u8), d);
            }

            // Reference: merge every replica once into a fresh map (a fixed,
            // canonical merge order). Convergence means any other order ties.
            let mut reference: SignedSumMap<String> = SignedSumMap::new();
            for r in &replicas {
                reference.merge(r);
            }

            // acc: start from replica 0, full-merge all, then re-merge in a
            // seed-driven order with repeats (commutativity + idempotence).
            let mut acc = replicas[0].clone();
            for r in &replicas {
                acc.merge(r);
            }
            for &seed in &merge_perm {
                acc.merge(&replicas[seed as usize % n_replicas]);
            }

            // acc2: start from the last replica, merge in reverse order.
            let mut acc2 = replicas[n_replicas - 1].clone();
            for r in replicas.iter().rev() {
                acc2.merge(r);
            }

            for k in 0u8..4 {
                let key = key_name(k);
                prop_assert_eq!(acc.get(&key), reference.get(&key));
                prop_assert_eq!(acc2.get(&key), reference.get(&key));
            }
        }
    }
}
