/// Convergent replicated data type merge semantics.
///
/// Implementations form a join-semilattice: `merge` must be commutative,
/// associative, and idempotent.
///
/// **Sealed** as part of Phase 13 (idiomatic-rust sweep): only the
/// CRDTs in this crate may implement `CrdtMerge` directly. Downstream
/// users compose CRDTs via `ORMap<K, V>` / `LWWMap<K, V>` (where the
/// value is a CRDT) or wrap them in domain-specific structs.
pub trait CrdtMerge: Clone + private::Sealed {
    fn merge(&mut self, other: &Self);
}

/// Optional delta-CRDT layer: emit a small "delta" describing the
/// last local change and merge incoming deltas into the full state.
///
/// Phase 8.C of `docs/full-port-plan.md`. CRDTs that implement
/// [`DeltaCrdt`] participate in delta-gossip — the Replicator ships
/// `Self::Delta` to peers instead of the full state, dramatically
/// reducing wire traffic for hot keys.
///
/// CRDTs whose state is small (counters, flags) can implement this
/// trivially with `Delta = Self`. Sets / maps emit a sub-state
/// containing only the keys / tags that changed.
pub trait DeltaCrdt: CrdtMerge {
    /// Per-CRDT delta type — typically `Self` for small CRDTs, a
    /// sub-state for set / map shapes.
    type Delta: Clone + Send + 'static;

    /// Take and clear the accumulated local delta. Returns `None` if
    /// no local change has happened since the last call.
    fn take_delta(&mut self) -> Option<Self::Delta>;

    /// Merge an incoming delta into local state.
    fn merge_delta(&mut self, delta: &Self::Delta);
}

mod private {
    pub trait Sealed {}

    impl Sealed for crate::counters::GCounter {}
    impl Sealed for crate::counters::PNCounter {}
    impl Sealed for crate::flag::Flag {}
    impl<T: Eq + std::hash::Hash + Clone> Sealed for crate::sets::GSet<T> {}
    impl<T: Eq + std::hash::Hash + Clone> Sealed for crate::sets::OrSet<T> {}
    impl<T: Clone> Sealed for crate::register::LwwRegister<T> {}
    impl<K, V> Sealed for crate::maps::ORMap<K, V>
    where
        K: Eq + std::hash::Hash + Clone,
        V: super::CrdtMerge,
    {
    }
    impl<K, V> Sealed for crate::maps::LWWMap<K, V>
    where
        K: Eq + std::hash::Hash + Clone,
        V: Clone,
    {
    }
    impl<K> Sealed for crate::maps::PNCounterMap<K> where K: Eq + std::hash::Hash + Clone {}
    impl Sealed for crate::signed_sum::SignedSum {}
    impl<K: Ord + Clone> Sealed for crate::signed_sum::SignedSumMap<K> {}
    impl<K, V> Sealed for crate::maps::ORMultiMap<K, V>
    where
        K: Eq + std::hash::Hash + Clone,
        V: Eq + std::hash::Hash + Clone,
    {
    }
}
