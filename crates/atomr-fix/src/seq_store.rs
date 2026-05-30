//! Pluggable sequence-number store.
//!
//! FIX requires both peers to maintain a strictly increasing outbound
//! `MsgSeqNum` and to track the highest inbound sequence number they have
//! processed, so that a reconnect can detect gaps and issue / honour a
//! `ResendRequest`. [`FixSeqStore`] abstracts that persistence point.
//!
//! [`InMemorySeqStore`] is the default, test-friendly implementation backed by
//! atomics. A durable implementation (for example one backed by
//! `atomr-persistence`) only needs to implement the same four async methods;
//! the session layer interacts with the store exclusively through this trait,
//! so swapping in a crash-safe store requires no changes to the FSM. We
//! deliberately keep `atomr-persistence` out of this crate's dependency tree —
//! this trait *is* the integration point.

use std::sync::atomic::{AtomicU64, Ordering};

/// Persistence for the two sequence counters a FIX session must keep.
///
/// * `next_out` returns the sequence number to stamp on the *next* outbound
///   message and atomically advances the counter, so two concurrent sends can
///   never reuse a number.
/// * `observed_in` records that an inbound message with sequence number `n`
///   was processed (tracking the high-water mark).
/// * `current_in` returns the *next expected* inbound sequence number, i.e.
///   one past the highest observed.
/// * `reset` returns both counters to their initial state (used for
///   `ResetSeqNumFlag=Y` logon and `SequenceReset`).
#[async_trait::async_trait]
pub trait FixSeqStore: Send + Sync {
    /// The next outbound `MsgSeqNum`, advancing the counter.
    async fn next_out(&self) -> u64;

    /// Peek the next outbound `MsgSeqNum` without advancing (for building a
    /// resend range or diagnostics).
    async fn peek_out(&self) -> u64;

    /// Record that an inbound message with `MsgSeqNum == n` was processed.
    async fn observed_in(&self, n: u64);

    /// The next *expected* inbound `MsgSeqNum` (one past the highest observed).
    async fn current_in(&self) -> u64;

    /// Reset both counters to 1 (next outbound = 1, next expected inbound = 1).
    async fn reset(&self);
}

/// In-memory [`FixSeqStore`] backed by atomics. Counters start at 1, matching
/// the FIX convention that the first message of a session carries
/// `MsgSeqNum=1`.
#[derive(Debug)]
pub struct InMemorySeqStore {
    /// Next outbound sequence number to hand out.
    out: AtomicU64,
    /// Next expected inbound sequence number.
    in_expected: AtomicU64,
}

impl InMemorySeqStore {
    /// A fresh store with both counters at 1.
    pub fn new() -> Self {
        InMemorySeqStore { out: AtomicU64::new(1), in_expected: AtomicU64::new(1) }
    }

    /// A store seeded with explicit counters — useful for simulating recovery
    /// after a reconnect where the previous session left off mid-stream.
    pub fn with_counters(next_out: u64, next_in: u64) -> Self {
        InMemorySeqStore { out: AtomicU64::new(next_out.max(1)), in_expected: AtomicU64::new(next_in.max(1)) }
    }
}

impl Default for InMemorySeqStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl FixSeqStore for InMemorySeqStore {
    async fn next_out(&self) -> u64 {
        self.out.fetch_add(1, Ordering::SeqCst)
    }

    async fn peek_out(&self) -> u64 {
        self.out.load(Ordering::SeqCst)
    }

    async fn observed_in(&self, n: u64) {
        // High-water mark: next expected = max(current, n + 1).
        let want = n.saturating_add(1);
        let mut cur = self.in_expected.load(Ordering::SeqCst);
        while want > cur {
            match self.in_expected.compare_exchange_weak(cur, want, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    async fn current_in(&self) -> u64 {
        self.in_expected.load(Ordering::SeqCst)
    }

    async fn reset(&self) {
        self.out.store(1, Ordering::SeqCst);
        self.in_expected.store(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn next_out_increments_and_persists() {
        let store = InMemorySeqStore::new();
        assert_eq!(store.next_out().await, 1);
        assert_eq!(store.next_out().await, 2);
        assert_eq!(store.next_out().await, 3);
        // peek does not advance.
        assert_eq!(store.peek_out().await, 4);
        assert_eq!(store.peek_out().await, 4);
    }

    #[tokio::test]
    async fn observed_in_tracks_high_water_mark() {
        let store = InMemorySeqStore::new();
        assert_eq!(store.current_in().await, 1);
        store.observed_in(1).await;
        assert_eq!(store.current_in().await, 2);
        store.observed_in(2).await;
        assert_eq!(store.current_in().await, 3);
        // An out-of-order lower number does not regress the high-water mark.
        store.observed_in(1).await;
        assert_eq!(store.current_in().await, 3);
        // Jumping ahead advances it.
        store.observed_in(9).await;
        assert_eq!(store.current_in().await, 10);
    }

    #[tokio::test]
    async fn reset_returns_to_initial() {
        let store = InMemorySeqStore::with_counters(50, 60);
        assert_eq!(store.peek_out().await, 50);
        assert_eq!(store.current_in().await, 60);
        store.reset().await;
        assert_eq!(store.peek_out().await, 1);
        assert_eq!(store.current_in().await, 1);
    }
}
