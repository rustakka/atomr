//! atomr-persistence-query.
//!
//! Phase 11 of `docs/full-port-plan.md` extends the read-journal
//! surface to match upstream: `events_by_persistence_id`,
//! `events_by_tag`, `current_*` variants, `all_persistence_ids`, and
//! a typed [`Offset`] type.

use async_trait::async_trait;
use atomr_persistence::{Journal, JournalError, PersistentRepr};

/// Typed read-journal offset. The in-memory backend uses `Sequence`
/// numbers; a SQL backend might emit `TimeBased` UUIDs. `NoOffset`
/// means "from the start."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[derive(Default)]
pub enum Offset {
    #[default]
    NoOffset,
    Sequence(u64),
    TimeBased(u128),
}

impl Offset {
    pub fn as_sequence(self) -> Option<u64> {
        match self {
            Self::NoOffset => Some(0),
            Self::Sequence(n) => Some(n),
            Self::TimeBased(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventEnvelope {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub payload: Vec<u8>,
    pub offset: u64,
    pub tags: Vec<String>,
    /// Manifest tag the event was written with. Mirrors
    /// [`PersistentRepr::manifest`]; used by readers that dispatch
    /// decoding based on event-schema version.
    pub manifest: String,
}

impl From<PersistentRepr> for EventEnvelope {
    fn from(r: PersistentRepr) -> Self {
        Self {
            persistence_id: r.persistence_id,
            sequence_nr: r.sequence_nr,
            payload: r.payload,
            offset: r.sequence_nr,
            tags: r.tags,
            manifest: r.manifest,
        }
    }
}

/// Read-journal surface. `current_*` variants take a snapshot at call
/// time; the non-current variants are tail-following (live) — backends
/// that only support snapshots return the snapshot and let callers
/// re-poll.
#[async_trait]
pub trait ReadJournal: Send + Sync + 'static {
    /// Replay events for a single persistence id, sequence-number
    /// bounded.
    async fn events_by_persistence_id(
        &self,
        persistence_id: &str,
        from_sequence_nr: u64,
        to_sequence_nr: u64,
    ) -> Result<Vec<EventEnvelope>, JournalError>;

    /// Snapshot variant of [`Self::events_by_persistence_id`] —
    /// default impl is identical (in-memory journals don't tail).
    async fn current_events_by_persistence_id(
        &self,
        persistence_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        self.events_by_persistence_id(persistence_id, from, to).await
    }

    /// All events with a given tag, returned in offset order.
    /// Default impl is empty so backends without tag indexing don't
    /// silently mis-behave.
    async fn events_by_tag(&self, _tag: &str, _offset: Offset) -> Result<Vec<EventEnvelope>, JournalError> {
        Ok(Vec::new())
    }

    async fn current_events_by_tag(
        &self,
        tag: &str,
        offset: Offset,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        self.events_by_tag(tag, offset).await
    }

    /// Distinct persistence ids known to the backend. Default impl
    /// returns empty (backends without an id index opt in).
    async fn all_persistence_ids(&self) -> Result<Vec<String>, JournalError> {
        Ok(Vec::new())
    }

    async fn current_persistence_ids(&self) -> Result<Vec<String>, JournalError> {
        self.all_persistence_ids().await
    }

    /// FR-8 — bitemporal *system-time* as-of query: return the events for
    /// `pid` **as they were known to the system at** `system_time_nanos`,
    /// excluding any later-recorded restatement (no lookahead).
    ///
    /// The default impl has no notion of system time, so it falls back to the
    /// full current event list — only a backend that records a per-row
    /// `system_time` (e.g. the SQL provider) can honor the cutoff. Override
    /// to get a true as-of slice.
    async fn events_as_of(
        &self,
        pid: &str,
        _system_time_nanos: u128,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        self.current_events_by_persistence_id(pid, 0, u64::MAX).await
    }

    /// FR-8 — full bitemporal slice: events for `pid` whose *valid time* is at
    /// or before `valid_time_nanos`, as known to the system at
    /// `system_time_nanos`. Distinguishes a corrected value (recorded later)
    /// from the value originally known at an earlier system time.
    ///
    /// Default impl ignores both time axes (returns the current list); only
    /// the SQL backend provides the true bitemporal slice.
    async fn events_valid_as_of(
        &self,
        pid: &str,
        _valid_time_nanos: u128,
        _system_time_nanos: u128,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        self.current_events_by_persistence_id(pid, 0, u64::MAX).await
    }
}

pub struct SimpleReadJournal<J: Journal> {
    journal: std::sync::Arc<J>,
}

impl<J: Journal> SimpleReadJournal<J> {
    pub fn new(journal: std::sync::Arc<J>) -> Self {
        Self { journal }
    }
}

#[async_trait]
impl<J: Journal> ReadJournal for SimpleReadJournal<J> {
    async fn events_by_persistence_id(
        &self,
        persistence_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        let reprs = self.journal.replay_messages(persistence_id, from, to, u64::MAX).await?;
        Ok(reprs.into_iter().map(Into::into).collect())
    }

    async fn events_by_tag(&self, tag: &str, offset: Offset) -> Result<Vec<EventEnvelope>, JournalError> {
        let from_seq = offset.as_sequence().unwrap_or(0);
        // Prefer the backend's indexed query when available.
        let backend_results = self.journal.events_by_tag(tag, from_seq, u64::MAX).await?;
        if !backend_results.is_empty() {
            return Ok(backend_results.into_iter().map(Into::into).collect());
        }
        // Fall back to scanning per-pid when the backend hasn't
        // implemented its own tag index.
        let ids = self.journal.all_persistence_ids().await?;
        let mut out = Vec::new();
        for id in ids {
            let reprs = self.journal.replay_messages(&id, from_seq, u64::MAX, u64::MAX).await?;
            for r in reprs {
                if r.tags.iter().any(|t| t == tag) {
                    out.push(r.into());
                }
            }
        }
        Ok(out)
    }

    async fn all_persistence_ids(&self) -> Result<Vec<String>, JournalError> {
        self.journal.all_persistence_ids().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomr_persistence::{InMemoryJournal, Journal, PersistentRepr};
    use std::sync::Arc;

    fn repr(pid: &str, seq: u64, tags: &[&str]) -> PersistentRepr {
        PersistentRepr {
            persistence_id: pid.into(),
            sequence_nr: seq,
            payload: vec![seq as u8],
            manifest: "evt".into(),
            writer_uuid: "w".into(),
            deleted: false,
            tags: tags.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[tokio::test]
    async fn events_by_persistence_id_replays_range() {
        let j = Arc::new(InMemoryJournal::default());
        j.write_messages(vec![repr("a", 1, &[]), repr("a", 2, &[]), repr("a", 3, &[])]).await.unwrap();
        let q = SimpleReadJournal::new(j);
        let evs = q.events_by_persistence_id("a", 1, 2).await.unwrap();
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].sequence_nr, 1);
        assert_eq!(evs[1].sequence_nr, 2);
    }

    #[tokio::test]
    async fn current_variant_matches_live() {
        let j = Arc::new(InMemoryJournal::default());
        j.write_messages(vec![repr("a", 1, &[])]).await.unwrap();
        let q = SimpleReadJournal::new(j);
        let live = q.events_by_persistence_id("a", 1, 99).await.unwrap();
        let snap = q.current_events_by_persistence_id("a", 1, 99).await.unwrap();
        assert_eq!(live.len(), snap.len());
    }

    #[tokio::test]
    async fn offset_sequence_round_trips() {
        assert_eq!(Offset::Sequence(7).as_sequence(), Some(7));
        assert_eq!(Offset::NoOffset.as_sequence(), Some(0));
        assert_eq!(Offset::TimeBased(123).as_sequence(), None);
    }

    #[tokio::test]
    async fn events_by_tag_returns_tagged_events_across_pids() {
        let j = Arc::new(InMemoryJournal::default());
        j.write_messages(vec![
            repr("a", 1, &["red"]),
            repr("a", 2, &["blue"]),
            repr("b", 1, &["red", "hot"]),
            repr("b", 2, &["green"]),
        ])
        .await
        .unwrap();
        let q = SimpleReadJournal::new(j);
        let red = q.events_by_tag("red", Offset::NoOffset).await.unwrap();
        assert_eq!(red.len(), 2);
        let blue = q.events_by_tag("blue", Offset::NoOffset).await.unwrap();
        assert_eq!(blue.len(), 1);
        let none = q.events_by_tag("nope", Offset::NoOffset).await.unwrap();
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn events_by_tag_respects_offset() {
        let j = Arc::new(InMemoryJournal::default());
        j.write_messages(vec![repr("a", 1, &["t"]), repr("a", 2, &["t"]), repr("a", 3, &["t"])])
            .await
            .unwrap();
        let q = SimpleReadJournal::new(j);
        let from2 = q.events_by_tag("t", Offset::Sequence(2)).await.unwrap();
        assert_eq!(from2.len(), 2);
        assert_eq!(from2[0].sequence_nr, 2);
    }

    #[tokio::test]
    async fn events_as_of_default_returns_current_events() {
        // The default (non-SQL) impl has no system_time, so as-of degrades to
        // the full current list — documented behavior.
        let j = Arc::new(InMemoryJournal::default());
        j.write_messages(vec![repr("a", 1, &[]), repr("a", 2, &[])]).await.unwrap();
        let q = SimpleReadJournal::new(j);
        let asof = q.events_as_of("a", 12345).await.unwrap();
        assert_eq!(asof.len(), 2);
        let bit = q.events_valid_as_of("a", 1, 2).await.unwrap();
        assert_eq!(bit.len(), 2);
    }

    #[tokio::test]
    async fn all_persistence_ids_lists_distinct_writers() {
        let j = Arc::new(InMemoryJournal::default());
        j.write_messages(vec![repr("a", 1, &[]), repr("b", 1, &[]), repr("a", 2, &[])]).await.unwrap();
        let q = SimpleReadJournal::new(j);
        let mut ids = q.all_persistence_ids().await.unwrap();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    }
}
