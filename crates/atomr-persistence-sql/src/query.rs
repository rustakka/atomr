//! `ReadJournal` adapter that reuses the SQL journal for event-by-id /
//! event-by-tag queries.

use std::sync::Arc;

use async_trait::async_trait;
use atomr_persistence::{Journal, JournalError};
use atomr_persistence_query::{EventEnvelope, ReadJournal};

use crate::journal::SqlJournal;

pub struct SqlReadJournal {
    journal: Arc<SqlJournal>,
}

impl SqlReadJournal {
    pub fn new(journal: Arc<SqlJournal>) -> Self {
        Self { journal }
    }

    pub async fn events_by_tag(
        &self,
        tag: &str,
        from_offset: u64,
        max: u64,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        let reprs = self.journal.events_by_tag(tag, from_offset, max).await?;
        Ok(reprs.into_iter().map(Into::into).collect())
    }
}

/// Clamp a `u128` nanosecond timestamp into the `i64` column domain. Values
/// beyond `i64::MAX` saturate (effectively "now / always").
fn clamp_nanos(v: u128) -> i64 {
    if v > i64::MAX as u128 {
        i64::MAX
    } else {
        v as i64
    }
}

#[async_trait]
impl ReadJournal for SqlReadJournal {
    async fn events_by_persistence_id(
        &self,
        persistence_id: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        let reprs = self.journal.replay_messages(persistence_id, from, to, u64::MAX).await?;
        Ok(reprs.into_iter().map(Into::into).collect())
    }

    /// FR-8 — true system-time as-of, backed by the `system_time` column.
    async fn events_as_of(
        &self,
        pid: &str,
        system_time_nanos: u128,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        let reprs = self.journal.replay_as_of(pid, clamp_nanos(system_time_nanos)).await?;
        Ok(reprs.into_iter().map(Into::into).collect())
    }

    /// FR-8 — true bitemporal slice, backed by `system_time` + `valid_time`.
    async fn events_valid_as_of(
        &self,
        pid: &str,
        valid_time_nanos: u128,
        system_time_nanos: u128,
    ) -> Result<Vec<EventEnvelope>, JournalError> {
        let reprs = self
            .journal
            .replay_valid_as_of(pid, clamp_nanos(valid_time_nanos), clamp_nanos(system_time_nanos))
            .await?;
        Ok(reprs.into_iter().map(Into::into).collect())
    }
}
