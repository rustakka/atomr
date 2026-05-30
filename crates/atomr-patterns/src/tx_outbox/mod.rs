//! FR-9 — *Transactional* outbox: atomic event + outbox write.
//!
//! Distinct from [`crate::outbox`], which is a journal-**tailing relay** that
//! re-publishes already-committed events at-least-once on a polling loop.
//! This module instead writes the event row **and** its outbox row inside the
//! **same** [`sqlx::Transaction`] the caller controls, so the two either
//! commit together or roll back together — there is no window where an event
//! exists without its outbox entry (or vice versa).
//!
//! The caller owns commit/rollback: [`TxOutbox::persist_with`] only enqueues
//! both INSERTs on the supplied transaction.
//!
//! ```ignore
//! let mut tx = pool.begin().await?;
//! TxOutbox.persist_with(&mut tx, event_row, outbox_row).await?;
//! tx.commit().await?; // both rows land atomically
//! ```

use thiserror::Error;

/// The event row to append to `event_journal`. A small param struct so we
/// never need to touch [`atomr_persistence::PersistentRepr`].
#[derive(Debug, Clone)]
pub struct EventRow {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub payload: Vec<u8>,
    pub manifest: String,
    pub writer_uuid: String,
}

/// The outbox row to append to `outbox` in the same transaction.
#[derive(Debug, Clone)]
pub struct OutboxRow {
    pub topic: String,
    pub payload: Vec<u8>,
}

/// Errors from the transactional outbox write.
#[derive(Debug, Error)]
pub enum OutboxError {
    #[error("backend error: {0}")]
    Backend(String),
}

impl OutboxError {
    pub fn backend(e: impl std::fmt::Display) -> Self {
        Self::Backend(e.to_string())
    }
}

/// Zero-sized handle for the transactional outbox write.
pub struct TxOutbox;

impl TxOutbox {
    /// Enqueue both the event INSERT and the outbox INSERT on `tx`.
    ///
    /// Both statements execute against the *same* transaction, so the caller's
    /// subsequent `tx.commit()` makes both durable atomically, and dropping /
    /// rolling back `tx` discards both. This method does **not** commit.
    ///
    /// The target tables are assumed to exist (the SQL provider's
    /// `event_journal` plus an `outbox(topic, payload, created_at)` table the
    /// application provisions); see the crate tests for the reference DDL.
    pub async fn persist_with(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Any>,
        event: EventRow,
        outbox: OutboxRow,
    ) -> Result<(), OutboxError> {
        let created_at = now_millis();

        sqlx::query(
            "INSERT INTO event_journal \
             (persistence_id, sequence_nr, payload, manifest, writer_uuid, deleted, created_at) \
             VALUES (?, ?, ?, ?, ?, 0, ?)",
        )
        .bind(&event.persistence_id)
        .bind(event.sequence_nr as i64)
        .bind(event.payload)
        .bind(&event.manifest)
        .bind(&event.writer_uuid)
        .bind(created_at)
        .execute(&mut **tx)
        .await
        .map_err(OutboxError::backend)?;

        sqlx::query("INSERT INTO outbox (topic, payload, created_at) VALUES (?, ?, ?)")
            .bind(&outbox.topic)
            .bind(outbox.payload)
            .bind(created_at)
            .execute(&mut **tx)
            .await
            .map_err(OutboxError::backend)?;

        Ok(())
    }
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}
