//! FR-9 — Write-Once-Read-Many (WORM) storage with a tamper-evident hash
//! chain.
//!
//! Two independent guarantees, each opt-in via [`WormConfig`]:
//!
//! * **`deny_update_delete`** installs dialect DDL that physically rejects
//!   `UPDATE`/`DELETE` on `event_journal` (SQLite: `BEFORE UPDATE/DELETE`
//!   triggers raising `RAISE(ABORT)`). Once on, the only legal mutation is
//!   `INSERT` — the journal is append-only.
//!
//! * **`hash_chain`** maintains a per-`persistence_id` Merkle-style chain:
//!   each row stores `prev_hash` (the prior row's `row_hash`) and
//!   `row_hash = SHA-256(prev_hash || persistence_id || seq_le || payload ||
//!   created_at_le)`. Any retroactive edit to a payload breaks every
//!   downstream link, which [`IntegrityVerify::verify_chain`] detects and
//!   localizes to the first bad sequence number.
//!
//! The columns ride in the additive `002_worm.sql` migration; no change to
//! [`atomr_persistence::PersistentRepr`].

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::journal::SqlJournal;

/// WORM behavior toggles, stored on a [`SqlJournal`] via
/// [`SqlJournal::with_worm`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WormConfig {
    /// Maintain the per-pid tamper-evident SHA-256 hash chain on write.
    pub hash_chain: bool,
    /// Install DDL that rejects UPDATE/DELETE on `event_journal`.
    pub deny_update_delete: bool,
}

impl WormConfig {
    /// Both protections on — the strongest WORM posture.
    pub fn enforced() -> Self {
        Self { hash_chain: true, deny_update_delete: true }
    }
}

/// Errors surfaced while verifying a hash chain.
#[derive(Debug, Error)]
pub enum IntegrityError {
    #[error("backend error: {0}")]
    Backend(String),
}

impl IntegrityError {
    pub fn backend(e: impl std::fmt::Display) -> Self {
        Self::Backend(e.to_string())
    }
}

/// Result of verifying a persistence id's hash chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainProof {
    /// Every stored `row_hash` matches a fresh recomputation.
    Intact { rows: u64 },
    /// The chain diverges at `first_bad_sequence_nr` (the earliest row whose
    /// recomputed hash or `prev_hash` link does not match what is stored).
    Tampered { first_bad_sequence_nr: u64 },
}

/// Compute the canonical row hash for a journal row.
///
/// `row_hash = SHA-256(prev_hash || persistence_id || seq_le || payload || created_at_le)`
/// where `prev_hash` is the previous row's `row_hash` for the same pid (empty
/// for the first row in the chain).
pub(crate) fn compute_row_hash(
    prev_hash: &[u8],
    persistence_id: &str,
    sequence_nr: u64,
    payload: &[u8],
    created_at: i64,
) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(prev_hash);
    h.update(persistence_id.as_bytes());
    h.update(sequence_nr.to_le_bytes());
    h.update(payload);
    h.update(created_at.to_le_bytes());
    h.finalize().to_vec()
}

/// Recompute and validate a persistence id's hash chain.
#[async_trait]
pub trait IntegrityVerify {
    async fn verify_chain(&self, pid: &str) -> Result<ChainProof, IntegrityError>;
}

#[async_trait]
impl IntegrityVerify for SqlJournal {
    async fn verify_chain(&self, pid: &str) -> Result<ChainProof, IntegrityError> {
        // Pull rows in sequence order with the stored chain columns.
        let rows: Vec<(i64, Vec<u8>, i64, Option<Vec<u8>>, Option<Vec<u8>>)> = sqlx::query_as(
            "SELECT sequence_nr, payload, created_at, prev_hash, row_hash \
             FROM event_journal WHERE persistence_id = ? ORDER BY sequence_nr ASC",
        )
        .bind(pid)
        .fetch_all(self.pool())
        .await
        .map_err(IntegrityError::backend)?;

        let mut expected_prev: Vec<u8> = Vec::new();
        let mut count = 0u64;
        for (seq, payload, created_at, stored_prev, stored_row) in rows {
            count += 1;
            let stored_prev = stored_prev.unwrap_or_default();
            // The prev_hash recorded on this row must equal the running hash.
            if stored_prev != expected_prev {
                return Ok(ChainProof::Tampered { first_bad_sequence_nr: seq as u64 });
            }
            let recomputed = compute_row_hash(&expected_prev, pid, seq as u64, &payload, created_at);
            match stored_row {
                Some(stored) if stored == recomputed => {
                    expected_prev = recomputed;
                }
                _ => {
                    return Ok(ChainProof::Tampered { first_bad_sequence_nr: seq as u64 });
                }
            }
        }
        Ok(ChainProof::Intact { rows: count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_hash_is_stable_and_chains() {
        let h1 = compute_row_hash(&[], "p", 1, b"a", 100);
        let h1b = compute_row_hash(&[], "p", 1, b"a", 100);
        assert_eq!(h1, h1b, "deterministic");
        let h2 = compute_row_hash(&h1, "p", 2, b"b", 200);
        // Changing any input changes the hash.
        assert_ne!(h1, h2);
        let h2_tampered = compute_row_hash(&h1, "p", 2, b"B", 200);
        assert_ne!(h2, h2_tampered);
    }
}
