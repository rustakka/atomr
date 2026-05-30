//! `Journal` implementation backed by sqlx.
//!
//! Uses the `sqlx::Any` pool so the same code targets every supported
//! dialect. Tag writes go to a companion `event_tags` table that powers
//! `events_by_tag`.

use std::sync::Arc;

use async_trait::async_trait;
use atomr_persistence::{Journal, JournalError, PersistentRepr};
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;

use crate::config::SqlConfig;
use crate::schema::{ensure_schema, init_drivers};
use crate::worm::{compute_row_hash, WormConfig};

/// Saturating cast from `u64` to `i64` so `u64::MAX` sentinels turn into
/// `i64::MAX` instead of wrapping negative.
fn clamp_i64(v: u64) -> i64 {
    if v > i64::MAX as u64 {
        i64::MAX
    } else {
        v as i64
    }
}

/// FR-8: extract `valid_time` (nanos) from a `valid_time:<nanos>` tag.
/// Returns `None` when absent so the column stays NULL.
fn parse_valid_time(tags: &[String]) -> Option<i64> {
    for t in tags {
        if let Some(rest) = t.strip_prefix("valid_time:") {
            if let Ok(n) = rest.parse::<i64>() {
                return Some(n);
            }
        }
    }
    None
}

pub struct SqlJournal {
    pool: AnyPool,
    cfg: SqlConfig,
    worm: WormConfig,
}

impl SqlJournal {
    /// Connect, install drivers, and optionally run migrations.
    pub async fn connect(cfg: SqlConfig) -> Result<Arc<Self>, JournalError> {
        init_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(cfg.max_connections)
            .connect(&cfg.url)
            .await
            .map_err(JournalError::backend)?;
        ensure_schema(&pool, &cfg).await?;
        Ok(Arc::new(Self { pool, cfg, worm: WormConfig::default() }))
    }

    /// Reuse an existing pool (for tests or app-wide sharing).
    pub async fn from_pool(pool: AnyPool, cfg: SqlConfig) -> Result<Arc<Self>, JournalError> {
        ensure_schema(&pool, &cfg).await?;
        Ok(Arc::new(Self { pool, cfg, worm: WormConfig::default() }))
    }

    /// Turn on WORM protections (FR-9).
    ///
    /// When `deny_update_delete` is set, this installs the dialect's
    /// append-only DDL immediately. When `hash_chain` is set, subsequent
    /// writes maintain the per-pid tamper-evident hash chain. Consumes the
    /// (typically freshly-built) journal and returns a reconfigured `Arc`.
    pub async fn with_worm(self: Arc<Self>, worm: WormConfig) -> Result<Arc<Self>, JournalError> {
        if worm.deny_update_delete {
            crate::schema::install_worm_triggers(&self.pool, &self.cfg).await?;
        }
        Ok(Arc::new(Self { pool: self.pool.clone(), cfg: self.cfg.clone(), worm }))
    }

    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }

    pub fn config(&self) -> &SqlConfig {
        &self.cfg
    }

    pub fn worm_config(&self) -> WormConfig {
        self.worm
    }

    async fn current_highest(&self, pid: &str) -> Result<u64, JournalError> {
        let row: Option<(Option<i64>,)> =
            sqlx::query_as("SELECT MAX(sequence_nr) FROM event_journal WHERE persistence_id = ?")
                .bind(pid)
                .fetch_optional(&self.pool)
                .await
                .map_err(JournalError::backend)?;
        Ok(row.and_then(|(v,)| v).map(|v| v as u64).unwrap_or(0))
    }

    /// FR-8 — system-time as-of: rows recorded at or before
    /// `system_time_nanos`. `system_time` falls back to `created_at` for rows
    /// written before the column existed. Later-recorded restatements (whose
    /// `system_time` is greater) are excluded → no lookahead.
    pub async fn replay_as_of(
        &self,
        pid: &str,
        system_time_nanos: i64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let rows: Vec<(String, i64, Vec<u8>, String, String, i32)> = sqlx::query_as(
            "SELECT persistence_id, sequence_nr, payload, manifest, writer_uuid, deleted \
             FROM event_journal \
             WHERE persistence_id = ? AND deleted = 0 \
               AND COALESCE(system_time, created_at) <= ? \
             ORDER BY sequence_nr ASC",
        )
        .bind(pid)
        .bind(system_time_nanos)
        .fetch_all(&self.pool)
        .await
        .map_err(JournalError::backend)?;
        self.hydrate(rows).await
    }

    /// FR-8 — bitemporal slice: rows whose `valid_time` is at or before
    /// `valid_time_nanos`, restricted to what was known to the system at
    /// `system_time_nanos`. Rows without a `valid_time` are treated as valid
    /// from their `system_time` (always-valid) so they remain visible.
    pub async fn replay_valid_as_of(
        &self,
        pid: &str,
        valid_time_nanos: i64,
        system_time_nanos: i64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let rows: Vec<(String, i64, Vec<u8>, String, String, i32)> = sqlx::query_as(
            "SELECT persistence_id, sequence_nr, payload, manifest, writer_uuid, deleted \
             FROM event_journal \
             WHERE persistence_id = ? AND deleted = 0 \
               AND COALESCE(system_time, created_at) <= ? \
               AND COALESCE(valid_time, COALESCE(system_time, created_at)) <= ? \
             ORDER BY sequence_nr ASC",
        )
        .bind(pid)
        .bind(system_time_nanos)
        .bind(valid_time_nanos)
        .fetch_all(&self.pool)
        .await
        .map_err(JournalError::backend)?;
        self.hydrate(rows).await
    }

    /// Attach tags to bare journal rows, reproducing `replay_messages` shape.
    async fn hydrate(
        &self,
        rows: Vec<(String, i64, Vec<u8>, String, String, i32)>,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let mut out = Vec::with_capacity(rows.len());
        for (pid, seq, payload, manifest, writer, deleted) in rows {
            let tags: Vec<(String,)> =
                sqlx::query_as("SELECT tag FROM event_tags WHERE persistence_id = ? AND sequence_nr = ?")
                    .bind(&pid)
                    .bind(seq)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(JournalError::backend)?;
            out.push(PersistentRepr {
                persistence_id: pid,
                sequence_nr: seq as u64,
                payload,
                manifest,
                writer_uuid: writer,
                deleted: deleted != 0,
                tags: tags.into_iter().map(|(t,)| t).collect(),
            });
        }
        Ok(out)
    }
}

#[async_trait]
impl Journal for SqlJournal {
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> Result<(), JournalError> {
        if messages.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(JournalError::backend)?;
        let mut by_pid: std::collections::BTreeMap<String, Vec<PersistentRepr>> =
            std::collections::BTreeMap::new();
        for m in messages {
            by_pid.entry(m.persistence_id.clone()).or_default().push(m);
        }
        for (pid, batch) in by_pid {
            let row: Option<(Option<i64>,)> =
                sqlx::query_as("SELECT MAX(sequence_nr) FROM event_journal WHERE persistence_id = ?")
                    .bind(&pid)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(JournalError::backend)?;
            let start = row.and_then(|(v,)| v).map(|v| v as u64 + 1).unwrap_or(1);

            // Seed the running hash from the latest existing row so the chain
            // survives across separate write batches.
            let mut prev_hash: Vec<u8> = if self.worm.hash_chain {
                let last: Option<(Option<Vec<u8>>,)> = sqlx::query_as(
                    "SELECT row_hash FROM event_journal WHERE persistence_id = ? \
                     ORDER BY sequence_nr DESC LIMIT 1",
                )
                .bind(&pid)
                .fetch_optional(&mut *tx)
                .await
                .map_err(JournalError::backend)?;
                last.and_then(|(h,)| h).unwrap_or_default()
            } else {
                Vec::new()
            };

            for (expected, msg) in (start..).zip(batch) {
                if msg.sequence_nr != expected {
                    return Err(JournalError::SequenceOutOfOrder { expected, got: msg.sequence_nr });
                }
                let created_at = chrono::Utc::now().timestamp_millis();
                // FR-8: system_time is backend-assigned (defaults to created_at);
                // valid_time is parsed from a `valid_time:<nanos>` tag if present.
                let system_time = created_at;
                let valid_time = parse_valid_time(&msg.tags);

                // FR-9: compute the chain hash for this row when enabled.
                let (row_hash_opt, prev_for_insert): (Option<Vec<u8>>, Option<Vec<u8>>) =
                    if self.worm.hash_chain {
                        let prev_for_insert =
                            if prev_hash.is_empty() { None } else { Some(prev_hash.clone()) };
                        let rh = compute_row_hash(
                            &prev_hash,
                            &msg.persistence_id,
                            msg.sequence_nr,
                            &msg.payload,
                            created_at,
                        );
                        prev_hash = rh.clone();
                        (Some(rh), prev_for_insert)
                    } else {
                        (None, None)
                    };

                sqlx::query(
                    "INSERT INTO event_journal (persistence_id, sequence_nr, payload, manifest, writer_uuid, deleted, created_at, prev_hash, row_hash, system_time, valid_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&msg.persistence_id)
                .bind(msg.sequence_nr as i64)
                .bind(msg.payload.clone())
                .bind(&msg.manifest)
                .bind(&msg.writer_uuid)
                .bind(0i32)
                .bind(created_at)
                .bind(prev_for_insert)
                .bind(row_hash_opt)
                .bind(system_time)
                .bind(valid_time)
                .execute(&mut *tx)
                .await
                .map_err(JournalError::backend)?;
                for tag in &msg.tags {
                    sqlx::query("INSERT INTO event_tags (persistence_id, sequence_nr, tag) VALUES (?, ?, ?)")
                        .bind(&msg.persistence_id)
                        .bind(msg.sequence_nr as i64)
                        .bind(tag)
                        .execute(&mut *tx)
                        .await
                        .map_err(JournalError::backend)?;
                }
            }
        }
        tx.commit().await.map_err(JournalError::backend)?;
        Ok(())
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> Result<(), JournalError> {
        sqlx::query("UPDATE event_journal SET deleted = 1 WHERE persistence_id = ? AND sequence_nr <= ?")
            .bind(persistence_id)
            .bind(to_sequence_nr as i64)
            .execute(&self.pool)
            .await
            .map_err(JournalError::backend)?;
        Ok(())
    }

    async fn replay_messages(
        &self,
        persistence_id: &str,
        from: u64,
        to: u64,
        max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let limit = clamp_i64(max);
        let to_bound = clamp_i64(to);
        let from_bound = clamp_i64(from);
        let rows: Vec<(String, i64, Vec<u8>, String, String, i32)> = sqlx::query_as(
            "SELECT persistence_id, sequence_nr, payload, manifest, writer_uuid, deleted FROM event_journal \
             WHERE persistence_id = ? AND sequence_nr >= ? AND sequence_nr <= ? AND deleted = 0 \
             ORDER BY sequence_nr ASC LIMIT ?",
        )
        .bind(persistence_id)
        .bind(from_bound)
        .bind(to_bound)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(JournalError::backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for (pid, seq, payload, manifest, writer, deleted) in rows {
            let tags: Vec<(String,)> =
                sqlx::query_as("SELECT tag FROM event_tags WHERE persistence_id = ? AND sequence_nr = ?")
                    .bind(&pid)
                    .bind(seq)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(JournalError::backend)?;
            out.push(PersistentRepr {
                persistence_id: pid,
                sequence_nr: seq as u64,
                payload,
                manifest,
                writer_uuid: writer,
                deleted: deleted != 0,
                tags: tags.into_iter().map(|(t,)| t).collect(),
            });
        }
        Ok(out)
    }

    async fn highest_sequence_nr(
        &self,
        persistence_id: &str,
        _from_sequence_nr: u64,
    ) -> Result<u64, JournalError> {
        self.current_highest(persistence_id).await
    }

    async fn events_by_tag(
        &self,
        tag: &str,
        from_offset: u64,
        max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let limit = clamp_i64(max);
        let rows: Vec<(String, i64, Vec<u8>, String, String, i32)> = sqlx::query_as(
            "SELECT j.persistence_id, j.sequence_nr, j.payload, j.manifest, j.writer_uuid, j.deleted \
             FROM event_journal j INNER JOIN event_tags t \
             ON j.persistence_id = t.persistence_id AND j.sequence_nr = t.sequence_nr \
             WHERE t.tag = ? AND j.sequence_nr >= ? AND j.deleted = 0 \
             ORDER BY j.persistence_id, j.sequence_nr ASC LIMIT ?",
        )
        .bind(tag)
        .bind(clamp_i64(from_offset))
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(JournalError::backend)?;
        Ok(rows
            .into_iter()
            .map(|(pid, seq, payload, manifest, writer, deleted)| PersistentRepr {
                persistence_id: pid,
                sequence_nr: seq as u64,
                payload,
                manifest,
                writer_uuid: writer,
                deleted: deleted != 0,
                tags: vec![tag.to_string()],
            })
            .collect())
    }
}
