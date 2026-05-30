-- FR-9 WORM hash-chain columns + FR-8 bitemporal columns.
-- All columns are nullable so existing rows remain valid.
ALTER TABLE event_journal ADD COLUMN prev_hash BLOB;
ALTER TABLE event_journal ADD COLUMN row_hash BLOB;
ALTER TABLE event_journal ADD COLUMN system_time INTEGER;
ALTER TABLE event_journal ADD COLUMN valid_time INTEGER;

CREATE INDEX IF NOT EXISTS idx_event_journal_system_time
    ON event_journal (persistence_id, system_time);
CREATE INDEX IF NOT EXISTS idx_event_journal_valid_time
    ON event_journal (persistence_id, valid_time);
