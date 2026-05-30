-- FR-9 WORM hash-chain columns + FR-8 bitemporal columns.
-- Nullable so existing rows remain valid. Applied once at bootstrap.
ALTER TABLE event_journal ADD COLUMN prev_hash VARBINARY(32) NULL;
ALTER TABLE event_journal ADD COLUMN row_hash VARBINARY(32) NULL;
ALTER TABLE event_journal ADD COLUMN system_time BIGINT NULL;
ALTER TABLE event_journal ADD COLUMN valid_time BIGINT NULL;

CREATE INDEX idx_event_journal_system_time
    ON event_journal (persistence_id, system_time);
CREATE INDEX idx_event_journal_valid_time
    ON event_journal (persistence_id, valid_time);

-- WORM enforcement (deny_update_delete) for MySQL is applied out-of-band
-- by the operator:
--   REVOKE UPDATE, DELETE ON event_journal FROM 'app'@'%';
-- or BEFORE UPDATE / BEFORE DELETE triggers that SIGNAL SQLSTATE '45000'
-- with a 'WORM: append-only' message.
