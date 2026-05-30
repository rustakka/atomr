-- FR-9 WORM hash-chain columns + FR-8 bitemporal columns.
-- Nullable so existing rows remain valid. Applied once at bootstrap.
IF COL_LENGTH('event_journal', 'prev_hash') IS NULL
    ALTER TABLE event_journal ADD prev_hash VARBINARY(32) NULL;
IF COL_LENGTH('event_journal', 'row_hash') IS NULL
    ALTER TABLE event_journal ADD row_hash VARBINARY(32) NULL;
IF COL_LENGTH('event_journal', 'system_time') IS NULL
    ALTER TABLE event_journal ADD system_time BIGINT NULL;
IF COL_LENGTH('event_journal', 'valid_time') IS NULL
    ALTER TABLE event_journal ADD valid_time BIGINT NULL;

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = N'idx_event_journal_system_time')
    CREATE INDEX idx_event_journal_system_time ON event_journal (persistence_id, system_time);
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = N'idx_event_journal_valid_time')
    CREATE INDEX idx_event_journal_valid_time ON event_journal (persistence_id, valid_time);

-- WORM enforcement (deny_update_delete) for SQL Server is applied out-of-band
-- by the operator:
--   DENY UPDATE, DELETE ON event_journal TO app_role;
-- or an INSTEAD OF UPDATE, DELETE trigger that raises:
--   THROW 50000, 'WORM: event_journal is append-only', 1;
