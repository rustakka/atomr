-- FR-9 WORM hash-chain columns + FR-8 bitemporal columns.
-- Nullable so existing rows remain valid.
ALTER TABLE event_journal ADD COLUMN IF NOT EXISTS prev_hash BYTEA;
ALTER TABLE event_journal ADD COLUMN IF NOT EXISTS row_hash BYTEA;
ALTER TABLE event_journal ADD COLUMN IF NOT EXISTS system_time BIGINT;
ALTER TABLE event_journal ADD COLUMN IF NOT EXISTS valid_time BIGINT;

CREATE INDEX IF NOT EXISTS idx_event_journal_system_time
    ON event_journal (persistence_id, system_time);
CREATE INDEX IF NOT EXISTS idx_event_journal_valid_time
    ON event_journal (persistence_id, valid_time);

-- WORM enforcement (deny_update_delete) for Postgres is applied out-of-band
-- by the operator, since it requires elevated privileges:
--   REVOKE UPDATE, DELETE ON event_journal FROM application_role;
-- or a BEFORE UPDATE/DELETE trigger raising an exception, e.g.:
--   CREATE OR REPLACE FUNCTION event_journal_worm() RETURNS trigger AS $$
--   BEGIN RAISE EXCEPTION 'WORM: event_journal is append-only'; END;
--   $$ LANGUAGE plpgsql;
--   CREATE TRIGGER event_journal_worm_guard
--     BEFORE UPDATE OR DELETE ON event_journal
--     FOR EACH ROW EXECUTE FUNCTION event_journal_worm();
