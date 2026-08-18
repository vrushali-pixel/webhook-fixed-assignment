-- events.event_id previously only had a plain (non-unique) index, so nothing
-- stopped two concurrent redeliveries of the same event from both passing an
-- EventExists check and being inserted. Enforce uniqueness at the database
-- level -- this is what actually makes ingestion idempotent under
-- concurrency, not the check-then-insert code that ran before it.
DROP INDEX IF EXISTS idx_events_event_id;

ALTER TABLE events
    ADD CONSTRAINT events_event_id_key UNIQUE (event_id);
