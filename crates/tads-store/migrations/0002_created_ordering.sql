-- Order the schema/track list endpoints chronologically (newest-active first), matching common
-- TADS UIs. `payload_schema.created` already exists; `track` gains one. Backfill from the stored
-- `doc`, then index (created DESC, id DESC) to support the keyset pagination.
ALTER TABLE track ADD COLUMN created timestamptz;

UPDATE track
   SET created = (doc->>'created')::timestamptz
 WHERE created IS NULL AND doc ? 'created';
UPDATE payload_schema
   SET created = (doc->>'created')::timestamptz
 WHERE created IS NULL AND doc ? 'created';

CREATE INDEX track_created_idx ON track (created DESC, track_id DESC);
CREATE INDEX payload_schema_created_idx ON payload_schema (created DESC, schema_id DESC);
