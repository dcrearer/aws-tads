-- TADS Postgres schema. The `doc` jsonb column is the authoritative serialized domain object
-- (same role the DynamoDB `doc` attribute played); scalar columns exist for keys, filters and
-- the timerange-overlap index. `btree_gist` lets the GiST index combine the uuid equality
-- component (track_id) with the int8range overlap component (tr) in one index.
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE payload_schema (
    schema_id   uuid PRIMARY KEY,
    external_id text UNIQUE,
    label       text,
    tags        jsonb NOT NULL DEFAULT '{}'::jsonb,
    doc         jsonb NOT NULL,
    created     timestamptz,
    updated     timestamptz
);

CREATE TABLE track (
    track_id        uuid PRIMARY KEY,
    schema_id       uuid NOT NULL,
    entity_id       text NOT NULL,
    label           text,
    editable_events boolean NOT NULL DEFAULT true,
    read_only       boolean NOT NULL DEFAULT false,
    allow_overlaps  boolean NOT NULL DEFAULT true,
    tags            jsonb NOT NULL DEFAULT '{}'::jsonb,
    doc             jsonb NOT NULL
);
CREATE INDEX track_schema_id_idx ON track (schema_id);
CREATE INDEX track_entity_id_idx ON track (entity_id);
CREATE INDEX track_tags_idx ON track USING gin (tags);

CREATE TABLE track_event (
    track_id  uuid NOT NULL,
    event_id  uuid NOT NULL,
    start_ns  bigint NOT NULL,
    end_ns    bigint NOT NULL,
    tr        int8range NOT NULL,
    timerange text NOT NULL,
    doc       jsonb NOT NULL,
    -- event_id is the identity (per spec the timerange is NOT the identity), so re-posting an
    -- event at a different timerange UPDATEs in place via ON CONFLICT (track_id, event_id) --
    -- this fixes the "moved-timerange orphans the old key" refinement that DynamoDB had.
    PRIMARY KEY (track_id, event_id)
);
-- Timerange overlap: WHERE track_id = $1 AND tr && int8range($lo, $hi, '[)')
CREATE INDEX track_event_overlap_idx ON track_event USING gist (track_id, tr);
-- Keyset-pagination order within a track: (start_ns, event_id) is unique per track.
CREATE INDEX track_event_order_idx ON track_event (track_id, start_ns, event_id);

CREATE TABLE webhook (
    webhook_id    uuid PRIMARY KEY,
    api_key_value text,
    status        text NOT NULL,
    doc           jsonb NOT NULL
);

CREATE TABLE track_delete_request (
    request_id uuid PRIMARY KEY,
    status     text NOT NULL,
    expiry     bigint,          -- epoch seconds; swept by the delete-worker (replaces DynamoDB TTL)
    created    timestamptz,
    doc        jsonb NOT NULL
);
CREATE INDEX track_delete_request_expiry_idx ON track_delete_request (expiry);

-- Transactional outbox: each resource mutation writes a row here in the SAME transaction as the
-- data change. The outbox-worker drains undispatched rows into the webhook dispatcher.
CREATE TABLE outbox (
    id            bigserial PRIMARY KEY,
    op            text NOT NULL,      -- insert | modify | remove
    resource      text NOT NULL,      -- schema | track | event
    aggregate_id  text NOT NULL,      -- track_id for events, else the resource id
    doc           jsonb,              -- new document (null for removes)
    key_fields    jsonb NOT NULL DEFAULT '{}'::jsonb,  -- ids needed to build a delete Change
    created_at    timestamptz NOT NULL DEFAULT now(),
    dispatched_at timestamptz
);
CREATE INDEX outbox_undispatched_idx ON outbox (id) WHERE dispatched_at IS NULL;
