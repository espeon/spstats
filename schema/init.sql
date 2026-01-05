CREATE DATABASE IF NOT EXISTS sp_stats;

USE sp_stats;

CREATE TABLE IF NOT EXISTS stream_place_events (
    at_uri String,
    ingested_at DateTime64(3),
    created_at Nullable(DateTime64(3)),
    did String,
    collection String,
    rkey String,
    event_type String,
    action String,
    is_backfill UInt8,
    record_data JSON
) ENGINE = MergeTree()
ORDER BY (collection, ingested_at, did)
PARTITION BY toYYYYMM(ingested_at);

-- useful indexes for common queries
CREATE INDEX IF NOT EXISTS idx_did ON stream_place_events (did) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX IF NOT EXISTS idx_at_uri ON stream_place_events (at_uri) TYPE bloom_filter GRANULARITY 1;
