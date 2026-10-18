CREATE DATABASE IF NOT EXISTS lms;

CREATE TABLE IF NOT EXISTS lms.logs
(
    tenant_id LowCardinality(String),
    dataset LowCardinality(String),
    event_time DateTime64(9, 'UTC'),
    ingest_time DateTime64(9, 'UTC'),
    event_id String,
    source_id String,
    host LowCardinality(String),
    service LowCardinality(String),
    severity LowCardinality(String),
    trace_id String,
    message String CODEC(ZSTD(1)),
    attributes String CODEC(ZSTD(1)),
    protocol LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(ingest_time)
ORDER BY (tenant_id, dataset, service, event_time, event_id)
TTL ingest_time + INTERVAL 10 DAY DELETE;
