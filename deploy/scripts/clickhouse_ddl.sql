-- ============================================================
-- 以图搜商品系统 · ClickHouse DDL
-- 对齐: 系统设计 v1.2 §3.6
-- ============================================================

CREATE TABLE IF NOT EXISTS search_quality_log (
    request_id      String,
    timestamp       DateTime64(3),
    merchant_scope_size  UInt32,
    top_k           UInt16,
    data_scope      LowCardinality(String),
    time_range      LowCardinality(String),
    result_count    UInt16,
    top1_score      Float32,
    top10_avg_score Float32,
    strategy        LowCardinality(String),
    confidence      LowCardinality(String),
    total_ms        UInt32,
    feature_ms      UInt16,
    ann_ms          UInt16,
    filter_ms       UInt16,
    refine_ms       UInt16,
    degraded        UInt8,
    expanded        UInt8,
    filter_skipped  UInt8,
    degrade_state   LowCardinality(String),
    zone_hit        LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, request_id)
TTL timestamp + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS behavior_log (
    event_type      LowCardinality(String),
    request_id      String,
    image_id        String,
    position        UInt16,
    timestamp       DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (request_id, timestamp)
TTL timestamp + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS search_quality_mv
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (ts, data_scope, confidence)
AS SELECT
    toStartOfHour(s.timestamp) AS ts,
    s.data_scope,
    s.confidence,
    count() AS search_count,
    countIf(b.event_type = 'click') AS click_count,
    countIf(b.event_type = 'inquiry') AS inquiry_count,
    avg(s.total_ms) AS avg_latency_ms
FROM search_quality_log s
LEFT JOIN behavior_log b ON s.request_id = b.request_id
GROUP BY ts, s.data_scope, s.confidence;
