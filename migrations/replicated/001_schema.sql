-- Replicated variant of the base schema. Use instead of 001_schema.sql
-- when deploying the two-node cluster (docker-compose.replicated.yml).
--
-- Only the engine clauses differ:
--   ReplacingMergeTree       → ReplicatedReplacingMergeTree
--   AggregatingMergeTree     → ReplicatedAggregatingMergeTree
-- The `{shard}` / `{replica}` macros are resolved by Keeper based on
-- deploy/clickhouse/server-{1,2}.xml.

CREATE DATABASE IF NOT EXISTS oi ON CLUSTER oi;

CREATE TABLE IF NOT EXISTS oi.instruments ON CLUSTER oi
(
    exchange             LowCardinality(String),
    symbol               String,
    base_asset           LowCardinality(String),
    quote_asset          LowCardinality(String),
    is_perpetual         UInt8,
    native_unit          LowCardinality(String),
    contract_multiplier  Nullable(Decimal64(8)),
    price_tick           Nullable(Decimal64(8)),
    qty_step             Nullable(Decimal64(8)),
    active               UInt8,
    updated_at           DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/oi/instruments',
    '{replica}',
    updated_at
)
ORDER BY (exchange, symbol);

CREATE TABLE IF NOT EXISTS oi.oi_minute ON CLUSTER oi
(
    exchange       LowCardinality(String),
    symbol         String,
    bucket_ts      DateTime64(3, 'UTC')   CODEC(Delta, LZ4),
    recv_ts        DateTime64(3, 'UTC')   CODEC(Delta, LZ4),

    native_value   Decimal128(18)         CODEC(ZSTD(1)),
    native_unit    LowCardinality(String),

    oi_coins       Nullable(Decimal128(18)) CODEC(ZSTD(1)),
    oi_usd         Nullable(Decimal128(6))  CODEC(ZSTD(1)) ,
    price_used     Nullable(Decimal64(8))   CODEC(ZSTD(1))   ,

    ingest_ts      DateTime64(3, 'UTC') DEFAULT now64()   CODEC(Delta, LZ4)
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/oi/oi_minute',
    '{replica}',
    ingest_ts
)
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (exchange, symbol, bucket_ts)
TTL toDateTime(bucket_ts) + INTERVAL 400 DAY;

CREATE TABLE IF NOT EXISTS oi.oi_hour ON CLUSTER oi
(
    exchange       LowCardinality(String),
    symbol         String,
    bucket_ts      DateTime64(3, 'UTC') CODEC(Delta, LZ4),
    native_avg     Float64              CODEC(Gorilla, LZ4),
    oi_coins_avg   Nullable(Float64)    CODEC(Gorilla, LZ4),
    oi_usd_avg     Nullable(Float64)    CODEC(Gorilla, LZ4),
    samples        UInt32
)
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/tables/{shard}/oi/oi_hour',
    '{replica}'
)
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (exchange, symbol, bucket_ts)
TTL toDateTime(bucket_ts) + INTERVAL 5 YEAR;

CREATE MATERIALIZED VIEW IF NOT EXISTS oi.mv_oi_minute_to_hour ON CLUSTER oi
TO oi.oi_hour AS
SELECT
    exchange,
    symbol,
    toStartOfHour(bucket_ts)                AS bucket_ts,
    avg(native_value)                       AS native_avg,
    avgIf(oi_coins, oi_coins IS NOT NULL)   AS oi_coins_avg,
    avgIf(oi_usd,   oi_usd   IS NOT NULL)   AS oi_usd_avg,
    count()                                 AS samples
FROM oi.oi_minute
GROUP BY exchange, symbol, bucket_ts;
