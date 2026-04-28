-- ClickHouse schema for OI collection (OHLC bars).
--
-- Each row is ONE one-minute OHLC bar per (exchange, symbol). The
-- collector aggregates intra-minute samples (REST + WS pushes) into
-- a single bar before flushing at the minute boundary.
--
-- For each value column there are four fields: open / high / low /
-- close. `samples` records how many observations folded into the
-- bar (1 for REST-only exchanges; many for WS-live exchanges).
-- `price_used_close` is single-valued by design — full price OHLC
-- belongs in a dedicated price feed.
--
-- Codecs: Gorilla + LZ4 for floating-shaped sequences (OI values),
-- Delta + LZ4 for monotonic timestamps.
-- TTL: 400 days for minute bars; rolled up to hourly bars (5y).

CREATE DATABASE IF NOT EXISTS oi;

-- Canonical instrument catalogue. Refreshed daily by the collector.
CREATE TABLE IF NOT EXISTS oi.instruments
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
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (exchange, symbol);

-- Minute OHLC bar. The row key makes (exchange, symbol) range scans
-- a tight index seek. ReplacingMergeTree on `ingest_ts` deduplicates
-- if the same minute is re-flushed (e.g. resync, WAL replay).
CREATE TABLE IF NOT EXISTS oi.oi_minute
(
    exchange       LowCardinality(String),
    symbol         String,
    bucket_ts      DateTime64(3, 'UTC')   CODEC(Delta, LZ4),
    first_recv_ts  DateTime64(3, 'UTC')   CODEC(Delta, LZ4),
    last_recv_ts   DateTime64(3, 'UTC')   CODEC(Delta, LZ4),
    samples        UInt32                 CODEC(T64, LZ4),

    native_unit    LowCardinality(String),
    native_open    Decimal128(18)         CODEC(Gorilla, LZ4),
    native_high    Decimal128(18)         CODEC(Gorilla, LZ4),
    native_low     Decimal128(18)         CODEC(Gorilla, LZ4),
    native_close   Decimal128(18)         CODEC(Gorilla, LZ4),

    oi_coins_open  Nullable(Decimal128(18)) CODEC(Gorilla, LZ4),
    oi_coins_high  Nullable(Decimal128(18)) CODEC(Gorilla, LZ4),
    oi_coins_low   Nullable(Decimal128(18)) CODEC(Gorilla, LZ4),
    oi_coins_close Nullable(Decimal128(18)) CODEC(Gorilla, LZ4),

    oi_usd_open    Nullable(Decimal128(6))  CODEC(Gorilla, LZ4),
    oi_usd_high    Nullable(Decimal128(6))  CODEC(Gorilla, LZ4),
    oi_usd_low     Nullable(Decimal128(6))  CODEC(Gorilla, LZ4),
    oi_usd_close   Nullable(Decimal128(6))  CODEC(Gorilla, LZ4),

    price_used_close Nullable(Decimal64(8)) CODEC(Gorilla, LZ4),

    ingest_ts      DateTime64(3, 'UTC') DEFAULT now64()   CODEC(Delta, LZ4)
)
ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (exchange, symbol, bucket_ts)
TTL toDateTime(bucket_ts) + INTERVAL 400 DAY;

-- Hourly rollup for historical dashboards. avg() of Decimal returns
-- Float64 in ClickHouse, so the rollup uses Float64 — exact accounting
-- still has the minute table.
CREATE TABLE IF NOT EXISTS oi.oi_hour
(
    exchange       LowCardinality(String),
    symbol         String,
    bucket_ts      DateTime64(3, 'UTC') CODEC(Delta, LZ4),
    native_open    Float64              CODEC(Gorilla, LZ4),
    native_high    Float64              CODEC(Gorilla, LZ4),
    native_low     Float64              CODEC(Gorilla, LZ4),
    native_close   Float64              CODEC(Gorilla, LZ4),
    oi_usd_open    Nullable(Float64)    CODEC(Gorilla, LZ4),
    oi_usd_high    Nullable(Float64)    CODEC(Gorilla, LZ4),
    oi_usd_low     Nullable(Float64)    CODEC(Gorilla, LZ4),
    oi_usd_close   Nullable(Float64)    CODEC(Gorilla, LZ4),
    samples        UInt64
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (exchange, symbol, bucket_ts)
TTL toDateTime(bucket_ts) + INTERVAL 5 YEAR;

-- Funding-rate samples, one per (exchange, symbol, minute). Funding
-- intra-minute variance is essentially zero (the rate moves with
-- the basis, which itself updates on a slow cadence), so we store a
-- single value per minute rather than OHLC. ReplacingMergeTree on
-- ingest_ts deduplicates re-flushes.
CREATE TABLE IF NOT EXISTS oi.funding_minute
(
    exchange         LowCardinality(String),
    symbol           String,
    bucket_ts        DateTime64(3, 'UTC')         CODEC(Delta, LZ4),
    recv_ts          DateTime64(3, 'UTC')         CODEC(Delta, LZ4),
    rate             Decimal64(12)                CODEC(Gorilla, LZ4),
    next_funding_ts  Nullable(DateTime64(3, 'UTC')) CODEC(Delta, LZ4),
    interval_hours   Nullable(UInt8),
    ingest_ts        DateTime64(3, 'UTC') DEFAULT now64()  CODEC(Delta, LZ4)
)
ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (exchange, symbol, bucket_ts)
TTL toDateTime(bucket_ts) + INTERVAL 400 DAY;

-- Settlement events — discrete records of actually-paid funding.
-- Distinct from oi.funding_minute (continuous predicted-rate
-- series): events are sparse, sit at exact venue settlement
-- boundaries (00/08/16 UTC for 8h venues, hourly for Hyperliquid),
-- and carry the realised rate. Retained 5 years because each
-- symbol only produces 3 events/day on most venues, so the
-- volume is small and historically valuable.
CREATE TABLE IF NOT EXISTS oi.funding_event
(
    exchange       LowCardinality(String),
    symbol         String,
    settlement_ts  DateTime64(3, 'UTC')         CODEC(Delta, LZ4),
    rate           Decimal64(12)                CODEC(Gorilla, LZ4),
    mark_price     Nullable(Decimal128(8))      CODEC(Gorilla, LZ4),
    ingest_ts      DateTime64(3, 'UTC') DEFAULT now64()  CODEC(Delta, LZ4)
)
ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMM(settlement_ts)
ORDER BY (exchange, symbol, settlement_ts)
TTL toDateTime(settlement_ts) + INTERVAL 1825 DAY;

-- Materialized view: minute → hour rollup on insert.
-- argMin/argMax pick the open/close from the actual bar
-- boundaries; max/min track the extremes; sum aggregates samples.
CREATE MATERIALIZED VIEW IF NOT EXISTS oi.mv_oi_minute_to_hour
TO oi.oi_hour AS
SELECT
    exchange,
    symbol,
    toStartOfHour(bucket_ts)                        AS bucket_ts,
    argMin(native_open, bucket_ts)                  AS native_open,
    max(native_high)                                AS native_high,
    min(native_low)                                 AS native_low,
    argMax(native_close, bucket_ts)                 AS native_close,
    argMinIf(oi_usd_open, bucket_ts, oi_usd_open IS NOT NULL)   AS oi_usd_open,
    maxIf(oi_usd_high, oi_usd_high IS NOT NULL)                 AS oi_usd_high,
    minIf(oi_usd_low,  oi_usd_low  IS NOT NULL)                 AS oi_usd_low,
    argMaxIf(oi_usd_close, bucket_ts, oi_usd_close IS NOT NULL) AS oi_usd_close,
    sum(samples)                                    AS samples
FROM oi.oi_minute
GROUP BY exchange, symbol, bucket_ts;
