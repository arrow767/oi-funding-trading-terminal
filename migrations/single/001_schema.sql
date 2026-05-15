-- ClickHouse schema for OI collection (OHLC bars).
--
-- Each row is ONE one-minute OHLC bar per (exchange, symbol). The
-- collector aggregates intra-minute samples (REST + WS pushes) into
-- a single bar before flushing at the minute boundary.
--
-- Numeric columns (OI values, prices, multipliers, funding rates) are
-- stored as String — full decimal precision is preserved in the
-- application layer via rust_decimal, and the binary wire protocol
-- for Nullable(Decimal*) is incompatible with String-typed Rust rows
-- as currently written. Migrating to native Decimal* later is a
-- non-breaking schema change (toDecimal128OrNull on read).

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
    contract_multiplier  Nullable(String),
    price_tick           Nullable(String),
    qty_step             Nullable(String),
    active               UInt8,
    updated_at           DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (exchange, symbol);

-- Minute OHLC bar. The row key makes (exchange, symbol) range scans
-- a tight index seek. ReplacingMergeTree on ingest_ts deduplicates
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
    native_open    String                 CODEC(ZSTD(1)),
    native_high    String                 CODEC(ZSTD(1)),
    native_low     String                 CODEC(ZSTD(1)),
    native_close   String                 CODEC(ZSTD(1)),

    oi_coins_open  Nullable(String)       CODEC(ZSTD(1)),
    oi_coins_high  Nullable(String)       CODEC(ZSTD(1)),
    oi_coins_low   Nullable(String)       CODEC(ZSTD(1)),
    oi_coins_close Nullable(String)       CODEC(ZSTD(1)),

    oi_usd_open    Nullable(String)       CODEC(ZSTD(1)),
    oi_usd_high    Nullable(String)       CODEC(ZSTD(1)),
    oi_usd_low     Nullable(String)       CODEC(ZSTD(1)),
    oi_usd_close   Nullable(String)       CODEC(ZSTD(1)),

    price_used_close Nullable(String)     CODEC(ZSTD(1)),

    ingest_ts      DateTime64(3, 'UTC') DEFAULT now64()   CODEC(Delta, LZ4)
)
ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (exchange, symbol, bucket_ts)
TTL toDateTime(bucket_ts) + INTERVAL 400 DAY;

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
    rate             String                       CODEC(ZSTD(1)),
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
    rate           String                       CODEC(ZSTD(1)),
    mark_price     Nullable(String)             CODEC(ZSTD(1)),
    ingest_ts      DateTime64(3, 'UTC') DEFAULT now64()  CODEC(Delta, LZ4)
)
ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMM(settlement_ts)
ORDER BY (exchange, symbol, settlement_ts)
TTL toDateTime(settlement_ts) + INTERVAL 1825 DAY;
