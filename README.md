# trading-terminal-oi

Production-oriented Open Interest (OI) collector and serving API for 9
perpetual-futures venues (`binance, bybit, okx, bingx, kucoin, mexc,
bitget, hyperliquid, aster`). Written in Rust, designed as a Cargo
workspace so each seam is swappable.

## What it does

- Polls each exchange **once per minute**, aligned to `HH:MM:02` UTC,
  for every perpetual instrument; venues with WS push (Bybit, OKX,
  Bitget, Hyperliquid) also feed sub-minute samples.
- Aggregates intra-minute samples into **OHLC bars** (one bar per
  `(exchange, symbol, minute)`), TradingView-style. REST-only
  exchanges produce degenerate `O=H=L=C` bars; WS-pushed exchanges
  carry full open/high/low/close per minute. `samples` counts how
  many observations folded into each bar.
- Stores OHLC for the native value AND each derived denomination
  (`oi_coins_*`, `oi_usd_*`) plus the price used at close — so you
  can reconstruct any view later without re-fetching.
- Also samples **funding rate** every minute per `(exchange, symbol)`
  from the same batch endpoints (separate `oi.funding_minute`
  table). Carried alongside OI by every production adapter; OKX
  fans out per-symbol because it has no batch funding endpoint.
  Available via gRPC `LatestFunding` / `FundingRange` and REST
  `/v1/funding/{latest,range}/:exchange/:symbol`.
- And **settlement events** — discrete `(rate, settlement_ts,
  mark_price?)` records of actually-paid funding at venue
  boundaries. Distinct from the predicted-rate series above; uses
  per-exchange history endpoints (Binance `/fapi/v1/fundingRate`,
  Bybit `/v5/market/funding/history`, OKX
  `/api/v5/public/funding-rate-history`, etc.) polled every 30 min
  via a dedicated sweep task with cursor `latest_funding_event` so
  re-polls fetch only new events. Stored in `oi.funding_event` (5y
  retention, sparse). Surfaced as gRPC `LatestFundingEvent` /
  `FundingEventRange` and REST
  `/v1/funding/events/{latest,range}/:exchange/:symbol`.
- Funding interval (`interval_hours`) is **discovered** for MEXC and
  KuCoin from per-symbol endpoints at every 6h discovery refresh
  (cached in the adapter); other adapters publish it inline on
  their batch tickers.
- Writes to **ClickHouse** (`oi.oi_minute` — minute bars, LZ4+Gorilla,
  partitioned by month, TTL 400 days; rolls up to hourly bars kept 5
  years).
- Keeps the latest snapshot per instrument in **Redis** (MessagePack,
  15 min TTL) so the API's `Latest` call is a single round-trip.
- Serves two APIs from one binary:
  - **gRPC** on `:50051` — typed, streamed, for the trading terminal.
  - **REST/JSON** on `:8080` — for dashboards and curl.
- Supervised WebSockets with automatic reconnect, ping/pong, exponential
  backoff with jitter, idle-timeout detection.
- Rate-limited per exchange (`governor` token buckets), 429/418 handling,
  retryable vs. permanent error taxonomy.
- Two-node replication with ClickHouse `ReplicatedMergeTree` +
  Keeper consensus. Keeper XML, per-node cluster XML, and a
  replicated-engine schema variant live under `deploy/clickhouse/` +
  `migrations/001_schema_replicated.sql`. **Redis-lease based
  primary/standby collector election is live** —
  `oi-replication::LeaseManager` uses a Lua `REFRESH_IF_OWNED` CAS to
  avoid split-brain on reordered refreshes; follower collectors keep
  WS state warm but skip writes. Daily clickhouse-backup to S3. See
  [docs/replication.md](docs/replication.md).
- **Cross-exchange price fallback.** When an exchange's own price
  feed misses a tick, USD enrichment borrows a fresh quote for the
  same canonical base asset from another venue (priority:
  Binance → Bybit → OKX → BingX → KuCoin → MEXC → Bitget →
  Hyperliquid → Aster — the `Exchange::all()` declaration order).
  Spelling differences are normalized — KuCoin's `XBT` resolves to
  `BTC`. `oi_usd` stays populated through partial price-feed
  outages instead of going `NULL`. Counter
  `oi_price_fallback_total{exchange,donor}` makes the divergence
  visible.
- **Prometheus metrics.** Collector (`:9090/metrics`):
  `oi_snapshots_written_total{exchange}`,
  `oi_live_publishes_total{exchange}`,
  `oi_rest_errors_total{exchange,kind}`,
  `oi_ws_reconnects_total{handler}`,
  `oi_instruments_tracked{exchange}`,
  `oi_price_fallback_total{exchange,donor,quote_match}`,
  `oi_leader`.
  API (`:9091/metrics`): `oi_api_requests_total{method,status}`,
  `oi_api_subscribe_frames_total`, `oi_api_request_seconds{method}`
  (histogram).
- **Gap-fill CLI:** `oi-collector resync --exchange X --bucket T` or
  `--minutes N`. Takes the current exchange snapshot and writes it to
  the specified bucket(s). Useful after short downtime; not a
  historical backfill — most exchanges don't publish past OI.
- **TLS + bearer-token auth on the API.** Optional via `[tls]` /
  `[auth]` in `deploy/api.toml`. When enabled, both gRPC and REST
  serve over rustls with the same cert; data endpoints require
  `Authorization: Bearer <token>` matching one of the configured
  tokens. Health (`/health*`) and `/metrics` are always exempt so
  probes and Prometheus scrapes don't carry credentials. Token
  comparison is constant-time. Rotate by appending a new token,
  deploying, then removing the old.
- **WAL CRC32.** Every WAL frame is `OIW1` magic + 4-byte big-endian
  CRC32 + msgpack payload. Reader rejects with quarantine on
  mismatch (counter `oi_wal_crc_mismatch_total`). Catches silent
  corruption before it reaches ClickHouse. Legacy unframed files
  from before the upgrade are still accepted (with a `warn` log) so
  in-place upgrades drain the existing queue without manual
  migration.
- **Durable, replicated WAL between collector and ClickHouse.** Enable
  `[wal] enabled = true` in `deploy/collector.toml` to persist every
  batch to disk (atomic rename + fsync) before the CH upsert. A
  background drainer replays files that failed to reach CH, so short
  CH outages don't lose any already-fetched minutes. Under HA both
  nodes' WAL stay in lockstep — when the leader dies, the standby's
  next drain tick replays its own up-to-date queue, so failover is
  lossless within the lease TTL. See
  [docs/replication.md §Replicated WAL](docs/replication.md). Metrics:
  `oi_wal_writes_total`, `oi_wal_acks_total`, `oi_wal_drained_total`,
  `oi_wal_pending_files` (gauge), `oi_wal_oldest_pending_age_seconds`
  (gauge — alert at 5m / 30m), `oi_wal_quarantined_total`,
  `oi_wal_reaped_total`. Three Prometheus alerts ship in
  `deploy/observability/prometheus-alerts.yml` to catch backlog growth,
  RTO breach, and a stuck drainer. On a long-running standby, set
  `wal.follower_max_age_secs` to bound disk usage — files older than
  the threshold are deleted on each drainer tick.
- **Cross-exchange price fallback** (collector). When a contracts-
  native exchange (MEXC, KuCoin, OKX) doesn't co-publish a fresh
  USD price for an instrument, the in-memory provider walks peer
  exchanges with the same canonical `(base, quote)` and borrows
  one. Stables (`USDT`/`USDC`/`FDUSD`/`BUSD`/`USD`/`TUSD`/`DAI`)
  collapse to a single `USD-PEG` family for matching; KuCoin's
  `XBT` is canonicalised to `BTC`. Two-pass: strict-quote first,
  then any-quote-same-base. Donor + quote_match are recorded on
  the snapshot's provenance and emitted as
  `oi_price_fallback_total{exchange,donor,quote_match}`.

## Going to production

* [docs/deployment.md](docs/deployment.md) — DigitalOcean droplet
  setup from scratch through to live data flow (~90 minutes
  end-to-end).
* [docs/sizing.md](docs/sizing.md) — RAM/CPU/disk/network budget,
  cost breakdown, droplet recommendations.
* [docs/terminal-integration.md](docs/terminal-integration.md) —
  Rust + TypeScript client examples, TradingView wiring, decimal
  handling, failure modes.

## Layout

```
crates/
  oi-core           # domain types, traits, unit conversion, errors
  oi-exchanges      # one adapter per venue + shared http/ws infra
  oi-storage        # ClickHouse + Redis + composite repo
  oi-collector      # daemon: scheduler, price provider, runner
  oi-api            # gRPC + REST server
  oi-replication    # failover lease, backup orchestration
proto/oi.proto      # wire schema (served by oi-api)
migrations/001_schema.sql
deploy/             # docker-compose (single + replicated), Dockerfile, configs
docs/               # adding-an-exchange, exchange-notes, replication
```

## Running locally

```sh
docker compose -f deploy/docker-compose.yml up --build
curl http://localhost:8080/health/ready
# After ~1 minute:
curl http://localhost:8080/v1/oi/latest/binance/BTCUSDT

# Prometheus metrics:
curl http://localhost:9090/metrics  # collector
curl http://localhost:9091/metrics  # api
```

Bundled gRPC client demo — calls `Latest` once, then tails live ticks:

```sh
cargo run --example subscribe -p oi-api -- \
    --addr http://127.0.0.1:50051 --exchange binance --symbol BTCUSDT
```

For terminals: the proto file is at [proto/oi.proto](proto/oi.proto)
(`OiService`). Rust consumers can depend on this crate and import
`oi_api::pb` directly — no separate protoc run needed.

## Observability

[deploy/observability/](deploy/observability/) ships a Prometheus
scrape config, alert rules (ingest stalls, WS storms, HA
flip-flops, API error-rate / p95), and a Grafana dashboard JSON.

Gap-fill after a short outage:
```sh
oi-collector resync --exchange binance --minutes 5
oi-collector resync --exchange all --bucket 2026-04-24T10:15:00Z
```

## Tests

```sh
cargo test --workspace
```

As of this commit: **150 tests pass** — 21 core (incl. 3 OHLC fold,
2 funding bar serde, 2 funding event serde), 60 exchanges unit + 21
wiremock integration across **all 9 adapters**, 19 storage, 20
collector, 4 replication, 4 auth unit + 1 auth-middleware end-to-end
+ 1 TLS round-trip. See [docs/testing.md](docs/testing.md) for the
opt-in testcontainers suite.

Two **opt-in** testcontainers-based tests boot real infra (Docker
required): a ClickHouse round-trip (`cargo test -p oi-storage --
--ignored clickhouse_roundtrip`) and a full **black-box e2e** pipeline
(`cargo test -p oi-api -- --ignored e2e_binance_through_rest`) that
runs fake Binance → collector logic → real ClickHouse + Redis → REST
API, and asserts the served bar matches the mocked input. The WS supervisor test
avoids network; every implemented adapter carries its own recorded-fixture
integration test in `crates/oi-exchanges/tests/*`.

## Status

| Adapter      | Status        | Notes                                                         |
|--------------|---------------|---------------------------------------------------------------|
| Binance USDM | **complete**  | Reference; per-symbol fan-out with bounded concurrency        |
| Bybit        | **complete**  | Batch `/v5/market/tickers` REST + **live v5 WS** → pub/sub    |
| Hyperliquid  | **complete**  | One POST covers discovery+OI+prices + **live activeAssetCtx** |
| OKX          | **complete**  | Batch REST open-interest + **live WS** open-interest channel  |
| Bitget       | **complete**  | Batch `/api/v2/mix/market/tickers` REST + **live ticker WS**  |
| MEXC         | **complete**  | Batch `/api/v1/contract/ticker`; contracts+multiplier         |
| Aster        | **complete**  | Binance-parity; delegates to Binance adapter, retags          |
| BingX        | **complete**  | Per-symbol OI fan-out + batch `premiumIndex` for prices       |
| KuCoin       | **complete**  | Batch `/api/v1/contracts/active`; contracts + multiplier      |

The collector enables every adapter by default
(`is_production_ready` in `oi-exchanges/src/lib.rs` is exhaustive over
the `Exchange` enum — adding a new variant is a compile error until a
real adapter exists). Pin a subset via `exchanges.enabled = [...]` in
`deploy/collector.toml`.

## Wire-format choice

gRPC + Protobuf for terminals (binary, streamed, no JSON
re-parsing). REST/JSON kept as a second endpoint for dashboards and
debugging — the two share one repository layer, one set of type
conversions, no drift.

## Decisions worth knowing

- **1m REST polling is the canonical write path to ClickHouse**; WS is
  sub-minute enrichment. Bybit's v5 `tickers.*` WS is live — deltas are
  merged per-symbol and published on the Redis `oi:stream` channel, so
  gRPC `Subscribe` clients see intra-minute updates without the
  durable store being polluted by malformed WS frames. The REST loop
  still runs and overwrites each minute's bucket at `:02`.
- **Decimal, not float.** `rust_decimal::Decimal` end-to-end. Some coins
  have 1e10+ OI values where `f64` silently loses resolution.
- **Closed `Exchange` enum.** Adding a venue forces a match-arm update
  everywhere relevant — the compiler is the checklist.
- **Idempotent writes.** `(exchange, symbol, bucket_ts)` is the row key;
  re-ingesting a minute overwrites. Gap-fill jobs can run safely.
