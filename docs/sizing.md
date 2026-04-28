# Sizing & cost

## Resource budget

* ClickHouse: 4–8 GB RAM (insertion buffers + page cache).
* Redis: 1–2 GB (latest cache + pub/sub backlog).
* `oi-collector`: 300–500 MB (per-exchange WS state, aggregator
  DashMaps, up to 4 concurrent WS streams).
* `oi-api`: 100–200 MB (axum + tonic, idle).
* OS / Docker overhead: 1–2 GB.

**Total: ~8–12 GB working set.** Production should run ≥ 16 GB to
absorb query bursts.

## Storage growth (full universe across all 9 exchanges)

* Active perpetuals universe: ~1800 instruments.
* `oi.oi_minute`: 1800 × 60 × 24 ≈ **2.6M rows/day**. After
  Gorilla + LZ4 compression (~10–15× on real OI data) ≈ **400 MB/day**.
  At 400-day TTL: **~160 GB**.
* `oi.funding_minute`: same row volume, smaller per-row payload.
  **~50 GB at 400-day TTL.**
* `oi.funding_event`: sparse — 3–24 events per symbol per day. At
  5-year TTL: **~5 GB total.**
* Boot disk + Docker images + logs: **~20 GB.**

**Total disk budget: ~250 GB** for full retention. Use a separate
Block Storage volume for `/var/lib/clickhouse` so IOPS isolation +
snapshotting are independent of the boot disk.

## CPU

Collector is mostly I/O-bound:
* REST polling: ~100 req/min total. Trivial.
* WS handlers: 4 long-lived stream consumers. Each is a single
  tokio task; no CPU pressure under steady state.
* Aggregator: per-sample fold is sub-µs.

ClickHouse query CPU scales with terminal usage:
* `Latest` calls served from the Redis cache → no CH load.
* `Range` calls scan minute-bar partitions. A 1-week range for one
  symbol is ~10K rows, returns in <50 ms on a single core.
* 50 concurrent `Range` calls would still fit comfortably in
  4 vCPUs.

## Network

* **Inbound** (exchanges → us): ~70 GB/day at active markets,
  dominated by Bybit/OKX/Bitget/Hyperliquid WS streams.
* **Outbound** (us → terminals): ~500 MB/day per Subscribe client
  on a full watchlist.

DigitalOcean droplet transfer allowance covers this comfortably.

## Droplet recommendations

| Scenario | Droplet | Disk | Monthly | Notes |
|---|---|---|---|---|
| MVP / one user | `s-2vcpu-4gb` | boot only | $24 | Tight; drop OI TTL to 30d. |
| **Recommended** | **`s-4vcpu-16gb-amd`** | + 250 GB Block | **~$109** | Sweet spot. AMD Premium, RAM headroom. |
| Read-heavy | `c-8` | + 250 GB Block | ~$185 | More CPU for many `Range` queries. |
| HA two-node | 2× `s-4vcpu-16gb-amd` | + 2× 250 GB | ~$240–300 | Replicated CH + lease failover. |
| Managed CH | DO Managed ClickHouse | included | $300+ | If you don't want to babysit CH. |

For the typical 1–10 trader desktop client deployment, the
`s-4vcpu-16gb-amd` plus 250 GB Block Storage is the right starting
point. See [docs/deployment.md](deployment.md) for the step-by-step.

## Region

Pick the region closest to where most of the exchange APIs live —
that's `fra1` (Frankfurt) or `ams3` (Amsterdam) for the EU-anchored
crowd, or `nyc1`/`nyc3` for US clusters. Latency on REST polling
isn't critical (we accept 200 ms RTT in our minute bucket) but for
the WS streams every 50 ms saved is one less reconnect-induced gap.
