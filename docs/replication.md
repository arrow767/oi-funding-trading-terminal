# Replication, backup, and failover

## Goals

1. **No data loss** on a single-host failure.
2. **No duplicate writes** — exactly one collector writes per minute, even
   during promotion.
3. **Time-to-recovery ≤ 30 s** of OI samples after a primary crash.
4. **Cold recovery ≤ 10 min** from the latest remote backup.

## Topology

```
┌──────────────────┐                   ┌──────────────────┐
│ host-A           │                   │ host-B           │
│                  │                   │                  │
│ [collector      ]│──── Redis lease ──│[collector       ]│
│ [clickhouse-1   ]│◀── ReplicatedMT ─▶│[clickhouse-2    ]│
│ [redis (master) ]│◀──  Sentinel    ─▶│[redis (replica) ]│
│ [api            ]│                   │[api             ]│
└──────────────────┘                   └──────────────────┘
                     \                /
                      └─ keeper (3×) ┘
```

* **ClickHouse replication:** both shards hold all data via
  `ReplicatedMergeTree`. Keeper (3 nodes, one of them can be co-located
  with Redis for cost) is the consensus layer. Writes to either node are
  propagated automatically. Reads go to the local node by default.
* **Collector HA:** one collector is the *writer*; the other is a *hot
  standby*. Ownership is decided by a Redis SETNX lease on
  `oi:lease:writer` with a 15-second TTL. The writer refreshes the lease
  every 5s; if it misses three refreshes, the standby takes over.
* **Redis HA:** Redis Sentinel with two sentinels + one observer. On
  primary failure, the standby is promoted within seconds.

## Backup

* **Hot backups:** `clickhouse-backup` sidecar takes daily incremental
  backups to S3 (zstd-compressed). Retention: 30 days remote, 3 local.
* **Disaster drill:** the backup job runs a weekly restore-verify against
  a throwaway container; alert on any checksum mismatch.
* **Schema-only snapshot:** `migrations/001_schema.sql` is committed to the
  repo — the backup keeps data; the repo keeps the schema of record.

## Operational runbook

### Primary host failure

1. Redis Sentinel promotes the replica (sub-second).
2. The standby collector's lease acquisition succeeds on the next tick
   (≤ 15 s).
3. ClickHouse replica on host-B accepts the writes automatically.
4. The API on host-B answers reads with `ReplicationInSync = 1`.

### Restoring a lost node

```sh
# On the replacement node:
docker compose pull
docker compose up -d clickhouse-N
# The new replica catches up from Keeper; no manual restore needed.
```

### Cold recovery from backup

```sh
docker compose exec clickhouse-backup clickhouse-backup restore_remote <name>
```

## Config files referenced

* `deploy/clickhouse/keeper.xml` — single-node Keeper template.
  Scale to a 3-node quorum by adding the commented `<server>` blocks
  and running additional Keeper containers.
* `deploy/clickhouse/server-{1,2}.xml` — per-node `macros`
  (`shard`/`replica`/`cluster`) and cluster topology for the
  `ReplicatedMergeTree` family.
* `migrations/001_schema_replicated.sql` — replicated-engine variant of
  the base schema; use this instead of `001_schema.sql` when running
  against the two-node compose stack.
* `deploy/backup/config.yml` — clickhouse-backup target + retention.

## Testing the replicated stack locally

```sh
docker compose -f deploy/docker-compose.replicated.yml up -d keeper clickhouse-1 clickhouse-2
# Apply the replicated schema to either node — it propagates via
# `ON CLUSTER oi`.
docker compose -f deploy/docker-compose.replicated.yml exec clickhouse-1 \
    clickhouse-client --multiquery < migrations/001_schema_replicated.sql
# Then bring up the collectors and API:
docker compose -f deploy/docker-compose.replicated.yml up -d
```

## Replicated WAL — how failover stays lossless

Both collectors run identical fetch loops; both write to a local
**`FileWal`** on every minute tick, regardless of leadership. Only the
lease holder actually pushes those WAL files into ClickHouse.

```
            ┌────────────── exchange APIs ──────────────┐
            │                                           │
            ▼                                           ▼
     ┌─ collector A ─┐                          ┌─ collector B ─┐
     │ fetch_oi      │ ◀── same payload ──▶     │ fetch_oi      │
     │ enrich        │                          │ enrich        │
     │ FileWal.append├──┐                    ┌──┤ FileWal.append│
     │ drainer       │  │                    │  │ drainer       │
     └───────────────┘  │                    │  └───────────────┘
                        ▼                    ▼
              [ wal_dir/A/*.mpk ]    [ wal_dir/B/*.mpk ]
                        │                    │
                        │  (only the leader) │
                        └─────────► ClickHouse ◄─────
```

* **Steady state.** Leader's `LeaderGatedRepo` passes the inner CH
  write through; `WalBacked` ack's the file. Follower's gated repo
  returns a synthetic "not leader" error; `WalBacked` keeps the file.
  The drainer on the follower is gated off — it refreshes metrics
  but doesn't touch the queue. So the follower's WAL grows in
  lockstep with the leader's writes.
* **Failover (≤ lease TTL ≈ 15s).** Lease flips to the standby. Its
  drainer's next tick (≤ `wal.drain_interval_secs`) sees
  `is_leader() == true` and replays the local backlog into CH —
  including the minute that was in flight when the old leader died.
* **The cost** is one extra fetch per minute per exchange (both
  nodes poll). All adapter rate limits have ~10× headroom over our
  steady-state load, so this is comfortable.
* **Bounded follower disk usage.** When `wal.follower_max_age_secs`
  is set, the drainer's follower branch deletes pending files older
  than that age each cycle (counter:
  `oi_wal_reaped_total`). Without it, files on a long-running
  standby grow forever; with it, you trade history-loss-on-late-promotion
  against bounded disk. Pick a value larger than your worst-case
  CH outage + planned-failover window — e.g. 24h is conservative.

What this DOES NOT cover:
* Both nodes crash simultaneously between fetch and WAL append → the
  in-flight minute is lost. Reduce the window with faster disk; we
  don't have a lower-level guarantee than `fsync`.
* Redis loses the lease key (Redis crash) — both nodes briefly think
  they're follower; writes pause until Redis recovers and one
  acquires. No data loss because both still WAL.

## Collector failover drill

```sh
# Confirm the lease is held by one collector:
docker compose exec redis redis-cli GET oi:lease:writer
# Kill the leader:
docker compose stop collector-primary
# Within ~15 s (lease TTL), the standby promotes:
docker compose logs collector-standby | grep "lease acquired"
# Restart — old leader comes back as standby:
docker compose start collector-primary
```
