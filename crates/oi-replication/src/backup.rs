//! Cron-style ClickHouse backup helpers. Actual execution is delegated to
//! `clickhouse-backup` running as a sidecar — this module only orchestrates
//! lifecycle (list, prune, verify, restore-test).
//! See `deploy/docker-compose.yml` for the sidecar and `docs/replication.md`.
