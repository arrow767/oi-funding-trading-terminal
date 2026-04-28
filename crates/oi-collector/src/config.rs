//! Daemon configuration. Loaded from TOML + env overrides (12-factor).

use oi_core::exchange::Exchange;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub clickhouse: ClickHouseCfg,
    pub redis: RedisCfg,
    #[serde(default)]
    pub exchanges: ExchangesCfg,
    #[serde(default)]
    pub failover: FailoverCfg,
    #[serde(default)]
    pub wal: WalCfg,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
}

/// Write-Ahead Log config. When enabled, every snapshot batch is
/// written to disk BEFORE the ClickHouse upsert; a background drainer
/// replays files if CH is down. Protects against CH outages and
/// collector crashes.
#[derive(Debug, Clone, Deserialize)]
pub struct WalCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_wal_dir")]
    pub dir: String,
    /// Soft threshold — logs a warning when pending files exceed
    /// this. No hard limit: the whole point of WAL is to absorb
    /// bursts; the operator's disk quota is the real cap.
    #[serde(default = "default_wal_soft_max")]
    pub soft_max_files: usize,
    /// Drainer cadence in seconds. Default 10s is aggressive enough
    /// to catch up fast after a CH hiccup but cheap when idle.
    #[serde(default = "default_wal_drain_secs")]
    pub drain_interval_secs: u64,
    /// Follower-side reaper. When this collector is a follower (HA
    /// standby), pending WAL files older than this are deleted to
    /// bound disk usage. The trade-off: any minute older than this
    /// is gone if the node is then promoted. `None` keeps
    /// everything — appropriate when you'd rather replay a stale
    /// queue than lose any history.
    #[serde(default)]
    pub follower_max_age_secs: Option<u64>,
}

impl Default for WalCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            dir: default_wal_dir(),
            soft_max_files: default_wal_soft_max(),
            drain_interval_secs: default_wal_drain_secs(),
            follower_max_age_secs: None,
        }
    }
}

fn default_wal_dir() -> String {
    "/var/lib/oi/wal".into()
}
fn default_wal_soft_max() -> usize {
    10_000
}
fn default_wal_drain_secs() -> u64 {
    10
}

/// HA lease config. When `enabled = false`, the collector always
/// writes (single-node deploys). When `true`, writes are gated on
/// holding the `oi:lease:writer` Redis lease — only one collector in
/// the cluster is active at a time.
#[derive(Debug, Clone, Deserialize)]
pub struct FailoverCfg {
    #[serde(default)]
    pub enabled: bool,
    /// Optional override. When absent a random UUID is used.
    #[serde(default)]
    pub node_id: Option<String>,
    /// Key in Redis for the lease. Defaults to `oi:lease:writer`.
    #[serde(default)]
    pub key: Option<String>,
    /// Lease TTL seconds. Default 15.
    #[serde(default)]
    pub ttl_secs: Option<u64>,
    /// Refresh interval seconds. Must be < ttl. Default 5.
    #[serde(default)]
    pub refresh_secs: Option<u64>,
}

impl Default for FailoverCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: None,
            key: None,
            ttl_secs: None,
            refresh_secs: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseCfg {
    pub url: String,
    #[serde(default = "default_db")]
    pub database: String,
    #[serde(default = "default_user")]
    pub user: String,
    #[serde(default)]
    pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisCfg {
    pub url: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExchangesCfg {
    /// List of exchanges to enable. When empty the collector runs ALL
    /// adapters whose `impl ExchangeAdapter` is complete. Stubbed adapters
    /// are skipped by the collector regardless.
    #[serde(default)]
    pub enabled: Vec<Exchange>,
}

fn default_db() -> String {
    "oi".into()
}
fn default_user() -> String {
    "default".into()
}
fn default_metrics_addr() -> String {
    "0.0.0.0:9090".into()
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        use figment::providers::{Env, Format, Toml};
        let cfg: Self = figment::Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("OI_").split("__"))
            .extract()?;
        Ok(cfg)
    }
}
