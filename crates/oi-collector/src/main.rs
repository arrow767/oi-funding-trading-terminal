//! oi-collector CLI.
//!
//! Subcommands:
//! * `run` — long-running collector daemon (default when no args).
//! * `resync` — gap-fill one bucket, or the last N minutes.

use clap::{Parser, Subcommand};
use oi_core::exchange::Exchange;
use oi_storage::{clickhouse::ClickHouseRepo, redis::RedisCache, CompositeRepository};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "oi-collector", version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Run the long-lived collector daemon. Default when no subcommand
    /// is supplied.
    Run,
    /// Re-fetch the current OI snapshot and write it to a specific
    /// bucket (or the last N minutes). Use after a short downtime to
    /// patch gaps.
    Resync {
        /// Exchange to resync. Pass `all` to walk every production
        /// adapter.
        #[arg(long)]
        exchange: String,
        /// Explicit bucket — RFC3339 timestamp floored to the minute.
        /// Mutually exclusive with `--minutes`.
        #[arg(long)]
        bucket: Option<String>,
        /// Resync the previous N minutes (1–120). Mutually exclusive
        /// with `--bucket`.
        #[arg(long)]
        minutes: Option<u32>,
        /// Path to config file (same format as the `run` subcommand).
        #[arg(long, env = "OI_CONFIG", default_value = "deploy/collector.toml")]
        config: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command.unwrap_or(Cmd::Run) {
        Cmd::Run => oi_collector::run(),
        Cmd::Resync {
            exchange,
            bucket,
            minutes,
            config,
        } => run_resync(&exchange, bucket.as_deref(), minutes, &config),
    }
}

fn run_resync(
    exchange_raw: &str,
    bucket: Option<&str>,
    minutes: Option<u32>,
    config_path: &std::path::Path,
) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    if bucket.is_none() && minutes.is_none() {
        anyhow::bail!("resync: must specify either --bucket or --minutes");
    }
    if bucket.is_some() && minutes.is_some() {
        anyhow::bail!("resync: --bucket and --minutes are mutually exclusive");
    }

    let cfg = oi_collector::config::Config::load(config_path)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async move {
        let ch = ClickHouseRepo::new(
            &cfg.clickhouse.url,
            &cfg.clickhouse.database,
            &cfg.clickhouse.user,
            &cfg.clickhouse.password,
        );
        let redis = RedisCache::connect(&cfg.redis.url).await?;
        let repo: Arc<dyn oi_core::traits::OiRepository> =
            Arc::new(CompositeRepository::new(ch, redis));

        let exchanges: Vec<Exchange> = if exchange_raw.eq_ignore_ascii_case("all") {
            Exchange::all()
                .iter()
                .copied()
                .filter(|e| oi_exchanges::is_production_ready(*e))
                .collect()
        } else {
            vec![Exchange::from_str(exchange_raw)
                .map_err(anyhow::Error::msg)?]
        };

        let mut total = 0usize;
        for ex in exchanges {
            let n = if let Some(bucket_str) = bucket {
                let ts = OffsetDateTime::parse(
                    bucket_str,
                    &time::format_description::well_known::Rfc3339,
                )?;
                oi_collector::backfill::resync_bucket(ex, ts, &repo).await?
            } else {
                oi_collector::backfill::resync_recent(ex, minutes.unwrap(), &repo).await?
            };
            println!("{ex}: wrote {n} snapshots");
            total += n;
        }
        println!("resync complete: {total} snapshots across exchanges");
        anyhow::Ok(())
    })
}
