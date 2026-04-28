//! Collector daemon library: scheduler, enrichment pipeline, health surface.

pub mod aggregator;
pub mod config;
pub mod scheduler;
pub mod runner;
pub mod price_provider;
pub mod live;
pub mod metrics;
pub mod backfill;
pub mod funding_sweep;

pub fn run() -> anyhow::Result<()> {
    runner::bootstrap()
}
