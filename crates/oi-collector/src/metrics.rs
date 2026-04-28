//! Prometheus metrics surface for the collector.
//!
//! Exposes an HTTP scrape endpoint at the configured `metrics_addr`
//! (`/metrics`). The `metrics` crate's macros are zero-cost when the
//! recorder is absent, so instrumentation can stay in the hot path
//! regardless of whether the endpoint was bound.
//!
//! Metric names follow the `oi_*` prefix so they don't collide with
//! ClickHouse/Redis exporters running side-by-side in the same
//! Prometheus scrape target.

use metrics::{counter, describe_counter, describe_gauge, gauge, Unit};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use tracing::info;

/// Install the global Prometheus recorder and start the scrape endpoint
/// on `addr`. Called once at collector startup. Failing here doesn't
/// stop the collector — metrics are best-effort.
pub fn install(addr: SocketAddr) -> anyhow::Result<()> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()?;

    describe_counter!(
        "oi_snapshots_written_total",
        Unit::Count,
        "Number of OI OHLC bars upserted into ClickHouse, by exchange."
    );
    describe_counter!(
        "oi_funding_written_total",
        Unit::Count,
        "Number of funding-rate samples upserted into ClickHouse, by exchange."
    );
    describe_counter!(
        "oi_funding_events_written_total",
        Unit::Count,
        "Number of settlement-event records upserted into ClickHouse, by exchange."
    );
    describe_counter!(
        "oi_rest_errors_total",
        Unit::Count,
        "REST fetch failures at the minute tick, by exchange and kind (http/schema/ratelimit/other)."
    );
    describe_counter!(
        "oi_live_publishes_total",
        Unit::Count,
        "Sub-minute live snapshots published to Redis pub/sub, by exchange."
    );
    describe_counter!(
        "oi_ws_reconnects_total",
        Unit::Count,
        "WebSocket reconnects observed by the supervisor, by handler name."
    );
    describe_gauge!(
        "oi_instruments_tracked",
        Unit::Count,
        "Active instruments being polled / subscribed, by exchange."
    );
    describe_gauge!(
        "oi_leader",
        "1 if this collector currently holds the failover lease, else 0. Always 1 when failover is disabled."
    );
    describe_counter!(
        "oi_price_fallback_total",
        Unit::Count,
        "Number of enrichments that borrowed a price from another exchange because the native feed was missing/stale, by target exchange and donor exchange."
    );

    info!(%addr, "prometheus metrics endpoint listening");
    Ok(())
}

// --- Helpers that callers use instead of the raw macros, so metric
// names and label shapes are one-stop-shoppable. -----------------------------

pub fn inc_snapshots(exchange: &str, n: u64) {
    counter!("oi_snapshots_written_total", "exchange" => exchange.to_owned()).increment(n);
}

pub fn inc_live_publishes(exchange: &str, n: u64) {
    counter!("oi_live_publishes_total", "exchange" => exchange.to_owned()).increment(n);
}

pub fn inc_rest_error(exchange: &str, kind: &'static str) {
    counter!(
        "oi_rest_errors_total",
        "exchange" => exchange.to_owned(),
        "kind" => kind,
    )
    .increment(1);
}

pub fn set_instruments_tracked(exchange: &str, n: usize) {
    // Cast is fine — a single exchange won't have 2^63 instruments.
    #[allow(clippy::cast_precision_loss)]
    gauge!("oi_instruments_tracked", "exchange" => exchange.to_owned()).set(n as f64);
}

pub fn set_leader(is_leader: bool) {
    gauge!("oi_leader").set(if is_leader { 1.0 } else { 0.0 });
}

pub fn inc_funding(exchange: &str, n: u64) {
    counter!("oi_funding_written_total", "exchange" => exchange.to_owned()).increment(n);
}

pub fn inc_funding_events(exchange: &str, n: u64) {
    counter!("oi_funding_events_written_total", "exchange" => exchange.to_owned())
        .increment(n);
}

pub fn inc_price_fallback(target: &str, donor: &str, quote_match: bool) {
    // `quote_match` is a 0/1 string label so PromQL can
    // `sum by (donor) (rate(...{quote_match="0"}[5m]))` to spotlight
    // loose-quote substitutions specifically.
    counter!(
        "oi_price_fallback_total",
        "exchange" => target.to_owned(),
        "donor" => donor.to_owned(),
        "quote_match" => if quote_match { "1" } else { "0" },
    )
    .increment(1);
}
