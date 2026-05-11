//! Prometheus endpoint for the API server.
//!
//! Prefix `oi_api_*` keeps the namespace separate from the
//! collector's `oi_*` metrics — both can be scraped into the same
//! Prometheus instance without collision.

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, histogram, Unit};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::time::Instant;
use tracing::info;

pub fn install(addr: SocketAddr) -> anyhow::Result<()> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()?;

    describe_counter!(
        "oi_api_requests_total",
        Unit::Count,
        "gRPC/REST requests served, by method and status_code."
    );
    describe_gauge!(
        "oi_api_ws_connections",
        "Currently-open native WebSocket connections to /ws/v1/oi/subscribe."
    );
    describe_counter!(
        "oi_api_ws_frames_sent_total",
        Unit::Count,
        "OI snapshot frames forwarded over native WebSocket. Counts in batches of 100."
    );
    describe_counter!(
        "oi_api_ws_lagged_total",
        Unit::Count,
        "Snapshots dropped because a WS client fell behind the broadcast ring buffer."
    );
    describe_counter!(
        "oi_api_subscribe_frames_total",
        Unit::Count,
        "Live OiSnapshot frames forwarded to gRPC Subscribe clients."
    );
    describe_histogram!(
        "oi_api_request_seconds",
        Unit::Seconds,
        "Request handler latency, by method."
    );
    info!(%addr, "oi-api prometheus endpoint listening");
    Ok(())
}

pub fn inc_request(method: &'static str, status: &'static str) {
    counter!(
        "oi_api_requests_total",
        "method" => method,
        "status" => status,
    )
    .increment(1);
}

pub fn inc_subscribe_frame() {
    counter!("oi_api_subscribe_frames_total").increment(1);
}

pub fn inc_ws_connections() {
    metrics::gauge!("oi_api_ws_connections").increment(1.0);
}

pub fn dec_ws_connections() {
    metrics::gauge!("oi_api_ws_connections").decrement(1.0);
}

pub fn inc_ws_frames_sent(n: u64) {
    counter!("oi_api_ws_frames_sent_total").increment(n);
}

pub fn inc_ws_lagged(skipped: u64) {
    counter!("oi_api_ws_lagged_total").increment(skipped);
}

/// RAII timer: records handler latency on drop. Use in each handler:
/// ```ignore
/// let _t = Timer::start("Latest");
/// ```
#[derive(Debug)]
pub struct Timer {
    method: &'static str,
    started: Instant,
}

impl Timer {
    pub fn start(method: &'static str) -> Self {
        Self {
            method,
            started: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed().as_secs_f64();
        histogram!("oi_api_request_seconds", "method" => self.method).record(elapsed);
    }
}
