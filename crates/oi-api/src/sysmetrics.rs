//! System resource metrics for the cloud admin monitoring page.
//!
//! Exposes a single auth-protected `/v1/system/metrics` endpoint with
//! CPU / RAM / disk / load + uptime. The cloud API (trading-terminal-
//! cloud) proxies this with the OI bearer so the browser never sees
//! the token; the admin "Servers" page renders the JSON.
//!
//! CPU usage is a delta measurement: sysinfo needs two samples a short
//! interval apart, so the handler refreshes, sleeps ~200 ms, refreshes
//! again. A process-lifetime peak is kept in an atomic (centi-percent)
//! so the page can show "peak since boot" without its own history.

use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use sysinfo::{Disks, System};

/// Peak global CPU %, ×100 so it fits an integer atomic. Monotonic for
/// the lifetime of the process; reset only by a restart.
static CPU_PEAK_CENTI: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Serialize)]
pub struct SystemMetrics {
    hostname: String,
    uptime_secs: u64,
    cpu: CpuMetrics,
    mem: MemMetrics,
    disk: DiskMetrics,
}

#[derive(Debug, Serialize)]
struct CpuMetrics {
    cores: usize,
    usage_pct: f32,
    /// Highest usage_pct observed since this api process started.
    peak_pct: f32,
    load1: f64,
    load5: f64,
    load15: f64,
}

#[derive(Debug, Serialize)]
struct MemMetrics {
    total_bytes: u64,
    used_bytes: u64,
    available_bytes: u64,
    used_pct: f32,
}

#[derive(Debug, Serialize)]
struct DiskMetrics {
    /// Mount point we measured (the largest real filesystem — that's
    /// where ClickHouse data + WAL live).
    mount: String,
    total_bytes: u64,
    used_bytes: u64,
    free_bytes: u64,
    used_pct: f32,
}

/// GET /v1/system/metrics — handler. Auth is enforced by the same
/// bearer middleware as the data endpoints (the route is mounted
/// behind it in `rest::router`).
pub async fn system_metrics() -> impl IntoResponse {
    let _t = crate::metrics::Timer::start("REST_SystemMetrics");

    let mut sys = System::new();

    // CPU delta: first sample, wait one MINIMUM_CPU_UPDATE_INTERVAL,
    // second sample. 200 ms is well above sysinfo's floor and keeps
    // the request snappy.
    sys.refresh_cpu_usage();
    tokio::time::sleep(Duration::from_millis(200)).await;
    sys.refresh_cpu_usage();
    sys.refresh_memory();

    let usage = sys.global_cpu_usage(); // 0..100
    // Update the lifetime peak (compare-and-swap loop — contention is
    // nil here but this keeps it correct if ever called concurrently).
    let centi = (usage * 100.0).round() as u32;
    let mut prev = CPU_PEAK_CENTI.load(Ordering::Relaxed);
    while centi > prev {
        match CPU_PEAK_CENTI.compare_exchange_weak(
            prev,
            centi,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(p) => prev = p,
        }
    }
    let peak_pct = CPU_PEAK_CENTI.load(Ordering::Relaxed) as f32 / 100.0;

    let load = System::load_average();

    let total_mem = sys.total_memory();
    let avail_mem = sys.available_memory();
    let used_mem = total_mem.saturating_sub(avail_mem);
    let mem_pct = if total_mem > 0 {
        used_mem as f32 / total_mem as f32 * 100.0
    } else {
        0.0
    };

    // Disk: pick the mount with the most total space — on the OI box
    // that's the NVMe holding ch_data + WAL, which is what we actually
    // care about running out of.
    let disks = Disks::new_with_refreshed_list();
    let mut best: Option<DiskMetrics> = None;
    for d in disks.list() {
        let total = d.total_space();
        let free = d.available_space();
        let used = total.saturating_sub(free);
        let cand = DiskMetrics {
            mount: d.mount_point().to_string_lossy().into_owned(),
            total_bytes: total,
            used_bytes: used,
            free_bytes: free,
            used_pct: if total > 0 {
                used as f32 / total as f32 * 100.0
            } else {
                0.0
            },
        };
        if best.as_ref().map_or(true, |b| cand.total_bytes > b.total_bytes) {
            best = Some(cand);
        }
    }
    let disk = best.unwrap_or(DiskMetrics {
        mount: "unknown".into(),
        total_bytes: 0,
        used_bytes: 0,
        free_bytes: 0,
        used_pct: 0.0,
    });

    let body = SystemMetrics {
        hostname: System::host_name().unwrap_or_else(|| "unknown".into()),
        uptime_secs: System::uptime(),
        cpu: CpuMetrics {
            cores: num_cpus(&sys),
            usage_pct: usage,
            peak_pct,
            load1: load.one,
            load5: load.five,
            load15: load.fifteen,
        },
        mem: MemMetrics {
            total_bytes: total_mem,
            used_bytes: used_mem,
            available_bytes: avail_mem,
            used_pct: mem_pct,
        },
        disk,
    };
    crate::metrics::inc_request("REST_SystemMetrics", "ok");
    (StatusCode::OK, Json(body))
}

fn num_cpus(sys: &System) -> usize {
    // refresh_cpu_usage already populated the cpu list; len = logical
    // cores. Falls back to 1 so callers never divide by zero.
    let n = sys.cpus().len();
    if n == 0 {
        1
    } else {
        n
    }
}
