//! Settlement-event sweep: every `interval_secs` walk every active
//! instrument and pull new funding events since the last one we
//! stored.
//!
//! Per-symbol fan-out at bounded concurrency. The
//! "since" cursor comes from `repo.latest_funding_event(id)` so we
//! never re-fetch what we already have. Idempotent at the storage
//! layer (`ReplacingMergeTree(ingest_ts)`) so a duplicate window
//! costs ~one extra IO and never breaks anything.
//!
//! Cadence is configurable; default 30 minutes is generous for 8h
//! venues (3 settlements per day → at most one new event per 8h)
//! and tight enough for Hyperliquid's hourly rate.

use futures::{stream::FuturesUnordered, StreamExt};
use oi_core::{
    funding::FundingEvent,
    instrument::InstrumentId,
    traits::{ExchangeAdapter, OiRepository},
};
use oi_replication::LeaseManager;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tracing::{debug, error, info, warn};

/// Spawn the per-exchange sweep task. Owns its own polling loop,
/// looks up symbols via the supplied adapter at the start of each
/// cycle, and writes events through the supplied repo.
pub fn spawn_funding_sweep(
    adapter: Arc<dyn ExchangeAdapter>,
    repo: Arc<dyn OiRepository>,
    instruments: Arc<parking_lot::RwLock<Vec<InstrumentId>>>,
    interval: Duration,
    concurrency: usize,
    lease: Option<LeaseManager>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let exchange = adapter.exchange();
        info!(%exchange, secs = interval.as_secs(), "funding sweep starting");
        // Stagger first run so all exchanges don't hit upstream
        // simultaneously on cold start.
        tokio::time::sleep(Duration::from_secs(15)).await;
        loop {
            // Skip on follower — leader will sweep, no point
            // duplicating outbound API calls or doubling write
            // pressure on the (idempotent) storage. The lease's
            // own writes pay for the catch-up.
            let is_leader = lease.as_ref().map_or(true, |l| l.is_leader());
            if is_leader {
                let ids: Vec<InstrumentId> = instruments.read().clone();
                let stats = sweep_once(&adapter, &repo, &ids, concurrency).await;
                info!(
                    %exchange,
                    instruments = ids.len(),
                    new_events = stats.events,
                    failures = stats.failures,
                    "funding sweep cycle complete"
                );
                crate::metrics::inc_funding_events(exchange.code(), stats.events);
            } else {
                debug!(%exchange, "funding sweep skipped (follower)");
            }
            tokio::time::sleep(interval).await;
        }
    })
}

/// Minimum wall-clock gap between launching two per-symbol funding-
/// history requests. Binance's WAF/abuse layer (NOT the JSON weight
/// limiter — it returns a bare HTML "403 Forbidden") blocks a tight
/// burst of ~570 requests from one IP. It 403-blocked the burst TAIL
/// every cycle, so the same later symbols (e.g. AIAUSDT) never
/// advanced while the early ones stayed fresh. ~8 req/s steady stays
/// under the abuse threshold; even the largest universe (~900 syms)
/// then finishes in ~2 min — trivial against the 30-min cadence.
const SPAWN_SPACING: Duration = Duration::from_millis(120);

/// A symbol whose latest stored settlement is fresher than this can't
/// have a new one yet (shortest interval anywhere is Hyperliquid's
/// 1 h), so the sweep skips its HTTP call. 50 min < 1 h leaves margin
/// for early/jittered settlements while still skipping the vast
/// majority of the every-cycle re-requests that tripped Binance's WAF.
const NOT_DUE_BEFORE: time::Duration = time::Duration::minutes(50);

#[derive(Debug, Default)]
struct SweepStats {
    events: u64,
    failures: u64,
}

async fn sweep_once(
    adapter: &Arc<dyn ExchangeAdapter>,
    repo: &Arc<dyn OiRepository>,
    instruments: &[InstrumentId],
    concurrency: usize,
) -> SweepStats {
    let mut stream = FuturesUnordered::new();
    let mut iter = instruments.iter().cloned();

    let prime = |inst: InstrumentId| {
        let adapter = adapter.clone();
        let repo = repo.clone();
        async move {
            let since = match repo.latest_funding_event(&inst).await {
                Ok(Some(e)) => Some(e.settlement_ts),
                Ok(None) => None,
                Err(e) => {
                    warn!(%inst, error=%e, "funding sweep: cursor lookup failed");
                    None
                }
            };
            // Due-gate: the shortest funding interval anywhere is 1 h
            // (Hyperliquid; most venues 4-8 h). If the last stored
            // settlement is fresher than NOT_DUE_BEFORE there cannot be
            // a new one yet, so skip the HTTP call entirely. This is
            // safe — the cursor is persistent and the fetch idempotent,
            // so a settlement is still picked up on the first sweep
            // after it's actually due (≤ one 30-min cycle late, same as
            // before). It cuts steady-state request volume ~16x, which
            // is what stops Binance's WAF returning bare 403s for the
            // burst tail (symbols like AIAUSDT that never advanced).
            if let Some(s) = since {
                if OffsetDateTime::now_utc() - s < NOT_DUE_BEFORE {
                    return Ok(Vec::new());
                }
            }
            adapter.fetch_funding_history(&inst, since).await
        }
    };

    for _ in 0..concurrency {
        if let Some(inst) = iter.next() {
            tokio::time::sleep(SPAWN_SPACING).await;
            stream.push(tokio::spawn(prime(inst)));
        }
    }

    let mut stats = SweepStats::default();
    let mut batch: Vec<FundingEvent> = Vec::new();
    while let Some(joined) = stream.next().await {
        if let Some(inst) = iter.next() {
            tokio::time::sleep(SPAWN_SPACING).await;
            stream.push(tokio::spawn(prime(inst)));
        }
        match joined {
            Err(e) => {
                stats.failures += 1;
                warn!(error=%e, "funding sweep task panicked");
            }
            Ok(Err(e)) => {
                stats.failures += 1;
                debug!(error=%e, "funding sweep: per-symbol fetch failed");
            }
            Ok(Ok(events)) if events.is_empty() => {}
            Ok(Ok(events)) => {
                stats.events += events.len() as u64;
                batch.extend(events);
                // Flush in chunks so a panic mid-sweep loses at
                // most one chunk; ClickHouse's
                // ReplacingMergeTree handles duplicates if the
                // next cycle picks them up again.
                if batch.len() >= 500 {
                    if let Err(e) = repo.upsert_funding_events(&batch).await {
                        error!(error=%e, count=batch.len(), "funding sweep: chunk upsert failed");
                        stats.failures += 1;
                    }
                    batch.clear();
                }
            }
        }
    }
    if !batch.is_empty() {
        if let Err(e) = repo.upsert_funding_events(&batch).await {
            error!(error=%e, count=batch.len(), "funding sweep: tail upsert failed");
            stats.failures += 1;
        }
    }
    let _ = OffsetDateTime::now_utc(); // for dropping warning
    stats
}
