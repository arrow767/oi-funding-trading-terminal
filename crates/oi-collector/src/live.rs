//! Sub-minute live streaming: spawn exchange WS handlers, fold each
//! incoming sample into the per-exchange OHLC aggregator, and
//! publish the resulting in-progress bar on the same Redis pub/sub
//! channel the minute-tick REST write path uses.
//!
//! Each WS frame produces an `OiSample` which the aggregator folds
//! into the open/high/low/close of the still-open minute bar. The
//! bar is published to subscribers on every fold, so TradingView-
//! style charts can redraw the in-progress candle in real time. The
//! aggregator's contents are still flushed by the REST tick at the
//! minute boundary, so ClickHouse sees one final bar per minute.
//!
//! ClickHouse is never written from this path — only the REST tick's
//! `flush_through(bucket_ts)` produces the durable rows. A WS gap
//! or corrupted frame can therefore not pollute history.

use crate::aggregator::OiAggregator;
use dashmap::DashMap;
use oi_exchanges::bitget::{
    enrich_bitget, extract_oi_update as bitget_extract, live_to_raw as bitget_to_raw,
    BitgetTickerWs,
};
use oi_exchanges::bybit::{
    classify_frame, enrich_bybit, BybitTickersWs, SymbolState,
};
use oi_exchanges::common::ws::{spawn_ws, Frame};
use oi_exchanges::hyperliquid::{
    enrich_hyperliquid, extract_oi_update as hl_extract, live_to_raw as hl_to_raw,
    HyperliquidActiveAssetCtxWs,
};
use oi_exchanges::okx::{enrich_okx, extract_oi_update, live_to_raw, OkxOpenInterestWs};
use oi_replication::LeaseManager;
use oi_storage::SnapshotPublisher;
use rust_decimal::Decimal;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::{debug, error, info, warn};

/// Gate: if a `LeaseManager` is supplied, only the leader publishes.
/// When `None`, publishing is unconditional (single-node deploys).
fn may_publish(lease: &Option<LeaseManager>) -> bool {
    lease.as_ref().map_or(true, |l| l.is_leader())
}

/// Spawn the Bybit live-stream task. Folds samples into the
/// supplied aggregator (shared with the REST loop) and publishes
/// the in-progress bar on every fold.
pub fn spawn_bybit_live(
    symbols: Vec<String>,
    publisher: SnapshotPublisher,
    lease: Option<LeaseManager>,
    aggregator: Arc<OiAggregator>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if symbols.is_empty() {
            warn!("bybit live: no symbols; task exiting");
            return;
        }
        info!(count = symbols.len(), "bybit live: starting WS supervisor");
        let shards: Vec<Vec<String>> = symbols.chunks(400).map(Vec::from).collect();
        for shard in shards {
            let pub2 = publisher.clone();
            let lease2 = lease.clone();
            let agg = aggregator.clone();
            tokio::spawn(async move {
                run_bybit_shard(shard, pub2, lease2, agg).await;
            });
        }
    })
}

async fn run_bybit_shard(
    symbols: Vec<String>,
    publisher: SnapshotPublisher,
    lease: Option<LeaseManager>,
    aggregator: Arc<OiAggregator>,
) {
    // Bybit's WS sends snapshot/delta frames. We hold per-symbol
    // delta-merge state in `state`; on every OI change we then
    // fold a single OiSample into the cross-exchange aggregator.
    let state: Arc<DashMap<String, SymbolState>> = Arc::new(DashMap::new());
    let handler = BybitTickersWs::new(symbols);
    let (mut rx, _jh) = spawn_ws(handler);

    while let Some(frame) = rx.recv().await {
        let Frame::Payload(value) = frame else {
            continue;
        };
        let Some((ty, symbol)) = classify_frame(&value) else {
            continue;
        };
        let Some(data) = value.get("data") else {
            continue;
        };

        let now = OffsetDateTime::now_utc();
        let bucket = floor_to_minute(now);
        let sample_opt = {
            let mut entry = state.entry(symbol.clone()).or_default();
            if entry.merge(ty, data).is_some() {
                entry.to_raw(&symbol, bucket, now).map(enrich_bybit)
            } else {
                None
            }
        };

        let Some(sample) = sample_opt else { continue };
        let bar = aggregator.observe(sample);
        if !may_publish(&lease) {
            continue;
        }
        debug!(symbol=%bar.instrument.symbol, samples=bar.samples, "bybit live: publishing");
        if let Err(e) = publisher.publish(&[bar]).await {
            error!(error=%e, "bybit live: publish failed");
        } else {
            crate::metrics::inc_live_publishes("bybit", 1);
        }
    }
}

fn floor_to_minute(ts: OffsetDateTime) -> OffsetDateTime {
    let sec = i64::from(ts.second());
    let nanos = ts.nanosecond();
    ts - time::Duration::seconds(sec) - time::Duration::nanoseconds(i64::from(nanos))
}

/// Spawn the OKX `open-interest` live task. Each push is
/// authoritative (no snapshot/delta split); `last` short-circuits
/// no-op pushes before reaching the aggregator.
pub fn spawn_okx_live(
    inst_ids: Vec<String>,
    publisher: SnapshotPublisher,
    lease: Option<LeaseManager>,
    aggregator: Arc<OiAggregator>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if inst_ids.is_empty() {
            warn!("okx live: no inst_ids; task exiting");
            return;
        }
        info!(count = inst_ids.len(), "okx live: starting WS supervisor");
        let shards: Vec<Vec<String>> = inst_ids.chunks(200).map(Vec::from).collect();
        for shard in shards {
            let pub2 = publisher.clone();
            let lease2 = lease.clone();
            let agg = aggregator.clone();
            tokio::spawn(async move {
                run_okx_shard(shard, pub2, lease2, agg).await;
            });
        }
    })
}

/// Spawn the Bitget ticker live task.
pub fn spawn_bitget_live(
    inst_ids: Vec<String>,
    publisher: SnapshotPublisher,
    lease: Option<LeaseManager>,
    aggregator: Arc<OiAggregator>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if inst_ids.is_empty() {
            warn!("bitget live: no inst_ids; task exiting");
            return;
        }
        info!(count = inst_ids.len(), "bitget live: starting WS supervisor");
        let shards: Vec<Vec<String>> = inst_ids.chunks(150).map(Vec::from).collect();
        for shard in shards {
            let pub2 = publisher.clone();
            let lease2 = lease.clone();
            let agg = aggregator.clone();
            tokio::spawn(async move {
                run_bitget_shard(shard, pub2, lease2, agg).await;
            });
        }
    })
}

/// Spawn the Hyperliquid `activeAssetCtx` live task.
pub fn spawn_hyperliquid_live(
    coins: Vec<String>,
    publisher: SnapshotPublisher,
    lease: Option<LeaseManager>,
    aggregator: Arc<OiAggregator>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if coins.is_empty() {
            warn!("hyperliquid live: no coins; task exiting");
            return;
        }
        info!(count = coins.len(), "hyperliquid live: starting WS supervisor");
        run_hyperliquid(coins, publisher, lease, aggregator).await;
    })
}

async fn run_hyperliquid(
    coins: Vec<String>,
    publisher: SnapshotPublisher,
    lease: Option<LeaseManager>,
    aggregator: Arc<OiAggregator>,
) {
    let last: Arc<DashMap<String, Decimal>> = Arc::new(DashMap::new());
    let handler = HyperliquidActiveAssetCtxWs::new(coins);
    let (mut rx, _jh) = spawn_ws(handler);

    while let Some(frame) = rx.recv().await {
        let Frame::Payload(value) = frame else {
            continue;
        };
        let Some((coin, new_value, price)) = hl_extract(&value) else {
            continue;
        };
        // No-op short-circuit BEFORE aggregator — repeat values
        // shouldn't bump the bar's `samples` counter or update
        // last_recv_ts.
        let changed = match last.get(&coin) {
            Some(v) if *v == new_value => false,
            _ => true,
        };
        if !changed {
            continue;
        }
        last.insert(coin.clone(), new_value);
        let now = OffsetDateTime::now_utc();
        let raw = hl_to_raw(coin, new_value, price, now);
        let sample = enrich_hyperliquid(raw);
        let bar = aggregator.observe(sample);
        if !may_publish(&lease) {
            continue;
        }
        debug!(symbol=%bar.instrument.symbol, samples=bar.samples, "hyperliquid live: publishing");
        if let Err(e) = publisher.publish(&[bar]).await {
            error!(error=%e, "hyperliquid live: publish failed");
        } else {
            crate::metrics::inc_live_publishes("hyperliquid", 1);
        }
    }
}

async fn run_bitget_shard(
    inst_ids: Vec<String>,
    publisher: SnapshotPublisher,
    lease: Option<LeaseManager>,
    aggregator: Arc<OiAggregator>,
) {
    let last: Arc<DashMap<String, Decimal>> = Arc::new(DashMap::new());
    let handler = BitgetTickerWs::new(inst_ids);
    let (mut rx, _jh) = spawn_ws(handler);

    while let Some(frame) = rx.recv().await {
        let Frame::Payload(value) = frame else {
            continue;
        };
        let Some((inst_id, new_value, price)) = bitget_extract(&value) else {
            continue;
        };
        let changed = match last.get(&inst_id) {
            Some(v) if *v == new_value => false,
            _ => true,
        };
        if !changed {
            continue;
        }
        last.insert(inst_id.clone(), new_value);
        let now = OffsetDateTime::now_utc();
        let raw = bitget_to_raw(inst_id, new_value, price, now);
        let sample = enrich_bitget(raw);
        let bar = aggregator.observe(sample);
        if !may_publish(&lease) {
            continue;
        }
        debug!(symbol=%bar.instrument.symbol, samples=bar.samples, "bitget live: publishing");
        if let Err(e) = publisher.publish(&[bar]).await {
            error!(error=%e, "bitget live: publish failed");
        } else {
            crate::metrics::inc_live_publishes("bitget", 1);
        }
    }
}

async fn run_okx_shard(
    inst_ids: Vec<String>,
    publisher: SnapshotPublisher,
    lease: Option<LeaseManager>,
    aggregator: Arc<OiAggregator>,
) {
    let last: Arc<DashMap<String, Decimal>> = Arc::new(DashMap::new());
    let handler = OkxOpenInterestWs::new(inst_ids);
    let (mut rx, _jh) = spawn_ws(handler);

    while let Some(frame) = rx.recv().await {
        let Frame::Payload(value) = frame else {
            continue;
        };
        let Some((inst_id, new_value)) = extract_oi_update(&value) else {
            continue;
        };
        let changed = match last.get(&inst_id) {
            Some(v) if *v == new_value => false,
            _ => true,
        };
        if !changed {
            continue;
        }
        last.insert(inst_id.clone(), new_value);
        let now = OffsetDateTime::now_utc();
        let raw = live_to_raw(inst_id, new_value, now);
        let sample = enrich_okx(raw);
        let bar = aggregator.observe(sample);
        if !may_publish(&lease) {
            continue;
        }
        debug!(symbol=%bar.instrument.symbol, samples=bar.samples, "okx live: publishing");
        if let Err(e) = publisher.publish(&[bar]).await {
            error!(error=%e, "okx live: publish failed");
        } else {
            crate::metrics::inc_live_publishes("okx", 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn floor_to_minute_is_correct() {
        assert_eq!(
            floor_to_minute(datetime!(2026-04-25 10:15:37.5 UTC)),
            datetime!(2026-04-25 10:15:00 UTC)
        );
    }
}
