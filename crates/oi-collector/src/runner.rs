//! Top-level wiring: bootstrap tracing, load config, build repos and
//! adapters, spin the scheduler loop.
//!
//! Design choice: each exchange gets its own `tokio::task`; they run
//! independently so a slow exchange doesn't hold up faster ones. All write
//! into the same `CompositeRepository`, which itself batches per-task.

use crate::config::Config;
use crate::price_provider::InMemoryPriceProvider;
use crate::scheduler;
use oi_core::{
    exchange::Exchange,
    instrument::InstrumentId,
    snapshot::OiSample,
    traits::{ExchangeAdapter, OiRepository},
};
use oi_exchanges::{
    aster::AsterAdapter, binance::BinanceUsdmAdapter, bingx::BingXAdapter,
    bitget::BitgetAdapter, bybit::BybitAdapter, hyperliquid::HyperliquidAdapter,
    kucoin::KuCoinAdapter, mexc::MexcAdapter, okx::OkxAdapter,
};
use oi_replication::{spawn_lease, LeaseConfig, LeaseManager};
use std::collections::HashMap;
use crate::aggregator::OiAggregator;
use oi_storage::{
    clickhouse::ClickHouseRepo, leader_gated::LeaderGate, redis::RedisCache,
    spawn_drainer_with_gate, CompositeRepository, FileWal, LeaderGatedRepo, WalBacked,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

pub fn bootstrap() -> anyhow::Result<()> {
    init_tracing();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("oi-collector")
        .build()?;
    rt.block_on(run_async())
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_current_span(false)
        .with_span_list(false)
        .init();
}

async fn run_async() -> anyhow::Result<()> {
    let cfg_path = std::env::var("OI_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("deploy/collector.toml"));
    let cfg = Config::load(&cfg_path)?;
    info!(path=?cfg_path, "config loaded");

    // Prometheus endpoint. Failures are logged but don't abort — the
    // collector's job is to collect, not to serve metrics.
    if let Ok(addr) = cfg.metrics_addr.parse::<std::net::SocketAddr>() {
        if let Err(e) = crate::metrics::install(addr) {
            warn!(error=%e, "metrics endpoint failed to bind");
        }
    } else {
        warn!(addr=%cfg.metrics_addr, "unparseable metrics_addr; skipping");
    }

    let ch = ClickHouseRepo::new(
        &cfg.clickhouse.url,
        &cfg.clickhouse.database,
        &cfg.clickhouse.user,
        &cfg.clickhouse.password,
    );
    ch.ensure_schema().await?;
    info!("clickhouse schema ready");

    let redis = RedisCache::connect(&cfg.redis.url).await?;

    // ---- Failover lease first ------------------------------------------
    // Storage layers below want a leader gate; we have to build the
    // lease before them.
    let lease: Option<LeaseManager> = if cfg.failover.enabled {
        let mut lc = LeaseConfig::new(&cfg.redis.url);
        if let Some(ref id) = cfg.failover.node_id {
            lc = lc.with_node_id(id);
        }
        if let Some(ref k) = cfg.failover.key {
            lc.key = k.clone();
        }
        if let Some(t) = cfg.failover.ttl_secs {
            lc.ttl = std::time::Duration::from_secs(t);
        }
        if let Some(r) = cfg.failover.refresh_secs {
            lc.refresh_interval = std::time::Duration::from_secs(r);
        }
        let mgr = spawn_lease(lc).await?;
        info!(node=%mgr.node_id(), "failover lease enabled");
        Some(mgr)
    } else {
        None
    };
    let leader_gate: Option<LeaderGate> = lease.as_ref().map(|m| {
        let m = m.clone();
        Arc::new(move || m.is_leader()) as LeaderGate
    });

    // ---- Storage stack -------------------------------------------------
    // ClickHouseRepo → LeaderGatedRepo (HA only) → WalBacked → composite.
    //
    // Effect of the gating:
    //   * Both nodes write to local WAL on every fetch tick.
    //   * Leader's gated repo passes the inner write through; WAL
    //     ack'ed on success.
    //   * Follower's gated repo synthesizes "not leader" — WAL file
    //     stays pending, drainer is gated off, queue grows in
    //     lockstep with the leader's queue (until promotion).
    //   * On failover, the new leader's drainer wakes up next tick
    //     and replays its own up-to-date queue.
    let ch_arc: Arc<dyn OiRepository> = Arc::new(ch);
    let leader_gated: Arc<dyn OiRepository> = if let Some(ref g) = leader_gate {
        Arc::new(LeaderGatedRepo::new(ch_arc.clone(), g.clone()))
    } else {
        ch_arc.clone()
    };

    let primary: Arc<dyn OiRepository> = if cfg.wal.enabled {
        let wal = FileWal::open(
            std::path::PathBuf::from(&cfg.wal.dir),
            cfg.wal.soft_max_files,
        )
        .await?;
        info!(dir=%cfg.wal.dir, "WAL enabled; snapshots will be persisted before CH write");
        // Drainer points at the gated repo so that when this node is a
        // follower, the drainer's per-file `inner.upsert` gets a
        // synthetic "not leader" Err and aborts the cycle — matching
        // the leader-gated steady state without a separate code path.
        let follower_max_age = cfg
            .wal
            .follower_max_age_secs
            .map(std::time::Duration::from_secs);
        let _drainer = spawn_drainer_with_gate(
            wal.clone(),
            leader_gated.clone(),
            std::time::Duration::from_secs(cfg.wal.drain_interval_secs),
            leader_gate.clone(),
            follower_max_age,
        );
        Arc::new(WalBacked::new(leader_gated, wal))
    } else {
        leader_gated
    };

    let mut composite = CompositeRepository::with_primary(primary, redis);
    if let Some(ref g) = leader_gate {
        composite = composite.with_leader_gate(g.clone());
    }
    // Keep a handle to the pub/sub publisher so live WS tasks can share
    // the same channel with the minute-tick REST write path.
    let live_publisher = composite.publisher();
    let repo: Arc<dyn OiRepository> = Arc::new(composite);
    let prices = Arc::new(InMemoryPriceProvider::with_tolerance(time::Duration::minutes(5)));

    // Build the adapter set. The crate-level `is_production_ready` helper
    // decides which enum variants have a real `impl ExchangeAdapter`;
    // stubs are skipped with a one-time warning rather than producing
    // per-minute failure logs.
    let enabled = if cfg.exchanges.enabled.is_empty() {
        // Default: every production-ready adapter. Adding a new venue =
        // flipping `is_production_ready` + adding a match arm below.
        Exchange::all()
            .iter()
            .copied()
            .filter(|ex| oi_exchanges::is_production_ready(*ex))
            .collect()
    } else {
        cfg.exchanges.enabled.clone()
    };

    // Per-exchange aggregator. The REST loop and any matching live
    // WS task share the same aggregator instance so a sample
    // arriving over WS at 10:00:30 and the REST poll at 10:00:02
    // (carrying the previous minute) fold into one OHLC bar each.
    // Single ownership of `flush_through` — the exchange's REST
    // loop — so there's no draining race.
    let aggregators: HashMap<Exchange, Arc<OiAggregator>> = enabled
        .iter()
        .copied()
        .map(|ex| (ex, Arc::new(OiAggregator::new())))
        .collect();
    let aggregators = Arc::new(aggregators);

    let mut handles = Vec::new();

    // Live WS side-task: spawned for every exchange that exposes a push
    // OI stream. It publishes intra-minute updates on the same Redis
    // pub/sub channel used by the minute-tick write path; ClickHouse
    // still comes from the REST loop so a WS gap can't pollute history.
    if enabled.contains(&Exchange::Bybit) {
        let publisher = live_publisher.clone();
        let lease_hnd = lease.clone();
        let agg = aggregators.get(&Exchange::Bybit).cloned();
        tokio::spawn(async move {
            // One-off discovery just to seed the WS subscription list.
            // The main REST loop also discovers on a 6h cadence and
            // writes metadata to ClickHouse — this sidestep does
            // neither, it only needs the symbol names.
            let adapter = match BybitAdapter::new() {
                Ok(a) => a,
                Err(e) => {
                    error!(error=%e, "bybit live: adapter ctor failed");
                    return;
                }
            };
            let metas = match adapter.discover_instruments().await {
                Ok(m) => m,
                Err(e) => {
                    error!(error=%e, "bybit live: discovery failed");
                    return;
                }
            };
            let symbols: Vec<String> = metas
                .iter()
                .filter(|m| m.active)
                .map(|m| m.id.symbol.clone())
                .collect();
            if let Some(agg) = agg {
                let _ = crate::live::spawn_bybit_live(symbols, publisher, lease_hnd, agg).await;
            }
        });
    }
    if enabled.contains(&Exchange::Bitget) {
        let publisher = live_publisher.clone();
        let lease_hnd = lease.clone();
        let agg = aggregators.get(&Exchange::Bitget).cloned();
        tokio::spawn(async move {
            let adapter = match BitgetAdapter::new() {
                Ok(a) => a,
                Err(e) => {
                    error!(error=%e, "bitget live: adapter ctor failed");
                    return;
                }
            };
            let metas = match adapter.discover_instruments().await {
                Ok(m) => m,
                Err(e) => {
                    error!(error=%e, "bitget live: discovery failed");
                    return;
                }
            };
            let inst_ids: Vec<String> = metas
                .iter()
                .filter(|m| m.active)
                .map(|m| m.id.symbol.clone())
                .collect();
            if let Some(agg) = agg {
                let _ = crate::live::spawn_bitget_live(inst_ids, publisher, lease_hnd, agg).await;
            }
        });
    }
    if enabled.contains(&Exchange::Hyperliquid) {
        let publisher = live_publisher.clone();
        let lease_hnd = lease.clone();
        let agg = aggregators.get(&Exchange::Hyperliquid).cloned();
        tokio::spawn(async move {
            let adapter = match HyperliquidAdapter::new() {
                Ok(a) => a,
                Err(e) => {
                    error!(error=%e, "hyperliquid live: adapter ctor failed");
                    return;
                }
            };
            let metas = match adapter.discover_instruments().await {
                Ok(m) => m,
                Err(e) => {
                    error!(error=%e, "hyperliquid live: discovery failed");
                    return;
                }
            };
            let coins: Vec<String> = metas
                .iter()
                .filter(|m| m.active)
                .map(|m| m.id.symbol.clone())
                .collect();
            if let Some(agg) = agg {
                let _ = crate::live::spawn_hyperliquid_live(coins, publisher, lease_hnd, agg).await;
            }
        });
    }
    if enabled.contains(&Exchange::Okx) {
        let publisher = live_publisher.clone();
        let lease_hnd = lease.clone();
        let agg = aggregators.get(&Exchange::Okx).cloned();
        tokio::spawn(async move {
            let adapter = match OkxAdapter::new() {
                Ok(a) => a,
                Err(e) => {
                    error!(error=%e, "okx live: adapter ctor failed");
                    return;
                }
            };
            let metas = match adapter.discover_instruments().await {
                Ok(m) => m,
                Err(e) => {
                    error!(error=%e, "okx live: discovery failed");
                    return;
                }
            };
            let inst_ids: Vec<String> = metas
                .iter()
                .filter(|m| m.active)
                .map(|m| m.id.symbol.clone())
                .collect();
            if let Some(agg) = agg {
                let _ = crate::live::spawn_okx_live(inst_ids, publisher, lease_hnd, agg).await;
            }
        });
    }

    // Per-exchange shared instrument list — written by the REST
    // loop on every discovery, read by the funding-history sweep
    // task. Keeps the two tasks independent (no channels) while
    // the sweep always sees the latest universe.
    let symbol_lists: HashMap<Exchange, Arc<parking_lot::RwLock<Vec<InstrumentId>>>> =
        enabled
            .iter()
            .copied()
            .map(|ex| (ex, Arc::new(parking_lot::RwLock::new(Vec::new()))))
            .collect();
    let symbol_lists = Arc::new(symbol_lists);

    for ex in enabled {
        let adapter: Arc<dyn ExchangeAdapter> = match ex {
            Exchange::Binance => Arc::new(BinanceUsdmAdapter::new()?),
            Exchange::Bybit => Arc::new(BybitAdapter::new()?),
            Exchange::Hyperliquid => Arc::new(HyperliquidAdapter::new()?),
            Exchange::Okx => Arc::new(OkxAdapter::new()?),
            Exchange::Bitget => Arc::new(BitgetAdapter::new()?),
            Exchange::Mexc => Arc::new(MexcAdapter::new()?),
            Exchange::Aster => Arc::new(AsterAdapter::new()?),
            Exchange::BingX => Arc::new(BingXAdapter::new()?),
            Exchange::KuCoin => Arc::new(KuCoinAdapter::new()?),
        };
        let repo_clone = repo.clone();
        let prices = prices.clone();
        let lease_clone = lease.clone();
        let aggregator = aggregators
            .get(&ex)
            .cloned()
            .expect("aggregator was constructed for every enabled exchange");
        let symbols = symbol_lists
            .get(&ex)
            .cloned()
            .expect("symbol list was constructed for every enabled exchange");

        // Settlement-event sweep — runs alongside the minute-tick
        // REST loop on its own slow cadence (default 30 min).
        let sweep_handle = crate::funding_sweep::spawn_funding_sweep(
            adapter.clone(),
            repo_clone.clone(),
            symbols.clone(),
            std::time::Duration::from_secs(30 * 60),
            8,
            lease_clone.clone(),
        );
        handles.push(sweep_handle);

        let adapter_for_loop = adapter;
        let repo_for_loop = repo_clone;
        let lease_for_loop = lease_clone;
        let handle = tokio::spawn(async move {
            run_exchange_loop(
                adapter_for_loop,
                repo_for_loop,
                prices,
                lease_for_loop,
                aggregator,
                symbols,
            )
            .await;
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }
    Ok(())
}

async fn run_exchange_loop(
    adapter: Arc<dyn ExchangeAdapter>,
    repo: Arc<dyn OiRepository>,
    prices: Arc<InMemoryPriceProvider>,
    lease: Option<LeaseManager>,
    aggregator: Arc<OiAggregator>,
    symbol_list: Arc<parking_lot::RwLock<Vec<InstrumentId>>>,
) {
    let exchange = adapter.exchange();
    info!(%exchange, "starting collector loop");

    // Discover instruments up-front. Refresh every 6h in a side task.
    let mut metas = match adapter.discover_instruments().await {
        Ok(m) => m,
        Err(e) => {
            error!(%exchange, error=%e, "instrument discovery failed");
            return;
        }
    };
    if let Err(e) = repo.upsert_instruments(&metas).await {
        error!(%exchange, error=%e, "instrument upsert failed");
    }
    // Register active instruments' (base, quote) pairs in the
    // cross-exchange fallback index. Idempotent — safe to call again
    // on rediscovery.
    for m in metas.iter().filter(|m| m.active) {
        prices.register_instrument(m.id.clone(), &m.base_asset, &m.quote_asset);
    }
    crate::metrics::set_instruments_tracked(
        exchange.code(),
        metas.iter().filter(|m| m.active).count(),
    );
    // Publish the active universe to the funding-sweep task. It
    // reads on its own schedule; each rediscovery overwrites
    // wholesale.
    *symbol_list.write() = metas
        .iter()
        .filter(|m| m.active)
        .map(|m| m.id.clone())
        .collect();
    info!(%exchange, count=metas.len(), "instruments discovered");

    let mut refresh_meta = tokio::time::interval(Duration::from_secs(6 * 3600));
    refresh_meta.tick().await; // skip immediate

    loop {
        let tick = scheduler::next_tick(Duration::from_secs(2)).await;
        let ids: Vec<InstrumentId> = metas
            .iter()
            .filter(|m| m.active)
            .map(|m| m.id.clone())
            .collect();

        // Prices first — OI depends on having a fresh USD price.
        match adapter.fetch_prices(&ids).await {
            Ok(quotes) => prices.ingest(quotes),
            Err(e) => {
                crate::metrics::inc_rest_error(exchange.code(), classify_err(&e));
                warn!(%exchange, error=%e, "price fetch failed; USD column will be NULL this minute");
            }
        }

        let raw = match adapter.fetch_oi(&ids, tick.bucket_ts).await {
            Ok(r) => r,
            Err(e) => {
                crate::metrics::inc_rest_error(exchange.code(), classify_err(&e));
                error!(%exchange, error=%e, "OI fetch failed");
                continue;
            }
        };

        // Enrich each raw OI into a single OiSample and observe it
        // into the aggregator. For REST-only exchanges (Binance,
        // MEXC, Aster, BingX, KuCoin) this is the ONE sample per
        // minute → the bar trivially has O=H=L=C. For exchanges
        // with WS live push, this REST sample is one of many
        // (the WS task observes others between minute ticks);
        // OHLC is built up across the whole minute.
        for r in raw {
            let Some(meta) = metas.iter().find(|m| m.id == r.instrument) else {
                warn!(%exchange, symbol=%r.instrument.symbol, "OI for unknown symbol; dropping");
                continue;
            };
            // Cross-exchange fallback: when this venue's own price
            // feed is silent, borrow a fresh quote for the same base
            // asset from another exchange (Binance > Bybit > … by
            // declaration order). USD column stays populated even
            // during partial price-feed outages.
            let price = match prices
                .price_usd_with_fallback(
                    &r.instrument,
                    &meta.base_asset,
                    &meta.quote_asset,
                    r.bucket_ts,
                )
                .await
            {
                Some((p, crate::price_provider::Provenance::Native)) => Some(p),
                Some((
                    p,
                    crate::price_provider::Provenance::Fallback { from, quote_match },
                )) => {
                    crate::metrics::inc_price_fallback(
                        exchange.code(),
                        from.code(),
                        quote_match,
                    );
                    Some(p)
                }
                None => None,
            };
            match OiSample::enrich(r, meta, price) {
                Ok(s) => {
                    aggregator.observe(s);
                }
                Err(e) => warn!(%exchange, error=%e, "enrich failed"),
            }
        }

        // Drain bars whose bucket has just closed. `tick.bucket_ts`
        // is the floor of the minute that produced these samples;
        // any bar at or before it is now final and ready to flush.
        // (Bars for the still-open current minute — populated only
        // by the WS live task between ticks — stay in the aggregator
        // for next time.)
        let snaps = aggregator.flush_through(tick.bucket_ts);

        // No runtime gate here — leadership is enforced inside the
        // storage stack:
        //   * `LeaderGatedRepo` fails the inner CH write on follower
        //     so `WalBacked` keeps the file pending.
        //   * `CompositeRepository` skips Redis cache + pub/sub on
        //     follower so the standby doesn't fight the leader.
        // The collector's job here is just "fetch + enrich + hand off".
        crate::metrics::set_leader(lease.as_ref().map_or(true, |l| l.is_leader()));
        match repo.upsert_snapshots(&snaps).await {
            Ok(()) => {
                crate::metrics::inc_snapshots(exchange.code(), snaps.len() as u64);
                info!(%exchange, wrote = snaps.len(), bucket = %tick.bucket_ts, "minute flushed");
            }
            Err(e) => {
                let is_follower_skip = lease.as_ref().map_or(false, |l| !l.is_leader())
                    && e.to_string().contains("not leader");
                if is_follower_skip {
                    debug!(%exchange, "follower; WAL queued for failover, no CH write");
                } else {
                    error!(%exchange, error=%e, "upsert failed");
                }
            }
        }

        // Funding rate — best-effort, separate write path. No WAL,
        // no aggregator (per-minute single value, no OHLC) — funding
        // is cheap to re-fetch on the next tick if a write fails.
        match adapter.fetch_funding(&ids, tick.bucket_ts).await {
            Ok(funding) if !funding.is_empty() => {
                if let Err(e) = repo.upsert_funding(&funding).await {
                    let is_follower_skip = lease.as_ref().map_or(false, |l| !l.is_leader())
                        && e.to_string().contains("not leader");
                    if !is_follower_skip {
                        warn!(%exchange, error=%e, "funding upsert failed");
                    }
                } else {
                    crate::metrics::inc_funding(exchange.code(), funding.len() as u64);
                }
            }
            Ok(_) => {}
            Err(e) => {
                crate::metrics::inc_rest_error(exchange.code(), classify_err(&e));
                warn!(%exchange, error=%e, "funding fetch failed");
            }
        }

        // Opportunistic metadata refresh: non-blocking.
        if refresh_meta.tick().now_or_never().is_some() {
            if let Ok(m) = adapter.discover_instruments().await {
                metas = m;
                let _ = repo.upsert_instruments(&metas).await;
                *symbol_list.write() = metas
                    .iter()
                    .filter(|m| m.active)
                    .map(|m| m.id.clone())
                    .collect();
            }
        }
    }
}

fn classify_err(e: &oi_core::error::ExchangeError) -> &'static str {
    use oi_core::error::ExchangeError as E;
    match e {
        E::Transient { .. } => "http",
        E::RateLimited { .. } => "ratelimit",
        E::Auth(_) => "auth",
        E::Schema(_) => "schema",
        E::NotFound(_) => "notfound",
        E::Unexpected(_) => "other",
    }
}

// Small future-polling helper re-exported from a util module to keep imports
// tidy. We don't need a full `futures::FutureExt` just for `now_or_never`.
trait NowOrNever: Sized {
    fn now_or_never(self) -> Option<()>;
}

impl<F> NowOrNever for F
where
    F: std::future::Future<Output = tokio::time::Instant>,
{
    fn now_or_never(self) -> Option<()> {
        use std::pin::pin;
        use std::task::{Context, Poll};
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut f = pin!(self);
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(_) => Some(()),
            Poll::Pending => None,
        }
    }
}
