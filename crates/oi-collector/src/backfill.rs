//! Gap-fill / resync subcommand.
//!
//! What this CAN do: take the current OI snapshot from each exchange
//! and write it to a specified `bucket_ts`. Useful for "I restarted
//! at HH:MM:15 and missed the HH:MM bucket" — a minute-ago snapshot
//! is close enough to backfill the hole.
//!
//! What this explicitly does NOT do: reconstruct historical OI from
//! hours ago. Most exchanges publish OI as a current-state value, not
//! as a time series — there's nothing to query. For true historical
//! gaps there's no universal API; the hole stays open and a dashboard
//! query will see a gap.
//!
//! Usage:
//! ```text
//! oi-collector resync --exchange binance --bucket 2026-04-24T10:15:00Z
//! oi-collector resync --exchange all --minutes 5
//! ```

use oi_core::{
    exchange::Exchange,
    instrument::InstrumentMeta,
    snapshot::{OiSample, OiSnapshot},
    traits::{ExchangeAdapter, OiRepository},
};
use oi_exchanges::{
    aster::AsterAdapter, binance::BinanceUsdmAdapter, bingx::BingXAdapter,
    bitget::BitgetAdapter, bybit::BybitAdapter, hyperliquid::HyperliquidAdapter,
    kucoin::KuCoinAdapter, mexc::MexcAdapter, okx::OkxAdapter,
};
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tracing::{info, warn};

fn adapter_for(ex: Exchange) -> anyhow::Result<Arc<dyn ExchangeAdapter>> {
    Ok(match ex {
        Exchange::Binance => Arc::new(BinanceUsdmAdapter::new()?),
        Exchange::Bybit => Arc::new(BybitAdapter::new()?),
        Exchange::Hyperliquid => Arc::new(HyperliquidAdapter::new()?),
        Exchange::Okx => Arc::new(OkxAdapter::new()?),
        Exchange::Bitget => Arc::new(BitgetAdapter::new()?),
        Exchange::Mexc => Arc::new(MexcAdapter::new()?),
        Exchange::Aster => Arc::new(AsterAdapter::new()?),
        Exchange::BingX => Arc::new(BingXAdapter::new()?),
        Exchange::KuCoin => Arc::new(KuCoinAdapter::new()?),
    })
}

/// Fetch current OI for `exchange` and write it as the snapshot for
/// `bucket`. Returns the number of rows upserted.
///
/// Discovery is called fresh per resync — the universe may have
/// shifted since last runtime discovery. For big universes this adds
/// ~1 round-trip; the overhead is acceptable for a repair tool.
pub async fn resync_bucket(
    exchange: Exchange,
    bucket: OffsetDateTime,
    repo: &Arc<dyn OiRepository>,
) -> anyhow::Result<usize> {
    let adapter = adapter_for(exchange)?;
    let metas = adapter.discover_instruments().await?;
    repo.upsert_instruments(&metas).await?;

    let ids: Vec<_> = metas
        .iter()
        .filter(|m| m.active)
        .map(|m| m.id.clone())
        .collect();

    let prices = adapter.fetch_prices(&ids).await.unwrap_or_default();
    let price_by_inst: std::collections::HashMap<_, _> =
        prices.into_iter().map(|p| (p.instrument.clone(), p)).collect();

    let raws = adapter.fetch_oi(&ids, bucket).await?;

    let meta_by_inst: std::collections::HashMap<_, &InstrumentMeta> =
        metas.iter().map(|m| (m.id.clone(), m)).collect();

    // Resync produces ONE bar per instrument: a single snapshot
    // promoted to a degenerate OHLC bar where O=H=L=C. This is the
    // best we can do — historical sub-minute fidelity is gone with
    // the exchange API, and the sample we just fetched is "current"
    // not "historical".
    let mut snaps = Vec::with_capacity(raws.len());
    for r in raws {
        let Some(meta) = meta_by_inst.get(&r.instrument) else {
            continue;
        };
        let price = r
            .price_hint
            .as_ref()
            .map(|q| q.price)
            .or_else(|| price_by_inst.get(&r.instrument).map(|q| q.price));
        match OiSample::enrich(r, meta, price) {
            Ok(s) => snaps.push(OiSnapshot::start_from_sample(s)),
            Err(e) => warn!(error=%e, "resync: enrich failed"),
        }
    }

    let n = snaps.len();
    repo.upsert_snapshots(&snaps).await?;
    info!(%exchange, wrote = n, %bucket, "resync bucket written");
    Ok(n)
}

/// Resync the previous `minutes` buckets. For each, calls
/// `resync_bucket`. Minutes are floor-aligned to UTC and walked
/// oldest-first.
pub async fn resync_recent(
    exchange: Exchange,
    minutes: u32,
    repo: &Arc<dyn OiRepository>,
) -> anyhow::Result<usize> {
    let now = OffsetDateTime::now_utc();
    let start_min = floor_to_minute(now) - Duration::minutes(i64::from(minutes));
    let mut total = 0;
    for i in 0..i64::from(minutes) {
        let bucket = start_min + Duration::minutes(i);
        match resync_bucket(exchange, bucket, repo).await {
            Ok(n) => total += n,
            Err(e) => warn!(%exchange, %bucket, error=%e, "resync minute failed"),
        }
    }
    Ok(total)
}

fn floor_to_minute(ts: OffsetDateTime) -> OffsetDateTime {
    let sec = i64::from(ts.second());
    let nanos = ts.nanosecond();
    ts - Duration::seconds(sec) - Duration::nanoseconds(i64::from(nanos))
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn floor_to_minute_zeroes_sub_minute_components() {
        assert_eq!(
            floor_to_minute(datetime!(2026-04-24 10:15:37.5 UTC)),
            datetime!(2026-04-24 10:15:00 UTC)
        );
    }
}
