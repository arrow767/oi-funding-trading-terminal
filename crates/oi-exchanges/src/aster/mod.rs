//! Aster Finance perpetual DEX adapter.
//!
//! Aster's Pro API is Binance-compatible at the path level:
//! `/fapi/v1/exchangeInfo`, `/fapi/v1/openInterest`, `/fapi/v1/premiumIndex`
//! have the same request/response shapes. We delegate to
//! `BinanceUsdmAdapter::with_base_url(…)` and rewrite the `Exchange`
//! tag on the outputs.
//!
//! This keeps one adapter's worth of parsing/retry code shared, while
//! isolating Aster-specific concerns (base URL, tagging, possible future
//! divergence) in this thin wrapper. If Aster's response shapes ever
//! diverge from Binance's, the wrapper is the place to add the
//! branching — the delegated adapter stays pristine.
//!
//! Unit: coins (per Binance parity).
//!
//! Rate limits: not yet publicly documented; we keep the Binance
//! defaults (20 rps burst 40) which are conservative enough that
//! anything Aster imposes will be higher.

use crate::binance::BinanceUsdmAdapter;
use async_trait::async_trait;
use oi_core::{
    error::ExchangeError,
    exchange::Exchange,
    instrument::{InstrumentId, InstrumentMeta},
    price::PriceQuote,
    snapshot::RawOi,
    traits::ExchangeAdapter,
};
use time::OffsetDateTime;

pub const DEFAULT_BASE_URL: &str = "https://fapi.asterdex.com";

#[derive(Clone)]
pub struct AsterAdapter {
    inner: BinanceUsdmAdapter,
}

impl std::fmt::Debug for AsterAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsterAdapter").finish()
    }
}

impl Default for AsterAdapter {
    fn default() -> Self {
        Self::new().expect("aster http client")
    }
}

impl AsterAdapter {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, ExchangeError> {
        Ok(Self {
            inner: BinanceUsdmAdapter::with_base_url(base_url)?,
        })
    }
}

/// Rewrite every `InstrumentId` in a collection to a new exchange tag.
/// The Binance adapter stamps its own `Exchange::Binance` on every
/// returned row — we re-tag to `Exchange::Aster` before anything leaves
/// this module.
fn retag(exchange: Exchange, mut id: InstrumentId) -> InstrumentId {
    id.exchange = exchange;
    id
}

#[async_trait]
impl ExchangeAdapter for AsterAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Aster
    }

    async fn discover_instruments(&self) -> Result<Vec<InstrumentMeta>, ExchangeError> {
        let metas = self.inner.discover_instruments().await?;
        Ok(metas
            .into_iter()
            .map(|mut m| {
                m.id = retag(Exchange::Aster, m.id);
                m
            })
            .collect())
    }

    async fn fetch_oi(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> Result<Vec<RawOi>, ExchangeError> {
        // The inner adapter pulls `.symbol` off the ID to build the URL
        // and doesn't inspect the exchange tag — so passing through as
        // `Exchange::Aster` IDs works, but let's re-tag to Binance for
        // the delegated call to match the inner adapter's own
        // post-condition (returned rows carry Binance).
        let forwarded: Vec<InstrumentId> = instruments
            .iter()
            .map(|i| InstrumentId::new(Exchange::Binance, i.symbol.clone()))
            .collect();
        let raw = self.inner.fetch_oi(&forwarded, bucket).await?;
        Ok(raw
            .into_iter()
            .map(|mut r| {
                r.instrument = retag(Exchange::Aster, r.instrument);
                if let Some(ref mut hint) = r.price_hint {
                    hint.instrument = retag(Exchange::Aster, hint.instrument.clone());
                }
                r
            })
            .collect())
    }

    async fn fetch_prices(
        &self,
        instruments: &[InstrumentId],
    ) -> Result<Vec<PriceQuote>, ExchangeError> {
        let forwarded: Vec<InstrumentId> = instruments
            .iter()
            .map(|i| InstrumentId::new(Exchange::Binance, i.symbol.clone()))
            .collect();
        let quotes = self.inner.fetch_prices(&forwarded).await?;
        Ok(quotes
            .into_iter()
            .map(|mut q| {
                q.instrument = retag(Exchange::Aster, q.instrument);
                q
            })
            .collect())
    }

    async fn fetch_funding_history(
        &self,
        instrument: &InstrumentId,
        since: Option<time::OffsetDateTime>,
    ) -> Result<Vec<oi_core::FundingEvent>, ExchangeError> {
        let forwarded = InstrumentId::new(Exchange::Binance, instrument.symbol.clone());
        let events = self.inner.fetch_funding_history(&forwarded, since).await?;
        Ok(events
            .into_iter()
            .map(|mut e| {
                e.instrument = retag(Exchange::Aster, e.instrument);
                e
            })
            .collect())
    }

    async fn fetch_funding(
        &self,
        instruments: &[InstrumentId],
        bucket: time::OffsetDateTime,
    ) -> Result<Vec<oi_core::FundingBar>, ExchangeError> {
        let forwarded: Vec<InstrumentId> = instruments
            .iter()
            .map(|i| InstrumentId::new(Exchange::Binance, i.symbol.clone()))
            .collect();
        let bars = self.inner.fetch_funding(&forwarded, bucket).await?;
        Ok(bars
            .into_iter()
            .map(|mut b| {
                b.instrument = retag(Exchange::Aster, b.instrument);
                b
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retag_changes_only_the_exchange() {
        let id = InstrumentId::new(Exchange::Binance, "BTCUSDT".to_owned());
        let tagged = retag(Exchange::Aster, id);
        assert_eq!(tagged.exchange, Exchange::Aster);
        assert_eq!(tagged.symbol, "BTCUSDT");
    }
}
