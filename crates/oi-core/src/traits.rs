//! Traits that define the extension seams of the platform.
//!
//! The collector never reaches into exchange or storage specifics — it composes
//! four traits:
//! * [`ExchangeAdapter`] — one per venue, produces `RawOi` streams.
//! * [`PriceProvider`] — supplies USD price for USD conversion.
//! * [`OiRepository`] — durable store for snapshots + metadata.
//! * [`OiCollector`] — the orchestration surface (used by the API for
//!   triggering on-demand backfills).
//!
//! Adding a new exchange = `impl ExchangeAdapter` inside `oi-exchanges`.
//! No other crate changes.

use crate::error::{ExchangeError, Result};
use crate::exchange::Exchange;
use crate::funding::{FundingBar, FundingEvent};
use crate::instrument::{InstrumentId, InstrumentMeta};
use crate::price::PriceQuote;
use crate::snapshot::{OiSnapshot, RawOi};
use async_trait::async_trait;
use rust_decimal::Decimal;
use time::OffsetDateTime;

/// One per exchange. Stateless between calls where possible — the collector
/// owns schedule, retries, and persistence.
#[async_trait]
pub trait ExchangeAdapter: Send + Sync + std::fmt::Debug {
    fn exchange(&self) -> Exchange;

    /// Full list of perpetual instruments currently offered. Called at
    /// startup and on a slow cadence (daily).
    async fn discover_instruments(&self) -> std::result::Result<Vec<InstrumentMeta>, ExchangeError>;

    /// Fetch the current OI snapshot for the given instruments. The collector
    /// calls this once per minute on the minute tick.
    ///
    /// Implementations MUST:
    /// * batch as many symbols as the exchange supports per-call,
    /// * respect rate limits (use governor / the exchange-specific limiter),
    /// * set `bucket_ts` to the aligned minute-start for the minute the
    ///   collector asked for (passed via `bucket`),
    /// * return partial results — a failure on one symbol must not drop
    ///   the batch. Errors per symbol are reported out-of-band via tracing.
    async fn fetch_oi(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> std::result::Result<Vec<RawOi>, ExchangeError>;

    /// Optional: exchanges that expose price in the same payload as OI (or
    /// on a cheap ticker endpoint) should return prices here so we avoid a
    /// second round-trip.
    async fn fetch_prices(
        &self,
        _instruments: &[InstrumentId],
    ) -> std::result::Result<Vec<PriceQuote>, ExchangeError> {
        Ok(Vec::new())
    }

    /// Optional: per-minute funding-rate samples. Most exchanges
    /// publish funding alongside OI/price in the same batch
    /// endpoint we already poll for `fetch_oi` / `fetch_prices`,
    /// so the typical implementation issues no extra HTTP call —
    /// it parses the `fundingRate` / `lastFundingRate` /
    /// `nextFundingTime` fields out of the cached payload.
    /// Default returns an empty vec so adapters opt in
    /// individually.
    async fn fetch_funding(
        &self,
        _instruments: &[InstrumentId],
        _bucket: OffsetDateTime,
    ) -> std::result::Result<Vec<FundingBar>, ExchangeError> {
        Ok(Vec::new())
    }

    /// Optional: settlement-event history for one instrument.
    /// `since` lets the collector ask only for events newer than
    /// the last one it persisted — most exchanges support a
    /// `startTime`/`from` parameter natively, so the call is cheap
    /// even on a long-running deployment.
    ///
    /// Per-instrument because most exchanges don't offer a batch
    /// history endpoint. Adapters that DO have batch (rare) can
    /// implement a smarter `fetch_funding_history_batch` later.
    async fn fetch_funding_history(
        &self,
        _instrument: &InstrumentId,
        _since: Option<OffsetDateTime>,
    ) -> std::result::Result<Vec<FundingEvent>, ExchangeError> {
        Ok(Vec::new())
    }
}

/// Supplies USD price when an exchange adapter doesn't co-publish one.
/// Typically backed by a pool of exchange tickers; falls back across sources.
#[async_trait]
pub trait PriceProvider: Send + Sync {
    /// Best-effort price for the given instrument at `near` time. Returns
    /// `None` if no source had a quote within tolerance (the snapshot will
    /// be stored with `oi_usd = NULL`).
    async fn price_usd(
        &self,
        instrument: &InstrumentId,
        near: OffsetDateTime,
    ) -> Option<Decimal>;
}

/// Durable repository. Writes are idempotent on `(instrument, bucket_ts)` —
/// re-ingesting a minute overwrites with the later sample.
#[async_trait]
pub trait OiRepository: Send + Sync {
    /// Upsert many snapshots. Implementations batch & use native bulk APIs
    /// (ClickHouse native insert, Redis pipelined MSET, …).
    async fn upsert_snapshots(&self, snaps: &[OiSnapshot]) -> Result<()>;

    /// Refresh static instrument metadata. Idempotent.
    async fn upsert_instruments(&self, metas: &[InstrumentMeta]) -> Result<()>;

    /// Range query, used by the API for the terminal's OI indicator.
    async fn range(
        &self,
        instrument: &InstrumentId,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Result<Vec<OiSnapshot>>;

    /// Most recent snapshot. Should be served from a hot cache (Redis).
    async fn latest(&self, instrument: &InstrumentId) -> Result<Option<OiSnapshot>>;

    /// Funding-rate write path. Default impl is a no-op so a
    /// repository (e.g. an in-memory test double) can opt out
    /// without ceremony — callers that need funding storage use
    /// `CompositeRepository` which implements all three methods.
    async fn upsert_funding(&self, _bars: &[FundingBar]) -> Result<()> {
        Ok(())
    }

    /// Funding-rate range query. Empty default for repos that don't
    /// store funding.
    async fn funding_range(
        &self,
        _instrument: &InstrumentId,
        _from: OffsetDateTime,
        _to: OffsetDateTime,
    ) -> Result<Vec<FundingBar>> {
        Ok(Vec::new())
    }

    /// Most recent funding sample for `instrument`.
    async fn latest_funding(
        &self,
        _instrument: &InstrumentId,
    ) -> Result<Option<FundingBar>> {
        Ok(None)
    }

    /// Persist settlement events. Idempotent on
    /// `(instrument, settlement_ts)` so repeat polls of the same
    /// history window are safe.
    async fn upsert_funding_events(&self, _events: &[FundingEvent]) -> Result<()> {
        Ok(())
    }

    /// Settlement events in `[from, to)`.
    async fn funding_events_range(
        &self,
        _instrument: &InstrumentId,
        _from: OffsetDateTime,
        _to: OffsetDateTime,
    ) -> Result<Vec<FundingEvent>> {
        Ok(Vec::new())
    }

    /// Most recent settlement event we've stored for `instrument`.
    /// Drives the collector's "since" cursor when polling history.
    async fn latest_funding_event(
        &self,
        _instrument: &InstrumentId,
    ) -> Result<Option<FundingEvent>> {
        Ok(None)
    }
}

/// Orchestration facade — mainly for the API to trigger backfills or
/// request a forced refresh.
#[async_trait]
pub trait OiCollector: Send + Sync {
    /// Kick a one-off collection cycle for a specific minute (used for
    /// gap-filling after an incident).
    async fn collect_minute(&self, exchange: Exchange, minute: OffsetDateTime) -> Result<usize>;
}
