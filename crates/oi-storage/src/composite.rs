//! Composite repository: ClickHouse for durability, Redis for hot reads.
//!
//! Write path: fan out to both; Redis miss is non-fatal (warning logged).
//! Read path: `latest` → Redis first, fall back to ClickHouse. `range` always
//! hits ClickHouse — Redis only caches the latest bar.

use crate::clickhouse::ClickHouseRepo;
use crate::leader_gated::LeaderGate;
use crate::pubsub::SnapshotPublisher;
use crate::redis::RedisCache;
use async_trait::async_trait;
use oi_core::{
    error::Result,
    funding::{FundingBar, FundingEvent},
    instrument::{InstrumentId, InstrumentMeta},
    snapshot::OiSnapshot,
    traits::OiRepository,
};
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::warn;

#[derive(Clone)]
pub struct CompositeRepository {
    primary: Arc<dyn OiRepository>,
    cache: RedisCache,
    publisher: SnapshotPublisher,
    /// HA gate. When `None`, all operations run unconditionally.
    /// When `Some`, the cache + pub/sub paths only fire on the
    /// leader — preventing the standby from fighting the leader on
    /// the shared Redis cache and pub/sub channel. The `primary`
    /// path is NOT gated here: it's the WAL→inner path, where the
    /// leader-gating happens inside the inner (e.g. via
    /// `LeaderGatedRepo` between `WalBacked` and `ClickHouseRepo`).
    leader_gate: Option<LeaderGate>,
}

impl std::fmt::Debug for CompositeRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeRepository")
            .field("cache", &self.cache)
            .finish()
    }
}

impl CompositeRepository {
    /// Short-hand for the common "ClickHouse primary + Redis cache"
    /// wiring. Preserves the pre-WAL API.
    pub fn new(primary: ClickHouseRepo, cache: RedisCache) -> Self {
        Self::with_primary(Arc::new(primary), cache)
    }

    /// Accept any `OiRepository` as primary — used to layer
    /// `WalBacked` between ClickHouse and the composite so writes
    /// survive a CH outage.
    pub fn with_primary(primary: Arc<dyn OiRepository>, cache: RedisCache) -> Self {
        let publisher = SnapshotPublisher::new(cache.connection());
        Self {
            primary,
            cache,
            publisher,
            leader_gate: None,
        }
    }

    /// Install an HA leader gate. When supplied, the cache and
    /// pub/sub paths only fire on the leader. Single-node deploys
    /// pass `None` (the default from `with_primary`).
    #[must_use]
    pub fn with_leader_gate(mut self, gate: LeaderGate) -> Self {
        self.leader_gate = Some(gate);
        self
    }

    /// Access the pub/sub publisher. Useful when a sibling task (e.g.
    /// a live WebSocket stream) needs to publish snapshots on the same
    /// channel used by the minute-tick write path.
    #[must_use]
    pub fn publisher(&self) -> SnapshotPublisher {
        self.publisher.clone()
    }
}

#[async_trait]
impl OiRepository for CompositeRepository {
    async fn upsert_snapshots(&self, snaps: &[OiSnapshot]) -> Result<()> {
        // Primary always runs (it's the WAL→CH path; the WAL append
        // is the durable record we want on every node).
        let primary_result = self.primary.upsert_snapshots(snaps).await;

        // Cache + pub/sub fire only on the leader (or unconditionally
        // when no gate was installed). On follower we still complete
        // — the leader's write to the same Redis already covers it.
        let is_leader = self
            .leader_gate
            .as_ref()
            .map_or(true, |g| g());

        if is_leader {
            if let Err(e) = self.cache.bulk_set_latest(snaps).await {
                warn!(error=%e, "redis bulk set failed; latest cache stale");
            }
            // Live push: pub/sub failure is non-fatal — subscribers
            // are an enhancement, not the durable contract.
            if let Err(e) = self.publisher.publish(snaps).await {
                warn!(error=%e, "redis pubsub publish failed; subscribers miss this tick");
            }
        }
        primary_result
    }

    async fn upsert_instruments(&self, metas: &[InstrumentMeta]) -> Result<()> {
        self.primary.upsert_instruments(metas).await
    }

    async fn range(
        &self,
        instrument: &InstrumentId,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Result<Vec<OiSnapshot>> {
        self.primary.range(instrument, from, to).await
    }

    async fn latest(&self, instrument: &InstrumentId) -> Result<Option<OiSnapshot>> {
        match self.cache.get_latest(instrument).await {
            Ok(Some(s)) => Ok(Some(s)),
            Ok(None) => self.primary.latest(instrument).await,
            Err(e) => {
                warn!(error=%e, "redis read failed; falling back to clickhouse");
                self.primary.latest(instrument).await
            }
        }
    }

    async fn upsert_funding(&self, bars: &[FundingBar]) -> Result<()> {
        // Funding doesn't go through the WAL — it's not durability-
        // critical and is cheap to re-fetch on the next tick. Just
        // delegate to the primary repo.
        self.primary.upsert_funding(bars).await
    }

    async fn funding_range(
        &self,
        instrument: &InstrumentId,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Result<Vec<FundingBar>> {
        self.primary.funding_range(instrument, from, to).await
    }

    async fn latest_funding(
        &self,
        instrument: &InstrumentId,
    ) -> Result<Option<FundingBar>> {
        self.primary.latest_funding(instrument).await
    }

    async fn upsert_funding_events(&self, events: &[FundingEvent]) -> Result<()> {
        self.primary.upsert_funding_events(events).await
    }

    async fn funding_events_range(
        &self,
        instrument: &InstrumentId,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Result<Vec<FundingEvent>> {
        self.primary.funding_events_range(instrument, from, to).await
    }

    async fn latest_funding_event(
        &self,
        instrument: &InstrumentId,
    ) -> Result<Option<FundingEvent>> {
        self.primary.latest_funding_event(instrument).await
    }
}
