//! Leader-gated repository decorator.
//!
//! Wraps an inner [`OiRepository`] (typically the ClickHouse repo)
//! and short-circuits write paths when the supplied gate returns
//! `false`. Used in HA deployments where both nodes run the
//! collector but only the lease-holder actually pushes to CH.
//!
//! On follower:
//! * `upsert_snapshots` returns `Err(CoreError::Storage("not leader"))`.
//!   That looks like a downstream failure — which is intentional:
//!   when this is composed under [`crate::WalBacked`], the WAL file
//!   stays in the queue, ready for a future leader to drain.
//! * Read paths (`range`, `latest`) pass through unconditionally —
//!   followers can still answer queries against their CH replica.
//! * `upsert_instruments` passes through too — instrument metadata
//!   is harmless to keep current on both replicas.

use async_trait::async_trait;
use oi_core::{
    error::{CoreError, Result},
    instrument::{InstrumentId, InstrumentMeta},
    snapshot::OiSnapshot,
    traits::OiRepository,
};
use std::sync::Arc;
use time::OffsetDateTime;

/// `Send + Sync` closure that resolves to the current leadership
/// state. Cheap to clone — typically wraps an `Arc<AtomicBool>`.
pub type LeaderGate = Arc<dyn Fn() -> bool + Send + Sync>;

pub struct LeaderGatedRepo {
    inner: Arc<dyn OiRepository>,
    is_leader: LeaderGate,
}

impl std::fmt::Debug for LeaderGatedRepo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderGatedRepo")
            .field("is_leader", &(self.is_leader)())
            .finish()
    }
}

impl LeaderGatedRepo {
    pub fn new(inner: Arc<dyn OiRepository>, is_leader: LeaderGate) -> Self {
        Self { inner, is_leader }
    }
}

#[async_trait]
impl OiRepository for LeaderGatedRepo {
    async fn upsert_snapshots(&self, snaps: &[OiSnapshot]) -> Result<()> {
        if (self.is_leader)() {
            self.inner.upsert_snapshots(snaps).await
        } else {
            // Synthetic error: keeps any wrapping WAL file pending so
            // the eventual leader (this node, after promotion, OR a
            // re-promoted peer) can drain it.
            Err(CoreError::Storage("not leader".into()))
        }
    }

    async fn upsert_instruments(&self, metas: &[InstrumentMeta]) -> Result<()> {
        // Idempotent metadata writes — fine on both replicas.
        self.inner.upsert_instruments(metas).await
    }

    async fn range(
        &self,
        instrument: &InstrumentId,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Result<Vec<OiSnapshot>> {
        self.inner.range(instrument, from, to).await
    }

    async fn latest(&self, instrument: &InstrumentId) -> Result<Option<OiSnapshot>> {
        self.inner.latest(instrument).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oi_core::{exchange::Exchange, unit::UnitKind};
    use rust_decimal_macros::dec;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    struct CountingInner {
        upserts: AtomicU64,
    }

    #[async_trait]
    impl OiRepository for CountingInner {
        async fn upsert_snapshots(&self, _: &[OiSnapshot]) -> Result<()> {
            self.upserts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn upsert_instruments(&self, _: &[InstrumentMeta]) -> Result<()> {
            Ok(())
        }
        async fn range(
            &self,
            _: &InstrumentId,
            _: OffsetDateTime,
            _: OffsetDateTime,
        ) -> Result<Vec<OiSnapshot>> {
            Ok(vec![])
        }
        async fn latest(&self, _: &InstrumentId) -> Result<Option<OiSnapshot>> {
            Ok(None)
        }
    }

    fn snap() -> OiSnapshot {
        let recv = time::macros::datetime!(2026-04-25 00:00:01 UTC);
        OiSnapshot {
            instrument: InstrumentId::new(Exchange::Binance, "BTCUSDT"),
            bucket_ts: time::macros::datetime!(2026-04-25 00:00:00 UTC),
            first_recv_ts: recv,
            last_recv_ts: recv,
            samples: 1,
            native_unit: UnitKind::Coins,
            native_open: dec!(1),
            native_high: dec!(1),
            native_low: dec!(1),
            native_close: dec!(1),
            oi_coins_open: Some(dec!(1)),
            oi_coins_high: Some(dec!(1)),
            oi_coins_low: Some(dec!(1)),
            oi_coins_close: Some(dec!(1)),
            oi_usd_open: None,
            oi_usd_high: None,
            oi_usd_low: None,
            oi_usd_close: None,
            price_used_close: None,
        }
    }

    #[tokio::test]
    async fn leader_passes_writes_through() {
        let inner = Arc::new(CountingInner {
            upserts: AtomicU64::new(0),
        });
        let leader = Arc::new(AtomicBool::new(true));
        let leader_clone = leader.clone();
        let gate: LeaderGate = Arc::new(move || leader_clone.load(Ordering::Acquire));
        let repo = LeaderGatedRepo::new(inner.clone(), gate);

        repo.upsert_snapshots(&[snap()]).await.unwrap();
        assert_eq!(inner.upserts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn follower_returns_err_and_skips_inner() {
        let inner = Arc::new(CountingInner {
            upserts: AtomicU64::new(0),
        });
        let leader = Arc::new(AtomicBool::new(false));
        let leader_clone = leader.clone();
        let gate: LeaderGate = Arc::new(move || leader_clone.load(Ordering::Acquire));
        let repo = LeaderGatedRepo::new(inner.clone(), gate);

        let err = repo.upsert_snapshots(&[snap()]).await.unwrap_err();
        assert!(err.to_string().contains("not leader"));
        assert_eq!(inner.upserts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn read_paths_pass_through_regardless_of_leadership() {
        let inner = Arc::new(CountingInner {
            upserts: AtomicU64::new(0),
        });
        let leader = Arc::new(AtomicBool::new(false));
        let leader_clone = leader.clone();
        let gate: LeaderGate = Arc::new(move || leader_clone.load(Ordering::Acquire));
        let repo = LeaderGatedRepo::new(inner, gate);

        let id = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        // Both should be Ok — reads aren't gated.
        repo.range(
            &id,
            time::macros::datetime!(2026-04-25 00:00:00 UTC),
            time::macros::datetime!(2026-04-25 01:00:00 UTC),
        )
        .await
        .unwrap();
        let _ = repo.latest(&id).await.unwrap();
    }

    #[tokio::test]
    async fn gate_can_flip_at_runtime() {
        let inner = Arc::new(CountingInner {
            upserts: AtomicU64::new(0),
        });
        let leader = Arc::new(AtomicBool::new(false));
        let leader_clone = leader.clone();
        let gate: LeaderGate = Arc::new(move || leader_clone.load(Ordering::Acquire));
        let repo = LeaderGatedRepo::new(inner.clone(), gate);

        assert!(repo.upsert_snapshots(&[snap()]).await.is_err());
        leader.store(true, Ordering::Release);
        repo.upsert_snapshots(&[snap()]).await.unwrap();
        assert_eq!(inner.upserts.load(Ordering::SeqCst), 1);
    }
}
