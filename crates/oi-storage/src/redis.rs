//! Redis hot cache for the most recent `OiSnapshot` per instrument.
//!
//! * Key:     `oi:latest:{exchange}:{symbol}`
//! * Value:   MessagePack-encoded `OiSnapshot` (denser than JSON, decodes
//!            faster than serde_json for this payload size).
//! * TTL:     15 minutes — latest should be touched every minute; if the
//!            value stops being refreshed the API falls back to ClickHouse.
//!
//! Also a separate `instruments` set that the API uses to enumerate symbols
//! without querying ClickHouse.

use oi_core::{
    error::{CoreError, Result},
    instrument::InstrumentId,
    snapshot::OiSnapshot,
};
use redis::{aio::ConnectionManager, AsyncCommands};

const LATEST_TTL_SECS: u64 = 15 * 60;

#[derive(Clone)]
pub struct RedisCache {
    conn: ConnectionManager,
}

impl std::fmt::Debug for RedisCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisCache").finish()
    }
}

impl RedisCache {
    pub async fn connect(url: &str) -> Result<Self> {
        let client = redis::Client::open(url).map_err(redis_err)?;
        let conn = ConnectionManager::new(client).await.map_err(redis_err)?;
        Ok(Self { conn })
    }

    /// Expose the underlying connection manager so other components
    /// (e.g. `SnapshotPublisher`) can reuse it for non-PUBSUB commands.
    /// The manager is multiplexed and safe to share across tasks.
    #[must_use]
    pub fn connection(&self) -> ConnectionManager {
        self.conn.clone()
    }

    /// Liveness probe. Issues a single `PING` against the multiplexed
    /// connection. Used by `/health/ready`.
    pub async fn probe(&self) -> Result<()> {
        let mut conn = self.conn.clone();
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        if pong == "PONG" {
            Ok(())
        } else {
            Err(CoreError::Storage(format!("unexpected PING reply: {pong}")))
        }
    }

    pub async fn set_latest(&self, snap: &OiSnapshot) -> Result<()> {
        let bytes = rmp_serde::to_vec_named(snap)
            .map_err(|e| CoreError::Storage(format!("msgpack: {e}")))?;
        let key = latest_key(&snap.instrument);
        let mut conn = self.conn.clone();
        let _: () = conn
            .set_ex(key, bytes, LATEST_TTL_SECS)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    pub async fn get_latest(&self, id: &InstrumentId) -> Result<Option<OiSnapshot>> {
        let mut conn = self.conn.clone();
        let bytes: Option<Vec<u8>> = conn.get(latest_key(id)).await.map_err(redis_err)?;
        match bytes {
            None => Ok(None),
            Some(b) => rmp_serde::from_slice::<OiSnapshot>(&b)
                .map(Some)
                .map_err(|e| CoreError::Storage(format!("msgpack decode: {e}"))),
        }
    }

    /// Pipeline many `set_latest`s. Called by the collector after each
    /// minute's enrichment — avoids 9×N round-trips.
    pub async fn bulk_set_latest(&self, snaps: &[OiSnapshot]) -> Result<()> {
        if snaps.is_empty() {
            return Ok(());
        }
        let mut pipe = redis::pipe();
        for s in snaps {
            let bytes = rmp_serde::to_vec_named(s)
                .map_err(|e| CoreError::Storage(format!("msgpack: {e}")))?;
            pipe.set_ex(latest_key(&s.instrument), bytes, LATEST_TTL_SECS)
                .ignore();
        }
        let mut conn = self.conn.clone();
        pipe.query_async::<()>(&mut conn).await.map_err(redis_err)?;
        Ok(())
    }
}

fn latest_key(id: &InstrumentId) -> String {
    format!("oi:latest:{}", id.key())
}

fn redis_err(e: redis::RedisError) -> CoreError {
    CoreError::Storage(format!("redis: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use oi_core::exchange::Exchange;

    #[test]
    fn latest_key_is_stable() {
        let id = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        assert_eq!(latest_key(&id), "oi:latest:binance:BTCUSDT");
    }
}
