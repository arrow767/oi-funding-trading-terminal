//! Redis pub/sub bus carrying freshly-written `OiSnapshot`s.
//!
//! The collector publishes after every minute flush. The API subscribes
//! per-client and fans out to gRPC streams. Payloads are MessagePack —
//! same codec as the hot cache, ~3–4× denser than JSON.
//!
//! Channel layout:
//! * `oi:stream`               — firehose, every snapshot.
//! * `oi:stream:{exchange}`    — per-exchange shard. Subscribers that only
//!   care about one venue can `SUBSCRIBE` (exact-match) instead of
//!   pattern-matching. Both channels are published to on every flush —
//!   the extra write is cheap, the extra read-filter saving is not.

use oi_core::{
    error::{CoreError, Result},
    snapshot::OiSnapshot,
};
use redis::RedisError;

pub const FIREHOSE_CHANNEL: &str = "oi:stream";

#[must_use]
pub fn exchange_channel(ex: oi_core::Exchange) -> String {
    format!("{FIREHOSE_CHANNEL}:{}", ex.code())
}

/// Publisher. Shares the Redis ConnectionManager with the rest of the
/// storage layer — normal commands (including PUBLISH) are fine on a
/// multiplexed connection. Only the subscriber side needs a dedicated
/// socket.
#[derive(Clone)]
pub struct SnapshotPublisher {
    conn: redis::aio::ConnectionManager,
}

impl std::fmt::Debug for SnapshotPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotPublisher").finish()
    }
}

impl SnapshotPublisher {
    pub fn new(conn: redis::aio::ConnectionManager) -> Self {
        Self { conn }
    }

    /// Pipeline-publish every snapshot to the firehose AND the
    /// per-exchange channel. Errors are returned — the collector logs
    /// and moves on; publish failure must not fail the write path.
    pub async fn publish(&self, snaps: &[OiSnapshot]) -> Result<()> {
        if snaps.is_empty() {
            return Ok(());
        }
        let mut pipe = redis::pipe();
        for s in snaps {
            let bytes = rmp_serde::to_vec_named(s)
                .map_err(|e| CoreError::Storage(format!("msgpack: {e}")))?;
            pipe.publish(FIREHOSE_CHANNEL, bytes.clone()).ignore();
            pipe.publish(exchange_channel(s.instrument.exchange), bytes)
                .ignore();
        }
        let mut conn = self.conn.clone();
        pipe.query_async::<()>(&mut conn).await.map_err(redis_err)?;
        Ok(())
    }
}

fn redis_err(e: RedisError) -> CoreError {
    CoreError::Storage(format!("redis: {e}"))
}

/// Subscribe to the firehose (or a per-exchange channel). Returns an
/// async stream of decoded snapshots.
///
/// The caller owns a dedicated Redis connection for the lifetime of the
/// subscription — that's required by the protocol, ConnectionManager
/// can't be used here.
pub async fn subscribe(
    url: &str,
    channel: &str,
) -> Result<impl futures::Stream<Item = OiSnapshot>> {
    use futures::StreamExt;
    let client = redis::Client::open(url).map_err(redis_err)?;
    let mut pubsub = client
        .get_async_pubsub()
        .await
        .map_err(redis_err)?;
    pubsub.subscribe(channel).await.map_err(redis_err)?;

    let stream = pubsub.into_on_message().filter_map(|msg| async move {
        let payload: Vec<u8> = msg.get_payload().ok()?;
        rmp_serde::from_slice::<OiSnapshot>(&payload).ok()
    });
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use oi_core::Exchange;

    #[test]
    fn channel_names_are_stable() {
        assert_eq!(FIREHOSE_CHANNEL, "oi:stream");
        assert_eq!(exchange_channel(Exchange::Binance), "oi:stream:binance");
        assert_eq!(exchange_channel(Exchange::Hyperliquid), "oi:stream:hyperliquid");
    }
}
