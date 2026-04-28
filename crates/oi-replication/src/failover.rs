//! Redis-backed leader lease for collector HA.
//!
//! Exactly one collector in a cluster holds the lease at any time.
//! The leader refreshes its lease every `refresh_interval`; if the
//! refresh is missed (process crash, network partition), the key
//! expires after `ttl` and any standby can acquire it.
//!
//! Semantics:
//! * Acquire: `SET oi:lease:writer <node_id> NX PX <ttl_ms>` —
//!   atomic, set-if-not-exists with millisecond TTL.
//! * Refresh: same command with `XX` (set-if-exists) instead of `NX`,
//!   plus a Lua script guard so we only refresh our OWN lease and
//!   never clobber a newer leader's key (a reordered refresh after a
//!   promotion would be a split-brain hazard otherwise).
//!
//! The manager exposes an `Arc<AtomicBool>` for the hot read path —
//! collector tasks check `is_leader()` per write without taking a
//! lock or making a Redis round-trip.

use redis::{AsyncCommands, RedisError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, info, warn};

pub const DEFAULT_LEASE_KEY: &str = "oi:lease:writer";

/// Refresh-or-die Lua script. Only renews the TTL if the key's value
/// still matches our node_id — otherwise the lease has already been
/// promoted elsewhere and we must NOT extend it.
const REFRESH_IF_OWNED: &str = r"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
";

#[derive(Debug, Error)]
pub enum FailoverError {
    #[error("redis: {0}")]
    Redis(#[from] RedisError),
}

#[derive(Clone, Debug)]
pub struct LeaseConfig {
    pub redis_url: String,
    pub node_id: String,
    pub key: String,
    /// How long a lease survives without refresh.
    pub ttl: Duration,
    /// How often the leader refreshes the TTL. Must be < `ttl`.
    pub refresh_interval: Duration,
}

impl LeaseConfig {
    pub fn new(redis_url: impl Into<String>) -> Self {
        Self {
            redis_url: redis_url.into(),
            node_id: uuid::Uuid::new_v4().to_string(),
            key: DEFAULT_LEASE_KEY.into(),
            ttl: Duration::from_secs(15),
            refresh_interval: Duration::from_secs(5),
        }
    }

    pub fn with_node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = id.into();
        self
    }
}

/// Handle returned to callers. Cheap to clone — the hot path is an
/// atomic load.
#[derive(Clone)]
pub struct LeaseManager {
    node_id: String,
    is_leader: Arc<AtomicBool>,
}

impl std::fmt::Debug for LeaseManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseManager")
            .field("node_id", &self.node_id)
            .field("is_leader", &self.is_leader.load(Ordering::Relaxed))
            .finish()
    }
}

impl LeaseManager {
    /// Returns true iff this node currently holds the lease.
    /// Atomic — safe to call from the hot write path per request.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

/// Spawn the lease supervisor. Returns a handle whose `is_leader()`
/// reflects the current state. The background task runs until the
/// handle is dropped (it doesn't actually check — it relies on the
/// runtime being shut down; collectors typically run forever).
pub async fn spawn_lease(cfg: LeaseConfig) -> Result<LeaseManager, FailoverError> {
    let client = redis::Client::open(cfg.redis_url.clone())?;
    let conn = redis::aio::ConnectionManager::new(client).await?;
    let is_leader = Arc::new(AtomicBool::new(false));
    let handle = LeaseManager {
        node_id: cfg.node_id.clone(),
        is_leader: is_leader.clone(),
    };
    tokio::spawn(run_loop(cfg, conn, is_leader));
    Ok(handle)
}

async fn run_loop(
    cfg: LeaseConfig,
    mut conn: redis::aio::ConnectionManager,
    is_leader: Arc<AtomicBool>,
) {
    let ttl_ms = cfg.ttl.as_millis() as i64;
    loop {
        let held = is_leader.load(Ordering::Acquire);
        match tick(&cfg, &mut conn, held, ttl_ms).await {
            Ok(new_state) => {
                if new_state != held {
                    if new_state {
                        info!(node=%cfg.node_id, key=%cfg.key, "lease acquired; promoting to leader");
                    } else {
                        warn!(node=%cfg.node_id, key=%cfg.key, "lease lost; demoting to follower");
                    }
                    is_leader.store(new_state, Ordering::Release);
                }
            }
            Err(e) => {
                // On Redis error, demote and retry. Better to be a
                // follower than to double-write.
                if held {
                    warn!(error=%e, "lease tick failed while leader; demoting");
                    is_leader.store(false, Ordering::Release);
                } else {
                    debug!(error=%e, "lease tick failed while follower");
                }
            }
        }
        tokio::time::sleep(cfg.refresh_interval).await;
    }
}

async fn tick(
    cfg: &LeaseConfig,
    conn: &mut redis::aio::ConnectionManager,
    currently_leader: bool,
    ttl_ms: i64,
) -> Result<bool, FailoverError> {
    if currently_leader {
        // Refresh-if-owned. Returns 1 if our TTL was extended, 0 if
        // the key had been stolen or expired.
        let refreshed: i64 = redis::Script::new(REFRESH_IF_OWNED)
            .key(cfg.key.clone())
            .arg(cfg.node_id.clone())
            .arg(ttl_ms)
            .invoke_async(conn)
            .await?;
        Ok(refreshed == 1)
    } else {
        // Try to acquire. `nx().px(ttl)` is atomic — no CAS race.
        let acquired: Option<String> = conn
            .set_options(
                cfg.key.as_str(),
                cfg.node_id.as_str(),
                redis::SetOptions::default()
                    .conditional_set(redis::ExistenceCheck::NX)
                    .with_expiration(redis::SetExpiry::PX(ttl_ms as u64)),
            )
            .await?;
        Ok(acquired.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_are_sane() {
        let cfg = LeaseConfig::new("redis://localhost");
        assert_eq!(cfg.key, "oi:lease:writer");
        assert_eq!(cfg.ttl, Duration::from_secs(15));
        assert!(cfg.refresh_interval < cfg.ttl);
        assert!(!cfg.node_id.is_empty());
    }

    #[test]
    fn custom_node_id_overrides_default() {
        let cfg = LeaseConfig::new("redis://localhost").with_node_id("node-A");
        assert_eq!(cfg.node_id, "node-A");
    }

    #[test]
    fn lease_manager_reports_initial_state_as_follower() {
        // Manual construction avoids the Redis connection — we're
        // testing the handle's purely-local behavior.
        let handle = LeaseManager {
            node_id: "test".into(),
            is_leader: Arc::new(AtomicBool::new(false)),
        };
        assert!(!handle.is_leader());
    }

    #[test]
    fn atomic_flip_is_visible_through_clones() {
        let flag = Arc::new(AtomicBool::new(false));
        let h1 = LeaseManager {
            node_id: "test".into(),
            is_leader: flag.clone(),
        };
        let h2 = h1.clone();
        flag.store(true, Ordering::Release);
        assert!(h1.is_leader());
        assert!(h2.is_leader());
    }
}
