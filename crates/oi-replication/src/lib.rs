//! Replication + backup. See `docs/replication.md`.
pub mod backup;
pub mod failover;

pub use failover::{spawn_lease, LeaseConfig, LeaseManager};
