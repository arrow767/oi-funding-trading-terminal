//! Storage layer: ClickHouse (primary) + Redis (hot cache).
//!
//! The `CompositeRepository` implements [`oi_core::OiRepository`] by writing
//! to both. Reads fall back gracefully: latest → Redis first, then ClickHouse.

pub mod clickhouse;
pub mod composite;
pub mod leader_gated;
pub mod pubsub;
pub mod redis;
pub mod wal;

pub use composite::CompositeRepository;
pub use leader_gated::{LeaderGate, LeaderGatedRepo};
pub use pubsub::{SnapshotPublisher, FIREHOSE_CHANNEL};
pub use wal::{spawn_drainer, spawn_drainer_with_gate, FileWal, WalBacked};
