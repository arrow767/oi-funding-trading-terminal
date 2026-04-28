//! File-backed Write-Ahead Log for snapshot batches.
//!
//! Every batch is persisted to `<dir>/<unix_ms>-<uuid>.mpk` (atomically,
//! via write-to-tmp + rename + fsync) **before** we attempt to write
//! to ClickHouse. On a successful CH write we delete the file; on
//! failure (CH unreachable, rate-limited, crash between attempts) the
//! file stays and a background drainer replays it.
//!
//! The design keeps steady-state overhead to one atomic file-create +
//! one delete per exchange per minute (~10 writes/minute total), which
//! is negligible on any modern disk. During a ClickHouse outage the
//! directory grows at ~10 files/minute; when CH comes back the drainer
//! catches up in seconds.
//!
//! Why not sled / rocksdb / sqlite: this is a simple MPSC queue where
//! the producer rarely contends (once per minute per exchange). A
//! plain directory is easier to inspect (`ls`, `cat`, `stat`) and has
//! no fsck / corruption concerns — each file is independent.

use oi_core::{
    error::{CoreError, Result},
    snapshot::OiSnapshot,
    traits::OiRepository,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, info, warn};

/// The filesystem suffix distinguishing pending from in-progress
/// writes. Drainer only picks up `*.mpk`; in-flight writes live as
/// dotfiles until the atomic rename.
const PENDING_SUFFIX: &str = ".mpk";

/// File-format magic. Bumped if the framing changes (e.g. compression,
/// alternate codec). Files starting with anything else are treated as
/// legacy raw msgpack — see `read()` for the fallback path.
const FRAME_MAGIC: &[u8; 4] = b"OIW1";

/// Frame layout (bytes):
///   0..4   magic (`OIW1`)
///   4..8   CRC32 of payload, big-endian
///   8..    payload (rmp-serde of `Vec<OiSnapshot>`)
const FRAME_HEADER_LEN: usize = 8;

/// File-backed durable queue. Cheap to clone — the underlying `dir`
/// is shared.
#[derive(Clone)]
pub struct FileWal {
    dir: PathBuf,
    /// Soft cap: when pending size exceeds this we log + emit a
    /// metric, but we don't refuse writes — the whole point of WAL
    /// is to absorb bursts.
    soft_max_files: usize,
}

impl std::fmt::Debug for FileWal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileWal")
            .field("dir", &self.dir)
            .field("soft_max_files", &self.soft_max_files)
            .finish()
    }
}

impl FileWal {
    pub async fn open(dir: PathBuf, soft_max_files: usize) -> Result<Self> {
        fs::create_dir_all(&dir)
            .await
            .map_err(|e| CoreError::Storage(format!("wal mkdir {dir:?}: {e}")))?;
        Ok(Self { dir, soft_max_files })
    }

    /// Serialize a batch and write it atomically. The returned path is
    /// the durable filename; `ack()` it after a successful downstream
    /// write.
    pub async fn append(&self, snaps: &[OiSnapshot]) -> Result<PathBuf> {
        let payload = rmp_serde::to_vec_named(&snaps.to_vec())
            .map_err(|e| CoreError::Storage(format!("wal msgpack: {e}")))?;
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| CoreError::Storage(format!("wal clock: {e}")))?
            .as_millis();
        let id = uuid_v4();
        let final_name = format!("{ts_ms}-{id}{PENDING_SUFFIX}");
        let final_path = self.dir.join(&final_name);
        // Write to a hidden tmp name first, fsync, then atomic rename.
        // The dot-prefix keeps `pending()` from seeing in-flight files.
        let tmp_path = self.dir.join(format!(".{final_name}.tmp"));

        // Frame the payload: magic + crc32(payload) + payload.
        let crc = crc32fast::hash(&payload);
        let total_bytes = FRAME_HEADER_LEN + payload.len();

        {
            let mut f = fs::File::create(&tmp_path)
                .await
                .map_err(|e| CoreError::Storage(format!("wal create {tmp_path:?}: {e}")))?;
            f.write_all(FRAME_MAGIC)
                .await
                .map_err(|e| CoreError::Storage(format!("wal write magic: {e}")))?;
            f.write_all(&crc.to_be_bytes())
                .await
                .map_err(|e| CoreError::Storage(format!("wal write crc: {e}")))?;
            f.write_all(&payload)
                .await
                .map_err(|e| CoreError::Storage(format!("wal write payload: {e}")))?;
            f.sync_all()
                .await
                .map_err(|e| CoreError::Storage(format!("wal fsync: {e}")))?;
        }

        fs::rename(&tmp_path, &final_path)
            .await
            .map_err(|e| CoreError::Storage(format!("wal rename: {e}")))?;

        metrics::counter!("oi_wal_write_bytes_total").increment(total_bytes as u64);
        metrics::counter!("oi_wal_writes_total").increment(1);
        self.refresh_metrics().await;
        if let Ok(count) = self.count_pending().await {
            if count > self.soft_max_files {
                warn!(
                    count,
                    cap = self.soft_max_files,
                    "WAL soft cap exceeded; downstream drain is lagging"
                );
            }
        }
        Ok(final_path)
    }

    /// Remove a drained file. Missing file is not an error (idempotent).
    pub async fn ack(&self, path: &Path) -> Result<()> {
        match fs::remove_file(path).await {
            Ok(()) => {
                metrics::counter!("oi_wal_acks_total").increment(1);
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CoreError::Storage(format!("wal ack {path:?}: {e}"))),
        }
    }

    /// List pending batches, oldest first.
    pub async fn pending(&self) -> Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        let mut rd = fs::read_dir(&self.dir)
            .await
            .map_err(|e| CoreError::Storage(format!("wal read_dir: {e}")))?;
        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(|e| CoreError::Storage(format!("wal read_dir next: {e}")))?
        {
            let name = entry.file_name();
            let Some(name_s) = name.to_str() else {
                continue;
            };
            // Skip dotfiles (in-flight writes) and anything missing our
            // suffix.
            if name_s.starts_with('.') || !name_s.ends_with(PENDING_SUFFIX) {
                continue;
            }
            out.push(entry.path());
        }
        // Filenames start with `<unix_ms>-` so lexicographic = chronological
        // for the first ~300 years.
        out.sort();
        Ok(out)
    }

    async fn count_pending(&self) -> Result<usize> {
        Ok(self.pending().await?.len())
    }

    /// Wall-clock age of the oldest pending file. Returns `None` when
    /// the queue is empty. Drives `oi_wal_oldest_pending_age_seconds`,
    /// which operators alert on when it exceeds their RTO budget — a
    /// rising value means the drainer is losing the race against new
    /// writes (CH outage) or has stopped (process bug).
    pub async fn oldest_pending_age(&self) -> Result<Option<Duration>> {
        let pending = self.pending().await?;
        Ok(pending
            .first()
            .and_then(|p| p.file_name().and_then(|n| n.to_str()).map(str::to_owned))
            .and_then(|name| parse_ts_ms(&name))
            .map(|ts_ms| {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(ts_ms);
                Duration::from_millis(now_ms.saturating_sub(ts_ms) as u64)
            }))
    }

    /// Refresh the WAL gauges (`oi_wal_pending_files`,
    /// `oi_wal_oldest_pending_age_seconds`). Call after every write
    /// and from the drainer's tick — between them, the gauge stays
    /// fresh under both bursty growth and an idle-but-non-empty
    /// queue.
    pub async fn refresh_metrics(&self) {
        let Ok(pending) = self.pending().await else {
            return;
        };
        metrics::gauge!("oi_wal_pending_files").set(pending.len() as f64);
        let age_secs = pending
            .first()
            .and_then(|p| p.file_name().and_then(|n| n.to_str()).map(str::to_owned))
            .and_then(|name| parse_ts_ms(&name))
            .map(|ts_ms| {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(ts_ms);
                (now_ms.saturating_sub(ts_ms) as f64) / 1000.0
            })
            .unwrap_or(0.0);
        metrics::gauge!("oi_wal_oldest_pending_age_seconds").set(age_secs);
    }

    /// Delete pending files whose wall-clock age exceeds `max_age`.
    /// Returns the number of files deleted.
    ///
    /// Used by the follower-side reaper to bound disk usage on a
    /// node that's been demoted but not re-promoted — its WAL grew
    /// in lockstep with the leader's during the last leadership era,
    /// but the local drainer is gated off, so without reaping the
    /// queue would grow forever. The trade-off: any minute older
    /// than `max_age` is gone if this node ever takes over leadership
    /// after that point. Pick `max_age` larger than your worst-case
    /// CH outage + planned-failover-window.
    pub async fn reap_older_than(&self, max_age: Duration) -> Result<usize> {
        let pending = self.pending().await?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let cutoff_ms = max_age.as_millis();
        let mut reaped = 0usize;
        for path in pending {
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            let Some(ts_ms) = parse_ts_ms(name) else {
                continue;
            };
            if now_ms.saturating_sub(ts_ms) <= cutoff_ms {
                // Files are sorted oldest-first, so once we hit one
                // young enough, all subsequent ones are also young.
                break;
            }
            match fs::remove_file(&path).await {
                Ok(()) => {
                    reaped += 1;
                    metrics::counter!("oi_wal_reaped_total").increment(1);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Raced with a concurrent ack — fine.
                }
                Err(e) => {
                    return Err(CoreError::Storage(format!(
                        "wal reap {path:?}: {e}"
                    )));
                }
            }
        }
        Ok(reaped)
    }

    /// Read and validate a pending file's serialized batch.
    ///
    /// Validates: magic header, CRC32 of payload, msgpack decode.
    /// Files written by the legacy unframed format (no `OIW1` magic)
    /// are accepted with a `tracing::warn` so an in-place upgrade
    /// drains the existing queue without manual migration. After the
    /// queue empties once, all subsequent files are framed.
    pub async fn read(&self, path: &Path) -> Result<Vec<OiSnapshot>> {
        let bytes = fs::read(path)
            .await
            .map_err(|e| CoreError::Storage(format!("wal read {path:?}: {e}")))?;

        if bytes.len() >= FRAME_HEADER_LEN && &bytes[..4] == FRAME_MAGIC {
            // Framed format. Bail loudly on CRC mismatch — silent
            // corruption (cosmic ray, FS bug, hardware) is exactly
            // why the magic + CRC are here.
            let stored_crc = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
            let payload = &bytes[FRAME_HEADER_LEN..];
            let actual_crc = crc32fast::hash(payload);
            if stored_crc != actual_crc {
                metrics::counter!("oi_wal_crc_mismatch_total").increment(1);
                return Err(CoreError::Storage(format!(
                    "wal crc mismatch {path:?}: stored={stored_crc:#x} actual={actual_crc:#x}"
                )));
            }
            return rmp_serde::from_slice::<Vec<OiSnapshot>>(payload)
                .map_err(|e| CoreError::Storage(format!("wal decode {path:?}: {e}")));
        }

        // Legacy: unframed raw msgpack. Surface visibly so operators
        // know the upgrade is mid-drain.
        warn!(
            path=%path.display(),
            "WAL file missing OIW1 magic; assuming legacy unframed format"
        );
        rmp_serde::from_slice::<Vec<OiSnapshot>>(&bytes)
            .map_err(|e| CoreError::Storage(format!("wal decode {path:?}: {e}")))
    }
}

/// Parse the `<unix_ms>-` prefix off a WAL filename. Returns `None`
/// for unrecognized shapes (e.g. files written by a future format
/// version).
fn parse_ts_ms(name: &str) -> Option<u128> {
    let dash = name.find('-')?;
    name[..dash].parse().ok()
}

/// Cheap random ID. Not cryptographically strong — we just need
/// collision-avoidance within the same millisecond for the same
/// process. Using `uuid` would pull in the full crate for one call.
fn uuid_v4() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{pid:x}-{n:x}")
}

/// Durable wrapper that writes through `FileWal` before delegating to
/// an inner `OiRepository`. On inner failure the WAL file stays; the
/// drainer (`spawn_drainer`) replays it on its own cadence.
pub struct WalBacked {
    inner: Arc<dyn OiRepository>,
    wal: FileWal,
}

impl std::fmt::Debug for WalBacked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalBacked").field("wal", &self.wal).finish()
    }
}

impl WalBacked {
    pub fn new(inner: Arc<dyn OiRepository>, wal: FileWal) -> Self {
        Self { inner, wal }
    }

    #[must_use]
    pub fn wal(&self) -> &FileWal {
        &self.wal
    }
}

#[async_trait::async_trait]
impl OiRepository for WalBacked {
    async fn upsert_snapshots(&self, snaps: &[OiSnapshot]) -> Result<()> {
        if snaps.is_empty() {
            return Ok(());
        }
        // WAL first: if we crash between append and upsert, the
        // drainer picks up on next boot.
        let path = self.wal.append(snaps).await?;
        match self.inner.upsert_snapshots(snaps).await {
            Ok(()) => self.wal.ack(&path).await,
            Err(e) => {
                // Intentional: file stays. Drainer replays.
                warn!(
                    error=%e,
                    path=%path.display(),
                    "inner upsert failed; snapshot queued in WAL for retry"
                );
                Err(e)
            }
        }
    }

    async fn upsert_instruments(
        &self,
        metas: &[oi_core::instrument::InstrumentMeta],
    ) -> Result<()> {
        // Metadata isn't worth WAL'ing — it's rediscovered on every
        // 6h cycle. Pass through directly.
        self.inner.upsert_instruments(metas).await
    }

    async fn range(
        &self,
        instrument: &oi_core::instrument::InstrumentId,
        from: time::OffsetDateTime,
        to: time::OffsetDateTime,
    ) -> Result<Vec<OiSnapshot>> {
        self.inner.range(instrument, from, to).await
    }

    async fn latest(
        &self,
        instrument: &oi_core::instrument::InstrumentId,
    ) -> Result<Option<OiSnapshot>> {
        self.inner.latest(instrument).await
    }
}

/// Spawn the background drainer. Every `interval` it walks pending
/// files oldest-first, replays each to `target`, and on success acks
/// it. A single failure aborts the cycle — we retry next tick rather
/// than thrashing a known-broken downstream.
pub fn spawn_drainer(
    wal: FileWal,
    target: Arc<dyn OiRepository>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    spawn_drainer_with_gate(wal, target, interval, None, None)
}

/// Drainer variant with an optional leader gate and follower
/// max-age reaper.
///
/// * `gate` — when `Some` and returns `false`, this cycle skips the
///   drain. The age gauge still ticks via `refresh_metrics`, so
///   alerts fire on a stuck follower queue.
/// * `follower_max_age` — when `Some` AND `gate` reports follower,
///   files older than this are deleted to bound disk usage on a
///   long-running standby. `None` keeps everything (the original
///   behavior — useful when the node will eventually be re-promoted
///   and you want zero data loss). Recommended: a value larger than
///   your worst-case CH outage + planned-failover window.
///
/// On a follower → leader transition the next tick (≤ `interval`)
/// kicks in immediately and replays the whole local backlog.
pub fn spawn_drainer_with_gate(
    wal: FileWal,
    target: Arc<dyn OiRepository>,
    interval: Duration,
    gate: Option<crate::leader_gated::LeaderGate>,
    follower_max_age: Option<Duration>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            dir=?wal.dir,
            gated = gate.is_some(),
            reaper_max_age_secs = follower_max_age.map(|d| d.as_secs()),
            "WAL drainer starting"
        );
        loop {
            tokio::time::sleep(interval).await;
            wal.refresh_metrics().await;
            let may_drain = gate.as_ref().map_or(true, |g| g());
            if !may_drain {
                // Follower path: drainer doesn't drain (only the
                // leader does), but it can reap files older than
                // `follower_max_age` if configured, so disk usage
                // stays bounded on a long-running standby.
                if let Some(max_age) = follower_max_age {
                    match wal.reap_older_than(max_age).await {
                        Ok(0) => {}
                        Ok(n) => warn!(
                            reaped = n,
                            max_age_secs = max_age.as_secs(),
                            "WAL reaper deleted aged files on follower"
                        ),
                        Err(e) => error!(error=%e, "WAL reap cycle failed"),
                    }
                }
                continue;
            }
            if let Err(e) = drain_once(&wal, &target).await {
                error!(error=%e, "WAL drain cycle failed");
            }
            wal.refresh_metrics().await;
        }
    })
}

async fn drain_once(wal: &FileWal, target: &Arc<dyn OiRepository>) -> Result<()> {
    let pending = wal.pending().await?;
    if pending.is_empty() {
        return Ok(());
    }
    debug!(count = pending.len(), "WAL drain cycle start");
    let mut drained = 0u64;
    for path in pending {
        let snaps = match wal.read(&path).await {
            Ok(s) => s,
            Err(e) => {
                // Corrupt file — rename it aside so subsequent drains
                // don't retry forever and a human can inspect it.
                error!(error=%e, path=%path.display(), "WAL read failed; quarantining file");
                quarantine(&path).await;
                continue;
            }
        };
        match target.upsert_snapshots(&snaps).await {
            Ok(()) => {
                wal.ack(&path).await?;
                drained += 1;
            }
            Err(e) => {
                warn!(error=%e, path=%path.display(), "WAL drain entry failed; stopping cycle");
                // Abort the cycle rather than spamming a broken CH.
                // The next interval retries from the oldest file.
                return Err(e);
            }
        }
    }
    if drained > 0 {
        metrics::counter!("oi_wal_drained_total").increment(drained);
        info!(drained, "WAL drain cycle complete");
    }
    Ok(())
}

async fn quarantine(path: &Path) {
    let quarantined = path.with_extension("corrupt");
    if let Err(e) = tokio::fs::rename(path, &quarantined).await {
        error!(error=%e, path=%path.display(), "WAL quarantine failed; leaving file in place");
    } else {
        metrics::counter!("oi_wal_quarantined_total").increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oi_core::{
        exchange::Exchange, instrument::InstrumentId, snapshot::OiSnapshot, unit::UnitKind,
    };
    use rust_decimal_macros::dec;

    fn tmpdir() -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("oi-wal-test-{}-{}", std::process::id(), uuid_v4()));
        p
    }

    fn sample_snap() -> OiSnapshot {
        // Degenerate one-sample bar: O=H=L=C. WAL doesn't care about
        // OHLC structure — it serialises whatever shape the type
        // currently has — so this fixture only needs to produce a
        // valid `OiSnapshot`.
        let recv = time::macros::datetime!(2026-04-24 10:00:02 UTC);
        OiSnapshot {
            instrument: InstrumentId::new(Exchange::Binance, "BTCUSDT".to_owned()),
            bucket_ts: time::macros::datetime!(2026-04-24 10:00:00 UTC),
            first_recv_ts: recv,
            last_recv_ts: recv,
            samples: 1,
            native_unit: UnitKind::Coins,
            native_open: dec!(100),
            native_high: dec!(100),
            native_low: dec!(100),
            native_close: dec!(100),
            oi_coins_open: Some(dec!(100)),
            oi_coins_high: Some(dec!(100)),
            oi_coins_low: Some(dec!(100)),
            oi_coins_close: Some(dec!(100)),
            oi_usd_open: Some(dec!(6_400_000)),
            oi_usd_high: Some(dec!(6_400_000)),
            oi_usd_low: Some(dec!(6_400_000)),
            oi_usd_close: Some(dec!(6_400_000)),
            price_used_close: Some(dec!(64_000)),
        }
    }

    #[tokio::test]
    async fn append_read_ack_roundtrip() {
        let dir = tmpdir();
        let wal = FileWal::open(dir.clone(), 1000).await.unwrap();
        let snap = sample_snap();

        let path = wal.append(&[snap.clone()]).await.unwrap();
        let pending = wal.pending().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], path);

        let replay = wal.read(&path).await.unwrap();
        assert_eq!(replay, vec![snap]);

        wal.ack(&path).await.unwrap();
        assert!(wal.pending().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn ack_of_missing_file_is_idempotent() {
        let dir = tmpdir();
        let wal = FileWal::open(dir.clone(), 1000).await.unwrap();
        let path = wal.append(&[sample_snap()]).await.unwrap();
        wal.ack(&path).await.unwrap();
        // Double-ack should not error.
        wal.ack(&path).await.unwrap();
    }

    #[tokio::test]
    async fn pending_is_sorted_chronologically() {
        let dir = tmpdir();
        let wal = FileWal::open(dir.clone(), 1000).await.unwrap();
        let a = wal.append(&[sample_snap()]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let b = wal.append(&[sample_snap()]).await.unwrap();
        let list = wal.pending().await.unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0], a);
        assert_eq!(list[1], b);
    }

    #[tokio::test]
    async fn append_writes_oiw1_framed_file() {
        let dir = tmpdir();
        let wal = FileWal::open(dir, 1000).await.unwrap();
        let path = wal.append(&[sample_snap()]).await.unwrap();
        let raw = std::fs::read(&path).unwrap();
        assert!(raw.len() > FRAME_HEADER_LEN);
        assert_eq!(&raw[..4], FRAME_MAGIC);
        // CRC must validate against the payload.
        let stored_crc = u32::from_be_bytes(raw[4..8].try_into().unwrap());
        assert_eq!(stored_crc, crc32fast::hash(&raw[FRAME_HEADER_LEN..]));
    }

    #[tokio::test]
    async fn read_rejects_corrupted_payload() {
        let dir = tmpdir();
        let wal = FileWal::open(dir, 1000).await.unwrap();
        let path = wal.append(&[sample_snap()]).await.unwrap();

        // Flip a byte in the payload — CRC must catch it.
        let mut raw = std::fs::read(&path).unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 0xff;
        std::fs::write(&path, &raw).unwrap();

        let err = wal.read(&path).await.unwrap_err();
        assert!(
            err.to_string().contains("crc mismatch"),
            "expected crc mismatch error, got: {err}"
        );
    }

    #[tokio::test]
    async fn read_accepts_legacy_unframed_msgpack() {
        let dir = tmpdir();
        let wal = FileWal::open(dir.clone(), 1000).await.unwrap();
        // Hand-craft a legacy file: raw msgpack, no magic.
        let snaps = vec![sample_snap()];
        let payload = rmp_serde::to_vec_named(&snaps).unwrap();
        let legacy_path = dir.join("0-deadbeef-1.mpk");
        std::fs::write(&legacy_path, &payload).unwrap();

        let read_back = wal.read(&legacy_path).await.unwrap();
        assert_eq!(read_back, snaps);
    }

    #[tokio::test]
    async fn reap_deletes_files_older_than_max_age() {
        let dir = tmpdir();
        let wal = FileWal::open(dir.clone(), 1000).await.unwrap();

        // Write one file with a real append (current ts).
        let recent = wal.append(&[sample_snap()]).await.unwrap();
        // And one synthetic file with a backdated ts in the name.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let old_ts_ms = now_ms - 10 * 60 * 1000; // 10 minutes old
        let old_path = dir.join(format!("{old_ts_ms}-deadbeef-1.mpk"));
        // Has to be a valid framed file so quarantine doesn't trip
        // on subsequent reads (not strictly tested here, but it's
        // future-proof).
        let payload = rmp_serde::to_vec_named(&vec![sample_snap()]).unwrap();
        let crc = crc32fast::hash(&payload);
        let mut framed = Vec::new();
        framed.extend_from_slice(b"OIW1");
        framed.extend_from_slice(&crc.to_be_bytes());
        framed.extend_from_slice(&payload);
        std::fs::write(&old_path, &framed).unwrap();

        // Reap with a 5-minute cutoff: old file goes, recent stays.
        let n = wal
            .reap_older_than(std::time::Duration::from_secs(5 * 60))
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert!(!old_path.exists());
        assert!(recent.exists());
    }

    #[tokio::test]
    async fn reap_is_a_noop_when_queue_is_empty_or_young() {
        let dir = tmpdir();
        let wal = FileWal::open(dir, 1000).await.unwrap();
        // Empty queue.
        assert_eq!(
            wal.reap_older_than(std::time::Duration::from_secs(60))
                .await
                .unwrap(),
            0
        );
        // Fresh file, generous max_age.
        wal.append(&[sample_snap()]).await.unwrap();
        assert_eq!(
            wal.reap_older_than(std::time::Duration::from_secs(3600))
                .await
                .unwrap(),
            0
        );
    }

    #[test]
    fn parse_ts_ms_handles_real_filenames_and_garbage() {
        // Real shape: "<unix_ms>-<pid_hex>-<ctr_hex>.mpk"
        assert_eq!(
            parse_ts_ms("1714000000000-2a3f-0.mpk"),
            Some(1_714_000_000_000)
        );
        // Pad-zero ts is fine.
        assert_eq!(parse_ts_ms("0-deadbeef-1.mpk"), Some(0));
        // No dash → unparseable.
        assert!(parse_ts_ms("nodashhere.mpk").is_none());
        // Non-numeric prefix → None (don't crash).
        assert!(parse_ts_ms("notanumber-x-y.mpk").is_none());
    }

    #[tokio::test]
    async fn oldest_pending_age_is_none_when_empty_else_monotone() {
        let dir = tmpdir();
        let wal = FileWal::open(dir, 1000).await.unwrap();
        assert!(wal.oldest_pending_age().await.unwrap().is_none());

        let _path = wal.append(&[sample_snap()]).await.unwrap();
        let a1 = wal.oldest_pending_age().await.unwrap().unwrap();
        // Sleep a beat — age should not go backwards.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let a2 = wal.oldest_pending_age().await.unwrap().unwrap();
        assert!(
            a2 >= a1,
            "age must be monotonic: a1={a1:?} a2={a2:?}"
        );
    }

    #[tokio::test]
    async fn oldest_pending_age_tracks_oldest_not_newest() {
        let dir = tmpdir();
        let wal = FileWal::open(dir, 1000).await.unwrap();
        let oldest = wal.append(&[sample_snap()]).await.unwrap();
        // Make a measurable gap between the two files.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _newest = wal.append(&[sample_snap()]).await.unwrap();
        let age = wal.oldest_pending_age().await.unwrap().unwrap();
        assert!(
            age >= std::time::Duration::from_millis(50),
            "age should reflect the FIRST file, got {age:?}"
        );
        wal.ack(&oldest).await.unwrap();
        // After ack, the newest becomes the oldest — age should drop.
        let age_after = wal.oldest_pending_age().await.unwrap().unwrap();
        assert!(
            age_after < age,
            "age should drop after acking the oldest; before={age:?} after={age_after:?}"
        );
    }

    #[tokio::test]
    async fn drainer_acks_successful_inner_write() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let dir = tmpdir();
        let wal = FileWal::open(dir.clone(), 1000).await.unwrap();
        let _a = wal.append(&[sample_snap()]).await.unwrap();
        let _b = wal.append(&[sample_snap(), sample_snap()]).await.unwrap();
        assert_eq!(wal.pending().await.unwrap().len(), 2);

        struct Counting {
            calls: AtomicU64,
            rows: AtomicU64,
        }
        #[async_trait::async_trait]
        impl OiRepository for Counting {
            async fn upsert_snapshots(&self, snaps: &[OiSnapshot]) -> Result<()> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.rows
                    .fetch_add(snaps.len() as u64, Ordering::SeqCst);
                Ok(())
            }
            async fn upsert_instruments(
                &self,
                _: &[oi_core::instrument::InstrumentMeta],
            ) -> Result<()> {
                Ok(())
            }
            async fn range(
                &self,
                _: &oi_core::instrument::InstrumentId,
                _: time::OffsetDateTime,
                _: time::OffsetDateTime,
            ) -> Result<Vec<OiSnapshot>> {
                Ok(vec![])
            }
            async fn latest(
                &self,
                _: &oi_core::instrument::InstrumentId,
            ) -> Result<Option<OiSnapshot>> {
                Ok(None)
            }
        }
        let inner: Arc<dyn OiRepository> = Arc::new(Counting {
            calls: AtomicU64::new(0),
            rows: AtomicU64::new(0),
        });
        drain_once(&wal, &inner).await.unwrap();
        assert!(wal.pending().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn drainer_stops_on_first_failure_and_leaves_files() {
        let dir = tmpdir();
        let wal = FileWal::open(dir.clone(), 1000).await.unwrap();
        wal.append(&[sample_snap()]).await.unwrap();
        wal.append(&[sample_snap()]).await.unwrap();

        struct Failing;
        #[async_trait::async_trait]
        impl OiRepository for Failing {
            async fn upsert_snapshots(&self, _: &[OiSnapshot]) -> Result<()> {
                Err(CoreError::Storage("synthetic downstream failure".into()))
            }
            async fn upsert_instruments(
                &self,
                _: &[oi_core::instrument::InstrumentMeta],
            ) -> Result<()> {
                Ok(())
            }
            async fn range(
                &self,
                _: &oi_core::instrument::InstrumentId,
                _: time::OffsetDateTime,
                _: time::OffsetDateTime,
            ) -> Result<Vec<OiSnapshot>> {
                Ok(vec![])
            }
            async fn latest(
                &self,
                _: &oi_core::instrument::InstrumentId,
            ) -> Result<Option<OiSnapshot>> {
                Ok(None)
            }
        }
        let inner: Arc<dyn OiRepository> = Arc::new(Failing);
        let err = drain_once(&wal, &inner).await.unwrap_err();
        assert!(err.to_string().contains("synthetic"));
        // Both files remain queued for the next cycle.
        assert_eq!(wal.pending().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn empty_batch_is_a_noop() {
        use crate::composite::CompositeRepository;
        use crate::clickhouse::ClickHouseRepo;
        use crate::redis::RedisCache;
        // We don't need a real repo for this test — WalBacked's
        // upsert_snapshots short-circuits before touching anything.
        let dir = tmpdir();
        let wal = FileWal::open(dir, 1000).await.unwrap();
        // Use a dummy inner whose methods are never invoked.
        struct Noop;
        #[async_trait::async_trait]
        impl OiRepository for Noop {
            async fn upsert_snapshots(&self, _: &[OiSnapshot]) -> Result<()> {
                panic!("should not be called for empty batch");
            }
            async fn upsert_instruments(&self, _: &[oi_core::instrument::InstrumentMeta]) -> Result<()> {
                unreachable!()
            }
            async fn range(&self, _: &oi_core::instrument::InstrumentId, _: time::OffsetDateTime, _: time::OffsetDateTime) -> Result<Vec<OiSnapshot>> {
                unreachable!()
            }
            async fn latest(&self, _: &oi_core::instrument::InstrumentId) -> Result<Option<OiSnapshot>> {
                unreachable!()
            }
        }
        let backed = WalBacked::new(Arc::new(Noop), wal);
        backed.upsert_snapshots(&[]).await.unwrap();
        // Unused imports only to prove the WalBacked type composes
        // with the rest of the layer without circularity.
        let _ = std::any::TypeId::of::<CompositeRepository>();
        let _ = std::any::TypeId::of::<ClickHouseRepo>();
        let _ = std::any::TypeId::of::<RedisCache>();
    }
}
