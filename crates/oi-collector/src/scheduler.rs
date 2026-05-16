//! Minute-tick scheduler.
//!
//! Aligns wake-ups to UTC minute boundaries. Emits the `bucket_ts` for the
//! minute that just closed so adapters fetch an already-final minute rather
//! than the in-progress one. A small `offset` delay (default 2s) gives
//! exchanges time to settle their own aggregations.
//!
//! ## Why this is stateful (the missed-tick fix)
//!
//! The previous implementation was a stateless `next_tick()` that, on every
//! call, recomputed the target bucket from the *current* wall clock
//! (`floor_minute(now - offset)`). The collector loop is serial:
//! `next_tick → fetch_prices → fetch_oi → flush → upsert → loop`. When one
//! iteration's work overran the minute boundary (Binance REST for ~570
//! symbols occasionally takes >60 s), the next `next_tick()` call observed a
//! wall clock already in a later minute and **silently skipped every minute
//! that elapsed during the overrun** — that minute's OI was never fetched and
//! is unrecoverable (exchange REST returns only *current* OI, not history).
//! Measured impact: ~28 % of minutes lost, globally, all exchanges.
//!
//! `MinuteScheduler` instead tracks `last_bucket` and always advances by
//! exactly one minute. If the loop is behind (previous iteration overran),
//! `next()` returns the next bucket **immediately** instead of skipping it —
//! the minute is processed late rather than dropped, and the schedule
//! self-heals (catches back up to the aligned `:00+offset` cadence as soon
//! as work is fast enough again).
//!
//! Note: this fixes *transient* overruns (occasional slow minutes), which is
//! the bulk of the observed loss. A *chronic* per-exchange fetch that always
//! exceeds 60 s is a capacity problem no scheduler can paper over — the lag
//! would grow unbounded and the OI label would drift from reality. Such a
//! case is surfaced via a `WARN` on every late tick so it is observable; the
//! real remedy there is faster (concurrent) `fetch_oi` in the adapter.

use time::{Duration as TDuration, OffsetDateTime};
use tokio::time::{sleep, Duration};
use tracing::warn;

#[derive(Debug, Clone, Copy)]
pub struct MinuteTick {
    /// The minute boundary we're firing for. Aligned to :00 of the minute.
    pub bucket_ts: OffsetDateTime,
    /// True when this tick fired without sleeping because the previous
    /// loop iteration overran its minute — i.e. we are behind real time
    /// and processing this bucket late rather than skipping it.
    pub late: bool,
}

/// Stateful minute scheduler. One instance per collector loop; not shared.
#[derive(Debug)]
pub struct MinuteScheduler {
    offset: TDuration,
    last_bucket: Option<OffsetDateTime>,
}

impl MinuteScheduler {
    #[must_use]
    pub fn new(offset: Duration) -> Self {
        Self {
            offset: TDuration::seconds(offset.as_secs() as i64),
            last_bucket: None,
        }
    }

    /// Return the next bucket to process — always exactly one minute after
    /// the previous one (no skips, no duplicates). Sleeps until that
    /// bucket's data is due (`bucket_start + 1min + offset`); if we're
    /// already past that deadline (the previous iteration overran), returns
    /// immediately so the minute is processed late instead of dropped.
    pub async fn next(&mut self) -> MinuteTick {
        let now = OffsetDateTime::now_utc();
        let (target, wait) = plan(self.last_bucket, now, self.offset);
        let late = wait <= TDuration::ZERO && self.last_bucket.is_some();
        if wait > TDuration::ZERO {
            sleep(Duration::from_millis(wait.whole_milliseconds().max(0) as u64)).await;
        } else if late {
            let behind = now - (target + TDuration::minutes(1) + self.offset);
            warn!(
                target_bucket = %target,
                behind_secs = behind.whole_seconds(),
                "scheduler: loop overran the minute; processing bucket late (not skipped). \
                 Chronic lag here means fetch_oi is too slow — needs concurrent fetch."
            );
        }
        self.last_bucket = Some(target);
        MinuteTick {
            bucket_ts: target,
            late,
        }
    }
}

/// Pure scheduling decision, factored out for deterministic testing.
///
/// Given the last bucket we processed (or `None` on first call), the
/// current time, and the settle `offset`, returns the next bucket to
/// process and how long to sleep before its data is due. A non-positive
/// wait means "process now, we're behind".
fn plan(
    last_bucket: Option<OffsetDateTime>,
    now: OffsetDateTime,
    offset: TDuration,
) -> (OffsetDateTime, TDuration) {
    let target = match last_bucket {
        // Always exactly one minute on from the previous bucket — this is
        // the no-skip invariant.
        Some(b) => b + TDuration::minutes(1),
        // First call: the minute that has already fully closed.
        None => floor_minute(now) - TDuration::minutes(1),
    };
    // The minute starting at `target` closes at `target + 1min`; its OI is
    // fetchable from `target + 1min + offset` onward.
    let deadline = target + TDuration::minutes(1) + offset;
    let wait = deadline - now;
    (target, wait)
}

fn floor_minute(ts: OffsetDateTime) -> OffsetDateTime {
    let sec = i64::from(ts.second());
    let nanos = ts.nanosecond();
    ts - TDuration::seconds(sec) - TDuration::nanoseconds(i64::from(nanos))
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    const OFF: TDuration = TDuration::seconds(2);

    #[test]
    fn first_call_targets_the_already_closed_minute() {
        // now = 10:00:30 → the minute 09:59:00 has fully closed; its
        // deadline (09:59 + 1min + 2s = 10:00:02) is already in the past,
        // so we process it immediately at startup.
        let now = datetime!(2026-05-16 10:00:30 UTC);
        let (target, wait) = plan(None, now, OFF);
        assert_eq!(target, datetime!(2026-05-16 09:59:00 UTC));
        assert!(wait <= TDuration::ZERO, "startup should not block a minute");
    }

    #[test]
    fn steady_state_sleeps_to_next_aligned_deadline() {
        // Processed 10:00:00; fast work, now 10:01:05. Next bucket is
        // 10:01:00, due at 10:02:02 → sleep ~57s.
        let last = Some(datetime!(2026-05-16 10:00:00 UTC));
        let now = datetime!(2026-05-16 10:01:05 UTC);
        let (target, wait) = plan(last, now, OFF);
        assert_eq!(target, datetime!(2026-05-16 10:01:00 UTC));
        assert_eq!(wait, TDuration::seconds(57));
    }

    #[test]
    fn overrun_processes_the_next_minute_immediately_not_skipped() {
        // Regression test for the data-loss bug. We processed 10:00:00,
        // but the work overran badly and it's now 10:02:40. The OLD
        // stateless scheduler would have jumped to bucket 10:02 and
        // SKIPPED 10:01 forever. The new one must return 10:01 with no
        // wait so the minute is processed (late), never dropped.
        let last = Some(datetime!(2026-05-16 10:00:00 UTC));
        let now = datetime!(2026-05-16 10:02:40 UTC);
        let (target, wait) = plan(last, now, OFF);
        assert_eq!(target, datetime!(2026-05-16 10:01:00 UTC));
        assert!(wait <= TDuration::ZERO, "behind schedule → process now");
    }

    #[test]
    fn no_skip_invariant_under_sustained_overrun() {
        // Simulate a loop that is always ~26s/min too slow: every
        // iteration `now` advances 86s while buckets advance 60s. Every
        // bucket must still appear exactly once, in order, no gaps.
        let mut last: Option<OffsetDateTime> = Some(datetime!(2026-05-16 10:00:00 UTC));
        let mut now = datetime!(2026-05-16 10:01:28 UTC);
        let mut produced = Vec::new();
        for _ in 0..10 {
            let (target, _wait) = plan(last, now, OFF);
            produced.push(target);
            last = Some(target);
            now += TDuration::seconds(86);
        }
        for w in produced.windows(2) {
            assert_eq!(
                w[1] - w[0],
                TDuration::minutes(1),
                "buckets must advance by exactly one minute with no skips"
            );
        }
        assert_eq!(produced.first(), Some(&datetime!(2026-05-16 10:01:00 UTC)));
        assert_eq!(produced.last(), Some(&datetime!(2026-05-16 10:10:00 UTC)));
    }

    #[test]
    fn self_heals_back_to_aligned_cadence_after_a_spike() {
        // One slow minute, then fast again → schedule realigns: the wait
        // becomes positive (we sleep to the aligned deadline) once the
        // loop is no longer behind.
        let last = Some(datetime!(2026-05-16 10:00:00 UTC));
        // Spike: behind at 10:02:40 → process 10:01 immediately.
        let (t1, w1) = plan(last, datetime!(2026-05-16 10:02:40 UTC), OFF);
        assert_eq!(t1, datetime!(2026-05-16 10:01:00 UTC));
        assert!(w1 <= TDuration::ZERO);
        // Work was fast this time; now 10:02:45. Next bucket 10:02:00 due
        // at 10:03:02 → positive wait, back on the aligned cadence.
        let (t2, w2) = plan(Some(t1), datetime!(2026-05-16 10:02:45 UTC), OFF);
        assert_eq!(t2, datetime!(2026-05-16 10:02:00 UTC));
        assert_eq!(w2, TDuration::seconds(17));
    }
}
