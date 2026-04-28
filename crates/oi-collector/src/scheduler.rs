//! Minute-tick scheduler.
//!
//! Aligns wake-ups to UTC minute boundaries. Emits the `bucket_ts` for the
//! minute that just closed so adapters fetch an already-final minute rather
//! than the in-progress one. A small `offset` delay (default 2s) gives
//! exchanges time to settle their own aggregations.

use time::{Duration as TDuration, OffsetDateTime};
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, Copy)]
pub struct MinuteTick {
    /// The minute boundary we're firing for. Aligned to :00 of the minute.
    pub bucket_ts: OffsetDateTime,
}

/// Sleep until the next `:00` + `offset`, then return the bucket that
/// **just closed**.
pub async fn next_tick(offset: Duration) -> MinuteTick {
    let now = OffsetDateTime::now_utc();
    let this_minute = floor_minute(now);
    let next_minute = this_minute + TDuration::minutes(1);
    let wait_until = next_minute + TDuration::seconds(offset.as_secs() as i64);
    let wait = (wait_until - now).max(TDuration::ZERO);
    sleep(Duration::from_millis(
        wait.whole_milliseconds().max(0) as u64,
    ))
    .await;
    MinuteTick {
        // The minute that just closed at `next_minute`; its samples should be
        // bucketed into its start — i.e. `this_minute` the first time this
        // function fires, and `next_minute` on subsequent calls. Compute
        // fresh from the wall clock to avoid drift.
        bucket_ts: floor_minute(OffsetDateTime::now_utc() - TDuration::seconds(offset.as_secs() as i64)),
    }
}

fn floor_minute(ts: OffsetDateTime) -> OffsetDateTime {
    let sec = i64::from(ts.second());
    let nanos = ts.nanosecond();
    ts - TDuration::seconds(sec) - TDuration::nanoseconds(i64::from(nanos))
}
