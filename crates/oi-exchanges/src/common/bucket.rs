//! Minute-bucket alignment.
//!
//! The collector ticks at minute boundaries and passes the `bucket_ts` to
//! each adapter. Adapters must NOT invent their own bucket — using the
//! collector's value keeps samples across exchanges comparable.

use time::{Duration, OffsetDateTime};

/// Truncate a timestamp to the start of its UTC minute.
#[must_use]
pub fn floor_to_minute(ts: OffsetDateTime) -> OffsetDateTime {
    let seconds = i64::from(ts.second());
    let nanos = ts.nanosecond();
    ts - Duration::seconds(seconds) - Duration::nanoseconds(i64::from(nanos))
}

/// Next minute tick that is strictly after `now`.
#[must_use]
pub fn next_minute_after(now: OffsetDateTime) -> OffsetDateTime {
    floor_to_minute(now) + Duration::minutes(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn floor_zeroes_seconds_and_nanos() {
        let t = datetime!(2026-04-24 10:15:37.123_456 UTC);
        assert_eq!(floor_to_minute(t), datetime!(2026-04-24 10:15:00 UTC));
    }

    #[test]
    fn floor_is_idempotent() {
        let t = datetime!(2026-04-24 10:15:00 UTC);
        assert_eq!(floor_to_minute(floor_to_minute(t)), t);
    }

    #[test]
    fn next_minute_is_strictly_after() {
        let t = datetime!(2026-04-24 10:15:00 UTC);
        assert_eq!(next_minute_after(t), datetime!(2026-04-24 10:16:00 UTC));
        let t2 = datetime!(2026-04-24 10:15:59.999 UTC);
        assert_eq!(next_minute_after(t2), datetime!(2026-04-24 10:16:00 UTC));
    }
}
