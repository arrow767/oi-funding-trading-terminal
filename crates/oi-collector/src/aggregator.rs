//! Per-instrument intra-minute OHLC aggregator.
//!
//! Sits between the producer side (REST polling, WS live frames) and
//! the durable write path. `observe(sample)` folds the sample into
//! the bar for `(instrument, bucket_ts)`. `flush_minute(bucket)`
//! emits and removes all bars whose bucket is `<= bucket`, returning
//! them as an iterator-friendly Vec ready for `repo.upsert_snapshots`.
//!
//! Live-stream callers can also peek at the in-progress bar via
//! `current(instrument)` to publish "partial" frames that
//! TradingView-style charts redraw in real time.

use dashmap::DashMap;
use oi_core::{
    instrument::InstrumentId,
    snapshot::{OiSample, OiSnapshot},
};
use time::OffsetDateTime;
use tracing::warn;

#[derive(Debug, Default)]
pub struct OiAggregator {
    bars: DashMap<InstrumentId, OiSnapshot>,
}

impl OiAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold a sample into the running bar for its instrument.
    ///
    /// If no bar exists for `(instrument, bucket_ts)`, one is
    /// created from this sample (open=high=low=close=value). If a
    /// bar exists for the same instrument but an earlier bucket, it
    /// is REPLACED — the older bucket should have been flushed by
    /// the minute scheduler; reaching this branch is either a
    /// scheduler miss or a sample arriving with a stale `bucket_ts`.
    /// We log and replace to avoid eating new data silently.
    ///
    /// Returns a clone of the current in-progress bar (post-fold) so
    /// the caller can publish a partial-bar frame.
    pub fn observe(&self, sample: OiSample) -> OiSnapshot {
        let instrument = sample.instrument.clone();
        let bucket_ts = sample.bucket_ts;
        let mut entry = self.bars.entry(instrument).or_insert_with(|| {
            OiSnapshot::start_from_sample(sample.clone())
        });

        // The `or_insert_with` path used a clone we still own (the
        // closure consumed our copy). When the entry was vacant the
        // bar is now correct and matches `sample`; nothing to do.
        // When occupied we have to merge — but only if the bucket
        // matches. Mismatch falls through to "replace".
        if entry.value().bucket_ts == bucket_ts {
            // If the entry was newly inserted, samples == 1 and
            // observing again would double-count. Skip when the
            // current bar is already the sample we just tried to
            // insert (entry-from-vacant case).
            if entry.value().samples == 1 && entry.value().last_recv_ts == sample.recv_ts {
                return entry.value().clone();
            }
            if let Err(e) = entry.value_mut().observe(&sample) {
                warn!(error=%e, "aggregator: observe failed; sample dropped");
            }
            return entry.value().clone();
        }

        // Bucket mismatch: scheduler should have flushed the old
        // bar already. Replace with a fresh one from this sample.
        warn!(
            instrument=%entry.key(),
            old_bucket=%entry.value().bucket_ts,
            new_bucket=%bucket_ts,
            "aggregator: bucket roll outside flush_minute; replacing bar (one minute missed)"
        );
        *entry.value_mut() = OiSnapshot::start_from_sample(sample);
        entry.value().clone()
    }

    /// Snapshot the current in-progress bar for an instrument
    /// without mutating state. Returns `None` if nothing's been
    /// observed.
    #[must_use]
    pub fn current(&self, instrument: &InstrumentId) -> Option<OiSnapshot> {
        self.bars.get(instrument).map(|r| r.value().clone())
    }

    /// Drain and return all bars whose bucket is at or before
    /// `up_to`. Bars for newer buckets stay (those are bars the
    /// scheduler hasn't yet asked to flush). The order of the
    /// returned Vec is unspecified.
    pub fn flush_through(&self, up_to: OffsetDateTime) -> Vec<OiSnapshot> {
        let mut out = Vec::new();
        // Two-step: collect keys, then remove. DashMap doesn't
        // expose a "drain matching" iterator that's safe with
        // concurrent observers.
        let to_flush: Vec<InstrumentId> = self
            .bars
            .iter()
            .filter(|r| r.value().bucket_ts <= up_to)
            .map(|r| r.key().clone())
            .collect();
        for k in to_flush {
            if let Some((_, bar)) = self.bars.remove(&k) {
                out.push(bar);
            }
        }
        out
    }

    /// Drain and return ALL bars regardless of bucket. Used at
    /// shutdown so in-progress bars get persisted even if they're
    /// for the still-open current minute.
    pub fn flush_all(&self) -> Vec<OiSnapshot> {
        let keys: Vec<InstrumentId> = self.bars.iter().map(|r| r.key().clone()).collect();
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some((_, bar)) = self.bars.remove(&k) {
                out.push(bar);
            }
        }
        out
    }

    /// Number of in-progress bars. Cheap; useful as a metric.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bars.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bars.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oi_core::{exchange::Exchange, unit::UnitKind};
    use rust_decimal_macros::dec;
    use time::macros::datetime;

    fn sample(value: rust_decimal::Decimal, recv: OffsetDateTime) -> OiSample {
        OiSample {
            instrument: InstrumentId::new(Exchange::Bybit, "BTCUSDT"),
            bucket_ts: datetime!(2026-04-25 10:00:00 UTC),
            recv_ts: recv,
            native_value: value,
            native_unit: UnitKind::Coins,
            oi_coins: Some(value),
            oi_usd: Some(value * dec!(64000)),
            price_used: Some(dec!(64000)),
        }
    }

    #[test]
    fn first_observe_creates_bar_with_one_sample() {
        let agg = OiAggregator::new();
        let bar = agg.observe(sample(dec!(100), datetime!(2026-04-25 10:00:01 UTC)));
        assert_eq!(bar.samples, 1);
        assert_eq!(bar.native_open, dec!(100));
        assert_eq!(bar.native_close, dec!(100));
        assert_eq!(agg.len(), 1);
    }

    #[test]
    fn many_observes_in_same_minute_fold_into_one_bar() {
        let agg = OiAggregator::new();
        agg.observe(sample(dec!(100), datetime!(2026-04-25 10:00:01 UTC)));
        agg.observe(sample(dec!(105), datetime!(2026-04-25 10:00:15 UTC)));
        agg.observe(sample(dec!(95), datetime!(2026-04-25 10:00:30 UTC)));
        let bar = agg.observe(sample(dec!(102), datetime!(2026-04-25 10:00:45 UTC)));
        assert_eq!(bar.samples, 4);
        assert_eq!(bar.native_open, dec!(100));
        assert_eq!(bar.native_high, dec!(105));
        assert_eq!(bar.native_low, dec!(95));
        assert_eq!(bar.native_close, dec!(102));
        assert_eq!(agg.len(), 1);
    }

    #[test]
    fn flush_through_drains_only_completed_buckets() {
        let agg = OiAggregator::new();
        // Two instruments, one bar each.
        agg.observe(sample(dec!(100), datetime!(2026-04-25 10:00:01 UTC)));
        let other = OiSample {
            instrument: InstrumentId::new(Exchange::Okx, "BTC-USDT-SWAP"),
            bucket_ts: datetime!(2026-04-25 10:01:00 UTC),
            recv_ts: datetime!(2026-04-25 10:01:01 UTC),
            native_value: dec!(200),
            native_unit: UnitKind::Coins,
            oi_coins: None,
            oi_usd: None,
            price_used: None,
        };
        agg.observe(other);
        // Flush through 10:00 — only the Bybit bar leaves.
        let drained = agg.flush_through(datetime!(2026-04-25 10:00:00 UTC));
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].instrument.exchange, Exchange::Bybit);
        assert_eq!(agg.len(), 1); // OKX bar stays
    }

    #[test]
    fn current_returns_in_progress_bar_without_mutating() {
        let agg = OiAggregator::new();
        agg.observe(sample(dec!(100), datetime!(2026-04-25 10:00:01 UTC)));
        agg.observe(sample(dec!(105), datetime!(2026-04-25 10:00:15 UTC)));
        let id = InstrumentId::new(Exchange::Bybit, "BTCUSDT");
        let peek = agg.current(&id).unwrap();
        assert_eq!(peek.samples, 2);
        assert_eq!(peek.native_high, dec!(105));
        // Doesn't drain.
        assert_eq!(agg.len(), 1);
    }

    #[test]
    fn bucket_roll_replaces_stale_bar_with_new_one() {
        let agg = OiAggregator::new();
        agg.observe(sample(dec!(100), datetime!(2026-04-25 10:00:01 UTC)));
        // Synthesize a sample for the next minute WITHOUT calling
        // flush_through — simulates a missed scheduler tick.
        let mut next_minute = sample(dec!(200), datetime!(2026-04-25 10:01:01 UTC));
        next_minute.bucket_ts = datetime!(2026-04-25 10:01:00 UTC);
        let new_bar = agg.observe(next_minute);
        assert_eq!(new_bar.bucket_ts, datetime!(2026-04-25 10:01:00 UTC));
        assert_eq!(new_bar.samples, 1);
        assert_eq!(new_bar.native_open, dec!(200));
        assert_eq!(agg.len(), 1);
    }

    #[test]
    fn flush_all_empties_the_aggregator() {
        let agg = OiAggregator::new();
        agg.observe(sample(dec!(1), datetime!(2026-04-25 10:00:01 UTC)));
        agg.observe(sample(dec!(2), datetime!(2026-04-25 10:00:02 UTC)));
        let bars = agg.flush_all();
        assert_eq!(bars.len(), 1); // same instrument folded
        assert_eq!(bars[0].samples, 2);
        assert!(agg.is_empty());
    }
}
