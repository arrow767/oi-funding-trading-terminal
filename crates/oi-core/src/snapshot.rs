//! OI value objects.
//!
//! Three layers, narrowing as data flows through the platform:
//!
//! 1. [`RawOi`] — what an exchange adapter produces from a single
//!    poll or WS frame. Native value + unit + optional co-fetched
//!    price.
//! 2. [`OiSample`] — a single observation, enriched with USD/coins
//!    derivations. The aggregator consumes a stream of these.
//! 3. [`OiSnapshot`] — the **OHLC bar** for a one-minute bucket.
//!    What the storage and API layers serialize. Multiple
//!    [`OiSample`]s within a bucket fold into one snapshot via
//!    [`OiSnapshot::start_from_sample`] + [`OiSnapshot::observe`].
//!
//! All three carry the minute-aligned `bucket_ts` separately from
//! per-sample `recv_ts`. The bar tracks `first_recv_ts` and
//! `last_recv_ts` so consumers can tell open-tick from close-tick
//! latency.

use crate::error::ExchangeError;
use crate::instrument::{InstrumentId, InstrumentMeta};
use crate::price::PriceQuote;
use crate::unit::UnitKind;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Raw OI as published by the exchange, before USD/coins enrichment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawOi {
    pub instrument: InstrumentId,
    /// Value as published (in `unit`).
    pub value: Decimal,
    pub unit: UnitKind,
    /// The minute this sample is bucketed into. Truncated to minute start (UTC).
    #[serde(with = "time::serde::rfc3339")]
    pub bucket_ts: OffsetDateTime,
    /// When the adapter received the data. Retained for latency metrics.
    #[serde(with = "time::serde::rfc3339")]
    pub recv_ts: OffsetDateTime,
    /// Optional co-fetched price (adapters that publish a ticker together
    /// with OI — like Hyperliquid — fill this in). Otherwise `None` and the
    /// collector joins a separate `PriceQuote` later.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_hint: Option<PriceQuote>,
}

/// A single enriched observation. Internal to the aggregator —
/// storage and API never see this; they see [`OiSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OiSample {
    pub instrument: InstrumentId,
    #[serde(with = "time::serde::rfc3339")]
    pub bucket_ts: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub recv_ts: OffsetDateTime,

    pub native_value: Decimal,
    pub native_unit: UnitKind,
    pub oi_coins: Option<Decimal>,
    pub oi_usd: Option<Decimal>,
    pub price_used: Option<Decimal>,
}

impl OiSample {
    /// Enrich a `RawOi` into an `OiSample` using the supplied
    /// instrument metadata and optional USD price.
    pub fn enrich(
        raw: RawOi,
        meta: &InstrumentMeta,
        price_usd: Option<Decimal>,
    ) -> std::result::Result<Self, ExchangeError> {
        if raw.instrument != meta.id {
            return Err(ExchangeError::Schema(format!(
                "instrument mismatch: raw={} meta={}",
                raw.instrument, meta.id
            )));
        }

        let mult = meta.contract_multiplier;
        // Prefer the explicit `price_hint` on the raw sample; fall back to the
        // externally supplied price.
        let price = raw.price_hint.as_ref().map(|q| q.price).or(price_usd);

        let oi_coins = raw.unit.to_coins(raw.value, mult);
        let oi_usd = raw.unit.to_usd(raw.value, mult, price);

        Ok(Self {
            instrument: raw.instrument,
            bucket_ts: raw.bucket_ts,
            recv_ts: raw.recv_ts,
            native_value: raw.value,
            native_unit: raw.unit,
            oi_coins,
            oi_usd,
            price_used: price,
        })
    }
}

/// One-minute OHLC bar. Folded from a stream of [`OiSample`]s sharing
/// the same `(instrument, bucket_ts)`.
///
/// Each value column has open / high / low / close. Nullable columns
/// (`oi_coins`, `oi_usd`) are independently tracked — when only some
/// samples in the minute have a price, the high/low/close reflect
/// only the priced samples, while the unpriced ones still drive
/// `native_*`. `samples` counts every observation regardless.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OiSnapshot {
    pub instrument: InstrumentId,
    #[serde(with = "time::serde::rfc3339")]
    pub bucket_ts: OffsetDateTime,
    /// recv_ts of the first sample folded into this bar.
    #[serde(with = "time::serde::rfc3339")]
    pub first_recv_ts: OffsetDateTime,
    /// recv_ts of the most recent sample folded in.
    #[serde(with = "time::serde::rfc3339")]
    pub last_recv_ts: OffsetDateTime,
    /// Number of samples folded so far. `1` for REST-only exchanges,
    /// many for WS-live exchanges.
    pub samples: u32,

    pub native_unit: UnitKind,
    pub native_open: Decimal,
    pub native_high: Decimal,
    pub native_low: Decimal,
    pub native_close: Decimal,

    pub oi_coins_open: Option<Decimal>,
    pub oi_coins_high: Option<Decimal>,
    pub oi_coins_low: Option<Decimal>,
    pub oi_coins_close: Option<Decimal>,

    pub oi_usd_open: Option<Decimal>,
    pub oi_usd_high: Option<Decimal>,
    pub oi_usd_low: Option<Decimal>,
    pub oi_usd_close: Option<Decimal>,

    /// Price used at the close — keeping just one (not full OHLC) is
    /// a deliberate choice: price OHLC would be redundant with
    /// dedicated price feeds and adds 4 columns for diminishing
    /// audit value. The close is sufficient for "what price drove
    /// the close OI?" forensics.
    pub price_used_close: Option<Decimal>,
}

impl OiSnapshot {
    /// Initialize a bar from the first sample in the bucket. Open =
    /// high = low = close = the sample's value, samples = 1.
    #[must_use]
    pub fn start_from_sample(s: OiSample) -> Self {
        Self {
            instrument: s.instrument,
            bucket_ts: s.bucket_ts,
            first_recv_ts: s.recv_ts,
            last_recv_ts: s.recv_ts,
            samples: 1,
            native_unit: s.native_unit,
            native_open: s.native_value,
            native_high: s.native_value,
            native_low: s.native_value,
            native_close: s.native_value,
            oi_coins_open: s.oi_coins,
            oi_coins_high: s.oi_coins,
            oi_coins_low: s.oi_coins,
            oi_coins_close: s.oi_coins,
            oi_usd_open: s.oi_usd,
            oi_usd_high: s.oi_usd,
            oi_usd_low: s.oi_usd,
            oi_usd_close: s.oi_usd,
            price_used_close: s.price_used,
        }
    }

    /// Fold a subsequent sample into this bar.
    ///
    /// Updates close (always), high/low (when the value crosses the
    /// running extreme), `last_recv_ts`, `samples`, and the close
    /// price. `first_recv_ts` and `*_open` are immutable once set.
    /// Samples whose `bucket_ts` doesn't match are rejected (caller
    /// bug — the aggregator routes by bucket).
    pub fn observe(&mut self, s: &OiSample) -> std::result::Result<(), ExchangeError> {
        if s.instrument != self.instrument || s.bucket_ts != self.bucket_ts {
            return Err(ExchangeError::Schema(format!(
                "observe: sample bucket {}@{} does not match bar {}@{}",
                s.instrument, s.bucket_ts, self.instrument, self.bucket_ts
            )));
        }
        self.samples = self.samples.saturating_add(1);
        self.last_recv_ts = s.recv_ts;

        // Native is always present.
        self.native_close = s.native_value;
        if s.native_value > self.native_high {
            self.native_high = s.native_value;
        }
        if s.native_value < self.native_low {
            self.native_low = s.native_value;
        }

        fold_optional(
            &mut self.oi_coins_open,
            &mut self.oi_coins_high,
            &mut self.oi_coins_low,
            &mut self.oi_coins_close,
            s.oi_coins,
        );
        fold_optional(
            &mut self.oi_usd_open,
            &mut self.oi_usd_high,
            &mut self.oi_usd_low,
            &mut self.oi_usd_close,
            s.oi_usd,
        );
        if let Some(p) = s.price_used {
            self.price_used_close = Some(p);
        }
        Ok(())
    }
}

/// Update OHLC fields for a nullable observation.
///
/// When this is the FIRST priced sample in the bar, all four fields
/// initialize to the value. After that, close updates always; high
/// and low only on extreme crossings; open never changes.
fn fold_optional(
    open: &mut Option<Decimal>,
    high: &mut Option<Decimal>,
    low: &mut Option<Decimal>,
    close: &mut Option<Decimal>,
    incoming: Option<Decimal>,
) {
    let Some(v) = incoming else { return };
    if open.is_none() {
        *open = Some(v);
        *high = Some(v);
        *low = Some(v);
        *close = Some(v);
        return;
    }
    *close = Some(v);
    if let Some(h) = *high {
        if v > h {
            *high = Some(v);
        }
    }
    if let Some(l) = *low {
        if v < l {
            *low = Some(v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::Exchange;
    use rust_decimal_macros::dec;
    use time::macros::datetime;

    fn meta_contracts() -> InstrumentMeta {
        InstrumentMeta {
            id: InstrumentId::new(Exchange::Okx, "BTC-USDT-SWAP"),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            is_perpetual: true,
            native_unit: UnitKind::Contracts,
            contract_multiplier: Some(dec!(0.01)),
            price_tick: Some(dec!(0.1)),
            qty_step: Some(dec!(0.01)),
            active: true,
        }
    }

    fn raw_at(value: Decimal, recv_ts: OffsetDateTime) -> RawOi {
        RawOi {
            instrument: meta_contracts().id,
            value,
            unit: UnitKind::Contracts,
            bucket_ts: datetime!(2026-04-24 10:00:00 UTC),
            recv_ts,
            price_hint: None,
        }
    }

    #[test]
    fn sample_enrich_contracts_with_price_fills_both_columns() {
        let meta = meta_contracts();
        let s = OiSample::enrich(
            raw_at(dec!(1000), datetime!(2026-04-24 10:00:02 UTC)),
            &meta,
            Some(dec!(50_000)),
        )
        .unwrap();
        assert_eq!(s.oi_coins, Some(dec!(10)));
        assert_eq!(s.oi_usd, Some(dec!(500_000)));
    }

    #[test]
    fn sample_enrich_rejects_mismatched_instrument() {
        let meta = meta_contracts();
        let mut raw = raw_at(dec!(1), datetime!(2026-04-24 10:00:00 UTC));
        raw.instrument = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        raw.unit = UnitKind::Coins;
        let err = OiSample::enrich(raw, &meta, None).unwrap_err();
        assert!(matches!(err, ExchangeError::Schema(_)));
    }

    fn mk_sample(value: Decimal, coins: Option<Decimal>, recv: OffsetDateTime) -> OiSample {
        OiSample {
            instrument: meta_contracts().id,
            bucket_ts: datetime!(2026-04-24 10:00:00 UTC),
            recv_ts: recv,
            native_value: value,
            native_unit: UnitKind::Contracts,
            oi_coins: coins,
            oi_usd: None,
            price_used: None,
        }
    }

    #[test]
    fn one_sample_yields_o_eq_h_eq_l_eq_c() {
        let s = mk_sample(dec!(100), Some(dec!(1)), datetime!(2026-04-24 10:00:02 UTC));
        let bar = OiSnapshot::start_from_sample(s);
        assert_eq!(bar.samples, 1);
        assert_eq!(bar.native_open, dec!(100));
        assert_eq!(bar.native_high, dec!(100));
        assert_eq!(bar.native_low, dec!(100));
        assert_eq!(bar.native_close, dec!(100));
        assert_eq!(bar.oi_coins_open, Some(dec!(1)));
        assert_eq!(bar.oi_coins_close, Some(dec!(1)));
    }

    #[test]
    fn ohlc_tracks_extremes_across_a_run_of_samples() {
        // Open=100, then 105 (new high), 95 (new low), 102 (close).
        let mut bar = OiSnapshot::start_from_sample(mk_sample(
            dec!(100),
            Some(dec!(10)),
            datetime!(2026-04-24 10:00:01 UTC),
        ));
        bar.observe(&mk_sample(dec!(105), Some(dec!(11)), datetime!(2026-04-24 10:00:15 UTC)))
            .unwrap();
        bar.observe(&mk_sample(dec!(95), Some(dec!(9)), datetime!(2026-04-24 10:00:30 UTC)))
            .unwrap();
        bar.observe(&mk_sample(dec!(102), Some(dec!(10.2)), datetime!(2026-04-24 10:00:45 UTC)))
            .unwrap();

        assert_eq!(bar.samples, 4);
        assert_eq!(bar.native_open, dec!(100));
        assert_eq!(bar.native_high, dec!(105));
        assert_eq!(bar.native_low, dec!(95));
        assert_eq!(bar.native_close, dec!(102));

        assert_eq!(bar.oi_coins_open, Some(dec!(10)));
        assert_eq!(bar.oi_coins_high, Some(dec!(11)));
        assert_eq!(bar.oi_coins_low, Some(dec!(9)));
        assert_eq!(bar.oi_coins_close, Some(dec!(10.2)));

        assert_eq!(bar.first_recv_ts, datetime!(2026-04-24 10:00:01 UTC));
        assert_eq!(bar.last_recv_ts, datetime!(2026-04-24 10:00:45 UTC));
    }

    #[test]
    fn first_priced_sample_in_bar_initializes_optional_ohlc_after_unpriced_open() {
        // Open is unpriced (oi_coins=None). Second sample arrives
        // with a price — open/high/low/close all start there.
        let mut bar = OiSnapshot::start_from_sample(mk_sample(
            dec!(100),
            None,
            datetime!(2026-04-24 10:00:01 UTC),
        ));
        assert_eq!(bar.oi_coins_open, None);
        bar.observe(&mk_sample(dec!(101), Some(dec!(20)), datetime!(2026-04-24 10:00:15 UTC)))
            .unwrap();
        // The bar's coins-open isn't the unpriced first sample's
        // None, it's the FIRST priced observation (20).
        assert_eq!(bar.oi_coins_open, Some(dec!(20)));
        assert_eq!(bar.oi_coins_high, Some(dec!(20)));
        assert_eq!(bar.oi_coins_low, Some(dec!(20)));
        assert_eq!(bar.oi_coins_close, Some(dec!(20)));
    }

    #[test]
    fn observe_rejects_cross_bucket_sample() {
        let mut bar = OiSnapshot::start_from_sample(mk_sample(
            dec!(1),
            None,
            datetime!(2026-04-24 10:00:00 UTC),
        ));
        let mut wrong = mk_sample(dec!(2), None, datetime!(2026-04-24 10:01:00 UTC));
        wrong.bucket_ts = datetime!(2026-04-24 10:01:00 UTC);
        let err = bar.observe(&wrong).unwrap_err();
        assert!(matches!(err, ExchangeError::Schema(_)));
    }
}
