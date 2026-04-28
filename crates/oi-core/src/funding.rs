//! Funding-rate value objects.
//!
//! Funding rate is the periodic premium/discount paid between long
//! and short perp holders. Settlement intervals vary by exchange:
//! 1h on Hyperliquid, 4h on a couple, 8h on most. Between
//! settlements every venue continuously publishes a "predicted" /
//! "current" rate that converges as the next settlement nears.
//!
//! We sample that predicted rate once per minute per
//! `(exchange, symbol)` and store it as a [`FundingBar`]. Because
//! intra-minute variance on funding rate is essentially zero (the
//! rate moves with the basis, which itself changes on a slow
//! cadence), we don't aggregate to OHLC like we do for OI — a
//! single value per minute is enough for charting and downstream
//! analytics.

use crate::instrument::InstrumentId;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// One-minute funding-rate sample.
///
/// `rate` is the value the exchange published at the moment of
/// fetch — typically the predicted rate for the upcoming settlement.
/// Just-paid rates (immediately after settlement) sit alongside the
/// predicted ones in the same series; consumers can detect a
/// settlement by an abrupt change combined with `next_funding_ts`
/// rolling forward.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingBar {
    pub instrument: InstrumentId,
    /// Minute-aligned bucket (UTC).
    #[serde(with = "time::serde::rfc3339")]
    pub bucket_ts: OffsetDateTime,
    /// When the adapter received the data — drives latency metrics.
    #[serde(with = "time::serde::rfc3339")]
    pub recv_ts: OffsetDateTime,
    /// The published funding rate. Decimal because rates carry many
    /// significant figures (e.g. `0.000125` = 1.25 bps).
    pub rate: Decimal,
    /// Timestamp of the next settlement (when `rate` will be paid
    /// to the appropriate side). May be `None` if the exchange
    /// doesn't publish it explicitly.
    #[serde(default, with = "rfc3339_opt")]
    pub next_funding_ts: Option<OffsetDateTime>,
    /// Settlement interval in hours where derivable from the
    /// payload (Bybit/OKX/Bitget publish it explicitly; others we
    /// can leave None and consumers infer from the cadence of
    /// `next_funding_ts` rolling forward).
    #[serde(default)]
    pub interval_hours: Option<u8>,
}

mod rfc3339_opt {
    use serde::{Deserializer, Serializer};
    use time::OffsetDateTime;

    pub fn serialize<S: Serializer>(
        v: &Option<OffsetDateTime>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match v {
            Some(t) => time::serde::rfc3339::serialize(t, s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<OffsetDateTime>, D::Error> {
        time::serde::rfc3339::option::deserialize(d)
    }
}

/// A settlement event — funding actually paid at the venue's
/// settlement boundary. Distinct from [`FundingBar`] (the
/// continuous predicted rate sampled per minute): events are
/// discrete, occur at exact known times (00:00 / 08:00 / 16:00
/// UTC for 8h venues, hourly for Hyperliquid), and carry the
/// rate the exchange actually applied to position holders.
///
/// Stored separately from `FundingBar` so consumers can:
/// * Aggregate "total funding paid by longs over a period"
///   without filtering an hours-coarse predicted-rate series.
/// * Backtest funding-arbitrage strategies against the exact
///   historical settlements rather than approximations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingEvent {
    pub instrument: InstrumentId,
    /// The settlement boundary timestamp (UTC). Primary key
    /// component along with `instrument`.
    #[serde(with = "time::serde::rfc3339")]
    pub settlement_ts: OffsetDateTime,
    /// The rate that was actually paid at this settlement.
    pub rate: Decimal,
    /// Mark price at settlement, when the exchange publishes it
    /// alongside the rate (Binance does, most others don't).
    /// Useful for computing the realised dollar payment.
    #[serde(default)]
    pub mark_price: Option<Decimal>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::Exchange;
    use rust_decimal_macros::dec;
    use time::macros::datetime;

    #[test]
    fn serde_roundtrip_with_optional_fields_set() {
        let bar = FundingBar {
            instrument: InstrumentId::new(Exchange::Binance, "BTCUSDT"),
            bucket_ts: datetime!(2026-04-26 10:00:00 UTC),
            recv_ts: datetime!(2026-04-26 10:00:02 UTC),
            rate: dec!(0.0001),
            next_funding_ts: Some(datetime!(2026-04-26 16:00:00 UTC)),
            interval_hours: Some(8),
        };
        let s = serde_json::to_string(&bar).unwrap();
        let back: FundingBar = serde_json::from_str(&s).unwrap();
        assert_eq!(bar, back);
    }

    #[test]
    fn funding_event_serde_roundtrip() {
        let e = FundingEvent {
            instrument: InstrumentId::new(Exchange::Binance, "BTCUSDT"),
            settlement_ts: datetime!(2026-04-26 16:00:00 UTC),
            rate: dec!(0.0001),
            mark_price: Some(dec!(64000.5)),
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: FundingEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn funding_event_serde_without_mark_price() {
        let e = FundingEvent {
            instrument: InstrumentId::new(Exchange::Bybit, "BTCUSDT"),
            settlement_ts: datetime!(2026-04-26 16:00:00 UTC),
            rate: dec!(-0.00012),
            mark_price: None,
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: FundingEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn serde_roundtrip_with_optional_fields_absent() {
        let bar = FundingBar {
            instrument: InstrumentId::new(Exchange::Hyperliquid, "BTC"),
            bucket_ts: datetime!(2026-04-26 10:00:00 UTC),
            recv_ts: datetime!(2026-04-26 10:00:02 UTC),
            rate: dec!(-0.00005),
            next_funding_ts: None,
            interval_hours: None,
        };
        let s = serde_json::to_string(&bar).unwrap();
        let back: FundingBar = serde_json::from_str(&s).unwrap();
        assert_eq!(bar, back);
    }
}
