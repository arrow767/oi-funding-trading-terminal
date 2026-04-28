//! Hyperliquid `activeAssetCtx` push parser.
//!
//! Push shape:
//! ```json
//! {"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{
//!   "markPx":"64000","openInterest":"1234.5","oraclePx":"64001",
//!   "midPx":"64000.1","funding":"0.0001","premium":"0.0002",
//!   "prevDayPx":"63000","dayNtlVlm":"..."
//! }}}
//! ```
//! Every push is authoritative (no snapshot/delta split). Stream
//! state is last-seen OI per coin — emit on change.

use oi_core::{
    exchange::Exchange,
    instrument::InstrumentId,
    price::{PriceQuote, PriceSource},
    snapshot::{OiSample, RawOi},
    unit::UnitKind,
};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use time::OffsetDateTime;

#[must_use]
pub fn extract_oi_update(frame: &Value) -> Option<(String, Decimal, Option<Decimal>)> {
    if frame.get("channel")?.as_str()? != "activeAssetCtx" {
        return None;
    }
    let data = frame.get("data")?;
    let coin = data.get("coin")?.as_str()?.to_owned();
    let ctx = data.get("ctx")?;
    let oi_s = ctx.get("openInterest")?.as_str()?;
    if oi_s.is_empty() {
        return None;
    }
    let value = Decimal::from_str(oi_s).ok()?;
    let price = ctx
        .get("markPx")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .and_then(|s| Decimal::from_str(s).ok());
    Some((coin, value, price))
}

#[must_use]
pub fn to_raw(
    coin: String,
    value: Decimal,
    price: Option<Decimal>,
    now: OffsetDateTime,
) -> RawOi {
    let bucket = floor_to_minute(now);
    let instrument = InstrumentId::new(Exchange::Hyperliquid, coin);
    let price_hint = price.map(|p| PriceQuote {
        instrument: instrument.clone(),
        price: p,
        source: PriceSource::Mark,
        ts: now,
    });
    RawOi {
        instrument,
        value,
        unit: UnitKind::Coins,
        bucket_ts: bucket,
        recv_ts: now,
        price_hint,
    }
}

#[must_use]
pub fn enrich_hyperliquid(raw: RawOi) -> OiSample {
    let price = raw.price_hint.as_ref().map(|q| q.price);
    let oi_usd = price.map(|p| raw.value * p);
    OiSample {
        instrument: raw.instrument,
        bucket_ts: raw.bucket_ts,
        recv_ts: raw.recv_ts,
        native_value: raw.value,
        native_unit: UnitKind::Coins,
        oi_coins: Some(raw.value),
        oi_usd,
        price_used: price,
    }
}

fn floor_to_minute(ts: OffsetDateTime) -> OffsetDateTime {
    let sec = i64::from(ts.second());
    let nanos = ts.nanosecond();
    ts - time::Duration::seconds(sec) - time::Duration::nanoseconds(i64::from(nanos))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    #[test]
    fn extracts_coin_oi_and_markpx() {
        let frame = json!({
            "channel": "activeAssetCtx",
            "data": {
                "coin": "BTC",
                "ctx": {
                    "markPx": "64000",
                    "openInterest": "1234.5",
                    "oraclePx": "64001"
                }
            }
        });
        let (c, oi, px) = extract_oi_update(&frame).unwrap();
        assert_eq!(c, "BTC");
        assert_eq!(oi, dec!(1234.5));
        assert_eq!(px, Some(dec!(64000)));
    }

    #[test]
    fn ignores_other_channels() {
        let frame = json!({"channel":"l2Book","data":{}});
        assert!(extract_oi_update(&frame).is_none());
    }

    #[test]
    fn enrich_produces_usd_from_coins_times_markpx() {
        let raw = to_raw(
            "BTC".into(),
            dec!(100),
            Some(dec!(64000)),
            time::macros::datetime!(2026-04-24 10:15:00 UTC),
        );
        let snap = enrich_hyperliquid(raw);
        assert_eq!(snap.oi_coins, Some(dec!(100)));
        assert_eq!(snap.oi_usd, Some(dec!(6_400_000)));
    }
}
