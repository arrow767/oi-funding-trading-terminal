//! Bitget v2 `ticker` push parser.
//!
//! Each push carries the full set of ticker fields (snapshot and
//! update both — Bitget doesn't send partial deltas on this channel),
//! so state is trivial: last-seen OI per symbol, emit on change.

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

/// Pull `(instId, coins, Option<mark_price>)` from a Bitget ticker frame.
/// Returns `None` for acks or non-ticker channels.
#[must_use]
pub fn extract_oi_update(frame: &Value) -> Option<(String, Decimal, Option<Decimal>)> {
    let arg = frame.get("arg")?;
    if arg.get("channel")?.as_str()? != "ticker" {
        return None;
    }
    let data = frame.get("data")?.as_array()?;
    let row = data.first()?;
    let inst_id = row.get("instId")?.as_str()?.to_owned();
    let oi_s = row.get("holdingAmount")?.as_str()?;
    if oi_s.is_empty() {
        return None;
    }
    let value = Decimal::from_str(oi_s).ok()?;
    let price = row
        .get("markPrice")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .and_then(|s| Decimal::from_str(s).ok());
    Some((inst_id, value, price))
}

#[must_use]
pub fn to_raw(
    inst_id: String,
    value: Decimal,
    price: Option<Decimal>,
    now: OffsetDateTime,
) -> RawOi {
    let bucket = floor_to_minute(now);
    let instrument = InstrumentId::new(Exchange::Bitget, inst_id);
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
pub fn enrich_bitget(raw: RawOi) -> OiSample {
    let price = raw.price_hint.as_ref().map(|q| q.price);
    let oi_coins = Some(raw.value);
    let oi_usd = price.map(|p| raw.value * p);
    OiSample {
        instrument: raw.instrument,
        bucket_ts: raw.bucket_ts,
        recv_ts: raw.recv_ts,
        native_value: raw.value,
        native_unit: UnitKind::Coins,
        oi_coins,
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
    fn extracts_oi_and_mark_price_from_ticker_push() {
        let frame = json!({
            "action": "snapshot",
            "arg": {"instType":"USDT-FUTURES","channel":"ticker","instId":"BTCUSDT"},
            "data": [{
                "instId": "BTCUSDT",
                "last": "64000",
                "markPrice": "64001",
                "holdingAmount": "12345.67",
                "ts": "1714000000000"
            }]
        });
        let (id, oi, px) = extract_oi_update(&frame).unwrap();
        assert_eq!(id, "BTCUSDT");
        assert_eq!(oi, dec!(12345.67));
        assert_eq!(px, Some(dec!(64001)));
    }

    #[test]
    fn ignores_non_ticker_channel() {
        let frame = json!({
            "arg": {"channel":"trade","instId":"BTCUSDT"},
            "data": [{"instId":"BTCUSDT","holdingAmount":"1"}]
        });
        assert!(extract_oi_update(&frame).is_none());
    }

    #[test]
    fn enrich_computes_usd_when_price_present() {
        let raw = to_raw(
            "BTCUSDT".into(),
            dec!(100),
            Some(dec!(64000)),
            time::macros::datetime!(2026-04-24 10:15:37 UTC),
        );
        let snap = enrich_bitget(raw);
        assert_eq!(snap.oi_coins, Some(dec!(100)));
        assert_eq!(snap.oi_usd, Some(dec!(6_400_000)));
        assert_eq!(
            snap.bucket_ts,
            time::macros::datetime!(2026-04-24 10:15:00 UTC)
        );
    }
}
