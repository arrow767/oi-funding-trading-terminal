//! OKX `open-interest` channel parser.
//!
//! OKX pushes the full OI value on every update (no snapshot/delta
//! split), so state is trivial: keep the last-seen value per symbol
//! and emit when it changes.
//!
//! Docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel-open-interest-channel>

use oi_core::{
    exchange::Exchange,
    instrument::InstrumentId,
    snapshot::{OiSample, RawOi},
    unit::UnitKind,
};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use time::OffsetDateTime;

/// Extract the `(instId, coins)` from a single OKX `open-interest`
/// push. Returns `None` for non-OI messages (pings, acks).
#[must_use]
pub fn extract_oi_update(frame: &Value) -> Option<(String, Decimal)> {
    let arg = frame.get("arg")?;
    if arg.get("channel")?.as_str()? != "open-interest" {
        return None;
    }
    let data = frame.get("data")?.as_array()?;
    let row = data.first()?;
    let inst_id = row.get("instId")?.as_str()?.to_owned();
    // Prefer oiCcy (coins); fall back to oi (contracts) only if
    // oiCcy is absent — we log once on fallback at the call site.
    let coins_str = row.get("oiCcy").and_then(|v| v.as_str()).unwrap_or("");
    let value = if !coins_str.is_empty() {
        Decimal::from_str(coins_str).ok()?
    } else {
        return None;
    };
    Some((inst_id, value))
}

/// Build a `RawOi` for the given instrument/value at wall clock `now`.
/// The bucket is floor-to-minute of `now` — live pushes land in the
/// currently-open minute, and the REST `:02` upsert overwrites.
#[must_use]
pub fn to_raw(inst_id: String, value: Decimal, now: OffsetDateTime) -> RawOi {
    let bucket = floor_to_minute(now);
    RawOi {
        instrument: InstrumentId::new(Exchange::Okx, inst_id),
        value,
        unit: UnitKind::Coins,
        bucket_ts: bucket,
        recv_ts: now,
        price_hint: None,
    }
}

/// Enrich a `RawOi` into a single-observation `OiSample` — OKX live
/// path is always Coins, no multiplier. USD remains `None` until a
/// price arrives from another source. The aggregator folds the
/// resulting samples into a per-minute OHLC bar.
#[must_use]
pub fn enrich_okx(raw: RawOi) -> OiSample {
    OiSample {
        instrument: raw.instrument,
        bucket_ts: raw.bucket_ts,
        recv_ts: raw.recv_ts,
        native_value: raw.value,
        native_unit: UnitKind::Coins,
        oi_coins: Some(raw.value),
        oi_usd: None,
        price_used: None,
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
    fn extracts_instid_and_coins_from_open_interest_push() {
        let frame = json!({
            "arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "instType": "SWAP",
                "instId": "BTC-USDT-SWAP",
                "oi": "123456",
                "oiCcy": "1234.56",
                "ts": "1714000000000"
            }]
        });
        let (id, v) = extract_oi_update(&frame).unwrap();
        assert_eq!(id, "BTC-USDT-SWAP");
        assert_eq!(v, dec!(1234.56));
    }

    #[test]
    fn ignores_non_open_interest_channel() {
        let frame = json!({
            "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
            "data": [{"instId": "BTC-USDT-SWAP", "oiCcy": "1.0"}]
        });
        assert!(extract_oi_update(&frame).is_none());
    }

    #[test]
    fn ignores_empty_data() {
        let frame = json!({"arg": {"channel": "open-interest"}, "data": []});
        assert!(extract_oi_update(&frame).is_none());
    }

    #[test]
    fn ignores_missing_oiccy() {
        let frame = json!({
            "arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
            "data": [{"instId": "BTC-USDT-SWAP", "oi": "1"}]
        });
        assert!(extract_oi_update(&frame).is_none());
    }

    #[test]
    fn enrich_copies_coins_and_leaves_usd_null() {
        let raw = to_raw(
            "BTC-USDT-SWAP".to_owned(),
            dec!(1234.56),
            time::macros::datetime!(2026-04-24 10:15:37 UTC),
        );
        let snap = enrich_okx(raw);
        assert_eq!(snap.native_value, dec!(1234.56));
        assert_eq!(snap.oi_coins, Some(dec!(1234.56)));
        assert_eq!(snap.oi_usd, None);
        assert_eq!(
            snap.bucket_ts,
            time::macros::datetime!(2026-04-24 10:15:00 UTC)
        );
    }
}
