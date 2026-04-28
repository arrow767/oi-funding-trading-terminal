//! Per-symbol state for Bybit `tickers.*` WS deltas.
//!
//! Bybit v5 WS sends the first frame per topic as `type: "snapshot"`
//! with the full set of fields, then `type: "delta"` frames that only
//! include fields that changed. To emit a complete `OiSnapshot` on
//! every update we keep a tiny per-symbol state and fold deltas into
//! it.
//!
//! The merger is a pure function over `serde_json::Value` — no I/O,
//! no tokio — so the rules are testable in isolation. The orchestrator
//! (see `oi-collector/src/live/bybit.rs`) owns the `DashMap` of states
//! and wires the output into the pub/sub publisher.

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

/// Accumulated view of one symbol's ticker fields.
#[derive(Debug, Default, Clone)]
pub struct SymbolState {
    pub open_interest: Option<Decimal>,
    pub mark_price: Option<Decimal>,
    pub last_price: Option<Decimal>,
}

impl SymbolState {
    /// Merge a Bybit `tickers.*` frame into this state. Returns the
    /// NEW OI value iff it changed (for "emit only on OI change"
    /// semantics); price-only updates don't produce a new
    /// `OiSnapshot` — they just update local state so the next OI
    /// change carries a fresh price.
    pub fn merge(&mut self, frame_type: FrameType, data: &Value) -> Option<Decimal> {
        let prev_oi = self.open_interest;

        // For a snapshot, replace any missing fields with the frame's
        // value; existing values are also overwritten (snapshots are
        // authoritative).
        // For a delta, only update fields that are present.
        if matches!(frame_type, FrameType::Snapshot) {
            // A snapshot may not include every field either — unknown
            // fields stay unknown. But we DO overwrite existing fields.
            self.open_interest = pick_decimal(data, "openInterest").or(self.open_interest);
            self.mark_price = pick_decimal(data, "markPrice").or(self.mark_price);
            self.last_price = pick_decimal(data, "lastPrice").or(self.last_price);
        } else {
            // Delta: only non-null present fields overwrite.
            if let Some(v) = pick_decimal(data, "openInterest") {
                self.open_interest = Some(v);
            }
            if let Some(v) = pick_decimal(data, "markPrice") {
                self.mark_price = Some(v);
            }
            if let Some(v) = pick_decimal(data, "lastPrice") {
                self.last_price = Some(v);
            }
        }

        match (prev_oi, self.open_interest) {
            (Some(a), Some(b)) if a == b => None,
            (None, None) => None,
            (_, Some(new)) => Some(new),
            (_, None) => None,
        }
    }

    /// Build a `RawOi` from current state. Caller supplies the symbol
    /// (we don't own it) and the bucket/recv timestamps. Returns
    /// `None` if there's no OI to report yet.
    pub fn to_raw(
        &self,
        symbol: &str,
        bucket_ts: OffsetDateTime,
        recv_ts: OffsetDateTime,
    ) -> Option<RawOi> {
        let value = self.open_interest?;
        let instrument = InstrumentId::new(Exchange::Bybit, symbol.to_owned());
        let price_hint = self
            .mark_price
            .or(self.last_price)
            .map(|p| PriceQuote {
                instrument: instrument.clone(),
                price: p,
                source: if self.mark_price.is_some() {
                    PriceSource::Mark
                } else {
                    PriceSource::Last
                },
                ts: recv_ts,
            });
        Some(RawOi {
            instrument,
            value,
            unit: UnitKind::Coins,
            bucket_ts,
            recv_ts,
            price_hint,
        })
    }
}

/// Enrich a `RawOi` into a single-observation `OiSample` without an
/// explicit `InstrumentMeta` — Bybit perps are always coins-native,
/// so there's no multiplier to look up. The aggregator folds the
/// resulting samples into one OHLC bar per minute.
#[must_use]
pub fn enrich_bybit(raw: RawOi) -> OiSample {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Snapshot,
    Delta,
}

/// Parse `"type"` + `"topic"` out of a Bybit data frame. Returns
/// `None` for non-data frames (op acks) or unexpected topics.
#[must_use]
pub fn classify_frame(frame: &Value) -> Option<(FrameType, String)> {
    let topic = frame.get("topic")?.as_str()?;
    let symbol = topic.strip_prefix("tickers.")?.to_owned();
    let ty = match frame.get("type")?.as_str()? {
        "snapshot" => FrameType::Snapshot,
        "delta" => FrameType::Delta,
        _ => return None,
    };
    Some((ty, symbol))
}

fn pick_decimal(data: &Value, key: &str) -> Option<Decimal> {
    let s = data.get(key)?.as_str()?;
    if s.is_empty() {
        return None;
    }
    Decimal::from_str(s).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    fn snapshot(symbol: &str, oi: &str, mark: &str) -> Value {
        json!({
            "topic": format!("tickers.{symbol}"),
            "type": "snapshot",
            "ts": 1714000000000u64,
            "data": { "symbol": symbol, "openInterest": oi, "markPrice": mark, "lastPrice": mark }
        })
    }

    fn delta(symbol: &str, fields: Value) -> Value {
        json!({
            "topic": format!("tickers.{symbol}"),
            "type": "delta",
            "ts": 1714000000500u64,
            "data": fields
        })
    }

    #[test]
    fn classifies_snapshot_and_delta() {
        let s = snapshot("BTCUSDT", "1.0", "64000");
        let (t, sym) = classify_frame(&s).unwrap();
        assert_eq!(t, FrameType::Snapshot);
        assert_eq!(sym, "BTCUSDT");

        let d = delta("BTCUSDT", json!({"openInterest": "2.0"}));
        let (t, _) = classify_frame(&d).unwrap();
        assert_eq!(t, FrameType::Delta);
    }

    #[test]
    fn snapshot_populates_all_fields() {
        let mut st = SymbolState::default();
        let f = snapshot("BTCUSDT", "100.0", "64000.0");
        let changed = st.merge(FrameType::Snapshot, f.get("data").unwrap());
        assert_eq!(changed, Some(dec!(100.0)));
        assert_eq!(st.open_interest, Some(dec!(100.0)));
        assert_eq!(st.mark_price, Some(dec!(64000.0)));
    }

    #[test]
    fn delta_oi_change_signals_emit() {
        let mut st = SymbolState::default();
        let f = snapshot("BTCUSDT", "100.0", "64000.0");
        st.merge(FrameType::Snapshot, f.get("data").unwrap());
        let d = delta("BTCUSDT", json!({"openInterest": "110.0"}));
        let changed = st.merge(FrameType::Delta, d.get("data").unwrap());
        assert_eq!(changed, Some(dec!(110.0)));
    }

    #[test]
    fn delta_without_oi_change_is_silent() {
        let mut st = SymbolState::default();
        let f = snapshot("BTCUSDT", "100.0", "64000.0");
        st.merge(FrameType::Snapshot, f.get("data").unwrap());
        // Price-only delta — updates mark_price but OI stayed same.
        let d = delta("BTCUSDT", json!({"markPrice": "64100.0"}));
        let changed = st.merge(FrameType::Delta, d.get("data").unwrap());
        assert_eq!(changed, None);
        assert_eq!(st.mark_price, Some(dec!(64100.0)));
    }

    #[test]
    fn delta_with_same_oi_is_silent() {
        let mut st = SymbolState::default();
        let f = snapshot("BTCUSDT", "100.0", "64000.0");
        st.merge(FrameType::Snapshot, f.get("data").unwrap());
        let d = delta("BTCUSDT", json!({"openInterest": "100.0"}));
        let changed = st.merge(FrameType::Delta, d.get("data").unwrap());
        assert_eq!(changed, None);
    }

    #[test]
    fn to_raw_uses_mark_price_hint_when_available() {
        let mut st = SymbolState::default();
        let f = snapshot("BTCUSDT", "100.0", "64000.0");
        st.merge(FrameType::Snapshot, f.get("data").unwrap());
        let raw = st
            .to_raw(
                "BTCUSDT",
                time::macros::datetime!(2026-04-24 10:00:00 UTC),
                time::macros::datetime!(2026-04-24 10:00:05 UTC),
            )
            .unwrap();
        assert_eq!(raw.value, dec!(100.0));
        let hint = raw.price_hint.unwrap();
        assert_eq!(hint.source, PriceSource::Mark);
    }

    #[test]
    fn enrich_computes_usd_from_coins_and_price() {
        let raw = RawOi {
            instrument: InstrumentId::new(Exchange::Bybit, "BTCUSDT".to_owned()),
            value: dec!(100),
            unit: UnitKind::Coins,
            bucket_ts: time::macros::datetime!(2026-04-24 10:00:00 UTC),
            recv_ts: time::macros::datetime!(2026-04-24 10:00:00 UTC),
            price_hint: Some(PriceQuote {
                instrument: InstrumentId::new(Exchange::Bybit, "BTCUSDT".to_owned()),
                price: dec!(64000),
                source: PriceSource::Mark,
                ts: time::macros::datetime!(2026-04-24 10:00:00 UTC),
            }),
        };
        let snap = enrich_bybit(raw);
        assert_eq!(snap.oi_coins, Some(dec!(100)));
        assert_eq!(snap.oi_usd, Some(dec!(6_400_000)));
    }
}
