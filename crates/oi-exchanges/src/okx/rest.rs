//! OKX SWAP (USDT/USDC/USD perp) adapter.
//!
//! Endpoints used:
//! * `GET /api/v5/public/instruments?instType=SWAP` — discovery. Returns
//!   `ctVal` (contract multiplier) and `ctValCcy` (denomination).
//!   <https://www.okx.com/docs-v5/en/#rest-api-public-data-get-instruments>
//! * `GET /api/v5/public/open-interest?instType=SWAP` — **batch** OI,
//!   returns `oi` (contracts) AND `oiCcy` (coins). We store `oiCcy` as
//!   `UnitKind::Coins`; it eliminates the multiplier dependency and
//!   sidesteps schema drift on `ctVal`.
//!   <https://www.okx.com/docs-v5/en/#rest-api-public-data-get-open-interest>
//! * `GET /api/v5/market/tickers?instType=SWAP` — batch last+mark prices.
//!   <https://www.okx.com/docs-v5/en/#rest-api-market-data-get-tickers>
//!
//! All OKX responses share the `{"code":"0","msg":"","data":[...]}`
//! envelope. `code != "0"` is a soft error (auth, invalid param) — we map
//! to `ExchangeError::Schema` with the OKX message included.
//!
//! Symbol format: OKX uses `BTC-USDT-SWAP` / `BTC-USD-SWAP`. We keep that
//! as the native symbol so reconciliation is direct.
//!
//! Unit: we normalize to **coins** using `oiCcy`. Stored
//! `contract_multiplier` equals the discovery `ctVal` for auditability —
//! downstream can cross-check `contracts * ctVal == coins`.
//!
//! Rate limits: `/public/*` is 20 req/2s/IP. One SWAP OI poll per minute
//! is 30 req/min — well under. Use 8 rps burst 16.

use crate::common::http::RateLimitedClient;
use async_trait::async_trait;
use oi_core::{
    error::ExchangeError,
    exchange::Exchange,
    instrument::{InstrumentId, InstrumentMeta},
    price::{PriceQuote, PriceSource},
    snapshot::RawOi,
    traits::ExchangeAdapter,
    unit::UnitKind,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use time::OffsetDateTime;
use tracing::warn;

pub const DEFAULT_BASE_URL: &str = "https://www.okx.com";

#[derive(Clone)]
pub struct OkxAdapter {
    http: RateLimitedClient,
    base_url: String,
}

impl std::fmt::Debug for OkxAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OkxAdapter")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl Default for OkxAdapter {
    fn default() -> Self {
        Self::new().expect("okx http client")
    }
}

impl OkxAdapter {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, ExchangeError> {
        let http = RateLimitedClient::new("okx", 8, 16)?;
        Ok(Self {
            http,
            base_url: base_url.into(),
        })
    }
}

// --- wire types ------------------------------------------------------------

/// The common OKX envelope. `code == "0"` means success.
#[derive(Debug, Deserialize)]
struct Envelope<T> {
    code: String,
    #[serde(default)]
    msg: String,
    // OKX always emits `data` (empty array on error). Avoid
    // `#[serde(default)]` here — it triggers an unnecessary `T: Default`
    // bound on the derive.
    data: Vec<T>,
}

impl<T> Envelope<T> {
    fn check(self) -> Result<Vec<T>, ExchangeError> {
        if self.code == "0" {
            Ok(self.data)
        } else {
            Err(ExchangeError::Schema(format!(
                "okx code={} msg={}",
                self.code, self.msg
            )))
        }
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct InstrumentRow {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "instType")]
    inst_type: String,
    #[serde(rename = "baseCcy", default)]
    base_ccy: String,
    #[serde(rename = "quoteCcy", default)]
    quote_ccy: String,
    /// For SWAP, `baseCcy` is empty — the base comes from `ctValCcy` +
    /// `settleCcy`. We derive base_asset from the instId: `BTC-USDT-SWAP`
    /// → `BTC` (first segment).
    #[serde(rename = "ctVal", default)]
    ct_val: String,
    #[serde(rename = "ctValCcy", default)]
    ct_val_ccy: String,
    #[serde(rename = "settleCcy", default)]
    settle_ccy: String,
    #[serde(rename = "tickSz", default)]
    tick_sz: String,
    #[serde(rename = "lotSz", default)]
    lot_sz: String,
    /// `"live"` | `"suspend"` | `"preopen"` | `"expired"`.
    state: String,
    /// `"linear"` | `"inverse"`. Linear SWAPs are our target set.
    #[serde(rename = "ctType", default)]
    ct_type: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OiRow {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "instType", default)]
    inst_type: String,
    /// OI in contracts.
    #[serde(default)]
    oi: String,
    /// OI in base-coin units. Preferred.
    #[serde(rename = "oiCcy", default)]
    oi_ccy: String,
    /// Server timestamp (ms).
    #[serde(default)]
    ts: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TickerRow {
    #[serde(rename = "instId")]
    inst_id: String,
    /// Last trade price (string).
    #[serde(default)]
    last: String,
    /// Mark price is on a different endpoint (`/public/mark-price`) but
    /// `last` is close enough for minute-bucket enrichment. The collector
    /// can be switched to `mark-price` later without touching this file
    /// much.
    #[serde(rename = "markPx", default)]
    mark_px: String,
    #[serde(default)]
    ts: String,
}

// --- trait impl ------------------------------------------------------------

#[async_trait]
impl ExchangeAdapter for OkxAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Okx
    }

    async fn discover_instruments(&self) -> Result<Vec<InstrumentMeta>, ExchangeError> {
        let url = format!("{}/api/v5/public/instruments?instType=SWAP", self.base_url);
        let env: Envelope<InstrumentRow> = self.http.get_json(&url).await?;
        let rows = env.check()?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            // Only linear USDT/USDC swaps. Inverse (coin-margined) swaps
            // have different OI semantics and get their own adapter later.
            if !r.ct_type.is_empty() && r.ct_type != "linear" {
                continue;
            }
            // derive base from instId: "BTC-USDT-SWAP" → "BTC"
            let base = r.inst_id.split('-').next().unwrap_or("").to_owned();
            let quote = if r.quote_ccy.is_empty() {
                r.settle_ccy.clone()
            } else {
                r.quote_ccy.clone()
            };
            let mult = if r.ct_val.is_empty() {
                None
            } else {
                Decimal::from_str(&r.ct_val).ok()
            };
            let price_tick = if r.tick_sz.is_empty() {
                None
            } else {
                Decimal::from_str(&r.tick_sz).ok()
            };
            let qty_step = if r.lot_sz.is_empty() {
                None
            } else {
                Decimal::from_str(&r.lot_sz).ok()
            };

            out.push(InstrumentMeta {
                id: InstrumentId::new(Exchange::Okx, r.inst_id),
                base_asset: base,
                quote_asset: quote,
                is_perpetual: true,
                // We normalize to Coins via `oiCcy`; the multiplier is
                // kept for cross-checks only.
                native_unit: UnitKind::Coins,
                contract_multiplier: mult,
                price_tick,
                qty_step,
                active: r.state == "live",
            });
        }
        Ok(out)
    }

    async fn fetch_oi(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> Result<Vec<RawOi>, ExchangeError> {
        let now = OffsetDateTime::now_utc();
        let url = format!("{}/api/v5/public/open-interest?instType=SWAP", self.base_url);
        let env: Envelope<OiRow> = self.http.get_json(&url).await?;
        let rows = env.check()?;

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();

        let mut out = Vec::with_capacity(wanted.len());
        for r in rows {
            if !wanted.contains(r.inst_id.as_str()) {
                continue;
            }
            // Prefer oiCcy (coins). Fall back to `oi` (contracts) only when
            // coins is empty — the adapter logs it, so we see schema drift.
            let (value, unit) = if !r.oi_ccy.is_empty() {
                match Decimal::from_str(&r.oi_ccy) {
                    Ok(v) => (v, UnitKind::Coins),
                    Err(e) => {
                        warn!(symbol=%r.inst_id, err=%e, "okx: bad oiCcy decimal");
                        continue;
                    }
                }
            } else if !r.oi.is_empty() {
                match Decimal::from_str(&r.oi) {
                    Ok(v) => (v, UnitKind::Contracts),
                    Err(e) => {
                        warn!(symbol=%r.inst_id, err=%e, "okx: bad oi decimal");
                        continue;
                    }
                }
            } else {
                continue;
            };
            out.push(RawOi {
                instrument: InstrumentId::new(Exchange::Okx, r.inst_id),
                value,
                unit,
                bucket_ts: bucket,
                recv_ts: now,
                price_hint: None,
            });
        }
        Ok(out)
    }

    async fn fetch_prices(
        &self,
        instruments: &[InstrumentId],
    ) -> Result<Vec<PriceQuote>, ExchangeError> {
        let url = format!("{}/api/v5/market/tickers?instType=SWAP", self.base_url);
        let env: Envelope<TickerRow> = self.http.get_json(&url).await?;
        let rows = env.check()?;

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();

        let mut out = Vec::new();
        for r in rows {
            if !wanted.contains(r.inst_id.as_str()) {
                continue;
            }
            // Prefer markPx if provided, otherwise last.
            let raw = if !r.mark_px.is_empty() {
                &r.mark_px
            } else if !r.last.is_empty() {
                &r.last
            } else {
                continue;
            };
            let Ok(price) = Decimal::from_str(raw) else {
                continue;
            };
            let ts = parse_ms(&r.ts).unwrap_or_else(OffsetDateTime::now_utc);
            let source = if !r.mark_px.is_empty() {
                PriceSource::Mark
            } else {
                PriceSource::Last
            };
            out.push(PriceQuote {
                instrument: InstrumentId::new(Exchange::Okx, r.inst_id),
                price,
                source,
                ts,
            });
        }
        Ok(out)
    }

    async fn fetch_funding_history(
        &self,
        instrument: &InstrumentId,
        since: Option<OffsetDateTime>,
    ) -> Result<Vec<oi_core::FundingEvent>, ExchangeError> {
        // /api/v5/public/funding-rate-history per-symbol;
        // max 100 rows per call. `before` is the cursor (rows
        // newer than this ts).
        // Docs: https://www.okx.com/docs-v5/en/#rest-api-public-data-get-funding-rate-history
        let mut url = format!(
            "{}/api/v5/public/funding-rate-history?instId={}&limit=100",
            self.base_url, instrument.symbol
        );
        if let Some(s) = since {
            let ms = s.unix_timestamp_nanos() / 1_000_000;
            url.push_str(&format!("&before={ms}"));
        }
        #[derive(Debug, Deserialize)]
        struct Row {
            #[serde(default, rename = "realizedRate")]
            realized_rate: String,
            #[serde(default, rename = "fundingRate")]
            funding_rate: String,
            #[serde(default, rename = "fundingTime")]
            funding_time: String,
        }
        let env: Envelope<Row> = self.http.get_json(&url).await?;
        let rows = env.check()?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            // Prefer realizedRate (what was actually paid) over
            // fundingRate (predicted at the boundary).
            let rate_str = if !r.realized_rate.is_empty() {
                &r.realized_rate
            } else if !r.funding_rate.is_empty() {
                &r.funding_rate
            } else {
                continue;
            };
            let Ok(rate) = Decimal::from_str(rate_str) else {
                continue;
            };
            let Some(ts) = parse_ms(&r.funding_time) else {
                continue;
            };
            out.push(oi_core::FundingEvent {
                instrument: instrument.clone(),
                settlement_ts: ts,
                rate,
                mark_price: None,
            });
        }
        Ok(out)
    }

    async fn fetch_funding(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> Result<Vec<oi_core::FundingBar>, ExchangeError> {
        // OKX has no batch funding endpoint — `/funding-rate?instId=`
        // is per-symbol. Fan out at the same bounded concurrency we
        // use for OI peers; with the public limit at 20 req/2s and
        // ~250 SWAPs the full sweep takes ~25s, well inside one
        // minute.
        use futures::{stream::FuturesUnordered, StreamExt};
        let now = OffsetDateTime::now_utc();
        let http = self.http.clone();
        let base = self.base_url.clone();

        let mut stream = FuturesUnordered::new();
        let mut iter = instruments.iter().cloned();
        let concurrency = 8usize;
        for _ in 0..concurrency {
            if let Some(inst) = iter.next() {
                let http = http.clone();
                let base = base.clone();
                stream.push(tokio::spawn(async move {
                    let r = fetch_one_funding(&http, &base, &inst.symbol).await;
                    (inst, r)
                }));
            }
        }
        let mut out = Vec::with_capacity(instruments.len());
        while let Some(res) = stream.next().await {
            if let Some(inst) = iter.next() {
                let http = http.clone();
                let base = base.clone();
                stream.push(tokio::spawn(async move {
                    let r = fetch_one_funding(&http, &base, &inst.symbol).await;
                    (inst, r)
                }));
            }
            match res {
                Ok((inst, Ok(Some((rate, next_ts))))) => {
                    out.push(oi_core::FundingBar {
                        instrument: inst,
                        bucket_ts: bucket,
                        recv_ts: now,
                        rate,
                        next_funding_ts: next_ts,
                        interval_hours: Some(8),
                    });
                }
                Ok((_, Ok(None))) => {}
                Ok((inst, Err(e))) => {
                    tracing::debug!(symbol=%inst.symbol, error=%e, "okx funding fetch failed");
                }
                Err(e) => tracing::warn!(error=%e, "okx funding task panicked"),
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct FundingRateRow {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(default, rename = "fundingRate")]
    funding_rate: String,
    #[serde(default, rename = "nextFundingTime")]
    next_funding_time: String,
    #[serde(default, rename = "fundingTime")]
    funding_time: String,
}

async fn fetch_one_funding(
    http: &crate::common::http::RateLimitedClient,
    base: &str,
    symbol: &str,
) -> Result<Option<(Decimal, Option<OffsetDateTime>)>, ExchangeError> {
    let url = format!("{base}/api/v5/public/funding-rate?instId={symbol}");
    let env: Envelope<FundingRateRow> = http.get_json(&url).await?;
    let rows = env.check()?;
    // OKX sometimes returns `data: []` for instruments mid-listing.
    let Some(r) = rows.into_iter().next() else {
        return Ok(None);
    };
    if r.funding_rate.is_empty() {
        return Ok(None);
    }
    let rate = Decimal::from_str(&r.funding_rate)
        .map_err(|e| ExchangeError::Schema(format!("okx fundingRate {symbol}: {e}")))?;
    let next_ts = parse_ms(&r.next_funding_time);
    Ok(Some((rate, next_ts)))
}

fn parse_ms(s: &str) -> Option<OffsetDateTime> {
    let ms: i64 = s.parse().ok()?;
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_success_passes_data() {
        let body = r#"{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP","oi":"1","oiCcy":"0.01","ts":"1"}]}"#;
        let env: Envelope<OiRow> = serde_json::from_str(body).unwrap();
        let rows = env.check().unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn envelope_non_zero_code_is_schema_error() {
        let body = r#"{"code":"51001","msg":"Instrument does not exist","data":[]}"#;
        let env: Envelope<OiRow> = serde_json::from_str(body).unwrap();
        let err = env.check().unwrap_err();
        assert!(matches!(err, ExchangeError::Schema(_)));
    }

    #[test]
    fn instrument_derives_base_from_instid() {
        let body = r#"{"instId":"BTC-USDT-SWAP","instType":"SWAP","baseCcy":"","quoteCcy":"USDT","ctVal":"0.01","ctValCcy":"BTC","settleCcy":"USDT","tickSz":"0.1","lotSz":"1","state":"live","ctType":"linear"}"#;
        let r: InstrumentRow = serde_json::from_str(body).unwrap();
        assert_eq!(r.inst_id, "BTC-USDT-SWAP");
        assert_eq!(r.inst_id.split('-').next().unwrap(), "BTC");
    }
}
