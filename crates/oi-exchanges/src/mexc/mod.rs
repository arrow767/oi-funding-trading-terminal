//! MEXC Contract (linear USDT-M perp) adapter.
//!
//! Endpoints:
//! * `GET /api/v1/contract/detail` — discovery. Returns every perp with
//!   `contractSize` (coins per contract, as a JSON number).
//!   <https://mexcdevelop.github.io/apidocs/contract_v1_en/#get-all-contract-information>
//! * `GET /api/v1/contract/ticker` — **batch** ticker for the full
//!   universe. Field `holdVol` is OI in **contracts**; `fairPrice` is
//!   mark-like.
//!   <https://mexcdevelop.github.io/apidocs/contract_v1_en/#contract-information>
//!
//! Envelope: `{success, code, data}`. Success is `code == 0`. MEXC
//! numerics (`contractSize`, `holdVol`, `fairPrice`) are JSON numbers,
//! not strings — we parse via the number's canonical string form to
//! keep `Decimal` precision.
//!
//! Symbol format: `BASE_QUOTE` (underscore), e.g. `BTC_USDT`.
//!
//! Unit: **Contracts**; `InstrumentMeta.contract_multiplier` carries
//! `contractSize`. The core `UnitKind::to_coins` / `to_usd` handle
//! the multiplication.
//!
//! Rate limits: 20 req/s/IP for market-data. Use 8 rps burst 16.

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

pub const DEFAULT_BASE_URL: &str = "https://contract.mexc.com";

#[derive(Clone)]
pub struct MexcAdapter {
    http: RateLimitedClient,
    base_url: String,
    /// Per-symbol funding interval cache populated lazily from
    /// `/api/v1/contract/funding_rate/{symbol}`. Refreshed on
    /// every `discover_instruments` so contracts whose cycle
    /// changes (rare but possible) re-converge within 6h.
    funding_intervals: std::sync::Arc<dashmap::DashMap<String, u8>>,
}

impl std::fmt::Debug for MexcAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MexcAdapter")
            .field("base_url", &self.base_url)
            .field("funding_intervals_known", &self.funding_intervals.len())
            .finish()
    }
}

impl Default for MexcAdapter {
    fn default() -> Self {
        Self::new().expect("mexc http client")
    }
}

impl MexcAdapter {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, ExchangeError> {
        let http = RateLimitedClient::new("mexc", 8, 16)?;
        Ok(Self {
            http,
            base_url: base_url.into(),
            funding_intervals: std::sync::Arc::new(dashmap::DashMap::new()),
        })
    }

    /// Populate the funding-interval cache for the given symbols.
    /// Per-symbol API call (~250 calls × 8 rps ≈ 30s for the
    /// whole universe) — runs only at discovery (every 6h),
    /// never per-minute. Best-effort: failures are logged and the
    /// affected symbol falls back to the heuristic default.
    async fn refresh_funding_intervals(&self, symbols: &[&str]) {
        use futures::{stream::FuturesUnordered, StreamExt};
        #[derive(Debug, Deserialize)]
        struct Inner {
            #[serde(default, rename = "collectCycle")]
            collect_cycle: Option<u8>,
        }
        let mut stream = FuturesUnordered::new();
        let mut iter = symbols.iter().copied();
        for _ in 0..8usize {
            if let Some(s) = iter.next() {
                let http = self.http.clone();
                let base = self.base_url.clone();
                let sym = s.to_owned();
                stream.push(tokio::spawn(async move {
                    let url = format!("{base}/api/v1/contract/funding_rate/{sym}");
                    let r: Result<Envelope<Inner>, ExchangeError> = http.get_json(&url).await;
                    (sym, r)
                }));
            }
        }
        while let Some(joined) = stream.next().await {
            if let Some(s) = iter.next() {
                let http = self.http.clone();
                let base = self.base_url.clone();
                let sym = s.to_owned();
                stream.push(tokio::spawn(async move {
                    let url = format!("{base}/api/v1/contract/funding_rate/{sym}");
                    let r: Result<Envelope<Inner>, ExchangeError> = http.get_json(&url).await;
                    (sym, r)
                }));
            }
            match joined {
                Ok((sym, Ok(env))) => {
                    if let Ok(inner) = env.check() {
                        if let Some(c) = inner.collect_cycle {
                            self.funding_intervals.insert(sym, c);
                        }
                    }
                }
                Ok((sym, Err(e))) => {
                    warn!(symbol=%sym, error=%e, "mexc: funding interval discovery failed");
                }
                Err(e) => warn!(error=%e, "mexc: interval discovery task panicked"),
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    #[serde(default)]
    success: bool,
    code: i64,
    data: T,
}

impl<T> Envelope<T> {
    fn check(self) -> Result<T, ExchangeError> {
        if self.code == 0 && self.success {
            return Ok(self.data);
        }
        if self.code == 510 || self.code == 429 {
            return Err(ExchangeError::RateLimited { retry_after: None });
        }
        Err(ExchangeError::Schema(format!("mexc code={}", self.code)))
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct DetailRow {
    symbol: String,
    #[serde(default, rename = "baseCoin")]
    base_coin: String,
    #[serde(default, rename = "quoteCoin")]
    quote_coin: String,
    /// Coins per contract. Always a JSON number, small
    /// (e.g. 0.0001 for BTC, 0.01 for ETH).
    #[serde(default, rename = "contractSize")]
    contract_size: Option<serde_json::Number>,
    #[serde(default, rename = "priceUnit")]
    price_unit: Option<serde_json::Number>,
    #[serde(default, rename = "volUnit")]
    vol_unit: Option<serde_json::Number>,
    /// 0 = normal, 1 = pending delist, 2 = delisted (per docs).
    #[serde(default)]
    state: i32,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TickerRow {
    symbol: String,
    #[serde(default, rename = "holdVol")]
    hold_vol: Option<serde_json::Number>,
    #[serde(default, rename = "fairPrice")]
    fair_price: Option<serde_json::Number>,
    #[serde(default, rename = "lastPrice")]
    last_price: Option<serde_json::Number>,
    #[serde(default, rename = "indexPrice")]
    index_price: Option<serde_json::Number>,
    /// Predicted funding rate for the next settlement.
    #[serde(default, rename = "fundingRate")]
    funding_rate: Option<serde_json::Number>,
    #[serde(default)]
    timestamp: Option<i64>,
}

fn number_to_decimal(n: &serde_json::Number) -> Option<Decimal> {
    // `to_string()` gives the number's canonical JSON representation
    // — so `0.0001` stays `"0.0001"`, not an f64-widened form.
    Decimal::from_str(&n.to_string()).ok()
}

#[async_trait]
impl ExchangeAdapter for MexcAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Mexc
    }

    async fn discover_instruments(&self) -> Result<Vec<InstrumentMeta>, ExchangeError> {
        let url = format!("{}/api/v1/contract/detail", self.base_url);
        let env: Envelope<Vec<DetailRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;

        let metas: Vec<InstrumentMeta> = rows
            .into_iter()
            .map(|r| {
                let mult = r.contract_size.as_ref().and_then(number_to_decimal);
                let price_tick = r.price_unit.as_ref().and_then(number_to_decimal);
                let qty_step = r.vol_unit.as_ref().and_then(number_to_decimal);
                InstrumentMeta {
                    id: InstrumentId::new(Exchange::Mexc, r.symbol),
                    base_asset: r.base_coin,
                    quote_asset: r.quote_coin,
                    is_perpetual: true,
                    native_unit: UnitKind::Contracts,
                    contract_multiplier: mult,
                    price_tick,
                    qty_step,
                    active: r.state == 0,
                }
            })
            .collect();

        // Refresh the funding-interval cache for active symbols.
        // Async fan-out — only runs at discovery cadence (6h), so
        // the per-symbol cost amortises to nothing.
        let active_syms: Vec<&str> = metas
            .iter()
            .filter(|m| m.active)
            .map(|m| m.id.symbol.as_str())
            .collect();
        self.refresh_funding_intervals(&active_syms).await;

        Ok(metas)
    }

    async fn fetch_oi(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> Result<Vec<RawOi>, ExchangeError> {
        let now = OffsetDateTime::now_utc();
        let url = format!("{}/api/v1/contract/ticker", self.base_url);
        let env: Envelope<Vec<TickerRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();

        let mut out = Vec::with_capacity(wanted.len());
        for r in rows {
            if !wanted.contains(r.symbol.as_str()) {
                continue;
            }
            let Some(n) = r.hold_vol.as_ref() else {
                continue;
            };
            let value = match number_to_decimal(n) {
                Some(v) => v,
                None => {
                    warn!(symbol=%r.symbol, "mexc: bad holdVol number");
                    continue;
                }
            };
            let price = r
                .fair_price
                .as_ref()
                .and_then(number_to_decimal)
                .or_else(|| r.last_price.as_ref().and_then(number_to_decimal));
            let price_hint = price.map(|p| PriceQuote {
                instrument: InstrumentId::new(Exchange::Mexc, r.symbol.clone()),
                price: p,
                source: if r.fair_price.is_some() {
                    PriceSource::Mark
                } else {
                    PriceSource::Last
                },
                ts: now,
            });
            out.push(RawOi {
                instrument: InstrumentId::new(Exchange::Mexc, r.symbol),
                value,
                unit: UnitKind::Contracts,
                bucket_ts: bucket,
                recv_ts: now,
                price_hint,
            });
        }
        Ok(out)
    }

    async fn fetch_funding_history(
        &self,
        instrument: &InstrumentId,
        since: Option<OffsetDateTime>,
    ) -> Result<Vec<oi_core::FundingEvent>, ExchangeError> {
        // /api/v1/contract/funding_rate/history per-symbol; 100/page.
        // Docs: https://mexcdevelop.github.io/apidocs/contract_v1_en/#query-funding-rate-history
        let url = format!(
            "{}/api/v1/contract/funding_rate/history?symbol={}&page_num=1&page_size=100",
            self.base_url, instrument.symbol
        );
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct Row {
            #[serde(default)]
            symbol: String,
            #[serde(default, rename = "fundingRate")]
            funding_rate: Option<serde_json::Number>,
            #[serde(default, rename = "settleTime")]
            settle_time: Option<i64>,
        }
        #[derive(Debug, Deserialize)]
        struct Page {
            #[serde(default, rename = "resultList")]
            result_list: Vec<Row>,
        }
        let env: Envelope<Page> = self.http.get_json(&url).await?;
        let page = env.check()?;
        let mut out = Vec::with_capacity(page.result_list.len());
        for r in page.result_list {
            let Some(rate) = r.funding_rate.as_ref().and_then(number_to_decimal) else {
                continue;
            };
            let Some(ms) = r.settle_time else { continue };
            let Ok(ts) =
                OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
            else {
                continue;
            };
            if let Some(s) = since {
                if ts <= s {
                    continue;
                }
            }
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
        // Reuses the same /api/v1/contract/ticker batch we already
        // poll for OI; funding rides along.
        let now = OffsetDateTime::now_utc();
        let url = format!("{}/api/v1/contract/ticker", self.base_url);
        let env: Envelope<Vec<TickerRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;
        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();
        let mut out = Vec::new();
        for r in rows {
            if !wanted.contains(r.symbol.as_str()) {
                continue;
            }
            let Some(rate) = r.funding_rate.as_ref().and_then(number_to_decimal) else {
                continue;
            };
            // Real interval from the discovery-time cache;
            // falls back to 8h only when the funding-rate
            // endpoint failed for this symbol on the last
            // refresh.
            let interval_hours = self
                .funding_intervals
                .get(r.symbol.as_str())
                .map(|v| *v)
                .or(Some(8));
            out.push(oi_core::FundingBar {
                instrument: InstrumentId::new(Exchange::Mexc, r.symbol),
                bucket_ts: bucket,
                recv_ts: now,
                rate,
                // MEXC publishes fundingRate but next-settle is
                // on a different endpoint — leave None and let
                // the client infer from the rate's roll cadence.
                next_funding_ts: None,
                interval_hours,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json::Number;

    #[test]
    fn number_to_decimal_preserves_small_contract_size() {
        let n: Number = serde_json::from_str("0.0001").unwrap();
        assert_eq!(number_to_decimal(&n), Some(dec!(0.0001)));
    }

    #[test]
    fn number_to_decimal_preserves_integers() {
        let n: Number = serde_json::from_str("123456").unwrap();
        assert_eq!(number_to_decimal(&n), Some(dec!(123456)));
    }

    #[test]
    fn envelope_success_requires_code_zero_and_success_true() {
        let body = r#"{"success":true,"code":0,"data":[]}"#;
        let env: Envelope<Vec<TickerRow>> = serde_json::from_str(body).unwrap();
        assert!(env.check().unwrap().is_empty());
    }

    #[test]
    fn envelope_rate_limit_code_maps_to_rate_limited() {
        let body = r#"{"success":false,"code":510,"data":[]}"#;
        let env: Envelope<Vec<TickerRow>> = serde_json::from_str(body).unwrap();
        assert!(matches!(
            env.check().unwrap_err(),
            ExchangeError::RateLimited { .. }
        ));
    }

    #[test]
    fn envelope_generic_failure_is_schema_error() {
        let body = r#"{"success":false,"code":1002,"data":[]}"#;
        let env: Envelope<Vec<TickerRow>> = serde_json::from_str(body).unwrap();
        assert!(matches!(env.check().unwrap_err(), ExchangeError::Schema(_)));
    }
}
