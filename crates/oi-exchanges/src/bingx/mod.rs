//! BingX Perpetual Swap (v2) adapter.
//!
//! Endpoints:
//! * `GET /openApi/swap/v2/quote/contracts` — discovery of all perps.
//!   <https://bingx-api.github.io/docs/#/en-us/swapV2/market-api.html>
//! * `GET /openApi/swap/v2/quote/openInterest?symbol=BTC-USDT` —
//!   per-symbol OI (BingX has no batch OI endpoint). Same fan-out
//!   pattern as Binance.
//! * `GET /openApi/swap/v2/quote/premiumIndex` — batch mark prices for
//!   every symbol in one call.
//!
//! Envelope: `{code, msg, data}`. `code == 0` is success; `100410`
//! indicates throttling.
//!
//! Symbol format: `BASE-QUOTE` (hyphen), e.g. `BTC-USDT`.
//!
//! Unit: coins.
//!
//! Rate limits: public market-data ~100 req/10s/IP (≈ 10 rps).
//! Fan-out OI at 5 rps burst 10 to stay well inside and leave headroom
//! for the price poll.

use crate::common::http::RateLimitedClient;
use async_trait::async_trait;
use futures::{stream::FuturesUnordered, StreamExt};
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
use tracing::{debug, warn};

pub const DEFAULT_BASE_URL: &str = "https://open-api.bingx.com";

#[derive(Clone)]
pub struct BingXAdapter {
    http: RateLimitedClient,
    base_url: String,
    concurrency: usize,
}

impl std::fmt::Debug for BingXAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BingXAdapter")
            .field("base_url", &self.base_url)
            .field("concurrency", &self.concurrency)
            .finish()
    }
}

impl Default for BingXAdapter {
    fn default() -> Self {
        Self::new().expect("bingx http client")
    }
}

impl BingXAdapter {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, ExchangeError> {
        let http = RateLimitedClient::new("bingx", 5, 10)?;
        Ok(Self {
            http,
            base_url: base_url.into(),
            concurrency: 8,
        })
    }
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    code: i64,
    #[serde(default)]
    msg: String,
    data: T,
}

impl<T> Envelope<T> {
    fn check(self) -> Result<T, ExchangeError> {
        if self.code == 0 {
            return Ok(self.data);
        }
        // Documented throttle codes at the public quote endpoints.
        if matches!(self.code, 100410 | 80014 | 429) {
            return Err(ExchangeError::RateLimited { retry_after: None });
        }
        Err(ExchangeError::Schema(format!(
            "bingx code={} msg={}",
            self.code, self.msg
        )))
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ContractRow {
    symbol: String,
    #[serde(default)]
    asset: String,
    #[serde(default)]
    currency: String,
    /// 1 = online, 0 = offline.
    #[serde(default)]
    status: i32,
    #[serde(default, rename = "quantityPrecision")]
    quantity_precision: i32,
    #[serde(default, rename = "pricePrecision")]
    price_precision: i32,
    #[serde(default, rename = "tradeMinLimit")]
    trade_min_limit: Option<serde_json::Number>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OpenInterestData {
    #[serde(default)]
    symbol: String,
    #[serde(default, rename = "openInterest")]
    open_interest: String,
    #[serde(default)]
    time: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct PremiumRow {
    symbol: String,
    #[serde(default, rename = "markPrice")]
    mark_price: String,
    #[serde(default, rename = "indexPrice")]
    index_price: String,
    #[serde(default, rename = "lastFundingRate")]
    last_funding_rate: String,
    /// Milliseconds since epoch.
    #[serde(default, rename = "nextFundingTime")]
    next_funding_time: i64,
}

#[async_trait]
impl ExchangeAdapter for BingXAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::BingX
    }

    async fn discover_instruments(&self) -> Result<Vec<InstrumentMeta>, ExchangeError> {
        let url = format!("{}/openApi/swap/v2/quote/contracts", self.base_url);
        let env: Envelope<Vec<ContractRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;

        Ok(rows
            .into_iter()
            .map(|r| {
                // Derive price_tick from `pricePrecision`: 10^-pricePrecision.
                let price_tick = if r.price_precision > 0 {
                    Decimal::from_str(&format!("0.{}1", "0".repeat((r.price_precision - 1) as usize)))
                        .ok()
                } else {
                    Some(Decimal::ONE)
                };
                InstrumentMeta {
                    id: InstrumentId::new(Exchange::BingX, r.symbol),
                    base_asset: r.asset,
                    quote_asset: r.currency,
                    is_perpetual: true,
                    native_unit: UnitKind::Coins,
                    contract_multiplier: None,
                    price_tick,
                    qty_step: None,
                    active: r.status == 1,
                }
            })
            .collect())
    }

    async fn fetch_oi(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> Result<Vec<RawOi>, ExchangeError> {
        let now = OffsetDateTime::now_utc();
        let http = self.http.clone();
        let base = self.base_url.clone();

        let mut stream = FuturesUnordered::new();
        let mut iter = instruments.iter().cloned();

        for _ in 0..self.concurrency {
            if let Some(inst) = iter.next() {
                let http = http.clone();
                let base = base.clone();
                stream.push(tokio::spawn(async move {
                    (inst.clone(), fetch_one(&http, &base, &inst.symbol).await)
                }));
            }
        }

        let mut out = Vec::with_capacity(instruments.len());
        while let Some(res) = stream.next().await {
            if let Some(inst) = iter.next() {
                let http = http.clone();
                let base = base.clone();
                stream.push(tokio::spawn(async move {
                    (inst.clone(), fetch_one(&http, &base, &inst.symbol).await)
                }));
            }
            match res {
                Err(e) => warn!(error=%e, "bingx OI task panicked/cancelled"),
                Ok((inst, Ok(value))) => out.push(RawOi {
                    instrument: inst,
                    value,
                    unit: UnitKind::Coins,
                    bucket_ts: bucket,
                    recv_ts: now,
                    price_hint: None,
                }),
                Ok((inst, Err(e))) => {
                    debug!(symbol=%inst.symbol, error=%e, "bingx OI fetch failed");
                }
            }
        }
        Ok(out)
    }

    async fn fetch_prices(
        &self,
        instruments: &[InstrumentId],
    ) -> Result<Vec<PriceQuote>, ExchangeError> {
        let now = OffsetDateTime::now_utc();
        let url = format!("{}/openApi/swap/v2/quote/premiumIndex", self.base_url);
        let env: Envelope<Vec<PremiumRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();
        let mut out = Vec::new();
        for r in rows {
            if !wanted.contains(r.symbol.as_str()) {
                continue;
            }
            let Ok(price) = Decimal::from_str(&r.mark_price) else {
                continue;
            };
            out.push(PriceQuote {
                instrument: InstrumentId::new(Exchange::BingX, r.symbol),
                price,
                source: PriceSource::Mark,
                ts: now,
            });
        }
        Ok(out)
    }

    async fn fetch_funding_history(
        &self,
        instrument: &InstrumentId,
        since: Option<OffsetDateTime>,
    ) -> Result<Vec<oi_core::FundingEvent>, ExchangeError> {
        // /openApi/swap/v2/quote/fundingRate per-symbol;
        // limit up to 1000.
        // Docs: https://bingx-api.github.io/docs/#/swapV2/market-api.html
        let mut url = format!(
            "{}/openApi/swap/v2/quote/fundingRate?symbol={}&limit=1000",
            self.base_url, instrument.symbol
        );
        if let Some(s) = since {
            let ms = s.unix_timestamp_nanos() / 1_000_000;
            url.push_str(&format!("&startTime={ms}"));
        }
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct Row {
            symbol: String,
            #[serde(default, rename = "fundingRate")]
            funding_rate: String,
            #[serde(default, rename = "fundingTime")]
            funding_time: i64,
        }
        let env: Envelope<Vec<Row>> = self.http.get_json(&url).await?;
        let rows = env.check()?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let Ok(rate) = Decimal::from_str(&r.funding_rate) else {
                continue;
            };
            let Ok(ts) = OffsetDateTime::from_unix_timestamp_nanos(
                i128::from(r.funding_time) * 1_000_000,
            ) else {
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
        // Same /premiumIndex batch we already poll for prices.
        let now = OffsetDateTime::now_utc();
        let url = format!("{}/openApi/swap/v2/quote/premiumIndex", self.base_url);
        let env: Envelope<Vec<PremiumRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;
        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();
        let mut out = Vec::new();
        for r in rows {
            if !wanted.contains(r.symbol.as_str()) || r.last_funding_rate.is_empty() {
                continue;
            }
            let Ok(rate) = Decimal::from_str(&r.last_funding_rate) else {
                continue;
            };
            let next_funding_ts = if r.next_funding_time > 0 {
                OffsetDateTime::from_unix_timestamp_nanos(
                    i128::from(r.next_funding_time) * 1_000_000,
                )
                .ok()
            } else {
                None
            };
            out.push(oi_core::FundingBar {
                instrument: InstrumentId::new(Exchange::BingX, r.symbol),
                bucket_ts: bucket,
                recv_ts: now,
                rate,
                next_funding_ts,
                interval_hours: Some(8),
            });
        }
        Ok(out)
    }
}

async fn fetch_one(
    http: &RateLimitedClient,
    base: &str,
    symbol: &str,
) -> Result<Decimal, ExchangeError> {
    let url = format!("{base}/openApi/swap/v2/quote/openInterest?symbol={symbol}");
    let env: Envelope<OpenInterestData> = http.get_json(&url).await?;
    let data = env.check()?;
    Decimal::from_str(&data.open_interest)
        .map_err(|e| ExchangeError::Schema(format!("bingx openInterest {symbol}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_success_passes() {
        let body = r#"{"code":0,"msg":"","data":{"openInterest":"1","symbol":"BTC-USDT","time":1}}"#;
        let env: Envelope<OpenInterestData> = serde_json::from_str(body).unwrap();
        let d = env.check().unwrap();
        assert_eq!(d.open_interest, "1");
    }

    #[test]
    fn envelope_throttle_code_is_rate_limited() {
        let body = r#"{"code":100410,"msg":"throttle","data":{"openInterest":"","symbol":"","time":0}}"#;
        let env: Envelope<OpenInterestData> = serde_json::from_str(body).unwrap();
        assert!(matches!(env.check().unwrap_err(), ExchangeError::RateLimited { .. }));
    }
}
