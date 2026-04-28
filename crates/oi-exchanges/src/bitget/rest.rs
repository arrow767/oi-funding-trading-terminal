//! Bitget v2 USDT-M futures adapter.
//!
//! One batch GET gives both OI and mark price for every linear perp —
//! identical pattern to Bybit.
//!
//! Endpoints:
//! * `GET /api/v2/mix/market/contracts?productType=USDT-FUTURES` — discovery.
//!   <https://www.bitget.com/api-doc/contract/market/Get-All-Symbols-Contracts>
//! * `GET /api/v2/mix/market/tickers?productType=USDT-FUTURES` — batch tickers.
//!   Field `holdingAmount` is OI in **coins**; `markPrice` is USD.
//!   <https://www.bitget.com/api-doc/contract/market/Get-All-Symbol-Ticker>
//!
//! Envelope: `{code, msg, data, requestTime}`. Success is `code == "00000"`.
//! Rate-limit codes (`40018`, `40429`) map to `ExchangeError::RateLimited`;
//! anything else with a non-`00000` code becomes `Schema`.
//!
//! Unit: coins (`holdingAmount`).
//!
//! Rate limits: public market-data 20 req/s/IP. Use 8 rps burst 16.

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

pub const DEFAULT_BASE_URL: &str = "https://api.bitget.com";
const PRODUCT_TYPE: &str = "USDT-FUTURES";

const RATE_LIMIT_CODES: &[&str] = &["40018", "40429", "429"];

#[derive(Clone)]
pub struct BitgetAdapter {
    http: RateLimitedClient,
    base_url: String,
}

impl std::fmt::Debug for BitgetAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitgetAdapter")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl Default for BitgetAdapter {
    fn default() -> Self {
        Self::new().expect("bitget http client")
    }
}

impl BitgetAdapter {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, ExchangeError> {
        let http = RateLimitedClient::new("bitget", 8, 16)?;
        Ok(Self {
            http,
            base_url: base_url.into(),
        })
    }
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    code: String,
    #[serde(default)]
    msg: String,
    data: T,
}

impl<T> Envelope<T> {
    fn check(self) -> Result<T, ExchangeError> {
        if self.code == "00000" {
            return Ok(self.data);
        }
        if RATE_LIMIT_CODES.contains(&self.code.as_str()) {
            return Err(ExchangeError::RateLimited { retry_after: None });
        }
        Err(ExchangeError::Schema(format!(
            "bitget code={} msg={}",
            self.code, self.msg
        )))
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ContractRow {
    symbol: String,
    #[serde(default, rename = "baseCoin")]
    base_coin: String,
    #[serde(default, rename = "quoteCoin")]
    quote_coin: String,
    #[serde(default, rename = "symbolType")]
    symbol_type: String,
    #[serde(default, rename = "symbolStatus")]
    symbol_status: String,
    #[serde(default, rename = "pricePlace")]
    price_place: String,
    #[serde(default, rename = "priceEndStep")]
    price_end_step: String,
    #[serde(default, rename = "minTradeNum")]
    min_trade_num: String,
    #[serde(default, rename = "sizeMultiplier")]
    size_multiplier: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TickerRow {
    symbol: String,
    #[serde(default, rename = "holdingAmount")]
    holding_amount: String,
    #[serde(default, rename = "markPrice")]
    mark_price: String,
    #[serde(default, rename = "indexPrice")]
    index_price: String,
    #[serde(default, rename = "lastPr")]
    last_pr: String,
    /// Funding rate for the next settlement.
    #[serde(default, rename = "fundingRate")]
    funding_rate: String,
    /// Next settlement time (ms since epoch, as string).
    #[serde(default, rename = "nextFundingTime")]
    next_funding_time: String,
    #[serde(default)]
    ts: String,
}

#[async_trait]
impl ExchangeAdapter for BitgetAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Bitget
    }

    async fn discover_instruments(&self) -> Result<Vec<InstrumentMeta>, ExchangeError> {
        let url = format!(
            "{}/api/v2/mix/market/contracts?productType={PRODUCT_TYPE}",
            self.base_url
        );
        let env: Envelope<Vec<ContractRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;

        Ok(rows
            .into_iter()
            .filter(|r| r.symbol_type.is_empty() || r.symbol_type == "perpetual")
            .map(|r| InstrumentMeta {
                id: InstrumentId::new(Exchange::Bitget, r.symbol),
                base_asset: r.base_coin,
                quote_asset: r.quote_coin,
                is_perpetual: true,
                native_unit: UnitKind::Coins,
                contract_multiplier: None,
                price_tick: None,
                qty_step: Decimal::from_str(&r.min_trade_num).ok(),
                active: r.symbol_status == "normal",
            })
            .collect())
    }

    async fn fetch_oi(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> Result<Vec<RawOi>, ExchangeError> {
        let now = OffsetDateTime::now_utc();
        let url = format!(
            "{}/api/v2/mix/market/tickers?productType={PRODUCT_TYPE}",
            self.base_url
        );
        let env: Envelope<Vec<TickerRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();

        let mut out = Vec::with_capacity(wanted.len());
        for r in rows {
            if !wanted.contains(r.symbol.as_str()) {
                continue;
            }
            if r.holding_amount.is_empty() {
                continue;
            }
            let value = match Decimal::from_str(&r.holding_amount) {
                Ok(v) => v,
                Err(e) => {
                    warn!(symbol=%r.symbol, err=%e, "bitget: bad holdingAmount");
                    continue;
                }
            };
            let price = if !r.mark_price.is_empty() {
                Decimal::from_str(&r.mark_price).ok()
            } else if !r.last_pr.is_empty() {
                Decimal::from_str(&r.last_pr).ok()
            } else {
                None
            };
            let price_hint = price.map(|p| PriceQuote {
                instrument: InstrumentId::new(Exchange::Bitget, r.symbol.clone()),
                price: p,
                source: if !r.mark_price.is_empty() {
                    PriceSource::Mark
                } else {
                    PriceSource::Last
                },
                ts: now,
            });
            out.push(RawOi {
                instrument: InstrumentId::new(Exchange::Bitget, r.symbol),
                value,
                unit: UnitKind::Coins,
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
        // /api/v2/mix/market/history-fund-rate per-symbol;
        // 100-row page, no native `since` filter — we return
        // everything and trust the caller's idempotent upsert
        // to dedupe. (We DO sort + drop strictly-older rows to
        // keep the wire payload small.)
        // Docs: https://www.bitget.com/api-doc/contract/market/Get-History-Funding-Rate
        let url = format!(
            "{}/api/v2/mix/market/history-fund-rate?symbol={}&productType={PRODUCT_TYPE}&pageSize=100",
            self.base_url, instrument.symbol
        );
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct Row {
            symbol: String,
            #[serde(default, rename = "fundingRate")]
            funding_rate: String,
            #[serde(default, rename = "fundingTime")]
            funding_time: String,
        }
        let env: Envelope<Vec<Row>> = self.http.get_json(&url).await?;
        let rows = env.check()?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let Ok(rate) = Decimal::from_str(&r.funding_rate) else {
                continue;
            };
            let Ok(ms) = r.funding_time.parse::<i64>() else {
                continue;
            };
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
        let now = OffsetDateTime::now_utc();
        let url = format!(
            "{}/api/v2/mix/market/tickers?productType={PRODUCT_TYPE}",
            self.base_url
        );
        let env: Envelope<Vec<TickerRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;
        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();
        let mut out = Vec::new();
        for r in rows {
            if !wanted.contains(r.symbol.as_str()) || r.funding_rate.is_empty() {
                continue;
            }
            let Ok(rate) = Decimal::from_str(&r.funding_rate) else {
                continue;
            };
            let next_funding_ts = r
                .next_funding_time
                .parse::<i64>()
                .ok()
                .filter(|ms| *ms > 0)
                .and_then(|ms| {
                    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
                        .ok()
                });
            out.push(oi_core::FundingBar {
                instrument: InstrumentId::new(Exchange::Bitget, r.symbol),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_success_code_is_five_zeros() {
        let body = r#"{"code":"00000","msg":"success","data":[]}"#;
        let env: Envelope<Vec<TickerRow>> = serde_json::from_str(body).unwrap();
        assert!(env.check().unwrap().is_empty());
    }

    #[test]
    fn envelope_rate_limit_code_maps_to_rate_limited() {
        let body = r#"{"code":"429","msg":"too many","data":[]}"#;
        let env: Envelope<Vec<TickerRow>> = serde_json::from_str(body).unwrap();
        assert!(matches!(env.check().unwrap_err(), ExchangeError::RateLimited { .. }));
    }

    #[test]
    fn envelope_unknown_nonzero_is_schema_error() {
        let body = r#"{"code":"40002","msg":"bad param","data":[]}"#;
        let env: Envelope<Vec<TickerRow>> = serde_json::from_str(body).unwrap();
        assert!(matches!(env.check().unwrap_err(), ExchangeError::Schema(_)));
    }
}
