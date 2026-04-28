//! KuCoin Futures linear-perpetual adapter.
//!
//! `GET /api/v1/contracts/active` doubles as discovery AND the
//! minute-snapshot source — one call returns every contract with
//! current `openInterest` (contracts) and `markPrice` (USD). Same
//! single-call-per-minute pattern as Bybit and Bitget.
//! <https://www.kucoin.com/docs/rest/futures-trading/market-data/get-open-contract-list>
//!
//! Envelope: `{code, data}`. Success is `code == "200000"`.
//! Rate-limit codes (`429000` family) map to `RateLimited`; other
//! non-`200000` to `Schema`.
//!
//! Symbol format: KuCoin uses no delimiter, e.g. `XBTUSDTM` (note
//! `XBT` for BTC, trailing `M` for margin). We keep the native form.
//!
//! Unit: **Contracts** with `multiplier` (coins per contract) taken
//! from the same response. `type == "FFWCSX"` is a linear perp;
//! `FFICSX` is inverse — we filter to linear.
//!
//! Rate limits: public market-data uses KuCoin's "resource pool"
//! model; safe budget is ~3 req/s. Use 3 rps burst 6.

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

pub const DEFAULT_BASE_URL: &str = "https://api-futures.kucoin.com";

const RATE_LIMIT_CODES: &[&str] = &["429000", "429100", "429200"];

#[derive(Clone)]
pub struct KuCoinAdapter {
    http: RateLimitedClient,
    base_url: String,
    /// Per-symbol funding interval cache populated from
    /// `/api/v1/funding-rate/{symbol}/current` (which returns
    /// `granularity` in ms). Refreshed at every discovery cycle
    /// (6h).
    funding_intervals: std::sync::Arc<dashmap::DashMap<String, u8>>,
}

impl std::fmt::Debug for KuCoinAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KuCoinAdapter")
            .field("base_url", &self.base_url)
            .field("funding_intervals_known", &self.funding_intervals.len())
            .finish()
    }
}

impl Default for KuCoinAdapter {
    fn default() -> Self {
        Self::new().expect("kucoin http client")
    }
}

impl KuCoinAdapter {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, ExchangeError> {
        let http = RateLimitedClient::new("kucoin", 3, 6)?;
        Ok(Self {
            http,
            base_url: base_url.into(),
            funding_intervals: std::sync::Arc::new(dashmap::DashMap::new()),
        })
    }

    /// Refresh the funding-interval cache by hitting
    /// `/api/v1/funding-rate/{symbol}/current` per symbol. Bounded
    /// concurrency 4 — KuCoin's resource pool is tighter than
    /// MEXC's.
    async fn refresh_funding_intervals(&self, symbols: &[&str]) {
        use futures::{stream::FuturesUnordered, StreamExt};
        #[derive(Debug, Deserialize)]
        struct Inner {
            #[serde(default)]
            granularity: Option<i64>,
        }
        let mut stream = FuturesUnordered::new();
        let mut iter = symbols.iter().copied();
        for _ in 0..4usize {
            if let Some(s) = iter.next() {
                let http = self.http.clone();
                let base = self.base_url.clone();
                let sym = s.to_owned();
                stream.push(tokio::spawn(async move {
                    let url =
                        format!("{base}/api/v1/funding-rate/{sym}/current");
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
                    let url =
                        format!("{base}/api/v1/funding-rate/{sym}/current");
                    let r: Result<Envelope<Inner>, ExchangeError> = http.get_json(&url).await;
                    (sym, r)
                }));
            }
            match joined {
                Ok((sym, Ok(env))) => {
                    if let Ok(inner) = env.check() {
                        if let Some(ms) = inner.granularity {
                            // ms → hours, rounded to nearest
                            // standard interval. Clamp into
                            // 1..=24 to keep the wire-tag a u8.
                            let hours = (ms / 3_600_000).clamp(1, 24) as u8;
                            self.funding_intervals.insert(sym, hours);
                        }
                    }
                }
                Ok((sym, Err(e))) => {
                    warn!(symbol=%sym, error=%e, "kucoin: interval discovery failed");
                }
                Err(e) => warn!(error=%e, "kucoin: interval task panicked"),
            }
        }
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
        if self.code == "200000" {
            return Ok(self.data);
        }
        if RATE_LIMIT_CODES.contains(&self.code.as_str()) {
            return Err(ExchangeError::RateLimited { retry_after: None });
        }
        Err(ExchangeError::Schema(format!(
            "kucoin code={} msg={}",
            self.code, self.msg
        )))
    }
}

/// Mixed-type row: some fields are JSON strings, some are numbers.
/// We deserialize numerics as `serde_json::Number` to preserve
/// precision when converting to `Decimal`.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ContractRow {
    symbol: String,
    #[serde(default, rename = "baseCurrency")]
    base_currency: String,
    #[serde(default, rename = "quoteCurrency")]
    quote_currency: String,
    #[serde(default, rename = "settleCurrency")]
    settle_currency: String,
    /// `FFWCSX` = linear perp, `FFICSX` = inverse perp.
    #[serde(default, rename = "type")]
    contract_type: String,
    #[serde(default)]
    multiplier: Option<serde_json::Number>,
    #[serde(default, rename = "tickSize")]
    tick_size: Option<serde_json::Number>,
    #[serde(default, rename = "openInterest")]
    open_interest: String,
    #[serde(default, rename = "markPrice")]
    mark_price: Option<serde_json::Number>,
    #[serde(default, rename = "lastTradePrice")]
    last_trade_price: Option<serde_json::Number>,
    #[serde(default, rename = "indexPrice")]
    index_price: Option<serde_json::Number>,
    /// "Open" = tradable.
    #[serde(default)]
    status: String,
    #[serde(default, rename = "isInverse")]
    is_inverse: bool,
    /// Predicted funding rate for the next settlement.
    #[serde(default, rename = "fundingFeeRate")]
    funding_fee_rate: Option<serde_json::Number>,
    /// Milliseconds since epoch.
    #[serde(default, rename = "nextFundingRateTime")]
    next_funding_rate_time: Option<i64>,
}

fn number_to_decimal(n: &serde_json::Number) -> Option<Decimal> {
    Decimal::from_str(&n.to_string()).ok()
}

#[async_trait]
impl ExchangeAdapter for KuCoinAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::KuCoin
    }

    async fn discover_instruments(&self) -> Result<Vec<InstrumentMeta>, ExchangeError> {
        let url = format!("{}/api/v1/contracts/active", self.base_url);
        let env: Envelope<Vec<ContractRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;

        let metas: Vec<InstrumentMeta> = rows
            .into_iter()
            .filter(|r| !r.is_inverse)
            .filter(|r| r.contract_type.is_empty() || r.contract_type == "FFWCSX")
            .map(|r| {
                let mult = r.multiplier.as_ref().and_then(number_to_decimal);
                let price_tick = r.tick_size.as_ref().and_then(number_to_decimal);
                InstrumentMeta {
                    id: InstrumentId::new(Exchange::KuCoin, r.symbol),
                    base_asset: r.base_currency,
                    quote_asset: r.quote_currency,
                    is_perpetual: true,
                    native_unit: UnitKind::Contracts,
                    contract_multiplier: mult,
                    price_tick,
                    qty_step: None,
                    active: r.status == "Open",
                }
            })
            .collect();

        // Per-symbol funding-interval discovery — only refreshes
        // active symbols and only at this 6h cadence.
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
        let url = format!("{}/api/v1/contracts/active", self.base_url);
        let env: Envelope<Vec<ContractRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();

        let mut out = Vec::with_capacity(wanted.len());
        for r in rows {
            if !wanted.contains(r.symbol.as_str()) {
                continue;
            }
            if r.open_interest.is_empty() {
                continue;
            }
            let value = match Decimal::from_str(&r.open_interest) {
                Ok(v) => v,
                Err(e) => {
                    warn!(symbol=%r.symbol, err=%e, "kucoin: bad openInterest decimal");
                    continue;
                }
            };
            let price = r
                .mark_price
                .as_ref()
                .and_then(number_to_decimal)
                .or_else(|| r.last_trade_price.as_ref().and_then(number_to_decimal));
            let price_hint = price.map(|p| PriceQuote {
                instrument: InstrumentId::new(Exchange::KuCoin, r.symbol.clone()),
                price: p,
                source: if r.mark_price.is_some() {
                    PriceSource::Mark
                } else {
                    PriceSource::Last
                },
                ts: now,
            });
            out.push(RawOi {
                instrument: InstrumentId::new(Exchange::KuCoin, r.symbol),
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
        // /api/v1/contract/funding-rates per-symbol; KuCoin demands
        // explicit `from`/`to` in milliseconds. Default the window
        // to the last 7 days when `since` is None — enough to
        // catch up after a cold start without overwhelming the API.
        let now = OffsetDateTime::now_utc();
        let from_ts = since.unwrap_or_else(|| now - time::Duration::days(7));
        let from_ms = from_ts.unix_timestamp_nanos() / 1_000_000;
        let to_ms = now.unix_timestamp_nanos() / 1_000_000;
        let url = format!(
            "{}/api/v1/contract/funding-rates?symbol={}&from={from_ms}&to={to_ms}",
            self.base_url, instrument.symbol
        );
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct Row {
            symbol: String,
            #[serde(default, rename = "fundingRate")]
            funding_rate: Option<serde_json::Number>,
            #[serde(default)]
            timepoint: Option<i64>,
        }
        let env: Envelope<Vec<Row>> = self.http.get_json(&url).await?;
        let rows = env.check()?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let Some(rate) = r.funding_rate.as_ref().and_then(number_to_decimal) else {
                continue;
            };
            let Some(ms) = r.timepoint else { continue };
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
        // Reuses the same /api/v1/contracts/active call we already
        // poll for OI; funding rides along.
        let now = OffsetDateTime::now_utc();
        let url = format!("{}/api/v1/contracts/active", self.base_url);
        let env: Envelope<Vec<ContractRow>> = self.http.get_json(&url).await?;
        let rows = env.check()?;
        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();
        let mut out = Vec::new();
        for r in rows {
            if !wanted.contains(r.symbol.as_str()) {
                continue;
            }
            let Some(rate) = r.funding_fee_rate.as_ref().and_then(number_to_decimal) else {
                continue;
            };
            let next_funding_ts = r
                .next_funding_rate_time
                .filter(|ms| *ms > 0)
                .and_then(|ms| {
                    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
                        .ok()
                });
            let interval_hours = self
                .funding_intervals
                .get(r.symbol.as_str())
                .map(|v| *v)
                .or(Some(8));
            out.push(oi_core::FundingBar {
                instrument: InstrumentId::new(Exchange::KuCoin, r.symbol),
                bucket_ts: bucket,
                recv_ts: now,
                rate,
                next_funding_ts,
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
    fn envelope_success_code_is_six_zeros_prefix() {
        let body = r#"{"code":"200000","data":[]}"#;
        let env: Envelope<Vec<ContractRow>> = serde_json::from_str(body).unwrap();
        assert!(env.check().unwrap().is_empty());
    }

    #[test]
    fn envelope_rate_limit_code_maps_to_rate_limited() {
        let body = r#"{"code":"429000","msg":"too many","data":[]}"#;
        let env: Envelope<Vec<ContractRow>> = serde_json::from_str(body).unwrap();
        assert!(matches!(
            env.check().unwrap_err(),
            ExchangeError::RateLimited { .. }
        ));
    }

    #[test]
    fn number_to_decimal_preserves_multiplier_precision() {
        let n: Number = serde_json::from_str("0.001").unwrap();
        assert_eq!(number_to_decimal(&n), Some(dec!(0.001)));
    }
}
