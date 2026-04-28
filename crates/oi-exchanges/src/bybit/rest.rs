//! Bybit v5 REST adapter. See parent module docstring for endpoint choices.

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

pub const DEFAULT_BASE_URL: &str = "https://api.bybit.com";

/// Documented rate-limit codes. Anything else with `retCode != 0` is a
/// schema/contract error and should not be retried.
const RATE_LIMIT_CODES: &[&str] = &["10006", "10018", "10029"];

#[derive(Clone)]
pub struct BybitAdapter {
    http: RateLimitedClient,
    base_url: String,
}

impl std::fmt::Debug for BybitAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BybitAdapter")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl Default for BybitAdapter {
    fn default() -> Self {
        Self::new().expect("bybit http client")
    }
}

impl BybitAdapter {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, ExchangeError> {
        let http = RateLimitedClient::new("bybit", 20, 40)?;
        Ok(Self {
            http,
            base_url: base_url.into(),
        })
    }
}

// --- wire types ------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(default, rename = "retMsg")]
    ret_msg: String,
    result: T,
}

impl<T> Envelope<T> {
    fn check(self) -> Result<T, ExchangeError> {
        if self.ret_code == 0 {
            return Ok(self.result);
        }
        let code_str = self.ret_code.to_string();
        if RATE_LIMIT_CODES.contains(&code_str.as_str()) {
            return Err(ExchangeError::RateLimited { retry_after: None });
        }
        Err(ExchangeError::Schema(format!(
            "bybit retCode={} retMsg={}",
            self.ret_code, self.ret_msg
        )))
    }
}

#[derive(Debug, Deserialize)]
struct InstrumentsPage {
    list: Vec<InstrumentRow>,
    #[serde(default, rename = "nextPageCursor")]
    next_page_cursor: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct InstrumentRow {
    symbol: String,
    #[serde(default, rename = "contractType")]
    contract_type: String,
    status: String,
    #[serde(default, rename = "baseCoin")]
    base_coin: String,
    #[serde(default, rename = "quoteCoin")]
    quote_coin: String,
    #[serde(default, rename = "priceFilter")]
    price_filter: Option<PriceFilter>,
    #[serde(default, rename = "lotSizeFilter")]
    lot_size_filter: Option<LotSizeFilter>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct PriceFilter {
    #[serde(default, rename = "tickSize")]
    tick_size: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LotSizeFilter {
    #[serde(default, rename = "qtyStep")]
    qty_step: String,
}

#[derive(Debug, Deserialize)]
struct TickersResult {
    list: Vec<TickerRow>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TickerRow {
    symbol: String,
    #[serde(default, rename = "openInterest")]
    open_interest: String,
    #[serde(default, rename = "openInterestValue")]
    open_interest_value: String,
    #[serde(default, rename = "markPrice")]
    mark_price: String,
    #[serde(default, rename = "lastPrice")]
    last_price: String,
    #[serde(default, rename = "indexPrice")]
    index_price: String,
    #[serde(default, rename = "fundingRate")]
    funding_rate: String,
    /// Milliseconds since epoch as a string (Bybit-specific).
    #[serde(default, rename = "nextFundingTime")]
    next_funding_time: String,
}

// --- trait impl ------------------------------------------------------------

#[async_trait]
impl ExchangeAdapter for BybitAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Bybit
    }

    async fn discover_instruments(&self) -> Result<Vec<InstrumentMeta>, ExchangeError> {
        let mut out = Vec::new();
        let mut cursor = String::new();
        // Hard cap to defend against a misconfigured server returning
        // endless cursors.
        for _page in 0..32 {
            let mut url = format!(
                "{}/v5/market/instruments-info?category=linear&limit=1000",
                self.base_url
            );
            if !cursor.is_empty() {
                // Bybit expects URL-encoded cursors; safest form is
                // hex-ish bytes — they typically contain "%3D" padding.
                // reqwest won't re-encode query string values so we pass
                // them through verbatim.
                url.push_str("&cursor=");
                url.push_str(&cursor);
            }
            let env: Envelope<InstrumentsPage> = self.http.get_json(&url).await?;
            let page = env.check()?;

            for r in page.list {
                // Only linear perpetuals (`LinearPerpetual`). Dated
                // futures (`LinearFutures`) have fixed expiry and we
                // scope out.
                if !r.contract_type.is_empty() && r.contract_type != "LinearPerpetual" {
                    continue;
                }
                let price_tick = r
                    .price_filter
                    .as_ref()
                    .and_then(|f| Decimal::from_str(&f.tick_size).ok());
                let qty_step = r
                    .lot_size_filter
                    .as_ref()
                    .and_then(|f| Decimal::from_str(&f.qty_step).ok());
                out.push(InstrumentMeta {
                    id: InstrumentId::new(Exchange::Bybit, r.symbol),
                    base_asset: r.base_coin,
                    quote_asset: r.quote_coin,
                    is_perpetual: true,
                    native_unit: UnitKind::Coins,
                    contract_multiplier: None,
                    price_tick,
                    qty_step,
                    active: r.status == "Trading",
                });
            }

            if page.next_page_cursor.is_empty() {
                return Ok(out);
            }
            cursor = page.next_page_cursor;
        }
        warn!("bybit discovery: hit pagination safety cap; returning partial list");
        Ok(out)
    }

    async fn fetch_oi(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> Result<Vec<RawOi>, ExchangeError> {
        let now = OffsetDateTime::now_utc();
        let url = format!("{}/v5/market/tickers?category=linear", self.base_url);
        let env: Envelope<TickersResult> = self.http.get_json(&url).await?;
        let page = env.check()?;

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();

        let mut out = Vec::with_capacity(wanted.len());
        for r in page.list {
            if !wanted.contains(r.symbol.as_str()) {
                continue;
            }
            if r.open_interest.is_empty() {
                continue;
            }
            let value = match Decimal::from_str(&r.open_interest) {
                Ok(v) => v,
                Err(e) => {
                    warn!(symbol=%r.symbol, err=%e, "bybit: bad openInterest decimal");
                    continue;
                }
            };
            // Co-fetched price — preferring markPrice for perps.
            let price = if !r.mark_price.is_empty() {
                Decimal::from_str(&r.mark_price).ok()
            } else if !r.last_price.is_empty() {
                Decimal::from_str(&r.last_price).ok()
            } else {
                None
            };
            let price_hint = price.map(|p| PriceQuote {
                instrument: InstrumentId::new(Exchange::Bybit, r.symbol.clone()),
                price: p,
                source: if !r.mark_price.is_empty() {
                    PriceSource::Mark
                } else {
                    PriceSource::Last
                },
                ts: now,
            });
            out.push(RawOi {
                instrument: InstrumentId::new(Exchange::Bybit, r.symbol),
                value,
                unit: UnitKind::Coins,
                bucket_ts: bucket,
                recv_ts: now,
                price_hint,
            });
        }
        Ok(out)
    }

    async fn fetch_prices(
        &self,
        instruments: &[InstrumentId],
    ) -> Result<Vec<PriceQuote>, ExchangeError> {
        // Same tickers endpoint; included here so that collector paths
        // that ask for prices explicitly also work. Coalesce with OI
        // fetch in future if needed.
        let now = OffsetDateTime::now_utc();
        let url = format!("{}/v5/market/tickers?category=linear", self.base_url);
        let env: Envelope<TickersResult> = self.http.get_json(&url).await?;
        let page = env.check()?;

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();
        let mut out = Vec::new();
        for r in page.list {
            if !wanted.contains(r.symbol.as_str()) {
                continue;
            }
            let raw = if !r.mark_price.is_empty() {
                &r.mark_price
            } else if !r.last_price.is_empty() {
                &r.last_price
            } else {
                continue;
            };
            let Ok(price) = Decimal::from_str(raw) else {
                continue;
            };
            let source = if !r.mark_price.is_empty() {
                PriceSource::Mark
            } else {
                PriceSource::Last
            };
            out.push(PriceQuote {
                instrument: InstrumentId::new(Exchange::Bybit, r.symbol),
                price,
                source,
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
        // /v5/market/funding/history is per-symbol; max 200 rows.
        // Bybit doesn't co-publish mark price on this endpoint —
        // mark_price stays None.
        // Docs: https://bybit-exchange.github.io/docs/v5/market/history-fund-rate
        let mut url = format!(
            "{}/v5/market/funding/history?category=linear&symbol={}&limit=200",
            self.base_url, instrument.symbol
        );
        if let Some(s) = since {
            let ms = s.unix_timestamp_nanos() / 1_000_000;
            url.push_str(&format!("&startTime={ms}"));
        }
        #[derive(Debug, Deserialize)]
        struct Row {
            #[serde(default, rename = "fundingRate")]
            funding_rate: String,
            #[serde(default, rename = "fundingRateTimestamp")]
            funding_rate_timestamp: String,
        }
        #[derive(Debug, Deserialize)]
        struct History {
            #[serde(default)]
            list: Vec<Row>,
        }
        let env: Envelope<History> = self.http.get_json(&url).await?;
        let history = env.check()?;
        let mut out = Vec::with_capacity(history.list.len());
        for r in history.list {
            let Ok(rate) = Decimal::from_str(&r.funding_rate) else {
                continue;
            };
            let Ok(ms) = r.funding_rate_timestamp.parse::<i64>() else {
                continue;
            };
            let Ok(ts) =
                OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
            else {
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
        // Reuses the same /v5/market/tickers call we already make
        // for OI + price; funding rides along.
        let now = OffsetDateTime::now_utc();
        let url = format!("{}/v5/market/tickers?category=linear", self.base_url);
        let env: Envelope<TickersResult> = self.http.get_json(&url).await?;
        let page = env.check()?;

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();

        let mut out = Vec::new();
        for r in page.list {
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
                instrument: InstrumentId::new(Exchange::Bybit, r.symbol),
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
    fn envelope_success_passes_through() {
        let body = r#"{"retCode":0,"retMsg":"OK","result":{"list":[]}}"#;
        let env: Envelope<TickersResult> = serde_json::from_str(body).unwrap();
        let res = env.check().unwrap();
        assert!(res.list.is_empty());
    }

    #[test]
    fn envelope_known_rate_limit_code_maps_to_rate_limited() {
        let body = r#"{"retCode":10006,"retMsg":"too many requests","result":{"list":[]}}"#;
        let env: Envelope<TickersResult> = serde_json::from_str(body).unwrap();
        let err = env.check().unwrap_err();
        assert!(matches!(err, ExchangeError::RateLimited { .. }));
    }

    #[test]
    fn envelope_unknown_nonzero_code_is_schema_error() {
        let body = r#"{"retCode":110025,"retMsg":"whatever","result":{"list":[]}}"#;
        let env: Envelope<TickersResult> = serde_json::from_str(body).unwrap();
        let err = env.check().unwrap_err();
        assert!(matches!(err, ExchangeError::Schema(_)));
    }

    #[test]
    fn instrument_row_parses_with_optional_filters() {
        let body = r#"{"symbol":"BTCUSDT","contractType":"LinearPerpetual","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT"}"#;
        let r: InstrumentRow = serde_json::from_str(body).unwrap();
        assert_eq!(r.symbol, "BTCUSDT");
        assert!(r.price_filter.is_none());
    }
}
