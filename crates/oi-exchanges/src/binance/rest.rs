//! Binance USD-M REST adapter. See parent module docs for endpoint choices.

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

pub const DEFAULT_BASE_URL: &str = "https://fapi.binance.com";

/// Binance USD-M perpetual futures adapter.
#[derive(Clone)]
pub struct BinanceUsdmAdapter {
    http: RateLimitedClient,
    base_url: String,
    /// Parallelism for per-symbol OI polls. Binance is fine with 20–40 in
    /// flight as long as total weight/min is under the cap.
    concurrency: usize,
}

impl std::fmt::Debug for BinanceUsdmAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceUsdmAdapter")
            .field("base_url", &self.base_url)
            .field("concurrency", &self.concurrency)
            .finish()
    }
}

impl BinanceUsdmAdapter {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    /// Construct with a custom base URL. Used by integration tests to
    /// swap `fapi.binance.com` for a local `wiremock` server. In
    /// production use `new()`.
    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, ExchangeError> {
        // 30 rps avg, 40 burst. The per-minute load is ~570 sequential
        // `/fapi/v1/openInterest` calls (one per symbol — Binance has no
        // bulk current-OI endpoint; prices+funding are a single bulk
        // `/fapi/v1/premiumIndex` each, negligible). At 20 rps that was
        // ~28s/cycle with zero headroom: any latency spike pushed the
        // cycle past the 60s minute boundary, and once the collector
        // loop was behind it ran cycles back-to-back with no inter-cycle
        // sleep → sustained max-rate hammering → Binance throttling → an
        // unbounded lag spiral (observed: 21min behind and growing).
        // 30 rps → ~19s/cycle (~3x headroom under 60s) so a spike stays
        // sub-minute, the scheduler keeps sleeping between cycles, the
        // spiral never starts, and no minute is ever dropped. 30 rps is
        // ~75% of Binance's ~40/s fapi IP allowance (openInterest weight
        // 1) — deliberate margin: an IP ban is the worst data-loss event
        // and the priority here is "never lose a minute". Raise further
        // only after measuring real weight headroom under peak symbols.
        let http = RateLimitedClient::new("binance", 30, 40)?;
        Ok(Self {
            http,
            base_url: base_url.into(),
            concurrency: 16,
        })
    }

    pub fn with_concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }
}

// --- response types ---------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct SymbolInfo {
    symbol: String,
    #[serde(rename = "contractType")]
    contract_type: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    status: String,
    /// Binance publishes tick size inside the `filters` array.
    filters: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // `symbol` / `time` retained for diagnostics on schema drift.
struct OpenInterestResp {
    symbol: String,
    #[serde(rename = "openInterest")]
    open_interest: String,
    /// Milliseconds since epoch; server-reported fetch time.
    time: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct MarkPriceResp {
    symbol: String,
    #[serde(rename = "markPrice")]
    mark_price: String,
    /// Last paid funding rate. Empty string when unset
    /// (newly-listed symbols mid-cycle).
    #[serde(default, rename = "lastFundingRate")]
    last_funding_rate: String,
    /// Milliseconds since epoch — when the next settlement is.
    #[serde(default, rename = "nextFundingTime")]
    next_funding_time: i64,
    /// Milliseconds since epoch.
    time: i64,
}

// --- trait impl -------------------------------------------------------------

#[async_trait]
impl ExchangeAdapter for BinanceUsdmAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }

    async fn discover_instruments(&self) -> Result<Vec<InstrumentMeta>, ExchangeError> {
        let url = format!("{}/fapi/v1/exchangeInfo", self.base_url);
        let info: ExchangeInfo = self.http.get_json(&url).await?;

        let mut out = Vec::with_capacity(info.symbols.len());
        for s in info.symbols {
            // Binance tags dated futures CURRENT_QUARTER / NEXT_QUARTER and
            // every perpetual with a `*PERPETUAL` contractType: plain
            // "PERPETUAL" for crypto and "TRADIFI_PERPETUAL" for the
            // equity/ETF/commodity ("TradFi") perps — TSLA, NVDA, XAU, CL,
            // … — which expose OI / mark price / funding on the very same
            // endpoints. Suffix-match so we collect every perpetual class
            // (including any future `*PERPETUAL`) and skip only the dated
            // contracts; an exact "PERPETUAL" check is precisely what hid
            // the TradFi perps until now.
            if !s.contract_type.ends_with("PERPETUAL") {
                continue; // dated future — skip
            }
            let active = s.status == "TRADING";
            let (price_tick, qty_step) = extract_filters(&s.filters);
            out.push(InstrumentMeta {
                id: InstrumentId::new(Exchange::Binance, s.symbol),
                base_asset: s.base_asset,
                quote_asset: s.quote_asset,
                is_perpetual: true,
                native_unit: UnitKind::Coins,
                contract_multiplier: None,
                price_tick,
                qty_step,
                active,
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
        let http = self.http.clone();
        let base = self.base_url.clone();

        // Bounded-concurrency fan-out. FuturesUnordered keeps overhead low
        // while honoring the rate limit held by the client.
        let mut stream = FuturesUnordered::new();
        let mut iter = instruments.iter().cloned();

        // Prime the pipeline.
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
            // Push the next unit of work as soon as one completes.
            if let Some(inst) = iter.next() {
                let http = http.clone();
                let base = base.clone();
                stream.push(tokio::spawn(async move {
                    (inst.clone(), fetch_one(&http, &base, &inst.symbol).await)
                }));
            }
            match res {
                Err(join_err) => {
                    warn!(error=%join_err, "binance OI task panicked/cancelled");
                }
                Ok((inst, Ok(value))) => {
                    out.push(RawOi {
                        instrument: inst,
                        value,
                        unit: UnitKind::Coins,
                        bucket_ts: bucket,
                        recv_ts: now,
                        price_hint: None,
                    });
                }
                Ok((inst, Err(e))) => {
                    // NotFound on a single symbol is an expected delisting —
                    // skip; other errors propagate via logs.
                    debug!(symbol=%inst.symbol, error=%e, "binance OI fetch failed");
                }
            }
        }
        Ok(out)
    }

    async fn fetch_prices(
        &self,
        instruments: &[InstrumentId],
    ) -> Result<Vec<PriceQuote>, ExchangeError> {
        // Single call gets every symbol's mark price — far cheaper than
        // per-symbol polls.
        let url = format!("{}/fapi/v1/premiumIndex", self.base_url);
        let all: Vec<MarkPriceResp> = self.http.get_json(&url).await?;

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();
        let mut out = Vec::new();
        for row in all {
            if !wanted.contains(row.symbol.as_str()) {
                continue;
            }
            let price = Decimal::from_str(&row.mark_price).map_err(|e| {
                ExchangeError::Schema(format!("mark price {}: {e}", row.symbol))
            })?;
            let ts = OffsetDateTime::from_unix_timestamp_nanos(
                i128::from(row.time) * 1_000_000,
            )
            .map_err(|e| ExchangeError::Schema(format!("ts: {e}")))?;
            out.push(PriceQuote {
                instrument: InstrumentId::new(Exchange::Binance, row.symbol),
                price,
                source: PriceSource::Mark,
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
        // /fapi/v1/fundingRate is per-symbol with optional
        // startTime; weight 1, 1000-row page. Idempotent on
        // (symbol, fundingTime).
        // Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History
        let mut url = format!(
            "{}/fapi/v1/fundingRate?symbol={}&limit=1000",
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
            #[serde(default, rename = "fundingTime")]
            funding_time: i64,
            #[serde(default, rename = "markPrice")]
            mark_price: String,
        }
        let rows: Vec<Row> = self.http.get_json(&url).await?;
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
            let mark_price = if r.mark_price.is_empty() {
                None
            } else {
                Decimal::from_str(&r.mark_price).ok()
            };
            out.push(oi_core::FundingEvent {
                instrument: instrument.clone(),
                settlement_ts: ts,
                rate,
                mark_price,
            });
        }
        Ok(out)
    }

    async fn fetch_funding(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> Result<Vec<oi_core::FundingBar>, ExchangeError> {
        // /premiumIndex is the same call we use for prices; we just
        // pluck different fields out of the same payload. Binance
        // funds every 8h.
        let now = OffsetDateTime::now_utc();
        let url = format!("{}/fapi/v1/premiumIndex", self.base_url);
        let all: Vec<MarkPriceResp> = self.http.get_json(&url).await?;

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();
        let mut out = Vec::new();
        for row in all {
            if !wanted.contains(row.symbol.as_str()) {
                continue;
            }
            if row.last_funding_rate.is_empty() {
                continue;
            }
            let Ok(rate) = Decimal::from_str(&row.last_funding_rate) else {
                continue;
            };
            let next_funding_ts = if row.next_funding_time > 0 {
                OffsetDateTime::from_unix_timestamp_nanos(
                    i128::from(row.next_funding_time) * 1_000_000,
                )
                .ok()
            } else {
                None
            };
            out.push(oi_core::FundingBar {
                instrument: InstrumentId::new(Exchange::Binance, row.symbol),
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
    let url = format!("{base}/fapi/v1/openInterest?symbol={symbol}");
    let resp: OpenInterestResp = http.get_json(&url).await?;
    Decimal::from_str(&resp.open_interest)
        .map_err(|e| ExchangeError::Schema(format!("openInterest {symbol}: {e}")))
}

fn extract_filters(filters: &[serde_json::Value]) -> (Option<Decimal>, Option<Decimal>) {
    let mut tick = None;
    let mut step = None;
    for f in filters {
        match f.get("filterType").and_then(|v| v.as_str()) {
            Some("PRICE_FILTER") => {
                tick = f
                    .get("tickSize")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Decimal::from_str(s).ok());
            }
            Some("LOT_SIZE") => {
                step = f
                    .get("stepSize")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Decimal::from_str(s).ok());
            }
            _ => {}
        }
    }
    (tick, step)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn parses_exchange_info_symbol_filters() {
        let filters = serde_json::json!([
            {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
            {"filterType": "MIN_NOTIONAL", "notional": "5"},
        ]);
        let filters = filters.as_array().unwrap().clone();
        let (tick, step) = extract_filters(&filters);
        assert_eq!(tick, Some(dec!(0.10)));
        assert_eq!(step, Some(dec!(0.001)));
    }

    #[test]
    fn parses_open_interest_response_shape() {
        let body = r#"{"symbol":"BTCUSDT","openInterest":"12345.678","time":1714000000000}"#;
        let resp: OpenInterestResp = serde_json::from_str(body).unwrap();
        assert_eq!(resp.symbol, "BTCUSDT");
        assert_eq!(Decimal::from_str(&resp.open_interest).unwrap(), dec!(12345.678));
    }

    #[test]
    fn parses_mark_price_response_shape() {
        let body = r#"[{"symbol":"BTCUSDT","markPrice":"64000.50","time":1714000000000}]"#;
        let resp: Vec<MarkPriceResp> = serde_json::from_str(body).unwrap();
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0].symbol, "BTCUSDT");
    }
}
