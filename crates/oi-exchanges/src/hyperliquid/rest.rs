//! Hyperliquid perpetual adapter.
//!
//! One POST `/info {"type":"metaAndAssetCtxs"}` returns the full universe
//! plus every asset's current context (OI, markPx, oraclePx, funding).
//! That single call IS our discovery AND our minute snapshot — one of the
//! simplest and cheapest adapters in the platform.
//!
//! Docs:
//! <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals>
//!
//! Response shape (simplified — real docs have more fields):
//! ```json
//! [
//!   { "universe": [ {"name": "BTC", "szDecimals": 5, "maxLeverage": 50}, ... ] },
//!   [ {"openInterest":"1234.5","markPx":"64000.0","oraclePx":"63999.0", ...}, ... ]
//! ]
//! ```
//! The two arrays are **index-parallel** — `universe[i]` describes
//! `assetCtxs[i]`. We zip them.
//!
//! Symbol format: the exchange exposes a coin name (`"BTC"`, `"ETH"`).
//! We use that as the symbol; users wanting the full pair
//! ("BTC-USD-PERP") derive it from `base_asset + quote_asset`.
//!
//! Unit: OI is in **coins** (base asset). Prices in USD.
//!
//! Rate limits: 1200 req/min per IP. One POST per minute is trivial.
//! Keep 5 rps / burst 10 to match the rest of the platform.

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
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};
use time::OffsetDateTime;
use tracing::warn;

pub const DEFAULT_BASE_URL: &str = "https://api.hyperliquid.xyz";

#[derive(Clone)]
pub struct HyperliquidAdapter {
    http: RateLimitedClient,
    base_url: String,
}

impl std::fmt::Debug for HyperliquidAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperliquidAdapter")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl Default for HyperliquidAdapter {
    fn default() -> Self {
        Self::new().expect("hyperliquid http client")
    }
}

impl HyperliquidAdapter {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, ExchangeError> {
        let http = RateLimitedClient::new("hyperliquid", 5, 10)?;
        Ok(Self {
            http,
            base_url: base_url.into(),
        })
    }

    /// Fetch `metaAndAssetCtxs` in one call. Shared by discovery & OI paths —
    /// splitting them would double the call count for no gain.
    async fn meta_and_ctxs(&self) -> Result<(MetaUniverse, Vec<AssetCtx>), ExchangeError> {
        let url = format!("{}/info", self.base_url);
        let body = InfoRequest {
            request_type: "metaAndAssetCtxs",
        };
        let resp: MetaAndCtxsResp = self.http.post_json(&url, &body).await?;
        match resp {
            MetaAndCtxsResp::Tuple(meta, ctxs) => Ok((meta, ctxs)),
        }
    }
}

// --- wire types ------------------------------------------------------------

#[derive(Serialize)]
struct InfoRequest {
    #[serde(rename = "type")]
    request_type: &'static str,
}

/// The response is a positional tuple `[meta, ctxs]`. serde(untagged) over a
/// single variant models that cleanly.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MetaAndCtxsResp {
    Tuple(MetaUniverse, Vec<AssetCtx>),
}

#[derive(Debug, Deserialize)]
struct MetaUniverse {
    universe: Vec<UniverseEntry>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct UniverseEntry {
    name: String,
    #[serde(rename = "szDecimals")]
    sz_decimals: u8,
    #[serde(default, rename = "maxLeverage")]
    max_leverage: Option<u32>,
    /// When true, the asset was delisted.
    #[serde(default, rename = "isDelisted")]
    is_delisted: bool,
    #[serde(default, rename = "onlyIsolated")]
    only_isolated: bool,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AssetCtx {
    #[serde(default, rename = "openInterest")]
    open_interest: Option<String>,
    #[serde(default, rename = "markPx")]
    mark_px: Option<String>,
    #[serde(default, rename = "oraclePx")]
    oracle_px: Option<String>,
    #[serde(default, rename = "midPx")]
    mid_px: Option<String>,
    #[serde(default)]
    funding: Option<String>,
    #[serde(default, rename = "prevDayPx")]
    prev_day_px: Option<String>,
    #[serde(default, rename = "dayNtlVlm")]
    day_ntl_vlm: Option<String>,
    #[serde(default)]
    premium: Option<String>,
}

// --- trait impl ------------------------------------------------------------

#[async_trait]
impl ExchangeAdapter for HyperliquidAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Hyperliquid
    }

    async fn discover_instruments(&self) -> Result<Vec<InstrumentMeta>, ExchangeError> {
        let (meta, _) = self.meta_and_ctxs().await?;
        Ok(meta
            .universe
            .into_iter()
            .map(|u| InstrumentMeta {
                id: InstrumentId::new(Exchange::Hyperliquid, u.name.clone()),
                base_asset: u.name,
                quote_asset: "USD".into(),
                is_perpetual: true,
                native_unit: UnitKind::Coins,
                contract_multiplier: None,
                price_tick: None,
                qty_step: None,
                active: !u.is_delisted,
            })
            .collect())
    }

    async fn fetch_oi(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> Result<Vec<RawOi>, ExchangeError> {
        let now = OffsetDateTime::now_utc();
        let (meta, ctxs) = self.meta_and_ctxs().await?;
        if meta.universe.len() != ctxs.len() {
            return Err(ExchangeError::Schema(format!(
                "universe/ctxs length mismatch: {} vs {}",
                meta.universe.len(),
                ctxs.len()
            )));
        }

        // Build a name -> (ctx, price) index for O(1) lookup.
        let mut by_name: HashMap<String, (&AssetCtx, Option<Decimal>)> =
            HashMap::with_capacity(meta.universe.len());
        for (u, c) in meta.universe.iter().zip(ctxs.iter()) {
            let price = c
                .mark_px
                .as_ref()
                .and_then(|s| Decimal::from_str(s).ok());
            by_name.insert(u.name.clone(), (c, price));
        }

        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();

        let mut out = Vec::with_capacity(wanted.len());
        for (name, (ctx, price)) in &by_name {
            if !wanted.contains(name.as_str()) {
                continue;
            }
            let Some(oi_s) = ctx.open_interest.as_ref() else {
                warn!(symbol=%name, "hyperliquid: missing openInterest");
                continue;
            };
            let value = match Decimal::from_str(oi_s) {
                Ok(v) => v,
                Err(e) => {
                    warn!(symbol=%name, error=%e, "hyperliquid: bad OI decimal");
                    continue;
                }
            };
            out.push(RawOi {
                instrument: InstrumentId::new(Exchange::Hyperliquid, name.clone()),
                value,
                unit: UnitKind::Coins,
                bucket_ts: bucket,
                recv_ts: now,
                price_hint: price.map(|p| PriceQuote {
                    instrument: InstrumentId::new(Exchange::Hyperliquid, name.clone()),
                    price: p,
                    source: PriceSource::Mark,
                    ts: now,
                }),
            });
        }
        Ok(out)
    }

    async fn fetch_prices(
        &self,
        instruments: &[InstrumentId],
    ) -> Result<Vec<PriceQuote>, ExchangeError> {
        // The OI call already returns prices co-located with OI; we only
        // hit this endpoint when the collector asks specifically for
        // prices (e.g. on a price-only refresh tick). Same cheap POST.
        let now = OffsetDateTime::now_utc();
        let (meta, ctxs) = self.meta_and_ctxs().await?;
        if meta.universe.len() != ctxs.len() {
            return Ok(Vec::new());
        }
        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();
        let mut out = Vec::new();
        for (u, c) in meta.universe.iter().zip(ctxs.iter()) {
            if !wanted.contains(u.name.as_str()) {
                continue;
            }
            let Some(s) = c.mark_px.as_ref() else { continue };
            let Ok(price) = Decimal::from_str(s) else { continue };
            out.push(PriceQuote {
                instrument: InstrumentId::new(Exchange::Hyperliquid, u.name.clone()),
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
        // POST /info {"type":"fundingHistory","coin":"BTC",
        // "startTime":<ms>}. `coin` is the symbol unchanged.
        // Hyperliquid pays hourly so this can be a fat list —
        // capped at 500 by the API.
        // Docs: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
        #[derive(serde::Serialize)]
        struct Body<'a> {
            #[serde(rename = "type")]
            kind: &'a str,
            coin: &'a str,
            #[serde(rename = "startTime")]
            start_time: i64,
        }
        let now = OffsetDateTime::now_utc();
        let start_time = since
            .unwrap_or_else(|| now - time::Duration::days(7))
            .unix_timestamp_nanos()
            / 1_000_000;
        let body = Body {
            kind: "fundingHistory",
            coin: &instrument.symbol,
            start_time: start_time as i64,
        };
        let url = format!("{}/info", self.base_url);
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct Row {
            coin: String,
            #[serde(default, rename = "fundingRate")]
            funding_rate: String,
            time: i64,
            #[serde(default)]
            premium: String,
        }
        let rows: Vec<Row> = self.http.post_json(&url, &body).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let Ok(rate) = Decimal::from_str(&r.funding_rate) else {
                continue;
            };
            let Ok(ts) =
                OffsetDateTime::from_unix_timestamp_nanos(i128::from(r.time) * 1_000_000)
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
        let (meta, ctxs) = self.meta_and_ctxs().await?;
        if meta.universe.len() != ctxs.len() {
            return Ok(Vec::new());
        }
        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();
        let mut out = Vec::new();
        for (u, c) in meta.universe.iter().zip(ctxs.iter()) {
            if !wanted.contains(u.name.as_str()) {
                continue;
            }
            let Some(s) = c.funding.as_ref() else { continue };
            let Ok(rate) = Decimal::from_str(s) else {
                continue;
            };
            out.push(oi_core::FundingBar {
                instrument: InstrumentId::new(Exchange::Hyperliquid, u.name.clone()),
                bucket_ts: bucket,
                recv_ts: now,
                rate,
                // Hyperliquid funds every hour; payload doesn't
                // expose the next-funding timestamp explicitly,
                // and clients can derive it from `floor(now, 1h) + 1h`.
                next_funding_ts: None,
                interval_hours: Some(1),
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialises_positional_meta_ctxs() {
        let body = r#"[
            {"universe":[{"name":"BTC","szDecimals":5,"maxLeverage":50},{"name":"ETH","szDecimals":4,"maxLeverage":50}]},
            [
              {"openInterest":"1234.5","markPx":"64000.0","oraclePx":"63999.0","premium":"0.0001"},
              {"openInterest":"9876.0","markPx":"3200.0","oraclePx":"3200.5"}
            ]
        ]"#;
        let parsed: MetaAndCtxsResp = serde_json::from_str(body).unwrap();
        let MetaAndCtxsResp::Tuple(meta, ctxs) = parsed;
        assert_eq!(meta.universe.len(), 2);
        assert_eq!(meta.universe[0].name, "BTC");
        assert_eq!(ctxs.len(), 2);
        assert_eq!(ctxs[0].open_interest.as_deref(), Some("1234.5"));
    }

    #[test]
    fn tolerates_missing_optional_fields() {
        let body = r#"[
            {"universe":[{"name":"BTC","szDecimals":5}]},
            [ {"openInterest":"1"} ]
        ]"#;
        let parsed: MetaAndCtxsResp = serde_json::from_str(body).unwrap();
        let MetaAndCtxsResp::Tuple(_meta, ctxs) = parsed;
        assert_eq!(ctxs[0].open_interest.as_deref(), Some("1"));
        assert!(ctxs[0].mark_px.is_none());
    }
}
