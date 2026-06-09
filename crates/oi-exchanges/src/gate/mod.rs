//! Gate.io USDT-margined perpetual-futures adapter.
//!
//! Endpoints (all public, host `https://api.gateio.ws`):
//! * `GET /api/v4/futures/usdt/contracts` — **batch** for the whole
//!   universe. One call carries discovery, OI, funding AND price per
//!   contract, so `discover_instruments` / `fetch_oi` / `fetch_funding`
//!   all hit the same endpoint (no per-symbol fan-out like MEXC/KuCoin).
//!   Relevant fields per contract:
//!     - `name`               → "BTC_USDT" (underscore form)
//!     - `position_size`      → OI in **contracts**, one-sided open
//!       interest. (`total_size` on the tickers endpoint is exactly 2×
//!       this — it counts both legs; the standard OI is the one-sided
//!       figure, which keeps Gate comparable to Binance/Bybit/… here.)
//!     - `quanto_multiplier`  → coins per contract (e.g. "0.0001" for BTC)
//!     - `mark_price`         → mark price for USD enrichment
//!     - `funding_rate`       → predicted next-settlement rate
//!     - `funding_interval`   → seconds between settlements (28800 = 8h)
//!     - `funding_next_apply` → next settlement unix time (seconds)
//!     - `in_delisting` / `status` → liveness
//!   <https://www.gate.io/docs/developers/apiv4/#list-all-futures-contracts>
//! * `GET /api/v4/futures/usdt/funding_rate?contract=X&limit=100` —
//!   settlement-event history; array of `{t: unixSeconds, r: "rate"}`.
//!
//! Symbol form: Gate uses the underscore form `BTC_USDT` on the wire. We
//! store the **stripped, upper-cased** form `BTCUSDT` as the
//! `InstrumentId.symbol` because that is what the terminal queries
//! (`gate:BTCUSDT`). Per-contract API calls reconstruct the underscore
//! form on the fly (`<base>_USDT`).
//!
//! Unit: **Contracts**; `InstrumentMeta.contract_multiplier` carries
//! `quanto_multiplier`. The core `UnitKind::to_coins` / `to_usd` do the
//! multiplication.
//!
//! Rate limits: Gate public futures market-data is generous
//! (~900 req / 10 s). We poll one batch per minute; 8 rps / burst 16 is
//! ample and stays well clear of the cap.

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

pub const DEFAULT_BASE_URL: &str = "https://api.gateio.ws";

#[derive(Clone)]
pub struct GateAdapter {
    http: RateLimitedClient,
    base_url: String,
}

impl std::fmt::Debug for GateAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GateAdapter")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl Default for GateAdapter {
    fn default() -> Self {
        Self::new().expect("gate http client")
    }
}

impl GateAdapter {
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, ExchangeError> {
        let http = RateLimitedClient::new("gate", 8, 16)?;
        Ok(Self {
            http,
            base_url: base_url.into(),
        })
    }

    fn contracts_url(&self) -> String {
        format!("{}/api/v4/futures/usdt/contracts", self.base_url)
    }

    /// "BTC_USDT" → "BTCUSDT" (the form the terminal queries).
    fn normalize(name: &str) -> String {
        name.replace('_', "").to_ascii_uppercase()
    }

    /// "BTCUSDT" → "BTC_USDT" (Gate's per-contract API param). All
    /// USDT-settled perps are `<base>_USDT`, so peel the quote suffix.
    fn denormalize(symbol: &str) -> String {
        match symbol.strip_suffix("USDT") {
            Some(base) if !base.is_empty() => format!("{base}_USDT"),
            _ => symbol.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Contract {
    /// Underscore form, e.g. "BTC_USDT".
    name: String,
    /// Coins per contract, decimal string ("0.0001").
    #[serde(default)]
    quanto_multiplier: Option<String>,
    /// Mark price, decimal string.
    #[serde(default)]
    mark_price: Option<String>,
    #[serde(default)]
    index_price: Option<String>,
    #[serde(default)]
    last_price: Option<String>,
    /// Predicted funding rate for the next settlement, decimal string.
    #[serde(default)]
    funding_rate: Option<String>,
    /// Seconds between funding settlements (28800 = 8h).
    #[serde(default)]
    funding_interval: i64,
    /// Next settlement, unix seconds.
    #[serde(default)]
    funding_next_apply: i64,
    /// Open interest in contracts (one-sided). JSON integer.
    #[serde(default)]
    position_size: Option<serde_json::Number>,
    /// Price-tick, decimal string ("0.1").
    #[serde(default)]
    order_price_round: Option<String>,
    #[serde(default)]
    in_delisting: bool,
    /// "trading" when live.
    #[serde(default)]
    status: String,
}

impl Contract {
    fn is_live(&self) -> bool {
        !self.in_delisting && (self.status.is_empty() || self.status == "trading")
    }

    fn base_quote(&self) -> (String, String) {
        match self.name.split_once('_') {
            Some((b, q)) => (b.to_ascii_uppercase(), q.to_ascii_uppercase()),
            None => (self.name.to_ascii_uppercase(), String::new()),
        }
    }
}

fn dec_from_str(s: &Option<String>) -> Option<Decimal> {
    s.as_deref()
        .filter(|v| !v.is_empty())
        .and_then(|v| Decimal::from_str(v).ok())
}

fn number_to_decimal(n: &serde_json::Number) -> Option<Decimal> {
    Decimal::from_str(&n.to_string()).ok()
}

#[async_trait]
impl ExchangeAdapter for GateAdapter {
    fn exchange(&self) -> Exchange {
        Exchange::Gate
    }

    async fn discover_instruments(&self) -> Result<Vec<InstrumentMeta>, ExchangeError> {
        let rows: Vec<Contract> = self.http.get_json(&self.contracts_url()).await?;
        let metas = rows
            .into_iter()
            .map(|c| {
                let (base, quote) = c.base_quote();
                InstrumentMeta {
                    id: InstrumentId::new(Exchange::Gate, Self::normalize(&c.name)),
                    base_asset: base,
                    quote_asset: quote,
                    is_perpetual: true,
                    native_unit: UnitKind::Contracts,
                    contract_multiplier: dec_from_str(&c.quanto_multiplier),
                    price_tick: dec_from_str(&c.order_price_round),
                    qty_step: None,
                    active: c.is_live(),
                }
            })
            .collect();
        Ok(metas)
    }

    async fn fetch_oi(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> Result<Vec<RawOi>, ExchangeError> {
        let now = OffsetDateTime::now_utc();
        let rows: Vec<Contract> = self.http.get_json(&self.contracts_url()).await?;
        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();

        let mut out = Vec::with_capacity(wanted.len());
        for c in rows {
            let sym = Self::normalize(&c.name);
            if !wanted.contains(sym.as_str()) {
                continue;
            }
            let Some(n) = c.position_size.as_ref() else {
                continue;
            };
            let Some(value) = number_to_decimal(n) else {
                warn!(symbol=%sym, "gate: bad position_size number");
                continue;
            };
            let price = dec_from_str(&c.mark_price)
                .or_else(|| dec_from_str(&c.last_price))
                .or_else(|| dec_from_str(&c.index_price));
            let price_hint = price.map(|p| PriceQuote {
                instrument: InstrumentId::new(Exchange::Gate, sym.clone()),
                price: p,
                source: if c.mark_price.is_some() {
                    PriceSource::Mark
                } else {
                    PriceSource::Last
                },
                ts: now,
            });
            out.push(RawOi {
                instrument: InstrumentId::new(Exchange::Gate, sym),
                value,
                unit: UnitKind::Contracts,
                bucket_ts: bucket,
                recv_ts: now,
                price_hint,
            });
        }
        Ok(out)
    }

    async fn fetch_funding(
        &self,
        instruments: &[InstrumentId],
        bucket: OffsetDateTime,
    ) -> Result<Vec<oi_core::FundingBar>, ExchangeError> {
        // Same batch we already poll for OI; funding rides along.
        let now = OffsetDateTime::now_utc();
        let rows: Vec<Contract> = self.http.get_json(&self.contracts_url()).await?;
        let wanted: std::collections::HashSet<&str> =
            instruments.iter().map(|i| i.symbol.as_str()).collect();

        let mut out = Vec::new();
        for c in rows {
            let sym = Self::normalize(&c.name);
            if !wanted.contains(sym.as_str()) {
                continue;
            }
            let Some(rate) = dec_from_str(&c.funding_rate) else {
                continue;
            };
            let interval_hours = if c.funding_interval > 0 {
                u8::try_from(c.funding_interval / 3600).ok().filter(|h| *h > 0)
            } else {
                None
            };
            let next_funding_ts = if c.funding_next_apply > 0 {
                OffsetDateTime::from_unix_timestamp(c.funding_next_apply).ok()
            } else {
                None
            };
            out.push(oi_core::FundingBar {
                instrument: InstrumentId::new(Exchange::Gate, sym),
                bucket_ts: bucket,
                recv_ts: now,
                rate,
                next_funding_ts,
                interval_hours,
            });
        }
        Ok(out)
    }

    async fn fetch_funding_history(
        &self,
        instrument: &InstrumentId,
        since: Option<OffsetDateTime>,
    ) -> Result<Vec<oi_core::FundingEvent>, ExchangeError> {
        // /api/v4/futures/usdt/funding_rate?contract=BTC_USDT&limit=100
        // → [{ "t": <unix seconds>, "r": "<rate>" }, ...]  (newest first)
        let contract = Self::denormalize(&instrument.symbol);
        let url = format!(
            "{}/api/v4/futures/usdt/funding_rate?contract={}&limit=100",
            self.base_url, contract
        );
        #[derive(Debug, Deserialize)]
        struct Row {
            #[serde(default)]
            t: i64,
            #[serde(default)]
            r: Option<String>,
        }
        let rows: Vec<Row> = self.http.get_json(&url).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let Some(rate) = row.r.as_deref().and_then(|v| Decimal::from_str(v).ok()) else {
                continue;
            };
            if row.t <= 0 {
                continue;
            }
            let Ok(ts) = OffsetDateTime::from_unix_timestamp(row.t) else {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // A trimmed but real-shape contract row from
    // GET /api/v4/futures/usdt/contracts.
    const CONTRACT_JSON: &str = r#"[{
        "name":"BTC_USDT","quanto_multiplier":"0.0001","mark_price":"62577",
        "index_price":"62593.35","last_price":"62577","funding_rate":"0.000032",
        "funding_interval":28800,"funding_next_apply":1781020800,
        "position_size":259890237,"order_price_round":"0.1",
        "in_delisting":false,"status":"trading"
    }]"#;

    #[test]
    fn parses_contract_shape() {
        let rows: Vec<Contract> = serde_json::from_str(CONTRACT_JSON).unwrap();
        let c = &rows[0];
        assert_eq!(c.name, "BTC_USDT");
        assert_eq!(number_to_decimal(c.position_size.as_ref().unwrap()), Some(dec!(259890237)));
        assert_eq!(dec_from_str(&c.quanto_multiplier), Some(dec!(0.0001)));
        assert_eq!(c.funding_interval, 28800);
        assert!(c.is_live());
    }

    #[test]
    fn symbol_normalize_roundtrip() {
        assert_eq!(GateAdapter::normalize("BTC_USDT"), "BTCUSDT");
        assert_eq!(GateAdapter::normalize("H_USDT"), "HUSDT");
        assert_eq!(GateAdapter::denormalize("BTCUSDT"), "BTC_USDT");
        assert_eq!(GateAdapter::denormalize("HUSDT"), "H_USDT");
    }

    #[test]
    fn base_quote_split() {
        let rows: Vec<Contract> = serde_json::from_str(CONTRACT_JSON).unwrap();
        let (b, q) = rows[0].base_quote();
        assert_eq!(b, "BTC");
        assert_eq!(q, "USDT");
    }

    #[test]
    fn delisting_or_non_trading_is_inactive() {
        let json = r#"[{"name":"X_USDT","in_delisting":true,"status":"trading"},
                       {"name":"Y_USDT","in_delisting":false,"status":"delisting"}]"#;
        let rows: Vec<Contract> = serde_json::from_str(json).unwrap();
        assert!(!rows[0].is_live());
        assert!(!rows[1].is_live());
    }
}
