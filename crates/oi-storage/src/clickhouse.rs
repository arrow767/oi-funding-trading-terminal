//! ClickHouse repository.
//!
//! Uses the native TCP protocol via the `clickhouse` crate with LZ4 transport
//! compression. Writes go through `Inserter` which batches in memory and
//! flushes either on row count or time — sized so the collector's 1-minute
//! tick comfortably writes in one flush.

use async_trait::async_trait;
use clickhouse::{error::Error as ChError, Client, Row};
use oi_core::{
    error::{CoreError, Result},
    funding::{FundingBar, FundingEvent},
    instrument::{InstrumentId, InstrumentMeta},
    snapshot::OiSnapshot,
    traits::OiRepository,
    unit::UnitKind,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use time::OffsetDateTime;
use tracing::warn;

#[derive(Clone)]
pub struct ClickHouseRepo {
    client: Client,
}

impl std::fmt::Debug for ClickHouseRepo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseRepo").finish()
    }
}

impl ClickHouseRepo {
    /// `url`: `http://ch-host:8123`. `database` is typically `oi`.
    pub fn new(url: &str, database: &str, user: &str, password: &str) -> Self {
        let client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(user)
            .with_password(password)
            .with_compression(clickhouse::Compression::Lz4);
        Self { client }
    }

    /// Cheap liveness probe used by `/health/ready`. Any server-side
    /// failure surfaces as an `Err`.
    pub async fn probe(&self) -> Result<()> {
        self.client
            .query("SELECT 1")
            .execute()
            .await
            .map_err(ch_err)
    }

    /// Run `migrations/single/001_schema.sql`. Safe to re-run.
    /// (The replicated variant lives in migrations/replicated/; nodes
    /// running in HA mode don't go through this path — ClickHouse's
    /// docker-entrypoint-initdb.d does the cluster DDL on first boot.)
    pub async fn ensure_schema(&self) -> Result<()> {
        let sql = include_str!("../../../migrations/single/001_schema.sql");
        for statement in split_sql(sql) {
            self.client
                .query(&statement)
                .execute()
                .await
                .map_err(ch_err)?;
        }
        Ok(())
    }
}

fn ch_err(e: ChError) -> CoreError {
    CoreError::Storage(format!("clickhouse: {e}"))
}

/// Split a SQL file on `;` statement boundaries. Strips `--` line
/// comments first so that semicolons inside comments (e.g. an English
/// sentence with a `;` in it) aren't mistaken for statement separators
/// — that was a real bug that fed ClickHouse half-statements starting
/// with stray `)`.
///
/// Does NOT understand string literals or block `/* */` comments.
/// Our hand-authored migrations don't use either, so this stays minimal.
fn split_sql(sql: &str) -> Vec<String> {
    let mut stripped = String::with_capacity(sql.len());
    for line in sql.lines() {
        let code = match line.find("--") {
            Some(idx) => &line[..idx],
            None => line,
        };
        stripped.push_str(code);
        stripped.push('\n');
    }
    stripped
        .split(';')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect()
}

// --- wire rows -------------------------------------------------------------

#[derive(Debug, Row, Serialize, Deserialize)]
struct InstrumentRow {
    exchange: String,
    symbol: String,
    base_asset: String,
    quote_asset: String,
    is_perpetual: u8,
    native_unit: String,
    contract_multiplier: Option<String>,
    price_tick: Option<String>,
    qty_step: Option<String>,
    active: u8,
}

/// One CH row = one OHLC bar. `Decimal128` columns are wired as
/// strings on the wire because the `clickhouse` crate's native
/// `Decimal128` support requires non-`Nullable` columns; passing
/// strings sidesteps that and lets `rust_decimal::Decimal::from_str`
/// preserve full precision on read-back.
#[derive(Debug, Row, Serialize, Deserialize)]
struct OiRow {
    exchange: String,
    symbol: String,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    bucket_ts: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    first_recv_ts: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    last_recv_ts: OffsetDateTime,
    samples: u32,
    native_unit: String,
    native_open: String,
    native_high: String,
    native_low: String,
    native_close: String,
    oi_coins_open: Option<String>,
    oi_coins_high: Option<String>,
    oi_coins_low: Option<String>,
    oi_coins_close: Option<String>,
    oi_usd_open: Option<String>,
    oi_usd_high: Option<String>,
    oi_usd_low: Option<String>,
    oi_usd_close: Option<String>,
    price_used_close: Option<String>,
}

/// One CH row = one minute funding sample. `rate` carries 12
/// decimal places of fraction — funding rates are typically in
/// the 1e-4 to 1e-3 range, so Decimal64(12) gives ample headroom.
#[derive(Debug, Row, Serialize, Deserialize)]
struct FundingRow {
    exchange: String,
    symbol: String,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    bucket_ts: OffsetDateTime,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    recv_ts: OffsetDateTime,
    rate: String,
    #[serde(with = "clickhouse::serde::time::datetime64::millis::option")]
    next_funding_ts: Option<OffsetDateTime>,
    interval_hours: Option<u8>,
}

const SELECT_FUNDING_RANGE: &str =
    "SELECT exchange, symbol, bucket_ts, recv_ts, rate, next_funding_ts, interval_hours \
     FROM oi.funding_minute \
     WHERE exchange = ? AND symbol = ? AND bucket_ts >= ? AND bucket_ts < ? \
     ORDER BY bucket_ts";

const SELECT_FUNDING_LATEST: &str =
    "SELECT exchange, symbol, bucket_ts, recv_ts, rate, next_funding_ts, interval_hours \
     FROM oi.funding_minute \
     WHERE exchange = ? AND symbol = ? \
     ORDER BY bucket_ts DESC LIMIT 1";

#[derive(Debug, Row, Serialize, Deserialize)]
struct FundingEventRow {
    exchange: String,
    symbol: String,
    #[serde(with = "clickhouse::serde::time::datetime64::millis")]
    settlement_ts: OffsetDateTime,
    rate: String,
    mark_price: Option<String>,
}

const SELECT_EVENTS_RANGE: &str =
    "SELECT exchange, symbol, settlement_ts, rate, mark_price \
     FROM oi.funding_event \
     WHERE exchange = ? AND symbol = ? AND settlement_ts >= ? AND settlement_ts < ? \
     ORDER BY settlement_ts";

const SELECT_EVENT_LATEST: &str =
    "SELECT exchange, symbol, settlement_ts, rate, mark_price \
     FROM oi.funding_event \
     WHERE exchange = ? AND symbol = ? \
     ORDER BY settlement_ts DESC LIMIT 1";

fn row_to_event(r: FundingEventRow) -> Result<FundingEvent> {
    let exchange = r.exchange.parse().map_err(CoreError::Storage)?;
    let rate = Decimal::from_str(&r.rate)
        .map_err(|e| CoreError::Storage(format!("event rate: {e}")))?;
    let mark_price = r
        .mark_price
        .map(|s| Decimal::from_str(&s))
        .transpose()
        .map_err(|e| CoreError::Storage(format!("event mark_price: {e}")))?;
    Ok(FundingEvent {
        instrument: InstrumentId::new(exchange, r.symbol),
        settlement_ts: r.settlement_ts,
        rate,
        mark_price,
    })
}

fn row_to_funding(r: FundingRow) -> Result<FundingBar> {
    let exchange = r.exchange.parse().map_err(CoreError::Storage)?;
    let rate = Decimal::from_str(&r.rate)
        .map_err(|e| CoreError::Storage(format!("funding rate: {e}")))?;
    Ok(FundingBar {
        instrument: InstrumentId::new(exchange, r.symbol),
        bucket_ts: r.bucket_ts,
        recv_ts: r.recv_ts,
        rate,
        next_funding_ts: r.next_funding_ts,
        interval_hours: r.interval_hours,
    })
}

fn unit_code(u: UnitKind) -> &'static str {
    match u {
        UnitKind::Coins => "coins",
        UnitKind::Contracts => "contracts",
        UnitKind::Usd => "usd",
    }
}

const OHLC_COLUMN_LIST: &str =
    "exchange, symbol, bucket_ts, first_recv_ts, last_recv_ts, samples, \
     native_unit, native_open, native_high, native_low, native_close, \
     oi_coins_open, oi_coins_high, oi_coins_low, oi_coins_close, \
     oi_usd_open, oi_usd_high, oi_usd_low, oi_usd_close, \
     price_used_close";

const SELECT_OHLC_COLUMNS: &str =
    "SELECT exchange, symbol, bucket_ts, first_recv_ts, last_recv_ts, samples, \
            native_unit, native_open, native_high, native_low, native_close, \
            oi_coins_open, oi_coins_high, oi_coins_low, oi_coins_close, \
            oi_usd_open, oi_usd_high, oi_usd_low, oi_usd_close, \
            price_used_close \
     FROM oi.oi_minute \
     WHERE exchange = ? AND symbol = ? AND bucket_ts >= ? AND bucket_ts < ? \
     ORDER BY bucket_ts";

const SELECT_OHLC_LATEST: &str =
    "SELECT exchange, symbol, bucket_ts, first_recv_ts, last_recv_ts, samples, \
            native_unit, native_open, native_high, native_low, native_close, \
            oi_coins_open, oi_coins_high, oi_coins_low, oi_coins_close, \
            oi_usd_open, oi_usd_high, oi_usd_low, oi_usd_close, \
            price_used_close \
     FROM oi.oi_minute \
     WHERE exchange = ? AND symbol = ? \
     ORDER BY bucket_ts DESC LIMIT 1";

// Suppress "unused" warning if read paths are stripped; the constant
// is still referenced by tests/migrations even when callers aren't.
#[allow(dead_code)]
const _OHLC_COLUMN_LIST_KEEP_ALIVE: &str = OHLC_COLUMN_LIST;

// --- trait -----------------------------------------------------------------

#[async_trait]
impl OiRepository for ClickHouseRepo {
    async fn upsert_snapshots(&self, snaps: &[OiSnapshot]) -> Result<()> {
        if snaps.is_empty() {
            return Ok(());
        }
        let mut insert = self
            .client
            .insert::<OiRow>("oi.oi_minute")
            .map_err(ch_err)?;
        for s in snaps {
            insert
                .write(&OiRow {
                    exchange: s.instrument.exchange.code().to_owned(),
                    symbol: s.instrument.symbol.clone(),
                    bucket_ts: s.bucket_ts,
                    first_recv_ts: s.first_recv_ts,
                    last_recv_ts: s.last_recv_ts,
                    samples: s.samples,
                    native_unit: unit_code(s.native_unit).to_owned(),
                    native_open: s.native_open.to_string(),
                    native_high: s.native_high.to_string(),
                    native_low: s.native_low.to_string(),
                    native_close: s.native_close.to_string(),
                    oi_coins_open: s.oi_coins_open.map(|d| d.to_string()),
                    oi_coins_high: s.oi_coins_high.map(|d| d.to_string()),
                    oi_coins_low: s.oi_coins_low.map(|d| d.to_string()),
                    oi_coins_close: s.oi_coins_close.map(|d| d.to_string()),
                    oi_usd_open: s.oi_usd_open.map(|d| d.to_string()),
                    oi_usd_high: s.oi_usd_high.map(|d| d.to_string()),
                    oi_usd_low: s.oi_usd_low.map(|d| d.to_string()),
                    oi_usd_close: s.oi_usd_close.map(|d| d.to_string()),
                    price_used_close: s.price_used_close.map(|d| d.to_string()),
                })
                .await
                .map_err(ch_err)?;
        }
        insert.end().await.map_err(ch_err)?;
        Ok(())
    }

    async fn upsert_instruments(&self, metas: &[InstrumentMeta]) -> Result<()> {
        if metas.is_empty() {
            return Ok(());
        }
        let mut insert = self
            .client
            .insert::<InstrumentRow>("oi.instruments")
            .map_err(ch_err)?;
        for m in metas {
            insert
                .write(&InstrumentRow {
                    exchange: m.id.exchange.code().to_owned(),
                    symbol: m.id.symbol.clone(),
                    base_asset: m.base_asset.clone(),
                    quote_asset: m.quote_asset.clone(),
                    is_perpetual: u8::from(m.is_perpetual),
                    native_unit: unit_code(m.native_unit).to_owned(),
                    contract_multiplier: m.contract_multiplier.map(|d| d.to_string()),
                    price_tick: m.price_tick.map(|d| d.to_string()),
                    qty_step: m.qty_step.map(|d| d.to_string()),
                    active: u8::from(m.active),
                })
                .await
                .map_err(ch_err)?;
        }
        insert.end().await.map_err(ch_err)?;
        Ok(())
    }

    async fn range(
        &self,
        instrument: &InstrumentId,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Result<Vec<OiSnapshot>> {
        let rows = self
            .client
            .query(SELECT_OHLC_COLUMNS)
            .bind(instrument.exchange.code())
            .bind(&instrument.symbol)
            .bind(from)
            .bind(to)
            .fetch_all::<OiRow>()
            .await
            .map_err(ch_err)?;
        rows.into_iter().map(row_to_snap).collect()
    }

    async fn latest(&self, instrument: &InstrumentId) -> Result<Option<OiSnapshot>> {
        let row = self
            .client
            .query(SELECT_OHLC_LATEST)
            .bind(instrument.exchange.code())
            .bind(&instrument.symbol)
            .fetch_optional::<OiRow>()
            .await
            .map_err(ch_err)?;
        row.map(row_to_snap).transpose()
    }

    async fn upsert_funding(&self, bars: &[FundingBar]) -> Result<()> {
        if bars.is_empty() {
            return Ok(());
        }
        let mut insert = self
            .client
            .insert::<FundingRow>("oi.funding_minute")
            .map_err(ch_err)?;
        for b in bars {
            insert
                .write(&FundingRow {
                    exchange: b.instrument.exchange.code().to_owned(),
                    symbol: b.instrument.symbol.clone(),
                    bucket_ts: b.bucket_ts,
                    recv_ts: b.recv_ts,
                    rate: b.rate.to_string(),
                    next_funding_ts: b.next_funding_ts,
                    interval_hours: b.interval_hours,
                })
                .await
                .map_err(ch_err)?;
        }
        insert.end().await.map_err(ch_err)?;
        Ok(())
    }

    async fn funding_range(
        &self,
        instrument: &InstrumentId,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Result<Vec<FundingBar>> {
        let rows = self
            .client
            .query(SELECT_FUNDING_RANGE)
            .bind(instrument.exchange.code())
            .bind(&instrument.symbol)
            .bind(from)
            .bind(to)
            .fetch_all::<FundingRow>()
            .await
            .map_err(ch_err)?;
        rows.into_iter().map(row_to_funding).collect()
    }

    async fn latest_funding(
        &self,
        instrument: &InstrumentId,
    ) -> Result<Option<FundingBar>> {
        let row = self
            .client
            .query(SELECT_FUNDING_LATEST)
            .bind(instrument.exchange.code())
            .bind(&instrument.symbol)
            .fetch_optional::<FundingRow>()
            .await
            .map_err(ch_err)?;
        row.map(row_to_funding).transpose()
    }

    async fn upsert_funding_events(&self, events: &[FundingEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut insert = self
            .client
            .insert::<FundingEventRow>("oi.funding_event")
            .map_err(ch_err)?;
        for e in events {
            insert
                .write(&FundingEventRow {
                    exchange: e.instrument.exchange.code().to_owned(),
                    symbol: e.instrument.symbol.clone(),
                    settlement_ts: e.settlement_ts,
                    rate: e.rate.to_string(),
                    mark_price: e.mark_price.map(|d| d.to_string()),
                })
                .await
                .map_err(ch_err)?;
        }
        insert.end().await.map_err(ch_err)?;
        Ok(())
    }

    async fn funding_events_range(
        &self,
        instrument: &InstrumentId,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Result<Vec<FundingEvent>> {
        let rows = self
            .client
            .query(SELECT_EVENTS_RANGE)
            .bind(instrument.exchange.code())
            .bind(&instrument.symbol)
            .bind(from)
            .bind(to)
            .fetch_all::<FundingEventRow>()
            .await
            .map_err(ch_err)?;
        rows.into_iter().map(row_to_event).collect()
    }

    async fn latest_funding_event(
        &self,
        instrument: &InstrumentId,
    ) -> Result<Option<FundingEvent>> {
        let row = self
            .client
            .query(SELECT_EVENT_LATEST)
            .bind(instrument.exchange.code())
            .bind(&instrument.symbol)
            .fetch_optional::<FundingEventRow>()
            .await
            .map_err(ch_err)?;
        row.map(row_to_event).transpose()
    }
}

fn row_to_snap(r: OiRow) -> Result<OiSnapshot> {
    let exchange = r.exchange.parse().map_err(CoreError::Storage)?;
    let native_unit = match r.native_unit.as_str() {
        "coins" => UnitKind::Coins,
        "contracts" => UnitKind::Contracts,
        "usd" => UnitKind::Usd,
        other => {
            warn!(?other, "unknown native_unit in storage; treating as coins");
            UnitKind::Coins
        }
    };
    let parse_required = |s: String, name: &str| -> Result<Decimal> {
        Decimal::from_str(&s).map_err(|e| CoreError::Storage(format!("{name}: {e}")))
    };
    let parse_optional = |o: Option<String>| -> Result<Option<Decimal>> {
        o.map(|s| Decimal::from_str(&s).map_err(|e| CoreError::Storage(format!("decimal: {e}"))))
            .transpose()
    };
    Ok(OiSnapshot {
        instrument: InstrumentId::new(exchange, r.symbol),
        bucket_ts: r.bucket_ts,
        first_recv_ts: r.first_recv_ts,
        last_recv_ts: r.last_recv_ts,
        samples: r.samples,
        native_unit,
        native_open: parse_required(r.native_open, "native_open")?,
        native_high: parse_required(r.native_high, "native_high")?,
        native_low: parse_required(r.native_low, "native_low")?,
        native_close: parse_required(r.native_close, "native_close")?,
        oi_coins_open: parse_optional(r.oi_coins_open)?,
        oi_coins_high: parse_optional(r.oi_coins_high)?,
        oi_coins_low: parse_optional(r.oi_coins_low)?,
        oi_coins_close: parse_optional(r.oi_coins_close)?,
        oi_usd_open: parse_optional(r.oi_usd_open)?,
        oi_usd_high: parse_optional(r.oi_usd_high)?,
        oi_usd_low: parse_optional(r.oi_usd_low)?,
        oi_usd_close: parse_optional(r.oi_usd_close)?,
        price_used_close: parse_optional(r.price_used_close)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_sql_produces_multiple_statements() {
        let sql = "CREATE TABLE a(x Int64) ENGINE=Memory;\n\n-- comment only\n;\nCREATE TABLE b(y Int64) ENGINE=Memory;";
        let parts = split_sql(sql);
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn split_sql_ignores_semicolons_inside_line_comments() {
        // Regression: this exact shape (a `;` inside a `--` comment between
        // two CREATE TABLEs) used to make split_sql leak fragments to CH.
        let sql = "\
-- header: (note about A; and about B).
CREATE TABLE a(x Int64) ENGINE=Memory;
-- another with ; in it.
CREATE TABLE b(y Int64) ENGINE=Memory;
";
        let parts = split_sql(sql);
        assert_eq!(parts.len(), 2);
        assert!(parts[0].contains("CREATE TABLE a"));
        assert!(parts[1].contains("CREATE TABLE b"));
    }
}
