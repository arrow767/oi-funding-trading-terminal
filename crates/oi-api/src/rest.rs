//! REST/JSON facade. Thin layer over the same repository the gRPC server
//! uses. Exists for debugging and browser clients; the terminal uses gRPC.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use futures::stream::{FuturesUnordered, StreamExt};
use oi_core::{exchange::Exchange, instrument::InstrumentId, traits::OiRepository};
use oi_storage::{clickhouse::ClickHouseRepo, redis::RedisCache};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, sync::Arc};
use time::OffsetDateTime;

/// Maximum items the server will accept in a single batch range request.
/// Above this the request is rejected outright (400) — protects against a
/// client accidentally (or maliciously) fanning out hundreds of CH queries
/// per request. 128 covers every realistic UI scenario (a dashboard with
/// dozens of OI/Funding panels).
const BATCH_MAX_ITEMS: usize = 128;

/// Bound on how many per-item repo queries we run concurrently inside a
/// single batch. Keeps ClickHouse from drowning when one client sends a
/// big batch. Result order is preserved by index regardless of completion
/// order, so callers can match results to their inputs.
const BATCH_CONCURRENCY: usize = 16;

#[derive(Clone)]
pub struct RestState {
    pub repo: Arc<dyn OiRepository>,
    /// Downstream probes for `/health/ready`. Optional because some
    /// deployments (tests, custom embeddings) wire up the API around
    /// a different repository.
    pub clickhouse: Option<ClickHouseRepo>,
    pub redis: Option<RedisCache>,
}

impl std::fmt::Debug for RestState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestState").finish()
    }
}

pub fn router(state: RestState) -> Router {
    Router::new()
        // Legacy `/health` kept — some deployments already scrape it.
        .route("/health", get(|| async { "ok" }))
        // k8s-style probes.
        .route("/health/live", get(|| async { (StatusCode::OK, "live") }))
        .route("/health/ready", get(ready))
        .route("/v1/oi/latest/:exchange/:symbol", get(latest))
        .route("/v1/oi/range/:exchange/:symbol", get(range))
        .route("/v1/funding/latest/:exchange/:symbol", get(funding_latest))
        .route("/v1/funding/range/:exchange/:symbol", get(funding_range))
        .route(
            "/v1/funding/events/latest/:exchange/:symbol",
            get(funding_event_latest),
        )
        .route(
            "/v1/funding/events/range/:exchange/:symbol",
            get(funding_event_range),
        )
        // Batch range endpoints — the terminal coalesces every active
        // panel's `EnsureSeriesRange`/`EnsureFundingSeriesRange` calls
        // landing within a short window (~30 ms) into ONE POST request
        // each, so opening a 6-pane workspace touches the server with
        // 1 RTT instead of 6. Single-pane callers still use the GET
        // routes above.
        .route("/v1/oi/range/batch", post(range_batch))
        .route("/v1/funding/range/batch", post(funding_range_batch))
        .route(
            "/v1/funding/events/range/batch",
            post(funding_event_range_batch),
        )
        // System resource metrics for the cloud admin monitoring page.
        // Behind the same bearer middleware as the data routes.
        .route(
            "/v1/system/metrics",
            get(crate::sysmetrics::system_metrics),
        )
        .with_state(state)
}

#[derive(Serialize)]
struct ReadyReport {
    clickhouse: String,
    redis: String,
}

async fn ready(State(state): State<RestState>) -> impl IntoResponse {
    let _t = crate::metrics::Timer::start("REST_Ready");
    let (ch_ok, ch_msg) = match state.clickhouse.as_ref() {
        None => (true, "skipped".to_owned()),
        Some(c) => match c.probe().await {
            Ok(()) => (true, "ok".into()),
            Err(e) => (false, e.to_string()),
        },
    };
    let (rs_ok, rs_msg) = match state.redis.as_ref() {
        None => (true, "skipped".to_owned()),
        Some(r) => match r.probe().await {
            Ok(()) => (true, "ok".into()),
            Err(e) => (false, e.to_string()),
        },
    };
    let status = if ch_ok && rs_ok {
        crate::metrics::inc_request("REST_Ready", "ok");
        StatusCode::OK
    } else {
        crate::metrics::inc_request("REST_Ready", "service_unavailable");
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(ReadyReport {
            clickhouse: ch_msg,
            redis: rs_msg,
        }),
    )
}

/// Wire-shape DTO. Decimal fields are strings to preserve full
/// precision across the JSON boundary; nullable fields use
/// `Option<String>` so they serialize as JSON `null`.
#[derive(Serialize)]
struct BarDto {
    exchange: String,
    symbol: String,
    bucket_ts: String,
    first_recv_ts: String,
    last_recv_ts: String,
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

/// Format an `OffsetDateTime` as RFC 3339 (`2026-05-15T12:44:00Z`).
/// `OffsetDateTime::to_string()` produces a non-standard
/// space-separated form with a `+HH:MM:SS` offset that .NET's
/// `DateTime.TryParse` rejects; clients dropped every bar on parse
/// before this helper landed. RFC 3339 is what every JSON consumer
/// in the ecosystem (browsers, .NET, Python, Go) handles natively.
fn ts_rfc3339(t: time::OffsetDateTime) -> String {
    t.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| t.to_string())
}

impl From<oi_core::OiSnapshot> for BarDto {
    fn from(s: oi_core::OiSnapshot) -> Self {
        let opt = |o: Option<rust_decimal::Decimal>| o.map(|d| d.to_string());
        Self {
            exchange: s.instrument.exchange.code().to_owned(),
            symbol: s.instrument.symbol,
            bucket_ts: ts_rfc3339(s.bucket_ts),
            first_recv_ts: ts_rfc3339(s.first_recv_ts),
            last_recv_ts: ts_rfc3339(s.last_recv_ts),
            samples: s.samples,
            native_unit: match s.native_unit {
                oi_core::UnitKind::Coins => "coins",
                oi_core::UnitKind::Contracts => "contracts",
                oi_core::UnitKind::Usd => "usd",
            }
            .to_owned(),
            native_open: s.native_open.to_string(),
            native_high: s.native_high.to_string(),
            native_low: s.native_low.to_string(),
            native_close: s.native_close.to_string(),
            oi_coins_open: opt(s.oi_coins_open),
            oi_coins_high: opt(s.oi_coins_high),
            oi_coins_low: opt(s.oi_coins_low),
            oi_coins_close: opt(s.oi_coins_close),
            oi_usd_open: opt(s.oi_usd_open),
            oi_usd_high: opt(s.oi_usd_high),
            oi_usd_low: opt(s.oi_usd_low),
            oi_usd_close: opt(s.oi_usd_close),
            price_used_close: opt(s.price_used_close),
        }
    }
}

async fn latest(
    State(state): State<RestState>,
    Path((ex, sym)): Path<(String, String)>,
) -> impl IntoResponse {
    let _t = crate::metrics::Timer::start("REST_Latest");
    let id = match parse_id(&ex, &sym) {
        Ok(x) => x,
        Err(e) => {
            crate::metrics::inc_request("REST_Latest", "bad_request");
            return (StatusCode::BAD_REQUEST, e).into_response();
        }
    };
    match state.repo.latest(&id).await {
        Ok(Some(s)) => {
            crate::metrics::inc_request("REST_Latest", "ok");
            (StatusCode::OK, Json(BarDto::from(s))).into_response()
        }
        Ok(None) => {
            crate::metrics::inc_request("REST_Latest", "not_found");
            (StatusCode::NOT_FOUND, "no data").into_response()
        }
        Err(e) => {
            crate::metrics::inc_request("REST_Latest", "internal");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

#[derive(Deserialize)]
struct RangeQuery {
    from: String,
    to: String,
    /// Wire format. Default `row` = legacy array-of-objects (one JSON
    /// object per bar). `col` switches to columnar (parallel arrays,
    /// short keys, unix-ms timestamps) which is ~30-40% smaller before
    /// gzip and 3-5× faster to parse on the client because there's no
    /// per-bar object construction. Server side both formats are equally
    /// cheap: we build BarDto then transpose for columnar.
    #[serde(default)]
    fmt: Option<String>,
}

/// Columnar OI range payload. Field name aliases are deliberately short
/// (2 chars) because the names are the only repeated content gzip can't
/// further dedupe — `native_close` appearing 1500× costs the same gzip
/// pre-image bytes as 1500 separate entries; shrinking to `nc` cuts that
/// 6×. Optional columns are entirely omitted via `skip_serializing_if`
/// when a venue exposes nothing in that axis (e.g. coins-side data on
/// venues that only report USD OI).
#[derive(Serialize, Default)]
struct OiColumns {
    /// Marker so the client can detect format from the JSON without
    /// reading the Content-Type header (which goes through nginx).
    fmt: &'static str,
    /// Bar count. Saves one full array traversal on the client.
    n: usize,
    /// bucket_ts as unix-ms (i64). Two reasons over RFC3339 strings:
    ///  * ~13 bytes vs ~20 per ts — adds up over 1500-bar windows
    ///  * client parses i64 directly into BucketMs — no string→DateTime
    ///    fallback chain that previously dropped bars on format drift.
    ts: Vec<i64>,
    #[serde(rename = "no")] native_open: Vec<String>,
    #[serde(rename = "nh")] native_high: Vec<String>,
    #[serde(rename = "nl")] native_low: Vec<String>,
    #[serde(rename = "nc")] native_close: Vec<String>,
    #[serde(rename = "co", skip_serializing_if = "Option::is_none")]
    oi_coins_open: Option<Vec<Option<String>>>,
    #[serde(rename = "ch", skip_serializing_if = "Option::is_none")]
    oi_coins_high: Option<Vec<Option<String>>>,
    #[serde(rename = "cl", skip_serializing_if = "Option::is_none")]
    oi_coins_low: Option<Vec<Option<String>>>,
    #[serde(rename = "cc", skip_serializing_if = "Option::is_none")]
    oi_coins_close: Option<Vec<Option<String>>>,
    #[serde(rename = "uo", skip_serializing_if = "Option::is_none")]
    oi_usd_open: Option<Vec<Option<String>>>,
    #[serde(rename = "uh", skip_serializing_if = "Option::is_none")]
    oi_usd_high: Option<Vec<Option<String>>>,
    #[serde(rename = "ul", skip_serializing_if = "Option::is_none")]
    oi_usd_low: Option<Vec<Option<String>>>,
    #[serde(rename = "uc", skip_serializing_if = "Option::is_none")]
    oi_usd_close: Option<Vec<Option<String>>>,
}

impl OiColumns {
    /// Transpose Vec<OiSnapshot> into the columnar layout. Optional
    /// columns are emitted iff at least one row has a value — saves
    /// venues that never populate (say) coins-side from carrying a
    /// 1500-element null array.
    fn from_snapshots(snaps: Vec<oi_core::OiSnapshot>) -> Self {
        let n = snaps.len();
        let mut out = OiColumns { fmt: "col", n, ..Default::default() };
        out.ts = Vec::with_capacity(n);
        out.native_open = Vec::with_capacity(n);
        out.native_high = Vec::with_capacity(n);
        out.native_low = Vec::with_capacity(n);
        out.native_close = Vec::with_capacity(n);

        // First pass: detect which optional axes have any data, so we can
        // either allocate the column upfront or leave it as None.
        let mut has_co = false;
        let mut has_uo = false;
        for s in &snaps {
            if !has_co && (s.oi_coins_open.is_some() || s.oi_coins_high.is_some()
                || s.oi_coins_low.is_some() || s.oi_coins_close.is_some()) { has_co = true; }
            if !has_uo && (s.oi_usd_open.is_some() || s.oi_usd_high.is_some()
                || s.oi_usd_low.is_some() || s.oi_usd_close.is_some()) { has_uo = true; }
            if has_co && has_uo { break; }
        }
        if has_co {
            out.oi_coins_open  = Some(Vec::with_capacity(n));
            out.oi_coins_high  = Some(Vec::with_capacity(n));
            out.oi_coins_low   = Some(Vec::with_capacity(n));
            out.oi_coins_close = Some(Vec::with_capacity(n));
        }
        if has_uo {
            out.oi_usd_open  = Some(Vec::with_capacity(n));
            out.oi_usd_high  = Some(Vec::with_capacity(n));
            out.oi_usd_low   = Some(Vec::with_capacity(n));
            out.oi_usd_close = Some(Vec::with_capacity(n));
        }

        let opt = |o: &Option<rust_decimal::Decimal>| o.as_ref().map(|d| d.to_string());
        for s in snaps {
            out.ts.push((s.bucket_ts.unix_timestamp_nanos() / 1_000_000) as i64);
            out.native_open.push(s.native_open.to_string());
            out.native_high.push(s.native_high.to_string());
            out.native_low.push(s.native_low.to_string());
            out.native_close.push(s.native_close.to_string());
            if let Some(v) = out.oi_coins_open.as_mut()  { v.push(opt(&s.oi_coins_open)); }
            if let Some(v) = out.oi_coins_high.as_mut()  { v.push(opt(&s.oi_coins_high)); }
            if let Some(v) = out.oi_coins_low.as_mut()   { v.push(opt(&s.oi_coins_low)); }
            if let Some(v) = out.oi_coins_close.as_mut() { v.push(opt(&s.oi_coins_close)); }
            if let Some(v) = out.oi_usd_open.as_mut()  { v.push(opt(&s.oi_usd_open)); }
            if let Some(v) = out.oi_usd_high.as_mut()  { v.push(opt(&s.oi_usd_high)); }
            if let Some(v) = out.oi_usd_low.as_mut()   { v.push(opt(&s.oi_usd_low)); }
            if let Some(v) = out.oi_usd_close.as_mut() { v.push(opt(&s.oi_usd_close)); }
        }
        out
    }
}

async fn range(
    State(state): State<RestState>,
    Path((ex, sym)): Path<(String, String)>,
    Query(q): Query<RangeQuery>,
) -> impl IntoResponse {
    let id = match parse_id(&ex, &sym) {
        Ok(x) => x,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let from = match parse_rfc3339(&q.from) {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let to = match parse_rfc3339(&q.to) {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let columnar = matches!(q.fmt.as_deref(), Some("col"));
    match state.repo.range(&id, from, to).await {
        Ok(snaps) if columnar => Json(OiColumns::from_snapshots(snaps)).into_response(),
        Ok(snaps) => Json(snaps.into_iter().map(BarDto::from).collect::<Vec<_>>()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn parse_id(ex: &str, sym: &str) -> Result<InstrumentId, String> {
    let ex = Exchange::from_str(ex)?;
    Ok(InstrumentId::new(ex, sym.to_owned()))
}

fn parse_rfc3339(s: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map_err(|e| format!("rfc3339: {e}"))
}

#[derive(Serialize)]
struct FundingDto {
    exchange: String,
    symbol: String,
    bucket_ts: String,
    recv_ts: String,
    /// Decimal-as-string to preserve precision over JSON.
    rate: String,
    next_funding_ts: Option<String>,
    interval_hours: Option<u8>,
}

impl From<oi_core::funding::FundingBar> for FundingDto {
    fn from(b: oi_core::funding::FundingBar) -> Self {
        Self {
            exchange: b.instrument.exchange.code().to_owned(),
            symbol: b.instrument.symbol,
            bucket_ts: ts_rfc3339(b.bucket_ts),
            recv_ts: ts_rfc3339(b.recv_ts),
            rate: b.rate.to_string(),
            next_funding_ts: b.next_funding_ts.map(ts_rfc3339),
            interval_hours: b.interval_hours,
        }
    }
}

/// Columnar funding range — same rationale as `OiColumns`. The shape is
/// narrower (no nested OHLC), so the gain over row format is smaller but
/// still meaningful when 1500 bars × the "rate" key would repeat.
#[derive(Serialize, Default)]
struct FundingColumns {
    fmt: &'static str,
    n: usize,
    ts: Vec<i64>,
    #[serde(rename = "r")] rate: Vec<String>,
    #[serde(rename = "nft", skip_serializing_if = "Option::is_none")]
    next_funding_ts: Option<Vec<Option<i64>>>,
    #[serde(rename = "ih", skip_serializing_if = "Option::is_none")]
    interval_hours: Option<Vec<Option<u8>>>,
}

impl FundingColumns {
    fn from_bars(bars: Vec<oi_core::funding::FundingBar>) -> Self {
        let n = bars.len();
        let mut out = FundingColumns { fmt: "col", n, ..Default::default() };
        out.ts = Vec::with_capacity(n);
        out.rate = Vec::with_capacity(n);
        let has_nft = bars.iter().any(|b| b.next_funding_ts.is_some());
        let has_ih  = bars.iter().any(|b| b.interval_hours.is_some());
        if has_nft { out.next_funding_ts = Some(Vec::with_capacity(n)); }
        if has_ih  { out.interval_hours  = Some(Vec::with_capacity(n)); }
        for b in bars {
            out.ts.push((b.bucket_ts.unix_timestamp_nanos() / 1_000_000) as i64);
            out.rate.push(b.rate.to_string());
            if let Some(v) = out.next_funding_ts.as_mut() {
                v.push(b.next_funding_ts.map(|t| (t.unix_timestamp_nanos() / 1_000_000) as i64));
            }
            if let Some(v) = out.interval_hours.as_mut() { v.push(b.interval_hours); }
        }
        out
    }
}

async fn funding_latest(
    State(state): State<RestState>,
    Path((ex, sym)): Path<(String, String)>,
) -> impl IntoResponse {
    let _t = crate::metrics::Timer::start("REST_FundingLatest");
    let id = match parse_id(&ex, &sym) {
        Ok(x) => x,
        Err(e) => {
            crate::metrics::inc_request("REST_FundingLatest", "bad_request");
            return (StatusCode::BAD_REQUEST, e).into_response();
        }
    };
    match state.repo.latest_funding(&id).await {
        Ok(Some(b)) => {
            crate::metrics::inc_request("REST_FundingLatest", "ok");
            (StatusCode::OK, Json(FundingDto::from(b))).into_response()
        }
        Ok(None) => {
            crate::metrics::inc_request("REST_FundingLatest", "not_found");
            (StatusCode::NOT_FOUND, "no funding data").into_response()
        }
        Err(e) => {
            crate::metrics::inc_request("REST_FundingLatest", "internal");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn funding_range(
    State(state): State<RestState>,
    Path((ex, sym)): Path<(String, String)>,
    Query(q): Query<RangeQuery>,
) -> impl IntoResponse {
    let _t = crate::metrics::Timer::start("REST_FundingRange");
    let id = match parse_id(&ex, &sym) {
        Ok(x) => x,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let from = match parse_rfc3339(&q.from) {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let to = match parse_rfc3339(&q.to) {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let columnar = matches!(q.fmt.as_deref(), Some("col"));
    match state.repo.funding_range(&id, from, to).await {
        Ok(bars) if columnar => Json(FundingColumns::from_bars(bars)).into_response(),
        Ok(bars) => Json(bars.into_iter().map(FundingDto::from).collect::<Vec<_>>())
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Serialize)]
struct FundingEventDto {
    exchange: String,
    symbol: String,
    settlement_ts: String,
    rate: String,
    mark_price: Option<String>,
}

impl From<oi_core::funding::FundingEvent> for FundingEventDto {
    fn from(e: oi_core::funding::FundingEvent) -> Self {
        Self {
            exchange: e.instrument.exchange.code().to_owned(),
            symbol: e.instrument.symbol,
            settlement_ts: ts_rfc3339(e.settlement_ts),
            rate: e.rate.to_string(),
            mark_price: e.mark_price.map(|d| d.to_string()),
        }
    }
}

async fn funding_event_latest(
    State(state): State<RestState>,
    Path((ex, sym)): Path<(String, String)>,
) -> impl IntoResponse {
    let _t = crate::metrics::Timer::start("REST_FundingEventLatest");
    let id = match parse_id(&ex, &sym) {
        Ok(x) => x,
        Err(e) => {
            crate::metrics::inc_request("REST_FundingEventLatest", "bad_request");
            return (StatusCode::BAD_REQUEST, e).into_response();
        }
    };
    match state.repo.latest_funding_event(&id).await {
        Ok(Some(e)) => {
            crate::metrics::inc_request("REST_FundingEventLatest", "ok");
            (StatusCode::OK, Json(FundingEventDto::from(e))).into_response()
        }
        Ok(None) => {
            crate::metrics::inc_request("REST_FundingEventLatest", "not_found");
            (StatusCode::NOT_FOUND, "no settlement events").into_response()
        }
        Err(e) => {
            crate::metrics::inc_request("REST_FundingEventLatest", "internal");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

async fn funding_event_range(
    State(state): State<RestState>,
    Path((ex, sym)): Path<(String, String)>,
    Query(q): Query<RangeQuery>,
) -> impl IntoResponse {
    let _t = crate::metrics::Timer::start("REST_FundingEventRange");
    let id = match parse_id(&ex, &sym) {
        Ok(x) => x,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let from = match parse_rfc3339(&q.from) {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let to = match parse_rfc3339(&q.to) {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    match state.repo.funding_events_range(&id, from, to).await {
        Ok(events) => Json(events.into_iter().map(FundingEventDto::from).collect::<Vec<_>>())
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ── batch range endpoints ────────────────────────────────────────────────
//
// Three POST handlers mirroring the single-instrument GETs above. Each
// accepts a `RangeBatchRequest { items: [...] }` body and returns a
// `RangeBatchResponse { results: [...] }` whose entries are in the SAME
// order as the input items — the client matches results by index. A
// per-item `error` field carries parse / repo failures so one bad input
// doesn't kill the whole batch.

#[derive(Deserialize)]
struct RangeBatchItem {
    exchange: String,
    symbol: String,
    from: String,
    to: String,
}

#[derive(Deserialize)]
struct RangeBatchRequest {
    items: Vec<RangeBatchItem>,
    /// Same opt-in semantics as the single-range `?fmt=col` query
    /// parameter — when `"col"`, every result entry's payload is the
    /// columnar layout instead of `bars: [...]`. The format choice
    /// applies to the whole batch (you can't mix row + col per-item).
    #[serde(default)]
    fmt: Option<String>,
}

#[derive(Serialize)]
struct RangeBatchResponse<P> {
    results: Vec<BatchEntry<P>>,
}

/// Generic batch entry. `payload` is flattened into the JSON object so
/// the wire shape is `{exchange, symbol, error?, ...payload fields...}` —
/// row path embeds `{bars: [...]}` (RowPayload), columnar path embeds
/// the `{fmt, n, ts, no, ...}` columns directly at the entry level. A
/// single helper drives both formats; only the finalize closure differs.
#[derive(Serialize)]
struct BatchEntry<P> {
    exchange: String,
    symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(flatten)]
    payload: P,
}

/// Row-shaped payload. Default = no bars, used for the error-case entry
/// so a single bad input doesn't kill the batch — the client sees
/// `{exchange, symbol, error, bars: []}` per failed item.
#[derive(Serialize)]
struct RowPayload<T> {
    bars: Vec<T>,
}

// Manual Default: the derive macro would inherit `T: Default` (since
// it tries to default each field), but `Vec::<T>::new()` doesn't need
// T to be Default. Hand-rolled avoids forcing `#[derive(Default)]` on
// every DTO that ever flows through a batch endpoint.
impl<T> Default for RowPayload<T> {
    fn default() -> Self { Self { bars: Vec::new() } }
}

/// Validate every item's `(exchange, symbol, from, to)`. Returns either
/// the full parsed list (in order) or 400 with the first parse error.
/// Empty / oversize batches are also rejected here.
fn parse_batch(req: &RangeBatchRequest)
    -> Result<Vec<(InstrumentId, OffsetDateTime, OffsetDateTime)>, (StatusCode, String)>
{
    if req.items.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "items: empty".to_owned()));
    }
    if req.items.len() > BATCH_MAX_ITEMS {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("items: {} exceeds max {}", req.items.len(), BATCH_MAX_ITEMS),
        ));
    }
    let mut parsed = Vec::with_capacity(req.items.len());
    for (i, it) in req.items.iter().enumerate() {
        let id = parse_id(&it.exchange, &it.symbol)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("items[{i}]: {e}")))?;
        let from = parse_rfc3339(&it.from)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("items[{i}].from: {e}")))?;
        let to = parse_rfc3339(&it.to)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("items[{i}].to: {e}")))?;
        parsed.push((id, from, to));
    }
    Ok(parsed)
}

/// Fan out per-item queries with bounded concurrency, preserving input
/// order. `query` runs the repo call; `finalize` takes the whole per-item
/// row vec and turns it into the payload that gets flattened into the
/// JSON response (e.g. `RowPayload { bars: ... }` or `OiColumns`). The
/// finalize-takes-Vec signature is what lets the same helper drive both
/// row and columnar formats — columnar needs the full vec to transpose.
/// Per-item errors land in the response's `error` field and are logged,
/// but never abort the batch.
async fn run_batch<T, P, FQuery, Fut, FFinalize>(
    items: Vec<(InstrumentId, OffsetDateTime, OffsetDateTime)>,
    query: FQuery,
    finalize: FFinalize,
) -> Vec<BatchEntry<P>>
where
    FQuery: Fn(InstrumentId, OffsetDateTime, OffsetDateTime) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = oi_core::error::Result<Vec<T>>> + Send,
    FFinalize: Fn(Vec<T>) -> P + Send + Sync + 'static + Copy,
    T: Send + 'static,
    P: Send + Default + 'static,
{
    use std::pin::Pin;
    type ItemResult<T> = (usize, String, String, oi_core::error::Result<Vec<T>>);

    let total = items.len();
    let query = Arc::new(query);
    let mut results: Vec<Option<BatchEntry<P>>> = (0..total).map(|_| None).collect();
    // Box::pin erases the unique per-async-block type so FuturesUnordered
    // can hold an arbitrary mix of priming / refill futures.
    let mut inflight: FuturesUnordered<Pin<Box<dyn std::future::Future<Output = ItemResult<T>> + Send>>>
        = FuturesUnordered::new();
    let mut next = 0usize;

    let spawn_one = |idx: usize, item: &(InstrumentId, OffsetDateTime, OffsetDateTime)|
        -> Pin<Box<dyn std::future::Future<Output = ItemResult<T>> + Send>>
    {
        let (id, from, to) = item.clone();
        let ex = id.exchange.code().to_owned();
        let sym = id.symbol.clone();
        let q = query.clone();
        Box::pin(async move {
            let out = (*q)(id, from, to).await;
            (idx, ex, sym, out)
        })
    };

    // Prime the in-flight set up to the fan-out cap.
    while next < total && inflight.len() < BATCH_CONCURRENCY {
        inflight.push(spawn_one(next, &items[next]));
        next += 1;
    }

    while let Some((idx, exchange, symbol, out)) = inflight.next().await {
        let entry = match out {
            Ok(rows) => BatchEntry {
                exchange,
                symbol,
                error: None,
                payload: finalize(rows),
            },
            Err(e) => BatchEntry {
                exchange,
                symbol,
                error: Some(e.to_string()),
                payload: P::default(),
            },
        };
        results[idx] = Some(entry);

        if next < total {
            inflight.push(spawn_one(next, &items[next]));
            next += 1;
        }
    }

    // All slots filled by the loop above.
    results.into_iter().map(|o| o.expect("batch slot")).collect()
}

async fn range_batch(
    State(state): State<RestState>,
    Json(req): Json<RangeBatchRequest>,
) -> impl IntoResponse {
    let _t = crate::metrics::Timer::start("REST_RangeBatch");
    let items = match parse_batch(&req) {
        Ok(v) => v,
        Err((code, msg)) => return (code, msg).into_response(),
    };
    let columnar = matches!(req.fmt.as_deref(), Some("col"));
    let repo = state.repo.clone();
    if columnar {
        let results = run_batch(
            items,
            move |id, from, to| {
                let repo = repo.clone();
                async move { repo.range(&id, from, to).await }
            },
            OiColumns::from_snapshots,
        )
        .await;
        Json(RangeBatchResponse { results }).into_response()
    } else {
        let results = run_batch(
            items,
            move |id, from, to| {
                let repo = repo.clone();
                async move { repo.range(&id, from, to).await }
            },
            |rows: Vec<oi_core::OiSnapshot>| RowPayload {
                bars: rows.into_iter().map(BarDto::from).collect(),
            },
        )
        .await;
        Json(RangeBatchResponse { results }).into_response()
    }
}

async fn funding_range_batch(
    State(state): State<RestState>,
    Json(req): Json<RangeBatchRequest>,
) -> impl IntoResponse {
    let _t = crate::metrics::Timer::start("REST_FundingRangeBatch");
    let items = match parse_batch(&req) {
        Ok(v) => v,
        Err((code, msg)) => return (code, msg).into_response(),
    };
    let columnar = matches!(req.fmt.as_deref(), Some("col"));
    let repo = state.repo.clone();
    if columnar {
        let results = run_batch(
            items,
            move |id, from, to| {
                let repo = repo.clone();
                async move { repo.funding_range(&id, from, to).await }
            },
            FundingColumns::from_bars,
        )
        .await;
        Json(RangeBatchResponse { results }).into_response()
    } else {
        let results = run_batch(
            items,
            move |id, from, to| {
                let repo = repo.clone();
                async move { repo.funding_range(&id, from, to).await }
            },
            |rows: Vec<oi_core::funding::FundingBar>| RowPayload {
                bars: rows.into_iter().map(FundingDto::from).collect(),
            },
        )
        .await;
        Json(RangeBatchResponse { results }).into_response()
    }
}

async fn funding_event_range_batch(
    State(state): State<RestState>,
    Json(req): Json<RangeBatchRequest>,
) -> impl IntoResponse {
    let _t = crate::metrics::Timer::start("REST_FundingEventRangeBatch");
    let items = match parse_batch(&req) {
        Ok(v) => v,
        Err((code, msg)) => return (code, msg).into_response(),
    };
    // Funding-events stay row-only: events are sparse (≤21 per 7d for an
    // 8h venue), so the per-key repeating cost columnar would save is
    // negligible and not worth a second wire format.
    let repo = state.repo.clone();
    let results = run_batch(
        items,
        move |id, from, to| {
            let repo = repo.clone();
            async move { repo.funding_events_range(&id, from, to).await }
        },
        |rows: Vec<oi_core::funding::FundingEvent>| RowPayload {
            bars: rows.into_iter().map(FundingEventDto::from).collect(),
        },
    )
    .await;
    Json(RangeBatchResponse { results }).into_response()
}
