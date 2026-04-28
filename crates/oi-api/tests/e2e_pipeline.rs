//! End-to-end black-box: fake Binance → collector pipeline → real
//! ClickHouse + Redis → REST endpoint. Asserts the bar we mocked is
//! the bar the API serves.
//!
//! Requires Docker. Opt in with:
//! ```sh
//! cargo test -p oi-api -- --ignored e2e_binance_through_rest
//! ```

use oi_core::{
    instrument::InstrumentId,
    snapshot::{OiSample, OiSnapshot},
    traits::ExchangeAdapter,
    traits::OiRepository,
};
use oi_exchanges::binance::BinanceUsdmAdapter;
use oi_storage::{clickhouse::ClickHouseRepo, redis::RedisCache, CompositeRepository};
use std::sync::Arc;
use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};
use time::macros::datetime;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn e2e_binance_through_rest() {
    // 1. Spin up ClickHouse and Redis in containers.
    let ch_container = GenericImage::new("clickhouse/clickhouse-server", "24.8")
        .with_wait_for(WaitFor::message_on_stderr("Ready for connections"))
        .with_exposed_port(8123.into())
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .start()
        .await
        .expect("clickhouse up");
    let ch_port = ch_container.get_host_port_ipv4(8123).await.unwrap();
    let ch_url = format!("http://127.0.0.1:{ch_port}");

    let redis_container = GenericImage::new("redis", "7.4-alpine")
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .with_exposed_port(6379.into())
        .start()
        .await
        .expect("redis up");
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let redis_url = format!("redis://127.0.0.1:{redis_port}");

    // 2. Fake Binance.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/fapi/v1/exchangeInfo"))
        .respond_with(ResponseTemplate::new(200).set_body_string(EXCHANGE_INFO))
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/fapi/v1/openInterest"))
        .and(query_param("symbol", "BTCUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"symbol":"BTCUSDT","openInterest":"777.5","time":1714000000000}"#,
        ))
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/fapi/v1/premiumIndex"))
        .respond_with(ResponseTemplate::new(200).set_body_string(PREMIUM_INDEX))
        .mount(&mock)
        .await;

    // 3. Wire up storage + apply schema.
    let ch_repo = ClickHouseRepo::new(&ch_url, "oi", "default", "");
    ch_repo.ensure_schema().await.expect("schema");
    let redis_cache = RedisCache::connect(&redis_url).await.expect("redis connect");
    let composite =
        CompositeRepository::new(ch_repo.clone(), redis_cache.clone());
    let repo: Arc<dyn OiRepository> = Arc::new(composite);

    // 4. Run the pipeline manually (no full runner — one tick is
    // enough to prove the data path).
    let adapter = BinanceUsdmAdapter::with_base_url(mock.uri()).unwrap();
    let metas = adapter.discover_instruments().await.expect("discover");
    repo.upsert_instruments(&metas).await.expect("upsert meta");
    let ids: Vec<InstrumentId> = metas.iter().map(|m| m.id.clone()).collect();

    // Price first (populates in-sample price_hint via premiumIndex).
    let quotes = adapter.fetch_prices(&ids).await.expect("prices");
    let price_by_inst: std::collections::HashMap<_, _> =
        quotes.into_iter().map(|q| (q.instrument.clone(), q.price)).collect();

    let bucket = datetime!(2026-04-24 10:00:00 UTC);
    let raw = adapter.fetch_oi(&ids, bucket).await.expect("fetch oi");
    let meta_by_id: std::collections::HashMap<_, _> =
        metas.iter().map(|m| (m.id.clone(), m)).collect();

    // Single-sample → degenerate bar via the aggregator's
    // `start_from_sample` constructor. Mirrors what the runner
    // would do on a one-tick minute.
    let mut snaps: Vec<OiSnapshot> = Vec::new();
    for r in raw {
        let meta = meta_by_id[&r.instrument];
        let price = price_by_inst.get(&r.instrument).copied();
        let sample = OiSample::enrich(r, meta, price).expect("enrich");
        snaps.push(OiSnapshot::start_from_sample(sample));
    }
    repo.upsert_snapshots(&snaps).await.expect("upsert snaps");

    // 5. Stand up the REST server around the same repo.
    let rest_state = oi_api::rest::RestState {
        repo: repo.clone(),
        clickhouse: Some(ch_repo),
        redis: Some(redis_cache),
    };
    let router = oi_api::rest::router(rest_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest_addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    // 6. Hit /health/ready — both probes should be green.
    let http = reqwest::Client::new();
    let ready = http
        .get(format!("http://{rest_addr}/health/ready"))
        .send()
        .await
        .unwrap();
    assert_eq!(ready.status(), 200, "ready should be OK");

    // 7. Hit /v1/oi/latest/binance/BTCUSDT — assert the bar we mocked.
    let bar_resp = http
        .get(format!("http://{rest_addr}/v1/oi/latest/binance/BTCUSDT"))
        .send()
        .await
        .unwrap();
    assert_eq!(bar_resp.status(), 200);
    let bar_json: serde_json::Value = bar_resp.json().await.unwrap();
    assert_eq!(bar_json["exchange"], "binance");
    assert_eq!(bar_json["symbol"], "BTCUSDT");
    // The aggregator promoted the single REST sample to a
    // degenerate bar where O=H=L=C — so all four match.
    assert_eq!(bar_json["native_open"], "777.5");
    assert_eq!(bar_json["native_close"], "777.5");
    assert_eq!(bar_json["samples"], 1);
    // USD = 777.5 * 64000.50
    assert_eq!(bar_json["oi_usd_close"], "49760388.75");

    server_handle.abort();
}

const EXCHANGE_INFO: &str = r#"{
  "timezone":"UTC","serverTime":1714000000000,
  "symbols":[{"symbol":"BTCUSDT","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDT","status":"TRADING","filters":[]}]
}"#;

const PREMIUM_INDEX: &str = r#"[
  {"symbol":"BTCUSDT","markPrice":"64000.50","indexPrice":"64010.00","estimatedSettlePrice":"63990.00","lastFundingRate":"0.0001","nextFundingTime":1714003200000,"interestRate":"0.0001","time":1714000000000}
]"#;
