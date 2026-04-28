//! Aster is Binance-compatible at the API level. This test proves the
//! delegation works end-to-end: a wiremock server that serves
//! Binance-shaped payloads produces `InstrumentId`s tagged as
//! `Exchange::Aster`.

use oi_core::{exchange::Exchange, instrument::InstrumentId, traits::ExchangeAdapter};
use oi_exchanges::aster::AsterAdapter;
use rust_decimal_macros::dec;
use time::macros::datetime;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn aster_delegates_to_binance_shape_and_retags() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/fapi/v1/exchangeInfo"))
        .respond_with(ResponseTemplate::new(200).set_body_string(EXCHANGE_INFO))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/fapi/v1/openInterest"))
        .and(query_param("symbol", "BTCUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"symbol":"BTCUSDT","openInterest":"500.0","time":1714000000000}"#,
        ))
        .mount(&server)
        .await;

    let adapter = AsterAdapter::with_base_url(server.uri()).unwrap();

    let metas = adapter.discover_instruments().await.unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].id.exchange, Exchange::Aster);
    assert_eq!(metas[0].id.symbol, "BTCUSDT");

    let ids = vec![InstrumentId::new(Exchange::Aster, "BTCUSDT".to_owned())];
    let raw = adapter
        .fetch_oi(&ids, datetime!(2026-04-24 10:00:00 UTC))
        .await
        .unwrap();
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].instrument.exchange, Exchange::Aster);
    assert_eq!(raw[0].value, dec!(500.0));
}

const EXCHANGE_INFO: &str = r#"{
  "timezone":"UTC","serverTime":1714000000000,
  "symbols":[{"symbol":"BTCUSDT","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDT","status":"TRADING","filters":[]}]
}"#;
