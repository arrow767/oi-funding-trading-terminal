//! Bybit adapter against wiremock.
//!
//! Covers: paginated discovery, batch tickers (OI + co-fetched price),
//! envelope error mapping (rate limit vs. schema).

use oi_core::{instrument::InstrumentId, traits::ExchangeAdapter, unit::UnitKind};
use oi_exchanges::bybit::BybitAdapter;
use rust_decimal_macros::dec;
use time::macros::datetime;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn bybit_discovery_follows_cursor_pagination() {
    let server = MockServer::start().await;

    // Page 1 has a non-empty cursor → server should follow.
    Mock::given(method("GET"))
        .and(path("/v5/market/instruments-info"))
        .and(query_param("category", "linear"))
        .and(query_param("cursor", "next_page_token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(PAGE_2))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v5/market/instruments-info"))
        .and(query_param("category", "linear"))
        .respond_with(ResponseTemplate::new(200).set_body_string(PAGE_1))
        .mount(&server)
        .await;

    let adapter = BybitAdapter::with_base_url(server.uri()).unwrap();
    let metas = adapter.discover_instruments().await.unwrap();

    // Page 1 BTC + ETH, page 2 DOGE — only linear perpetuals.
    // SOLUSDT_240628 (LinearFutures) filtered out.
    let symbols: Vec<&str> = metas.iter().map(|m| m.id.symbol.as_str()).collect();
    assert_eq!(symbols, vec!["BTCUSDT", "ETHUSDT", "DOGEUSDT"]);
    for m in &metas {
        assert_eq!(m.native_unit, UnitKind::Coins);
        assert!(m.is_perpetual);
    }
    assert_eq!(metas[0].price_tick, Some(dec!(0.10)));
    assert_eq!(metas[0].qty_step, Some(dec!(0.001)));
}

#[tokio::test]
async fn bybit_fetch_oi_includes_mark_price_hint() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v5/market/tickers"))
        .and(query_param("category", "linear"))
        .respond_with(ResponseTemplate::new(200).set_body_string(TICKERS))
        .mount(&server)
        .await;

    let adapter = BybitAdapter::with_base_url(server.uri()).unwrap();
    let ids = vec![
        InstrumentId::new(oi_core::exchange::Exchange::Bybit, "BTCUSDT".to_owned()),
        InstrumentId::new(oi_core::exchange::Exchange::Bybit, "ETHUSDT".to_owned()),
    ];
    let bucket = datetime!(2026-04-24 10:00:00 UTC);
    let raw = adapter.fetch_oi(&ids, bucket).await.unwrap();
    assert_eq!(raw.len(), 2);

    let btc = raw.iter().find(|r| r.instrument.symbol == "BTCUSDT").unwrap();
    assert_eq!(btc.value, dec!(12345.67));
    assert_eq!(btc.unit, UnitKind::Coins);
    let hint = btc.price_hint.as_ref().expect("price co-fetched from tickers");
    assert_eq!(hint.price, dec!(64001.00));
}

#[tokio::test]
async fn bybit_rate_limit_code_surfaces_as_rate_limited() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v5/market/tickers"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"retCode":10006,"retMsg":"too many requests","result":{"list":[]}}"#,
        ))
        .mount(&server)
        .await;

    let adapter = BybitAdapter::with_base_url(server.uri()).unwrap();
    let err = adapter
        .fetch_oi(&[], datetime!(2026-04-24 10:00:00 UTC))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        oi_core::error::ExchangeError::RateLimited { .. }
    ));
}

const PAGE_1: &str = r#"{
  "retCode": 0,
  "retMsg": "OK",
  "result": {
    "category": "linear",
    "nextPageCursor": "next_page_token",
    "list": [
      {"symbol":"BTCUSDT","contractType":"LinearPerpetual","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","priceFilter":{"tickSize":"0.10"},"lotSizeFilter":{"qtyStep":"0.001"}},
      {"symbol":"ETHUSDT","contractType":"LinearPerpetual","status":"Trading","baseCoin":"ETH","quoteCoin":"USDT","priceFilter":{"tickSize":"0.01"},"lotSizeFilter":{"qtyStep":"0.001"}},
      {"symbol":"SOLUSDT_240628","contractType":"LinearFutures","status":"Trading","baseCoin":"SOL","quoteCoin":"USDT"}
    ]
  }
}"#;

const PAGE_2: &str = r#"{
  "retCode": 0,
  "retMsg": "OK",
  "result": {
    "category": "linear",
    "nextPageCursor": "",
    "list": [
      {"symbol":"DOGEUSDT","contractType":"LinearPerpetual","status":"Trading","baseCoin":"DOGE","quoteCoin":"USDT","priceFilter":{"tickSize":"0.00001"},"lotSizeFilter":{"qtyStep":"1"}}
    ]
  }
}"#;

const TICKERS: &str = r#"{
  "retCode": 0,
  "retMsg": "OK",
  "result": {
    "category": "linear",
    "list": [
      {"symbol":"BTCUSDT","openInterest":"12345.67","openInterestValue":"789012345.67","markPrice":"64001.00","lastPrice":"64000.00","indexPrice":"64000.50"},
      {"symbol":"ETHUSDT","openInterest":"200000.0","openInterestValue":"640000000","markPrice":"3200.00","lastPrice":"3200.10","indexPrice":"3200.05"},
      {"symbol":"DOGEUSDT","openInterest":"5000000","openInterestValue":"600000","markPrice":"0.12","lastPrice":"0.12","indexPrice":"0.12"}
    ]
  }
}"#;
