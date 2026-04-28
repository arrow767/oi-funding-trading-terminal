use oi_core::{instrument::InstrumentId, traits::ExchangeAdapter, unit::UnitKind};
use oi_exchanges::bitget::BitgetAdapter;
use rust_decimal_macros::dec;
use time::macros::datetime;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn bitget_discovery_filters_perpetuals_only() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/mix/market/contracts"))
        .and(query_param("productType", "USDT-FUTURES"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CONTRACTS))
        .mount(&server)
        .await;
    let adapter = BitgetAdapter::with_base_url(server.uri()).unwrap();
    let metas = adapter.discover_instruments().await.unwrap();
    let syms: Vec<&str> = metas.iter().map(|m| m.id.symbol.as_str()).collect();
    assert_eq!(syms, vec!["BTCUSDT", "ETHUSDT"]);
    for m in &metas {
        assert_eq!(m.native_unit, UnitKind::Coins);
    }
}

#[tokio::test]
async fn bitget_fetch_oi_uses_holding_amount_and_mark_price() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/mix/market/tickers"))
        .and(query_param("productType", "USDT-FUTURES"))
        .respond_with(ResponseTemplate::new(200).set_body_string(TICKERS))
        .mount(&server)
        .await;
    let adapter = BitgetAdapter::with_base_url(server.uri()).unwrap();
    let ids = vec![InstrumentId::new(
        oi_core::exchange::Exchange::Bitget,
        "BTCUSDT".to_owned(),
    )];
    let raw = adapter
        .fetch_oi(&ids, datetime!(2026-04-24 10:00:00 UTC))
        .await
        .unwrap();
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].value, dec!(12345.67));
    assert_eq!(raw[0].unit, UnitKind::Coins);
    let hint = raw[0].price_hint.as_ref().unwrap();
    assert_eq!(hint.price, dec!(64001));
}

const CONTRACTS: &str = r#"{"code":"00000","msg":"success","data":[
  {"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","symbolType":"perpetual","symbolStatus":"normal","minTradeNum":"0.001"},
  {"symbol":"ETHUSDT","baseCoin":"ETH","quoteCoin":"USDT","symbolType":"perpetual","symbolStatus":"normal","minTradeNum":"0.01"},
  {"symbol":"SOLUSDT_240628","baseCoin":"SOL","quoteCoin":"USDT","symbolType":"delivery","symbolStatus":"normal"}
]}"#;

const TICKERS: &str = r#"{"code":"00000","msg":"success","data":[
  {"symbol":"BTCUSDT","holdingAmount":"12345.67","markPrice":"64001","indexPrice":"64000.5","lastPr":"64000","ts":"1714000000000"},
  {"symbol":"ETHUSDT","holdingAmount":"200000","markPrice":"3200","indexPrice":"3200.5","lastPr":"3201","ts":"1714000000000"}
]}"#;
