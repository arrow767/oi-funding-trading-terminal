use oi_core::{instrument::InstrumentId, traits::ExchangeAdapter, unit::UnitKind};
use oi_exchanges::mexc::MexcAdapter;
use rust_decimal_macros::dec;
use time::macros::datetime;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn mexc_discovery_captures_contract_multiplier() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/contract/detail"))
        .respond_with(ResponseTemplate::new(200).set_body_string(DETAIL))
        .mount(&server)
        .await;
    let adapter = MexcAdapter::with_base_url(server.uri()).unwrap();
    let metas = adapter.discover_instruments().await.unwrap();
    let btc = metas.iter().find(|m| m.id.symbol == "BTC_USDT").unwrap();
    assert_eq!(btc.native_unit, UnitKind::Contracts);
    assert_eq!(btc.contract_multiplier, Some(dec!(0.0001)));
    let delisted = metas.iter().find(|m| m.id.symbol == "OLD_USDT").unwrap();
    assert!(!delisted.active);
}

#[tokio::test]
async fn mexc_fetch_oi_returns_contracts_with_price_hint() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/contract/ticker"))
        .respond_with(ResponseTemplate::new(200).set_body_string(TICKER))
        .mount(&server)
        .await;
    let adapter = MexcAdapter::with_base_url(server.uri()).unwrap();
    let ids = vec![InstrumentId::new(
        oi_core::exchange::Exchange::Mexc,
        "BTC_USDT".to_owned(),
    )];
    let raw = adapter
        .fetch_oi(&ids, datetime!(2026-04-24 10:00:00 UTC))
        .await
        .unwrap();
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].value, dec!(123456));
    assert_eq!(raw[0].unit, UnitKind::Contracts);
    assert_eq!(raw[0].price_hint.as_ref().unwrap().price, dec!(64001));
}

const DETAIL: &str = r#"{"success":true,"code":0,"data":[
  {"symbol":"BTC_USDT","baseCoin":"BTC","quoteCoin":"USDT","contractSize":0.0001,"priceUnit":0.1,"volUnit":1,"state":0},
  {"symbol":"ETH_USDT","baseCoin":"ETH","quoteCoin":"USDT","contractSize":0.01,"priceUnit":0.01,"volUnit":1,"state":0},
  {"symbol":"OLD_USDT","baseCoin":"OLD","quoteCoin":"USDT","contractSize":1,"state":2}
]}"#;

const TICKER: &str = r#"{"success":true,"code":0,"data":[
  {"symbol":"BTC_USDT","holdVol":123456,"fairPrice":64001,"lastPrice":64000,"indexPrice":64000.5,"timestamp":1714000000000},
  {"symbol":"ETH_USDT","holdVol":50000,"fairPrice":3200,"lastPrice":3201,"indexPrice":3200.5,"timestamp":1714000000000}
]}"#;
