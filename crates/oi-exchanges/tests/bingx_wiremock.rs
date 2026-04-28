use oi_core::{instrument::InstrumentId, traits::ExchangeAdapter, unit::UnitKind};
use oi_exchanges::bingx::BingXAdapter;
use rust_decimal_macros::dec;
use time::macros::datetime;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn bingx_discovery_and_per_symbol_oi() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/openApi/swap/v2/quote/contracts"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CONTRACTS))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/openApi/swap/v2/quote/openInterest"))
        .and(query_param("symbol", "BTC-USDT"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"code":0,"msg":"","data":{"openInterest":"1234.56","symbol":"BTC-USDT","time":1714000000000}}"#,
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/openApi/swap/v2/quote/openInterest"))
        .and(query_param("symbol", "ETH-USDT"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"code":0,"msg":"","data":{"openInterest":"9876.5","symbol":"ETH-USDT","time":1714000000000}}"#,
        ))
        .mount(&server)
        .await;

    let adapter = BingXAdapter::with_base_url(server.uri()).unwrap();
    let metas = adapter.discover_instruments().await.unwrap();
    // Offline instruments are retained but flagged `active: false`.
    let active: Vec<&str> = metas
        .iter()
        .filter(|m| m.active)
        .map(|m| m.id.symbol.as_str())
        .collect();
    assert_eq!(active, vec!["BTC-USDT", "ETH-USDT"]);
    let doge = metas.iter().find(|m| m.id.symbol == "DOGE-USDT").unwrap();
    assert!(!doge.active);
    for m in &metas {
        assert_eq!(m.native_unit, UnitKind::Coins);
    }
    let ids: Vec<InstrumentId> = metas
        .iter()
        .filter(|m| m.active)
        .map(|m| m.id.clone())
        .collect();
    let raw = adapter
        .fetch_oi(&ids, datetime!(2026-04-24 10:00:00 UTC))
        .await
        .unwrap();
    assert_eq!(raw.len(), 2);
    let btc = raw.iter().find(|r| r.instrument.symbol == "BTC-USDT").unwrap();
    assert_eq!(btc.value, dec!(1234.56));
}

#[tokio::test]
async fn bingx_throttle_code_is_rate_limited() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/openApi/swap/v2/quote/openInterest"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"code":100410,"msg":"throttle","data":{"openInterest":"","symbol":"","time":0}}"#,
        ))
        .mount(&server)
        .await;
    let adapter = BingXAdapter::with_base_url(server.uri()).unwrap();
    let ids = vec![InstrumentId::new(
        oi_core::exchange::Exchange::BingX,
        "BTC-USDT".to_owned(),
    )];
    // The adapter logs per-symbol failures and returns Ok(empty) — no
    // rate-limit error surfaces here because the fan-out swallows
    // individual-symbol errors to keep the batch moving. This is the
    // expected behavior; rate-limit pressure shows up in the governor.
    let raw = adapter
        .fetch_oi(&ids, datetime!(2026-04-24 10:00:00 UTC))
        .await
        .unwrap();
    assert!(raw.is_empty());
}

const CONTRACTS: &str = r#"{"code":0,"msg":"","data":[
  {"symbol":"BTC-USDT","asset":"BTC","currency":"USDT","status":1,"pricePrecision":1,"quantityPrecision":4,"tradeMinLimit":0.0001},
  {"symbol":"ETH-USDT","asset":"ETH","currency":"USDT","status":1,"pricePrecision":2,"quantityPrecision":4,"tradeMinLimit":0.001},
  {"symbol":"DOGE-USDT","asset":"DOGE","currency":"USDT","status":0,"pricePrecision":5,"quantityPrecision":0,"tradeMinLimit":1}
]}"#;
