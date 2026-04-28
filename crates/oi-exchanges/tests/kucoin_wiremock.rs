use oi_core::{instrument::InstrumentId, traits::ExchangeAdapter, unit::UnitKind};
use oi_exchanges::kucoin::KuCoinAdapter;
use rust_decimal_macros::dec;
use time::macros::datetime;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn kucoin_single_call_covers_discovery_and_oi() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/contracts/active"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CONTRACTS))
        .mount(&server)
        .await;

    let adapter = KuCoinAdapter::with_base_url(server.uri()).unwrap();

    let metas = adapter.discover_instruments().await.unwrap();
    // Linear perps only — inverse `XBTUSDM` filtered out.
    let syms: Vec<&str> = metas.iter().map(|m| m.id.symbol.as_str()).collect();
    assert_eq!(syms, vec!["XBTUSDTM", "ETHUSDTM"]);
    for m in &metas {
        assert_eq!(m.native_unit, UnitKind::Contracts);
        assert!(m.contract_multiplier.is_some());
    }
    let btc_meta = metas.iter().find(|m| m.id.symbol == "XBTUSDTM").unwrap();
    assert_eq!(btc_meta.contract_multiplier, Some(dec!(0.001)));

    let ids: Vec<InstrumentId> = metas.iter().map(|m| m.id.clone()).collect();
    let raw = adapter
        .fetch_oi(&ids, datetime!(2026-04-24 10:00:00 UTC))
        .await
        .unwrap();
    assert_eq!(raw.len(), 2);
    let btc = raw.iter().find(|r| r.instrument.symbol == "XBTUSDTM").unwrap();
    assert_eq!(btc.value, dec!(123456));
    assert_eq!(btc.unit, UnitKind::Contracts);
    let hint = btc.price_hint.as_ref().unwrap();
    assert_eq!(hint.price, dec!(64001));
}

#[tokio::test]
async fn kucoin_429000_code_surfaces_as_rate_limited() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/contracts/active"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"code":"429000","msg":"throttled","data":[]}"#,
        ))
        .mount(&server)
        .await;
    let adapter = KuCoinAdapter::with_base_url(server.uri()).unwrap();
    let err = adapter.discover_instruments().await.unwrap_err();
    assert!(matches!(err, oi_core::error::ExchangeError::RateLimited { .. }));
}

const CONTRACTS: &str = r#"{"code":"200000","data":[
  {"symbol":"XBTUSDTM","rootSymbol":"USDT","type":"FFWCSX","baseCurrency":"XBT","quoteCurrency":"USDT","settleCurrency":"USDT","multiplier":0.001,"tickSize":0.1,"openInterest":"123456","markPrice":64001,"lastTradePrice":64000,"indexPrice":64000.5,"isInverse":false,"status":"Open"},
  {"symbol":"ETHUSDTM","rootSymbol":"USDT","type":"FFWCSX","baseCurrency":"ETH","quoteCurrency":"USDT","settleCurrency":"USDT","multiplier":0.01,"tickSize":0.01,"openInterest":"98760","markPrice":3200,"lastTradePrice":3201,"indexPrice":3200.5,"isInverse":false,"status":"Open"},
  {"symbol":"XBTUSDM","rootSymbol":"USD","type":"FFICSX","baseCurrency":"XBT","quoteCurrency":"USD","settleCurrency":"XBT","multiplier":1,"tickSize":1,"openInterest":"1000","markPrice":64000,"lastTradePrice":64000,"indexPrice":64000.5,"isInverse":true,"status":"Open"}
]}"#;
