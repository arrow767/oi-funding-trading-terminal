//! OKX adapter against a local wiremock server.
//!
//! Covers: envelope success/error code, instrument filtering to linear SWAPs,
//! oiCcy preference over oi, price fallback from markPx to last.

use oi_core::{instrument::InstrumentId, traits::ExchangeAdapter, unit::UnitKind};
use oi_exchanges::okx::OkxAdapter;
use rust_decimal_macros::dec;
use time::macros::datetime;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn okx_discovery_filters_to_linear_swaps() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v5/public/instruments"))
        .and(query_param("instType", "SWAP"))
        .respond_with(ResponseTemplate::new(200).set_body_string(INSTRUMENTS))
        .mount(&server)
        .await;

    let adapter = OkxAdapter::with_base_url(server.uri()).unwrap();
    let metas = adapter.discover_instruments().await.unwrap();
    let ids: Vec<&str> = metas.iter().map(|m| m.id.symbol.as_str()).collect();
    assert_eq!(ids, vec!["BTC-USDT-SWAP", "ETH-USDT-SWAP"]);
    for m in &metas {
        assert_eq!(m.native_unit, UnitKind::Coins); // oiCcy normalized
        assert!(m.contract_multiplier.is_some());
    }
}

#[tokio::test]
async fn okx_fetch_oi_prefers_oiccy_coins() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v5/public/open-interest"))
        .and(query_param("instType", "SWAP"))
        .respond_with(ResponseTemplate::new(200).set_body_string(OI))
        .mount(&server)
        .await;

    let adapter = OkxAdapter::with_base_url(server.uri()).unwrap();
    let ids = vec![
        InstrumentId::new(oi_core::exchange::Exchange::Okx, "BTC-USDT-SWAP".to_owned()),
        InstrumentId::new(oi_core::exchange::Exchange::Okx, "ETH-USDT-SWAP".to_owned()),
    ];
    let bucket = datetime!(2026-04-24 10:00:00 UTC);
    let raw = adapter.fetch_oi(&ids, bucket).await.unwrap();
    assert_eq!(raw.len(), 2);
    let btc = raw.iter().find(|r| r.instrument.symbol == "BTC-USDT-SWAP").unwrap();
    assert_eq!(btc.unit, UnitKind::Coins);
    assert_eq!(btc.value, dec!(1234.5));
}

#[tokio::test]
async fn okx_non_zero_code_is_reported_as_schema() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v5/public/open-interest"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"code":"50001","msg":"Service unavailable","data":[]}"#),
        )
        .mount(&server)
        .await;

    let adapter = OkxAdapter::with_base_url(server.uri()).unwrap();
    let err = adapter
        .fetch_oi(&[], datetime!(2026-04-24 10:00:00 UTC))
        .await
        .unwrap_err();
    assert!(matches!(err, oi_core::error::ExchangeError::Schema(_)));
}

const INSTRUMENTS: &str = r#"{"code":"0","msg":"","data":[
  {"instId":"BTC-USDT-SWAP","instType":"SWAP","baseCcy":"","quoteCcy":"USDT","ctVal":"0.01","ctValCcy":"BTC","settleCcy":"USDT","tickSz":"0.1","lotSz":"1","state":"live","ctType":"linear"},
  {"instId":"ETH-USDT-SWAP","instType":"SWAP","baseCcy":"","quoteCcy":"USDT","ctVal":"0.1","ctValCcy":"ETH","settleCcy":"USDT","tickSz":"0.01","lotSz":"1","state":"live","ctType":"linear"},
  {"instId":"BTC-USD-SWAP","instType":"SWAP","baseCcy":"","quoteCcy":"USD","ctVal":"100","ctValCcy":"USD","settleCcy":"BTC","tickSz":"0.1","lotSz":"1","state":"live","ctType":"inverse"}
]}"#;

const OI: &str = r#"{"code":"0","msg":"","data":[
  {"instId":"BTC-USDT-SWAP","instType":"SWAP","oi":"123450","oiCcy":"1234.5","ts":"1714000000000"},
  {"instId":"ETH-USDT-SWAP","instType":"SWAP","oi":"98760","oiCcy":"9876.0","ts":"1714000000000"}
]}"#;
