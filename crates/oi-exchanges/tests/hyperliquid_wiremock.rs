//! End-to-end: `HyperliquidAdapter` against a wiremock `/info` endpoint.
//!
//! Verifies that one POST returns both discovery and OI data, that the
//! universe/ctxs arrays are zipped by index, and that `price_hint` is
//! populated so enrichment can skip a second round-trip.

use oi_core::{instrument::InstrumentId, traits::ExchangeAdapter, unit::UnitKind};
use oi_exchanges::hyperliquid::HyperliquidAdapter;
use rust_decimal_macros::dec;
use time::macros::datetime;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn hyperliquid_discovers_and_fetches_oi_in_one_call() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/info"))
        .and(body_partial_json(serde_json::json!({"type": "metaAndAssetCtxs"})))
        .respond_with(ResponseTemplate::new(200).set_body_string(RESPONSE))
        .mount(&server)
        .await;

    let adapter = HyperliquidAdapter::with_base_url(server.uri()).unwrap();

    let metas = adapter.discover_instruments().await.unwrap();
    let names: Vec<&str> = metas.iter().map(|m| m.id.symbol.as_str()).collect();
    assert_eq!(names, vec!["BTC", "ETH", "DOGE"]);
    for m in &metas {
        assert_eq!(m.native_unit, UnitKind::Coins);
        assert!(m.is_perpetual);
    }
    // Delisted asset flagged inactive.
    assert!(metas.iter().find(|m| m.id.symbol == "DOGE").unwrap().active == false);

    let ids: Vec<InstrumentId> = metas
        .iter()
        .filter(|m| m.active)
        .map(|m| m.id.clone())
        .collect();

    let bucket = datetime!(2026-04-24 10:00:00 UTC);
    let raw = adapter.fetch_oi(&ids, bucket).await.unwrap();
    assert_eq!(raw.len(), 2);

    let btc = raw.iter().find(|r| r.instrument.symbol == "BTC").unwrap();
    assert_eq!(btc.value, dec!(1234.5));
    assert_eq!(btc.unit, UnitKind::Coins);
    let hint = btc.price_hint.as_ref().expect("price_hint populated by co-fetch");
    assert_eq!(hint.price, dec!(64000));
}

const RESPONSE: &str = r#"[
  {"universe":[
    {"name":"BTC","szDecimals":5,"maxLeverage":50},
    {"name":"ETH","szDecimals":4,"maxLeverage":50},
    {"name":"DOGE","szDecimals":0,"maxLeverage":10,"isDelisted":true}
  ]},
  [
    {"openInterest":"1234.5","markPx":"64000","oraclePx":"63999","premium":"0.0001"},
    {"openInterest":"9876.0","markPx":"3200","oraclePx":"3200.5"},
    {"openInterest":"0","markPx":"0.12"}
  ]
]"#;
