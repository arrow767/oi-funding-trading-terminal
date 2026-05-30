//! End-to-end: `BinanceUsdmAdapter` pointed at a local `wiremock` server.
//!
//! Proves discovery, OI fan-out, and price batch all consume real response
//! shapes. Any Binance schema change that affects these paths surfaces here
//! rather than in production.

use oi_core::{exchange::Exchange, instrument::InstrumentId, traits::ExchangeAdapter, unit::UnitKind};
use oi_exchanges::binance::BinanceUsdmAdapter;
use rust_decimal_macros::dec;
use time::macros::datetime;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn binance_discovery_oi_and_prices_end_to_end() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/fapi/v1/exchangeInfo"))
        .respond_with(ResponseTemplate::new(200).set_body_string(EXCHANGE_INFO))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/fapi/v1/openInterest"))
        .and(query_param("symbol", "BTCUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"symbol":"BTCUSDT","openInterest":"80123.456","time":1714000000000}"#,
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/fapi/v1/openInterest"))
        .and(query_param("symbol", "ETHUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"symbol":"ETHUSDT","openInterest":"555000.0","time":1714000000000}"#,
        ))
        .mount(&server)
        .await;

    // TradFi perp (equity): contractType TRADIFI_PERPETUAL must be
    // discovered and polled exactly like a crypto PERPETUAL.
    Mock::given(method("GET"))
        .and(path("/fapi/v1/openInterest"))
        .and(query_param("symbol", "TSLAUSDT"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"symbol":"TSLAUSDT","openInterest":"43965.43","time":1714000000000}"#,
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/fapi/v1/premiumIndex"))
        .respond_with(ResponseTemplate::new(200).set_body_string(PREMIUM_INDEX))
        .mount(&server)
        .await;

    let adapter = BinanceUsdmAdapter::with_base_url(server.uri()).unwrap();

    // Discovery: every perpetual class (crypto PERPETUAL + TradFi
    // TRADIFI_PERPETUAL) is kept; dated quarterly contracts are skipped.
    let metas = adapter.discover_instruments().await.unwrap();
    let symbols: Vec<&str> = metas.iter().map(|m| m.id.symbol.as_str()).collect();
    assert_eq!(symbols, vec!["BTCUSDT", "ETHUSDT", "TSLAUSDT"]);
    for m in &metas {
        assert_eq!(m.native_unit, UnitKind::Coins);
        assert!(m.is_perpetual);
        assert!(m.active);
    }

    let ids: Vec<InstrumentId> = metas.iter().map(|m| m.id.clone()).collect();

    // OI fan-out: all perpetuals returned, correct unit + value.
    let bucket = datetime!(2026-04-24 10:00:00 UTC);
    let raw = adapter.fetch_oi(&ids, bucket).await.unwrap();
    assert_eq!(raw.len(), 3);
    let btc = raw.iter().find(|r| r.instrument.symbol == "BTCUSDT").unwrap();
    assert_eq!(btc.unit, UnitKind::Coins);
    assert_eq!(btc.value, dec!(80123.456));
    assert_eq!(btc.bucket_ts, bucket);
    let tsla = raw.iter().find(|r| r.instrument.symbol == "TSLAUSDT").unwrap();
    assert_eq!(tsla.unit, UnitKind::Coins);
    assert_eq!(tsla.value, dec!(43965.43));

    // Prices: batch call filters to requested symbols.
    let quotes = adapter.fetch_prices(&ids).await.unwrap();
    let btc_price = quotes.iter().find(|q| q.instrument.symbol == "BTCUSDT").unwrap();
    assert_eq!(btc_price.price, dec!(64000.50));
    assert_eq!(btc_price.instrument.exchange, Exchange::Binance);
}

#[tokio::test]
async fn binance_handles_rate_limit_response() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/fapi/v1/exchangeInfo"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "2")
                .set_body_string(r#"{"code":-1003,"msg":"Too many requests"}"#),
        )
        .mount(&server)
        .await;

    let adapter = BinanceUsdmAdapter::with_base_url(server.uri()).unwrap();
    let err = adapter.discover_instruments().await.unwrap_err();
    match err {
        oi_core::error::ExchangeError::RateLimited { retry_after } => {
            assert_eq!(retry_after, Some(std::time::Duration::from_secs(2)));
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }
}

const EXCHANGE_INFO: &str = r#"{
  "timezone": "UTC",
  "serverTime": 1714000000000,
  "symbols": [
    {
      "symbol": "BTCUSDT",
      "contractType": "PERPETUAL",
      "baseAsset": "BTC",
      "quoteAsset": "USDT",
      "status": "TRADING",
      "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
        {"filterType": "LOT_SIZE", "stepSize": "0.001"}
      ]
    },
    {
      "symbol": "ETHUSDT",
      "contractType": "PERPETUAL",
      "baseAsset": "ETH",
      "quoteAsset": "USDT",
      "status": "TRADING",
      "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
        {"filterType": "LOT_SIZE", "stepSize": "0.001"}
      ]
    },
    {
      "symbol": "BTCUSDT_240628",
      "contractType": "CURRENT_QUARTER",
      "baseAsset": "BTC",
      "quoteAsset": "USDT",
      "status": "TRADING",
      "filters": []
    },
    {
      "symbol": "TSLAUSDT",
      "contractType": "TRADIFI_PERPETUAL",
      "underlyingType": "EQUITY",
      "baseAsset": "TSLA",
      "quoteAsset": "USDT",
      "status": "TRADING",
      "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
        {"filterType": "LOT_SIZE", "stepSize": "0.01"}
      ]
    }
  ]
}"#;

const PREMIUM_INDEX: &str = r#"[
  {"symbol":"BTCUSDT","markPrice":"64000.50","indexPrice":"64010.00","estimatedSettlePrice":"63990.00","lastFundingRate":"0.0001","nextFundingTime":1714003200000,"interestRate":"0.0001","time":1714000000000},
  {"symbol":"ETHUSDT","markPrice":"3200.10","indexPrice":"3201.00","estimatedSettlePrice":"3199.90","lastFundingRate":"0.0001","nextFundingTime":1714003200000,"interestRate":"0.0001","time":1714000000000},
  {"symbol":"DOGEUSDT","markPrice":"0.12","indexPrice":"0.12","estimatedSettlePrice":"0.12","lastFundingRate":"0.0001","nextFundingTime":1714003200000,"interestRate":"0.0001","time":1714000000000}
]"#;
