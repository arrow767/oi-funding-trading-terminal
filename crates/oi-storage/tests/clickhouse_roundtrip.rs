//! Integration test: spin up ClickHouse in a testcontainer, apply the
//! schema, upsert a few snapshots, read them back via `range()` and
//! `latest()`.
//!
//! Requires a Docker daemon. When Docker is unavailable (air-gapped CI,
//! no containerd) the test is skipped via the `#[ignore]` attribute —
//! opt in with `cargo test -- --ignored clickhouse_roundtrip`.

use oi_core::{
    exchange::Exchange,
    instrument::{InstrumentId, InstrumentMeta},
    snapshot::OiSnapshot,
    traits::OiRepository,
    unit::UnitKind,
};
use oi_storage::clickhouse::ClickHouseRepo;
use rust_decimal_macros::dec;
use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};
use time::macros::datetime;

#[tokio::test]
#[ignore = "requires docker; opt in with --ignored"]
async fn clickhouse_schema_and_roundtrip() {
    // Pin to a known-good version. The image exposes 8123 (HTTP) and
    // 9000 (native); we only use HTTP since the clickhouse crate
    // drives that.
    let container = GenericImage::new("clickhouse/clickhouse-server", "24.8")
        .with_wait_for(WaitFor::message_on_stderr("Ready for connections"))
        .with_exposed_port(8123.into())
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .start()
        .await
        .expect("start clickhouse container");

    let host_port = container
        .get_host_port_ipv4(8123)
        .await
        .expect("mapped port");
    let url = format!("http://127.0.0.1:{host_port}");

    let repo = ClickHouseRepo::new(&url, "oi", "default", "");
    repo.ensure_schema().await.expect("schema");

    // Register the instrument first so FK-style reads are coherent.
    let id = InstrumentId::new(Exchange::Binance, "BTCUSDT".to_owned());
    let meta = InstrumentMeta {
        id: id.clone(),
        base_asset: "BTC".into(),
        quote_asset: "USDT".into(),
        is_perpetual: true,
        native_unit: UnitKind::Coins,
        contract_multiplier: None,
        price_tick: Some(dec!(0.1)),
        qty_step: Some(dec!(0.001)),
        active: true,
    };
    repo.upsert_instruments(&[meta]).await.expect("upsert meta");

    // Insert three buckets.
    let base = datetime!(2026-04-24 10:00:00 UTC);
    let snaps = vec![
        snap(&id, base, dec!(100.0), dec!(64_000)),
        snap(
            &id,
            base + time::Duration::minutes(1),
            dec!(110.0),
            dec!(64_100),
        ),
        snap(
            &id,
            base + time::Duration::minutes(2),
            dec!(95.0),
            dec!(63_900),
        ),
    ];
    repo.upsert_snapshots(&snaps).await.expect("upsert snaps");

    // Range across all three.
    let got = repo
        .range(
            &id,
            base,
            base + time::Duration::minutes(5),
        )
        .await
        .expect("range");
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].native_close, dec!(100.0));
    assert_eq!(got[2].native_close, dec!(95.0));

    // Latest returns the most recent bucket.
    let latest = repo.latest(&id).await.expect("latest").expect("some");
    assert_eq!(latest.bucket_ts, base + time::Duration::minutes(2));
    assert_eq!(latest.native_close, dec!(95.0));

    // Idempotency: re-upsert the middle bucket with a new value; the
    // `ReplacingMergeTree` engine picks the later `ingest_ts`. A
    // `FINAL` read is needed to guarantee dedup at query time.
    let repeat = snap(
        &id,
        base + time::Duration::minutes(1),
        dec!(999.0),
        dec!(64_100),
    );
    repo.upsert_snapshots(&[repeat]).await.expect("re-upsert");
}

/// Build a degenerate one-sample bar (open=high=low=close) so the
/// CH round-trip exercises real OHLC fields without manufacturing
/// fake intra-minute fluctuation.
fn snap(
    id: &InstrumentId,
    bucket: time::OffsetDateTime,
    oi: rust_decimal::Decimal,
    price: rust_decimal::Decimal,
) -> OiSnapshot {
    let recv = bucket + time::Duration::seconds(2);
    OiSnapshot {
        instrument: id.clone(),
        bucket_ts: bucket,
        first_recv_ts: recv,
        last_recv_ts: recv,
        samples: 1,
        native_unit: UnitKind::Coins,
        native_open: oi,
        native_high: oi,
        native_low: oi,
        native_close: oi,
        oi_coins_open: Some(oi),
        oi_coins_high: Some(oi),
        oi_coins_low: Some(oi),
        oi_coins_close: Some(oi),
        oi_usd_open: Some(oi * price),
        oi_usd_high: Some(oi * price),
        oi_usd_low: Some(oi * price),
        oi_usd_close: Some(oi * price),
        price_used_close: Some(price),
    }
}
