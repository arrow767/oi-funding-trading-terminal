//! TLS termination round-trip: generate a self-signed cert via
//! `rcgen`, write it to temp files, boot the same `axum_server::bind_rustls`
//! path the production server uses, and verify HTTPS actually serves
//! a request.
//!
//! This catches regressions in the cert-loading path (relative paths,
//! key encoding mismatches, rustls feature flags) that pure-config
//! tests can't.

use axum::Router;
use oi_api::{
    auth::{rest_auth_middleware, AuthState},
    rest::{router, RestState},
};
use std::sync::Arc;

#[tokio::test]
async fn axum_server_serves_over_rustls_with_self_signed_cert() {
    // rustls 0.23 requires explicit provider selection — same call
    // production makes in `oi_api::server::run_async`. Ignore the
    // result: first call wins, subsequent calls return Err.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // 1. Generate a self-signed cert covering 127.0.0.1.
    let cert_key = rcgen::generate_simple_self_signed(vec![
        "127.0.0.1".to_owned(),
        "localhost".to_owned(),
    ])
    .expect("rcgen self-signed");

    // Write PEMs to temp files — the production loader reads from
    // disk, and that's the path we want to exercise.
    let dir = std::env::temp_dir().join(format!(
        "oi-api-tls-{}-{}",
        std::process::id(),
        rand_id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert_key.cert.pem()).unwrap();
    std::fs::write(&key_path, cert_key.key_pair.serialize_pem()).unwrap();

    // 2. Build the same router shape production uses (auth layered on
    // top so we exercise BOTH features in one shot).
    let state = RestState {
        repo: Arc::new(NoopRepo),
        clickhouse: None,
        redis: None,
    };
    let auth = AuthState::new(vec!["s3cr3t".into()]);
    let app: Router = router(state).layer(axum::middleware::from_fn_with_state(
        auth,
        rest_auth_middleware,
    ));

    // Bind on an ephemeral port so parallel test runs don't collide.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener); // axum_server::bind_rustls needs to bind itself.

    let cfg = axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .expect("load rustls cfg");

    // 3. Server task.
    let server = tokio::spawn(async move {
        axum_server::bind_rustls(addr, cfg)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    // Tiny back-off so the listener is up. axum_server doesn't expose
    // a "ready" hook; for a one-shot test, polling once after a brief
    // sleep is the simplest reliable signal.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 4. Client trusts the self-signed cert. We use
    // `danger_accept_invalid_certs` instead of feeding rcgen's CA back
    // because that's what every CI integration test does — the goal
    // is to prove the SERVER speaks valid TLS, not to test cert
    // validation.
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    // Public path — TLS handshake + auth bypass.
    let r = client
        .get(format!("https://{addr}/health/live"))
        .send()
        .await
        .expect("https GET /health/live");
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "live");

    // Protected path with valid bearer — TLS + auth happy path.
    let r = client
        .get(format!("https://{addr}/v1/oi/latest/binance/BTCUSDT"))
        .bearer_auth("s3cr3t")
        .send()
        .await
        .expect("https GET protected");
    // 404 because NoopRepo has no data — but the request reached the
    // handler, so TLS + auth both worked.
    assert_eq!(r.status(), 404);

    // Protected path without bearer — TLS works, auth rejects.
    let r = client
        .get(format!("https://{addr}/v1/oi/latest/binance/BTCUSDT"))
        .send()
        .await
        .expect("https GET protected (no auth)");
    assert_eq!(r.status(), 401);

    server.abort();
}

fn rand_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{nanos:x}")
}

// --- minimal in-memory repo ---------------------------------------

use async_trait::async_trait;
use oi_core::{
    error::Result,
    instrument::{InstrumentId, InstrumentMeta},
    snapshot::OiSnapshot,
    traits::OiRepository,
};
use time::OffsetDateTime;

#[derive(Debug)]
struct NoopRepo;

#[async_trait]
impl OiRepository for NoopRepo {
    async fn upsert_snapshots(&self, _: &[OiSnapshot]) -> Result<()> {
        Ok(())
    }
    async fn upsert_instruments(&self, _: &[InstrumentMeta]) -> Result<()> {
        Ok(())
    }
    async fn range(
        &self,
        _: &InstrumentId,
        _: OffsetDateTime,
        _: OffsetDateTime,
    ) -> Result<Vec<OiSnapshot>> {
        Ok(vec![])
    }
    async fn latest(&self, _: &InstrumentId) -> Result<Option<OiSnapshot>> {
        Ok(None)
    }
}
