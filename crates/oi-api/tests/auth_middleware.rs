//! Verifies the REST auth middleware against a live router. Drives
//! the same `Router::layer(from_fn_with_state)` pattern the bootstrap
//! uses, so what passes here matches production behaviour.

use axum::Router;
use oi_api::{
    auth::{rest_auth_middleware, AuthState},
    rest::{router, RestState},
};
use std::sync::Arc;

#[tokio::test]
async fn health_endpoints_bypass_auth_data_endpoints_require_it() {
    // Build a router with auth wired the same way `server.rs` does.
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

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    // Tiny yield so the server task has bound.
    tokio::task::yield_now().await;

    let client = reqwest::Client::new();

    // /health/live must work WITHOUT auth.
    let r = client
        .get(format!("http://{addr}/health/live"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200, "health/live should bypass auth");

    // /v1/oi/latest WITHOUT auth → 401.
    let r = client
        .get(format!("http://{addr}/v1/oi/latest/binance/BTCUSDT"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "no bearer → unauthorized");
    assert!(r.headers().contains_key("www-authenticate"));

    // /v1/oi/latest WITH wrong bearer → 401.
    let r = client
        .get(format!("http://{addr}/v1/oi/latest/binance/BTCUSDT"))
        .bearer_auth("wrong-token")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "wrong bearer → unauthorized");

    // /v1/oi/latest WITH valid bearer → middleware passes; handler
    // then 404s because the NoopRepo has no data. The point is the
    // 401 went away; reaching the handler proves the middleware
    // approved.
    let r = client
        .get(format!("http://{addr}/v1/oi/latest/binance/BTCUSDT"))
        .bearer_auth("s3cr3t")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "valid bearer should reach handler");
}

// --- minimal in-memory repo for the test ---------------------------

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
