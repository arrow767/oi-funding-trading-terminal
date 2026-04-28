//! API server bootstrap. Spawns the gRPC server and the Axum REST server
//! on separate tasks; shutdown is cooperative on Ctrl+C.
//!
//! Both endpoints can independently run with TLS termination
//! (`[tls]`) and bearer-token auth (`[auth]`). Health and metrics
//! paths bypass auth so probes / Prometheus scrapes don't need
//! credentials.

use crate::{
    auth::{grpc_auth_interceptor, rest_auth_middleware, AuthState},
    config::Config,
    grpc::OiGrpc,
    rest,
};
use oi_core::traits::OiRepository;
use oi_storage::{clickhouse::ClickHouseRepo, redis::RedisCache, CompositeRepository};
use std::{path::PathBuf, sync::Arc};
use tracing::{info, warn};

pub fn bootstrap() -> anyhow::Result<()> {
    init_tracing();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("oi-api")
        .build()?;
    rt.block_on(run_async())
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}

async fn run_async() -> anyhow::Result<()> {
    // rustls 0.23 requires the crypto provider to be installed
    // explicitly when more than one provider feature could match.
    // First-installer-wins; subsequent calls return Err which we
    // ignore. Safe whether or not a downstream library already did
    // it.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let cfg_path = std::env::var("OI_API_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("deploy/api.toml"));
    let cfg = Config::load(&cfg_path)?;
    info!(path=?cfg_path, tls=cfg.tls.enabled, auth=cfg.auth.enabled, "config loaded");

    if let Ok(addr) = cfg.metrics_addr.parse::<std::net::SocketAddr>() {
        if let Err(e) = crate::metrics::install(addr) {
            warn!(error=%e, "metrics endpoint failed to bind");
        }
    }

    let ch = ClickHouseRepo::new(
        &cfg.clickhouse.url,
        &cfg.clickhouse.database,
        &cfg.clickhouse.user,
        &cfg.clickhouse.password,
    );
    let redis = RedisCache::connect(&cfg.redis.url).await?;
    let repo: Arc<dyn OiRepository> =
        Arc::new(CompositeRepository::new(ch.clone(), redis.clone()));

    let auth_state = AuthState::new(cfg.auth.tokens.clone());
    let grpc_addr: std::net::SocketAddr = cfg.grpc_addr.parse()?;
    let rest_addr: std::net::SocketAddr = cfg.rest_addr.parse()?;

    // ---- gRPC --------------------------------------------------------
    let grpc_svc = OiGrpc::new(repo.clone(), cfg.redis.url.clone()).into_service();
    let cfg_for_grpc = cfg.clone();
    let auth_for_grpc = auth_state.clone();
    let grpc_task = tokio::spawn(async move {
        let mut builder = tonic::transport::Server::builder();
        if cfg_for_grpc.tls.enabled {
            let cert = tokio::fs::read(&cfg_for_grpc.tls.cert_path).await?;
            let key = tokio::fs::read(&cfg_for_grpc.tls.key_path).await?;
            let identity = tonic::transport::Identity::from_pem(cert, key);
            builder = builder.tls_config(
                tonic::transport::ServerTlsConfig::new().identity(identity),
            )?;
            info!(%grpc_addr, "starting gRPC over TLS");
        } else {
            info!(%grpc_addr, "starting gRPC plaintext");
        }

        // Auth interceptor wraps the service when enabled.
        if cfg_for_grpc.auth.enabled {
            let interceptor = grpc_auth_interceptor(auth_for_grpc);
            let svc = tonic::service::interceptor::InterceptedService::new(
                grpc_svc,
                interceptor,
            );
            anyhow::Ok(builder.add_service(svc).serve(grpc_addr).await?)
        } else {
            anyhow::Ok(builder.add_service(grpc_svc).serve(grpc_addr).await?)
        }
    });

    // ---- REST --------------------------------------------------------
    let rest_state = rest::RestState {
        repo,
        clickhouse: Some(ch),
        redis: Some(redis),
    };
    let cfg_for_rest = cfg.clone();
    let auth_for_rest = auth_state.clone();
    let rest_task = tokio::spawn(async move {
        let mut router = rest::router(rest_state);
        if cfg_for_rest.auth.enabled {
            router = router.layer(axum::middleware::from_fn_with_state(
                auth_for_rest,
                rest_auth_middleware,
            ));
            info!("REST bearer auth enabled");
        }
        if cfg_for_rest.tls.enabled {
            let rustls_cfg = axum_server::tls_rustls::RustlsConfig::from_pem_file(
                &cfg_for_rest.tls.cert_path,
                &cfg_for_rest.tls.key_path,
            )
            .await?;
            info!(%rest_addr, "starting REST over TLS");
            axum_server::bind_rustls(rest_addr, rustls_cfg)
                .serve(router.into_make_service())
                .await?;
        } else {
            info!(%rest_addr, "starting REST plaintext");
            let listener = tokio::net::TcpListener::bind(rest_addr).await?;
            axum::serve(listener, router).await?;
        }
        anyhow::Ok(())
    });

    tokio::select! {
        r = grpc_task => { r??; }
        r = rest_task => { r??; }
        _ = tokio::signal::ctrl_c() => { info!("ctrl+c; shutting down"); }
    }
    Ok(())
}
