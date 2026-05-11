//! gRPC + REST API for OI consumers (terminals, dashboards).

pub mod auth;
pub mod config;
pub mod grpc;
pub mod metrics;
pub mod rest;
pub mod server;
pub mod ws;

/// Generated proto types + gRPC client/server. Re-exported so
/// terminals that depend on this crate get the typed stubs without
/// re-running protoc themselves.
pub mod pb {
    tonic::include_proto!("oi.v1");
}

pub fn run() -> anyhow::Result<()> {
    server::bootstrap()
}
