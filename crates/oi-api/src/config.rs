//! API server configuration.

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub clickhouse: ClickHouseCfg,
    pub redis: RedisCfg,
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String,
    #[serde(default = "default_rest_addr")]
    pub rest_addr: String,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
    #[serde(default)]
    pub tls: TlsCfg,
    #[serde(default)]
    pub auth: AuthCfg,
}

/// TLS termination for gRPC and REST. When `enabled = false`, the API
/// serves plaintext (matches single-tenant internal deploys). When
/// enabled, the SAME cert/key pair is used by both endpoints —
/// terminals connect over mTLS-style trust to a single CN.
#[derive(Debug, Clone, Deserialize)]
pub struct TlsCfg {
    #[serde(default)]
    pub enabled: bool,
    /// PEM-encoded server certificate (chain).
    #[serde(default)]
    pub cert_path: String,
    /// PEM-encoded private key.
    #[serde(default)]
    pub key_path: String,
}

impl Default for TlsCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: String::new(),
            key_path: String::new(),
        }
    }
}

/// Bearer-token auth. When `enabled = false`, all paths are open
/// (matches in-VPC deploys). When enabled, requests to data
/// endpoints (`/v1/oi/*`, gRPC) MUST carry
/// `Authorization: Bearer <token>` matching one of `tokens`.
/// Health and metrics paths are always exempt — k8s probes and
/// Prometheus scrapes don't carry auth.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub tokens: Vec<String>,
}

impl Default for AuthCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            tokens: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseCfg {
    pub url: String,
    #[serde(default = "default_db")]
    pub database: String,
    #[serde(default = "default_user")]
    pub user: String,
    #[serde(default)]
    pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisCfg {
    pub url: String,
}

fn default_db() -> String {
    "oi".into()
}
fn default_user() -> String {
    "default".into()
}
fn default_grpc_addr() -> String {
    "0.0.0.0:50051".into()
}
fn default_rest_addr() -> String {
    "0.0.0.0:8080".into()
}
fn default_metrics_addr() -> String {
    "0.0.0.0:9091".into()
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        use figment::providers::{Env, Format, Toml};
        let cfg: Self = figment::Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("OI_API_").split("__"))
            .extract()?;
        Ok(cfg)
    }
}
