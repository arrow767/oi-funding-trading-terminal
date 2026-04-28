//! Bearer-token authentication.
//!
//! Both the REST middleware and the gRPC interceptor compare the
//! `Authorization: Bearer <token>` header / metadata against a fixed
//! list of accepted tokens. Constant-time compare avoids leaking the
//! token via timing.
//!
//! Public paths (`/health/*`, `/metrics`) bypass the REST middleware —
//! Kubernetes probes and Prometheus scrapes don't carry auth.

use axum::{
    body::Body,
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;
use tonic::{metadata::MetadataValue, Status};

/// Set of accepted tokens. Cheap to clone — `Arc` ensures the list is
/// shared and thread-safe.
#[derive(Clone, Debug)]
pub struct AuthState {
    pub tokens: Arc<Vec<String>>,
}

impl AuthState {
    pub fn new(tokens: Vec<String>) -> Self {
        Self {
            tokens: Arc::new(tokens),
        }
    }

    /// Constant-time comparison of `presented` against the accepted
    /// list. Returns true on first match.
    fn accepts(&self, presented: &str) -> bool {
        // We compare each accepted token in CT to leak no info about
        // which token was tried. Length differences DO leak via the
        // bytes-equal short-circuit but that's acceptable: token
        // length is not the secret part.
        self.tokens
            .iter()
            .any(|valid| ct_eq(valid.as_bytes(), presented.as_bytes()))
    }
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Extract `Bearer <token>` from a header value-string. Returns
/// `None` if absent or malformed.
fn extract_bearer(header_value: Option<&str>) -> Option<&str> {
    header_value.and_then(|v| v.strip_prefix("Bearer "))
}

/// REST middleware. Public paths (matched by prefix) are forwarded
/// without auth. Everything else requires a valid bearer.
pub async fn rest_auth_middleware(
    axum::extract::State(state): axum::extract::State<AuthState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if is_public_path(path) {
        return next.run(request).await;
    }
    let header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let Some(token) = extract_bearer(header) else {
        return unauthenticated_response();
    };
    if state.accepts(token) {
        next.run(request).await
    } else {
        unauthenticated_response()
    }
}

fn is_public_path(path: &str) -> bool {
    // Exact-prefix match. `/healthier` would NOT match `/health` —
    // that's correct (no accidental bypass via similar names).
    path == "/health"
        || path.starts_with("/health/")
        || path == "/metrics"
}

fn unauthenticated_response() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(axum::http::header::WWW_AUTHENTICATE, "Bearer")],
        "missing or invalid bearer token",
    )
        .into_response()
}

/// gRPC interceptor. Returns `Status::unauthenticated` on failure.
/// Cloned per-request by tonic; `tokens` is `Arc`'d so this is cheap.
pub fn grpc_auth_interceptor(
    state: AuthState,
) -> impl tonic::service::Interceptor + Clone {
    move |req: tonic::Request<()>| -> Result<tonic::Request<()>, Status> {
        let header: Option<&MetadataValue<_>> = req.metadata().get("authorization");
        let token_str = header.and_then(|v| v.to_str().ok());
        let Some(token) = extract_bearer(token_str) else {
            return Err(Status::unauthenticated("missing bearer token"));
        };
        if state.accepts(token) {
            Ok(req)
        } else {
            Err(Status::unauthenticated("invalid bearer token"))
        }
    }
}

use axum::response::IntoResponse;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_bearer_recognizes_only_proper_prefix() {
        assert_eq!(extract_bearer(Some("Bearer s3cr3t")), Some("s3cr3t"));
        assert_eq!(extract_bearer(Some("bearer s3cr3t")), None); // case-sensitive
        assert_eq!(extract_bearer(Some("Token s3cr3t")), None);
        assert_eq!(extract_bearer(None), None);
    }

    #[test]
    fn ct_eq_matches_eq_for_equal_and_unequal_inputs() {
        assert!(ct_eq(b"abcd", b"abcd"));
        assert!(!ct_eq(b"abcd", b"abce"));
        assert!(!ct_eq(b"abc", b"abcd"));
        assert!(ct_eq(b"", b""));
    }

    #[test]
    fn auth_state_accepts_listed_tokens_only() {
        let s = AuthState::new(vec!["alpha".into(), "bravo".into()]);
        assert!(s.accepts("alpha"));
        assert!(s.accepts("bravo"));
        assert!(!s.accepts("charlie"));
        assert!(!s.accepts(""));
    }

    #[test]
    fn public_path_check_is_strict_prefix() {
        assert!(is_public_path("/health"));
        assert!(is_public_path("/health/live"));
        assert!(is_public_path("/health/ready"));
        assert!(is_public_path("/metrics"));
        // Not public:
        assert!(!is_public_path("/healthier"));
        assert!(!is_public_path("/v1/oi/latest/binance/BTCUSDT"));
        assert!(!is_public_path("/"));
    }
}
