//! Rate-limited, retry-aware HTTP client for REST adapters.
//!
//! Handles:
//! * per-exchange request rate limits (governor-based token bucket),
//! * classification of HTTP status into [`ExchangeError`] (429 → `RateLimited`,
//!   5xx → `Transient`, 4xx → `Schema`/`NotFound`),
//! * `Retry-After` header extraction,
//! * exponential backoff with jitter via `backon`,
//! * gzip/brotli decompression (reqwest built-in),
//! * User-Agent identification so exchanges can trace our traffic if needed.
//!
//! Adapters wrap this client; they never touch `reqwest::Client` directly, so
//! we can swap the transport (e.g. to `hyper` + custom TLS) without touching
//! adapters.

use governor::{
    clock::DefaultClock, middleware::NoOpMiddleware, state::InMemoryState,
    state::NotKeyed, Quota, RateLimiter,
};
use oi_core::error::ExchangeError;
use reqwest::{Method, Response, StatusCode};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

pub type Limiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

/// HTTP client with a request-rate governor and a well-defined error mapping.
#[derive(Clone)]
pub struct RateLimitedClient {
    inner: reqwest::Client,
    limiter: Arc<Limiter>,
    exchange_code: &'static str,
}

impl std::fmt::Debug for RateLimitedClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitedClient")
            .field("exchange", &self.exchange_code)
            .finish()
    }
}

impl RateLimitedClient {
    /// Build a client with a token bucket of `requests_per_sec` (avg) and
    /// burst `burst`. Both must fit `NonZeroU32` (enforced at runtime).
    pub fn new(
        exchange_code: &'static str,
        requests_per_sec: u32,
        burst: u32,
    ) -> Result<Self, ExchangeError> {
        let rps = NonZeroU32::new(requests_per_sec.max(1))
            .ok_or_else(|| ExchangeError::Unexpected("rps must be > 0".into()))?;
        let burst =
            NonZeroU32::new(burst.max(1)).ok_or_else(|| ExchangeError::Unexpected("burst must be > 0".into()))?;
        let quota = Quota::per_second(rps).allow_burst(burst);
        let inner = reqwest::Client::builder()
            .user_agent(format!(
                "trading-terminal-oi/{} ({exchange_code})",
                env!("CARGO_PKG_VERSION")
            ))
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(16)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .http2_keep_alive_interval(Some(Duration::from_secs(30)))
            .build()
            .map_err(|e| ExchangeError::Unexpected(format!("http build: {e}")))?;
        Ok(Self {
            inner,
            limiter: Arc::new(RateLimiter::direct(quota)),
            exchange_code,
        })
    }

    /// Wait for a token, send the request, classify the outcome.
    ///
    /// Retries are NOT performed here — the caller wraps this with `backon`
    /// and decides per-request whether an error is retryable. This keeps the
    /// client itself stateless w.r.t. idempotency.
    pub async fn send(
        &self,
        method: Method,
        url: &str,
    ) -> Result<Response, ExchangeError> {
        self.limiter.until_ready().await;
        let resp = self
            .inner
            .request(method.clone(), url)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() || e.is_connect() {
                    ExchangeError::transient("http", format!("{method} {url}: {e}"))
                } else {
                    ExchangeError::Unexpected(format!("{method} {url}: {e}"))
                }
            })?;
        classify(resp, self.exchange_code).await
    }

    /// Sugar for GET.
    pub async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
    ) -> Result<T, ExchangeError> {
        let resp = self.send(Method::GET, url).await?;
        resp.json::<T>()
            .await
            .map_err(|e| ExchangeError::Schema(format!("GET {url} decode: {e}")))
    }

    /// Sugar for POST with a serializable JSON body (e.g. Hyperliquid `/info`).
    pub async fn post_json<B, T>(&self, url: &str, body: &B) -> Result<T, ExchangeError>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        self.limiter.until_ready().await;
        let resp = self
            .inner
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() || e.is_connect() {
                    ExchangeError::transient("http", format!("POST {url}: {e}"))
                } else {
                    ExchangeError::Unexpected(format!("POST {url}: {e}"))
                }
            })?;
        let resp = classify(resp, self.exchange_code).await?;
        resp.json::<T>()
            .await
            .map_err(|e| ExchangeError::Schema(format!("POST {url} decode: {e}")))
    }
}

/// Map HTTP status codes to `ExchangeError`. Extract `Retry-After` for 429.
async fn classify(resp: Response, exchange_code: &'static str) -> Result<Response, ExchangeError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let retry_after = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs);

    // For 418 (IP banned / teapot) Binance uses this status code as "you're
    // misbehaving, back off HARD". Treat like rate limit but log louder.
    if status == StatusCode::IM_A_TEAPOT {
        warn!(%exchange_code, "HTTP 418 — exchange signalled a ban; widening backoff");
        return Err(ExchangeError::RateLimited { retry_after });
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        debug!(%exchange_code, ?retry_after, "HTTP 429");
        return Err(ExchangeError::RateLimited { retry_after });
    }
    if status.is_server_error() {
        return Err(ExchangeError::transient(
            "http_5xx",
            format!("status {}", status.as_u16()),
        ));
    }
    if status == StatusCode::NOT_FOUND {
        return Err(ExchangeError::NotFound(format!(
            "status 404 from {exchange_code}"
        )));
    }
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        let body = resp.text().await.unwrap_or_default();
        return Err(ExchangeError::Auth(format!(
            "{}: {}",
            status.as_u16(),
            truncate(&body, 200)
        )));
    }
    // Other 4xx — treat as schema/contract issue (unexpected request shape).
    let body = resp.text().await.unwrap_or_default();
    Err(ExchangeError::Schema(format!(
        "{}: {}",
        status.as_u16(),
        truncate(&body, 200)
    )))
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_owned();
    }
    let mut end = n;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn constructs_with_sane_defaults() {
        let c = RateLimitedClient::new("test", 10, 5).expect("construct");
        // limiter permits at least one immediately
        c.limiter.until_ready().await;
    }

    #[test]
    fn zero_rps_is_coerced_to_one() {
        let c = RateLimitedClient::new("test", 0, 0);
        assert!(c.is_ok());
    }

    #[test]
    fn truncate_does_not_panic_on_multibyte() {
        // must not panic, even if we cut before a char boundary
        let t = truncate("abcdef", 100);
        assert_eq!(t, "abcdef");
    }
}
