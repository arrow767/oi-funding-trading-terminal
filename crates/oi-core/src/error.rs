//! Unified error types. Every crate above `oi-core` converts into one of these
//! at its public boundary so callers don't have to pattern-match on
//! exchange-specific errors.

use thiserror::Error;

/// Errors raised by exchange adapters.
///
/// Transient vs. permanent is explicit: the collector retries only on
/// [`ExchangeError::Transient`]. Permanent errors propagate up and surface in
/// alerts — they indicate a bug or a contract change on the exchange side.
#[derive(Debug, Error)]
pub enum ExchangeError {
    /// Retryable: network blip, 5xx, 429, ws disconnect.
    #[error("transient error ({source_kind}): {message}")]
    Transient {
        source_kind: &'static str,
        message: String,
    },

    /// Rate limited. `retry_after` is populated from the exchange response
    /// when available; `None` means "use exponential backoff".
    #[error("rate limited; retry_after={retry_after:?}")]
    RateLimited {
        retry_after: Option<std::time::Duration>,
    },

    /// Authentication/signature failure. Never retry.
    #[error("auth error: {0}")]
    Auth(String),

    /// Exchange returned malformed data or a schema we don't recognize.
    /// Propagates to the "contract drift" alert.
    #[error("schema drift: {0}")]
    Schema(String),

    /// Instrument was delisted / symbol not found. Collector will remove it
    /// from the rotation.
    #[error("instrument not found: {0}")]
    NotFound(String),

    /// Anything else we haven't classified yet.
    #[error("unexpected: {0}")]
    Unexpected(String),
}

impl ExchangeError {
    pub fn transient(kind: &'static str, msg: impl Into<String>) -> Self {
        Self::Transient {
            source_kind: kind,
            message: msg.into(),
        }
    }

    /// Is this error worth retrying?
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient { .. } | Self::RateLimited { .. })
    }
}

/// Top-level error type used across the crates. Each layer narrows to its
/// own `Result` alias; this is the lingua franca.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error(transparent)]
    Exchange(#[from] ExchangeError),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("other: {0}")]
    Other(String),
}

pub type Result<T, E = CoreError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_and_ratelimited_are_retryable() {
        assert!(ExchangeError::transient("http", "502 bad gateway").is_retryable());
        assert!(
            ExchangeError::RateLimited {
                retry_after: Some(std::time::Duration::from_secs(1))
            }
            .is_retryable()
        );
        assert!(!ExchangeError::Auth("bad sig".into()).is_retryable());
        assert!(!ExchangeError::Schema("unknown field".into()).is_retryable());
    }
}
