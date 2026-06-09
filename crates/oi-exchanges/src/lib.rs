//! Exchange adapters. One module per venue; each implements `ExchangeAdapter`.
//!
//! Common infrastructure lives in submodules:
//! * [`common::http`] — a rate-limited HTTP client with retry/backoff.
//! * [`common::ws`]   — a supervised WebSocket wrapper (reconnect, ping/pong,
//!   exponential backoff).
//! * [`common::bucket`] — minute-bucket alignment helpers.
//!
//! Adding a new exchange — see `docs/adding-an-exchange.md`.
pub mod common;

pub mod binance;
pub mod bybit;
pub mod okx;
pub mod bingx;
pub mod kucoin;
pub mod mexc;
pub mod bitget;
pub mod hyperliquid;
pub mod aster;
pub mod gate;

/// Which adapters are production-ready. The collector consults this to
/// decide whether to instantiate an adapter or log "skipped stub". Keeping
/// it here means adding `Exchange::X => true` next to the corresponding
/// `impl ExchangeAdapter` is a single-file change.
#[must_use]
pub fn is_production_ready(ex: oi_core::Exchange) -> bool {
    // Exhaustive match rather than a tuple of `matches!` arms — adding
    // a new `Exchange` variant will fail the build until this is
    // updated, which is the point.
    use oi_core::Exchange as E;
    match ex {
        E::Binance | E::Bybit | E::Hyperliquid | E::Okx | E::Bitget | E::Mexc
        | E::Aster | E::BingX | E::KuCoin | E::Gate => true,
    }
}
