//! Bybit v5 linear-perpetual adapter.
//!
//! REST path (primary for the minute tick):
//! * `GET /v5/market/instruments-info?category=linear&cursor=…` — discovery
//!   with cursor-based pagination. Returns `priceFilter.tickSize` and
//!   `lotSizeFilter.qtyStep` plus status (`Trading` | `Closed` | …).
//!   <https://bybit-exchange.github.io/docs/v5/market/instrument>
//! * `GET /v5/market/tickers?category=linear` — **one call** returns every
//!   linear symbol's `openInterest` (coins), `openInterestValue` (USD),
//!   `markPrice`, `lastPrice`. This is our per-minute snapshot source —
//!   we don't need `/v5/market/open-interest` at all for 1m cadence.
//!   (`/v5/market/open-interest` has a minimum interval of 5m anyway.)
//!   <https://bybit-exchange.github.io/docs/v5/market/tickers>
//!
//! WS path (`ws.rs`, not yet wired to the collector):
//! * `wss://stream.bybit.com/v5/public/linear`
//! * Subscribe: `{"op":"subscribe","args":["tickers.BTCUSDT", ...]}`
//!   Max 10 args per message; max 500 topics per connection.
//! * Heartbeat: client sends `{"op":"ping"}` every 20s; server responds
//!   with `{"op":"pong"}`. Server disconnects after 5 minutes silent.
//! * Data: first frame per topic is `type: "snapshot"` (full fields);
//!   subsequent frames are `type: "delta"` (only changed fields) — we
//!   merge into a per-symbol state.
//!
//! All Bybit responses use the envelope `{retCode, retMsg, result}`.
//! `retCode != 0` is a logical error (bad param, rate limit, internal);
//! we map to `ExchangeError::Schema` except for the documented rate-limit
//! codes (`10006`, `10018`) which become `RateLimited`.
//!
//! Unit: `openInterest` is in **coins** (base asset). `openInterestValue`
//! is USD. We store Coins native and let `oi-core` derive USD from
//! (coins × mark price) — giving us a consistent USD across all adapters
//! rather than a Bybit-specific pre-computed figure.
//!
//! Rate limits: public endpoints 600 req/5s per IP (≈ 120 rps). One
//! tickers call per minute + a few discovery pages per 6h is ~1 req/min.
//! Use 20 rps burst 40.

mod rest;
mod stream;
mod ws;

pub use rest::BybitAdapter;
pub use stream::{classify_frame, enrich_bybit, FrameType, SymbolState};
pub use ws::BybitTickersWs;
