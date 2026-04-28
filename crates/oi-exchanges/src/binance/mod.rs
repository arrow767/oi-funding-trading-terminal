//! Binance USD-M Futures adapter (reference implementation).
//!
//! Endpoints used:
//! * `GET /fapi/v1/exchangeInfo` — instrument discovery (perps only).
//!   <https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information>
//! * `GET /fapi/v1/openInterest?symbol=…` — current OI snapshot per symbol.
//!   <https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest>
//! * `GET /fapi/v1/premiumIndex` (single call, all symbols) — mark price for USD
//!   conversion. Cheap: one request covers the full universe.
//!   <https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Mark-Price>
//!
//! Rate limits (as of documented quota, USD-M futures):
//! * 2400 request-weight per minute per IP.
//! * `openInterest` weight = 1. `exchangeInfo` weight = 1. `premiumIndex`
//!   (no symbol) weight = 10.
//! So polling all ~250 perps every minute = ~250 weight/min. Well under cap.
//! We still rate-limit at 20 req/s (avg) with burst 40 to avoid spikes.
//!
//! Unit semantics: USD-M `openInterest.openInterest` is reported in **coins**
//! (base asset). Verified against responses, e.g. `BTCUSDT → 12345.678`.
//!
//! WebSocket: Binance publishes no dedicated OI stream for USD-M futures —
//! OI is REST-only. The WS layer is still wired up (markPrice stream) for
//! lower-latency price updates — see `ws.rs`.

mod rest;
mod ws;

pub use rest::BinanceUsdmAdapter;
