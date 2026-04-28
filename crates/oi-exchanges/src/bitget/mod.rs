//! Bitget v2 USDT-M futures adapter: REST (minute snapshot) + live WS.
//!
//! See `rest.rs` for endpoint docs and the REST adapter.
//! `ws.rs` implements the v2 public `ticker` channel with Bitget's
//! plain-text `ping`/`pong` heartbeat.
//! `stream.rs` is the pure-function message parser used by the
//! collector's live task.

pub mod rest;
pub mod stream;
pub mod ws;

pub use rest::BitgetAdapter;
pub use stream::{enrich_bitget, extract_oi_update, to_raw as live_to_raw};
pub use ws::BitgetTickerWs;
