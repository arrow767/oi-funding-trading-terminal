//! OKX v5 adapter: REST (minute snapshot) + live WS push.
//!
//! See `rest.rs` for the REST adapter and endpoint docs; `ws.rs` for the
//! `open-interest` WebSocket channel with OKX's plain-text `ping`
//! heartbeat; `stream.rs` for the pure-function message parser used by
//! the collector's live task.

pub mod rest;
pub mod stream;
pub mod ws;

pub use rest::OkxAdapter;
pub use stream::{enrich_okx, extract_oi_update, to_raw as live_to_raw};
pub use ws::OkxOpenInterestWs;
