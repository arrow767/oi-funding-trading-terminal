//! Hyperliquid adapter: REST snapshot + live `activeAssetCtx` WS.
//!
//! See `rest.rs` for the one-POST discovery+OI flow. `ws.rs` implements
//! the `activeAssetCtx` subscription; `stream.rs` parses the push.

pub mod rest;
pub mod stream;
pub mod ws;

pub use rest::HyperliquidAdapter;
pub use stream::{enrich_hyperliquid, extract_oi_update, to_raw as live_to_raw};
pub use ws::HyperliquidActiveAssetCtxWs;
