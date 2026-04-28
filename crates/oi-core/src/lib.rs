//! Core domain for the OI collection platform.
//!
//! This crate is intentionally dependency-light. It contains:
//! * value objects (`Exchange`, `InstrumentId`, `OiSnapshot`, …)
//! * the `ExchangeAdapter` trait that every exchange integration implements
//! * unit-conversion helpers that normalize exchange-native OI into USD
//! * error taxonomy shared across all crates
//!
//! Higher layers (`oi-storage`, `oi-exchanges`, `oi-collector`, `oi-api`)
//! depend only on types defined here, so adding a new exchange requires
//! touching `oi-exchanges` only — no core changes.

pub mod error;
pub mod exchange;
pub mod funding;
pub mod instrument;
pub mod price;
pub mod snapshot;
pub mod traits;
pub mod unit;

pub use error::{CoreError, ExchangeError};
pub use exchange::Exchange;
pub use funding::{FundingBar, FundingEvent};
pub use instrument::{InstrumentId, InstrumentMeta};
pub use price::{PriceQuote, PriceSource};
pub use snapshot::{OiSample, OiSnapshot, RawOi};
pub use traits::{ExchangeAdapter, OiCollector, OiRepository, PriceProvider};
pub use unit::UnitKind;
