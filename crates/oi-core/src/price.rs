//! Price quotes used to convert OI into USD at the same minute.
//!
//! We keep the price *alongside* the OI snapshot — consumers can re-derive
//! USD from (native_oi, price) at any later time and compare against the
//! pre-computed `oi_usd`. This makes schema drift in conversion logic
//! auditable instead of destructive.

use crate::instrument::InstrumentId;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Where did this price come from?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceSource {
    /// Exchange-published mark price (preferred for perps).
    Mark,
    /// Last trade price.
    Last,
    /// Index / oracle price (Hyperliquid, Deribit-style).
    Index,
    /// Best bid + ask midpoint.
    Mid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceQuote {
    pub instrument: InstrumentId,
    pub price: Decimal,
    pub source: PriceSource,
    #[serde(with = "time::serde::rfc3339")]
    pub ts: OffsetDateTime,
}
