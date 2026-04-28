//! Instrument identity and static metadata.
//!
//! An `InstrumentId` is a stable, storage-safe key: `(exchange, symbol)`.
//! Symbols are stored in the exchange's native format so reconciliation with
//! exchange responses is direct — normalizing to e.g. "BTC-USDT" everywhere
//! loses information (USDT-M vs. COIN-M, perp vs. dated).

use crate::exchange::Exchange;
use crate::unit::UnitKind;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Stable identity of a tradable instrument, unique within the system.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstrumentId {
    pub exchange: Exchange,
    /// Symbol as returned by the exchange (`BTCUSDT`, `BTC-USDT-SWAP`, …).
    pub symbol: String,
}

impl InstrumentId {
    pub fn new(exchange: Exchange, symbol: impl Into<String>) -> Self {
        Self {
            exchange,
            symbol: symbol.into(),
        }
    }

    /// Canonical ClickHouse / Redis key form: `"binance:BTCUSDT"`.
    /// Stable — do not change without a migration.
    #[must_use]
    pub fn key(&self) -> String {
        format!("{}:{}", self.exchange.code(), self.symbol)
    }
}

impl std::fmt::Display for InstrumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.key())
    }
}

/// Static per-instrument metadata. Populated by the adapter's
/// `discover_instruments()` call at startup and refreshed on a long cadence
/// (daily is fine) to catch new listings and contract-multiplier changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentMeta {
    pub id: InstrumentId,
    /// Base asset (`BTC`).
    pub base_asset: String,
    /// Quote asset (`USDT`, `USD`, `USDC`, …).
    pub quote_asset: String,
    /// Is this a perpetual (vs. dated) future?
    pub is_perpetual: bool,
    /// In what unit does the exchange publish OI for this instrument?
    pub native_unit: UnitKind,
    /// For `UnitKind::Contracts` — how many `base_asset` per 1 contract.
    /// `None` for `Coins` / `Usd`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_multiplier: Option<Decimal>,
    /// Price step / tick — stored for downstream consumers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_tick: Option<Decimal>,
    /// Quantity step. Helps the API surface human-round numbers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty_step: Option<Decimal>,
    /// True if the instrument is currently tradable. Delisted instruments
    /// stop receiving new OI samples but their history is retained.
    pub active: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn id_key_is_exchange_colon_symbol() {
        let id = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        assert_eq!(id.key(), "binance:BTCUSDT");
        assert_eq!(id.to_string(), "binance:BTCUSDT");
    }

    #[test]
    fn meta_serde_roundtrip() {
        let meta = InstrumentMeta {
            id: InstrumentId::new(Exchange::Okx, "BTC-USDT-SWAP"),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            is_perpetual: true,
            native_unit: UnitKind::Contracts,
            contract_multiplier: Some(dec!(0.01)),
            price_tick: Some(dec!(0.1)),
            qty_step: Some(dec!(0.01)),
            active: true,
        };
        let s = serde_json::to_string(&meta).unwrap();
        let back: InstrumentMeta = serde_json::from_str(&s).unwrap();
        assert_eq!(meta, back);
    }
}
