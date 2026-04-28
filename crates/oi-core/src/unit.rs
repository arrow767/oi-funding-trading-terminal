//! Unit semantics for Open Interest values.
//!
//! Different exchanges publish OI in different units. We MUST preserve the
//! native value and the unit so downstream consumers can reconstruct either
//! view (coins vs. dollars) without re-fetching.
//!
//! Real-world mapping (verified against public docs as of the reference build;
//! see `docs/exchange-notes.md` for the citations and any subsequent changes):
//!
//! | Exchange     | Endpoint                                 | Native unit      |
//! |--------------|------------------------------------------|------------------|
//! | Binance USDM | GET /fapi/v1/openInterest                | `Coins`          |
//! | Binance COIN | GET /dapi/v1/openInterest                | `Contracts`*     |
//! | Bybit v5     | GET /v5/market/open-interest             | `Coins`          |
//! | OKX          | GET /api/v5/public/open-interest         | `Contracts`*     |
//! | BingX        | GET /openApi/swap/v2/quote/openInterest  | `Coins`          |
//! | KuCoin       | GET /api/v1/contracts/{symbol}           | `Contracts`*     |
//! | MEXC         | GET /api/v1/contract/ticker              | `Contracts`*     |
//! | Bitget       | GET /api/v2/mix/market/open-interest     | `Coins`          |
//! | Hyperliquid  | POST /info (type=metaAndAssetCtxs)       | `Coins`          |
//! | Aster        | GET /fapi/v1/openInterest (binance-like) | `Coins`          |
//!
//! *For `Contracts` the adapter must supply a `contract_multiplier` via
//! instrument metadata (e.g. OKX BTC-USDT-SWAP: 1 contract = 0.01 BTC).
//!
//! USD is NEVER stored as the native unit — it is always a derived column
//! computed from the native value times a price snapshot at the same minute.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// The unit in which an exchange reports OI on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitKind {
    /// Native asset quantity (e.g. "12,345.6 BTC").
    Coins,
    /// Quote-currency value published directly by the exchange.
    /// Rare but exists — we treat it as pre-multiplied USD.
    Usd,
    /// Exchange-native contracts. Conversion to coins requires a
    /// `contract_multiplier` in `InstrumentMeta`.
    Contracts,
}

impl UnitKind {
    /// Convert a native value into base-asset (coins) using `multiplier` when
    /// the native unit is `Contracts`. Returns `None` if multiplier missing.
    #[must_use]
    pub fn to_coins(self, native: Decimal, multiplier: Option<Decimal>) -> Option<Decimal> {
        match self {
            Self::Coins => Some(native),
            Self::Contracts => multiplier.map(|m| native * m),
            // Can't derive coins from a USD amount without a price,
            // but collectors always carry price too — see `to_usd` below.
            Self::Usd => None,
        }
    }

    /// Convert a native value into USD. `multiplier` converts `Contracts` to
    /// coins; `price` is the USD price of one coin. Returns `None` when the
    /// inputs are insufficient.
    #[must_use]
    pub fn to_usd(
        self,
        native: Decimal,
        multiplier: Option<Decimal>,
        price: Option<Decimal>,
    ) -> Option<Decimal> {
        match self {
            Self::Usd => Some(native),
            Self::Coins => price.map(|p| native * p),
            Self::Contracts => match (multiplier, price) {
                (Some(m), Some(p)) => Some(native * m * p),
                _ => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn coins_to_usd_uses_price() {
        let usd = UnitKind::Coins.to_usd(dec!(10), None, Some(dec!(2000)));
        assert_eq!(usd, Some(dec!(20000)));
    }

    #[test]
    fn contracts_to_coins_requires_multiplier() {
        let coins = UnitKind::Contracts.to_coins(dec!(100), Some(dec!(0.01)));
        assert_eq!(coins, Some(dec!(1)));
        assert!(UnitKind::Contracts.to_coins(dec!(100), None).is_none());
    }

    #[test]
    fn contracts_to_usd_multiplies_both() {
        let usd = UnitKind::Contracts.to_usd(dec!(100), Some(dec!(0.01)), Some(dec!(50000)));
        assert_eq!(usd, Some(dec!(50000)));
    }

    #[test]
    fn usd_native_returns_itself() {
        assert_eq!(
            UnitKind::Usd.to_usd(dec!(123456), None, None),
            Some(dec!(123456))
        );
    }

    #[test]
    fn usd_cannot_be_converted_back_to_coins_without_price() {
        assert!(UnitKind::Usd.to_coins(dec!(1000), None).is_none());
    }
}
