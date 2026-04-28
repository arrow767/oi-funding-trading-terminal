//! Exchange enumeration.
//!
//! Keep this as a closed enum instead of a string: the collector sizes fan-out
//! per exchange, ClickHouse materialized views partition by exchange id, and we
//! want the compiler to force a match update when a new venue is added.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    Binance,
    Bybit,
    Okx,
    BingX,
    KuCoin,
    Mexc,
    Bitget,
    Hyperliquid,
    Aster,
}

impl Exchange {
    /// Stable short code. Used as a column value in ClickHouse and as a
    /// Redis key prefix — DO NOT rename without a migration.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Okx => "okx",
            Self::BingX => "bingx",
            Self::KuCoin => "kucoin",
            Self::Mexc => "mexc",
            Self::Bitget => "bitget",
            Self::Hyperliquid => "hyperliquid",
            Self::Aster => "aster",
        }
    }

    /// All exchanges known to the system. The collector iterates this to
    /// register adapters on startup.
    #[must_use]
    pub const fn all() -> &'static [Exchange] {
        &[
            Self::Binance,
            Self::Bybit,
            Self::Okx,
            Self::BingX,
            Self::KuCoin,
            Self::Mexc,
            Self::Bitget,
            Self::Hyperliquid,
            Self::Aster,
        ]
    }
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

impl FromStr for Exchange {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "binance" => Ok(Self::Binance),
            "bybit" => Ok(Self::Bybit),
            "okx" => Ok(Self::Okx),
            "bingx" => Ok(Self::BingX),
            "kucoin" => Ok(Self::KuCoin),
            "mexc" => Ok(Self::Mexc),
            "bitget" => Ok(Self::Bitget),
            "hyperliquid" => Ok(Self::Hyperliquid),
            "aster" => Ok(Self::Aster),
            other => Err(format!("unknown exchange: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_roundtrip_for_every_exchange() {
        for ex in Exchange::all() {
            assert_eq!(Exchange::from_str(ex.code()).unwrap(), *ex);
        }
    }

    #[test]
    fn from_str_is_case_insensitive() {
        assert_eq!(Exchange::from_str("BINANCE").unwrap(), Exchange::Binance);
        assert_eq!(Exchange::from_str("BinGx").unwrap(), Exchange::BingX);
    }

    #[test]
    fn unknown_exchange_errors() {
        assert!(Exchange::from_str("deribit").is_err());
    }
}
