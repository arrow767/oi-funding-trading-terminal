//! In-memory price cache + cross-exchange USD-price fallback.
//!
//! Adapters push their `fetch_prices` output here once per minute.
//! Lookups by `InstrumentId` are exact; `price_usd_with_fallback`
//! also looks across exchanges for the same canonical (base, quote)
//! when the native source is missing or stale.
//!
//! Quote-currency awareness: a strict pass first looks for donors
//! whose quote canonicalizes to the same family (USDT / USDC /
//! FDUSD / USD all collapse to `USD-PEG`); a loose pass then accepts
//! any quote for the same base. Provenance carries `quote_match` so
//! audit/log can tell whether the borrowed price came from a peer
//! quote (best) or a different family (acceptable but worth logging).
//!
//! Base canonicalization: KuCoin uses `XBT` for Bitcoin while every
//! other venue uses `BTC`. We normalize before indexing.

use async_trait::async_trait;
use dashmap::DashMap;
use oi_core::{
    exchange::Exchange, instrument::InstrumentId, price::PriceQuote,
    traits::PriceProvider,
};
use rust_decimal::Decimal;
use time::{Duration, OffsetDateTime};
use tracing::trace;

/// Where a returned USD price came from. Logged + metric-labelled so
/// operators can spot exchanges whose own price feed is degraded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Provenance {
    /// Price from the instrument's own exchange.
    Native,
    /// Price borrowed from another exchange. `quote_match` is true
    /// when the donor's quote currency was in the same canonical
    /// family (USD-pegged stables vs. USD itself), false when we
    /// fell through to a different quote family (last-resort).
    Fallback {
        from: Exchange,
        quote_match: bool,
    },
}

/// Per-instrument metadata held in the by-base index.
#[derive(Debug, Clone)]
struct IndexEntry {
    id: InstrumentId,
    quote_canonical: String,
}

#[derive(Debug, Default)]
pub struct InMemoryPriceProvider {
    /// Primary index: exact `(exchange, symbol)` → (price, ts).
    prices: DashMap<InstrumentId, (Decimal, OffsetDateTime)>,
    /// Secondary index: canonical base asset → list of instrument
    /// entries (each with the donor's canonical quote). Walked in
    /// `Exchange::all()` order so Binance is preferred — most-liquid
    /// first.
    by_base: DashMap<String, Vec<IndexEntry>>,
    /// Maximum age of a price before we treat it as unusable.
    tolerance: Duration,
}

impl InMemoryPriceProvider {
    pub fn with_tolerance(tolerance: Duration) -> Self {
        Self {
            prices: DashMap::new(),
            by_base: DashMap::new(),
            tolerance,
        }
    }

    pub fn ingest(&self, quotes: impl IntoIterator<Item = PriceQuote>) {
        for q in quotes {
            self.prices.insert(q.instrument, (q.price, q.ts));
        }
    }

    /// Register an instrument's (base, quote) pair in the
    /// fallback index. Idempotent on instrument id — duplicate
    /// registrations are filtered.
    pub fn register_instrument(
        &self,
        instrument: InstrumentId,
        base_asset: &str,
        quote_asset: &str,
    ) {
        let base = canonical_base(base_asset).to_owned();
        let quote = canonical_quote(quote_asset).to_owned();
        let mut bucket = self.by_base.entry(base).or_default();
        if !bucket.iter().any(|e| e.id == instrument) {
            bucket.push(IndexEntry {
                id: instrument,
                quote_canonical: quote,
            });
        }
    }

    /// Lookup with cross-exchange fallback.
    ///
    /// Resolution order:
    /// 1. Native exact-match — `(exchange, symbol)`.
    /// 2. Same canonical base AND same canonical quote, walking
    ///    `Exchange::all()` order.
    /// 3. Same canonical base, any quote — last resort.
    /// 4. `None`.
    pub async fn price_usd_with_fallback(
        &self,
        instrument: &InstrumentId,
        base_asset: &str,
        quote_asset: &str,
        near: OffsetDateTime,
    ) -> Option<(Decimal, Provenance)> {
        if let Some(p) = self.fresh(instrument, near) {
            return Some((p, Provenance::Native));
        }

        let canonical = canonical_base(base_asset);
        let target_quote = canonical_quote(quote_asset);
        let entries = self.by_base.get(canonical)?.clone();

        // Pass 1 — strict quote match.
        if let Some(found) = self.scan_pass(&entries, instrument, near, Some(target_quote)) {
            return Some(found);
        }
        // Pass 2 — any quote, same base.
        self.scan_pass(&entries, instrument, near, None)
    }

    /// One pass over the by-base bucket. When `require_quote` is
    /// `Some`, only entries whose canonical quote equals that string
    /// are considered. Walks in `Exchange::all()` order so Binance
    /// always beats Bybit beats OKX, etc.
    fn scan_pass(
        &self,
        entries: &[IndexEntry],
        target: &InstrumentId,
        near: OffsetDateTime,
        require_quote: Option<&str>,
    ) -> Option<(Decimal, Provenance)> {
        for ex in Exchange::all() {
            for entry in entries.iter().filter(|e| e.id.exchange == *ex) {
                if entry.id == *target {
                    continue;
                }
                if let Some(q) = require_quote {
                    if entry.quote_canonical != q {
                        continue;
                    }
                }
                if let Some(p) = self.fresh(&entry.id, near) {
                    let quote_match = require_quote.is_some();
                    trace!(
                        target = %target,
                        donor = %entry.id,
                        quote_match,
                        "price fallback"
                    );
                    return Some((
                        p,
                        Provenance::Fallback {
                            from: *ex,
                            quote_match,
                        },
                    ));
                }
            }
        }
        None
    }

    fn fresh(&self, instrument: &InstrumentId, near: OffsetDateTime) -> Option<Decimal> {
        let entry = self.prices.get(instrument)?;
        let (price, ts) = *entry;
        if (near - ts).abs() <= self.tolerance {
            Some(price)
        } else {
            None
        }
    }
}

/// Normalize per-exchange base-asset spellings to a single canonical
/// form. Currently only KuCoin's `XBT` for Bitcoin.
#[must_use]
pub fn canonical_base(asset: &str) -> &str {
    match asset.to_ascii_uppercase().as_str() {
        "XBT" => "BTC",
        _ => asset,
    }
}

/// Collapse USD-pegged stablecoins and USD itself into a single
/// canonical family `"USD-PEG"`. Any non-stable quote is returned
/// uppercased verbatim.
///
/// The list reflects what trades on our 9 exchanges. New
/// stablecoins (e.g. PYUSD, GUSD) can be added here without
/// touching call sites.
#[must_use]
pub fn canonical_quote(quote: &str) -> &str {
    match quote.to_ascii_uppercase().as_str() {
        "USDT" | "USDC" | "BUSD" | "FDUSD" | "USD" | "TUSD" | "DAI" => "USD-PEG",
        // No-allocation passthrough for the unmatched case: we'd
        // ideally `to_ascii_uppercase` but that allocates. Callers
        // should pass already-uppercase strings (instrument metadata
        // always does).
        _ => quote,
    }
}

#[async_trait]
impl PriceProvider for InMemoryPriceProvider {
    async fn price_usd(&self, instrument: &InstrumentId, near: OffsetDateTime) -> Option<Decimal> {
        self.fresh(instrument, near)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oi_core::{exchange::Exchange, price::PriceSource};
    use rust_decimal_macros::dec;
    use time::macros::datetime;

    fn quote(
        ex: Exchange,
        sym: &str,
        price: Decimal,
        ts: OffsetDateTime,
    ) -> PriceQuote {
        PriceQuote {
            instrument: InstrumentId::new(ex, sym.to_owned()),
            price,
            source: PriceSource::Mark,
            ts,
        }
    }

    #[tokio::test]
    async fn recent_price_is_returned() {
        let p = InMemoryPriceProvider::with_tolerance(Duration::minutes(5));
        let id = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        p.ingest([quote(
            Exchange::Binance,
            "BTCUSDT",
            dec!(65000),
            datetime!(2026-04-25 10:00:00 UTC),
        )]);
        let got = p.price_usd(&id, datetime!(2026-04-25 10:01:00 UTC)).await;
        assert_eq!(got, Some(dec!(65000)));
    }

    #[tokio::test]
    async fn stale_price_is_suppressed() {
        let p = InMemoryPriceProvider::with_tolerance(Duration::minutes(5));
        let id = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        p.ingest([quote(
            Exchange::Binance,
            "BTCUSDT",
            dec!(65000),
            datetime!(2026-04-25 10:00:00 UTC),
        )]);
        let got = p.price_usd(&id, datetime!(2026-04-25 11:00:00 UTC)).await;
        assert_eq!(got, None);
    }

    #[tokio::test]
    async fn fallback_finds_same_quote_family_first() {
        let p = InMemoryPriceProvider::with_tolerance(Duration::minutes(5));
        let target = InstrumentId::new(Exchange::Mexc, "BTC_USDT");
        let bybit_usdc = InstrumentId::new(Exchange::Bybit, "BTCUSDC");
        let binance_usdt = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        // All three quote into the USD-PEG family.
        p.register_instrument(target.clone(), "BTC", "USDT");
        p.register_instrument(bybit_usdc.clone(), "BTC", "USDC");
        p.register_instrument(binance_usdt.clone(), "BTC", "USDT");
        // Only Bybit has a fresh quote.
        p.ingest([quote(
            Exchange::Bybit,
            "BTCUSDC",
            dec!(64995),
            datetime!(2026-04-25 10:00:00 UTC),
        )]);
        let got = p
            .price_usd_with_fallback(
                &target,
                "BTC",
                "USDT",
                datetime!(2026-04-25 10:01:00 UTC),
            )
            .await
            .unwrap();
        // USDC and USDT both → USD-PEG, so quote_match is true.
        assert_eq!(
            got,
            (
                dec!(64995),
                Provenance::Fallback {
                    from: Exchange::Bybit,
                    quote_match: true,
                }
            )
        );
    }

    #[tokio::test]
    async fn loose_pass_picks_up_different_quote_family() {
        let p = InMemoryPriceProvider::with_tolerance(Duration::minutes(5));
        let target = InstrumentId::new(Exchange::Mexc, "BTC_USDT");
        // Pretend Binance only quotes BTC against an unusual asset
        // ("EUR" — purely synthetic; we need a non-USD-PEG canonical).
        let binance_eur = InstrumentId::new(Exchange::Binance, "BTCEUR");
        p.register_instrument(target.clone(), "BTC", "USDT");
        p.register_instrument(binance_eur.clone(), "BTC", "EUR");
        p.ingest([quote(
            Exchange::Binance,
            "BTCEUR",
            dec!(60000),
            datetime!(2026-04-25 10:00:00 UTC),
        )]);
        let got = p
            .price_usd_with_fallback(
                &target,
                "BTC",
                "USDT",
                datetime!(2026-04-25 10:01:00 UTC),
            )
            .await
            .unwrap();
        // No same-quote candidate fresh; we fall through to loose
        // pass. quote_match == false signals "this is approximate".
        assert_eq!(
            got,
            (
                dec!(60000),
                Provenance::Fallback {
                    from: Exchange::Binance,
                    quote_match: false,
                }
            )
        );
    }

    #[tokio::test]
    async fn strict_pass_beats_loose_when_both_available() {
        let p = InMemoryPriceProvider::with_tolerance(Duration::minutes(5));
        let target = InstrumentId::new(Exchange::Mexc, "BTC_USDT");
        let binance_eur = InstrumentId::new(Exchange::Binance, "BTCEUR");
        let bybit_usdt = InstrumentId::new(Exchange::Bybit, "BTCUSDT");
        p.register_instrument(target.clone(), "BTC", "USDT");
        p.register_instrument(binance_eur.clone(), "BTC", "EUR");
        p.register_instrument(bybit_usdt.clone(), "BTC", "USDT");
        // Binance EUR is fresh, Bybit USDT is also fresh. Strict
        // (USDT-USDT) must beat loose (Binance EUR) even though
        // Binance is earlier in the priority order.
        p.ingest([
            quote(
                Exchange::Binance,
                "BTCEUR",
                dec!(60000),
                datetime!(2026-04-25 10:00:00 UTC),
            ),
            quote(
                Exchange::Bybit,
                "BTCUSDT",
                dec!(64995),
                datetime!(2026-04-25 10:00:00 UTC),
            ),
        ]);
        let got = p
            .price_usd_with_fallback(
                &target,
                "BTC",
                "USDT",
                datetime!(2026-04-25 10:01:00 UTC),
            )
            .await
            .unwrap();
        assert_eq!(
            got,
            (
                dec!(64995),
                Provenance::Fallback {
                    from: Exchange::Bybit,
                    quote_match: true,
                }
            )
        );
    }

    #[tokio::test]
    async fn native_price_wins_over_fallback() {
        let p = InMemoryPriceProvider::with_tolerance(Duration::minutes(5));
        let mexc = InstrumentId::new(Exchange::Mexc, "BTC_USDT");
        let binance = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        p.register_instrument(mexc.clone(), "BTC", "USDT");
        p.register_instrument(binance.clone(), "BTC", "USDT");
        p.ingest([
            quote(Exchange::Mexc, "BTC_USDT", dec!(64950), datetime!(2026-04-25 10:00:00 UTC)),
            quote(Exchange::Binance, "BTCUSDT", dec!(65000), datetime!(2026-04-25 10:00:00 UTC)),
        ]);
        let got = p
            .price_usd_with_fallback(
                &mexc,
                "BTC",
                "USDT",
                datetime!(2026-04-25 10:01:00 UTC),
            )
            .await
            .unwrap();
        assert_eq!(got, (dec!(64950), Provenance::Native));
    }

    #[tokio::test]
    async fn xbt_canonicalized_to_btc_for_kucoin() {
        let p = InMemoryPriceProvider::with_tolerance(Duration::minutes(5));
        let kucoin = InstrumentId::new(Exchange::KuCoin, "XBTUSDTM");
        let binance = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        p.register_instrument(kucoin.clone(), "XBT", "USDT");
        p.register_instrument(binance.clone(), "BTC", "USDT");
        p.ingest([quote(
            Exchange::Binance,
            "BTCUSDT",
            dec!(65000),
            datetime!(2026-04-25 10:00:00 UTC),
        )]);
        let got = p
            .price_usd_with_fallback(
                &kucoin,
                "XBT",
                "USDT",
                datetime!(2026-04-25 10:01:00 UTC),
            )
            .await
            .unwrap();
        assert_eq!(
            got,
            (
                dec!(65000),
                Provenance::Fallback {
                    from: Exchange::Binance,
                    quote_match: true,
                }
            )
        );
    }

    #[tokio::test]
    async fn fallback_returns_none_when_all_sources_stale() {
        let p = InMemoryPriceProvider::with_tolerance(Duration::minutes(5));
        let mexc = InstrumentId::new(Exchange::Mexc, "BTC_USDT");
        let binance = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        p.register_instrument(mexc.clone(), "BTC", "USDT");
        p.register_instrument(binance.clone(), "BTC", "USDT");
        p.ingest([
            quote(Exchange::Mexc, "BTC_USDT", dec!(64950), datetime!(2026-04-25 09:00:00 UTC)),
            quote(Exchange::Binance, "BTCUSDT", dec!(65000), datetime!(2026-04-25 09:00:00 UTC)),
        ]);
        let got = p
            .price_usd_with_fallback(
                &mexc,
                "BTC",
                "USDT",
                datetime!(2026-04-25 10:00:00 UTC),
            )
            .await;
        assert_eq!(got, None);
    }

    #[tokio::test]
    async fn declaration_order_priority_within_strict_pass() {
        let p = InMemoryPriceProvider::with_tolerance(Duration::minutes(5));
        let target = InstrumentId::new(Exchange::Mexc, "BTC_USDT");
        let bybit = InstrumentId::new(Exchange::Bybit, "BTCUSDT");
        let binance = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        for id in [&target, &bybit, &binance] {
            p.register_instrument(id.clone(), "BTC", "USDT");
        }
        p.ingest([
            quote(Exchange::Bybit, "BTCUSDT", dec!(64900), datetime!(2026-04-25 10:00:00 UTC)),
            quote(Exchange::Binance, "BTCUSDT", dec!(65000), datetime!(2026-04-25 10:00:00 UTC)),
        ]);
        let got = p
            .price_usd_with_fallback(
                &target,
                "BTC",
                "USDT",
                datetime!(2026-04-25 10:01:00 UTC),
            )
            .await
            .unwrap();
        assert_eq!(
            got,
            (
                dec!(65000),
                Provenance::Fallback {
                    from: Exchange::Binance,
                    quote_match: true,
                }
            )
        );
    }

    #[test]
    fn canonical_base_handles_known_aliases() {
        assert_eq!(canonical_base("BTC"), "BTC");
        assert_eq!(canonical_base("XBT"), "BTC");
        assert_eq!(canonical_base("xbt"), "BTC");
        assert_eq!(canonical_base("ETH"), "ETH");
    }

    #[test]
    fn canonical_quote_collapses_usd_pegged_stables() {
        for q in ["USDT", "USDC", "BUSD", "FDUSD", "USD", "TUSD", "DAI"] {
            assert_eq!(canonical_quote(q), "USD-PEG", "{q}");
        }
        assert_eq!(canonical_quote("usdt"), "USD-PEG"); // case-insensitive
        // Non-stables stay distinct so we don't blindly merge them.
        assert_eq!(canonical_quote("EUR"), "EUR");
        assert_eq!(canonical_quote("BTC"), "BTC");
    }

    #[test]
    fn register_instrument_is_idempotent() {
        let p = InMemoryPriceProvider::with_tolerance(Duration::minutes(5));
        let id = InstrumentId::new(Exchange::Binance, "BTCUSDT");
        p.register_instrument(id.clone(), "BTC", "USDT");
        p.register_instrument(id.clone(), "BTC", "USDT");
        p.register_instrument(id.clone(), "BTC", "USDT");
        let entries = p.by_base.get("BTC").unwrap().clone();
        assert_eq!(entries.len(), 1);
    }
}
