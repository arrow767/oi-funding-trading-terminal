# Adding a new exchange

This is the "единый скил" the project is designed around — every new venue
should take the same shape and touch only one module.

## Checklist

1. **Read the docs first.**
   Confirm these four things before writing any code:
   * Endpoint(s) returning perpetual OI. Note the **unit** (coins vs. contracts
     vs. USD). Most contract exchanges ALSO return coins; prefer that.
   * Batching: can one call fetch the whole universe, or is it per-symbol?
   * Rate limits and error codes (especially 418/429 analogues).
   * If WS is offered for OI: channel name, subscription shape, heartbeat
     expectations.
   * For contract units: `contractSize` / `multiplier` / `ctVal` — WHERE
     is it published and how does it change over time?

2. **Add a case to `Exchange`** (if not already present) in
   [`crates/oi-core/src/exchange.rs`](../crates/oi-core/src/exchange.rs).
   The compiler will walk you through every required match arm.

3. **Create `crates/oi-exchanges/src/<name>/mod.rs`.**
   Copy the `binance/` module as a starting point. Replace the base URL,
   endpoint paths, and response structs. Fill the unit accordingly:
   * Coins-native → `UnitKind::Coins`, `contract_multiplier: None`.
   * Contracts-native → `UnitKind::Contracts`, set
     `contract_multiplier` from the discovery endpoint.
   * USD-native (rare) → `UnitKind::Usd`.

4. **Register the module** in
   [`crates/oi-exchanges/src/lib.rs`](../crates/oi-exchanges/src/lib.rs).

5. **Wire it into the collector** at the match in
   [`crates/oi-collector/src/runner.rs`](../crates/oi-collector/src/runner.rs).
   Add one match arm. No other code changes.

6. **Write tests** in the adapter's `mod.rs`:
   * `parses_discover_response_shape` — paste a real captured response,
     assert the parsed struct.
   * `parses_openinterest_response_shape` — same, for the OI endpoint.
   * If contracts-native, include a test that enrichment produces the
     expected coins & USD values for a known symbol.

7. **Measure before shipping**:
   * Run locally for 10 minutes, check ClickHouse for 10 rows per symbol.
   * Compare with the exchange's published OI on their own dashboard.
   * Check the `oi_coins` column is within 0.1% of the displayed number.

## Common gotchas

* **Hyphenated symbols:** BingX uses `BTC-USDT`, OKX uses
  `BTC-USDT-SWAP`. Keep the exchange-native form as the symbol.
* **Inverse contracts:** Binance COIN-M has `contractSize` in USD, not
  coins. Mark those with `UnitKind::Contracts` and a multiplier whose
  unit is documented in the instrument metadata.
* **WS subscription caps:** KuCoin, Hyperliquid have "max subscriptions
  per connection" limits. Shard across connections if exceeded.
* **Timestamp drift:** always use the collector-supplied `bucket_ts`; do
  not derive from the exchange's server timestamp. Different exchanges
  skew up to ±3 seconds.
* **Decimal precision:** parse strings via `rust_decimal::Decimal`,
  never `f64`. Some coins (SHIB, PEPE) have 1e9+ OI values; f64 loses
  resolution.

## Verification against the contract

After implementing, a CI job runs the adapter against a recorded fixture:
`tests/fixtures/<exchange>/oi_snapshot.json`. If the adapter's parsing
diverges from the captured shape, the test fails and surfaces a schema
drift — the adapter is updated, not the fixture.
