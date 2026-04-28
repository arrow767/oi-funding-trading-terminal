# Testing

```sh
cargo test --workspace               # 125 unit + integration, no Docker
cargo test --workspace -- --ignored  # +2 testcontainers tests, needs Docker
```

## Test taxonomy

* **Unit (`#[test]` / `#[tokio::test]` inside `src/`).** Pure
  functions, parsers, state machines. ~80% of the suite.
* **Wiremock integration (`crates/oi-exchanges/tests/`).** A real
  exchange adapter against a local mock HTTP server. Catches schema
  drift and HTTP status mapping — every production adapter has one.
* **Live-server integration
  (`crates/oi-api/tests/auth_middleware.rs`,
  `tls_roundtrip.rs`).** Boots the actual axum router on an ephemeral
  port and drives it with `reqwest`. Catches middleware ordering,
  TLS feature flags, `from_pem_file` quirks.
* **Testcontainers (`#[ignore]`).** Boots a real ClickHouse / Redis
  container. Ground-truth proof that the SQL/Redis we generate
  works against real engines, not just the crate's typecheck.

## Running the testcontainers suite

Both tests are gated `#[ignore]` so they don't slow down the default
suite or fail in environments without Docker. Run them when
exercising changes to storage paths:

```sh
# Storage-only round-trip: schema apply, upsert, range, latest.
cargo test -p oi-storage -- --ignored clickhouse_roundtrip

# Full black-box: fake Binance → collector logic → real CH + Redis
# → REST endpoint → reqwest assertion.
cargo test -p oi-api -- --ignored e2e_binance_through_rest
```

Requirements:

* Docker daemon reachable. On macOS / Windows that means
  Docker Desktop running; on Linux that means the user is in the
  `docker` group or running with sudo.
* Internet access for the first run (pulls
  `clickhouse/clickhouse-server:24.8` and `redis:7.4-alpine`).

The suite normally completes in 15–25 s once images are cached.

## CI suggestion

A two-job pipeline mirrors local development:

```yaml
# .github/workflows/ci.yml
jobs:
  fast:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@1.89
      - uses: Swatinem/rust-cache@v2
      - run: cargo test --workspace --locked

  integration:
    runs-on: ubuntu-latest
    services: {}  # docker daemon is provided by the runner image
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@1.89
      - uses: Swatinem/rust-cache@v2
      - run: cargo test --workspace --locked -- --ignored
```

`fast` blocks PR merges; `integration` blocks main-branch pushes
(or runs on a nightly schedule if you want PRs to be cheaper).
