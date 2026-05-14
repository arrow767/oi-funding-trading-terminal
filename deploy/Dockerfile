# Multi-stage build. Each binary is its own final image so we don't ship
# the toolchain. `cargo chef` would speed up CI; kept minimal here.

FROM rust:1.89-slim AS builder
WORKDIR /build

# System deps needed by reqwest (rustls), clickhouse (LZ4).
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev cmake \
 && rm -rf /var/lib/apt/lists/*

COPY . .
# Cap parallelism: rustc+LLVM use ~1.5–2 GB per job during release
# optimization, so on a 12 GB host (Aurum-class) -j8 risks OOM during
# the linker pass. -j4 keeps peak around 6–8 GB. Bump if you have more.
ENV CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0
RUN cargo build --release --bin oi-collector --bin oi-api

# --- collector -------------------------------------------------------------
FROM debian:bookworm-slim AS collector
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/oi-collector /usr/local/bin/oi-collector
COPY --from=builder /build/migrations /opt/oi/migrations
USER nobody
ENTRYPOINT ["/usr/local/bin/oi-collector"]

# --- api -------------------------------------------------------------------
FROM debian:bookworm-slim AS api
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/oi-api /usr/local/bin/oi-api
USER nobody
EXPOSE 50051 8080
ENTRYPOINT ["/usr/local/bin/oi-api"]
