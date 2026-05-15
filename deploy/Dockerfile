# Multi-stage build. Each binary is its own final image so we don't ship
# the toolchain. `cargo chef` would speed up CI; kept minimal here.

# Pin to bookworm so the resulting binaries link against glibc 2.36
# — same version as the debian:bookworm-slim runtime stage. Without
# the suffix `rust:1.89-slim` follows Debian's testing/stable rotation
# and picks up glibc 2.38+ from trixie, which then ELF-fails at
# runtime with `GLIBC_2.38 not found`.
FROM rust:1.89-slim-bookworm AS builder
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
# Pre-create the WAL directory owned by `nobody` so that when a docker
# named volume is mounted here for the first time, the volume copies
# this ownership/permission. Without the chown the volume would be
# root:root and `nobody` would fail every WAL write with EACCES — and
# WAL failures break every minute-flush, so the OI table stays empty.
# Numeric ids because the `nobody` shell user doesn't have a stable
# uid across distros (Debian uses 65534).
RUN mkdir -p /var/lib/oi/wal && chown -R 65534:65534 /var/lib/oi
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
