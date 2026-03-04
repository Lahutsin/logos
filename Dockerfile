# syntax=docker/dockerfile:1

# Build stage
FROM rust:1.78-bookworm AS builder
WORKDIR /app

# Pre-copy manifests for caching
COPY Cargo.toml Cargo.lock ./
# Copy sources
COPY src ./src
COPY examples ./examples
COPY tests ./tests

RUN cargo build --release

# Runtime stage (small, glibc, with CA certs)
FROM debian:bookworm-slim
RUN useradd -m logos \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/logos /app/logos

USER logos
EXPOSE 9092 9100

ENV RK_BIND_ADDR=0.0.0.0:9092 \
    RK_ADMIN_ADDR=0.0.0.0:9100 \
    RK_DATA_DIR=/var/lib/logos \
    RK_METADATA_REFRESH_MS=1000 \
    RK_MAX_CONNECTIONS=512 \
    RK_REPLICATION_ACKS=1 \
    RK_REPLICATION_TIMEOUT_MS=1000 \
    RK_REPLICATION_RETRIES=2 \
    RK_REPLICATION_BACKOFF_MS=200 \
    RK_FSYNC=true

VOLUME ["/var/lib/logos"]
ENTRYPOINT ["/app/logos"]
