FROM rust:1.91-alpine AS builder
RUN apk add --no-cache \
    build-base \
    musl-dev \
    pkgconfig \
    openssl-dev \
    openssl-libs-static \
    perl \
    make \
    zlib-dev
RUN rustup target add x86_64-unknown-linux-musl
WORKDIR /app

# ── Dependency caching layer ────────────────────────────────────────────────
COPY Cargo.toml Cargo.lock ./

RUN mkdir -p src && echo "fn main() {}" > src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    rm -rf src \
           target/x86_64-unknown-linux-musl/release/.fingerprint/dradis-* \
           target/x86_64-unknown-linux-musl/release/deps/dradis-* \
           target/x86_64-unknown-linux-musl/release/dradis* \
           target/x86_64-unknown-linux-musl/release/incremental

# ── Application source ──────────────────────────────────────────────────────
COPY src ./src
RUN touch src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    strip target/x86_64-unknown-linux-musl/release/dradis && \
    cp target/x86_64-unknown-linux-musl/release/dradis /dradis-bin && \
    rm -rf target /usr/local/cargo/registry /usr/local/cargo/git

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
ENV TZ=America/New_York
WORKDIR /app
# Control Tower REST API (axum)
EXPOSE 9000
COPY --from=builder /dradis-bin ./dradis
# Liveness check: /api/health must respond within 10s.
# Docker will mark the container unhealthy after 3 consecutive failures
# (~90 s of silence) so an operator / restart policy can act on it.
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget -qO- http://localhost:9000/api/health || exit 1
ENTRYPOINT ["./dradis"]

