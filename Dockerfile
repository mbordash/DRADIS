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
# DRADIS_FEATURES selects the venue build. Default (empty) = intl_clob.
# US retail: --build-arg DRADIS_FEATURES="--no-default-features --features us_retail"
ARG DRADIS_FEATURES=""
COPY Cargo.toml Cargo.lock ./

RUN mkdir -p src && echo "fn main() {}" > src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl $DRADIS_FEATURES && \
    rm -rf src \
           target/x86_64-unknown-linux-musl/release/.fingerprint/dradis-* \
           target/x86_64-unknown-linux-musl/release/deps/dradis-* \
           target/x86_64-unknown-linux-musl/release/dradis* \
           target/x86_64-unknown-linux-musl/release/incremental

# ── Application source ──────────────────────────────────────────────────────
COPY src ./src
# src/config.rs is gitignored — every checkout holds a different profile — so it
# is absent from a fresh clone AND from the `git archive HEAD` that deploy/ami
# uploads to the builder instance. Without this the build dies at
# `error[E0583]: file not found for module 'config'`.
#
# The image ships the CONSERVATIVE profile, matching the Setup view's own advice
# that it is "the recommended starting point".
#
# This is NOT merely a seed. Of the 420 constants in a profile, only 159 are
# backed by DynamicConfig and therefore changeable from the Setup view; the other
# 261 are compile-time, and 140 of those differ between profiles. So the baked
# profile permanently fixes a third of the risk posture no matter which profile a
# customer later selects. Baking the least aggressive one means a customer who
# never touches Setup, or who picks Conservative, is never running something
# racier than they asked for.
RUN cp src/config.conservative.rs.example src/config.rs
RUN touch src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl $DRADIS_FEATURES && \
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
# Use 127.0.0.1 instead of localhost: Alpine containers may not have localhost
# in /etc/hosts, causing "can't connect" failures even when the API is running.
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget -qO- http://127.0.0.1:9000/api/health || exit 1
ENTRYPOINT ["./dradis"]

