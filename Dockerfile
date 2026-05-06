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
ENTRYPOINT ["./dradis"]
