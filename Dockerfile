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
           target/x86_64-unknown-linux-musl/release/.fingerprint/rustpolybot-* \
           target/x86_64-unknown-linux-musl/release/deps/rustpolybot-* \
           target/x86_64-unknown-linux-musl/release/rustpolybot* \
           target/x86_64-unknown-linux-musl/release/incremental

# ── Application source ──────────────────────────────────────────────────────
COPY src ./src
RUN touch src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    strip target/x86_64-unknown-linux-musl/release/rustpolybot && \
    cp target/x86_64-unknown-linux-musl/release/rustpolybot /rustpolybot-bin && \
    rm -rf target /usr/local/cargo/registry /usr/local/cargo/git

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
ENV TZ=America/New_York
# Create app directory and set as working directory
WORKDIR /app
COPY --from=builder /rustpolybot-bin ./rustpolybot
ENTRYPOINT ["./rustpolybot"]
