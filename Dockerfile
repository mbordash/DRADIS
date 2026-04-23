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
# Copy only the manifest files first. Docker will cache this layer and the
# cargo build below as long as Cargo.toml/Cargo.lock don't change.
# Changing source files will NOT invalidate the dependency cache.
COPY Cargo.toml Cargo.lock ./

# Create a minimal stub so `cargo build` can resolve and compile all deps.
# After building deps, strip the stub artifacts so the final link step is clean.
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    rm -rf src \
           target/x86_64-unknown-linux-musl/release/.fingerprint/rustpolybot-* \
           target/x86_64-unknown-linux-musl/release/deps/rustpolybot-* \
           target/x86_64-unknown-linux-musl/release/rustpolybot* \
           target/x86_64-unknown-linux-musl/release/incremental

# ── Application source ──────────────────────────────────────────────────────
# Copy the real source. Only this layer and the final link step are re-run
# when source files change.
COPY src ./src
# Build the real binary, then strip all debug symbols and delete every build
# artifact except the final binary to keep this layer as small as possible.
RUN touch src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    strip target/x86_64-unknown-linux-musl/release/rustpolybot && \
    cp target/x86_64-unknown-linux-musl/release/rustpolybot /rustpolybot-bin && \
    rm -rf target /usr/local/cargo/registry /usr/local/cargo/git

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /rustpolybot-bin /rustpolybot
ENTRYPOINT ["/rustpolybot"]