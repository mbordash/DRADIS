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
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    rm -rf src

# ── Application source ──────────────────────────────────────────────────────
# Copy the real source. Only this layer and the final link step are re-run
# when source files change.
COPY src ./src
# Touch main.rs so cargo knows the source is newer than the cached dep artifacts.
RUN touch src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rustpolybot /rustpolybot
ENTRYPOINT ["/rustpolybot"]