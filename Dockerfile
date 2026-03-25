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
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rustpolybot /rustpolybot
ENTRYPOINT ["/rustpolybot"]