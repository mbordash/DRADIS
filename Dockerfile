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

# ── Venue selection ─────────────────────────────────────────────────────────
# The three venues are mutually exclusive Cargo features, so each is a separate
# binary. DRADIS_VENUES is a space-separated list of the ones to bake:
#
#   (unset)            → intl only. Production (deploy-live.sh) and local dev.
#   "intl us kalshi"   → all three. The AWS Marketplace AMI, which is one
#                        product covering every venue; deploy/ami/provision.sh
#                        passes this and the customer picks in the Setup view.
#
# Keeping the default single-venue matters: each extra venue is a full Rust
# release build, and cargo rebuilds dependencies whenever the feature set
# changes, so a three-venue image costs roughly three times the build time.
ARG DRADIS_VENUES="intl"
COPY Cargo.toml Cargo.lock ./

# ── Dependency caching layer ────────────────────────────────────────────────
# Primes the dependency graph for the FIRST venue in the list. Later venues
# enable different optional dependencies (alloy / polymarket-us / rsa), so
# cargo rebuilds for those regardless — the win is real only for the common
# single-venue case, which is exactly the one that gets rebuilt often.
RUN set -eux; \
    first=$(set -- $DRADIS_VENUES; echo "$1"); \
    case "$first" in \
        intl)   flags="" ;; \
        us)     flags="--no-default-features --features us_retail" ;; \
        kalshi) flags="--no-default-features --features kalshi" ;; \
        *) echo "unknown venue '$first' in DRADIS_VENUES (want: intl us kalshi)"; exit 1 ;; \
    esac; \
    mkdir -p src && echo "fn main() {}" > src/main.rs; \
    cargo build --release --target x86_64-unknown-linux-musl $flags; \
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
# Each venue is built once and copied twice: the unstripped binary is kept for
# symbols, the stripped one ships.
#
# `[profile.release] debug = 1` puts line tables in the binary so the OS
# watchdog's thread dump can name source lines. `strip` removes exactly that,
# and stripping is still right for the runtime image — debug info would land in
# every customer's AMI to no purpose. So the symbols are copied aside first and
# exported by the `debuginfo` stage below, which deploy/ami/provision.sh
# extracts and build-ami.sh retrieves with the release. Without it a deadlock on
# a customer's box produces stacks that cannot be symbolized, which is the one
# place such a dump matters most.
RUN set -eux; \
    mkdir -p /out/bin /out/debug; \
    for v in $DRADIS_VENUES; do \
        case "$v" in \
            intl)   flags="" ;; \
            us)     flags="--no-default-features --features us_retail" ;; \
            kalshi) flags="--no-default-features --features kalshi" ;; \
            *) echo "unknown venue '$v' in DRADIS_VENUES (want: intl us kalshi)"; exit 1 ;; \
        esac; \
        echo "── building venue: $v ──"; \
        touch src/main.rs; \
        cargo build --release --target x86_64-unknown-linux-musl $flags; \
        cp target/x86_64-unknown-linux-musl/release/dradis "/out/debug/dradis-$v.debug"; \
        strip target/x86_64-unknown-linux-musl/release/dradis; \
        cp target/x86_64-unknown-linux-musl/release/dradis "/out/bin/dradis-$v"; \
    done; \
    rm -rf target /usr/local/cargo/registry /usr/local/cargo/git

# Symbols for the binaries above, in a stage of their own so they never reach
# the runtime image. Extracted with `docker create` + `docker cp` against
# `--target debuginfo`; see deploy/ami/provision.sh.
FROM scratch AS debuginfo
COPY --from=builder /out/debug /

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
ENV TZ=America/New_York
WORKDIR /app
# Control Tower REST API (axum)
EXPOSE 9000
# One binary per baked venue; entrypoint.sh selects at container start.
COPY --from=builder /out/bin ./bin
COPY deploy/entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh
# The engine opens its SQLite shards at logs/<asset>-dradis.db, relative to this
# WORKDIR. SQLite creates the FILE but never the directory, so without this the
# open fails, DB_POOL is never set, and every database-backed endpoint answers
# "DB not ready" forever — which is exactly how the Marketplace AMI behaved
# until 2026-08-20. deploy-live.sh happened to hide it by bind-mounting a logs
# directory over the same path.
RUN mkdir -p /app/logs /app/data
# Liveness check: /api/health must respond within 10s.
# Docker will mark the container unhealthy after 3 consecutive failures
# (~90 s of silence) so an operator / restart policy can act on it.
# Use 127.0.0.1 instead of localhost: Alpine containers may not have localhost
# in /etc/hosts, causing "can't connect" failures even when the API is running.
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget -qO- http://127.0.0.1:9000/api/health || exit 1
ENTRYPOINT ["/app/entrypoint.sh"]

