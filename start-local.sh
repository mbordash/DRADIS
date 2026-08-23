#!/usr/bin/env bash
# =============================================================================
# start-local.sh — Runs DRADIS + Control Tower locally for development
#
# Usage:
#   ./start-local.sh             # intl CLOB, BTC (default)
#   ./start-local.sh eth         # intl CLOB, ETH
#   VENUE=us ./start-local.sh    # US Retail venue (us_retail build)
#   VENUE=kalshi ./start-local.sh # Kalshi venue (kalshi build)
#   RUST_LOG=debug ./start-local.sh
#
# Venue selection (VENUE env var):
#   intl  → default build (international CLOB, self-custody)   [requires POLYMARKET_PRIVATE_KEY + POLYGON_RPC_URL]
#   us    → us_retail build (US Retail, custodial API key)     [requires POLYMARKET_US_KEY_ID + POLYMARKET_US_SECRET_KEY]
#
# Logs:
#   logs/dradis-local.log     ← DRADIS output
#   Control Tower prints to terminal (hot reload)
#
# Stop both:
#   ./stop-local.sh   OR   Ctrl+C followed by: kill $(cat .dradis-local.pid)
# =============================================================================

set -euo pipefail

VENUE=${VENUE:-intl}
CRYPTO=${1:-btc}

# Every venue runs as its own INSTANCE so two can soak side by side.
#
# Ports were already per-run, but four things were not and would have collided:
# all three venue builds write the same target/release/dradis, and the log, pid
# file and Control Tower port were fixed names. Interleaving two venues into one
# log is the worst of those — correlating the wrong log with the right database
# has cost a whole investigation before.
#
# Defaults keep a single-venue run byte-identical to before: Kalshi on 9000/3002
# with logs/dradis-local.log.
INSTANCE=${INSTANCE:-$VENUE}
case "$VENUE" in
    us|us_retail) DEFAULT_API=9001; DEFAULT_UI=3003 ;;
    kalshi)       DEFAULT_API=9000; DEFAULT_UI=3002 ;;
    *)            DEFAULT_API=9002; DEFAULT_UI=3004 ;;
esac
API_PORT=${API_PORT:-$DEFAULT_API}
UI_PORT=${UI_PORT:-$DEFAULT_UI}

# Per-instance paths. The binary is copied aside because a second `cargo build`
# with different features would otherwise overwrite the running venue's binary.
LOG_FILE="logs/dradis-${INSTANCE}.log"
PID_FILE=".dradis-${INSTANCE}.pid"
STOP_FILE=".dradis-${INSTANCE}.stop"
BIN="target/release/dradis-${INSTANCE}"

# Map the selected venue to its cargo feature flags + runtime asset.
case "$VENUE" in
    us|us_retail)
        VENUE=us
        CARGO_FEATURE_ARGS=(--no-default-features --features us_retail)
        echo "🚀 Starting DRADIS + Control Tower locally (VENUE=us — US Retail)"
        ;;
    kalshi)
        VENUE=kalshi
        CARGO_FEATURE_ARGS=(--no-default-features --features kalshi)
        echo "🚀 Starting DRADIS + Control Tower locally (VENUE=kalshi — Kalshi)"
        ;;
    intl|intl_clob)
        VENUE=intl
        CARGO_FEATURE_ARGS=()
        echo "🚀 Starting DRADIS + Control Tower locally (VENUE=intl, CRYPTO=$CRYPTO)"
        ;;
    *)
        echo "❌  Unknown VENUE='$VENUE'. Use VENUE=intl (default), VENUE=us, or VENUE=kalshi."
        exit 1
        ;;
esac

# ── Sanity checks ─────────────────────────────────────────────────────────────
if [ ! -f ".env" ]; then
    echo "❌  .env not found. Copy .env.example to .env and fill in your credentials."
    exit 1
fi

# Load .env into the environment so the DRADIS binary inherits credentials
# (e.g. DRADIS_API_KEY, POLYMARKET_*). `set -a` auto-exports every variable
# assigned while sourcing; `set +a` restores normal behavior afterwards.
set -a
# shellcheck disable=SC1091
source .env
set +a

if [ ! -f "control-tower/package.json" ]; then
    echo "❌  control-tower/ not found. Run this script from the DRADIS repo root."
    exit 1
fi

# Install UI deps if needed
if [ ! -d "control-tower/node_modules" ]; then
    echo "📦 Installing Control Tower dependencies..."
    (cd control-tower && npm install)
fi

mkdir -p logs

# ── Clean up any previous session ─────────────────────────────────────────────
# A prior run may still be alive (Ctrl+C only stops the UI foreground process;
# the dradis binary keeps running). A stale binary holding :$API_PORT would
# silently serve the dashboard from the OLD build while the new one fails to
# bind — observed 2026-08-08 with three concurrent dradis processes.
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "🧹 Stopping previous DRADIS (PID $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null || true
        sleep 1
    fi
    rm -f "$PID_FILE"
fi
STALE=$(lsof -ti :"$API_PORT" 2>/dev/null || true)
if [ -n "$STALE" ]; then
    echo "🧹 Freeing :$API_PORT (stale PID(s): $STALE)..."
    kill $STALE 2>/dev/null || true
    sleep 1
    # Escalate only if still holding the port
    STALE=$(lsof -ti :"$API_PORT" 2>/dev/null || true)
    [ -n "$STALE" ] && kill -9 $STALE 2>/dev/null || true
fi
# Lingering binaries for THIS instance only. A blanket pkill on
# "target/release/dradis" would kill the other venue soaking alongside it, which
# is precisely what per-instance binaries exist to prevent.
pkill -f "$BIN" 2>/dev/null && echo "🧹 Killed lingering $INSTANCE binary" || true

# Rotate previous log so each session starts clean
if [ -f "$LOG_FILE" ]; then
    mv "$LOG_FILE" "${LOG_FILE}.prev"
    echo "📋 Previous log archived → ${LOG_FILE}.prev"
fi

# ── Start DRADIS API + trading engine ─────────────────────────────────────────
echo "⚙️  Building DRADIS (release, VENUE=$VENUE)..."
cargo build --release ${CARGO_FEATURE_ARGS[@]+"${CARGO_FEATURE_ARGS[@]}"} 2>&1 | tail -3
# Copy the build aside immediately. Building another venue overwrites
# target/release/dradis, and a running instance would then be restarted by its
# supervisor onto the WRONG venue's binary — silently, since the path is the same.
cp target/release/dradis "$BIN"
echo "   binary → $BIN"

# Supervise the engine, the way the container does.
#
# "Restart engine" in the Control Tower works by exiting the process and letting
# something bring it back: docker-compose has `restart: always`, and the stall
# watchdog's process::exit(1) relies on the same contract. The supervisor lives in
# its own script — see tools/supervise-dradis.sh for why a shell function here
# could not do the job.
echo "🦀 Starting DRADIS ($VENUE, API on :$API_PORT, supervised)..."
nohup ./tools/supervise-dradis.sh "$VENUE" "$API_PORT" "$CRYPTO" "$INSTANCE" >> "$LOG_FILE" 2>&1 &

DRADIS_PID=$!
echo $DRADIS_PID > "$PID_FILE"
echo "   supervisor PID $DRADIS_PID → $LOG_FILE"

# Wait for the API to come up
echo -n "   Waiting for API on :$API_PORT"
for i in $(seq 1 20); do
    if curl -sf "http://127.0.0.1:$API_PORT/api/health" > /dev/null 2>&1; then
        echo " ✓"
        break
    fi
    echo -n "."
    sleep 1
done

# ── Start Control Tower UI ─────────────────────────────────────────────────────
echo "🌐 Starting Control Tower UI on :$UI_PORT..."
echo "   (hot reload — press Ctrl+C to stop this process)"
echo ""
echo "   Dashboard → http://localhost:$UI_PORT"
echo "   API       → http://localhost:$API_PORT/api/health"
echo ""

# Route ALL browser traffic through the Next.js proxy (src/app/api/[...path]/route.ts)
# instead of letting the browser call :$API_PORT directly. The proxy runs server-side
# and injects X-API-Key from DRADIS_API_KEY — the browser never sees the key, and
# requests stop 401'ing when DRADIS_API_KEY is set on the engine.
#
# Forcing NEXT_PUBLIC_API_URL='' here OVERRIDES any value in control-tower/.env.local
# (a process-env var set before `next dev` wins over .env files), so BASE='' in api.ts
# → same-origin /api/* → proxy.
#
# The proxy reads DRADIS_API_KEY from the environment. It is inherited here from .env
# (sourced above with `set -a`) — keep DRADIS_API_KEY in .env so the engine and the
# proxy use the SAME value. (Next also auto-loads control-tower/.env.local, but .env
# is the single source of truth that the Rust engine reads too.)
# NEXT_DIST_DIR keeps two instances' build artifacts apart; without it a second
# dev server overwrites the first's .next and the running one starts answering
# 500 with MODULE_NOT_FOUND.
NEXT_PUBLIC_API_URL='' \
DRADIS_API_URL=http://127.0.0.1:$API_PORT \
NEXT_DIST_DIR=".next-$INSTANCE" \
    npm --prefix control-tower run dev -- -p $UI_PORT

