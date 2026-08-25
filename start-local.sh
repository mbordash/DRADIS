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

# The UI-managed secrets file must be per-instance too, and it was the one thing
# here that was not.
#
# `$DRADIS_DATA_DIR/secrets.env` holds every value the Setup view writes, and
# `load_secrets_file()` OVERRIDES the process environment from it at startup. So
# three venues sharing ./data shared one file: setting the AI autonomy tier on
# one venue rewrote it for all of them, and the next restart brought every
# instance up on whichever tier had been set last. Measured 2026-08-25 — the
# operator had set Polymarket US to tier 3, Kalshi to 2 and intl to 1, and every
# llm_actions row on all three venues was stamped tier 1. The tiers cannot be
# tested at all while the file is shared, and nothing reports the collision.
#
# Production is unaffected: one AMI runs one venue with one data directory. This
# is a cost of running three venues out of a single checkout, so the fix belongs
# in the dev launcher rather than the engine.
DATA_DIR="data-${INSTANCE}"
if [ ! -d "$DATA_DIR" ]; then
    mkdir -p "$DATA_DIR"
    # Seed from the shared directory so credentials and the admin password
    # carry over rather than dropping the operator into the first-boot wizard.
    if [ -f "data/secrets.env" ]; then
        cp "data/secrets.env" "$DATA_DIR/secrets.env"
        echo "🔐 Seeded $DATA_DIR/secrets.env from data/secrets.env"
    fi
fi
export DRADIS_DATA_DIR="$DATA_DIR"

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
# Verify the freshly-built binary is actually THIS venue before copying it aside.
#
# All three venue builds write target/release/dradis, so anything that builds
# another venue in between — a `cargo test` matrix, a parallel window — leaves
# the wrong binary at that path. Copying it then hands the instance a different
# venue entirely, and the supervisor keeps restarting it that way. This happened
# on 2026-08-23: a Kalshi build was copied to the Polymarket US instance and ran
# there, visible only because a Kalshi squadron id appeared in the US log.
#
# `--build-venue` prints the compiled venue without starting anything.
BUILT_VENUE=$(./target/release/dradis --build-venue 2>/dev/null || echo "unknown")
if [ "$BUILT_VENUE" != "$VENUE" ]; then
    echo "❌  target/release/dradis is a '$BUILT_VENUE' build, expected '$VENUE'."
    echo "    Something rebuilt it for another venue after the build above."
    echo "    Re-run this script; nothing was copied or started."
    exit 1
fi
# Install atomically: copy to a temp path on the same filesystem, then rename.
#
# `cp` writes through the SAME inode, so overwriting a binary that is currently
# executing invalidates its code signature — macOS then SIGKILLs both the running
# process and any new one launched from that path (observed 2026-08-23: exit 137
# on a plain `--build-venue`). `mv` is a rename: the new file gets a fresh inode
# and the running process keeps the one it mapped, undisturbed until its
# supervisor restarts it.
cp target/release/dradis "$BIN.tmp"
mv -f "$BIN.tmp" "$BIN"
echo "   binary → $BIN ($BUILT_VENUE)"

# Pin the debug symbols to THIS binary.
#
# `[profile.release] debug = 1` emits target/release/dradis.dSYM, but that path
# is rebuilt by any later `cargo build`/`cargo test`, while this instance keeps
# running the binary copied above. The two then describe different code, and
# symbolizing a stall dump against the drifted .dSYM silently produces plausible
# but WRONG answers — on 2026-08-25 it named an axum handler as the thread
# deadlocked in a raptor. Snapshotting the bundle beside the binary keeps them
# matched for as long as this instance lives.
if [ -d target/release/dradis.dSYM ]; then
    rm -rf "$BIN.dSYM.tmp" "$BIN.dSYM"
    cp -R target/release/dradis.dSYM "$BIN.dSYM.tmp"
    mv -f "$BIN.dSYM.tmp" "$BIN.dSYM"
    echo "   symbols → $BIN.dSYM"
fi

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

