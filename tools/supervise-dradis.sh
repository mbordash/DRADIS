#!/usr/bin/env bash
#
# Keep the local DRADIS engine running, the way docker-compose's `restart: always`
# does in the container.
#
# This is a standalone script rather than a function inside start-local.sh on
# purpose. As a backgrounded shell function it shared job control with its parent:
# killing the engine reported the *parent's* job as terminated and left no
# supervisor behind, so the loop never ran even once — the local log contained
# zero respawn lines across a whole day of restarts. A separate script has its own
# PID and its own children, so the engine is unambiguously a child that can die
# without taking the supervisor with it.
#
# Usage: supervise-dradis.sh <venue> <api_port> [crypto_filter] [instance]
set -u

VENUE=${1:?venue required}
API_PORT=${2:?api port required}
CRYPTO=${3:-btc}
# Instance name scopes the log, the stop marker and the binary, so two venues can
# soak side by side without one's restart touching the other.
INSTANCE=${4:-$VENUE}

cd "$(dirname "$0")/.." || exit 1
LOG="logs/dradis-${INSTANCE}.log"
STOP=".dradis-${INSTANCE}.stop"
BIN="target/release/dradis-${INSTANCE}"
# Fall back to the shared path for a supervisor started before per-instance
# binaries existed, so an in-flight run is not left pointing at nothing.
[ -x "$BIN" ] || BIN="target/release/dradis"

# Never die because a terminal went away or a reader closed a pipe: this process
# outlives the shell that started it.
trap '' PIPE HUP

rm -f "$STOP"
# Re-check on every supervisor start, not just at launch: the binary can be
# replaced under a long-running supervisor by a later build.
BUILT=$("./$BIN" --build-venue 2>/dev/null || echo unknown)
if [ "$BUILT" != "$VENUE" ]; then
    echo "❌ $BIN is a '$BUILT' build but this supervisor is for '$VENUE' — refusing to start" >> "$LOG"
    exit 1
fi
echo "🛡️  Supervisor started (venue=$VENUE instance=$INSTANCE port=$API_PORT bin=$BIN pid=$$)" >> "$LOG"

while true; do
    # Re-read .env before every launch.
    #
    # start-local.sh sources .env once with `set -a`, so the engine inherits
    # those values — and dotenv does NOT override an existing environment
    # variable. An edit to .env therefore had no effect on restart: the stale
    # exported value won, silently. Observed 2026-08-23 with
    # ENABLE_LLM_ADVISOR, where flipping it to true and restarting the engine
    # left the advisor switched off with nothing in the log to say why.
    #
    # Sourcing here means a restart picks up the file, which is what an operator
    # editing .env and restarting expects.
    if [ -f .env ]; then
        set -a
        # shellcheck disable=SC1091
        . ./.env
        set +a
    fi

    case "$VENUE" in
        us)     ASSETS=${ASSETS:-us}     API_PORT=$API_PORT RUST_LOG=${RUST_LOG:-info,dradis=info} "./$BIN" >> "$LOG" 2>&1 ;;
        kalshi) ASSETS=${ASSETS:-kalshi} API_PORT=$API_PORT RUST_LOG=${RUST_LOG:-info,dradis=info} "./$BIN" >> "$LOG" 2>&1 ;;
        *)      CRYPTO_FILTER=$CRYPTO    API_PORT=$API_PORT RUST_LOG=${RUST_LOG:-info,dradis=info} "./$BIN" >> "$LOG" 2>&1 ;;
    esac
    code=$?

    if [ -f "$STOP" ]; then
        echo "🛑 Engine exited ($code) and a stop was requested — supervisor exiting" >> "$LOG"
        exit 0
    fi
    echo "♻️  Engine exited ($code) — respawning in 2s" >> "$LOG"
    sleep 2
done
