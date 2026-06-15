#!/usr/bin/env bash
# =============================================================================
# start-local.sh — Runs DRADIS + Control Tower locally for development
#
# Usage:
#   ./start-local.sh          # BTC (default)
#   ./start-local.sh eth      # ETH
#   RUST_LOG=debug ./start-local.sh
#
# Logs:
#   logs/dradis-local.log     ← DRADIS output
#   Control Tower prints to terminal (hot reload)
#
# Stop both:
#   ./stop-local.sh   OR   Ctrl+C followed by: kill $(cat .dradis-local.pid)
# =============================================================================

set -euo pipefail

CRYPTO=${1:-btc}
API_PORT=${API_PORT:-9000}
UI_PORT=${UI_PORT:-3002}

echo "🚀 Starting DRADIS + Control Tower locally (CRYPTO=$CRYPTO)"

# ── Sanity checks ─────────────────────────────────────────────────────────────
if [ ! -f ".env" ]; then
    echo "❌  .env not found. Copy .env.example to .env and fill in your credentials."
    exit 1
fi

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

# Rotate previous log so each session starts clean
if [ -f "logs/dradis-local.log" ]; then
    mv "logs/dradis-local.log" "logs/dradis-local.log.prev"
    echo "📋 Previous log archived → logs/dradis-local.log.prev"
fi

# ── Start DRADIS API + trading engine ─────────────────────────────────────────
echo "⚙️  Building DRADIS (release)..."
cargo build --release 2>&1 | tail -3

echo "🦀 Starting DRADIS (GHOST_MODE, API on :$API_PORT)..."
RUST_LOG=${RUST_LOG:-info,dradis=info} \
API_PORT=$API_PORT \
CRYPTO_FILTER=$CRYPTO \
    ./target/release/dradis >> logs/dradis-local.log 2>&1 &

DRADIS_PID=$!
echo $DRADIS_PID > .dradis-local.pid
echo "   PID $DRADIS_PID → logs/dradis-local.log"

# Wait for the API to come up
echo -n "   Waiting for API on :$API_PORT"
for i in $(seq 1 20); do
    if curl -sf "http://localhost:$API_PORT/api/health" > /dev/null 2>&1; then
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

DRADIS_API_URL=http://localhost:$API_PORT \
    npm --prefix control-tower run dev -- -p $UI_PORT

