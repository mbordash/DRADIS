#!/usr/bin/env bash
# =============================================================================
# start-local.sh — Runs DRADIS + Control Tower locally for development
#
# Usage:
#   ./start-local.sh             # intl CLOB, BTC (default)
#   ./start-local.sh eth         # intl CLOB, ETH
#   VENUE=us ./start-local.sh    # US Retail venue (us_retail build)
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
API_PORT=${API_PORT:-9000}
UI_PORT=${UI_PORT:-3002}

# Map the selected venue to its cargo feature flags + runtime asset.
case "$VENUE" in
    us|us_retail)
        VENUE=us
        CARGO_FEATURE_ARGS=(--no-default-features --features us_retail)
        echo "🚀 Starting DRADIS + Control Tower locally (VENUE=us — US Retail)"
        ;;
    intl|intl_clob)
        VENUE=intl
        CARGO_FEATURE_ARGS=()
        echo "🚀 Starting DRADIS + Control Tower locally (VENUE=intl, CRYPTO=$CRYPTO)"
        ;;
    *)
        echo "❌  Unknown VENUE='$VENUE'. Use VENUE=intl (default) or VENUE=us."
        exit 1
        ;;
esac

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
echo "⚙️  Building DRADIS (release, VENUE=$VENUE)..."
cargo build --release ${CARGO_FEATURE_ARGS[@]+"${CARGO_FEATURE_ARGS[@]}"} 2>&1 | tail -3

if [ "$VENUE" = "us" ]; then
    echo "🦀 Starting DRADIS (US Retail, API on :$API_PORT)..."
    RUST_LOG=${RUST_LOG:-info,dradis=info} \
    API_PORT=$API_PORT \
    ASSETS=${ASSETS:-us} \
        ./target/release/dradis >> logs/dradis-local.log 2>&1 &
else
    echo "🦀 Starting DRADIS (GHOST_MODE, API on :$API_PORT)..."
    RUST_LOG=${RUST_LOG:-info,dradis=info} \
    API_PORT=$API_PORT \
    CRYPTO_FILTER=$CRYPTO \
        ./target/release/dradis >> logs/dradis-local.log 2>&1 &
fi

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

