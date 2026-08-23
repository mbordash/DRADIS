#!/usr/bin/env bash
# Stops the DRADIS process started by start-local.sh

API_PORT=${API_PORT:-9000}

# Tell the supervisor this exit is deliberate BEFORE killing anything, or it
# will dutifully respawn the engine we are trying to stop.
touch .dradis-local.stop

# Kill via PID file if present (tools/supervise-dradis.sh)
if [ -f ".dradis-local.pid" ]; then
    PID=$(cat .dradis-local.pid)
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "🛑 Stopped DRADIS (PID $PID)"
    else
        echo "⚠️  DRADIS PID $PID is not running"
    fi
    rm -f .dradis-local.pid
else
    echo "⚠️  No .dradis-local.pid found — scanning for stale processes..."
fi

# Also free the port in case the terminal was killed and PID file is stale
STALE=$(lsof -ti :$API_PORT 2>/dev/null)
if [ -n "$STALE" ]; then
    kill -9 $STALE 2>/dev/null
    echo "🧹 Killed stale process on :$API_PORT (PID $STALE)"
fi

# Kill any lingering release binary by name
pkill -f "supervise-dradis.sh" 2>/dev/null && echo "🧹 Stopped supervisor"
pkill -f "target/release/dradis" 2>/dev/null && echo "🧹 Killed lingering dradis binary"

rm -f .dradis-local.stop
echo "✅ Done"
