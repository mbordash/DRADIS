#!/usr/bin/env bash
# Stops the DRADIS process started by start-local.sh

# Stops ONE instance. Defaults to the Kalshi instance for backwards
# compatibility; pass INSTANCE=us to stop the Polymarket US soak instead.
INSTANCE=${INSTANCE:-kalshi}
case "$INSTANCE" in
    us) API_PORT=${API_PORT:-9001} ;;
    kalshi) API_PORT=${API_PORT:-9000} ;;
    *) API_PORT=${API_PORT:-9002} ;;
esac
PID_FILE=".dradis-${INSTANCE}.pid"
STOP_FILE=".dradis-${INSTANCE}.stop"
BIN="target/release/dradis-${INSTANCE}"

# Tell the supervisor this exit is deliberate BEFORE killing anything, or it
# will dutifully respawn the engine we are trying to stop.
touch "$STOP_FILE"

# Kill via PID file if present (tools/supervise-dradis.sh)
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "🛑 Stopped DRADIS (PID $PID)"
    else
        echo "⚠️  DRADIS PID $PID is not running"
    fi
    rm -f "$PID_FILE"
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
pkill -f "supervise-dradis.sh .* ${INSTANCE}$" 2>/dev/null && echo "🧹 Stopped $INSTANCE supervisor"
pkill -f "$BIN" 2>/dev/null && echo "🧹 Killed lingering $INSTANCE binary"

rm -f "$STOP_FILE"
echo "✅ Done"
