#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# fix-arb-pnl-history.sh
#
# Fixes historical ArbitrageStrategy trade PnL records in all production DBs.
#
# Three bugs were present in record_settled_arb_trade() (now fixed in code):
#   1. All settlements were written to the BTC (primary) DB, so SOL/ETH markets
#      accumulated in btc-dradis.db with wrong asset attribution.
#   2. PERMANENTLY_SETTLED_CONDITIONS is in-memory → cleared on restart → the same
#      settlement was re-recorded once per session restart (up to ~10× duplicates).
#   3. Single-leg P&L was (1.00 - entry_winner) × shares, ignoring the loser's cost
#      (e.g. +$4.60 shown instead of +$0.10 for a near-50/50 arb pair).
#
# This script corrects historical records in btc-dradis.db, eth-dradis.db, and
# sol-dradis.db.  It is safe to run while DRADIS is stopped (recommended) or live.
#
# Usage (on the production server):
#   cd ~/dradis
#   bash tools/fix-arb-pnl-history.sh [logs/]
#
# Pass the logs directory as the first argument (default: logs/)
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

LOGS_DIR="${1:-logs}"
BTC_DB="${LOGS_DIR}/btc-dradis.db"
ETH_DB="${LOGS_DIR}/eth-dradis.db"
SOL_DB="${LOGS_DIR}/sol-dradis.db"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Pre-flight checks ─────────────────────────────────────────────────────────

command -v sqlite3 >/dev/null 2>&1 || error "sqlite3 not found"

for db in "$BTC_DB" "$ETH_DB" "$SOL_DB"; do
    [ -f "$db" ] || warn "Database not found, skipping: $db"
done

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo " DRADIS ArbitrageStrategy PnL History Fix"
echo " Target: $LOGS_DIR"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# ── Helper: count rows matching a query ───────────────────────────────────────
count_rows() {
    local db="$1" sql="$2"
    sqlite3 "$db" "$sql" 2>/dev/null || echo 0
}

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Show before state
# ─────────────────────────────────────────────────────────────────────────────

echo "── Before state ─────────────────────────────────────────────────────────"
for db in "$BTC_DB" "$ETH_DB" "$SOL_DB"; do
    [ -f "$db" ] || continue
    name=$(basename "$db" .db)
    total=$(count_rows "$db" "SELECT COUNT(*) FROM trades WHERE strategy='ArbitrageStrategy';")
    single=$(count_rows "$db" "SELECT COUNT(*) FROM trades WHERE strategy='ArbitrageStrategy' AND reason LIKE 'Settlement (single-leg%';")
    correct=$(count_rows "$db" "SELECT COUNT(*) FROM trades WHERE strategy='ArbitrageStrategy' AND reason NOT LIKE 'Settlement (single-leg%';")
    info "${name}: ${total} arb trades  (${single} single-leg [BOGUS]  /  ${correct} correct)"
done
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: BTC DB — remove all single-leg settlements for non-BTC markets
#
# These are Solana / Ethereum / unknown markets that were written to the BTC DB
# due to the asset-routing bug.  The correct records already exist in SOL/ETH DBs.
# ─────────────────────────────────────────────────────────────────────────────

if [ -f "$BTC_DB" ]; then
    info "BTC DB: removing non-BTC single-leg settlements..."

    # Count what we'll delete
    to_delete=$(count_rows "$BTC_DB" "
        SELECT COUNT(*) FROM trades
        WHERE strategy = 'ArbitrageStrategy'
          AND reason LIKE 'Settlement (single-leg%'
          AND (
            market LIKE '%Solana%' OR market LIKE '%SOL%' OR market LIKE '%solana%'
            OR market LIKE '%Ethereum%' OR market LIKE '%ETH%' OR market LIKE '%ethereum%'
          );
    ")
    info "  → ${to_delete} non-BTC single-leg record(s) to delete"

    sqlite3 "$BTC_DB" "
        DELETE FROM trades
        WHERE strategy = 'ArbitrageStrategy'
          AND reason LIKE 'Settlement (single-leg%'
          AND (
            market LIKE '%Solana%' OR market LIKE '%SOL%' OR market LIKE '%solana%'
            OR market LIKE '%Ethereum%' OR market LIKE '%ETH%' OR market LIKE '%ethereum%'
          );
    "
    info "  ✓ Deleted ${to_delete} record(s)"

    # Also delete any remaining single-leg BTC settlements (duplicates of correct records)
    # Keep only if there is NO correct (non-single-leg) record for the same market
    remaining_single=$(count_rows "$BTC_DB" "
        SELECT COUNT(*) FROM trades
        WHERE strategy = 'ArbitrageStrategy'
          AND reason LIKE 'Settlement (single-leg%';
    ")
    if [ "$remaining_single" -gt 0 ]; then
        info "  BTC DB still has ${remaining_single} single-leg records for BTC markets"
        # For each, check if there's already a correct settlement for the same market
        warn "  Checking if any remaining single-leg records have correct counterparts..."
        sqlite3 "$BTC_DB" "
            DELETE FROM trades
            WHERE strategy = 'ArbitrageStrategy'
              AND reason LIKE 'Settlement (single-leg%'
              AND market IN (
                SELECT DISTINCT market FROM trades
                WHERE strategy = 'ArbitrageStrategy'
                  AND reason NOT LIKE 'Settlement (single-leg%'
              );
        "
        still_remaining=$(count_rows "$BTC_DB" "
            SELECT COUNT(*) FROM trades
            WHERE strategy = 'ArbitrageStrategy'
              AND reason LIKE 'Settlement (single-leg%';
        ")
        info "  → ${still_remaining} single-leg record(s) remaining (genuine single-leg BTC positions)"
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: ETH DB — remove any single-leg settlements that don't belong to ETH markets
# ─────────────────────────────────────────────────────────────────────────────

if [ -f "$ETH_DB" ]; then
    info "ETH DB: removing non-ETH single-leg settlements..."
    to_delete=$(count_rows "$ETH_DB" "
        SELECT COUNT(*) FROM trades
        WHERE strategy = 'ArbitrageStrategy'
          AND reason LIKE 'Settlement (single-leg%'
          AND market NOT LIKE '%Ethereum%'
          AND market NOT LIKE '%ETH%'
          AND market NOT LIKE '%ethereum%';
    ")
    if [ "$to_delete" -gt 0 ]; then
        sqlite3 "$ETH_DB" "
            DELETE FROM trades
            WHERE strategy = 'ArbitrageStrategy'
              AND reason LIKE 'Settlement (single-leg%'
              AND market NOT LIKE '%Ethereum%'
              AND market NOT LIKE '%ETH%'
              AND market NOT LIKE '%ethereum%';
        "
        info "  ✓ Deleted ${to_delete} non-ETH single-leg record(s)"
    else
        info "  ✓ No non-ETH single-leg records found"
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: SOL DB — remove any single-leg settlements that don't belong to SOL markets
# ─────────────────────────────────────────────────────────────────────────────

if [ -f "$SOL_DB" ]; then
    info "SOL DB: removing non-SOL single-leg settlements..."
    to_delete=$(count_rows "$SOL_DB" "
        SELECT COUNT(*) FROM trades
        WHERE strategy = 'ArbitrageStrategy'
          AND reason LIKE 'Settlement (single-leg%'
          AND market NOT LIKE '%Solana%'
          AND market NOT LIKE '%SOL%'
          AND market NOT LIKE '%solana%';
    ")
    if [ "$to_delete" -gt 0 ]; then
        sqlite3 "$SOL_DB" "
            DELETE FROM trades
            WHERE strategy = 'ArbitrageStrategy'
              AND reason LIKE 'Settlement (single-leg%'
              AND market NOT LIKE '%Solana%'
              AND market NOT LIKE '%SOL%'
              AND market NOT LIKE '%solana%';
        "
        info "  ✓ Deleted ${to_delete} non-SOL single-leg record(s)"
    else
        info "  ✓ No non-SOL single-leg records found"
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: All DBs — deduplicate correct settlements (same market, multiple rows)
#         Keep only the EARLIEST correct settlement per market (first = most accurate,
#         recorded when both YES+NO legs were still visible in the Data API).
# ─────────────────────────────────────────────────────────────────────────────

info "All DBs: deduplicating repeated correct settlements per market..."

for db in "$BTC_DB" "$ETH_DB" "$SOL_DB"; do
    [ -f "$db" ] || continue
    name=$(basename "$db" .db)

    dupes=$(count_rows "$db" "
        SELECT COUNT(*) FROM trades
        WHERE strategy = 'ArbitrageStrategy'
          AND id NOT IN (
            SELECT MIN(id)
            FROM trades
            WHERE strategy = 'ArbitrageStrategy'
            GROUP BY market
          );
    ")

    if [ "$dupes" -gt 0 ]; then
        sqlite3 "$db" "
            DELETE FROM trades
            WHERE strategy = 'ArbitrageStrategy'
              AND id NOT IN (
                SELECT MIN(id)
                FROM trades
                WHERE strategy = 'ArbitrageStrategy'
                GROUP BY market
              );
        "
        info "  ${name}: removed ${dupes} duplicate settlement(s)"
    else
        info "  ${name}: no duplicates found"
    fi
done

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: Show final state
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo "── After state ──────────────────────────────────────────────────────────"
for db in "$BTC_DB" "$ETH_DB" "$SOL_DB"; do
    [ -f "$db" ] || continue
    name=$(basename "$db" .db)
    echo ""
    info "${name}: ArbitrageStrategy settlements:"
    sqlite3 "$db" "
        SELECT '  ' || market || '  ' || side || '  pnl=$' || ROUND(CAST(pnl AS REAL), 4) || '  [' || reason || ']'
        FROM trades
        WHERE strategy = 'ArbitrageStrategy'
        ORDER BY ts DESC;
    " 2>/dev/null || true
done

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo " ✅ Cleanup complete."
echo ""
echo " NOTE: This script cleaned historical records in the local DB copies."
echo " The forward-going fix (record_settled_arb_trade in cleanup.rs) requires"
echo " deploying the updated DRADIS binary to production."
echo "═══════════════════════════════════════════════════════════════"
echo ""

