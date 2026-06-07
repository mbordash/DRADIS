#!/usr/bin/env bash
#
# cleanup-contaminated-trades.sh - Remove contaminated Bitcoin trades from ETH/SOL databases
#
# Run this on the production server AFTER deploying the fix to prevent new contamination.
# This will delete all Bitcoin trades that were incorrectly written to non-BTC asset databases.

set -euo pipefail

DRADIS_DIR="${1:-~/dradis/logs}"

echo "🧹 Cleaning contaminated trades from asset databases..."
echo "   Directory: $DRADIS_DIR"
echo ""

# Backup databases before cleaning
echo "📦 Creating backups..."
cp "$DRADIS_DIR/eth-dradis.db" "$DRADIS_DIR/eth-dradis.db.backup-$(date +%Y%m%d-%H%M%S)"
cp "$DRADIS_DIR/sol-dradis.db" "$DRADIS_DIR/sol-dradis.db.backup-$(date +%Y%m%d-%H%M%S)"
echo "✅ Backups created"
echo ""

# Check contamination before cleanup
echo "📊 Before cleanup:"
echo -n "   ETH Bitcoin trades: "
sqlite3 "$DRADIS_DIR/eth-dradis.db" "SELECT COUNT(*) FROM trades WHERE market LIKE '%Bitcoin%';"
echo -n "   SOL Bitcoin trades: "
sqlite3 "$DRADIS_DIR/sol-dradis.db" "SELECT COUNT(*) FROM trades WHERE market LIKE '%Bitcoin%';"
echo ""

# Delete Bitcoin trades from ETH database
echo "🗑️  Cleaning ETH database..."
sqlite3 "$DRADIS_DIR/eth-dradis.db" <<SQL
DELETE FROM trades WHERE market LIKE '%Bitcoin%';
DELETE FROM entries WHERE market LIKE '%Bitcoin%';
VACUUM;
SQL
echo "✅ ETH database cleaned"

# Delete Bitcoin trades from SOL database
echo "🗑️  Cleaning SOL database..."
sqlite3 "$DRADIS_DIR/sol-dradis.db" <<SQL
DELETE FROM trades WHERE market LIKE '%Bitcoin%';
DELETE FROM entries WHERE market LIKE '%Bitcoin%';
VACUUM;
SQL
echo "✅ SOL database cleaned"
echo ""

# Verify cleanup
echo "📊 After cleanup:"
echo -n "   ETH Bitcoin trades: "
sqlite3 "$DRADIS_DIR/eth-dradis.db" "SELECT COUNT(*) FROM trades WHERE market LIKE '%Bitcoin%';"
echo -n "   SOL Bitcoin trades: "
sqlite3 "$DRADIS_DIR/sol-dradis.db" "SELECT COUNT(*) FROM trades WHERE market LIKE '%Bitcoin%';"
echo ""

# Also clean ETH/SOL trades from BTC database if any
echo "🔍 Checking for reverse contamination in BTC database..."
ETH_IN_BTC=$(sqlite3 "$DRADIS_DIR/btc-dradis.db" "SELECT COUNT(*) FROM trades WHERE market LIKE '%Ethereum%' OR market LIKE '%ETH%';")
SOL_IN_BTC=$(sqlite3 "$DRADIS_DIR/btc-dradis.db" "SELECT COUNT(*) FROM trades WHERE market LIKE '%Solana%' OR market LIKE '%SOL%';")

if [ "$ETH_IN_BTC" -gt 0 ] || [ "$SOL_IN_BTC" -gt 0 ]; then
    echo "⚠️  Found reverse contamination - cleaning BTC database..."
    cp "$DRADIS_DIR/btc-dradis.db" "$DRADIS_DIR/btc-dradis.db.backup-$(date +%Y%m%d-%H%M%S)"
    sqlite3 "$DRADIS_DIR/btc-dradis.db" <<SQL
DELETE FROM trades WHERE market LIKE '%Ethereum%' OR market LIKE '%ETH%';
DELETE FROM trades WHERE market LIKE '%Solana%' OR market LIKE '%SOL%';
DELETE FROM entries WHERE market LIKE '%Ethereum%' OR market LIKE '%ETH%';
DELETE FROM entries WHERE market LIKE '%Solana%' OR market LIKE '%SOL%';
VACUUM;
SQL
    echo "✅ BTC database cleaned"
else
    echo "✅ BTC database is clean (no reverse contamination)"
fi

echo ""
echo "✅ Database cleanup complete!"
echo "   Backups saved in $DRADIS_DIR/*.backup-*"
echo ""
echo "Next steps:"
echo "  1. Deploy the fixed code with ./deploy-live.sh"
echo "  2. Monitor logs to ensure no new contamination occurs"

