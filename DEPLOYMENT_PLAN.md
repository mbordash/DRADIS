# DRADIS Production Fix Deployment Plan

## Problem Summary

**Root Cause:** The `detect_orphaned_arb_settlements` function was scanning ALL asset pools (btc, eth, sol) from every squadron, causing Bitcoin trades to be written to all databases multiple times.

**Current State:**
- ✅ BTC database: 1 Bitcoin trade (correct)
- ❌ ETH database: 37 Bitcoin trades (contaminated)  
- ❌ SOL database: 2 Bitcoin trades (contaminated)
- ❌ Production is still running OLD buggy code
- ✅ Fix is committed and pushed to GitHub

## Solution

We have TWO problems to solve:
1. **Stop new contamination** → Deploy the fix
2. **Clean existing contamination** → Run cleanup script

## Deployment Steps

### Step 1: Deploy the Fixed Code

```bash
# On your local machine
cd /Users/mbordash/DRADIS
./deploy-live.sh
```

This will:
- Copy the fixed code to production
- Rebuild Docker containers with the fix
- Restart all squadrons (BTC, ETH, SOL)

**Expected time:** 5-7 minutes

### Step 2: Verify Fix is Active

```bash
# SSH into production
ssh -i ~/.ssh/rustpolybot-ireland-key-2026.pem ubuntu@52.211.208.155

# Check container logs to see the fix is running
docker logs dradis-live --tail 50 | grep "Orphan detection"
```

You should see logs like:
```
🔍 Orphan detection [BTC]: Found auto-settled arbitrage...
🔍 Orphan detection [ETH]: Found auto-settled arbitrage...
🔍 Orphan detection [SOL]: Found auto-settled arbitrage...
```

Each squadron should only log for its OWN asset.

### Step 3: Clean Up Contaminated Databases

```bash
# Still SSH'd into production
cd ~/dradis
curl -O https://raw.githubusercontent.com/mbordash/DRADIS/main/tools/cleanup-contaminated-trades.sh
chmod +x cleanup-contaminated-trades.sh

# Stop containers before cleaning databases
docker stop dradis-live

# Run cleanup
./cleanup-contaminated-trades.sh logs

# Restart containers
docker start dradis-live
```

**Alternative:** If you don't want to download the script, you can manually clean:

```bash
# Backup first
cd ~/dradis/logs
cp eth-dradis.db eth-dradis.db.backup-$(date +%Y%m%d-%H%M%S)
cp sol-dradis.db sol-dradis.db.backup-$(date +%Y%m%d-%H%M%S)

# Clean ETH database
sqlite3 eth-dradis.db "DELETE FROM trades WHERE market LIKE '%Bitcoin%'; DELETE FROM entries WHERE market LIKE '%Bitcoin%'; VACUUM;"

# Clean SOL database  
sqlite3 sol-dradis.db "DELETE FROM trades WHERE market LIKE '%Bitcoin%'; DELETE FROM entries WHERE market LIKE '%Bitcoin%'; VACUUM;"

# Verify cleanup
echo "ETH Bitcoin trades:" && sqlite3 eth-dradis.db "SELECT COUNT(*) FROM trades WHERE market LIKE '%Bitcoin%';"
echo "SOL Bitcoin trades:" && sqlite3 sol-dradis.db "SELECT COUNT(*) FROM trades WHERE market LIKE '%Bitcoin%';"
```

Should both show `0`.

### Step 4: Verify Everything is Working

1. **Check Control Tower UI** at http://52.211.208.155:3002
   - Portfolio value should show ~$81 (not $242)
   - Chart should show buy/sell markers
   - Each squadron page should only show its own asset trades

2. **Check Database Integrity**
   ```bash
   # SSH into production
   ssh -i ~/.ssh/rustpolybot-ireland-key-2026.pem ubuntu@52.211.208.155
   
   cd ~/dradis/logs
   
   # Count trades in each database
   echo "BTC trades:" && sqlite3 btc-dradis.db "SELECT COUNT(*), market FROM trades GROUP BY market;"
   echo "ETH trades:" && sqlite3 eth-dradis.db "SELECT COUNT(*), market FROM trades GROUP BY market;"
   echo "SOL trades:" && sqlite3 sol-dradis.db "SELECT COUNT(*), market FROM trades GROUP BY market;"
   ```

3. **Monitor for 30 minutes** to ensure no new contamination occurs

## Recovery Plan (If Something Goes Wrong)

The cleanup script creates backups automatically. To restore:

```bash
cd ~/dradis/logs
# Find the backup file (newest one)
ls -lt *.backup-* | head -3

# Restore ETH
cp eth-dradis.db.backup-YYYYMMDD-HHMMSS eth-dradis.db

# Restore SOL
cp sol-dradis.db.backup-YYYYMMDD-HHMMSS sol-dradis.db

# Restart
docker restart dradis-live
```

## Nuclear Option: Complete Database Reset

If you want to start completely fresh:

```bash
# SSH into production
ssh -i ~/.ssh/rustpolybot-ireland-key-2026.pem ubuntu@52.211.208.155

# Stop containers
docker stop dradis-live

# Backup everything
cd ~/dradis/logs
mkdir -p backup-$(date +%Y%m%d-%H%M%S)
mv *.db backup-*/

# Restart - DRADIS will create fresh databases
docker start dradis-live
```

**Warning:** This will delete ALL historical trade data and session history.

## Expected Results After Fix

1. ✅ Portfolio shows correct value (~$81)
2. ✅ Chart markers visible for buy/sell events  
3. ✅ BTC squadron only shows Bitcoin trades
4. ✅ ETH squadron only shows Ethereum trades (when they occur)
5. ✅ SOL squadron only shows Solana trades (when they occur)
6. ✅ No cross-contamination in future trades

## Files Changed in This Fix

- `src/tasks/cleanup.rs` - Asset-scoped orphan detection
- `src/squadron/patrol_tasks.rs` - Pass asset to settlement task
- `src/squadron/patrol_impl.rs` - Connect asset parameter
- `control-tower/src/app/api/portfolio/route.ts` - Fix collateral counting
- `control-tower/src/app/page.tsx` - Aggregate markers from all assets
- `control-tower/src/components/*.tsx` - UI improvements

All changes committed in: `d971650`

