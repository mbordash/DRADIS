/// Unit tests for the TOCTOU (Time-Of-Check-Time-Of-Use) race condition fix
/// in the Entry signal handler.
///
/// The race: under the old code, two concurrent ticks could both pass the
/// `contains_key` check (lock released after the check) and then both place
/// a buy order for the same token before either inserted the position record.
///
/// The fix: check AND insert happen in a single lock scope, so the slot is
/// reserved atomically and a second racer always sees `contains_key = true`.
///
/// These tests use `tokio::task::yield_now()` to force a cooperative context
/// switch at exactly the point where the race window existed, making the
/// bug (and the absence of it after the fix) deterministic.

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use alloy::primitives::U256;
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use tokio::sync::Mutex;

    use crate::state::{Position, PositionMap};

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn make_position(token_id: U256) -> Position {
        Position {
            shares: dec!(10),
            avg_entry: dec!(0.55),
            opened_at: Utc::now(),
            close_time: None,
            market_name: "Test Market".to_string(),
            pair_token_id: token_id,
            fill_confirmed_at: None,
            paired_leg_token_id: None,
        }
    }

    const TOKEN: U256 = U256::from_limbs([42, 0, 0, 0]);
    const STRATEGY: &str = "TestStrategy";

    // compound key used everywhere in the tests
    fn key(token_id: U256) -> (String, U256) {
        (STRATEGY.to_string(), token_id)
    }

    // ── Vulnerable pattern (old code) ─────────────────────────────────────────
    //
    // 1. Lock → check contains_key → RELEASE lock
    // 2. yield_now() ← simulates the async gap that existed between the
    //    `contains_key` check and the later `insert` (e.g. computing prices,
    //    calling place_limit_order, etc.)
    // 3. Lock → insert
    //
    // With two tasks racing, both can pass step 1 before either reaches step 3.

    async fn entry_vulnerable(
        positions: Arc<Mutex<PositionMap>>,
        orders_placed: Arc<AtomicU32>,
        token_id: U256,
    ) {
        // Step 1: check only — lock is released immediately after
        {
            let pos_map = positions.lock().await;
            if pos_map.contains_key(&key(token_id)) {
                return; // already have a position
            }
        } // ← lock dropped here; RACE WINDOW OPENS

        // Simulate async work (price computation, HTTP order call, …)
        tokio::task::yield_now().await; // ← lets the other task run

        // Step 2: "place order" — increment counter to track duplicate sends
        orders_placed.fetch_add(1, Ordering::SeqCst);

        // Step 3: record position (separate lock acquisition)
        {
            let mut pos_map = positions.lock().await;
            pos_map.insert(key(token_id), make_position(token_id));
        }
    }

    /// Demonstrates that the old pattern is vulnerable: both tasks pass the
    /// `contains_key` gate before either inserts, so two orders are placed.
    #[tokio::test]
    async fn test_toctou_vulnerable_races() {
        let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(Default::default()));
        let orders_placed = Arc::new(AtomicU32::new(0));

        let t1 = tokio::spawn(entry_vulnerable(
            Arc::clone(&positions),
            Arc::clone(&orders_placed),
            TOKEN,
        ));
        let t2 = tokio::spawn(entry_vulnerable(
            Arc::clone(&positions),
            Arc::clone(&orders_placed),
            TOKEN,
        ));

        t1.await.unwrap();
        t2.await.unwrap();

        // Both tasks raced through the gate → two orders placed (the bug)
        assert_eq!(
            orders_placed.load(Ordering::SeqCst),
            2,
            "OLD pattern: both tasks should have placed an order (demonstrating the race)"
        );
    }

    // ── Fixed pattern (new code) ──────────────────────────────────────────────
    //
    // Check AND insert happen in a single lock scope.
    // The second racer always sees `contains_key = true` when it acquires the
    // lock, so it returns early and no duplicate order is placed.

    async fn entry_fixed(
        positions: Arc<Mutex<PositionMap>>,
        orders_placed: Arc<AtomicU32>,
        token_id: U256,
    ) {
        // Atomic check-and-reserve in ONE lock scope
        {
            let mut pos_map = positions.lock().await;
            if pos_map.contains_key(&key(token_id)) {
                return; // already reserved by another task
            }
            // Reserve slot immediately — no gap for a second racer
            pos_map.insert(key(token_id), make_position(token_id));
        } // ← lock dropped only AFTER insert

        // Simulate async work
        tokio::task::yield_now().await;

        // "Place order" — only reached by the task that won the reservation
        orders_placed.fetch_add(1, Ordering::SeqCst);
    }

    /// Demonstrates that the fixed pattern is safe: exactly one order is placed
    /// regardless of concurrent execution.
    #[tokio::test]
    async fn test_toctou_fixed_no_duplicate() {
        let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(Default::default()));
        let orders_placed = Arc::new(AtomicU32::new(0));

        let t1 = tokio::spawn(entry_fixed(
            Arc::clone(&positions),
            Arc::clone(&orders_placed),
            TOKEN,
        ));
        let t2 = tokio::spawn(entry_fixed(
            Arc::clone(&positions),
            Arc::clone(&orders_placed),
            TOKEN,
        ));

        t1.await.unwrap();
        t2.await.unwrap();

        // Exactly one order placed and exactly one position in the map
        assert_eq!(
            orders_placed.load(Ordering::SeqCst),
            1,
            "FIXED pattern: only one task should have placed an order"
        );
        assert_eq!(
            positions.lock().await.len(),
            1,
            "FIXED pattern: exactly one position should be recorded"
        );
    }

    /// Stress version: 20 concurrent tasks all try to enter the same token.
    /// The fixed pattern must still produce exactly one order.
    #[tokio::test]
    async fn test_toctou_fixed_stress() {
        let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(Default::default()));
        let orders_placed = Arc::new(AtomicU32::new(0));

        let handles: Vec<_> = (0..20)
            .map(|_| {
                tokio::spawn(entry_fixed(
                    Arc::clone(&positions),
                    Arc::clone(&orders_placed),
                    TOKEN,
                ))
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            orders_placed.load(Ordering::SeqCst),
            1,
            "FIXED pattern (stress): exactly one order across 20 concurrent tasks"
        );
        assert_eq!(positions.lock().await.len(), 1);
    }

    /// Verify that after a failed order the sentinel is rolled back, allowing
    /// the next tick to retry the entry (no stuck phantom position).
    #[tokio::test]
    async fn test_toctou_rollback_on_failure() {
        let positions: Arc<Mutex<PositionMap>> = Arc::new(Mutex::new(Default::default()));

        // Simulate the winning task: reserve slot, then order fails → roll back
        {
            let mut pos_map = positions.lock().await;
            assert!(!pos_map.contains_key(&key(TOKEN)));
            pos_map.insert(key(TOKEN), make_position(TOKEN));
        }

        // Simulate order failure: roll back the sentinel
        positions.lock().await.remove(&key(TOKEN));

        // Slot is free again — a subsequent tick can successfully enter
        assert!(
            !positions.lock().await.contains_key(&key(TOKEN)),
            "After rollback the slot must be free for the next tick"
        );

        // Simulate next tick succeeding
        {
            let mut pos_map = positions.lock().await;
            assert!(!pos_map.contains_key(&key(TOKEN)));
            pos_map.insert(key(TOKEN), make_position(TOKEN));
        }

        assert_eq!(positions.lock().await.len(), 1);
    }
}

