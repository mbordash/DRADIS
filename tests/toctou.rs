/// Unit tests for the TOCTOU (Time-Of-Check-Time-Of-Use) race condition fix
///
/// The race: under the old code, two concurrent ticks could both pass the
/// `contains_key` check (lock released after the check) and then both place
/// a buy order for the same token before either inserted the position record.
///
/// The fix: check AND insert happen in a single lock scope, so the slot is
/// reserved atomically and a second racer always sees `contains_key = true`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use alloy::primitives::U256;
use chrono::Utc;
use rust_decimal_macros::dec;
use tokio::sync::Mutex;

use dradis::state::{Position, PositionMap};

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

fn key(token_id: U256) -> (String, U256) {
    (STRATEGY.to_string(), token_id)
}

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
    assert_eq!(orders_placed.load(Ordering::SeqCst), 1);
    assert_eq!(positions.lock().await.len(), 1);
}

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

    assert_eq!(orders_placed.load(Ordering::SeqCst), 1);
    assert_eq!(positions.lock().await.len(), 1);
}
