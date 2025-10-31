// DEVELOPER DEBUGGING GUIDE: How to manually reproduce deadlock scenarios
//
// If you need to investigate potential deadlocks in the locking system, you can
// temporarily modify the source code to inject delays and force a deadlock:
//
// Step 1: In transaction.rs drop_collection(), after acquiring version_chains lock:
//     let mut chains = db.version_chains.write().unwrap();
//     // ADD THIS: std::thread::sleep(std::time::Duration::from_secs(2));
//     chains.remove(name);
//
// Step 2: Run two threads:
//     - Thread A: drop_collection (holds version_chains, sleeps 2s)
//     - Thread B: commit (tries to get commit_mu, then version_chains)
//
// This will cause:
//     Thread B gets commit_mu while A sleeps with version_chains,
//     then B blocks waiting for version_chains -> deadlock if A tries to get commit_mu.
//
// The test below (test_actual_concurrent_scenario) provides a real test that
// detects actual deadlocks without code modification.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use std::fs;
use jasonisnthappy::core::database::Database;

#[test]
fn test_actual_concurrent_scenario() {
    // Let's try to create maximum contention without modifying code
    let test_path = "/tmp/test_force_deadlock.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();
    let db = Arc::new(db);

    // Setup
    {
        let mut tx = db.begin().unwrap();
        let _ = tx.create_collection("test");
        let _ = tx.commit();
    }

    let barrier = Arc::new(Barrier::new(2));
    let b1 = barrier.clone();
    let b2 = barrier.clone();

    let db1 = db.clone();
    let db2 = db.clone();

    // Thread 1: Continuously drop/create (holds version_chains briefly)
    let h1 = thread::spawn(move || {
        b1.wait();
        for i in 0..100 {
            let mut tx = db1.begin().unwrap();
            let coll_name = format!("coll_{}", i);
            let _ = tx.create_collection(&coll_name);
            let _ = tx.commit();

            // Immediately try to drop
            let mut tx = db1.begin().unwrap();
            let _ = tx.drop_collection(&coll_name);
            // Don't commit yet - hold the transaction
            thread::sleep(Duration::from_micros(10)); // Tiny delay
            let _ = tx.commit();
        }
    });

    // Thread 2: Continuously commit with version chain updates
    let h2 = thread::spawn(move || {
        b2.wait();
        for i in 0..100 {
            let mut tx = db2.begin().unwrap();
            if let Ok(mut c) = tx.collection("test") {
                let _ = c.insert(serde_json::json!({"_id": format!("doc{}", i), "x": i}));
            }
            // This commit will try to acquire version_chains
            let _ = tx.commit();
            thread::sleep(Duration::from_micros(10));
        }
    });

    // Wait with timeout
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    loop {
        if h1.is_finished() && h2.is_finished() {
            break;
        }
        if start.elapsed() > timeout {
            panic!("POTENTIAL DEADLOCK: Threads didn't complete in {} seconds", timeout.as_secs());
        }
        thread::sleep(Duration::from_millis(100));
    }

    h1.join().unwrap();
    h2.join().unwrap();

    println!("No deadlock detected with maximum contention");
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}
