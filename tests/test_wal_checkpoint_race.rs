// Test to reproduce WAL checkpoint race condition (Issue #3)
//
// The race:
// 1. TX starts, TxBTree writes pages to pager cache (for "immediate availability")
// 2. Pages are marked dirty in cache
// 3. Checkpoint runs: flushes ALL dirty pages (including uncommitted TX pages)
// 4. Checkpoint truncates WAL
// 5. Crash happens before TX commits
// 6. On recovery: disk has uncommitted changes, WAL has no record of them
//
// This violates ACID durability - uncommitted data is persisted.

use std::sync::{Arc, Barrier, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::Duration;
use std::fs;
use jasonisnthappy::core::database::Database;

/// Test that demonstrates uncommitted data can be flushed by checkpoint
#[test]
fn test_checkpoint_flushes_uncommitted_data() {
    let test_path = "/tmp/test_wal_checkpoint_race.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();
    let db = Arc::new(db);

    // Setup: create collection
    {
        let mut tx = db.begin().unwrap();
        tx.create_collection("test").unwrap();
        tx.commit().unwrap();
    }

    // Insert some initial data and checkpoint to clear WAL
    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();
        coll.insert(serde_json::json!({"_id": "initial", "value": "committed"})).unwrap();
        tx.commit().unwrap();
    }
    db.checkpoint().unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let uncommitted_flushed = Arc::new(AtomicBool::new(false));

    let db1 = db.clone();
    let b1 = barrier.clone();
    let flag = uncommitted_flushed.clone();

    // Thread 1: Start transaction, insert data, but DON'T commit
    // This will write to pager cache
    let h1 = thread::spawn(move || {
        let mut tx = db1.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        // Insert document - this writes to TxBTree which writes to pager cache
        coll.insert(serde_json::json!({"_id": "uncommitted_doc", "value": "should_not_persist"})).unwrap();

        // Signal that we've written to cache
        b1.wait();

        // Wait a bit to give checkpoint time to run
        thread::sleep(Duration::from_millis(500));

        // Check if our uncommitted data was flushed
        // We do this by NOT committing and letting the transaction drop
        // The transaction should rollback, but if checkpoint flushed our pages...

        // DON'T commit - let transaction drop (implicit rollback)
        drop(tx);

        flag.store(true, Ordering::SeqCst);
    });

    let db2 = db.clone();
    let b2 = barrier.clone();

    // Thread 2: Run checkpoint while TX1 has uncommitted changes in cache
    let h2 = thread::spawn(move || {
        // Wait for TX1 to write to cache
        b2.wait();

        // Small delay to ensure TX1's writes are in cache
        thread::sleep(Duration::from_millis(50));

        // Run checkpoint - this should NOT flush uncommitted data
        // but the current implementation might!
        db2.checkpoint().unwrap();
    });

    h1.join().unwrap();
    h2.join().unwrap();

    // Now verify: the uncommitted document should NOT exist
    // If checkpoint flushed uncommitted data, it might be on disk

    // Close and reopen database to simulate recovery
    drop(db);

    let db = Database::open(test_path).unwrap();
    let mut tx = db.begin().unwrap();

    // Try to find the uncommitted document
    if let Ok(coll) = tx.collection("test") {
        let result = coll.find_by_id("uncommitted_doc");

        match result {
            Ok(doc) => {
                // BUG! Uncommitted data was persisted!
                panic!(
                    "RACE CONDITION DETECTED: Uncommitted document was persisted!\n\
                     Document: {:?}\n\
                     This means checkpoint flushed uncommitted transaction data.",
                    doc
                );
            }
            Err(_) => {
                println!("Good: uncommitted document was not persisted (not found)");
            }
        }
    }

    // Verify initial committed data is still there
    if let Ok(coll) = tx.collection("test") {
        let result = coll.find_by_id("initial");
        match result {
            Ok(_) => println!("Good: initial committed data preserved"),
            Err(e) => panic!("Initial committed data should exist: {:?}", e),
        }
    }

    // Cleanup
    drop(tx);
    drop(db);
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

/// More aggressive test with multiple uncommitted transactions
#[test]
fn test_multiple_uncommitted_during_checkpoint() {
    let test_path = "/tmp/test_multi_uncommitted_checkpoint.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();
    let db = Arc::new(db);

    // Setup
    {
        let mut tx = db.begin().unwrap();
        tx.create_collection("test").unwrap();
        tx.commit().unwrap();
    }

    let num_writers = 4;
    let barrier = Arc::new(Barrier::new(num_writers + 1)); // +1 for checkpoint thread

    let mut handles = vec![];

    // Spawn multiple writer threads that DON'T commit
    for i in 0..num_writers {
        let db_clone = db.clone();
        let b = barrier.clone();

        let h = thread::spawn(move || {
            let mut tx = db_clone.begin().unwrap();
            let mut coll = tx.collection("test").unwrap();

            // Insert uncommitted data
            for j in 0..10 {
                let doc = serde_json::json!({
                    "_id": format!("uncommitted_{}_{}", i, j),
                    "thread": i,
                    "index": j
                });
                let _ = coll.insert(doc);
            }

            // Wait at barrier
            b.wait();

            // Sleep to give checkpoint time
            thread::sleep(Duration::from_millis(200));

            // DON'T commit - let transaction drop
        });

        handles.push(h);
    }

    // Checkpoint thread
    let db_cp = db.clone();
    let b_cp = barrier.clone();
    let cp_handle = thread::spawn(move || {
        // Wait for all writers to have data in cache
        b_cp.wait();

        thread::sleep(Duration::from_millis(50));

        // Checkpoint
        db_cp.checkpoint().unwrap();
    });

    // Wait for all threads
    for h in handles {
        h.join().unwrap();
    }
    cp_handle.join().unwrap();

    // Reopen and check
    drop(db);

    let db = Database::open(test_path).unwrap();
    let mut tx = db.begin().unwrap();
    let coll = tx.collection("test").unwrap();

    let mut leaked_count = 0;
    for i in 0..num_writers {
        for j in 0..10 {
            let doc_id = format!("uncommitted_{}_{}", i, j);
            if coll.find_by_id(&doc_id).is_ok() {
                leaked_count += 1;
            }
        }
    }

    if leaked_count > 0 {
        panic!(
            "RACE CONDITION: {} uncommitted documents were persisted to disk!",
            leaked_count
        );
    }

    println!("Good: no uncommitted documents were persisted");

    // Cleanup
    drop(tx);
    drop(db);
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}
