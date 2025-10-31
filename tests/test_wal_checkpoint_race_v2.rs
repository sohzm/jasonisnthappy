// Test WAL checkpoint race - scenario where COMMITTED data can be lost
//
// The scenario described in the issue:
// 1. TX1 commits: writes version B to WAL, syncs WAL, writes to cache
// 2. Checkpoint starts: reads WAL (has version A from earlier, NOT version B yet due to timing)
// 3. Checkpoint writes version A to disk
// 4. Checkpoint flushes cache (might have version B)
// 5. Crash AFTER checkpoint writes version A but BEFORE version B hits disk
// 6. On recovery: version B is lost
//
// The key is whether checkpoint can read a STALE version of WAL while
// a concurrent transaction has already committed newer data.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use std::fs;
use jasonisnthappy::core::database::Database;

/// Test if committed data can be lost during checkpoint
#[test]
fn test_committed_data_survives_checkpoint() {
    let test_path = "/tmp/test_committed_checkpoint_race.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();
    let db = Arc::new(db);

    // Setup: create collection with initial data
    {
        let mut tx = db.begin().unwrap();
        tx.create_collection("test").unwrap();
        let mut coll = tx.collection("test").unwrap();
        coll.insert(serde_json::json!({"_id": "initial", "version": 0})).unwrap();
        tx.commit().unwrap();
    }

    // Checkpoint to establish baseline
    db.checkpoint().unwrap();

    let iterations = 100;
    let mut data_lost = false;

    for iter in 0..iterations {
        let db1 = db.clone();
        let db2 = db.clone();

        let barrier = Arc::new(Barrier::new(2));
        let b1 = barrier.clone();
        let b2 = barrier.clone();

        let expected_value = iter + 1;

        // Thread 1: Commit a transaction
        let h1 = thread::spawn(move || {
            let mut tx = db1.begin().unwrap();
            let mut coll = tx.collection("test").unwrap();

            // Update document to new version
            let _ = coll.delete_by_id("doc1");
            coll.insert(serde_json::json!({
                "_id": "doc1",
                "version": expected_value,
                "data": format!("value_{}", expected_value)
            })).unwrap();

            // Sync with checkpoint thread
            b1.wait();

            // Commit
            tx.commit().unwrap();
        });

        // Thread 2: Run checkpoint concurrently
        let h2 = thread::spawn(move || {
            // Wait for TX to be ready
            b2.wait();

            // Small random delay to vary timing
            thread::sleep(Duration::from_micros(10));

            // Checkpoint
            let _ = db2.checkpoint();
        });

        h1.join().unwrap();
        h2.join().unwrap();

        // Verify the committed data is visible
        {
            let mut tx = db.begin().unwrap();
            let coll = tx.collection("test").unwrap();

            match coll.find_by_id("doc1") {
                Ok(doc) => {
                    let version = doc.get("version").and_then(|v| v.as_i64()).unwrap_or(-1);
                    if version != expected_value as i64 {
                        println!("Iteration {}: Expected version {}, got {}", iter, expected_value, version);
                        data_lost = true;
                    }
                }
                Err(e) => {
                    println!("Iteration {}: Document not found: {:?}", iter, e);
                    data_lost = true;
                }
            }
        }
    }

    // Final verification after all iterations
    {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("test").unwrap();

        match coll.find_by_id("doc1") {
            Ok(doc) => {
                let version = doc.get("version").and_then(|v| v.as_i64()).unwrap_or(-1);
                println!("Final version: {} (expected {})", version, iterations);
                if version != iterations as i64 {
                    data_lost = true;
                }
            }
            Err(_) => {
                data_lost = true;
            }
        }
    }

    // Cleanup
    drop(db);

    // Reopen to verify persistence
    let db = Database::open(test_path).unwrap();
    {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("test").unwrap();

        match coll.find_by_id("doc1") {
            Ok(doc) => {
                let version = doc.get("version").and_then(|v| v.as_i64()).unwrap_or(-1);
                println!("After reopen version: {} (expected {})", version, iterations);
                if version != iterations as i64 {
                    panic!("DATA LOSS AFTER REOPEN: Expected version {}, got {}", iterations, version);
                }
            }
            Err(e) => {
                panic!("DATA LOSS AFTER REOPEN: Document not found: {:?}", e);
            }
        }
    }

    assert!(!data_lost, "Data was lost during concurrent checkpoint operations");

    println!("Test passed: all committed data survived {} checkpoint operations", iterations);

    // Cleanup
    drop(db);
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

/// Stress test with many concurrent writers and checkpoints
#[test]
fn test_checkpoint_stress() {
    let test_path = "/tmp/test_checkpoint_stress.db";
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
    let ops_per_writer = 50;
    let checkpoint_interval_ms = 20;

    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let running_cp = running.clone();

    // Checkpoint thread
    let db_cp = db.clone();
    let cp_handle = thread::spawn(move || {
        let mut checkpoints = 0;
        while running_cp.load(std::sync::atomic::Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(checkpoint_interval_ms));
            let _ = db_cp.checkpoint();
            checkpoints += 1;
        }
        checkpoints
    });

    // Writer threads
    let mut handles = vec![];
    for writer_id in 0..num_writers {
        let db_w = db.clone();
        let h = thread::spawn(move || {
            for op in 0..ops_per_writer {
                let mut tx = db_w.begin().unwrap();
                let mut coll = tx.collection("test").unwrap();

                let doc_id = format!("doc_{}_{}", writer_id, op);
                coll.insert(serde_json::json!({
                    "_id": doc_id,
                    "writer": writer_id,
                    "op": op,
                    "timestamp": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64
                })).unwrap();

                tx.commit().unwrap();
            }
        });
        handles.push(h);
    }

    // Wait for writers to finish
    for h in handles {
        h.join().unwrap();
    }

    // Stop checkpoint thread
    running.store(false, std::sync::atomic::Ordering::SeqCst);
    let checkpoint_count = cp_handle.join().unwrap();
    println!("Performed {} checkpoints during stress test", checkpoint_count);

    // Final checkpoint
    db.checkpoint().unwrap();

    // Verify all data is present
    let mut missing = 0;
    {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("test").unwrap();

        for writer_id in 0..num_writers {
            for op in 0..ops_per_writer {
                let doc_id = format!("doc_{}_{}", writer_id, op);
                if coll.find_by_id(&doc_id).is_err() {
                    missing += 1;
                    println!("Missing: {}", doc_id);
                }
            }
        }
    }

    // Reopen and verify
    drop(db);
    let db = Database::open(test_path).unwrap();

    let mut missing_after_reopen = 0;
    {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("test").unwrap();

        for writer_id in 0..num_writers {
            for op in 0..ops_per_writer {
                let doc_id = format!("doc_{}_{}", writer_id, op);
                if coll.find_by_id(&doc_id).is_err() {
                    missing_after_reopen += 1;
                }
            }
        }
    }

    // Cleanup
    drop(db);
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    assert_eq!(missing, 0, "Missing {} documents before reopen", missing);
    assert_eq!(missing_after_reopen, 0, "Missing {} documents after reopen", missing_after_reopen);

    println!("Stress test passed: all {} documents preserved", num_writers * ops_per_writer);
}
