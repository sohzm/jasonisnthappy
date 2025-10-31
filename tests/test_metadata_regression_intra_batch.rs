// Test for intra-batch metadata regression
// This tests if transactions within the SAME batch can cause metadata regression

use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn test_intra_batch_metadata_regression() {
    // This test tries to create a scenario where multiple transactions
    // commit in the same batch and potentially regress metadata

    let path = "/tmp/test_intra_batch_regression.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    // Setup: Create initial data
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();
        users.insert(json!({"_id": "init", "value": 0})).unwrap();
        tx.commit().unwrap();
    }

    // Start 10 transactions concurrently, all trying to commit at the same time
    // This should batch them together
    let barrier = Arc::new(Barrier::new(10));
    let mut handles = vec![];

    for i in 0..10 {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();

        let handle = thread::spawn(move || {
            let mut tx = db_clone.begin().unwrap();
            let mut users = tx.collection("users").unwrap();

            // Each transaction does some work
            for j in 0..10 {
                users.insert(json!({
                    "_id": format!("tx{}_doc{}", i, j),
                    "tx": i,
                    "doc": j
                })).unwrap();
            }

            // Sync point - all transactions start committing at the same time
            barrier_clone.wait();

            // Commit
            tx.commit()
        });
        handles.push(handle);
    }

    // Collect results
    let mut success_count = 0;
    for handle in handles {
        if handle.join().unwrap().is_ok() {
            success_count += 1;
        }
    }

    println!("Successful commits: {}/10", success_count);

    // Verify all committed data is present
    let mut verify_tx = db.begin().unwrap();
    let users = verify_tx.collection("users").unwrap();

    let mut total_found = 0;
    for i in 0..10 {
        for j in 0..10 {
            let doc_id = format!("tx{}_doc{}", i, j);
            if users.find_by_id(&doc_id).is_ok() {
                total_found += 1;
            } else {
                eprintln!("Missing document: {}", doc_id);
            }
        }
    }

    println!("Documents found: {}/100", total_found);

    // Check: No data should be lost
    // If metadata regression happens, some transactions' data will vanish
    assert_eq!(total_found, 100,
        "METADATA REGRESSION BUG: Lost data in intra-batch commits! Only found {}/100 documents",
        total_found);

    // Cleanup
    db.close().unwrap();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_batch_commit_with_stale_snapshot() {
    // Test scenario where TX1 has a very old snapshot
    // and tries to commit alongside newer transactions

    let path = "/tmp/test_stale_snapshot.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    // Setup
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();
        users.insert(json!({"_id": "init", "value": 0})).unwrap();
        tx.commit().unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = barrier.clone();
    let db_clone = db.clone();
    let db_clone2 = db.clone();

    // TX1: Start early, wait a long time
    let handle1 = thread::spawn(move || {
        let mut tx1 = db_clone.begin().unwrap();
        let mut users1 = tx1.collection("users").unwrap();

        // TX1 reads current state
        let _ = users1.find_by_id("init").unwrap();

        // Signal and wait for TX2 to do lots of work
        barrier_clone.wait();
        thread::sleep(Duration::from_millis(100));

        // Now TX1 does a single insert with a VERY stale snapshot
        users1.insert(json!({"_id": "tx1_late", "value": 1})).unwrap();

        // Try to commit
        tx1.commit()
    });

    // TX2: Do lots of work quickly
    let handle2 = thread::spawn(move || {
        barrier.wait();

        // TX2 does many sequential commits
        for batch in 0..5 {
            let mut tx2 = db_clone2.begin().unwrap();
            let mut users2 = tx2.collection("users").unwrap();

            for i in 0..20 {
                users2.insert(json!({
                    "_id": format!("tx2_batch{}_doc{}", batch, i),
                    "batch": batch,
                    "doc": i
                })).unwrap();
            }

            tx2.commit().unwrap();
        }
    });

    handle2.join().unwrap();
    let tx1_result = handle1.join().unwrap();

    println!("TX1 (stale) commit result: {:?}", tx1_result);

    // Verify all data is present
    let mut verify_tx = db.begin().unwrap();
    let users = verify_tx.collection("users").unwrap();

    // Check TX2's data (5 batches * 20 docs = 100 docs)
    let mut tx2_found = 0;
    for batch in 0..5 {
        for i in 0..20 {
            let doc_id = format!("tx2_batch{}_doc{}", batch, i);
            if users.find_by_id(&doc_id).is_ok() {
                tx2_found += 1;
            }
        }
    }

    println!("TX2 documents found: {}/100", tx2_found);

    assert_eq!(tx2_found, 100,
        "METADATA REGRESSION: TX2 data lost due to stale TX1 commit! Found {}/100",
        tx2_found);

    // If TX1 succeeded, verify its data too
    if tx1_result.is_ok() {
        assert!(users.find_by_id("tx1_late").is_ok(),
            "TX1 committed but its data is missing");
    }

    // Cleanup
    db.close().unwrap();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}
