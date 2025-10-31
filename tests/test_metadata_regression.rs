// Test to reproduce Issue #1: Metadata Regression Bug
//
// Scenario:
// - TX1 snapshots root=X, does a minimal operation
// - TX2 commits, updates root X -> Y (where Y >> X due to many inserts)
// - TX1 commits - should NOT regress root back to X or lose TX2's data
//
// Expected: TX1 either succeeds with correct root, or fails with conflict
// Bug: TX1 succeeds but regresses root, losing TX2's data

use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn test_metadata_regression_concurrent_commits() {
    let path = "/tmp/test_metadata_regression.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    // Setup: Insert one document so collection exists with root > 0
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();
        users.insert(json!({"_id": "setup", "name": "Setup Doc"})).unwrap();
        tx.commit().unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = barrier.clone();
    let db_clone = db.clone();
    let db_clone2 = db.clone();

    // Thread 1 (TX1): Start transaction, wait, then do minimal work and commit
    let handle1 = thread::spawn(move || {
        // Start TX1 and take snapshot
        let mut tx1 = db_clone.begin().unwrap();
        let mut users1 = tx1.collection("users").unwrap();

        // Do a simple read to establish snapshot
        let _ = users1.find_by_id("setup").unwrap();

        // Signal that TX1 has started and wait for TX2 to commit
        barrier_clone.wait();

        // Small delay to ensure TX2 commits first
        thread::sleep(Duration::from_millis(50));

        // TX1 does a minimal write (single insert)
        users1.insert(json!({"_id": "tx1_doc", "name": "TX1 Document"})).unwrap();

        // Try to commit TX1
        let result = tx1.commit();
        result
    });

    // Thread 2 (TX2): Wait for TX1 to start, then do many inserts and commit
    let handle2 = thread::spawn(move || {
        // Wait for TX1 to start
        barrier.wait();

        // Small delay to ensure TX1 has taken its snapshot
        thread::sleep(Duration::from_millis(10));

        // TX2 does many inserts to significantly advance the root pointer
        let mut tx2 = db_clone2.begin().unwrap();
        let mut users2 = tx2.collection("users").unwrap();

        for i in 0..100 {
            users2.insert(json!({
                "_id": format!("tx2_doc_{}", i),
                "name": format!("TX2 Document {}", i),
                "index": i
            })).unwrap();
        }

        // Commit TX2 - this should advance the root pointer significantly
        tx2.commit().unwrap();
    });

    // Wait for both threads
    handle2.join().unwrap();
    let tx1_result = handle1.join().unwrap();

    println!("TX1 commit result: {:?}", tx1_result);

    // Now verify the data integrity
    let mut verify_tx = db.begin().unwrap();
    let users = verify_tx.collection("users").unwrap();

    // Check that all TX2 documents are present
    let mut tx2_docs_found = 0;
    for i in 0..100 {
        let doc_id = format!("tx2_doc_{}", i);
        match users.find_by_id(&doc_id) {
            Ok(doc) => {
                assert_eq!(doc["name"], format!("TX2 Document {}", i));
                tx2_docs_found += 1;
            }
            Err(e) => {
                eprintln!("ERROR: TX2 document {} not found: {:?}", doc_id, e);
            }
        }
    }

    println!("TX2 documents found: {}/100", tx2_docs_found);

    // The critical check: TX2's data must NOT be lost
    assert_eq!(tx2_docs_found, 100,
        "METADATA REGRESSION BUG: TX2's documents were lost! Only found {}/100",
        tx2_docs_found);

    // If TX1 succeeded, its document should also be present
    if tx1_result.is_ok() {
        let tx1_doc = users.find_by_id("tx1_doc");
        assert!(tx1_doc.is_ok(), "TX1 committed successfully but its document is missing");
    }

    // Count total documents
    let total_count = users.count().unwrap();
    println!("Total documents in collection: {}", total_count);

    // Cleanup
    db.close().unwrap();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_metadata_regression_no_doc_writes() {
    // Test the specific scenario from the bug description:
    // TX1 does a query-only operation (no doc writes)
    let path = "/tmp/test_metadata_regression_no_writes.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    // Setup
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();
        users.insert(json!({"_id": "setup", "value": 0})).unwrap();
        tx.commit().unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = barrier.clone();
    let db_clone = db.clone();
    let db_clone2 = db.clone();

    // TX1: Query-only transaction (no writes)
    let handle1 = thread::spawn(move || {
        let mut tx1 = db_clone.begin().unwrap();
        let users1 = tx1.collection("users").unwrap();

        // Just read, no writes
        let _ = users1.find_by_id("setup").unwrap();

        barrier_clone.wait();
        thread::sleep(Duration::from_millis(50));

        // Try to commit with no writes
        tx1.commit()
    });

    // TX2: Write many documents
    let handle2 = thread::spawn(move || {
        barrier.wait();
        thread::sleep(Duration::from_millis(10));

        let mut tx2 = db_clone2.begin().unwrap();
        let mut users2 = tx2.collection("users").unwrap();

        for i in 0..50 {
            users2.insert(json!({
                "_id": format!("doc_{}", i),
                "value": i
            })).unwrap();
        }

        tx2.commit().unwrap();
    });

    handle2.join().unwrap();
    let _ = handle1.join().unwrap();

    // Verify TX2's data is intact
    let mut verify_tx = db.begin().unwrap();
    let users = verify_tx.collection("users").unwrap();

    let mut found = 0;
    for i in 0..50 {
        if users.find_by_id(&format!("doc_{}", i)).is_ok() {
            found += 1;
        }
    }

    assert_eq!(found, 50,
        "METADATA REGRESSION BUG: TX2's documents were lost! Only found {}/50",
        found);

    // Cleanup
    db.close().unwrap();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_metadata_regression_update_operation() {
    // Test with update operations instead of inserts
    let path = "/tmp/test_metadata_regression_update.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    // Setup: Create some initial documents
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();
        for i in 0..10 {
            users.insert(json!({"_id": format!("user_{}", i), "value": 0})).unwrap();
        }
        tx.commit().unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = barrier.clone();
    let db_clone = db.clone();
    let db_clone2 = db.clone();

    // TX1: Update one document
    let handle1 = thread::spawn(move || {
        let mut tx1 = db_clone.begin().unwrap();
        let mut users1 = tx1.collection("users").unwrap();

        // Read to establish snapshot
        let _ = users1.find_by_id("user_0").unwrap();

        barrier_clone.wait();
        thread::sleep(Duration::from_millis(50));

        // Update one document
        users1.update_by_id("user_0", json!({"_id": "user_0", "value": 999})).unwrap();

        tx1.commit()
    });

    // TX2: Insert many new documents
    let handle2 = thread::spawn(move || {
        barrier.wait();
        thread::sleep(Duration::from_millis(10));

        let mut tx2 = db_clone2.begin().unwrap();
        let mut users2 = tx2.collection("users").unwrap();

        for i in 100..200 {
            users2.insert(json!({
                "_id": format!("new_user_{}", i),
                "value": i
            })).unwrap();
        }

        tx2.commit().unwrap();
    });

    handle2.join().unwrap();
    let _ = handle1.join().unwrap();

    // Verify both TX1 and TX2's changes are present
    let mut verify_tx = db.begin().unwrap();
    let users = verify_tx.collection("users").unwrap();

    // Check TX2's new documents
    let mut tx2_found = 0;
    for i in 100..200 {
        if users.find_by_id(&format!("new_user_{}", i)).is_ok() {
            tx2_found += 1;
        }
    }

    assert_eq!(tx2_found, 100,
        "METADATA REGRESSION BUG: TX2's new documents were lost! Only found {}/100",
        tx2_found);

    // Cleanup
    db.close().unwrap();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}
