// Test to expose metadata regression bug via page recycling
// This test creates a scenario where page numbers DECREASE due to the free list,
// which should expose the flaw in the `new_root > current_root` check

use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn test_metadata_regression_with_page_recycling() {
    let path = "/tmp/test_page_recycling_regression.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    // Phase 1: Insert many documents to grow the tree and allocate high page numbers
    println!("Phase 1: Inserting documents to grow tree...");
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();

        // Insert 500 documents to allocate many pages
        for i in 0..500 {
            users.insert(json!({
                "_id": format!("initial_{}", i),
                "value": i,
                "padding": "x".repeat(1000) // Make docs large to use more pages
            })).unwrap();
        }
        tx.commit().unwrap();
    }

    println!("Phase 1 complete - tree grown to high page numbers");

    // Phase 2: Delete most documents to populate the free list
    println!("Phase 2: Deleting documents to create free list...");
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();

        // Delete 450 documents, leaving only 50
        // This frees many pages and populates the free list
        for i in 0..450 {
            users.delete_by_id(&format!("initial_{}", i)).unwrap();
        }
        tx.commit().unwrap();
    }

    println!("Phase 2 complete - free list populated with low page numbers");

    // Phase 3: Concurrent transactions where TX2 will reuse freed pages
    println!("Phase 3: Starting concurrent transactions...");

    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = barrier.clone();
    let db_clone = db.clone();
    let db_clone2 = db.clone();

    // TX1: Start with snapshot, do minimal work, try to commit late
    let handle1 = thread::spawn(move || {
        let mut tx1 = db_clone.begin().unwrap();
        let mut users1 = tx1.collection("users").unwrap();

        // Read to establish snapshot at current state
        let _ = users1.find_by_id("initial_451");

        println!("TX1: Snapshot taken");

        // Wait for TX2 to start
        barrier_clone.wait();

        // Give TX2 time to complete its work
        thread::sleep(Duration::from_millis(100));

        println!("TX1: Doing insert with stale snapshot...");

        // TX1 inserts one document
        // This will allocate a NEW high page number (no free list used)
        users1.insert(json!({
            "_id": "tx1_doc",
            "value": 999,
            "padding": "x".repeat(1000)
        })).unwrap();

        println!("TX1: Attempting commit...");
        tx1.commit()
    });

    // TX2: Insert many documents that will reuse freed pages (lower page numbers)
    let handle2 = thread::spawn(move || {
        barrier.wait();

        println!("TX2: Starting work...");
        thread::sleep(Duration::from_millis(10));

        let mut tx2 = db_clone2.begin().unwrap();
        let mut users2 = tx2.collection("users").unwrap();

        // TX2 inserts 100 documents
        // These will reuse pages from the free list (lower page numbers)
        for i in 0..100 {
            users2.insert(json!({
                "_id": format!("tx2_new_{}", i),
                "value": i + 1000,
                "padding": "x".repeat(1000)
            })).unwrap();
        }

        println!("TX2: Committing (will reuse freed pages)...");
        tx2.commit().unwrap();
        println!("TX2: Commit successful");
    });

    // Wait for both transactions
    handle2.join().unwrap();
    let tx1_result = handle1.join().unwrap();

    println!("TX1 commit result: {:?}", tx1_result);

    // Phase 4: Verify data integrity
    println!("Phase 4: Verifying data integrity...");

    let mut verify_tx = db.begin().unwrap();
    let users = verify_tx.collection("users").unwrap();

    // Count surviving initial documents (should be 50: initial_450 to initial_499)
    let mut initial_found = 0;
    for i in 450..500 {
        if users.find_by_id(&format!("initial_{}", i)).is_ok() {
            initial_found += 1;
        }
    }
    println!("Initial documents (450-499) found: {}/50", initial_found);

    // Count TX2's documents - THIS IS THE CRITICAL CHECK
    let mut tx2_found = 0;
    let mut tx2_missing = Vec::new();
    for i in 0..100 {
        let doc_id = format!("tx2_new_{}", i);
        match users.find_by_id(&doc_id) {
            Ok(_) => tx2_found += 1,
            Err(_) => tx2_missing.push(doc_id),
        }
    }

    println!("TX2 documents found: {}/100", tx2_found);
    if !tx2_missing.is_empty() {
        println!("TX2 missing documents: {:?}", &tx2_missing[..tx2_missing.len().min(10)]);
    }

    // Check TX1's document if it committed
    if tx1_result.is_ok() {
        match users.find_by_id("tx1_doc") {
            Ok(_) => println!("TX1 document found"),
            Err(_) => println!("WARNING: TX1 committed but document missing!"),
        }
    }

    let total_count = users.count().unwrap();
    println!("Total documents in collection: {}", total_count);

    // THE BUG: If new_root > current_root fails when current_root uses recycled pages,
    // TX1 will regress the root pointer and TX2's data will be lost
    assert_eq!(tx2_found, 100,
        "🔥 METADATA REGRESSION BUG DETECTED! TX2's data was lost due to page recycling. \
         Found {}/100 documents. The bug manifested because TX1's new_root > current_root \
         check failed when current_root used a lower recycled page number.",
        tx2_found);

    assert_eq!(initial_found, 50,
        "Initial documents were lost! Found {}/50", initial_found);

    // Cleanup
    db.close().unwrap();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_page_recycling_basic() {
    // Simpler test to verify page recycling happens
    let path = "/tmp/test_page_recycling_basic.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();

    // Insert documents
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();

        for i in 0..100 {
            users.insert(json!({
                "_id": format!("doc_{}", i),
                "data": "x".repeat(500)
            })).unwrap();
        }
        tx.commit().unwrap();
    }

    // Delete most documents
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();

        for i in 0..80 {
            users.delete_by_id(&format!("doc_{}", i)).unwrap();
        }
        tx.commit().unwrap();
    }

    // Insert new documents - should reuse freed pages
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();

        for i in 0..50 {
            users.insert(json!({
                "_id": format!("new_{}", i),
                "data": "y".repeat(500)
            })).unwrap();
        }
        tx.commit().unwrap();
    }

    // Verify all surviving data is present
    {
        let mut tx = db.begin().unwrap();
        let users = tx.collection("users").unwrap();

        // Check old survivors
        for i in 80..100 {
            assert!(users.find_by_id(&format!("doc_{}", i)).is_ok(),
                "Old document doc_{} should exist", i);
        }

        // Check new documents
        for i in 0..50 {
            assert!(users.find_by_id(&format!("new_{}", i)).is_ok(),
                "New document new_{} should exist", i);
        }

        // Note: count() may include MVCC tombstones, so we verify by checking
        // that all expected documents are accessible instead of exact count
        println!("Total count (may include MVCC tombstones): {}", users.count().unwrap());
    }

    // Cleanup
    db.close().unwrap();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}
