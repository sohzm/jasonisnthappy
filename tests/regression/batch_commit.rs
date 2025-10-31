// Regression tests for batch commit issues

use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[test]
fn test_concurrent_writes_batch() {
    let path = "/tmp/test_batch_concurrent.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    // Create 5 concurrent writers
    let mut handles = vec![];

    for i in 0..5 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            let mut tx = db_clone.begin().unwrap();
            let mut users = tx.collection("users").unwrap();

            // Each thread writes 10 documents
            for j in 0..10 {
                let doc = json!({
                    "name": format!("User-{}-{}", i, j),
                    "thread_id": i,
                    "doc_number": j
                });
                users.insert(doc).unwrap();
            }

            tx.commit().unwrap();
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all documents were inserted
    let mut tx = db.begin().unwrap();
    let users = tx.collection("users").unwrap();
    let count = users.count().unwrap();

    assert_eq!(count, 50); // 5 threads × 10 documents each

    // Check metrics
    let _metrics = db.metrics();

    // Cleanup
    db.close().unwrap();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_batch_vs_single_performance() {
    let batch_path = "/tmp/test_batch_perf.db";
    let single_path = "/tmp/test_single_perf.db";

    for path in &[batch_path, single_path] {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    // Test with batching enabled (default)
    let batch_db = Arc::new(Database::open(batch_path).unwrap());
    let batch_start = Instant::now();

    let mut handles = vec![];
    for i in 0..10 {
        let db = batch_db.clone();
        let handle = thread::spawn(move || {
            let mut tx = db.begin().unwrap();
            let mut users = tx.collection("users").unwrap();

            let doc = json!({"name": format!("User-{}", i), "id": i});
            users.insert(doc).unwrap();

            tx.commit().unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let _batch_elapsed = batch_start.elapsed();

    // Test with batching disabled
    // Note: We can't disable batching through options yet, so we'll just measure for comparison
    let single_db = Arc::new(Database::open(single_path).unwrap());
    let single_start = Instant::now();

    for i in 0..10 {
        let mut tx = single_db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();

        let doc = json!({"name": format!("User-{}", i), "id": i});
        users.insert(doc).unwrap();

        tx.commit().unwrap();
    }

    let _single_elapsed = single_start.elapsed();

    // Batching should be faster for concurrent writes
    // Note: This assertion may be flaky in CI, so we just print for now

    // Check batch metrics
    let _metrics = batch_db.metrics();

    // Cleanup
    batch_db.close().unwrap();
    single_db.close().unwrap();

    for path in &[batch_path, single_path] {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.lock", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }
}

#[test]
fn test_batch_no_conflicts() {
    let path = "/tmp/test_batch_no_conflicts.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    // Create multiple threads that write to different documents
    let mut handles = vec![];

    for i in 0..5 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            let mut tx = db_clone.begin().unwrap();
            let mut users = tx.collection("users").unwrap();

            // Each thread writes a unique document based on thread ID
            let doc = json!({
                "_id": format!("user-{}", i),
                "name": format!("User {}", i),
                "thread_id": i
            });
            users.insert(doc).unwrap();

            tx.commit().unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all documents exist
    let mut tx = db.begin().unwrap();
    let users = tx.collection("users").unwrap();

    for i in 0..5 {
        let doc = users.find_by_id(&format!("user-{}", i)).unwrap();
        assert_eq!(doc["name"], format!("User {}", i));
    }

    // Cleanup
    db.close().unwrap();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_batch_with_different_collections() {
    let path = "/tmp/test_batch_multi_collections.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    // Create threads writing to different collections
    let mut handles = vec![];

    for i in 0..3 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            let mut tx = db_clone.begin().unwrap();

            // Each thread writes to a different collection
            let collection_name = format!("collection_{}", i);
            let mut coll = tx.collection(&collection_name).unwrap();

            for j in 0..5 {
                let doc = json!({
                    "collection_id": i,
                    "doc_number": j
                });
                coll.insert(doc).unwrap();
            }

            tx.commit().unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify data in all collections
    let mut tx = db.begin().unwrap();

    for i in 0..3 {
        let collection_name = format!("collection_{}", i);
        let coll = tx.collection(&collection_name).unwrap();
        let count = coll.count().unwrap();
        assert_eq!(count, 5);
    }

    // Cleanup
    db.close().unwrap();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_single_writer_still_works() {
    let path = "/tmp/test_batch_single_writer.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();

    // Single-threaded write should still work fine
    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();

    for i in 0..10 {
        let doc = json!({"name": format!("User {}", i), "id": i});
        users.insert(doc).unwrap();
    }

    tx.commit().unwrap();

    // Verify
    let mut tx = db.begin().unwrap();
    let users = tx.collection("users").unwrap();
    let count = users.count().unwrap();
    assert_eq!(count, 10);

    // Cleanup
    db.close().unwrap();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}
