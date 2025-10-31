// Regression tests for B-tree performance degradation

// Test to demonstrate B-tree performance degradation as database grows

use jasonisnthappy::core::database::Database;
use std::fs;
use std::time::Instant;

#[test]
fn test_insert_performance_vs_database_size() {
    let test_path = "/tmp/test_btree_degradation.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();

    let batch_size = 100;
    let num_batches = 20; // Will insert 2000 total docs

    for batch in 0..num_batches {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        for i in 0..batch_size {
            let doc = serde_json::json!({
                "_id": format!("doc_{}_{}", batch, i),
                "data": "x".repeat(1000),
            });
            coll.insert(doc).unwrap();
        }

        tx.commit().unwrap();
    }

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

#[test]
fn test_fresh_db_vs_full_db() {
    // Test 1: Fresh database
    let fresh_time = {
        let test_path = "/tmp/test_fresh.db";
        let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
        let db = Database::open(test_path).unwrap();

        let start = Instant::now();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        for i in 0..1000 {
            let doc = serde_json::json!({
                "_id": format!("doc{}", i),
                "data": "x".repeat(1000),
            });
            coll.insert(doc).unwrap();
        }

        tx.commit().unwrap();
        let fresh_time = start.elapsed();

        let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

        fresh_time
    };

    // Test 2: Database with existing data
    let full_time = {
        let test_path = "/tmp/test_full.db";
        let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
        let db = Database::open(test_path).unwrap();

        // Pre-populate with 5000 docs
        for batch in 0..50 {
            let mut tx = db.begin().unwrap();
            let mut coll = tx.collection("test").unwrap();

            for i in 0..100 {
                let doc = serde_json::json!({
                    "_id": format!("existing_{}_{}", batch, i),
                    "data": "x".repeat(1000),
                });
                coll.insert(doc).unwrap();
            }

            tx.commit().unwrap();
        }

        // Now measure inserting 1000 more
        let start = Instant::now();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        for i in 0..1000 {
            let doc = serde_json::json!({
                "_id": format!("new_doc{}", i),
                "data": "x".repeat(1000),
            });
            coll.insert(doc).unwrap();
        }

        tx.commit().unwrap();
        let full_time = start.elapsed();

        let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

        full_time
    };

    // Verify that full DB is not dramatically slower (allow up to 5x slowdown)
    let slowdown = full_time.as_secs_f64() / fresh_time.as_secs_f64();
    assert!(slowdown < 5.0, "Performance degradation too severe: {:.2}x", slowdown);
}
