// Debug/profiling tests for commit performance analysis

use jasonisnthappy::core::database::Database;
use std::fs;
use std::time::Instant;

#[test]
fn test_commit_timing_breakdown() {
    let test_path = "/tmp/test_commit_profile.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();

    // Test with 500 docs to see timing
    let num_docs = 500;

    // Build transaction
    let build_start = Instant::now();
    let mut tx = db.begin().unwrap();
    let mut coll = tx.collection("test").unwrap();

    for i in 0..num_docs {
        let doc = serde_json::json!({
            "_id": format!("doc{}", i),
            "data": "x".repeat(1000),
        });
        coll.insert(doc).unwrap();
    }
    let _build_time = build_start.elapsed();

    // Now commit and measure
    let commit_start = Instant::now();
    tx.commit().unwrap();
    let _commit_time = commit_start.elapsed();

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

#[test]
fn test_wal_write_timing() {
    // Test if WAL writing is the bottleneck
    let test_path = "/tmp/test_wal_timing.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();

    for size in [10, 50, 100, 500] {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        for i in 0..size {
            let doc = serde_json::json!({
                "_id": format!("s{}_doc{}", size, i),
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
fn test_btree_insert_timing() {
    // Test if B-tree inserts are the bottleneck
    let test_path = "/tmp/test_btree_timing.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();

    for size in [10, 50, 100, 500, 1000] {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        for i in 0..size {
            let doc = serde_json::json!({
                "_id": format!("batch{}_doc{}", size, i),
                "data": "x".repeat(1000),
            });
            coll.insert(doc).unwrap();
        }

        // Commit (but don't time it separately here)
        tx.commit().unwrap();
    }

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}
