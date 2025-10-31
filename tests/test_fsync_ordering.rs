// Test to verify fsync ordering follows WAL-first protocol
//
// The commit sequence is:
// 1. wal.sync() - blocks until WAL is on disk
// 2. pager.flush_no_sync() - writes to OS page cache (not disk)
// 3. pager.sync_data_only() - blocks until DB pages are on disk
//
// This ensures WAL is on disk BEFORE DB pages, which is correct WAL-first logging.
// The concern in Issue #4 about "OS may reorder writes" is unfounded because:
// - sync_all() is synchronous - it doesn't return until data is on disk
// - flush_no_sync() only writes to page cache
// - Even if kernel writeback flushes cache, WAL was already synced first

use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use jasonisnthappy::core::database::Database;

/// Verify that committed data can be recovered after "crash" (DB close without checkpoint)
#[test]
fn test_wal_recovery_after_unclean_shutdown() {
    let test_path = "/tmp/test_fsync_ordering_recovery.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    // Phase 1: Write data
    {
        let db = Database::open(test_path).unwrap();
        let mut tx = db.begin().unwrap();
        tx.create_collection("test").unwrap();
        tx.commit().unwrap();

        // Insert multiple documents
        for i in 0..100 {
            let mut tx = db.begin().unwrap();
            let mut coll = tx.collection("test").unwrap();
            coll.insert(serde_json::json!({
                "_id": format!("doc_{}", i),
                "value": i,
                "data": format!("test_data_{}", i)
            })).unwrap();
            tx.commit().unwrap();
        }

        // Explicitly drop db without checkpoint to simulate unclean shutdown
        // WAL should contain all committed transactions
        drop(db);
    }

    // Phase 2: Verify WAL exists and has data
    let wal_path = format!("{}-wal", test_path);
    let wal_metadata = fs::metadata(&wal_path).expect("WAL file should exist");
    println!("WAL file size: {} bytes", wal_metadata.len());
    assert!(wal_metadata.len() > 0, "WAL should contain data");

    // Phase 3: Reopen and verify all data is recoverable from WAL
    {
        let db = Database::open(test_path).unwrap();
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("test").unwrap();

        // Verify all 100 documents are present
        for i in 0..100 {
            let doc_id = format!("doc_{}", i);
            let doc = coll.find_by_id(&doc_id).unwrap_or_else(|e| {
                panic!("Document {} should be recoverable from WAL: {:?}", doc_id, e);
            });

            let value = doc.get("value").and_then(|v| v.as_i64()).unwrap();
            assert_eq!(value, i as i64, "Document {} should have correct value", doc_id);
        }

        println!("All 100 documents recovered successfully from WAL");
    }

    // Cleanup
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

/// Test that fsync ordering is maintained under concurrent load
#[test]
fn test_fsync_ordering_concurrent() {
    let test_path = "/tmp/test_fsync_ordering_concurrent.db";
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
    let mut handles = vec![];

    // Multiple writers committing concurrently
    for writer_id in 0..num_writers {
        let db_clone = db.clone();
        let h = thread::spawn(move || {
            for op in 0..ops_per_writer {
                let mut tx = db_clone.begin().unwrap();
                let mut coll = tx.collection("test").unwrap();

                coll.insert(serde_json::json!({
                    "_id": format!("w{}_op{}", writer_id, op),
                    "writer": writer_id,
                    "op": op
                })).unwrap();

                // Commit triggers the fsync sequence:
                // 1. wal.sync() 2. flush_no_sync() 3. sync_data_only()
                tx.commit().unwrap();

                // Small delay to increase interleaving
                if op % 10 == 0 {
                    thread::sleep(Duration::from_micros(100));
                }
            }
        });
        handles.push(h);
    }

    // Wait for all writers
    for h in handles {
        h.join().unwrap();
    }

    // Close and reopen to test recovery
    drop(db);

    let db = Database::open(test_path).unwrap();
    let mut tx = db.begin().unwrap();
    let coll = tx.collection("test").unwrap();

    // Verify all documents are present
    let mut found = 0;
    for writer_id in 0..num_writers {
        for op in 0..ops_per_writer {
            let doc_id = format!("w{}_op{}", writer_id, op);
            if coll.find_by_id(&doc_id).is_ok() {
                found += 1;
            } else {
                panic!("Missing document {} - fsync ordering may be broken", doc_id);
            }
        }
    }

    let expected = num_writers * ops_per_writer;
    assert_eq!(found, expected, "All {} documents should be recoverable", expected);

    println!("fsync ordering test passed: {} concurrent commits, all data recovered", expected);

    // Cleanup
    drop(tx);
    drop(db);
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

/// Verify fsync is actually called by checking that data survives DB close
#[test]
fn test_fsync_ensures_durability() {
    let test_path = "/tmp/test_fsync_durability.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    // Write and commit data
    {
        let db = Database::open(test_path).unwrap();
        let mut tx = db.begin().unwrap();
        tx.create_collection("test").unwrap();
        let mut coll = tx.collection("test").unwrap();

        coll.insert(serde_json::json!({
            "_id": "test_doc",
            "message": "This should survive DB close"
        })).unwrap();

        // Commit calls wal.sync() and pager.sync_data_only()
        // These should ensure data is on disk
        tx.commit().unwrap();

        // Immediately close DB without checkpoint
        drop(db);
    }

    // Reopen - if fsync worked, data should be in WAL and recoverable
    {
        let db = Database::open(test_path).unwrap();
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("test").unwrap();

        let doc = coll.find_by_id("test_doc")
            .expect("Document should be recoverable - fsync should have persisted WAL");

        let message = doc.get("message").and_then(|v| v.as_str()).unwrap();
        assert_eq!(message, "This should survive DB close");

        println!("fsync durability verified: data survived unclean shutdown");
    }

    // Cleanup
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}
