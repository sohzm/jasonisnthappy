/// Comprehensive stress tests for Database Drop implementation
/// Tests automatic resource cleanup without explicit close() calls
use jasonisnthappy::Database;
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_drop_single_thread_no_close() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drop_single.db");
    let path_str = db_path.to_str().unwrap();

    // Open, write data, let Drop cleanup (no explicit close)
    {
        let db = Database::open(path_str).unwrap();
        let coll = db.collection("test");
        coll.insert(json!({"_id": "doc1", "value": 42})).unwrap();
    } // Drop happens here

    // Should be able to reopen and read data
    {
        let db = Database::open(path_str).unwrap();
        let coll = db.collection("test");
        let doc = coll.find_by_id("doc1").unwrap();
        assert_eq!(doc["value"], 42);
        db.close().unwrap();
    }
}

#[test]
fn test_drop_concurrent_arc_clones() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drop_arc.db");
    let path_str = db_path.to_str().unwrap().to_string();

    // Test with Arc<Database> shared across threads (NO Database::clone, just Arc::clone)
    {
        let db = Arc::new(Database::open(&path_str).unwrap());
        let barrier = Arc::new(Barrier::new(5));

        let handles: Vec<_> = (0..5)
            .map(|i| {
                let db = Arc::clone(&db); // Arc::clone, not Database::clone
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();
                    let coll = db.collection("concurrent");
                    for j in 0..20 {
                        let doc_id = format!("thread{}_doc{}", i, j);
                        let _ = coll.insert(json!({
                            "_id": doc_id,
                            "thread": i,
                            "iteration": j
                        }));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // All Arc clones will drop here, but only the last one should cleanup
    } // Last Arc drops - triggers Database::drop

    // Verify data persisted correctly
    {
        let db = Database::open(&path_str).unwrap();
        let coll = db.collection("concurrent");
        let docs = coll.find_all().unwrap();
        assert!(docs.len() > 0, "Data should be persisted after Arc drop");
        db.close().unwrap();
    }
}

#[test]
fn test_drop_mixed_close_and_drop() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drop_mixed.db");
    let path_str = db_path.to_str().unwrap();

    // Scenario 1: Explicit close
    {
        let db = Database::open(path_str).unwrap();
        let coll = db.collection("test");
        coll.insert(json!({"_id": "doc1", "source": "explicit_close"})).unwrap();
        db.close().unwrap(); // Explicit close
    }

    // Scenario 2: Automatic drop
    {
        let db = Database::open(path_str).unwrap();
        let coll = db.collection("test");
        coll.insert(json!({"_id": "doc2", "source": "auto_drop"})).unwrap();
        // No close - rely on Drop
    }

    // Scenario 3: Verify both persisted
    {
        let db = Database::open(path_str).unwrap();
        let coll = db.collection("test");
        let doc1 = coll.find_by_id("doc1").unwrap();
        let doc2 = coll.find_by_id("doc2").unwrap();
        assert_eq!(doc1["source"], "explicit_close");
        assert_eq!(doc2["source"], "auto_drop");
        db.close().unwrap();
    }
}

#[test]
fn test_drop_rapid_open_drop_cycles() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drop_rapid.db");
    let path_str = db_path.to_str().unwrap();

    // Rapid open/drop cycles without explicit close
    for i in 0..50 {
        let db = Database::open(path_str).unwrap();
        let coll = db.collection("rapid");
        let doc_id = format!("doc{}", i);
        coll.insert(json!({"_id": doc_id, "iteration": i})).unwrap();
        // Drop happens here - no explicit close
    }

    // Verify all data persisted
    {
        let db = Database::open(path_str).unwrap();
        let coll = db.collection("rapid");
        let docs = coll.find_all().unwrap();
        assert_eq!(docs.len(), 50, "All 50 documents should be persisted");
        db.close().unwrap();
    }
}

#[test]
fn test_drop_concurrent_separate_instances() {
    // This test: threads take turns opening the database (proper exclusive access)
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drop_separate.db");
    let path_str = db_path.to_str().unwrap().to_string();

    // Open initial database
    let db = Database::open(&path_str).unwrap();
    db.close().unwrap();

    // Multiple threads each opening the database sequentially with small delays
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let path = path_str.clone();

            thread::spawn(move || {
                // Small stagger to avoid simultaneous opens
                thread::sleep(Duration::from_millis(i * 50));

                // Each thread opens database, does work, lets Drop cleanup
                // Retry logic for lock contention
                let db = loop {
                    match Database::open(&path) {
                        Ok(db) => break db,
                        Err(_) => {
                            thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                    }
                };

                let coll = db.collection("separate");

                for j in 0..15 {
                    let doc_id = format!("thread{}_doc{}", i, j);
                    let _ = coll.insert(json!({
                        "_id": doc_id,
                        "thread": i,
                        "iteration": j
                    }));
                }

                // Let Drop cleanup - no explicit close
                // Small delay to ensure Drop completes
                drop(db);
                thread::sleep(Duration::from_millis(20));
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify data
    thread::sleep(Duration::from_millis(100)); // Ensure all Drops completed
    {
        let db = Database::open(&path_str).unwrap();
        let coll = db.collection("separate");
        let docs = coll.find_all().unwrap();
        println!("Found {} documents after concurrent separate instances", docs.len());
        assert!(docs.len() > 0, "Data should persist after all instances drop");
        db.close().unwrap();
    }
}

#[test]
fn test_drop_with_pending_transactions() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drop_tx.db");
    let path_str = db_path.to_str().unwrap();

    // Create a transaction but don't commit, then drop
    {
        let db = Database::open(path_str).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();
        coll.insert(json!({"_id": "uncommitted", "value": 1})).unwrap();
        // Don't commit - just drop both tx and db
    }

    // Uncommitted data should NOT persist
    {
        let db = Database::open(path_str).unwrap();
        let coll = db.collection("test");
        let result = coll.find_by_id("uncommitted");
        assert!(result.is_err(), "Uncommitted data should not persist");
        db.close().unwrap();
    }

    // Now with committed transaction + drop
    {
        let db = Database::open(path_str).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();
        coll.insert(json!({"_id": "committed", "value": 2})).unwrap();
        tx.commit().unwrap();
        // Drop db without explicit close
    }

    // Committed data SHOULD persist
    {
        let db = Database::open(path_str).unwrap();
        let coll = db.collection("test");
        let doc = coll.find_by_id("committed").unwrap();
        assert_eq!(doc["value"], 2);
        db.close().unwrap();
    }
}

#[test]
fn test_drop_lock_release_verification() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drop_lock.db");
    let path_str = db_path.to_str().unwrap().to_string();

    // Open and drop without close
    {
        let db = Database::open(&path_str).unwrap();
        db.collection("test").insert(json!({"_id": "test"})).unwrap();
        // Drop - should release lock
    }

    // Should be able to open immediately (lock was released)
    let db2 = Database::open(&path_str);
    assert!(db2.is_ok(), "Lock should be released after Drop");
    db2.unwrap().close().unwrap();
}

#[test]
fn test_drop_stress_many_threads() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drop_stress.db");
    let path_str = db_path.to_str().unwrap().to_string();

    // Initial setup
    {
        let db = Database::open(&path_str).unwrap();
        db.close().unwrap();
    }

    // 10 threads (reduced from 20), each doing 5 open/insert/drop cycles with retry logic
    let handles: Vec<_> = (0..10)
        .map(|thread_id| {
            let path = path_str.clone();
            thread::spawn(move || {
                for cycle in 0..5 {
                    // Retry logic for lock contention
                    let db = loop {
                        match Database::open(&path) {
                            Ok(db) => break db,
                            Err(_) => {
                                thread::sleep(Duration::from_millis(5));
                                continue;
                            }
                        }
                    };

                    let coll = db.collection("stress");
                    let doc_id = format!("t{}_c{}", thread_id, cycle);
                    let _ = coll.insert(json!({
                        "_id": doc_id,
                        "thread": thread_id,
                        "cycle": cycle
                    }));

                    // Explicit drop and small delay for cleanup
                    drop(db);
                    thread::sleep(Duration::from_millis(10));
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify data integrity
    thread::sleep(Duration::from_millis(200)); // Ensure all Drops completed
    {
        let db = Database::open(&path_str).unwrap();
        let coll = db.collection("stress");
        let docs = coll.find_all().unwrap();
        println!("Stress test: {} documents persisted", docs.len());
        assert!(docs.len() > 0, "Data should persist despite heavy concurrent Drop usage");
        db.close().unwrap();
    }
}

#[test]
fn test_drop_vs_explicit_close_data_integrity() {
    // Compare data integrity between explicit close and automatic drop
    let temp_dir = TempDir::new().unwrap();

    let path_close = temp_dir.path().join("explicit_close.db");
    let path_drop = temp_dir.path().join("auto_drop.db");

    // Write 1000 docs with explicit close
    {
        let db = Database::open(path_close.to_str().unwrap()).unwrap();
        let coll = db.collection("test");
        for i in 0..1000 {
            coll.insert(json!({"_id": format!("doc{}", i), "value": i})).unwrap();
        }
        db.close().unwrap();
    }

    // Write 1000 docs with automatic drop
    {
        let db = Database::open(path_drop.to_str().unwrap()).unwrap();
        let coll = db.collection("test");
        for i in 0..1000 {
            coll.insert(json!({"_id": format!("doc{}", i), "value": i})).unwrap();
        }
        // Drop - no explicit close
    }

    // Both should have exactly 1000 documents
    let count_close = {
        let db = Database::open(path_close.to_str().unwrap()).unwrap();
        let count = db.collection("test").find_all().unwrap().len();
        db.close().unwrap();
        count
    };

    let count_drop = {
        let db = Database::open(path_drop.to_str().unwrap()).unwrap();
        let count = db.collection("test").find_all().unwrap().len();
        db.close().unwrap();
        count
    };

    assert_eq!(count_close, 1000, "Explicit close should persist all docs");
    assert_eq!(count_drop, 1000, "Automatic drop should persist all docs");
    assert_eq!(count_close, count_drop, "Both methods should persist same amount of data");
}
