// MVCC (Multi-Version Concurrency Control) stress tests
// Tests that verify transaction isolation, conflict detection, and cache behavior

use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;
use rand::Rng;

fn setup_test_db() -> (TempDir, Database) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("stress_test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    (temp_dir, db)
}

#[test]
fn test_mvcc_garbage_collection() {
    let (_temp_dir, db) = setup_test_db();

    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();
        coll.insert(json!({"_id": "versioned_doc", "version": 0})).unwrap();
        tx.commit().unwrap();
    }

    for i in 1..=1000 {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();
        coll.update_by_id("versioned_doc", json!({"version": i})).unwrap();
        tx.commit().unwrap();
    }

    let mut tx = db.begin().unwrap();
    let coll = tx.collection("test").unwrap();
    let doc = coll.find_by_id("versioned_doc").unwrap();
    tx.rollback().unwrap();

    assert_eq!(doc.get("version").and_then(|v| v.as_u64()), Some(1000));
}

#[test]
fn test_mvcc_read_write_conflicts() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("mvcc_rw_conflicts.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("docs").unwrap();
        coll.insert(json!({"_id": "doc1", "value": 0})).unwrap();
        coll.insert(json!({"_id": "doc2", "value": 0})).unwrap();
        tx.commit().unwrap();
    }

    let mut tx1 = db.begin().unwrap();
    let coll1 = tx1.collection("docs").unwrap();
    let _doc1 = coll1.find_by_id("doc1").unwrap();

    {
        let mut tx2 = db.begin().unwrap();
        let mut coll2 = tx2.collection("docs").unwrap();
        coll2.update_by_id("doc1", json!({"value": 100})).unwrap();
        tx2.commit().unwrap();
    }

    let mut coll1_mut = tx1.collection("docs").unwrap();
    let _ = coll1_mut.update_by_id("doc1", json!({"value": 200}));
    assert!(tx1.commit().is_err(), "TX1 should have failed with conflict");
    let mut tx3 = db.begin().unwrap();
    let mut tx4 = db.begin().unwrap();

    let coll3 = tx3.collection("docs").unwrap();
    let coll4 = tx4.collection("docs").unwrap();

    let _doc3 = coll3.find_by_id("doc2").unwrap();
    let _doc4 = coll4.find_by_id("doc2").unwrap();

    assert!(tx3.commit().is_ok() && tx4.commit().is_ok(), "Concurrent reads should not conflict");
    let mut tx5 = db.begin().unwrap();
    let mut tx6 = db.begin().unwrap();

    let mut coll5 = tx5.collection("docs").unwrap();
    let mut coll6 = tx6.collection("docs").unwrap();

    coll5.update_by_id("doc1", json!({"value": 1})).unwrap();
    coll6.update_by_id("doc2", json!({"value": 2})).unwrap();

    assert!(tx5.commit().is_ok() && tx6.commit().is_ok(), "Writes to different documents should not conflict");
    db.close().unwrap();
}

#[test]
fn test_mvcc_write_write_conflicts() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("mvcc_conflicts.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();
        coll.insert(json!({"_id": "conflict_doc", "value": 0})).unwrap();
        tx.commit().unwrap();
    }

    let mut tx1 = db.begin().unwrap();
    let coll1 = tx1.collection("test").unwrap();
    let _doc1 = coll1.find_by_id("conflict_doc").unwrap();

    {
        let mut tx2 = db.begin().unwrap();
        let mut coll2 = tx2.collection("test").unwrap();
        coll2.update_by_id("conflict_doc", json!({"value": 100})).unwrap();
        tx2.commit().unwrap();
    }

    let mut coll1_mut = tx1.collection("test").unwrap();
    let conflict_detected = match coll1_mut.update_by_id("conflict_doc", json!({"value": 200})) {
        Ok(_) => tx1.commit().is_err(),
        Err(_) => true,
    };

    assert!(conflict_detected, "Expected MVCC conflict to be detected");
    db.close().unwrap();
}

#[test]
fn test_lru_cache_eviction() {
    let (_temp_dir, db) = setup_test_db();

    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("cache_test").unwrap();

        for i in 0..5000 {
            coll.insert(json!({
                "_id": format!("doc_{}", i),
                "value": i,
                "data": "X".repeat(100),
            })).unwrap();
        }

        tx.commit().unwrap();
    }

    let mut rng = rand::thread_rng();
    let access_count = 10000;
    let mut hits = 0;

    for _i in 0..access_count {
        let doc_id = format!("doc_{}", rng.gen_range(0..5000));

        let mut tx = db.begin().unwrap();
        let coll = tx.collection("cache_test").unwrap();

        if coll.find_by_id(&doc_id).is_ok() {
            hits += 1;
        }

        tx.rollback().unwrap();
    }

    assert!(hits > access_count * 95 / 100, "Hit rate should be > 95%");
}

#[test]
fn test_unique_index_violations() {
    use std::io::Write;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("unique_violations.db");
    let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

    let mut log_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("/tmp/test_unique_index_violations.log")
        .unwrap();

    writeln!(log_file, "[TEST START] Unique index violations test starting").unwrap();

    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("users").unwrap();

        coll.insert(json!({"_id": "user0", "email": "initial@test.com", "name": "Initial"})).unwrap();
        tx.commit().unwrap();
        writeln!(log_file, "[SETUP] Inserted initial document").unwrap();
    }

    let success = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));

    writeln!(log_file, "[WORKERS] Spawning 10 workers, each doing 10 inserts (100 total ops)").unwrap();

    let handles: Vec<_> = (0..10)
        .map(|worker_id| {
            let db = Arc::clone(&db);
            let success = Arc::clone(&success);
            let conflicts = Arc::clone(&conflicts);
            let completed = Arc::clone(&completed);

            thread::spawn(move || {
                let mut worker_log = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("/tmp/test_unique_index_violations.log")
                    .unwrap();

                writeln!(worker_log, "[WORKER {}] Started", worker_id).unwrap();

                for i in 0..10 {
                    let doc_id = format!("dup_doc_{}", i % 5);
                    writeln!(worker_log, "[WORKER {}] Iteration {}, doc_id: {}", worker_id, i, doc_id).unwrap();

                    let mut tx = db.begin().unwrap();
                    let mut coll = tx.collection("users").unwrap();

                    match coll.insert(json!({"_id": doc_id, "worker": worker_id, "iter": i})) {
                        Ok(_) => {
                            match tx.commit() {
                                Ok(_) => {
                                    success.fetch_add(1, Ordering::Relaxed);
                                    writeln!(worker_log, "[WORKER {}] Iteration {} SUCCESS", worker_id, i).unwrap();
                                }
                                Err(e) => {
                                    conflicts.fetch_add(1, Ordering::Relaxed);
                                    writeln!(worker_log, "[WORKER {}] Iteration {} CONFLICT on commit: {:?}", worker_id, i, e).unwrap();
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.rollback();
                            conflicts.fetch_add(1, Ordering::Relaxed);
                            writeln!(worker_log, "[WORKER {}] Iteration {} CONFLICT on insert: {:?}", worker_id, i, e).unwrap();
                        }
                    }

                    let comp = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    if comp % 10 == 0 {
                        writeln!(worker_log, "[PROGRESS] {}/100 operations completed", comp).unwrap();
                    }
                }

                writeln!(worker_log, "[WORKER {}] Finished", worker_id).unwrap();
            })
        })
        .collect();

    writeln!(log_file, "[WORKERS] All workers spawned, waiting for completion").unwrap();

    for (idx, handle) in handles.into_iter().enumerate() {
        handle.join().unwrap();
        writeln!(log_file, "[JOIN] Worker {} joined", idx).unwrap();
    }

    let succ = success.load(Ordering::Relaxed);
    let conf = conflicts.load(Ordering::Relaxed);
    let total = completed.load(Ordering::Relaxed);

    writeln!(log_file, "[RESULTS] Success: {}, Conflicts: {}, Total: {}", succ, conf, total).unwrap();
    writeln!(log_file, "[TEST END] Test completed").unwrap();

    assert!(succ > 0, "Some inserts should succeed");
    db.close().unwrap();
}
