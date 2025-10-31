// Debug tests for deadlock scenario analysis

use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

#[test]
fn test_mixed_read_write_deadlock() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

    // Prepopulate some data
    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("data").unwrap();
        for i in 0..10 {
            coll.insert(json!({"_id": format!("initial_{}", i), "value": i})).unwrap();
        }
        tx.commit().unwrap();
    }

    let writes_done = Arc::new(AtomicU64::new(0));
    let reads_done = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Spawn 3 writer threads
    for worker_id in 0..3 {
        let db = Arc::clone(&db);
        let writes = Arc::clone(&writes_done);

        let handle = thread::spawn(move || {
            for op_id in 0..5 {
                let mut tx = db.begin().unwrap();
                let mut coll = tx.collection("data").unwrap();
                let doc = json!({
                    "_id": format!("w{}_op{}", worker_id, op_id),
                    "worker": worker_id,
                    "op": op_id,
                });

                if coll.insert(doc).is_ok() {
                    if tx.commit().is_ok() {
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        handles.push(handle);
    }

    // Spawn 3 reader threads
    for _reader_id in 0..3 {
        let db = Arc::clone(&db);
        let reads = Arc::clone(&reads_done);

        let handle = thread::spawn(move || {
            for _op_id in 0..10 {
                let mut tx = db.begin().unwrap();
                let coll = tx.collection("data").unwrap();

                if coll.find_all().is_ok() {
                    reads.fetch_add(1, Ordering::Relaxed);
                }
                // Drop tx (implicit rollback)
            }
        });
        handles.push(handle);
    }

    // Wait with timeout
    for handle in handles.into_iter() {
        handle.join().unwrap();
    }

    let _writes_completed = writes_done.load(Ordering::Relaxed);
    let _reads_completed = reads_done.load(Ordering::Relaxed);
}
