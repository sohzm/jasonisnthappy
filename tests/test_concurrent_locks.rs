// Stress test for concurrent lock ordering in transactions
// Tests the scenario where drop_collection and commit happen concurrently

use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_concurrent_drop_and_commit() {
    use std::fs;
    let test_path = "/tmp/test_concurrent_locks.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    // Open database
    use jasonisnthappy::core::database::Database;
    let db = Database::open(test_path).expect("Failed to open database");
    let db = Arc::new(db);

    // Create initial collection
    {
        let mut tx = db.begin().unwrap();
        let _ = tx.create_collection("coll1"); // Ignore error if already exists
        let mut c = tx.collection("coll1").unwrap();
        let _ = c.insert(serde_json::json!({"_id": "doc1", "x": 1})); // Ignore error if already exists
        let _ = tx.commit();
    }

    println!("Testing concurrent drop_collection and commit...");

    let mut handles = vec![];

    // Spawn multiple threads doing drop/create operations
    for i in 0..5 {
        let db_clone = db.clone();
        let h = thread::spawn(move || {
            for j in 0..10 {
                let coll_name = format!("coll_{}", i);

                // Create and drop collection repeatedly
                {
                    let mut tx = db_clone.begin().unwrap();
                    let _ = tx.create_collection(&coll_name);
                    let _ = tx.commit();
                }

                thread::sleep(Duration::from_micros(100));

                {
                    let mut tx = db_clone.begin().unwrap();
                    let _ = tx.drop_collection(&coll_name);
                    let _ = tx.commit();
                }

                if j % 3 == 0 {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });
        handles.push(h);
    }

    // Spawn threads doing concurrent commits with writes
    for i in 0..5 {
        let db_clone = db.clone();
        let h = thread::spawn(move || {
            for j in 0..20 {
                let mut tx = db_clone.begin().unwrap();

                // Try to write to coll1 (might fail if dropped)
                if let Ok(mut c) = tx.collection("coll1") {
                    let doc_id = format!("doc_{}_{}", i, j);
                    let _ = c.insert(serde_json::json!({"_id": doc_id, "i": i, "j": j}));
                }

                let _ = tx.commit();

                if j % 5 == 0 {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });
        handles.push(h);
    }

    // Wait with timeout to detect deadlock
    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();

    for handle in handles {
        while !handle.is_finished() {
            if start.elapsed() > timeout {
                panic!("DEADLOCK DETECTED: Thread did not complete within {} seconds", timeout.as_secs());
            }
            thread::sleep(Duration::from_millis(100));
        }
        handle.join().expect("Thread panicked");
    }

    println!("Stress test completed successfully - no deadlock detected");

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

#[test]
fn test_concurrent_rename_and_commit() {
    use std::fs;
    use jasonisnthappy::core::database::Database;
    let test_path = "/tmp/test_concurrent_rename.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).expect("Failed to open database");
    let db = Arc::new(db);

    // Create collections
    {
        let mut tx = db.begin().unwrap();
        let _ = tx.create_collection("base_coll"); // Ignore error if already exists
        let _ = tx.commit();
    }

    println!("Testing concurrent rename_collection and commit...");

    let mut handles = vec![];

    // Threads doing rename operations
    for i in 0..3 {
        let db_clone = db.clone();
        let h = thread::spawn(move || {
            for j in 0..10 {
                let old_name = format!("coll_{}_{}", i, j);
                let new_name = format!("coll_{}_{}",i, j + 1);

                {
                    let mut tx = db_clone.begin().unwrap();
                    let _ = tx.create_collection(&old_name);
                    let _ = tx.commit();
                }

                thread::sleep(Duration::from_micros(100));

                {
                    let mut tx = db_clone.begin().unwrap();
                    let _ = tx.rename_collection(&old_name, &new_name);
                    let _ = tx.commit();
                }

                thread::sleep(Duration::from_millis(1));
            }
        });
        handles.push(h);
    }

    // Threads doing commits with version chain updates
    for i in 0..3 {
        let db_clone = db.clone();
        let h = thread::spawn(move || {
            for j in 0..20 {
                let mut tx = db_clone.begin().unwrap();

                if let Ok(mut c) = tx.collection("base_coll") {
                    let doc_id = format!("doc_{}_{}", i, j);
                    let _ = c.insert(serde_json::json!({"_id": doc_id, "val": i * 1000 + j}));
                }

                let _ = tx.commit();
                thread::sleep(Duration::from_millis(1));
            }
        });
        handles.push(h);
    }

    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();

    for handle in handles {
        while !handle.is_finished() {
            if start.elapsed() > timeout {
                panic!("DEADLOCK DETECTED: Thread did not complete within {} seconds", timeout.as_secs());
            }
            thread::sleep(Duration::from_millis(100));
        }
        handle.join().expect("Thread panicked");
    }

    println!("Rename stress test completed - no deadlock detected");

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}
