// Concurrency stress tests
// Tests that focus on concurrent operations, multi-threading, and race conditions

use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn setup_test_db() -> (TempDir, Database) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("stress_test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    (temp_dir, db)
}

#[test]
fn test_concurrent_writes_high_volume() {
    let (_temp_dir, db) = setup_test_db();
    let db = Arc::new(db);

    let num_workers = 10;
    let ops_per_worker = 10;
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_workers)
        .map(|worker_id| {
            let db = Arc::clone(&db);
            let success = Arc::clone(&success_count);
            let errors = Arc::clone(&error_count);

            thread::spawn(move || {
                for op_id in 0..ops_per_worker {
                    match db.begin() {
                        Ok(mut tx) => {
                            match tx.collection("data") {
                                Ok(mut coll) => {
                                    let doc = json!({
                                        "_id": format!("w{}_op{}", worker_id, op_id),
                                        "worker": worker_id,
                                        "op": op_id,
                                        "data": "X".repeat(500),
                                    });

                                    match coll.insert(doc) {
                                        Ok(_) => {
                                            match tx.commit() {
                                                Ok(_) => { success.fetch_add(1, Ordering::Relaxed); }
                                                Err(_e) => {
                                                    errors.fetch_add(1, Ordering::Relaxed);
                                                }
                                            }
                                        }
                                        Err(_e) => {
                                            let _ = tx.rollback();
                                            errors.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                                Err(_e) => {
                                    errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(_e) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let _success = success_count.load(Ordering::Relaxed);
    let _errors = error_count.load(Ordering::Relaxed);

    // Checkpoint to flush WAL to main database file before closing
    db.checkpoint().unwrap();

    drop(db);
    let db = Database::open(_temp_dir.path().join("stress_test.db").to_str().unwrap()).unwrap();

    let mut tx = db.begin().unwrap();
    let coll = tx.collection("data").unwrap();
    let count = coll.count().unwrap();

    let success = success_count.load(Ordering::Relaxed);
    assert!(success > 0, "No successful operations!");
    assert_eq!(count, success as usize, "Document count mismatch!");
}

#[test]
fn test_checkpoint_while_writing() {
    let (_temp_dir, db) = setup_test_db();
    let db = Arc::new(db);

    let num_workers = 10;
    let ops_per_worker = 20;
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_workers)
        .map(|worker_id| {
            let db = Arc::clone(&db);
            let success = Arc::clone(&success_count);
            let errors = Arc::clone(&error_count);

            thread::spawn(move || {
                for op_id in 0..ops_per_worker {
                    let mut tx = match db.begin() {
                        Ok(tx) => tx,
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    let mut coll = match tx.collection("data") {
                        Ok(c) => c,
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    let doc = json!({
                        "worker": worker_id,
                        "op": op_id,
                        "data": "X".repeat(500),
                    });

                    match coll.insert(doc) {
                        Ok(_) => {
                            match tx.commit() {
                                Ok(_) => { success.fetch_add(1, Ordering::Relaxed); }
                                Err(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                            }
                        }
                        Err(_) => {
                            let _ = tx.rollback();
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(100));

    let db_checkpoint = Arc::clone(&db);
    let checkpoint_handle = thread::spawn(move || {
        db_checkpoint.checkpoint()
    });

    let _checkpoint_result = checkpoint_handle.join().unwrap();

    for handle in handles {
        handle.join().unwrap();
    }

    let success = success_count.load(Ordering::Relaxed);

    assert!(success > 0, "No successful operations!");
}

#[test]
fn test_realistic_workload_mixed() {
    let (_temp_dir, db) = setup_test_db();
    let db = Arc::new(db);
    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("data").unwrap();
        for i in 0..100 {
            let doc = json!({
                "_id": format!("initial_{}", i),
                "value": i,
            });
            coll.insert(doc).unwrap();
        }
        tx.commit().unwrap();
    }

    let num_workers = 10;
    let ops_per_worker = 100;
    let read_ratio = 0.8;
    let success_count = Arc::new(AtomicU64::new(0));
    let read_count = Arc::new(AtomicU64::new(0));
    let write_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_workers)
        .map(|worker_id| {
            let db = Arc::clone(&db);
            let success = Arc::clone(&success_count);
            let reads = Arc::clone(&read_count);
            let writes = Arc::clone(&write_count);

            thread::spawn(move || {
                for op_id in 0..ops_per_worker {
                    let is_read = (op_id % 10) as f64 / 10.0 < read_ratio;

                    if is_read {
                        let mut tx = db.begin().unwrap();
                        let coll = tx.collection("data").unwrap();
                        if coll.find_all().is_ok() {
                            success.fetch_add(1, Ordering::Relaxed);
                            reads.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        let mut tx = db.begin().unwrap();
                        let mut coll = tx.collection("data").unwrap();
                        let doc = json!({
                            "_id": format!("w{}_op{}", worker_id, op_id),
                            "worker": worker_id,
                            "op": op_id,
                            "data": "X".repeat(100),
                        });

                        match coll.insert(doc) {
                            Ok(_) => {
                                match tx.commit() {
                                    Ok(_) => {
                                        success.fetch_add(1, Ordering::Relaxed);
                                        writes.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(_) => {
                                    }
                                }
                            }
                            Err(_) => {
                                let _ = tx.rollback();
                            }
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let success = success_count.load(Ordering::Relaxed);
    let reads = read_count.load(Ordering::Relaxed);
    let writes = write_count.load(Ordering::Relaxed);

    assert!(success > 0, "No successful operations!");
    assert!(reads > 0, "No reads performed!");
    assert!(writes > 0, "No writes performed!");
}

#[test]
fn test_interleaved_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("interleaved.db");
    let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("data").unwrap();
        for i in 0..20 {
            coll.insert(json!({"_id": format!("doc_{}", i), "value": i})).unwrap();
        }
        tx.commit().unwrap();
    }

    let success = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..5)
        .map(|worker_id| {
            let db = Arc::clone(&db);
            let success = Arc::clone(&success);
            let conflicts = Arc::clone(&conflicts);

            thread::spawn(move || {
                for op in 0..20 {
                    let mut tx = db.begin().unwrap();
                    let doc_id = format!("doc_{}", (worker_id * 4 + op) % 20);

                    let result = if op % 3 == 0 {
                        let coll = tx.collection("data").unwrap();
                        coll.find_by_id(&doc_id).map(|_| ())
                    } else {
                        let mut coll = tx.collection("data").unwrap();
                        coll.update_by_id(&doc_id, json!({"value": worker_id * 1000 + op}))
                    };

                    if result.is_ok() {
                        match tx.commit() {
                            Ok(_) => { success.fetch_add(1, Ordering::Relaxed); }
                            Err(_) => { conflicts.fetch_add(1, Ordering::Relaxed); }
                        }
                    } else {
                        let _ = tx.rollback();
                        conflicts.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let succ = success.load(Ordering::Relaxed);

    assert!(succ > 0, "Some operations should succeed");
    db.close().unwrap();
}

#[test]
fn test_concurrent_database_opens() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("concurrent_opens.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    {
        let db = Database::open(&db_path_str).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();
        coll.insert(json!({"_id": "initial", "value": 0})).unwrap();
        tx.commit().unwrap();
        db.close().unwrap();
    }

    let handles: Vec<_> = (0..10)
        .map(|_i| {
            let path = db_path_str.clone();
            thread::spawn(move || {
                match Database::open(&path) {
                    Ok(db) => {
                        let mut tx = db.begin().unwrap();
                        let coll = tx.collection("test").unwrap();
                        let _doc = coll.find_by_id("initial").unwrap();
                        tx.rollback().unwrap();
                        db.close().unwrap();
                        true
                    }
                    Err(_) => false
                }
            })
        })
        .collect();

    let mut success_count = 0;
    for handle in handles {
        if handle.join().unwrap() {
            success_count += 1;
        }
    }

    assert!(success_count > 0, "At least some concurrent opens should succeed");
}

#[test]
fn test_rapid_open_close() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("rapid_test.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    for i in 0..50 {
        let db = Database::open(&db_path_str).unwrap();

        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("data").unwrap();
        let doc = json!({
            "_id": format!("cycle_{}", i),
            "cycle": i,
        });
        coll.insert(doc).unwrap();
        tx.commit().unwrap();

        db.close().unwrap();
    }

    let db = Database::open(&db_path_str).unwrap();
    let mut tx = db.begin().unwrap();
    let coll = tx.collection("data").unwrap();
    let count = coll.count().unwrap();

    assert_eq!(count, 50, "Expected 50 documents");

    db.close().unwrap();
}

// Multiprocess stress tests
// Tests that verify correct behavior with multiple OS processes accessing the same database

use std::env;
use std::process::{Command, exit};

#[test]
fn test_multiprocess_writes() {
    if let Ok(worker_id_str) = env::var("TEST_WORKER") {
        let worker_id: usize = worker_id_str.parse().unwrap();
        let db_path = env::var("TEST_DB_PATH").unwrap();
        run_write_worker(&db_path, worker_id);
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("multiprocess.db");
    let db_path_str = db_path.to_str().unwrap();

    {
        let db = Database::open(db_path_str).unwrap();
        drop(db);
    }

    const NUM_PROCESSES: usize = 3;
    const INSERTS_PER_PROCESS: usize = 10;

    let mut handles = vec![];
    let errors = Arc::new(std::sync::Mutex::new(Vec::new()));

    for worker_id in 0..NUM_PROCESSES {
        let db_path_str = db_path_str.to_string();
        let errors = Arc::clone(&errors);

        let handle = thread::spawn(move || {
            let exe = env::current_exe().expect("Failed to get current exe");

            let output = Command::new(exe)
                .arg("--test")
                .arg("test_multiprocess_writes")
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env("TEST_WORKER", worker_id.to_string())
                .env("TEST_DB_PATH", &db_path_str)
                .output()
                .expect("Failed to spawn child process");

            if !output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                let mut errors = errors.lock().unwrap();
                errors.push(format!(
                    "Worker {} failed: {:?}\nOutput: {}\n{}",
                    worker_id, output.status, stdout, stderr
                ));
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let errors = errors.lock().unwrap();
    if !errors.is_empty() {
        panic!("Worker errors:\n{}", errors.join("\n"));
    }

    let db = Database::open(db_path_str).unwrap();

    {
        let mut tx = db.begin().unwrap();
        tx.rollback().unwrap();
    }

    let mut tx = db.begin().unwrap();
    let collection = tx.collection("users").unwrap();

    let count = collection.count().unwrap();
    let expected = NUM_PROCESSES * INSERTS_PER_PROCESS;

    assert_eq!(
        count, expected,
        "Expected {} documents, got {}",
        expected, count
    );

    let all_docs = collection.find_all().unwrap();
    tx.rollback().unwrap();

    let mut worker_counts = vec![0; NUM_PROCESSES];
    for doc in &all_docs {
        if let Some(worker) = doc.get("worker").and_then(|v| v.as_u64()) {
            worker_counts[worker as usize] += 1;
        }
    }

    for (i, count) in worker_counts.iter().enumerate() {
        assert_eq!(
            *count, INSERTS_PER_PROCESS,
            "Worker {}: expected {} docs, got {}",
            i, INSERTS_PER_PROCESS, count
        );
    }
}

fn run_write_worker(db_path: &str, worker_id: usize) {
    thread::sleep(Duration::from_millis((worker_id * 10) as u64));

    let db = {
        let mut attempts = 0;
        let max_attempts = 10;
        loop {
            match Database::open(db_path) {
                Ok(db) => break db,
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        eprintln!("Worker {}: Failed to open database after {} attempts: {}", worker_id, attempts, e);
                        exit(1);
                    }
                    thread::sleep(Duration::from_millis(50 * attempts as u64));
                }
            }
        }
    };

    let mut tx = db.begin().unwrap_or_else(|e| {
        eprintln!("Worker {}: Failed to begin transaction: {}", worker_id, e);
        exit(1);
    });

    let mut collection = tx.collection("users").unwrap_or_else(|e| {
        eprintln!("Worker {}: Failed to get collection: {}", worker_id, e);
        exit(1);
    });

    for i in 0..10 {
        let doc = json!({
            "worker": worker_id,
            "index": i,
            "value": worker_id * 1000 + i,
        });

        if let Err(e) = collection.insert(doc) {
            eprintln!("Worker {}: Insert {} failed: {}", worker_id, i, e);
            exit(1);
        }
    }

    if let Err(e) = tx.commit() {
        eprintln!("Worker {}: Commit failed: {}", worker_id, e);
        exit(1);
    }
}

#[test]
fn test_lock_contention() {
    if let Ok(worker_id_str) = env::var("TEST_LOCK_WORKER") {
        let worker_id: usize = worker_id_str.parse().unwrap();
        let db_path = env::var("TEST_DB_PATH").unwrap();
        run_lock_worker(&db_path, worker_id);
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("lock_contention.db");
    let db_path_str = db_path.to_str().unwrap();

    const NUM_PROCESSES: usize = 5;

    let mut handles = vec![];

    for worker_id in 0..NUM_PROCESSES {
        let db_path_str = db_path_str.to_string();

        let handle = thread::spawn(move || {
            let exe = env::current_exe().expect("Failed to get current exe");

            let _output = Command::new(exe)
                .arg("--test")
                .arg("test_lock_contention")
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env("TEST_LOCK_WORKER", worker_id.to_string())
                .env("TEST_DB_PATH", &db_path_str)
                .output()
                .expect("Failed to spawn child process");
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let db = Database::open(db_path_str).unwrap();

    let mut tx = db.begin().unwrap();
    let collection = tx.collection("users").unwrap();
    let count = collection.count().unwrap();
    tx.rollback().unwrap();

    assert!(count > 0, "Expected at least one document written");
}

fn run_lock_worker(db_path: &str, worker_id: usize) {
    let db = Database::open(db_path).unwrap_or_else(|e| {
        eprintln!("Lock worker {}: Failed to open database: {}", worker_id, e);
        exit(1);
    });

    let mut tx = db.begin().unwrap_or_else(|e| {
        eprintln!("Lock worker {}: Failed to begin transaction: {}", worker_id, e);
        exit(1);
    });

    let hold_duration = 50 + worker_id * 10;
    thread::sleep(Duration::from_millis(hold_duration as u64));

    let mut collection = tx.collection("users").unwrap_or_else(|e| {
        eprintln!("Lock worker {}: Failed to get collection: {}", worker_id, e);
        exit(1);
    });

    collection.insert(json!({"worker": worker_id})).unwrap_or_else(|e| {
        eprintln!("Lock worker {}: Insert failed: {}", worker_id, e);
        exit(1);
    });

    tx.commit().unwrap_or_else(|e| {
        eprintln!("Lock worker {}: Commit failed: {}", worker_id, e);
        exit(1);
    });
}

#[test]
fn test_combined_process_and_thread_concurrency() {
    if let Ok(worker_id_str) = env::var("TEST_COMBINED_PROCESS") {
        let process_id: usize = worker_id_str.parse().unwrap();
        let db_path = env::var("TEST_DB_PATH").unwrap();
        run_combined_worker(&db_path, process_id);
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("combined.db");
    let db_path_str = db_path.to_str().unwrap();

    {
        let db = Database::open(db_path_str).unwrap();
        drop(db);
    }

    const NUM_PROCESSES: usize = 5;

    let mut handles = vec![];

    for process_id in 0..NUM_PROCESSES {
        let db_path_str = db_path_str.to_string();

        let handle = thread::spawn(move || {
            let exe = env::current_exe().expect("Failed to get current exe");

            let _output = Command::new(exe)
                .arg("--test")
                .arg("test_combined_process_and_thread_concurrency")
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env("TEST_COMBINED_PROCESS", process_id.to_string())
                .env("TEST_DB_PATH", &db_path_str)
                .output()
                .expect("Failed to spawn child process");
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let db = Database::open(db_path_str).unwrap();

    let mut tx = db.begin().unwrap();
    let collection = tx.collection("data").unwrap();
    let count = collection.count().unwrap();
    tx.rollback().unwrap();

    assert!(count > 0, "Expected at least some documents written");
}

fn run_combined_worker(db_path: &str, process_id: usize) {
    const THREADS_PER_PROCESS: usize = 10;
    const OPS_PER_THREAD: usize = 60;

    let db = Arc::new(Database::open(db_path).unwrap_or_else(|e| {
        eprintln!("Process {}: Failed to open database: {}", process_id, e);
        exit(1);
    }));

    let mut handles = vec![];

    for thread_id in 0..THREADS_PER_PROCESS {
        let db = Arc::clone(&db);

        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            use rand::Rng;

            for op_id in 0..OPS_PER_THREAD {
                let mut tx = match db.begin() {
                    Ok(tx) => tx,
                    Err(_) => continue,
                };

                let mut collection = match tx.collection("data") {
                    Ok(c) => c,
                    Err(_) => continue,
                };

                let doc = json!({
                    "_id": format!("p{}_t{}_op{}", process_id, thread_id, op_id),
                    "process": process_id,
                    "thread": thread_id,
                    "op": op_id,
                    "value": rng.gen_range(0..1000),
                });

                let _ = collection.insert(doc);
                let _ = tx.commit();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
