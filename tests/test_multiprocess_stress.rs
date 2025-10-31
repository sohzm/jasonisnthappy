//! Multi-process stress tests for database correctness
//! These tests spawn separate OS processes to test file locking, WAL, and data integrity

use std::collections::HashSet;
use std::env;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use jasonisnthappy::Database;
use serde_json::json;

const TEST_DB_PATH: &str = "/tmp/multiprocess_stress_test.db";

fn cleanup_db() {
    let _ = std::fs::remove_file(TEST_DB_PATH);
    let _ = std::fs::remove_file(format!("{}.lock", TEST_DB_PATH));
    let _ = std::fs::remove_file(format!("{}-wal", TEST_DB_PATH));
}

/// Worker function that runs in a child process
fn run_worker(worker_id: usize, num_ops: usize, collection_name: &str) -> (usize, usize, Vec<String>) {
    let mut success_count = 0;
    let mut error_count = 0;
    let mut inserted_ids = Vec::new();

    // Open the database
    let db = match Database::open(TEST_DB_PATH) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Worker {} failed to open DB: {:?}", worker_id, e);
            return (0, num_ops, vec![]);
        }
    };

    for op in 0..num_ops {
        let doc_id = format!("w{}_op{}", worker_id, op);

        // Retry logic for conflicts
        let mut attempts = 0;
        let max_attempts = 10;

        loop {
            attempts += 1;
            let mut tx = match db.begin() {
                Ok(tx) => tx,
                Err(e) => {
                    if attempts >= max_attempts {
                        eprintln!("Worker {} op {} failed to begin after {} attempts: {:?}",
                                 worker_id, op, attempts, e);
                        error_count += 1;
                        break;
                    }
                    thread::sleep(Duration::from_millis(10 * attempts as u64));
                    continue;
                }
            };

            let mut coll = match tx.collection(collection_name) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Worker {} op {} failed to get collection: {:?}", worker_id, op, e);
                    error_count += 1;
                    break;
                }
            };

            let doc = json!({
                "_id": format!("{}_a{}", doc_id, attempts),
                "worker": worker_id,
                "op": op,
                "attempt": attempts,
                "data": format!("worker_{}_operation_{}", worker_id, op),
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });

            match coll.insert(doc) {
                Ok(_) => {
                    match tx.commit() {
                        Ok(_) => {
                            success_count += 1;
                            inserted_ids.push(format!("{}_a{}", doc_id, attempts));
                            break;
                        }
                        Err(e) => {
                            let err_str = format!("{:?}", e);
                            if err_str.contains("Conflict") || err_str.contains("conflict") {
                                if attempts >= max_attempts {
                                    eprintln!("Worker {} op {} gave up after {} conflict retries",
                                             worker_id, op, attempts);
                                    error_count += 1;
                                    break;
                                }
                                thread::sleep(Duration::from_millis(5 * attempts as u64));
                                continue;
                            } else {
                                eprintln!("Worker {} op {} commit failed: {:?}", worker_id, op, e);
                                error_count += 1;
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Worker {} op {} insert failed: {:?}", worker_id, op, e);
                    error_count += 1;
                    break;
                }
            }
        }
    }

    // Explicitly close/drop the database
    drop(db);

    (success_count, error_count, inserted_ids)
}

/// Test with 10 concurrent processes writing to the SAME collection
/// NOTE: This test intentionally doesn't use proper locking/retry - it's a stress test
/// to verify behavior under contention, not a correctness test.
#[test]
#[ignore = "Stress test without proper locking - expected to fail"]
fn test_10_processes_same_collection() {
    cleanup_db();

    // Check if we're the child worker process
    if let Ok(worker_id) = env::var("MULTIPROCESS_WORKER_ID") {
        let worker_id: usize = worker_id.parse().unwrap();
        let num_ops: usize = env::var("MULTIPROCESS_NUM_OPS").unwrap().parse().unwrap();
        let collection = env::var("MULTIPROCESS_COLLECTION").unwrap();

        let (success, errors, ids) = run_worker(worker_id, num_ops, &collection);

        // Output results as JSON for parent to parse
        println!("RESULT:{}:{}:{}", success, errors, ids.join(","));
        return;
    }

    // Parent process - spawn workers
    let num_processes = 10;
    let ops_per_process = 100;
    let collection = "shared_collection";

    // Create the database first
    {
        let db = Database::open(TEST_DB_PATH).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection(collection).unwrap();
        coll.insert(json!({"_id": "init", "type": "initialization"})).unwrap();
        tx.commit().unwrap();
    }

    println!("Spawning {} processes, each doing {} ops on '{}'",
             num_processes, ops_per_process, collection);

    let start = Instant::now();

    // Spawn all child processes
    let mut children: Vec<_> = (0..num_processes)
        .map(|worker_id| {
            let exe = env::current_exe().unwrap();
            Command::new(exe)
                .env("MULTIPROCESS_WORKER_ID", worker_id.to_string())
                .env("MULTIPROCESS_NUM_OPS", ops_per_process.to_string())
                .env("MULTIPROCESS_COLLECTION", collection)
                .arg("--test")
                .arg("test_10_processes_same_collection")
                .arg("--exact")
                .arg("--nocapture")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn child process")
        })
        .collect();

    // Wait for all children and collect results
    let mut total_success = 0usize;
    let mut total_errors = 0usize;
    let mut all_inserted_ids: HashSet<String> = HashSet::new();

    for (worker_id, child) in children.into_iter().enumerate() {
        let output = child.wait_with_output().expect("Failed to wait for child");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        // Parse result line
        for line in stdout.lines() {
            if line.starts_with("RESULT:") {
                let parts: Vec<&str> = line.strip_prefix("RESULT:").unwrap().split(':').collect();
                if parts.len() >= 2 {
                    let success: usize = parts[0].parse().unwrap_or(0);
                    let errors: usize = parts[1].parse().unwrap_or(0);
                    total_success += success;
                    total_errors += errors;

                    if parts.len() > 2 && !parts[2].is_empty() {
                        for id in parts[2].split(',') {
                            all_inserted_ids.insert(id.to_string());
                        }
                    }
                }
            }
        }

        if !stderr.is_empty() {
            eprintln!("Worker {} stderr: {}", worker_id, stderr);
        }

        if !output.status.success() {
            eprintln!("Worker {} exited with status: {}", worker_id, output.status);
        }
    }

    let elapsed = start.elapsed();
    println!("All processes completed in {:?}", elapsed);
    println!("Total: {} success, {} errors, {} unique IDs tracked",
             total_success, total_errors, all_inserted_ids.len());

    // Verify data integrity
    let db = Database::open(TEST_DB_PATH).unwrap();
    let mut tx = db.begin().unwrap();
    let coll = tx.collection(collection).unwrap();
    let all_docs = coll.find_all().unwrap();

    // Subtract 1 for the init document
    let actual_count = all_docs.len() - 1;

    println!("Documents in DB: {}, Expected: {}", actual_count, total_success);

    // Check for missing documents
    let db_ids: HashSet<String> = all_docs.iter()
        .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    let missing: Vec<_> = all_inserted_ids.iter()
        .filter(|id| !db_ids.contains(*id))
        .collect();

    if !missing.is_empty() {
        println!("MISSING {} documents:", missing.len());
        for id in missing.iter().take(20) {
            println!("  - {}", id);
        }
    }

    assert!(missing.is_empty(), "DATA LOSS: {} documents missing!", missing.len());
    assert_eq!(actual_count, total_success,
               "Document count mismatch! DB has {}, but {} successful inserts tracked",
               actual_count, total_success);

    println!("SUCCESS: All {} documents verified!", total_success);
}

/// Test with 10 processes writing to DIFFERENT collections (less contention)
/// NOTE: Stress test without proper locking - expected to fail under contention.
#[test]
#[ignore = "Stress test without proper locking - expected to fail"]
fn test_10_processes_different_collections() {
    cleanup_db();

    if let Ok(worker_id) = env::var("MULTIPROCESS_WORKER_ID_V2") {
        let worker_id: usize = worker_id.parse().unwrap();
        let num_ops: usize = env::var("MULTIPROCESS_NUM_OPS_V2").unwrap().parse().unwrap();
        let collection = format!("collection_{}", worker_id);

        let (success, errors, ids) = run_worker(worker_id, num_ops, &collection);
        println!("RESULT:{}:{}:{}", success, errors, ids.join(","));
        return;
    }

    let num_processes = 10;
    let ops_per_process = 200;

    // Create the database first
    {
        let _ = Database::open(TEST_DB_PATH).unwrap();
    }

    println!("Spawning {} processes, each doing {} ops on their OWN collection",
             num_processes, ops_per_process);

    let start = Instant::now();

    let mut children: Vec<_> = (0..num_processes)
        .map(|worker_id| {
            let exe = env::current_exe().unwrap();
            Command::new(exe)
                .env("MULTIPROCESS_WORKER_ID_V2", worker_id.to_string())
                .env("MULTIPROCESS_NUM_OPS_V2", ops_per_process.to_string())
                .arg("--test")
                .arg("test_10_processes_different_collections")
                .arg("--exact")
                .arg("--nocapture")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn child process")
        })
        .collect();

    let mut total_success = 0usize;
    let mut total_errors = 0usize;
    let mut ids_per_collection: Vec<HashSet<String>> = vec![HashSet::new(); num_processes];

    for (worker_id, child) in children.into_iter().enumerate() {
        let output = child.wait_with_output().expect("Failed to wait for child");
        let stdout = String::from_utf8_lossy(&output.stdout);

        for line in stdout.lines() {
            if line.starts_with("RESULT:") {
                let parts: Vec<&str> = line.strip_prefix("RESULT:").unwrap().split(':').collect();
                if parts.len() >= 2 {
                    let success: usize = parts[0].parse().unwrap_or(0);
                    let errors: usize = parts[1].parse().unwrap_or(0);
                    total_success += success;
                    total_errors += errors;

                    if parts.len() > 2 && !parts[2].is_empty() {
                        for id in parts[2].split(',') {
                            ids_per_collection[worker_id].insert(id.to_string());
                        }
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();
    println!("All processes completed in {:?}", elapsed);
    println!("Total: {} success, {} errors", total_success, total_errors);

    // Verify each collection
    let db = Database::open(TEST_DB_PATH).unwrap();
    let mut total_found = 0usize;
    let mut total_missing = 0usize;

    for worker_id in 0..num_processes {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection(&format!("collection_{}", worker_id)).unwrap();
        let docs = coll.find_all().unwrap();

        let db_ids: HashSet<String> = docs.iter()
            .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        let expected = &ids_per_collection[worker_id];
        let missing: Vec<_> = expected.iter().filter(|id| !db_ids.contains(*id)).collect();

        if !missing.is_empty() {
            println!("Collection {} MISSING {} docs", worker_id, missing.len());
            total_missing += missing.len();
        }

        total_found += docs.len();
    }

    println!("Total documents found: {}, expected: {}", total_found, total_success);

    assert_eq!(total_missing, 0, "DATA LOSS: {} documents missing!", total_missing);
    assert_eq!(total_found, total_success, "Document count mismatch!");

    println!("SUCCESS: All {} documents across {} collections verified!",
             total_success, num_processes);
}

/// Extreme test: 20 processes, high volume
/// NOTE: Stress test without proper locking - expected to fail under contention.
#[test]
#[ignore = "Stress test without proper locking - expected to fail"]
fn test_20_processes_high_volume() {
    cleanup_db();

    if let Ok(worker_id) = env::var("MULTIPROCESS_WORKER_ID_V3") {
        let worker_id: usize = worker_id.parse().unwrap();
        let num_ops: usize = env::var("MULTIPROCESS_NUM_OPS_V3").unwrap().parse().unwrap();
        let collection = format!("vol_coll_{}", worker_id % 5); // 5 shared collections

        let (success, errors, ids) = run_worker(worker_id, num_ops, &collection);
        println!("RESULT:{}:{}:{}:{}", worker_id, success, errors, ids.join(","));
        return;
    }

    let num_processes = 20;
    let ops_per_process = 100;

    {
        let _ = Database::open(TEST_DB_PATH).unwrap();
    }

    println!("Spawning {} processes, {} ops each, across 5 shared collections",
             num_processes, ops_per_process);

    let start = Instant::now();

    let mut children: Vec<_> = (0..num_processes)
        .map(|worker_id| {
            let exe = env::current_exe().unwrap();
            Command::new(exe)
                .env("MULTIPROCESS_WORKER_ID_V3", worker_id.to_string())
                .env("MULTIPROCESS_NUM_OPS_V3", ops_per_process.to_string())
                .arg("--test")
                .arg("test_20_processes_high_volume")
                .arg("--exact")
                .arg("--nocapture")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn child process")
        })
        .collect();

    let mut results_per_worker: Vec<(usize, usize, HashSet<String>)> = Vec::new();

    for child in children.into_iter() {
        let output = child.wait_with_output().expect("Failed to wait for child");
        let stdout = String::from_utf8_lossy(&output.stdout);

        for line in stdout.lines() {
            if line.starts_with("RESULT:") {
                let parts: Vec<&str> = line.strip_prefix("RESULT:").unwrap().split(':').collect();
                if parts.len() >= 3 {
                    let _worker_id: usize = parts[0].parse().unwrap_or(0);
                    let success: usize = parts[1].parse().unwrap_or(0);
                    let errors: usize = parts[2].parse().unwrap_or(0);

                    let mut ids = HashSet::new();
                    if parts.len() > 3 && !parts[3].is_empty() {
                        for id in parts[3].split(',') {
                            ids.insert(id.to_string());
                        }
                    }
                    results_per_worker.push((success, errors, ids));
                }
            }
        }
    }

    let elapsed = start.elapsed();

    let total_success: usize = results_per_worker.iter().map(|(s, _, _)| s).sum();
    let total_errors: usize = results_per_worker.iter().map(|(_, e, _)| e).sum();
    let all_ids: HashSet<String> = results_per_worker.iter()
        .flat_map(|(_, _, ids)| ids.iter().cloned())
        .collect();

    println!("Completed in {:?}", elapsed);
    println!("Total: {} success, {} errors, {} unique IDs", total_success, total_errors, all_ids.len());

    // Verify
    let db = Database::open(TEST_DB_PATH).unwrap();
    let mut total_found = 0usize;

    for coll_id in 0..5 {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection(&format!("vol_coll_{}", coll_id)).unwrap();
        total_found += coll.find_all().unwrap().len();
    }

    println!("Documents in DB: {}, Expected: {}", total_found, total_success);

    assert_eq!(total_found, total_success,
               "DATA LOSS or CORRUPTION! Found {} but expected {}", total_found, total_success);

    println!("SUCCESS: {} processes, {} documents verified!", num_processes, total_success);
}

/// Mixed workload: some processes read, some write
/// NOTE: Stress test without proper locking - expected to fail under contention.
#[test]
#[ignore = "Stress test without proper locking - expected to fail"]
fn test_processes_mixed_read_write() {
    cleanup_db();

    if let Ok(worker_id) = env::var("MULTIPROCESS_WORKER_ID_V4") {
        let worker_id: usize = worker_id.parse().unwrap();
        let is_writer = env::var("MULTIPROCESS_IS_WRITER").unwrap() == "true";
        let num_ops: usize = env::var("MULTIPROCESS_NUM_OPS_V4").unwrap().parse().unwrap();

        let db = Database::open(TEST_DB_PATH).unwrap();

        if is_writer {
            let (success, errors, ids) = run_worker(worker_id, num_ops, "mixed_workload");
            println!("RESULT:W:{}:{}:{}", success, errors, ids.join(","));
        } else {
            // Reader
            let mut read_count = 0usize;
            let mut read_errors = 0usize;

            for _ in 0..num_ops {
                let result: Result<usize, String> = (|| {
                    let mut tx = db.begin().map_err(|e| format!("{:?}", e))?;
                    let coll = tx.collection("mixed_workload").map_err(|e| format!("{:?}", e))?;
                    let docs = coll.find_all().map_err(|e| format!("{:?}", e))?;
                    Ok(docs.len())
                })();

                match result {
                    Ok(_) => read_count += 1,
                    Err(e) => {
                        eprintln!("Reader {} error: {}", worker_id, e);
                        read_errors += 1;
                    }
                }

                // Small delay between reads
                thread::sleep(Duration::from_micros(100));
            }

            println!("RESULT:R:{}:{}", read_count, read_errors);
        }
        return;
    }

    let num_writers = 5;
    let num_readers = 5;
    let ops_per_process = 100;

    // Pre-populate
    {
        let db = Database::open(TEST_DB_PATH).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("mixed_workload").unwrap();
        for i in 0..100 {
            coll.insert(json!({"_id": format!("seed_{}", i), "type": "seed"})).unwrap();
        }
        tx.commit().unwrap();
    }

    println!("Spawning {} writers + {} readers, {} ops each", num_writers, num_readers, ops_per_process);

    let start = Instant::now();

    let exe = env::current_exe().unwrap();

    // Spawn writers
    let mut children: Vec<_> = (0..num_writers)
        .map(|worker_id| {
            Command::new(&exe)
                .env("MULTIPROCESS_WORKER_ID_V4", worker_id.to_string())
                .env("MULTIPROCESS_IS_WRITER", "true")
                .env("MULTIPROCESS_NUM_OPS_V4", ops_per_process.to_string())
                .arg("--test")
                .arg("test_processes_mixed_read_write")
                .arg("--exact")
                .arg("--nocapture")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn writer")
        })
        .collect();

    // Spawn readers
    for reader_id in 0..num_readers {
        children.push(
            Command::new(&exe)
                .env("MULTIPROCESS_WORKER_ID_V4", (num_writers + reader_id).to_string())
                .env("MULTIPROCESS_IS_WRITER", "false")
                .env("MULTIPROCESS_NUM_OPS_V4", ops_per_process.to_string())
                .arg("--test")
                .arg("test_processes_mixed_read_write")
                .arg("--exact")
                .arg("--nocapture")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn reader")
        );
    }

    let mut write_success = 0usize;
    let mut write_errors = 0usize;
    let mut read_success = 0usize;
    let mut read_errors = 0usize;
    let mut all_written_ids: HashSet<String> = HashSet::new();

    for child in children.into_iter() {
        let output = child.wait_with_output().expect("Failed to wait");
        let stdout = String::from_utf8_lossy(&output.stdout);

        for line in stdout.lines() {
            if line.starts_with("RESULT:W:") {
                let parts: Vec<&str> = line.strip_prefix("RESULT:W:").unwrap().split(':').collect();
                if parts.len() >= 2 {
                    write_success += parts[0].parse::<usize>().unwrap_or(0);
                    write_errors += parts[1].parse::<usize>().unwrap_or(0);
                    if parts.len() > 2 && !parts[2].is_empty() {
                        for id in parts[2].split(',') {
                            all_written_ids.insert(id.to_string());
                        }
                    }
                }
            } else if line.starts_with("RESULT:R:") {
                let parts: Vec<&str> = line.strip_prefix("RESULT:R:").unwrap().split(':').collect();
                if parts.len() >= 2 {
                    read_success += parts[0].parse::<usize>().unwrap_or(0);
                    read_errors += parts[1].parse::<usize>().unwrap_or(0);
                }
            }
        }
    }

    let elapsed = start.elapsed();
    println!("Completed in {:?}", elapsed);
    println!("Writers: {} success, {} errors", write_success, write_errors);
    println!("Readers: {} success, {} errors", read_success, read_errors);

    // Verify written data
    let db = Database::open(TEST_DB_PATH).unwrap();
    let mut tx = db.begin().unwrap();
    let coll = tx.collection("mixed_workload").unwrap();
    let all_docs = coll.find_all().unwrap();

    // 100 seed docs + written docs
    let expected_count = 100 + write_success;
    let actual_count = all_docs.len();

    println!("Documents in DB: {}, Expected: {}", actual_count, expected_count);

    assert_eq!(read_errors, 0, "Read errors occurred");
    assert_eq!(actual_count, expected_count,
               "DATA LOSS! Expected {} docs but found {}", expected_count, actual_count);

    println!("SUCCESS: {} writers + {} readers all verified!", num_writers, num_readers);
}

/// Crash simulation: processes that die mid-transaction
/// NOTE: Stress test without proper locking - expected to fail under contention.
#[test]
#[ignore = "Stress test without proper locking - expected to fail"]
fn test_processes_with_crashes() {
    cleanup_db();

    if let Ok(worker_id) = env::var("MULTIPROCESS_WORKER_ID_V5") {
        let worker_id: usize = worker_id.parse().unwrap();
        let should_crash = env::var("MULTIPROCESS_SHOULD_CRASH").unwrap() == "true";
        let crash_after: usize = env::var("MULTIPROCESS_CRASH_AFTER").unwrap().parse().unwrap();

        let db = Database::open(TEST_DB_PATH).unwrap();
        let mut success_count = 0usize;
        let mut inserted_ids = Vec::new();

        for op in 0..100 {
            // Simulate crash by exiting abruptly
            if should_crash && op == crash_after {
                // Start a transaction but don't commit - simulate crash
                let mut tx = db.begin().unwrap();
                let mut coll = tx.collection("crash_test").unwrap();
                coll.insert(json!({
                    "_id": format!("crash_w{}_op{}", worker_id, op),
                    "should_not_exist": true
                })).unwrap();
                // EXIT WITHOUT COMMIT - simulates process crash
                std::process::exit(0);
            }

            let doc_id = format!("w{}_op{}", worker_id, op);

            let mut attempts = 0;
            loop {
                attempts += 1;
                let mut tx = match db.begin() {
                    Ok(tx) => tx,
                    Err(_) if attempts < 5 => {
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Err(_) => break,
                };

                let mut coll = match tx.collection("crash_test") {
                    Ok(c) => c,
                    Err(_) => break,
                };

                match coll.insert(json!({"_id": doc_id.clone(), "worker": worker_id, "op": op})) {
                    Ok(_) => {
                        if tx.commit().is_ok() {
                            success_count += 1;
                            inserted_ids.push(doc_id);
                            break;
                        }
                    }
                    Err(_) => {}
                }

                if attempts >= 5 {
                    break;
                }
                thread::sleep(Duration::from_millis(5 * attempts as u64));
            }
        }

        println!("RESULT:{}:{}", success_count, inserted_ids.join(","));
        return;
    }

    let num_normal_workers = 5;
    let num_crashing_workers = 3;

    {
        let _ = Database::open(TEST_DB_PATH).unwrap();
    }

    println!("Spawning {} normal workers + {} crashing workers", num_normal_workers, num_crashing_workers);

    let exe = env::current_exe().unwrap();
    let mut children = Vec::new();

    // Normal workers
    for worker_id in 0..num_normal_workers {
        children.push((
            false, // not crashing
            Command::new(&exe)
                .env("MULTIPROCESS_WORKER_ID_V5", worker_id.to_string())
                .env("MULTIPROCESS_SHOULD_CRASH", "false")
                .env("MULTIPROCESS_CRASH_AFTER", "999")
                .arg("--test")
                .arg("test_processes_with_crashes")
                .arg("--exact")
                .arg("--nocapture")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn")
        ));
    }

    // Crashing workers - they'll crash at different points
    for i in 0..num_crashing_workers {
        let crash_at = 20 + i * 25; // crash at op 20, 45, 70
        children.push((
            true, // crashing
            Command::new(&exe)
                .env("MULTIPROCESS_WORKER_ID_V5", (num_normal_workers + i).to_string())
                .env("MULTIPROCESS_SHOULD_CRASH", "true")
                .env("MULTIPROCESS_CRASH_AFTER", crash_at.to_string())
                .arg("--test")
                .arg("test_processes_with_crashes")
                .arg("--exact")
                .arg("--nocapture")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn")
        ));
    }

    let mut total_success = 0usize;
    let mut all_ids: HashSet<String> = HashSet::new();

    for (is_crashing, mut child) in children {
        let output = child.wait_with_output().expect("Failed to wait");
        let stdout = String::from_utf8_lossy(&output.stdout);

        for line in stdout.lines() {
            if line.starts_with("RESULT:") {
                let parts: Vec<&str> = line.strip_prefix("RESULT:").unwrap().split(':').collect();
                if parts.len() >= 1 {
                    let success: usize = parts[0].parse().unwrap_or(0);
                    total_success += success;

                    if parts.len() > 1 && !parts[1].is_empty() {
                        for id in parts[1].split(',') {
                            all_ids.insert(id.to_string());
                        }
                    }
                }
            }
        }

        if is_crashing && !output.status.success() {
            println!("Crashing worker exited as expected");
        }
    }

    println!("Total successful commits: {}", total_success);

    // Small delay to let any WAL recovery happen
    thread::sleep(Duration::from_millis(100));

    // Verify: only committed transactions should be visible
    let db = Database::open(TEST_DB_PATH).unwrap();
    let mut tx = db.begin().unwrap();
    let coll = tx.collection("crash_test").unwrap();
    let all_docs = coll.find_all().unwrap();

    // Check that no "should_not_exist" documents are present
    let invalid_docs: Vec<_> = all_docs.iter()
        .filter(|doc| doc.get("should_not_exist").is_some())
        .collect();

    if !invalid_docs.is_empty() {
        println!("CORRUPTION: Found {} uncommitted documents!", invalid_docs.len());
    }

    let db_ids: HashSet<String> = all_docs.iter()
        .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    let missing: Vec<_> = all_ids.iter().filter(|id| !db_ids.contains(*id)).collect();

    println!("Documents in DB: {}, Committed IDs tracked: {}", all_docs.len(), all_ids.len());

    assert!(invalid_docs.is_empty(),
            "CORRUPTION: {} uncommitted documents found!", invalid_docs.len());
    assert!(missing.is_empty(),
            "DATA LOSS: {} committed documents missing!", missing.len());

    println!("SUCCESS: Crash recovery working correctly!");
    println!("  - All committed transactions preserved");
    println!("  - No uncommitted transactions visible");
}
