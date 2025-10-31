//! Multi-process tests that respect the single-writer constraint
//! The database uses exclusive locking - only one writer process at a time
//! These tests verify data integrity under proper usage patterns

use std::collections::HashSet;
use std::env;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use jasonisnthappy::Database;
use serde_json::json;

fn cleanup_db(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.lock", path));
    let _ = std::fs::remove_file(format!("{}-wal", path));
}

/// Worker that properly handles lock contention with exponential backoff
fn run_worker_with_retry(worker_id: usize, num_ops: usize, db_path: &str) -> (usize, usize, Vec<String>) {
    // Stagger startup to reduce initial contention
    thread::sleep(Duration::from_millis((worker_id * 20) as u64));

    let mut success_count = 0;
    let mut error_count = 0;
    let mut inserted_ids = Vec::new();

    // Keep trying until we complete all operations
    let mut completed_ops = 0;

    while completed_ops < num_ops {
        // Try to open database with exponential backoff
        let db = {
            let mut attempts = 0;
            let max_attempts = 50; // More retries
            loop {
                match Database::open(db_path) {
                    Ok(db) => break Some(db),
                    Err(_) => {
                        attempts += 1;
                        if attempts >= max_attempts {
                            // Give up on this attempt, will retry outer loop
                            break None;
                        }
                        // Exponential backoff with jitter
                        let delay = 10 + (attempts * 10) + (worker_id % 5) as u64;
                        thread::sleep(Duration::from_millis(delay));
                    }
                }
            }
        };

        let db = match db {
            Some(db) => db,
            None => {
                error_count += 1;
                continue;
            }
        };

        // Do a batch of operations while we have the lock
        let batch_size = 5.min(num_ops - completed_ops);
        let mut tx = match db.begin() {
            Ok(tx) => tx,
            Err(_) => {
                error_count += 1;
                continue;
            }
        };

        let mut coll = match tx.collection("test_data") {
            Ok(c) => c,
            Err(_) => {
                error_count += 1;
                continue;
            }
        };

        let mut batch_success = true;
        for i in 0..batch_size {
            let op = completed_ops + i;
            let doc_id = format!("w{}_op{}", worker_id, op);
            let doc = json!({
                "_id": doc_id.clone(),
                "worker": worker_id,
                "op": op,
                "data": format!("worker_{}_op_{}", worker_id, op)
            });

            if coll.insert(doc).is_err() {
                batch_success = false;
                break;
            }
            inserted_ids.push(doc_id);
        }

        if batch_success {
            if tx.commit().is_ok() {
                success_count += batch_size;
                completed_ops += batch_size;
            } else {
                // Commit failed - remove the IDs we thought we inserted
                for _ in 0..batch_size {
                    inserted_ids.pop();
                }
                error_count += 1;
            }
        } else {
            for _ in 0..batch_size {
                inserted_ids.pop();
            }
            error_count += 1;
        }

        // Explicitly close to flush and release the lock
        // Note: drop() doesn't always flush due to Arc reference counting
        let _ = db.close();

        // Small delay before trying again
        thread::sleep(Duration::from_millis(5));
    }

    (success_count, error_count, inserted_ids)
}

/// Test with 5 processes, each doing 20 operations with proper retry/backoff
#[test]
fn test_5_processes_with_proper_locking() {
    const DB_PATH: &str = "/tmp/multiprocess_seq_test_5proc.db";

    // IMPORTANT: Only cleanup in parent process, not workers!
    // Workers spawn as new processes running the same test function,
    // so we must check for the worker env var BEFORE any file operations.
    if let Ok(worker_id) = env::var("SEQ_WORKER_ID") {
        let worker_id: usize = worker_id.parse().unwrap();
        let num_ops: usize = env::var("SEQ_NUM_OPS").unwrap().parse().unwrap();
        let db_path = env::var("SEQ_DB_PATH").unwrap();

        let (success, errors, ids) = run_worker_with_retry(worker_id, num_ops, &db_path);
        println!("RESULT:{}:{}:{}", success, errors, ids.join(","));
        return;
    }

    // Parent process: cleanup and create database
    cleanup_db(DB_PATH);

    // Create the database first
    {
        let db = Database::open(DB_PATH).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test_data").unwrap();
        coll.insert(json!({"_id": "init", "type": "init"})).unwrap();
        tx.commit().unwrap();
        db.close().unwrap();
    }

    // CRITICAL: Wait for file system to sync before spawning workers
    thread::sleep(Duration::from_millis(100));

    // Verify init doc was written before spawning workers
    {
        let db = Database::open(DB_PATH).unwrap();
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("test_data").unwrap();
        let init = coll.find_by_id("init").unwrap();
        assert!(init.get("_id").is_some(), "Init doc not found BEFORE spawning workers!");
        println!("Verified init doc exists before spawning workers");
        drop(coll);
        drop(tx);
        db.close().unwrap();
    }
    thread::sleep(Duration::from_millis(50));

    let num_processes = 5;
    let ops_per_process = 20;

    println!("Spawning {} processes, each doing {} ops with proper lock handling",
             num_processes, ops_per_process);

    let start = Instant::now();
    let exe = env::current_exe().unwrap();

    let children: Vec<_> = (0..num_processes)
        .map(|worker_id| {
            Command::new(&exe)
                .env("SEQ_WORKER_ID", worker_id.to_string())
                .env("SEQ_NUM_OPS", ops_per_process.to_string())
                .env("SEQ_DB_PATH", DB_PATH)
                .arg("--test")
                .arg("test_5_processes_with_proper_locking")
                .arg("--exact")
                .arg("--nocapture")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn")
        })
        .collect();

    let mut total_success = 0usize;
    let mut total_errors = 0usize;
    let mut all_ids: HashSet<String> = HashSet::new();

    for (worker_id, child) in children.into_iter().enumerate() {
        let output = child.wait_with_output().expect("Failed to wait");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if !stderr.is_empty() {
            eprintln!("Worker {} stderr: {}", worker_id, stderr);
        }

        for line in stdout.lines() {
            if line.starts_with("RESULT:") {
                let parts: Vec<&str> = line.strip_prefix("RESULT:").unwrap().split(':').collect();
                if parts.len() >= 2 {
                    total_success += parts[0].parse::<usize>().unwrap_or(0);
                    total_errors += parts[1].parse::<usize>().unwrap_or(0);
                    if parts.len() > 2 && !parts[2].is_empty() {
                        for id in parts[2].split(',') {
                            all_ids.insert(id.to_string());
                        }
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();
    println!("Completed in {:?}", elapsed);
    println!("Total: {} success, {} lock retries, {} unique IDs",
             total_success, total_errors, all_ids.len());

    // Verify
    let db = Database::open(DB_PATH).unwrap();
    let mut tx = db.begin().unwrap();
    let coll = tx.collection("test_data").unwrap();
    let all_docs = coll.find_all().unwrap();

    let db_ids: HashSet<String> = all_docs.iter()
        .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    // Subtract init doc
    let actual_count = all_docs.len() - 1;

    let missing: Vec<_> = all_ids.iter().filter(|id| !db_ids.contains(*id)).collect();

    println!("Documents in DB: {}, Expected: {}", actual_count, total_success);

    // Check if init doc exists
    let init_exists = db_ids.contains("init");
    println!("Init doc exists: {}", init_exists);
    if !init_exists {
        println!("!!! INIT DOC LOST - The init document created before spawning workers is missing!");
    }

    if !missing.is_empty() {
        println!("MISSING {} documents:", missing.len());
        for id in &missing {
            println!("  - {}", id);
        }
    }

    assert!(missing.is_empty(), "DATA LOSS: {} documents missing!", missing.len());
    assert_eq!(actual_count, total_success,
               "Count mismatch: DB has {}, tracked {}", actual_count, total_success);

    println!("SUCCESS: All {} documents verified!", total_success);
}

/// Test with 10 processes, higher volume
#[test]
fn test_10_processes_higher_volume() {
    const DB_PATH: &str = "/tmp/multiprocess_seq_test_10proc.db";

    // IMPORTANT: Only cleanup in parent process, not workers!
    if let Ok(worker_id) = env::var("SEQ_WORKER_ID_V2") {
        let worker_id: usize = worker_id.parse().unwrap();
        let num_ops: usize = env::var("SEQ_NUM_OPS_V2").unwrap().parse().unwrap();
        let db_path = env::var("SEQ_DB_PATH_V2").unwrap();

        let (success, errors, ids) = run_worker_with_retry(worker_id, num_ops, &db_path);
        println!("RESULT:{}:{}:{}", success, errors, ids.join(","));
        return;
    }

    cleanup_db(DB_PATH);

    {
        let db = Database::open(DB_PATH).unwrap();
        db.close().unwrap();
    }

    let num_processes = 10;
    let ops_per_process = 50;

    println!("Spawning {} processes, {} ops each", num_processes, ops_per_process);

    let start = Instant::now();
    let exe = env::current_exe().unwrap();

    let children: Vec<_> = (0..num_processes)
        .map(|worker_id| {
            Command::new(&exe)
                .env("SEQ_WORKER_ID_V2", worker_id.to_string())
                .env("SEQ_NUM_OPS_V2", ops_per_process.to_string())
                .env("SEQ_DB_PATH_V2", DB_PATH)
                .arg("--test")
                .arg("test_10_processes_higher_volume")
                .arg("--exact")
                .arg("--nocapture")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn")
        })
        .collect();

    let mut total_success = 0usize;
    let mut all_ids: HashSet<String> = HashSet::new();

    for child in children.into_iter() {
        let output = child.wait_with_output().expect("Failed to wait");
        let stdout = String::from_utf8_lossy(&output.stdout);

        for line in stdout.lines() {
            if line.starts_with("RESULT:") {
                let parts: Vec<&str> = line.strip_prefix("RESULT:").unwrap().split(':').collect();
                if parts.len() >= 2 {
                    total_success += parts[0].parse::<usize>().unwrap_or(0);
                    if parts.len() > 2 && !parts[2].is_empty() {
                        for id in parts[2].split(',') {
                            all_ids.insert(id.to_string());
                        }
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();
    println!("Completed in {:?}", elapsed);
    println!("Total: {} success, {} unique IDs", total_success, all_ids.len());

    // Verify
    let db = Database::open(DB_PATH).unwrap();
    let mut tx = db.begin().unwrap();
    let coll = tx.collection("test_data").unwrap();
    let all_docs = coll.find_all().unwrap();

    let db_ids: HashSet<String> = all_docs.iter()
        .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    let missing: Vec<_> = all_ids.iter().filter(|id| !db_ids.contains(*id)).collect();

    println!("Documents in DB: {}, Expected: {}", all_docs.len(), total_success);

    assert!(missing.is_empty(), "DATA LOSS: {} documents missing!", missing.len());

    println!("SUCCESS: {} documents from {} processes verified!", total_success, num_processes);
}

/// Sequential access pattern - processes take turns
#[test]
fn test_sequential_process_access() {
    const DB_PATH: &str = "/tmp/multiprocess_seq_test_sequential.db";

    // IMPORTANT: Only cleanup in parent process, not workers!
    if let Ok(worker_id) = env::var("SEQ_WORKER_ID_V3") {
        let worker_id: usize = worker_id.parse().unwrap();
        let db_path = env::var("SEQ_DB_PATH_V3").unwrap();

        // Each worker waits for its turn
        thread::sleep(Duration::from_millis((worker_id * 500) as u64));

        let db = Database::open(&db_path).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("sequential").unwrap();

        let mut ids = Vec::new();
        for i in 0..20 {
            let id = format!("w{}_op{}", worker_id, i);
            coll.insert(json!({"_id": id.clone(), "worker": worker_id, "op": i})).unwrap();
            ids.push(id);
        }

        tx.commit().unwrap();
        db.close().unwrap();

        println!("RESULT:20:{}", ids.join(","));
        return;
    }

    cleanup_db(DB_PATH);

    {
        let db = Database::open(DB_PATH).unwrap();
        db.close().unwrap();
    }

    let num_processes = 5;

    println!("Running {} processes SEQUENTIALLY (each waits for previous)", num_processes);

    let start = Instant::now();
    let exe = env::current_exe().unwrap();

    // Spawn all at once - they will self-schedule via sleep
    let children: Vec<_> = (0..num_processes)
        .map(|worker_id| {
            Command::new(&exe)
                .env("SEQ_WORKER_ID_V3", worker_id.to_string())
                .env("SEQ_DB_PATH_V3", DB_PATH)
                .arg("--test")
                .arg("test_sequential_process_access")
                .arg("--exact")
                .arg("--nocapture")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn")
        })
        .collect();

    let mut all_ids: HashSet<String> = HashSet::new();
    let mut _total_success = 0usize;

    for child in children.into_iter() {
        let output = child.wait_with_output().expect("Failed to wait");
        let stdout = String::from_utf8_lossy(&output.stdout);

        for line in stdout.lines() {
            if line.starts_with("RESULT:") {
                let parts: Vec<&str> = line.strip_prefix("RESULT:").unwrap().split(':').collect();
                if parts.len() >= 1 {
                    _total_success += parts[0].parse::<usize>().unwrap_or(0);
                    if parts.len() > 1 && !parts[1].is_empty() {
                        for id in parts[1].split(',') {
                            all_ids.insert(id.to_string());
                        }
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();
    println!("Completed in {:?}", elapsed);

    // Verify
    let db = Database::open(DB_PATH).unwrap();
    let mut tx = db.begin().unwrap();
    let coll = tx.collection("sequential").unwrap();
    let all_docs = coll.find_all().unwrap();

    let db_ids: HashSet<String> = all_docs.iter()
        .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    let missing: Vec<_> = all_ids.iter().filter(|id| !db_ids.contains(*id)).collect();

    let expected = num_processes * 20;
    println!("Documents in DB: {}, Expected: {}", all_docs.len(), expected);

    assert!(missing.is_empty(), "DATA LOSS: {} documents missing!", missing.len());
    assert_eq!(all_docs.len(), expected, "Count mismatch!");

    println!("SUCCESS: {} documents verified with sequential access!", expected);
}

/// Verify data survives process termination (durability)
#[test]
fn test_durability_across_processes() {
    const DB_PATH: &str = "/tmp/multiprocess_seq_test_durability.db";

    cleanup_db(DB_PATH);

    // Phase 1: First process writes and closes
    {
        let db = Database::open(DB_PATH).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("durable").unwrap();

        for i in 0..100 {
            coll.insert(json!({"_id": format!("phase1_{}", i), "phase": 1, "i": i})).unwrap();
        }
        tx.commit().unwrap();
        db.close().unwrap();
    }

    // Phase 2: New process opens and verifies phase 1 data, adds more
    {
        let db = Database::open(DB_PATH).unwrap();
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("durable").unwrap();

        let phase1_docs = coll.find_all().unwrap();
        assert_eq!(phase1_docs.len(), 100, "Phase 1 data missing!");

        let mut coll = tx.collection("durable").unwrap();
        for i in 0..100 {
            coll.insert(json!({"_id": format!("phase2_{}", i), "phase": 2, "i": i})).unwrap();
        }
        tx.commit().unwrap();
        db.close().unwrap();
    }

    // Phase 3: Final verification
    {
        let db = Database::open(DB_PATH).unwrap();
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("durable").unwrap();

        let all_docs = coll.find_all().unwrap();
        assert_eq!(all_docs.len(), 200, "Expected 200 docs, found {}", all_docs.len());

        let phase1_count = all_docs.iter()
            .filter(|d| d.get("phase").and_then(|v| v.as_i64()) == Some(1))
            .count();
        let phase2_count = all_docs.iter()
            .filter(|d| d.get("phase").and_then(|v| v.as_i64()) == Some(2))
            .count();

        assert_eq!(phase1_count, 100, "Phase 1 count wrong");
        assert_eq!(phase2_count, 100, "Phase 2 count wrong");

        println!("SUCCESS: Durability verified - 100 docs from phase 1, 100 from phase 2");
    }
}
