//! Investigation test to find where data loss occurs
//! This test adds detailed logging to track exactly where commits succeed but data is lost

use std::collections::HashSet;
use std::env;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;
use std::fs;
use std::io::Write;

use jasonisnthappy::Database;
use serde_json::json;

const TEST_DB_PATH: &str = "/tmp/data_loss_investigation.db";
const LOG_DIR: &str = "/tmp/data_loss_logs";

fn cleanup() {
    let _ = fs::remove_file(TEST_DB_PATH);
    let _ = fs::remove_file(format!("{}.lock", TEST_DB_PATH));
    let _ = fs::remove_file(format!("{}-wal", TEST_DB_PATH));
    let _ = fs::remove_dir_all(LOG_DIR);
    let _ = fs::create_dir_all(LOG_DIR);
}

/// Detailed worker with comprehensive logging
fn run_logged_worker(worker_id: usize, num_ops: usize) -> Vec<String> {
    let log_path = format!("{}/worker_{}.log", LOG_DIR, worker_id);
    let mut log_file = fs::File::create(&log_path).unwrap();

    writeln!(log_file, "Worker {} starting, {} ops", worker_id, num_ops).unwrap();

    // Stagger
    thread::sleep(Duration::from_millis((worker_id * 50) as u64));

    let mut committed_ids = Vec::new();
    let mut current_op = 0;

    while current_op < num_ops {
        writeln!(log_file, "Op {}: Attempting to open DB", current_op).unwrap();

        // Try to open with retries
        let db = {
            let mut attempts = 0;
            loop {
                match Database::open(TEST_DB_PATH) {
                    Ok(db) => {
                        writeln!(log_file, "Op {}: DB opened on attempt {}", current_op, attempts + 1).unwrap();
                        break Some(db);
                    }
                    Err(e) => {
                        attempts += 1;
                        writeln!(log_file, "Op {}: Open failed (attempt {}): {:?}", current_op, attempts, e).unwrap();
                        if attempts >= 30 {
                            break None;
                        }
                        thread::sleep(Duration::from_millis(20 * attempts as u64));
                    }
                }
            }
        };

        let db = match db {
            Some(db) => db,
            None => {
                writeln!(log_file, "Op {}: GAVE UP opening DB", current_op).unwrap();
                current_op += 1;
                continue;
            }
        };

        // Begin transaction
        writeln!(log_file, "Op {}: Beginning transaction", current_op).unwrap();
        let mut tx = match db.begin() {
            Ok(tx) => tx,
            Err(e) => {
                writeln!(log_file, "Op {}: Begin failed: {:?}", current_op, e).unwrap();
                drop(db);
                current_op += 1;
                continue;
            }
        };

        // Get collection
        let mut coll = match tx.collection("investigation") {
            Ok(c) => c,
            Err(e) => {
                writeln!(log_file, "Op {}: Collection failed: {:?}", current_op, e).unwrap();
                drop(tx);
                drop(db);
                current_op += 1;
                continue;
            }
        };

        // Insert document
        let doc_id = format!("w{}_op{}", worker_id, current_op);
        let doc = json!({
            "_id": doc_id.clone(),
            "worker": worker_id,
            "op": current_op
        });

        writeln!(log_file, "Op {}: Inserting doc {}", current_op, doc_id).unwrap();
        if let Err(e) = coll.insert(doc) {
            writeln!(log_file, "Op {}: Insert failed: {:?}", current_op, e).unwrap();
            drop(coll);
            drop(tx);
            drop(db);
            current_op += 1;
            continue;
        }

        // Commit
        writeln!(log_file, "Op {}: Committing", current_op).unwrap();
        drop(coll); // Drop collection reference before commit

        match tx.commit() {
            Ok(_) => {
                writeln!(log_file, "Op {}: COMMIT SUCCESS for {}", current_op, doc_id).unwrap();
                committed_ids.push(doc_id.clone());
            }
            Err(e) => {
                writeln!(log_file, "Op {}: COMMIT FAILED: {:?}", current_op, e).unwrap();
            }
        }

        // Verify immediately after commit (while we still have the lock)
        writeln!(log_file, "Op {}: Verifying commit", current_op).unwrap();
        let verify_tx = db.begin();
        if let Ok(mut vtx) = verify_tx {
            if let Ok(vcoll) = vtx.collection("investigation") {
                match vcoll.find_by_id(&doc_id) {
                    Ok(doc) => {
                        if doc.get("_id").is_some() {
                            writeln!(log_file, "Op {}: VERIFIED - doc {} found in DB", current_op, doc_id).unwrap();
                        } else {
                            writeln!(log_file, "Op {}: WARNING - doc {} NOT FOUND after commit!", current_op, doc_id).unwrap();
                        }
                    }
                    Err(e) => {
                        writeln!(log_file, "Op {}: VERIFY ERROR: {:?}", current_op, e).unwrap();
                    }
                }
            }
        }

        // Close database
        writeln!(log_file, "Op {}: Closing DB", current_op).unwrap();
        if let Err(e) = db.close() {
            writeln!(log_file, "Op {}: Close error: {:?}", current_op, e).unwrap();
        }
        writeln!(log_file, "Op {}: DB closed", current_op).unwrap();

        current_op += 1;

        // Brief delay
        thread::sleep(Duration::from_millis(10));
    }

    writeln!(log_file, "Worker {} finished. Committed {} docs: {:?}",
             worker_id, committed_ids.len(), committed_ids).unwrap();

    committed_ids
}

#[test]
fn test_investigate_data_loss() {
    cleanup();

    if let Ok(worker_id) = env::var("INVESTIGATE_WORKER_ID") {
        let worker_id: usize = worker_id.parse().unwrap();
        let num_ops: usize = env::var("INVESTIGATE_NUM_OPS").unwrap().parse().unwrap();

        let ids = run_logged_worker(worker_id, num_ops);
        println!("RESULT:{}", ids.join(","));
        return;
    }

    // Initialize DB
    {
        let db = Database::open(TEST_DB_PATH).unwrap();
        db.close().unwrap();
    }

    let num_processes = 3;
    let ops_per_process = 10;

    println!("Starting investigation with {} processes, {} ops each", num_processes, ops_per_process);
    println!("Logs will be written to {}", LOG_DIR);

    let exe = env::current_exe().unwrap();

    let children: Vec<_> = (0..num_processes)
        .map(|worker_id| {
            Command::new(&exe)
                .env("INVESTIGATE_WORKER_ID", worker_id.to_string())
                .env("INVESTIGATE_NUM_OPS", ops_per_process.to_string())
                .arg("--test")
                .arg("test_investigate_data_loss")
                .arg("--exact")
                .arg("--nocapture")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("Failed to spawn")
        })
        .collect();

    let mut all_committed: HashSet<String> = HashSet::new();

    for child in children.into_iter() {
        let output = child.wait_with_output().expect("Failed to wait");
        let stdout = String::from_utf8_lossy(&output.stdout);

        for line in stdout.lines() {
            if line.starts_with("RESULT:") {
                let ids_str = line.strip_prefix("RESULT:").unwrap();
                if !ids_str.is_empty() {
                    for id in ids_str.split(',') {
                        all_committed.insert(id.to_string());
                    }
                }
            }
        }
    }

    println!("\n=== RESULTS ===");
    println!("Workers reported {} committed documents", all_committed.len());

    // Final verification
    let db = Database::open(TEST_DB_PATH).unwrap();
    let mut tx = db.begin().unwrap();
    let coll = tx.collection("investigation").unwrap();
    let all_docs = coll.find_all().unwrap();

    let db_ids: HashSet<String> = all_docs.iter()
        .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    println!("Documents actually in DB: {}", db_ids.len());

    let missing: Vec<_> = all_committed.iter()
        .filter(|id| !db_ids.contains(*id))
        .collect();

    if !missing.is_empty() {
        println!("\n!!! DATA LOSS DETECTED !!!");
        println!("Missing {} documents:", missing.len());
        for id in &missing {
            println!("  - {}", id);

            // Check if we can find this in the logs
            for worker_id in 0..num_processes {
                let log_path = format!("{}/worker_{}.log", LOG_DIR, worker_id);
                if let Ok(log_content) = fs::read_to_string(&log_path) {
                    if log_content.contains(&format!("COMMIT SUCCESS for {}", id)) {
                        println!("    ^ Worker {} reported COMMIT SUCCESS for this doc!", worker_id);
                        // Find if verification passed
                        if log_content.contains(&format!("VERIFIED - doc {} found", id)) {
                            println!("    ^ Worker {} VERIFIED the doc after commit!", worker_id);
                        }
                    }
                }
            }
        }
    } else {
        println!("\nNo data loss detected!");
    }

    // Print logs for analysis
    println!("\n=== WORKER LOGS ===");
    for worker_id in 0..num_processes {
        let log_path = format!("{}/worker_{}.log", LOG_DIR, worker_id);
        if let Ok(content) = fs::read_to_string(&log_path) {
            println!("\n--- Worker {} ---", worker_id);
            println!("{}", content);
        }
    }

    assert!(missing.is_empty(), "DATA LOSS: {} documents missing!", missing.len());
}
