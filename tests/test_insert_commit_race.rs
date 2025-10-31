// Minimal reproducer focusing on INSERT operations
// Hypothesis: tx.commit() returns Ok but document isn't actually persisted

use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::TempDir;

#[test]
fn test_insert_commit_verification() {
    const RUNS: usize = 100;
    const WORKERS: usize = 2;
    const INSERTS_PER_WORKER: usize = 50;

    println!("\n=== Testing INSERT + COMMIT reliability ({} runs) ===\n", RUNS);

    let mut failures = 0;

    for run in 0..RUNS {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join(format!("insert_test_{}.db", run));
        let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

        // Track which documents SHOULD be in DB (commit returned Ok)
        let committed_docs = Arc::new(Mutex::new(Vec::new()));

        let handles: Vec<_> = (0..WORKERS)
            .map(|worker_id| {
                let db = Arc::clone(&db);
                let committed_docs = Arc::clone(&committed_docs);

                thread::spawn(move || {
                    for i in 0..INSERTS_PER_WORKER {
                        let doc_id = format!("w{}_d{}", worker_id, i);
                        let doc = json!({
                            "_id": doc_id.clone(),
                            "worker": worker_id,
                            "seq": i,
                        });

                        let mut tx = db.begin().unwrap();
                        let mut collection = tx.collection("test").unwrap();

                        if collection.insert(doc).is_ok() {
                            match tx.commit() {
                                Ok(_) => {
                                    // Commit succeeded - document SHOULD be in DB
                                    let mut docs = committed_docs.lock().unwrap();
                                    docs.push(doc_id.clone());
                                }
                                Err(e) => {
                                    eprintln!("RUN{} W{} I{}: Commit failed: {}", run, worker_id, i, e);
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

        // Verify: all committed documents should be in DB
        let committed = committed_docs.lock().unwrap();
        let mut tx = db.begin().unwrap();
        let collection = tx.collection("test").unwrap();
        let db_docs = collection.find_all().unwrap();

        let db_ids: Vec<String> = db_docs
            .iter()
            .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        let missing: Vec<_> = committed
            .iter()
            .filter(|id| !db_ids.contains(id))
            .collect();

        if !missing.is_empty() {
            failures += 1;
            println!("\n!!! FAILURE on run {} !!!", run);
            println!("Committed {} docs, but only {} in DB", committed.len(), db_ids.len());
            println!("Missing {} documents:", missing.len());
            for id in &missing {
                println!("  - {}", id);
            }

            // Show what IS in the DB
            println!("\nDocuments in DB:");
            for id in &db_ids {
                println!("  + {}", id);
            }
        } else if run % 20 == 0 {
            println!("Run {}: PASS ({} docs)", run, committed.len());
        }
    }

    println!("\n=== SUMMARY ===");
    println!("Runs: {}, Failures: {}, Rate: {:.1}%", RUNS, failures, failures as f64 / RUNS as f64 * 100.0);

    if failures > 0 {
        panic!("INSERT+COMMIT race detected! {} failures out of {} runs", failures, RUNS);
    }
}

#[test]
fn test_concurrent_insert_with_immediate_verification() {
    // Simpler test: Each worker inserts, then IMMEDIATELY checks if doc is in DB
    const WORKERS: usize = 4;
    const INSERTS_PER_WORKER: usize = 25;

    println!("\n=== Testing concurrent INSERT with immediate verification ===\n");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("immediate_verify.db");
    let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

    let failures = Arc::new(Mutex::new(Vec::new()));

    let handles: Vec<_> = (0..WORKERS)
        .map(|worker_id| {
            let db = Arc::clone(&db);
            let failures = Arc::clone(&failures);

            thread::spawn(move || {
                for i in 0..INSERTS_PER_WORKER {
                    let doc_id = format!("worker_{}_doc_{}", worker_id, i);
                    let doc = json!({
                        "_id": doc_id.clone(),
                        "worker": worker_id,
                        "iteration": i,
                    });

                    // INSERT
                    let mut tx = db.begin().unwrap();
                    let mut collection = tx.collection("test").unwrap();
                    collection.insert(doc).unwrap();

                    match tx.commit() {
                        Ok(_) => {
                            // Commit succeeded - verify document is actually in DB
                            let mut verify_tx = db.begin().unwrap();
                            let verify_collection = verify_tx.collection("test").unwrap();

                            match verify_collection.find_by_id(&doc_id) {
                                Ok(_) => {
                                    // Good - document found
                                }
                                Err(_) => {
                                    // Document NOT FOUND after successful commit!
                                    let mut fails = failures.lock().unwrap();
                                    fails.push(format!("W{} I{}: {} committed but NOT FOUND immediately after!",
                                        worker_id, i, doc_id));
                                }
                            }
                        }
                        Err(e) => {
                            println!("W{} I{}: Commit failed (expected): {}", worker_id, i, e);
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let fails = failures.lock().unwrap();
    if !fails.is_empty() {
        println!("\n!!! FAILURES DETECTED !!!");
        for fail in fails.iter() {
            println!("  {}", fail);
        }
        panic!("{} verification failures detected", fails.len());
    } else {
        println!("✓ All {} inserts verified successfully", WORKERS * INSERTS_PER_WORKER);
    }
}
