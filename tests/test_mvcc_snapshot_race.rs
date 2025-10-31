// Test if MVCC snapshot isolation could cause "missing documents" in verification
// Hypothesis: Final verification transaction might see an old snapshot

use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

#[test]
fn test_snapshot_sees_all_committed_data() {
    const WORKERS: usize = 4;
    const INSERTS_PER_WORKER: usize = 50;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("snapshot_test.db");
    let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

    // Barrier to ensure all workers finish before verification
    let barrier = Arc::new(Barrier::new(WORKERS + 1));

    let handles: Vec<_> = (0..WORKERS)
        .map(|worker_id| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                for i in 0..INSERTS_PER_WORKER {
                    let doc_id = format!("w{}_d{}", worker_id, i);
                    let doc = json!({
                        "_id": doc_id,
                        "worker": worker_id,
                        "seq": i,
                    });

                    let mut tx = db.begin().unwrap();
                    let mut collection = tx.collection("test").unwrap();
                    collection.insert(doc).unwrap();
                    tx.commit().unwrap();
                }

                // Wait for all workers to finish
                barrier.wait();
            })
        })
        .collect();

    // Wait for all workers to finish
    barrier.wait();

    // Small delay to ensure MVCC state is updated
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Now start a NEW transaction and verify it sees ALL committed documents
    let mut verify_tx = db.begin().unwrap();
    let collection = verify_tx.collection("test").unwrap();
    let db_docs = collection.find_all().unwrap();

    let expected_count = WORKERS * INSERTS_PER_WORKER;
    if db_docs.len() != expected_count {
        println!("\n!!! SNAPSHOT ISOLATION ISSUE !!!");
        println!("Expected {} documents, but found {}", expected_count, db_docs.len());

        // Check which documents are missing
        let db_ids: std::collections::HashSet<String> = db_docs
            .iter()
            .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        let mut missing = Vec::new();
        for worker_id in 0..WORKERS {
            for i in 0..INSERTS_PER_WORKER {
                let doc_id = format!("w{}_d{}", worker_id, i);
                if !db_ids.contains(&doc_id) {
                    missing.push(doc_id);
                }
            }
        }

        println!("Missing {} documents:", missing.len());
        for (idx, id) in missing.iter().take(10).enumerate() {
            println!("  {}: {}", idx + 1, id);
        }
        if missing.len() > 10 {
            println!("  ... and {} more", missing.len() - 10);
        }

        panic!("Snapshot isolation issue: verification transaction doesn't see all committed data!");
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("✓ Snapshot isolation correct: verification sees all {} committed documents", expected_count);
}

#[test]
fn test_concurrent_commit_then_immediate_verify() {
    // More aggressive test: commit from multiple threads, then IMMEDIATELY verify from main thread
    const WORKERS: usize = 2;
    const OPS: usize = 100;

    for run in 0..20 {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join(format!("run{}.db", run));
        let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

        let barrier = Arc::new(Barrier::new(WORKERS + 1));

        let handles: Vec<_> = (0..WORKERS)
            .map(|worker_id| {
                let db = Arc::clone(&db);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    for i in 0..OPS {
                        let doc_id = format!("w{}_d{}", worker_id, i);
                        let doc = json!({"_id": doc_id, "w": worker_id, "i": i});

                        let mut tx = db.begin().unwrap();
                        let mut collection = tx.collection("test").unwrap();
                        collection.insert(doc).unwrap();

                        match tx.commit() {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("Run {} W{} I{}: commit failed: {}", run, worker_id, i, e);
                            }
                        }
                    }
                    barrier.wait();
                })
            })
            .collect();

        // Wait for all workers
        barrier.wait();

        // IMMEDIATELY verify (no delay)
        let mut verify_tx = db.begin().unwrap();
        let collection = verify_tx.collection("test").unwrap();
        let docs = collection.find_all().unwrap();

        let expected = WORKERS * OPS;
        if docs.len() != expected {
            println!("\n!!! FAILURE on run {} !!!", run);
            println!("Expected {} docs, found {}", expected, docs.len());
            panic!("Immediate verification failed!");
        }

        if run % 5 == 0 {
            println!("Run {}: OK ({} docs)", run, docs.len());
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    println!("✓ All 20 runs passed");
}
