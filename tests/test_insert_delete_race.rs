// Test INSERT + DELETE race conditions
// Hypothesis: DELETE operations might interfere with INSERT operations

use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::TempDir;
use rand::{thread_rng, Rng};

#[test]
fn test_insert_delete_concurrent_same_docs() {
    // Worker 0: Inserts docs
    // Worker 1: Deletes docs from truth
    // At the end: DB and truth should match

    const RUNS: usize = 50;

    println!("\n=== Testing INSERT/DELETE race ({} runs) ===\n", RUNS);

    let mut failures = 0;

    for run in 0..RUNS {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join(format!("run{}.db", run));
        let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

        // Track which docs should be in DB (committed inserts - committed deletes)
        let truth = Arc::new(Mutex::new(std::collections::HashSet::new()));

        let handles: Vec<_> = (0..2)
            .map(|worker_id| {
                let db = Arc::clone(&db);
                let truth = Arc::clone(&truth);

                thread::spawn(move || {
                    let mut rng = thread_rng();

                    for i in 0..50 {
                        let op = rng.gen_range(0..2);

                        match op {
                            0 => {
                                // INSERT
                                let doc_id = format!("w{}_d{}", worker_id, i);
                                let doc = json!({"_id": doc_id.clone(), "w": worker_id, "i": i});

                                let mut tx = db.begin().unwrap();
                                let mut collection = tx.collection("test").unwrap();

                                if collection.insert(doc).is_ok() && tx.commit().is_ok() {
                                    let mut t = truth.lock().unwrap();
                                    t.insert(doc_id);
                                }
                            }
                            1 => {
                                // DELETE
                                let truth_snapshot = {
                                    let t = truth.lock().unwrap();
                                    t.iter().cloned().collect::<Vec<_>>()
                                };

                                if !truth_snapshot.is_empty() && i > 5 {
                                    let doc_id = &truth_snapshot[rng.gen_range(0..truth_snapshot.len())];

                                    let mut tx = db.begin().unwrap();
                                    let mut collection = tx.collection("test").unwrap();

                                    if collection.delete_by_id(doc_id).is_ok() && tx.commit().is_ok() {
                                        let mut t = truth.lock().unwrap();
                                        t.remove(doc_id);
                                    }
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify
        let truth_set = truth.lock().unwrap();
        let mut verify_tx = db.begin().unwrap();
        let collection = verify_tx.collection("test").unwrap();
        let db_docs = collection.find_all().unwrap();

        let db_ids: std::collections::HashSet<String> = db_docs
            .iter()
            .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        if db_ids != *truth_set {
            failures += 1;
            println!("\n!!! FAILURE on run {} !!!", run);
            println!("Truth has {} docs, DB has {} docs", truth_set.len(), db_ids.len());

            for id in truth_set.iter() {
                if !db_ids.contains(id) {
                    println!("  Missing from DB: {}", id);
                }
            }

            for id in &db_ids {
                if !truth_set.contains(id) {
                    println!("  Extra in DB: {}", id);
                }
            }
        } else if run % 10 == 0 {
            println!("Run {}: PASS ({} docs)", run, truth_set.len());
        }
    }

    println!("\n=== SUMMARY ===");
    println!("Runs: {}, Failures: {}, Rate: {:.1}%", RUNS, failures, failures as f64 / RUNS as f64 * 100.0);

    if failures > 0 {
        panic!("INSERT/DELETE race detected! {} failures out of {} runs", failures, RUNS);
    }
}

#[test]
fn test_insert_update_delete_pattern() {
    // Replicate the exact pattern from model_based_concurrent
    const RUNS: usize = 30;
    const WORKERS: usize = 2;
    const OPS_PER_WORKER: usize = 50;

    println!("\n=== Replicating model_based pattern ({} runs) ===\n", RUNS);

    let mut failures = 0;

    for run in 0..RUNS {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join(format!("pattern{}.db", run));
        let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

        let truth = Arc::new(Mutex::new(std::collections::HashMap::new()));

        let handles: Vec<_> = (0..WORKERS)
            .map(|worker_id| {
                let db = Arc::clone(&db);
                let truth = Arc::clone(&truth);

                thread::spawn(move || {
                    let mut rng = thread_rng();

                    for i in 0..OPS_PER_WORKER {
                        let operation = rng.gen_range(0..3);

                        match operation {
                            0 => {
                                // INSERT
                                let doc_id = format!("worker_{}_doc_{}", worker_id, i);
                                let doc = json!({
                                    "_id": doc_id.clone(),
                                    "worker": worker_id,
                                    "iteration": i,
                                    "value": rng.gen_range(0..1000),
                                });

                                let mut tx = db.begin().unwrap();
                                let mut collection = tx.collection("concurrent_test").unwrap();

                                if collection.insert(doc.clone()).is_ok() && tx.commit().is_ok() {
                                    let mut truth = truth.lock().unwrap();
                                    truth.insert(doc_id, doc);
                                }
                            }
                            1 => {
                                // UPDATE
                                let truth_docs = {
                                    let truth = truth.lock().unwrap();
                                    truth.keys().cloned().collect::<Vec<_>>()
                                };

                                if !truth_docs.is_empty() {
                                    let doc_id = &truth_docs[rng.gen_range(0..truth_docs.len())];
                                    let updates = json!({"value": rng.gen_range(0..1000)});

                                    let mut tx = db.begin().unwrap();
                                    let mut collection = tx.collection("concurrent_test").unwrap();

                                    if collection.update_by_id(doc_id, updates.clone()).is_ok() && tx.commit().is_ok() {
                                        let mut truth = truth.lock().unwrap();
                                        if let Some(doc) = truth.get_mut(doc_id) {
                                            if let (Some(doc_obj), Some(updates_obj)) = (doc.as_object_mut(), updates.as_object()) {
                                                for (key, value) in updates_obj {
                                                    doc_obj.insert(key.clone(), value.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            2 => {
                                // DELETE
                                let truth_docs = {
                                    let truth = truth.lock().unwrap();
                                    truth.keys().cloned().collect::<Vec<_>>()
                                };

                                if !truth_docs.is_empty() && i > 10 {
                                    let doc_id = truth_docs[rng.gen_range(0..truth_docs.len())].clone();

                                    let mut tx = db.begin().unwrap();
                                    let mut collection = tx.collection("concurrent_test").unwrap();

                                    if collection.delete_by_id(&doc_id).is_ok() && tx.commit().is_ok() {
                                        let mut truth = truth.lock().unwrap();
                                        truth.remove(&doc_id);
                                    }
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify
        let truth = truth.lock().unwrap();
        let mut verify_tx = db.begin().unwrap();
        let collection = verify_tx.collection("concurrent_test").unwrap();
        let db_docs = collection.find_all().unwrap();

        let db_ids: std::collections::HashSet<String> = db_docs
            .iter()
            .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        let truth_ids: std::collections::HashSet<String> = truth.keys().cloned().collect();

        if db_ids != truth_ids {
            failures += 1;
            println!("\n!!! FAILURE on run {} !!!", run);
            println!("Truth has {} docs, DB has {} docs", truth_ids.len(), db_ids.len());

            for id in &truth_ids {
                if !db_ids.contains(id) {
                    println!("  Missing from DB: {}", id);
                }
            }

            for id in &db_ids {
                if !truth_ids.contains(id) {
                    println!("  Extra in DB: {}", id);
                }
            }
        } else if run % 10 == 0 {
            println!("Run {}: PASS ({} docs)", run, truth.len());
        }
    }

    println!("\n=== SUMMARY ===");
    println!("Runs: {}, Failures: {}, Rate: {:.1}%", RUNS, failures, failures as f64 / RUNS as f64 * 100.0);

    if failures > 0 {
        panic!("Pattern test failed! {} failures out of {} runs", failures, RUNS);
    }
}
