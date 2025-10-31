// Reproducer for the flaky test_model_based_concurrent failure
// Goal: Run the test multiple times to reliably reproduce the "Missing from DB" issue

use jasonisnthappy::core::database::Database;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::TempDir;
use rand::{thread_rng, Rng};

#[derive(Debug, Clone)]
struct TruthModel {
    collections: HashMap<String, HashMap<String, Value>>,
}

impl TruthModel {
    fn new() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }

    fn insert(&mut self, collection: &str, doc_id: &str, doc: Value) {
        self.collections
            .entry(collection.to_string())
            .or_insert_with(HashMap::new)
            .insert(doc_id.to_string(), doc.clone());
    }

    fn update(&mut self, collection: &str, doc_id: &str, updates: &Value) -> bool {
        if let Some(coll) = self.collections.get_mut(collection) {
            if let Some(doc) = coll.get_mut(doc_id) {
                if let (Some(doc_obj), Some(updates_obj)) = (doc.as_object_mut(), updates.as_object()) {
                    for (key, value) in updates_obj {
                        doc_obj.insert(key.clone(), value.clone());
                    }
                    return true;
                }
            }
        }
        false
    }

    fn delete(&mut self, collection: &str, doc_id: &str) -> bool {
        if let Some(coll) = self.collections.get_mut(collection) {
            coll.remove(doc_id).is_some()
        } else {
            false
        }
    }

    fn find_all(&self, collection: &str) -> Vec<Value> {
        self.collections
            .get(collection)
            .map(|coll| coll.values().cloned().collect())
            .unwrap_or_default()
    }

    fn get_doc_ids(&self, collection: &str) -> Vec<String> {
        self.collections
            .get(collection)
            .map(|coll| coll.keys().cloned().collect())
            .unwrap_or_default()
    }
}

fn verify_state(db: &Database, truth: &TruthModel, collection_name: &str) -> Result<(), String> {
    let mut tx = db.begin().map_err(|e| format!("Failed to begin: {}", e))?;
    let collection = tx.collection(collection_name).map_err(|e| format!("Failed to get collection: {}", e))?;
    let db_docs = collection.find_all().map_err(|e| format!("Failed to find_all: {}", e))?;

    let db_ids: Vec<String> = db_docs
        .iter()
        .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    let truth_ids = truth.get_doc_ids(collection_name);

    if db_ids.len() != truth_ids.len() {
        let mut error = format!(
            "State mismatch: DB has {} docs, truth has {} docs\n",
            db_ids.len(),
            truth_ids.len()
        );

        for id in &db_ids {
            if !truth_ids.contains(id) {
                error.push_str(&format!("  Extra in DB: {}\n", id));
            }
        }

        for id in &truth_ids {
            if !db_ids.contains(id) {
                error.push_str(&format!("  Missing from DB: {}\n", id));
            }
        }

        return Err(error);
    }

    Ok(())
}

#[test]
fn test_reproduce_flaky_concurrent() {
    const RUNS: usize = 50;
    const WORKERS: usize = 2;
    const OPS_PER_WORKER: usize = 50;

    println!("\n=== Running concurrent test {} times to reproduce flaky failure ===\n", RUNS);

    let mut failures = 0;
    let mut failure_details = Vec::new();

    for run in 0..RUNS {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join(format!("concurrent_{}.db", run));
        let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());
        let truth = Arc::new(Mutex::new(TruthModel::new()));

        // Track all operations for debugging
        let operations_log = Arc::new(Mutex::new(Vec::new()));

        let handles: Vec<_> = (0..WORKERS)
            .map(|worker_id| {
                let db = Arc::clone(&db);
                let truth = Arc::clone(&truth);
                let operations_log = Arc::clone(&operations_log);

                thread::spawn(move || {
                    let mut rng = thread_rng();
                    let collection_name = "concurrent_test";

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
                                let mut collection = tx.collection(collection_name).unwrap();

                                let insert_result = collection.insert(doc.clone());
                                let commit_result = if insert_result.is_ok() {
                                    tx.commit()
                                } else {
                                    Err(jasonisnthappy::Error::Other("insert failed".to_string()))
                                };

                                if insert_result.is_ok() && commit_result.is_ok() {
                                    let mut truth = truth.lock().unwrap();
                                    truth.insert(collection_name, &doc_id, doc.clone());

                                    let mut log = operations_log.lock().unwrap();
                                    log.push(format!("RUN{} W{} I{}: INSERT {} - COMMITTED", run, worker_id, i, doc_id));
                                } else {
                                    let mut log = operations_log.lock().unwrap();
                                    log.push(format!("RUN{} W{} I{}: INSERT {} - FAILED (insert={:?}, commit={:?})",
                                        run, worker_id, i, doc_id, insert_result.is_ok(), commit_result.is_ok()));
                                }
                            }
                            1 => {
                                // UPDATE
                                let truth_docs = {
                                    let truth = truth.lock().unwrap();
                                    truth.find_all(collection_name)
                                };

                                if !truth_docs.is_empty() {
                                    let random_doc = &truth_docs[rng.gen_range(0..truth_docs.len())];
                                    if let Some(doc_id_str) = random_doc.get("_id").and_then(|v| v.as_str()) {
                                        let doc_id = doc_id_str.to_string();
                                        let updates = json!({"value": rng.gen_range(0..1000)});

                                        let mut tx = db.begin().unwrap();
                                        let mut collection = tx.collection(collection_name).unwrap();

                                        if collection.update_by_id(&doc_id, updates.clone()).is_ok() && tx.commit().is_ok() {
                                            let mut truth = truth.lock().unwrap();
                                            truth.update(collection_name, &doc_id, &updates);
                                        }
                                    }
                                }
                            }
                            2 => {
                                // DELETE
                                let truth_docs = {
                                    let truth = truth.lock().unwrap();
                                    truth.find_all(collection_name)
                                };

                                if !truth_docs.is_empty() && i > 10 {
                                    let random_doc = &truth_docs[rng.gen_range(0..truth_docs.len())];
                                    if let Some(doc_id_str) = random_doc.get("_id").and_then(|v| v.as_str()) {
                                        let doc_id = doc_id_str.to_string();

                                        let mut tx = db.begin().unwrap();
                                        let mut collection = tx.collection(collection_name).unwrap();

                                        if collection.delete_by_id(&doc_id).is_ok() && tx.commit().is_ok() {
                                            let mut truth = truth.lock().unwrap();
                                            truth.delete(collection_name, &doc_id);

                                            let mut log = operations_log.lock().unwrap();
                                            log.push(format!("RUN{} W{} I{}: DELETE {} - COMMITTED", run, worker_id, i, doc_id));
                                        }
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

        let truth = truth.lock().unwrap();
        match verify_state(&db, &truth, "concurrent_test") {
            Ok(_) => {
                if run % 10 == 0 {
                    println!("Run {}/{}: PASS", run + 1, RUNS);
                }
            }
            Err(err) => {
                failures += 1;
                println!("\n!!! FAILURE on run {}/{} !!!", run + 1, RUNS);
                println!("{}", err);

                let log = operations_log.lock().unwrap();
                println!("\nOperation log (last 20 operations):");
                for (idx, entry) in log.iter().rev().take(20).rev().enumerate() {
                    println!("  {}: {}", idx, entry);
                }

                failure_details.push((run, err));
            }
        }
    }

    println!("\n=== SUMMARY ===");
    println!("Total runs: {}", RUNS);
    println!("Failures: {}", failures);
    println!("Success rate: {:.1}%", (RUNS - failures) as f64 / RUNS as f64 * 100.0);

    if failures > 0 {
        println!("\nFailure details:");
        for (run, err) in &failure_details {
            println!("  Run {}: {}", run, err.lines().next().unwrap_or("unknown"));
        }

        panic!("Flaky test reproduced! {} failures out of {} runs ({:.1}%)",
            failures, RUNS, failures as f64 / RUNS as f64 * 100.0);
    } else {
        println!("\nCould not reproduce the flaky failure in {} runs", RUNS);
    }
}
