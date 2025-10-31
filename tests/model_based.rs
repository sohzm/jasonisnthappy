
use jasonisnthappy::core::database::Database;
use serde_json::{json, Value};
use std::collections::HashMap;
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
            .insert(doc_id.to_string(), deep_copy(&doc));
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
            .map(|coll| coll.values().map(|doc| deep_copy(doc)).collect())
            .unwrap_or_default()
    }
}

fn deep_copy(value: &Value) -> Value {
    value.clone()
}

fn documents_match(a: &[Value], b: &[Value]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut a_map: HashMap<String, String> = HashMap::new();
    let mut b_map: HashMap<String, String> = HashMap::new();

    for doc in a {
        if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
            a_map.insert(id.to_string(), serde_json::to_string(doc).unwrap());
        }
    }

    for doc in b {
        if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
            b_map.insert(id.to_string(), serde_json::to_string(doc).unwrap());
        }
    }

    a_map == b_map
}

fn log_differences(db_docs: &[Value], truth_docs: &[Value]) -> String {
    let mut result = String::new();

    let db_map: HashMap<String, String> = db_docs
        .iter()
        .filter_map(|doc| {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                Some((id.to_string(), serde_json::to_string(doc).unwrap()))
            } else {
                None
            }
        })
        .collect();

    let truth_map: HashMap<String, String> = truth_docs
        .iter()
        .filter_map(|doc| {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                Some((id.to_string(), serde_json::to_string(doc).unwrap()))
            } else {
                None
            }
        })
        .collect();

    // Check for missing/extra documents
    for id in db_map.keys() {
        if !truth_map.contains_key(id) {
            result.push_str(&format!("  Extra in DB: {}\n", id));
        }
    }

    for id in truth_map.keys() {
        if !db_map.contains_key(id) {
            result.push_str(&format!("  Missing from DB: {}\n", id));
        }
    }

    // Check for content differences
    let mut content_diffs = 0;
    for (id, db_json) in &db_map {
        if let Some(truth_json) = truth_map.get(id) {
            if db_json != truth_json {
                content_diffs += 1;
                if content_diffs <= 5 {  // Show first 5 differences
                    result.push_str(&format!("  Content mismatch for ID: {}\n", id));
                    result.push_str(&format!("    DB:    {}\n", db_json));
                    result.push_str(&format!("    Truth: {}\n", truth_json));
                }
            }
        }
    }
    if content_diffs > 5 {
        result.push_str(&format!("  ... and {} more content mismatches\n", content_diffs - 5));
    }

    result
}

fn verify_full_state(db: &Database, truth: &TruthModel, collection_name: &str) -> Result<(), String> {
    let mut tx = db.begin().map_err(|e| format!("Failed to begin transaction: {}", e))?;
    let collection = tx.collection(collection_name)
        .map_err(|e| format!("Failed to get collection: {}", e))?;

    let db_docs = collection
        .find_all()
        .map_err(|e| format!("Failed to get all docs: {}", e))?;

    let truth_docs = truth.find_all(collection_name);

    if !documents_match(&db_docs, &truth_docs) {
        let mut error = format!(
            "State mismatch: DB has {} docs, truth has {} docs\n",
            db_docs.len(),
            truth_docs.len()
        );
        error.push_str(&log_differences(&db_docs, &truth_docs));
        return Err(error);
    }

    Ok(())
}

#[test]
fn test_model_based_correctness() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("model_based.db");

    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let mut truth = TruthModel::new();
    let mut rng = thread_rng();

    const OPERATIONS: usize = 1000;
    let mut insert_count = 0;
    let mut update_count = 0;
    let mut delete_count = 0;
    let mut query_count = 0;
    let mut divergences = 0;

    println!("Running {} random operations with model-based verification...", OPERATIONS);

    for i in 0..OPERATIONS {
        let operation = rng.gen_range(0..4);
        let collection_name = "test_collection";

        match operation {
            0 => {
                let doc_id = format!("doc_{}", i);
                let doc = json!({
                    "_id": doc_id,
                    "type": "test",
                    "iteration": i,
                    "value": rng.gen_range(0..1000),
                    "timestamp": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                });

                let mut tx = db.begin().unwrap();
                let mut collection = tx.collection(collection_name).unwrap();

                match collection.insert(doc.clone()) {
                    Ok(_) => {
                        if tx.commit().is_ok() {
                            truth.insert(collection_name, &doc_id, doc);
                            insert_count += 1;
                        }
                    }
                    Err(_) => {
                        let _ = tx.rollback();
                    }
                }
            }
            1 => {
                let all_docs = truth.find_all(collection_name);
                if !all_docs.is_empty() {
                    let random_doc = &all_docs[rng.gen_range(0..all_docs.len())];
                    if let Some(doc_id) = random_doc.get("_id").and_then(|v| v.as_str()) {
                        let updates = json!({
                            "updated_at": std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                            "value": rng.gen_range(0..1000),
                        });

                        let mut tx = db.begin().unwrap();
                        let mut collection = tx.collection(collection_name).unwrap();

                        match collection.update_by_id(doc_id, updates.clone()) {
                            Ok(_) => {
                                if tx.commit().is_ok() {
                                    truth.update(collection_name, doc_id, &updates);
                                    update_count += 1;
                                }
                            }
                            Err(_) => {
                                let _ = tx.rollback();
                            }
                        }
                    }
                }
            }
            2 => {
                let all_docs = truth.find_all(collection_name);
                if !all_docs.is_empty() {
                    let random_doc = &all_docs[rng.gen_range(0..all_docs.len())];
                    if let Some(doc_id) = random_doc.get("_id").and_then(|v| v.as_str()) {
                        let mut tx = db.begin().unwrap();
                        let mut collection = tx.collection(collection_name).unwrap();

                        match collection.delete_by_id(doc_id) {
                            Ok(_) => {
                                if tx.commit().is_ok() {
                                    truth.delete(collection_name, doc_id);
                                    delete_count += 1;
                                }
                            }
                            Err(_) => {
                                let _ = tx.rollback();
                            }
                        }
                    }
                }
            }
            3 => {
                query_count += 1;

                let mut tx = db.begin().unwrap();
                let collection = tx.collection(collection_name).unwrap();

                if let Ok(db_docs) = collection.find_all() {
                    let truth_docs = truth.find_all(collection_name);

                    if !documents_match(&db_docs, &truth_docs) {
                        divergences += 1;
                        eprintln!(
                            "DIVERGENCE at operation {}: DB has {} docs, truth has {} docs",
                            i,
                            db_docs.len(),
                            truth_docs.len()
                        );
                        eprintln!("{}", log_differences(&db_docs, &truth_docs));
                    }
                }

                let _ = tx.rollback();
            }
            _ => unreachable!(),
        }

        if i % 100 == 0 && i > 0 {
            if let Err(err) = verify_full_state(&db, &truth, collection_name) {
                divergences += 1;
                eprintln!("Full state verification failed at operation {}: {}", i, err);
            }
            println!(
                "Progress: {}/{} ops (I:{} U:{} D:{} Q:{}, Divergences:{})",
                i, OPERATIONS, insert_count, update_count, delete_count, query_count, divergences
            );
        }
    }

    println!("\n{}", "=".repeat(60));
    println!("MODEL-BASED CORRECTNESS TEST RESULTS");
    println!("{}", "=".repeat(60));
    println!("Total operations: {}", OPERATIONS);
    println!("  Inserts: {}", insert_count);
    println!("  Updates: {}", update_count);
    println!("  Deletes: {}", delete_count);
    println!("  Queries: {}", query_count);
    println!("Divergences detected: {}", divergences);
    println!("{}", "=".repeat(60));

    verify_full_state(&db, &truth, "test_collection")
        .expect("Final state verification FAILED");

    assert_eq!(divergences, 0, "CRITICAL FAILURE: Database diverged from truth model {} times", divergences);

    println!("✓✓✓ SUCCESS: Zero divergence - DB matches truth model perfectly ✓✓✓");
}

#[test]
fn test_model_based_with_crash() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("model_crash.db");
    let db_path_str = db_path.to_str().unwrap();

    let mut truth = TruthModel::new();
    let mut rng = thread_rng();

    const CRASH_INTERVAL: usize = 100;
    const TOTAL_OPERATIONS: usize = 500;
    let mut crashes = 0;

    for round in 0..(TOTAL_OPERATIONS / CRASH_INTERVAL) {
        println!("=== Round {} (after {} crashes) ===", round + 1, crashes);

        let db = Database::open(db_path_str).unwrap();

        for i in 0..CRASH_INTERVAL {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("test").unwrap();

            let doc_id = format!("doc_{}_{}", round, i);
            let doc = json!({
                "_id": doc_id,
                "round": round,
                "iter": i,
                "value": rng.gen_range(0..1000),
            });

            match collection.insert(doc.clone()) {
                Ok(_) => {
                    match tx.commit() {
                        Ok(_) => {
                            truth.insert("test", &doc_id, doc);
                        }
                        Err(e) => {
                            eprintln!("Commit failed for {}: {}", doc_id, e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Insert failed for {}: {}", doc_id, e);
                    let _ = tx.rollback();
                }
            }
        }

        db.checkpoint().unwrap_or_else(|e| {
            eprintln!("Warning: checkpoint failed: {}", e);
        });

        verify_full_state(&db, &truth, "test").expect("State diverged before crash");

        drop(db);

        crashes += 1;

        let db = Database::open(db_path_str)
            .unwrap_or_else(|e| panic!("Failed to recover after crash {}: {}", crashes, e));

        verify_full_state(&db, &truth, "test")
            .unwrap_or_else(|e| panic!("State diverged after crash {}: {}", crashes, e));

        drop(db);
    }

    println!("✓ Survived {} crashes with zero data loss", crashes);
}

// Concurrent model-based test with 2 workers performing 50 random operations each
// Tests batch commit correctness under concurrent load with proper transaction isolation
//
// Fixed concurrency bugs:
// 1. Stale root regression - prevented transactions with no writes from regressing metadata
// 2. Missing WAL writes - rebased pages now properly written to WAL for durability
// 3. DELETE tracking - delete operations now tracked and replayed correctly during rebase
#[test]
fn test_model_based_concurrent() {
    use std::sync::{Arc, Mutex};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("model_concurrent.db");

    let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());
    let truth = Arc::new(Mutex::new(TruthModel::new()));

    const WORKERS: usize = 2;
    const OPS_PER_WORKER: usize = 50;

    let handles: Vec<_> = (0..WORKERS)
        .map(|worker_id| {
            let db = Arc::clone(&db);
            let truth = Arc::clone(&truth);

            thread::spawn(move || {
                let mut rng = thread_rng();
                let collection_name = "concurrent_test";

                for i in 0..OPS_PER_WORKER {
                    let operation = rng.gen_range(0..3);

                    match operation {
                        0 => {
                            let doc_id = format!("worker_{}_doc_{}", worker_id, i);
                            let doc = json!({
                                "_id": doc_id.clone(),
                                "worker": worker_id,
                                "iteration": i,
                                "value": rng.gen_range(0..1000),
                            });

                            let mut tx = db.begin().unwrap();
                            let mut collection = tx.collection(collection_name).unwrap();

                            if collection.insert(doc.clone()).is_ok() && tx.commit().is_ok() {
                                let mut truth = truth.lock().unwrap();
                                truth.insert(collection_name, &doc_id, doc);
                            }
                        }
                        1 => {
                            // Pick a doc that exists in truth to avoid race conditions
                            let truth_docs = {
                                let truth = truth.lock().unwrap();
                                truth.find_all(collection_name)
                            };

                            if !truth_docs.is_empty() {
                                let random_doc = &truth_docs[rng.gen_range(0..truth_docs.len())];
                                if let Some(doc_id_str) = random_doc.get("_id").and_then(|v| v.as_str()) {
                                    let doc_id = doc_id_str.to_string();
                                    let updates = json!({
                                        "value": rng.gen_range(0..1000),
                                    });

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
                            // Pick a doc that exists in truth to avoid race conditions
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
    verify_full_state(&db, &truth, "concurrent_test")
        .expect("Concurrent model-based test failed");

    println!("✓ Concurrent model-based test passed with {} workers × {} ops", WORKERS, OPS_PER_WORKER);
}
