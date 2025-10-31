// Test to verify B-tree integrity after concurrent commits
use jasonisnthappy::Database;
use serde_json::json;
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}, Mutex};
use std::thread;
use std::collections::HashMap;

#[test]
fn test_tree_integrity() {
    let db_path = "/tmp/tree_integrity_test.db";
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{}.lock", db_path));
    let _ = std::fs::remove_file(format!("{}-wal", db_path));

    let db = Arc::new(Database::open(db_path).unwrap());
    let counter = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // Track committed documents per collection
    type TruthMap = HashMap<String, HashMap<String, bool>>;
    let truth: Arc<Mutex<TruthMap>> = Arc::new(Mutex::new(HashMap::new()));

    // Initialize collections
    {
        let mut t = truth.lock().unwrap();
        for coll in ["users", "products", "orders", "events", "analytics"] {
            t.insert(coll.to_string(), HashMap::new());
        }
    }

    let mut handles = vec![];
    let collections = ["users", "products", "orders", "events", "analytics"];

    // 8 writer threads
    for thread_id in 0..8 {
        let db = db.clone();
        let counter = counter.clone();
        let stop = stop.clone();
        let truth = truth.clone();

        handles.push(thread::spawn(move || {
            let mut local_count = 0;
            while !stop.load(Ordering::Relaxed) && local_count < 500 {
                let n = counter.fetch_add(1, Ordering::Relaxed);
                let doc_id = format!("doc_{}_{}", thread_id, n);
                let coll_name = collections[n as usize % collections.len()];

                let mut tx = db.begin().unwrap();
                let mut coll = tx.collection(coll_name).unwrap();

                // 70% inserts, 20% updates, 10% deletes
                let op_type = (n % 10) as u8;

                if op_type < 7 {
                    // Insert
                    match coll.insert(json!({"_id": doc_id.clone(), "thread": thread_id})) {
                        Ok(_) => {
                            match tx.commit() {
                                Ok(_) => {
                                    let mut t = truth.lock().unwrap();
                                    t.get_mut(coll_name).unwrap().insert(doc_id.clone(), true);
                                    local_count += 1;
                                }
                                Err(_e) => {}
                            }
                        }
                        Err(_e) => {}
                    }
                } else if op_type < 9 {
                    // Update - pick random existing doc
                    let existing_id = {
                        let t = truth.lock().unwrap();
                        let docs: Vec<_> = t.get(coll_name).unwrap().keys().cloned().collect();
                        if docs.is_empty() { None } else { Some(docs[n as usize % docs.len()].clone()) }
                    };
                    if let Some(id) = existing_id {
                        if coll.update_by_id(&id, json!({"updated": true})).is_ok() {
                            let _ = tx.commit();
                        }
                    }
                } else {
                    // Delete - pick random existing doc
                    let existing_id = {
                        let t = truth.lock().unwrap();
                        let docs: Vec<_> = t.get(coll_name).unwrap().keys().cloned().collect();
                        if docs.is_empty() { None } else { Some(docs[n as usize % docs.len()].clone()) }
                    };
                    if let Some(id) = existing_id {
                        if coll.delete_by_id(&id).is_ok() {
                            match tx.commit() {
                                Ok(_) => {
                                    let mut t = truth.lock().unwrap();
                                    t.get_mut(coll_name).unwrap().remove(&id);
                                }
                                Err(_e) => {}
                            }
                        }
                    }
                }
            }
        }));
    }

    // Let threads run
    thread::sleep(std::time::Duration::from_secs(15));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    // Checkpoint
    db.checkpoint().unwrap();

    // Verify each collection
    let truth = truth.lock().unwrap();
    let mut all_passed = true;

    for coll_name in collections {
        let expected = truth.get(coll_name).unwrap();

        // Read all docs from this collection - use fresh transaction
        let mut tx = db.begin().unwrap();
        let coll = tx.collection(coll_name).unwrap();
        let docs = coll.find_all().unwrap();
        drop(coll);
        drop(tx);

        // Debug: verify collection by checking _id pattern matches expected collection
        eprintln!("[DEBUG] {} has {} docs", coll_name, docs.len());

        // Verify each doc belongs to this collection
        let coll_idx = collections.iter().position(|&c| c == coll_name).unwrap();
        for doc in &docs {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                // Parse n from doc_THREAD_N
                let parts: Vec<&str> = id.split('_').collect();
                if parts.len() == 3 {
                    if let Ok(n) = parts[2].parse::<usize>() {
                        let expected_coll_idx = n % 5;
                        if expected_coll_idx != coll_idx {
                            eprintln!("[ERROR] Doc {} is in {} but should be in {} (n={})",
                                id, coll_name, collections[expected_coll_idx], n);
                        }
                    }
                }
            }
        }

        let db_ids: std::collections::HashSet<String> = docs.iter()
            .filter_map(|d| d.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        // Find missing
        let mut missing = vec![];
        for id in expected.keys() {
            if !db_ids.contains(id) {
                missing.push(id.clone());
            }
        }

        // Find extra
        let mut extra = vec![];
        for id in &db_ids {
            if !expected.contains_key(id) {
                extra.push(id.clone());
            }
        }

        if !missing.is_empty() || !extra.is_empty() || expected.len() != db_ids.len() {
            eprintln!("[{}] FAILED: expected={}, got={}", coll_name, expected.len(), db_ids.len());
            if !missing.is_empty() {
                eprintln!("  Missing: {:?}", &missing[..missing.len().min(10)]);
            }
            if !extra.is_empty() {
                eprintln!("  Extra: {:?}", &extra[..extra.len().min(10)]);
            }
            all_passed = false;
        } else {
            eprintln!("[{}] OK: {} documents", coll_name, expected.len());
        }
    }

    assert!(all_passed, "Some collections failed verification");
}
