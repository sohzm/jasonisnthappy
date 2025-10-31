// Detailed logging version to understand exactly when/how data is lost

use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use rand::{thread_rng, Rng};

#[derive(Debug, Clone)]
struct Operation {
    timestamp_us: u128,
    worker_id: usize,
    iteration: usize,
    op_type: String,
    doc_id: String,
    commit_result: String,
}

#[test]
fn test_with_detailed_logging() {
    const RUNS: usize = 100;

    println!("\n=== Running with detailed logging ===\n");

    for run in 0..RUNS {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join(format!("logged_{}.db", run));
        let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

        let truth = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let op_log = Arc::new(Mutex::new(Vec::new()));

        let handles: Vec<_> = (0..2)
            .map(|worker_id| {
                let db = Arc::clone(&db);
                let truth = Arc::clone(&truth);
                let op_log = Arc::clone(&op_log);

                thread::spawn(move || {
                    let mut rng = thread_rng();

                    for i in 0..50 {
                        let operation = rng.gen_range(0..3);

                        match operation {
                            0 => {
                                // INSERT
                                let doc_id = format!("worker_{}_doc_{}", worker_id, i);
                                let doc = json!({
                                    "_id": doc_id.clone(),
                                    "worker": worker_id,
                                    "iteration": i,
                                });

                                let mut tx = db.begin().unwrap();
                                let mut collection = tx.collection("test").unwrap();

                                let insert_ok = collection.insert(doc.clone()).is_ok();
                                let commit_result = if insert_ok {
                                    tx.commit()
                                } else {
                                    Err(jasonisnthappy::Error::Other("insert failed".to_string()))
                                };

                                let timestamp_us = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_micros();

                                if commit_result.is_ok() {
                                    // Commit succeeded - add to truth
                                    let mut truth = truth.lock().unwrap();
                                    truth.insert(doc_id.clone(), doc);

                                    let mut log = op_log.lock().unwrap();
                                    log.push(Operation {
                                        timestamp_us,
                                        worker_id,
                                        iteration: i,
                                        op_type: "INSERT".to_string(),
                                        doc_id: doc_id.clone(),
                                        commit_result: "OK".to_string(),
                                    });

                                    // IMMEDIATELY verify it's in DB
                                    let mut verify_tx = db.begin().unwrap();
                                    let verify_collection = verify_tx.collection("test").unwrap();
                                    let found = verify_collection.find_by_id(&doc_id).is_ok();

                                    if !found {
                                        let mut log = op_log.lock().unwrap();
                                        log.push(Operation {
                                            timestamp_us: timestamp_us + 1,
                                            worker_id,
                                            iteration: i,
                                            op_type: "VERIFY_FAIL".to_string(),
                                            doc_id: doc_id.clone(),
                                            commit_result: "NOT_FOUND".to_string(),
                                        });
                                    }
                                } else {
                                    let mut log = op_log.lock().unwrap();
                                    log.push(Operation {
                                        timestamp_us,
                                        worker_id,
                                        iteration: i,
                                        op_type: "INSERT".to_string(),
                                        doc_id: doc_id.clone(),
                                        commit_result: format!("ERR: {:?}", commit_result),
                                    });
                                }
                            }
                            1 => {
                                // UPDATE
                                let truth_docs = {
                                    let truth = truth.lock().unwrap();
                                    truth.keys().cloned().collect::<Vec<_>>()
                                };

                                if !truth_docs.is_empty() {
                                    let doc_id = truth_docs[rng.gen_range(0..truth_docs.len())].clone();
                                    let updates = json!({"value": rng.gen_range(0..1000)});

                                    let mut tx = db.begin().unwrap();
                                    let mut collection = tx.collection("test").unwrap();

                                    if collection.update_by_id(&doc_id, updates.clone()).is_ok() && tx.commit().is_ok() {
                                        let mut truth = truth.lock().unwrap();
                                        if let Some(doc) = truth.get_mut(&doc_id) {
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

                                    let timestamp_us = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_micros();

                                    let mut tx = db.begin().unwrap();
                                    let mut collection = tx.collection("test").unwrap();

                                    if collection.delete_by_id(&doc_id).is_ok() && tx.commit().is_ok() {
                                        let mut truth = truth.lock().unwrap();
                                        truth.remove(&doc_id);

                                        let mut log = op_log.lock().unwrap();
                                        log.push(Operation {
                                            timestamp_us,
                                            worker_id,
                                            iteration: i,
                                            op_type: "DELETE".to_string(),
                                            doc_id: doc_id.clone(),
                                            commit_result: "OK".to_string(),
                                        });
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
        let collection = verify_tx.collection("test").unwrap();
        let db_docs = collection.find_all().unwrap();

        let db_ids: std::collections::HashSet<String> = db_docs
            .iter()
            .filter_map(|doc| doc.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        let truth_ids: std::collections::HashSet<String> = truth.keys().cloned().collect();

        if db_ids != truth_ids {
            println!("\n╔═══════════════════════════════════════════════════════╗");
            println!("║  !!! FAILURE DETECTED on run {} !!!                  ║", run);
            println!("╚═══════════════════════════════════════════════════════╝\n");

            println!("Truth has {} docs, DB has {} docs", truth_ids.len(), db_ids.len());

            let missing: Vec<_> = truth_ids.iter().filter(|id| !db_ids.contains(*id)).collect();
            let extra: Vec<_> = db_ids.iter().filter(|id| !truth_ids.contains(*id)).collect();

            if !missing.is_empty() {
                println!("\nMissing from DB ({} docs):", missing.len());
                for id in &missing {
                    println!("  - {}", id);
                }
            }

            if !extra.is_empty() {
                println!("\nExtra in DB ({} docs):", extra.len());
                for id in &extra {
                    println!("  + {}", id);
                }
            }

            // Find operations related to missing documents
            let log = op_log.lock().unwrap();
            println!("\n═══ OPERATION LOG FOR MISSING DOCUMENTS ═══");
            for missing_id in &missing {
                println!("\nDocument: {}", missing_id);
                let mut ops: Vec<_> = log.iter().filter(|op| &op.doc_id == *missing_id).collect();
                ops.sort_by_key(|op| op.timestamp_us);

                for op in &ops {
                    println!("  [{:16}] W{} I{:2} {:12} {} → {}",
                        op.timestamp_us,
                        op.worker_id,
                        op.iteration,
                        op.op_type,
                        op.doc_id,
                        op.commit_result);
                }
            }

            println!("\n═══ RECENT OPERATIONS (all docs, last 30) ═══");
            let mut all_ops: Vec<_> = log.iter().collect();
            all_ops.sort_by_key(|op| op.timestamp_us);

            for op in all_ops.iter().rev().take(30).rev() {
                println!("[{:16}] W{} I{:2} {:12} {} → {}",
                    op.timestamp_us,
                    op.worker_id,
                    op.iteration,
                    op.op_type,
                    op.doc_id,
                    op.commit_result);
            }

            panic!("Test failed on run {}", run);
        } else if run % 20 == 0 {
            println!("Run {}: PASS ({} docs)", run, truth.len());
        }
    }

    println!("\n✓ All {} runs passed", RUNS);
}
