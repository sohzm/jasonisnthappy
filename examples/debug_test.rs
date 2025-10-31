// Simple concurrent test to isolate the issue
use jasonisnthappy::Database;
use serde_json::json;
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::thread;
use std::collections::HashMap;

fn main() {
    let db_path = "/tmp/debug_test.db";
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{}.lock", db_path));
    let _ = std::fs::remove_file(format!("{}-wal", db_path));

    let db = Arc::new(Database::open(db_path).unwrap());
    let counter = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    
    // Track what we committed
    let committed: Arc<std::sync::RwLock<HashMap<String, bool>>> = 
        Arc::new(std::sync::RwLock::new(HashMap::new()));

    let mut handles = vec![];
    
    // 8 writer threads (like the soak test)
    for thread_id in 0..8 {
        let db = db.clone();
        let counter = counter.clone();
        let stop = stop.clone();
        let committed = committed.clone();
        
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let n = counter.fetch_add(1, Ordering::Relaxed);
                let doc_id = format!("doc_{}_{}", thread_id, n);
                
                // Use multiple collections like the soak test
                let collections = ["users", "products", "orders", "events", "analytics"];
                let coll_name = collections[n as usize % collections.len()];

                let mut tx = db.begin().unwrap();
                let mut coll = tx.collection(coll_name).unwrap();
                
                match coll.insert(json!({"_id": doc_id.clone(), "data": "test"})) {
                    Ok(_) => {
                        match tx.commit() {
                            Ok(_) => {
                                let mut c = committed.write().unwrap();
                                c.insert(doc_id.clone(), true);
                                eprintln!("[T{}] Committed: {}", thread_id, doc_id);
                            }
                            Err(e) => {
                                eprintln!("[T{}] Commit failed for {}: {:?}", thread_id, doc_id, e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[T{}] Insert failed for {}: {:?}", thread_id, doc_id, e);
                    }
                }
                thread::sleep(std::time::Duration::from_millis(1));
            }
        }));
    }

    // Let it run for a bit
    thread::sleep(std::time::Duration::from_secs(10));
    stop.store(true, Ordering::Relaxed);
    
    for h in handles {
        h.join().unwrap();
    }

    // Now verify
    db.checkpoint().unwrap();
    
    let committed = committed.read().unwrap();
    eprintln!("\n=== VERIFICATION ===");
    eprintln!("Total committed in truth: {}", committed.len());
    
    // Read back all docs from all collections
    let mut tx = db.begin().unwrap();
    let mut all_docs = vec![];
    for coll_name in ["users", "products", "orders", "events", "analytics"] {
        let coll = tx.collection(coll_name).unwrap();
        all_docs.extend(coll.find_all().unwrap());
    }
    let docs = all_docs;
    eprintln!("Total in DB: {}", docs.len());
    
    // Find missing
    let mut missing = vec![];
    let mut extra = vec![];
    
    let db_ids: std::collections::HashSet<String> = docs.iter()
        .filter_map(|d| d.get("_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();
    
    for id in committed.keys() {
        if !db_ids.contains(id) {
            missing.push(id.clone());
        }
    }
    
    for id in &db_ids {
        if !committed.contains_key(id) {
            extra.push(id.clone());
        }
    }
    
    if !missing.is_empty() {
        eprintln!("MISSING from DB: {:?}", &missing[..missing.len().min(10)]);
    }
    if !extra.is_empty() {
        eprintln!("EXTRA in DB: {:?}", &extra[..extra.len().min(10)]);
    }
    
    if missing.is_empty() && extra.is_empty() {
        eprintln!("TEST PASSED!");
    } else {
        eprintln!("TEST FAILED!");
        std::process::exit(1);
    }
}
