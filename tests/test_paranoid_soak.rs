//! Paranoid Soak Test - Long-running data integrity verification
//!
//! This test is designed to run for hours and catch any data loss or corruption.
//! It maintains a "ground truth" model and continuously verifies the database matches.
//!
//! Run with: cargo test --test test_paranoid_soak -- --ignored --nocapture 2>&1 | tee /tmp/soak_test.log
//!
//! Or for a specific duration:
//! SOAK_DURATION_SECS=3600 cargo test --test test_paranoid_soak -- --ignored --nocapture

use jasonisnthappy::Database;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use rand::{thread_rng, Rng, seq::SliceRandom};
use rand::distributions::Alphanumeric;

// ============================================================================
// CONFIGURATION
// ============================================================================

const DEFAULT_DURATION_SECS: u64 = 300; // 5 minutes default, override with SOAK_DURATION_SECS
const NUM_WRITER_THREADS: usize = 8;
const NUM_READER_THREADS: usize = 4;
const VERIFY_INTERVAL_SECS: u64 = 30; // Full verification every 30 seconds
const CHECKPOINT_INTERVAL_SECS: u64 = 60;
const LOG_INTERVAL_SECS: u64 = 10;

// Document size distribution (realistic workload)
const SMALL_DOC_CHANCE: f64 = 0.70;   // 70% small docs (100 bytes - 10KB)
const MEDIUM_DOC_CHANCE: f64 = 0.25;  // 25% medium docs (10KB - 500KB)
const LARGE_DOC_CHANCE: f64 = 0.05;   // 5% large docs (500KB - 3MB)

// Operation distribution
const INSERT_CHANCE: f64 = 0.50;      // 50% inserts
const UPDATE_CHANCE: f64 = 0.25;      // 25% updates
const DELETE_CHANCE: f64 = 0.10;      // 10% deletes
const READ_CHANCE: f64 = 0.15;        // 15% point reads (in writers)

// Collections to use
const COLLECTIONS: &[&str] = &["users", "products", "orders", "events", "analytics"];

// ============================================================================
// GROUND TRUTH MODEL
// ============================================================================

#[derive(Debug, Clone)]
struct Document {
    id: String,
    data: Value,
    version: u64,
}

struct GroundTruth {
    collections: HashMap<String, HashMap<String, Document>>,
    next_version: u64,
}

impl GroundTruth {
    fn new() -> Self {
        let mut collections = HashMap::new();
        for coll in COLLECTIONS {
            collections.insert(coll.to_string(), HashMap::new());
        }
        Self {
            collections,
            next_version: 1,
        }
    }

    fn insert(&mut self, collection: &str, id: &str, data: Value) {
        let version = self.next_version;
        self.next_version += 1;

        if let Some(coll) = self.collections.get_mut(collection) {
            coll.insert(id.to_string(), Document {
                id: id.to_string(),
                data,
                version,
            });
        }
    }

    fn update(&mut self, collection: &str, id: &str, updates: Value) -> bool {
        if let Some(coll) = self.collections.get_mut(collection) {
            if let Some(doc) = coll.get_mut(id) {
                if let (Some(doc_obj), Some(updates_obj)) = (doc.data.as_object_mut(), updates.as_object()) {
                    for (key, value) in updates_obj {
                        doc_obj.insert(key.clone(), value.clone());
                    }
                    doc.version = self.next_version;
                    self.next_version += 1;
                    return true;
                }
            }
        }
        false
    }

    fn delete(&mut self, collection: &str, id: &str) -> bool {
        if let Some(coll) = self.collections.get_mut(collection) {
            coll.remove(id).is_some()
        } else {
            false
        }
    }

    fn get(&self, collection: &str, id: &str) -> Option<&Document> {
        self.collections.get(collection).and_then(|c| c.get(id))
    }

    fn get_random_id(&self, collection: &str) -> Option<String> {
        self.collections.get(collection).and_then(|c| {
            let ids: Vec<_> = c.keys().collect();
            if ids.is_empty() {
                None
            } else {
                let mut rng = thread_rng();
                Some(ids.choose(&mut rng).unwrap().to_string())
            }
        })
    }

    fn count(&self, collection: &str) -> usize {
        self.collections.get(collection).map(|c| c.len()).unwrap_or(0)
    }

    fn total_count(&self) -> usize {
        self.collections.values().map(|c| c.len()).sum()
    }
}

// ============================================================================
// DOCUMENT GENERATORS (Realistic Data)
// ============================================================================

fn generate_random_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn pick<'a>(choices: &[&'a str]) -> &'a str {
    choices[thread_rng().gen_range(0..choices.len())]
}

fn generate_user_doc(id: &str, size_class: &str) -> Value {
    let mut rng = thread_rng();

    let bio_size = match size_class {
        "small" => rng.gen_range(10..500),
        "medium" => rng.gen_range(5_000..50_000),
        "large" => rng.gen_range(500_000..2_000_000),
        _ => 100,
    };

    let theme = pick(&["dark", "light", "auto"]);
    let language = pick(&["en", "es", "fr", "de", "ja"]);

    json!({
        "_id": id,
        "type": "user",
        "email": format!("{}@example.com", generate_random_string(10)),
        "name": {
            "first": generate_random_string(8),
            "last": generate_random_string(12),
        },
        "age": rng.gen_range(18..80),
        "created_at": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        "preferences": {
            "theme": theme,
            "language": language,
            "notifications": rng.gen_bool(0.7),
        },
        "bio": generate_random_string(bio_size),
        "tags": (0..rng.gen_range(1..10)).map(|_| generate_random_string(8)).collect::<Vec<_>>(),
    })
}

fn generate_product_doc(id: &str, size_class: &str) -> Value {
    let mut rng = thread_rng();

    let description_size = match size_class {
        "small" => rng.gen_range(50..500),
        "medium" => rng.gen_range(10_000..100_000),
        "large" => rng.gen_range(500_000..2_500_000),
        _ => 200,
    };

    let num_images = match size_class {
        "small" => rng.gen_range(1..3),
        "medium" => rng.gen_range(5..20),
        "large" => rng.gen_range(20..50),
        _ => 2,
    };

    let currency = pick(&["USD", "EUR", "GBP", "JPY"]);
    let category = pick(&["electronics", "clothing", "home", "sports", "books"]);

    json!({
        "_id": id,
        "type": "product",
        "sku": format!("SKU-{}", generate_random_string(10)),
        "name": generate_random_string(20),
        "price": rng.gen_range(1.0..10000.0_f64),
        "currency": currency,
        "category": category,
        "in_stock": rng.gen_bool(0.8),
        "quantity": rng.gen_range(0..1000),
        "description": generate_random_string(description_size),
        "images": (0..num_images).map(|i| json!({
            "url": format!("https://cdn.example.com/products/{}/{}.jpg", id, i),
            "alt": generate_random_string(20),
            "width": thread_rng().gen_range(100..4000),
            "height": thread_rng().gen_range(100..4000),
        })).collect::<Vec<_>>(),
        "ratings": {
            "average": rng.gen_range(1.0..5.0_f64),
            "count": rng.gen_range(0..10000),
        },
    })
}

fn generate_order_doc(id: &str, size_class: &str) -> Value {
    let mut rng = thread_rng();

    let num_items = match size_class {
        "small" => rng.gen_range(1..5),
        "medium" => rng.gen_range(10..100),
        "large" => rng.gen_range(500..2000),
        _ => 3,
    };

    let status = pick(&["pending", "processing", "shipped", "delivered", "cancelled"]);
    let payment_method = pick(&["credit_card", "paypal", "bank_transfer"]);
    let payment_status = pick(&["pending", "completed", "failed"]);

    json!({
        "_id": id,
        "type": "order",
        "user_id": format!("user_{}", rng.gen_range(1..100000)),
        "status": status,
        "created_at": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        "shipping_address": {
            "street": format!("{} {} St", rng.gen_range(1..9999), generate_random_string(10)),
            "city": generate_random_string(12),
            "state": generate_random_string(2).to_uppercase(),
            "zip": format!("{:05}", rng.gen_range(10000..99999)),
            "country": "US",
        },
        "items": (0..num_items).map(|_| json!({
            "product_id": format!("prod_{}", thread_rng().gen_range(1..50000)),
            "quantity": thread_rng().gen_range(1..10),
            "price": thread_rng().gen_range(1.0..500.0_f64),
            "name": generate_random_string(15),
        })).collect::<Vec<_>>(),
        "total": rng.gen_range(10.0..5000.0_f64),
        "payment": {
            "method": payment_method,
            "status": payment_status,
        },
    })
}

fn generate_event_doc(id: &str, size_class: &str) -> Value {
    let mut rng = thread_rng();

    let payload_size = match size_class {
        "small" => rng.gen_range(50..500),
        "medium" => rng.gen_range(5_000..50_000),
        "large" => rng.gen_range(500_000..3_000_000),
        _ => 100,
    };

    let event_type = pick(&["click", "view", "purchase", "signup", "logout"]);
    let device_type = pick(&["mobile", "desktop", "tablet"]);
    let os = pick(&["iOS", "Android", "Windows", "macOS", "Linux"]);
    let browser = pick(&["Chrome", "Safari", "Firefox", "Edge"]);
    let country = pick(&["US", "UK", "DE", "FR", "JP", "BR"]);

    json!({
        "_id": id,
        "type": "event",
        "event_type": event_type,
        "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
        "user_id": format!("user_{}", rng.gen_range(1..100000)),
        "session_id": generate_random_string(32),
        "device": {
            "type": device_type,
            "os": os,
            "browser": browser,
        },
        "location": {
            "country": country,
            "city": generate_random_string(10),
            "ip": format!("{}.{}.{}.{}", rng.gen_range(1..255), rng.gen_range(0..255), rng.gen_range(0..255), rng.gen_range(0..255)),
        },
        "payload": generate_random_string(payload_size),
    })
}

fn generate_analytics_doc(id: &str, size_class: &str) -> Value {
    let mut rng = thread_rng();

    let num_data_points = match size_class {
        "small" => rng.gen_range(10..100),
        "medium" => rng.gen_range(500..5000),
        "large" => rng.gen_range(10000..50000),
        _ => 50,
    };

    let metric = pick(&["page_views", "conversions", "revenue", "sessions", "bounce_rate"]);
    let aggregation = pick(&["hourly", "daily", "weekly", "monthly"]);

    json!({
        "_id": id,
        "type": "analytics",
        "metric": metric,
        "period": {
            "start": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() - 86400,
            "end": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        },
        "aggregation": aggregation,
        "data_points": (0..num_data_points).map(|i| json!({
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() - (i * 3600),
            "value": thread_rng().gen_range(0.0..10000.0_f64),
            "count": thread_rng().gen_range(0..100000),
        })).collect::<Vec<_>>(),
        "summary": {
            "total": rng.gen_range(0.0..1000000.0_f64),
            "average": rng.gen_range(0.0..10000.0_f64),
            "min": rng.gen_range(0.0..100.0_f64),
            "max": rng.gen_range(100.0..100000.0_f64),
        },
    })
}

fn generate_document(collection: &str, id: &str) -> Value {
    let mut rng = thread_rng();
    let roll: f64 = rng.gen();

    let size_class = if roll < SMALL_DOC_CHANCE {
        "small"
    } else if roll < SMALL_DOC_CHANCE + MEDIUM_DOC_CHANCE {
        "medium"
    } else {
        "large"
    };

    match collection {
        "users" => generate_user_doc(id, size_class),
        "products" => generate_product_doc(id, size_class),
        "orders" => generate_order_doc(id, size_class),
        "events" => generate_event_doc(id, size_class),
        "analytics" => generate_analytics_doc(id, size_class),
        _ => generate_user_doc(id, size_class),
    }
}

fn generate_update() -> Value {
    let mut rng = thread_rng();
    json!({
        "updated_at": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        "update_note": generate_random_string(rng.gen_range(10..100)),
    })
}

// ============================================================================
// STATISTICS
// ============================================================================

struct Stats {
    inserts: AtomicU64,
    updates: AtomicU64,
    deletes: AtomicU64,
    reads: AtomicU64,
    insert_failures: AtomicU64,
    update_failures: AtomicU64,
    delete_failures: AtomicU64,
    read_failures: AtomicU64,
    verification_passes: AtomicU64,
    verification_failures: AtomicU64,
    bytes_written: AtomicU64,
    // Verification pause synchronization
    verifying: AtomicBool,
    workers_paused: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            insert_failures: AtomicU64::new(0),
            update_failures: AtomicU64::new(0),
            delete_failures: AtomicU64::new(0),
            read_failures: AtomicU64::new(0),
            verification_passes: AtomicU64::new(0),
            verification_failures: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            verifying: AtomicBool::new(false),
            workers_paused: AtomicU64::new(0),
        }
    }

    fn wait_for_verification_pause(&self, num_workers: u64) {
        // Set verifying flag
        self.verifying.store(true, Ordering::SeqCst);
        // Wait for all workers to pause
        while self.workers_paused.load(Ordering::SeqCst) < num_workers {
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn resume_workers(&self) {
        self.verifying.store(false, Ordering::SeqCst);
        // Wait for workers to resume
        while self.workers_paused.load(Ordering::SeqCst) > 0 {
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn check_pause(&self) {
        if self.verifying.load(Ordering::SeqCst) {
            self.workers_paused.fetch_add(1, Ordering::SeqCst);
            while self.verifying.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(5));
            }
            self.workers_paused.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn summary(&self) -> String {
        format!(
            "Inserts: {} (failed: {}), Updates: {} (failed: {}), Deletes: {} (failed: {}), Reads: {} (failed: {}), Verifications: {} passed / {} failed, Bytes written: {:.2} MB",
            self.inserts.load(Ordering::Relaxed),
            self.insert_failures.load(Ordering::Relaxed),
            self.updates.load(Ordering::Relaxed),
            self.update_failures.load(Ordering::Relaxed),
            self.deletes.load(Ordering::Relaxed),
            self.delete_failures.load(Ordering::Relaxed),
            self.reads.load(Ordering::Relaxed),
            self.read_failures.load(Ordering::Relaxed),
            self.verification_passes.load(Ordering::Relaxed),
            self.verification_failures.load(Ordering::Relaxed),
            self.bytes_written.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        )
    }
}

// ============================================================================
// LOGGER
// ============================================================================

struct Logger {
    file: File,
    start_time: Instant,
}

impl Logger {
    fn new(path: &str) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .expect("Failed to create log file");

        Self {
            file,
            start_time: Instant::now(),
        }
    }

    fn log(&mut self, msg: &str) {
        let elapsed = self.start_time.elapsed().as_secs();
        let timestamp = format!("[{:02}:{:02}:{:02}]", elapsed / 3600, (elapsed % 3600) / 60, elapsed % 60);
        let line = format!("{} {}\n", timestamp, msg);
        print!("{}", line);
        let _ = self.file.write_all(line.as_bytes());
        let _ = self.file.flush();
    }
}

// ============================================================================
// VERIFICATION
// ============================================================================

/// Compare two JSON values with tolerance for floating point differences
fn json_values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Object(a_map), Value::Object(b_map)) => {
            if a_map.len() != b_map.len() {
                return false;
            }
            for (key, a_val) in a_map {
                match b_map.get(key) {
                    Some(b_val) => {
                        if !json_values_equal(a_val, b_val) {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
            true
        }
        (Value::Array(a_arr), Value::Array(b_arr)) => {
            if a_arr.len() != b_arr.len() {
                return false;
            }
            for (a_val, b_val) in a_arr.iter().zip(b_arr.iter()) {
                if !json_values_equal(a_val, b_val) {
                    return false;
                }
            }
            true
        }
        (Value::Number(a_num), Value::Number(b_num)) => {
            // For floating point, compare with tolerance
            match (a_num.as_f64(), b_num.as_f64()) {
                (Some(a_f), Some(b_f)) => {
                    if a_f == b_f {
                        return true;
                    }
                    // Allow small relative difference
                    let diff = (a_f - b_f).abs();
                    let max_val = a_f.abs().max(b_f.abs());
                    if max_val == 0.0 {
                        diff < 1e-10
                    } else {
                        diff / max_val < 1e-10
                    }
                }
                _ => a_num == b_num,
            }
        }
        _ => a == b,
    }
}

fn verify_collection(
    db: &Database,
    truth: &GroundTruth,
    collection: &str,
) -> Result<(), String> {
    let mut tx = db.begin().map_err(|e| format!("Failed to begin tx: {}", e))?;
    let coll = tx.collection(collection).map_err(|e| format!("Failed to get collection: {}", e))?;

    let db_docs = coll.find_all().map_err(|e| format!("Failed to find_all: {}", e))?;

    // Build maps for comparison
    let mut db_map: HashMap<String, Value> = HashMap::new();
    for doc in &db_docs {
        if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
            db_map.insert(id.to_string(), doc.clone());
        }
    }

    let truth_docs = truth.collections.get(collection).ok_or("Collection not in truth")?;

    // Check counts match
    if db_map.len() != truth_docs.len() {
        // Find which docs are missing
        let missing_from_db: Vec<_> = truth_docs.keys()
            .filter(|id| !db_map.contains_key(*id))
            .take(5)
            .collect();
        let extra_in_db: Vec<_> = db_map.keys()
            .filter(|id| !truth_docs.contains_key(*id))
            .take(5)
            .collect();

        return Err(format!(
            "Count mismatch in '{}': DB has {}, truth has {}\n      Missing from DB (first 5): {:?}\n      Extra in DB (first 5): {:?}",
            collection, db_map.len(), truth_docs.len(), missing_from_db, extra_in_db
        ));
    }

    // Check for missing documents
    for (id, truth_doc) in truth_docs {
        if !db_map.contains_key(id) {
            return Err(format!("Missing document in '{}': {}", collection, id));
        }

        // Verify content matches
        // Use JSON value comparison which handles float precision better
        if !json_values_equal(&db_map[id], &truth_doc.data) {
            let db_json = serde_json::to_string(&db_map[id]).unwrap();
            let truth_json = serde_json::to_string(&truth_doc.data).unwrap();
            return Err(format!(
                "Content mismatch in '{}' for doc '{}'\nDB: {}\nTruth: {}",
                collection, id,
                &db_json[..db_json.len().min(200)],
                &truth_json[..truth_json.len().min(200)]
            ));
        }
    }

    // Check for extra documents (shouldn't exist in DB but not in truth)
    for id in db_map.keys() {
        if !truth_docs.contains_key(id) {
            return Err(format!("Extra document in '{}': {} (not in truth)", collection, id));
        }
    }

    Ok(())
}

fn verify_all(
    db: &Database,
    truth: &GroundTruth,
    stats: &Stats,
    logger: &mut Logger,
) -> bool {
    logger.log("Starting full verification...");

    let mut all_passed = true;

    for collection in COLLECTIONS {
        match verify_collection(db, truth, collection) {
            Ok(()) => {
                logger.log(&format!("  {} OK ({} docs)", collection, truth.count(collection)));
            }
            Err(e) => {
                logger.log(&format!("  {} FAILED: {}", collection, e));
                all_passed = false;
            }
        }
    }

    if all_passed {
        stats.verification_passes.fetch_add(1, Ordering::Relaxed);
        logger.log(&format!("Verification PASSED (total: {} docs)", truth.total_count()));
    } else {
        stats.verification_failures.fetch_add(1, Ordering::Relaxed);
        logger.log("Verification FAILED!");
    }

    all_passed
}

// ============================================================================
// WORKER THREADS
// ============================================================================

fn writer_thread(
    thread_id: usize,
    db: Arc<Database>,
    truth: Arc<RwLock<GroundTruth>>,
    stats: Arc<Stats>,
    stop_flag: Arc<AtomicBool>,
    doc_counter: Arc<AtomicU64>,
) {
    let mut rng = thread_rng();

    while !stop_flag.load(Ordering::Relaxed) {
        // Check if we should pause for verification
        stats.check_pause();

        let collection = COLLECTIONS[rng.gen_range(0..COLLECTIONS.len())];
        let roll: f64 = rng.gen();

        if roll < INSERT_CHANCE {
            // INSERT
            let doc_num = doc_counter.fetch_add(1, Ordering::Relaxed);
            let doc_id = format!("doc_{}_{}", thread_id, doc_num);
            let doc = generate_document(collection, &doc_id);
            let doc_size = serde_json::to_string(&doc).unwrap().len();

            match db.begin() {
                Ok(mut tx) => {
                    match tx.collection(collection) {
                        Ok(mut coll) => {
                            match coll.insert(doc.clone()) {
                                Ok(_) => {
                                    match tx.commit() {
                                        Ok(_) => {
                                            // Success! Update ground truth
                                            let mut truth = truth.write().unwrap();
                                            truth.insert(collection, &doc_id, doc);
                                            stats.inserts.fetch_add(1, Ordering::Relaxed);
                                            stats.bytes_written.fetch_add(doc_size as u64, Ordering::Relaxed);
                                        }
                                        Err(_) => {
                                            stats.insert_failures.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                                Err(_) => {
                                    let _ = tx.rollback();
                                    stats.insert_failures.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(_) => {
                            stats.insert_failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Err(_) => {
                    stats.insert_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else if roll < INSERT_CHANCE + UPDATE_CHANCE {
            // UPDATE
            let maybe_id = {
                let truth = truth.read().unwrap();
                truth.get_random_id(collection)
            };

            if let Some(doc_id) = maybe_id {
                let updates = generate_update();

                match db.begin() {
                    Ok(mut tx) => {
                        match tx.collection(collection) {
                            Ok(mut coll) => {
                                match coll.update_by_id(&doc_id, updates.clone()) {
                                    Ok(_) => {
                                        match tx.commit() {
                                            Ok(_) => {
                                                let mut truth = truth.write().unwrap();
                                                truth.update(collection, &doc_id, updates);
                                                stats.updates.fetch_add(1, Ordering::Relaxed);
                                            }
                                            Err(_) => {
                                                stats.update_failures.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        let _ = tx.rollback();
                                        stats.update_failures.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            Err(_) => {
                                stats.update_failures.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {
                        stats.update_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        } else if roll < INSERT_CHANCE + UPDATE_CHANCE + DELETE_CHANCE {
            // DELETE
            let maybe_id = {
                let truth = truth.read().unwrap();
                truth.get_random_id(collection)
            };

            if let Some(doc_id) = maybe_id {
                match db.begin() {
                    Ok(mut tx) => {
                        match tx.collection(collection) {
                            Ok(mut coll) => {
                                match coll.delete_by_id(&doc_id) {
                                    Ok(_) => {
                                        match tx.commit() {
                                            Ok(_) => {
                                                let mut truth = truth.write().unwrap();
                                                truth.delete(collection, &doc_id);
                                                stats.deletes.fetch_add(1, Ordering::Relaxed);
                                            }
                                            Err(_) => {
                                                stats.delete_failures.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        let _ = tx.rollback();
                                        stats.delete_failures.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            Err(_) => {
                                stats.delete_failures.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {
                        stats.delete_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        } else {
            // POINT READ (verify single document)
            let maybe_id = {
                let truth = truth.read().unwrap();
                truth.get_random_id(collection)
            };

            if let Some(doc_id) = maybe_id {
                match db.begin() {
                    Ok(mut tx) => {
                        match tx.collection(collection) {
                            Ok(coll) => {
                                match coll.find_by_id(&doc_id) {
                                    Ok(db_doc) => {
                                        // Verify it matches truth
                                        let truth = truth.read().unwrap();
                                        if let Some(truth_doc) = truth.get(collection, &doc_id) {
                                            let db_json = serde_json::to_string(&db_doc).unwrap();
                                            let truth_json = serde_json::to_string(&truth_doc.data).unwrap();
                                            if db_json == truth_json {
                                                stats.reads.fetch_add(1, Ordering::Relaxed);
                                            } else {
                                                stats.read_failures.fetch_add(1, Ordering::Relaxed);
                                            }
                                        } else {
                                            // Doc was deleted between getting ID and reading
                                            stats.reads.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                    Err(_) => {
                                        // Might have been deleted
                                        stats.reads.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            Err(_) => {
                                stats.read_failures.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {
                        stats.read_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        // Small sleep to avoid overwhelming
        thread::sleep(Duration::from_micros(rng.gen_range(100..1000)));
    }
}

fn reader_thread(
    _thread_id: usize,
    db: Arc<Database>,
    truth: Arc<RwLock<GroundTruth>>,
    stats: Arc<Stats>,
    stop_flag: Arc<AtomicBool>,
) {
    let mut rng = thread_rng();

    while !stop_flag.load(Ordering::Relaxed) {
        // Check if we should pause for verification
        stats.check_pause();

        let collection = COLLECTIONS[rng.gen_range(0..COLLECTIONS.len())];

        // Random read pattern: either point read or small range
        if rng.gen_bool(0.7) {
            // Point read
            let maybe_id = {
                let truth = truth.read().unwrap();
                truth.get_random_id(collection)
            };

            if let Some(doc_id) = maybe_id {
                match db.begin() {
                    Ok(mut tx) => {
                        if let Ok(coll) = tx.collection(collection) {
                            if coll.find_by_id(&doc_id).is_ok() {
                                stats.reads.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {}
                }
            }
        } else {
            // Collection scan (find_all but just count)
            match db.begin() {
                Ok(mut tx) => {
                    if let Ok(coll) = tx.collection(collection) {
                        if coll.find_all().is_ok() {
                            stats.reads.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Err(_) => {}
            }
        }

        thread::sleep(Duration::from_millis(rng.gen_range(10..100)));
    }
}

// ============================================================================
// MAIN TEST
// ============================================================================

#[test]
#[ignore] // Run with: cargo test --test test_paranoid_soak -- --ignored --nocapture
fn test_paranoid_soak() {
    let duration_secs = std::env::var("SOAK_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_DURATION_SECS);

    let log_path = "/tmp/paranoid_soak_test.log";
    let db_path = "/tmp/paranoid_soak_test.db";

    // Cleanup previous run
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{}.lock", db_path));
    let _ = std::fs::remove_file(format!("{}-wal", db_path));

    let mut logger = Logger::new(log_path);

    logger.log("=================================================================");
    logger.log("PARANOID SOAK TEST - Data Integrity Verification");
    logger.log("=================================================================");
    logger.log(&format!("Duration: {} seconds ({:.1} hours)", duration_secs, duration_secs as f64 / 3600.0));
    logger.log(&format!("Writer threads: {}", NUM_WRITER_THREADS));
    logger.log(&format!("Reader threads: {}", NUM_READER_THREADS));
    logger.log(&format!("Verification interval: {} seconds", VERIFY_INTERVAL_SECS));
    logger.log(&format!("Database: {}", db_path));
    logger.log(&format!("Log file: {}", log_path));
    logger.log("=================================================================");

    // Initialize
    let db = Arc::new(Database::open(db_path).expect("Failed to open database"));
    let truth = Arc::new(RwLock::new(GroundTruth::new()));
    let stats = Arc::new(Stats::new());
    let stop_flag = Arc::new(AtomicBool::new(false));
    let doc_counter = Arc::new(AtomicU64::new(0));

    let start_time = Instant::now();

    // Spawn writer threads
    let mut handles = Vec::new();
    for i in 0..NUM_WRITER_THREADS {
        let db = Arc::clone(&db);
        let truth = Arc::clone(&truth);
        let stats = Arc::clone(&stats);
        let stop_flag = Arc::clone(&stop_flag);
        let doc_counter = Arc::clone(&doc_counter);

        handles.push(thread::spawn(move || {
            writer_thread(i, db, truth, stats, stop_flag, doc_counter);
        }));
    }

    // Spawn reader threads
    for i in 0..NUM_READER_THREADS {
        let db = Arc::clone(&db);
        let truth = Arc::clone(&truth);
        let stats = Arc::clone(&stats);
        let stop_flag = Arc::clone(&stop_flag);

        handles.push(thread::spawn(move || {
            reader_thread(i, db, truth, stats, stop_flag);
        }));
    }

    // Main monitoring loop
    let mut last_verify = Instant::now();
    let mut last_checkpoint = Instant::now();
    let mut last_log = Instant::now();

    while start_time.elapsed().as_secs() < duration_secs {
        thread::sleep(Duration::from_secs(1));

        // Periodic logging
        if last_log.elapsed().as_secs() >= LOG_INTERVAL_SECS {
            let truth = truth.read().unwrap();
            logger.log(&format!(
                "Status: {} | Docs in truth: {}",
                stats.summary(),
                truth.total_count()
            ));
            last_log = Instant::now();
        }

        // Periodic checkpoint
        if last_checkpoint.elapsed().as_secs() >= CHECKPOINT_INTERVAL_SECS {
            logger.log("Running checkpoint...");
            match db.checkpoint() {
                Ok(_) => logger.log("Checkpoint completed"),
                Err(e) => logger.log(&format!("Checkpoint failed: {}", e)),
            }
            last_checkpoint = Instant::now();
        }

        // Periodic full verification
        if last_verify.elapsed().as_secs() >= VERIFY_INTERVAL_SECS {
            // Pause all workers during verification
            let num_workers = (NUM_WRITER_THREADS + NUM_READER_THREADS) as u64;
            stats.wait_for_verification_pause(num_workers);

            let truth = truth.read().unwrap();
            let passed = verify_all(&db, &truth, &stats, &mut logger);
            drop(truth); // Release read lock before resuming

            // Resume workers
            stats.resume_workers();

            if !passed {
                logger.log("CRITICAL: Verification failed! Stopping test.");
                stop_flag.store(true, Ordering::Relaxed);
                break;
            }
            last_verify = Instant::now();
        }
    }

    // Stop all threads
    logger.log("Stopping worker threads...");
    stop_flag.store(true, Ordering::Relaxed);

    for handle in handles {
        let _ = handle.join();
    }

    // Final checkpoint
    logger.log("Final checkpoint...");
    db.checkpoint().expect("Final checkpoint failed");

    // Final verification
    logger.log("=================================================================");
    logger.log("FINAL VERIFICATION");
    logger.log("=================================================================");

    let truth = truth.read().unwrap();
    let final_passed = verify_all(&db, &truth, &stats, &mut logger);

    // Test reopen (crash simulation)
    logger.log("Testing database reopen...");
    drop(db);

    let db = Database::open(db_path).expect("Failed to reopen database");
    let reopen_passed = verify_all(&db, &truth, &stats, &mut logger);

    // Summary
    logger.log("=================================================================");
    logger.log("FINAL SUMMARY");
    logger.log("=================================================================");
    logger.log(&format!("Duration: {:.1} minutes", start_time.elapsed().as_secs() as f64 / 60.0));
    logger.log(&stats.summary());
    logger.log(&format!("Final verification: {}", if final_passed { "PASSED" } else { "FAILED" }));
    logger.log(&format!("Reopen verification: {}", if reopen_passed { "PASSED" } else { "FAILED" }));
    logger.log("=================================================================");

    if final_passed && reopen_passed {
        logger.log("TEST PASSED - No data loss detected!");
    } else {
        logger.log("TEST FAILED - Data integrity issues detected!");
    }

    assert!(final_passed, "Final verification failed!");
    assert!(reopen_passed, "Reopen verification failed!");
}

// ============================================================================
// QUICK SMOKE TEST (for CI)
// ============================================================================

#[test]
fn test_paranoid_soak_smoke() {
    // Quick 10-second version for regular test runs
    std::env::set_var("SOAK_DURATION_SECS", "10");

    let db_path = "/tmp/paranoid_soak_smoke.db";
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{}.lock", db_path));
    let _ = std::fs::remove_file(format!("{}-wal", db_path));

    let db = Arc::new(Database::open(db_path).expect("Failed to open database"));
    let truth = Arc::new(RwLock::new(GroundTruth::new()));
    let stats = Arc::new(Stats::new());
    let stop_flag = Arc::new(AtomicBool::new(false));
    let doc_counter = Arc::new(AtomicU64::new(0));

    // Run just 2 writers for 5 seconds
    let mut handles = Vec::new();
    for i in 0..2 {
        let db = Arc::clone(&db);
        let truth = Arc::clone(&truth);
        let stats = Arc::clone(&stats);
        let stop_flag = Arc::clone(&stop_flag);
        let doc_counter = Arc::clone(&doc_counter);

        handles.push(thread::spawn(move || {
            writer_thread(i, db, truth, stats, stop_flag, doc_counter);
        }));
    }

    thread::sleep(Duration::from_secs(5));
    stop_flag.store(true, Ordering::Relaxed);

    for handle in handles {
        let _ = handle.join();
    }

    db.checkpoint().unwrap();

    // Verify
    let truth = truth.read().unwrap();
    for collection in COLLECTIONS {
        verify_collection(&db, &truth, collection).expect("Verification failed");
    }

    println!("Smoke test passed: {}", stats.summary());
}
