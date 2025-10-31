use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::time::Instant;
use tempfile::TempDir;

fn random_string(length: usize) -> String {
    use rand::Rng;
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

fn main() {
    println!("Testing batch insert performance with REUSED database (like Go)...\n");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("bench.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    // Test Insert10 - 20 iterations
    let mut times = Vec::new();
    for i in 0..20 {
        let start = Instant::now();
        let mut tx = db.begin().unwrap();
        let mut collection = tx.collection("bench").unwrap();
        for j in 0..10 {
            let doc = json!({"seq": i * 10 + j, "data": random_string(100)});
            collection.insert(doc).unwrap();
        }
        tx.commit().unwrap();
        times.push(start.elapsed());
    }

    let total: std::time::Duration = times.iter().sum();
    let avg = total / 20;
    println!("Insert10 (reused DB):  Avg={:.3}ms  Total={:.3}ms",
             avg.as_secs_f64() * 1000.0,
             total.as_secs_f64() * 1000.0);

    // Test Insert100 - 20 iterations
    let mut times = Vec::new();
    for i in 0..20 {
        let start = Instant::now();
        let mut tx = db.begin().unwrap();
        let mut collection = tx.collection("bench").unwrap();
        for j in 0..100 {
            let doc = json!({"seq": 1000 + i * 100 + j, "data": random_string(100)});
            collection.insert(doc).unwrap();
        }
        tx.commit().unwrap();
        times.push(start.elapsed());
    }

    let total: std::time::Duration = times.iter().sum();
    let avg = total / 20;
    println!("Insert100 (reused DB): Avg={:.3}ms  Total={:.3}ms",
             avg.as_secs_f64() * 1000.0,
             total.as_secs_f64() * 1000.0);

    // Test Insert1000 - 20 iterations
    let mut times = Vec::new();
    for i in 0..20 {
        let start = Instant::now();
        let mut tx = db.begin().unwrap();
        let mut collection = tx.collection("bench").unwrap();
        for j in 0..1000 {
            let doc = json!({"seq": 10000 + i * 1000 + j, "data": random_string(100)});
            collection.insert(doc).unwrap();
        }
        tx.commit().unwrap();
        times.push(start.elapsed());
    }

    let total: std::time::Duration = times.iter().sum();
    let avg = total / 20;
    println!("Insert1000 (reused DB): Avg={:.3}ms  Total={:.3}ms",
             avg.as_secs_f64() * 1000.0,
             total.as_secs_f64() * 1000.0);
}
