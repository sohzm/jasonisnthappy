use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn random_string(length: usize) -> String {
    use rand::Rng;
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

fn benchmark<F>(name: &str, iterations: usize, mut f: F)
where
    F: FnMut(),
{
    let mut times = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let start = Instant::now();
        f();
        times.push(start.elapsed());
    }

    let total: Duration = times.iter().sum();
    let avg = total / iterations as u32;
    let min = times.iter().min().unwrap();
    let max = times.iter().max().unwrap();

    println!("{:<30} {:>12} {:>12} {:>12}",
             name,
             format!("{:.3}ms", avg.as_secs_f64() * 1000.0),
             format!("{:.3}ms", min.as_secs_f64() * 1000.0),
             format!("{:.3}ms", max.as_secs_f64() * 1000.0));
}

fn main() {
    println!("\n{:<30} {:>12} {:>12} {:>12}", "Benchmark", "Avg", "Min", "Max");
    println!("{}", "=".repeat(70));

    // Insert1
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();
        let mut i = 0;

        benchmark("Insert1", 20, || {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({"seq": i, "data": random_string(100)});
            collection.insert(doc).unwrap();
            tx.commit().unwrap();
            i += 1;
        });
    }

    // Insert10
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();
        let mut i = 0;

        benchmark("Insert10", 20, || {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            for _ in 0..10 {
                let doc = json!({"seq": i, "data": random_string(100)});
                collection.insert(doc).unwrap();
                i += 1;
            }
            tx.commit().unwrap();
        });
    }

    // Insert50
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();
        let mut i = 0;

        benchmark("Insert50", 15, || {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            for _ in 0..50 {
                let doc = json!({"seq": i, "data": random_string(100)});
                collection.insert(doc).unwrap();
                i += 1;
            }
            tx.commit().unwrap();
        });
    }

    // Insert100
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();
        let mut i = 0;

        benchmark("Insert100", 15, || {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            for _ in 0..100 {
                let doc = json!({"seq": i, "data": random_string(100)});
                collection.insert(doc).unwrap();
                i += 1;
            }
            tx.commit().unwrap();
        });
    }

    // Insert500
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();
        let mut i = 0;

        benchmark("Insert500", 10, || {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            for _ in 0..500 {
                let doc = json!({"seq": i, "data": random_string(100)});
                collection.insert(doc).unwrap();
                i += 1;
            }
            tx.commit().unwrap();
        });
    }

    // Insert1000 - THE BIG ONE
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();
        let mut i = 0;

        benchmark("Insert1000", 10, || {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            for _ in 0..1000 {
                let doc = json!({"seq": i, "data": random_string(100)});
                collection.insert(doc).unwrap();
                i += 1;
            }
            tx.commit().unwrap();
        });
    }

    println!("\n✅ Insert benchmarks completed!");
    println!("\n🎯 Target: Insert1000 < 34ms (Go's time)");
}
