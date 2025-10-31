use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
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

// New function that mimics Go's b.RunParallel behavior
// Creates a worker pool once and distributes work across threads using atomic counter
fn benchmark_parallel<F>(name: &str, total_ops: usize, concurrency: usize, f: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let f = Arc::new(f);
    let counter = Arc::new(AtomicUsize::new(0));

    // Start timing BEFORE spawning threads
    let start = Instant::now();

    // Spawn worker pool ONCE (not per iteration!)
    let handles: Vec<_> = (0..concurrency)
        .map(|_| {
            let counter = Arc::clone(&counter);
            let work = Arc::clone(&f);
            thread::spawn(move || {
                // Each worker processes operations until total_ops is reached
                loop {
                    let current = counter.fetch_add(1, Ordering::SeqCst);
                    if current >= total_ops {
                        break;
                    }
                    work();
                }
            })
        })
        .collect();

    // Wait for all workers to finish
    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();

    // Report average time per operation (total time / num ops)
    let avg_per_op = elapsed / total_ops as u32;

    // For consistency with the benchmark function, report: Avg, Min, Max (all same for single run)
    println!("{:<30} {:>12} {:>12} {:>12}",
             name,
             format!("{:.3}ms", avg_per_op.as_secs_f64() * 1000.0),
             format!("{:.3}ms", avg_per_op.as_secs_f64() * 1000.0),
             format!("{:.3}ms", avg_per_op.as_secs_f64() * 1000.0));
}

fn main() {
    println!("\n{:<30} {:>12} {:>12} {:>12}", "Benchmark", "Avg", "Min", "Max");
    println!("{}", "=".repeat(70));

    // WriteOnly_C1_Doc100
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();
        let db = Arc::new(db);
        let mut doc_num = 0;

        benchmark("WriteOnly_C1_Doc100", 20, || {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({
                "seq": doc_num,
                "data": random_string(100)
            });
            collection.insert(doc).unwrap();
            tx.commit().unwrap();
            doc_num += 1;
        });
    }

    // WriteOnly_C4_Doc100 - Fixed to use worker pool
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();
        let db = Arc::new(db);
        let counter = Arc::new(Mutex::new(0));

        benchmark_parallel("WriteOnly_C4_Doc100", 80, 4, move || {
            let db = Arc::clone(&db);
            let counter = Arc::clone(&counter);

            let seq = {
                let mut c = counter.lock().unwrap();
                let val = *c;
                *c += 1;
                val
            };

            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({
                "seq": seq,
                "data": random_string(100)
            });
            if collection.insert(doc).is_ok() {
                let _ = tx.commit();
            }
        });
    }

    // WriteOnly_C16_Doc100 - Fixed to use worker pool
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();
        let db = Arc::new(db);
        let counter = Arc::new(Mutex::new(0));

        benchmark_parallel("WriteOnly_C16_Doc100", 160, 16, move || {
            let db = Arc::clone(&db);
            let counter = Arc::clone(&counter);

            let seq = {
                let mut c = counter.lock().unwrap();
                let val = *c;
                *c += 1;
                val
            };

            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({
                "seq": seq,
                "data": random_string(100)
            });
            if collection.insert(doc).is_ok() {
                let _ = tx.commit();
            }
        });
    }

    // Read1500_C1 - pre-populate first
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();

        let mut doc_ids = Vec::new();
        for i in 0..1500 {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({"seq": i, "data": random_string(100)});
            let id = collection.insert(doc).unwrap();
            doc_ids.push(id);
            tx.commit().unwrap();
        }

        let db = Arc::new(db);
        let mut i = 0;
        benchmark("Read1500_C1", 1000, || {
            let mut tx = db.begin().unwrap();
            let collection = tx.collection("bench").unwrap();
            let _ = collection.find_by_id(&doc_ids[i % 1500]);
            let _ = tx.rollback();
            i += 1;
        });
    }

    // Read1500_C4 - Fixed to use worker pool
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();

        let mut doc_ids = Vec::new();
        for i in 0..1500 {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({"seq": i, "data": random_string(100)});
            let id = collection.insert(doc).unwrap();
            doc_ids.push(id);
            tx.commit().unwrap();
        }

        let db = Arc::new(db);
        let doc_ids = Arc::new(doc_ids);
        let counter = Arc::new(Mutex::new(0));

        benchmark_parallel("Read1500_C4", 400, 4, move || {
            let db = Arc::clone(&db);
            let doc_ids = Arc::clone(&doc_ids);
            let counter = Arc::clone(&counter);

            let i = {
                let mut c = counter.lock().unwrap();
                let val = *c;
                *c += 1;
                val
            };

            let mut tx = db.begin().unwrap();
            let collection = tx.collection("bench").unwrap();
            let _ = collection.find_by_id(&doc_ids[i % 1500]);
            let _ = tx.rollback();
        });
    }

    // Read1500_C16 - Fixed to use worker pool
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();

        let mut doc_ids = Vec::new();
        for i in 0..1500 {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({"seq": i, "data": random_string(100)});
            let id = collection.insert(doc).unwrap();
            doc_ids.push(id);
            tx.commit().unwrap();
        }

        let db = Arc::new(db);
        let doc_ids = Arc::new(doc_ids);
        let counter = Arc::new(Mutex::new(0));

        benchmark_parallel("Read1500_C16", 800, 16, move || {
            let db = Arc::clone(&db);
            let doc_ids = Arc::clone(&doc_ids);
            let counter = Arc::clone(&counter);

            let i = {
                let mut c = counter.lock().unwrap();
                let val = *c;
                *c += 1;
                val
            };

            let mut tx = db.begin().unwrap();
            let collection = tx.collection("bench").unwrap();
            let _ = collection.find_by_id(&doc_ids[i % 1500]);
            let _ = tx.rollback();
        });
    }

    // Insert1
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();

        benchmark("Insert1", 20, || {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({"seq": 0, "data": random_string(100)});
            collection.insert(doc).unwrap();
            tx.commit().unwrap();
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

    // Insert1000
    {
        // Open ONE database outside benchmark (not timed) - like Go does
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

    // Find100
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();

        let mut doc_ids = Vec::new();
        for i in 0..100 {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({"seq": i, "data": random_string(100)});
            let id = collection.insert(doc).unwrap();
            doc_ids.push(id);
            tx.commit().unwrap();
        }

        let mut i = 0;
        benchmark("Find100", 500, || {
            let mut tx = db.begin().unwrap();
            let collection = tx.collection("bench").unwrap();
            let _ = collection.find_by_id(&doc_ids[i % 100]);
            let _ = tx.rollback();
            i += 1;
        });
    }

    // Find1500
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();

        let mut doc_ids = Vec::new();
        for i in 0..1500 {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({"seq": i, "data": random_string(100)});
            let id = collection.insert(doc).unwrap();
            doc_ids.push(id);
            tx.commit().unwrap();
        }

        let mut i = 0;
        benchmark("Find1500", 500, || {
            let mut tx = db.begin().unwrap();
            let collection = tx.collection("bench").unwrap();
            let _ = collection.find_by_id(&doc_ids[i % 1500]);
            let _ = tx.rollback();
            i += 1;
        });
    }

    // Find2500
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();

        let mut doc_ids = Vec::new();
        for i in 0..2500 {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({"seq": i, "data": random_string(100)});
            let id = collection.insert(doc).unwrap();
            doc_ids.push(id);
            tx.commit().unwrap();
        }

        let mut i = 0;
        benchmark("Find2500", 500, || {
            let mut tx = db.begin().unwrap();
            let collection = tx.collection("bench").unwrap();
            let _ = collection.find_by_id(&doc_ids[i % 2500]);
            let _ = tx.rollback();
            i += 1;
        });
    }

    // Update
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();

        let mut doc_ids = Vec::new();
        for i in 0..100 {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let doc = json!({"seq": i, "data": random_string(100), "age": 30});
            let id = collection.insert(doc).unwrap();
            doc_ids.push(id);
            tx.commit().unwrap();
        }

        let mut i = 0;
        benchmark("Update", 100, || {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("bench").unwrap();
            let update = json!({"age": 31});
            let _ = collection.update_by_id(&doc_ids[i % 100], update);
            let _ = tx.commit();
            i += 1;
        });
    }

    println!("\n✅ All benchmarks completed!");
}
