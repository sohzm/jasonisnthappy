
use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct WorkloadScenario {
    name: &'static str,
    num_workers: usize,
    ops_per_worker: usize,
    read_write_ratio: f64,
}

#[test]
fn test_realistic_workloads() {
    let scenarios = vec![
        WorkloadScenario {
            name: "SingleWriter",
            num_workers: 1,
            ops_per_worker: 100,
            read_write_ratio: 0.0,
        },
        WorkloadScenario {
            name: "LowConcurrency_3Writers",
            num_workers: 3,
            ops_per_worker: 50,
            read_write_ratio: 0.0,
        },
        WorkloadScenario {
            name: "MediumConcurrency_10Writers",
            num_workers: 10,
            ops_per_worker: 30,
            read_write_ratio: 0.0,
        },
        WorkloadScenario {
            name: "MixedWorkload_5Writers_80PctReads",
            num_workers: 5,
            ops_per_worker: 100,
            read_write_ratio: 0.8,
        },
        WorkloadScenario {
            name: "HighConcurrency_20Writers",
            num_workers: 20,
            ops_per_worker: 25,
            read_write_ratio: 0.0,
        },
    ];

    for scenario in scenarios {
        println!("\n{}", "=".repeat(60));
        println!("Scenario: {}", scenario.name);
        println!("{}", "=".repeat(60));

        run_workload_scenario(&scenario);
    }
}

fn run_workload_scenario(scenario: &WorkloadScenario) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join(format!("{}.db", scenario.name));

    let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let total_write_time = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    let mut handles = vec![];

    for worker_id in 0..scenario.num_workers {
        let db = Arc::clone(&db);
        let success_count = Arc::clone(&success_count);
        let error_count = Arc::clone(&error_count);
        let total_write_time = Arc::clone(&total_write_time);
        let ops_per_worker = scenario.ops_per_worker;
        let read_write_ratio = scenario.read_write_ratio;

        let handle = thread::spawn(move || {
            for op_id in 0..ops_per_worker {
                if read_write_ratio > 0.0 && ((op_id % 10) as f64 / 10.0) < read_write_ratio {
                    if let Ok(mut tx) = db.begin() {
                        if let Ok(collection) = tx.collection("data") {
                            let _ = collection.find_all();
                        }
                        let _ = tx.rollback();
                    }
                    continue;
                }

                let write_start = Instant::now();

                let mut tx = match db.begin() {
                    Ok(tx) => tx,
                    Err(_) => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                let mut collection = match tx.collection("data") {
                    Ok(c) => c,
                    Err(_) => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                let doc = json!({
                    "_id": format!("w{}_op{}", worker_id, op_id),
                    "worker": worker_id,
                    "op": op_id,
                    "data": "X".repeat(100),
                });

                if collection.insert(doc).is_err() {
                    let _ = tx.rollback();
                    error_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                if tx.commit().is_err() {
                    error_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                let write_duration = write_start.elapsed();
                total_write_time.fetch_add(write_duration.as_micros() as u64, Ordering::Relaxed);
                success_count.fetch_add(1, Ordering::Relaxed);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();

    let success = success_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);
    let total_write_micros = total_write_time.load(Ordering::Relaxed);

    let _expected_writes = if scenario.read_write_ratio > 0.0 {
        ((scenario.num_workers * scenario.ops_per_worker) as f64 * (1.0 - scenario.read_write_ratio)) as u64
    } else {
        (scenario.num_workers * scenario.ops_per_worker) as u64
    };

    let avg_write_time = if success > 0 {
        Duration::from_micros(total_write_micros / success)
    } else {
        Duration::from_micros(0)
    };

    let throughput = success as f64 / elapsed.as_secs_f64();

    println!("Workers: {}, Total ops: {}", scenario.num_workers, scenario.num_workers * scenario.ops_per_worker);
    println!("Success: {}, Errors: {}", success, errors);
    println!("Total time: {:?}", elapsed);
    println!("Avg write time: {:?}", avg_write_time);
    println!("Throughput: {:.1} writes/sec", throughput);

    let mut tx = db.begin().unwrap();
    let collection = tx.collection("data").unwrap();
    let docs = collection.find_all().unwrap();
    tx.rollback().unwrap();

    if docs.len() as u64 != success {
        panic!("Data loss! Expected {} docs, got {}", success, docs.len());
    } else {
        println!("✓ No data loss: {} documents", docs.len());
    }

    println!("✓ Scenario {} completed successfully", scenario.name);
}

#[test]
fn test_write_heavy_workload() {
    println!("\n=== Write-Heavy Workload ===");
    println!("10 workers × 100 ops each = 1,000 total writes");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("write_heavy.db");
    let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

    let success = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = vec![];

    for worker_id in 0..10 {
        let db = Arc::clone(&db);
        let success = Arc::clone(&success);

        let handle = thread::spawn(move || {
            for op_id in 0..100 {
                let mut tx = db.begin().unwrap();
                let mut collection = tx.collection("data").unwrap();

                let doc = json!({
                    "_id": format!("w{}_op{}", worker_id, op_id),
                    "worker": worker_id,
                    "value": op_id,
                });

                if collection.insert(doc).is_ok() && tx.commit().is_ok() {
                    success.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let count = success.load(Ordering::Relaxed);

    println!("Completed: {} writes in {:?}", count, elapsed);
    println!("Throughput: {:.1} writes/sec", count as f64 / elapsed.as_secs_f64());
    println!("✓ Write-heavy workload completed");
}

#[test]
fn test_read_heavy_workload() {
    println!("\n=== Read-Heavy Workload ===");
    println!("Prepopulate 100 docs, then 10 workers × 100 reads (90% reads, 10% writes)");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("read_heavy.db");
    let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

    {
        let mut tx = db.begin().unwrap();
        let mut collection = tx.collection("data").unwrap();

        for i in 0..100 {
            collection.insert(json!({
                "_id": format!("doc_{}", i),
                "value": i,
            })).unwrap();
        }

        tx.commit().unwrap();
    }

    let reads = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = vec![];

    for worker_id in 0..10 {
        let db = Arc::clone(&db);
        let reads = Arc::clone(&reads);
        let writes = Arc::clone(&writes);

        let handle = thread::spawn(move || {
            for op_id in 0..100 {
                if op_id % 10 < 9 {
                    let mut tx = db.begin().unwrap();
                    let collection = tx.collection("data").unwrap();
                    if collection.find_all().is_ok() {
                        reads.fetch_add(1, Ordering::Relaxed);
                    }
                    let _ = tx.rollback();
                } else {
                    let mut tx = db.begin().unwrap();
                    let mut collection = tx.collection("data").unwrap();

                    let doc = json!({
                        "_id": format!("w{}_op{}", worker_id, op_id),
                        "worker": worker_id,
                    });

                    if collection.insert(doc).is_ok() && tx.commit().is_ok() {
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let read_count = reads.load(Ordering::Relaxed);
    let write_count = writes.load(Ordering::Relaxed);

    println!("Reads: {}, Writes: {}", read_count, write_count);
    println!("Total time: {:?}", elapsed);
    println!("Read throughput: {:.1} reads/sec", read_count as f64 / elapsed.as_secs_f64());
    println!("✓ Read-heavy workload completed");
}
