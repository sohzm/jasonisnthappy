/// Test to demonstrate and measure checkpoint blocking writes
///
/// Issue #9: Checkpoint acquires WAL lock which blocks write_frame() calls
/// Even though checkpoint runs in background thread, concurrent writes must wait

use jasonisnthappy::Database;
use serde_json::json;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, Instant};
use std::thread;

#[test]
fn test_checkpoint_blocks_writes() {
    // Clean up all database files
    let _ = std::fs::remove_file("/tmp/checkpoint_blocking_test.db");
    let _ = std::fs::remove_file("/tmp/checkpoint_blocking_test.db.lock");
    let _ = std::fs::remove_file("/tmp/checkpoint_blocking_test.db-wal");
    let _ = std::fs::remove_file("/tmp/checkpoint_blocking_output.log");

    let db = Arc::new(Database::open("/tmp/checkpoint_blocking_test.db").unwrap());
    let items = db.collection("items");

    // Set aggressive auto-checkpoint threshold to trigger frequently
    db.set_auto_checkpoint_threshold(100); // Checkpoint after 100 WAL frames

    // Write initial documents to build up WAL
    for i in 0..150 {
        items.insert(json!({
            "id": i,
            "data": "x".repeat(100)
        })).unwrap();
    }

    println!("Initial documents written, WAL should trigger checkpoint soon");

    // Give checkpoint time to start in background
    thread::sleep(Duration::from_millis(100));

    // Now measure write latencies while checkpoint may be running
    let max_latency = Arc::new(AtomicU64::new(0));
    let total_writes = Arc::new(AtomicU64::new(0));
    let blocked_writes = Arc::new(AtomicU64::new(0)); // Writes > 5ms

    let mut handles = vec![];

    // Spawn 4 writer threads to create contention
    for thread_id in 0..4 {
        let db = db.clone();
        let _max_latency = max_latency.clone();
        let total_writes = total_writes.clone();
        let blocked_writes = blocked_writes.clone();

        let handle = thread::spawn(move || {
            let items = db.collection("items");
            let mut local_max = 0u64;

            for i in 0..100 {
                let start = Instant::now();

                items.insert(json!({
                    "thread": thread_id,
                    "seq": i,
                    "data": "y".repeat(100)
                })).unwrap();

                let latency = start.elapsed();
                let latency_ms = latency.as_millis() as u64;

                if latency_ms > local_max {
                    local_max = latency_ms;
                }

                if latency_ms > 5 {
                    blocked_writes.fetch_add(1, Ordering::Relaxed);

                    // Log significant blocking
                    use std::io::Write;
                    let _ = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("/tmp/checkpoint_blocking_output.log")
                        .and_then(|mut f| {
                            writeln!(f, "Thread {} write {} BLOCKED for {}ms",
                                thread_id, i, latency_ms)
                        });
                }

                total_writes.fetch_add(1, Ordering::Relaxed);
            }

            local_max
        });

        handles.push(handle);
    }

    // Wait for all threads and collect max latencies
    let mut thread_max_latencies = vec![];
    for handle in handles {
        thread_max_latencies.push(handle.join().unwrap());
    }

    // Compute global max
    let global_max = thread_max_latencies.iter().max().copied().unwrap_or(0);
    max_latency.store(global_max, Ordering::Relaxed);

    let total = total_writes.load(Ordering::Relaxed);
    let blocked = blocked_writes.load(Ordering::Relaxed);
    let max_ms = max_latency.load(Ordering::Relaxed);

    // Write summary
    use std::io::Write;
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/checkpoint_blocking_output.log")
        .and_then(|mut f| {
            writeln!(f, "\n=== SUMMARY ===")?;
            writeln!(f, "Total writes: {}", total)?;
            writeln!(f, "Blocked writes (>5ms): {} ({:.1}%)", blocked,
                (blocked as f64 / total as f64) * 100.0)?;
            writeln!(f, "Max latency: {}ms", max_ms)?;
            writeln!(f, "\nThread max latencies: {:?}", thread_max_latencies)?;
            Ok(())
        });

    println!("\n=== CHECKPOINT BLOCKING TEST RESULTS ===");
    println!("Total writes: {}", total);
    println!("Blocked writes (>5ms): {} ({:.1}%)", blocked,
        (blocked as f64 / total as f64) * 100.0);
    println!("Max write latency: {}ms", max_ms);
    println!("Thread max latencies: {:?}", thread_max_latencies);
    println!("\nDetailed log: /tmp/checkpoint_blocking_output.log");

    // Assert: Demonstrate the issue exists
    // If checkpoint is blocking writes, we should see some writes > 10ms
    if max_ms > 10 {
        println!("\n✅ ISSUE REPRODUCED: Checkpoint blocked writes (max {}ms)", max_ms);
    } else {
        println!("\n⚠️  Issue not clearly demonstrated in this run (max {}ms)", max_ms);
        println!("   May need more aggressive checkpoint triggering or larger WAL");
    }
}

#[test]
fn test_checkpoint_lock_contention_direct() {
    // More direct test: Manually trigger checkpoint and measure concurrent write latency

    // Clean up all database files
    let _ = std::fs::remove_file("/tmp/checkpoint_direct_test.db");
    let _ = std::fs::remove_file("/tmp/checkpoint_direct_test.db.lock");
    let _ = std::fs::remove_file("/tmp/checkpoint_direct_test.db-wal");
    let _ = std::fs::remove_file("/tmp/checkpoint_direct_output.log");

    let db = Arc::new(Database::open("/tmp/checkpoint_direct_test.db").unwrap());
    let data = db.collection("data");

    // Disable auto-checkpoint
    db.set_auto_checkpoint_threshold(0);

    // Write documents to build WAL (typical embedded workload)
    for i in 0..50 {
        data.insert(json!({
            "id": i,
            "payload": "x".repeat(100)
        })).unwrap();
    }

    println!("Wrote 50 documents, WAL should have ~50 frames");

    // Measure baseline write latency (no checkpoint)
    let baseline_start = Instant::now();
    for i in 0..10 {
        data.insert(json!({"baseline": i})).unwrap();
    }
    let baseline_latency = baseline_start.elapsed().as_millis() / 10;

    println!("Baseline write latency: ~{}ms per insert", baseline_latency);

    // Now trigger checkpoint manually and measure concurrent write latency
    let db2 = db.clone();

    // Start checkpoint in separate thread
    let checkpoint_handle = thread::spawn(move || {
        let start = Instant::now();
        println!("Starting manual checkpoint...");
        db2.checkpoint().unwrap();
        let duration = start.elapsed();
        println!("Checkpoint completed in {}ms", duration.as_millis());
        duration
    });

    // Give checkpoint a moment to start and acquire lock
    thread::sleep(Duration::from_millis(10));

    // Now try to write while checkpoint is running
    let write_latencies = Arc::new(std::sync::Mutex::new(Vec::new()));
    let write_latencies_clone = write_latencies.clone();

    let write_handle = thread::spawn(move || {
        let data = db.collection("data");
        for i in 0..20 {
            let start = Instant::now();
            data.insert(json!({"concurrent": i})).unwrap();
            let latency_ms = start.elapsed().as_millis() as u64;

            write_latencies_clone.lock().unwrap().push(latency_ms);

            if latency_ms > 5 {
                use std::io::Write;
                let _ = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("/tmp/checkpoint_direct_output.log")
                    .and_then(|mut f| {
                        writeln!(f, "Write {} during checkpoint: {}ms", i, latency_ms)
                    });
            }
        }
    });

    let checkpoint_duration = checkpoint_handle.join().unwrap();
    write_handle.join().unwrap();

    let latencies = write_latencies.lock().unwrap();
    let max_concurrent_latency = latencies.iter().max().copied().unwrap_or(0);
    let avg_concurrent_latency = latencies.iter().sum::<u64>() / latencies.len() as u64;

    use std::io::Write;
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/checkpoint_direct_output.log")
        .and_then(|mut f| {
            writeln!(f, "\n=== DIRECT CHECKPOINT TEST SUMMARY ===")?;
            writeln!(f, "Checkpoint duration: {}ms", checkpoint_duration.as_millis())?;
            writeln!(f, "Baseline write latency: ~{}ms", baseline_latency)?;
            writeln!(f, "Concurrent writes - Max: {}ms, Avg: {}ms",
                max_concurrent_latency, avg_concurrent_latency)?;
            writeln!(f, "All latencies: {:?}", latencies.clone())?;
            Ok(())
        });

    println!("\n=== DIRECT CHECKPOINT TEST RESULTS ===");
    println!("Checkpoint duration: {}ms", checkpoint_duration.as_millis());
    println!("Baseline write latency: ~{}ms", baseline_latency);
    println!("Concurrent write latency - Max: {}ms, Avg: {}ms",
        max_concurrent_latency, avg_concurrent_latency);
    println!("Detailed log: /tmp/checkpoint_direct_output.log");

    if max_concurrent_latency as u128 > baseline_latency * 2 {
        println!("\n✅ ISSUE REPRODUCED: Writes blocked during checkpoint");
        println!("   Max concurrent latency ({}ms) >> baseline ({}ms)",
            max_concurrent_latency, baseline_latency);
    } else {
        println!("\n⚠️  Blocking not clearly demonstrated (may be too fast)");
    }
}
