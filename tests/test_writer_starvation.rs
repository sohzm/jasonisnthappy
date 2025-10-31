/// Test for writer starvation under heavy read load
///
/// Issue #10: Rust's std::sync::RwLock has no fairness guarantees.
/// Under heavy read load, writers can be starved waiting for read locks to release.

use jasonisnthappy::Database;
use serde_json::json;
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn test_writers_starved_by_heavy_reads() {
    // Clean up
    let _ = std::fs::remove_file("/tmp/test_writer_starvation.db");
    let _ = std::fs::remove_file("/tmp/test_writer_starvation.db.lock");
    let _ = std::fs::remove_file("/tmp/test_writer_starvation.db-wal");
    let _ = std::fs::remove_file("/tmp/writer_starvation_output.log");

    let db = Arc::new(Database::open("/tmp/test_writer_starvation.db").unwrap());
    let items = db.collection("items");

    // Pre-populate with data for readers to access
    for i in 0..100 {
        items.insert(json!({
            "id": i,
            "data": format!("item_{}", i)
        })).unwrap();
    }

    println!("\n=== Writer Starvation Test ===");
    println!("Testing if heavy read load starves writers\n");

    // Shared state
    let read_count = Arc::new(AtomicU64::new(0));
    let write_count = Arc::new(AtomicU64::new(0));
    let writer_wait_time = Arc::new(AtomicU64::new(0));
    let max_writer_wait = Arc::new(AtomicU64::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));

    let mut handles = vec![];

    // Spawn MANY readers (simulate heavy read load)
    // 50 reader threads continuously reading
    for reader_id in 0..50 {
        let db = db.clone();
        let read_count = read_count.clone();
        let stop_flag = stop_flag.clone();

        let handle = thread::spawn(move || {
            let items = db.collection("items");
            while !stop_flag.load(Ordering::Relaxed) {
                // Continuous reads
                let doc_id = format!("id is {}", reader_id % 100);
                let _ = items.find_one(&doc_id);
                read_count.fetch_add(1, Ordering::Relaxed);

                // Small sleep to prevent CPU spinning
                thread::sleep(Duration::from_micros(100));
            }
        });

        handles.push(handle);
    }

    // Give readers time to build up read load
    thread::sleep(Duration::from_millis(50));

    // Spawn a few writers trying to write during heavy read load
    for writer_id in 0..5 {
        let db = db.clone();
        let write_count = write_count.clone();
        let writer_wait_time = writer_wait_time.clone();
        let max_writer_wait = max_writer_wait.clone();

        let handle = thread::spawn(move || {
            let items = db.collection("items");

            for i in 0..20 {
                let start = Instant::now();

                // Try to write - this should acquire write locks on version_chains, etc.
                items.insert(json!({
                    "writer": writer_id,
                    "seq": i,
                    "data": format!("writer_{}_{}", writer_id, i)
                })).unwrap();

                let elapsed = start.elapsed();
                let elapsed_ms = elapsed.as_millis() as u64;

                write_count.fetch_add(1, Ordering::Relaxed);
                writer_wait_time.fetch_add(elapsed_ms, Ordering::Relaxed);

                // Track max wait time
                let mut current_max = max_writer_wait.load(Ordering::Relaxed);
                while elapsed_ms > current_max {
                    match max_writer_wait.compare_exchange(
                        current_max,
                        elapsed_ms,
                        Ordering::Relaxed,
                        Ordering::Relaxed
                    ) {
                        Ok(_) => break,
                        Err(actual) => current_max = actual,
                    }
                }

                if elapsed_ms > 100 {
                    use std::io::Write;
                    let _ = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("/tmp/writer_starvation_output.log")
                        .and_then(|mut f| {
                            writeln!(f, "Writer {} insert {} took {}ms (potential starvation)",
                                writer_id, i, elapsed_ms)?;
                            Ok(())
                        });
                }

                // Small sleep between writes
                thread::sleep(Duration::from_millis(5));
            }
        });

        handles.push(handle);
    }

    // Let the test run for a bit
    thread::sleep(Duration::from_secs(3));

    // Stop readers
    stop_flag.store(true, Ordering::Relaxed);

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Collect results
    let total_reads = read_count.load(Ordering::Relaxed);
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_wait = writer_wait_time.load(Ordering::Relaxed);
    let max_wait = max_writer_wait.load(Ordering::Relaxed);
    let avg_wait = if total_writes > 0 { total_wait / total_writes } else { 0 };

    // Write summary
    use std::io::Write;
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/writer_starvation_output.log")
        .and_then(|mut f| {
            writeln!(f, "\n=== SUMMARY ===")?;
            writeln!(f, "Total reads: {}", total_reads)?;
            writeln!(f, "Total writes: {}", total_writes)?;
            writeln!(f, "Average writer latency: {}ms", avg_wait)?;
            writeln!(f, "Max writer latency: {}ms", max_wait)?;
            writeln!(f, "\nRead:Write ratio: {}:1", total_reads / total_writes.max(1))?;
            Ok(())
        });

    println!("\n=== RESULTS ===");
    println!("Total reads: {}", total_reads);
    println!("Total writes: {}", total_writes);
    println!("Average writer latency: {}ms", avg_wait);
    println!("Max writer latency: {}ms", max_wait);
    println!("Read:Write ratio: {}:1", total_reads / total_writes.max(1));
    println!("\nDetailed log: /tmp/writer_starvation_output.log");

    // Analysis
    if max_wait > 500 {
        println!("\n⚠️  WRITER STARVATION DETECTED!");
        println!("   Max writer latency {}ms indicates writers are being starved", max_wait);
        println!("   by heavy read load. Consider using parking_lot::RwLock for fairness.");
    } else if max_wait > 100 {
        println!("\n⚠️  MODERATE writer delays detected (max {}ms)", max_wait);
        println!("   Not critical for embedded use, but monitor in production");
    } else {
        println!("\n✅ No significant writer starvation detected");
        println!("   Max writer latency {}ms is acceptable", max_wait);
    }

    // Assert for test passing
    assert!(total_writes == 100, "Expected 100 writes (5 writers * 20 each), got {}", total_writes);
}

#[test]
fn test_writer_starvation_baseline() {
    // Baseline test: measure write latency WITHOUT heavy read load

    let _ = std::fs::remove_file("/tmp/test_writer_baseline.db");
    let _ = std::fs::remove_file("/tmp/test_writer_baseline.db.lock");
    let _ = std::fs::remove_file("/tmp/test_writer_baseline.db-wal");

    let db = Arc::new(Database::open("/tmp/test_writer_baseline.db").unwrap());
    let items = db.collection("items");

    // Pre-populate
    for i in 0..100 {
        items.insert(json!({"id": i})).unwrap();
    }

    println!("\n=== Baseline Writer Latency (No Read Load) ===");

    let mut latencies = Vec::new();

    for i in 0..100 {
        let start = Instant::now();
        items.insert(json!({
            "seq": i,
            "data": format!("item_{}", i)
        })).unwrap();
        let elapsed = start.elapsed().as_millis() as u64;
        latencies.push(elapsed);
    }

    let avg = latencies.iter().sum::<u64>() / latencies.len() as u64;
    let max = *latencies.iter().max().unwrap();
    let p95 = {
        let mut sorted = latencies.clone();
        sorted.sort();
        sorted[(sorted.len() * 95) / 100]
    };

    println!("Average write latency: {}ms", avg);
    println!("Max write latency: {}ms", max);
    println!("P95 write latency: {}ms", p95);

    println!("\nThis baseline will be compared with heavy read load test.");
}
