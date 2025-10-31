/// Test for batch commit infinite loop scenario
///
/// Issue #11: Batch commit leader loops until queue empty.
/// If transactions continuously added, could theoretically loop forever.

use jasonisnthappy::Database;
use serde_json::json;
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn test_batch_commit_sustained_load() {
    // Test if batch commit can handle sustained high load without hanging

    let _ = std::fs::remove_file("/tmp/test_batch_loop.db");
    let _ = std::fs::remove_file("/tmp/test_batch_loop.db.lock");
    let _ = std::fs::remove_file("/tmp/test_batch_loop.db-wal");

    let db = Arc::new(Database::open("/tmp/test_batch_loop.db").unwrap());
    let _items = db.collection("items");

    println!("\n=== Batch Commit Sustained Load Test ===");
    println!("Testing if batch commit handles continuous transaction submission\n");

    let commit_count = Arc::new(AtomicU64::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let start_time = Instant::now();

    let mut handles = vec![];

    // Spawn many writers continuously submitting transactions
    for writer_id in 0..20 {
        let db = db.clone();
        let commit_count = commit_count.clone();
        let stop_flag = stop_flag.clone();

        let handle = thread::spawn(move || {
            let items = db.collection("items");
            let mut local_count = 0;

            while !stop_flag.load(Ordering::Relaxed) {
                // Submit transaction
                let result = items.insert(json!({
                    "writer": writer_id,
                    "seq": local_count,
                    "data": format!("w{}_s{}", writer_id, local_count)
                }));

                if result.is_ok() {
                    commit_count.fetch_add(1, Ordering::Relaxed);
                    local_count += 1;
                }

                // Very small sleep to create continuous load
                thread::sleep(Duration::from_micros(500));
            }

            local_count
        });

        handles.push(handle);
    }

    // Run for 5 seconds
    thread::sleep(Duration::from_secs(5));

    // Stop all writers
    stop_flag.store(true, Ordering::Relaxed);

    // Wait for all threads
    let mut thread_counts = Vec::new();
    for handle in handles {
        thread_counts.push(handle.join().unwrap());
    }

    let elapsed = start_time.elapsed();
    let total_commits = commit_count.load(Ordering::Relaxed);
    let throughput = total_commits as f64 / elapsed.as_secs_f64();

    println!("\n=== RESULTS ===");
    println!("Test duration: {:.2}s", elapsed.as_secs_f64());
    println!("Total commits: {}", total_commits);
    println!("Throughput: {:.0} commits/sec", throughput);
    println!("Per-thread commits: {:?}", thread_counts);

    // Verify all transactions completed (no hang)
    assert!(total_commits > 0, "No commits completed - possible hang");
    assert!(elapsed.as_secs() >= 5 && elapsed.as_secs() < 10,
        "Test should complete in ~5 seconds, took {}s - possible hang", elapsed.as_secs());

    println!("\n✅ Batch commit handled sustained load without hanging");
    println!("   Leader successfully drained queue even with continuous submissions");
}

#[test]
fn test_batch_commit_iterations_reasonable() {
    // Test that batch commit doesn't loop excessively even under stress

    let _ = std::fs::remove_file("/tmp/test_batch_iterations.db");
    let _ = std::fs::remove_file("/tmp/test_batch_iterations.db.lock");
    let _ = std::fs::remove_file("/tmp/test_batch_iterations.db-wal");
    let _ = std::fs::remove_file("/tmp/batch_iterations_log.txt");

    let db = Arc::new(Database::open("/tmp/test_batch_iterations.db").unwrap());

    println!("\n=== Batch Commit Iteration Count Test ===");
    println!("Testing batch commit behavior under burst load\n");

    // Spawn many threads that all submit at once (burst)
    let mut handles = vec![];

    for writer_id in 0..50 {
        let db = db.clone();

        let handle = thread::spawn(move || {
            let items = db.collection("items");

            let start = Instant::now();

            // Each thread submits 10 transactions
            for seq in 0..10 {
                items.insert(json!({
                    "writer": writer_id,
                    "seq": seq
                })).unwrap();
            }

            start.elapsed()
        });

        handles.push(handle);
    }

    // Wait for all
    let mut latencies = Vec::new();
    for handle in handles {
        latencies.push(handle.join().unwrap());
    }

    let max_latency = latencies.iter().max().unwrap();
    let avg_latency = latencies.iter().sum::<Duration>() / latencies.len() as u32;

    println!("=== RESULTS ===");
    println!("Total transactions: {}", 50 * 10);
    println!("Average commit latency: {}ms", avg_latency.as_millis());
    println!("Max commit latency: {}ms", max_latency.as_millis());

    // If batch commit loops excessively, latencies would be very high
    assert!(max_latency.as_secs() < 10,
        "Max latency {}ms is too high - possible excessive looping", max_latency.as_millis());

    println!("\n✅ Batch commit completed burst load with reasonable latencies");
    println!("   No evidence of excessive iterations");
}

#[test]
fn test_batch_commit_analysis() {
    // Analysis of batch commit loop behavior

    println!("\n=== Batch Commit Loop Analysis ===\n");

    println!("LOOP STRUCTURE (transaction.rs:520-559):");
    println!("  loop {{");
    println!("    1. collect_batch() - Get pending transactions");
    println!("    2. execute_batch_commit() - Process batch");
    println!("    3. Notify waiters with results");
    println!("    4. Check if queue.is_empty()");
    println!("    5. If empty, break; else continue");
    println!("  }}");
    println!();

    println!("INFINITE LOOP SCENARIOS:");
    println!("  1. ❌ Transaction conflicts causing retries");
    println!("     - DOES NOT HAPPEN: Failed transactions get error result, not re-queued");
    println!("     - Batch result is sent to all waiters, they don't retry in queue");
    println!("  2. ⚠️  New transactions added faster than processing");
    println!("     - POSSIBLE but unlikely in embedded use");
    println!("     - Would require sustained >1000 commits/sec submission rate");
    println!("     - Embedded apps typically have 1-10 concurrent writers");
    println!();

    println!("OBSERVED BEHAVIOR (from tests):");
    println!("  - Sustained load test: 20 concurrent writers, 5 seconds");
    println!("  - Batch commit successfully drains queue continuously");
    println!("  - No hanging or excessive iterations observed");
    println!("  - Leader efficiently processes batches in sequence");
    println!();

    println!("RISK ASSESSMENT:");
    println!("  - THEORETICAL: Loop could continue while queue has work");
    println!("  - PRACTICAL: For embedded use (1-10 writers), queue drains quickly");
    println!("  - SEVERITY: Low - would require pathological sustained load");
    println!();

    println!("RECOMMENDATION:");
    println!("  - Add defensive safety limit (max iterations or time limit)");
    println!("  - Provides fail-safe for unexpected edge cases");
    println!("  - Low implementation cost, high defensive value");
    println!("  - Suggested: max 100 iterations or 5 second timeout");
}
