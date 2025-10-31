// Test to verify whether lock ordering deadlock can occur between
// drop_collection() and commit()
//
// Analysis:
// - drop_collection() acquires: version_chains.write() ONLY
// - commit() acquires: commit_mu FIRST, then version_chains.write() LATER
//
// For ABBA deadlock, we'd need:
// - Thread A: holds version_chains, waiting for commit_mu
// - Thread B: holds commit_mu, waiting for version_chains
//
// But drop_collection() releases version_chains before returning,
// and commit() is called separately AFTER drop_collection returns.
// So A never holds version_chains while trying to get commit_mu.

use std::sync::{Arc, Barrier, atomic::{AtomicBool, AtomicU32, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use std::fs;
use jasonisnthappy::core::database::Database;

/// Test with maximum interleaving to try to trigger deadlock
#[test]
fn test_interleaved_drop_and_commit() {
    let test_path = "/tmp/test_lock_ordering_deadlock.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();
    let db = Arc::new(db);

    // Track if we detected any deadlock
    let deadlock_detected = Arc::new(AtomicBool::new(false));
    let operations_completed = Arc::new(AtomicU32::new(0));

    let num_iterations = 500;
    let num_thread_pairs = 4;

    let mut handles = vec![];

    for pair_id in 0..num_thread_pairs {
        let db1 = db.clone();
        let db2 = db.clone();
        let deadlock_flag1 = deadlock_detected.clone();
        let deadlock_flag2 = deadlock_detected.clone();
        let ops_counter1 = operations_completed.clone();
        let ops_counter2 = operations_completed.clone();
        let barrier = Arc::new(Barrier::new(2));
        let b1 = barrier.clone();
        let b2 = barrier.clone();

        // Thread A: Creates collection, drops it, commits
        let h1 = thread::spawn(move || {
            for i in 0..num_iterations {
                b1.wait(); // Sync with Thread B

                let coll_name = format!("coll_{}_{}", pair_id, i);

                // Create collection
                let mut tx = db1.begin().unwrap();
                let _ = tx.create_collection(&coll_name);
                let _ = tx.commit();

                // Start timing for deadlock detection
                let start = Instant::now();

                // Drop collection (acquires version_chains)
                let mut tx = db1.begin().unwrap();
                let _ = tx.drop_collection(&coll_name);

                // Now commit (acquires commit_mu, then version_chains)
                match tx.commit() {
                    Ok(_) => {
                        ops_counter1.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        // Error is okay, but check for timeout
                        if start.elapsed() > Duration::from_secs(5) {
                            deadlock_flag1.store(true, Ordering::SeqCst);
                            eprintln!("Potential deadlock in thread A: {:?}", e);
                        }
                    }
                }
            }
        });

        // Thread B: Inserts documents and commits (competing for locks)
        let h2 = thread::spawn(move || {
            for i in 0..num_iterations {
                b2.wait(); // Sync with Thread A

                let coll_name = format!("data_{}_{}", pair_id, i);

                // Create collection if needed
                let mut tx = db2.begin().unwrap();
                let _ = tx.create_collection(&coll_name);
                let _ = tx.commit();

                // Start timing
                let start = Instant::now();

                // Insert doc and commit
                let mut tx = db2.begin().unwrap();
                if let Ok(mut c) = tx.collection(&coll_name) {
                    let _ = c.insert(serde_json::json!({"_id": format!("doc{}", i), "x": i}));
                }

                match tx.commit() {
                    Ok(_) => {
                        ops_counter2.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        if start.elapsed() > Duration::from_secs(5) {
                            deadlock_flag2.store(true, Ordering::SeqCst);
                            eprintln!("Potential deadlock in thread B: {:?}", e);
                        }
                    }
                }
            }
        });

        handles.push(h1);
        handles.push(h2);
    }

    // Wait with overall timeout
    let overall_timeout = Duration::from_secs(60);
    let start = Instant::now();

    loop {
        let all_finished = handles.iter().all(|h| h.is_finished());
        if all_finished {
            break;
        }
        if start.elapsed() > overall_timeout {
            panic!("DEADLOCK DETECTED: Threads did not complete within {} seconds. Operations completed: {}",
                   overall_timeout.as_secs(),
                   operations_completed.load(Ordering::SeqCst));
        }
        thread::sleep(Duration::from_millis(100));
    }

    // Join all threads
    for h in handles {
        h.join().expect("Thread panicked");
    }

    assert!(!deadlock_detected.load(Ordering::SeqCst), "Deadlock was detected during test");

    let ops = operations_completed.load(Ordering::SeqCst);
    println!("Test completed successfully: {} operations without deadlock", ops);

    // Cleanup
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

/// Test with garbage_collect running concurrently with commits
#[test]
fn test_garbage_collect_vs_commit() {
    let test_path = "/tmp/test_gc_vs_commit.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();
    let db = Arc::new(db);

    // Setup initial collection
    {
        let mut tx = db.begin().unwrap();
        let _ = tx.create_collection("test");
        let _ = tx.commit();
    }

    let deadlock_detected = Arc::new(AtomicBool::new(false));
    let ops_completed = Arc::new(AtomicU32::new(0));

    let db1 = db.clone();
    let db2 = db.clone();
    let flag1 = deadlock_detected.clone();
    let flag2 = deadlock_detected.clone();
    let ops1 = ops_completed.clone();
    let ops2 = ops_completed.clone();

    let barrier = Arc::new(Barrier::new(2));
    let b1 = barrier.clone();
    let b2 = barrier.clone();

    // Thread A: Continuous garbage collection
    let h1 = thread::spawn(move || {
        b1.wait();
        for _ in 0..100 {
            let start = Instant::now();
            match db1.garbage_collect() {
                Ok(_) => {
                    ops1.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    if start.elapsed() > Duration::from_secs(5) {
                        flag1.store(true, Ordering::SeqCst);
                    }
                }
            }
            thread::sleep(Duration::from_micros(100));
        }
    });

    // Thread B: Continuous commits with version chain updates
    let h2 = thread::spawn(move || {
        b2.wait();
        for i in 0..200 {
            let start = Instant::now();
            let mut tx = db2.begin().unwrap();
            if let Ok(mut c) = tx.collection("test") {
                // Update same doc to create version chain entries
                let _ = c.insert(serde_json::json!({"_id": format!("doc{}", i % 10), "val": i}));
            }
            match tx.commit() {
                Ok(_) => {
                    ops2.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    if start.elapsed() > Duration::from_secs(5) {
                        flag2.store(true, Ordering::SeqCst);
                    }
                }
            }
        }
    });

    // Wait with timeout
    let timeout = Duration::from_secs(30);
    let start = Instant::now();

    loop {
        if h1.is_finished() && h2.is_finished() {
            break;
        }
        if start.elapsed() > timeout {
            panic!("DEADLOCK: GC vs commit threads didn't complete");
        }
        thread::sleep(Duration::from_millis(100));
    }

    h1.join().unwrap();
    h2.join().unwrap();

    assert!(!deadlock_detected.load(Ordering::SeqCst));
    println!("GC vs commit test completed: {} operations", ops_completed.load(Ordering::SeqCst));

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}
