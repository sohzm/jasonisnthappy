//! High-concurrency stress tests to expose race conditions
//! These tests use much higher thread counts and operation volumes than normal tests

use jasonisnthappy::Database;
use serde_json::json;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

fn cleanup(name: &str) {
    let _ = std::fs::remove_file(format!("/tmp/{}.db", name));
    let _ = std::fs::remove_file(format!("/tmp/{}.db.lock", name));
    let _ = std::fs::remove_file(format!("/tmp/{}.db-wal", name));
}

/// Test with 50 concurrent writer threads
#[test]
fn test_50_concurrent_writers() {
    cleanup("stress_50_writers");
    let db = Arc::new(Database::open("/tmp/stress_50_writers.db").unwrap());

    let num_threads = 50;
    let ops_per_thread = 100;
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let inserted_ids: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let success = Arc::clone(&success_count);
            let errors = Arc::clone(&error_count);
            let ids = Arc::clone(&inserted_ids);

            thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let doc_id = format!("t{}_op{}", thread_id, i);
                    let coll_name = format!("coll_{}", thread_id % 5);

                    let mut tx = match db.begin() {
                        Ok(tx) => tx,
                        Err(e) => {
                            eprintln!("Thread {} begin failed: {:?}", thread_id, e);
                            errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    let mut coll = match tx.collection(&coll_name) {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("Thread {} collection failed: {:?}", thread_id, e);
                            errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    let doc = json!({
                        "_id": doc_id.clone(),
                        "thread": thread_id,
                        "op": i,
                        "data": format!("thread_{}_op_{}", thread_id, i)
                    });

                    match coll.insert(doc) {
                        Ok(_) => {
                            match tx.commit() {
                                Ok(_) => {
                                    success.fetch_add(1, Ordering::Relaxed);
                                    ids.lock().unwrap().insert(doc_id);
                                }
                                Err(e) => {
                                    eprintln!("Thread {} commit failed: {:?}", thread_id, e);
                                    errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Thread {} insert failed: {:?}", thread_id, e);
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let total_success = success_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);
    let expected = inserted_ids.lock().unwrap().len();

    println!("Results: {} success, {} errors, tracked {}", total_success, total_errors, expected);

    // Verify data integrity
    let mut total_found = 0usize;
    for coll_id in 0..5 {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection(&format!("coll_{}", coll_id)).unwrap();
        let docs = coll.find_all().unwrap();
        total_found += docs.len();
    }

    println!("Total documents found: {}, expected: {}", total_found, expected);

    assert_eq!(total_errors, 0, "Some operations failed");
    assert_eq!(total_found, expected, "Data loss detected! Found {} but inserted {}", total_found, expected);
}

/// Test with 100 concurrent threads doing mixed operations
/// This test uses retry logic to handle TxConflict (expected MVCC behavior)
#[test]
fn test_100_threads_mixed_operations() {
    cleanup("stress_100_mixed");
    let db = Arc::new(Database::open("/tmp/stress_100_mixed.db").unwrap());

    let num_threads = 100;
    let ops_per_thread = 50;
    let success_count = Arc::new(AtomicU64::new(0));
    let conflict_count = Arc::new(AtomicU64::new(0));
    let other_error_count = Arc::new(AtomicU64::new(0));

    // Pre-populate with some documents
    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("shared").unwrap();
        for i in 0..100 {
            coll.insert(json!({"_id": format!("seed_{}", i), "id": i, "value": 0})).unwrap();
        }
        tx.commit().unwrap();
    }

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let success = Arc::clone(&success_count);
            let conflicts = Arc::clone(&conflict_count);
            let other_errors = Arc::clone(&other_error_count);

            thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let op_type = (thread_id + i) % 4;

                    // Retry up to 3 times on TxConflict
                    let mut attempts = 0;
                    let max_attempts = 3;

                    loop {
                        attempts += 1;
                        let result: Result<(), String> = match op_type {
                            0 => {
                                // Insert
                                let mut tx = db.begin().map_err(|e| format!("{:?}", e))?;
                                let mut coll = tx.collection("shared").map_err(|e| format!("{:?}", e))?;
                                coll.insert(json!({
                                    "_id": format!("t{}_i{}_a{}", thread_id, i, attempts),
                                    "thread": thread_id,
                                    "op": i,
                                    "type": "insert"
                                })).map_err(|e| format!("{:?}", e))?;
                                tx.commit().map_err(|e| format!("{:?}", e))
                            }
                            1 => {
                                // Read
                                let mut tx = db.begin().map_err(|e| format!("{:?}", e))?;
                                let coll = tx.collection("shared").map_err(|e| format!("{:?}", e))?;
                                let _ = coll.find_all().map_err(|e| format!("{:?}", e))?;
                                Ok(())
                            }
                            2 => {
                                // Update
                                let mut tx = db.begin().map_err(|e| format!("{:?}", e))?;
                                let mut coll = tx.collection("shared").map_err(|e| format!("{:?}", e))?;
                                let _ = coll.update_by_id(
                                    &format!("seed_{}", i % 100),
                                    json!({"updated_by": thread_id})
                                );
                                tx.commit().map_err(|e| format!("{:?}", e))
                            }
                            _ => {
                                // Insert to different collection
                                let mut tx = db.begin().map_err(|e| format!("{:?}", e))?;
                                let mut coll = tx.collection(&format!("thread_{}", thread_id % 10)).map_err(|e| format!("{:?}", e))?;
                                coll.insert(json!({"op": i})).map_err(|e| format!("{:?}", e))?;
                                tx.commit().map_err(|e| format!("{:?}", e))
                            }
                        };

                        match result {
                            Ok(_) => {
                                success.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Err(e) if e.contains("TxConflict") || e.contains("Conflict") => {
                                conflicts.fetch_add(1, Ordering::Relaxed);
                                if attempts >= max_attempts {
                                    break; // Give up after max retries
                                }
                                std::thread::sleep(std::time::Duration::from_micros(100 * attempts as u64));
                            }
                            Err(e) => {
                                eprintln!("Thread {} op {} non-conflict error: {}", thread_id, i, e);
                                other_errors.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                }
                Ok::<(), String>(())
            })
        })
        .collect();

    for handle in handles {
        let _ = handle.join().expect("Thread panicked");
    }

    let total_success = success_count.load(Ordering::Relaxed);
    let total_conflicts = conflict_count.load(Ordering::Relaxed);
    let total_other_errors = other_error_count.load(Ordering::Relaxed);

    println!("Results: {} success, {} conflicts (retried), {} other errors",
             total_success, total_conflicts, total_other_errors);

    // Conflicts are expected under high contention - that's MVCC working correctly
    // We just want to ensure no data corruption or unexpected errors
    assert_eq!(total_other_errors, 0, "Non-conflict errors occurred");
    assert!(total_success > 0, "No successful operations");
}

/// Stress test: rapid concurrent commits (30 threads, 50 commits each)
#[test]
fn test_rapid_concurrent_commits() {
    cleanup("stress_rapid_commits");
    let db = Arc::new(Database::open("/tmp/stress_rapid_commits.db").unwrap());

    let num_threads = 30;
    let commits_per_thread = 50;
    let panic_count = Arc::new(AtomicU64::new(0));
    let success_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let panics = Arc::clone(&panic_count);
            let successes = Arc::clone(&success_count);

            thread::spawn(move || {
                for commit_num in 0..commits_per_thread {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let mut tx = db.begin().unwrap();
                        let mut coll = tx.collection(&format!("tx_coll_{}", thread_id % 10)).unwrap();

                        // Insert a few documents per transaction
                        for doc_num in 0..5 {
                            coll.insert(json!({
                                "_id": format!("t{}_c{}_d{}", thread_id, commit_num, doc_num),
                                "thread": thread_id,
                                "commit": commit_num,
                                "doc": doc_num
                            })).unwrap();
                        }

                        tx.commit().unwrap();
                    }));

                    if result.is_err() {
                        panics.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Thread {} commit {} panicked!", thread_id, commit_num);
                    } else {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let total_panics = panic_count.load(Ordering::Relaxed);
    let total_successes = success_count.load(Ordering::Relaxed);
    println!("Commits: {} successful, {} panicked", total_successes, total_panics);

    // Verify data
    let mut total_docs = 0usize;
    for coll_id in 0..10 {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection(&format!("tx_coll_{}", coll_id)).unwrap();
        total_docs += coll.find_all().unwrap().len();
    }

    let expected = (total_successes as usize) * 5;
    println!("Total docs: {}, expected: {}", total_docs, expected);

    assert_eq!(total_panics, 0, "Some transactions panicked");
    assert_eq!(total_docs, expected, "Data loss: expected {} docs, found {}", expected, total_docs);
}

/// Test concurrent operations on the SAME documents (high contention)
#[test]
fn test_high_contention_same_documents() {
    cleanup("stress_contention");
    let db = Arc::new(Database::open("/tmp/stress_contention.db").unwrap());

    // Create 10 documents that all threads will fight over
    let mut doc_ids = Vec::new();
    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("contended").unwrap();
        for i in 0..10 {
            let id = format!("contended_{}", i);
            coll.insert(json!({"_id": id.clone(), "counter": 0, "id": i})).unwrap();
            doc_ids.push(id);
        }
        tx.commit().unwrap();
    }

    let doc_ids = Arc::new(doc_ids);
    let num_threads = 50;
    let ops_per_thread = 100;
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let doc_ids = Arc::clone(&doc_ids);
            let successes = Arc::clone(&success_count);
            let errors = Arc::clone(&error_count);

            thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let doc_idx = (thread_id + i) % 10;
                    let doc_id = &doc_ids[doc_idx];

                    let mut tx = match db.begin() {
                        Ok(tx) => tx,
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    let mut coll = match tx.collection("contended") {
                        Ok(c) => c,
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    // Try to update the document
                    match coll.update_by_id(doc_id, json!({"last_updater": thread_id, "update_count": i})) {
                        Ok(_) => {
                            match tx.commit() {
                                Ok(_) => { successes.fetch_add(1, Ordering::Relaxed); }
                                Err(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                            }
                        }
                        Err(_) => {
                            // Update failures are expected under contention
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let total_successes = success_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);

    println!("High contention results: {} successes, {} errors (errors expected under contention)",
             total_successes, total_errors);

    // All documents should still exist and be readable
    let mut tx = db.begin().unwrap();
    let coll = tx.collection("contended").unwrap();
    let final_count = coll.find_all().unwrap().len();
    assert_eq!(final_count, 10, "Documents were lost! Expected 10, found {}", final_count);
}

/// Extreme test: 200 threads, short bursts
#[test]
fn test_extreme_200_threads() {
    cleanup("stress_extreme_200");
    let db = Arc::new(Database::open("/tmp/stress_extreme_200.db").unwrap());

    let num_threads = 200;
    let ops_per_thread = 20;
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let inserted_ids: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let success = Arc::clone(&success_count);
            let errors = Arc::clone(&error_count);
            let ids = Arc::clone(&inserted_ids);

            thread::spawn(move || {
                // Each thread uses its own collection to reduce contention
                let coll_name = format!("extreme_{}", thread_id % 20);

                for i in 0..ops_per_thread {
                    let doc_id = format!("t{}_i{}", thread_id, i);

                    let mut tx = match db.begin() {
                        Ok(tx) => tx,
                        Err(e) => {
                            eprintln!("Thread {} begin error: {:?}", thread_id, e);
                            errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    let mut coll = match tx.collection(&coll_name) {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("Thread {} collection error: {:?}", thread_id, e);
                            errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    match coll.insert(json!({"_id": doc_id.clone(), "t": thread_id, "i": i})) {
                        Ok(_) => {
                            match tx.commit() {
                                Ok(_) => {
                                    success.fetch_add(1, Ordering::Relaxed);
                                    ids.lock().unwrap().insert(doc_id);
                                }
                                Err(e) => {
                                    eprintln!("Thread {} commit error: {:?}", thread_id, e);
                                    errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Thread {} insert error: {:?}", thread_id, e);
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let total_success = success_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);
    let expected = inserted_ids.lock().unwrap().len();

    println!("Extreme test: {} success, {} errors, tracked {}", total_success, total_errors, expected);

    // Verify
    let mut total_found = 0usize;
    for coll_id in 0..20 {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection(&format!("extreme_{}", coll_id)).unwrap();
        total_found += coll.find_all().unwrap().len();
    }

    assert_eq!(total_errors, 0, "Operations failed");
    assert_eq!(total_found, expected, "Data loss: found {} but inserted {}", total_found, expected);
}

/// Test: 20 Writers and 30 readers competing
/// Writers use retry logic for TxConflict (expected MVCC behavior)
#[test]
fn test_concurrent_readers_writers_stress() {
    cleanup("stress_rw");
    let db = Arc::new(Database::open("/tmp/stress_rw.db").unwrap());

    // Pre-populate
    {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("rw_stress").unwrap();
        for i in 0..1000 {
            coll.insert(json!({"_id": format!("seed_{}", i), "id": i, "data": format!("initial_{}", i)})).unwrap();
        }
        tx.commit().unwrap();
    }

    let num_writers = 20;
    let num_readers = 30;
    let ops_per_thread = 100;

    let write_success = Arc::new(AtomicU64::new(0));
    let read_success = Arc::new(AtomicU64::new(0));
    let write_conflicts = Arc::new(AtomicU64::new(0));
    let write_other_errors = Arc::new(AtomicU64::new(0));
    let read_errors = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    // Spawn writers with retry logic
    for writer_id in 0..num_writers {
        let db = Arc::clone(&db);
        let success = Arc::clone(&write_success);
        let conflicts = Arc::clone(&write_conflicts);
        let other_errors = Arc::clone(&write_other_errors);

        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let mut attempts = 0;
                let max_attempts = 5;

                loop {
                    attempts += 1;
                    let result: Result<(), String> = if i % 2 == 0 {
                        // Insert
                        let mut tx = db.begin().map_err(|e| format!("{:?}", e))?;
                        let mut coll = tx.collection("rw_stress").map_err(|e| format!("{:?}", e))?;
                        coll.insert(json!({"_id": format!("w{}_i{}_a{}", writer_id, i, attempts), "writer": writer_id, "op": i}))
                            .map_err(|e| format!("{:?}", e))?;
                        tx.commit().map_err(|e| format!("{:?}", e))
                    } else {
                        // Update
                        let mut tx = db.begin().map_err(|e| format!("{:?}", e))?;
                        let mut coll = tx.collection("rw_stress").map_err(|e| format!("{:?}", e))?;
                        let _ = coll.update_by_id(
                            &format!("seed_{}", (writer_id * ops_per_thread + i) % 1000),
                            json!({"modified_by": writer_id})
                        );
                        tx.commit().map_err(|e| format!("{:?}", e))
                    };

                    match result {
                        Ok(_) => {
                            success.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(e) if e.contains("TxConflict") || e.contains("Conflict") => {
                            conflicts.fetch_add(1, Ordering::Relaxed);
                            if attempts >= max_attempts {
                                break; // Give up after max retries
                            }
                            std::thread::sleep(std::time::Duration::from_micros(50 * attempts as u64));
                        }
                        Err(e) => {
                            eprintln!("Writer {} non-conflict error: {}", writer_id, e);
                            other_errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }
            Ok::<(), String>(())
        }));
    }

    // Spawn readers
    for reader_id in 0..num_readers {
        let db = Arc::clone(&db);
        let success = Arc::clone(&read_success);
        let errors = Arc::clone(&read_errors);

        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let result: Result<usize, String> = {
                    let mut tx = db.begin().map_err(|e| format!("{:?}", e))?;
                    let coll = tx.collection("rw_stress").map_err(|e| format!("{:?}", e))?;

                    if i % 3 == 0 {
                        // Full scan
                        let docs = coll.find_all().map_err(|e| format!("{:?}", e))?;
                        Ok(docs.len())
                    } else {
                        // Single doc read
                        let _ = coll.find_by_id(&format!("seed_{}", (reader_id * ops_per_thread + i) % 1000))
                            .map_err(|e| format!("{:?}", e))?;
                        Ok(1)
                    }
                };

                match result {
                    Ok(_) => { success.fetch_add(1, Ordering::Relaxed); }
                    Err(e) => {
                        eprintln!("Reader {} error: {}", reader_id, e);
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Ok::<(), String>(())
        }));
    }

    for handle in handles {
        let _ = handle.join().expect("Thread panicked");
    }

    let w_success = write_success.load(Ordering::Relaxed);
    let r_success = read_success.load(Ordering::Relaxed);
    let w_conflicts = write_conflicts.load(Ordering::Relaxed);
    let w_other_errors = write_other_errors.load(Ordering::Relaxed);
    let r_errors = read_errors.load(Ordering::Relaxed);

    println!("Writers: {} success, {} conflicts (retried), {} other errors", w_success, w_conflicts, w_other_errors);
    println!("Readers: {} success, {} errors", r_success, r_errors);

    // Conflicts are expected - that's MVCC working correctly
    assert_eq!(w_other_errors, 0, "Non-conflict write errors occurred");
    assert_eq!(r_errors, 0, "Read errors occurred");
    assert!(w_success > 0, "No successful writes");
}
