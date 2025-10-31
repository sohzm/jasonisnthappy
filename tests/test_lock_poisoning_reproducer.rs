// Test to reproduce lock poisoning issue (Issue #6)
// This test demonstrates that if a thread panics while holding a lock,
// the lock becomes poisoned and all subsequent operations fail.

use jasonisnthappy::core::database::Database;
use std::sync::Arc;
use std::thread;
use std::panic;
use std::fs;

#[test]
fn test_lock_poisoning_cascading_failure() {
    let test_path = "/tmp/test_lock_poison.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Arc::new(Database::open(test_path).unwrap());

    // Create a collection
    {
        let mut tx = db.begin().unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().unwrap();
    }

    // Thread 1: Start a transaction and panic while holding locks
    let db1 = Arc::clone(&db);
    let handle = thread::spawn(move || {
        // Start a transaction - this acquires locks
        let mut tx = db1.begin().unwrap();
        let mut coll = tx.collection("users").unwrap();

        // Insert a document
        coll.insert(serde_json::json!({"id": 1, "name": "Alice"})).unwrap();

        // Simulate a panic (e.g., from assertion failure, OOM, bug, etc.)
        panic!("Simulating a panic while holding locks!");

        // This commit would never happen
        // tx.commit().unwrap();
    });

    // Wait for thread to panic
    let _ = handle.join();

    println!("Thread 1 panicked, now trying to use database...");

    // Thread 2: Try to use the database after lock poisoning
    // According to the issue, this should fail with "lock poisoned" panic
    let db2 = Arc::clone(&db);
    let handle2 = thread::spawn(move || {
        // Try to start a new transaction - this should encounter poisoned locks
        match panic::catch_unwind(panic::AssertUnwindSafe(|| {
            db2.begin()
        })) {
            Ok(result) => {
                println!("Transaction succeeded: {:?}", result.is_ok());
                result
            },
            Err(e) => {
                println!("Transaction panicked!");
                if let Some(s) = e.downcast_ref::<&str>() {
                    println!("Panic message: {}", s);
                    assert!(s.contains("lock") || s.contains("poison"),
                           "Expected lock poisoning panic, got: {}", s);
                } else if let Some(s) = e.downcast_ref::<String>() {
                    println!("Panic message: {}", s);
                    assert!(s.contains("lock") || s.contains("poison"),
                           "Expected lock poisoning panic, got: {}", s);
                }
                panic!("Lock poisoning confirmed");
            }
        }
    });

    let result = handle2.join();
    match result {
        Ok(_) => {
            println!("✓ No lock poisoning detected - locks were properly released");
            println!("  This suggests the issue may not exist or locks are not held across panic boundaries");
        },
        Err(_) => {
            println!("✗ Lock poisoning confirmed - database is unusable after panic");
            println!("  This is the bug described in Issue #6");
        }
    }

    // Cleanup
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

#[test]
fn test_lock_poisoning_simple() {
    use std::sync::{Arc, RwLock};

    // Simplified test showing basic lock poisoning behavior
    let lock = Arc::new(RwLock::new(42));

    // Thread that panics while holding write lock
    let lock1 = Arc::clone(&lock);
    let handle = thread::spawn(move || {
        let _guard = lock1.write().unwrap();
        panic!("Panic while holding lock!");
    });

    let _ = handle.join();

    // Try to acquire lock - should be poisoned
    let lock2 = Arc::clone(&lock);
    let result = panic::catch_unwind(panic::AssertUnwindSafe(move || {
        // This will get a PoisonError
        let guard = lock2.write().expect("lock poisoned");
        *guard
    }));

    assert!(result.is_err(), "Lock should be poisoned but .expect() panicked as expected");
}

#[test]
fn test_what_happens_with_poisoned_lock() {
    use std::sync::{Arc, RwLock};

    println!("\n=== Testing lock poisoning behavior ===");

    let lock = Arc::new(RwLock::new(42));

    // Poison the lock
    let lock1 = Arc::clone(&lock);
    let handle = thread::spawn(move || {
        let mut guard = lock1.write().unwrap();
        *guard = 99;
        panic!("Poisoning the lock!");
    });
    let _ = handle.join();

    println!("Lock has been poisoned");

    // Scenario 1: Using .expect() - This WILL panic
    println!("\n1. Testing .expect() on poisoned lock:");
    let lock2 = Arc::clone(&lock);
    let result = panic::catch_unwind(panic::AssertUnwindSafe(move || {
        let _guard = lock2.write().expect("lock poisoned");
        println!("  This won't print - expect() panicked");
    }));
    match result {
        Ok(_) => println!("  ✗ UNEXPECTED: .expect() succeeded"),
        Err(_) => println!("  ✓ CONFIRMED: .expect() panicked (cascading failure!)"),
    }

    // Scenario 2: Using .unwrap_or_else() to recover
    println!("\n2. Testing recovery with unwrap_or_else:");
    let lock3 = Arc::clone(&lock);
    match lock3.write() {
        Ok(_guard) => println!("  Lock acquired (not poisoned)"),
        Err(poisoned) => {
            println!("  ✓ Lock is poisoned, but we can recover the data");
            let guard = poisoned.into_inner();
            println!("  Value in poisoned lock: {}", *guard);
        }
    }

    // Scenario 3: Show that .unwrap() also panics
    println!("\n3. Testing .unwrap() on poisoned lock:");
    let lock4 = Arc::clone(&lock);
    let result = panic::catch_unwind(panic::AssertUnwindSafe(move || {
        let _guard = lock4.write().unwrap();
    }));
    match result {
        Ok(_) => println!("  ✗ UNEXPECTED: .unwrap() succeeded"),
        Err(_) => println!("  ✓ CONFIRMED: .unwrap() also panicked"),
    }

    println!("\n=== Summary ===");
    println!("✓ Locks CAN be poisoned if thread panics while holding them");
    println!("✓ .expect() and .unwrap() cause cascading failures");
    println!("✓ Proper handling: match on Result and use .into_inner() to recover");
}
