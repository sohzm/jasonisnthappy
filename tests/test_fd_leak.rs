// Test to check for file descriptor leaks

use jasonisnthappy::core::database::Database;
use std::fs;
use std::process::Command;

#[cfg(target_os = "macos")]
fn get_open_fd_count() -> usize {
    let pid = std::process::id();
    let output = Command::new("lsof")
        .args(&["-p", &pid.to_string()])
        .output()
        .expect("Failed to run lsof");

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.lines().count().saturating_sub(1) // Subtract header line
}

#[cfg(target_os = "linux")]
fn get_open_fd_count() -> usize {
    let pid = std::process::id();
    let path = format!("/proc/{}/fd", pid);
    fs::read_dir(path).map(|d| d.count()).unwrap_or(0)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn get_open_fd_count() -> usize {
    0 // Can't test on other platforms
}

#[test]
fn test_no_fd_leak_on_database_operations() {
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        println!("FD leak test not supported on this platform");
        return;
    }

    let test_path = "/tmp/test_fd_leak.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    println!("\n=== File Descriptor Leak Test ===\n");

    let initial_fds = get_open_fd_count();
    println!("Initial FD count: {}", initial_fds);

    // Open and close database multiple times
    for i in 0..10 {
        {
            let db = Database::open(test_path).unwrap();

            // Do some operations
            let mut tx = db.begin().unwrap();
            let mut coll = tx.collection("test").unwrap();

            for j in 0..10 {
                let doc = serde_json::json!({
                    "_id": format!("doc_{}_{}", i, j),
                    "data": "test"
                });
                coll.insert(doc).unwrap();
            }

            tx.commit().unwrap();

            // db is dropped here
        }

        let current_fds = get_open_fd_count();
        println!("After iteration {}: {} FDs (delta: +{})",
            i, current_fds, current_fds as i32 - initial_fds as i32);

        // Allow some margin for OS buffering, but shouldn't grow unbounded
        if current_fds > initial_fds + 20 {
            println!("\nWARNING: FD count growing! Possible leak detected.");
            println!("Initial: {}, Current: {}", initial_fds, current_fds);
            panic!("File descriptor leak detected!");
        }
    }

    let final_fds = get_open_fd_count();
    println!("\nFinal FD count: {} (delta: +{})",
        final_fds, final_fds as i32 - initial_fds as i32);

    println!("\nIf FD count stayed relatively constant, no leak exists");
    println!("Expected: ±5 FDs due to normal OS operations");

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

#[test]
fn test_no_fd_leak_with_many_transactions() {
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        println!("FD leak test not supported on this platform");
        return;
    }

    let test_path = "/tmp/test_fd_leak_txs.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    println!("\n=== FD Leak Test - Many Transactions ===\n");

    let db = Database::open(test_path).unwrap();

    let initial_fds = get_open_fd_count();
    println!("Initial FD count: {}", initial_fds);

    // Run many transactions without closing database
    for i in 0..100 {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        let doc = serde_json::json!({
            "_id": format!("doc{}", i),
            "data": "test"
        });
        coll.insert(doc).unwrap();
        tx.commit().unwrap();

        if i % 20 == 19 {
            let current_fds = get_open_fd_count();
            println!("After {} txs: {} FDs (delta: +{})",
                i + 1, current_fds, current_fds as i32 - initial_fds as i32);
        }
    }

    let final_fds = get_open_fd_count();
    println!("\nFinal FD count: {} (delta: +{})",
        final_fds, final_fds as i32 - initial_fds as i32);

    if final_fds > initial_fds + 20 {
        println!("\nWARNING: FD leak detected after 100 transactions!");
        panic!("File descriptor leak!");
    } else {
        println!("\nOK: No FD leak detected");
    }

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}
