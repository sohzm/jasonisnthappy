// Debug tests for clone overhead analysis

use jasonisnthappy::core::database::Database;
use std::fs;
use std::time::Instant;

#[test]
fn test_clone_overhead_in_commit() {
    let test_path = "/tmp/test_clone_overhead.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();

    // Test with varying transaction sizes to see impact of cloning
    let sizes = vec![10, 50, 100, 500, 1000];

    for size in sizes {
        let mut times = vec![];

        // Run multiple times for average
        for run in 0..5 {
            let start = Instant::now();

            {
                let mut tx = db.begin().unwrap();
                let mut coll = tx.collection("test").unwrap();

                // Insert many documents - each goes into the writes HashMap
                for i in 0..size {
                    let doc = serde_json::json!({
                        "_id": format!("doc_{}_{}_{}", size, run, i),
                        "data": "x".repeat(1000), // ~1KB per document
                    });
                    coll.insert(doc).unwrap();
                }

                // Commit triggers clones at:
                // - Line 339: writes.iter().map(|(&k, v)| (k, v.clone()))
                // - Line 386 (rebase path): writes.clone()
                tx.commit().unwrap();
            }

            times.push(start.elapsed().as_micros());
        }

        let _avg = times.iter().sum::<u128>() / times.len() as u128;
        let _min = *times.iter().min().unwrap();
        let _max = *times.iter().max().unwrap();
    }

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

#[test]
fn test_memory_allocation_from_clones() {
    /*
    Key clones in transaction.rs:

    1. Line 217: get_writes() returns writes.clone()
       - Clones entire HashMap<PageNum, Vec<u8>>
       - Called from line 698 during unique constraint validation

    2. Line 339: commit() creates writes_snapshot
       - Clones all Vec<u8> values: writes.iter().map(|(&k, v)| (k, v.clone()))
       - Called on EVERY commit

    3. Line 386: Rebase path in commit()
       - Full HashMap clone: writes.clone()
       - Called when concurrent modifications detected

    Impact calculation:
    - Transaction with 100 documents × 4KB each = 400KB writes HashMap
    - Line 339 clone: 400KB copied
    - If rebase needed (line 386): another 400KB copied
    - Total: 800KB of unnecessary allocations per commit

    For 1000 commits: 800MB of wasted allocations!
    */
}
