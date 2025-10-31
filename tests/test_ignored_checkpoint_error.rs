// Test to demonstrate the ignored checkpoint error problem

use std::fs;
use jasonisnthappy::core::database::Database;

#[test]
fn test_checkpoint_error_silently_ignored() {
    let test_path = "/tmp/test_checkpoint_error.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();

    // Set very low checkpoint threshold to trigger frequent checkpoints
    db.set_auto_checkpoint_threshold(10);

    // Insert many documents to trigger checkpoints
    for i in 0..100 {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();
        let doc = serde_json::json!({"_id": format!("doc{}", i), "x": i});
        coll.insert(doc).unwrap();
        tx.commit().unwrap();
    }

    // If checkpoint failed silently, the WAL would keep growing
    // but we'd have no way to know about it

    let frame_count = db.frame_count();
    println!("Frame count after 100 inserts: {}", frame_count);

    // This test can't actually demonstrate the bug without causing
    // a real checkpoint failure (e.g., disk full, permissions issue)
    // But it shows that errors are silently swallowed

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

#[test]
fn test_what_happens_if_checkpoint_fails() {
    /*
    Current behavior (line database.rs:664):
        let result = self.wal.checkpoint(&self.pager);
        let _ = result;  // ← Error silently ignored!

    Problems:
    1. If checkpoint fails due to I/O error (disk full, permissions, etc)
    2. WAL keeps growing unbounded
    3. No error logged, no metrics updated, no alert
    4. Eventually leads to:
       - Excessive disk usage
       - Slower reads (must replay huge WAL)
       - Potential data loss if WAL gets corrupted

    Fix needed:
    - At minimum: Log the error
    - Better: Update metrics, allow monitoring
    - Best: Propagate error to caller if critical
    */

    println!("This test documents the ignored checkpoint error issue");
}
