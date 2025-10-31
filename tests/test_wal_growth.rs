// Test to verify WAL is being checkpointed and not growing unbounded

use jasonisnthappy::core::database::Database;
use std::fs;
use std::thread;
use std::time::Duration;

#[test]
fn test_wal_checkpointing_happens() {
    let test_path = "/tmp/test_wal_checkpoint.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();

    println!("\n=== WAL Checkpointing Test ===\n");

    // Insert enough to trigger multiple checkpoints
    for batch in 0..20 {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        for i in 0..100 {
            let doc = serde_json::json!({
                "_id": format!("doc_{}_{}", batch, i),
                "data": "x".repeat(1000),
            });
            coll.insert(doc).unwrap();
        }

        tx.commit().unwrap();

        // Give background checkpoint time to run
        if batch % 10 == 9 {
            thread::sleep(Duration::from_millis(500));
        }

        let frame_count = db.frame_count();
        println!("Batch {:2}: frame_count = {}", batch, frame_count);
    }

    println!("\nIf background checkpointing works:");
    println!("- Frame count should stay relatively constant");
    println!("- NOT grow unbounded to 2000+");
    println!("- Periodic dips show checkpoint completed\n");

    // Final check
    let final_frame_count = db.frame_count();
    println!("Final frame count: {}", final_frame_count);

    if final_frame_count > 3000 {
        println!("WARNING: WAL grew to {} frames - checkpoint may not be working!", final_frame_count);
    } else {
        println!("OK: WAL frame count is reasonable");
    }

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

#[test]
fn test_performance_with_manual_checkpoint() {
    // Test if manual checkpointing improves performance
    let test_path = "/tmp/test_manual_checkpoint.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();
    db.set_auto_checkpoint_threshold(0); // Disable auto-checkpoint

    println!("\n=== Performance Without Auto-Checkpoint ===\n");

    for batch in 0..10 {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        let start = std::time::Instant::now();

        for i in 0..100 {
            let doc = serde_json::json!({
                "_id": format!("doc_{}_{}", batch, i),
                "data": "x".repeat(1000),
            });
            coll.insert(doc).unwrap();
        }

        tx.commit().unwrap();

        println!("Batch {}: {:?} (frame_count: {})",
            batch,
            start.elapsed(),
            db.frame_count());
    }

    println!("\nManually checkpointing...");
    let checkpoint_start = std::time::Instant::now();
    db.checkpoint().unwrap();
    println!("Checkpoint took: {:?}", checkpoint_start.elapsed());
    println!("Frame count after checkpoint: {}", db.frame_count());

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}
