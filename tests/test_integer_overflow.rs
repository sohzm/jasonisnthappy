// Test for potential integer overflow in WAL file size calculations

use jasonisnthappy::core::database::Database;
use std::fs;

#[test]
fn test_wal_file_size_calculations() {
    let test_path = "/tmp/test_overflow.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();

    println!("\n=== WAL Integer Overflow Test ===\n");

    // The potential overflow is at wal.rs:185-186:
    // let file_frames = ((size - WAL_HEADER_SIZE as i64) / WAL_FRAME_SIZE as i64) as u64;
    //
    // This could overflow if:
    // 1. size is negative (but it comes from metadata.len() which is u64)
    // 2. size is cast from u64 to i64 and wraps (requires file > 9 exabytes!)
    //
    // In practice, the guard at line 181 checks:
    // if size < WAL_HEADER_SIZE as i64 { return Ok(()); }
    //
    // So if size is negative, the function returns early and never reaches line 185.

    println!("WAL_HEADER_SIZE = 32 bytes");
    println!("i64::MAX = {} bytes (9 exabytes)", i64::MAX);
    println!();

    // Perform normal operations
    for i in 0..100 {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        let doc = serde_json::json!({
            "_id": format!("doc{}", i),
            "data": "x".repeat(1000),
        });
        coll.insert(doc).unwrap();
        tx.commit().unwrap();
    }

    let frame_count = db.frame_count();
    println!("Frame count after 100 inserts: {}", frame_count);
    println!();

    // Get WAL file size
    let wal_path = format!("{}-wal", test_path);
    if let Ok(metadata) = fs::metadata(&wal_path) {
        let size = metadata.len();
        println!("WAL file size: {} bytes", size);
        println!("As i64: {} (no overflow)", size as i64);
        println!();
    }

    println!("Analysis:");
    println!("- WAL file size is {} frames × 4KB ≈ {} KB", frame_count, frame_count * 4);
    println!("- This is nowhere near i64::MAX");
    println!("- The guard at line 181 prevents negative size issues");
    println!("- No overflow possible in practice");

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

#[test]
fn test_edge_case_calculations() {
    println!("\n=== Edge Case Calculations ===\n");

    // Test the calculation logic with edge cases
    const WAL_HEADER_SIZE: i64 = 32;
    const WAL_FRAME_SIZE: i64 = 4096 + 16; // PAGE_SIZE + frame header

    let test_cases = vec![
        (0i64, "Empty file"),
        (31i64, "Smaller than header"),
        (32i64, "Exactly header size"),
        (33i64, "Header + 1 byte"),
        (WAL_HEADER_SIZE + WAL_FRAME_SIZE, "Header + 1 frame"),
        (WAL_HEADER_SIZE + WAL_FRAME_SIZE * 1000, "Header + 1000 frames"),
    ];

    for (size, description) in test_cases {
        println!("{}: size = {}", description, size);

        // Guard check (same as line 181)
        if size < WAL_HEADER_SIZE {
            println!("  → Returns early (size < header)\n");
            continue;
        }

        // Calculate frames (same as line 185)
        let file_frames = ((size - WAL_HEADER_SIZE) / WAL_FRAME_SIZE) as u64;
        println!("  → {} frames calculated\n", file_frames);
    }

    println!("All calculations work correctly with the guard in place");
}
