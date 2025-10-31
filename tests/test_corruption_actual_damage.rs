//! Tests verifying that corruption is now PREVENTED by validation on open.
//!
//! These tests show that the corruption scenarios that previously caused
//! data loss are now detected and rejected before any damage can occur.

use jasonisnthappy::Database;
use serde_json::json;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

const PAGE_SIZE: usize = 4096;

/// Helper to create a valid database and return its path
fn create_valid_db(name: &str) -> String {
    let path = format!("/tmp/test_actual_corruption_{}.db", name);
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(format!("{}.lock", &path));
    let _ = fs::remove_file(format!("{}-wal", &path));
    path
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

/// Helper to read the header from a database file
fn read_header(path: &str) -> Vec<u8> {
    let mut file = File::open(path).expect("Failed to open file");
    let mut header = vec![0u8; PAGE_SIZE];
    file.read_exact(&mut header).expect("Failed to read header");
    header
}

/// Helper to write a modified header back to the database file
fn write_header(path: &str, header: &[u8]) {
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("Failed to open file for writing");
    file.seek(SeekFrom::Start(0)).expect("Failed to seek");
    file.write_all(header).expect("Failed to write header");
    file.sync_all().expect("Failed to sync");
}

fn get_metadata_page(header: &[u8]) -> u64 {
    u64::from_le_bytes(header[24..32].try_into().unwrap())
}

fn get_free_count(header: &[u8]) -> u32 {
    u32::from_le_bytes(header[20..24].try_into().unwrap())
}

fn set_free_count(header: &mut [u8], free_count: u32) {
    header[20..24].copy_from_slice(&free_count.to_le_bytes());
}

fn set_free_list_entry(header: &mut [u8], index: usize, page_num: u64) {
    let offset = 40 + (index * 8);
    header[offset..offset + 8].copy_from_slice(&page_num.to_le_bytes());
}

// =============================================================================
// TEST 1: Duplicate free list - CORRUPTION NOW PREVENTED
// Previously: Same page allocated twice = data loss
// Now: Detected on open, database refuses to load
// =============================================================================

#[test]
fn test_duplicate_freelist_corruption_is_prevented() {
    let path = create_valid_db("overwrite_prevented");

    // Step 1: Create database with some documents, then delete to create free pages
    {
        let db = Database::open(&path).expect("open");
        let mut tx = db.begin().expect("begin");
        let mut coll = tx.collection("test").expect("collection");

        for i in 0..5 {
            coll.insert(json!({"_id": format!("temp{}", i), "data": "x".repeat(100)}))
                .expect("insert");
        }
        tx.commit().expect("commit");

        let mut tx2 = db.begin().expect("begin");
        let mut coll2 = tx2.collection("test").expect("collection");
        for i in 0..5 {
            coll2.delete_by_id(&format!("temp{}", i)).expect("delete");
        }
        tx2.commit().expect("commit");
    }

    // Step 2: Corrupt free list - add same page multiple times
    // Note: We don't rely on deletes creating free entries (pages are now freed
    // during GC, not during transaction). Instead, we just pick a valid page number
    // and add it as duplicates to the free list.
    let mut header = read_header(&path);

    // Use page 2 (or any page > 0 that exists) as the duplicate entry
    // Page 0 is header, page 1+ are data pages
    let duplicate_page: u64 = 2;
    println!("Corrupting free list: adding page {} multiple times", duplicate_page);

    set_free_count(&mut header, 3);
    set_free_list_entry(&mut header, 0, duplicate_page);
    set_free_list_entry(&mut header, 1, duplicate_page); // DUPLICATE!
    set_free_list_entry(&mut header, 2, duplicate_page); // DUPLICATE!
    write_header(&path, &header);

    // Step 3: Try to reopen - should be REJECTED
    let _ = fs::remove_file(format!("{}.lock", &path));
    let result = Database::open(&path);

    println!("\n=== CORRUPTION PREVENTION VERIFICATION ===");

    match result {
        Ok(_) => {
            panic!("Database should have rejected duplicate free list!");
        }
        Err(err) => {
            let err_msg = err.to_string();
            println!("PREVENTED: Database correctly rejected corrupted file");
            println!("Error: {}", err_msg);
            assert!(err_msg.contains("duplicate"),
                "Error should mention duplicate entries");
        }
    }

    cleanup(&path);
}

// =============================================================================
// TEST 2: Metadata page in free list - CORRUPTION NOW PREVENTED
// Previously: Allocating metadata page overwrote collection info
// Now: Detected on open, database refuses to load
// =============================================================================

#[test]
fn test_metadata_in_freelist_corruption_is_prevented() {
    let path = create_valid_db("meta_destroy_prevented");

    // Step 1: Create database with important data
    {
        let db = Database::open(&path).expect("open");
        let mut tx = db.begin().expect("begin");
        let mut coll = tx.collection("important_collection").expect("collection");

        coll.insert(json!({"_id": "critical1", "data": "very important data 1"})).unwrap();
        coll.insert(json!({"_id": "critical2", "data": "very important data 2"})).unwrap();

        tx.commit().expect("commit");
        println!("Data created successfully");
    }

    // Step 2: Corrupt - add metadata page to free list
    let mut header = read_header(&path);
    let metadata_page = get_metadata_page(&header);
    println!("Metadata page is: {}", metadata_page);

    set_free_count(&mut header, 1);
    set_free_list_entry(&mut header, 0, metadata_page); // CATASTROPHIC!
    write_header(&path, &header);

    // Step 3: Try to reopen - should be REJECTED
    let _ = fs::remove_file(format!("{}.lock", &path));
    let result = Database::open(&path);

    println!("\n=== CORRUPTION PREVENTION VERIFICATION ===");

    match result {
        Ok(_) => {
            panic!("Database should have rejected metadata page in free list!");
        }
        Err(err) => {
            let err_msg = err.to_string();
            println!("PREVENTED: Database correctly rejected corrupted file");
            println!("Error: {}", err_msg);
            assert!(err_msg.contains("metadata_page") && err_msg.contains("free_list"),
                "Error should mention metadata_page in free_list");
        }
    }

    cleanup(&path);
}

// =============================================================================
// TEST 3: Invalid free list entry - CORRUPTION NOW PREVENTED
// Previously: Writing to page 99999 created 409MB sparse file
// Now: Detected on open, database refuses to load
// =============================================================================

#[test]
fn test_invalid_freelist_corruption_is_prevented() {
    let path = create_valid_db("invalid_write_prevented");

    // Step 1: Create a small database
    {
        let db = Database::open(&path).expect("open");
        let mut tx = db.begin().expect("begin");
        let mut coll = tx.collection("safe").expect("collection");
        coll.insert(json!({"_id": "safe_doc", "value": 12345})).unwrap();
        tx.commit().unwrap();
    }

    let file_size = fs::metadata(&path).unwrap().len();
    println!("File has {} bytes", file_size);

    // Step 2: Corrupt free list with invalid page numbers
    let mut header = read_header(&path);
    let invalid_page = 99999; // Way beyond file
    set_free_count(&mut header, 1);
    set_free_list_entry(&mut header, 0, invalid_page);
    write_header(&path, &header);

    // Step 3: Try to open - should be REJECTED
    let _ = fs::remove_file(format!("{}.lock", &path));
    let result = Database::open(&path);

    println!("\n=== CORRUPTION PREVENTION VERIFICATION ===");

    match result {
        Ok(_) => {
            panic!("Database should have rejected invalid free list entries!");
        }
        Err(err) => {
            let err_msg = err.to_string();
            println!("PREVENTED: Database correctly rejected corrupted file");
            println!("Error: {}", err_msg);
            assert!(err_msg.contains("free_list") && err_msg.contains("99999"),
                "Error should mention invalid free_list entry");
        }
    }

    // Verify file wasn't corrupted (no sparse file created)
    let new_file_size = fs::metadata(&path).unwrap().len();
    assert_eq!(file_size, new_file_size,
        "File size should remain unchanged (no sparse file created)");
    println!("File size unchanged: {} bytes (corruption prevented!)", new_file_size);

    cleanup(&path);
}

// =============================================================================
// SUMMARY: Prove corruption is now prevented
// =============================================================================

#[test]
fn test_summary_corruption_prevention() {
    println!("\n");
    println!("=================================================================");
    println!("CORRUPTION PREVENTION SUMMARY");
    println!("=================================================================");
    println!("");
    println!("All previously demonstrated corruption scenarios are now PREVENTED:");
    println!("");
    println!("1. DUPLICATE FREE LIST → PREVENTED ✓");
    println!("   - Previously: Two documents got same page, data loss");
    println!("   - Now: Detected on open with 'duplicate page' error");
    println!("");
    println!("2. METADATA IN FREE LIST → PREVENTED ✓");
    println!("   - Previously: Metadata overwritten, database unusable");
    println!("   - Now: Detected on open with 'metadata_page in free_list' error");
    println!("");
    println!("3. INVALID FREE LIST → PREVENTED ✓");
    println!("   - Previously: 409MB sparse file, wasted disk space");
    println!("   - Now: Detected on open with 'invalid free_list entry' error");
    println!("");
    println!("=================================================================");
    println!("CONCLUSION: Corruption detection on open successfully prevents");
    println!("data loss that would have occurred with previous implementation.");
    println!("=================================================================");
}
