/// Test to verify overflow page chain validation is working correctly
/// These tests verify that the cycle detection and validation prevent the issues
/// demonstrated in test_overflow_chain_cycle.rs
use jasonisnthappy::core::constants::*;
use jasonisnthappy::core::document::*;
use jasonisnthappy::core::pager::Pager;
use std::fs;

/// Test Case 1: Verify cycle detection works - reading a document with a cycle returns an error
#[test]
fn test_cycle_detection_on_read() {
    let path = "/tmp/test_cycle_detection.db";
    let _ = fs::remove_file(path);

    // Create a large document that needs overflow pages
    let data = vec![42u8; PAGE_SIZE * 3];
    let pager = Pager::open(path, 100, 0o644, false).unwrap();
    let page_num = write_document(&pager, "large_doc", &data).unwrap();

    // Create a cycle in the overflow chain
    let first_page_data = pager.read_page(page_num).unwrap();
    let first_overflow = u64::from_le_bytes(
        first_page_data[PAGE_SIZE - 8..].try_into().unwrap()
    );

    if first_overflow != 0 {
        let overflow1_data = pager.read_page(first_overflow).unwrap();
        let second_overflow = u64::from_le_bytes(
            overflow1_data[PAGE_SIZE - 8..].try_into().unwrap()
        );

        if second_overflow != 0 {
            // Create the cycle
            let mut corrupted_overflow2 = pager.read_page(second_overflow).unwrap();
            corrupted_overflow2[PAGE_SIZE - 8..].copy_from_slice(&first_overflow.to_le_bytes());
            pager.write_page(second_overflow, &corrupted_overflow2).unwrap();
            pager.flush().unwrap();

            println!("Created cycle: overflow2({}) -> overflow1({})", second_overflow, first_overflow);

            // Try to read - should fail with cycle detection error
            let result = read_document(&pager, page_num);

            assert!(result.is_err(), "Should have detected cycle and returned error");

            let err_msg = format!("{:?}", result.unwrap_err());
            assert!(err_msg.contains("cycle"), "Error should mention cycle: {}", err_msg);

            println!("✅ Cycle detection working: {}", err_msg);
        }
    }

    pager.close().unwrap();
    let _ = fs::remove_file(path);
}

/// Test Case 2: Verify excessive data_len is rejected
#[test]
fn test_excessive_data_len_rejected() {
    let path = "/tmp/test_excessive_data_len.db";
    let _ = fs::remove_file(path);

    let pager = Pager::open(path, 100, 0o644, false).unwrap();
    let data = vec![42u8; 1000];
    let page_num = write_document(&pager, "doc", &data).unwrap();

    // Corrupt data_len to be larger than 100MB (the max allowed)
    let mut corrupted_page = pager.read_page(page_num).unwrap();
    let id_len = u16::from_le_bytes(corrupted_page[0..2].try_into().unwrap()) as usize;
    let data_len_offset = 2 + id_len;

    // Set to 2GB (exceeds MAX_DOCUMENT_SIZE of 1GB)
    let fake_data_len = 2_000_000_000u32;
    corrupted_page[data_len_offset..data_len_offset + 4].copy_from_slice(&fake_data_len.to_le_bytes());
    pager.write_page(page_num, &corrupted_page).unwrap();
    pager.flush().unwrap();

    // Try to read - should fail with InvalidDocument error
    let result = read_document(&pager, page_num);

    assert!(result.is_err(), "Should have rejected excessive data_len");
    println!("✅ Excessive data_len rejected: {:?}", result.unwrap_err());

    pager.close().unwrap();
    let _ = fs::remove_file(path);
}

/// Test Case 3: Verify delete with cycle is detected
#[test]
fn test_cycle_detection_on_delete() {
    let path = "/tmp/test_delete_cycle_detection.db";
    let _ = fs::remove_file(path);

    let data = vec![42u8; PAGE_SIZE * 3];
    let pager = Pager::open(path, 100, 0o644, false).unwrap();
    let page_num = write_document(&pager, "large_doc", &data).unwrap();

    // Create cycle
    let first_page_data = pager.read_page(page_num).unwrap();
    let first_overflow = u64::from_le_bytes(
        first_page_data[PAGE_SIZE - 8..].try_into().unwrap()
    );

    if first_overflow != 0 {
        let overflow1_data = pager.read_page(first_overflow).unwrap();
        let second_overflow = u64::from_le_bytes(
            overflow1_data[PAGE_SIZE - 8..].try_into().unwrap()
        );

        if second_overflow != 0 {
            let mut corrupted_overflow2 = pager.read_page(second_overflow).unwrap();
            corrupted_overflow2[PAGE_SIZE - 8..].copy_from_slice(&first_overflow.to_le_bytes());
            pager.write_page(second_overflow, &corrupted_overflow2).unwrap();
            pager.flush().unwrap();

            // Try to delete - should fail with cycle detection error
            let result = delete_document(&pager, page_num);

            assert!(result.is_err(), "Should have detected cycle during delete");

            let err_msg = format!("{:?}", result.unwrap_err());
            assert!(err_msg.contains("cycle"), "Error should mention cycle: {}", err_msg);

            println!("✅ Cycle detection on delete working: {}", err_msg);
        }
    }

    pager.close().unwrap();
    let _ = fs::remove_file(path);
}

/// Test Case 4: Verify normal documents still work correctly
#[test]
fn test_normal_documents_still_work() {
    let path = "/tmp/test_normal_docs.db";
    let _ = fs::remove_file(path);

    let pager = Pager::open(path, 100, 0o644, false).unwrap();

    // Test small document (single page)
    let small_data = b"Hello, World!";
    let small_page = write_document(&pager, "small", small_data).unwrap();
    let small_doc = read_document(&pager, small_page).unwrap();
    assert_eq!(small_doc.id, "small");
    assert_eq!(small_doc.data, small_data);
    println!("✅ Small document works");

    // Test large document (multiple pages with overflow)
    let large_data = vec![42u8; PAGE_SIZE * 2];
    let large_page = write_document(&pager, "large", &large_data).unwrap();
    let large_doc = read_document(&pager, large_page).unwrap();
    assert_eq!(large_doc.id, "large");
    assert_eq!(large_doc.data, large_data);
    println!("✅ Large document with overflow works");

    // Test delete
    delete_document(&pager, small_page).unwrap();
    delete_document(&pager, large_page).unwrap();
    println!("✅ Delete works");

    pager.close().unwrap();
    let _ = fs::remove_file(path);
}

/// Test Case 5: Verify max chain length limit works
#[test]
fn test_max_chain_length_limit() {
    // Verify the constant is set to allow large documents (up to 1GB)
    // With PAGE_SIZE ~4KB, we need ~250K pages for 1GB documents
    assert!(MAX_OVERFLOW_CHAIN_LENGTH > 0);
    assert!(MAX_OVERFLOW_CHAIN_LENGTH >= 250000, "Should support at least 1GB documents");
    assert!(MAX_OVERFLOW_CHAIN_LENGTH <= 1000000, "Chain length limit should not be excessive");
    println!("✅ MAX_OVERFLOW_CHAIN_LENGTH = {} (supports ~{}MB documents)",
             MAX_OVERFLOW_CHAIN_LENGTH,
             (MAX_OVERFLOW_CHAIN_LENGTH * 4) / 1024);
}
