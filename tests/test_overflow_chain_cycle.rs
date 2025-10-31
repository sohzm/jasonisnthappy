/// Test to demonstrate overflow page chain cycle vulnerability
/// This test shows that corrupted overflow chains can cause infinite loops
use jasonisnthappy::core::constants::*;
use jasonisnthappy::core::document::*;
use jasonisnthappy::core::pager::Pager;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

/// Test Case 1: Overflow chain with a cycle causes excessive memory allocation
///
/// Setup: Create a document with 2 overflow pages, then manually corrupt
/// the second overflow page to point back to the first overflow page,
/// creating a cycle: first_page -> overflow1 -> overflow2 -> overflow1 (cycle!)
///
/// With corrupted data_len, the read loop will cycle through the same pages
/// thousands of times, accumulating duplicate data and consuming excessive memory.
#[test]
fn test_overflow_chain_cycle_memory_exhaustion() {
    let path = "/tmp/test_overflow_cycle.db";
    let _ = fs::remove_file(path);

    // Create a large document that needs overflow pages
    // We need at least 2 overflow pages to create a cycle
    let data = vec![42u8; PAGE_SIZE * 3]; // 3 pages worth of data

    let pager = Pager::open(path, 100, 0o644, false).unwrap();
    let page_num = write_document(&pager, "large_doc", &data).unwrap();

    // Now manually corrupt the overflow chain to create a cycle
    // We need to find the overflow pages and create a cycle

    // Read the first page to find the first overflow pointer
    let first_page_data = pager.read_page(page_num).unwrap();
    let overflow_offset = PAGE_SIZE - 8;
    let first_overflow = u64::from_le_bytes(
        first_page_data[overflow_offset..overflow_offset + 8]
            .try_into()
            .unwrap()
    );

    println!("First overflow page: {}", first_overflow);

    if first_overflow != 0 {
        // Read the first overflow page to find the second overflow
        let overflow1_data = pager.read_page(first_overflow).unwrap();
        let second_overflow = u64::from_le_bytes(
            overflow1_data[PAGE_SIZE - 8..]
                .try_into()
                .unwrap()
        );

        println!("Second overflow page: {}", second_overflow);

        if second_overflow != 0 {
            // Create the cycle: make overflow2 point back to overflow1
            // This simulates corruption that could happen due to a bug or disk corruption
            let mut corrupted_overflow2 = pager.read_page(second_overflow).unwrap();
            corrupted_overflow2[PAGE_SIZE - 8..].copy_from_slice(&first_overflow.to_le_bytes());

            // Write the corrupted page back
            pager.write_page(second_overflow, &corrupted_overflow2).unwrap();

            // ALSO corrupt the first page to have a larger data_len
            // This forces the loop to keep following the cycle
            let mut corrupted_first = pager.read_page(page_num).unwrap();
            let id_len = u16::from_le_bytes(corrupted_first[0..2].try_into().unwrap()) as usize;
            let data_len_offset = 2 + id_len;

            // Set data_len to 10MB (much larger than actual data)
            let fake_data_len = 10_000_000u32;
            corrupted_first[data_len_offset..data_len_offset + 4].copy_from_slice(&fake_data_len.to_le_bytes());
            pager.write_page(page_num, &corrupted_first).unwrap();

            pager.flush().unwrap();

            println!("Created cycle: overflow2({}) -> overflow1({}) with corrupted data_len={}",
                     second_overflow, first_overflow, fake_data_len);

            // Now try to read the document - this will hang in an infinite loop
            // We'll run this in a separate thread with a timeout
            let read_completed = Arc::new(AtomicBool::new(false));
            let read_completed_clone = read_completed.clone();
            let iterations = Arc::new(AtomicUsize::new(0));
            let iterations_clone = iterations.clone();

            let pager_clone = Pager::open(path, 100, 0o644, false).unwrap();
            let _handle = thread::spawn(move || {
                // Try to read - this should hang
                let result = read_document_with_iteration_count(&pager_clone, page_num, iterations_clone);
                read_completed_clone.store(true, Ordering::SeqCst);
                result
            });

            // Wait for 2 seconds
            thread::sleep(Duration::from_secs(2));

            let completed = read_completed.load(Ordering::SeqCst);
            let iter_count = iterations.load(Ordering::SeqCst);

            println!("After 2 seconds: completed={}, iterations={}", completed, iter_count);

            // The vulnerability: with a cycle, the same pages are read repeatedly
            // With corrupted data_len=10MB and only 2 overflow pages (~8KB each),
            // the loop must cycle through ~1223 times to accumulate 10MB of (duplicate) data
            // This demonstrates:
            // 1. No cycle detection - same pages read 1000+ times
            // 2. Excessive memory allocation - allocates 10MB for a document that's actually only ~12KB
            // 3. Performance degradation - takes 2+ seconds for what should be instant

            if completed {
                // It completed by accumulating garbage data
                assert!(iter_count > 1000,
                    "With corrupted data_len, should cycle 1000+ times. Got {} iterations", iter_count);
                println!("✅ Successfully demonstrated overflow chain cycle vulnerability:");
                println!("   - Cycled through same 2 pages {} times", iter_count);
                println!("   - Accumulated ~10MB of duplicate garbage data");
                println!("   - No cycle detection - severe performance degradation");
                println!("   - Potential memory exhaustion with larger corrupted data_len");
            } else {
                // Still running after 2 seconds - also demonstrates the problem
                assert!(iter_count > 100,
                    "Should have looped many times, got {} iterations", iter_count);
                println!("✅ Successfully demonstrated overflow chain causes performance issue:");
                println!("   - Still running after 2 seconds");
                println!("   - Already looped {} times", iter_count);
            }
        }
    }

    pager.close().unwrap();
    let _ = fs::remove_file(path);
}

/// Helper function that counts iterations while reading overflow chain
fn read_document_with_iteration_count(
    pager: &Pager,
    page_num: PageNum,
    iterations: Arc<AtomicUsize>,
) -> jasonisnthappy::core::errors::Result<Document> {
    let page_data = pager.read_page(page_num)?;
    let mut offset = 0;

    let id_len = u16::from_le_bytes(page_data[offset..offset + 2].try_into().unwrap()) as usize;
    offset += 2;

    let doc_id = String::from_utf8(page_data[offset..offset + id_len].to_vec())
        .map_err(|_| jasonisnthappy::core::errors::Error::InvalidDocument)?;
    offset += id_len;

    let data_len = u32::from_le_bytes(page_data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let overflow_offset = PAGE_SIZE - 8;
    let overflow_page = u64::from_le_bytes(page_data[overflow_offset..overflow_offset + 8].try_into().unwrap());

    if overflow_page == 0 {
        let data = page_data[offset..offset + data_len].to_vec();
        Ok(Document { id: doc_id, data })
    } else {
        let first_chunk_size = PAGE_SIZE - DOC_ID_LEN_SIZE - id_len - DATA_LEN_SIZE - OVERFLOW_SIZE;
        let mut data = Vec::with_capacity(data_len);
        let first_data_len = first_chunk_size.min(data_len);
        data.extend_from_slice(&page_data[offset..offset + first_data_len]);

        let mut current_overflow = overflow_page;

        // This is the vulnerable loop - no cycle detection!
        while current_overflow != 0 && data.len() < data_len {
            iterations.fetch_add(1, Ordering::SeqCst);

            let overflow_data = pager.read_page(current_overflow)?;
            let remaining = data_len - data.len();
            let chunk_size = remaining.min(MAX_OVERFLOW_DATA);

            data.extend_from_slice(&overflow_data[..chunk_size]);

            current_overflow = u64::from_le_bytes(
                overflow_data[PAGE_SIZE - 8..].try_into().unwrap()
            );
        }

        Ok(Document { id: doc_id, data })
    }
}

/// Test Case 2: Excessively large data_len is now rejected with proper error
/// This tests that data_len validation prevents memory exhaustion
#[test]
fn test_overflow_chain_excessive_length() {
    let path = "/tmp/test_overflow_excessive.db";
    let _ = fs::remove_file(path);

    let pager = Pager::open(path, 100, 0o644, false).unwrap();

    // Create a normal document
    let data = vec![42u8; 1000];
    let page_num = write_document(&pager, "doc", &data).unwrap();

    // Corrupt the data_len field to be extremely large
    let mut corrupted_page = pager.read_page(page_num).unwrap();

    // data_len is at offset 2 (after id_len which is 2 bytes)
    // For doc_id "doc" (3 bytes), data_len is at offset 2 + 2 + 3 = 7
    let id_len = u16::from_le_bytes(corrupted_page[0..2].try_into().unwrap()) as usize;
    let data_len_offset = 2 + id_len;

    // Set data_len to 2GB (exceeds MAX_DOCUMENT_SIZE of 1GB)
    let fake_data_len = 2_000_000_000u32;
    corrupted_page[data_len_offset..data_len_offset + 4].copy_from_slice(&fake_data_len.to_le_bytes());

    pager.write_page(page_num, &corrupted_page).unwrap();
    pager.flush().unwrap();

    println!("Attempting to read document with corrupted data_len=2GB (actual data=1000 bytes)");

    // Try to read - should now return InvalidDocument error instead of panicking
    let result = read_document(&pager, page_num);

    assert!(result.is_err(), "Should reject document with excessive data_len");

    let err_msg = format!("{:?}", result.unwrap_err());
    println!("✅ Properly rejected with error: {}", err_msg);

    assert!(err_msg.contains("InvalidDocument"), "Should be InvalidDocument error");

    pager.close().unwrap();
    let _ = fs::remove_file(path);
}

/// Test Case 3: Delete document with cyclic overflow chain now returns error instead of hanging
#[test]
fn test_delete_document_with_cycle_returns_error() {
    let path = "/tmp/test_delete_cycle.db";
    let _ = fs::remove_file(path);

    let data = vec![42u8; PAGE_SIZE * 3];
    let pager = Pager::open(path, 100, 0o644, false).unwrap();
    let page_num = write_document(&pager, "large_doc", &data).unwrap();

    // Create cycle in overflow chain (same as test 1)
    let first_page_data = pager.read_page(page_num).unwrap();
    let first_overflow = u64::from_le_bytes(
        first_page_data[PAGE_SIZE - 8..]
            .try_into()
            .unwrap()
    );

    if first_overflow != 0 {
        let overflow1_data = pager.read_page(first_overflow).unwrap();
        let second_overflow = u64::from_le_bytes(
            overflow1_data[PAGE_SIZE - 8..]
                .try_into()
                .unwrap()
        );

        if second_overflow != 0 {
            // Create cycle
            let mut corrupted_overflow2 = pager.read_page(second_overflow).unwrap();
            corrupted_overflow2[PAGE_SIZE - 8..].copy_from_slice(&first_overflow.to_le_bytes());
            pager.write_page(second_overflow, &corrupted_overflow2).unwrap();
            pager.flush().unwrap();

            println!("Created cycle: overflow2({}) -> overflow1({})", second_overflow, first_overflow);

            // Try to delete - should now return error instead of hanging
            let result = delete_document(&pager, page_num);

            assert!(result.is_err(), "Should detect cycle and return error");

            let err_msg = format!("{:?}", result.unwrap_err());
            assert!(err_msg.contains("cycle"), "Error should mention cycle: {}", err_msg);

            println!("✅ Delete properly detected cycle and returned error: {}", err_msg);
        }
    }

    pager.close().unwrap();
    let _ = fs::remove_file(path);
}
