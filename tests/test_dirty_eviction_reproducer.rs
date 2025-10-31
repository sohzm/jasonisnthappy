/// DEFINITIVE REPRODUCER FOR ISSUE #5
///
/// The actual bug: When cache is full of dirty pages and we try to add a new dirty page,
/// the insertion SILENTLY FAILS because:
/// 1. put_dirty() calls put() first (which adds the page as clean)
/// 2. put() triggers eviction when cache exceeds capacity
/// 3. eviction scans for clean pages and finds the newly added page (not yet marked dirty!)
/// 4. eviction removes the newly added page
/// 5. Then put_dirty() tries to mark it dirty, but it's already evicted
///
/// Impact: During heavy write workloads, page writes can silently fail,
/// leading to data loss or corruption.

use jasonisnthappy::core::lru_cache::LRUCache;

#[test]
fn test_put_dirty_fails_when_cache_full_of_dirty_pages() {
    println!("\n=== REPRODUCER: put_dirty() Silently Fails ===\n");

    let cache = LRUCache::new(3);

    // Fill cache with dirty pages
    cache.put_dirty(1, vec![1; 100]);
    cache.put_dirty(2, vec![2; 100]);
    cache.put_dirty(3, vec![3; 100]);

    println!("Cache filled with 3 dirty pages");
    assert_eq!(cache.len(), 3);
    assert!(cache.is_dirty(1));
    assert!(cache.is_dirty(2));
    assert!(cache.is_dirty(3));

    // Try to add a 4th dirty page
    println!("Attempting to add page 4...");
    cache.put_dirty(4, vec![4; 100]);

    // Check if page 4 made it into the cache
    let has_page_4 = cache.get_read_only(4).is_some();
    let cache_size = cache.len();

    println!("After put_dirty(4):");
    println!("  Cache size: {}", cache_size);
    println!("  Page 4 in cache: {}", has_page_4);

    if !has_page_4 {
        println!("\n❌ BUG CONFIRMED!");
        println!("   put_dirty(4) SILENTLY FAILED");
        println!("   The page was added but immediately evicted before being marked dirty");
        println!("   This can cause DATA LOSS in real workloads!");
        panic!("put_dirty() silently failed - this is a critical bug");
    } else {
        println!("\n✅ Page 4 was successfully added");
    }
}

#[test]
fn test_data_loss_scenario() {
    println!("\n=== DATA LOSS SCENARIO ===\n");

    let cache = LRUCache::new(5);

    // Simulate a bulk write where all pages become dirty
    for i in 0..5 {
        cache.put_dirty(i, vec![i as u8; 200]);
    }

    println!("Wrote 5 pages (cache full of dirty pages)");

    // Try to write more pages - these should not silently fail!
    let mut failed_inserts = vec![];

    for i in 5..10 {
        cache.put_dirty(i, vec![i as u8; 200]);

        if cache.get_read_only(i).is_none() {
            failed_inserts.push(i);
            println!("  Page {} insertion FAILED silently!", i);
        }
    }

    if !failed_inserts.is_empty() {
        println!("\n❌ CRITICAL BUG:");
        println!("   {} out of 5 page insertions FAILED silently", failed_inserts.len());
        println!("   Failed pages: {:?}", failed_inserts);
        println!("   In a real database, this would cause DATA LOSS!");
        panic!("{} page insertions failed silently", failed_inserts.len());
    } else {
        println!("\n✅ All pages inserted successfully");
    }
}
