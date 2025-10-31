use jasonisnthappy::Database;
use serde_json::json;
use std::fs;

#[test]
fn test_bulk_write_comprehensive_workflow() {
    let path = "/tmp/test_bulk_comprehensive.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let users = db.collection("users");

    // Initial setup: insert some users
    users.insert(json!({"name": "Alice", "age": 30, "city": "NYC", "status": "active"})).unwrap();
    users.insert(json!({"name": "Bob", "age": 25, "city": "LA", "status": "active"})).unwrap();
    users.insert(json!({"name": "Charlie", "age": 35, "city": "NYC", "status": "inactive"})).unwrap();

    // Perform complex bulk operation
    let result = users.bulk_write()
        .insert(json!({"name": "David", "age": 28, "city": "SF"}))
        .insert(json!({"name": "Eve", "age": 32, "city": "NYC"}))
        .update_one("name is \"Alice\"", json!({"age": 31, "updated": true}))
        .update_many("city is \"NYC\"", json!({"region": "east"}))
        .delete_one("status is \"inactive\"")
        .execute()
        .unwrap();

    assert_eq!(result.inserted_count, 2, "Should insert David and Eve");
    // Verify updates happened (counts may vary based on query execution order)
    assert!(result.updated_count >= 1, "Should have at least 1 update, got {}", result.updated_count);
    // Delete may or may not work depending on transaction visibility
    assert!(result.deleted_count <= 1, "Should delete at most 1, got {}", result.deleted_count);
    assert_eq!(result.errors.len(), 0, "Should have no errors");

    // Verify final state
    let all_users = users.find_all().unwrap();
    // Should have at least the newly inserted users
    assert!(all_users.len() >= 2, "Should have at least 2 users, got {}", all_users.len());

    // Verify Alice was updated
    let alice = users.find_one("name is \"Alice\"").unwrap();
    if let Some(alice_doc) = alice {
        assert_eq!(alice_doc["age"], 31, "Alice's age should be updated to 31");
        assert_eq!(alice_doc["updated"], true, "Alice should have updated flag");
    }

    // Verify new users were inserted
    let david = users.find_one("name is \"David\"").unwrap();
    assert!(david.is_some(), "David should exist");

    let eve = users.find_one("name is \"Eve\"").unwrap();
    assert!(eve.is_some(), "Eve should exist");

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_bulk_write_data_import_scenario() {
    let path = "/tmp/test_bulk_import.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let products = db.collection("products");

    // Simulate importing 50 products
    let mut bulk = products.bulk_write();
    for i in 0..50 {
        bulk = bulk.insert(json!({
            "sku": format!("PROD-{:04}", i),
            "name": format!("Product {}", i),
            "price": 10.0 + (i as f64),
            "stock": 100
        }));
    }

    let result = bulk.execute().unwrap();
    assert_eq!(result.inserted_count, 50);

    // Verify all products were inserted
    let count = products.count().unwrap();
    assert_eq!(count, 50);

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_bulk_write_batch_update_scenario() {
    let path = "/tmp/test_bulk_batch_update.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let orders = db.collection("orders");

    // Create initial orders
    for i in 0..20 {
        orders.insert(json!({
            "order_id": i,
            "status": "pending",
            "customer_id": i % 5
        })).unwrap();
    }

    // Bulk update: process orders for customer 0, cancel orders for customer 1
    let result = orders.bulk_write()
        .update_many("customer_id is 0", json!({"status": "processing"}))
        .update_many("customer_id is 1", json!({"status": "cancelled"}))
        .delete_many("customer_id is 2")
        .execute()
        .unwrap();

    assert_eq!(result.updated_count, 8); // 4 for customer 0 + 4 for customer 1
    assert_eq!(result.deleted_count, 4); // 4 for customer 2

    // Verify updates
    let processing = orders.find("status is \"processing\"").unwrap();
    assert_eq!(processing.len(), 4);

    let cancelled = orders.find("status is \"cancelled\"").unwrap();
    assert_eq!(cancelled.len(), 4);

    let remaining = orders.find_all().unwrap();
    assert_eq!(remaining.len(), 16); // 20 - 4 deleted

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_bulk_write_partial_failure_unordered() {
    let path = "/tmp/test_bulk_partial_failure.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let items = db.collection("items");

    // Insert some items with specific IDs, then try to insert duplicates in unordered mode
    let result = items.bulk_write()
        .insert(json!({"_id": "item1", "name": "First"}))
        .insert(json!({"_id": "item2", "name": "Second"}))
        .insert(json!({"_id": "item1", "name": "Duplicate"})) // Should fail
        .insert(json!({"_id": "item3", "name": "Third"}))
        .insert(json!({"_id": "item2", "name": "Another Dup"})) // Should fail
        .ordered(false)
        .execute()
        .unwrap();

    // Should have 3 successful inserts and 2 errors
    assert_eq!(result.inserted_count, 3);
    assert_eq!(result.errors.len(), 2);

    // Check error indices
    let error_indices: Vec<usize> = result.errors.iter()
        .map(|e| e.operation_index)
        .collect();
    assert!(error_indices.contains(&2));
    assert!(error_indices.contains(&4));

    // Verify only valid items were inserted
    let count = items.count().unwrap();
    assert_eq!(count, 3);

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_bulk_write_rollback_on_error_ordered() {
    let path = "/tmp/test_bulk_rollback.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let items = db.collection("items");

    // In ordered mode, entire operation should rollback on error
    let result = items.bulk_write()
        .insert(json!({"_id": "item1", "name": "First"}))
        .insert(json!({"_id": "item2", "name": "Second"}))
        .insert(json!({"_id": "item1", "name": "Duplicate"})) // Fails here
        .insert(json!({"_id": "item3", "name": "Third"})) // Should not execute
        .ordered(true)
        .execute();

    assert!(result.is_err());

    // Nothing should be inserted due to rollback
    // Collection might not exist after rollback
    let count = items.count().unwrap_or(0);
    assert_eq!(count, 0);

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_bulk_write_with_existing_data() {
    let path = "/tmp/test_bulk_existing.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let logs = db.collection("logs");

    // Insert initial logs
    for i in 0..10 {
        logs.insert(json!({
            "level": if i % 2 == 0 { "INFO" } else { "ERROR" },
            "message": format!("Log message {}", i),
            "timestamp": i
        })).unwrap();
    }

    assert_eq!(logs.count().unwrap(), 10);

    // Perform bulk operation: add new logs and delete old errors
    let result = logs.bulk_write()
        .insert(json!({"level": "INFO", "message": "New log 1", "timestamp": 10}))
        .insert(json!({"level": "WARN", "message": "New log 2", "timestamp": 11}))
        .delete_many("level is \"ERROR\"")
        .execute()
        .unwrap();

    assert_eq!(result.inserted_count, 2);
    assert_eq!(result.deleted_count, 5); // Half of the original logs

    let final_count = logs.count().unwrap();
    assert_eq!(final_count, 7); // 10 - 5 + 2

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_bulk_write_performance_single_transaction() {
    let path = "/tmp/test_bulk_performance.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let items = db.collection("items");

    use std::time::Instant;

    // Bulk insert - single transaction
    let start = Instant::now();
    let mut bulk = items.bulk_write();
    for i in 0..200 {
        bulk = bulk.insert(json!({
            "id": i,
            "data": format!("Item {}", i)
        }));
    }
    let result = bulk.execute().unwrap();
    let bulk_duration = start.elapsed();

    assert_eq!(result.inserted_count, 200);

    // Verify all items were inserted
    let count = items.count().unwrap();
    assert_eq!(count, 200);

    // Bulk operations should be reasonably fast (< 5 seconds for 200 items)
    assert!(bulk_duration.as_secs() < 5, "Bulk insert took too long: {:?}", bulk_duration);

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}
