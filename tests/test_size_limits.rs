/// Tests for bulk operation size limits
use jasonisnthappy::{Database, DatabaseOptions};
use serde_json::json;
use tempfile::TempDir;

#[test]
fn test_insert_many_respects_limit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let opts = DatabaseOptions {
        max_bulk_operations: 100, // Set low limit for testing
        ..Default::default()
    };

    let db = Database::open_with_options(db_path.to_str().unwrap(), opts).unwrap();
    let coll = db.collection("test");

    // Try to insert 101 documents (exceeds limit of 100)
    let docs: Vec<_> = (0..101)
        .map(|i| json!({"_id": format!("doc{}", i), "value": i}))
        .collect();

    let result = coll.insert_many(docs);
    assert!(result.is_err(), "Should fail when exceeding limit");

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("101"), "Error should mention actual count");
    assert!(err_msg.contains("100"), "Error should mention limit");

    // Should succeed with exactly the limit
    let docs: Vec<_> = (0..100)
        .map(|i| json!({"_id": format!("doc{}", i), "value": i}))
        .collect();

    assert!(coll.insert_many(docs).is_ok(), "Should succeed at exact limit");

    db.close().unwrap();
}

#[test]
fn test_bulk_write_respects_limit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let opts = DatabaseOptions {
        max_bulk_operations: 50, // Set low limit for testing
        ..Default::default()
    };

    let db = Database::open_with_options(db_path.to_str().unwrap(), opts).unwrap();
    let coll = db.collection("test");

    // Build bulk write with 51 operations (exceeds limit)
    let mut bulk = coll.bulk_write();
    for i in 0..51 {
        bulk = bulk.insert(json!({"_id": format!("doc{}", i), "value": i}));
    }

    let result = bulk.execute();
    assert!(result.is_err(), "Should fail when exceeding limit");

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("51"), "Error should mention actual count");
    assert!(err_msg.contains("50"), "Error should mention limit");

    // Should succeed with exactly the limit
    let mut bulk = coll.bulk_write();
    for i in 0..50 {
        bulk = bulk.insert(json!({"_id": format!("doc{}", i), "value": i}));
    }

    assert!(bulk.execute().is_ok(), "Should succeed at exact limit");

    db.close().unwrap();
}

#[test]
fn test_default_limits_are_generous() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Use default options
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    assert_eq!(db.max_bulk_operations(), 100_000, "Default should be 100K");
    assert_eq!(db.max_document_size(), 67_108_864, "Default should be 64MB");
    assert_eq!(db.max_request_body_size(), 52_428_800, "Default should be 50MB");

    db.close().unwrap();
}

#[test]
fn test_custom_limits() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let opts = DatabaseOptions {
        max_bulk_operations: 500_000,
        max_document_size: 100_000_000, // 100MB
        max_request_body_size: 75_000_000, // 75MB
        ..Default::default()
    };

    let db = Database::open_with_options(db_path.to_str().unwrap(), opts).unwrap();

    assert_eq!(db.max_bulk_operations(), 500_000);
    assert_eq!(db.max_document_size(), 100_000_000);
    assert_eq!(db.max_request_body_size(), 75_000_000);

    db.close().unwrap();
}

#[test]
fn test_empty_bulk_operations_allowed() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let coll = db.collection("test");

    // Empty insert_many should succeed
    let result = coll.insert_many(vec![]);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0);

    // Empty bulk_write should succeed
    let result = coll.bulk_write().execute();
    assert!(result.is_ok());

    db.close().unwrap();
}

#[test]
fn test_large_but_within_limit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Use default options (with cache sized for large bulk operations)
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let coll = db.collection("test");

    // Insert 10,000 documents (well within default 100K limit)
    let docs: Vec<_> = (0..10_000)
        .map(|i| json!({"_id": format!("doc{}", i), "value": i}))
        .collect();

    let result = coll.insert_many(docs);
    if let Err(e) = &result {
        eprintln!("Error: {:?}", e);
    }
    assert!(result.is_ok(), "Should succeed with 10K docs");
    assert_eq!(result.unwrap().len(), 10_000);

    db.close().unwrap();
}
