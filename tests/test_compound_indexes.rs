use jasonisnthappy::Database;
use serde_json::json;
use std::fs;

fn setup_test_db(name: &str) -> Database {
    let path = format!("/tmp/test_compound_idx_{}.db", name);
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    Database::open(&path).unwrap()
}

fn cleanup_test_db(name: &str) {
    let path = format!("/tmp/test_compound_idx_{}.db", name);
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_create_compound_index_simple() {
    let db = setup_test_db("create_simple");

    // Create collection with some documents
    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();

    users.insert(json!({
        "name": "Alice",
        "city": "NYC",
        "age": 30
    })).unwrap();

    users.insert(json!({
        "name": "Bob",
        "city": "SF",
        "age": 25
    })).unwrap();

    tx.commit().unwrap();

    // Create compound index
    let result = db.create_compound_index("users", "city_age_idx", &["city", "age"], false);
    assert!(result.is_ok());

    cleanup_test_db("create_simple");
}

#[test]
fn test_compound_index_enforces_unique_constraint() {
    let db = setup_test_db("unique_constraint");

    // Insert first document
    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();

    users.insert(json!({
        "city": "NYC",
        "age": 30
    })).unwrap();

    tx.commit().unwrap();

    // Create unique compound index
    db.create_compound_index("users", "city_age_idx", &["city", "age"], true).unwrap();

    // Try to insert duplicate combination
    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();

    let result = users.insert(json!({
        "city": "NYC",
        "age": 30
    }));

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("unique constraint"));

    cleanup_test_db("unique_constraint");
}

#[test]
fn test_compound_index_allows_partial_duplicates() {
    let db = setup_test_db("partial_duplicates");

    // Create unique compound index first
    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();
    users.insert(json!({ "city": "NYC", "age": 30 })).unwrap();
    tx.commit().unwrap();

    db.create_compound_index("users", "city_age_idx", &["city", "age"], true).unwrap();

    // Can insert same city with different age
    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();
    let result1 = users.insert(json!({ "city": "NYC", "age": 25 }));
    assert!(result1.is_ok());
    tx.commit().unwrap();

    // Can insert different city with same age
    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();
    let result2 = users.insert(json!({ "city": "SF", "age": 30 }));
    assert!(result2.is_ok());

    cleanup_test_db("partial_duplicates");
}

#[test]
fn test_compound_index_with_nested_fields() {
    let db = setup_test_db("nested_fields");

    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();

    users.insert(json!({
        "name": "Alice",
        "address": {
            "city": "NYC",
            "zip": "10001"
        }
    })).unwrap();

    users.insert(json!({
        "name": "Bob",
        "address": {
            "city": "NYC",
            "zip": "10002"
        }
    })).unwrap();

    tx.commit().unwrap();

    // Create compound index on nested fields
    let result = db.create_compound_index("users", "addr_idx", &["address.city", "address.zip"], false);
    assert!(result.is_ok());

    cleanup_test_db("nested_fields");
}

#[test]
fn test_compound_index_with_null_values() {
    let db = setup_test_db("null_values");

    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();

    users.insert(json!({
        "name": "Alice",
        "city": "NYC",
        "age": 30
    })).unwrap();

    users.insert(json!({
        "name": "Bob",
        "city": "NYC"
        // age is missing (null)
    })).unwrap();

    users.insert(json!({
        "name": "Charlie"
        // both city and age are missing
    })).unwrap();

    tx.commit().unwrap();

    // Create compound index - should handle nulls gracefully
    let result = db.create_compound_index("users", "city_age_idx", &["city", "age"], false);
    assert!(result.is_ok());

    cleanup_test_db("null_values");
}

#[test]
fn test_compound_index_three_fields() {
    let db = setup_test_db("three_fields");

    let mut tx = db.begin().unwrap();
    let mut products = tx.collection("products").unwrap();

    products.insert(json!({
        "category": "electronics",
        "brand": "Apple",
        "model": "iPhone 15",
        "price": 999
    })).unwrap();

    products.insert(json!({
        "category": "electronics",
        "brand": "Samsung",
        "model": "Galaxy S24",
        "price": 899
    })).unwrap();

    tx.commit().unwrap();

    // Create 3-field compound index
    let result = db.create_compound_index("products", "cat_brand_model_idx",
        &["category", "brand", "model"], false);
    assert!(result.is_ok());

    cleanup_test_db("three_fields");
}

#[test]
fn test_compound_index_different_data_types() {
    let db = setup_test_db("different_types");

    let mut tx = db.begin().unwrap();
    let mut events = tx.collection("events").unwrap();

    events.insert(json!({
        "user_id": 123,
        "event_type": "login",
        "is_successful": true,
        "timestamp": "2024-01-15"
    })).unwrap();

    events.insert(json!({
        "user_id": 456,
        "event_type": "logout",
        "is_successful": true,
        "timestamp": "2024-01-15"
    })).unwrap();

    tx.commit().unwrap();

    // Create compound index with different types (number, string, boolean)
    let result = db.create_compound_index("events", "user_event_idx",
        &["user_id", "event_type", "is_successful"], false);
    assert!(result.is_ok());

    cleanup_test_db("different_types");
}

#[test]
fn test_compound_index_prevents_duplicate_creation() {
    let db = setup_test_db("duplicate_creation");

    db.create_compound_index("users", "city_age_idx", &["city", "age"], false).unwrap();

    // Try to create same index again
    let result = db.create_compound_index("users", "city_age_idx", &["city", "age"], false);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));

    cleanup_test_db("duplicate_creation");
}

#[test]
fn test_compound_index_empty_fields_error() {
    let db = setup_test_db("empty_fields");

    let result = db.create_compound_index("users", "empty_idx", &[], false);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("at least one field"));

    cleanup_test_db("empty_fields");
}

#[test]
fn test_single_field_through_compound_api() {
    let db = setup_test_db("single_field_compound");

    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();
    users.insert(json!({ "email": "alice@example.com" })).unwrap();
    tx.commit().unwrap();

    // Creating a "compound" index with single field should work
    let result = db.create_compound_index("users", "email_idx", &["email"], true);
    assert!(result.is_ok());

    // Should enforce uniqueness
    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();
    let result = users.insert(json!({ "email": "alice@example.com" }));
    assert!(result.is_err());

    cleanup_test_db("single_field_compound");
}

#[test]
fn test_backward_compatibility_with_old_create_index() {
    let db = setup_test_db("backward_compat");

    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();
    users.insert(json!({ "email": "alice@example.com", "name": "Alice" })).unwrap();
    tx.commit().unwrap();

    // Old API should still work
    let result = db.create_index("users", "email_idx", "email", true);
    assert!(result.is_ok());

    // Unique constraint should work
    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();
    let result = users.insert(json!({ "email": "alice@example.com", "name": "Alice2" }));
    assert!(result.is_err());

    cleanup_test_db("backward_compat");
}

#[test]
fn test_compound_index_survives_reopen() {
    let db_path = "/tmp/test_compound_idx_reopen.db";
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(format!("{}.lock", db_path));
    let _ = fs::remove_file(format!("{}-wal", db_path));

    {
        let db = Database::open(db_path).unwrap();

        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();
        users.insert(json!({ "city": "NYC", "age": 30 })).unwrap();
        tx.commit().unwrap();

        db.create_compound_index("users", "city_age_idx", &["city", "age"], true).unwrap();

        // Test that unique constraint works BEFORE closing
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();
        let result = users.insert(json!({ "city": "NYC", "age": 30 }));
        assert!(result.is_err(), "Should fail before closing");

        db.close().unwrap();
    }

    // Reopen database
    {
        let db = Database::open(db_path).unwrap();

        // First, verify the existing document is there
        let mut tx = db.begin().unwrap();
        let users = tx.collection("users").unwrap();
        let docs = users.find_all().unwrap();
        assert_eq!(docs.len(), 1, "Should have 1 document after reopen");
        drop(tx);

        // Now test unique constraint enforcement
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();
        let result = users.insert(json!({ "city": "NYC", "age": 30 }));

        assert!(result.is_err(), "Insert should have failed due to unique constraint");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unique constraint"),
            "Expected unique constraint error, got: {}", err_msg);

        db.close().unwrap();
    }

    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(format!("{}.lock", db_path));
    let _ = fs::remove_file(format!("{}-wal", db_path));
}

#[test]
fn test_compound_index_large_dataset() {
    let db = setup_test_db("large_dataset");

    // Insert many documents
    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();

    for i in 0..100 {
        users.insert(json!({
            "city": format!("City{}", i % 10),
            "age": 20 + (i % 50),
            "name": format!("User{}", i)
        })).unwrap();
    }

    tx.commit().unwrap();

    // Create compound index on existing data
    let result = db.create_compound_index("users", "city_age_idx", &["city", "age"], false);
    assert!(result.is_ok());

    // Verify we can still insert new documents
    let mut tx = db.begin().unwrap();
    let mut users = tx.collection("users").unwrap();
    let result = users.insert(json!({
        "city": "NewCity",
        "age": 40,
        "name": "NewUser"
    }));
    assert!(result.is_ok());

    cleanup_test_db("large_dataset");
}

#[test]
fn test_compound_index_with_complex_values() {
    let db = setup_test_db("complex_values");

    let mut tx = db.begin().unwrap();
    let mut docs = tx.collection("docs").unwrap();

    docs.insert(json!({
        "tags": ["rust", "database"],
        "metadata": {"version": 1},
        "status": "active"
    })).unwrap();

    tx.commit().unwrap();

    // Arrays and objects can be indexed
    let result = db.create_compound_index("docs", "tags_meta_idx",
        &["tags", "metadata"], false);
    assert!(result.is_ok());

    cleanup_test_db("complex_values");
}

#[test]
fn test_multiple_compound_indexes_same_collection() {
    let db = setup_test_db("multiple_indexes");

    let mut tx = db.begin().unwrap();
    let mut products = tx.collection("products").unwrap();

    products.insert(json!({
        "category": "electronics",
        "brand": "Apple",
        "price": 999,
        "rating": 4.5
    })).unwrap();

    tx.commit().unwrap();

    // Create multiple compound indexes
    let result1 = db.create_compound_index("products", "cat_brand_idx",
        &["category", "brand"], false);
    assert!(result1.is_ok());

    let result2 = db.create_compound_index("products", "cat_price_idx",
        &["category", "price"], false);
    assert!(result2.is_ok());

    let result3 = db.create_compound_index("products", "brand_rating_idx",
        &["brand", "rating"], false);
    assert!(result3.is_ok());

    cleanup_test_db("multiple_indexes");
}
