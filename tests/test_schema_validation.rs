use jasonisnthappy::{Database, Schema, ValueType};
use serde_json::json;
use std::collections::HashMap;
use std::fs;

#[test]
fn test_schema_set_get_remove() {
    let path = "/tmp/test_schema_set_get.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();

    // Initially, no schema
    assert!(db.get_schema("users").is_none());

    // Set a schema
    let mut schema = Schema::new();
    schema.value_type = Some(ValueType::Object);
    schema.required = Some(vec!["name".to_string()]);

    db.set_schema("users", schema.clone()).unwrap();

    // Get the schema
    let retrieved = db.get_schema("users").unwrap();
    assert_eq!(retrieved.required, Some(vec!["name".to_string()]));

    // Remove the schema
    db.remove_schema("users").unwrap();
    assert!(db.get_schema("users").is_none());

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_schema_enforced_on_insert() {
    let path = "/tmp/test_schema_insert.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let users = db.collection("users");

    // Set schema requiring name and email
    let mut schema = Schema::new();
    schema.value_type = Some(ValueType::Object);
    schema.required = Some(vec!["name".to_string(), "email".to_string()]);

    let mut properties = HashMap::new();

    let mut name_schema = Schema::new();
    name_schema.value_type = Some(ValueType::String);
    name_schema.min_length = Some(1);
    properties.insert("name".to_string(), name_schema);

    let mut email_schema = Schema::new();
    email_schema.value_type = Some(ValueType::String);
    properties.insert("email".to_string(), email_schema);

    schema.properties = Some(properties);
    db.set_schema("users", schema).unwrap();

    // Valid insert should succeed
    let result = users.insert(json!({
        "name": "Alice",
        "email": "alice@example.com"
    }));
    assert!(result.is_ok());

    // Missing required field should fail
    let result = users.insert(json!({
        "name": "Bob"
    }));
    assert!(result.is_err());

    // Wrong type should fail
    let result = users.insert(json!({
        "name": 123,
        "email": "invalid@example.com"
    }));
    assert!(result.is_err());

    // Empty name should fail (violates min_length)
    let result = users.insert(json!({
        "name": "",
        "email": "test@example.com"
    }));
    assert!(result.is_err());

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_schema_enforced_on_update() {
    let path = "/tmp/test_schema_update.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let users = db.collection("users");

    // Insert without schema
    let id = users.insert(json!({
        "name": "Alice",
        "age": 30
    })).unwrap();

    // Set schema requiring age to be between 0 and 120
    let mut schema = Schema::new();
    schema.value_type = Some(ValueType::Object);

    let mut properties = HashMap::new();
    let mut age_schema = Schema::new();
    age_schema.value_type = Some(ValueType::Number);
    age_schema.minimum = Some(0.0);
    age_schema.maximum = Some(120.0);
    properties.insert("age".to_string(), age_schema);

    schema.properties = Some(properties);
    db.set_schema("users", schema).unwrap();

    // Valid update should succeed
    let result = users.update_by_id(&id, json!({"age": 31}));
    assert!(result.is_ok());

    // Invalid update should fail
    let result = users.update_by_id(&id, json!({"age": 150}));
    assert!(result.is_err());

    let result = users.update_by_id(&id, json!({"age": -5}));
    assert!(result.is_err());

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_schema_with_nested_objects() {
    let path = "/tmp/test_schema_nested.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let users = db.collection("users");

    // Set schema with nested address object
    let mut schema = Schema::new();
    schema.value_type = Some(ValueType::Object);

    let mut address_schema = Schema::new();
    address_schema.value_type = Some(ValueType::Object);
    address_schema.required = Some(vec!["city".to_string(), "zip".to_string()]);

    let mut address_props = HashMap::new();

    let mut city_schema = Schema::new();
    city_schema.value_type = Some(ValueType::String);
    address_props.insert("city".to_string(), city_schema);

    let mut zip_schema = Schema::new();
    zip_schema.value_type = Some(ValueType::String);
    zip_schema.min_length = Some(5);
    zip_schema.max_length = Some(10);
    address_props.insert("zip".to_string(), zip_schema);

    address_schema.properties = Some(address_props);

    let mut properties = HashMap::new();
    properties.insert("address".to_string(), address_schema);
    schema.properties = Some(properties);

    db.set_schema("users", schema).unwrap();

    // Valid nested document
    let result = users.insert(json!({
        "name": "Alice",
        "address": {
            "city": "NYC",
            "zip": "10001"
        }
    }));
    assert!(result.is_ok());

    // Missing nested required field
    let result = users.insert(json!({
        "name": "Bob",
        "address": {
            "city": "LA"
        }
    }));
    assert!(result.is_err());

    // Invalid nested field type
    let result = users.insert(json!({
        "name": "Charlie",
        "address": {
            "city": 12345,
            "zip": "90001"
        }
    }));
    assert!(result.is_err());

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_schema_with_arrays() {
    let path = "/tmp/test_schema_arrays.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let posts = db.collection("posts");

    // Set schema requiring tags array
    let mut schema = Schema::new();
    schema.value_type = Some(ValueType::Object);

    let mut properties = HashMap::new();

    let mut tags_schema = Schema::new();
    tags_schema.value_type = Some(ValueType::Array);
    tags_schema.min_length = Some(1);
    tags_schema.max_length = Some(5);

    let mut tag_item_schema = Schema::new();
    tag_item_schema.value_type = Some(ValueType::String);
    tags_schema.items = Some(Box::new(tag_item_schema));

    properties.insert("tags".to_string(), tags_schema);
    schema.properties = Some(properties);

    db.set_schema("posts", schema).unwrap();

    // Valid array
    let result = posts.insert(json!({
        "title": "Hello",
        "tags": ["rust", "database"]
    }));
    assert!(result.is_ok());

    // Empty array (violates min_length)
    let result = posts.insert(json!({
        "title": "World",
        "tags": []
    }));
    assert!(result.is_err());

    // Too many tags
    let result = posts.insert(json!({
        "title": "Test",
        "tags": ["a", "b", "c", "d", "e", "f"]
    }));
    assert!(result.is_err());

    // Wrong item type
    let result = posts.insert(json!({
        "title": "Test",
        "tags": ["valid", 123]
    }));
    assert!(result.is_err());

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_schema_with_enums() {
    let path = "/tmp/test_schema_enums.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let tasks = db.collection("tasks");

    // Set schema with enum for status
    let mut schema = Schema::new();
    schema.value_type = Some(ValueType::Object);

    let mut properties = HashMap::new();

    let mut status_schema = Schema::new();
    status_schema.enum_values = Some(vec![
        json!("pending"),
        json!("in_progress"),
        json!("completed")
    ]);
    properties.insert("status".to_string(), status_schema);

    schema.properties = Some(properties);
    db.set_schema("tasks", schema).unwrap();

    // Valid enum value
    let result = tasks.insert(json!({
        "title": "Task 1",
        "status": "pending"
    }));
    assert!(result.is_ok());

    // Invalid enum value
    let result = tasks.insert(json!({
        "title": "Task 2",
        "status": "invalid"
    }));
    assert!(result.is_err());

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_schema_persistence() {
    let path = "/tmp/test_schema_persistence.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    {
        let db = Database::open(path).unwrap();

        let mut schema = Schema::new();
        schema.value_type = Some(ValueType::Object);
        schema.required = Some(vec!["name".to_string()]);

        db.set_schema("users", schema).unwrap();
    }

    // Reopen database
    {
        let db = Database::open(path).unwrap();
        let retrieved = db.get_schema("users").unwrap();

        assert_eq!(retrieved.required, Some(vec!["name".to_string()]));
    }

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_integer_type_validation() {
    let path = "/tmp/test_schema_integer.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let coll = db.collection("numbers");

    let mut schema = Schema::new();
    schema.value_type = Some(ValueType::Object);

    let mut properties = HashMap::new();
    let mut count_schema = Schema::new();
    count_schema.value_type = Some(ValueType::Integer);
    properties.insert("count".to_string(), count_schema);

    schema.properties = Some(properties);
    db.set_schema("numbers", schema).unwrap();

    // Integer should pass
    assert!(coll.insert(json!({"count": 42})).is_ok());
    assert!(coll.insert(json!({"count": -10})).is_ok());

    // Float should fail
    assert!(coll.insert(json!({"count": 3.14})).is_err());

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}
