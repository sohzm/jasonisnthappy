use jasonisnthappy::Database;
use serde_json::json;
use std::fs;

#[test]
fn test_comprehensive_introspection() {
    let path = "/tmp/test_comprehensive_introspection.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();

    // Start with empty database
    let collections = db.list_collections().unwrap();
    assert_eq!(collections.len(), 0);

    let info = db.info().unwrap();
    assert_eq!(info.total_documents, 0);
    assert_eq!(info.collections.len(), 0);

    // Create multiple collections with documents
    let users = db.collection("users");
    for i in 0..20 {
        users.insert(json!({
            "name": format!("User{}", i),
            "age": 20 + (i % 50),
            "email": format!("user{}@example.com", i)
        })).unwrap();
    }

    let products = db.collection("products");
    for i in 0..15 {
        products.insert(json!({
            "name": format!("Product{}", i),
            "price": 10.0 + (i as f64),
            "category": if i % 2 == 0 { "electronics" } else { "books" }
        })).unwrap();
    }

    let orders = db.collection("orders");
    for i in 0..10 {
        orders.insert(json!({
            "order_id": i,
            "user_id": i % 20,
            "total": 100.0 + (i as f64 * 10.0)
        })).unwrap();
    }

    // Create indexes
    db.create_compound_index("users", "age_idx", &["age"], false).unwrap();
    db.create_compound_index("users", "email_idx", &["email"], false).unwrap();
    db.create_compound_index("products", "category_price_idx", &["category", "price"], false).unwrap();

    // Test list_collections
    let collections = db.list_collections().unwrap();
    assert_eq!(collections.len(), 3);
    assert_eq!(collections, vec!["orders", "products", "users"]);

    // Test collection_stats
    let users_stats = db.collection_stats("users").unwrap();
    assert_eq!(users_stats.name, "users");
    assert_eq!(users_stats.document_count, 20);
    assert_eq!(users_stats.indexes.len(), 2);

    let products_stats = db.collection_stats("products").unwrap();
    assert_eq!(products_stats.name, "products");
    assert_eq!(products_stats.document_count, 15);
    assert_eq!(products_stats.indexes.len(), 1);

    let orders_stats = db.collection_stats("orders").unwrap();
    assert_eq!(orders_stats.name, "orders");
    assert_eq!(orders_stats.document_count, 10);
    assert_eq!(orders_stats.indexes.len(), 0);

    // Test list_indexes
    let users_indexes = db.list_indexes("users").unwrap();
    assert_eq!(users_indexes.len(), 2);

    let user_idx_names: Vec<String> = users_indexes.iter().map(|idx| idx.name.clone()).collect();
    assert!(user_idx_names.contains(&"age_idx".to_string()));
    assert!(user_idx_names.contains(&"email_idx".to_string()));

    let products_indexes = db.list_indexes("products").unwrap();
    assert_eq!(products_indexes.len(), 1);
    assert_eq!(products_indexes[0].name, "category_price_idx");
    assert_eq!(products_indexes[0].fields, vec!["category", "price"]);

    // Test database info
    let info = db.info().unwrap();
    assert_eq!(info.path, path);
    assert_eq!(info.collections.len(), 3);
    assert_eq!(info.total_documents, 45); // 20 + 15 + 10
    assert!(!info.read_only);
    assert!(info.file_size > 0);
    assert!(info.num_pages > 0);

    // Verify collections in info are sorted
    assert_eq!(info.collections[0].name, "orders");
    assert_eq!(info.collections[1].name, "products");
    assert_eq!(info.collections[2].name, "users");

    // Verify each collection's info matches
    assert_eq!(info.collections[0].document_count, 10);
    assert_eq!(info.collections[1].document_count, 15);
    assert_eq!(info.collections[2].document_count, 20);

    db.close().unwrap();

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_introspection_after_modifications() {
    let path = "/tmp/test_introspection_modifications.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let users = db.collection("users");

    // Insert initial documents
    for i in 0..10 {
        users.insert(json!({"name": format!("User{}", i), "age": 20 + i})).unwrap();
    }

    let stats = db.collection_stats("users").unwrap();
    assert_eq!(stats.document_count, 10);

    // Delete some documents
    users.delete("age < 25").unwrap();

    let stats = db.collection_stats("users").unwrap();
    assert_eq!(stats.document_count, 5);

    // Add more documents
    for i in 10..15 {
        users.insert(json!({"name": format!("User{}", i), "age": 30 + i})).unwrap();
    }

    let stats = db.collection_stats("users").unwrap();
    assert_eq!(stats.document_count, 10); // 5 remaining + 5 new

    let info = db.info().unwrap();
    assert_eq!(info.total_documents, 10);

    db.close().unwrap();

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_introspection_empty_collections() {
    let path = "/tmp/test_introspection_empty.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();

    // Create collection but add no documents
    let users = db.collection("users");
    users.insert(json!({"name": "Alice"})).unwrap();

    // Now delete it
    users.delete("name is \"Alice\"").unwrap();

    let stats = db.collection_stats("users").unwrap();
    assert_eq!(stats.document_count, 0);
    assert!(stats.btree_root > 0); // Collection exists but is empty

    let info = db.info().unwrap();
    assert_eq!(info.total_documents, 0);
    assert_eq!(info.collections.len(), 1);

    db.close().unwrap();

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_introspection_with_bulk_operations() {
    let path = "/tmp/test_introspection_bulk.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let users = db.collection("users");

    // Use bulk operations to populate
    let mut bulk = users.bulk_write();
    for i in 0..50 {
        bulk = bulk.insert(json!({
            "name": format!("User{}", i),
            "age": 20 + (i % 60)
        }));
    }
    let result = bulk.execute().unwrap();
    assert_eq!(result.inserted_count, 50);

    // Check introspection
    let stats = db.collection_stats("users").unwrap();
    assert_eq!(stats.document_count, 50);

    let info = db.info().unwrap();
    assert_eq!(info.total_documents, 50);

    // Bulk delete
    users.bulk_write()
        .delete_many("age < 30")
        .execute()
        .unwrap();

    let stats = db.collection_stats("users").unwrap();
    assert!(stats.document_count < 50);

    db.close().unwrap();

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_introspection_persistence() {
    let path = "/tmp/test_introspection_persistence.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    // Create database and add data
    {
        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        for i in 0..10 {
            users.insert(json!({"name": format!("User{}", i)})).unwrap();
        }

        db.create_compound_index("users", "name_idx", &["name"], false).unwrap();

        let stats = db.collection_stats("users").unwrap();
        assert_eq!(stats.document_count, 10);
        assert_eq!(stats.indexes.len(), 1);

        db.close().unwrap();
    }

    // Reopen and verify introspection still works
    {
        let db = Database::open(path).unwrap();

        let collections = db.list_collections().unwrap();
        assert_eq!(collections.len(), 1);
        assert_eq!(collections[0], "users");

        let stats = db.collection_stats("users").unwrap();
        assert_eq!(stats.document_count, 10);
        assert_eq!(stats.indexes.len(), 1);

        let indexes = db.list_indexes("users").unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "name_idx");

        let info = db.info().unwrap();
        assert_eq!(info.total_documents, 10);

        db.close().unwrap();
    }

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}
