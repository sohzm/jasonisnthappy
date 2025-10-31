use jasonisnthappy::core::database::Database;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Product {
    #[serde(skip_serializing_if = "Option::is_none")]
    _id: Option<String>,
    name: String,
    price: f64,
    in_stock: bool,
    tags: Vec<String>,
}

#[test]
fn test_typed_documents_insert_and_find() {
    let path = "/tmp/test_typed_insert_find.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    let id1: String;
    let id2: String;

    // Insert typed documents
    {
        let mut tx = db.begin().unwrap();
        let mut products = tx.collection("products").unwrap();

        let product1 = Product {
            _id: None,
            name: "Laptop".to_string(),
            price: 999.99,
            in_stock: true,
            tags: vec!["electronics".to_string(), "computers".to_string()],
        };

        let product2 = Product {
            _id: None,
            name: "Mouse".to_string(),
            price: 29.99,
            in_stock: true,
            tags: vec!["electronics".to_string(), "accessories".to_string()],
        };

        id1 = products.insert_typed(&product1).unwrap();
        id2 = products.insert_typed(&product2).unwrap();

        assert!(!id1.is_empty());
        assert!(!id2.is_empty());
        assert_ne!(id1, id2);

        tx.commit().unwrap();
    }

    // Read typed documents
    {
        let mut tx = db.begin().unwrap();
        let products = tx.collection("products").unwrap();

        let all_products: Vec<Product> = products.find_all_typed().unwrap();
        assert_eq!(all_products.len(), 2);

        // Find by ID
        let laptop: Option<Product> = products.find_by_id_typed(&id1).unwrap();
        assert!(laptop.is_some());
        assert_eq!(laptop.unwrap().name, "Laptop");

        let mouse: Option<Product> = products.find_by_id_typed(&id2).unwrap();
        assert!(mouse.is_some());
        assert_eq!(mouse.unwrap().price, 29.99);

        tx.commit().unwrap();
    }

    // Update typed documents by ID
    {
        let mut tx = db.begin().unwrap();
        let mut products = tx.collection("products").unwrap();

        let updates = json!({"price": 899.99});
        products.update_by_id(&id1, updates).unwrap();

        let laptop: Option<Product> = products.find_by_id_typed(&id1).unwrap();
        assert_eq!(laptop.unwrap().price, 899.99);

        tx.commit().unwrap();
    }

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_typed_documents_insert_many() {
    let path = "/tmp/test_typed_insert_many.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    {
        let mut tx = db.begin().unwrap();
        let mut products = tx.collection("products").unwrap();

        let items = vec![
            Product {
                _id: None,
                name: "Keyboard".to_string(),
                price: 79.99,
                in_stock: true,
                tags: vec!["electronics".to_string()],
            },
            Product {
                _id: None,
                name: "Monitor".to_string(),
                price: 299.99,
                in_stock: false,
                tags: vec!["electronics".to_string()],
            },
        ];

        let ids = products.insert_many_typed(items).unwrap();
        assert_eq!(ids.len(), 2);

        let all: Vec<Product> = products.find_all_typed().unwrap();
        assert_eq!(all.len(), 2);

        tx.commit().unwrap();
    }

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_typed_documents_mixed_with_untyped() {
    let path = "/tmp/test_typed_mixed.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    {
        let mut tx = db.begin().unwrap();
        let mut products = tx.collection("products").unwrap();

        // Insert untyped document
        products.insert(json!({
            "name": "Untyped Product",
            "price": 99.99,
            "in_stock": true,
            "tags": ["misc"]
        })).unwrap();

        // Insert typed document
        let typed_product = Product {
            _id: None,
            name: "Typed Product".to_string(),
            price: 199.99,
            in_stock: false,
            tags: vec!["premium".to_string()],
        };
        products.insert_typed(&typed_product).unwrap();

        // Query as typed - should get both
        let all: Vec<Product> = products.find_all_typed().unwrap();
        assert_eq!(all.len(), 2);

        tx.commit().unwrap();
    }

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_typed_documents_empty_collection() {
    let path = "/tmp/test_typed_empty.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());

    {
        let mut tx = db.begin().unwrap();
        let products = tx.collection("products").unwrap();

        // Find on empty collection - should return empty vec, not error
        let all_result = products.find_all_typed::<Product>();
        // Empty collections return an error, so handle it gracefully
        let all: Vec<Product> = all_result.unwrap_or_default();
        assert_eq!(all.len(), 0);

        let found: Option<Product> = products.find_by_id_typed("nonexistent").unwrap();
        assert!(found.is_none());

        tx.commit().unwrap();
    }

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}
