use jasonisnthappy::Database;
use serde_json::json;
use std::fs;

#[test]
fn test_aggregation_comprehensive_analytics() {
    let path = "/tmp/test_aggregation_comprehensive.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let sales = db.collection("sales");

    // Insert sample sales data
    sales.insert(json!({
        "product": "Laptop",
        "category": "Electronics",
        "price": 1200.0,
        "quantity": 5,
        "region": "North"
    })).unwrap();

    sales.insert(json!({
        "product": "Mouse",
        "category": "Electronics",
        "price": 25.0,
        "quantity": 50,
        "region": "North"
    })).unwrap();

    sales.insert(json!({
        "product": "Desk",
        "category": "Furniture",
        "price": 350.0,
        "quantity": 10,
        "region": "South"
    })).unwrap();

    sales.insert(json!({
        "product": "Chair",
        "category": "Furniture",
        "price": 150.0,
        "quantity": 20,
        "region": "North"
    })).unwrap();

    sales.insert(json!({
        "product": "Monitor",
        "category": "Electronics",
        "price": 400.0,
        "quantity": 8,
        "region": "South"
    })).unwrap();

    sales.insert(json!({
        "product": "Keyboard",
        "category": "Electronics",
        "price": 75.0,
        "quantity": 30,
        "region": "East"
    })).unwrap();

    // Test 1: Group by category with multiple aggregations
    let category_analysis = sales.aggregate()
        .group_by("category")
        .count("total_products")
        .sum("price", "total_price")
        .avg("price", "avg_price")
        .min("price", "min_price")
        .max("price", "max_price")
        .sort("total_price", false) // descending
        .execute()
        .unwrap();

    assert_eq!(category_analysis.len(), 2);

    // Electronics should be first (higher total price)
    let electronics = &category_analysis[0];
    assert_eq!(electronics["_id"], "Electronics");
    assert_eq!(electronics["total_products"], 4);
    assert_eq!(electronics["total_price"], 1700.0);
    assert_eq!(electronics["min_price"], 25.0);
    assert_eq!(electronics["max_price"], 1200.0);

    let furniture = &category_analysis[1];
    assert_eq!(furniture["_id"], "Furniture");
    assert_eq!(furniture["total_products"], 2);
    assert_eq!(furniture["total_price"], 500.0);

    // Test 2: Filter then group - High value items by region
    let high_value_regions = sales.aggregate()
        .match_("price > 100")
        .group_by("region")
        .count("num_items")
        .sum("price", "total_value")
        .execute()
        .unwrap();

    assert_eq!(high_value_regions.len(), 2); // North and South (East has only keyboard at 75)

    let north = high_value_regions.iter()
        .find(|r| r["_id"] == "North")
        .unwrap();
    assert_eq!(north["num_items"], 2); // Laptop and Chair
    assert_eq!(north["total_value"], 1350.0);

    // Test 3: Complex multi-stage pipeline with projection
    let complex_pipeline = sales.aggregate()
        .match_("category is \"Electronics\"")
        .sort("price", false)
        .limit(3)
        .project(&["product", "price"])
        .execute()
        .unwrap();

    assert_eq!(complex_pipeline.len(), 3);
    assert_eq!(complex_pipeline[0]["product"], "Laptop");
    assert_eq!(complex_pipeline[1]["product"], "Monitor");
    assert_eq!(complex_pipeline[2]["product"], "Keyboard");
    // Verify other fields are excluded
    assert!(complex_pipeline[0].get("category").is_none());
    assert!(complex_pipeline[0].get("_id").is_some()); // _id always included

    // Test 4: Skip and limit for pagination
    let paginated = sales.aggregate()
        .sort("price", true) // ascending
        .skip(1)
        .limit(2)
        .execute()
        .unwrap();

    assert_eq!(paginated.len(), 2);
    assert_eq!(paginated[0]["product"], "Keyboard");
    assert_eq!(paginated[1]["product"], "Chair");

    // Test 5: Group by then filter results
    let category_counts = sales.aggregate()
        .group_by("category")
        .count("count")
        .execute()
        .unwrap();

    // Verify we get both categories
    assert_eq!(category_counts.len(), 2);
    let electronics_count = category_counts.iter()
        .find(|r| r["_id"] == "Electronics")
        .unwrap();
    assert_eq!(electronics_count["count"], 4);

    // Test 6: Exclude fields instead of project
    let without_sensitive = sales.aggregate()
        .match_("product is \"Laptop\"")
        .exclude(&["quantity", "region"])
        .execute()
        .unwrap();

    assert_eq!(without_sensitive.len(), 1);
    assert!(without_sensitive[0].get("product").is_some());
    assert!(without_sensitive[0].get("price").is_some());
    assert!(without_sensitive[0].get("quantity").is_none());
    assert!(without_sensitive[0].get("region").is_none());

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_aggregation_empty_collection() {
    let path = "/tmp/test_aggregation_empty.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let empty = db.collection("empty");

    let results = empty.aggregate()
        .group_by("field")
        .count("total")
        .execute()
        .unwrap();

    assert_eq!(results.len(), 0);

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_aggregation_single_document() {
    let path = "/tmp/test_aggregation_single.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let coll = db.collection("single");

    coll.insert(json!({"value": 42})).unwrap();

    let results = coll.aggregate()
        .group_by("value")
        .count("count")
        .execute()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["count"], 1);

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_aggregation_null_and_missing_values() {
    let path = "/tmp/test_aggregation_nulls.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let coll = db.collection("nulls");

    coll.insert(json!({"category": "A", "value": 10})).unwrap();
    coll.insert(json!({"category": "A", "value": null})).unwrap();
    coll.insert(json!({"category": "A"})).unwrap(); // missing value field
    coll.insert(json!({"value": 20})).unwrap(); // missing category field

    let results = coll.aggregate()
        .group_by("category")
        .count("count")
        .sum("value", "total")
        .execute()
        .unwrap();

    // Should have 2 groups: "A" and "null" (for missing category)
    assert_eq!(results.len(), 2);

    let group_a = results.iter().find(|r| r["_id"] == "A").unwrap();
    assert_eq!(group_a["count"], 3);
    assert_eq!(group_a["total"], 10.0); // Only the first doc's value counts

    let group_null = results.iter().find(|r| r["_id"] == "null").unwrap();
    assert_eq!(group_null["count"], 1);
    assert_eq!(group_null["total"], 20.0);

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_aggregation_real_world_scenario() {
    // Simulate a real e-commerce analytics scenario
    let path = "/tmp/test_aggregation_ecommerce.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let orders = db.collection("orders");

    // Insert order data
    orders.insert(json!({
        "customer": "alice@example.com",
        "product": "Laptop",
        "amount": 1200.0,
        "status": "completed",
        "rating": 5
    })).unwrap();

    orders.insert(json!({
        "customer": "bob@example.com",
        "product": "Mouse",
        "amount": 25.0,
        "status": "completed",
        "rating": 4
    })).unwrap();

    orders.insert(json!({
        "customer": "alice@example.com",
        "product": "Keyboard",
        "amount": 75.0,
        "status": "completed",
        "rating": 5
    })).unwrap();

    orders.insert(json!({
        "customer": "charlie@example.com",
        "product": "Monitor",
        "amount": 400.0,
        "status": "pending",
        "rating": null
    })).unwrap();

    orders.insert(json!({
        "customer": "bob@example.com",
        "product": "Chair",
        "amount": 150.0,
        "status": "completed",
        "rating": 3
    })).unwrap();

    // Query: Find top spending customers (completed orders only)
    let top_customers = orders.aggregate()
        .match_("status is \"completed\"")
        .group_by("customer")
        .sum("amount", "total_spent")
        .count("order_count")
        .avg("rating", "avg_rating")
        .sort("total_spent", false)
        .limit(5)
        .execute()
        .unwrap();

    assert_eq!(top_customers.len(), 2); // alice and bob (charlie's order is pending, so filtered out)

    // Alice should be top customer
    let alice = &top_customers[0];
    assert_eq!(alice["_id"], "alice@example.com");
    assert_eq!(alice["total_spent"], 1275.0);
    assert_eq!(alice["order_count"], 2);
    assert_eq!(alice["avg_rating"], 5.0);

    // Bob should be second
    let bob = &top_customers[1];
    assert_eq!(bob["_id"], "bob@example.com");
    assert_eq!(bob["total_spent"], 175.0);
    assert_eq!(bob["order_count"], 2);
    assert_eq!(bob["avg_rating"], 3.5); // average of 4 and 3

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}
