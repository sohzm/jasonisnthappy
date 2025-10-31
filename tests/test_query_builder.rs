use jasonisnthappy::{Database, SortOrder};
use serde_json::json;
use std::fs;

#[test]
fn test_sorting_limiting_pagination_integration() {
    let path = "/tmp/test_query_integration.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let users = db.collection("users");

    // Insert test data
    for i in 1..=50 {
        users.insert(json!({
            "name": format!("User{}", i),
            "age": 20 + (i % 40),
            "city": if i % 3 == 0 { "NYC" } else if i % 3 == 1 { "LA" } else { "SF" },
            "score": i * 10,
        })).unwrap();
    }

    // Test 1: Simple sort
    let results = users.query()
        .sort_by("age", SortOrder::Asc)
        .limit(5)
        .execute()
        .unwrap();
    assert_eq!(results.len(), 5);
    assert_eq!(results[0]["age"], 20);

    // Test 2: Filter + Sort + Limit
    let results = users.query()
        .filter("city is \"NYC\"")
        .sort_by("score", SortOrder::Desc)
        .limit(3)
        .execute()
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0]["city"], "NYC");
    // Scores should be in descending order
    assert!(results[0]["score"].as_i64().unwrap() > results[1]["score"].as_i64().unwrap());

    // Test 3: Pagination
    let page1 = users.query()
        .sort_by("name", SortOrder::Asc)
        .limit(10)
        .skip(0)
        .execute()
        .unwrap();
    let page2 = users.query()
        .sort_by("name", SortOrder::Asc)
        .limit(10)
        .skip(10)
        .execute()
        .unwrap();
    assert_eq!(page1.len(), 10);
    assert_eq!(page2.len(), 10);
    assert_ne!(page1[0]["name"], page2[0]["name"]);

    // Test 4: Multi-field sort
    let results = users.query()
        .sort_by("city", SortOrder::Asc)
        .sort_by("age", SortOrder::Desc)
        .limit(20)
        .execute()
        .unwrap();
    assert_eq!(results.len(), 20);

    // Verify cities are sorted
    let mut prev_city = "";
    for result in &results {
        let city = result["city"].as_str().unwrap();
        assert!(city >= prev_city);
        prev_city = city;
    }

    // Test 5: Count with filter
    let count = users.query()
        .filter("age > 30")
        .count()
        .unwrap();
    assert!(count > 0);

    // Test 6: First with filter
    let first = users.query()
        .filter("score > 300")
        .sort_by("score", SortOrder::Asc)
        .first()
        .unwrap();
    assert!(first.is_some());
    let doc = first.unwrap();
    assert!(doc["score"].as_i64().unwrap() > 300);

    // Test 7: Complex query
    let results = users.query()
        .filter("age > 25 and age < 45")
        .sort_by("score", SortOrder::Desc)
        .skip(5)
        .limit(10)
        .execute()
        .unwrap();
    assert!(results.len() <= 10);
    for result in &results {
        let age = result["age"].as_i64().unwrap();
        assert!(age > 25 && age < 45);
    }

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_nested_field_sorting() {
    let path = "/tmp/test_nested_sorting.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let users = db.collection("users");

    users.insert(json!({
        "name": "Alice",
        "profile": {
            "age": 30,
            "address": {
                "city": "NYC",
                "zip": "10001"
            }
        }
    })).unwrap();

    users.insert(json!({
        "name": "Bob",
        "profile": {
            "age": 25,
            "address": {
                "city": "LA",
                "zip": "90001"
            }
        }
    })).unwrap();

    users.insert(json!({
        "name": "Charlie",
        "profile": {
            "age": 35,
            "address": {
                "city": "SF",
                "zip": "94101"
            }
        }
    })).unwrap();

    // Sort by nested field
    let results = users.query()
        .sort_by("profile.age", SortOrder::Asc)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0]["name"], "Bob");
    assert_eq!(results[1]["name"], "Alice");
    assert_eq!(results[2]["name"], "Charlie");

    // Sort by deeply nested field
    let results = users.query()
        .sort_by("profile.address.city", SortOrder::Asc)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0]["profile"]["address"]["city"], "LA");
    assert_eq!(results[1]["profile"]["address"]["city"], "NYC");
    assert_eq!(results[2]["profile"]["address"]["city"], "SF");

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_empty_collection_queries() {
    let path = "/tmp/test_empty_queries.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let users = db.collection("users");

    // Query empty collection
    let results = users.query().execute().unwrap();
    assert_eq!(results.len(), 0);

    let count = users.query().count().unwrap();
    assert_eq!(count, 0);

    let first = users.query().first().unwrap();
    assert!(first.is_none());

    // With sorting
    let results = users.query()
        .sort_by("age", SortOrder::Asc)
        .limit(10)
        .execute()
        .unwrap();
    assert_eq!(results.len(), 0);

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}
#[test]
fn test_projections_integration() {
    let path = "/tmp/test_projections_integration.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let users = db.collection("users");

    // Insert documents with sensitive data
    for i in 1..=10 {
        users.insert(json!({
            "name": format!("User{}", i),
            "email": format!("user{}@example.com", i),
            "password_hash": format!("hash_{}", i),
            "age": 20 + i,
            "profile": {
                "bio": format!("Bio for user {}", i),
                "avatar": format!("avatar{}.jpg", i),
                "private_notes": format!("Secret notes {}", i)
            }
        })).unwrap();
    }

    // Test 1: Include projection - only return public fields
    let results = users.query()
        .project(&["name", "email", "age"])
        .limit(5)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 5);
    for doc in &results {
        assert!(doc.get("name").is_some());
        assert!(doc.get("email").is_some());
        assert!(doc.get("age").is_some());
        // Sensitive fields should not be present
        assert!(doc.get("password_hash").is_none());
        assert!(doc.get("profile").is_none());
    }

    // Test 2: Nested field projection
    let results = users.query()
        .project(&["name", "profile.bio", "profile.avatar"])
        .limit(3)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 3);
    for doc in &results {
        assert!(doc.get("name").is_some());
        assert!(doc["profile"].get("bio").is_some());
        assert!(doc["profile"].get("avatar").is_some());
        // private_notes should not be present
        assert!(doc["profile"].get("private_notes").is_none());
    }

    // Test 3: Exclude projection - hide sensitive fields
    let results = users.query()
        .exclude(&["password_hash", "profile.private_notes"])
        .limit(3)
        .execute()
        .unwrap();

    assert_eq!(results.len(), 3);
    for doc in &results {
        assert!(doc.get("name").is_some());
        assert!(doc.get("email").is_some());
        assert!(doc.get("password_hash").is_none());
        assert!(doc["profile"].get("private_notes").is_none());
        assert!(doc["profile"].get("bio").is_some());
    }

    // Test 4: Projection with filter and sort
    let results = users.query()
        .filter("age > 25")
        .sort_by("age", SortOrder::Desc)
        .project(&["name", "age"])
        .limit(5)
        .execute()
        .unwrap();

    assert!(results.len() <= 5);
    for doc in &results {
        assert!(doc.get("name").is_some());
        assert!(doc.get("age").is_some());
        assert!(doc["age"].as_i64().unwrap() > 25);
        assert!(doc.get("email").is_none());
    }

    // Test 5: Exclude _id
    let results = users.query()
        .exclude(&["_id", "password_hash"])
        .first()
        .unwrap();

    assert!(results.is_some());
    let doc = results.unwrap();
    assert!(doc.get("_id").is_none());
    assert!(doc.get("name").is_some());
    assert!(doc.get("password_hash").is_none());

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_upsert_integration() {
    let path = "/tmp/test_upsert_integration.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    let sessions = db.collection("sessions");
    let metrics = db.collection("metrics");

    // Test 1: Session management with upsert_by_id
    let session_id = "sess_abc123";
    
    // First access - insert
    let result = sessions.upsert_by_id(session_id, json!({
        "user_id": "user_1",
        "ip": "192.168.1.1",
        "created_at": "2024-01-01T10:00:00Z",
        "last_accessed": "2024-01-01T10:00:00Z"
    })).unwrap();
    
    assert!(matches!(result, jasonisnthappy::UpsertResult::Inserted(_)));
    
    // Second access - update
    let result = sessions.upsert_by_id(session_id, json!({
        "user_id": "user_1",
        "ip": "192.168.1.1",
        "created_at": "2024-01-01T10:00:00Z",
        "last_accessed": "2024-01-01T10:30:00Z",
        "page_views": 5
    })).unwrap();
    
    assert!(matches!(result, jasonisnthappy::UpsertResult::Updated(_)));
    
    // Verify only one session exists
    let count = sessions.count().unwrap();
    assert_eq!(count, 1);
    
    let session = sessions.find_by_id(session_id).unwrap();
    assert_eq!(session["page_views"], 5);
    assert_eq!(session["last_accessed"], "2024-01-01T10:30:00Z");

    // Test 2: Metrics tracking with upsert by query
    for day in 1..=10 {
        let date = format!("2024-01-{:02}", day);
        
        // Simulate multiple events per day
        for _ in 0..5 {
            metrics.upsert(&format!("date is \"{}\"", date), json!({
                "date": date.clone(),
                "event_type": "page_view",
                "count": 1
            })).unwrap();
        }
    }
    
    // Should only have 10 documents (one per day), not 50
    let count = metrics.count().unwrap();
    assert_eq!(count, 10);
    
    // Test 3: User profile with upsert by email query
    let users = db.collection("users");
    
    let email = "alice@example.com";
    
    // First login - create profile
    users.upsert(&format!("email is \"{}\"", email), json!({
        "name": "Alice",
        "email": email,
        "login_count": 1,
        "last_login": "2024-01-01T10:00:00Z"
    })).unwrap();
    
    // Second login - update profile
    users.upsert(&format!("email is \"{}\"", email), json!({
        "name": "Alice Smith",
        "email": email,
        "login_count": 2,
        "last_login": "2024-01-02T14:00:00Z",
        "preferences": {"theme": "dark"}
    })).unwrap();
    
    // Verify only one user profile
    let user_count = users.count().unwrap();
    assert_eq!(user_count, 1);
    
    let user = users.find_one(&format!("email is \"{}\"", email)).unwrap().unwrap();
    assert_eq!(user["name"], "Alice Smith");
    assert_eq!(user["login_count"], 2);
    assert_eq!(user["preferences"]["theme"], "dark");
    
    // Test 4: Idempotent API requests
    let api_requests = db.collection("api_requests");
    let idempotency_key = "req_xyz789";
    
    // Same request made multiple times (network retry)
    for _ in 0..3 {
        api_requests.upsert_by_id(idempotency_key, json!({
            "request_id": idempotency_key,
            "endpoint": "/api/charge",
            "amount": 100,
            "status": "completed",
            "timestamp": "2024-01-01T10:00:00Z"
        })).unwrap();
    }
    
    // Should only process once
    let count = api_requests.count().unwrap();
    assert_eq!(count, 1);

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_distinct_integration() {
    let path = "/tmp/test_distinct_integration.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Database::open(path).unwrap();
    
    // Test 1: Get distinct cities for filter UI
    let users = db.collection("users");
    
    for i in 1..=100 {
        users.insert(json!({
            "name": format!("User{}", i),
            "age": 20 + (i % 50),
            "city": match i % 5 {
                0 => "New York",
                1 => "Los Angeles",
                2 => "Chicago",
                3 => "Houston",
                _ => "Phoenix",
            },
            "country": "USA"
        })).unwrap();
    }
    
    // Get all distinct cities for a dropdown filter
    let cities = users.distinct("city").unwrap();
    assert_eq!(cities.len(), 5);
    
    let mut city_names: Vec<String> = cities.iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    city_names.sort();
    
    assert_eq!(city_names, vec!["Chicago", "Houston", "Los Angeles", "New York", "Phoenix"]);
    
    // Test 2: Distinct tags from blog posts (array field)
    let posts = db.collection("posts");
    
    posts.insert(json!({
        "title": "Getting Started with Rust",
        "tags": ["rust", "programming", "beginners"]
    })).unwrap();
    
    posts.insert(json!({
        "title": "Building a Database",
        "tags": ["rust", "database", "systems"]
    })).unwrap();
    
    posts.insert(json!({
        "title": "Web Development",
        "tags": ["web", "programming", "javascript"]
    })).unwrap();
    
    posts.insert(json!({
        "title": "Advanced Rust",
        "tags": ["rust", "advanced", "systems"]
    })).unwrap();
    
    // Get all unique tags across all posts
    let tags = posts.distinct("tags").unwrap();
    assert_eq!(tags.len(), 8); // All unique tags
    
    let mut tag_names: Vec<String> = tags.iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    tag_names.sort();
    
    assert_eq!(tag_names, vec![
        "advanced", "beginners", "database", "javascript", 
        "programming", "rust", "systems", "web"
    ]);
    
    // Test 3: Count distinct for analytics
    let events = db.collection("events");
    
    for i in 1..=1000 {
        events.insert(json!({
            "user_id": format!("user_{}", i % 50), // 50 unique users
            "event_type": if i % 3 == 0 { "click" } else { "view" },
            "timestamp": format!("2024-01-01T{:02}:00:00Z", i % 24)
        })).unwrap();
    }
    
    // How many unique users?
    let unique_users = events.count_distinct("user_id").unwrap();
    assert_eq!(unique_users, 50);
    
    // How many event types?
    let event_types = events.count_distinct("event_type").unwrap();
    assert_eq!(event_types, 2);
    
    // Test 4: Nested field distinct
    let products = db.collection("products");
    
    products.insert(json!({
        "name": "Laptop",
        "manufacturer": {"name": "Dell", "country": "USA"}
    })).unwrap();
    
    products.insert(json!({
        "name": "Phone",
        "manufacturer": {"name": "Apple", "country": "USA"}
    })).unwrap();
    
    products.insert(json!({
        "name": "Tablet",
        "manufacturer": {"name": "Samsung", "country": "Korea"}
    })).unwrap();
    
    products.insert(json!({
        "name": "Monitor",
        "manufacturer": {"name": "Dell", "country": "USA"}
    })).unwrap();
    
    // Get distinct manufacturer names
    let manufacturers = products.distinct("manufacturer.name").unwrap();
    assert_eq!(manufacturers.len(), 3);
    
    let mut mfr_names: Vec<String> = manufacturers.iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    mfr_names.sort();
    
    assert_eq!(mfr_names, vec!["Apple", "Dell", "Samsung"]);
    
    // Get distinct countries
    let countries = products.distinct("manufacturer.country").unwrap();
    assert_eq!(countries.len(), 2);

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}
