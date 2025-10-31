use jasonisnthappy::core::database::Database;
use jasonisnthappy::core::watch::ChangeOperation;
use serde_json::json;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_watch_insert_events() {
    let path = "/tmp/test_watch_insert.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());
    let coll = db.collection("users");

    // Subscribe to changes
    let (handle, rx) = coll.watch().subscribe().unwrap();

    // Insert documents
    coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
    coll.insert(json!({"name": "Bob", "age": 25})).unwrap();

    // Receive events
    let event1 = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(event1.collection, "users");
    assert_eq!(event1.operation, ChangeOperation::Insert);
    assert!(event1.document.is_some());
    let doc1 = event1.document.unwrap();
    assert_eq!(doc1["name"], "Alice");

    let event2 = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(event2.operation, ChangeOperation::Insert);
    assert_eq!(event2.document.unwrap()["name"], "Bob");

    drop(handle);
    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_watch_update_events() {
    let path = "/tmp/test_watch_update.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());
    let coll = db.collection("users");

    // Insert a document first
    let id = coll.insert(json!({"name": "Alice", "age": 30})).unwrap();

    // Subscribe to changes AFTER inserting
    let (_handle, rx) = coll.watch().subscribe().unwrap();

    // Update the document using a transaction (this properly tracks insert vs update)
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();
        users.update_by_id(&id, json!({"age": 31})).unwrap();
        tx.commit().unwrap();
    }

    // Receive update event
    let event = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(event.operation, ChangeOperation::Update);
    assert_eq!(event.doc_id, id);
    let doc = event.document.unwrap();
    assert_eq!(doc["age"], 31);

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_watch_with_filter() {
    let path = "/tmp/test_watch_filter.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());
    let coll = db.collection("users");

    // Subscribe with filter
    let (_handle, rx) = coll.watch()
        .filter("age > 28")
        .subscribe()
        .unwrap();

    // Insert documents - only one should match filter
    coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
    coll.insert(json!({"name": "Bob", "age": 25})).unwrap();
    coll.insert(json!({"name": "Charlie", "age": 35})).unwrap();

    // Should receive only 2 events (Alice and Charlie)
    let event1 = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(event1.document.as_ref().unwrap()["age"].as_i64().unwrap() > 28);

    let event2 = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(event2.document.as_ref().unwrap()["age"].as_i64().unwrap() > 28);

    // Bob's event (age 25) should not be received
    let result = rx.recv_timeout(Duration::from_millis(100));
    assert!(result.is_err()); // Timeout - no more events

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_watch_multiple_subscribers() {
    let path = "/tmp/test_watch_multiple.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());
    let coll = db.collection("users");

    // Create two subscribers
    let (_handle1, rx1) = coll.watch().subscribe().unwrap();
    let (_handle2, rx2) = coll.watch().subscribe().unwrap();

    // Insert a document
    coll.insert(json!({"name": "Alice", "age": 30})).unwrap();

    // Both should receive the event
    let event1 = rx1.recv_timeout(Duration::from_secs(1)).unwrap();
    let event2 = rx2.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(event1.doc_id, event2.doc_id);
    assert_eq!(event1.operation, ChangeOperation::Insert);
    assert_eq!(event2.operation, ChangeOperation::Insert);

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_watch_handle_drop_unsubscribes() {
    let path = "/tmp/test_watch_drop.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());
    let coll = db.collection("users");

    let (handle, rx) = coll.watch().subscribe().unwrap();

    // Insert a document - should receive event
    coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
    let event = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(event.operation, ChangeOperation::Insert);

    // Drop the handle
    drop(handle);

    // Insert another document - should NOT receive event (channel closed or watcher removed)
    coll.insert(json!({"name": "Bob", "age": 25})).unwrap();

    // The channel should be closed or we timeout
    let result = rx.recv_timeout(Duration::from_millis(100));
    // Either channel is closed (Err with Disconnected) or timeout
    assert!(result.is_err());

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_watch_with_transactions() {
    let path = "/tmp/test_watch_transactions.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());
    let coll = db.collection("users");

    let (_handle, rx) = coll.watch().subscribe().unwrap();

    // Use transactions
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();

        users.insert(json!({"name": "Alice", "age": 30})).unwrap();
        users.insert(json!({"name": "Bob", "age": 25})).unwrap();

        tx.commit().unwrap();
    }

    // Should receive events after commit
    let event1 = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let event2 = rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert_eq!(event1.operation, ChangeOperation::Insert);
    assert_eq!(event2.operation, ChangeOperation::Insert);

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_watch_rollback_no_events() {
    let path = "/tmp/test_watch_rollback.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());
    let coll = db.collection("users");

    let (_handle, rx) = coll.watch().subscribe().unwrap();

    // Use transaction and rollback
    {
        let mut tx = db.begin().unwrap();
        let mut users = tx.collection("users").unwrap();

        users.insert(json!({"name": "Alice", "age": 30})).unwrap();

        tx.rollback().unwrap();
    }

    // Should NOT receive events (transaction was rolled back)
    let result = rx.recv_timeout(Duration::from_millis(100));
    assert!(result.is_err()); // Timeout - no events

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}

#[test]
fn test_watch_different_collections() {
    let path = "/tmp/test_watch_collections.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    let db = Arc::new(Database::open(path).unwrap());
    let users_coll = db.collection("users");
    let posts_coll = db.collection("posts");

    // Watch users
    let (_handle_users, rx_users) = users_coll.watch().subscribe().unwrap();

    // Watch posts
    let (_handle_posts, rx_posts) = posts_coll.watch().subscribe().unwrap();

    // Insert into users
    users_coll.insert(json!({"name": "Alice"})).unwrap();

    // Insert into posts
    posts_coll.insert(json!({"title": "Hello"})).unwrap();

    // Users watcher should receive users event
    let event_users = rx_users.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(event_users.collection, "users");

    // Posts watcher should receive posts event
    let event_posts = rx_posts.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(event_posts.collection, "posts");

    db.close().unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));
}
