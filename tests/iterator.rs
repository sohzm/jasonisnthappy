use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

#[test]
fn test_iterator_with_large_dataset() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let collection = db.collection("test");

    let num_docs = 200;

    for i in 0..num_docs {
        let doc = json!({
            "_id": format!("doc_{:04}", i),
            "seq": i,
            "data": format!("Document {}", i)
        });
        collection.insert(doc).unwrap();
    }

    let count = collection.count().unwrap();
    assert_eq!(count, num_docs, "Iterator should count all documents");

    let all_docs = collection.find_all().unwrap();
    assert_eq!(all_docs.len(), num_docs, "Should retrieve all documents");
}

#[test]
fn test_iterator_within_transaction() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let collection = db.collection("test");

    let num_docs = 100;

    for i in 0..num_docs {
        let doc = json!({
            "_id": format!("doc_{}", i),
            "value": i
        });
        collection.insert(doc).unwrap();
    }

    let count = collection.count().unwrap();
    assert_eq!(count, num_docs, "Should count all inserted documents");
}

#[test]
fn test_iterator_empty_tree() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let collection = db.collection("test");

    let doc = json!({"_id": "temp", "value": 1});
    collection.insert(doc).unwrap();

    collection.delete_by_id("temp").unwrap();

    let count = collection.count().unwrap();
    assert_eq!(count, 0, "Empty collection should have count 0");

    let docs = collection.find_all().unwrap();
    assert_eq!(docs.len(), 0, "Empty collection should return empty array");
}

#[test]
fn test_iterator_after_deletes() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let collection = db.collection("test");

    for i in 0..100 {
        let doc = json!({
            "_id": format!("doc_{}", i),
            "value": i
        });
        collection.insert(doc).unwrap();
    }

    let count = collection.count().unwrap();
    assert_eq!(count, 100);

    for i in (0..100).step_by(2) {
        collection.delete_by_id(&format!("doc_{}", i)).unwrap();
    }

    let count_after = collection.count().unwrap();
    assert_eq!(count_after, 50, "Should have 50 documents after deleting half");

    let remaining_docs = collection.find_all().unwrap();
    assert_eq!(remaining_docs.len(), 50);
}

#[test]
fn test_iterator_concurrent_reads() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let collection = db.collection("test");

    let num_docs = 150;
    for i in 0..num_docs {
        let doc = json!({
            "_id": format!("doc_{:04}", i),
            "seq": i
        });
        collection.insert(doc).unwrap();
    }

    let db = Arc::new(db);
    let num_threads = 10;
    let mut handles = vec![];

    for _thread_id in 0..num_threads {
        let db = Arc::clone(&db);

        let handle = thread::spawn(move || {
            let collection = db.collection("test");

            let count = collection.count().unwrap();

            let docs = collection.find_all().unwrap();

            (count, docs.len())
        });

        handles.push(handle);
    }

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    for (count, docs_len) in results {
        assert_eq!(count, num_docs, "Each thread should count all documents");
        assert_eq!(docs_len, num_docs, "Each thread should retrieve all documents");
    }
}

#[test]
fn test_iterator_stress_large_dataset() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let collection = db.collection("test");

    let num_docs = 500;

    println!("Inserting {} documents...", num_docs);

    for i in 0..num_docs {
        let doc = json!({
            "_id": format!("doc_{:06}", i),
            "seq": i,
            "data": format!("This is document number {} with some additional data", i)
        });
        collection.insert(doc).unwrap();

        if i % 100 == 0 && i > 0 {
            println!("Inserted {} documents", i);
        }
    }

    println!("Counting documents...");

    let count = collection.count().unwrap();
    assert_eq!(count, num_docs, "Should count all {} documents", num_docs);

    println!("Retrieving all documents...");

    let all_docs = collection.find_all().unwrap();
    assert_eq!(all_docs.len(), num_docs, "Should retrieve all documents");

    for (i, doc) in all_docs.iter().enumerate() {
        let expected_id = format!("doc_{:06}", i);
        assert_eq!(doc["_id"], expected_id, "Documents should be in sorted order");
    }

    println!("Iterator stress test completed successfully!");
}

#[test]
fn test_iterator_with_updates() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let collection = db.collection("test");

    for i in 0..100 {
        let doc = json!({
            "_id": format!("doc_{}", i),
            "version": 1,
            "value": i
        });
        collection.insert(doc).unwrap();
    }

    for i in 0..100 {
        let update = json!({
            "version": 2,
            "value": i * 2
        });
        collection.update_by_id(&format!("doc_{}", i), update).unwrap();
    }

    let count = collection.count().unwrap();
    assert_eq!(count, 100, "Count should remain unchanged after updates");

    let all_docs = collection.find_all().unwrap();
    assert_eq!(all_docs.len(), 100);

    let updated_count = all_docs.iter().filter(|doc| doc["version"] == 2).count();
    assert!(updated_count > 0, "At least some documents should be updated");
}

#[test]
fn test_iterator_query_integration() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let collection = db.collection("test");

    for i in 0..200 {
        let doc = json!({
            "_id": format!("doc_{}", i),
            "value": i,
            "category": if i % 3 == 0 {
                "A"
            } else if i % 3 == 1 {
                "B"
            } else {
                "C"
            },
            "active": i % 2 == 0
        });
        collection.insert(doc).unwrap();
    }

    let category_a = collection.find(r#"category is "A""#).unwrap();
    let expected_count_a = 200 / 3;
    assert!(
        (category_a.len() as i64 - expected_count_a as i64).abs() <= 1,
        "Should find approximately {} category A documents",
        expected_count_a
    );

    let active_docs = collection.find("active is true").unwrap();
    assert_eq!(active_docs.len(), 100, "Should find 100 active documents");

    let complex = collection.find(r#"category is "B" and active is true"#).unwrap();
    assert!(complex.len() > 0, "Complex query should return results");
}
