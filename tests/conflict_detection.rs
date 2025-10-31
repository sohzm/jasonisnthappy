use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

#[test]
fn test_write_write_conflict_detection() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    let collection = db.collection("test");

    let doc_id = "conflict_test_doc";

    let doc1 = json!({
        "_id": doc_id,
        "thread": 1,
        "data": "First insert"
    });
    let result1 = collection.insert(doc1);
    assert!(result1.is_ok(), "First insert should succeed");

    let doc2 = json!({
        "_id": doc_id,
        "thread": 2,
        "data": "Second insert"
    });
    let result2 = collection.insert(doc2);
    assert!(result2.is_err(), "Second insert with same ID should fail");

    let doc = collection.find_by_id(doc_id).unwrap();
    assert_eq!(doc["thread"], 1, "Should have the first document");
}

#[test]
fn test_concurrent_inserts_different_documents() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

    let num_threads = 10;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];

    for i in 0..num_threads {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier.wait();

            let collection = db.collection("test");

            let mut inserted = Vec::new();
            let mut errors = Vec::new();
            for j in 0..5 {
                let doc = json!({
                    "_id": format!("doc_{}_{}", i, j),
                    "thread": i,
                    "seq": j,
                    "data": format!("Thread {} doc {}", i, j)
                });

                match collection.insert(doc) {
                    Ok(id) => inserted.push(id),
                    Err(e) => errors.push(format!("{:?}", e)),
                }
            }
            (inserted, errors)
        });

        handles.push(handle);
    }

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    let total_inserted: usize = results.iter().map(|(v, _)| v.len()).sum();
    let total_errors: Vec<_> = results.iter().flat_map(|(_, e)| e).collect();

    if !total_errors.is_empty() {
        println!("Errors occurred during concurrent inserts:");
        for (i, err) in total_errors.iter().enumerate().take(5) {
            println!("  Error {}: {}", i + 1, err);
        }
        if total_errors.len() > 5 {
            println!("  ... and {} more errors", total_errors.len() - 5);
        }
    }

    let collection = db.collection("test");
    let count = collection.count().unwrap();

    println!("Successfully inserted: {}/{}", total_inserted, num_threads * 5);
    println!("Documents in collection: {}", count);
    println!("Total errors: {}", total_errors.len());

    assert!(count > 0, "Should have at least some documents");
    assert!(total_inserted > 0, "Should have successfully inserted some documents");
}

#[test]
fn test_concurrent_updates_same_document() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let collection = db.collection("test");
    let doc_id = "update_test_doc";
    let doc = json!({
        "_id": doc_id,
        "counter": 0
    });
    collection.insert(doc).unwrap();

    let db = Arc::new(db);
    let num_threads = 5;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];

    for i in 0..num_threads {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier.wait();

            let collection = db.collection("test");
            let update = json!({
                "counter": i,
                "updated_by": i
            });

            collection.update_by_id(doc_id, update)
        });

        handles.push(handle);
    }

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    let successes = results.iter().filter(|r| r.is_ok()).count();
    assert!(successes >= 1, "At least one update should succeed");

    let collection = db.collection("test");
    let doc = collection.find_by_id(doc_id);
    assert!(doc.is_ok(), "Document should still exist");
}

#[test]
fn test_stress_concurrent_mixed_operations() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Arc::new(Database::open(db_path.to_str().unwrap()).unwrap());

    let collection = db.collection("stress");
    for i in 0..10 {
        let doc = json!({
            "_id": format!("doc_{}", i),
            "value": i
        });
        collection.insert(doc).unwrap();
    }

    let num_threads = 20;
    let operations_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier.wait();

            let collection = db.collection("stress");
            let mut op_count = 0;

            for op in 0..operations_per_thread {
                match op % 3 {
                    0 => {
                        let doc_id = format!("doc_{}", op % 10);
                        let _ = collection.find_by_id(&doc_id);
                        op_count += 1;
                    }
                    1 => {
                        let doc = json!({
                            "_id": format!("thread_{}_doc_{}", thread_id, op),
                            "thread": thread_id,
                            "op": op
                        });
                        if collection.insert(doc).is_ok() {
                            op_count += 1;
                        }
                    }
                    _ => {
                        let doc_id = format!("doc_{}", op % 10);
                        let update = json!({
                            "value": op,
                            "thread": thread_id
                        });
                        if collection.update_by_id(&doc_id, update).is_ok() {
                            op_count += 1;
                        }
                    }
                }
            }

            op_count
        });

        handles.push(handle);
    }

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    let total_ops: usize = results.iter().sum();
    println!("Total successful operations: {}", total_ops);
    assert!(total_ops > 0, "Some operations should have succeeded");
}
