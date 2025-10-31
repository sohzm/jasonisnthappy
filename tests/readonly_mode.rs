use jasonisnthappy::core::database::{Database, DatabaseOptions};
use serde_json::json;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

#[test]
fn test_readonly_mode_basic() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap();

    let db = Database::open(db_path_str).unwrap();
    let collection = db.collection("test");

    for i in 0..10 {
        let doc = json!({
            "_id": format!("doc_{}", i),
            "value": i
        });
        collection.insert(doc).unwrap();
    }

    for i in 0..10 {
        let doc = collection.find_by_id(&format!("doc_{}", i)).unwrap();
        assert_eq!(doc["value"], i, "Should be able to read doc_{}", i);
    }

    let count = collection.count().unwrap();
    assert_eq!(count, 10, "Should have 10 documents");
}

#[test]
fn test_readonly_prevents_writes() {

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap();

    let db = Database::open(db_path_str).unwrap();
    let collection = db.collection("test");
    let doc = json!({"_id": "doc_1", "value": 1});
    collection.insert(doc).unwrap();

    let doc = collection.find_by_id("doc_1").unwrap();
    assert_eq!(doc["value"], 1);
}

#[test]
fn test_readonly_multiple_connections() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    {
        let db = Database::open(&db_path_str).unwrap();
        let collection = db.collection("test");

        for i in 0..100 {
            let doc = json!({
                "_id": format!("doc_{}", i),
                "value": i
            });
            collection.insert(doc).unwrap();
        }

        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    let num_readers = 5;
    let mut handles = vec![];

    for reader_id in 0..num_readers {
        let db_path = db_path_str.clone();

        let handle = thread::spawn(move || {
            let opts = DatabaseOptions {
                read_only: true,
                ..Default::default()
            };
            let db = Database::open_with_options(&db_path, opts).unwrap();
            let collection = db.collection("test");

            let mut read_count = 0;
            for i in 0..100 {
                if i % num_readers == reader_id {
                    if collection.find_by_id(&format!("doc_{}", i)).is_ok() {
                        read_count += 1;
                    }
                }
            }

            read_count
        });

        handles.push(handle);
    }

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    let total_reads: usize = results.iter().sum();
    assert_eq!(total_reads, 100, "All readers should successfully read all documents");
}

#[test]
fn test_readonly_concurrent_with_queries() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    {
        let db = Database::open(&db_path_str).unwrap();
        let collection = db.collection("test");

        for i in 0..50 {
            let doc = json!({
                "_id": format!("doc_{}", i),
                "value": i,
                "type": if i % 2 == 0 { "even" } else { "odd" }
            });
            collection.insert(doc).unwrap();
        }

        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    let db_path_arc = Arc::new(db_path_str);
    let mut handles = vec![];

    {
        let db_path = Arc::clone(&db_path_arc);
        handles.push(thread::spawn(move || {
            let opts = DatabaseOptions {
                read_only: true,
                ..Default::default()
            };
            let db = Database::open_with_options(&db_path, opts).unwrap();
            let collection = db.collection("test");
            collection.count().unwrap()
        }));
    }

    {
        let db_path = Arc::clone(&db_path_arc);
        handles.push(thread::spawn(move || {
            let opts = DatabaseOptions {
                read_only: true,
                ..Default::default()
            };
            let db = Database::open_with_options(&db_path, opts).unwrap();
            let collection = db.collection("test");
            let mut count = 0;
            for i in (0..50).step_by(5) {
                if collection.find_by_id(&format!("doc_{}", i)).is_ok() {
                    count += 1;
                }
            }
            count
        }));
    }

    {
        let db_path = Arc::clone(&db_path_arc);
        handles.push(thread::spawn(move || {
            let opts = DatabaseOptions {
                read_only: true,
                ..Default::default()
            };
            let db = Database::open_with_options(&db_path, opts).unwrap();
            let collection = db.collection("test");
            let results = collection.find(r#"type is "even""#).unwrap();
            results.len()
        }));
    }

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    assert_eq!(results[0], 50, "Count should return 50");
    assert_eq!(results[1], 10, "Should find 10 documents");
    assert_eq!(results[2], 25, "Should find 25 even documents");
}

#[test]
fn test_readonly_stress_many_readers() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    {
        let db = Database::open(&db_path_str).unwrap();
        let collection = db.collection("test");

        for i in 0..200 {
            let doc = json!({
                "_id": format!("doc_{}", i),
                "value": i,
                "data": format!("Data for document {}", i)
            });
            collection.insert(doc).unwrap();
        }

        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    let num_readers = 20;
    let reads_per_reader = 50;
    let db_path_arc = Arc::new(db_path_str);
    let mut handles = vec![];

    for reader_id in 0..num_readers {
        let db_path = Arc::clone(&db_path_arc);

        let handle = thread::spawn(move || {
            let opts = DatabaseOptions {
                read_only: true,
                ..Default::default()
            };
            let db = Database::open_with_options(&db_path, opts).unwrap();
            let collection = db.collection("test");
            let mut successful_reads = 0;

            for i in 0..reads_per_reader {
                let doc_id = format!("doc_{}", (reader_id * reads_per_reader + i) % 200);
                if collection.find_by_id(&doc_id).is_ok() {
                    successful_reads += 1;
                }
            }

            successful_reads
        });

        handles.push(handle);
    }

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    let total_reads: usize = results.iter().sum();
    let expected_reads = num_readers * reads_per_reader;
    assert_eq!(
        total_reads, expected_reads,
        "All readers should successfully read their documents"
    );
}
