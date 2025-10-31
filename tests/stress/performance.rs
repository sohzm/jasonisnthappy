// Performance stress tests
// Tests that focus on high-volume operations, large documents, and system throughput

use jasonisnthappy::core::database::Database;
use serde_json::json;
use tempfile::TempDir;

fn setup_test_db() -> (TempDir, Database) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("stress_test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();
    (temp_dir, db)
}

#[test]
fn test_crud_20k_documents() {
    let (_temp_dir, db) = setup_test_db();

    let num_docs = 20_000;

    // CREATE: Insert documents in batches of 1000
    {
        let batch_size = 1000;
        let mut _total_inserted = 0;
        for batch_start in (0..num_docs).step_by(batch_size) {
            let mut tx = db.begin().unwrap();
            let mut coll = tx.collection("crud_test").unwrap();

            let batch_end = (batch_start + batch_size).min(num_docs);
            let mut batch_count = 0;
            for i in batch_start..batch_end {
                let doc = json!({
                    "_id": format!("doc_{}", i),
                    "index": i,
                    "value": i * 10,
                    "status": "active",
                    "data": "X".repeat(500),
                });

                match coll.insert(doc) {
                    Ok(_) => batch_count += 1,
                    Err(e) => {
                        panic!("Insert failed for doc_{}: {:?}", i, e);
                    }
                }
            }

            match tx.commit() {
                Ok(_) => {
                    _total_inserted += batch_count;
                }
                Err(e) => {
                    panic!("Commit failed for batch {}-{}: {:?}", batch_start, batch_end, e);
                }
            }
        }
    }

    // Verify count
    {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("crud_test").unwrap();
        let count = coll.count().unwrap();

        assert_eq!(count, num_docs, "Expected {} documents", num_docs);
        tx.rollback().unwrap();
    }

    // READ: Read all documents
    {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("crud_test").unwrap();

        let docs = coll.find_all().unwrap();
        assert_eq!(docs.len(), num_docs, "Expected {} documents in find_all", num_docs);

        // Verify first and last documents
        let first = coll.find_by_id("doc_0").unwrap();
        assert_eq!(first.get("index").and_then(|v| v.as_u64()), Some(0));

        let last = coll.find_by_id(&format!("doc_{}", num_docs - 1)).unwrap();
        assert_eq!(last.get("index").and_then(|v| v.as_u64()), Some((num_docs - 1) as u64));

        tx.rollback().unwrap();
    }

    // Perform some spot checks on specific documents
    {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("crud_test").unwrap();

        // Check some random documents exist and have correct values
        for i in [0, 100, 1000, 5000, 10000, 15000, 19999] {
            let doc = coll.find_by_id(&format!("doc_{}", i)).unwrap();
            assert_eq!(doc.get("index").and_then(|v| v.as_u64()), Some(i as u64));
            assert_eq!(doc.get("value").and_then(|v| v.as_u64()), Some((i * 10) as u64));
            assert_eq!(doc.get("status").and_then(|v| v.as_str()), Some("active"));
        }

        tx.rollback().unwrap();
    }
}

#[test]
fn test_massive_documents_20mb() {
    let (_temp_dir, db) = setup_test_db();

    // Each document is ~20MB
    let doc_size = 20 * 1024 * 1024; // 20MB
    let num_docs = 100;

    // Insert phase
    for i in 0..num_docs {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("massive_docs").unwrap();

        let large_data = "X".repeat(doc_size);
        let doc = json!({
            "_id": format!("massive_doc_{}", i),
            "index": i,
            "size_mb": 20,
            "data": large_data,
        });

        coll.insert(doc).unwrap();
        tx.commit().unwrap();
    }

    // Verify all documents exist
    {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("massive_docs").unwrap();
        let count = coll.count().unwrap();
        assert_eq!(count, num_docs, "Expected {} documents after insert", num_docs);
        tx.rollback().unwrap();
    }

    // Remove phase
    for i in 0..num_docs {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("massive_docs").unwrap();

        coll.delete_by_id(&format!("massive_doc_{}", i)).unwrap();
        tx.commit().unwrap();
    }

    // Verify all documents removed
    {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("massive_docs").unwrap();
        let count = coll.count().unwrap();
        assert_eq!(count, 0, "Expected 0 documents after removal");
        tx.rollback().unwrap();
    }
}

#[test]
fn test_5_massive_documents() {
    use jasonisnthappy::core::database::Database;
    use serde_json::json;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    for i in 0..5 {

        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("massive_docs").unwrap();

        let large_data = "X".repeat(20 * 1024 * 1024); // 20MB
        let doc = json!({
            "_id": format!("massive_doc_{}", i),
            "index": i,
            "data": large_data,
        });

        coll.insert(doc).unwrap();
        tx.commit().unwrap();
    }
}

#[test]
fn test_large_multipage_documents() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("large_docs.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let mut tx = db.begin().unwrap();
    let mut coll = tx.collection("large").unwrap();

    let num_docs = 50;
    for i in 0..num_docs {
        let large_data = "X".repeat(100_000);
        let doc = json!({
            "_id": format!("large_{}", i),
            "size": "100KB",
            "data": large_data,
        });
        coll.insert(doc).unwrap();
    }
    tx.commit().unwrap();
    let mut tx2 = db.begin().unwrap();
    let coll2 = tx2.collection("large").unwrap();
    let docs = coll2.find_all().unwrap();
    assert_eq!(docs.len(), num_docs, "Expected {} documents", num_docs);

    for (i, doc) in docs.iter().enumerate() {
        let data = doc.get("data").unwrap().as_str().unwrap();
        assert_eq!(data.len(), 100_000, "Doc {} has wrong size", i);
    }

    tx2.rollback().unwrap();
    db.close().unwrap();
}
