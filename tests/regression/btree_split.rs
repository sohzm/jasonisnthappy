// Regression tests for B-tree split bug (fixed in commit 0c02366)

use jasonisnthappy::core::database::Database;
use serde_json::json;
use tempfile::TempDir;

#[test]
fn test_exactly_50_docs() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_50.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let mut tx = db.begin().unwrap();
    let mut coll = tx.collection("test").unwrap();

    for i in 0..50 {
        coll.insert(json!({
            "_id": format!("doc_{}", i),
            "value": i,
        })).unwrap();
    }

    tx.commit().unwrap();

    let mut tx = db.begin().unwrap();
    let coll = tx.collection("test").unwrap();
    let count = coll.count().unwrap();

    assert_eq!(count, 50, "All 50 documents should persist");
}

#[test]
fn test_exactly_51_docs() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_51.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let mut tx = db.begin().unwrap();
    let mut coll = tx.collection("test").unwrap();

    for i in 0..51 {
        coll.insert(json!({
            "_id": format!("doc_{}", i),
            "value": i,
        })).unwrap();
    }

    tx.commit().unwrap();

    let mut tx = db.begin().unwrap();
    let coll = tx.collection("test").unwrap();
    let count = coll.count().unwrap();

    assert_eq!(count, 51, "All 51 documents should persist (this will fail due to bug)");
}

#[test]
fn test_exactly_60_docs() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_60.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let mut tx = db.begin().unwrap();
    let mut coll = tx.collection("test").unwrap();

    for i in 0..60 {
        coll.insert(json!({
            "_id": format!("doc_{}", i),
            "value": i,
        })).unwrap();
    }

    tx.commit().unwrap();

    let mut tx = db.begin().unwrap();
    let coll = tx.collection("test").unwrap();
    let count = coll.count().unwrap();

    assert_eq!(count, 60, "All 60 documents should persist (this will fail due to bug)");
}

#[test]
fn test_incremental_inserts() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_incremental.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    for threshold in [45, 48, 49, 50, 51, 52, 55] {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection(&format!("test_{}", threshold)).unwrap();

        for i in 0..threshold {
            coll.insert(json!({
                "_id": format!("doc_{}", i),
                "value": i,
            })).unwrap();
        }

        tx.commit().unwrap();

        let mut tx = db.begin().unwrap();
        let coll = tx.collection(&format!("test_{}", threshold)).unwrap();
        let count = coll.count().unwrap();

        assert_eq!(count, threshold);
    }
}

#[test]
fn test_100_docs_verbose() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_100.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let mut tx = db.begin().unwrap();
    let mut coll = tx.collection("test").unwrap();

    for i in 0..100 {
        coll.insert(json!({
            "_id": format!("doc_{}", i),
            "value": i,
        })).unwrap();
    }

    tx.commit().unwrap();

    let mut tx = db.begin().unwrap();
    let coll = tx.collection("test").unwrap();
    let count = coll.count().unwrap();

    assert_eq!(count, 100, "All 100 documents should persist");
}

#[test]
fn test_crash_scenario_reproduction() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("crash_test.db");
    let db_path_str = db_path.to_str().unwrap();

    {
        let db = Database::open(db_path_str).unwrap();

        for i in 0..100 {
            let mut tx = db.begin().unwrap();
            let mut collection = tx.collection("test").unwrap();

            let doc_id = format!("doc_0_{}", i);
            let doc = json!({
                "_id": doc_id,
                "round": 0,
                "iter": i,
                "value": i * 10,
            });

            collection.insert(doc.clone()).unwrap();
            tx.commit().unwrap();
        }

        let _ = db.checkpoint();

        let mut tx = db.begin().unwrap();
        let collection = tx.collection("test").unwrap();
        let _count_before = collection.count().unwrap();
        tx.rollback().unwrap();

        drop(db);
    }

    {
        let db = Database::open(db_path_str).unwrap();

        let mut tx = db.begin().unwrap();
        let collection = tx.collection("test").unwrap();
        let count_after = collection.count().unwrap();

        assert_eq!(count_after, 100, "All 100 documents should persist after reopen");
    }
}
