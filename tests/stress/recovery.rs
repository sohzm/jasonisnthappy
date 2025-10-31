// Recovery stress tests
// Tests that verify database recovery mechanisms work correctly

use jasonisnthappy::core::database::Database;
use serde_json::json;
use tempfile::TempDir;

#[test]
fn test_repeated_recovery_cycles() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("recovery_cycles.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let mut total_inserted = 0;

    for cycle in 0..10 {

        let db = Database::open(&db_path_str).unwrap();

        let mut cycle_inserts = 0;
        for i in 0..100 {
            let mut tx = db.begin().unwrap();
            let mut coll = tx.collection("test").unwrap();
            let doc = json!({
                "_id": format!("cycle{}_doc{}", cycle, i),
                "cycle": cycle,
                "iter": i,
                "data": "X".repeat(50),
            });

            coll.insert(doc).unwrap();
            tx.commit().unwrap();
            cycle_inserts += 1;
        }

        total_inserted += cycle_inserts;

        db.close().unwrap();
    }
    let db = Database::open(&db_path_str).unwrap();
    let mut tx = db.begin().unwrap();
    let coll = tx.collection("test").unwrap();
    let docs = coll.find_all().unwrap();
    tx.rollback().unwrap();

    assert_eq!(docs.len(), total_inserted, "Expected {} documents", total_inserted);
    db.close().unwrap();
}

#[test]
fn test_massive_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("massive_rollback.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let mut tx = db.begin().unwrap();
    let mut coll = tx.collection("test_rollback").unwrap();

    let num_docs = 10000;
    for i in 0..num_docs {
        let doc = json!({
            "_id": format!("doc_{}", i),
            "value": i,
            "data": "X".repeat(100),
        });
        coll.insert(doc).unwrap();
    }

    let docs = coll.find_all().unwrap();
    assert_eq!(docs.len(), num_docs, "Expected {} documents in transaction", num_docs);

    tx.rollback().unwrap();
    let mut tx2 = db.begin().unwrap();
    let coll2 = tx2.collection("test_rollback").unwrap();
    let docs = coll2.find_all().unwrap();
    tx2.rollback().unwrap();

    assert_eq!(docs.len(), 0, "Expected 0 documents after rollback");

    let mut tx3 = db.begin().unwrap();
    let mut coll3 = tx3.collection("test_rollback").unwrap();
    coll3.insert(json!({"test": "after_rollback"})).unwrap();
    tx3.commit().unwrap();

    db.close().unwrap();
}
