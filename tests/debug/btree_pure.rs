// Debug tests for B-tree performance in isolation

use jasonisnthappy::core::database::Database;
use std::fs;
use std::time::Instant;

#[test]
fn test_btree_search_performance() {
    let test_path = "/tmp/test_btree_search.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();

    // Insert 5000 documents
    for batch in 0..50 {
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        for i in 0..100 {
            let doc = serde_json::json!({
                "_id": format!("doc_{}_{}", batch, i),
                "value": batch * 100 + i,
            });
            coll.insert(doc).unwrap();
        }

        tx.commit().unwrap();
    }

    // Now measure search performance
    let searches = vec![
        ("doc_0_0", "First document"),
        ("doc_25_50", "Middle document"),
        ("doc_49_99", "Last document"),
    ];

    for (doc_id, _description) in searches {
        let mut times = vec![];

        for _ in 0..100 {
            let mut tx = db.begin().unwrap();
            let coll = tx.collection("test").unwrap();

            let start = Instant::now();
            let _result = coll.find_by_id(doc_id);
            let elapsed = start.elapsed();

            times.push(elapsed.as_nanos());
        }

        let _avg = times.iter().sum::<u128>() / times.len() as u128;
        let _min = *times.iter().min().unwrap();
        let _max = *times.iter().max().unwrap();
    }

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}

#[test]
fn test_single_transaction_many_inserts() {
    // Test if inserting many docs in ONE transaction shows degradation
    let test_path = "/tmp/test_single_tx.db";
    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));

    let db = Database::open(test_path).unwrap();

    let mut tx = db.begin().unwrap();
    let mut coll = tx.collection("test").unwrap();

    for i in 0..1000 {
        let doc = serde_json::json!({
            "_id": format!("doc{}", i),
            "data": "x".repeat(1000),
        });
        coll.insert(doc).unwrap();
    }

    tx.commit().unwrap();

    let _ = fs::remove_file(test_path);
    let _ = fs::remove_file(format!("{}.lock", test_path));
    let _ = fs::remove_file(format!("{}-wal", test_path));
}
