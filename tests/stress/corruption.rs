// Corruption stress tests
// Tests that verify the database handles corrupted data gracefully

use jasonisnthappy::core::database::Database;
use serde_json::json;
use tempfile::TempDir;

#[test]
fn test_corrupted_header() {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("corrupted_header.db");
    let db_path_str = db_path.to_str().unwrap().to_string();
    {
        let db = Database::open(&db_path_str).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();
        coll.insert(json!({"_id": "test", "value": 42})).unwrap();
        tx.commit().unwrap();
        db.close().unwrap();
    }
    {
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path_str)
            .unwrap();

        db_file.seek(SeekFrom::Start(0)).unwrap();
        db_file.write_all(b"DEAD").unwrap();
        db_file.sync_all().unwrap();
    }

    assert!(Database::open(&db_path_str).is_err(), "Should reject corrupted header");
}

#[test]
fn test_corrupted_pages() {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("corrupted_pages.db");
    let db_path_str = db_path.to_str().unwrap().to_string();
    {
        let db = Database::open(&db_path_str).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        for i in 0..50 {
            coll.insert(json!({"_id": format!("doc{}", i), "value": i})).unwrap();
        }
        tx.commit().unwrap();
        db.close().unwrap();
    }
    {
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path_str)
            .unwrap();

        let file_size = db_file.metadata().unwrap().len();
        if file_size > 16384 {
            let corrupt_offset = 12288;
            db_file.seek(SeekFrom::Start(corrupt_offset)).unwrap();

            let garbage = vec![0xFF; 1024];
            db_file.write_all(&garbage).unwrap();
            db_file.sync_all().unwrap();
        } else {
            return;
        }
    }

    // Database should handle corruption gracefully
    let _ = Database::open(&db_path_str);
}

#[test]
fn test_corrupted_wal() {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("corrupted_wal.db");
    let db_path_str = db_path.to_str().unwrap().to_string();
    let wal_path = format!("{}-wal", db_path_str);
    {
        let db = Database::open(&db_path_str).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        for i in 0..20 {
            let doc = json!({
                "_id": format!("doc{}", i),
                "value": i * 100,
                "data": "X".repeat(50),
            });
            coll.insert(doc).unwrap();
        }
        tx.commit().unwrap();
        db.close().unwrap();
    }
    {
        let db = Database::open(&db_path_str).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();
        coll.insert(json!({"_id": "wal_test", "data": "test"})).unwrap();
        tx.commit().unwrap();
        db.close().unwrap();
    }

    match OpenOptions::new().read(true).write(true).open(&wal_path) {
        Ok(mut wal_file) => {
            let wal_size = wal_file.metadata().unwrap().len();
            if wal_size < 100 {
                return;
            }

            use std::io::Read;
            let mut wal_data = Vec::new();
            wal_file.seek(SeekFrom::Start(0)).unwrap();
            wal_file.read_to_end(&mut wal_data).unwrap();

            let corrupt_pos = (wal_size / 2) as usize;
            wal_data[corrupt_pos] ^= 0xFF;
            wal_data[corrupt_pos + 1] ^= 0xFF;
            wal_data[corrupt_pos + 10] ^= 0x55;

            wal_file.seek(SeekFrom::Start(0)).unwrap();
            wal_file.write_all(&wal_data).unwrap();
            wal_file.sync_all().unwrap();
        }
        Err(_) => return
    }

    // Database should either handle corruption gracefully or reject it
    let _ = Database::open(&db_path_str);
}

#[test]
fn test_partial_wal_write() {
    use std::fs::OpenOptions;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("partial_wal.db");
    let db_path_str = db_path.to_str().unwrap().to_string();
    let wal_path = format!("{}-wal", db_path_str);
    {
        let db = Database::open(&db_path_str).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        for i in 0..30 {
            coll.insert(json!({"_id": format!("doc{}", i), "value": i})).unwrap();
        }
        tx.commit().unwrap();
        db.close().unwrap();
    }
    {
        let db = Database::open(&db_path_str).unwrap();
        let mut tx = db.begin().unwrap();
        let mut coll = tx.collection("test").unwrap();

        for i in 30..60 {
            coll.insert(json!({"_id": format!("doc{}", i), "value": i})).unwrap();
        }
        tx.commit().unwrap();
        db.close().unwrap();
    }

    match OpenOptions::new().write(true).open(&wal_path) {
        Ok(wal_file) => {
            let original_size = wal_file.metadata().unwrap().len();
            if original_size > 1000 {
                let truncate_size = (original_size as f64 * 0.7) as u64;
                wal_file.set_len(truncate_size).unwrap();
            } else {
                return;
            }
        }
        Err(_) => return
    }

    if let Ok(db) = Database::open(&db_path_str) {
        let mut tx = db.begin().unwrap();
        let coll = tx.collection("test").unwrap();
        let docs = coll.find_all().unwrap();
        tx.rollback().unwrap();

        assert!(docs.len() >= 30, "Should have recovered at least initial 30 documents");
        db.close().unwrap();
    }
}
