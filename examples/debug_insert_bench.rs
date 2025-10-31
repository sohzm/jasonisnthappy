use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::time::Instant;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "/tmp/debug_insert_bench.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    println!("=== Insert Performance Debug Benchmark ===\n");

    // Test 1: Single inserts with commit each time (worst case)
    {
        let db = Database::open(path)?;
        let total_start = Instant::now();
        let n = 100;

        for i in 0..n {
            let doc = json!({"name": format!("user{}", i), "age": i, "active": true});

            let tx_start = Instant::now();
            let mut tx = db.begin()?;
            let tx_time = tx_start.elapsed();

            let coll_start = Instant::now();
            let mut users = tx.collection("users")?;
            let coll_time = coll_start.elapsed();

            let insert_start = Instant::now();
            let _id = users.insert(doc)?;
            let insert_time = insert_start.elapsed();

            let commit_start = Instant::now();
            tx.commit()?;
            let commit_time = commit_start.elapsed();

            if i < 5 {
                println!("Insert #{}: tx={:?}, coll={:?}, insert={:?}, commit={:?}",
                    i, tx_time, coll_time, insert_time, commit_time);
            }
        }

        let total_time = total_start.elapsed();
        println!("\nTest 1: {} individual inserts with commits", n);
        println!("Total time: {:?}", total_time);
        println!("Per insert: {:?}", total_time / n);
        println!("Inserts/sec: {:.2}", n as f64 / total_time.as_secs_f64());

        db.close()?;
    }

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    // Test 2: Batch inserts in single transaction
    {
        let db = Database::open(path)?;
        let n = 1000;

        let total_start = Instant::now();
        let mut tx = db.begin()?;
        let mut users = tx.collection("users")?;

        let batch_start = Instant::now();
        for i in 0..n {
            let doc = json!({"name": format!("user{}", i), "age": i, "active": true});
            users.insert(doc)?;
        }
        let batch_time = batch_start.elapsed();

        let commit_start = Instant::now();
        tx.commit()?;
        let commit_time = commit_start.elapsed();

        let total_time = total_start.elapsed();

        println!("\nTest 2: {} inserts in single transaction", n);
        println!("Batch insert time: {:?}", batch_time);
        println!("Commit time: {:?}", commit_time);
        println!("Total time: {:?}", total_time);
        println!("Per insert: {:?}", total_time / n);
        println!("Inserts/sec: {:.2}", n as f64 / total_time.as_secs_f64());

        db.close()?;
    }

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    // Test 3: Large batch with timing breakdown
    {
        let db = Database::open(path)?;
        let n = 5000;

        println!("\nTest 3: {} inserts with detailed timing", n);

        let total_start = Instant::now();
        let mut tx = db.begin()?;
        let mut users = tx.collection("users")?;

        let mut insert_times = Vec::new();
        for i in 0..n {
            let doc = json!({"name": format!("user{}", i), "age": i, "active": true});
            let insert_start = Instant::now();
            users.insert(doc)?;
            insert_times.push(insert_start.elapsed());
        }

        let commit_start = Instant::now();
        tx.commit()?;
        let commit_time = commit_start.elapsed();

        let total_time = total_start.elapsed();

        // Calculate insert time statistics
        let total_insert_time: std::time::Duration = insert_times.iter().sum();
        let avg_insert_time = total_insert_time / insert_times.len() as u32;
        let min_insert_time = insert_times.iter().min().unwrap();
        let max_insert_time = insert_times.iter().max().unwrap();

        println!("Total time: {:?}", total_time);
        println!("Commit time: {:?} ({:.1}% of total)",
            commit_time,
            (commit_time.as_secs_f64() / total_time.as_secs_f64()) * 100.0);
        println!("Insert times: avg={:?}, min={:?}, max={:?}",
            avg_insert_time, min_insert_time, max_insert_time);
        println!("Per insert: {:?}", total_time / n);
        println!("Inserts/sec: {:.2}", n as f64 / total_time.as_secs_f64());

        // Show first few and last few insert times
        println!("\nFirst 10 insert times:");
        for (i, time) in insert_times.iter().take(10).enumerate() {
            println!("  Insert {}: {:?}", i, time);
        }
        println!("Last 10 insert times:");
        for (i, time) in insert_times.iter().rev().take(10).rev().enumerate() {
            let idx = n - 10 + i as u32;
            println!("  Insert {}: {:?}", idx, time);
        }

        db.close()?;
    }

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.lock", path));
    let _ = fs::remove_file(format!("{}-wal", path));

    Ok(())
}
