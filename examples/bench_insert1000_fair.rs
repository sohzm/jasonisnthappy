use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::time::Instant;
use tempfile::TempDir;

fn main() {
    println!("Fair benchmark: Insert1000 - exactly like Go benchmark\n");

    // Exactly like Go: one database, 20 iterations
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("bench.db");
    let db = Database::open(db_path.to_str().unwrap()).unwrap();

    let mut times = Vec::new();

    for i in 0..20 {
        let start = Instant::now();

        let mut tx = db.begin().unwrap();
        let mut collection = tx.collection("bench").unwrap();

        for j in 0..1000 {
            let doc = json!({
                "name": format!("user_{}_{}", i, j),
                "age": 30
            });
            collection.insert(doc).unwrap();
        }

        tx.commit().unwrap();

        let elapsed = start.elapsed();
        times.push(elapsed);

        if i < 5 || i >= 15 {
            println!("Iteration {:2}: {:.3}ms (DB now has {} docs)",
                     i, elapsed.as_secs_f64() * 1000.0, (i + 1) * 1000);
        } else if i == 5 {
            println!("...");
        }
    }

    let total: std::time::Duration = times.iter().sum();
    let avg = total / 20;

    println!("\nResults:");
    println!("  Total time: {:.3}ms", total.as_secs_f64() * 1000.0);
    println!("  Average:    {:.3}ms", avg.as_secs_f64() * 1000.0);
    println!("  Per op:     {:.0}ns", avg.as_nanos() as f64);

    // Compare to Go
    println!("\nComparison:");
    println!("  Go:   31.40ms per iteration");
    println!("  Rust: {:.2}ms per iteration", avg.as_secs_f64() * 1000.0);
    let ratio = avg.as_secs_f64() / 0.03140;
    if ratio < 1.0 {
        println!("  Rust is {:.1}% FASTER", (1.0 - ratio) * 100.0);
    } else {
        println!("  Rust is {:.1}% slower", (ratio - 1.0) * 100.0);
    }
}
