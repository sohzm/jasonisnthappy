use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::time::Instant;
use tempfile::TempDir;

fn random_string(length: usize) -> String {
    use rand::Rng;
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

fn main() {
    // Match Go's benchmark exactly:
    // 1. Open ONE database (not timed)
    // 2. Run iterations on same DB (timed)

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("bench.db").to_str().unwrap()).unwrap();

    println!("\nRunning 10 iterations of Insert1000 (like Go's benchmark):\n");

    let mut times = Vec::new();
    let mut doc_num = 0;

    for iteration in 0..10 {
        let frames_before = db.frame_count();
        let start = Instant::now();

        let mut tx = db.begin().unwrap();
        let mut collection = tx.collection("bench").unwrap();

        let insert_start = Instant::now();
        for _ in 0..1000 {
            let doc = json!({
                "seq": doc_num,
                "data": random_string(100)
            });
            collection.insert(doc).unwrap();
            doc_num += 1;
        }
        let insert_time = insert_start.elapsed();

        let commit_start = Instant::now();
        tx.commit().unwrap();
        let commit_time = commit_start.elapsed();

        let elapsed = start.elapsed();
        let frames_after = db.frame_count();
        times.push(elapsed);

        println!("Iteration {}: {:.1}ms (inserts: {:.1}ms, commit: {:.1}ms, frames: {} -> {})",
                 iteration, elapsed.as_secs_f64() * 1000.0,
                 insert_time.as_secs_f64() * 1000.0,
                 commit_time.as_secs_f64() * 1000.0,
                 frames_before, frames_after);
    }

    let total: std::time::Duration = times.iter().sum();
    let avg = total / times.len() as u32;
    let min = times.iter().min().unwrap();
    let max = times.iter().max().unwrap();

    println!("\n{:<15} {:>12} {:>12} {:>12}", "Benchmark", "Avg", "Min", "Max");
    println!("{}", "=".repeat(55));
    println!("{:<15} {:>12} {:>12} {:>12}",
             "Insert1000",
             format!("{:.3}ms", avg.as_secs_f64() * 1000.0),
             format!("{:.3}ms", min.as_secs_f64() * 1000.0),
             format!("{:.3}ms", max.as_secs_f64() * 1000.0));

    println!("\n🎯 Go's target: ~32ms per iteration");
}
