use jasonisnthappy::{Database, BackupInfo};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open database
    println!("=== Database Observability Demo ===\n");
    let db = Database::open("demo.db")?;
    println!("✓ Database opened: demo.db\n");

    // 2. Insert some data
    println!("Inserting sample data...");
    let mut tx = db.begin()?;
    let mut users = tx.collection("users")?;

    for i in 1..=10 {
        users.insert(json!({
            "name": format!("User {}", i),
            "age": 20 + i,
            "email": format!("user{}@example.com", i)
        }))?;
    }
    tx.commit()?;
    println!("✓ Inserted 10 documents\n");

    // 3. Read data to generate metrics
    let mut tx = db.begin()?;
    let users = tx.collection("users")?;
    let docs = users.find_all()?;
    println!("✓ Read {} documents\n", docs.len());

    // 4. Display metrics
    println!("=== Current Metrics ===");
    let metrics = db.metrics();
    println!("Transactions:");
    println!("  - Active: {}", metrics.active_transactions);
    println!("  - Committed: {}", metrics.transactions_committed);
    println!("  - Aborted: {}", metrics.transactions_aborted);
    println!("  - Commit Rate: {:.1}%", metrics.commit_rate * 100.0);

    println!("\nCache:");
    println!("  - Hit Rate: {:.1}%", metrics.cache_hit_rate * 100.0);
    println!("  - Hits: {}", metrics.cache_hits);
    println!("  - Misses: {}", metrics.cache_misses);

    println!("\nStorage:");
    println!("  - Pages Allocated: {}", metrics.pages_allocated);
    println!("  - Pages Freed: {}", metrics.pages_freed);
    println!("  - WAL Writes: {}", metrics.wal_writes);
    println!("  - Checkpoints: {}", metrics.checkpoints);

    println!("\nDocuments:");
    println!("  - Inserted: {}", metrics.documents_inserted);
    println!("  - Updated: {}", metrics.documents_updated);
    println!("  - Deleted: {}", metrics.documents_deleted);
    println!("  - Read: {}", metrics.documents_read);
    println!();

    // 5. Create backup
    println!("=== Creating Backup ===");
    let backup_path = "demo_backup.db";
    db.backup(backup_path)?;
    println!("✓ Backup created: {}", backup_path);

    // 6. Verify backup
    let backup_info: BackupInfo = Database::verify_backup(backup_path)?;
    println!("  - Collections: {}", backup_info.num_collections);
    println!("  - Pages: {}", backup_info.num_pages);
    println!("  - File Size: {} bytes", backup_info.file_size);
    println!();

    // 7. Start Web UI (optional - requires web-ui feature)
    #[cfg(feature = "web-ui")]
    {
        println!("=== Starting Web UI ===");
        let addr = "127.0.0.1:8080";
        let _web_server = db.start_web_ui(addr)?;
        println!("✓ Web UI started at http://{}", addr);
        println!("\nEndpoints:");
        println!("  - Dashboard:     http://{}/", addr);
        println!("  - Metrics JSON:  http://{}/metrics", addr);
        println!("  - Collections:   http://{}/api/collections", addr);
        println!("  - Health Check:  http://{}/health", addr);
        println!("\nPress Ctrl+C to stop...");

        // Keep the server running
        std::thread::park();
    }

    #[cfg(not(feature = "web-ui"))]
    {
        println!("Web UI not enabled. Run with --features web-ui to enable.");
    }

    Ok(())
}
