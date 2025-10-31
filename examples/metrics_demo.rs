use jasonisnthappy::{Database, BackupInfo};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Cleanup any existing files
    let _ = std::fs::remove_file("metrics_demo.db");
    let _ = std::fs::remove_file("metrics_demo.db.lock");
    let _ = std::fs::remove_file("metrics_demo.db-wal");
    let _ = std::fs::remove_file("metrics_demo_backup.db");

    println!("=== Observability & Backup Demo ===\n");

    // 1. Open database
    let db = Database::open("metrics_demo.db")?;
    println!("✓ Database opened\n");

    // 2. Insert data
    println!("Inserting 50 documents...");
    for batch in 0..5 {
        let mut tx = db.begin()?;
        let mut users = tx.collection("users")?;

        for i in 1..=10 {
            users.insert(json!({
                "name": format!("User {}", batch * 10 + i),
                "age": 20 + i,
                "active": true
            }))?;
        }
        tx.commit()?;
    }
    println!("✓ Inserted 50 documents in 5 transactions\n");

    // 3. Read and update some data
    let mut tx = db.begin()?;
    let mut users = tx.collection("users")?;
    let all_docs = users.find_all()?;
    println!("✓ Read {} documents", all_docs.len());

    // Update first 5 documents
    for doc in all_docs.iter().take(5) {
        let id = doc["_id"].as_str().unwrap();
        users.update_by_id(id, json!({"active": false}))?;
    }
    tx.commit()?;
    println!("✓ Updated 5 documents\n");

    // 4. Display comprehensive metrics
    println!("=== Metrics Snapshot ===\n");
    let metrics = db.metrics();

    println!("📊 TRANSACTIONS:");
    println!("   Active:          {}", metrics.active_transactions);
    println!("   Begun:           {}", metrics.transactions_begun);
    println!("   Committed:       {}", metrics.transactions_committed);
    println!("   Aborted:         {}", metrics.transactions_aborted);
    println!("   Commit Rate:     {:.1}%", metrics.commit_rate * 100.0);
    println!("   Conflicts:       {}", metrics.transaction_conflicts);

    println!("\n💾 CACHE:");
    println!("   Hit Rate:        {:.2}%", metrics.cache_hit_rate * 100.0);
    println!("   Hits:            {}", metrics.cache_hits);
    println!("   Misses:          {}", metrics.cache_misses);
    println!("   Total Requests:  {}", metrics.cache_total_requests);
    println!("   Dirty Pages:     {}", metrics.dirty_pages);

    println!("\n📦 STORAGE:");
    println!("   Pages Allocated: {}", metrics.pages_allocated);
    println!("   Pages Freed:     {}", metrics.pages_freed);
    println!("   WAL Writes:      {}", metrics.wal_writes);
    println!("   WAL Bytes:       {} bytes", metrics.wal_bytes_written);
    println!("   Checkpoints:     {}", metrics.checkpoints);

    println!("\n📄 DOCUMENTS:");
    println!("   Inserted:        {}", metrics.documents_inserted);
    println!("   Updated:         {}", metrics.documents_updated);
    println!("   Deleted:         {}", metrics.documents_deleted);
    println!("   Read:            {}", metrics.documents_read);
    println!("   Total Ops:       {}", metrics.total_document_operations);

    println!("\n⚠️  ERRORS:");
    println!("   I/O Errors:      {}", metrics.io_errors);
    println!("   Conflicts:       {}", metrics.transaction_conflicts);

    // 5. Create backup
    println!("\n=== Creating Backup ===");
    db.backup("metrics_demo_backup.db")?;
    println!("✓ Backup created");

    // 6. Verify backup
    let info: BackupInfo = Database::verify_backup("metrics_demo_backup.db")?;
    println!("✓ Backup verified:");
    println!("   Version:         {}", info.version);
    println!("   Collections:     {}", info.num_collections);
    println!("   Pages:           {}", info.num_pages);
    println!("   File Size:       {} KB", info.file_size / 1024);

    // 7. Verify backup by opening it
    println!("\n=== Verifying Backup Data ===");
    let backup_db = Database::open("metrics_demo_backup.db")?;
    let mut backup_tx = backup_db.begin()?;
    let backup_users = backup_tx.collection("users")?;
    let backup_docs = backup_users.find_all()?;
    println!("✓ Backup contains {} documents", backup_docs.len());
    println!("✓ Backup is fully functional!\n");

    println!("=== Demo Complete! ===");
    println!("\nTo view metrics in your browser:");
    println!("  cargo run --example observability_demo --features web-ui");
    println!("  Then visit: http://127.0.0.1:8080\n");

    // Cleanup
    db.close()?;
    backup_db.close()?;

    Ok(())
}
