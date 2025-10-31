
use jasonisnthappy::core::database::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("example.db")?;

    println!("=== JasonIsntHappy Database Example ===\n");

    println!("1. Creating transaction...");
    let mut tx = db.begin()?;

    println!("2. Writing documents...");
    tx.write_document("users", "user1", 100)?;
    tx.write_document("users", "user2", 101)?;

    println!("3. Committing transaction...");
    tx.commit()?;

    println!("4. Transaction committed successfully!\n");

    let metadata = db.get_metadata();
    println!("Database metadata:");
    println!("  Collections: {}", metadata.collections.len());

    println!("\n5. Adding 'posts' collection to metadata...");
    db.update_metadata(|m| {
        m.get_collection("posts");
    })?;

    let metadata = db.get_metadata();
    println!("  Collections: {}", metadata.collections.len());

    println!("\n6. Closing database...");
    db.close()?;

    println!("\n=== Example Complete ===");
    println!("\nNote: This is a simplified example showing core functionality.");
    println!("Full collection API with B-tree integration is a work in progress.");

    std::fs::remove_file("example.db").ok();
    std::fs::remove_file("example.db.lock").ok();
    std::fs::remove_file("example.db-wal").ok();

    Ok(())
}
