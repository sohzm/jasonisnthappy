
use jasonisnthappy::core::database::Database;
use serde_json::json;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Collection CRUD Example ===\n");

    let _ = std::fs::remove_file("crud_example.db");
    let _ = std::fs::remove_file("crud_example.db.lock");
    let _ = std::fs::remove_file("crud_example.db-wal");

    let db = Arc::new(Database::open("crud_example.db")?);
    let users = db.collection("users");

    println!("1. Inserting documents...");
    let alice = json!({"name": "Alice", "age": 30, "city": "New York"});
    let bob = json!({"name": "Bob", "age": 25, "city": "San Francisco"});
    let charlie = json!({"name": "Charlie", "age": 35, "city": "Seattle"});

    let id1 = users.insert(alice)?;
    let id2 = users.insert(bob)?;
    let id3 = users.insert(charlie)?;

    println!("   Inserted Alice: {}", id1);
    println!("   Inserted Bob: {}", id2);
    println!("   Inserted Charlie: {}", id3);

    println!("\n2. Counting documents...");
    let count = users.count()?;
    println!("   Total documents: {}", count);

    println!("\n3. Finding document by ID...");
    let found = users.find_by_id(&id1)?;
    println!("   Found: {}", serde_json::to_string_pretty(&found)?);

    println!("\n4. Finding all documents...");
    let all_users = users.find_all()?;
    for user in &all_users {
        println!("   - {}", user["name"]);
    }

    println!("\n5. Updating Alice's age...");
    users.update_by_id(&id1, json!({"age": 31}))?;
    let updated = users.find_by_id(&id1)?;
    println!("   Updated age: {}", updated["age"]);

    println!("\n6. Deleting Bob...");
    users.delete_by_id(&id2)?;
    let count_after_delete = users.count()?;
    println!("   Documents after delete: {}", count_after_delete);

    println!("\n7. Remaining documents:");
    let remaining = users.find_all()?;
    for user in &remaining {
        println!("   - {} (age {})", user["name"], user["age"]);
    }

    db.close()?;

    println!("\n8. Cleaning up...");
    std::fs::remove_file("crud_example.db")?;
    std::fs::remove_file("crud_example.db.lock")?;
    std::fs::remove_file("crud_example.db-wal")?;

    println!("\n=== Example Complete ===");

    Ok(())
}
