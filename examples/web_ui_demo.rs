use jasonisnthappy::Database;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Cleanup
    let _ = std::fs::remove_file("web_demo.db");
    let _ = std::fs::remove_file("web_demo.db.lock");
    let _ = std::fs::remove_file("web_demo.db-wal");

    println!("🚀 Starting Web UI Demo\n");

    let db = Database::open("web_demo.db")?;

    // Create diverse sample data for testing search & pagination
    println!("📝 Inserting sample data...");

    // === USERS COLLECTION (50 documents) ===
    let mut tx = db.begin()?;
    let mut users = tx.collection("users")?;

    let first_names = ["alice", "bob", "charlie", "diana", "ethan", "fiona", "george", "hannah", "ivan", "julia"];
    let roles = ["admin", "user", "moderator", "guest"];
    let departments = ["engineering", "sales", "marketing", "support", "hr"];

    for i in 1..=50 {
        let fname_idx = (i - 1) % first_names.len();
        let name = format!("{}{}", first_names[fname_idx], i);
        users.insert(json!({
            "username": name,
            "email": format!("{}@company.com", name),
            "full_name": format!("{} {}", first_names[fname_idx].to_uppercase(), format!("User{}", i)),
            "age": 22 + (i % 35),
            "role": roles[i % roles.len()],
            "department": departments[i % departments.len()],
            "active": i % 4 != 0,
            "credits": i * 150,
            "joined_at": format!("2024-{:02}-{:02}", 1 + (i % 12), 1 + (i % 28)),
            "preferences": {
                "theme": if i % 2 == 0 { "dark" } else { "light" },
                "notifications": i % 3 == 0,
                "language": if i % 5 == 0 { "es" } else { "en" }
            }
        }))?;
    }
    tx.commit()?;

    // === PRODUCTS COLLECTION (40 documents) ===
    let mut tx = db.begin()?;
    let mut products = tx.collection("products")?;

    let product_names = [
        "wireless mouse", "mechanical keyboard", "laptop stand", "usb cable", "monitor",
        "headphones", "webcam", "microphone", "desk lamp", "ergonomic chair",
        "notebook", "pen set", "coffee mug", "water bottle", "backpack"
    ];
    let categories = ["electronics", "accessories", "furniture", "office supplies"];

    for i in 1..=40 {
        let base_name = product_names[(i - 1) % product_names.len()];
        products.insert(json!({
            "name": format!("{} {}", base_name, if i > 15 { "pro" } else { "standard" }),
            "sku": format!("SKU-{:05}", i * 100),
            "price": (i as f64) * 12.99 + 5.0,
            "cost": (i as f64) * 7.50,
            "in_stock": i % 3 != 0,
            "quantity": i * 10,
            "category": categories[i % categories.len()],
            "tags": vec![
                if i % 2 == 0 { "popular" } else { "new" },
                if i % 3 == 0 { "sale" } else { "featured" }
            ],
            "rating": 3.0 + ((i % 5) as f64) * 0.5,
            "reviews_count": i * 3,
            "supplier": format!("Supplier-{}", (i % 5) + 1),
        }))?;
    }
    tx.commit()?;

    // === ORDERS COLLECTION (30 documents) ===
    let mut tx = db.begin()?;
    let mut orders = tx.collection("orders")?;

    let statuses = ["pending", "processing", "shipped", "delivered", "cancelled"];

    for i in 1..=30 {
        let num_items = (i % 4) + 1;
        let mut items = vec![];
        for j in 1..=num_items {
            items.push(json!({
                "product_id": format!("SKU-{:05}", ((i + j) % 40) * 100),
                "quantity": j,
                "price": (j as f64) * 29.99,
            }));
        }

        orders.insert(json!({
            "order_id": format!("ORD-{:08}", i * 1000),
            "customer_id": format!("user_{}", (i % 50) + 1),
            "status": statuses[i % statuses.len()],
            "items": items,
            "total": (i as f64) * 47.99,
            "shipping_address": {
                "street": format!("{} Main Street", i * 100),
                "city": if i % 3 == 0 { "New York" } else if i % 3 == 1 { "Los Angeles" } else { "Chicago" },
                "zip": format!("{:05}", 10000 + i * 100),
                "country": "USA"
            },
            "created_at": format!("2024-{:02}-{:02}T{:02}:00:00Z", 1 + (i % 12), 1 + (i % 28), i % 24),
            "notes": if i % 5 == 0 { "express shipping requested" } else { "" },
        }))?;
    }
    tx.commit()?;

    // === BLOG_POSTS COLLECTION (25 documents) ===
    let mut tx = db.begin()?;
    let mut posts = tx.collection("blog_posts")?;

    let topics = ["technology", "productivity", "design", "business", "tutorial"];
    let authors = ["alice", "bob", "charlie", "diana", "ethan"];

    for i in 1..=25 {
        let topic = topics[i % topics.len()];
        posts.insert(json!({
            "title": format!("how to improve your {} skills in 2024", topic),
            "slug": format!("{}-skills-{}", topic, i),
            "author": authors[i % authors.len()],
            "content": format!("This is a comprehensive guide about {}. It covers best practices, tips, and tricks. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.", topic),
            "excerpt": format!("Learn about {} with this detailed guide", topic),
            "tags": vec![topic, if i % 2 == 0 { "featured" } else { "tutorial" }, "2024"],
            "published": i % 5 != 0,
            "views": i * 123,
            "likes": i * 45,
            "comments_count": i * 7,
            "published_at": format!("2024-{:02}-{:02}T10:00:00Z", 1 + (i % 12), 1 + (i % 28)),
            "seo": {
                "meta_description": format!("comprehensive guide to {}", topic),
                "keywords": vec![topic, "guide", "2024", "tutorial"]
            }
        }))?;
    }
    tx.commit()?;

    println!("✅ Created 4 collections:");
    println!("   - users: 50 documents");
    println!("   - products: 40 documents");
    println!("   - orders: 30 documents");
    println!("   - blog_posts: 25 documents");
    println!("   Total: 145 documents\n");

    // Start web UI
    println!("🌐 Starting Web UI Server...\n");
    println!("╔════════════════════════════════════════════╗");
    println!("║  Web UI: http://127.0.0.1:8080           ║");
    println!("╠════════════════════════════════════════════╣");
    println!("║  • Click collections to view documents    ║");
    println!("║  • Metrics auto-refresh every 5 seconds   ║");
    println!("║  • Press ESC to close document viewer     ║");
    println!("║  • Press Ctrl+C to stop server            ║");
    println!("╚════════════════════════════════════════════╝\n");

    let _web_server = db.start_web_ui("127.0.0.1:8080")?;

    println!("✨ Server running! Open http://127.0.0.1:8080 in your browser\n");

    // Keep server alive
    std::thread::park();

    Ok(())
}
