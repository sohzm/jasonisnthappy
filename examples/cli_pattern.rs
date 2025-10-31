/// Example: How to use CLI args to control web UI
/// This shows the pattern for your future CLI binary

use jasonisnthappy::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse CLI arguments
    let args: Vec<String> = std::env::args().collect();

    // Check for --no-web-ui flag
    let enable_web_ui = !args.contains(&"--no-web-ui".to_string());
    let db_path = args.get(1).map(|s| s.as_str()).unwrap_or("my.db");

    println!("Opening database: {}", db_path);
    let db = Database::open(db_path)?;

    // Conditionally start web UI based on CLI flag
    #[cfg(feature = "web-ui")]
    let _web_server = if enable_web_ui {
        println!("Starting web UI at http://127.0.0.1:8080");
        println!("  (use --no-web-ui to disable)");
        Some(db.start_web_ui("127.0.0.1:8080")?)
    } else {
        println!("Web UI disabled (--no-web-ui flag detected)");
        None
    };

    // Show metrics regardless
    let metrics = db.metrics();
    println!("\nCurrent metrics:");
    println!("  Transactions: {}", metrics.transactions_committed);
    println!("  Cache hit rate: {:.1}%", metrics.cache_hit_rate * 100.0);

    #[cfg(feature = "web-ui")]
    if enable_web_ui {
        println!("\nWeb UI running. Press Ctrl+C to stop...");
        std::thread::park();
    }

    Ok(())
}
