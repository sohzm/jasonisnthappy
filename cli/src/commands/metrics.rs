use anyhow::Result;
use crate::commands::CommandContext;
use crate::formatter::{print_info, format_json};
use serde_json::json;
use std::time::Duration;

pub fn show(ctx: &CommandContext) -> Result<()> {
    let metrics = ctx.db.metrics();

    let metrics_json = json!({
        "transactions": {
            "committed": metrics.transactions_committed,
            "aborted": metrics.transactions_aborted,
            "active": metrics.active_transactions,
        },
        "cache": {
            "hits": metrics.cache_hits,
            "misses": metrics.cache_misses,
            "hit_rate": format!("{:.2}%", metrics.cache_hit_rate * 100.0),
        },
        "storage": {
            "pages_allocated": metrics.pages_allocated,
            "pages_freed": metrics.pages_freed,
            "dirty_pages": metrics.dirty_pages,
        },
        "wal": {
            "writes": metrics.wal_writes,
            "bytes_written": metrics.wal_bytes_written,
            "checkpoints": metrics.checkpoints,
        },
    });

    println!("{}", format_json(&metrics_json, &ctx.format)?);
    Ok(())
}

pub fn watch(ctx: &CommandContext, collection: &str) -> Result<()> {
    print_info(&format!("Watching collection '{}' for changes... (Press Ctrl+C to stop)", collection));

    let coll = ctx.db.collection(collection);

    let (_handle, receiver) = coll.watch().subscribe()?;

    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(event) => {
                let event_json = json!({
                    "collection": event.collection,
                    "operation": format!("{:?}", event.operation),
                    "doc_id": event.doc_id,
                    "document": event.document,
                });

                println!("{}", format_json(&event_json, &crate::formatter::OutputFormat::Pretty)?);
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                // No event, continue waiting
                continue;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("Watch stream disconnected");
            }
        }
    }
}
