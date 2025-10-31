use anyhow::Result;
use crate::commands::CommandContext;
use crate::formatter::{print_success, print_info, format_json};
use serde_json::json;

pub fn info(ctx: &CommandContext) -> Result<()> {
    let db_info = ctx.db.info()?;
    let metrics = ctx.db.metrics();

    let info_json = json!({
        "collections": db_info.collections.iter().map(|c| {
            json!({
                "name": c.name,
                "document_count": c.document_count,
                "indexes": c.indexes.iter().map(|idx| {
                    json!({
                        "name": idx.name,
                        "fields": idx.fields,
                        "unique": idx.unique,
                    })
                }).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
        "total_documents": db_info.total_documents,
        "metrics": {
            "transactions_committed": metrics.transactions_committed,
            "transactions_aborted": metrics.transactions_aborted,
            "cache_hit_rate": format!("{:.2}%", metrics.cache_hit_rate * 100.0),
            "wal_writes": metrics.wal_writes,
            "cache_hits": metrics.cache_hits,
            "cache_misses": metrics.cache_misses,
        }
    });

    println!("{}", format_json(&info_json, &ctx.format)?);
    Ok(())
}

pub fn backup(ctx: &CommandContext, destination: &str) -> Result<()> {
    print_info(&format!("Creating backup to '{}'...", destination));

    ctx.db.backup(destination)?;

    print_success("Backup created successfully");
    Ok(())
}

pub fn compact(ctx: &CommandContext) -> Result<()> {
    print_info("Compacting database...");

    ctx.db.checkpoint()?;

    print_success("Database compacted successfully");
    Ok(())
}
