use anyhow::Result;
use crate::commands::CommandContext;
use crate::formatter::{print_success, print_info, format_json};
use serde_json::json;

pub fn list(ctx: &CommandContext) -> Result<()> {
    let db_info = ctx.db.info()?;

    let collections_json = json!(
        db_info.collections.iter().map(|c| {
            json!({
                "name": c.name,
                "document_count": c.document_count,
                "index_count": c.indexes.len(),
            })
        }).collect::<Vec<_>>()
    );

    println!("{}", format_json(&collections_json, &ctx.format)?);
    Ok(())
}

pub fn create(ctx: &CommandContext, name: &str) -> Result<()> {
    // Creating a collection is implicit - just ensure it exists
    let _collection = ctx.db.collection(name);

    print_success(&format!("Collection '{}' created", name));
    Ok(())
}

pub fn drop(_ctx: &CommandContext, name: &str) -> Result<()> {
    // Note: jasonisnthappy doesn't support dropping collections
    // Collections are implicitly created and persist in metadata
    anyhow::bail!("Dropping collections is not supported. Collection '{}' will remain in metadata.", name)
}

pub fn info(ctx: &CommandContext, name: &str) -> Result<()> {
    let db_info = ctx.db.info()?;

    let collection_info = db_info
        .collections
        .iter()
        .find(|c| c.name == name)
        .ok_or_else(|| anyhow::anyhow!("Collection '{}' not found", name))?;

    let info_json = json!({
        "name": collection_info.name,
        "document_count": collection_info.document_count,
        "indexes": collection_info.indexes.iter().map(|idx| {
            json!({
                "name": idx.name,
                "fields": idx.fields,
                "unique": idx.unique,
            })
        }).collect::<Vec<_>>(),
    });

    println!("{}", format_json(&info_json, &ctx.format)?);
    Ok(())
}
