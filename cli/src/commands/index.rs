use anyhow::Result;
use crate::commands::CommandContext;
use crate::formatter::{print_success, format_json};
use serde_json::json;

pub fn list(ctx: &CommandContext, collection: &str) -> Result<()> {
    let db_info = ctx.db.info()?;

    let collection_info = db_info
        .collections
        .iter()
        .find(|c| c.name == collection)
        .ok_or_else(|| anyhow::anyhow!("Collection '{}' not found", collection))?;

    let indexes_json = json!(
        collection_info.indexes.iter().map(|idx| {
            json!({
                "name": idx.name,
                "fields": idx.fields,
                "unique": idx.unique,
            })
        }).collect::<Vec<_>>()
    );

    println!("{}", format_json(&indexes_json, &ctx.format)?);
    Ok(())
}

pub fn create(ctx: &CommandContext, collection: &str, field: &str, unique: bool) -> Result<()> {
    let index_name = format!("{}_idx", field);

    ctx.db.create_index(collection, &index_name, field, unique)?;

    let index_type = if unique { "unique index" } else { "index" };
    print_success(&format!("Created {} on field '{}' in collection '{}'", index_type, field, collection));
    Ok(())
}

pub fn drop(ctx: &CommandContext, collection: &str, name: &str) -> Result<()> {
    ctx.db.drop_index(collection, name)?;

    print_success(&format!("Dropped index '{}' from collection '{}'", name, collection));
    Ok(())
}
