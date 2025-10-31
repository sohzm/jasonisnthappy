use anyhow::Result;
use crate::commands::CommandContext;
use crate::formatter::{print_success, print_info, format_json};
use crate::utils::parse_json;
use serde_json::json;

pub fn set(ctx: &CommandContext, collection: &str, schema_str: &str) -> Result<()> {
    let schema_value = parse_json(schema_str)?;
    let schema: jasonisnthappy::Schema = serde_json::from_value(schema_value)?;

    ctx.db.set_schema(collection, schema)?;

    print_success(&format!("Schema set for collection '{}'", collection));
    Ok(())
}

pub fn get(ctx: &CommandContext, collection: &str) -> Result<()> {
    // Schema information is stored in metadata
    let metadata = ctx.db.get_metadata();

    if let Some(coll_meta) = metadata.collections.get(collection) {
        if let Some(schema) = &coll_meta.schema {
            let schema_json = serde_json::to_value(schema)?;
            println!("{}", format_json(&schema_json, &ctx.format)?);
        } else {
            print_info(&format!("No schema set for collection '{}'", collection));
        }
    } else {
        anyhow::bail!("Collection '{}' not found", collection);
    }

    Ok(())
}

pub fn validate(ctx: &CommandContext, collection: &str) -> Result<()> {
    print_info(&format!("Validating all documents in collection '{}'...", collection));

    // Get the collection's schema
    let metadata = ctx.db.get_metadata();
    let coll_meta = metadata.collections.get(collection)
        .ok_or_else(|| anyhow::anyhow!("Collection '{}' not found", collection))?;

    if coll_meta.schema.is_none() {
        print_info(&format!("No schema set for collection '{}'", collection));
        return Ok(());
    }

    let schema = coll_meta.schema.as_ref().unwrap();

    // Get all documents
    let coll = ctx.db.collection(collection);
    let docs = coll.find_all()?;

    let mut valid_count = 0;
    let mut invalid_count = 0;
    let mut errors = Vec::new();

    for (idx, doc) in docs.iter().enumerate() {
        match schema.validate(doc) {
            Ok(_) => valid_count += 1,
            Err(e) => {
                invalid_count += 1;
                errors.push(json!({
                    "document_index": idx,
                    "error": e.to_string(),
                }));
            }
        }
    }

    let result = json!({
        "total": docs.len(),
        "valid": valid_count,
        "invalid": invalid_count,
        "errors": errors,
    });

    println!("{}", format_json(&result, &ctx.format)?);

    if invalid_count > 0 {
        print_info(&format!("Found {} invalid document(s)", invalid_count));
    } else {
        print_success("All documents are valid");
    }

    Ok(())
}
