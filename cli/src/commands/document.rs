use anyhow::Result;
use crate::commands::CommandContext;
use crate::formatter::{print_success, print_info, format_json};
use crate::utils::parse_json;
use serde_json::{json, Value};
use std::fs;
use indicatif::{ProgressBar, ProgressStyle};

pub fn insert(ctx: &CommandContext, collection: &str, document: &str) -> Result<()> {
    let doc = parse_json(document)?;

    let coll = ctx.db.collection(collection);
    let id = coll.insert(doc)?;

    print_success(&format!("Document inserted with ID: {}", id));
    Ok(())
}

pub fn find(
    ctx: &CommandContext,
    collection: &str,
    query: &str,
    limit: Option<usize>,
    skip: Option<usize>,
) -> Result<()> {
    let coll = ctx.db.collection(collection);

    // If query is empty, use find_all, otherwise use find with query language
    let results = if query.is_empty() || query == "{}" {
        let all_docs = coll.find_all()?;

        // Apply limit/skip manually if needed
        let docs = if let Some(skip_val) = skip {
            all_docs.into_iter().skip(skip_val).collect::<Vec<_>>()
        } else {
            all_docs
        };

        if let Some(limit_val) = limit {
            docs.into_iter().take(limit_val).collect()
        } else {
            docs
        }
    } else {
        // Use query language (e.g., "age > 30")
        let mut all_results = coll.find(query)?;

        // Apply limit/skip manually if needed
        if let Some(skip_val) = skip {
            all_results = all_results.into_iter().skip(skip_val).collect();
        }

        if let Some(limit_val) = limit {
            all_results.into_iter().take(limit_val).collect()
        } else {
            all_results
        }
    };

    let results_json = Value::Array(results);
    println!("{}", format_json(&results_json, &ctx.format)?);

    Ok(())
}

pub fn find_one(ctx: &CommandContext, collection: &str, query: &str) -> Result<()> {
    let coll = ctx.db.collection(collection);

    let result = if query.is_empty() || query == "{}" {
        // Get first document
        let docs = coll.find_all()?;
        docs.into_iter().next()
    } else {
        coll.find_one(query)?
    };

    if let Some(doc) = result {
        println!("{}", format_json(&doc, &ctx.format)?);
    } else {
        print_info("No document found");
    }

    Ok(())
}

pub fn update(ctx: &CommandContext, collection: &str, query: &str, update: &str) -> Result<()> {
    let update_doc = parse_json(update)?;

    let coll = ctx.db.collection(collection);
    let count = coll.update(query, update_doc)?;

    print_success(&format!("Updated {} document(s)", count));
    Ok(())
}

pub fn delete(ctx: &CommandContext, collection: &str, query: &str) -> Result<()> {
    let coll = ctx.db.collection(collection);
    let count = coll.delete(query)?;

    print_success(&format!("Deleted {} document(s)", count));
    Ok(())
}

pub fn count(ctx: &CommandContext, collection: &str, query: &str) -> Result<()> {
    let coll = ctx.db.collection(collection);
    let count = if query.is_empty() || query == "{}" {
        coll.count()?
    } else {
        // Use find and count results for query language
        coll.find(query)?.len()
    };

    println!("{}", format_json(&json!({ "count": count }), &ctx.format)?);
    Ok(())
}

pub fn export(ctx: &CommandContext, collection: &str, output: &str) -> Result<()> {
    print_info(&format!("Exporting collection '{}' to '{}'...", collection, output));

    let coll = ctx.db.collection(collection);

    // Find all documents
    let docs = coll.find_all()?;

    // Create progress bar
    let pb = ProgressBar::new(docs.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")?
            .progress_chars("=>-"),
    );

    // Write to file
    let json_str = serde_json::to_string_pretty(&docs)?;
    fs::write(output, json_str)?;

    pb.finish_with_message("done");

    print_success(&format!("Exported {} document(s) to '{}'", docs.len(), output));
    Ok(())
}

pub fn import(ctx: &CommandContext, collection: &str, input: &str) -> Result<()> {
    print_info(&format!("Importing from '{}' to collection '{}'...", input, collection));

    let json_str = fs::read_to_string(input)?;
    let docs: Vec<Value> = serde_json::from_str(&json_str)?;

    // Create progress bar
    let pb = ProgressBar::new(docs.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")?
            .progress_chars("=>-"),
    );

    let coll = ctx.db.collection(collection);

    let ids = coll.insert_many(docs)?;
    pb.finish_with_message("done");

    print_success(&format!("Imported {} document(s)", ids.len()));
    Ok(())
}
