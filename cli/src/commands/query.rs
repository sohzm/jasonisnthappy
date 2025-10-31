use anyhow::Result;
use crate::commands::CommandContext;
use crate::formatter::format_json;
use crate::utils::parse_json_array;
use serde_json::Value;
use jasonisnthappy::SortOrder;

pub fn run(
    ctx: &CommandContext,
    collection: &str,
    filter: &str,
    sort: Option<&str>,
    order: &str,
    limit: Option<usize>,
    skip: Option<usize>,
) -> Result<()> {
    let coll = ctx.db.collection(collection);

    let mut builder = coll.query();

    if filter != "{}" {
        builder = builder.filter(filter);
    }

    if let Some(sort_field) = sort {
        let sort_order = match order.to_lowercase().as_str() {
            "desc" | "descending" => SortOrder::Desc,
            _ => SortOrder::Asc,
        };
        builder = builder.sort_by(sort_field, sort_order);
    }

    if let Some(limit) = limit {
        builder = builder.limit(limit);
    }

    if let Some(skip) = skip {
        builder = builder.skip(skip);
    }

    let results = builder.execute()?;

    let results_json = Value::Array(results);
    println!("{}", format_json(&results_json, &ctx.format)?);

    Ok(())
}

pub fn aggregate(ctx: &CommandContext, collection: &str, _pipeline: &str) -> Result<()> {
    // Note: The aggregation pipeline API uses builder methods like match_(), group_by(), etc.
    // For now, this is a simplified implementation
    let coll = ctx.db.collection(collection);

    // Execute a simple aggregation (group all documents)
    let results = coll.aggregate()
        .execute()?;

    let results_json = Value::Array(results);
    println!("{}", format_json(&results_json, &ctx.format)?);

    anyhow::bail!("Advanced aggregation pipelines are not yet supported in the CLI. Use the database API directly for complex aggregations.")
}

pub fn search(ctx: &CommandContext, collection: &str, text: &str, limit: Option<usize>) -> Result<()> {
    let coll = ctx.db.collection(collection);

    // Search returns Vec<SearchResult>, limit is handled internally
    let results = coll.search(text)?;

    // Apply limit if specified
    let limited_results: Vec<_> = if let Some(limit) = limit {
        results.into_iter().take(limit).collect()
    } else {
        results
    };

    let results_json = Value::Array(
        limited_results
            .into_iter()
            .map(|r| {
                serde_json::json!({
                    "score": r.score,
                    "doc_id": r.doc_id,
                })
            })
            .collect(),
    );

    println!("{}", format_json(&results_json, &ctx.format)?);

    Ok(())
}
