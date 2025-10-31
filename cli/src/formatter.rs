use anyhow::Result;
use colored::Colorize;
use comfy_table::{Table, Cell, Color, Attribute};
use serde_json::Value;

pub enum OutputFormat {
    Json,
    Pretty,
    Table,
}

impl OutputFormat {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => OutputFormat::Json,
            "table" => OutputFormat::Table,
            _ => OutputFormat::Pretty,
        }
    }
}

pub fn format_json(value: &Value, format: &OutputFormat) -> Result<String> {
    match format {
        OutputFormat::Json => Ok(serde_json::to_string(value)?),
        OutputFormat::Pretty => {
            let json_str = serde_json::to_string_pretty(value)?;
            Ok(colored_json::to_colored_json_auto(&json_str)?.to_string())
        }
        OutputFormat::Table => {
            // For table format, treat as array of objects
            if let Value::Array(arr) = value {
                format_table(arr)
            } else if let Value::Object(_) = value {
                format_table(&vec![value.clone()])
            } else {
                Ok(serde_json::to_string_pretty(value)?)
            }
        }
    }
}

pub fn format_table(docs: &[Value]) -> Result<String> {
    if docs.is_empty() {
        return Ok("No documents found".to_string());
    }

    let mut table = Table::new();
    table.load_preset(comfy_table::presets::UTF8_FULL);
    table.apply_modifier(comfy_table::modifiers::UTF8_ROUND_CORNERS);

    // Get all unique keys from all documents
    let mut keys = std::collections::HashSet::new();
    for doc in docs {
        if let Value::Object(map) = doc {
            for key in map.keys() {
                keys.insert(key.clone());
            }
        }
    }
    let mut keys: Vec<_> = keys.into_iter().collect();
    keys.sort();

    // Add header
    let header: Vec<Cell> = keys
        .iter()
        .map(|k| Cell::new(k).fg(Color::Cyan).add_attribute(Attribute::Bold))
        .collect();
    table.set_header(header);

    // Add rows
    for doc in docs {
        if let Value::Object(map) = doc {
            let row: Vec<Cell> = keys
                .iter()
                .map(|key| {
                    let val = map.get(key).unwrap_or(&Value::Null);
                    Cell::new(format_value_compact(val))
                })
                .collect();
            table.add_row(row);
        }
    }

    Ok(table.to_string())
}

fn format_value_compact(val: &Value) -> String {
    match val {
        Value::Null => "null".dimmed().to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(arr) => format!("[{} items]", arr.len()),
        Value::Object(obj) => format!("{{{} fields}}", obj.len()),
    }
}

pub fn print_success(msg: &str) {
    println!("{} {}", "✓".green().bold(), msg);
}

pub fn print_error(msg: &str) {
    eprintln!("{} {}", "✗".red().bold(), msg);
}

pub fn print_info(msg: &str) {
    println!("{} {}", "ℹ".blue().bold(), msg);
}

pub fn print_warning(msg: &str) {
    println!("{} {}", "⚠".yellow().bold(), msg);
}
