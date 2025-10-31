use anyhow::{Result, Context};
use serde_json::Value;

pub fn parse_json(input: &str) -> Result<Value> {
    serde_json::from_str(input).context("Failed to parse JSON")
}

pub fn parse_json_object(input: &str) -> Result<serde_json::Map<String, Value>> {
    let value = parse_json(input)?;
    match value {
        Value::Object(obj) => Ok(obj),
        _ => anyhow::bail!("Expected JSON object, got {}", value),
    }
}

pub fn parse_json_array(input: &str) -> Result<Vec<Value>> {
    let value = parse_json(input)?;
    match value {
        Value::Array(arr) => Ok(arr),
        _ => anyhow::bail!("Expected JSON array, got {}", value),
    }
}
