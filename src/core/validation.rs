
use crate::core::errors::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

fn reserved_collection_names() -> HashSet<&'static str> {
    let mut set = HashSet::new();
    set.insert("_metadata");
    set.insert("_internal");
    set.insert("_system");
    set
}

pub fn validate_collection_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::CollectionNameEmpty);
    }

    if name.len() > 64 {
        return Err(Error::CollectionNameTooLong);
    }

    let first_char = name.chars().next()
        .ok_or(Error::CollectionNameEmpty)?;
    if !first_char.is_alphabetic() && first_char != '_' {
        return Err(Error::CollectionNameInvalidStart);
    }

    for ch in name.chars() {
        if !ch.is_alphanumeric() && ch != '_' {
            return Err(Error::CollectionNameInvalidChar);
        }
    }

    if reserved_collection_names().contains(name) {
        return Err(Error::CollectionNameReserved);
    }

    Ok(())
}

// ==================== Schema Validation ====================

/// JSON Schema for document validation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    /// Type of the value
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub value_type: Option<ValueType>,

    /// Required fields (for objects)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<Vec<String>>,

    /// Properties schema (for objects)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, Schema>>,

    /// Schema for array items
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Box<Schema>>,

    /// Minimum value (for numbers)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum: Option<f64>,

    /// Maximum value (for numbers)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum: Option<f64>,

    /// Minimum length (for strings and arrays)
    #[serde(rename = "minLength", skip_serializing_if = "Option::is_none")]
    pub min_length: Option<usize>,

    /// Maximum length (for strings and arrays)
    #[serde(rename = "maxLength", skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,

    /// Allowed values (enum)
    #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ValueType {
    String,
    Number,
    Integer,
    Boolean,
    Object,
    Array,
    Null,
}

impl Schema {
    /// Create a new empty schema
    pub fn new() -> Self {
        Self {
            value_type: None,
            required: None,
            properties: None,
            items: None,
            minimum: None,
            maximum: None,
            min_length: None,
            max_length: None,
            enum_values: None,
        }
    }

    /// Validate a document against this schema
    pub fn validate(&self, value: &Value) -> Result<()> {
        self.validate_with_path(value, "")
    }

    fn validate_with_path(&self, value: &Value, path: &str) -> Result<()> {
        // Type validation
        if let Some(ref expected_type) = self.value_type {
            self.validate_type(value, expected_type, path)?;
        }

        // Enum validation
        if let Some(ref allowed_values) = self.enum_values {
            if !allowed_values.contains(value) {
                return Err(Error::SchemaValidation(format!(
                    "Value at '{}' must be one of {:?}, got: {}",
                    path, allowed_values, value
                )));
            }
        }

        match value {
            Value::Object(obj) => {
                // Required fields validation
                if let Some(ref required) = self.required {
                    for field in required {
                        if !obj.contains_key(field) {
                            return Err(Error::SchemaValidation(format!(
                                "Missing required field '{}' at '{}'",
                                field, path
                            )));
                        }
                    }
                }

                // Properties validation
                if let Some(ref properties) = self.properties {
                    for (key, val) in obj {
                        if let Some(prop_schema) = properties.get(key) {
                            let new_path = if path.is_empty() {
                                key.clone()
                            } else {
                                format!("{}.{}", path, key)
                            };
                            prop_schema.validate_with_path(val, &new_path)?;
                        }
                    }
                }
            }
            Value::Array(arr) => {
                // Min/max length validation for arrays
                if let Some(min) = self.min_length {
                    if arr.len() < min {
                        return Err(Error::SchemaValidation(format!(
                            "Array at '{}' length {} is less than minimum {}",
                            path,
                            arr.len(),
                            min
                        )));
                    }
                }
                if let Some(max) = self.max_length {
                    if arr.len() > max {
                        return Err(Error::SchemaValidation(format!(
                            "Array at '{}' length {} exceeds maximum {}",
                            path,
                            arr.len(),
                            max
                        )));
                    }
                }

                // Items validation
                if let Some(ref items_schema) = self.items {
                    for (idx, item) in arr.iter().enumerate() {
                        let new_path = format!("{}[{}]", path, idx);
                        items_schema.validate_with_path(item, &new_path)?;
                    }
                }
            }
            Value::String(s) => {
                // Min/max length validation for strings
                if let Some(min) = self.min_length {
                    if s.len() < min {
                        return Err(Error::SchemaValidation(format!(
                            "String at '{}' length {} is less than minimum {}",
                            path,
                            s.len(),
                            min
                        )));
                    }
                }
                if let Some(max) = self.max_length {
                    if s.len() > max {
                        return Err(Error::SchemaValidation(format!(
                            "String at '{}' length {} exceeds maximum {}",
                            path,
                            s.len(),
                            max
                        )));
                    }
                }
            }
            Value::Number(n) => {
                let num_val = n.as_f64().unwrap_or(0.0);

                // Minimum validation
                if let Some(min) = self.minimum {
                    if num_val < min {
                        return Err(Error::SchemaValidation(format!(
                            "Number at '{}' ({}) is less than minimum {}",
                            path, num_val, min
                        )));
                    }
                }

                // Maximum validation
                if let Some(max) = self.maximum {
                    if num_val > max {
                        return Err(Error::SchemaValidation(format!(
                            "Number at '{}' ({}) exceeds maximum {}",
                            path, num_val, max
                        )));
                    }
                }

                // Integer validation
                if let Some(ValueType::Integer) = self.value_type {
                    if !n.is_i64() && !n.is_u64() {
                        return Err(Error::SchemaValidation(format!(
                            "Value at '{}' must be an integer, got: {}",
                            path, n
                        )));
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn validate_type(&self, value: &Value, expected: &ValueType, path: &str) -> Result<()> {
        let matches = match (expected, value) {
            (ValueType::String, Value::String(_)) => true,
            (ValueType::Number, Value::Number(_)) => true,
            (ValueType::Integer, Value::Number(n)) => n.is_i64() || n.is_u64(),
            (ValueType::Boolean, Value::Bool(_)) => true,
            (ValueType::Object, Value::Object(_)) => true,
            (ValueType::Array, Value::Array(_)) => true,
            (ValueType::Null, Value::Null) => true,
            _ => false,
        };

        if !matches {
            return Err(Error::SchemaValidation(format!(
                "Type mismatch at '{}': expected {:?}, got {}",
                path,
                expected,
                type_name(value)
            )));
        }

        Ok(())
    }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "boolean",
        Value::Object(_) => "object",
        Value::Array(_) => "array",
        Value::Null => "null",
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_names() {
        assert!(validate_collection_name("users").is_ok());
        assert!(validate_collection_name("Users").is_ok());
        assert!(validate_collection_name("user_profiles").is_ok());
        assert!(validate_collection_name("_temp").is_ok());
        assert!(validate_collection_name("collection123").is_ok());
        assert!(validate_collection_name("a").is_ok());
    }

    #[test]
    fn test_empty_name() {
        assert!(matches!(
            validate_collection_name(""),
            Err(Error::CollectionNameEmpty)
        ));
    }

    #[test]
    fn test_too_long() {
        let long_name = "a".repeat(65);
        assert!(matches!(
            validate_collection_name(&long_name),
            Err(Error::CollectionNameTooLong)
        ));
    }

    #[test]
    fn test_invalid_start() {
        assert!(matches!(
            validate_collection_name("123users"),
            Err(Error::CollectionNameInvalidStart)
        ));
        assert!(matches!(
            validate_collection_name("-users"),
            Err(Error::CollectionNameInvalidStart)
        ));
    }

    #[test]
    fn test_invalid_chars() {
        assert!(matches!(
            validate_collection_name("user-profiles"),
            Err(Error::CollectionNameInvalidChar)
        ));
        assert!(matches!(
            validate_collection_name("user.profiles"),
            Err(Error::CollectionNameInvalidChar)
        ));
        assert!(matches!(
            validate_collection_name("user profiles"),
            Err(Error::CollectionNameInvalidChar)
        ));
        assert!(matches!(
            validate_collection_name("user@profiles"),
            Err(Error::CollectionNameInvalidChar)
        ));
    }

    #[test]
    fn test_reserved_names() {
        assert!(matches!(
            validate_collection_name("_metadata"),
            Err(Error::CollectionNameReserved)
        ));
        assert!(matches!(
            validate_collection_name("_internal"),
            Err(Error::CollectionNameReserved)
        ));
        assert!(matches!(
            validate_collection_name("_system"),
            Err(Error::CollectionNameReserved)
        ));
    }

    // ========== Schema Validation Tests ==========

    use serde_json::json;

    #[test]
    fn test_type_validation() {
        let mut schema = Schema::new();
        schema.value_type = Some(ValueType::String);

        assert!(schema.validate(&json!("hello")).is_ok());
        assert!(schema.validate(&json!(42)).is_err());
        assert!(schema.validate(&json!(true)).is_err());
    }

    #[test]
    fn test_required_fields() {
        let mut schema = Schema::new();
        schema.value_type = Some(ValueType::Object);
        schema.required = Some(vec!["name".to_string(), "email".to_string()]);

        assert!(schema.validate(&json!({"name": "Alice", "email": "alice@example.com"})).is_ok());
        assert!(schema.validate(&json!({"name": "Alice"})).is_err());
        assert!(schema.validate(&json!({"email": "alice@example.com"})).is_err());
    }

    #[test]
    fn test_property_validation() {
        let mut schema = Schema::new();
        schema.value_type = Some(ValueType::Object);

        let mut name_schema = Schema::new();
        name_schema.value_type = Some(ValueType::String);
        name_schema.min_length = Some(1);

        let mut properties = HashMap::new();
        properties.insert("name".to_string(), name_schema);
        schema.properties = Some(properties);

        assert!(schema.validate(&json!({"name": "Alice"})).is_ok());
        assert!(schema.validate(&json!({"name": ""})).is_err());
        assert!(schema.validate(&json!({"name": 123})).is_err());
    }

    #[test]
    fn test_number_range() {
        let mut schema = Schema::new();
        schema.value_type = Some(ValueType::Number);
        schema.minimum = Some(0.0);
        schema.maximum = Some(100.0);

        assert!(schema.validate(&json!(50)).is_ok());
        assert!(schema.validate(&json!(0)).is_ok());
        assert!(schema.validate(&json!(100)).is_ok());
        assert!(schema.validate(&json!(-1)).is_err());
        assert!(schema.validate(&json!(101)).is_err());
    }

    #[test]
    fn test_string_length() {
        let mut schema = Schema::new();
        schema.value_type = Some(ValueType::String);
        schema.min_length = Some(3);
        schema.max_length = Some(10);

        assert!(schema.validate(&json!("hello")).is_ok());
        assert!(schema.validate(&json!("hi")).is_err());
        assert!(schema.validate(&json!("this is too long")).is_err());
    }

    #[test]
    fn test_array_validation() {
        let mut schema = Schema::new();
        schema.value_type = Some(ValueType::Array);
        schema.min_length = Some(1);
        schema.max_length = Some(5);

        let mut item_schema = Schema::new();
        item_schema.value_type = Some(ValueType::Number);
        schema.items = Some(Box::new(item_schema));

        assert!(schema.validate(&json!([1, 2, 3])).is_ok());
        assert!(schema.validate(&json!([])).is_err());
        assert!(schema.validate(&json!([1, 2, 3, 4, 5, 6])).is_err());
        assert!(schema.validate(&json!([1, "two", 3])).is_err());
    }

    #[test]
    fn test_enum_validation() {
        let mut schema = Schema::new();
        schema.enum_values = Some(vec![json!("pending"), json!("active"), json!("completed")]);

        assert!(schema.validate(&json!("pending")).is_ok());
        assert!(schema.validate(&json!("active")).is_ok());
        assert!(schema.validate(&json!("invalid")).is_err());
    }

    #[test]
    fn test_integer_validation() {
        let mut schema = Schema::new();
        schema.value_type = Some(ValueType::Integer);

        assert!(schema.validate(&json!(42)).is_ok());
        assert!(schema.validate(&json!(-10)).is_ok());
        assert!(schema.validate(&json!(3.14)).is_err());
    }

    #[test]
    fn test_nested_object_validation() {
        let mut schema = Schema::new();
        schema.value_type = Some(ValueType::Object);

        let mut address_schema = Schema::new();
        address_schema.value_type = Some(ValueType::Object);
        address_schema.required = Some(vec!["city".to_string()]);

        let mut city_schema = Schema::new();
        city_schema.value_type = Some(ValueType::String);

        let mut address_props = HashMap::new();
        address_props.insert("city".to_string(), city_schema);
        address_schema.properties = Some(address_props);

        let mut properties = HashMap::new();
        properties.insert("address".to_string(), address_schema);
        schema.properties = Some(properties);

        assert!(schema.validate(&json!({"address": {"city": "NYC"}})).is_ok());
        assert!(schema.validate(&json!({"address": {}})).is_err());
        assert!(schema.validate(&json!({"address": {"city": 123}})).is_err());
    }

    #[test]
    fn test_schema_serialization() {
        let mut schema = Schema::new();
        schema.value_type = Some(ValueType::Object);
        schema.required = Some(vec!["name".to_string()]);

        let json_str = serde_json::to_string(&schema).unwrap();
        let deserialized: Schema = serde_json::from_str(&json_str).unwrap();

        assert_eq!(schema, deserialized);
    }
}
