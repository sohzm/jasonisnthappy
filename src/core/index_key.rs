
use serde_json::{Value, Map};
use std::cmp::Ordering;
use crate::core::errors::*;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexKey {
    pub field_value: Value,
    pub doc_id: String,
}

pub fn compare_index_keys(a: &IndexKey, b: &IndexKey) -> Ordering {
    match compare_values(&a.field_value, &b.field_value) {
        Ordering::Equal => a.doc_id.cmp(&b.doc_id),
        other => other,
    }
}

pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    use Value::*;

    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,

        (Bool(a_val), Bool(b_val)) => a_val.cmp(b_val),
        (Bool(_), _) => Ordering::Less,
        (_, Bool(_)) => Ordering::Greater,

        (Number(a_num), Number(b_num)) => {
            let a_f64 = a_num.as_f64().unwrap_or(0.0);
            let b_f64 = b_num.as_f64().unwrap_or(0.0);

            if a_f64 < b_f64 {
                Ordering::Less
            } else if a_f64 > b_f64 {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        (Number(_), _) => Ordering::Less,
        (_, Number(_)) => Ordering::Greater,

        (String(a_str), String(b_str)) => a_str.cmp(b_str),
        (String(_), _) => Ordering::Less,
        (_, String(_)) => Ordering::Greater,

        (Array(a_arr), Array(b_arr)) => {
            for (a_elem, b_elem) in a_arr.iter().zip(b_arr.iter()) {
                match compare_values(a_elem, b_elem) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            a_arr.len().cmp(&b_arr.len())
        }
        (Array(_), _) => Ordering::Less,
        (_, Array(_)) => Ordering::Greater,

        (Object(a_obj), Object(b_obj)) => {
            let mut a_keys: Vec<_> = a_obj.keys().collect();
            let mut b_keys: Vec<_> = b_obj.keys().collect();
            a_keys.sort();
            b_keys.sort();

            for (a_key, b_key) in a_keys.iter().zip(b_keys.iter()) {
                match a_key.cmp(b_key) {
                    Ordering::Equal => {
                        match compare_values(&a_obj[*a_key], &b_obj[*b_key]) {
                            Ordering::Equal => continue,
                            other => return other,
                        }
                    }
                    other => return other,
                }
            }
            a_keys.len().cmp(&b_keys.len())
        }
    }
}

pub fn serialize_index_key(key: &IndexKey) -> Result<String> {
    let value_json = serde_json::to_string(&key.field_value)?;
    Ok(format!("{}|{}", value_json, key.doc_id))
}

pub fn deserialize_index_key(s: &str) -> Result<IndexKey> {
    if let Some(pos) = s.find('|') {
        let value_str = &s[..pos];
        let doc_id = &s[pos + 1..];

        let field_value: Value = serde_json::from_str(value_str)?;

        Ok(IndexKey {
            field_value,
            doc_id: doc_id.to_string(),
        })
    } else {
        Err(Error::Other("invalid index key format".to_string()))
    }
}

pub fn extract_field_value(doc: &Map<String, Value>, field: &str) -> Value {
    let parts: Vec<&str> = field.split('.').collect();
    let mut current = Value::Object(doc.clone());

    for part in parts {
        match current {
            Value::Object(obj) => {
                if let Some(value) = obj.get(part) {
                    current = value.clone();
                } else {
                    return Value::Null;
                }
            }
            _ => return Value::Null,
        }
    }

    current
}

/// Compound index key structure for multi-field indexes
#[derive(Debug, Clone, PartialEq)]
pub struct CompoundIndexKey {
    pub field_values: Vec<Value>,
    pub doc_id: String,
}

/// Serialize a compound index key to a string
/// Format: "<field1_json>|<field2_json>|...|<doc_id>"
pub fn serialize_compound_index_key(key: &CompoundIndexKey) -> Result<String> {
    let mut parts = Vec::with_capacity(key.field_values.len() + 1);

    for value in &key.field_values {
        let value_json = serde_json::to_string(value)?;
        parts.push(value_json);
    }

    parts.push(key.doc_id.clone());

    Ok(parts.join("|"))
}

/// Deserialize a compound index key from a string
pub fn deserialize_compound_index_key(s: &str, num_fields: usize) -> Result<CompoundIndexKey> {
    let parts: Vec<&str> = s.split('|').collect();

    if parts.len() < num_fields + 1 {
        return Err(Error::Other(format!(
            "invalid compound index key format: expected {} fields + doc_id, got {} parts",
            num_fields, parts.len()
        )));
    }

    let mut field_values = Vec::with_capacity(num_fields);
    for i in 0..num_fields {
        let value: Value = serde_json::from_str(parts[i])?;
        field_values.push(value);
    }

    // The rest is the doc_id (join remaining parts in case doc_id contains '|')
    let doc_id = parts[num_fields..].join("|");

    Ok(CompoundIndexKey {
        field_values,
        doc_id,
    })
}

/// Compare two compound index keys
pub fn compare_compound_index_keys(a: &CompoundIndexKey, b: &CompoundIndexKey) -> Ordering {
    // Compare field values in order
    for (val_a, val_b) in a.field_values.iter().zip(b.field_values.iter()) {
        match compare_values(val_a, val_b) {
            Ordering::Equal => continue,
            other => return other,
        }
    }

    // If all field values are equal, compare by number of fields
    match a.field_values.len().cmp(&b.field_values.len()) {
        Ordering::Equal => a.doc_id.cmp(&b.doc_id),
        other => other,
    }
}

/// Extract multiple field values from a document
pub fn extract_field_values(doc: &Map<String, Value>, fields: &[String]) -> Vec<Value> {
    fields.iter().map(|field| extract_field_value(doc, field)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_compare_values() {
        assert_eq!(compare_values(&json!(null), &json!(null)), Ordering::Equal);
        assert_eq!(compare_values(&json!(null), &json!(true)), Ordering::Less);
        assert_eq!(compare_values(&json!(true), &json!(1)), Ordering::Less);
        assert_eq!(compare_values(&json!(1), &json!("hello")), Ordering::Less);
        assert_eq!(compare_values(&json!("a"), &json!(["array"])), Ordering::Less);
        assert_eq!(compare_values(&json!([1]), &json!({"key": "val"})), Ordering::Less);

        assert_eq!(compare_values(&json!(1), &json!(2)), Ordering::Less);
        assert_eq!(compare_values(&json!(2.5), &json!(2.5)), Ordering::Equal);
        assert_eq!(compare_values(&json!(10), &json!(5)), Ordering::Greater);

        assert_eq!(compare_values(&json!("apple"), &json!("banana")), Ordering::Less);
        assert_eq!(compare_values(&json!("hello"), &json!("hello")), Ordering::Equal);
    }

    #[test]
    fn test_serialize_deserialize_index_key() {
        let key = IndexKey {
            field_value: json!("alice@example.com"),
            doc_id: "doc123".to_string(),
        };

        let serialized = serialize_index_key(&key).unwrap();
        assert!(serialized.contains("|"));
        assert!(serialized.contains("doc123"));

        let deserialized = deserialize_index_key(&serialized).unwrap();
        assert_eq!(deserialized.field_value, json!("alice@example.com"));
        assert_eq!(deserialized.doc_id, "doc123");
    }

    #[test]
    fn test_serialize_complex_values() {
        let key = IndexKey {
            field_value: json!({"name": "Alice", "age": 30}),
            doc_id: "doc456".to_string(),
        };

        let serialized = serialize_index_key(&key).unwrap();
        let deserialized = deserialize_index_key(&serialized).unwrap();

        assert_eq!(deserialized.field_value, json!({"name": "Alice", "age": 30}));
        assert_eq!(deserialized.doc_id, "doc456");
    }

    #[test]
    fn test_extract_field_value() {
        let doc = serde_json::from_value::<Map<String, Value>>(json!({
            "name": "Alice",
            "email": "alice@example.com",
            "address": {
                "city": "SF",
                "zip": "94102"
            }
        })).unwrap();

        assert_eq!(extract_field_value(&doc, "name"), json!("Alice"));
        assert_eq!(extract_field_value(&doc, "email"), json!("alice@example.com"));
        assert_eq!(extract_field_value(&doc, "address.city"), json!("SF"));
        assert_eq!(extract_field_value(&doc, "address.zip"), json!("94102"));
        assert_eq!(extract_field_value(&doc, "nonexistent"), json!(null));
        assert_eq!(extract_field_value(&doc, "address.nonexistent"), json!(null));
    }

    #[test]
    fn test_compare_index_keys() {
        let key1 = IndexKey {
            field_value: json!("alice"),
            doc_id: "doc1".to_string(),
        };

        let key2 = IndexKey {
            field_value: json!("bob"),
            doc_id: "doc2".to_string(),
        };

        let key3 = IndexKey {
            field_value: json!("alice"),
            doc_id: "doc2".to_string(),
        };

        assert_eq!(compare_index_keys(&key1, &key2), Ordering::Less);
        assert_eq!(compare_index_keys(&key2, &key1), Ordering::Greater);
        assert_eq!(compare_index_keys(&key1, &key1), Ordering::Equal);

        assert_eq!(compare_index_keys(&key1, &key3), Ordering::Less);
    }

    #[test]
    fn test_serialize_compound_index_key() {
        let key = CompoundIndexKey {
            field_values: vec![json!("NYC"), json!(30)],
            doc_id: "doc123".to_string(),
        };

        let serialized = serialize_compound_index_key(&key).unwrap();
        assert_eq!(serialized, "\"NYC\"|30|doc123");
    }

    #[test]
    fn test_deserialize_compound_index_key() {
        let serialized = "\"NYC\"|30|doc123";
        let key = deserialize_compound_index_key(serialized, 2).unwrap();

        assert_eq!(key.field_values.len(), 2);
        assert_eq!(key.field_values[0], json!("NYC"));
        assert_eq!(key.field_values[1], json!(30));
        assert_eq!(key.doc_id, "doc123");
    }

    #[test]
    fn test_compound_key_roundtrip() {
        let original = CompoundIndexKey {
            field_values: vec![json!("San Francisco"), json!(true), json!(null)],
            doc_id: "doc456".to_string(),
        };

        let serialized = serialize_compound_index_key(&original).unwrap();
        let deserialized = deserialize_compound_index_key(&serialized, 3).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_compare_compound_index_keys() {
        let key1 = CompoundIndexKey {
            field_values: vec![json!("NYC"), json!(25)],
            doc_id: "doc1".to_string(),
        };

        let key2 = CompoundIndexKey {
            field_values: vec![json!("NYC"), json!(30)],
            doc_id: "doc2".to_string(),
        };

        let key3 = CompoundIndexKey {
            field_values: vec![json!("SF"), json!(25)],
            doc_id: "doc3".to_string(),
        };

        // Same first field, different second field
        assert_eq!(compare_compound_index_keys(&key1, &key2), Ordering::Less);

        // Different first field
        assert_eq!(compare_compound_index_keys(&key1, &key3), Ordering::Less);

        // Same key
        assert_eq!(compare_compound_index_keys(&key1, &key1), Ordering::Equal);
    }

    #[test]
    fn test_compare_compound_keys_with_doc_id() {
        let key1 = CompoundIndexKey {
            field_values: vec![json!("NYC"), json!(30)],
            doc_id: "doc1".to_string(),
        };

        let key2 = CompoundIndexKey {
            field_values: vec![json!("NYC"), json!(30)],
            doc_id: "doc2".to_string(),
        };

        // Same field values, different doc_id
        assert_eq!(compare_compound_index_keys(&key1, &key2), Ordering::Less);
    }

    #[test]
    fn test_extract_field_values() {
        let doc = serde_json::from_value::<Map<String, Value>>(json!({
            "name": "Alice",
            "city": "NYC",
            "age": 30,
            "address": {
                "zip": "10001"
            }
        })).unwrap();

        let values = extract_field_values(&doc, &[
            "city".to_string(),
            "age".to_string()
        ]);

        assert_eq!(values.len(), 2);
        assert_eq!(values[0], json!("NYC"));
        assert_eq!(values[1], json!(30));
    }

    #[test]
    fn test_extract_field_values_with_nested() {
        let doc = serde_json::from_value::<Map<String, Value>>(json!({
            "name": "Alice",
            "address": {
                "city": "NYC",
                "zip": "10001"
            }
        })).unwrap();

        let values = extract_field_values(&doc, &[
            "address.city".to_string(),
            "address.zip".to_string()
        ]);

        assert_eq!(values.len(), 2);
        assert_eq!(values[0], json!("NYC"));
        assert_eq!(values[1], json!("10001"));
    }

    #[test]
    fn test_extract_field_values_with_null() {
        let doc = serde_json::from_value::<Map<String, Value>>(json!({
            "name": "Alice",
            "city": "NYC"
        })).unwrap();

        let values = extract_field_values(&doc, &[
            "city".to_string(),
            "nonexistent".to_string()
        ]);

        assert_eq!(values.len(), 2);
        assert_eq!(values[0], json!("NYC"));
        assert_eq!(values[1], json!(null));
    }

    #[test]
    fn test_compound_key_with_complex_values() {
        let key = CompoundIndexKey {
            field_values: vec![
                json!({"nested": "object"}),
                json!([1, 2, 3]),
                json!("string")
            ],
            doc_id: "doc789".to_string(),
        };

        let serialized = serialize_compound_index_key(&key).unwrap();
        let deserialized = deserialize_compound_index_key(&serialized, 3).unwrap();

        assert_eq!(key, deserialized);
    }
}
