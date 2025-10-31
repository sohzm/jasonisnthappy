
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use crate::core::errors::*;
use crate::core::validation::Schema;
use crate::core::text_search::TextIndexMeta;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Metadata {
    pub collections: HashMap<String, CollectionMeta>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CollectionMeta {
    pub btree_root: u64,
    #[serde(default)]
    pub indexes: HashMap<String, IndexMeta>,
    #[serde(default)]
    pub text_indexes: HashMap<String, TextIndexMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexMeta {
    pub name: String,
    /// Deprecated: Use `fields` instead. Kept for backward compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// Fields included in this index. For single-field indexes, this contains one element.
    /// For compound indexes, contains multiple fields in order.
    #[serde(default)]
    pub fields: Vec<String>,
    pub btree_root: u64,
    pub unique: bool,
}

impl IndexMeta {
    /// Returns the list of fields in this index.
    /// Handles backward compatibility with old single-field indexes.
    pub fn get_fields(&self) -> Vec<String> {
        if !self.fields.is_empty() {
            self.fields.clone()
        } else if let Some(ref field) = self.field {
            vec![field.clone()]
        } else {
            Vec::new()
        }
    }

    /// Returns true if this is a compound index (has multiple fields).
    pub fn is_compound(&self) -> bool {
        self.get_fields().len() > 1
    }
}

impl Metadata {
    pub fn new() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let trimmed = data.iter()
            .rposition(|&b| b != 0)
            .map(|pos| &data[..=pos])
            .unwrap_or(&data[..0]);

        Ok(serde_json::from_slice(trimmed)?)
    }

    pub fn get_collection(&mut self, name: &str) -> &mut CollectionMeta {
        self.collections.entry(name.to_string()).or_insert(CollectionMeta {
            btree_root: 0,
            indexes: HashMap::new(),
            text_indexes: HashMap::new(),
            schema: None,
        })
    }

    pub fn clone(&self) -> Self {
        Self {
            collections: self.collections.iter().map(|(k, v)| {
                (k.clone(), CollectionMeta {
                    btree_root: v.btree_root,
                    indexes: v.indexes.iter().map(|(ik, iv)| {
                        (ik.clone(), IndexMeta {
                            name: iv.name.clone(),
                            field: iv.field.clone(),
                            fields: iv.fields.clone(),
                            btree_root: iv.btree_root,
                            unique: iv.unique,
                        })
                    }).collect(),
                    text_indexes: v.text_indexes.clone(),
                    schema: v.schema.clone(),
                })
            }).collect(),
        }
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_metadata() {
        let meta = Metadata::new();
        assert_eq!(meta.collections.len(), 0);
    }

    #[test]
    fn test_get_collection() {
        let mut meta = Metadata::new();

        let coll = meta.get_collection("users");
        assert_eq!(coll.btree_root, 0);
        assert_eq!(coll.indexes.len(), 0);

        coll.btree_root = 42;
        let coll2 = meta.get_collection("users");
        assert_eq!(coll2.btree_root, 42);
    }

    #[test]
    fn test_serialization() {
        let mut meta = Metadata::new();

        let coll = meta.get_collection("users");
        coll.btree_root = 100;
        coll.indexes.insert("email_idx".to_string(), IndexMeta {
            name: "email_idx".to_string(),
            field: None,
            fields: vec!["email".to_string()],
            btree_root: 200,
            unique: true,
        });

        let data = meta.serialize().unwrap();

        let meta2 = Metadata::deserialize(&data).unwrap();

        assert_eq!(meta2.collections.len(), 1);
        let coll2 = meta2.collections.get("users").unwrap();
        assert_eq!(coll2.btree_root, 100);
        assert_eq!(coll2.indexes.len(), 1);

        let idx = coll2.indexes.get("email_idx").unwrap();
        assert_eq!(idx.get_fields(), vec!["email".to_string()]);
        assert_eq!(idx.btree_root, 200);
        assert!(idx.unique);
    }

    #[test]
    fn test_clone() {
        let mut meta = Metadata::new();

        let coll = meta.get_collection("users");
        coll.btree_root = 100;
        coll.indexes.insert("email_idx".to_string(), IndexMeta {
            name: "email_idx".to_string(),
            field: None,
            fields: vec!["email".to_string()],
            btree_root: 200,
            unique: true,
        });

        let meta2 = meta.clone();

        assert_eq!(meta2.collections.len(), 1);
        let coll2 = meta2.collections.get("users").unwrap();
        assert_eq!(coll2.btree_root, 100);
        assert_eq!(coll2.indexes.len(), 1);
    }

    #[test]
    fn test_backward_compatibility_single_field() {
        // Test that old single-field indexes still work
        let mut meta = Metadata::new();
        let coll = meta.get_collection("users");
        coll.indexes.insert("old_idx".to_string(), IndexMeta {
            name: "old_idx".to_string(),
            field: Some("email".to_string()),
            fields: Vec::new(),
            btree_root: 100,
            unique: false,
        });

        let idx = &coll.indexes["old_idx"];
        assert_eq!(idx.get_fields(), vec!["email".to_string()]);
        assert!(!idx.is_compound());
    }

    #[test]
    fn test_compound_index_meta() {
        let mut meta = Metadata::new();
        let coll = meta.get_collection("users");
        coll.indexes.insert("compound_idx".to_string(), IndexMeta {
            name: "compound_idx".to_string(),
            field: None,
            fields: vec!["city".to_string(), "age".to_string()],
            btree_root: 200,
            unique: false,
        });

        let idx = &coll.indexes["compound_idx"];
        assert_eq!(idx.get_fields(), vec!["city".to_string(), "age".to_string()]);
        assert!(idx.is_compound());
    }
}
