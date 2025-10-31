
use crate::core::tx_btree::TxBTree;
use crate::core::document::{read_versioned_document, write_versioned_document};
use crate::core::errors::*;
use crate::core::transaction::Transaction;
use crate::core::database::Database;
use crate::core::metadata::IndexMeta;
use crate::core::constants::PageNum;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TxCollection<'tx> {
    tx: &'tx mut Transaction,
    name: String,
    btree: TxBTree,
    indexes: HashMap<String, TxBTree>,
    index_meta: HashMap<String, IndexMeta>,
}

impl<'tx> TxCollection<'tx> {
    pub(crate) fn new(tx: &'tx mut Transaction, db: Arc<Database>, name: String) -> Result<Self> {
        let metadata = db.get_metadata();
        let coll_meta = metadata.collections.get(&name);

        // Use the transaction's snapshot root, not the current committed root
        // This ensures we see a consistent snapshot view
        let btree_root = tx.get_snapshot_root(&name).unwrap_or(0);

        let pager = tx.get_pager().clone();
        let tx_writes = tx.get_writes_arc();

        let btree = if btree_root == 0 {
            TxBTree::create_empty(pager.clone(), tx_writes.clone())?
        } else {
            TxBTree::new(pager.clone(), btree_root, tx_writes.clone())
        };

        let mut indexes = HashMap::new();
        let mut index_meta = HashMap::new();

        if let Some(coll) = coll_meta {
            for (index_name, idx_meta) in &coll.indexes {
                index_meta.insert(index_name.clone(), idx_meta.clone());

                let index_btree = if idx_meta.btree_root == 0 {
                    TxBTree::create_empty(pager.clone(), tx_writes.clone())?
                } else {
                    TxBTree::new(pager.clone(), idx_meta.btree_root, tx_writes.clone())
                };

                indexes.insert(index_name.clone(), index_btree);
            }
        }

        Ok(Self { tx, name, btree, indexes, index_meta })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn insert(&mut self, doc: Value) -> Result<String> {
        if !doc.is_object() {
            return Err(Error::InvalidDocumentFormat {
                reason: "document must be an object".to_string(),
                collection: Some(self.name.clone()),
            });
        }

        if !self.tx.is_active() {
            return Err(Error::TxNotActive);
        }

        let mut doc_map = doc.as_object()
            .ok_or_else(|| Error::InvalidDocumentFormat {
                reason: "document must be an object".to_string(),
                collection: Some(self.name.clone()),
            })?
            .clone();

        let doc_id = if let Some(id) = doc_map.get("_id") {
            id.as_str()
                .ok_or_else(|| Error::InvalidDocumentFormat {
                    reason: "_id must be a string".to_string(),
                    collection: Some(self.name.clone()),
                })?
                .to_string()
        } else {
            let id = generate_id();
            doc_map.insert("_id".to_string(), Value::String(id.clone()));
            id
        };

        let data = serde_json::to_vec(&doc_map)?;

        let existed = self.btree.search(&doc_id).is_ok();
        self.tx.track_doc_existed_in_snapshot(&self.name, &doc_id, existed);

        if existed {
            return Err(Error::DocumentAlreadyExists {
                collection: self.name.clone(),
                id: doc_id,
            });
        }

        let pager = self.tx.get_pager();
        let mut tx_writes = std::collections::HashMap::new();
        let (page_num, _page_data) = write_versioned_document(
            &pager,
            &doc_id,
            &data,
            self.tx.mvcc_tx_id,
            0,
            &mut tx_writes,
        )?;

        self.btree.insert(&doc_id, page_num)?;

        for (index_name, index_btree) in &mut self.indexes {
            let index_meta = &self.index_meta[index_name];
            let index_fields = index_meta.get_fields();

            use crate::core::index_key::{
                IndexKey, serialize_index_key, extract_field_values,
                CompoundIndexKey, serialize_compound_index_key
            };

            let key_str = if index_fields.len() == 1 {
                // Single-field index
                let field_value = extract_field_values(&doc_map, &index_fields)[0].clone();

                if index_meta.unique {
                    let value_json = serde_json::to_string(&field_value)?;
                    let prefix = format!("{}|", value_json);

                    if index_btree.has_prefix(&prefix)? {
                        return Err(Error::Other(format!(
                            "unique constraint violation on index {}: value {:?} already exists",
                            index_name, field_value
                        )));
                    }
                }

                let index_key = IndexKey {
                    field_value,
                    doc_id: doc_id.clone(),
                };
                serialize_index_key(&index_key)?
            } else {
                // Compound index
                let field_values = extract_field_values(&doc_map, &index_fields);

                if index_meta.unique {
                    // For compound unique constraints, serialize just the field values part
                    let values_json: Vec<String> = field_values.iter()
                        .map(|v| serde_json::to_string(v).unwrap_or_default())
                        .collect();
                    let prefix = format!("{}|", values_json.join("|"));

                    if index_btree.has_prefix(&prefix)? {
                        return Err(Error::Other(format!(
                            "unique constraint violation on index {}: combination {:?} already exists",
                            index_name, field_values
                        )));
                    }
                }

                let compound_key = CompoundIndexKey {
                    field_values,
                    doc_id: doc_id.clone(),
                };
                serialize_compound_index_key(&compound_key)?
            };

            index_btree.insert(&key_str, page_num)?;
        }

        // Add all pages (including overflow pages) to transaction write buffer
        for (pg_num, pg_data) in tx_writes {
            self.tx.write_page(pg_num, pg_data)?;
        }

        self.tx.write_document(&self.name, &doc_id, page_num)?;
        self.tx.set_collection_root(&self.name, self.btree.get_current_root());

        // Track metrics
        if let Some(db) = self.tx.get_database() {
            db.metrics_ref().document_inserted();
        }

        Ok(doc_id)
    }

    pub fn find_by_id(&self, id: &str) -> Result<Value> {
        let page_num = self.btree.search(id)?;
        let pager = self.tx.get_pager();

        let tx_writes_arc = self.tx.get_writes_arc();
        let tx_writes = tx_writes_arc.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
        let vdoc = read_versioned_document(&pager, page_num, &*tx_writes)?;

        // Check if this is our own write first (always visible to us)
        let is_own_write = vdoc.xmin == self.tx.mvcc_tx_id;

        if !is_own_write && !vdoc.is_visible(self.tx.snapshot_id) {
            return Err(Error::DocumentNotFound {
                collection: self.name.clone(),
                id: id.to_string(),
            });
        }

        // Track the document for read-write conflict detection
        // This allows us to detect if another transaction modifies a document we read
        if !is_own_write {
            self.tx.track_doc_existed_in_snapshot(&self.name, id, true);
            self.tx.track_doc_original_xmin(&self.name, id, vdoc.xmin);
        }

        let result: Value = serde_json::from_slice(&vdoc.data)?;

        // Track metrics
        if let Some(db) = self.tx.get_database() {
            db.metrics_ref().document_read();
        }

        Ok(result)
    }

    pub fn update_by_id(&mut self, id: &str, updates: Value) -> Result<()> {

        if !updates.is_object() {
            return Err(Error::InvalidDocumentFormat {
                reason: "updates must be an object".to_string(),
                collection: Some(self.name.clone()),
            });
        }

        if !self.tx.is_active() {
            return Err(Error::TxNotActive);
        }

        let old_page_num = self.btree.search(id)?;

        let pager = self.tx.get_pager();
        let vdoc = {
            let tx_writes_arc = self.tx.get_writes_arc();
            let tx_writes = tx_writes_arc.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
            read_versioned_document(&pager, old_page_num, &*tx_writes)?
        };

        // Check if this is our own write first (always visible to us)
        let is_own_write = vdoc.xmin == self.tx.mvcc_tx_id;

        if !is_own_write && !vdoc.is_visible(self.tx.snapshot_id) {
            return Err(Error::DocumentNotFound {
                collection: self.name.clone(),
                id: id.to_string(),
            });
        }

        // Track that this document existed in our snapshot for conflict detection
        self.tx.track_doc_existed_in_snapshot(&self.name, id, true);
        // Track the original xmin for conflict detection (only if not our own write)
        if !is_own_write {
            self.tx.track_doc_original_xmin(&self.name, id, vdoc.xmin);
        }

        if !is_own_write {
            let old_doc_version = crate::core::mvcc::DocumentVersion {
                doc_id: id.to_string(),
                xmin: vdoc.xmin,
                xmax: vdoc.xmax,
                data: vdoc.data.clone(),
                page_num: old_page_num,
            };
            self.tx.add_old_version(&self.name, id, old_doc_version);
        }

        let mut doc: serde_json::Map<String, Value> = serde_json::from_slice(&vdoc.data)?;

        let updates_map = updates.as_object()
            .ok_or_else(|| Error::InvalidDocumentFormat {
                reason: "updates must be an object".to_string(),
                collection: Some(self.name.clone()),
            })?;
        for (key, value) in updates_map {
            doc.insert(key.clone(), value.clone());
        }

        doc.insert("_id".to_string(), Value::String(id.to_string()));

        let new_data = serde_json::to_vec(&doc)?;

        let mut tx_writes = std::collections::HashMap::new();
        let (new_page_num, _page_data) = write_versioned_document(
            &pager,
            id,
            &new_data,
            self.tx.mvcc_tx_id,
            0,
            &mut tx_writes,
        )?;


        // Use update instead of delete+insert to avoid duplicate entries
        self.btree.update(id, new_page_num)?;

        // Add all pages (including overflow pages) to transaction write buffer
        for (pg_num, pg_data) in tx_writes {
            self.tx.write_page(pg_num, pg_data)?;
        }

        self.tx.write_document(&self.name, id, new_page_num)?;

        let new_root = self.btree.get_current_root();
        self.tx.set_collection_root(&self.name, new_root);

        // Track metrics
        if let Some(db) = self.tx.get_database() {
            db.metrics_ref().document_updated();
        }

        Ok(())
    }

    pub fn delete_by_id(&mut self, id: &str) -> Result<()> {
        if !self.tx.is_active() {
            return Err(Error::TxNotActive);
        }

        let page_num = self.btree.search(id)?;

        let pager = self.tx.get_pager();
        let vdoc = {
            let tx_writes_arc = self.tx.get_writes_arc();
            let tx_writes = tx_writes_arc.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
            read_versioned_document(&pager, page_num, &*tx_writes)?
        };

        // Check if this is our own write first (always visible to us)
        let is_own_write = vdoc.xmin == self.tx.mvcc_tx_id;

        if !is_own_write && !vdoc.is_visible(self.tx.snapshot_id) {
            return Err(Error::DocumentNotFound {
                collection: self.name.clone(),
                id: id.to_string(),
            });
        }
        if !is_own_write {
            let old_doc_version = crate::core::mvcc::DocumentVersion {
                doc_id: id.to_string(),
                xmin: vdoc.xmin,
                xmax: vdoc.xmax,
                data: vdoc.data.clone(),
                page_num,
            };
            self.tx.add_old_version(&self.name, id, old_doc_version);
        }

        self.btree.delete(id)?;

        self.tx.write_document(&self.name, id, PageNum::MAX)?;

        self.tx.set_collection_root(&self.name, self.btree.get_current_root());

        // Track metrics
        if let Some(db) = self.tx.get_database() {
            db.metrics_ref().document_deleted();
        }

        Ok(())
    }

    pub fn find_all(&self) -> Result<Vec<Value>> {
        let mut results = Vec::new();
        let pager = self.tx.get_pager();
        let tx_writes_arc = self.tx.get_writes_arc();
        let tx_writes = tx_writes_arc.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;

        let mut iter = self.btree.iterator()?;
        while iter.next() {
            let (_doc_id, page_num) = iter.entry();
            match read_versioned_document(&pager, page_num, &*tx_writes) {
                Ok(vdoc) => {
                    // Check if this is our own write first (always visible to us)
                    let is_own_write = vdoc.xmin == self.tx.mvcc_tx_id;

                    if is_own_write || vdoc.is_visible(self.tx.snapshot_id) {
                        if let Ok(doc) = serde_json::from_slice(&vdoc.data) {
                            results.push(doc);

                            // Track metrics for each document read
                            if let Some(db) = self.tx.get_database() {
                                db.metrics_ref().document_read();
                            }
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(results)
    }

    pub fn count(&self) -> Result<usize> {
        let mut count = 0;
        let pager = self.tx.get_pager();
        let tx_writes_arc = self.tx.get_writes_arc();
        let tx_writes = tx_writes_arc.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;

        let mut iter = self.btree.iterator()?;
        while iter.next() {
            let (_doc_id, page_num) = iter.entry();
            match read_versioned_document(&pager, page_num, &*tx_writes) {
                Ok(vdoc) => {
                    // Check if this is our own write first (always visible to us)
                    let is_own_write = vdoc.xmin == self.tx.mvcc_tx_id;

                    if is_own_write || vdoc.is_visible(self.tx.snapshot_id) {
                        count += 1;
                    }
                }
                Err(_e) => {
                    continue;
                }
            }
        }

        Ok(count)
    }

    // ========== TYPED DOCUMENT METHODS ==========
    // These methods provide type-safe wrappers around the Value-based methods

    /// Insert a typed document into the collection
    pub fn insert_typed<T: Serialize>(&mut self, doc: &T) -> Result<String> {
        let value = serde_json::to_value(doc)
            .map_err(|e| Error::SerializationError {
                context: format!("insert_typed in collection '{}'", self.name),
                error: e.to_string(),
            })?;
        self.insert(value)
    }

    /// Insert multiple typed documents into the collection
    pub fn insert_many_typed<T: Serialize>(&mut self, docs: Vec<T>) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        for doc in docs {
            let id = self.insert_typed(&doc)?;
            ids.push(id);
        }
        Ok(ids)
    }

    /// Find a typed document by ID
    pub fn find_by_id_typed<T: DeserializeOwned>(&self, id: &str) -> Result<Option<T>> {
        match self.find_by_id(id) {
            Ok(value) => {
                let typed = serde_json::from_value(value)
                    .map_err(|e| Error::DeserializationError {
                        context: format!("find_by_id_typed in collection '{}', document '{}'", self.name, id),
                        error: e.to_string(),
                    })?;
                Ok(Some(typed))
            }
            Err(Error::NotFound) => Ok(None),
            Err(Error::DocumentNotFound { .. }) => Ok(None),
            Err(Error::Other(msg)) if msg.contains("not found") => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Find all typed documents in the collection
    pub fn find_all_typed<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        let values = self.find_all()?;
        values
            .into_iter()
            .map(|value| {
                serde_json::from_value(value)
                    .map_err(|e| Error::DeserializationError {
                        context: format!("find_all_typed in collection '{}'", self.name),
                        error: e.to_string(),
                    })
            })
            .collect()
    }

    /// Update a typed document by ID
    pub fn update_by_id_typed<T: Serialize>(&mut self, id: &str, updates: &T) -> Result<()> {
        let value = serde_json::to_value(updates)
            .map_err(|e| Error::SerializationError {
                context: format!("update_by_id_typed in collection '{}', document '{}'", self.name, id),
                error: e.to_string(),
            })?;
        self.update_by_id(id, value)
    }
}

fn generate_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash, Hasher};

    let random_state = RandomState::new();
    let mut hasher = random_state.build_hasher();
    timestamp.hash(&mut hasher);
    let random_part = hasher.finish();

    format!("{}_{:x}", timestamp, random_part)
}
