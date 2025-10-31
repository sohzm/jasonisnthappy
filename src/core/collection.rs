
use crate::core::aggregation::AggregationPipeline;
use crate::core::btree::BTree;
use crate::core::database::Database;
use crate::core::document::{read_versioned_document, write_versioned_document, delete_document};
use crate::core::errors::*;
use crate::core::query::parser::parse_query;
use crate::core::query_builder::QueryBuilder;
use crate::core::watch::WatchBuilder;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Result of an upsert operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpsertResult {
    /// A new document was inserted with the given ID
    Inserted(String),
    /// An existing document was updated with the given ID
    Updated(String),
}

/// Result of a bulk write operation
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BulkWriteResult {
    /// Number of documents successfully inserted
    pub inserted_count: usize,
    /// Number of documents successfully updated
    pub updated_count: usize,
    /// Number of documents successfully deleted
    pub deleted_count: usize,
    /// Errors that occurred during execution (in unordered mode)
    pub errors: Vec<BulkWriteError>,
}

/// Error that occurred during a bulk write operation
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BulkWriteError {
    /// Index of the operation that failed
    pub operation_index: usize,
    /// Error message
    pub message: String,
}

/// A single operation in a bulk write
#[derive(Debug, Clone)]
enum BulkOperation {
    Insert(Value),
    UpdateOne { query: String, updates: Value },
    UpdateMany { query: String, updates: Value },
    DeleteOne(String),
    DeleteMany(String),
}

/// Builder for bulk write operations
pub struct BulkWrite<'a> {
    collection: &'a Collection,
    operations: Vec<BulkOperation>,
    ordered: bool,
}

impl<'a> BulkWrite<'a> {
    fn new(collection: &'a Collection) -> Self {
        Self {
            collection,
            operations: Vec::new(),
            ordered: true, // Default to ordered execution
        }
    }

    /// Add an insert operation to the bulk write
    pub fn insert(mut self, doc: Value) -> Self {
        self.operations.push(BulkOperation::Insert(doc));
        self
    }

    /// Add an update_one operation to the bulk write
    pub fn update_one(mut self, query: &str, updates: Value) -> Self {
        self.operations.push(BulkOperation::UpdateOne {
            query: query.to_string(),
            updates,
        });
        self
    }

    /// Add an update_many operation to the bulk write
    pub fn update_many(mut self, query: &str, updates: Value) -> Self {
        self.operations.push(BulkOperation::UpdateMany {
            query: query.to_string(),
            updates,
        });
        self
    }

    /// Add a delete_one operation to the bulk write
    pub fn delete_one(mut self, query: &str) -> Self {
        self.operations.push(BulkOperation::DeleteOne(query.to_string()));
        self
    }

    /// Add a delete_many operation to the bulk write
    pub fn delete_many(mut self, query: &str) -> Self {
        self.operations.push(BulkOperation::DeleteMany(query.to_string()));
        self
    }

    /// Set whether operations should be executed in order (stop on first error)
    /// or unordered (continue on errors). Default is ordered.
    pub fn ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    /// Execute all bulk operations in a single transaction
    pub fn execute(self) -> Result<BulkWriteResult> {
        let mut result = BulkWriteResult {
            inserted_count: 0,
            updated_count: 0,
            deleted_count: 0,
            errors: Vec::new(),
        };

        if self.operations.is_empty() {
            return Ok(result);
        }

        // Check bulk operation size limit
        let max_bulk_ops = self.collection.db.max_bulk_operations();
        if self.operations.len() > max_bulk_ops {
            return Err(Error::BulkOperationTooLarge {
                count: self.operations.len(),
                limit: max_bulk_ops,
            });
        }

        // Extract fields to avoid borrow issues
        let operations = self.operations;
        let ordered = self.ordered;
        let collection = self.collection;

        // Execute all operations in a single transaction
        let mut tx = collection.db.begin()?;
        let tx_id = tx.mvcc_tx_id;

        let metadata = collection.db.get_metadata();
        let btree_root = metadata.collections
            .get(&collection.name)
            .map(|c| c.btree_root)
            .unwrap_or(0);

        let pager = tx.get_pager().clone();
        let btree = if btree_root == 0 {
            BTree::new(pager.clone())?
        } else {
            BTree::open(pager.clone(), btree_root)
        };

        // Process each operation
        for (index, operation) in operations.into_iter().enumerate() {
            let op_result = match operation {
                BulkOperation::Insert(doc) => {
                    execute_insert(collection, &btree, &pager, &mut tx, tx_id, doc)
                        .map(|_| (1, 0, 0))
                }
                BulkOperation::UpdateOne { query, updates } => {
                    execute_update_one(collection, &btree, &pager, &mut tx, tx_id, &query, updates)
                        .map(|count| (0, count, 0))
                }
                BulkOperation::UpdateMany { query, updates } => {
                    execute_update_many(collection, &btree, &pager, &mut tx, tx_id, &query, updates)
                        .map(|count| (0, count, 0))
                }
                BulkOperation::DeleteOne(query) => {
                    execute_delete_one(collection, &btree, &pager, &tx, &query)
                        .map(|count| (0, 0, count))
                }
                BulkOperation::DeleteMany(query) => {
                    execute_delete_many(collection, &btree, &pager, &tx, &query)
                        .map(|count| (0, 0, count))
                }
            };

            match op_result {
                Ok((inserted, updated, deleted)) => {
                    result.inserted_count += inserted;
                    result.updated_count += updated;
                    result.deleted_count += deleted;
                }
                Err(e) => {
                    let error = BulkWriteError {
                        operation_index: index,
                        message: e.to_string(),
                    };

                    if ordered {
                        // In ordered mode, stop on first error and rollback
                        return Err(Error::Other(format!(
                            "bulk write failed at operation {}: {}",
                            index, error.message
                        )));
                    } else {
                        // In unordered mode, collect error and continue
                        result.errors.push(error);
                    }
                }
            }
        }

        // Update metadata with new btree root
        let new_root = btree.root_page();
        collection.db.update_metadata(|m| {
            let coll = m.get_collection(&collection.name);
            coll.btree_root = new_root;
        })?;

        // Commit the transaction
        tx.commit()?;

        Ok(result)
    }

}

// Helper functions for bulk operations
fn execute_insert(
    collection: &Collection,
    btree: &BTree,
    pager: &Arc<crate::core::pager::Pager>,
    tx: &mut crate::core::transaction::Transaction,
    tx_id: u64,
    doc: Value,
) -> Result<String> {
        let mut doc_map = doc.as_object()
            .ok_or_else(|| Error::Other("document must be an object".to_string()))?
            .clone();

        let doc_id = if let Some(id) = doc_map.get("_id") {
            id.as_str()
                .ok_or_else(|| Error::Other("_id must be a string".to_string()))?
                .to_string()
        } else {
            let id = generate_id();
            doc_map.insert("_id".to_string(), Value::String(id.clone()));
            id
        };

        // Check if document already exists
        if btree.search(&doc_id).is_ok() {
            return Err(Error::Other(format!("document with ID {} already exists", doc_id)));
        }

        let data = serde_json::to_vec(&doc_map)?;

        let mut tx_writes = std::collections::HashMap::new();
        let (page_num, _page_data) = write_versioned_document(
            pager,
            &doc_id,
            &data,
            tx_id,
            0,
            &mut tx_writes,
        )?;

        btree.insert(&doc_id, page_num)?;

        // Add all pages to transaction write buffer
        for (pg_num, pg_data) in tx_writes {
            tx.write_page(pg_num, pg_data)?;
        }

        tx.write_document(&collection.name, &doc_id, page_num)?;

        Ok(doc_id)
}

fn execute_update_one(
    collection: &Collection,
    btree: &BTree,
    pager: &Arc<crate::core::pager::Pager>,
    tx: &mut crate::core::transaction::Transaction,
    tx_id: u64,
    query: &str,
    updates: Value,
) -> Result<usize> {
        if !updates.is_object() {
            return Err(Error::Other("updates must be an object".to_string()));
        }

        // Find first matching document
        let doc = find_one_in_tx(btree, pager, tx, query)?;

        if let Some(doc) = doc {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                execute_update_by_id(collection, btree, pager, tx, tx_id, id, updates)?;
                return Ok(1);
            }
        }

        Ok(0)
}

fn execute_update_many(
    collection: &Collection,
    btree: &BTree,
    pager: &Arc<crate::core::pager::Pager>,
    tx: &mut crate::core::transaction::Transaction,
    tx_id: u64,
    query: &str,
    updates: Value,
) -> Result<usize> {
        if !updates.is_object() {
            return Err(Error::Other("updates must be an object".to_string()));
        }

        let docs = find_in_tx(btree, pager, tx, query)?;
        let mut count = 0;

        for doc in docs {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                execute_update_by_id(collection, btree, pager, tx, tx_id, id, updates.clone())?;
                count += 1;
            }
        }

        Ok(count)
}

fn execute_update_by_id(
    collection: &Collection,
    btree: &BTree,
    pager: &Arc<crate::core::pager::Pager>,
    tx: &mut crate::core::transaction::Transaction,
    tx_id: u64,
    id: &str,
    updates: Value,
) -> Result<()> {
        let old_page_num = btree.search(id)?;

        let vdoc = {
            let tx_writes_arc = tx.get_writes_arc();
            let tx_writes_read = tx_writes_arc.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
            read_versioned_document(pager, old_page_num, &*tx_writes_read)?
        };

        if !vdoc.is_visible(tx.snapshot_id) {
            return Err(Error::Other("document not found".to_string()));
        }

        let mut doc: serde_json::Map<String, Value> = serde_json::from_slice(&vdoc.data)?;

        let updates_map = updates.as_object()
            .ok_or_else(|| Error::Other("updates must be an object".to_string()))?;
        for (key, value) in updates_map {
            doc.insert(key.clone(), value.clone());
        }

        doc.insert("_id".to_string(), Value::String(id.to_string()));

        // Validate against schema if one is set
        let metadata = collection.db.get_metadata();
        if let Some(coll_meta) = metadata.collections.get(&collection.name) {
            if let Some(ref schema) = coll_meta.schema {
                schema.validate(&Value::Object(doc.clone()))?;
            }
        }

        let new_data = serde_json::to_vec(&doc)?;

        let mut tx_writes = std::collections::HashMap::new();
        let (new_page_num, _page_data) = write_versioned_document(
            pager,
            id,
            &new_data,
            tx_id,
            0,
            &mut tx_writes,
        )?;

        btree.delete(id)?;
        btree.insert(id, new_page_num)?;

        for (pg_num, pg_data) in tx_writes {
            tx.write_page(pg_num, pg_data)?;
        }

        tx.write_document(&collection.name, id, new_page_num)?;

        Ok(())
}

fn execute_delete_one(
    _collection: &Collection,
    btree: &BTree,
    pager: &Arc<crate::core::pager::Pager>,
    tx: &crate::core::transaction::Transaction,
    query: &str,
) -> Result<usize> {
        let doc = find_one_in_tx(btree, pager, tx, query)?;

        if let Some(doc) = doc {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                execute_delete_by_id(btree, pager, id)?;
                return Ok(1);
            }
        }

        Ok(0)
}

fn execute_delete_many(
    _collection: &Collection,
    btree: &BTree,
    pager: &Arc<crate::core::pager::Pager>,
    tx: &crate::core::transaction::Transaction,
    query: &str,
) -> Result<usize> {
        let docs = find_in_tx(btree, pager, tx, query)?;
        let mut count = 0;

        for doc in docs {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                execute_delete_by_id(btree, pager, id)?;
                count += 1;
            }
        }

        Ok(count)
}

fn execute_delete_by_id(
    btree: &BTree,
    pager: &Arc<crate::core::pager::Pager>,
    id: &str,
) -> Result<()> {
        let page_num = btree.search(id)?;
        delete_document(pager, page_num)?;
        btree.delete(id)?;
        Ok(())
}

fn find_in_tx(
    btree: &BTree,
    pager: &Arc<crate::core::pager::Pager>,
    tx: &crate::core::transaction::Transaction,
    query: &str,
) -> Result<Vec<Value>> {
        let ast = parse_query(query)
            .map_err(|e| Error::Other(format!("failed to parse query: {}", e)))?;

        let all_docs = find_all_in_tx(btree, pager, tx)?;
        let mut results = Vec::new();

        for doc in all_docs {
            if let Some(doc_map) = doc.as_object() {
                if ast.eval(doc_map) {
                    results.push(doc);
                }
            }
        }

        Ok(results)
}

fn find_one_in_tx(
    btree: &BTree,
    pager: &Arc<crate::core::pager::Pager>,
    tx: &crate::core::transaction::Transaction,
    query: &str,
) -> Result<Option<Value>> {
    let docs = find_in_tx(btree, pager, tx, query)?;
    Ok(docs.into_iter().next())
}

fn find_all_in_tx(
    btree: &BTree,
    pager: &Arc<crate::core::pager::Pager>,
    tx: &crate::core::transaction::Transaction,
) -> Result<Vec<Value>> {
        let mut results = Vec::new();
        let tx_writes_arc = tx.get_writes_arc();
        let tx_writes = tx_writes_arc.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;

        let mut iter = btree.iterator()?;
        while iter.next() {
            let (_doc_id, page_num) = iter.entry();
            match read_versioned_document(pager, page_num, &*tx_writes) {
                Ok(vdoc) => {
                    if vdoc.is_visible(tx.snapshot_id) {
                        if let Ok(doc) = serde_json::from_slice(&vdoc.data) {
                            results.push(doc);
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(results)
}

pub struct Collection {
    db: Arc<Database>,
    name: String,
}

impl Collection {
    pub(crate) fn new(db: Arc<Database>, name: String) -> Self {
        Self { db, name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Create a new query builder for this collection
    pub fn query(&self) -> QueryBuilder<'_> {
        QueryBuilder::new(self)
    }

    /// Create a new bulk write builder for this collection
    pub fn bulk_write(&self) -> BulkWrite<'_> {
        BulkWrite::new(self)
    }

    /// Create a new aggregation pipeline for this collection
    pub fn aggregate(&self) -> AggregationPipeline<'_> {
        AggregationPipeline::new(self)
    }

    pub fn insert(&self, doc: Value) -> Result<String> {
        let mut doc_map = doc.as_object()
            .ok_or_else(|| Error::Other("document must be an object".to_string()))?
            .clone();

        // Validate against schema if one is set
        let metadata = self.db.get_metadata();
        if let Some(coll_meta) = metadata.collections.get(&self.name) {
            if let Some(ref schema) = coll_meta.schema {
                schema.validate(&Value::Object(doc_map.clone()))?;
            }
        }

        let doc_id = if let Some(id) = doc_map.get("_id") {
            id.as_str()
                .ok_or_else(|| Error::Other("_id must be a string".to_string()))?
                .to_string()
        } else {
            let id = generate_id();
            doc_map.insert("_id".to_string(), Value::String(id.clone()));
            id
        };

        let data = serde_json::to_vec(&doc_map)?;

        let mut tx = self.db.begin()?;
        let tx_id = tx.mvcc_tx_id;

        let metadata = self.db.get_metadata();
        let btree_root = metadata.collections
            .get(&self.name)
            .map(|c| c.btree_root)
            .unwrap_or(0);

        let pager = tx.get_pager();
        let btree = if btree_root == 0 {
            BTree::new(pager.clone())?
        } else {
            BTree::open(pager.clone(), btree_root)
        };

        if btree.search(&doc_id).is_ok() {
            return Err(Error::Other(format!("document with ID {} already exists", doc_id)));
        }

        let mut tx_writes = std::collections::HashMap::new();
        let (page_num, _page_data) = write_versioned_document(
            &pager,
            &doc_id,
            &data,
            tx_id,
            0,
            &mut tx_writes,
        )?;

        btree.insert(&doc_id, page_num)?;

        // Add all pages (including overflow pages) to transaction write buffer
        for (pg_num, pg_data) in tx_writes {
            tx.write_page(pg_num, pg_data)?;
        }

        tx.write_document(&self.name, &doc_id, page_num)?;

        let new_root = btree.root_page();

        // Track the root change in the transaction - commit will update metadata
        tx.set_collection_root(&self.name, new_root);

        tx.commit()?;

        Ok(doc_id)
    }

    pub fn find_by_id(&self, id: &str) -> Result<Value> {
        let tx = self.db.begin()?;

        let metadata = self.db.get_metadata();
        let btree_root = metadata.collections
            .get(&self.name)
            .ok_or_else(|| Error::Other(format!("collection {} not found", self.name)))?
            .btree_root;

        if btree_root == 0 {
            return Err(Error::Other("document not found".to_string()));
        }

        let pager = tx.get_pager();
        let btree = BTree::open(pager.clone(), btree_root);

        let page_num = btree.search(id)?;

        let vdoc = {
            let tx_writes_arc = tx.get_writes_arc();
            let tx_writes = tx_writes_arc.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
            read_versioned_document(&pager, page_num, &*tx_writes)?
        };

        if !vdoc.is_visible(tx.snapshot_id) {
            return Err(Error::Other("document not found".to_string()));
        }

        let result: Value = serde_json::from_slice(&vdoc.data)?;

        Ok(result)
    }

    pub fn find_all(&self) -> Result<Vec<Value>> {
        let tx = self.db.begin()?;

        let metadata = self.db.get_metadata();
        let btree_root = metadata.collections
            .get(&self.name)
            .ok_or_else(|| Error::Other(format!("collection {} not found", self.name)))?
            .btree_root;

        if btree_root == 0 {
            return Ok(Vec::new());
        }

        let pager = tx.get_pager();
        let btree = BTree::open(pager.clone(), btree_root);

        let mut results = Vec::new();
        let tx_writes_arc = tx.get_writes_arc();
        let tx_writes = tx_writes_arc.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;

        let mut iter = btree.iterator()?;
        while iter.next() {
            let (_doc_id, page_num) = iter.entry();
            match read_versioned_document(&pager, page_num, &*tx_writes) {
                Ok(vdoc) => {
                    if vdoc.is_visible(tx.snapshot_id) {
                        if let Ok(doc) = serde_json::from_slice(&vdoc.data) {
                            results.push(doc);
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(results)
    }

    pub fn update_by_id(&self, id: &str, updates: Value) -> Result<()> {
        if !updates.is_object() {
            return Err(Error::Other("updates must be an object".to_string()));
        }

        let mut tx = self.db.begin()?;
        let tx_id = tx.mvcc_tx_id;

        let metadata = self.db.get_metadata();
        let btree_root = metadata.collections
            .get(&self.name)
            .ok_or_else(|| Error::Other(format!("collection {} not found", self.name)))?
            .btree_root;

        if btree_root == 0 {
            return Err(Error::Other("document not found".to_string()));
        }

        let pager = tx.get_pager();
        let btree = BTree::open(pager.clone(), btree_root);

        let old_page_num = btree.search(id)?;

        let vdoc = {
            let tx_writes_arc = tx.get_writes_arc();
            let tx_writes = tx_writes_arc.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
            read_versioned_document(&pager, old_page_num, &*tx_writes)?
        };

        if !vdoc.is_visible(tx.snapshot_id) {
            return Err(Error::Other("document not found".to_string()));
        }

        let mut doc: serde_json::Map<String, Value> = serde_json::from_slice(&vdoc.data)?;

        let updates_map = updates.as_object()
            .ok_or_else(|| Error::Other("updates must be an object".to_string()))?;
        for (key, value) in updates_map {
            doc.insert(key.clone(), value.clone());
        }

        doc.insert("_id".to_string(), Value::String(id.to_string()));

        // Validate against schema if one is set
        let metadata = self.db.get_metadata();
        if let Some(coll_meta) = metadata.collections.get(&self.name) {
            if let Some(ref schema) = coll_meta.schema {
                schema.validate(&Value::Object(doc.clone()))?;
            }
        }

        let new_data = serde_json::to_vec(&doc)?;

        let mut tx_writes = std::collections::HashMap::new();
        let (new_page_num, _page_data) = write_versioned_document(
            &pager,
            id,
            &new_data,
            tx_id,
            0,
            &mut tx_writes,
        )?;

        btree.delete(id)?;
        btree.insert(id, new_page_num)?;

        // Add all pages (including overflow pages) to transaction write buffer
        for (pg_num, pg_data) in tx_writes {
            tx.write_page(pg_num, pg_data)?;
        }

        tx.write_document(&self.name, id, new_page_num)?;

        let new_root = btree.root_page();

        // Track the root change in the transaction - commit will update metadata
        tx.set_collection_root(&self.name, new_root);

        tx.commit()?;

        Ok(())
    }

    pub fn delete_by_id(&self, id: &str) -> Result<()> {
        let mut tx = self.db.begin()?;
        let _tx_id = tx.mvcc_tx_id;

        let metadata = self.db.get_metadata();
        let btree_root = metadata.collections
            .get(&self.name)
            .ok_or_else(|| Error::Other(format!("collection {} not found", self.name)))?
            .btree_root;

        if btree_root == 0 {
            return Err(Error::Other("document not found".to_string()));
        }

        let pager = tx.get_pager();
        let btree = BTree::open(pager.clone(), btree_root);

        let page_num = btree.search(id)?;

        delete_document(&pager, page_num)?;

        btree.delete(id)?;

        let new_root = btree.root_page();

        // Track the root change in the transaction - commit will update metadata
        tx.set_collection_root(&self.name, new_root);

        tx.commit()?;

        Ok(())
    }

    pub fn count_with_query(&self, query: Option<&str>) -> Result<usize> {
        if let Some(q) = query {
            let docs = self.find(q)?;
            Ok(docs.len())
        } else {
            self.count()
        }
    }

    pub fn count(&self) -> Result<usize> {
        let tx = self.db.begin()?;

        let metadata = self.db.get_metadata();
        let btree_root = metadata.collections
            .get(&self.name)
            .ok_or_else(|| Error::Other(format!("collection {} not found", self.name)))?
            .btree_root;

        if btree_root == 0 {
            return Ok(0);
        }

        let pager = tx.get_pager();
        let btree = BTree::open(pager.clone(), btree_root);

        let mut count = 0;
        let tx_writes_arc = tx.get_writes_arc();
        let tx_writes = tx_writes_arc.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;

        let mut iter = btree.iterator()?;
        while iter.next() {
            let (_doc_id, page_num) = iter.entry();
            match read_versioned_document(&pager, page_num, &*tx_writes) {
                Ok(vdoc) => {
                    if vdoc.is_visible(tx.snapshot_id) {
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

    pub fn find(&self, query: &str) -> Result<Vec<Value>> {
        let ast = parse_query(query)
            .map_err(|e| Error::Other(format!("failed to parse query: {}", e)))?;

        let all_docs = self.find_all()?;
        let mut results = Vec::new();

        for doc in all_docs {
            if let Some(doc_map) = doc.as_object() {
                if ast.eval(doc_map) {
                    results.push(doc);
                }
            }
        }

        Ok(results)
    }

    pub fn find_one(&self, query: &str) -> Result<Option<Value>> {
        let docs = self.find(query)?;
        Ok(docs.into_iter().next())
    }

    pub fn update(&self, query: &str, updates: Value) -> Result<usize> {
        if !updates.is_object() {
            return Err(Error::Other("updates must be an object".to_string()));
        }

        let docs = self.find(query)?;
        let mut count = 0;

        for doc in docs {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                self.update_by_id(id, updates.clone())?;
                count += 1;
            }
        }

        Ok(count)
    }

    pub fn update_one(&self, query: &str, updates: Value) -> Result<bool> {
        if let Some(doc) = self.find_one(query)? {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                self.update_by_id(id, updates)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn delete(&self, query: &str) -> Result<usize> {
        let docs = self.find(query)?;
        let mut count = 0;

        for doc in docs {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                self.delete_by_id(id)?;
                count += 1;
            }
        }

        Ok(count)
    }

    pub fn delete_one(&self, query: &str) -> Result<bool> {
        if let Some(doc) = self.find_one(query)? {
            if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                self.delete_by_id(id)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn insert_many(&self, docs: Vec<Value>) -> Result<Vec<String>> {
        if docs.is_empty() {
            return Ok(Vec::new());
        }

        // Check bulk operation size limit
        let max_bulk_ops = self.db.max_bulk_operations();
        if docs.len() > max_bulk_ops {
            return Err(Error::BulkOperationTooLarge {
                count: docs.len(),
                limit: max_bulk_ops,
            });
        }

        // Execute all inserts in a single transaction
        let mut tx = self.db.begin()?;
        let tx_id = tx.mvcc_tx_id;

        let metadata = self.db.get_metadata();
        let btree_root = metadata.collections
            .get(&self.name)
            .map(|c| c.btree_root)
            .unwrap_or(0);

        let pager = tx.get_pager().clone();
        let btree = if btree_root == 0 {
            BTree::new(pager.clone())?
        } else {
            BTree::open(pager.clone(), btree_root)
        };

        let mut ids = Vec::new();

        // Insert each document within the same transaction
        for doc in docs {
            let id = execute_insert(self, &btree, &pager, &mut tx, tx_id, doc)?;
            ids.push(id);
        }

        // Update metadata with new btree root
        let new_root = btree.root_page();
        self.db.update_metadata(|m| {
            let coll = m.get_collection(&self.name);
            coll.btree_root = new_root;
        })?;

        // Commit the transaction - all or nothing
        tx.commit()?;

        Ok(ids)
    }

    /// Upsert a document by ID - update if exists, insert if not
    pub fn upsert_by_id(&self, id: &str, doc: Value) -> Result<UpsertResult> {
        if !doc.is_object() {
            return Err(Error::Other("document must be an object".to_string()));
        }

        // Try to find existing document
        let exists = self.find_by_id(id).is_ok();

        if exists {
            // Update existing document
            self.update_by_id(id, doc)?;
            Ok(UpsertResult::Updated(id.to_string()))
        } else {
            // Insert new document with the specified ID
            let mut doc_map = doc.as_object()
                .ok_or_else(|| Error::Other("document must be an object".to_string()))?
                .clone();

            // Set the _id field
            doc_map.insert("_id".to_string(), Value::String(id.to_string()));

            self.insert(Value::Object(doc_map))?;
            Ok(UpsertResult::Inserted(id.to_string()))
        }
    }

    /// Upsert using a query - update first match if exists, insert if not
    pub fn upsert(&self, query: &str, doc: Value) -> Result<UpsertResult> {
        if !doc.is_object() {
            return Err(Error::Other("document must be an object".to_string()));
        }

        // Try to find existing document matching the query
        // Handle case where collection doesn't exist yet
        let existing = match self.find_one(query) {
            Ok(doc) => doc,
            Err(Error::Other(msg)) if msg.contains("not found") => None,
            Err(e) => return Err(e),
        };

        if let Some(existing_doc) = existing {
            // Extract the ID and update
            if let Some(id) = existing_doc.get("_id").and_then(|v| v.as_str()) {
                self.update_by_id(id, doc)?;
                return Ok(UpsertResult::Updated(id.to_string()));
            }
        }

        // No match found - insert new document
        let id = self.insert(doc)?;
        Ok(UpsertResult::Inserted(id))
    }

    /// Get distinct values for a field across all documents
    pub fn distinct(&self, field: &str) -> Result<Vec<Value>> {
        use std::collections::HashSet;

        let all_docs = match self.find_all() {
            Ok(docs) => docs,
            Err(Error::Other(msg)) if msg.contains("not found") => Vec::new(),
            Err(e) => return Err(e),
        };

        let mut seen = HashSet::new();
        let mut results = Vec::new();

        for doc in all_docs {
            if let Some(doc_map) = doc.as_object() {
                let value = get_nested_field_value(doc_map, field);

                // Handle array fields - extract all values from arrays
                if let Value::Array(arr) = &value {
                    for item in arr {
                        let value_str = serde_json::to_string(item).unwrap_or_default();
                        if seen.insert(value_str.clone()) {
                            results.push(item.clone());
                        }
                    }
                } else {
                    // Regular field
                    let value_str = serde_json::to_string(&value).unwrap_or_default();
                    if seen.insert(value_str) {
                        results.push(value);
                    }
                }
            }
        }

        Ok(results)
    }

    /// Count distinct values for a field
    pub fn count_distinct(&self, field: &str) -> Result<usize> {
        let distinct_values = self.distinct(field)?;
        Ok(distinct_values.len())
    }

    /// Watch for changes to documents in this collection
    ///
    /// # Example
    /// ```no_run
    /// use jasonisnthappy::Database;
    ///
    /// # fn main() -> jasonisnthappy::Result<()> {
    /// let db = Database::open("my.db")?;
    /// let collection = db.collection("users");
    /// let (handle, rx) = collection.watch()
    ///     .filter("age > 18")
    ///     .subscribe()?;
    ///
    /// // In another thread
    /// while let Ok(event) = rx.recv() {
    ///     println!("Change: {:?}", event);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn watch(&self) -> WatchBuilder<'_> {
        WatchBuilder::new(&self.name, self.db.get_watchers())
    }

    /// Perform full-text search on indexed fields
    ///
    /// Returns documents sorted by relevance (highest score first).
    /// This method requires a text index to be created on the collection first.
    ///
    /// # Arguments
    /// * `query` - Search query string (tokenized and matched against indexed fields)
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # use serde_json::json;
    /// # let db = Database::open("my.db").unwrap();
    /// # let posts = db.collection("posts");
    /// # db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();
    /// // Search for documents containing "rust database"
    /// let results = posts.search("rust database").unwrap();
    ///
    /// for result in results {
    ///     println!("Document: {} (score: {})", result.doc_id, result.score);
    ///     let doc = posts.find_by_id(&result.doc_id).unwrap();
    ///     println!("{:?}", doc);
    /// }
    /// ```
    pub fn search(&self, query: &str) -> Result<Vec<crate::core::text_search::SearchResult>> {
        use crate::core::text_search::TextIndex;
        use crate::core::btree::BTree;

        // Find the first text index for this collection
        let (text_index_meta, fields) = {
            let metadata = self.db.get_metadata();
            let coll_meta = metadata.collections.get(&self.name);

            let coll_meta = match coll_meta {
                Some(meta) => meta,
                None => {
                    return Err(Error::Other(format!(
                        "collection {} does not exist",
                        self.name
                    )));
                }
            };

            if coll_meta.text_indexes.is_empty() {
                return Err(Error::Other(format!(
                    "no text index exists on collection {}. Create one with db.create_text_index()",
                    self.name
                )));
            }

            // Use the first text index
            let (_, text_index_meta) = coll_meta.text_indexes.iter().next()
                .ok_or_else(|| Error::Other("text index metadata corrupted".to_string()))?;
            (text_index_meta.clone(), text_index_meta.fields.clone())
        };

        // Load the text index B-tree
        let index_btree = BTree::open(self.db.get_pager(), text_index_meta.btree_root);
        let text_index = TextIndex::new(index_btree, fields);

        // Get total document count for IDF calculation
        let total_docs = self.count()?;

        // Perform search
        text_index.search(query, total_docs)
    }

    // ========== TYPED DOCUMENT METHODS ==========
    // These methods provide type-safe wrappers around the Value-based methods

    /// Insert a typed document into the collection
    ///
    /// # Example
    /// ```no_run
    /// use jasonisnthappy::Database;
    /// use serde::{Serialize, Deserialize};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct User {
    ///     name: String,
    ///     age: u32,
    /// }
    ///
    /// # fn main() -> jasonisnthappy::Result<()> {
    /// let db = Database::open("my.db")?;
    /// let collection = db.collection("users");
    /// let user = User { name: "Alice".to_string(), age: 30 };
    /// let id = collection.insert_typed(&user)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_typed<T: Serialize>(&self, doc: &T) -> Result<String> {
        let value = serde_json::to_value(doc)
            .map_err(|e| Error::Other(format!("Failed to serialize document: {}", e)))?;
        self.insert(value)
    }

    /// Insert multiple typed documents into the collection
    pub fn insert_many_typed<T: Serialize>(&self, docs: Vec<T>) -> Result<Vec<String>> {
        let values: Result<Vec<Value>> = docs
            .iter()
            .map(|doc| {
                serde_json::to_value(doc)
                    .map_err(|e| Error::Other(format!("Failed to serialize document: {}", e)))
            })
            .collect();
        self.insert_many(values?)
    }

    /// Find a typed document by ID
    ///
    /// # Example
    /// ```no_run
    /// use jasonisnthappy::Database;
    /// use serde::{Serialize, Deserialize};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct User {
    ///     name: String,
    ///     age: u32,
    /// }
    ///
    /// # fn main() -> jasonisnthappy::Result<()> {
    /// let db = Database::open("my.db")?;
    /// let collection = db.collection("users");
    /// let user: Option<User> = collection.find_by_id_typed("user_123")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn find_by_id_typed<T: DeserializeOwned>(&self, id: &str) -> Result<Option<T>> {
        match self.find_by_id(id) {
            Ok(value) => {
                let typed = serde_json::from_value(value)
                    .map_err(|e| Error::Other(format!("Failed to deserialize document: {}", e)))?;
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
                    .map_err(|e| Error::Other(format!("Failed to deserialize document: {}", e)))
            })
            .collect()
    }

    /// Find typed documents matching a query
    pub fn find_typed<T: DeserializeOwned>(&self, query: &str) -> Result<Vec<T>> {
        let values = self.find(query)?;
        values
            .into_iter()
            .map(|value| {
                serde_json::from_value(value)
                    .map_err(|e| Error::Other(format!("Failed to deserialize document: {}", e)))
            })
            .collect()
    }

    /// Find one typed document matching a query
    pub fn find_one_typed<T: DeserializeOwned>(&self, query: &str) -> Result<Option<T>> {
        match self.find_one(query)? {
            Some(value) => {
                let typed = serde_json::from_value(value)
                    .map_err(|e| Error::Other(format!("Failed to deserialize document: {}", e)))?;
                Ok(Some(typed))
            }
            None => Ok(None),
        }
    }

    /// Update a typed document by ID
    pub fn update_by_id_typed<T: Serialize>(&self, id: &str, updates: &T) -> Result<()> {
        let value = serde_json::to_value(updates)
            .map_err(|e| Error::Other(format!("Failed to serialize updates: {}", e)))?;
        self.update_by_id(id, value)
    }

    /// Update typed documents matching a query
    pub fn update_typed<T: Serialize>(&self, query: &str, updates: &T) -> Result<usize> {
        let value = serde_json::to_value(updates)
            .map_err(|e| Error::Other(format!("Failed to serialize updates: {}", e)))?;
        self.update(query, value)
    }

    /// Update one typed document matching a query
    pub fn update_one_typed<T: Serialize>(&self, query: &str, updates: &T) -> Result<bool> {
        let value = serde_json::to_value(updates)
            .map_err(|e| Error::Other(format!("Failed to serialize updates: {}", e)))?;
        self.update_one(query, value)
    }

    /// Upsert a typed document by ID
    pub fn upsert_by_id_typed<T: Serialize>(&self, id: &str, doc: &T) -> Result<UpsertResult> {
        let value = serde_json::to_value(doc)
            .map_err(|e| Error::Other(format!("Failed to serialize document: {}", e)))?;
        self.upsert_by_id(id, value)
    }

    /// Upsert a typed document matching a query
    pub fn upsert_typed<T: Serialize>(&self, query: &str, doc: &T) -> Result<UpsertResult> {
        let value = serde_json::to_value(doc)
            .map_err(|e| Error::Other(format!("Failed to serialize document: {}", e)))?;
        self.upsert(query, value)
    }
}

/// Helper function to get nested field value from a document map
fn get_nested_field_value(doc: &serde_json::Map<String, Value>, field: &str) -> Value {
    let parts: Vec<&str> = field.split('.').collect();
    let mut current = Value::Object(doc.clone());

    for part in parts {
        if let Some(obj) = current.as_object() {
            current = obj.get(part).cloned().unwrap_or(Value::Null);
        } else {
            return Value::Null;
        }
    }

    current
}

fn generate_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash, Hasher};

    let random_state = RandomState::new();
    let mut hasher = random_state.build_hasher();
    timestamp.hash(&mut hasher);
    let random_part = hasher.finish();

    format!("{}_{:x}", timestamp, random_part)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;

    #[test]
    fn test_generate_id() {
        let id1 = generate_id();
        let id2 = generate_id();

        assert!(!id1.is_empty());
        assert!(!id2.is_empty());
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_collection_name() {
        let path = "/tmp/test_collection_name.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        assert_eq!(coll.name(), "users");

        db.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_collection_insert_find() {
        let path = "/tmp/test_collection_insert.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let doc = json!({"name": "Alice", "age": 30});
        let id = coll.insert(doc).unwrap();

        let found = coll.find_by_id(&id).unwrap();
        assert_eq!(found["name"], "Alice");
        assert_eq!(found["age"], 30);

        db.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_collection_find_with_query() {
        let path = "/tmp/test_collection_find_query.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        coll.insert(json!({"name": "Alice", "age": 30, "city": "NYC"})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25, "city": "LA"})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 35, "city": "NYC"})).unwrap();

        let results = coll.find("age > 28").unwrap();
        assert_eq!(results.len(), 2);

        let results = coll.find("city is \"NYC\"").unwrap();
        assert_eq!(results.len(), 2);

        let results = coll.find("age > 28 and city is \"NYC\"").unwrap();
        assert_eq!(results.len(), 2);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_collection_find_one() {
        let path = "/tmp/test_collection_find_one.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25})).unwrap();

        let result = coll.find_one("age > 28").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap()["name"], "Alice");

        let result = coll.find_one("age > 100").unwrap();
        assert!(result.is_none());

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_collection_update_with_query() {
        let path = "/tmp/test_collection_update_query.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 35})).unwrap();

        let count = coll.update("age > 28", json!({"status": "senior"})).unwrap();
        assert_eq!(count, 2);

        let results = coll.find("status is \"senior\"").unwrap();
        assert_eq!(results.len(), 2);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_collection_update_one() {
        let path = "/tmp/test_collection_update_one.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 35})).unwrap();

        let updated = coll.update_one("age > 28", json!({"status": "updated"})).unwrap();
        assert!(updated);

        let results = coll.find("status is \"updated\"").unwrap();
        assert_eq!(results.len(), 1);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_collection_delete_with_query() {
        let path = "/tmp/test_collection_delete_query.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 35})).unwrap();

        let count = coll.delete("age > 28").unwrap();
        assert_eq!(count, 2);

        let remaining = coll.find_all().unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0]["name"], "Bob");

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_collection_delete_one() {
        let path = "/tmp/test_collection_delete_one.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 35})).unwrap();

        let deleted = coll.delete_one("age > 28").unwrap();
        assert!(deleted);

        let remaining = coll.find_all().unwrap();
        assert_eq!(remaining.len(), 1);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_collection_insert_many() {
        let path = "/tmp/test_collection_insert_many.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let docs = vec![
            json!({"name": "Alice", "age": 30}),
            json!({"name": "Bob", "age": 25}),
            json!({"name": "Charlie", "age": 35}),
        ];

        let ids = coll.insert_many(docs).unwrap();
        assert_eq!(ids.len(), 3);

        let all = coll.find_all().unwrap();
        assert_eq!(all.len(), 3);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_collection_count_with_query() {
        let path = "/tmp/test_collection_count_query.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 35})).unwrap();

        let count = coll.count_with_query(None).unwrap();
        assert_eq!(count, 3);

        let count = coll.count_with_query(Some("age > 28")).unwrap();
        assert_eq!(count, 2);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_upsert_by_id_insert() {
        let path = "/tmp/test_upsert_by_id_insert.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        // Upsert a new document
        let result = coll.upsert_by_id("user1", json!({
            "name": "Alice",
            "age": 30
        })).unwrap();

        assert_eq!(result, UpsertResult::Inserted("user1".to_string()));

        // Verify it was inserted
        let doc = coll.find_by_id("user1").unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["age"], 30);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_upsert_by_id_update() {
        let path = "/tmp/test_upsert_by_id_update.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        // Insert a document first
        coll.insert(json!({"_id": "user1", "name": "Alice", "age": 30})).unwrap();

        // Upsert the same ID - should update
        let result = coll.upsert_by_id("user1", json!({
            "name": "Alice",
            "age": 31,
            "city": "NYC"
        })).unwrap();

        assert_eq!(result, UpsertResult::Updated("user1".to_string()));

        // Verify it was updated
        let doc = coll.find_by_id("user1").unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["age"], 31);
        assert_eq!(doc["city"], "NYC");

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_upsert_by_query_insert() {
        let path = "/tmp/test_upsert_by_query_insert.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        // Upsert with a query that doesn't match anything
        let result = coll.upsert("email is \"alice@example.com\"", json!({
            "name": "Alice",
            "email": "alice@example.com",
            "age": 30
        })).unwrap();

        match result {
            UpsertResult::Inserted(id) => {
                // Verify the document was inserted
                let doc = coll.find_by_id(&id).unwrap();
                assert_eq!(doc["name"], "Alice");
                assert_eq!(doc["email"], "alice@example.com");
            }
            UpsertResult::Updated(_) => panic!("Expected insert, got update"),
        }

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_upsert_by_query_update() {
        let path = "/tmp/test_upsert_by_query_update.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        // Insert a document first
        let id = coll.insert(json!({
            "name": "Alice",
            "email": "alice@example.com",
            "age": 30
        })).unwrap();

        // Upsert with a matching query - should update
        let result = coll.upsert("email is \"alice@example.com\"", json!({
            "name": "Alice Updated",
            "email": "alice@example.com",
            "age": 31,
            "city": "NYC"
        })).unwrap();

        assert_eq!(result, UpsertResult::Updated(id.clone()));

        // Verify it was updated
        let doc = coll.find_by_id(&id).unwrap();
        assert_eq!(doc["name"], "Alice Updated");
        assert_eq!(doc["age"], 31);
        assert_eq!(doc["city"], "NYC");

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_upsert_idempotency() {
        let path = "/tmp/test_upsert_idempotency.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "sessions".to_string());

        let session_id = "session_123";

        // First upsert - should insert
        let result1 = coll.upsert_by_id(session_id, json!({
            "user_id": "user1",
            "created_at": "2024-01-01T00:00:00Z"
        })).unwrap();
        assert_eq!(result1, UpsertResult::Inserted(session_id.to_string()));

        // Second upsert with same ID - should update
        let result2 = coll.upsert_by_id(session_id, json!({
            "user_id": "user1",
            "created_at": "2024-01-01T00:00:00Z",
            "last_accessed": "2024-01-01T01:00:00Z"
        })).unwrap();
        assert_eq!(result2, UpsertResult::Updated(session_id.to_string()));

        // Third upsert - should still update
        let result3 = coll.upsert_by_id(session_id, json!({
            "user_id": "user1",
            "created_at": "2024-01-01T00:00:00Z",
            "last_accessed": "2024-01-01T02:00:00Z"
        })).unwrap();
        assert_eq!(result3, UpsertResult::Updated(session_id.to_string()));

        // Should only have one document
        let count = coll.count().unwrap();
        assert_eq!(count, 1);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_upsert_race_condition_prevention() {
        let path = "/tmp/test_upsert_race.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "metrics".to_string());

        // Simulate counter increment pattern
        // Multiple upserts should not create duplicates
        for _ in 0..10 {
            let _ = coll.upsert("metric_name is \"page_views\"", json!({
                "metric_name": "page_views",
                "count": 1
            }));
        }

        // Should only have one document, not 10
        let results = coll.find("metric_name is \"page_views\"").unwrap();
        assert_eq!(results.len(), 1);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_distinct_simple() {
        let path = "/tmp/test_distinct_simple.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        coll.insert(json!({"name": "Alice", "city": "NYC"})).unwrap();
        coll.insert(json!({"name": "Bob", "city": "LA"})).unwrap();
        coll.insert(json!({"name": "Charlie", "city": "NYC"})).unwrap();
        coll.insert(json!({"name": "David", "city": "SF"})).unwrap();
        coll.insert(json!({"name": "Eve", "city": "LA"})).unwrap();

        let cities = coll.distinct("city").unwrap();
        assert_eq!(cities.len(), 3);

        // Convert to strings for easier comparison
        let mut city_strs: Vec<String> = cities.iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        city_strs.sort();

        assert_eq!(city_strs, vec!["LA", "NYC", "SF"]);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_distinct_with_nulls() {
        let path = "/tmp/test_distinct_nulls.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        coll.insert(json!({"name": "Alice", "city": "NYC"})).unwrap();
        coll.insert(json!({"name": "Bob"})).unwrap(); // No city
        coll.insert(json!({"name": "Charlie", "city": "NYC"})).unwrap();
        coll.insert(json!({"name": "David"})).unwrap(); // No city

        let cities = coll.distinct("city").unwrap();
        assert_eq!(cities.len(), 2); // "NYC" and null

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_distinct_nested_field() {
        let path = "/tmp/test_distinct_nested.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        coll.insert(json!({
            "name": "Alice",
            "address": {"city": "NYC", "country": "USA"}
        })).unwrap();
        coll.insert(json!({
            "name": "Bob",
            "address": {"city": "London", "country": "UK"}
        })).unwrap();
        coll.insert(json!({
            "name": "Charlie",
            "address": {"city": "NYC", "country": "USA"}
        })).unwrap();

        let cities = coll.distinct("address.city").unwrap();
        assert_eq!(cities.len(), 2);

        let mut city_strs: Vec<String> = cities.iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        city_strs.sort();

        assert_eq!(city_strs, vec!["London", "NYC"]);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_distinct_array_field() {
        let path = "/tmp/test_distinct_array.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "posts".to_string());

        coll.insert(json!({
            "title": "Post 1",
            "tags": ["rust", "database", "performance"]
        })).unwrap();
        coll.insert(json!({
            "title": "Post 2",
            "tags": ["rust", "web", "async"]
        })).unwrap();
        coll.insert(json!({
            "title": "Post 3",
            "tags": ["database", "sql", "performance"]
        })).unwrap();

        let tags = coll.distinct("tags").unwrap();
        assert_eq!(tags.len(), 6); // rust, database, performance, web, async, sql

        let mut tag_strs: Vec<String> = tags.iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        tag_strs.sort();

        assert_eq!(tag_strs, vec!["async", "database", "performance", "rust", "sql", "web"]);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_count_distinct() {
        let path = "/tmp/test_count_distinct.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        for i in 1..=100 {
            coll.insert(json!({
                "name": format!("User{}", i),
                "city": if i % 3 == 0 { "NYC" } else if i % 3 == 1 { "LA" } else { "SF" }
            })).unwrap();
        }

        let count = coll.count_distinct("city").unwrap();
        assert_eq!(count, 3);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_distinct_empty_collection() {
        let path = "/tmp/test_distinct_empty.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let cities = coll.distinct("city").unwrap();
        assert_eq!(cities.len(), 0);

        let count = coll.count_distinct("city").unwrap();
        assert_eq!(count, 0);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_distinct_numbers() {
        let path = "/tmp/test_distinct_numbers.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "data".to_string());

        coll.insert(json!({"value": 1})).unwrap();
        coll.insert(json!({"value": 2})).unwrap();
        coll.insert(json!({"value": 1})).unwrap();
        coll.insert(json!({"value": 3})).unwrap();
        coll.insert(json!({"value": 2})).unwrap();

        let values = coll.distinct("value").unwrap();
        assert_eq!(values.len(), 3);

        let mut nums: Vec<i64> = values.iter()
            .map(|v| v.as_i64().unwrap())
            .collect();
        nums.sort();

        assert_eq!(nums, vec![1, 2, 3]);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_bulk_write_inserts() {
        let path = "/tmp/test_bulk_write_inserts.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let result = coll.bulk_write()
            .insert(json!({"name": "Alice", "age": 30}))
            .insert(json!({"name": "Bob", "age": 25}))
            .insert(json!({"name": "Charlie", "age": 35}))
            .execute()
            .unwrap();

        assert_eq!(result.inserted_count, 3);
        assert_eq!(result.updated_count, 0);
        assert_eq!(result.deleted_count, 0);
        assert_eq!(result.errors.len(), 0);

        let all = coll.find_all().unwrap();
        assert_eq!(all.len(), 3);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_bulk_write_mixed_operations() {
        let path = "/tmp/test_bulk_write_mixed.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        // First, insert some initial data
        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 25})).unwrap();

        // Now perform bulk operations
        let result = coll.bulk_write()
            .insert(json!({"name": "Charlie", "age": 35}))
            .update_one("name is \"Alice\"", json!({"age": 31}))
            .delete_one("name is \"Bob\"")
            .execute()
            .unwrap();

        assert_eq!(result.inserted_count, 1);
        assert_eq!(result.updated_count, 1);
        assert_eq!(result.deleted_count, 1);
        assert_eq!(result.errors.len(), 0);

        // Verify results
        let all = coll.find_all().unwrap();
        assert_eq!(all.len(), 2); // Alice (updated) and Charlie (inserted)

        let alice = coll.find_one("name is \"Alice\"").unwrap().unwrap();
        assert_eq!(alice["age"], 31);

        let bob_result = coll.find_one("name is \"Bob\"").unwrap();
        assert!(bob_result.is_none());

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_bulk_write_update_many() {
        let path = "/tmp/test_bulk_write_update_many.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        // Insert initial data
        coll.insert(json!({"name": "Alice", "age": 30, "city": "NYC"})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 35, "city": "NYC"})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 25, "city": "LA"})).unwrap();

        let result = coll.bulk_write()
            .update_many("city is \"NYC\"", json!({"status": "updated"}))
            .execute()
            .unwrap();

        assert_eq!(result.updated_count, 2);
        assert_eq!(result.inserted_count, 0);
        assert_eq!(result.deleted_count, 0);

        let updated = coll.find("status is \"updated\"").unwrap();
        assert_eq!(updated.len(), 2);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_bulk_write_delete_many() {
        let path = "/tmp/test_bulk_write_delete_many.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        // Insert initial data
        coll.insert(json!({"name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"name": "Bob", "age": 35})).unwrap();
        coll.insert(json!({"name": "Charlie", "age": 40})).unwrap();
        coll.insert(json!({"name": "David", "age": 25})).unwrap();

        let result = coll.bulk_write()
            .delete_many("age > 30")
            .execute()
            .unwrap();

        assert_eq!(result.deleted_count, 2); // Bob and Charlie
        assert_eq!(result.inserted_count, 0);
        assert_eq!(result.updated_count, 0);

        let remaining = coll.find_all().unwrap();
        assert_eq!(remaining.len(), 2); // Alice and David

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_bulk_write_ordered_with_error() {
        let path = "/tmp/test_bulk_write_ordered_error.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        // Try to insert with duplicate ID
        let result = coll.bulk_write()
            .insert(json!({"_id": "user1", "name": "Alice"}))
            .insert(json!({"_id": "user1", "name": "Bob"})) // Duplicate ID
            .insert(json!({"_id": "user2", "name": "Charlie"}))
            .ordered(true)
            .execute();

        // Should fail in ordered mode
        assert!(result.is_err());

        // First insert should be rolled back due to transaction failure
        // Collection might not exist since transaction was rolled back
        let count = coll.count().unwrap_or(0);
        assert_eq!(count, 0);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_bulk_write_unordered_with_error() {
        let path = "/tmp/test_bulk_write_unordered_error.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        // Try to insert with duplicate ID in unordered mode
        let result = coll.bulk_write()
            .insert(json!({"_id": "user1", "name": "Alice"}))
            .insert(json!({"_id": "user1", "name": "Bob"})) // Duplicate ID
            .insert(json!({"_id": "user2", "name": "Charlie"}))
            .ordered(false)
            .execute()
            .unwrap();

        // Should succeed but with errors
        assert_eq!(result.inserted_count, 2); // user1 and user2
        assert_eq!(result.errors.len(), 1); // One error for duplicate
        assert_eq!(result.errors[0].operation_index, 1);

        let count = coll.count().unwrap();
        assert_eq!(count, 2);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_bulk_write_empty() {
        let path = "/tmp/test_bulk_write_empty.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let result = coll.bulk_write().execute().unwrap();

        assert_eq!(result.inserted_count, 0);
        assert_eq!(result.updated_count, 0);
        assert_eq!(result.deleted_count, 0);
        assert_eq!(result.errors.len(), 0);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_bulk_write_transaction_atomicity() {
        let path = "/tmp/test_bulk_write_atomicity.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        // Insert initial data with different scenario
        coll.insert(json!({"_id": "alice", "name": "Alice", "age": 30})).unwrap();
        coll.insert(json!({"_id": "bob", "name": "Bob", "age": 25})).unwrap();
        let initial_count = coll.count().unwrap();
        assert_eq!(initial_count, 2);

        // This should fail due to duplicate ID and rollback everything
        let result = coll.bulk_write()
            .insert(json!({"_id": "new1", "name": "Charlie"}))
            .insert(json!({"_id": "new2", "name": "David"}))
            .insert(json!({"_id": "alice", "name": "Duplicate"})) // Duplicate ID - should fail
            .ordered(true)
            .execute();

        // Should fail in ordered mode
        assert!(result.is_err());

        // No new documents should exist due to rollback
        let final_count = coll.count().unwrap_or(initial_count);
        // In ordered mode with rollback, count should remain the same or collection may not exist
        assert!(final_count <= initial_count, "Count should not increase after failed bulk operation");

        // Verify no document with ID "new1" or "new2" exists
        let new1 = coll.find_by_id("new1");
        assert!(new1.is_err() || new1.is_ok() && new1.unwrap() == serde_json::Value::Null);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_bulk_write_large_batch() {
        let path = "/tmp/test_bulk_write_large.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let mut bulk = coll.bulk_write();
        for i in 0..100 {
            bulk = bulk.insert(json!({
                "name": format!("User{}", i),
                "index": i
            }));
        }

        let result = bulk.execute().unwrap();

        assert_eq!(result.inserted_count, 100);
        assert_eq!(result.errors.len(), 0);

        let count = coll.count().unwrap();
        assert_eq!(count, 100);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    // ========== TYPED DOCUMENT TESTS ==========

    use serde::{Serialize, Deserialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct User {
        #[serde(skip_serializing_if = "Option::is_none")]
        _id: Option<String>,
        name: String,
        age: u32,
        email: String,
    }

    #[test]
    fn test_insert_typed() {
        let path = "/tmp/test_insert_typed.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let user = User {
            _id: None,
            name: "Alice".to_string(),
            age: 30,
            email: "alice@example.com".to_string(),
        };

        let id = coll.insert_typed(&user).unwrap();
        assert!(!id.is_empty());

        // Verify the document was inserted
        let found: Option<User> = coll.find_by_id_typed(&id).unwrap();
        assert!(found.is_some());
        let found_user = found.unwrap();
        assert_eq!(found_user.name, "Alice");
        assert_eq!(found_user.age, 30);
        assert_eq!(found_user.email, "alice@example.com");

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_insert_many_typed() {
        let path = "/tmp/test_insert_many_typed.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let users = vec![
            User {
                _id: None,
                name: "Alice".to_string(),
                age: 30,
                email: "alice@example.com".to_string(),
            },
            User {
                _id: None,
                name: "Bob".to_string(),
                age: 25,
                email: "bob@example.com".to_string(),
            },
        ];

        let ids = coll.insert_many_typed(users).unwrap();
        assert_eq!(ids.len(), 2);

        let count = coll.count().unwrap();
        assert_eq!(count, 2);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_find_by_id_typed() {
        let path = "/tmp/test_find_by_id_typed.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let user = User {
            _id: None,
            name: "Alice".to_string(),
            age: 30,
            email: "alice@example.com".to_string(),
        };

        let id = coll.insert_typed(&user).unwrap();

        // Find existing document
        let found: Option<User> = coll.find_by_id_typed(&id).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Alice");

        // Find non-existent document
        let not_found: Option<User> = coll.find_by_id_typed("nonexistent").unwrap();
        assert!(not_found.is_none());

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_find_all_typed() {
        let path = "/tmp/test_find_all_typed.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let users = vec![
            User {
                _id: None,
                name: "Alice".to_string(),
                age: 30,
                email: "alice@example.com".to_string(),
            },
            User {
                _id: None,
                name: "Bob".to_string(),
                age: 25,
                email: "bob@example.com".to_string(),
            },
        ];

        coll.insert_many_typed(users).unwrap();

        let all_users: Vec<User> = coll.find_all_typed().unwrap();
        assert_eq!(all_users.len(), 2);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_find_typed() {
        let path = "/tmp/test_find_typed.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let users = vec![
            User {
                _id: None,
                name: "Alice".to_string(),
                age: 30,
                email: "alice@example.com".to_string(),
            },
            User {
                _id: None,
                name: "Bob".to_string(),
                age: 25,
                email: "bob@example.com".to_string(),
            },
            User {
                _id: None,
                name: "Charlie".to_string(),
                age: 35,
                email: "charlie@example.com".to_string(),
            },
        ];

        coll.insert_many_typed(users).unwrap();

        // Find users older than 28
        let found_users: Vec<User> = coll.find_typed("age > 28").unwrap();
        assert_eq!(found_users.len(), 2);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_find_one_typed() {
        let path = "/tmp/test_find_one_typed.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let user = User {
            _id: None,
            name: "Alice".to_string(),
            age: 30,
            email: "alice@example.com".to_string(),
        };

        coll.insert_typed(&user).unwrap();

        let found: Option<User> = coll.find_one_typed("name is \"Alice\"").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Alice");

        let not_found: Option<User> = coll.find_one_typed("name is \"Bob\"").unwrap();
        assert!(not_found.is_none());

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_update_by_id_typed() {
        let path = "/tmp/test_update_by_id_typed.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let user = User {
            _id: None,
            name: "Alice".to_string(),
            age: 30,
            email: "alice@example.com".to_string(),
        };

        let id = coll.insert_typed(&user).unwrap();

        let updates = json!({"age": 31});
        coll.update_by_id(&id, updates).unwrap();

        let updated: Option<User> = coll.find_by_id_typed(&id).unwrap();
        assert_eq!(updated.unwrap().age, 31);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_update_typed() {
        let path = "/tmp/test_update_typed.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let users = vec![
            User {
                _id: None,
                name: "Alice".to_string(),
                age: 30,
                email: "alice@example.com".to_string(),
            },
            User {
                _id: None,
                name: "Bob".to_string(),
                age: 30,
                email: "bob@example.com".to_string(),
            },
        ];

        coll.insert_many_typed(users).unwrap();

        let updates = json!({"age": 31});
        let count = coll.update_typed("age is 30", &updates).unwrap();
        assert_eq!(count, 2);

        let all_users: Vec<User> = coll.find_all_typed().unwrap();
        for user in all_users {
            assert_eq!(user.age, 31);
        }

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_upsert_by_id_typed() {
        let path = "/tmp/test_upsert_by_id_typed.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let user = User {
            _id: None,
            name: "Alice".to_string(),
            age: 30,
            email: "alice@example.com".to_string(),
        };

        // Insert new document
        let result = coll.upsert_by_id_typed("user1", &user).unwrap();
        assert_eq!(result, UpsertResult::Inserted("user1".to_string()));

        // Update existing document
        let updated_user = User {
            _id: None,
            name: "Alice Updated".to_string(),
            age: 31,
            email: "alice@example.com".to_string(),
        };
        let result = coll.upsert_by_id_typed("user1", &updated_user).unwrap();
        assert_eq!(result, UpsertResult::Updated("user1".to_string()));

        let found: Option<User> = coll.find_by_id_typed("user1").unwrap();
        assert_eq!(found.unwrap().age, 31);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_upsert_typed() {
        let path = "/tmp/test_upsert_typed.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        let user = User {
            _id: None,
            name: "Alice".to_string(),
            age: 30,
            email: "alice@example.com".to_string(),
        };

        // Insert new document
        let result = coll.upsert_typed("name is \"Alice\"", &user).unwrap();
        assert!(matches!(result, UpsertResult::Inserted(_)));

        // Update existing document
        let updated_user = User {
            _id: None,
            name: "Alice".to_string(),
            age: 31,
            email: "alice@example.com".to_string(),
        };
        let result = coll.upsert_typed("name is \"Alice\"", &updated_user).unwrap();
        assert!(matches!(result, UpsertResult::Updated(_)));

        let found: Option<User> = coll.find_one_typed("name is \"Alice\"").unwrap();
        assert_eq!(found.unwrap().age, 31);

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_typed_serialization_error() {
        let path = "/tmp/test_typed_serialization_error.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Arc::new(Database::open(path).unwrap());
        let coll = Collection::new(db.clone(), "users".to_string());

        // Insert a document that can't be deserialized as User
        coll.insert(json!({"not_a_user": "data"})).unwrap();

        // This should fail during deserialization
        let result: Result<Vec<User>> = coll.find_all_typed();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(matches!(err, Error::Other(_)));

        db.close().unwrap();
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }
}
