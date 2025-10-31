use napi::bindgen_prelude::*;
use napi::threadsafe_function::ThreadsafeFunctionCallMode;
use napi_derive::napi;
use jasonisnthappy::{
    Database as CoreDatabase,
    Transaction as CoreTransaction,
    Collection as CoreCollection,
    SortOrder,
};
use jasonisnthappy::core::database::{DatabaseOptions, TransactionConfig};
use jasonisnthappy::core::watch::ChangeOperation;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

// ==================
// Database Options
// ==================

#[napi(object)]
pub struct JsDatabaseOptions {
    pub cache_size: Option<u32>,
    pub auto_checkpoint_threshold: Option<u32>,
    pub file_permissions: Option<u32>,
    pub read_only: Option<bool>,
    pub max_bulk_operations: Option<u32>,
    pub max_document_size: Option<u32>,
    pub max_request_body_size: Option<u32>,
}

impl From<JsDatabaseOptions> for DatabaseOptions {
    fn from(opts: JsDatabaseOptions) -> Self {
        let mut db_opts = DatabaseOptions::default();
        if let Some(size) = opts.cache_size {
            db_opts.cache_size = size as usize;
        }
        if let Some(threshold) = opts.auto_checkpoint_threshold {
            db_opts.auto_checkpoint_threshold = threshold as u64;
        }
        if let Some(perms) = opts.file_permissions {
            db_opts.file_permissions = perms;
        }
        if let Some(ro) = opts.read_only {
            db_opts.read_only = ro;
        }
        if let Some(max_bulk) = opts.max_bulk_operations {
            db_opts.max_bulk_operations = max_bulk as usize;
        }
        if let Some(max_doc) = opts.max_document_size {
            db_opts.max_document_size = max_doc as usize;
        }
        if let Some(max_req) = opts.max_request_body_size {
            db_opts.max_request_body_size = max_req as usize;
        }
        db_opts
    }
}

#[napi(object)]
pub struct JsTransactionConfig {
    pub max_retries: Option<u32>,
    pub retry_backoff_base_ms: Option<u32>,
    pub max_retry_backoff_ms: Option<u32>,
}

impl From<JsTransactionConfig> for TransactionConfig {
    fn from(cfg: JsTransactionConfig) -> Self {
        let mut tx_cfg = TransactionConfig::default();
        if let Some(retries) = cfg.max_retries {
            tx_cfg.max_retries = retries as usize;
        }
        if let Some(backoff) = cfg.retry_backoff_base_ms {
            tx_cfg.retry_backoff_base_ms = backoff as u64;
        }
        if let Some(max_backoff) = cfg.max_retry_backoff_ms {
            tx_cfg.max_retry_backoff_ms = max_backoff as u64;
        }
        tx_cfg
    }
}

impl From<TransactionConfig> for JsTransactionConfig {
    fn from(cfg: TransactionConfig) -> Self {
        JsTransactionConfig {
            max_retries: Some(cfg.max_retries as u32),
            retry_backoff_base_ms: Some(cfg.retry_backoff_base_ms as u32),
            max_retry_backoff_ms: Some(cfg.max_retry_backoff_ms as u32),
        }
    }
}

#[napi(object)]
pub struct JsUpsertResult {
    pub id: String,
    pub inserted: bool,
}

// ==================
// Database Class
// ==================

#[napi]
pub struct Database {
    inner: Arc<CoreDatabase>,
}

#[napi]
impl Database {
    /// Opens a database at the specified path
    #[napi(factory)]
    pub fn open(path: String) -> Result<Database> {
        let db = CoreDatabase::open(&path)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(Database {
            inner: Arc::new(db),
        })
    }

    /// Opens a database with custom options
    #[napi(factory)]
    pub fn open_with_options(path: String, options: JsDatabaseOptions) -> Result<Database> {
        let db_opts: DatabaseOptions = options.into();
        let db = CoreDatabase::open_with_options(&path, db_opts)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(Database {
            inner: Arc::new(db),
        })
    }

    /// Closes the database connection
    /// Note: The database is automatically closed when garbage collected,
    /// but this method can be called for explicit cleanup.
    #[napi]
    pub fn close(&self) {
        // The database will be closed when the Arc is dropped.
        // This is a no-op for API compatibility - actual cleanup
        // happens via Rust's Drop trait when all references are gone.
    }

    /// Returns default database options
    #[napi]
    pub fn default_database_options() -> JsDatabaseOptions {
        let opts = DatabaseOptions::default();
        JsDatabaseOptions {
            cache_size: Some(opts.cache_size as u32),
            auto_checkpoint_threshold: Some(opts.auto_checkpoint_threshold as u32),
            file_permissions: Some(opts.file_permissions),
            read_only: Some(opts.read_only),
            max_bulk_operations: Some(opts.max_bulk_operations as u32),
            max_document_size: Some(opts.max_document_size as u32),
            max_request_body_size: Some(opts.max_request_body_size as u32),
        }
    }

    /// Returns default transaction configuration
    #[napi]
    pub fn default_transaction_config() -> JsTransactionConfig {
        TransactionConfig::default().into()
    }

    // Configuration

    /// Sets the transaction configuration
    #[napi]
    pub fn set_transaction_config(&self, config: JsTransactionConfig) {
        self.inner.set_transaction_config(config.into())
    }

    /// Gets the current transaction configuration
    #[napi]
    pub fn get_transaction_config(&self) -> JsTransactionConfig {
        self.inner.get_transaction_config().into()
    }

    /// Sets the auto-checkpoint threshold in WAL frames
    #[napi]
    pub fn set_auto_checkpoint_threshold(&self, threshold: u32) {
        self.inner.set_auto_checkpoint_threshold(threshold as u64)
    }

    // Database Info

    /// Gets the database file path
    #[napi]
    pub fn get_path(&self) -> String {
        self.inner.path().to_string()
    }

    /// Checks if the database is read-only
    #[napi]
    pub fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    /// Returns the maximum number of bulk operations allowed
    #[napi]
    pub fn max_bulk_operations(&self) -> u32 {
        self.inner.max_bulk_operations() as u32
    }

    /// Returns the maximum document size in bytes
    #[napi]
    pub fn max_document_size(&self) -> u32 {
        self.inner.max_document_size() as u32
    }

    /// Returns the maximum HTTP request body size in bytes
    #[napi]
    pub fn max_request_body_size(&self) -> u32 {
        self.inner.max_request_body_size() as u32
    }

    /// Lists all collections in the database
    #[napi]
    pub fn list_collections(&self) -> Result<Vec<String>> {
        self.inner.list_collections()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Gets statistics for a collection
    #[napi(ts_return_type = "any")]
    pub fn collection_stats(&self, collection_name: String) -> Result<serde_json::Value> {
        self.inner.collection_stats(&collection_name)
            .and_then(|stats| serde_json::to_value(stats).map_err(|e| e.into()))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Gets database information
    #[napi(ts_return_type = "any")]
    pub fn database_info(&self) -> Result<serde_json::Value> {
        self.inner.info()
            .and_then(|info| serde_json::to_value(info).map_err(|e| e.into()))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Index Management

    /// Lists all indexes for a collection
    #[napi(ts_return_type = "any[]")]
    pub fn list_indexes(&self, collection_name: String) -> Result<Vec<serde_json::Value>> {
        self.inner.list_indexes(&collection_name)
            .and_then(|indexes| {
                indexes.into_iter()
                    .map(|idx| serde_json::to_value(idx).map_err(|e| e.into()))
                    .collect()
            })
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Creates a single-field index
    #[napi]
    pub fn create_index(
        &self,
        collection_name: String,
        index_name: String,
        field: String,
        unique: bool,
    ) -> Result<()> {
        self.inner.create_index(&collection_name, &index_name, &field, unique)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Creates a compound index on multiple fields
    #[napi]
    pub fn create_compound_index(
        &self,
        collection_name: String,
        index_name: String,
        fields: Vec<String>,
        unique: bool,
    ) -> Result<()> {
        let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
        self.inner.create_compound_index(&collection_name, &index_name, &field_refs, unique)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Creates a full-text search index
    #[napi]
    pub fn create_text_index(
        &self,
        collection_name: String,
        index_name: String,
        field: String,
    ) -> Result<()> {
        self.inner.create_text_index(&collection_name, &index_name, &[&field])
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Drops an index from a collection
    #[napi]
    pub fn drop_index(
        &self,
        collection_name: String,
        index_name: String,
    ) -> Result<()> {
        self.inner.drop_index(&collection_name, &index_name)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Schema Validation

    /// Sets a JSON schema for validation
    #[napi(ts_args_type = "collectionName: string, schema: any")]
    pub fn set_schema(&self, collection_name: String, schema: serde_json::Value) -> Result<()> {
        let schema: jasonisnthappy::Schema = serde_json::from_value(schema)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        self.inner.set_schema(&collection_name, schema)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Gets the JSON schema for a collection
    #[napi(ts_return_type = "any | null")]
    pub fn get_schema(&self, collection_name: String) -> Result<Option<serde_json::Value>> {
        match self.inner.get_schema(&collection_name) {
            Some(schema) => {
                let value = serde_json::to_value(schema)
                    .map_err(|e| Error::from_reason(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Removes the JSON schema from a collection
    #[napi]
    pub fn remove_schema(&self, collection_name: String) -> Result<()> {
        self.inner.remove_schema(&collection_name)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Maintenance

    /// Performs a manual WAL checkpoint
    #[napi]
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Creates a backup of the database
    #[napi]
    pub fn backup(&self, dest_path: String) -> Result<()> {
        self.inner.backup(&dest_path)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Verifies the integrity of a backup
    #[napi(ts_return_type = "any")]
    pub fn verify_backup(backup_path: String) -> Result<serde_json::Value> {
        CoreDatabase::verify_backup(&backup_path)
            .and_then(|info| serde_json::to_value(info).map_err(|e| e.into()))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Performs garbage collection
    #[napi(ts_return_type = "any")]
    pub fn garbage_collect(&self) -> Result<serde_json::Value> {
        self.inner.garbage_collect()
            .and_then(|stats| serde_json::to_value(stats).map_err(|e| e.into()))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Gets database metrics
    #[napi(ts_return_type = "any")]
    pub fn metrics(&self) -> Result<serde_json::Value> {
        let metrics = self.inner.metrics();
        serde_json::to_value(metrics)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Gets the number of WAL frames
    #[napi]
    pub fn frame_count(&self) -> u32 {
        self.inner.frame_count() as u32
    }

    // Transaction Operations

    /// Begins a new transaction
    #[napi]
    pub fn begin_transaction(&self) -> Result<Transaction> {
        let tx = self.inner.begin()
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(Transaction { inner: Some(tx) })
    }

    /// Gets a collection reference for non-transactional operations
    #[napi]
    pub fn get_collection(&self, name: String) -> Collection {
        let coll = self.inner.collection(&name);
        Collection { inner: Some(coll) }
    }

    /// Starts the web UI server at the given address
    /// Returns a WebServer handle that can be used to stop the server
    #[napi]
    pub fn start_web_ui(&self, addr: String) -> Result<WebServer> {
        let server = self.inner.start_web_ui(&addr)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(WebServer { inner: Some(server) })
    }
}

// ==================
// WebServer Class
// ==================

#[napi]
pub struct WebServer {
    inner: Option<jasonisnthappy::core::web_server::WebServer>,
}

#[napi]
impl WebServer {
    /// Stops the web server
    #[napi]
    pub fn stop(&mut self) {
        if let Some(server) = self.inner.take() {
            server.shutdown();
        }
    }
}

// ==================
// Transaction Class
// ==================

#[napi]
pub struct Transaction {
    inner: Option<CoreTransaction>,
}

#[napi]
impl Transaction {
    /// Checks if the transaction is still active
    #[napi]
    pub fn is_active(&self) -> bool {
        self.inner.as_ref().map(|tx| tx.is_active()).unwrap_or(false)
    }

    /// Commits the transaction
    #[napi]
    pub fn commit(&mut self) -> Result<()> {
        let mut tx = self.inner.take()
            .ok_or_else(|| Error::from_reason("Transaction already closed"))?;

        tx.commit()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Rolls back the transaction
    #[napi]
    pub fn rollback(&mut self) -> Result<()> {
        if let Some(mut tx) = self.inner.take() {
            tx.rollback()
                .map_err(|e| Error::from_reason(e.to_string()))?;
        }
        Ok(())
    }

    // Basic CRUD

    /// Inserts a document into a collection
    #[napi(ts_args_type = "collectionName: string, doc: any", ts_return_type = "string")]
    pub fn insert(&mut self, collection_name: String, doc: serde_json::Value) -> Result<String> {
        let tx = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Transaction is closed"))?;

        let mut coll = tx.collection(&collection_name)
            .map_err(|e| Error::from_reason(e.to_string()))?;

        coll.insert(doc)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Finds a document by ID
    #[napi(ts_args_type = "collectionName: string, id: string", ts_return_type = "any")]
    pub fn find_by_id(&mut self, collection_name: String, id: String) -> Result<serde_json::Value> {
        let tx = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Transaction is closed"))?;

        let coll = tx.collection(&collection_name)
            .map_err(|e| Error::from_reason(e.to_string()))?;

        coll.find_by_id(&id)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Updates a document by ID
    #[napi(ts_args_type = "collectionName: string, id: string, doc: any")]
    pub fn update_by_id(&mut self, collection_name: String, id: String, doc: serde_json::Value) -> Result<()> {
        let tx = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Transaction is closed"))?;

        let mut coll = tx.collection(&collection_name)
            .map_err(|e| Error::from_reason(e.to_string()))?;

        coll.update_by_id(&id, doc)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Deletes a document by ID
    #[napi]
    pub fn delete_by_id(&mut self, collection_name: String, id: String) -> Result<()> {
        let tx = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Transaction is closed"))?;

        let mut coll = tx.collection(&collection_name)
            .map_err(|e| Error::from_reason(e.to_string()))?;

        coll.delete_by_id(&id)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Finds all documents in a collection
    #[napi(ts_args_type = "collectionName: string", ts_return_type = "any[]")]
    pub fn find_all(&mut self, collection_name: String) -> Result<Vec<serde_json::Value>> {
        let tx = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Transaction is closed"))?;

        let coll = tx.collection(&collection_name)
            .map_err(|e| Error::from_reason(e.to_string()))?;

        coll.find_all()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Counts documents in a collection
    #[napi]
    pub fn count(&mut self, collection_name: String) -> Result<u32> {
        let tx = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Transaction is closed"))?;

        let coll = tx.collection(&collection_name)
            .map_err(|e| Error::from_reason(e.to_string()))?;

        coll.count()
            .map(|c| c as u32)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Collection Management

    /// Creates a new collection
    #[napi]
    pub fn create_collection(&mut self, collection_name: String) -> Result<()> {
        let tx = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Transaction is closed"))?;

        tx.create_collection(&collection_name)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Drops a collection
    #[napi]
    pub fn drop_collection(&mut self, collection_name: String) -> Result<()> {
        let tx = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Transaction is closed"))?;

        tx.drop_collection(&collection_name)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Renames a collection
    #[napi]
    pub fn rename_collection(&mut self, old_name: String, new_name: String) -> Result<()> {
        let tx = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Transaction is closed"))?;

        tx.rename_collection(&old_name, &new_name)
            .map_err(|e| Error::from_reason(e.to_string()))
    }
}

// ==================
// Collection Class
// ==================

#[napi]
pub struct Collection {
    inner: Option<CoreCollection>,
}

#[napi]
impl Collection {
    /// Gets the collection name
    #[napi]
    pub fn name(&self) -> Result<String> {
        self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))
            .map(|c| c.name().to_string())
    }

    // Basic CRUD

    /// Inserts a document
    #[napi(ts_args_type = "doc: any", ts_return_type = "string")]
    pub fn insert(&mut self, doc: serde_json::Value) -> Result<String> {
        let coll = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.insert(doc)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Finds a document by ID
    #[napi(ts_return_type = "any")]
    pub fn find_by_id(&self, id: String) -> Result<serde_json::Value> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.find_by_id(&id)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Updates a document by ID
    #[napi(ts_args_type = "id: string, doc: any")]
    pub fn update_by_id(&mut self, id: String, doc: serde_json::Value) -> Result<()> {
        let coll = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.update_by_id(&id, doc)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Deletes a document by ID
    #[napi]
    pub fn delete_by_id(&mut self, id: String) -> Result<()> {
        let coll = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.delete_by_id(&id)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Finds all documents
    #[napi(ts_return_type = "any[]")]
    pub fn find_all(&self) -> Result<Vec<serde_json::Value>> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.find_all()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Counts all documents
    #[napi]
    pub fn count(&self) -> Result<u32> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.count()
            .map(|c| c as u32)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Query/Filter Operations

    /// Finds documents matching a filter
    #[napi(ts_return_type = "any[]")]
    pub fn find(&self, filter: String) -> Result<Vec<serde_json::Value>> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.find(&filter)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Finds first document matching a filter
    #[napi(ts_return_type = "any | null")]
    pub fn find_one(&self, filter: String) -> Result<Option<serde_json::Value>> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.find_one(&filter)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Updates all documents matching a filter
    #[napi(ts_args_type = "filter: string, update: any")]
    pub fn update(&mut self, filter: String, update: serde_json::Value) -> Result<u32> {
        let coll = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.update(&filter, update)
            .map(|c| c as u32)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Updates first document matching a filter
    #[napi(ts_args_type = "filter: string, update: any")]
    pub fn update_one(&mut self, filter: String, update: serde_json::Value) -> Result<bool> {
        let coll = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.update_one(&filter, update)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Deletes all documents matching a filter
    #[napi]
    pub fn delete(&mut self, filter: String) -> Result<u32> {
        let coll = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.delete(&filter)
            .map(|c| c as u32)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Deletes first document matching a filter
    #[napi]
    pub fn delete_one(&mut self, filter: String) -> Result<bool> {
        let coll = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.delete_one(&filter)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Upsert Operations

    /// Upserts a document by ID
    #[napi(ts_args_type = "id: string, doc: any")]
    pub fn upsert_by_id(&mut self, id: String, doc: serde_json::Value) -> Result<JsUpsertResult> {
        let coll = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.upsert_by_id(&id, doc)
            .map(|result| match result {
                jasonisnthappy::UpsertResult::Inserted(id) => JsUpsertResult { id, inserted: true },
                jasonisnthappy::UpsertResult::Updated(id) => JsUpsertResult { id, inserted: false },
            })
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Upserts documents matching a filter
    #[napi(ts_args_type = "filter: string, doc: any")]
    pub fn upsert(&mut self, filter: String, doc: serde_json::Value) -> Result<JsUpsertResult> {
        let coll = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.upsert(&filter, doc)
            .map(|result| match result {
                jasonisnthappy::UpsertResult::Inserted(id) => JsUpsertResult { id, inserted: true },
                jasonisnthappy::UpsertResult::Updated(id) => JsUpsertResult { id, inserted: false },
            })
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Bulk Operations

    /// Inserts multiple documents
    #[napi(ts_args_type = "docs: any[]", ts_return_type = "string[]")]
    pub fn insert_many(&mut self, docs: Vec<serde_json::Value>) -> Result<Vec<String>> {
        let coll = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.insert_many(docs)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Advanced Operations

    /// Gets distinct values for a field
    #[napi(ts_return_type = "any[]")]
    pub fn distinct(&self, field: String) -> Result<Vec<serde_json::Value>> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.distinct(&field)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Counts distinct values for a field
    #[napi]
    pub fn count_distinct(&self, field: String) -> Result<u32> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.count_distinct(&field)
            .map(|c| c as u32)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Performs full-text search
    #[napi(ts_return_type = "any[]")]
    pub fn search(&self, query: String) -> Result<Vec<serde_json::Value>> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.search(&query)
            .and_then(|results| {
                results.into_iter()
                    .map(|r| serde_json::to_value(r).map_err(|e| e.into()))
                    .collect()
            })
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Counts documents matching a filter
    #[napi]
    pub fn count_with_query(&self, filter: Option<String>) -> Result<u32> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        coll.count_with_query(filter.as_deref())
            .map(|c| c as u32)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Query Builder Helpers

    /// Executes a query with all options
    #[napi(ts_return_type = "any[]")]
    pub fn query_with_options(
        &self,
        filter: Option<String>,
        sort_field: Option<String>,
        sort_asc: Option<bool>,
        limit: Option<u32>,
        skip: Option<u32>,
        project_fields: Option<Vec<String>>,
        exclude_fields: Option<Vec<String>>,
    ) -> Result<Vec<serde_json::Value>> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        let mut query = coll.query();

        if let Some(f) = filter {
            query = query.filter(&f);
        }
        if let Some(sf) = sort_field {
            let order = if sort_asc.unwrap_or(true) {
                SortOrder::Asc
            } else {
                SortOrder::Desc
            };
            query = query.sort_by(&sf, order);
        }
        if let Some(l) = limit {
            query = query.limit(l as usize);
        }
        if let Some(s) = skip {
            query = query.skip(s as usize);
        }
        if let Some(pf) = project_fields {
            let pf_refs: Vec<&str> = pf.iter().map(|s| s.as_str()).collect();
            query = query.project(&pf_refs);
        }
        if let Some(ef) = exclude_fields {
            let ef_refs: Vec<&str> = ef.iter().map(|s| s.as_str()).collect();
            query = query.exclude(&ef_refs);
        }

        query.execute()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Counts documents with query options
    #[napi]
    pub fn query_count(&self, filter: Option<String>, skip: Option<u32>, limit: Option<u32>) -> Result<u32> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        let mut query = coll.query();

        if let Some(f) = filter {
            query = query.filter(&f);
        }
        if let Some(s) = skip {
            query = query.skip(s as usize);
        }
        if let Some(l) = limit {
            query = query.limit(l as usize);
        }

        query.count()
            .map(|c| c as u32)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Gets the first document matching a query
    #[napi(ts_return_type = "any | null")]
    pub fn query_first(
        &self,
        filter: Option<String>,
        sort_field: Option<String>,
        sort_asc: Option<bool>,
    ) -> Result<Option<serde_json::Value>> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        let mut query = coll.query();

        if let Some(f) = filter {
            query = query.filter(&f);
        }
        if let Some(sf) = sort_field {
            let order = if sort_asc.unwrap_or(true) {
                SortOrder::Asc
            } else {
                SortOrder::Desc
            };
            query = query.sort_by(&sf, order);
        }

        query.first()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Bulk Write

    /// Executes multiple operations in a transaction
    #[napi(ts_args_type = "operations: any[], ordered?: boolean", ts_return_type = "any")]
    pub fn bulk_write(&mut self, operations: Vec<serde_json::Value>, ordered: Option<bool>) -> Result<serde_json::Value> {
        let coll = self.inner.as_mut()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        let mut bulk = coll.bulk_write();

        if ordered.unwrap_or(true) {
            bulk = bulk.ordered(true);
        }

        // Parse operations
        for op in operations {
            let op_obj = op.as_object()
                .ok_or_else(|| Error::from_reason("Invalid operation format"))?;

            let op_type = op_obj.get("op")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::from_reason("Missing 'op' field"))?;

            match op_type {
                "insert" => {
                    let doc = op_obj.get("doc")
                        .ok_or_else(|| Error::from_reason("Missing 'doc' field for insert"))?
                        .clone();
                    bulk = bulk.insert(doc);
                }
                "update_one" => {
                    let filter = op_obj.get("filter")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| Error::from_reason("Missing 'filter' field"))?;
                    let update = op_obj.get("update")
                        .ok_or_else(|| Error::from_reason("Missing 'update' field"))?
                        .clone();
                    bulk = bulk.update_one(filter, update);
                }
                "delete" | "delete_many" => {
                    let filter = op_obj.get("filter")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| Error::from_reason("Missing 'filter' field"))?;
                    bulk = bulk.delete_many(filter);
                }
                _ => return Err(Error::from_reason(format!("Unknown operation type: {}", op_type))),
            }
        }

        let result = bulk.execute()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        serde_json::to_value(result)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Aggregation

    /// Executes an aggregation pipeline
    #[napi(ts_args_type = "pipeline: any[]", ts_return_type = "any[]")]
    pub fn aggregate(&self, pipeline: Vec<serde_json::Value>) -> Result<Vec<serde_json::Value>> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        let mut agg = coll.aggregate();

        // Parse pipeline stages
        for stage in pipeline {
            let stage_obj = stage.as_object()
                .ok_or_else(|| Error::from_reason("Invalid pipeline stage format"))?;

            if let Some(match_filter) = stage_obj.get("match").and_then(|v| v.as_str()) {
                agg = agg.match_(match_filter);
            }
            if let Some(group_by) = stage_obj.get("group_by").and_then(|v| v.as_str()) {
                agg = agg.group_by(group_by);
            }
            if let Some(count_field) = stage_obj.get("count").and_then(|v| v.as_str()) {
                agg = agg.count(count_field);
            }
            if let Some(sum_obj) = stage_obj.get("sum").and_then(|v| v.as_object()) {
                let field = sum_obj.get("field").and_then(|v| v.as_str())
                    .ok_or_else(|| Error::from_reason("Missing 'field' in sum"))?;
                let output = sum_obj.get("output").and_then(|v| v.as_str())
                    .ok_or_else(|| Error::from_reason("Missing 'output' in sum"))?;
                agg = agg.sum(field, output);
            }
            if let Some(avg_obj) = stage_obj.get("avg").and_then(|v| v.as_object()) {
                let field = avg_obj.get("field").and_then(|v| v.as_str())
                    .ok_or_else(|| Error::from_reason("Missing 'field' in avg"))?;
                let output = avg_obj.get("output").and_then(|v| v.as_str())
                    .ok_or_else(|| Error::from_reason("Missing 'output' in avg"))?;
                agg = agg.avg(field, output);
            }
            if let Some(min_obj) = stage_obj.get("min").and_then(|v| v.as_object()) {
                let field = min_obj.get("field").and_then(|v| v.as_str())
                    .ok_or_else(|| Error::from_reason("Missing 'field' in min"))?;
                let output = min_obj.get("output").and_then(|v| v.as_str())
                    .ok_or_else(|| Error::from_reason("Missing 'output' in min"))?;
                agg = agg.min(field, output);
            }
            if let Some(max_obj) = stage_obj.get("max").and_then(|v| v.as_object()) {
                let field = max_obj.get("field").and_then(|v| v.as_str())
                    .ok_or_else(|| Error::from_reason("Missing 'field' in max"))?;
                let output = max_obj.get("output").and_then(|v| v.as_str())
                    .ok_or_else(|| Error::from_reason("Missing 'output' in max"))?;
                agg = agg.max(field, output);
            }
            if let Some(sort_obj) = stage_obj.get("sort").and_then(|v| v.as_object()) {
                let field = sort_obj.get("field").and_then(|v| v.as_str())
                    .ok_or_else(|| Error::from_reason("Missing 'field' in sort"))?;
                let asc = sort_obj.get("asc").and_then(|v| v.as_bool()).unwrap_or(true);
                agg = agg.sort(field, asc);
            }
            if let Some(limit_val) = stage_obj.get("limit").and_then(|v| v.as_u64()) {
                agg = agg.limit(limit_val as usize);
            }
            if let Some(skip_val) = stage_obj.get("skip").and_then(|v| v.as_u64()) {
                agg = agg.skip(skip_val as usize);
            }
            if let Some(project_arr) = stage_obj.get("project").and_then(|v| v.as_array()) {
                let fields: Vec<String> = project_arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                agg = agg.project(&field_refs);
            }
            if let Some(exclude_arr) = stage_obj.get("exclude").and_then(|v| v.as_array()) {
                let fields: Vec<String> = exclude_arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                agg = agg.exclude(&field_refs);
            }
        }

        agg.execute()
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // Watch / Change Streams

    /// Starts watching for changes on the collection
    /// The callback receives (operation: string, docId: string, document: any | null)
    #[napi(ts_args_type = "filter: string | undefined, callback: (operation: string, docId: string, document: any) => void")]
    pub fn watch(
        &self,
        filter: Option<String>,
        callback: Function<(String, String, serde_json::Value), ()>,
    ) -> Result<WatchHandle> {
        let coll = self.inner.as_ref()
            .ok_or_else(|| Error::from_reason("Collection is closed"))?;

        // Create the watch builder
        let mut builder = coll.watch();
        if let Some(f) = filter {
            builder = builder.filter(&f);
        }

        // Subscribe to get the channel and handle
        let (rust_handle, receiver) = builder.subscribe()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        // Create threadsafe function for the callback
        let tsfn = callback.build_threadsafe_function()
            .build()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        // Create stop flag
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_clone = stop_flag.clone();

        // Spawn a thread to read from the channel and call the JS callback
        let thread_handle = thread::spawn(move || {
            while !stop_flag_clone.load(Ordering::Relaxed) {
                // Try to receive with a timeout to allow checking stop flag
                match receiver.recv_timeout(std::time::Duration::from_millis(100)) {
                    Ok(event) => {
                        let op_str = match event.operation {
                            ChangeOperation::Insert => "insert".to_string(),
                            ChangeOperation::Update => "update".to_string(),
                            ChangeOperation::Delete => "delete".to_string(),
                        };
                        let doc = event.document.unwrap_or(serde_json::Value::Null);
                        tsfn.call((op_str, event.doc_id, doc), ThreadsafeFunctionCallMode::NonBlocking);
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        // Continue checking stop flag
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        // Channel closed, stop the thread
                        break;
                    }
                }
            }
        });

        Ok(WatchHandle {
            _rust_handle: Some(rust_handle),
            stop_flag,
            thread_handle: Some(thread_handle),
        })
    }
}

// ==================
// Watch Handle
// ==================

#[napi]
pub struct WatchHandle {
    _rust_handle: Option<jasonisnthappy::core::watch::WatchHandle>,
    stop_flag: Arc<AtomicBool>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

#[napi]
impl WatchHandle {
    /// Stops watching and cleans up resources
    #[napi]
    pub fn stop(&mut self) {
        // Signal the thread to stop
        self.stop_flag.store(true, Ordering::Relaxed);

        // Wait for the thread to finish
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }

        // Drop the rust handle to unsubscribe
        self._rust_handle.take();
    }
}
