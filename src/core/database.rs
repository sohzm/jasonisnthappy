
use crate::core::constants::*;
use crate::core::errors::*;
use crate::core::metadata::Metadata;
use crate::core::metrics::{Metrics, MetricsSnapshot};
use crate::core::mvcc::TransactionManager;
use crate::core::pager::Pager;
use crate::core::transaction::Transaction;
use crate::core::wal::WAL;
use crate::core::btree::BTree;
use crate::core::watch::{WatcherStorage, new_watcher_storage};
use crate::core::buffer_pool::BufferPool;
use fs2::FileExt;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::sync::atomic::AtomicU64;
use crate::core::errors::PoisonedLockExt;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

#[derive(Debug, Clone)]
pub struct DatabaseOptions {
    pub cache_size: usize,
    pub auto_checkpoint_threshold: u64,
    pub file_permissions: u32,
    pub read_only: bool,
    /// Maximum number of documents in bulk operations (insert_many, bulk_write)
    /// Default: 100,000
    pub max_bulk_operations: usize,
    /// Maximum size of a single document in bytes
    /// Default: 64MB (67,108,864 bytes)
    pub max_document_size: usize,
    /// Maximum HTTP request body size for web server in bytes
    /// Default: 50MB (52,428,800 bytes)
    pub max_request_body_size: usize,
}

#[derive(Debug, Clone)]
pub struct TransactionConfig {
    pub max_retries: usize,
    pub retry_backoff_base_ms: u64,
    pub max_retry_backoff_ms: u64,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_backoff_base_ms: 1,
            max_retry_backoff_ms: 100,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GarbageCollectionStats {
    pub versions_removed: usize,
    pub pages_freed: usize,
    pub bytes_freed: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BackupInfo {
    pub version: u32,
    pub num_pages: u64,
    pub num_collections: usize,
    pub file_size: u64,
}

/// Information about a single collection
#[derive(Debug, Clone, serde::Serialize)]
pub struct CollectionInfo {
    pub name: String,
    pub document_count: usize,
    pub btree_root: u64,
    pub indexes: Vec<IndexInfo>,
}

/// Information about an index
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexInfo {
    pub name: String,
    pub fields: Vec<String>,
    pub unique: bool,
    pub btree_root: u64,
}

/// Overall database information
#[derive(Debug, Clone, serde::Serialize)]
pub struct DatabaseInfo {
    pub path: String,
    pub version: u32,
    pub num_pages: u64,
    pub file_size: u64,
    pub collections: Vec<CollectionInfo>,
    pub total_documents: usize,
    pub read_only: bool,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            cache_size: 25_000,  // 25K pages = ~100MB cache (sized for large bulk operations)
            auto_checkpoint_threshold: 1000,
            file_permissions: 0o644,
            read_only: false,
            max_bulk_operations: 100_000,           // 100K documents
            max_document_size: 67_108_864,          // 64MB
            max_request_body_size: 52_428_800,      // 50MB
        }
    }
}

/// Configuration for batch commit optimization
#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub enabled: bool,
    pub max_batch_size: usize,
    pub collect_timeout_micros: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            enabled: true, // Re-enabled with logging
            max_batch_size: 32,
            collect_timeout_micros: 100, // 0.1ms
        }
    }
}

/// Represents a transaction waiting to be committed in a batch
#[derive(Clone)]
pub(crate) struct PendingWrite {
    pub writes: HashMap<PageNum, Vec<u8>>,
    pub doc_writes: HashMap<String, HashMap<String, PageNum>>,
    pub snapshot_roots: HashMap<String, PageNum>,
    pub updated_roots: HashMap<String, PageNum>,
    pub old_versions: HashMap<String, HashMap<String, crate::core::mvcc::DocumentVersion>>,
    pub modified_collections: HashSet<String>,
    pub doc_existed_in_snapshot: HashMap<String, HashMap<String, bool>>,
    pub doc_original_xmin: HashMap<String, HashMap<String, TransactionID>>,

    pub _tx_id: u64,
    pub snapshot_id: TransactionID,
    pub mvcc_tx_id: TransactionID,

    pub completion: Arc<(Mutex<Option<Result<()>>>, std::sync::Condvar)>,
    pub _submitted_at: std::time::Instant,
}

pub struct Database {
    pager: Arc<Pager>,
    wal: Arc<WAL>,
    metadata: Arc<RwLock<Metadata>>,
    tx_manager: Arc<TransactionManager>,
    lock_file: Arc<Mutex<File>>,
    path: String,
    read_only: bool,
    commit_mu: Arc<Mutex<()>>,
    pub(crate) version_chains: Arc<RwLock<HashMap<String, HashMap<String, crate::core::mvcc::VersionChain>>>>,
    tx_config: Arc<RwLock<TransactionConfig>>,
    auto_checkpoint_threshold: Arc<RwLock<u64>>,
    checkpoint_in_progress: Arc<(Mutex<bool>, Condvar)>,
    metrics: Arc<Metrics>,
    watchers: WatcherStorage,
    // Per-database buffer pools and transaction ID counter
    node_serialize_pool: Arc<BufferPool>,
    page_buffer_pool: Arc<BufferPool>,
    tx_id_counter: Arc<AtomicU64>,
    // Batch commit support
    pub(crate) pending_writes: Arc<Mutex<std::collections::VecDeque<PendingWrite>>>,
    pub(crate) batch_config: BatchConfig,
    // Operation size limits
    max_bulk_operations: usize,
    max_document_size: usize,
    max_request_body_size: usize,
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            pager: self.pager.clone(),
            wal: self.wal.clone(),
            metadata: self.metadata.clone(),
            tx_manager: self.tx_manager.clone(),
            lock_file: self.lock_file.clone(),
            path: self.path.clone(),
            read_only: self.read_only,
            commit_mu: self.commit_mu.clone(),
            version_chains: self.version_chains.clone(),
            tx_config: self.tx_config.clone(),
            auto_checkpoint_threshold: self.auto_checkpoint_threshold.clone(),
            checkpoint_in_progress: self.checkpoint_in_progress.clone(),
            metrics: self.metrics.clone(),
            watchers: self.watchers.clone(),
            node_serialize_pool: self.node_serialize_pool.clone(),
            page_buffer_pool: self.page_buffer_pool.clone(),
            tx_id_counter: self.tx_id_counter.clone(),
            pending_writes: self.pending_writes.clone(),
            batch_config: self.batch_config.clone(),
            max_bulk_operations: self.max_bulk_operations,
            max_document_size: self.max_document_size,
            max_request_body_size: self.max_request_body_size,
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Only cleanup resources if this is the last Database instance
        // Arc::strong_count returns the number of strong references
        // If it's 2, that means: 1 for self.lock_file + 1 for the temporary Arc we'd create to check
        // If it's 1, we're the only holder and should cleanup
        if Arc::strong_count(&self.lock_file) == 1 {
            let (lock, cvar) = &*self.checkpoint_in_progress;
            let timeout = std::time::Duration::from_secs(30);

            if let Ok(guard) = lock.lock() {
                let _ = cvar.wait_timeout_while(guard, timeout, |in_progress| *in_progress);
            }

            // Ignore all errors since Drop can't return Result
            // Try to flush and close gracefully
            let _ = self.pager.flush();
            let _ = self.wal.close();

            // Unlock the database file so other processes can open it
            if let Ok(lock_file) = self.lock_file.lock() {
                let _ = FileExt::unlock(&*lock_file);
            }
        }
    }
}

impl Database {
    pub fn open(path: &str) -> Result<Self> {
        Self::open_with_options(path, DatabaseOptions::default())
    }

    pub fn open_with_options(path: &str, opts: DatabaseOptions) -> Result<Self> {
        if path.is_empty() {
            return Err(Error::Other("database path cannot be empty".to_string()));
        }

        if path.contains("..") {
            return Err(Error::Other("database path cannot contain '..'".to_string()));
        }

        let lock_path = format!("{}.lock", path);

        #[cfg(unix)]
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(opts.file_permissions)
            .open(&lock_path)?;

        #[cfg(not(unix))]
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&lock_path)?;

        if opts.read_only {
            fs2::FileExt::try_lock_shared(&lock_file)?;
        } else {
            fs2::FileExt::try_lock_exclusive(&lock_file)?;
        }

        let pager = Arc::new(Pager::open(
            path,
            opts.cache_size,
            opts.file_permissions,
            opts.read_only,
        )?);

        let wal = Arc::new(WAL::open(path, opts.file_permissions)?);

        let frame_count = wal.frame_count();
        if !opts.read_only && frame_count > 0 {
            let frames = wal.read_all_frames()?;

            let mut latest_meta_page: Option<u64> = None;
            let mut latest_num_pages: Option<u64> = None;
            for frame in &frames {
                if frame.page_num == 0 && frame.page_data.len() >= 32 {
                    let meta_page_bytes: [u8; 8] = frame.page_data[24..32].try_into()
                        .map_err(|_| Error::DataCorruption {
                            details: "invalid metadata page in WAL frame".to_string()
                        })?;
                    let meta_page = u64::from_le_bytes(meta_page_bytes);
                    if meta_page > 0 {
                        latest_meta_page = Some(meta_page);
                    }

                    let num_pages_bytes: [u8; 8] = frame.page_data[12..20].try_into()
                        .map_err(|_| Error::DataCorruption {
                            details: "invalid num_pages in WAL frame".to_string()
                        })?;
                    let num_pages = u64::from_le_bytes(num_pages_bytes);
                    latest_num_pages = Some(num_pages);
                }
            }

            let mut max_page = 0u64;
            for frame in &frames {
                if frame.page_num > max_page {
                    max_page = frame.page_num;
                }
            }

            if let Some(num_pages) = latest_num_pages {
                pager.set_num_pages(num_pages)?;
            } else if max_page >= pager.num_pages()? {
                pager.set_num_pages(max_page + 1)?;
            }

            wal.checkpoint(&pager)?;

            if let Some(meta_page) = latest_meta_page {
                pager.set_metadata_page(meta_page)?;
                pager.write_header()?;
            }
        }

        let tx_manager = Arc::new(TransactionManager::new());

        let current_tx_id = pager.get_current_transaction_id()?;
        tx_manager.initialize_from_pager(current_tx_id);

        let metadata_page_num = pager.metadata_page()?;
        let metadata = if metadata_page_num > 0 {
            let meta_data = pager.read_page(metadata_page_num)?;
            let meta = Metadata::deserialize(&meta_data)?;
            meta
        } else {
            let meta = Metadata::new();

            let meta_page = pager.alloc_page()?;
            let mut meta_data = meta.serialize()?;

            if meta_data.len() < PAGE_SIZE {
                meta_data.resize(PAGE_SIZE, 0);
            }

            pager.write_page_transfer(meta_page, meta_data)?;
            pager.set_metadata_page(meta_page)?;
            pager.write_header()?;
            pager.flush()?;

            meta
        };

        let metrics = Arc::new(Metrics::new());

        // Set metrics on pager and WAL for instrumentation
        pager.set_metrics(metrics.clone());
        wal.set_metrics(metrics.clone());

        Ok(Database {
            pager,
            wal,
            metadata: Arc::new(RwLock::new(metadata)),
            tx_manager,
            lock_file: Arc::new(Mutex::new(lock_file)),
            path: path.to_string(),
            read_only: opts.read_only,
            commit_mu: Arc::new(Mutex::new(())),
            version_chains: Arc::new(RwLock::new(HashMap::new())),
            tx_config: Arc::new(RwLock::new(TransactionConfig::default())),
            watchers: new_watcher_storage(),
            auto_checkpoint_threshold: Arc::new(RwLock::new(opts.auto_checkpoint_threshold)),
            checkpoint_in_progress: Arc::new((Mutex::new(false), Condvar::new())),
            metrics,
            // Initialize per-database buffer pools and TX ID counter
            node_serialize_pool: Arc::new(BufferPool::new(128)),
            page_buffer_pool: Arc::new(BufferPool::new(256)),
            tx_id_counter: Arc::new(AtomicU64::new(1)),
            // Initialize batch commit support
            pending_writes: Arc::new(Mutex::new(std::collections::VecDeque::new())),
            batch_config: BatchConfig::default(),
            // Initialize operation size limits
            max_bulk_operations: opts.max_bulk_operations,
            max_document_size: opts.max_document_size,
            max_request_body_size: opts.max_request_body_size,
        })
    }

    pub fn begin(&self) -> Result<Transaction> {
        let metadata = self.metadata.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
        let mut collection_roots = HashMap::new();

        for (name, coll_meta) in metadata.collections.iter() {
            collection_roots.insert(name.clone(), coll_meta.btree_root);
        }
        drop(metadata);

        let mut tx = Transaction::new(
            self.pager.clone(),
            self.wal.clone(),
            self.tx_manager.clone(),
            collection_roots,
            self.commit_mu.clone(),
            Some(self.tx_id_counter.clone()),
        )?;

        // Track transaction metrics
        self.metrics.transaction_begun();

        let db_ref = Arc::new(Self {
            pager: self.pager.clone(),
            wal: self.wal.clone(),
            metadata: self.metadata.clone(),
            tx_manager: self.tx_manager.clone(),
            lock_file: self.lock_file.clone(),
            path: self.path.clone(),
            read_only: self.read_only,
            commit_mu: self.commit_mu.clone(),
            version_chains: self.version_chains.clone(),
            tx_config: self.tx_config.clone(),
            auto_checkpoint_threshold: self.auto_checkpoint_threshold.clone(),
            checkpoint_in_progress: self.checkpoint_in_progress.clone(),
            metrics: self.metrics.clone(),
            watchers: self.watchers.clone(),
            node_serialize_pool: self.node_serialize_pool.clone(),
            page_buffer_pool: self.page_buffer_pool.clone(),
            tx_id_counter: self.tx_id_counter.clone(),
            pending_writes: self.pending_writes.clone(),
            batch_config: self.batch_config.clone(),
            max_bulk_operations: self.max_bulk_operations,
            max_document_size: self.max_document_size,
            max_request_body_size: self.max_request_body_size,
        });
        tx.set_database(db_ref);

        Ok(tx)
    }

    pub fn get_metadata(&self) -> Metadata {
        let metadata = self.metadata.read()
            .recover_poison();
        metadata.clone()
    }

    pub(crate) fn get_watchers(&self) -> WatcherStorage {
        self.watchers.clone()
    }

    pub(crate) fn get_pager(&self) -> Arc<Pager> {
        self.pager.clone()
    }

    pub fn update_metadata<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&mut Metadata),
    {
        if self.read_only {
            return Err(Error::Other("database is read-only".to_string()));
        }

        let mut metadata = self.metadata.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
        f(&mut metadata);

        let mut meta_data = metadata.serialize()?;

        if meta_data.len() < PAGE_SIZE {
            meta_data.resize(PAGE_SIZE, 0);
        }

        let meta_page = self.pager.metadata_page()?;

        if meta_page > 0 {
            self.pager.write_page_transfer(meta_page, meta_data)?;
        } else {
            let new_meta_page = self.pager.alloc_page()?;
            self.pager.write_page_transfer(new_meta_page, meta_data)?;
            self.pager.set_metadata_page(new_meta_page)?;
            self.pager.write_header()?;
        }

        self.pager.flush()?;
        Ok(())
    }

    pub(crate) fn update_metadata_no_flush<F>(&self, f: F)
    where
        F: FnOnce(&mut Metadata),
    {
        let mut metadata = self.metadata.write()
            .recover_poison();
        f(&mut metadata);
    }

    fn save_metadata(&self) -> Result<()> {
        self.update_metadata(|_| {})
    }

    pub fn checkpoint(&self) -> Result<()> {
        if self.read_only {
            return Err(Error::Other("cannot checkpoint in read-only mode".to_string()));
        }

        self.wal.checkpoint(&self.pager)
    }

    pub fn garbage_collect(&self) -> Result<GarbageCollectionStats> {
        let mut stats = GarbageCollectionStats {
            versions_removed: 0,
            pages_freed: 0,
            bytes_freed: 0,
        };

        if self.read_only {
            return Err(Error::Other("cannot garbage collect: database is in read-only mode".to_string()));
        }

        let oldest_active_tx = self.tx_manager.get_oldest_active_transaction()?;

        let mut version_chains = self.version_chains.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.version_chains".to_string() })?;

        let mut empty_collections: Vec<String> = Vec::new();

        for (coll_name, doc_chains) in version_chains.iter_mut() {
            for (_doc_id, version_chain) in doc_chains.iter_mut() {
                let removed_versions = version_chain.garbage_collect(oldest_active_tx)?;

                for version in removed_versions {
                    if let Err(_e) = crate::core::document::delete_document(&self.pager, version.page_num) {
                        continue;
                    }

                    stats.versions_removed += 1;
                    stats.pages_freed += 1;
                }
            }

            doc_chains.retain(|_doc_id, chain| {
                chain.get_versions().map(|v| v.len() > 0).unwrap_or(false)
            });

            if doc_chains.is_empty() {
                empty_collections.push(coll_name.clone());
            }
        }

        for coll_name in empty_collections {
            version_chains.remove(&coll_name);
        }

        stats.bytes_freed = (stats.pages_freed as i64) * (PAGE_SIZE as i64);

        Ok(stats)
    }

    pub fn close(&self) -> Result<()> {
        let (lock, cvar) = &*self.checkpoint_in_progress;
        let timeout = std::time::Duration::from_secs(30);

        let guard = lock.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.checkpoint_in_progress".to_string() })?;

        let _ = cvar.wait_timeout_while(guard, timeout, |in_progress| *in_progress)
            .map_err(|_| Error::LockPoisoned { lock_name: "database.checkpoint_in_progress".to_string() })?;

        self.pager.flush()?;

        self.wal.close()?;

        let lock_file = self.lock_file.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.lock_file".to_string() })?;
        FileExt::unlock(&*lock_file)?;

        Ok(())
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub fn max_bulk_operations(&self) -> usize {
        self.max_bulk_operations
    }

    pub fn max_document_size(&self) -> usize {
        self.max_document_size
    }

    pub fn max_request_body_size(&self) -> usize {
        self.max_request_body_size
    }

    pub fn collection(&self, name: &str) -> crate::core::collection::Collection {
        crate::core::collection::Collection::new(
            std::sync::Arc::new(Self {
                pager: self.pager.clone(),
                wal: self.wal.clone(),
                metadata: self.metadata.clone(),
                tx_manager: self.tx_manager.clone(),
                lock_file: self.lock_file.clone(),
                path: self.path.clone(),
                read_only: self.read_only,
                commit_mu: self.commit_mu.clone(),
                version_chains: self.version_chains.clone(),
                tx_config: self.tx_config.clone(),
                auto_checkpoint_threshold: self.auto_checkpoint_threshold.clone(),
                checkpoint_in_progress: self.checkpoint_in_progress.clone(),
                metrics: self.metrics.clone(),
                watchers: self.watchers.clone(),
                node_serialize_pool: self.node_serialize_pool.clone(),
                page_buffer_pool: self.page_buffer_pool.clone(),
                tx_id_counter: self.tx_id_counter.clone(),
                pending_writes: self.pending_writes.clone(),
                batch_config: self.batch_config.clone(),
                max_bulk_operations: self.max_bulk_operations,
                max_document_size: self.max_document_size,
                max_request_body_size: self.max_request_body_size,
            }),
            name.to_string(),
        )
    }

    /// List all collections in the database
    pub fn list_collections(&self) -> Result<Vec<String>> {
        let metadata = self.metadata.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
        let mut names: Vec<String> = metadata.collections.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    /// Get detailed statistics for a specific collection
    pub fn collection_stats(&self, name: &str) -> Result<CollectionInfo> {
        let metadata = self.metadata.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
        let coll_meta = metadata.collections.get(name)
            .ok_or_else(|| Error::Other(format!("collection {} not found", name)))?;

        // Count documents by iterating the btree
        let document_count = if coll_meta.btree_root == 0 {
            0
        } else {
            self.count_documents_in_btree(coll_meta.btree_root)?
        };

        // Collect index information
        let mut indexes = Vec::new();
        for (idx_name, idx_meta) in &coll_meta.indexes {
            indexes.push(IndexInfo {
                name: idx_name.clone(),
                fields: idx_meta.fields.clone(),
                unique: idx_meta.unique,
                btree_root: idx_meta.btree_root,
            });
        }

        Ok(CollectionInfo {
            name: name.to_string(),
            document_count,
            btree_root: coll_meta.btree_root,
            indexes,
        })
    }

    /// List all indexes for a specific collection
    pub fn list_indexes(&self, collection_name: &str) -> Result<Vec<IndexInfo>> {
        let metadata = self.metadata.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
        let coll_meta = metadata.collections.get(collection_name)
            .ok_or_else(|| Error::Other(format!("collection {} not found", collection_name)))?;

        let mut indexes = Vec::new();
        for (idx_name, idx_meta) in &coll_meta.indexes {
            indexes.push(IndexInfo {
                name: idx_name.clone(),
                fields: idx_meta.fields.clone(),
                unique: idx_meta.unique,
                btree_root: idx_meta.btree_root,
            });
        }

        Ok(indexes)
    }

    /// Get comprehensive database information
    pub fn info(&self) -> Result<DatabaseInfo> {
        let metadata = self.metadata.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;

        // Get file size
        let file_size = std::fs::metadata(&self.path)
            .map(|m| m.len())
            .unwrap_or(0);

        // Collect collection information
        let mut collections = Vec::new();
        let mut total_documents = 0;

        for coll_name in metadata.collections.keys() {
            match self.collection_stats(coll_name) {
                Ok(coll_info) => {
                    total_documents += coll_info.document_count;
                    collections.push(coll_info);
                }
                Err(_) => continue,
            }
        }

        collections.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(DatabaseInfo {
            path: self.path.clone(),
            version: VERSION,
            num_pages: self.pager.num_pages()?,
            file_size,
            collections,
            total_documents,
            read_only: self.read_only,
        })
    }

    /// Helper: Count documents in a btree
    fn count_documents_in_btree(&self, root_page: u64) -> Result<usize> {
        use crate::core::btree::BTree;
        use crate::core::document::read_versioned_document;

        let tx = self.begin()?;
        let pager = tx.get_pager();
        let btree = BTree::open(pager.clone(), root_page);

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
                Err(_) => continue,
            }
        }

        Ok(count)
    }

    pub fn run_transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: Fn(&mut Transaction) -> Result<R>,
    {
        let config = self.tx_config.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.tx_config".to_string() })?
            .clone();
        let mut last_err = None;

        for attempt in 0..=(config.max_retries) {
            let mut tx = self.begin()?;

            match f(&mut tx) {
                Ok(result) => {
                    match tx.commit() {
                        Ok(_) => return Ok(result),
                        Err(e) => {
                            if matches!(e, Error::TxConflict) {
                                self.metrics.transaction_conflict();
                                last_err = Some(e);
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.rollback();
                    return Err(e);
                }
            }

            if attempt < config.max_retries {
                let backoff_ms = config.retry_backoff_base_ms * (1 << attempt);
                let backoff_ms = backoff_ms.min(config.max_retry_backoff_ms);
                if backoff_ms > 0 {
                    std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                }
            }
        }

        Err(last_err.unwrap_or(Error::TxConflict))
    }

    pub fn set_transaction_config(&self, config: TransactionConfig) {
        *self.tx_config.write()
            .recover_poison() = config;
    }

    pub fn get_transaction_config(&self) -> TransactionConfig {
        self.tx_config.read()
            .recover_poison()
            .clone()
    }

    pub fn set_auto_checkpoint_threshold(&self, threshold: u64) {
        *self.auto_checkpoint_threshold.write()
            .recover_poison() = threshold;
    }

    pub fn frame_count(&self) -> u64 {
        self.wal.frame_count()
    }

    pub(crate) fn maybe_auto_checkpoint(&self) {
        let threshold = *self.auto_checkpoint_threshold.read()
            .recover_poison();
        if threshold == 0 {
            return;
        }

        let frame_count = self.wal.frame_count();

        if frame_count < threshold {
            return;
        }

        // Try to acquire the checkpoint lock without blocking
        let (lock, _cvar) = &*self.checkpoint_in_progress;
        let mut in_progress = match lock.try_lock() {
            Ok(guard) => guard,
            Err(_) => return, // Another checkpoint is in progress
        };

        if *in_progress {
            return;
        }

        *in_progress = true;
        drop(in_progress); // Release lock before spawning thread

        // Run checkpoint in BACKGROUND thread to avoid blocking commits
        let wal = self.wal.clone();
        let pager = self.pager.clone();
        let in_progress_flag = self.checkpoint_in_progress.clone();

        std::thread::spawn(move || {
            let _result = wal.checkpoint(&pager);

            // Mark checkpoint as complete and notify waiters
            let (lock, cvar) = &*in_progress_flag;
            if let Ok(mut flag) = lock.lock() {
                *flag = false;
                cvar.notify_all(); // Wake up any threads waiting in close()/drop()
            }
        });
    }

    /// Create a single-field index on a collection.
    pub fn create_index(&self, collection_name: &str, index_name: &str, field: &str, unique: bool) -> Result<()> {
        self.create_compound_index(collection_name, index_name, &[field], unique)
    }

    /// Create a compound index on multiple fields.
    ///
    /// # Arguments
    /// * `collection_name` - Name of the collection to index
    /// * `index_name` - Name for the index
    /// * `fields` - Ordered list of fields to include in the compound index
    /// * `unique` - If true, enforce unique constraint on the combination of field values
    ///
    /// # Examples
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// // Create compound index on city and age
    /// db.create_compound_index("users", "city_age_idx", &["city", "age"], false).unwrap();
    ///
    /// // Query will benefit from leftmost prefix rule:
    /// // - Queries on "city" alone can use this index
    /// // - Queries on both "city" and "age" can use this index
    /// // - Queries on "age" alone cannot use this index
    /// ```
    pub fn create_compound_index(&self, collection_name: &str, index_name: &str, fields: &[&str], unique: bool) -> Result<()> {
        use crate::core::validation::validate_collection_name;
        use crate::core::btree::BTree;

        validate_collection_name(collection_name)?;

        if fields.is_empty() {
            return Err(Error::Other("index must have at least one field".to_string()));
        }

        if self.read_only {
            return Err(Error::Other("cannot create index: database is in read-only mode".to_string()));
        }

        self.wal.checkpoint(&self.pager)?;

        {
            let metadata = self.metadata.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
            if let Some(coll_meta) = metadata.collections.get(collection_name) {
                if coll_meta.indexes.contains_key(index_name) {
                    return Err(Error::Other(format!(
                        "index {} already exists on collection {}",
                        index_name, collection_name
                    )));
                }
            }
        }

        let index_btree = BTree::new(self.pager.clone())?;

        let coll_btree_root = {
            let metadata = self.metadata.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
            metadata.collections
                .get(collection_name)
                .map(|c| c.btree_root)
                .unwrap_or(0)
        };

        let fields_vec: Vec<String> = fields.iter().map(|s| s.to_string()).collect();

        if coll_btree_root != 0 {
            self.build_compound_index_from_btree(&index_btree, coll_btree_root, &fields_vec, unique)?;
        }

        // Get the root page AFTER building the index (it may have changed due to splits)
        let index_root = index_btree.root_page();

        {
            let mut metadata = self.metadata.write()
                .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
            let coll_meta = metadata.get_collection(collection_name);
            coll_meta.indexes.insert(
                index_name.to_string(),
                crate::core::metadata::IndexMeta {
                    name: index_name.to_string(),
                    field: None,  // Deprecated field
                    fields: fields_vec,
                    btree_root: index_root,
                    unique,
                },
            );
        }

        self.save_metadata()?;
        self.pager.flush()?;

        // Write header to persist num_pages (new pages allocated for index)
        self.pager.write_header()?;

        Ok(())
    }

    /// Create a text index for full-text search on specified fields
    ///
    /// # Arguments
    /// * `collection_name` - Name of the collection to index
    /// * `index_name` - Name for the text index
    /// * `fields` - List of text fields to index for search
    ///
    /// # Examples
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// // Create text index on title and body fields
    /// db.create_text_index("posts", "search_idx", &["title", "body"]).unwrap();
    ///
    /// // Now you can search across these fields
    /// let posts = db.collection("posts");
    /// let results = posts.search("rust database").unwrap();
    /// ```
    pub fn create_text_index(&self, collection_name: &str, index_name: &str, fields: &[&str]) -> Result<()> {
        use crate::core::validation::validate_collection_name;
        use crate::core::btree::BTree;
        use crate::core::text_search::{TextIndex, TextIndexMeta};

        validate_collection_name(collection_name)?;

        if fields.is_empty() {
            return Err(Error::Other("text index must have at least one field".to_string()));
        }

        if self.read_only {
            return Err(Error::Other("cannot create text index: database is in read-only mode".to_string()));
        }

        self.wal.checkpoint(&self.pager)?;

        {
            let metadata = self.metadata.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
            if let Some(coll_meta) = metadata.collections.get(collection_name) {
                if coll_meta.text_indexes.contains_key(index_name) {
                    return Err(Error::Other(format!(
                        "text index {} already exists on collection {}",
                        index_name, collection_name
                    )));
                }
            }
        }

        let index_btree = BTree::new(self.pager.clone())?;
        let fields_vec: Vec<String> = fields.iter().map(|s| s.to_string()).collect();
        let mut text_index = TextIndex::new(index_btree, fields_vec.clone());

        // Build index from existing documents
        let coll_btree_root = {
            let metadata = self.metadata.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
            metadata.collections
                .get(collection_name)
                .map(|c| c.btree_root)
                .unwrap_or(0)
        };

        if coll_btree_root != 0 {
            self.build_text_index_from_btree(&mut text_index, coll_btree_root, &fields_vec)?;
        }

        // Get the root page AFTER building the index
        let index_root = text_index.btree().root_page();

        {
            let mut metadata = self.metadata.write()
                .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
            let coll_meta = metadata.get_collection(collection_name);
            coll_meta.text_indexes.insert(
                index_name.to_string(),
                TextIndexMeta {
                    name: index_name.to_string(),
                    fields: fields_vec,
                    btree_root: index_root,
                },
            );
        }

        self.save_metadata()?;
        self.pager.flush()?;

        // Write header to persist num_pages
        self.pager.write_header()?;

        Ok(())
    }

    /// Drop an index from a collection.
    ///
    /// # Arguments
    /// * `collection_name` - Name of the collection
    /// * `index_name` - Name of the index to drop
    ///
    /// # Examples
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// // Drop the email index
    /// db.drop_index("users", "email_idx").unwrap();
    /// ```
    pub fn drop_index(&self, collection_name: &str, index_name: &str) -> Result<()> {
        use crate::core::validation::validate_collection_name;

        validate_collection_name(collection_name)?;

        if self.read_only {
            return Err(Error::Other("cannot drop index: database is in read-only mode".to_string()));
        }

        // Check if collection exists and has the index
        {
            let metadata = self.metadata.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
            let coll_meta = metadata.collections.get(collection_name)
                .ok_or_else(|| Error::Other(format!("collection {} does not exist", collection_name)))?;

            if !coll_meta.indexes.contains_key(index_name) {
                return Err(Error::Other(format!(
                    "index {} does not exist on collection {}",
                    index_name, collection_name
                )));
            }
        }

        // Remove the index from metadata
        {
            let mut metadata = self.metadata.write()
                .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
            if let Some(coll_meta) = metadata.collections.get_mut(collection_name) {
                coll_meta.indexes.remove(index_name);
            }
        }

        // Save metadata and flush
        self.save_metadata()?;
        self.pager.flush()?;
        self.pager.write_header()?;

        Ok(())
    }

    /// Set a validation schema for a collection
    ///
    /// Documents inserted or updated in this collection will be validated against this schema.
    ///
    /// # Example
    /// ```no_run
    /// use jasonisnthappy::{Database, Schema, ValueType};
    /// use std::collections::HashMap;
    ///
    /// let db = Database::open("my.db").unwrap();
    ///
    /// let mut schema = Schema::new();
    /// schema.value_type = Some(ValueType::Object);
    /// schema.required = Some(vec!["name".to_string(), "email".to_string()]);
    ///
    /// let mut properties = HashMap::new();
    /// let mut name_schema = Schema::new();
    /// name_schema.value_type = Some(ValueType::String);
    /// name_schema.min_length = Some(1);
    /// properties.insert("name".to_string(), name_schema);
    ///
    /// schema.properties = Some(properties);
    ///
    /// db.set_schema("users", schema).unwrap();
    /// ```
    pub fn set_schema(&self, collection_name: &str, schema: crate::core::validation::Schema) -> Result<()> {
        use crate::core::validation::validate_collection_name;

        validate_collection_name(collection_name)?;

        if self.read_only {
            return Err(Error::Other("cannot set schema: database is in read-only mode".to_string()));
        }

        {
            let mut metadata = self.metadata.write()
                .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
            let coll_meta = metadata.get_collection(collection_name);
            coll_meta.schema = Some(schema);
        }

        self.save_metadata()?;
        self.pager.flush()?;

        Ok(())
    }

    /// Get the validation schema for a collection
    ///
    /// Returns None if no schema is set for the collection.
    pub fn get_schema(&self, collection_name: &str) -> Option<crate::core::validation::Schema> {
        let metadata = self.metadata.read()
            .recover_poison();
        metadata.collections
            .get(collection_name)
            .and_then(|c| c.schema.clone())
    }

    /// Remove the validation schema from a collection
    ///
    /// After removing the schema, documents will no longer be validated on insert/update.
    pub fn remove_schema(&self, collection_name: &str) -> Result<()> {
        use crate::core::validation::validate_collection_name;

        validate_collection_name(collection_name)?;

        if self.read_only {
            return Err(Error::Other("cannot remove schema: database is in read-only mode".to_string()));
        }

        {
            let mut metadata = self.metadata.write()
                .map_err(|_| Error::LockPoisoned { lock_name: "database.metadata".to_string() })?;
            if let Some(coll_meta) = metadata.collections.get_mut(collection_name) {
                coll_meta.schema = None;
            }
        }

        self.save_metadata()?;
        self.pager.flush()?;

        Ok(())
    }

    fn build_compound_index_from_btree(
        &self,
        index_btree: &BTree,
        root_page: PageNum,
        fields: &[String],
        unique: bool,
    ) -> Result<()> {
        self.scan_btree_node_for_compound_index(index_btree, root_page, fields, unique)
    }

    fn scan_btree_node_for_compound_index(
        &self,
        index_btree: &BTree,
        page_num: PageNum,
        fields: &[String],
        unique: bool,
    ) -> Result<()> {
        use crate::core::btree::{deserialize_node, NodeType};
        use crate::core::document::read_versioned_document;
        use crate::core::index_key::{
            IndexKey, serialize_index_key, extract_field_values,
            CompoundIndexKey, serialize_compound_index_key
        };
        use serde_json::Value;

        if page_num == 0 {
            return Ok(());
        }

        let page_data = self.pager.read_page(page_num)?;
        let node = deserialize_node(page_num, &page_data)?;

        if node.node_type == NodeType::LeafNode {
            for entry in &node.entries {
                let doc_page_num = entry.value;
                let doc_id = &entry.key;

                let vdoc = match read_versioned_document(&self.pager, doc_page_num, &std::collections::HashMap::new()) {
                    Ok(vdoc) => vdoc,
                    Err(_) => continue,
                };

                let doc_map: serde_json::Map<String, Value> = match serde_json::from_slice(&vdoc.data) {
                    Ok(map) => map,
                    Err(_) => continue,
                };

                let key_str = if fields.len() == 1 {
                    // Single-field index (backward compatible)
                    let field_value = extract_field_values(&doc_map, fields)[0].clone();
                    let index_key = IndexKey {
                        field_value,
                        doc_id: doc_id.clone(),
                    };
                    serialize_index_key(&index_key)?
                } else {
                    // Compound index
                    let field_values = extract_field_values(&doc_map, fields);
                    let compound_key = CompoundIndexKey {
                        field_values,
                        doc_id: doc_id.clone(),
                    };
                    serialize_compound_index_key(&compound_key)?
                };

                if unique {
                    if index_btree.search(&key_str).is_ok() {
                        return Err(Error::Other(format!(
                            "unique constraint violation on fields {:?}: duplicate value found",
                            fields
                        )));
                    }
                }

                index_btree.insert(&key_str, doc_page_num)?;
            }

            if node.next_leaf != 0 {
                return self.scan_btree_node_for_compound_index(index_btree, node.next_leaf, fields, unique);
            }
        } else {
            for child_page in &node.children {
                self.scan_btree_node_for_compound_index(index_btree, *child_page, fields, unique)?;
            }
        }

        Ok(())
    }

    fn build_text_index_from_btree(
        &self,
        text_index: &mut crate::core::text_search::TextIndex,
        root_page: PageNum,
        fields: &[String],
    ) -> Result<()> {
        self.scan_btree_node_for_text_index(text_index, root_page, fields)
    }

    fn scan_btree_node_for_text_index(
        &self,
        text_index: &mut crate::core::text_search::TextIndex,
        page_num: PageNum,
        fields: &[String],
    ) -> Result<()> {
        use crate::core::btree::{deserialize_node, NodeType};
        use crate::core::document::read_versioned_document;
        use serde_json::Value;

        if page_num == 0 {
            return Ok(());
        }

        let page_data = self.pager.read_page(page_num)?;
        let node = deserialize_node(page_num, &page_data)?;

        if node.node_type == NodeType::LeafNode {
            for entry in &node.entries {
                let doc_page_num = entry.value;
                let doc_id = &entry.key;

                let vdoc = match read_versioned_document(&self.pager, doc_page_num, &std::collections::HashMap::new()) {
                    Ok(vdoc) => vdoc,
                    Err(_) => continue,
                };

                let doc_map: serde_json::Map<String, Value> = match serde_json::from_slice(&vdoc.data) {
                    Ok(map) => map,
                    Err(_) => continue,
                };

                // Extract text field values
                let mut field_values = std::collections::HashMap::new();
                for field in fields {
                    if let Some(value) = doc_map.get(field) {
                        if let Some(text) = value.as_str() {
                            field_values.insert(field.clone(), text.to_string());
                        }
                    }
                }

                // Index the document if it has any text fields
                if !field_values.is_empty() {
                    text_index.index_document(doc_id, &field_values)?;
                }
            }

            if node.next_leaf != 0 {
                return self.scan_btree_node_for_text_index(text_index, node.next_leaf, fields);
            }
        } else {
            for child_page in &node.children {
                self.scan_btree_node_for_text_index(text_index, *child_page, fields)?;
            }
        }

        Ok(())
    }

    /// Get a snapshot of current database metrics.
    /// This is a zero-cost operation that reads atomic counters.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Get a reference to the internal metrics object for instrumentation.
    pub(crate) fn metrics_ref(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// Create a backup of the database by checkpointing WAL and copying the main file.
    /// The backup is created atomically (written to temp file, then renamed).
    ///
    /// # Arguments
    /// * `dest_path` - Destination path for the backup file
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// db.backup("./backups/mydb-2024-01-15.db").unwrap();
    /// ```
    pub fn backup(&self, dest_path: &str) -> Result<()> {
        if self.read_only {
            return Err(Error::Other("cannot backup: database is read-only".to_string()));
        }

        // Step 1: Checkpoint WAL to flush all pending writes to main db file
        self.checkpoint()?;

        // Step 2: Get source and dest paths
        let source_path = &self.path;
        let temp_dest = format!("{}.tmp", dest_path);

        // Step 3: Copy file to temporary location
        let bytes_copied = std::fs::copy(source_path, &temp_dest)?;

        // Step 4: Verify the copy (compare file sizes)
        let source_metadata = std::fs::metadata(source_path)?;
        if bytes_copied != source_metadata.len() {
            let _ = std::fs::remove_file(&temp_dest);
            return Err(Error::Other(format!(
                "backup verification failed: source={} bytes, copied={} bytes",
                source_metadata.len(),
                bytes_copied
            )));
        }

        // Step 5: Atomic rename from temp to final destination
        std::fs::rename(&temp_dest, dest_path)?;

        Ok(())
    }

    /// Verify a backup file by checking its magic number and metadata.
    ///
    /// # Arguments
    /// * `backup_path` - Path to the backup file to verify
    ///
    /// # Returns
    /// Returns `Ok(BackupInfo)` with backup details if valid, or an error if corrupted.
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// let info = Database::verify_backup("./backups/mydb.db").unwrap();
    /// println!("Backup has {} collections", info.num_collections);
    /// ```
    pub fn verify_backup(backup_path: &str) -> Result<BackupInfo> {
        use std::io::Read;

        // Open the backup file read-only
        let mut file = std::fs::File::open(backup_path)?;

        // Read and verify the header (first page)
        let mut header_buf = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut header_buf)?;

        // Check magic number
        if &header_buf[0..4] != MAGIC {
            return Err(Error::InvalidMagic);
        }

        // Parse version
        let version_bytes: [u8; 4] = header_buf[4..8].try_into()
            .map_err(|_| Error::DataCorruption {
                details: "invalid version in backup header".to_string()
            })?;
        let version = u32::from_le_bytes(version_bytes);

        // Parse page count
        let num_pages_bytes: [u8; 8] = header_buf[12..20].try_into()
            .map_err(|_| Error::DataCorruption {
                details: "invalid num_pages in backup header".to_string()
            })?;
        let num_pages = u64::from_le_bytes(num_pages_bytes);

        // Parse metadata page
        let metadata_page_bytes: [u8; 8] = header_buf[24..32].try_into()
            .map_err(|_| Error::DataCorruption {
                details: "invalid metadata_page in backup header".to_string()
            })?;
        let metadata_page = u64::from_le_bytes(metadata_page_bytes);

        // If there's a metadata page, count collections
        let num_collections = if metadata_page > 0 {
            // Open in temporary read-only mode to read metadata
            let temp_pager = Pager::open(backup_path, 100, 0o644, true)?;
            let meta_data = temp_pager.read_page(metadata_page)?;
            let metadata = Metadata::deserialize(&meta_data)?;
            metadata.collections.len()
        } else {
            0
        };

        let file_metadata = std::fs::metadata(backup_path)?;

        Ok(BackupInfo {
            version,
            num_pages,
            num_collections,
            file_size: file_metadata.len(),
        })
    }

    /// Start a web UI server for exploring the database and viewing metrics.
    /// The server runs in a background thread and serves a dashboard at the specified address.
    ///
    /// # Arguments
    /// * `addr` - Address to bind the server to (e.g., "127.0.0.1:8080")
    ///
    /// # Returns
    /// Returns a `WebServer` handle that will shutdown the server when dropped.
    ///
    /// # Example
    /// ```no_run
    /// # use jasonisnthappy::Database;
    /// # let db = Database::open("my.db").unwrap();
    /// let web_server = db.start_web_ui("127.0.0.1:8080").unwrap();
    /// println!("Web UI available at http://127.0.0.1:8080");
    /// // Server will automatically stop when web_server is dropped
    /// ```
    #[cfg(feature = "web-ui")]
    pub fn start_web_ui(&self, addr: &str) -> Result<crate::core::web_server::WebServer> {
        let db = Arc::new(Self {
            pager: self.pager.clone(),
            wal: self.wal.clone(),
            metadata: self.metadata.clone(),
            tx_manager: self.tx_manager.clone(),
            lock_file: self.lock_file.clone(),
            path: self.path.clone(),
            read_only: self.read_only,
            commit_mu: self.commit_mu.clone(),
            version_chains: self.version_chains.clone(),
            tx_config: self.tx_config.clone(),
            auto_checkpoint_threshold: self.auto_checkpoint_threshold.clone(),
            checkpoint_in_progress: self.checkpoint_in_progress.clone(),
            metrics: self.metrics.clone(),
            watchers: self.watchers.clone(),
            node_serialize_pool: self.node_serialize_pool.clone(),
            page_buffer_pool: self.page_buffer_pool.clone(),
            tx_id_counter: self.tx_id_counter.clone(),
            pending_writes: self.pending_writes.clone(),
            batch_config: self.batch_config.clone(),
            max_bulk_operations: self.max_bulk_operations,
            max_document_size: self.max_document_size,
            max_request_body_size: self.max_request_body_size,
        });

        crate::core::web_server::WebServer::start(db, addr)
            .map_err(|e| Error::Other(format!("Failed to start web UI: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_database_open() {
        let path = "/tmp/test_db_open.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        assert_eq!(db.path(), path);
        assert!(!db.is_read_only());

        db.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_database_begin_transaction() {
        let path = "/tmp/test_db_begin_tx.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();

        let tx = db.begin().unwrap();
        assert!(tx.is_active());

        db.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_database_metadata() {
        let path = "/tmp/test_db_metadata.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();

        let meta = db.get_metadata();
        assert_eq!(meta.collections.len(), 0);

        db.update_metadata(|m| {
            m.get_collection("users");
        }).unwrap();

        let meta = db.get_metadata();
        assert_eq!(meta.collections.len(), 1);
        assert!(meta.collections.contains_key("users"));

        db.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_database_read_only() {
        let path = "/tmp/test_db_readonly.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        {
            let db = Database::open(path).unwrap();
            db.close().unwrap();
        }

        let opts = DatabaseOptions {
            read_only: true,
            ..Default::default()
        };

        let db = Database::open_with_options(path, opts).unwrap();
        assert!(db.is_read_only());

        let result = db.update_metadata(|m| {
            m.get_collection("users");
        });
        assert!(result.is_err());

        db.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_database_cannot_open_twice() {
        let path = "/tmp/test_db_double_open.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db1 = Database::open(path).unwrap();

        let result = Database::open(path);
        assert!(result.is_err());

        db1.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_database_reopen() {
        let path = "/tmp/test_db_reopen.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        {
            let db = Database::open(path).unwrap();
            db.update_metadata(|m| {
                m.get_collection("users");
            }).unwrap();
            db.close().unwrap();
        }

        {
            let db = Database::open(path).unwrap();
            let meta = db.get_metadata();
            assert_eq!(meta.collections.len(), 1);
            assert!(meta.collections.contains_key("users"));
            db.close().unwrap();
        }

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_database_drop_without_close() {
        // Test that Drop implementation properly cleans up resources
        // even when close() is not explicitly called
        let path = "/tmp/test_db_drop_auto.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        // Open and use database, but don't call close()
        {
            let db = Database::open(path).unwrap();
            db.update_metadata(|m| {
                m.get_collection("test_collection");
            }).unwrap();
            // Drop happens here automatically - no explicit close()
        }

        // If Drop worked correctly, we should be able to reopen the database
        {
            let db = Database::open(path).unwrap();
            let meta = db.get_metadata();
            assert_eq!(meta.collections.len(), 1);
            assert!(meta.collections.contains_key("test_collection"));
            db.close().unwrap();
        }

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_database_backup() {
        let path = "/tmp/test_db_backup.db";
        let backup_path = "/tmp/test_db_backup_copy.db";

        // Cleanup
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
        let _ = fs::remove_file(backup_path);
        let _ = fs::remove_file(format!("{}.lock", backup_path));
        let _ = fs::remove_file(format!("{}-wal", backup_path));

        {
            // Create database with some data
            let db = Database::open(path).unwrap();
            let mut tx = db.begin().unwrap();
            let mut users = tx.collection("users").unwrap();

            users.insert(serde_json::json!({
                "name": "Alice",
                "age": 30
            })).unwrap();

            tx.commit().unwrap();

            // Create backup
            db.backup(backup_path).unwrap();
            db.close().unwrap();
        }

        // Verify backup info
        let info = Database::verify_backup(backup_path).unwrap();
        assert_eq!(info.num_collections, 1);
        assert!(info.file_size > 0);

        // Open backup and verify data
        {
            let backup_db = Database::open(backup_path).unwrap();
            let meta = backup_db.get_metadata();
            assert_eq!(meta.collections.len(), 1);
            assert!(meta.collections.contains_key("users"));

            let mut tx = backup_db.begin().unwrap();
            let users = tx.collection("users").unwrap();
            let docs = users.find_all().unwrap();
            assert_eq!(docs.len(), 1);
            assert_eq!(docs[0]["name"], "Alice");

            backup_db.close().unwrap();
        }

        // Cleanup
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
        let _ = fs::remove_file(backup_path);
        let _ = fs::remove_file(format!("{}.lock", backup_path));
        let _ = fs::remove_file(format!("{}-wal", backup_path));
    }

    #[test]
    fn test_list_collections() {
        use serde_json::json;

        let path = "/tmp/test_list_collections.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();

        // Initially empty
        let collections = db.list_collections().unwrap();
        assert_eq!(collections.len(), 0);

        // Add some collections by inserting documents
        let users = db.collection("users");
        users.insert(json!({"name": "Alice"})).unwrap();

        let products = db.collection("products");
        products.insert(json!({"name": "Widget"})).unwrap();

        let orders = db.collection("orders");
        orders.insert(json!({"order_id": 1})).unwrap();

        // List should be sorted
        let collections = db.list_collections().unwrap();
        assert_eq!(collections.len(), 3);
        assert_eq!(collections, vec!["orders", "products", "users"]);

        db.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_collection_stats() {
        use serde_json::json;

        let path = "/tmp/test_collection_stats.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        // Insert some documents
        users.insert(json!({"name": "Alice", "age": 30})).unwrap();
        users.insert(json!({"name": "Bob", "age": 25})).unwrap();
        users.insert(json!({"name": "Charlie", "age": 35})).unwrap();

        // Get stats
        let stats = db.collection_stats("users").unwrap();
        assert_eq!(stats.name, "users");
        assert_eq!(stats.document_count, 3);
        assert!(stats.btree_root > 0);
        assert_eq!(stats.indexes.len(), 0); // No indexes yet

        // Test non-existent collection
        let result = db.collection_stats("nonexistent");
        assert!(result.is_err());

        db.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_list_indexes() {
        use serde_json::json;

        let path = "/tmp/test_list_indexes.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        // Insert some documents
        users.insert(json!({"name": "Alice", "age": 30})).unwrap();
        users.insert(json!({"name": "Bob", "age": 25})).unwrap();

        // Initially no indexes
        let indexes = db.list_indexes("users").unwrap();
        assert_eq!(indexes.len(), 0);

        // Create some indexes
        db.create_compound_index("users", "age_idx", &["age"], false).unwrap();
        db.create_compound_index("users", "name_age_idx", &["name", "age"], false).unwrap();

        // List indexes
        let indexes = db.list_indexes("users").unwrap();
        assert_eq!(indexes.len(), 2);

        // Verify index info
        let age_idx = indexes.iter().find(|idx| idx.name == "age_idx");
        assert!(age_idx.is_some());
        let age_idx = age_idx.unwrap();
        assert_eq!(age_idx.fields, vec!["age"]);
        assert!(!age_idx.unique);

        let compound_idx = indexes.iter().find(|idx| idx.name == "name_age_idx");
        assert!(compound_idx.is_some());
        let compound_idx = compound_idx.unwrap();
        assert_eq!(compound_idx.fields, vec!["name", "age"]);

        // Test non-existent collection
        let result = db.list_indexes("nonexistent");
        assert!(result.is_err());

        db.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_database_info() {
        use serde_json::json;

        let path = "/tmp/test_database_info.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();

        // Empty database info
        let info = db.info().unwrap();
        assert_eq!(info.path, path);
        assert!(info.version > 0);
        assert!(info.num_pages > 0);
        assert!(info.file_size > 0);
        assert_eq!(info.collections.len(), 0);
        assert_eq!(info.total_documents, 0);
        assert!(!info.read_only);

        // Add some data
        let users = db.collection("users");
        users.insert(json!({"name": "Alice"})).unwrap();
        users.insert(json!({"name": "Bob"})).unwrap();

        let products = db.collection("products");
        products.insert(json!({"name": "Widget"})).unwrap();
        products.insert(json!({"name": "Gadget"})).unwrap();
        products.insert(json!({"name": "Gizmo"})).unwrap();

        // Get updated info
        let info = db.info().unwrap();
        assert_eq!(info.collections.len(), 2);
        assert_eq!(info.total_documents, 5);

        // Verify collections are sorted
        assert_eq!(info.collections[0].name, "products");
        assert_eq!(info.collections[1].name, "users");

        // Verify counts
        assert_eq!(info.collections[0].document_count, 3);
        assert_eq!(info.collections[1].document_count, 2);

        db.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_introspection_with_indexes() {
        use serde_json::json;

        let path = "/tmp/test_introspection_indexes.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));

        let db = Database::open(path).unwrap();
        let users = db.collection("users");

        // Insert documents and create indexes
        for i in 0..10 {
            users.insert(json!({"name": format!("User{}", i), "age": 20 + i})).unwrap();
        }

        db.create_compound_index("users", "age_idx", &["age"], false).unwrap();
        db.create_compound_index("users", "name_idx", &["name"], false).unwrap();

        // Get collection stats
        let stats = db.collection_stats("users").unwrap();
        assert_eq!(stats.document_count, 10);
        assert_eq!(stats.indexes.len(), 2);

        // Get database info
        let info = db.info().unwrap();
        assert_eq!(info.total_documents, 10);
        assert_eq!(info.collections[0].indexes.len(), 2);

        db.close().unwrap();

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.lock", path));
        let _ = fs::remove_file(format!("{}-wal", path));
    }
}
