
use crate::core::constants::*;
use crate::core::errors::*;
use crate::core::mvcc::TransactionManager;
use crate::core::pager::Pager;
use crate::core::wal::WAL;
use crate::core::tx_collection::TxCollection;
use crate::core::database::Database;
use crate::core::watch::{emit_change, ChangeOperation};
use crate::core::document::read_versioned_document;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use crate::core::errors::PoisonedLockExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    Active,
    Committed,
    RolledBack,
}

pub struct Transaction {
    pub tx_id: u64,
    pub mvcc_tx_id: TransactionID,
    pub snapshot_id: TransactionID,
    pub state: TxState,

    writes: Arc<RwLock<HashMap<PageNum, Vec<u8>>>>,
    doc_writes: Arc<RwLock<HashMap<String, HashMap<String, PageNum>>>>,

    snapshot_roots: HashMap<String, PageNum>,

    updated_roots: Arc<RwLock<HashMap<String, PageNum>>>,

    doc_existed_in_snapshot: Arc<RwLock<HashMap<String, HashMap<String, bool>>>>,
    // Track the xmin of documents when we first read them (for conflict detection)
    doc_original_xmin: Arc<RwLock<HashMap<String, HashMap<String, TransactionID>>>>,

    pager: Arc<Pager>,
    wal: Arc<WAL>,
    tx_manager: Arc<TransactionManager>,

    db: Option<Arc<Database>>,

    modified_collections: Arc<RwLock<HashSet<String>>>,

    old_versions: Arc<RwLock<HashMap<String, HashMap<String, crate::core::mvcc::DocumentVersion>>>>,

    commit_mu: Arc<Mutex<()>>,
}

static GLOBAL_TX_ID: AtomicU64 = AtomicU64::new(1);

impl Transaction {
    pub fn new(
        pager: Arc<Pager>,
        wal: Arc<WAL>,
        tx_manager: Arc<TransactionManager>,
        collection_roots: HashMap<String, PageNum>,
        commit_mu: Arc<Mutex<()>>,
        tx_id_counter: Option<Arc<AtomicU64>>,
    ) -> Result<Self> {
        // Use per-database TX ID counter if provided, otherwise fall back to global
        let tx_id = if let Some(counter) = &tx_id_counter {
            counter.fetch_add(1, Ordering::SeqCst)
        } else {
            GLOBAL_TX_ID.fetch_add(1, Ordering::SeqCst)
        };
        let mvcc_tx_id = tx_manager.begin_transaction()?;
        let snapshot_id = tx_manager.get_latest_committed_tx_id();


        Ok(Self {
            tx_id,
            mvcc_tx_id,
            snapshot_id,
            state: TxState::Active,
            writes: Arc::new(RwLock::new(HashMap::new())),
            doc_writes: Arc::new(RwLock::new(HashMap::new())),
            snapshot_roots: collection_roots.clone(),
            updated_roots: Arc::new(RwLock::new(collection_roots)),
            doc_existed_in_snapshot: Arc::new(RwLock::new(HashMap::new())),
            doc_original_xmin: Arc::new(RwLock::new(HashMap::new())),
            pager,
            wal,
            tx_manager,
            db: None,
            modified_collections: Arc::new(RwLock::new(HashSet::new())),
            old_versions: Arc::new(RwLock::new(HashMap::new())),
            commit_mu,
        })
    }

    pub(crate) fn set_database(&mut self, db: Arc<Database>) {
        self.db = Some(db);
    }

    pub fn collection(&mut self, name: &str) -> Result<TxCollection<'_>> {
        let db = self.db.as_ref()
            .expect("Transaction must have database reference set")
            .clone();
        TxCollection::new(self, db, name.to_string())
    }

    /// Create a new collection
    pub fn create_collection(&mut self, name: &str) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TxNotActive);
        }

        let db = self.db.as_ref()
            .expect("Transaction must have database reference set");

        // Check if collection already exists
        {
            let metadata = db.get_metadata();
            if metadata.collections.contains_key(name) {
                return Err(Error::CollectionAlreadyExists {
                    name: name.to_string(),
                });
            }
        }

        // Create the collection in metadata
        db.update_metadata_no_flush(|m| {
            m.get_collection(name);
        });

        Ok(())
    }

    /// Drop (delete) a collection and all its documents
    pub fn drop_collection(&mut self, name: &str) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TxNotActive);
        }

        let db = self.db.as_ref()
            .expect("Transaction must have database reference set");

        // Check if collection exists and remove it
        let collection_existed = {
            let metadata = db.get_metadata();
            metadata.collections.contains_key(name)
        };

        if !collection_existed {
            return Err(Error::CollectionDoesNotExist {
                name: name.to_string(),
            });
        }

        // Remove from metadata
        db.update_metadata_no_flush(|m| {
            m.collections.remove(name);
        });

        // Remove from version chains
        let mut chains = db.version_chains.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.version_chains".to_string() })?;
        chains.remove(name);

        Ok(())
    }

    /// Rename a collection
    pub fn rename_collection(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TxNotActive);
        }

        let db = self.db.as_ref()
            .expect("Transaction must have database reference set");

        // Check preconditions
        {
            let metadata = db.get_metadata();
            if metadata.collections.contains_key(new_name) {
                return Err(Error::CollectionAlreadyExists {
                    name: new_name.to_string(),
                });
            }
            if !metadata.collections.contains_key(old_name) {
                return Err(Error::CollectionDoesNotExist {
                    name: old_name.to_string(),
                });
            }
        }

        // Move collection metadata
        db.update_metadata_no_flush(|m| {
            if let Some(collection_meta) = m.collections.remove(old_name) {
                m.collections.insert(new_name.to_string(), collection_meta);
            }
        });

        // Move version chains
        let mut chains = db.version_chains.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "database.version_chains".to_string() })?;
        if let Some(chain) = chains.remove(old_name) {
            chains.insert(new_name.to_string(), chain);
        }

        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.state == TxState::Active
    }

    pub fn write_page(&self, page_num: PageNum, data: Vec<u8>) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TxNotActive);
        }

        let mut writes = self.writes.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
        writes.insert(page_num, data);
        Ok(())
    }

    pub fn write_document(&self, collection: &str, doc_id: &str, page_num: PageNum) -> Result<()> {
        if !self.is_active() {
            return Err(Error::TxNotActive);
        }

        let mut doc_writes = self.doc_writes.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_writes".to_string() })?;
        doc_writes
            .entry(collection.to_string())
            .or_insert_with(HashMap::new)
            .insert(doc_id.to_string(), page_num);
        Ok(())
    }

    pub fn get_writes(&self) -> HashMap<PageNum, Vec<u8>> {
        let writes = self.writes.read()
            .recover_poison();
        writes.clone()
    }

    pub fn get_writes_arc(&self) -> Arc<RwLock<HashMap<PageNum, Vec<u8>>>> {
        self.writes.clone()
    }

    pub fn get_pager(&self) -> &Arc<Pager> {
        &self.pager
    }

    pub(crate) fn get_database(&self) -> Option<&Arc<Database>> {
        self.db.as_ref()
    }

    pub(crate) fn add_old_version(&self, collection: &str, doc_id: &str, version: crate::core::mvcc::DocumentVersion) {
        let mut old_versions = self.old_versions.write()
            .recover_poison();
        old_versions
            .entry(collection.to_string())
            .or_insert_with(HashMap::new)
            .insert(doc_id.to_string(), version);
    }

    pub(crate) fn set_collection_root(&self, collection: &str, new_root: PageNum) {
        let mut updated_roots = self.updated_roots.write()
            .recover_poison();
        updated_roots.insert(collection.to_string(), new_root);

        let mut modified = self.modified_collections.write()
            .recover_poison();
        modified.insert(collection.to_string());
    }

    fn detect_write_conflicts_with_context(
        &self,
        collection_name: &str,
        current_root: PageNum,
        snapshot_id: TransactionID,
        doc_writes_map: &HashMap<String, HashMap<String, PageNum>>,
        doc_existed_map: &HashMap<String, HashMap<String, bool>>,
        doc_xmin_map: &HashMap<String, HashMap<String, TransactionID>>,
    ) -> Result<()> {
        use crate::core::tx_btree::TxBTree;
        use crate::core::document::read_versioned_document;

        let collection_writes = match doc_writes_map.get(collection_name) {
            Some(writes) if !writes.is_empty() => writes,
            _ => return Ok(()), // No writes, no conflicts
        };

        let collection_xmins = doc_xmin_map.get(collection_name);

        // Only create B-tree if we have documents that need conflict checking
        let needs_check = collection_writes.iter().any(|(doc_id, _)| {
            let existed = doc_existed_map
                .get(collection_name)
                .and_then(|docs| docs.get(doc_id).copied())
                .unwrap_or(false);
            existed && collection_xmins.and_then(|xmins| xmins.get(doc_id)).is_some()
        });

        if !needs_check {
            return Ok(());
        }

        // Create B-tree to search for COMMITTED versions (not our writes)
        let empty_writes = Arc::new(RwLock::new(HashMap::new()));
        let current_btree = TxBTree::new(self.pager.clone(), current_root, empty_writes);

        // Check each document we wrote
        for (doc_id, _) in collection_writes.iter() {
            // Check if this document existed in our snapshot
            let existed_in_snapshot = doc_existed_map
                .get(collection_name)
                .and_then(|docs| docs.get(doc_id).copied())
                .unwrap_or(false);

            if !existed_in_snapshot {
                continue; // New insert - no need to check
            }

            // Get the xmin we saw when we first read this document
            let original_xmin = collection_xmins
                .and_then(|xmins| xmins.get(doc_id).copied());

            if let Some(orig_xmin) = original_xmin {
                // Search B-tree to find COMMITTED version (not our writes)
                match current_btree.search(doc_id) {
                    Ok(committed_page_num) => {
                        // Read the committed version
                        let empty_map = HashMap::new();
                        match read_versioned_document(&self.pager, committed_page_num, &empty_map) {
                            Ok(committed_vdoc) => {
                                // Conflict if committed version is different and newer than our snapshot
                                if committed_vdoc.xmin != orig_xmin {
                                    if committed_vdoc.xmin > snapshot_id {
                                        return Err(Error::TxConflict);
                                    }
                                }
                            }
                            Err(_) => {
                                return Err(Error::TxConflict);
                            }
                        }
                    }
                    Err(_) => {
                        // Document was deleted - CONFLICT
                        return Err(Error::TxConflict);
                    }
                }
            }
        }

        Ok(())
    }

    fn detect_write_conflicts(&self, collection_name: &str, current_root: PageNum) -> Result<()> {
        use crate::core::tx_btree::TxBTree;
        use crate::core::document::read_versioned_document;

        let doc_writes = self.doc_writes.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_writes".to_string() })?;
        let collection_writes = match doc_writes.get(collection_name) {
            Some(writes) if !writes.is_empty() => writes,
            _ => return Ok(()), // No writes, no conflicts
        };

        let doc_xmins = self.doc_original_xmin.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_original_xmin".to_string() })?;
        let collection_xmins = doc_xmins.get(collection_name);

        // Only create B-tree if we have documents that need conflict checking
        let needs_check = collection_writes.iter().any(|(doc_id, _)| {
            let existed = self.doc_existed_in_snapshot
                .read()
                .recover_poison()
                .get(collection_name)
                .and_then(|docs| docs.get(doc_id).copied())
                .unwrap_or(false);
            existed && collection_xmins.and_then(|xmins| xmins.get(doc_id)).is_some()
        });

        if !needs_check {
            return Ok(());
        }

        // Create B-tree to search for COMMITTED versions (not our writes)
        let empty_writes = Arc::new(RwLock::new(HashMap::new()));
        let current_btree = TxBTree::new(self.pager.clone(), current_root, empty_writes);

        // Check each document we wrote
        for (doc_id, _) in collection_writes.iter() {
            // Check if this document existed in our snapshot
            let existed_in_snapshot = self.doc_existed_in_snapshot
                .read()
                .recover_poison()
                .get(collection_name)
                .and_then(|docs| docs.get(doc_id).copied())
                .unwrap_or(false);

            // OPTIMIZATION: Skip conflict check for new inserts (didn't exist in snapshot)
            if !existed_in_snapshot {
                continue;
            }

            // Get the xmin we saw when we first read this document
            let original_xmin = collection_xmins
                .and_then(|xmins| xmins.get(doc_id).copied());

            if let Some(orig_xmin) = original_xmin {
                // Search B-tree to find COMMITTED version (not our writes)
                match current_btree.search(doc_id) {
                    Ok(committed_page_num) => {
                        // Read the committed version directly from pager (not from any transaction writes)
                        let empty_map = HashMap::new();
                        match read_versioned_document(&self.pager, committed_page_num, &empty_map) {
                            Ok(committed_vdoc) => {
                                // Conflict if the committed version was created by a different transaction
                                // after our snapshot AND is different from what we read
                                if committed_vdoc.xmin != orig_xmin {
                                    // Someone modified it - check if they committed after our snapshot
                                    if committed_vdoc.xmin > self.snapshot_id {
                                        // CONFLICT: Document modified after our snapshot
                                        return Err(Error::TxConflict);
                                    }
                                }
                            }
                            Err(_) => {
                                // Could not read document - might be corrupted, treat as conflict
                                return Err(Error::TxConflict);
                            }
                        }
                    }
                    Err(_) => {
                        // Document was deleted - CONFLICT
                        return Err(Error::TxConflict);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.state != TxState::Active {
            return Err(Error::TxAlreadyDone);
        }

        // Check if we have any writes (acquire and drop lock immediately)
        let has_writes = {
            let writes = self.writes.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
            !writes.is_empty()
        };

        if !has_writes {
            self.state = TxState::Committed;
            self.tx_manager.commit_transaction(self.mvcc_tx_id)?;

            // Track metrics
            if let Some(db) = &self.db {
                db.metrics_ref().transaction_committed();
            }

            return Ok(());
        }

        // Check if batching is enabled
        let batch_enabled = self.db.as_ref().map(|db| db.batch_config.enabled).unwrap_or(false);

        if batch_enabled {
            return self.commit_batched();
        } else {
            return self.commit_single();
        }
    }

    fn commit_batched(&mut self) -> Result<()> {
        use std::time::Instant;

        // Phase 1: Prepare write request
        let pending = self.prepare_write_request()?;
        let completion = pending.completion.clone();

        // Phase 2: Submit to queue
        if let Some(db) = &self.db {
            let mut queue = db.pending_writes.lock()
                .map_err(|_| Error::LockPoisoned { lock_name: "database.pending_writes".to_string() })?;
            queue.push_back(pending);
        } else {
            return Err(Error::Other("database reference not set".into()));
        }

        // Phase 3: Try to become leader by acquiring commit_mu
        let _commit_guard = self.commit_mu.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.commit_mu".to_string() })?;

        // Phase 3.5: Check if we've already been processed by another leader
        {
            let (lock, _) = &*completion;
            let result = lock.lock()
                .map_err(|_| Error::LockPoisoned { lock_name: "completion.lock".to_string() })?;
            if result.is_some() {
                // We've been processed by another leader, just wait and return
                drop(result);
                drop(_commit_guard);

                // Wait for result
                let (lock, cvar) = &*completion;
                let mut result = lock.lock()
                    .map_err(|_| Error::LockPoisoned { lock_name: "completion.lock".to_string() })?;
                while result.is_none() {
                    result = cvar.wait(result)
                        .map_err(|_| Error::LockPoisoned { lock_name: "completion.condvar".to_string() })?;
                }

                let commit_result = result.take().unwrap()?;
                self.state = TxState::Committed;
                return Ok(commit_result);
            }
        }

        loop {
            let mut batch = self.collect_batch()?;
            let batch_size = batch.len();

            // Phase 5: Execute batch commit
            let batch_start = Instant::now();
            let batch_result = self.execute_batch_commit(&mut batch);

            // Phase 6: Notify all waiters with results
            for pending_write in batch {
                let (lock, cvar) = &*pending_write.completion;
                let mut result = lock.lock()
                    .map_err(|_| Error::LockPoisoned { lock_name: "pending_write.completion".to_string() })?;
                *result = Some(batch_result.clone());
                cvar.notify_one();
            }

            // Phase 7: Track metrics
            if let Some(db) = &self.db {
                let elapsed = batch_start.elapsed();
                db.metrics_ref().batch_committed(batch_size, elapsed);

                // Also track individual transaction commit
                db.metrics_ref().transaction_committed();
                db.maybe_auto_checkpoint();
            }

            // Check if there are more items in the queue
            let queue_empty = {
                let db = self.db.as_ref().ok_or_else(|| Error::Other("database reference not set".into()))?;
                let queue = db.pending_writes.lock()
                    .map_err(|_| Error::LockPoisoned { lock_name: "database.pending_writes".to_string() })?;
                queue.is_empty()
            };

            if queue_empty {
                break;  // No more work, exit loop
            }
            // Otherwise, loop and process next batch
        }

        // Release commit_mu (implicit drop)
        drop(_commit_guard);

        // Phase 8: Wait for our own result
        let (lock, cvar) = &*completion;
        let mut result = lock.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "completion.lock".to_string() })?;
        while result.is_none() {
            result = cvar.wait(result)
                .map_err(|_| Error::LockPoisoned { lock_name: "completion.condvar".to_string() })?;
        }

        let commit_result = result.take().unwrap()?;

        // Mark ourselves as committed
        self.state = TxState::Committed;

        Ok(commit_result)
    }

    fn commit_single(&mut self) -> Result<()> {
        if let Some(db) = &self.db {
            let modified = self.modified_collections.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.modified_collections".to_string() })?;

            for collection_name in modified.iter() {
                let current_metadata = db.get_metadata();
                let current_root = current_metadata.collections
                    .get(collection_name)
                    .map(|c| c.btree_root)
                    .unwrap_or(0);

                // Always check for write conflicts, even if root hasn't changed
                // (documents can be modified without changing the tree structure)
                self.detect_write_conflicts(collection_name, current_root)?;
            }
        }

        // Now acquire commit lock AFTER conflict detection
        let _commit_guard = self.commit_mu.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.commit_mu".to_string() })?;

        // Conflict detection passed! Now write to WAL and pager.
        // Snapshot the writes to release the lock quickly
        let writes_snapshot: Vec<(PageNum, Vec<u8>)> = {
            let writes = self.writes.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
            writes.iter().map(|(&k, v)| (k, v.clone())).collect()
        };

        // Write frames with minimal cloning
        for (page_num, page_data) in writes_snapshot {
            // WAL needs data for serialization
            self.wal.write_frame(self.mvcc_tx_id, page_num, page_data.clone())?;
            // Pager takes ownership
            self.pager.write_page_transfer(page_num, page_data)?;
        }

        self.wal.refresh_frame_count()?;

        // Now update metadata for each modified collection
        if let Some(db) = &self.db {
            use crate::core::tx_btree::TxBTree;
            use crate::core::document::read_versioned_document;

            let modified = self.modified_collections.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.modified_collections".to_string() })?;

            for collection_name in modified.iter() {
                let snapshot_root = self.snapshot_roots.get(collection_name).copied().unwrap_or(0);

                let current_metadata = db.get_metadata();
                let current_root = current_metadata.collections
                    .get(collection_name)
                    .map(|c| c.btree_root)
                    .unwrap_or(0);

                if current_root == snapshot_root {
                    // Fast path: no concurrent changes to tree structure
                    let updated = self.updated_roots.read()
                        .map_err(|_| Error::LockPoisoned { lock_name: "transaction.updated_roots".to_string() })?;
                    if let Some(&new_btree_root) = updated.get(collection_name) {
                        db.update_metadata_no_flush(|m| {
                            let coll = m.get_collection(collection_name);
                            coll.btree_root = new_btree_root;
                        });
                    }
                } else {
                    let writes_snapshot: HashMap<PageNum, Vec<u8>> = {
                        let writes = self.writes.read()
                            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
                        writes.clone()
                    };

                    self.wal.refresh_frame_count()?;

                    self.validate_unique_constraints_for_collection(collection_name)?;

                    let mut page_remapping: HashMap<PageNum, PageNum> = HashMap::new();
                    let doc_writes_guard = self.doc_writes.read()
                        .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_writes".to_string() })?;
                    if let Some(collection_doc_writes) = doc_writes_guard.get(collection_name) {
                        for (_, &old_page_num) in collection_doc_writes.iter() {
                            if let Ok(_page_data) = self.pager.read_page(old_page_num) {
                                let writes_for_check = self.writes.read()
                                    .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
                                if let Ok(current_doc) = read_versioned_document(&self.pager, old_page_num, &writes_for_check) {
                                    drop(writes_for_check);
                                    if current_doc.xmin != self.mvcc_tx_id {
                                        // Page was written by another transaction - allocate new page
                                        let new_page_num = self.pager.alloc_page()?;

                                        // Get our saved data for this page
                                        if let Some(our_page_data) = writes_snapshot.get(&old_page_num) {
                                            self.pager.write_page_transfer(new_page_num, our_page_data.clone())?;

                                            page_remapping.insert(old_page_num, new_page_num);
                                            let mut writes = self.writes.write()
                                                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
                                            writes.insert(new_page_num, our_page_data.clone());
                                            writes.remove(&old_page_num);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    drop(doc_writes_guard);

                    let documents_to_rebase: Vec<(String, PageNum)> = {
                        let doc_writes_guard = self.doc_writes.read()
                            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_writes".to_string() })?;
                        if let Some(collection_doc_writes) = doc_writes_guard.get(collection_name) {
                            collection_doc_writes.iter()
                                .map(|(doc_id, page_num)| {
                                    let remapped_page = page_remapping.get(page_num).copied().unwrap_or(*page_num);
                                    (doc_id.clone(), remapped_page)
                                })
                                .collect()
                        } else {
                            Vec::new()
                        }
                    };

                    {
                        let mut writes = self.writes.write()
                            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
                        for (_, page_num) in &documents_to_rebase {
                            if !writes.contains_key(page_num) {
                                if let Ok(page_data) = self.pager.read_page(*page_num) {
                                    writes.insert(*page_num, page_data);
                                }
                            }
                        }
                    }

                    let mut rebased_btree = TxBTree::new(self.pager.clone(), current_root, self.writes.clone());
                    for (doc_id, page_num) in &documents_to_rebase {
                        rebased_btree.insert(doc_id, *page_num)?;
                    }

                    {
                        let mut doc_writes_mut = self.doc_writes.write()
                            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_writes".to_string() })?;
                        if let Some(collection_doc_writes) = doc_writes_mut.get_mut(collection_name) {
                            for (doc_id, page_num) in &documents_to_rebase {
                                collection_doc_writes.insert(doc_id.clone(), *page_num);
                            }
                        }
                    }

                    let rebased_root = rebased_btree.get_current_root();

                    {
                        let mut writes = self.writes.write()
                            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;

                        for (_, &new_page) in rebased_btree.get_cow_pages().iter() {
                            if !writes.contains_key(&new_page) {
                                if let Ok(page_data) = self.pager.read_page(new_page) {
                                    writes.insert(new_page, page_data);
                                }
                            }
                        }

                        for &new_page in rebased_btree.get_new_pages().keys() {
                            if !writes.contains_key(&new_page) {
                                if let Ok(page_data) = self.pager.read_page(new_page) {
                                    writes.insert(new_page, page_data);
                                }
                            }
                        }
                    }

                    // Update metadata with rebased root
                    db.update_metadata_no_flush(|m| {
                        let coll = m.get_collection(collection_name);
                        coll.btree_root = rebased_root;
                    });
                }
            }

            let metadata = db.get_metadata();
            let mut meta_data = metadata.serialize()?;
            if meta_data.len() < PAGE_SIZE {
                meta_data.resize(PAGE_SIZE, 0);
            }

            let meta_page = self.pager.metadata_page()?;
            let meta_page = if meta_page == 0 {
                self.pager.alloc_page()?
            } else {
                meta_page
            };

            self.wal.write_frame(self.mvcc_tx_id, meta_page, meta_data.clone())?;
            self.pager.write_page_transfer(meta_page, meta_data)?;

            let mut header_data = self.pager.get_header_data()?;
            header_data[24..32].copy_from_slice(&meta_page.to_le_bytes());
            self.wal.write_frame(self.mvcc_tx_id, 0, header_data)?;

            self.pager.set_metadata_page(meta_page)?;
            self.pager.set_next_transaction_id(self.mvcc_tx_id + 1)?;

            self.pager.write_header_no_sync()?;

            self.wal.sync()?;

            self.pager.flush_no_sync()?;

            self.pager.sync_data_only()?;
        }

        self.state = TxState::Committed;
        self.tx_manager.commit_transaction(self.mvcc_tx_id)?;

        // Track metrics
        if let Some(db) = &self.db {
            db.metrics_ref().transaction_committed();
            db.maybe_auto_checkpoint();
        }

        if let Some(db) = &self.db {
            let old_versions = self.old_versions.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.old_versions".to_string() })?;
            if !old_versions.is_empty() {
                let mut version_chains = db.version_chains.write()
                    .map_err(|_| Error::LockPoisoned { lock_name: "database.version_chains".to_string() })?;

                for (coll_name, doc_versions) in old_versions.iter() {
                    let coll_chains = version_chains.entry(coll_name.clone())
                        .or_insert_with(HashMap::new);

                    for (doc_id, old_version) in doc_versions.iter() {
                        let chain = coll_chains.entry(doc_id.clone())
                            .or_insert_with(|| crate::core::mvcc::VersionChain::new(doc_id.clone()));

                        let mut version_with_xmax = old_version.clone();
                        version_with_xmax.xmax = self.mvcc_tx_id;
                        chain.add_version(version_with_xmax)?;
                    }
                }
            }
        }

        // Emit change events for watchers
        if let Some(db) = &self.db {
            let watchers = db.get_watchers();
            let doc_writes = self.doc_writes.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_writes".to_string() })?;
            let doc_existed = self.doc_existed_in_snapshot.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_existed_in_snapshot".to_string() })?;
            let writes = self.writes.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;

            for (collection_name, docs) in doc_writes.iter() {
                for (doc_id, page_num) in docs.iter() {
                    // Determine if this was an insert or update
                    let is_insert = doc_existed
                        .get(collection_name)
                        .and_then(|coll_docs| coll_docs.get(doc_id))
                        .map(|existed| !existed)
                        .unwrap_or(true); // Default to insert if not tracked

                    let operation = if is_insert {
                        ChangeOperation::Insert
                    } else {
                        ChangeOperation::Update
                    };

                    // Read the document data
                    let document = read_versioned_document(&self.pager, *page_num, &writes)
                        .ok()
                        .and_then(|vdoc| serde_json::from_slice(&vdoc.data).ok());

                    emit_change(&watchers, collection_name, operation, doc_id, document);
                }
            }
        }

        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        if self.state != TxState::Active {
            return Err(Error::TxAlreadyDone);
        }

        {
            let mut writes = self.writes.write()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
            writes.clear();
        }

        {
            let mut doc_writes = self.doc_writes.write()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_writes".to_string() })?;
            doc_writes.clear();
        }

        self.state = TxState::RolledBack;
        self.tx_manager.abort_transaction(self.mvcc_tx_id)?;

        // Track metrics
        if let Some(db) = &self.db {
            db.metrics_ref().transaction_aborted();
        }

        Ok(())
    }

    pub fn get_snapshot_root(&self, collection: &str) -> Option<PageNum> {
        // Check updated_roots first (for writes in this transaction)
        // Then fall back to snapshot_roots (original snapshot)
        let updated_roots = self.updated_roots.read()
            .recover_poison();
        if let Some(&root) = updated_roots.get(collection) {
            return Some(root);
        }
        self.snapshot_roots.get(collection).copied()
    }

    pub(crate) fn track_doc_existed_in_snapshot(&self, collection: &str, doc_id: &str, existed: bool) {
        let mut doc_existed = self.doc_existed_in_snapshot.write()
            .expect("transaction.doc_existed_in_snapshot lock poisoned");
        let collection_map = doc_existed
            .entry(collection.to_string())
            .or_insert_with(HashMap::new);

        // Only track if not already tracked - preserve the original snapshot state
        // This ensures that if we insert then update in the same transaction,
        // we remember it didn't exist in the original snapshot
        collection_map.entry(doc_id.to_string()).or_insert(existed);
    }

    pub(crate) fn track_doc_original_xmin(&self, collection: &str, doc_id: &str, xmin: TransactionID) {
        let mut doc_xmin = self.doc_original_xmin.write()
            .recover_poison();
        let collection_map = doc_xmin
            .entry(collection.to_string())
            .or_insert_with(HashMap::new);

        // Only track the FIRST xmin we see (when we first read the document)
        // This is the version we based our changes on
        collection_map.entry(doc_id.to_string()).or_insert(xmin);
    }

    fn validate_unique_constraints_for_collection(&self, collection_name: &str) -> Result<()> {
        let db = match &self.db {
            Some(db) => db,
            None => return Ok(()),
        };

        let doc_writes = self.doc_writes.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_writes".to_string() })?;
        let doc_existed = self.doc_existed_in_snapshot.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_existed_in_snapshot".to_string() })?;

        let docs = match doc_writes.get(collection_name) {
            Some(docs) => docs,
            None => return Ok(()),
        };

        let coll_name = collection_name;

        let metadata = db.get_metadata();
        let coll_meta = match metadata.collections.get(coll_name) {
            Some(meta) => meta,
            None => return Ok(()),
        };

        for (index_name, index_meta) in &coll_meta.indexes {
            if !index_meta.unique {
                continue;
            }

            use crate::core::btree::BTree;
            let index_btree = if index_meta.btree_root == 0 {
                continue;
            } else {
                BTree::open(self.pager.clone(), index_meta.btree_root)
            };

            for (doc_id, page_num) in docs {
                let existed = doc_existed
                    .get(coll_name)
                    .and_then(|m| m.get(doc_id))
                    .copied()
                    .unwrap_or(false);

                if !existed {
                    use crate::core::document::read_versioned_document;
                    let tx_writes_arc = self.get_writes_arc();
                    let tx_writes = tx_writes_arc.read()
                        .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
                    let vdoc = read_versioned_document(&self.pager, *page_num, &*tx_writes)?;

                    let doc_map: serde_json::Map<String, serde_json::Value> =
                        serde_json::from_slice(&vdoc.data)?;

                    use crate::core::index_key::extract_field_values;

                    let index_fields = index_meta.get_fields();
                    let prefix = if index_fields.len() == 1 {
                        // Single-field index
                        let field_value = extract_field_values(&doc_map, &index_fields)[0].clone();
                        let value_json = serde_json::to_string(&field_value)?;
                        format!("{}|", value_json)
                    } else {
                        // Compound index
                        let field_values = extract_field_values(&doc_map, &index_fields);
                        let values_json: Vec<String> = field_values.iter()
                            .map(|v| serde_json::to_string(v).unwrap_or_default())
                            .collect();
                        format!("{}|", values_json.join("|"))
                    };

                    let mut has_conflict = false;
                    let mut iter = index_btree.iterator()?;
                    while iter.next() {
                        let (key, _value) = iter.entry();
                        if key.starts_with(&prefix) {
                            has_conflict = true;
                            break;
                        }
                    }

                    if has_conflict {
                        return Err(Error::Other(format!(
                            "unique constraint violation on index '{}' for collection '{}': \
                            value was inserted by another transaction",
                            index_name, coll_name
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    // ===== BATCH COMMIT METHODS =====

    fn prepare_write_request(&mut self) -> Result<crate::core::database::PendingWrite> {
        use std::time::Instant;

        // Snapshot all data needed for commit
        let writes = {
            let w = self.writes.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "transaction.writes".to_string() })?;
            w.iter().map(|(&k, v)| (k, v.clone())).collect()
        };

        let doc_writes = self.doc_writes.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_writes".to_string() })?.clone();
        let updated_roots = self.updated_roots.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.updated_roots".to_string() })?.clone();
        let old_versions = self.old_versions.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.old_versions".to_string() })?.clone();
        let modified_collections = self.modified_collections.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.modified_collections".to_string() })?.clone();
        let doc_existed_in_snapshot = self.doc_existed_in_snapshot.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_existed_in_snapshot".to_string() })?.clone();

        let doc_original_xmin = self.doc_original_xmin.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "transaction.doc_original_xmin".to_string() })?.clone();

        Ok(crate::core::database::PendingWrite {
            writes,
            doc_writes,
            snapshot_roots: self.snapshot_roots.clone(),
            updated_roots,
            old_versions,
            modified_collections,
            doc_existed_in_snapshot,
            doc_original_xmin,
            _tx_id: self.tx_id,
            snapshot_id: self.snapshot_id,
            mvcc_tx_id: self.mvcc_tx_id,
            completion: Arc::new((Mutex::new(None), std::sync::Condvar::new())),
            _submitted_at: Instant::now(),
        })
    }

    fn collect_batch(&self) -> Result<Vec<crate::core::database::PendingWrite>> {
        use std::time::{Duration, Instant};
        use std::collections::HashSet;

        let db = self.db.as_ref().ok_or_else(|| Error::Other("database reference not set".into()))?;
        let config = &db.batch_config;
        let mut batch = Vec::new();

        // Track which documents are already in this batch
        let mut batch_documents: HashSet<(String, String)> = HashSet::new();

        let start = Instant::now();

        loop {
            // Pop next pending write from queue
            let pending = {
                let mut queue = db.pending_writes.lock()
                    .map_err(|_| Error::LockPoisoned { lock_name: "database.pending_writes".to_string() })?;
                queue.pop_front()
            };

            match pending {
                Some(p) => {
                    // Check if this TX modifies any document already in the batch
                    let mut has_conflict = false;
                    for (collection, docs) in &p.doc_writes {
                        for doc_id in docs.keys() {
                            let key = (collection.clone(), doc_id.clone());
                            if batch_documents.contains(&key) {
                                has_conflict = true;
                                break;
                            }
                        }
                        if has_conflict {
                            break;
                        }
                    }

                    if has_conflict {
                        // Put this TX back at the front of the queue for next batch
                        let mut queue = db.pending_writes.lock()
                            .map_err(|_| Error::LockPoisoned { lock_name: "database.pending_writes".to_string() })?;
                        queue.push_front(p);
                        // Stop collecting - we'll process current batch first
                        break;
                    }

                    // No conflict - add this TX to the batch and track its documents
                    for (collection, docs) in &p.doc_writes {
                        for doc_id in docs.keys() {
                            batch_documents.insert((collection.clone(), doc_id.clone()));
                        }
                    }

                    batch.push(p);

                    // Stop if we hit max batch size
                    if batch.len() >= config.max_batch_size {
                        break;
                    }
                }
                None => {
                    // Queue empty - should we wait for more?
                    if batch.is_empty() {
                        // This shouldn't happen (we submitted ours)
                        return Err(Error::Other("batch collection failed: empty batch".into()));
                    }

                    // If we have at least one TX and timeout not reached, wait briefly
                    if start.elapsed().as_micros() < config.collect_timeout_micros as u128 {
                        // Small sleep to allow more TXs to queue up
                        std::thread::sleep(Duration::from_micros(10));
                        continue;
                    } else {
                        // Timeout reached, commit what we have
                        break;
                    }
                }
            }
        }

        Ok(batch)
    }

    fn check_batch_conflicts(&self, batch: &[crate::core::database::PendingWrite]) -> Result<()> {
        let mut seen_writes = HashMap::new();

        for (idx, pending) in batch.iter().enumerate() {
            for (collection, docs) in &pending.doc_writes {
                for doc_id in docs.keys() {
                    let key = (collection.clone(), doc_id.clone());

                    if let Some(first_idx) = seen_writes.get(&key) {
                        // Conflict! Two TXs in batch modify same doc
                        return Err(Error::Other(format!(
                            "Batch conflict: TX {} and TX {} both modify {}/{}",
                            first_idx, idx, collection, doc_id
                        )));
                    }

                    seen_writes.insert(key, idx);
                }
            }
        }

        Ok(())
    }

    fn validate_pending_write(&self, pending: &crate::core::database::PendingWrite) -> Result<bool> {
        let db = self.db.as_ref().ok_or_else(|| Error::Other("database reference not set".into()))?;
        let mut needs_rebase = false;

        for collection_name in pending.modified_collections.iter() {
            let current_metadata = db.get_metadata();
            let current_root = current_metadata.collections
                .get(collection_name)
                .map(|c| c.btree_root)
                .unwrap_or(0);

            let snapshot_root = pending.snapshot_roots.get(collection_name).copied().unwrap_or(0);

            // If collection root changed, we need to check for document-level conflicts
            if current_root != snapshot_root {
                needs_rebase = true;

                // Document-level conflict detection (not collection-level!)
                // Only reject if there's an actual document conflict
                let has_doc_conflict = self.detect_document_level_conflicts(
                    collection_name,
                    current_root,
                    pending.snapshot_id,
                    &pending.doc_writes,
                    &pending.doc_existed_in_snapshot,
                    &pending.doc_original_xmin,
                )?;

                if has_doc_conflict {
                    return Err(Error::TxConflict); // Real document conflict - reject
                }

                // No document conflicts - rebase will be needed
            } else {
                // Root hasn't changed - still check for MVCC conflicts on documents
                self.detect_write_conflicts_with_context(
                    collection_name,
                    current_root,
                    pending.snapshot_id,
                    &pending.doc_writes,
                    &pending.doc_existed_in_snapshot,
                    &pending.doc_original_xmin,
                )?;
            }
        }

        Ok(needs_rebase)
    }

    fn detect_document_level_conflicts(
        &self,
        collection_name: &str,
        current_root: PageNum,
        snapshot_id: TransactionID,
        doc_writes: &HashMap<String, HashMap<String, PageNum>>,
        doc_existed_map: &HashMap<String, HashMap<String, bool>>,
        doc_original_xmin: &HashMap<String, HashMap<String, TransactionID>>,
    ) -> Result<bool> {
        use crate::core::tx_btree::TxBTree;
        use crate::core::document::read_versioned_document;

        // Get this collection's document writes
        let collection_writes = match doc_writes.get(collection_name) {
            Some(writes) if !writes.is_empty() => writes,
            _ => return Ok(false), // No writes, no conflicts
        };

        let collection_xmins = doc_original_xmin.get(collection_name);

        // Build B-tree from CURRENT root to check committed state
        let empty_writes = Arc::new(RwLock::new(HashMap::new()));
        let current_btree = TxBTree::new(self.pager.clone(), current_root, empty_writes);

        // Check each document we modified
        for (doc_id, _) in collection_writes.iter() {
            // Check if this document existed in our snapshot
            let existed_in_snapshot = doc_existed_map
                .get(collection_name)
                .and_then(|docs| docs.get(doc_id).copied())
                .unwrap_or(false);

            if !existed_in_snapshot {
                // New insert - check if someone else inserted it
                match current_btree.search(doc_id) {
                    Ok(committed_page) => {
                        // Document exists in current state but not in our snapshot
                        // Someone else inserted it - CONFLICT!
                        let empty_map = HashMap::new();
                        match read_versioned_document(&self.pager, committed_page, &empty_map) {
                            Ok(committed_vdoc) => {
                                if committed_vdoc.xmin > snapshot_id {
                                    return Ok(true); // Conflict detected
                                }
                            }
                            Err(_) => return Ok(true), // Error reading = conflict
                        }
                    }
                    Err(_) => {
                        // Document doesn't exist in current state - no conflict
                        continue;
                    }
                }
            } else {
                // Document existed - check if it was modified
                let original_xmin = collection_xmins
                    .and_then(|xmins| xmins.get(doc_id).copied());

                if let Some(orig_xmin) = original_xmin {
                    match current_btree.search(doc_id) {
                        Ok(committed_page) => {
                            let empty_map = HashMap::new();
                            match read_versioned_document(&self.pager, committed_page, &empty_map) {
                                Ok(committed_vdoc) => {
                                    // Check if document was modified after our snapshot
                                    if committed_vdoc.xmin != orig_xmin
                                       && committed_vdoc.xmin > snapshot_id {
                                        return Ok(true); // Conflict detected
                                    }
                                }
                                Err(_) => return Ok(true), // Error = conflict
                            }
                        }
                        Err(_) => {
                            // Document was deleted - CONFLICT
                            return Ok(true);
                        }
                    }
                }
            }
        }

        Ok(false) // No document conflicts
    }

    #[allow(dead_code)]
    fn rebase_pending_write(
        &self,
        pending: &mut crate::core::database::PendingWrite,
        db: &Arc<crate::core::database::Database>,
    ) -> Result<()> {
        use crate::core::tx_btree::TxBTree;

        // Rebase each modified collection
        for collection_name in pending.modified_collections.iter() {
            let current_metadata = db.get_metadata();
            let current_root = current_metadata.collections
                .get(collection_name)
                .map(|c| c.btree_root)
                .unwrap_or(0);

            let snapshot_root = pending.snapshot_roots.get(collection_name).copied().unwrap_or(0);

            if current_root == snapshot_root {
                continue; // No rebase needed for this collection
            }

            // Build rebased B-tree starting from CURRENT root (not snapshot root)
            // This includes all changes from other transactions that committed after our snapshot
            let pending_writes_arc = Arc::new(RwLock::new(pending.writes.clone()));
            let mut rebased_btree = TxBTree::new(
                self.pager.clone(),
                current_root,  // ← Start from CURRENT, not snapshot!
                pending_writes_arc.clone()
            );

            // Re-insert all our documents into the rebased tree
            let doc_writes_for_collection = pending.doc_writes
                .get(collection_name)
                .cloned()
                .unwrap_or_default();

            for (doc_id, page_num) in &doc_writes_for_collection {
                rebased_btree.insert(doc_id, *page_num)?;
            }

            // Get the new root after rebasing
            let new_root = rebased_btree.get_current_root();

            // The rebased B-tree has written its structural pages to pending_writes_arc
            // Merge them back into pending.writes
            {
                let rebased_writes = pending_writes_arc.read()
                    .map_err(|_| Error::LockPoisoned { lock_name: "pending_writes_arc".to_string() })?;
                for (page_num, page_data) in rebased_writes.iter() {
                    pending.writes.insert(*page_num, page_data.clone());
                }
            }

            // Update the collection root to the rebased root
            pending.updated_roots.insert(collection_name.clone(), new_root);
        }

        Ok(())
    }

    fn rebase_single_collection(
        &self,
        pending: &mut crate::core::database::PendingWrite,
        collection_name: &str,
        current_root: PageNum,
        _db: &Arc<crate::core::database::Database>,
    ) -> Result<()> {
        use crate::core::tx_btree::TxBTree;

        // Build rebased B-tree starting from CURRENT root
        let pending_writes_arc = Arc::new(RwLock::new(pending.writes.clone()));
        let mut rebased_btree = TxBTree::new(
            self.pager.clone(),
            current_root,
            pending_writes_arc.clone()
        );

        // Re-apply all document operations for this collection (inserts/updates/deletes)
        let doc_writes_for_collection = pending.doc_writes
            .get(collection_name)
            .cloned()
            .unwrap_or_default();

        // First: Re-insert documents (PageNum != MAX)
        // Then: Re-delete documents (PageNum == MAX)
        // This order ensures deletes are applied after inserts
        for (doc_id, page_num) in &doc_writes_for_collection {
            if *page_num != PageNum::MAX {
                rebased_btree.insert(doc_id, *page_num)?;
            }
        }

        for (doc_id, page_num) in &doc_writes_for_collection {
            if *page_num == PageNum::MAX {
                // This is a delete operation - re-apply it
                rebased_btree.delete(doc_id)?;
            }
        }

        // Get the new root after rebasing
        let new_root = rebased_btree.get_current_root();

        // Merge rebased writes back into pending
        let mut new_pages = Vec::new();
        {
            let rebased_writes = pending_writes_arc.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "pending_writes_arc".to_string() })?;
            for (page_num, page_data) in rebased_writes.iter() {
                if !pending.writes.contains_key(page_num) {
                    new_pages.push(*page_num);
                }
                pending.writes.insert(*page_num, page_data.clone());
            }
        }

        // Update the collection root
        pending.updated_roots.insert(collection_name.to_string(), new_root);

        Ok(())
    }

    fn execute_batch_commit(&self, batch: &mut [crate::core::database::PendingWrite]) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| Error::Other("database reference not set".into()))?;

        // PHASE 1: Validate all transactions for conflicts (no pre-rebase needed)
        for pending in batch.iter_mut() {
            let _needs_rebase = self.validate_pending_write(pending)?;
            // Rebase will happen sequentially in PHASE 4
        }

        // PHASE 2: Check for intra-batch conflicts (after rebase)
        self.check_batch_conflicts(batch)?;

        // PHASE 3: Write all frames to WAL (buffered)
        for pending in batch.iter() {
            for (page_num, page_data) in &pending.writes {
                self.wal.write_frame(pending.mvcc_tx_id, *page_num, page_data.clone())?;
            }
        }

        // PHASE 4: Apply transactions sequentially within batch
        // Each TX rebases onto the previous TX's root to avoid metadata overwriting
        {
            for (_idx, pending) in batch.iter_mut().enumerate() {
                // Collect collection names first to avoid borrow issues
                let collections: Vec<String> = pending.modified_collections.iter().cloned().collect();

                for collection_name in collections {
                    // Get current root (updated by previous TXs in this batch)
                    let current_metadata = db.get_metadata();
                    let current_root = current_metadata.collections
                        .get(&collection_name)
                        .map(|c| c.btree_root)
                        .unwrap_or(0);

                    // If root changed since this TX's snapshot, rebase
                    let snapshot_root = pending.snapshot_roots.get(&collection_name).copied().unwrap_or(0);

                    if current_root != snapshot_root {
                        // Track which pages exist before rebase
                        let pages_before_rebase: std::collections::HashSet<PageNum> =
                            pending.writes.keys().copied().collect();

                        // Rebase just this one transaction
                        self.rebase_single_collection(pending, &collection_name, current_root, &db)?;

                        for (page_num, page_data) in &pending.writes {
                            if !pages_before_rebase.contains(page_num) {
                                self.wal.write_frame(pending.mvcc_tx_id, *page_num, page_data.clone())?;
                            }
                        }
                    }

                    let new_root = pending.updated_roots.get(&collection_name).copied().unwrap_or(current_root);

                    let should_update = new_root != current_root;

                    if should_update {
                        db.update_metadata_no_flush(|m| {
                            let coll = m.get_collection(&collection_name);
                            coll.btree_root = new_root;
                        });
                    }
                }
            }

            let metadata = db.get_metadata();
            let mut meta_data = metadata.serialize()?;
            if meta_data.len() < PAGE_SIZE {
                meta_data.resize(PAGE_SIZE, 0);
            }

            let meta_page = self.pager.metadata_page()?;
            let meta_page = if meta_page == 0 {
                self.pager.alloc_page()?
            } else {
                meta_page
            };

            let max_tx_id = batch.iter().map(|p| p.mvcc_tx_id).max().unwrap_or(self.mvcc_tx_id);

            self.wal.write_frame(max_tx_id, meta_page, meta_data.clone())?;
            self.pager.write_page_transfer(meta_page, meta_data)?;

            let mut header_data = self.pager.get_header_data()?;
            header_data[24..32].copy_from_slice(&meta_page.to_le_bytes());
            self.wal.write_frame(max_tx_id, 0, header_data)?;

            self.pager.set_metadata_page(meta_page)?;
            self.pager.set_next_transaction_id(max_tx_id + 1)?;

            self.pager.write_header_no_sync()?;
        }

        self.wal.sync()?;

        for pending in batch.iter() {
            for (page_num, page_data) in &pending.writes {
                self.pager.write_page_transfer(*page_num, page_data.clone())?;
            }
        }

        self.pager.flush_no_sync()?;

        self.pager.sync_data_only()?;

        for pending in batch.iter() {
            self.tx_manager.commit_transaction(pending.mvcc_tx_id)?;
        }

        for pending in batch.iter() {
            if !pending.old_versions.is_empty() {
                let mut version_chains = db.version_chains.write()
                    .map_err(|_| Error::LockPoisoned { lock_name: "database.version_chains".to_string() })?;

                for (coll_name, doc_versions) in pending.old_versions.iter() {
                    let coll_chains = version_chains.entry(coll_name.clone())
                        .or_insert_with(HashMap::new);

                    for (doc_id, old_version) in doc_versions.iter() {
                        let chain = coll_chains.entry(doc_id.clone())
                            .or_insert_with(|| crate::core::mvcc::VersionChain::new(doc_id.clone()));

                        let mut version_with_xmax = old_version.clone();
                        version_with_xmax.xmax = pending.mvcc_tx_id;
                        chain.add_version(version_with_xmax)?;
                    }
                }
            }
        }

        let watchers = db.get_watchers();
        for pending in batch.iter() {
            for (collection_name, docs) in pending.doc_writes.iter() {
                for (doc_id, page_num) in docs.iter() {
                    let is_insert = pending.doc_existed_in_snapshot
                        .get(collection_name)
                        .and_then(|coll_docs| coll_docs.get(doc_id))
                        .map(|existed| !existed)
                        .unwrap_or(true);

                    let operation = if is_insert {
                        ChangeOperation::Insert
                    } else {
                        ChangeOperation::Update
                    };

                    // Read the document data from pending writes
                    let document = pending.writes.get(page_num)
                        .and_then(|_page_data| {
                            read_versioned_document(&self.pager, *page_num, &pending.writes)
                                .ok()
                                .and_then(|vdoc| serde_json::from_slice(&vdoc.data).ok())
                        });

                    emit_change(&watchers, collection_name, operation, doc_id, document);
                }
            }
        }

        Ok(())
    }

}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.state == TxState::Active {
            if let Ok(mut writes) = self.writes.write() {
                writes.clear();
            }
            if let Ok(mut doc_writes) = self.doc_writes.write() {
                doc_writes.clear();
            }

            let _ = self.tx_manager.abort_transaction(self.mvcc_tx_id);

            self.state = TxState::RolledBack;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_transaction_new() {
        let path = "/tmp/test_tx_new.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 100, 0o644, false).unwrap());
        let wal = Arc::new(WAL::open(path, 0o644).unwrap());
        let tx_manager = Arc::new(TransactionManager::new());

        let tx = Transaction::new(
            pager.clone(),
            wal.clone(),
            tx_manager.clone(),
            HashMap::new(),
            Arc::new(Mutex::new(())),
            None,
        ).unwrap();

        assert_eq!(tx.state, TxState::Active);
        assert!(tx.is_active());

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_transaction_write_commit() {
        let path = "/tmp/test_tx_write_commit.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 100, 0o644, false).unwrap());
        let wal = Arc::new(WAL::open(path, 0o644).unwrap());
        let tx_manager = Arc::new(TransactionManager::new());

        let mut tx = Transaction::new(
            pager.clone(),
            wal.clone(),
            tx_manager.clone(),
            HashMap::new(),
            Arc::new(Mutex::new(())),
            None,
        ).unwrap();

        let page_num = pager.alloc_page().unwrap();
        let data = vec![42u8; PAGE_SIZE];
        tx.write_page(page_num, data.clone()).unwrap();

        tx.commit().unwrap();

        assert_eq!(tx.state, TxState::Committed);
        assert!(!tx.is_active());

        let read_data = pager.read_page(page_num).unwrap();
        assert_eq!(read_data, data);

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_transaction_rollback() {
        let path = "/tmp/test_tx_rollback.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 100, 0o644, false).unwrap());
        let wal = Arc::new(WAL::open(path, 0o644).unwrap());
        let tx_manager = Arc::new(TransactionManager::new());

        let mut tx = Transaction::new(
            pager.clone(),
            wal.clone(),
            tx_manager.clone(),
            HashMap::new(),
            Arc::new(Mutex::new(())),
            None,
        ).unwrap();

        let page_num = pager.alloc_page().unwrap();
        let data = vec![42u8; PAGE_SIZE];
        tx.write_page(page_num, data.clone()).unwrap();

        tx.rollback().unwrap();

        assert_eq!(tx.state, TxState::RolledBack);

        let writes_arc = tx.get_writes_arc();
        let writes = writes_arc.read().recover_poison();
        assert!(writes.is_empty());

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_transaction_cannot_commit_twice() {
        let path = "/tmp/test_tx_double_commit.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 100, 0o644, false).unwrap());
        let wal = Arc::new(WAL::open(path, 0o644).unwrap());
        let tx_manager = Arc::new(TransactionManager::new());

        let mut tx = Transaction::new(
            pager.clone(),
            wal.clone(),
            tx_manager.clone(),
            HashMap::new(),
            Arc::new(Mutex::new(())),
            None,
        ).unwrap();

        tx.commit().unwrap();

        let result = tx.commit();
        assert!(matches!(result, Err(Error::TxAlreadyDone)));

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_transaction_mvcc_integration() {
        let path = "/tmp/test_tx_mvcc.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 100, 0o644, false).unwrap());
        let wal = Arc::new(WAL::open(path, 0o644).unwrap());
        let tx_manager = Arc::new(TransactionManager::new());

        let commit_mu = Arc::new(Mutex::new(()));

        let mut tx1 = Transaction::new(
            pager.clone(),
            wal.clone(),
            tx_manager.clone(),
            HashMap::new(),
            commit_mu.clone(),
            None,
        ).unwrap();

        let mut tx2 = Transaction::new(
            pager.clone(),
            wal.clone(),
            tx_manager.clone(),
            HashMap::new(),
            commit_mu.clone(),
            None,
        ).unwrap();

        assert_ne!(tx1.mvcc_tx_id, tx2.mvcc_tx_id);

        tx1.commit().unwrap();

        assert!(tx2.is_active());

        tx2.commit().unwrap();

        assert_eq!(tx1.state, TxState::Committed);
        assert_eq!(tx2.state, TxState::Committed);

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_transaction_write_after_commit_fails() {
        let path = "/tmp/test_tx_write_after_commit.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 100, 0o644, false).unwrap());
        let wal = Arc::new(WAL::open(path, 0o644).unwrap());
        let tx_manager = Arc::new(TransactionManager::new());

        let mut tx = Transaction::new(
            pager.clone(),
            wal.clone(),
            tx_manager.clone(),
            HashMap::new(),
            Arc::new(Mutex::new(())),
            None,
        ).unwrap();

        tx.commit().unwrap();

        let page_num = pager.alloc_page().unwrap();
        let data = vec![42u8; PAGE_SIZE];
        let result = tx.write_page(page_num, data);

        assert!(matches!(result, Err(Error::TxNotActive)));

        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path));
    }
}
