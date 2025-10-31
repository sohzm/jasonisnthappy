
use crate::core::constants::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub id: TransactionID,
    pub start_time: TransactionID,
    pub status: TxStatus,
}

pub struct TransactionManager {
    next_tx_id: Arc<AtomicU64>,
    last_committed_tx_id: Arc<AtomicU64>,
    active_txs: Arc<RwLock<HashMap<TransactionID, TransactionInfo>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_tx_id: Arc::new(AtomicU64::new(1)),
            last_committed_tx_id: Arc::new(AtomicU64::new(0)),
            active_txs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn initialize_from_pager(&self, current_tx_id: TransactionID) {
        self.next_tx_id.store(current_tx_id, Ordering::SeqCst);

        if current_tx_id > 0 {
            self.last_committed_tx_id.store(current_tx_id - 1, Ordering::SeqCst);
        }
    }

    pub fn begin_transaction(&self) -> crate::core::errors::Result<TransactionID> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);

        let snapshot_time = self.get_latest_committed_tx_id();

        let mut active_txs = self.active_txs.write()
            .map_err(|_| crate::core::errors::Error::LockPoisoned { lock_name: "mvcc.active_txs".to_string() })?;
        active_txs.insert(tx_id, TransactionInfo {
            id: tx_id,
            start_time: snapshot_time,
            status: TxStatus::Active,
        });

        Ok(tx_id)
    }

    pub fn register_transaction(&self, tx_id: TransactionID) -> crate::core::errors::Result<()> {
        let snapshot_time = self.get_latest_committed_tx_id();

        let mut active_txs = self.active_txs.write()
            .map_err(|_| crate::core::errors::Error::LockPoisoned { lock_name: "mvcc.active_txs".to_string() })?;
        active_txs.insert(tx_id, TransactionInfo {
            id: tx_id,
            start_time: snapshot_time,
            status: TxStatus::Active,
        });
        Ok(())
    }

    pub fn commit_transaction(&self, tx_id: TransactionID) -> crate::core::errors::Result<()> {
        let mut active_txs = self.active_txs.write()
            .map_err(|_| crate::core::errors::Error::LockPoisoned { lock_name: "mvcc.active_txs".to_string() })?;

        if let Some(info) = active_txs.get_mut(&tx_id) {
            info.status = TxStatus::Committed;
        }
        active_txs.remove(&tx_id);

        let current_last = self.last_committed_tx_id.load(Ordering::SeqCst);
        if tx_id > current_last {
            self.last_committed_tx_id.store(tx_id, Ordering::SeqCst);
        }
        Ok(())
    }

    pub fn abort_transaction(&self, tx_id: TransactionID) -> crate::core::errors::Result<()> {
        let mut active_txs = self.active_txs.write()
            .map_err(|_| crate::core::errors::Error::LockPoisoned { lock_name: "mvcc.active_txs".to_string() })?;

        if let Some(info) = active_txs.get_mut(&tx_id) {
            info.status = TxStatus::Aborted;
        }
        active_txs.remove(&tx_id);
        Ok(())
    }

    pub fn get_latest_committed_tx_id(&self) -> TransactionID {
        self.last_committed_tx_id.load(Ordering::SeqCst)
    }

    pub fn get_oldest_active_transaction(&self) -> crate::core::errors::Result<TransactionID> {
        let active_txs = self.active_txs.read()
            .map_err(|_| crate::core::errors::Error::LockPoisoned { lock_name: "mvcc.active_txs".to_string() })?;

        if active_txs.is_empty() {
            return Ok(self.get_latest_committed_tx_id() + 1);
        }

        let mut oldest = TransactionID::MAX;
        for info in active_txs.values() {
            if info.start_time < oldest {
                oldest = info.start_time;
            }
        }

        Ok(oldest)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct DocumentVersion {
    pub doc_id: String,
    pub xmin: TransactionID,
    pub xmax: TransactionID,
    pub data: Vec<u8>,
    pub page_num: PageNum,
}

impl DocumentVersion {
    pub fn is_visible(&self, tx_id: TransactionID) -> bool {
        if self.xmin > tx_id {
            return false;
        }

        if self.xmax != 0 && self.xmax <= tx_id {
            return false;
        }

        true
    }
}

pub struct VersionChain {
    pub doc_id: String,
    versions: Arc<RwLock<Vec<DocumentVersion>>>,
}

impl VersionChain {
    pub fn new(doc_id: String) -> Self {
        Self {
            doc_id,
            versions: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn add_version(&self, version: DocumentVersion) -> crate::core::errors::Result<()> {
        let mut versions = self.versions.write()
            .map_err(|_| crate::core::errors::Error::LockPoisoned { lock_name: "version_chain.versions".to_string() })?;
        versions.push(version);
        Ok(())
    }

    pub fn garbage_collect(&self, oldest_active_tx: TransactionID) -> crate::core::errors::Result<Vec<DocumentVersion>> {
        let mut versions = self.versions.write()
            .map_err(|_| crate::core::errors::Error::LockPoisoned { lock_name: "version_chain.versions".to_string() })?;

        let mut removed = Vec::new();
        let mut kept = Vec::new();

        for version in versions.drain(..) {
            if version.xmin >= oldest_active_tx ||
               (version.xmax == 0 || version.xmax >= oldest_active_tx) {
                kept.push(version);
            } else {
                removed.push(version);
            }
        }

        *versions = kept;
        Ok(removed)
    }

    pub fn get_versions(&self) -> crate::core::errors::Result<Vec<DocumentVersion>> {
        let versions = self.versions.read()
            .map_err(|_| crate::core::errors::Error::LockPoisoned { lock_name: "version_chain.versions".to_string() })?;
        Ok(versions.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_manager_new() {
        let tm = TransactionManager::new();
        assert_eq!(tm.get_latest_committed_tx_id(), 0);
    }

    #[test]
    fn test_begin_commit_transaction() {
        let tm = TransactionManager::new();

        let tx1 = tm.begin_transaction().unwrap();
        assert_eq!(tx1, 1);

        let tx2 = tm.begin_transaction().unwrap();
        assert_eq!(tx2, 2);

        tm.commit_transaction(tx1).unwrap();
        assert_eq!(tm.get_latest_committed_tx_id(), 1);

        tm.commit_transaction(tx2).unwrap();
        assert_eq!(tm.get_latest_committed_tx_id(), 2);
    }

    #[test]
    fn test_abort_transaction() {
        let tm = TransactionManager::new();

        let tx1 = tm.begin_transaction().unwrap();
        let _ = tm.abort_transaction(tx1);

        assert_eq!(tm.get_latest_committed_tx_id(), 0);
    }

    #[test]
    fn test_oldest_active_transaction() {
        let tm = TransactionManager::new();

        assert_eq!(tm.get_oldest_active_transaction().unwrap(), 1);

        let tx1 = tm.begin_transaction().unwrap();
        let tx2 = tm.begin_transaction().unwrap();

        let oldest = tm.get_oldest_active_transaction().unwrap();
        assert!(oldest <= tx1);

        let _ = tm.commit_transaction(tx1);
        tm.commit_transaction(tx2).unwrap();

        assert_eq!(tm.get_oldest_active_transaction().unwrap(), tm.get_latest_committed_tx_id() + 1);
    }

    #[test]
    fn test_document_version_visibility() {
        let version = DocumentVersion {
            doc_id: "doc1".to_string(),
            xmin: 5,
            xmax: 0,
            data: vec![1, 2, 3],
            page_num: 100,
        };

        assert!(!version.is_visible(4));

        assert!(version.is_visible(5));
        assert!(version.is_visible(10));

        let deleted_version = DocumentVersion {
            doc_id: "doc1".to_string(),
            xmin: 5,
            xmax: 10,
            data: vec![1, 2, 3],
            page_num: 100,
        };

        assert!(deleted_version.is_visible(5));
        assert!(deleted_version.is_visible(9));

        assert!(!deleted_version.is_visible(10));
        assert!(!deleted_version.is_visible(15));
    }

    #[test]
    fn test_version_chain() {
        let chain = VersionChain::new("doc1".to_string());

        let _ = chain.add_version(DocumentVersion {
            doc_id: "doc1".to_string(),
            xmin: 1,
            xmax: 5,
            data: vec![1],
            page_num: 100,
        });

        let _ = chain.add_version(DocumentVersion {
            doc_id: "doc1".to_string(),
            xmin: 5,
            xmax: 0,
            data: vec![2],
            page_num: 101,
        });

        let versions = chain.get_versions().unwrap();
        assert_eq!(versions.len(), 2);

        let removed = chain.garbage_collect(10).unwrap();

        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].xmin, 1);

        let versions = chain.get_versions().unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].xmin, 5);
    }

    #[test]
    fn test_initialize_from_pager() {
        let tm = TransactionManager::new();

        tm.initialize_from_pager(100);

        assert_eq!(tm.next_tx_id.load(Ordering::SeqCst), 100);
        assert_eq!(tm.get_latest_committed_tx_id(), 99);
    }
}
