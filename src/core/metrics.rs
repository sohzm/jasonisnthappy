use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Metrics collected during database operations.
/// All fields use atomic types for lock-free updates with minimal overhead.
#[derive(Debug)]
pub struct Metrics {
    // Transaction metrics
    transactions_begun: AtomicU64,
    transactions_committed: AtomicU64,
    transactions_aborted: AtomicU64,
    active_transactions: AtomicUsize,

    // Batch commit metrics
    batches_committed: AtomicU64,
    total_batched_txs: AtomicU64,
    max_batch_size: AtomicUsize,
    total_batch_time_micros: AtomicU64,

    // Storage metrics
    pages_allocated: AtomicU64,
    pages_freed: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    dirty_pages: AtomicUsize,

    // WAL metrics
    wal_writes: AtomicU64,
    wal_bytes_written: AtomicU64,
    checkpoints: AtomicU64,

    // Operation metrics
    documents_inserted: AtomicU64,
    documents_updated: AtomicU64,
    documents_deleted: AtomicU64,
    documents_read: AtomicU64,

    // Error metrics
    io_errors: AtomicU64,
    transaction_conflicts: AtomicU64,
}

/// Snapshot of metrics at a point in time.
/// Includes computed values like cache hit rate.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    // Transaction metrics
    pub transactions_begun: u64,
    pub transactions_committed: u64,
    pub transactions_aborted: u64,
    pub active_transactions: usize,
    pub total_transactions: u64,
    pub commit_rate: f64,

    // Batch commit metrics
    pub batches_committed: u64,
    pub total_batched_txs: u64,
    pub max_batch_size: usize,
    pub avg_batch_size: f64,
    pub avg_batch_time_micros: f64,

    // Storage metrics
    pub pages_allocated: u64,
    pub pages_freed: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_total_requests: u64,
    pub cache_hit_rate: f64,
    pub dirty_pages: usize,

    // WAL metrics
    pub wal_writes: u64,
    pub wal_bytes_written: u64,
    pub checkpoints: u64,

    // Operation metrics
    pub documents_inserted: u64,
    pub documents_updated: u64,
    pub documents_deleted: u64,
    pub documents_read: u64,
    pub total_document_operations: u64,

    // Error metrics
    pub io_errors: u64,
    pub transaction_conflicts: u64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            transactions_begun: AtomicU64::new(0),
            transactions_committed: AtomicU64::new(0),
            transactions_aborted: AtomicU64::new(0),
            active_transactions: AtomicUsize::new(0),

            batches_committed: AtomicU64::new(0),
            total_batched_txs: AtomicU64::new(0),
            max_batch_size: AtomicUsize::new(0),
            total_batch_time_micros: AtomicU64::new(0),

            pages_allocated: AtomicU64::new(0),
            pages_freed: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            dirty_pages: AtomicUsize::new(0),

            wal_writes: AtomicU64::new(0),
            wal_bytes_written: AtomicU64::new(0),
            checkpoints: AtomicU64::new(0),

            documents_inserted: AtomicU64::new(0),
            documents_updated: AtomicU64::new(0),
            documents_deleted: AtomicU64::new(0),
            documents_read: AtomicU64::new(0),

            io_errors: AtomicU64::new(0),
            transaction_conflicts: AtomicU64::new(0),
        }
    }

    // Transaction metrics
    #[inline]
    pub fn transaction_begun(&self) {
        self.transactions_begun.fetch_add(1, Ordering::Relaxed);
        self.active_transactions.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn transaction_committed(&self) {
        self.transactions_committed.fetch_add(1, Ordering::Relaxed);
        self.active_transactions.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn transaction_aborted(&self) {
        self.transactions_aborted.fetch_add(1, Ordering::Relaxed);
        self.active_transactions.fetch_sub(1, Ordering::Relaxed);
    }

    // Batch commit metrics
    #[inline]
    pub fn batch_committed(&self, batch_size: usize, elapsed: std::time::Duration) {
        self.batches_committed.fetch_add(1, Ordering::Relaxed);
        self.total_batched_txs.fetch_add(batch_size as u64, Ordering::Relaxed);
        self.total_batch_time_micros.fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);

        // Update max batch size
        let mut current_max = self.max_batch_size.load(Ordering::Relaxed);
        while batch_size > current_max {
            match self.max_batch_size.compare_exchange_weak(
                current_max,
                batch_size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }

    // Storage metrics
    #[inline]
    pub fn page_allocated(&self) {
        self.pages_allocated.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn page_freed(&self) {
        self.pages_freed.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_dirty_pages(&self, count: usize) {
        self.dirty_pages.store(count, Ordering::Relaxed);
    }

    // WAL metrics
    #[inline]
    pub fn wal_write(&self, bytes: u64) {
        self.wal_writes.fetch_add(1, Ordering::Relaxed);
        self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn checkpoint_completed(&self) {
        self.checkpoints.fetch_add(1, Ordering::Relaxed);
    }

    // Operation metrics
    #[inline]
    pub fn document_inserted(&self) {
        self.documents_inserted.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn document_updated(&self) {
        self.documents_updated.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn document_deleted(&self) {
        self.documents_deleted.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn document_read(&self) {
        self.documents_read.fetch_add(1, Ordering::Relaxed);
    }

    // Error metrics
    #[inline]
    pub fn io_error(&self) {
        self.io_errors.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn transaction_conflict(&self) {
        self.transaction_conflicts.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a snapshot of current metrics.
    /// Uses Relaxed ordering since we don't need strict consistency for monitoring.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let transactions_begun = self.transactions_begun.load(Ordering::Relaxed);
        let transactions_committed = self.transactions_committed.load(Ordering::Relaxed);
        let transactions_aborted = self.transactions_aborted.load(Ordering::Relaxed);
        let total_transactions = transactions_committed + transactions_aborted;

        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let cache_total_requests = cache_hits + cache_misses;

        let documents_inserted = self.documents_inserted.load(Ordering::Relaxed);
        let documents_updated = self.documents_updated.load(Ordering::Relaxed);
        let documents_deleted = self.documents_deleted.load(Ordering::Relaxed);
        let documents_read = self.documents_read.load(Ordering::Relaxed);

        let batches_committed = self.batches_committed.load(Ordering::Relaxed);
        let total_batched_txs = self.total_batched_txs.load(Ordering::Relaxed);
        let total_batch_time = self.total_batch_time_micros.load(Ordering::Relaxed);

        MetricsSnapshot {
            transactions_begun,
            transactions_committed,
            transactions_aborted,
            active_transactions: self.active_transactions.load(Ordering::Relaxed),
            total_transactions,
            commit_rate: if total_transactions > 0 {
                transactions_committed as f64 / total_transactions as f64
            } else {
                0.0
            },

            batches_committed,
            total_batched_txs,
            max_batch_size: self.max_batch_size.load(Ordering::Relaxed),
            avg_batch_size: if batches_committed > 0 {
                total_batched_txs as f64 / batches_committed as f64
            } else {
                0.0
            },
            avg_batch_time_micros: if batches_committed > 0 {
                total_batch_time as f64 / batches_committed as f64
            } else {
                0.0
            },

            pages_allocated: self.pages_allocated.load(Ordering::Relaxed),
            pages_freed: self.pages_freed.load(Ordering::Relaxed),
            cache_hits,
            cache_misses,
            cache_total_requests,
            cache_hit_rate: if cache_total_requests > 0 {
                cache_hits as f64 / cache_total_requests as f64
            } else {
                0.0
            },
            dirty_pages: self.dirty_pages.load(Ordering::Relaxed),

            wal_writes: self.wal_writes.load(Ordering::Relaxed),
            wal_bytes_written: self.wal_bytes_written.load(Ordering::Relaxed),
            checkpoints: self.checkpoints.load(Ordering::Relaxed),

            documents_inserted,
            documents_updated,
            documents_deleted,
            documents_read,
            total_document_operations: documents_inserted + documents_updated + documents_deleted + documents_read,

            io_errors: self.io_errors.load(Ordering::Relaxed),
            transaction_conflicts: self.transaction_conflicts.load(Ordering::Relaxed),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_initialization() {
        let metrics = Metrics::new();
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.transactions_begun, 0);
        assert_eq!(snapshot.active_transactions, 0);
        assert_eq!(snapshot.cache_hit_rate, 0.0);
    }

    #[test]
    fn test_transaction_metrics() {
        let metrics = Metrics::new();

        metrics.transaction_begun();
        metrics.transaction_begun();
        assert_eq!(metrics.snapshot().active_transactions, 2);

        metrics.transaction_committed();
        assert_eq!(metrics.snapshot().active_transactions, 1);
        assert_eq!(metrics.snapshot().transactions_committed, 1);

        metrics.transaction_aborted();
        assert_eq!(metrics.snapshot().active_transactions, 0);
        assert_eq!(metrics.snapshot().transactions_aborted, 1);
    }

    #[test]
    fn test_cache_hit_rate() {
        let metrics = Metrics::new();

        metrics.cache_hit();
        metrics.cache_hit();
        metrics.cache_hit();
        metrics.cache_miss();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.cache_hits, 3);
        assert_eq!(snapshot.cache_misses, 1);
        assert_eq!(snapshot.cache_hit_rate, 0.75);
    }

    #[test]
    fn test_commit_rate() {
        let metrics = Metrics::new();

        metrics.transaction_begun();
        metrics.transaction_committed();

        metrics.transaction_begun();
        metrics.transaction_committed();

        metrics.transaction_begun();
        metrics.transaction_aborted();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_transactions, 3);
        assert_eq!(snapshot.commit_rate, 2.0 / 3.0);
    }

    #[test]
    fn test_document_operations() {
        let metrics = Metrics::new();

        metrics.document_inserted();
        metrics.document_inserted();
        metrics.document_updated();
        metrics.document_deleted();
        metrics.document_read();
        metrics.document_read();
        metrics.document_read();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.documents_inserted, 2);
        assert_eq!(snapshot.documents_updated, 1);
        assert_eq!(snapshot.documents_deleted, 1);
        assert_eq!(snapshot.documents_read, 3);
        assert_eq!(snapshot.total_document_operations, 7);
    }
}
