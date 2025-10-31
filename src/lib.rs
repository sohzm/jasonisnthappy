
pub mod core;

pub use core::{Database, Transaction, Collection, SortOrder, UpsertResult, BulkWrite, BulkWriteResult, BulkWriteError, CollectionInfo, IndexInfo, DatabaseInfo, AggregationPipeline, Schema, ValueType, ChangeEvent, ChangeOperation, WatchBuilder, WatchHandle, SearchResult};
pub use core::errors::{Error, Result};
pub use core::database::{BackupInfo, DatabaseOptions};
pub use core::metrics::MetricsSnapshot;

#[cfg(feature = "web-ui")]
pub use core::WebServer;
