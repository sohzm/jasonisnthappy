
pub mod constants;
pub mod errors;
pub mod pager;
pub mod document;
pub mod btree;
pub mod tx_btree;
pub mod wal;
pub mod mvcc;
pub mod transaction;
pub mod database;
pub mod collection;
pub mod tx_collection;
pub mod metadata;
pub mod lru_cache;
pub mod index_key;
pub mod validation;
pub mod query;
pub mod query_builder;
pub mod buffer_pool;
pub mod metrics;
pub mod aggregation;
pub mod watch;
pub mod text_search;

#[cfg(feature = "web-ui")]
pub mod web_server;

pub use constants::*;
pub use database::{Database, CollectionInfo, IndexInfo, DatabaseInfo};
pub use transaction::Transaction;
pub use collection::{Collection, UpsertResult, BulkWrite, BulkWriteResult, BulkWriteError};
pub use tx_collection::TxCollection;
pub use metrics::{Metrics, MetricsSnapshot};
pub use query_builder::{QueryBuilder, SortOrder};
pub use aggregation::AggregationPipeline;
pub use validation::{Schema, ValueType};
pub use watch::{ChangeEvent, ChangeOperation, WatchBuilder, WatchHandle};
pub use text_search::SearchResult;

#[cfg(feature = "web-ui")]
pub use web_server::WebServer;
