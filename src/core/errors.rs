use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("transaction is not active")]
    TxNotActive,

    #[error("transaction already committed or rolled back")]
    TxAlreadyDone,

    #[error("transaction conflict: data was modified by another transaction")]
    TxConflict,

    #[error("database already open in this process")]
    DatabaseAlreadyOpen,

    #[error("database is closed")]
    DatabaseClosed,

    #[error("database is read-only, cannot perform operation: {operation}")]
    DatabaseReadOnly { operation: String },

    #[error("database reference not set (internal error)")]
    DatabaseReferenceNotSet,

    #[error("collection name cannot be empty")]
    CollectionNameEmpty,

    #[error("collection name too long (max 64 characters)")]
    CollectionNameTooLong,

    #[error("collection name must start with letter or underscore")]
    CollectionNameInvalidStart,

    #[error("collection name contains invalid characters (use alphanumeric and underscore only)")]
    CollectionNameInvalidChar,

    #[error("collection name is reserved")]
    CollectionNameReserved,

    #[error("collection '{name}' already exists")]
    CollectionAlreadyExists { name: String },

    #[error("collection '{name}' does not exist")]
    CollectionDoesNotExist { name: String },

    #[error("document not found: collection={collection:?}, id={id:?}")]
    DocumentNotFound { collection: String, id: String },

    #[error("document already exists: collection={collection:?}, id={id:?}")]
    DocumentAlreadyExists { collection: String, id: String },

    #[error("document exceeds maximum size")]
    DocumentTooLarge,

    #[error("bulk operation exceeds maximum size: operation has {count} items but limit is {limit}")]
    BulkOperationTooLarge { count: usize, limit: usize },

    #[error("invalid document format")]
    InvalidDocument,

    #[error("invalid document format: {reason}{}", if let Some(coll) = collection { format!(" (collection: '{}')", coll) } else { String::new() })]
    InvalidDocumentFormat {
        reason: String,
        collection: Option<String>,
    },

    #[error("transaction conflict: collection={collection:?}, document={document_id:?}, operation={operation:?}")]
    Conflict {
        collection: String,
        document_id: String,
        operation: String,
    },

    #[error("validation error: field={field:?}, value={value}, reason={reason:?}")]
    Validation {
        field: String,
        value: String,
        reason: String,
    },

    #[error("schema validation error: {0}")]
    SchemaValidation(String),

    #[error("serialization error: {error} ({context})")]
    SerializationError { context: String, error: String },

    #[error("deserialization error: {error} ({context})")]
    DeserializationError { context: String, error: String },

    #[error("invalid index key format: got '{format}', expected '{expected}'")]
    InvalidIndexKey { format: String, expected: String },

    #[error("data corruption in {component}: page={page_num}, {details}")]
    Corruption {
        component: String,
        page_num: u64,
        details: String,
    },

    #[error("lock poisoned: {lock_name} (another thread panicked while holding this lock)")]
    LockPoisoned { lock_name: String },

    #[error("data corruption: {details}")]
    DataCorruption { details: String },

    #[error("operation cancelled: {operation}: {error}")]
    ContextCancelled { operation: String, error: String },

    #[error("invalid magic number")]
    InvalidMagic,

    #[error("unsupported version")]
    InvalidVersion,

    #[error("invalid page number")]
    InvalidPageNum,

    #[error("page size must be 4096 bytes")]
    InvalidPageSize,

    #[error("WAL file corrupted")]
    WALCorrupted,

    #[error("invalid transaction ID")]
    WALInvalidTx,

    #[error("WAL checksum verification failed")]
    WALChecksumFail,

    #[error("document not found")]
    NotFound,

    #[error("IO error: {0}")]
    Io(String),

    #[error("{0}")]
    Other(String),
}

// Manual From implementations for common error types
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Other(format!("JSON error: {}", err))
    }
}

// Helper for converting PoisonError to our Error type
impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        Error::LockPoisoned {
            lock_name: "unknown".to_string(),
        }
    }
}

// Helper for converting TryFromSliceError (used in binary parsing)
impl From<std::array::TryFromSliceError> for Error {
    fn from(_: std::array::TryFromSliceError) -> Self {
        Error::DataCorruption {
            details: "failed to parse binary data".to_string(),
        }
    }
}

pub trait PoisonedLockExt<'a, T> {
    fn recover_poison(self) -> T;
}

impl<'a, T> PoisonedLockExt<'a, std::sync::RwLockReadGuard<'a, T>> for std::sync::LockResult<std::sync::RwLockReadGuard<'a, T>> {
    fn recover_poison(self) -> std::sync::RwLockReadGuard<'a, T> {
        match self {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl<'a, T> PoisonedLockExt<'a, std::sync::RwLockWriteGuard<'a, T>> for std::sync::LockResult<std::sync::RwLockWriteGuard<'a, T>> {
    fn recover_poison(self) -> std::sync::RwLockWriteGuard<'a, T> {
        match self {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl<'a, T> PoisonedLockExt<'a, std::sync::MutexGuard<'a, T>> for std::sync::LockResult<std::sync::MutexGuard<'a, T>> {
    fn recover_poison(self) -> std::sync::MutexGuard<'a, T> {
        match self {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
