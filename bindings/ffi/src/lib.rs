use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use jasonisnthappy::core::{Database, Transaction};
use jasonisnthappy::core::query_builder::SortOrder;
use jasonisnthappy::core::watch::ChangeOperation;
use serde_json::Value;

#[cfg(feature = "web-ui")]
use jasonisnthappy::core::web_server::WebServer;

// Opaque pointer for database
#[repr(C)]
pub struct CDatabase {
    inner: Arc<Database>,
}

// Opaque pointer for web server
#[cfg(feature = "web-ui")]
#[repr(C)]
pub struct CWebServer {
    inner: Option<WebServer>,
}

// Opaque pointer for transaction
#[repr(C)]
pub struct CTransaction {
    inner: Transaction,
}

// Opaque pointer for non-transactional collection
#[repr(C)]
pub struct CCollection {
    inner: jasonisnthappy::core::collection::Collection,
}

// Opaque pointer for watch handle
pub struct CWatchHandle {
    _watch_handle: jasonisnthappy::core::watch::WatchHandle,
    stop_flag: Arc<AtomicBool>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

/// C callback function type for watch events
///
/// # Parameters
/// - collection: Name of the collection where the change occurred
/// - operation: "insert", "update", or "delete"
/// - doc_id: ID of the document
/// - doc_json: JSON representation of the document (NULL for delete operations)
/// - user_data: User-provided context pointer passed to watch_start
pub type WatchCallback = extern "C" fn(
    collection: *const c_char,
    operation: *const c_char,
    doc_id: *const c_char,
    doc_json: *const c_char,
    user_data: *mut std::os::raw::c_void,
);

/// C callback function type for run_transaction
///
/// # Parameters
/// - tx: Transaction handle to use for operations
/// - user_data: User-provided context pointer
///
/// # Returns
/// - 0 for success (commit), -1 for error (rollback)
pub type TransactionCallback = extern "C" fn(
    tx: *mut CTransaction,
    user_data: *mut std::os::raw::c_void,
) -> i32;

// Wrapper to make callback context Send-able (caller must ensure thread safety)
// We store user_data as usize since it's just a pointer value
struct SendableCallbackContext {
    callback: WatchCallback,
    user_data_addr: usize,
}
// Safe because we're just passing pointer values across threads
unsafe impl Send for SendableCallbackContext {}

// For builders, we'll store the collection pointer and rebuild on each call
// This avoids lifetime issues with FFI

// Error structure for C API
#[repr(C)]
pub struct CError {
    pub code: i32,
    pub message: *mut c_char,
}

impl CError {
    fn from_error(err: jasonisnthappy::Error) -> Self {
        let message = CString::new(err.to_string()).unwrap_or_else(|_| CString::new("Unknown error").unwrap());
        CError {
            code: -1,
            message: message.into_raw(),
        }
    }

    fn success() -> Self {
        CError {
            code: 0,
            message: ptr::null_mut(),
        }
    }
}

// Database configuration structures
#[repr(C)]
pub struct CDatabaseOptions {
    pub cache_size: usize,
    pub auto_checkpoint_threshold: u64,
    pub file_permissions: u32,
    pub read_only: bool,
    pub max_bulk_operations: usize,
    pub max_document_size: usize,
    pub max_request_body_size: usize,
}

impl From<CDatabaseOptions> for jasonisnthappy::core::database::DatabaseOptions {
    fn from(opts: CDatabaseOptions) -> Self {
        jasonisnthappy::core::database::DatabaseOptions {
            cache_size: opts.cache_size,
            auto_checkpoint_threshold: opts.auto_checkpoint_threshold,
            file_permissions: opts.file_permissions,
            read_only: opts.read_only,
            max_bulk_operations: opts.max_bulk_operations,
            max_document_size: opts.max_document_size,
            max_request_body_size: opts.max_request_body_size,
        }
    }
}

#[repr(C)]
pub struct CTransactionConfig {
    pub max_retries: usize,
    pub retry_backoff_base_ms: u64,
    pub max_retry_backoff_ms: u64,
}

impl From<CTransactionConfig> for jasonisnthappy::core::database::TransactionConfig {
    fn from(cfg: CTransactionConfig) -> Self {
        jasonisnthappy::core::database::TransactionConfig {
            max_retries: cfg.max_retries,
            retry_backoff_base_ms: cfg.retry_backoff_base_ms,
            max_retry_backoff_ms: cfg.max_retry_backoff_ms,
        }
    }
}

impl From<jasonisnthappy::core::database::TransactionConfig> for CTransactionConfig {
    fn from(cfg: jasonisnthappy::core::database::TransactionConfig) -> Self {
        CTransactionConfig {
            max_retries: cfg.max_retries,
            retry_backoff_base_ms: cfg.retry_backoff_base_ms,
            max_retry_backoff_ms: cfg.max_retry_backoff_ms,
        }
    }
}

unsafe fn c_str_to_string(s: *const c_char) -> Result<String, CError> {
    if s.is_null() {
        return Err(CError {
            code: -1,
            message: CString::new("Null pointer provided").unwrap().into_raw(),
        });
    }

    CStr::from_ptr(s)
        .to_str()
        .map(|s| s.to_string())
        .map_err(|_| CError {
            code: -1,
            message: CString::new("Invalid UTF-8 string").unwrap().into_raw(),
        })
}

// ============================================================================
// Database Management
// ============================================================================

#[no_mangle]
pub extern "C" fn jasonisnthappy_open(
    path: *const c_char,
    error_out: *mut CError,
) -> *mut CDatabase {
    let path_str = match unsafe { c_str_to_string(path) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return ptr::null_mut();
        }
    };

    match Database::open(&path_str) {
        Ok(db) => Box::into_raw(Box::new(CDatabase { inner: Arc::new(db) })),
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_open_with_options(
    path: *const c_char,
    options: CDatabaseOptions,
    error_out: *mut CError,
) -> *mut CDatabase {
    let path_str = match unsafe { c_str_to_string(path) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return ptr::null_mut();
        }
    };

    let rust_options = jasonisnthappy::core::database::DatabaseOptions::from(options);

    match Database::open_with_options(&path_str, rust_options) {
        Ok(db) => Box::into_raw(Box::new(CDatabase { inner: Arc::new(db) })),
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_close(db: *mut CDatabase) {
    if !db.is_null() {
        unsafe {
            let _ = Box::from_raw(db);
        }
    }
}

// ============================================================================
// Database Configuration
// ============================================================================

#[no_mangle]
pub extern "C" fn jasonisnthappy_set_transaction_config(
    db: *mut CDatabase,
    config: CTransactionConfig,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let db_ref = unsafe { &(*db).inner };
    let rust_config = jasonisnthappy::core::database::TransactionConfig::from(config);

    db_ref.set_transaction_config(rust_config);

    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    0
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_get_transaction_config(
    db: *mut CDatabase,
    config_out: *mut CTransactionConfig,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let db_ref = unsafe { &(*db).inner };
    let rust_config = db_ref.get_transaction_config();

    if !config_out.is_null() {
        unsafe {
            *config_out = CTransactionConfig::from(rust_config);
        }
    }

    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    0
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_set_auto_checkpoint_threshold(
    db: *mut CDatabase,
    threshold: u64,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let db_ref = unsafe { &(*db).inner };
    db_ref.set_auto_checkpoint_threshold(threshold);

    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    0
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_default_database_options() -> CDatabaseOptions {
    let defaults = jasonisnthappy::core::database::DatabaseOptions::default();
    CDatabaseOptions {
        cache_size: defaults.cache_size,
        auto_checkpoint_threshold: defaults.auto_checkpoint_threshold,
        file_permissions: defaults.file_permissions,
        read_only: defaults.read_only,
        max_bulk_operations: defaults.max_bulk_operations,
        max_document_size: defaults.max_document_size,
        max_request_body_size: defaults.max_request_body_size,
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_default_transaction_config() -> CTransactionConfig {
    let defaults = jasonisnthappy::core::database::TransactionConfig::default();
    CTransactionConfig {
        max_retries: defaults.max_retries,
        retry_backoff_base_ms: defaults.retry_backoff_base_ms,
        max_retry_backoff_ms: defaults.max_retry_backoff_ms,
    }
}

// ============================================================================
// Transaction Management
// ============================================================================

#[no_mangle]
pub extern "C" fn jasonisnthappy_begin_transaction(
    db: *mut CDatabase,
    error_out: *mut CError,
) -> *mut CTransaction {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return ptr::null_mut();
    }

    let db_ref = unsafe { &(*db).inner };

    match db_ref.begin() {
        Ok(tx) => Box::into_raw(Box::new(CTransaction { inner: tx })),
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_commit(
    tx: *mut CTransaction,
    error_out: *mut CError,
) -> i32 {
    if tx.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null transaction pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let mut tx_obj = unsafe { Box::from_raw(tx) };

    match tx_obj.inner.commit() {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_rollback(tx: *mut CTransaction) {
    if !tx.is_null() {
        unsafe {
            let mut tx_obj = Box::from_raw(tx);
            let _ = tx_obj.inner.rollback();
        }
    }
}

/// Run a transaction with automatic retries on conflict
///
/// This is a convenience wrapper that handles begin/commit/rollback with automatic
/// retries according to the database's transaction config.
///
/// # Parameters
/// - db: Database handle
/// - callback: Function called with the transaction - return 0 to commit, -1 to rollback
/// - user_data: Optional user context pointer passed to callback
/// - error_out: Output for error information
///
/// # Returns
/// 0 on successful commit, -1 on error
///
/// # Example
/// The callback should perform all operations and return 0 for success:
/// ```c
/// int32_t my_callback(CTransaction* tx, void* user_data) {
///     // Do operations with tx
///     return 0;  // success - will commit
///     // return -1;  // error - will rollback
/// }
/// jasonisnthappy_run_transaction(db, my_callback, user_data, &error);
/// ```
#[no_mangle]
pub extern "C" fn jasonisnthappy_run_transaction(
    db: *mut CDatabase,
    callback: TransactionCallback,
    user_data: *mut std::os::raw::c_void,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    unsafe {
        let db_ref = &(*db).inner;

        // Get transaction config for retry logic
        let config = db_ref.get_transaction_config();
        let mut last_err = None;

        // Retry loop (same logic as Database::run_transaction)
        for attempt in 0..=(config.max_retries) {
            // Begin new transaction
            let tx = match db_ref.begin() {
                Ok(tx) => tx,
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = CError::from_error(e);
                    }
                    return -1;
                }
            };

            // Wrap in CTransaction for callback
            let c_tx = Box::into_raw(Box::new(CTransaction { inner: tx }));

            // Call user callback
            let callback_result = callback(c_tx, user_data);

            // Get back the transaction
            let mut tx_obj = Box::from_raw(c_tx);

            // Handle result
            if callback_result == 0 {
                // Callback succeeded - try to commit
                match tx_obj.inner.commit() {
                    Ok(_) => {
                        if !error_out.is_null() {
                            *error_out = CError::success();
                        }
                        return 0;  // Success!
                    }
                    Err(e) => {
                        if matches!(e, jasonisnthappy::Error::TxConflict) {
                            // Conflict - retry
                            last_err = Some(e);
                        } else {
                            // Other error - fail immediately
                            if !error_out.is_null() {
                                *error_out = CError::from_error(e);
                            }
                            return -1;
                        }
                    }
                }
            } else {
                // Callback returned error - rollback and fail
                let _ = tx_obj.inner.rollback();
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new("Transaction callback returned error")
                            .unwrap()
                            .into_raw(),
                    };
                }
                return -1;
            }

            // Backoff before retry
            if attempt < config.max_retries {
                let backoff_ms = config.retry_backoff_base_ms * (1 << attempt);
                let backoff_ms = backoff_ms.min(config.max_retry_backoff_ms);
                if backoff_ms > 0 {
                    std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                }
            }
        }

        // All retries exhausted
        if !error_out.is_null() {
            *error_out = if let Some(err) = last_err {
                CError::from_error(err)
            } else {
                CError {
                    code: -1,
                    message: CString::new("Transaction conflict - max retries exhausted")
                        .unwrap()
                        .into_raw(),
                }
            };
        }
        -1
    }
}

/// Check if a transaction is still active (not committed or rolled back)
#[no_mangle]
pub extern "C" fn jasonisnthappy_transaction_is_active(
    tx: *mut CTransaction,
    error_out: *mut CError,
) -> i32 {
    if tx.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null transaction pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    unsafe {
        let tx_ref = &(*tx).inner;
        if tx_ref.is_active() {
            1  // Active
        } else {
            0  // Not active
        }
    }
}

// ============================================================================
// Document Operations (within a transaction)
// ============================================================================

#[no_mangle]
pub extern "C" fn jasonisnthappy_insert(
    tx: *mut CTransaction,
    collection_name: *const c_char,
    json: *const c_char,
    id_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if tx.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null transaction pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let json_str = match unsafe { c_str_to_string(json) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let value: Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid JSON: {}", e)).unwrap().into_raw(),
                    };
                }
            }
            return -1;
        }
    };

    let tx_ref = unsafe { &mut (*tx).inner };

    let result = (|| -> jasonisnthappy::Result<String> {
        let mut coll = tx_ref.collection(&coll_name)?;
        coll.insert(value)
    })();

    match result {
        Ok(id) => {
            if !id_out.is_null() {
                let c_id = CString::new(id).unwrap();
                unsafe { *id_out = c_id.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_find_by_id(
    tx: *mut CTransaction,
    collection_name: *const c_char,
    id: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if tx.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null transaction pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let doc_id = match unsafe { c_str_to_string(id) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let tx_ref = unsafe { &mut (*tx).inner };

    let result = (|| -> jasonisnthappy::Result<Value> {
        let coll = tx_ref.collection(&coll_name)?;
        coll.find_by_id(&doc_id)
    })();

    match result {
        Ok(doc) => {
            let json_str = serde_json::to_string(&doc).unwrap();
            let c_str = CString::new(json_str).unwrap();
            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("not found") || err_str.contains("does not exist") {
                if !json_out.is_null() {
                    unsafe { *json_out = ptr::null_mut(); }
                }
                if !error_out.is_null() {
                    unsafe { *error_out = CError::success(); }
                }
                1
            } else {
                if !error_out.is_null() {
                    unsafe { *error_out = CError::from_error(e); }
                }
                -1
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_update_by_id(
    tx: *mut CTransaction,
    collection_name: *const c_char,
    id: *const c_char,
    json: *const c_char,
    error_out: *mut CError,
) -> i32 {
    if tx.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null transaction pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let doc_id = match unsafe { c_str_to_string(id) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let json_str = match unsafe { c_str_to_string(json) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let value: Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid JSON: {}", e)).unwrap().into_raw(),
                    };
                }
            }
            return -1;
        }
    };

    let tx_ref = unsafe { &mut (*tx).inner };

    let result = (|| -> jasonisnthappy::Result<()> {
        let mut coll = tx_ref.collection(&coll_name)?;
        coll.update_by_id(&doc_id, value)
    })();

    match result {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_delete_by_id(
    tx: *mut CTransaction,
    collection_name: *const c_char,
    id: *const c_char,
    error_out: *mut CError,
) -> i32 {
    if tx.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null transaction pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let doc_id = match unsafe { c_str_to_string(id) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let tx_ref = unsafe { &mut (*tx).inner };

    let result = (|| -> jasonisnthappy::Result<()> {
        let mut coll = tx_ref.collection(&coll_name)?;
        coll.delete_by_id(&doc_id)
    })();

    match result {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_find_all(
    tx: *mut CTransaction,
    collection_name: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if tx.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null transaction pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let tx_ref = unsafe { &mut (*tx).inner };

    let result = (|| -> jasonisnthappy::Result<Vec<Value>> {
        let coll = tx_ref.collection(&coll_name)?;
        coll.find_all()
    })();

    match result {
        Ok(docs) => {
            let json_str = serde_json::to_string(&docs).unwrap();
            let c_str = CString::new(json_str).unwrap();
            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

// ============================================================================
// Memory Management
// ============================================================================

#[no_mangle]
pub extern "C" fn jasonisnthappy_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            let _ = CString::from_raw(s);
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_free_error(error: CError) {
    if !error.message.is_null() {
        unsafe {
            let _ = CString::from_raw(error.message);
        }
    }
}

// ============================================================================
// Advanced Query Operations
// ============================================================================

// TODO: The find(), insert_many(), update(), and delete() methods need to be
// added to TxCollection in the core library. They currently exist only in
// Collection (non-transactional). Uncomment these FFI functions once they're
// implemented in TxCollection.

/*
#[no_mangle]
pub extern "C" fn jasonisnthappy_find(
    tx: *mut CTransaction,
    collection_name: *const c_char,
    query: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    // Implementation commented out - needs TxCollection::find()
    -1
}
*/

#[no_mangle]
pub extern "C" fn jasonisnthappy_count(
    tx: *mut CTransaction,
    collection_name: *const c_char,
    count_out: *mut u64,
    error_out: *mut CError,
) -> i32 {
    if tx.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null transaction pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let tx_ref = unsafe { &mut (*tx).inner };

    let result = (|| -> jasonisnthappy::Result<usize> {
        let coll = tx_ref.collection(&coll_name)?;
        coll.count()
    })();

    match result {
        Ok(count) => {
            if !count_out.is_null() {
                unsafe { *count_out = count as u64; }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

/*
#[no_mangle]
pub extern "C" fn jasonisnthappy_insert_many(...) { }
#[no_mangle]
pub extern "C" fn jasonisnthappy_update(...) { }
#[no_mangle]
pub extern "C" fn jasonisnthappy_delete(...) { }
*/

// ============================================================================
// Collection Management
// ============================================================================

#[no_mangle]
pub extern "C" fn jasonisnthappy_create_collection(
    tx: *mut CTransaction,
    collection_name: *const c_char,
    error_out: *mut CError,
) -> i32 {
    if tx.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null transaction pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let tx_ref = unsafe { &mut (*tx).inner };

    match tx_ref.create_collection(&coll_name) {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_drop_collection(
    tx: *mut CTransaction,
    collection_name: *const c_char,
    error_out: *mut CError,
) -> i32 {
    if tx.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null transaction pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let tx_ref = unsafe { &mut (*tx).inner };

    match tx_ref.drop_collection(&coll_name) {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_rename_collection(
    tx: *mut CTransaction,
    old_name: *const c_char,
    new_name: *const c_char,
    error_out: *mut CError,
) -> i32 {
    if tx.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null transaction pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let old_name_str = match unsafe { c_str_to_string(old_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let new_name_str = match unsafe { c_str_to_string(new_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let tx_ref = unsafe { &mut (*tx).inner };

    match tx_ref.rename_collection(&old_name_str, &new_name_str) {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_list_collections(
    db: *mut CDatabase,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let db_ref = unsafe { &(*db).inner };
    let metadata = db_ref.get_metadata();
    let mut collections: Vec<String> = metadata.collections.keys().cloned().collect();
    collections.sort();

    let json_str = serde_json::to_string(&collections).unwrap_or_else(|_| "[]".to_string());
    let c_str = CString::new(json_str).unwrap();

    if !json_out.is_null() {
        unsafe { *json_out = c_str.into_raw(); }
    }
    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    0
}

/// List all indexes for a collection
///
/// Returns JSON array of index objects with: name, fields (array), unique (bool), btree_root
#[no_mangle]
pub extern "C" fn jasonisnthappy_list_indexes(
    db: *mut CDatabase,
    collection_name: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() || collection_name.is_null() || json_out.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    unsafe {
        let db_ref = &(*db).inner;

        let coll_name = match CStr::from_ptr(collection_name).to_str() {
            Ok(s) => s,
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid collection_name UTF-8: {}", e))
                            .unwrap()
                            .into_raw(),
                    };
                }
                return -1;
            }
        };

        match db_ref.list_indexes(coll_name) {
            Ok(indexes) => {
                // Convert to JSON-friendly format
                let json_indexes: Vec<_> = indexes.iter().map(|idx| {
                    serde_json::json!({
                        "name": idx.name,
                        "fields": idx.fields,
                        "unique": idx.unique,
                        "btree_root": idx.btree_root
                    })
                }).collect();

                match serde_json::to_string(&json_indexes) {
                    Ok(json) => {
                        *json_out = CString::new(json).unwrap().into_raw();
                        0
                    }
                    Err(e) => {
                        if !error_out.is_null() {
                            *error_out = CError {
                                code: -1,
                                message: CString::new(format!("Failed to serialize indexes: {}", e))
                                    .unwrap()
                                    .into_raw(),
                            };
                        }
                        -1
                    }
                }
            }
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError::from_error(e);
                }
                -1
            }
        }
    }
}

// ============================================================================
// Index Management
// ============================================================================

#[no_mangle]
pub extern "C" fn jasonisnthappy_create_index(
    db: *mut CDatabase,
    collection_name: *const c_char,
    index_name: *const c_char,
    field: *const c_char,
    unique: bool,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let idx_name = match unsafe { c_str_to_string(index_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let field_name = match unsafe { c_str_to_string(field) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let db_ref = unsafe { &(*db).inner };

    match db_ref.create_index(&coll_name, &idx_name, &field_name, unique) {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_create_compound_index(
    db: *mut CDatabase,
    collection_name: *const c_char,
    index_name: *const c_char,
    fields: *const *const c_char,
    num_fields: usize,
    unique: bool,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    if fields.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null fields array pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let idx_name = match unsafe { c_str_to_string(index_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    // Convert array of C strings to Vec<&str>
    let mut field_names: Vec<String> = Vec::new();
    for i in 0..num_fields {
        unsafe {
            let field_ptr = *fields.add(i);
            match c_str_to_string(field_ptr) {
                Ok(s) => field_names.push(s),
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = e;
                    }
                    return -1;
                }
            }
        }
    }

    let field_refs: Vec<&str> = field_names.iter().map(|s| s.as_str()).collect();

    let db_ref = unsafe { &(*db).inner };

    match db_ref.create_compound_index(&coll_name, &idx_name, &field_refs, unique) {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_create_text_index(
    db: *mut CDatabase,
    collection_name: *const c_char,
    index_name: *const c_char,
    fields: *const *const c_char,
    num_fields: usize,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    if fields.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null fields array pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let idx_name = match unsafe { c_str_to_string(index_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    // Convert array of C strings to Vec<&str>
    let mut field_names: Vec<String> = Vec::new();
    for i in 0..num_fields {
        unsafe {
            let field_ptr = *fields.add(i);
            match c_str_to_string(field_ptr) {
                Ok(s) => field_names.push(s),
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = e;
                    }
                    return -1;
                }
            }
        }
    }

    let field_refs: Vec<&str> = field_names.iter().map(|s| s.as_str()).collect();

    let db_ref = unsafe { &(*db).inner };

    match db_ref.create_text_index(&coll_name, &idx_name, &field_refs) {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_drop_index(
    db: *mut CDatabase,
    collection_name: *const c_char,
    index_name: *const c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let idx_name = match unsafe { c_str_to_string(index_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let db_ref = unsafe { &(*db).inner };

    match db_ref.drop_index(&coll_name, &idx_name) {
        Ok(_) => 0,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(e.to_string()).unwrap().into_raw(),
                    };
                }
            }
            -1
        }
    }
}

// ============================================================================
// Database Info & Stats
// ============================================================================

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_stats(
    db: *mut CDatabase,
    collection_name: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let db_ref = unsafe { &(*db).inner };

    match db_ref.collection_stats(&coll_name) {
        Ok(stats) => {
            let json_obj = serde_json::json!({
                "name": stats.name,
                "document_count": stats.document_count,
                "btree_root": stats.btree_root,
                "indexes": stats.indexes.iter().map(|idx| {
                    serde_json::json!({
                        "name": idx.name,
                        "fields": idx.fields,
                        "unique": idx.unique,
                        "btree_root": idx.btree_root,
                    })
                }).collect::<Vec<_>>(),
            });

            let json_str = serde_json::to_string(&json_obj).unwrap_or_else(|_| "{}".to_string());
            let c_str = CString::new(json_str).unwrap();

            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_database_info(
    db: *mut CDatabase,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let db_ref = unsafe { &(*db).inner };

    match db_ref.info() {
        Ok(info) => {
            let json_obj = serde_json::json!({
                "path": info.path,
                "version": info.version,
                "num_pages": info.num_pages,
                "file_size": info.file_size,
                "total_documents": info.total_documents,
                "read_only": info.read_only,
                "collections": info.collections.iter().map(|coll| {
                    serde_json::json!({
                        "name": coll.name,
                        "document_count": coll.document_count,
                        "btree_root": coll.btree_root,
                        "indexes": coll.indexes.iter().map(|idx| {
                            serde_json::json!({
                                "name": idx.name,
                                "fields": idx.fields,
                                "unique": idx.unique,
                                "btree_root": idx.btree_root,
                            })
                        }).collect::<Vec<_>>(),
                    })
                }).collect::<Vec<_>>(),
            });

            let json_str = serde_json::to_string(&json_obj).unwrap_or_else(|_| "{}".to_string());
            let c_str = CString::new(json_str).unwrap();

            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_get_path(
    db: *mut CDatabase,
    path_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let db_ref = unsafe { &(*db).inner };
    let path = db_ref.path();

    let c_str = CString::new(path).unwrap();
    if !path_out.is_null() {
        unsafe { *path_out = c_str.into_raw(); }
    }
    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    0
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_is_read_only(
    db: *mut CDatabase,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let db_ref = unsafe { &(*db).inner };
    let read_only = db_ref.is_read_only();

    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    if read_only { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_max_bulk_operations(
    db: *mut CDatabase,
    error_out: *mut CError,
) -> usize {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return 0;
    }
    let db_ref = unsafe { &(*db).inner };
    let value = db_ref.max_bulk_operations();

    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    value
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_max_document_size(
    db: *mut CDatabase,
    error_out: *mut CError,
) -> usize {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return 0;
    }
    let db_ref = unsafe { &(*db).inner };
    let value = db_ref.max_document_size();

    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    value
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_max_request_body_size(
    db: *mut CDatabase,
    error_out: *mut CError,
) -> usize {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return 0;
    }
    let db_ref = unsafe { &(*db).inner };
    let value = db_ref.max_request_body_size();

    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    value
}

// ============================================================================
// Schema Validation
// ============================================================================

#[no_mangle]
pub extern "C" fn jasonisnthappy_set_schema(
    db: *mut CDatabase,
    collection_name: *const c_char,
    schema_json: *const c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let schema_str = match unsafe { c_str_to_string(schema_json) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    // Parse the JSON schema
    let schema: jasonisnthappy::core::validation::Schema = match serde_json::from_str(&schema_str) {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid schema JSON: {}", e)).unwrap().into_raw(),
                    };
                }
            }
            return -1;
        }
    };

    let db_ref = unsafe { &(*db).inner };

    match db_ref.set_schema(&coll_name, schema) {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_get_schema(
    db: *mut CDatabase,
    collection_name: *const c_char,
    schema_json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let db_ref = unsafe { &(*db).inner };

    match db_ref.get_schema(&coll_name) {
        Some(schema) => {
            let json_str = serde_json::to_string(&schema).unwrap_or_else(|_| "{}".to_string());
            let c_str = CString::new(json_str).unwrap();

            if !schema_json_out.is_null() {
                unsafe { *schema_json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        None => {
            // No schema set for this collection
            if !schema_json_out.is_null() {
                unsafe { *schema_json_out = ptr::null_mut(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            1 // Return 1 to indicate "no schema found" but not an error
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_remove_schema(
    db: *mut CDatabase,
    collection_name: *const c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let db_ref = unsafe { &(*db).inner };

    match db_ref.remove_schema(&coll_name) {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

// ============================================================================
// Non-Transactional Collection API
// ============================================================================

#[no_mangle]
pub extern "C" fn jasonisnthappy_get_collection(
    db: *mut CDatabase,
    collection_name: *const c_char,
    error_out: *mut CError,
) -> *mut CCollection {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return ptr::null_mut();
    }

    let coll_name = match unsafe { c_str_to_string(collection_name) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return ptr::null_mut();
        }
    };

    let db_ref = unsafe { &(*db).inner };
    let collection = db_ref.collection(&coll_name);

    Box::into_raw(Box::new(CCollection { inner: collection }))
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_free(coll: *mut CCollection) {
    if !coll.is_null() {
        unsafe {
            let _ = Box::from_raw(coll);
        }
    }
}

// Upsert operations
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_upsert_by_id(
    coll: *mut CCollection,
    id: *const c_char,
    json: *const c_char,
    result_out: *mut i32,
    id_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let doc_id = match unsafe { c_str_to_string(id) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let json_str = match unsafe { c_str_to_string(json) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let value: Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid JSON: {}", e)).unwrap().into_raw(),
                    };
                }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.upsert_by_id(&doc_id, value) {
        Ok(result) => {
            match result {
                jasonisnthappy::core::collection::UpsertResult::Inserted(id) => {
                    if !result_out.is_null() {
                        unsafe { *result_out = 0; } // 0 = inserted
                    }
                    if !id_out.is_null() {
                        let c_id = CString::new(id).unwrap();
                        unsafe { *id_out = c_id.into_raw(); }
                    }
                }
                jasonisnthappy::core::collection::UpsertResult::Updated(id) => {
                    if !result_out.is_null() {
                        unsafe { *result_out = 1; } // 1 = updated
                    }
                    if !id_out.is_null() {
                        let c_id = CString::new(id).unwrap();
                        unsafe { *id_out = c_id.into_raw(); }
                    }
                }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_upsert(
    coll: *mut CCollection,
    query: *const c_char,
    json: *const c_char,
    result_out: *mut i32,
    id_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let query_str = match unsafe { c_str_to_string(query) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let json_str = match unsafe { c_str_to_string(json) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let value: Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid JSON: {}", e)).unwrap().into_raw(),
                    };
                }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.upsert(&query_str, value) {
        Ok(result) => {
            match result {
                jasonisnthappy::core::collection::UpsertResult::Inserted(id) => {
                    if !result_out.is_null() {
                        unsafe { *result_out = 0; } // 0 = inserted
                    }
                    if !id_out.is_null() {
                        let c_id = CString::new(id).unwrap();
                        unsafe { *id_out = c_id.into_raw(); }
                    }
                }
                jasonisnthappy::core::collection::UpsertResult::Updated(id) => {
                    if !result_out.is_null() {
                        unsafe { *result_out = 1; } // 1 = updated
                    }
                    if !id_out.is_null() {
                        let c_id = CString::new(id).unwrap();
                        unsafe { *id_out = c_id.into_raw(); }
                    }
                }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

// Query/find operations
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_find(
    coll: *mut CCollection,
    query: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let query_str = match unsafe { c_str_to_string(query) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.find(&query_str) {
        Ok(docs) => {
            let json_str = serde_json::to_string(&docs).unwrap();
            let c_str = CString::new(json_str).unwrap();
            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_find_one(
    coll: *mut CCollection,
    query: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let query_str = match unsafe { c_str_to_string(query) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.find_one(&query_str) {
        Ok(Some(doc)) => {
            let json_str = serde_json::to_string(&doc).unwrap();
            let c_str = CString::new(json_str).unwrap();
            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Ok(None) => {
            // Not found
            if !json_out.is_null() {
                unsafe { *json_out = ptr::null_mut(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            1 // Return 1 to indicate "not found"
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

// Update/delete operations with queries
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_update(
    coll: *mut CCollection,
    query: *const c_char,
    updates_json: *const c_char,
    count_out: *mut usize,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let query_str = match unsafe { c_str_to_string(query) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let updates_str = match unsafe { c_str_to_string(updates_json) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let updates: Value = match serde_json::from_str(&updates_str) {
        Ok(v) => v,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid JSON: {}", e)).unwrap().into_raw(),
                    };
                }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.update(&query_str, updates) {
        Ok(count) => {
            if !count_out.is_null() {
                unsafe { *count_out = count; }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_update_one(
    coll: *mut CCollection,
    query: *const c_char,
    updates_json: *const c_char,
    updated_out: *mut bool,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let query_str = match unsafe { c_str_to_string(query) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let updates_str = match unsafe { c_str_to_string(updates_json) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let updates: Value = match serde_json::from_str(&updates_str) {
        Ok(v) => v,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid JSON: {}", e)).unwrap().into_raw(),
                    };
                }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.update_one(&query_str, updates) {
        Ok(updated) => {
            if !updated_out.is_null() {
                unsafe { *updated_out = updated; }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_delete(
    coll: *mut CCollection,
    query: *const c_char,
    count_out: *mut usize,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let query_str = match unsafe { c_str_to_string(query) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.delete(&query_str) {
        Ok(count) => {
            if !count_out.is_null() {
                unsafe { *count_out = count; }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_delete_one(
    coll: *mut CCollection,
    query: *const c_char,
    deleted_out: *mut bool,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let query_str = match unsafe { c_str_to_string(query) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.delete_one(&query_str) {
        Ok(deleted) => {
            if !deleted_out.is_null() {
                unsafe { *deleted_out = deleted; }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

// Bulk insert
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_insert_many(
    coll: *mut CCollection,
    docs_json: *const c_char,
    ids_json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let docs_str = match unsafe { c_str_to_string(docs_json) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let docs: Vec<Value> = match serde_json::from_str(&docs_str) {
        Ok(v) => v,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid JSON array: {}", e)).unwrap().into_raw(),
                    };
                }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.insert_many(docs) {
        Ok(ids) => {
            let json_str = serde_json::to_string(&ids).unwrap();
            let c_str = CString::new(json_str).unwrap();
            if !ids_json_out.is_null() {
                unsafe { *ids_json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

// Distinct operations
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_distinct(
    coll: *mut CCollection,
    field: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let field_str = match unsafe { c_str_to_string(field) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.distinct(&field_str) {
        Ok(values) => {
            let json_str = serde_json::to_string(&values).unwrap();
            let c_str = CString::new(json_str).unwrap();
            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_count_distinct(
    coll: *mut CCollection,
    field: *const c_char,
    count_out: *mut usize,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let field_str = match unsafe { c_str_to_string(field) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.count_distinct(&field_str) {
        Ok(count) => {
            if !count_out.is_null() {
                unsafe { *count_out = count; }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

// Text search
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_search(
    coll: *mut CCollection,
    query: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let query_str = match unsafe { c_str_to_string(query) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.search(&query_str) {
        Ok(results) => {
            // SearchResult has doc_id and score
            let json_array: Vec<serde_json::Value> = results.iter().map(|r| {
                serde_json::json!({
                    "doc_id": r.doc_id,
                    "score": r.score,
                })
            }).collect();

            let json_str = serde_json::to_string(&json_array).unwrap();
            let c_str = CString::new(json_str).unwrap();
            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

// Basic Collection CRUD (non-transactional)
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_insert(
    coll: *mut CCollection,
    json: *const c_char,
    id_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let json_str = match unsafe { c_str_to_string(json) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let value: Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid JSON: {}", e)).unwrap().into_raw(),
                    };
                }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.insert(value) {
        Ok(id) => {
            if !id_out.is_null() {
                let c_id = CString::new(id).unwrap();
                unsafe { *id_out = c_id.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_find_by_id(
    coll: *mut CCollection,
    id: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let doc_id = match unsafe { c_str_to_string(id) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.find_by_id(&doc_id) {
        Ok(doc) => {
            let json_str = serde_json::to_string(&doc).unwrap();
            let c_str = CString::new(json_str).unwrap();
            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_update_by_id(
    coll: *mut CCollection,
    id: *const c_char,
    updates_json: *const c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let doc_id = match unsafe { c_str_to_string(id) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let updates_str = match unsafe { c_str_to_string(updates_json) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let updates: Value = match serde_json::from_str(&updates_str) {
        Ok(v) => v,
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid JSON: {}", e)).unwrap().into_raw(),
                    };
                }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.update_by_id(&doc_id, updates) {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_delete_by_id(
    coll: *mut CCollection,
    id: *const c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let doc_id = match unsafe { c_str_to_string(id) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.delete_by_id(&doc_id) {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_find_all(
    coll: *mut CCollection,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.find_all() {
        Ok(docs) => {
            let json_str = serde_json::to_string(&docs).unwrap();
            let c_str = CString::new(json_str).unwrap();
            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_count(
    coll: *mut CCollection,
    count_out: *mut usize,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_ref = unsafe { &(*coll).inner };

    match coll_ref.count() {
        Ok(count) => {
            if !count_out.is_null() {
                unsafe { *count_out = count; }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_name(
    coll: *mut CCollection,
    name_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_ref = unsafe { &(*coll).inner };
    let name = coll_ref.name();

    if !name_out.is_null() {
        let c_str = CString::new(name).unwrap();
        unsafe { *name_out = c_str.into_raw(); }
    }
    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    0
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_count_with_query(
    coll: *mut CCollection,
    query: *const c_char,
    count_out: *mut usize,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null collection pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let coll_ref = unsafe { &(*coll).inner };

    let query_option = if query.is_null() {
        None
    } else {
        match unsafe { c_str_to_string(query) } {
            Ok(s) => Some(s),
            Err(e) => {
                if !error_out.is_null() {
                    unsafe { *error_out = e; }
                }
                return -1;
            }
        }
    };

    match coll_ref.count_with_query(query_option.as_deref()) {
        Ok(count) => {
            if !count_out.is_null() {
                unsafe { *count_out = count; }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

// Typed method variants - in FFI these use JSON but provide API parity
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_insert_typed(
    coll: *mut CCollection,
    json: *const c_char,
    id_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    jasonisnthappy_collection_insert(coll, json, id_out, error_out)
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_insert_many_typed(
    coll: *mut CCollection,
    docs_json: *const c_char,
    ids_json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    jasonisnthappy_collection_insert_many(coll, docs_json, ids_json_out, error_out)
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_find_by_id_typed(
    coll: *mut CCollection,
    id: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    jasonisnthappy_collection_find_by_id(coll, id, json_out, error_out)
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_find_all_typed(
    coll: *mut CCollection,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    jasonisnthappy_collection_find_all(coll, json_out, error_out)
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_find_typed(
    coll: *mut CCollection,
    query: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    jasonisnthappy_collection_find(coll, query, json_out, error_out)
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_find_one_typed(
    coll: *mut CCollection,
    query: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    jasonisnthappy_collection_find_one(coll, query, json_out, error_out)
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_update_by_id_typed(
    coll: *mut CCollection,
    id: *const c_char,
    updates_json: *const c_char,
    error_out: *mut CError,
) -> i32 {
    jasonisnthappy_collection_update_by_id(coll, id, updates_json, error_out)
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_update_typed(
    coll: *mut CCollection,
    query: *const c_char,
    updates_json: *const c_char,
    count_out: *mut usize,
    error_out: *mut CError,
) -> i32 {
    jasonisnthappy_collection_update(coll, query, updates_json, count_out, error_out)
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_update_one_typed(
    coll: *mut CCollection,
    query: *const c_char,
    updates_json: *const c_char,
    updated_out: *mut bool,
    error_out: *mut CError,
) -> i32 {
    jasonisnthappy_collection_update_one(coll, query, updates_json, updated_out, error_out)
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_upsert_by_id_typed(
    coll: *mut CCollection,
    id: *const c_char,
    json: *const c_char,
    result_out: *mut i32,
    id_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    jasonisnthappy_collection_upsert_by_id(coll, id, json, result_out, id_out, error_out)
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_upsert_typed(
    coll: *mut CCollection,
    query: *const c_char,
    json: *const c_char,
    result_out: *mut i32,
    id_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    jasonisnthappy_collection_upsert(coll, query, json, result_out, id_out, error_out)
}

// ============================================================================
// Query Builder Helpers
// ============================================================================

/// Query with all options in a single call (simplified query builder for FFI)
///
/// # Parameters
/// - filter: Optional query filter string (NULL = no filter)
/// - sort_field: Optional field to sort by (NULL = no sort)
/// - sort_ascending: true for ascending, false for descending
/// - limit: Max results (0 = no limit)
/// - skip: Skip N results (0 = no skip)
/// - project_json: Optional JSON array of fields to include (NULL = all fields)
/// - exclude_json: Optional JSON array of fields to exclude (NULL = none)
///
/// Note: Cannot specify both project_json and exclude_json
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_query_with_options(
    coll: *mut CCollection,
    filter: *const c_char,
    sort_field: *const c_char,
    sort_ascending: bool,
    limit: usize,
    skip: usize,
    project_json: *const c_char,
    exclude_json: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() || json_out.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    unsafe {
        let collection = &(*coll).inner;

        // Start building the query
        let mut query_builder = collection.query();

        // Add filter if provided
        if !filter.is_null() {
            match CStr::from_ptr(filter).to_str() {
                Ok(filter_str) => {
                    query_builder = query_builder.filter(filter_str);
                }
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!("Invalid filter UTF-8: {}", e))
                                .unwrap()
                                .into_raw(),
                        };
                    }
                    return -1;
                }
            }
        }

        // Add sorting if provided
        if !sort_field.is_null() {
            match CStr::from_ptr(sort_field).to_str() {
                Ok(field_str) => {
                    let order = if sort_ascending {
                        SortOrder::Asc
                    } else {
                        SortOrder::Desc
                    };
                    query_builder = query_builder.sort_by(field_str, order);
                }
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!("Invalid sort_field UTF-8: {}", e))
                                .unwrap()
                                .into_raw(),
                        };
                    }
                    return -1;
                }
            }
        }

        // Add limit if non-zero
        if limit > 0 {
            query_builder = query_builder.limit(limit);
        }

        // Add skip if non-zero
        if skip > 0 {
            query_builder = query_builder.skip(skip);
        }

        // Add projection if provided
        if !project_json.is_null() && !exclude_json.is_null() {
            if !error_out.is_null() {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Cannot specify both project_json and exclude_json")
                        .unwrap()
                        .into_raw(),
                };
            }
            return -1;
        }

        if !project_json.is_null() {
            match CStr::from_ptr(project_json).to_str() {
                Ok(proj_str) => {
                    match serde_json::from_str::<Vec<String>>(proj_str) {
                        Ok(fields) => {
                            let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                            query_builder = query_builder.project(&field_refs);
                        }
                        Err(e) => {
                            if !error_out.is_null() {
                                *error_out = CError {
                                    code: -1,
                                    message: CString::new(format!(
                                        "Invalid project_json format (expected JSON array): {}",
                                        e
                                    ))
                                    .unwrap()
                                    .into_raw(),
                                };
                            }
                            return -1;
                        }
                    }
                }
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!("Invalid project_json UTF-8: {}", e))
                                .unwrap()
                                .into_raw(),
                        };
                    }
                    return -1;
                }
            }
        }

        if !exclude_json.is_null() {
            match CStr::from_ptr(exclude_json).to_str() {
                Ok(excl_str) => {
                    match serde_json::from_str::<Vec<String>>(excl_str) {
                        Ok(fields) => {
                            let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                            query_builder = query_builder.exclude(&field_refs);
                        }
                        Err(e) => {
                            if !error_out.is_null() {
                                *error_out = CError {
                                    code: -1,
                                    message: CString::new(format!(
                                        "Invalid exclude_json format (expected JSON array): {}",
                                        e
                                    ))
                                    .unwrap()
                                    .into_raw(),
                                };
                            }
                            return -1;
                        }
                    }
                }
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!("Invalid exclude_json UTF-8: {}", e))
                                .unwrap()
                                .into_raw(),
                        };
                    }
                    return -1;
                }
            }
        }

        // Execute the query
        match query_builder.execute() {
            Ok(docs) => {
                match serde_json::to_string(&docs) {
                    Ok(json_str) => {
                        *json_out = CString::new(json_str).unwrap().into_raw();
                        0
                    }
                    Err(e) => {
                        if !error_out.is_null() {
                            *error_out = CError {
                                code: -1,
                                message: CString::new(format!("Failed to serialize results: {}", e))
                                    .unwrap()
                                    .into_raw(),
                            };
                        }
                        -1
                    }
                }
            }
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Query failed: {}", e))
                            .unwrap()
                            .into_raw(),
                    };
                }
                -1
            }
        }
    }
}

/// Query and count results (no fetch)
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_query_count(
    coll: *mut CCollection,
    filter: *const c_char,
    skip: usize,
    limit: usize,
    count_out: *mut usize,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() || count_out.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    unsafe {
        let collection = &(*coll).inner;

        let mut query_builder = collection.query();

        // Add filter if provided
        if !filter.is_null() {
            match CStr::from_ptr(filter).to_str() {
                Ok(filter_str) => {
                    query_builder = query_builder.filter(filter_str);
                }
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!("Invalid filter UTF-8: {}", e))
                                .unwrap()
                                .into_raw(),
                        };
                    }
                    return -1;
                }
            }
        }

        // Add skip/limit if provided
        if skip > 0 {
            query_builder = query_builder.skip(skip);
        }
        if limit > 0 {
            query_builder = query_builder.limit(limit);
        }

        // Execute count
        match query_builder.count() {
            Ok(count) => {
                *count_out = count;
                0
            }
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Count failed: {}", e))
                            .unwrap()
                            .into_raw(),
                    };
                }
                -1
            }
        }
    }
}

/// Query and return first result
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_query_first(
    coll: *mut CCollection,
    filter: *const c_char,
    sort_field: *const c_char,
    sort_ascending: bool,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() || json_out.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    unsafe {
        let collection = &(*coll).inner;

        let mut query_builder = collection.query();

        // Add filter if provided
        if !filter.is_null() {
            match CStr::from_ptr(filter).to_str() {
                Ok(filter_str) => {
                    query_builder = query_builder.filter(filter_str);
                }
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!("Invalid filter UTF-8: {}", e))
                                .unwrap()
                                .into_raw(),
                        };
                    }
                    return -1;
                }
            }
        }

        // Add sorting if provided
        if !sort_field.is_null() {
            match CStr::from_ptr(sort_field).to_str() {
                Ok(field_str) => {
                    let order = if sort_ascending {
                        SortOrder::Asc
                    } else {
                        SortOrder::Desc
                    };
                    query_builder = query_builder.sort_by(field_str, order);
                }
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!("Invalid sort_field UTF-8: {}", e))
                                .unwrap()
                                .into_raw(),
                        };
                    }
                    return -1;
                }
            }
        }

        // Execute first()
        match query_builder.first() {
            Ok(Some(doc)) => {
                match serde_json::to_string(&doc) {
                    Ok(json_str) => {
                        *json_out = CString::new(json_str).unwrap().into_raw();
                        0
                    }
                    Err(e) => {
                        if !error_out.is_null() {
                            *error_out = CError {
                                code: -1,
                                message: CString::new(format!("Failed to serialize result: {}", e))
                                    .unwrap()
                                    .into_raw(),
                            };
                        }
                        -1
                    }
                }
            }
            Ok(None) => {
                // No results - return null string
                *json_out = ptr::null_mut();
                0
            }
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Query failed: {}", e))
                            .unwrap()
                            .into_raw(),
                    };
                }
                -1
            }
        }
    }
}

// ============================================================================
// Bulk Write Operations
// ============================================================================

/// Execute bulk write operations in a single transaction
///
/// # Parameters
/// - operations_json: JSON array of operations, each with:
///   - "op": "insert" | "update_one" | "update_many" | "delete_one" | "delete_many"
///   - "doc": document (for insert)
///   - "query": query string (for update/delete)
///   - "updates": updates object (for update)
/// - ordered: if true, stop on first error; if false, continue on errors
/// - result_json_out: BulkWriteResult as JSON (inserted_count, updated_count, deleted_count, errors)
///
/// # Example operations_json:
/// ```json
/// [
///   {"op": "insert", "doc": {"name": "Alice", "age": 30}},
///   {"op": "update_one", "query": "name is 'Bob'", "updates": {"age": 31}},
///   {"op": "delete_many", "query": "age < 18"}
/// ]
/// ```
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_bulk_write(
    coll: *mut CCollection,
    operations_json: *const c_char,
    ordered: bool,
    result_json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() || operations_json.is_null() || result_json_out.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    unsafe {
        let collection = &(*coll).inner;

        // Parse operations JSON
        let ops_str = match CStr::from_ptr(operations_json).to_str() {
            Ok(s) => s,
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid operations_json UTF-8: {}", e))
                            .unwrap()
                            .into_raw(),
                    };
                }
                return -1;
            }
        };

        let operations: Vec<Value> = match serde_json::from_str(ops_str) {
            Ok(ops) => ops,
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!(
                            "Invalid operations_json format (expected JSON array): {}",
                            e
                        ))
                        .unwrap()
                        .into_raw(),
                    };
                }
                return -1;
            }
        };

        // Build bulk write
        let mut bulk = collection.bulk_write();
        bulk = bulk.ordered(ordered);

        for (index, operation) in operations.iter().enumerate() {
            let op_obj = match operation.as_object() {
                Some(obj) => obj,
                None => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!(
                                "Operation at index {} is not an object",
                                index
                            ))
                            .unwrap()
                            .into_raw(),
                        };
                    }
                    return -1;
                }
            };

            let op_type = match op_obj.get("op").and_then(|v| v.as_str()) {
                Some(t) => t,
                None => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!(
                                "Operation at index {} missing 'op' field",
                                index
                            ))
                            .unwrap()
                            .into_raw(),
                        };
                    }
                    return -1;
                }
            };

            match op_type {
                "insert" => {
                    let doc = match op_obj.get("doc") {
                        Some(d) => d.clone(),
                        None => {
                            if !error_out.is_null() {
                                *error_out = CError {
                                    code: -1,
                                    message: CString::new(format!(
                                        "Insert operation at index {} missing 'doc' field",
                                        index
                                    ))
                                    .unwrap()
                                    .into_raw(),
                                };
                            }
                            return -1;
                        }
                    };
                    bulk = bulk.insert(doc);
                }
                "update_one" => {
                    let query = match op_obj.get("query").and_then(|v| v.as_str()) {
                        Some(q) => q,
                        None => {
                            if !error_out.is_null() {
                                *error_out = CError {
                                    code: -1,
                                    message: CString::new(format!(
                                        "update_one at index {} missing 'query' field",
                                        index
                                    ))
                                    .unwrap()
                                    .into_raw(),
                                };
                            }
                            return -1;
                        }
                    };
                    let updates = match op_obj.get("updates") {
                        Some(u) => u.clone(),
                        None => {
                            if !error_out.is_null() {
                                *error_out = CError {
                                    code: -1,
                                    message: CString::new(format!(
                                        "update_one at index {} missing 'updates' field",
                                        index
                                    ))
                                    .unwrap()
                                    .into_raw(),
                                };
                            }
                            return -1;
                        }
                    };
                    bulk = bulk.update_one(query, updates);
                }
                "update_many" => {
                    let query = match op_obj.get("query").and_then(|v| v.as_str()) {
                        Some(q) => q,
                        None => {
                            if !error_out.is_null() {
                                *error_out = CError {
                                    code: -1,
                                    message: CString::new(format!(
                                        "update_many at index {} missing 'query' field",
                                        index
                                    ))
                                    .unwrap()
                                    .into_raw(),
                                };
                            }
                            return -1;
                        }
                    };
                    let updates = match op_obj.get("updates") {
                        Some(u) => u.clone(),
                        None => {
                            if !error_out.is_null() {
                                *error_out = CError {
                                    code: -1,
                                    message: CString::new(format!(
                                        "update_many at index {} missing 'updates' field",
                                        index
                                    ))
                                    .unwrap()
                                    .into_raw(),
                                };
                            }
                            return -1;
                        }
                    };
                    bulk = bulk.update_many(query, updates);
                }
                "delete_one" => {
                    let query = match op_obj.get("query").and_then(|v| v.as_str()) {
                        Some(q) => q,
                        None => {
                            if !error_out.is_null() {
                                *error_out = CError {
                                    code: -1,
                                    message: CString::new(format!(
                                        "delete_one at index {} missing 'query' field",
                                        index
                                    ))
                                    .unwrap()
                                    .into_raw(),
                                };
                            }
                            return -1;
                        }
                    };
                    bulk = bulk.delete_one(query);
                }
                "delete_many" => {
                    let query = match op_obj.get("query").and_then(|v| v.as_str()) {
                        Some(q) => q,
                        None => {
                            if !error_out.is_null() {
                                *error_out = CError {
                                    code: -1,
                                    message: CString::new(format!(
                                        "delete_many at index {} missing 'query' field",
                                        index
                                    ))
                                    .unwrap()
                                    .into_raw(),
                                };
                            }
                            return -1;
                        }
                    };
                    bulk = bulk.delete_many(query);
                }
                _ => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!(
                                "Unknown operation type '{}' at index {}",
                                op_type, index
                            ))
                            .unwrap()
                            .into_raw(),
                        };
                    }
                    return -1;
                }
            }
        }

        // Execute bulk write
        match bulk.execute() {
            Ok(result) => {
                // Convert result to JSON
                let result_json = serde_json::json!({
                    "inserted_count": result.inserted_count,
                    "updated_count": result.updated_count,
                    "deleted_count": result.deleted_count,
                    "errors": result.errors.iter().map(|e| {
                        serde_json::json!({
                            "operation_index": e.operation_index,
                            "message": &e.message
                        })
                    }).collect::<Vec<_>>()
                });

                match serde_json::to_string(&result_json) {
                    Ok(json_str) => {
                        *result_json_out = CString::new(json_str).unwrap().into_raw();
                        0
                    }
                    Err(e) => {
                        if !error_out.is_null() {
                            *error_out = CError {
                                code: -1,
                                message: CString::new(format!("Failed to serialize result: {}", e))
                                    .unwrap()
                                    .into_raw(),
                            };
                        }
                        -1
                    }
                }
            }
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Bulk write failed: {}", e))
                            .unwrap()
                            .into_raw(),
                    };
                }
                -1
            }
        }
    }
}

// ============================================================================
// Aggregation Pipeline
// ============================================================================

/// Execute an aggregation pipeline
///
/// # Parameters
/// - pipeline_json: JSON array of pipeline stages, each with:
///   - "match": query string (filter stage)
///   - "group_by": {field: "...", accumulators: [{type: "count|sum|avg|min|max", output_field: "...", field: "..."}]}
///   - "sort": {field: "...", ascending: true|false}
///   - "limit": number
///   - "skip": number
///   - "project": ["field1", "field2", ...]
///   - "exclude": ["field1", "field2", ...]
///
/// # Example pipeline_json:
/// ```json
/// [
///   {"match": "status is 'active'"},
///   {"group_by": {"field": "city", "accumulators": [
///     {"type": "count", "output_field": "total"},
///     {"type": "sum", "field": "amount", "output_field": "total_amount"}
///   ]}},
///   {"sort": {"field": "total", "ascending": false}},
///   {"limit": 10}
/// ]
/// ```
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_aggregate(
    coll: *mut CCollection,
    pipeline_json: *const c_char,
    result_json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() || pipeline_json.is_null() || result_json_out.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    unsafe {
        let collection = &(*coll).inner;

        // Parse pipeline JSON
        let pipeline_str = match CStr::from_ptr(pipeline_json).to_str() {
            Ok(s) => s,
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Invalid pipeline_json UTF-8: {}", e))
                            .unwrap()
                            .into_raw(),
                    };
                }
                return -1;
            }
        };

        let stages: Vec<Value> = match serde_json::from_str(pipeline_str) {
            Ok(stages) => stages,
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!(
                            "Invalid pipeline_json format (expected JSON array): {}",
                            e
                        ))
                        .unwrap()
                        .into_raw(),
                    };
                }
                return -1;
            }
        };

        // Build aggregation pipeline
        let mut pipeline = collection.aggregate();

        for (index, stage) in stages.iter().enumerate() {
            let stage_obj = match stage.as_object() {
                Some(obj) => obj,
                None => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!(
                                "Stage at index {} is not an object",
                                index
                            ))
                            .unwrap()
                            .into_raw(),
                        };
                    }
                    return -1;
                }
            };

            // Match stage
            if let Some(query) = stage_obj.get("match").and_then(|v| v.as_str()) {
                pipeline = pipeline.match_(query);
            }
            // Group by stage
            else if let Some(group) = stage_obj.get("group_by") {
                let group_obj = match group.as_object() {
                    Some(obj) => obj,
                    None => {
                        if !error_out.is_null() {
                            *error_out = CError {
                                code: -1,
                                message: CString::new(format!(
                                    "group_by at index {} must be an object",
                                    index
                                ))
                                .unwrap()
                                .into_raw(),
                            };
                        }
                        return -1;
                    }
                };

                let field = match group_obj.get("field").and_then(|v| v.as_str()) {
                    Some(f) => f,
                    None => {
                        if !error_out.is_null() {
                            *error_out = CError {
                                code: -1,
                                message: CString::new(format!(
                                    "group_by at index {} missing 'field'",
                                    index
                                ))
                                .unwrap()
                                .into_raw(),
                            };
                        }
                        return -1;
                    }
                };

                pipeline = pipeline.group_by(field);

                // Process accumulators
                if let Some(accumulators) = group_obj.get("accumulators").and_then(|v| v.as_array()) {
                    for acc in accumulators {
                        let acc_obj = match acc.as_object() {
                            Some(obj) => obj,
                            None => continue,
                        };

                        let acc_type = match acc_obj.get("type").and_then(|v| v.as_str()) {
                            Some(t) => t,
                            None => continue,
                        };

                        let output_field = match acc_obj.get("output_field").and_then(|v| v.as_str()) {
                            Some(f) => f,
                            None => continue,
                        };

                        match acc_type {
                            "count" => {
                                pipeline = pipeline.count(output_field);
                            }
                            "sum" => {
                                if let Some(field) = acc_obj.get("field").and_then(|v| v.as_str()) {
                                    pipeline = pipeline.sum(field, output_field);
                                }
                            }
                            "avg" => {
                                if let Some(field) = acc_obj.get("field").and_then(|v| v.as_str()) {
                                    pipeline = pipeline.avg(field, output_field);
                                }
                            }
                            "min" => {
                                if let Some(field) = acc_obj.get("field").and_then(|v| v.as_str()) {
                                    pipeline = pipeline.min(field, output_field);
                                }
                            }
                            "max" => {
                                if let Some(field) = acc_obj.get("field").and_then(|v| v.as_str()) {
                                    pipeline = pipeline.max(field, output_field);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            // Sort stage
            else if let Some(sort) = stage_obj.get("sort") {
                let sort_obj = match sort.as_object() {
                    Some(obj) => obj,
                    None => {
                        if !error_out.is_null() {
                            *error_out = CError {
                                code: -1,
                                message: CString::new(format!(
                                    "sort at index {} must be an object",
                                    index
                                ))
                                .unwrap()
                                .into_raw(),
                            };
                        }
                        return -1;
                    }
                };

                let field = match sort_obj.get("field").and_then(|v| v.as_str()) {
                    Some(f) => f,
                    None => {
                        if !error_out.is_null() {
                            *error_out = CError {
                                code: -1,
                                message: CString::new(format!(
                                    "sort at index {} missing 'field'",
                                    index
                                ))
                                .unwrap()
                                .into_raw(),
                            };
                        }
                        return -1;
                    }
                };

                let ascending = sort_obj.get("ascending").and_then(|v| v.as_bool()).unwrap_or(true);
                pipeline = pipeline.sort(field, ascending);
            }
            // Limit stage
            else if let Some(limit) = stage_obj.get("limit").and_then(|v| v.as_u64()) {
                pipeline = pipeline.limit(limit as usize);
            }
            // Skip stage
            else if let Some(skip) = stage_obj.get("skip").and_then(|v| v.as_u64()) {
                pipeline = pipeline.skip(skip as usize);
            }
            // Project stage
            else if let Some(project) = stage_obj.get("project").and_then(|v| v.as_array()) {
                let fields: Vec<String> = project
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                pipeline = pipeline.project(&field_refs);
            }
            // Exclude stage
            else if let Some(exclude) = stage_obj.get("exclude").and_then(|v| v.as_array()) {
                let fields: Vec<String> = exclude
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
                pipeline = pipeline.exclude(&field_refs);
            } else {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!(
                            "Unknown or invalid stage at index {}",
                            index
                        ))
                        .unwrap()
                        .into_raw(),
                    };
                }
                return -1;
            }
        }

        // Execute the pipeline
        match pipeline.execute() {
            Ok(results) => {
                match serde_json::to_string(&results) {
                    Ok(json_str) => {
                        *result_json_out = CString::new(json_str).unwrap().into_raw();
                        0
                    }
                    Err(e) => {
                        if !error_out.is_null() {
                            *error_out = CError {
                                code: -1,
                                message: CString::new(format!("Failed to serialize results: {}", e))
                                    .unwrap()
                                    .into_raw(),
                            };
                        }
                        -1
                    }
                }
            }
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Aggregation failed: {}", e))
                            .unwrap()
                            .into_raw(),
                    };
                }
                -1
            }
        }
    }
}

// ============================================================================
// Watch / Change Streams
// ============================================================================

/// Start watching a collection for changes
///
/// Creates a background thread that monitors changes to the collection and calls
/// the provided callback function for each change event.
///
/// # Parameters
/// - coll: Collection to watch
/// - filter: Optional query filter (NULL = watch all changes)
/// - callback: Function to call for each change event
/// - user_data: Optional user context pointer passed to callback
/// - handle_out: Output pointer for the watch handle (use to stop watching)
///
/// # Returns
/// 0 on success, -1 on error
///
/// # Safety
/// The callback will be called from a background thread. Ensure thread safety.
/// Call jasonisnthappy_watch_stop() to stop watching and clean up the thread.
#[no_mangle]
pub extern "C" fn jasonisnthappy_collection_watch_start(
    coll: *mut CCollection,
    filter: *const c_char,
    callback: WatchCallback,
    user_data: *mut std::os::raw::c_void,
    handle_out: *mut *mut CWatchHandle,
    error_out: *mut CError,
) -> i32 {
    if coll.is_null() || handle_out.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    unsafe {
        let collection = &(*coll).inner;

        // Build watch
        let mut watch_builder = collection.watch();

        // Add filter if provided
        if !filter.is_null() {
            match CStr::from_ptr(filter).to_str() {
                Ok(filter_str) => {
                    watch_builder = watch_builder.filter(filter_str);
                }
                Err(e) => {
                    if !error_out.is_null() {
                        *error_out = CError {
                            code: -1,
                            message: CString::new(format!("Invalid filter UTF-8: {}", e))
                                .unwrap()
                                .into_raw(),
                        };
                    }
                    return -1;
                }
            }
        }

        // Subscribe to changes
        match watch_builder.subscribe() {
            Ok((watch_handle, receiver)) => {
                let stop_flag = Arc::new(AtomicBool::new(false));
                let stop_flag_clone = stop_flag.clone();

                // Wrap callback and user_data to make them Send-able (caller ensures thread safety)
                let context = SendableCallbackContext {
                    callback,
                    user_data_addr: user_data as usize,
                };

                // Spawn thread to handle events
                let thread_handle = thread::spawn(move || {
                    while !stop_flag_clone.load(Ordering::Relaxed) {
                        match receiver.recv_timeout(std::time::Duration::from_millis(100)) {
                            Ok(event) => {
                                // Convert event to C strings
                                let collection_cstr = match CString::new(event.collection.as_str()) {
                                    Ok(s) => s,
                                    Err(_) => continue,
                                };

                                let operation_str = match event.operation {
                                    ChangeOperation::Insert => "insert",
                                    ChangeOperation::Update => "update",
                                    ChangeOperation::Delete => "delete",
                                };

                                let operation_cstr = CString::new(operation_str).unwrap();
                                let doc_id_cstr = match CString::new(event.doc_id.as_str()) {
                                    Ok(s) => s,
                                    Err(_) => continue,
                                };

                                let doc_json_cstr = if let Some(doc) = &event.document {
                                    match serde_json::to_string(doc) {
                                        Ok(json_str) => match CString::new(json_str) {
                                            Ok(s) => Some(s),
                                            Err(_) => None,
                                        },
                                        Err(_) => None,
                                    }
                                } else {
                                    None
                                };

                                // Call the callback
                                (context.callback)(
                                    collection_cstr.as_ptr(),
                                    operation_cstr.as_ptr(),
                                    doc_id_cstr.as_ptr(),
                                    doc_json_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
                                    context.user_data_addr as *mut std::os::raw::c_void,
                                );
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                // Continue waiting
                                continue;
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                // Channel closed, exit thread
                                break;
                            }
                        }
                    }
                });

                // Create handle
                let handle = Box::new(CWatchHandle {
                    _watch_handle: watch_handle,
                    stop_flag,
                    thread_handle: Some(thread_handle),
                });

                *handle_out = Box::into_raw(handle);
                0
            }
            Err(e) => {
                if !error_out.is_null() {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Failed to start watching: {}", e))
                            .unwrap()
                            .into_raw(),
                    };
                }
                -1
            }
        }
    }
}

/// Stop watching and clean up resources
///
/// Signals the background thread to stop and waits for it to finish.
/// After calling this, the handle pointer is no longer valid.
///
/// # Parameters
/// - handle: Watch handle returned by jasonisnthappy_collection_watch_start
///
/// # Safety
/// The handle must have been created by jasonisnthappy_collection_watch_start.
/// Do not use the handle after calling this function.
#[no_mangle]
pub extern "C" fn jasonisnthappy_watch_stop(handle: *mut CWatchHandle) {
    if !handle.is_null() {
        unsafe {
            let mut watch_handle = Box::from_raw(handle);

            // Signal thread to stop
            watch_handle.stop_flag.store(true, Ordering::Relaxed);

            // Wait for thread to finish
            if let Some(thread_handle) = watch_handle.thread_handle.take() {
                let _ = thread_handle.join();
            }

            // Box is dropped here, cleaning up the WatchHandle (which unsubscribes)
        }
    }
}

// ============================================================================
// Maintenance & Monitoring
// ============================================================================

#[no_mangle]
pub extern "C" fn jasonisnthappy_checkpoint(
    db: *mut CDatabase,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let db_ref = unsafe { &(*db).inner };

    match db_ref.checkpoint() {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_backup(
    db: *mut CDatabase,
    backup_path: *const c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let path_str = match unsafe { c_str_to_string(backup_path) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    let db_ref = unsafe { &(*db).inner };

    match db_ref.backup(&path_str) {
        Ok(_) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_verify_backup(
    db: *mut CDatabase,
    backup_path: *const c_char,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let path_str = match unsafe { c_str_to_string(backup_path) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return -1;
        }
    };

    match Database::verify_backup(&path_str) {
        Ok(backup_info) => {
            let json_obj = serde_json::json!({
                "version": backup_info.version,
                "num_pages": backup_info.num_pages,
                "num_collections": backup_info.num_collections,
                "file_size": backup_info.file_size,
            });
            let json_str = serde_json::to_string(&json_obj).unwrap_or_else(|_| "{}".to_string());
            let c_str = CString::new(json_str).unwrap();

            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_garbage_collect(
    db: *mut CDatabase,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let db_ref = unsafe { &(*db).inner };

    match db_ref.garbage_collect() {
        Ok(stats) => {
            let json_obj = serde_json::json!({
                "versions_removed": stats.versions_removed,
                "pages_freed": stats.pages_freed,
                "bytes_freed": stats.bytes_freed,
            });
            let json_str = serde_json::to_string(&json_obj).unwrap_or_else(|_| "{}".to_string());
            let c_str = CString::new(json_str).unwrap();

            if !json_out.is_null() {
                unsafe { *json_out = c_str.into_raw(); }
            }
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            0
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::from_error(e); }
            }
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_metrics(
    db: *mut CDatabase,
    json_out: *mut *mut c_char,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let db_ref = unsafe { &(*db).inner };
    let metrics = db_ref.metrics();

    let json_str = serde_json::to_string(&metrics).unwrap_or_else(|_| "{}".to_string());
    let c_str = CString::new(json_str).unwrap();

    if !json_out.is_null() {
        unsafe { *json_out = c_str.into_raw(); }
    }
    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    0
}

#[no_mangle]
pub extern "C" fn jasonisnthappy_frame_count(
    db: *mut CDatabase,
    count_out: *mut u64,
    error_out: *mut CError,
) -> i32 {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return -1;
    }

    let db_ref = unsafe { &(*db).inner };
    let count = db_ref.frame_count();

    if !count_out.is_null() {
        unsafe { *count_out = count; }
    }
    if !error_out.is_null() {
        unsafe { *error_out = CError::success(); }
    }
    0
}

// ============================================================================
// Web Server (when web-ui feature is enabled)
// ============================================================================

#[cfg(feature = "web-ui")]
#[no_mangle]
pub extern "C" fn jasonisnthappy_start_web_server(
    db: *mut CDatabase,
    addr: *const c_char,
    error_out: *mut CError,
) -> *mut CWebServer {
    if db.is_null() {
        if !error_out.is_null() {
            unsafe {
                *error_out = CError {
                    code: -1,
                    message: CString::new("Null database pointer").unwrap().into_raw(),
                };
            }
        }
        return ptr::null_mut();
    }

    let addr_str = match unsafe { c_str_to_string(addr) } {
        Ok(s) => s,
        Err(e) => {
            if !error_out.is_null() {
                unsafe { *error_out = e; }
            }
            return ptr::null_mut();
        }
    };

    let db_ref = unsafe { &(*db).inner };
    let db_arc = Arc::clone(db_ref);

    match WebServer::start(db_arc, &addr_str) {
        Ok(server) => {
            if !error_out.is_null() {
                unsafe { *error_out = CError::success(); }
            }
            Box::into_raw(Box::new(CWebServer { inner: Some(server) }))
        }
        Err(e) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = CError {
                        code: -1,
                        message: CString::new(format!("Failed to start web server: {}", e))
                            .unwrap().into_raw(),
                    };
                }
            }
            ptr::null_mut()
        }
    }
}

#[cfg(feature = "web-ui")]
#[no_mangle]
pub extern "C" fn jasonisnthappy_stop_web_server(server: *mut CWebServer) {
    if !server.is_null() {
        unsafe {
            let mut server_obj = Box::from_raw(server);
            if let Some(s) = server_obj.inner.take() {
                s.shutdown();
            }
        }
    }
}
