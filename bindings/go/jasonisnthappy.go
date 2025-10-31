package jasonisnthappy

//go:generate go run download_static.go

/*
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/lib/darwin-arm64
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/lib/darwin-amd64
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/lib/linux-arm64
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/lib/linux-amd64
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/lib/windows-amd64

#cgo darwin LDFLAGS: -framework Security -framework CoreFoundation -ljasonisnthappy
#cgo linux LDFLAGS: -ljasonisnthappy -lm -ldl -lpthread
#cgo windows LDFLAGS: -ljasonisnthappy -lws2_32 -luserenv -lbcrypt

#include <stdlib.h>
#include "jasonisnthappy.h"

// Forward declaration for the watch callback bridge
extern void goWatchCallbackBridge(char *collection, char *operation, char *doc_id, char *doc_json, void *user_data);
*/
import "C"
import (
	"encoding/json"
	"sync"
	"unsafe"
)

// Error represents a jasonisnthappy error
type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string {
	return e.Message
}

// Watch callback infrastructure
var (
	watchCallbacks   = make(map[uintptr]WatchCallback)
	watchCallbacksMu sync.RWMutex
	nextCallbackID   uintptr = 1
)

// WatchCallback is called when a change event occurs
type WatchCallback func(collection, operation, docID, docJSON string)

//export goWatchCallbackBridge
func goWatchCallbackBridge(collection *C.char, operation *C.char, docID *C.char, docJSON *C.char, userData unsafe.Pointer) {
	callbackID := uintptr(userData)

	watchCallbacksMu.RLock()
	callback, exists := watchCallbacks[callbackID]
	watchCallbacksMu.RUnlock()

	if !exists {
		return
	}

	callback(
		C.GoString(collection),
		C.GoString(operation),
		C.GoString(docID),
		C.GoString(docJSON),
	)
}

// Database represents a jasonisnthappy database connection
type Database struct {
	db *C.CDatabase
}

// Transaction represents a database transaction
type Transaction struct {
	tx *C.CTransaction
}

// UpsertResult represents the result of an upsert operation
type UpsertResult struct {
	// ID is the document ID
	ID string
	// Inserted is true if a new document was inserted, false if an existing document was updated
	Inserted bool
}

// DatabaseOptions holds configuration for opening a database
type DatabaseOptions struct {
	CacheSize               uint   `json:"cache_size"`
	AutoCheckpointThreshold uint64 `json:"auto_checkpoint_threshold"`
	FilePermissions         uint32 `json:"file_permissions"`
	ReadOnly                bool   `json:"read_only"`
	MaxBulkOperations       uint   `json:"max_bulk_operations"`
	MaxDocumentSize         uint   `json:"max_document_size"`
	MaxRequestBodySize      uint   `json:"max_request_body_size"`
}

// DefaultDatabaseOptions returns the default database options
func DefaultDatabaseOptions() DatabaseOptions {
	cOpts := C.jasonisnthappy_default_database_options()
	return DatabaseOptions{
		CacheSize:               uint(cOpts.cache_size),
		AutoCheckpointThreshold: uint64(cOpts.auto_checkpoint_threshold),
		FilePermissions:         uint32(cOpts.file_permissions),
		ReadOnly:                bool(cOpts.read_only),
		MaxBulkOperations:       uint(cOpts.max_bulk_operations),
		MaxDocumentSize:         uint(cOpts.max_document_size),
		MaxRequestBodySize:      uint(cOpts.max_request_body_size),
	}
}

// Open opens a database at the given path
func Open(path string) (*Database, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var cErr C.CError
	db := C.jasonisnthappy_open(cPath, &cErr)

	if db == nil {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	return &Database{db: db}, nil
}

// OpenWithOptions opens a database with custom options
func OpenWithOptions(path string, opts DatabaseOptions) (*Database, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	cOpts := C.CDatabaseOptions{
		cache_size:               C.uintptr_t(opts.CacheSize),
		auto_checkpoint_threshold: C.ulonglong(opts.AutoCheckpointThreshold),
		file_permissions:         C.uint(opts.FilePermissions),
		read_only:                C.bool(opts.ReadOnly),
		max_bulk_operations:      C.uintptr_t(opts.MaxBulkOperations),
		max_document_size:        C.uintptr_t(opts.MaxDocumentSize),
		max_request_body_size:    C.uintptr_t(opts.MaxRequestBodySize),
	}

	var cErr C.CError
	db := C.jasonisnthappy_open_with_options(cPath, cOpts, &cErr)

	if db == nil {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	return &Database{db: db}, nil
}

// Close closes the database
func (d *Database) Close() {
	if d.db != nil {
		C.jasonisnthappy_close(d.db)
		d.db = nil
	}
}

// BeginTransaction starts a new transaction
func (d *Database) BeginTransaction() (*Transaction, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	var cErr C.CError
	tx := C.jasonisnthappy_begin_transaction(d.db, &cErr)

	if tx == nil {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	return &Transaction{tx: tx}, nil
}

// Commit commits the transaction
func (t *Transaction) Commit() error {
	if t.tx == nil {
		return &Error{Code: -1, Message: "Transaction is already closed"}
	}

	var cErr C.CError
	result := C.jasonisnthappy_commit(t.tx, &cErr)
	t.tx = nil // Transaction is consumed

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() {
	if t.tx != nil {
		C.jasonisnthappy_rollback(t.tx)
		t.tx = nil
	}
}

// Insert inserts a document into a collection
func (t *Transaction) Insert(collectionName string, doc interface{}) (string, error) {
	if t.tx == nil {
		return "", &Error{Code: -1, Message: "Transaction is closed"}
	}

	jsonBytes, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	cJSON := C.CString(string(jsonBytes))
	defer C.free(unsafe.Pointer(cJSON))

	var cID *C.char
	var cErr C.CError

	result := C.jasonisnthappy_insert(t.tx, cCollName, cJSON, &cID, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return "", err
	}

	id := C.GoString(cID)
	C.jasonisnthappy_free_string(cID)

	return id, nil
}

// FindByID finds a document by ID
func (t *Transaction) FindByID(collectionName, id string, result interface{}) (bool, error) {
	if t.tx == nil {
		return false, &Error{Code: -1, Message: "Transaction is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	cID := C.CString(id)
	defer C.free(unsafe.Pointer(cID))

	var cJSON *C.char
	var cErr C.CError

	status := C.jasonisnthappy_find_by_id(t.tx, cCollName, cID, &cJSON, &cErr)

	if status == -1 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return false, err
	}

	if status == 1 {
		// Not found
		return false, nil
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	if err := json.Unmarshal([]byte(jsonStr), result); err != nil {
		return false, err
	}

	return true, nil
}

// UpdateByID updates a document by ID
func (t *Transaction) UpdateByID(collectionName, id string, doc interface{}) error {
	if t.tx == nil {
		return &Error{Code: -1, Message: "Transaction is closed"}
	}

	jsonBytes, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	cID := C.CString(id)
	defer C.free(unsafe.Pointer(cID))

	cJSON := C.CString(string(jsonBytes))
	defer C.free(unsafe.Pointer(cJSON))

	var cErr C.CError
	result := C.jasonisnthappy_update_by_id(t.tx, cCollName, cID, cJSON, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// DeleteByID deletes a document by ID
func (t *Transaction) DeleteByID(collectionName, id string) error {
	if t.tx == nil {
		return &Error{Code: -1, Message: "Transaction is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	cID := C.CString(id)
	defer C.free(unsafe.Pointer(cID))

	var cErr C.CError
	result := C.jasonisnthappy_delete_by_id(t.tx, cCollName, cID, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// FindAll finds all documents in a collection
func (t *Transaction) FindAll(collectionName string, result interface{}) error {
	if t.tx == nil {
		return &Error{Code: -1, Message: "Transaction is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	var cJSON *C.char
	var cErr C.CError

	status := C.jasonisnthappy_find_all(t.tx, cCollName, &cJSON, &cErr)

	if status != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	if err := json.Unmarshal([]byte(jsonStr), result); err != nil {
		return err
	}

	return nil
}

// Helper function to convert C error to Go error
func cErrorToGoError(cErr *C.CError) error {
	if cErr.code == 0 {
		return nil
	}
	return &Error{
		Code:    int(cErr.code),
		Message: C.GoString(cErr.message),
	}
}

// ============================================================================
// Database Configuration
// ============================================================================

// TransactionConfig holds transaction retry configuration
type TransactionConfig struct {
	MaxRetries          uint   `json:"max_retries"`
	RetryBackoffBaseMs  uint64 `json:"retry_backoff_base_ms"`
	MaxRetryBackoffMs   uint64 `json:"max_retry_backoff_ms"`
}

// SetTransactionConfig sets the transaction configuration
func (d *Database) SetTransactionConfig(config TransactionConfig) error {
	if d.db == nil {
		return &Error{Code: -1, Message: "Database is closed"}
	}

	cConfig := C.CTransactionConfig{
		max_retries:          C.ulong(config.MaxRetries),
		retry_backoff_base_ms: C.ulonglong(config.RetryBackoffBaseMs),
		max_retry_backoff_ms:  C.ulonglong(config.MaxRetryBackoffMs),
	}

	var cErr C.CError
	result := C.jasonisnthappy_set_transaction_config(d.db, cConfig, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// GetTransactionConfig gets the transaction configuration
func (d *Database) GetTransactionConfig() (*TransactionConfig, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	var cConfig C.CTransactionConfig
	var cErr C.CError
	result := C.jasonisnthappy_get_transaction_config(d.db, &cConfig, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	return &TransactionConfig{
		MaxRetries:         uint(cConfig.max_retries),
		RetryBackoffBaseMs: uint64(cConfig.retry_backoff_base_ms),
		MaxRetryBackoffMs:  uint64(cConfig.max_retry_backoff_ms),
	}, nil
}

// SetAutoCheckpointThreshold sets the auto-checkpoint threshold
func (d *Database) SetAutoCheckpointThreshold(threshold uint64) error {
	if d.db == nil {
		return &Error{Code: -1, Message: "Database is closed"}
	}

	var cErr C.CError
	result := C.jasonisnthappy_set_auto_checkpoint_threshold(d.db, C.ulonglong(threshold), &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// ============================================================================
// Database Info & Stats
// ============================================================================

// GetPath returns the database file path
func (d *Database) GetPath() (string, error) {
	if d.db == nil {
		return "", &Error{Code: -1, Message: "Database is closed"}
	}

	var cPath *C.char
	var cErr C.CError
	result := C.jasonisnthappy_get_path(d.db, &cPath, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return "", err
	}

	path := C.GoString(cPath)
	C.jasonisnthappy_free_string(cPath)

	return path, nil
}

// IsReadOnly returns whether the database is read-only
func (d *Database) IsReadOnly() (bool, error) {
	if d.db == nil {
		return false, &Error{Code: -1, Message: "Database is closed"}
	}

	var cErr C.CError
	result := C.jasonisnthappy_is_read_only(d.db, &cErr)

	if result == -1 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return false, err
	}

	return result == 1, nil
}

// MaxBulkOperations returns the maximum number of bulk operations allowed
func (d *Database) MaxBulkOperations() (uint, error) {
	if d.db == nil {
		return 0, &Error{Code: -1, Message: "Database is closed"}
	}

	var cErr C.CError
	result := C.jasonisnthappy_max_bulk_operations(d.db, &cErr)

	return uint(result), nil
}

// MaxDocumentSize returns the maximum document size in bytes
func (d *Database) MaxDocumentSize() (uint, error) {
	if d.db == nil {
		return 0, &Error{Code: -1, Message: "Database is closed"}
	}

	var cErr C.CError
	result := C.jasonisnthappy_max_document_size(d.db, &cErr)

	return uint(result), nil
}

// MaxRequestBodySize returns the maximum HTTP request body size in bytes
func (d *Database) MaxRequestBodySize() (uint, error) {
	if d.db == nil {
		return 0, &Error{Code: -1, Message: "Database is closed"}
	}

	var cErr C.CError
	result := C.jasonisnthappy_max_request_body_size(d.db, &cErr)

	return uint(result), nil
}

// ListCollections returns a list of all collection names
func (d *Database) ListCollections() ([]string, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	var cJSON *C.char
	var cErr C.CError
	result := C.jasonisnthappy_list_collections(d.db, &cJSON, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	var collections []string
	if err := json.Unmarshal([]byte(jsonStr), &collections); err != nil {
		return nil, err
	}

	return collections, nil
}

// CollectionStats returns statistics for a collection
func (d *Database) CollectionStats(collectionName string) (map[string]interface{}, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	var cJSON *C.char
	var cErr C.CError
	result := C.jasonisnthappy_collection_stats(d.db, cCollName, &cJSON, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	var stats map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &stats); err != nil {
		return nil, err
	}

	return stats, nil
}

// DatabaseInfo returns comprehensive database information
func (d *Database) DatabaseInfo() (map[string]interface{}, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	var cJSON *C.char
	var cErr C.CError
	result := C.jasonisnthappy_database_info(d.db, &cJSON, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	var info map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &info); err != nil {
		return nil, err
	}

	return info, nil
}

// IndexInfo represents index metadata
type IndexInfo struct {
	Name      string   `json:"name"`
	Fields    []string `json:"fields"`
	Unique    bool     `json:"unique"`
	BTreeRoot uint64   `json:"btree_root"`
}

// ListIndexes returns all indexes for a collection
func (d *Database) ListIndexes(collectionName string) ([]IndexInfo, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	var cJSON *C.char
	var cErr C.CError
	result := C.jasonisnthappy_list_indexes(d.db, cCollName, &cJSON, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	var indexes []IndexInfo
	if err := json.Unmarshal([]byte(jsonStr), &indexes); err != nil {
		return nil, err
	}

	return indexes, nil
}

// ============================================================================
// Index Management
// ============================================================================

// CreateIndex creates a single-field index
func (d *Database) CreateIndex(collectionName, indexName, field string, unique bool) error {
	if d.db == nil {
		return &Error{Code: -1, Message: "Database is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	cIndexName := C.CString(indexName)
	defer C.free(unsafe.Pointer(cIndexName))

	cField := C.CString(field)
	defer C.free(unsafe.Pointer(cField))

	var cErr C.CError
	result := C.jasonisnthappy_create_index(d.db, cCollName, cIndexName, cField, C.bool(unique), &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// CreateCompoundIndex creates a compound index on multiple fields
func (d *Database) CreateCompoundIndex(collectionName, indexName string, fields []string, unique bool) error {
	if d.db == nil {
		return &Error{Code: -1, Message: "Database is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	cIndexName := C.CString(indexName)
	defer C.free(unsafe.Pointer(cIndexName))

	// Convert Go []string to C array of strings
	cFields := make([]*C.char, len(fields))
	for i, field := range fields {
		cFields[i] = C.CString(field)
		defer C.free(unsafe.Pointer(cFields[i]))
	}

	var cErr C.CError
	result := C.jasonisnthappy_create_compound_index(
		d.db,
		cCollName,
		cIndexName,
		(**C.char)(unsafe.Pointer(&cFields[0])),
		C.ulong(len(fields)),
		C.bool(unique),
		&cErr,
	)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// CreateTextIndex creates a text search index
func (d *Database) CreateTextIndex(collectionName, indexName string, fields []string) error {
	if d.db == nil {
		return &Error{Code: -1, Message: "Database is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	cIndexName := C.CString(indexName)
	defer C.free(unsafe.Pointer(cIndexName))

	cFields := make([]*C.char, len(fields))
	for i, field := range fields {
		cFields[i] = C.CString(field)
		defer C.free(unsafe.Pointer(cFields[i]))
	}

	var cErr C.CError
	result := C.jasonisnthappy_create_text_index(
		d.db,
		cCollName,
		cIndexName,
		(**C.char)(unsafe.Pointer(&cFields[0])),
		C.ulong(len(fields)),
		&cErr,
	)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// DropIndex drops an index (stub - not yet implemented in core)
func (d *Database) DropIndex(collectionName, indexName string) error {
	if d.db == nil {
		return &Error{Code: -1, Message: "Database is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	cIndexName := C.CString(indexName)
	defer C.free(unsafe.Pointer(cIndexName))

	var cErr C.CError
	result := C.jasonisnthappy_drop_index(d.db, cCollName, cIndexName, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// ============================================================================
// Schema Validation
// ============================================================================

// SetSchema sets a JSON schema for a collection
func (d *Database) SetSchema(collectionName string, schema map[string]interface{}) error {
	if d.db == nil {
		return &Error{Code: -1, Message: "Database is closed"}
	}

	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return err
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	cSchema := C.CString(string(schemaBytes))
	defer C.free(unsafe.Pointer(cSchema))

	var cErr C.CError
	result := C.jasonisnthappy_set_schema(d.db, cCollName, cSchema, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// GetSchema gets the JSON schema for a collection
func (d *Database) GetSchema(collectionName string) (map[string]interface{}, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	var cJSON *C.char
	var cErr C.CError
	result := C.jasonisnthappy_get_schema(d.db, cCollName, &cJSON, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	if cJSON == nil {
		return nil, nil // No schema set
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &schema); err != nil {
		return nil, err
	}

	return schema, nil
}

// RemoveSchema removes the JSON schema from a collection
func (d *Database) RemoveSchema(collectionName string) error {
	if d.db == nil {
		return &Error{Code: -1, Message: "Database is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	var cErr C.CError
	result := C.jasonisnthappy_remove_schema(d.db, cCollName, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// ============================================================================
// Maintenance & Monitoring
// ============================================================================

// Checkpoint performs a manual checkpoint
func (d *Database) Checkpoint() error {
	if d.db == nil {
		return &Error{Code: -1, Message: "Database is closed"}
	}

	var cErr C.CError
	result := C.jasonisnthappy_checkpoint(d.db, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// Backup creates a backup of the database
func (d *Database) Backup(destPath string) error {
	if d.db == nil {
		return &Error{Code: -1, Message: "Database is closed"}
	}

	cDestPath := C.CString(destPath)
	defer C.free(unsafe.Pointer(cDestPath))

	var cErr C.CError
	result := C.jasonisnthappy_backup(d.db, cDestPath, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// VerifyBackup verifies a backup file
func (d *Database) VerifyBackup(backupPath string) (map[string]interface{}, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	cBackupPath := C.CString(backupPath)
	defer C.free(unsafe.Pointer(cBackupPath))

	var cJSON *C.char
	var cErr C.CError
	result := C.jasonisnthappy_verify_backup(d.db, cBackupPath, &cJSON, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	var info map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &info); err != nil {
		return nil, err
	}

	return info, nil
}

// GarbageCollect performs garbage collection
func (d *Database) GarbageCollect() (map[string]interface{}, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	var cJSON *C.char
	var cErr C.CError
	result := C.jasonisnthappy_garbage_collect(d.db, &cJSON, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	var stats map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &stats); err != nil {
		return nil, err
	}

	return stats, nil
}

// Metrics returns database metrics
func (d *Database) Metrics() (map[string]interface{}, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	var cJSON *C.char
	var cErr C.CError
	result := C.jasonisnthappy_metrics(d.db, &cJSON, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	var metrics map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &metrics); err != nil {
		return nil, err
	}

	return metrics, nil
}

// FrameCount returns the current frame count
func (d *Database) FrameCount() (uint64, error) {
	if d.db == nil {
		return 0, &Error{Code: -1, Message: "Database is closed"}
	}

	var count C.ulonglong
	var cErr C.CError
	result := C.jasonisnthappy_frame_count(d.db, &count, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return 0, err
	}

	return uint64(count), nil
}

// ============================================================================
// Additional Transaction Operations
// ============================================================================

// RunTransaction runs a transaction with automatic retries
func (d *Database) RunTransaction(fn func(*Transaction) error) error {
	if d.db == nil {
		return &Error{Code: -1, Message: "Database is closed"}
	}

	// We'll use begin/commit/rollback manually since Go callbacks don't work with CGo easily
	// This is a simpler approach than the C callback version
	config, err := d.GetTransactionConfig()
	if err != nil {
		config = &TransactionConfig{MaxRetries: 3, RetryBackoffBaseMs: 10, MaxRetryBackoffMs: 1000}
	}

	var lastErr error
	for attempt := uint(0); attempt <= config.MaxRetries; attempt++ {
		tx, err := d.BeginTransaction()
		if err != nil {
			return err
		}

		err = fn(tx)
		if err != nil {
			tx.Rollback()
			return err
		}

		err = tx.Commit()
		if err == nil {
			return nil // Success!
		}

		// Check if it's a conflict
		if err, ok := err.(*Error); ok && err.Message == "Transaction conflict" {
			lastErr = err
			// Retry with backoff
			if attempt < config.MaxRetries {
				backoff := config.RetryBackoffBaseMs * (1 << attempt)
				if backoff > config.MaxRetryBackoffMs {
					backoff = config.MaxRetryBackoffMs
				}
				if backoff > 0 {
					// time.Sleep(time.Duration(backoff) * time.Millisecond)
					// For now, no sleep in Go binding - could import time if needed
				}
			}
		} else {
			return err // Non-conflict error
		}
	}

	if lastErr != nil {
		return lastErr
	}
	return &Error{Code: -1, Message: "Transaction failed after retries"}
}

// IsActive returns whether the transaction is still active
func (t *Transaction) IsActive() bool {
	if t.tx == nil {
		return false
	}

	var cErr C.CError
	result := C.jasonisnthappy_transaction_is_active(t.tx, &cErr)

	return result == 1
}

// CreateCollection creates a new collection within the transaction
func (t *Transaction) CreateCollection(name string) error {
	if t.tx == nil {
		return &Error{Code: -1, Message: "Transaction is closed"}
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var cErr C.CError
	result := C.jasonisnthappy_create_collection(t.tx, cName, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// DropCollection drops a collection within the transaction
func (t *Transaction) DropCollection(name string) error {
	if t.tx == nil {
		return &Error{Code: -1, Message: "Transaction is closed"}
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var cErr C.CError
	result := C.jasonisnthappy_drop_collection(t.tx, cName, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// RenameCollection renames a collection within the transaction
func (t *Transaction) RenameCollection(oldName, newName string) error {
	if t.tx == nil {
		return &Error{Code: -1, Message: "Transaction is closed"}
	}

	cOldName := C.CString(oldName)
	defer C.free(unsafe.Pointer(cOldName))

	cNewName := C.CString(newName)
	defer C.free(unsafe.Pointer(cNewName))

	var cErr C.CError
	result := C.jasonisnthappy_rename_collection(t.tx, cOldName, cNewName, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// Count returns the number of documents in a collection
func (t *Transaction) Count(collectionName string) (uint64, error) {
	if t.tx == nil {
		return 0, &Error{Code: -1, Message: "Transaction is closed"}
	}

	cCollName := C.CString(collectionName)
	defer C.free(unsafe.Pointer(cCollName))

	var count C.ulonglong
	var cErr C.CError
	result := C.jasonisnthappy_count(t.tx, cCollName, &count, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return 0, err
	}

	return uint64(count), nil
}

// ============================================================================
// Collection API
// ============================================================================

// Collection represents a database collection for non-transactional operations
type Collection struct {
	coll *C.CCollection
}

// GetCollection gets a collection handle for non-transactional operations
func (d *Database) GetCollection(name string) (*Collection, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var cErr C.CError
	coll := C.jasonisnthappy_get_collection(d.db, cName, &cErr)

	if coll == nil {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	return &Collection{coll: coll}, nil
}

// Free releases the collection handle
func (c *Collection) Free() {
	if c.coll != nil {
		C.jasonisnthappy_collection_free(c.coll)
		c.coll = nil
	}
}

// Insert inserts a document and returns its ID
func (c *Collection) Insert(doc interface{}) (string, error) {
	if c.coll == nil {
		return "", &Error{Code: -1, Message: "Collection is closed"}
	}

	jsonBytes, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}

	cJSON := C.CString(string(jsonBytes))
	defer C.free(unsafe.Pointer(cJSON))

	var cID *C.char
	var cErr C.CError
	result := C.jasonisnthappy_collection_insert(c.coll, cJSON, &cID, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return "", err
	}

	id := C.GoString(cID)
	C.jasonisnthappy_free_string(cID)

	return id, nil
}

// FindByID finds a document by ID
func (c *Collection) FindByID(id string, result interface{}) (bool, error) {
	if c.coll == nil {
		return false, &Error{Code: -1, Message: "Collection is closed"}
	}

	cID := C.CString(id)
	defer C.free(unsafe.Pointer(cID))

	var cJSON *C.char
	var cErr C.CError
	status := C.jasonisnthappy_collection_find_by_id(c.coll, cID, &cJSON, &cErr)

	if status == -1 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return false, err
	}

	if status == 1 {
		return false, nil // Not found
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	if err := json.Unmarshal([]byte(jsonStr), result); err != nil {
		return false, err
	}

	return true, nil
}

// UpdateByID updates a document by ID
func (c *Collection) UpdateByID(id string, updates interface{}) error {
	if c.coll == nil {
		return &Error{Code: -1, Message: "Collection is closed"}
	}

	jsonBytes, err := json.Marshal(updates)
	if err != nil {
		return err
	}

	cID := C.CString(id)
	defer C.free(unsafe.Pointer(cID))

	cJSON := C.CString(string(jsonBytes))
	defer C.free(unsafe.Pointer(cJSON))

	var cErr C.CError
	result := C.jasonisnthappy_collection_update_by_id(c.coll, cID, cJSON, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// DeleteByID deletes a document by ID
func (c *Collection) DeleteByID(id string) error {
	if c.coll == nil {
		return &Error{Code: -1, Message: "Collection is closed"}
	}

	cID := C.CString(id)
	defer C.free(unsafe.Pointer(cID))

	var cErr C.CError
	result := C.jasonisnthappy_collection_delete_by_id(c.coll, cID, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	return nil
}

// FindAll finds all documents in the collection
func (c *Collection) FindAll(result interface{}) error {
	if c.coll == nil {
		return &Error{Code: -1, Message: "Collection is closed"}
	}

	var cJSON *C.char
	var cErr C.CError
	status := C.jasonisnthappy_collection_find_all(c.coll, &cJSON, &cErr)

	if status != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	return json.Unmarshal([]byte(jsonStr), result)
}

// Count returns the number of documents in the collection
func (c *Collection) Count() (uint64, error) {
	if c.coll == nil {
		return 0, &Error{Code: -1, Message: "Collection is closed"}
	}

	var count C.ulong
	var cErr C.CError
	result := C.jasonisnthappy_collection_count(c.coll, &count, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return 0, err
	}

	return uint64(count), nil
}

// Name returns the collection name
func (c *Collection) Name() (string, error) {
	if c.coll == nil {
		return "", &Error{Code: -1, Message: "Collection is closed"}
	}

	var cName *C.char
	var cErr C.CError
	result := C.jasonisnthappy_collection_name(c.coll, &cName, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return "", err
	}

	name := C.GoString(cName)
	C.jasonisnthappy_free_string(cName)

	return name, nil
}

// ====================
// Query/Filter Operations
// ====================

// Find finds documents matching a filter
func (c *Collection) Find(filter string, result interface{}) error {
	if c.coll == nil {
		return &Error{Code: -1, Message: "Collection is closed"}
	}

	cFilter := C.CString(filter)
	defer C.free(unsafe.Pointer(cFilter))

	var cJSON *C.char
	var cErr C.CError
	status := C.jasonisnthappy_collection_find(c.coll, cFilter, &cJSON, &cErr)

	if status != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	return json.Unmarshal([]byte(jsonStr), result)
}

// FindOne finds the first document matching a filter
func (c *Collection) FindOne(filter string, result interface{}) (bool, error) {
	if c.coll == nil {
		return false, &Error{Code: -1, Message: "Collection is closed"}
	}

	cFilter := C.CString(filter)
	defer C.free(unsafe.Pointer(cFilter))

	var cJSON *C.char
	var cErr C.CError
	status := C.jasonisnthappy_collection_find_one(c.coll, cFilter, &cJSON, &cErr)

	if status != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return false, err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	if jsonStr == "" || jsonStr == "null" {
		return false, nil
	}

	if err := json.Unmarshal([]byte(jsonStr), result); err != nil {
		return false, err
	}

	return true, nil
}

// Update updates all documents matching a filter
func (c *Collection) Update(filter string, update interface{}) (uint64, error) {
	if c.coll == nil {
		return 0, &Error{Code: -1, Message: "Collection is closed"}
	}

	updateJSON, err := json.Marshal(update)
	if err != nil {
		return 0, err
	}

	cFilter := C.CString(filter)
	defer C.free(unsafe.Pointer(cFilter))
	cUpdate := C.CString(string(updateJSON))
	defer C.free(unsafe.Pointer(cUpdate))

	var count C.ulong
	var cErr C.CError
	result := C.jasonisnthappy_collection_update(c.coll, cFilter, cUpdate, &count, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return 0, err
	}

	return uint64(count), nil
}

// UpdateOne updates the first document matching a filter
func (c *Collection) UpdateOne(filter string, update interface{}) (bool, error) {
	if c.coll == nil {
		return false, &Error{Code: -1, Message: "Collection is closed"}
	}

	updateJSON, err := json.Marshal(update)
	if err != nil {
		return false, err
	}

	cFilter := C.CString(filter)
	defer C.free(unsafe.Pointer(cFilter))
	cUpdate := C.CString(string(updateJSON))
	defer C.free(unsafe.Pointer(cUpdate))

	var updated C.bool
	var cErr C.CError
	result := C.jasonisnthappy_collection_update_one(c.coll, cFilter, cUpdate, &updated, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return false, err
	}

	return bool(updated), nil
}

// Delete deletes all documents matching a filter
func (c *Collection) Delete(filter string) (uint64, error) {
	if c.coll == nil {
		return 0, &Error{Code: -1, Message: "Collection is closed"}
	}

	cFilter := C.CString(filter)
	defer C.free(unsafe.Pointer(cFilter))

	var count C.ulong
	var cErr C.CError
	result := C.jasonisnthappy_collection_delete(c.coll, cFilter, &count, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return 0, err
	}

	return uint64(count), nil
}

// DeleteOne deletes the first document matching a filter
func (c *Collection) DeleteOne(filter string) (bool, error) {
	if c.coll == nil {
		return false, &Error{Code: -1, Message: "Collection is closed"}
	}

	cFilter := C.CString(filter)
	defer C.free(unsafe.Pointer(cFilter))

	var deleted C.bool
	var cErr C.CError
	result := C.jasonisnthappy_collection_delete_one(c.coll, cFilter, &deleted, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return false, err
	}

	return bool(deleted), nil
}

// ====================
// Upsert Operations
// ====================

// UpsertByID inserts or updates a document by ID
// Returns UpsertResult with the ID and whether it was inserted or updated
func (c *Collection) UpsertByID(id string, doc interface{}) (*UpsertResult, error) {
	if c.coll == nil {
		return nil, &Error{Code: -1, Message: "Collection is closed"}
	}

	docJSON, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	cID := C.CString(id)
	defer C.free(unsafe.Pointer(cID))
	cDoc := C.CString(string(docJSON))
	defer C.free(unsafe.Pointer(cDoc))

	var resultCode C.int
	var cResultID *C.char
	var cErr C.CError
	status := C.jasonisnthappy_collection_upsert_by_id(c.coll, cID, cDoc, &resultCode, &cResultID, &cErr)

	if status != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	resultID := C.GoString(cResultID)
	C.jasonisnthappy_free_string(cResultID)

	// resultCode 0 = Inserted, 1 = Updated
	return &UpsertResult{
		ID:       resultID,
		Inserted: resultCode == 0,
	}, nil
}

// Upsert upserts documents matching a filter
// Returns UpsertResult with the ID and whether it was inserted or updated
func (c *Collection) Upsert(filter string, doc interface{}) (*UpsertResult, error) {
	if c.coll == nil {
		return nil, &Error{Code: -1, Message: "Collection is closed"}
	}

	docJSON, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	cFilter := C.CString(filter)
	defer C.free(unsafe.Pointer(cFilter))
	cDoc := C.CString(string(docJSON))
	defer C.free(unsafe.Pointer(cDoc))

	var resultCode C.int
	var cResultID *C.char
	var cErr C.CError
	status := C.jasonisnthappy_collection_upsert(c.coll, cFilter, cDoc, &resultCode, &cResultID, &cErr)

	if status != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	resultID := C.GoString(cResultID)
	C.jasonisnthappy_free_string(cResultID)

	// resultCode 0 = Inserted, 1 = Updated
	return &UpsertResult{
		ID:       resultID,
		Inserted: resultCode == 0,
	}, nil
}

// ====================
// Bulk Operations
// ====================

// InsertMany inserts multiple documents at once
func (c *Collection) InsertMany(docs []interface{}) ([]string, error) {
	if c.coll == nil {
		return nil, &Error{Code: -1, Message: "Collection is closed"}
	}

	docsJSON, err := json.Marshal(docs)
	if err != nil {
		return nil, err
	}

	cDocs := C.CString(string(docsJSON))
	defer C.free(unsafe.Pointer(cDocs))

	var cIDs *C.char
	var cErr C.CError
	result := C.jasonisnthappy_collection_insert_many(c.coll, cDocs, &cIDs, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	idsStr := C.GoString(cIDs)
	C.jasonisnthappy_free_string(cIDs)

	var ids []string
	if err := json.Unmarshal([]byte(idsStr), &ids); err != nil {
		return nil, err
	}

	return ids, nil
}

// ====================
// Advanced Operations
// ====================

// Distinct returns distinct values for a field
func (c *Collection) Distinct(field string) ([]interface{}, error) {
	if c.coll == nil {
		return nil, &Error{Code: -1, Message: "Collection is closed"}
	}

	cField := C.CString(field)
	defer C.free(unsafe.Pointer(cField))

	var cJSON *C.char
	var cErr C.CError
	result := C.jasonisnthappy_collection_distinct(c.coll, cField, &cJSON, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	var values []interface{}
	if err := json.Unmarshal([]byte(jsonStr), &values); err != nil {
		return nil, err
	}

	return values, nil
}

// CountDistinct counts distinct values for a field
func (c *Collection) CountDistinct(field string) (uint64, error) {
	if c.coll == nil {
		return 0, &Error{Code: -1, Message: "Collection is closed"}
	}

	cField := C.CString(field)
	defer C.free(unsafe.Pointer(cField))

	var count C.ulong
	var cErr C.CError
	result := C.jasonisnthappy_collection_count_distinct(c.coll, cField, &count, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return 0, err
	}

	return uint64(count), nil
}

// Search performs full-text search
func (c *Collection) Search(query string, result interface{}) error {
	if c.coll == nil {
		return &Error{Code: -1, Message: "Collection is closed"}
	}

	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var cJSON *C.char
	var cErr C.CError
	status := C.jasonisnthappy_collection_search(c.coll, cQuery, &cJSON, &cErr)

	if status != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	return json.Unmarshal([]byte(jsonStr), result)
}

// CountWithQuery counts documents matching a filter
func (c *Collection) CountWithQuery(filter string) (uint64, error) {
	if c.coll == nil {
		return 0, &Error{Code: -1, Message: "Collection is closed"}
	}

	cFilter := C.CString(filter)
	defer C.free(unsafe.Pointer(cFilter))

	var count C.ulong
	var cErr C.CError
	result := C.jasonisnthappy_collection_count_with_query(c.coll, cFilter, &count, &cErr)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return 0, err
	}

	return uint64(count), nil
}

// ====================
// Typed Variants (same as non-typed in FFI)
// ====================

// InsertTyped is an alias for Insert (same implementation in FFI)
func (c *Collection) InsertTyped(doc interface{}) (string, error) {
	return c.Insert(doc)
}

// InsertManyTyped is an alias for InsertMany (same implementation in FFI)
func (c *Collection) InsertManyTyped(docs []interface{}) ([]string, error) {
	return c.InsertMany(docs)
}

// FindByIDTyped is an alias for FindByID (same implementation in FFI)
func (c *Collection) FindByIDTyped(id string, result interface{}) (bool, error) {
	return c.FindByID(id, result)
}

// FindAllTyped is an alias for FindAll (same implementation in FFI)
func (c *Collection) FindAllTyped(result interface{}) error {
	return c.FindAll(result)
}

// FindTyped is an alias for Find (same implementation in FFI)
func (c *Collection) FindTyped(filter string, result interface{}) error {
	return c.Find(filter, result)
}

// FindOneTyped is an alias for FindOne (same implementation in FFI)
func (c *Collection) FindOneTyped(filter string, result interface{}) (bool, error) {
	return c.FindOne(filter, result)
}

// UpdateByIDTyped is an alias for UpdateByID (same implementation in FFI)
func (c *Collection) UpdateByIDTyped(id string, update interface{}) error {
	return c.UpdateByID(id, update)
}

// UpdateTyped is an alias for Update (same implementation in FFI)
func (c *Collection) UpdateTyped(filter string, update interface{}) (uint64, error) {
	return c.Update(filter, update)
}

// UpdateOneTyped is an alias for UpdateOne (same implementation in FFI)
func (c *Collection) UpdateOneTyped(filter string, update interface{}) (bool, error) {
	return c.UpdateOne(filter, update)
}

// UpsertByIDTyped is an alias for UpsertByID (same implementation in FFI)
func (c *Collection) UpsertByIDTyped(id string, doc interface{}) (*UpsertResult, error) {
	return c.UpsertByID(id, doc)
}

// UpsertTyped is an alias for Upsert (same implementation in FFI)
func (c *Collection) UpsertTyped(filter string, doc interface{}) (*UpsertResult, error) {
	return c.Upsert(filter, doc)
}

// ====================
// Query Builder Helpers
// ====================

// QueryWithOptions executes a query with all options in one call
func (c *Collection) QueryWithOptions(
	filter string,
	sortField string,
	sortAsc bool,
	limit uint64,
	skip uint64,
	projectFields []string,
	excludeFields []string,
	result interface{},
) error {
	if c.coll == nil {
		return &Error{Code: -1, Message: "Collection is closed"}
	}

	var cFilter *C.char
	if filter != "" {
		cFilter = C.CString(filter)
		defer C.free(unsafe.Pointer(cFilter))
	}

	var cSortField *C.char
	if sortField != "" {
		cSortField = C.CString(sortField)
		defer C.free(unsafe.Pointer(cSortField))
	}

	var cProject *C.char
	if len(projectFields) > 0 {
		projectJSON, err := json.Marshal(projectFields)
		if err != nil {
			return err
		}
		cProject = C.CString(string(projectJSON))
		defer C.free(unsafe.Pointer(cProject))
	}

	var cExclude *C.char
	if len(excludeFields) > 0 {
		excludeJSON, err := json.Marshal(excludeFields)
		if err != nil {
			return err
		}
		cExclude = C.CString(string(excludeJSON))
		defer C.free(unsafe.Pointer(cExclude))
	}

	var cJSON *C.char
	var cErr C.CError
	status := C.jasonisnthappy_collection_query_with_options(
		c.coll,
		cFilter,
		cSortField,
		C.bool(sortAsc),
		C.ulong(limit),
		C.ulong(skip),
		cProject,
		cExclude,
		&cJSON,
		&cErr,
	)

	if status != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	return json.Unmarshal([]byte(jsonStr), result)
}

// QueryCount counts documents matching a filter (optimized)
func (c *Collection) QueryCount(filter string, skip uint64, limit uint64) (uint64, error) {
	if c.coll == nil {
		return 0, &Error{Code: -1, Message: "Collection is closed"}
	}

	var cFilter *C.char
	if filter != "" {
		cFilter = C.CString(filter)
		defer C.free(unsafe.Pointer(cFilter))
	}

	var count C.uintptr_t
	var cErr C.CError
	result := C.jasonisnthappy_collection_query_count(
		c.coll,
		cFilter,
		C.uintptr_t(skip),
		C.uintptr_t(limit),
		&count,
		&cErr,
	)

	if result != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return 0, err
	}

	return uint64(count), nil
}

// QueryFirst gets the first document matching a filter
func (c *Collection) QueryFirst(filter string, sortField string, sortAsc bool, result interface{}) (bool, error) {
	if c.coll == nil {
		return false, &Error{Code: -1, Message: "Collection is closed"}
	}

	var cFilter *C.char
	if filter != "" {
		cFilter = C.CString(filter)
		defer C.free(unsafe.Pointer(cFilter))
	}

	var cSortField *C.char
	if sortField != "" {
		cSortField = C.CString(sortField)
		defer C.free(unsafe.Pointer(cSortField))
	}

	var cJSON *C.char
	var cErr C.CError
	status := C.jasonisnthappy_collection_query_first(
		c.coll,
		cFilter,
		cSortField,
		C.bool(sortAsc),
		&cJSON,
		&cErr,
	)

	if status != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return false, err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	if jsonStr == "" || jsonStr == "null" {
		return false, nil
	}

	if err := json.Unmarshal([]byte(jsonStr), result); err != nil {
		return false, err
	}

	return true, nil
}

// ====================
// Bulk Write Operations
// ====================

// BulkWriteOperation represents a single bulk operation
type BulkWriteOperation struct {
	Op     string                 `json:"op"`
	Filter string                 `json:"filter,omitempty"`
	Doc    map[string]interface{} `json:"doc,omitempty"`
	Update map[string]interface{} `json:"update,omitempty"`
}

// BulkWriteError represents an error from a bulk write operation
type BulkWriteError struct {
	OperationIndex int    `json:"operation_index"`
	Message        string `json:"message"`
}

// BulkWriteResult contains the result of a bulk write operation
type BulkWriteResult struct {
	InsertedCount int              `json:"inserted_count"`
	UpdatedCount  int              `json:"updated_count"`
	DeletedCount  int              `json:"deleted_count"`
	InsertedIDs   []string         `json:"inserted_ids,omitempty"`
	Errors        []BulkWriteError `json:"errors,omitempty"`
}

// BulkWrite executes multiple operations in a single transaction
func (c *Collection) BulkWrite(operations []BulkWriteOperation, ordered bool) (*BulkWriteResult, error) {
	if c.coll == nil {
		return nil, &Error{Code: -1, Message: "Collection is closed"}
	}

	opsJSON, err := json.Marshal(operations)
	if err != nil {
		return nil, err
	}

	cOps := C.CString(string(opsJSON))
	defer C.free(unsafe.Pointer(cOps))

	var cResult *C.char
	var cErr C.CError
	status := C.jasonisnthappy_collection_bulk_write(c.coll, cOps, C.bool(ordered), &cResult, &cErr)

	if status != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	resultStr := C.GoString(cResult)
	C.jasonisnthappy_free_string(cResult)

	var result BulkWriteResult
	if err := json.Unmarshal([]byte(resultStr), &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// ====================
// Aggregation Pipeline
// ====================

// AggregationStage represents a single aggregation stage
type AggregationStage struct {
	Match     string              `json:"match,omitempty"`
	GroupBy   string              `json:"group_by,omitempty"`
	Count     string              `json:"count,omitempty"`
	Sum       *AggregationField   `json:"sum,omitempty"`
	Avg       *AggregationField   `json:"avg,omitempty"`
	Min       *AggregationField   `json:"min,omitempty"`
	Max       *AggregationField   `json:"max,omitempty"`
	Sort      *SortOptions        `json:"sort,omitempty"`
	Limit     int                 `json:"limit,omitempty"`
	Skip      int                 `json:"skip,omitempty"`
	Project   []string            `json:"project,omitempty"`
	Exclude   []string            `json:"exclude,omitempty"`
}

// AggregationField represents a field for aggregation functions
type AggregationField struct {
	Field  string `json:"field"`
	Output string `json:"output"`
}

// SortOptions represents sort options for aggregation
type SortOptions struct {
	Field string `json:"field"`
	Asc   bool   `json:"asc"`
}

// Aggregate executes an aggregation pipeline
func (c *Collection) Aggregate(pipeline []AggregationStage, result interface{}) error {
	if c.coll == nil {
		return &Error{Code: -1, Message: "Collection is closed"}
	}

	pipelineJSON, err := json.Marshal(pipeline)
	if err != nil {
		return err
	}

	cPipeline := C.CString(string(pipelineJSON))
	defer C.free(unsafe.Pointer(cPipeline))

	var cJSON *C.char
	var cErr C.CError
	status := C.jasonisnthappy_collection_aggregate(c.coll, cPipeline, &cJSON, &cErr)

	if status != 0 {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return err
	}

	jsonStr := C.GoString(cJSON)
	C.jasonisnthappy_free_string(cJSON)

	return json.Unmarshal([]byte(jsonStr), result)
}

// ====================
// Web Server
// ====================

// WebServer represents a running web UI server
type WebServer struct {
	server *C.CWebServer
}

// StartWebUI starts the web UI server at the given address
// Returns a WebServer handle that can be used to stop the server
//
// Example:
//
//	server, err := db.StartWebUI("127.0.0.1:8080")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer server.Stop()
//	fmt.Println("Web UI available at http://127.0.0.1:8080")
func (d *Database) StartWebUI(addr string) (*WebServer, error) {
	if d.db == nil {
		return nil, &Error{Code: -1, Message: "Database is closed"}
	}

	cAddr := C.CString(addr)
	defer C.free(unsafe.Pointer(cAddr))

	var cErr C.CError
	server := C.jasonisnthappy_start_web_server(d.db, cAddr, &cErr)

	if server == nil {
		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	return &WebServer{server: server}, nil
}

// Stop stops the web UI server
func (ws *WebServer) Stop() {
	if ws.server != nil {
		C.jasonisnthappy_stop_web_server(ws.server)
		ws.server = nil
	}
}

// ====================
// Watch / Change Streams
// ====================

// WatchHandle represents a handle to an active watch operation
type WatchHandle struct {
	handle     *C.CWatchHandle
	callbackID uintptr
}

// WatchStart starts watching for changes on the collection
//
// The callback will be called for each change event that matches the filter.
// Pass an empty string for filter to watch all changes.
//
// Example:
//
//	handle, err := collection.WatchStart("", func(coll, op, docID, docJSON string) {
//	    fmt.Printf("Change: %s %s in %s\n", op, docID, coll)
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer handle.Stop()
func (c *Collection) WatchStart(filter string, callback WatchCallback) (*WatchHandle, error) {
	if c.coll == nil {
		return nil, &Error{Code: -1, Message: "Collection is freed"}
	}

	// Register the callback
	watchCallbacksMu.Lock()
	callbackID := nextCallbackID
	nextCallbackID++
	watchCallbacks[callbackID] = callback
	watchCallbacksMu.Unlock()

	// Prepare filter
	var cFilter *C.char
	if filter != "" {
		cFilter = C.CString(filter)
		defer C.free(unsafe.Pointer(cFilter))
	}

	// Call FFI function
	var cErr C.CError
	var cHandle *C.CWatchHandle

	result := C.jasonisnthappy_collection_watch_start(
		c.coll,
		cFilter,
		(C.watch_callback_fn)(unsafe.Pointer(C.goWatchCallbackBridge)),
		unsafe.Pointer(callbackID),
		&cHandle,
		&cErr,
	)

	if result != 0 {
		// Cleanup callback registration on error
		watchCallbacksMu.Lock()
		delete(watchCallbacks, callbackID)
		watchCallbacksMu.Unlock()

		err := cErrorToGoError(&cErr)
		C.jasonisnthappy_free_error(cErr)
		return nil, err
	}

	return &WatchHandle{
		handle:     cHandle,
		callbackID: callbackID,
	}, nil
}

// Stop stops watching and cleans up resources
func (w *WatchHandle) Stop() {
	if w.handle != nil {
		C.jasonisnthappy_watch_stop(w.handle)
		w.handle = nil

		// Cleanup callback registration
		watchCallbacksMu.Lock()
		delete(watchCallbacks, w.callbackID)
		watchCallbacksMu.Unlock()
	}
}
