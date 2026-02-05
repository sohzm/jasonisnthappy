#ifndef JASONISNTHAPPY_H
#define JASONISNTHAPPY_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct Arc_Database Arc_Database;

typedef struct CWatchHandle CWatchHandle;

typedef struct Option_WebServer Option_WebServer;

typedef struct CDatabase {
  struct Arc_Database inner;
} CDatabase;

typedef struct CError {
  int32_t code;
  char *message;
} CError;

typedef struct CDatabaseOptions {
  uintptr_t cache_size;
  uint64_t auto_checkpoint_threshold;
  uint32_t file_permissions;
  bool read_only;
  uintptr_t max_bulk_operations;
  uintptr_t max_document_size;
  uintptr_t max_request_body_size;
} CDatabaseOptions;

typedef struct CTransactionConfig {
  uintptr_t max_retries;
  uint64_t retry_backoff_base_ms;
  uint64_t max_retry_backoff_ms;
} CTransactionConfig;

typedef struct CTransaction {
  Transaction inner;
} CTransaction;

/**
 * C callback function type for run_transaction
 *
 * # Parameters
 * - tx: Transaction handle to use for operations
 * - user_data: User-provided context pointer
 *
 * # Returns
 * - 0 for success (commit), -1 for error (rollback)
 */
typedef int32_t (*TransactionCallback)(struct CTransaction *tx, void *user_data);

typedef struct CCollection {
  Collection inner;
} CCollection;

/**
 * C callback function type for watch events
 *
 * # Parameters
 * - collection: Name of the collection where the change occurred
 * - operation: "insert", "update", or "delete"
 * - doc_id: ID of the document
 * - doc_json: JSON representation of the document (NULL for delete operations)
 * - user_data: User-provided context pointer passed to watch_start
 */
typedef void (*WatchCallback)(const char *collection,
                              const char *operation,
                              const char *doc_id,
                              const char *doc_json,
                              void *user_data);

typedef struct CWebServer {
  struct Option_WebServer inner;
} CWebServer;

struct CDatabase *jasonisnthappy_open(const char *path, struct CError *error_out);

struct CDatabase *jasonisnthappy_open_with_options(const char *path,
                                                   struct CDatabaseOptions options,
                                                   struct CError *error_out);

void jasonisnthappy_close(struct CDatabase *db);

int32_t jasonisnthappy_set_transaction_config(struct CDatabase *db,
                                              struct CTransactionConfig config,
                                              struct CError *error_out);

int32_t jasonisnthappy_get_transaction_config(struct CDatabase *db,
                                              struct CTransactionConfig *config_out,
                                              struct CError *error_out);

int32_t jasonisnthappy_set_auto_checkpoint_threshold(struct CDatabase *db,
                                                     uint64_t threshold,
                                                     struct CError *error_out);

struct CDatabaseOptions jasonisnthappy_default_database_options(void);

struct CTransactionConfig jasonisnthappy_default_transaction_config(void);

struct CTransaction *jasonisnthappy_begin_transaction(struct CDatabase *db,
                                                      struct CError *error_out);

int32_t jasonisnthappy_commit(struct CTransaction *tx, struct CError *error_out);

void jasonisnthappy_rollback(struct CTransaction *tx);

/**
 * Run a transaction with automatic retries on conflict
 *
 * This is a convenience wrapper that handles begin/commit/rollback with automatic
 * retries according to the database's transaction config.
 *
 * # Parameters
 * - db: Database handle
 * - callback: Function called with the transaction - return 0 to commit, -1 to rollback
 * - user_data: Optional user context pointer passed to callback
 * - error_out: Output for error information
 *
 * # Returns
 * 0 on successful commit, -1 on error
 *
 * # Example
 * The callback should perform all operations and return 0 for success:
 * ```c
 * int32_t my_callback(CTransaction* tx, void* user_data) {
 *     // Do operations with tx
 *     return 0;  // success - will commit
 *     // return -1;  // error - will rollback
 * }
 * jasonisnthappy_run_transaction(db, my_callback, user_data, &error);
 * ```
 */
int32_t jasonisnthappy_run_transaction(struct CDatabase *db,
                                       TransactionCallback callback,
                                       void *user_data,
                                       struct CError *error_out);

/**
 * Check if a transaction is still active (not committed or rolled back)
 */
int32_t jasonisnthappy_transaction_is_active(struct CTransaction *tx, struct CError *error_out);

int32_t jasonisnthappy_insert(struct CTransaction *tx,
                              const char *collection_name,
                              const char *json,
                              char **id_out,
                              struct CError *error_out);

int32_t jasonisnthappy_find_by_id(struct CTransaction *tx,
                                  const char *collection_name,
                                  const char *id,
                                  char **json_out,
                                  struct CError *error_out);

int32_t jasonisnthappy_update_by_id(struct CTransaction *tx,
                                    const char *collection_name,
                                    const char *id,
                                    const char *json,
                                    struct CError *error_out);

int32_t jasonisnthappy_delete_by_id(struct CTransaction *tx,
                                    const char *collection_name,
                                    const char *id,
                                    struct CError *error_out);

int32_t jasonisnthappy_find_all(struct CTransaction *tx,
                                const char *collection_name,
                                char **json_out,
                                struct CError *error_out);

void jasonisnthappy_free_string(char *s);

void jasonisnthappy_free_error(struct CError error);

int32_t jasonisnthappy_count(struct CTransaction *tx,
                             const char *collection_name,
                             uint64_t *count_out,
                             struct CError *error_out);

int32_t jasonisnthappy_create_collection(struct CTransaction *tx,
                                         const char *collection_name,
                                         struct CError *error_out);

int32_t jasonisnthappy_drop_collection(struct CTransaction *tx,
                                       const char *collection_name,
                                       struct CError *error_out);

int32_t jasonisnthappy_rename_collection(struct CTransaction *tx,
                                         const char *old_name,
                                         const char *new_name,
                                         struct CError *error_out);

int32_t jasonisnthappy_list_collections(struct CDatabase *db,
                                        char **json_out,
                                        struct CError *error_out);

/**
 * List all indexes for a collection
 *
 * Returns JSON array of index objects with: name, fields (array), unique (bool), btree_root
 */
int32_t jasonisnthappy_list_indexes(struct CDatabase *db,
                                    const char *collection_name,
                                    char **json_out,
                                    struct CError *error_out);

int32_t jasonisnthappy_create_index(struct CDatabase *db,
                                    const char *collection_name,
                                    const char *index_name,
                                    const char *field,
                                    bool unique,
                                    struct CError *error_out);

int32_t jasonisnthappy_create_compound_index(struct CDatabase *db,
                                             const char *collection_name,
                                             const char *index_name,
                                             const char *const *fields,
                                             uintptr_t num_fields,
                                             bool unique,
                                             struct CError *error_out);

int32_t jasonisnthappy_create_text_index(struct CDatabase *db,
                                         const char *collection_name,
                                         const char *index_name,
                                         const char *const *fields,
                                         uintptr_t num_fields,
                                         struct CError *error_out);

int32_t jasonisnthappy_drop_index(struct CDatabase *db,
                                  const char *collection_name,
                                  const char *index_name,
                                  struct CError *error_out);

int32_t jasonisnthappy_collection_stats(struct CDatabase *db,
                                        const char *collection_name,
                                        char **json_out,
                                        struct CError *error_out);

int32_t jasonisnthappy_database_info(struct CDatabase *db,
                                     char **json_out,
                                     struct CError *error_out);

int32_t jasonisnthappy_get_path(struct CDatabase *db, char **path_out, struct CError *error_out);

int32_t jasonisnthappy_is_read_only(struct CDatabase *db, struct CError *error_out);

uintptr_t jasonisnthappy_max_bulk_operations(struct CDatabase *db, struct CError *error_out);

uintptr_t jasonisnthappy_max_document_size(struct CDatabase *db, struct CError *error_out);

uintptr_t jasonisnthappy_max_request_body_size(struct CDatabase *db, struct CError *error_out);

int32_t jasonisnthappy_set_schema(struct CDatabase *db,
                                  const char *collection_name,
                                  const char *schema_json,
                                  struct CError *error_out);

int32_t jasonisnthappy_get_schema(struct CDatabase *db,
                                  const char *collection_name,
                                  char **schema_json_out,
                                  struct CError *error_out);

int32_t jasonisnthappy_remove_schema(struct CDatabase *db,
                                     const char *collection_name,
                                     struct CError *error_out);

struct CCollection *jasonisnthappy_get_collection(struct CDatabase *db,
                                                  const char *collection_name,
                                                  struct CError *error_out);

void jasonisnthappy_collection_free(struct CCollection *coll);

int32_t jasonisnthappy_collection_upsert_by_id(struct CCollection *coll,
                                               const char *id,
                                               const char *json,
                                               int32_t *result_out,
                                               char **id_out,
                                               struct CError *error_out);

int32_t jasonisnthappy_collection_upsert(struct CCollection *coll,
                                         const char *query,
                                         const char *json,
                                         int32_t *result_out,
                                         char **id_out,
                                         struct CError *error_out);

int32_t jasonisnthappy_collection_find(struct CCollection *coll,
                                       const char *query,
                                       char **json_out,
                                       struct CError *error_out);

int32_t jasonisnthappy_collection_find_one(struct CCollection *coll,
                                           const char *query,
                                           char **json_out,
                                           struct CError *error_out);

int32_t jasonisnthappy_collection_update(struct CCollection *coll,
                                         const char *query,
                                         const char *updates_json,
                                         uintptr_t *count_out,
                                         struct CError *error_out);

int32_t jasonisnthappy_collection_update_one(struct CCollection *coll,
                                             const char *query,
                                             const char *updates_json,
                                             bool *updated_out,
                                             struct CError *error_out);

int32_t jasonisnthappy_collection_delete(struct CCollection *coll,
                                         const char *query,
                                         uintptr_t *count_out,
                                         struct CError *error_out);

int32_t jasonisnthappy_collection_delete_one(struct CCollection *coll,
                                             const char *query,
                                             bool *deleted_out,
                                             struct CError *error_out);

int32_t jasonisnthappy_collection_insert_many(struct CCollection *coll,
                                              const char *docs_json,
                                              char **ids_json_out,
                                              struct CError *error_out);

int32_t jasonisnthappy_collection_distinct(struct CCollection *coll,
                                           const char *field,
                                           char **json_out,
                                           struct CError *error_out);

int32_t jasonisnthappy_collection_count_distinct(struct CCollection *coll,
                                                 const char *field,
                                                 uintptr_t *count_out,
                                                 struct CError *error_out);

int32_t jasonisnthappy_collection_search(struct CCollection *coll,
                                         const char *query,
                                         char **json_out,
                                         struct CError *error_out);

int32_t jasonisnthappy_collection_insert(struct CCollection *coll,
                                         const char *json,
                                         char **id_out,
                                         struct CError *error_out);

int32_t jasonisnthappy_collection_find_by_id(struct CCollection *coll,
                                             const char *id,
                                             char **json_out,
                                             struct CError *error_out);

int32_t jasonisnthappy_collection_update_by_id(struct CCollection *coll,
                                               const char *id,
                                               const char *updates_json,
                                               struct CError *error_out);

int32_t jasonisnthappy_collection_delete_by_id(struct CCollection *coll,
                                               const char *id,
                                               struct CError *error_out);

int32_t jasonisnthappy_collection_find_all(struct CCollection *coll,
                                           char **json_out,
                                           struct CError *error_out);

int32_t jasonisnthappy_collection_count(struct CCollection *coll,
                                        uintptr_t *count_out,
                                        struct CError *error_out);

int32_t jasonisnthappy_collection_name(struct CCollection *coll,
                                       char **name_out,
                                       struct CError *error_out);

int32_t jasonisnthappy_collection_count_with_query(struct CCollection *coll,
                                                   const char *query,
                                                   uintptr_t *count_out,
                                                   struct CError *error_out);

int32_t jasonisnthappy_collection_insert_typed(struct CCollection *coll,
                                               const char *json,
                                               char **id_out,
                                               struct CError *error_out);

int32_t jasonisnthappy_collection_insert_many_typed(struct CCollection *coll,
                                                    const char *docs_json,
                                                    char **ids_json_out,
                                                    struct CError *error_out);

int32_t jasonisnthappy_collection_find_by_id_typed(struct CCollection *coll,
                                                   const char *id,
                                                   char **json_out,
                                                   struct CError *error_out);

int32_t jasonisnthappy_collection_find_all_typed(struct CCollection *coll,
                                                 char **json_out,
                                                 struct CError *error_out);

int32_t jasonisnthappy_collection_find_typed(struct CCollection *coll,
                                             const char *query,
                                             char **json_out,
                                             struct CError *error_out);

int32_t jasonisnthappy_collection_find_one_typed(struct CCollection *coll,
                                                 const char *query,
                                                 char **json_out,
                                                 struct CError *error_out);

int32_t jasonisnthappy_collection_update_by_id_typed(struct CCollection *coll,
                                                     const char *id,
                                                     const char *updates_json,
                                                     struct CError *error_out);

int32_t jasonisnthappy_collection_update_typed(struct CCollection *coll,
                                               const char *query,
                                               const char *updates_json,
                                               uintptr_t *count_out,
                                               struct CError *error_out);

int32_t jasonisnthappy_collection_update_one_typed(struct CCollection *coll,
                                                   const char *query,
                                                   const char *updates_json,
                                                   bool *updated_out,
                                                   struct CError *error_out);

int32_t jasonisnthappy_collection_upsert_by_id_typed(struct CCollection *coll,
                                                     const char *id,
                                                     const char *json,
                                                     int32_t *result_out,
                                                     char **id_out,
                                                     struct CError *error_out);

int32_t jasonisnthappy_collection_upsert_typed(struct CCollection *coll,
                                               const char *query,
                                               const char *json,
                                               int32_t *result_out,
                                               char **id_out,
                                               struct CError *error_out);

/**
 * Query with all options in a single call (simplified query builder for FFI)
 *
 * # Parameters
 * - filter: Optional query filter string (NULL = no filter)
 * - sort_field: Optional field to sort by (NULL = no sort)
 * - sort_ascending: true for ascending, false for descending
 * - limit: Max results (0 = no limit)
 * - skip: Skip N results (0 = no skip)
 * - project_json: Optional JSON array of fields to include (NULL = all fields)
 * - exclude_json: Optional JSON array of fields to exclude (NULL = none)
 *
 * Note: Cannot specify both project_json and exclude_json
 */
int32_t jasonisnthappy_collection_query_with_options(struct CCollection *coll,
                                                     const char *filter,
                                                     const char *sort_field,
                                                     bool sort_ascending,
                                                     uintptr_t limit,
                                                     uintptr_t skip,
                                                     const char *project_json,
                                                     const char *exclude_json,
                                                     char **json_out,
                                                     struct CError *error_out);

/**
 * Query and count results (no fetch)
 */
int32_t jasonisnthappy_collection_query_count(struct CCollection *coll,
                                              const char *filter,
                                              uintptr_t skip,
                                              uintptr_t limit,
                                              uintptr_t *count_out,
                                              struct CError *error_out);

/**
 * Query and return first result
 */
int32_t jasonisnthappy_collection_query_first(struct CCollection *coll,
                                              const char *filter,
                                              const char *sort_field,
                                              bool sort_ascending,
                                              char **json_out,
                                              struct CError *error_out);

/**
 * Execute bulk write operations in a single transaction
 *
 * # Parameters
 * - operations_json: JSON array of operations, each with:
 *   - "op": "insert" | "update_one" | "update_many" | "delete_one" | "delete_many"
 *   - "doc": document (for insert)
 *   - "query": query string (for update/delete)
 *   - "updates": updates object (for update)
 * - ordered: if true, stop on first error; if false, continue on errors
 * - result_json_out: BulkWriteResult as JSON (inserted_count, updated_count, deleted_count, errors)
 *
 * # Example operations_json:
 * ```json
 * [
 *   {"op": "insert", "doc": {"name": "Alice", "age": 30}},
 *   {"op": "update_one", "query": "name is 'Bob'", "updates": {"age": 31}},
 *   {"op": "delete_many", "query": "age < 18"}
 * ]
 * ```
 */
int32_t jasonisnthappy_collection_bulk_write(struct CCollection *coll,
                                             const char *operations_json,
                                             bool ordered,
                                             char **result_json_out,
                                             struct CError *error_out);

/**
 * Execute an aggregation pipeline
 *
 * # Parameters
 * - pipeline_json: JSON array of pipeline stages, each with:
 *   - "match": query string (filter stage)
 *   - "group_by": {field: "...", accumulators: [{type: "count|sum|avg|min|max", output_field: "...", field: "..."}]}
 *   - "sort": {field: "...", ascending: true|false}
 *   - "limit": number
 *   - "skip": number
 *   - "project": ["field1", "field2", ...]
 *   - "exclude": ["field1", "field2", ...]
 *
 * # Example pipeline_json:
 * ```json
 * [
 *   {"match": "status is 'active'"},
 *   {"group_by": {"field": "city", "accumulators": [
 *     {"type": "count", "output_field": "total"},
 *     {"type": "sum", "field": "amount", "output_field": "total_amount"}
 *   ]}},
 *   {"sort": {"field": "total", "ascending": false}},
 *   {"limit": 10}
 * ]
 * ```
 */
int32_t jasonisnthappy_collection_aggregate(struct CCollection *coll,
                                            const char *pipeline_json,
                                            char **result_json_out,
                                            struct CError *error_out);

/**
 * Start watching a collection for changes
 *
 * Creates a background thread that monitors changes to the collection and calls
 * the provided callback function for each change event.
 *
 * # Parameters
 * - coll: Collection to watch
 * - filter: Optional query filter (NULL = watch all changes)
 * - callback: Function to call for each change event
 * - user_data: Optional user context pointer passed to callback
 * - handle_out: Output pointer for the watch handle (use to stop watching)
 *
 * # Returns
 * 0 on success, -1 on error
 *
 * # Safety
 * The callback will be called from a background thread. Ensure thread safety.
 * Call jasonisnthappy_watch_stop() to stop watching and clean up the thread.
 */
int32_t jasonisnthappy_collection_watch_start(struct CCollection *coll,
                                              const char *filter,
                                              WatchCallback callback,
                                              void *user_data,
                                              struct CWatchHandle **handle_out,
                                              struct CError *error_out);

/**
 * Stop watching and clean up resources
 *
 * Signals the background thread to stop and waits for it to finish.
 * After calling this, the handle pointer is no longer valid.
 *
 * # Parameters
 * - handle: Watch handle returned by jasonisnthappy_collection_watch_start
 *
 * # Safety
 * The handle must have been created by jasonisnthappy_collection_watch_start.
 * Do not use the handle after calling this function.
 */
void jasonisnthappy_watch_stop(struct CWatchHandle *handle);

int32_t jasonisnthappy_checkpoint(struct CDatabase *db, struct CError *error_out);

int32_t jasonisnthappy_backup(struct CDatabase *db,
                              const char *backup_path,
                              struct CError *error_out);

int32_t jasonisnthappy_verify_backup(struct CDatabase *db,
                                     const char *backup_path,
                                     char **json_out,
                                     struct CError *error_out);

int32_t jasonisnthappy_garbage_collect(struct CDatabase *db,
                                       char **json_out,
                                       struct CError *error_out);

int32_t jasonisnthappy_metrics(struct CDatabase *db, char **json_out, struct CError *error_out);

int32_t jasonisnthappy_frame_count(struct CDatabase *db,
                                   uint64_t *count_out,
                                   struct CError *error_out);

struct CWebServer *jasonisnthappy_start_web_server(struct CDatabase *db,
                                                   const char *addr,
                                                   struct CError *error_out);

void jasonisnthappy_stop_web_server(struct CWebServer *server);

#endif  /* JASONISNTHAPPY_H */
