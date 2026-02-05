#ifndef JASONISNTHAPPY_H
#define JASONISNTHAPPY_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/* Opaque pointer types - internal layout hidden from C */
typedef struct CDatabase CDatabase;
typedef struct CTransaction CTransaction;
typedef struct CCollection CCollection;
typedef struct CWebServer CWebServer;
typedef struct CWatchHandle CWatchHandle;

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

/**
 * C callback function type for run_transaction
 */
typedef int32_t (*transaction_callback_fn)(CTransaction *tx, void *user_data);

/**
 * C callback function type for watch events
 */
typedef void (*watch_callback_fn)(const char *collection,
                                  const char *operation,
                                  const char *doc_id,
                                  const char *doc_json,
                                  void *user_data);

/* Database Operations */
CDatabase *jasonisnthappy_open(const char *path, CError *error_out);
CDatabase *jasonisnthappy_open_with_options(const char *path, CDatabaseOptions options, CError *error_out);
void jasonisnthappy_close(CDatabase *db);
CDatabaseOptions jasonisnthappy_default_database_options(void);
CTransactionConfig jasonisnthappy_default_transaction_config(void);
int32_t jasonisnthappy_set_transaction_config(CDatabase *db, CTransactionConfig config, CError *error_out);
int32_t jasonisnthappy_get_transaction_config(CDatabase *db, CTransactionConfig *config_out, CError *error_out);
int32_t jasonisnthappy_set_auto_checkpoint_threshold(CDatabase *db, uint64_t threshold, CError *error_out);
int32_t jasonisnthappy_get_path(CDatabase *db, char **path_out, CError *error_out);
int32_t jasonisnthappy_is_read_only(CDatabase *db, bool *read_only_out, CError *error_out);
uintptr_t jasonisnthappy_max_bulk_operations(CDatabase *db, CError *error_out);
uintptr_t jasonisnthappy_max_document_size(CDatabase *db, CError *error_out);
uintptr_t jasonisnthappy_max_request_body_size(CDatabase *db, CError *error_out);
int32_t jasonisnthappy_list_collections(CDatabase *db, char **json_out, CError *error_out);
int32_t jasonisnthappy_collection_stats(CDatabase *db, const char *collection_name, char **json_out, CError *error_out);
int32_t jasonisnthappy_database_info(CDatabase *db, char **json_out, CError *error_out);
int32_t jasonisnthappy_list_indexes(CDatabase *db, const char *collection_name, char **json_out, CError *error_out);
int32_t jasonisnthappy_create_index(CDatabase *db, const char *collection_name, const char *index_name, const char *field, bool unique, CError *error_out);
int32_t jasonisnthappy_create_compound_index(CDatabase *db, const char *collection_name, const char *index_name, const char *fields_json, bool unique, CError *error_out);
int32_t jasonisnthappy_create_text_index(CDatabase *db, const char *collection_name, const char *index_name, const char *field, CError *error_out);
int32_t jasonisnthappy_drop_index(CDatabase *db, const char *collection_name, const char *index_name, CError *error_out);
int32_t jasonisnthappy_set_schema(CDatabase *db, const char *collection_name, const char *schema_json, CError *error_out);
int32_t jasonisnthappy_get_schema(CDatabase *db, const char *collection_name, char **schema_out, CError *error_out);
int32_t jasonisnthappy_remove_schema(CDatabase *db, const char *collection_name, CError *error_out);
int32_t jasonisnthappy_checkpoint(CDatabase *db, CError *error_out);
int32_t jasonisnthappy_backup(CDatabase *db, const char *dest_path, CError *error_out);
int32_t jasonisnthappy_garbage_collect(CDatabase *db, char **result_out, CError *error_out);
int32_t jasonisnthappy_metrics(CDatabase *db, char **json_out, CError *error_out);
int64_t jasonisnthappy_frame_count(CDatabase *db, CError *error_out);
int32_t jasonisnthappy_verify_backup(const char *backup_path, char **info_out, CError *error_out);

/* Transaction Operations */
CTransaction *jasonisnthappy_begin_transaction(CDatabase *db, CError *error_out);
int32_t jasonisnthappy_commit(CTransaction *tx, CError *error_out);
void jasonisnthappy_rollback(CTransaction *tx);
int32_t jasonisnthappy_run_transaction(CDatabase *db, transaction_callback_fn callback, void *user_data, CError *error_out);
int32_t jasonisnthappy_transaction_is_active(CTransaction *tx, bool *is_active_out, CError *error_out);
int32_t jasonisnthappy_insert(CTransaction *tx, const char *collection_name, const char *doc_json, char **id_out, CError *error_out);
int32_t jasonisnthappy_find_by_id(CTransaction *tx, const char *collection_name, const char *doc_id, char **doc_out, CError *error_out);
int32_t jasonisnthappy_update_by_id(CTransaction *tx, const char *collection_name, const char *doc_id, const char *doc_json, CError *error_out);
int32_t jasonisnthappy_delete_by_id(CTransaction *tx, const char *collection_name, const char *doc_id, CError *error_out);
int32_t jasonisnthappy_find_all(CTransaction *tx, const char *collection_name, char **docs_out, CError *error_out);
int64_t jasonisnthappy_count(CTransaction *tx, const char *collection_name, CError *error_out);
int32_t jasonisnthappy_create_collection(CTransaction *tx, const char *collection_name, CError *error_out);
int32_t jasonisnthappy_drop_collection(CTransaction *tx, const char *collection_name, CError *error_out);
int32_t jasonisnthappy_rename_collection(CTransaction *tx, const char *old_name, const char *new_name, CError *error_out);

/* Collection Operations */
CCollection *jasonisnthappy_get_collection(CDatabase *db, const char *collection_name, CError *error_out);
void jasonisnthappy_collection_free(CCollection *coll);
int32_t jasonisnthappy_collection_name(CCollection *coll, char **name_out, CError *error_out);
int32_t jasonisnthappy_collection_insert(CCollection *coll, const char *doc_json, char **id_out, CError *error_out);
int32_t jasonisnthappy_collection_find_by_id(CCollection *coll, const char *doc_id, char **doc_out, CError *error_out);
int32_t jasonisnthappy_collection_update_by_id(CCollection *coll, const char *doc_id, const char *updates_json, CError *error_out);
int32_t jasonisnthappy_collection_delete_by_id(CCollection *coll, const char *doc_id, CError *error_out);
int32_t jasonisnthappy_collection_find_all(CCollection *coll, char **docs_out, CError *error_out);
int64_t jasonisnthappy_collection_count(CCollection *coll, CError *error_out);
int32_t jasonisnthappy_collection_find(CCollection *coll, const char *filter, char **docs_out, CError *error_out);
int32_t jasonisnthappy_collection_find_one(CCollection *coll, const char *filter, char **doc_out, CError *error_out);
int64_t jasonisnthappy_collection_update(CCollection *coll, const char *filter, const char *updates_json, CError *error_out);
int32_t jasonisnthappy_collection_update_one(CCollection *coll, const char *filter, const char *updates_json, bool *updated_out, CError *error_out);
int64_t jasonisnthappy_collection_delete(CCollection *coll, const char *filter, CError *error_out);
int32_t jasonisnthappy_collection_delete_one(CCollection *coll, const char *filter, bool *deleted_out, CError *error_out);
int32_t jasonisnthappy_collection_upsert_by_id(CCollection *coll, const char *doc_id, const char *doc_json, char **result_out, CError *error_out);
int32_t jasonisnthappy_collection_upsert(CCollection *coll, const char *filter, const char *doc_json, char **result_out, CError *error_out);
int32_t jasonisnthappy_collection_insert_many(CCollection *coll, const char *docs_json, char **ids_out, CError *error_out);
int32_t jasonisnthappy_collection_bulk_write(CCollection *coll, const char *operations_json, bool ordered, char **result_out, CError *error_out);
int32_t jasonisnthappy_collection_aggregate(CCollection *coll, const char *pipeline_json, char **result_out, CError *error_out);
int32_t jasonisnthappy_collection_distinct(CCollection *coll, const char *field, char **values_out, CError *error_out);
int64_t jasonisnthappy_collection_count_distinct(CCollection *coll, const char *field, CError *error_out);
int32_t jasonisnthappy_collection_search(CCollection *coll, const char *query, char **results_out, CError *error_out);
int64_t jasonisnthappy_collection_count_with_query(CCollection *coll, const char *filter, CError *error_out);
int32_t jasonisnthappy_collection_query_with_options(CCollection *coll, const char *filter, const char *sort_field, bool sort_asc, int64_t limit, int64_t skip, const char *project_fields, const char *exclude_fields, char **docs_out, CError *error_out);
int64_t jasonisnthappy_collection_query_count(CCollection *coll, const char *filter, int64_t skip, int64_t limit, CError *error_out);
int32_t jasonisnthappy_collection_query_first(CCollection *coll, const char *filter, const char *sort_field, bool sort_asc, char **doc_out, CError *error_out);

/* Watch Operations */
int32_t jasonisnthappy_collection_watch_start(CCollection *coll, const char *filter, watch_callback_fn callback, void *user_data, CWatchHandle **handle_out, CError *error_out);
void jasonisnthappy_watch_stop(CWatchHandle *handle);

/* Web Server */
CWebServer *jasonisnthappy_start_web_server(CDatabase *db, const char *addr, CError *error_out);
void jasonisnthappy_stop_web_server(CWebServer *server);

/* Utility */
void jasonisnthappy_free_string(char *s);
void jasonisnthappy_free_error(CError error);

#endif /* JASONISNTHAPPY_H */
