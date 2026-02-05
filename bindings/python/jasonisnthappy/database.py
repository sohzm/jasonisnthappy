"""
Python bindings for jasonisnthappy database using ctypes.
"""

import ctypes
import json
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable


@dataclass
class UpsertResult:
    """Result of an upsert operation."""
    id: str
    inserted: bool


def _get_library_path():
    """Get library path, checking package directory first, then fallback to loader."""
    system = platform.system()
    machine = platform.machine().lower()

    if system == "Darwin":
        lib_dir = "darwin-arm64" if machine == "arm64" else "darwin-amd64"
        lib_name = "libjasonisnthappy.dylib"
    elif system == "Linux":
        lib_dir = "linux-arm64" if machine in ("aarch64", "arm64") else "linux-amd64"
        lib_name = "libjasonisnthappy.so"
    elif system == "Windows":
        lib_dir = "windows-amd64"
        lib_name = "jasonisnthappy.dll"
    else:
        # Unsupported platform, let loader handle it
        from .loader import get_library_path
        return get_library_path()

    package_dir = Path(__file__).parent

    # Check package root directory first (where wheel build puts it)
    root_lib_path = package_dir / lib_name
    if root_lib_path.exists():
        return str(root_lib_path)

    # Check lib/<platform>/ directory
    platform_lib_path = package_dir / "lib" / lib_dir / lib_name
    if platform_lib_path.exists():
        return str(platform_lib_path)

    # Fallback to loader (downloads to ~/.jasonisnthappy/)
    from .loader import get_library_path
    return get_library_path()


# Load the library (from package dir or auto-download if needed)
_lib = ctypes.CDLL(_get_library_path())


# ==================
# C Structures
# ==================

class CError(ctypes.Structure):
    _fields_ = [
        ("code", ctypes.c_int32),
        ("message", ctypes.c_char_p),
    ]


class CDatabaseOptions(ctypes.Structure):
    _fields_ = [
        ("cache_size", ctypes.c_size_t),
        ("auto_checkpoint_threshold", ctypes.c_uint64),
        ("file_permissions", ctypes.c_uint32),
        ("read_only", ctypes.c_bool),
        ("max_bulk_operations", ctypes.c_size_t),
        ("max_document_size", ctypes.c_size_t),
        ("max_request_body_size", ctypes.c_size_t),
    ]


class CTransactionConfig(ctypes.Structure):
    _fields_ = [
        ("max_retries", ctypes.c_size_t),
        ("retry_backoff_base_ms", ctypes.c_uint64),
        ("max_retry_backoff_ms", ctypes.c_uint64),
    ]


# ==================
# Function Signatures (85 total)
# ==================

# Database Operations (21)
_lib.jasonisnthappy_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_open.restype = ctypes.c_void_p

_lib.jasonisnthappy_open_with_options.argtypes = [ctypes.c_char_p, CDatabaseOptions, ctypes.POINTER(CError)]
_lib.jasonisnthappy_open_with_options.restype = ctypes.c_void_p

_lib.jasonisnthappy_close.argtypes = [ctypes.c_void_p]
_lib.jasonisnthappy_close.restype = None

_lib.jasonisnthappy_default_database_options.argtypes = []
_lib.jasonisnthappy_default_database_options.restype = CDatabaseOptions

_lib.jasonisnthappy_default_transaction_config.argtypes = []
_lib.jasonisnthappy_default_transaction_config.restype = CTransactionConfig

_lib.jasonisnthappy_set_transaction_config.argtypes = [ctypes.c_void_p, CTransactionConfig, ctypes.POINTER(CError)]
_lib.jasonisnthappy_set_transaction_config.restype = ctypes.c_int32

_lib.jasonisnthappy_get_transaction_config.argtypes = [ctypes.c_void_p, ctypes.POINTER(CTransactionConfig), ctypes.POINTER(CError)]
_lib.jasonisnthappy_get_transaction_config.restype = ctypes.c_int32

_lib.jasonisnthappy_set_auto_checkpoint_threshold.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(CError)]
_lib.jasonisnthappy_set_auto_checkpoint_threshold.restype = ctypes.c_int32

_lib.jasonisnthappy_get_path.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_get_path.restype = ctypes.c_int32

_lib.jasonisnthappy_is_read_only.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_bool), ctypes.POINTER(CError)]
_lib.jasonisnthappy_is_read_only.restype = ctypes.c_int32

_lib.jasonisnthappy_max_bulk_operations.argtypes = [ctypes.c_void_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_max_bulk_operations.restype = ctypes.c_size_t

_lib.jasonisnthappy_max_document_size.argtypes = [ctypes.c_void_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_max_document_size.restype = ctypes.c_size_t

_lib.jasonisnthappy_max_request_body_size.argtypes = [ctypes.c_void_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_max_request_body_size.restype = ctypes.c_size_t

_lib.jasonisnthappy_list_collections.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_list_collections.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_stats.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_stats.restype = ctypes.c_int32

_lib.jasonisnthappy_database_info.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_database_info.restype = ctypes.c_int32

_lib.jasonisnthappy_list_indexes.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_list_indexes.restype = ctypes.c_int32

_lib.jasonisnthappy_create_index.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_bool, ctypes.POINTER(CError)]
_lib.jasonisnthappy_create_index.restype = ctypes.c_int32

_lib.jasonisnthappy_create_compound_index.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_bool, ctypes.POINTER(CError)]
_lib.jasonisnthappy_create_compound_index.restype = ctypes.c_int32

_lib.jasonisnthappy_create_text_index.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_create_text_index.restype = ctypes.c_int32

_lib.jasonisnthappy_drop_index.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_drop_index.restype = ctypes.c_int32

_lib.jasonisnthappy_set_schema.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_set_schema.restype = ctypes.c_int32

_lib.jasonisnthappy_get_schema.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_get_schema.restype = ctypes.c_int32

_lib.jasonisnthappy_remove_schema.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_remove_schema.restype = ctypes.c_int32

# Maintenance & Monitoring (6)
_lib.jasonisnthappy_checkpoint.argtypes = [ctypes.c_void_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_checkpoint.restype = ctypes.c_int32

_lib.jasonisnthappy_backup.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_backup.restype = ctypes.c_int32

_lib.jasonisnthappy_verify_backup.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_verify_backup.restype = ctypes.c_int32

_lib.jasonisnthappy_garbage_collect.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_garbage_collect.restype = ctypes.c_int32

_lib.jasonisnthappy_metrics.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_metrics.restype = ctypes.c_int32

_lib.jasonisnthappy_frame_count.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(CError)]
_lib.jasonisnthappy_frame_count.restype = ctypes.c_int32

# Web Server
_lib.jasonisnthappy_start_web_server.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_start_web_server.restype = ctypes.c_void_p

_lib.jasonisnthappy_stop_web_server.argtypes = [ctypes.c_void_p]
_lib.jasonisnthappy_stop_web_server.restype = None

# Watch callback type: void (*)(const char*, const char*, const char*, const char*, void*)
WatchCallbackType = ctypes.CFUNCTYPE(
    None,  # return type
    ctypes.c_char_p,  # collection
    ctypes.c_char_p,  # operation
    ctypes.c_char_p,  # doc_id
    ctypes.c_char_p,  # doc_json
    ctypes.c_void_p,  # user_data
)

_lib.jasonisnthappy_collection_watch_start.argtypes = [
    ctypes.c_void_p,  # coll
    ctypes.c_char_p,  # filter
    WatchCallbackType,  # callback
    ctypes.c_void_p,  # user_data
    ctypes.POINTER(ctypes.c_void_p),  # handle_out
    ctypes.POINTER(CError),  # error_out
]
_lib.jasonisnthappy_collection_watch_start.restype = ctypes.c_int32

_lib.jasonisnthappy_watch_stop.argtypes = [ctypes.c_void_p]
_lib.jasonisnthappy_watch_stop.restype = None

# Transaction Operations (14)
_lib.jasonisnthappy_begin_transaction.argtypes = [ctypes.c_void_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_begin_transaction.restype = ctypes.c_void_p

_lib.jasonisnthappy_commit.argtypes = [ctypes.c_void_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_commit.restype = ctypes.c_int32

_lib.jasonisnthappy_rollback.argtypes = [ctypes.c_void_p]
_lib.jasonisnthappy_rollback.restype = None

_lib.jasonisnthappy_transaction_is_active.argtypes = [ctypes.c_void_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_transaction_is_active.restype = ctypes.c_int32

_lib.jasonisnthappy_insert.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_insert.restype = ctypes.c_int32

_lib.jasonisnthappy_find_by_id.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_find_by_id.restype = ctypes.c_int32

_lib.jasonisnthappy_update_by_id.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_update_by_id.restype = ctypes.c_int32

_lib.jasonisnthappy_delete_by_id.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_delete_by_id.restype = ctypes.c_int32

_lib.jasonisnthappy_find_all.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_find_all.restype = ctypes.c_int32

_lib.jasonisnthappy_count.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(CError)]
_lib.jasonisnthappy_count.restype = ctypes.c_int32

_lib.jasonisnthappy_create_collection.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_create_collection.restype = ctypes.c_int32

_lib.jasonisnthappy_drop_collection.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_drop_collection.restype = ctypes.c_int32

_lib.jasonisnthappy_rename_collection.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_rename_collection.restype = ctypes.c_int32

# Collection Operations (50+)
_lib.jasonisnthappy_get_collection.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_get_collection.restype = ctypes.c_void_p

_lib.jasonisnthappy_collection_free.argtypes = [ctypes.c_void_p]
_lib.jasonisnthappy_collection_free.restype = None

_lib.jasonisnthappy_collection_insert.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_insert.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_find_by_id.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_find_by_id.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_update_by_id.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_update_by_id.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_delete_by_id.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_delete_by_id.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_find_all.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_find_all.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_count.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_count.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_name.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_name.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_find.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_find.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_find_one.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_find_one.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_update.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_update.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_update_one.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_bool), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_update_one.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_delete.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_delete_one.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_bool), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_delete_one.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_upsert_by_id.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_int32), ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_upsert_by_id.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_upsert.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_int32), ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_upsert.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_insert_many.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_insert_many.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_distinct.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_distinct.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_count_distinct.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_count_distinct.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_search.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_search.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_count_with_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_count_with_query.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_query_with_options.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_bool, ctypes.c_uint64, ctypes.c_uint64, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_query_with_options.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_query_count.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_query_count.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_query_first.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_bool, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_query_first.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_bulk_write.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_bool, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_bulk_write.restype = ctypes.c_int32

_lib.jasonisnthappy_collection_aggregate.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(CError)]
_lib.jasonisnthappy_collection_aggregate.restype = ctypes.c_int32

# Utility (2)
_lib.jasonisnthappy_free_string.argtypes = [ctypes.c_char_p]
_lib.jasonisnthappy_free_string.restype = None

_lib.jasonisnthappy_free_error.argtypes = [CError]
_lib.jasonisnthappy_free_error.restype = None


# ==================
# Helper Functions
# ==================

def _check_error(error: CError) -> None:
    """Check if an error occurred and raise an exception if so."""
    if error.code != 0 and error.message:
        message = error.message.decode("utf-8")
        _lib.jasonisnthappy_free_error(error)
        raise RuntimeError(message)


# ==================
# Database Class
# ==================

class Database:
    """
    Database represents a jasonisnthappy database instance.

    Example:
        >>> db = Database.open("./my_database.db")
        >>> try:
        ...     tx = db.begin_transaction()
        ...     # ... work with transaction
        ... finally:
        ...     db.close()
    """

    def __init__(self, db_ptr: int):
        self._db = db_ptr

    @staticmethod
    def open(path: str) -> "Database":
        """Opens a database at the specified path."""
        error = CError()
        db_ptr = _lib.jasonisnthappy_open(path.encode("utf-8"), ctypes.byref(error))

        if not db_ptr:
            _check_error(error)
            raise RuntimeError("Failed to open database")

        return Database(db_ptr)

    @staticmethod
    def open_with_options(path: str, options: CDatabaseOptions) -> "Database":
        """Opens a database with custom options."""
        error = CError()
        db_ptr = _lib.jasonisnthappy_open_with_options(path.encode("utf-8"), options, ctypes.byref(error))

        if not db_ptr:
            _check_error(error)
            raise RuntimeError("Failed to open database")

        return Database(db_ptr)

    @staticmethod
    def default_database_options() -> CDatabaseOptions:
        """Returns default database options."""
        return _lib.jasonisnthappy_default_database_options()

    @staticmethod
    def default_transaction_config() -> CTransactionConfig:
        """Returns default transaction configuration."""
        return _lib.jasonisnthappy_default_transaction_config()

    def close(self) -> None:
        """Closes the database and frees associated resources."""
        if self._db:
            _lib.jasonisnthappy_close(self._db)
            self._db = None

    # Configuration
    def set_transaction_config(self, config: CTransactionConfig) -> None:
        """Sets the transaction configuration."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        result = _lib.jasonisnthappy_set_transaction_config(self._db, config, ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to set transaction config")

    def get_transaction_config(self) -> CTransactionConfig:
        """Gets the current transaction configuration."""
        if not self._db:
            raise RuntimeError("Database is closed")

        config = CTransactionConfig()
        error = CError()
        result = _lib.jasonisnthappy_get_transaction_config(self._db, ctypes.byref(config), ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to get transaction config")

        return config

    def set_auto_checkpoint_threshold(self, threshold: int) -> None:
        """Sets the auto-checkpoint threshold in WAL frames."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        result = _lib.jasonisnthappy_set_auto_checkpoint_threshold(self._db, threshold, ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to set auto-checkpoint threshold")

    # Database Info
    def get_path(self) -> str:
        """Gets the database file path."""
        if not self._db:
            raise RuntimeError("Database is closed")

        path_out = ctypes.c_char_p()
        error = CError()
        result = _lib.jasonisnthappy_get_path(self._db, ctypes.byref(path_out), ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to get path")

        path = path_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(path_out)
        return path

    def is_read_only(self) -> bool:
        """Checks if the database is read-only."""
        if not self._db:
            raise RuntimeError("Database is closed")

        read_only = ctypes.c_bool()
        error = CError()
        result = _lib.jasonisnthappy_is_read_only(self._db, ctypes.byref(read_only), ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to check read-only status")

        return read_only.value

    def max_bulk_operations(self) -> int:
        """Returns the maximum number of bulk operations allowed."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        result = _lib.jasonisnthappy_max_bulk_operations(self._db, ctypes.byref(error))
        return result

    def max_document_size(self) -> int:
        """Returns the maximum document size in bytes."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        result = _lib.jasonisnthappy_max_document_size(self._db, ctypes.byref(error))
        return result

    def max_request_body_size(self) -> int:
        """Returns the maximum HTTP request body size in bytes."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        result = _lib.jasonisnthappy_max_request_body_size(self._db, ctypes.byref(error))
        return result

    def list_collections(self) -> List[str]:
        """Lists all collections in the database."""
        if not self._db:
            raise RuntimeError("Database is closed")

        json_out = ctypes.c_char_p()
        error = CError()
        result = _lib.jasonisnthappy_list_collections(self._db, ctypes.byref(json_out), ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to list collections")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """Gets statistics for a collection."""
        if not self._db:
            raise RuntimeError("Database is closed")

        json_out = ctypes.c_char_p()
        error = CError()
        result = _lib.jasonisnthappy_collection_stats(
            self._db,
            collection_name.encode("utf-8"),
            ctypes.byref(json_out),
            ctypes.byref(error)
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to get collection stats")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def database_info(self) -> Dict[str, Any]:
        """Gets database information."""
        if not self._db:
            raise RuntimeError("Database is closed")

        json_out = ctypes.c_char_p()
        error = CError()
        result = _lib.jasonisnthappy_database_info(self._db, ctypes.byref(json_out), ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to get database info")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    # Index Management
    def list_indexes(self, collection_name: str) -> List[Dict[str, Any]]:
        """Lists all indexes for a collection."""
        if not self._db:
            raise RuntimeError("Database is closed")

        json_out = ctypes.c_char_p()
        error = CError()
        result = _lib.jasonisnthappy_list_indexes(
            self._db,
            collection_name.encode("utf-8"),
            ctypes.byref(json_out),
            ctypes.byref(error)
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to list indexes")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def create_index(self, collection_name: str, index_name: str, field: str, unique: bool = False) -> None:
        """Creates a single-field index."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        result = _lib.jasonisnthappy_create_index(
            self._db,
            collection_name.encode("utf-8"),
            index_name.encode("utf-8"),
            field.encode("utf-8"),
            unique,
            ctypes.byref(error)
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to create index")

    def create_compound_index(self, collection_name: str, index_name: str, fields: List[str], unique: bool = False) -> None:
        """Creates a compound index on multiple fields."""
        if not self._db:
            raise RuntimeError("Database is closed")

        fields_json = json.dumps(fields)
        error = CError()
        result = _lib.jasonisnthappy_create_compound_index(
            self._db,
            collection_name.encode("utf-8"),
            index_name.encode("utf-8"),
            fields_json.encode("utf-8"),
            unique,
            ctypes.byref(error)
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to create compound index")

    def create_text_index(self, collection_name: str, index_name: str, field: str) -> None:
        """Creates a full-text search index."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        result = _lib.jasonisnthappy_create_text_index(
            self._db,
            collection_name.encode("utf-8"),
            index_name.encode("utf-8"),
            field.encode("utf-8"),
            ctypes.byref(error)
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to create text index")

    def drop_index(self, collection_name: str, index_name: str) -> None:
        """Drops an index (stub implementation)."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        result = _lib.jasonisnthappy_drop_index(
            self._db,
            collection_name.encode("utf-8"),
            index_name.encode("utf-8"),
            ctypes.byref(error)
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to drop index")

    # Schema Validation
    def set_schema(self, collection_name: str, schema: Dict[str, Any]) -> None:
        """Sets a JSON schema for validation."""
        if not self._db:
            raise RuntimeError("Database is closed")

        schema_json = json.dumps(schema)
        error = CError()
        result = _lib.jasonisnthappy_set_schema(
            self._db,
            collection_name.encode("utf-8"),
            schema_json.encode("utf-8"),
            ctypes.byref(error)
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to set schema")

    def get_schema(self, collection_name: str) -> Optional[Dict[str, Any]]:
        """Gets the JSON schema for a collection."""
        if not self._db:
            raise RuntimeError("Database is closed")

        json_out = ctypes.c_char_p()
        error = CError()
        result = _lib.jasonisnthappy_get_schema(
            self._db,
            collection_name.encode("utf-8"),
            ctypes.byref(json_out),
            ctypes.byref(error)
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to get schema")

        if not json_out.value:
            return None

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def remove_schema(self, collection_name: str) -> None:
        """Removes the JSON schema from a collection."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        result = _lib.jasonisnthappy_remove_schema(
            self._db,
            collection_name.encode("utf-8"),
            ctypes.byref(error)
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to remove schema")

    # Maintenance
    def checkpoint(self) -> None:
        """Performs a manual WAL checkpoint."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        result = _lib.jasonisnthappy_checkpoint(self._db, ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to checkpoint")

    def backup(self, dest_path: str) -> None:
        """Creates a backup of the database."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        result = _lib.jasonisnthappy_backup(self._db, dest_path.encode("utf-8"), ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to backup")

    def verify_backup(self, backup_path: str) -> Dict[str, Any]:
        """Verifies the integrity of a backup and returns backup info."""
        if not self._db:
            raise RuntimeError("Database is closed")

        json_out = ctypes.c_char_p()
        error = CError()
        result = _lib.jasonisnthappy_verify_backup(
            self._db,
            backup_path.encode("utf-8"),
            ctypes.byref(json_out),
            ctypes.byref(error)
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to verify backup")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def garbage_collect(self) -> Dict[str, Any]:
        """Performs garbage collection and returns stats."""
        if not self._db:
            raise RuntimeError("Database is closed")

        json_out = ctypes.c_char_p()
        error = CError()
        result = _lib.jasonisnthappy_garbage_collect(self._db, ctypes.byref(json_out), ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to garbage collect")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def metrics(self) -> Dict[str, Any]:
        """Gets database metrics."""
        if not self._db:
            raise RuntimeError("Database is closed")

        json_out = ctypes.c_char_p()
        error = CError()
        result = _lib.jasonisnthappy_metrics(self._db, ctypes.byref(json_out), ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to get metrics")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def frame_count(self) -> int:
        """Gets the number of WAL frames."""
        if not self._db:
            raise RuntimeError("Database is closed")

        count = ctypes.c_uint64()
        error = CError()
        result = _lib.jasonisnthappy_frame_count(self._db, ctypes.byref(count), ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to get frame count")

        return count.value

    # Transaction Operations
    def begin_transaction(self) -> "Transaction":
        """Begins a new transaction."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        tx_ptr = _lib.jasonisnthappy_begin_transaction(self._db, ctypes.byref(error))

        if not tx_ptr:
            _check_error(error)
            raise RuntimeError("Failed to begin transaction")

        return Transaction(tx_ptr)

    def get_collection(self, name: str) -> "Collection":
        """Gets a collection reference for non-transactional operations."""
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        coll_ptr = _lib.jasonisnthappy_get_collection(self._db, name.encode("utf-8"), ctypes.byref(error))

        if not coll_ptr:
            _check_error(error)
            raise RuntimeError("Failed to get collection")

        return Collection(coll_ptr)

    def start_web_ui(self, addr: str) -> "WebServer":
        """
        Starts the web UI server at the given address.

        Returns a WebServer handle that can be used to stop the server.

        Example:
            >>> server = db.start_web_ui("127.0.0.1:8080")
            >>> print("Web UI available at http://127.0.0.1:8080")
            >>> # ... later ...
            >>> server.stop()
        """
        if not self._db:
            raise RuntimeError("Database is closed")

        error = CError()
        server_ptr = _lib.jasonisnthappy_start_web_server(
            self._db, addr.encode("utf-8"), ctypes.byref(error)
        )

        if not server_ptr:
            _check_error(error)
            raise RuntimeError("Failed to start web server")

        return WebServer(server_ptr)

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


# ==================
# WebServer Class
# ==================

class WebServer:
    """
    WebServer represents a running web UI server.

    Example:
        >>> server = db.start_web_ui("127.0.0.1:8080")
        >>> print("Web UI available at http://127.0.0.1:8080")
        >>> # ... later ...
        >>> server.stop()
    """

    def __init__(self, server_ptr):
        self._server = server_ptr

    def stop(self) -> None:
        """Stops the web server."""
        if self._server:
            _lib.jasonisnthappy_stop_web_server(self._server)
            self._server = None

    def __enter__(self) -> "WebServer":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()


# ==================
# Transaction Class
# ==================

class Transaction:
    """
    Transaction represents a database transaction.

    Example:
        >>> tx = db.begin_transaction()
        >>> try:
        ...     doc_id = tx.insert("users", {"name": "Alice", "age": 30})
        ...     doc = tx.find_by_id("users", doc_id)
        ...     tx.commit()
        ... except Exception:
        ...     tx.rollback()
        ...     raise
    """

    def __init__(self, tx_ptr: int):
        self._tx = tx_ptr

    def is_active(self) -> bool:
        """Checks if the transaction is still active."""
        if not self._tx:
            return False

        error = CError()
        result = _lib.jasonisnthappy_transaction_is_active(self._tx, ctypes.byref(error))

        if result < 0:
            _check_error(error)
            raise RuntimeError("Failed to check transaction status")

        return result == 1

    def commit(self) -> None:
        """Commits the transaction."""
        if not self._tx:
            raise RuntimeError("Transaction is already closed")

        error = CError()
        result = _lib.jasonisnthappy_commit(self._tx, ctypes.byref(error))
        self._tx = None

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to commit transaction")

    def rollback(self) -> None:
        """Rolls back the transaction."""
        if self._tx:
            _lib.jasonisnthappy_rollback(self._tx)
            self._tx = None

    def insert(self, collection_name: str, doc: Dict[str, Any]) -> str:
        """Inserts a document into a collection."""
        if not self._tx:
            raise RuntimeError("Transaction is closed")

        json_str = json.dumps(doc)
        id_out = ctypes.c_char_p()
        error = CError()

        result = _lib.jasonisnthappy_insert(
            self._tx,
            collection_name.encode("utf-8"),
            json_str.encode("utf-8"),
            ctypes.byref(id_out),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to insert document")

        doc_id = id_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(id_out)
        return doc_id

    def find_by_id(self, collection_name: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Finds a document by its ID."""
        if not self._tx:
            raise RuntimeError("Transaction is closed")

        json_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_find_by_id(
            self._tx,
            collection_name.encode("utf-8"),
            doc_id.encode("utf-8"),
            ctypes.byref(json_out),
            ctypes.byref(error),
        )

        if status == -1:
            _check_error(error)
            raise RuntimeError("Failed to find document")

        if status == 1 or not json_out:
            return None

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)

        return json.loads(json_str)

    def update_by_id(self, collection_name: str, doc_id: str, doc: Dict[str, Any]) -> None:
        """Updates a document by its ID."""
        if not self._tx:
            raise RuntimeError("Transaction is closed")

        json_str = json.dumps(doc)
        error = CError()

        result = _lib.jasonisnthappy_update_by_id(
            self._tx,
            collection_name.encode("utf-8"),
            doc_id.encode("utf-8"),
            json_str.encode("utf-8"),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to update document")

    def delete_by_id(self, collection_name: str, doc_id: str) -> None:
        """Deletes a document by its ID."""
        if not self._tx:
            raise RuntimeError("Transaction is closed")

        error = CError()
        result = _lib.jasonisnthappy_delete_by_id(
            self._tx,
            collection_name.encode("utf-8"),
            doc_id.encode("utf-8"),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to delete document")

    def find_all(self, collection_name: str) -> List[Dict[str, Any]]:
        """Finds all documents in a collection."""
        if not self._tx:
            raise RuntimeError("Transaction is closed")

        json_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_find_all(
            self._tx,
            collection_name.encode("utf-8"),
            ctypes.byref(json_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to find all documents")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)

        return json.loads(json_str)

    def count(self, collection_name: str) -> int:
        """Counts documents in a collection."""
        if not self._tx:
            raise RuntimeError("Transaction is closed")

        count = ctypes.c_uint64()
        error = CError()

        result = _lib.jasonisnthappy_count(
            self._tx,
            collection_name.encode("utf-8"),
            ctypes.byref(count),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to count documents")

        return count.value

    def create_collection(self, collection_name: str) -> None:
        """Creates a new collection."""
        if not self._tx:
            raise RuntimeError("Transaction is closed")

        error = CError()
        result = _lib.jasonisnthappy_create_collection(
            self._tx,
            collection_name.encode("utf-8"),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to create collection")

    def drop_collection(self, collection_name: str) -> None:
        """Drops a collection."""
        if not self._tx:
            raise RuntimeError("Transaction is closed")

        error = CError()
        result = _lib.jasonisnthappy_drop_collection(
            self._tx,
            collection_name.encode("utf-8"),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to drop collection")

    def rename_collection(self, old_name: str, new_name: str) -> None:
        """Renames a collection."""
        if not self._tx:
            raise RuntimeError("Transaction is closed")

        error = CError()
        result = _lib.jasonisnthappy_rename_collection(
            self._tx,
            old_name.encode("utf-8"),
            new_name.encode("utf-8"),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to rename collection")

    def __enter__(self) -> "Transaction":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()


# ==================
# Collection Class
# ==================

class Collection:
    """
    Collection represents a non-transactional collection handle.

    Example:
        >>> coll = db.get_collection("users")
        >>> try:
        ...     doc_id = coll.insert({"name": "Bob", "age": 25})
        ...     doc = coll.find_by_id(doc_id)
        ... finally:
        ...     coll.close()
    """

    def __init__(self, coll_ptr: int):
        self._coll = coll_ptr

    def close(self) -> None:
        """Frees the collection handle."""
        if self._coll:
            _lib.jasonisnthappy_collection_free(self._coll)
            self._coll = None

    def name(self) -> str:
        """Gets the collection name."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        name_out = ctypes.c_char_p()
        error = CError()
        result = _lib.jasonisnthappy_collection_name(self._coll, ctypes.byref(name_out), ctypes.byref(error))

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to get collection name")

        name = name_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(name_out)
        return name

    # Basic CRUD
    def insert(self, doc: Dict[str, Any]) -> str:
        """Inserts a document."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        json_str = json.dumps(doc)
        id_out = ctypes.c_char_p()
        error = CError()

        result = _lib.jasonisnthappy_collection_insert(
            self._coll,
            json_str.encode("utf-8"),
            ctypes.byref(id_out),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to insert document")

        doc_id = id_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(id_out)
        return doc_id

    def find_by_id(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Finds a document by ID."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        json_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_find_by_id(
            self._coll,
            doc_id.encode("utf-8"),
            ctypes.byref(json_out),
            ctypes.byref(error),
        )

        if status == -1:
            _check_error(error)
            raise RuntimeError("Failed to find document")

        if status == 1 or not json_out:
            return None

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def update_by_id(self, doc_id: str, doc: Dict[str, Any]) -> None:
        """Updates a document by ID."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        json_str = json.dumps(doc)
        error = CError()

        result = _lib.jasonisnthappy_collection_update_by_id(
            self._coll,
            doc_id.encode("utf-8"),
            json_str.encode("utf-8"),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to update document")

    def delete_by_id(self, doc_id: str) -> None:
        """Deletes a document by ID."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        error = CError()
        result = _lib.jasonisnthappy_collection_delete_by_id(
            self._coll,
            doc_id.encode("utf-8"),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to delete document")

    def find_all(self) -> List[Dict[str, Any]]:
        """Finds all documents."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        json_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_find_all(
            self._coll,
            ctypes.byref(json_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to find all documents")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def count(self) -> int:
        """Counts all documents."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        count = ctypes.c_uint64()
        error = CError()

        result = _lib.jasonisnthappy_collection_count(
            self._coll,
            ctypes.byref(count),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to count documents")

        return count.value

    # Query/Filter Operations
    def find(self, filter_str: str) -> List[Dict[str, Any]]:
        """Finds documents matching a filter."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        json_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_find(
            self._coll,
            filter_str.encode("utf-8"),
            ctypes.byref(json_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to find documents")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def find_one(self, filter_str: str) -> Optional[Dict[str, Any]]:
        """Finds first document matching a filter."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        json_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_find_one(
            self._coll,
            filter_str.encode("utf-8"),
            ctypes.byref(json_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to find document")

        if not json_out or not json_out.value:
            return None

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)

        if json_str == "null" or not json_str:
            return None

        return json.loads(json_str)

    def update(self, filter_str: str, update: Dict[str, Any]) -> int:
        """Updates all documents matching a filter. Returns count updated."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        update_json = json.dumps(update)
        count = ctypes.c_uint64()
        error = CError()

        result = _lib.jasonisnthappy_collection_update(
            self._coll,
            filter_str.encode("utf-8"),
            update_json.encode("utf-8"),
            ctypes.byref(count),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to update documents")

        return count.value

    def update_one(self, filter_str: str, update: Dict[str, Any]) -> bool:
        """Updates first document matching a filter. Returns True if updated."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        update_json = json.dumps(update)
        updated = ctypes.c_bool()
        error = CError()

        result = _lib.jasonisnthappy_collection_update_one(
            self._coll,
            filter_str.encode("utf-8"),
            update_json.encode("utf-8"),
            ctypes.byref(updated),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to update document")

        return updated.value

    def delete(self, filter_str: str) -> int:
        """Deletes all documents matching a filter. Returns count deleted."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        count = ctypes.c_uint64()
        error = CError()

        result = _lib.jasonisnthappy_collection_delete(
            self._coll,
            filter_str.encode("utf-8"),
            ctypes.byref(count),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to delete documents")

        return count.value

    def delete_one(self, filter_str: str) -> bool:
        """Deletes first document matching a filter. Returns True if deleted."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        deleted = ctypes.c_bool()
        error = CError()

        result = _lib.jasonisnthappy_collection_delete_one(
            self._coll,
            filter_str.encode("utf-8"),
            ctypes.byref(deleted),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to delete document")

        return deleted.value

    # Upsert Operations
    def upsert_by_id(self, doc_id: str, doc: Dict[str, Any]) -> UpsertResult:
        """Upserts a document by ID. Returns UpsertResult with id and inserted flag."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        doc_json = json.dumps(doc)
        result_code = ctypes.c_int32()
        id_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_upsert_by_id(
            self._coll,
            doc_id.encode("utf-8"),
            doc_json.encode("utf-8"),
            ctypes.byref(result_code),
            ctypes.byref(id_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to upsert document")

        result_id = id_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(id_out)
        # result_code: 0 = Inserted, 1 = Updated
        return UpsertResult(id=result_id, inserted=(result_code.value == 0))

    def upsert(self, filter_str: str, doc: Dict[str, Any]) -> UpsertResult:
        """Upserts a document matching a filter. Returns UpsertResult with id and inserted flag."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        doc_json = json.dumps(doc)
        result_code = ctypes.c_int32()
        id_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_upsert(
            self._coll,
            filter_str.encode("utf-8"),
            doc_json.encode("utf-8"),
            ctypes.byref(result_code),
            ctypes.byref(id_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to upsert document")

        result_id = id_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(id_out)
        # result_code: 0 = Inserted, 1 = Updated
        return UpsertResult(id=result_id, inserted=(result_code.value == 0))

    # Bulk Operations
    def insert_many(self, docs: List[Dict[str, Any]]) -> List[str]:
        """Inserts multiple documents. Returns list of IDs."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        docs_json = json.dumps(docs)
        ids_out = ctypes.c_char_p()
        error = CError()

        result = _lib.jasonisnthappy_collection_insert_many(
            self._coll,
            docs_json.encode("utf-8"),
            ctypes.byref(ids_out),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to insert documents")

        ids_str = ids_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(ids_out)
        return json.loads(ids_str)

    # Advanced Operations
    def distinct(self, field: str) -> List[Any]:
        """Gets distinct values for a field."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        json_out = ctypes.c_char_p()
        error = CError()

        result = _lib.jasonisnthappy_collection_distinct(
            self._coll,
            field.encode("utf-8"),
            ctypes.byref(json_out),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to get distinct values")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def count_distinct(self, field: str) -> int:
        """Counts distinct values for a field."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        count = ctypes.c_uint64()
        error = CError()

        result = _lib.jasonisnthappy_collection_count_distinct(
            self._coll,
            field.encode("utf-8"),
            ctypes.byref(count),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to count distinct values")

        return count.value

    def search(self, query: str) -> List[Dict[str, Any]]:
        """Performs full-text search."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        json_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_search(
            self._coll,
            query.encode("utf-8"),
            ctypes.byref(json_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to search")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def count_with_query(self, filter_str: str) -> int:
        """Counts documents matching a filter."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        count = ctypes.c_uint64()
        error = CError()

        result = _lib.jasonisnthappy_collection_count_with_query(
            self._coll,
            filter_str.encode("utf-8"),
            ctypes.byref(count),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to count documents")

        return count.value

    # Query Builder Helpers
    def query_with_options(
        self,
        filter_str: Optional[str] = None,
        sort_field: Optional[str] = None,
        sort_asc: bool = True,
        limit: int = 0,
        skip: int = 0,
        project_fields: Optional[List[str]] = None,
        exclude_fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Executes a query with all options."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        filter_c = filter_str.encode("utf-8") if filter_str else None
        sort_c = sort_field.encode("utf-8") if sort_field else None
        project_c = json.dumps(project_fields).encode("utf-8") if project_fields else None
        exclude_c = json.dumps(exclude_fields).encode("utf-8") if exclude_fields else None

        json_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_query_with_options(
            self._coll,
            filter_c,
            sort_c,
            sort_asc,
            limit,
            skip,
            project_c,
            exclude_c,
            ctypes.byref(json_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to query documents")

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)
        return json.loads(json_str)

    def query_count(self, filter_str: Optional[str] = None, skip: int = 0, limit: int = 0) -> int:
        """Counts documents with query options."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        filter_c = filter_str.encode("utf-8") if filter_str else None
        count = ctypes.c_size_t()
        error = CError()

        result = _lib.jasonisnthappy_collection_query_count(
            self._coll,
            filter_c,
            skip,
            limit,
            ctypes.byref(count),
            ctypes.byref(error),
        )

        if result != 0:
            _check_error(error)
            raise RuntimeError("Failed to count documents")

        return count.value

    def query_first(
        self,
        filter_str: Optional[str] = None,
        sort_field: Optional[str] = None,
        sort_asc: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Gets the first document matching a query."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        filter_c = filter_str.encode("utf-8") if filter_str else None
        sort_c = sort_field.encode("utf-8") if sort_field else None
        json_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_query_first(
            self._coll,
            filter_c,
            sort_c,
            sort_asc,
            ctypes.byref(json_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to query document")

        if not json_out or not json_out.value:
            return None

        json_str = json_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(json_out)

        if json_str == "null" or not json_str:
            return None

        return json.loads(json_str)

    # Bulk Write
    def bulk_write(self, operations: List[Dict[str, Any]], ordered: bool = True) -> Dict[str, Any]:
        """Executes multiple operations in a transaction."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        ops_json = json.dumps(operations)
        result_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_bulk_write(
            self._coll,
            ops_json.encode("utf-8"),
            ordered,
            ctypes.byref(result_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to execute bulk write")

        result_str = result_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(result_out)
        return json.loads(result_str)

    # Aggregation
    def aggregate(self, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Executes an aggregation pipeline."""
        if not self._coll:
            raise RuntimeError("Collection is closed")

        pipeline_json = json.dumps(pipeline)
        result_out = ctypes.c_char_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_aggregate(
            self._coll,
            pipeline_json.encode("utf-8"),
            ctypes.byref(result_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to execute aggregation")

        result_str = result_out.value.decode("utf-8")
        _lib.jasonisnthappy_free_string(result_out)
        return json.loads(result_str)

    # Watch / Change Streams
    def watch(
        self,
        callback: Callable[[str, str, Optional[Dict[str, Any]]], None],
        filter_str: Optional[str] = None,
    ) -> "WatchHandle":
        """
        Starts watching for changes on the collection.

        The callback receives (operation, doc_id, document) where:
        - operation: "insert", "update", or "delete"
        - doc_id: The document ID
        - document: The document data (None for delete operations)

        Example:
            >>> def on_change(op, doc_id, doc):
            ...     print(f"{op}: {doc_id}")
            >>> handle = collection.watch(on_change)
            >>> # ... later ...
            >>> handle.stop()
        """
        if not self._coll:
            raise RuntimeError("Collection is closed")

        # Create the callback wrapper that will be called from C
        def c_callback(collection, operation, doc_id, doc_json, user_data):
            try:
                op_str = operation.decode("utf-8") if operation else ""
                id_str = doc_id.decode("utf-8") if doc_id else ""
                doc = json.loads(doc_json.decode("utf-8")) if doc_json else None
                callback(op_str, id_str, doc)
            except Exception:
                pass  # Silently ignore callback errors

        # Wrap the callback to prevent garbage collection
        c_callback_wrapped = WatchCallbackType(c_callback)

        filter_c = filter_str.encode("utf-8") if filter_str else None
        handle_out = ctypes.c_void_p()
        error = CError()

        status = _lib.jasonisnthappy_collection_watch_start(
            self._coll,
            filter_c,
            c_callback_wrapped,
            None,  # user_data not needed since we use closure
            ctypes.byref(handle_out),
            ctypes.byref(error),
        )

        if status != 0:
            _check_error(error)
            raise RuntimeError("Failed to start watch")

        return WatchHandle(handle_out, c_callback_wrapped)

    def __enter__(self) -> "Collection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


# ==================
# WatchHandle Class
# ==================

class WatchHandle:
    """
    WatchHandle represents an active watch operation.

    Example:
        >>> def on_change(op, doc_id, doc):
        ...     print(f"{op}: {doc_id}")
        >>> handle = collection.watch(on_change)
        >>> # ... later ...
        >>> handle.stop()
    """

    def __init__(self, handle_ptr, callback_ref):
        self._handle = handle_ptr
        # Keep a reference to the callback to prevent garbage collection
        self._callback_ref = callback_ref

    def stop(self) -> None:
        """Stops watching and cleans up resources."""
        if self._handle:
            _lib.jasonisnthappy_watch_stop(self._handle)
            self._handle = None
            self._callback_ref = None

    def __enter__(self) -> "WatchHandle":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
