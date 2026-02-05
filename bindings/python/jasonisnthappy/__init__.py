"""
jasonisnthappy - Python bindings for jasonisnthappy embedded database
"""

from .database import Database, Transaction, UpsertResult, WebServer, WatchHandle

__version__ = "0.1.1"
__all__ = ["Database", "Transaction", "UpsertResult", "WebServer", "WatchHandle"]
