"""
database/connection.py — SQLite connection management.

Uses WAL (Write-Ahead Logging) mode for concurrent reads with a single writer.
All JSON columns are stored as TEXT and queried with SQLite's built-in JSON functions.
"""

import sqlite3
import threading
from contextlib import contextmanager
from config import DB_PATH


# Thread-local storage so each thread gets its own connection
_local = threading.local()


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """
    Return a thread-local SQLite connection.
    Creates the connection and applies performance pragmas on first access.
    """
    if not hasattr(_local, "connections"):
        _local.connections = {}

    if db_path not in _local.connections:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # WAL mode: concurrent reads don't block writes
        conn.execute("PRAGMA journal_mode=WAL")
        # Faster writes: OS handles crash safety (acceptable for prototype)
        conn.execute("PRAGMA synchronous=NORMAL")
        # Larger page cache reduces I/O
        conn.execute("PRAGMA cache_size=-32000")   # ~32 MB
        # Enable foreign key enforcement
        conn.execute("PRAGMA foreign_keys=ON")

        _local.connections[db_path] = conn

    return _local.connections[db_path]


@contextmanager
def transaction(db_path: str = DB_PATH):
    """Context manager that commits on success and rolls back on exception."""
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def close_connection(db_path: str = DB_PATH):
    """Close the thread-local connection for the given db_path."""
    if hasattr(_local, "connections") and db_path in _local.connections:
        _local.connections[db_path].close()
        del _local.connections[db_path]
