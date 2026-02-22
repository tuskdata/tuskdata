"""SQLite storage for plugins.

Plugins get isolated SQLite databases in ~/.tusk/plugins/{id}.db
Data module can query these via DuckDB's sqlite_scanner extension.

Example:
    # In plugin code
    from tusk.plugins.storage import get_plugin_db, init_plugin_db

    init_plugin_db("tusk-security", '''
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    with get_plugin_db("tusk-security") as db:
        db.execute("INSERT INTO scans DEFAULT VALUES")

    # In Data module (DuckDB)
    SELECT * FROM sqlite_scan('~/.tusk/plugins/tusk_security.db', 'scans')
"""

import sqlite3
from pathlib import Path
from contextlib import contextmanager
from typing import Generator

from tusk.core.config import TUSK_DIR


def get_plugins_dir() -> Path:
    """Get plugins directory, creating if needed"""
    plugins_dir = TUSK_DIR / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    return plugins_dir


def get_plugin_db_path(plugin_id: str) -> Path:
    """Get path to plugin's SQLite database

    Args:
        plugin_id: Plugin identifier (e.g., 'tusk-security')

    Returns:
        Path to ~/.tusk/plugins/{sanitized_id}.db
    """
    # Sanitize plugin_id for filename
    safe_id = plugin_id.replace("-", "_").replace(".", "_")
    return get_plugins_dir() / f"{safe_id}.db"


@contextmanager
def get_plugin_db(plugin_id: str) -> Generator[sqlite3.Connection, None, None]:
    """Context manager for plugin database connection.

    Usage:
        with get_plugin_db("tusk-security") as db:
            db.execute("CREATE TABLE IF NOT EXISTS ...")
            db.execute("INSERT INTO ...")

    Args:
        plugin_id: Plugin identifier

    Yields:
        sqlite3.Connection with Row factory enabled
    """
    db_path = get_plugin_db_path(plugin_id)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Dict-like access

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_plugin_db(plugin_id: str, schema: str) -> None:
    """Initialize plugin database with schema.

    Args:
        plugin_id: Plugin identifier
        schema: SQL schema (CREATE TABLE statements)
    """
    with get_plugin_db(plugin_id) as db:
        db.executescript(schema)


def query_plugin_db(plugin_id: str, sql: str, params: tuple = ()) -> list[dict]:
    """Execute query and return results as list of dicts.

    Args:
        plugin_id: Plugin identifier
        sql: SQL query
        params: Query parameters

    Returns:
        List of row dicts
    """
    with get_plugin_db(plugin_id) as db:
        cursor = db.execute(sql, params)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def execute_plugin_db(plugin_id: str, sql: str, params: tuple = ()) -> int:
    """Execute statement and return lastrowid.

    Args:
        plugin_id: Plugin identifier
        sql: SQL statement
        params: Statement parameters

    Returns:
        Last inserted row ID
    """
    with get_plugin_db(plugin_id) as db:
        cursor = db.execute(sql, params)
        return cursor.lastrowid or 0
