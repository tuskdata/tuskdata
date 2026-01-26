"""SQLite engine using stdlib sqlite3"""

import time
import sqlite3
from pathlib import Path

from tusk.core.connection import ConnectionConfig
from tusk.core.result import QueryResult, ColumnInfo


def execute_query(config: ConnectionConfig, sql: str) -> QueryResult:
    """Execute SQL query and return results (sync - sqlite3 doesn't need async)"""
    start = time.perf_counter()

    try:
        path = Path(config.path).expanduser() if config.path else ":memory:"
        conn = sqlite3.connect(str(path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(sql)

        # Get column info
        columns = []
        if cursor.description:
            columns = [
                ColumnInfo(name=desc[0], type="TEXT")  # SQLite is dynamically typed
                for desc in cursor.description
            ]

        # Fetch rows
        rows = [tuple(row) for row in cursor.fetchall()]

        elapsed = (time.perf_counter() - start) * 1000

        conn.close()

        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            execution_time_ms=round(elapsed, 2),
        )

    except Exception as e:
        return QueryResult.from_error(str(e))


def get_schema(config: ConnectionConfig) -> dict:
    """Get database schema (tables and columns)"""
    try:
        path = Path(config.path).expanduser() if config.path else ":memory:"
        conn = sqlite3.connect(str(path))
        cursor = conn.cursor()

        # Get all tables
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]

        # Build schema tree
        schema = {"main": {}}

        for table in tables:
            cursor.execute(f"PRAGMA table_info(`{table}`)")
            columns = [
                {"name": row[1], "type": row[2] or "ANY"}
                for row in cursor.fetchall()
            ]
            schema["main"][table] = columns

        conn.close()
        return schema

    except Exception as e:
        return {"error": str(e)}


def test_connection(config: ConnectionConfig) -> tuple[bool, str]:
    """Test if connection works"""
    result = execute_query(config, "SELECT 1")
    if result.error:
        return False, result.error
    return True, "Connection successful"


def check_connection(config: ConnectionConfig) -> bool:
    """Quick check if connection is online"""
    result = execute_query(config, "SELECT 1")
    return not result.error


def get_row_counts(config: ConnectionConfig) -> dict:
    """Get row counts for all tables"""
    try:
        path = Path(config.path).expanduser() if config.path else ":memory:"
        conn = sqlite3.connect(str(path))
        cursor = conn.cursor()

        # Get all tables
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """)
        tables = [row[0] for row in cursor.fetchall()]

        counts = {}
        for table in tables:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                count = cursor.fetchone()[0]
                counts[f"main.{table}"] = count
            except Exception:
                pass

        conn.close()
        return counts
    except Exception:
        return {}
