"""Query history persistence using SQLite"""

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Literal
import msgspec


class QueryHistoryEntry(msgspec.Struct):
    """A query history entry"""
    id: int
    connection_id: str
    connection_name: str
    sql: str
    executed_at: str
    execution_time_ms: float
    row_count: int | None = None
    error: str | None = None
    status: Literal["success", "error"] = "success"


class SavedQuery(msgspec.Struct):
    """A saved query"""
    id: int
    name: str
    sql: str
    connection_id: str | None
    created_at: str
    updated_at: str
    folder: str | None = None


class QueryHistory:
    """Manages query history in SQLite"""

    def __init__(self, db_path: Path | None = None):
        if db_path is None:
            db_path = Path.home() / ".tusk" / "history.db"

        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize the database schema"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS query_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    connection_id TEXT NOT NULL,
                    connection_name TEXT NOT NULL,
                    sql TEXT NOT NULL,
                    executed_at TEXT NOT NULL,
                    execution_time_ms REAL NOT NULL,
                    row_count INTEGER,
                    error TEXT,
                    status TEXT NOT NULL DEFAULT 'success'
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_history_connection
                ON query_history(connection_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_history_executed_at
                ON query_history(executed_at DESC)
            """)
            # Saved queries table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS saved_queries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    sql TEXT NOT NULL,
                    connection_id TEXT,
                    folder TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_saved_name
                ON saved_queries(name)
            """)
            conn.commit()

    def add(
        self,
        connection_id: str,
        connection_name: str,
        sql: str,
        execution_time_ms: float,
        row_count: int | None = None,
        error: str | None = None
    ) -> int:
        """Add a query to history, returns the entry ID"""
        status = "error" if error else "success"
        executed_at = datetime.now().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO query_history
                (connection_id, connection_name, sql, executed_at, execution_time_ms, row_count, error, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (connection_id, connection_name, sql, executed_at, execution_time_ms, row_count, error, status))
            conn.commit()
            return cursor.lastrowid

    def get_recent(self, limit: int = 50, connection_id: str | None = None) -> list[QueryHistoryEntry]:
        """Get recent queries, optionally filtered by connection"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            if connection_id:
                cursor = conn.execute("""
                    SELECT * FROM query_history
                    WHERE connection_id = ?
                    ORDER BY executed_at DESC
                    LIMIT ?
                """, (connection_id, limit))
            else:
                cursor = conn.execute("""
                    SELECT * FROM query_history
                    ORDER BY executed_at DESC
                    LIMIT ?
                """, (limit,))

            return [QueryHistoryEntry(**dict(row)) for row in cursor.fetchall()]

    def search(self, query: str, limit: int = 50) -> list[QueryHistoryEntry]:
        """Search queries by SQL text"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM query_history
                WHERE sql LIKE ?
                ORDER BY executed_at DESC
                LIMIT ?
            """, (f"%{query}%", limit))

            return [QueryHistoryEntry(**dict(row)) for row in cursor.fetchall()]

    def clear(self, connection_id: str | None = None):
        """Clear history, optionally for a specific connection"""
        with sqlite3.connect(self.db_path) as conn:
            if connection_id:
                conn.execute("DELETE FROM query_history WHERE connection_id = ?", (connection_id,))
            else:
                conn.execute("DELETE FROM query_history")
            conn.commit()

    def delete(self, entry_id: int):
        """Delete a specific history entry"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM query_history WHERE id = ?", (entry_id,))
            conn.commit()

    # Saved Queries methods

    def save_query(
        self,
        name: str,
        sql: str,
        connection_id: str | None = None,
        folder: str | None = None
    ) -> int:
        """Save a query, returns the entry ID"""
        now = datetime.now().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO saved_queries (name, sql, connection_id, folder, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (name, sql, connection_id, folder, now, now))
            conn.commit()
            return cursor.lastrowid

    def get_saved_queries(self, connection_id: str | None = None) -> list[SavedQuery]:
        """Get all saved queries, optionally filtered by connection"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            if connection_id:
                cursor = conn.execute("""
                    SELECT * FROM saved_queries
                    WHERE connection_id = ? OR connection_id IS NULL
                    ORDER BY folder NULLS FIRST, name
                """, (connection_id,))
            else:
                cursor = conn.execute("""
                    SELECT * FROM saved_queries
                    ORDER BY folder NULLS FIRST, name
                """)

            return [SavedQuery(**dict(row)) for row in cursor.fetchall()]

    def get_saved_query(self, query_id: int) -> SavedQuery | None:
        """Get a specific saved query by ID"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM saved_queries WHERE id = ?
            """, (query_id,))
            row = cursor.fetchone()
            return SavedQuery(**dict(row)) if row else None

    def update_saved_query(
        self,
        query_id: int,
        name: str | None = None,
        sql: str | None = None,
        folder: str | None = None
    ) -> bool:
        """Update a saved query"""
        with sqlite3.connect(self.db_path) as conn:
            # Build update query dynamically
            updates = []
            params = []

            if name is not None:
                updates.append("name = ?")
                params.append(name)
            if sql is not None:
                updates.append("sql = ?")
                params.append(sql)
            if folder is not None:
                updates.append("folder = ?")
                params.append(folder if folder else None)

            if not updates:
                return False

            updates.append("updated_at = ?")
            params.append(datetime.now().isoformat())
            params.append(query_id)

            conn.execute(f"""
                UPDATE saved_queries
                SET {', '.join(updates)}
                WHERE id = ?
            """, params)
            conn.commit()
            return True

    def delete_saved_query(self, query_id: int):
        """Delete a saved query"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM saved_queries WHERE id = ?", (query_id,))
            conn.commit()


# Global instance
_history: QueryHistory | None = None


def get_history() -> QueryHistory:
    """Get the global query history instance"""
    global _history
    if _history is None:
        _history = QueryHistory()
    return _history
