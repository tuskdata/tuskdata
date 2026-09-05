"""Query execution engine for BI plugin.

Routes queries to SQLite, DuckDB, or PostgreSQL based on source_type.
DuckDB supports cross-plugin queries via sqlite_scan().
"""

import json
import re
import sqlite3
import time
from threading import Lock
from typing import Any

import structlog
log = structlog.get_logger()


# Module-level TTL cache for query results. Keyed on
# (source_type, connection_ref, sql, frozen params, limit, frozen rls).
_CACHE: dict[tuple, tuple[float, dict]] = {}
_CACHE_LOCK = Lock()
_DEFAULT_TTL = 30  # seconds


def _cache_get(key: tuple):
    now = time.monotonic()
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit is None:
            return None
        expires_at, value = hit
        if expires_at < now:
            _CACHE.pop(key, None)
            return None
        return value


def _cache_put(key: tuple, value: dict, ttl: int) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (time.monotonic() + ttl, value)


def clear_cache() -> int:
    """Empty the cache. Returns number of entries evicted."""
    with _CACHE_LOCK:
        n = len(_CACHE)
        _CACHE.clear()
        return n


class BIQueryEngine:
    """Execute SQL queries against various data sources."""

    def execute(
        self,
        source_type: str,
        connection_ref: str,
        sql: str,
        params: dict | None = None,
        limit: int = 1000,
        rls_clauses: dict[str, str] | None = None,
        cache_ttl: int = _DEFAULT_TTL,
    ) -> dict:
        """Execute a query and return results.

        Args:
            source_type: 'sqlite', 'duckdb', or 'postgres'
            connection_ref: DB path or connection reference
            sql: SQL query to execute
            params: Optional named parameters (:name style)
            limit: Maximum rows to return
            rls_clauses: Optional row-level security filters {column: value}
            cache_ttl: Seconds to cache the result; 0 disables caching

        Returns:
            {"columns": [...], "rows": [...], "row_count": N, "truncated": bool}
        """
        cache_key = None
        if cache_ttl > 0:
            cache_key = (
                source_type,
                connection_ref,
                sql,
                tuple(sorted((params or {}).items())),
                limit,
                tuple(sorted((rls_clauses or {}).items())),
            )
            cached = _cache_get(cache_key)
            if cached is not None:
                return {**cached, "from_cache": True}

        # Postgres uses %s placeholders; SQLite/DuckDB use `?`. Pick the
        # right one for the target so downstream drivers don't choke.
        placeholder = "%s" if source_type == "postgres" else "?"

        query_params: list = []
        if params:
            sql, query_params = self._apply_params(sql, params, placeholder=placeholder)

        # Apply row-level security filters (wrap in subselect like apply_time_filter)
        if rls_clauses:
            conditions = []
            for col, val in rls_clauses.items():
                safe_col = re.sub(r'[^\w]', '', col)
                conditions.append(f'"{safe_col}" = {placeholder}')
                query_params.append(val)
            where = " AND ".join(conditions)
            sql = f"SELECT * FROM ({sql}) _rls WHERE {where}"

        # Enforce LIMIT if not present
        sql_upper = sql.strip().upper()
        if "LIMIT" not in sql_upper and sql_upper.startswith("SELECT"):
            sql = f"{sql.rstrip().rstrip(';')} LIMIT {limit}"

        if source_type == "sqlite":
            result = self._exec_sqlite(connection_ref, sql, query_params)
        elif source_type == "duckdb":
            result = self._exec_duckdb(sql, query_params)
        elif source_type == "postgres":
            result = self._exec_postgres(connection_ref, sql, query_params)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        if cache_key is not None and "error" not in result:
            _cache_put(cache_key, result, cache_ttl)
        return result

    def _exec_sqlite(self, db_path: str, sql: str, params: list | None = None) -> dict:
        """Execute query against a SQLite database."""
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(sql, params or [])
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = [list(row) for row in cursor.fetchall()]
            return {
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "truncated": False,
            }
        finally:
            conn.close()

    def _exec_duckdb(self, sql: str, params: list | None = None) -> dict:
        """Execute query using DuckDB (supports sqlite_scan for cross-plugin queries)."""
        try:
            import duckdb
        except ImportError:
            raise RuntimeError("DuckDB not installed")

        conn = duckdb.connect()
        try:
            # Install and load SQLite extension for cross-plugin queries
            conn.execute("INSTALL sqlite; LOAD sqlite;")
        except Exception:
            pass  # Already installed

        try:
            result = conn.execute(sql, params or [])
            columns = [desc[0] for desc in result.description] if result.description else []
            rows = [list(row) for row in result.fetchall()]
            return {
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "truncated": False,
            }
        finally:
            conn.close()

    def _exec_postgres(self, connection_ref: str, sql: str, params: list | None = None) -> dict:
        """Execute query against PostgreSQL via tusk's connection manager."""
        try:
            from tusk.core.connection import list_connections
            conn_info = next((c for c in list_connections() if c.name == connection_ref), None)
            if not conn_info:
                raise ValueError(f"PostgreSQL connection not found: {connection_ref}")

            import psycopg
            conninfo = f"host={conn_info.host or 'localhost'} port={int(conn_info.port or 5432)} dbname={conn_info.database or 'postgres'} user={conn_info.user or ''} password={conn_info.password or ''}"
            with psycopg.connect(conninfo) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params or [])
                    columns = [desc.name for desc in cur.description] if cur.description else []
                    rows = [list(row) for row in cur.fetchall()]
                    return {
                        "columns": columns,
                        "rows": rows,
                        "row_count": len(rows),
                        "truncated": False,
                    }
        except ImportError:
            raise RuntimeError("psycopg not available for PostgreSQL queries")
        except Exception as e:
            log.error("PostgreSQL query error", error=str(e), connection=connection_ref)
            return {
                "columns": [],
                "rows": [],
                "row_count": 0,
                "error": str(e),
            }

    def _apply_params(self, sql: str, params: dict, placeholder: str = "?") -> tuple[str, list]:
        """Convert `:param_name` style placeholders to positional ones.

        `placeholder` is `?` for SQLite/DuckDB or `%s` for psycopg/PostgreSQL.
        Returns (sql_with_placeholders, ordered_values) for safe parameterized
        execution.
        """
        ordered_values = []
        def _replace(match):
            name = match.group(1)
            if name in params:
                ordered_values.append(params[name])
                return placeholder
            return match.group(0)
        safe_sql = re.sub(r":([a-zA-Z_]\w*)", _replace, sql)
        return safe_sql, ordered_values

    def get_table_list(self, source_type: str, connection_ref: str) -> list[str]:
        """Get list of tables in a data source."""
        if source_type == "sqlite":
            conn = sqlite3.connect(connection_ref)
            try:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                )
                return [row[0] for row in cursor.fetchall()]
            finally:
                conn.close()

        elif source_type == "duckdb":
            try:
                import duckdb
                conn = duckdb.connect()
                result = conn.execute("SHOW TABLES")
                tables = [row[0] for row in result.fetchall()]
                conn.close()
                return tables
            except ImportError:
                return []

        elif source_type == "postgres":
            try:
                result = self._exec_postgres(connection_ref, """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public' ORDER BY table_name
                """)
                return [row[0] for row in result["rows"]]
            except Exception:
                return []

        return []

    @staticmethod
    def _safe_table_name(table: str) -> str:
        """Validate and quote a table name to prevent SQL injection."""
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', table):
            raise ValueError(f"Invalid table name: {table}")
        return f'"{table}"'

    def get_table_schema(
        self, source_type: str, connection_ref: str, table: str
    ) -> list[dict]:
        """Get column names and types for a table.

        Returns: [{"name": "col", "type": "TEXT"}, ...]
        """
        safe = self._safe_table_name(table)

        if source_type == "sqlite":
            conn = sqlite3.connect(connection_ref)
            try:
                cursor = conn.execute(f"PRAGMA table_info({safe})")
                return [
                    {"name": row[1], "type": row[2] or "TEXT"}
                    for row in cursor.fetchall()
                ]
            finally:
                conn.close()

        elif source_type == "duckdb":
            try:
                import duckdb
                conn = duckdb.connect()
                result = conn.execute(f"DESCRIBE {safe}")
                schema = [
                    {"name": row[0], "type": row[1]}
                    for row in result.fetchall()
                ]
                conn.close()
                return schema
            except Exception:
                return []

        elif source_type == "postgres":
            try:
                result = self._exec_postgres(connection_ref, """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = 'public'
                    ORDER BY ordinal_position
                """, [table])
                return [
                    {"name": row[0], "type": row[1]}
                    for row in result["rows"]
                ]
            except Exception:
                return []

        return []

    def get_table_preview(
        self, source_type: str, connection_ref: str, table: str, limit: int = 20
    ) -> dict:
        """Get first N rows from a table."""
        safe = self._safe_table_name(table)
        sql = f"SELECT * FROM {safe} LIMIT {int(limit)}"
        return self.execute(source_type, connection_ref, sql, limit=limit)

    def apply_variables(self, sql: str, variables: dict[str, str]) -> tuple[str, dict]:
        """Rewrite `$var_name` placeholders to `:__tusk_var_N` bind placeholders.

        Returns (rewritten_sql, params_dict). Callers must forward the dict
        to `execute()` via `params=` so values are bound by the DB driver
        instead of string-substituted (closes the SQL injection vector that
        the old string-replace implementation had).
        """
        params: dict = {}
        # Match $identifier — longest-name wins to avoid partial matches.
        names = sorted(variables.keys(), key=len, reverse=True)
        counter = 0
        for name in names:
            placeholder = f"${name}"
            while placeholder in sql:
                key = f"__tusk_var_{counter}"
                sql = sql.replace(placeholder, f":{key}", 1)
                params[key] = variables[name]
                counter += 1
        return sql, params

    def pivot_data(
        self,
        columns: list[str],
        rows: list[list],
        row_field: str,
        col_field: str,
        value_field: str,
        agg_func: str = "sum",
    ) -> dict:
        """Pivot a result set into a cross-tab table.

        Args:
            columns: Column names from the query result
            rows: Row data from the query result
            row_field: Column to use as row headers
            col_field: Column to use as column headers
            value_field: Column containing values to aggregate
            agg_func: Aggregation function (sum, avg, count, min, max)

        Returns:
            {"row_headers": [...], "col_headers": [...], "pivot_rows": [[...], ...]}
        """
        if row_field not in columns or col_field not in columns or value_field not in columns:
            return {"row_headers": [], "col_headers": [], "pivot_rows": []}

        row_idx = columns.index(row_field)
        col_idx = columns.index(col_field)
        val_idx = columns.index(value_field)

        # Collect unique row/col headers preserving order
        row_headers: list[str] = []
        col_headers: list[str] = []
        # Accumulate values: {(row_val, col_val): [values]}
        cells: dict[tuple[str, str], list[float]] = {}

        for row in rows:
            r = str(row[row_idx])
            c = str(row[col_idx])
            try:
                v = float(row[val_idx])
            except (ValueError, TypeError):
                v = 0.0

            if r not in row_headers:
                row_headers.append(r)
            if c not in col_headers:
                col_headers.append(c)

            key = (r, c)
            if key not in cells:
                cells[key] = []
            cells[key].append(v)

        # Aggregate
        def _agg(values: list[float]) -> float:
            if not values:
                return 0.0
            if agg_func == "avg":
                return sum(values) / len(values)
            elif agg_func == "count":
                return float(len(values))
            elif agg_func == "min":
                return min(values)
            elif agg_func == "max":
                return max(values)
            else:  # sum
                return sum(values)

        pivot_rows = []
        for rh in row_headers:
            pivot_row = []
            for ch in col_headers:
                pivot_row.append(_agg(cells.get((rh, ch), [])))
            pivot_rows.append(pivot_row)

        return {
            "row_headers": row_headers,
            "col_headers": col_headers,
            "pivot_rows": pivot_rows,
        }

    def apply_time_filter(
        self, sql: str, time_from: str | None, time_to: str | None,
        columns: list[str] | None = None,
    ) -> str:
        """Inject time range WHERE clause if timestamp columns detected."""
        if not time_from and not time_to:
            return sql

        ts_keywords = ("time", "date", "created", "updated", "timestamp")
        ts_cols = [c for c in (columns or []) if any(kw in c.lower() for kw in ts_keywords)]
        if not ts_cols:
            return sql

        ts_col = ts_cols[0]
        conditions = []
        if time_from:
            safe = re.sub(r"[^\w\s\-\.\:TZ]", "", time_from)
            conditions.append(f'"{ts_col}" >= \'{safe}\'')
        if time_to:
            safe = re.sub(r"[^\w\s\-\.\:TZ]", "", time_to)
            conditions.append(f'"{ts_col}" <= \'{safe}\'')

        where_clause = " AND ".join(conditions)
        return f"SELECT * FROM ({sql}) _tq WHERE {where_clause}"
