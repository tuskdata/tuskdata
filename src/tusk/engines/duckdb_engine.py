"""DuckDB engine for analytics and federated queries"""

import re
import time
import duckdb
from pathlib import Path

from tusk.core.result import QueryResult, ColumnInfo

# Valid extension/database name: alphanumeric + underscores
_VALID_NAME_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def _escape_duckdb_string(value: str) -> str:
    """Escape a string for use in DuckDB SQL (single-quote escaping)."""
    return value.replace("'", "''")


def _safe_path(path: str) -> str:
    """Validate and escape a file path for DuckDB SQL.

    Resolves the path and ensures it's a real filesystem path,
    then escapes single quotes for safe SQL interpolation.
    """
    resolved = str(Path(path).expanduser().resolve())
    return _escape_duckdb_string(resolved)


class DuckDBEngine:
    """DuckDB engine with support for Parquet, CSV, and federated queries"""

    def __init__(self, path: str = ":memory:"):
        self.conn = duckdb.connect(path)
        self._install_extensions()

    def _install_extensions(self):
        """Install required extensions"""
        # Core extensions - always installed
        core_extensions = [
            "parquet",
            "postgres_scanner",
            "sqlite",
            "spatial",  # For geospatial data (GeoJSON, WKT, etc.)
        ]

        for ext in core_extensions:
            try:
                self.conn.execute(f"INSTALL {ext}; LOAD {ext};")
            except Exception:
                pass  # Already installed or not available

    def get_extensions(self) -> list[dict]:
        """Get list of installed and available extensions"""
        try:
            result = self.conn.execute("""
                SELECT
                    extension_name,
                    installed,
                    loaded,
                    description
                FROM duckdb_extensions()
                ORDER BY extension_name
            """)
            return [
                {
                    "name": row[0],
                    "installed": row[1],
                    "loaded": row[2],
                    "description": row[3] or "",
                }
                for row in result.fetchall()
            ]
        except Exception as e:
            return [{"error": str(e)}]

    def install_extension(self, name: str) -> dict:
        """Install and load a DuckDB extension"""
        if not _VALID_NAME_RE.match(name):
            return {"success": False, "error": f"Invalid extension name: {name}"}
        try:
            self.conn.execute(f"INSTALL {name};")
            self.conn.execute(f"LOAD {name};")
            return {"success": True, "message": f"Extension '{name}' installed and loaded"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def uninstall_extension(self, name: str) -> dict:
        """Uninstall a DuckDB extension"""
        try:
            # DuckDB doesn't have UNINSTALL, but we can force reinstall
            # For now, just report that uninstall isn't supported
            return {"success": False, "error": "DuckDB doesn't support uninstalling extensions. Delete ~/.duckdb/extensions/ manually."}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def load_extension(self, name: str) -> dict:
        """Load an already installed extension"""
        if not _VALID_NAME_RE.match(name):
            return {"success": False, "error": f"Invalid extension name: {name}"}
        try:
            self.conn.execute(f"LOAD {name};")
            return {"success": True, "message": f"Extension '{name}' loaded"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def execute(self, sql: str) -> QueryResult:
        """Execute SQL query and return results"""
        start = time.perf_counter()

        try:
            result = self.conn.execute(sql)

            columns = []
            rows = []

            if result.description:
                columns = [
                    ColumnInfo(name=desc[0], type=str(desc[1]))
                    for desc in result.description
                ]

                # Detect GEOMETRY columns and re-execute with ST_AsGeoJSON
                geo_cols = [
                    i for i, desc in enumerate(result.description)
                    if str(desc[1]).upper() in ("GEOMETRY", "GEOGRAPHY")
                ]

                if geo_cols:
                    try:
                        wrapped = []
                        for i, desc in enumerate(result.description):
                            col_name = desc[0].replace('"', '""')
                            if i in geo_cols:
                                wrapped.append(f'ST_AsGeoJSON("{col_name}") AS "{col_name}"')
                            else:
                                wrapped.append(f'"{col_name}"')
                        wrapped_sql = f"SELECT {', '.join(wrapped)} FROM ({sql}) AS _tusk_geo"
                        result = self.conn.execute(wrapped_sql)
                        for i in geo_cols:
                            columns[i] = ColumnInfo(name=columns[i].name, type="GEOMETRY")
                    except Exception:
                        pass  # Fall through to fetch raw if wrapping fails

                rows = result.fetchall()

            elapsed = (time.perf_counter() - start) * 1000

            return QueryResult(
                columns=columns,
                rows=rows,
                row_count=len(rows),
                execution_time_ms=round(elapsed, 2),
            )

        except Exception as e:
            return QueryResult.from_error(str(e))

    def attach_postgres(self, name: str, conn_string: str):
        """Attach a PostgreSQL database for federated queries"""
        if not _VALID_NAME_RE.match(name):
            raise ValueError(f"Invalid database name: {name}")
        safe_conn = _escape_duckdb_string(conn_string)
        self.conn.execute(f"""
            ATTACH '{safe_conn}' AS {name} (TYPE postgres, READ_ONLY)
        """)

    def detach_database(self, name: str):
        """Detach a database"""
        if not _VALID_NAME_RE.match(name):
            return
        try:
            self.conn.execute(f"DETACH {name}")
        except Exception:
            pass

    def get_parquet_info(self, path: str) -> dict:
        """Get info about a Parquet file"""
        safe = _safe_path(path)
        try:
            # Get row count
            result = self.conn.execute(f"SELECT count(*) FROM read_parquet('{safe}')")
            row_count = result.fetchone()[0]

            # Get schema
            schema_result = self.conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{safe}')")
            columns = [
                {"name": row[0], "type": row[1]}
                for row in schema_result.fetchall()
            ]

            return {
                "row_count": row_count,
                "columns": columns,
            }
        except Exception as e:
            return {"error": str(e)}

    def get_csv_info(self, path: str) -> dict:
        """Get info about a CSV file"""
        safe = _safe_path(path)
        try:
            # Get row count
            result = self.conn.execute(f"SELECT count(*) FROM read_csv_auto('{safe}', max_line_size=20000000)")
            row_count = result.fetchone()[0]

            # Get schema
            schema_result = self.conn.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{safe}', max_line_size=20000000)")
            columns = [
                {"name": row[0], "type": row[1]}
                for row in schema_result.fetchall()
            ]

            return {
                "row_count": row_count,
                "columns": columns,
            }
        except Exception as e:
            return {"error": str(e)}

    def get_sqlite_tables(self, path: str) -> list[dict]:
        """Get tables from a SQLite file"""
        safe = _safe_path(path)
        try:
            # Attach SQLite file
            self.conn.execute(f"ATTACH '{safe}' AS sqlite_temp (TYPE sqlite)")

            # Get tables
            result = self.conn.execute("""
                SELECT name FROM sqlite_temp.sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            """)

            tables = []
            for row in result.fetchall():
                table_name = row[0]
                try:
                    count_result = self.conn.execute(f"SELECT count(*) FROM sqlite_temp.{table_name}")
                    count = count_result.fetchone()[0]
                except Exception:
                    count = 0
                tables.append({"name": table_name, "row_count": count})

            # Detach
            self.conn.execute("DETACH sqlite_temp")

            return tables
        except Exception as e:
            try:
                self.conn.execute("DETACH sqlite_temp")
            except Exception:
                pass
            return [{"error": str(e)}]

    def preview_file(self, path: str, file_type: str, limit: int = 100) -> QueryResult:
        """Preview a file's contents"""
        safe = _safe_path(path)
        limit = int(limit)  # Ensure limit is an integer

        if file_type == "parquet":
            return self.execute(f"SELECT * FROM read_parquet('{safe}') LIMIT {limit}")
        elif file_type in ("csv", "tsv"):
            return self.execute(f"SELECT * FROM read_csv_auto('{safe}', max_line_size=20000000) LIMIT {limit}")
        elif file_type == "json":
            return self.execute(f"SELECT * FROM read_json_auto('{safe}', maximum_object_size=134217728) LIMIT {limit}")
        else:
            return QueryResult.from_error(f"Unsupported file type: {file_type}")

    def preview_sqlite_table(self, path: str, table: str, limit: int = 100) -> QueryResult:
        """Preview a SQLite table"""
        safe = _safe_path(path)
        safe_table = _escape_duckdb_string(table)
        limit = int(limit)
        return self.execute(f"SELECT * FROM sqlite_scan('{safe}', '{safe_table}') LIMIT {limit}")

    def export_to_parquet(self, sql: str, output_path: str) -> dict:
        """Export query results to a Parquet file"""
        safe_output = _safe_path(output_path)
        try:
            self.conn.execute(f"COPY ({sql}) TO '{safe_output}' (FORMAT PARQUET)")
            return {"success": True, "path": str(Path(output_path).expanduser().resolve())}
        except Exception as e:
            return {"success": False, "error": str(e)}


# Global DuckDB instance (in-memory)
_engine: DuckDBEngine | None = None


def get_duckdb_engine() -> DuckDBEngine:
    """Get the global DuckDB engine instance"""
    global _engine
    if _engine is None:
        _engine = DuckDBEngine()
    return _engine


def execute_query(path: str, sql: str) -> QueryResult:
    """Execute a query on a DuckDB file"""
    resolved = str(Path(path).expanduser().resolve())
    engine = DuckDBEngine(resolved)
    return engine.execute(sql)


def get_schema(path: str) -> dict:
    """Get schema from a DuckDB database file"""
    path = str(Path(path).expanduser())

    try:
        conn = duckdb.connect(path, read_only=True)

        # Get all tables
        tables_result = conn.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """)

        schema = {}

        for schema_name, table_name in tables_result.fetchall():
            if schema_name not in schema:
                schema[schema_name] = {}

            # Get columns for this table (parameterized)
            cols_result = conn.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
            """, [schema_name, table_name])

            columns = []
            for col_name, col_type in cols_result.fetchall():
                columns.append({
                    "name": col_name,
                    "type": col_type,
                    "is_primary_key": False,  # DuckDB doesn't expose PK info easily
                    "is_foreign_key": False,
                })

            schema[schema_name][table_name] = columns

        conn.close()

        # If empty, return main schema with empty tables
        if not schema:
            schema = {"main": {}}

        return schema

    except Exception as e:
        return {"error": str(e)}


def check_connection(path: str) -> bool:
    """Check if DuckDB file is accessible"""
    try:
        path = str(Path(path).expanduser())
        conn = duckdb.connect(path, read_only=True)
        conn.execute("SELECT 1")
        conn.close()
        return True
    except Exception:
        return False


def get_row_counts(path: str) -> dict:
    """Get row counts for all tables in a DuckDB database"""
    try:
        path = str(Path(path).expanduser())
        conn = duckdb.connect(path, read_only=True)

        tables_result = conn.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """)

        counts = {}
        for schema_name, table_name in tables_result.fetchall():
            try:
                count_result = conn.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
                count = count_result.fetchone()[0]
                counts[f"{schema_name}.{table_name}"] = count
            except Exception:
                pass

        conn.close()
        return counts
    except Exception:
        return {}


def test_connection(path: str) -> dict:
    """Test connection to a DuckDB file"""
    path = str(Path(path).expanduser())

    try:
        conn = duckdb.connect(path, read_only=True)
        result = conn.execute("SELECT 1").fetchone()
        conn.close()
        return {"success": True, "message": "Connection successful"}
    except Exception as e:
        return {"success": False, "message": str(e)}
