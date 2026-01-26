"""DuckDB engine for analytics and federated queries"""

import time
import duckdb
from pathlib import Path

from tusk.core.result import QueryResult, ColumnInfo


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
        self.conn.execute(f"""
            ATTACH '{conn_string}' AS {name} (TYPE postgres, READ_ONLY)
        """)

    def detach_database(self, name: str):
        """Detach a database"""
        try:
            self.conn.execute(f"DETACH {name}")
        except Exception:
            pass

    def get_parquet_info(self, path: str) -> dict:
        """Get info about a Parquet file"""
        path = str(Path(path).expanduser())
        try:
            # Get row count
            result = self.conn.execute(f"SELECT count(*) FROM read_parquet('{path}')")
            row_count = result.fetchone()[0]

            # Get schema
            schema_result = self.conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')")
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
        path = str(Path(path).expanduser())
        try:
            # Get row count
            result = self.conn.execute(f"SELECT count(*) FROM read_csv_auto('{path}')")
            row_count = result.fetchone()[0]

            # Get schema
            schema_result = self.conn.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{path}')")
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
        path = str(Path(path).expanduser())
        try:
            # Attach SQLite file
            self.conn.execute(f"ATTACH '{path}' AS sqlite_temp (TYPE sqlite)")

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
        path = str(Path(path).expanduser())

        if file_type == "parquet":
            return self.execute(f"SELECT * FROM read_parquet('{path}') LIMIT {limit}")
        elif file_type in ("csv", "tsv"):
            return self.execute(f"SELECT * FROM read_csv_auto('{path}') LIMIT {limit}")
        elif file_type == "json":
            return self.execute(f"SELECT * FROM read_json_auto('{path}') LIMIT {limit}")
        else:
            return QueryResult.from_error(f"Unsupported file type: {file_type}")

    def preview_sqlite_table(self, path: str, table: str, limit: int = 100) -> QueryResult:
        """Preview a SQLite table"""
        path = str(Path(path).expanduser())
        return self.execute(f"SELECT * FROM sqlite_scan('{path}', '{table}') LIMIT {limit}")

    def export_to_parquet(self, sql: str, output_path: str) -> dict:
        """Export query results to a Parquet file"""
        output_path = str(Path(output_path).expanduser())
        try:
            self.conn.execute(f"COPY ({sql}) TO '{output_path}' (FORMAT PARQUET)")
            return {"success": True, "path": output_path}
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
    path = str(Path(path).expanduser())
    engine = DuckDBEngine(path)
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

            # Get columns for this table
            cols_result = conn.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """)

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
