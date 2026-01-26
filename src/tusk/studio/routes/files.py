"""API routes for file management and DuckDB"""

import sqlite3
import time
from pathlib import Path
from litestar import Controller, get, post, delete
from litestar.params import Body
import duckdb

from tusk.core.history import get_history
from tusk.core.files import (
    scan_directory,
    get_registered_folders,
    add_folder,
    remove_folder,
    get_file_icon,
    format_rows,
    SUPPORTED_EXTENSIONS,
)
from tusk.core.connection import get_connection
from tusk.engines.duckdb_engine import get_duckdb_engine


class FilesController(Controller):
    """API for file management"""

    path = "/api/files"

    @get("/folders")
    async def list_folders(self) -> dict:
        """List all registered folders with their files"""
        folders = get_registered_folders()
        result = []

        for folder_path in folders:
            p = Path(folder_path)
            files = scan_directory(folder_path)

            result.append({
                "path": folder_path,
                "name": p.name,
                "files": [
                    {
                        "path": f.path,
                        "name": f.name,
                        "extension": f.extension,
                        "size_bytes": f.size_bytes,
                        "size_human": f.size_human,
                        "file_type": f.file_type,
                        "icon": get_file_icon(f.file_type),
                    }
                    for f in files
                ],
            })

        return {"folders": result}

    @post("/folders")
    async def add_folder_path(self, data: dict = Body()) -> dict:
        """Add a folder to monitor"""
        path = data.get("path", "")
        if not path:
            return {"success": False, "error": "No path provided"}

        expanded = str(Path(path).expanduser().resolve())

        if not Path(expanded).exists():
            return {"success": False, "error": f"Path does not exist: {path}"}

        if not Path(expanded).is_dir():
            return {"success": False, "error": f"Path is not a directory: {path}"}

        if add_folder(expanded):
            return {"success": True, "path": expanded}
        else:
            return {"success": False, "error": "Folder already registered"}

    @delete("/folders", status_code=200)
    async def remove_folder_path(self, data: dict = Body()) -> dict:
        """Remove a folder from monitoring"""
        path = data.get("path", "")
        if remove_folder(path):
            return {"success": True}
        return {"success": False, "error": "Folder not found"}

    @get("/scan")
    async def scan_folder(self, path: str) -> dict:
        """Scan a specific folder for files"""
        files = scan_directory(path)
        return {
            "path": path,
            "files": [
                {
                    "path": f.path,
                    "name": f.name,
                    "extension": f.extension,
                    "size_bytes": f.size_bytes,
                    "size_human": f.size_human,
                    "file_type": f.file_type,
                    "icon": get_file_icon(f.file_type),
                }
                for f in files
            ],
        }

    @get("/browse")
    async def browse_directory(self, path: str = "~") -> dict:
        """Browse a directory - list all files and subdirectories"""
        p = Path(path).expanduser().resolve()

        if not p.exists():
            return {"error": f"Path does not exist: {path}"}

        if not p.is_dir():
            return {"error": f"Not a directory: {path}"}

        items = []
        try:
            for item in sorted(p.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
                # Skip hidden files
                if item.name.startswith('.'):
                    continue

                if item.is_dir():
                    items.append({
                        "name": item.name,
                        "path": str(item),
                        "type": "directory",
                        "icon": "folder"
                    })
                else:
                    ext = item.suffix.lower()
                    # Only show supported data files
                    if ext in ('.csv', '.parquet', '.json', '.sqlite', '.db', '.duckdb', '.pbf', '.geojson', '.tsv'):
                        size = item.stat().st_size
                        size_human = f"{size / 1024 / 1024:.1f} MB" if size > 1024*1024 else f"{size / 1024:.1f} KB"
                        items.append({
                            "name": item.name,
                            "path": str(item),
                            "type": "file",
                            "extension": ext,
                            "size": size_human,
                            "icon": "file"
                        })
        except PermissionError:
            return {"error": f"Permission denied: {path}"}

        return {
            "path": str(p),
            "parent": str(p.parent) if p.parent != p else None,
            "items": items
        }

    @post("/create")
    async def create_database_file(self, data: dict = Body()) -> dict:
        """Create a new SQLite or DuckDB database file"""
        file_type = data.get("type", "sqlite")
        path = data.get("path", "").strip()

        if not path:
            return {"success": False, "error": "No path provided"}

        expanded = Path(path).expanduser().resolve()

        # Check if file already exists
        if expanded.exists():
            return {"success": False, "error": f"File already exists: {path}"}

        # Ensure parent directory exists
        parent = expanded.parent
        if not parent.exists():
            try:
                parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                return {"success": False, "error": f"Cannot create directory: {e}"}

        try:
            if file_type == "sqlite":
                # Create empty SQLite database
                conn = sqlite3.connect(str(expanded))
                conn.close()
            elif file_type == "duckdb":
                # Create empty DuckDB database
                conn = duckdb.connect(str(expanded))
                conn.close()
            else:
                return {"success": False, "error": f"Unknown file type: {file_type}"}

            # Auto-register the parent folder if not already registered
            add_folder(str(parent))

            return {
                "success": True,
                "path": str(expanded),
                "folder": str(parent)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    @get("/info")
    async def get_file_info(self, path: str) -> dict:
        """Get detailed info about a file (row count, schema)"""
        engine = get_duckdb_engine()
        p = Path(path).expanduser()

        if not p.exists():
            return {"error": f"File not found: {path}"}

        ext = p.suffix.lower()
        file_type = SUPPORTED_EXTENSIONS.get(ext)

        if file_type == "parquet":
            info = engine.get_parquet_info(str(p))
            if "error" in info:
                return info
            return {
                "path": str(p),
                "name": p.name,
                "file_type": "parquet",
                "row_count": info["row_count"],
                "row_count_human": format_rows(info["row_count"]),
                "columns": info["columns"],
            }

        elif file_type in ("csv", "tsv"):
            info = engine.get_csv_info(str(p))
            if "error" in info:
                return info
            return {
                "path": str(p),
                "name": p.name,
                "file_type": file_type,
                "row_count": info["row_count"],
                "row_count_human": format_rows(info["row_count"]),
                "columns": info["columns"],
            }

        elif file_type == "sqlite":
            tables = engine.get_sqlite_tables(str(p))
            if tables and "error" in tables[0]:
                return {"error": tables[0]["error"]}
            return {
                "path": str(p),
                "name": p.name,
                "file_type": "sqlite",
                "tables": tables,
            }

        else:
            return {"error": f"Unsupported file type: {ext}"}

    @get("/preview")
    async def preview_file(self, path: str, table: str | None = None) -> dict:
        """Preview file contents (first 100 rows)"""
        engine = get_duckdb_engine()
        p = Path(path).expanduser()

        if not p.exists():
            return {"error": f"File not found: {path}"}

        ext = p.suffix.lower()
        file_type = SUPPORTED_EXTENSIONS.get(ext)

        if file_type == "sqlite" and table:
            result = engine.preview_sqlite_table(str(p), table)
        elif file_type:
            result = engine.preview_file(str(p), file_type)
        else:
            return {"error": f"Unsupported file type: {ext}"}

        return result.to_dict()


class DuckDBController(Controller):
    """API for DuckDB queries"""

    path = "/api/duckdb"

    @post("/query")
    async def execute_query(self, data: dict = Body()) -> dict:
        """Execute a DuckDB query"""
        sql = data.get("sql", "").strip()
        if not sql:
            return {"error": "No SQL provided"}

        # Check if we need to attach any Postgres connections
        attach_connections = data.get("attach", [])

        engine = get_duckdb_engine()

        # Attach requested Postgres connections
        attached = []
        for conn_id in attach_connections:
            config = get_connection(conn_id)
            if config and config.type == "postgres":
                try:
                    engine.attach_postgres(conn_id, config.dsn)
                    attached.append(conn_id)
                except Exception as e:
                    # Already attached or other error
                    pass

        start_time = time.time()
        try:
            result = engine.execute(sql)
            result_dict = result.to_dict()

            # Save to history
            execution_time_ms = (time.time() - start_time) * 1000
            history = get_history()
            history.add(
                connection_id="duckdb-local",
                connection_name="DuckDB (Local)",
                sql=sql,
                execution_time_ms=execution_time_ms,
                row_count=result_dict.get("row_count"),
                error=result_dict.get("error")
            )

            return result_dict
        except Exception as e:
            # Save error to history too
            execution_time_ms = (time.time() - start_time) * 1000
            history = get_history()
            history.add(
                connection_id="duckdb-local",
                connection_name="DuckDB (Local)",
                sql=sql,
                execution_time_ms=execution_time_ms,
                error=str(e)
            )
            return {"error": str(e)}
        finally:
            # Detach connections after query
            for conn_id in attached:
                engine.detach_database(conn_id)

    @post("/attach")
    async def attach_postgres(self, data: dict = Body()) -> dict:
        """Attach a PostgreSQL connection for federated queries"""
        conn_id = data.get("connection_id")
        if not conn_id:
            return {"success": False, "error": "No connection_id provided"}

        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Only PostgreSQL connections can be attached"}

        try:
            engine = get_duckdb_engine()
            engine.attach_postgres(conn_id, config.dsn)
            return {"success": True, "name": conn_id}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @post("/detach")
    async def detach_database(self, data: dict = Body()) -> dict:
        """Detach a database"""
        name = data.get("name")
        if not name:
            return {"success": False, "error": "No name provided"}

        try:
            engine = get_duckdb_engine()
            engine.detach_database(name)
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @post("/export-parquet")
    async def export_parquet(self, data: dict = Body()) -> dict:
        """Export query results to Parquet file"""
        sql = data.get("sql", "").strip()
        output_path = data.get("path", "").strip()

        if not sql:
            return {"success": False, "error": "No SQL provided"}
        if not output_path:
            return {"success": False, "error": "No output path provided"}

        engine = get_duckdb_engine()
        return engine.export_to_parquet(sql, output_path)
