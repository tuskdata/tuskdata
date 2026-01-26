"""API routes for Tusk Studio"""

import time
import msgspec
from litestar import Controller, get, post, put, delete
from litestar.params import Body

from tusk.core.connection import (
    ConnectionConfig,
    add_connection,
    get_connection,
    list_connections,
    delete_connection,
    update_connection,
    save_connections_to_file,
)
from tusk.core.history import get_history
from tusk.core.geo import detect_geometry_columns, rows_to_geojson, to_dict as geo_to_dict
from tusk.engines import postgres, sqlite
from tusk.engines import duckdb_engine


class APIController(Controller):
    """REST API for connections and queries"""

    path = "/api"

    @get("/connections")
    async def list_conns(self) -> list[dict]:
        """List all connections (without passwords)"""
        conns = list_connections()
        return [c.to_dict(include_password=False) for c in conns]

    @post("/connections")
    async def create_conn(self, data: dict = Body()) -> dict:
        """Create a new connection"""
        conn_type = data.get("type", "postgres")

        if conn_type == "postgres":
            config = ConnectionConfig(
                name=data["name"],
                type="postgres",
                host=data.get("host", "localhost"),
                port=int(data.get("port", 5432)),
                database=data.get("database", ""),
                user=data.get("user", ""),
                password=data.get("password", ""),
            )
        elif conn_type == "duckdb":
            config = ConnectionConfig(
                name=data["name"],
                type="duckdb",
                path=data.get("path", ""),
            )
        else:  # sqlite
            config = ConnectionConfig(
                name=data["name"],
                type="sqlite",
                path=data.get("path", ""),
            )

        conn_id = add_connection(config)
        save_connections_to_file()

        return {"id": conn_id, "name": config.name, "type": config.type}

    @put("/connections/{conn_id:str}")
    async def update_conn(self, conn_id: str, data: dict = Body()) -> dict:
        """Update an existing connection"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        # Build update kwargs, only including provided fields
        update_kwargs = {}
        if "name" in data:
            update_kwargs["name"] = data["name"]
        if "host" in data:
            update_kwargs["host"] = data["host"]
        if "port" in data:
            update_kwargs["port"] = int(data["port"])
        if "database" in data:
            update_kwargs["database"] = data["database"]
        if "user" in data:
            update_kwargs["user"] = data["user"]
        if "password" in data and data["password"]:  # Only update if password provided
            update_kwargs["password"] = data["password"]
        if "path" in data:
            update_kwargs["path"] = data["path"]

        updated = update_connection(conn_id, **update_kwargs)
        if updated:
            save_connections_to_file()
            return {"id": updated.id, "name": updated.name, "type": updated.type}
        return {"error": "Failed to update connection"}

    @delete("/connections/{conn_id:str}", status_code=200)
    async def remove_conn(self, conn_id: str) -> dict:
        """Delete a connection"""
        if delete_connection(conn_id):
            save_connections_to_file()
            return {"deleted": True}
        return {"deleted": False, "error": "Connection not found"}

    @get("/connections/{conn_id:str}")
    async def get_conn(self, conn_id: str) -> dict:
        """Get a connection's details (for editing)"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}
        return config.to_dict(include_password=False)

    @get("/connections/{conn_id:str}/databases")
    async def list_databases(self, conn_id: str) -> dict:
        """List all databases on a PostgreSQL server"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Database listing only available for PostgreSQL"}

        databases = await postgres.list_databases(config)
        return {"databases": databases}

    @post("/connections/{conn_id:str}/clone")
    async def clone_conn_to_database(self, conn_id: str, data: dict = Body()) -> dict:
        """Clone a connection to connect to a different database on the same server"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Database cloning only available for PostgreSQL"}

        new_database = data.get("database")
        if not new_database:
            return {"error": "No database specified"}

        # Check if connection to this database already exists
        existing = list_connections()
        for conn in existing:
            if (conn.type == "postgres" and
                conn.host == config.host and
                conn.port == config.port and
                conn.database == new_database):
                # Already exists, just return it
                return {"id": conn.id, "name": conn.name, "type": conn.type, "existing": True}

        # Create new connection with same credentials but different database
        new_name = data.get("name", f"{config.host}:{config.port}/{new_database}")
        new_config = ConnectionConfig(
            name=new_name,
            type="postgres",
            host=config.host,
            port=config.port,
            database=new_database,
            user=config.user,
            password=config.password,
        )

        new_id = add_connection(new_config)
        save_connections_to_file()

        return {"id": new_id, "name": new_config.name, "type": new_config.type, "existing": False}

    @post("/connections/test")
    async def test_conn_new(self, data: dict = Body()) -> dict:
        """Test a connection without saving it"""
        conn_type = data.get("type", "postgres")

        if conn_type == "postgres":
            config = ConnectionConfig(
                name="test",
                type="postgres",
                host=data.get("host", "localhost"),
                port=int(data.get("port", 5432)),
                database=data.get("database", ""),
                user=data.get("user", ""),
                password=data.get("password", ""),
            )
            success, message = await postgres.test_connection(config)
        elif conn_type == "duckdb":
            result = duckdb_engine.test_connection(data.get("path", ""))
            success, message = result["success"], result["message"]
        else:
            config = ConnectionConfig(
                name="test",
                type="sqlite",
                path=data.get("path", ""),
            )
            success, message = sqlite.test_connection(config)

        return {"success": success, "message": message}

    @post("/connections/{conn_id:str}/test")
    async def test_conn(self, conn_id: str) -> dict:
        """Test an existing connection"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type == "postgres":
            success, message = await postgres.test_connection(config)
        elif config.type == "duckdb":
            result = duckdb_engine.test_connection(config.path)
            success, message = result["success"], result["message"]
        else:
            success, message = sqlite.test_connection(config)

        return {"success": success, "message": message}

    @get("/connections/{conn_id:str}/schema")
    async def get_conn_schema(self, conn_id: str) -> dict:
        """Get schema for a connection"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type == "postgres":
            return await postgres.get_schema(config)
        elif config.type == "duckdb":
            return duckdb_engine.get_schema(config.path)
        else:
            return sqlite.get_schema(config)

    @post("/query")
    async def run_query(self, data: dict = Body()) -> dict:
        """Execute a query"""
        conn_id = data.get("connection_id")
        sql = data.get("sql", "").strip()

        if not sql:
            return {"error": "No SQL provided"}

        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        start_time = time.time()

        try:
            if config.type == "postgres":
                result = await postgres.execute_query(config, sql)
            elif config.type == "duckdb":
                result = duckdb_engine.execute_query(config.path, sql)
            else:
                result = sqlite.execute_query(config, sql)

            execution_time_ms = (time.time() - start_time) * 1000
            result_dict = result.to_dict()

            # Save to history
            history = get_history()
            history.add(
                connection_id=conn_id,
                connection_name=config.name,
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
                connection_id=conn_id,
                connection_name=config.name,
                sql=sql,
                execution_time_ms=execution_time_ms,
                error=str(e)
            )
            return {"error": str(e)}

    @get("/history")
    async def get_query_history(self, connection_id: str | None = None, limit: int = 50) -> dict:
        """Get query history"""
        history = get_history()
        entries = history.get_recent(limit=limit, connection_id=connection_id)
        return {
            "history": [
                {
                    "id": e.id,
                    "connection_id": e.connection_id,
                    "connection_name": e.connection_name,
                    "sql": e.sql,
                    "executed_at": e.executed_at,
                    "execution_time_ms": round(e.execution_time_ms, 2),
                    "row_count": e.row_count,
                    "status": e.status,
                    "error": e.error,
                }
                for e in entries
            ]
        }

    @delete("/history/{entry_id:int}", status_code=200)
    async def delete_history_entry(self, entry_id: int) -> dict:
        """Delete a history entry"""
        history = get_history()
        history.delete(entry_id)
        return {"deleted": True}

    @delete("/history", status_code=200)
    async def clear_history(self, connection_id: str | None = None) -> dict:
        """Clear query history"""
        history = get_history()
        history.clear(connection_id=connection_id)
        return {"cleared": True}

    # Saved Queries endpoints

    @get("/saved-queries")
    async def list_saved_queries(self, connection_id: str | None = None) -> dict:
        """Get all saved queries"""
        history = get_history()
        queries = history.get_saved_queries(connection_id=connection_id)
        return {
            "queries": [
                {
                    "id": q.id,
                    "name": q.name,
                    "sql": q.sql,
                    "connection_id": q.connection_id,
                    "folder": q.folder,
                    "created_at": q.created_at,
                    "updated_at": q.updated_at,
                }
                for q in queries
            ]
        }

    @post("/saved-queries")
    async def save_query(self, data: dict = Body()) -> dict:
        """Save a new query"""
        name = data.get("name")
        sql = data.get("sql")

        if not name or not sql:
            return {"error": "Name and SQL are required"}

        history = get_history()
        query_id = history.save_query(
            name=name,
            sql=sql,
            connection_id=data.get("connection_id"),
            folder=data.get("folder")
        )

        return {"id": query_id, "name": name}

    @get("/saved-queries/{query_id:int}")
    async def get_saved_query(self, query_id: int) -> dict:
        """Get a specific saved query"""
        history = get_history()
        query = history.get_saved_query(query_id)

        if not query:
            return {"error": "Query not found"}

        return {
            "id": query.id,
            "name": query.name,
            "sql": query.sql,
            "connection_id": query.connection_id,
            "folder": query.folder,
            "created_at": query.created_at,
            "updated_at": query.updated_at,
        }

    @put("/saved-queries/{query_id:int}")
    async def update_saved_query(self, query_id: int, data: dict = Body()) -> dict:
        """Update a saved query"""
        history = get_history()

        success = history.update_saved_query(
            query_id=query_id,
            name=data.get("name"),
            sql=data.get("sql"),
            folder=data.get("folder")
        )

        if success:
            return {"id": query_id, "updated": True}
        return {"error": "Failed to update query"}

    @delete("/saved-queries/{query_id:int}", status_code=200)
    async def delete_saved_query(self, query_id: int) -> dict:
        """Delete a saved query"""
        history = get_history()
        history.delete_saved_query(query_id)
        return {"deleted": True}

    # Geo endpoints

    @post("/geo/detect")
    async def detect_geo(self, data: dict = Body()) -> dict:
        """Detect geometry columns in query results"""
        columns = data.get("columns", [])
        rows = data.get("rows", [])

        # Convert rows to tuples if they're lists
        rows = [tuple(r) if isinstance(r, list) else r for r in rows]

        geo_indices = detect_geometry_columns(columns, rows)

        return {
            "has_geometry": len(geo_indices) > 0,
            "geometry_columns": geo_indices
        }

    @post("/geo/geojson")
    async def to_geojson(self, data: dict = Body()) -> dict:
        """Convert query results to GeoJSON"""
        columns = data.get("columns", [])
        rows = data.get("rows", [])
        geo_column_idx = data.get("geo_column", 0)

        # Convert rows to tuples if they're lists
        rows = [tuple(r) if isinstance(r, list) else r for r in rows]

        geojson = rows_to_geojson(columns, rows, geo_column_idx)

        # Convert msgspec Structs to dict for JSON serialization
        return geo_to_dict(geojson)

    # DuckDB Extensions

    @get("/duckdb/extensions")
    async def list_duckdb_extensions(self) -> dict:
        """List DuckDB extensions (installed and available)"""
        engine = duckdb_engine.get_duckdb_engine()
        extensions = engine.get_extensions()
        return {"extensions": extensions}

    @post("/duckdb/extensions/{name:str}/install")
    async def install_duckdb_extension(self, name: str) -> dict:
        """Install and load a DuckDB extension"""
        engine = duckdb_engine.get_duckdb_engine()
        return engine.install_extension(name)

    @post("/duckdb/extensions/{name:str}/load")
    async def load_duckdb_extension(self, name: str) -> dict:
        """Load an already installed DuckDB extension"""
        engine = duckdb_engine.get_duckdb_engine()
        return engine.load_extension(name)
