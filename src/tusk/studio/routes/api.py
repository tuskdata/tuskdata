"""API routes for Tusk Studio"""

import json
import math
import re
import time
import uuid
from pathlib import Path

import msgspec
from litestar import Controller, Request, get, post, put, delete
from litestar.params import Body
from litestar.response import Response

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
from tusk.core import query_tracker
from tusk.engines import postgres, sqlite
from tusk.engines import duckdb_engine


# Keywords that change schema/structure — used to drop the schema cache after
# successful run. Comments/strings inside SQL aren't stripped on purpose: a
# user-typed comment with the word "CREATE" only triggers a single redundant
# fetch on the next schema poll, which is cheap.
_DDL_RE = re.compile(r"(?im)^\s*(create|drop|alter|truncate|rename|comment)\b")


def _maybe_invalidate_schema(conn_id: str | None, sql: str) -> None:
    """Drop the schema cache if `sql` looks like DDL."""
    if not conn_id or not sql:
        return
    if _DDL_RE.search(sql):
        try:
            postgres.invalidate_schema_cache(conn_id)
        except Exception:
            pass


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
                ssh_host=data.get("ssh_host") or None,
                ssh_port=int(data.get("ssh_port", 22)),
                ssh_user=data.get("ssh_user") or None,
                ssh_password=data.get("ssh_password") or None,
                ssh_private_key=data.get("ssh_private_key") or None,
                ssh_known_hosts=data.get("ssh_known_hosts") or None,
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
        # SSH tunnel fields. Empty string clears the field; missing keeps current.
        for ssh_field in ("ssh_host", "ssh_user", "ssh_known_hosts"):
            if ssh_field in data:
                update_kwargs[ssh_field] = data[ssh_field] or None
        if "ssh_port" in data:
            update_kwargs["ssh_port"] = int(data["ssh_port"] or 22)
        if "ssh_password" in data and data["ssh_password"]:
            update_kwargs["ssh_password"] = data["ssh_password"]
        if "ssh_private_key" in data and data["ssh_private_key"]:
            update_kwargs["ssh_private_key"] = data["ssh_private_key"]

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
        """Get a connection's details (for editing). Secrets are stripped."""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}
        return config.to_dict(include_secrets=False)

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

        # Create new connection with same credentials but different database.
        # Carry the SSH tunnel config too — the user expects "switch database"
        # to keep using the same bastion they configured for the parent.
        new_name = data.get("name", f"{config.host}:{config.port}/{new_database}")
        new_config = ConnectionConfig(
            name=new_name,
            type="postgres",
            host=config.host,
            port=config.port,
            database=new_database,
            user=config.user,
            password=config.password,
            ssh_host=config.ssh_host,
            ssh_port=config.ssh_port,
            ssh_user=config.ssh_user,
            ssh_password=config.ssh_password,
            ssh_private_key=config.ssh_private_key,
            ssh_known_hosts=config.ssh_known_hosts,
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
                ssh_host=data.get("ssh_host") or None,
                ssh_port=int(data.get("ssh_port", 22)),
                ssh_user=data.get("ssh_user") or None,
                ssh_password=data.get("ssh_password") or None,
                ssh_private_key=data.get("ssh_private_key") or None,
                ssh_known_hosts=data.get("ssh_known_hosts") or None,
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

    @post("/connections/{conn_id:str}/reconnect")
    async def reconnect_conn(self, conn_id: str) -> dict:
        """Drop the cached connection pool + SSH tunnel for this
        connection and re-test. Use this after a network blip when
        Tusk is still holding stale handles to a server that came
        back. The auto-retry inside `execute_query` handles the
        common case automatically; this endpoint is the manual
        "fix it now" button for the sidebar."""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type == "postgres":
            try:
                await postgres._reset_connection(config)
            except Exception as e:
                return {"success": False, "error": f"reset failed: {e}"}
            success, message = await postgres.test_connection(config)
            return {"success": success, "message": message, "recycled": True}

        # Non-postgres connections don't have pools or SSH tunnels;
        # fall back to a plain test.
        return await self.test_conn(conn_id)

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

    @get("/connections/{conn_id:str}/schema-graph")
    async def get_schema_graph(self, conn_id: str) -> dict:
        """Return a graph-shaped schema (tables, columns, FKs) plus a layout.

        Layout is loaded from ~/.tusk/schema_layouts/{conn_id}.json if present;
        otherwise we compute a deterministic grid keyed off FK count so the
        diagram doesn't reshuffle on every load.
        """
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}
        if config.type != "postgres":
            return {"error": "Schema graph is only available for PostgreSQL connections"}

        # Tables + columns (single trip)
        tables_sql = """
            SELECT
                c.table_schema,
                c.table_name,
                c.column_name,
                c.data_type,
                c.ordinal_position
            FROM information_schema.columns c
            JOIN information_schema.tables t
                ON t.table_schema = c.table_schema
                AND t.table_name = c.table_name
            WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
              AND t.table_type = 'BASE TABLE'
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
        """
        cols_result = await postgres.execute_query(config, tables_sql)
        if cols_result.error:
            return {"error": cols_result.error}

        # Primary keys
        pk_sql = """
            SELECT
                tc.table_schema,
                tc.table_name,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
        """
        pk_result = await postgres.execute_query(config, pk_sql)
        primary_keys: set[tuple[str, str, str]] = set()
        if not pk_result.error:
            for row in pk_result.rows:
                primary_keys.add((row[0], row[1], row[2]))

        # Foreign keys — pg_constraint is the most reliable source
        fk_sql = """
            SELECT
                ns.nspname            AS from_schema,
                cl.relname            AS from_table,
                att.attname           AS from_column,
                fns.nspname           AS to_schema,
                fcl.relname           AS to_table,
                fatt.attname          AS to_column
            FROM pg_constraint con
            JOIN pg_class       cl   ON cl.oid = con.conrelid
            JOIN pg_namespace   ns   ON ns.oid = cl.relnamespace
            JOIN pg_class       fcl  ON fcl.oid = con.confrelid
            JOIN pg_namespace   fns  ON fns.oid = fcl.relnamespace
            JOIN unnest(con.conkey)  WITH ORDINALITY AS k(attnum, ord)  ON TRUE
            JOIN unnest(con.confkey) WITH ORDINALITY AS fk(attnum, ord) ON fk.ord = k.ord
            JOIN pg_attribute att  ON att.attrelid  = cl.oid  AND att.attnum  = k.attnum
            JOIN pg_attribute fatt ON fatt.attrelid = fcl.oid AND fatt.attnum = fk.attnum
            WHERE con.contype = 'f'
              AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY ns.nspname, cl.relname, k.ord
        """
        fk_result = await postgres.execute_query(config, fk_sql)
        fks: list[dict] = []
        fk_columns: set[tuple[str, str, str]] = set()
        if not fk_result.error:
            for row in fk_result.rows:
                from_schema, from_table, from_col, to_schema, to_table, to_col = row
                fks.append({
                    "from_schema": from_schema,
                    "from_table": from_table,
                    "from_column": from_col,
                    "to_schema": to_schema,
                    "to_table": to_table,
                    "to_column": to_col,
                })
                fk_columns.add((from_schema, from_table, from_col))

        # Row counts (from pg_stat_user_tables)
        try:
            row_counts_raw = await postgres.get_row_counts(config)
        except Exception:
            row_counts_raw = {}

        # Build table dict
        tables_map: dict[tuple[str, str], dict] = {}
        for row in cols_result.rows:
            schema_name, table_name, col_name, col_type, _ = row
            key = (schema_name, table_name)
            if key not in tables_map:
                full_name = f"{schema_name}.{table_name}"
                tables_map[key] = {
                    "name": table_name,
                    "schema": schema_name,
                    "row_count": int(row_counts_raw.get(full_name, 0) or 0),
                    "columns": [],
                }
            tables_map[key]["columns"].append({
                "name": col_name,
                "type": col_type,
                "is_pk": (schema_name, table_name, col_name) in primary_keys,
                "is_fk": (schema_name, table_name, col_name) in fk_columns,
            })

        tables = list(tables_map.values())

        # Layout — load saved, else deterministic grid sorted by FK count desc.
        layout_dir = Path.home() / ".tusk" / "schema_layouts"
        layout_dir.mkdir(parents=True, exist_ok=True)
        layout_path = layout_dir / f"{conn_id}.json"
        layout: dict[str, dict[str, float]] = {}
        if layout_path.is_file():
            try:
                layout = json.loads(layout_path.read_text())
            except Exception:
                layout = {}

        # Build deterministic grid for any tables not in saved layout. Sorting
        # by FK count then name gives a stable "popular tables in the middle"
        # feel without needing a real graph layout lib.
        fk_count_per_table: dict[str, int] = {}
        for fk in fks:
            fk_count_per_table[fk["from_table"]] = fk_count_per_table.get(fk["from_table"], 0) + 1
            fk_count_per_table[fk["to_table"]] = fk_count_per_table.get(fk["to_table"], 0) + 1

        sorted_tables = sorted(
            tables,
            key=lambda t: (-fk_count_per_table.get(t["name"], 0), t["name"]),
        )
        cols_per_row = max(1, int(math.ceil(math.sqrt(max(1, len(sorted_tables))))))
        cell_w, cell_h = 280, 240
        margin_x, margin_y = 80, 80
        for idx, t in enumerate(sorted_tables):
            if t["name"] in layout:
                continue
            r = idx // cols_per_row
            c = idx % cols_per_row
            layout[t["name"]] = {
                "x": margin_x + c * cell_w,
                "y": margin_y + r * cell_h,
            }

        return {
            "tables": tables,
            "fks": fks,
            "layout": layout,
        }

    @post("/connections/{conn_id:str}/schema-layout")
    async def save_schema_layout(self, conn_id: str, data: dict = Body()) -> dict:
        """Persist the user's drag-positioned layout to ~/.tusk/schema_layouts/{conn_id}.json."""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        layout = data.get("layout") or {}
        if not isinstance(layout, dict):
            return {"error": "layout must be an object"}

        # Coerce + sanitize: only keep {table_name: {x: float, y: float}} entries.
        clean: dict[str, dict[str, float]] = {}
        for name, pos in layout.items():
            if not isinstance(name, str) or not isinstance(pos, dict):
                continue
            try:
                x = float(pos.get("x", 0))
                y = float(pos.get("y", 0))
            except (TypeError, ValueError):
                continue
            clean[name] = {"x": x, "y": y}

        layout_dir = Path.home() / ".tusk" / "schema_layouts"
        layout_dir.mkdir(parents=True, exist_ok=True)
        layout_path = layout_dir / f"{conn_id}.json"
        layout_path.write_text(json.dumps(clean, indent=2))
        return {"saved": True, "tables": len(clean)}

    @get("/connections/{conn_id:str}/row-counts")
    async def get_row_counts(self, conn_id: str) -> dict:
        """Get row counts for all tables in a connection"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        try:
            if config.type == "postgres":
                counts = await postgres.get_row_counts(config)
            elif config.type == "duckdb":
                counts = duckdb_engine.get_row_counts(config.path)
            else:
                counts = sqlite.get_row_counts(config)
            return {"counts": counts}
        except Exception as e:
            return {"error": str(e)}

    @get("/connections/{conn_id:str}/status")
    async def get_conn_status(self, conn_id: str) -> dict:
        """Check if a connection is online"""
        config = get_connection(conn_id)
        if not config:
            return {"online": False, "error": "Connection not found"}

        try:
            if config.type == "postgres":
                online = await postgres.check_connection(config)
            elif config.type == "duckdb":
                online = duckdb_engine.check_connection(config.path)
            else:
                online = sqlite.check_connection(config)
            return {"online": online}
        except Exception as e:
            return {"online": False, "error": str(e)}

    @post("/query")
    async def run_query(self, data: dict = Body()) -> dict:
        """Execute a query.

        Optional pagination params (PostgreSQL only):
            page: Page number (1-indexed)
            page_size: Rows per page (default 100)

        Optional cancellation:
            request_id: Client-supplied id used by POST /query/cancel

        If page is provided, returns paginated results with total_count.
        """
        conn_id = data.get("connection_id")
        sql = data.get("sql", "").strip()
        page = data.get("page")  # Optional: for server-side pagination
        page_size = data.get("page_size", 100)
        request_id = data.get("request_id") or uuid.uuid4().hex

        if not sql:
            return {"error": "No SQL provided"}

        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        start_time = time.time()
        query_tracker.register(query_tracker.TrackedQuery(
            request_id=request_id,
            connection_id=conn_id or "",
            engine=config.type,
        ))

        try:
            if config.type == "postgres":
                if page is not None and page > 0:
                    result = await postgres.execute_query_paginated(
                        config, sql, page=page, page_size=page_size, request_id=request_id
                    )
                else:
                    result = await postgres.execute_query(config, sql, request_id=request_id)
            elif config.type == "duckdb":
                if page is not None and page > 0:
                    result = duckdb_engine.execute_query(config.path, sql, page=page, page_size=page_size)
                else:
                    result = duckdb_engine.execute_query(config.path, sql)
            else:
                if page is not None and page > 0:
                    result = sqlite.execute_query(config, sql, page=page, page_size=page_size)
                else:
                    result = sqlite.execute_query(config, sql)

            execution_time_ms = (time.time() - start_time) * 1000
            result_dict = result.to_dict()
            result_dict["request_id"] = request_id

            # If this looks like DDL on a Postgres connection, drop the schema
            # cache so the next /schema call sees the new structure. Cheap
            # keyword check — false positives just cause a re-fetch on the
            # next schema poll.
            if config.type == "postgres" and not result_dict.get("error"):
                _maybe_invalidate_schema(conn_id, sql)

            history = get_history()
            history.add(
                connection_id=conn_id,
                connection_name=config.name,
                sql=sql,
                execution_time_ms=execution_time_ms,
                row_count=result_dict.get("total_count") or result_dict.get("row_count"),
                error=result_dict.get("error")
            )

            return result_dict

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            history = get_history()
            history.add(
                connection_id=conn_id,
                connection_name=config.name,
                sql=sql,
                execution_time_ms=execution_time_ms,
                error=str(e)
            )
            return {"error": str(e), "request_id": request_id}

        finally:
            query_tracker.unregister(request_id)

    @post("/query/cancel")
    async def cancel_query(self, request: Request, data: dict = Body()) -> dict:
        """Cancel an in-flight query by request_id.

        Same auth contract as the admin panel: in single-user mode the call
        must come from loopback; in multi-user mode it requires an
        authenticated session. Without this guard a CSRF-tokened request
        from any user could cancel another user's running query.
        """
        from tusk.studio.routes.admin import _check_admin_auth
        _check_admin_auth(request, None)

        request_id = data.get("request_id")
        if not request_id:
            return {"error": "request_id required"}

        tracked = query_tracker.get(request_id)
        if not tracked:
            return {"cancelled": False, "reason": "not_found"}

        if tracked.engine == "postgres" and tracked.pid:
            config = get_connection(tracked.connection_id)
            if not config:
                return {"cancelled": False, "reason": "connection_gone"}
            ok = await postgres.cancel_query(config, tracked.pid)
            return {"cancelled": ok, "pid": tracked.pid}

        return {"cancelled": False, "reason": "engine_unsupported"}

    @post("/explain")
    async def explain_query(self, data: dict = Body()) -> dict:
        """Return the EXPLAIN plan for a SQL statement.

        Same auth scope as `/api/query` (the user already has access to
        the connection). Unlike the admin-gated endpoint, this works in
        single-user remote mode too — EXPLAIN is read-only and there's
        no privilege escalation.
        """
        connection_id = data.get("connection_id")
        sql = (data.get("sql") or "").strip()
        analyze = bool(data.get("analyze", False))

        if not connection_id:
            return {"error": "connection_id required"}
        if not sql:
            return {"error": "sql required"}

        config = get_connection(connection_id)
        if not config:
            return {"error": "Connection not found"}
        if config.type != "postgres":
            return {"error": "EXPLAIN is only available for PostgreSQL connections"}

        # Reuse the helper that the admin route uses, but skip the guard.
        from tusk.admin.processes import explain_query as _explain
        return await _explain(config, sql, analyze=analyze)

    @post("/query/map-data")
    async def get_map_data(self, data: dict = Body()) -> dict:
        """Fetch geometry data optimized for map rendering.

        This endpoint returns only geometry columns as GeoJSON, optimized for
        rendering on a map. Useful for large datasets where you want to show
        all features on a map but paginate the table view.

        Args:
            connection_id: Connection ID
            sql: SQL query
            simplify_tolerance: Optional geometry simplification (in coordinate units)
            max_features: Maximum features to return (default 100000)

        Returns:
            GeoJSON FeatureCollection with total_count and truncated flag
        """
        conn_id = data.get("connection_id")
        sql = data.get("sql", "").strip()
        simplify_tolerance = data.get("simplify_tolerance")
        max_features = data.get("max_features", 100_000)

        if not sql:
            return {"error": "No SQL provided"}

        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Map data endpoint only available for PostgreSQL with PostGIS"}

        return await postgres.fetch_geometries(
            config,
            sql,
            simplify_tolerance=simplify_tolerance,
            max_features=max_features,
        )

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


@get("/api/health")
async def health_check() -> dict:
    """Health check endpoint with dependency status.

    Reports each sub-component (scheduler, plugins, ibis) so a load balancer
    or orchestrator can distinguish "up" from "degraded".
    """
    import tusk

    deps: dict[str, str] = {}

    try:
        from tusk.core.scheduler import get_scheduler
        sched = get_scheduler()
        if sched and sched.scheduler.running:
            deps["scheduler"] = "up"
        elif sched:
            deps["scheduler"] = "idle"
        else:
            deps["scheduler"] = "unavailable"
    except Exception:
        deps["scheduler"] = "unavailable"

    try:
        from tusk.plugins.registry import get_all_plugins
        plugins = get_all_plugins()
        deps["plugins"] = f"loaded:{len(plugins)}"
    except Exception:
        deps["plugins"] = "unavailable"

    try:
        from tusk.engines.ibis_engine import HAS_IBIS
        deps["ibis"] = "up" if HAS_IBIS else "unavailable"
    except Exception:
        deps["ibis"] = "unavailable"

    degraded = [k for k, v in deps.items() if v == "down"]
    status = "ok" if not degraded else "degraded"

    return {"status": status, "version": tusk.__version__, "deps": deps}


@get("/api/metrics")
async def metrics() -> Response:
    """Prometheus text-format exposition.

    Scrape with `curl http://host:port/api/metrics`. Counters are reset on
    restart (in-memory). Histograms are left for follow-up.
    """
    from tusk.core import query_tracker, rate_limit
    from tusk.core.connection import list_connections
    import tusk

    try:
        from tusk.core.scheduler import get_scheduler
        sched = get_scheduler()
        scheduler_up = 1 if (sched and sched.scheduler.running) else 0
    except Exception:
        scheduler_up = 0

    try:
        from tusk.plugins.registry import get_all_plugins
        plugins_loaded = len(get_all_plugins())
    except Exception:
        plugins_loaded = 0

    try:
        from tusk.engines.ibis_engine import HAS_IBIS
        ibis_available = 1 if HAS_IBIS else 0
    except Exception:
        ibis_available = 0

    lines = [
        "# HELP tusk_build_info Build information",
        "# TYPE tusk_build_info gauge",
        f'tusk_build_info{{version="{tusk.__version__}"}} 1',
        "",
        "# HELP tusk_connections_registered Number of registered database connections",
        "# TYPE tusk_connections_registered gauge",
        f"tusk_connections_registered {len(list_connections())}",
        "",
        "# HELP tusk_queries_in_flight Queries currently executing",
        "# TYPE tusk_queries_in_flight gauge",
        f"tusk_queries_in_flight {len(query_tracker.list_active())}",
        "",
        "# HELP tusk_rate_limit_buckets Distinct rate-limit tracking buckets",
        "# TYPE tusk_rate_limit_buckets gauge",
        f"tusk_rate_limit_buckets {len(rate_limit._buckets)}",
        "",
        "# HELP tusk_scheduler_up 1 if APScheduler is running, 0 otherwise",
        "# TYPE tusk_scheduler_up gauge",
        f"tusk_scheduler_up {scheduler_up}",
        "",
        "# HELP tusk_plugins_loaded Number of loaded plugins",
        "# TYPE tusk_plugins_loaded gauge",
        f"tusk_plugins_loaded {plugins_loaded}",
        "",
        "# HELP tusk_ibis_available 1 if ibis-framework is importable",
        "# TYPE tusk_ibis_available gauge",
        f"tusk_ibis_available {ibis_available}",
        "",
    ]
    return Response(
        content="\n".join(lines),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
