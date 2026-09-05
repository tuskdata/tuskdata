"""Tusk MCP tools.

Plain Litestar routes marked with ``mcp_tool=...``; the ``litestar-mcp``
plugin serves them as an MCP server (Streamable HTTP) at ``POST /mcp``.
An agent (Claude Code, Cursor, Claude Desktop) therefore uses the
connections already configured in Tusk, with the same auth and inside
the same process — there is no separate MCP server re-implementing
connection loading, password decryption or SSH tunnels.

Everything here is read-only on purpose: ``run_query`` rejects anything
that is not a query and enforces a row cap. Writing to a database from
an agent is the user's call, not the tool's.

Auth: in single-user mode the endpoint is open like the rest of the UI.
In multi-user mode the client sends a personal API token
(``Authorization: Bearer tusk_...``, minted on the Profile page or with
``tusk auth token create``) and acts as that user. Every call lands in
the audit log with the user, the tool and the SQL.

Client setup::

    claude mcp add --transport http tusk http://127.0.0.1:8000/mcp \\
        --header "Authorization: Bearer tusk_..."

Handlers use flat parameters (and GET) on purpose: litestar-mcp derives
each tool's input schema from the handler signature, and a
``data: Struct = Body()`` parameter becomes a nested ``data`` object that
clients end up sending as a string.
"""

from __future__ import annotations

import asyncio
import re

from litestar import Controller, Request, get

from tusk.core.connection import get_connection, list_connections
from tusk.core.logging import get_logger

log = get_logger(__name__)

# Statements starting with one of these are reads. Everything else is refused.
_READ_PREFIXES = ("select", "with", "values", "table", "show")
# Words that, anywhere in the statement, mean it writes data or changes
# state — this also catches data-modifying CTEs (WITH ... DELETE ...).
_WRITE_WORDS = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|truncate|create|grant|revoke|"
    r"copy|call|do|vacuum|analyze|refresh|lock|set|reset|cluster|reindex|"
    r"comment|security|import|load|attach|detach|pg_terminate_backend|"
    r"pg_cancel_backend|pg_sleep|dblink|lo_import|lo_export)\b",
    re.I,
)
MAX_ROWS = 1000
DEFAULT_ROWS = 200
_QUERY_TYPES = ("postgres", "duckdb", "sqlite")


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"--[^\n]*", "", sql)
    return re.sub(r"/\*.*?\*/", "", sql, flags=re.S)


def is_read_only_sql(sql: str) -> tuple[bool, str]:
    """Return (ok, reason). Exactly one statement, a read, with no write
    verbs anywhere (CTEs can write too)."""
    cleaned = _strip_comments(sql or "").strip().rstrip(";").strip()
    if not cleaned:
        return False, "empty SQL"
    if ";" in cleaned:
        return False, "only one statement is allowed"
    first = cleaned.split(None, 1)[0].lower().lstrip("(")
    if first not in _READ_PREFIXES:
        return False, f"only read queries are allowed (got '{first.upper()}')"
    m = _WRITE_WORDS.search(cleaned)
    if m:
        return False, f"'{m.group(0).upper()}' is not allowed through MCP"
    return True, ""


def _conn_summary(c) -> dict:
    return {
        "id": c.id,
        "name": c.name,
        "type": str(c.type),
        "host": c.host,
        "port": c.port,
        "database": c.database,
        "path": c.path,
    }


def _not_found(connection_id: str) -> dict:
    ids = [c.id for c in list_connections()]
    return {
        "error": f"connection '{connection_id}' not found",
        "hint": f"call list_connections first; known ids: {ids}",
    }


def _audit(request: Request, tool: str, resource: str | None, details: str | None = None) -> None:
    """Every tool call is an audit event: who (token → user), what, on which
    connection, and the SQL. Best-effort — auditing must never break a call."""
    try:
        from tusk.core.auth import log_audit
        from tusk.studio.routes.base import _current_user_id

        log_audit(
            f"mcp.{tool}",
            user_id=_current_user_id(request) or None,
            resource=resource,
            details=(details or "")[:2000] or None,
            ip_address=request.client.host if request.client else None,
        )
    except Exception as e:  # noqa: BLE001
        log.warning("mcp_audit_failed", tool=tool, error=str(e))


async def _run_read_query(conn, sql: str, limit: int) -> dict:
    """Run an already-validated read query on any supported engine, with the
    row cap enforced by wrapping (works even if the query has its own LIMIT).
    We ask for one extra row to know whether we truncated."""
    inner = _strip_comments(sql).strip().rstrip(";").strip()
    wrapped = f"SELECT * FROM ({inner}) AS _mcp LIMIT {limit + 1}"

    if conn.type == "postgres":
        from tusk.engines.postgres import execute_query

        result = await execute_query(conn, wrapped)
    elif conn.type == "duckdb":
        from tusk.engines.duckdb_engine import execute_query as duckdb_query

        result = await asyncio.to_thread(duckdb_query, conn.path or ":memory:", wrapped)
    elif conn.type == "sqlite":
        from tusk.engines.sqlite import execute_query as sqlite_query

        result = await asyncio.to_thread(sqlite_query, conn, wrapped)
    else:
        return {"error": f"run_query does not support '{conn.type}' connections"}

    if result.error:
        return {"error": result.error, "sql": sql}
    rows = [list(r) for r in result.rows[:limit]]
    columns = [c.name if hasattr(c, "name") else str(c) for c in result.columns]
    out = {
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "truncated": len(result.rows) > limit,
        "limit": limit,
        "engine": str(conn.type),
    }
    # Geometry in the result (WKT/GeoJSON/hex-WKB text or a PostGIS column):
    # hand agents and map clients a FeatureCollection as well, so a
    # "restaurants in Piantini" answer can be drawn without another hop.
    try:
        from tusk.core.geo import detect_geometry_columns, rows_to_geojson, to_dict

        col_dicts = [{"name": c.name, "type": getattr(c, "type", "")} for c in result.columns]
        geo_idx = detect_geometry_columns(col_dicts, [tuple(r) for r in rows])
        if geo_idx:
            out["geometry_column"] = columns[geo_idx[0]]
            out["geojson"] = to_dict(rows_to_geojson(col_dicts, [tuple(r) for r in rows], geo_idx[0]))
    except Exception as exc:  # noqa: BLE001 — never fail a query over the map extra
        log.debug("mcp_geojson_skipped", error=str(exc))
    return out


class MCPToolsController(Controller):
    """Read-only routes exposed as MCP tools."""

    path = "/api/mcp-tools"
    tags = ["mcp"]

    @get(
        "/connections",
        mcp_tool="list_connections",
        mcp_description=(
            "List the database connections configured in Tusk. Returns id, name, "
            "type (postgres/duckdb/sqlite), host, port and database. Use the `id` "
            "as `connection_id` in the other tools."
        ),
    )
    async def list_connections_tool(self, request: Request) -> dict:
        """List the database connections configured in Tusk."""
        _audit(request, "list_connections", None)
        return {"connections": [_conn_summary(c) for c in list_connections()]}

    @get(
        "/schema",
        mcp_tool="get_schema",
        mcp_description=(
            "Describe the schema of a PostgreSQL connection: every table with row "
            "counts, plus full columns/PK/FK for the tables that match `focus` "
            "(a table name, a column name, or a few words about what you're "
            "looking for). Call this before writing SQL — table and column names "
            "here are the source of truth."
        ),
    )
    async def get_schema_tool(self, request: Request, connection_id: str, focus: str = "") -> dict:
        """Describe the schema of a connection.

        Args:
            connection_id: id from list_connections.
            focus: optional table/column name or keywords to expand in detail.
        """
        conn = get_connection(connection_id)
        if conn is None:
            return _not_found(connection_id)
        if conn.type != "postgres":
            return {"error": f"get_schema only supports PostgreSQL connections (this one is {conn.type})"}
        # Same summary the Copilot uses: full table list plus detail for the
        # relevant tables, already tuned so a model doesn't hallucinate.
        from tusk.studio.routes.ai import _schema_summary

        _audit(request, "get_schema", connection_id, focus)
        text = await _schema_summary(connection_id, focus or "")
        return {"connection_id": connection_id, "database": conn.database, "schema": text}

    @get(
        "/query",
        mcp_tool="run_query",
        mcp_description=(
            "Run a READ-ONLY SQL query (SELECT / WITH / VALUES) on a PostgreSQL, "
            "DuckDB or SQLite connection and return columns + rows. Anything that "
            "writes or changes state is rejected. Rows are capped by `limit` "
            "(default 200, max 1000); `truncated` tells you if the cap was hit. "
            "Prefer aggregations and explicit column lists over SELECT *."
        ),
    )
    async def run_query_tool(
        self, request: Request, connection_id: str, sql: str, limit: int = DEFAULT_ROWS
    ) -> dict:
        """Run a read-only SQL query.

        Args:
            connection_id: id from list_connections.
            sql: a single SELECT / WITH / VALUES statement.
            limit: max rows to return (1-1000, default 200).
        """
        conn = get_connection(connection_id)
        if conn is None:
            return _not_found(connection_id)
        if conn.type not in _QUERY_TYPES:
            return {"error": f"run_query does not support '{conn.type}' connections"}
        ok, reason = is_read_only_sql(sql)
        if not ok:
            _audit(request, "run_query.rejected", connection_id, f"{reason}: {sql}")
            return {"error": reason}
        limit = max(1, min(int(limit or DEFAULT_ROWS), MAX_ROWS))
        _audit(request, "run_query", connection_id, sql)
        out = await _run_read_query(conn, sql, limit)
        log.info("mcp_run_query", connection_id=connection_id, rows=out.get("row_count"), truncated=out.get("truncated"))
        return out

    @get(
        "/explain",
        mcp_tool="explain_query",
        mcp_description=(
            "Return the PostgreSQL execution plan (EXPLAIN) for a read-only query "
            "as text lines. Set `analyze` to true to actually run it and get real "
            "timings — only for queries that are safe to execute."
        ),
    )
    async def explain_tool(self, request: Request, connection_id: str, sql: str, analyze: bool = False) -> dict:
        """Explain a read-only query.

        Args:
            connection_id: id from list_connections (PostgreSQL only).
            sql: a single SELECT / WITH / VALUES statement.
            analyze: run the query for real timings (EXPLAIN ANALYZE).
        """
        conn = get_connection(connection_id)
        if conn is None:
            return _not_found(connection_id)
        if conn.type != "postgres":
            return {"error": f"explain_query only supports PostgreSQL connections (this one is {conn.type})"}
        ok, reason = is_read_only_sql(sql)
        if not ok:
            return {"error": reason}
        inner = _strip_comments(sql).strip().rstrip(";").strip()
        opts = "ANALYZE, " if analyze else ""
        from tusk.engines.postgres import execute_query

        _audit(request, "explain_query", connection_id, sql)
        result = await execute_query(conn, f"EXPLAIN ({opts}FORMAT TEXT) {inner}")
        if result.error:
            return {"error": result.error, "sql": sql}
        return {"plan": [r[0] for r in result.rows], "analyzed": bool(analyze)}

    @get(
        "/schema-changes",
        mcp_tool="schema_changes",
        mcp_description=(
            "What changed in a connection's schema recently, as recorded by Schema "
            "Watch: tables/columns/keys/indexes added, removed or altered, with a "
            "timestamp. Use it to answer 'what changed since yesterday?' or to "
            "explain why a query broke. Empty if the connection isn't watched."
        ),
    )
    async def schema_changes_tool(self, request: Request, connection_id: str, days: int = 7) -> dict:
        """Recent schema changes for a watched connection.

        Args:
            connection_id: id from list_connections.
            days: look back this many days (default 7).
        """
        from tusk.core import schema_watch as sw

        if get_connection(connection_id) is None:
            return _not_found(connection_id)
        _audit(request, "schema_changes", connection_id, f"days={days}")
        latest = sw.latest_snapshot(connection_id)
        changes = sw.list_changes(connection_id, since=sw.since_days(days), limit=50)
        return {
            "watched": latest is not None,
            "last_snapshot_at": latest["taken_at"] if latest else None,
            "changes": [{"detected_at": c["detected_at"], "summary": c["summary"], "diff": c["diff"]} for c in changes],
        }

    @get(
        "/contract",
        mcp_tool="contract_status",
        mcp_description=(
            "Whether the connection's frozen data contract (expected tables, columns, "
            "types, keys) currently holds, and the open violation if not. Use it "
            "before trusting a query result or to explain a broken report."
        ),
    )
    async def contract_status_tool(self, request: Request, connection_id: str) -> dict:
        """Data contract status for a connection.

        Args:
            connection_id: id from list_connections.
        """
        from tusk.core import contracts as ct

        if get_connection(connection_id) is None:
            return _not_found(connection_id)
        _audit(request, "contract_status", connection_id)
        contract = ct.active_contract(connection_id)
        if not contract:
            return {"has_contract": False, "holds": None, "hint": "no contract frozen for this connection"}
        v = ct.open_violation(contract["id"])
        return {
            "has_contract": True,
            "contract": {"id": contract["id"], "name": contract["name"], "frozen_at": contract["created_at"], "tables": contract["table_count"]},
            "holds": v is None,
            "violation": ({"detected_at": v["detected_at"], "summary": v["summary"], "items": v["violations"]} if v else None),
        }

    @get(
        "/saved-queries",
        mcp_tool="list_saved_queries",
        mcp_description=(
            "List the SQL queries the user saved in Tusk Studio (id, name, "
            "connection_id, sql). Run one with run_saved_query — these are "
            "vetted queries, prefer them over writing SQL from scratch when one "
            "matches the question."
        ),
    )
    async def list_saved_queries_tool(self, request: Request, connection_id: str = "") -> dict:
        """List saved queries, optionally for one connection.

        Args:
            connection_id: optional id from list_connections to filter by.
        """
        from tusk.core.history import get_history
        from tusk.studio.routes.base import _current_user_id

        owner = _current_user_id(request)
        queries = get_history().get_saved_queries(
            connection_id=connection_id or None,
            for_user_id=owner or None,
        )
        _audit(request, "list_saved_queries", connection_id or None)
        return {
            "queries": [
                {"id": q.id, "name": q.name, "connection_id": q.connection_id, "sql": q.sql}
                for q in queries
            ]
        }

    @get(
        "/saved-queries/run",
        mcp_tool="run_saved_query",
        mcp_description=(
            "Run a saved query by id (from list_saved_queries) on its own "
            "connection, or on `connection_id` if given. Same read-only rules and "
            "row cap as run_query."
        ),
    )
    async def run_saved_query_tool(
        self, request: Request, query_id: int, connection_id: str = "", limit: int = DEFAULT_ROWS
    ) -> dict:
        """Run a saved query.

        Args:
            query_id: id from list_saved_queries.
            connection_id: override the connection the query was saved with.
            limit: max rows to return (1-1000, default 200).
        """
        from tusk.core.history import get_history

        saved = get_history().get_saved_query(int(query_id))
        if not saved:
            return {"error": f"saved query {query_id} not found", "hint": "call list_saved_queries"}
        cid = connection_id or saved.connection_id or ""
        conn = get_connection(cid) if cid else None
        if conn is None:
            return {"error": f"saved query {query_id} has no usable connection; pass connection_id"}
        if conn.type not in _QUERY_TYPES:
            return {"error": f"run_saved_query does not support '{conn.type}' connections"}
        ok, reason = is_read_only_sql(saved.sql)
        if not ok:
            return {"error": f"saved query is not read-only: {reason}"}
        limit = max(1, min(int(limit or DEFAULT_ROWS), MAX_ROWS))
        _audit(request, "run_saved_query", cid, f"#{query_id} {saved.name}: {saved.sql}")
        out = await _run_read_query(conn, saved.sql, limit)
        out["query"] = {"id": saved.id, "name": saved.name}
        return out

    @get(
        "/advise",
        mcp_tool="advise",
        mcp_description=(
            "Database health advice for a PostgreSQL connection: foreign keys without an index, "
            "sequential-scan-heavy tables, unused or duplicate indexes, dead tuples, missing statistics, "
            "and the slowest queries when pg_stat_statements is installed. Each finding carries the SQL "
            "to run. Nothing is applied."
        ),
    )
    async def advise_tool(self, request: Request, connection_id: str) -> dict:
        """Args:
            connection_id: a PostgreSQL connection id from list_connections.
        """
        conn = get_connection(connection_id)
        if not conn:
            return _not_found(connection_id)
        if conn.type != "postgres":
            return {"error": f"advise only supports PostgreSQL connections (this one is {conn.type})"}
        from tusk.core.advisor import analyze

        _audit(request, "advise", connection_id, "")
        report = await analyze(conn)
        return report.to_dict()
