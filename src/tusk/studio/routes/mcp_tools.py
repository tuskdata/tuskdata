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

Client setup: ``claude mcp add --transport http tusk http://127.0.0.1:8000/mcp``

Handlers use flat parameters (and GET) on purpose: litestar-mcp derives
each tool's input schema from the handler signature, and a
``data: Struct = Body()`` parameter becomes a nested ``data`` object that
clients end up sending as a string.
"""

from __future__ import annotations

import re

from litestar import Controller, get

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
    async def list_connections_tool(self) -> dict:
        """List the database connections configured in Tusk."""
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
    async def get_schema_tool(self, connection_id: str, focus: str = "") -> dict:
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

        text = await _schema_summary(connection_id, focus or "")
        return {"connection_id": connection_id, "database": conn.database, "schema": text}

    @get(
        "/query",
        mcp_tool="run_query",
        mcp_description=(
            "Run a READ-ONLY SQL query (SELECT / WITH / VALUES) on a PostgreSQL "
            "connection and return columns + rows. Anything that writes or "
            "changes state is rejected. Rows are capped by `limit` (default 200, "
            "max 1000); `truncated` tells you if the cap was hit. Prefer "
            "aggregations and explicit column lists over SELECT *."
        ),
    )
    async def run_query_tool(self, connection_id: str, sql: str, limit: int = DEFAULT_ROWS) -> dict:
        """Run a read-only SQL query.

        Args:
            connection_id: id from list_connections (PostgreSQL only).
            sql: a single SELECT / WITH / VALUES statement.
            limit: max rows to return (1-1000, default 200).
        """
        conn = get_connection(connection_id)
        if conn is None:
            return _not_found(connection_id)
        if conn.type != "postgres":
            return {"error": f"run_query only supports PostgreSQL connections (this one is {conn.type})"}
        ok, reason = is_read_only_sql(sql)
        if not ok:
            return {"error": reason}
        limit = max(1, min(int(limit or DEFAULT_ROWS), MAX_ROWS))
        inner = _strip_comments(sql).strip().rstrip(";").strip()
        # Wrapping enforces the cap even when the query carries its own
        # LIMIT; we ask for one extra row to know whether we truncated.
        wrapped = f"SELECT * FROM ({inner}) AS _mcp LIMIT {limit + 1}"

        from tusk.engines.postgres import execute_query

        result = await execute_query(conn, wrapped)
        if result.error:
            return {"error": result.error, "sql": sql}
        rows = [list(r) for r in result.rows[:limit]]
        truncated = len(result.rows) > limit
        log.info("mcp_run_query", connection_id=connection_id, rows=len(rows), truncated=truncated)
        return {
            "columns": list(result.columns),
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
            "limit": limit,
        }

    @get(
        "/explain",
        mcp_tool="explain_query",
        mcp_description=(
            "Return the PostgreSQL execution plan (EXPLAIN) for a read-only query "
            "as text lines. Set `analyze` to true to actually run it and get real "
            "timings — only for queries that are safe to execute."
        ),
    )
    async def explain_tool(self, connection_id: str, sql: str, analyze: bool = False) -> dict:
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

        result = await execute_query(conn, f"EXPLAIN ({opts}FORMAT TEXT) {inner}")
        if result.error:
            return {"error": result.error, "sql": sql}
        return {"plan": [r[0] for r in result.rows], "analyzed": bool(analyze)}
