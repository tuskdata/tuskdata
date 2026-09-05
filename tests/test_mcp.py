"""MCP server (`POST /mcp`) and the tools' read-only guards.

Calls go over JSON-RPC exactly as a real client (Claude Code, Cursor)
would send them. No Postgres needed: this covers the tool listing, the
argument schema and the guards; real execution against a database is
covered by the manual walkthrough.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from tusk.studio.routes.mcp_tools import is_read_only_sql

META = {
    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    "io.modelcontextprotocol/clientCapabilities": {},
}


@pytest.fixture(scope="module")
def mcp_client():
    home = tempfile.mkdtemp(prefix="tusk_mcp_test_")
    Path(home, ".tusk").mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = home
    os.environ["TUSK_AUTH_MODE"] = "single"

    from litestar.testing import TestClient
    from tusk.studio.app import app

    # Same trick as test_middleware: the app is a module-level singleton
    # and another test file may already have closed the scheduler. But
    # litestar-mcp discovers the tools in its own startup hook, so keep
    # every hook that isn't Tusk's own (scheduler, notifications, ...).
    saved_startup = list(app.on_startup or [])
    saved_shutdown = list(app.on_shutdown or [])

    def _is_tusk_hook(fn) -> bool:
        return getattr(fn, "__module__", "").startswith("tusk.")

    app.on_startup[:] = [fn for fn in saved_startup if not _is_tusk_hook(fn)]
    app.on_shutdown[:] = [fn for fn in saved_shutdown if not _is_tusk_hook(fn)]
    try:
        with TestClient(app=app) as client:
            yield client
    finally:
        app.on_startup[:] = saved_startup
        app.on_shutdown[:] = saved_shutdown


def rpc(client, method: str, params: dict | None = None) -> dict:
    payload = dict(params or {})
    payload["_meta"] = META
    r = client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": payload},
        # Headers must mirror the body, as real clients do: protocol
        # version = `_meta.protocolVersion`, Mcp-Method = the RPC method.
        headers={
            "MCP-Protocol-Version": META["io.modelcontextprotocol/protocolVersion"],
            "Mcp-Method": method,
            **({"Mcp-Name": payload["name"]} if "name" in payload else {}),
        },
    )
    assert r.status_code == 200, r.text
    return r.json()


def tool_result(body: dict) -> dict:
    """A tool's structured result (structuredContent), or the first
    content block when the server only returns text."""
    result = body["result"]
    if isinstance(result.get("structuredContent"), dict):
        return result["structuredContent"]
    import json

    return json.loads(result["content"][0]["text"])


# ── Pure guards ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "sql",
    [
        "select 1",
        "  SELECT a, b FROM t WHERE x = 1 ORDER BY a LIMIT 5;",
        "with c as (select 1) select * from c",
        "VALUES (1), (2)",
        "-- comment\nselect /* block */ 1",
        "select * from t; -- trailing semicolon + comment is still one statement",
    ],
)
def test_read_only_accepts_queries(sql):
    ok, reason = is_read_only_sql(sql)
    assert ok, reason


@pytest.mark.parametrize(
    "sql,fragment",
    [
        ("delete from users", "read queries"),
        ("update t set a = 1", "read queries"),
        ("drop table t", "read queries"),
        ("select 1; delete from t", "one statement"),
        ("with d as (delete from t returning 1) select * from d", "DELETE"),
        ("select pg_sleep(60)", "PG_SLEEP"),
        ("", "empty"),
    ],
)
def test_read_only_rejects_writes(sql, fragment):
    ok, reason = is_read_only_sql(sql)
    assert not ok
    assert fragment.lower() in reason.lower()


# ── Endpoint JSON-RPC ─────────────────────────────────────────


def test_mcp_requires_no_csrf(mcp_client):
    """An agent has no CSRF cookie: /mcp must be exempt (a protocol 400,
    not a middleware 403)."""
    r = mcp_client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == -32602


def test_tools_list_exposes_the_four_read_only_tools(mcp_client):
    body = rpc(mcp_client, "tools/list")
    tools = {t["name"]: t for t in body["result"]["tools"]}
    assert set(tools) >= {"list_connections", "get_schema", "run_query", "explain_query"}
    props = tools["run_query"]["inputSchema"]["properties"]
    # Flat parameters (not a nested `data` object): that's what clients
    # know how to fill in.
    assert set(props) == {"connection_id", "sql", "limit"}
    assert set(tools["run_query"]["inputSchema"]["required"]) == {"connection_id", "sql"}


def test_list_connections_returns_shape(mcp_client):
    body = rpc(mcp_client, "tools/call", {"name": "list_connections", "arguments": {}})
    out = tool_result(body)
    assert "connections" in out and isinstance(out["connections"], list)


def test_run_query_unknown_connection(mcp_client):
    body = rpc(
        mcp_client,
        "tools/call",
        {"name": "run_query", "arguments": {"connection_id": "nope", "sql": "select 1"}},
    )
    out = tool_result(body)
    assert "not found" in out["error"]
    assert "list_connections" in out["hint"]


def test_get_schema_unknown_connection(mcp_client):
    body = rpc(mcp_client, "tools/call", {"name": "get_schema", "arguments": {"connection_id": "nope"}})
    assert "not found" in tool_result(body)["error"]
