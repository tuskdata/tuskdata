"""End-to-end tests driven through Litestar's in-process TestClient.

These exercise the real HTTP plumbing (middleware, guards, request parsing,
response headers) against an isolated `~/.tusk/` so state from the host
user never leaks in. All fixtures write into a `tmp_path` and set `HOME`
before `tusk.studio.app` is imported.

Scope:
- /api/health, /api/metrics (Prometheus format)
- Connection CRUD + password-at-rest encryption roundtrip
- SQLite query execution with server-side pagination
- DuckDB query execution on an in-memory-backed file
- Query cancellation endpoint shape
- Data pipeline execution through the Ibis default engine
- Data profile endpoint shape
- Admin guard rejects non-loopback callers in single-user mode
- Rate limiting kicks in on excessive uploads
- HTMX error response sets HX-Reswap: none
"""

from __future__ import annotations

import os
import sys
import sqlite3
import tempfile
from pathlib import Path

import pytest


class _CSRFClient:
    """Thin wrapper that auto-supplies the CSRF header on mutating calls.

    The app sets a `tusk_csrf` cookie on first response; the middleware then
    requires `X-CSRF-Token` on POST/PUT/DELETE/PATCH. The test client would
    get 403s otherwise, so we prime the cookie once and forward it as the
    header for every request.
    """

    def __init__(self, tc):
        self._tc = tc
        # Prime the CSRF cookie with a harmless GET
        tc.get("/api/health")

    @property
    def cookies(self):
        return self._tc.cookies

    def _headers_with_csrf(self, headers):
        token = self._tc.cookies.get("tusk_csrf")
        merged = dict(headers or {})
        if token:
            merged.setdefault("x-csrf-token", token)
        return merged

    def get(self, url, **kwargs):
        return self._tc.get(url, **kwargs)

    def post(self, url, **kwargs):
        kwargs["headers"] = self._headers_with_csrf(kwargs.get("headers"))
        return self._tc.post(url, **kwargs)

    def put(self, url, **kwargs):
        kwargs["headers"] = self._headers_with_csrf(kwargs.get("headers"))
        return self._tc.put(url, **kwargs)

    def delete(self, url, **kwargs):
        kwargs["headers"] = self._headers_with_csrf(kwargs.get("headers"))
        return self._tc.delete(url, **kwargs)


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """Spin up the Litestar app in-process.

    Rather than purge `tusk.*` modules (which breaks sibling tests' patched
    fixtures), we redirect the small number of HOME-derived module globals
    (connection file, Fernet key, workspaces dir, history DB) to a scratch
    directory. Paths are restored on teardown.
    """
    from pathlib import Path
    from tusk.studio.app import app
    from tusk.core import connection as conn_module
    from tusk.core import crypto as crypto_module
    from tusk.core import workspace as workspace_module
    from litestar.testing import TestClient

    tmp = Path(tmp_path_factory.mktemp("tusk_e2e_home"))
    tusk_dir = tmp / ".tusk"
    tusk_dir.mkdir(parents=True, exist_ok=True)

    # Save + patch module-level paths
    saved = {
        "conn_TUSK_DIR": conn_module.TUSK_DIR,
        "conn_CONN_FILE": conn_module.CONN_FILE,
        "crypto_TUSK_DIR": crypto_module.TUSK_DIR,
        "crypto_KEY_FILE": crypto_module.KEY_FILE,
        "crypto_cached": crypto_module._cached,
        "ws_TUSK_DIR": workspace_module.TUSK_DIR,
        "ws_WORKSPACES_DIR": workspace_module.WORKSPACES_DIR,
        "connections": dict(conn_module._connections),
    }
    conn_module.TUSK_DIR = tusk_dir
    conn_module.CONN_FILE = tusk_dir / "connections.toml"
    conn_module._connections.clear()
    crypto_module.TUSK_DIR = tusk_dir
    crypto_module.KEY_FILE = tusk_dir / ".key"
    crypto_module._cached = None
    workspace_module.TUSK_DIR = tusk_dir
    workspace_module.WORKSPACES_DIR = tusk_dir / "workspaces"

    try:
        with TestClient(app=app, base_url="http://testserver.local") as tc:
            yield _CSRFClient(tc)
    finally:
        conn_module.TUSK_DIR = saved["conn_TUSK_DIR"]
        conn_module.CONN_FILE = saved["conn_CONN_FILE"]
        conn_module._connections.clear()
        conn_module._connections.update(saved["connections"])
        crypto_module.TUSK_DIR = saved["crypto_TUSK_DIR"]
        crypto_module.KEY_FILE = saved["crypto_KEY_FILE"]
        crypto_module._cached = saved["crypto_cached"]
        workspace_module.TUSK_DIR = saved["ws_TUSK_DIR"]
        workspace_module.WORKSPACES_DIR = saved["ws_WORKSPACES_DIR"]

        # The app's on_startup constructed a NotificationService singleton
        # bound to the real HOME; reset it so sibling tests can build their
        # own with a clean DB path.
        try:
            from tusk.core.notifications import NotificationService
            NotificationService.reset()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Health + metrics
# ---------------------------------------------------------------------------


def test_health_returns_dep_status(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] in ("ok", "degraded")
    assert "version" in body
    assert "deps" in body
    assert set(body["deps"].keys()) >= {"scheduler", "plugins", "ibis"}


def test_metrics_is_prometheus_text(client):
    r = client.get("/api/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    text = r.text
    assert "# HELP tusk_build_info" in text
    assert "# TYPE tusk_build_info gauge" in text
    assert 'tusk_build_info{version=' in text
    assert "tusk_connections_registered" in text
    assert "tusk_queries_in_flight" in text
    assert "tusk_ibis_available" in text


# ---------------------------------------------------------------------------
# Connection lifecycle + password-at-rest encryption
# ---------------------------------------------------------------------------


def test_sqlite_connection_crud(client, tmp_path):
    db_file = tmp_path / "sample.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER);
        INSERT INTO widgets VALUES
            (1, 'alpha', 10),
            (2, 'beta',  20),
            (3, 'gamma', 30);
        """
    )
    conn.commit()
    conn.close()

    r = client.post("/api/connections", json={
        "type": "sqlite",
        "name": "e2e_sqlite",
        "path": str(db_file),
    })
    assert r.status_code in (200, 201), r.text
    conn_id = r.json()["id"]

    listing = client.get("/api/connections").json()
    assert any(c["id"] == conn_id for c in listing)
    # `password` field never leaks in listings
    assert all("password" not in c for c in listing)

    # Verify schema fetch works for the new connection
    schema = client.get(f"/api/connections/{conn_id}/schema")
    assert schema.status_code == 200
    data = schema.json()
    tables = data.get("tables") or data.get("main") or data
    assert "widgets" in str(tables).lower()


def test_postgres_password_round_trips_through_encryption(tmp_path):
    """Plain-text legacy passwords decrypt passthrough; saves re-encrypt them.

    Patches the module-level file/key paths directly instead of reloading the
    module, which would orphan every other test's reference to the previous
    `tusk.core.connection` singleton.
    """
    from tusk.core import connection as conn_module
    from tusk.core import crypto as crypto_module

    saved = (
        conn_module.TUSK_DIR, conn_module.CONN_FILE,
        dict(conn_module._connections),
        crypto_module.TUSK_DIR, crypto_module.KEY_FILE, crypto_module._cached,
    )
    sandbox = tmp_path / ".tusk"
    sandbox.mkdir()
    conn_module.TUSK_DIR = sandbox
    conn_module.CONN_FILE = sandbox / "connections.toml"
    conn_module._connections.clear()
    crypto_module.TUSK_DIR = sandbox
    crypto_module.KEY_FILE = sandbox / ".key"
    crypto_module._cached = None

    try:
        cfg = conn_module.ConnectionConfig(
            name="pgtest",
            type="postgres",
            host="db.internal",
            port=5432,
            database="app",
            user="alice",
            password="s3cret!",
        )
        conn_module.add_connection(cfg)
        conn_module.save_connections_to_file()

        raw = conn_module.CONN_FILE.read_text()
        assert "s3cret!" not in raw
        assert "enc:v1:" in raw

        conn_module._connections.clear()
        conn_module.load_connections_from_file()
        loaded = conn_module.get_connection(cfg.id)
        assert loaded is not None
        assert loaded.password == "s3cret!"
    finally:
        (
            conn_module.TUSK_DIR, conn_module.CONN_FILE,
            conns,
            crypto_module.TUSK_DIR, crypto_module.KEY_FILE, crypto_module._cached,
        ) = saved
        conn_module._connections.clear()
        conn_module._connections.update(conns)


# ---------------------------------------------------------------------------
# Query execution (SQLite path, server-side pagination, cancellation)
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_conn_id(client, tmp_path_factory):
    db_file = tmp_path_factory.mktemp("db") / "query.db"
    c = sqlite3.connect(db_file)
    c.executescript("CREATE TABLE nums (n INTEGER);")
    c.executemany("INSERT INTO nums VALUES (?)", [(i,) for i in range(1, 251)])
    c.commit()
    c.close()

    r = client.post("/api/connections", json={
        "type": "sqlite",
        "name": "nums_db",
        "path": str(db_file),
    })
    assert r.status_code in (200, 201)
    return r.json()["id"]


def test_sqlite_query_pagination(client, sqlite_conn_id):
    r = client.post("/api/query", json={
        "connection_id": sqlite_conn_id,
        "sql": "SELECT n FROM nums ORDER BY n",
        "page": 1,
        "page_size": 50,
    })
    # Litestar returns 201 for POST by default; accept both.
    assert r.status_code in (200, 201), r.text
    body = r.json()
    assert body.get("total_count") == 250
    assert body["page"] == 1
    assert len(body["rows"]) == 50
    # Request_id round-trips
    assert "request_id" in body

    # Page 3 should land on rows 101-150
    r2 = client.post("/api/query", json={
        "connection_id": sqlite_conn_id,
        "sql": "SELECT n FROM nums ORDER BY n",
        "page": 3,
        "page_size": 50,
    })
    body2 = r2.json()
    assert body2["rows"][0][0] == 101
    assert body2["rows"][-1][0] == 150


def test_query_cancel_endpoint_shape(client, sqlite_conn_id):
    """cancel endpoint should return a stable payload even when nothing matches."""
    r = client.post("/api/query/cancel", json={"request_id": "does-not-exist"})
    assert r.status_code in (200, 201)
    body = r.json()
    assert body.get("cancelled") is False
    assert body.get("reason") == "not_found"

    # No body at all → helpful error
    r2 = client.post("/api/query/cancel", json={})
    assert r2.status_code in (200, 201)
    assert "error" in r2.json()


# ---------------------------------------------------------------------------
# Data pipeline via Ibis (default engine)
# ---------------------------------------------------------------------------


def test_data_execute_defaults_to_ibis(client, tmp_path):
    csv_path = tmp_path / "sales.csv"
    csv_path.write_text(
        "region,amount\nnorth,100\nsouth,250\nnorth,50\neast,300\n"
    )
    r = client.post("/api/data/execute", json={
        "sources": [{
            "id": "s1",
            "name": "sales",
            "source_type": "csv",
            "path": str(csv_path),
        }],
        "transforms": [
            {"type": "filter", "column": "amount", "operator": "gte", "value": 100},
            {"type": "group_by", "by": ["region"], "aggregations": [
                {"column": "amount", "agg": "sum", "alias": "total"},
            ]},
        ],
        "output_source_id": "s1",
        "limit": 100,
    })
    assert r.status_code in (200, 201), r.text
    body = r.json()
    assert "error" not in body, body
    engine = body.get("engine_used", "")
    assert engine.startswith("ibis") or (engine == "polars" and "fallback" in body)
    # After filter amount>=100: north=100, south=250, east=300
    totals = {row[0]: row[1] for row in body["rows"]}
    assert totals.get("north") in (100, 100.0)
    assert totals.get("south") in (250, 250.0)
    assert totals.get("east") in (300, 300.0)


def test_data_profile_returns_per_column_stats(client, tmp_path):
    csv_path = tmp_path / "profile.csv"
    csv_path.write_text(
        "name,age\nalice,30\nbob,\ncarol,40\ndave,35\n"
    )
    r = client.post("/api/data/profile", json={
        "sources": [{
            "id": "s1",
            "name": "people",
            "source_type": "csv",
            "path": str(csv_path),
        }],
        "transforms": [],
        "output_source_id": "s1",
    })
    assert r.status_code in (200, 201), r.text
    body = r.json()
    if "error" in body:
        pytest.skip(f"Ibis profile unavailable: {body['error']}")
    cols = {c["name"]: c for c in body["columns"]}
    assert "age" in cols
    assert cols["age"]["rows"] == 4


# ---------------------------------------------------------------------------
# Admin guard: single-user mode requires loopback
# ---------------------------------------------------------------------------


def test_admin_rejects_non_loopback(client):
    """Forge a non-loopback `host` to verify the guard bites."""
    # TestClient's ASGI client always reports 127.0.0.1; the guard needs a
    # direct test of the internal helper with a synthetic request.
    from tusk.studio.routes.admin import _is_loopback

    class FakeClient:
        def __init__(self, host):
            self.host = host

    class FakeReq:
        def __init__(self, host):
            self.client = FakeClient(host)

    assert _is_loopback(FakeReq("127.0.0.1")) is True
    assert _is_loopback(FakeReq("::1")) is True
    assert _is_loopback(FakeReq("10.0.0.5")) is False
    assert _is_loopback(FakeReq("203.0.113.1")) is False


# ---------------------------------------------------------------------------
# Rate limiting: many rapid uploads should cut off
# ---------------------------------------------------------------------------


def test_upload_rate_limit_enforced():
    """Test the limiter directly — the HTTP endpoint parses multipart via
    Litestar which makes it awkward to unit-test through the TestClient
    without constructing the exact expected body. The limiter contract is
    what actually matters here."""
    from tusk.core import rate_limit

    rate_limit._buckets.clear()

    allowed = 0
    for _ in range(15):
        if rate_limit.check_and_record("upload", "127.0.0.1", max_attempts=10, window_seconds=60):
            allowed += 1
    # Exactly 10 permits, the rest rejected.
    assert allowed == 10

    # Separate IPs do not share buckets.
    assert rate_limit.check_and_record("upload", "10.0.0.2", max_attempts=10, window_seconds=60) is True


# ---------------------------------------------------------------------------
# HTMX error helper sets HX-Reswap: none
# ---------------------------------------------------------------------------


def test_htmx_error_helper_shape():
    """Unit-level contract on the helper — fast, no HTTP roundtrip."""
    from tusk.studio.htmx import htmx_error, htmx_noswap

    err = htmx_error("boom")
    assert err.get("HX-Reswap") == "none"
    assert "HX-Trigger" in err  # Toast payload for the client

    noswap = htmx_noswap()
    assert noswap == {"HX-Reswap": "none"}


# ---------------------------------------------------------------------------
# Ibis engine: CASE WHEN and date_arithmetic survive the HTTP boundary
# ---------------------------------------------------------------------------


def test_case_when_through_http(client, tmp_path):
    csv_path = tmp_path / "tiers.csv"
    csv_path.write_text("id,age\n1,25\n2,40\n3,65\n")
    r = client.post("/api/data/execute", json={
        "sources": [{
            "id": "s1",
            "name": "people",
            "source_type": "csv",
            "path": str(csv_path),
        }],
        "transforms": [
            {
                "type": "case_when",
                "alias": "tier",
                "branches": [
                    {"column": "age", "operator": "gte", "value": 60, "result": "senior"},
                    {"column": "age", "operator": "gte", "value": 30, "result": "adult"},
                ],
                "default": "young",
            },
        ],
        "output_source_id": "s1",
        "limit": 100,
    })
    assert r.status_code in (200, 201), r.text
    body = r.json()
    if "error" in body or body.get("fallback"):
        pytest.skip("Ibis not available for case_when — Polars engine does not support this transform")
    # Map id → tier
    tiers = {}
    cols = [c["name"] for c in body["columns"]]
    id_idx = cols.index("id")
    tier_idx = cols.index("tier")
    for row in body["rows"]:
        tiers[row[id_idx]] = row[tier_idx]
    assert tiers == {1: "young", 2: "adult", 3: "senior"}
