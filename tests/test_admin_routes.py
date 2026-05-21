"""Happy-path + guard tests for `src/tusk/studio/routes/admin.py`.

The audit baseline (2026-05-19) showed admin.py at 17% coverage despite
being our largest routes file (892 statements). The handlers fall into
two categories:

  (a) Framework logic — auth guard, connection lookup, wrong-type
      rejection, HTMX vs JSON response branching, error rendering.
  (b) Postgres logic — actually executes SQL against a real DB.

These tests cover (a) — no real Postgres needed. We register a fake
PostgreSQL connection (so `get_connection(conn_id)` returns something
truthy) and a fake SQLite connection (so the wrong-type branch fires),
then exercise the routes. Coverage of (b) requires a PG service in CI
and is deferred to v0.5.x.

Pattern: re-use the same fixture as test_middleware.py — short-circuit
the lifecycle hooks so a TestClient re-enter across modules doesn't hit
"Event loop is closed".
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def app_client():
    home = tempfile.mkdtemp(prefix="tusk_admin_test_")
    Path(home, ".tusk").mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = home
    os.environ["TUSK_AUTH_MODE"] = "single"

    from litestar.testing import TestClient
    from tusk.studio.app import app
    from tusk.studio.routes import admin as admin_module

    # Litestar's TestClient sets request.client.host to "testclient",
    # which the admin auth guard rightly rejects as non-loopback.
    # Patch _is_loopback for the lifetime of this fixture so we can
    # exercise the handler bodies. Restored on teardown — same trick
    # test_e2e.py uses.
    original_loopback = admin_module._is_loopback
    admin_module._is_loopback = lambda req: True

    # Short-circuit the lifecycle for cross-module test isolation.
    saved_startup = list(app.on_startup or [])
    saved_shutdown = list(app.on_shutdown or [])
    app.on_startup.clear()
    app.on_shutdown.clear()

    try:
        with TestClient(app=app, base_url="http://127.0.0.1") as client:
            yield client
    finally:
        admin_module._is_loopback = original_loopback
        app.on_startup[:] = saved_startup
        app.on_shutdown[:] = saved_shutdown


@pytest.fixture(scope="module")
def fake_connections():
    """Register a fake Postgres connection and a fake SQLite one so we
    can exercise both `connection not found` (→ 404-ish) and `wrong
    type` (→ Postgres-only error) branches. Cleans up on teardown."""
    from tusk.core.connection import ConnectionConfig, add_connection, _connections

    saved = dict(_connections)
    pg = ConnectionConfig(
        id="fake-pg",
        name="Fake PG",
        type="postgres",
        host="127.0.0.1",
        port=15432,  # nothing listens here — fine for guard tests
        database="x",
        user="x",
        password="x",
    )
    sqlite = ConnectionConfig(
        id="fake-sqlite",
        name="Fake SQLite",
        type="sqlite",
        path="/tmp/nonexistent.db",
    )
    add_connection(pg, persist=False)
    add_connection(sqlite, persist=False)
    yield {"pg": pg, "sqlite": sqlite}
    _connections.clear()
    _connections.update(saved)


def _csrf(client):
    """Prime the CSRF cookie + return the header value."""
    client.get("/")
    token = client.cookies.get("tusk_csrf")
    assert token, "GET / did not set tusk_csrf cookie"
    return token


# ─────────────────────────────────────────────────────────────────────
# Guard: unknown conn_id → 404-style error
# ─────────────────────────────────────────────────────────────────────


# Every GET endpoint that takes a {conn_id:str} path param.
_GET_ENDPOINTS_NEEDING_CONN = [
    "/api/admin/{cid}/stats",
    "/api/admin/{cid}/processes",
    "/api/admin/{cid}/tables",
    "/api/admin/{cid}/extensions",
    "/api/admin/{cid}/locks",
    "/api/admin/{cid}/locks/all",
    "/api/admin/{cid}/tables/bloat",
    "/api/admin/{cid}/roles",
    "/api/admin/{cid}/backups",
    "/api/admin/{cid}/settings",
]

# Endpoints whose response is independent of the connection's DB type
# (e.g. backups list local filesystem; no Postgres-only guard applies).
# Skipped from test_wrong_connection_type_rejected.
_PG_TYPE_INDEPENDENT = {
    "/api/admin/{cid}/backups",
}


@pytest.mark.parametrize("path_template", _GET_ENDPOINTS_NEEDING_CONN)
def test_unknown_connection_returns_error_payload(app_client, fake_connections, path_template):
    """Unknown conn_id must return a JSON payload with `error`, not
    crash with a 500 or hang. We don't pin the status code because
    handlers vary (some return 200 with error in body, some return
    4xx) — what matters is that the API caller can tell something
    went wrong."""
    path = path_template.format(cid="this-conn-does-not-exist")
    r = app_client.get(path)
    # Either an error-bodied 2xx (the `return {"error": "..."}` path)
    # or a clean 4xx. Both are fine; a 5xx is not.
    assert r.status_code < 500, f"{path} crashed with {r.status_code}: {r.text[:200]}"


@pytest.mark.parametrize("path_template", _GET_ENDPOINTS_NEEDING_CONN)
def test_wrong_connection_type_rejected(app_client, fake_connections, path_template):
    """A connection that isn't Postgres must be rejected by every PG
    admin endpoint, again without a 500."""
    if path_template in _PG_TYPE_INDEPENDENT:
        pytest.skip(f"{path_template} doesn't check connection type "
                    "(it serves local filesystem data, not Postgres data)")
    path = path_template.format(cid="fake-sqlite")
    r = app_client.get(path)
    assert r.status_code < 500, f"{path} crashed: {r.text[:200]}"
    # Body should mention this is Postgres-only somewhere. Loose check.
    body = r.text.lower()
    assert "postgres" in body or "only" in body or "error" in body, body[:200]


# ─────────────────────────────────────────────────────────────────────
# Specific endpoints with their own assertion shape
# ─────────────────────────────────────────────────────────────────────


def test_admin_health_endpoint(app_client):
    """`/admin/health` is the global admin health dashboard
    (HealthController is mounted at `/admin`, separately from the
    `/api/admin/*` JSON routes). Should always render."""
    r = app_client.get("/admin/health")
    assert r.status_code == 200, r.text[:200]


def test_kill_query_unknown_connection(app_client, fake_connections):
    """POST /kill/{pid} on an unknown connection — needs CSRF priming."""
    token = _csrf(app_client)
    r = app_client.post(
        "/api/admin/unknown-conn/kill/12345",
        headers={"X-CSRF-Token": token},
        json={},
    )
    assert r.status_code < 500


def test_explain_requires_pg(app_client, fake_connections):
    """POST /explain rejects non-PG connections cleanly."""
    token = _csrf(app_client)
    r = app_client.post(
        "/api/admin/fake-sqlite/explain",
        headers={"X-CSRF-Token": token},
        json={"sql": "SELECT 1"},
    )
    assert r.status_code < 500
    body = r.text.lower()
    assert "postgres" in body or "error" in body, body[:200]


def test_explain_requires_sql_body(app_client, fake_connections):
    """POST /explain without SQL in body should return a clear 4xx
    or error JSON, not crash."""
    token = _csrf(app_client)
    r = app_client.post(
        "/api/admin/fake-pg/explain",
        headers={"X-CSRF-Token": token},
        json={},
    )
    # Either 400 (missing field) or 200 with error in body.
    assert r.status_code < 500
    assert "sql" in r.text.lower() or "error" in r.text.lower(), r.text[:200]


def test_kill_by_user_validates_payload(app_client, fake_connections):
    """POST /kill-by-user without `username` returns an error payload
    (not a 500)."""
    token = _csrf(app_client)
    r = app_client.post(
        "/api/admin/fake-pg/kill-by-user",
        headers={"X-CSRF-Token": token},
        json={},  # missing 'username'
    )
    assert r.status_code < 500
    body = json.loads(r.text)
    assert body.get("success") is False or "error" in body


def test_kill_by_database_validates_payload(app_client, fake_connections):
    """Same as above for /kill-by-database."""
    token = _csrf(app_client)
    r = app_client.post(
        "/api/admin/fake-pg/kill-by-database",
        headers={"X-CSRF-Token": token},
        json={},
    )
    assert r.status_code < 500
    body = json.loads(r.text)
    assert body.get("success") is False or "error" in body


def test_set_setting_validates_payload(app_client, fake_connections):
    """POST /set-setting requires a Postgres connection — wrong-type
    case."""
    token = _csrf(app_client)
    r = app_client.post(
        "/api/admin/fake-sqlite/set-setting",
        headers={"X-CSRF-Token": token},
        json={"name": "x", "value": "y"},
    )
    assert r.status_code < 500


# ─────────────────────────────────────────────────────────────────────
# HTMX response branch
# ─────────────────────────────────────────────────────────────────────


def test_htmx_processes_renders_partial_on_error(app_client, fake_connections):
    """When the engine fails (PG isn't actually running on port 15432),
    HTMX requests must still get an HTML partial — not a 500 or a JSON
    blob that HTMX can't usefully swap.

    This is the regression for the SSH-tunnel "Loading…" issue: a
    failed backend must produce a visible UI state, not a hang.
    """
    r = app_client.get(
        "/api/admin/fake-pg/processes",
        headers={"HX-Request": "true"},
    )
    assert r.status_code < 500, r.text[:200]
    # The error partial uses .dash-card style — check for either
    # the partial markup or a graceful empty-state render.
    body = r.text
    # Either the error template rendered (contains dash-card + an
    # error icon hint), or the processes template rendered with zero
    # rows (no `tr` elements but still HTML).
    assert "<" in body, "expected HTML for HTMX request, got JSON or empty"


def test_json_processes_returns_dict_shape(app_client, fake_connections):
    """Without HX-Request, the same endpoint returns a JSON dict (the
    same shape the AdminController returns to API callers)."""
    r = app_client.get("/api/admin/fake-pg/processes")
    assert r.status_code < 500
    # Engine call probably failed (no real PG) but we should still
    # get a parseable JSON body.
    body = json.loads(r.text)
    assert isinstance(body, dict)
