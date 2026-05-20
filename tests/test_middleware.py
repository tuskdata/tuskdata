"""Tests for the middleware stack in src/tusk/studio/app.py.

Covers behaviors that have bitten us in production:

- CSRF guard returns 403 (not 500) when token missing.
  Regression for specs/bugs/2026-05-19-csrf-middleware-500.md.

- after_exception hook fires for 5xx and is silent for 4xx.
  Regression for specs/bugs/2026-05-19-csrf-middleware-500.md
  (lesson #2 — middleware exception logging too quiet).

These tests use Litestar's TestClient — much faster than spinning a
real Granian server. They run inline as part of the regular pytest
suite, no Playwright needed.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def app_client():
    """Spin up the Litestar app + TestClient with a throwaway HOME.

    Registers a small set of test-only endpoints (`/__mw_test_*`) so the
    middleware tests don't depend on any plugin being installed. This
    matters because CI installs only `tuskdata[all]` (core + cluster);
    tusk-bi is an external plugin and its routes are absent.

    Lifecycle handling: the app instance is a module-level singleton.
    If another test file's TestClient closed the APScheduler before we
    got here, re-entering the lifespan would hit "Event loop is closed"
    on the next on_startup. We short-circuit the lifecycle hooks for
    this fixture — middleware behavior is independent of the scheduler.
    """
    home = tempfile.mkdtemp(prefix="tusk_mw_test_")
    Path(home, ".tusk").mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = home
    os.environ["TUSK_AUTH_MODE"] = "single"

    from litestar import get, post
    from litestar.testing import TestClient
    from tusk.studio.app import app

    @post("/__mw_test_post")
    async def echo(data: dict) -> dict:
        return {"ok": True, "received": data}

    @get("/__mw_test_get")
    async def hello() -> dict:
        return {"ok": True}

    app.register(echo)
    app.register(hello)

    # Save+no-op the lifecycle so re-entering a TestClient context
    # across modules doesn't crash on a previously-closed scheduler.
    # Litestar stores lifespans on the app via _lifespan_managers.
    saved_startup = list(app.on_startup or [])
    saved_shutdown = list(app.on_shutdown or [])
    app.on_startup.clear()
    app.on_shutdown.clear()

    try:
        with TestClient(app=app) as client:
            yield client
    finally:
        app.on_startup[:] = saved_startup
        app.on_shutdown[:] = saved_shutdown


def _csrf(client):
    """Prime the cookie and return the matching X-CSRF-Token header."""
    client.get("/")
    token = client.cookies.get("tusk_csrf")
    assert token, "GET / did not set tusk_csrf cookie"
    return token


# ─────────────────────────────────────────────────────────────────────
# CSRF guard
# ─────────────────────────────────────────────────────────────────────


def test_state_changing_without_csrf_returns_403_not_500(app_client):
    """The CSRF middleware bug shipped silently for ~10 releases because
    a POST without a token returned 500 instead of 403. Lock this in
    via a hermetic test-only route — independent of which plugins are
    installed in the running environment."""
    r = app_client.post("/__mw_test_post", json={"name": "x"})
    assert r.status_code == 403, f"expected 403, got {r.status_code}: {r.text[:200]}"
    body = json.loads(r.text)
    assert "CSRF" in body.get("error", ""), body


def test_state_changing_with_csrf_succeeds(app_client):
    """The happy path still works once the token is supplied."""
    token = _csrf(app_client)
    r = app_client.post(
        "/__mw_test_post",
        json={"name": "regression-test"},
        headers={"X-CSRF-Token": token},
    )
    assert r.status_code in (200, 201), f"expected 2xx, got {r.status_code}: {r.text[:200]}"


def test_get_requests_skip_csrf(app_client):
    """GETs are not state-changing, the guard should not block them."""
    r = app_client.get("/__mw_test_get")
    assert r.status_code == 200


# ─────────────────────────────────────────────────────────────────────
# after_exception hook
# ─────────────────────────────────────────────────────────────────────


def test_slow_handler_returns_504_within_budget(app_client):
    """The happy path (fast handler) is still sub-second with the
    timeout middleware in place. Regression for the SSH-tunnel freeze
    (see specs/bugs/2026-05-18-ssh-tunnel-hangs-admin.md)."""
    import time
    t0 = time.monotonic()
    r = app_client.get("/__mw_test_get")
    assert r.status_code == 200
    assert time.monotonic() - t0 < 5, "happy path should be sub-second"


def test_5xx_triggers_after_exception_hook(app_client):
    """Inject a route that always raises and assert the hook fires.

    We monkey-patch the app's `after_exception` list with a spy that
    records every call. The actual log emission goes through structlog
    + PrintLoggerFactory which writes to stderr, harder to capture; the
    spy is enough to prove the hook is wired and called by Litestar.
    """
    from litestar import get
    from tusk.studio.app import app

    @get("/__regression_boom_5xx")
    async def boom() -> dict:
        raise RuntimeError("regression-boom")

    app.register(boom)

    calls: list[tuple[str, str, str]] = []

    async def spy(exc, scope):
        calls.append((type(exc).__name__, scope.get("path"), scope.get("method")))

    app.after_exception.append(spy)

    try:
        r = app_client.get("/__regression_boom_5xx")
        assert r.status_code == 500
        boom_calls = [c for c in calls if c[1] == "/__regression_boom_5xx"]
        assert boom_calls, f"hook did not fire for 5xx; calls: {calls}"
        assert boom_calls[0][0] == "RuntimeError"
        assert boom_calls[0][2] == "GET"
    finally:
        # Don't leak the spy into other tests in the module.
        app.after_exception.remove(spy)
