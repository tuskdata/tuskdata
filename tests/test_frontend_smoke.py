"""Frontend smoke tests — headless browser hits every page and asserts:

1. The page returns 200.
2. **No JavaScript console errors** (Alpine expression errors, undefined
   references, etc.) — this is the test that would have caught the
   v0.4.4 cmdk bug where a missing `cmdkPalette` global left the search
   overlay frozen on top of every page.
3. **No full-screen overlays are blocking the page** at first paint —
   verified by clicking a known interactive element on the page (the
   "Search or jump to…" button itself, which lives in the topnav and
   should be reachable without first dismissing anything).
4. **Critical landmarks exist** — each page is checked for a unique
   element so we know the template wasn't truncated mid-render.

The fixture spawns a real `tusk studio` server on a random port. Tests
that need an authenticated session register a single-user mode via the
default config (no auth required), so the server boots into a state
where every page is reachable.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

# Skip the whole module if Playwright isn't installed — keeps the rest
# of the suite green on machines that haven't run `playwright install`.
playwright = pytest.importorskip("playwright.sync_api")
from playwright.sync_api import sync_playwright, ConsoleMessage, Error  # noqa: E402


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="module")
def tusk_server():
    """Boot `tusk studio` on a free port. Yields the base URL."""
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    # Force single-user mode so every page is reachable without login.
    env["TUSK_AUTH_MODE"] = "single"
    # Use a throwaway HOME so the test doesn't pollute the dev's ~/.tusk.
    home = Path("/tmp") / f"tusk_e2e_home_{port}"
    home.mkdir(parents=True, exist_ok=True)
    env["HOME"] = str(home)

    # Use the venv's tusk binary — the test runs against installed code,
    # not src/, so we catch packaging bugs (missing static files, etc.).
    proc = subprocess.Popen(
        [sys.executable.replace("/python", "/tusk"), "studio", "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    # Wait for the server to start accepting connections.
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.2)
    else:
        proc.terminate()
        out = proc.stdout.read().decode() if proc.stdout else ""
        pytest.fail(f"Tusk did not start within 20s. Output:\n{out}")

    # Give Litestar+Granian one more moment to register routes.
    time.sleep(0.5)

    yield base_url

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


PAGES_TO_CHECK = [
    # (path, expected substring that proves the template rendered).
    # Pick landmarks that exist regardless of fixture state (no
    # connections, no history, no jobs) — `col-cards` etc. only render
    # when data is present.
    ("/home", "home-grid"),
    ("/studio", "studio-grid"),
    ("/schema", "schema-canvas"),
    ("/explore", "explore-page"),
    ("/scheduled", "sched-page"),
    ("/admin", "dash-grid"),
    ("/data", "git-branch"),
    ("/settings/ai", "settings-card"),
    ("/login", "login"),
]


def _collect_console_errors(page, errors: list[str]):
    """Record every JS console error and uncaught exception. Used as
    the gate that would have caught the cmdk bug."""

    def on_console(msg: ConsoleMessage):
        # Alpine logs expression errors as `console.warn` AND a separate
        # `Uncaught ReferenceError` — capture both. The Tailwind CDN
        # warning is noise we don't care about.
        if msg.type in ("error",) or "Alpine Expression Error" in msg.text:
            if "cdn.tailwindcss.com should not be used in production" in msg.text:
                return
            errors.append(f"[{msg.type}] {msg.text}")

    def on_pageerror(err: Error):
        errors.append(f"[pageerror] {err.message}")

    page.on("console", on_console)
    page.on("pageerror", on_pageerror)


@pytest.mark.parametrize("path,landmark", PAGES_TO_CHECK)
def test_page_renders_without_js_errors(tusk_server, path, landmark):
    """Every page renders without console errors and the page body is
    actually interactive (no full-screen overlay blocking clicks).
    """
    errors: list[str] = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context()
        page = context.new_page()
        _collect_console_errors(page, errors)

        response = page.goto(f"{tusk_server}{path}", wait_until="networkidle", timeout=15_000)
        assert response is not None, f"no response for {path}"
        assert response.status == 200, f"{path} returned {response.status}"

        # Landmark check — proves the template fully rendered.
        body = page.content()
        assert landmark in body, f"{path} missing landmark {landmark!r}"

        # Overlay-blocks-the-page check. The cmdk overlay is the most
        # likely offender; assert it is hidden at first paint. We use
        # the actual computed display to dodge any inline-style trickery.
        overlay_visible = page.evaluate(
            "() => { const el = document.querySelector('.cmdk-mask');"
            "  if (!el) return false;"
            "  return getComputedStyle(el).display !== 'none'; }"
        )
        assert overlay_visible is False, f"{path}: cmdk overlay is visible at first paint — page is unusable"

        # Make sure the topnav is interactable (clicking the search
        # button should not be blocked by anything covering it).
        search_btn = page.locator(".kbd-search")
        if search_btn.count():
            # If the click would be intercepted by an overlay, Playwright
            # raises within the timeout — that's exactly the failure
            # mode we want this test to catch.
            search_btn.first.click(timeout=2_000)
            # Now the cmdk SHOULD be visible; close it.
            page.keyboard.press("Escape")

        browser.close()

    assert not errors, f"{path} produced JS errors:\n" + "\n".join(errors)


def test_cmdk_opens_with_search_button(tusk_server):
    """Clicking the search button in the topnav opens the palette.

    Regression catch: in v0.4.4.1 the button used `@click="$dispatch(...)"`
    which silently no-ops when the topnav has no Alpine x-data ancestor,
    so only ⌘K worked. Switched to a plain `onclick` with
    `window.dispatchEvent(new CustomEvent(...))`.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(f"{tusk_server}/home", wait_until="networkidle")

        page.locator(".kbd-search").first.click()

        page.wait_for_function(
            "() => { const el = document.querySelector('.cmdk-mask');"
            "  return el && getComputedStyle(el).display !== 'none'; }",
            timeout=2_000,
        )
        page.wait_for_function(
            "() => document.activeElement && document.activeElement.tagName === 'INPUT'",
            timeout=2_000,
        )

        page.keyboard.press("Escape")
        browser.close()


def test_cmdk_opens_with_keyboard(tusk_server):
    """⌘K / Ctrl+K opens the command palette."""
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(f"{tusk_server}/home", wait_until="networkidle")

        # Press Ctrl+K (Cmd+K on darwin — Playwright doesn't care, both
        # routes hit the same handler in cmdk.js).
        page.keyboard.press("Control+k")
        # Overlay should now be visible.
        page.wait_for_function(
            "() => { const el = document.querySelector('.cmdk-mask');"
            "  return el && getComputedStyle(el).display !== 'none'; }",
            timeout=2_000,
        )
        # Input should be focused — Alpine's nextTick + the input.focus()
        # call inside openPalette() resolve a frame later, so we poll
        # rather than assert synchronously.
        page.wait_for_function(
            "() => document.activeElement && document.activeElement.tagName === 'INPUT'",
            timeout=2_000,
        )

        page.keyboard.press("Escape")
        browser.close()


def test_plugin_static_assets_resolve(tusk_server):
    """Plugin static assets at `/static/plugins/{id}/...` must resolve.

    Regression catch: when plugin assets moved out of the venv to
    `PLUGIN_STATIC_DIR`, the StaticFilesConfig for `/static` was
    registered first and shadowed `/static/plugins`, returning 404 for
    every plugin .js / .css. Each plugin's CSS/JS shipped fine but the
    UI was broken because the assets never loaded.
    """
    import urllib.request

    # Hit a known plugin static file from each of the 4 plugins. The
    # filenames are stable v0.4.5+ contracts.
    plugin_assets = [
        "/static/plugins/bi/bi.css",
        "/static/plugins/bi/bi.js",
        "/static/plugins/ci/ci.js",
        "/static/plugins/security/security.js",
        "/static/plugins/cluster/cluster.js",
    ]
    for asset in plugin_assets:
        req = urllib.request.Request(f"{tusk_server}{asset}")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                assert resp.status == 200, f"{asset} returned {resp.status}"
                body = resp.read()
                assert len(body) > 0, f"{asset} returned empty body"
        except urllib.error.HTTPError as e:
            pytest.fail(f"{asset} returned {e.code} — plugin assets are not being served")


def test_ai_provider_accepts_local_urls():
    """The AI provider URL is admin-supplied and the canonical use case
    is a local Ollama (`localhost`, `host.docker.internal`, or a LAN
    IP). The SSRF guard that protects notification webhooks/downloads
    must NOT be applied here — otherwise constructing the provider
    raises and the feature is unusable.

    Regression catch: in v0.4.5 the OllamaProvider constructor called
    `validate_outbound_url(...)` which rejected `host.docker.internal`
    (didn't resolve in test env) and `10.0.0.188` (private range), so
    pressing Save on /settings/ai surfaced "unsafe URL" and the user
    couldn't enable the feature.
    """
    from tusk.core.ai import OllamaProvider, build_provider, AIConfig

    # Construct directly — used to throw UnsafeURL.
    for url in (
        "http://localhost:11434",
        "http://127.0.0.1:11434",
        "http://host.docker.internal:11434",
        "http://10.0.0.188:11434",
        "http://192.168.1.50:11434",
    ):
        OllamaProvider(url, "qwen2.5-coder:3b")

    # And the factory should accept these too.
    cfg = AIConfig(
        enabled=True,
        provider="ollama",
        base_url="http://10.0.0.188:11434",
        model="qwen2.5-coder:3b",
    )
    provider = build_provider(cfg)
    assert provider is not None, "build_provider returned None for a local Ollama URL"
    assert provider.base_url == "http://10.0.0.188:11434"


def test_studio_query_error_does_not_crash_editor(tusk_server):
    """Highlighting a SQL parse-error position must never throw a
    `Mark decorations may not be empty` from CodeMirror.

    Regression catch: in v0.4.6.1 a query that ran fine but had an
    error reported at position == doc.length crashed the editor with
    that exact message — `highlightQueryError` produced an empty
    Decoration.mark range.
    """
    errors: list[str] = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        _collect_console_errors(page, errors)
        page.goto(f"{tusk_server}/studio", wait_until="networkidle")

        # Drive the highlight directly — server doesn't have a real
        # connection in the test fixture, so we exercise the function
        # the same way a real error response would.
        page.wait_for_function("() => typeof window.highlightQueryError === 'function'", timeout=5_000)
        for pos in [0, 1, 9999]:  # edges + past-end
            page.evaluate(f"window.highlightQueryError({pos})")
        # Force a doc that's empty + position past end — the original bug.
        page.evaluate("""
            if (window.editor) {
                window.editor.dispatch({
                    changes: { from: 0, to: window.editor.state.doc.length, insert: '' }
                });
            }
            window.highlightQueryError(50);
        """)
        browser.close()

    mark_errors = [e for e in errors if "Mark decorations may not be empty" in e]
    assert not mark_errors, f"highlightQueryError crashed the editor:\n" + "\n".join(mark_errors)


def test_plugin_static_path_traversal_blocked(tusk_server):
    """`/static/plugins/{id}/{path:path}` must reject `..` segments.

    Regression catch: the `serve_plugin_asset` handler uses
    `Path.resolve()` + `relative_to()` containment, but a bug here
    would let an authenticated user read arbitrary files via a URL
    like `/static/plugins/bi/../../../../etc/passwd`. This test
    confirms the guard fires.
    """
    import urllib.error
    import urllib.request

    payloads = [
        "/static/plugins/bi/../../../../etc/passwd",
        "/static/plugins/bi/../bi/bi.css",  # backs up then forward — still inside, but suspicious
        "/static/plugins/..../etc/passwd",
        "/static/plugins/bi%2F..%2F..%2F..%2Fetc%2Fpasswd",
    ]
    for path in payloads:
        req = urllib.request.Request(f"{tusk_server}{path}")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                body = resp.read()
                # The benign "bi/../bi/bi.css" form might 200 because it
                # resolves back inside; that's fine — what matters is
                # we never serve a 200 with /etc/passwd content.
                assert b"root:x:" not in body, f"{path} leaked /etc/passwd"
        except urllib.error.HTTPError as e:
            # Expected: 404 or 400 for traversal attempts.
            assert e.code in (400, 404), f"{path} returned unexpected {e.code}"


def test_ai_prompt_length_validation(tusk_server):
    """`/api/ai/sql` must reject oversized prompts (DoS / token-spend
    guard). Regression catch for audit finding #5."""
    import json
    import urllib.error
    import urllib.request

    # Prime the CSRF cookie.
    cookie_jar: dict[str, str] = {}
    with urllib.request.urlopen(f"{tusk_server}/api/auth/status", timeout=5) as resp:
        for h in resp.headers.get_all("Set-Cookie") or []:
            for part in h.split(";"):
                if "=" in part:
                    k, v = part.strip().split("=", 1)
                    if k in ("tusk_csrf", "tusk_session"):
                        cookie_jar[k] = v
    cookie_header = "; ".join(f"{k}={v}" for k, v in cookie_jar.items())

    huge_prompt = "x" * 10_000  # over the 8000-char cap
    body = json.dumps({"prompt": huge_prompt}).encode()
    req = urllib.request.Request(
        f"{tusk_server}/api/ai/sql",
        data=body,
        method="POST",
        headers={
            "content-type": "application/json",
            "x-csrf-token": cookie_jar.get("tusk_csrf", ""),
            "cookie": cookie_header,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            assert data.get("error") and "too long" in data["error"].lower(), \
                f"oversized prompt accepted: {data}"
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        assert e.code in (400, 413), f"unexpected {e.code}: {body}"


def test_schedule_save_results_as_traversal_blocked():
    """Path-traversal regression for `save_results_as` (audit #1)."""
    import asyncio
    from tusk.core.scheduled_tasks import _handle_query

    async def go(name: str):
        try:
            await _handle_query({
                "connection_id": "no-such-connection",  # will fail on connection lookup
                "sql": "SELECT 1",
                "save_results_as": name,
            })
        except Exception as e:
            return e
        return None

    # Names that should be REJECTED by the regex BEFORE the
    # connection-lookup path runs. We're checking that the error
    # message complains about the name, not about a missing connection.
    for evil in ["../../etc/foo", "/etc/foo", "foo/bar", "foo\\bar", ".."]:
        err = asyncio.run(go(evil))
        assert err is not None, f"evil name {evil!r} did not raise"
        msg = str(err).lower()
        assert "save_results_as" in msg or "escapes" in msg, \
            f"name {evil!r} did NOT trip the regex/containment guard: {err}"


def test_correlation_id_propagates(tusk_server):
    """`X-Correlation-ID` sent by the client must come back unchanged
    on the response so external traces line up with server logs."""
    import urllib.request

    req = urllib.request.Request(
        f"{tusk_server}/api/auth/status",
        headers={"X-Correlation-ID": "abc123"},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200
        cid = resp.headers.get("X-Correlation-ID")
        assert cid == "abc123", f"expected echoed correlation id, got {cid!r}"


def test_correlation_id_generated_when_missing(tusk_server):
    """When the client omits the header, the middleware mints a
    16-hex-char id and ships it back so callers can capture it."""
    import re
    import urllib.request

    req = urllib.request.Request(f"{tusk_server}/api/auth/status")
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200
        cid = resp.headers.get("X-Correlation-ID")
        assert cid, "no X-Correlation-ID header on response"
        assert re.match(r"^[0-9a-f]{16}$", cid), f"unexpected correlation id format: {cid!r}"


def test_admin_health_renders(tusk_server):
    """`/admin/health` returns 200 in single-user mode (loopback is the
    default admin allowance) and the page body contains the dashboard
    landmark."""
    import urllib.request

    req = urllib.request.Request(f"{tusk_server}/admin/health")
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200
        body = resp.read().decode("utf-8", errors="replace")
        assert "health-cards" in body, "health page missing #health-cards landmark"
        assert "System Health" in body, "health page missing title"


def test_homepage_renders_real_stats(tusk_server):
    """Homepage greeting + stat cards must render with computed values
    (not template literal placeholders)."""
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(f"{tusk_server}/home", wait_until="networkidle")

        # Greeting hero
        greeting = page.locator(".greeting").first.text_content()
        assert greeting and ("Good morning" in greeting or "Good afternoon" in greeting or "Good evening" in greeting), \
            f"greeting did not render: {greeting!r}"

        # Three stat cards must be present
        stat_cards = page.locator(".home-stat").count()
        assert stat_cards == 3, f"expected 3 stat cards, found {stat_cards}"

        # No literal `{{ ... }}` template placeholders left behind
        body = page.content()
        assert "{{" not in body or body.count("{{") < 5, "template placeholders leaked into rendered HTML"

        browser.close()


# ─────────────────────────────────────────────────────────────────
# v0.4.9 — Per-user isolation
# ─────────────────────────────────────────────────────────────────


def test_history_owner_isolation_in_history_layer():
    """Per-user isolation (v0.4.9): a row stamped with owner_id='u1' must
    NOT show up when another user queries with for_user_id='u2'.

    Unit test on the history layer — multi-user spawn-and-login E2E is
    expensive in this fixture, so we exercise the same code path the
    routes use.
    """
    import tempfile
    from pathlib import Path
    from tusk.core.history import QueryHistory

    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "history.db"
        h = QueryHistory(db)

        # User 1 saves a query and runs one.
        h.save_query(name="u1-only", sql="SELECT 1", owner_id="u1")
        h.add(connection_id="c", connection_name="C", sql="SELECT 1",
              execution_time_ms=1.0, owner_id="u1")

        # User 2 saves a query and runs one.
        h.save_query(name="u2-only", sql="SELECT 2", owner_id="u2")
        h.add(connection_id="c", connection_name="C", sql="SELECT 2",
              execution_time_ms=1.0, owner_id="u2")

        # Legacy unowned (single-user / pre-migration) — visible to all.
        h.save_query(name="legacy", sql="SELECT 0", owner_id="")
        h.add(connection_id="c", connection_name="C", sql="SELECT 0",
              execution_time_ms=1.0, owner_id="")

        # u1 sees only their own + legacy.
        u1_saved = {q.name for q in h.get_saved_queries(for_user_id="u1")}
        assert u1_saved == {"u1-only", "legacy"}, f"u1 saw: {u1_saved}"

        # u2 sees only their own + legacy.
        u2_saved = {q.name for q in h.get_saved_queries(for_user_id="u2")}
        assert u2_saved == {"u2-only", "legacy"}, f"u2 saw: {u2_saved}"

        # Admin (no for_user_id) sees everything.
        all_saved = {q.name for q in h.get_saved_queries()}
        assert all_saved == {"u1-only", "u2-only", "legacy"}, f"admin saw: {all_saved}"

        # Same isolation on history.
        u1_hist = {e.sql for e in h.get_recent(for_user_id="u1")}
        assert u1_hist == {"SELECT 1", "SELECT 0"}, f"u1 history saw: {u1_hist}"
        u2_hist = {e.sql for e in h.get_recent(for_user_id="u2")}
        assert u2_hist == {"SELECT 2", "SELECT 0"}, f"u2 history saw: {u2_hist}"


def test_legacy_unowned_history_visible_in_single_user():
    """Single-user mode (no for_user_id passed) shows owner_id='' rows fine.

    Backwards-compat regression: rows persisted before v0.4.9 have
    owner_id='' (the DEFAULT) — the listing must not hide them.
    """
    import tempfile
    from pathlib import Path
    from tusk.core.history import QueryHistory

    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "history.db"
        h = QueryHistory(db)

        h.add(connection_id="c", connection_name="C", sql="legacy SQL",
              execution_time_ms=1.0)  # no owner_id — defaults to ''
        h.save_query(name="legacy-saved", sql="SELECT 1")

        # Single-user mode passes for_user_id=None → see everything.
        rows = h.get_recent()
        assert any(e.sql == "legacy SQL" for e in rows), \
            "legacy unowned row not visible in single-user listing"
        saved = h.get_saved_queries()
        assert any(q.name == "legacy-saved" for q in saved), \
            "legacy unowned saved query not visible in single-user listing"


def test_scheduled_jobs_owner_isolation():
    """Scheduled jobs follow the same owner_id filter contract as history."""
    import tempfile
    from pathlib import Path
    from unittest.mock import patch
    import tusk.core.scheduled_tasks as st

    with tempfile.TemporaryDirectory() as tmp:
        # Redirect the SCHEDULER_DB to a temp path for this test only.
        with patch.object(st, "SCHEDULER_DB", Path(tmp) / "scheduler.db"), \
             patch.object(st, "TUSK_DIR", Path(tmp)):
            spec_a = st.JobSpec(
                id="job_a",
                kind=st.JobKind.QUERY,
                name="A",
                payload={"connection_id": "c", "sql": "SELECT 1", "save_results_as": None},
                trigger={"type": "interval", "minutes": 5},
                enabled=False,  # don't actually wire APScheduler
                owner_id="u1",
            )
            spec_b = st.JobSpec(
                id="job_b",
                kind=st.JobKind.QUERY,
                name="B",
                payload={"connection_id": "c", "sql": "SELECT 2", "save_results_as": None},
                trigger={"type": "interval", "minutes": 5},
                enabled=False,
                owner_id="u2",
            )
            spec_legacy = st.JobSpec(
                id="job_legacy",
                kind=st.JobKind.QUERY,
                name="legacy",
                payload={"connection_id": "c", "sql": "SELECT 0", "save_results_as": None},
                trigger={"type": "interval", "minutes": 5},
                enabled=False,
                owner_id="",
            )
            st.save_job(spec_a)
            st.save_job(spec_b)
            st.save_job(spec_legacy)

            u1_jobs = {j["id"] for j in st.list_jobs(for_user_id="u1")}
            assert u1_jobs == {"job_a", "job_legacy"}, f"u1 saw: {u1_jobs}"
            u2_jobs = {j["id"] for j in st.list_jobs(for_user_id="u2")}
            assert u2_jobs == {"job_b", "job_legacy"}, f"u2 saw: {u2_jobs}"
            all_jobs = {j["id"] for j in st.list_jobs()}
            assert all_jobs == {"job_a", "job_b", "job_legacy"}, f"admin saw: {all_jobs}"


def test_owner_id_migration_idempotent():
    """Running _init_db on a DB that already has owner_id must not error.

    Catches the case where Tusk restarts and the ALTER TABLE would fail
    because the column already exists — the try/except sqlite3.OperationalError
    must swallow that.
    """
    import tempfile
    from pathlib import Path
    from tusk.core.history import QueryHistory

    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "history.db"
        # First init creates the schema + adds owner_id.
        QueryHistory(db)
        # Second init must be a no-op (idempotent ALTER).
        QueryHistory(db)
        # And third for good measure.
        h = QueryHistory(db)
        # And it should still work end-to-end.
        h.save_query(name="test", sql="SELECT 1", owner_id="u1")
        assert len(h.get_saved_queries(for_user_id="u1")) == 1
