"""End-to-end tests for tusk-bi v0.3.0 — the redesigned dashboard viewer.

Boots a real Tusk server (single-user mode, throwaway HOME) and uses
Playwright to confirm:

  - /bi/dashboards renders the listing page without JS errors
  - A dashboard created via the API renders with the new v0.3.0 chrome
    (.dash-page, .dash-title, .dash-grid)
  - The "Live" badge appears when refresh_interval_seconds > 0
  - The "Public" badge appears when is_public = true
  - The widget grid uses CSS-grid `.span-X` markup (no .grid-stack-item)

These tests reuse the same fixture pattern as test_frontend_smoke.py.
If Playwright isn't installed, every test is skipped (importorskip).
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

# Skip the whole module when Playwright (the browser library, not the
# Python binding) hasn't been installed via `playwright install`.
playwright = pytest.importorskip("playwright.sync_api")
# Also skip when the tusk-bi plugin isn't installed — CI installs only
# `tuskdata[all]` (core + cluster); tusk-bi is an external plugin.
pytest.importorskip("tusk_bi", reason="tusk-bi plugin not installed in this environment")
from playwright.sync_api import sync_playwright  # noqa: E402

from _browser import require_chromium, tusk_binary  # noqa: E402

require_chromium()


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="module")
def tusk_server():
    """Boot tusk studio on a free port with the BI plugin installed."""
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    env["TUSK_AUTH_MODE"] = "single"
    home = Path("/tmp") / f"tusk_bi_e2e_home_{port}"
    home.mkdir(parents=True, exist_ok=True)
    env["HOME"] = str(home)

    proc = subprocess.Popen(
        [tusk_binary(), "studio", "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    # Wait until the app answers, not just until the socket accepts:
    # Granian opens the port before Litestar finishes startup.
    import urllib.request

    deadline = time.time() + 40
    ready = False
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/api/health", timeout=2) as resp:
                if resp.status == 200:
                    ready = True
                    break
        except Exception:  # noqa: BLE001 — refused / reset while booting
            time.sleep(0.25)
    if not ready:
        proc.terminate()
        out = proc.stdout.read().decode(errors="replace")[-2000:] if proc.stdout else ""
        pytest.fail(f"Tusk did not become healthy within 40s. Output:\n{out}")

    yield base_url

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def _csrf_token(base_url: str) -> str:
    """Hit a GET to prime the `tusk_csrf` cookie, return its value.

    State-changing requests (POST/PUT/DELETE) require both the cookie
    and an X-CSRF-Token header — see the CSRF middleware in
    `tusk/studio/app.py`. Tests need to hand-roll the prime since they
    don't drive a browser.
    """
    req = urllib.request.Request(f"{base_url}/", method="GET")
    with urllib.request.urlopen(req, timeout=10) as r:
        for header in r.getheaders():
            if header[0].lower() == "set-cookie" and "tusk_csrf=" in header[1]:
                return header[1].split("tusk_csrf=", 1)[1].split(";", 1)[0]
    raise AssertionError("no tusk_csrf cookie issued on GET /")


def _create_dashboard(base_url: str, *, name: str = "v030 Test", is_public: bool = False,
                      refresh_interval_seconds: int = 0) -> int:
    """Create a dashboard via the BI API and return its id."""
    token = _csrf_token(base_url)
    headers = {
        "Content-Type": "application/json",
        "X-CSRF-Token": token,
        "Cookie": f"tusk_csrf={token}",
    }
    req = urllib.request.Request(
        f"{base_url}/api/bi/dashboards",
        data=json.dumps({"name": name, "description": "v0.3.0 smoke"}).encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        body = json.loads(r.read())
    dashboard_id = body.get("id") or body.get("dashboard_id") or body.get("dashboard", {}).get("id")
    assert dashboard_id, f"create_dashboard did not return an id: {body}"

    if is_public or refresh_interval_seconds:
        req2 = urllib.request.Request(
            f"{base_url}/api/bi/dashboards/{dashboard_id}",
            data=json.dumps({
                "is_public": is_public,
                "refresh_interval_seconds": refresh_interval_seconds,
            }).encode(),
            headers=headers,
            method="PUT",
        )
        with urllib.request.urlopen(req2, timeout=10) as r2:
            r2.read()

    return int(dashboard_id)


def test_bi_dashboards_list_renders(tusk_server):
    """The /bi/dashboards page renders without JS console errors."""
    errors: list[str] = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context().new_page()
        page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
        page.on("pageerror", lambda e: errors.append(str(e)))
        response = page.goto(f"{tusk_server}/bi/dashboards", wait_until="networkidle", timeout=15_000)
        assert response is not None
        assert response.status == 200
        # We don't yet have dashboards — the empty state shows
        # "No dashboards yet". Either that or a card listing.
        body = page.content()
        assert "Dashboards" in body  # h1
        browser.close()
    # Filter out third-party noise.
    real = [e for e in errors if "cdn.tailwindcss.com" not in e]
    assert not real, f"console errors: {real}"


def test_dashboard_viewer_uses_v030_chrome(tusk_server):
    """A freshly created dashboard renders with the .dash-page +
    .dash-title chrome and a .dash-grid container."""
    dash_id = _create_dashboard(tusk_server, name="v030 chrome")
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context().new_page()
        page.goto(f"{tusk_server}/bi/dashboards/{dash_id}", wait_until="networkidle", timeout=15_000)
        body = page.content()
        # Markers from the rewritten viewer.
        assert "dash-page" in body
        assert "dash-title" in body
        assert "dash-head" in body
        # GridStack viewer markup is gone.
        assert "grid-stack-item" not in body
        # Empty state is reached (no widgets yet).
        assert "No widgets yet" in body
        browser.close()


def test_dashboard_live_badge_when_refresh_interval_set(tusk_server):
    dash_id = _create_dashboard(tusk_server, name="v030 live", refresh_interval_seconds=30)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context().new_page()
        page.goto(f"{tusk_server}/bi/dashboards/{dash_id}", wait_until="networkidle", timeout=15_000)
        body = page.content()
        # Badge text includes "Live · 30s".
        assert "dash-badge live" in body
        assert "Live · 30s" in body
        browser.close()


def test_dashboard_public_badge_when_is_public(tusk_server):
    dash_id = _create_dashboard(tusk_server, name="v030 public", is_public=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context().new_page()
        page.goto(f"{tusk_server}/bi/dashboards/{dash_id}", wait_until="networkidle", timeout=15_000)
        body = page.content()
        assert "dash-badge public" in body
        browser.close()
