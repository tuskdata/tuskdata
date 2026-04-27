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
