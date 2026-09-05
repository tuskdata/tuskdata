"""Shared helpers for the Playwright-driven tests.

Two things every browser test module needs: the venv's `tusk` binary
(built from the interpreter path — `python3` on Linux must not become
`tusk3`) and a way to skip cleanly when the Playwright *browsers* are not
installed. The Python package can be present (it arrives as a transitive
dependency) while `playwright install chromium` was never run.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Several test modules point HOME at a throwaway directory (module-scoped
# fixtures in test_admin_routes, test_middleware, ...). Playwright resolves
# its browser cache from HOME at launch time, so once one of those has run
# the browsers "disappear" for every later module. Pin the real location
# now, while HOME is still the developer's / the CI runner's.
if "PLAYWRIGHT_BROWSERS_PATH" not in os.environ:
    _home = Path.home()
    if sys.platform == "darwin":
        _cache = _home / "Library" / "Caches" / "ms-playwright"
    elif sys.platform == "win32":
        _cache = Path(os.environ.get("LOCALAPPDATA", _home / "AppData" / "Local")) / "ms-playwright"
    else:
        _cache = _home / ".cache" / "ms-playwright"
    if _cache.exists():
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(_cache)


def tusk_binary() -> str:
    """Path to the `tusk` console script next to the running interpreter."""
    exe = Path(sys.executable)
    candidate = exe.with_name("tusk" + (".exe" if exe.suffix == ".exe" else ""))
    if not candidate.exists():
        pytest.skip(f"tusk console script not found at {candidate} — install the project first")
    return str(candidate)


def require_chromium() -> None:
    """Skip the calling module when Chromium isn't installed for Playwright."""
    from playwright.sync_api import Error, sync_playwright

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            browser.close()
    except Error as exc:  # browser binary missing, sandbox issues, ...
        pytest.skip(f"Playwright Chromium not available: {str(exc).splitlines()[0][:120]}")
