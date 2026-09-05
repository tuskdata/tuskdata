"""`tusk app` — Tusk in a native window (preview).

Tusk already runs as a local web server, exactly like pgAdmin; the only
thing missing for a "desktop app" is the window. This wraps the Studio in
a pywebview window (OS WebView: WebKit on macOS, WebView2 on Windows, GTK
WebKit on Linux — about 1 MB, no bundled Chromium).

Two modes:

- ``tusk app``                 start ``tusk studio`` on a free loopback
                               port in a child process, wait until it is
                               healthy, open the window, stop the server
                               when the window closes.
- ``tusk app --url http://…``  just open a window on an existing Tusk
                               (your Coolify deploy, a teammate's box).

Preview status: no packaging, no code signing, no auto-update — you still
install with pip. Those come with the desktop release once the signing
accounts exist.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

WINDOW_TITLE = "Tusk (preview)"
DEFAULT_SIZE = (1440, 900)
HEALTH_TIMEOUT_S = 40.0


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_for_health(base_url: str, timeout: float = HEALTH_TIMEOUT_S, proc: subprocess.Popen | None = None) -> bool:
    """Poll ``/api/health`` until it answers 200 or `timeout` passes. Stops
    early if the child process died."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            return False
        try:
            with urllib.request.urlopen(f"{base_url}/api/health", timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:  # noqa: BLE001 — refused / reset while booting
            pass
        time.sleep(0.25)
    return False


def studio_command(port: int) -> list[str]:
    """How to start the server as a child: the `tusk` console script next
    to this interpreter when it exists (installed package), else the
    module path (editable / source checkout)."""
    exe = Path(sys.executable)
    script = exe.with_name("tusk" + (".exe" if exe.suffix == ".exe" else ""))
    if script.exists():
        return [str(script), "studio", "--host", "127.0.0.1", "--port", str(port)]
    return [
        sys.executable, "-c",
        f"import sys; sys.argv = ['tusk', 'studio', '--host', '127.0.0.1', '--port', '{port}']; "
        "from tusk.cli import main; main()",
    ]


def _load_webview():
    try:
        import webview  # pywebview
    except ImportError:
        return None
    return webview


def run_app(url: str | None = None, port: int | None = None, *, webview_module=None) -> int:
    """Entry point for ``tusk app``. Returns a process exit code.

    `webview_module` lets tests inject a fake instead of pywebview.
    """
    webview = webview_module or _load_webview()
    if webview is None:
        print("tusk app needs pywebview. Install it with:\n\n"
              '  uv pip install "tuskdata[app]"\n')
        return 1

    proc: subprocess.Popen | None = None
    if url:
        target = url.rstrip("/")
        print(f"Opening {target} in a window…")
    else:
        port = port or free_port()
        target = f"http://127.0.0.1:{port}"
        print(f"Starting Tusk Studio on {target} …")
        # The `tusk` script itself spawns Granian; run the whole thing in its
        # own process group so closing the window takes the server down
        # too, not just the launcher.
        popen_kw: dict = {}
        if sys.platform == "win32":
            popen_kw["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            popen_kw["start_new_session"] = True
        proc = subprocess.Popen(
            studio_command(port),
            stdout=subprocess.DEVNULL if not _verbose() else None,
            stderr=subprocess.STDOUT if not _verbose() else None,
            **popen_kw,
        )
        if not wait_for_health(target, proc=proc):
            _stop(proc)
            print("Tusk Studio did not become healthy in time. Run `tusk studio` to see the error.")
            return 2

    try:
        webview.create_window(WINDOW_TITLE, target, width=DEFAULT_SIZE[0], height=DEFAULT_SIZE[1])
        webview.start()
    finally:
        _stop(proc)
    return 0


def _verbose() -> bool:
    import os

    return os.environ.get("TUSK_APP_VERBOSE", "").lower() in ("1", "true", "yes")


def _stop(proc: subprocess.Popen | None) -> None:
    """Stop the launcher AND its Granian workers (process group)."""
    if proc is None or proc.poll() is not None:
        return
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"], capture_output=True)
    else:
        import os
        import signal

        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        if sys.platform != "win32":
            import os
            import signal

            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        proc.kill()
