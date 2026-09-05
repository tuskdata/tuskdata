"""`tusk app` (preview): server bootstrap + window, with pywebview faked."""

from __future__ import annotations

import subprocess
import sys

from tusk import app_window


class FakeWebview:
    def __init__(self):
        self.windows = []
        self.started = False

    def create_window(self, title, url, **kw):
        self.windows.append((title, url, kw))

    def start(self):
        self.started = True


def test_missing_pywebview_prints_hint(capsys, monkeypatch):
    monkeypatch.setattr(app_window, "_load_webview", lambda: None)
    assert app_window.run_app() == 1
    assert 'tuskdata[app]' in capsys.readouterr().out


def test_url_mode_opens_window_without_server(monkeypatch):
    fake = FakeWebview()
    spawned = []
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: spawned.append(a) or None)
    assert app_window.run_app(url="http://10.0.0.188:7000/", webview_module=fake) == 0
    assert spawned == []
    title, url, kw = fake.windows[0]
    assert url == "http://10.0.0.188:7000" and "preview" in title.lower() and fake.started


def test_local_mode_starts_server_waits_and_stops(monkeypatch):
    fake = FakeWebview()

    class Proc:
        def __init__(self):
            self.terminated = False

        def poll(self):
            return None

        def terminate(self):
            self.terminated = True

        def wait(self, timeout=None):
            return 0

    proc = Proc()
    calls = {}
    def fake_popen(cmd, **k):
        calls["cmd"] = cmd
        return proc

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setattr(app_window, "_stop", lambda p: setattr(p, "terminated", True) if p else None)
    monkeypatch.setattr(app_window, "wait_for_health", lambda url, timeout=40.0, proc=None: calls.setdefault("health", url) or True)

    assert app_window.run_app(port=8123, webview_module=fake) == 0
    assert "8123" in " ".join(calls["cmd"]) and "studio" in " ".join(calls["cmd"])
    assert calls["health"] == "http://127.0.0.1:8123"
    assert fake.windows[0][1] == "http://127.0.0.1:8123"
    assert proc.terminated  # server stopped when the window closes


def test_unhealthy_server_is_stopped_and_reported(monkeypatch, capsys):
    fake = FakeWebview()

    class Proc:
        terminated = False

        def poll(self):
            return None

        def terminate(self):
            self.terminated = True

        def wait(self, timeout=None):
            return 0

    proc = Proc()
    monkeypatch.setattr(subprocess, "Popen", lambda cmd, **k: proc)
    monkeypatch.setattr(app_window, "_stop", lambda p: setattr(p, "terminated", True) if p else None)
    monkeypatch.setattr(app_window, "wait_for_health", lambda *a, **k: False)
    assert app_window.run_app(port=8124, webview_module=fake) == 2
    assert proc.terminated and fake.windows == []
    assert "did not become healthy" in capsys.readouterr().out


def test_studio_command_targets_this_interpreter():
    cmd = app_window.studio_command(9000)
    assert "9000" in " ".join(cmd)
    assert cmd[0].startswith(str(sys.exec_prefix)) or cmd[0] == sys.executable


def test_free_port_is_usable():
    import socket

    port = app_window.free_port()
    s = socket.socket()
    s.bind(("127.0.0.1", port))
    s.close()
