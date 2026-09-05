"""Studio ergonomics (0.4.34): connection colour, UI preferences, plan
insight endpoint guards."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from tusk.core import config as cfg
from tusk.studio.routes.api import _clean_color


# ── pure ──────────────────────────────────────────────────────


@pytest.mark.parametrize("raw,expected", [
    ("#E5484D", "#e5484d"), (" #3b82f6 ", "#3b82f6"), ("", None), (None, None),
    ("red", None), ("#fff", None), ("#12345g", None), ("javascript:alert(1)", None),
])
def test_clean_color(raw, expected):
    assert _clean_color(raw) == expected


def test_config_roundtrip_keeps_ui_prefs(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_FILE", tmp_path / "config.toml")
    monkeypatch.setattr(cfg, "_config", None)
    cfg.update_config(table_preview_rows=150, map_tiles_url="https://t.example/{z}/{x}/{y}.png",
                      map_tiles_attribution="© me")
    monkeypatch.setattr(cfg, "_config", None)  # force a reload from disk
    c = cfg.get_config()
    assert c.table_preview_rows == 150
    assert c.map_tiles_url == "https://t.example/{z}/{x}/{y}.png"
    assert c.map_tiles_attribution == "© me"
    assert c.to_dict()["ui"]["table_preview_rows"] == 150


def test_connection_color_survives_toml(tmp_path: Path, monkeypatch):
    from tusk.core import connection as cn

    monkeypatch.setattr(cn, "CONN_FILE", tmp_path / "connections.toml")
    cn._connections.clear()
    c = cn.ConnectionConfig(name="prod", type="sqlite", path=":memory:", color="#e5484d")
    cn.add_connection(c)
    cn.save_connections_to_file()
    cn._connections.clear()
    cn.load_connections_from_file()
    loaded = cn.get_connection(c.id)
    assert loaded and loaded.color == "#e5484d"
    assert loaded.to_dict()["color"] == "#e5484d"
    cn.update_connection(c.id, color=None)
    assert cn.get_connection(c.id).color is None
    assert "color" not in cn.get_connection(c.id).to_dict()  # TOML has no null
    cn._connections.clear()


# ── HTTP ──────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def client():
    home = tempfile.mkdtemp(prefix="tusk_prefs_test_")
    Path(home, ".tusk").mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = home
    from litestar.testing import TestClient
    from tusk.studio.app import app

    saved_startup = list(app.on_startup or [])
    saved_shutdown = list(app.on_shutdown or [])
    app.on_startup[:] = [f for f in saved_startup if not getattr(f, "__module__", "").startswith("tusk.")]
    app.on_shutdown[:] = [f for f in saved_shutdown if not getattr(f, "__module__", "").startswith("tusk.")]
    try:
        with TestClient(app=app) as c:
            yield c
    finally:
        app.on_startup[:] = saved_startup
        app.on_shutdown[:] = saved_shutdown


def _csrf(client) -> dict:
    client.get("/studio")
    return {"x-csrf-token": client.cookies.get("tusk_csrf") or ""}


def test_settings_ui_validates(client, tmp_path: Path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_FILE", tmp_path / "config.toml")
    h = _csrf(client)
    assert "must be between" in client.post("/api/settings/ui", json={"table_preview_rows": 0}, headers=h).json()["error"]
    assert "placeholders" in client.post("/api/settings/ui", json={"map_tiles_url": "https://x/tiles.png"}, headers=h).json()["error"]
    assert "http" in client.post("/api/settings/ui", json={"map_tiles_url": "ftp://x/{z}/{x}/{y}"}, headers=h).json()["error"]
    r = client.post("/api/settings/ui", json={"table_preview_rows": 321, "map_tiles_url": "", "map_tiles_attribution": "x" * 900}, headers=h)
    assert r.json()["success"] and r.json()["ui"]["table_preview_rows"] == 321
    assert len(r.json()["ui"]["map_tiles_attribution"]) == 500
    assert client.get("/api/settings/").json()["ui"]["table_preview_rows"] == 321
    assert client.get("/settings/studio").status_code == 200


def test_connection_api_accepts_color(client, tmp_path: Path, monkeypatch):
    from tusk.core import connection as cn

    monkeypatch.setattr(cn, "CONN_FILE", tmp_path / "connections.toml")
    h = _csrf(client)
    r = client.post("/api/connections", json={"name": "c", "type": "sqlite", "path": ":memory:", "color": "#30A46C"}, headers=h)
    cid = r.json()["id"]
    listed = {c["id"]: c for c in client.get("/api/connections").json()}
    assert listed[cid]["color"] == "#30a46c"
    client.put(f"/api/connections/{cid}", json={"color": "not-a-colour"}, headers=h)
    assert {c["id"]: c for c in client.get("/api/connections").json()}[cid].get("color") is None
    client.delete(f"/api/connections/{cid}", headers=h)


def test_plan_insight_guards(client, monkeypatch):
    h = _csrf(client)
    assert client.post("/api/ai/plan-insight", json={"sql": "", "plan": []}, headers=h).json()["code"] == 400
    # No provider → 412, never a 500. (The AI config path is resolved at
    # import time from the developer's real HOME, so force it.)
    monkeypatch.setattr("tusk.studio.routes.ai.get_provider", lambda: None)
    r = client.post("/api/ai/plan-insight", json={"sql": "select 1", "plan": [{"Plan": {}}]}, headers=h).json()
    assert r["code"] == 412


def test_base_context_exposes_ui_prefs(client):
    html = client.get("/studio").text
    assert "window.TUSK_UI = {" in html and '"table_preview_rows"' in html


def test_save_connections_never_truncates_on_serialization_error(tmp_path: Path, monkeypatch):
    """A value tomli_w can't encode must leave the previous file intact
    (2026-09-05: a None colour wiped every connection)."""
    from tusk.core import connection as cn

    monkeypatch.setattr(cn, "CONN_FILE", tmp_path / "connections.toml")
    cn._connections.clear()
    cn.add_connection(cn.ConnectionConfig(name="keep", type="sqlite", path=":memory:"))
    cn.save_connections_to_file()
    before = (tmp_path / "connections.toml").read_bytes()
    assert b"keep" in before

    def boom(_data):
        raise TypeError("cannot encode None")

    monkeypatch.setattr(cn.tomli_w, "dumps", boom)
    with pytest.raises(TypeError):
        cn.save_connections_to_file()
    assert (tmp_path / "connections.toml").read_bytes() == before
    cn._connections.clear()
