"""connections.toml must never be overwritten by a process that did not load it."""

from __future__ import annotations

import tomllib

import tusk.core.connection as conn_mod
from tusk.core.connection import ConnectionConfig


def test_unloaded_registry_does_not_wipe_file(tmp_path, monkeypatch):
    f = tmp_path / "connections.toml"
    f.write_text('[[connections]]\nid = "a1"\nname = "prod"\ntype = "postgres"\nhost = "h"\nport = 5432\ndatabase = "d"\nuser = "u"\npassword = ""\n')
    monkeypatch.setattr(conn_mod, "CONN_FILE", f)
    monkeypatch.setattr(conn_mod, "TUSK_DIR", tmp_path)
    monkeypatch.setattr(conn_mod, "_connections", {})
    monkeypatch.setattr(conn_mod, "_loaded_from_file", False)
    assert conn_mod.save_connections_to_file() is False
    assert len(tomllib.loads(f.read_text())["connections"]) == 1

    # Once loaded, deleting the last connection legitimately empties the file.
    conn_mod.load_connections_from_file()
    assert conn_mod.delete_connection("a1")
    assert tomllib.loads(f.read_text())["connections"] == []


def test_unloaded_registry_may_write_when_it_has_content(tmp_path, monkeypatch):
    f = tmp_path / "connections.toml"
    monkeypatch.setattr(conn_mod, "CONN_FILE", f)
    monkeypatch.setattr(conn_mod, "TUSK_DIR", tmp_path)
    monkeypatch.setattr(conn_mod, "_connections", {})
    monkeypatch.setattr(conn_mod, "_loaded_from_file", False)
    conn_mod.add_connection(ConnectionConfig(id="x1", name="x", type="sqlite", path="/tmp/x.db"))
    assert len(tomllib.loads(f.read_text())["connections"]) == 1
