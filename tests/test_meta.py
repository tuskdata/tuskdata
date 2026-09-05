"""tusk.core.meta — one metadata file, legacy files folded in on first open."""

from __future__ import annotations

import sqlite3

from tusk.core import meta


def _legacy(path, ddl: str, rows: list[tuple], insert: str):
    conn = sqlite3.connect(path)
    conn.executescript(ddl)
    conn.executemany(insert, rows)
    conn.commit()
    conn.close()


def test_connect_folds_legacy_files(tmp_path):
    _legacy(
        tmp_path / "users.db",
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE);"
        "CREATE INDEX idx_users_name ON users(username);",
        [(1, "alice"), (2, "bob")],
        "INSERT INTO users VALUES (?, ?)",
    )
    _legacy(
        tmp_path / "history.db",
        "CREATE TABLE query_history (id INTEGER PRIMARY KEY, sql TEXT);",
        [(7, "select 1")],
        "INSERT INTO query_history VALUES (?, ?)",
    )
    target = tmp_path / "tusk.db"
    meta._migrated.discard(target)

    conn = meta.connect(target)
    try:
        assert conn.execute("SELECT username FROM users ORDER BY id").fetchall() == [("alice",), ("bob",)]
        assert conn.execute("SELECT sql FROM query_history").fetchone() == ("select 1",)
        assert conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_users_name'").fetchone()
        # AUTOINCREMENT continues after the copied rows.
        conn.execute("INSERT INTO users (username) VALUES ('carol')")
        assert conn.execute("SELECT id FROM users WHERE username='carol'").fetchone()[0] == 3
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    finally:
        conn.close()

    assert not (tmp_path / "users.db").exists()
    assert (tmp_path / "users.db.migrated").exists()
    assert (tmp_path / "history.db.migrated").exists()


def test_migration_runs_once_and_never_merges_into_existing_tables(tmp_path):
    target = tmp_path / "tusk.db"
    meta._migrated.discard(target)
    c = meta.connect(target)
    c.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)")
    c.execute("INSERT INTO users VALUES (1, 'existing')")
    c.commit()
    c.close()

    _legacy(tmp_path / "users.db", "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);", [(9, "ghost")], "INSERT INTO users VALUES (?, ?)")
    # Already migrated in this process: the legacy file is ignored.
    assert meta.migrate_legacy(target) == []
    assert (tmp_path / "users.db").exists()

    # A fresh process would see it, but must not merge into a populated table.
    meta._migrated.discard(target)
    assert meta.migrate_legacy(target) == ["users.db"]
    c = meta.connect(target)
    assert c.execute("SELECT username FROM users").fetchall() == [("existing",)]
    c.close()


def test_non_default_paths_do_not_migrate(tmp_path):
    _legacy(tmp_path / "users.db", "CREATE TABLE users (id INTEGER PRIMARY KEY);", [(1,)], "INSERT INTO users VALUES (?)")
    c = meta.connect(tmp_path / "other.db")
    assert c.execute("SELECT name FROM sqlite_master WHERE name='users'").fetchone() is None
    c.close()
    assert (tmp_path / "users.db").exists()
