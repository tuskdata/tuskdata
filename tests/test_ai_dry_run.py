"""Copilot SQL is EXPLAIN-checked against the database before it is shown."""

from __future__ import annotations

import asyncio
import os
import uuid

import psycopg
import pytest

from tusk.core.connection import ConnectionConfig
from tusk.studio.routes.ai import _dry_run, _looks_like_schema_error

ADMIN_DSN = os.environ.get("TUSK_TEST_PG_DSN", "postgresql://postgres@localhost:5432/postgres")


def test_schema_error_detection():
    assert _looks_like_schema_error('column o.product_id does not exist')
    assert _looks_like_schema_error('relation "orderz" does not exist')
    assert not _looks_like_schema_error("syntax error at or near FROM")


def test_dry_run_skips_what_it_cannot_check():
    assert asyncio.run(_dry_run(None, "SELECT 1")) == (None, None)
    assert asyncio.run(_dry_run("x", "-- no orders table here")) == (None, None)
    assert asyncio.run(_dry_run("x", "DELETE FROM t")) == (None, None)


def test_dry_run_against_postgres(tmp_path, monkeypatch):
    import tusk.core.connection as conn_mod

    name = f"tusk_test_dry_{uuid.uuid4().hex[:8]}"
    try:
        admin = psycopg.connect(ADMIN_DSN, autocommit=True)
    except Exception:
        pytest.skip("no local PostgreSQL")
    admin.execute(f'CREATE DATABASE "{name}"')
    monkeypatch.setattr(conn_mod, "CONN_FILE", tmp_path / "connections.toml")  # never the user's file
    try:
        with psycopg.connect(ADMIN_DSN.rsplit("/", 1)[0] + f"/{name}", autocommit=True) as c:
            c.execute("CREATE TABLE orders (id int, customer_id int)")
        conn = ConnectionConfig(id="t-dry", name="dry", type="postgres", host="localhost", port=5432, database=name, user="postgres", password="")
        conn_mod.add_connection(conn, persist=False)
        try:
            assert asyncio.run(_dry_run("t-dry", "SELECT id, customer_id FROM orders")) == (True, None)
            ok, err = asyncio.run(_dry_run("t-dry", "SELECT o.product_id FROM orders o"))
            assert ok is False and "product_id" in err and _looks_like_schema_error(err)
        finally:
            conn_mod.delete_connection("t-dry")
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()
