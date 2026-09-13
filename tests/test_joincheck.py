"""Copilot joins are checked against the foreign keys (0.4.49)."""

from __future__ import annotations

import asyncio
import os
import uuid

import psycopg
import pytest

from tusk.core.joincheck import check_joins, parse_aliases, parse_joins, render_fks, type_family, warnings

ADMIN_DSN = os.environ.get("TUSK_TEST_PG_DSN", "postgresql://postgres@localhost:5432/postgres")

CATALOG = {
    "orders": {
        "cols": [{"name": "id", "type": "integer"}, {"name": "customer_id", "type": "integer"}, {"name": "code", "type": "text"}],
        "pks": ["id"], "fks": [{"col": "customer_id", "to_table": "customers", "to_col": "id"}],
    },
    "customers": {"cols": [{"name": "id", "type": "integer"}, {"name": "country", "type": "text"}, {"name": "region_id", "type": "integer"}],
                  "pks": ["id"], "fks": [{"col": "region_id", "to_table": "regions", "to_col": "region_id"}]},
    "regions": {"cols": [{"name": "region_id", "type": "integer"}, {"name": "name", "type": "text"}], "pks": ["region_id"], "fks": []},
    "products": {"cols": [{"name": "id", "type": "integer"}, {"name": "sku", "type": "character varying(20)"}], "pks": ["id"], "fks": []},
    "order_items": {
        "cols": [{"name": "order_id", "type": "integer"}, {"name": "product_id", "type": "integer"}],
        "pks": [], "fks": [{"col": "order_id", "to_table": "orders", "to_col": "id"}, {"col": "product_id", "to_table": "products", "to_col": "id"}],
    },
    "sales.invoices": {"cols": [{"name": "id", "type": "uuid"}, {"name": "order_id", "type": "integer"}], "pks": ["id"],
                       "fks": [{"col": "order_id", "to_table": "orders", "to_col": "id"}]},
}


def test_aliases_and_tables():
    aliases, ctes = parse_aliases('SELECT * FROM orders o JOIN customers AS c ON o.customer_id = c.id LEFT JOIN "Products" p ON 1=1 WHERE o.id > 1')
    assert aliases["o"] == "orders" and aliases["c"] == "customers" and aliases["p"] == "Products"
    assert aliases["orders"] == "orders"
    assert "where" not in aliases  # keyword after a table is not an alias
    assert ctes == set()


def test_cte_and_schema_qualified():
    sql = "WITH recent AS (SELECT * FROM orders WHERE id > 5) SELECT * FROM recent r JOIN sales.invoices i ON r.id = i.order_id JOIN public.customers c ON r.customer_id = c.id"
    aliases, ctes = parse_aliases(sql)
    assert ctes == {"recent"}
    assert aliases["i"] == "sales.invoices" and aliases["c"] == "customers"
    joins = parse_joins(sql)
    assert joins[0]["left"]["table"] is None  # CTE side is unknown
    assert joins[0]["right"]["table"] == "sales.invoices"


def test_fk_join_is_fine():
    f = check_joins("SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id", CATALOG)
    assert [x["status"] for x in f] == ["fk"]
    assert warnings(f) == []


def test_fk_in_reverse_direction_and_using():
    # USING needs the same column name on both sides: customers.region_id -> regions.region_id
    f = check_joins("SELECT * FROM orders o JOIN customers c ON c.id = o.customer_id JOIN regions USING (region_id)", CATALOG)
    assert [x["status"] for x in f] == ["fk", "fk"]
    assert f[1]["left"] == "regions.region_id" and f[1]["right"] == "customers.region_id"


def test_invented_join_is_flagged():
    # The demo failure: a 9B model joining orders straight to products.
    f = check_joins("SELECT p.sku FROM orders o JOIN products p ON o.id = p.id", CATALOG)
    assert f[0]["status"] == "no_fk"
    assert "orders.id" in f[0]["detail"] and "products.id" in f[0]["detail"]


def test_type_mismatch_is_flagged_harder():
    f = check_joins("SELECT * FROM orders o JOIN sales.invoices i ON o.code = i.id", CATALOG)
    assert f[0]["status"] == "type_mismatch"
    assert "uuid" in f[0]["detail"]


def test_unknown_sides_are_not_counted():
    sql = "WITH t AS (SELECT id, customer_id FROM orders) SELECT * FROM t JOIN customers c ON t.customer_id = c.id JOIN (SELECT 1 AS x) s ON s.x = c.id"
    f = check_joins(sql, CATALOG)
    assert all(x["status"] == "unknown" for x in f)
    assert warnings(f) == []


def test_type_families():
    assert type_family("character varying(50)") == type_family("text") == "text"
    assert type_family("bigint") == "int" and type_family("numeric(10,2)") == "num"
    assert type_family("timestamp with time zone") == type_family("date") == "time"
    assert type_family("integer[]") == "array"


def test_render_fks_lists_only_the_tables_involved():
    text = render_fks(CATALOG, {"orders", "products"})
    assert "orders.customer_id -> customers.id" in text
    assert "order_items.product_id -> products.id" in text
    assert "sales.invoices" in text  # points at orders
    assert render_fks(CATALOG, {"regions"}).count("\n") == 0  # only customers.region_id -> regions.region_id


def test_join_check_against_postgres(tmp_path, monkeypatch):
    import tusk.core.connection as conn_mod
    from tusk.core.connection import ConnectionConfig
    from tusk.studio.routes.ai import _check_joins

    name = f"tusk_test_join_{uuid.uuid4().hex[:8]}"
    try:
        admin = psycopg.connect(ADMIN_DSN, autocommit=True)
    except Exception:
        pytest.skip("no local PostgreSQL")
    admin.execute(f'CREATE DATABASE "{name}"')
    monkeypatch.setattr(conn_mod, "CONN_FILE", tmp_path / "connections.toml")  # never the user's file
    try:
        with psycopg.connect(ADMIN_DSN.rsplit("/", 1)[0] + f"/{name}", autocommit=True) as c:
            c.execute("CREATE TABLE customers (id int PRIMARY KEY, country text)")
            c.execute("CREATE TABLE products (id int PRIMARY KEY, sku text)")
            c.execute("CREATE TABLE orders (id int PRIMARY KEY, customer_id int REFERENCES customers(id), code text)")
        conn = ConnectionConfig(id="t-join", name="join", type="postgres", host="localhost", port=5432, database=name, user="postgres", password="")
        conn_mod.add_connection(conn, persist=False)
        try:
            ok = asyncio.run(_check_joins("t-join", "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"))
            assert [f["status"] for f in ok] == ["fk"]
            bad = asyncio.run(_check_joins("t-join", "SELECT * FROM orders o JOIN products p ON o.id = p.id"))
            assert [f["status"] for f in bad] == ["no_fk"]
            worse = asyncio.run(_check_joins("t-join", "SELECT * FROM orders o JOIN products p ON o.code = p.id"))
            assert [f["status"] for f in worse] == ["type_mismatch"]
            assert asyncio.run(_check_joins(None, "SELECT 1")) == []
        finally:
            conn_mod.delete_connection("t-join")
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()
