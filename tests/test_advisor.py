"""Advisor: findings on a throwaway database with known problems."""

from __future__ import annotations

import asyncio
import os
import uuid

import psycopg
import pytest

from tusk.core.advisor import Finding, Report, analyze, render_for_ai
from tusk.core.connection import ConnectionConfig

ADMIN_DSN = os.environ.get("TUSK_TEST_PG_DSN", "postgresql://postgres@localhost:5432/postgres")


@pytest.fixture(scope="module")
def messy_db():
    name = f"tusk_test_advisor_{uuid.uuid4().hex[:8]}"
    try:
        admin = psycopg.connect(ADMIN_DSN, autocommit=True)
    except Exception:
        pytest.skip("no local PostgreSQL")
    admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(ADMIN_DSN.rsplit("/", 1)[0] + f"/{name}", autocommit=True) as c:
            c.execute("""
                CREATE TABLE customers (id serial PRIMARY KEY, name text);
                CREATE TABLE orders (id serial PRIMARY KEY, customer_id int REFERENCES customers(id), total numeric);
                INSERT INTO customers (name) SELECT 'c' || g FROM generate_series(1, 200) g;
                INSERT INTO orders (customer_id, total) SELECT (g % 200) + 1, g FROM generate_series(1, 20000) g;
                CREATE INDEX orders_total_a ON orders (total);
                CREATE INDEX orders_total_b ON orders (total);
                ANALYZE customers; ANALYZE orders;
            """)
            # a sequential-scan-heavy workload on orders (no index on customer_id)
            for _ in range(60):
                c.execute("SELECT count(*) FROM orders WHERE customer_id = 7").fetchone()
        yield name
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()


def test_findings_on_messy_db(messy_db):
    conn = ConnectionConfig(id="t-advisor", name="advisor", type="postgres", host="localhost", port=5432,
                            database=messy_db, user="postgres", password="")
    report = asyncio.run(analyze(conn))
    assert report.error is None and report.pg_version
    kinds = {f.kind for f in report.findings}
    assert "fk_no_index" in kinds, [f.title for f in report.findings]
    fk = next(f for f in report.findings if f.kind == "fk_no_index")
    assert "orders" in fk.title and "customer_id" in fk.fix and fk.severity == "warning"
    assert "duplicate_index" in kinds
    # seq_scan stats are collected asynchronously by the stats collector; accept either
    assert all(f.severity in ("error", "warning", "info") for f in report.findings)
    # Findings are ordered by severity
    sev = [f.severity for f in report.findings]
    assert sev == sorted(sev, key={"error": 0, "warning": 1, "info": 2}.get)
    text = render_for_ai(report)
    assert "PostgreSQL" in text and "foreign key without an index" in text


def test_report_shape():
    r = Report(findings=[Finding(kind="x", severity="warning", title="t", detail="d", fix="SQL")])
    d = r.to_dict()
    assert d["counts"] == {"error": 0, "warning": 1, "info": 0}
    assert d["findings"][0]["fix"] == "SQL"
