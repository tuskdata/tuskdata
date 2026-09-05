"""Data contracts: freeze, evaluate, violations log, notifications, YAML."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from tusk.core import contracts as ct
from tusk.core import schema_watch as sw


@pytest.fixture(autouse=True)
def isolated_db(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(sw, "DB_PATH", tmp_path / "schema_watch.db")


def _cat(**tables):
    out = {}
    for name, (cols, pks, fks) in tables.items():
        out[name] = {
            "cols": [{"name": c[0], "type": c[1], "nn": c[2]} for c in cols],
            "pks": list(pks),
            "fks": [{"col": f[0], "to_table": f[1], "to_col": f[2]} for f in fks],
            "indexes": [{"name": f"{name}_pkey", "def": "CREATE UNIQUE INDEX"}],
        }
    return out


BASE = _cat(
    orders=([("id", "integer", True), ("total", "numeric", False), ("customer_id", "integer", True)],
            ["id"], [("customer_id", "customers", "id")]),
    customers=([("id", "integer", True), ("name", "text", False)], ["id"], []),
)


# ── evaluate ──────────────────────────────────────────────────


def test_contract_holds_on_identical_and_additive_changes():
    expected = ct._expected_subset(BASE, None)
    assert ct.evaluate(expected, BASE) == []
    bigger = _cat(
        orders=([("id", "integer", True), ("total", "numeric", False), ("customer_id", "integer", True), ("status", "text", False)],
                ["id"], [("customer_id", "customers", "id")]),
        customers=([("id", "integer", True), ("name", "text", False)], ["id"], []),
        invoices=([("id", "integer", True)], ["id"], []),
    )
    assert ct.evaluate(expected, bigger) == []  # additions never violate


def test_evaluate_reports_every_breaking_change():
    expected = ct._expected_subset(BASE, None)
    live = _cat(
        orders=([("id", "bigint", True), ("total", "numeric", True)], ["id", "region"], []),
    )
    kinds = {(v["table"], v["kind"], v.get("column")) for v in ct.evaluate(expected, live)}
    assert ("customers", "table_missing", None) in kinds
    assert ("orders", "column_missing", "customer_id") in kinds
    assert ("orders", "type_changed", "id") in kinds
    assert ("orders", "nullability_changed", "total") in kinds
    assert ("orders", "pk_changed", None) in kinds
    assert ("orders", "fk_missing", "customer_id") in kinds
    s = ct.summarize(ct.evaluate(expected, live))
    assert "customers: table no longer exists" in s and "orders.id: type integer → bigint" in s


def test_freeze_subset_and_single_active_contract():
    c1 = ct.freeze_contract("c1", BASE, tables=["orders"], name="orders only")
    assert c1["table_count"] == 1 and set(c1["expected"]) == {"orders"}
    assert "indexes" not in c1["expected"]["orders"]
    c2 = ct.freeze_contract("c1", BASE)
    assert ct.active_contract("c1")["id"] == c2["id"]
    assert [c["id"] for c in ct.list_contracts("c1")] == [c2["id"]]
    assert len(ct.list_contracts("c1", include_inactive=True)) == 2
    with pytest.raises(ValueError):
        ct.freeze_contract("c1", BASE, tables=["nope"])


# ── check_contracts: log + notify ─────────────────────────────


class FakeSvc:
    def __init__(self):
        self.sent = []

    def send(self, key, message, **kw):
        self.sent.append((key, message, kw))
        return 1


def test_check_contracts_records_once_and_resolves(monkeypatch):
    svc = FakeSvc()
    monkeypatch.setattr("tusk.core.notifications.get_notification_service", lambda: svc)
    monkeypatch.setattr("tusk.core.connection.get_connection", lambda cid: type("C", (), {"name": "Prod"})())
    ct.freeze_contract("c1", BASE)

    assert ct.check_contracts("c1", BASE) == []
    broken = _cat(customers=([("id", "integer", True), ("name", "text", False)], ["id"], []))
    v1 = ct.check_contracts("c1", broken)
    assert v1 and v1[0]["kind"] == "table_missing"
    assert len(svc.sent) == 1 and svc.sent[0][0] == "contract.violated" and "Prod" in svc.sent[0][2]["title"]

    # Same breakage again: recorded once, not re-notified.
    ct.check_contracts("c1", broken)
    assert len(svc.sent) == 1
    assert len(ct.list_violations(ct.active_contract("c1")["id"])) == 1

    # Fixed: the open violation is resolved and a restore notice goes out.
    assert ct.check_contracts("c1", BASE) == []
    assert svc.sent[-1][0] == "contract.restored"
    assert ct.open_violation(ct.active_contract("c1")["id"]) is None
    assert ct.list_violations(ct.active_contract("c1")["id"])[0]["resolved_at"]


def test_run_watch_evaluates_contract(monkeypatch):
    svc = FakeSvc()
    catalogs = [BASE, _cat(customers=([("id", "integer", True), ("name", "text", False)], ["id"], []))]

    async def fake_fetch(conn, **kw):
        return catalogs.pop(0)

    class Conn:
        id, name, type = "c1", "Prod", "postgres"

    monkeypatch.setattr("tusk.core.catalog.fetch_catalog", fake_fetch)
    monkeypatch.setattr("tusk.core.connection.get_connection", lambda cid: Conn())
    monkeypatch.setattr("tusk.core.notifications.get_notification_service", lambda: svc)

    asyncio.run(sw.run_watch("c1"))  # baseline
    ct.freeze_contract("c1", BASE)
    out = asyncio.run(sw.run_watch("c1"))
    assert out["changed"] and out["contract_violations"][0]["table"] == "orders"
    keys = [k for k, _, _ in svc.sent]
    assert "schema.changed" in keys and "contract.violated" in keys


def test_yaml_export_is_readable():
    c = ct.freeze_contract("c1", BASE, name="prod v1")
    y = ct.to_yaml(c)
    assert y.startswith("# Tusk data contract")
    assert "contract: prod v1" in y and "  orders:" in y and "primary_key: [id]" in y
    assert "references: customers.id" in y and "nullable: false" in y
