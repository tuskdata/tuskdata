"""Schema Watch: catalog diff, summary, storage, and the run loop with a
faked catalog (no database needed)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from tusk.core import schema_watch as sw


@pytest.fixture(autouse=True)
def isolated_db(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(sw, "DB_PATH", tmp_path / "schema_watch.db")


def _catalog(**tables):
    """Tiny builder: _catalog(orders=(cols, pks, fks, indexes))."""
    out = {}
    for name, (cols, pks, fks, indexes) in tables.items():
        out[name] = {
            "cols": [{"name": c[0], "type": c[1], "nn": c[2]} for c in cols],
            "pks": list(pks),
            "fks": [{"col": f[0], "to_table": f[1], "to_col": f[2]} for f in fks],
            "indexes": [{"name": i, "def": f"CREATE INDEX {i}"} for i in indexes],
        }
    return out


BASE = _catalog(
    orders=([("id", "integer", True), ("total", "numeric", False), ("customer_id", "integer", True)],
            ["id"], [("customer_id", "customers", "id")], ["orders_pkey"]),
    customers=([("id", "integer", True), ("name", "text", False)], ["id"], [], ["customers_pkey"]),
)


# ── diff / summary ────────────────────────────────────────────


def test_identical_catalogs_have_no_changes():
    d = sw.diff_catalogs(BASE, BASE)
    assert not sw.has_changes(d)
    assert sw.summarize(d) == "No schema changes."


def test_diff_detects_every_kind_of_change():
    curr = _catalog(
        orders=([("id", "bigint", True), ("total", "numeric", True), ("status", "text", False)],
                ["id"], [], ["orders_pkey", "orders_status_idx"]),
        invoices=([("id", "integer", True)], ["id"], [], []),
    )
    d = sw.diff_catalogs(BASE, curr)
    assert d["tables_added"] == ["invoices"]
    assert d["tables_removed"] == ["customers"]
    ch = d["tables_changed"]["orders"]
    assert [c["name"] for c in ch["columns_added"]] == ["status"]
    assert [c["name"] for c in ch["columns_removed"]] == ["customer_id"]
    changed = {c["name"]: c for c in ch["columns_changed"]}
    assert changed["id"]["from"]["type"] == "integer" and changed["id"]["to"]["type"] == "bigint"
    assert changed["total"]["from"]["nn"] is False and changed["total"]["to"]["nn"] is True
    assert ch["fks_removed"] == [{"col": "customer_id", "to_table": "customers", "to_col": "id"}]
    assert ch["indexes_added"] == ["orders_status_idx"]
    assert "pk_changed" not in ch

    s = sw.summarize(d)
    assert "+ table invoices" in s and "- table customers" in s
    assert "orders.id: type integer → bigint" in s
    assert "orders.total: NOT NULL added" in s
    assert "- FK customer_id → customers.id" in s


def test_summary_caps_the_item_list():
    curr = {f"t{i}": {"cols": [], "pks": [], "fks": [], "indexes": []} for i in range(12)}
    s = sw.summarize(sw.diff_catalogs({}, curr), max_items=3)
    assert s.count("+ table") == 3 and "and 9 more" in s


# ── storage ───────────────────────────────────────────────────


def test_snapshots_and_changes_roundtrip():
    assert sw.latest_snapshot("c1") is None
    sid = sw.save_snapshot("c1", BASE)
    latest = sw.latest_snapshot("c1")
    assert latest["id"] == sid and latest["table_count"] == 2 and latest["column_count"] == 5
    assert latest["catalog"] == BASE

    diff = sw.diff_catalogs(BASE, {})
    sw.record_change("c1", sid + 1, sid, diff, sw.summarize(diff))
    changes = sw.list_changes("c1")
    assert len(changes) == 1 and changes[0]["diff"]["tables_removed"] == ["customers", "orders"]
    assert sw.list_changes("other") == []


def test_prune_keeps_newest_snapshots():
    for i in range(5):
        sw.save_snapshot("c1", {"t": {"cols": [{"name": str(i), "type": "int", "nn": False}], "pks": [], "fks": [], "indexes": []}})
    removed = sw.prune_snapshots("c1", keep_last=2)
    assert removed == 3
    assert sw.latest_snapshot("c1")["catalog"]["t"]["cols"][0]["name"] == "4"


# ── run loop ──────────────────────────────────────────────────


class _Conn:
    id = "c1"
    name = "Prod"
    type = "postgres"


def test_run_watch_baseline_then_change_notifies(monkeypatch):
    catalogs = [BASE, BASE, _catalog(orders=([("id", "integer", True)], ["id"], [], []))]

    async def fake_fetch(conn, **kw):
        return catalogs.pop(0)

    sent: list[dict] = []

    class FakeSvc:
        def send(self, key, message, **kw):
            sent.append({"key": key, "message": message, **kw})
            return 1

    monkeypatch.setattr("tusk.core.catalog.fetch_catalog", fake_fetch)
    monkeypatch.setattr("tusk.core.connection.get_connection", lambda cid: _Conn() if cid == "c1" else None)
    monkeypatch.setattr("tusk.core.notifications.get_notification_service", lambda: FakeSvc())

    first = asyncio.run(sw.run_watch("c1"))
    assert first["first_run"] and not first["changed"] and sent == []

    same = asyncio.run(sw.run_watch("c1"))
    assert not same["changed"] and sent == [] and sw.list_changes("c1") == []

    third = asyncio.run(sw.run_watch("c1"))
    assert third["changed"] and "- table customers" in third["summary"]
    assert len(sent) == 1 and sent[0]["key"] == "schema.changed" and "Prod" in sent[0]["title"]
    assert sent[0]["context"]["tables_removed"] == ["customers"]
    assert len(sw.list_changes("c1")) == 1


def test_run_watch_rejects_unknown_or_non_postgres(monkeypatch):
    monkeypatch.setattr("tusk.core.connection.get_connection", lambda cid: None)
    with pytest.raises(ValueError):
        asyncio.run(sw.run_watch("nope"))

    class Duck:
        id = "d"
        name = "duck"
        type = "duckdb"

    monkeypatch.setattr("tusk.core.connection.get_connection", lambda cid: Duck())
    with pytest.raises(ValueError):
        asyncio.run(sw.run_watch("d"))


def test_scheduler_kind_is_wired():
    from tusk.core import scheduled_tasks as st

    assert st.JobKind.SCHEMA_WATCH in st._KIND_HANDLERS
    captured = {}
    st_add = st.add_job
    st.add_job = lambda spec: captured.update({"id": spec.id, "kind": spec.kind, "payload": spec.payload}) or spec.id
    try:
        st.add_schema_watch_schedule("c1", hour=7)
    finally:
        st.add_job = st_add
    assert captured["kind"] == st.JobKind.SCHEMA_WATCH and captured["payload"]["connection_id"] == "c1"
