"""The Copilot's verification loop: dry run, join check, one correction."""

from __future__ import annotations

import asyncio

import pytest

from tusk.studio.routes import ai as ai_mod
from tusk.studio.routes.ai import SQLResponse, _verify_sql

CATALOG = {
    "orders": {"cols": [{"name": "id", "type": "integer"}, {"name": "customer_id", "type": "integer"}], "pks": ["id"],
               "fks": [{"col": "customer_id", "to_table": "customers", "to_col": "id"}]},
    "customers": {"cols": [{"name": "id", "type": "integer"}, {"name": "name", "type": "text"}], "pks": ["id"], "fks": []},
    "products": {"cols": [{"name": "id", "type": "integer"}, {"name": "name", "type": "text"}], "pks": ["id"], "fks": []},
    "order_items": {"cols": [{"name": "order_id", "type": "integer"}, {"name": "product_id", "type": "integer"}], "pks": [],
                    "fks": [{"col": "order_id", "to_table": "orders", "to_col": "id"}, {"col": "product_id", "to_table": "products", "to_col": "id"}]},
}
BAD = "SELECT p.name FROM orders o JOIN products p ON o.id = p.id"
GOOD = "SELECT p.name FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON oi.product_id = p.id"


@pytest.fixture
def harness(monkeypatch):
    calls: list[str] = []
    answers: list[SQLResponse] = []

    async def fake_complete(provider, prompt, struct, **kw):
        calls.append(prompt)
        return answers.pop(0)

    async def fake_dry_run(connection_id, sql):
        return (False, 'column o.nope does not exist') if "nope" in sql else (True, None)

    async def fake_catalog(connection_id):
        return CATALOG

    monkeypatch.setattr(ai_mod, "complete_struct", fake_complete)
    monkeypatch.setattr(ai_mod, "_dry_run", fake_dry_run)
    monkeypatch.setattr(ai_mod, "_catalog_for", fake_catalog)
    return calls, answers


def _run(first: SQLResponse):
    return asyncio.run(_verify_sql(None, "sys", "### Detailed schema\n...", "conn", first))


def test_fk_backed_join_passes_untouched(harness):
    calls, answers = harness
    out = _run(SQLResponse(sql=GOOD, explanation="ok", confidence="high"))
    assert out["sql"] == GOOD and out["confidence"] == "high" and out["join_warnings"] == 0
    assert [j["status"] for j in out["joins"]] == ["fk", "fk"]
    assert calls == []  # no correction round


def test_invented_join_gets_one_correction_with_the_fk_list(harness):
    calls, answers = harness
    answers.append(SQLResponse(sql=GOOD, explanation="through order_items", confidence="high"))
    out = _run(SQLResponse(sql=BAD, explanation="direct", confidence="high"))
    assert out["sql"] == GOOD and out["join_warnings"] == 0 and out["confidence"] == "high"
    assert len(calls) == 1
    assert "### Join check" in calls[0]
    assert "order_items.product_id -> products.id" in calls[0]
    assert "o.id = p.id" in calls[0]


def test_correction_that_is_no_better_is_dropped_and_confidence_capped(harness):
    calls, answers = harness
    answers.append(SQLResponse(sql=BAD, explanation="still direct", confidence="high"))
    out = _run(SQLResponse(sql=BAD, explanation="direct", confidence="high"))
    assert out["sql"] == BAD
    assert out["join_warnings"] == 1 and out["joins"][0]["status"] == "no_fk"
    assert out["confidence"] == "medium"  # high is capped, the card names the join


def test_type_mismatch_forces_low(harness):
    calls, answers = harness
    answers.append(SQLResponse(sql="SELECT 1 FROM orders o JOIN customers c ON o.id = c.name", explanation="", confidence="high"))
    out = _run(SQLResponse(sql="SELECT 1 FROM orders o JOIN customers c ON o.id = c.name", explanation="", confidence="high"))
    assert out["joins"][0]["status"] == "type_mismatch" and out["confidence"] == "low"


def test_only_one_correction_per_request(harness):
    # Dry run fails first → schema correction happens; the corrected SQL has
    # a join without FK, but a second correction is NOT attempted.
    calls, answers = harness
    answers.append(SQLResponse(sql=BAD, explanation="fixed column", confidence="high"))
    out = _run(SQLResponse(sql="SELECT o.nope FROM orders o", explanation="", confidence="high"))
    assert out["sql"] == BAD and out["verified"] is True
    assert len(calls) == 1 and "### Correction needed" in calls[0]
    assert out["join_warnings"] == 1 and out["confidence"] == "medium"


def test_unverifiable_sql_skips_the_join_check(harness, monkeypatch):
    calls, answers = harness

    async def none_dry_run(connection_id, sql):
        return None, None

    monkeypatch.setattr(ai_mod, "_dry_run", none_dry_run)
    out = _run(SQLResponse(sql=BAD, explanation="", confidence="medium"))
    assert out["joins"] == [] and out["join_warnings"] == 0 and calls == []
