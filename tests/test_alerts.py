"""Alert rules: storage, the state machine (ok → pending → firing → resolved), errors."""

from __future__ import annotations

import asyncio

import pytest

from tusk.core import alerts


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setattr(alerts, "DB_PATH", tmp_path / "alerts.db")
    sent: list[tuple] = []
    monkeypatch.setattr(alerts, "_notify", lambda event, rule, value, variant: sent.append((event, rule.id, value)))
    return sent


def _rule(**kw):
    base = dict(name="cpu", source_kind="metric", source_ref="connections_pct", op="gt", threshold=80, connection_id="c1")
    base.update(kw)
    return alerts.create_rule(**base)


def test_crud_and_validation():
    r = _rule()
    assert alerts.get_rule(r.id).name == "cpu"
    assert alerts.list_rules()[0].condition == "> 80%"
    alerts.update_rule(r.id, threshold=90, enabled=False)
    assert alerts.get_rule(r.id).threshold == 90 and not alerts.get_rule(r.id).enabled
    assert alerts.delete_rule(r.id) and alerts.get_rule(r.id) is None
    with pytest.raises(ValueError):
        _rule(op="between")
    with pytest.raises(ValueError):
        _rule(source_ref="nope")
    with pytest.raises(ValueError):
        _rule(connection_id=None)


def test_fires_once_and_resolves(isolated_db):
    r = _rule()
    values = iter([50, 95, 97, 60])

    async def ev(rule):
        return next(values)

    run = lambda: asyncio.run(alerts.check_rule(alerts.get_rule(r.id), evaluator=ev))  # noqa: E731
    assert run()["transition"] == "none"
    assert run()["transition"] == "fired"
    assert run()["transition"] == "none"        # still firing, no repeat
    assert run()["transition"] == "resolved"
    assert [e for e, _, _ in isolated_db] == ["alert.fired", "alert.resolved"]
    assert alerts.get_rule(r.id).state == "ok"


def test_duration_must_hold(isolated_db, monkeypatch):
    r = _rule(for_seconds=120)
    clock = [1000.0]
    monkeypatch.setattr(alerts.time, "time", lambda: clock[0])

    async def ev(rule):
        return 99

    first = asyncio.run(alerts.check_rule(alerts.get_rule(r.id), evaluator=ev))
    assert first["transition"] == "none" and first["state"] == "pending"
    clock[0] += 60
    assert asyncio.run(alerts.check_rule(alerts.get_rule(r.id), evaluator=ev))["transition"] == "none"
    clock[0] += 61
    assert asyncio.run(alerts.check_rule(alerts.get_rule(r.id), evaluator=ev))["transition"] == "fired"
    assert len(isolated_db) == 1


def test_error_is_recorded_not_paged(isolated_db):
    r = _rule()

    async def ev(rule):
        raise ValueError("connection refused")

    out = asyncio.run(alerts.check_rule(alerts.get_rule(r.id), evaluator=ev))
    assert out["transition"] == "error"
    assert alerts.get_rule(r.id).state == "error" and "refused" in alerts.get_rule(r.id).last_error
    assert isolated_db == []


def test_first_number_picks_first_numeric_cell():
    assert alerts._first_number(["a", "b"], [("x", 3)]) == 3.0
    assert alerts._first_number([], []) == 0.0
    with pytest.raises(ValueError):
        alerts._first_number(["a"], [("x",)])
