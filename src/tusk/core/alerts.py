"""Alert rules: *when <value> <op> <threshold> [for <duration>] → notify*.

A rule watches one number:

* a **saved query** (first numeric cell of the first row), run on its
  connection;
* a **dashboard widget** (Analytics), through the BI engine;
* an **Admin metric** of a PostgreSQL connection (connections used %,
  active queries, cache hit ratio, database size, longest running query).

The scheduler evaluates every enabled rule once a minute. When the
condition holds for at least ``for_seconds`` the rule goes to ``firing``
and an ``alert.fired`` notification goes out through whatever channels are
subscribed to it; when it stops holding, ``alert.resolved``. Errors while
evaluating are recorded on the rule (state ``error``) and do not page.

Rules live in ``tusk.db`` (``alert_rules``).
"""

from __future__ import annotations

import asyncio
import operator
import time
import uuid
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from tusk.core import meta
from tusk.core.logging import get_logger

log = get_logger("alerts")

DB_PATH: Path = meta.TUSK_DB

OPS: dict[str, Callable[[float, float], bool]] = {
    "gt": operator.gt,
    "gte": operator.ge,
    "lt": operator.lt,
    "lte": operator.le,
    "eq": operator.eq,
    "ne": operator.ne,
}
OP_LABELS = {"gt": ">", "gte": "≥", "lt": "<", "lte": "≤", "eq": "=", "ne": "≠"}

SOURCE_KINDS = ("query", "widget", "metric")

# Admin metrics a rule can watch on a PostgreSQL connection.
METRICS: dict[str, tuple[str, str]] = {
    "connections_pct": ("Connections used", "%"),
    "active_queries": ("Active queries", ""),
    "cache_hit_ratio": ("Cache hit ratio", "%"),
    "db_size_gb": ("Database size", "GB"),
    "longest_query_s": ("Longest running query", "s"),
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS alert_rules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    connection_id TEXT,
    op TEXT NOT NULL,
    threshold REAL NOT NULL,
    for_seconds INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'ok',
    last_value REAL,
    last_checked_at REAL,
    breached_since REAL,
    last_fired_at REAL,
    last_error TEXT,
    owner_id TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL
);
"""


@dataclass
class AlertRule:
    id: str
    name: str
    source_kind: str
    source_ref: str
    connection_id: str | None
    op: str
    threshold: float
    for_seconds: int = 0
    enabled: bool = True
    state: str = "ok"
    last_value: float | None = None
    last_checked_at: float | None = None
    breached_since: float | None = None
    last_fired_at: float | None = None
    last_error: str | None = None
    owner_id: str = ""
    created_at: float = field(default_factory=time.time)

    @property
    def condition(self) -> str:
        unit = METRICS.get(self.source_ref, ("", ""))[1] if self.source_kind == "metric" else ""
        return f"{OP_LABELS.get(self.op, self.op)} {self.threshold:g}{unit}"

    @property
    def source_label(self) -> str:
        if self.source_kind == "metric":
            return METRICS.get(self.source_ref, (self.source_ref, ""))[0]
        return f"{self.source_kind} #{self.source_ref}"

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["condition"] = self.condition
        d["source_label"] = self.source_label
        return d


# ── Storage ─────────────────────────────────────────────────────────────


def _connect():
    conn = meta.connect(DB_PATH)
    conn.executescript(_SCHEMA)
    return conn


def list_rules(owner_id: str | None = None) -> list[AlertRule]:
    conn = _connect()
    try:
        conn.row_factory = _dict_factory
        sql = "SELECT * FROM alert_rules"
        args: tuple = ()
        if owner_id:
            sql += " WHERE owner_id = ?"
            args = (owner_id,)
        rows = conn.execute(sql + " ORDER BY name", args).fetchall()
        return [_from_dict(r) for r in rows]
    finally:
        conn.close()


def get_rule(rule_id: str) -> AlertRule | None:
    conn = _connect()
    try:
        conn.row_factory = _dict_factory
        row = conn.execute("SELECT * FROM alert_rules WHERE id = ?", (rule_id,)).fetchone()
        return _from_dict(row) if row else None
    finally:
        conn.close()


def _dict_factory(cursor, row):
    return {d[0]: row[i] for i, d in enumerate(cursor.description)}


def _from_dict(d: dict) -> AlertRule:
    d = dict(d)
    d["enabled"] = bool(d["enabled"])
    return AlertRule(**d)


def create_rule(
    *,
    name: str,
    source_kind: str,
    source_ref: str,
    op: str,
    threshold: float,
    connection_id: str | None = None,
    for_seconds: int = 0,
    enabled: bool = True,
    owner_id: str = "",
) -> AlertRule:
    name = (name or "").strip()
    if not name:
        raise ValueError("name is required")
    if source_kind not in SOURCE_KINDS:
        raise ValueError(f"source_kind must be one of {', '.join(SOURCE_KINDS)}")
    if op not in OPS:
        raise ValueError(f"op must be one of {', '.join(OPS)}")
    if source_kind == "metric":
        if source_ref not in METRICS:
            raise ValueError(f"unknown metric '{source_ref}'")
        if not connection_id:
            raise ValueError("metric rules need a PostgreSQL connection")
    if not str(source_ref).strip():
        raise ValueError("source_ref is required")
    rule = AlertRule(
        id=uuid.uuid4().hex[:12],
        name=name,
        source_kind=source_kind,
        source_ref=str(source_ref).strip(),
        connection_id=connection_id or None,
        op=op,
        threshold=float(threshold),
        for_seconds=max(0, int(for_seconds or 0)),
        enabled=bool(enabled),
        owner_id=owner_id or "",
    )
    conn = _connect()
    try:
        conn.execute(
            """INSERT INTO alert_rules (id, name, source_kind, source_ref, connection_id, op, threshold,
                                       for_seconds, enabled, state, owner_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ok', ?, ?)""",
            (rule.id, rule.name, rule.source_kind, rule.source_ref, rule.connection_id, rule.op,
             rule.threshold, rule.for_seconds, int(rule.enabled), rule.owner_id, rule.created_at),
        )
        conn.commit()
    finally:
        conn.close()
    return rule


def update_rule(rule_id: str, **fields: Any) -> AlertRule | None:
    allowed = {"name", "op", "threshold", "for_seconds", "enabled", "source_ref", "connection_id"}
    sets = {k: v for k, v in fields.items() if k in allowed and v is not None}
    if not sets:
        return get_rule(rule_id)
    if "op" in sets and sets["op"] not in OPS:
        raise ValueError("invalid op")
    if "enabled" in sets:
        sets["enabled"] = int(bool(sets["enabled"]))
    if "threshold" in sets:
        sets["threshold"] = float(sets["threshold"])
    if "for_seconds" in sets:
        sets["for_seconds"] = max(0, int(sets["for_seconds"]))
    conn = _connect()
    try:
        assignments = ", ".join(f"{k} = ?" for k in sets)
        conn.execute(f"UPDATE alert_rules SET {assignments} WHERE id = ?", (*sets.values(), rule_id))
        # Re-arm when the rule is edited or paused: old breach timing is meaningless now.
        conn.execute("UPDATE alert_rules SET breached_since = NULL WHERE id = ?", (rule_id,))
        conn.commit()
    finally:
        conn.close()
    return get_rule(rule_id)


def delete_rule(rule_id: str) -> bool:
    conn = _connect()
    try:
        cur = conn.execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def _save_state(rule: AlertRule) -> None:
    conn = _connect()
    try:
        conn.execute(
            """UPDATE alert_rules SET state = ?, last_value = ?, last_checked_at = ?, breached_since = ?,
                                     last_fired_at = ?, last_error = ? WHERE id = ?""",
            (rule.state, rule.last_value, rule.last_checked_at, rule.breached_since,
             rule.last_fired_at, rule.last_error, rule.id),
        )
        conn.commit()
    finally:
        conn.close()


# ── Evaluation ──────────────────────────────────────────────────────────


def _first_number(columns: list, rows: list) -> float:
    """First numeric cell of the first row; a query with no rows counts as 0."""
    if not rows:
        return 0.0
    for cell in rows[0]:
        if isinstance(cell, bool):
            continue
        if isinstance(cell, (int, float, Decimal)):
            return float(cell)
        if isinstance(cell, str):
            try:
                return float(cell)
            except ValueError:
                continue
    raise ValueError("the first row has no numeric column")


async def _value_from_query(rule: AlertRule) -> float:
    from tusk.core.connection import get_connection
    from tusk.core.history import QueryHistory
    from tusk.studio.routes.mcp_tools import _run_read_query, is_read_only_sql

    saved = QueryHistory().get_saved_query(int(rule.source_ref))
    if not saved:
        raise ValueError(f"saved query #{rule.source_ref} no longer exists")
    conn = get_connection(rule.connection_id or saved.connection_id or "")
    if not conn:
        raise ValueError("the query's connection no longer exists")
    ok, reason = is_read_only_sql(saved.sql)
    if not ok:
        raise ValueError(f"only read-only queries can drive an alert: {reason}")
    out = await _run_read_query(conn, saved.sql, 1)
    if out.get("error"):
        raise ValueError(out["error"])
    return _first_number(out["columns"], out["rows"])


async def _value_from_widget(rule: AlertRule) -> float:
    from tusk.bi.db import get_widget
    from tusk.bi.routes.api import _engine

    widget = get_widget(int(rule.source_ref))
    if not widget or not widget.get("query_sql"):
        raise ValueError(f"widget #{rule.source_ref} has no query")
    from tusk.bi.db import get_saved_query

    query = get_saved_query(widget["query_id"])
    if not query:
        raise ValueError("the widget's query no longer exists")
    result = await asyncio.to_thread(
        _engine.execute,
        source_type=query["source_type"],
        connection_ref=query["connection_ref"],
        sql=query["sql"],
        limit=1,
        cache_ttl=0,
    )
    if result.get("error"):
        raise ValueError(result["error"])
    rows = result.get("rows") or []
    cols = result.get("columns") or []
    rows = [tuple(r.values()) if isinstance(r, dict) else tuple(r) for r in rows]
    return _first_number(cols, rows)


async def _value_from_metric(rule: AlertRule) -> float:
    from tusk.admin.processes import get_active_queries
    from tusk.admin.stats import get_server_stats
    from tusk.core.connection import get_connection

    conn = get_connection(rule.connection_id or "")
    if not conn or conn.type != "postgres":
        raise ValueError("metric rules need a PostgreSQL connection")
    key = rule.source_ref
    if key == "longest_query_s":
        procs = await get_active_queries(conn)
        if isinstance(procs, dict):
            raise ValueError(procs.get("error", "could not read pg_stat_activity"))
        return float(max((p.duration_seconds for p in procs), default=0))
    stats = await get_server_stats(conn)
    if isinstance(stats, dict):
        raise ValueError(stats.get("error", "could not read server stats"))
    if key == "connections_pct":
        return 100.0 * stats.connections / max(1, stats.max_connections)
    if key == "active_queries":
        return float(stats.active_queries)
    if key == "cache_hit_ratio":
        return float(stats.cache_hit_ratio)
    if key == "db_size_gb":
        return stats.db_size_bytes / (1024**3)
    raise ValueError(f"unknown metric '{key}'")


async def evaluate_rule(rule: AlertRule) -> float:
    """Current value of the rule's source. Raises ValueError with a reason."""
    if rule.source_kind == "query":
        return await _value_from_query(rule)
    if rule.source_kind == "widget":
        return await _value_from_widget(rule)
    if rule.source_kind == "metric":
        return await _value_from_metric(rule)
    raise ValueError(f"unknown source kind '{rule.source_kind}'")


def _notify(event_key: str, rule: AlertRule, value: float, variant: str) -> None:
    from tusk.core.notifications import get_notification_service

    verb = "fired" if event_key == "alert.fired" else "resolved"
    message = f"{rule.name}: {rule.source_label} is {value:g} ({rule.condition})"
    if verb == "resolved":
        message = f"{rule.name}: back to normal, {rule.source_label} is {value:g} (was {rule.condition})"
    get_notification_service().send(
        event_key,
        message,
        title=f"Alert {verb}: {rule.name}",
        icon="bell-ring" if verb == "fired" else "bell-off",
        variant=variant,
        link="/notifications/settings#alerts",
        context={"rule_id": rule.id, "value": value, "threshold": rule.threshold, "op": rule.op,
                 "source_kind": rule.source_kind, "source_ref": rule.source_ref},
        rate_key=f"{event_key}:{rule.id}",
    )


async def check_rule(rule: AlertRule, *, notify: bool = True, evaluator=None) -> dict:
    """Evaluate one rule and apply the state machine. Returns what happened."""
    now = time.time()
    rule.last_checked_at = now
    try:
        value = await (evaluator or evaluate_rule)(rule)
    except Exception as exc:  # noqa: BLE001 — a broken rule must not stop the others
        rule.last_error = str(exc)[:500]
        rule.state = "error"
        rule.breached_since = None
        _save_state(rule)
        log.warning("alert_rule_error", rule=rule.name, error=str(exc))
        return {"rule_id": rule.id, "transition": "error", "error": rule.last_error}

    rule.last_error = None
    rule.last_value = value
    breached = OPS[rule.op](value, rule.threshold)
    transition = "none"
    if breached:
        if rule.breached_since is None:
            rule.breached_since = now
        held_for = now - rule.breached_since
        if rule.state != "firing" and held_for >= rule.for_seconds:
            rule.state = "firing"
            rule.last_fired_at = now
            transition = "fired"
            if notify:
                _notify("alert.fired", rule, value, "error")
        elif rule.state != "firing":
            rule.state = "pending"
    else:
        rule.breached_since = None
        if rule.state == "firing":
            transition = "resolved"
            if notify:
                _notify("alert.resolved", rule, value, "success")
        rule.state = "ok"
    _save_state(rule)
    return {"rule_id": rule.id, "transition": transition, "value": value, "state": rule.state}


async def check_all(*, notify: bool = True) -> list[dict]:
    """Evaluate every enabled rule. Called by the scheduler once a minute."""
    results = []
    for rule in list_rules():
        if not rule.enabled:
            continue
        results.append(await check_rule(rule, notify=notify))
    fired = [r for r in results if r.get("transition") in ("fired", "resolved")]
    if fired:
        log.info("alerts_checked", rules=len(results), transitions=len(fired))
    return results


def rules_summary() -> dict[str, int]:
    rules = list_rules()
    return {
        "total": len(rules),
        "firing": sum(1 for r in rules if r.state == "firing"),
        "error": sum(1 for r in rules if r.state == "error"),
    }

