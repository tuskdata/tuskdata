"""Data Contracts, layer 2: frozen schema contracts.

A contract is inferred, not written: "freeze" takes the current catalog of
a connection (all tables or a chosen subset) and stores it as the expected
schema. Every Schema Watch run then evaluates the live catalog against the
active contracts and raises ``contract.violated`` when reality drifts in a
way that breaks consumers.

What counts as a violation (a *breaking* change for whoever reads the data):

- an expected table is gone
- an expected column is gone
- a column's type changed
- a column's nullability changed (either direction — NOT NULL added breaks
  writers, dropped NOT NULL breaks readers that assumed a value)
- the primary key changed
- an expected foreign key is gone

Additions (new tables, new columns, new indexes) are never violations —
they are reported by Schema Watch as changes, nothing more.

Storage shares ``~/.tusk/schema_watch.db`` (see core/schema_watch.py).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from tusk.core.logging import get_logger
from tusk.core.schema_watch import _connect

log = get_logger(__name__)

EVENT_KEY = "contract.violated"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_tables() -> None:
    conn = _connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS contracts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id TEXT NOT NULL,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT,
                snapshot_id INTEGER,
                expected_json TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_contracts_conn ON contracts(connection_id, active);
            CREATE TABLE IF NOT EXISTS contract_violations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_id INTEGER NOT NULL,
                connection_id TEXT NOT NULL,
                detected_at TEXT NOT NULL,
                snapshot_id INTEGER,
                summary TEXT NOT NULL,
                violations_json TEXT NOT NULL,
                resolved_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_violations_contract ON contract_violations(contract_id, resolved_at);
            """
        )
        conn.commit()
    finally:
        conn.close()


# ── contracts ─────────────────────────────────────────────────


def _expected_subset(catalog: dict, tables: list[str] | None) -> dict:
    """The contract keeps columns / PK / FK of the chosen tables; indexes are
    a performance concern, not a contract one."""
    chosen = catalog if not tables else {t: catalog[t] for t in tables if t in catalog}
    return {
        name: {"cols": t.get("cols", []), "pks": t.get("pks", []), "fks": t.get("fks", [])}
        for name, t in chosen.items()
    }


def freeze_contract(
    connection_id: str,
    catalog: dict,
    *,
    name: str | None = None,
    tables: list[str] | None = None,
    created_by: str | None = None,
    snapshot_id: int | None = None,
    replace: bool = True,
) -> dict:
    """Store `catalog` (or a subset of its tables) as the expected schema.

    With `replace=True` (default) any active contract on the same
    connection is deactivated first: one connection, one current contract.
    """
    _ensure_tables()
    expected = _expected_subset(catalog, tables)
    if not expected:
        raise ValueError("nothing to freeze: no matching tables in the catalog")
    name = (name or "").strip() or f"{'all tables' if not tables else f'{len(expected)} tables'} · {_now()[:10]}"
    conn = _connect()
    try:
        if replace:
            conn.execute("UPDATE contracts SET active = 0 WHERE connection_id = ? AND active = 1", (connection_id,))
        cur = conn.execute(
            "INSERT INTO contracts (connection_id, name, created_at, created_by, snapshot_id, expected_json, active) VALUES (?, ?, ?, ?, ?, ?, 1)",
            (connection_id, name, _now(), created_by, snapshot_id, json.dumps(expected, separators=(",", ":"))),
        )
        conn.commit()
        cid = int(cur.lastrowid)
    finally:
        conn.close()
    log.info("contract_frozen", connection_id=connection_id, contract_id=cid, tables=len(expected))
    return get_contract(cid)


def _row_to_contract(r, with_expected: bool = True) -> dict:
    out = {
        "id": r["id"],
        "connection_id": r["connection_id"],
        "name": r["name"],
        "created_at": r["created_at"],
        "created_by": r["created_by"],
        "snapshot_id": r["snapshot_id"],
        "active": bool(r["active"]),
        "table_count": len(json.loads(r["expected_json"])),
    }
    if with_expected:
        out["expected"] = json.loads(r["expected_json"])
    return out


def get_contract(contract_id: int) -> dict | None:
    _ensure_tables()
    conn = _connect()
    try:
        r = conn.execute("SELECT * FROM contracts WHERE id = ?", (contract_id,)).fetchone()
        return _row_to_contract(r) if r else None
    finally:
        conn.close()


def active_contract(connection_id: str) -> dict | None:
    _ensure_tables()
    conn = _connect()
    try:
        r = conn.execute(
            "SELECT * FROM contracts WHERE connection_id = ? AND active = 1 ORDER BY id DESC LIMIT 1", (connection_id,)
        ).fetchone()
        return _row_to_contract(r) if r else None
    finally:
        conn.close()


def list_contracts(connection_id: str | None = None, include_inactive: bool = False) -> list[dict]:
    _ensure_tables()
    conn = _connect()
    try:
        sql, params = "SELECT * FROM contracts", []
        clauses = []
        if connection_id:
            clauses.append("connection_id = ?")
            params.append(connection_id)
        if not include_inactive:
            clauses.append("active = 1")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        rows = conn.execute(sql + " ORDER BY id DESC", params).fetchall()
        return [_row_to_contract(r, with_expected=False) for r in rows]
    finally:
        conn.close()


def deactivate_contract(contract_id: int) -> bool:
    _ensure_tables()
    conn = _connect()
    try:
        cur = conn.execute("UPDATE contracts SET active = 0 WHERE id = ? AND active = 1", (contract_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


# ── evaluation ────────────────────────────────────────────────


def evaluate(expected: dict, catalog: dict) -> list[dict]:
    """Compare the expected schema with the live catalog. Returns a list of
    violations, each ``{"table", "kind", "detail"}``; empty means the
    contract holds."""
    out: list[dict] = []
    for tname, exp in expected.items():
        live = catalog.get(tname)
        if live is None:
            out.append({"table": tname, "kind": "table_missing", "detail": "table no longer exists"})
            continue
        live_cols = {c["name"]: c for c in live.get("cols", [])}
        for col in exp.get("cols", []):
            lc = live_cols.get(col["name"])
            if lc is None:
                out.append({"table": tname, "kind": "column_missing", "column": col["name"], "detail": "column no longer exists"})
                continue
            if lc.get("type") != col.get("type"):
                out.append({"table": tname, "kind": "type_changed", "column": col["name"],
                            "detail": f"type {col.get('type')} → {lc.get('type')}"})
            if bool(lc.get("nn")) != bool(col.get("nn")):
                out.append({"table": tname, "kind": "nullability_changed", "column": col["name"],
                            "detail": "NOT NULL added" if lc.get("nn") else "NOT NULL dropped"})
        if list(exp.get("pks", [])) != list(live.get("pks", [])):
            out.append({"table": tname, "kind": "pk_changed",
                        "detail": f"primary key {exp.get('pks', [])} → {live.get('pks', [])}"})
        live_fks = {(f["col"], f["to_table"], f["to_col"]) for f in live.get("fks", [])}
        for f in exp.get("fks", []):
            if (f["col"], f["to_table"], f["to_col"]) not in live_fks:
                out.append({"table": tname, "kind": "fk_missing", "column": f["col"],
                            "detail": f"FK {f['col']} → {f['to_table']}.{f['to_col']} is gone"})
    return out


def summarize(violations: list[dict], max_items: int = 8) -> str:
    if not violations:
        return "Contract holds."
    parts = []
    for v in violations:
        where = v["table"] + (f".{v['column']}" if v.get("column") else "")
        parts.append(f"{where}: {v['detail']}")
    shown = parts[:max_items]
    more = len(parts) - len(shown)
    return "; ".join(shown) + (f"; … and {more} more" if more > 0 else "")


# ── violations log ────────────────────────────────────────────


def open_violation(contract_id: int) -> dict | None:
    conn = _connect()
    try:
        r = conn.execute(
            "SELECT * FROM contract_violations WHERE contract_id = ? AND resolved_at IS NULL ORDER BY id DESC LIMIT 1",
            (contract_id,),
        ).fetchone()
        return _violation_row(r) if r else None
    finally:
        conn.close()


def _violation_row(r) -> dict:
    return {
        "id": r["id"],
        "contract_id": r["contract_id"],
        "connection_id": r["connection_id"],
        "detected_at": r["detected_at"],
        "snapshot_id": r["snapshot_id"],
        "summary": r["summary"],
        "violations": json.loads(r["violations_json"]),
        "resolved_at": r["resolved_at"],
    }


def list_violations(contract_id: int, limit: int = 50) -> list[dict]:
    _ensure_tables()
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT * FROM contract_violations WHERE contract_id = ? ORDER BY id DESC LIMIT ?", (contract_id, limit)
        ).fetchall()
        return [_violation_row(r) for r in rows]
    finally:
        conn.close()


def check_contracts(connection_id: str, catalog: dict, *, snapshot_id: int | None = None, notify: bool = True) -> list[dict]:
    """Evaluate the active contract of `connection_id` against `catalog`.

    Records a violation when the contract is broken and it's a *new*
    breakage (different from the currently open one), resolves the open
    violation when the contract holds again, and notifies on both.
    Returns the current violations (empty when the contract holds or
    there is no contract).
    """
    contract = active_contract(connection_id)
    if not contract:
        return []
    violations = evaluate(contract["expected"], catalog)
    summary = summarize(violations)
    current = open_violation(contract["id"])
    conn_name = _connection_name(connection_id)

    if violations:
        if current and current["violations"] == violations:
            return violations  # same breakage as before, already reported
        conn = _connect()
        try:
            if current:
                conn.execute("UPDATE contract_violations SET resolved_at = ? WHERE id = ?", (_now(), current["id"]))
            conn.execute(
                "INSERT INTO contract_violations (contract_id, connection_id, detected_at, snapshot_id, summary, violations_json) VALUES (?, ?, ?, ?, ?, ?)",
                (contract["id"], connection_id, _now(), snapshot_id, summary, json.dumps(violations, separators=(",", ":"))),
            )
            conn.commit()
        finally:
            conn.close()
        log.warning("contract_violated", connection_id=connection_id, contract_id=contract["id"], summary=summary)
        if notify:
            _notify(EVENT_KEY, f"Contract violated: {conn_name}", summary, "warning", connection_id, contract, violations)
        return violations

    if current:
        conn = _connect()
        try:
            conn.execute("UPDATE contract_violations SET resolved_at = ? WHERE id = ?", (_now(), current["id"]))
            conn.commit()
        finally:
            conn.close()
        log.info("contract_restored", connection_id=connection_id, contract_id=contract["id"])
        if notify:
            _notify("contract.restored", f"Contract holds again: {conn_name}", "All expected tables and columns are back.",
                    "success", connection_id, contract, [])
    return []


def _connection_name(connection_id: str) -> str:
    try:
        from tusk.core.connection import get_connection

        c = get_connection(connection_id)
        return c.name if c else connection_id
    except Exception:  # noqa: BLE001
        return connection_id


def _notify(event_key: str, title: str, message: str, variant: str, connection_id: str, contract: dict, violations: list[dict]) -> None:
    try:
        from tusk.core.notifications import get_notification_service

        get_notification_service().send(
            event_key,
            message,
            title=title,
            icon="file-check-2",
            variant=variant,
            link=f"/schema?connection={connection_id}",
            context={
                "connection_id": connection_id,
                "contract_id": contract["id"],
                "contract": contract["name"],
                "violations": violations,
            },
        )
    except Exception as e:  # noqa: BLE001 — never break the watch
        log.warning("contract_notify_failed", error=str(e))


# ── export ────────────────────────────────────────────────────


def to_yaml(contract: dict) -> str:
    """A readable YAML rendering of the contract (no PyYAML dependency;
    the shape is simple enough to emit by hand)."""
    lines = [
        "# Tusk data contract — generated, edit at your own risk",
        f"contract: {_q(contract['name'])}",
        f"connection_id: {_q(contract['connection_id'])}",
        f"frozen_at: {_q(contract['created_at'])}",
        "tables:",
    ]
    for tname, t in contract.get("expected", {}).items():
        lines.append(f"  {_q(tname)}:")
        if t.get("pks"):
            lines.append(f"    primary_key: [{', '.join(_q(c) for c in t['pks'])}]")
        lines.append("    columns:")
        for c in t.get("cols", []):
            lines.append(f"      - name: {_q(c['name'])}")
            lines.append(f"        type: {_q(c.get('type', ''))}")
            lines.append(f"        nullable: {'false' if c.get('nn') else 'true'}")
        if t.get("fks"):
            lines.append("    foreign_keys:")
            for f in t["fks"]:
                lines.append(f"      - column: {_q(f['col'])}")
                lines.append(f"        references: {_q(f['to_table'] + '.' + f['to_col'])}")
    return "\n".join(lines) + "\n"


def _q(value: str) -> str:
    s = str(value)
    return json.dumps(s) if any(ch in s for ch in ":#'\"{}[],&*!|>%@`\n") or s.strip() != s or s == "" else s
