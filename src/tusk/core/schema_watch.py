"""Schema Watch: snapshot a connection's catalog, diff it against the
previous snapshot, keep the history, and raise ``schema.changed``.

This is the engine behind Data Contracts: a contract is just "the schema
I expect", and Schema Watch is what notices when reality drifts. Layer 1
here only reports what changed; the contract layer (0.4.33) decides
whether a change is a violation.

Storage: ``~/.tusk/schema_watch.db``. Snapshots are the full catalog as
JSON (a few hundred KB for a large database — cheap), changes are the
structured diff plus a one-paragraph summary.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tusk.core.config import TUSK_DIR
from tusk.core.logging import get_logger

log = get_logger(__name__)

DB_PATH: Path = TUSK_DIR / "schema_watch.db"
EVENT_KEY = "schema.changed"


# ── storage ───────────────────────────────────────────────────


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            connection_id TEXT NOT NULL,
            taken_at TEXT NOT NULL,
            table_count INTEGER NOT NULL,
            column_count INTEGER NOT NULL,
            catalog_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_snapshots_conn ON snapshots(connection_id, id);
        CREATE TABLE IF NOT EXISTS changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            connection_id TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            snapshot_id INTEGER NOT NULL,
            prev_snapshot_id INTEGER,
            summary TEXT NOT NULL,
            diff_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_changes_conn ON changes(connection_id, id);
        """
    )
    return conn


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_snapshot(connection_id: str, catalog: dict) -> int:
    conn = _connect()
    try:
        cur = conn.execute(
            "INSERT INTO snapshots (connection_id, taken_at, table_count, column_count, catalog_json) VALUES (?, ?, ?, ?, ?)",
            (
                connection_id,
                _now(),
                len(catalog),
                sum(len(t.get("cols", [])) for t in catalog.values()),
                json.dumps(catalog, separators=(",", ":")),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def latest_snapshot(connection_id: str) -> dict | None:
    """{"id", "taken_at", "table_count", "column_count", "catalog"} or None."""
    conn = _connect()
    try:
        r = conn.execute(
            "SELECT * FROM snapshots WHERE connection_id = ? ORDER BY id DESC LIMIT 1", (connection_id,)
        ).fetchone()
        if not r:
            return None
        return {
            "id": r["id"],
            "taken_at": r["taken_at"],
            "table_count": r["table_count"],
            "column_count": r["column_count"],
            "catalog": json.loads(r["catalog_json"]),
        }
    finally:
        conn.close()


def prune_snapshots(connection_id: str, keep_last: int = 30) -> int:
    """Keep the newest `keep_last` snapshots per connection. Changes keep
    their own copy of the diff, so pruning never loses history."""
    conn = _connect()
    try:
        cur = conn.execute(
            """DELETE FROM snapshots WHERE connection_id = ? AND id NOT IN (
                   SELECT id FROM snapshots WHERE connection_id = ? ORDER BY id DESC LIMIT ?
               )""",
            (connection_id, connection_id, keep_last),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def record_change(connection_id: str, snapshot_id: int, prev_snapshot_id: int | None, diff: dict, summary: str) -> int:
    conn = _connect()
    try:
        cur = conn.execute(
            "INSERT INTO changes (connection_id, detected_at, snapshot_id, prev_snapshot_id, summary, diff_json) VALUES (?, ?, ?, ?, ?, ?)",
            (connection_id, _now(), snapshot_id, prev_snapshot_id, summary, json.dumps(diff, separators=(",", ":"))),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def list_changes(connection_id: str, *, since: datetime | None = None, limit: int = 50) -> list[dict]:
    conn = _connect()
    try:
        sql = "SELECT id, detected_at, snapshot_id, prev_snapshot_id, summary, diff_json FROM changes WHERE connection_id = ?"
        params: list = [connection_id]
        if since is not None:
            sql += " AND detected_at >= ?"
            params.append(since.isoformat())
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return [
            {
                "id": r["id"],
                "detected_at": r["detected_at"],
                "snapshot_id": r["snapshot_id"],
                "prev_snapshot_id": r["prev_snapshot_id"],
                "summary": r["summary"],
                "diff": json.loads(r["diff_json"]),
            }
            for r in rows
        ]
    finally:
        conn.close()


# ── diff ──────────────────────────────────────────────────────


def diff_catalogs(prev: dict, curr: dict) -> dict:
    """Structural diff between two catalogs (see core/catalog.py shape).

    Returns::

        {
          "tables_added": [...], "tables_removed": [...],
          "tables_changed": {
             "orders": {
                "columns_added": [{"name","type","nn"}],
                "columns_removed": [...],
                "columns_changed": [{"name", "from": {...}, "to": {...}}],
                "pk_changed": {"from": [...], "to": [...]} | None,
                "fks_added": [...], "fks_removed": [...],
                "indexes_added": ["name"], "indexes_removed": ["name"],
             }
          }
        }
    """
    prev_names, curr_names = set(prev), set(curr)
    out: dict = {
        "tables_added": sorted(curr_names - prev_names),
        "tables_removed": sorted(prev_names - curr_names),
        "tables_changed": {},
    }
    for name in sorted(prev_names & curr_names):
        a, b = prev[name], curr[name]
        pa = {c["name"]: c for c in a.get("cols", [])}
        pb = {c["name"]: c for c in b.get("cols", [])}
        change: dict = {}
        added = [pb[n] for n in pb if n not in pa]
        removed = [pa[n] for n in pa if n not in pb]
        changed = []
        for n in pa:
            if n in pb and (pa[n].get("type") != pb[n].get("type") or bool(pa[n].get("nn")) != bool(pb[n].get("nn"))):
                changed.append({"name": n, "from": {"type": pa[n].get("type"), "nn": bool(pa[n].get("nn"))},
                                "to": {"type": pb[n].get("type"), "nn": bool(pb[n].get("nn"))}})
        if added:
            change["columns_added"] = added
        if removed:
            change["columns_removed"] = removed
        if changed:
            change["columns_changed"] = changed
        if list(a.get("pks", [])) != list(b.get("pks", [])):
            change["pk_changed"] = {"from": list(a.get("pks", [])), "to": list(b.get("pks", []))}
        fa = {(f["col"], f["to_table"], f["to_col"]) for f in a.get("fks", [])}
        fb = {(f["col"], f["to_table"], f["to_col"]) for f in b.get("fks", [])}
        if fb - fa:
            change["fks_added"] = [{"col": c, "to_table": t, "to_col": k} for c, t, k in sorted(fb - fa)]
        if fa - fb:
            change["fks_removed"] = [{"col": c, "to_table": t, "to_col": k} for c, t, k in sorted(fa - fb)]
        ia = {i["name"] for i in a.get("indexes", [])}
        ib = {i["name"] for i in b.get("indexes", [])}
        if ib - ia:
            change["indexes_added"] = sorted(ib - ia)
        if ia - ib:
            change["indexes_removed"] = sorted(ia - ib)
        if change:
            out["tables_changed"][name] = change
    return out


def has_changes(diff: dict) -> bool:
    return bool(diff.get("tables_added") or diff.get("tables_removed") or diff.get("tables_changed"))


def summarize(diff: dict, max_items: int = 8) -> str:
    """One paragraph a human (or a Slack channel) can read at a glance."""
    parts: list[str] = []
    for t in diff.get("tables_added", []):
        parts.append(f"+ table {t}")
    for t in diff.get("tables_removed", []):
        parts.append(f"- table {t}")
    for t, ch in diff.get("tables_changed", {}).items():
        for c in ch.get("columns_added", []):
            parts.append(f"{t}: + column {c['name']} {c.get('type', '')}".rstrip())
        for c in ch.get("columns_removed", []):
            parts.append(f"{t}: - column {c['name']}")
        for c in ch.get("columns_changed", []):
            f, to = c["from"], c["to"]
            what = []
            if f.get("type") != to.get("type"):
                what.append(f"type {f.get('type')} → {to.get('type')}")
            if f.get("nn") != to.get("nn"):
                what.append("NOT NULL added" if to.get("nn") else "now nullable")
            parts.append(f"{t}.{c['name']}: " + ", ".join(what))
        if ch.get("pk_changed"):
            parts.append(f"{t}: primary key {ch['pk_changed']['from']} → {ch['pk_changed']['to']}")
        for fk in ch.get("fks_added", []):
            parts.append(f"{t}: + FK {fk['col']} → {fk['to_table']}.{fk['to_col']}")
        for fk in ch.get("fks_removed", []):
            parts.append(f"{t}: - FK {fk['col']} → {fk['to_table']}.{fk['to_col']}")
        for i in ch.get("indexes_added", []):
            parts.append(f"{t}: + index {i}")
        for i in ch.get("indexes_removed", []):
            parts.append(f"{t}: - index {i}")
    if not parts:
        return "No schema changes."
    shown = parts[:max_items]
    more = len(parts) - len(shown)
    return "; ".join(shown) + (f"; … and {more} more" if more > 0 else "")


# ── run ───────────────────────────────────────────────────────


async def run_watch(connection_id: str, *, notify: bool = True, keep_snapshots: int = 30) -> dict:
    """Snapshot the connection, diff against the previous snapshot, store,
    and notify when something changed.

    Returns ``{"changed", "first_run", "summary", "diff", "snapshot_id",
    "table_count"}``. Raises ValueError for an unknown / non-Postgres
    connection and RuntimeError when the catalog can't be read — the
    scheduler turns those into a failed run (and `scheduler.job.error`).
    """
    from tusk.core.catalog import fetch_catalog
    from tusk.core.connection import get_connection

    conn = get_connection(connection_id)
    if conn is None:
        raise ValueError(f"connection {connection_id} not found")
    if conn.type != "postgres":
        raise ValueError(f"schema watch only supports PostgreSQL connections (got {conn.type})")

    catalog = await fetch_catalog(conn)
    previous = latest_snapshot(connection_id)
    snapshot_id = save_snapshot(connection_id, catalog)
    prune_snapshots(connection_id, keep_last=keep_snapshots)

    # Data Contracts (core/contracts.py): every snapshot is checked against
    # the connection's active contract; violations notify on their own.
    from tusk.core.contracts import check_contracts

    violations = check_contracts(connection_id, catalog, snapshot_id=snapshot_id, notify=notify)

    if previous is None:
        log.info("schema_watch_baseline", connection_id=connection_id, tables=len(catalog))
        return {
            "changed": False, "first_run": True, "summary": f"Baseline taken: {len(catalog)} tables.",
            "diff": {}, "snapshot_id": snapshot_id, "table_count": len(catalog),
            "contract_violations": violations,
        }

    diff = diff_catalogs(previous["catalog"], catalog)
    changed = has_changes(diff)
    summary = summarize(diff)
    if changed:
        record_change(connection_id, snapshot_id, previous["id"], diff, summary)
        log.warning("schema_changed", connection_id=connection_id, summary=summary)
        if notify:
            _notify(conn, summary, diff)
    else:
        log.info("schema_watch_unchanged", connection_id=connection_id, tables=len(catalog))
    return {
        "changed": changed, "first_run": False, "summary": summary, "diff": diff,
        "snapshot_id": snapshot_id, "table_count": len(catalog),
        "contract_violations": violations,
    }


def _notify(conn, summary: str, diff: dict) -> None:
    try:
        from tusk.core.notifications import get_notification_service

        get_notification_service().send(
            EVENT_KEY,
            summary,
            title=f"Schema changed: {conn.name}",
            icon="git-compare",
            variant="warning",
            link=f"/schema?connection={conn.id}",
            context={
                "connection_id": conn.id,
                "connection": conn.name,
                "tables_added": diff.get("tables_added", []),
                "tables_removed": diff.get("tables_removed", []),
                "tables_changed": sorted(diff.get("tables_changed", {})),
            },
        )
    except Exception as e:  # noqa: BLE001 — notifications must never break the watch
        log.warning("schema_watch_notify_failed", error=str(e))


def since_days(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))
