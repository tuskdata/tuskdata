"""Tusk's own metadata store: one SQLite file, one way to open it.

Every subsystem (auth, sessions, tokens, history, saved queries, AI memory,
notifications, scheduler, jobs, Schema Watch, contracts, admin stats) keeps
its tables in ``~/.tusk/tusk.db``. Plugins keep their own file under
``~/.tusk/plugins/`` — that is deliberate isolation, not legacy.

Before 0.4.38 each subsystem opened its own file (``users.db``,
``history.db``, ``scheduler.db``…). The first ``connect()`` in a process
folds those files into ``tusk.db`` and renames them ``*.db.migrated`` so a
rollback is a rename away. Table names never collided, so the copy is a
plain "create table as it was, copy the rows, recreate the indexes".

Why a single file: one thing to back up, one thing to snapshot on a PVC,
one place to look with ``sqlite3``. And if a Postgres-backed store is ever
needed, there is exactly one function to swap.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from tusk.core.config import TUSK_DIR
from tusk.core.logging import get_logger

log = get_logger("meta")

DB_NAME = "tusk.db"
TUSK_DB: Path = TUSK_DIR / DB_NAME

# Files that older versions wrote next to tusk.db, in migration order.
LEGACY_FILES: tuple[str, ...] = (
    "users.db",
    "history.db",
    "ai_memory.db",
    "notifications.db",
    "scheduler.db",
    "schema_watch.db",
    "jobs.db",
    "stats_history.db",
)

_lock = threading.Lock()
_migrated: set[Path] = set()


def connect(
    path: Path | str | None = None,
    *,
    timeout: float = 10.0,
    row_factory: type | None = None,
) -> sqlite3.Connection:
    """Open the metadata store with the pragmas every subsystem relies on.

    ``path`` defaults to :data:`TUSK_DB`; tests pass a temporary file. When
    the target is a ``tusk.db`` the legacy per-subsystem files next to it
    are folded in first (once per process).
    """
    target = Path(path) if path is not None else TUSK_DB
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.name == DB_NAME:
        migrate_legacy(target)
    conn = sqlite3.connect(target, timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    if row_factory is not None:
        conn.row_factory = row_factory
    return conn


def migrate_legacy(target: Path | None = None) -> list[str]:
    """Fold the pre-0.4.38 files next to ``target`` into it. Returns the
    files that were migrated. Safe to call repeatedly; a no-op after the
    first call in a process and when nothing is left to migrate."""
    target = Path(target) if target is not None else TUSK_DB
    with _lock:
        if target in _migrated:
            return []
        _migrated.add(target)
        done: list[str] = []
        for name in LEGACY_FILES:
            legacy = target.parent / name
            if not legacy.is_file():
                continue
            try:
                _fold(legacy, target)
            except Exception as exc:  # noqa: BLE001 — keep the app booting; the old file is untouched
                log.error("meta_migration_failed", file=name, error=str(exc))
                continue
            legacy.rename(legacy.with_suffix(".db.migrated"))
            for side in ("-wal", "-shm"):
                extra = legacy.with_name(legacy.name + side)
                if extra.exists():
                    extra.unlink()
            done.append(name)
            log.info("meta_migrated", file=name, into=str(target))
        return done


def _fold(legacy: Path, target: Path) -> None:
    """Copy every user table (and its indexes) from ``legacy`` into ``target``.

    Tables that already exist in the target are left alone: that only
    happens if a previous migration was interrupted after copying, and
    merging rows blindly could duplicate them.
    """
    conn = sqlite3.connect(target)
    try:
        conn.execute("ATTACH DATABASE ? AS src", (str(legacy),))
        existing = {
            r[0] for r in conn.execute("SELECT name FROM main.sqlite_master WHERE type = 'table'")
        }
        tables = conn.execute(
            "SELECT name, sql FROM src.sqlite_master "
            "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL"
        ).fetchall()
        indexes = conn.execute(
            "SELECT name, tbl_name, sql FROM src.sqlite_master "
            "WHERE type = 'index' AND sql IS NOT NULL"
        ).fetchall()
        conn.execute("BEGIN")
        for name, sql in tables:
            if name in existing:
                log.warning("meta_table_exists_skipped", table=name, file=legacy.name)
                continue
            conn.execute(sql)
            conn.execute(f'INSERT INTO main."{name}" SELECT * FROM src."{name}"')
        for name, tbl, sql in indexes:
            if tbl in existing:
                continue
            conn.execute(sql)
        conn.execute("COMMIT")
        conn.execute("DETACH DATABASE src")
    finally:
        conn.close()
