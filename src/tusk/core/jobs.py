"""In-process background job registry with SQLite persistence.

Long-running operations (pg_dump, pg_restore, AdGuard query log fetch,
plugin scans, etc.) used to run inline inside async route handlers and
freeze the Granian worker for the entire duration. The job registry
moves them off the request path: the route submits a job, returns
immediately with a job_id, and the work happens in a background thread
(`submit_sync`) or asyncio.Task (`submit_async`).

Persistence (`~/.tusk/jobs.db`) means:

  - the activity drawer doesn't go blank when Tusk restarts,
  - jobs that were running when Tusk died are correctly marked
    `interrupted` on the next boot (their subprocess parent is gone —
    we can't resume, but at least we stop lying about it),
  - completed jobs stay around for ~7 days as history.

The registry does *not* attempt to resume interrupted work. pg_dump
checkpoints don't exist; rerunning is the only option.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import msgspec
import structlog

from tusk.core.connection import TUSK_DIR

log = structlog.get_logger("jobs")

JOBS_DB = TUSK_DIR / "jobs.db"

# Status enum (string for sqlite friendliness):
# - running        : actively executing
# - done           : completed successfully
# - failed         : raised an exception or returned a (False, ...) result
# - failed_timeout : exceeded its declared max_duration_s; watchdog killed it
# - interrupted    : was running when Tusk died; subprocess gone, can't resume


# Default maximum wall-clock duration (seconds) per job kind. Jobs that
# exceed this are killed by the watchdog. Tuned for SMB workloads:
# generous enough not to kill legitimate work, tight enough that a
# truly stuck job doesn't tie up a worker forever.
DEFAULT_MAX_DURATION_S: dict[str, int] = {
    "backup": 3600,                       # pg_dump on large DBs
    "restore": 3600,
    "create_db": 1200,
    "create_database_from_backup": 3600,
    "dns_fetch": 1800,
    "ci_run": 3600,
    "query": 600,
    "dashboard_refresh": 300,
}
# Catch-all for kinds not in the map above. 30 min is a reasonable
# upper bound for ad-hoc plugin jobs.
DEFAULT_MAX_DURATION_FALLBACK_S = 1800


class Job(msgspec.Struct, kw_only=True):
    """Snapshot of a job's state. Returned by the registry's read APIs."""

    id: str
    label: str
    kind: str  # "backup" | "restore" | "create_db" | "dns_fetch" | plugin-defined
    status: str
    owner_id: str | None = None
    started_at: str
    ended_at: str | None = None
    result: str | None = None
    error: str | None = None
    href: str | None = None  # link to result (e.g. /api/admin/backups/<file>)
    # Wall-clock deadline (seconds since started_at). Watchdog reads
    # this to decide when to mark a job failed_timeout. 0 means "no
    # deadline" — used for jobs that are expected to be long-lived
    # or external (e.g. a remote process we just observe).
    max_duration_s: int = 0


# ResultHandler: caller-provided function that converts the raw return
# value of the underlying callable into (success: bool, message: str,
# href: str | None). Lets us keep the registry oblivious to caller-
# specific shapes — backup returns (success, message, filepath); DNS
# fetch returns a ScanResult; plugin jobs may return whatever they want.
ResultHandler = Callable[[Any], tuple[bool, str, str | None]]


def _default_result_handler(result: Any) -> tuple[bool, str, str | None]:
    """Fallback: assume success, stringify the result as the message."""
    if isinstance(result, tuple) and len(result) >= 2 and isinstance(result[0], bool):
        success, message = result[0], str(result[1])
        return success, message, None
    return True, (str(result) if result is not None else ""), None


class JobRegistry:
    """SQLite-backed registry. One global instance per process — see
    :func:`get_registry`. Thread-safe via a single lock around DB writes.
    """

    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        kind TEXT NOT NULL,
        status TEXT NOT NULL,
        owner_id TEXT,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        result TEXT,
        error TEXT,
        href TEXT,
        max_duration_s INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
    CREATE INDEX IF NOT EXISTS idx_jobs_owner_started
        ON jobs(owner_id, started_at DESC);
    """

    def __init__(self, db_path: Path | None = None):
        # Resolve the path lazily so test code can monkey-patch
        # `JOBS_DB` to a tempdir before instantiating the registry.
        # Using a default-arg value would freeze the path at class-
        # definition time, which silently breaks the override.
        self._db_path = db_path if db_path is not None else JOBS_DB
        self._db_lock = threading.Lock()
        # Track in-flight asyncio.Tasks per job_id so the watchdog can
        # cancel them on timeout. Sync (thread-based) jobs CAN'T be
        # killed cleanly from outside in Python — we still mark the
        # DB row as failed_timeout, but the thread keeps running until
        # it returns on its own; Granian's worker recycle will clean
        # that up eventually.
        self._async_tasks: dict[str, asyncio.Task] = {}
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._db_lock, self._connect() as conn:
            conn.executescript(self._SCHEMA)
            # Migration for DBs created before v0.4.16: backfill
            # max_duration_s column if missing.
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(jobs)")}
            if "max_duration_s" not in cols:
                conn.execute(
                    "ALTER TABLE jobs ADD COLUMN max_duration_s INTEGER NOT NULL DEFAULT 0"
                )
                log.info("Migrated jobs table: added max_duration_s column")

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        return Job(**{k: row[k] for k in row.keys()})

    def _insert(self, job: Job) -> None:
        with self._db_lock, self._connect() as conn:
            conn.execute(
                """INSERT INTO jobs
                   (id, label, kind, status, owner_id, started_at,
                    ended_at, result, error, href, max_duration_s)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (job.id, job.label, job.kind, job.status, job.owner_id,
                 job.started_at, job.ended_at, job.result, job.error, job.href,
                 job.max_duration_s),
            )

    def _update(self, job_id: str, **kwargs: Any) -> None:
        if not kwargs:
            return
        cols = ", ".join(f"{k}=?" for k in kwargs)
        params = (*kwargs.values(), job_id)
        with self._db_lock, self._connect() as conn:
            conn.execute(f"UPDATE jobs SET {cols} WHERE id=?", params)

    def submit_sync(
        self,
        *,
        label: str,
        kind: str,
        owner_id: str | None,
        fn: Callable,
        args: tuple = (),
        kwargs: dict | None = None,
        result_handler: ResultHandler = _default_result_handler,
        max_duration_s: int | None = None,
    ) -> str:
        """Run `fn(*args, **kwargs)` in a daemon thread. Returns job_id.

        `max_duration_s` is the wall-clock budget for the watchdog to
        enforce. Pass an explicit value to override; otherwise the
        registry picks a sensible default from DEFAULT_MAX_DURATION_S.
        Pass 0 to opt out (no deadline). Note: sync jobs that exceed
        the budget get marked failed_timeout in the DB but the Python
        thread itself cannot be killed from outside — Granian worker
        recycle eventually cleans up the slot.
        """
        kwargs = dict(kwargs or {})
        job_id = uuid.uuid4().hex[:12]
        if max_duration_s is None:
            max_duration_s = DEFAULT_MAX_DURATION_S.get(kind, DEFAULT_MAX_DURATION_FALLBACK_S)
        job = Job(
            id=job_id, label=label, kind=kind, status="running",
            owner_id=owner_id or None,
            started_at=datetime.now(timezone.utc).isoformat(),
            max_duration_s=max_duration_s,
        )
        self._insert(job)

        def runner() -> None:
            try:
                result = fn(*args, **kwargs)
            except Exception as e:
                log.exception("Job failed", job_id=job_id, label=label, kind=kind)
                self._mark_failed(job_id, str(e))
                return
            self._finalize(job_id, result, result_handler)

        threading.Thread(target=runner, daemon=True, name=f"job-{job_id}").start()
        log.info("Job submitted", job_id=job_id, kind=kind, label=label)
        return job_id

    def submit_async(
        self,
        *,
        label: str,
        kind: str,
        owner_id: str | None,
        coro_fn: Callable[..., Awaitable[Any]],
        args: tuple = (),
        kwargs: dict | None = None,
        result_handler: ResultHandler = _default_result_handler,
        max_duration_s: int | None = None,
    ) -> str:
        """Run `coro_fn(*args, **kwargs)` as an asyncio.Task on the
        current running loop. Must be called from inside a loop.

        `max_duration_s` budget is enforced by the watchdog. For async
        jobs we keep a handle to the Task so the watchdog can
        `task.cancel()` it — clean shutdown, unlike sync jobs.
        """
        kwargs = dict(kwargs or {})
        job_id = uuid.uuid4().hex[:12]
        if max_duration_s is None:
            max_duration_s = DEFAULT_MAX_DURATION_S.get(kind, DEFAULT_MAX_DURATION_FALLBACK_S)
        job = Job(
            id=job_id, label=label, kind=kind, status="running",
            owner_id=owner_id or None,
            started_at=datetime.now(timezone.utc).isoformat(),
            max_duration_s=max_duration_s,
        )
        self._insert(job)

        async def runner() -> None:
            try:
                result = await coro_fn(*args, **kwargs)
            except asyncio.CancelledError:
                # Watchdog (or shutdown) cancelled us. The watchdog
                # already wrote failed_timeout into the DB before
                # cancelling, so we just propagate.
                raise
            except Exception as e:
                log.exception("Job failed", job_id=job_id, label=label, kind=kind)
                self._mark_failed(job_id, str(e))
                return
            finally:
                # Drop the task handle so we don't leak the dict.
                self._async_tasks.pop(job_id, None)
            self._finalize(job_id, result, result_handler)

        loop = asyncio.get_event_loop()
        task = loop.create_task(runner(), name=f"job-{job_id}")
        self._async_tasks[job_id] = task
        log.info("Job submitted", job_id=job_id, kind=kind, label=label)
        return job_id

    def _finalize(self, job_id: str, raw_result: Any, handler: ResultHandler) -> None:
        try:
            success, message, href = handler(raw_result)
        except Exception as e:
            log.exception("Result handler raised", job_id=job_id)
            self._mark_failed(job_id, f"result handler error: {e}")
            return
        now = datetime.now(timezone.utc).isoformat()
        if success:
            self._update(
                job_id, status="done", result=message[:1000] if message else None,
                href=href, ended_at=now,
            )
        else:
            self._update(
                job_id, status="failed", error=message[:1000] if message else None,
                ended_at=now,
            )

    def _mark_failed(self, job_id: str, error: str) -> None:
        self._update(
            job_id, status="failed", error=error[:1000],
            ended_at=datetime.now(timezone.utc).isoformat(),
        )

    def get(self, job_id: str) -> Job | None:
        with self._db_lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._row_to_job(row) if row else None

    def list(self, owner_id: str | None, *, limit: int = 50) -> list[Job]:
        """List jobs visible to `owner_id`. Empty owner_id sees system
        jobs (those with NULL owner) plus their own. None as owner_id
        means "no auth filter — return everything" (admin/single-user)."""
        with self._db_lock, self._connect() as conn:
            if owner_id is None:
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY started_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM jobs
                       WHERE owner_id = ? OR owner_id IS NULL
                       ORDER BY started_at DESC LIMIT ?""",
                    (owner_id, limit),
                ).fetchall()
        return [self._row_to_job(r) for r in rows]

    def list_running(self, owner_id: str | None) -> list[Job]:
        with self._db_lock, self._connect() as conn:
            if owner_id is None:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE status='running' "
                    "ORDER BY started_at DESC"
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM jobs WHERE status='running'
                       AND (owner_id = ? OR owner_id IS NULL)
                       ORDER BY started_at DESC""",
                    (owner_id,),
                ).fetchall()
        return [self._row_to_job(r) for r in rows]

    def mark_timed_out(self) -> int:
        """Watchdog tick. Find every `running` job whose wall-clock has
        exceeded its `max_duration_s`, mark them `failed_timeout`, and
        attempt to cancel the underlying asyncio.Task.

        Returns the number of jobs killed. Designed to be called every
        ~30s from the scheduler.

        Sync jobs we can't kill from outside (Python doesn't allow that
        without ugly ctypes hacks that risk locking up the interpreter).
        We mark them dead in the DB so the UI stops lying, and rely on
        Granian's worker recycle to free the slot. For async jobs we
        cancel the Task cleanly.
        """
        from datetime import datetime as _dt, timezone as _tz
        now = _dt.now(_tz.utc)
        now_iso = now.isoformat()
        killed = 0

        # Read running jobs with a non-zero deadline. Skip 0 (opted out).
        with self._db_lock, self._connect() as conn:
            rows = conn.execute(
                """SELECT id, kind, label, started_at, max_duration_s FROM jobs
                   WHERE status='running' AND max_duration_s > 0"""
            ).fetchall()

        for row in rows:
            try:
                started = _dt.fromisoformat(row["started_at"])
                if started.tzinfo is None:
                    started = started.replace(tzinfo=_tz.utc)
                elapsed = (now - started).total_seconds()
            except Exception:
                continue
            if elapsed < row["max_duration_s"]:
                continue

            job_id = row["id"]
            err = f"exceeded max_duration_s={row['max_duration_s']} (ran {int(elapsed)}s)"
            self._update(
                job_id, status="failed_timeout", error=err, ended_at=now_iso,
            )
            log.warning(
                "Job timed out",
                job_id=job_id, kind=row["kind"], label=row["label"],
                elapsed_s=int(elapsed), max_s=row["max_duration_s"],
            )
            killed += 1

            # Async path: cancel the Task. Sync path: nothing we can
            # do safely from outside the thread; Granian recycle will
            # eventually free the worker.
            task = self._async_tasks.pop(job_id, None)
            if task is not None and not task.done():
                task.cancel()

        return killed

    def mark_interrupted_on_startup(self) -> int:
        """Called by `app.on_startup`. Any row still `running` belongs
        to a process that has since died (we're in a fresh boot). Mark
        them so the UI doesn't show a perpetual spinner."""
        now = datetime.now(timezone.utc).isoformat()
        with self._db_lock, self._connect() as conn:
            cur = conn.execute(
                """UPDATE jobs SET status='interrupted', ended_at=?,
                   error='Tusk restarted while job was running'
                   WHERE status='running'""",
                (now,),
            )
            count = cur.rowcount
        if count > 0:
            log.warning("Marked stale jobs as interrupted", count=count)
        return count

    def prune_old(self, *, days: int = 7) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with self._db_lock, self._connect() as conn:
            cur = conn.execute(
                """DELETE FROM jobs
                   WHERE ended_at IS NOT NULL AND ended_at < ?""",
                (cutoff,),
            )
            count = cur.rowcount
        if count > 0:
            log.info("Pruned old jobs", count=count, days=days)
        return count


_registry: JobRegistry | None = None


def get_registry() -> JobRegistry:
    """Module-level singleton. Lazy so test code can override `JOBS_DB`
    before first access."""
    global _registry
    if _registry is None:
        _registry = JobRegistry()
    return _registry


def submit_job_sync(
    *,
    label: str,
    kind: str,
    owner_id: str | None,
    fn: Callable,
    args: tuple = (),
    kwargs: dict | None = None,
    result_handler: ResultHandler = _default_result_handler,
) -> str:
    """Convenience: get_registry().submit_sync(...)."""
    return get_registry().submit_sync(
        label=label, kind=kind, owner_id=owner_id,
        fn=fn, args=args, kwargs=kwargs, result_handler=result_handler,
    )


def submit_job_async(
    *,
    label: str,
    kind: str,
    owner_id: str | None,
    coro_fn: Callable[..., Awaitable[Any]],
    args: tuple = (),
    kwargs: dict | None = None,
    result_handler: ResultHandler = _default_result_handler,
) -> str:
    """Convenience: get_registry().submit_async(...)."""
    return get_registry().submit_async(
        label=label, kind=kind, owner_id=owner_id,
        coro_fn=coro_fn, args=args, kwargs=kwargs, result_handler=result_handler,
    )
