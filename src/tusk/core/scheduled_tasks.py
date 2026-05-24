"""Scheduled task implementations and generic Job model.

Tusk's scheduler is generic: backups, vacuum, and analyze are 3 of N kinds.
The user-facing definition (a `JobSpec`) is persisted in
``~/.tusk/scheduler.db`` (separate from APScheduler's own jobstore — APScheduler
manages execution, this table is the source of truth for the UI).

Adding a new kind:
1. Append it to :class:`JobKind`.
2. Register a coroutine in :data:`_KIND_HANDLERS` taking a ``payload`` dict.
3. (Optional) add a typed helper like :func:`add_query_job`.
"""

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Awaitable, Callable

import msgspec
import structlog

from tusk.core.connection import get_connection
from tusk.core.scheduler import get_scheduler

log = structlog.get_logger()

TUSK_DIR = Path.home() / ".tusk"
SCHEDULER_DB = TUSK_DIR / "scheduler.db"


# ─────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────


class JobKind(StrEnum):
    BACKUP = "backup"
    VACUUM = "vacuum"
    ANALYZE = "analyze"
    QUERY = "query"
    PIPELINE = "pipeline"
    PLUGIN = "plugin"


class JobSpec(msgspec.Struct):
    """User-facing scheduled job definition.

    ``payload`` shape depends on ``kind`` — see ``_KIND_HANDLERS`` below.

    ``trigger`` is one of:
      - ``{"type": "cron", "hour": 2, "minute": 0, "day_of_week": "*"}``
        (also accepts ``"cron": "0 2 * * *"``)
      - ``{"type": "interval", "hours": 0, "minutes": 5, "seconds": 0}``
      - ``{"type": "date", "run_date": "2026-04-30T10:00:00"}``

    ``owner_id`` is the logged-in user id in multi-user mode (v0.4.9), or
    '' in single-user mode (means 'unowned/global'). Listing helpers can
    filter by ``for_user_id``; admins pass None to see everything.
    """

    id: str
    kind: JobKind
    name: str
    payload: dict
    trigger: dict
    enabled: bool = True
    notify_on_success: bool = False
    notify_on_failure: bool = True
    owner_id: str = ""


# ─────────────────────────────────────────────────────────────
# Persistence (~/.tusk/scheduler.db)
# ─────────────────────────────────────────────────────────────


def _connect() -> sqlite3.Connection:
    TUSK_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(SCHEDULER_DB))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_db() -> None:
    conn = _connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduled_jobs (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                name TEXT NOT NULL,
                payload TEXT NOT NULL,
                trigger TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                notify_on_success INTEGER NOT NULL DEFAULT 0,
                notify_on_failure INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                last_run_at TEXT,
                last_run_status TEXT,
                last_run_error TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                error TEXT,
                duration_ms INTEGER
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs(job_id, started_at DESC)"
        )
        # Per-pipeline materialization records. Separate from ``job_runs``
        # (the generic dispatcher audit trail) so the parquet output
        # paths + row counts don't bloat every other job kind's history.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                output_path TEXT,
                rows_written INTEGER,
                error TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_job ON pipeline_runs(job_id, id DESC)"
        )
        # v0.4.9 — per-user isolation: idempotent ALTER to add owner_id.
        try:
            conn.execute("ALTER TABLE scheduled_jobs ADD COLUMN owner_id TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            pass  # column already exists
        conn.commit()
    finally:
        conn.close()


def save_job(spec: JobSpec) -> None:
    _init_db()
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO scheduled_jobs
                (id, kind, name, payload, trigger, enabled, notify_on_success, notify_on_failure, created_at, owner_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                kind=excluded.kind,
                name=excluded.name,
                payload=excluded.payload,
                trigger=excluded.trigger,
                enabled=excluded.enabled,
                notify_on_success=excluded.notify_on_success,
                notify_on_failure=excluded.notify_on_failure,
                owner_id=excluded.owner_id
            """,
            (
                spec.id,
                str(spec.kind),
                spec.name,
                json.dumps(spec.payload),
                json.dumps(spec.trigger),
                1 if spec.enabled else 0,
                1 if spec.notify_on_success else 0,
                1 if spec.notify_on_failure else 0,
                datetime.now(timezone.utc).isoformat(),
                spec.owner_id or "",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _row_to_spec(row: sqlite3.Row) -> JobSpec:
    # owner_id column is added by an idempotent ALTER. On databases created
    # before v0.4.9 the row may not expose it via row.keys() if a partial
    # init ran — defend with a try/except.
    try:
        owner_id = row["owner_id"] or ""
    except (KeyError, IndexError):
        owner_id = ""
    return JobSpec(
        id=row["id"],
        kind=JobKind(row["kind"]),
        name=row["name"],
        payload=json.loads(row["payload"]),
        trigger=json.loads(row["trigger"]),
        enabled=bool(row["enabled"]),
        notify_on_success=bool(row["notify_on_success"]),
        notify_on_failure=bool(row["notify_on_failure"]),
        owner_id=owner_id,
    )


def get_job(job_id: str) -> JobSpec | None:
    _init_db()
    conn = _connect()
    try:
        row = conn.execute("SELECT * FROM scheduled_jobs WHERE id=?", (job_id,)).fetchone()
        return _row_to_spec(row) if row else None
    finally:
        conn.close()


def list_jobs(for_user_id: str | None = None) -> list[dict]:
    """Return jobs joined with execution metadata for the UI.

    When ``for_user_id`` is non-None and non-empty, only jobs owned by
    that user OR legacy unowned jobs (owner_id='') are returned. Pass
    None to see everything (admin / single-user mode).
    """
    _init_db()
    conn = _connect()
    try:
        if for_user_id:
            rows = conn.execute(
                "SELECT * FROM scheduled_jobs "
                "WHERE owner_id IN (?, '') "
                "ORDER BY created_at DESC",
                (for_user_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM scheduled_jobs ORDER BY created_at DESC"
            ).fetchall()
    finally:
        conn.close()

    out: list[dict] = []
    for r in rows:
        spec = _row_to_spec(r)
        out.append(
            {
                "id": spec.id,
                "kind": str(spec.kind),
                "name": spec.name,
                "payload": spec.payload,
                "trigger": spec.trigger,
                "enabled": spec.enabled,
                "notify_on_success": spec.notify_on_success,
                "notify_on_failure": spec.notify_on_failure,
                "owner_id": spec.owner_id,
                "created_at": r["created_at"],
                "last_run_at": r["last_run_at"],
                "last_run_status": r["last_run_status"],
                "last_run_error": r["last_run_error"],
            }
        )
    return out


def delete_job(job_id: str) -> bool:
    _init_db()
    conn = _connect()
    try:
        cur = conn.execute("DELETE FROM scheduled_jobs WHERE id=?", (job_id,))
        conn.execute("DELETE FROM job_runs WHERE job_id=?", (job_id,))
        conn.execute("DELETE FROM pipeline_runs WHERE job_id=?", (job_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_trigger(job_id: str, trigger: dict) -> bool:
    """Replace the trigger on a stored job, re-register it in
    APScheduler so the new schedule actually fires (B9 in 0.4.26).

    Returns True if the row was updated. The caller is responsible
    for validating the trigger dict (raises ValueError downstream
    when re-registering an invalid one).
    """
    import json as _json
    _init_db()
    conn = _connect()
    try:
        cur = conn.execute(
            "UPDATE scheduled_jobs SET trigger=? WHERE id=?",
            (_json.dumps(trigger), job_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            return False
    finally:
        conn.close()
    # Re-register: remove the old APScheduler entry and add the spec
    # again with the new trigger.
    spec = get_job(job_id)
    if not spec:
        return False
    sched = get_scheduler()
    sched.remove_job(job_id)
    _register_with_scheduler(spec)
    return True


def set_enabled(job_id: str, enabled: bool) -> bool:
    _init_db()
    conn = _connect()
    try:
        cur = conn.execute(
            "UPDATE scheduled_jobs SET enabled=? WHERE id=?",
            (1 if enabled else 0, job_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def record_run(
    job_id: str,
    status: str,
    started_at: datetime,
    finished_at: datetime | None,
    error: str | None,
) -> None:
    _init_db()
    duration_ms = None
    if finished_at is not None:
        duration_ms = int((finished_at - started_at).total_seconds() * 1000)
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO job_runs (job_id, started_at, finished_at, status, error, duration_ms)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                started_at.isoformat(),
                finished_at.isoformat() if finished_at else None,
                status,
                error,
                duration_ms,
            ),
        )
        conn.execute(
            """
            UPDATE scheduled_jobs
            SET last_run_at=?, last_run_status=?, last_run_error=?
            WHERE id=?
            """,
            (started_at.isoformat(), status, error, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_runs(job_id: str, limit: int = 10) -> list[dict]:
    _init_db()
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT started_at, finished_at, status, error, duration_ms
            FROM job_runs
            WHERE job_id=?
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (job_id, max(1, min(limit, 100))),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def record_pipeline_run(
    job_id: str,
    started_at: datetime,
    ended_at: datetime,
    output_path: str | None,
    rows_written: int | None,
    error: str | None = None,
) -> int:
    """Persist a pipeline materialization record; returns the new row id."""
    _init_db()
    conn = _connect()
    try:
        cur = conn.execute(
            """
            INSERT INTO pipeline_runs
                (job_id, started_at, ended_at, output_path, rows_written, error, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                started_at.isoformat(),
                ended_at.isoformat(),
                output_path,
                rows_written,
                error,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid or 0)
    finally:
        conn.close()


def get_pipeline_runs(job_id: str, limit: int = 10) -> list[dict]:
    """Return last ``limit`` pipeline runs for a job, newest first."""
    _init_db()
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT id, job_id, started_at, ended_at, output_path,
                   rows_written, error, created_at
            FROM pipeline_runs
            WHERE job_id=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (job_id, max(1, min(limit, 100))),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_pipeline_run(run_id: int) -> dict | None:
    """Return a single pipeline run row by its primary key."""
    _init_db()
    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT id, job_id, started_at, ended_at, output_path,
                   rows_written, error, created_at
            FROM pipeline_runs WHERE id=?
            """,
            (run_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────
# Trigger conversion helpers
# ─────────────────────────────────────────────────────────────


def _register_with_scheduler(spec: JobSpec) -> None:
    """Register a JobSpec with APScheduler so it actually runs."""
    scheduler = get_scheduler()
    trigger = spec.trigger or {}
    ttype = trigger.get("type", "cron")

    async def task() -> None:
        await _run_job(spec.id)

    if ttype == "cron":
        cron = trigger.get("cron")
        if cron:
            parts = cron.split()
            if len(parts) >= 5:
                minute, hour, day, month, day_of_week = parts[:5]
                scheduler.add_cron_job(
                    func=task,
                    job_id=spec.id,
                    name=spec.name,
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week,
                )
                return
        scheduler.add_cron_job(
            func=task,
            job_id=spec.id,
            name=spec.name,
            hour=trigger.get("hour", 0),
            minute=trigger.get("minute", 0),
            day_of_week=trigger.get("day_of_week", "*"),
            day=trigger.get("day", "*"),
            month=trigger.get("month", "*"),
        )
    elif ttype == "interval":
        scheduler.add_interval_job(
            func=task,
            job_id=spec.id,
            name=spec.name,
            hours=int(trigger.get("hours", 0) or 0),
            minutes=int(trigger.get("minutes", 0) or 0),
            seconds=int(trigger.get("seconds", 0) or 0),
        )
    elif ttype == "date":
        run_date_raw = trigger.get("run_date")
        if not run_date_raw:
            raise ValueError("date trigger requires run_date")
        run_date = datetime.fromisoformat(run_date_raw)
        scheduler.add_date_job(
            func=task,
            job_id=spec.id,
            name=spec.name,
            run_date=run_date,
        )
    else:
        raise ValueError(f"Unknown trigger type: {ttype}")


# ─────────────────────────────────────────────────────────────
# Job dispatcher (the single function APScheduler calls)
# ─────────────────────────────────────────────────────────────


HandlerFn = Callable[[dict], Awaitable[None]]
_KIND_HANDLERS: dict[JobKind, HandlerFn] = {}


def register_kind_handler(kind: JobKind, handler: HandlerFn) -> None:
    """Register a coroutine handler for a job kind.

    Plugins call this to extend the scheduler with new kinds.
    """
    _KIND_HANDLERS[kind] = handler


async def _run_job(spec_id: str) -> None:
    """Single dispatch entrypoint. APScheduler calls this for every spec."""
    spec = get_job(spec_id)
    if spec is None:
        log.warning("scheduled_job_missing", job_id=spec_id)
        return
    if not spec.enabled:
        log.info("scheduled_job_skipped_disabled", job_id=spec_id)
        return

    handler = _KIND_HANDLERS.get(spec.kind)
    if handler is None:
        log.error("scheduled_job_no_handler", job_id=spec_id, kind=str(spec.kind))
        return

    started = datetime.now(timezone.utc)
    error: str | None = None
    status = "ok"
    # Inject run-context into the payload so handlers that need it (e.g.
    # `_handle_pipeline`, which writes per-job parquet artifacts) can
    # read ``_job_id`` / ``_job_name`` without changing the handler
    # signature contract. Built-in handlers ignore unknown keys.
    runtime_payload = {
        **spec.payload,
        "_job_id": spec.id,
        "_job_name": spec.name,
    }
    try:
        await handler(runtime_payload)
    except Exception as exc:  # noqa: BLE001 — generic dispatcher
        error = f"{type(exc).__name__}: {exc}"
        status = "error"
        log.error("scheduled_job_failed", job_id=spec_id, kind=str(spec.kind), error=error)
    finally:
        finished = datetime.now(timezone.utc)
        record_run(spec_id, status, started, finished, error)
        _maybe_notify(spec, status, error)


def _maybe_notify(spec: JobSpec, status: str, error: str | None) -> None:
    if status == "ok" and not spec.notify_on_success:
        return
    if status == "error" and not spec.notify_on_failure:
        return
    try:
        from tusk.core.notifications import get_notification_service

        svc = get_notification_service()
        event_key = f"scheduler.job.{status}"
        title = f"Scheduled job {status}: {spec.name}"
        message = error or f"Job '{spec.name}' completed successfully."
        svc.send(
            event_key,
            message,
            title=title,
            icon="clock",
            variant="error" if status == "error" else "success",
            context={"job_id": spec.id, "kind": str(spec.kind)},
        )
    except Exception as e:  # noqa: BLE001 — notifications must never break scheduler
        log.warning("scheduler_notify_failed", job_id=spec.id, error=str(e))


# ─────────────────────────────────────────────────────────────
# Built-in kind handlers
# ─────────────────────────────────────────────────────────────


async def _handle_backup(payload: dict) -> None:
    # `BackupService` was refactored to a free function in an earlier
    # release; the scheduler kept referencing the old class name and
    # every scheduled backup was failing with ImportError. Fixed 0.4.25.
    from tusk.admin.backup import create_backup
    import asyncio

    connection_id = payload.get("connection_id")
    if not connection_id:
        raise ValueError("backup payload missing connection_id")
    config = get_connection(connection_id)
    if not config or config.type != "postgres":
        raise ValueError(f"connection {connection_id} not found or not postgres")
    fmt = str(payload.get("format", "plain"))
    tables = payload.get("tables") or None
    # create_backup is sync (subprocess-driven pg_dump); run in a thread
    # so we don't block the scheduler event loop on a multi-GB dump.
    success, message, _path = await asyncio.to_thread(
        create_backup, config, format=fmt, tables=tables,
    )
    if not success:
        raise RuntimeError(message or "backup failed")


async def _handle_vacuum(payload: dict) -> None:
    from tusk.engines import postgres

    connection_id = payload.get("connection_id")
    full = bool(payload.get("full", False))
    config = get_connection(connection_id)
    if not config or config.type != "postgres":
        raise ValueError(f"connection {connection_id} not found or not postgres")

    schema = await postgres.get_schema(config)
    tables: list[str] = []
    for schema_name, schema_tables in schema.items():
        if schema_name in ("information_schema", "pg_catalog"):
            continue
        for table_name in schema_tables.keys():
            tables.append(f'"{schema_name}"."{table_name}"')

    cmd = "VACUUM FULL ANALYZE" if full else "VACUUM ANALYZE"
    errors = 0
    for table in tables:
        try:
            await postgres.execute_query(config, f"{cmd} {table}")
        except Exception as e:  # noqa: BLE001 — collect & continue
            log.warning("vacuum_table_failed", table=table, error=str(e))
            errors += 1
    if errors and errors == len(tables):
        raise RuntimeError("VACUUM failed on every table")


async def _handle_analyze(payload: dict) -> None:
    from tusk.engines import postgres

    connection_id = payload.get("connection_id")
    config = get_connection(connection_id)
    if not config or config.type != "postgres":
        raise ValueError(f"connection {connection_id} not found or not postgres")
    await postgres.execute_query(config, "ANALYZE")


async def _handle_query(payload: dict) -> None:
    from tusk.engines import postgres

    connection_id = payload.get("connection_id")
    sql = payload.get("sql")
    save_results_as = payload.get("save_results_as")
    if not connection_id or not sql:
        raise ValueError("query payload missing connection_id or sql")

    # Validate save_results_as BEFORE the connection lookup so
    # path-traversal attempts surface immediately and don't depend
    # on whether the connection happens to exist (audit #1).
    if save_results_as:
        import re
        if not re.match(r"^[A-Za-z0-9_-]{1,64}$", save_results_as):
            raise ValueError(
                "save_results_as must be 1-64 chars of [A-Za-z0-9_-]; "
                f"got {save_results_as!r}"
            )

    config = get_connection(connection_id)
    if not config:
        raise ValueError(f"connection {connection_id} not found")
    result = await postgres.execute_query(config, sql)
    if result.error:
        raise RuntimeError(result.error)
    if save_results_as:
        # Persist to ~/.tusk/scheduled_results/{name}.json so other tabs can pick it up.
        # Path-traversal guard: alphanumeric + dash + underscore only.
        # `..` / `/` / NUL would let a malicious editor (multi-user) overwrite
        # arbitrary files (e.g. ~/.ssh/authorized_keys via the JSON dump).
        import re
        if not re.match(r"^[A-Za-z0-9_-]{1,64}$", save_results_as):
            raise ValueError(
                "save_results_as must be 1-64 chars of [A-Za-z0-9_-]; "
                f"got {save_results_as!r}"
            )
        out_dir = TUSK_DIR / "scheduled_results"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = (out_dir / f"{save_results_as}.json").resolve()
        # Defense-in-depth: even with the regex above, verify the resolved
        # path stays inside out_dir.
        if not str(out_path).startswith(str(out_dir.resolve()) + "/"):
            raise ValueError("save_results_as escapes the output directory")
        out_path.write_text(json.dumps(result.to_dict(), default=str))


def _dataset_to_pipeline(dataset, pipeline_id: str):
    """Translate a workspace ``DatasetState`` into a Polars ``Pipeline``.

    The dataset itself is the primary source; any ``join_sources`` ride
    along so JOIN / CONCAT transforms can resolve them. Transform dicts
    go through the same ``_parse_transform`` parser that ``/api/data/*``
    routes use, so behaviour stays in lockstep with the Data tab.
    """
    from tusk.engines.polars_engine import DataSource, Pipeline
    from tusk.studio.routes.data import _parse_transform

    sources = [
        DataSource(
            id=dataset.id,
            name=dataset.name,
            source_type=dataset.source_type,
            path=dataset.path,
            connection_id=dataset.connection_id,
            query=dataset.query,
            osm_layer=dataset.osm_layer,
        )
    ]
    for js in dataset.join_sources or []:
        sources.append(
            DataSource(
                id=js.get("id", ""),
                name=js.get("name", "Unnamed"),
                source_type=js.get("source_type", "csv"),
                path=js.get("path"),
                connection_id=js.get("connection_id"),
                query=js.get("query"),
                osm_layer=js.get("osm_layer"),
            )
        )

    transforms = []
    for t in dataset.transforms or []:
        parsed = _parse_transform(t)
        if parsed is not None:
            transforms.append(parsed)

    return Pipeline(
        id=pipeline_id,
        name=dataset.name,
        sources=sources,
        transforms=transforms,
        output_source_id=dataset.id,
    )


async def _handle_pipeline(payload: dict) -> None:
    """Run a saved Data tab pipeline.

    A pipeline is identified by a ``(workspace, dataset_id)`` pair from
    :mod:`tusk.core.workspace`. The dataset definition is re-loaded and
    converted into a Polars :class:`Pipeline`, executed via the same
    ``_run_pipeline`` helper that powers ``/api/data/*``, and the
    resulting DataFrame is materialized to
    ``~/.tusk/pipeline_runs/{job_id}/{utc_timestamp}.parquet``.

    A row is recorded in the ``pipeline_runs`` table on success **and**
    on failure — callers can audit which runs produced output even
    after a partial failure.
    """
    import asyncio

    from tusk.core.workspace import load_workspace
    from tusk.engines.polars_engine import _run_pipeline

    workspace = payload.get("workspace") or "default"
    pipeline_id = payload.get("pipeline_id")
    # ``_job_id`` is injected by ``_run_job`` before dispatch. When the
    # handler is called directly (tests, CLI) we fall back to the dataset
    # id so the parquet directory is still keyed by something stable.
    job_id = payload.get("_job_id") or pipeline_id
    if not pipeline_id:
        raise ValueError("pipeline payload missing pipeline_id")

    state = load_workspace(workspace)
    if state is None:
        raise ValueError(f"workspace '{workspace}' not found")
    matches = [d for d in state.datasets if d.id == pipeline_id]
    if not matches:
        raise ValueError(f"pipeline (dataset) '{pipeline_id}' not found")
    dataset = matches[0]

    pipeline = _dataset_to_pipeline(dataset, pipeline_id)

    started = datetime.now(timezone.utc)
    out_dir = TUSK_DIR / "pipeline_runs" / job_id
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = started.strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"{timestamp}.parquet"

    try:
        # Pipeline execution can take seconds (file I/O, joins, network
        # for SQL sources) — keep it off the event loop.
        df = await asyncio.to_thread(_run_pipeline, pipeline, None)
        rows_written = int(df.height)
        await asyncio.to_thread(df.write_parquet, out_path)
    except Exception as exc:
        ended = datetime.now(timezone.utc)
        record_pipeline_run(
            job_id=job_id,
            started_at=started,
            ended_at=ended,
            output_path=None,
            rows_written=None,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise

    ended = datetime.now(timezone.utc)
    record_pipeline_run(
        job_id=job_id,
        started_at=started,
        ended_at=ended,
        output_path=str(out_path),
        rows_written=rows_written,
        error=None,
    )
    log.info(
        "pipeline_run_complete",
        job_id=job_id,
        pipeline_id=pipeline_id,
        rows_written=rows_written,
        output_path=str(out_path),
    )


async def _handle_plugin(payload: dict) -> None:
    """Invoke a plugin-registered handler.

    Payload: ``{"plugin_id": "tusk-ci", "kind": "ci.deploy", "payload": {...}}``.
    Plugins register handlers via :func:`register_kind_handler` with a
    composite key (``f"plugin:{plugin_id}.{kind}"``) — this dispatcher
    looks that up.
    """
    plugin_id = payload.get("plugin_id")
    inner_kind = payload.get("kind")
    inner_payload = payload.get("payload") or {}
    if not plugin_id or not inner_kind:
        raise ValueError("plugin payload missing plugin_id or kind")
    composite = f"plugin:{plugin_id}.{inner_kind}"
    handler = _PLUGIN_HANDLERS.get(composite)
    if handler is None:
        raise ValueError(f"no plugin handler registered for {composite}")
    await handler(inner_payload)


_PLUGIN_HANDLERS: dict[str, HandlerFn] = {}


def register_plugin_handler(plugin_id: str, kind: str, handler: HandlerFn) -> None:
    """Plugins call this on startup to register their job kinds."""
    _PLUGIN_HANDLERS[f"plugin:{plugin_id}.{kind}"] = handler


# Wire built-ins
_KIND_HANDLERS[JobKind.BACKUP] = _handle_backup
_KIND_HANDLERS[JobKind.VACUUM] = _handle_vacuum
_KIND_HANDLERS[JobKind.ANALYZE] = _handle_analyze
_KIND_HANDLERS[JobKind.QUERY] = _handle_query
_KIND_HANDLERS[JobKind.PIPELINE] = _handle_pipeline
_KIND_HANDLERS[JobKind.PLUGIN] = _handle_plugin


# ─────────────────────────────────────────────────────────────
# Back-compat: legacy task functions (kept so existing imports work)
# ─────────────────────────────────────────────────────────────


async def scheduled_backup_task(connection_id: str, backup_dir: str | None = None) -> None:
    await _handle_backup({"connection_id": connection_id, "backup_dir": backup_dir})


async def scheduled_vacuum_task(connection_id: str, full: bool = False) -> None:
    await _handle_vacuum({"connection_id": connection_id, "full": full})


async def scheduled_analyze_task(connection_id: str) -> None:
    await _handle_analyze({"connection_id": connection_id})


def setup_default_schedules() -> None:
    """Setup default scheduled tasks (called on app startup)."""
    _init_db()
    scheduler = get_scheduler()
    scheduler.start()
    # Re-register every persisted, enabled JobSpec.
    conn = _connect()
    try:
        rows = conn.execute("SELECT * FROM scheduled_jobs WHERE enabled=1").fetchall()
    finally:
        conn.close()
    for row in rows:
        try:
            spec = _row_to_spec(row)
            _register_with_scheduler(spec)
        except Exception as e:  # noqa: BLE001 — never break startup
            log.error("scheduler_rehydrate_failed", job_id=row["id"], error=str(e))
    log.info("default_schedules_initialized", restored=len(rows))


# ─────────────────────────────────────────────────────────────
# Generic + typed helpers (cron schedules)
# ─────────────────────────────────────────────────────────────


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def add_job(spec: JobSpec) -> str:
    """Persist a JobSpec and register it with APScheduler."""
    save_job(spec)
    if spec.enabled:
        _register_with_scheduler(spec)
    return spec.id


def add_backup_schedule(
    connection_id: str,
    hour: int = 2,
    minute: int = 0,
    day_of_week: str = "*",
    backup_dir: str | None = None,
    owner_id: str | None = None,
) -> str:
    spec = JobSpec(
        id=f"backup_{connection_id}",
        kind=JobKind.BACKUP,
        name=f"Backup {connection_id}",
        payload={"connection_id": connection_id, "backup_dir": backup_dir},
        trigger={
            "type": "cron",
            "hour": hour,
            "minute": minute,
            "day_of_week": day_of_week,
        },
        owner_id=owner_id or "",
    )
    return add_job(spec)


def add_vacuum_schedule(
    connection_id: str,
    hour: int = 3,
    minute: int = 0,
    day_of_week: str = "sun",
    full: bool = False,
    owner_id: str | None = None,
) -> str:
    spec = JobSpec(
        id=f"vacuum_{connection_id}",
        kind=JobKind.VACUUM,
        name=f"VACUUM {connection_id}",
        payload={"connection_id": connection_id, "full": full},
        trigger={
            "type": "cron",
            "hour": hour,
            "minute": minute,
            "day_of_week": day_of_week,
        },
        owner_id=owner_id or "",
    )
    return add_job(spec)


def add_analyze_schedule(
    connection_id: str,
    hour: int = 4,
    minute: int = 0,
    owner_id: str | None = None,
) -> str:
    spec = JobSpec(
        id=f"analyze_{connection_id}",
        kind=JobKind.ANALYZE,
        name=f"ANALYZE {connection_id}",
        payload={"connection_id": connection_id},
        trigger={"type": "cron", "hour": hour, "minute": minute, "day_of_week": "*"},
        owner_id=owner_id or "",
    )
    return add_job(spec)


def add_query_job(
    name: str,
    connection_id: str,
    sql: str,
    trigger: dict,
    save_results_as: str | None = None,
    owner_id: str | None = None,
) -> str:
    spec = JobSpec(
        id=_new_id("query"),
        kind=JobKind.QUERY,
        name=name,
        payload={
            "connection_id": connection_id,
            "sql": sql,
            "save_results_as": save_results_as,
        },
        trigger=trigger,
        owner_id=owner_id or "",
    )
    return add_job(spec)


def add_pipeline_job(
    name: str,
    pipeline_id: str,
    trigger: dict,
    workspace: str = "default",
    owner_id: str | None = None,
) -> str:
    spec = JobSpec(
        id=_new_id("pipeline"),
        kind=JobKind.PIPELINE,
        name=name,
        payload={"pipeline_id": pipeline_id, "workspace": workspace},
        trigger=trigger,
        owner_id=owner_id or "",
    )
    return add_job(spec)


def add_plugin_job(
    name: str,
    plugin_id: str,
    kind: str,
    payload: dict,
    trigger: dict,
    owner_id: str | None = None,
) -> str:
    spec = JobSpec(
        id=_new_id(f"plugin_{plugin_id}"),
        kind=JobKind.PLUGIN,
        name=name,
        payload={"plugin_id": plugin_id, "kind": kind, "payload": payload},
        trigger=trigger,
        owner_id=owner_id or "",
    )
    return add_job(spec)


def remove_schedule(job_id: str) -> bool:
    """Remove a scheduled job (DB + APScheduler)."""
    scheduler = get_scheduler()
    scheduler.remove_job(job_id)
    return delete_job(job_id)


# ─────────────────────────────────────────────────────────────
# One-time scheduled tasks (kept for back-compat)
# ─────────────────────────────────────────────────────────────


def add_backup_once(connection_id: str, run_date: datetime, backup_dir: str | None = None) -> str:
    spec = JobSpec(
        id=f"backup_once_{connection_id}_{run_date.strftime('%Y%m%d%H%M')}",
        kind=JobKind.BACKUP,
        name=f"Backup {connection_id} (once)",
        payload={"connection_id": connection_id, "backup_dir": backup_dir},
        trigger={"type": "date", "run_date": run_date.isoformat()},
    )
    return add_job(spec)


def add_vacuum_once(connection_id: str, run_date: datetime, full: bool = False) -> str:
    spec = JobSpec(
        id=f"vacuum_once_{connection_id}_{run_date.strftime('%Y%m%d%H%M')}",
        kind=JobKind.VACUUM,
        name=f"VACUUM {connection_id} (once)",
        payload={"connection_id": connection_id, "full": full},
        trigger={"type": "date", "run_date": run_date.isoformat()},
    )
    return add_job(spec)


def add_analyze_once(connection_id: str, run_date: datetime) -> str:
    spec = JobSpec(
        id=f"analyze_once_{connection_id}_{run_date.strftime('%Y%m%d%H%M')}",
        kind=JobKind.ANALYZE,
        name=f"ANALYZE {connection_id} (once)",
        payload={"connection_id": connection_id},
        trigger={"type": "date", "run_date": run_date.isoformat()},
    )
    return add_job(spec)
