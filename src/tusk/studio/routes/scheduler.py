"""Scheduler API routes.

All endpoints are guarded by :func:`tusk.studio.routes.admin._check_admin_auth`
(audit finding #4 — scheduler is admin-only).
"""

from datetime import datetime
from pathlib import Path

from litestar import Controller, Request, get, post, put, delete
from litestar.exceptions import NotFoundException
from litestar.params import Body
from litestar.response import File

from tusk.core.scheduler import get_scheduler
from tusk.core.scheduled_tasks import (
    TUSK_DIR,
    add_backup_schedule,
    add_schema_watch_schedule,
    add_vacuum_schedule,
    add_analyze_schedule,
    add_backup_once,
    add_vacuum_once,
    add_analyze_once,
    add_query_job,
    add_pipeline_job,
    add_plugin_job,
    list_jobs as list_specs,
    get_runs,
    get_pipeline_run,
    get_pipeline_runs,
    remove_schedule,
    set_trigger,
)
from tusk.studio.routes.admin import _check_admin_auth
from tusk.studio.routes.base import _current_user_id


def _next_runs(scheduler, job_id: str, limit: int = 10) -> list[str]:
    """Return up to ``limit`` upcoming run timestamps for an APScheduler job."""
    job = scheduler.scheduler.get_job(job_id)
    if job is None or job.trigger is None:
        return []

    runs: list[str] = []
    cursor = None
    try:
        for _ in range(limit):
            cursor = job.trigger.get_next_fire_time(cursor, datetime.now(cursor.tzinfo) if cursor else datetime.utcnow())
            if cursor is None:
                break
            runs.append(cursor.isoformat())
    except Exception:
        # Some triggers (e.g. DateTrigger past run_date) raise — that's fine.
        pass
    return runs


class SchedulerController(Controller):
    """REST API for scheduled tasks (admin-only)."""

    path = "/api/scheduler"
    guards = [_check_admin_auth]

    # ─────────────────────────────────────────────────────────
    # Listing & metadata
    # ─────────────────────────────────────────────────────────

    @get("/info")
    async def scheduler_info(self) -> dict:
        """Scheduler-wide config the frontend wants to surface (timezone
        next to cron-expression hints, mostly). Added 0.4.26 for B11."""
        scheduler = get_scheduler()
        tz = getattr(scheduler.scheduler, "timezone", None)
        tz_name = str(tz) if tz else "UTC"
        return {"timezone": tz_name}

    @get("/jobs")
    async def list_jobs(self) -> dict:
        """List all scheduled jobs (unified, includes kind + last run + next runs)."""
        scheduler = get_scheduler()
        specs = list_specs()
        ap_jobs = {j.id: j for j in scheduler.scheduler.get_jobs()} if scheduler._started else {}

        out = []
        for s in specs:
            ap = ap_jobs.get(s["id"])
            next_run = ap.next_run_time.isoformat() if ap and ap.next_run_time else None
            out.append(
                {
                    **s,
                    "next_run": next_run,
                    "next_runs": _next_runs(scheduler, s["id"], 10) if ap else [],
                    "running": ap is not None and ap.next_run_time is not None,
                }
            )
        return {"jobs": out}

    @get("/jobs/{job_id:str}/runs")
    async def job_runs(self, job_id: str) -> dict:
        """Last 10 runs of a job."""
        return {"runs": get_runs(job_id, limit=10)}

    @get("/jobs/{job_id:str}/pipeline-runs")
    async def job_pipeline_runs(self, job_id: str) -> dict:
        """Last 10 pipeline materializations for a job (newest first).

        Each entry includes ``output_path`` (parquet file) and
        ``rows_written``. The ``id`` field is the primary key for use
        with the ``/pipeline-runs/{run_id}/download`` endpoint.
        """
        return {"runs": get_pipeline_runs(job_id, limit=10)}

    @get("/pipeline-runs/{run_id:int}/download")
    async def download_pipeline_run(self, run_id: int) -> File:
        """Stream the parquet output for a pipeline run.

        Containment guard: the recorded ``output_path`` must live inside
        ``~/.tusk/pipeline_runs/`` — defends against a tampered DB row
        pointing at an arbitrary file (single-user mode is loopback-only,
        but defense-in-depth is cheap here).
        """
        row = get_pipeline_run(run_id)
        if not row or not row.get("output_path"):
            raise NotFoundException(f"pipeline run {run_id} not found")
        path = Path(row["output_path"]).resolve()
        root = (TUSK_DIR / "pipeline_runs").resolve()
        try:
            path.relative_to(root)
        except ValueError:
            raise NotFoundException(f"pipeline run {run_id} output is outside the runs directory")
        if not path.is_file():
            raise NotFoundException(f"pipeline run {run_id} output file is missing on disk")
        return File(path=path, filename=path.name, media_type="application/octet-stream")

    # ─────────────────────────────────────────────────────────
    # Create — built-in kinds (back-compat shapes)
    # ─────────────────────────────────────────────────────────

    @post("/jobs/backup")
    async def add_backup_job(self, request: Request, data: dict = Body()) -> dict:
        connection_id = data.get("connection_id")
        if not connection_id:
            return {"error": "connection_id is required"}
        backup_dir = (data.get("backup_dir") or "").strip() or None
        fmt = str(data.get("format") or "plain")
        if fmt not in ("plain", "custom", "directory"):
            return {"error": f"Invalid backup format: {fmt}"}
        try:
            keep_last = int(data.get("keep_last") or 0)
        except (TypeError, ValueError):
            return {"error": "keep_last must be an integer"}
        run_date = data.get("run_date")
        owner_id = _current_user_id(request)

        if run_date:
            try:
                dt = datetime.fromisoformat(run_date)
                job_id = add_backup_once(
                    connection_id=connection_id, run_date=dt, backup_dir=backup_dir, format=fmt,
                )
            except ValueError:
                return {"error": "Invalid date format. Use ISO format: YYYY-MM-DDTHH:MM:SS"}
        else:
            job_id = add_backup_schedule(
                connection_id=connection_id,
                hour=data.get("hour", 2),
                minute=data.get("minute", 0),
                day_of_week=data.get("day_of_week", "*"),
                backup_dir=backup_dir,
                owner_id=owner_id,
                format=fmt,
                keep_last=keep_last or None,
            )

        return {"success": True, "job_id": job_id}

    @post("/jobs/vacuum")
    async def add_vacuum_job(self, request: Request, data: dict = Body()) -> dict:
        connection_id = data.get("connection_id")
        if not connection_id:
            return {"error": "connection_id is required"}
        full = data.get("full", False)
        run_date = data.get("run_date")
        owner_id = _current_user_id(request)

        if run_date:
            try:
                dt = datetime.fromisoformat(run_date)
                job_id = add_vacuum_once(connection_id=connection_id, run_date=dt, full=full)
            except ValueError:
                return {"error": "Invalid date format. Use ISO format: YYYY-MM-DDTHH:MM:SS"}
        else:
            job_id = add_vacuum_schedule(
                connection_id=connection_id,
                hour=data.get("hour", 3),
                minute=data.get("minute", 0),
                day_of_week=data.get("day_of_week", "sun"),
                full=full,
                owner_id=owner_id,
            )

        return {"success": True, "job_id": job_id}

    @post("/jobs/schema_watch")
    async def add_schema_watch_job(self, request: Request, data: dict = Body()) -> dict:
        connection_id = data.get("connection_id")
        if not connection_id:
            return {"error": "connection_id is required"}
        job_id = add_schema_watch_schedule(
            connection_id=connection_id,
            hour=data.get("hour", 6),
            minute=data.get("minute", 0),
            day_of_week=data.get("day_of_week", "*"),
            owner_id=_current_user_id(request),
        )
        return {"success": True, "job_id": job_id}

    @post("/jobs/analyze")
    async def add_analyze_job(self, request: Request, data: dict = Body()) -> dict:
        connection_id = data.get("connection_id")
        if not connection_id:
            return {"error": "connection_id is required"}
        run_date = data.get("run_date")
        owner_id = _current_user_id(request)

        if run_date:
            try:
                dt = datetime.fromisoformat(run_date)
                job_id = add_analyze_once(connection_id=connection_id, run_date=dt)
            except ValueError:
                return {"error": "Invalid date format. Use ISO format: YYYY-MM-DDTHH:MM:SS"}
        else:
            job_id = add_analyze_schedule(
                connection_id=connection_id,
                hour=data.get("hour", 4),
                minute=data.get("minute", 0),
                owner_id=owner_id,
            )

        return {"success": True, "job_id": job_id}

    # ─────────────────────────────────────────────────────────
    # Create — new generic kinds
    # ─────────────────────────────────────────────────────────

    @post("/jobs/query")
    async def add_query_job_endpoint(self, request: Request, data: dict = Body()) -> dict:
        name = data.get("name")
        connection_id = data.get("connection_id")
        sql = data.get("sql")
        trigger = data.get("trigger") or {}
        save_results_as = data.get("save_results_as")
        if not (name and connection_id and sql and trigger):
            return {"error": "name, connection_id, sql and trigger are required"}
        try:
            job_id = add_query_job(
                name=name,
                connection_id=connection_id,
                sql=sql,
                trigger=trigger,
                save_results_as=save_results_as,
                owner_id=_current_user_id(request),
            )
        except ValueError as e:
            return {"error": str(e)}
        return {"success": True, "job_id": job_id}

    @post("/jobs/pipeline")
    async def add_pipeline_job_endpoint(self, request: Request, data: dict = Body()) -> dict:
        name = data.get("name")
        pipeline_id = data.get("pipeline_id")
        trigger = data.get("trigger") or {}
        workspace = data.get("workspace") or "default"
        if not (name and pipeline_id and trigger):
            return {"error": "name, pipeline_id and trigger are required"}
        try:
            job_id = add_pipeline_job(
                name=name,
                pipeline_id=pipeline_id,
                trigger=trigger,
                workspace=workspace,
                owner_id=_current_user_id(request),
            )
        except ValueError as e:
            return {"error": str(e)}
        return {"success": True, "job_id": job_id}

    @post("/jobs/plugin")
    async def add_plugin_job_endpoint(self, request: Request, data: dict = Body()) -> dict:
        name = data.get("name")
        plugin_id = data.get("plugin_id")
        kind = data.get("kind")
        payload = data.get("payload") or {}
        trigger = data.get("trigger") or {}
        if not (name and plugin_id and kind and trigger):
            return {"error": "name, plugin_id, kind and trigger are required"}
        try:
            job_id = add_plugin_job(
                name=name,
                plugin_id=plugin_id,
                kind=kind,
                payload=payload,
                trigger=trigger,
                owner_id=_current_user_id(request),
            )
        except ValueError as e:
            return {"error": str(e)}
        return {"success": True, "job_id": job_id}

    # ─────────────────────────────────────────────────────────
    # Mutations
    # ─────────────────────────────────────────────────────────

    @delete("/jobs/{job_id:str}", status_code=200)
    async def remove_job(self, job_id: str) -> dict:
        success = remove_schedule(job_id)
        return {"success": success}

    @post("/jobs/{job_id:str}/pause")
    async def pause_job(self, job_id: str) -> dict:
        scheduler = get_scheduler()
        success = scheduler.pause_job(job_id)
        return {"success": success}

    @post("/jobs/{job_id:str}/resume")
    async def resume_job(self, job_id: str) -> dict:
        scheduler = get_scheduler()
        success = scheduler.resume_job(job_id)
        return {"success": success}

    @post("/jobs/{job_id:str}/run")
    async def run_job_now(self, job_id: str) -> dict:
        scheduler = get_scheduler()
        success = scheduler.run_job_now(job_id)
        return {"success": success}

    @put("/jobs/{job_id:str}/trigger")
    async def update_trigger(self, job_id: str, data: dict = Body()) -> dict:
        """Replace the trigger for an existing job (B9 in 0.4.26).

        Body: ``{"trigger": {"type": "cron", "cron": "..."}}`` or interval.
        Returns ``{"success": bool}``.
        """
        trigger = data.get("trigger")
        if not isinstance(trigger, dict) or "type" not in trigger:
            return {"error": "trigger object with `type` is required"}
        try:
            ok = set_trigger(job_id, trigger)
        except ValueError as e:
            return {"error": str(e)}
        if not ok:
            return {"error": "job not found or could not be updated"}
        return {"success": True}
