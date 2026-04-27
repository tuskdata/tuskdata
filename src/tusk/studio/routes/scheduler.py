"""Scheduler API routes.

All endpoints are guarded by :func:`tusk.studio.routes.admin._check_admin_auth`
(audit finding #4 — scheduler is admin-only).
"""

from datetime import datetime

from litestar import Controller, get, post, delete
from litestar.params import Body

from tusk.core.scheduler import get_scheduler
from tusk.core.scheduled_tasks import (
    add_backup_schedule,
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
    remove_schedule,
)
from tusk.studio.routes.admin import _check_admin_auth


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

    # ─────────────────────────────────────────────────────────
    # Create — built-in kinds (back-compat shapes)
    # ─────────────────────────────────────────────────────────

    @post("/jobs/backup")
    async def add_backup_job(self, data: dict = Body()) -> dict:
        connection_id = data.get("connection_id")
        if not connection_id:
            return {"error": "connection_id is required"}
        backup_dir = data.get("backup_dir")
        run_date = data.get("run_date")

        if run_date:
            try:
                dt = datetime.fromisoformat(run_date)
                job_id = add_backup_once(connection_id=connection_id, run_date=dt, backup_dir=backup_dir)
            except ValueError:
                return {"error": "Invalid date format. Use ISO format: YYYY-MM-DDTHH:MM:SS"}
        else:
            job_id = add_backup_schedule(
                connection_id=connection_id,
                hour=data.get("hour", 2),
                minute=data.get("minute", 0),
                day_of_week=data.get("day_of_week", "*"),
                backup_dir=backup_dir,
            )

        return {"success": True, "job_id": job_id}

    @post("/jobs/vacuum")
    async def add_vacuum_job(self, data: dict = Body()) -> dict:
        connection_id = data.get("connection_id")
        if not connection_id:
            return {"error": "connection_id is required"}
        full = data.get("full", False)
        run_date = data.get("run_date")

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
            )

        return {"success": True, "job_id": job_id}

    @post("/jobs/analyze")
    async def add_analyze_job(self, data: dict = Body()) -> dict:
        connection_id = data.get("connection_id")
        if not connection_id:
            return {"error": "connection_id is required"}
        run_date = data.get("run_date")

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
            )

        return {"success": True, "job_id": job_id}

    # ─────────────────────────────────────────────────────────
    # Create — new generic kinds
    # ─────────────────────────────────────────────────────────

    @post("/jobs/query")
    async def add_query_job_endpoint(self, data: dict = Body()) -> dict:
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
            )
        except ValueError as e:
            return {"error": str(e)}
        return {"success": True, "job_id": job_id}

    @post("/jobs/pipeline")
    async def add_pipeline_job_endpoint(self, data: dict = Body()) -> dict:
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
            )
        except ValueError as e:
            return {"error": str(e)}
        return {"success": True, "job_id": job_id}

    @post("/jobs/plugin")
    async def add_plugin_job_endpoint(self, data: dict = Body()) -> dict:
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
