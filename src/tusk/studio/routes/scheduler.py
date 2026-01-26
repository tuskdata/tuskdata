"""Scheduler API routes"""

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
    remove_schedule,
)


class SchedulerController(Controller):
    """REST API for scheduled tasks"""

    path = "/api/scheduler"

    @get("/jobs")
    async def list_jobs(self) -> dict:
        """List all scheduled jobs"""
        scheduler = get_scheduler()
        jobs = scheduler.get_jobs()
        return {
            "jobs": [
                {
                    "id": job.id,
                    "name": job.name,
                    "trigger": job.trigger,
                    "schedule": job.schedule,
                    "next_run": job.next_run,
                    "enabled": job.enabled,
                }
                for job in jobs
            ]
        }

    @post("/jobs/backup")
    async def add_backup_job(self, data: dict = Body()) -> dict:
        """Add a scheduled backup job (cron or one-time)"""
        connection_id = data.get("connection_id")
        if not connection_id:
            return {"error": "connection_id is required"}

        backup_dir = data.get("backup_dir")
        run_date = data.get("run_date")  # ISO format: "2024-03-15T10:30:00"

        if run_date:
            # One-time job at specific date
            try:
                dt = datetime.fromisoformat(run_date)
                job_id = add_backup_once(
                    connection_id=connection_id,
                    run_date=dt,
                    backup_dir=backup_dir,
                )
            except ValueError:
                return {"error": "Invalid date format. Use ISO format: YYYY-MM-DDTHH:MM:SS"}
        else:
            # Recurring cron job
            hour = data.get("hour", 2)
            minute = data.get("minute", 0)
            day_of_week = data.get("day_of_week", "*")

            job_id = add_backup_schedule(
                connection_id=connection_id,
                hour=hour,
                minute=minute,
                day_of_week=day_of_week,
                backup_dir=backup_dir,
            )

        return {"success": True, "job_id": job_id}

    @post("/jobs/vacuum")
    async def add_vacuum_job(self, data: dict = Body()) -> dict:
        """Add a scheduled VACUUM job (cron or one-time)"""
        connection_id = data.get("connection_id")
        if not connection_id:
            return {"error": "connection_id is required"}

        full = data.get("full", False)
        run_date = data.get("run_date")

        if run_date:
            try:
                dt = datetime.fromisoformat(run_date)
                job_id = add_vacuum_once(
                    connection_id=connection_id,
                    run_date=dt,
                    full=full,
                )
            except ValueError:
                return {"error": "Invalid date format. Use ISO format: YYYY-MM-DDTHH:MM:SS"}
        else:
            hour = data.get("hour", 3)
            minute = data.get("minute", 0)
            day_of_week = data.get("day_of_week", "sun")

            job_id = add_vacuum_schedule(
                connection_id=connection_id,
                hour=hour,
                minute=minute,
                day_of_week=day_of_week,
                full=full,
            )

        return {"success": True, "job_id": job_id}

    @post("/jobs/analyze")
    async def add_analyze_job(self, data: dict = Body()) -> dict:
        """Add a scheduled ANALYZE job (cron or one-time)"""
        connection_id = data.get("connection_id")
        if not connection_id:
            return {"error": "connection_id is required"}

        run_date = data.get("run_date")

        if run_date:
            try:
                dt = datetime.fromisoformat(run_date)
                job_id = add_analyze_once(
                    connection_id=connection_id,
                    run_date=dt,
                )
            except ValueError:
                return {"error": "Invalid date format. Use ISO format: YYYY-MM-DDTHH:MM:SS"}
        else:
            hour = data.get("hour", 4)
            minute = data.get("minute", 0)

            job_id = add_analyze_schedule(
                connection_id=connection_id,
                hour=hour,
                minute=minute,
            )

        return {"success": True, "job_id": job_id}

    @delete("/jobs/{job_id:str}", status_code=200)
    async def remove_job(self, job_id: str) -> dict:
        """Remove a scheduled job"""
        success = remove_schedule(job_id)
        return {"success": success}

    @post("/jobs/{job_id:str}/pause")
    async def pause_job(self, job_id: str) -> dict:
        """Pause a scheduled job"""
        scheduler = get_scheduler()
        success = scheduler.pause_job(job_id)
        return {"success": success}

    @post("/jobs/{job_id:str}/resume")
    async def resume_job(self, job_id: str) -> dict:
        """Resume a paused job"""
        scheduler = get_scheduler()
        success = scheduler.resume_job(job_id)
        return {"success": success}

    @post("/jobs/{job_id:str}/run")
    async def run_job_now(self, job_id: str) -> dict:
        """Trigger a job to run immediately"""
        scheduler = get_scheduler()
        success = scheduler.run_job_now(job_id)
        return {"success": success}
