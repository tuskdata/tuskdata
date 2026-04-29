"""Scheduled tasks service using APScheduler"""

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.events import EVENT_JOB_ERROR
from datetime import datetime, timezone
from typing import Callable, Any
import msgspec

log = structlog.get_logger()


def _on_job_error(event) -> None:
    """APScheduler error listener — fires a `scheduler.job.error`
    notification so admins find out about failed jobs without grepping
    the logs.

    Wrapped in a try/except so a notification failure never bubbles back
    into APScheduler's executor (which would mark the listener bad and
    silently drop subsequent error events).
    """
    try:
        job_id = getattr(event, "job_id", "unknown")
        exc = getattr(event, "exception", None)
        traceback = getattr(event, "traceback", None) or ""

        # Truncate traceback so the in-app notification doesn't blow up.
        tb_short = ""
        if traceback:
            tb_short = traceback if len(traceback) <= 600 else traceback[:600] + "…"

        context = {
            "job_id": job_id,
            "error": str(exc) if exc else "unknown error",
            "traceback": tb_short,
        }

        from tusk.core.notifications import dispatch_event

        dispatch_event(
            "scheduler.job.error",
            context=context,
            title=f"Scheduled job failed: {job_id}",
            message=f"{job_id}: {exc}",
            variant="error",
            icon="alert-triangle",
        )
        log.warning("scheduler_job_failed", job_id=job_id, error=str(exc))
    except Exception as hook_err:
        # Never let a hook bug crash the scheduler.
        log.error("scheduler_error_hook_failed", error=str(hook_err))


class ScheduledJob(msgspec.Struct):
    """Scheduled job info"""
    id: str
    name: str
    trigger: str  # 'cron', 'interval', or 'date'
    schedule: str  # Human readable schedule
    next_run: str | None
    enabled: bool = True


class SchedulerService:
    """Service for managing scheduled tasks"""

    _instance: "SchedulerService | None" = None

    def __init__(self):
        self.scheduler = AsyncIOScheduler(
            jobstores={"default": MemoryJobStore()},
            job_defaults={
                "coalesce": True,  # Combine missed executions
                "max_instances": 1,  # Only one instance at a time
                "misfire_grace_time": 60 * 5,  # 5 minutes grace period
            },
        )
        # Wire the error listener so failed jobs surface as notifications.
        # Listener is registered before start so we never miss an early
        # failure.
        try:
            self.scheduler.add_listener(_on_job_error, EVENT_JOB_ERROR)
        except Exception as e:
            log.warning("scheduler_listener_failed", error=str(e))
        self._started = False

    # `_scheduler` alias kept for the /admin/health dashboard, which
    # introspects the underlying APScheduler instance.
    @property
    def _scheduler(self):
        return self.scheduler

    @classmethod
    def get_instance(cls) -> "SchedulerService":
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = SchedulerService()
        return cls._instance

    def start(self):
        """Start the scheduler"""
        if not self._started:
            self.scheduler.start()
            self._started = True
            log.info("scheduler_started")

    def shutdown(self):
        """Shutdown the scheduler"""
        if self._started:
            self.scheduler.shutdown(wait=False)
            self._started = False
            log.info("scheduler_shutdown")

    def add_cron_job(
        self,
        func: Callable,
        job_id: str,
        name: str,
        hour: int | str = "*",
        minute: int | str = 0,
        day_of_week: str = "*",
        day: int | str = "*",
        month: int | str = "*",
        **kwargs: Any,
    ) -> str:
        """Add a cron-based scheduled job"""
        trigger = CronTrigger(
            hour=hour,
            minute=minute,
            day_of_week=day_of_week,
            day=day,
            month=month,
        )

        self.scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            name=name,
            replace_existing=True,
            **kwargs,
        )

        log.info("cron_job_added", job_id=job_id, name=name)
        return job_id

    def add_interval_job(
        self,
        func: Callable,
        job_id: str,
        name: str,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        **kwargs: Any,
    ) -> str:
        """Add an interval-based scheduled job"""
        trigger = IntervalTrigger(
            hours=hours,
            minutes=minutes,
            seconds=seconds,
        )

        self.scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            name=name,
            replace_existing=True,
            **kwargs,
        )

        log.info("interval_job_added", job_id=job_id, name=name)
        return job_id

    def add_date_job(
        self,
        func: Callable,
        job_id: str,
        name: str,
        run_date: datetime,
        **kwargs: Any,
    ) -> str:
        """Add a one-time job to run at a specific date/time"""
        trigger = DateTrigger(run_date=run_date)

        self.scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            name=name,
            replace_existing=True,
            **kwargs,
        )

        log.info("date_job_added", job_id=job_id, name=name, run_date=run_date.isoformat())
        return job_id

    def remove_job(self, job_id: str) -> bool:
        """Remove a scheduled job"""
        try:
            self.scheduler.remove_job(job_id)
            log.info("job_removed", job_id=job_id)
            return True
        except Exception as e:
            log.warning("job_remove_failed", job_id=job_id, error=str(e))
            return False

    def pause_job(self, job_id: str) -> bool:
        """Pause a scheduled job"""
        try:
            self.scheduler.pause_job(job_id)
            log.info("job_paused", job_id=job_id)
            return True
        except Exception:
            return False

    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job"""
        try:
            self.scheduler.resume_job(job_id)
            log.info("job_resumed", job_id=job_id)
            return True
        except Exception:
            return False

    def get_jobs(self) -> list[ScheduledJob]:
        """Get all scheduled jobs"""
        jobs = []
        for job in self.scheduler.get_jobs():
            # Parse trigger info
            trigger_type = "unknown"
            schedule = str(job.trigger)

            if isinstance(job.trigger, CronTrigger):
                trigger_type = "cron"
                # Build human readable schedule
                fields = job.trigger.fields
                schedule = f"cron({fields})"
            elif isinstance(job.trigger, IntervalTrigger):
                trigger_type = "interval"
                schedule = f"every {job.trigger.interval}"
            elif isinstance(job.trigger, DateTrigger):
                trigger_type = "date"
                schedule = f"once at {job.trigger.run_date.strftime('%Y-%m-%d %H:%M')}"

            next_run = None
            if job.next_run_time:
                next_run = job.next_run_time.isoformat()

            jobs.append(
                ScheduledJob(
                    id=job.id,
                    name=job.name or job.id,
                    trigger=trigger_type,
                    schedule=schedule,
                    next_run=next_run,
                    enabled=job.next_run_time is not None,
                )
            )

        return jobs

    def run_job_now(self, job_id: str) -> bool:
        """Trigger a job to run immediately"""
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                job.modify(next_run_time=datetime.now(timezone.utc))
                log.info("job_triggered", job_id=job_id)
                return True
            return False
        except Exception as e:
            log.error("job_trigger_failed", job_id=job_id, error=str(e))
            return False


def get_scheduler() -> SchedulerService:
    """Get the global scheduler service"""
    return SchedulerService.get_instance()
