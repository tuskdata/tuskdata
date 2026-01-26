"""Scheduled tasks service using APScheduler"""

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from datetime import datetime
from typing import Callable, Any
import msgspec

log = structlog.get_logger()


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
        self._started = False

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
                job.modify(next_run_time=datetime.now())
                log.info("job_triggered", job_id=job_id)
                return True
            return False
        except Exception as e:
            log.error("job_trigger_failed", job_id=job_id, error=str(e))
            return False


def get_scheduler() -> SchedulerService:
    """Get the global scheduler service"""
    return SchedulerService.get_instance()
