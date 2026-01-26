"""Scheduled task implementations"""

import structlog
from datetime import datetime
from pathlib import Path

from tusk.core.connection import list_connections, get_connection
from tusk.core.scheduler import get_scheduler

log = structlog.get_logger()


async def scheduled_backup_task(connection_id: str, backup_dir: str | None = None):
    """Run a scheduled backup for a PostgreSQL connection"""
    from tusk.admin.backup import BackupService

    config = get_connection(connection_id)
    if not config:
        log.warning("scheduled_backup_skipped", reason="connection_not_found", connection_id=connection_id)
        return

    if config.type != "postgres":
        log.warning("scheduled_backup_skipped", reason="not_postgres", connection_id=connection_id)
        return

    try:
        backup_service = BackupService()
        result = await backup_service.create_backup(config, backup_dir)

        if result.get("success"):
            log.info(
                "scheduled_backup_completed",
                connection_id=connection_id,
                connection_name=config.name,
                backup_file=result.get("filename"),
            )
        else:
            log.error(
                "scheduled_backup_failed",
                connection_id=connection_id,
                error=result.get("error"),
            )
    except Exception as e:
        log.error("scheduled_backup_error", connection_id=connection_id, error=str(e))


async def scheduled_vacuum_task(connection_id: str, full: bool = False):
    """Run VACUUM on all tables for a PostgreSQL connection"""
    from tusk.engines import postgres

    config = get_connection(connection_id)
    if not config:
        log.warning("scheduled_vacuum_skipped", reason="connection_not_found", connection_id=connection_id)
        return

    if config.type != "postgres":
        log.warning("scheduled_vacuum_skipped", reason="not_postgres", connection_id=connection_id)
        return

    try:
        # Get all tables
        schema = await postgres.get_schema(config)
        tables = []
        for schema_name, schema_tables in schema.items():
            if schema_name in ("information_schema", "pg_catalog"):
                continue
            for table_name in schema_tables.keys():
                tables.append(f'"{schema_name}"."{table_name}"')

        # Run VACUUM on each table
        vacuum_cmd = "VACUUM FULL ANALYZE" if full else "VACUUM ANALYZE"
        success_count = 0
        error_count = 0

        for table in tables:
            try:
                await postgres.execute_query(config, f"{vacuum_cmd} {table}")
                success_count += 1
            except Exception as e:
                log.warning("vacuum_table_failed", table=table, error=str(e))
                error_count += 1

        log.info(
            "scheduled_vacuum_completed",
            connection_id=connection_id,
            connection_name=config.name,
            full=full,
            success_count=success_count,
            error_count=error_count,
        )

    except Exception as e:
        log.error("scheduled_vacuum_error", connection_id=connection_id, error=str(e))


async def scheduled_analyze_task(connection_id: str):
    """Run ANALYZE on all tables for a PostgreSQL connection"""
    from tusk.engines import postgres

    config = get_connection(connection_id)
    if not config:
        log.warning("scheduled_analyze_skipped", reason="connection_not_found", connection_id=connection_id)
        return

    if config.type != "postgres":
        return

    try:
        await postgres.execute_query(config, "ANALYZE")
        log.info("scheduled_analyze_completed", connection_id=connection_id, connection_name=config.name)
    except Exception as e:
        log.error("scheduled_analyze_error", connection_id=connection_id, error=str(e))


def setup_default_schedules():
    """Setup default scheduled tasks (called on app startup)"""
    # This is a placeholder - actual schedules would be configured via UI or config
    # For now, we just initialize the scheduler
    scheduler = get_scheduler()
    scheduler.start()
    log.info("default_schedules_initialized")


# Helper functions for adding scheduled tasks

def add_backup_schedule(
    connection_id: str,
    hour: int = 2,
    minute: int = 0,
    day_of_week: str = "*",
    backup_dir: str | None = None,
) -> str:
    """Add a scheduled backup for a connection"""
    scheduler = get_scheduler()
    job_id = f"backup_{connection_id}"

    async def task():
        await scheduled_backup_task(connection_id, backup_dir)

    scheduler.add_cron_job(
        func=task,
        job_id=job_id,
        name=f"Backup {connection_id}",
        hour=hour,
        minute=minute,
        day_of_week=day_of_week,
    )

    return job_id


def add_vacuum_schedule(
    connection_id: str,
    hour: int = 3,
    minute: int = 0,
    day_of_week: str = "sun",
    full: bool = False,
) -> str:
    """Add a scheduled VACUUM for a connection"""
    scheduler = get_scheduler()
    job_id = f"vacuum_{connection_id}"

    async def task():
        await scheduled_vacuum_task(connection_id, full=full)

    scheduler.add_cron_job(
        func=task,
        job_id=job_id,
        name=f"VACUUM {connection_id}",
        hour=hour,
        minute=minute,
        day_of_week=day_of_week,
    )

    return job_id


def add_analyze_schedule(
    connection_id: str,
    hour: int = 4,
    minute: int = 0,
) -> str:
    """Add a daily ANALYZE schedule for a connection"""
    scheduler = get_scheduler()
    job_id = f"analyze_{connection_id}"

    async def task():
        await scheduled_analyze_task(connection_id)

    scheduler.add_cron_job(
        func=task,
        job_id=job_id,
        name=f"ANALYZE {connection_id}",
        hour=hour,
        minute=minute,
    )

    return job_id


def remove_schedule(job_id: str) -> bool:
    """Remove a scheduled task"""
    scheduler = get_scheduler()
    return scheduler.remove_job(job_id)


# One-time scheduled tasks (date trigger)

def add_backup_once(
    connection_id: str,
    run_date: datetime,
    backup_dir: str | None = None,
) -> str:
    """Schedule a one-time backup at a specific date/time"""
    scheduler = get_scheduler()
    job_id = f"backup_once_{connection_id}_{run_date.strftime('%Y%m%d%H%M')}"

    async def task():
        await scheduled_backup_task(connection_id, backup_dir)

    scheduler.add_date_job(
        func=task,
        job_id=job_id,
        name=f"Backup {connection_id} (once)",
        run_date=run_date,
    )

    return job_id


def add_vacuum_once(
    connection_id: str,
    run_date: datetime,
    full: bool = False,
) -> str:
    """Schedule a one-time VACUUM at a specific date/time"""
    scheduler = get_scheduler()
    job_id = f"vacuum_once_{connection_id}_{run_date.strftime('%Y%m%d%H%M')}"

    async def task():
        await scheduled_vacuum_task(connection_id, full=full)

    scheduler.add_date_job(
        func=task,
        job_id=job_id,
        name=f"VACUUM {connection_id} (once)",
        run_date=run_date,
    )

    return job_id


def add_analyze_once(
    connection_id: str,
    run_date: datetime,
) -> str:
    """Schedule a one-time ANALYZE at a specific date/time"""
    scheduler = get_scheduler()
    job_id = f"analyze_once_{connection_id}_{run_date.strftime('%Y%m%d%H%M')}"

    async def task():
        await scheduled_analyze_task(connection_id)

    scheduler.add_date_job(
        func=task,
        job_id=job_id,
        name=f"ANALYZE {connection_id} (once)",
        run_date=run_date,
    )

    return job_id
