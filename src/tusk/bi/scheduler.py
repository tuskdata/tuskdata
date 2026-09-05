"""Snapshot scheduler for automated query execution"""

import asyncio
import json
from datetime import datetime, timedelta

import structlog
log = structlog.get_logger()


def _parse_cron_field(field: str, min_val: int, max_val: int) -> list[int]:
    """Parse a single cron field into a list of values."""
    if field == "*":
        return list(range(min_val, max_val + 1))

    values = []
    for part in field.split(","):
        part = part.strip()
        if "/" in part:
            base, step = part.split("/", 1)
            step = int(step)
            if base == "*":
                start = min_val
            else:
                start = int(base)
            values.extend(range(start, max_val + 1, step))
        elif "-" in part:
            start, end = part.split("-", 1)
            values.extend(range(int(start), int(end) + 1))
        else:
            values.append(int(part))

    return sorted(set(v for v in values if min_val <= v <= max_val))


def parse_cron(expr: str) -> dict:
    """Parse a cron expression into its components.

    Supports: minute hour day month weekday
    Example: "*/15 * * * *" -> every 15 minutes

    Returns dict with keys: minute, hour, day, month, weekday
    """
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {expr} (expected 5 fields)")

    return {
        "minute": _parse_cron_field(parts[0], 0, 59),
        "hour": _parse_cron_field(parts[1], 0, 23),
        "day": _parse_cron_field(parts[2], 1, 31),
        "month": _parse_cron_field(parts[3], 1, 12),
        "weekday": _parse_cron_field(parts[4], 0, 6),
    }


def calculate_next_run(cron_expr: str, after: datetime | None = None) -> datetime:
    """Calculate the next run time for a cron expression.

    Args:
        cron_expr: Cron expression string
        after: Start time (default: now)

    Returns:
        Next datetime when the cron should fire
    """
    cron = parse_cron(cron_expr)
    now = after or datetime.now()
    candidate = now.replace(second=0, microsecond=0) + timedelta(minutes=1)

    # Search up to 366 days ahead
    for _ in range(366 * 24 * 60):
        if (
            candidate.minute in cron["minute"]
            and candidate.hour in cron["hour"]
            and candidate.day in cron["day"]
            and candidate.month in cron["month"]
            and candidate.weekday() in cron["weekday"]
        ):
            return candidate
        candidate += timedelta(minutes=1)

    # Fallback: 1 hour from now
    return now + timedelta(hours=1)


class SnapshotScheduler:
    """Background scheduler for query snapshots."""

    def __init__(self):
        self._running = False

    async def run(self) -> None:
        """Main loop: check due schedules every 30 seconds."""
        self._running = True
        log.info("Snapshot scheduler started")

        while self._running:
            try:
                await self._check_schedules()
            except Exception as e:
                log.error("Scheduler error", error=str(e))

            await asyncio.sleep(30)

    async def _check_schedules(self) -> None:
        """Check for due schedules and execute them."""
        from tusk.bi.db import get_schedules

        schedules = get_schedules(enabled_only=True)
        now = datetime.now()

        for schedule in schedules:
            next_run = schedule.get("next_run_at")
            if not next_run:
                # First run — calculate next run time
                next_time = calculate_next_run(schedule["cron_expr"])
                from tusk.bi.db import update_schedule_run
                update_schedule_run(schedule["id"], next_time.isoformat())
                continue

            try:
                next_dt = datetime.fromisoformat(next_run)
            except (ValueError, TypeError):
                continue

            if now >= next_dt:
                await self._execute_scheduled(schedule)

    async def _execute_scheduled(self, schedule: dict) -> None:
        """Execute a scheduled query and save snapshot."""
        from tusk.bi.db import (
            get_saved_query,
            get_data_source,
            save_snapshot,
            rotate_snapshots,
            update_schedule_run,
        )
        from tusk.bi.engine import BIQueryEngine

        query_id = schedule["query_id"]
        query = get_saved_query(query_id)
        if not query:
            log.warning("Scheduled query not found", query_id=query_id)
            return

        source = get_data_source(query["source_id"])
        if not source:
            log.warning("Data source not found", source_id=query["source_id"])
            return

        engine = BIQueryEngine()

        try:
            result = engine.execute(
                source_type=source["source_type"],
                connection_ref=source["connection_ref"],
                sql=query["sql"],
            )

            # Extract aggregate value for sparklines (first numeric value in first row)
            value = None
            if result["rows"]:
                for v in result["rows"][0]:
                    try:
                        value = float(v)
                        break
                    except (ValueError, TypeError):
                        continue

            save_snapshot(
                query_id=query_id,
                row_count=result["row_count"],
                data=json.dumps(result),
                value=value,
            )

            # Rotate old snapshots
            max_keep = schedule.get("max_snapshots", 100)
            rotate_snapshots(query_id, max_keep)

            log.info("Snapshot saved", query_id=query_id, rows=result["row_count"])

        except Exception as e:
            log.error("Snapshot execution failed", query_id=query_id, error=str(e))

        # Calculate and save next run time
        next_time = calculate_next_run(schedule["cron_expr"])
        update_schedule_run(schedule["id"], next_time.isoformat())

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        log.info("Snapshot scheduler stopped")


def register_snapshot_jobs() -> None:
    """Register snapshot schedules as APScheduler jobs on the Tusk scheduler."""
    try:
        from tusk.core.scheduler import get_scheduler
        from tusk.bi.db import get_schedules

        scheduler = get_scheduler()
        schedules = get_schedules(enabled_only=True)

        for sched in schedules:
            try:
                cron = parse_cron(sched["cron_expr"])
            except ValueError:
                continue

            job_id = f"bi_snapshot_{sched['id']}"

            def make_job(s=sched):
                def run():
                    _execute_snapshot_sync(s)
                return run

            # Convert cron fields to APScheduler format
            minute = ",".join(str(m) for m in cron["minute"])
            hour = ",".join(str(h) for h in cron["hour"])
            day = ",".join(str(d) for d in cron["day"])
            month = ",".join(str(m) for m in cron["month"])
            day_of_week = ",".join(str(d) for d in cron["weekday"])

            scheduler.add_cron_job(
                make_job(),
                job_id=job_id,
                name=f"BI Snapshot: {sched.get('query_name', sched['query_id'])}",
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
            )

        if schedules:
            log.info("Registered BI snapshot jobs", count=len(schedules))
    except Exception as e:
        log.warning("Failed to register BI snapshot jobs", error=str(e))


def _execute_snapshot_sync(schedule: dict) -> None:
    """Execute a snapshot query synchronously (called by APScheduler)."""
    from tusk.bi.db import (
        get_saved_query, get_data_source,
        save_snapshot, rotate_snapshots, update_schedule_run,
    )
    from tusk.bi.engine import BIQueryEngine

    query = get_saved_query(schedule["query_id"])
    if not query:
        return

    source = get_data_source(query["source_id"])
    if not source:
        return

    engine = BIQueryEngine()
    try:
        result = engine.execute(
            source_type=source["source_type"],
            connection_ref=source["connection_ref"],
            sql=query["sql"],
            limit=1000,
        )

        value = None
        if result.get("rows") and result["rows"][0]:
            for cell in result["rows"][0]:
                try:
                    value = float(cell)
                    break
                except (ValueError, TypeError):
                    continue

        save_snapshot(
            query_id=schedule["query_id"],
            row_count=result.get("row_count", 0),
            data=json.dumps(result),
            value=value,
        )
        rotate_snapshots(schedule["query_id"], schedule.get("max_snapshots", 100))

        # Check threshold rules on widgets using this query
        if value is not None:
            _check_thresholds(schedule["query_id"], value, query)

        next_time = calculate_next_run(schedule["cron_expr"])
        update_schedule_run(schedule["id"], next_time.isoformat())

        log.info("Snapshot saved", query_id=schedule["query_id"])
    except Exception as e:
        log.error("Snapshot failed", query_id=schedule["query_id"], error=str(e))
        # Send notification if available
        try:
            from tusk.core.notifications import get_notification_service
            svc = get_notification_service()
            svc.send(
                "bi.schedule.failed",
                f"Scheduled query '{query.get('name', schedule['query_id'])}' failed: {e}",
                title="BI Schedule Failed", icon="alert-triangle", variant="error",
            )
        except Exception:
            pass


def _check_thresholds(query_id: int, value: float, query: dict) -> None:
    """Check if a snapshot value crosses any widget threshold rules.

    Threshold rules in widget config: [{"operator": ">", "value": 100, "event": "bi.threshold.crossed"}]
    Supported operators: >, <, >=, <=, ==, !=
    """
    from tusk.bi.db import get_widget_thresholds

    widget_thresholds = get_widget_thresholds(query_id)
    if not widget_thresholds:
        return

    op_funcs = {
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
    }

    for wt in widget_thresholds:
        for rule in wt["rules"]:
            operator = rule.get("operator", ">")
            threshold_value = rule.get("value")
            event = rule.get("event", "bi.threshold.crossed")

            if threshold_value is None:
                continue

            try:
                threshold_float = float(threshold_value)
            except (ValueError, TypeError):
                continue

            check_fn = op_funcs.get(operator)
            if not check_fn:
                continue

            if check_fn(value, threshold_float):
                query_name = query.get("name", str(query_id))
                log.warning(
                    "Threshold crossed",
                    query_id=query_id,
                    value=value,
                    operator=operator,
                    threshold=threshold_float,
                    widget_id=wt["widget_id"],
                )
                try:
                    from tusk.core.notifications import get_notification_service
                    svc = get_notification_service()
                    svc.send(
                        event,
                        f"Query '{query_name}': value {value} {operator} {threshold_float}",
                        title="BI Threshold Alert",
                        icon="alert-triangle",
                        variant="warning",
                    )
                except Exception:
                    pass
