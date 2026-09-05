"""Schema Watch API: run a check now, read the history, HTMX panel for the
Schema page. The scheduled kind lives in core/scheduled_tasks.py."""

from __future__ import annotations

from litestar import Controller, Request, get, post
from litestar.response import Template

from tusk.core.connection import get_connection
from tusk.core import schema_watch as sw
from tusk.studio.routes.base import _current_user_id


class SchemaWatchController(Controller):
    path = "/api/schema-watch"
    tags = ["schema-watch"]

    @post("/{connection_id:str}/run")
    async def run_now(self, request: Request, connection_id: str) -> dict:
        """Take a snapshot now and diff it against the last one."""
        if get_connection(connection_id) is None:
            return {"error": "Connection not found"}
        try:
            out = await sw.run_watch(connection_id)
        except (ValueError, RuntimeError) as e:
            return {"error": str(e)}
        from tusk.core.auth import log_audit

        log_audit(
            "schema_watch.run",
            user_id=_current_user_id(request) or None,
            resource=connection_id,
            details=out["summary"],
            ip_address=request.client.host if request.client else None,
        )
        return out

    @get("/{connection_id:str}/status")
    async def status(self, connection_id: str) -> dict:
        """Latest snapshot metadata + how many changes were recorded."""
        latest = sw.latest_snapshot(connection_id)
        changes = sw.list_changes(connection_id, limit=1)
        return {
            "connection_id": connection_id,
            "watched": latest is not None,
            "last_snapshot_at": latest["taken_at"] if latest else None,
            "table_count": latest["table_count"] if latest else None,
            "column_count": latest["column_count"] if latest else None,
            "last_change_at": changes[0]["detected_at"] if changes else None,
            "last_change_summary": changes[0]["summary"] if changes else None,
        }

    @get("/{connection_id:str}/changes")
    async def changes(self, connection_id: str, days: int = 30, limit: int = 50) -> dict:
        """Recorded schema changes, newest first."""
        return {
            "connection_id": connection_id,
            "changes": sw.list_changes(connection_id, since=sw.since_days(days), limit=max(1, min(int(limit), 200))),
        }

    @get("/{connection_id:str}/panel")
    async def panel(self, connection_id: str) -> Template:
        """HTMX partial for the Schema page."""
        latest = sw.latest_snapshot(connection_id)
        changes = sw.list_changes(connection_id, limit=10)
        return Template(
            "partials/schema_watch_panel.html",
            context={
                "connection_id": connection_id,
                "latest": latest,
                "changes": changes,
            },
        )
