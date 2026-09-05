"""Tusk BI Plugin - Dashboards, charts, and analytics"""

from pathlib import Path
from tusk.plugins.base import TuskPlugin

# Built into TuskData since 0.4.36; the version is the core version.
try:
    from tusk import __version__
except Exception:  # pragma: no cover - import-time edge cases
    __version__ = "0.0.0+dev"


class BIPlugin(TuskPlugin):
    """Business Intelligence and analytics for Tusk."""

    @property
    def name(self) -> str:
        return "tusk-bi"

    @property
    def version(self) -> str:
        return __version__

    @property
    def description(self) -> str:
        return "Dashboards, charts, and analytics"

    @property
    def tab_id(self) -> str:
        return "bi"

    @property
    def tab_label(self) -> str:
        return "Analytics"

    @property
    def tab_icon(self) -> str:
        return "bar-chart-3"

    @property
    def requires_storage(self) -> bool:
        return True

    def get_route_handlers(self) -> list:
        from tusk.bi.routes import BIPageController, BIAPIController, EmbedAPIController, EmbedPageController
        return [BIPageController, BIAPIController, EmbedAPIController, EmbedPageController]

    def get_templates_path(self) -> Path | None:
        return Path(__file__).parent / "templates" / "bi"

    def get_static_path(self) -> Path | None:
        return Path(__file__).parent / "static" / "bi"

    def get_datasets(self) -> list[dict]:
        return [
            {
                "name": "bi_saved_queries",
                "description": "Saved BI queries",
                "table": "saved_queries",
            },
            {
                "name": "bi_dashboards",
                "description": "Dashboard definitions",
                "table": "dashboards",
            },
            {
                "name": "bi_snapshots",
                "description": "Query result snapshots",
                "table": "snapshots",
            },
        ]

    def get_cli_commands(self) -> dict[str, callable]:
        from tusk.bi.cli import handle_bi_cli
        return {"bi": handle_bi_cli}

    def get_notification_events(self) -> list[dict]:
        return [
            {"event_key": "bi.threshold.crossed", "label": "Threshold Crossed", "description": "A dashboard metric crossed its threshold"},
            {"event_key": "bi.schedule.failed", "label": "Schedule Failed", "description": "A scheduled query execution failed"},
        ]

    async def on_startup(self) -> None:
        from tusk.bi.db import init_db, discover_plugin_sources
        init_db()
        discover_plugin_sources()

        from tusk.bi.prebuilt import ensure_prebuilt_dashboards
        ensure_prebuilt_dashboards()

        from tusk.bi.scheduler import register_snapshot_jobs
        register_snapshot_jobs()

        # Register cleanup job for expired embed tokens
        try:
            from tusk.core.scheduler import get_scheduler
            from tusk.bi.db import delete_expired_embed_tokens
            scheduler = get_scheduler()
            scheduler.add_interval_job(
                delete_expired_embed_tokens,
                job_id="bi_embed_token_cleanup",
                name="Cleanup expired BI embed tokens",
                hours=1,
            )
        except Exception:
            pass

    async def on_shutdown(self) -> None:
        pass
