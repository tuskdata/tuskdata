"""Litestar application for Tusk Studio"""

import asyncio
import secrets
from pathlib import Path
from litestar import Litestar, Request, Response
from litestar.middleware.base import AbstractMiddleware
from litestar.static_files import StaticFilesConfig
from litestar.template import TemplateConfig
from litestar.config.compression import CompressionConfig
from litestar.types import ASGIApp, Receive, Scope, Send

from litestar.contrib.minijinja import MiniJinjaTemplateEngine

import os

from tusk.studio.routes import (
    PageController,
    APIController,
    AdminController,
    SettingsController,
    FilesController,
    DuckDBController,
    DataController,
    AuthController,
    UsersController,
    GroupsController,
    AuthSetupController,
    ProfileController,
    AuditLogController,
    SchedulerController,
    DownloadsController,
    NotificationPageController,
    NotificationAPIController,
    health_check,
)
from tusk.core.connection import load_connections_from_file
from tusk.core.logging import setup_logging, get_logger
from tusk.core.scheduler import get_scheduler
from tusk.plugins.registry import (
    discover_plugins,
    get_all_plugins,
    get_plugin_route_handlers,
)
from tusk.plugins.templates import (
    setup_plugin_templates,
    setup_plugin_statics,
    cleanup_plugin_statics,
)

# Paths
STUDIO_DIR = Path(__file__).parent
TEMPLATES_DIR = STUDIO_DIR / "templates"
STATIC_DIR = STUDIO_DIR / "static"

CSRF_COOKIE = "tusk_csrf"
CSRF_HEADER = "x-csrf-token"
# Paths exempt from CSRF (login needs to work without a token, health, static, etc.)
_CSRF_EXEMPT_PREFIXES = ("/static/", "/api/auth/login", "/api/auth/setup", "/api/auth/status", "/api/auth/config", "/health", "/api/ci/webhook", "/api/ci/sse/", "/bi/public/", "/embed/", "/api/embed/")
_STATE_CHANGING_METHODS = {"POST", "PUT", "DELETE", "PATCH"}


class CSRFMiddleware(AbstractMiddleware):
    """Double-submit cookie CSRF protection.

    - Sets a `tusk_csrf` cookie on every response if not present.
    - On POST/PUT/DELETE/PATCH, validates that the `X-CSRF-Token` header
      matches the cookie value.
    - HTMX is configured in base.html to send this header automatically.
    """

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        path = request.url.path
        method = request.method

        # Skip CSRF for exempt paths
        exempt = any(path.startswith(p) for p in _CSRF_EXEMPT_PREFIXES)

        if not exempt and method in _STATE_CHANGING_METHODS:
            cookie_token = request.cookies.get(CSRF_COOKIE)
            header_token = request.headers.get(CSRF_HEADER)

            if not cookie_token or not header_token or not secrets.compare_digest(cookie_token, header_token):
                response = Response(
                    content={"error": "CSRF token missing or invalid"},
                    status_code=403,
                )
                await response(scope, receive, send)
                return

        # Wrap send to add CSRF cookie if not present
        csrf_token = request.cookies.get(CSRF_COOKIE)

        async def send_with_csrf(message):
            if message["type"] == "http.response.start" and not csrf_token:
                headers = list(message.get("headers", []))
                new_token = secrets.token_urlsafe(32)
                cookie = f"{CSRF_COOKIE}={new_token}; Path=/; SameSite=Lax".encode()
                headers.append((b"set-cookie", cookie))
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_with_csrf)


def get_route_handlers() -> list:
    """Collect all route handlers including plugins"""
    # Core handlers
    handlers = [
        PageController,
        APIController,
        AdminController,
        SettingsController,
        FilesController,
        DuckDBController,
        DataController,
        AuthController,
        UsersController,
        GroupsController,
        AuthSetupController,
        ProfileController,
        AuditLogController,
        SchedulerController,
        DownloadsController,
        NotificationPageController,
        NotificationAPIController,
        health_check,
    ]

    # Add plugin handlers
    handlers.extend(get_plugin_route_handlers())

    return handlers


def on_startup() -> None:
    """Initialize logging, load connections, discover plugins, and start scheduler"""
    debug = os.environ.get("TUSK_DEBUG", "").lower() in ("1", "true", "yes")
    setup_logging(debug=debug)
    log = get_logger("studio")
    log.info("Starting Tusk Studio")

    # Load connections
    load_connections_from_file()
    log.info("Connections loaded")

    # Discover plugins
    plugins = discover_plugins()
    if plugins:
        log.info("Plugins discovered", count=len(plugins))

    # Call plugin startup hooks
    for plugin in get_all_plugins():
        try:
            # Check if there's already a running event loop
            try:
                loop = asyncio.get_running_loop()
                # If we're in a running loop, schedule the coroutine
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, plugin.on_startup())
                    future.result(timeout=30)
            except RuntimeError:
                # No running loop, we can use asyncio.run directly
                asyncio.run(plugin.on_startup())
            log.info("Plugin started", plugin=plugin.name)
        except Exception as e:
            log.error("Plugin startup failed", plugin=plugin.name, error=str(e))

    # Setup plugin templates and statics
    setup_plugin_templates(TEMPLATES_DIR)
    setup_plugin_statics(STATIC_DIR)

    # Start the task scheduler
    scheduler = get_scheduler()
    scheduler.start()
    log.info("Scheduler started")

    # Schedule session cleanup every hour
    try:
        from tusk.core.auth import cleanup_expired_sessions
        scheduler.add_interval_job(
            cleanup_expired_sessions,
            job_id="session_cleanup",
            name="Cleanup expired sessions",
            hours=1,
        )
        # Run once at startup too
        cleaned = cleanup_expired_sessions()
        if cleaned:
            log.info("Expired sessions cleaned at startup", count=cleaned)
    except Exception as e:
        log.warning("Failed to register session cleanup", error=str(e))

    # Schedule temp export file cleanup every 30 min
    import tempfile
    def cleanup_temp_exports():
        """Remove tusk_export_* files older than 30 minutes."""
        import time
        tmp_dir = Path(tempfile.gettempdir())
        cutoff = time.time() - 1800  # 30 min ago
        cleaned = 0
        for f in tmp_dir.glob("tusk_export_*"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    cleaned += 1
            except OSError:
                pass
        if cleaned:
            log.info("Cleaned temp export files", count=cleaned)

    scheduler.add_interval_job(
        cleanup_temp_exports,
        job_id="temp_export_cleanup",
        name="Cleanup temp export files",
        minutes=30,
    )

    # Register scheduled downloads
    try:
        from tusk.core.downloads import schedule_downloads
        schedule_downloads()
    except Exception as e:
        log.warning("Failed to register scheduled downloads", error=str(e))

    # Initialize notification system
    try:
        from tusk.core.notifications import get_notification_service
        svc = get_notification_service()
        svc.register_core_events()

        # Register plugin notification events
        for plugin in get_all_plugins():
            if hasattr(plugin, "get_notification_events"):
                for ev in plugin.get_notification_events():
                    svc.register_event(ev["event_key"], ev.get("plugin_id", plugin.name), ev["label"], ev.get("description", ""))

        # Retry failed notifications every 5 minutes
        scheduler.add_interval_job(
            svc.retry_failed,
            job_id="notification_retry",
            name="Retry failed notifications",
            minutes=5,
        )

        # Clean old in-app notifications daily
        def cleanup_old_notifications():
            svc.clear_in_app(older_than_days=7)
            svc.clear_history(older_than_days=30)

        scheduler.add_interval_job(
            cleanup_old_notifications,
            job_id="notification_cleanup",
            name="Cleanup old notifications",
            hours=24,
        )

        log.info("Notification system initialized")
    except Exception as e:
        log.warning("Failed to initialize notifications", error=str(e))


def on_shutdown() -> None:
    """Cleanup on shutdown"""
    log = get_logger("studio")

    # Call plugin shutdown hooks
    for plugin in get_all_plugins():
        try:
            try:
                loop = asyncio.get_running_loop()
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, plugin.on_shutdown())
                    future.result(timeout=30)
            except RuntimeError:
                asyncio.run(plugin.on_shutdown())
            log.info("Plugin stopped", plugin=plugin.name)
        except Exception as e:
            log.error("Plugin shutdown failed", plugin=plugin.name, error=str(e))

    # Close PostgreSQL connection pools
    try:
        from tusk.engines.postgres import close_pools
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, close_pools())
                future.result(timeout=10)
        except RuntimeError:
            asyncio.run(close_pools())
    except Exception as e:
        log.warning("Failed to close connection pools", error=str(e))

    # Cleanup plugin files
    cleanup_plugin_statics(STATIC_DIR)

    log.info("Tusk Studio stopped")


# Discover plugins before creating app (needed for route handlers)
discover_plugins()

app = Litestar(
    route_handlers=get_route_handlers(),
    template_config=TemplateConfig(
        directory=TEMPLATES_DIR,
        engine=MiniJinjaTemplateEngine,
    ),
    static_files_config=[
        StaticFilesConfig(
            directories=[STATIC_DIR],
            path="/static",
        )
    ],
    compression_config=CompressionConfig(
        backend="zstd",
        minimum_size=500,  # Compress responses larger than 500 bytes
    ),
    middleware=[CSRFMiddleware],
    on_startup=[on_startup],
    on_shutdown=[on_shutdown],
    debug=os.environ.get("TUSK_DEBUG", "").lower() in ("1", "true", "yes"),
)
