"""Litestar application for Tusk Studio"""

import asyncio
import secrets
import shutil
from pathlib import Path
from litestar import Litestar, Request, Response
from litestar.middleware import ASGIMiddleware
from litestar.static_files import create_static_files_router
from litestar.template import TemplateConfig
from litestar.config.compression import CompressionConfig
from litestar.types import ASGIApp, Receive, Scope, Send
from litestar.enums import ScopeType

from litestar.plugins.minijinja import MiniJinjaTemplateEngine
from litestar_mcp import LitestarMCP, MCPConfig
from litestar.openapi import OpenAPIConfig

import tusk

import os

from tusk.studio.routes import (
    PageController,
    APIController,
    AdminController,
    HealthController,
    SettingsController,
    FilesController,
    DuckDBController,
    DataController,
    ExploreController,
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
    AICopilotController,
    AISettingsPageController,
    JobsController,
    MCPToolsController,
    health_check,
    metrics,
)
from tusk.core.connection import load_connections_from_file
from tusk.core.logging import setup_logging, get_logger, _correlation_id
from tusk.core.otel import init_otel
from tusk.core.scheduler import get_scheduler
from tusk.plugins.registry import (
    discover_plugins,
    get_all_plugins,
    get_plugin_route_handlers,
)
from tusk.plugins.templates import (
    setup_plugin_templates,
    setup_plugin_statics,
    PLUGIN_STATIC_DIR,
)

# Paths
STUDIO_DIR = Path(__file__).parent
TEMPLATES_DIR = STUDIO_DIR / "templates"
STATIC_DIR = STUDIO_DIR / "static"

CSRF_COOKIE = "tusk_csrf"
CSRF_HEADER = "x-csrf-token"
SESSION_COOKIE = "tusk_session"

# Default request timeout (seconds). A handler that doesn't return
# inside this budget gets cancelled and the caller sees 504. Tuned for
# SMB load — most read endpoints respond in <2s, exports + backups
# already returned 202 + job_id by then.
#
# Override per-path via _SLOW_PATH_PREFIXES below. Override at runtime
# via TUSK_REQUEST_TIMEOUT env var (number of seconds, or 0 to disable).
_DEFAULT_REQUEST_TIMEOUT_S = float(os.environ.get("TUSK_REQUEST_TIMEOUT", "60") or 60)

# A few endpoints legitimately need a longer budget. Mostly streaming
# downloads + long-running synchronous fallbacks that haven't been
# migrated to background jobs yet. Keep this list **small** — every
# entry is a deviation from the contract that the UI never hangs.
_SLOW_PATH_PREFIXES = {
    "/api/admin/": 300.0,            # admin reads can be slow on big DBs (pg_stat_activity etc.)
    "/api/data/export": 600.0,       # CSV/Parquet exports
    "/api/data/": 300.0,             # Ibis/Polars pipeline execute — cold-start can be 5-15s
                                     # and chained transforms can stack. CI runners are slow.
    "/api/downloads/": 600.0,        # file downloads
}

# Skip the timeout entirely for paths where it would do more harm than
# good (SSE / WebSocket-ish streams, embed pages serving long iframes).
_TIMEOUT_EXEMPT_PREFIXES = (
    "/static/",
    "/api/ci/sse/",
    "/health",
    "/metrics",
)


def _timeout_for(path: str) -> float:
    """Return the timeout in seconds for `path`, or 0 to disable."""
    if _DEFAULT_REQUEST_TIMEOUT_S <= 0:
        return 0.0
    if any(path.startswith(p) for p in _TIMEOUT_EXEMPT_PREFIXES):
        return 0.0
    for prefix, override in _SLOW_PATH_PREFIXES.items():
        if path.startswith(prefix):
            return override
    return _DEFAULT_REQUEST_TIMEOUT_S
# Paths exempt from CSRF (login needs to work without a token, health, static, etc.)
_CSRF_EXEMPT_PREFIXES = ("/static/", "/api/auth/login", "/api/auth/setup", "/api/auth/status", "/api/auth/config", "/health", "/api/ci/webhook", "/api/ci/sse/", "/bi/public/", "/embed/", "/api/embed/", "/mcp")
_STATE_CHANGING_METHODS = {"POST", "PUT", "DELETE", "PATCH"}

# Paths that don't require a session in multi-user mode.
# Anything outside this list is gated by SessionRequiredMiddleware.
_PUBLIC_PREFIXES = (
    "/static/",
    "/login",
    "/health",
    "/metrics",
    "/favicon.ico",
    "/api/auth/login",
    "/api/auth/logout",
    "/api/auth/status",
    "/api/auth/config",
    "/api/auth/setup",
    # External integrations carry their own auth (HMAC, signed token, etc.)
    "/api/ci/webhook",
    "/api/ci/sse/",
    "/bi/public/",
    "/embed/",
    "/api/embed/",
)


class RequestTimeoutMiddleware(ASGIMiddleware):
    """Bound every HTTP request to a max wall-clock budget.

    Last night's SSH-tunnel hang (see specs/bugs/2026-05-18-...) made
    the pod unresponsive without crashing it. A frozen handler ties
    up a worker until Granian's recycle kicks in (1h by default).
    A request-level timeout is the surgical version: cancel the
    handler, free the worker, return 504 to the caller.

    Budget defaults to 60s and is overridable via TUSK_REQUEST_TIMEOUT.
    Some routes get a longer budget (see _SLOW_PATH_PREFIXES); a few
    are exempt entirely (SSE, static, health). Set the env to 0 to
    disable.
    """

    # ASGIMiddleware filters scopes at the framework level — no more
    # `if scope["type"] != "http"` inside the handler.
    scopes = (ScopeType.HTTP,)

    async def handle(self, scope: Scope, receive: Receive, send: Send, next_app: ASGIApp) -> None:
        path = Request(scope).url.path
        timeout = _timeout_for(path)

        if timeout <= 0:
            await next_app(scope, receive, send)
            return

        # Track whether the downstream app started writing a response.
        # If it did, we can't cleanly send a 504 anymore — the caller
        # has bytes in flight. In that rare case we just log and let
        # the cancellation propagate.
        response_started = False

        async def tracking_send(message):
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await asyncio.wait_for(
                next_app(scope, receive, tracking_send),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            timeout_log = get_logger("studio.exceptions")
            timeout_log.error(
                "Request timeout exceeded",
                path=path,
                method=scope.get("method"),
                timeout_s=timeout,
            )
            if not response_started:
                body = b'{"error": "Request timeout exceeded"}'
                await send({
                    "type": "http.response.start",
                    "status": 504,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(body)).encode("ascii")),
                    ],
                })
                await send({"type": "http.response.body", "body": body})


class CSRFMiddleware(ASGIMiddleware):
    """Double-submit cookie CSRF protection.

    - Sets a `tusk_csrf` cookie on every response if not present.
    - On POST/PUT/DELETE/PATCH, validates that the `X-CSRF-Token` header
      matches the cookie value.
    - HTMX is configured in base.html to send this header automatically.
    """

    scopes = (ScopeType.HTTP,)

    async def handle(self, scope: Scope, receive: Receive, send: Send, next_app: ASGIApp) -> None:
        request = Request(scope)
        path = request.url.path
        method = request.method

        # Skip CSRF for exempt paths
        exempt = any(path.startswith(p) for p in _CSRF_EXEMPT_PREFIXES)

        if not exempt and method in _STATE_CHANGING_METHODS:
            cookie_token = request.cookies.get(CSRF_COOKIE)
            header_token = request.headers.get(CSRF_HEADER)

            if not cookie_token or not header_token or not secrets.compare_digest(cookie_token, header_token):
                # Emit the 403 directly via the ASGI send channel.
                # Wrapping a Litestar `Response` and calling it as
                # `await response(scope, receive, send)` only works for
                # ASGIApp instances — Response is not one, and the
                # implicit cast Litestar used to do internally was
                # dropped in 2.x, which produced a 500 instead of a
                # clean 403 here. See bugs/2026-05-19-csrf-middleware-500.md.
                body = b'{"error": "CSRF token missing or invalid"}'
                await send({
                    "type": "http.response.start",
                    "status": 403,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(body)).encode("ascii")),
                    ],
                })
                await send({"type": "http.response.body", "body": body})
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

        await next_app(scope, receive, send_with_csrf)


class CorrelationIDMiddleware(ASGIMiddleware):
    """Attach an `X-Correlation-ID` to every HTTP request/response.

    Reads the incoming `X-Correlation-ID` header (any value the caller
    sent) and falls back to a freshly generated 16-hex-char id when
    absent or empty. The id is stashed in the `_correlation_id`
    contextvar so the structlog processor in `core.logging` can attach
    it to every log line emitted while the request is in flight, then
    echoed back on the outgoing response so the caller can correlate
    server logs with client traces.
    """

    scopes = (ScopeType.HTTP,)

    async def handle(self, scope: Scope, receive: Receive, send: Send, next_app: ASGIApp) -> None:
        # Pull the header out of the raw ASGI scope (case-insensitive).
        incoming = ""
        for name, value in scope.get("headers") or ():
            if name.lower() == b"x-correlation-id":
                try:
                    incoming = value.decode("ascii", errors="replace").strip()
                except Exception:
                    incoming = ""
                break

        cid = incoming or secrets.token_hex(8)
        token = _correlation_id.set(cid)

        cid_bytes = cid.encode("ascii", errors="replace")

        async def send_with_correlation(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers") or [])
                # Drop any pre-existing header so we don't ship duplicates.
                headers = [(n, v) for (n, v) in headers if n.lower() != b"x-correlation-id"]
                headers.append((b"x-correlation-id", cid_bytes))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await next_app(scope, receive, send_with_correlation)
        finally:
            _correlation_id.reset(token)


class SessionRequiredMiddleware(ASGIMiddleware):
    """Require a valid session cookie in multi-user mode.

    Single-user mode is a no-op (anyone with network access is already
    trusted). In multi-user mode, every request outside the public
    allowlist must carry a `tusk_session` cookie matching a live
    session row, otherwise:

    - HTML/HTMX navigations are redirected to `/login?redirect=…`
    - JSON / API requests get a 401

    This was the v0.3.0 release-blocker: only `AdminController` and
    `ClusterController` had per-controller guards, so an unauthenticated
    request could still hit `/api/query`, `/api/scheduler/*`, file
    uploads, notification webhooks, etc.
    """

    scopes = (ScopeType.HTTP,)

    async def handle(self, scope: Scope, receive: Receive, send: Send, next_app: ASGIApp) -> None:
        # Single-user mode → no auth gate.
        from tusk.core.config import get_config
        if get_config().auth_mode != "multi":
            await next_app(scope, receive, send)
            return

        request = Request(scope)
        path = request.url.path

        if any(path.startswith(p) for p in _PUBLIC_PREFIXES):
            await next_app(scope, receive, send)
            return

        # Validate session.
        from tusk.core.auth import get_session, get_user_by_id
        session_id = request.cookies.get(SESSION_COOKIE)
        valid = False
        if session_id:
            session = get_session(session_id)
            if session:
                user = get_user_by_id(session.user_id)
                if user and user.is_active:
                    valid = True

        if valid:
            await next_app(scope, receive, send)
            return

        # Reject. HTMX boost / fetch get JSON; full nav gets a redirect.
        accept = request.headers.get("accept", "")
        is_html = "text/html" in accept and "application/json" not in accept
        is_htmx = request.headers.get("hx-request") == "true"

        if is_htmx:
            response = Response(
                content="",
                status_code=401,
                headers={"HX-Redirect": f"/login?redirect={path}"},
            )
        elif is_html and request.method == "GET":
            response = Response(
                content="",
                status_code=303,
                headers={"Location": f"/login?redirect={path}"},
            )
        else:
            response = Response(
                content={"error": "Authentication required"},
                status_code=401,
            )
        await response(scope, receive, send)


def get_route_handlers() -> list:
    """Collect all route handlers including plugins"""
    # Core handlers
    handlers = [
        PageController,
        APIController,
        AdminController,
        HealthController,
        SettingsController,
        FilesController,
        DuckDBController,
        DataController,
        ExploreController,
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
        AICopilotController,
        AISettingsPageController,
        JobsController,
        MCPToolsController,
        health_check,
        metrics,
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

    # Initialize OpenTelemetry as early as possible so request spans
    # captured by the Litestar instrumentation cover the whole boot
    # window. Returns False quickly when TUSK_OTEL_ENDPOINT is unset.
    try:
        init_otel()
    except Exception as e:
        log.warning("OTEL init failed (non-fatal)", error=str(e))

    # Load connections
    load_connections_from_file()
    log.info("Connections loaded")

    # Job registry: any persisted job still in `running` state was
    # left over from a process that's no longer alive. Mark them so the
    # activity drawer doesn't show a perpetual spinner. Then trim
    # history older than a week to keep the SQLite file from growing
    # without bound.
    try:
        from tusk.core.jobs import get_registry as _get_jobs_registry
        registry = _get_jobs_registry()
        registry.mark_interrupted_on_startup()
        registry.prune_old(days=7)
    except Exception as e:
        log.warning("Job registry startup hook failed (non-fatal)", error=str(e))

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

    # Setup plugin templates and statics. Templates still live in the
    # venv (consumed once at import time) but static assets relocate to
    # PLUGIN_STATIC_DIR (~/.tusk/plugin_static by default) so Docker
    # deploys don't require a rebuild on plugin asset changes.
    setup_plugin_templates(TEMPLATES_DIR)

    # Nuke any legacy in-venv copy or stale symlink from a previous
    # version / test run before `setup_plugin_statics` recreates the
    # symlink. `shutil.rmtree` only handles real directories — symlinks
    # need `unlink()`.
    legacy = STATIC_DIR / "plugins"
    if legacy.is_symlink() or legacy.exists():
        try:
            if legacy.is_symlink():
                legacy.unlink()
            else:
                shutil.rmtree(legacy, ignore_errors=True)
        except OSError:
            pass

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

    # Prune AI conversation memory daily — drops sessions untouched
    # for 30 days so the local SQLite doesn't grow forever.
    try:
        from tusk.core.ai_memory import prune_stale_sessions
        scheduler.add_interval_job(
            prune_stale_sessions,
            job_id="ai_memory_prune",
            name="Prune stale AI conversations",
            hours=24,
        )
    except Exception as e:
        log.warning("Failed to register AI memory prune job", error=str(e))

    # Schedule temp export + upload cleanup every 30 min
    import tempfile
    def cleanup_temp_files():
        """Remove tusk_export_* files and stale tusk_uploads/* contents."""
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

        uploads_dir = tmp_dir / "tusk_uploads"
        upload_cutoff = time.time() - 7200  # 2 hours for uploads
        if uploads_dir.exists():
            for f in uploads_dir.iterdir():
                try:
                    if f.is_file() and f.stat().st_mtime < upload_cutoff:
                        f.unlink()
                        cleaned += 1
                except OSError:
                    pass

        if cleaned:
            log.info("Cleaned temp files", count=cleaned)

    scheduler.add_interval_job(
        cleanup_temp_files,
        job_id="temp_file_cleanup",
        name="Cleanup temp export and upload files",
        minutes=30,
    )

    # Job watchdog — kill background jobs that have exceeded their
    # declared max_duration_s. Runs every 30s so a stuck job doesn't
    # tie up worker slots for hours.
    try:
        from tusk.core.jobs import get_registry as _get_jobs_registry

        def _job_watchdog_tick():
            killed = _get_jobs_registry().mark_timed_out()
            if killed > 0:
                log.warning("Job watchdog killed timed-out jobs", count=killed)

        scheduler.add_interval_job(
            _job_watchdog_tick,
            job_id="job_watchdog",
            name="Kill background jobs past their max_duration_s",
            seconds=30,
        )
    except Exception as e:
        log.warning("Failed to register job watchdog", error=str(e))

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

    # Close PostgreSQL connection pools + SSH tunnels
    try:
        from tusk.engines.postgres import close_pools
        from tusk.core.ssh_tunnel import close_all_tunnels

        async def _shutdown_async():
            await close_pools()
            await close_all_tunnels()

        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, _shutdown_async())
                future.result(timeout=10)
        except RuntimeError:
            asyncio.run(_shutdown_async())
    except Exception as e:
        log.warning("Failed to close connection pools / ssh tunnels", error=str(e))

    # Plugin static assets are NOT removed here: PLUGIN_STATIC_DIR is
    # shared by every process on this HOME and a worker recycle would
    # wipe the live instance's assets. See tusk.plugins.templates.

    log.info("Tusk Studio stopped")


# Discover plugins before creating app (needed for route handlers)
discover_plugins()


async def _log_unhandled_exception(exception: Exception, scope: Scope) -> None:
    """Litestar `after_exception` hook — log every exception with its
    traceback before the framework converts it to an HTTP response.

    Without this, Litestar's default behavior converts unhandled
    exceptions to a generic 500 with **no traceback in the structlog
    output**. That is precisely how the CSRF middleware bug stayed
    invisible across ~10 releases (see specs/bugs/2026-05-19-csrf-...).

    Decisions:
    - Log at ERROR with `exc_info=True` so the full traceback lands
      in the standard logger pipeline (which structlog wraps).
    - Tag the record with the request path + method so a 500 in admin
      isn't indistinguishable from one in BI.
    - We DON'T differentiate 4xx vs 5xx here — Litestar's HTTPException
      subclasses are also caught, but those are typically intentional
      and quiet. We filter the noisy 4xx classes out so we keep signal:
      404, 401, 403, 422 are routine.
    """
    from litestar.exceptions import HTTPException

    if isinstance(exception, HTTPException) and 400 <= exception.status_code < 500:
        # Routine client errors — quiet.
        return

    log = get_logger("studio.exceptions")
    path = scope.get("path", "<unknown>")
    method = scope.get("method", "<unknown>")
    log.error(
        "Unhandled exception",
        method=method,
        path=path,
        exc_type=type(exception).__name__,
        exc_info=exception,
    )

# Static-files router (replaces deprecated StaticFilesConfig).
# Plugin assets are served by an explicit handler in PageController
# (`/static/plugins/{plugin_id}/{filename}`) instead of a second
# router — Litestar's prefix matching couldn't pick the most-specific
# prefix and the plugin assets were shadowed.
_static_files_router = create_static_files_router(
    path="/static",
    directories=[STATIC_DIR],
)

app = Litestar(
    route_handlers=[*get_route_handlers(), _static_files_router],
    # MCP server: routes marked `mcp_tool=` (routes/mcp_tools.py) are served
    # as tools at `POST /mcp` (Streamable HTTP). Inherits the session
    # middleware; CSRF is exempt because the client is an agent, not a
    # browser.
    plugins=[
        LitestarMCP(
            MCPConfig(
                base_path="/mcp",
                name="TuskData",
                instructions=(
                    "TuskData is a PostgreSQL workbench. Start with list_connections, "
                    "then get_schema(connection_id, focus) before writing SQL; run_query "
                    "is read-only and row-capped."
                ),
            )
        )
    ],
    template_config=TemplateConfig(
        directory=TEMPLATES_DIR,
        engine=MiniJinjaTemplateEngine,
    ),
    compression_config=CompressionConfig(
        backend="zstd",
        minimum_size=500,  # Compress responses larger than 500 bytes
    ),
    # Middleware execution order is outside-in: RequestTimeout runs
    # first (so it bounds the whole stack, including auth + CSRF),
    # then session check, then correlation-id tagging, then CSRF
    # validation, then the actual handler.
    # NOTE: ASGIMiddleware subclasses are passed as INSTANCES, not
    # classes (the old AbstractMiddleware took classes — Litestar 2.15
    # changed the contract).
    middleware=[
        RequestTimeoutMiddleware(),
        SessionRequiredMiddleware(),
        CorrelationIDMiddleware(),
        CSRFMiddleware(),
    ],
    # Litestar's default OpenAPI controller registers at `/schema`, which
    # collides with our user-facing Schema viewer page. Move it under
    # `/api/openapi` so `/schema` is free for the application UI.
    openapi_config=OpenAPIConfig(
        title="Tusk API",
        version=tusk.__version__,
        path="/api/openapi",
    ),
    on_startup=[on_startup],
    on_shutdown=[on_shutdown],
    # Centralized error logging — see _log_unhandled_exception.
    # Without this, Litestar swallows tracebacks at the default log
    # level and 5xx bugs become invisible (the CSRF middleware bug
    # lived undetected for ~10 releases for this exact reason).
    after_exception=[_log_unhandled_exception],
    debug=os.environ.get("TUSK_DEBUG", "").lower() in ("1", "true", "yes"),
)
