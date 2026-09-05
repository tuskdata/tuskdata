"""Page routes for Tusk Studio"""

import mimetypes
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import msgspec
from litestar import get, Request
from litestar.exceptions import NotFoundException
from litestar.response import Template, Response, File

from tusk.core.auth import get_session, get_user_by_id, get_user_groups, get_user_permissions
from tusk.core.config import get_config
from tusk.core.connection import list_connections
from tusk.plugins.templates import PLUGIN_STATIC_DIR
from tusk.studio.routes.base import TuskController, get_request_user

# Plugin id format guard — keeps `..` and slashes out of the path.
_PLUGIN_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{0,30}$")

SESSION_COOKIE = "tusk_session"


def _greeting_for(hour: int) -> str:
    """Server-time-of-day greeting. Used by the homepage hero so the
    text is right at first paint without a JS dance."""
    if hour < 12:
        return "Good morning"
    if hour < 18:
        return "Good afternoon"
    return "Good evening"


def _resolve_user_name(request: Request) -> str:
    """Best-effort display name. Falls back to 'there' so the greeting
    still reads naturally in single-user mode."""
    user = get_request_user(request)
    if not user:
        return "there"
    return user.display_name or user.username


def _compute_home_stats() -> dict:
    """Aggregate the numbers the homepage cards need.

    Cheap enough to run inline (single sqlite read + an in-process
    bucket loop). If history grows above ~100k rows we'll want to
    promote this to a background snapshot, but for now the v0.4.4
    homepage is the only caller and it's keyed off the request.
    """
    from tusk.core.history import get_history

    h = get_history()
    # Pull a generous window once and bucket in Python — one round-trip
    # beats five separate aggregate queries on a small SQLite db.
    recent = h.get_recent(limit=2000)
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    day_ago = now - timedelta(hours=24)

    queries_week = 0
    latencies_24h: list[float] = []
    # 7 buckets, oldest → newest, indexed by days-ago.
    by_day = [0] * 7
    # 24 buckets, oldest → newest, indexed by hours-ago.
    by_hour = [0.0] * 24
    by_hour_count = [0] * 24

    pipelines_today = 0
    errors_today = 0

    for e in recent:
        try:
            dt = datetime.fromisoformat(e.executed_at.replace("Z", "+00:00"))
        except Exception:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        if dt > week_ago:
            queries_week += 1
            days_ago = (now - dt).days
            if 0 <= days_ago < 7:
                by_day[6 - days_ago] += 1

        if dt > day_ago:
            if e.execution_time_ms:
                latencies_24h.append(e.execution_time_ms)
            hours_ago = int((now - dt).total_seconds() // 3600)
            if 0 <= hours_ago < 24 and e.execution_time_ms:
                by_hour[23 - hours_ago] += e.execution_time_ms
                by_hour_count[23 - hours_ago] += 1
            if e.status == "error":
                errors_today += 1
            else:
                pipelines_today += 1

    avg_latency = round(sum(latencies_24h) / len(latencies_24h)) if latencies_24h else 0
    # Average per-bucket so the latency sparkline reflects shape, not volume.
    latency_buckets = [
        round(by_hour[i] / by_hour_count[i]) if by_hour_count[i] else 0
        for i in range(24)
    ]

    conns = list_connections()
    by_type: dict[str, int] = {}
    for c in conns:
        by_type[c.type] = by_type.get(c.type, 0) + 1
    # Pre-render the connection breakdown — MiniJinja can't .append() inside
    # a `{% for %}`, so we hand the template a finished string.
    by_type_label = " · ".join(f"{v} {k}" for k, v in sorted(by_type.items())) or "no connections yet"

    return {
        "queries_week": queries_week,
        "queries_by_day": by_day,
        "avg_latency_ms": avg_latency,
        "latency_by_hour": latency_buckets,
        "active_connections": len(conns),
        "max_connections": max(10, len(conns)),
        "connections_by_type": by_type,
        "by_type_label": by_type_label,
        "pipelines_today": pipelines_today,
        "errors_today": errors_today,
    }


class PageController(TuskController):
    """Serves HTML pages"""

    path = "/"

    def _render_home(self, request: Request) -> Template:
        """Shared homepage render path used by `/` and `/home`."""
        from tusk.core.history import get_history

        stats = _compute_home_stats()
        recent = [
            {
                "id": e.id,
                "connection_name": e.connection_name,
                "sql": e.sql,
                "sql_preview": (e.sql or "").strip().replace("\n", " ")[:120],
                "executed_at": e.executed_at,
                "execution_time_ms": round(e.execution_time_ms, 1) if e.execution_time_ms else 0,
                "row_count": e.row_count,
                "status": e.status,
            }
            for e in get_history().get_recent(limit=4)
        ]
        now = datetime.now(timezone.utc).astimezone()
        return self.render(
            "home.html",
            active_page="home",
            greeting=_greeting_for(now.hour),
            user_name=_resolve_user_name(request),
            stats=stats,
            recent_queries=recent,
        )

    @get("/")
    async def index(self, request: Request) -> Template:
        """Root: homepage when the user has any data on disk, else
        the Studio so the first-run experience is immediate."""
        from tusk.core.history import get_history

        any_history = bool(get_history().get_recent(limit=1))
        any_connections = bool(list_connections())
        if any_history or any_connections:
            return self._render_home(request)
        return self.render("index.html", active_page="studio")

    @get("/home")
    async def home(self, request: Request) -> Template:
        """Greeting + stat cards + recent + AI suggestions panel."""
        return self._render_home(request)

    @get("/studio")
    async def studio(self) -> Template:
        """Explicit Studio route — `/` may redirect away from it once
        the user has data."""
        return self.render("index.html", active_page="studio")

    @get("/admin")
    async def admin(self) -> Template:
        """Admin dashboard page"""
        conns = list_connections()
        pg_conns = [
            {"id": c.id, "name": c.name}
            for c in conns if c.type == "postgres"
        ]
        return self.render("admin.html", active_page="admin", pg_connections=pg_conns)

    @get("/data")
    async def data(self) -> Template:
        """Data/ETL pipeline builder page"""
        return self.render("data.html", active_page="data")

    @get("/schema")
    async def schema(self, request: Request) -> Template:
        """Schema viewer (ER diagram) — Postgres only. `?connection=<id>`
        (used by Schema Watch / contract notification links) preselects."""
        conns = list_connections()
        pg_conns = [
            {"id": c.id, "name": c.name, "database": c.database}
            for c in conns if c.type == "postgres"
        ]
        wanted = request.query_params.get("connection", "")
        if wanted and any(c["id"] == wanted for c in pg_conns):
            pg_conns.sort(key=lambda c: c["id"] != wanted)
        return self.render(
            "schema.html",
            active_page="schema",
            pg_connections=pg_conns,
        )

    @get("/explore")
    async def explore(self) -> Template:
        """Data explorer / per-column profile page — Postgres only."""
        conns = list_connections()
        pg_conns = [
            {"id": c.id, "name": c.name, "database": c.database}
            for c in conns if c.type == "postgres"
        ]
        return self.render(
            "explore.html",
            active_page="explore",
            pg_connections=pg_conns,
        )

    @get("/scheduled")
    async def scheduled(self) -> Template:
        """Scheduled jobs page (cron / interval / one-shot)."""
        conns = list_connections()
        connections = [
            {"id": c.id, "name": c.name, "type": c.type}
            for c in conns
        ]
        return self.render(
            "scheduled.html",
            active_page="scheduled",
            connections=connections,
        )

    @get("/settings")
    async def settings_hub(self) -> Template:
        """Settings hub — lists every settings category in one place
        (AI Copilot, notifications, anything plugins register later).
        Reachable from the top-nav gear icon."""
        return self.render("settings_hub.html", active_page="settings")

    @get("/login")
    async def login(self) -> Template:
        """Login page"""
        return self.render("login.html", active_page="login")

    @get("/users")
    async def users(self) -> Template:
        """User management page (admin)"""
        return self.render("users.html", active_page="users")

    @get("/profile")
    async def profile(self, request: Request) -> Template:
        """User profile page — server-rendered with user data"""
        config = get_config()
        user = None
        groups = []
        permissions = []
        api_tokens = []

        if config.auth_mode == "multi":
            user = get_request_user(request)
            if user:
                groups = [{"id": g.id, "name": g.name} for g in get_user_groups(user.id)]
                permissions = list(get_user_permissions(user.id))
                from tusk.core.api_tokens import list_tokens

                api_tokens = [msgspec.to_builtins(t) for t in list_tokens(user.id)]

        return self.render(
            "profile.html",
            active_page="profile",
            profile_user=user,
            api_tokens=api_tokens,
            profile_groups=groups,
            profile_permissions=permissions,
        )

    @get("/static/plugins/{plugin_id:str}/{filename:path}")
    async def serve_plugin_asset(self, plugin_id: str, filename: str) -> File:
        """Serve plugin static assets out of `PLUGIN_STATIC_DIR`.

        Litestar's `StaticFilesConfig` doesn't pick the most-specific
        prefix when two configs share a stem (`/static` and
        `/static/plugins`), so the plugin assets get shadowed by the
        main `/static` mount and 404. An explicit handler dodges that.
        Path-traversal is blocked by validating `plugin_id` against a
        regex and ensuring the final resolved path stays under
        `PLUGIN_STATIC_DIR/{plugin_id}`.

        Litestar passes `filename` with a leading slash because of the
        `:path` converter; strip it so the join behaves naturally.
        """
        from tusk.core.logging import get_logger
        log = get_logger("plugin_static")

        if not _PLUGIN_ID_RE.match(plugin_id):
            raise NotFoundException("plugin not found")

        # `:path` captures leave a leading slash on the captured value.
        clean_filename = filename.lstrip("/")
        if not clean_filename:
            raise NotFoundException("filename required")

        plugin_root = (PLUGIN_STATIC_DIR / plugin_id).resolve()
        target = (plugin_root / clean_filename).resolve()

        # Containment check — block `..` traversal.
        try:
            target.relative_to(plugin_root)
        except ValueError:
            log.warning("Plugin asset traversal blocked", plugin_id=plugin_id, filename=clean_filename)
            raise NotFoundException("plugin asset not found")

        if not target.is_file():
            log.debug(
                "Plugin asset miss",
                plugin_id=plugin_id,
                filename=clean_filename,
                target=str(target),
                static_dir=str(PLUGIN_STATIC_DIR),
            )
            raise NotFoundException("plugin asset not found")

        # Force correct MIME for types `mimetypes.guess_type` misses on
        # some Python builds. `.mjs` returning `application/octet-stream`
        # would make browsers reject `<script type="module">` imports
        # (BI plugin ships ESM); `.wasm` would similarly fail.
        suffix = target.suffix.lower()
        _MIME_OVERRIDES = {
            ".mjs": "text/javascript",
            ".js":  "text/javascript",
            ".css": "text/css",
            ".wasm": "application/wasm",
            ".svg": "image/svg+xml",
            ".json": "application/json",
            ".map": "application/json",
        }
        media_type = _MIME_OVERRIDES.get(suffix)
        if not media_type:
            media_type, _ = mimetypes.guess_type(str(target))
        return File(
            path=target,
            media_type=media_type or "application/octet-stream",
            filename=target.name,
            content_disposition_type="inline",
        )

    @get("/favicon.ico")
    async def favicon(self) -> Response:
        """Return a simple SVG favicon"""
        # Mammoth emoji as SVG favicon
        svg = '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">
            <text y=".9em" font-size="90">&#129443;</text>
        </svg>'''
        return Response(
            content=svg,
            media_type="image/svg+xml",
            headers={"Cache-Control": "public, max-age=86400"}
        )
