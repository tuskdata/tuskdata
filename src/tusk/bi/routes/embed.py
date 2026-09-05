"""Embed routes for external dashboard embedding (iframe-ready)"""

import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timedelta

from litestar import Controller, Request, get, post
from litestar.params import Body
from litestar.response import Response, Template
import msgspec

import structlog
log = structlog.get_logger()


def _get_embed_secret() -> str:
    """Get or generate the embed secret for HMAC token signing."""
    secret = os.environ.get("TUSK_EMBED_SECRET", "")
    if secret:
        return secret

    # Try loading from config file
    config_path = os.path.expanduser("~/.tusk/config.toml")
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            for line in f:
                if line.strip().startswith("embed_secret"):
                    parts = line.split("=", 1)
                    if len(parts) == 2:
                        return parts[1].strip().strip('"').strip("'")

    # Generate and persist a new secret
    import secrets
    secret = secrets.token_urlsafe(32)
    config_dir = os.path.expanduser("~/.tusk")
    os.makedirs(config_dir, exist_ok=True)
    with open(config_path, "a") as f:
        f.write(f'\nembed_secret = "{secret}"\n')
    log.info("Generated new embed secret", path=config_path)
    return secret


def _generate_embed_token(dashboard_id: int, rls_clauses: dict, expires_at: str, app_id: str) -> str:
    """Generate an HMAC-signed embed token.

    Format: <hmac_prefix>.<base64_payload>
    """
    secret = _get_embed_secret()
    payload = {
        "dashboard_id": dashboard_id,
        "rls": rls_clauses,
        "exp": expires_at,
        "app": app_id,
    }
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    payload_b64 = base64.urlsafe_b64encode(payload_json.encode()).decode()
    signature = hmac.new(secret.encode(), payload_json.encode(), hashlib.sha256).hexdigest()[:16]
    return f"{signature}.{payload_b64}"


class EmbedAPIController(Controller):
    """API for generating embed tokens (server-to-server)"""

    path = "/api/embed"

    @post("/token")
    async def create_embed_token(self, request: Request, data: dict = Body()) -> Response:
        """Generate an embed token for a dashboard.

        Requires X-Embed-Key header matching the configured embed_secret.
        Body: { dashboard_id, rls_clauses?, expires_in_seconds?, app_id? }
        Returns: { token, embed_url }
        """
        # Validate embed key
        embed_key = request.headers.get("x-embed-key", "")
        secret = _get_embed_secret()
        if not embed_key or not hmac.compare_digest(embed_key, secret):
            return Response(
                content=msgspec.json.encode({"error": "Invalid or missing X-Embed-Key"}),
                media_type="application/json",
                status_code=403,
            )

        dashboard_id = data.get("dashboard_id")
        if not dashboard_id:
            return Response(
                content=msgspec.json.encode({"error": "dashboard_id is required"}),
                media_type="application/json",
                status_code=400,
            )

        # Verify dashboard exists
        from tusk.bi.db import get_dashboard
        dashboard = get_dashboard(dashboard_id)
        if not dashboard:
            return Response(
                content=msgspec.json.encode({"error": "Dashboard not found"}),
                media_type="application/json",
                status_code=404,
            )

        rls_clauses = data.get("rls_clauses", {})
        expires_in = data.get("expires_in_seconds", 3600)
        app_id = data.get("app_id", "")
        expires_at = (datetime.now() + timedelta(seconds=expires_in)).isoformat()

        # Generate token
        token = _generate_embed_token(dashboard_id, rls_clauses, expires_at, app_id)

        # Store in DB
        from tusk.bi.db import create_embed_token
        create_embed_token(
            dashboard_id=dashboard_id,
            token=token,
            rls_clauses=json.dumps(rls_clauses),
            expires_at=expires_at,
            app_id=app_id,
        )

        # Build embed URL
        scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
        host = request.headers.get("x-forwarded-host", request.headers.get("host", "localhost:8000"))
        embed_url = f"{scheme}://{host}/embed/dashboard/{dashboard_id}?token={token}"

        log.info("Embed token created", dashboard_id=dashboard_id, app_id=app_id)

        return Response(
            content=msgspec.json.encode({
                "token": token,
                "embed_url": embed_url,
                "expires_at": expires_at,
            }),
            media_type="application/json",
        )


class EmbedPageController(Controller):
    """Render embedded dashboards (iframe-friendly, no auth required)"""

    path = "/embed"

    @get("/dashboard/{dashboard_id:int}")
    async def embed_dashboard(self, request: Request, dashboard_id: int) -> Template | Response:
        """Render an embedded dashboard with minimal chrome.

        Validates token query param against embed_tokens table.
        Passes RLS clauses from token to widget render context.
        """
        token = request.query_params.get("token", "")
        if not token:
            return Response(
                content=b"Missing token parameter",
                media_type="text/html",
                status_code=403,
            )

        # Validate token against DB
        from tusk.bi.db import get_embed_token
        embed = get_embed_token(token)
        if not embed:
            return Response(
                content=b"Invalid or expired embed token",
                media_type="text/html",
                status_code=403,
            )

        # Check expiry
        try:
            expires = datetime.fromisoformat(embed["expires_at"])
            if datetime.now() > expires:
                return Response(
                    content=b"Embed token has expired",
                    media_type="text/html",
                    status_code=403,
                )
        except (ValueError, TypeError):
            return Response(
                content=b"Invalid token expiry",
                media_type="text/html",
                status_code=403,
            )

        # Verify dashboard_id matches
        if embed["dashboard_id"] != dashboard_id:
            return Response(
                content=b"Token does not match this dashboard",
                media_type="text/html",
                status_code=403,
            )

        # Load dashboard + widgets
        from tusk.bi.db import get_dashboard, get_widgets, get_dashboard_tabs
        dashboard = get_dashboard(dashboard_id)
        if not dashboard:
            return Response(
                content=b"Dashboard not found",
                media_type="text/html",
                status_code=404,
            )

        widgets = get_widgets(dashboard_id)
        tabs = get_dashboard_tabs(dashboard_id)

        # Parse RLS clauses from token record
        rls_clauses = {}
        try:
            rls_clauses = json.loads(embed.get("rls_clauses", "{}"))
        except (json.JSONDecodeError, TypeError):
            pass

        # Encode RLS for passing to widget render calls
        rls_param = ""
        if rls_clauses:
            rls_param = base64.urlsafe_b64encode(
                json.dumps(rls_clauses).encode()
            ).decode()

        return Template(
            template_name="plugins/bi/embed_dashboard.html",
            context={
                "dashboard": dashboard,
                "widgets": widgets,
                "tabs": tabs,
                "rls_param": rls_param,
                "token": token,
                "active_tab_id": None,
            },
        )
