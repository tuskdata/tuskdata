"""Authentication routes"""

import msgspec
from litestar import Controller, get, post, Response, Request, delete
from litestar.params import Body
from litestar.response import Redirect, Template

from tusk.core.auth import (
    authenticate,
    create_session,
    get_session,
    delete_session,
    get_user_by_id,
    get_user_permissions,
    is_auth_enabled,
    list_users,
    create_user,
    update_user,
    update_password,
    delete_user,
    list_groups,
    get_group,
    create_group,
    add_user_to_group,
    remove_user_from_group,
    get_user_groups,
    setup_default_groups,
    setup_admin_user,
    log_audit,
    get_audit_logs,
    get_audit_log_count,
    check_login_rate_limit,
    record_login_attempt,
    generate_csrf_token,
    validate_csrf_token,
    cleanup_expired_sessions,
    PERMISSIONS,
)
from tusk.core.config import get_config
from tusk.core.logging import get_logger
from tusk.studio.routes.base import get_request_user
from tusk.studio.htmx import is_htmx, htmx_toast, htmx_error

log = get_logger("auth")

SESSION_COOKIE = "tusk_session"

PASSWORD_MIN_LENGTH = 8


def _validate_password(password: str) -> str | None:
    """Return an error message if the password fails policy, else None."""
    if len(password) < PASSWORD_MIN_LENGTH:
        return f"Password must be at least {PASSWORD_MIN_LENGTH} characters"
    has_letter = any(c.isalpha() for c in password)
    has_digit = any(c.isdigit() for c in password)
    if not (has_letter and has_digit):
        return "Password must contain at least one letter and one digit"
    return None


class AuthController(Controller):
    """Authentication API"""

    path = "/api/auth"

    @get("/status")
    async def auth_status(self, request: Request) -> dict:
        """Get authentication status and current user"""
        config = get_config()
        auth_enabled = config.auth_mode == "multi"

        if not auth_enabled:
            return {
                "auth_enabled": False,
                "user": None,
                "permissions": list(PERMISSIONS.keys()),  # All permissions in single mode
            }

        # Session cookie or API token
        user = get_request_user(request)
        if not user:
            return {"auth_enabled": True, "user": None, "permissions": []}

        permissions = list(get_user_permissions(user.id))

        return {
            "auth_enabled": True,
            "user": {
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "display_name": user.display_name,
                "is_admin": user.is_admin,
            },
            "permissions": permissions,
        }

    @post("/login")
    async def login(self, request: Request, data: dict = Body()) -> Response:
        """Login with username and password"""
        config = get_config()

        if config.auth_mode != "multi":
            return Response(
                content={"error": "Authentication not enabled"},
                status_code=400,
            )

        # Rate limiting
        client_ip = request.client.host if request.client else "unknown"
        if not check_login_rate_limit(client_ip):
            log.warning("Login rate limited", ip=client_ip)
            return Response(
                content={"error": "Too many login attempts. Please wait a minute."},
                status_code=429,
            )

        username = data.get("username", "").strip()
        password = data.get("password", "")

        if not username or not password:
            return Response(
                content={"error": "Username and password required"},
                status_code=400,
            )

        record_login_attempt(client_ip)

        user = authenticate(username, password)
        if not user:
            log.warning("Failed login attempt", username=username, ip=client_ip)
            return Response(
                content={"error": "Invalid username or password"},
                status_code=401,
            )

        # Create session
        ip_address = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")
        session = create_session(user.id, ip_address, user_agent)

        log.info("User logged in", username=username, user_id=user.id)
        log_audit("login", user_id=user.id, resource="session", ip_address=ip_address)

        response = Response(
            content={
                "success": True,
                "user": {
                    "id": user.id,
                    "username": user.username,
                    "display_name": user.display_name,
                    "is_admin": user.is_admin,
                },
            },
            status_code=200,
        )

        # Set session cookie
        response.set_cookie(
            key=SESSION_COOKIE,
            value=session.id,
            max_age=config.session_lifetime,
            httponly=True,
            samesite="lax",
        )

        return response

    @post("/logout")
    async def logout(self, request: Request) -> Response:
        """Logout and invalidate session"""
        session_id = request.cookies.get(SESSION_COOKIE)

        if session_id:
            session = get_session(session_id)
            user_id = session.user_id if session else None
            delete_session(session_id)
            log.info("User logged out", session_id=session_id[:8])
            ip_address = request.client.host if request.client else None
            log_audit("logout", user_id=user_id, resource="session", ip_address=ip_address)

        response = Response(content={"success": True}, status_code=200)
        response.delete_cookie(SESSION_COOKIE)
        return response

    @get("/config")
    async def get_auth_config(self) -> dict:
        """Get auth configuration (public info only)"""
        config = get_config()
        return {
            "mode": config.auth_mode,
            "allow_registration": config.allow_registration,
        }


class ProfileController(Controller):
    """User profile API (current user)"""

    path = "/api/profile"

    @get("/")
    async def get_profile(self, request: Request) -> dict:
        """Get current user's profile"""
        user = await self._get_current_user(request)
        if not user:
            return {"error": "Not authenticated"}

        groups = get_user_groups(user.id)
        permissions = list(get_user_permissions(user.id))

        return {
            "user": {
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "display_name": user.display_name,
                "is_admin": user.is_admin,
                "created_at": user.created_at,
                "last_login": user.last_login,
            },
            "groups": [{"id": g.id, "name": g.name} for g in groups],
            "permissions": permissions,
        }

    @post("/")
    async def update_profile(self, request: Request, data: dict = Body()) -> Response | dict:
        """Update current user's profile"""
        user = await self._get_current_user(request)
        if not user:
            if is_htmx(request):
                return Response(content="", status_code=200, headers=htmx_error("Not authenticated"))
            return {"error": "Not authenticated"}

        updates = {}
        if "email" in data:
            updates["email"] = data["email"].strip() or None
        if "display_name" in data:
            updates["display_name"] = data["display_name"].strip() or None

        if updates:
            update_user(user.id, **updates)
            log.info("Profile updated", user_id=user.id)

        if is_htmx(request):
            return Response(content="", status_code=200, headers=htmx_toast("Profile updated", "success"))
        return {"success": True}

    @post("/password")
    async def change_password(self, request: Request, data: dict = Body()) -> Response | dict:
        """Change current user's password"""
        user = await self._get_current_user(request)
        if not user:
            if is_htmx(request):
                return Response(content="", status_code=200, headers=htmx_error("Not authenticated"))
            return {"error": "Not authenticated"}

        current_password = data.get("current_password", "")
        new_password = data.get("new_password", "")

        # Verify current password
        if not authenticate(user.username, current_password):
            if is_htmx(request):
                return Response(content="", status_code=200, headers=htmx_error("Current password is incorrect"))
            return {"error": "Current password is incorrect"}

        if not new_password:
            if is_htmx(request):
                return Response(content="", status_code=200, headers=htmx_toast("New password required", "warning"))
            return {"error": "New password required"}
        err = _validate_password(new_password)
        if err:
            if is_htmx(request):
                return Response(content="", status_code=200, headers=htmx_toast(err, "warning"))
            return {"error": err}

        update_password(user.id, new_password)
        log.info("Password changed", user_id=user.id)
        if is_htmx(request):
            return Response(content="", status_code=200, headers=htmx_toast("Password changed successfully", "success"))
        return {"success": True}

    # ── Personal API tokens ──────────────────────────────────

    @get("/tokens")
    async def list_tokens(self, request: Request) -> dict:
        """The current user's active API tokens (never the secrets)."""
        user = await self._get_current_user(request)
        if not user:
            return {"error": "Not authenticated", "tokens": []}
        from tusk.core.api_tokens import list_tokens

        return {"tokens": [msgspec.to_builtins(t) for t in list_tokens(user.id)]}

    @post("/tokens")
    async def create_token(self, request: Request, data: dict = Body()) -> dict:
        """Mint a token. The plaintext comes back exactly once."""
        user = await self._get_current_user(request)
        if not user:
            return {"error": "Not authenticated"}
        from tusk.core.api_tokens import create_token

        name = str(data.get("name") or "").strip()
        expires_days = data.get("expires_days")
        try:
            expires_days = int(expires_days) if expires_days not in (None, "", 0, "0") else None
            token, plaintext = create_token(user.id, name, expires_days=expires_days)
        except (TypeError, ValueError) as e:
            return {"error": str(e)}
        ip_address = request.client.host if request.client else None
        log_audit("token.create", user_id=user.id, resource=token.id, details=name, ip_address=ip_address)
        return {"token": plaintext, "id": token.id, "name": token.name, "expires_at": token.expires_at}

    @delete("/tokens/{token_id:str}", status_code=200)
    async def revoke_token(self, request: Request, token_id: str) -> dict:
        """Revoke one of the current user's tokens."""
        user = await self._get_current_user(request)
        if not user:
            return {"error": "Not authenticated"}
        from tusk.core.api_tokens import revoke_token

        if not revoke_token(token_id, user_id=user.id):
            return {"error": "Token not found"}
        ip_address = request.client.host if request.client else None
        log_audit("token.revoke", user_id=user.id, resource=token_id, ip_address=ip_address)
        return {"success": True}

    async def _get_current_user(self, request: Request):
        """Current user from the session cookie or an API token"""
        return get_request_user(request)


class UsersController(Controller):
    """User management API (admin only)"""

    path = "/api/users"

    @get("/")
    async def get_users(self, request: Request) -> dict | Template:
        """List all users"""
        # Check permission
        if not await self._check_admin(request):
            return {"error": "Unauthorized", "users": []}

        users = list_users()
        user_list = []
        for u in users:
            user_dict = {
                "id": u.id,
                "username": u.username,
                "email": u.email,
                "display_name": u.display_name,
                "is_admin": u.is_admin,
                "is_active": u.is_active,
                "created_at": u.created_at,
                "last_login": u.last_login,
                "last_login_display": u.last_login if u.last_login else None,
                "groups": [{"id": g.id, "name": g.name} for g in get_user_groups(u.id)],
            }
            user_list.append(user_dict)

        if is_htmx(request):
            return Template("partials/users/list.html", context={"users": user_list})

        return {"users": user_list}

    @post("/")
    async def create_new_user(self, request: Request, data: dict = Body()) -> Response | dict:
        """Create a new user"""
        if not await self._check_admin(request):
            if is_htmx(request):
                return Response(content="", status_code=403, headers=htmx_error("Unauthorized"))
            return {"error": "Unauthorized"}

        username = data.get("username", "").strip()
        password = data.get("password", "")
        email = data.get("email", "").strip() or None
        display_name = data.get("display_name", "").strip() or None
        is_admin = data.get("is_admin", False)

        if not username:
            if is_htmx(request):
                return Response(content="", status_code=200, headers=htmx_toast("Username required", "warning"))
            return {"error": "Username required"}
        if not password:
            if is_htmx(request):
                return Response(content="", status_code=200, headers=htmx_toast("Password required", "warning"))
            return {"error": "Password required"}
        err = _validate_password(password)
        if err:
            if is_htmx(request):
                return Response(content="", status_code=200, headers=htmx_toast(err, "warning"))
            return {"error": err}

        try:
            user = create_user(
                username=username,
                password=password,
                email=email,
                display_name=display_name,
                is_admin=is_admin,
            )
            log.info("User created", username=username, user_id=user.id)
            ip_address = request.client.host if request.client else None
            log_audit("user.create", user_id=user.id, resource=username, ip_address=ip_address)

            if is_htmx(request):
                return Response(content="", status_code=200, headers=htmx_toast(f"User '{username}' created", "success"))

            return {
                "success": True,
                "user": {
                    "id": user.id,
                    "username": user.username,
                },
            }
        except Exception as e:
            log.error("Failed to create user", username=username, error=str(e))
            if is_htmx(request):
                return Response(content="", status_code=200, headers=htmx_error(str(e)))
            return {"error": str(e)}

    @get("/{user_id:str}")
    async def get_user(self, request: Request, user_id: str) -> dict:
        """Get user details"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized"}

        user = get_user_by_id(user_id)
        if not user:
            return {"error": "User not found"}

        groups = get_user_groups(user_id)

        return {
            "user": {
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "display_name": user.display_name,
                "is_admin": user.is_admin,
                "is_active": user.is_active,
                "created_at": user.created_at,
                "last_login": user.last_login,
            },
            "groups": [{"id": g.id, "name": g.name} for g in groups],
        }

    @post("/{user_id:str}")
    async def update_existing_user(self, request: Request, user_id: str, data: dict = Body()) -> dict:
        """Update user"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized"}

        updates = {}
        if "email" in data:
            updates["email"] = data["email"].strip() or None
        if "display_name" in data:
            updates["display_name"] = data["display_name"].strip() or None
        if "is_admin" in data:
            updates["is_admin"] = bool(data["is_admin"])
        if "is_active" in data:
            updates["is_active"] = bool(data["is_active"])

        if updates:
            update_user(user_id, **updates)
            log.info("User updated", user_id=user_id, updates=list(updates.keys()))

        return {"success": True}

    @post("/{user_id:str}/password")
    async def reset_user_password(self, request: Request, user_id: str, data: dict = Body()) -> dict:
        """Reset user password"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized"}

        password = data.get("password", "")
        if not password:
            return {"error": "Password required"}
        err = _validate_password(password)
        if err:
            return {"error": err}

        update_password(user_id, password)
        log.info("Password reset", user_id=user_id)
        return {"success": True}

    @post("/{user_id:str}/delete")
    async def delete_existing_user(self, request: Request, user_id: str) -> Response | Template | dict:
        """Delete user"""
        if not await self._check_admin(request):
            if is_htmx(request):
                return Response(content="", status_code=403, headers=htmx_error("Unauthorized"))
            return {"error": "Unauthorized"}

        delete_user(user_id)
        log.info("User deleted", user_id=user_id)
        ip_address = request.client.host if request.client else None
        log_audit("user.delete", user_id=user_id, resource=user_id, ip_address=ip_address)

        if is_htmx(request):
            # Re-render the users list
            return await self.get_users(request)

        return {"success": True}

    @post("/{user_id:str}/groups")
    async def add_to_group(self, request: Request, user_id: str, data: dict = Body()) -> dict:
        """Add user to group"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized"}

        group_id = data.get("group_id")
        if not group_id:
            return {"error": "Group ID required"}

        current_user = await self._get_current_user(request)
        add_user_to_group(user_id, group_id, current_user.id if current_user else None)
        return {"success": True}

    @post("/{user_id:str}/groups/{group_id:str}/remove")
    async def remove_from_group(self, request: Request, user_id: str, group_id: str) -> dict:
        """Remove user from group"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized"}

        remove_user_from_group(user_id, group_id)
        return {"success": True}

    async def _check_admin(self, request: Request) -> bool:
        """Check if current user is admin"""
        config = get_config()
        if config.auth_mode != "multi":
            return True  # No auth = full access

        user = await self._get_current_user(request)
        return user is not None and user.is_admin

    async def _get_current_user(self, request: Request):
        """Current user from the session cookie or an API token"""
        return get_request_user(request)


class GroupsController(Controller):
    """Group management API (admin only)"""

    path = "/api/groups"

    @get("/")
    async def get_groups(self, request: Request) -> dict | Template:
        """List all groups"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized", "groups": []}

        groups = list_groups()
        group_list = [
            {
                "id": g.id,
                "name": g.name,
                "description": g.description,
                "permissions": g.permissions,
                "created_at": g.created_at,
            }
            for g in groups
        ]

        if is_htmx(request):
            return Template("partials/users/groups.html", context={"groups": group_list})

        return {"groups": group_list}

    @post("/")
    async def create_new_group(self, request: Request, data: dict = Body()) -> dict:
        """Create a new group"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized"}

        name = data.get("name", "").strip()
        description = data.get("description", "").strip() or None
        permissions = data.get("permissions", [])

        if not name:
            return {"error": "Group name required"}

        group = create_group(name, description, permissions)
        log.info("Group created", name=name, group_id=group.id)
        return {"success": True, "group": {"id": group.id, "name": group.name}}

    @get("/{group_id:str}")
    async def get_group_details(self, request: Request, group_id: str) -> dict:
        """Get group details"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized"}

        group = get_group(group_id)
        if not group:
            return {"error": "Group not found"}

        return {
            "group": {
                "id": group.id,
                "name": group.name,
                "description": group.description,
                "permissions": group.permissions,
                "created_at": group.created_at,
            }
        }

    @get("/permissions")
    async def get_all_permissions(self, request: Request) -> dict:
        """Get all available permissions"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized"}

        return {
            "permissions": [
                {"code": code, "description": desc}
                for code, desc in PERMISSIONS.items()
            ]
        }

    async def _check_admin(self, request: Request) -> bool:
        """Check if current user is admin"""
        config = get_config()
        if config.auth_mode != "multi":
            return True  # No auth = full access

        user = get_request_user(request)
        return user is not None and user.is_admin


class AuthSetupController(Controller):
    """Auth setup endpoints"""

    path = "/api/auth/setup"

    @post("/init")
    async def init_auth(self, data: dict = Body()) -> dict:
        """Initialize auth system (create default groups and admin user)"""
        config = get_config()

        if config.auth_mode != "multi":
            return {"error": "Auth mode not enabled. Set auth_mode to 'multi' first."}

        # Setup default groups
        setup_default_groups()

        # Create admin user if needed
        admin_password = data.get("admin_password", "admin")
        user = setup_admin_user(password=admin_password)

        if user:
            log.info("Auth system initialized", admin_user=user.username)
            return {
                "success": True,
                "message": "Auth system initialized",
                "admin_user": user.username,
            }
        else:
            return {
                "success": True,
                "message": "Auth system already initialized",
            }


class AuditLogController(Controller):
    """Audit log API (admin only)"""

    path = "/api/audit"

    @get("/")
    async def get_logs(self, request: Request) -> dict | Template:
        """Get audit log entries"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized", "entries": []}

        # Parse query params
        limit = int(request.query_params.get("limit", "100"))
        offset = int(request.query_params.get("offset", "0"))
        user_id = request.query_params.get("user_id")
        action = request.query_params.get("action")

        search = (request.query_params.get("search") or "").strip()
        # Pull a generous window when filtering by free text — `search` is
        # applied after fetch since the audit_log SQLite table is small
        # and there's no FTS index.
        fetch_limit = max(limit, 1000) if search else limit
        entries = get_audit_logs(limit=fetch_limit, offset=offset, user_id=user_id, action=action)

        if search:
            needle = search.lower()
            entries = [
                e for e in entries
                if needle in (e.get("username") or "").lower()
                or needle in (e.get("action") or "").lower()
                or needle in (e.get("resource") or "").lower()
                or needle in (e.get("details") or "").lower()
                or needle in (e.get("ip_address") or "").lower()
            ]
            total = len(entries)
            entries = entries[:limit]
        else:
            total = get_audit_log_count(user_id=user_id, action=action)

        # Display-friendly timestamps. Entries are dicts here; setting a
        # new key gives the template `e.timestamp_display`.
        for e in entries:
            ts = e.get("timestamp")
            if ts:
                e["timestamp_display"] = ts.replace("T", " ").split(".")[0]

        if is_htmx(request):
            return Template("partials/users/audit.html", context={
                "entries": entries,
                "total": total,
                "limit": limit,
                "offset": offset,
                "page": (offset // max(limit, 1)) + 1,
                "total_pages": max(1, (total + limit - 1) // max(limit, 1)),
                "search": search,
                "action_filter": action or "",
            })

        return {"entries": entries, "total": total}

    @get("/export")
    async def export_logs(self, request: Request) -> Response:
        """Stream the audit log as CSV. Honors the same filters as the
        list endpoint (action, search, user_id). Hard-capped at 50k rows
        so a malicious user can't exhaust memory."""
        if not await self._check_admin(request):
            return Response(content="Unauthorized", status_code=401, media_type="text/plain")

        import csv
        from io import StringIO

        action = request.query_params.get("action")
        user_id = request.query_params.get("user_id")
        search = (request.query_params.get("search") or "").strip()
        fmt = (request.query_params.get("format") or "csv").lower()

        entries = get_audit_logs(limit=50_000, offset=0, user_id=user_id, action=action)
        if search:
            needle = search.lower()
            entries = [
                e for e in entries
                if needle in (e.get("username") or "").lower()
                or needle in (e.get("action") or "").lower()
                or needle in (e.get("resource") or "").lower()
                or needle in (e.get("details") or "").lower()
                or needle in (e.get("ip_address") or "").lower()
            ]

        if fmt == "json":
            import json as _json
            body = _json.dumps(entries, indent=2, default=str)
            return Response(
                content=body,
                media_type="application/json",
                headers={"Content-Disposition": 'attachment; filename="audit_log.json"'},
            )

        buf = StringIO()
        writer = csv.writer(buf)
        writer.writerow(["timestamp", "user", "user_id", "action", "resource", "details", "ip_address"])
        for e in entries:
            writer.writerow([
                e.get("timestamp", ""),
                e.get("username", ""),
                e.get("user_id", ""),
                e.get("action", ""),
                e.get("resource", "") or "",
                e.get("details", "") or "",
                e.get("ip_address", "") or "",
            ])
        return Response(
            content=buf.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="audit_log.csv"'},
        )

    async def _check_admin(self, request: Request) -> bool:
        """Check if current user is admin"""
        config = get_config()
        if config.auth_mode != "multi":
            return True

        user = get_request_user(request)
        return user is not None and user.is_admin
