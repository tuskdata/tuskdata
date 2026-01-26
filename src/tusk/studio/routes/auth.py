"""Authentication routes"""

from litestar import Controller, get, post, Response, Request
from litestar.params import Body
from litestar.response import Redirect

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
    PERMISSIONS,
)
from tusk.core.config import get_config
from tusk.core.logging import get_logger

log = get_logger("auth")

SESSION_COOKIE = "tusk_session"


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

        # Check session cookie
        session_id = request.cookies.get(SESSION_COOKIE)
        if not session_id:
            return {"auth_enabled": True, "user": None, "permissions": []}

        session = get_session(session_id)
        if not session:
            return {"auth_enabled": True, "user": None, "permissions": []}

        user = get_user_by_id(session.user_id)
        if not user or not user.is_active:
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

        username = data.get("username", "").strip()
        password = data.get("password", "")

        if not username or not password:
            return Response(
                content={"error": "Username and password required"},
                status_code=400,
            )

        user = authenticate(username, password)
        if not user:
            log.warning("Failed login attempt", username=username)
            return Response(
                content={"error": "Invalid username or password"},
                status_code=401,
            )

        # Create session
        ip_address = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")
        session = create_session(user.id, ip_address, user_agent)

        log.info("User logged in", username=username, user_id=user.id)

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
            delete_session(session_id)
            log.info("User logged out", session_id=session_id[:8])

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
    async def update_profile(self, request: Request, data: dict = Body()) -> dict:
        """Update current user's profile"""
        user = await self._get_current_user(request)
        if not user:
            return {"error": "Not authenticated"}

        updates = {}
        if "email" in data:
            updates["email"] = data["email"].strip() or None
        if "display_name" in data:
            updates["display_name"] = data["display_name"].strip() or None

        if updates:
            update_user(user.id, **updates)
            log.info("Profile updated", user_id=user.id)

        return {"success": True}

    @post("/password")
    async def change_password(self, request: Request, data: dict = Body()) -> dict:
        """Change current user's password"""
        user = await self._get_current_user(request)
        if not user:
            return {"error": "Not authenticated"}

        current_password = data.get("current_password", "")
        new_password = data.get("new_password", "")

        # Verify current password
        if not authenticate(user.username, current_password):
            return {"error": "Current password is incorrect"}

        if not new_password:
            return {"error": "New password required"}
        if len(new_password) < 6:
            return {"error": "Password must be at least 6 characters"}

        update_password(user.id, new_password)
        log.info("Password changed", user_id=user.id)
        return {"success": True}

    async def _get_current_user(self, request: Request):
        """Get current user from session"""
        config = get_config()
        if config.auth_mode != "multi":
            return None

        session_id = request.cookies.get(SESSION_COOKIE)
        if not session_id:
            return None

        session = get_session(session_id)
        if not session:
            return None

        return get_user_by_id(session.user_id)


class UsersController(Controller):
    """User management API (admin only)"""

    path = "/api/users"

    @get("/")
    async def get_users(self, request: Request) -> dict:
        """List all users"""
        # Check permission
        if not await self._check_admin(request):
            return {"error": "Unauthorized", "users": []}

        users = list_users()
        return {
            "users": [
                {
                    "id": u.id,
                    "username": u.username,
                    "email": u.email,
                    "display_name": u.display_name,
                    "is_admin": u.is_admin,
                    "is_active": u.is_active,
                    "created_at": u.created_at,
                    "last_login": u.last_login,
                }
                for u in users
            ]
        }

    @post("/")
    async def create_new_user(self, request: Request, data: dict = Body()) -> dict:
        """Create a new user"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized"}

        username = data.get("username", "").strip()
        password = data.get("password", "")
        email = data.get("email", "").strip() or None
        display_name = data.get("display_name", "").strip() or None
        is_admin = data.get("is_admin", False)

        if not username:
            return {"error": "Username required"}
        if not password:
            return {"error": "Password required"}
        if len(password) < 6:
            return {"error": "Password must be at least 6 characters"}

        try:
            user = create_user(
                username=username,
                password=password,
                email=email,
                display_name=display_name,
                is_admin=is_admin,
            )
            log.info("User created", username=username, user_id=user.id)
            return {
                "success": True,
                "user": {
                    "id": user.id,
                    "username": user.username,
                },
            }
        except Exception as e:
            log.error("Failed to create user", username=username, error=str(e))
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
        if len(password) < 6:
            return {"error": "Password must be at least 6 characters"}

        update_password(user_id, password)
        log.info("Password reset", user_id=user_id)
        return {"success": True}

    @post("/{user_id:str}/delete")
    async def delete_existing_user(self, request: Request, user_id: str) -> dict:
        """Delete user"""
        if not await self._check_admin(request):
            return {"error": "Unauthorized"}

        delete_user(user_id)
        log.info("User deleted", user_id=user_id)
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
        """Get current user from session"""
        session_id = request.cookies.get(SESSION_COOKIE)
        if not session_id:
            return None

        session = get_session(session_id)
        if not session:
            return None

        return get_user_by_id(session.user_id)


class GroupsController(Controller):
    """Group management API (admin only)"""

    path = "/api/groups"

    @get("/")
    async def get_groups(self, request: Request) -> dict:
        """List all groups"""
        groups = list_groups()
        return {
            "groups": [
                {
                    "id": g.id,
                    "name": g.name,
                    "description": g.description,
                    "permissions": g.permissions,
                    "created_at": g.created_at,
                }
                for g in groups
            ]
        }

    @post("/")
    async def create_new_group(self, request: Request, data: dict = Body()) -> dict:
        """Create a new group"""
        config = get_config()
        if config.auth_mode == "multi":
            session_id = request.cookies.get(SESSION_COOKIE)
            if session_id:
                session = get_session(session_id)
                if session:
                    user = get_user_by_id(session.user_id)
                    if not user or not user.is_admin:
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
    async def get_group_details(self, group_id: str) -> dict:
        """Get group details"""
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
    async def get_all_permissions(self) -> dict:
        """Get all available permissions"""
        return {
            "permissions": [
                {"code": code, "description": desc}
                for code, desc in PERMISSIONS.items()
            ]
        }


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
